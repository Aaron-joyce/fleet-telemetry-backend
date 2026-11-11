import asyncio
import json
import random
from unittest.mock import AsyncMock, MagicMock, patch

import aiomqtt
import pytest

from app.consumer import start_consumer
from app.state import fleet_state


class MockMessage:
    def __init__(self, topic: str, payload: bytes):
        self.topic = topic
        self.payload = payload


class MockMessages:
    def __init__(self, messages):
        self.messages = messages

    def __aiter__(self):
        self._iter = iter(self.messages)
        return self

    async def __anext__(self):
        try:
            item = next(self._iter)
            if isinstance(item, Exception):
                raise item
            return item
        except StopIteration:
            raise asyncio.CancelledError()


class MockMQTTClient:
    def __init__(self, messages=None):
        self.messages = MockMessages(messages or [])

    async def subscribe(self, topic, qos=1):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.mark.asyncio
async def test_consumer_valid_and_stale_messages():
    """Test consuming valid telemetry and dropping stale frames."""
    valid_payload = json.dumps({"x": 50.0, "y": 60.0, "t": 200.0}).encode("utf-8")
    stale_payload = json.dumps({"x": 10.0, "y": 10.0, "t": 50.0}).encode("utf-8")

    messages = [
        MockMessage("fleet/telemetry/robot_001", valid_payload),
        MockMessage("fleet/telemetry/robot_001", stale_payload),
    ]

    mock_client = MockMQTTClient(messages)

    with patch("aiomqtt.Client", return_value=mock_client):
        await start_consumer()

    robot = await fleet_state.get_robot("robot_001")
    assert robot["x"] == 50.0
    assert robot["y"] == 60.0
    assert robot["t"] == 200.0
    assert robot["dropped_frames"] == 1


@pytest.mark.asyncio
async def test_consumer_reports_stale_frame_as_not_updated():
    """Test stale frame return value is False when consumed."""
    valid = json.dumps({"x": 50.0, "y": 60.0, "t": 200.0}).encode()
    stale = json.dumps({"x": 10.0, "y": 10.0, "t": 200.0}).encode()

    messages = [
        MockMessage("fleet/telemetry/robot_001", valid),
        MockMessage("fleet/telemetry/robot_001", stale),
    ]
    mock_client = MockMQTTClient(messages)
    results = []

    original_update = fleet_state.update_robot

    async def recording_update(robot_id, telemetry):
        result = await original_update(robot_id, telemetry)
        results.append(result)
        return result

    with patch("aiomqtt.Client", return_value=mock_client):
        with patch.object(fleet_state, "update_robot", side_effect=recording_update):
            await start_consumer()

    assert results == [True, False]  # first accepted, second dropped



@pytest.mark.asyncio
async def test_consumer_malformed_payloads():
    """Test handling of JSONDecodeError and UnicodeDecodeError without crashing loop."""
    invalid_json = MockMessage("fleet/telemetry/robot_001", b"{invalid json")
    invalid_unicode = MockMessage("fleet/telemetry/robot_001", b"\x80\x81\x82")

    messages = [invalid_json, invalid_unicode]
    mock_client = MockMQTTClient(messages)

    with patch("aiomqtt.Client", return_value=mock_client):
        # Should finish cleanly without raising exceptions
        await start_consumer()


@pytest.mark.asyncio
async def test_consumer_unexpected_processing_exception():
    """Test handling of unexpected exception during telemetry processing."""
    valid_payload = json.dumps({"x": 1.0, "t": 300.0}).encode("utf-8")
    messages = [MockMessage("fleet/telemetry/robot_001", valid_payload)]
    mock_client = MockMQTTClient(messages)

    with patch("aiomqtt.Client", return_value=mock_client), patch.object(
        fleet_state, "update_robot", side_effect=RuntimeError("State update error")
    ):
        await start_consumer()


@pytest.mark.asyncio
async def test_consumer_mqtt_error_reconnect():
    """Test aiomqtt.MqttError handling and reconnect backoff logic."""
    call_count = 0

    class ExceptionClient:
        async def __aenter__(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiomqtt.MqttError("Connection dropped")
            raise asyncio.CancelledError()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("aiomqtt.Client", return_value=ExceptionClient()):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await start_consumer()
            assert mock_sleep.call_count == 1
            sleep_val = mock_sleep.call_args[0][0]
            assert 0.0 <= sleep_val <= 1.0


@pytest.mark.asyncio
async def test_consumer_unexpected_outer_exception():
    """Test handling unexpected non-MQTT exceptions in outer loop."""
    call_count = 0

    class ExceptionClient:
        async def __aenter__(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Unexpected error")
            raise asyncio.CancelledError()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("aiomqtt.Client", return_value=ExceptionClient()):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await start_consumer()
            assert mock_sleep.call_count == 1
            sleep_val = mock_sleep.call_args[0][0]
            assert 0.0 <= sleep_val <= 1.0


@pytest.mark.asyncio
async def test_consumer_cancelled_error():
    """Test asyncio.CancelledError cleanly breaks consumer loop."""
    class CancelledClient:
        async def __aenter__(self):
            raise asyncio.CancelledError()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("aiomqtt.Client", return_value=CancelledClient()):
        await start_consumer()


def test_consumer_main_block_keyboard_interrupt():
    """Test __main__ block handling KeyboardInterrupt."""
    import runpy
    with patch("asyncio.run", side_effect=KeyboardInterrupt):
        runpy.run_module("app.consumer", run_name="__main__")


@pytest.mark.asyncio
async def test_consumer_reboot_and_recovery():
    """Test E: Consumer integration: robot reboots and keeps reporting."""
    frame1 = json.dumps(
        {"x": 10.0, "y": 20.0, "t": 500.0, "session_id": "s1"}
    ).encode("utf-8")
    frame2 = json.dumps(
        {"x": 11.0, "y": 21.0, "t": 100.0, "session_id": "s1"}
    ).encode("utf-8")
    frame3 = json.dumps(
        {"x": 12.0, "y": 22.0, "t": 5.0, "session_id": "s2"}
    ).encode("utf-8")

    messages = [
        MockMessage("fleet/telemetry/robot_001", frame1),
        MockMessage("fleet/telemetry/robot_001", frame2),
        MockMessage("fleet/telemetry/robot_001", frame3),
    ]

    mock_client = MockMQTTClient(messages)

    with patch("aiomqtt.Client", return_value=mock_client):
        await start_consumer()

    robot = await fleet_state.get_robot("robot_001")
    assert robot is not None
    assert robot["t"] == 5.0
    assert robot["session_id"] == "s2"
    assert robot["x"] == 12.0
    assert robot["y"] == 22.0
    assert robot["dropped_frames"] == 1


@pytest.mark.asyncio
async def test_consumer_reconnect_uses_jitter_first_interval():
    """Test A: Jitter is applied; asyncio.sleep is called with expected jittered value."""
    rng = random.Random(42)
    expected_sleep = rng.uniform(0, 1.0)  # pre-compute with same seed

    rng2 = random.Random(42)  # same seed for the actual call

    call_count = 0

    class FailOnceThenCancel:
        async def __aenter__(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiomqtt.MqttError("dropped")
            raise asyncio.CancelledError()

        async def __aexit__(self, *a):
            pass

    with patch("aiomqtt.Client", return_value=FailOnceThenCancel()):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await start_consumer(_rng=rng2)
            mock_sleep.assert_called_once_with(pytest.approx(expected_sleep))


@pytest.mark.asyncio
async def test_consumer_backoff_sequence_caps_at_30():
    """Test B: Full backoff sequence: intervals double up to cap 30.0."""
    caps = []

    class AlwaysFail:
        count = 0

        async def __aenter__(self):
            AlwaysFail.count += 1
            if AlwaysFail.count > 7:
                raise asyncio.CancelledError()
            raise aiomqtt.MqttError("boom")

        async def __aexit__(self, *a):
            pass

    rng = MagicMock()
    rng.uniform.side_effect = lambda lo, hi: (caps.append(hi), hi)[1]

    with patch("aiomqtt.Client", return_value=AlwaysFail()):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await start_consumer(_rng=rng)

    assert caps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_consumer_backoff_resets_after_success():
    """Test C: Backoff resets to 1.0 after successful connection."""
    caps = []
    call_count = 0

    class FailSucceedFailClient:
        def __init__(self):
            self.messages = MockMessages([aiomqtt.MqttError("stream drop")])

        async def subscribe(self, topic, qos=1):
            pass

        async def __aenter__(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiomqtt.MqttError("fail 1")
            elif call_count == 2:
                return self
            raise asyncio.CancelledError()

        async def __aexit__(self, *a):
            pass

    rng = MagicMock()
    rng.uniform.side_effect = lambda lo, hi: (caps.append(hi), hi)[1]

    with patch("aiomqtt.Client", return_value=FailSucceedFailClient()):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await start_consumer(_rng=rng)

    assert caps == [1.0, 1.0]


@pytest.mark.asyncio
async def test_consumer_unexpected_exception_uses_jitter():
    """Test D: Unexpected exceptions also receive jitter."""
    call_count = 0

    class ExceptionClient:
        async def __aenter__(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Unexpected error")
            raise asyncio.CancelledError()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    rng = MagicMock()
    rng.uniform.return_value = 0.42

    with patch("aiomqtt.Client", return_value=ExceptionClient()):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await start_consumer(_rng=rng)
            rng.uniform.assert_called_once_with(0, 1.0)
            mock_sleep.assert_called_once_with(0.42)



