import asyncio
import json
from unittest.mock import patch, AsyncMock
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

    with patch("aiomqtt.Client", return_value=mock_client):
        with patch.object(
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
            mock_sleep.assert_called_once_with(1.0)


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
            mock_sleep.assert_called_once_with(1.0)


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

