import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import pytest

from app.main import lifespan, stale_robot_watcher, app
from app.state import fleet_state


@pytest.mark.asyncio
async def test_lifespan_startup_and_shutdown():
    """Verify lifespan starts all three background tasks and cancels them cleanly on shutdown."""
    async def mock_task():
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            pass

    with patch("app.main.start_consumer", side_effect=mock_task) as mock_consumer:
        with patch("app.main.stale_robot_watcher", side_effect=mock_task) as mock_watcher:
            with patch("app.main.fleet_broadcast_worker", side_effect=mock_task) as mock_broadcast:
                async with lifespan(app):
                    assert mock_consumer.called
                    assert mock_watcher.called
                    assert mock_broadcast.called

    # If context exited without raising exceptions, all three tasks were cancelled and gathered cleanly.


@pytest.mark.asyncio
async def test_stale_robot_watcher_sweeps_stale_robots():
    """Test stale_robot_watcher calls check_and_mark_stale and logs stale robots."""
    with patch.object(
        fleet_state, "check_and_mark_stale", new_callable=AsyncMock
    ) as mock_check:
        mock_check.return_value = ["robot_001"]
        # Raise CancelledError during sleep to terminate the while True loop after 1 iteration
        with patch("asyncio.sleep", side_effect=asyncio.CancelledError()):
            await stale_robot_watcher()

        mock_check.assert_called_once_with(timeout_seconds=5.0)


@pytest.mark.asyncio
async def test_stale_robot_watcher_exception_handling():
    """Test stale_robot_watcher handles unexpected exceptions gracefully."""
    call_count = 0

    async def mock_check_with_error(timeout_seconds=5.0):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Database read failure")
        raise asyncio.CancelledError()

    with patch.object(fleet_state, "check_and_mark_stale", side_effect=mock_check_with_error):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await stale_robot_watcher()
            mock_sleep.assert_called_once_with(1.0)


@pytest.mark.asyncio
async def test_fleet_broadcast_worker_sends_when_clients_connected():
    """Test fleet_broadcast_worker serialises state and calls manager.broadcast when clients exist."""
    from app.main import fleet_broadcast_worker
    from app.websocket import manager

    with patch.object(manager, "active_connections", new={object()}):
        with patch.object(manager, "broadcast", new_callable=AsyncMock) as mock_broadcast:
            with patch("asyncio.sleep", side_effect=[None, asyncio.CancelledError()]):
                await fleet_broadcast_worker()

    assert mock_broadcast.call_count >= 1
    sent_payload = mock_broadcast.call_args[0][0]
    import json
    assert isinstance(json.loads(sent_payload), list)


@pytest.mark.asyncio
async def test_fleet_broadcast_worker_skips_when_no_clients():
    """Test fleet_broadcast_worker skips broadcast when no clients are connected."""
    from app.main import fleet_broadcast_worker
    from app.websocket import manager

    with patch.object(manager, "active_connections", new=set()):
        with patch.object(manager, "broadcast", new_callable=AsyncMock) as mock_broadcast:
            with patch("asyncio.sleep", side_effect=[None, asyncio.CancelledError()]):
                await fleet_broadcast_worker()

    mock_broadcast.assert_not_called()


@pytest.mark.asyncio
async def test_fleet_broadcast_worker_exception_recovery():
    """Test fleet_broadcast_worker recovers from unexpected exceptions without crashing."""
    from app.main import fleet_broadcast_worker
    from app.websocket import manager

    call_count = 0

    async def fail_then_cancel(*_):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("boom")
        raise asyncio.CancelledError()

    with patch.object(manager, "active_connections", new={object()}):
        with patch.object(manager, "broadcast", side_effect=fail_then_cancel):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await fleet_broadcast_worker()
