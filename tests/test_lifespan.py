import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import pytest

from app.main import lifespan, stale_robot_watcher, app
from app.state import fleet_state


@pytest.mark.asyncio
async def test_lifespan_startup_and_shutdown():
    """Verify lifespan starts background tasks and cancels them cleanly on shutdown."""
    async def mock_task():
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            pass

    with patch("app.main.start_consumer", side_effect=mock_task) as mock_consumer:
        with patch("app.main.stale_robot_watcher", side_effect=mock_task) as mock_watcher:
            async with lifespan(app):
                assert mock_consumer.called
                assert mock_watcher.called

    # If context exited without raising exceptions, tasks were cancelled and gathered cleanly.


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
