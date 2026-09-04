import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from fastapi import WebSocket

from app.websocket import ConnectionManager


@pytest.fixture
def manager():
    """Fresh ConnectionManager instance per test."""
    return ConnectionManager()


@pytest.mark.asyncio
async def test_connect_adds_websocket(manager: ConnectionManager):
    """Test connect() accepts the websocket and adds it to active_connections."""
    mock_ws = AsyncMock(spec=WebSocket)
    await manager.connect(mock_ws)
    mock_ws.accept.assert_called_once()
    assert mock_ws in manager.active_connections


@pytest.mark.asyncio
async def test_disconnect_removes_websocket(manager: ConnectionManager):
    """Test disconnect() removes the websocket from active_connections."""
    mock_ws = AsyncMock(spec=WebSocket)
    await manager.connect(mock_ws)
    assert mock_ws in manager.active_connections

    await manager.disconnect(mock_ws)
    assert mock_ws not in manager.active_connections


@pytest.mark.asyncio
async def test_disconnect_unknown_websocket_is_safe(manager: ConnectionManager):
    """Test disconnect() on an unknown websocket doesn't raise (uses discard)."""
    mock_ws = AsyncMock(spec=WebSocket)
    await manager.disconnect(mock_ws)  # Should not raise


@pytest.mark.asyncio
async def test_broadcast_sends_to_all_clients(manager: ConnectionManager):
    """Test broadcast() delivers the message to every connected client."""
    ws1, ws2 = AsyncMock(spec=WebSocket), AsyncMock(spec=WebSocket)
    await manager.connect(ws1)
    await manager.connect(ws2)

    await manager.broadcast("hello")

    ws1.send_text.assert_called_once_with("hello")
    ws2.send_text.assert_called_once_with("hello")


@pytest.mark.asyncio
async def test_broadcast_skips_when_no_clients(manager: ConnectionManager):
    """Test broadcast() returns immediately when there are no connected clients."""
    # No exception, no calls — just a no-op
    await manager.broadcast("hello")


@pytest.mark.asyncio
async def test_broadcast_disconnects_failed_client(manager: ConnectionManager):
    """Test broadcast() removes a client that raises an exception on send."""
    good_ws = AsyncMock(spec=WebSocket)
    bad_ws = AsyncMock(spec=WebSocket)
    bad_ws.send_text = AsyncMock(side_effect=RuntimeError("connection lost"))

    await manager.connect(good_ws)
    await manager.connect(bad_ws)

    await manager.broadcast("test")

    # Good client received the message
    good_ws.send_text.assert_called_once_with("test")
    # Failed client was disconnected from the manager
    assert bad_ws not in manager.active_connections
    assert good_ws in manager.active_connections
