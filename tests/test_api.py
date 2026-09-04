import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient
from fastapi.testclient import TestClient
from fastapi import WebSocketDisconnect

from app.main import app, websocket_fleet_endpoint
from app.state import fleet_state


@pytest.mark.asyncio
async def test_get_all_robots(async_client: AsyncClient, sample_robots_data):
    """Test GET /robots returns 200 OK and list of all seeded robots."""
    response = await async_client.get("/robots")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2
    ids = {r["robot_id"] for r in data}
    assert ids == {"robot_001", "robot_002"}


@pytest.mark.asyncio
async def test_get_robot_valid_id(async_client: AsyncClient):
    """Test GET /robots/{robot_id} returns valid robot dictionary with 200 OK."""
    response = await async_client.get("/robots/robot_001")
    assert response.status_code == 200
    data = response.json()
    assert data["robot_id"] == "robot_001"
    assert data["x"] == 10.0
    assert data["battery"] == 85.0


@pytest.mark.asyncio
async def test_get_robot_invalid_id(async_client: AsyncClient):
    """Test GET /robots/{robot_id} with invalid ID returns 404 and detail message."""
    response = await async_client.get("/robots/non_existent_robot")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"] == "Robot with id 'non_existent_robot' not found."


def test_websocket_robots_connect_and_disconnect():
    """Test WebSocket endpoint /ws/robots registers with ConnectionManager and cleans up on close."""
    from app.websocket import manager
    with patch("app.main.fleet_broadcast_worker", new_callable=AsyncMock):
        with TestClient(app) as client:
            with client.websocket_connect("/ws/robots"):
                # While connected the manager should have exactly one active connection
                assert len(manager.active_connections) == 1
            # After disconnect the manager should have cleaned up
            assert len(manager.active_connections) == 0


@pytest.mark.asyncio
async def test_websocket_robots_disconnect_handling():
    """Test WebSocketDisconnect is caught and manager.disconnect is called."""
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect(code=1000))

    with patch("app.main.manager.connect", new_callable=AsyncMock):
        with patch("app.main.manager.disconnect", new_callable=AsyncMock) as mock_disconnect:
            await websocket_fleet_endpoint(mock_ws)
            mock_disconnect.assert_called_once_with(mock_ws)


@pytest.mark.asyncio
async def test_websocket_robots_generic_exception_handling():
    """Test unexpected exceptions are caught and manager.disconnect is still called."""
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.receive_text = AsyncMock(side_effect=RuntimeError("Simulated network error"))

    with patch("app.main.manager.connect", new_callable=AsyncMock):
        with patch("app.main.manager.disconnect", new_callable=AsyncMock) as mock_disconnect:
            await websocket_fleet_endpoint(mock_ws)
            mock_disconnect.assert_called_once_with(mock_ws)
