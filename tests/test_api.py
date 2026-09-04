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
    """Test WebSocket endpoint /ws/robots receives snapshot frame and closes cleanly."""
    client = TestClient(app)
    with client.websocket_connect("/ws/robots") as websocket:
        data = websocket.receive_json()
        assert isinstance(data, list)
        assert len(data) == 2
        robot_ids = [r["robot_id"] for r in data]
        assert "robot_001" in robot_ids


@pytest.mark.asyncio
async def test_websocket_robots_disconnect_handling():
    """Test WebSocketDisconnect handling in websocket endpoint."""
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.send_json = AsyncMock(side_effect=WebSocketDisconnect(code=1000))

    # Should handle WebSocketDisconnect without throwing
    await websocket_fleet_endpoint(mock_ws)
    mock_ws.accept.assert_called_once()
    mock_ws.send_json.assert_called_once()


@pytest.mark.asyncio
async def test_websocket_robots_generic_exception_handling():
    """Test generic exception handling in websocket endpoint."""
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.send_json = AsyncMock(side_effect=RuntimeError("Simulated network error"))

    # Should catch Exception, log warning, and return cleanly
    await websocket_fleet_endpoint(mock_ws)
    mock_ws.accept.assert_called_once()
    mock_ws.send_json.assert_called_once()
