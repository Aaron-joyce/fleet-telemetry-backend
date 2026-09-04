import asyncio
from typing import AsyncGenerator, Dict, Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.state import FleetState, fleet_state


@pytest.fixture
def sample_robots_data() -> Dict[str, Dict[str, Any]]:
    """Return sample robot seed data for testing."""
    return {
        "robot_001": {
            "x": 10.0,
            "y": 20.0,
            "battery": 85.0,
            "status": "active",
            "t": 100.0,
        },
        "robot_002": {
            "x": 5.0,
            "y": 15.0,
            "battery": 90.0,
            "status": "idle",
            "t": 105.0,
        },
    }


@pytest.fixture
def isolated_state(sample_robots_data: Dict[str, Dict[str, Any]]) -> FleetState:
    """Provide a fresh, isolated FleetState instance per test."""
    return FleetState(initial_robots=sample_robots_data)


@pytest_asyncio.fixture(autouse=True)
async def reset_global_fleet_state(
    sample_robots_data: Dict[str, Dict[str, Any]]
) -> AsyncGenerator[None, None]:
    """Reset the global fleet_state singleton before and after each test."""
    await fleet_state.reset(sample_robots_data)
    yield
    await fleet_state.reset({})


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Provide an httpx.AsyncClient bound to the FastAPI app without lifespan."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
