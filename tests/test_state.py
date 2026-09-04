import asyncio
import time
import pytest
from app.state import FleetState, load_robots


def test_load_robots_success():
    """Test loading valid robots from default JSON seed file."""
    robots = load_robots("robots.json")
    assert isinstance(robots, dict)
    assert len(robots) > 0
    # Check that robot items have robot_id
    for r_id, meta in robots.items():
        assert meta["robot_id"] == r_id


def test_load_robots_file_not_found():
    """Test load_robots raises FileNotFoundError when seed file is missing."""
    with pytest.raises(FileNotFoundError) as exc_info:
        load_robots("non_existent_file.json")
    assert "Configuration file not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fleet_state_init_empty():
    """Test FleetState initialization with no initial robots."""
    state = FleetState()
    robots = await state.get_all_robots()
    assert robots == []


@pytest.mark.asyncio
async def test_monotonic_timestamp_rule(isolated_state: FleetState):
    """Test monotonic timestamp update rule (accept newer, reject older/equal)."""
    robot_id = "robot_001"
    
    # Accept newer timestamp (105.0 > 100.0)
    updated = await isolated_state.update_robot(robot_id, {"t": 105.0, "x": 12.0})
    assert updated is True
    robot = await isolated_state.get_robot(robot_id)
    assert robot["t"] == 105.0
    assert robot["x"] == 12.0

    # Reject duplicate timestamp (105.0 == 105.0)
    updated_dup = await isolated_state.update_robot(robot_id, {"t": 105.0, "x": 15.0})
    assert updated_dup is False
    robot_after_dup = await isolated_state.get_robot(robot_id)
    assert robot_after_dup["x"] == 12.0  # Unchanged

    # Reject older timestamp (90.0 < 105.0)
    updated_old = await isolated_state.update_robot(robot_id, {"t": 90.0, "x": 20.0})
    assert updated_old is False
    robot_after_old = await isolated_state.get_robot(robot_id)
    assert robot_after_old["x"] == 12.0  # Unchanged


@pytest.mark.asyncio
async def test_partial_field_merges(isolated_state: FleetState):
    """Test updating coordinates preserves existing battery level and other fields."""
    robot_id = "robot_001"
    initial = await isolated_state.get_robot(robot_id)
    assert initial["battery"] == 85.0
    assert initial["y"] == 20.0

    # Update only x coordinate and t
    updated = await isolated_state.update_robot(robot_id, {"t": 110.0, "x": 99.0})
    assert updated is True

    current = await isolated_state.get_robot(robot_id)
    assert current["x"] == 99.0
    assert current["y"] == 20.0  # Preserved
    assert current["battery"] == 85.0  # Preserved
    assert current["status"] == "active"  # Preserved


@pytest.mark.asyncio
async def test_brand_new_robot_id(isolated_state: FleetState):
    """Test updating a brand-new robot ID not present in seed data."""
    new_id = "robot_new_999"
    assert await isolated_state.get_robot(new_id) is None

    updated = await isolated_state.update_robot(
        new_id, {"t": 50.0, "x": 1.0, "y": 2.0, "battery": 75.0}
    )
    assert updated is True

    robot = await isolated_state.get_robot(new_id)
    assert robot is not None
    assert robot["robot_id"] == new_id
    assert robot["x"] == 1.0
    assert robot["y"] == 2.0
    assert robot["battery"] == 75.0
    assert robot["status"] == "active"
    assert robot["t"] == 50.0


@pytest.mark.asyncio
async def test_check_and_mark_stale(isolated_state: FleetState):
    """Test check_and_mark_stale transitions inactive robots to offline and skips already-offline ones."""
    # Set up robot_001 last_seen_at in the past
    async with isolated_state._lock:
        isolated_state._robots["robot_001"]["last_seen_at"] = time.monotonic() - 10.0
        # Set robot_002 status to offline
        isolated_state._robots["robot_002"]["status"] = "offline"
        isolated_state._robots["robot_002"]["last_seen_at"] = time.monotonic() - 10.0

    stale_ids = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)
    assert "robot_001" in stale_ids
    assert "robot_002" not in stale_ids  # Already offline, so skipped

    r1 = await isolated_state.get_robot("robot_001")
    assert r1["status"] == "offline"

    # Second check should skip robot_001 since status is now offline
    stale_ids_2 = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)
    assert "robot_001" not in stale_ids_2


@pytest.mark.asyncio
async def test_copy_on_read_isolation(isolated_state: FleetState):
    """Test mutating returned dict from get_all_robots or get_robot does not mutate internal store."""
    all_robots = await isolated_state.get_all_robots()
    assert len(all_robots) > 0
    all_robots[0]["x"] = 99999.0

    # Internal state should be unchanged
    fresh_robot = await isolated_state.get_robot(all_robots[0]["robot_id"])
    assert fresh_robot["x"] != 99999.0

    # Mutating single get_robot dictionary
    single_robot = await isolated_state.get_robot("robot_001")
    single_robot["battery"] = 0.0

    fresh_robot_001 = await isolated_state.get_robot("robot_001")
    assert fresh_robot_001["battery"] == 85.0


@pytest.mark.asyncio
async def test_get_robot_non_existent(isolated_state: FleetState):
    """Test get_robot returns None for non-existent robot ID."""
    assert await isolated_state.get_robot("does_not_exist") is None


@pytest.mark.asyncio
async def test_reset_functionality(isolated_state: FleetState):
    """Test resetting state with None and with a custom dictionary."""
    await isolated_state.reset()
    assert await isolated_state.get_all_robots() == []

    new_robots = {
        "robot_reset_1": {"x": 1.0, "y": 2.0, "battery": 50.0, "status": "active", "t": 5.0}
    }
    await isolated_state.reset(new_robots)
    robots = await isolated_state.get_all_robots()
    assert len(robots) == 1
    assert robots[0]["robot_id"] == "robot_reset_1"


def test_module_load_robots_file_not_found_fallback():
    """Test module import fallback when load_robots raises FileNotFoundError at module level."""
    import importlib
    from unittest.mock import patch
    import app.state

    with patch("pathlib.Path.is_file", return_value=False):
        importlib.reload(app.state)
        assert app.state._initial_data == {}

    # Restore normal state module
    importlib.reload(app.state)


