import asyncio
import time
from unittest.mock import patch
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
async def test_update_robot_explicit_status(isolated_state: FleetState):
    """Test updating robot with explicit status overrides current status."""
    robot_id = "robot_001"
    updated = await isolated_state.update_robot(
        robot_id, {"t": 120.0, "status": "charging"}
    )
    assert updated is True
    robot = await isolated_state.get_robot(robot_id)
    assert robot["status"] == "charging"


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

    stale_ids, _ = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)
    assert "robot_001" in stale_ids
    assert "robot_002" not in stale_ids  # Already offline, so skipped

    r1 = await isolated_state.get_robot("robot_001")
    assert r1["status"] == "offline"
    assert r1["offline_since"] is not None

    # Second check should skip robot_001 since status is now offline
    stale_ids_2, _ = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)
    assert "robot_001" not in stale_ids_2


@pytest.mark.asyncio
async def test_check_and_mark_stale_just_under_threshold(isolated_state: FleetState):
    """Test boundary: robot just under / at threshold (5.0s) is NOT marked offline."""
    base_time = 1000.0
    async with isolated_state._lock:
        # Exactly 5.0s ago (not strictly greater than 5.0s)
        isolated_state._robots["robot_001"]["last_seen_at"] = base_time - 5.0

    with patch("time.monotonic", return_value=base_time):
        offline_ids, _ = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)

    assert "robot_001" not in offline_ids
    robot = await isolated_state.get_robot("robot_001")
    assert robot["status"] != "offline"


@pytest.mark.asyncio
async def test_check_and_mark_stale_just_over_threshold(isolated_state: FleetState):
    """Test boundary: robot just over threshold (5.1s) IS marked offline."""
    base_time = 1000.0
    async with isolated_state._lock:
        # 5.1s ago — strictly greater than 5.0s
        isolated_state._robots["robot_001"]["last_seen_at"] = base_time - 5.1

    with patch("time.monotonic", return_value=base_time):
        offline_ids, _ = await isolated_state.check_and_mark_stale(timeout_seconds=5.0)

    assert "robot_001" in offline_ids
    robot = await isolated_state.get_robot("robot_001")
    assert robot["status"] == "offline"


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


@pytest.mark.asyncio
async def test_session_normal_ordering_enforced(isolated_state: FleetState):
    """Test A: Normal session, ordering still enforced within session."""
    robot_id = "robot_session_a"
    # send t=100 session="abc" -> accepted
    res1 = await isolated_state.update_robot(
        robot_id, {"t": 100.0, "session_id": "abc", "x": 1.0}
    )
    assert res1 is True
    r1 = await isolated_state.get_robot(robot_id)
    assert r1["t"] == 100.0
    assert r1["session_id"] == "abc"
    assert r1["dropped_frames"] == 0

    # send t=90 session="abc" -> dropped, dropped_frames == 1
    res2 = await isolated_state.update_robot(
        robot_id, {"t": 90.0, "session_id": "abc", "x": 2.0}
    )
    assert res2 is False
    r2 = await isolated_state.get_robot(robot_id)
    assert r2["t"] == 100.0
    assert r2["x"] == 1.0
    assert r2["dropped_frames"] == 1

    # send t=110 session="abc" -> accepted
    res3 = await isolated_state.update_robot(
        robot_id, {"t": 110.0, "session_id": "abc", "x": 3.0}
    )
    assert res3 is True
    r3 = await isolated_state.get_robot(robot_id)
    assert r3["t"] == 110.0
    assert r3["x"] == 3.0
    assert r3["dropped_frames"] == 1


@pytest.mark.asyncio
async def test_session_reset_recovers_frozen_robot(isolated_state: FleetState):
    """Test B: Session reset recovers a frozen robot after reboot."""
    robot_id = "robot_session_b"
    # send t=500 session="abc" -> accepted
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 500.0, "session_id": "abc", "x": 10.0, "y": 20.0}
        )
        is True
    )

    # send t=10 session="abc" -> dropped (old session, stale t)
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 10.0, "session_id": "abc", "x": 15.0, "y": 25.0}
        )
        is False
    )
    r_stale = await isolated_state.get_robot(robot_id)
    assert r_stale["dropped_frames"] == 1
    assert r_stale["t"] == 500.0

    # send t=10 session="xyz" -> accepted (new session resets baseline)
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 10.0, "session_id": "xyz", "x": 30.0, "y": 40.0}
        )
        is True
    )
    r_reset = await isolated_state.get_robot(robot_id)
    assert r_reset["t"] == 10.0
    assert r_reset["x"] == 30.0
    assert r_reset["y"] == 40.0
    assert r_reset["session_id"] == "xyz"

    # send t=11 session="xyz" -> accepted
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 11.0, "session_id": "xyz", "x": 31.0, "y": 41.0}
        )
        is True
    )
    r_next = await isolated_state.get_robot(robot_id)
    assert r_next["t"] == 11.0

    # send t=10 session="xyz" -> dropped
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 10.0, "session_id": "xyz", "x": 32.0, "y": 42.0}
        )
        is False
    )
    r_final = await isolated_state.get_robot(robot_id)
    assert r_final["t"] == 11.0
    assert r_final["session_id"] == "xyz"
    assert r_final["dropped_frames"] == 2


@pytest.mark.asyncio
async def test_no_session_id_backward_compatibility(isolated_state: FleetState):
    """Test C: Backward compatibility when session_id is absent."""
    robot_id = "robot_session_c"
    # send t=100 no session_id -> accepted
    assert await isolated_state.update_robot(robot_id, {"t": 100.0, "x": 5.0}) is True
    r1 = await isolated_state.get_robot(robot_id)
    assert r1["t"] == 100.0
    assert r1["session_id"] is None
    assert r1["dropped_frames"] == 0

    # send t=90 no session_id -> dropped, dropped_frames == 1
    assert await isolated_state.update_robot(robot_id, {"t": 90.0, "x": 6.0}) is False
    r2 = await isolated_state.get_robot(robot_id)
    assert r2["t"] == 100.0
    assert r2["x"] == 5.0
    assert r2["dropped_frames"] == 1


@pytest.mark.asyncio
async def test_dropped_frames_counter_accumulates(isolated_state: FleetState):
    """Test D: dropped_frames counter accumulates across multiple stale frames."""
    robot_id = "robot_session_d"
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 100.0, "session_id": "s1"}
        )
        is True
    )

    # Send 3 stale frames within the same session
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 90.0, "session_id": "s1"}
        )
        is False
    )
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 80.0, "session_id": "s1"}
        )
        is False
    )
    assert (
        await isolated_state.update_robot(
            robot_id, {"t": 100.0, "session_id": "s1"}
        )
        is False
    )

    robot = await isolated_state.get_robot(robot_id)
    assert robot["dropped_frames"] == 3


@pytest.mark.asyncio
async def test_eviction_removes_offline_robot(isolated_state: FleetState):
    """Test A: Eviction removes offline robot after TTL."""
    # Force robot_001 offline and set offline_since far in the past
    async with isolated_state._lock:
        isolated_state._robots["robot_001"]["status"] = "offline"
        isolated_state._robots["robot_001"]["offline_since"] = time.monotonic() - 400.0

    _, evicted = await isolated_state.check_and_mark_stale(
        timeout_seconds=5.0,
        eviction_ttl_seconds=300.0,
    )

    assert "robot_001" in evicted
    assert await isolated_state.get_robot("robot_001") is None


@pytest.mark.asyncio
async def test_eviction_does_not_remove_robot_within_ttl(isolated_state: FleetState):
    """Test B: Robot within TTL is not evicted."""
    async with isolated_state._lock:
        isolated_state._robots["robot_001"]["status"] = "offline"
        isolated_state._robots["robot_001"]["offline_since"] = time.monotonic() - 60.0  # 60s, TTL=300

    _, evicted = await isolated_state.check_and_mark_stale(
        timeout_seconds=5.0,
        eviction_ttl_seconds=300.0,
    )

    assert "robot_001" not in evicted
    assert await isolated_state.get_robot("robot_001") is not None


@pytest.mark.asyncio
async def test_evicted_robot_disappears_from_get_all_robots(isolated_state: FleetState):
    """Test C: Evicted robot disappears from get_all_robots."""
    all_robots_before = await isolated_state.get_all_robots()
    count_before = len(all_robots_before)
    assert any(r["robot_id"] == "robot_001" for r in all_robots_before)

    async with isolated_state._lock:
        isolated_state._robots["robot_001"]["status"] = "offline"
        isolated_state._robots["robot_001"]["offline_since"] = time.monotonic() - 400.0

    _, evicted = await isolated_state.check_and_mark_stale(
        timeout_seconds=5.0,
        eviction_ttl_seconds=300.0,
    )
    assert "robot_001" in evicted

    all_robots_after = await isolated_state.get_all_robots()
    assert len(all_robots_after) == count_before - 1
    assert not any(r["robot_id"] == "robot_001" for r in all_robots_after)


@pytest.mark.asyncio
async def test_reconnected_robot_clears_offline_since(isolated_state: FleetState):
    """Test D: Robot that reconnects clears offline_since."""
    # Set robot offline
    async with isolated_state._lock:
        isolated_state._robots["robot_001"]["status"] = "offline"
        isolated_state._robots["robot_001"]["offline_since"] = time.monotonic() - 60.0

    # Robot sends a fresh frame with higher t
    await isolated_state.update_robot("robot_001", {"t": 9999.0, "x": 1.0})

    robot = await isolated_state.get_robot("robot_001")
    assert robot["offline_since"] is None
    assert robot["status"] != "offline"




