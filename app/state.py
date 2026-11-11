import asyncio
import json
from pathlib import Path
import time
from typing import Any


def load_robots(
    file_path: str | Path = "robots.json",
) -> dict[str, dict[str, Any]]:
    path = Path(__file__).resolve().parent.parent / "data" / Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {path.resolve()}"
        )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {robot["robot_id"]: robot for robot in data}


class FleetState:

    def __init__(
        self, initial_robots: dict[str, dict[str, Any]] | None = None
    ) -> None:
        self._lock = asyncio.Lock()
        self._robots: dict[str, dict[str, Any]] = {}
        now = time.monotonic()

        if initial_robots:
            for robot_id, meta in initial_robots.items():
                self._robots[robot_id] = {
                    "robot_id": robot_id,
                    "x": float(meta.get("x", 0.0)),
                    "y": float(meta.get("y", 0.0)),
                    "battery": float(meta.get("battery", 100.0)),
                    "status": meta.get("status", "idle"),
                    "t": float(meta.get("t", 0.0)),
                    "last_seen_at": now,
                    "session_id": meta.get("session_id"),
                    "dropped_frames": int(meta.get("dropped_frames", 0)),
                }

    async def update_robot(
        self, robot_id: str, telemetry: dict[str, Any]
    ) -> bool:
        incoming_t = float(telemetry.get("t", 0.0))
        incoming_session = telemetry.get("session_id")
        now = time.monotonic()

        async with self._lock:
            current = self._robots.get(robot_id)

            if current:
                session_changed = (
                    incoming_session is not None
                    and incoming_session != current.get("session_id")
                )

                if not session_changed and incoming_t <= current.get("t", 0.0):
                    current["dropped_frames"] = current.get("dropped_frames", 0) + 1
                    return False

                # Accept: either newer t within same session, or new session entirely
                current.update(
                    {
                        "x": float(telemetry.get("x", current["x"])),
                        "y": float(telemetry.get("y", current["y"])),
                        "battery": float(
                            telemetry.get("battery", current["battery"])
                        ),
                        "status": telemetry.get("status", current["status"]),
                        "t": incoming_t,
                        "last_seen_at": now,
                        "session_id": incoming_session,
                    }
                )
            else:
                # First-ever frame for this robot
                self._robots[robot_id] = {
                    "robot_id": robot_id,
                    "x": float(telemetry.get("x", 0.0)),
                    "y": float(telemetry.get("y", 0.0)),
                    "battery": float(telemetry.get("battery", 100.0)),
                    "status": telemetry.get("status", "active"),
                    "t": incoming_t,
                    "last_seen_at": now,
                    "session_id": incoming_session,
                    "dropped_frames": 0,
                }
            return True

    async def check_and_mark_stale(
        self, timeout_seconds: float = 5.0
    ) -> list[str]:
        now = time.monotonic()
        offline_ids: list[str] = []

        async with self._lock:
            for robot_id, robot in self._robots.items():
                if (
                    robot.get("status") != "offline"
                    and (now - robot.get("last_seen_at", now)) > timeout_seconds
                ):
                    robot["status"] = "offline"
                    offline_ids.append(robot_id)

        return offline_ids

    async def get_all_robots(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [dict(robot) for robot in self._robots.values()]

    async def get_robot(self, robot_id: str) -> dict[str, Any] | None:
        async with self._lock:
            robot = self._robots.get(robot_id)
            return dict(robot) if robot else None

    async def reset(
        self, initial_robots: dict[str, dict[str, Any]] | None = None
    ) -> None:
        async with self._lock:
            self._robots.clear()
            now = time.monotonic()
            if initial_robots:
                for robot_id, meta in initial_robots.items():
                    self._robots[robot_id] = {
                        "robot_id": robot_id,
                        "x": float(meta.get("x", 0.0)),
                        "y": float(meta.get("y", 0.0)),
                        "battery": float(meta.get("battery", 100.0)),
                        "status": meta.get("status", "idle"),
                        "t": float(meta.get("t", 0.0)),
                        "last_seen_at": now,
                        "session_id": meta.get("session_id"),
                        "dropped_frames": int(meta.get("dropped_frames", 0)),
                    }


try:
    _initial_data = load_robots()
except FileNotFoundError:
    _initial_data = {}
fleet_state = FleetState(_initial_data)
