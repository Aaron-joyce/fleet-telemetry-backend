import asyncio
import json
from pathlib import Path
from typing import Any

"""Loads robots.json and indexes entries by robot_id."""
def load_robots(file_path: str | Path = "robots.json") -> dict[str, dict[str, Any]]:
    path = Path(__file__).resolve().parent.parent / "data" /Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path.resolve()}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {robot["robot_id"]: robot for robot in data}

class FleetState:
    def __init__(self, initial_robots:dict[str, dict[str, Any]] | None = None) -> None:
        self._lock = asyncio.Lock()
        self._robots:dict[str, dict[str, Any]] = {}

        if initial_robots:
            for robot_id, meta in initial_robots.items():
                self._robots[robot_id]={
                    "robot_id" : robot_id,
                    "x":float(meta.get("x", 0.0)),
                    "y":float(meta.get("y", 0.0)),
                    "battery": float(meta.get("battery", 100.0)),
                    "status":meta.get("status", "idle"),
                    "t":float(meta.get("t", 0.0)),
                }

    async def update_robot(self, robot_id:str, telemetry:dict[str, Any])-> bool:
        incoming_t = float(telemetry.get("t", 0.0))

        async with self._lock:
            current = self._robots.get(robot_id)

            if current and incoming_t<=current.get("t", 0.0):
                return False

            if current:
                current.update(
                    {
                        "x": float(telemetry.get("x", current["x"])),
                        "y": float(telemetry.get("y", current["y"])),
                        "battery": float(telemetry.get("battery", current["battery"])),
                        "status": telemetry.get("status", current["status"]),
                        "t": incoming_t,
                    }
                )
            else:
                self._robots[robot_id] = {
                    "robot_id": robot_id,
                    "x": float(telemetry.get("x", 0.0)),
                    "y": float(telemetry.get("y", 0.0)),
                    "battery": float(telemetry.get("battery", 100.0)),
                    "status": telemetry.get("status", "active"),
                    "t": incoming_t,
                }
            return True
    async def get_all_robots(self)-> list[dict[str, Any]]:
        async with self._lock:
            return [dict(robot) for robot in self._robots.values()]

    async def get_robot(self, robot_id:str)-> dict[str, Any] | None:
        async with self._lock:
            robot = self._robots.get(robot_id)
            return dict(robot) if robot else None

    async def reset(self, initial_robots:dict[str, dict[str, Any]] | None=None) -> None:
        async with self._lock:
            self._robots.clear()
            if initial_robots:
                for robot_id, meta in initial_robots.items():
                    self._robots[robot_id] = {
                        "robot_id": robot_id,
                        "x": float(meta.get("x", 0.0)),
                        "y": float(meta.get("y", 0.0)),
                        "battery": float(meta.get("battery", 100.0)),
                        "status": meta.get("status", "idle"),
                        "t": float(meta.get("t", 0.0)),
                    }

try:
    _initial_data = load_robots()
except FileNotFoundError:
    _initial_data = {}
fleet_state = FleetState(_initial_data)
