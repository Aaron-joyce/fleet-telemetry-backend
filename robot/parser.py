import json
from collections import defaultdict
from collections.abc import Generator
from pathlib import Path
from typing import Any


"""Streams events line-by-line from events.jsonl"""
def stream_events(file_path: str | Path = "events.jsonl") -> Generator[dict[str, Any]]:
    path = Path(__file__).parent.parent / "data" /Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Log file not found: {path.resolve()}")

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                yield json.loads(clean_line)
            except json.JSONDecodeError as err:
                print(f"[WARN] Skipping malformed JSON on line {line_num}: {err}")


def load_events_by_robot(
    file_path: str | Path = "events.jsonl",
) -> dict[str, list[dict[str, Any]]]:
    """Groups all recorded events by robot_id and sorts each robot's timeline chronologically by time."""
    robot_events: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for event in stream_events(file_path):
        robot_id = event.get("robot_id")
        if robot_id:
            robot_events[robot_id].append(event)

    # Ensure chronological order in place
    for r_id in robot_events:
        robot_events[r_id].sort(key=lambda item: item.get("t", 0))

    return dict(robot_events)


if __name__ == "__main__":
    robots = load_robots()
    print(f"Loaded {len(robots)} robots: {list(robots.keys())}")

    events_map = load_events_by_robot()
    for r_id, evts in events_map.items():
        print(f"Robot {r_id}: {len(evts)} events (t_start={evts[0]['t']}s, t_end={evts[-1]['t']}s)")
