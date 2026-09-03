import asyncio
import json
import os
from typing import Any

import aiomqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MULTIPLIER = float(os.getenv("MULTIPLIER", 1.0))
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "fleet/telemetry")

async def start_publisher(robot_id:str, events:list[dict ], multiplier:float=MULTIPLIER, topic:str=TOPIC_PREFIX):
    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as client:
        prev_t = events[0].get("t", 0.0) if events else 0.0
        for event in events:
            curr_t = event.get("t", prev_t)
            time_delta =  max(0.0, (curr_t-prev_t)/multiplier)

            if time_delta>0:
                await asyncio.sleep(time_delta)

            payload = json.dumps(event)
            full_topic = f"{topic}/{robot_id}"
            await client.publish(full_topic, payload=payload, qos=1)
            print(f"[{robot_id}] Published to {full_topic}: t={curr_t}")
            prev_t = curr_t


def run_publisher_sync(
    robot_id: str,
    events: list[dict[str, Any]],
    multiplier: float = MULTIPLIER,
) -> None:
    """Entry point callable for multiprocessing.Process in run_fleet.py."""
    asyncio.run(start_publisher(robot_id, events, multiplier))

if __name__=="__main__":
    import sys

    from parser import load_events_by_robot

    target_id = sys.argv[1] if len(sys.argv) > 1 else  "r1"
    fleet_events = load_events_by_robot()
    run_publisher_sync(target_id, fleet_events[target_id])
