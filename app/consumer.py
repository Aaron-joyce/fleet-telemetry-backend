import asyncio
import json
import logging
import os
from collections.abc import KeysView
from typing import Any

import aiomqtt
from dotenv import load_dotenv

from app.state import fleet_state

load_dotenv()
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "fleet/telemetry")
SUBSCRIBE_TOPIC = f"{TOPIC_PREFIX}/+"

logger = logging.getLogger("fleet_consumer")
logging.basicConfig(level=logging.DEBUG)

async def start_consumer() -> None:
    logger.info(f"Starting MQTT consumer, connecting to {MQTT_HOST}:{MQTT_PORT}....")

    async with aiomqtt.Client(hostname=MQTT_HOST, port=MQTT_PORT) as client:
        await client.subscribe(SUBSCRIBE_TOPIC, qos=1)
        logger.info(f"Subscribed to {SUBSCRIBE_TOPIC}")

        async for message in client.messages:
            try:
                robot_id = str(message.topic).split("/")[-1]
                payload:dict[str, Any] = json.loads(message.payload.decode("utf-8"))

                updated = await fleet_state.update_robot(robot_id=robot_id, telemetry=payload)
                if updated:
                    logger.debug(f"{robot_id} Telemetry updated= {round(payload.get('t', 0.0),ndigits=2)}")
                else:
                    logger.debug(f"{robot_id} Dropped Stale/out-of-order data.")
            except (json.JSONDecodeError, UnicodeDecodeError) as err:
                logger.warning(f"Malformed message on topic {message.topic}: {err}")
            except Exception as e:
                logger.error(f"Unexpected error processing telemetry frame: {e}")

if __name__=="__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        logger.info("Consumer stopped manually.")
