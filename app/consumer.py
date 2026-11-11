import asyncio
import json
import logging
import os
from typing import Any

import aiomqtt
from dotenv import load_dotenv

from app.broker_state import broker_state
from app.state import fleet_state

load_dotenv()
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "fleet/telemetry")
SUBSCRIBE_TOPIC = f"{TOPIC_PREFIX}/+"

logger = logging.getLogger("fleet_consumer")
logging.basicConfig(level=logging.DEBUG)

async def start_consumer() -> None:

    reconnect_interval = 1.0
    max_reconnect_interval = 30

    while True:
        try:
            logger.info(f"Starting MQTT consumer, connecting to {MQTT_HOST}:{MQTT_PORT}....")
            async with aiomqtt.Client(hostname=MQTT_HOST, port=MQTT_PORT) as client:
                await client.subscribe(SUBSCRIBE_TOPIC, qos=1)
                broker_state.set_connected(True)
                logger.info(f"Subscribed to {SUBSCRIBE_TOPIC}")
                reconnect_interval = 1.0

                async for message in client.messages:
                    try:
                        robot_id = str(message.topic).split("/")[-1]
                        payload:dict[str, Any] = json.loads(message.payload.decode("utf-8"))

                        updated = await fleet_state.update_robot(robot_id=robot_id, telemetry=payload)
                        incoming_session = payload.get("session_id")
                        if updated:
                            if incoming_session:
                                logger.debug(
                                    f"{robot_id} frame accepted (session={incoming_session}, t={round(payload.get('t', 0.0), 2)})"
                                )
                            else:
                                logger.debug(
                                    f"{robot_id} Telemetry updated t={round(payload.get('t', 0.0), 2)}"
                                )
                        else:
                            logger.debug(
                                f"{robot_id} Dropped stale/out-of-order frame (session={incoming_session}, t={round(payload.get('t', 0.0), 2)})"
                            )
                    except (json.JSONDecodeError, UnicodeDecodeError) as err:
                        logger.warning(f"Malformed message on topic {message.topic}: {err}")
                    except Exception as e:
                        logger.error(f"Unexpected error processing telemetry frame: {e}")

        except aiomqtt.MqttError as err:
            broker_state.set_connected(False)
            logger.warning(f"MQTT connection dropped: {err}. Reconnecting in {reconnect_interval}")
            await asyncio.sleep(reconnect_interval)
            reconnect_interval = min(reconnect_interval*2, max_reconnect_interval)
        except asyncio.CancelledError:
            broker_state.set_connected(False)
            logger.info("MQTT Shutdown requested")
            break
        except Exception as err:
            broker_state.set_connected(False)
            logger.error(f"Unexpected error in MQTT consumer {err}")
            await asyncio.sleep(reconnect_interval)

if __name__=="__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        logger.info("Consumer stopped manually.")
