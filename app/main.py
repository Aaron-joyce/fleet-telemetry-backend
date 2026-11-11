import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from json import dumps
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from app.broker_state import broker_state
from app.consumer import start_consumer
from app.state import fleet_state
from app.websocket import manager

logger = logging.getLogger("fleet_api")
logging.basicConfig(level=logging.INFO)

RECONNECT_GRACE_SECONDS = 10.0  # pause stale-marking after broker reconnect
OFFLINE_EVICTION_TTL = float(os.getenv("OFFLINE_EVICTION_TTL_SECONDS", 300.0))


async def stale_robot_watcher():
    """Periodically sweeps robots and flags them as offline if silent."""
    while True:
        try:
            if not broker_state.is_connected:
                logger.debug(
                    "Broker disconnected — skipping stale check to avoid false positives."
                )
                await asyncio.sleep(1.0)
                continue

            # Grace period: don't mark stale immediately after a reconnect
            reconnected_at = broker_state.reconnected_at
            if (
                reconnected_at
                and (time.monotonic() - reconnected_at) < RECONNECT_GRACE_SECONDS
            ):
                logger.debug(
                    "Within reconnect grace period — skipping stale check."
                )
                await asyncio.sleep(1.0)
                continue

            offline_robots, evicted_robots = await fleet_state.check_and_mark_stale(
                timeout_seconds=5.0,
                eviction_ttl_seconds=OFFLINE_EVICTION_TTL,
            )
            if offline_robots:
                logger.info(
                    "Robots marked offline (no heartbeat): %s", offline_robots
                )
            if evicted_robots:
                logger.info(
                    "Robots evicted from cache (TTL expired): %s",
                    evicted_robots,
                )
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error in stale robot watcher: %s", e)
            await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_task = asyncio.create_task(start_consumer())
    watcher_task = asyncio.create_task(stale_robot_watcher())
    broadcast_task = asyncio.create_task(fleet_broadcast_worker())
    logger.info("Background tasks initiated.")

    yield

    logger.info("Canceling background tasks...")
    consumer_task.cancel()
    watcher_task.cancel()
    broadcast_task.cancel()
    await asyncio.gather(consumer_task, watcher_task, broadcast_task, return_exceptions=True)
    logger.info("Background tasks cleanly terminated.")

# Fetch Fleet State Periodically and broadcast it to all active WebSocket Clients
async def fleet_broadcast_worker():
    while True:
        try:
            # Only serialize and send if at least one client is connected
            if manager.active_connections:
                robots = await fleet_state.get_all_robots()
                payload = dumps(robots)
                await manager.broadcast(payload)
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error in fleet broadcast worker: %s", e)
            await asyncio.sleep(0.5)

app = FastAPI(
    title="Fleet Telemetry Backend",
    description="Real-time telemetry ingestion and streaming service for robot fleets.",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "broker_connected": broker_state.is_connected,
    }


@app.get("/robots")
async def get_all_robots() -> list[dict[str, Any]]:
    return await fleet_state.get_all_robots()


@app.get("/robots/{robot_id}")
async def get_robot(robot_id: str) -> dict[str, Any]:
    robot = await fleet_state.get_robot(robot_id)
    if not robot:
        raise HTTPException(
            status_code=404, detail=f"Robot with id '{robot_id}' not found."
        )
    return robot


@app.websocket("/ws/robots")
async def websocket_fleet_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info("Client connected to /ws/robots")

    try:
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        await manager.disconnect(websocket)
        logger.info("Client disconnected from WebSocket")
