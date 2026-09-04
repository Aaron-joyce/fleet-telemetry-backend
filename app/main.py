import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from app.consumer import start_consumer
from app.state import fleet_state

logger = logging.getLogger("fleet_api")
logging.basicConfig(level=logging.INFO)


async def stale_robot_watcher():
    """Periodically sweeps robots and flags them as stale if silent."""
    while True:
        try:
            stale_robots = await fleet_state.check_and_mark_stale(
                timeout_seconds=5.0
            )
            if stale_robots:
                logger.info(
                    "Robots marked stale (no heartbeat): %s", stale_robots
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
    logger.info("Background tasks initiated.")

    yield

    logger.info("Canceling background tasks...")
    consumer_task.cancel()
    watcher_task.cancel()
    await asyncio.gather(consumer_task, watcher_task, return_exceptions=True)
    logger.info("Background tasks cleanly terminated.")


app = FastAPI(
    title="Fleet Telemetry Backend",
    description="Real-time telemetry ingestion and streaming service for robot fleets.",
    version="0.1.0",
    lifespan=lifespan,
)


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
    await websocket.accept()
    logger.info("Client connected to /ws/robots")

    try:
        while True:
            snapshot = await fleet_state.get_all_robots()
            await websocket.send_json(snapshot)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info("Client disconnected from /ws/robots.")
    except Exception as e:
        logger.warning(
            "WebSocket client connection closed with exception: %s", e
        )
