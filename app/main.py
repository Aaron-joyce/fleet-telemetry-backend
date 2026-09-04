import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from app.consumer import start_consumer
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from app.state import fleet_state

logger = logging.getLogger("fleet_api")
logging.basicConfig(level=logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting background MQTT consumer task...")
    consumer_task = asyncio.create_task(start_consumer())

    yield

    logger.info("Shutting Down MQTT consumer task...")
    consumer_task.cancel()

    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("MQTT consumer stopped cleanly")

app = FastAPI(
    title="Fleet Telemetry Backend",
    description="Real-time telemetry ingestion and streaming service for robot fleet",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/robots")
async def get_all_robots()-> list[dict[str, Any]]:
    return await fleet_state.get_all_robots()

@app.get("/robots/{robot_id}")
async def get_robot(robot_id) -> dict[str, Any]:
    robot = await fleet_state.get_robot(robot_id)
    if not robot:
        raise HTTPException(
            status_code=404,
            detail=f"Robot with id {robot_id} not found."
        )
    return robot

@app.websocket("/ws/robots")
async def websocket_fleet_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("Client Connected to /ws/robots")

    try:
        while True:
            snapshot = await fleet_state.get_all_robots()
            await websocket.send_json(snapshot)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info("Client disconnected from /ws/robots.")
    except Exception as e:
        logger.warning(f"WebSocket client connection closed with error: {e}")
