import asyncio
from fastapi import WebSocket


class ConnectionManager:

  def __init__(self) -> None:
    self.active_connections: set[WebSocket] = set()
    self._lock = asyncio.Lock()

  async def connect(self, websocket: WebSocket):
    await websocket.accept()
    async with self._lock:
      self.active_connections.add(websocket)

  async def disconnect(self, websocket: WebSocket):
    async with self._lock:
      self.active_connections.discard(websocket)

  async def broadcast(self, message: str):
    # Snapshot clients and release the lock immediately
    async with self._lock:
      clients = list(self.active_connections)

    if not clients:
      return

    async def _send(ws: WebSocket):
      try:
        await asyncio.wait_for(ws.send_text(message), timeout=1.0)
      except Exception:
        await self.disconnect(ws)

    await asyncio.gather(*[_send(ws) for ws in clients], return_exceptions=True)


manager = ConnectionManager()
