"""
SSEManager — manages open SSE connections per user.
src/core/sse/sse_manager.py

Each connected user gets an asyncio.Queue.
Redis subscriber pushes events into the relevant queues.
SSE endpoints drain their queue and stream to the client.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class SSEManager:
    """Singleton — holds {user_id → list[asyncio.Queue]} for open SSE connections."""

    def __init__(self) -> None:
        # user_id (str) → list of queues (one per open browser tab / connection)
        self._connections: dict[str, list[asyncio.Queue]] = defaultdict(list)

    def connect(self, user_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._connections[user_id].append(q)
        logger.info("sse_connected", user_id=user_id, total=len(self._connections[user_id]))
        return q

    def disconnect(self, user_id: str, q: asyncio.Queue) -> None:
        try:
            self._connections[user_id].remove(q)
        except ValueError:
            pass
        if not self._connections[user_id]:
            del self._connections[user_id]
        logger.info("sse_disconnected", user_id=user_id)

    async def push(self, user_id: str, event: str, data: dict) -> None:
        """Push an event to all open connections for user_id."""
        queues = self._connections.get(user_id, [])
        if not queues:
            return
        payload = {"event": event, "data": data}
        for q in queues:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning("sse_queue_full", user_id=user_id)

    async def broadcast(self, user_ids: list[str], event: str, data: dict) -> None:
        for uid in user_ids:
            await self.push(uid, event, data)

    @asynccontextmanager
    async def subscribe(
        self, user_id: str
    ) -> AsyncGenerator[asyncio.Queue, None]:
        """Context manager — auto-connects and disconnects."""
        q = self.connect(user_id)
        try:
            yield q
        finally:
            self.disconnect(user_id, q)

    async def event_stream(
        self, user_id: str, q: asyncio.Queue
    ) -> AsyncGenerator[str, None]:
        """Drain queue and yield SSE-formatted strings."""
        try:
            while True:
                payload = await asyncio.wait_for(q.get(), timeout=30)
                event   = payload["event"]
                data    = json.dumps(payload["data"])
                yield f"event: {event}\ndata: {data}\n\n"
        except asyncio.TimeoutError:
            # Keepalive ping — prevents proxy from closing idle connections
            yield ": keepalive\n\n"
        except asyncio.CancelledError:
            return


# ── Global singleton ──────────────────────────────────────────────────────────
sse_manager = SSEManager()