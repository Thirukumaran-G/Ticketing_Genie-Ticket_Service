from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# Max events buffered per tab before we drop the oldest
_QUEUE_MAX_SIZE = 100


class SSEManager:

    def __init__(self) -> None:
        # actor_id → list of active queues (one per open browser tab/connection)
        self._subscribers: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._lock = asyncio.Lock()

    # ── Subscribe (one connection) ─────────────────────────────────────────────

    @asynccontextmanager
    async def subscribe(self, actor_id: str) -> AsyncIterator[asyncio.Queue]:
        """
        Async context manager — yields a dedicated Queue for this connection.
        Registers on entry, deregisters on exit (even on crash/disconnect).

        Usage in SSE route:
            async with sse_manager.subscribe(actor_id) as q:
                while True:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"event: {payload['event']}\\ndata: ...\\n\\n"
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)

        async with self._lock:
            self._subscribers[actor_id].append(q)
            tab_count = len(self._subscribers[actor_id])

        logger.info(
            "sse_subscriber_connected",
            actor_id=actor_id,
            open_tabs=tab_count,
        )

        try:
            yield q
        finally:
            async with self._lock:
                try:
                    self._subscribers[actor_id].remove(q)
                except ValueError:
                    pass  # already removed — race condition guard
                remaining = len(self._subscribers[actor_id])
                if remaining == 0:
                    del self._subscribers[actor_id]

            logger.info(
                "sse_subscriber_disconnected",
                actor_id=actor_id,
                remaining_tabs=remaining if 'remaining' in dir() else 0,
            )

    # ── Push event to all tabs for one user ───────────────────────────────────

    async def push(self, actor_id: str, payload: dict) -> int:
        """
        Push payload to ALL open queues for actor_id.

        payload format expected by SSE routes:
            { "event": "notification" | "queue_update" | "read_receipt",
              "data":  { ... } }

        Returns number of queues that received the event.
        Dead / full queues are silently skipped and removed.
        """
        async with self._lock:
            queues = list(self._subscribers.get(actor_id, []))

        if not queues:
            logger.debug("sse_push_no_subscribers", actor_id=actor_id)
            return 0

        delivered = 0
        dead: list[asyncio.Queue] = []

        for q in queues:
            try:
                q.put_nowait(payload)
                delivered += 1
            except asyncio.QueueFull:
                # Tab is not consuming — mark for removal
                logger.warning(
                    "sse_queue_full_dropping_tab",
                    actor_id=actor_id,
                )
                dead.append(q)
            except Exception as exc:
                logger.warning(
                    "sse_push_queue_error",
                    actor_id=actor_id,
                    error=str(exc),
                )
                dead.append(q)

        # Remove dead queues
        if dead:
            async with self._lock:
                for dq in dead:
                    try:
                        self._subscribers[actor_id].remove(dq)
                    except ValueError:
                        pass
                if not self._subscribers.get(actor_id):
                    self._subscribers.pop(actor_id, None)

        logger.debug(
            "sse_push_delivered",
            actor_id=actor_id,
            delivered=delivered,
            total_queues=len(queues),
        )
        return delivered

    # publish is an alias — workers imported both names historically
    async def publish(self, actor_id: str, payload: dict) -> int:
        return await self.push(actor_id, payload)

    # ── Convenience: push a notification event ────────────────────────────────

    async def push_notification(
        self,
        actor_id:      str,
        notif_type:    str,
        title:         str,
        message:       str,
        ticket_number: str | None = None,
        ticket_id:     str | None = None,
        extra:         dict | None = None,
    ) -> int:
        """
        Shorthand for pushing a 'notification' event.
        Avoids repeating the payload structure everywhere.
        """
        data: dict = {
            "type":    notif_type,
            "title":   title,
            "message": message,
        }
        if ticket_number:
            data["ticket_number"] = ticket_number
        if ticket_id:
            data["ticket_id"] = ticket_id
        if extra:
            data.update(extra)

        return await self.push(actor_id, {"event": "notification", "data": data})

    # ── Diagnostics ───────────────────────────────────────────────────────────

    @property
    def connected_users(self) -> list[str]:
        return list(self._subscribers.keys())

    def tab_count(self, actor_id: str) -> int:
        return len(self._subscribers.get(actor_id, []))

    def total_connections(self) -> int:
        return sum(len(qs) for qs in self._subscribers.values())


# Process-level singleton
sse_manager = SSEManager()