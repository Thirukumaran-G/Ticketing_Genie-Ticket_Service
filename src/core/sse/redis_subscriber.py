# reddis subscriber.py

from __future__ import annotations

import asyncio
import json
from typing import Any

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_NOTIFICATION_CHANNEL_PREFIX = "sse:notifications:"
_INTERNAL_NOTE_CHANNEL_PREFIX = "sse:internal_notes:"


# ── Celery-side publish helpers (called from workers) ─────────────────────────

def _sync_publish(broker_url: str, channel: str, payload: dict) -> None:
    """
    Synchronous Redis publish — safe to call from Celery tasks.
    Creates a short-lived connection per call (tasks are infrequent enough).
    """
    try:
        import redis as sync_redis
        client = sync_redis.from_url(broker_url, decode_responses=True)
        client.publish(channel, json.dumps(payload))
        client.close()
    except Exception as exc:
        logger.warning(
            "redis_publish_failed",
            channel=channel,
            error=str(exc),
        )


def publish_notification(
    broker_url: str,
    actor_id:   str,
    data:       dict,
) -> None:
    """Push a notification event to a specific user."""
    _sync_publish(
        broker_url,
        f"{_NOTIFICATION_CHANNEL_PREFIX}{actor_id}",
        {"event": "notification", "data": data},
    )


def publish_internal_note(
    broker_url: str,
    actor_id:   str,
    data:       dict,
) -> None:
    """Push an internal_note event to a specific user."""
    _sync_publish(
        broker_url,
        f"{_INTERNAL_NOTE_CHANNEL_PREFIX}{actor_id}",
        {"event": "internal_note", "data": data},
    )


# ── FastAPI-side listener (runs as asyncio background task) ───────────────────

async def _redis_listener(broker_url: str) -> None:
    """
    Subscribe to all SSE channels using Redis pub/sub pattern matching.
    Fans out received messages to connected SSE clients via sse_manager.

    Runs indefinitely — reconnects on transient Redis errors.
    """
    import redis.asyncio as aioredis
    from src.core.sse.sse_manager import sse_manager

    PATTERNS = [
        f"{_NOTIFICATION_CHANNEL_PREFIX}*",
        f"{_INTERNAL_NOTE_CHANNEL_PREFIX}*",
    ]

    while True:
        try:
            client = aioredis.from_url(
                broker_url,
                encoding="utf-8",
                decode_responses=True,
            )
            pubsub = client.pubsub()
            await pubsub.psubscribe(*PATTERNS)

            logger.info(
                "redis_sse_listener_started",
                patterns=PATTERNS,
            )

            async for message in pubsub.listen():
                if message["type"] not in ("pmessage", "message"):
                    continue

                channel: str = message.get("channel", "")
                raw:     str = message.get("data", "{}")

                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(
                        "redis_listener_bad_json",
                        channel=channel,
                        raw=raw[:200],
                    )
                    continue

                # Extract actor_id from channel name
                actor_id = _extract_actor_id(channel)
                if not actor_id:
                    continue

                await sse_manager.push(actor_id, payload)

        except asyncio.CancelledError:
            logger.info("redis_sse_listener_cancelled")
            break
        except Exception as exc:
            logger.error(
                "redis_sse_listener_error",
                error=str(exc),
            )
            await asyncio.sleep(5)  # back-off before reconnect


def _extract_actor_id(channel: str) -> str | None:
    """
    Extract actor_id from channel name.
    e.g. "sse:notifications:abc-123" → "abc-123"
    """
    for prefix in (
        _NOTIFICATION_CHANNEL_PREFIX,
        _INTERNAL_NOTE_CHANNEL_PREFIX,
    ):
        if channel.startswith(prefix):
            return channel[len(prefix):]
    return None


def start_redis_listener(app: Any) -> None:
    from src.config.settings import settings

    async def _start():
        task = asyncio.create_task(
            _redis_listener(settings.CELERY_BROKER_URL),
            name="redis_sse_listener",
        )
        app.state.redis_listener_task = task
        logger.info("redis_sse_listener_task_created")

    app.add_event_handler("startup", _start)

    async def _stop():
        task = getattr(app.state, "redis_listener_task", None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("redis_sse_listener_task_stopped")

    app.add_event_handler("shutdown", _stop)