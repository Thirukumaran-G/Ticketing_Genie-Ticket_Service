"""
Redis pub/sub subscriber — background task started on FastAPI lifespan.
src/core/sse/redis_subscriber.py

Channels:
  queue:<user_id>     → pushed when a ticket is assigned to an agent
  notify:<user_id>    → pushed when a notification row is created for a user

Celery workers publish to these channels after DB commit.
This subscriber picks them up and routes into SSEManager queues.
"""

from __future__ import annotations

import asyncio
import json

import redis.asyncio as aioredis

from src.config.settings import settings
from src.core.sse.sse_manager import sse_manager
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# ── Channel patterns ──────────────────────────────────────────────────────────
QUEUE_CHANNEL_PREFIX  = "queue:"     # queue:<agent_id>
NOTIFY_CHANNEL_PREFIX = "notify:"    # notify:<user_id>


async def redis_subscriber_loop() -> None:
    """
    Subscribes to Redis pub/sub and pushes events into SSEManager.
    Runs as a background asyncio task — started in FastAPI lifespan.
    Reconnects automatically on disconnect.
    """
    while True:
        try:
            client = aioredis.from_url(
                settings.CELERY_BROKER_URL,
                decode_responses=True,
            )
            pubsub = client.pubsub()

            # Subscribe to all queue and notify channels via pattern
            await pubsub.psubscribe(
                f"{QUEUE_CHANNEL_PREFIX}*",
                f"{NOTIFY_CHANNEL_PREFIX}*",
            )
            logger.info("redis_subscriber_started")

            async for message in pubsub.listen():
                if message["type"] not in ("pmessage", "message"):
                    continue
                try:
                    channel: str = message.get("channel") or ""
                    raw:     str = message.get("data") or "{}"
                    payload: dict = json.loads(raw)

                    if channel.startswith(QUEUE_CHANNEL_PREFIX):
                        user_id = channel.removeprefix(QUEUE_CHANNEL_PREFIX)
                        await sse_manager.push(user_id, "queue_update", payload)

                    elif channel.startswith(NOTIFY_CHANNEL_PREFIX):
                        user_id = channel.removeprefix(NOTIFY_CHANNEL_PREFIX)
                        await sse_manager.push(user_id, "notification", payload)

                except Exception as exc:
                    logger.error("redis_subscriber_message_error", error=str(exc))

        except Exception as exc:
            logger.error("redis_subscriber_connection_error", error=str(exc))
            await asyncio.sleep(5)   # backoff before reconnect


# ── Publisher helpers (called from Celery workers after commit) ───────────────

def publish_queue_update(redis_url: str, agent_id: str, ticket_data: dict) -> None:
    """Synchronous publish from Celery worker — uses sync redis client."""
    import redis as sync_redis
    import json

    r = sync_redis.from_url(redis_url)
    channel = f"{QUEUE_CHANNEL_PREFIX}{agent_id}"
    r.publish(channel, json.dumps(ticket_data))
    r.close()


def publish_notification(redis_url: str, user_id: str, notif_data: dict) -> None:
    """Synchronous publish from Celery worker — uses sync redis client."""
    import redis as sync_redis
    import json

    r = sync_redis.from_url(redis_url)
    channel = f"{NOTIFY_CHANNEL_PREFIX}{user_id}"
    r.publish(channel, json.dumps(notif_data))
    r.close()