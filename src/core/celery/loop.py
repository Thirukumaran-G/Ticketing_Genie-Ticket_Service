from __future__ import annotations

import asyncio
import threading

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_loop: asyncio.AbstractEventLoop | None = None
_lock = threading.Lock()


def get_worker_loop() -> asyncio.AbstractEventLoop:
    global _loop
    if _loop is None or _loop.is_closed():
        with _lock:
            if _loop is None or _loop.is_closed():
                _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
    return _loop


def run_async(coro):
    """
    Drop-in replacement for asyncio.run() — reuses the worker loop.
    All exceptions are logged with full traceback before re-raising
    so nothing is ever silently swallowed.
    """
    try:
        return get_worker_loop().run_until_complete(coro)
    except Exception as exc:
        logger.error(
            "run_async_unhandled_exception",
            error=str(exc),
            exc_info=True,
        )
        raise