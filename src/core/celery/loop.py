"""
One persistent asyncio event loop per Celery worker process.
All tasks use run_async() instead of asyncio.run().
"""
from __future__ import annotations

import asyncio
import threading

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
    """Drop-in replacement for asyncio.run() — reuses the worker loop."""
    return get_worker_loop().run_until_complete(coro)