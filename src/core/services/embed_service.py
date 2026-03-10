"""
EmbedService — generates 384-dim sentence embeddings.

Model: sentence-transformers/all-MiniLM-L6-v2
Dim:   384
Used for:
  - ticket_embedding         on Ticket
  - skill_embedding          on TeamMember
  - agent_skill_embeddings   on Persona
"""

from __future__ import annotations

import asyncio
from functools import lru_cache

from sentence_transformers import SentenceTransformer

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


@lru_cache(maxsize=1)
def _get_model() -> SentenceTransformer:
    """Load model once and cache — shared across all EmbedService instances."""
    logger.info("embed_model_loading", model=_MODEL_NAME)
    model = SentenceTransformer(_MODEL_NAME)
    logger.info("embed_model_ready", model=_MODEL_NAME)
    return model


class EmbedService:
    """Async wrapper around SentenceTransformer.

    encode() is CPU-bound — runs in a thread pool via asyncio.to_thread
    so it never blocks the event loop.
    """

    async def embed(self, text: str) -> list[float]:
        """Embed a single string → 384-dim normalized float list."""
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text.")

        model = _get_model()

        # Run blocking encode in thread pool — keeps FastAPI event loop free
        vector = await asyncio.to_thread(
            model.encode,
            text.strip(),
            normalize_embeddings=True,
        )
        return vector.tolist()

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple strings in one pass — more efficient than looping embed()."""
        if not texts:
            return []

        clean = [t.strip() for t in texts if t and t.strip()]
        if not clean:
            raise ValueError("All texts are empty.")

        model = _get_model()

        vectors = await asyncio.to_thread(
            model.encode,
            clean,
            normalize_embeddings=True,
            batch_size=32,
        )
        return [v.tolist() for v in vectors]