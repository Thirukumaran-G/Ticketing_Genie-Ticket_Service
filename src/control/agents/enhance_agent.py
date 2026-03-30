from __future__ import annotations

import asyncio
import os
from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableSerializable
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class EnhancementResult(BaseModel):
    enhanced_text: str = Field(
        description="The enhanced version of the agent's reply",
    )
    changes_summary: str = Field(
        description="One sentence describing what was improved (e.g. 'Clarified tone and fixed grammar')",
        max_length=200,
    )


class EnhanceAgent:
    """
    Enhances a support agent's draft reply.

    Improvements applied:
      - Professional, empathetic tone
      - Grammar and spelling corrections
      - Clearer sentence structure
      - Appropriate formality for B2B support
      - Preserves all technical content and intent
    """

    SYSTEM_PROMPT = """You are an expert support communications editor for a B2B SaaS platform.

Your job is to enhance a support agent's draft reply. Follow these rules strictly:

1. PRESERVE all technical details, steps, links, and factual content exactly.
2. IMPROVE tone — make it warm, professional, and empathetic without being sycophantic.
3. FIX grammar, spelling, and punctuation errors.
4. IMPROVE sentence clarity and flow — break up run-on sentences, remove filler words.
5. DO NOT add new information, promises, or commitments not in the original.
6. DO NOT add generic sign-offs like "Best regards" or "Sincerely" unless already present.
7. KEEP the same language as the original draft (do not translate).
8. KEEP the reply concise — do not pad with unnecessary filler.
9. If the draft is already excellent, return it with only minimal changes.

{mode_context}

Return the enhanced reply and a brief one-sentence summary of changes made."""

    MODE_CONTEXTS = {
        "reply": "This is a customer-facing reply. Be professional and empathetic.",
        "internal": "This is an internal note for teammates. Be direct and concise — no need for customer-facing tone.",
    }

    def __init__(self) -> None:
        self._chain: RunnableSerializable[dict[str, Any], EnhancementResult] | None = None

    def _build_system_prompt(self, mode: str) -> str:
        mode_context = self.MODE_CONTEXTS.get(mode, self.MODE_CONTEXTS["reply"])
        return self.SYSTEM_PROMPT.format(mode_context=mode_context)

    def _get_chain(
        self, mode: str
    ) -> RunnableSerializable[dict[str, Any], EnhancementResult]:
        llm = ChatGroq(
            api_key=SecretStr(settings.GROQ_API_KEY),
            model=settings.GROQ_MODEL,
            temperature=0.3,
            max_tokens=1024,
            stop_sequences=None,
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", self._build_system_prompt(mode)),
            (
                "human",
                "Please enhance the following draft reply:\n\n---\n{draft}\n---",
            ),
        ])
        return prompt | llm.with_structured_output(EnhancementResult)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    async def enhance(
        self,
        draft: str,
        mode: str = "reply",  # "reply" | "internal"
    ) -> EnhancementResult:
        if not draft or not draft.strip():
            return EnhancementResult(
                enhanced_text=draft,
                changes_summary="No changes — draft was empty.",
            )

        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing_enhance", fallback="passthrough")
            return self._passthrough_fallback(draft)

        try:
            chain = self._get_chain(mode)

            def _invoke() -> EnhancementResult:
                result = chain.invoke({"draft": draft})
                if not isinstance(result, EnhancementResult):
                    raise TypeError(f"Unexpected enhancer output: {type(result)}")
                return result

            result: EnhancementResult = await asyncio.to_thread(_invoke)

            logger.info(
                "reply_enhanced",
                mode=mode,
                original_len=len(draft),
                enhanced_len=len(result.enhanced_text),
                changes=result.changes_summary,
            )
            return result

        except Exception as exc:
            logger.error(
                "enhance_agent_failed",
                error=str(exc),
                fallback="passthrough",
            )
            return self._passthrough_fallback(draft)

    def _passthrough_fallback(self, draft: str) -> EnhancementResult:
        """Return the original text unchanged when LLM is unavailable."""
        return EnhancementResult(
            enhanced_text=draft,
            changes_summary="Enhancement unavailable — original text returned.",
        )