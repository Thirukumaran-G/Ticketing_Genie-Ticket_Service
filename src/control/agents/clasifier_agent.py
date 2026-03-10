import asyncio
from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableSerializable
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.settings import settings
from src.observability.logging.logger import get_logger

import os
from dotenv import load_dotenv
load_dotenv(override=True)

os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")

logger = get_logger(__name__)


class ClassificationResult(BaseModel):
    """Strict schema for AI classification output."""

    priority: str = Field(
        description="Ticket priority: low, medium, high, or critical",
        pattern="^(low|medium|high|critical)$",
    )
    severity: str = Field(
        description="Issue severity: low, medium, high, or critical",
        pattern="^(low|medium|high|critical)$",
    )
    reason: str = Field(
        description="Brief justification for the classification (1-2 sentences)",
        max_length=500,
    )
    confidence: float = Field(
        description="Confidence score between 0 and 1",
        ge=0.0,
        le=1.0,
        default=0.8,
    )


class ClassifierAgent:
    SYSTEM_PROMPT = """
    You are an expert support ticket classifier for a B2B SaaS platform.

    Analyze the ticket title and description, then classify:
        - priority: how urgently the issue needs attention (low/medium/high/critical)
        - severity: how severe the impact is on the customer (low/medium/high/critical)

    Priority rules:
        - critical: system down, data loss, security breach, blocking all users
        - high: major feature broken, significant revenue impact, multiple users affected
        - medium: feature degraded, workaround exists, single user or team affected
        - low: minor issue, cosmetic, feature request, question

    Severity rules:
        - critical: complete outage, data corruption/loss
        - high: major functionality broken, no workaround
        - medium: partial functionality affected, workaround exists
        - low: minor inconvenience, cosmetic issue."""

    def __init__(self) -> None:
        self._chain: RunnableSerializable[dict[str, Any], ClassificationResult] | None = None

    def _get_chain(self) -> RunnableSerializable[dict[str, Any], ClassificationResult]:
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,
                temperature=0.1,
                max_tokens=300,
                stop_sequences=None,
            )

            prompt = ChatPromptTemplate.from_messages([
                ("system", self.SYSTEM_PROMPT),
                ("human", "Title: {title}\n\nDescription: {description}\n\nClassify this ticket."),
            ])

            self._chain = prompt | llm.with_structured_output(ClassificationResult)  # type: ignore[assignment]

        return self._chain  # type: ignore[return-value]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def classify(self, title: str, description: str) -> ClassificationResult:
        """Classify ticket — returns ClassificationResult with strict schema validation."""
        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing", fallback="rule_based")
            return self._rule_based_classify(title, description)

        try:
            chain = self._get_chain()

            def _invoke() -> ClassificationResult:
                result = chain.invoke({"title": title, "description": description})
                if not isinstance(result, ClassificationResult):
                    raise TypeError(f"Unexpected classifier output type: {type(result)}")
                return result

            # asyncio.to_thread is safe across all asyncio.run() calls —
            # unlike get_event_loop() which breaks when the loop is closed/recreated
            result: ClassificationResult = await asyncio.to_thread(_invoke)

            logger.info(
                "ticket_classified",
                priority=result.priority,
                severity=result.severity,
                confidence=result.confidence,
            )
            return result

        except Exception as exc:
            logger.error("ai_classification_failed", error=str(exc), fallback="rule_based")
            return self._rule_based_classify(title, description)

    def _rule_based_classify(self, title: str, description: str) -> ClassificationResult:
        """Keyword-based fallback classifier."""
        text = (title + " " + description).lower()

        critical_kw = ["down", "outage", "data loss", "breach", "critical", "emergency", "urgent"]
        high_kw = ["broken", "fail", "error", "not working", "crash", "cannot", "unable"]
        medium_kw = ["slow", "issue", "problem", "wrong", "incorrect", "unexpected"]

        if any(k in text for k in critical_kw):
            return ClassificationResult(
                priority="critical",
                severity="critical",
                reason="Keyword match: critical indicators detected.",
                confidence=0.6,
            )
        if any(k in text for k in high_kw):
            return ClassificationResult(
                priority="high",
                severity="high",
                reason="Keyword match: high severity indicators detected.",
                confidence=0.6,
            )
        if any(k in text for k in medium_kw):
            return ClassificationResult(
                priority="medium",
                severity="medium",
                reason="Keyword match: medium severity indicators detected.",
                confidence=0.6,
            )

        return ClassificationResult(
            priority="low",
            severity="low",
            reason="No severity indicators found — defaulting to low.",
            confidence=0.5,
        )