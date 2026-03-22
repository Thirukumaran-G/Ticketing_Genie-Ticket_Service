import asyncio
import os
from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from typing import Any
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableSerializable
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class ClassificationResult(BaseModel):
    severity: str = Field(
        description="Issue severity: low, medium, high, or critical",
        pattern="^(low|medium|high|critical)$",
    )
    reason: str = Field(
        description="Brief justification for the severity (1-2 sentences, max 300 chars)",
        max_length=300,
    )


_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


class ClassifierAgent:
    BASE_SYSTEM_PROMPT = """You are an expert support ticket classifier for a B2B SaaS platform.

Analyze the ticket title and description, then determine ONLY the severity of the issue.

Severity rules:
  - critical: complete outage, data loss, data corruption, security breach, system down
  - high:     major feature broken, no workaround available, significant user impact
  - medium:   partial functionality affected, workaround exists, limited user impact
  - low:      minor issue, cosmetic problem, feature request, general question

{product_context}

Return ONLY severity and a brief reason. Do NOT determine priority — that is handled by the system."""

    def __init__(self) -> None:
        self._chain: RunnableSerializable[dict[str, Any], ClassificationResult] | None = None

    def _build_system_prompt(self, product_description: str | None) -> str:
        if product_description:
            product_context = (
                f"Product context (use this to calibrate severity):\n"
                f"{product_description}\n\n"
                f"Consider the nature and criticality of this product when assessing "
                f"the impact of the reported issue."
            )
        else:
            product_context = ""
        return self.BASE_SYSTEM_PROMPT.format(product_context=product_context)

    def _get_chain(
        self, product_description: str | None
    ) -> RunnableSerializable[dict[str, Any], ClassificationResult]:
        llm = ChatGroq(
            api_key=SecretStr(settings.GROQ_API_KEY),
            model=settings.GROQ_MODEL,
            temperature=0.1,
            max_tokens=200,
            stop_sequences=None,
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", self._build_system_prompt(product_description)),
            ("human", "Title: {title}\n\nDescription: {description}\n\nClassify the severity."),
        ])
        return prompt | llm.with_structured_output(ClassificationResult)

    # classifier agent with retry logic 
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def classify(
        self,
        title:               str,
        description:         str,
        product_description: str | None = None,
        product_id:          str | None = None,
        session=None,
    ) -> ClassificationResult:
        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing", fallback="keyword_rule")
            return await self._keyword_fallback(title, description, product_id, session)

        try:
            chain = self._get_chain(product_description)

            # Wraps the blocking chain.invoke() in a closure
            def _invoke() -> ClassificationResult:
                result = chain.invoke({"title": title, "description": description})
                if not isinstance(result, ClassificationResult):
                    raise TypeError(f"Unexpected classifier output: {type(result)}")
                return result
            
            # Runs _invoke() in a thread — non-blocking from event loop's perspective
            result: ClassificationResult = await asyncio.to_thread(_invoke)
            logger.info("ticket_classified_llm", severity=result.severity)
            return result

        except Exception as exc:
            logger.error(
                "ai_classification_failed",
                error=str(exc),
                fallback="keyword_rule",
            )
            return await self._keyword_fallback(title, description, product_id, session)

    async def _keyword_fallback(
        self,
        title:       str,
        description: str,
        product_id:  str | None,
        session,
    ) -> ClassificationResult:
        text_blob = (title + " " + description).lower()

        if session is None or product_id is None:
            logger.warning(
                "keyword_fallback_no_session_or_product",
                product_id=product_id,
                has_session=session is not None,
            )
            return ClassificationResult(
                severity="low",
                reason="LLM unavailable and no product context for keyword matching — defaulting to low.",
            )

        try:
            from sqlalchemy import select, text as sa_text
            import uuid as _uuid

            result = await session.execute(
                sa_text(
                    """
                    SELECT keyword, severity
                    FROM ticket.keyword_rule
                    WHERE product_id = :pid
                      AND is_active = TRUE
                    """
                ),
                {"pid": _uuid.UUID(product_id)},
            )
            rows = result.fetchall()

            if not rows:
                logger.warning(
                    "keyword_fallback_no_rules_found",
                    product_id=product_id,
                )
                return ClassificationResult(
                    severity="low",
                    reason="LLM unavailable and no keyword rules configured for this product — defaulting to low.",
                )

            matched_severity: str | None = None

            for row in rows:
                keyword  = row.keyword.lower()
                severity = row.severity.lower()

                if keyword in text_blob:
                    if matched_severity is None:
                        matched_severity = severity
                    else:
                        # keep highest severity among all matches
                        if _SEVERITY_RANK.get(severity, 0) > _SEVERITY_RANK.get(matched_severity, 0):
                            matched_severity = severity

            if matched_severity:
                logger.info(
                    "keyword_fallback_matched",
                    severity=matched_severity,
                    product_id=product_id,
                )
                return ClassificationResult(
                    severity=matched_severity,
                    reason=f"Severity determined via keyword match (LLM unavailable).",
                )

            logger.info("keyword_fallback_no_match", product_id=product_id)
            return ClassificationResult(
                severity="low",
                reason="LLM unavailable, no keywords matched in ticket text — defaulting to low.",
            )

        except Exception as exc:
            logger.error("keyword_fallback_error", product_id=product_id, error=str(exc))
            return ClassificationResult(
                severity="low",
                reason="LLM unavailable and keyword lookup failed — defaulting to low.",
            )