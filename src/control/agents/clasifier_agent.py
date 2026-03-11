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
    """LLM returns ONLY severity + reason. Priority is derived from DB table."""

    severity: str = Field(
        description="Issue severity: low, medium, high, or critical",
        pattern="^(low|medium|high|critical)$",
    )
    reason: str = Field(
        description="Brief justification for the severity (1-2 sentences, max 300 chars)",
        max_length=300,
    )


class ClassifierAgent:
    SYSTEM_PROMPT = """You are an expert support ticket classifier for a B2B SaaS platform.

Analyze the ticket title and description, then determine ONLY the severity of the issue.

Severity rules:
  - critical: complete outage, data loss, data corruption, security breach, system down
  - high:     major feature broken, no workaround available, significant user impact
  - medium:   partial functionality affected, workaround exists, limited user impact
  - low:      minor issue, cosmetic problem, feature request, general question

Return ONLY severity and a brief reason. Do NOT determine priority — that is handled by the system."""

    def __init__(self) -> None:
        self._chain: RunnableSerializable[dict[str, Any], ClassificationResult] | None = None

    def _get_chain(self) -> RunnableSerializable[dict[str, Any], ClassificationResult]:
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,
                temperature=0.1,
                max_tokens=200,
                stop_sequences=None,
            )
            prompt = ChatPromptTemplate.from_messages([
                ("system", self.SYSTEM_PROMPT),
                ("human", "Title: {title}\n\nDescription: {description}\n\nClassify the severity."),
            ])
            self._chain = prompt | llm.with_structured_output(ClassificationResult)
        return self._chain

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def classify(self, title: str, description: str) -> ClassificationResult:
        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing", fallback="rule_based")
            return self._rule_based_classify(title, description)

        try:
            chain = self._get_chain()

            def _invoke() -> ClassificationResult:
                result = chain.invoke({"title": title, "description": description})
                if not isinstance(result, ClassificationResult):
                    raise TypeError(f"Unexpected classifier output: {type(result)}")
                return result

            result: ClassificationResult = await asyncio.to_thread(_invoke)
            logger.info("ticket_classified", severity=result.severity)
            return result

        except Exception as exc:
            logger.error("ai_classification_failed", error=str(exc), fallback="rule_based")
            return self._rule_based_classify(title, description)

    def _rule_based_classify(self, title: str, description: str) -> ClassificationResult:
        text = (title + " " + description).lower()
        critical_kw = ["down", "outage", "data loss", "breach", "critical", "emergency"]
        high_kw     = ["broken", "fail", "error", "not working", "crash", "cannot", "unable"]
        medium_kw   = ["slow", "issue", "problem", "wrong", "incorrect", "unexpected"]

        if any(k in text for k in critical_kw):
            return ClassificationResult(severity="critical", reason="Critical indicators detected in ticket.")
        if any(k in text for k in high_kw):
            return ClassificationResult(severity="high", reason="High severity indicators detected.")
        if any(k in text for k in medium_kw):
            return ClassificationResult(severity="medium", reason="Medium severity indicators detected.")
        return ClassificationResult(severity="low", reason="No severity indicators found — defaulting to low.")