from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class EmailExtractionResult(BaseModel):
    title: str = Field(default="")
    description: str = Field(default="")
    product_name: str | None = Field(default=None)
    severity: str | None = Field(default=None)
    environment: str | None = Field(default=None)

class _LLMExtraction(BaseModel):
    title: str = Field(
        description="The ticket title from the email subject. Clean and concise. Empty string if not determinable.",
        default="",
    )
    description: str = Field(
        description="Full issue description from the email body. Preserve all technical detail. Empty string if not determinable.",
        default="",
    )
    product_name_raw: str | None = Field(
        description=(
            "Copy the EXACT words the sender used to name the product, system, or service "
            "they are reporting an issue with — ONLY if they named it explicitly by name. "
            "Examples of EXPLICIT naming: 'Grocenow', 'PayFlow', 'DataSync Pro'. "
            "Examples that are NOT explicit names — return null for these: "
            "'our platform', 'the app', 'the system', 'our service', 'the website', "
            "'our tool', 'the portal', 'our product'. "
            "If the sender did not write an explicit product name, return null."
        ),
        default=None,
    )
    severity: str | None = Field(
        description=(
            "Issue severity: exactly one of: critical, high, medium, low. "
            "Infer from content — critical (outage/data loss), high (major feature broken), "
            "medium (partial issue, workaround exists), low (minor/question). "
            "Return null if unclear."
        ),
        default=None,
    )
    environment: str | None = Field(
        description=(
            "Return exactly one of: prod, stage, dev. "
            "Map 'production' -> prod, 'staging' -> stage, 'development' -> dev. "
            "Return null if not mentioned anywhere in the email."
        ),
        default=None,
    )


class EmailExtractionAgent:
    SYSTEM_PROMPT = """You are an expert support ticket intake specialist.

Your job is to extract structured information from an inbound support email.

Extraction rules:
- title            : extract from the email subject; clean and concise
- description      : extract from the email body; preserve all technical detail
- product_name_raw : copy the EXACT words the sender used to name their product.
                     ONLY set this if the sender wrote an explicit product or service name.
                     Vague references like "our platform", "the app", "the system",
                     "our service", "the website", "our tool" are NOT product names — return null.
                     If the sender did not name the product explicitly, return null.
- severity         : infer from content; null if unclear
- environment      : prod / stage / dev only; null if not mentioned

Return ONLY the structured fields. No commentary."""

    def _build_chain(self):
        llm = ChatGroq(
            api_key=SecretStr(settings.GROQ_API_KEY),
            model=settings.GROQ_MODEL,
            temperature=0.0,
            max_tokens=400,
            stop_sequences=None,
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", self.SYSTEM_PROMPT),
            ("human", "Subject: {subject}\n\nBody:\n{body}\n\nExtract the ticket fields."),
        ])
        return prompt | llm.with_structured_output(_LLMExtraction)

    @staticmethod
    def _match_product(raw: str | None, valid_products: list[str]) -> str | None:
        """
        Deterministic Python matching — the LLM never sees the product list,
        so it cannot hallucinate from it.

        Match order:
          1. Exact match (case-insensitive)
          2. raw is a substring of a product name  e.g. "groce" inside "Grocenow"
          3. product name is a substring of raw    e.g. "Grocenow" inside "Grocenow platform"
          4. No match -> None
        """
        if not raw:
            return None

        raw_lower = raw.lower().strip()

        for p in valid_products:
            if p.lower().strip() == raw_lower:
                return p

        for p in valid_products:
            if raw_lower in p.lower():
                return p

        for p in valid_products:
            if p.lower() in raw_lower:
                return p

        logger.info("email_extraction_product_no_match", raw=raw)
        return None

    async def extract(
        self,
        subject: str,
        body: str,
        valid_products: list[str],
    ) -> EmailExtractionResult:
        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing_email_extraction")
            return EmailExtractionResult(
                title=subject.strip(),
                description=body.strip(),
                product_name=None,
                severity=None,
                environment=None,
            )

        try:
            chain = self._build_chain()

            def _invoke() -> _LLMExtraction:
                result = chain.invoke({"subject": subject, "body": body})
                if not isinstance(result, _LLMExtraction):
                    raise TypeError(f"Unexpected extraction output: {type(result)}")
                return result

            llm_result: _LLMExtraction = await asyncio.to_thread(_invoke)

            matched_product = self._match_product(llm_result.product_name_raw, valid_products)

            result = EmailExtractionResult(
                title=llm_result.title,
                description=llm_result.description,
                product_name=matched_product,
                severity=llm_result.severity,
                environment=llm_result.environment,
            )

            logger.info(
                "email_extraction_ok",
                title=result.title[:60] if result.title else "",
                product_name_raw=llm_result.product_name_raw,
                product_name=result.product_name,       
                severity=result.severity,
                environment=result.environment,
            )
            return result

        except Exception as exc:
            logger.error("email_extraction_failed", error=str(exc))

            return EmailExtractionResult(
                title=subject.strip(),
                description=body.strip(),
                product_name=None,
                severity=None,
                environment=None,
            )