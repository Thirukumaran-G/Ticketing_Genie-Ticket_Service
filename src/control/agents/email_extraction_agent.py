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
    title: str = Field(
        description=(
            "The ticket title extracted from the email subject. "
            "Clean and concise. Empty string if not determinable."
        ),
        default="",
    )
    description: str = Field(
        description=(
            "The full issue description extracted from the email body. "
            "Preserve all relevant technical detail. Empty string if not determinable."
        ),
        default="",
    )
    product_name: str | None = Field(
        description=(
            "The product name the issue relates to. "
            "MUST exactly match one of the valid product names provided in the prompt. "
            "Return null if no valid product can be identified from the email."
        ),
        default=None,
    )
    severity: str | None = Field(
        description=(
            "Issue severity: must be exactly one of: critical, high, medium, low. "
            "Infer from the email content. Return null if not determinable."
        ),
        default=None,
    )
    environment: str | None = Field(
        description=(
            "Deployment environment. "
            # ── FIX Bug 4: corrected mapping so LLM returns exact DB values ──
            "Return exactly one of: prod, stage, dev. "
            "Map 'production' -> prod, 'staging' -> stage, 'development' -> dev. "
            "Return null if not mentioned."
        ),
        default=None,
    )


class EmailExtractionAgent:
    BASE_SYSTEM_PROMPT = """You are an expert support ticket intake specialist.

Your job is to extract structured information from an inbound support email.

Valid products (you MUST match product_name exactly to one of these, case-insensitive comparison will be done after):
{product_list}

Extraction rules:
- title        : extract from the email subject; clean and concise
- description  : extract from the email body; preserve all technical detail
- product_name : must match one of the valid products above EXACTLY (same spelling); null if none match
- severity     : infer from content — critical (outage/data loss), high (major feature broken),
                 medium (partial issue with workaround), low (minor/question); null if unclear
- environment  : return exactly one of: prod, stage, dev
                 Map 'production' -> prod, 'staging' -> stage, 'development' -> dev
                 Return null if environment is not mentioned anywhere in the email

Return ONLY the structured fields. Do not add commentary."""

    def _build_chain(self, product_list: list[str]):
        llm = ChatGroq(
            api_key=SecretStr(settings.GROQ_API_KEY),
            model=settings.GROQ_MODEL,
            temperature=0.0,
            max_tokens=400,
            stop_sequences=None,
        )
        product_str = "\n".join(f"  - {p}" for p in product_list)
        system = self.BASE_SYSTEM_PROMPT.format(product_list=product_str)
        prompt = ChatPromptTemplate.from_messages([
            ("system", system),
            (
                "human",
                "Subject: {subject}\n\nBody:\n{body}\n\nExtract the ticket fields.",
            ),
        ])
        return prompt | llm.with_structured_output(EmailExtractionResult)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
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
            chain = self._build_chain(valid_products)

            def _invoke() -> EmailExtractionResult:
                result = chain.invoke({"subject": subject, "body": body})
                if not isinstance(result, EmailExtractionResult):
                    raise TypeError(f"Unexpected extraction output: {type(result)}")
                return result

            result: EmailExtractionResult = await asyncio.to_thread(_invoke)
            logger.info(
                "email_extraction_ok",
                title=result.title[:60] if result.title else "",
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