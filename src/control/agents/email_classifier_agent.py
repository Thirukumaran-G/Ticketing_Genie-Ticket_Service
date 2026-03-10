"""Email classifier agent — decides if inbound email is support-related."""

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from groq import APIConnectionError, APIStatusError, AuthenticationError, RateLimitError
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr

from src.config.settings import settings
from src.core.exceptions.base import (
    LLMAPIStatusException,
    LLMAuthenticationException,
    LLMConnectionException,
    LLMException,
    LLMRateLimitException,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class EmailClassificationResult(BaseModel):
    """Structured output for email triage."""

    is_support_request: bool = Field(
        description="True if this email is a genuine support/help request"
    )
    is_thread_reply: bool = Field(
        description="True if this email is a reply to an existing ticket thread"
    )
    extracted_ticket_number: str | None = Field(
        default=None,
        description="Ticket number extracted from subject/body e.g. TKT-ABC123, or null",
    )
    suggested_title: str = Field(
        description="Clean, concise ticket title derived from the email subject/body",
        max_length=500,
    )
    suggested_description: str = Field(
        description="Cleaned body text suitable for ticket description",
    )
    confidence: float = Field(
        description="Confidence score 0.0–1.0",
        ge=0.0,
        le=1.0,
        default=0.8,
    )
    rejection_reason: str | None = Field(
        default=None,
        description="Why this email was rejected (spam, OOO, newsletter, etc.) if not support",
    )


class EmailClassifierAgent:
    """
    Analyses raw inbound email and decides:
      1. Is it a support request?
      2. Is it a thread reply (should be appended to existing ticket)?
      3. Extract/clean title + description for ticket creation.
    """

    SYSTEM_PROMPT = """You are an intelligent email triage agent for a B2B SaaS support system.

Your job is to analyse an inbound email and determine:
1. Whether it is a genuine support/help request (not spam, OOO, newsletter, marketing etc.)
2. Whether it is a reply to an existing ticket thread (look for ticket numbers like TKT-XXXXXX in subject/body)
3. Extract a clean title and description suitable for creating or updating a support ticket.

Rules:
- Mark is_support_request=false for: auto-replies, out-of-office, newsletters, marketing, spam, receipts, notifications from other systems.
- Mark is_thread_reply=true if the subject contains "Re:" and/or there is a ticket number pattern in the email.
- extracted_ticket_number should be the ticket number string (e.g. "TKT-AB1234") if found, else null.
- suggested_title: derive from subject, strip "Re:", "Fwd:", ticket refs. Max 500 chars.
- suggested_description: the meaningful body text, strip signatures, quoted replies, disclaimers.
- Be strict: when in doubt about is_support_request, prefer false to avoid noise tickets.
"""

    def __init__(self) -> None:
        self._chain: Any = None

    @contextmanager
    def _groq_error_handler(self) -> Generator[None, None, None]:
        try:
            yield
        except RateLimitError as exc:
            raise LLMRateLimitException(retry_after=getattr(exc, "retry_after", None)) from exc
        except AuthenticationError as exc:
            raise LLMAuthenticationException() from exc
        except APIConnectionError as exc:
            raise LLMConnectionException() from exc
        except APIStatusError as exc:
            raise LLMAPIStatusException(api_status_code=exc.status_code) from exc
        except Exception as exc:
            raise LLMException() from exc

    def _get_chain(self) -> Any:
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,
                temperature=0.0,
                max_tokens=600,
                stop_sequences=None,
            )
            prompt = ChatPromptTemplate.from_messages([
                ("system", self.SYSTEM_PROMPT),
                (
                    "human",
                    "From: {from_email}\nSubject: {subject}\n\nBody:\n{body}",
                ),
            ])
            self._chain = prompt | llm.with_structured_output(EmailClassificationResult)
        return self._chain

    async def classify(
        self,
        from_email: str,
        subject: str,
        body: str,
    ) -> EmailClassificationResult:
        """Classify email — async wrapper around sync LangChain chain."""

        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing — falling back to rule-based email classifier")
            return self._rule_based_classify(from_email, subject, body)

        try:
            chain = self._get_chain()

            def _invoke() -> EmailClassificationResult:
                with self._groq_error_handler():
                    result = chain.invoke(
                        {"from_email": from_email, "subject": subject, "body": body}
                    )
                    if not isinstance(result, EmailClassificationResult):
                        raise TypeError(f"Unexpected type: {type(result)}")
                    return result

            result: EmailClassificationResult = await asyncio.to_thread(_invoke)

            logger.info(
                "email_classified",
                from_email=from_email,
                is_support=result.is_support_request,
                is_reply=result.is_thread_reply,
                ticket_ref=result.extracted_ticket_number,
                confidence=result.confidence,
            )
            return result

        except (LLMException, LLMRateLimitException, LLMConnectionException,
                LLMAuthenticationException, LLMAPIStatusException) as exc:
            logger.error("email_ai_classification_failed", error=str(exc), fallback="rule_based")
            return self._rule_based_classify(from_email, subject, body)
        except Exception as exc:
            logger.error("email_classification_unexpected_error", error=str(exc))
            return self._rule_based_classify(from_email, subject, body)

    def _rule_based_classify(
        self,
        from_email: str,
        subject: str,
        body: str,
    ) -> EmailClassificationResult:
        """Keyword fallback when Groq is unavailable."""
        import re

        subject_lower = subject.lower()
        body_lower = body.lower()
        combined = subject_lower + " " + body_lower

        # Reject patterns
        reject_patterns = [
            "out of office", "auto-reply", "automatic reply", "unsubscribe",
            "newsletter", "no-reply", "noreply", "do not reply", "delivery failed",
            "mailer-daemon", "postmaster",
        ]
        if any(p in combined for p in reject_patterns):
            return EmailClassificationResult(
                is_support_request=False,
                is_thread_reply=False,
                suggested_title=subject[:500],
                suggested_description=body[:2000],
                confidence=0.9,
                rejection_reason="Auto-reply or non-support email detected by keyword filter.",
            )

        # Thread reply detection
        ticket_pattern = re.compile(r"TKT-[A-Z0-9]{6}", re.IGNORECASE)
        ticket_match = ticket_pattern.search(subject + " " + body)
        is_reply = subject_lower.startswith("re:") or ticket_match is not None
        ticket_number = ticket_match.group(0).upper() if ticket_match else None

        # Support keywords
        support_kw = [
            "help", "issue", "problem", "error", "bug", "broken", "not working",
            "support", "ticket", "cannot", "unable", "fail", "crash", "urgent",
        ]
        is_support = any(k in combined for k in support_kw) or is_reply

        clean_subject = re.sub(r"^(re:|fwd?:)\s*", "", subject, flags=re.IGNORECASE).strip()
        if ticket_number:
            clean_subject = re.sub(
                r"TKT-[A-Z0-9]{6}", "", clean_subject, flags=re.IGNORECASE
            ).strip(" -[]")

        return EmailClassificationResult(
            is_support_request=is_support,
            is_thread_reply=is_reply,
            extracted_ticket_number=ticket_number,
            suggested_title=clean_subject[:500] or subject[:500],
            suggested_description=body[:5000],
            confidence=0.6,
            rejection_reason=None if is_support else "No support keywords found.",
        )