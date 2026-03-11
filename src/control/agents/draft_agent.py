import os
from contextlib import contextmanager
from collections.abc import Generator
from typing import Any
from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from groq import APIConnectionError, APIStatusError, AuthenticationError, RateLimitError
from langchain_core.messages import BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, SecretStr

from src.config.settings import settings
from src.core.exceptions.base import (
    LLMAPIStatusException, LLMAuthenticationException,
    LLMConnectionException, LLMException, LLMRateLimitException,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class DraftResult(BaseModel):
    subject: str = Field(description="Email reply subject line", max_length=255)
    body: str = Field(description="Short 5-line draft reply body")


class DraftAgent:
    SYSTEM_PROMPT = """You are a customer support agent for a B2B SaaS company.

Write an extremely short acknowledgement reply to the customer's support ticket.

Strict rules:
- Line 1: Warm greeting (e.g. "Hi [Customer],")
- Line 2: "Thank you for reaching out to us."
- Lines 3-5: 3 lines ONLY addressing the specific issue — acknowledge it, confirm it's being looked into, ask ONE clarifying question if needed.
- Do NOT add sign-off, promises, timelines, or any extra lines.
- Maximum 5 lines total. No exceptions."""

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

    def _get_chain(self) -> None:
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,
                temperature=0.2,
                max_tokens=150,
                stop_sequences=None,
            )
            prompt = ChatPromptTemplate.from_messages([
                ("system", self.SYSTEM_PROMPT),
                ("human", "Ticket title: {ticket_title}\n\nDescription: {ticket_description}"),
            ])
            self._chain = prompt | llm

    def generate_draft(self, ticket_title: str, ticket_description: str) -> DraftResult | None:
        with self._groq_error_handler():
            self._get_chain()
            if self._chain is None:
                return None

            response: BaseMessage = self._chain.invoke({
                "ticket_title":       ticket_title,
                "ticket_description": ticket_description,
            })

            return DraftResult(
                subject=f"Re: {ticket_title}",
                body=str(response.content).strip(),
            )