from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from groq import (
    APIConnectionError,
    APIStatusError,
    AuthenticationError,
    RateLimitError,
)
from langchain_core.messages import BaseMessage
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

import os
from dotenv import load_dotenv
load_dotenv(override=True)

os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")


class DraftResult(BaseModel):
    """Strict schema for AI draft output."""

    subject: str = Field(description="Email reply subject line", max_length=255)
    body: str = Field(description="Full email reply body in professional tone")
    tone: str = Field(description="empathetic | formal | technical", default="empathetic")


class DraftAgent:
    """Generates AI-powered draft responses for support tickets."""

    SYSTEM_PROMPT = """You are an expert customer support agent for a B2B SaaS company.
    Generate a professional, empathetic email reply to the customer's support ticket.

    Rules:
    - Start with acknowledgement of the issue
    - Be specific to the problem described
    - Include next steps or request for clarification if needed
    - Professional but warm tone
    - Do NOT promise specific timelines unless the priority is low
"""

    def __init__(self) -> None:
        self._chain: Any = None

    @contextmanager
    def _groq_error_handler(self) -> Generator[None, None, None]:
        """Maps raw Groq SDK errors to structured LLM exceptions."""
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
        """Initialize the LangChain chain if not already set."""
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,  # ← reads from .env: llama-3.3-70b-versatile
                stop_sequences=None,
            )
            prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", self.SYSTEM_PROMPT),
                    ("human", "{ticket_title}\n\n{ticket_description}"),
                ]
            )
            self._chain = prompt | llm

    def generate_draft(self, ticket_title: str, ticket_description: str) -> DraftResult | None:
        with self._groq_error_handler():
            self._get_chain()

            if self._chain is None:
                return None

            response: BaseMessage = self._chain.invoke(
                {
                    "ticket_title": ticket_title,
                    "ticket_description": ticket_description,
                }
            )

            return DraftResult(
                subject=f"Re: {ticket_title}",
                body=str(response.content),
            )