from __future__ import annotations

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


class TeamRoutingResult(BaseModel):
    """Strict schema for team routing output."""

    team_name: str = Field(
        description="Exact name of the team best suited to handle this ticket.",
    )
    reason: str = Field(
        description="One sentence explaining why this team was chosen.",
        max_length=300,
    )


class TeamRoutingAgent:
    SYSTEM_PROMPT = """You are a support ticket routing assistant for a B2B SaaS platform.

Your job is to assign a support ticket to the most appropriate team based on:
  - The ticket title and description
  - The available team names provided

Rules:
  - You MUST choose exactly one team from the provided list
  - The team_name in your response must match EXACTLY one of the names in the list
  - Base your decision on the nature of the problem described in the ticket
  - If unsure, pick the most general support team available"""

    def __init__(self) -> None:
        self._chain: RunnableSerializable[dict[str, Any], TeamRoutingResult] | None = None

    def _get_chain(self) -> RunnableSerializable[dict[str, Any], TeamRoutingResult]:
        if self._chain is None:
            llm = ChatGroq(
                api_key=SecretStr(settings.GROQ_API_KEY),
                model=settings.GROQ_MODEL,
                temperature=0,
                max_tokens=200,
                stop_sequences=None,
            )
            prompt = ChatPromptTemplate.from_messages([
                ("system", self.SYSTEM_PROMPT),
                ("human", (
                    "Ticket title: {title}\n"
                    "Ticket description: {description}\n"
                    "Ticket severity: {severity}\n\n"
                    "Available teams:\n{team_list}\n\n"
                    "Which team should handle this ticket?"
                )),
            ])
            self._chain = prompt | llm.with_structured_output(TeamRoutingResult)  # type: ignore[assignment]

        return self._chain  # type: ignore[return-value]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def route(
        self,
        title: str,
        description: str,
        severity: str,
        team_names: list[str],
    ) -> TeamRoutingResult:
        """Route ticket to the best team — returns TeamRoutingResult with strict schema."""
        if not settings.GROQ_API_KEY:
            logger.warning("groq_api_key_missing_routing", fallback="first_team")
            return TeamRoutingResult(
                team_name=team_names[0],
                reason="API key missing — defaulting to first available team.",
            )

        try:
            chain = self._get_chain()
            team_list_str = "\n".join(f"  - {name}" for name in team_names)

            def _invoke() -> TeamRoutingResult:
                result = chain.invoke({
                    "title":       title,
                    "description": description or "N/A",
                    "severity":    severity or "unknown",
                    "team_list":   team_list_str,
                })
                if not isinstance(result, TeamRoutingResult):
                    raise TypeError(f"Unexpected routing output type: {type(result)}")
                return result

            result: TeamRoutingResult = await asyncio.to_thread(_invoke)

            logger.info(
                "team_routing_result",
                team_name=result.team_name,
                reason=result.reason,
            )
            return result

        except Exception as exc:
            logger.error("team_routing_failed", error=str(exc), fallback="first_team")
            return TeamRoutingResult(
                team_name=team_names[0],
                reason=f"Routing failed ({exc}) — defaulting to first available team.",
            )