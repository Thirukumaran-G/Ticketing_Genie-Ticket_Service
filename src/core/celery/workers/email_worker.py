from __future__ import annotations

import asyncio

from src.core.celery.app import celery_app
from src.core.celery.utils import fetch_customer_email, fetch_user_email
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


def _is_valid_email(addr: str | None) -> bool:
    return bool(addr and addr != "None" and "@" in addr)


# ── Acknowledgement Email ─────────────────────────────────────────────────────

@celery_app.task(
    name="ticket.email.send_ack",
    bind=True,
    max_retries=3,
)
def send_acknowledgement_email(
    self,
    customer_email: str,
    ticket_number: str,
    ticket_id: str,
) -> None:
    """Send auto-acknowledgement to customer immediately after ticket creation."""

    async def _send() -> None:
        if not _is_valid_email(customer_email):
            logger.error(
                "ack_email_invalid_address",
                ticket_number=ticket_number,
                customer_email=customer_email,
            )
            return  # Invalid address — do not retry

        from src.handlers.http_clients.email_client import EmailClient
        await EmailClient().send_acknowledgement(customer_email, ticket_number, ticket_id)

    try:
        asyncio.run(_send())
        logger.info(
            "ack_email_sent",
            ticket_number=ticket_number,
            customer_email=customer_email,
        )
    except Exception as exc:
        logger.error(
            "ack_email_failed",
            ticket_number=ticket_number,
            error=str(exc),
        )
        if _is_valid_email(customer_email):
            raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


# ── Generic Notification Email ────────────────────────────────────────────────

@celery_app.task(
    name="ticket.email.send_notification",
    bind=True,
    max_retries=3,
)
def send_notification_email(
    self,
    recipient_id: str,
    recipient_type: str,         # "persona" | "customer"
    subject: str,
    body: str,
    recipient_email: str | None = None,
) -> None:
    """Send a generic notification email.

    recipient_email may be passed directly (preferred) to avoid
    an extra auth-service round-trip. If absent, it is resolved
    from auth-service based on recipient_type.
    """

    async def _send() -> None:
        email_addr = recipient_email

        if not _is_valid_email(email_addr):
            # Resolve from auth-service
            if recipient_type == "customer":
                email_addr = await fetch_customer_email(recipient_id)
            else:
                email_addr = await fetch_user_email(recipient_id)

        if not _is_valid_email(email_addr):
            logger.error(
                "notification_email_no_valid_address",
                recipient_id=recipient_id,
                recipient_type=recipient_type,
                provided=recipient_email,
                resolved=email_addr,
            )
            return  # Data/config issue — do not retry

        from src.handlers.http_clients.email_client import EmailClient
        await EmailClient().send_generic(email_addr, subject, body)
        logger.info(
            "notification_email_sent",
            recipient_id=recipient_id,
            recipient_type=recipient_type,
            email=email_addr,
        )

    try:
        asyncio.run(_send())
    except Exception as exc:
        logger.error(
            "notification_email_failed",
            recipient_id=recipient_id,
            subject=subject,
            error=str(exc),
        )
        if _is_valid_email(recipient_email):
            raise self.retry(exc=exc, countdown=30)