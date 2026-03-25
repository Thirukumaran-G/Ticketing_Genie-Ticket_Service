# email_worker.py
from __future__ import annotations

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async
from src.core.celery.utils import fetch_customer_email, fetch_user_email
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


def _is_valid_email(addr: str | None) -> bool:
    return bool(addr and addr != "None" and "@" in addr)


# ── Generic Notification Email ────────────────────────────────────────────────
# Single email delivery layer for the entire system.
# Called exclusively by notification_worker when recipient preference is email,
# and directly by ai_worker for transactional ticket receipts.
#
# NOTE: send_acknowledgement_email (ticket.email.send_ack) has been removed.
# ai_worker.run_ai_classification owns all transactional ticket receipts
# via send_notification_email.delay() directly.

@celery_app.task(
    name="ticket.email.send_notification",
    bind=True,
    max_retries=3,
)
def send_notification_email(
    self,
    recipient_id:    str,
    recipient_type:  str,           # "agent" | "team_lead" | "customer"
    subject:         str,
    body:            str,
    recipient_email: str | None = None,
) -> None:
    """
    Deliver a notification email to any recipient.

    recipient_email should always be passed directly by the caller
    after resolving from auth-service. The fallback resolution here
    exists only as a safety net.

    recipient_type values:
      "agent"      — internal support agent
      "team_lead"  — team lead
      "customer"   — end customer
    """

    async def _send() -> None:
        email_addr = recipient_email

        # Fallback resolution if email not passed directly
        if not _is_valid_email(email_addr):
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
            return

        from src.handlers.http_clients.email_client import EmailClient
        await EmailClient().send_generic(email_addr, subject, body)

        logger.info(
            "notification_email_sent",
            recipient_id=recipient_id,
            recipient_type=recipient_type,
            email=email_addr,
            subject=subject,
        )

    try:
        run_async(_send())
    except Exception as exc:
        logger.error(
            "notification_email_failed",
            recipient_id=recipient_id,
            recipient_type=recipient_type,
            subject=subject,
            error=str(exc),
        )
        if _is_valid_email(recipient_email):
            raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))