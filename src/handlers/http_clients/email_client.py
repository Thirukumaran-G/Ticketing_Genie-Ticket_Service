from collections.abc import Generator
from contextlib import contextmanager
from email.mime.text import MIMEText

import aiosmtplib

from src.config.settings import settings
from src.core.exceptions.base import (
    EmailException,
    EmailNotConfiguredException,
    EmailSendFailedException,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class EmailClient:
    """Sends transactional emails via SMTP."""

    @contextmanager
    def _smtp_error_handler(self, to: str, subject: str) -> Generator[None, None, None]:
        """Context manager that maps raw SMTP errors to structured email exceptions."""
        try:
            yield
        except aiosmtplib.SMTPAuthenticationError as exc:
            logger.error("smtp_authentication_failed", error=str(exc))
            raise EmailException(message="SMTP authentication failed.") from exc
        except aiosmtplib.SMTPConnectError as exc:
            logger.error("smtp_connection_failed", error=str(exc))
            raise EmailException(message="Could not connect to SMTP server.") from exc
        except aiosmtplib.SMTPException as exc:
            logger.error("email_send_failed", to=to, subject=subject, error=str(exc))
            raise EmailSendFailedException() from exc
        except Exception as exc:
            logger.error("email_unexpected_error", to=to, error=str(exc))
            raise EmailException() from exc

    async def _send(self, to_email: str, subject: str, body: str) -> None:
        """Send a plain-text email via SMTP."""
        if not settings.SMTP_USERNAME:
            logger.warning("smtp_not_configured_email_skipped", to=to_email)
            raise EmailNotConfiguredException()

        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = settings.SMTP_USERNAME
        msg["To"] = to_email

        with self._smtp_error_handler(to_email, subject):
            await aiosmtplib.send(
                msg,
                hostname=settings.SMTP_HOST,
                port=settings.SMTP_PORT,
                username=settings.SMTP_USERNAME,
                password=settings.SMTP_PASSWORD,
                start_tls=True,
            )
            logger.info("email_sent", to=to_email, subject=subject)

    async def send_acknowledgement(self, customer_email: str, ticket_number: str, ticket_id: int) -> None:
        """Send auto-acknowledgement email to customer after ticket creation."""
        subject = f"[{ticket_number}] We received your support request"
        body = (
            f"Dear Customer,\n\n"
            f"Thank you for reaching out. We have received your support request.\n\n"
            f"Your ticket number is: {ticket_number}\n"
            f"Our team will review your request and respond within the agreed SLA timeframe.\n\n"
            f"Best regards,\nTicketing Genie Support Team"
        )
        await self._send(customer_email, subject, body)

    async def send_generic(self, to_email: str, subject: str, body: str) -> None:
        """Send a generic notification email."""
        await self._send(to_email, subject, body)

    async def resolve_email(self, recipient_id: int, recipient_type: str) -> str | None:
        """Resolve email address for a recipient via auth service."""
        from src.handlers.http_clients.auth_client import AuthHttpClient
        auth = AuthHttpClient()
        if recipient_type == "customer":
            return await auth.get_customer_email(recipient_id)
        return await auth.get_user_email(recipient_id)
