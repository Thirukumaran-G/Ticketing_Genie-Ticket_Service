"""IMAP email listener — polls inbox for unseen emails, classifies them,
creates new tickets or appends interactions for thread replies.

Threading strategy:
- New support email  → create_ticket_from_email Celery task
- Reply to existing  → append_email_reply_to_ticket Celery task
- Non-support email  → log + discard
"""

from __future__ import annotations

import email
import imaplib
import re
import textwrap
from email.header import decode_header, make_header
from email.message import Message

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_TICKET_RE = re.compile(r"TKT-[A-Z0-9]{6}", re.IGNORECASE)


# ── Header helpers ────────────────────────────────────────────────────────────

def _decode_header_value(raw: str | None) -> str:
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw or ""


def _extract_body(msg: Message) -> str:
    """Walk MIME parts; prefer text/plain, fall back to text/html stripped of tags."""
    plain_parts: list[str] = []
    html_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if "attachment" in disp:
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = payload.decode("utf-8", errors="replace")

            if ctype == "text/plain":
                plain_parts.append(text)
            elif ctype == "text/html":
                html_parts.append(_strip_html(text))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = payload.decode("utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                html_parts.append(_strip_html(text))
            else:
                plain_parts.append(text)

    body = "\n".join(plain_parts) or "\n".join(html_parts)
    return _clean_body(body)


def _strip_html(html: str) -> str:
    """Naïve HTML → plain text (no external deps)."""
    import html as html_mod
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html_mod.unescape(text)


def _clean_body(body: str) -> str:
    """Remove quoted reply blocks and trailing whitespace."""
    lines = body.splitlines()
    cleaned: list[str] = []
    for line in lines:
        stripped = line.strip()
        # Stop at common quoted-reply markers
        if stripped.startswith(">") or re.match(
            r"^(on .+wrote:|from:.+sent:.+to:|-----original message-----)",
            stripped,
            re.IGNORECASE,
        ):
            break
        cleaned.append(line)
    return "\n".join(cleaned).strip()


# ── IMAP core ─────────────────────────────────────────────────────────────────

def _connect_imap() -> imaplib.IMAP4_SSL:
    """Open an authenticated IMAP4_SSL connection."""
    if not settings.IMAP_HOST or not settings.IMAP_USER or not settings.IMAP_PASSWORD:
        raise RuntimeError("IMAP not configured — set IMAP_HOST, IMAP_USER, IMAP_PASSWORD")

    conn = imaplib.IMAP4_SSL(settings.IMAP_HOST, settings.IMAP_PORT)
    conn.login(settings.IMAP_USER, settings.IMAP_PASSWORD)
    return conn


def poll_imap_inbox() -> None:
    """
    Entry point called by the Celery beat task every 60 s.

    For each UNSEEN email:
      - Parse headers + body
      - Fire Celery task: process_inbound_email
    """
    from src.core.celery_app import process_inbound_email

    if not settings.IMAP_HOST or not settings.IMAP_USER:
        logger.debug("poll_imap_inbox skipped — IMAP not configured")
        return

    mailbox = settings.IMAP_MAILBOX or "INBOX"

    try:
        conn = _connect_imap()
    except Exception as exc:
        logger.error("imap_connect_failed", error=str(exc))
        return

    try:
        conn.select(mailbox)
        status, data = conn.search(None, "UNSEEN")
        if status != "OK" or not data or not data[0]:
            logger.debug("imap_no_unseen_emails", mailbox=mailbox)
            return

        uids: list[bytes] = data[0].split()
        logger.info("imap_unseen_emails_found", count=len(uids), mailbox=mailbox)

        for uid in uids:
            try:
                _process_single_email(conn, uid)
            except Exception as exc:
                logger.error("imap_process_email_error", uid=uid.decode(), error=str(exc))
                # Mark as seen anyway to avoid re-processing loops on persistent errors
                try:
                    conn.store(uid, "+FLAGS", "\\Seen")
                except Exception:
                    pass

    except Exception as exc:
        logger.error("imap_poll_error", error=str(exc))
    finally:
        try:
            conn.logout()
        except Exception:
            pass


def _process_single_email(conn: imaplib.IMAP4_SSL, uid: bytes) -> None:
    """Fetch, parse and dispatch a single email UID."""
    from src.core.celery_app import process_inbound_email

    status, msg_data = conn.fetch(uid, "(RFC822)")
    if status != "OK" or not msg_data or not msg_data[0]:
        logger.warning("imap_fetch_failed", uid=uid.decode())
        return

    raw = msg_data[0][1]  # type: ignore[index]
    if not isinstance(raw, bytes):
        return

    msg = email.message_from_bytes(raw)

    message_id: str = (msg.get("Message-ID") or "").strip()
    from_header: str = _decode_header_value(msg.get("From", ""))
    subject: str = _decode_header_value(msg.get("Subject", "(no subject)"))
    in_reply_to: str = (msg.get("In-Reply-To") or "").strip()
    references: str = (msg.get("References") or "").strip()
    body: str = _extract_body(msg)

    # Extract sender email address
    from_email_match = re.search(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", from_header)
    from_email: str = from_email_match.group(0).lower() if from_email_match else from_header

    if not message_id:
        # Generate a pseudo message-id to avoid duplicates
        import hashlib, time
        message_id = f"<pseudo-{hashlib.md5(f'{from_email}{subject}{time.time()}'.encode()).hexdigest()}@local>"

    logger.info(
        "imap_email_parsed",
        message_id=message_id,
        from_email=from_email,
        subject=subject[:80],
        has_in_reply_to=bool(in_reply_to),
    )

    # Mark as seen before dispatching so a crash in Celery doesn't re-process
    conn.store(uid, "+FLAGS", "\\Seen")

    process_inbound_email.delay(
        message_id=message_id,
        from_email=from_email,
        subject=subject,
        body=body,
        in_reply_to=in_reply_to or None,
        references=references or None,
    )