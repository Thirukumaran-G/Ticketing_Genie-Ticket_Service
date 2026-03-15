"""
IMAP email listener.
src/handlers/email/imap_listener.py

Credentials (imap_user, imap_password) are passed in at call time
— loaded from ticket.email_config table by the beat task.
IMAP_HOST, IMAP_PORT, IMAP_MAILBOX remain in settings/env.
"""
from __future__ import annotations

import base64
import email
import imaplib
import re
from email.header import decode_header, make_header
from email.message import Message

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_SUPPORTED_ATTACHMENT_MIME = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "application/pdf",
}


def _decode_header_value(raw: str | None) -> str:
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw or ""


def _extract_body(msg: Message) -> str:
    plain_parts: list[str] = []
    html_parts:  list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp  = str(part.get("Content-Disposition", ""))
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


def _extract_attachments(msg: Message) -> list[dict]:
    attachments = []
    if not msg.is_multipart():
        return attachments
    for part in msg.walk():
        ctype = part.get_content_type().lower()
        if ctype not in _SUPPORTED_ATTACHMENT_MIME:
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        filename = _decode_header_value(
            part.get_filename() or f"attachment.{ctype.split('/')[-1]}"
        )
        attachments.append({
            "filename":  filename[:255],
            "mime_type": ctype,
            "data_b64":  base64.b64encode(payload).decode("ascii"),
        })
    return attachments


def _strip_html(html: str) -> str:
    import html as html_mod
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html_mod.unescape(text)


def _clean_body(body: str) -> str:
    lines   = body.splitlines()
    cleaned: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(">") or re.match(
            r"^(on .+wrote:|from:.+sent:.+to:|-----original message-----)",
            stripped,
            re.IGNORECASE,
        ):
            break
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _extract_from_email(from_header: str) -> str:
    """
    Extract the sender email address from the From header.
    Handles formats:
      - plain         : user@domain.com
      - display name  : John Doe <user@domain.com>
      - quoted name   : "John Doe" <user@domain.com>
    Strategy: find the @ sign, then take everything before it
    (up to a space or <) as local part, and everything after it
    as domain — no regex, pure string splitting.
    """
    raw = from_header.strip()

    # If angle brackets present e.g. "John <user@example.com>"
    # extract only the part inside < >
    if "<" in raw and ">" in raw:
        start = raw.index("<") + 1
        end   = raw.index(">")
        raw   = raw[start:end].strip()

    # Now raw should be plain user@domain or close to it
    raw = raw.strip().lower()

    if "@" not in raw:
        # fallback — return as-is lowercased
        return raw

    local, domain = raw.split("@", 1)
    # strip any trailing whitespace or stray chars from domain
    domain = domain.strip()
    return f"{local}@{domain}"


def _connect_imap(imap_user: str, imap_password: str) -> imaplib.IMAP4_SSL:
    """Connect using credentials loaded from email_config table."""
    if not settings.IMAP_HOST:
        raise RuntimeError("IMAP_HOST not set in environment/settings")
    conn = imaplib.IMAP4_SSL(settings.IMAP_HOST, settings.IMAP_PORT)
    conn.login(imap_user, imap_password)
    return conn


def poll_imap_inbox(imap_user: str, imap_password: str) -> None:
    """
    Entry point called by poll_inbox_task every 20 s.
    Credentials come from email_config table, not env.
    Host/port/mailbox remain in settings.
    """
    from src.core.celery.workers.email_inbound_worker import process_inbound_email

    if not settings.IMAP_HOST:
        logger.debug("poll_imap_inbox_skipped_no_host")
        return

    mailbox = settings.IMAP_MAILBOX or "INBOX"

    try:
        conn = _connect_imap(imap_user, imap_password)
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
        logger.info("imap_unseen_emails_found", count=len(uids))

        for uid in uids:
            try:
                _process_single_email(conn, uid, process_inbound_email)
            except Exception as exc:
                logger.error(
                    "imap_process_email_error",
                    uid=uid.decode(),
                    error=str(exc),
                )
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


def _process_single_email(
    conn,
    uid:                   bytes,
    process_inbound_email,
) -> None:
    status, msg_data = conn.fetch(uid, "(RFC822)")
    if status != "OK" or not msg_data or not msg_data[0]:
        logger.warning("imap_fetch_failed", uid=uid.decode())
        return

    raw = msg_data[0][1]
    if not isinstance(raw, bytes):
        return

    msg = email.message_from_bytes(raw)

    message_id:  str = (msg.get("Message-ID") or "").strip()
    from_header: str = _decode_header_value(msg.get("From", ""))
    subject:     str = _decode_header_value(msg.get("Subject", "(no subject)"))
    in_reply_to: str = (msg.get("In-Reply-To") or "").strip()
    references:  str = (msg.get("References") or "").strip()
    body:        str = _extract_body(msg)
    attachments: list[dict] = _extract_attachments(msg)

    # ── Extract sender email — pure split, no regex ───────────────────────────
    from_email: str = _extract_from_email(from_header)

    if not message_id:
        import hashlib, time
        message_id = (
            f"<pseudo-"
            f"{hashlib.md5(f'{from_email}{subject}{time.time()}'.encode()).hexdigest()}"
            f"@local>"
        )

    logger.info(
        "imap_email_parsed",
        message_id=message_id,
        from_email=from_email,
        subject=subject[:80],
        has_in_reply_to=bool(in_reply_to),
        attachment_count=len(attachments),
    )

    conn.store(uid, "+FLAGS", "\\Seen")

    process_inbound_email.apply_async(
        kwargs={
            "message_id":  message_id,
            "from_email":  from_email,
            "subject":     subject,
            "body":        body,
            "in_reply_to": in_reply_to or None,
            "references":  references or None,
            "attachments": attachments or None,
        },
        queue="email_inbound",
    )