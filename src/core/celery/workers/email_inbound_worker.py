from __future__ import annotations

import base64
import uuid as _uuid

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_SUPPORTED_ATTACHMENT_MIME = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "application/pdf",
}

_PORTAL_URL = None


def _get_portal_url() -> str:
    global _PORTAL_URL
    if _PORTAL_URL is None:
        from src.config.settings import settings
        _PORTAL_URL = getattr(settings, "PORTAL_URL", "https://support.ticketinggenie.com")
    return _PORTAL_URL


async def _load_imap_credentials(session) -> dict[str, str | None]:
    """
    Load all IMAP config from ticket.email_config table.
    Returns dict with keys: IMAP_USER, IMAP_PASSWORD, IMAP_HOST, IMAP_PORT.
    Falls back to settings for HOST/PORT if not in DB.
    """
    from sqlalchemy import select
    from src.data.models.postgres.models import EmailConfig
    from src.config.settings import settings

    result = await session.execute(
        select(EmailConfig).where(
            EmailConfig.key.in_([
                "IMAP_USER",
                "IMAP_PASSWORD",
                "IMAP_HOST",
                "IMAP_PORT",
            ]),
            EmailConfig.is_active.is_(True),
        )
    )
    rows = result.scalars().all()
    db_cfg: dict[str, str] = {
        row.key: row.value
        for row in rows
        if row.value
    }

    return {
        "IMAP_USER":     db_cfg.get("IMAP_USER"),
        "IMAP_PASSWORD": db_cfg.get("IMAP_PASSWORD"),
        "IMAP_HOST":     db_cfg.get("IMAP_HOST") or settings.IMAP_HOST,
        "IMAP_PORT":     db_cfg.get("IMAP_PORT") or str(settings.IMAP_PORT),
        "IMAP_MAILBOX":  db_cfg.get("IMAP_MAILBOX") or settings.IMAP_MAILBOX,
    }


# ── Beat entry point ──────────────────────────────────────────────────────────

@celery_app.task(name="ticket.email.poll_inbox", queue="email_inbound")
def poll_inbox_task() -> None:
    """Called by Celery Beat every 20 s."""

    async def _load_and_poll() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.handlers.email.imap_listener import poll_imap_inbox

        async with CelerySessionFactory() as session:
            cfg = await _load_imap_credentials(session)

        imap_user     = cfg.get("IMAP_USER")
        imap_password = cfg.get("IMAP_PASSWORD")
        imap_host     = cfg.get("IMAP_HOST")
        imap_port     = int(cfg.get("IMAP_PORT") or 993)
        imap_mailbox  = cfg.get("IMAP_MAILBOX") or "INBOX"

        if not imap_user or not imap_password:
            logger.debug("poll_inbox_task_skipped_no_imap_credentials")
            return

        if not imap_host:
            logger.debug("poll_inbox_task_skipped_no_imap_host")
            return

        poll_imap_inbox(
            imap_user=imap_user,
            imap_password=imap_password,
            imap_host=imap_host,
            imap_port=imap_port,
            imap_mailbox=imap_mailbox,
        )

    run_async(_load_and_poll())


# ── Main inbound processing task ──────────────────────────────────────────────

@celery_app.task(
    name="ticket.email.process_inbound",
    bind=True,
    max_retries=3,
    queue="email_inbound",
)
def process_inbound_email(
    self,
    message_id:  str,
    from_email:  str,
    subject:     str,
    body:        str,
    in_reply_to: str | None = None,
    references:  str | None = None,
    attachments: list[dict] | None = None,
) -> dict:

    async def _run() -> dict:
        from src.handlers.http_clients.auth_inbound_client import AuthInboundClient
        from src.data.clients.postgres_client import CelerySessionFactory

        auth_client = AuthInboundClient()

        # ── Step 1: domain validation ─────────────────────────────────────────
        if "@" not in from_email:
            logger.info("inbound_email_invalid_sender", from_email=from_email)
            return {"status": "discarded", "reason": "invalid_sender"}

        domain  = from_email.split("@", 1)[1].lower().strip()
        company = await auth_client.get_company_by_domain(domain)

        if not company:
            logger.info(
                "inbound_email_domain_not_registered",
                from_email=from_email,
                domain=domain,
            )
            async with CelerySessionFactory() as session:
                await _record_email_processing(
                    session, message_id, from_email, subject,
                    in_reply_to, references,
                    status="discarded",
                    failure_reason="domain_not_registered",
                )
                await session.commit()
            return {"status": "discarded", "reason": "domain_not_registered"}

        # ── Step 2: fetch active products ─────────────────────────────────────
        raw_products = await auth_client.list_active_products()
        if not raw_products:
            logger.error("inbound_email_no_products_from_auth_service")
            async with CelerySessionFactory() as session:
                await _record_email_processing(
                    session, message_id, from_email, subject,
                    in_reply_to, references,
                    status="failed",
                    failure_reason="no_active_products",
                )
                await session.commit()
            return {"status": "failed", "reason": "no_active_products"}

        product_map: dict[str, tuple[str, str]] = {
            p.name.lower().strip(): (p.id, p.name)
            for p in raw_products
        }
        valid_product_names = [p.name for p in raw_products]

        async with CelerySessionFactory() as session:

            # ── Step 3: thread detection ──────────────────────────────────────
            parent_ticket = None
            has_reply_signal = (
                bool(in_reply_to)
                or bool(references)
                or "TKT-" in subject.upper()
            )
            if has_reply_signal:
                parent_ticket = await _find_ticket_by_thread(
                    session,
                    in_reply_to=in_reply_to,
                    references=references,
                    subject=subject,
                )

            # ── Step 4: user create-or-get ────────────────────────────────────
            full_name = (
                from_email.split("@")[0]
                .replace(".", " ")
                .replace("_", " ")
                .title()
            )
            customer = await auth_client.create_or_get_customer(
                email=from_email,
                full_name=full_name,
                company_id=company.company_id,
            )
            if not customer:
                logger.error(
                    "inbound_email_user_resolution_failed",
                    from_email=from_email,
                )
                await _record_email_processing(
                    session, message_id, from_email, subject,
                    in_reply_to, references,
                    status="failed",
                    failure_reason="user_resolution_failed",
                )
                await session.commit()
                return {"status": "failed", "reason": "user_resolution_failed"}

            # ── Step 5: thread reply ──────────────────────────────────────────
            if parent_ticket:
                result = await _handle_thread_reply(
                    session=session,
                    ticket=parent_ticket,
                    customer=customer,
                    body=body,
                    message_id=message_id,
                    from_email=from_email,
                    subject=subject,
                    in_reply_to=in_reply_to,
                    references=references,
                    attachments=attachments or [],
                )
                await session.commit()
                return result

            # ── Step 6: LLM extraction ────────────────────────────────────────
            from src.control.agents.email_extraction_agent import (
                EmailExtractionAgent,
                EmailExtractionResult,
            )
            agent     = EmailExtractionAgent()
            extracted: EmailExtractionResult = await agent.extract(
                subject=subject,
                body=body,
                valid_products=valid_product_names,
            )

            # ── Step 7: product resolution ────────────────────────────────────
            product_id_str       = None
            matched_product_name = None
            if extracted.product_name:
                match = product_map.get(extracted.product_name.lower().strip())
                if match:
                    product_id_str, matched_product_name = match

            # ── Step 8: completeness check ────────────────────────────────────
            missing = _find_missing_fields(extracted, product_id_str)
            if missing:
                await _send_incomplete_info_reply(
                    to_email=from_email,
                    subject=subject,
                    missing_fields=missing,
                    valid_products=valid_product_names,
                )
                await _record_email_processing(
                    session, message_id, from_email, subject,
                    in_reply_to, references,
                    status="discarded",
                    failure_reason=f"incomplete_fields:{','.join(missing)}",
                )
                await session.commit()
                return {
                    "status":  "discarded",
                    "reason":  "incomplete_fields",
                    "missing": missing,
                }

            # ── Step 9: subscription check ────────────────────────────────────
            has_sub = await _verify_subscription(
                session,
                company_id=company.company_id,
                product_id=product_id_str,
            )
            if not has_sub:
                await _send_no_subscription_reply(
                    to_email=from_email,
                    subject=subject,
                    product_name=matched_product_name,
                )
                await _record_email_processing(
                    session, message_id, from_email, subject,
                    in_reply_to, references,
                    status="discarded",
                    failure_reason="no_active_subscription",
                )
                await session.commit()
                return {"status": "discarded", "reason": "no_active_subscription"}

            # ── Step 10: create ticket ────────────────────────────────────────
            ticket = await _create_ticket_from_email(
                session=session,
                customer=customer,
                company=company,
                extracted=extracted,
                product_id_str=product_id_str,
                message_id=message_id,
                from_email=from_email,
                subject=subject,
                in_reply_to=in_reply_to,
                references=references,
                attachments=attachments or [],
            )
            await session.commit()

            # ── Step 11: reply email ──────────────────────────────────────────
            if customer.is_new:
                await _send_welcome_new_user(
                    to_email=from_email,
                    ticket_number=ticket["ticket_number"],
                    temp_password=customer.temp_password,
                )
            else:
                await _send_existing_user_portal_nudge(
                    to_email=from_email,
                    ticket_number=ticket["ticket_number"],
                )

            # ── Step 12: AI worker handoff ────────────────────────────────────
            from src.core.celery.workers.ai_worker import run_ai_classification
            run_ai_classification.apply_async(
                args=[ticket["ticket_id"]],
                kwargs={"customer_email": from_email},
                queue="ai_tasks",
            )

            logger.info(
                "inbound_email_ticket_created",
                ticket_id=ticket["ticket_id"],
                ticket_number=ticket["ticket_number"],
                from_email=from_email,
            )
            return {
                "status":        "created",
                "ticket_id":     ticket["ticket_id"],
                "ticket_number": ticket["ticket_number"],
            }

    try:
        return run_async(_run())
    except Exception as exc:
        logger.error(
            "process_inbound_email_error",
            message_id=message_id,
            error=str(exc),
        )
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


# ── Subscription check ────────────────────────────────────────────────────────

async def _verify_subscription(
    session,
    company_id: str,
    product_id: str,
) -> bool:
    from sqlalchemy import Boolean, Column, MetaData, Table, select
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    _auth_sub = Table(
        "company_product_subscription",
        MetaData(schema="auth"),
        Column("id",         PG_UUID(as_uuid=True)),
        Column("company_id", PG_UUID(as_uuid=True)),
        Column("product_id", PG_UUID(as_uuid=True)),
        Column("is_active",  Boolean),
    )

    result = await session.execute(
        select(_auth_sub.c.id).where(
            _auth_sub.c.company_id == _uuid.UUID(company_id),
            _auth_sub.c.product_id == _uuid.UUID(product_id),
            _auth_sub.c.is_active.is_(True),
        ).limit(1)
    )
    return result.fetchone() is not None


# ── Thread detection ──────────────────────────────────────────────────────────

def _extract_ticket_number_from_subject(subject: str) -> str | None:
    upper = subject.upper()
    if "TKT-" not in upper:
        return None

    idx       = upper.index("TKT-")
    candidate = subject[idx:]

    ticket_number = ""
    for ch in candidate:
        if ch.isalnum() or ch == "-":
            ticket_number += ch
        else:
            break

    if (
        len(ticket_number) == 10
        and ticket_number.upper().startswith("TKT-")
        and ticket_number[4:].isdigit()
    ):
        return ticket_number.upper()

    return None


async def _find_ticket_by_thread(
    session,
    in_reply_to: str | None,
    references:  str | None,
    subject:     str | None = None,
) -> dict | None:
    from sqlalchemy import select
    from src.data.models.postgres.models import EmailProcessing, Ticket

    candidates: list[str] = []
    if in_reply_to:
        candidates.append(in_reply_to.strip())
    if references:
        candidates.extend(r.strip() for r in references.split() if r.strip())

    for mid in candidates:
        ep_result = await session.execute(
            select(EmailProcessing).where(
                EmailProcessing.message_id == mid,
                EmailProcessing.ticket_id.is_not(None),
            ).limit(1)
        )
        ep = ep_result.scalar_one_or_none()
        if not ep:
            continue

        ticket_result = await session.execute(
            select(Ticket).where(Ticket.id == ep.ticket_id)
        )
        ticket = ticket_result.scalar_one_or_none()
        if ticket:
            logger.info(
                "thread_detected_via_message_id",
                matched_message_id=mid,
                ticket_number=ticket.ticket_number,
            )
            return {
                "id":            ticket.id,
                "ticket_number": ticket.ticket_number,
                "status":        ticket.status,
                "customer_id":   ticket.customer_id,
            }

    if subject:
        ticket_number = _extract_ticket_number_from_subject(subject)
        if ticket_number:
            ticket_result = await session.execute(
                select(Ticket).where(Ticket.ticket_number == ticket_number)
            )
            ticket = ticket_result.scalar_one_or_none()
            if ticket:
                logger.info(
                    "thread_detected_via_subject",
                    ticket_number=ticket_number,
                    subject=subject[:80],
                )
                return {
                    "id":            ticket.id,
                    "ticket_number": ticket.ticket_number,
                    "status":        ticket.status,
                    "customer_id":   ticket.customer_id,
                }

    logger.debug(
        "thread_not_detected",
        in_reply_to=in_reply_to,
        subject=(subject or "")[:80],
    )
    return None


# ── Thread reply handler ──────────────────────────────────────────────────────

async def _handle_thread_reply(
    session,
    ticket:      dict,
    customer,
    body:        str,
    message_id:  str,
    from_email:  str,
    subject:     str,
    in_reply_to: str | None,
    references:  str | None,
    attachments: list[dict],
) -> dict:
    import uuid6
    from src.data.models.postgres.models import Conversation

    conv = Conversation(
        id=uuid6.uuid7(),
        ticket_id=ticket["id"],
        author_id=_uuid.UUID(customer.user_id),
        author_type="customer",
        content=body.strip() or "(no content)",
        is_internal=False,
        is_ai_draft=False,
    )
    session.add(conv)
    await session.flush()

    if attachments:
        await _save_attachments(session, str(ticket["id"]), attachments)

    await _record_email_processing(
        session, message_id, from_email, subject,
        in_reply_to, references,
        status="processed",
        ticket_id=str(ticket["id"]),
        is_thread_reply=True,
    )

    if customer.is_new:
        await _send_thread_reply_new_user(
            to_email=from_email,
            ticket_number=ticket["ticket_number"],
            temp_password=customer.temp_password,
        )
    else:
        await _send_thread_reply_existing_user(
            to_email=from_email,
            ticket_number=ticket["ticket_number"],
        )

    logger.info(
        "inbound_email_thread_reply_added",
        ticket_id=str(ticket["id"]),
        ticket_number=ticket["ticket_number"],
        from_email=from_email,
    )
    return {
        "status":        "thread_reply",
        "ticket_id":     str(ticket["id"]),
        "ticket_number": ticket["ticket_number"],
    }


# ── Ticket creation ───────────────────────────────────────────────────────────

async def _create_ticket_from_email(
    session,
    customer,
    company,
    extracted,
    product_id_str: str,
    message_id:     str,
    from_email:     str,
    subject:        str,
    in_reply_to:    str | None,
    references:     str | None,
    attachments:    list[dict],
) -> dict:
    import uuid6
    from src.constants import TicketSource, TicketStatus
    from src.data.models.postgres.models import Ticket

    tier_snapshot = await _fetch_tier_snapshot(
        session,
        user_id=customer.user_id,
        product_id=product_id_str,
    )

    ticket_number = await _generate_ticket_number(session)

    ticket = Ticket(
        id=uuid6.uuid7(),
        ticket_number=ticket_number,
        title=(extracted.title or "")[:500],
        description=extracted.description or "",
        status=TicketStatus.NEW.value,
        severity=extracted.severity,
        source=TicketSource.EMAIL.value,
        environment=extracted.environment,
        product_id=_uuid.UUID(product_id_str),
        customer_id=_uuid.UUID(customer.user_id),
        company_id=_uuid.UUID(company.company_id),
        tier_snapshot=tier_snapshot,
        email_message_id=message_id,
        customer_priority=extracted.severity,
        priority_overridden=False,
        reopen_count=0,
        on_hold_duration_accumulated=0,
    )
    session.add(ticket)
    await session.flush()
    await session.refresh(ticket)

    if attachments:
        await _save_attachments(session, str(ticket.id), attachments)

    await _record_email_processing(
        session, message_id, from_email, subject,
        in_reply_to, references,
        status="processed",
        ticket_id=str(ticket.id),
        is_thread_reply=False,
    )

    return {
        "ticket_id":     str(ticket.id),
        "ticket_number": ticket.ticket_number,
    }


async def _generate_ticket_number(session) -> str:
    from sqlalchemy import func, select
    from src.data.models.postgres.models import Ticket

    result = await session.execute(select(func.count()).select_from(Ticket))
    count  = result.scalar() or 0
    return f"TKT-{count + 1:06d}"


async def _fetch_tier_snapshot(
    session,
    user_id:    str,
    product_id: str,
) -> str | None:
    from sqlalchemy import Boolean, Column, MetaData, String, Table, select
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    _meta = MetaData(schema="auth")

    _sub = Table(
        "company_product_subscription", _meta,
        Column("company_id", PG_UUID(as_uuid=True)),
        Column("product_id", PG_UUID(as_uuid=True)),
        Column("tier_id",    PG_UUID(as_uuid=True)),
        Column("is_active",  Boolean),
    )
    _tier = Table(
        "tier", _meta,
        Column("id",   PG_UUID(as_uuid=True)),
        Column("name", String),
    )
    _user = Table(
        "user", _meta,
        Column("id",         PG_UUID(as_uuid=True)),
        Column("company_id", PG_UUID(as_uuid=True)),
    )

    result = await session.execute(
        select(_tier.c.name)
        .select_from(
            _sub
            .join(_tier, _tier.c.id == _sub.c.tier_id)
            .join(_user, _user.c.company_id == _sub.c.company_id)
        )
        .where(
            _user.c.id        == _uuid.UUID(user_id),
            _sub.c.product_id == _uuid.UUID(product_id),
            _sub.c.is_active.is_(True),
        )
        .limit(1)
    )
    row = result.fetchone()
    return row.name if row else None


# ── Attachment persistence ────────────────────────────────────────────────────

async def _save_attachments(
    session,
    ticket_id:   str,
    attachments: list[dict],
) -> None:
    import uuid6
    from src.data.models.postgres.models import Attachment

    for att in attachments:
        mime = (att.get("mime_type") or "").lower()
        if mime not in _SUPPORTED_ATTACHMENT_MIME:
            logger.warning(
                "inbound_email_attachment_unsupported_mime",
                mime=mime,
                filename=att.get("filename"),
            )
            continue

        filename  = (att.get("filename") or "attachment")[:255]
        data_b64  = att.get("data_b64") or ""
        file_size = len(base64.b64decode(data_b64)) if data_b64 else 0

        attachment = Attachment(
            id=uuid6.uuid7(),
            ticket_id=_uuid.UUID(ticket_id),
            file_name=filename,
            file_path=f"email_attachments/{ticket_id}/{filename}",
            file_size=file_size,
            mime_type=mime[:100],
        )
        session.add(attachment)

    await session.flush()


# ── Email processing record ───────────────────────────────────────────────────

async def _record_email_processing(
    session,
    message_id:      str,
    from_email:      str,
    subject:         str,
    in_reply_to:     str | None,
    references:      str | None,
    status:          str,
    ticket_id:       str | None = None,
    is_thread_reply: bool = False,
    failure_reason:  str | None = None,
) -> None:
    import uuid6
    from sqlalchemy import select
    from src.data.models.postgres.models import EmailProcessing

    existing_result = await session.execute(
        select(EmailProcessing).where(
            EmailProcessing.message_id == message_id
        )
    )
    existing = existing_result.scalar_one_or_none()

    if existing:
        existing.status         = status
        existing.failure_reason = failure_reason
        if ticket_id:
            existing.ticket_id  = _uuid.UUID(ticket_id)
        await session.flush()
        return

    ep = EmailProcessing(
        id=uuid6.uuid7(),
        message_id=message_id,
        in_reply_to=in_reply_to,
        references=references,
        from_email=from_email,
        subject=subject[:500],
        status=status,
        ticket_id=_uuid.UUID(ticket_id) if ticket_id else None,
        is_thread_reply=is_thread_reply,
        failure_reason=failure_reason,
        retry_count=0,
    )
    session.add(ep)
    await session.flush()


# ── Completeness check ────────────────────────────────────────────────────────

def _find_missing_fields(extracted, product_id_str: str | None) -> list[str]:
    missing = []
    if not (extracted.title or "").strip():
        missing.append("title")
    if not (extracted.description or "").strip():
        missing.append("description")
    if not extracted.product_name or product_id_str is None:
        missing.append("product_name")
    if not extracted.severity:
        missing.append("severity")
    return missing



async def _send_reply_email(to_email: str, subject: str, body: str) -> None:
    try:
        from src.handlers.http_clients.email_client import EmailClient
        await EmailClient().send_generic(to_email, subject, body)
    except Exception as exc:
        logger.error(
            "inbound_email_reply_send_failed",
            to=to_email,
            subject=subject,
            error=str(exc),
        )


async def _send_incomplete_info_reply(
    to_email:       str,
    subject:        str,
    missing_fields: list[str],
    valid_products: list[str],
) -> None:
    field_labels = {
        "title":        "Issue Title (make your email subject descriptive)",
        "description":  "Issue Description (explain the problem in the email body)",
        "product_name": "Product Name (must match one of the registered products below)",
        "severity":     "Severity (include one of: critical / high / medium / low)",
    }
    missing_lines = "\n".join(f"  • {field_labels.get(f, f)}" for f in missing_fields)
    product_lines = "\n".join(f"  • {p}" for p in sorted(valid_products))

    body = (
        f"Dear Customer,\n\n"
        f"Thank you for contacting our support team.\n\n"
        f"We were unable to create a support ticket because the following "
        f"required information is missing or unclear:\n\n"
        f"{missing_lines}\n\n"
        f"Our registered products are:\n\n"
        f"{product_lines}\n\n"
        f"Minimum required information to create a ticket:\n"
        f"  • Issue Title        — a concise summary in your email subject\n"
        f"  • Issue Description  — full details of the problem in the email body\n"
        f"  • Product Name       — one of the products listed above\n"
        f"  • Severity           — one of: critical / high / medium / low\n\n"
        f"Optional:\n"
        f"  • Environment        — production / staging / dev\n"
        f"  • Attachments        — JPEG, PNG, or PDF files\n\n"
        f"Please reply with all required information and we will "
        f"create your ticket immediately.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"Re: {subject} — Action required: missing ticket information",
        body=body,
    )


async def _send_no_subscription_reply(
    to_email:     str,
    subject:      str,
    product_name: str | None,
) -> None:
    body = (
        f"Dear Customer,\n\n"
        f"Thank you for reaching out.\n\n"
        f"We were unable to create a support ticket because your organisation "
        f"does not have an active subscription for: "
        f"{product_name or 'the specified product'}.\n\n"
        f"If you believe this is an error, please contact your account manager "
        f"or reply with your correct product name.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"Re: {subject} — No active subscription found",
        body=body,
    )


async def _send_welcome_new_user(
    to_email:      str,
    ticket_number: str,
    temp_password: str,
) -> None:
    portal_url = _get_portal_url()
    body = (
        f"Dear Customer,\n\n"
        f"Your support ticket has been created successfully.\n\n"
        f"Ticket Number: {ticket_number}\n\n"
        f"We have also created a portal account for you so you can track your "
        f"ticket and communicate with our team directly.\n\n"
        f"Portal URL : {portal_url}\n"
        f"Username   : {to_email}\n"
        f"Password   : {temp_password}\n\n"
        f"IMPORTANT: You will be asked to change your password on first login.\n\n"
        f"From the portal you can:\n"
        f"  • Track your ticket status in real time\n"
        f"  • Reply directly to agent messages\n"
        f"  • Create new support tickets\n"
        f"  • View your full ticket history\n\n"
        f"For all future communication please use the portal rather than email.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"[{ticket_number}] Ticket created — Your portal access details",
        body=body,
    )


async def _send_existing_user_portal_nudge(
    to_email:      str,
    ticket_number: str,
) -> None:
    portal_url = _get_portal_url()
    body = (
        f"Dear Customer,\n\n"
        f"Your support ticket has been created successfully.\n\n"
        f"Ticket Number: {ticket_number}\n\n"
        f"For all further communication please use our support portal:\n\n"
        f"  {portal_url}\n\n"
        f"Log in with your existing credentials to track your ticket, "
        f"reply to our team, and attach additional files.\n\n"
        f"Our team will respond within your SLA window.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"[{ticket_number}] Ticket created — Please use the portal for updates",
        body=body,
    )


async def _send_thread_reply_new_user(
    to_email:      str,
    ticket_number: str,
    temp_password: str,
) -> None:
    portal_url = _get_portal_url()
    body = (
        f"Dear Customer,\n\n"
        f"Your reply has been added to ticket {ticket_number}.\n\n"
        f"We have also set up a portal account for you.\n\n"
        f"Portal URL : {portal_url}\n"
        f"Username   : {to_email}\n"
        f"Password   : {temp_password}\n\n"
        f"IMPORTANT: You will be asked to change your password on first login.\n\n"
        f"For all further communication please use the portal — it gives you "
        f"real-time status updates and a full conversation history.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"[{ticket_number}] Reply received — Your portal access details",
        body=body,
    )


async def _send_thread_reply_existing_user(
    to_email:      str,
    ticket_number: str,
) -> None:
    portal_url = _get_portal_url()
    body = (
        f"Dear Customer,\n\n"
        f"Your reply has been added to ticket {ticket_number}.\n\n"
        f"For all further communication please log in to the portal:\n\n"
        f"  {portal_url}\n\n"
        f"If you have lost your password, use the Forgot Password option "
        f"on the login page.\n\n"
        f"— Ticketing Genie Support Team"
    )
    await _send_reply_email(
        to_email=to_email,
        subject=f"[{ticket_number}] Reply received — Please use the portal",
        body=body,
    )