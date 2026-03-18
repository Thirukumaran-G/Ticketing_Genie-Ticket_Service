# Ticket service 
from __future__ import annotations

import uuid

from sqlalchemy import cast, Date, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import ConflictException, NotFoundException
from src.core.services.audit_service import audit_service
from src.data.models.postgres.models import (
    EmailConfig, KeywordRule, ProductConfig,
    SeverityPriorityMap, SLARule, Ticket,
)
from src.data.repositories.admin_repository import (
    EmailConfigRepository,
    KeywordRuleRepository,
    ProductConfigRepository,
    SeverityPriorityMapRepository,
    SLARuleRepository,
)
from src.handlers.http_clients.auth_client import AuthHttpClient
from src.observability.logging.logger import get_logger
from src.schemas.admin_schema import (
    EmailConfigUpdateRequest,
    KeywordRuleCreateRequest,
    KeywordRuleUpdateRequest,
    ProductConfigUpsertRequest,
    SeverityPriorityMapCreateRequest,
    SLARuleCreateRequest,
)

logger = get_logger(__name__)

_SYSTEM_ONLY_KEYS = {"IMAP_HOST", "IMAP_PORT", "IMAP_MAILBOX"}


class AdminService:

    def __init__(self, session: AsyncSession) -> None:
        self._session             = session
        self._email_repo          = EmailConfigRepository(session)
        self._sla_repo            = SLARuleRepository(session)
        self._sev_repo            = SeverityPriorityMapRepository(session)
        self._kw_repo             = KeywordRuleRepository(session)
        self._product_config_repo = ProductConfigRepository(session)
        self._auth_client         = AuthHttpClient()

    # ── Reference data (HTTP → auth-service) ─────────────────────────────────

    async def list_tiers(self, admin_token: str) -> list[dict]:
        return await self._auth_client.get_tiers(admin_token)

    async def list_products(self, admin_token: str) -> list[dict]:
        return await self._auth_client.get_products(admin_token)

    # ── Email Config ──────────────────────────────────────────────────────────

    async def list_email_config(self) -> list[EmailConfig]:
        configs = await self._email_repo.list_all()
        for c in configs:
            if c.is_secret:
                c.value = "***"
        return configs

    async def upsert_email_config(
        self,
        payload: list[EmailConfigUpdateRequest],
        admin_id: str,
    ) -> list[EmailConfig]:
        results: list[EmailConfig] = []
        for item in payload:
            if item.key in _SYSTEM_ONLY_KEYS:
                logger.warning("admin_system_key_blocked", key=item.key)
                continue
            config = await self._email_repo.upsert(
                key=item.key,
                value=item.value,
                is_secret=item.is_secret,
                updated_by=admin_id,
            )
            results.append(config)
            await audit_service.log(
                entity_type="email_config",
                entity_id=config.id,
                action="email_config_upserted",
                actor_id=uuid.UUID(admin_id),
                actor_type="user",
                new_value={"key": item.key, "is_secret": item.is_secret},
            )
        await self._session.commit()
        logger.info("email_config_upserted", count=len(results), admin_id=admin_id)
        return results
    
    async def delete_email_config(self, key: str, admin_id: str) -> None:
        configs = await self._email_repo.list_all()
        existing = next((c for c in configs if c.key == key), None)
        if not existing:
            raise NotFoundException(f"Email config key '{key}' not found.")
        if key in _SYSTEM_ONLY_KEYS:
            raise ConflictException(f"Key '{key}' is system-only and cannot be deleted.")
        await self._email_repo.delete_email_config(key)
        await self._session.commit()
        logger.info("email_config_deleted", key=key, admin_id=admin_id)

    async def create_email_config(
    self,
    key: str,
    value: str,
    is_secret: bool,
    admin_id: str,
) -> EmailConfig:   
        configs = await self._email_repo.list_all()
        existing = next((c for c in configs if c.key == key), None)
        if existing:
            raise ConflictException(f"Key '{key}' already exists. Use upsert to update.")
        config = await self._email_repo.create_email_config(
            key=key,
            value=value,
            is_secret=is_secret,
            updated_by=admin_id,
        )
        await self._session.commit()
        logger.info("email_config_created", key=key, admin_id=admin_id)
        return config

    # ── SLA Rules ─────────────────────────────────────────────────────────────

    async def list_sla_rules(self) -> list[SLARule]:
        return await self._sla_repo.list_active()

    async def upsert_sla_rule(
        self, payload: SLARuleCreateRequest, admin_id: str
    ) -> SLARule:
        existing = await self._sla_repo.get_by_tier_and_priority(
            str(payload.tier_id), payload.priority
        )
        if existing:
            old_value = {
                "response_time_min":   existing.response_time_min,
                "resolution_time_min": existing.resolution_time_min,
            }
            existing.response_time_min   = payload.response_time_min
            existing.resolution_time_min = payload.resolution_time_min
            existing.is_active           = True
            await self._session.flush()
            await self._session.refresh(existing)
            await audit_service.log(
                entity_type="sla_rule",
                entity_id=existing.id,
                action="sla_rule_updated",
                actor_id=uuid.UUID(admin_id),
                actor_type="user",
                old_value=old_value,
                new_value={
                    "response_time_min":   payload.response_time_min,
                    "resolution_time_min": payload.resolution_time_min,
                },
                changed_fields=["response_time_min", "resolution_time_min"],
            )
            await self._session.commit()
            logger.info("sla_rule_updated", rule_id=str(existing.id))
            return existing

        rule = SLARule(
            tier_id=payload.tier_id,
            priority=payload.priority,
            response_time_min=payload.response_time_min,
            resolution_time_min=payload.resolution_time_min,
            created_by=uuid.UUID(admin_id),
        )
        self._session.add(rule)
        await self._session.flush()
        await self._session.refresh(rule)
        await audit_service.log(
            entity_type="sla_rule",
            entity_id=rule.id,
            action="sla_rule_created",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={
                "tier_id":             str(payload.tier_id),
                "priority":            payload.priority,
                "response_time_min":   payload.response_time_min,
                "resolution_time_min": payload.resolution_time_min,
            },
        )
        await self._session.commit()
        logger.info("sla_rule_created", rule_id=str(rule.id))
        return rule

    async def hard_delete_sla_rule(self, rule_id: str, admin_id: str) -> None:
        rule = await self._sla_repo.get_by_id(rule_id)
        if not rule:
            raise NotFoundException(f"SLA rule {rule_id} not found.")
        await self._sla_repo.hard_delete_sla_rule(rule_id)
        await self._session.commit()
        logger.info("sla_rule_hard_deleted", rule_id=rule_id)

    # ── Severity Priority Map ─────────────────────────────────────────────────

    async def list_severity_priority_map(self) -> list[SeverityPriorityMap]:
        return await self._sev_repo.list_all()

    async def upsert_severity_priority_map(
        self, payload: SeverityPriorityMapCreateRequest, admin_id: str
    ) -> SeverityPriorityMap:
        existing = await self._sev_repo.get_by_severity_and_tier(
            payload.severity, str(payload.tier_id)
        )
        if existing:
            old_priority              = existing.derived_priority
            existing.derived_priority = payload.derived_priority
            await self._session.flush()
            await self._session.refresh(existing)
            await audit_service.log(
                entity_type="severity_priority_map",
                entity_id=existing.id,
                action="severity_priority_map_updated",
                actor_id=uuid.UUID(admin_id),
                actor_type="user",
                old_value={"derived_priority": old_priority},
                new_value={"derived_priority": payload.derived_priority},
                changed_fields=["derived_priority"],
            )
            await self._session.commit()
            return existing

        mapping = SeverityPriorityMap(
            severity=payload.severity,
            tier_id=payload.tier_id,
            derived_priority=payload.derived_priority,
        )
        self._session.add(mapping)
        await self._session.flush()
        await self._session.refresh(mapping)
        await audit_service.log(
            entity_type="severity_priority_map",
            entity_id=mapping.id,
            action="severity_priority_map_created",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={
                "severity":         payload.severity,
                "tier_id":          str(payload.tier_id),
                "derived_priority": payload.derived_priority,
            },
        )
        await self._session.commit()
        return mapping

    async def hard_delete_severity_priority_map(self, map_id: str, admin_id: str) -> None:
        mapping = await self._sev_repo.get_by_id(map_id)
        if not mapping:
            raise NotFoundException(f"Severity priority map {map_id} not found.")
        await self._sev_repo.hard_delete_severity_priority_map(map_id)
        await self._session.commit()
        logger.info("severity_priority_map_hard_deleted", map_id=map_id)

    # ── Keyword Rules ─────────────────────────────────────────────────────────

    async def list_keyword_rules(self) -> list[KeywordRule]:
        return await self._kw_repo.list_active()

    async def create_keyword_rule(
    self, payload: KeywordRuleCreateRequest, admin_id: str
) -> KeywordRule:
        existing = await self._kw_repo.get_by_keyword(payload.keyword)
        if existing and existing.is_active:
            raise ConflictException(f"Keyword '{payload.keyword}' already exists.")
        rule = KeywordRule(
            keyword=payload.keyword,
            severity=payload.severity,
            product_id=payload.product_id,  # ← add this
            created_by=uuid.UUID(admin_id),
        )
        self._session.add(rule)
        await self._session.flush()
        await self._session.refresh(rule)
        await audit_service.log(
            entity_type="keyword_rule",
            entity_id=rule.id,
            action="keyword_rule_created",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={"keyword": payload.keyword, "severity": payload.severity, "product_id": str(payload.product_id)},
        )
        await self._session.commit()
        logger.info("keyword_rule_created", rule_id=str(rule.id))
        return rule

    async def update_keyword_rule(
        self, rule_id: str, payload: KeywordRuleUpdateRequest, admin_id: str
    ) -> KeywordRule:
        rule = await self._kw_repo.get_by_id(rule_id)
        if not rule:
            raise NotFoundException(f"Keyword rule {rule_id} not found.")
        changed_fields: list[str] = []
        old_value: dict = {}
        if payload.keyword is not None and payload.keyword != rule.keyword:
            old_value["keyword"] = rule.keyword
            rule.keyword = payload.keyword
            changed_fields.append("keyword")
        if payload.severity is not None and payload.severity != rule.severity:
            old_value["severity"] = rule.severity
            rule.severity = payload.severity
            changed_fields.append("severity")
        if payload.is_active is not None and payload.is_active != rule.is_active:
            old_value["is_active"] = rule.is_active
            rule.is_active = payload.is_active
            changed_fields.append("is_active")
        if changed_fields:
            await self._session.flush()
            await self._session.refresh(rule)
            await audit_service.log(
                entity_type="keyword_rule",
                entity_id=rule.id,
                action="keyword_rule_updated",
                actor_id=uuid.UUID(admin_id),
                actor_type="user",
                old_value=old_value,
                new_value={f: getattr(rule, f) for f in changed_fields},
                changed_fields=changed_fields,
            )
            await self._session.commit()
            logger.info("keyword_rule_updated", rule_id=rule_id, fields=changed_fields)
        return rule

    async def hard_delete_keyword_rule(self, rule_id: str, admin_id: str) -> None:
        rule = await self._kw_repo.get_by_id(rule_id)
        if not rule:
            raise NotFoundException(f"Keyword rule {rule_id} not found.")
        await self._kw_repo.hard_delete_keyword_rule(rule_id)
        await self._session.commit()
        logger.info("keyword_rule_hard_deleted", rule_id=rule_id)

    # ── Product Config ────────────────────────────────────────────────────────

    async def list_product_configs(self) -> list[ProductConfig]:
        return await self._product_config_repo.list_active()

    async def upsert_product_config(
        self, product_id: str, payload: ProductConfigUpsertRequest, admin_id: str
    ) -> ProductConfig:
        existing = await self._product_config_repo.get_by_product_id(product_id)
        if existing:
            old_value = {
                "min_severity":    existing.min_severity,
                "default_escalate": existing.default_escalate,
            }
            existing.min_severity     = payload.min_severity
            existing.default_escalate = payload.default_escalate
            existing.is_active        = True
            existing.updated_by       = uuid.UUID(admin_id)
            await self._session.flush()
            await self._session.refresh(existing)
            await audit_service.log(
                entity_type="product_config",
                entity_id=existing.id,
                action="product_config_updated",
                actor_id=uuid.UUID(admin_id),
                actor_type="user",
                old_value=old_value,
                new_value={
                    "min_severity":    payload.min_severity,
                    "default_escalate": payload.default_escalate,
                },
                changed_fields=["min_severity", "default_escalate"],
            )
            await self._session.commit()
            return existing

        config = ProductConfig(
            product_id=uuid.UUID(product_id),
            min_severity=payload.min_severity,
            default_escalate=payload.default_escalate,
            updated_by=uuid.UUID(admin_id),
        )
        self._session.add(config)
        await self._session.flush()
        await self._session.refresh(config)
        await audit_service.log(
            entity_type="product_config",
            entity_id=config.id,
            action="product_config_created",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={
                "product_id":      product_id,
                "min_severity":    payload.min_severity,
                "default_escalate": payload.default_escalate,
            },
        )
        await self._session.commit()
        logger.info("product_config_created", config_id=str(config.id))
        return config

    async def hard_delete_product_config(self, product_id: str, admin_id: str) -> None:
        existing = await self._product_config_repo.get_by_product_id(product_id)
        if not existing:
            raise NotFoundException(f"ProductConfig for product {product_id} not found.")
        await self._product_config_repo.hard_delete_product_config(product_id)
        await self._session.commit()
        logger.info("product_config_hard_deleted", product_id=product_id)

    # ── Reports ───────────────────────────────────────────────────────────────

    async def report_open_tickets_by_priority(self) -> dict:
        result = await self._session.execute(
            select(Ticket.priority, func.count(Ticket.id).label("count"))
            .where(Ticket.status == "assigned")
            .group_by(Ticket.priority)
            .order_by(Ticket.priority)
        )
        return {
            "open_tickets_by_priority": [
                {"priority": row.priority or "unset", "count": row.count}
                for row in result.all()
            ]
        }
    
    # ─── NEW REPORT METHODS ───────────────────────────────────────────────────────

    async def report_tickets_by_severity(self) -> dict:
        from sqlalchemy import func, select
        from src.data.models.postgres.models import Ticket
        rows = await self._session.execute(
            select(Ticket.severity, func.count(Ticket.id).label("count"))
            .group_by(Ticket.severity)
        )
        return {"tickets_by_severity": [{"severity": r.severity, "count": r.count} for r in rows]}

    async def report_tickets_by_status(self) -> dict:
        from sqlalchemy import func, select
        from src.data.models.postgres.models import Ticket
        rows = await self._session.execute(
            select(Ticket.status, func.count(Ticket.id).label("count"))
            .group_by(Ticket.status)
        )
        return {"tickets_by_status": [{"status": r.status, "count": r.count} for r in rows]}

    async def report_avg_resolution_time(self) -> dict:
        from sqlalchemy import func, select
        from src.data.models.postgres.models import Ticket
        result = await self._session.execute(
            select(
                func.avg(
                    func.extract("epoch", Ticket.resolved_at - Ticket.created_at)
                ).label("avg_seconds"),
                func.min(
                    func.extract("epoch", Ticket.resolved_at - Ticket.created_at)
                ).label("min_seconds"),
                func.max(
                    func.extract("epoch", Ticket.resolved_at - Ticket.created_at)
                ).label("max_seconds"),
            ).where(Ticket.resolved_at.isnot(None))
        )
        row = result.fetchone()
        return {
            "avg_resolution_time_min": round((row.avg_seconds or 0) / 60, 1),
            "min_resolution_time_min": round((row.min_seconds or 0) / 60, 1),
            "max_resolution_time_min": round((row.max_seconds or 0) / 60, 1),
        }

    async def report_sla_breach_by_severity(self) -> dict:
        from sqlalchemy import func, select
        from src.data.models.postgres.models import Ticket
        rows = await self._session.execute(
            select(Ticket.severity, func.count(Ticket.id).label("breached"))
            .where(Ticket.sla_breached_at.isnot(None))
            .group_by(Ticket.severity)
        )
        return {"sla_breach_by_severity": [{"severity": r.severity, "breached": r.breached} for r in rows]}

    async def report_tickets_created_by_day(self, days: int = 30) -> dict:
        from sqlalchemy import cast, Date, func, select, text
        from src.data.models.postgres.models import Ticket
        rows = await self._session.execute(
            select(
                cast(Ticket.created_at, Date).label("day"),
                func.count(Ticket.id).label("count"),
            )
            .where(Ticket.created_at >= func.now() - text(f"interval '{days} days'"))
            .group_by(cast(Ticket.created_at, Date))
            .order_by(cast(Ticket.created_at, Date))
        )
        return {"tickets_by_day": [{"day": str(r.day), "count": r.count} for r in rows]}

    async def report_top_companies_by_tickets(self, limit: int = 10) -> dict:
        from sqlalchemy import func, select
        from src.data.models.postgres.models import Ticket
        rows = await self._session.execute(
            select(Ticket.company_id, func.count(Ticket.id).label("total"))
            .where(Ticket.company_id.isnot(None))
            .group_by(Ticket.company_id)
            .order_by(func.count(Ticket.id).desc())
            .limit(limit)
        )
        return {"top_companies": [{"company_id": str(r.company_id), "total": r.total} for r in rows]}
    
    async def report_dashboard_summary(self) -> dict:
        from sqlalchemy import cast, Date, func, select
        from src.data.models.postgres.models import Ticket
 
        # Only "assigned" counts as active work-in-progress
        assigned_status = "assigned"
 
        # 1. assigned ticket count
        assigned_res = await self._session.execute(
            select(func.count(Ticket.id)).where(Ticket.status == assigned_status)
        )
        assigned_count: int = assigned_res.scalar_one() or 0
 
        # 2. total all-time SLA resolve breaches
        breach_res = await self._session.execute(
            select(func.count(Ticket.id)).where(Ticket.sla_breached_at.isnot(None))
        )
        total_breaches: int = breach_res.scalar_one() or 0
 
        # 3. avg first response time (minutes)
        frt_res = await self._session.execute(
            select(
                func.avg(
                    func.extract("epoch", Ticket.first_response_at - Ticket.created_at)
                ).label("avg_seconds")
            ).where(Ticket.first_response_at.isnot(None))
        )
        avg_seconds = frt_res.scalar_one() or 0
 
        # 4. resolved today
        today_res = await self._session.execute(
            select(func.count(Ticket.id)).where(
                cast(Ticket.resolved_at, Date) == func.current_date()
            )
        )
        resolved_today: int = today_res.scalar_one() or 0
 
        return {
            "open_ticket_count":      assigned_count,   # key kept for frontend compat
            "total_sla_breaches":     total_breaches,
            "avg_first_response_min": round((avg_seconds or 0) / 60, 1),
            "tickets_resolved_today": resolved_today,
        }

    async def report_sla_breaches_by_day(self) -> dict:
        from sqlalchemy import cast, Date, func, select, literal_column, union_all, text
 
        # Response breaches by day
        response_q = (
            select(
                cast(Ticket.response_sla_breached_at, Date).label("day"),
                func.count(Ticket.id).label("response_breach_count"),
                literal_column("0").label("resolve_breach_count"),
            )
            .where(Ticket.response_sla_breached_at.isnot(None))
            .group_by(cast(Ticket.response_sla_breached_at, Date))
        )
 
        # Resolve breaches by day
        resolve_q = (
            select(
                cast(Ticket.sla_breached_at, Date).label("day"),
                literal_column("0").label("response_breach_count"),
                func.count(Ticket.id).label("resolve_breach_count"),
            )
            .where(Ticket.sla_breached_at.isnot(None))
            .group_by(cast(Ticket.sla_breached_at, Date))
        )
 
        # Merge both into one series keyed by day using a CTE
        combined = union_all(response_q, resolve_q).subquery("combined")
 
        merged = await self._session.execute(
            select(
                combined.c.day,
                func.sum(combined.c.response_breach_count).label("response_breach_count"),
                func.sum(combined.c.resolve_breach_count).label("resolve_breach_count"),
            )
            .group_by(combined.c.day)
            .order_by(combined.c.day)
        )
 
        return {
            "sla_breaches_by_day": [
                {
                    "day":                  str(row.day),
                    "breach_count":         row.response_breach_count + row.resolve_breach_count,  # kept for sparkline compat
                    "response_breach_count": row.response_breach_count,
                    "resolve_breach_count":  row.resolve_breach_count,
                }
                for row in merged.all()
            ]
        }
    
    async def list_team_members(self, team_id: str) -> list:
        from sqlalchemy import select
        from src.data.models.postgres.models import TeamMember
        import uuid as uuid_lib
 
        try:
            tid = uuid_lib.UUID(team_id)
        except (ValueError, AttributeError):
            return []
 
        rows = await self._session.execute(
            select(TeamMember).where(TeamMember.team_id == tid)
        )
        members = rows.scalars().all()
 
        result = []
        for m in members:
            skills = m.skills if isinstance(m.skills, dict) else {}
            skill_text = (
                skills.get("skill_text", "")
                or skills.get("text", "")
                or ""
            )
            result.append({
                "id":         str(m.id),
                "team_id":    str(m.team_id),
                "user_id":    str(m.user_id),
                "experience": m.experience,
                "skills":     skills,
                "skill_text": skill_text,
            })
 
        return result

    async def report_first_response_time(self) -> dict:
        result = await self._session.execute(
            select(
                func.avg(
                    func.extract(
                        "epoch", Ticket.first_response_at - Ticket.created_at
                    )
                ).label("avg_seconds"),
                func.percentile_cont(0.5)
                .within_group(
                    func.extract(
                        "epoch", Ticket.first_response_at - Ticket.created_at
                    )
                )
                .label("median_seconds"),
            ).where(Ticket.first_response_at.isnot(None))
        )
        row = result.one()
        return {
            "average_first_response_time_min": round(
                (row.avg_seconds or 0) / 60, 2
            ),
            "median_first_response_time_min": round(
                (row.median_seconds or 0) / 60, 2
            ),
        }

    async def report_tickets_by_product(self, admin_token: str) -> dict:
        result = await self._session.execute(
            select(
                Ticket.product_id,
                func.count(Ticket.id).label("total"),
                func.count(Ticket.resolved_at).label("resolved"),
                func.avg(
                    func.extract(
                        "epoch", Ticket.resolved_at - Ticket.created_at
                    )
                ).label("avg_resolution_seconds"),
            )
            .where(Ticket.product_id.isnot(None))
            .group_by(Ticket.product_id)
        )
        rows = result.all()
        try:
            products = await self._auth_client.get_products(admin_token)
            product_name_map: dict[str, str] = {
                p["id"]: p["name"] for p in products
            }
        except Exception as exc:
            logger.warning("report_tickets_by_product_http_failed", error=str(exc))
            product_name_map = {}
        return {
            "tickets_by_product": [
                {
                    "product_id":              str(row.product_id),
                    "product_name":            product_name_map.get(
                        str(row.product_id), "unknown"
                    ),
                    "total":                   row.total,
                    "resolved":                row.resolved,
                    "avg_resolution_time_min": round(
                        (row.avg_resolution_seconds or 0) / 60, 2
                    ),
                }
                for row in rows
            ]
        }