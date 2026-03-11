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

    async def deactivate_sla_rule(self, rule_id: str, admin_id: str) -> None:
        rule = await self._sla_repo.get_by_id(rule_id)
        if not rule:
            raise NotFoundException(f"SLA rule {rule_id} not found.")
        await self._sla_repo.deactivate(rule_id)
        await audit_service.log(
            entity_type="sla_rule",
            entity_id=rule.id,
            action="sla_rule_deactivated",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("sla_rule_deactivated", rule_id=rule_id)

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

    async def delete_severity_priority_map(
        self, map_id: str, admin_id: str
    ) -> None:
        mapping = await self._sev_repo.get_by_id(map_id)
        if not mapping:
            raise NotFoundException(f"Severity priority map {map_id} not found.")
        await self._sev_repo.delete(map_id)
        await audit_service.log(
            entity_type="severity_priority_map",
            entity_id=mapping.id,
            action="severity_priority_map_deleted",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={
                "severity":         mapping.severity,
                "tier_id":          str(mapping.tier_id),
                "derived_priority": mapping.derived_priority,
            },
        )
        await self._session.commit()
        logger.info("severity_priority_map_deleted", map_id=map_id)

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
            new_value={"keyword": payload.keyword, "severity": payload.severity},
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

    async def deactivate_keyword_rule(self, rule_id: str, admin_id: str) -> None:
        rule = await self._kw_repo.get_by_id(rule_id)
        if not rule:
            raise NotFoundException(f"Keyword rule {rule_id} not found.")
        await self._kw_repo.deactivate(rule_id)
        await audit_service.log(
            entity_type="keyword_rule",
            entity_id=rule.id,
            action="keyword_rule_deactivated",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("keyword_rule_deactivated", rule_id=rule_id)

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

    async def deactivate_product_config(
        self, product_id: str, admin_id: str
    ) -> None:
        existing = await self._product_config_repo.get_by_product_id(product_id)
        if not existing:
            raise NotFoundException(
                f"ProductConfig for product {product_id} not found."
            )
        await self._product_config_repo.deactivate_by_product_id(product_id)
        await audit_service.log(
            entity_type="product_config",
            entity_id=existing.id,
            action="product_config_deactivated",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("product_config_deactivated", product_id=product_id)

    # ── Reports ───────────────────────────────────────────────────────────────

    async def report_open_tickets_by_priority(self) -> dict:
        open_statuses = ("new", "acknowledged", "in_progress", "on_hold", "reopened")
        result = await self._session.execute(
            select(Ticket.priority, func.count(Ticket.id).label("count"))
            .where(Ticket.status.in_(open_statuses))
            .group_by(Ticket.priority)
            .order_by(Ticket.priority)
        )
        return {
            "open_tickets_by_priority": [
                {"priority": row.priority or "unset", "count": row.count}
                for row in result.all()
            ]
        }

    async def report_sla_breaches_by_day(self) -> dict:
        result = await self._session.execute(
            select(
                cast(Ticket.sla_breached_at, Date).label("day"),
                func.count(Ticket.id).label("breach_count"),
            )
            .where(Ticket.sla_breached_at.isnot(None))
            .group_by("day")
            .order_by("day")
        )
        return {
            "sla_breaches_by_day": [
                {"day": str(row.day), "breach_count": row.breach_count}
                for row in result.all()
            ]
        }

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