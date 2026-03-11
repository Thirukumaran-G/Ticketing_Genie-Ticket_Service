from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_ADMIN, require_role
from src.core.services.admin_service import AdminService
from src.core.services.team_service import TeamService
from src.data.clients.postgres_client import get_db_session
from src.schemas.admin_schema import (
    EmailConfigUpdateRequest,
    EmailConfigResponse,
    ProductConfigUpsertRequest,
    ProductConfigResponse,
    SLARuleCreateRequest,
    SLARuleResponse,
    SeverityPriorityMapCreateRequest,
    SeverityPriorityMapResponse,
    KeywordRuleCreateRequest,
    KeywordRuleResponse,
    KeywordRuleUpdateRequest,
    TeamCreateRequest,
    TeamMemberAddRequest,
    TeamMemberResponse,
    TeamResponse,
)

router = APIRouter(prefix="/admin", tags=["Admin — Configuration"])

_AdminActor = Depends(require_role(ROLE_ADMIN))


def _admin_svc(session: AsyncSession = Depends(get_db_session)) -> AdminService:
    return AdminService(session)


def _team_svc(session: AsyncSession = Depends(get_db_session)) -> TeamService:
    return TeamService(session)


def _get_token(request: Request) -> str:
    return request.headers.get("Authorization", "").removeprefix("Bearer ").strip()


# ── Reference data from auth-service ─────────────────────────────────────────

@router.get("/tiers", summary="List tiers from auth-service")
async def list_tiers(
    request: Request,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[dict]:
    return await service.list_tiers(_get_token(request))


@router.get("/products", summary="List products from auth-service")
async def list_products(
    request: Request,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[dict]:
    return await service.list_products(_get_token(request))


# ── Email Config ──────────────────────────────────────────────────────────────

@router.get(
    "/email-config",
    response_model=list[EmailConfigResponse],
    summary="List email config (secrets masked)",
)
async def list_email_config(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[EmailConfigResponse]:
    configs = await service.list_email_config()
    return [EmailConfigResponse.model_validate(c) for c in configs]


@router.put(
    "/email-config",
    response_model=list[EmailConfigResponse],
    summary="Bulk upsert email config — IMAP_USER and IMAP_PASSWORD only",
)
async def upsert_email_config(
    payload: list[EmailConfigUpdateRequest],
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[EmailConfigResponse]:
    configs = await service.upsert_email_config(payload, actor.actor_id)
    return [EmailConfigResponse.model_validate(c) for c in configs]


# ── SLA Rules ─────────────────────────────────────────────────────────────────

@router.get(
    "/sla-rules",
    response_model=list[SLARuleResponse],
    summary="List all active SLA rules",
)
async def list_sla_rules(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[SLARuleResponse]:
    rules = await service.list_sla_rules()
    return [SLARuleResponse.model_validate(r) for r in rules]


@router.post(
    "/sla-rules",
    response_model=SLARuleResponse,
    status_code=201,
    summary="Create or update SLA rule (upsert by tier_id + priority)",
)
async def upsert_sla_rule(
    payload: SLARuleCreateRequest,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> SLARuleResponse:
    rule = await service.upsert_sla_rule(payload, actor.actor_id)
    return SLARuleResponse.model_validate(rule)


@router.delete(
    "/sla-rules/{rule_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Deactivate an SLA rule",
)
async def deactivate_sla_rule(
    rule_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.deactivate_sla_rule(str(rule_id), actor.actor_id)


# ── Severity / Priority Map ───────────────────────────────────────────────────

@router.get(
    "/severity-priority-map",
    response_model=list[SeverityPriorityMapResponse],
    summary="List all severity → priority mappings",
)
async def list_severity_priority_map(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[SeverityPriorityMapResponse]:
    mappings = await service.list_severity_priority_map()
    return [SeverityPriorityMapResponse.model_validate(m) for m in mappings]


@router.post(
    "/severity-priority-map",
    response_model=SeverityPriorityMapResponse,
    status_code=201,
    summary="Upsert severity → priority mapping",
)
async def upsert_severity_priority_map(
    payload: SeverityPriorityMapCreateRequest,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> SeverityPriorityMapResponse:
    mapping = await service.upsert_severity_priority_map(payload, actor.actor_id)
    return SeverityPriorityMapResponse.model_validate(mapping)


@router.delete(
    "/severity-priority-map/{map_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Delete a severity → priority mapping",
)
async def delete_severity_priority_map(
    map_id:  uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.delete_severity_priority_map(str(map_id), actor.actor_id)


# ── Keyword Rules ─────────────────────────────────────────────────────────────

@router.get(
    "/keyword-rules",
    response_model=list[KeywordRuleResponse],
    summary="List all active keyword rules",
)
async def list_keyword_rules(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[KeywordRuleResponse]:
    rules = await service.list_keyword_rules()
    return [KeywordRuleResponse.model_validate(r) for r in rules]


@router.post(
    "/keyword-rules",
    response_model=KeywordRuleResponse,
    status_code=201,
    summary="Create a keyword rule",
)
async def create_keyword_rule(
    payload: KeywordRuleCreateRequest,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> KeywordRuleResponse:
    rule = await service.create_keyword_rule(payload, actor.actor_id)
    return KeywordRuleResponse.model_validate(rule)


@router.patch(
    "/keyword-rules/{rule_id}",
    response_model=KeywordRuleResponse,
    summary="Update keyword or severity",
)
async def update_keyword_rule(
    rule_id: uuid.UUID,
    payload: KeywordRuleUpdateRequest,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> KeywordRuleResponse:
    rule = await service.update_keyword_rule(str(rule_id), payload, actor.actor_id)
    return KeywordRuleResponse.model_validate(rule)


@router.delete(
    "/keyword-rules/{rule_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Deactivate a keyword rule",
)
async def deactivate_keyword_rule(
    rule_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.deactivate_keyword_rule(str(rule_id), actor.actor_id)


# ── Product Config ────────────────────────────────────────────────────────────

@router.get(
    "/product-config",
    response_model=list[ProductConfigResponse],
    summary="List all product configs",
)
async def list_product_configs(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[ProductConfigResponse]:
    configs = await service.list_product_configs()
    return [ProductConfigResponse.model_validate(c) for c in configs]


@router.put(
    "/product-config/{product_id}",
    response_model=ProductConfigResponse,
    summary="Upsert product config",
)
async def upsert_product_config(
    product_id: uuid.UUID,
    payload:    ProductConfigUpsertRequest,
    actor:      CurrentActor = _AdminActor,
    service:    AdminService = Depends(_admin_svc),
) -> ProductConfigResponse:
    config = await service.upsert_product_config(
        str(product_id), payload, actor.actor_id
    )
    return ProductConfigResponse.model_validate(config)


@router.delete(
    "/product-config/{product_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Deactivate product config",
)
async def deactivate_product_config(
    product_id: uuid.UUID,
    actor:      CurrentActor = _AdminActor,
    service:    AdminService = Depends(_admin_svc),
) -> None:
    await service.deactivate_product_config(str(product_id), actor.actor_id)


# ── Reports ───────────────────────────────────────────────────────────────────

@router.get(
    "/reports/open-tickets-by-priority",
    summary="Open tickets grouped by priority",
)
async def report_open_tickets_by_priority(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_open_tickets_by_priority()


@router.get(
    "/reports/sla-breaches-by-day",
    summary="SLA breaches trend grouped by day",
)
async def report_sla_breaches_by_day(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_sla_breaches_by_day()


@router.get(
    "/reports/first-response-time",
    summary="Average and median first response time",
)
async def report_first_response_time(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_first_response_time()


@router.get(
    "/reports/tickets-by-product",
    summary="Ticket stats by product",
)
async def report_tickets_by_product(
    request: Request,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_tickets_by_product(_get_token(request))


# ── Teams ─────────────────────────────────────────────────────────────────────

@router.get(
    "/teams",
    response_model=list[TeamResponse],
    summary="List all active teams",
)
async def list_teams(
    actor:   CurrentActor = _AdminActor,
    service: TeamService  = Depends(_team_svc),
) -> list[TeamResponse]:
    teams = await service.list_teams()
    return [TeamResponse.model_validate(t) for t in teams]


@router.post(
    "/teams",
    response_model=TeamResponse,
    status_code=201,
    summary="Create a team",
)
async def create_team(
    payload: TeamCreateRequest,
    actor:   CurrentActor = _AdminActor,
    service: TeamService  = Depends(_team_svc),
) -> TeamResponse:
    team = await service.create_team(payload, actor.actor_id)
    return TeamResponse.model_validate(team)


@router.delete(
    "/teams/{team_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Deactivate a team",
)
async def deactivate_team(
    team_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: TeamService  = Depends(_team_svc),
) -> None:
    await service.deactivate_team(str(team_id), actor.actor_id)


@router.get(
    "/products/{product_id}/teams",
    response_model=list[TeamResponse],
    summary="Get all active teams for a product",
)
async def list_teams_by_product(
    product_id: uuid.UUID,
    actor:      CurrentActor = _AdminActor,
    service:    TeamService  = Depends(_team_svc),
) -> list[TeamResponse]:
    teams = await service.list_teams_by_product(str(product_id))
    return [TeamResponse.model_validate(t) for t in teams]


# ── Team Members ──────────────────────────────────────────────────────────────

@router.post(
    "/teams/{team_id}/members",
    response_model=TeamMemberResponse,
    status_code=201,
    summary="Add a member to a team — skill text is auto-embedded",
)
async def add_team_member(
    team_id: uuid.UUID,
    payload: TeamMemberAddRequest,
    actor:   CurrentActor = _AdminActor,
    service: TeamService  = Depends(_team_svc),
) -> TeamMemberResponse:
    member = await service.add_member(str(team_id), payload, actor.actor_id)
    return TeamMemberResponse.model_validate(member)


@router.delete(
    "/teams/{team_id}/members/{member_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Remove a member from a team",
)
async def remove_team_member(
    team_id:   uuid.UUID,
    member_id: uuid.UUID,
    actor:     CurrentActor = _AdminActor,
    service:   TeamService  = Depends(_team_svc),
) -> None:
    await service.remove_member(str(member_id), actor.actor_id)