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
    EmailConfigCreateRequest
)

router = APIRouter(prefix="/admin", tags=["Admin — Configuration"])

_AdminActor = Depends(require_role(ROLE_ADMIN))


def _admin_svc(session: AsyncSession = Depends(get_db_session)) -> AdminService:
    return AdminService(session)


def _team_svc(session: AsyncSession = Depends(get_db_session)) -> TeamService:
    return TeamService(session)


def _get_token(request: Request) -> str:
    return request.headers.get("Authorization", "").removeprefix("Bearer ").strip()


# list tiers
@router.get("/tiers", summary="List tiers from auth-service")
async def list_tiers(
    request: Request,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[dict]:
    return await service.list_tiers(_get_token(request))

# list products
@router.get("/products", summary="List products from auth-service")
async def list_products(
    request: Request,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[dict]:
    return await service.list_products(_get_token(request))


# list the email config
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

# update email congig
@router.put(
    "/email-config",
    response_model=list[EmailConfigResponse],
    summary="upsert email config — IMAP_USER and IMAP_PASSWORD only",
)
async def upsert_email_config(
    payload: list[EmailConfigUpdateRequest],
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list[EmailConfigResponse]:
    configs = await service.upsert_email_config(payload, actor.actor_id)
    return [EmailConfigResponse.model_validate(c) for c in configs]

# new email config
@router.post(
    "/email-config",
    response_model=EmailConfigResponse,
    status_code=201,
    summary="Create a new email config entry",
)
async def create_email_config(
    payload: EmailConfigCreateRequest,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> EmailConfigResponse:
    config = await service.create_email_config(
        key=payload.key,
        value=payload.value,
        is_secret=payload.is_secret,
        admin_id=actor.actor_id,
    )
    return EmailConfigResponse.model_validate(config)


# delete email config
@router.delete(
    "/email-config/{key}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Hard delete an email config entry",
)
async def delete_email_config(
    key:     str,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.delete_email_config(key, actor.actor_id)


# get sla rules 
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


# create new sla rule
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


# delete sla rule
@router.delete(
    "/sla-rules/{rule_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Hard delete an SLA rule",
)
async def delete_sla_rule(
    rule_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.hard_delete_sla_rule(str(rule_id), actor.actor_id)


# set severity-priority map
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


# create new severity-priority mapping
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

# delete severity-priority map
@router.delete(
    "/severity-priority-map/{map_id}",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Hard delete a severity → priority mapping",
)
async def delete_severity_priority_map(
    map_id:  uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.hard_delete_severity_priority_map(str(map_id), actor.actor_id)


# keyword rules
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
    summary="Hard delete a keyword rule",
)
async def delete_keyword_rule(
    rule_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> None:
    await service.hard_delete_keyword_rule(str(rule_id), actor.actor_id)


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
    summary="Hard delete product config",
)
async def hard_delete_product_config(
    product_id: uuid.UUID,
    actor:      CurrentActor = _AdminActor,
    service:    AdminService = Depends(_admin_svc),
) -> None:
    await service.hard_delete_product_config(str(product_id), actor.actor_id)


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

@router.get(
    "/reports/dashboard-summary",
    summary="Aggregated dashboard summary stats",
)
async def report_dashboard_summary(
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_dashboard_summary()

@router.get("/reports/tickets-by-severity", summary="Tickets grouped by severity")
async def report_tickets_by_severity(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_tickets_by_severity()

@router.get("/reports/tickets-by-status", summary="Tickets grouped by status")
async def report_tickets_by_status(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_tickets_by_status()

@router.get("/reports/avg-resolution-time", summary="Average resolution time stats")
async def report_avg_resolution_time(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_avg_resolution_time()

@router.get("/reports/sla-breach-by-severity", summary="SLA breaches grouped by severity")
async def report_sla_breach_by_severity(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_sla_breach_by_severity()

@router.get("/reports/tickets-by-day", summary="Tickets created per day (last 30 days)")
async def report_tickets_by_day(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_tickets_created_by_day()

@router.get("/reports/top-companies", summary="Top companies by ticket volume")
async def report_top_companies(
    actor: CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> dict:
    return await service.report_top_companies_by_tickets()



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

@router.get(
    "/teams/{team_id}/members",
    summary="List members of a team",
)
async def list_team_members(
    team_id: uuid.UUID,
    actor:   CurrentActor = _AdminActor,
    service: AdminService = Depends(_admin_svc),
) -> list:
    return await service.list_team_members(str(team_id))


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

