from __future__ import annotations

import asyncio
from sentence_transformers import SentenceTransformer

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.clients.postgres_client import AsyncSessionFactory
from src.data.models.postgres.models import Team, TeamMember
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# ── Embedding model ───────────────────────────────────────────────────────────
# all-MiniLM-L6-v2 → 384-dim — same model used across ticket + persona
_embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def _embed(text_: str) -> list[float]:
    return _embed_model.encode(text_, normalize_embeddings=True).tolist()


# ── Team definitions ──────────────────────────────────────────────────────────
# Same 4 teams are seeded for every product in PRODUCT_CODES.
# team_lead_email is resolved to user_id at runtime.

PRODUCT_CODES = ["PROD-001", "PROD-002", "PROD-003"]

TEAMS: list[dict] = [
    {
        "name":            "Product Support Team",
        "team_lead_email": "rudresh.jd2022ai-ds@sece.ac.in",
        "members": [
            {
                "email":      "vimalsrinivasansn@gmail.com",
                "experience": 5,
                "skill_text": (
                    "Product Support Agent focused on diagnosing bugs, UI issues, "
                    "dashboard errors, and broken features. Always collect steps to "
                    "reproduce, environment details. Provide workarounds where possible "
                    "and escalate critical issues immediately to the engineering team."
                ),
            },
            {
                "email":      "larwinj4683@gmail.com",
                "experience": 6,
                "skill_text": (
                    "Product Support Agent focused on handling feature requests and "
                    "roadmap clarifications. Always capture the user's problem, current "
                    "workaround, and business impact. Translate user needs into structured "
                    "product insights and coordinate with Product Managers for evaluation. "
                    "Be consultative, curious, and manage expectations on delivery timelines."
                ),
            },
            {
                "email":      "sylendravinayak@gmail.com",
                "experience": 7,
                "skill_text": (
                    "Product Support Agent focused on product settings, integrations, "
                    "feature toggles, and configuration failures. Guide users through setup, "
                    "diagnose misconfigured workflows, and resolve integration breakdowns "
                    "step by step. Document all configuration changes and escalate when "
                    "issues exceed user permissions. Be clear, structured, and solution-focused."
                ),
            },
        ],
    },
    {
        "name":            "Billing and Finance Team",
        "team_lead_email": "thirukumaran.g2022ai-ds@sece.ac.in",
        "members": [
            {
                "email":      "j.d.rudresh@gmail.com",
                "experience": 5,
                "skill_text": (
                    "Billing Support Agent focused on resolving billing discrepancies, "
                    "incorrect charges, duplicate payments, failed transactions, and payment "
                    "gateway errors. Investigate overcharging issues, perform payment "
                    "reconciliation, and ensure financial accuracy."
                ),
            },
            {
                "email":      "kishoreshankar870@gmail.com",
                "experience": 6,
                "skill_text": (
                    "Billing Support Agent focused on subscription management, refund "
                    "processing, and compliance. Handle plan upgrades, downgrades, "
                    "cancellations, credit adjustments, coupon failures, and invoice "
                    "generation issues. Apply strong knowledge of pricing models, GST, "
                    "and taxation rules."
                ),
            },
        ],
    },
    {
        "name":            "Account Management Team",
        "team_lead_email": "vetrivel.a2022ai-ds@sece.ac.in",
        "members": [
            {
                "email":      "baranekumar56@gmail.com",
                "experience": 5,
                "skill_text": (
                    "Account Support Agent focused on login failures, password recovery, "
                    "account lockouts, two-factor authentication failures, SSO issues, and "
                    "unauthorized access attempts. Resolve session management problems and "
                    "permission errors with strict adherence to authentication security protocols."
                ),
            },
            {
                "email":      "mithunprabha82@gmail.com",
                "experience": 6,
                "skill_text": (
                    "Account Support Agent focused on profile updates, role assignments, "
                    "account merging, account deactivation, data deletion requests, and email "
                    "or username change workflows. Handle user onboarding end-to-end and "
                    "ensure smooth account transitions."
                ),
            },
        ],
    },
    {
        "name":            "Customer Support Team",
        "team_lead_email": "gthirukumaranias96776@gmail.com",
        "members": [
            {
                "email":      "pragateesh.g2022ai-ds@sece.ac.in",
                "experience": 5,
                "skill_text": (
                    "Customer Support Agent capable of resolving common issues such as "
                    "navigation confusion, report access, feature discovery, notification "
                    "settings, and general platform usage questions. Specialist in handling "
                    "general queries, basic troubleshooting, and how-to questions. First "
                    "point of contact for new users and onboarding assistance."
                ),
            },
        ],
    },
]


# ── Resolvers ─────────────────────────────────────────────────────────────────

async def _resolve_product_ids(session: AsyncSession) -> dict[str, str]:
    """product_code → product_id from auth.product."""
    rows = (await session.execute(
        text("SELECT id::text, code FROM auth.product WHERE is_active = TRUE")
    )).fetchall()
    if not rows:
        raise RuntimeError("No products in auth.product — create products first.")
    return {row.code: row.id for row in rows}


async def _resolve_user_ids(session: AsyncSession) -> dict[str, str]:
    """email → user_id from auth.user."""
    rows = (await session.execute(
        text("SELECT id::text, email FROM auth.user WHERE is_active = TRUE")
    )).fetchall()
    return {row.email: row.id for row in rows}


# ── Seeder ────────────────────────────────────────────────────────────────────

async def seed_teams() -> None:
    async with AsyncSessionFactory() as session:
        product_map = await _resolve_product_ids(session)
        user_map    = await _resolve_user_ids(session)

        teams_inserted   = 0
        teams_skipped    = 0
        members_inserted = 0
        members_skipped  = 0

        for product_code in PRODUCT_CODES:
            product_id = product_map.get(product_code)
            if not product_id:
                logger.warning("team_seeder_product_not_found", product_code=product_code)
                continue

            print(f"\n── Product: {product_code} ──")

            for team_def in TEAMS:
                # ── Resolve team lead ─────────────────────────────────────────
                team_lead_id = user_map.get(team_def["team_lead_email"])
                if not team_lead_id:
                    logger.warning(
                        "team_seeder_lead_not_found",
                        email=team_def["team_lead_email"],
                        team=team_def["name"],
                    )
                    # seed team without lead rather than skipping

                # ── Upsert team ───────────────────────────────────────────────
                existing_team = (await session.execute(
                    select(Team).where(
                        Team.name       == team_def["name"],
                        Team.product_id == product_id,
                    )
                )).scalar_one_or_none()

                if existing_team:
                    logger.info(
                        "team_seeder_skip_existing",
                        name=team_def["name"],
                        product=product_code,
                    )
                    teams_skipped += 1
                    team = existing_team
                else:
                    team = Team(
                        name=team_def["name"],
                        product_id=product_id,
                        team_lead_id=team_lead_id,
                    )
                    session.add(team)
                    await session.flush()
                    await session.refresh(team)
                    teams_inserted += 1
                    print(f"  ✓ Team created: {team_def['name']}")
                    logger.info(
                        "team_seeder_inserted",
                        name=team_def["name"],
                        product=product_code,
                    )

                # ── Seed members ──────────────────────────────────────────────
                for member in team_def["members"]:
                    user_id = user_map.get(member["email"])
                    if not user_id:
                        logger.warning(
                            "team_seeder_member_not_found",
                            email=member["email"],
                            team=team_def["name"],
                        )
                        members_skipped += 1
                        continue

                    existing_member = (await session.execute(
                        select(TeamMember).where(
                            TeamMember.team_id == team.id,
                            TeamMember.user_id == user_id,
                        )
                    )).scalar_one_or_none()

                    if existing_member:
                        logger.info(
                            "team_seeder_member_skip_existing",
                            email=member["email"],
                            team=team_def["name"],
                        )
                        members_skipped += 1
                        continue

                    # Generate 384-dim embedding from skill paragraph
                    print(f"    Embedding skill for {member['email']}...")
                    embedding = _embed(member["skill_text"])

                    session.add(TeamMember(
                        team_id=team.id,
                        user_id=user_id,
                        experience=member["experience"],
                        skills={"skill_text": member["skill_text"]},
                        skill_embedding=embedding,
                    ))
                    members_inserted += 1
                    print(f"    ✓ Member added: {member['email']}")
                    logger.info(
                        "team_seeder_member_inserted",
                        email=member["email"],
                        team=team_def["name"],
                        product=product_code,
                    )

        await session.commit()

        print(
            f"\nTeam seeder done — "
            f"teams inserted: {teams_inserted}, skipped: {teams_skipped} | "
            f"members inserted: {members_inserted}, skipped: {members_skipped}"
        )
        logger.info(
            "team_seeder_done",
            teams_inserted=teams_inserted,
            teams_skipped=teams_skipped,
            members_inserted=members_inserted,
            members_skipped=members_skipped,
        )


if __name__ == "__main__":
    asyncio.run(seed_teams())