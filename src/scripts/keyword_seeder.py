from __future__ import annotations

import asyncio

from sqlalchemy import text

from src.data.clients.postgres_client import AsyncSessionFactory
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

KEYWORD_DATA: dict[str, list[dict]] = {
    "PROD-001": [  # GTK-FUNDS
        {"keyword": "payment failed",           "severity": "critical"},
        {"keyword": "transaction error",        "severity": "critical"},
        {"keyword": "fund not transferred",     "severity": "critical"},
        {"keyword": "double charge",            "severity": "critical"},
        {"keyword": "ledger mismatch",          "severity": "critical"},
        {"keyword": "outage",                   "severity": "critical"},
        {"keyword": "system down",              "severity": "critical"},
        {"keyword": "unable to pay",            "severity": "high"},
        {"keyword": "payment pending",          "severity": "high"},
        {"keyword": "reconciliation failed",    "severity": "high"},
        {"keyword": "withdrawal failed",        "severity": "high"},
        {"keyword": "slow transfer",            "severity": "medium"},
        {"keyword": "incorrect balance",        "severity": "medium"},
        {"keyword": "wrong amount",             "severity": "medium"},
        {"keyword": "fee question",             "severity": "low"},
        {"keyword": "how to transfer",          "severity": "low"},
        {"keyword": "documentation",            "severity": "low"},
    ],
    "PROD-002": [  # Grocenow
        {"keyword": "order not delivered",      "severity": "critical"},
        {"keyword": "app down",                 "severity": "critical"},
        {"keyword": "checkout failure",         "severity": "critical"},
        {"keyword": "payment not processed",    "severity": "critical"},
        {"keyword": "order cancelled",          "severity": "high"},
        {"keyword": "wrong items",              "severity": "high"},
        {"keyword": "delivery delayed",         "severity": "high"},
        {"keyword": "tracking not working",     "severity": "high"},
        {"keyword": "inventory mismatch",       "severity": "medium"},
        {"keyword": "slow loading",             "severity": "medium"},
        {"keyword": "promo not applied",        "severity": "medium"},
        {"keyword": "missing item",             "severity": "medium"},
        {"keyword": "how to reorder",           "severity": "low"},
        {"keyword": "suggestion",               "severity": "low"},
        {"keyword": "feature request",          "severity": "low"},
    ],
    "PROD-003": [  # Ecommerce-Bot
        {"keyword": "bot not responding",       "severity": "critical"},
        {"keyword": "integration broken",       "severity": "critical"},
        {"keyword": "bot down",                 "severity": "critical"},
        {"keyword": "webhook failure",          "severity": "critical"},
        {"keyword": "wrong response",           "severity": "high"},
        {"keyword": "bot giving errors",        "severity": "high"},
        {"keyword": "order status wrong",       "severity": "high"},
        {"keyword": "returns not handled",      "severity": "high"},
        {"keyword": "slow response",            "severity": "medium"},
        {"keyword": "incorrect recommendation", "severity": "medium"},
        {"keyword": "ui bug",                   "severity": "medium"},
        {"keyword": "how to configure",         "severity": "low"},
        {"keyword": "feature request",          "severity": "low"},
        {"keyword": "documentation",            "severity": "low"},
    ],
}


async def seed_keyword_rules() -> None:
    async with AsyncSessionFactory() as session:
        inserted = 0
        skipped  = 0

        for product_code, keywords in KEYWORD_DATA.items():
            result = await session.execute(
                text("SELECT id FROM auth.product WHERE code = :code"),
                {"code": product_code},
            )
            row = result.fetchone()
            if not row:
                logger.warning("keyword_seeder_product_not_found", code=product_code)
                print(f"Product not found, skipping: {product_code}")
                continue

            product_id = row.id

            for entry in keywords:
                existing = await session.execute(
                    text(
                        """
                        SELECT id FROM ticket.keyword_rule
                        WHERE keyword    = :keyword
                          AND product_id = :product_id
                        """
                    ),
                    {"keyword": entry["keyword"], "product_id": product_id},
                )
                if existing.fetchone():
                    logger.info(
                        "keyword_seeder_skip_existing",
                        keyword=entry["keyword"],
                        product=product_code,
                    )
                    skipped += 1
                    continue

                await session.execute(
                    text(
                        """
                        INSERT INTO ticket.keyword_rule (id, keyword, severity, product_id, is_active)
                        VALUES (gen_random_uuid(), :keyword, :severity, :product_id, TRUE)
                        """
                    ),
                    {
                        "keyword":    entry["keyword"],
                        "severity":   entry["severity"],
                        "product_id": product_id,
                    },
                )
                inserted += 1

        await session.commit()
        logger.info("keyword_seeder_done", inserted=inserted, skipped=skipped)
        print(f"Keyword seeder done — inserted: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(seed_keyword_rules())