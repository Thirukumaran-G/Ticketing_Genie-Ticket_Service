from __future__ import annotations

from dataclasses import dataclass

import httpx

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_BASE = f"{settings.AUTH_SERVICE_URL}/api/v1/auth"
_TIMEOUT = 8.0


@dataclass
class ProductInfo:
    id:          str
    name:        str
    code:        str
    description: str | None


@dataclass
class CompanyInfo:
    company_id:   str
    company_name: str
    domain:       str


@dataclass
class CustomerResult:
    user_id:       str
    email:         str
    full_name:     str
    temp_password: str    # empty string if user already existed
    is_new:        bool


class AuthInboundClient:
    """
    Thin async HTTP client for auth-service internal endpoints
    used exclusively by the email inbound Celery worker.
    """

    # ── Products ──────────────────────────────────────────────────────────────

    async def list_active_products(self) -> list[ProductInfo]:
        """
        Fetch all active products from auth-service.
        Returns empty list on any error — caller must handle.
        """
        url = f"{_BASE}/internal/products/active"
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(url)

            if resp.status_code == 200:
                data = resp.json()
                products = [
                    ProductInfo(
                        id=p["id"],
                        name=p["name"],
                        code=p["code"],
                        description=p.get("description"),
                    )
                    for p in data.get("products", [])
                ]
                logger.info("auth_client_products_fetched", count=len(products))
                return products

            logger.warning(
                "auth_client_products_fetch_failed",
                status=resp.status_code,
            )
        except Exception as exc:
            logger.error("auth_client_products_error", error=str(exc))

        return []

    # ── Company domain lookup ─────────────────────────────────────────────────

    async def get_company_by_domain(self, domain: str) -> CompanyInfo | None:
        """
        Validate that the email domain belongs to a registered active company.
        Returns None if not found or on any error.
        """
        url = f"{_BASE}/internal/companies/by-domain/{domain.lower().strip()}"
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(url)

            if resp.status_code == 200:
                data = resp.json()
                logger.info(
                    "auth_client_company_resolved",
                    domain=domain,
                    company_id=data["company_id"],
                )
                return CompanyInfo(
                    company_id=data["company_id"],
                    company_name=data["company_name"],
                    domain=data["domain"],
                )

            if resp.status_code == 404:
                logger.info("auth_client_domain_not_registered", domain=domain)
                return None

            logger.warning(
                "auth_client_company_lookup_failed",
                domain=domain,
                status=resp.status_code,
            )
        except Exception as exc:
            logger.error("auth_client_company_error", domain=domain, error=str(exc))

        return None

    # ── Customer create-or-get ────────────────────────────────────────────────

    async def create_or_get_customer(
        self,
        email:      str,
        full_name:  str,
        company_id: str,
    ) -> CustomerResult | None:
        url = f"{_BASE}/internal/customers/create-or-get"
        payload = {
            "email":             email.lower().strip(),
            "full_name":         full_name,
            "company_id":        company_id,
            "preferred_contact": "email",
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(url, json=payload)

            if resp.status_code in (200, 201):
                data = resp.json()
                logger.info(
                    "auth_client_customer_resolved",
                    user_id=data["user_id"],
                    is_new=data["is_new"],
                    email=email,
                )
                return CustomerResult(
                    user_id=data["user_id"],
                    email=data["email"],
                    full_name=data["full_name"],
                    temp_password=data["temp_password"],
                    is_new=data["is_new"],
                )

            logger.error(
                "auth_client_customer_create_failed",
                email=email,
                status=resp.status_code,
                body=resp.text[:200],
            )
        except Exception as exc:
            logger.error(
                "auth_client_customer_error",
                email=email,
                error=str(exc),
            )

        return None

    # ── Subscription check ────────────────────────────────────────────────────

    async def verify_subscription(
        self,
        company_id: str,
        product_id: str,
    ) -> bool:
        """
        Check if a company has an active subscription for a product.
        Reuses the existing customer tier endpoint — if it returns 200
        the subscription exists; 404 means no active subscription.
        This avoids needing a new endpoint since tier lookup already
        validates the subscription internally.
        """
        url = (
            f"{_BASE}/internal/customers/subscription-check"
            f"?company_id={company_id}&product_id={product_id}"
        )
        # ── We use a direct DB cross-schema query in the worker instead.
        # This method is a placeholder — see email_inbound_worker._verify_subscription
        # which queries auth.company_product_subscription directly (same DB).
        raise NotImplementedError(
            "Use _verify_subscription() in email_inbound_worker — "
            "direct cross-schema DB query is faster than an HTTP round-trip here."
        )