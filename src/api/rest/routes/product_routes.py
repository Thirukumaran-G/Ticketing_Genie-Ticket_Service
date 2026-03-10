from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from src.api.rest.dependencies import require_scopes
from src.constants import Scope
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.product_repository import ProductRepository
from src.schemas.ticket_schema import ProductResponse

router = APIRouter(prefix="/customer/products", tags=["Customer — Products"])

def _repo(session: AsyncSession = Depends(get_db_session)) -> ProductRepository:
    return ProductRepository(session)

@router.get("", response_model=list[ProductResponse])
async def list_products(
    _actor=Depends(require_scopes(Scope.TICKET_READ)),
    repo: ProductRepository = Depends(_repo),
) -> list[ProductResponse]:
    return [ProductResponse.model_validate(p) for p in await repo.list_active()]