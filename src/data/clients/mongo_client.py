from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from src.config.settings import settings

_mongo_client: AsyncIOMotorClient[Any] | None = None


def get_mongo_client() -> AsyncIOMotorClient[Any]:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(
            settings.MONGO_URL,
            uuidRepresentation="standard", 
        )
    return _mongo_client


def get_mongo_db() -> AsyncIOMotorDatabase[Any]:
    return get_mongo_client()[settings.MONGO_DB_NAME]


def get_audit_collection()-> AsyncIOMotorCollection[dict[str, Any]]:
    return get_mongo_db()["audit_log"]
