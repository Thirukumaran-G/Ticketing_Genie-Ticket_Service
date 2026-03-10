from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.core.exceptions.base import AppException
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

def setup_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
        logger.warning("app_exception", error_code=exc.error_code, message=exc.message)
        return JSONResponse(
            status_code=exc.status_code,
            content={"error_code": exc.error_code, "message": exc.message, "details": exc.details},
        )

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_exception", error=str(exc))
        return JSONResponse(status_code=500, content={"error_code": "INTERNAL_ERROR", "message": "Unexpected error."})
