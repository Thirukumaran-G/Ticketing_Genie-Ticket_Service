import uvicorn

from src.api.rest.app import create_app
from src.config.settings import settings
from src.core.sse.redis_subscriber import start_redis_listener


app = create_app()

start_redis_listener(app)


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=False,
        log_level=settings.LOG_LEVEL.lower(),
    )