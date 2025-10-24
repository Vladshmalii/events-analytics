from fastapi import FastAPI
from app.api import events, stats
from app.db.postgres import init_db
from app.db.clickhouse import init_clickhouse
from app.db.redis_client import redis_client
from app.middleware.rate_limit import rate_limit_middleware
from app.middleware.logging import logging_middleware
from prometheus_client import make_asgi_app
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

app = FastAPI(title="Events Analytics API")

@app.on_event("startup")
async def startup():
    await redis_client.connect()
    await init_db()
    await init_clickhouse()

@app.on_event("shutdown")
async def shutdown():
    await redis_client.close()

app.middleware("http")(rate_limit_middleware)
app.middleware("http")(logging_middleware)

app.include_router(events.router, tags=["events"])
app.include_router(stats.router, tags=["stats"])

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/health")
async def health():
    return {"status": "healthy"}