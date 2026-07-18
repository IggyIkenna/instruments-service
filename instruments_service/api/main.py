"""instruments-service — FastAPI health API.

Exposes /health and /readiness endpoints via UTL make_health_router.

HTTP lifecycle (STARTED / STOPPED / FAILED) uses ``fastapi_uei_lifespan`` — same sink rules as
``ServiceBootstrap`` for batch/live CLIs (see unified-trading-library ``http_lifecycle.py``).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI
from unified_trading_library import (
    fastapi_uei_lifespan,
    make_health_router,
)

_last_processed_date: date | None = None


def set_last_processed_date(d: date) -> None:
    """Set last processed date."""
    global _last_processed_date
    _last_processed_date = d


def _data_freshness() -> dict[str, object]:
    if _last_processed_date is None:
        return {"last_processed_date": None, "stale": True}
    return {"last_processed_date": _last_processed_date.isoformat(), "stale": False}


@asynccontextmanager
async def _lifespan(app: FastAPI):
    async with fastapi_uei_lifespan(
        "instruments-service",
        entrypoint="instruments_service.api.main",
    ):
        yield


def create_app() -> FastAPI:
    """Create app."""
    app = FastAPI(
        title="instruments-service",
        version="0.1.117",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=_lifespan,
    )
    health_router = make_health_router(
        service_name="instruments-service",
        version="0.1.117",
        data_freshness=_data_freshness,
    )
    app.include_router(health_router)
    return app


app = create_app()
