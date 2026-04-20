"""instruments-service — FastAPI health API.

Exposes /health and /readiness endpoints via UTL make_health_router.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI
from unified_trading_library import (
    close_events,
    log_event,
    make_health_router,
    setup_events,
)

_last_processed_date: date | None = None


def set_last_processed_date(d: date) -> None:
    global _last_processed_date
    _last_processed_date = d


def _data_freshness() -> dict[str, object]:
    if _last_processed_date is None:
        return {"last_processed_date": None, "stale": True}
    return {"last_processed_date": _last_processed_date.isoformat(), "stale": False}


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Emit UEI lifecycle markers (STARTED / STOPPED / FAILED) — see codex lifecycle-events."""
    setup_events("instruments-service", "test", sink=None)
    try:
        log_event("STARTED", details={"entrypoint": "instruments_service.api.main"})
        yield
    except BaseException as exc:
        log_event("FAILED", details={"entrypoint": "instruments_service.api.main", "error": repr(exc)[:240]})
        raise
    finally:
        log_event("STOPPED", details={"entrypoint": "instruments_service.api.main"})
        close_events()


def create_app() -> FastAPI:
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
