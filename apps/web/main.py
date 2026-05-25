"""FastAPI app factory.

Lifespan opens the DB pool + Redis bus and attaches them to `app.state`.
Routers (added in Tasks 9.2–9.4 and Chunk 10 Task 10.1) live under
`apps/web/routers/` and are included here.

Tests construct `create_app(lifespan=None)` so the lifespan does NOT attempt
real DB/Redis connections; they override `get_db` / `get_bus` via
`app.dependency_overrides` and use `TestClient(app)` WITHOUT the `with`
context manager (which is what would otherwise trigger the lifespan).
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, cast

import structlog
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from apps.web.deps import get_bus, get_db
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.settings import load_settings

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open DB pool and Redis bus on startup; close on shutdown.

    If `bus.connect()` raises after `db.connect()` has succeeded, the DB pool
    must still be released. Both connect calls live inside try-blocks that
    unwind on failure so no resource leaks even on partial startup.
    """
    settings = load_settings()
    db = Database(settings.database.url)
    bus = RedisBus(url=settings.redis.url)
    await db.connect()
    try:
        await bus.connect()
    except BaseException:
        await db.disconnect()
        raise
    app.state.db = db
    app.state.bus = bus
    try:
        yield
    finally:
        await bus.disconnect()
        await db.disconnect()


_LIFESPAN_SENTINEL: Any = object()


def create_app(
    *,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None | Any = _LIFESPAN_SENTINEL,
) -> FastAPI:
    """Build the FastAPI app.

    `lifespan` defaults to the real lifespan defined above. Pass
    `lifespan=None` from tests to bypass DB/Redis startup; pair that with
    `app.dependency_overrides[get_db|get_bus] = ...` so the routes never read
    `app.state`.
    """
    lifespan_arg = globals()["lifespan"] if lifespan is _LIFESPAN_SENTINEL else lifespan
    app = FastAPI(
        title="chain-indexer",
        lifespan=cast(
            "Callable[[FastAPI], AbstractAsyncContextManager[None]] | None",
            lifespan_arg,
        ),
    )

    @app.get("/healthz")
    async def healthz(
        db: Database = Depends(get_db),  # noqa: B008
        bus: RedisBus = Depends(get_bus),  # noqa: B008
    ) -> JSONResponse:
        db_ok = True
        try:
            async with db.session() as s:
                await s.execute(text("SELECT 1"))
        except Exception:  # pragma: no cover (covered by integration test)
            db_ok = False

        redis_ok = await bus.ping()  # RedisBus.ping() swallows errors itself (Chunk 3)

        body = {"db": "ok" if db_ok else "fail", "redis": "ok" if redis_ok else "fail"}
        status_code = 200 if db_ok and redis_ok else 503
        return JSONResponse(body, status_code=status_code)

    # Routers (registered in later tasks of this chunk):
    from apps.web.routers import chains as chains_router  # noqa: E402
    from apps.web.routers import channels as channels_router  # noqa: E402
    from apps.web.routers import subscriptions as subs_router  # noqa: E402

    app.include_router(chains_router.router)
    app.include_router(channels_router.router)
    app.include_router(subs_router.router)
    return app


def main() -> None:  # pragma: no cover — used by the `chain-indexer-web` entrypoint
    import uvicorn

    settings = load_settings()
    uvicorn.run("apps.web.main:create_app", factory=True,
                host=settings.web.host, port=settings.web.port)
