from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_exposition_format() -> None:
    from apps.web.main import create_app

    app = create_app(lifespan=None)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as c:
        r = await c.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    # At least one chain_indexer_* family should appear.
    assert "chain_indexer_" in r.text


@pytest.mark.asyncio
async def test_middleware_records_api_request_counter() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from apps.web.deps import get_bus, get_db
    from apps.web.main import create_app
    from core.metrics import API_REQUESTS_TOTAL

    app = create_app(lifespan=None)
    # db.session() raises so healthz catches it → db_ok=False.
    # bus.ping() returns False so redis_ok=False.
    # Combined: route returns 503.
    db_mock = MagicMock()
    db_mock.session = MagicMock(side_effect=RuntimeError("no db"))
    bus_mock = MagicMock()
    bus_mock.ping = AsyncMock(return_value=False)
    app.dependency_overrides[get_db] = lambda: db_mock
    app.dependency_overrides[get_bus] = lambda: bus_mock

    # Hit a real route. The healthz route will fail (mocked db), but the
    # middleware still records on the response.
    before = API_REQUESTS_TOTAL.labels(
        method="GET", path="/healthz", status="503",
    )._value.get()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        await c.get("/healthz")

    after = API_REQUESTS_TOTAL.labels(
        method="GET", path="/healthz", status="503",
    )._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_middleware_skips_metrics_endpoint() -> None:
    """A scrape of /metrics must NOT itself be counted."""
    from apps.web.main import create_app
    from core.metrics import API_REQUESTS_TOTAL

    app = create_app(lifespan=None)
    before = API_REQUESTS_TOTAL.labels(
        method="GET", path="/metrics", status="200",
    )._value.get()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        await c.get("/metrics")

    after = API_REQUESTS_TOTAL.labels(
        method="GET", path="/metrics", status="200",
    )._value.get()
    assert after - before == 0
