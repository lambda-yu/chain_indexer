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
