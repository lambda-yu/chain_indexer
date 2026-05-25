from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest
import respx

from core.notifier.http import HttpChannel
from core.notifier.retry import RetryAbort, RetryExhausted


@pytest.mark.asyncio
async def test_post_json_success() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"})
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await ch.send({"hello": "world"})
        assert route.called
        sent = json.loads(route.calls.last.request.content)
        assert sent == {"hello": "world"}
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_hmac_signature_is_byte_correct() -> None:
    secret = "topsecret"
    ch = HttpChannel(config={"url": "https://example.com/hook", "hmac_secret": secret})
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(return_value=httpx.Response(200))
            payload = {"x": 1}
            await ch.send(payload)
        req = route.calls.last.request
        body = req.content
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        assert req.headers["X-Signature"] == f"sha256={expected}"
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_5xx_is_retried_then_succeeds() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"}, base_delay=0.0)
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                side_effect=[
                    httpx.Response(503),
                    httpx.Response(502),
                    httpx.Response(200),
                ]
            )
            await ch.send({})
        assert route.call_count == 3
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_5xx_eventually_gives_up() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"}, base_delay=0.0)
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(return_value=httpx.Response(500))
            with pytest.raises(RetryExhausted):
                await ch.send({})
        assert route.call_count == 3
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_4xx_404_not_retried() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"}, base_delay=0.0)
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(return_value=httpx.Response(404))
            with pytest.raises(RetryAbort):
                await ch.send({})
            assert route.call_count == 1
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_4xx_429_is_retried() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"}, base_delay=0.0)
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                side_effect=[httpx.Response(429), httpx.Response(200)]
            )
            await ch.send({})
            assert route.call_count == 2
    finally:
        await ch.stop()
