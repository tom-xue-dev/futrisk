"""Dashboard hosting (dashboard review L9/L10): security headers on the SPA, gzip for static and API answers,
long-lived caching for hashed assets and ``no-cache`` for ``index.html``; ``/docs`` keeps working."""

from __future__ import annotations

import gzip
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from apigw_support import Harness, hdr, make_gateway, with_keys

from futrisk.common.config import FullConfig
from futrisk.messages.enums import Role
from futrisk.services.api_gateway.app import SPA_CSP, create_app

ASSET = "index-P82DP1qn.js"


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[tuple[Harness, httpx.AsyncClient]]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text('<!doctype html><script type="module" src="./assets/' + ASSET +
                                     '"></script><div id="app"></div>', encoding="utf-8")
    (dist / "assets" / ASSET).write_text("console.log('dashboard');\n" * 200, encoding="utf-8")  # > 1 KiB
    (dist / "robots.txt").write_text("User-agent: *\n", encoding="utf-8")
    app = create_app(harness.gw, static=dist)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                               base_url="http://testserver")
    try:
        yield harness, client
    finally:
        await client.aclose()
        await harness.close()


class TestSpa:
    async def test_security_headers_on_the_dashboard(self, h: tuple[Harness, httpx.AsyncClient]) -> None:
        _, c = h
        for path in ("/", "/index.html", f"/assets/{ASSET}", "/robots.txt"):
            r = await c.get(path)
            assert r.status_code == 200, path
            assert r.headers["content-security-policy"] == SPA_CSP, path
            assert r.headers["x-content-type-options"] == "nosniff" and r.headers["referrer-policy"] == "no-referrer"
        assert "frame-ancestors 'none'" in SPA_CSP and "connect-src 'self' ws: wss:" in SPA_CSP
        # the API keeps its own headers (no SPA policy), Swagger UI (CDN assets) gets none
        api = await c.get("/api/v1/health")
        assert "content-security-policy" not in api.headers and api.headers["referrer-policy"] == "no-referrer"
        docs = await c.get("/docs")
        assert docs.status_code == 200 and "content-security-policy" not in docs.headers

    async def test_cache_headers(self, h: tuple[Harness, httpx.AsyncClient]) -> None:
        _, c = h
        index = await c.get("/")
        assert index.headers["cache-control"] == "no-cache"
        asset = await c.get(f"/assets/{ASSET}")
        assert asset.headers["cache-control"] == "public, max-age=31536000, immutable"
        assert (await c.get("/robots.txt")).headers["cache-control"] == "no-cache"  # not hashed
        again = await c.get(f"/assets/{ASSET}", headers={"If-None-Match": asset.headers["etag"]})
        assert again.status_code == 304 and again.headers["cache-control"] == "public, max-age=31536000, immutable"

    async def test_gzip_for_static_and_api(self, h: tuple[Harness, httpx.AsyncClient]) -> None:
        harness, c = h
        # httpx decodes transparently: check the wire encoding and that the payload survives
        r = await c.get(f"/assets/{ASSET}", headers={"Accept-Encoding": "gzip"})
        assert r.headers["content-encoding"] == "gzip" and r.text.startswith("console.log")
        big = await c.get("/api/v1/system/status", headers=hdr(Role.VIEWER) | {"Accept-Encoding": "gzip"})
        assert big.headers["content-encoding"] == "gzip" and big.json()["ok"] is True
        assert big.headers["x-result-code"] == "OK"  # the envelope headers survive compression
        small = await c.get("/api/v1/health", headers={"Accept-Encoding": "gzip"})
        assert "content-encoding" not in small.headers  # below 1 KiB: sent as is
        raw = await c.get(f"/assets/{ASSET}", headers={"Accept-Encoding": "identity"})
        assert "content-encoding" not in raw.headers
        assert gzip.compress(raw.content)  # sanity: plain bytes when the client does not accept gzip
