"""crawler.py için birim testleri (httpx.MockTransport ile)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import httpx

from crawler import fetch_journal_shortnames

SAMPLE_HTML = """
<html>
  <body>
    <a href="/tr/pub/test-dergi">Test Dergi</a>
    <a href="/tr/pub/page/faq">SSS</a>
    <a href="/en/pub/baska">Baska</a>
  </body>
</html>
"""


def _mock_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=SAMPLE_HTML)


def test_fetch_journal_shortnames_extracts_slug():
    transport = httpx.MockTransport(_mock_handler)
    slugs: list[str] = asyncio.run(
        _run(transport)
    )

    assert isinstance(slugs, list)
    assert "test-dergi" in slugs
    assert "faq" not in slugs
    assert "baska" not in slugs


async def _run(transport: httpx.MockTransport) -> list[str]:
    async with httpx.AsyncClient(transport=transport) as client:
        return await fetch_journal_shortnames(client, max_pages=1)
