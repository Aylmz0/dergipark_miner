"""crawler.py için birim testleri (httpx.MockTransport ile)."""

import asyncio
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import httpx

from crawler import fetch_journal_shortnames, harvest_all_journals

SAMPLE_HTML = """
<html>
  <body>
    <a href="/tr/pub/test-dergi">Test Dergi</a>
    <a href="/tr/pub/page/faq">SSS</a>
    <a href="/en/pub/baska">Baska</a>
  </body>
</html>
"""

SAMPLE_OAI = """<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-07-14T00:00:00Z</responseDate>
  <request verb="ListRecords">https://dergipark.org.tr/api/public/oai/test-dergi</request>
  <ListRecords>
    <record><header><identifier>a</identifier></header></record>
    <record><header><identifier>b</identifier></header></record>
  </ListRecords>
</OAI-PMH>
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


_conc = {"current": 0, "max": 0, "lock": threading.Lock()}


def _oai_handler(request: httpx.Request) -> httpx.Response:
    with _conc["lock"]:
        _conc["current"] += 1
        _conc["max"] = max(_conc["max"], _conc["current"])
    try:
        return httpx.Response(200, text=SAMPLE_OAI)
    finally:
        with _conc["lock"]:
            _conc["current"] -= 1


def test_harvest_all_journals_returns_all_and_respects_concurrency():
    _conc["current"] = 0
    _conc["max"] = 0
    shortnames = ["dergi-a", "dergi-b", "dergi-c"]
    transport = httpx.MockTransport(_oai_handler)

    result: dict[str, list[str]] = asyncio.run(
        _harvest(transport, shortnames, concurrency=2)
    )

    assert set(result.keys()) == set(shortnames)
    for name in shortnames:
        pages = result[name]
        assert isinstance(pages, list)
        assert len(pages) == 1
        assert pages[0].count("<record>") == 2
    # semaphore concurrency=2 ile sinirlandi, ayni anda en fazla 2 istek
    assert _conc["max"] <= 2


async def _harvest(
    transport: httpx.MockTransport, shortnames: list[str], concurrency: int
) -> dict[str, list[str]]:
    async with httpx.AsyncClient(transport=transport) as client:
        return await harvest_all_journals(
            client, shortnames, concurrency=concurrency, max_records_per_journal=50
        )

