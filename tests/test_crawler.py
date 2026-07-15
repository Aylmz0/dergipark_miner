"""crawler.py için birim testleri (httpx.MockTransport ile)."""

import asyncio
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import httpx

from crawler import fetch_journal_shortnames, harvest_all_journals, parse_article_metadata

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


OAI_PAGE_1 = """<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record><header><identifier>a</identifier></header></record>
    <resumptionToken completeListSize="3" cursor="0">token_page2</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

OAI_PAGE_2 = """<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record><header><identifier>b</identifier></header></record>
    <record><header><identifier>c</identifier></header></record>
  </ListRecords>
</OAI-PMH>
"""

_call_log = []


def _resumption_handler(request: httpx.Request) -> httpx.Response:
    url = str(request.url)
    _call_log.append(url)
    if "resumptionToken=token_page2" in url:
        return httpx.Response(200, text=OAI_PAGE_2)
    return httpx.Response(200, text=OAI_PAGE_1)


def test_harvest_follows_resumption_token():
    _call_log.clear()
    transport = httpx.MockTransport(_resumption_handler)

    result: dict[str, list[str]] = asyncio.run(
        _harvest(transport, ["dergi-x"], concurrency=1)
    )

    pages = result["dergi-x"]
    assert len(pages) == 2
    assert pages[0].count("<record>") == 1
    assert pages[1].count("<record>") == 2
    assert len(_call_log) == 2
    assert "resumptionToken=token_page2" in _call_log[1]


SAMPLE_RECORD_XML = """<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <request verb="ListRecords">https://dergipark.org.tr/api/public/oai/ihad</request>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:dergipark.org.tr:article/1057489</identifier>
        <datestamp>2026-05-13</datestamp>
      </header>
      <metadata>
        <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:title xml:lang="en-US"><![CDATA[The Influence of Fatwa]]></dc:title>
          <dc:title xml:lang="tr-TR"><![CDATA[16. Yüzyılın İlk Yarısında Fetva]]></dc:title>
          <dc:creator><![CDATA[Orğun, Fırat]]></dc:creator>
          <dc:creator><![CDATA[Kumaş, Ali]]></dc:creator>
          <dc:date>2022-01-14</dc:date>
          <dc:identifier>https://dergipark.org.tr/en/pub/ihad/article/1057489</dc:identifier>
          <dc:identifier>https://doi.org/10.29228/ihad.12345</dc:identifier>
          <dc:relation>https://dergipark.org.tr/en/pub/ihad</dc:relation>
        </oai_dc:dc>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>
"""


def test_parse_article_metadata_extracts_fields():
    meta = parse_article_metadata(SAMPLE_RECORD_XML)

    assert meta["title"] == "The Influence of Fatwa"
    assert meta["doi"] == "10.29228/ihad.12345"
    assert meta["authors"] == ["Orğun, Fırat", "Kumaş, Ali"]
    assert meta["publication_date"] == "2022-01-14"
    assert meta["journal_shortname"] == "ihad"
    assert meta["oai_identifier"] == "oai:dergipark.org.tr:article/1057489"


def test_parse_article_metadata_handles_missing_fields():
    meta = parse_article_metadata("<record><metadata><oai_dc:dc/></metadata></record>")

    assert meta["title"] == ""
    assert meta["doi"] == ""
    assert meta["authors"] == []
    assert meta["publication_date"] == ""
    assert meta["journal_shortname"] == ""
    assert meta["oai_identifier"] == ""


def test_parse_article_metadata_prefers_en_title():
    """Birden fazla dc:title varsa en-US olanı tercih et."""
    xml = """<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <metadata>
            <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/">
              <dc:title xml:lang="tr-TR"><![CDATA[Turkce Baslik]]></dc:title>
              <dc:title xml:lang="en-US"><![CDATA[English Title]]></dc:title>
              <dc:relation>https://dergipark.org.tr/en/pub/ornak</dc:relation>
            </oai_dc:dc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """
    meta = parse_article_metadata(xml)
    assert meta["title"] == "English Title"
    assert meta["journal_shortname"] == "ornak"


def test_parse_article_metadata_falls_back_to_source():
    """dc:relation'da /pub/ yoksa dc:source'a bak."""
    xml = """<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <metadata>
            <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/">
              <dc:title><![CDATA[Baslik]]></dc:title>
              <dc:source>Deniz Bilimleri Dergisi No. 12</dc:source>
            </oai_dc:dc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """
    meta = parse_article_metadata(xml)
    assert meta["title"] == "Baslik"
    assert meta["journal_shortname"] == ""


def test_parse_article_metadata_source_has_pub_slug():
    """dc:relation'da yok ama dc:source içinde /pub/ varsa shortname bulunur."""
    xml = """<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <metadata>
            <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/">
              <dc:title><![CDATA[Test]]></dc:title>
              <dc:source>https://dergipark.org.tr/en/pub/dergixyz/issue/123</dc:source>
            </oai_dc:dc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """
    meta = parse_article_metadata(xml)
    assert meta["journal_shortname"] == "dergixyz"

