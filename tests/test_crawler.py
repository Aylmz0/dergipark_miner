"""crawler.py için birim testleri (httpx.MockTransport ile)."""

import asyncio
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import httpx

from crawler import (
    download_pdf,
    fetch_article_pdf_url,
    fetch_journal_shortnames,
    fetch_journals,
    harvest_all_journals,
    load_pdf_errors,
    log_pdf_error,
    parse_article_metadata,
)

SAMPLE_HTML = """
<html>
  <body>
    <nav>
      <a href="/tr/pub/page/faq">SSS</a>
      <a href="/tr/pub/trends">Trendler</a>
    </nav>
    <table>
      <tr><td>
        <h5><a href="/tr/pub/test-dergi">Test Dergi</a></h5>
      </td></tr>
    </table>
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


async def _run_journals(transport: httpx.MockTransport) -> list[dict]:
    async with httpx.AsyncClient(transport=transport) as client:
        return await fetch_journals(client, max_pages=1)


def test_fetch_journals_returns_dicts():
    transport = httpx.MockTransport(_mock_handler)
    journals = asyncio.run(_run_journals(transport))
    assert isinstance(journals, list)
    assert all(isinstance(j, dict) for j in journals)
    assert all("short_name" in j and "name" in j for j in journals)


def test_fetch_journals_extracts_name():
    transport = httpx.MockTransport(_mock_handler)
    journals = asyncio.run(_run_journals(transport))
    test_derji = [j for j in journals if j["short_name"] == "test-dergi"]
    assert len(test_derji) == 1
    assert test_derji[0]["name"] == "Test Dergi"


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
    assert "journal_name" in meta


def test_parse_article_metadata_handles_missing_fields():
    meta = parse_article_metadata("<record><metadata><oai_dc:dc/></metadata></record>")

    assert meta["title"] == ""
    assert meta["doi"] == ""
    assert meta["authors"] == []
    assert meta["publication_date"] == ""
    assert meta["journal_shortname"] == ""
    assert meta["oai_identifier"] == ""
    assert meta["journal_name"] == ""


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


# ---------------------------------------------------------------------------
# fetch_article_pdf_url tests
# ---------------------------------------------------------------------------

def test_fetch_article_pdf_url_finds_download_link():
    def handler(request: httpx.Request) -> httpx.Response:
        html = """
        <html><body>
            <a href="/tr/download/article-file/5788429">PDF Indir</a>
            <a href="/tr/pub/ihad/article/12345">View</a>
        </body></html>
        """
        return httpx.Response(200, text=html)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://dergipark.org.tr")
    result = asyncio.run(fetch_article_pdf_url(client, "https://dergipark.org.tr/en/pub/ihad/article/12345"))
    assert result is not None
    assert "download/article-file" in result


def test_fetch_article_pdf_url_finds_pdf_link():
    def handler(request: httpx.Request) -> httpx.Response:
        html = """
        <html><body>
            <a href="/tr/pub/ihad/article/12345/paper.pdf">PDF</a>
        </body></html>
        """
        return httpx.Response(200, text=html)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://dergipark.org.tr")
    result = asyncio.run(fetch_article_pdf_url(client, "https://dergipark.org.tr/en/pub/ihad/article/12345"))
    assert result is not None
    assert result.endswith(".pdf")


def test_fetch_article_pdf_url_no_pdf_found():
    def handler(request: httpx.Request) -> httpx.Response:
        html = "<html><body><a href='/view'>View</a></body></html>"
        return httpx.Response(200, text=html)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://dergipark.org.tr")
    result = asyncio.run(fetch_article_pdf_url(client, "https://dergipark.org.tr/en/pub/ihad/article/12345"))
    assert result is None


def test_fetch_article_pdf_url_absolute_pdf_link():
    def handler(request: httpx.Request) -> httpx.Response:
        html = '<html><body><a href="https://cdn.example.com/paper.pdf">PDF</a></body></html>'
        return httpx.Response(200, text=html)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://dergipark.org.tr")
    result = asyncio.run(fetch_article_pdf_url(client, "https://dergipark.org.tr/en/pub/ihad/article/12345"))
    assert result == "https://cdn.example.com/paper.pdf"


# ---------------------------------------------------------------------------
# download_pdf tests
# ---------------------------------------------------------------------------

def test_download_pdf_writes_file(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"%PDF-1.4 fake content")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = tmp_path / "test.pdf"
    result = asyncio.run(download_pdf(client, "https://example.com/paper.pdf", dest))
    assert result is True
    assert dest.exists()
    assert dest.read_bytes() == b"%PDF-1.4 fake content"


def test_download_pdf_handles_error(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="Not Found")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = tmp_path / "missing.pdf"
    result = asyncio.run(download_pdf(client, "https://example.com/missing.pdf", dest))
    assert result is False
    assert not dest.exists()


# ---------------------------------------------------------------------------
# PDF error logging tests
# ---------------------------------------------------------------------------

def test_log_pdf_error_and_load(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404 Not Found")
    errors = load_pdf_errors()
    assert len(errors) == 1
    assert errors[0]["oai_id"] == "oai:test:1"
    assert errors[0]["error"] == "404 Not Found"


def test_log_pdf_error_no_duplicates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404")
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404")
    errors = load_pdf_errors()
    assert len(errors) == 1

