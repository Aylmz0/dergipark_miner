"""Unit tests for crawler.py (httpx.MockTransport)."""

import asyncio
import threading

import httpx
import pytest

from src.crawler import (
    download_pdf,
    fetch_journals,
    harvest_all_journals,
    is_permanently_failed,
    load_pdf_errors,
    log_pdf_error,
    parse_article_metadata,
    remove_pdf_error,
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
    journals = asyncio.run(_run_journals(transport))
    slugs = [j["short_name"] for j in journals]

    assert isinstance(slugs, list)
    assert "test-dergi" in slugs
    assert "faq" not in slugs
    assert "baska" not in slugs


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
    # Semaphore with concurrency=2 limits to max 2 concurrent requests
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
          <dc:description>This article examines the influence of fatwa...</dc:description>
          <dc:subject>Islamic Law</dc:subject>
          <dc:subject>Fatwa</dc:subject>
          <dc:language>en</dc:language>
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
    assert meta["journal_name"] == ""
    assert meta["abstract"] == "This article examines the influence of fatwa..."
    assert meta["keywords"] == ["Islamic Law", "Fatwa"]
    assert meta["language"] == "en"


def test_parse_article_metadata_handles_missing_fields():
    meta = parse_article_metadata("<record><metadata><oai_dc:dc/></metadata></record>")

    assert meta["title"] == ""
    assert meta["doi"] == ""
    assert meta["authors"] == []
    assert meta["publication_date"] == ""
    assert meta["journal_shortname"] == ""
    assert meta["oai_identifier"] == ""
    assert meta["journal_name"] == ""
    assert meta["abstract"] == ""
    assert meta["keywords"] == []
    assert meta["language"] == ""


def test_parse_article_metadata_prefers_en_title():
    """When multiple dc:title exist, prefer en-US language."""
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
    """When dc:relation has no /pub/, check dc:source."""
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
    """When dc:relation is missing but dc:source contains /pub/, find shortname."""
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
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(download_pdf(client, "https://example.com/missing.pdf", dest))
    assert not dest.exists()


# ---------------------------------------------------------------------------
# PDF error logging tests
# ---------------------------------------------------------------------------

def test_log_pdf_error_and_load(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404 Not Found")
    errors = load_pdf_errors()
    assert len(errors) == 1
    assert errors[0]["oai_id"] == "oai:test:1"
    assert errors[0]["error"] == "404 Not Found"


def test_log_pdf_error_no_duplicates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404")
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404")
    errors = load_pdf_errors()
    assert len(errors) == 1
    assert errors[0]["retry_count"] == 2  # Called twice


def test_is_permanently_failed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    assert is_permanently_failed("oai:test:1", max_retries=3) is False

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "err")
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "err")
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "err")
    assert is_permanently_failed("oai:test:1", max_retries=3) is True


def test_remove_pdf_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/1.pdf", "err1")
    log_pdf_error("oai:test:2", "https://example.com/2.pdf", "err2")
    assert len(load_pdf_errors()) == 2

    remove_pdf_error("oai:test:1")
    errors = load_pdf_errors()
    assert len(errors) == 1
    assert errors[0]["oai_id"] == "oai:test:2"


def test_log_pdf_error_transient_no_increment(tmp_path, monkeypatch):
    """Transient errors do not increment retry_count."""
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "429 Too Many", is_transient=True)
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "429 Too Many", is_transient=True)
    errors = load_pdf_errors()
    assert errors[0]["retry_count"] == 0


def test_log_pdf_error_permanent_increments(tmp_path, monkeypatch):
    """Permanent errors increment retry_count."""
    monkeypatch.chdir(tmp_path)
    from src.crawler import PDF_ERRORS_PATH
    PDF_ERRORS_PATH.unlink(missing_ok=True)

    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404 Not Found")
    log_pdf_error("oai:test:1", "https://example.com/paper.pdf", "404 Not Found")
    errors = load_pdf_errors()
    assert errors[0]["retry_count"] == 2


# ---------------------------------------------------------------------------
# scrape_article_html_metadata tests
# ---------------------------------------------------------------------------


def test_scrape_article_html_metadata_extracts_all():
    from src.crawler import scrape_article_html_metadata

    html = """<html><head>
    <meta name="citation_abstract" content="This is the abstract." />
    <meta name="citation_keywords" content="keyword1;keyword2;keyword3" />
    <meta name="citation_language" content="en" />
    <meta name="citation_author_orcid" content="0000-0001-2345-6789" />
    <meta name="citation_author_orcid" content="0000-0002-3456-7890" />
    <meta name="DC.Type.articleType" content="Research Article" />
    <meta name="DC.Source.ISSN" content="1234-5678" />
    <meta name="DC.Source.Issue" content="39" />
    <meta name="citation_reference" content="Ref A" />
    <meta name="citation_reference" content="Ref B" />
    <meta name="citation_publisher" content="Springer" />
    <meta name="citation_volume" content="7" />
    <meta name="citation_firstpage" content="1" />
    <meta name="citation_lastpage" content="32" />
    <meta name="DC.Identifier.pageNumber" content="1-32" />
    <meta name="citation_pdf_url" content="/tr/download/article-file/123" />
    <meta name="citation_journal_title" content="Test Journal" />
    </head><body></body></html>"""

    result = scrape_article_html_metadata(html)
    assert result["abstract"] == "This is the abstract."
    assert result["keywords"] == ["keyword1", "keyword2", "keyword3"]
    assert result["language"] == "en"
    assert result["orcid"] == ["0000-0001-2345-6789", "0000-0002-3456-7890"]
    assert result["article_type"] == "Research Article"
    assert result["issn"] == "1234-5678"
    assert result["issue"] == "39"
    assert result["references"] == ["Ref A", "Ref B"]
    assert result["publisher"] == "Springer"
    assert result["volume"] == "7"
    assert result["firstpage"] == "1"
    assert result["lastpage"] == "32"
    assert result["pages"] == "1-32"
    assert result["pdf_url"] == "/tr/download/article-file/123"
    assert result["journal_title"] == "Test Journal"


def test_scrape_article_html_metadata_empty():
    from src.crawler import scrape_article_html_metadata

    result = scrape_article_html_metadata("<html><head></head><body></body></html>")
    assert result["abstract"] == ""
    assert result["keywords"] == []
    assert result["language"] == ""
    assert result["orcid"] == []
    assert result["article_type"] == ""
    assert result["issn"] == ""
    assert result["issue"] == ""
    assert result["references"] == []
    assert result["publisher"] == ""
    assert result["volume"] == ""
    assert result["firstpage"] == ""
    assert result["lastpage"] == ""
    assert result["pages"] == ""
    assert result["pdf_url"] == ""
    assert result["journal_title"] == ""


def test_scrape_article_html_metadata_citation_issn_fallback():
    from src.crawler import scrape_article_html_metadata

    html = """<html><head>
    <meta name="citation_issn" content="9999-0000" />
    </head><body></body></html>"""
    result = scrape_article_html_metadata(html)
    assert result["issn"] == "9999-0000"

