"""Tests for src/enrich.py (OpenAlex is_oa enrichment only)."""

import asyncio

import httpx

from src.enrich import (
    enrich_all_articles,
    enrich_article_is_oa,
    fetch_openalex_work,
)
from src.storage import init_db, insert_article


def _make_article(conn, title="Test Article", oai_id="oai:test:1", doi=None):
    """Create a test article and return its ID."""
    article = {
        "title": title,
        "doi": doi or "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "journal_name": "Test Journal",
        "oai_identifier": oai_id,
    }
    return insert_article(conn, article)


def _make_client(handler):
    """Create a mock httpx.AsyncClient."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, base_url="https://api.openalex.org")


def _ok_handler(request: httpx.Request) -> httpx.Response:
    """Return a successful work JSON response."""
    body = {
        "id": "https://openalex.org/W123",
        "doi": "https://doi.org/10.1000/test",
        "cited_by_count": 5,
        "referenced_works": [],
    }
    return httpx.Response(200, json=body)


def _not_found_handler(request: httpx.Request) -> httpx.Response:
    """Return empty results response."""
    return httpx.Response(200, json={"results": []})


# ---------------------------------------------------------------------------
# fetch_openalex_work tests
# ---------------------------------------------------------------------------

def test_fetch_openalex_work_by_doi():
    client = _make_client(_ok_handler)
    result = asyncio.run(fetch_openalex_work(client, doi="10.1000/test"))
    assert result is not None
    assert result["id"] == "https://openalex.org/W123"


def test_fetch_openalex_work_by_title():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [_ok_handler(request).json()]})

    client = _make_client(handler)
    result = asyncio.run(fetch_openalex_work(client, title="Test Title"))
    assert result is not None


def test_fetch_openalex_work_not_found():
    client = _make_client(_not_found_handler)
    result = asyncio.run(fetch_openalex_work(client, doi="10.9999/none"))
    assert result is None


def test_fetch_openalex_work_no_params():
    client = _make_client(_ok_handler)
    result = asyncio.run(fetch_openalex_work(client))
    assert result is None


def test_fetch_openalex_work_retry_on_error():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(500, text="Server Error")
        return _ok_handler(request)

    client = _make_client(handler)
    result = asyncio.run(fetch_openalex_work(client, doi="10.1000/test"))
    assert result is not None
    assert call_count == 3


# ---------------------------------------------------------------------------
# enrich_article_is_oa tests
# ---------------------------------------------------------------------------

def test_enrich_article_is_oa_open(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, doi="10.1000/test", oai_id="oai:test:oa1")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "results": [{
                "id": "https://openalex.org/W123",
                "doi": "https://doi.org/10.1000/test",
                "title": "Test",
                "open_access": {"is_oa": True},
            }],
        })

    client = _make_client(handler)
    result = asyncio.run(enrich_article_is_oa(client, aid, "10.1000/test", "Test"))
    assert result == 1
    conn.close()


def test_enrich_article_is_oa_closed(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, doi="10.1000/closed", oai_id="oai:test:oa2")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "results": [{
                "id": "https://openalex.org/W456",
                "doi": "https://doi.org/10.1000/closed",
                "title": "Closed",
                "open_access": {"is_oa": False},
            }],
        })

    client = _make_client(handler)
    result = asyncio.run(enrich_article_is_oa(client, aid, "10.1000/closed", "Closed"))
    assert result == 0
    conn.close()


def test_enrich_article_is_oa_not_found(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, doi="10.9999/none", oai_id="oai:test:oa3")
    client = _make_client(_not_found_handler)

    result = asyncio.run(enrich_article_is_oa(client, aid, "10.9999/none", ""))
    assert result == 0
    conn.close()


def test_enrich_article_is_oa_404(tmp_path):
    """404 errors are handled as debug logs, not exceptions."""
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, doi="10.9999/missing", oai_id="oai:test:oa404")
    client = _make_client(lambda r: httpx.Response(404, text="Not Found"))

    result = asyncio.run(enrich_article_is_oa(client, aid, "10.9999/missing", ""))
    assert result == 0
    conn.close()


# ---------------------------------------------------------------------------
# enrich_all_articles tests
# ---------------------------------------------------------------------------

def test_enrich_all_articles_skips_already_enriched(tmp_path):
    """Articles with enriched_at set are skipped."""
    conn = init_db(str(tmp_path / "test.db"))
    _make_article(conn, oai_id="oai:test:enriched1", doi="10.1111/done")
    _make_article(conn, oai_id="oai:test:notdone1", doi="10.2222/todo")

    conn.execute(
        "UPDATE articles SET enriched_at = datetime('now') WHERE oai_identifier = ?",
        ("oai:test:enriched1",),
    )
    conn.commit()

    call_count = 0

    def handler(request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json={
            "results": [{
                "id": "https://openalex.org/W123",
                "doi": "https://doi.org/10.2222/todo",
                "title": "Test",
                "open_access": {"is_oa": True},
            }],
        })

    client = _make_client(handler)
    result = asyncio.run(enrich_all_articles(client, conn, concurrency=1, batch_size=50))

    assert result["enriched"] == 1
    assert call_count == 1
    conn.close()
