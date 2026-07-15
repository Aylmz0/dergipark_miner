"""src/enrich.py + src/storage.py insert_citation testleri."""

import asyncio

import httpx

from src.enrich import enrich_article_citations, fetch_openalex_work, fetch_openalex_work_by_id
from src.storage import init_db, insert_article, insert_citation


def _make_article(conn, title="Test Article", oai_id="oai:test:1"):
    """Test icut article olusturur ve id dondurur."""
    article = {
        "title": title,
        "doi": "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "journal_name": "Test Journal",
        "oai_identifier": oai_id,
    }
    return insert_article(conn, article)


def _make_client(handler):
    """Mock httpx.AsyncClient olusturur."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, base_url="https://api.openalex.org")


def _ok_handler(request: httpx.Request) -> httpx.Response:
    """Basariyla work JSON donduren handler."""
    body = {
        "id": "https://openalex.org/W123",
        "doi": "https://doi.org/10.1000/test",
        "cited_by_count": 5,
        "referenced_works": [
            "https://openalex.org/W456",
            "https://openalex.org/W789",
        ],
    }
    return httpx.Response(200, json=body)


def _not_found_handler(request: httpx.Request) -> httpx.Response:
    """Bos sonuc donduren handler (results=[])."""
    return httpx.Response(200, json={"results": []})


def _error_handler(request: httpx.Request) -> httpx.Response:
    """500 donduren handler."""
    return httpx.Response(500, text="Internal Server Error")


# ---------------------------------------------------------------------------
# fetch_openalex_work tests
# ---------------------------------------------------------------------------

def test_fetch_openalex_work_by_doi():
    client = _make_client(_ok_handler)
    result = asyncio.run(fetch_openalex_work(client, doi="10.1000/test"))
    assert result is not None
    assert result["id"] == "https://openalex.org/W123"
    assert result["cited_by_count"] == 5
    assert len(result["referenced_works"]) == 2


def test_fetch_openalex_work_by_title():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [_ok_handler(request).json()]})

    client = _make_client(handler)
    result = asyncio.run(fetch_openalex_work(client, title="Test Title"))
    assert result is not None
    assert "referenced_works" in result


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
# fetch_openalex_work_by_id tests
# ---------------------------------------------------------------------------

def test_fetch_openalex_work_by_id_found():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "https://openalex.org/W123",
            "doi": "https://doi.org/10.1000/test",
            "title": "Test Title",
        })

    client = _make_client(handler)
    result = asyncio.run(fetch_openalex_work_by_id(client, "W123"))
    assert result is not None
    assert result["doi"] == "https://doi.org/10.1000/test"
    assert result["title"] == "Test Title"


def test_fetch_openalex_work_by_id_not_found():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="Not Found")

    client = _make_client(handler)
    try:
        result = asyncio.run(fetch_openalex_work_by_id(client, "W999"))
    except Exception:
        result = None
    assert result is None


def test_fetch_openalex_work_by_id_empty_doi():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "https://openalex.org/W456",
            "title": "No DOI Title",
        })

    client = _make_client(handler)
    result = asyncio.run(fetch_openalex_work_by_id(client, "W456"))
    assert result is not None
    assert result["doi"] == ""
    assert result["title"] == "No DOI Title"


# ---------------------------------------------------------------------------
# enrich_article_citations tests
# ---------------------------------------------------------------------------

def test_enrich_article_citations_writes_rows(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, title="Test Article", oai_id="oai:test:enrich1")
    client = _make_client(_ok_handler)

    count = asyncio.run(enrich_article_citations(client, aid, {"doi": "10.1000/test", "title": "Test"}, conn))
    assert count == 2

    rows = conn.execute(
        "SELECT cited_openalex_id FROM citations WHERE citing_article_id = ?", (aid,)
    ).fetchall()
    cited_ids = {r[0] for r in rows}
    assert cited_ids == {"W456", "W789"}
    conn.close()


def test_enrich_article_citations_no_work(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, title="Test Article", oai_id="oai:test:enrich2")
    client = _make_client(_not_found_handler)

    count = asyncio.run(enrich_article_citations(client, aid, {"doi": "10.9999/none", "title": ""}, conn))
    assert count == 0
    assert conn.execute("SELECT COUNT(*) FROM citations").fetchone()[0] == 0
    conn.close()


def test_enrich_article_citations_no_doi_no_title(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, title="Test Article", oai_id="oai:test:enrich3")
    client = _make_client(_ok_handler)

    count = asyncio.run(enrich_article_citations(client, aid, {"doi": "", "title": ""}, conn))
    assert count == 0
    conn.close()


# ---------------------------------------------------------------------------
# insert_citation roundtrip tests
# ---------------------------------------------------------------------------

def test_insert_citation_roundtrip(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, oai_id="oai:test:cite1")

    row_id = insert_citation(
        conn,
        citing_article_id=aid,
        cited_openalex_id="W987654",
        cited_doi="10.1000/test",
        cited_title="Test Cited",
    )
    assert row_id > 0
    row = conn.execute(
        "SELECT citing_article_id, cited_openalex_id, cited_doi, cited_title FROM citations WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert row[0] == aid
    assert row[1] == "W987654"
    assert row[2] == "10.1000/test"
    assert row[3] == "Test Cited"
    conn.close()


def test_insert_citation_dedup(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, oai_id="oai:test:dedup1")

    first = insert_citation(
        conn,
        citing_article_id=aid,
        cited_openalex_id="W111",
        cited_doi="10.1000/a",
        cited_title="A",
    )
    second = insert_citation(
        conn,
        citing_article_id=aid,
        cited_openalex_id="W111",
        cited_doi="10.1000/a",
        cited_title="A",
    )
    assert first == second
    count = conn.execute("SELECT COUNT(*) FROM citations").fetchone()[0]
    assert count == 1
    conn.close()


def test_insert_citation_no_dedup_without_openalex_id(tmp_path):
    conn = init_db(str(tmp_path / "test.db"))
    aid = _make_article(conn, oai_id="oai:test:nodedup")

    insert_citation(conn, citing_article_id=aid, cited_doi="10.1000/x")
    insert_citation(conn, citing_article_id=aid, cited_doi="10.1000/x")
    count = conn.execute("SELECT COUNT(*) FROM citations").fetchone()[0]
    assert count == 2
    conn.close()
