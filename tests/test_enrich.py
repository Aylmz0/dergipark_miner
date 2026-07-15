"""src/enrich.py + src/storage.py insert_citation testleri."""

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
