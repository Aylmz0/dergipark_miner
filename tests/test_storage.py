"""Unit tests for storage.py (in-memory SQLite)."""

import pytest

from src.storage import init_db, insert_article, insert_journals


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


def test_insert_article_roundtrip(conn):
    article = {
        "title": "The Influence of Fatwa",
        "doi": "10.29228/ihad.12345",
        "authors": ["Orğun, Fırat", "Kumaş, Ali"],
        "publication_date": "2022-01-14",
        "journal_shortname": "ihad",
    }
    article_id = insert_article(conn, article)

    row = conn.execute(
        "SELECT title, doi, publication_date, journal_id FROM articles WHERE id = ?",
        (article_id,),
    ).fetchone()
    assert row[0] == "The Influence of Fatwa"
    assert row[1] == "10.29228/ihad.12345"
    assert row[2] == "2022-01-14"

    journal = conn.execute(
        "SELECT short_name FROM journals WHERE id = ?", (row[3],)
    ).fetchone()
    assert journal[0] == "ihad"


def test_insert_article_upsert_by_doi(conn):
    base = {
        "title": "Eski Baslik",
        "doi": "10.29228/ihad.999",
        "authors": [],
        "publication_date": "2020-01-01",
        "journal_shortname": "ihad",
    }
    first_id = insert_article(conn, base)
    updated = dict(base, title="Yeni Baslik", publication_date="2021-05-05")
    second_id = insert_article(conn, updated)

    assert first_id == second_id
    count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE doi = ?", (base["doi"],)
    ).fetchone()[0]
    assert count == 1
    row = conn.execute(
        "SELECT title, publication_date FROM articles WHERE id = ?", (second_id,)
    ).fetchone()
    assert row[0] == "Yeni Baslik"
    assert row[1] == "2021-05-05"


def test_insert_article_upsert_by_oai_identifier(conn):
    """Duplicate prevention by oai_identifier when DOI is absent."""
    base = {
        "title": "Eski Baslik",
        "doi": "",
        "authors": [],
        "publication_date": "2020-01-01",
        "journal_shortname": "ihad",
        "oai_identifier": "oai:dergipark.org.tr:article/999999",
    }
    first_id = insert_article(conn, base)
    updated = dict(base, title="Yeni Baslik", publication_date="2021-05-05")
    second_id = insert_article(conn, updated)

    assert first_id == second_id
    count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE oai_identifier = ?", (base["oai_identifier"],)
    ).fetchone()[0]
    assert count == 1
    row = conn.execute(
        "SELECT title, publication_date FROM articles WHERE id = ?", (second_id,)
    ).fetchone()
    assert row[0] == "Yeni Baslik"
    assert row[1] == "2021-05-05"


def test_insert_article_writes_authors(conn):
    """insert_article writes authors to authors table."""
    article = {
        "title": "Yazarli Makale",
        "doi": "",
        "authors": ["Yazar, Ali", "Yazar, Veli"],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "oai_identifier": "oai:dergipark.org.tr:article/555555",
    }
    article_id = insert_article(conn, article)

    rows = conn.execute(
        "SELECT name FROM authors WHERE article_id = ? ORDER BY name", (article_id,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "Yazar, Ali"
    assert rows[1][0] == "Yazar, Veli"


def test_insert_article_upsert_replaces_authors(conn):
    """UPSERT deletes old authors and inserts new ones."""
    article = {
        "title": "Makale",
        "doi": "",
        "authors": ["Eski Yazar"],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "oai_identifier": "oai:dergipark.org.tr:article/666666",
    }
    first_id = insert_article(conn, article)

    updated = dict(article, authors=["Yeni Yazar A", "Yeni Yazar B"])
    second_id = insert_article(conn, updated)

    assert first_id == second_id
    rows = conn.execute(
        "SELECT name FROM authors WHERE article_id = ? ORDER BY name", (second_id,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "Yeni Yazar A"
    assert rows[1][0] == "Yeni Yazar B"


def test_insert_article_writes_journal_name(conn):
    """insert_article writes journal name to journals.name column."""
    article = {
        "title": "Test Makale",
        "doi": "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "ihad",
        "journal_name": "Islam Hukuku Arastirmalari Dergisi",
        "oai_identifier": "oai:dergipark.org.tr:article/777777",
    }
    insert_article(conn, article)

    row = conn.execute(
        "SELECT name FROM journals WHERE short_name = 'ihad'"
    ).fetchone()
    assert row[0] == "Islam Hukuku Arastirmalari Dergisi"


def test_insert_article_enriches_existing_journal_name(conn):
    """When existing journal name is empty, fills it from new data."""
    conn.execute("INSERT INTO journals (short_name, name) VALUES ('j1', '')")
    conn.commit()

    article = {
        "title": "Test",
        "doi": "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "j1",
        "journal_name": "Yeni Dergi Adi",
        "oai_identifier": "oai:dergipark.org.tr:article/888888",
    }
    insert_article(conn, article)

    row = conn.execute("SELECT name FROM journals WHERE short_name = 'j1'").fetchone()
    assert row[0] == "Yeni Dergi Adi"


# ---------------------------------------------------------------------------
# insert_journals tests
# ---------------------------------------------------------------------------

def test_insert_journals_adds_new_journals(conn):
    journals = [
        {"short_name": "j1", "name": "Dergi Bir"},
        {"short_name": "j2", "name": "Dergi Iki"},
    ]
    insert_journals(conn, journals)
    rows = conn.execute("SELECT short_name, name FROM journals ORDER BY short_name").fetchall()
    assert len(rows) == 2
    assert rows[0] == ("j1", "Dergi Bir")
    assert rows[1] == ("j2", "Dergi Iki")


def test_insert_journals_skips_existing(conn):
    conn.execute("INSERT INTO journals (short_name, name) VALUES ('j1', 'Eski Ad')")
    conn.commit()
    insert_journals(conn, [{"short_name": "j1", "name": "Yeni Ad"}])
    row = conn.execute("SELECT name FROM journals WHERE short_name = 'j1'").fetchone()
    assert row[0] == "Eski Ad"


def test_insert_journals_fills_empty_name(conn):
    conn.execute("INSERT INTO journals (short_name, name) VALUES ('j1', '')")
    conn.commit()
    insert_journals(conn, [{"short_name": "j1", "name": "Dolugu Isim"}])
    row = conn.execute("SELECT name FROM journals WHERE short_name = 'j1'").fetchone()
    assert row[0] == "Dolugu Isim"


def test_insert_article_content_hash_dedup(conn):
    """DOI ve OAI ID yoksa content_hash ile dedup yapilir."""
    article = {
        "title": "Ayni Baslik",
        "doi": "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "oai_identifier": "",
    }
    id1 = insert_article(conn, article)
    id2 = insert_article(conn, article)
    assert id1 == id2  # ayni makale, ayni id

    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    assert total == 1


def test_insert_article_content_hash_different(conn):
    """Farkli baslik = farkli content_hash = farkli makale."""
    base = {
        "doi": "",
        "authors": [],
        "publication_date": "2024-01-01",
        "journal_shortname": "testjournal",
        "oai_identifier": "",
    }
    id1 = insert_article(conn, {**base, "title": "Makale Bir"})
    id2 = insert_article(conn, {**base, "title": "Makale Iki"})
    assert id1 != id2

    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    assert total == 2
