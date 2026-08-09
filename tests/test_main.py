"""Unit tests for main.py functions."""

import json
import sqlite3

import pytest

from main import _update_article_metadata
from src.storage import init_db


@pytest.fixture
def conn():
    """Create a fresh in-memory SQLite database with schema."""
    conn = sqlite3.connect(":memory:")
    conn = init_db(":memory:")
    # Insert a test journal and article
    conn.execute("INSERT INTO journals (short_name, name) VALUES (?, ?)", ("testjournal", "Test Journal"))
    journal_id = conn.execute("SELECT id FROM journals WHERE short_name = ?", ("testjournal",)).fetchone()[0]
    conn.execute(
        "INSERT INTO articles (journal_id, title, oai_identifier) VALUES (?, ?, ?)",
        (journal_id, "Test Article", "oai:dergipark.org.tr:article/12345"),
    )
    conn.commit()
    yield conn
    conn.close()


def test_update_article_metadata_fills_empty_fields(conn):
    """html_meta values fill empty DB fields."""
    html_meta = {
        "abstract": "This is an abstract",
        "keywords": ["keyword1", "keyword2"],
        "language": "eng",
        "journal_title": "Test Journal Title",
        "issn": "1234-5678",
        "publisher": "Test Publisher",
        "volume": "10",
        "issue": "2",
        "firstpage": "1",
        "lastpage": "20",
        "pages": "1-20",
        "pdf_url": "https://example.com/paper.pdf",
        "orcid": "0000-0000-0000-0000",
        "article_type": "research-article",
        "references": ["Ref 1", "Ref 2"],
    }
    _update_article_metadata(conn, "oai:dergipark.org.tr:article/12345", html_meta)

    row = conn.execute(
        "SELECT abstract, language, journal_title, issn, publisher, volume, issue, "
        "firstpage, lastpage, pages, pdf_url, article_type, is_oa "
        "FROM articles WHERE oai_identifier = ?",
        ("oai:dergipark.org.tr:article/12345",),
    ).fetchone()

    assert row[0] == "This is an abstract"  # abstract
    assert row[1] == "eng"  # language
    assert row[2] == "Test Journal Title"  # journal_title
    assert row[3] == "1234-5678"  # issn
    assert row[4] == "Test Publisher"  # publisher
    assert row[5] == "10"  # volume
    assert row[6] == "2"  # issue
    assert row[7] == "1"  # firstpage
    assert row[8] == "20"  # lastpage
    assert row[9] == "1-20"  # pages
    assert row[10] == "https://example.com/paper.pdf"  # pdf_url
    assert row[11] == "research-article"  # article_type
    assert row[12] == 1  # is_oa (set because pdf_url exists)


def test_update_article_metadata_does_not_overwrite_existing(conn):
    """Existing DB values are NOT overwritten by html_meta."""
    # Set existing values
    conn.execute(
        "UPDATE articles SET abstract = ?, language = ?, journal_title = ? WHERE oai_identifier = ?",
        ("Existing abstract", "tur", "Existing Title", "oai:dergipark.org.tr:article/12345"),
    )
    conn.commit()

    html_meta = {
        "abstract": "New abstract",
        "language": "eng",
        "journal_title": "New Title",
    }
    _update_article_metadata(conn, "oai:dergipark.org.tr:article/12345", html_meta)

    row = conn.execute(
        "SELECT abstract, language, journal_title FROM articles WHERE oai_identifier = ?",
        ("oai:dergipark.org.tr:article/12345",),
    ).fetchone()

    assert row[0] == "Existing abstract"  # NOT overwritten
    assert row[1] == "tur"  # NOT overwritten
    assert row[2] == "Existing Title"  # NOT overwritten


def test_update_article_metadata_empty_oai_id_returns(conn):
    """Empty oai_id returns without error."""
    _update_article_metadata(conn, "", {"abstract": "test"})
    # No crash = pass


def test_update_article_metadata_unknown_oai_id_returns(conn):
    """Unknown oai_id returns without error."""
    _update_article_metadata(conn, "oai:unknown:article/99999", {"abstract": "test"})
    # No crash = pass


def test_update_article_metadata_sets_is_oa_when_pdf_url(conn):
    """is_oa is set to 1 when pdf_url is present in html_meta."""
    _update_article_metadata(conn, "oai:dergipark.org.tr:article/12345", {"pdf_url": "https://example.com/paper.pdf"})

    is_oa = conn.execute(
        "SELECT is_oa FROM articles WHERE oai_identifier = ?",
        ("oai:dergipark.org.tr:article/12345",),
    ).fetchone()[0]
    assert is_oa == 1


def test_update_article_metadata_serializes_keywords(conn):
    """Keywords are stored as JSON string."""
    _update_article_metadata(
        conn,
        "oai:dergipark.org.tr:article/12345",
        {"keywords": ["kw1", "kw2"]},
    )

    kw_json = conn.execute(
        "SELECT keywords FROM articles WHERE oai_identifier = ?",
        ("oai:dergipark.org.tr:article/12345",),
    ).fetchone()[0]
    assert json.loads(kw_json) == ["kw1", "kw2"]


def test_update_article_metadata_stores_full_metadata_dict(conn):
    """Full html_meta dict is stored in metadata column."""
    html_meta = {"abstract": "test", "language": "eng", "pdf_url": "https://example.com/paper.pdf"}
    _update_article_metadata(conn, "oai:dergipark.org.tr:article/12345", html_meta)

    meta_json = conn.execute(
        "SELECT metadata FROM articles WHERE oai_identifier = ?",
        ("oai:dergipark.org.tr:article/12345",),
    ).fetchone()[0]
    meta = json.loads(meta_json)
    assert meta["abstract"] == "test"
    assert meta["language"] == "eng"
