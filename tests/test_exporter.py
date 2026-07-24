"""Tests for src/exporter.py."""

from __future__ import annotations

import json

import pytest

from src.exporter import export_all_articles
from src.storage import init_db


def _seed_db(conn) -> None:
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (1, 'ihad', 'Islam Hukuku')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (1, 1, 'Makale I', '10.1234/test.1', '2024-01-01', 'oai:dergipark:article/1')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (2, 1, 'Makale II', NULL, '2024-06-15', 'oai:dergipark:article/2')"
    )
    conn.execute("INSERT INTO authors (article_id, name) VALUES (1, 'Yazar, Ali')")
    conn.execute("INSERT INTO authors (article_id, name) VALUES (1, 'Yazar, Veli')")
    conn.execute("INSERT INTO authors (article_id, name) VALUES (2, 'Yazar, Ayşe')")
    conn.commit()


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


def test_export_all_articles(tmp_path, conn):
    _seed_db(conn)
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    assert out.exists()
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_export_all_articles_structure(tmp_path, conn):
    _seed_db(conn)
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    first = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert first["slug"] == "ihad"
    assert first["article_id"] == "oai:dergipark:article/1"
    assert first["article_url"] == "https://dergipark.org.tr/tr/pub/ihad/article/1"
    assert first["title"] == "Makale I"
    assert first["doi"] == "10.1234/test.1"
    assert first["date"] == "2024-01-01"
    assert first["authors"] == ["Yazar, Ali", "Yazar, Veli"]
    assert first["journal_title"] == ""
    assert first["issn"] == ""
    assert first["publisher"] == ""
    assert first["volume"] == ""
    assert first["issue"] == ""
    assert first["firstpage"] == ""
    assert first["lastpage"] == ""
    assert first["pages"] == ""
    assert first["language"] == ""
    assert first["abstract"] == ""
    assert first["is_oa"] is False
    assert first["pdf_url"] is None
    assert first["keywords"] == []
    assert first["orcid"] == []
    assert first["article_type"] == ""
    assert first["references"] == []
    assert first["metadata"] == []


def test_export_all_articles_no_doi(tmp_path, conn):
    _seed_db(conn)
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    second = json.loads(out.read_text(encoding="utf-8").splitlines()[1])
    assert second["doi"] is None


def test_export_all_articles_sorted_by_slug(tmp_path, conn):
    _seed_db(conn)
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (2, 'tip', 'Tip Dergisi')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (3, 2, 'Tip Makalesi', '10.9999/tip', '2024-03-10', 'oai:dergipark:article/3')"
    )
    conn.commit()
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    slugs = [json.loads(line)["slug"] for line in lines]
    assert slugs == ["ihad", "ihad", "tip"]


def test_export_all_articles_empty_db(tmp_path, conn):
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    assert out.exists()
    content = out.read_text(encoding="utf-8").strip()
    assert content == ""


def test_export_all_articles_creates_output_dir(tmp_path, conn):
    _seed_db(conn)
    nested = tmp_path / "deep" / "nested" / "all.jsonl"
    out = export_all_articles(conn, nested)
    assert out.exists()


def test_export_all_articles_unicode(tmp_path, conn):
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (1, 'test', 'Test')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (1, 1, 'Türkçe Makale: ğüşıöç', NULL, '2024-01-01', 'oai:test:1')"
    )
    conn.execute("INSERT INTO authors (article_id, name) VALUES (1, 'Yazar, Şefik')")
    conn.commit()
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["title"] == "Türkçe Makale: ğüşıöç"
    assert row["authors"] == ["Yazar, Şefik"]


def test_export_all_articles_idempotent(tmp_path, conn):
    _seed_db(conn)
    f1 = export_all_articles(conn, tmp_path / "all.jsonl")
    content1 = f1.read_text(encoding="utf-8")
    f2 = export_all_articles(conn, tmp_path / "all.jsonl")
    content2 = f2.read_text(encoding="utf-8")
    assert content1 == content2


def test_export_all_articles_doi_normalized(tmp_path, conn):
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (1, 'test', 'Test')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (1, 1, 'DOI Test', 'https://doi.org/10.1234/abc', '2024-01-01', 'oai:test:1')"
    )
    conn.commit()
    out = export_all_articles(conn, tmp_path / "all.jsonl")
    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["doi"] == "10.1234/abc"
