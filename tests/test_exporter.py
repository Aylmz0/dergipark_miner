"""
Tests for src/exporter.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from exporter import export_all_journals, export_journal_json, export_journals_json
from storage import init_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    conn.execute(
        "INSERT INTO citations (citing_article_id, cited_title, cited_doi, cited_openalex_id) "
        "VALUES (1, 'Referans I', '10.5678/ref1', 'W111')"
    )
    conn.execute(
        "INSERT INTO citations (citing_article_id, cited_title, cited_doi, cited_openalex_id) "
        "VALUES (1, 'Referans II', NULL, 'W222')"
    )
    conn.execute(
        "INSERT INTO citations (citing_article_id, cited_title, cited_doi, cited_openalex_id) "
        "VALUES (2, 'Referans III', '10.5678/ref3', NULL)"
    )
    conn.commit()


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_export_journal_json(tmp_path, conn):
    _seed_db(conn)
    out = export_journal_json(conn, "ihad", tmp_path)
    assert out.exists()
    assert out.name == "ihad.jsonl"
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_export_journal_json_first_article(tmp_path, conn):
    _seed_db(conn)
    out = export_journal_json(conn, "ihad", tmp_path)
    first = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert first["id"] == "oai:dergipark:article/1"
    assert first["title"] == "Makale I"
    assert first["doi"] == "10.1234/test.1"
    assert first["date"] == "2024-01-01"
    assert first["authors"] == ["Yazar, Ali", "Yazar, Veli"]
    assert first["journal"] == "ihad"
    assert len(first["citations"]) == 2


def test_export_journal_json_no_citations(tmp_path, conn):
    _seed_db(conn)
    out = export_journal_json(conn, "ihad", tmp_path)
    second = json.loads(out.read_text(encoding="utf-8").splitlines()[1])
    assert second["doi"] is None
    assert len(second["citations"]) == 1


def test_export_journal_json_unknown_journal(tmp_path, conn):
    _seed_db(conn)
    with pytest.raises(ValueError, match="Dergi bulunamadı"):
        export_journal_json(conn, "nonexistent", tmp_path)


def test_export_all_journals(tmp_path, conn):
    _seed_db(conn)
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (2, 'tip', 'Tip Dergisi')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (3, 2, 'Tip Makalesi', '10.9999/tip', '2024-03-10', 'oai:dergipark:article/3')"
    )
    conn.execute("INSERT INTO authors (article_id, name) VALUES (3, 'Doktor, Mehmet')")
    conn.commit()
    files = export_all_journals(conn, tmp_path)
    assert len(files) == 2
    names = sorted(f.name for f in files)
    assert names == ["ihad.jsonl", "tip.jsonl"]


def test_export_empty_journal(tmp_path, conn):
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (1, 'bos', 'Bos Dergi')"
    )
    conn.commit()
    out = export_journal_json(conn, "bos", tmp_path)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 0


def test_export_creates_output_dir(tmp_path, conn):
    _seed_db(conn)
    nested = tmp_path / "deep" / "nested"
    out = export_journal_json(conn, "ihad", nested)
    assert out.exists()


def test_export_citation_structure(tmp_path, conn):
    _seed_db(conn)
    out = export_journal_json(conn, "ihad", tmp_path)
    first = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    c = first["citations"][0]
    assert c == {"title": "Referans I", "doi": "10.5678/ref1", "openalex_id": "W111"}


def test_export_unicode(tmp_path, conn):
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (1, 'test', 'Test')"
    )
    conn.execute(
        "INSERT INTO articles (id, journal_id, title, doi, publication_date, oai_identifier) "
        "VALUES (1, 1, 'Türkçe Makale: ğüşıöç', NULL, '2024-01-01', 'oai:test:1')"
    )
    conn.execute("INSERT INTO authors (article_id, name) VALUES (1, 'Yazar, Şefik')")
    conn.commit()
    out = export_journal_json(conn, "test", tmp_path)
    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["title"] == "Türkçe Makale: ğüşıöç"
    assert row["authors"] == ["Yazar, Şefik"]


def test_export_idempotent(tmp_path, conn):
    _seed_db(conn)
    f1 = export_journal_json(conn, "ihad", tmp_path)
    content1 = f1.read_text(encoding="utf-8")
    f2 = export_journal_json(conn, "ihad", tmp_path)
    content2 = f2.read_text(encoding="utf-8")
    assert content1 == content2


def test_export_journals_json(tmp_path, conn):
    _seed_db(conn)
    out = export_journals_json(conn, tmp_path)
    assert out.exists()
    assert out.name == "journals.jsonl"
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["short_name"] == "ihad"
    assert row["name"] == "Islam Hukuku"
    assert row["article_count"] == 2


def test_export_journals_json_multiple(tmp_path, conn):
    _seed_db(conn)
    conn.execute(
        "INSERT INTO journals (id, short_name, name) VALUES (2, 'tip', 'Tip Dergisi')"
    )
    conn.commit()
    out = export_journals_json(conn, tmp_path)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    names = [json.loads(l)["short_name"] for l in lines]
    assert names == ["ihad", "tip"]
