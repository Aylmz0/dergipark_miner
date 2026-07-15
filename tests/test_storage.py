"""storage.py için birim testleri (geçici SQLite ile)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from storage import init_db, insert_article


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
