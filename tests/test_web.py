"""Route sanity tests for the web dashboard.

Uses a temporary SQLite DB and monkeypatched file paths, so the real
miner data files are never touched.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from src.storage import init_db, insert_article, insert_journals
from src.web import app as web_app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "test.sqlite"
    conn = init_db(db)
    insert_journals(conn, [{"short_name": "abantmedj", "name": "Abant Medical Journal"}])
    for i in range(3):
        insert_article(
            conn,
            {
                "title": f"Makale {i}",
                "journal_shortname": "abantmedj",
                "journal_name": "Abant Medical Journal",
                "publication_date": f"202{i}",
                "oai_identifier": f"oai:dergipark.org.tr:article/10{i}",
                "doi": f"10.1000/{i}",
                "authors": ["Yazar A", "Yazar B"],
                "abstract": f"Özet {i}",
                "keywords": ["tıp", "sağlık"],
                "language": "tur",
                "pdf_url": f"https://dergipark.org.tr/pdf/{i}",
                "is_oa": 1 if i % 2 == 0 else 0,
            },
        )
    # insert_article does not write is_oa/pdf_url (enrichment does); simulate it
    conn.execute(
        "UPDATE articles SET is_oa = 1, pdf_url = 'https://dergipark.org.tr/pdf/x' WHERE id % 2 = 1"
    )
    conn.commit()
    conn.close()

    ckpt = {
        "processed_shortnames": ["abantmedj"],
        "failed_journals": {"acin": "audit: 1 missing"},
        "failed_articles": [],
        "processed_articles": 3,
        "pdf_downloaded": 2,
        "pdf_failed": 1,
        "resume_tokens": {},
    }
    (tmp_path / "checkpoint.json").write_text(json.dumps(ckpt), encoding="utf-8")
    (tmp_path / "pdf_errors.json").write_text(
        json.dumps([{"oai_id": "oai:1", "error": "timeout", "retry_count": 2}]), encoding="utf-8"
    )
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    export_path = export_dir / "all_articles.jsonl"
    export_path.write_text('{"title": "x"}\n', encoding="utf-8")

    monkeypatch.setattr(web_app, "DB_PATH", db)
    monkeypatch.setattr(web_app, "CHECKPOINT_PATH", tmp_path / "checkpoint.json")
    monkeypatch.setattr(web_app, "PDF_ERRORS_PATH", tmp_path / "pdf_errors.json")
    monkeypatch.setattr(web_app, "EXPORT_PATH", export_path)

    with TestClient(web_app.app) as c:
        yield c


def test_healthz(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["db_exists"] is True


def test_dashboard(client):
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.text
    assert "Toplam Dergi" in body
    assert "3" in body


def test_journals_page(client):
    resp = client.get("/journals")
    assert resp.status_code == 200
    assert "abantmedj" in resp.text


def test_journals_search(client):
    resp = client.get("/journals?q=abant")
    assert resp.status_code == 200
    assert "abantmedj" in resp.text


def test_journals_empty_search(client):
    resp = client.get("/journals?q=zzzz")
    assert resp.status_code == 200
    assert "Sonuç bulunamadı" in resp.text


def test_journal_detail(client):
    resp = client.get("/journals/abantmedj")
    assert resp.status_code == 200
    assert "Makaleler" in resp.text
    assert "Makale 0" in resp.text


def test_journal_detail_404(client):
    resp = client.get("/journals/yokolan")
    assert resp.status_code == 404


def test_articles_page(client):
    resp = client.get("/articles")
    assert resp.status_code == 200
    assert "Makale 1" in resp.text


def test_articles_search(client):
    resp = client.get("/articles?q=Makale 2")
    assert resp.status_code == 200
    assert "Makale 2" in resp.text
    assert "Makale 1" not in resp.text


def test_articles_search_turkish_fold(client):
    resp = client.get("/articles?q=makale")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text


def test_articles_search_author(client):
    resp = client.get("/articles?q=Yazar")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text


def test_articles_search_doi(client):
    resp = client.get("/articles?q=10.1000/2")
    assert resp.status_code == 200
    assert "Makale 2" in resp.text


def test_articles_search_wildcard_literal(client):
    resp = client.get("/articles?q=%25Makale")
    assert resp.status_code == 200
    assert "Sonuç bulunamadı" in resp.text


def test_global_search_all(client):
    resp = client.get("/search?q=Yazar")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text
    assert "Yazar" in resp.text


def test_global_search_journal(client):
    resp = client.get("/search?q=abant")
    assert resp.status_code == 200
    assert "Abant Medical" in resp.text


def test_global_search_empty(client):
    resp = client.get("/search")
    assert resp.status_code == 200
    assert "Türkçe karakter duyarsız" in resp.text


def test_global_search_author_row(client):
    resp = client.get("/search?q=Yazar A")
    assert resp.status_code == 200
    assert "3 makale" in resp.text


def test_articles_filters(client):
    resp = client.get("/articles?oa=1")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text
    assert "Makale 1" not in resp.text


def test_articles_htmx_partial(client):
    resp = client.get("/articles?page=1", headers={"HX-Request": "true"})
    assert resp.status_code == 200
    assert "Makale 0" in resp.text


def test_article_detail(client):
    resp = client.get("/articles/2")
    assert resp.status_code == 200
    assert "Yazar A" in resp.text
    assert "Özet 1" in resp.text


def test_article_detail_404(client):
    resp = client.get("/articles/99999")
    assert resp.status_code == 404


def test_failures_page(client):
    resp = client.get("/failures")
    assert resp.status_code == 200
    assert "audit: 1 missing" in resp.text
    assert "timeout" in resp.text


def test_exports_page(client):
    resp = client.get("/exports")
    assert resp.status_code == 200
    assert "all_articles.jsonl" in resp.text


def test_export_download(client):
    resp = client.get("/api/export.jsonl")
    assert resp.status_code == 200
    assert resp.content == b'{"title": "x"}\n'


def test_missing_db_returns_503(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "DB_PATH", tmp_path / "nope.sqlite")
    with TestClient(web_app.app) as c:
        resp = c.get("/")
    assert resp.status_code == 503
