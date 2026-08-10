"""Route sanity tests for the web dashboard.

Uses a temporary SQLite DB and monkeypatched file paths, so the real
miner data files are never touched.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from src.storage import fts_sync, init_db, insert_article, insert_journals
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
    # Harvested metadata fields (enricher writes these separately)
    conn.execute(
        "UPDATE articles SET article_type='Araştırma Makalesi', publisher='Ankara Üniversitesi',"
        " issn='0901-0000', volume='1', issue='1', pages='1-12' WHERE title='Makale 0'"
    )
    conn.execute("UPDATE articles SET article_type='Derleme' WHERE title='Makale 1'")
    conn.execute("UPDATE articles SET article_type='Kısa Bildiri' WHERE title='Makale 2'")
    conn.execute(
        "UPDATE articles SET abstract='Ankara deprem veri analizi' WHERE title='Makale 1'"
    )
    # Reindex rows touched by raw SQL (mirrors the enricher's fts_sync call)
    for aid, in conn.execute("SELECT id FROM articles").fetchall():
        fts_sync(conn, aid)
    insert_article(
        conn,
        {
            "title": "Ankara University Semineri",
            "journal_shortname": "abantmedj",
            "journal_name": "Abant Medical Journal",
            "publication_date": "2019",
            "oai_identifier": "oai:dergipark.org.tr:article/104",
            "authors": [],
            "abstract": "Sismik araştırma raporu içerir",
            "language": "tur",
        },
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
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    export_path = export_dir / "all_articles.jsonl"
    export_path.write_text('{"title": "x"}\n', encoding="utf-8")

    monkeypatch.setattr(web_app, "DB_PATH", db)
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


def test_articles_search_type(client):
    resp = client.get("/articles?q=Derleme")
    assert resp.status_code == 200
    assert "Makale 1" in resp.text
    assert "Makale 0" not in resp.text


def test_articles_search_publisher(client):
    resp = client.get("/articles?q=%C3%9Cniversitesi")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text
    assert "Makale 1" not in resp.text


def test_articles_search_issn(client):
    resp = client.get("/articles?q=0901-0000")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text
    assert "Makale 1" not in resp.text


def test_articles_search_multiterm_and(client):
    resp = client.get("/articles?q=Ankara%20%C3%9Cniversitesi")
    assert resp.status_code == 200
    assert "Makale 0" in resp.text
    assert "Makale 1" not in resp.text


def test_articles_relevance_rank_title_over_abstract(client):
    resp = client.get("/articles?q=Ankara")
    body = resp.text
    title_pos = body.index("Ankara University Semineri")
    abstract_only_pos = body.index("Ankara deprem") if "Ankara deprem" in body else len(body)
    assert title_pos < abstract_only_pos


def test_articles_snippet_marks_when_q(client):
    resp = client.get("/articles?q=Ankara")
    assert "<mark>" in resp.text


def test_articles_no_mark_without_q(client):
    resp = client.get("/articles")
    assert "<mark>" not in resp.text


def test_articles_match_chips(client):
    resp = client.get("/articles?q=Makale")
    assert "eşleşti" in resp.text or "Başlık" in resp.text


def test_articles_elapsed_ms(client):
    resp = client.get("/articles?q=Ankara")
    assert "ms" in resp.text


def test_article_detail_similar(client):
    resp = client.get("/articles/2")
    assert resp.status_code == 200
    assert "Benzer Makaleler" in resp.text
    assert "Makale 0" in resp.text


def test_articles_type_filter(client):
    resp = client.get("/articles?type=Derleme")
    assert resp.status_code == 200
    assert "Makale 1" in resp.text
    assert "Makale 0" not in resp.text


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
    assert "Ankara deprem" in resp.text


def test_article_detail_404(client):
    resp = client.get("/articles/99999")
    assert resp.status_code == 404


def test_failures_gone(client):
    resp = client.get("/failures")
    assert resp.status_code == 404


def test_exports_page(client):
    resp = client.get("/exports")
    assert resp.status_code == 200
    assert "Makaleler" in resp.text
    assert "/api/export.articles" in resp.text
    assert "btn-paper" in resp.text


def test_export_download(client):
    resp = client.get("/api/export.jsonl")
    assert resp.status_code == 200
    assert resp.content == b'{"title": "x"}\n'


def test_export_articles_jsonl_filters(client):
    resp = client.get("/api/export.articles?language=tur&oa=1")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")
    rows = [json.loads(line) for line in resp.text.strip().splitlines()]
    assert rows and all(r["language"] == "tur" for r in rows)


def test_export_articles_csv_bom(client):
    resp = client.get("/api/export.articles.csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert resp.content.startswith(b"\xef\xbb\xbf")


def test_export_articles_csv_rows(client):
    resp = client.get("/api/export.articles.csv?language=tur")
    body = resp.content.decode("utf-8-sig")
    lines = body.strip().splitlines()
    assert len(lines) == 4  # 3 tur makale + başlıksız 4. si de tur


def test_export_journals_jsonl(client):
    resp = client.get("/api/export.journals")
    assert resp.status_code == 200
    rows = [json.loads(line) for line in resp.text.strip().splitlines()]
    assert len(rows) == 1
    assert rows[0]["short_name"] == "abantmedj"


def test_export_journals_csv(client):
    resp = client.get("/api/export.journals.csv")
    assert resp.status_code == 200
    assert resp.content.startswith(b"\xef\xbb\xbf")
    body = resp.content.decode("utf-8-sig")
    assert "abantmedj" in body


def test_missing_db_returns_503(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "DB_PATH", tmp_path / "nope.sqlite")
    with TestClient(web_app.app) as c:
        resp = c.get("/")
    assert resp.status_code == 503
