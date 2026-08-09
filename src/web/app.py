"""Dergipark miner web dashboard (read-only monitoring panel).

FastAPI + Jinja2 + HTMX frontend over the miner's SQLite database.
Safe to run while the miner writes (WAL mode).

Usage:
    uvicorn src.web.app:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.web import queries

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "Sobiad_Mining.sqlite"
EXPORT_PATH = ROOT / "data" / "exports" / "all_articles.jsonl"
CHECKPOINT_PATH = ROOT / "checkpoint.json"
PDF_ERRORS_PATH = ROOT / "data" / "pdf_errors.json"
STATIC_DIR = ROOT / "static"
TEMPLATE_DIR = ROOT / "src" / "web" / "templates"

PAGE_SIZE = 50

app = FastAPI(title="Dergipark Miner Panel")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
app.mount("/assets", StaticFiles(directory=str(STATIC_DIR)), name="assets")


def db_open() -> sqlite3.Connection:
    """Fresh read-only connection per request (WAL-safe, no locks)."""
    if not DB_PATH.exists():
        raise HTTPException(status_code=503, detail="Database not initialized yet")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    queries.register(conn)
    return conn


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError, json.JSONDecodeError:
        return default


def _path_display(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def checkpoint_view() -> dict:
    ckpt = _load_json(CHECKPOINT_PATH, {})
    return {
        "processed_shortnames": ckpt.get("processed_shortnames", []),
        "failed_journals": ckpt.get("failed_journals", {}),
        "failed_articles": ckpt.get("failed_articles", []),
        "processed_articles": ckpt.get("processed_articles", 0),
        "pdf_downloaded": ckpt.get("pdf_downloaded", 0),
        "pdf_failed": ckpt.get("pdf_failed", 0),
        "resume_tokens": ckpt.get("resume_tokens", {}),
        "updated_at": _path_display(CHECKPOINT_PATH) if CHECKPOINT_PATH.exists() else None,
    }


def export_view() -> dict:
    try:
        st = EXPORT_PATH.stat()
        return {
            "exists": True,
            "size_mb": round(st.st_size / 1024 / 1024, 1),
            "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        }
    except OSError:
        return {"exists": False, "size_mb": 0, "modified": None}


def pdf_stats() -> dict:
    errors = _load_json(PDF_ERRORS_PATH, [])
    return {"error_count": len(errors), "errors": errors}


def render(request: Request, name: str, **ctx) -> HTMLResponse:
    return templates.TemplateResponse(request, name, ctx)


def pagination(total: int, page: int) -> dict:
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))
    return {"page": page, "pages": pages, "offset": (page - 1) * PAGE_SIZE, "total": total}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = db_open()
    try:
        stats = queries.db_stats(conn)
        recent_rows = queries.search_journals(conn, limit=8, order="recent")
    finally:
        conn.close()
    return render(
        request,
        "dashboard.html",
        stats=stats,
        recent_rows=recent_rows,
        ckpt=checkpoint_view(),
        export_=export_view(),
        pdfs=pdf_stats(),
    )


@app.get("/journals", response_class=HTMLResponse)
def journals_page(request: Request, q: str = "", page: int = 1):
    conn = db_open()
    try:
        total = queries.count_journals(conn, q)
        pg = pagination(total, page)
        rows = queries.search_journals(conn, q, limit=PAGE_SIZE, offset=pg["offset"])
    finally:
        conn.close()
    if request.headers.get("HX-Request"):
        return render(request, "partials/journal_rows.html", rows=rows, pg=pg, q=q)
    return render(request, "journals.html", rows=rows, pg=pg, q=q)


@app.get("/search", response_class=HTMLResponse)
def search_page(request: Request, q: str = ""):
    q = q.strip()
    if not q:
        return render(
            request, "search.html", q="", journal_rows=[], article_rows=[], author_rows=[]
        )
    conn = db_open()
    try:
        journal_rows = queries.search_journals(conn, q, limit=10)
        article_rows = queries.search_articles(conn, q, limit=15)
        author_rows = queries.search_authors(conn, q, limit=10)
        journal_total = queries.count_journals(conn, q)
        article_total = queries.count_articles(conn, q)
        author_total = queries.count_authors(conn, q)
    finally:
        conn.close()
    return render(
        request,
        "search.html",
        q=q,
        journal_rows=journal_rows,
        article_rows=article_rows,
        author_rows=author_rows,
        journal_total=journal_total,
        article_total=article_total,
        author_total=author_total,
    )


@app.get("/journals/{short_name}", response_class=HTMLResponse)
def journal_detail_page(request: Request, short_name: str, page: int = 1):
    conn = db_open()
    try:
        journal = queries.get_journal(conn, short_name)
        if journal is None:
            raise HTTPException(status_code=404, detail="Dergi bulunamadı")
        total = queries.count_journal_articles(conn, journal["id"])
        pg = pagination(total, page)
        rows = queries.journal_articles(conn, journal["id"], limit=PAGE_SIZE, offset=pg["offset"])
    finally:
        conn.close()
    return render(
        request,
        "journal_detail.html",
        journal=journal,
        rows=rows,
        pg=pg,
        base_url=f"/journals/{short_name}?",
        target="#journal-articles",
    )


@app.get("/articles", response_class=HTMLResponse)
def articles_page(
    request: Request,
    q: str = "",
    language: str = "",
    oa: str = "",
    year: str = "",
    pdf: str = "",
    page: int = 1,
):
    conn = db_open()
    try:
        total = queries.count_articles(
            conn, search=q, language=language, is_oa=oa, year=year, pdf=pdf
        )
        pg = pagination(total, page)
        rows = queries.search_articles(
            conn,
            search=q,
            language=language,
            is_oa=oa,
            year=year,
            pdf=pdf,
            limit=PAGE_SIZE,
            offset=pg["offset"],
        )
        lang_opts = queries.languages(conn)
        year_opts = queries.years(conn)
    finally:
        conn.close()
    ctx = {
        "rows": rows,
        "pg": pg,
        "q": q,
        "language": language,
        "oa": oa,
        "year": year,
        "pdf": pdf,
        "lang_opts": lang_opts,
        "year_opts": year_opts,
    }
    if request.headers.get("HX-Request"):
        base_url = "/articles?" + urlencode(
            {
                k: v
                for k, v in (
                    ("q", q),
                    ("language", language),
                    ("oa", oa),
                    ("year", year),
                    ("pdf", pdf),
                )
                if v
            }
        )
        return render(
            request,
            "partials/article_rows.html",
            rows=rows,
            pg=pg,
            base_url=base_url,
            target="#article-rows",
        )
    return render(request, "articles.html", **ctx)


@app.get("/articles/{article_id}", response_class=HTMLResponse)
def article_detail_page(request: Request, article_id: int):
    conn = db_open()
    try:
        article = queries.get_article(conn, article_id)
    finally:
        conn.close()
    if article is None:
        raise HTTPException(status_code=404, detail="Makale bulunamadı")
    if article.get("oai_identifier") and "/" in article["oai_identifier"]:
        article["article_url"] = (
            f"https://dergipark.org.tr/tr/pub/{article['short_name']}/"
            f"article/{article['oai_identifier'].split('/')[-1]}"
        )
    else:
        article["article_url"] = None
    return render(request, "article_detail.html", article=article)


@app.get("/failures", response_class=HTMLResponse)
def failures_page(request: Request):
    return render(request, "failures.html", ckpt=checkpoint_view(), pdfs=pdf_stats())


@app.get("/exports", response_class=HTMLResponse)
def exports_page(request: Request):
    return render(request, "exports.html", export_=export_view())


@app.get("/api/export.jsonl")
def export_jsonl():
    if not EXPORT_PATH.exists():
        raise HTTPException(status_code=404, detail="Export hazır değil")
    return FileResponse(
        EXPORT_PATH, media_type="application/x-ndjson", filename="all_articles.jsonl"
    )


@app.get("/healthz")
def healthz():
    exists = DB_PATH.exists()
    return {"ok": True, "db_exists": exists}


def main() -> None:
    """Run uvicorn for this app: python -m src.web.app [--host H] [--port P]."""
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(prog="dergipark-web")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default 8000)")
    args = parser.parse_args()
    uvicorn.run("src.web.app:app", host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
