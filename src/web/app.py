"""Dergipark miner web dashboard (read-only monitoring panel).

FastAPI + Jinja2 + HTMX frontend over the miner's SQLite database.
Safe to run while the miner writes (WAL mode).

Usage:
    uvicorn src.web.app:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
import time
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
STATIC_DIR = ROOT / "static"
TEMPLATE_DIR = ROOT / "src" / "web" / "templates"

PAGE_SIZE = 50

# Fold target -> source chars that fold onto it (for snippet <mark> regexes).
_FOLD_REGEX_SOURCES = {
    "i": "İıIî",
    "s": "SşŞ",
    "g": "GğĞ",
    "u": "UüÜû",
    "o": "OöÖ",
    "c": "CçÇ",
    "a": "AâÂ",
}
_MATCH_LABELS = [
    ("title", "Başlık"),
    ("abstract", "Özet"),
    ("keywords", "Anahtar kelime"),
    ("article_type", "Tür"),
    ("publisher", "Yayıncı"),
    ("doi", "DOI"),
    ("issn", "ISSN"),
    ("volume", "Cilt"),
    ("issue", "Sayı"),
    ("pages", "Sayfa"),
    ("oai_identifier", "OAI"),
    ("references", "Kaynakça"),
    ("authors_csv", "Yazar"),
    ("journal_title", "Dergi adı"),
    ("journal_name", "Dergi adı"),
    ("short_name", "Kısa ad"),
]

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


def render(request: Request, name: str, **ctx) -> HTMLResponse:
    return templates.TemplateResponse(request, name, ctx)


def pagination(total: int, page: int) -> dict:
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))
    return {"page": page, "pages": pages, "offset": (page - 1) * PAGE_SIZE, "total": total}


_TR_FOLD_TABLE = str.maketrans(
    "İIıîŞşĞğÜüûÖöÇçÂâ", "iiii" + "ss" + "gg" + "uuu" + "oo" + "cc" + "aa"
)


def _fold_char(ch: str) -> str:
    return ch.translate(_TR_FOLD_TABLE).lower()


def _search_terms(q: str) -> list[str]:
    return [t for t in q.split() if t]


def _term_pattern(term: str) -> str:
    """Regex matching one search term, Turkish-fold aware (İstanbul ↔ istanbul)."""
    parts = []
    for ch in term:
        folded = _fold_char(ch)
        src = _FOLD_REGEX_SOURCES.get(folded)
        if src:
            parts.append("[" + folded + src + "]")
        else:
            parts.append(re.escape(folded))
    return "".join(parts)


def _mark_snippet(abstract: str | None, q: str, max_len: int = 190) -> str:
    """Snippet around the first term match, matching terms wrapped in <mark>.

    Escapes text before highlighting so tags survive HTML escaping.
    """
    if not abstract:
        return ""
    text = " ".join(abstract.split())
    terms = _search_terms(q)
    if not terms:
        return html.escape(text[:max_len])
    patterns = [_term_pattern(t) for t in terms]
    match = None
    for pat in patterns:
        m = re.search(pat, text)
        if m and (match is None or m.start() < match.start()):
            match = m
    if match is None:
        return html.escape(text[:max_len])
    start = match.start()
    seg_start = max(0, text.rfind(" ", 0, start - 30) if start > 30 else 0)
    seg_end = min(len(text), seg_start + max_len)
    seg = text[seg_start:seg_end].strip()
    escaped = html.escape(seg)
    comb = "|".join(f"({p})" for p in patterns)
    out = re.sub(comb, lambda m: f"<mark>{m.group(0)}</mark>", escaped)
    prefix = "… " if seg_start > 0 else ""
    suffix = " …" if seg_end < len(text) else ""
    return prefix + out + suffix


def _apply_match_labels(rows: list[dict], q: str) -> list[dict]:
    """Attach `match_labels` (which fields matched) to each row."""
    if not q:
        return rows
    terms = {queries._fold_text(t) for t in _search_terms(q)}
    for row in rows:
        labels = [label for col, label in _MATCH_LABELS if _row_hits(row, col, terms)]
        row["match_labels"] = labels[:3]
    return rows


def _row_hits(row: dict, col: str, terms: set[str]) -> bool:
    value = row.get(col)
    if value is None:
        return False
    if isinstance(value, list):
        value = " ".join(value)
    folded = queries._fold_text(str(value))
    return any(t in folded for t in terms if t)


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


def _apply_snippets(rows: list[dict], q: str) -> list[dict]:
    """Attach the pre-escaped highlight snippet to each article row."""
    for row in rows:
        row["snippet_html"] = _mark_snippet(row.get("abstract"), q)
    return rows


@app.get("/search", response_class=HTMLResponse)
def search_page(request: Request, q: str = ""):
    q = q.strip()
    if not q:
        return render(
            request, "search.html", q="", journal_rows=[], article_rows=[], author_rows=[]
        )
    t0 = time.perf_counter()
    conn = db_open()
    try:
        journal_rows = queries.search_journals(conn, q, limit=10)
        article_rows = _apply_match_labels(
            _apply_snippets(queries.search_articles(conn, q, limit=15), q), q
        )
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
        elapsed_ms=round((time.perf_counter() - t0) * 1000),
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
        rows = _apply_snippets(
            queries.journal_articles(conn, journal["id"], limit=PAGE_SIZE, offset=pg["offset"]), ""
        )
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
    type: str = "",
    sort: str = "date_desc",
    page: int = 1,
):
    t0 = time.perf_counter()
    conn = db_open()
    try:
        total = queries.count_articles(
            conn, search=q, language=language, is_oa=oa, year=year, pdf=pdf,
            article_type=type,
        )
        pg = pagination(total, page)
        rows = _apply_match_labels(
            _apply_snippets(
                queries.search_articles(
                    conn,
                    search=q,
                    language=language,
                    is_oa=oa,
                    year=year,
                    pdf=pdf,
                    article_type=type,
                    sort=sort,
                    limit=PAGE_SIZE,
                    offset=pg["offset"],
                ),
                q,
            ),
            q,
        )
        lang_opts = queries.languages(conn)
        year_opts = queries.years(conn)
        type_opts = queries.article_types(conn)
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
        "type": type,
        "sort": sort,
        "lang_opts": lang_opts,
        "year_opts": year_opts,
        "type_opts": type_opts,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000),
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
                    ("type", type),
                    ("sort", sort),
                )
                if v
            }
        )
        return render(
            request,
            "partials/article_rows.html",
            rows=rows,
            pg=pg,
            q=q,
            elapsed_ms=ctx["elapsed_ms"],
            base_url=base_url,
            target="#article-rows",
        )
    return render(request, "articles.html", **ctx)


def _parse_keywords(value) -> list[str]:
    """Parse the keywords column (JSON list or comma string)."""
    if not value:
        return []
    items: list[str] = []
    if isinstance(value, list):
        items = [str(k) for k in value]
    else:
        text = value.strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    items = [str(k) for k in parsed]
            except json.JSONDecodeError:
                items = []
        if not items:
            items = [k.strip() for k in re.split(r"[,;]", text) if k.strip()]
    out: list[str] = []
    for item in items:
        out.extend(k.strip() for k in re.split(r"[,;]", item) if k.strip())
    return out


@app.get("/articles/{article_id}", response_class=HTMLResponse)
def article_detail_page(request: Request, article_id: int):
    conn = db_open()
    try:
        article = queries.get_article(conn, article_id)
        if article is None:
            raise HTTPException(status_code=404, detail="Makale bulunamadı")
        article["_keyword_list"] = _parse_keywords(article.get("keywords"))
        similar = _apply_snippets(
            queries.similar_articles(conn, article_id, article["_keyword_list"]),
            "",
        )
    finally:
        conn.close()
    if article.get("oai_identifier") and "/" in article["oai_identifier"]:
        article["article_url"] = (
            f"https://dergipark.org.tr/tr/pub/{article['short_name']}/"
            f"article/{article['oai_identifier'].split('/')[-1]}"
        )
    else:
        article["article_url"] = None
    return render(request, "article_detail.html", article=article, similar=similar)


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
