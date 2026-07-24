"""JSONL export — single file, all journals."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def _normalize_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    return doi.removeprefix("https://doi.org/").removeprefix("http://doi.org/")


def _build_article_row(conn: sqlite3.Connection, article: dict) -> dict:
    authors = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM authors WHERE article_id = ? ORDER BY id",
            (article["id"],),
        ).fetchall()
    ]
    citations = []
    for c in conn.execute(
        "SELECT cited_title, cited_doi, cited_openalex_id "
        "FROM citations WHERE citing_article_id = ? ORDER BY id",
        (article["id"],),
    ).fetchall():
        citations.append({"title": c[0], "doi": _normalize_doi(c[1]), "openalex_id": c[2]})

    oai_id = article.get("oai_identifier") or ""
    slug = article["journal_short"]
    article_number = oai_id.split("/")[-1] if "/" in oai_id else ""
    url = f"https://dergipark.org.tr/tr/pub/{slug}/article/{article_number}" if article_number else ""

    pdf_name = oai_id.replace("/", "_") + ".pdf" if oai_id else ""
    pdf_path = Path("data/raw_pdfs") / pdf_name
    pdf_downloaded = pdf_path.exists() if pdf_name else False

    return {
        "slug": slug,
        "title": article["title"],
        "doi": _normalize_doi(article["doi"]),
        "date": article["publication_date"],
        "oai_identifier": oai_id,
        "url": url,
        "pdf_downloaded": pdf_downloaded,
        "authors": authors,
        "citations": citations,
    }


def export_all_articles(
    conn: sqlite3.Connection,
    output_path: str | Path,
) -> Path:
    """Export all articles to a single JSONL file.

    Articles are sorted by journal slug, then by ID. Each line contains:
    {"slug": ..., "title": ..., "doi": ..., "date": ..., "authors": [...], "citations": [...]}

    Args:
        conn: SQLite connection (row_factory will be set to sqlite3.Row).
        output_path: Destination file path.

    Returns:
        Path to the written JSONL file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn.row_factory = sqlite3.Row

    articles = conn.execute(
        """
        SELECT a.id, a.title, a.doi, a.publication_date,
               a.oai_identifier, j.short_name AS journal_short
        FROM articles a
        JOIN journals j ON a.journal_id = j.id
        ORDER BY j.short_name, a.id
        """
    ).fetchall()

    with open(output_path, "w", encoding="utf-8") as f:
        for article in articles:
            row = _build_article_row(conn, dict(article))
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return output_path
