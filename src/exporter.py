"""JSONL export — single file, all journals."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from loguru import logger


def _normalize_doi(doi: str | None) -> str | None:
    """Remove https://doi.org/ or http://doi.org/ prefix from a DOI.

    Args:
        doi: DOI string or None.

    Returns:
        Normalized DOI string, or None if input was None/empty.
    """
    if not doi:
        return None
    return doi.removeprefix("https://doi.org/").removeprefix("http://doi.org/")


def _json_loads(text: str | None) -> list:
    """Deserialize a JSON string to list, returning [] on failure."""
    if not text:
        return []
    try:
        result = json.loads(text)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _build_article_row(conn: sqlite3.Connection, article: dict) -> dict:
    """Build a JSONL row dict for one article with authors and citations.

    Args:
        conn: SQLite connection.
        article: Dict with article fields (id, title, doi, etc.).

    Returns:
        Dict ready for JSON serialization.
    """
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
        "abstract": article.get("abstract") or "",
        "keywords": _json_loads(article.get("keywords")),
        "language": article.get("language") or "",
        "authors": authors,
        "citations": citations,
    }


def export_all_articles(
    conn: sqlite3.Connection,
    output_path: str | Path,
) -> Path:
    """Export all articles to a single JSONL file.

    Articles are sorted by journal slug, then by ID. Each line contains:
    {"slug": ..., "title": ..., "doi": ..., "date": ..., "abstract": ...,
     "keywords": [...], "language": ..., "authors": [...], "citations": [...]}

    Per-row error handling: if a single article fails to build, it is skipped
    and counted as failed. The export does not crash on bad data.

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
               a.oai_identifier, a.abstract, a.keywords, a.language,
               j.short_name AS journal_short
        FROM articles a
        JOIN journals j ON a.journal_id = j.id
        ORDER BY j.short_name, a.id
        """
    ).fetchall()

    succeeded = 0
    failed = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for article in articles:
            try:
                row = _build_article_row(conn, dict(article))
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                succeeded += 1
            except Exception as exc:
                failed += 1
                logger.warning("Export skip article id={}: {}", article["id"], exc)

    logger.info("Export: {} succeeded, {} failed", succeeded, failed)
    return output_path
