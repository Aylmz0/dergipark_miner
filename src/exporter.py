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


def _json_loads(text: str | None) -> list | dict:
    """Deserialize a JSON string, returning [] on failure."""
    if not text:
        return []
    try:
        result = json.loads(text)
        return result if isinstance(result, (list, dict)) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _build_article_row(conn: sqlite3.Connection, article: dict) -> dict:
    """Build a JSONL row dict for one article with authors.

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

    oai_id = article.get("oai_identifier") or ""
    slug = article["journal_short"]
    article_number = oai_id.split("/")[-1] if "/" in oai_id else ""
    url = f"https://dergipark.org.tr/tr/pub/{slug}/article/{article_number}" if article_number else ""

    return {
        "slug": slug,
        "article_id": oai_id,
        "article_url": url,
        "title": article["title"],
        "authors": authors,
        "journal_title": article.get("journal_title") or "",
        "issn": article.get("issn") or "",
        "publisher": article.get("publisher") or "",
        "volume": article.get("volume") or "",
        "issue": article.get("issue") or "",
        "firstpage": article.get("firstpage") or "",
        "lastpage": article.get("lastpage") or "",
        "pages": article.get("pages") or "",
        "date": article["publication_date"],
        "language": article.get("language") or "",
        "doi": _normalize_doi(article["doi"]),
        "abstract": article.get("abstract") or "",
        "is_oa": bool(article.get("is_oa")),
        "pdf_url": article.get("pdf_url") or None,
        "keywords": _json_loads(article.get("keywords")),
        "orcid": _json_loads(article.get("orcid")),
        "article_type": article.get("article_type") or "",
        "references": _json_loads(article.get("references")),
        "metadata": _json_loads(article.get("metadata")),
    }


def export_all_articles(
    conn: sqlite3.Connection,
    output_path: str | Path,
) -> Path:
    """Export all articles to a single JSONL file.

    Articles are sorted by journal slug, then by ID. Each line contains
    all metadata fields: slug, article_id, article_url, title, authors,
    journal_title, issn, publisher, volume, issue, firstpage, lastpage,
    pages, date, language, doi, abstract, is_oa, pdf_url, keywords,
    orcid, article_type, references, metadata.

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
               a.journal_title, a.issn, a.publisher, a.volume, a.issue,
               a.firstpage, a.lastpage, a.pages, a.pdf_url,
               a.orcid, a.article_type, a.[references], a.metadata, a.is_oa,
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
