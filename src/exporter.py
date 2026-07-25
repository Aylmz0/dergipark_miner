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
    except json.JSONDecodeError, TypeError:
        return []


_FETCH_SIZE = 1000


def export_all_articles(
    conn: sqlite3.Connection,
    output_path: str | Path,
) -> Path:
    """Export all articles to a single JSONL file.

    Uses streaming cursor with fetchmany() + single JOIN query (no N+1).

    Args:
        conn: SQLite connection.
        output_path: Destination file path.

    Returns:
        Path to the written JSONL file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn.row_factory = None

    # Single query: authors aggregated via GROUP_CONCAT (no N+1)
    cursor = conn.execute(
        """
        SELECT a.id, a.title, a.doi, a.publication_date,
               a.oai_identifier, a.abstract, a.keywords, a.language,
               a.journal_title, a.issn, a.publisher, a.volume, a.issue,
               a.firstpage, a.lastpage, a.pages, a.pdf_url,
               a.orcid, a.article_type, a.[references], a.is_oa,
               j.short_name,
               GROUP_CONCAT(au.name, '||') AS author_names
        FROM articles a
        JOIN journals j ON a.journal_id = j.id
        LEFT JOIN authors au ON au.article_id = a.id
        GROUP BY a.id
        ORDER BY j.short_name, a.id
        """
    )

    succeeded = 0
    failed = 0
    with open(output_path, "w", encoding="utf-8") as f:
        while True:
            batch = cursor.fetchmany(_FETCH_SIZE)
            if not batch:
                break
            for article in batch:
                try:
                    authors = [n for n in (article[22] or "").split("||") if n]
                    row = _build_row(article, authors)
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    succeeded += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("Export skip article id={}: {}", article[0], exc)

    logger.info("Export: {} succeeded, {} failed", succeeded, failed)
    return output_path


def _build_row(article: tuple, authors: list[str]) -> dict:
    """Build a JSONL row dict from a raw cursor row + authors.

    Args:
        article: Tuple from SELECT query.
        authors: Pre-aggregated author name list.

    Returns:
        Dict ready for JSON serialization.
    """
    oai_id = article[4] or ""
    slug = article[21]
    article_number = oai_id.split("/")[-1] if "/" in oai_id else ""
    url = (
        f"https://dergipark.org.tr/tr/pub/{slug}/article/{article_number}" if article_number else ""
    )

    return {
        "slug": slug,
        "article_id": oai_id,
        "article_url": url,
        "title": article[1],
        "authors": authors,
        "journal_title": article[8] or "",
        "issn": article[9] or "",
        "publisher": article[10] or "",
        "volume": article[11] or "",
        "issue": article[12] or "",
        "firstpage": article[13] or "",
        "lastpage": article[14] or "",
        "pages": article[15] or "",
        "date": article[3],
        "language": article[7] or "",
        "doi": _normalize_doi(article[2]),
        "abstract": article[5] or "",
        "keywords": _json_loads(article[6]),
        "is_oa": bool(article[20]),
        "pdf_url": article[16] or None,
        "orcid": _json_loads(article[17]),
        "article_type": article[18] or "",
        "references": _json_loads(article[19]),
    }
