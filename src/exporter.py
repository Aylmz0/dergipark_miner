"""
JSONL export — dergi bazlı makale verisi.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def _normalize_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    return doi.removeprefix("https://doi.org/").removeprefix("http://doi.org/")


def _build_article_row(conn: sqlite3.Connection, article: dict) -> dict:
    """Tek bir makale satırı (JSON nesnesi) oluşturur."""
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
    return {
        "id": article["oai_identifier"],
        "title": article["title"],
        "doi": _normalize_doi(article["doi"]),
        "date": article["publication_date"],
        "authors": authors,
        "journal": article["journal_short"],
        "citations": citations,
    }


def export_journal_json(
    conn: sqlite3.Connection,
    journal_shortname: str,
    output_dir: str | Path,
) -> Path:
    """
    Belirtilen derginin tüm makalelerini JSONL formatına dönüştürür.

    Returns:
        Oluşturulan dosya yolu.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{journal_shortname}.jsonl"

    conn.row_factory = sqlite3.Row

    journal = conn.execute(
        "SELECT id FROM journals WHERE short_name = ?",
        (journal_shortname,),
    ).fetchone()
    if journal is None:
        raise ValueError(f"Dergi bulunamadı: {journal_shortname}")

    articles = conn.execute(
        """
        SELECT a.id, a.title, a.doi, a.publication_date,
               a.oai_identifier, j.short_name AS journal_short
        FROM articles a
        JOIN journals j ON a.journal_id = j.id
        WHERE a.journal_id = ?
        ORDER BY a.id
        """,
        (journal["id"],),
    ).fetchall()

    with open(out_file, "w", encoding="utf-8") as f:
        for article in articles:
            row = _build_article_row(conn, dict(article))
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return out_file


def export_all_journals(
    conn: sqlite3.Connection,
    output_dir: str | Path,
) -> list[Path]:
    """
    Tüm dergiler için ayrı ayrı JSONL dosyaları oluşturur.

    Returns:
        Oluşturulan dosya yolları listesi.
    """
    journals = conn.execute("SELECT short_name FROM journals ORDER BY short_name").fetchall()

    files = []
    for (short_name,) in journals:
        path = export_journal_json(conn, short_name, output_dir)
        files.append(path)
    return files


def export_journals_json(
    conn: sqlite3.Connection,
    output_dir: str | Path,
) -> Path:
    """Tum dergileri tek bir JSONL dosyasi olarak disa aktarir.

    Her satir: {short_name, name, article_count}
    Returns:
        Olusturulan dosya yolu.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "journals.jsonl"

    rows = conn.execute(
        """
        SELECT j.short_name, j.name, COUNT(a.id) AS article_count
        FROM journals j
        LEFT JOIN articles a ON a.journal_id = j.id
        GROUP BY j.id
        ORDER BY j.short_name
        """
    ).fetchall()

    with open(out_file, "w", encoding="utf-8") as f:
        for short_name, name, article_count in rows:
            row = {
                "short_name": short_name,
                "name": name or "",
                "article_count": article_count,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return out_file
