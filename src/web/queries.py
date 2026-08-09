"""Read-only SQLite queries for the dashboard.

Every function in this module runs SELECT only. The miner writes via
WAL mode while the dashboard reads — no locks, no conflicts.

All queries are parameterized; never interpolate user input directly.

Search uses a custom Turkish-aware fold function (`tr_fold`, registered
on the connection by the app) so that "Istanbul" matches "İstanbul" and
vice versa. User input is LIKE-escaped (percent, underscore, backslash)
so wildcards the user types don't turn into pattern metacharacters.
"""

from __future__ import annotations

import sqlite3

_ARTICLE_COLS = """a.id, a.journal_id, j.short_name, j.name AS journal_name,
       a.title, a.doi, a.publication_date, a.oai_identifier, a.abstract,
       a.keywords, a.language, a.journal_title, a.issn, a.publisher,
       a.volume, a.issue, a.firstpage, a.lastpage, a.pages, a.pdf_url,
       a.orcid, a.article_type, a.[references], a.is_oa, a.enriched_at,
       a.content_hash, a.created_at"""

_LIKE_ESCAPES = str.maketrans({"\\": "\\\\", "%": "\\%", "_": "\\_"})


def register(conn: sqlite3.Connection) -> None:
    """Register search/misc SQLite functions on a connection (idempotent)."""
    conn.create_function("tr_fold", 1, _fold_text)


def _fold_text(text: str | None) -> str:
    """Case-fold a string for Turkish-aware LIKE matching.

    Unifies dotted/dotless i and the Turkish digraph markers so that
    "İstanbul", "istanbul", "Istanbul" and "ıstanbul" all match each other.
    """
    if not text:
        return ""
    return (
        text.lower()
        .replace("ı", "i")
        .replace("ş", "s")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ö", "o")
        .replace("ç", "c")
        .replace("î", "i")
        .replace("â", "a")
        .replace("û", "u")
    )


def _fold_pattern(term: str) -> str:
    """Escape LIKE metacharacters so user input is literal, then wrap with %."""
    return f"%{term.translate(_LIKE_ESCAPES)}%"


def _pattern(col: str) -> str:
    """SQL fragment: fold(col) LIKE '%'||fold(?)||'%' ESCAPE '\\' (param 'p')."""
    return f"tr_fold({col}) LIKE '%' || tr_fold(?) || '%' ESCAPE '\\'"


def _rows(cur: sqlite3.Cursor) -> list[dict]:
    """Convert cursor rows to dicts using column names (row_factory agnostic)."""
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def db_stats(conn: sqlite3.Connection) -> dict:
    """Aggregate stats for the dashboard.

    Returns:
        Dict with total journals, total articles, journals with articles,
        open-access counts, PDF counts, and date range.
    """
    total_journals = conn.execute("SELECT COUNT(*) FROM journals").fetchone()[0]
    total_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    journals_with_articles = conn.execute(
        "SELECT COUNT(DISTINCT journal_id) FROM articles"
    ).fetchone()[0]
    oa_articles = conn.execute("SELECT COUNT(*) FROM articles WHERE is_oa = 1").fetchone()[0]
    pdf_articles = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE pdf_url IS NOT NULL AND pdf_url != ''"
    ).fetchone()[0]
    has_doi = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE doi IS NOT NULL AND doi != ''"
    ).fetchone()[0]
    enriched = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE enriched_at IS NOT NULL"
    ).fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(publication_date), MAX(publication_date) FROM articles"
    ).fetchone()
    return {
        "total_journals": total_journals,
        "total_articles": total_articles,
        "journals_with_articles": journals_with_articles,
        "open_access": oa_articles,
        "with_pdf": pdf_articles,
        "with_doi": has_doi,
        "enriched": enriched,
        "date_min": date_range[0] or "",
        "date_max": date_range[1] or "",
    }


def search_journals(
    conn: sqlite3.Connection,
    search: str = "",
    limit: int = 50,
    offset: int = 0,
    order: str = "alpha",
) -> list[dict]:
    """Page through journals with article/pdf counts. Never N+1.

    Args:
        search: Term matched against short_name and name (Turkish-folded).
        order: "alpha" (short_name) or "recent" (latest article date).
    """
    where, params = _journals_where(search)
    order_sql = (
        "ORDER BY j.short_name" if order == "alpha" else "ORDER BY latest_date DESC, j.short_name"
    )
    cur = conn.execute(
        f"""
        SELECT j.id, j.short_name, j.name, j.article_count,
               COUNT(a.id) AS harvested,
               SUM(CASE WHEN a.pdf_url IS NOT NULL AND a.pdf_url != '' THEN 1 ELSE 0 END) AS pdfs,
               MAX(a.publication_date) AS latest_date
        FROM journals j
        LEFT JOIN articles a ON a.journal_id = j.id
        {where}
        GROUP BY j.id, j.short_name, j.name, j.article_count
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    )
    rows = _rows(cur)
    for row in rows:
        row["pdfs"] = row["pdfs"] or 0
    return rows


def _journals_where(search: str) -> tuple[str, tuple]:
    if not search:
        return "", ()
    return (
        f"WHERE ({_pattern('j.short_name')} OR {_pattern('j.name')})",
        (_fold_pattern(search), _fold_pattern(search)),
    )


def count_journals(conn: sqlite3.Connection, search: str = "") -> int:
    where, params = _journals_where(search)
    return conn.execute(f"SELECT COUNT(*) FROM journals j {where}", params).fetchone()[0]


def get_journal(conn: sqlite3.Connection, short_name: str) -> dict | None:
    """Fetch a single journal by short_name."""
    row = conn.execute(
        """
        SELECT j.id, j.short_name, j.name, j.article_count,
               COUNT(a.id) AS harvested,
               SUM(CASE WHEN a.pdf_url IS NOT NULL AND a.pdf_url != '' THEN 1 ELSE 0 END) AS pdfs
        FROM journals j
        LEFT JOIN articles a ON a.journal_id = j.id
        WHERE j.short_name = ?
        GROUP BY j.id, j.short_name, j.name, j.article_count
        """,
        (short_name,),
    ).fetchone()
    if row is None:
        return None
    data = {
        "id": row[0],
        "short_name": row[1],
        "name": row[2],
        "article_count": row[3],
        "harvested": row[4],
        "pdfs": row[5] or 0,
    }
    return data


def count_journal_articles(conn: sqlite3.Connection, journal_id: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM articles WHERE journal_id = ?", (journal_id,)
    ).fetchone()[0]


def journal_articles(
    conn: sqlite3.Connection, journal_id: int, limit: int = 50, offset: int = 0
) -> list[dict]:
    """Articles for a journal with author names aggregated."""
    return _article_query(
        conn, where="WHERE a.journal_id = ?", params=(journal_id,), limit=limit, offset=offset
    )


def languages(conn: sqlite3.Connection) -> list[str]:
    """Distinct languages present in articles (for filter dropdown)."""
    return [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT language FROM articles WHERE language IS NOT NULL AND language != '' ORDER BY language"
        ).fetchall()
    ]


def years(conn: sqlite3.Connection) -> list[str]:
    """Distinct publication years (for filter dropdown)."""
    return [
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT substr(publication_date, 1, 4) AS yr
            FROM articles
            WHERE publication_date IS NOT NULL AND publication_date != ''
            ORDER BY yr DESC
            """
        ).fetchall()
    ]


def _article_where(
    search: str = "", language: str = "", is_oa: str = "", year: str = "", pdf: str = ""
) -> tuple[str, tuple]:
    clauses: list[str] = []
    params: list[object] = []
    if search:
        p = _fold_pattern(search)
        clauses.append(
            f"({_pattern('a.title')} OR {_pattern('a.abstract')} OR {_pattern('a.doi')} "
            f"OR {_pattern('a.keywords')} OR {_pattern('a.journal_title')} "
            f"OR EXISTS (SELECT 1 FROM authors au2 "
            f"WHERE au2.article_id = a.id AND {_pattern('au2.name')}) "
            f"OR EXISTS (SELECT 1 FROM journals j2 "
            f"WHERE j2.id = a.journal_id AND {_pattern('j2.name')}))"
        )
        params.extend([p] * 7)
    if language:
        clauses.append("a.language = ?")
        params.append(language)
    if is_oa == "1":
        clauses.append("a.is_oa = 1")
    elif is_oa == "0":
        clauses.append("(a.is_oa IS NULL OR a.is_oa = 0)")
    if year:
        clauses.append("substr(a.publication_date, 1, 4) = ?")
        params.append(year)
    if pdf == "1":
        clauses.append("a.pdf_url IS NOT NULL AND a.pdf_url != ''")
    elif pdf == "0":
        clauses.append("(a.pdf_url IS NULL OR a.pdf_url = '')")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, tuple(params)


def search_articles(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Page through articles with filters applied."""
    where, params = _article_where(search, language, is_oa, year, pdf)
    return _article_query(conn, where, params, limit, offset)


def count_articles(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
) -> int:
    where, params = _article_where(search, language, is_oa, year, pdf)
    return conn.execute(f"SELECT COUNT(*) FROM articles a {where}", params).fetchone()[0]


def search_authors(
    conn: sqlite3.Connection, search: str = "", limit: int = 20, offset: int = 0
) -> list[dict]:
    """Distinct authors matching a name query, with article counts."""
    if not search:
        return []
    clause = _pattern("name")
    cur = conn.execute(
        f"""
        SELECT name, COUNT(*) AS article_count
        FROM authors
        WHERE {clause}
        GROUP BY name
        ORDER BY article_count DESC, name
        LIMIT ? OFFSET ?
        """,
        (_fold_pattern(search), limit, offset),
    )
    return _rows(cur)


def count_authors(conn: sqlite3.Connection, search: str = "") -> int:
    if not search:
        return 0
    clause = _pattern("name")
    return conn.execute(
        f"SELECT COUNT(DISTINCT name) FROM authors WHERE {clause}", (_fold_pattern(search),)
    ).fetchone()[0]


def _article_query(
    conn: sqlite3.Connection, where: str, params: tuple, limit: int, offset: int
) -> list[dict]:
    cur = conn.execute(
        f"""
        SELECT {_ARTICLE_COLS}
        FROM articles a
        JOIN journals j ON j.id = a.journal_id
        {where}
        ORDER BY a.publication_date DESC, a.id DESC
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    )
    return _rows(cur)


def get_article(conn: sqlite3.Connection, article_id: int) -> dict | None:
    """Single article row keyed by numeric id."""
    cur = conn.execute(
        f"""
        SELECT {_ARTICLE_COLS}
        FROM articles a
        JOIN journals j ON j.id = a.journal_id
        WHERE a.id = ?
        """,
        (article_id,),
    )
    data = next(iter(_rows(cur)), None)
    if data is None:
        return None
    data["authors"] = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM authors WHERE article_id = ? ORDER BY id", (article_id,)
        ).fetchall()
    ]
    return data
