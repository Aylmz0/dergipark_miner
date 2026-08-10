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
       a.content_hash, a.created_at, a.author_institutions,
       (SELECT GROUP_CONCAT(au.name, ', ') FROM authors au
        WHERE au.article_id = a.id) AS authors_csv"""

_LIKE_ESCAPES = str.maketrans({"\\": "\\\\", "%": "\\%", "_": "\\_"})

# Relevance weights: SQL column pattern -> per-term score. Higher = stronger signal.
_SEARCH_SCORES = {
    "a.title": 10,
    "a.keywords": 6,
    "a.journal_title": 6,
    "a.article_type": 5,
    "a.publisher": 5,
    "a.doi": 4,
    "a.issn": 4,
    "a.volume": 3,
    "a.issue": 3,
    "a.pages": 3,
    "a.oai_identifier": 3,
    "a.abstract": 2,
    "a.[references]": 1,
}
_AUTHOR_SCORE = 6
_JOURNAL_SCORE = 6

# bm25 column weights, in articles_fts column order:
# title, abstract, keywords, jtitle, jname, atype, publisher, doi,
# issn, volume, issue, pages, oai, refs, authors
_FTS_WEIGHTS = (
    10.0, 2.0, 6.0, 6.0, 6.0, 5.0, 5.0, 4.0, 4.0,
    3.0, 3.0, 3.0, 3.0, 1.0, 6.0,
)


def register(conn: sqlite3.Connection) -> None:
    """Register search/misc SQLite functions on a connection (idempotent)."""
    conn.create_function("tr_fold", 1, _fold_text)
    conn.create_function("tr_match", 2, _sql_match)


def _sql_match(text: str | None, term: str | None) -> bool:
    """SQLite UDF: does folded `text` contain folded `term`?

    One Python call per row instead of one per searched column — the
    hot path of every article query.
    """
    return bool(text and term and _fold_text(term) in _fold_text(text))


def _fold_text(text: str | None) -> str:
    """Case-fold a string for Turkish-aware LIKE matching.

    Unifies dotted/dotless i and the Turkish digraph markers so that
    "İstanbul", "istanbul", "Istanbul" and "ıstanbul" all match each other.

    NOTE: 'İ' must map to 'i' BEFORE lower() — lower('İ') yields 'i\u0307'
    (i + combining dot), which would break matching for both LIKE and the
    FTS trigram index.
    """
    if not text:
        return ""
    return (
        text.replace("İ", "i")
        .lower()
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


def _fts_available(conn: sqlite3.Connection) -> bool:
    """True when the FTS5 index table exists in this database."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'articles_fts'"
    ).fetchone()
    return bool(row)


def _fts_phrase(terms: list[str]) -> str | None:
    """Build an FTS5 MATCH string: folded quoted phrases ANDed together.

    Returns None when FTS cannot serve the query (any term shorter than
    the trigram minimum of 3 chars, or containing a double quote), so the
    caller falls back to the LIKE-based path.
    """
    phrases: list[str] = []
    for term in terms:
        t = _fold_text(term).strip()
        if len(t) < 3 or '"' in t:
            return None
        phrases.append(f'"{t}"')
    return " AND ".join(phrases)


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


def article_types(conn: sqlite3.Connection) -> list[str]:
    """Distinct article types (for the type filter dropdown)."""
    return [
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT article_type FROM articles
            WHERE article_type IS NOT NULL AND article_type != ''
            ORDER BY article_type
            """
        ).fetchall()
    ]


def _article_search(
    conn: sqlite3.Connection, search: str
) -> tuple[str, str, tuple, tuple]:
    """Build per-term AND clause + relevance score expr for a search query.

    Multi-word queries: every term must match somewhere (AND), and each
    term's score is summed across fields it matches. Returns
    (clause, score_expr, params); params feed both the clause and the score
    expr, in the same order.

    Two paths:
    - FTS path (default when the articles_fts trigram index exists and
      every term is >= 3 chars): a single `f.articles_fts MATCH ?` clause.
      Authors and journal names are part of the index, so the per-term
      author/journal EXISTS checks are skipped in the WHERE clause but kept
      in the score expression.
    - LIKE fallback: per-term `tr_match(haystack, ?) OR author_ex OR
      journal_ex` clauses, matching the pre-FTS behavior.
    """
    terms = [t for t in search.split() if t]
    if not terms:
        return "", "", (), ()
    fts_phrase = _fts_phrase(terms) if _fts_available(conn) else None
    haystack = (
        "COALESCE(a.title, '')||'\n'||COALESCE(a.abstract, '')||'\n'||"
        "COALESCE(a.keywords, '')||'\n'||COALESCE(a.journal_title, '')||'\n'||"
        "COALESCE(a.article_type, '')||'\n'||COALESCE(a.publisher, '')||'\n'||"
        "COALESCE(a.doi, '')||'\n'||COALESCE(a.issn, '')||'\n'||"
        "COALESCE(a.volume, '')||'\n'||COALESCE(a.issue, '')||'\n'||"
        "COALESCE(a.pages, '')||'\n'||COALESCE(a.oai_identifier, '')||'\n'||"
        "COALESCE(a.[references], '')"
    )
    clauses: list[str] = []
    score_parts: list[str] = []
    score_params: list[object] = []
    where_params: list[object] = []
    for term in terms:
        p = _fold_pattern(term)
        for col, weight in _SEARCH_SCORES.items():
            pat = _pattern(col)
            score_parts.append(f"CASE WHEN {pat} THEN {weight} ELSE 0 END")
        author_ex = (
            f"EXISTS (SELECT 1 FROM authors au2 WHERE au2.article_id = a.id "
            f"AND {_pattern('au2.name')})"
        )
        journal_ex = (
            f"EXISTS (SELECT 1 FROM journals j2 WHERE j2.id = a.journal_id "
            f"AND {_pattern('j2.name')})"
        )
        score_parts.append(f"CASE WHEN {author_ex} THEN {_AUTHOR_SCORE} ELSE 0 END")
        score_parts.append(f"CASE WHEN {journal_ex} THEN {_JOURNAL_SCORE} ELSE 0 END")
        # SELECT-list placeholders (score) bind before the WHERE parameters.
        # tr_match folds internally so it gets the raw term; LIKE patterns
        # (score + EXISTS) get the %-wrapped pattern.
        score_params.extend([p] * (len(_SEARCH_SCORES) + 2))
        if fts_phrase is None:
            term_clauses = [f"tr_match({haystack}, ?)", author_ex, journal_ex]
            clauses.append(f"({' OR '.join(term_clauses)})")
            where_params.append(term)  # tr_match haystack (raw)
            where_params.append(p)  # author EXISTS
            where_params.append(p)  # journal EXISTS
    if fts_phrase is not None:
        weights = ", ".join(repr(w) for w in _FTS_WEIGHTS)
        return (
            "f.articles_fts MATCH ?",
            f"bm25(articles_fts, {weights})",
            (),
            (fts_phrase,),
        )
    clause = " AND ".join(clauses)
    score_expr = " + ".join(score_parts)
    return clause, score_expr, tuple(score_params), tuple(where_params)


def _article_where(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
    article_type: str = "",
) -> tuple[str, tuple, str | None, bool]:
    """WHERE clause + params + relevance score expr (None when no search).

    Search covers every harvested field: title, abstract, keywords, DOI,
    journal title/name, authors, article type, publisher, ISSN, volume,
    issue, pages, OAI id, references. Multi-term queries AND terms together.

    Returns (where_sql, params, score_expr, fts_used); fts_used tells
    callers to join the articles_fts index table.
    """
    clauses: list[str] = []
    params: list[object] = []
    score_expr: str | None = None
    fts_used = False
    search_clause, score_expr, _score_params, search_params = _article_search(conn, search)
    if search_clause:
        clauses.append(f"({search_clause})")
        params.extend(search_params)
        fts_used = search_clause.startswith("f.articles_fts")
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
    if article_type:
        clauses.append("tr_fold(a.article_type) = tr_fold(?)")
        params.append(article_type)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, tuple(params), score_expr, fts_used


_ARTICLE_SORTS = {
    "date_desc": "ORDER BY a.publication_date DESC, a.id DESC",
    "date_asc": "ORDER BY a.publication_date ASC, a.id ASC",
    "title": "ORDER BY tr_fold(a.title) COLLATE NOCASE, a.id DESC",
    "journal": "ORDER BY tr_fold(j.name) COLLATE NOCASE, a.publication_date DESC",
}


def search_articles(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
    article_type: str = "",
    sort: str = "date_desc",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Page through articles with filters. Ranked by relevance when searching."""
    order_sql = _ARTICLE_SORTS.get(sort, _ARTICLE_SORTS["date_desc"])
    where, params, _, fts_used = _article_where(
        conn, search, language, is_oa, year, pdf, article_type
    )
    select_sql = _ARTICLE_COLS
    if search.strip():
        _, score_expr, score_params, _ = _article_search(conn, search)
        # bm25 scores are negative (lower = better); the LIKE score is
        # positive (higher = better).
        order_sql = (
            "ORDER BY score ASC, a.publication_date DESC, a.id DESC"
            if fts_used
            else "ORDER BY score DESC, a.publication_date DESC, a.id DESC"
        )
        select_sql = f"{_ARTICLE_COLS}, ({score_expr}) AS score"
        # SELECT-list placeholders (score) bind before WHERE placeholders
        params = (*score_params, *params)
    return _article_query(
        conn, select_sql, order_sql, where, params, limit, offset, fts=fts_used
    )


def count_articles(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
    article_type: str = "",
) -> int:
    where, params, _, fts_used = _article_where(
        conn, search, language, is_oa, year, pdf, article_type
    )
    fts_join = "JOIN articles_fts f ON f.rowid = a.id\n" if fts_used else ""
    return conn.execute(
        f"SELECT COUNT(*) FROM articles a\n{fts_join}{where}", params
    ).fetchone()[0]


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
    conn: sqlite3.Connection,
    select_sql: str = _ARTICLE_COLS,
    order_sql: str = "ORDER BY a.publication_date DESC, a.id DESC",
    where: str = "",
    params: tuple = (),
    limit: int = 50,
    offset: int = 0,
    fts: bool = False,
) -> list[dict]:
    fts_join = "JOIN articles_fts f ON f.rowid = a.id\n" if fts else ""
    cur = conn.execute(
        f"""
        SELECT {select_sql}
        FROM articles a
        JOIN journals j ON j.id = a.journal_id
        {fts_join}
        {where}
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    )
    return _rows(cur)


def iter_articles(
    conn: sqlite3.Connection,
    search: str = "",
    language: str = "",
    is_oa: str = "",
    year: str = "",
    pdf: str = "",
    article_type: str = "",
    sort: str = "date_desc",
    batch_size: int = 1000,
):
    """Stream filtered article rows as dicts (export endpoints).

    Same filters as search_articles but streams via fetchmany instead of
    materializing the whole result set. Generator: one yield per row.
    """
    order_sql = _ARTICLE_SORTS.get(sort, _ARTICLE_SORTS["date_desc"])
    where, params, _, fts_used = _article_where(
        conn, search, language, is_oa, year, pdf, article_type
    )
    select_sql = _ARTICLE_COLS
    if search.strip():
        _, score_expr, score_params, _ = _article_search(conn, search)
        order_sql = (
            "ORDER BY score ASC, a.publication_date DESC, a.id DESC"
            if fts_used
            else "ORDER BY score DESC, a.publication_date DESC, a.id DESC"
        )
        select_sql = f"{_ARTICLE_COLS}, ({score_expr}) AS score"
        params = (*score_params, *params)
    fts_join = "JOIN articles_fts f ON f.rowid = a.id\n" if fts_used else ""
    cur = conn.execute(
        f"""
        SELECT {select_sql}
        FROM articles a
        JOIN journals j ON j.id = a.journal_id
        {fts_join}
        {where}
        {order_sql}
        """,
        params,
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            yield dict(zip((d[0] for d in cur.description), row, strict=False))


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


def similar_articles(
    conn: sqlite3.Connection,
    article_id: int,
    keywords: list[str],
    limit: int = 3,
) -> list[dict]:
    """Articles sharing keywords or the same journal, ranked by overlap.

    Keyword overlap scores 2 per hit, same journal +1. Works on the
    raw JSON keyword column via tr_fold LIKE. Single query, no N+1.
    """
    terms = [k for k in (keywords or []) if k][:5]
    fold_terms = [_fold_pattern(t) for t in terms]
    jid_row = conn.execute(
        "SELECT journal_id FROM articles WHERE id = ?", (article_id,)
    ).fetchone()
    if jid_row is None:
        return []
    jid = jid_row[0]

    kw_pats = [_pattern("a.keywords")] * len(terms)
    score_sql = " + ".join(
        [f"CASE WHEN {p} THEN 2 ELSE 0 END" for p in kw_pats]
        + ["CASE WHEN a.journal_id = ? THEN 1 ELSE 0 END"]
    )
    where_ors = " OR ".join(["a.journal_id = ?", *kw_pats])
    score_params = [*fold_terms, jid]
    where_params = [article_id, jid, *fold_terms]
    cur = conn.execute(
        f"""
        SELECT {_ARTICLE_COLS}, ({score_sql}) AS sim_score
        FROM articles a
        JOIN journals j ON j.id = a.journal_id
        WHERE a.id != ? AND ({where_ors})
        ORDER BY sim_score DESC, a.publication_date DESC, a.id DESC
        LIMIT ?
        """,
        (*score_params, *where_params, limit),
    )
    return _rows(cur)
