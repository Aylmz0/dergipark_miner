"""SQLite persistence layer for journals, articles, authors, and citations."""

import hashlib
import json
import sqlite3
from pathlib import Path


def _json_dumps(obj: list) -> str:
    """Serialize a list to JSON string for storage."""
    return json.dumps(obj, ensure_ascii=False)


def init_db(path: str | Path) -> sqlite3.Connection:
    """Initialize the database schema and return a connection.

    Creates tables with IF NOT EXISTS. Runs migrations for missing columns.
    Enables foreign key constraints (PRAGMA foreign_keys = ON).

    Schema:
        journals: id, short_name (UNIQUE), name, created_at
        articles: id, journal_id (FK), title, doi, publication_date,
                  oai_identifier, abstract, keywords, language,
                  enriched_at, content_hash, created_at
        authors: id, article_id (FK), name, created_at
        citations: id, citing_article_id (FK), cited_article_id (FK),
                   cited_openalex_id, cited_doi, cited_title, created_at

    Args:
        path: Path to the SQLite database file.

    Returns:
        Open sqlite3.Connection with foreign keys enabled.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS journals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            short_name   TEXT UNIQUE NOT NULL,
            name         TEXT,
            article_count INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id     INTEGER NOT NULL,
            title          TEXT,
            doi            TEXT,
            publication_date TEXT,
            oai_identifier TEXT,
            abstract       TEXT,
            keywords       TEXT,
            language       TEXT,
            journal_title  TEXT,
            issn           TEXT,
            publisher      TEXT,
            volume         TEXT,
            issue          TEXT,
            firstpage      TEXT,
            lastpage       TEXT,
            pages          TEXT,
            pdf_url        TEXT,
            orcid          TEXT,
            article_type   TEXT,
            [references]   TEXT,
            author_institutions TEXT,
            metadata       TEXT,
            is_oa          INTEGER,
            enriched_at    TEXT,
            content_hash   TEXT,
            created_at     TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (journal_id) REFERENCES journals(id)
        );

        CREATE TABLE IF NOT EXISTS authors (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            name        TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (article_id) REFERENCES articles(id)
        );
        """
    )

    # Old databases may be missing columns; add them if needed
    _MIGRATE_COLUMNS = [
        ("oai_identifier", "TEXT"),
        ("enriched_at", "TEXT"),
        ("content_hash", "TEXT"),
        ("abstract", "TEXT"),
        ("keywords", "TEXT"),
        ("language", "TEXT"),
        ("journal_title", "TEXT"),
        ("issn", "TEXT"),
        ("publisher", "TEXT"),
        ("volume", "TEXT"),
        ("issue", "TEXT"),
        ("firstpage", "TEXT"),
        ("lastpage", "TEXT"),
        ("pages", "TEXT"),
        ("pdf_url", "TEXT"),
        ("orcid", "TEXT"),
        ("article_type", "TEXT"),
        ("[references]", "TEXT"),
        ("author_institutions", "TEXT"),
        ("metadata", "TEXT"),
        ("is_oa", "INTEGER"),
    ]
    cols = {row[1] for row in conn.execute("PRAGMA table_info(articles)").fetchall()}
    for col_name, col_type in _MIGRATE_COLUMNS:
        # [references] is stored as "references" in PRAGMA output
        lookup = col_name.strip("[]")
        if lookup not in cols:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col_name} {col_type}")

    # article_count column may be missing in journals; add it
    journal_cols = {row[1] for row in conn.execute("PRAGMA table_info(journals)").fetchall()}
    if "article_count" not in journal_cols:
        conn.execute("ALTER TABLE journals ADD COLUMN article_count INTEGER DEFAULT 0")

    # Performance indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_oai ON articles(oai_identifier)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_enriched ON articles(enriched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)")

    conn.commit()
    return conn


def _resolve_journal(conn: sqlite3.Connection, short_name: str, name: str = "") -> int:
    """Resolve a journal short_name to its ID, inserting if needed.

    If the journal exists but has an empty name and a new name is provided,
    updates the name.

    Args:
        conn: SQLite connection.
        short_name: Journal short name (slug).
        name: Journal display name.

    Returns:
        The journal's row ID.
    """
    short_name = short_name or "__unknown__"
    row = conn.execute(
        "SELECT id, name FROM journals WHERE short_name = ?", (short_name,)
    ).fetchone()
    if row:
        journal_id = row[0]
        existing_name = row[1] or ""
        if not existing_name and name:
            conn.execute("UPDATE journals SET name = ? WHERE id = ?", (name, journal_id))
        return journal_id
    cur = conn.execute(
        "INSERT INTO journals (short_name, name) VALUES (?, ?)",
        (short_name, name),
    )
    return cur.lastrowid


def update_journal_article_count(conn: sqlite3.Connection, short_name: str, count: int) -> None:
    """Update the article count for a journal.

    Args:
        conn: SQLite connection.
        short_name: Journal short name (slug).
        count: Number of articles found in this journal.
    """
    conn.execute(
        "UPDATE journals SET article_count = ? WHERE short_name = ?",
        (count, short_name),
    )


def insert_journals(conn: sqlite3.Connection, journals: list[dict]) -> None:
    """Bulk insert or update journals in the database.

    Existing journals are skipped. Empty names are filled if provided.

    Args:
        conn: SQLite connection.
        journals: List of dicts with 'short_name' and 'name' keys.
    """
    for j in journals:
        _resolve_journal(conn, j.get("short_name", ""), j.get("name", ""))
    conn.commit()


def _make_content_hash(title: str, journal_id: int, publication_date: str) -> str:
    """Generate a content hash for deduplication when DOI/OAI ID are absent.

    Args:
        title: Article title.
        journal_id: Journal row ID.
        publication_date: Publication date string.

    Returns:
        MD5 hex digest of the combined fields.
    """
    raw = f"{title}|{journal_id}|{publication_date}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def insert_article(conn: sqlite3.Connection, article: dict) -> int:
    """Insert or update an article in the articles table.

    UPSERT priority: oai_identifier > doi > content_hash > new row.
    Authors are written to the authors table (old rows deleted on update).

    Args:
        conn: SQLite connection.
        article: Dict with keys: title, doi, authors, publication_date,
                 journal_shortname, oai_identifier, journal_name,
                 abstract, keywords, language, and all HTML-scraped fields.

    Returns:
        The article's row ID.
    """
    journal_id = _resolve_journal(
        conn, article.get("journal_shortname") or "", article.get("journal_name") or ""
    )
    title = article.get("title") or ""
    doi = article.get("doi") or ""
    publication_date = article.get("publication_date") or ""
    oai_identifier = article.get("oai_identifier") or ""
    authors = article.get("authors") or []
    abstract = article.get("abstract") or ""
    keywords = article.get("keywords") or []
    language = article.get("language") or ""

    article_id = None

    # 1. Find by oai_identifier
    if oai_identifier:
        existing = conn.execute(
            "SELECT id FROM articles WHERE oai_identifier = ?", (oai_identifier,)
        ).fetchone()
        if existing:
            article_id = existing[0]
            conn.execute(
                """
                UPDATE articles
                SET journal_id=?, title=?, doi=?, publication_date=?,
                    oai_identifier=?, abstract=?, keywords=?, language=?
                WHERE id=?
                """,
                (
                    journal_id,
                    title,
                    doi,
                    publication_date,
                    oai_identifier,
                    abstract,
                    _json_dumps(keywords),
                    language,
                    article_id,
                ),
            )

    # 2. Find by DOI
    if article_id is None and doi:
        existing = conn.execute("SELECT id FROM articles WHERE doi = ?", (doi,)).fetchone()
        if existing:
            article_id = existing[0]
            conn.execute(
                """
                UPDATE articles
                SET journal_id=?, title=?, publication_date=?, oai_identifier=?,
                    abstract=?, keywords=?, language=?
                WHERE id=?
                """,
                (
                    journal_id,
                    title,
                    publication_date,
                    oai_identifier,
                    abstract,
                    _json_dumps(keywords),
                    language,
                    article_id,
                ),
            )

    # 3. Find by content hash (when no DOI or OAI ID)
    if article_id is None and not oai_identifier and not doi and title:
        content_hash = _make_content_hash(title, journal_id, publication_date)
        existing = conn.execute(
            "SELECT id FROM articles WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        if existing:
            article_id = existing[0]

    # 4. Insert new row
    if article_id is None:
        content_hash = None
        if not oai_identifier and not doi and title:
            content_hash = _make_content_hash(title, journal_id, publication_date)
        cur = conn.execute(
            """
            INSERT INTO articles (journal_id, title, doi, publication_date, oai_identifier,
                                  content_hash, abstract, keywords, language)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                journal_id,
                title,
                doi,
                publication_date,
                oai_identifier,
                content_hash,
                abstract,
                _json_dumps(keywords),
                language,
            ),
        )
        article_id = cur.lastrowid

    _write_authors(conn, article_id, authors)
    return article_id


def _write_authors(conn: sqlite3.Connection, article_id: int, authors: list) -> None:
    """Replace all authors for an article (delete-then-insert).

    Args:
        conn: SQLite connection.
        article_id: Article row ID.
        authors: List of author name strings.
    """
    conn.execute("DELETE FROM authors WHERE article_id = ?", (article_id,))
    for name in authors:
        name = (name or "").strip()
        if not name:
            continue
        conn.execute(
            "INSERT INTO authors (article_id, name) VALUES (?, ?)",
            (article_id, name),
        )
