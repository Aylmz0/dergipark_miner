"""SQLite persistence layer for journals, articles, authors, and citations."""

import hashlib
import sqlite3
from pathlib import Path


def init_db(path: str | Path) -> sqlite3.Connection:
    """Initialize the database schema and return a connection.

    Creates tables with IF NOT EXISTS. Runs migrations for missing columns.
    Enables foreign key constraints (PRAGMA foreign_keys = ON).

    Schema:
        journals: id, short_name (UNIQUE), name, created_at
        articles: id, journal_id (FK), title, doi, publication_date,
                  oai_identifier, enriched_at, content_hash, created_at
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

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS journals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            short_name   TEXT UNIQUE NOT NULL,
            name         TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id     INTEGER NOT NULL,
            title          TEXT,
            doi            TEXT,
            publication_date TEXT,
            oai_identifier TEXT,
            enriched_at    TEXT,
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

        CREATE TABLE IF NOT EXISTS citations (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            citing_article_id   INTEGER NOT NULL,
            cited_article_id    INTEGER,
            cited_openalex_id   TEXT,
            cited_doi           TEXT,
            cited_title         TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (citing_article_id) REFERENCES articles(id),
            FOREIGN KEY (cited_article_id) REFERENCES articles(id)
        );
        """
    )

    # Eski veritabaninda oai_identifier kolonu eksik olabilir; ekle
    cols = {row[1] for row in conn.execute("PRAGMA table_info(articles)").fetchall()}
    if "oai_identifier" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN oai_identifier TEXT")

    # cited_openalex_id kolonu eksik olabilir; ekle
    cit_cols = {row[1] for row in conn.execute("PRAGMA table_info(citations)").fetchall()}
    if "cited_openalex_id" not in cit_cols:
        conn.execute("ALTER TABLE citations ADD COLUMN cited_openalex_id TEXT")

    # enriched_at kolonu eksik olabilir; ekle
    if "enriched_at" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN enriched_at TEXT")

    # content_hash kolonu eksik olabilir; ekle (dedup icin)
    if "content_hash" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN content_hash TEXT")

    # Performans icin indexler
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_oai ON articles(oai_identifier)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_enriched ON articles(enriched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_citations_citing ON citations(citing_article_id)")

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
                 journal_shortname, oai_identifier, journal_name.

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

    article_id = None

    # 1. oai_identifier ile bul
    if oai_identifier:
        existing = conn.execute(
            "SELECT id FROM articles WHERE oai_identifier = ?", (oai_identifier,)
        ).fetchone()
        if existing:
            article_id = existing[0]
            conn.execute(
                """
                UPDATE articles
                SET journal_id=?, title=?, doi=?, publication_date=?, oai_identifier=?
                WHERE id=?
                """,
                (journal_id, title, doi, publication_date, oai_identifier, article_id),
            )

    # 2. DOI ile bul
    if article_id is None and doi:
        existing = conn.execute("SELECT id FROM articles WHERE doi = ?", (doi,)).fetchone()
        if existing:
            article_id = existing[0]
            conn.execute(
                """
                UPDATE articles
                SET journal_id=?, title=?, publication_date=?, oai_identifier=?
                WHERE id=?
                """,
                (journal_id, title, publication_date, oai_identifier, article_id),
            )

    # 3. Content hash ile bul (DOI ve OAI ID yoksa)
    if article_id is None and not oai_identifier and not doi and title:
        content_hash = _make_content_hash(title, journal_id, publication_date)
        existing = conn.execute(
            "SELECT id FROM articles WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        if existing:
            article_id = existing[0]

    # 4. Yeni satir
    if article_id is None:
        content_hash = None
        if not oai_identifier and not doi and title:
            content_hash = _make_content_hash(title, journal_id, publication_date)
        cur = conn.execute(
            """
            INSERT INTO articles (journal_id, title, doi, publication_date, oai_identifier, content_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (journal_id, title, doi, publication_date, oai_identifier, content_hash),
        )
        article_id = cur.lastrowid

    _write_authors(conn, article_id, authors)
    conn.commit()
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


def insert_citation(
    conn: sqlite3.Connection,
    *,
    citing_article_id: int,
    cited_openalex_id: str | None = None,
    cited_doi: str | None = None,
    cited_title: str | None = None,
) -> int:
    """Insert a citation row, deduplicating by (citing_article_id, cited_openalex_id).

    If cited_openalex_id is provided and a duplicate exists, returns the
    existing row ID. Without cited_openalex_id, no dedup is performed.

    Args:
        conn: SQLite connection.
        citing_article_id: ID of the citing article.
        cited_openalex_id: OpenAlex work ID of the cited work.
        cited_doi: DOI of the cited work.
        cited_title: Title of the cited work.

    Returns:
        Row ID of the inserted or existing citation.
    """
    if citing_article_id is None:
        return -1

    openalex_id = (cited_openalex_id or "").strip()
    doi = (cited_doi or "").strip()
    title = (cited_title or "").strip()

    # Dedup: ayni citing + cited_openalex_id varsa mevcut satiri dondur
    if openalex_id:
        row = conn.execute(
            "SELECT id FROM citations WHERE citing_article_id = ? AND cited_openalex_id = ?",
            (citing_article_id, openalex_id),
        ).fetchone()
        if row:
            return row[0]

    cur = conn.execute(
        """
        INSERT INTO citations (citing_article_id, cited_article_id, cited_openalex_id, cited_doi, cited_title)
        VALUES (?, NULL, ?, ?, ?)
        """,
        (citing_article_id, openalex_id or None, doi or None, title or None),
    )
    conn.commit()
    return cur.lastrowid


def resolve_citation_targets(conn: sqlite3.Connection, *, cited_doi: str = "") -> int | None:
    """Find an article in our database matching the given DOI.

    Normalizes the DOI by removing https://doi.org/ prefix.

    Args:
        conn: SQLite connection.
        cited_doi: DOI string to look up.

    Returns:
        Matching article row ID, or None if not found.
    """
    doi = (cited_doi or "").strip()
    if not doi:
        return None
    # Normalize: https://doi.org/10.xxx -> 10.xxx
    if doi.startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/") :]
    elif doi.startswith("http://doi.org/"):
        doi = doi[len("http://doi.org/") :]
    elif doi.startswith("doi.org/"):
        doi = doi[len("doi.org/") :]
    row = conn.execute("SELECT id FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row[0] if row else None
