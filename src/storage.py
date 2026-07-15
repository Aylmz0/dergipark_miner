"""polars ve sqlite3 ile veri normalizasyonu ve kalıcı depolama.

Faz 4: SQLite şema tasarımı (journals, articles, authors, citations).
"""

import sqlite3
from pathlib import Path


def init_db(path: str | Path) -> sqlite3.Connection:
    """Veritabanı şemasını kurar ve bağlantıyı döndürür.

    Tablolar IF NOT EXISTS ile oluşturulur; var olan tablolar atlanır.
    İlişkiler: articles.journal_id -> journals.id,
               authors.article_id -> articles.id,
               citations.citing_article_id / cited_article_id -> articles.id.
    Foreign key kısıtları etkinleştirilir (PRAGMA foreign_keys = ON).
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

    conn.commit()
    return conn


def _resolve_journal(conn: sqlite3.Connection, short_name: str) -> int:
    """journal_shortname'i journals tablosunda çözer; yoksa ekler ve id döndürür."""
    short_name = short_name or "__unknown__"
    row = conn.execute(
        "SELECT id FROM journals WHERE short_name = ?", (short_name,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute("INSERT INTO journals (short_name) VALUES (?)", (short_name,))
    conn.commit()
    return cur.lastrowid


def insert_article(conn: sqlite3.Connection, article: dict) -> int:
    """GÖREV 5'in metadata dict'ini articles tablosuna yazar, article id'sini döndürür.

    - journal_shortname -> journal_id çözülür (gerekirse journals'a eklenir).
    - Oncelik: oai_identifier > doi > yeni satır.
    - oai_identifier veya doi varsa mevcut kaydi guncelle (UPSERT).
    - Dönen değer articles.id (rowid).
    """
    journal_id = _resolve_journal(conn, article.get("journal_shortname") or "")
    title = article.get("title") or ""
    doi = article.get("doi") or ""
    publication_date = article.get("publication_date") or ""
    oai_identifier = article.get("oai_identifier") or ""

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
            conn.commit()
            return article_id

    # 2. DOI ile bul
    if doi:
        existing = conn.execute(
            "SELECT id FROM articles WHERE doi = ?", (doi,)
        ).fetchone()
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
            conn.commit()
            return article_id

    # 3. Yeni satir
    cur = conn.execute(
        """
        INSERT INTO articles (journal_id, title, doi, publication_date, oai_identifier)
        VALUES (?, ?, ?, ?, ?)
        """,
        (journal_id, title, doi, publication_date, oai_identifier),
    )
    conn.commit()
    return cur.lastrowid
