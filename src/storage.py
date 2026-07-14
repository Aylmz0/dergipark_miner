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
    conn.commit()
    return conn
