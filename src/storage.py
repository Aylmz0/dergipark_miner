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

    conn.commit()
    return conn


def _resolve_journal(conn: sqlite3.Connection, short_name: str, name: str = "") -> int:
    """journal_shortname'i journals tablosunda cozer; yoksa ekler ve id dondurur.

    - Yeni dergi eklerken name'i de yazar.
    - Mevcut dergi bulundugunda name bossa ve gelen name doluysa gunceller.
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


def insert_article(conn: sqlite3.Connection, article: dict) -> int:
    """GÖREV 5'in metadata dict'ini articles tablosuna yazar, article id'sini döndürür.

    - journal_shortname -> journal_id çözülür (gerekirse journals'a eklenir).
    - Oncelik: oai_identifier > doi > yeni satır.
    - oai_identifier veya doi varsa mevcut kaydi guncelle (UPSERT).
    - Yazarlari authors tablosuna yazar (UPSERT'te eskilerini siler).
    - Dönen değer articles.id (rowid).
    """
    journal_id = _resolve_journal(conn, article.get("journal_shortname") or "", article.get("journal_name") or "")
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

    # 3. Yeni satir
    if article_id is None:
        cur = conn.execute(
            """
            INSERT INTO articles (journal_id, title, doi, publication_date, oai_identifier)
            VALUES (?, ?, ?, ?, ?)
            """,
            (journal_id, title, doi, publication_date, oai_identifier),
        )
        article_id = cur.lastrowid

    _write_authors(conn, article_id, authors)
    conn.commit()
    return article_id


def _write_authors(conn: sqlite3.Connection, article_id: int, authors: list) -> None:
    """Makaleye ait yazarlari authors tablosuna yazar. UPSERT'te eski satirlari siler yeniden ekler."""
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
    """Citations tablosuna atif yazar; ayni (citing, cited_openalex) tekrarlanmaz.

    - Ayni citing_article_id + cited_openalex_id kombinasyonu varsa atlar (dedup).
    - cited_openalex_id None veya bos string ise dedup atlanmaz; teorik olarak ayni
      makale birden fazla kez tanimlanabilir (farkli openalex id yoksa).
    - Donus: inserted row id'si veya mevcut row id'si.
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
    """Verilen DOI ile bizim articles tablomuzda eslesen makale id'sini dondurur.

    DOI'yi normalize eder: https://doi.org/ prefix'ini kaldirir.
    Bulamazsa None dondurur.
    """
    doi = (cited_doi or "").strip()
    if not doi:
        return None
    # Normalize: https://doi.org/10.xxx -> 10.xxx
    if doi.startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/"):]
    elif doi.startswith("http://doi.org/"):
        doi = doi[len("http://doi.org/"):]
    elif doi.startswith("doi.org/"):
        doi = doi[len("doi.org/"):]
    row = conn.execute("SELECT id FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row[0] if row else None
