"""FTS5 search path tests: trigram matching, folding, fallbacks."""

import pytest

from src.storage import fts_sync, init_db, insert_article, insert_journals
from src.web import queries


@pytest.fixture()
def conn(tmp_path):
    c = init_db(tmp_path / "fts.sqlite")
    queries.register(c)
    insert_journals(c, [{"short_name": "dergj", "name": "Anadolu Journal"}])
    insert_article(
        c,
        {
            "title": "İstanbul deprem çalışması",
            "journal_shortname": "dergj",
            "journal_name": "Anadolu Journal",
            "publication_date": "2023",
            "oai_identifier": "oai:1",
            "authors": ["Ali Veli"],
            "abstract": "Sismolojik analiz bulguları",
            "language": "tur",
        },
    )
    insert_article(
        c,
        {
            "title": "Covid-19 pandemisinin etkileri",
            "journal_shortname": "dergj",
            "journal_name": "Anadolu Journal",
            "publication_date": "2022",
            "oai_identifier": "oai:2",
            "authors": ["Ayşe Koç"],
            "abstract": "PET/CT incelemesi",
            "language": "tur",
        },
    )
    insert_article(
        c,
        {
            "title": "fNIRS yöntemi ile nörogörüntüleme",
            "journal_shortname": "dergj",
            "journal_name": "Anadolu Journal",
            "publication_date": "2021",
            "oai_identifier": "oai:3",
            "authors": [],
            "abstract": "Serebral korteks aktivitesi",
            "language": "tur",
        },
    )
    c.commit()
    return c


def _titles(rows):
    return [r["title"] for r in rows]


def test_fts_folds_turkish_capital_i(conn):
    assert _titles(queries.search_articles(conn, "istanbul")) == [
        "İstanbul deprem çalışması"
    ]
    assert _titles(queries.search_articles(conn, "İstanbul")) == [
        "İstanbul deprem çalışması"
    ]


def test_fts_substring_and_punctuation(conn):
    assert _titles(queries.search_articles(conn, "deprem")) == [
        "İstanbul deprem çalışması"
    ]
    # hyphenated terms index and match as substrings
    assert _titles(queries.search_articles(conn, "covid-19")) == [
        "Covid-19 pandemisinin etkileri"
    ]
    assert _titles(queries.search_articles(conn, "pet/ct")) == [
        "Covid-19 pandemisinin etkileri"
    ]


def test_fts_multi_term_and(conn):
    rows = queries.search_articles(conn, "istanbul deprem")
    assert len(rows) == 1
    assert rows[0]["title"] == "İstanbul deprem çalışması"


def test_fts_counts_match_search(conn):
    total = queries.count_articles(conn, "istanbul")
    assert total == len(queries.search_articles(conn, "istanbul", limit=100)) == 1


def test_fts_searches_authors_and_journal_names(conn):
    rows = queries.search_articles(conn, "ali")
    assert _titles(rows) == ["İstanbul deprem çalışması"]
    rows = queries.search_articles(conn, "koç")
    assert _titles(rows) == ["Covid-19 pandemisinin etkileri"]
    rows = queries.search_articles(conn, "anadolu")
    assert len(rows) == 3


def test_fts_sync_after_update(conn):
    conn.execute(
        "UPDATE articles SET abstract = 'Derin dendritik yapılar' WHERE id = 2"
    )
    fts_sync(conn, 2)
    conn.commit()
    assert _titles(queries.search_articles(conn, "dendritik")) == [
        "Covid-19 pandemisinin etkileri"
    ]


def test_short_terms_fall_back_to_like(conn, monkeypatch):
    # 2-char terms cannot use the trigram index; LIKE path must still match
    rows = queries.search_articles(conn, "de")  # 'deprem' içinde geçer
    assert rows and rows[0]["title"] == "İstanbul deprem çalışması"

    def _no_fts(conn):
        return False

    monkeypatch.setattr(queries, "_fts_available", _no_fts)
    assert _titles(queries.search_articles(conn, "istanbul"))


def test_fts_phrase_rejects_double_quotes(conn):
    # a phrase containing '"' disqualifies FTS; LIKE path must not crash
    rows = queries.search_articles(conn, 'istanbul "x"')
    assert rows == []


def test_count_articles_without_fts(conn, monkeypatch):
    def _no_fts(conn):
        return False

    monkeypatch.setattr(queries, "_fts_available", _no_fts)
    assert queries.count_articles(conn, "istanbul") == 1
