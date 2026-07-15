"""OpenAlex uzerinden makale zenginlestirme: referans/atif listesi cekme."""

from __future__ import annotations

import asyncio

import httpx
from loguru import logger

from src.crawler import DEFAULT_USER_AGENT, OPENALEX_BASE
from src.storage import insert_citation, resolve_citation_targets
from src.utils import retry

OPENALEX_MAIL = "dergipark-miner@example.com"


@retry(max_attempts=3, backoff=1.0)
async def fetch_openalex_work(client: httpx.AsyncClient, *, doi: str = "", title: str = "") -> dict | None:
    """OpenAlex'den bir work JSON'u ceker.

    Once DOI ile arar (dogrudan /works/https://doi.org/{doi}),
    DOI yoksa title ile /works?search={title}.
    Bulamazsa None dondurur.
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"}
    params: dict[str, str] = {"mailto": OPENALEX_MAIL}

    if doi:
        url = f"{OPENALEX_BASE}/works/https://doi.org/{doi}"
    elif title:
        url = f"{OPENALEX_BASE}/works"
        params["search"] = title
    else:
        return None

    logger.debug("OpenAlex fetch: url={}", url)
    r = await client.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()

    # DOI ile dogrudan cekimde sonuc tek dict; search ile results listesi
    if "id" in data:
        return data
    results = data.get("results", [])
    return results[0] if results else None


@retry(max_attempts=3, backoff=1.0)
async def fetch_openalex_work_by_id(client: httpx.AsyncClient, work_id: str) -> dict | None:
    """OpenAlex'den bir work id'si (Wxxxx) ile doi ve title bilgisini ceker.

    Donus: {"doi": "...", "title": "..."} veya None.
    """
    url = f"{OPENALEX_BASE}/works/{work_id}"
    params = {"mailto": OPENALEX_MAIL}
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"}

    logger.debug("OpenAlex fetch by id: url={}", url)
    r = await client.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()

    if "doi" not in data and "title" not in data:
        return None
    return {"doi": data.get("doi") or "", "title": data.get("title") or ""}


async def enrich_article_citations(
    client: httpx.AsyncClient,
    article_id: int,
    article: dict,
    conn,
) -> int:
    """Bir makalenin referans listesini OpenAlex'den cekip citations tablosuna yazar.

    article dict'inden doi veya title ile OpenAlex'de work arar.
    Bulursa referenced_works listesindeki her makale icin insert_citation cagirir.
    Kac referans yazildiysa dondurur.
    """
    doi = article.get("doi") or ""
    title = article.get("title") or ""

    if not doi and not title:
        return 0

    work = await fetch_openalex_work(client, doi=doi, title=title)
    if not work:
        return 0

    ref_ids = work.get("referenced_works") or []
    count = 0

    for ref_id in ref_ids:
        wid = ref_id.split("/")[-1] if "/" in ref_id else ref_id
        insert_citation(
            conn,
            citing_article_id=article_id,
            cited_openalex_id=wid,
        )
        count += 1

    return count


async def enrich_all_articles(
    client: httpx.AsyncClient,
    conn,
    *,
    concurrency: int = 3,
    batch_size: int = 50,
) -> dict:
    """Tum makaleleri OpenAlex uzerinden zenginlestirir (referans/atif cekme).

    articles tablosundan (id, doi, title) LIMIT batch_size ceker.
    Her makale icin enrich_article_citations'ı semaphore ile cagirir.
    Donus: {enriched: N, citations_added: M, failed: K}
    """
    sem = asyncio.Semaphore(concurrency)

    rows = conn.execute(
        "SELECT id, doi, title FROM articles LIMIT ?", (batch_size,)
    ).fetchall()
    if not rows:
        logger.info("Zenginlestirilecek makale yok")
        return {"enriched": 0, "citations_added": 0, "failed": 0}

    logger.info("Zenginlestirme basliyor: {} makale (concurrency={})", len(rows), concurrency)

    enriched = 0
    citations_added = 0
    failed = 0

    async def _process(article_id: int, doi: str, title: str):
        nonlocal enriched, citations_added, failed
        async with sem:
            try:
                count = await enrich_article_citations(
                    client, article_id, {"doi": doi, "title": title}, conn
                )
                enriched += 1
                citations_added += count
                logger.debug("Makale {} -> {} referans", article_id, count)
                await asyncio.sleep(0.15)  # rate-limit
            except Exception:
                failed += 1
                logger.exception("Makale {} zenginlestirilemedi", article_id)

    tasks = [_process(aid, doi or "", title or "") for aid, doi, title in rows]
    await asyncio.gather(*tasks)

    logger.info(
        "Zenginlestirme tamamlandi: enriched={}, citations={}, failed={}",
        enriched, citations_added, failed,
    )
    return {"enriched": enriched, "citations_added": citations_added, "failed": failed}


async def enrich_existing_citations(
    client: httpx.AsyncClient,
    conn,
    *,
    concurrency: int = 3,
    batch_size: int = 100,
) -> dict:
    """Mevcut citations satirlarini OpenAlex ile zenginlestirir (DOI/title cozme).

    cited_openalex_id dolu ama cited_doi BOŞ olan citation'lari ceker.
    Her biri icin fetch_openalex_work_by_id ile DOI/title bilgisini ceker,
    citations satirini UPDATE eder, ve bizim DB'mizde eslesme varsa
    cited_article_id'yi baglar.

    Donus: {enriched: N, linked_to_our_db: K, failed: M}
    """
    sem = asyncio.Semaphore(concurrency)

    rows = conn.execute(
        """
        SELECT id, cited_openalex_id FROM citations
        WHERE cited_openalex_id IS NOT NULL AND cited_openalex_id != ''
          AND (cited_doi IS NULL OR cited_doi = '')
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not rows:
        logger.info("Zenginlestirilecek citation yok")
        return {"enriched": 0, "linked_to_our_db": 0, "failed": 0}

    logger.info("Citation zenginlestirme basliyor: {} satir (concurrency={})", len(rows), concurrency)

    enriched = 0
    linked = 0
    failed = 0

    async def _process(citation_id: int, openalex_id: str):
        nonlocal enriched, linked, failed
        async with sem:
            try:
                info = await fetch_openalex_work_by_id(client, openalex_id)
                if not info:
                    failed += 1
                    return

                doi = info.get("doi") or ""
                title = info.get("title") or ""

                # resolve once: bizim DB'de var mi?
                article_id = None
                if doi:
                    article_id = resolve_citation_targets(conn, cited_doi=doi)

                conn.execute(
                    "UPDATE citations SET cited_doi = ?, cited_title = ?, cited_article_id = ? WHERE id = ?",
                    (doi or None, title or None, article_id, citation_id),
                )
                conn.commit()
                enriched += 1
                if article_id:
                    linked += 1
                await asyncio.sleep(0.15)
            except Exception:
                failed += 1
                logger.exception("Citation {} zenginlestirilemedi", citation_id)

    tasks = [_process(cid, oid) for cid, oid in rows]
    await asyncio.gather(*tasks)

    logger.info(
        "Citation zenginlestirme tamamlandi: enriched={}, linked={}, failed={}",
        enriched, linked, failed,
    )
    return {"enriched": enriched, "linked_to_our_db": linked, "failed": failed}
