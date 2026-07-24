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
async def fetch_openalex_work(
    client: httpx.AsyncClient, *, doi: str = "", title: str = ""
) -> dict | None:
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

    try:
        work = await fetch_openalex_work(client, doi=doi, title=title)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            logger.debug("Makale {} OpenAlex'de bulunamadi", article_id)
            return 0
        raise
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
    max_total: int = 0,
    on_batch_done=None,
) -> dict:
    """Tum makaleleri OpenAlex uzerinden zenginlestirir (referans/atif cekme).

    articles tablosundan (id, doi, title) WHERE enriched_at IS NULL ceker.
    while loop ile tum makaleler islenene kadar devam eder.
    on_batch_done(enriched, citations_added, failed) callback ile checkpoint saglanir.
    max_total > 0 ise o kadar makale islenir ve durur.
    Donus: {enriched: N, citations_added: M, failed: K}
    """
    sem = asyncio.Semaphore(concurrency)
    total_enriched = 0
    total_citations = 0
    total_failed = 0

    while True:
        if max_total > 0:
            remaining = max_total - total_enriched
            if remaining <= 0:
                break
            current_batch = min(batch_size, remaining)
        else:
            current_batch = batch_size

        rows = conn.execute(
            "SELECT id, doi, title FROM articles WHERE enriched_at IS NULL LIMIT ?",
            (current_batch,),
        ).fetchall()
        if not rows:
            break

        logger.info("Zenginlestirme batch: {} makale (toplam: {})", len(rows), total_enriched)

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
                    conn.execute(
                        "UPDATE articles SET enriched_at = datetime('now') WHERE id = ?",
                        (article_id,),
                    )
                    conn.commit()
                    enriched += 1
                    citations_added += count
                    await asyncio.sleep(0.15)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        # OpenAlex'de yok — isaretledik, tekrar denemeyecek
                        conn.execute(
                            "UPDATE articles SET enriched_at = datetime('now') WHERE id = ?",
                            (article_id,),
                        )
                        conn.commit()
                        enriched += 1
                    else:
                        # Gecici hata (500/502/503) — enriched_at SET ETME, tekrar denensin
                        failed += 1
                        logger.warning(
                            "Makale {} zenginlestirilemedi ({}): {}",
                            article_id,
                            exc.response.status_code,
                            exc,
                        )
                except Exception:
                    failed += 1
                    logger.exception("Makale {} zenginlestirilemedi", article_id)

        tasks = [_process(aid, doi or "", title or "") for aid, doi, title in rows]
        await asyncio.gather(*tasks, return_exceptions=True)

        total_enriched += enriched
        total_citations += citations_added
        total_failed += failed

        if on_batch_done:
            on_batch_done(total_enriched, total_citations, total_failed)

    logger.info(
        "Zenginlestirme tamamlandi: enriched={}, citations={}, failed={}",
        total_enriched,
        total_citations,
        total_failed,
    )
    return {"enriched": total_enriched, "citations_added": total_citations, "failed": total_failed}


async def enrich_existing_citations(
    client: httpx.AsyncClient,
    conn,
    *,
    concurrency: int = 3,
    batch_size: int = 100,
    max_total: int = 0,
    on_batch_done=None,
) -> dict:
    """Mevcut citations satirlarini OpenAlex ile zenginlestirir (DOI/title cozme).

    cited_openalex_id dolu ama cited_doi BOŞ olan citation'lari ceker.
    while loop ile tum citation'lar islenene kadar devam eder.
    Donus: {enriched: N, linked_to_our_db: K, failed: M}
    """
    sem = asyncio.Semaphore(concurrency)
    total_enriched = 0
    total_linked = 0
    total_failed = 0

    while True:
        if max_total > 0:
            remaining = max_total - total_enriched
            if remaining <= 0:
                break
            current_batch = min(batch_size, remaining)
        else:
            current_batch = batch_size

        rows = conn.execute(
            """
            SELECT id, cited_openalex_id FROM citations
            WHERE cited_openalex_id IS NOT NULL AND cited_openalex_id != ''
              AND (cited_doi IS NULL OR cited_doi = '')
            LIMIT ?
            """,
            (current_batch,),
        ).fetchall()

        if not rows:
            break

        logger.info(
            "Citation zenginlestirme batch: {} satir (toplam: {})", len(rows), total_enriched
        )

        enriched = 0
        linked = 0
        failed = 0

        async def _process(citation_id: int, openalex_id: str):
            nonlocal enriched, linked, failed
            async with sem:
                try:
                    info = await fetch_openalex_work_by_id(client, openalex_id)
                    if not info:
                        # Bulunamadi — isaretledik ki tekrar denemesin
                        conn.execute(
                            "UPDATE citations SET cited_doi = '__not_found__' WHERE id = ?",
                            (citation_id,),
                        )
                        conn.commit()
                        failed += 1
                        return

                    doi = info.get("doi") or ""
                    title = info.get("title") or ""

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
                except httpx.HTTPStatusError as exc:
                    # 404 bile olsa isaretledik
                    conn.execute(
                        "UPDATE citations SET cited_doi = '__not_found__' WHERE id = ?",
                        (citation_id,),
                    )
                    conn.commit()
                    enriched += 1
                    if exc.response.status_code != 404:
                        logger.warning("Citation {} zenginlestirilemedi: {}", citation_id, exc)
                except Exception:
                    failed += 1
                    logger.exception("Citation {} zenginlestirilemedi", citation_id)

        tasks = [_process(cid, oid) for cid, oid in rows]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Ilerleme yoksa dur (sonsuz dongu onleme)
        if enriched == 0 and failed > 0:
            logger.warning("Citation batch'te ilerleme yok, duruluyor ({} basarisiz)", failed)
            total_failed += failed
            break

        total_enriched += enriched
        total_linked += linked
        total_failed += failed

        if on_batch_done:
            on_batch_done(total_enriched, total_linked, total_failed)

    logger.info(
        "Citation zenginlestirme tamamlandi: enriched={}, linked={}, failed={}",
        total_enriched,
        total_linked,
        total_failed,
    )
    return {"enriched": total_enriched, "linked_to_our_db": total_linked, "failed": total_failed}
