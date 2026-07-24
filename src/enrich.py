"""OpenAlex API integration for article enrichment (is_oa only)."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable

import httpx
from loguru import logger

from src.crawler import DEFAULT_USER_AGENT, OPENALEX_BASE
from src.utils import retry

OPENALEX_MAIL = "dergipark-miner@example.com"


@retry(max_attempts=3, backoff=1.0)
async def fetch_openalex_work(
    client: httpx.AsyncClient, *, doi: str = "", title: str = ""
) -> dict | None:
    """Fetch a work from OpenAlex by DOI or title search.

    Args:
        client: Async HTTP client.
        doi: DOI string (without https://doi.org/ prefix).
        title: Title search query.

    Returns:
        Work dict from OpenAlex, or None if not found.
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

    if "id" in data:
        return data
    results = data.get("results", [])
    return results[0] if results else None


async def enrich_article_is_oa(
    client: httpx.AsyncClient,
    article_id: int,
    doi: str,
    title: str,
) -> int:
    """Fetch open_access.is_oa for one article from OpenAlex.

    Args:
        client: Async HTTP client.
        article_id: Article row ID (for logging only).
        doi: DOI string.
        title: Title string.

    Returns:
        1 if open access, 0 otherwise.
    """
    if not doi and not title:
        return 0

    try:
        work = await fetch_openalex_work(client, doi=doi, title=title)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            logger.debug("Article {} not found in OpenAlex", article_id)
            return 0
        raise
    if not work:
        return 0

    open_access = work.get("open_access") or {}
    return 1 if open_access.get("is_oa") else 0


async def enrich_all_articles(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    *,
    concurrency: int = 3,
    batch_size: int = 50,
    max_total: int = 0,
    on_batch_done: Callable[[int, int], None] | None = None,
) -> dict:
    """Enrich all unprocessed articles with is_oa from OpenAlex.

    Processes articles where enriched_at IS NULL in batches.

    Args:
        client: Async HTTP client.
        conn: SQLite connection.
        concurrency: Max concurrent OpenAlex requests.
        batch_size: Articles per batch.
        max_total: Stop after processing this many articles (0=unlimited).
        on_batch_done: Callback with (enriched, failed).

    Returns:
        Dict with enriched and failed counts.
    """
    sem = asyncio.Semaphore(concurrency)
    total_enriched = 0
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

        logger.info("Enrichment batch: {} articles (total: {})", len(rows), total_enriched)

        enriched = 0
        failed = 0

        async def _process(article_id: int, doi: str, title: str):
            nonlocal enriched, failed
            async with sem:
                try:
                    is_oa = await enrich_article_is_oa(client, article_id, doi, title)
                    conn.execute(
                        "UPDATE articles SET enriched_at = datetime('now'), is_oa = ? WHERE id = ?",
                        (is_oa, article_id),
                    )
                    conn.commit()
                    enriched += 1
                    await asyncio.sleep(0.15)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        conn.execute(
                            "UPDATE articles SET enriched_at = datetime('now'), is_oa = 0 WHERE id = ?",
                            (article_id,),
                        )
                        conn.commit()
                        enriched += 1
                    else:
                        failed += 1
                        logger.warning(
                            "Article {} enrichment failed ({}): {}",
                            article_id,
                            exc.response.status_code,
                            exc,
                        )
                except Exception:
                    failed += 1
                    logger.exception("Article {} enrichment failed", article_id)

        tasks = [_process(aid, doi or "", title or "") for aid, doi, title in rows]
        await asyncio.gather(*tasks, return_exceptions=True)

        total_enriched += enriched
        total_failed += failed

        if on_batch_done:
            on_batch_done(total_enriched, total_failed)

    logger.info(
        "Enrichment done: enriched={}, failed={}",
        total_enriched,
        total_failed,
    )
    return {"enriched": total_enriched, "failed": total_failed}
