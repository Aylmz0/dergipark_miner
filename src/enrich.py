"""OpenAlex uzerinden makale zenginlestirme: referans/atif listesi cekme."""

from __future__ import annotations

import httpx
from loguru import logger

from src.crawler import DEFAULT_USER_AGENT, OPENALEX_BASE
from src.storage import insert_citation
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
        # OpenAlex URL'den W-id cikar (orn. https://openalex.org/W123456 -> W123456)
        wid = ref_id.split("/")[-1] if "/" in ref_id else ref_id
        insert_citation(
            conn,
            citing_article_id=article_id,
            cited_openalex_id=wid,
        )
        count += 1

    return count
