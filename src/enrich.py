"""OpenAlex uzerinden makale zenginlestirme: referans/atif listesi cekme."""

from __future__ import annotations

import httpx

from src.crawler import OPENALEX_BASE
from src.storage import insert_citation


async def fetch_openalex_work(client: httpx.AsyncClient, *, doi: str = "", title: str = "") -> dict | None:
    """OpenAlex'den bir work JSON'u ceker.

    Once DOI ile arar; DOI yoksa title ile.
    Bulamazsa None dondurur.
    """
    params: dict[str, str] = {}
    if doi:
        params["filter"] = f"doi:{doi}"
    elif title:
        params["filter"] = f"title.search:{title}"
    else:
        return None

    params["per_page"] = "1"

    try:
        r = await client.get(f"{OPENALEX_BASE}/works", params=params)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else None
    except Exception:
        return None


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
