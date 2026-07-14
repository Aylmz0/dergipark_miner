"""DergiPark veri madenciliği için sabitler ve dergi keşif fonksiyonu."""

import asyncio
from selectolax.parser import HTMLParser

import httpx

from utils import logger, retry, setup_logger

# DergiPark OAI-PMH taban URL'i (canli testle 2026-07 teyit edildi, 200 + <OAI-PMH>).
# OAI-PMH dergi bazlidir; tekil dergi URL'i f"{DERGIPARK_OAI_BASE}/{dergi_kisa_adi}"
# seklindedir (orn. "https://dergipark.org.tr/api/public/oai/ihad").
DERGIPARK_OAI_BASE = "https://dergipark.org.tr/api/public/oai"

# DergiPark dergi kesif (index) sayfasi. Sayfalama "?page=N" ile yapilir.
DERGIPARK_JOURNALS_URL = "https://dergipark.org.tr/tr/pub/explore/journals"

# OpenAlex REST API taban URL'i (resmi dokumantasyona gore dogru).
OPENALEX_BASE = "https://api.openalex.org"

# Tum dis kaynakli HTTP isteklerinde kullanilacak varsayilan User-Agent.
DEFAULT_USER_AGENT = "dergipark-miner/0.1 (https://github.com/example/dergipark_miner)"


def _extract_slugs(html: str) -> set[str]:
    """HTML icindeki dergi linklerinden (/tr/pub/{slug}) slug listesini cikarir."""
    tree = HTMLParser(html)
    slugs: set[str] = set()
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href") or ""
        if not href.startswith("/tr/pub/"):
            continue
        slug = href[len("/tr/pub/") :].strip("/")
        if not slug or "/" in slug or slug.startswith("page"):
            continue
        slugs.add(slug)
    return slugs


@retry(max_attempts=3, backoff=1.0)
async def fetch_journal_shortnames(
    client: "httpx.AsyncClient", *, max_pages: int = 5
) -> list[str]:
    """DergiPark dergi kesif sayfasini asenkron gezip dergi kisa adlarini (slug) toplar.

    Fonksiyon disaridan bir httpx.AsyncClient alir; kendi client'ini uretmez.
    Sayfalar arasinda kisa bekleme yapar ve max_pages ile sinirlidir.
    Sadece slug listesi (str) doner, dergi metadatasi parse ETMEZ.
    """
    setup_logger()

    collected: set[str] = set()
    for page in range(1, max_pages + 1):
        url = DERGIPARK_JOURNALS_URL if page == 1 else f"{DERGIPARK_JOURNALS_URL}?page={page}"
        logger.info("Dergi listesi cekiliyor: {}", url)
        resp = await client.get(url, headers={"User-Agent": DEFAULT_USER_AGENT})
        resp.raise_for_status()
        page_slugs = _extract_slugs(resp.text)
        logger.info("Sayfa {}: {} dergi bulundu", page, len(page_slugs))
        collected |= page_slugs
        if page < max_pages:
            await asyncio.sleep(0.5)

    return sorted(collected)
