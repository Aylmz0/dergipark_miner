"""DergiPark veri madenciliği için sabitler ve dergi keşif fonksiyonu."""

import asyncio
import random

import httpx
from selectolax.parser import HTMLParser

from utils import logger, retry, setup_logger

# DergiPark OAI-PMH taban URL'i (canli testle 2026-07 teyit edildi, 200 + <OAI-PMH>).
# OAI-PMH dergi bazlidir; tekil dergi URL'i f"{DERGIPARK_OAI_BASE}/{dergi_kisa_adi}"
# seklindedir (orn. "https://dergipark.org.tr/api/public/oai/ihad").
DERGIPARK_OAI_BASE = "https://dergipark.org.tr/api/public/oai"

# DergiPark dergi kesif (index) sayfasi. Sayfalama "?page=N" ile yapilir.
DERGIPARK_JOURNALS_URL = "https://dergipark.org.tr/tr/pub/explore/journals"

# OpenAlex REST API taban URL'i (resmi dokumantasyona gore dogru).
OPENALEX_BASE = "https://api.openalex.org"

# OAI-PMH ListRecords icin kullanilan metadata formati.
# Canli teyit (2026-07, dergi "ihad"): desteklenen prefix'ler oai_dc, oai_etdms,
# oai_marc, oai_mods; ListRecords ornegi:
#   {DERGIPARK_OAI_BASE}/{short_name}?verb=ListRecords&metadataPrefix=oai_dc  (200, sayfa basi 100 kayit)
OAI_METADATA_PREFIX = "oai_dc"

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
async def fetch_journal_shortnames(client: httpx.AsyncClient, *, max_pages: int = 5) -> list[str]:
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


@retry(max_attempts=3, backoff=1.0)
async def _fetch_oai(client: httpx.AsyncClient, url: str) -> str:
    """Tek bir OAI HTTP GET istegini retry ile çeker, ham XML gövdesini döndürür."""
    resp = await client.get(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    resp.raise_for_status()
    return resp.text


async def harvest_all_journals(
    client: httpx.AsyncClient,
    shortnames: list[str],
    *,
    concurrency: int = 5,
    max_records_per_journal: int = 50,
) -> dict[str, list[str]]:
    """Dergi listesindeki her dergi için OAI ListRecords ham XML'lerini asenkron toplar.

    Donus: {short_name: [raw_oai_xml_string, ...]}  (ham XML; parse EDILMEZ).
    - asyncio.Semaphore ile eszamanlilik sinirlanir.
    - Dergi istegi utils.retry ile sarilir; kalici hatada o dergi [] ile atlanir (crash yok).
    - Istekler arasi rastgele 0.5-1.5s bekleme ile rate-limit yapilir.
    - client disaridan alinir; fonksiyon icinde client uretmez.
    """
    setup_logger()
    semaphore = asyncio.Semaphore(concurrency)
    total = len(shortnames)
    completed = 0
    results: dict[str, list[str]] = {}

    async def _harvest_one(shortname: str, index: int) -> None:
        nonlocal completed
        xml_pages: list[str] = []
        async with semaphore:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            url = f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&metadataPrefix={OAI_METADATA_PREFIX}"
            collected_records = 0
            try:
                while collected_records < max_records_per_journal:
                    body = await _fetch_oai(client, url)
                    xml_pages.append(body)
                    collected_records += body.count("<record>")
                    if "<resumptionToken" not in body:
                        break
                    token = (
                        body.split("<resumptionToken", 1)[1]
                        .split(">", 1)[1]
                        .split("</resumptionToken>", 1)[0]
                        .strip()
                    )
                    if not token:
                        break
                    url = (
                        f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&resumptionToken={token}"
                    )
            except Exception as exc:  # retry sonrasi kalici hata
                logger.error("Dergi '{}' harvest edilemedi: {}", shortname, exc)
            finally:
                results[shortname] = xml_pages
                completed += 1
                logger.info("{}/{} dergi bitti: {}", completed, total, shortname)

    await asyncio.gather(*(_harvest_one(name, i) for i, name in enumerate(shortnames, 1)))
    return results
