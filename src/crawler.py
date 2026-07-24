"""DergiPark veri madenciligi icin sabitler, dergi kesif ve PDF indirme fonksiyonlari."""

import asyncio
import json
import random
import re
from datetime import UTC
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

from src.utils import logger, retry, setup_logger

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


def _extract_journals(html: str) -> list[dict]:
    """HTML icindeki dergi linklerinden slug + full name cikarir.

    DergiPark listeleme sayfasindaki dergi linkleri <h5> icindedir:
        <h5><a href="/tr/pub/{slug}">Full Dergi Adi</a></h5>
    Navigasyon linkleri (navbar, dropdown) filtrelenir.
    """
    tree = HTMLParser(html)
    journals: dict[str, str] = {}
    for h5 in tree.css("h5"):
        for anchor in h5.css("a"):
            href = anchor.attributes.get("href") or ""
            if not href.startswith("/tr/pub/"):
                continue
            slug = href[len("/tr/pub/") :].strip("/")
            if not slug or "/" in slug or slug.startswith("page"):
                continue
            name = (anchor.text() or "").strip()
            if slug not in journals:
                journals[slug] = name
    return [{"short_name": s, "name": n} for s, n in sorted(journals.items())]


def _extract_max_page(html: str) -> int:
    """HTML pagination kontrolunden max sayfa sayisini cikarir.

    DergiPark sayfasinda <ul class="pagination"> icinde ?page=N linkleri bulunur.
    Bulunamazsa 1 dondurur.
    """
    tree = HTMLParser(html)
    max_page = 1
    for link in tree.css("ul.pagination a"):
        href = link.attributes.get("href") or ""
        if "page=" in href:
            try:
                part = href.split("page=")[-1].split("&")[0]
                page_num = int(part)
                if page_num > max_page:
                    max_page = page_num
            except ValueError, IndexError:
                continue
    return max_page


def _extract_slugs(html: str) -> set[str]:
    """HTML icindeki dergi linklerinden (/tr/pub/{slug}) slug listesini cikarir."""
    return {j["short_name"] for j in _extract_journals(html)}


@retry(max_attempts=3, backoff=1.0)
async def fetch_journals(client: httpx.AsyncClient, *, max_pages: int = 0) -> list[dict]:
    """DergiPark dergi kesif sayfasini asenkron gezip dergi bilgilerini toplar.

    Fonksiyon disaridan bir httpx.AsyncClient alir; kendi client'ini uretmez.
    max_pages=0 ise HTML pagination'dan otomatik algılanır.
    Sayfalar arasinda kisa bekleme yapar.
    Donus: [{short_name: "ihad", name: "Islam Hukuku..."}, ...]
    """
    setup_logger()

    # Ilk sayfayi cek, max sayfayi algila
    logger.info("Dergi listesi cekiliyor: {}", DERGIPARK_JOURNALS_URL)
    resp = await client.get(
        DERGIPARK_JOURNALS_URL, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
    )
    resp.raise_for_status()
    first_page_journals = _extract_journals(resp.text)

    if max_pages == 0:
        max_pages = _extract_max_page(resp.text)
        logger.info("Otomalgilanan toplam sayfa: {}", max_pages)

    collected: dict[str, str] = {}
    for j in first_page_journals:
        collected[j["short_name"]] = j["name"]
    logger.info("Sayfa 1: {} dergi bulundu", len(first_page_journals))

    for page in range(2, max_pages + 1):
        url = f"{DERGIPARK_JOURNALS_URL}?page={page}"
        logger.info("Dergi listesi cekiliyor: {}", url)
        resp = await client.get(
            url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
        )
        resp.raise_for_status()
        page_journals = _extract_journals(resp.text)
        logger.info("Sayfa {}: {} dergi bulundu", page, len(page_journals))
        for j in page_journals:
            if j["short_name"] not in collected:
                collected[j["short_name"]] = j["name"]
        if page < max_pages:
            await asyncio.sleep(random.uniform(1.0, 2.0))

    return [{"short_name": s, "name": n} for s, n in sorted(collected.items())]


# backward compat alias
async def fetch_journal_shortnames(client: httpx.AsyncClient, *, max_pages: int = 5) -> list[str]:
    """fetch_journals'un eski arayuzu — sadece short_name listesi dondurur."""
    journals = await fetch_journals(client, max_pages=max_pages)
    return [j["short_name"] for j in journals]


@retry(max_attempts=3, backoff=1.0)
async def _fetch_oai(client: httpx.AsyncClient, url: str) -> str:
    """Tek bir OAI HTTP GET istegini retry ile çeker, ham XML gövdesini döndürür."""
    resp = await client.get(url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


async def harvest_all_journals(
    client: httpx.AsyncClient,
    shortnames: list[str],
    *,
    concurrency: int = 1,
    max_records_per_journal: int = 20,
    inter_request_delay: float = 1.5,
    adaptive_delay=None,
    on_journal_done=None,
) -> dict[str, list[str]] | None:
    """Dergi listesindeki her dergi icin OAI ListRecords ham XML'lerini asenkron toplar.

    on_journal_done callable verilirse: streaming mod — her dergi bitince
    on_journal_done(shortname, xml_pages) cagrilir, XML'ler RAM'de tutulmaz.
    Verilmezse: eski mod — {shortname: [xml_pages]} dondurur.

    - asyncio.Semaphore ile eszamanlilik sinirlanir.
    - Dergi istegi utils.retry ile sarilir; kalici hatada o dergi [] ile atlanir (crash yok).
    - inter_request_delay: istekler arasi bekleme (saniye); +/- %30 rastgelelik.
    - adaptive_delay: AdaptiveDelay nesnesi; 429'da gecikme artar, basarida azalir.
    - client disaridan alinir; fonksiyon icinde client uretmez.
    """
    setup_logger()
    semaphore = asyncio.Semaphore(concurrency)
    total = len(shortnames)
    completed = 0
    results: dict[str, list[str]] = {} if on_journal_done is None else None

    async def _harvest_one(shortname: str, index: int) -> None:
        nonlocal completed
        xml_pages: list[str] = []
        async with semaphore:
            delay = adaptive_delay.get(shortname) if adaptive_delay else inter_request_delay
            await asyncio.sleep(random.uniform(delay * 0.7, delay * 1.3))
            url = f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&metadataPrefix={OAI_METADATA_PREFIX}"
            collected_records = 0
            try:
                while collected_records < max_records_per_journal:
                    body = await _fetch_oai(client, url)
                    xml_pages.append(body)
                    collected_records += body.count("<record>")
                    if adaptive_delay:
                        adaptive_delay.on_success(shortname)
                    if "<resumptionToken" not in body:
                        break
                    token_node = HTMLParser(body).css_first("resumptionToken")
                    token = (token_node.text(strip=True) if token_node else "") or ""
                    if not token:
                        break
                    url = (
                        f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&resumptionToken={token}"
                    )
            except Exception as exc:  # retry sonrasi kalici hata
                if adaptive_delay and "429" in str(exc):
                    adaptive_delay.on_429(shortname)
                logger.error("Dergi '{}' harvest edilemedi: {}", shortname, exc)
            finally:
                if on_journal_done is not None:
                    on_journal_done(shortname, xml_pages)
                else:
                    results[shortname] = xml_pages
                completed += 1
                logger.info("{}/{} dergi bitti: {}", completed, total, shortname)

    await asyncio.gather(*(_harvest_one(name, i) for i, name in enumerate(shortnames, 1)))
    return results


def _node_text(node) -> str:
    """selectolax, CDATA icerigini HTML yorumuna cevirir; bu yuzden serialized HTML
    uzerinden etiketleri ve CDATA isaretlerini temizleyip metni cikarir."""
    html = node.html or ""
    inner = re.sub(r"^<[^<>]*>", "", html)
    inner = re.sub(r"<[^<>]*>$", "", inner)
    inner = inner.replace("<!--[CDATA[", "").replace("]]-->", "")
    return inner.strip()


def parse_article_metadata(xml_text: str) -> dict:
    """Tek bir OAI kaydinin (oai_dc) ham XML'inden makale metadatasini cikarir.

    Donus alanlari: title, doi, authors (list[str]), publication_date,
                     journal_shortname, oai_identifier.
    Alan bulunamazsa "" veya [] doner; hicbir zaman crash yapmaz.
    GÖREV 5'in dondurdugu ham OAI XML seklıyle uyumludur.
    """
    tree = HTMLParser(xml_text)
    titles: list[tuple[str, str]] = []  # (text, lang)
    creators: list[str] = []
    dates: list[str] = []
    identifiers: list[str] = []
    relations: list[str] = []
    sources: list[str] = []

    # header'daki <identifier> (oai:dergipark.org.tr:article/...)
    oai_id = ""
    header_node = tree.css_first("header identifier")
    if header_node:
        oai_id = _node_text(header_node)

    for node in tree.css("*"):
        tag = node.tag
        if not tag or not tag.startswith("dc:"):
            continue
        text = _node_text(node)
        if tag == "dc:title":
            lang = (node.attributes.get("xml:lang") or "").lower()
            titles.append((text, lang))
        elif tag == "dc:creator":
            creators.append(text)
        elif tag == "dc:date":
            dates.append(text)
        elif tag == "dc:identifier":
            identifiers.append(text)
        elif tag == "dc:relation":
            relations.append(text)
        elif tag == "dc:source":
            sources.append(text)

    # en baslik tercih et; yoksa ilkini al
    title = ""
    if titles:
        en_titles = [t for t, lang in titles if lang.startswith("en")]
        title = en_titles[0] if en_titles else titles[0][0]

    doi = ""
    for ident in identifiers:
        m = re.search(r"10\.\d{4,9}/[^\s\"'<]+", ident)
        if m:
            doi = m.group(0)
            break
        if "doi.org/" in ident:
            doi = ident.split("doi.org/")[-1].strip("/")
            break

    # journal_shortname: once relation'da, sonra source'da /pub/{slug} ara
    journal_shortname = ""
    for pool in (relations, sources):
        for item in pool:
            m = re.search(r"/pub/([^/\s]+)", item)
            if m:
                journal_shortname = m.group(1)
                break
        if journal_shortname:
            break

    # journal_name: dc:source icindeki dergi adi (URL olmayan ilk deger)
    journal_name = ""
    for src in sources:
        src = src.strip()
        if src and not src.startswith("http"):
            journal_name = src
            break

    return {
        "title": title,
        "doi": doi,
        "authors": [a for a in creators if a],
        "publication_date": dates[0] if dates else "",
        "journal_shortname": journal_shortname,
        "oai_identifier": oai_id,
        "journal_name": journal_name,
    }


# ---------------------------------------------------------------------------
# PDF indirme fonksiyonlari
# ---------------------------------------------------------------------------

PDF_ERRORS_PATH = Path("data/pdf_errors.json")


@retry(max_attempts=3, backoff=1.0)
async def fetch_article_pdf_url(client: httpx.AsyncClient, article_url: str) -> str | None:
    """Makale detay sayfasindan PDF indirme linkini ceker.

    DergiPark makale sayfalarinda PDF linki:
      - /tr/download/article-file/{id}  (en yaygin pattern)
      - /download/article/{id}
      - .pdf uzantili link
    selectolax ile HTML parse eder, ilk PDF linkini dondurur.
    Bulamazsa None dondurur.
    """
    logger.debug("PDF linki cekiliyor: {}", article_url)
    resp = await client.get(
        article_url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
    )
    resp.raise_for_status()

    tree = HTMLParser(resp.text)

    # 1. /download/article-file/ (DergiPark'in asil PDF pattern'i)
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href") or ""
        if "/download/article-file/" in href:
            return _resolve_pdf_url(article_url, href)

    # 2. "download" + ".pdf" iceren link
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href") or ""
        if "download" in href and href.endswith(".pdf"):
            return _resolve_pdf_url(article_url, href)

    # 3. .pdf uzantili link
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href") or ""
        if href.endswith(".pdf"):
            return _resolve_pdf_url(article_url, href)

    # 4. href icinde .pdf gecen herhangi bir link
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href") or ""
        if ".pdf" in href.lower():
            return _resolve_pdf_url(article_url, href)

    return None


def _resolve_pdf_url(base_url: str, href: str) -> str:
    """Goreli PDF URL'ini mutlak URL'ye cevirir."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        from urllib.parse import urlparse

        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return base_url.rstrip("/") + "/" + href


@retry(max_attempts=3, backoff=2.0)
async def download_pdf(
    client: httpx.AsyncClient,
    pdf_url: str,
    dest_path: Path,
    *,
    inter_request_delay: float = 2.0,
) -> bool:
    """PDF dosyasini stream modunda indirir.

    Args:
        client: httpx.AsyncClient
        pdf_url: PDF dosyasinin URL'i
        dest_path: Kaydedilecek dosya yolu (orn. data/raw_pdfs/oai_id.pdf)
        inter_request_delay: indirme sonrasi bekleme (rate-limit)

    Returns:
        True basarili, False basarisiz.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    logger.debug("PDF indiriliyor: {} -> {}", pdf_url, dest_path)
    try:
        async with client.stream(
            "GET", pdf_url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
        ) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    f.write(chunk)

        size_kb = dest_path.stat().st_size / 1024
        logger.info("PDF indirildi: {} ({:.1f} KB)", dest_path.name, size_kb)
        await asyncio.sleep(inter_request_delay)
        return True
    except Exception as exc:
        logger.error("PDF indirilemedi: {} -> {}", pdf_url, exc)
        return False


def load_pdf_errors() -> list[dict]:
    """pdf_errors.json dosyasindaki hata listesini okur."""
    if not PDF_ERRORS_PATH.exists():
        return []
    try:
        return json.loads(PDF_ERRORS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_pdf_errors(errors: list[dict]) -> None:
    """pdf_errors.json dosyasina hata listesini yazar."""
    PDF_ERRORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PDF_ERRORS_PATH.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")


def log_pdf_error(oai_id: str, article_url: str, error: str) -> None:
    """Basarisiz PDF indirmesini pdf_errors.json'a kaydeder.

    Ayni oai_id icin tekrar cagirilirsa retry_count artirilir.
    """
    from datetime import datetime

    errors = load_pdf_errors()
    existing = next((e for e in errors if e.get("oai_id") == oai_id), None)
    if existing:
        existing["retry_count"] = existing.get("retry_count", 1) + 1
        existing["error"] = error
        existing["last_attempt"] = datetime.now(UTC).isoformat()
    else:
        errors.append(
            {
                "oai_id": oai_id,
                "article_url": article_url,
                "error": error,
                "retry_count": 1,
                "last_attempt": datetime.now(UTC).isoformat(),
            }
        )
    save_pdf_errors(errors)


def is_permanently_failed(oai_id: str, max_retries: int = 3) -> bool:
    """Makale max retry sayisini asip asamadigini kontrol eder."""
    errors = load_pdf_errors()
    entry = next((e for e in errors if e.get("oai_id") == oai_id), None)
    if not entry:
        return False
    return entry.get("retry_count", 0) >= max_retries


def remove_pdf_error(oai_id: str) -> None:
    """Basarili indirmesinden sonra makaleyi hata listesinden siler."""
    errors = load_pdf_errors()
    errors = [e for e in errors if e.get("oai_id") != oai_id]
    save_pdf_errors(errors)
