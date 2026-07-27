"""Constants, journal discovery, and PDF download functions for DergiPark data mining."""

import asyncio
import json
import random
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from loguru import logger
from selectolax.parser import HTMLParser

from src.utils import AdaptiveDelay, retry, setup_logger

# DergiPark OAI-PMH base URL (verified live 2026-07, returns 200 + <OAI-PMH>).
# OAI-PMH is per-journal; single journal URL is "{DERGIPARK_OAI_BASE}/{short_name}"
# e.g. "https://dergipark.org.tr/api/public/oai/ihad".
DERGIPARK_OAI_BASE = "https://dergipark.org.tr/api/public/oai"

# DergiPark journal discovery (index) page. Pagination via "?page=N".
DERGIPARK_JOURNALS_URL = "https://dergipark.org.tr/tr/pub/explore/journals"

# OAI-PMH ListRecords metadata format.
# Live verified (2026-07, journal "ihad"): supported prefixes are oai_dc, oai_etdms,
# oai_marc, oai_mods; ListRecords example:
#   {DERGIPARK_OAI_BASE}/{short_name}?verb=ListRecords&metadataPrefix=oai_dc  (200, 100 records/page)
OAI_METADATA_PREFIX = "oai_dc"

# Default User-Agent for all external HTTP requests.
DEFAULT_USER_AGENT = "dergipark-miner/0.1 (https://github.com/example/dergipark_miner)"

# PDF retry limits
MAX_PDF_RETRIES = 10
MAX_IMMEDIATE_RETRIES = 3

# Paths
PDF_DIR = Path("data/raw_pdfs")


def pdf_destination(oai_id: str) -> Path:
    """Convert an OAI identifier to a PDF file path."""
    safe_name = oai_id.replace("/", "_") + ".pdf"
    return PDF_DIR / safe_name


def lookup_article_by_oai(conn, oai_id: str):
    """Look up article + journal by OAI identifier. Returns (article_id, short_name) or None."""
    row = conn.execute(
        "SELECT a.id, j.short_name FROM articles a "
        "JOIN journals j ON a.journal_id = j.id "
        "WHERE a.oai_identifier = ?",
        (oai_id,),
    ).fetchone()
    return row


def _extract_journals(html: str) -> list[dict]:
    """Extract journal slug and name from DergiPark listing page HTML.

    Parses <h5><a href="/tr/pub/{slug}">Name</a></h5> elements,
    filtering out navigation links.

    Args:
        html: Raw HTML of a DergiPark journals listing page.

    Returns:
        List of dicts with 'short_name' and 'name' keys, sorted by slug.
    """
    tree = HTMLParser(html)
    journals: dict[str, str] = {}
    for h5 in tree.css("h5"):
        for anchor in h5.css("a"):
            href = anchor.attributes.get("href") or ""
            if not href.startswith("/tr/pub/"):
                continue
            slug = href[len("/tr/pub/") :].strip("/")
            if not slug or "/" in slug:
                continue
            name = (anchor.text() or "").strip()
            if slug not in journals:
                journals[slug] = name
    return [{"short_name": s, "name": n} for s, n in sorted(journals.items())]


@retry(max_attempts=3, backoff=1.0)
async def fetch_journals(client: httpx.AsyncClient, *, max_pages: int = 1000) -> list[dict]:
    """Discover all journals from DergiPark listing pages via async pagination.

    Stops when: page returns 0 journals, no new journals found, or max_pages reached.

    Args:
        client: Async HTTP client.
        max_pages: Maximum number of pages to scan (safety limit).

    Returns:
        List of dicts with 'short_name' and 'name' keys, sorted alphabetically.
    """
    setup_logger()

    collected: dict[str, str] = {}
    page = 1

    while page <= max_pages:
        url = DERGIPARK_JOURNALS_URL if page == 1 else f"{DERGIPARK_JOURNALS_URL}?page={page}"
        logger.info("Fetching journal list: {}", url)
        resp = await client.get(
            url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
        )
        resp.raise_for_status()
        page_journals = _extract_journals(resp.text)

        if not page_journals:
            logger.info("Page returned 0 journals, stopping: page {}", page)
            break

        before = len(collected)
        for j in page_journals:
            collected[j["short_name"]] = j["name"]
        logger.info(
            "Page {}: {} journals found (total: {})", page, len(page_journals), len(collected)
        )

        # All journals already collected (out-of-bounds redirect guard)
        if len(collected) == before and page > 1:
            logger.info("No new journals added, stopping: page {}", page)
            break

        page += 1
        await asyncio.sleep(random.uniform(1.0, 2.0))

    return [{"short_name": s, "name": n} for s, n in sorted(collected.items())]


@retry(max_attempts=3, backoff=1.0)
async def _fetch_oai(client: httpx.AsyncClient, url: str) -> str:
    """Fetch a single OAI-PMH endpoint and return raw XML body.

    Args:
        client: Async HTTP client.
        url: Full OAI-PMH request URL.

    Returns:
        Raw XML response body as string.
    """
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
    adaptive_delay: AdaptiveDelay | None = None,
    on_journal_done: Callable[[str, list[str], bool, int], None] | None = None,
    on_page_done: Callable[[str, str, int, list[str]], None] | None = None,
    resume_tokens: dict[str, str] | None = None,
) -> dict[str, list[str]] | None:
    """Harvest OAI-PMH ListRecords XML for all journals concurrently.

    Streaming mode: if on_journal_done callback is provided, XML pages are
    passed to the callback immediately and not held in memory.

    Args:
        client: Async HTTP client.
        shortnames: List of journal short names to harvest.
        concurrency: Max concurrent requests (semaphore limit).
        max_records_per_journal: Max records to fetch per journal.
        inter_request_delay: Base delay between requests in seconds (+/- 30% jitter).
        adaptive_delay: AdaptiveDelay instance for 429 backoff.
        on_journal_done: Callback: on_journal_done(shortname, xml_pages, success, flushed_count).
        on_page_done: Callback: on_page_done(shortname, token, collected, xml_pages).
        resume_tokens: Dict mapping shortname to resumption token for resume.

    Returns:
        Dict mapping shortname to list of XML page strings (streaming mode returns None).
    """
    setup_logger()
    semaphore = asyncio.Semaphore(concurrency)
    total = len(shortnames)
    completed = 0
    results: dict[str, list[str]] = {} if on_journal_done is None else None

    async def _harvest_one(shortname: str, index: int) -> None:
        nonlocal completed
        xml_pages: list[str] = []
        flushed_count = 0
        harvest_success = False
        async with semaphore:
            delay = adaptive_delay.get(shortname) if adaptive_delay else inter_request_delay
            await asyncio.sleep(random.uniform(delay * 0.7, delay * 1.3))

            initial_url = f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&metadataPrefix={OAI_METADATA_PREFIX}"

            # Resume from saved token if available
            saved_token = (resume_tokens or {}).get(shortname, "")
            if saved_token:
                url = f"{DERGIPARK_OAI_BASE}/{shortname}?verb=ListRecords&resumptionToken={saved_token}"
                logger.debug("Journal '{}' resuming from saved token", shortname)
            else:
                url = initial_url

            collected_records = 0
            token_restarts = 0
            max_token_restarts = 3

            try:
                while collected_records < max_records_per_journal:
                    try:
                        body = await _fetch_oai(client, url)
                    except Exception as fetch_exc:
                        # Check for badResumptionToken error
                        error_msg = str(fetch_exc).lower()
                        is_token_error = (
                            "badresumptiontoken" in error_msg or "resumptiontoken" in error_msg
                        )
                        if is_token_error and token_restarts < max_token_restarts:
                            token_restarts += 1
                            logger.warning(
                                "Journal '{}' token error, restarting (attempt {}/{})",
                                shortname,
                                token_restarts,
                                max_token_restarts,
                            )
                            xml_pages.clear()
                            collected_records = 0
                            url = initial_url
                            await asyncio.sleep(random.uniform(2.0, 5.0))
                            continue
                        raise

                    xml_pages.append(body)
                    collected_records += len(HTMLParser(body).css("record"))
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
                    # Persist token for crash recovery + flush page to DB
                    if on_page_done:
                        on_page_done(shortname, token, collected_records, xml_pages)
                        flushed_count = len(xml_pages)
                harvest_success = True
            except Exception as exc:  # Permanent error after retry
                if adaptive_delay and "429" in str(exc):
                    adaptive_delay.on_429(shortname)
                logger.error("Journal '{}' harvest failed: {}", shortname, exc)
                xml_pages.clear()  # Don't process partial data
            finally:
                if on_journal_done is not None:
                    on_journal_done(
                        shortname, xml_pages, success=harvest_success, flushed_count=flushed_count
                    )
                else:
                    results[shortname] = xml_pages
                completed += 1
                logger.info("{}/{} journals done: {}", completed, total, shortname)

    await asyncio.gather(*(_harvest_one(name, i) for i, name in enumerate(shortnames, 1)))
    return results


def _node_text(node) -> str:
    """Extract text content from a selectolax node, handling CDATA sections.

    selectolax converts CDATA to HTML comments; this function strips
    surrounding tags and CDATA markers to return plain text.

    Args:
        node: A selectolax HTML node.

    Returns:
        Cleaned text content.
    """
    html = node.html or ""
    inner = re.sub(r"^<[^<>]*>", "", html)
    inner = re.sub(r"<[^<>]*>$", "", inner)
    inner = inner.replace("<!--[CDATA[", "").replace("]]-->", "")
    return inner.strip()


def _split_records(xml_text: str) -> list[str]:
    """Split an OAI-PMH ListRecords page into individual <record> XML strings.

    Each page contains up to 100 records. parse_article_metadata expects
    a single record, so we must split before calling it.

    Args:
        xml_text: Full OAI-PMH ListRecords XML page.

    Returns:
        List of individual <record>...</record> XML strings.
    """
    parts = xml_text.split("<record>")
    return ["<record>" + p.split("</record>")[0] + "</record>" for p in parts if "</record>" in p]


def parse_article_metadata(xml_text: str) -> dict:
    """Parse article metadata from a single OAI-PMH (oai_dc) XML record.

    Extracts title (prefers en-US), DOI, authors, publication date,
    journal shortname, OAI identifier, journal name, abstract, keywords,
    and language.

    Args:
        xml_text: Raw OAI-PMH XML for a single record.

    Returns:
        Dict with keys: title, doi, authors, publication_date,
        journal_shortname, oai_identifier, journal_name, abstract,
        keywords, language. Empty string/list for missing fields.
    """
    tree = HTMLParser(xml_text)
    titles: list[tuple[str, str]] = []  # (text, lang)
    creators: list[str] = []
    dates: list[str] = []
    identifiers: list[str] = []
    relations: list[str] = []
    sources: list[str] = []
    descriptions: list[str] = []
    subjects: list[str] = []
    languages: list[str] = []

    # Extract header <identifier> (oai:dergipark.org.tr:article/...)
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
        elif tag == "dc:description":
            descriptions.append(text)
        elif tag == "dc:subject":
            subjects.append(text)
        elif tag == "dc:language":
            languages.append(text)

    # Prefer English title; fall back to first available
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

    # journal_shortname: check relation first, then source for /pub/{slug}
    journal_shortname = ""
    for pool in (relations, sources):
        for item in pool:
            m = re.search(r"/pub/([^/\s]+)", item)
            if m:
                journal_shortname = m.group(1)
                break
        if journal_shortname:
            break

    # journal_name: first non-URL value in dc:source
    journal_name = ""
    for src in sources:
        src = src.strip()
        if src and not src.startswith("http"):
            journal_name = src
            break

    # abstract: longest dc:description (multiple may exist)
    abstract = ""
    if descriptions:
        abstract = max(descriptions, key=len).strip()

    # keywords: all dc:subject values
    keywords = [s.strip() for s in subjects if s.strip()]

    # language: first dc:language value
    language = languages[0].strip() if languages else ""

    return {
        "title": title,
        "doi": doi,
        "authors": [a for a in creators if a],
        "publication_date": dates[0] if dates else "",
        "journal_shortname": journal_shortname,
        "oai_identifier": oai_id,
        "journal_name": journal_name,
        "abstract": abstract,
        "keywords": keywords,
        "language": language,
    }


# ---------------------------------------------------------------------------
# PDF download functions
# ---------------------------------------------------------------------------

PDF_ERRORS_PATH = Path("data/pdf_errors.json")


def scrape_article_html_metadata(html: str) -> dict:
    """Extract rich metadata from DergiPark article page HTML <meta> tags.

    Extracts: abstract, keywords, language, orcid (list), article_type,
    issn, issue, references (list), publisher, volume, firstpage, lastpage,
    pages, pdf_url, journal_title.

    Args:
        html: Raw HTML of a DergiPark article detail page.

    Returns:
        Dict with extracted fields. Empty string/list for missing fields.
    """
    tree = HTMLParser(html)
    result: dict = {
        "abstract": "",
        "keywords": [],
        "language": "",
        "orcid": [],
        "article_type": "",
        "issn": "",
        "issue": "",
        "references": [],
        "publisher": "",
        "volume": "",
        "firstpage": "",
        "lastpage": "",
        "pages": "",
        "pdf_url": "",
        "journal_title": "",
    }

    for node in tree.css("meta"):
        name = (node.attributes.get("name") or "").lower()
        content = (node.attributes.get("content") or "").strip()
        if not content:
            continue

        if name == "citation_abstract":
            result["abstract"] = content
        elif name == "citation_keywords":
            result["keywords"] = [k.strip() for k in content.split(";") if k.strip()]
        elif name == "citation_language":
            result["language"] = content
        elif name == "citation_author_orcid":
            result["orcid"].append(content)
        elif name == "dc.type.articletype" or name == "citation_article_type":
            result["article_type"] = content
        elif name == "dc.source.issn" or name == "citation_issn":
            if not result["issn"]:
                result["issn"] = content
        elif name == "dc.source.issue" or name == "citation_issue":
            result["issue"] = content
        elif name == "citation_reference":
            result["references"].append(content)
        elif name == "citation_publisher":
            result["publisher"] = content
        elif name == "citation_volume":
            result["volume"] = content
        elif name == "citation_firstpage":
            result["firstpage"] = content
        elif name == "citation_lastpage":
            result["lastpage"] = content
        elif name == "dc.identifier.pagenumber":
            result["pages"] = content
        elif name == "citation_pdf_url":
            result["pdf_url"] = content
        elif (name == "citation_journal_title" or name == "dc.source") and not result[
            "journal_title"
        ]:
            result["journal_title"] = content

    return result


def _pdf_link_priority(href: str) -> int:
    """Rank a link href for PDF download priority (lower is better).

    Args:
        href: The href attribute from an <a> tag.

    Returns:
        Priority rank: 0=best (article-file download), 999=not a PDF link.
    """
    if "/download/article-file/" in href:
        return 0
    if "download" in href and href.endswith(".pdf"):
        return 1
    if href.endswith(".pdf"):
        return 2
    if ".pdf" in href.lower():
        return 3
    return 999


def _resolve_pdf_url(base_url: str, href: str) -> str:
    """Convert a relative PDF href to an absolute URL.

    Args:
        base_url: Base article URL for resolving relative paths.
        href: The href attribute from an <a> tag.

    Returns:
        Absolute URL string.
    """
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return base_url.rstrip("/") + "/" + href


@retry(max_attempts=3, backoff=2.0)
async def download_pdf(
    client: httpx.AsyncClient,
    pdf_url: str,
    dest_path: Path,
) -> bool:
    """Download a PDF file in streaming mode.

    Exceptions propagate to caller for proper transient/permanent classification.

    Args:
        client: httpx.AsyncClient.
        pdf_url: URL of the PDF file.
        dest_path: Destination file path (e.g. data/raw_pdfs/oai_id.pdf).

    Returns:
        True if download succeeded.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    logger.debug("Downloading PDF: {} -> {}", pdf_url, dest_path)
    async with client.stream(
        "GET", pdf_url, headers={"User-Agent": DEFAULT_USER_AGENT}, follow_redirects=True
    ) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=8192):
                f.write(chunk)

    size_kb = dest_path.stat().st_size / 1024
    logger.info("PDF downloaded: {} ({:.1f} KB)", dest_path.name, size_kb)
    return True


def load_pdf_errors() -> list[dict]:
    """Load the PDF error list from pdf_errors.json.

    Returns:
        List of error dicts, or empty list if file missing/invalid.
    """
    if not PDF_ERRORS_PATH.exists():
        return []
    try:
        return json.loads(PDF_ERRORS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_pdf_errors(errors: list[dict]) -> None:
    """Write the PDF error list to pdf_errors.json atomically (tmp + rename).

    Args:
        errors: List of error dicts to persist.
    """
    PDF_ERRORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PDF_ERRORS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(PDF_ERRORS_PATH)


def log_pdf_error(oai_id: str, article_url: str, error: str, is_transient: bool = False) -> None:
    """Log a failed PDF download to pdf_errors.json.

    If the same oai_id already exists, increments retry_count.
    Transient errors (is_transient=True) do not increment retry_count.

    Args:
        oai_id: OAI identifier of the article.
        article_url: URL of the article page.
        error: Error message string.
        is_transient: Whether the error is temporary (e.g. 429, 500).
    """
    errors = load_pdf_errors()
    existing = next((e for e in errors if e.get("oai_id") == oai_id), None)
    if existing:
        if not is_transient:
            existing["retry_count"] = existing.get("retry_count", 0) + 1
        existing["error"] = error
        existing["last_attempt"] = datetime.now(UTC).isoformat()
    else:
        errors.append(
            {
                "oai_id": oai_id,
                "article_url": article_url,
                "error": error,
                "retry_count": 0 if is_transient else 1,
                "last_attempt": datetime.now(UTC).isoformat(),
            }
        )
    save_pdf_errors(errors)


def is_permanently_failed(
    oai_id: str, max_retries: int = 3, cache: list[dict] | None = None
) -> bool:
    """Check if an article has exceeded the maximum retry count.

    Args:
        oai_id: OAI identifier of the article.
        max_retries: Maximum allowed retry attempts.
        cache: Pre-loaded pdf_errors list (avoids repeated disk reads).

    Returns:
        True if retry_count >= max_retries.
    """
    errors = cache if cache is not None else load_pdf_errors()
    entry = next((e for e in errors if e.get("oai_id") == oai_id), None)
    if not entry:
        return False
    return entry.get("retry_count", 0) >= max_retries


def remove_pdf_error(oai_id: str) -> None:
    """Remove an article from the PDF error list after successful download.

    Args:
        oai_id: OAI identifier of the article to remove.
    """
    errors = load_pdf_errors()
    errors = [e for e in errors if e.get("oai_id") != oai_id]
    save_pdf_errors(errors)
