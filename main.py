"""Dergipark-miner main orchestrator.

Pipeline: discover journals -> OAI-PMH harvest -> parse -> SQLite ->
PDF download + HTML metadata (is_oa from citation_pdf_url) -> JSONL export.

Resumes from checkpoint. Handles Ctrl+C gracefully.

Usage:
    python main.py                  # full pipeline
    python main.py --limit 50       # max 50 journals
    python main.py --limit 0        # unlimited journals
"""

import argparse
import asyncio
import contextlib
import json
import signal
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from loguru import logger

from src.crawler import (
    DEFAULT_USER_AGENT,
    MAX_IMMEDIATE_RETRIES,
    MAX_PDF_RETRIES,
    _pdf_link_priority,
    _resolve_pdf_url,
    _split_records,
    download_pdf,
    fetch_journals,
    harvest_all_journals,
    is_permanently_failed,
    load_pdf_errors,
    log_pdf_error,
    lookup_article_by_oai,
    parse_article_metadata,
    pdf_destination,
    remove_pdf_error,
    scrape_article_html_metadata,
)
from src.exporter import export_all_articles
from src.storage import init_db, insert_article, insert_journals, update_journal_article_count
from src.utils import AdaptiveDelay, load_checkpoint, save_checkpoint, setup_logger

CHECKPOINT_PATH = Path("checkpoint.json")
DB_PATH = Path("data/Sobiad_Mining.sqlite")
EXPORT_PATH = Path("data/exports/all_articles.jsonl")

# Pipeline limits
JOURNAL_RETRY_ROUNDS = 3
PDF_RETRY_ROUNDS = 5
MAX_FAILED_JOURNALS = 100
MAX_FAILED_ARTICLES = 100

# Shutdown flag for graceful Ctrl+C handling
_shutdown_requested = False


@dataclass
class PipelineState:
    """Mutable state shared across pipeline phases."""

    ckpt: dict
    processed_shortnames: list[str] = field(default_factory=list)
    failed_journals: dict[str, str] = field(default_factory=dict)
    failed_articles: list[dict] = field(default_factory=list)
    processed_articles: int = 0
    pdf_downloaded: int = 0
    pdf_failed: int = 0
    adaptive_delay: AdaptiveDelay | None = None
    resume_tokens: dict[str, str] = field(default_factory=dict)
    harvested_count: dict[str, int] = field(default_factory=dict)
    harvested_ids: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def from_checkpoint(cls, ckpt: dict) -> PipelineState:
        """Create state from loaded checkpoint."""
        return cls(
            ckpt=ckpt,
            processed_shortnames=ckpt.get("processed_shortnames", []),
            failed_journals=ckpt.get("failed_journals", {}),
            failed_articles=ckpt.get("failed_articles", []),
            processed_articles=ckpt.get("processed_articles", 0),
            pdf_downloaded=ckpt.get("pdf_downloaded", 0),
            pdf_failed=ckpt.get("pdf_failed", 0),
            resume_tokens=ckpt.get("resume_tokens", {}),
        )

    def save_ckpt(self) -> None:
        """Persist state to checkpoint file."""
        self.ckpt.update(
            {
                "processed_shortnames": self.processed_shortnames,
                "processed_articles": self.processed_articles,
                "failed_journals": self.failed_journals,
                "failed_articles": self.failed_articles,
                "adaptive_delays": self.adaptive_delay.to_dict() if self.adaptive_delay else {},
                "resume_tokens": self.resume_tokens,
                "pdf_downloaded": self.pdf_downloaded,
                "pdf_failed": self.pdf_failed,
            }
        )
        _save_ckpt(self.ckpt)


def _save_ckpt(ckpt: dict) -> None:
    """Save checkpoint safely (atomic write)."""
    try:
        save_checkpoint(CHECKPOINT_PATH, state=ckpt)
    except Exception:
        logger.warning("Checkpoint write failed")


def _handle_signal(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutdown_requested
    if _shutdown_requested:
        logger.warning("Second Ctrl+C — forcing exit")
        raise SystemExit(1)
    _shutdown_requested = True
    logger.warning("Ctrl+C received — completing current work then exiting...")


def _check_shutdown():
    """Check if shutdown was requested. Raises to exit gracefully."""
    if _shutdown_requested:
        raise KeyboardInterrupt("Graceful shutdown")


def _update_article_metadata(conn, oai_id: str, html_meta: dict) -> None:
    """Update article with metadata scraped from HTML page.

    Only updates fields that are empty in the DB but present in html_meta.
    Does NOT overwrite OAI-PMH data.

    Args:
        conn: SQLite connection.
        oai_id: OAI identifier of the article.
        html_meta: Dict from scrape_article_html_metadata().
    """
    if not oai_id:
        return
    row = conn.execute(
        "SELECT id FROM articles WHERE oai_identifier = ?",
        (oai_id,),
    ).fetchone()
    if not row:
        return
    article_id = row[0]

    # Read current values to avoid overwriting
    cur = conn.execute(
        "SELECT abstract, keywords, language, journal_title, issn, publisher, "
        "volume, issue, firstpage, lastpage, pages, pdf_url, orcid, "
        "article_type, [references], author_institutions, metadata "
        "FROM articles WHERE id = ?",
        (article_id,),
    ).fetchone()
    if not cur:
        return

    (
        cur_abstract,
        cur_keywords,
        cur_language,
        cur_journal_title,
        cur_issn,
        cur_publisher,
        cur_volume,
        cur_issue,
        cur_firstpage,
        cur_lastpage,
        cur_pages,
        cur_pdf_url,
        cur_orcid,
        cur_article_type,
        cur_references,
        cur_author_institutions,
        cur_metadata,
    ) = cur

    updates: list[str] = []
    params: list = []

    # Helper: update if current is empty and new value exists
    def _add(col: str, cur_val, new_val, serialize: bool = False):
        if not cur_val and new_val:
            updates.append(f"{col} = ?")
            params.append(json.dumps(new_val, ensure_ascii=False) if serialize else new_val)

    _add("abstract", cur_abstract, html_meta.get("abstract"))
    # Keywords: HTML meta wins over OAI dc:subject (OAI subjects are
    # unlabelled tr/en mix + category names; HTML citation_keywords is clean).
    if html_meta.get("keywords"):
        updates.append("keywords = ?")
        params.append(json.dumps(html_meta["keywords"], ensure_ascii=False))
    _add("language", cur_language, html_meta.get("language"))
    _add("journal_title", cur_journal_title, html_meta.get("journal_title"))
    _add("issn", cur_issn, html_meta.get("issn"))
    _add("publisher", cur_publisher, html_meta.get("publisher"))
    _add("volume", cur_volume, html_meta.get("volume"))
    _add("issue", cur_issue, html_meta.get("issue"))
    _add("firstpage", cur_firstpage, html_meta.get("firstpage"))
    _add("lastpage", cur_lastpage, html_meta.get("lastpage"))
    _add("pages", cur_pages, html_meta.get("pages"))
    _add("pdf_url", cur_pdf_url, html_meta.get("pdf_url"))
    _add("orcid", cur_orcid, html_meta.get("orcid"), serialize=True)
    _add("article_type", cur_article_type, html_meta.get("article_type"))
    _add("[references]", cur_references, html_meta.get("references"), serialize=True)
    _add(
        "author_institutions",
        cur_author_institutions,
        html_meta.get("author_institutions"),
        serialize=True,
    )

    # metadata: store full meta dict
    if not cur_metadata and html_meta:
        # Filter out empty values to keep metadata compact
        meta = {k: v for k, v in html_meta.items() if v}
        if meta:
            updates.append("metadata = ?")
            params.append(json.dumps(meta, ensure_ascii=False))

    # is_oa: DergiPark is always open access when citation_pdf_url exists
    if html_meta.get("pdf_url"):
        updates.append("is_oa = 1")

    if updates:
        params.append(article_id)
        conn.execute(
            f"UPDATE articles SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()


async def run(limit: int = 0) -> None:
    """Run the full mining pipeline."""
    global _shutdown_requested
    _shutdown_requested = False

    setup_logger()
    logger.info("=== dergipark-miner started ===")

    # 1. Database (init first — needed for checkpoint reload)
    conn = init_db(DB_PATH)
    logger.info("Database ready: {}", DB_PATH)

    # Checkpoint yukle
    ckpt = load_checkpoint(CHECKPOINT_PATH)
    state = PipelineState.from_checkpoint(ckpt)

    if state.processed_shortnames:
        logger.info(
            "Checkpoint loaded: {} processed, {} articles",
            len(state.processed_shortnames),
            state.processed_articles,
        )

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=httpx.Timeout(60.0),
    ) as client:
        try:
            await _run_pipeline(client, conn, state, limit=limit)
        except KeyboardInterrupt:
            logger.warning("Graceful shutdown — saving checkpoint...")
        except Exception:
            logger.exception("Pipeline error")
        finally:
            with contextlib.suppress(Exception):
                export_all_articles(conn, EXPORT_PATH)
            state.save_ckpt()
            conn.close()
            logger.info("=== dergipark-miner finished ===")


async def _run_pipeline(
    client: httpx.AsyncClient,
    conn,
    state: PipelineState,
    *,
    limit: int,
) -> None:
    """Core pipeline logic. Can be interrupted gracefully."""

    # 2. Discover journals (auto-pagination)
    try:
        all_journals = await fetch_journals(client)
        insert_journals(conn, all_journals)
        logger.info("Journal list: {} journals found, saved to DB", len(all_journals))
    except Exception:
        logger.exception("Failed to fetch journal list")
        # Reload from DB if fetch fails
        rows = conn.execute("SELECT short_name FROM journals").fetchall()
        if rows:
            all_journals = [{"short_name": r[0], "name": ""} for r in rows]
            logger.warning("Using {} journals from DB", len(all_journals))
        else:
            logger.error("Failed to fetch journals and no DB data, exiting")
            return

    _check_shutdown()

    # Skip already-processed journals from checkpoint
    remaining = [j for j in all_journals if j["short_name"] not in state.processed_shortnames]
    if state.processed_shortnames:
        logger.info(
            "Checkpoint: {} journals to skip, {} journals to process",
            len(state.processed_shortnames),
            len(remaining),
        )

    # Adaptive delay tracker (shared for OAI-PMH + PDF)
    state.adaptive_delay = AdaptiveDelay(
        default=2.0,
        min_delay=2.0,
        max_delay=60.0,
        backoff_factor=2.0,
        decay_factor=0.9,
    )
    if state.ckpt.get("adaptive_delays"):
        state.adaptive_delay = AdaptiveDelay.from_dict(state.ckpt["adaptive_delays"])

    # 3. OAI-PMH harvest + parse + insert (streaming)
    remaining_names = [j["short_name"] for j in remaining]
    if limit > 0:
        remaining_names = remaining_names[:limit]
        logger.info("Limit: processing {} journals (all articles each)", len(remaining_names))
    max_per_journal = 999999

    def _audit_journal(conn, shortname: str) -> list[str]:
        """Check DB vs expected count. Return missing oai_identifiers."""
        expected = state.harvested_count.get(shortname, 0)
        h_ids = state.harvested_ids.get(shortname, set())
        row = conn.execute(
            "SELECT j.id FROM journals j WHERE j.short_name = ?", (shortname,)
        ).fetchone()
        if not row:
            return []
        journal_id = row[0]
        db_ids = {
            r[0]
            for r in conn.execute(
                "SELECT oai_identifier FROM articles WHERE journal_id = ? AND oai_identifier IS NOT NULL",
                (journal_id,),
            )
        }
        missing = sorted(h_ids - db_ids)
        db_count = len(db_ids)
        if missing:
            logger.warning(
                "Audit '{}': DB={}, expected={}, missing={}: {}",
                shortname,
                db_count,
                expected,
                len(missing),
                missing[:10],
            )
        state.harvested_count.pop(shortname, None)
        state.harvested_ids.pop(shortname, None)
        return missing

    def on_journal_done(
        shortname: str, xml_pages: list[str], success: bool = True, flushed_count: int = 0
    ) -> None:
        """Called when each journal harvest completes: process remaining pages + audit + export."""
        # Process remaining unflushed pages (flushed pages were popped by on_page_done)
        journal_article_count = 0
        for xml_text in xml_pages:
            for record_xml in _split_records(xml_text):
                try:
                    meta = parse_article_metadata(record_xml)
                    insert_article(conn, meta)
                    state.processed_articles += 1
                    journal_article_count += 1
                    oid = meta.get("oai_identifier", "")
                    if oid:
                        state.harvested_ids.setdefault(shortname, set()).add(oid)
                except Exception as exc:
                    state.failed_articles.append({"shortname": shortname, "error": str(exc)})

        state.harvested_count[shortname] = state.harvested_count.get(shortname, 0) + journal_article_count

        # Trim failed_articles
        if len(state.failed_articles) > MAX_FAILED_ARTICLES:
            state.failed_articles = state.failed_articles[-MAX_FAILED_ARTICLES:]

        conn.commit()

        if success:
            # Audit: check DB vs expected
            missing = _audit_journal(conn, shortname)
            if missing:
                logger.warning(
                    "Journal '{}': {} missing articles, retrying on next run",
                    shortname,
                    len(missing),
                )
                if shortname not in state.failed_journals:
                    state.failed_journals[shortname] = f"audit: {len(missing)} missing"
            else:
                state.processed_shortnames.append(shortname)
                if shortname in state.failed_journals:
                    del state.failed_journals[shortname]
                state.resume_tokens.pop(shortname, None)
                update_journal_article_count(conn, shortname, journal_article_count)
        else:
            if shortname not in state.failed_journals:
                state.failed_journals[shortname] = "harvest failed"
            # Cleanup memory for failed journals
            state.harvested_count.pop(shortname, None)
            state.harvested_ids.pop(shortname, None)

        state.save_ckpt()

    def on_page_done(shortname: str, token: str, collected: int, xml_pages: list[str]) -> None:
        """Flush latest page to DB + save resumption token for crash recovery."""
        if xml_pages:
            last_page = xml_pages.pop(0)
            page_count = 0
            page_ids: set[str] = set()
            for record_xml in _split_records(last_page):
                try:
                    meta = parse_article_metadata(record_xml)
                    insert_article(conn, meta)
                    state.processed_articles += 1
                    page_count += 1
                    oid = meta.get("oai_identifier", "")
                    if oid:
                        page_ids.add(oid)
                except Exception as exc:
                    state.failed_articles.append({"shortname": shortname, "error": str(exc)})
                    if len(state.failed_articles) > MAX_FAILED_ARTICLES:
                        state.failed_articles = state.failed_articles[-MAX_FAILED_ARTICLES:]
            state.harvested_count[shortname] = state.harvested_count.get(shortname, 0) + page_count
            state.harvested_ids.setdefault(shortname, set()).update(page_ids)
            conn.commit()
        state.resume_tokens[shortname] = token
        state.ckpt["resume_tokens"] = state.resume_tokens
        _save_ckpt(state.ckpt)

    try:
        await harvest_all_journals(
            client,
            remaining_names,
            concurrency=1,
            max_records_per_journal=max_per_journal,
            inter_request_delay=2.0,
            adaptive_delay=state.adaptive_delay,
            on_journal_done=on_journal_done,
            on_page_done=on_page_done,
            resume_tokens=state.resume_tokens,
        )
        logger.info("Harvest done: {} articles total", state.processed_articles)
    except Exception:
        logger.exception("Harvest failed")

    _check_shutdown()

    # Journal retry: retry failed journals up to 3 rounds (like PDF retry)
    for journal_retry_round in range(1, JOURNAL_RETRY_ROUNDS + 1):
        if _shutdown_requested:
            break
        retry_journals = [s for s in state.failed_journals if s not in state.processed_shortnames]
        if not retry_journals:
            break
        logger.info(
            "Journal retry round {}/3: {} journals ({})",
            journal_retry_round,
            len(retry_journals),
            retry_journals[:5],
        )
        round_wait = 15 + journal_retry_round * 15
        await asyncio.sleep(round_wait)
        try:
            await harvest_all_journals(
                client,
                retry_journals,
                concurrency=1,
                max_records_per_journal=max_per_journal,
                inter_request_delay=2.0,
                adaptive_delay=state.adaptive_delay,
                on_journal_done=on_journal_done,
                on_page_done=on_page_done,
                resume_tokens=state.resume_tokens,
            )
        except Exception:
            logger.exception("Journal retry round {} failed", journal_retry_round)
        state.save_ckpt()

    # Mark remaining journals as incomplete
    for shortname in remaining_names:
        if shortname not in state.processed_shortnames and shortname not in state.failed_journals:
            state.failed_journals[shortname] = "harvest incomplete"

    # Trim failed_journals (keep checkpoint size small)
    if len(state.failed_journals) > MAX_FAILED_JOURNALS:
        sorted_keys = sorted(state.failed_journals.keys())
        state.failed_journals = {k: state.failed_journals[k] for k in sorted_keys[-MAX_FAILED_JOURNALS:]}

    state.save_ckpt()

    # 3b. Export after harvest (before PDF download)
    try:
        export_all_articles(conn, EXPORT_PATH)
        logger.info("Post-harvest export done: {}", EXPORT_PATH)
    except Exception:
        logger.exception("Post-harvest export error")

    # 4. PDF indirme (rate-limit aware: adaptive delay between downloads)
    try:
        # Streaming cursor: fetch in batches, not all at once
        _pdf_cursor = conn.execute(
            """
            SELECT a.id, a.oai_identifier, j.short_name
            FROM articles a
            JOIN journals j ON a.journal_id = j.id
            """
        )

        pdf_checkpoint_interval = 25
        pdf_counter = 0
        pdf_errors_cache = load_pdf_errors()
        _failed_set: set[str] = set()  # unique failures only

        async def _download_one(oai_id: str, journal: str, pdf_path: Path) -> tuple[bool, bool]:
            """Download a single PDF with immediate retry on transient errors."""
            article_url = (
                f"https://dergipark.org.tr/tr/pub/{journal}/article/{oai_id.split('/')[-1]}"
            )
            for attempt in range(MAX_IMMEDIATE_RETRIES):
                try:
                    # Fetch article page once: extract PDF URL + metadata
                    resp = await client.get(
                        article_url,
                        headers={"User-Agent": DEFAULT_USER_AGENT},
                        follow_redirects=True,
                    )
                    resp.raise_for_status()

                    # Extract PDF URL from HTML
                    from selectolax.parser import HTMLParser as _HP

                    tree = _HP(resp.text)
                    best: str | None = None
                    best_priority = 999
                    for anchor in tree.css("a"):
                        href = anchor.attributes.get("href") or ""
                        if not href:
                            continue
                        priority = _pdf_link_priority(href)
                        if priority < best_priority:
                            best = href
                            best_priority = priority
                            if priority == 0:
                                break
                    pdf_url = _resolve_pdf_url(article_url, best) if best else None

                    # Extract metadata from HTML and update DB
                    try:
                        html_meta = scrape_article_html_metadata(resp.text)
                        _update_article_metadata(conn, oai_id, html_meta)
                    except Exception:
                        pass  # Non-fatal: metadata scrape failure shouldn't block PDF

                    if not pdf_url:
                        log_pdf_error(oai_id, article_url, "PDF link not found")
                        return False, True
                    ok = await download_pdf(client, pdf_url, pdf_path)
                    if ok:
                        remove_pdf_error(oai_id)
                        state.adaptive_delay.on_success(journal)
                        return True, False
                    log_pdf_error(oai_id, article_url, "PDF download failed")
                    return False, True
                except httpx.HTTPStatusError as exc:
                    is_transient = exc.response.status_code in (429, 500, 502, 503)
                    if exc.response.status_code == 429:
                        state.adaptive_delay.on_429(journal)
                        if attempt < MAX_IMMEDIATE_RETRIES - 1:
                            current_delay = state.adaptive_delay.get(journal)
                            wait = current_delay * (attempt + 1)
                            logger.warning(
                                "429 on PDF (attempt {}/{}), retry in {:.1f}s",
                                attempt + 1,
                                MAX_IMMEDIATE_RETRIES,
                                wait,
                            )
                            await asyncio.sleep(wait)
                            continue
                    log_pdf_error(oai_id, article_url, str(exc), is_transient=is_transient)
                    return False, True
                except Exception as exc:
                    err_str = str(exc)
                    is_transient = any(
                        s in err_str
                        for s in ("Errno -3", "Errno -5", "NameError", "Temporary failure")
                    )
                    log_pdf_error(oai_id, article_url, err_str, is_transient=is_transient)
                    return False, True
            return False, True

        async def _do_download(oai_id: str, journal: str, pdf_path: Path) -> None:
            nonlocal pdf_counter
            downloaded, failed = await _download_one(oai_id, journal, pdf_path)
            state.pdf_downloaded += downloaded
            if failed and oai_id not in _failed_set:
                state.pdf_failed += 1
                _failed_set.add(oai_id)
            pdf_counter += 1
            if pdf_counter % pdf_checkpoint_interval == 0:
                state.save_ckpt()
            _check_shutdown()
            await asyncio.sleep(state.adaptive_delay.get(journal))

        # Retry previously failed PDFs first
        for entry in pdf_errors_cache[:]:
            oai_id = entry.get("oai_id", "")
            if is_permanently_failed(oai_id, max_retries=MAX_PDF_RETRIES, cache=pdf_errors_cache):
                continue
            row = lookup_article_by_oai(conn, oai_id)
            if not row:
                continue
            _, journal = row
            pdf_path = pdf_destination(oai_id)
            if pdf_path.exists():
                remove_pdf_error(oai_id)
                continue
            await _do_download(oai_id, journal, pdf_path)

        # Then normal download (streaming cursor)
        while True:
            batch = _pdf_cursor.fetchmany(500)
            if not batch:
                break
            for _article_id, oai_id, journal in batch:
                if not oai_id:
                    continue
                pdf_path = pdf_destination(oai_id)
                if pdf_path.exists():
                    remove_pdf_error(oai_id)
                    continue
                if is_permanently_failed(oai_id, max_retries=MAX_PDF_RETRIES, cache=pdf_errors_cache):
                    continue
                await _do_download(oai_id, journal, pdf_path)

        state.save_ckpt()
        logger.info("PDF download: {} succeeded, {} failed", state.pdf_downloaded, state.pdf_failed)

        # Retry loop: retry transient failures up to 5 more rounds
        for retry_round in range(1, PDF_RETRY_ROUNDS + 1):
            if _shutdown_requested:
                break
            pdf_errors_cache = load_pdf_errors()  # Reload after each round
            retry_candidates = [
                e
                for e in pdf_errors_cache
                if not is_permanently_failed(
                    e.get("oai_id", ""), max_retries=MAX_PDF_RETRIES, cache=pdf_errors_cache
                )
                and e.get("retry_count", 0) < MAX_PDF_RETRIES
            ]
            if not retry_candidates:
                break
            logger.info("PDF retry round {}: {} candidates", retry_round, len(retry_candidates))
            # Progressive wait between retry rounds: 30s, 45s, 60s
            round_wait = 15 + retry_round * 15
            await asyncio.sleep(round_wait)
            for entry in retry_candidates:
                if _shutdown_requested:
                    break
                oai_id = entry.get("oai_id", "")
                row = lookup_article_by_oai(conn, oai_id)
                if not row:
                    continue
                _, journal = row
                pdf_path = pdf_destination(oai_id)
                if pdf_path.exists():
                    remove_pdf_error(oai_id)
                    continue
                await _do_download(oai_id, journal, pdf_path)

        state.save_ckpt()
        logger.info("PDF final: {} succeeded, {} failed", state.pdf_downloaded, state.pdf_failed)
    except Exception:
        logger.exception("PDF download error")

    _check_shutdown()

    # 5. Export
    try:
        export_all_articles(conn, EXPORT_PATH)
        logger.info("Export done: {}", EXPORT_PATH)
    except Exception:
        logger.exception("Export error")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    parser = argparse.ArgumentParser(description="dergipark-miner: academic data mining tool")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max journals to process (0=unlimited, all articles fetched)",
    )
    args = parser.parse_args()

    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(run(limit=args.limit))
