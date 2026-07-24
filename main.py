"""Dergipark-miner main orchestrator.

Pipeline: discover journals -> OAI-PMH harvest -> parse -> SQLite ->
PDF download + HTML metadata -> OpenAlex is_oa -> JSONL export.

Resumes from checkpoint. Handles Ctrl+C gracefully.

Usage:
    python main.py                  # full pipeline
    python main.py --skip-enrich    # skip OpenAlex is_oa enrichment
    python main.py --limit 50       # max 50 journals
    python main.py --limit 0        # unlimited journals
"""

import argparse
import asyncio
import contextlib
import json
import signal
from pathlib import Path

import httpx
from loguru import logger

from src.crawler import (
    DEFAULT_USER_AGENT,
    _pdf_link_priority,
    _resolve_pdf_url,
    _split_records,
    download_pdf,
    fetch_journals,
    harvest_all_journals,
    is_permanently_failed,
    load_pdf_errors,
    log_pdf_error,
    parse_article_metadata,
    remove_pdf_error,
    scrape_article_html_metadata,
)
from src.enrich import enrich_all_articles
from src.exporter import export_all_articles
from src.storage import init_db, insert_article, insert_journals, update_journal_article_count
from src.utils import AdaptiveDelay, load_checkpoint, save_checkpoint, setup_logger

CHECKPOINT_PATH = Path("checkpoint.json")

# Shutdown flag for graceful Ctrl+C handling
_shutdown_requested = False


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
        "article_type, [references], metadata FROM articles WHERE id = ?",
        (article_id,),
    ).fetchone()
    if not cur:
        return

    (cur_abstract, cur_keywords, cur_language, cur_journal_title, cur_issn,
     cur_publisher, cur_volume, cur_issue, cur_firstpage, cur_lastpage,
     cur_pages, cur_pdf_url, cur_orcid, cur_article_type, cur_references,
     cur_metadata) = cur

    updates: list[str] = []
    params: list = []

    # Helper: update if current is empty and new value exists
    def _add(col: str, cur_val, new_val, serialize: bool = False):
        if not cur_val and new_val:
            updates.append(f"{col} = ?")
            params.append(json.dumps(new_val, ensure_ascii=False) if serialize else new_val)

    _add("abstract", cur_abstract, html_meta.get("abstract"))
    _add("keywords", cur_keywords, html_meta.get("keywords"), serialize=True)
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

    # metadata: store full meta dict
    if not cur_metadata and html_meta:
        # Filter out empty values to keep metadata compact
        meta = {k: v for k, v in html_meta.items() if v}
        if meta:
            updates.append("metadata = ?")
            params.append(json.dumps(meta, ensure_ascii=False))

    if updates:
        params.append(article_id)
        conn.execute(
            f"UPDATE articles SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()


async def run(skip_enrich: bool = False, limit: int = 0) -> None:
    """Run the full mining pipeline."""
    global _shutdown_requested
    _shutdown_requested = False

    setup_logger()
    logger.info("=== dergipark-miner started ===")

    # Checkpoint yukle
    ckpt = load_checkpoint(CHECKPOINT_PATH)
    discovered_journals: list[str] = ckpt.get("discovered_journals", [])
    processed_shortnames: list[str] = ckpt.get("processed_shortnames", [])
    failed_journals: dict[str, str] = ckpt.get("failed_journals", {})
    failed_articles: list[dict] = ckpt.get("failed_articles", [])
    processed_articles: int = ckpt.get("processed_articles", 0)
    pdf_downloaded: int = ckpt.get("pdf_downloaded", 0)
    pdf_failed: int = ckpt.get("pdf_failed", 0)

    if processed_shortnames:
        logger.info(
            "Checkpoint loaded: {} journals discovered, {} processed, {} articles",
            len(discovered_journals),
            len(processed_shortnames),
            processed_articles,
        )

    # 1. Database
    db_path = Path("data/Sobiad_Mining.sqlite")
    conn = init_db(db_path)
    logger.info("Database ready: {}", db_path)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=httpx.Timeout(60.0),
    ) as client:
        try:
            await _run_pipeline(
                client,
                conn,
                ckpt,
                discovered_journals=discovered_journals,
                processed_shortnames=processed_shortnames,
                failed_journals=failed_journals,
                failed_articles=failed_articles,
                processed_articles=processed_articles,
                pdf_downloaded=pdf_downloaded,
                pdf_failed=pdf_failed,
                skip_enrich=skip_enrich,
                limit=limit,
            )
        except KeyboardInterrupt:
            logger.warning("Graceful shutdown — saving checkpoint...")
        except Exception:
            logger.exception("Pipeline error")
        finally:
            _save_ckpt(ckpt)
            conn.close()
            logger.info("=== dergipark-miner finished ===")


async def _run_pipeline(
    client: httpx.AsyncClient,
    conn,
    ckpt: dict,
    *,
    discovered_journals: list[str],
    processed_shortnames: list[str],
    failed_journals: dict[str, str],
    failed_articles: list[dict],
    processed_articles: int,
    pdf_downloaded: int,
    pdf_failed: int,
    skip_enrich: bool,
    limit: int,
) -> None:
    """Core pipeline logic. Can be interrupted gracefully."""

    # 2. Discover journals (auto-pagination)
    try:
        all_journals = await fetch_journals(client)
        discovered_journals = [j["short_name"] for j in all_journals]
        insert_journals(conn, all_journals)
        ckpt["discovered_journals"] = discovered_journals
        _save_ckpt(ckpt)
        logger.info("Journal list: {} journals found, saved to DB", len(all_journals))
    except Exception:
        logger.exception("Failed to fetch journal list")
        if discovered_journals:
            logger.warning("Using {} journals from checkpoint", len(discovered_journals))
            all_journals = [{"short_name": s, "name": ""} for s in discovered_journals]
        else:
            logger.error("Failed to fetch journals and no checkpoint data, exiting")
            return

    _check_shutdown()

    # Skip already-processed journals from checkpoint
    remaining = [j for j in all_journals if j["short_name"] not in processed_shortnames]
    if processed_shortnames:
        logger.info(
            "Checkpoint: {} journals to skip, {} journals to process",
            len(processed_shortnames),
            len(remaining),
        )

    # Adaptive delay tracker
    adaptive_delay = AdaptiveDelay(
        default=2.0,
        min_delay=0.5,
        max_delay=30.0,
        backoff_factor=2.0,
        decay_factor=0.9,
    )
    if ckpt.get("adaptive_delays"):
        adaptive_delay = AdaptiveDelay.from_dict(ckpt["adaptive_delays"])

    # 3. OAI-PMH harvest + parse + insert (streaming)
    remaining_names = [j["short_name"] for j in remaining]
    if limit > 0:
        remaining_names = remaining_names[:limit]
        logger.info("Limit: processing {} journals (all articles each)", len(remaining_names))
    max_per_journal = 999999

    def on_journal_done(shortname: str, xml_pages: list[str], success: bool = True) -> None:
        """Called when each journal harvest completes: parse XML and write to DB."""
        nonlocal processed_articles
        journal_article_count = 0
        for xml_text in xml_pages:
            for record_xml in _split_records(xml_text):
                try:
                    meta = parse_article_metadata(record_xml)
                    insert_article(conn, meta)
                    processed_articles += 1
                    journal_article_count += 1
                except Exception as exc:
                    failed_articles.append({"shortname": shortname, "error": str(exc)})

        if success and xml_pages:
            # Harvest completed successfully — mark as processed
            processed_shortnames.append(shortname)
            if shortname in failed_journals:
                del failed_journals[shortname]
            # Store per-journal article count
            update_journal_article_count(conn, shortname, journal_article_count)
        else:
            # Harvest failed or returned no data — NOT added to processed_shortnames
            # Will be retried on next run
            if shortname not in failed_journals:
                failed_journals[shortname] = "harvest failed" if not success else "no records returned"

        ckpt.update(
            {
                "processed_shortnames": processed_shortnames,
                "processed_articles": processed_articles,
                "failed_journals": failed_journals,
                "failed_articles": failed_articles,
                "adaptive_delays": adaptive_delay.to_dict(),
            }
        )
        _save_ckpt(ckpt)

    try:
        await harvest_all_journals(
            client,
            remaining_names,
            concurrency=1,
            max_records_per_journal=max_per_journal,
            inter_request_delay=2.0,
            adaptive_delay=adaptive_delay,
            on_journal_done=on_journal_done,
        )
        logger.info("Harvest done: {} articles total", processed_articles)
    except Exception:
        logger.exception("Harvest failed")

    _check_shutdown()

    # Mark remaining journals as incomplete
    for shortname in remaining_names:
        if shortname not in processed_shortnames and shortname not in failed_journals:
            failed_journals[shortname] = "harvest incomplete"

    # Trim failed_journals (keep checkpoint size small)
    if len(failed_journals) > 100:
        sorted_keys = sorted(failed_journals.keys())
        failed_journals = {k: failed_journals[k] for k in sorted_keys[-100:]}

    _save_ckpt(ckpt)

    # 4. PDF indirme (rate-limit aware: adaptive delay between downloads)
    try:
        articles = conn.execute(
            """
            SELECT a.id, a.oai_identifier, j.short_name
            FROM articles a
            JOIN journals j ON a.journal_id = j.id
            """
        ).fetchall()

        pdf_checkpoint_interval = 25
        pdf_counter = 0
        pdf_delay = 1.5

        async def _download_one(
            oai_id: str, journal: str, pdf_path: Path
        ) -> tuple[bool, bool]:
            """Download a single PDF with immediate retry on transient errors."""
            nonlocal pdf_delay
            article_url = (
                f"https://dergipark.org.tr/tr/pub/{journal}/article/"
                f"{oai_id.split('/')[-1]}"
            )
            max_immediate_retries = 3
            for attempt in range(max_immediate_retries):
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
                        pdf_delay = max(pdf_delay * 0.9, 1.0)
                        return True, False
                    log_pdf_error(oai_id, article_url, "PDF download failed")
                    return False, True
                except httpx.HTTPStatusError as exc:
                    is_transient = exc.response.status_code in (429, 500, 502, 503)
                    if exc.response.status_code == 429:
                        pdf_delay = min(pdf_delay * 2.0, 30.0)
                        if attempt < max_immediate_retries - 1:
                            wait = pdf_delay * (attempt + 1)
                            logger.warning(
                                "429 on PDF (attempt {}/{}), retry in {:.1f}s",
                                attempt + 1, max_immediate_retries, wait,
                            )
                            await asyncio.sleep(wait)
                            continue
                    log_pdf_error(oai_id, article_url, str(exc), is_transient=is_transient)
                    return False, True
                except Exception as exc:
                    log_pdf_error(oai_id, article_url, str(exc))
                    return False, True
            return False, True

        async def _do_download(oai_id: str, journal: str, pdf_path: Path) -> None:
            nonlocal pdf_downloaded, pdf_failed, pdf_counter
            downloaded, failed = await _download_one(oai_id, journal, pdf_path)
            pdf_downloaded += downloaded
            pdf_failed += failed
            pdf_counter += 1
            if pdf_counter % pdf_checkpoint_interval == 0:
                ckpt["pdf_downloaded"] = pdf_downloaded
                ckpt["pdf_failed"] = pdf_failed
                _save_ckpt(ckpt)
            _check_shutdown()
            await asyncio.sleep(pdf_delay)

        # Retry previously failed PDFs first
        pdf_errors = load_pdf_errors()
        for entry in pdf_errors[:]:
            oai_id = entry.get("oai_id", "")
            if is_permanently_failed(oai_id, max_retries=5):
                continue
            row = conn.execute(
                "SELECT a.id, j.short_name FROM articles a "
                "JOIN journals j ON a.journal_id = j.id "
                "WHERE a.oai_identifier = ?",
                (oai_id,),
            ).fetchone()
            if not row:
                continue
            _, journal = row
            safe_name = oai_id.replace("/", "_") + ".pdf"
            pdf_path = Path("data/raw_pdfs") / safe_name
            await _do_download(oai_id, journal, pdf_path)

        # Then normal download
        for _article_id, oai_id, journal in articles:
            if not oai_id:
                continue
            safe_name = oai_id.replace("/", "_") + ".pdf"
            pdf_path = Path("data/raw_pdfs") / safe_name
            if pdf_path.exists():
                continue
            if is_permanently_failed(oai_id, max_retries=5):
                continue
            await _do_download(oai_id, journal, pdf_path)

        ckpt["pdf_downloaded"] = pdf_downloaded
        ckpt["pdf_failed"] = pdf_failed
        _save_ckpt(ckpt)
        logger.info("PDF download: {} succeeded, {} failed", pdf_downloaded, pdf_failed)

        # Retry loop: retry transient (429) failures up to 3 more rounds
        for retry_round in range(1, 4):
            if _shutdown_requested:
                break
            pdf_errors = load_pdf_errors()
            retry_candidates = [
                e for e in pdf_errors
                if not is_permanently_failed(e.get("oai_id", ""), max_retries=5)
                and e.get("retry_count", 0) < 5
            ]
            if not retry_candidates:
                break
            logger.info("PDF retry round {}: {} candidates", retry_round, len(retry_candidates))
            pdf_delay = min(pdf_delay * 2.0, 30.0)
            for entry in retry_candidates:
                if _shutdown_requested:
                    break
                oai_id = entry.get("oai_id", "")
                row = conn.execute(
                    "SELECT a.id, j.short_name FROM articles a "
                    "JOIN journals j ON a.journal_id = j.id "
                    "WHERE a.oai_identifier = ?",
                    (oai_id,),
                ).fetchone()
                if not row:
                    continue
                _, journal = row
                safe_name = oai_id.replace("/", "_") + ".pdf"
                pdf_path = Path("data/raw_pdfs") / safe_name
                if pdf_path.exists():
                    continue
                await _do_download(oai_id, journal, pdf_path)

        ckpt["pdf_downloaded"] = pdf_downloaded
        ckpt["pdf_failed"] = pdf_failed
        _save_ckpt(ckpt)
        logger.info("PDF final: {} succeeded, {} failed", pdf_downloaded, pdf_failed)
    except Exception:
        logger.exception("PDF download error")

    _check_shutdown()

    # 5. OpenAlex zenginlestirme
    if skip_enrich:
        logger.info("OpenAlex enrichment skipped (--skip-enrich)")
    else:

        def on_enrich_batch(done, failed):
            ckpt["enriched_count"] = done
            _save_ckpt(ckpt)

        try:
            result = await enrich_all_articles(
                client,
                conn,
                concurrency=3,
                batch_size=50,
                on_batch_done=on_enrich_batch,
            )
            ckpt["enriched_count"] = result["enriched"]
            _save_ckpt(ckpt)
            logger.info(
                "Enrichment done: enriched={}, failed={}",
                result["enriched"],
                result["failed"],
            )
        except Exception:
            logger.exception("Enrichment error")

    # 6. Export
    try:
        export_all_articles(conn, Path("data/exports/all_articles.jsonl"))
        logger.info("Export done: data/exports/all_articles.jsonl")
    except Exception:
        logger.exception("Export error")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    parser = argparse.ArgumentParser(description="dergipark-miner: academic data mining tool")
    parser.add_argument(
        "--skip-enrich", action="store_true", help="Skip OpenAlex enrichment"
    )
    parser.add_argument("--limit", type=int, default=0, help="Max journals to process (0=unlimited, all articles fetched)")
    args = parser.parse_args()

    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(run(skip_enrich=args.skip_enrich, limit=args.limit))
