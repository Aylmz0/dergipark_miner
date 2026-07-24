"""Dergipark-miner main orchestrator.

Pipeline: discover journals -> OAI-PMH harvest -> parse -> SQLite ->
PDF download -> OpenAlex enrichment -> JSONL export.

Resumes from checkpoint.

Usage:
    python main.py                  # full pipeline
    python main.py --skip-enrich    # skip OpenAlex enrichment
    python main.py --limit 50       # max 50 articles per journal
    python main.py --limit 0        # unlimited articles
"""

import argparse
from pathlib import Path

import httpx
from loguru import logger

from src.crawler import (
    DEFAULT_USER_AGENT,
    download_pdf,
    fetch_article_pdf_url,
    fetch_journals,
    harvest_all_journals,
    is_permanently_failed,
    load_pdf_errors,
    log_pdf_error,
    parse_article_metadata,
    remove_pdf_error,
)
from src.enrich import enrich_all_articles, enrich_existing_citations
from src.exporter import export_all_articles
from src.storage import init_db, insert_article, insert_journals
from src.utils import AdaptiveDelay, load_checkpoint, save_checkpoint, setup_logger

CHECKPOINT_PATH = Path("checkpoint.json")


def _save_ckpt(ckpt: dict) -> None:
    """Save checkpoint safely (atomic write)."""
    try:
        save_checkpoint(CHECKPOINT_PATH, state=ckpt)
    except Exception:
        logger.warning("Checkpoint yazilamadi")


async def run(skip_enrich: bool = False, limit: int = 0) -> None:
    """Run the full mining pipeline."""
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
                conn.close()
                return

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
        max_per_journal = limit if limit > 0 else 999999

        def on_journal_done(shortname: str, xml_pages: list[str]) -> None:
            """Called when each journal harvest completes: parse XML and write to DB."""
            nonlocal processed_articles
            for xml_text in xml_pages:
                try:
                    meta = parse_article_metadata(xml_text)
                    insert_article(conn, meta)
                    processed_articles += 1
                except Exception as exc:
                    failed_articles.append({"shortname": shortname, "error": str(exc)})

            if xml_pages:
                # Success — add to processed list
                processed_shortnames.append(shortname)
                if shortname in failed_journals:
                    del failed_journals[shortname]
            else:
                # Failed — add to failed list, do NOT add to processed
                if shortname not in failed_journals:
                    failed_journals[shortname] = "no records returned"

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

        # Mark remaining journals as incomplete
        for shortname in remaining_names:
            if shortname not in processed_shortnames and shortname not in failed_journals:
                failed_journals[shortname] = "harvest incomplete"

        # Trim failed_journals (keep checkpoint size small)
        if len(failed_journals) > 100:
            sorted_keys = sorted(failed_journals.keys())
            failed_journals = {k: failed_journals[k] for k in sorted_keys[-100:]}

        _save_ckpt(ckpt)

        # 4. PDF indirme
        try:
            articles = conn.execute(
                """
                SELECT a.id, a.oai_identifier, j.short_name
                FROM articles a
                JOIN journals j ON a.journal_id = j.id
                """
            ).fetchall()

            pdf_checkpoint_interval = 50
            pdf_counter = 0

            async def _download_one(
                oai_id: str, journal: str, pdf_path: Path
            ) -> tuple[bool, bool]:
                """Download a single PDF. Returns (downloaded, failed)."""
                article_url = (
                    f"https://dergipark.org.tr/tr/pub/{journal}/article/"
                    f"{oai_id.split('/')[-1]}"
                )
                try:
                    pdf_url = await fetch_article_pdf_url(client, article_url)
                    if not pdf_url:
                        log_pdf_error(oai_id, article_url, "PDF link not found")
                        return False, True
                    ok = await download_pdf(client, pdf_url, pdf_path)
                    if ok:
                        remove_pdf_error(oai_id)
                        return True, False
                    log_pdf_error(oai_id, article_url, "PDF download failed")
                    return False, True
                except httpx.HTTPStatusError as exc:
                    is_transient = exc.response.status_code in (429, 500, 502, 503)
                    log_pdf_error(oai_id, article_url, str(exc), is_transient=is_transient)
                    return False, True
                except Exception as exc:
                    log_pdf_error(oai_id, article_url, str(exc))
                    return False, True

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
                downloaded, failed = await _download_one(oai_id, journal, pdf_path)
                pdf_downloaded += downloaded
                pdf_failed += failed
                pdf_counter += 1
                if pdf_counter % pdf_checkpoint_interval == 0:
                    ckpt["pdf_downloaded"] = pdf_downloaded
                    ckpt["pdf_failed"] = pdf_failed
                    _save_ckpt(ckpt)

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
                downloaded, failed = await _download_one(oai_id, journal, pdf_path)
                pdf_downloaded += downloaded
                pdf_failed += failed
                pdf_counter += 1
                if pdf_counter % pdf_checkpoint_interval == 0:
                    ckpt["pdf_downloaded"] = pdf_downloaded
                    ckpt["pdf_failed"] = pdf_failed
                    _save_ckpt(ckpt)

            ckpt["pdf_downloaded"] = pdf_downloaded
            ckpt["pdf_failed"] = pdf_failed
            _save_ckpt(ckpt)
            logger.info("PDF download: {} succeeded, {} failed", pdf_downloaded, pdf_failed)
        except Exception:
            logger.exception("PDF download error")

        # 5. OpenAlex zenginlestirme
        if skip_enrich:
            logger.info("OpenAlex enrichment skipped (--skip-enrich)")
        else:

            def on_enrich_batch(done, citations, failed):
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
                    "Enrichment done: enriched={}, citations={}, failed={}",
                    result["enriched"],
                    result["citations_added"],
                    result["failed"],
                )
            except Exception:
                logger.exception("Enrichment error")

        # 6. Citation DOI resolution
        try:
            result2 = await enrich_existing_citations(
                client,
                conn,
                concurrency=3,
                batch_size=100,
            )
            logger.info(
                "Citation enrichment done: enriched={}, linked={}, failed={}",
                result2["enriched"],
                result2["linked_to_our_db"],
                result2["failed"],
            )
        except Exception:
            logger.exception("Citation enrichment error")

    # 7. Export
    try:
        export_all_articles(conn, Path("data/exports/all_articles.jsonl"))
        logger.info("Export done: data/exports/all_articles.jsonl")
    except Exception:
        logger.exception("Export error")

    # Final checkpoint
    _save_ckpt(ckpt)

    conn.close()
    logger.info("=== dergipark-miner finished ===")


if __name__ == "__main__":
    import asyncio

    parser = argparse.ArgumentParser(description="dergipark-miner: academic data mining tool")
    parser.add_argument(
        "--skip-enrich", action="store_true", help="Skip OpenAlex enrichment"
    )
    parser.add_argument("--limit", type=int, default=0, help="Max articles per journal (0=unlimited)")
    args = parser.parse_args()

    asyncio.run(run(skip_enrich=args.skip_enrich, limit=args.limit))
