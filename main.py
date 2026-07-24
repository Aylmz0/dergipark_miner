"""dergipark-miner ana orkestrasyon dosyasi.

Dergi listesini kesfet -> OAI ham XML cek -> parse et -> SQLite'a yaz ->
PDF indir -> OpenAlex ile zenginlestir -> JSONL export.

Checkpoint ile kaldigi yerden devam eder.

Kullanim:
    python main.py                          # tum pipeline
    python main.py --skip-enrich            # OpenAlex zenginlestirmesini atla
    python main.py --limit 50               # dergi basi max 50 makale
    python main.py --limit 0                # sinirsiz makale
"""

import argparse
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))

import httpx  # noqa: E402
from loguru import logger  # noqa: E402

from src.crawler import (  # noqa: E402
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
from src.enrich import enrich_all_articles, enrich_existing_citations  # noqa: E402
from src.exporter import export_all_articles  # noqa: E402
from src.storage import init_db, insert_article, insert_journals  # noqa: E402
from src.utils import AdaptiveDelay, load_checkpoint, save_checkpoint, setup_logger  # noqa: E402

CHECKPOINT_PATH = Path("checkpoint.json")


def _save_ckpt(ckpt: dict) -> None:
    """Checkpoint'i guvenli sekilde kaydeder."""
    try:
        save_checkpoint(CHECKPOINT_PATH, state=ckpt)
    except Exception:
        logger.warning("Checkpoint yazilamadi")


async def run(skip_enrich: bool = False, limit: int = 0):
    setup_logger()
    logger.info("=== dergipark-miner baslatildi ===")

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
            "Checkpoint yuklendi: {} dergi kesfedildi, {} islenmis, {} makale",
            len(discovered_journals),
            len(processed_shortnames),
            processed_articles,
        )

    # 1. Veritabani
    db_path = Path("data/Sobiad_Mining.sqlite")
    conn = init_db(db_path)
    logger.info("Veritaban hazir: {}", db_path)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=httpx.Timeout(60.0),
    ) as client:
        # 2. Dergi listesini kesfet (otomatik pagination)
        try:
            all_journals = await fetch_journals(client)
            discovered_journals = [j["short_name"] for j in all_journals]
            insert_journals(conn, all_journals)
            ckpt["discovered_journals"] = discovered_journals
            _save_ckpt(ckpt)
            logger.info("Dergi listesi: {} dergi bulundu, DB'ye kaydedildi", len(all_journals))
        except Exception:
            logger.exception("Dergi listesi cekilemedi")
            if not discovered_journals:
                all_journals = [{"short_name": "ihad", "name": ""}]
                discovered_journals = ["ihad"]
            else:
                all_journals = [{"short_name": s, "name": ""} for s in discovered_journals]

        # Checkpoint'te islenmis dergileri atla
        remaining = [j for j in all_journals if j["short_name"] not in processed_shortnames]
        if processed_shortnames:
            logger.info(
                "Checkpoint: {} dergi atlanacak, {} dergi islenecek",
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

        # 3. OAI ham XML cek + parse + insert (streaming)
        remaining_names = [j["short_name"] for j in remaining]
        max_per_journal = limit if limit > 0 else 999999

        def on_journal_done(shortname: str, xml_pages: list[str]):
            """Her dergi bitince cagrilir: XML'leri parse edip DB'ye yazar."""
            nonlocal processed_articles
            for xml_text in xml_pages:
                try:
                    meta = parse_article_metadata(xml_text)
                    insert_article(conn, meta)
                    processed_articles += 1
                except Exception as exc:
                    failed_articles.append({"shortname": shortname, "error": str(exc)})

            processed_shortnames.append(shortname)
            if shortname in failed_journals:
                del failed_journals[shortname]
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
            logger.info("Harvest tamamlandi: {} makale toplam", processed_articles)
        except Exception:
            logger.exception("Harvest basarisiz")

        # Basarisiz dergileri isaret
        for shortname in remaining_names:
            if shortname not in processed_shortnames and shortname not in failed_journals:
                failed_journals[shortname] = "harvest incomplete"
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

            # Once hata listesinden retry et
            pdf_errors = load_pdf_errors()
            for entry in pdf_errors[:]:
                oai_id = entry.get("oai_id", "")
                if is_permanently_failed(oai_id, max_retries=3):
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
                article_url = (
                    f"https://dergipark.org.tr/tr/pub/{journal}/article/{oai_id.split('/')[-1]}"
                )
                try:
                    pdf_url = await fetch_article_pdf_url(client, article_url)
                    if not pdf_url:
                        log_pdf_error(oai_id, article_url, "PDF link bulunamadi")
                        pdf_failed += 1
                        continue
                    safe_name = oai_id.replace("/", "_") + ".pdf"
                    pdf_path = Path("data/raw_pdfs") / safe_name
                    ok = await download_pdf(client, pdf_url, pdf_path)
                    if ok:
                        remove_pdf_error(oai_id)
                        pdf_downloaded += 1
                    else:
                        log_pdf_error(oai_id, article_url, "PDF indirilemedi")
                        pdf_failed += 1
                except Exception as exc:
                    log_pdf_error(oai_id, article_url, str(exc))
                    pdf_failed += 1

            # Sonra normal indirme
            for _article_id, oai_id, journal in articles:
                if not oai_id:
                    continue
                safe_name = oai_id.replace("/", "_") + ".pdf"
                pdf_path = Path("data/raw_pdfs") / safe_name
                if pdf_path.exists():
                    continue
                if is_permanently_failed(oai_id, max_retries=3):
                    continue

                article_url = (
                    f"https://dergipark.org.tr/tr/pub/{journal}/article/{oai_id.split('/')[-1]}"
                )
                try:
                    pdf_url = await fetch_article_pdf_url(client, article_url)
                    if not pdf_url:
                        log_pdf_error(oai_id, article_url, "PDF link bulunamadi")
                        pdf_failed += 1
                        continue
                    ok = await download_pdf(client, pdf_url, pdf_path)
                    if ok:
                        remove_pdf_error(oai_id)
                        pdf_downloaded += 1
                    else:
                        log_pdf_error(oai_id, article_url, "PDF indirilemedi")
                        pdf_failed += 1
                except Exception as exc:
                    log_pdf_error(oai_id, article_url, str(exc))
                    pdf_failed += 1

            ckpt["pdf_downloaded"] = pdf_downloaded
            ckpt["pdf_failed"] = pdf_failed
            _save_ckpt(ckpt)
            logger.info("PDF indirme: {} basarili, {} basarisiz", pdf_downloaded, pdf_failed)
        except Exception:
            logger.exception("PDF indirme hatasi")

        # 5. OpenAlex zenginlestirme
        if skip_enrich:
            logger.info("OpenAlex zenginlestirme atlandi (--skip-enrich)")
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
                    "Zenginlestirme: enriched={}, citations={}, failed={}",
                    result["enriched"],
                    result["citations_added"],
                    result["failed"],
                )
            except Exception:
                logger.exception("Zenginlestirme hatasi")

            # 6. Citation DOI/baglanti zenginlestirme
            try:
                result2 = await enrich_existing_citations(
                    client,
                    conn,
                    concurrency=3,
                    batch_size=100,
                )
                logger.info(
                    "Citation zenginlestirme: enriched={}, linked={}, failed={}",
                    result2["enriched"],
                    result2["linked_to_our_db"],
                    result2["failed"],
                )
            except Exception:
                logger.exception("Citation zenginlestirme hatasi")

    # 7. Export
    try:
        export_all_articles(conn, Path("data/exports/all_articles.jsonl"))
        logger.info("Export tamamlandi: data/exports/all_articles.jsonl")
    except Exception:
        logger.exception("Export hatasi")

    # Son checkpoint
    _save_ckpt(ckpt)

    conn.close()
    logger.info("=== dergipark-miner bitti ===")


if __name__ == "__main__":
    import asyncio

    parser = argparse.ArgumentParser(description="dergipark-miner: academic data mining tool")
    parser.add_argument(
        "--skip-enrich", action="store_true", help="OpenAlex zenginlestirmesini atla"
    )
    parser.add_argument("--limit", type=int, default=0, help="Dergi basi max makale (0=sinirsiz)")
    args = parser.parse_args()

    asyncio.run(run(skip_enrich=args.skip_enrich, limit=args.limit))
