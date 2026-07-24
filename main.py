"""dergipark-miner ana orkestrasyon dosyasi.

DergiOAI cek -> parse et -> SQLite'a yaz -> OpenAlex ile zenginlestir.
Checkpoint ile kaldigi yerden devam eder.

Kullanim:
    python main.py                  # tum pipeline
    python main.py --skip-enrich    # OpenAlex zenginlestirmesini atla
"""

import argparse
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))

import httpx  # noqa: E402
from loguru import logger  # noqa: E402
from tqdm import tqdm  # noqa: E402

from src.crawler import (  # noqa: E402
    DEFAULT_USER_AGENT,
    download_pdf,
    fetch_article_pdf_url,
    fetch_journal_shortnames,
    harvest_all_journals,
    log_pdf_error,
    parse_article_metadata,
)
from src.enrich import enrich_all_articles, enrich_existing_citations  # noqa: E402
from src.storage import init_db, insert_article  # noqa: E402
from src.utils import load_checkpoint, save_checkpoint, setup_logger  # noqa: E402

CHECKPOINT_PATH = Path("checkpoint.json")


async def run(skip_enrich: bool = False):
    setup_logger()
    logger.info("=== dergipark-miner baslatildi ===")

    # Checkpoint yukle
    try:
        ckpt = load_checkpoint(CHECKPOINT_PATH)
        processed_shortnames: list[str] = ckpt.get("processed_shortnames", [])
        processed_articles: int = ckpt.get("processed_articles", 0)
    except Exception:
        logger.warning("Checkpoint okunamadi, bastan basliyor")
        processed_shortnames = []
        processed_articles = 0

    if processed_shortnames:
        logger.info("Checkpoint yuklendi: {} dergi islenmis, {} makale", len(processed_shortnames), processed_articles)

    # 1. Veritabani
    db_path = Path("data/Sobiad_Mining.sqlite")
    conn = init_db(db_path)
    logger.info("Veritaban hazir: {}", db_path)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=httpx.Timeout(60.0),
    ) as client:
        # 2. Dergi listesini kesfet (kucuk batch)
        try:
            all_shortnames = await fetch_journal_shortnames(client, max_pages=1)
            shortnames = all_shortnames[:20]  # rate-limit icin sinirli
            logger.info("Dergi listesi: {} dergi bulundu, {} islenecek", len(all_shortnames), len(shortnames))
        except Exception:
            logger.exception("Dergi listesi cekilemedi; ornek liste kullaniyor")
            shortnames = ["ihad"]

        # Checkpoint'te islenmis dergileri atla
        remaining = [s for s in shortnames if s not in processed_shortnames]
        if processed_shortnames:
            logger.info("Checkpoint: {} dergi atlanacak, {} dergi islenecek", len(processed_shortnames), len(remaining))

        # 3. OAI ham XML cek (dusuk hizli)
        try:
            results = await harvest_all_journals(
                client,
                remaining,
                concurrency=1,
                max_records_per_journal=10,
                inter_request_delay=2.0,
            )
            logger.info("Harvest tamamlandi: {} dergi", len(results))
        except Exception:
            logger.exception("Harvest basarisiz")
            results = {}

        # 4. Parse + insert + checkpoint
        total = processed_articles
        for shortname, xml_pages in results.items():
            for xml_text in tqdm(xml_pages, desc=f"{shortname}", leave=False):
                try:
                    meta = parse_article_metadata(xml_text)
                    insert_article(conn, meta)
                    total += 1
                except Exception:
                    logger.exception("Parse/insert hatasi: {}", shortname)

            # Her dergi sonunda checkpoint yaz
            processed_shortnames.append(shortname)
            try:
                save_checkpoint(CHECKPOINT_PATH, state={
                    "processed_shortnames": processed_shortnames,
                    "processed_articles": total,
                })
            except Exception:
                logger.warning("Checkpoint yazilamadi")

        logger.info("Makaleler yazildi: {}", total)

        # 4b. PDF indirme
        try:
            articles = conn.execute(
                """
                SELECT a.id, a.oai_identifier, j.short_name
                FROM articles a
                JOIN journals j ON a.journal_id = j.id
                LIMIT 50
                """
            ).fetchall()
            pdf_downloaded = 0
            pdf_failed = 0
            for _article_id, oai_id, journal in articles:
                if not oai_id:
                    continue
                # PDF dosya adi: oai_id'deki / yerine _ ile
                safe_name = oai_id.replace("/", "_") + ".pdf"
                pdf_path = Path("data/raw_pdfs") / safe_name
                if pdf_path.exists():
                    continue  # zaten indirildi

                article_url = f"https://dergipark.org.tr/tr/pub/{journal}/article/{oai_id.split('/')[-1]}"
                try:
                    pdf_url = await fetch_article_pdf_url(client, article_url)
                    if not pdf_url:
                        log_pdf_error(oai_id, article_url, "PDF link bulunamadi")
                        pdf_failed += 1
                        continue
                    ok = await download_pdf(client, pdf_url, pdf_path)
                    if ok:
                        pdf_downloaded += 1
                    else:
                        log_pdf_error(oai_id, article_url, "PDF indirilemedi")
                        pdf_failed += 1
                except Exception as exc:
                    log_pdf_error(oai_id, article_url, str(exc))
                    pdf_failed += 1

            logger.info("PDF indirme: {} basarili, {} basarisiz", pdf_downloaded, pdf_failed)
        except Exception:
            logger.exception("PDF indirme hatasi")

        # 5. OpenAlex zenginlestirme
        if skip_enrich:
            logger.info("OpenAlex zenginlestirme atlandi (--skip-enrich)")
        else:
            try:
                result = await enrich_all_articles(client, conn, concurrency=3, batch_size=50)
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
                result2 = await enrich_existing_citations(client, conn, concurrency=3, batch_size=100)
                logger.info(
                    "Citation zenginlestirme: enriched={}, linked={}, failed={}",
                    result2["enriched"],
                    result2["linked_to_our_db"],
                    result2["failed"],
                )
            except Exception:
                logger.exception("Citation zenginlestirme hatasi")

    # Son checkpoint
    try:
        save_checkpoint(CHECKPOINT_PATH, state={
            "processed_shortnames": processed_shortnames,
            "processed_articles": total,
        })
    except Exception:
        logger.warning("Son checkpoint yazilamadi")

    conn.close()
    logger.info("=== dergipark-miner bitti ===")


if __name__ == "__main__":
    import asyncio

    parser = argparse.ArgumentParser(description="dergipark-miner: academic data mining tool")
    parser.add_argument("--skip-enrich", action="store_true", help="OpenAlex zenginlestirmesini atla")
    args = parser.parse_args()

    asyncio.run(run(skip_enrich=args.skip_enrich))
