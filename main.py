"""dergipark-miner ana orkestrasyon dosyasi.

DergiOAI cek -> parse et -> SQLite'a yaz -> OpenAlex ile zenginlestir.
"""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))

import httpx  # noqa: E402
from loguru import logger  # noqa: E402
from tqdm import tqdm  # noqa: E402

from src.crawler import (  # noqa: E402
    DEFAULT_USER_AGENT,
    fetch_journal_shortnames,
    harvest_all_journals,
    parse_article_metadata,
)
from src.enrich import enrich_all_articles  # noqa: E402
from src.storage import init_db, insert_article  # noqa: E402
from src.utils import setup_logger  # noqa: E402


async def run():
    setup_logger()
    logger.info("=== dergipark-miner baslatildi ===")

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

        # 3. OAI ham XML cek (dusuk hizli)
        try:
            results = await harvest_all_journals(
                client,
                shortnames,
                concurrency=1,
                max_records_per_journal=10,
                inter_request_delay=2.0,
            )
            logger.info("Harvest tamamlandi: {} dergi", len(results))
        except Exception:
            logger.exception("Harvest basarisiz")
            results = {}

        # 4. Parse + insert
        total = 0
        for shortname, xml_pages in results.items():
            for xml_text in tqdm(xml_pages, desc=f"{shortname}", leave=False):
                try:
                    meta = parse_article_metadata(xml_text)
                    insert_article(conn, meta)
                    total += 1
                except Exception:
                    logger.exception("Parse/insert hatasi: {}", shortname)
        logger.info("Makaleler yazildi: {}", total)

        # 5. OpenAlex zenginlestirme
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

    conn.close()
    logger.info("=== dergipark-miner bitti ===")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
