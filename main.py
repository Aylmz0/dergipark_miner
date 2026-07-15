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

from src.crawler import DEFAULT_USER_AGENT, parse_article_metadata  # noqa: E402
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

    # 2. Dergi listesi (sabit ornek; gercek fetch sonra eklenir)
    sample_journals = ["ihad", "tip"]

    # 3. OAI ham XML -> parse -> insert
    sample_records = {
        "ihad": [
            """<?xml version="1.0"?>
            <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
              <ListRecords>
                <record>
                  <metadata>
                    <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/">
                      <dc:title><![CDATA[Ornek Makale I]]></dc:title>
                      <dc:creator><![CDATA[Yazar, Ali]]></dc:creator>
                      <dc:date>2024-01-01</dc:date>
                      <dc:relation>https://dergipark.org.tr/en/pub/ihad</dc:relation>
                    </oai_dc:dc>
                  </metadata>
                </record>
              </ListRecords>
            </OAI-PMH>"""
        ],
    }

    total = 0
    for journal_name in tqdm(sample_journals, desc="Dergiler"):
        xml_pages = sample_records.get(journal_name, [])
        for xml_text in xml_pages:
            meta = parse_article_metadata(xml_text)
            insert_article(conn, meta)
            total += 1
            logger.debug("Kaydedildi: {}", meta.get("title", ""))

    logger.info("Makaleler yazildi: {}", total)

    # 4. OpenAlex zenginlestirme
    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=httpx.Timeout(30.0),
    ) as client:
        result = await enrich_all_articles(client, conn)
        logger.info(
            "Zenginlestirme: enriched={}, citations={}, failed={}",
            result["enriched"],
            result["citations_added"],
            result["failed"],
        )

    conn.close()
    logger.info("=== dergipark-miner bitti ===")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
