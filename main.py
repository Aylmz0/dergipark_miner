"""dergipark-miner ana orkestrasyon dosyasi.

Minimal iskelet: src modullerini kullanarak asagidaki adimlari sirayla calistirir.
Gercek veri cekmez; loguru + tqdm ile ilerleme gosterir.
"""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))        # src paketini import icin
sys.path.insert(0, str(_root / "src")) # crawler icindeki "from utils import" icin

from loguru import logger  # noqa: E402
from tqdm import tqdm  # noqa: E402

from src.crawler import parse_article_metadata  # noqa: E402
from src.storage import init_db, insert_article  # noqa: E402
from src.utils import setup_logger  # noqa: E402


async def run():
    """Ornek akis iskeleti: dergi listesi -> ham XML -> parse -> SQLite."""

    setup_logger()
    logger.info("=== dergipark-miner baslatildi ===")

    # 1. Veritabanini hazirla
    db_path = Path("data/Sobiad_Mining.sqlite")
    conn = init_db(db_path)
    logger.info("Veritaban hazir: {}", db_path)

    # 2. Ornek dergi listesi (gercek ag cekmeden)
    sample_journals = ["ihad", "tip"]
    logger.info("Dergi listesi hazir: {} dergi", len(sample_journals))

    # 3. Ornek ham XML verisi (gercek OAI cekmeden)
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

    # 4. Parse et ve veritabanina yaz
    total = 0
    for journal_name in tqdm(sample_journals, desc="Dergiler"):
        xml_pages = sample_records.get(journal_name, [])
        for xml_text in xml_pages:
            meta = parse_article_metadata(xml_text)
            insert_article(conn, meta)
            total += 1
            logger.debug("Kaydedildi: {}", meta.get("title", ""))

    conn.close()
    logger.info("Tamamlandi: {} makale islendi", total)
    logger.info("=== dergipark-miner bitti ===")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
