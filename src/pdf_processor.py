"""opendataloader-pdf (hibrit docling) ile PDF işleme hattı.

Faz 3 kapsamında implemente edilecek.
"""

import opendataloader_pdf
from loguru import logger


def convert_pdf(pdf_path: str, *, hybrid: str = "docling-fast") -> dict:
    """Verilen PDF dosyasını opendataloader_pdf ile işler.

    Args:
        pdf_path: İşlenecek PDF dosyasının yolu.
        hybrid: Hibritleme modu (varsayılan "docling-fast").

    Returns:
        convert() fonksiyonunun dönüş değeri (None; çıktı dosya olarak yazılır).

    Raises:
        Herhangi bir hata loglanır ve yeniden fırlatılır.
    """
    try:
        result = opendataloader_pdf.convert(input_path=pdf_path, hybrid=hybrid)
        return result
    except Exception as exc:
        logger.error("PDF işlenemedi ({}): {}", pdf_path, exc)
        raise
