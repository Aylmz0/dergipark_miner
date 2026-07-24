"""PDF processing pipeline using opendataloader-pdf (Phase 3 stub)."""

import opendataloader_pdf
from loguru import logger


def convert_pdf(pdf_path: str, *, hybrid: str = "docling-fast") -> None:
    """Process a PDF file using opendataloader_pdf.

    Args:
        pdf_path: Path to the PDF file to process.
        hybrid: Hybridization mode (default "docling-fast").

    Raises:
        Re-raises any exception after logging.
    """
    try:
        result = opendataloader_pdf.convert(input_path=pdf_path, hybrid=hybrid)
        return result
    except Exception as exc:
        logger.error("PDF işlenemedi ({}): {}", pdf_path, exc)
        raise
