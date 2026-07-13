"""Ortak yardımcı fonksiyonlar (Telegram bot, retry, vb.).

Faz 2-5 boyunca kullanılacak yardımcılar.
"""

import asyncio
import functools
import time
from pathlib import Path
from typing import Callable

from loguru import logger


def retry(
    max_attempts: int = 3,
    backoff: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
):
    """Hem senkron hem async fonksiyonlara takılabilen yeniden deneme dekoratörü.

    Başarısız denemeler arasında üstel bekleme (backoff * 2**deneme) yapar.
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    logger.warning(
                        "Deneme {}/{} basarisiz: {}", attempt, max_attempts, exc
                    )
                    if attempt < max_attempts:
                        time.sleep(backoff * (2 ** (attempt - 1)))
            assert last_exc is not None
            raise last_exc

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    logger.warning(
                        "Deneme {}/{} basarisiz: {}", attempt, max_attempts, exc
                    )
                    if attempt < max_attempts:
                        await asyncio.sleep(backoff * (2 ** (attempt - 1)))
            assert last_exc is not None
            raise last_exc

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


_logger_ready = False


def setup_logger() -> None:
    """loguru'yu yapılandırır: hatalar data/error.log'a (rotation), konsola bilgi seviyesinde.

    Birden fazla çağrıda tekrar handler eklenmez (guard).
    """
    global _logger_ready
    if _logger_ready:
        return

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    logger.add(
        data_dir / "error.log",
        level="ERROR",
        rotation="10 MB",
        backtrace=True,
        diagnose=True,
    )
    _logger_ready = True
