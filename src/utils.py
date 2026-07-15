"""Ortak yardımcı fonksiyonlar (Telegram bot, retry, vb.).

Faz 2-5 boyunca kullanılacak yardımcılar.
"""

import asyncio
import functools
import inspect
import time
from collections.abc import Callable
from pathlib import Path

from loguru import logger


def retry(
    max_attempts: int = 5,
    backoff: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    rate_limit_backoff: float = 5.0,
):
    """Hem senkron hem async fonksiyonlara takılabilen yeniden deneme dekoratörü.

    Başarısız denemeler arasında üstel bekleme (backoff * 2**deneme) yapar.
    HTTP 429 (Too Many Requests) hatalarinda daha uzun bekleme kullanir.
    """

    def _is_rate_limit(exc: BaseException) -> bool:
        msg = str(exc)
        if "429" in msg or "Too Many Requests" in msg:
            return True
        code = getattr(exc, "status_code", None)
        return code == 429

    def _wait_time(exc: BaseException, attempt: int) -> float:
        base = rate_limit_backoff if _is_rate_limit(exc) else backoff
        return base * (2 ** (attempt - 1))

    def decorator(func: Callable):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    wait = _wait_time(exc, attempt)
                    logger.warning(
                        "Deneme {}/{} basarisiz: {} (bekleme: {:.1f}s)",
                        attempt, max_attempts, exc, wait,
                    )
                    if attempt < max_attempts:
                        time.sleep(wait)
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
                    wait = _wait_time(exc, attempt)
                    logger.warning(
                        "Deneme {}/{} basarisiz: {} (bekleme: {:.1f}s)",
                        attempt, max_attempts, exc, wait,
                    )
                    if attempt < max_attempts:
                        await asyncio.sleep(wait)
            assert last_exc is not None
            raise last_exc

        if inspect.iscoroutinefunction(func):
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
