"""Shared utility functions (retry, logging, checkpoint, adaptive delay)."""

import asyncio
import functools
import inspect
import json
import os
import time
from collections.abc import Callable
from pathlib import Path

from loguru import logger

# DergiPark serves PDFs and article pages from this host.
PDF_BASE_URL = "https://dergipark.org.tr"


def pdf_filename(oai_id: str) -> str:
    """Map an OAI identifier to the harvested PDF filename.

    Stored under data/raw_pdfs as "<oai:...>_<id>.<ext>" — the same
    normalized name the crawler writes (crawler.pdf_destination).
    """
    return (oai_id or "").replace("/", "_") + ".pdf"


def retry(
    max_attempts: int = 5,
    backoff: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    rate_limit_backoff: float = 5.0,
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504),
):
    """Decorator for retrying sync and async functions with exponential backoff.

    HTTP 429 errors use longer backoff. Client errors (404, 400, 401, 403)
    are raised immediately without retry.

    Args:
        max_attempts: Maximum number of attempts.
        backoff: Base backoff in seconds (multiplied by 2^(attempt-1)).
        exceptions: Tuple of exception types to catch and retry.
        rate_limit_backoff: Backoff multiplier for 429 rate-limit errors.
        retry_on_status: HTTP status codes that trigger retry.

    Returns:
        Decorator function.
    """

    def _get_status_code(exc: BaseException) -> int | None:
        code = getattr(exc, "status_code", None)
        if isinstance(code, int):
            return code
        msg = str(exc)
        for pattern in ("429", "404", "500", "502", "503", "504"):
            if pattern in msg:
                return int(pattern)
        return None

    def _is_rate_limit(exc: BaseException) -> bool:
        code = _get_status_code(exc)
        if code == 429:
            return True
        msg = str(exc)
        return "429" in msg or "Too Many Requests" in msg

    def _should_retry(exc: BaseException) -> bool:
        code = _get_status_code(exc)
        if code is None:
            return True  # status code yoksa mevcut davranis (retry)
        return code in retry_on_status

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
                    if not _should_retry(exc):
                        raise
                    last_exc = exc
                    wait = _wait_time(exc, attempt)
                    logger.warning(
                        "Attempt {}/{} failed: {} (wait: {:.1f}s)",
                        attempt,
                        max_attempts,
                        exc,
                        wait,
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
                    if not _should_retry(exc):
                        raise
                    last_exc = exc
                    wait = _wait_time(exc, attempt)
                    logger.warning(
                        "Attempt {}/{} failed: {} (wait: {:.1f}s)",
                        attempt,
                        max_attempts,
                        exc,
                        wait,
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
    """Configure loguru: console INFO, rotating error.log for ERROR+.

    Console level can be overridden with LOGURU_LEVEL env var.
    Guard prevents duplicate handlers on multiple calls.
    """
    global _logger_ready
    if _logger_ready:
        return

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    log_level = os.environ.get("LOGURU_LEVEL", "INFO").upper()

    # Remove default handler, add console handler with INFO level
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level=log_level,
        format="{time:HH:mm:ss} | {level: <8} | {message}",
    )

    # File handler: ERROR+ only, rotate 10MB
    logger.add(
        data_dir / "error.log",
        level="ERROR",
        rotation="10 MB",
        backtrace=True,
        diagnose=False,
    )
    _logger_ready = True


def save_checkpoint(path: str | Path = "checkpoint.json", *, state: dict) -> None:
    """Save checkpoint state atomically (tmp + rename).

    Args:
        path: Checkpoint file path.
        state: Dict to serialize as JSON.
    """
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(path)


def load_checkpoint(path: str | Path = "checkpoint.json") -> dict:
    """Load checkpoint state from file. Returns {} if missing or corrupt.

    Args:
        path: Checkpoint file path.

    Returns:
        Deserialized checkpoint dict, or empty dict on error.
    """
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Checkpoint file corrupt: {}", path)
        return {}


class AdaptiveDelay:
    """Per-journal adaptive delay tracker.

    On 429: increase delay (multiply by backoff_factor).
    On success: decrease delay (multiply by decay_factor), clamped to min_delay.

    Args:
        default: Default delay in seconds.
        min_delay: Minimum allowed delay.
        max_delay: Maximum allowed delay.
        backoff_factor: Multiplier applied on 429 errors.
        decay_factor: Multiplier applied on successful requests.
    """

    def __init__(
        self,
        default: float = 2.0,
        min_delay: float = 0.5,
        max_delay: float = 30.0,
        backoff_factor: float = 2.0,
        decay_factor: float = 0.9,
    ):
        self._delays: dict[str, float] = {}
        self._default = default
        self._min = min_delay
        self._max = max_delay
        self._backoff = backoff_factor
        self._decay = decay_factor

    def get(self, journal: str) -> float:
        return self._delays.get(journal, self._default)

    def on_429(self, journal: str) -> float:
        current = self.get(journal)
        new_delay = min(current * self._backoff, self._max)
        self._delays[journal] = new_delay
        return new_delay

    def on_success(self, journal: str) -> float:
        current = self.get(journal)
        new_delay = max(current * self._decay, self._min)
        self._delays[journal] = new_delay
        return new_delay

    def to_dict(self) -> dict[str, float]:
        return dict(self._delays)

    @classmethod
    def from_dict(cls, data: dict[str, float], **kwargs) -> AdaptiveDelay:
        obj = cls(**kwargs)
        obj._delays = dict(data)
        return obj
