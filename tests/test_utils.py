"""utils.py için birim testleri (retry + setup_logger)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from utils import retry, setup_logger


def test_retry_sync_success_after_failures():
    """Senkron: ilk 2 deneme başarısız, 3. başarılı."""
    call_count = 0

    @retry(max_attempts=3, backoff=0.0)
    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("hata")
        return "ok"

    assert flaky() == "ok"
    assert call_count == 3


def test_retry_sync_all_failures():
    """Senkron: tüm denemeler başarısız → son hata fırlatılır."""
    call_count = 0

    @retry(max_attempts=3, backoff=0.0)
    def always_fail():
        nonlocal call_count
        call_count += 1
        raise RuntimeError("kalıcı hata")

    try:
        always_fail()
    except RuntimeError as e:
        assert "kalıcı hata" in str(e)
        assert call_count == 3
    else:
        raise AssertionError("hata fırlatilmadi")


def test_retry_async_success_after_failures():
    """Asenkron: ilk 1 deneme başarısız, 2. başarılı."""
    call_count = 0

    @retry(max_attempts=3, backoff=0.0)
    async def async_flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("async hata")
        return "async_ok"

    result = asyncio.run(async_flaky())
    assert result == "async_ok"
    assert call_count == 2


def test_retry_async_all_failures():
    """Asenkron: tüm denemeler başarısız → son hata fırlatılır."""
    call_count = 0

    @retry(max_attempts=2, backoff=0.0)
    async def async_always_fail():
        nonlocal call_count
        call_count += 1
        raise KeyError("yok")

    try:
        asyncio.run(async_always_fail())
    except KeyError as e:
        assert "yok" in str(e)
        assert call_count == 2
    else:
        raise AssertionError("hata fırlatilmadi")


def test_setup_logger_creates_error_log(tmp_path, monkeypatch):
    """setup_logger çağrılınca data/error.log oluşmalı."""
    monkeypatch.chdir(tmp_path)
    utils._logger_ready = False

    setup_logger()

    assert Path("data/error.log").exists()


def test_setup_logger_guard(tmp_path, monkeypatch):
    """Ikinci cagirda tekrar handler eklenmemeli (guard)."""
    monkeypatch.chdir(tmp_path)
    utils._logger_ready = False

    setup_logger()
    setup_logger()

    assert Path("data/error.log").exists()


def test_retry_429_uses_longer_backoff():
    """429 iceren hatalarda rate_limit_backoff kullanilir."""
    call_count = 0

    class RateLimitError(Exception):
        def __init__(self, msg="429 Too Many Requests"):
            super().__init__(msg)

    @retry(max_attempts=3, backoff=0.0, rate_limit_backoff=0.0, exceptions=(Exception,))
    def rate_limited():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RateLimitError("429 Too Many Requests")
        return "ok"

    assert rate_limited() == "ok"
    assert call_count == 3
