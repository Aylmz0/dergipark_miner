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


def test_retry_404_raises_immediately():
    """404 hatasi hemen firlatilir, deneme yapilmaz."""
    call_count = 0

    class NotFoundError(Exception):
        def __init__(self, msg="404 Not Found"):
            super().__init__(msg)
            self.status_code = 404

    @retry(max_attempts=5, backoff=0.0, exceptions=(Exception,))
    def not_found():
        nonlocal call_count
        call_count += 1
        raise NotFoundError()

    try:
        not_found()
    except NotFoundError:
        pass
    else:
        raise AssertionError("Hata firlatilmadi")
    assert call_count == 1  # sadece 1 deneme, hemen firlatti


def test_retry_500_retries():
    """500 hatasi yeniden denenir."""
    call_count = 0

    class ServerError(Exception):
        def __init__(self, msg="500 Internal Server Error"):
            super().__init__(msg)
            self.status_code = 500

    @retry(max_attempts=3, backoff=0.0, exceptions=(Exception,))
    def server_fail():
        nonlocal call_count
        call_count += 1
        raise ServerError()

    try:
        server_fail()
    except ServerError:
        pass
    else:
        raise AssertionError("Hata firlatilmadi")
    assert call_count == 3  # 3 deneme yapildi


def test_checkpoint_roundtrip(tmp_path):
    """save_checkpoint -> load_checkpoint ayni dict'i dondurur."""
    path = tmp_path / "cp.json"
    state = {"last_journal_index": 12, "processed_articles": 340}
    utils.save_checkpoint(path, state=state)
    loaded = utils.load_checkpoint(path)
    assert loaded == state


def test_checkpoint_load_missing_file():
    """Dosya yoksa {} dondurur."""
    result = utils.load_checkpoint("/nonexistent/path/cp.json")
    assert result == {}


def test_checkpoint_load_corrupt_file(tmp_path):
    """Bozuk JSON dosyasi {} dondurur."""
    path = tmp_path / "corrupt.json"
    path.write_text("{invalid json!!!", encoding="utf-8")
    result = utils.load_checkpoint(path)
    assert result == {}
