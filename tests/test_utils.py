"""Unit tests for utils.py (retry + setup_logger)."""

import asyncio
from pathlib import Path

import src.utils as utils
from src.utils import retry, setup_logger


def test_retry_sync_success_after_failures():
    """Sync: first 2 attempts fail, 3rd succeeds."""
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
    """Sync: all attempts fail -> last exception raised."""
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
        raise AssertionError("Exception not raised")


def test_retry_async_success_after_failures():
    """Async: first attempt fails, 2nd succeeds."""
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
    """Async: all attempts fail -> last exception raised."""
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
        raise AssertionError("Exception not raised")


def test_setup_logger_creates_error_log(tmp_path, monkeypatch):
    """setup_logger should create data/error.log."""
    monkeypatch.chdir(tmp_path)
    utils._logger_ready = False

    setup_logger()

    assert Path("data/error.log").exists()


def test_setup_logger_guard(tmp_path, monkeypatch):
    """Second call should not add duplicate handlers (guard)."""
    monkeypatch.chdir(tmp_path)
    utils._logger_ready = False

    setup_logger()
    setup_logger()

    assert Path("data/error.log").exists()


def test_retry_429_uses_longer_backoff():
    """429 errors use rate_limit_backoff instead of normal backoff."""
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
    """404 errors are raised immediately without retry."""
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
        raise AssertionError("Exception not raised")
    assert call_count == 1  # Only 1 attempt, raised immediately


def test_retry_500_retries():
    """500 errors are retried."""
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
        raise AssertionError("Exception not raised")
    assert call_count == 3  # 3 attempts made


def test_checkpoint_roundtrip(tmp_path):
    """save_checkpoint -> load_checkpoint returns same dict."""
    path = tmp_path / "cp.json"
    state = {"last_journal_index": 12, "processed_articles": 340}
    utils.save_checkpoint(path, state=state)
    loaded = utils.load_checkpoint(path)
    assert loaded == state


def test_checkpoint_load_missing_file():
    """Missing file returns {}."""
    result = utils.load_checkpoint("/nonexistent/path/cp.json")
    assert result == {}


def test_checkpoint_load_corrupt_file(tmp_path):
    """Corrupt JSON returns {}."""
    path = tmp_path / "corrupt.json"
    path.write_text("{invalid json!!!", encoding="utf-8")
    result = utils.load_checkpoint(path)
    assert result == {}


def test_adaptive_delay_default():
    """Default delay starts at default value."""
    d = utils.AdaptiveDelay(default=2.0)
    assert d.get("j1") == 2.0


def test_adaptive_delay_on_429_increases():
    """Delay increases on 429."""
    d = utils.AdaptiveDelay(default=2.0, backoff_factor=2.0)
    new_delay = d.on_429("j1")
    assert new_delay == 4.0
    assert d.get("j1") == 4.0


def test_adaptive_delay_on_success_decays():
    """Delay decreases on success."""
    d = utils.AdaptiveDelay(default=2.0, decay_factor=0.5)
    d.on_429("j1")  # 2.0 -> 4.0
    new_delay = d.on_success("j1")  # 4.0 -> 2.0
    assert new_delay == 2.0


def test_adaptive_delay_respects_min_max():
    """Respects min/max bounds."""
    d = utils.AdaptiveDelay(default=2.0, min_delay=0.5, max_delay=10.0, backoff_factor=3.0)
    d.on_429("j1")  # 2.0 -> 6.0
    d.on_429("j1")  # 6.0 -> 10.0 (max)
    d.on_429("j1")  # 10.0 -> 10.0 (capped)
    assert d.get("j1") == 10.0

    d2 = utils.AdaptiveDelay(default=2.0, min_delay=0.5, decay_factor=0.1)
    d2.on_success("j1")  # 2.0 -> 0.5 (min)
    d2.on_success("j1")  # 0.5 -> 0.5 (capped)
    assert d2.get("j1") == 0.5


def test_adaptive_delay_to_dict_from_dict():
    """to_dict/from_dict serialization roundtrip."""
    d = utils.AdaptiveDelay(default=2.0)
    d.on_429("j1")
    d.on_429("j2")
    data = d.to_dict()
    assert data == {"j1": 4.0, "j2": 4.0}

    d2 = utils.AdaptiveDelay.from_dict(data, default=2.0)
    assert d2.get("j1") == 4.0
    assert d2.get("j2") == 4.0
