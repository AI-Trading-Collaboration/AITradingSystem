"""DATA-026: SEC companyfacts download retries a request that failed before any response.

The 2026-10-20 release run lost its whole day to one TLS handshake failure in the SEC component,
because the provider made a single attempt per company. The provider now retries a request that
fails before an HTTP response arrives (TLS, connect, timeout, reset), with a short bounded
backoff, like the FMP and official-policy adapters. HTTP error statuses and every other exception
are not retried, and a request that exhausts its attempts raises its last exception unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests

import ai_trading_system.fundamentals.sec_companyfacts as sec_module
from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig
from ai_trading_system.external_request_cache import CachedHttpStatusError
from ai_trading_system.fundamentals.sec_companyfacts import (
    SEC_REQUEST_MAX_ATTEMPTS,
    SEC_REQUEST_RETRY_BACKOFF_SECONDS,
    SecCompanyFactsRequest,
    SecEdgarCompanyFactsProvider,
    download_sec_companyfacts,
)

PAYLOAD = b'{"cik":1045810,"facts":{}}'
REQUEST = SecCompanyFactsRequest(ticker="NVDA", cik="0001045810")


class _Response:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self.headers = {"content-type": "application/json"}
        self.content = content
        self.text = content.decode("utf-8")

    def json(self) -> object:
        return {}


class _ScriptedRequests:
    """Each get() takes the next scripted outcome: an exception to raise or a status code."""

    exceptions = requests.exceptions

    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[str] = []

    def get(self, url: str, **kwargs: object) -> _Response:
        self.calls.append(url)
        outcome = self.outcomes.pop(0) if self.outcomes else 200
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, int)
        return _Response(outcome, PAYLOAD)


def _provider(
    tmp_path: Path,
    scripted: _ScriptedRequests,
    sleeps: list[float],
    **kwargs: object,
) -> SecEdgarCompanyFactsProvider:
    return SecEdgarCompanyFactsProvider(
        user_agent="owner@example.test",
        requests_module=scripted,
        request_cache_dir=tmp_path,
        sleeper=sleeps.append,
        **kwargs,  # type: ignore[arg-type]
    )


def test_defaults_are_three_attempts_with_five_and_fifteen_seconds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert SEC_REQUEST_MAX_ATTEMPTS == 3
    assert tuple(SEC_REQUEST_RETRY_BACKOFF_SECONDS) == (5.0, 15.0)
    sleeps: list[float] = []
    monkeypatch.setattr(sec_module.time, "sleep", sleeps.append)
    scripted = _ScriptedRequests(
        [requests.exceptions.SSLError("one"), requests.exceptions.SSLError("two")]
    )
    provider = SecEdgarCompanyFactsProvider(
        user_agent="owner@example.test",
        requests_module=scripted,
        request_cache_dir=tmp_path,
    )

    assert provider.download_companyfacts_raw(REQUEST) == PAYLOAD
    assert len(scripted.calls) == 3
    assert sleeps == [5.0, 15.0]


@pytest.mark.parametrize(
    "failure",
    [
        requests.exceptions.SSLError("EOF occurred in violation of protocol"),
        requests.exceptions.ConnectTimeout("connect timed out"),
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ConnectionError("connection reset"),
        requests.exceptions.ChunkedEncodingError("incomplete read"),
    ],
)
def test_failure_before_a_response_is_retried_and_then_succeeds(
    tmp_path: Path,
    failure: Exception,
) -> None:
    scripted = _ScriptedRequests([failure])
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps)

    assert provider.download_companyfacts_raw(REQUEST) == PAYLOAD
    assert len(scripted.calls) == 2
    assert sleeps == [5.0]
    assert len(list(tmp_path.rglob("metadata.json"))) == 1


def test_exhausted_attempts_raise_the_last_exception_and_cache_nothing(tmp_path: Path) -> None:
    scripted = _ScriptedRequests(
        [
            requests.exceptions.SSLError("first"),
            requests.exceptions.SSLError("second"),
            requests.exceptions.SSLError("third"),
            requests.exceptions.SSLError("fourth"),
        ]
    )
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps)

    with pytest.raises(requests.exceptions.SSLError, match="third"):
        provider.download_companyfacts_raw(REQUEST)

    assert len(scripted.calls) == 3
    assert sleeps == [5.0, 15.0]
    assert not list(tmp_path.rglob("metadata.json"))


@pytest.mark.parametrize("status_code", [429, 500, 503])
def test_http_error_statuses_are_not_retried(tmp_path: Path, status_code: int) -> None:
    scripted = _ScriptedRequests([status_code])
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps)

    with pytest.raises(CachedHttpStatusError):
        provider.download_companyfacts_raw(REQUEST)

    assert len(scripted.calls) == 1
    assert sleeps == []


def test_other_exceptions_are_not_retried(tmp_path: Path) -> None:
    scripted = _ScriptedRequests([ValueError("not a transport failure")])
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps)

    with pytest.raises(ValueError, match="not a transport failure"):
        provider.download_companyfacts_raw(REQUEST)

    assert len(scripted.calls) == 1
    assert sleeps == []


def test_cached_response_needs_no_request_and_no_sleep(tmp_path: Path) -> None:
    scripted = _ScriptedRequests([requests.exceptions.ConnectionError("blip")])
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps)

    first = provider.download_companyfacts_raw(REQUEST)
    second = provider.download_companyfacts_raw(REQUEST)

    assert first == second == PAYLOAD
    assert len(scripted.calls) == 2  # failed attempt + one successful request, then the cache
    assert sleeps == [5.0]


def test_custom_schedule_is_used(tmp_path: Path) -> None:
    scripted = _ScriptedRequests([requests.exceptions.SSLError("one")])
    sleeps: list[float] = []
    provider = _provider(tmp_path, scripted, sleeps, max_attempts=2, retry_backoff_seconds=(0.0,))

    assert provider.download_companyfacts_raw(REQUEST) == PAYLOAD
    assert sleeps == [0.0]

    single = _ScriptedRequests([requests.exceptions.SSLError("only")])
    single_sleeps: list[float] = []
    no_retry = _provider(
        tmp_path / "single", single, single_sleeps, max_attempts=1, retry_backoff_seconds=()
    )
    with pytest.raises(requests.exceptions.SSLError, match="only"):
        no_retry.download_companyfacts_raw(REQUEST)
    assert len(single.calls) == 1
    assert single_sleeps == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": 3, "retry_backoff_seconds": (1.0,)},
        {"max_attempts": 3, "retry_backoff_seconds": (-1.0, 1.0)},
    ],
)
def test_constructor_rejects_unusable_schedules(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        SecEdgarCompanyFactsProvider(user_agent="owner@example.test", **kwargs)  # type: ignore[arg-type]


def test_download_survives_one_blip_for_one_company(tmp_path: Path) -> None:
    config = SecCompaniesConfig(
        companies=[
            SecCompanyConfig(ticker="NVDA", cik="0001045810", company_name="NVIDIA Corporation"),
            SecCompanyConfig(ticker="MSFT", cik="0000789019", company_name="Microsoft Corporation"),
        ]
    )
    scripted = _ScriptedRequests([200, requests.exceptions.SSLError("blip"), 200])
    sleeps: list[float] = []
    provider = _provider(tmp_path / "cache", scripted, sleeps)

    summary = download_sec_companyfacts(
        config=config, output_dir=tmp_path / "out", provider=provider
    )

    assert summary.company_count == 2
    assert len(scripted.calls) == 3
    assert sleeps == [5.0]
    manifest = pd.read_csv(summary.manifest_path)
    assert sorted(manifest["ticker"]) == ["MSFT", "NVDA"]
