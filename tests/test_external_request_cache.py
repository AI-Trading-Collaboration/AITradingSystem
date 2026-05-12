from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import ai_trading_system.data.market_data as market_data_module
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    FredRateProvider,
    MarketstackPriceProvider,
    PriceRequest,
    ProviderDownloadError,
    RateRequest,
    YFinancePriceProvider,
)
from ai_trading_system.external_request_cache import cached_requests_get
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    SecEdgarCompanyFactsProvider,
)


def test_cached_requests_get_reuses_identical_request_and_redacts_secret(
    tmp_path: Path,
) -> None:
    fake_requests = _FakeRequests(payload={"ok": True})

    first = cached_requests_get(
        provider="Example Vendor",
        api_family="example",
        url="https://vendor.example.test/data",
        params={"symbol": "NVDA", "apikey": "secret-key"},
        requests_module=fake_requests,
        cache_dir=tmp_path,
    )
    second = cached_requests_get(
        provider="Example Vendor",
        api_family="example",
        url="https://vendor.example.test/data",
        params={"symbol": "NVDA", "apikey": "another-secret-key"},
        requests_module=fake_requests,
        cache_dir=tmp_path,
    )

    assert first.from_cache is False
    assert second.from_cache is True
    assert first.json() == {"ok": True}
    assert second.json() == {"ok": True}
    assert len(fake_requests.calls) == 1

    metadata_text = next(tmp_path.rglob("metadata.json")).read_text(encoding="utf-8")
    metadata = json.loads(metadata_text)
    assert metadata["request_identity"]["params"]["apikey"] == "***"
    assert "secret-key" not in metadata_text
    assert "another-secret-key" not in metadata_text


def test_fmp_price_provider_uses_request_cache_for_repeated_request(tmp_path: Path) -> None:
    fake_requests = _FakeRequests(
        payload=[
            {
                "date": "2026-05-01",
                "adjOpen": 100.0,
                "adjHigh": 102.0,
                "adjLow": 99.0,
                "adjClose": 101.0,
                "volume": 1234,
            }
        ]
    )
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
    )
    request = PriceRequest(tickers=["GOOG"], start=date(2026, 5, 1), end=date(2026, 5, 1))

    first = provider.download_prices(request)
    second = provider.download_prices(request)

    assert first.equals(second)
    assert len(fake_requests.calls) == 2  # non-split-adjusted + dividend-adjusted once each


def test_sec_companyfacts_provider_uses_request_cache_for_repeated_request(
    tmp_path: Path,
) -> None:
    raw = b'{"cik":1045810,"facts":{}}'
    fake_requests = _FakeRequests(payload={}, content=raw)
    provider = SecEdgarCompanyFactsProvider(
        user_agent="owner@example.test",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
    )
    request = SecCompanyFactsRequest(ticker="NVDA", cik="0001045810")

    first = provider.download_companyfacts_raw(request)
    second = provider.download_companyfacts_raw(request)

    assert first == raw
    assert second == raw
    assert len(fake_requests.calls) == 1
    metadata_text = next(tmp_path.rglob("metadata.json")).read_text(encoding="utf-8")
    assert "owner@example.test" not in metadata_text


def test_yfinance_provider_uses_dataframe_cache(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    raw = pd.DataFrame(
        {
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.5],
            "Adj Close": [100.5],
            "Volume": [1000],
        },
        index=pd.to_datetime(["2026-05-01"]),
    )
    fake_yfinance = _FakeYFinance(raw)

    def fake_import_module(name: str) -> Any:
        if name != "yfinance":
            raise AssertionError(name)
        return fake_yfinance

    monkeypatch.setattr(market_data_module, "import_module", fake_import_module)
    provider = YFinancePriceProvider(request_cache_dir=tmp_path)
    request = PriceRequest(tickers=["NVDA"], start=date(2026, 5, 1), end=date(2026, 5, 1))

    first = provider.download_prices(request)
    second = provider.download_prices(request)

    assert first.equals(second)
    assert fake_yfinance.call_count == 1


def test_marketstack_provider_reports_sanitized_pre_response_failure(tmp_path: Path) -> None:
    fake_requests = _FailingRequests("timeout access_key=marketstack-secret")
    provider = MarketstackPriceProvider(
        api_key="marketstack-secret",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
    )
    request = PriceRequest(tickers=["NVDA"], start=date(2026, 5, 1), end=date(2026, 5, 1))

    try:
        provider.download_prices(request)
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("Marketstack failure did not raise ProviderDownloadError")

    assert diagnostic.provider == "Marketstack"
    assert diagnostic.stage == "http_request"
    assert diagnostic.cache_status == "MISS_NO_RESPONSE"
    assert diagnostic.cache_key
    assert diagnostic.request_parameters["access_key"] == "***"
    assert diagnostic.request_parameters["symbols"] == "NVDA"
    assert diagnostic.exception_type == "TimeoutError"
    assert "marketstack-secret" not in (diagnostic.exception_message or "")
    assert not list(tmp_path.rglob("metadata.json"))


def test_fred_provider_reports_series_pre_response_failure(tmp_path: Path) -> None:
    fake_requests = _FailingRequests("read timeout while fetching DGS10")
    provider = FredRateProvider(
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=2,
        retry_backoff_seconds=0,
    )
    request = RateRequest(
        series_ids=["DGS10"],
        start=date(2018, 1, 1),
        end=date(2026, 5, 11),
    )

    try:
        provider.download_rates(request)
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("FRED failure did not raise ProviderDownloadError")

    assert diagnostic.provider == "Federal Reserve Economic Data"
    assert diagnostic.api_family == "fredgraph_csv"
    assert diagnostic.stage == "http_request"
    assert diagnostic.cache_status == "MISS_NO_RESPONSE"
    assert diagnostic.cache_key
    assert diagnostic.request_parameters == {
        "coed": "2026-05-11",
        "cosd": "2018-01-01",
        "id": "DGS10",
    }
    assert diagnostic.exception_type == "TimeoutError"
    assert "DGS10" in (diagnostic.exception_message or "")
    assert diagnostic.attempt_count == 2
    assert diagnostic.max_attempts == 2
    assert diagnostic.timeout_seconds == 60
    assert len(fake_requests.calls) == 2
    assert not list(tmp_path.rglob("metadata.json"))


class _FakeResponse:
    def __init__(
        self,
        payload: object,
        *,
        content: bytes | None = None,
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self.headers = {"content-type": "application/json"}
        self.content = (
            content
            if content is not None
            else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
        self.text = self.content.decode("utf-8")

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError(self.status_code)


class _FakeRequests:
    def __init__(self, payload: object, *, content: bytes | None = None) -> None:
        self.payload = payload
        self.content = content
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return _FakeResponse(self.payload, content=self.content)


class _FailingRequests:
    def __init__(self, message: str) -> None:
        self.message = message
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        raise TimeoutError(self.message)


class _FakeYFinance:
    def __init__(self, raw: pd.DataFrame) -> None:
        self.raw = raw
        self.call_count = 0

    def download(self, **_kwargs: Any) -> pd.DataFrame:
        self.call_count += 1
        return self.raw
