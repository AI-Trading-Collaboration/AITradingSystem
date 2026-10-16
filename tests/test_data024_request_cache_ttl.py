"""DATA-024: a successful response of a date-less, mutable source expires.

The 2026-10-15 trial of the daily release bundle showed SEC companyfacts frozen in the runtime copy:
HTTP 200-399 responses were `positive_persistent`, so a request whose key has no date (a ticker's
companyfacts, an FMP endpoint with only a symbol) was served from the first stored response forever.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import requests
import yaml

import ai_trading_system.external_request_cache as cache_module
from ai_trading_system import official_policy_sources
from ai_trading_system.external_request_cache import (
    CachedHttpResponse,
    cached_requests_get,
    external_request_cache_trace,
    lookup_external_request_cache,
    write_external_request_cache_response,
)
from ai_trading_system.external_request_cache_policy import (
    DEFAULT_EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_PATH,
    evaluate_external_request_cache_lifecycle,
    load_external_request_cache_lifecycle_policy,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    SecEdgarCompanyFactsProvider,
)
from ai_trading_system.paths import CODE_ROOT
from ai_trading_system.valuation_sources import FMP_SOURCE_NAME, FmpHttpValuationProvider

BOUNDED_CLASS = "positive_bounded_mutable_source"
TTL_SECONDS = 43200
T0 = datetime(2026, 10, 15, 3, 0, tzinfo=UTC)
FMP_ENDPOINTS = (
    "quote-short",
    "key-metrics-ttm",
    "ratios-ttm",
    "analyst-estimates",
    "key-metrics",
    "ratios",
    "price-target-summary",
    "price-target-consensus",
    "grades",
    "grades-consensus",
    "ratings-snapshot",
    "earnings-calendar",
)
BOUNDED = [
    ("SEC EDGAR", "companyfacts"),
    *[(FMP_SOURCE_NAME, endpoint) for endpoint in FMP_ENDPOINTS],
    ("Official policy source", "source_fetch"),
]
PERSISTENT = [
    ("Financial Modeling Prep", "eod_daily_prices"),
    ("Marketstack", "eod_daily_prices"),
    ("Federal Reserve Economic Data", "fredgraph_csv"),
    ("SEC EDGAR", "filing_archive"),
    ("SEC EDGAR", "sec_pit_submissions"),
    ("SEC EDGAR", "sec_pit_companyfacts"),
    ("TSMC Investor Relations", "quarterly_text"),
    ("Example Vendor", "example"),
]
URL = "https://vendor.example.test/data"


def _decision(provider: str | None, api_family: str | None, status_code: int = 200):
    return evaluate_external_request_cache_lifecycle(
        status_code=status_code,
        response_headers={},
        observed_at=T0,
        policy=load_external_request_cache_lifecycle_policy(),
        provider=provider,
        api_family=api_family,
    )


class _Response:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self.headers = {"content-type": "application/json"}
        self.content = json.dumps(payload).encode("utf-8")
        self.text = self.content.decode("utf-8")

    def json(self) -> object:
        return self.payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError(self.status_code)


class _Requests:
    exceptions = requests.exceptions

    def __init__(self, payload: object = None, error: Exception | None = None) -> None:
        self.payload = [{"symbol": "NVDA"}] if payload is None else payload
        self.error = error
        self.calls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> _Response:
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        return _Response(self.payload)


def _travel(monkeypatch: pytest.MonkeyPatch, hours: float) -> None:
    """Make the cache module's clock run `hours` ahead."""
    real = cache_module.datetime

    class Later(real):  # type: ignore[valid-type, misc]
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            return real.now(tz) + timedelta(hours=hours)

    monkeypatch.setattr(cache_module, "datetime", Later)


def _seed(
    cache_dir: Path,
    *,
    age: timedelta,
    provider: str = "SEC EDGAR",
    family: str = "companyfacts",
    **kwargs,
):
    return write_external_request_cache_response(
        provider=provider,
        api_family=family,
        method="GET",
        url=URL,
        status_code=200,
        response_headers={"content-type": "application/json"},
        content=b'{"stored": true}',
        cache_dir=cache_dir,
        requested_at=datetime.now(tz=UTC) - age,
        **kwargs,
    )


def _get(
    cache_dir: Path, fake: _Requests, provider: str = "SEC EDGAR", family: str = "companyfacts"
):
    return cached_requests_get(
        provider=provider,
        api_family=family,
        url=URL,
        requests_module=fake,
        cache_dir=cache_dir,
    )


# --- the policy ---


@pytest.mark.parametrize(("provider", "family"), BOUNDED)
def test_date_less_mutable_sources_expire_after_twelve_hours(provider: str, family: str) -> None:
    decision = _decision(provider, family)

    assert decision.lifecycle_class == BOUNDED_CLASS
    assert decision.ttl_seconds == TTL_SECONDS
    assert decision.expires_at == T0 + timedelta(seconds=TTL_SECONDS)
    assert decision.retry_after_seconds is None
    assert decision.persistent is False


@pytest.mark.parametrize(("provider", "family"), PERSISTENT)
def test_every_other_source_stays_persistent(provider: str, family: str) -> None:
    decision = _decision(provider, family)

    assert decision.lifecycle_class == "positive_persistent"
    assert decision.ttl_seconds is None
    assert decision.persistent is True


@pytest.mark.parametrize(
    ("provider", "family"),
    [(None, None), ("SEC EDGAR", None), (None, "companyfacts"), ("Other Vendor", "companyfacts")],
)
def test_a_decision_needs_both_provider_and_family_to_be_bounded(
    provider: str | None, family: str | None
) -> None:
    assert _decision(provider, family).persistent is True


def test_failures_of_a_bounded_source_keep_the_negative_rules() -> None:
    assert _decision("SEC EDGAR", "companyfacts", 404).ttl_seconds == 900
    assert _decision("SEC EDGAR", "companyfacts", 500).ttl_seconds == 300
    assert _decision("SEC EDGAR", "companyfacts", 404).lifecycle_class != BOUNDED_CLASS
    assert _decision("SEC EDGAR", "companyfacts", 304).lifecycle_class == BOUNDED_CLASS
    assert _decision("SEC EDGAR", "companyfacts", 199).ttl_seconds == 0


def _policy_file(tmp_path: Path, change) -> Path:  # type: ignore[no-untyped-def]
    raw = yaml.safe_load(
        DEFAULT_EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_PATH.read_text(encoding="utf-8")
    )
    change(raw)
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
    return path


def test_a_policy_file_without_positive_rules_still_loads(tmp_path: Path) -> None:
    path = _policy_file(tmp_path, lambda raw: raw.pop("positive_ttl_rules"))

    policy = load_external_request_cache_lifecycle_policy(path)

    assert policy.positive_ttl_rules == ()
    assert evaluate_external_request_cache_lifecycle(
        status_code=200,
        response_headers={},
        observed_at=T0,
        policy=policy,
        provider="SEC EDGAR",
        api_family="companyfacts",
    ).persistent


def _rule(**changes: object) -> dict[str, object]:
    rule: dict[str, object] = {
        "lifecycle_class": BOUNDED_CLASS,
        "provider": "Some Vendor",
        "api_families": ["a"],
        "ttl_seconds": 60,
    }
    rule.update(changes)
    return rule


@pytest.mark.parametrize(
    "bad",
    [
        _rule(ttl_seconds=0),
        _rule(ttl_seconds=-5),
        _rule(ttl_seconds=True),
        _rule(ttl_seconds="60"),
        _rule(api_families=[]),
        _rule(api_families="a"),
        _rule(api_families=[""]),
        _rule(api_families=[3]),
        _rule(provider=""),
        _rule(lifecycle_class=""),
        "not a mapping",
    ],
)
def test_a_malformed_positive_rule_is_refused(tmp_path: Path, bad: object) -> None:
    path = _policy_file(tmp_path, lambda raw: raw["positive_ttl_rules"].append(bad))

    with pytest.raises(ValueError):
        load_external_request_cache_lifecycle_policy(path)


def test_overlapping_positive_rules_are_refused(tmp_path: Path) -> None:
    path = _policy_file(
        tmp_path,
        lambda raw: raw["positive_ttl_rules"].append(
            _rule(provider="SEC EDGAR", api_families=["x", "companyfacts"])
        ),
    )

    with pytest.raises(ValueError, match="overlap"):
        load_external_request_cache_lifecycle_policy(path)


def test_positive_rules_must_be_a_list(tmp_path: Path) -> None:
    path = _policy_file(tmp_path, lambda raw: raw.update(positive_ttl_rules={"provider": "x"}))

    with pytest.raises(ValueError):
        load_external_request_cache_lifecycle_policy(path)


# --- lookup and revalidation ---


def _lookup(
    cache_dir: Path, *, hours: float, provider: str = "SEC EDGAR", family: str = "companyfacts"
):
    return lookup_external_request_cache(
        provider=provider,
        api_family=family,
        method="GET",
        url=URL,
        cache_dir=cache_dir,
        evaluated_at=T0 + timedelta(hours=hours),
    )


def test_a_bounded_entry_hits_until_its_ttl_then_asks_for_revalidation(tmp_path: Path) -> None:
    stored = write_external_request_cache_response(
        provider="SEC EDGAR",
        api_family="companyfacts",
        method="GET",
        url=URL,
        status_code=200,
        response_headers={"content-type": "application/json"},
        content=b'{"stored": true}',
        cache_dir=tmp_path,
        requested_at=T0,
    )

    inside = _lookup(tmp_path, hours=11.99)
    expired = _lookup(tmp_path, hours=12)

    assert inside.status == "HIT" and inside.response is not None
    assert inside.response.content == b'{"stored": true}'
    assert expired.status == "EXPIRED_REVALIDATE" and expired.response is None
    metadata = json.loads(stored.cache_metadata_path.read_text(encoding="utf-8"))
    assert metadata["lifecycle_class"] == BOUNDED_CLASS and metadata["ttl_seconds"] == TTL_SECONDS
    assert (
        stored.cache_metadata_path.parent / metadata["body_path"]
    ).read_bytes() == b'{"stored": true}'


def test_a_persistent_entry_never_expires(tmp_path: Path) -> None:
    write_external_request_cache_response(
        provider="SEC EDGAR",
        api_family="filing_archive",
        method="GET",
        url=URL,
        status_code=200,
        response_headers={},
        content=b"filing",
        cache_dir=tmp_path,
        requested_at=T0,
    )

    found = _lookup(tmp_path, hours=24 * 400, family="filing_archive")

    assert found.status == "HIT" and found.response is not None


def test_an_expired_entry_is_fetched_live_once_and_then_served_again(tmp_path: Path) -> None:
    _seed(tmp_path, age=timedelta(hours=13))
    fake = _Requests(payload={"fresh": True})

    first = _get(tmp_path, fake)
    second = _get(tmp_path, fake)

    assert first.from_cache is False and first.json() == {"fresh": True}
    assert second.from_cache is True and second.json() == {"fresh": True}
    assert len(fake.calls) == 1


def test_a_fresh_entry_is_served_without_a_live_request(tmp_path: Path) -> None:
    _seed(tmp_path, age=timedelta(hours=1))
    fake = _Requests(payload={"fresh": True})

    found = _get(tmp_path, fake)

    assert found.from_cache is True and found.content == b'{"stored": true}'
    assert fake.calls == []


def test_a_persistent_family_is_served_whatever_its_age(tmp_path: Path) -> None:
    _seed(tmp_path, age=timedelta(days=400), family="filing_archive")
    fake = _Requests()

    found = _get(tmp_path, fake, family="filing_archive")

    assert found.from_cache is True and fake.calls == []


def test_an_entry_stored_before_the_rule_existed_is_not_served(tmp_path: Path) -> None:
    without_rules = dataclasses.replace(
        load_external_request_cache_lifecycle_policy(), positive_ttl_rules=()
    )
    stored = _seed(tmp_path, age=timedelta(minutes=5), lifecycle_policy=without_rules)
    assert json.loads(stored.cache_metadata_path.read_text(encoding="utf-8"))["ttl_seconds"] is None
    fake = _Requests(payload={"fresh": True})

    found = _get(tmp_path, fake)

    assert found.from_cache is False and found.json() == {"fresh": True}
    assert len(fake.calls) == 1
    assert _get(tmp_path, fake).from_cache is True


def test_a_failed_revalidation_raises_and_never_serves_the_stale_entry(tmp_path: Path) -> None:
    stored = _seed(tmp_path, age=timedelta(hours=13))
    fake = _Requests(error=ConnectionError("network down"))

    with pytest.raises(ConnectionError):
        _get(tmp_path, fake)

    assert len(fake.calls) >= 1
    assert stored.cache_metadata_path.exists()
    assert list(stored.cache_metadata_path.parent.glob("bodies/*.body"))
    again = lookup_external_request_cache(
        provider="SEC EDGAR",
        api_family="companyfacts",
        method="GET",
        url=URL,
        cache_dir=tmp_path,
    )
    assert again.response is None and again.status == "EXPIRED_REVALIDATE"


# --- the real callers use the names the policy lists ---


def test_sec_companyfacts_provider_refetches_after_the_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _Requests(payload={"cik": 1045810, "facts": {}})
    provider = SecEdgarCompanyFactsProvider(
        user_agent="owner@example.test", requests_module=fake, request_cache_dir=tmp_path
    )
    request = SecCompanyFactsRequest(ticker="NVDA", cik="0001045810")

    with external_request_cache_trace() as events:
        provider.download_companyfacts_raw(request)
        provider.download_companyfacts_raw(request)
        assert len(fake.calls) == 1
        _travel(monkeypatch, 13)
        provider.download_companyfacts_raw(request)
        assert len(fake.calls) == 2
        provider.download_companyfacts_raw(request)
        assert len(fake.calls) == 2

    assert {(event.provider, event.api_family) for event in events} == {
        ("SEC EDGAR", "companyfacts")
    }
    assert [event.from_cache for event in events] == [False, True, False, True]


def test_every_fmp_valuation_endpoint_is_bounded_and_refetched_after_the_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _Requests(payload=[{"symbol": "NVDA"}])
    provider = FmpHttpValuationProvider("key", requests_module=fake, request_cache_dir=tmp_path)

    def call_all() -> None:
        provider.fetch_quote_short("NVDA")
        provider.fetch_key_metrics_ttm("NVDA")
        provider.fetch_ratios_ttm("NVDA")
        provider.fetch_analyst_estimates("NVDA", period="annual", limit=10)
        provider.fetch_key_metrics("NVDA", period="annual", limit=5)
        provider.fetch_ratios("NVDA", period="annual", limit=5)
        provider.fetch_price_target_summary("NVDA")
        provider.fetch_price_target_consensus("NVDA")
        provider.fetch_grades("NVDA")
        provider.fetch_grades_consensus("NVDA")
        provider.fetch_ratings_snapshot("NVDA")
        provider.fetch_earnings_calendar(from_date=T0.date(), to_date=T0.date() + timedelta(days=7))

    with external_request_cache_trace() as events:
        call_all()
        assert len(fake.calls) == len(FMP_ENDPOINTS)
        call_all()
        assert len(fake.calls) == len(FMP_ENDPOINTS)
        _travel(monkeypatch, 13)
        call_all()
        assert len(fake.calls) == 2 * len(FMP_ENDPOINTS)

    assert {(event.provider, event.api_family) for event in events} == {
        (FMP_SOURCE_NAME, endpoint) for endpoint in FMP_ENDPOINTS
    }
    for event in events:
        assert _decision(event.provider, event.api_family).ttl_seconds == TTL_SECONDS


def test_official_policy_client_fetches_through_the_bounded_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, str]] = []

    def spy(*, provider: str, api_family: str, url: str, **kwargs: Any) -> CachedHttpResponse:
        seen.append((provider, api_family))
        return CachedHttpResponse(
            status_code=200,
            headers={},
            content=b"page",
            url=url,
            cache_key="k",
            cache_metadata_path=Path("metadata.json"),
            from_cache=False,
        )

    monkeypatch.setattr(official_policy_sources, "cached_urllib_get", spy)

    official_policy_sources.UrllibOfficialPolicyHttpClient().get("https://policy.example.test/page")

    assert seen == [("Official policy source", "source_fetch")]
    assert _decision(*seen[0]).ttl_seconds == TTL_SECONDS


# --- records ---


def test_the_requirement_and_flow_documents_describe_the_rule() -> None:
    requirement = (
        CODE_ROOT / "docs" / "requirements" / "DATA-024_External_Request_Cache_Positive_Ttl.md"
    ).read_text(encoding="utf-8")
    flow = (CODE_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    policy = DEFAULT_EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_PATH.read_text(encoding="utf-8")

    assert "positive_ttl_rules" in requirement and "positive_bounded_mutable_source" in requirement
    assert "DATA-024" in flow and "positive_ttl_rules" in flow
    assert "DATA-024" in policy
