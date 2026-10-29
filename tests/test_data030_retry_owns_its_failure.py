"""DATA-030: a caller's own transport retry is not blocked by its own failed revalidation.

2026-10-28 12:00 JST (as_of 2026-10-27, release 20261023-63b5aa1a4) lost the whole daily run in
official_policy_sources. Every day the first fetch of an official page finds its 12-hour cache
entry expired, so it goes through the revalidation coordinator. The first network attempt failed,
the coordinator recorded the lease as FAILED, and the client's retry one second later (DATA-027)
was refused with OWNER_FAILURE_RETRY_DISABLED: since DATA-028 a FAILED lease blocks every acquire
of the same causal state for 135 seconds, the retrying caller included. No second request was sent.
SEC companyfacts (DATA-026) and the FMP valuation endpoints wrap the cache the same way.

Now one retry loop is one owner: the caller makes one owner id before its loop and passes it on
every attempt, and the coordinator lets that owner take a new lease after its own failure. Any other
owner is still blocked for 135 seconds, so a new loop or a new process cannot multiply requests.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import json
import re
import urllib.error
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import requests
import yaml

import ai_trading_system.data.market_data as market_data_module
import ai_trading_system.fundamentals.sec_companyfacts as sec_module
import ai_trading_system.official_policy_sources as official_module
import ai_trading_system.valuation_sources as valuation_module
from ai_trading_system.data.market_data import FredRateProvider, RateRequest
from ai_trading_system.external_request_cache import (
    CachedHttpResponse,
    ExternalRequestCacheLookup,
    cached_requests_get,
    cached_urllib_get,
    lookup_external_request_cache,
    write_external_request_cache_response,
)
from ai_trading_system.external_request_cache_revalidation_coordination import (
    ExternalRequestRevalidationCoordinator,
    RevalidationCoordinationError,
    RevalidationProbe,
    load_revalidation_coordination_policy,
    new_revalidation_owner_id,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    SecEdgarCompanyFactsProvider,
)
from ai_trading_system.official_policy_sources import (
    OfficialPolicyTransportError,
    UrllibOfficialPolicyHttpClient,
)
from ai_trading_system.valuation_sources import (
    EodhdHttpEarningsTrendsProvider,
    FmpHttpValuationProvider,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "ai_trading_system"
POLICY_PATH = ROOT / "config/data/external_request_cache_revalidation_coordination_policy.yaml"
POLICY_SHA256 = "55bce8a6ab0d9540912ad3097d9e561f855b68512281e9346b585d30bd380664"
LIFECYCLE_PATH = ROOT / "config/data/external_request_cache_lifecycle_policy.yaml"
LIFECYCLE_SHA256 = "56bf9a9d9afa5988e0946730d7f6bb0926208b2226b7bb715788548102553a2f"
OWNER_PATTERN = re.compile(r"owner-[a-z0-9-]{8,80}")
KEY_A = "a" * 64
T0 = datetime(2026, 10, 28, 3, 7, 11, tzinfo=UTC)

OFFICIAL_PROVIDER = "Official policy source"
OFFICIAL_FAMILY = "source_fetch"
OFFICIAL_URL = "https://www.federalregister.gov/api/v1/documents.json?per_page=50"
SEC_PROVIDER = "SEC EDGAR"
SEC_FAMILY = "companyfacts"
SEC_USER_AGENT = "owner@example.test"
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}
SEC_REQUEST = SecCompanyFactsRequest(ticker="NVDA", cik="0001045810")
FMP_PROVIDER = "Financial Modeling Prep"
FMP_KEY = "test-key"
STALE = b'{"stale": true}'
FRESH = b'{"fresh": true}'
FMP_ROWS = b'[{"symbol": "NVDA", "price": 180.5}]'
GOOD_CSV = b"observation_date,DGS2\n2026-10-20,3.90\n2026-10-21,3.95\n"


# --- helpers ---


def _probe() -> RevalidationProbe[str]:
    return RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256="1" * 64,
        reason_code="EXPIRED_REVALIDATE",
    )


def _coordinator(tmp_path: Path) -> ExternalRequestRevalidationCoordinator:
    return ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)


def _fail(coordinator: ExternalRequestRevalidationCoordinator, owner_id: str, at: datetime) -> None:
    acquired = coordinator.acquire(_probe(), owner_id=owner_id, now=at)
    assert acquired.status == "OWNER"
    assert acquired.lease is not None
    coordinator.complete(acquired.lease, outcome="OWNER_FAILURE", now=at)


def _seed_expired(
    tmp_path: Path,
    *,
    provider: str,
    api_family: str,
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    status_code: int = 200,
    age: timedelta = timedelta(hours=13),
) -> ExternalRequestCacheLookup:
    """Store an entry old enough to be expired, so the next call revalidates through a lease."""

    write_external_request_cache_response(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        params=params,
        headers=headers,
        status_code=status_code,
        response_headers={"content-type": "application/json"},
        content=STALE,
        cache_dir=tmp_path,
        requested_at=datetime.now(tz=UTC) - age,
    )
    lookup = lookup_external_request_cache(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        params=params,
        headers=headers,
        cache_dir=tmp_path,
    )
    assert lookup.status == "EXPIRED_REVALIDATE"
    return lookup


def _lease_events(lookup: ExternalRequestCacheLookup) -> list[tuple[str, str]]:
    coordinator = ExternalRequestRevalidationCoordinator(
        lookup.metadata_path.parent, cache_key=lookup.cache_key
    )
    replay = coordinator.replay()
    assert replay.status != "FAIL"
    return [(str(event["event_type"]), str(event["owner_id"])) for event in replay.events]


class _Response:
    """An HTTP 200 answer from the fake requests module."""

    status_code = 200
    ok = True

    def __init__(self, content: bytes) -> None:
        self.headers = {"content-type": "application/json"}
        self.content = content
        self.text = content.decode("utf-8")

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        return None


class _ScriptedRequests:
    """The n-th get() takes the n-th outcome; the last outcome repeats."""

    exceptions = requests.exceptions

    def __init__(self, outcomes: list[bytes | Exception]) -> None:
        self.outcomes = outcomes
        self.calls: list[str] = []

    def get(self, url: str, **_kwargs: Any) -> _Response:
        self.calls.append(url)
        outcome = self.outcomes[min(len(self.calls), len(self.outcomes)) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return _Response(outcome)


class _UrlopenResponse:
    status = 200

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers = self

    def items(self) -> list[tuple[str, str]]:
        return [("Content-Type", "application/json")]

    def read(self) -> bytes:
        return self.body

    def __enter__(self) -> _UrlopenResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def _scripted_urlopen(
    monkeypatch: pytest.MonkeyPatch, outcomes: list[bytes | Exception]
) -> list[str]:
    calls: list[str] = []

    def fake_urlopen(request: Any, timeout: float = 30) -> _UrlopenResponse:
        calls.append(str(request.full_url))
        outcome = outcomes[min(len(calls), len(outcomes)) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return _UrlopenResponse(outcome)

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    return calls


def _official_cache_in(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        official_module,
        "cached_urllib_get",
        functools.partial(cached_urllib_get, cache_dir=tmp_path),
    )


def _response(content: bytes) -> CachedHttpResponse:
    return CachedHttpResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        content=content,
        url="https://example.test/",
        cache_key="k",
        cache_metadata_path=Path("metadata.json"),
        from_cache=False,
    )


def _owner_spy(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    name: str,
    failure: Exception,
    content: bytes,
) -> list[str | None]:
    """Replace a module's cached GET: the first call fails, later calls answer `content`."""

    owners: list[str | None] = []

    def spy(**kwargs: Any) -> CachedHttpResponse:
        owners.append(kwargs.get("revalidation_owner_id"))
        if len(owners) == 1:
            raise failure
        return _response(content)

    monkeypatch.setattr(module, name, spy)
    return owners


# --- the coordinator ---


def test_owner_ids_are_valid_and_new_each_time() -> None:
    first = new_revalidation_owner_id()
    second = new_revalidation_owner_id()

    assert OWNER_PATTERN.fullmatch(first)
    assert OWNER_PATTERN.fullmatch(second)
    assert first != second


def test_the_same_owner_takes_a_new_lease_after_its_own_failure(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail(coordinator, "owner-loop0001", T0)

    retry = coordinator.acquire(_probe(), owner_id="owner-loop0001", now=T0 + timedelta(seconds=1))

    assert retry.status == "OWNER"
    assert retry.lease is not None
    assert retry.lease.generation == 2
    replay = coordinator.replay()
    assert replay.status != "FAIL"
    assert [(event["event_type"], event["owner_id"]) for event in replay.events] == [
        ("ACQUIRED", "owner-loop0001"),
        ("FAILED", "owner-loop0001"),
        ("ACQUIRED", "owner-loop0001"),
    ]


def test_another_owner_is_still_blocked_inside_the_window(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail(coordinator, "owner-loop0001", T0)

    for seconds in (0, 1, 60, 134):
        blocked = coordinator.acquire(
            _probe(), owner_id="owner-other001", now=T0 + timedelta(seconds=seconds)
        )
        assert blocked.status == "OWNER_FAILURE_BLOCKED"
        assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"
    later = coordinator.acquire(
        _probe(), owner_id="owner-other001", now=T0 + timedelta(seconds=136)
    )
    assert later.status == "OWNER"


def test_a_failed_retry_blocks_other_owners_for_its_own_window(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail(coordinator, "owner-loop0001", T0)
    _fail(coordinator, "owner-loop0001", T0 + timedelta(seconds=2))

    blocked = coordinator.acquire(
        _probe(), owner_id="owner-other001", now=T0 + timedelta(seconds=136)
    )
    assert blocked.status == "OWNER_FAILURE_BLOCKED"
    assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"

    later = coordinator.acquire(
        _probe(), owner_id="owner-other001", now=T0 + timedelta(seconds=138)
    )
    assert later.status == "OWNER"
    assert later.lease is not None
    assert later.lease.generation == 3


def test_only_the_owner_of_the_latest_failure_is_let_through(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail(coordinator, "owner-loop0001", T0)
    _fail(coordinator, "owner-other001", T0 + timedelta(seconds=200))

    blocked = coordinator.acquire(
        _probe(), owner_id="owner-loop0001", now=T0 + timedelta(seconds=201)
    )

    assert blocked.status == "OWNER_FAILURE_BLOCKED"
    assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"


def test_the_same_owner_still_waits_for_an_active_lease(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    first = coordinator.acquire(_probe(), owner_id="owner-loop0001", now=T0)
    assert first.status == "OWNER"

    again = coordinator.acquire(_probe(), owner_id="owner-loop0001", now=T0 + timedelta(seconds=1))

    assert again.status == "WAITER"
    assert again.lease is None


def test_the_coordination_and_lifecycle_policies_are_unchanged() -> None:
    policy = load_revalidation_coordination_policy(POLICY_PATH)

    assert policy.allow_waiter_retry_after_owner_failure is False
    assert policy.waiter_timeout_seconds == 135
    assert policy.lease_ttl_seconds == 120
    for path, expected in ((POLICY_PATH, POLICY_SHA256), (LIFECYCLE_PATH, LIFECYCLE_SHA256)):
        content = path.read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == expected


def test_execute_runs_every_lease_under_the_given_owner(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    published: list[str] = []
    fetches: list[str] = []

    def probe() -> RevalidationProbe[str]:
        if published:
            return RevalidationProbe[str](
                status="REUSABLE",
                generation_id="generation-2",
                body_sha256="2" * 64,
                reason_code="CACHE_HIT",
                value=published[-1],
            )
        return _probe()

    def failing_fetch() -> str:
        fetches.append("failed")
        raise ConnectionError("reset by peer")

    def fetch() -> str:
        fetches.append("ok")
        return "fresh"

    with pytest.raises(ConnectionError):
        coordinator.execute(
            probe=probe, fetch=failing_fetch, publish=published.append, owner_id="owner-loop0001"
        )
    result = coordinator.execute(
        probe=probe, fetch=fetch, publish=published.append, owner_id="owner-loop0001"
    )

    assert result.status == "WINNER_PUBLISHED"
    assert result.value == "fresh"
    assert result.lease_generation == 2
    assert fetches == ["failed", "ok"]
    assert [(event["event_type"], event["owner_id"]) for event in coordinator.replay().events] == [
        ("ACQUIRED", "owner-loop0001"),
        ("FAILED", "owner-loop0001"),
        ("ACQUIRED", "owner-loop0001"),
        ("COMPLETED", "owner-loop0001"),
    ]


def test_execute_without_an_owner_is_a_new_owner_each_call(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    fetches: list[str] = []

    def failing_fetch() -> str:
        fetches.append("failed")
        raise ConnectionError("reset by peer")

    with pytest.raises(ConnectionError):
        coordinator.execute(probe=_probe, fetch=failing_fetch, publish=lambda _value: None)
    with pytest.raises(RevalidationCoordinationError, match="OWNER_FAILURE_RETRY_DISABLED"):
        coordinator.execute(probe=_probe, fetch=failing_fetch, publish=lambda _value: None)

    assert fetches == ["failed"]
    owners = [event["owner_id"] for event in coordinator.replay().events]
    assert len(owners) == 2
    assert all(OWNER_PATTERN.fullmatch(str(owner)) for owner in owners)


def test_execute_rejects_a_malformed_owner_before_any_request(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    fetches: list[str] = []

    def fetch() -> str:
        fetches.append("called")
        return "fresh"

    with pytest.raises(ValueError, match="owner_id"):
        coordinator.execute(
            probe=_probe, fetch=fetch, publish=lambda _value: None, owner_id="Loop 1"
        )

    assert fetches == []
    assert coordinator.replay().events == ()


# --- the cache ---


def test_requests_retry_with_the_same_owner_reaches_the_network(tmp_path: Path) -> None:
    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json"
    lookup = _seed_expired(tmp_path, provider=SEC_PROVIDER, api_family=SEC_FAMILY, url=url)
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer"), FRESH])
    owner = new_revalidation_owner_id()

    def call(owner_id: str | None) -> CachedHttpResponse:
        return cached_requests_get(
            provider=SEC_PROVIDER,
            api_family=SEC_FAMILY,
            url=url,
            requests_module=scripted,
            cache_dir=tmp_path,
            revalidation_owner_id=owner_id,
        )

    with pytest.raises(requests.ConnectionError):
        call(owner)
    refreshed = call(owner)

    assert refreshed.from_cache is False
    assert refreshed.content == FRESH
    assert len(scripted.calls) == 2
    reused = call(None)
    assert reused.from_cache is True
    assert reused.content == FRESH
    assert len(scripted.calls) == 2
    assert _lease_events(lookup) == [
        ("ACQUIRED", owner),
        ("FAILED", owner),
        ("ACQUIRED", owner),
        ("COMPLETED", owner),
    ]


def test_requests_call_from_another_owner_is_still_refused(tmp_path: Path) -> None:
    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json"
    _seed_expired(tmp_path, provider=SEC_PROVIDER, api_family=SEC_FAMILY, url=url)
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer"), FRESH])

    def call(owner_id: str) -> CachedHttpResponse:
        return cached_requests_get(
            provider=SEC_PROVIDER,
            api_family=SEC_FAMILY,
            url=url,
            requests_module=scripted,
            cache_dir=tmp_path,
            revalidation_owner_id=owner_id,
        )

    with pytest.raises(requests.ConnectionError):
        call(new_revalidation_owner_id())
    with pytest.raises(RevalidationCoordinationError, match="OWNER_FAILURE_RETRY_DISABLED"):
        call(new_revalidation_owner_id())

    assert len(scripted.calls) == 1


def test_urllib_retry_with_the_same_owner_reaches_the_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookup = _seed_expired(
        tmp_path, provider=OFFICIAL_PROVIDER, api_family=OFFICIAL_FAMILY, url=OFFICIAL_URL
    )
    calls = _scripted_urlopen(monkeypatch, [urllib.error.URLError("reset by peer"), FRESH])
    owner = new_revalidation_owner_id()

    def call() -> CachedHttpResponse:
        return cached_urllib_get(
            provider=OFFICIAL_PROVIDER,
            api_family=OFFICIAL_FAMILY,
            url=OFFICIAL_URL,
            cache_dir=tmp_path,
            revalidation_owner_id=owner,
        )

    with pytest.raises(urllib.error.URLError):
        call()
    refreshed = call()

    assert refreshed.from_cache is False
    assert refreshed.content == FRESH
    assert len(calls) == 2
    assert [event for event, _owner in _lease_events(lookup)] == [
        "ACQUIRED",
        "FAILED",
        "ACQUIRED",
        "COMPLETED",
    ]


def test_the_owner_is_not_part_of_the_cache_key(tmp_path: Path) -> None:
    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000002.json"
    scripted = _ScriptedRequests([FRESH])

    first = cached_requests_get(
        provider=SEC_PROVIDER,
        api_family=SEC_FAMILY,
        url=url,
        requests_module=scripted,
        cache_dir=tmp_path,
        revalidation_owner_id=new_revalidation_owner_id(),
    )
    second = cached_requests_get(
        provider=SEC_PROVIDER,
        api_family=SEC_FAMILY,
        url=url,
        requests_module=scripted,
        cache_dir=tmp_path,
    )
    third = cached_requests_get(
        provider=SEC_PROVIDER,
        api_family=SEC_FAMILY,
        url=url,
        requests_module=scripted,
        cache_dir=tmp_path,
        revalidation_owner_id=new_revalidation_owner_id(),
    )

    assert first.from_cache is False
    assert second.from_cache is True
    assert third.from_cache is True
    assert first.cache_key == second.cache_key == third.cache_key
    assert len(scripted.calls) == 1
    metadata = json.loads(first.cache_metadata_path.read_text(encoding="utf-8"))
    assert "owner" not in json.dumps(metadata["request_identity"])
    assert not (first.cache_metadata_path.parent / "revalidation_coordination").exists()


# --- the six retry loops, end to end through a real cache ---


def test_official_source_retries_a_revalidation_that_failed_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookup = _seed_expired(
        tmp_path, provider=OFFICIAL_PROVIDER, api_family=OFFICIAL_FAMILY, url=OFFICIAL_URL
    )
    _official_cache_in(monkeypatch, tmp_path)
    calls = _scripted_urlopen(monkeypatch, [urllib.error.URLError("reset by peer"), FRESH])
    sleeps: list[float] = []

    response = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append).get(OFFICIAL_URL)

    assert response.status_code == 200
    assert response.body == FRESH
    assert response.transport_attempt_count == 2
    assert sleeps == [1.0]
    assert len(calls) == 2
    events = _lease_events(lookup)
    assert [event for event, _owner in events] == ["ACQUIRED", "FAILED", "ACQUIRED", "COMPLETED"]
    assert len({owner for _event, owner in events}) == 1


def test_official_source_exhausts_with_a_transport_error_not_a_cache_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_expired(
        tmp_path, provider=OFFICIAL_PROVIDER, api_family=OFFICIAL_FAMILY, url=OFFICIAL_URL
    )
    _official_cache_in(monkeypatch, tmp_path)
    calls = _scripted_urlopen(monkeypatch, [urllib.error.URLError("reset by peer")])
    sleeps: list[float] = []

    with pytest.raises(OfficialPolicyTransportError) as caught:
        UrllibOfficialPolicyHttpClient(sleeper=sleeps.append).get(OFFICIAL_URL)

    assert caught.value.attempt_count == 3
    assert caught.value.exception_type == "URLError"
    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]


def test_a_new_official_call_inside_the_window_sends_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_expired(
        tmp_path, provider=OFFICIAL_PROVIDER, api_family=OFFICIAL_FAMILY, url=OFFICIAL_URL
    )
    _official_cache_in(monkeypatch, tmp_path)
    calls = _scripted_urlopen(monkeypatch, [urllib.error.URLError("reset by peer")])
    client = UrllibOfficialPolicyHttpClient(sleeper=lambda _seconds: None)

    with pytest.raises(OfficialPolicyTransportError):
        client.get(OFFICIAL_URL)
    with pytest.raises(RevalidationCoordinationError, match="OWNER_FAILURE_RETRY_DISABLED"):
        client.get(OFFICIAL_URL)

    assert len(calls) == 3


def test_sec_companyfacts_retries_a_revalidation_that_failed_once(tmp_path: Path) -> None:
    sleeps: list[float] = []
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer"), FRESH])
    provider = SecEdgarCompanyFactsProvider(
        user_agent=SEC_USER_AGENT,
        requests_module=scripted,
        request_cache_dir=tmp_path,
        sleeper=sleeps.append,
    )
    lookup = _seed_expired(
        tmp_path,
        provider=SEC_PROVIDER,
        api_family=SEC_FAMILY,
        url=provider.endpoint_for(SEC_REQUEST.cik),
        headers=SEC_HEADERS,
    )

    assert provider.download_companyfacts_raw(SEC_REQUEST) == FRESH
    assert len(scripted.calls) == 2
    assert sleeps == [5.0]
    events = _lease_events(lookup)
    assert [event for event, _owner in events] == ["ACQUIRED", "FAILED", "ACQUIRED", "COMPLETED"]
    assert len({owner for _event, owner in events}) == 1


def test_sec_companyfacts_exhausts_with_the_last_transport_error(tmp_path: Path) -> None:
    sleeps: list[float] = []
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer")])
    provider = SecEdgarCompanyFactsProvider(
        user_agent=SEC_USER_AGENT,
        requests_module=scripted,
        request_cache_dir=tmp_path,
        sleeper=sleeps.append,
    )
    _seed_expired(
        tmp_path,
        provider=SEC_PROVIDER,
        api_family=SEC_FAMILY,
        url=provider.endpoint_for(SEC_REQUEST.cik),
        headers=SEC_HEADERS,
    )

    with pytest.raises(requests.ConnectionError):
        provider.download_companyfacts_raw(SEC_REQUEST)

    assert len(scripted.calls) == 3
    assert sleeps == [5.0, 15.0]


def test_fmp_valuation_retries_a_revalidation_that_failed_once(tmp_path: Path) -> None:
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer"), FMP_ROWS])
    provider = FmpHttpValuationProvider(
        FMP_KEY,
        requests_module=scripted,
        request_cache_dir=tmp_path,
        retry_backoff_seconds=0,
    )
    lookup = _seed_expired(
        tmp_path,
        provider=FMP_PROVIDER,
        api_family="quote-short",
        url=provider.endpoint_for("quote-short"),
        params={"symbol": "NVDA", "apikey": FMP_KEY},
    )

    assert provider.fetch_quote_short("NVDA") == [{"symbol": "NVDA", "price": 180.5}]
    assert len(scripted.calls) == 2
    events = _lease_events(lookup)
    assert [event for event, _owner in events] == ["ACQUIRED", "FAILED", "ACQUIRED", "COMPLETED"]
    assert len({owner for _event, owner in events}) == 1


def test_eodhd_retries_an_expired_failure_that_failed_again_once(tmp_path: Path) -> None:
    scripted = _ScriptedRequests([requests.ConnectionError("reset by peer"), b'{"trends": []}'])
    provider = EodhdHttpEarningsTrendsProvider(
        FMP_KEY,
        requests_module=scripted,
        request_cache_dir=tmp_path,
        retry_backoff_seconds=0,
    )
    _seed_expired(
        tmp_path,
        provider=valuation_module.EODHD_SOURCE_NAME,
        api_family=valuation_module.EODHD_EARNINGS_TRENDS_ENDPOINT,
        url=provider.endpoint_for(valuation_module.EODHD_EARNINGS_TRENDS_ENDPOINT),
        params={"symbols": "NVDA.US", "fmt": "json", "api_token": FMP_KEY},
        status_code=503,
        age=timedelta(minutes=10),
    )

    assert provider.fetch_earnings_trends(("NVDA.US",)) == {"trends": []}
    assert len(scripted.calls) == 2


# --- every retry loop keeps one owner per loop ---


def test_official_loop_passes_one_owner_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    owners = _owner_spy(
        monkeypatch, official_module, "cached_urllib_get", urllib.error.URLError("reset"), FRESH
    )
    client = UrllibOfficialPolicyHttpClient(sleeper=lambda _seconds: None)

    client.get(OFFICIAL_URL)
    client.get(OFFICIAL_URL)

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def test_sec_loop_passes_one_owner_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = _owner_spy(
        monkeypatch, sec_module, "cached_requests_get", requests.ConnectionError("reset"), FRESH
    )
    provider = SecEdgarCompanyFactsProvider(
        user_agent=SEC_USER_AGENT,
        requests_module=requests,
        request_cache_dir=tmp_path,
        sleeper=lambda _seconds: None,
    )

    provider.download_companyfacts_raw(SEC_REQUEST)
    provider.download_companyfacts_raw(SEC_REQUEST)

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def test_fmp_valuation_loop_passes_one_owner_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = _owner_spy(
        monkeypatch,
        valuation_module,
        "cached_requests_get",
        requests.ConnectionError("reset"),
        FMP_ROWS,
    )
    provider = FmpHttpValuationProvider(
        FMP_KEY, requests_module=requests, request_cache_dir=tmp_path, retry_backoff_seconds=0
    )

    provider.fetch_quote_short("NVDA")
    provider.fetch_quote_short("NVDA")

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def test_eodhd_loop_passes_one_owner_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = _owner_spy(
        monkeypatch,
        valuation_module,
        "cached_requests_get",
        requests.ConnectionError("reset"),
        b'{"trends": []}',
    )
    provider = EodhdHttpEarningsTrendsProvider(
        FMP_KEY, requests_module=requests, request_cache_dir=tmp_path, retry_backoff_seconds=0
    )

    provider.fetch_earnings_trends(("NVDA.US",))
    provider.fetch_earnings_trends(("NVDA.US",))

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def test_fred_loop_passes_one_owner_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = _owner_spy(
        monkeypatch,
        market_data_module,
        "cached_requests_get",
        requests.ConnectionError("reset"),
        GOOD_CSV,
    )
    provider = FredRateProvider(
        requests_module=requests,
        request_cache_dir=tmp_path,
        max_attempts=2,
        retry_backoff_seconds=0,
    )
    request = RateRequest(series_ids=["DGS2"], start=date(2026, 10, 1), end=date(2026, 10, 21))

    provider.download_rates(request)
    provider.download_rates(request)

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def test_fmp_price_loop_passes_one_owner_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = _owner_spy(
        monkeypatch,
        market_data_module,
        "cached_requests_get",
        requests.ConnectionError("reset"),
        b'[{"date": "2026-10-21", "close": 180.5}]',
    )

    def fetch() -> list[dict[str, Any]]:
        return market_data_module._fetch_fmp_price_records(
            requests=requests,
            url="https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted",
            params={"symbol": "NVDA", "from": "2026-10-01", "to": "2026-10-21"},
            response_label="FMP price",
            request_cache_dir=tmp_path,
            retry_backoff_seconds=0,
        )

    assert fetch() == [{"date": "2026-10-21", "close": 180.5}]
    fetch()

    _assert_one_owner_per_loop(owners, first_loop_calls=2)


def _assert_one_owner_per_loop(owners: list[str | None], *, first_loop_calls: int) -> None:
    """The first loop failed once and then succeeded; the second loop succeeded at once."""

    assert len(owners) == first_loop_calls + 1
    first_loop = owners[:first_loop_calls]
    second_loop = owners[first_loop_calls]
    assert first_loop[0] is not None
    assert OWNER_PATTERN.fullmatch(first_loop[0])
    assert set(first_loop) == {first_loop[0]}
    assert second_loop is not None
    assert OWNER_PATTERN.fullmatch(second_loop)
    assert second_loop != first_loop[0]


RETRY_LOOPS = {
    ("official_policy_sources.py", "get"),
    ("fundamentals/sec_companyfacts.py", "download_companyfacts_raw"),
    ("valuation_sources.py", "_get"),
    ("data/market_data.py", "_request_fred_response"),
    ("data/market_data.py", "_fetch_fmp_price_records"),
}
CACHED_GETS = {"cached_requests_get", "cached_urllib_get"}


def _retry_loop_calls() -> list[tuple[str, str, ast.Call, ast.For, ast.AST]]:
    """Cached GETs inside `for attempt in ...` loops, each with its innermost function."""

    found: dict[int, tuple[str, str, ast.Call, ast.For, ast.AST]] = {}
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = path.relative_to(SRC).as_posix()
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for loop in ast.walk(function):
                if not (
                    isinstance(loop, ast.For)
                    and isinstance(loop.target, ast.Name)
                    and loop.target.id == "attempt"
                ):
                    continue
                for node in ast.walk(loop):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id in CACHED_GETS
                    ):
                        found[id(node)] = (relative, function.name, node, loop, function)
    return list(found.values())


def test_every_retry_loop_over_the_cache_passes_its_own_owner() -> None:
    calls = _retry_loop_calls()

    assert {(path, function) for path, function, _call, _loop, _fn in calls} == RETRY_LOOPS
    assert len(calls) == 6
    for path, function_name, call, loop, function in calls:
        where = f"{path}:{function_name}"
        owner = [keyword for keyword in call.keywords if keyword.arg == "revalidation_owner_id"]
        assert len(owner) == 1, where
        assert isinstance(owner[0].value, ast.Name), where
        name = owner[0].value.id
        loop_nodes = {id(node) for node in ast.walk(loop)}
        assignments = [
            node
            for node in ast.walk(function)
            if isinstance(node, ast.Assign)
            and id(node) not in loop_nodes
            and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
        ]
        assert len(assignments) == 1, where
        value = assignments[0].value
        assert isinstance(value, ast.Call), where
        assert isinstance(value.func, ast.Name), where
        assert value.func.id == "new_revalidation_owner_id", where
        assert assignments[0].lineno < loop.lineno, where


# --- records ---


def test_the_records_describe_the_rule() -> None:
    requirement = (
        ROOT / "docs/requirements/DATA-030_Transport_Retry_Owns_Its_Failure.md"
    ).read_text(encoding="utf-8")
    flow = (ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    card = yaml.safe_load((ROOT / "tasks/DATA-030.yaml").read_text(encoding="utf-8"))
    roadmap = yaml.safe_load((ROOT / "docs/work_overview/roadmap.yaml").read_text(encoding="utf-8"))

    for text in (
        "revalidation_owner_id",
        "new_revalidation_owner_id",
        "OWNER_FAILURE_RETRY_DISABLED",
    ):
        assert text in requirement
    assert "DATA-030" in flow and "revalidation_owner_id" in flow
    assert card["id"] == "DATA-030"
    assert card["status"] == "IN_PROGRESS"
    assert "docs/requirements/DATA-030_Transport_Retry_Owns_Its_Failure.md" in card["notes"]
    assert roadmap["cards"]["DATA-030"]
    milestones = [
        milestone
        for line in roadmap["lines"]
        for stage in line["stages"]
        for milestone in stage.get("milestones", [])
        if "DATA-030" in milestone.get("cards", [])
    ]
    assert len(milestones) == 1
    assert milestones[0]["state"] == "now"
