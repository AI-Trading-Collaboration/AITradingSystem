"""DATA-028: a failed or unusable live response must not block later runs forever.

2026-10-22 12:00 (second day of the daily release bundle, as_of 2026-10-21) failed twice in the
external request cache layer, not on the network:

* the official policy source was refused with OWNER_FAILURE_RETRY_DISABLED: the revalidation
  coordinator keeps the FAILED lease of the previous run (its fingerprint is the stale cache entry,
  which is unchanged because the refresh never succeeded), so every later run was refused without a
  live request;
* FRED answered HTTP 200 with a body that is not CSV; the cache stored it as a good response and the
  second attempt read the same bad body from the cache.
"""

from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import ai_trading_system.data.market_data as market_data_module
import ai_trading_system.external_request_cache_revalidation_coordination as coordination
from ai_trading_system.data.market_data import (
    FredRateProvider,
    ProviderDownloadError,
    RateRequest,
)
from ai_trading_system.external_request_cache import (
    cached_requests_get,
    lookup_external_request_cache,
    write_external_request_cache_response,
)
from ai_trading_system.external_request_cache_revalidation_coordination import (
    ExternalRequestRevalidationCoordinator,
    RevalidationCoordinationError,
    RevalidationProbe,
    load_revalidation_coordination_policy,
)

POLICY_PATH = (
    Path(__file__).parents[1]
    / "config/data/external_request_cache_revalidation_coordination_policy.yaml"
)
KEY_A = "a" * 64
BODY_OLD = "1" * 64
T0 = datetime(2026, 10, 22, 3, 0, tzinfo=UTC)
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_PROVIDER = "Federal Reserve Economic Data"
FRED_FAMILY = "fredgraph_csv"
GOOD_CSV = b"observation_date,DGS2\n2026-10-20,3.90\n2026-10-21,3.95\n"
# pandas: "Error tokenizing data. C error: Expected 1 fields in line 5, saw 3"
NOT_CSV = b"a\n1\n2\n3\n4,5,6\n"
WRONG_COLUMNS_CSV = b"observation_date,OTHER\n2026-10-20,1\n2026-10-21,2\n"
SEC_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json"


def _probe() -> RevalidationProbe[str]:
    return RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )


def _coordinator(tmp_path: Path) -> ExternalRequestRevalidationCoordinator:
    return ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)


def _fail_first_owner(coordinator: ExternalRequestRevalidationCoordinator) -> None:
    first = coordinator.acquire(_probe(), owner_id="owner-first000", now=T0)
    assert first.status == "OWNER"
    assert first.lease is not None
    coordinator.complete(first.lease, outcome="OWNER_FAILURE", now=T0)


def test_policy_still_fails_closed_for_waiters() -> None:
    policy = load_revalidation_coordination_policy(POLICY_PATH)

    assert policy.allow_waiter_retry_after_owner_failure is False
    assert policy.waiter_timeout_seconds == 135


def test_owner_failure_still_blocks_inside_the_waiter_window(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail_first_owner(coordinator)

    for seconds in (0, 1, 60, 134):
        blocked = coordinator.acquire(
            _probe(), owner_id="owner-late0000", now=T0 + timedelta(seconds=seconds)
        )
        assert blocked.status == "OWNER_FAILURE_BLOCKED"
        assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"

    replay = coordinator.replay()
    assert replay.current_state == "FAILED"
    assert len(replay.events) == 2


def test_owner_failure_does_not_block_a_later_run(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail_first_owner(coordinator)

    later = coordinator.acquire(
        _probe(), owner_id="owner-nextday0", now=T0 + timedelta(seconds=136)
    )

    assert later.status == "OWNER"
    assert later.lease is not None
    assert later.lease.generation == 2
    replay = coordinator.replay()
    assert replay.current_state == "ACTIVE"
    assert [event["event_type"] for event in replay.events] == ["ACQUIRED", "FAILED", "ACQUIRED"]

    coordinator.complete(later.lease, outcome="PUBLISHED", now=T0 + timedelta(seconds=140))
    assert coordinator.replay().current_state == "COMPLETED"


def test_a_second_failure_blocks_again_for_its_own_window(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    _fail_first_owner(coordinator)
    next_run = T0 + timedelta(days=1)

    second = coordinator.acquire(_probe(), owner_id="owner-second00", now=next_run)
    assert second.status == "OWNER"
    assert second.lease is not None
    coordinator.complete(second.lease, outcome="OWNER_FAILURE", now=next_run)

    blocked = coordinator.acquire(
        _probe(), owner_id="owner-third000", now=next_run + timedelta(seconds=30)
    )
    assert blocked.status == "OWNER_FAILURE_BLOCKED"
    assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"

    third = coordinator.acquire(
        _probe(), owner_id="owner-fourth00", now=next_run + timedelta(seconds=136)
    )
    assert third.status == "OWNER"
    assert third.lease is not None
    assert third.lease.generation == 3
    assert len(coordinator.replay().events) == 5


class _Response:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self.headers = {"content-type": "text/csv"}
        self.content = content
        self.text = content.decode("utf-8")

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError(self.status_code)


class _SequencedRequests:
    """Answers the n-th call with the n-th step; the last step repeats."""

    def __init__(self, steps: list[bytes | Exception]) -> None:
        self.steps = steps
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _Response:
        self.calls.append({"url": url, **kwargs})
        step = self.steps[min(len(self.calls), len(self.steps)) - 1]
        if isinstance(step, Exception):
            raise step
        return _Response(step)


def test_failed_revalidation_blocks_the_same_episode_but_not_a_later_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = replace(
        load_revalidation_coordination_policy(POLICY_PATH),
        lease_ttl_seconds=1,
        waiter_timeout_seconds=1,
    )
    monkeypatch.setattr(coordination, "load_revalidation_coordination_policy", lambda: policy)
    write_external_request_cache_response(
        provider="SEC EDGAR",
        api_family="companyfacts",
        method="GET",
        url=SEC_URL,
        status_code=200,
        response_headers={"content-type": "application/json"},
        content=b'{"stale": true}',
        cache_dir=tmp_path,
        requested_at=datetime.now(tz=UTC) - timedelta(hours=13),
    )
    fake_requests = _SequencedRequests([ConnectionError("reset by peer"), b'{"fresh": true}'])

    def call() -> Any:
        return cached_requests_get(
            provider="SEC EDGAR",
            api_family="companyfacts",
            url=SEC_URL,
            requests_module=fake_requests,
            cache_dir=tmp_path,
        )

    with pytest.raises(ConnectionError):
        call()
    with pytest.raises(RevalidationCoordinationError, match="OWNER_FAILURE_RETRY_DISABLED"):
        call()
    assert len(fake_requests.calls) == 1

    time.sleep(1.3)
    refreshed = call()

    assert refreshed.from_cache is False
    assert refreshed.json() == {"fresh": True}
    assert len(fake_requests.calls) == 2
    assert call().from_cache is True
    assert len(fake_requests.calls) == 2


def _provider(requests_module: Any, cache_dir: Path) -> FredRateProvider:
    return FredRateProvider(
        requests_module=requests_module,
        request_cache_dir=cache_dir,
        max_attempts=1,
        retry_backoff_seconds=0,
    )


def _request() -> RateRequest:
    return RateRequest(series_ids=["DGS2"], start=date(2026, 10, 1), end=date(2026, 10, 21))


def _download_error(provider: FredRateProvider) -> ProviderDownloadError:
    with pytest.raises(ProviderDownloadError) as caught:
        provider.download_rates(_request())
    return caught.value


EXPECTED_RATES = [
    {"date": "2026-10-20", "series": "DGS2", "value": 3.9},
    {"date": "2026-10-21", "series": "DGS2", "value": 3.95},
]


@pytest.mark.parametrize(
    ("bad_body", "stage"),
    [(NOT_CSV, "csv_parse"), (WRONG_COLUMNS_CSV, "schema")],
)
def test_fred_unusable_body_is_invalidated_so_the_next_call_fetches_again(
    tmp_path: Path, bad_body: bytes, stage: str
) -> None:
    fake_requests = _SequencedRequests([bad_body, GOOD_CSV])
    provider = _provider(fake_requests, tmp_path)

    error = _download_error(provider)

    assert error.diagnostic.stage == stage
    assert len(fake_requests.calls) == 1
    lookup = lookup_external_request_cache(
        provider=FRED_PROVIDER,
        api_family=FRED_FAMILY,
        method="GET",
        url=FRED_URL,
        params={"id": "DGS2", "cosd": "2026-10-01", "coed": "2026-10-21"},
        cache_dir=tmp_path,
    )
    assert lookup.response is None
    assert lookup.status == "INVALIDATED_REVALIDATE"

    rates = provider.download_rates(_request())

    assert len(fake_requests.calls) == 2
    assert rates.to_dict(orient="records") == EXPECTED_RATES


def test_fred_poisoned_cached_response_recovers_on_the_next_call(tmp_path: Path) -> None:
    write_external_request_cache_response(
        provider=FRED_PROVIDER,
        api_family=FRED_FAMILY,
        method="GET",
        url=FRED_URL,
        params={"id": "DGS2", "cosd": "2026-10-01", "coed": "2026-10-21"},
        status_code=200,
        response_headers={"content-type": "text/csv"},
        content=NOT_CSV,
        cache_dir=tmp_path,
    )
    fake_requests = _SequencedRequests([GOOD_CSV])
    provider = _provider(fake_requests, tmp_path)

    error = _download_error(provider)

    assert error.diagnostic.stage == "csv_parse"
    assert fake_requests.calls == []

    rates = provider.download_rates(_request())

    assert len(fake_requests.calls) == 1
    assert rates.to_dict(orient="records") == EXPECTED_RATES


def test_fred_usable_response_is_kept_in_the_cache(tmp_path: Path) -> None:
    fake_requests = _SequencedRequests([GOOD_CSV])
    provider = _provider(fake_requests, tmp_path)

    first = provider.download_rates(_request())
    second = provider.download_rates(_request())

    assert len(fake_requests.calls) == 1
    assert first.to_dict(orient="records") == EXPECTED_RATES
    assert second.to_dict(orient="records") == EXPECTED_RATES


def test_fred_invalidation_failure_does_not_hide_the_parse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(**_kwargs: Any) -> None:
        raise OSError("disk is read-only")

    monkeypatch.setattr(market_data_module, "invalidate_external_request_cache", refuse)
    fake_requests = _SequencedRequests([NOT_CSV])

    error = _download_error(_provider(fake_requests, tmp_path))

    assert error.diagnostic.stage == "csv_parse"
    assert "could not be parsed as CSV" in str(error)


def test_fred_invalidation_leaves_a_newer_generation_alone(tmp_path: Path) -> None:
    params = {"id": "DGS2", "cosd": "2026-10-01", "coed": "2026-10-21"}
    write_external_request_cache_response(
        provider=FRED_PROVIDER,
        api_family=FRED_FAMILY,
        method="GET",
        url=FRED_URL,
        params=params,
        status_code=200,
        response_headers={"content-type": "text/csv"},
        content=GOOD_CSV,
        cache_dir=tmp_path,
    )
    current = lookup_external_request_cache(
        provider=FRED_PROVIDER,
        api_family=FRED_FAMILY,
        method="GET",
        url=FRED_URL,
        params=params,
        cache_dir=tmp_path,
    )
    assert current.response is not None
    older_unusable_read = replace(current.response, content=NOT_CSV)

    _provider(_SequencedRequests([GOOD_CSV]), tmp_path)._invalidate_unusable_response(
        response=older_unusable_read,
        params=params,
        request_cache_dir=tmp_path,
        stage="csv_parse",
    )

    after = lookup_external_request_cache(
        provider=FRED_PROVIDER,
        api_family=FRED_FAMILY,
        method="GET",
        url=FRED_URL,
        params=params,
        cache_dir=tmp_path,
    )
    assert after.response is not None
    assert after.status != "INVALIDATED_REVALIDATE"
