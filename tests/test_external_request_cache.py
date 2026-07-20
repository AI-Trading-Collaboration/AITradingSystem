from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests

import ai_trading_system.data.market_data as market_data_module
import ai_trading_system.external_request_cache as external_request_cache_module
from ai_trading_system.data.market_data import (
    CboeVixPriceProvider,
    FmpPriceProvider,
    FredRateProvider,
    MarketstackPriceProvider,
    PriceRequest,
    ProviderDownloadError,
    RateRequest,
    YFinancePriceProvider,
)
from ai_trading_system.external_request_cache import (
    CachedHttpStatusError,
    cached_requests_get,
    external_request_cache_paths,
    external_request_cache_trace,
    external_request_identity,
    invalidate_external_request_cache,
    lookup_external_request_cache,
    safe_response_headers,
    sha256_json,
    write_external_request_cache_response,
)
from ai_trading_system.external_request_cache_policy import (
    evaluate_external_request_cache_lifecycle,
    load_external_request_cache_lifecycle_policy,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    SecEdgarCompanyFactsProvider,
)
from ai_trading_system.fundamentals.sec_pit_backfill import SecPitEdgarProvider


def test_cached_requests_get_reuses_identical_request_and_redacts_secret(
    tmp_path: Path,
) -> None:
    fake_requests = _FakeRequests(payload={"ok": True})

    with external_request_cache_trace() as events:
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
    assert [event.from_cache for event in events] == [False, True]

    metadata_text = next(tmp_path.rglob("metadata.json")).read_text(encoding="utf-8")
    metadata = json.loads(metadata_text)
    assert metadata["request_identity"]["params"]["apikey"] == "***"
    assert "secret-key" not in metadata_text
    assert "another-secret-key" not in metadata_text


def test_http_failure_lifecycle_policy_covers_ttl_and_retry_after_cap() -> None:
    policy = load_external_request_cache_lifecycle_policy()
    observed_at = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    assert policy.status == "pilot_baseline"
    assert policy.owner == "data_platform_owner + operations_owner"

    expected_ttls = {
        408: 300,
        425: 300,
        429: 300,
        500: 300,
        599: 300,
        401: 300,
        403: 300,
        404: 900,
        410: 900,
        400: 3600,
        499: 3600,
        199: 0,
        600: 0,
    }
    for status_code, expected_ttl in expected_ttls.items():
        decision = evaluate_external_request_cache_lifecycle(
            status_code=status_code,
            response_headers={},
            observed_at=observed_at,
            policy=policy,
        )
        assert decision.ttl_seconds == expected_ttl
        assert decision.expires_at == observed_at + timedelta(seconds=expected_ttl)

    capped = evaluate_external_request_cache_lifecycle(
        status_code=429,
        response_headers={"Retry-After": "99999"},
        observed_at=observed_at,
        policy=policy,
    )
    assert capped.retry_after_seconds == 21600
    assert capped.ttl_seconds == 21600

    credential = evaluate_external_request_cache_lifecycle(
        status_code=403,
        response_headers={"Retry-After": "99999"},
        observed_at=observed_at,
        policy=policy,
    )
    assert credential.retry_after_seconds is None
    assert credential.ttl_seconds == 300

    positive = evaluate_external_request_cache_lifecycle(
        status_code=304,
        response_headers={},
        observed_at=observed_at,
        policy=policy,
    )
    assert positive.persistent is True
    assert positive.ttl_seconds is None


def test_v2_negative_observation_hits_then_expires_without_deleting_evidence(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    response = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="negative",
        method="GET",
        url="https://vendor.example.test/failure",
        status_code=500,
        response_headers={"content-type": "text/plain"},
        content=b"temporary failure",
        cache_dir=tmp_path,
        requested_at=observed_at,
    )
    metadata = json.loads(response.cache_metadata_path.read_text(encoding="utf-8"))
    body_path = response.cache_metadata_path.parent / metadata["body_path"]
    observation_path = response.cache_metadata_path.parent / metadata["negative_observation_path"]

    within_ttl = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="negative",
        method="GET",
        url="https://vendor.example.test/failure",
        cache_dir=tmp_path,
        evaluated_at=observed_at + timedelta(seconds=299),
    )
    expired = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="negative",
        method="GET",
        url="https://vendor.example.test/failure",
        cache_dir=tmp_path,
        evaluated_at=observed_at + timedelta(seconds=300),
    )

    assert metadata["schema_version"] == "external_request_cache.v2"
    assert Path(metadata["body_path"]) == Path("bodies") / f"{metadata['body_sha256']}.body"
    assert within_ttl.status == "HIT"
    assert within_ttl.response is not None
    assert within_ttl.response.status_code == 500
    assert expired.status == "EXPIRED_REVALIDATE"
    assert expired.response is None
    assert body_path.read_bytes() == b"temporary failure"
    assert observation_path.exists()


def test_expired_failure_live_revalidates_and_preserves_old_observation(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2020, 1, 1, tzinfo=UTC)
    old = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="revalidate",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=503,
        response_headers={"content-type": "text/plain"},
        content=b"unavailable",
        cache_dir=tmp_path,
        requested_at=observed_at,
    )
    old_metadata = json.loads(old.cache_metadata_path.read_text(encoding="utf-8"))
    old_body = old.cache_metadata_path.parent / old_metadata["body_path"]
    old_observation = old.cache_metadata_path.parent / old_metadata["negative_observation_path"]
    fake_requests = _FakeRequests(payload={"ok": True})

    recovered = cached_requests_get(
        provider="Example Vendor",
        api_family="revalidate",
        url="https://vendor.example.test/data",
        requests_module=fake_requests,
        cache_dir=tmp_path,
    )
    current = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="revalidate",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )

    assert recovered.from_cache is False
    assert recovered.status_code == 200
    assert len(fake_requests.calls) == 1
    assert current.status == "HIT"
    assert current.response is not None and current.response.json() == {"ok": True}
    assert old_body.read_bytes() == b"unavailable"
    assert old_observation.exists()


def test_negative_observation_tamper_fails_closed_even_if_pointer_hash_is_updated(
    tmp_path: Path,
) -> None:
    response = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="observation-tamper",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=429,
        response_headers={"Retry-After": "600"},
        content=b"quota",
        cache_dir=tmp_path,
    )
    metadata = json.loads(response.cache_metadata_path.read_text(encoding="utf-8"))
    observation_path = response.cache_metadata_path.parent / metadata["negative_observation_path"]
    observation = json.loads(observation_path.read_text(encoding="utf-8"))
    observation["ttl_seconds"] = 99999
    tampered_raw = (
        json.dumps(observation, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode()
    observation_path.write_bytes(tampered_raw)
    metadata["negative_observation_sha256"] = sha256(tampered_raw).hexdigest()
    response.cache_metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    lookup = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="observation-tamper",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert lookup.status == "MISS"
    assert lookup.response is None


def test_legacy_v1_cache_key_and_response_body_remain_readable(tmp_path: Path) -> None:
    identity = external_request_identity(
        provider="Legacy Vendor",
        api_family="legacy",
        method="GET",
        url="https://legacy.example.test/data",
        params={"symbol": "NVDA", "apikey": "old-secret"},
    )
    assert identity["schema_version"] == "external_request_cache.v1"
    cache_key = sha256_json(identity)
    metadata_path, body_path = external_request_cache_paths(
        tmp_path,
        provider="Legacy Vendor",
        api_family="legacy",
        cache_key=cache_key,
    )
    content = b'{"legacy":true}'
    body_path.parent.mkdir(parents=True, exist_ok=True)
    body_path.write_bytes(content)
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": "external_request_cache.v1",
                "cache_key": cache_key,
                "created_at": "2026-07-20T00:00:00+00:00",
                "provider": "Legacy Vendor",
                "api_family": "legacy",
                "method": "GET",
                "request_identity": identity,
                "status_code": 200,
                "response_headers": {"content-type": "application/json"},
                "body_path": str(body_path),
                "body_size_bytes": len(content),
                "body_sha256": sha256(content).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    fake_requests = _FakeRequests(payload={"live": True})

    response = cached_requests_get(
        provider="Legacy Vendor",
        api_family="legacy",
        url="https://legacy.example.test/data",
        params={"symbol": "NVDA", "apikey": "new-secret"},
        requests_module=fake_requests,
        cache_dir=tmp_path,
    )

    assert response.from_cache is True
    assert response.json() == {"legacy": True}
    assert fake_requests.calls == []


def test_v2_cache_body_path_or_checksum_tamper_fails_closed(tmp_path: Path) -> None:
    response = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="tamper",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=200,
        response_headers={},
        content=b"trusted",
        cache_dir=tmp_path,
    )
    metadata = json.loads(response.cache_metadata_path.read_text(encoding="utf-8"))
    metadata["body_path"] = "../escaped.body"
    response.cache_metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    escaped = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="tamper",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert escaped.status == "MISS"
    assert escaped.response is None

    metadata["body_path"] = str(Path("bodies") / f"{metadata['body_sha256']}.body")
    response.cache_metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    body_path = response.cache_metadata_path.parent / metadata["body_path"]
    body_path.write_bytes(b"tampered")
    checksum_failure = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="tamper",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert checksum_failure.status == "MISS"
    assert checksum_failure.response is None


def test_cas_invalidation_preserves_event_and_new_generation_is_unlocked(
    tmp_path: Path,
) -> None:
    first = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="invalidate",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=200,
        response_headers={},
        content=b"first",
        cache_dir=tmp_path,
    )
    current = json.loads(first.cache_metadata_path.read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="stale cache invalidation target"):
        invalidate_external_request_cache(
            provider="Example Vendor",
            api_family="invalidate",
            method="GET",
            url="https://vendor.example.test/data",
            expected_generation_id="stale-generation",
            expected_body_sha256=current["body_sha256"],
            actor="operator@example.test",
            reason="credential rotation",
            reference="OPS-064-test",
            cache_dir=tmp_path,
        )

    result = invalidate_external_request_cache(
        provider="Example Vendor",
        api_family="invalidate",
        method="GET",
        url="https://vendor.example.test/data",
        expected_generation_id=current["generation_id"],
        expected_body_sha256=current["body_sha256"],
        actor="operator@example.test",
        reason="credential rotation",
        reference="OPS-064-test",
        cache_dir=tmp_path,
    )
    invalidated = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="invalidate",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert invalidated.status == "INVALIDATED_REVALIDATE"
    assert invalidated.response is None
    assert result.lifecycle_event_path.exists()
    assert result.invalidation_path.exists()

    second = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="invalidate",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=200,
        response_headers={},
        content=b"second",
        cache_dir=tmp_path,
    )
    unlocked = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="invalidate",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert unlocked.status == "HIT"
    assert unlocked.response is not None and unlocked.response.content == b"second"
    assert result.lifecycle_event_path.exists()
    assert result.invalidation_path.exists()
    assert second.cache_metadata_path == first.cache_metadata_path


def test_cas_invalidation_rejects_pointer_change_during_event_write(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    first = write_external_request_cache_response(
        provider="Example Vendor",
        api_family="invalidate-race",
        method="GET",
        url="https://vendor.example.test/data",
        status_code=200,
        response_headers={},
        content=b"first",
        cache_dir=tmp_path,
    )
    first_metadata = json.loads(first.cache_metadata_path.read_text(encoding="utf-8"))
    original_writer = external_request_cache_module._write_cache_json_atomically
    race_injected = False

    def racing_writer(path: Path, payload: Any) -> Any:
        nonlocal race_injected
        result = original_writer(path, payload)
        if path.parent.name == "lifecycle_events" and not race_injected:
            race_injected = True
            write_external_request_cache_response(
                provider="Example Vendor",
                api_family="invalidate-race",
                method="GET",
                url="https://vendor.example.test/data",
                status_code=200,
                response_headers={},
                content=b"racing-generation",
                cache_dir=tmp_path,
            )
        return result

    monkeypatch.setattr(
        external_request_cache_module,
        "_write_cache_json_atomically",
        racing_writer,
    )
    with pytest.raises(ValueError, match="changed while writing lifecycle event"):
        invalidate_external_request_cache(
            provider="Example Vendor",
            api_family="invalidate-race",
            method="GET",
            url="https://vendor.example.test/data",
            expected_generation_id=first_metadata["generation_id"],
            expected_body_sha256=first_metadata["body_sha256"],
            actor="operator@example.test",
            reason="race test",
            reference="OPS-064-race",
            cache_dir=tmp_path,
        )

    current = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="invalidate-race",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert current.status == "HIT"
    assert current.response is not None and current.response.content == b"racing-generation"
    assert len(list(first.cache_metadata_path.parent.glob("lifecycle_events/*.json"))) == 1
    assert not (first.cache_metadata_path.parent / "invalidation.json").exists()


def test_concurrent_writes_publish_a_pointer_to_a_complete_immutable_body(
    tmp_path: Path,
) -> None:
    contents = [f"payload-{index}".encode() for index in range(16)]

    def write(content: bytes) -> None:
        write_external_request_cache_response(
            provider="Example Vendor",
            api_family="concurrent",
            method="GET",
            url="https://vendor.example.test/data",
            status_code=200,
            response_headers={},
            content=content,
            cache_dir=tmp_path,
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(write, contents))

    lookup = lookup_external_request_cache(
        provider="Example Vendor",
        api_family="concurrent",
        method="GET",
        url="https://vendor.example.test/data",
        cache_dir=tmp_path,
    )
    assert lookup.status == "HIT"
    assert lookup.response is not None
    assert lookup.response.content in contents
    assert lookup.body_sha256 == sha256(lookup.response.content).hexdigest()
    assert len(list(lookup.metadata_path.parent.glob("bodies/*.body"))) == len(contents)


def test_safe_response_headers_retries_mutating_mapping() -> None:
    headers = _RaisesOnceHeaders({"content-type": "application/json", "x-request-id": "abc"})

    safe_headers = safe_response_headers(headers)

    assert safe_headers == {"content-type": "application/json", "x-request-id": "abc"}


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


def test_fmp_price_provider_retries_transient_ssl_error_then_succeeds(tmp_path: Path) -> None:
    fake_requests = _TransientSslRequests(
        payload=[
            {
                "date": "2026-07-16",
                "adjOpen": 319.0,
                "adjHigh": 324.0,
                "adjLow": 317.0,
                "adjClose": 322.0,
                "volume": 1234,
            }
        ],
        failures_before_success=1,
    )
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    prices = provider.download_prices(
        PriceRequest(tickers=["AMAT"], start=date(2026, 7, 16), end=date(2026, 7, 16))
    )

    assert prices.loc[0, "ticker"] == "AMAT"
    assert prices.loc[0, "date"] == "2026-07-16"
    assert len(fake_requests.calls) == 3  # failed raw + successful raw + adjusted
    assert len(list(tmp_path.rglob("metadata.json"))) == 2


def test_fmp_price_provider_reports_sanitized_exhausted_ssl_retries(
    tmp_path: Path,
) -> None:
    fake_requests = _TransientSslRequests(
        payload=[],
        failures_before_success=10,
        error_message="EOF occurred in violation of protocol apikey=test-key",
    )
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    try:
        provider.download_prices(
            PriceRequest(tickers=["AMAT"], start=date(2026, 7, 16), end=date(2026, 7, 16))
        )
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("FMP TLS failure did not raise ProviderDownloadError")

    assert diagnostic.provider == "Financial Modeling Prep"
    assert diagnostic.api_family == "eod_daily_prices"
    assert diagnostic.stage == "http_request"
    assert diagnostic.cache_status == "MISS_NO_RESPONSE"
    assert diagnostic.cache_key
    assert diagnostic.request_parameters == {
        "apikey": "***",
        "from": "2026-07-16",
        "symbol": "AMAT",
        "to": "2026-07-16",
    }
    assert diagnostic.exception_type == "SSLError"
    assert "test-key" not in (diagnostic.exception_message or "")
    assert diagnostic.attempt_count == 3
    assert diagnostic.max_attempts == 3
    assert diagnostic.timeout_seconds == 30
    assert len(fake_requests.calls) == 3
    assert not list(tmp_path.rglob("metadata.json"))


def test_fmp_price_provider_does_not_retry_http_error(tmp_path: Path) -> None:
    fake_requests = _FakeRequests(
        payload={"Error Message": "provider unavailable"},
        status_code=500,
    )
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    try:
        provider.download_prices(
            PriceRequest(tickers=["AMAT"], start=date(2026, 7, 16), end=date(2026, 7, 16))
        )
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("FMP HTTP failure did not raise ProviderDownloadError")

    assert diagnostic.stage == "http_status"
    assert diagnostic.http_status == 500
    assert diagnostic.attempt_count == 1
    assert len(fake_requests.calls) == 1


def test_fmp_price_provider_classifies_non_json_http_error_before_json(
    tmp_path: Path,
) -> None:
    fake_requests = _FakeRequests(
        payload=None,
        content=b"<html>temporary upstream failure</html>",
        status_code=500,
    )
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    try:
        provider.download_prices(
            PriceRequest(tickers=["AMAT"], start=date(2026, 7, 16), end=date(2026, 7, 16))
        )
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("FMP non-JSON HTTP failure did not raise ProviderDownloadError")

    assert diagnostic.stage == "http_status"
    assert diagnostic.http_status == 500
    assert diagnostic.error_code == "unknown"
    assert diagnostic.attempt_count == 1
    assert len(fake_requests.calls) == 1


def test_fmp_price_provider_classifies_provider_error_without_retry(tmp_path: Path) -> None:
    fake_requests = _FakeRequests(payload={"Error Message": "subscription unavailable"})
    provider = FmpPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_attempts=3,
        retry_backoff_seconds=0,
    )

    try:
        provider.download_prices(
            PriceRequest(tickers=["AMAT"], start=date(2026, 7, 16), end=date(2026, 7, 16))
        )
    except ProviderDownloadError as exc:
        diagnostic = exc.diagnostic
    else:
        raise AssertionError("FMP provider error did not raise ProviderDownloadError")

    assert diagnostic.stage == "provider_error"
    assert diagnostic.error_code == "subscription unavailable"
    assert diagnostic.attempt_count == 1
    assert len(fake_requests.calls) == 1


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


def test_sec_pit_provider_reuses_recent_403_under_shared_negative_lifecycle(
    tmp_path: Path,
) -> None:
    fake_requests = _FakeRequests(payload={"error": "forbidden"}, status_code=403)
    provider = SecPitEdgarProvider(
        user_agent="owner@example.test",
        max_requests_per_second=1_000_000,
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
        max_retries=0,
    )

    for _ in range(2):
        with pytest.raises(CachedHttpStatusError):
            provider.download_companyfacts_raw("NVDA", "0001045810")

    assert len(fake_requests.calls) == 1
    metadata = json.loads(next(tmp_path.rglob("metadata.json")).read_text(encoding="utf-8"))
    assert metadata["status_code"] == 403
    assert metadata["ttl_seconds"] == 300
    assert len(list(tmp_path.rglob("negative_observations/*.json"))) == 1


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


def test_cboe_vix_provider_reuses_stable_full_history_cache_for_new_window(
    tmp_path: Path,
) -> None:
    fake_requests = _FakeCboeRequests()
    provider = CboeVixPriceProvider(
        requests_module=fake_requests,
        request_cache_dir=tmp_path,
    )

    first = provider.download_prices(
        PriceRequest(tickers=["^VIX"], start=date(2026, 5, 1), end=date(2026, 5, 1))
    )
    second = provider.download_prices(
        PriceRequest(tickers=["^VIX"], start=date(2026, 5, 1), end=date(2026, 5, 2))
    )

    assert first["date"].tolist() == ["2026-05-01"]
    assert second["date"].tolist() == ["2026-05-01", "2026-05-02"]
    assert len(fake_requests.calls) == 1

    metadata = json.loads(next(tmp_path.rglob("metadata.json")).read_text(encoding="utf-8"))
    params = metadata["request_identity"]["params"]
    assert params == {
        "content": "full_history_csv",
        "interval": "1d",
        "ticker": "^VIX",
    }


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


def test_fred_provider_tail_refreshes_from_latest_cached_observation(tmp_path: Path) -> None:
    write_external_request_cache_response(
        provider="Federal Reserve Economic Data",
        api_family="fredgraph_csv",
        method="GET",
        url=FredRateProvider.base_url,
        params={"id": "DGS2", "cosd": "2018-01-01", "coed": "2026-06-02"},
        status_code=200,
        response_headers={"content-type": "text/csv"},
        content=b"observation_date,DGS2\n2026-06-01,4.05\n2026-06-02,\n",
        cache_dir=tmp_path,
    )
    fake_requests = _FakeRequests(
        payload={},
        content=b"observation_date,DGS2\n2026-06-01,4.05\n2026-06-02,4.05\n",
    )
    provider = FredRateProvider(requests_module=fake_requests, request_cache_dir=tmp_path)
    request = RateRequest(
        series_ids=["DGS2"],
        start=date(2018, 1, 1),
        end=date(2026, 6, 3),
    )

    rates = provider.download_rates(request)

    assert rates.to_dict(orient="records") == [
        {"date": "2026-06-01", "series": "DGS2", "value": 4.05},
        {"date": "2026-06-02", "series": "DGS2", "value": 4.05},
    ]
    assert len(fake_requests.calls) == 1
    assert fake_requests.calls[0]["params"] == {
        "id": "DGS2",
        "cosd": "2026-06-01",
        "coed": "2026-06-03",
    }


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
    def __init__(
        self,
        payload: object,
        *,
        content: bytes | None = None,
        status_code: int = 200,
    ) -> None:
        self.payload = payload
        self.content = content
        self.status_code = status_code
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return _FakeResponse(
            self.payload,
            status_code=self.status_code,
            content=self.content,
        )


class _TransientSslRequests(_FakeRequests):
    exceptions = requests.exceptions

    def __init__(
        self,
        payload: object,
        *,
        failures_before_success: int,
        error_message: str = "EOF occurred in violation of protocol",
    ) -> None:
        super().__init__(payload)
        self.failures_before_success = failures_before_success
        self.error_message = error_message

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise requests.exceptions.SSLError(self.error_message)
        return _FakeResponse(self.payload)


class _RaisesOnceHeaders(dict[str, str]):
    def __init__(self, values: dict[str, str]) -> None:
        super().__init__(values)
        self._raised = False

    def items(self):  # type: ignore[override]
        if not self._raised:
            self._raised = True
            raise RuntimeError("dictionary keys changed during iteration")
        return super().items()


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


class _FakeCboeResponse:
    text = (
        "DATE,OPEN,HIGH,LOW,CLOSE\n"
        "05/01/2026,16.5,17.2,15.9,16.8\n"
        "05/02/2026,18.0,19.0,17.5,18.2\n"
    )
    content = text.encode("utf-8")
    headers = {"content-type": "text/csv"}
    ok = True
    status_code = 200


class _FakeCboeRequests:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeCboeResponse:
        self.calls.append({"url": url, **kwargs})
        return _FakeCboeResponse()
