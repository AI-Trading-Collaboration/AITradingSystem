from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.external_request_cache_policy import (
    ExternalRequestCacheLifecycleDecision,
    ExternalRequestCacheLifecyclePolicy,
    evaluate_external_request_cache_lifecycle,
    load_external_request_cache_lifecycle_policy,
)
from ai_trading_system.external_request_cache_revalidation_coordination import (
    ExternalRequestRevalidationCoordinator,
    RevalidationProbe,
)
from ai_trading_system.platform.artifacts import (
    ArtifactWriteError,
    ArtifactWriteResult,
    write_bytes_atomic,
    write_json_atomic,
)

LEGACY_EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION = "external_request_cache.v1"
EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION = "external_request_cache.v2"
EXTERNAL_REQUEST_CACHE_NEGATIVE_OBSERVATION_SCHEMA_VERSION = (
    "external_request_cache_negative_observation.v1"
)
EXTERNAL_REQUEST_CACHE_INVALIDATION_SCHEMA_VERSION = "external_request_cache_invalidation.v1"
EXTERNAL_REQUEST_CACHE_LIFECYCLE_EVENT_SCHEMA_VERSION = "external_request_cache_lifecycle_event.v1"
DEFAULT_EXTERNAL_REQUEST_CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "external_request_cache"
# Windows can briefly deny replace while another writer or reader has the pointer open.
_ATOMIC_REPLACE_CONTENTION_MAX_ATTEMPTS = 8
_ATOMIC_REPLACE_CONTENTION_BASE_DELAY_SECONDS = 0.005
ExternalRequestCacheLookupStatus = Literal[
    "HIT",
    "MISS",
    "EXPIRED_REVALIDATE",
    "INVALIDATED_REVALIDATE",
]

_SENSITIVE_PARAM_TOKENS = (
    "api_key",
    "apikey",
    "api-token",
    "api_token",
    "access_key",
    "authorization",
    "bearer",
    "client_secret",
    "cookie",
    "key",
    "password",
    "secret",
    "token",
)
_EXCLUDED_KEY_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "user-agent",
        "x-api-key",
        "x-client-secret",
    }
)


@dataclass(frozen=True)
class CachedHttpResponse:
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    url: str
    cache_key: str
    cache_metadata_path: Path
    from_cache: bool

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400

    @property
    def text(self) -> str:
        return self.content.decode(_charset_from_headers(self.headers), errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.ok:
            return
        raise CachedHttpStatusError(self)


class CachedHttpStatusError(RuntimeError):
    def __init__(self, response: CachedHttpResponse) -> None:
        super().__init__(
            f"Cached HTTP request failed: status_code={response.status_code}, url={response.url}"
        )
        self.response = response


@dataclass(frozen=True)
class ExternalRequestCacheLookup:
    cache_key: str
    metadata_path: Path
    body_path: Path
    response: CachedHttpResponse | None
    status: ExternalRequestCacheLookupStatus = "MISS"
    generation_id: str | None = None
    body_sha256: str | None = None


@dataclass(frozen=True)
class ExternalRequestCacheInvalidationResult:
    cache_key: str
    generation_id: str
    body_sha256: str
    invalidation_path: Path
    lifecycle_event_path: Path
    invalidated_at: datetime


@dataclass(frozen=True)
class _ValidatedCacheGeneration:
    generation_id: str | None
    body_path: Path
    body_sha256: str
    content: bytes
    expires_at: datetime | None


@dataclass(frozen=True)
class ExternalRequestCacheEvent:
    provider: str
    api_family: str
    cache_key: str
    cache_metadata_path: Path
    from_cache: bool
    status_code: int | None
    quota_limit: int | None = None
    quota_remaining: int | None = None
    increment_usage: int | None = None


_CACHE_TRACE_EVENTS: ContextVar[list[ExternalRequestCacheEvent] | None] = ContextVar(
    "external_request_cache_trace_events",
    default=None,
)


@contextmanager
def external_request_cache_trace() -> Iterator[list[ExternalRequestCacheEvent]]:
    events: list[ExternalRequestCacheEvent] = []
    token = _CACHE_TRACE_EVENTS.set(events)
    try:
        yield events
    finally:
        _CACHE_TRACE_EVENTS.reset(token)


def record_external_request_cache_event(
    *,
    provider: str,
    api_family: str,
    cache_key: str,
    cache_metadata_path: Path,
    from_cache: bool,
    status_code: int | None,
    response_headers: Mapping[str, str] | None = None,
) -> None:
    events = _CACHE_TRACE_EVENTS.get()
    if events is None:
        return
    headers = response_headers or {}
    events.append(
        ExternalRequestCacheEvent(
            provider=provider,
            api_family=api_family,
            cache_key=cache_key,
            cache_metadata_path=cache_metadata_path,
            from_cache=from_cache,
            status_code=status_code,
            quota_limit=_header_int(headers, "x-quota-limit"),
            quota_remaining=_header_int(headers, "x-quota-remaining"),
            increment_usage=_header_int(headers, "x-increment-usage"),
        )
    )


def cached_requests_get(
    *,
    provider: str,
    api_family: str,
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 30,
    requests_module: Any,
    cache_dir: Path | str | None = DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
) -> CachedHttpResponse:
    cache_dir = _effective_cache_dir(cache_dir)
    lookup = lookup_external_request_cache(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        params=params,
        headers=headers,
        cache_dir=None if cache_dir is None else Path(cache_dir),
    )
    if lookup.response is not None:
        record_external_request_cache_event(
            provider=provider,
            api_family=api_family,
            cache_key=lookup.response.cache_key,
            cache_metadata_path=lookup.response.cache_metadata_path,
            from_cache=True,
            status_code=lookup.response.status_code,
            response_headers=lookup.response.headers,
        )
        return lookup.response

    def fetch_live_response() -> CachedHttpResponse:
        kwargs: dict[str, Any] = {"timeout": timeout}
        if params is not None:
            kwargs["params"] = dict(params)
        if headers is not None:
            kwargs["headers"] = dict(headers)
        response = requests_module.get(url, **kwargs)
        status_code = _response_status_code(response)
        response_headers = _mapping_from_headers(getattr(response, "headers", {}) or {})
        content = _response_content(response)
        return CachedHttpResponse(
            status_code=status_code,
            headers=response_headers,
            content=content,
            url=url,
            cache_key=lookup.cache_key,
            cache_metadata_path=lookup.metadata_path,
            from_cache=False,
        )

    def publish_live_response(response: CachedHttpResponse) -> None:
        if cache_dir is None:
            return
        write_external_request_cache_response(
            provider=provider,
            api_family=api_family,
            method="GET",
            url=url,
            params=params,
            headers=headers,
            status_code=response.status_code,
            response_headers=response.headers,
            content=response.content,
            cache_dir=Path(cache_dir),
            requested_at=datetime.now(tz=UTC),
        )

    cached_response = _coordinate_revalidation_if_required(
        initial_lookup=lookup,
        lookup=lambda: lookup_external_request_cache(
            provider=provider,
            api_family=api_family,
            method="GET",
            url=url,
            params=params,
            headers=headers,
            cache_dir=None if cache_dir is None else Path(cache_dir),
        ),
        fetch=fetch_live_response,
        publish=publish_live_response,
    )
    record_external_request_cache_event(
        provider=provider,
        api_family=api_family,
        cache_key=cached_response.cache_key,
        cache_metadata_path=cached_response.cache_metadata_path,
        from_cache=cached_response.from_cache,
        status_code=cached_response.status_code,
        response_headers=cached_response.headers,
    )
    return cached_response


def cached_urllib_get(
    *,
    provider: str,
    api_family: str,
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 30,
    cache_dir: Path | str | None = DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
) -> CachedHttpResponse:
    cache_dir = _effective_cache_dir(cache_dir)
    lookup = lookup_external_request_cache(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        headers=headers,
        cache_dir=None if cache_dir is None else Path(cache_dir),
    )
    if lookup.response is not None:
        record_external_request_cache_event(
            provider=provider,
            api_family=api_family,
            cache_key=lookup.response.cache_key,
            cache_metadata_path=lookup.response.cache_metadata_path,
            from_cache=True,
            status_code=lookup.response.status_code,
            response_headers=lookup.response.headers,
        )
        return lookup.response

    def fetch_live_response() -> CachedHttpResponse:
        request = urllib.request.Request(
            url,
            headers=dict(headers or {}),
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = int(response.status)
                response_headers = dict(response.headers.items())
                content = response.read()
        except urllib.error.HTTPError as exc:
            status_code = int(exc.code)
            response_headers = dict(exc.headers.items())
            content = exc.read()

        return CachedHttpResponse(
            status_code=status_code,
            headers=response_headers,
            content=content,
            url=url,
            cache_key=lookup.cache_key,
            cache_metadata_path=lookup.metadata_path,
            from_cache=False,
        )

    def publish_live_response(response: CachedHttpResponse) -> None:
        if cache_dir is None:
            return
        write_external_request_cache_response(
            provider=provider,
            api_family=api_family,
            method="GET",
            url=url,
            headers=headers,
            status_code=response.status_code,
            response_headers=response.headers,
            content=response.content,
            cache_dir=Path(cache_dir),
            requested_at=datetime.now(tz=UTC),
        )

    cached_response = _coordinate_revalidation_if_required(
        initial_lookup=lookup,
        lookup=lambda: lookup_external_request_cache(
            provider=provider,
            api_family=api_family,
            method="GET",
            url=url,
            headers=headers,
            cache_dir=None if cache_dir is None else Path(cache_dir),
        ),
        fetch=fetch_live_response,
        publish=publish_live_response,
    )
    record_external_request_cache_event(
        provider=provider,
        api_family=api_family,
        cache_key=cached_response.cache_key,
        cache_metadata_path=cached_response.cache_metadata_path,
        from_cache=cached_response.from_cache,
        status_code=cached_response.status_code,
        response_headers=cached_response.headers,
    )
    return cached_response


def _coordinate_revalidation_if_required(
    *,
    initial_lookup: ExternalRequestCacheLookup,
    lookup: Callable[[], ExternalRequestCacheLookup],
    fetch: Callable[[], CachedHttpResponse],
    publish: Callable[[CachedHttpResponse], None],
) -> CachedHttpResponse:
    if initial_lookup.status not in {"EXPIRED_REVALIDATE", "INVALIDATED_REVALIDATE"}:
        response = fetch()
        publish(response)
        return response

    coordinator = ExternalRequestRevalidationCoordinator(
        initial_lookup.metadata_path.parent,
        cache_key=initial_lookup.cache_key,
    )

    def probe() -> RevalidationProbe[CachedHttpResponse]:
        current = lookup()
        if current.response is not None:
            return RevalidationProbe(
                status="REUSABLE",
                generation_id=current.generation_id,
                body_sha256=current.body_sha256,
                reason_code="CACHE_HIT",
                value=current.response,
            )
        if current.status in {"EXPIRED_REVALIDATE", "INVALIDATED_REVALIDATE"}:
            return RevalidationProbe(
                status="NEEDS_REVALIDATION",
                generation_id=current.generation_id,
                body_sha256=current.body_sha256,
                reason_code=current.status,
            )
        return RevalidationProbe(
            status="INVALID",
            generation_id=current.generation_id,
            body_sha256=current.body_sha256,
            reason_code="CACHE_LOOKUP_INVALID",
        )

    return coordinator.execute(probe=probe, fetch=fetch, publish=publish).value


def lookup_external_request_cache(
    *,
    provider: str,
    api_family: str,
    method: str,
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    json_payload: Mapping[str, Any] | None = None,
    cache_dir: Path | None = DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
    evaluated_at: datetime | None = None,
    lifecycle_policy: ExternalRequestCacheLifecyclePolicy | None = None,
) -> ExternalRequestCacheLookup:
    identity = external_request_identity(
        provider=provider,
        api_family=api_family,
        method=method,
        url=url,
        params=params,
        headers=headers,
        json_payload=json_payload,
    )
    cache_key = sha256_json(identity)
    metadata_path, legacy_body_path = external_request_cache_paths(
        cache_dir or DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
    )
    if cache_dir is None or not metadata_path.exists():
        return _empty_lookup(cache_key, metadata_path, legacy_body_path)

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_lookup(cache_key, metadata_path, legacy_body_path)
    policy = lifecycle_policy or load_external_request_cache_lifecycle_policy()
    generation = _validated_cache_generation(
        metadata=metadata,
        metadata_path=metadata_path,
        legacy_body_path=legacy_body_path,
        cache_key=cache_key,
        policy=policy,
    )
    if generation is None:
        return _empty_lookup(cache_key, metadata_path, legacy_body_path)

    current_time = _aware_utc(evaluated_at or datetime.now(tz=UTC), field="evaluated_at")
    if generation.expires_at is not None and current_time >= generation.expires_at:
        return ExternalRequestCacheLookup(
            cache_key=cache_key,
            metadata_path=metadata_path,
            body_path=generation.body_path,
            response=None,
            status="EXPIRED_REVALIDATE",
            generation_id=generation.generation_id,
            body_sha256=generation.body_sha256,
        )
    invalidation_status = _current_generation_invalidation_status(
        metadata_path=metadata_path,
        cache_key=cache_key,
        generation=generation,
    )
    if invalidation_status == "INVALID":
        return _empty_lookup(cache_key, metadata_path, generation.body_path)
    if invalidation_status == "TARGETED":
        return ExternalRequestCacheLookup(
            cache_key=cache_key,
            metadata_path=metadata_path,
            body_path=generation.body_path,
            response=None,
            status="INVALIDATED_REVALIDATE",
            generation_id=generation.generation_id,
            body_sha256=generation.body_sha256,
        )

    headers_payload = metadata.get("response_headers")
    response = CachedHttpResponse(
        status_code=int(metadata.get("status_code")),
        headers=dict(headers_payload) if isinstance(headers_payload, Mapping) else {},
        content=generation.content,
        url=str(metadata.get("url") or url),
        cache_key=cache_key,
        cache_metadata_path=metadata_path,
        from_cache=True,
    )
    return ExternalRequestCacheLookup(
        cache_key=cache_key,
        metadata_path=metadata_path,
        body_path=generation.body_path,
        response=response,
        status="HIT",
        generation_id=generation.generation_id,
        body_sha256=generation.body_sha256,
    )


def default_external_request_cache_dir(
    *,
    requests_module: Any | None = None,
    explicit_cache_dir: Path | str | None = None,
) -> Path | str | None:
    if explicit_cache_dir is not None:
        return explicit_cache_dir
    if requests_module is not None:
        return None
    if _running_under_pytest():
        return None
    return DEFAULT_EXTERNAL_REQUEST_CACHE_DIR


def write_external_request_cache_response(
    *,
    provider: str,
    api_family: str,
    method: str,
    url: str,
    status_code: int,
    response_headers: Mapping[str, str],
    content: bytes,
    cache_dir: Path = DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    json_payload: Mapping[str, Any] | None = None,
    requested_at: datetime | None = None,
    lifecycle_policy: ExternalRequestCacheLifecyclePolicy | None = None,
) -> CachedHttpResponse:
    identity = external_request_identity(
        provider=provider,
        api_family=api_family,
        method=method,
        url=url,
        params=params,
        headers=headers,
        json_payload=json_payload,
    )
    cache_key = sha256_json(identity)
    metadata_path, _legacy_body_path = external_request_cache_paths(
        cache_dir,
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
    )
    requested_timestamp = _aware_utc(
        requested_at or datetime.now(tz=UTC),
        field="requested_at",
    )
    body_sha256 = sha256(content).hexdigest()
    request_dir = metadata_path.parent
    body_path = request_dir / "bodies" / f"{body_sha256}.body"
    generation_id = _generation_id(requested_timestamp)
    policy = lifecycle_policy or load_external_request_cache_lifecycle_policy()
    safe_headers = safe_response_headers(response_headers)
    lifecycle = evaluate_external_request_cache_lifecycle(
        status_code=status_code,
        response_headers=safe_headers,
        observed_at=requested_timestamp,
        policy=policy,
    )
    _write_cache_bytes_atomically(body_path, content)
    negative_observation_path: Path | None = None
    negative_observation_sha256: str | None = None
    if lifecycle.expires_at is not None:
        negative_observation_path = request_dir / "negative_observations" / f"{generation_id}.json"
        observation = {
            "schema_version": EXTERNAL_REQUEST_CACHE_NEGATIVE_OBSERVATION_SCHEMA_VERSION,
            "cache_key": cache_key,
            "generation_id": generation_id,
            "observed_at": requested_timestamp.isoformat(),
            "provider": provider,
            "api_family": api_family,
            "request_identity": identity,
            "status_code": status_code,
            "response_headers": safe_headers,
            "body_path": _relative_cache_path(request_dir, body_path),
            "body_size_bytes": len(content),
            "body_sha256": body_sha256,
            "policy_id": lifecycle.policy_id,
            "policy_version": lifecycle.policy_version,
            "lifecycle_class": lifecycle.lifecycle_class,
            "ttl_seconds": lifecycle.ttl_seconds,
            "retry_after_seconds": lifecycle.retry_after_seconds,
            "expires_at": lifecycle.expires_at.isoformat(),
        }
        result = _write_cache_json_atomically(negative_observation_path, observation)
        negative_observation_sha256 = result.sha256
    metadata = {
        "schema_version": EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
        "cache_key": cache_key,
        "generation_id": generation_id,
        "created_at": requested_timestamp.isoformat(),
        "provider": provider,
        "api_family": api_family,
        "method": method.upper(),
        "url": _safe_url_for_metadata(url),
        "endpoint": identity["endpoint"],
        "endpoint_host": identity["endpoint_host"],
        "request_identity": identity,
        "status_code": status_code,
        "response_headers": safe_headers,
        "body_path": _relative_cache_path(request_dir, body_path),
        "body_size_bytes": len(content),
        "body_sha256": body_sha256,
        "policy_id": lifecycle.policy_id,
        "policy_version": lifecycle.policy_version,
        "lifecycle_class": lifecycle.lifecycle_class,
        "ttl_seconds": lifecycle.ttl_seconds,
        "retry_after_seconds": lifecycle.retry_after_seconds,
        "expires_at": (
            lifecycle.expires_at.isoformat() if lifecycle.expires_at is not None else None
        ),
        "negative_observation_path": (
            _relative_cache_path(request_dir, negative_observation_path)
            if negative_observation_path is not None
            else None
        ),
        "negative_observation_sha256": negative_observation_sha256,
    }
    _write_cache_json_atomically(metadata_path, metadata)
    return CachedHttpResponse(
        status_code=status_code,
        headers=dict(response_headers),
        content=content,
        url=url,
        cache_key=cache_key,
        cache_metadata_path=metadata_path,
        from_cache=False,
    )


def invalidate_external_request_cache(
    *,
    provider: str,
    api_family: str,
    method: str,
    url: str,
    expected_generation_id: str,
    expected_body_sha256: str,
    actor: str,
    reason: str,
    reference: str,
    cache_dir: Path = DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    json_payload: Mapping[str, Any] | None = None,
    invalidated_at: datetime | None = None,
    lifecycle_policy: ExternalRequestCacheLifecyclePolicy | None = None,
) -> ExternalRequestCacheInvalidationResult:
    actor_value = _required_audit_text(actor, field="actor")
    reason_value = _required_audit_text(reason, field="reason")
    reference_value = _required_audit_text(reference, field="reference")
    expected_generation = _required_audit_text(
        expected_generation_id,
        field="expected_generation_id",
    )
    expected_checksum = _required_sha256(
        expected_body_sha256,
        field="expected_body_sha256",
    )
    lookup = lookup_external_request_cache(
        provider=provider,
        api_family=api_family,
        method=method,
        url=url,
        params=params,
        headers=headers,
        json_payload=json_payload,
        cache_dir=cache_dir,
        evaluated_at=invalidated_at,
        lifecycle_policy=lifecycle_policy,
    )
    if lookup.generation_id is None:
        raise ValueError("cache invalidation requires a valid v2 current generation")
    if lookup.generation_id != expected_generation or lookup.body_sha256 != expected_checksum:
        raise ValueError("stale cache invalidation target")
    if not _current_pointer_matches(
        lookup.metadata_path,
        generation_id=expected_generation,
        body_sha256=expected_checksum,
    ):
        raise ValueError("stale cache invalidation target")

    timestamp = _aware_utc(
        invalidated_at or datetime.now(tz=UTC),
        field="invalidated_at",
    )
    event_id = _generation_id(timestamp)
    request_dir = lookup.metadata_path.parent
    event_path = request_dir / "lifecycle_events" / f"{event_id}.json"
    event = {
        "schema_version": EXTERNAL_REQUEST_CACHE_LIFECYCLE_EVENT_SCHEMA_VERSION,
        "event_id": event_id,
        "event_type": "EXPLICIT_INVALIDATION",
        "cache_key": lookup.cache_key,
        "generation_id": expected_generation,
        "body_sha256": expected_checksum,
        "actor": actor_value,
        "reason": reason_value,
        "reference": reference_value,
        "occurred_at": timestamp.isoformat(),
    }
    event_result = _write_cache_json_atomically(event_path, event)
    if not _current_pointer_matches(
        lookup.metadata_path,
        generation_id=expected_generation,
        body_sha256=expected_checksum,
    ):
        raise ValueError("stale cache invalidation target changed while writing lifecycle event")
    invalidation_path = request_dir / "invalidation.json"
    invalidation = {
        "schema_version": EXTERNAL_REQUEST_CACHE_INVALIDATION_SCHEMA_VERSION,
        "cache_key": lookup.cache_key,
        "generation_id": expected_generation,
        "body_sha256": expected_checksum,
        "actor": actor_value,
        "reason": reason_value,
        "reference": reference_value,
        "invalidated_at": timestamp.isoformat(),
        "lifecycle_event_path": _relative_cache_path(request_dir, event_path),
        "lifecycle_event_sha256": event_result.sha256,
    }
    _write_cache_json_atomically(invalidation_path, invalidation)
    if not _current_pointer_matches(
        lookup.metadata_path,
        generation_id=expected_generation,
        body_sha256=expected_checksum,
    ):
        raise ValueError("stale cache invalidation target changed while publishing invalidation")
    return ExternalRequestCacheInvalidationResult(
        cache_key=lookup.cache_key,
        generation_id=expected_generation,
        body_sha256=expected_checksum,
        invalidation_path=invalidation_path,
        lifecycle_event_path=event_path,
        invalidated_at=timestamp,
    )


def external_request_identity(
    *,
    provider: str,
    api_family: str,
    method: str,
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    json_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(url)
    endpoint = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    query_params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    return {
        # Request identity remains v1 so v2 storage can read existing cache keys.
        "schema_version": LEGACY_EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
        "provider": provider,
        "api_family": api_family,
        "method": method.upper(),
        "endpoint": endpoint,
        "endpoint_host": parsed.netloc,
        "url_query": _sanitize_query_pairs(query_params),
        "params": _sanitize_mapping(params or {}),
        "headers": _headers_for_key(headers or {}),
        "json_payload": _sanitize_mapping(json_payload or {}),
    }


def external_request_cache_paths(
    cache_dir: Path,
    *,
    provider: str,
    api_family: str,
    cache_key: str,
) -> tuple[Path, Path]:
    request_dir = (
        cache_dir
        / _safe_path_token(provider)
        / _safe_path_token(api_family)
        / cache_key[:2]
        / cache_key
    )
    return request_dir / "metadata.json", request_dir / "response.body"


def safe_request_headers(headers: Mapping[str, str]) -> dict[str, str]:
    safe_headers: dict[str, str] = {}
    for key, value in _stable_mapping_items(headers):
        normalized = str(key).lower()
        if normalized in _EXCLUDED_KEY_HEADERS or _is_sensitive_key(normalized):
            safe_headers[str(key)] = "***"
        else:
            safe_headers[str(key)] = str(value)
    return safe_headers


def safe_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(key): str(value) for key, value in _stable_mapping_items(headers)}


def sanitize_diagnostic_text(
    value: object,
    *,
    extra_secrets: tuple[str, ...] = (),
    max_length: int = 500,
) -> str:
    text = str(value)
    for secret in extra_secrets:
        if secret:
            text = text.replace(secret, "***")
    for key_token in _SENSITIVE_PARAM_TOKENS:
        pattern = rf"(?i)\b({re.escape(key_token)})=([^&\s;,)]+)"
        text = re.sub(pattern, lambda match: f"{match.group(1)}=***", text)
    text = re.sub(
        r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._\-]+",
        r"\1***",
        text,
    )
    text = re.sub(
        r"(?i)(bearer\s+)[A-Za-z0-9._\-]+",
        r"\1***",
        text,
    )
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def sha256_json(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(serialized.encode("utf-8")).hexdigest()


def _headers_for_key(headers: Mapping[str, str]) -> dict[str, str]:
    key_headers: dict[str, str] = {}
    for key, value in _stable_mapping_items(headers):
        normalized = str(key).lower()
        if normalized in _EXCLUDED_KEY_HEADERS or _is_sensitive_key(normalized):
            continue
        key_headers[str(key)] = str(value)
    return dict(sorted(key_headers.items()))


def _sanitize_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in _stable_mapping_items(values):
        key_str = str(key)
        if _is_sensitive_key(key_str):
            sanitized[key_str] = "***"
        else:
            sanitized[key_str] = _sanitize_value(value)
    return dict(sorted(sanitized.items()))


def _sanitize_query_pairs(values: list[tuple[str, str]]) -> list[tuple[str, str]]:
    sanitized: list[tuple[str, str]] = []
    for key, value in values:
        sanitized.append((key, "***" if _is_sensitive_key(key) else value))
    return sorted(sanitized)


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(token.replace("-", "_") in normalized for token in _SENSITIVE_PARAM_TOKENS)


def _safe_url_for_metadata(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    safe_query = urllib.parse.urlencode(_sanitize_query_pairs(urllib.parse.parse_qsl(parsed.query)))
    return urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, safe_query, parsed.fragment)
    )


def _response_content(response: Any) -> bytes:
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray):
        return bytes(content)
    text = getattr(response, "text", None)
    if isinstance(text, str):
        return text.encode("utf-8")
    if hasattr(response, "json"):
        return json.dumps(response.json(), ensure_ascii=False).encode("utf-8")
    return b""


def _response_status_code(response: Any) -> int:
    try:
        status_code = response.status_code
    except AttributeError:
        return 200
    return int(status_code)


def _effective_cache_dir(cache_dir: Path | str | None) -> Path | str | None:
    if cache_dir is None:
        return None
    if Path(cache_dir) == DEFAULT_EXTERNAL_REQUEST_CACHE_DIR and _running_under_pytest():
        return None
    return cache_dir


def _running_under_pytest() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _mapping_from_headers(headers: Any) -> dict[str, str]:
    if isinstance(headers, Mapping):
        return {str(key): str(value) for key, value in _stable_mapping_items(headers)}
    if hasattr(headers, "items"):
        return {str(key): str(value) for key, value in _stable_mapping_items(headers)}
    return {}


def _header_int(headers: Mapping[str, str], name: str) -> int | None:
    value: str | None = None
    wanted = name.lower()
    for key, header_value in _stable_mapping_items(headers):
        if str(key).lower() == wanted:
            value = str(header_value)
            break
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _validated_cache_generation(
    *,
    metadata: Any,
    metadata_path: Path,
    legacy_body_path: Path,
    cache_key: str,
    policy: ExternalRequestCacheLifecyclePolicy,
) -> _ValidatedCacheGeneration | None:
    if not isinstance(metadata, Mapping) or metadata.get("cache_key") != cache_key:
        return None
    schema_version = metadata.get("schema_version")
    if schema_version not in {
        LEGACY_EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
        EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
    }:
        return None
    status_code = metadata.get("status_code")
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        return None
    headers = metadata.get("response_headers")
    if not isinstance(headers, Mapping):
        return None
    request_identity = metadata.get("request_identity")
    if (
        not isinstance(request_identity, Mapping)
        or request_identity.get("schema_version") != LEGACY_EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION
        or sha256_json(request_identity) != cache_key
        or metadata.get("provider") != request_identity.get("provider")
        or metadata.get("api_family") != request_identity.get("api_family")
        or metadata.get("method") != request_identity.get("method")
    ):
        return None
    created_at = _parse_aware_datetime(metadata.get("created_at"))
    if created_at is None:
        return None
    body_sha256 = metadata.get("body_sha256")
    if not isinstance(body_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", body_sha256):
        return None

    request_dir = metadata_path.parent
    if schema_version == LEGACY_EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION:
        body_path = _validated_legacy_body_path(
            metadata=metadata,
            request_dir=request_dir,
            legacy_body_path=legacy_body_path,
        )
        generation_id = None
    else:
        generation_id_value = metadata.get("generation_id")
        if not isinstance(generation_id_value, str) or not generation_id_value.strip():
            return None
        generation_id = generation_id_value
        body_path = _resolve_contained_path(request_dir, metadata.get("body_path"))
        if body_path != (request_dir / "bodies" / f"{body_sha256}.body").resolve():
            return None
    if body_path is None:
        return None
    try:
        content = body_path.read_bytes()
    except OSError:
        return None
    body_size = metadata.get("body_size_bytes")
    if (
        not isinstance(body_size, int)
        or isinstance(body_size, bool)
        or body_size != len(content)
        or sha256(content).hexdigest() != body_sha256
    ):
        return None

    lifecycle = evaluate_external_request_cache_lifecycle(
        status_code=status_code,
        response_headers={str(key): str(value) for key, value in headers.items()},
        observed_at=created_at,
        policy=policy,
    )
    if schema_version == EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION:
        if not _valid_v2_lifecycle_metadata(metadata, lifecycle=lifecycle):
            return None
        if lifecycle.expires_at is not None:
            if generation_id is None or not _valid_negative_observation(
                metadata=metadata,
                request_dir=request_dir,
                cache_key=cache_key,
                generation_id=generation_id,
                status_code=status_code,
                body_sha256=body_sha256,
                lifecycle=lifecycle,
            ):
                return None
    return _ValidatedCacheGeneration(
        generation_id=generation_id,
        body_path=body_path,
        body_sha256=body_sha256,
        content=content,
        expires_at=lifecycle.expires_at,
    )


def _valid_v2_lifecycle_metadata(
    metadata: Mapping[str, Any],
    *,
    lifecycle: ExternalRequestCacheLifecycleDecision,
) -> bool:
    if metadata.get("policy_id") != lifecycle.policy_id:
        return False
    if metadata.get("policy_version") != lifecycle.policy_version:
        return False
    if metadata.get("lifecycle_class") != lifecycle.lifecycle_class:
        return False
    if metadata.get("ttl_seconds") != lifecycle.ttl_seconds:
        return False
    if metadata.get("retry_after_seconds") != lifecycle.retry_after_seconds:
        return False
    expires_at = metadata.get("expires_at")
    if lifecycle.expires_at is None:
        return expires_at is None
    parsed_expires_at = _parse_aware_datetime(expires_at)
    return parsed_expires_at == lifecycle.expires_at


def _valid_negative_observation(
    *,
    metadata: Mapping[str, Any],
    request_dir: Path,
    cache_key: str,
    generation_id: str,
    status_code: int,
    body_sha256: str,
    lifecycle: ExternalRequestCacheLifecycleDecision,
) -> bool:
    observation_path = _resolve_contained_path(
        request_dir,
        metadata.get("negative_observation_path"),
    )
    if observation_path != (request_dir / "negative_observations" / f"{generation_id}.json"):
        return False
    expected_sha256 = metadata.get("negative_observation_sha256")
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        return False
    try:
        raw = observation_path.read_bytes()
        observation = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if sha256(raw).hexdigest() != expected_sha256 or not isinstance(observation, Mapping):
        return False
    expected_body_path = metadata.get("body_path")
    return (
        observation.get("schema_version")
        == EXTERNAL_REQUEST_CACHE_NEGATIVE_OBSERVATION_SCHEMA_VERSION
        and observation.get("cache_key") == cache_key
        and observation.get("generation_id") == generation_id
        and observation.get("observed_at") == metadata.get("created_at")
        and observation.get("provider") == metadata.get("provider")
        and observation.get("api_family") == metadata.get("api_family")
        and observation.get("request_identity") == metadata.get("request_identity")
        and observation.get("status_code") == status_code
        and observation.get("response_headers") == metadata.get("response_headers")
        and observation.get("body_path") == expected_body_path
        and observation.get("body_size_bytes") == metadata.get("body_size_bytes")
        and observation.get("body_sha256") == body_sha256
        and observation.get("policy_id") == lifecycle.policy_id
        and observation.get("policy_version") == lifecycle.policy_version
        and observation.get("lifecycle_class") == lifecycle.lifecycle_class
        and observation.get("ttl_seconds") == lifecycle.ttl_seconds
        and observation.get("retry_after_seconds") == lifecycle.retry_after_seconds
        and lifecycle.expires_at is not None
        and observation.get("expires_at") == lifecycle.expires_at.isoformat()
    )


def _validated_legacy_body_path(
    *,
    metadata: Mapping[str, Any],
    request_dir: Path,
    legacy_body_path: Path,
) -> Path | None:
    expected = legacy_body_path.resolve()
    raw_path = metadata.get("body_path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None
    path = Path(raw_path)
    candidates = (
        [path.resolve()]
        if path.is_absolute()
        else [
            (request_dir / path).resolve(),
            path.resolve(),
        ]
    )
    return expected if expected in candidates else None


def _current_generation_invalidation_status(
    *,
    metadata_path: Path,
    cache_key: str,
    generation: _ValidatedCacheGeneration,
) -> Literal["NONE", "STALE", "TARGETED", "INVALID"]:
    if generation.generation_id is None:
        return "NONE"
    request_dir = metadata_path.parent
    invalidation_path = request_dir / "invalidation.json"
    if not invalidation_path.exists():
        return "NONE"
    try:
        invalidation = json.loads(invalidation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "INVALID"
    if not isinstance(invalidation, Mapping):
        return "INVALID"
    required_text = ("generation_id", "body_sha256", "actor", "reason", "reference")
    if (
        invalidation.get("schema_version") != EXTERNAL_REQUEST_CACHE_INVALIDATION_SCHEMA_VERSION
        or invalidation.get("cache_key") != cache_key
        or any(
            not isinstance(invalidation.get(field), str) or not str(invalidation.get(field)).strip()
            for field in required_text
        )
        or _parse_aware_datetime(invalidation.get("invalidated_at")) is None
        or not re.fullmatch(r"[0-9a-f]{64}", str(invalidation.get("body_sha256")))
    ):
        return "INVALID"
    event_path = _resolve_contained_path(request_dir, invalidation.get("lifecycle_event_path"))
    event_sha256 = invalidation.get("lifecycle_event_sha256")
    if (
        event_path is None
        or event_path.parent != request_dir / "lifecycle_events"
        or not isinstance(event_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", event_sha256)
    ):
        return "INVALID"
    try:
        event_raw = event_path.read_bytes()
        event = json.loads(event_raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return "INVALID"
    if (
        sha256(event_raw).hexdigest() != event_sha256
        or not isinstance(event, Mapping)
        or event.get("schema_version") != EXTERNAL_REQUEST_CACHE_LIFECYCLE_EVENT_SCHEMA_VERSION
        or event.get("event_type") != "EXPLICIT_INVALIDATION"
        or event.get("event_id") != event_path.stem
        or event.get("cache_key") != cache_key
        or event.get("generation_id") != invalidation.get("generation_id")
        or event.get("body_sha256") != invalidation.get("body_sha256")
        or event.get("actor") != invalidation.get("actor")
        or event.get("reason") != invalidation.get("reason")
        or event.get("reference") != invalidation.get("reference")
        or event.get("occurred_at") != invalidation.get("invalidated_at")
        or _parse_aware_datetime(event.get("occurred_at")) is None
    ):
        return "INVALID"
    if (
        invalidation.get("generation_id") != generation.generation_id
        or invalidation.get("body_sha256") != generation.body_sha256
    ):
        return "STALE"
    return "TARGETED"


def _current_pointer_matches(
    metadata_path: Path,
    *,
    generation_id: str,
    body_sha256: str,
) -> bool:
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(metadata, Mapping)
        and metadata.get("schema_version") == EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION
        and metadata.get("generation_id") == generation_id
        and metadata.get("body_sha256") == body_sha256
    )


def _charset_from_headers(headers: Mapping[str, str]) -> str:
    content_type = ""
    for key, value in _stable_mapping_items(headers):
        if str(key).lower() == "content-type":
            content_type = str(value)
            break
    for part in content_type.split(";"):
        token = part.strip()
        if token.lower().startswith("charset="):
            return token.split("=", 1)[1].strip() or "utf-8"
    return "utf-8"


def _stable_mapping_items(values: Any) -> tuple[tuple[Any, Any], ...]:
    for _ in range(2):
        try:
            return tuple(values.items())
        except RuntimeError as exc:
            if "dictionary keys changed during iteration" not in str(exc):
                raise
    copied = values.copy() if hasattr(values, "copy") else dict(values)
    return tuple(copied.items())


def _empty_lookup(
    cache_key: str,
    metadata_path: Path,
    body_path: Path,
) -> ExternalRequestCacheLookup:
    return ExternalRequestCacheLookup(
        cache_key=cache_key,
        metadata_path=metadata_path,
        body_path=body_path,
        response=None,
        status="MISS",
    )


def _resolve_contained_path(root: Path, raw_path: Any) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None
    path = Path(raw_path)
    candidate = path if path.is_absolute() else root / path
    try:
        resolved_root = root.resolve()
        resolved = candidate.resolve()
        resolved.relative_to(resolved_root)
    except (OSError, ValueError):
        return None
    return resolved


def _relative_cache_path(request_dir: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(request_dir.resolve()))
    except ValueError as exc:
        raise ValueError(f"cache artifact path escapes request directory: {path}") from exc


def _parse_aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _generation_id(timestamp: datetime) -> str:
    return f"{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid4().hex}"


def _required_audit_text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty text")
    return value.strip()


def _required_sha256(value: str, *, field: str) -> str:
    token = _required_audit_text(value, field=field)
    if not re.fullmatch(r"[0-9a-f]{64}", token):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return token


def _write_cache_bytes_atomically(path: Path, content: bytes) -> ArtifactWriteResult:
    return _atomic_replace_with_contention_retry(
        lambda: write_bytes_atomic(path, content),
    )


def _write_cache_json_atomically(
    path: Path,
    payload: Mapping[str, Any],
) -> ArtifactWriteResult:
    return _atomic_replace_with_contention_retry(
        lambda: write_json_atomic(path, payload),
    )


def _atomic_replace_with_contention_retry(
    writer: Callable[[], ArtifactWriteResult],
) -> ArtifactWriteResult:
    for attempt in range(_ATOMIC_REPLACE_CONTENTION_MAX_ATTEMPTS):
        try:
            result = writer()
        except ArtifactWriteError as exc:
            if not isinstance(exc.__cause__, PermissionError):
                raise
            if attempt + 1 >= _ATOMIC_REPLACE_CONTENTION_MAX_ATTEMPTS:
                raise
            time.sleep(_ATOMIC_REPLACE_CONTENTION_BASE_DELAY_SECONDS * (2**attempt))
        else:
            return result
    raise AssertionError("atomic replace retry loop exhausted without returning or raising")


def _safe_path_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    return token[:80] or "unknown"
