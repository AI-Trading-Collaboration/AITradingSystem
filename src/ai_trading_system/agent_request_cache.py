from __future__ import annotations

import json
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

AgentRequestCacheStatus = Literal["DISABLED", "MISS", "EXPIRED", "HIT"]

AGENT_REQUEST_CACHE_SCHEMA_VERSION = "agent_request_cache.v1"
DEFAULT_AGENT_REQUEST_CACHE_DIR = Path("data/processed/agent_request_cache")
DEFAULT_AGENT_REQUEST_CACHE_TTL_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class AgentCachedResponse:
    status_code: int
    headers: Mapping[str, str]
    body: dict[str, Any]


@dataclass(frozen=True)
class AgentRequestCacheLookup:
    status: AgentRequestCacheStatus
    cache_key: str
    cache_path: Path | None = None
    response: AgentCachedResponse | None = None
    client_request_id: str = ""
    request_timestamp: datetime | None = None
    cache_created_at: datetime | None = None
    cache_expires_at: datetime | None = None


def lookup_agent_request_cache(
    *,
    cache_dir: Path | None,
    provider: str,
    api_family: str,
    endpoint: str,
    request_payload: Mapping[str, Any],
    input_checksum_sha256: str,
    now: datetime,
    ttl_seconds: float,
) -> AgentRequestCacheLookup:
    cache_key = agent_request_cache_key(
        provider=provider,
        api_family=api_family,
        endpoint=endpoint,
        request_payload=request_payload,
        input_checksum_sha256=input_checksum_sha256,
    )
    if cache_dir is None:
        return AgentRequestCacheLookup(status="DISABLED", cache_key=cache_key)

    cache_path = cache_dir / f"{cache_key}.json"
    if not cache_path.exists():
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)

    try:
        entry = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if not isinstance(entry, Mapping):
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if entry.get("schema_version") != AGENT_REQUEST_CACHE_SCHEMA_VERSION:
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if entry.get("cache_key") != cache_key:
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if entry.get("provider") != provider or entry.get("api_family") != api_family:
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if entry.get("input_checksum_sha256") != input_checksum_sha256:
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)

    created_at = parse_cache_datetime(entry.get("created_at"))
    expires_at = parse_cache_datetime(entry.get("expires_at"))
    if created_at is None:
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    if expires_at is None:
        expires_at = created_at + timedelta(seconds=ttl_seconds)
    if now >= expires_at:
        return AgentRequestCacheLookup(
            status="EXPIRED",
            cache_key=cache_key,
            cache_path=cache_path,
            cache_created_at=created_at,
            cache_expires_at=expires_at,
        )

    response_payload = entry.get("response")
    if not isinstance(response_payload, Mapping):
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    status_code = response_payload.get("status_code")
    body = response_payload.get("body")
    if not isinstance(status_code, int) or not isinstance(body, dict):
        return AgentRequestCacheLookup(status="MISS", cache_key=cache_key, cache_path=cache_path)
    headers = response_payload.get("headers")
    response = AgentCachedResponse(
        status_code=status_code,
        headers=dict(headers) if isinstance(headers, Mapping) else {},
        body=body,
    )
    return AgentRequestCacheLookup(
        status="HIT",
        cache_key=cache_key,
        cache_path=cache_path,
        response=response,
        client_request_id=str(entry.get("client_request_id") or ""),
        request_timestamp=parse_cache_datetime(entry.get("request_timestamp")),
        cache_created_at=created_at,
        cache_expires_at=expires_at,
    )


def write_agent_request_archive_entry(
    *,
    cache_dir: Path,
    provider: str,
    api_family: str,
    cache_key: str,
    endpoint: str,
    request_headers: Mapping[str, str],
    request_payload: Mapping[str, Any],
    response: AgentCachedResponse | None,
    input_checksum_sha256: str,
    output_checksum_sha256: str = "",
    precheck_id: str,
    client_name: str,
    timeout_seconds: float,
    client_request_id: str,
    timestamp: datetime,
    diagnostics: Mapping[str, Any],
    audit_attempts: tuple[Mapping[str, Any], ...],
) -> Path:
    archive_dir = cache_dir / "archive" / provider / api_family / timestamp.strftime("%Y-%m-%d")
    archive_path = archive_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}_"
        f"{cache_key}_{safe_filename_token(client_request_id)}.json"
    )
    payload = agent_request_cache_payload(
        record_kind="request_archive",
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
        endpoint=endpoint,
        request_headers=request_headers,
        request_payload=request_payload,
        response=response,
        input_checksum_sha256=input_checksum_sha256,
        output_checksum_sha256=output_checksum_sha256,
        precheck_id=precheck_id,
        client_name=client_name,
        timeout_seconds=timeout_seconds,
        client_request_id=client_request_id,
        timestamp=timestamp,
        expires_at=None,
        archive_path=None,
        diagnostics=diagnostics,
        audit_attempts=audit_attempts,
    )
    write_json_atomically(archive_path, payload)
    return archive_path


def write_agent_request_cache_entry(
    *,
    cache_dir: Path,
    provider: str,
    api_family: str,
    cache_key: str,
    endpoint: str,
    request_headers: Mapping[str, str],
    request_payload: Mapping[str, Any],
    response: AgentCachedResponse,
    input_checksum_sha256: str,
    output_checksum_sha256: str,
    precheck_id: str,
    client_name: str,
    timeout_seconds: float,
    client_request_id: str,
    timestamp: datetime,
    expires_at: datetime,
    archive_path: Path | None,
    diagnostics: Mapping[str, Any],
    audit_attempts: tuple[Mapping[str, Any], ...],
) -> Path:
    cache_path = cache_dir / f"{cache_key}.json"
    payload = agent_request_cache_payload(
        record_kind="success_cache",
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
        endpoint=endpoint,
        request_headers=request_headers,
        request_payload=request_payload,
        response=response,
        input_checksum_sha256=input_checksum_sha256,
        output_checksum_sha256=output_checksum_sha256,
        precheck_id=precheck_id,
        client_name=client_name,
        timeout_seconds=timeout_seconds,
        client_request_id=client_request_id,
        timestamp=timestamp,
        expires_at=expires_at,
        archive_path=archive_path,
        diagnostics=diagnostics,
        audit_attempts=audit_attempts,
    )
    write_json_atomically(cache_path, payload)
    return cache_path


def agent_request_cache_payload(
    *,
    record_kind: str,
    provider: str,
    api_family: str,
    cache_key: str,
    endpoint: str,
    request_headers: Mapping[str, str],
    request_payload: Mapping[str, Any],
    response: AgentCachedResponse | None,
    input_checksum_sha256: str,
    output_checksum_sha256: str,
    precheck_id: str,
    client_name: str,
    timeout_seconds: float,
    client_request_id: str,
    timestamp: datetime,
    expires_at: datetime | None,
    archive_path: Path | None,
    diagnostics: Mapping[str, Any],
    audit_attempts: tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    response_payload = None
    if response is not None:
        response_payload = {
            "status_code": response.status_code,
            "headers": safe_response_headers(response.headers),
            "body": response.body,
        }
    return {
        "schema_version": AGENT_REQUEST_CACHE_SCHEMA_VERSION,
        "record_kind": record_kind,
        "provider": provider,
        "api_family": api_family,
        "cache_key": cache_key,
        "created_at": timestamp.isoformat(),
        "expires_at": None if expires_at is None else expires_at.isoformat(),
        "request_timestamp": timestamp.isoformat(),
        "precheck_id": precheck_id,
        "endpoint": endpoint,
        "endpoint_host": urllib.parse.urlparse(endpoint).netloc,
        "client_name": client_name,
        "timeout_seconds": timeout_seconds,
        "client_request_id": client_request_id,
        "provider_request_id": ""
        if response is None
        else _response_header(response.headers, "x-request-id"),
        "input_checksum_sha256": input_checksum_sha256,
        "output_checksum_sha256": output_checksum_sha256,
        "request_payload_checksum_sha256": sha256_json(request_payload),
        "request": {
            "headers": safe_request_headers(request_headers),
            "payload": request_payload,
        },
        "response": response_payload,
        "diagnostics": dict(diagnostics),
        "attempts": [dict(attempt) for attempt in audit_attempts],
        "archive_path": "" if archive_path is None else str(archive_path),
    }


def agent_request_cache_key(
    *,
    provider: str,
    api_family: str,
    endpoint: str,
    request_payload: Mapping[str, Any],
    input_checksum_sha256: str,
) -> str:
    return sha256_json(
        {
            "schema_version": AGENT_REQUEST_CACHE_SCHEMA_VERSION,
            "provider": provider,
            "api_family": api_family,
            "endpoint": endpoint,
            "input_checksum_sha256": input_checksum_sha256,
            "request_payload": request_payload,
        }
    )


def agent_cache_report_counts(
    cache_status: AgentRequestCacheStatus,
    cache_write_count: int,
) -> dict[str, int]:
    return {
        "agent_request_count": 1,
        "agent_cache_hit_count": 1 if cache_status == "HIT" else 0,
        "agent_cache_miss_count": 1 if cache_status in {"MISS", "EXPIRED"} else 0,
        "agent_cache_expired_count": 1 if cache_status == "EXPIRED" else 0,
        "agent_cache_disabled_count": 1 if cache_status == "DISABLED" else 0,
        "agent_cache_write_count": cache_write_count,
    }


def parse_cache_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def safe_request_headers(headers: Mapping[str, str]) -> dict[str, str]:
    safe_headers: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() == "authorization":
            safe_headers[key] = "Bearer ***"
        else:
            safe_headers[key] = str(value)
    return safe_headers


def safe_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(key): str(value) for key, value in headers.items()}


def safe_filename_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    return token[:80] or "no_client_request_id"


def write_json_atomically(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def sha256_json(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(serialized.encode("utf-8")).hexdigest()


def _response_header(headers: Mapping[str, str], name: str) -> str:
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return ""
