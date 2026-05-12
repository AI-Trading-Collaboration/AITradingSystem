from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION = "external_request_cache.v1"
DEFAULT_EXTERNAL_REQUEST_CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "external_request_cache"

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
        return lookup.response

    kwargs: dict[str, Any] = {"timeout": timeout}
    if params is not None:
        kwargs["params"] = dict(params)
    if headers is not None:
        kwargs["headers"] = dict(headers)
    response = requests_module.get(url, **kwargs)
    status_code = _response_status_code(response)
    response_headers = _mapping_from_headers(getattr(response, "headers", {}) or {})
    content = _response_content(response)
    if cache_dir is None:
        return CachedHttpResponse(
            status_code=status_code,
            headers=response_headers,
            content=content,
            url=url,
            cache_key=lookup.cache_key,
            cache_metadata_path=lookup.metadata_path,
            from_cache=False,
        )
    return write_external_request_cache_response(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        params=params,
        headers=headers,
        status_code=status_code,
        response_headers=response_headers,
        content=content,
        cache_dir=Path(cache_dir),
        requested_at=datetime.now(tz=UTC),
    )


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
        return lookup.response

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

    if cache_dir is None:
        return CachedHttpResponse(
            status_code=status_code,
            headers=response_headers,
            content=content,
            url=url,
            cache_key=lookup.cache_key,
            cache_metadata_path=lookup.metadata_path,
            from_cache=False,
        )
    return write_external_request_cache_response(
        provider=provider,
        api_family=api_family,
        method="GET",
        url=url,
        headers=headers,
        status_code=status_code,
        response_headers=response_headers,
        content=content,
        cache_dir=Path(cache_dir),
        requested_at=datetime.now(tz=UTC),
    )


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
    metadata_path, body_path = external_request_cache_paths(
        cache_dir or DEFAULT_EXTERNAL_REQUEST_CACHE_DIR,
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
    )
    if cache_dir is None or not metadata_path.exists() or not body_path.exists():
        return ExternalRequestCacheLookup(
            cache_key=cache_key,
            metadata_path=metadata_path,
            body_path=body_path,
            response=None,
        )

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        content = body_path.read_bytes()
    except (OSError, json.JSONDecodeError):
        return ExternalRequestCacheLookup(
            cache_key=cache_key,
            metadata_path=metadata_path,
            body_path=body_path,
            response=None,
        )
    if not _valid_metadata(metadata, cache_key, content):
        return ExternalRequestCacheLookup(
            cache_key=cache_key,
            metadata_path=metadata_path,
            body_path=body_path,
            response=None,
        )

    headers_payload = metadata.get("response_headers")
    response = CachedHttpResponse(
        status_code=int(metadata.get("status_code")),
        headers=dict(headers_payload) if isinstance(headers_payload, Mapping) else {},
        content=content,
        url=str(metadata.get("url") or url),
        cache_key=cache_key,
        cache_metadata_path=metadata_path,
        from_cache=True,
    )
    return ExternalRequestCacheLookup(
        cache_key=cache_key,
        metadata_path=metadata_path,
        body_path=body_path,
        response=response,
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
    metadata_path, body_path = external_request_cache_paths(
        cache_dir,
        provider=provider,
        api_family=api_family,
        cache_key=cache_key,
    )
    requested_timestamp = requested_at or datetime.now(tz=UTC)
    body_sha256 = sha256(content).hexdigest()
    _write_bytes_atomically(body_path, content)
    metadata = {
        "schema_version": EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
        "cache_key": cache_key,
        "created_at": requested_timestamp.isoformat(),
        "provider": provider,
        "api_family": api_family,
        "method": method.upper(),
        "url": _safe_url_for_metadata(url),
        "endpoint": identity["endpoint"],
        "endpoint_host": identity["endpoint_host"],
        "request_identity": identity,
        "status_code": status_code,
        "response_headers": safe_response_headers(response_headers),
        "body_path": str(body_path),
        "body_size_bytes": len(content),
        "body_sha256": body_sha256,
    }
    _write_json_atomically(metadata_path, metadata)
    return CachedHttpResponse(
        status_code=status_code,
        headers=dict(response_headers),
        content=content,
        url=url,
        cache_key=cache_key,
        cache_metadata_path=metadata_path,
        from_cache=False,
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
    endpoint = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, "", "", "")
    )
    query_params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    return {
        "schema_version": EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION,
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
    for key, value in headers.items():
        normalized = key.lower()
        if normalized in _EXCLUDED_KEY_HEADERS or _is_sensitive_key(normalized):
            safe_headers[str(key)] = "***"
        else:
            safe_headers[str(key)] = str(value)
    return safe_headers


def safe_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(key): str(value) for key, value in headers.items()}


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
    for key, value in headers.items():
        normalized = key.lower()
        if normalized in _EXCLUDED_KEY_HEADERS or _is_sensitive_key(normalized):
            continue
        key_headers[str(key)] = str(value)
    return dict(sorted(key_headers.items()))


def _sanitize_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in values.items():
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
        return {str(key): str(value) for key, value in headers.items()}
    if hasattr(headers, "items"):
        return {str(key): str(value) for key, value in headers.items()}
    return {}


def _valid_metadata(metadata: Any, cache_key: str, content: bytes) -> bool:
    if not isinstance(metadata, Mapping):
        return False
    if metadata.get("schema_version") != EXTERNAL_REQUEST_CACHE_SCHEMA_VERSION:
        return False
    if metadata.get("cache_key") != cache_key:
        return False
    if not isinstance(metadata.get("status_code"), int):
        return False
    expected_sha = metadata.get("body_sha256")
    return isinstance(expected_sha, str) and expected_sha == sha256(content).hexdigest()


def _charset_from_headers(headers: Mapping[str, str]) -> str:
    content_type = ""
    for key, value in headers.items():
        if key.lower() == "content-type":
            content_type = value
            break
    for part in content_type.split(";"):
        token = part.strip()
        if token.lower().startswith("charset="):
            return token.split("=", 1)[1].strip() or "utf-8"
    return "utf-8"


def _write_bytes_atomically(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_bytes(content)
    tmp_path.replace(path)


def _write_json_atomically(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _safe_path_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    return token[:80] or "unknown"
