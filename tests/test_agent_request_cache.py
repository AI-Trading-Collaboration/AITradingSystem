from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_system.agent_request_cache import (
    AGENT_REQUEST_CACHE_SCHEMA_VERSION,
    AgentCachedResponse,
    lookup_agent_request_cache,
    write_agent_request_archive_entry,
    write_agent_request_cache_entry,
)


def test_agent_request_cache_uses_provider_and_api_family(tmp_path: Path) -> None:
    now = datetime(2026, 5, 4, 12, tzinfo=UTC)
    request_payload = {"model": "gpt-test", "input": [{"role": "user", "content": "hello"}]}
    request_headers = {
        "Authorization": "Bearer sk-secret",
        "X-Client-Request-Id": "client-1",
    }
    response = AgentCachedResponse(
        status_code=200,
        headers={"x-request-id": "req_agent_1"},
        body={"id": "resp_agent_1", "output_text": "{}"},
    )

    miss = lookup_agent_request_cache(
        cache_dir=tmp_path,
        provider="openai",
        api_family="responses",
        endpoint="https://api.openai.com/v1/responses",
        request_payload=request_payload,
        input_checksum_sha256="input-checksum",
        now=now,
        ttl_seconds=24 * 60 * 60,
    )
    archive_path = write_agent_request_archive_entry(
        cache_dir=tmp_path,
        provider="openai",
        api_family="responses",
        cache_key=miss.cache_key,
        endpoint="https://api.openai.com/v1/responses",
        request_headers=request_headers,
        request_payload=request_payload,
        response=response,
        input_checksum_sha256="input-checksum",
        output_checksum_sha256="output-checksum",
        precheck_id="precheck:test",
        client_name="requests",
        timeout_seconds=120,
        client_request_id="client-1",
        timestamp=now,
        diagnostics={"transport": "openai_responses_api"},
        audit_attempts=({"attempt": 1, "http_status": 200},),
    )
    cache_path = write_agent_request_cache_entry(
        cache_dir=tmp_path,
        provider="openai",
        api_family="responses",
        cache_key=miss.cache_key,
        endpoint="https://api.openai.com/v1/responses",
        request_headers=request_headers,
        request_payload=request_payload,
        response=response,
        input_checksum_sha256="input-checksum",
        output_checksum_sha256="output-checksum",
        precheck_id="precheck:test",
        client_name="requests",
        timeout_seconds=120,
        client_request_id="client-1",
        timestamp=now,
        expires_at=now + timedelta(hours=24),
        archive_path=archive_path,
        diagnostics={"transport": "openai_responses_api"},
        audit_attempts=({"attempt": 1, "http_status": 200},),
    )

    cache_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cache_payload["schema_version"] == AGENT_REQUEST_CACHE_SCHEMA_VERSION
    assert cache_payload["provider"] == "openai"
    assert cache_payload["api_family"] == "responses"
    assert cache_payload["provider_request_id"] == "req_agent_1"
    assert cache_payload["client_name"] == "requests"
    assert cache_payload["request"]["headers"]["Authorization"] == "Bearer ***"
    assert "sk-secret" not in cache_path.read_text(encoding="utf-8")
    assert archive_path.parent == tmp_path / "archive" / "openai" / "responses" / "2026-05-04"

    hit = lookup_agent_request_cache(
        cache_dir=tmp_path,
        provider="openai",
        api_family="responses",
        endpoint="https://api.openai.com/v1/responses",
        request_payload=request_payload,
        input_checksum_sha256="input-checksum",
        now=now + timedelta(hours=1),
        ttl_seconds=24 * 60 * 60,
    )
    other_provider = lookup_agent_request_cache(
        cache_dir=tmp_path,
        provider="other-agent",
        api_family="responses",
        endpoint="https://api.openai.com/v1/responses",
        request_payload=request_payload,
        input_checksum_sha256="input-checksum",
        now=now + timedelta(hours=1),
        ttl_seconds=24 * 60 * 60,
    )

    assert hit.status == "HIT"
    assert hit.response == response
    assert other_provider.status == "MISS"
    assert other_provider.cache_key != hit.cache_key
