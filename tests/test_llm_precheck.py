from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

import ai_trading_system.llm_precheck as llm_precheck_module
from ai_trading_system.cli import app
from ai_trading_system.config import (
    DataSourceConfig,
    DataSourceLlmPermissionConfig,
    DataSourcesConfig,
)
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_HTTP_CLIENT,
    DEFAULT_OPENAI_LLM_MODEL,
    DEFAULT_OPENAI_MAX_RETRIES,
    DEFAULT_OPENAI_REASONING_EFFORT,
    DEFAULT_OPENAI_TIMEOUT_SECONDS,
    OPENAI_LLM_CLAIM_RESPONSE_FORMAT,
    LlmClaimPrecheckInput,
    OpenAIJsonResponse,
    load_llm_claim_precheck_input,
    run_openai_claim_precheck,
    write_llm_claim_prereview_queue,
)


def test_llm_claim_precheck_fails_closed_without_provider_permission(
    tmp_path: Path,
) -> None:
    packet = _packet(source_id="vendor_news", content_sent_level="full_text")
    called = False

    def fake_post(
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        nonlocal called
        called = True
        return _openai_response()

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="vendor_news",
                    source_type="paid_vendor",
                    external_llm_allowed=False,
                    max_content_sent_level="metadata_only",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert called is False
    assert report.status == "FAIL"
    assert "llm_precheck_permission_denied" in {issue.code for issue in report.issues}


def test_openai_precheck_defaults_are_daily_safe() -> None:
    assert DEFAULT_OPENAI_LLM_MODEL == "gpt-5.5"
    assert DEFAULT_OPENAI_REASONING_EFFORT == "high"
    assert DEFAULT_OPENAI_TIMEOUT_SECONDS == 120.0
    assert DEFAULT_OPENAI_MAX_RETRIES == 2
    assert DEFAULT_OPENAI_HTTP_CLIENT == "requests"


def test_llm_claim_precheck_rejects_unsupported_reasoning_effort(
    tmp_path: Path,
) -> None:
    packet = _packet(content_sent_level="full_text")
    called = False

    def fake_post(
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        nonlocal called
        called = True
        return _openai_response()

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        reasoning_effort="ultra",
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert called is False
    assert report.status == "FAIL"
    assert "openai_reasoning_effort_invalid" in {issue.code for issue in report.issues}


def test_llm_claim_precheck_rejects_unsupported_http_client(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    packet = _packet(content_sent_level="full_text")

    def fail_post(*_args: Any, **_kwargs: Any) -> OpenAIJsonResponse:
        raise AssertionError("unsupported http_client must fail before sending a request")

    monkeypatch.setattr(llm_precheck_module, "_post_json_requests", fail_post)

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_client="httpx",
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert report.record_count == 0
    assert "openai_http_client_invalid" in {issue.code for issue in report.issues}


def test_llm_claim_precheck_uses_requests_client_by_default(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    calls: list[dict[str, Any]] = []

    class FakeRequestsResponse:
        status_code = 200
        headers = {"x-request-id": "req_requests_default"}
        text = ""

        def json(self) -> dict[str, Any]:
            return _openai_response(request_id="req_requests_default").body

    def fake_requests_post(
        url: str,
        *,
        headers: Mapping[str, str],
        json: Mapping[str, Any],
        timeout: float,
    ) -> FakeRequestsResponse:
        calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "payload": dict(json),
                "timeout": timeout,
            }
        )
        return FakeRequestsResponse()

    monkeypatch.setattr(llm_precheck_module.requests, "post", fake_requests_post)

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert len(calls) == 1
    assert calls[0]["timeout"] == DEFAULT_OPENAI_TIMEOUT_SECONDS
    assert calls[0]["payload"]["store"] is False
    assert calls[0]["payload"]["model"] == DEFAULT_OPENAI_LLM_MODEL
    assert calls[0]["payload"]["reasoning"] == {"effort": DEFAULT_OPENAI_REASONING_EFFORT}
    assert calls[0]["headers"]["X-Client-Request-Id"].startswith("aits-llm-precheck-")
    assert report.records[0].client_request_id == calls[0]["headers"]["X-Client-Request-Id"]


def test_openai_claim_precheck_reuses_recent_request_cache(tmp_path: Path) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    cache_dir = tmp_path / "openai_cache"
    calls: list[str] = []

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        calls.append(headers["X-Client-Request-Id"])
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    data_sources = DataSourcesConfig(
        sources=[
            _source(
                source_id="sec_company_facts",
                source_type="primary_source",
                external_llm_allowed=True,
                max_content_sent_level="full_text",
                cache_allowed=True,
            )
        ]
    )
    first_time = datetime(2026, 5, 4, 12, tzinfo=UTC)

    first_report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=data_sources,
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        openai_cache_dir=cache_dir,
        generated_at=first_time,
    )
    second_report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=data_sources,
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        openai_cache_dir=cache_dir,
        generated_at=first_time + timedelta(hours=1),
    )

    assert len(calls) == 1
    assert first_report.records[0].cache_status == "MISS"
    assert first_report.openai_cache_write_count == 1
    assert second_report.records[0].cache_status == "HIT"
    assert second_report.openai_cache_hit_count == 1
    assert second_report.records[0].request_id == calls[0]
    cache_text = Path(first_report.records[0].cache_path).read_text(encoding="utf-8")
    cache_payload = json.loads(cache_text)
    assert cache_payload["provider"] == "openai"
    assert cache_payload["api_family"] == "responses"
    assert cache_payload["request"]["headers"]["Authorization"] == "Bearer ***"
    assert "sk-test" not in cache_text
    assert list((cache_dir / "archive" / "openai" / "responses" / "2026-05-04").glob("*.json"))


def test_openai_claim_precheck_refreshes_expired_request_cache(tmp_path: Path) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    cache_dir = tmp_path / "openai_cache"
    calls: list[str] = []

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        calls.append(headers["X-Client-Request-Id"])
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    data_sources = DataSourcesConfig(
        sources=[
            _source(
                source_id="sec_company_facts",
                source_type="primary_source",
                external_llm_allowed=True,
                max_content_sent_level="full_text",
                cache_allowed=True,
            )
        ]
    )
    first_time = datetime(2026, 5, 4, 12, tzinfo=UTC)

    run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=data_sources,
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        openai_cache_dir=cache_dir,
        openai_cache_ttl_seconds=3600,
        generated_at=first_time,
    )
    expired_report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=data_sources,
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        openai_cache_dir=cache_dir,
        openai_cache_ttl_seconds=3600,
        generated_at=first_time + timedelta(hours=2),
    )

    assert len(calls) == 2
    assert expired_report.records[0].cache_status == "EXPIRED"
    assert expired_report.openai_cache_expired_count == 1
    assert expired_report.openai_cache_write_count == 1


def test_openai_claim_precheck_fails_closed_when_cache_not_allowed(
    tmp_path: Path,
) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    called = False

    def fake_post(
        _url: str,
        _headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        nonlocal called
        called = True
        return _openai_response()

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                    cache_allowed=False,
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        openai_cache_dir=tmp_path / "openai_cache",
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert called is False
    assert report.status == "FAIL"
    assert "llm_precheck_cache_permission_denied" in {issue.code for issue in report.issues}


def test_llm_claim_precheck_writes_pending_review_queue_without_source_text(
    tmp_path: Path,
) -> None:
    source_text = "Official release says export controls now require extra licenses."
    packet = _packet(content_text=source_text, content_sent_level="full_text")
    captured_payload: dict[str, Any] = {}
    captured_timeout: list[float] = []

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        captured_payload.update(dict(payload))
        captured_timeout.append(timeout_seconds)
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )
    queue_path = write_llm_claim_prereview_queue(
        report,
        tmp_path / "llm_claim_prereview_queue.json",
    )
    payload = json.loads(queue_path.read_text(encoding="utf-8"))
    queue_text = queue_path.read_text(encoding="utf-8")

    assert report.status == "PASS_WITH_WARNINGS"
    assert captured_payload["model"] == DEFAULT_OPENAI_LLM_MODEL
    assert captured_payload["reasoning"] == {"effort": DEFAULT_OPENAI_REASONING_EFFORT}
    assert captured_timeout == [DEFAULT_OPENAI_TIMEOUT_SECONDS]
    assert report.records[0].model == DEFAULT_OPENAI_LLM_MODEL
    assert report.records[0].reasoning_effort == DEFAULT_OPENAI_REASONING_EFFORT
    assert report.records[0].source_type == "llm_extracted"
    assert report.records[0].manual_review_status == "pending_review"
    assert report.records[0].automatic_score_eligible is False
    assert report.records[0].position_gate_eligible is False
    assert report.records[0].claims[0].automatic_score_eligible is False
    assert report.records[0].claims[0].position_gate_eligible is False
    assert payload["schema_version"] == "llm_claim_prereview_queue.v2"
    assert payload["claim_count"] == 1
    assert payload["records"][0]["reasoning_effort"] == DEFAULT_OPENAI_REASONING_EFFORT
    assert source_text not in queue_text
    assert "sk-test" not in queue_text


def test_llm_claim_precheck_retries_retryable_http_error_before_success(
    tmp_path: Path,
) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    captured_timeout: list[float] = []

    def fake_post(
        _url: str,
        _headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        captured_timeout.append(timeout_seconds)
        if len(captured_timeout) < 3:
            status_code = 520 if len(captured_timeout) == 1 else 502
            return OpenAIJsonResponse(
                status_code=status_code,
                headers={},
                body={"error": {"message": "bad gateway"}},
            )
        return _openai_response()

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert captured_timeout == [DEFAULT_OPENAI_TIMEOUT_SECONDS] * 3
    assert report.status == "PASS_WITH_WARNINGS"
    assert report.record_count == 1
    retry_issue = next(
        issue for issue in report.issues if issue.code == "openai_responses_api_retry_succeeded"
    )
    attempts = retry_issue.diagnostics["attempts"]
    assert len(attempts) == 3
    assert [attempt["http_client"] for attempt in attempts] == ["custom", "custom", "custom"]
    assert attempts[0]["client_request_id"].startswith("aits-llm-precheck-")
    assert attempts[1]["client_request_id"].endswith("-retry-2")
    assert [attempt["http_status"] for attempt in attempts] == [520, 502, 200]
    assert retry_issue.diagnostics["final_attempt"]["input_checksum_sha256"]


def test_llm_claim_precheck_fails_after_retry_exhaustion(tmp_path: Path) -> None:
    packet = _packet(content_text="Official release says export controls changed.")
    call_count = 0

    def fake_post(
        _url: str,
        _headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        nonlocal call_count
        call_count += 1
        return OpenAIJsonResponse(
            status_code=520,
            headers={},
            body={"error": {"message": "edge timeout"}},
        )

    report = run_openai_claim_precheck(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_company_facts",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        input_path=tmp_path / "input.yaml",
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert call_count == DEFAULT_OPENAI_MAX_RETRIES + 1
    assert report.status == "FAIL"
    assert report.record_count == 0
    error_issue = next(
        issue for issue in report.issues if issue.code == "openai_responses_api_error"
    )
    assert "已重试 2 次" in error_issue.message
    assert error_issue.diagnostics["attempt_count"] == 3
    assert [attempt["http_status"] for attempt in error_issue.diagnostics["attempts"]] == [
        520,
        520,
        520,
    ]
    assert "sk-test" not in json.dumps(error_issue.diagnostics, ensure_ascii=False)


def test_openai_claim_schema_cannot_emit_trade_action_fields() -> None:
    claim_properties = OPENAI_LLM_CLAIM_RESPONSE_FORMAT["schema"]["properties"]["claims"][
        "items"
    ]["properties"]

    assert "recommended_action" not in claim_properties
    assert "position_size" not in claim_properties
    assert claim_properties["manual_review_status"]["const"] == "pending_review"
    assert claim_properties["prohibited_actions_ack"]["const"] is True


def test_load_llm_claim_precheck_input_yaml(tmp_path: Path) -> None:
    input_path = tmp_path / "llm_input.yaml"
    input_path.write_text(
        yaml.safe_dump(_packet().model_dump(mode="json"), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    loaded = load_llm_claim_precheck_input(input_path)

    assert loaded.precheck_id == "precheck:claim:2026-05-04"
    assert loaded.source_id == "sec_company_facts"


def test_llm_precheck_claims_cli_stops_when_permission_denied(tmp_path: Path) -> None:
    input_path = tmp_path / "llm_input.yaml"
    report_path = tmp_path / "llm_report.md"
    queue_path = tmp_path / "llm_queue.json"
    config_path = tmp_path / "data_sources.yaml"
    input_path.write_text(
        yaml.safe_dump(
            _packet(source_id="vendor_news", content_sent_level="full_text").model_dump(
                mode="json"
            ),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="vendor_news",
                source_type="paid_vendor",
                external_llm_allowed=False,
                max_content_sent_level="metadata_only",
            )
        ]
    )
    config_path.write_text(
        yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "llm",
            "precheck-claims",
            "--input-path",
            str(input_path),
            "--data-sources-path",
            str(config_path),
            "--queue-path",
            str(queue_path),
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert report_path.exists()
    assert not queue_path.exists()
    assert "provider 权限未知" not in report_path.read_text(encoding="utf-8")
    assert "external_llm_allowed=false" in report_path.read_text(encoding="utf-8")


def _packet(**overrides: Any) -> LlmClaimPrecheckInput:
    values: dict[str, Any] = {
        "precheck_id": "precheck:claim:2026-05-04",
        "source_id": "sec_company_facts",
        "source_url": "https://www.sec.gov/Archives/example",
        "source_name": "SEC EDGAR",
        "source_title": "Example filing",
        "published_at": date(2026, 5, 4),
        "captured_at": date(2026, 5, 4),
        "content_text": "Official filing says AI infrastructure revenue increased.",
        "content_sent_level": "full_text",
    }
    values.update(overrides)
    return LlmClaimPrecheckInput.model_validate(values)


def _source(
    *,
    source_id: str,
    source_type: str,
    external_llm_allowed: bool,
    max_content_sent_level: str,
    cache_allowed: bool = False,
) -> DataSourceConfig:
    return DataSourceConfig(
        source_id=source_id,
        provider="Test Provider",
        source_type=source_type,
        status="active",
        domains=["news_events"],
        endpoint="https://example.test",
        adapter="test",
        cadence="event_driven",
        audit_fields=[
            "provider",
            "endpoint",
            "request_parameters",
            "downloaded_at",
            "row_count",
            "checksum",
        ],
        validation_checks=["schema"],
        limitations=["test source"],
        llm_permission=DataSourceLlmPermissionConfig(
            license_scope="test_scope",
            personal_use_only=True,
            external_llm_allowed=external_llm_allowed,
            cache_allowed=cache_allowed,
            redistribution_allowed=False,
            max_content_sent_level=max_content_sent_level,
            approval_ref="owner_test_approval" if external_llm_allowed else "not_approved",
            reviewed_at=date(2026, 5, 4),
        ),
    )


def _openai_response(request_id: str = "req_test") -> OpenAIJsonResponse:
    output = {
        "overall_summary_zh": "该来源包含一条待复核风险事件线索。",
        "prohibited_actions_ack": True,
        "claims": [
            {
                "claim_id": "claim:export_control:2026-05-04",
                "claim_text_zh": "公告可能显示 AI 芯片出口许可要求收紧。",
                "source_span_ref": "paragraph:1",
                "affected_tickers": ["NVDA", "AMD"],
                "affected_nodes": ["export_controls"],
                "claim_type": "risk_event",
                "novelty": "new",
                "impact_horizon": "short_term",
                "evidence_grade_suggestion": "B",
                "confidence": 0.72,
                "conflicts_or_uncertainties": ["需要确认公告是否已经生效"],
                "required_review_questions": ["是否为官方公告？"],
                "risk_event_candidate": {
                    "risk_id_candidate": ["ai_chip_export_control_upgrade"],
                    "status_candidate": "active_candidate",
                    "level_candidate": "L2",
                    "severity_candidate": "high",
                    "probability_candidate": "medium",
                    "scope_candidate": "node",
                    "time_sensitivity_candidate": "short_term",
                    "action_class_candidate": "position_gate_candidate",
                    "missing_confirmations": ["人工确认生效日期"],
                    "review_questions": ["是否影响 NVDA/AMD 出口许可？"],
                },
                "thesis_signal_match": [],
                "manual_review_status": "pending_review",
                "prohibited_actions_ack": True,
            }
        ],
    }
    return OpenAIJsonResponse(
        status_code=200,
        headers={"x-request-id": request_id},
        body={
            "id": "resp_test",
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps(output, ensure_ascii=False),
                        }
                    ],
                }
            ],
        },
    )
