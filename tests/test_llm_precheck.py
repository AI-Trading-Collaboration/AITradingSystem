from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    DataSourceConfig,
    DataSourceLlmPermissionConfig,
    DataSourcesConfig,
)
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_LLM_MODEL,
    DEFAULT_OPENAI_REASONING_EFFORT,
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


def test_llm_claim_precheck_writes_pending_review_queue_without_source_text(
    tmp_path: Path,
) -> None:
    source_text = "Official release says export controls now require extra licenses."
    packet = _packet(content_text=source_text, content_sent_level="full_text")
    captured_payload: dict[str, Any] = {}

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        captured_payload.update(dict(payload))
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
            cache_allowed=False,
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
