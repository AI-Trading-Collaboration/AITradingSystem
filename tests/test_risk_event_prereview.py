from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.main import get_command
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    DataSourceConfig,
    DataSourceLlmPermissionConfig,
    DataSourcesConfig,
    load_risk_events,
)
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_LLM_MODEL,
    DEFAULT_OPENAI_MAX_RETRIES,
    DEFAULT_OPENAI_REASONING_EFFORT,
    DEFAULT_OPENAI_TIMEOUT_SECONDS,
    LlmClaimPrecheckInput,
    OpenAIJsonResponse,
)
from ai_trading_system.official_policy_sources import (
    OfficialPolicyCandidate,
    OfficialPolicySourceFetchReport,
    write_official_policy_candidates_csv,
)
from ai_trading_system.risk_event_candidate_triage import (
    triage_official_policy_candidates,
    write_risk_event_candidate_triage_csv,
)
from ai_trading_system.risk_event_prereview import (
    OPENAI_RISK_EVENT_PREREVIEW_SCHEMA,
    RiskEventPreReviewImportReport,
    import_risk_event_prereview_csv,
    render_risk_event_prereview_import_report,
    run_openai_risk_event_prereview,
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_queue,
)


def test_import_risk_event_prereview_csv_writes_pending_llm_queue(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "openai_prereview.csv"
    _write_csv(input_path, [_row()])

    report = import_risk_event_prereview_csv(
        input_path,
        risk_events=load_risk_events(),
    )
    markdown = render_risk_event_prereview_import_report(report)
    queue_path = write_risk_event_prereview_queue(
        report,
        tmp_path / "risk_event_prereview_queue.json",
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )
    payload = json.loads(queue_path.read_text(encoding="utf-8"))

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.record_count == 1
    assert report.pending_review_count == 1
    assert report.records[0].source_type == "llm_extracted"
    assert report.records[0].manual_review_status == "pending_review"
    assert report.records[0].automatic_score_eligible is False
    assert report.records[0].position_gate_eligible is False
    assert "high_impact_prereview_requires_human_confirmation" in {
        issue.code for issue in report.issues
    }
    assert "不得评分/不得触发仓位闸门" in markdown
    assert payload["schema_version"] == "risk_event_prereview_queue.v2"
    assert payload["record_count"] == 1
    assert payload["records"][0]["source_type"] == "llm_extracted"
    assert payload["records"][0]["reasoning_effort"] == DEFAULT_OPENAI_REASONING_EFFORT


def test_import_risk_event_prereview_csv_rejects_confirmed_or_non_llm_output(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "openai_prereview.csv"
    _write_csv(
        input_path,
        [
            _row(
                source_type="manual_input",
                manual_review_status="confirmed",
            )
        ],
    )

    report = import_risk_event_prereview_csv(input_path)

    assert report.passed is False
    assert report.records == ()
    assert "risk_event_prereview_row_invalid" in {issue.code for issue in report.issues}


def test_import_risk_event_prereview_csv_rejects_paid_vendor_without_permission(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "openai_prereview.csv"
    _write_csv(
        input_path,
        [_row(original_source_type="paid_vendor", external_llm_permitted="false")],
    )

    report = import_risk_event_prereview_csv(input_path)

    assert report.passed is False
    assert "risk_event_prereview_row_invalid" in {issue.code for issue in report.issues}


def test_write_risk_event_prereview_queue_refuses_failed_import(tmp_path: Path) -> None:
    input_path = tmp_path / "openai_prereview.csv"
    _write_csv(input_path, [_row(prohibited_actions_ack="false")])
    report = import_risk_event_prereview_csv(input_path)

    with pytest.raises(ValueError, match="预审导入存在错误"):
        write_risk_event_prereview_queue(report, tmp_path / "queue.json")


def test_risk_events_import_prereview_csv_cli_writes_queue_and_report(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "openai_prereview.csv"
    output_path = tmp_path / "risk_event_prereview_import.md"
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    _write_csv(input_path, [_row()])

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "import-prereview-csv",
            "--input-path",
            str(input_path),
            "--queue-path",
            str(queue_path),
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "风险事件 OpenAI 预审导入状态：PASS_WITH_WARNINGS" in result.output
    assert output_path.exists()
    assert queue_path.exists()


def test_run_openai_risk_event_prereview_writes_pending_queue_without_source_text(
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

    report = run_openai_risk_event_prereview(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_policy",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "risk_input.yaml",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )
    queue_path = write_risk_event_prereview_queue(
        report,
        tmp_path / "risk_event_prereview_queue.json",
    )
    payload = json.loads(queue_path.read_text(encoding="utf-8"))
    queue_text = queue_path.read_text(encoding="utf-8")

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.source_kind == "openai_live"
    assert report.record_count == 1
    assert captured_payload["model"] == DEFAULT_OPENAI_LLM_MODEL
    assert captured_payload["reasoning"] == {"effort": DEFAULT_OPENAI_REASONING_EFFORT}
    assert captured_timeout == [DEFAULT_OPENAI_TIMEOUT_SECONDS]
    assert report.records[0].source_type == "llm_extracted"
    assert report.records[0].manual_review_status == "pending_review"
    assert report.records[0].model == DEFAULT_OPENAI_LLM_MODEL
    assert report.records[0].reasoning_effort == DEFAULT_OPENAI_REASONING_EFFORT
    assert report.records[0].automatic_score_eligible is False
    assert report.records[0].position_gate_eligible is False
    assert report.records[0].source_permission["external_llm_allowed"] is True
    assert payload["source_kind"] == "openai_live"
    assert payload["record_count"] == 1
    assert payload["records"][0]["response_id"] == "resp_test"
    assert source_text not in queue_text
    assert "sk-test" not in queue_text


def test_openai_prereview_allows_production_request_after_as_of_with_cutoff(
    tmp_path: Path,
) -> None:
    request_time = datetime(2026, 5, 12, 4, 10, tzinfo=UTC)
    packet = _packet(
        published_at=datetime(2026, 5, 11, tzinfo=UTC).date(),
        captured_at=datetime(2026, 5, 11, tzinfo=UTC).date(),
    )

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    report = run_openai_risk_event_prereview(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_policy",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "risk_input.yaml",
        as_of=datetime(2026, 5, 11, tzinfo=UTC).date(),
        generated_at=request_time,
        request_visibility_cutoff=request_time,
        http_post_json=fake_post,
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.record_count == 1
    assert "risk_event_prereview_request_in_future" not in {
        issue.code for issue in report.issues
    }


def test_openai_prereview_rejects_request_after_as_of_without_cutoff(
    tmp_path: Path,
) -> None:
    packet = _packet(
        published_at=datetime(2026, 5, 11, tzinfo=UTC).date(),
        captured_at=datetime(2026, 5, 11, tzinfo=UTC).date(),
    )

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    report = run_openai_risk_event_prereview(
        packet,
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_policy",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="full_text",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "risk_input.yaml",
        as_of=datetime(2026, 5, 11, tzinfo=UTC).date(),
        generated_at=datetime(2026, 5, 12, 4, 10, tzinfo=UTC),
        http_post_json=fake_post,
    )

    assert report.passed is False
    assert report.records == ()
    assert "risk_event_prereview_request_in_future" in {
        issue.code for issue in report.issues
    }


def test_risk_events_precheck_openai_cli_fails_closed_without_permission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "risk_input.yaml"
    report_path = tmp_path / "risk_event_prereview_openai.md"
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    config_path = tmp_path / "data_sources.yaml"
    input_path.write_text(
        yaml.safe_dump(
            _packet(content_sent_level="full_text").model_dump(mode="json"),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="official_policy",
                source_type="primary_source",
                external_llm_allowed=False,
                max_content_sent_level="full_text",
            )
        ]
    )
    config_path.write_text(
        yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "precheck-openai",
            "--input-path",
            str(input_path),
            "--data-sources-path",
            str(config_path),
            "--queue-path",
            str(queue_path),
            "--output-path",
            str(report_path),
            "--as-of",
            "2026-05-04",
        ],
    )

    assert result.exit_code == 1
    assert report_path.exists()
    assert not queue_path.exists()
    assert "external_llm_allowed=false" in report_path.read_text(encoding="utf-8")


def test_openai_prereview_schema_keeps_output_pending_review_only() -> None:
    properties = OPENAI_RISK_EVENT_PREREVIEW_SCHEMA["schema"]["properties"]

    assert properties["source_type"]["const"] == "llm_extracted"
    assert properties["manual_review_status"]["const"] == "pending_review"
    assert "reasoning_effort" in properties
    assert properties["prohibited_actions_ack"]["const"] is True


def test_official_candidates_auto_precheck_writes_only_pending_review(
    tmp_path: Path,
) -> None:
    captured_payloads: list[dict[str, Any]] = []
    captured_timeout: list[float] = []

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        captured_payloads.append(dict(payload))
        captured_timeout.append(timeout_seconds)
        return _openai_response(request_id=headers["X-Client-Request-Id"])

    report = run_openai_risk_event_prereview_for_official_candidates(
        (_official_candidate(tmp_path),),
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_bis_federal_register_notices",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="metadata_only",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )
    queue_path = write_risk_event_prereview_queue(report, tmp_path / "queue.json")
    payload = json.loads(queue_path.read_text(encoding="utf-8"))
    request_payload = json.loads(captured_payloads[0]["input"][1]["content"])

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.record_count == 1
    assert report.records[0].source_type == "llm_extracted"
    assert report.records[0].manual_review_status == "pending_review"
    assert report.records[0].automatic_score_eligible is False
    assert report.records[0].position_gate_eligible is False
    assert payload["record_count"] == 1
    assert request_payload["content_sent_level"] == "metadata_only"
    assert captured_timeout == [DEFAULT_OPENAI_TIMEOUT_SECONDS]
    assert "sk-test" not in queue_path.read_text(encoding="utf-8")


def test_official_candidates_auto_precheck_prioritizes_ai_policy_candidates(
    tmp_path: Path,
) -> None:
    captured_payloads: list[dict[str, Any]] = []

    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        captured_payloads.append(dict(payload))
        return _openai_irrelevant_response(request_id=headers["X-Client-Request-Id"])

    low_priority = replace(
        _official_candidate(tmp_path),
        candidate_id="aaa:congress:china-only",
        source_id="official_congress_bills",
        provider="Congress.gov API",
        source_name="Congress.gov API",
        source_title="Resolution about China economic influence",
        matched_topics=("china_technology",),
        affected_tickers=(),
        affected_nodes=(),
    )
    high_priority = replace(
        _official_candidate(tmp_path),
        candidate_id="zzz:bis:export-controls",
        source_id="official_bis_federal_register_notices",
        provider="Federal Register API / BIS notices",
        source_name="Federal Register API / BIS notices",
        source_title="BIS advanced computing export controls update",
        matched_topics=("export_controls", "ai_policy"),
        affected_tickers=("NVDA", "AMD"),
        affected_nodes=("export_controls",),
    )

    report = run_openai_risk_event_prereview_for_official_candidates(
        (low_priority, high_priority),
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_bis_federal_register_notices",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="metadata_only",
                ),
                _source(
                    source_id="official_congress_bills",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="metadata_only",
                ),
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
        max_candidates=1,
    )
    request_payload = json.loads(captured_payloads[0]["input"][1]["content"])

    assert report.status == "PASS_WITH_WARNINGS"
    assert request_payload["source_name"] == "Federal Register API / BIS notices"
    assert "BIS advanced computing export controls" in request_payload["source_title"]
    assert any(
        issue.code == "risk_event_prereview_candidate_limit_applied"
        for issue in report.issues
    )


def test_official_candidates_irrelevant_output_does_not_add_review_queue(
    tmp_path: Path,
) -> None:
    def fake_post(
        _url: str,
        headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        return _openai_irrelevant_response(request_id=headers["X-Client-Request-Id"])

    report = run_openai_risk_event_prereview_for_official_candidates(
        (_official_candidate(tmp_path),),
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_bis_federal_register_notices",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="metadata_only",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        http_post_json=fake_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )
    queue_path = write_risk_event_prereview_queue(report, tmp_path / "queue.json")
    payload = json.loads(queue_path.read_text(encoding="utf-8"))

    assert report.status == "PASS"
    assert report.record_count == 0
    assert report.pending_review_count == 0
    assert payload["record_count"] == 0


def test_precheck_triaged_official_candidates_cli_filters_high_priority_bucket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    triage_path = tmp_path / "official_policy_candidate_triage_2026-05-10.csv"
    report_path = tmp_path / "risk_event_prereview_triaged_openai_2026-05-10.md"
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    high_candidate = _official_candidate(tmp_path)
    low_candidate = replace(
        high_candidate,
        candidate_id="official:ofac:bank",
        source_id="official_ofac_sdn_xml",
        provider="OFAC Sanctions List Service",
        source_name="OFAC Sanctions List Service",
        source_title="VTB BANK PJSC",
        matched_topics=("sanctions", "russia_geopolitics"),
        matched_risk_ids=("ai_chip_export_control_upgrade",),
        affected_tickers=("NVDA", "AMD"),
        affected_nodes=("export_controls",),
    )
    official_report = OfficialPolicySourceFetchReport(
        as_of=datetime(2026, 5, 10, tzinfo=UTC).date(),
        since=datetime(2026, 5, 7, tzinfo=UTC).date(),
        generated_at=datetime(2026, 5, 10, tzinfo=UTC),
        raw_dir=tmp_path / "raw",
        processed_dir=tmp_path,
        payloads=(),
        candidates=(high_candidate, low_candidate),
    )
    write_official_policy_candidates_csv(official_report, candidate_path)
    triage_report = triage_official_policy_candidates(
        candidate_path,
        as_of=datetime(2026, 5, 10, tzinfo=UTC).date(),
    )
    write_risk_event_candidate_triage_csv(triage_report, triage_path)
    captured_candidate_ids: list[str] = []

    def fake_run(
        candidates: tuple[OfficialPolicyCandidate, ...],
        **kwargs: Any,
    ) -> RiskEventPreReviewImportReport:
        captured_candidate_ids.extend(candidate.candidate_id for candidate in candidates)
        return RiskEventPreReviewImportReport(
            input_path=Path(kwargs["input_path"]),
            row_count=len(candidates),
            checksum_sha256="a" * 64,
            records=(),
            source_kind="openai_live",
            issues=(),
        )

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(
        "ai_trading_system.cli.run_openai_risk_event_prereview_for_official_candidates",
        fake_run,
    )

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "precheck-triaged-official-candidates",
            "--candidate-input-path",
            str(candidate_path),
            "--triage-input-path",
            str(triage_path),
            "--queue-path",
            str(queue_path),
            "--output-path",
            str(report_path),
            "--as-of",
            "2026-05-10",
        ],
    )

    assert result.exit_code == 0
    assert captured_candidate_ids == [high_candidate.candidate_id]
    assert "高优先级官方候选 OpenAI 预审状态：PASS" in result.output
    assert report_path.exists()
    assert queue_path.exists()


def test_official_candidates_auto_precheck_reports_openai_timeout(
    tmp_path: Path,
) -> None:
    call_count = 0

    def timeout_post(
        _url: str,
        _headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        nonlocal call_count
        call_count += 1
        raise TimeoutError("read timed out")

    first_candidate = _official_candidate(tmp_path)
    second_candidate = replace(first_candidate, candidate_id=f"{first_candidate.candidate_id}:2")
    report = run_openai_risk_event_prereview_for_official_candidates(
        (first_candidate, second_candidate),
        api_key="sk-test",
        data_sources=DataSourcesConfig(
            sources=[
                _source(
                    source_id="official_bis_federal_register_notices",
                    source_type="primary_source",
                    external_llm_allowed=True,
                    max_content_sent_level="metadata_only",
                )
            ]
        ),
        risk_events=load_risk_events(),
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        http_post_json=timeout_post,
        generated_at=datetime(2026, 5, 4, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert report.record_count == 0
    assert call_count == DEFAULT_OPENAI_MAX_RETRIES + 1
    error_issue = next(
        issue for issue in report.issues if issue.code == "openai_responses_api_request_failed"
    )
    assert error_issue.diagnostics["attempt_count"] == 3
    assert [
        attempt["exception_type"] for attempt in error_issue.diagnostics["attempts"]
    ] == ["TimeoutError", "TimeoutError", "TimeoutError"]
    assert [
        attempt["http_client"] for attempt in error_issue.diagnostics["attempts"]
    ] == ["custom", "custom", "custom"]
    markdown = render_risk_event_prereview_import_report(report)
    assert "## 请求诊断" in markdown
    assert "Client request id" in markdown
    assert "custom" in markdown
    assert "sk-test" not in markdown


def test_score_daily_help_exposes_risk_event_openai_precheck_option() -> None:
    result = CliRunner().invoke(
        app,
        ["score-daily", "--help"],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0
    assert "OpenAI 预审" in result.output
    score_command = get_command(app).commands["score-daily"]
    option_strings = {
        option
        for param in score_command.params
        for option in [*getattr(param, "opts", []), *getattr(param, "secondary_opts", [])]
    }
    assert "--skip-risk-event-openai-precheck" in option_strings
    assert "--openai-http-client" in option_strings


def _write_csv(input_path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
        "precheck_id",
        "source_url",
        "source_name",
        "captured_at",
        "model",
        "reasoning_effort",
        "prompt_version",
        "request_id",
        "request_timestamp",
        "input_checksum_sha256",
        "output_checksum_sha256",
        "status_suggestion",
        "level_suggestion",
        "raw_summary",
        "human_review_questions",
        "prohibited_actions_ack",
        "source_title",
        "published_at",
        "original_source_type",
        "external_llm_permitted",
        "source_type",
        "manual_review_status",
        "matched_risk_ids",
        "affected_tickers",
        "affected_nodes",
        "evidence_grade_suggestion",
        "confidence",
        "uncertainty_reasons",
        "dedupe_key",
        "notes",
    ]
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(_csv_cell(row.get(column, "")) for column in columns))
    input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _row(**overrides: str) -> dict[str, str]:
    values = {
        "precheck_id": "precheck:export_control:2026-05-04",
        "source_url": "https://example.test/policy-release",
        "source_name": "BIS press release",
        "captured_at": "2026-05-04",
        "model": DEFAULT_OPENAI_LLM_MODEL,
        "reasoning_effort": DEFAULT_OPENAI_REASONING_EFFORT,
        "prompt_version": "risk_event_prereview_v1",
        "request_id": "resp_test_123",
        "request_timestamp": "2026-05-04T01:00:00Z",
        "input_checksum_sha256": "a" * 64,
        "output_checksum_sha256": "b" * 64,
        "status_suggestion": "active_candidate",
        "level_suggestion": "L2",
        "raw_summary": "模型预审认为该公告可能影响 AI 芯片出口限制。",
        "human_review_questions": "是否为官方一手公告;是否影响 NVDA/AMD 出口许可",
        "prohibited_actions_ack": "true",
        "source_title": "Policy release",
        "published_at": "2026-05-04",
        "original_source_type": "primary_source",
        "external_llm_permitted": "true",
        "source_type": "llm_extracted",
        "manual_review_status": "pending_review",
        "matched_risk_ids": "ai_chip_export_control_upgrade",
        "affected_tickers": "NVDA;AMD",
        "affected_nodes": "export_controls",
        "evidence_grade_suggestion": "B",
        "confidence": "0.73",
        "uncertainty_reasons": "需要确认公告是否已经生效",
        "dedupe_key": "policy-release-2026-05-04",
        "notes": "unit test",
    }
    values.update(overrides)
    return values


def _csv_cell(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _packet(**overrides: Any) -> LlmClaimPrecheckInput:
    values: dict[str, Any] = {
        "precheck_id": "precheck:risk:2026-05-04",
        "source_id": "official_policy",
        "source_url": "https://example.test/policy-release",
        "source_name": "Official policy source",
        "source_title": "Policy release",
        "published_at": datetime(2026, 5, 4, tzinfo=UTC).date(),
        "captured_at": datetime(2026, 5, 4, tzinfo=UTC).date(),
        "content_text": "Official policy release excerpt.",
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
        domains=["news_events", "risk_events"],
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
            reviewed_at=datetime(2026, 5, 4, tzinfo=UTC).date(),
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


def _openai_irrelevant_response(request_id: str = "req_test") -> OpenAIJsonResponse:
    output = {
        "overall_summary_zh": "该来源没有可复核风险事件。",
        "prohibited_actions_ack": True,
        "claims": [
            {
                "claim_id": "claim:irrelevant:2026-05-04",
                "claim_text_zh": "该公告不构成 AI 投资相关政策或地缘风险事件。",
                "source_span_ref": "metadata:title",
                "affected_tickers": [],
                "affected_nodes": [],
                "claim_type": "risk_event",
                "novelty": "duplicate",
                "impact_horizon": "unclear",
                "evidence_grade_suggestion": "C",
                "confidence": 0.80,
                "conflicts_or_uncertainties": [],
                "required_review_questions": ["无需人工复核。"],
                "risk_event_candidate": {
                    "risk_id_candidate": ["ai_chip_export_control_upgrade"],
                    "status_candidate": "irrelevant",
                    "level_candidate": "none",
                    "severity_candidate": "none",
                    "probability_candidate": "none",
                    "scope_candidate": "none",
                    "time_sensitivity_candidate": "none",
                    "action_class_candidate": "none",
                    "missing_confirmations": [],
                    "review_questions": [],
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
            "id": "resp_irrelevant",
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


def _official_candidate(tmp_path: Path) -> OfficialPolicyCandidate:
    return OfficialPolicyCandidate(
        candidate_id="official:official_bis_federal_register_notices:test",
        as_of=datetime(2026, 5, 4, tzinfo=UTC).date(),
        source_id="official_bis_federal_register_notices",
        provider="Federal Register API / BIS notices",
        source_type="primary_source",
        source_name="Federal Register API / BIS notices",
        source_url="https://www.federalregister.gov/d/2026-00001",
        source_title="BIS updates Entity List rules for advanced computing chips",
        published_at=datetime(2026, 5, 4, tzinfo=UTC).date(),
        captured_at=datetime(2026, 5, 4, tzinfo=UTC).date(),
        matched_topics=("export_controls",),
        matched_risk_ids=("ai_chip_export_control_upgrade",),
        affected_tickers=("NVDA", "AMD"),
        affected_nodes=("export_controls",),
        evidence_grade_floor="A",
        review_status="pending_review",
        review_questions=("是否影响 NVDA/AMD 出口许可？",),
        raw_payload_path=tmp_path / "raw.json",
        raw_payload_sha256="c" * 64,
        row_count=1,
    )
