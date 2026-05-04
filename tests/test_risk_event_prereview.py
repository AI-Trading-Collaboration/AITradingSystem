from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_risk_events
from ai_trading_system.risk_event_prereview import (
    OPENAI_RISK_EVENT_PREREVIEW_SCHEMA,
    import_risk_event_prereview_csv,
    render_risk_event_prereview_import_report,
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
    assert payload["schema_version"] == "risk_event_prereview_queue.v1"
    assert payload["record_count"] == 1
    assert payload["records"][0]["source_type"] == "llm_extracted"


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


def test_openai_prereview_schema_keeps_output_pending_review_only() -> None:
    properties = OPENAI_RISK_EVENT_PREREVIEW_SCHEMA["schema"]["properties"]

    assert properties["source_type"]["const"] == "llm_extracted"
    assert properties["manual_review_status"]["const"] == "pending_review"
    assert properties["prohibited_actions_ack"]["const"] is True


def _write_csv(input_path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
        "precheck_id",
        "source_url",
        "source_name",
        "captured_at",
        "model",
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
        "model": "gpt-5.2",
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
