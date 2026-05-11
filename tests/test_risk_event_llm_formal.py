from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_risk_events
from ai_trading_system.risk_event_llm_formal import (
    build_llm_formal_assessment_report,
    write_llm_formal_assessment_outputs,
)
from ai_trading_system.risk_events import (
    build_risk_event_occurrence_review_report,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
)


def test_llm_formal_assessment_writes_occurrence_and_attestation(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    output_dir = tmp_path / "occurrences"
    _write_queue(queue_path, [_record(status_suggestion="active_candidate")])

    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 5, 10),
        risk_events=load_risk_events(),
    )
    written_paths = write_llm_formal_assessment_outputs(report, output_dir)
    validation = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 10),
    )
    review = build_risk_event_occurrence_review_report(validation)

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.occurrence_count == 1
    assert report.attestation is not None
    assert len(written_paths) == 2
    assert review.score_eligible_active_items[0].source_types == (
        "llm_extracted",
        "primary_source",
    )
    assert review.score_eligible_active_items[0].position_gate_eligible is False
    assert review.has_current_review_attestation is True


def test_apply_llm_formal_assessment_cli_writes_outputs(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    output_dir = tmp_path / "occurrences"
    report_path = tmp_path / "llm_formal.md"
    validation_path = tmp_path / "occurrences.md"
    _write_queue(queue_path, [_record(status_suggestion="watch")])

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "apply-llm-formal-assessment",
            "--queue-path",
            str(queue_path),
            "--output-dir",
            str(output_dir),
            "--output-path",
            str(report_path),
            "--validation-report-path",
            str(validation_path),
            "--as-of",
            "2026-05-10",
        ],
    )

    assert result.exit_code == 0
    assert "LLM 正式风险评估状态：PASS_WITH_WARNINGS" in result.output
    assert report_path.exists()
    assert validation_path.exists()
    assert len(list(output_dir.glob("*.yaml"))) == 2


def test_llm_formal_attestation_with_empty_queue_keeps_current_source_scope(
    tmp_path: Path,
) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    output_dir = tmp_path / "occurrences"
    _write_queue(queue_path, [])

    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 5, 12),
        risk_events=load_risk_events(),
    )
    written_paths = write_llm_formal_assessment_outputs(report, output_dir)
    validation = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 12),
    )

    assert report.status == "PASS"
    assert report.occurrence_count == 0
    assert report.attestation is not None
    assert len(written_paths) == 1
    assert validation.current_review_attestation_count == 1
    assert validation.status == "PASS"


def _write_queue(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "risk_event_prereview_queue.v2",
                "generated_at": "2026-05-10T00:00:00Z",
                "source_kind": "openai_live",
                "source_input_path": "official_policy_candidate_triage_2026-05-10.csv",
                "source_input_checksum_sha256": "a" * 64,
                "row_count": len(records),
                "record_count": len(records),
                "records": records,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _record(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "precheck_id": "precheck:official:export_controls:claim_001",
        "source_url": "https://www.congress.gov/bill/119th-congress/house-bill/8689",
        "source_name": "Congress.gov API",
        "source_title": "Strategic Export Controls and Border Security Enhancement Act",
        "published_at": "2026-05-07",
        "captured_at": "2026-05-10",
        "original_source_type": "primary_source",
        "external_llm_permitted": True,
        "source_type": "llm_extracted",
        "manual_review_status": "pending_review",
        "model": "gpt-5.5",
        "reasoning_effort": "high",
        "prompt_version": "risk_event_prereview_v1",
        "request_id": "req_test",
        "response_id": "resp_test",
        "client_request_id": "client_req_test",
        "request_timestamp": "2026-05-10T00:00:00Z",
        "input_checksum_sha256": "b" * 64,
        "output_checksum_sha256": "c" * 64,
        "source_permission": {"external_llm_allowed": True},
        "matched_risk_ids": ["ai_chip_export_control_upgrade"],
        "status_suggestion": "active_candidate",
        "level_suggestion": "L2",
        "affected_tickers": ["NVDA", "AMD", "TSM", "INTC"],
        "affected_nodes": ["export_controls", "gpu_asic_demand"],
        "evidence_grade_suggestion": "B",
        "confidence": 0.78,
        "uncertainty_reasons": ["需要确认正式生效日期"],
        "human_review_questions": ["是否已生效？"],
        "dedupe_key": "export-controls-2026-05-10",
        "prohibited_actions_ack": True,
        "raw_summary": "LLM 判断该官方来源可能构成 AI 芯片出口限制升级风险。",
        "notes": "unit test",
    }
    values.update(overrides)
    return values
