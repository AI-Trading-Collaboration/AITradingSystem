from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
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
    quarantine_unknown_risk_event_occurrence,
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


def test_llm_formal_unknown_risk_id_fails_before_any_output(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    output_dir = tmp_path / "occurrences"
    _write_queue(
        queue_path,
        [_record(matched_risk_ids=["unreviewed_llm_candidate"])],
    )

    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=load_risk_events(),
    )

    assert report.status == "FAIL"
    assert report.occurrence_count == 0
    assert report.attestation is None
    assert "llm_formal_assessment_unknown_risk_id" in {issue.code for issue in report.issues}
    with pytest.raises(ValueError, match="report has errors"):
        write_llm_formal_assessment_outputs(report, output_dir)
    assert not output_dir.exists()


def test_llm_formal_mixed_ids_selects_reviewed_id(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    _write_queue(
        queue_path,
        [
            _record(
                matched_risk_ids=[
                    "unreviewed_llm_candidate",
                    "ai_chip_export_control_upgrade",
                ]
            )
        ],
    )

    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=load_risk_events(),
    )

    assert report.passed is True
    assert report.occurrence_count == 1
    assert report.occurrences[0].event_id == "ai_chip_export_control_upgrade"


def test_llm_formal_requires_reviewed_risk_config(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    _write_queue(queue_path, [_record()])

    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=None,
    )

    assert report.status == "FAIL"
    assert report.occurrence_count == 0
    assert report.attestation is None
    assert "llm_formal_assessment_risk_config_required" in {issue.code for issue in report.issues}


def test_quarantine_unknown_occurrence_preserves_bytes_and_is_idempotent(
    tmp_path: Path,
) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    occurrence_root = tmp_path / "occurrences"
    occurrence_root.mkdir()
    _write_queue(queue_path, [_record()])
    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=load_risk_events(),
    )
    payload = report.occurrences[0].model_dump(mode="json", exclude_none=False)
    payload["event_id"] = "unreviewed_llm_candidate"
    source = occurrence_root / "invalid_unknown_occurrence.yaml"
    source.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    original_bytes = source.read_bytes()
    quarantined_at = datetime(2026, 9, 1, 1, 0, tzinfo=UTC)

    result = quarantine_unknown_risk_event_occurrence(
        source,
        occurrence_root=occurrence_root,
        risk_events=load_risk_events(),
        authorization_id="owner_instruction:2026-09-01:fix_then_rerun",
        quarantined_at=quarantined_at,
    )
    reused = quarantine_unknown_risk_event_occurrence(
        source,
        occurrence_root=occurrence_root,
        risk_events=load_risk_events(),
        authorization_id="owner_instruction:2026-09-01:fix_then_rerun",
    )

    assert not source.exists()
    assert result.quarantined_path.read_bytes() == original_bytes
    assert result.source_sha256 == reused.source_sha256
    assert reused.idempotent_reuse is True
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    assert receipt["reason_code"] == "UNKNOWN_RISK_EVENT_ID"
    assert receipt["active_store_excluded"] is True
    assert receipt["source_bytes_preserved"] is True
    assert receipt["production_effect"] == "none"
    assert load_risk_event_occurrence_store(occurrence_root).loaded == ()

    result.quarantined_path.write_bytes(original_bytes + b"\n# tampered\n")
    with pytest.raises(ValueError, match="size mismatch"):
        quarantine_unknown_risk_event_occurrence(
            source,
            occurrence_root=occurrence_root,
            risk_events=load_risk_events(),
            authorization_id="owner_instruction:2026-09-01:fix_then_rerun",
        )


def test_quarantine_refuses_known_or_nested_occurrence(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    occurrence_root = tmp_path / "occurrences"
    occurrence_root.mkdir()
    _write_queue(queue_path, [_record()])
    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=load_risk_events(),
    )
    payload = report.occurrences[0].model_dump(mode="json", exclude_none=False)
    known_source = occurrence_root / "known.yaml"
    known_source.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    nested = occurrence_root / "nested"
    nested.mkdir()
    nested_source = nested / "unknown.yaml"
    payload["event_id"] = "unreviewed_llm_candidate"
    nested_source.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="configured risk event"):
        quarantine_unknown_risk_event_occurrence(
            known_source,
            occurrence_root=occurrence_root,
            risk_events=load_risk_events(),
            authorization_id="owner_instruction:test",
        )
    with pytest.raises(ValueError, match="direct child"):
        quarantine_unknown_risk_event_occurrence(
            nested_source,
            occurrence_root=occurrence_root,
            risk_events=load_risk_events(),
            authorization_id="owner_instruction:test",
        )


def test_quarantine_unknown_occurrence_cli_validates_active_store(tmp_path: Path) -> None:
    queue_path = tmp_path / "risk_event_prereview_queue.json"
    occurrence_root = tmp_path / "occurrences"
    validation_path = tmp_path / "risk_event_occurrences.md"
    occurrence_root.mkdir()
    _write_queue(queue_path, [_record()])
    report = build_llm_formal_assessment_report(
        queue_path,
        as_of=date(2026, 8, 31),
        risk_events=load_risk_events(),
    )
    payload = report.occurrences[0].model_dump(mode="json", exclude_none=False)
    payload["event_id"] = "unreviewed_llm_candidate"
    source = occurrence_root / "invalid_unknown_occurrence.yaml"
    source.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "quarantine-unknown-occurrence",
            "--occurrence-path",
            str(source),
            "--authorization-id",
            "owner_instruction:2026-09-01:fix_then_rerun",
            "--as-of",
            "2026-08-31",
            "--input-dir",
            str(occurrence_root),
            "--validation-report-path",
            str(validation_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "已受治理隔离" in result.output
    assert validation_path.exists()
    assert (occurrence_root / "quarantine" / source.name).exists()


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
