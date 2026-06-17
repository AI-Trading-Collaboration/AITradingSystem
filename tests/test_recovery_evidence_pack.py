from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.recovery_evidence_pack import (
    PASS_WITH_WARNINGS_STATUS,
    RECOVERY_EVIDENCE_BLOCKED,
    RECOVERY_EVIDENCE_COMPLETE,
    RECOVERY_EVIDENCE_PARTIAL,
    SOURCE_REPORT_SPECS,
    build_recovery_evidence_pack_payload,
    validate_recovery_evidence_pack_payload,
)

RUN_DATE = date(2026, 6, 17)


def test_recovery_evidence_pack_complete_with_visible_source_blockers(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        source_status_overrides={
            "etf_dynamic_v3_cost_sensitivity_review": "NOT_MEANINGFUL_UNDER_COSTS",
            "etf_dynamic_v3_benchmark_baseline_control": (
                "CANDIDATE_UNDERPERFORMS_BASELINES"
            ),
        },
    )

    payload = build_recovery_evidence_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_recovery_evidence_pack_payload(payload)

    assert payload["recovery_evidence_status"] == RECOVERY_EVIDENCE_COMPLETE
    assert payload["summary"]["source_report_count"] == len(SOURCE_REPORT_SPECS)
    assert payload["summary"]["remaining_recovery_blocker_count"] == 2
    assert {
        blocker["source_id"] for blocker in payload["remaining_recovery_blockers"]
    } == {"cost_sensitivity_review", "benchmark_baseline_control"}
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_evidence_pack_blocks_missing_required_source(
    tmp_path: Path,
) -> None:
    payload = build_recovery_evidence_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            omit={"etf_dynamic_v3_paper_shadow_health"},
        ),
        project_root=tmp_path,
    )
    validation = validate_recovery_evidence_pack_payload(payload)

    assert payload["recovery_evidence_status"] == RECOVERY_EVIDENCE_BLOCKED
    assert any(
        issue["source_id"] == "paper_shadow_health"
        for issue in payload["structural_blockers"]
    )
    assert validation["validation_status"] == "FAIL"


def test_recovery_evidence_pack_partial_when_validation_missing(
    tmp_path: Path,
) -> None:
    payload = build_recovery_evidence_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            omit_validation={"etf_dynamic_v3_benchmark_baseline_control"},
        ),
        project_root=tmp_path,
    )
    validation = validate_recovery_evidence_pack_payload(payload)

    assert payload["recovery_evidence_status"] == RECOVERY_EVIDENCE_PARTIAL
    assert payload["summary"]["partial_reason_count"] == 1
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_evidence_pack_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-06-17.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(
        json.dumps(report_index, ensure_ascii=False),
        encoding="utf-8",
    )
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "recovery-evidence-pack",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-recovery-evidence-pack",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "recovery_evidence_pack_2026-06-17.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (reports_dir / "recovery_evidence_pack_validation_2026-06-17.json").read_text(
            encoding="utf-8"
        )
    )
    assert report_payload["recovery_evidence_status"] == RECOVERY_EVIDENCE_COMPLETE
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"]["recovery_evidence_pack"].endswith(
        "recovery_evidence_pack_2026-06-17.json"
    )


def _report_index_payload(
    tmp_path: Path,
    *,
    source_status_overrides: dict[str, str] | None = None,
    omit: set[str] | None = None,
    omit_validation: set[str] | None = None,
) -> dict[str, object]:
    source_status_overrides = source_status_overrides or {}
    omit = omit or set()
    omit_validation = omit_validation or set()
    reports: list[dict[str, object]] = []
    for spec in SOURCE_REPORT_SPECS:
        report_id = str(spec["report_id"])
        validation_report_id = str(spec.get("validation_report_id") or report_id)
        if report_id in omit:
            continue
        validation_path: Path | None = None
        source_path = _source_path(tmp_path, report_id, str(spec["preferred_json_names"][0]))
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_payload = _source_payload(
            report_id,
            source_status_overrides.get(report_id, str(spec["pass_statuses"][0])),
        )
        source_path.write_text(
            json.dumps(source_payload, ensure_ascii=False),
            encoding="utf-8",
        )
        if report_id not in omit_validation and validation_report_id not in omit_validation:
            validation_path = _source_path(
                tmp_path,
                validation_report_id,
                str(spec["validation_json_names"][0]),
            )
            validation_path.parent.mkdir(parents=True, exist_ok=True)
            validation_path.write_text(
                json.dumps(
                    _validation_payload(validation_report_id),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        reports.append(_index_entry(report_id, source_path))
        if validation_report_id != report_id and validation_path is not None:
            reports.append(_index_entry(validation_report_id, validation_path))
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "summary": {
            "report_count": len(reports),
            "missing_count": 0,
            "stale_count": 0,
            "required_missing_count": 0,
            "unwaived_warning_count": 0,
            "explicit_waiver_count": 0,
        },
        "reports": reports,
        "production_effect": "none",
    }


def _source_path(tmp_path: Path, report_id: str, name: str) -> Path:
    return tmp_path / "sources" / report_id / name


def _index_entry(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "artifact_status": "AVAILABLE",
        "freshness_status": "FRESH",
        "production_effect": "none",
    }


def _source_payload(report_id: str, status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": report_id,
        "status": status,
        "candidate": "median_plus_regime_mismatch_filter",
        "next_required_action": "review_recovery_source",
        "production_effect": "none",
        "summary": {"status": status},
    }


def _validation_payload(report_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": f"{report_id}_validation",
        "validation_status": "PASS",
        "status": "PASS",
        "production_effect": "none",
        "summary": {"failed_check_count": 0},
    }
