from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.research_governance_recovery_pack import (
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    RECOVERY_GOVERNANCE_BLOCKED,
    RECOVERY_GOVERNANCE_HEALTHY,
    RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS,
    RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED,
    SOURCE_REPORT_SPECS,
    build_research_governance_recovery_pack_payload,
    validate_research_governance_recovery_pack_payload,
)

RUN_DATE = date(2026, 6, 17)


def test_research_governance_recovery_pack_healthy(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_HEALTHY
    assert payload["summary"]["source_report_count"] == len(SOURCE_REPORT_SPECS)
    assert payload["summary"]["remaining_blocker_count"] == 0
    assert payload["normal_paper_shadow_boundary"]["normal_paper_shadow_may_resume"] is True
    assert payload["extended_shadow_boundary"]["extended_shadow_remains_forbidden"] is False
    assert payload["live_trading_boundary"]["live_trading_remains_forbidden"] is True
    assert validation["validation_status"] == PASS_STATUS


def test_research_governance_recovery_pack_blocks_real_recovery_markers(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        payload_overrides={
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
                "next_required_action": (
                    "return_candidate_to_research_until_net_improvement_survives_costs"
                ),
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "CANDIDATE_UNDERPERFORMS_BASELINES",
            },
            "research_monthly_review_pack": {
                "monthly_review_status": "MONTHLY_REVIEW_BLOCKED",
            },
            "paper_shadow_promotion_board": {
                "board_decision": "HOLD_FOR_MORE_DATA",
            },
            "extended_shadow_observation_clock": {
                "observation_clock_status": "OBSERVATION_PERIOD_UNMET",
            },
            "extended_shadow_protocol": {
                "eligibility_status": "EXTENDED_SHADOW_BLOCKED",
            },
            "research_roadmap_dashboard": {
                "dashboard_status": "ROADMAP_BLOCKED",
            },
            "decision_snapshot_lifecycle_policy": {
                "snapshot_lifecycle_status": "SNAPSHOT_MISSING_BLOCKING",
            },
        },
        validation_overrides={
            "research_monthly_review_pack_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
            "paper_shadow_promotion_board_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
            "extended_shadow_protocol_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
            "research_roadmap_dashboard_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
            "decision_snapshot_lifecycle_policy_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
        },
    )

    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_BLOCKED
    assert payload["summary"]["remaining_blocker_count"] >= 8
    assert payload["normal_paper_shadow_boundary"]["normal_paper_shadow_may_resume"] is False
    assert payload["extended_shadow_boundary"]["extended_shadow_remains_forbidden"] is True
    assert payload["live_trading_boundary"]["live_trading_remains_forbidden"] is True
    assert {
        blocker["source_id"] for blocker in payload["remaining_blockers"]
    }.issuperset(
        {
            "cost_sensitivity_metrics",
            "benchmark_baseline_metrics",
            "monthly_review",
            "promotion_board",
            "observation_clock",
            "extended_shadow_protocol",
            "roadmap_dashboard",
            "decision_snapshot_lifecycle",
        }
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_governance_recovery_pack_healthy_with_source_warnings(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        payload_overrides={
            "etf_dynamic_v3_signal_input_recovery": {
                "restoration_status": "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
                "status": "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
                "next_required_action": "review_signal_input_warnings",
            },
        },
    )

    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS
    assert payload["summary"]["remaining_blocker_count"] == 0
    assert payload["summary"]["remaining_warning_count"] == 1
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_governance_recovery_pack_missing_source_fails_closed(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            omit={"etf_dynamic_v3_cost_sensitivity_review"},
        ),
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_BLOCKED
    assert payload["summary"]["structural_blocker_count"] == 1
    assert validation["validation_status"] == "FAIL"


def test_research_governance_recovery_pack_unwaived_report_index_warning_state(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(tmp_path)
    report_index["status"] = "PASS_WITH_WARNINGS"
    report_index["summary"] = {
        **dict(report_index["summary"]),
        "unwaived_warning_count": 1,
    }
    report_index["visibility_audit"] = {"unwaived_issue_ids": ["daily_score_stale"]}
    report_index["warnings"] = ["daily_score_stale:age_days=2;sla=1"]

    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS
    assert payload["summary"]["remaining_warning_count"] == 0
    assert payload["summary"]["report_index_unwaived_warning_count"] == 1
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS
    assert any(
        issue["issue_id"] == "recovery_governance_contains_unwaived_report_index_warnings"
        for issue in validation["warning_issues"]
    )


def test_research_governance_recovery_pack_rejects_live_trading_boundary_drift(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    payload["live_trading_boundary"] = {
        **dict(payload["live_trading_boundary"]),
        "live_trading_remains_forbidden": False,
        "live_trading_may_resume": True,
    }
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "live_trading_forbidden"
        for issue in validation["blocking_issues"]
    )


def test_research_governance_recovery_pack_manual_review_when_owner_holds(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        payload_overrides={
            "owner_decision_audit_log": {
                "summary": {"latest_owner_action": "hold"},
                "latest_owner_action": "hold",
            },
        },
    )

    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)

    assert (
        payload["recovery_governance_status"]
        == RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED
    )
    assert payload["summary"]["remaining_blocker_count"] == 0
    assert payload["summary"]["manual_review_item_count"] == 1
    assert payload["normal_paper_shadow_boundary"]["normal_paper_shadow_may_resume"] is False
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_governance_recovery_pack_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-06-17.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "research-governance-recovery-pack",
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
            "validate-research-governance-recovery-pack",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "research_governance_recovery_pack_2026-06-17.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir
            / "research_governance_recovery_pack_validation_2026-06-17.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_HEALTHY
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"][
        "research_governance_recovery_pack"
    ].endswith("research_governance_recovery_pack_2026-06-17.json")


def test_reader_brief_research_governance_recovery_pack_summary(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_research_governance_recovery_pack_payload(payload)
    report_path = tmp_path / "research_governance_recovery_pack_2026-06-17.json"
    validation_path = (
        tmp_path / "research_governance_recovery_pack_validation_2026-06-17.json"
    )
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._research_governance_recovery_pack_summary(
        {
            "reports": [
                {
                    "report_id": "research_governance_recovery_pack",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "research_governance_recovery_pack_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["recovery_governance_status"] == RECOVERY_GOVERNANCE_HEALTHY
    assert summary["validation_status"] == PASS_STATUS
    assert summary["source_report_count"] == len(SOURCE_REPORT_SPECS)
    assert summary["live_trading_remains_forbidden"] is True
    assert summary["production_effect"] == "none"


def _report_index_payload(
    tmp_path: Path,
    *,
    payload_overrides: dict[str, dict[str, object]] | None = None,
    validation_overrides: dict[str, dict[str, object]] | None = None,
    omit: set[str] | None = None,
) -> dict[str, object]:
    payload_overrides = payload_overrides or {}
    validation_overrides = validation_overrides or {}
    omit = omit or set()
    reports: list[dict[str, object]] = []
    for spec in SOURCE_REPORT_SPECS:
        report_id = str(spec["report_id"])
        validation_report_id = str(spec.get("validation_report_id") or report_id)
        if report_id not in omit:
            source_path = _source_path(tmp_path, report_id, str(spec["preferred_json_names"][0]))
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_payload = _source_payload(spec)
            source_payload.update(payload_overrides.get(report_id, {}))
            _merge_summary(source_payload, payload_overrides.get(report_id, {}))
            source_path.write_text(
                json.dumps(source_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            reports.append(_index_entry(report_id, source_path))
        if validation_report_id not in omit:
            validation_path = _source_path(
                tmp_path,
                validation_report_id,
                str(spec["validation_json_names"][0]),
            )
            validation_path.parent.mkdir(parents=True, exist_ok=True)
            validation_payload = _validation_payload(validation_report_id)
            validation_payload.update(validation_overrides.get(validation_report_id, {}))
            validation_path.write_text(
                json.dumps(validation_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            if validation_report_id != report_id:
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


def _source_payload(spec: dict[str, object]) -> dict[str, object]:
    report_id = str(spec["report_id"])
    status = str(spec["pass_statuses"][0])
    status_field = str(spec["status_fields"][0])
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "status": status,
        status_field: status,
        "candidate": "median_plus_regime_mismatch_filter",
        "next_required_action": "review_recovery_governance_source",
        "next_action": "review_recovery_governance_source",
        "production_effect": "none",
        "summary": {status_field: status, "status": status},
    }
    if report_id == "owner_decision_audit_log":
        payload.update({"latest_owner_action": "continue_normal_shadow"})
        payload["summary"] = {
            **dict(payload["summary"]),
            "latest_owner_action": "continue_normal_shadow",
        }
    if report_id == "etf_dynamic_v3_normal_paper_shadow_resumption_gate":
        payload.update({"normal_paper_shadow_may_resume": True})
        payload["summary"] = {
            **dict(payload["summary"]),
            "normal_paper_shadow_may_resume": True,
        }
    return payload


def _validation_payload(report_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": report_id,
        "validation_status": "PASS",
        "status": "PASS",
        "production_effect": "none",
        "summary": {"failed_check_count": 0},
    }


def _merge_summary(payload: dict[str, object], overrides: dict[str, object]) -> None:
    summary_override = overrides.get("summary")
    if isinstance(summary_override, dict):
        payload["summary"] = {**dict(payload.get("summary", {})), **summary_override}
