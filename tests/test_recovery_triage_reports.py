from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.recovery_triage import (
    PASS_WITH_WARNINGS_STATUS,
    build_recovery_blocker_triage_payload,
    build_recovery_owner_action_map_payload,
    build_recovery_pack_source_depth_audit_payload,
    build_report_index_warning_triage_payload,
    validate_recovery_blocker_triage_payload,
    validate_recovery_owner_action_map_payload,
    validate_recovery_pack_source_depth_audit_payload,
    validate_report_index_warning_triage_payload,
)
from ai_trading_system.reports.research_governance_recovery_pack import (
    RECOVERY_GOVERNANCE_BLOCKED,
    SOURCE_REPORT_SPECS,
    build_research_governance_recovery_pack_payload,
)

RUN_DATE = date(2026, 6, 17)


def test_recovery_blocker_triage_expands_all_nine_blockers(tmp_path: Path) -> None:
    recovery_pack = _blocked_recovery_pack(tmp_path)

    payload = build_recovery_blocker_triage_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
    )
    validation = validate_recovery_blocker_triage_payload(payload)

    assert payload["summary"]["recovery_blocker_count"] == 9
    assert payload["summary"]["normal_paper_shadow_may_resume"] is False
    assert payload["summary"]["extended_shadow_remains_forbidden"] is True
    assert payload["summary"]["live_trading_remains_forbidden"] is True
    assert {item["source_id"] for item in payload["blocker_triage"]} == {
        "normal_paper_shadow_resumption_gate",
        "cost_sensitivity_metrics",
        "benchmark_baseline_metrics",
        "monthly_review",
        "promotion_board",
        "observation_clock",
        "extended_shadow_protocol",
        "roadmap_dashboard",
        "decision_snapshot_lifecycle",
    }
    assert all(item["blocks_normal_paper_shadow"] for item in payload["blocker_triage"])
    assert all(item["blocks_extended_shadow"] for item in payload["blocker_triage"])
    assert all(not item["live_trading_implication"] for item in payload["blocker_triage"])
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_report_index_warning_triage_keeps_unwaived_warnings_visible(
    tmp_path: Path,
) -> None:
    report_index = _report_index_warning_payload(tmp_path)

    payload = build_report_index_warning_triage_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
    )
    validation = validate_report_index_warning_triage_payload(payload)

    assert payload["summary"]["unwaived_warning_count"] == 9
    assert payload["summary"]["true_blocker_count"] == 1
    assert payload["summary"]["silent_waiver_count"] == 0
    assert all(item["waiver_action"] == "not_applied" for item in payload["warning_triage"])
    assert {
        item["warning_classification"] for item in payload["warning_triage"]
    }.issuperset({"true_blocker", "governance_warning", "stale_artifact_warning"})
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_source_depth_audit_detects_available_but_unhealthy_sources(
    tmp_path: Path,
) -> None:
    report_index = _recovery_report_index_payload(tmp_path)
    recovery_pack = _blocked_recovery_pack(tmp_path, report_index_payload=report_index)

    payload = build_recovery_pack_source_depth_audit_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_recovery_pack_source_depth_audit_payload(payload)

    assert payload["summary"]["source_availability"] == "16/16"
    assert payload["summary"]["blocked_source_count"] == 9
    assert payload["summary"]["unhealthy_source_count"] >= 9
    assert any(
        source["source_id"] == "normal_paper_shadow_resumption_gate"
        and source["health_status"] == "BLOCKED"
        for source in payload["unhealthy_sources"]
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_owner_action_map_preserves_forbidden_boundaries(
    tmp_path: Path,
) -> None:
    report_index = _recovery_report_index_payload(tmp_path)
    recovery_pack = _blocked_recovery_pack(tmp_path, report_index_payload=report_index)
    blocker_triage = build_recovery_blocker_triage_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
    )
    warning_triage = build_report_index_warning_triage_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_warning_payload(tmp_path),
    )
    source_audit = build_recovery_pack_source_depth_audit_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        report_index_payload=report_index,
        project_root=tmp_path,
    )

    payload = build_recovery_owner_action_map_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_triage_payload=blocker_triage,
        report_index_warning_triage_payload=warning_triage,
        source_depth_audit_payload=source_audit,
    )
    validation = validate_recovery_owner_action_map_payload(payload)

    assert payload["summary"]["next_owner_action"]
    assert payload["summary"]["next_code_action"]
    assert payload["summary"]["next_data_action"]
    assert payload["live_trading_forbidden"] is True
    assert payload["summary"]["remain_blocked_action_count"] == 9
    assert payload["paper_shadow_resumption_preconditions"][0]["status"] == "BLOCKED"
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_triage_cli_writes_outputs_and_reruns_recovery_pack(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _recovery_report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-06-17.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    recovery_pack = _blocked_recovery_pack(tmp_path, report_index_payload=report_index)
    recovery_pack_path = reports_dir / "research_governance_recovery_pack_2026-06-17.json"
    recovery_pack_path.write_text(
        json.dumps(recovery_pack, ensure_ascii=False),
        encoding="utf-8",
    )
    runner = CliRunner()

    commands = [
        [
            "reports",
            "recovery-blocker-triage",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--recovery-pack-path",
            str(recovery_pack_path),
        ],
        [
            "reports",
            "validate-recovery-blocker-triage",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
        [
            "reports",
            "report-index-warning-triage",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
        ],
        [
            "reports",
            "validate-report-index-warning-triage",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
        [
            "reports",
            "recovery-pack-source-depth-audit",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--recovery-pack-path",
            str(recovery_pack_path),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
        [
            "reports",
            "validate-recovery-pack-source-depth-audit",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
        [
            "reports",
            "recovery-owner-action-map",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
        [
            "reports",
            "validate-recovery-owner-action-map",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
        [
            "reports",
            "recovery-governance-rerun-after-triage",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output

    rerun_payload = json.loads(recovery_pack_path.read_text(encoding="utf-8"))
    assert rerun_payload["recovery_governance_status"] == RECOVERY_GOVERNANCE_BLOCKED
    assert rerun_payload["summary"]["remaining_blocker_count"] == 9
    assert rerun_payload["normal_paper_shadow_boundary"]["normal_paper_shadow_may_resume"] is False
    assert rerun_payload["extended_shadow_boundary"]["extended_shadow_remains_forbidden"] is True
    assert rerun_payload["live_trading_boundary"]["live_trading_remains_forbidden"] is True
    assert rerun_payload["triage_context"]["summary"]["recovery_blocker_count"] == 9


def _blocked_recovery_pack(
    tmp_path: Path,
    *,
    report_index_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index_payload or _recovery_report_index_payload(tmp_path),
        project_root=tmp_path,
    )


def _recovery_report_index_payload(tmp_path: Path) -> dict[str, object]:
    reports: list[dict[str, object]] = []
    for spec in SOURCE_REPORT_SPECS:
        report_id = str(spec["report_id"])
        validation_report_id = str(spec.get("validation_report_id") or report_id)
        source_path = _source_path(tmp_path, report_id, str(spec["preferred_json_names"][0]))
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_payload = _source_payload(spec, _blocked_status_for_source(str(spec["source_id"])))
        source_path.write_text(
            json.dumps(source_payload, ensure_ascii=False),
            encoding="utf-8",
        )
        validation_path = _source_path(
            tmp_path,
            validation_report_id,
            str(spec["validation_json_names"][0]),
        )
        validation_path.parent.mkdir(parents=True, exist_ok=True)
        validation_payload = _validation_payload(
            validation_report_id,
            _validation_status_for_source(str(spec["source_id"])),
        )
        validation_path.write_text(
            json.dumps(validation_payload, ensure_ascii=False),
            encoding="utf-8",
        )
        reports.append(_index_entry(report_id, source_path))
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


def _report_index_warning_payload(tmp_path: Path) -> dict[str, object]:
    warning_specs = [
        ("daily_score", "Daily Score Report", "daily", True),
        ("evidence_dashboard", "Evidence Dashboard", "daily", False),
        ("calculation_explainers", "Calculation Explainers", "daily", False),
        ("score_change_attribution", "Score Change Attribution", "daily", False),
        ("market_panel", "Market Price Panel", "daily", False),
        ("research_governance_summary", "Research Governance Summary", "governance", False),
        ("artifact_lineage_graph", "Artifact Lineage Graph", "governance", False),
        ("artifact_lineage_validation", "Artifact Lineage Validation", "governance", False),
        ("market_data_freshness", "Market Data Freshness", "data_quality", False),
    ]
    reports: list[dict[str, object]] = []
    warnings: list[str] = []
    issue_ids: list[str] = []
    for report_id, title, group, required in warning_specs:
        artifact_path = tmp_path / "outputs" / "reports" / f"{report_id}_2026-06-15.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text("{}", encoding="utf-8")
        issue_id = f"{report_id}_stale"
        warning_text = f"{issue_id}:age_days=2;sla=1"
        warnings.append(warning_text)
        issue_ids.append(issue_id)
        reports.append(
            {
                "report_id": report_id,
                "title": title,
                "group": group,
                "cadence": "daily",
                "owner": "system",
                "required_for_daily_reading": required,
                "latest_artifact_path": str(artifact_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "STALE",
                "age_days": 2,
                "freshness_sla_days": 1,
                "owner_action": "regenerate_if_missing_or_stale",
                "visibility_status": "WARNING",
                "visibility_issue": {
                    "issue_id": issue_id,
                    "issue_status": "STALE",
                    "warning_text": warning_text,
                    "severity": "warning",
                    "report_id": report_id,
                    "required_for_daily_reading": required,
                },
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "report_count": len(reports),
            "missing_count": 0,
            "stale_count": len(reports),
            "required_missing_count": 0,
            "unwaived_warning_count": len(warnings),
            "explicit_waiver_count": 0,
        },
        "reports": reports,
        "warnings": warnings,
        "visibility_audit": {"unwaived_issue_ids": issue_ids},
        "production_effect": "none",
    }


def _source_path(tmp_path: Path, report_id: str, name: str) -> Path:
    return tmp_path / "sources" / report_id / name


def _index_entry(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "artifact_status": "AVAILABLE",
        "artifact_date": RUN_DATE.isoformat(),
        "freshness_status": "FRESH",
        "age_days": 0,
        "freshness_sla_days": 7,
        "production_effect": "none",
    }


def _source_payload(spec: dict[str, object], status: str) -> dict[str, object]:
    report_id = str(spec["report_id"])
    status_field = str(spec["status_fields"][0])
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "as_of": RUN_DATE.isoformat(),
        "status": status,
        status_field: status,
        "candidate": "median_plus_regime_mismatch_filter",
        "next_required_action": _next_action_for_status(status),
        "next_action": _next_action_for_status(status),
        "production_effect": "none",
        "summary": {status_field: status, "status": status},
    }
    if report_id == "owner_decision_audit_log":
        payload.update({"latest_owner_action": "hold"})
        payload["summary"] = {**dict(payload["summary"]), "latest_owner_action": "hold"}
    if report_id == "etf_dynamic_v3_normal_paper_shadow_resumption_gate":
        payload.update({"normal_paper_shadow_may_resume": False})
        payload["summary"] = {
            **dict(payload["summary"]),
            "normal_paper_shadow_may_resume": False,
        }
    return payload


def _validation_payload(report_id: str, status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": report_id,
        "as_of": RUN_DATE.isoformat(),
        "validation_status": status,
        "status": status,
        "production_effect": "none",
        "summary": {"failed_check_count": 0, "warning_check_count": int("WARNING" in status)},
    }


def _blocked_status_for_source(source_id: str) -> str:
    return {
        "signal_input_restoration": "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
        "signal_completeness_recovery": "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
        "readiness_health_recovery": "MANUAL_REVIEW_REQUIRED",
        "normal_paper_shadow_resumption_gate": "RESUME_NORMAL_SHADOW_BLOCKED",
        "cost_sensitivity_metrics": "NOT_MEANINGFUL_UNDER_COSTS",
        "benchmark_baseline_metrics": "CANDIDATE_UNDERPERFORMS_BASELINES",
        "monthly_review": "MONTHLY_REVIEW_BLOCKED",
        "promotion_board": "HOLD_FOR_MORE_DATA",
        "observation_clock": "OBSERVATION_PERIOD_UNMET",
        "extended_shadow_protocol": "EXTENDED_SHADOW_BLOCKED",
        "roadmap_dashboard": "ROADMAP_BLOCKED",
        "decision_snapshot_lifecycle": "SNAPSHOT_MISSING_BLOCKING",
    }.get(source_id, str(_spec_by_source_id(source_id)["pass_statuses"][0]))


def _validation_status_for_source(source_id: str) -> str:
    return (
        "PASS_WITH_WARNINGS"
        if source_id
        in {
            "recovery_evidence_pack",
            "monthly_review",
            "promotion_board",
            "observation_clock",
            "extended_shadow_protocol",
            "roadmap_dashboard",
            "decision_snapshot_lifecycle",
        }
        else "PASS"
    )


def _next_action_for_status(status: str) -> str:
    if status == "RESUME_NORMAL_SHADOW_BLOCKED":
        return "record_manual_owner_review_before_normal_shadow_resumption"
    if status == "NOT_MEANINGFUL_UNDER_COSTS":
        return "return_candidate_to_research_until_net_improvement_survives_costs"
    if status == "CANDIDATE_UNDERPERFORMS_BASELINES":
        return "return_candidate_to_research_until_it_outperforms_baseline_controls"
    if status == "SNAPSHOT_MISSING_BLOCKING":
        return "run_canonical_score_daily_or_keep_same_day_reader_brief_blocked"
    return "review_recovery_governance_source"


def _spec_by_source_id(source_id: str) -> dict[str, object]:
    for spec in SOURCE_REPORT_SPECS:
        if spec["source_id"] == source_id:
            return spec
    raise AssertionError(source_id)
