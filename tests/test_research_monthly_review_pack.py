from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.research_monthly_review_pack import (
    BLOCKED_STATUS,
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    READY_STATUS,
    SOURCE_SPECS,
    build_research_monthly_review_pack_payload,
    validate_research_monthly_review_pack_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_research_monthly_review_pack_aggregates_candidate_blockers(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "INSUFFICIENT_COST_INPUTS",
                "blocking_reasons": ["candidate_metrics:insufficient_cost_inputs"],
                "next_required_action": (
                    "provide_numeric_turnover_and_performance_metrics_before_cost_review"
                ),
            },
            "etf_dynamic_v3_signal_input_completeness": {
                "signal_input_status": "BLOCKING",
                "signal_input_blocking_input_ids": ["etf_feature_matrix"],
                "next_required_action": "stop_paper_shadow_until_signal_inputs_are_restored",
            },
        },
    )

    payload = build_research_monthly_review_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_monthly_review_pack_payload(payload)

    assert payload["monthly_review_status"] == BLOCKED_STATUS
    assert payload["summary"]["active_candidate_count"] == 1
    assert payload["summary"]["paper_shadow_candidate_count"] == 1
    assert payload["summary"]["needs_evidence_candidate_count"] == 1
    assert payload["summary"]["major_blocker_count"] >= 2
    assert payload["data_governance_status"]["status"] == "BLOCKED"
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_monthly_review_pack_blocks_adverse_cost_and_benchmark_statuses(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
                "next_required_action": (
                    "return_candidate_to_research_until_net_improvement_survives_costs"
                ),
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "CANDIDATE_UNDERPERFORMS_BASELINES",
                "next_required_action": (
                    "return_candidate_to_research_until_it_outperforms_baseline_controls"
                ),
            },
        },
    )

    payload = build_research_monthly_review_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )

    assert payload["monthly_review_status"] == BLOCKED_STATUS
    assert {
        issue["source_id"] for issue in payload["major_blockers"]
    } >= {"cost_sensitivity_reports", "benchmark_comparison_reports"}


def test_research_monthly_review_pack_validation_fails_missing_required_source(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(tmp_path, omit={"artifact_lineage_graph"})

    payload = build_research_monthly_review_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_monthly_review_pack_payload(payload)

    assert payload["monthly_review_status"] == BLOCKED_STATUS
    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "required_source_artifacts_available"
        for issue in validation["blocking_issues"]
    )


def test_research_monthly_review_pack_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "research-monthly-review-pack",
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
            "validate-research-monthly-review-pack",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "research_monthly_review_pack_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "research_monthly_review_pack_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["monthly_review_status"] == READY_STATUS
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["research_monthly_review_pack"].endswith(
        "research_monthly_review_pack_2026-05-04.json"
    )


def test_reader_brief_research_monthly_review_pack_summary(tmp_path: Path) -> None:
    payload = build_research_monthly_review_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_research_monthly_review_pack_payload(payload)
    report_path = tmp_path / "research_monthly_review_pack_2026-05-04.json"
    validation_path = tmp_path / "research_monthly_review_pack_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._research_monthly_review_pack_summary(
        {
            "reports": [
                {
                    "report_id": "research_monthly_review_pack",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "research_monthly_review_pack_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["monthly_review_status"] == READY_STATUS
    assert summary["validation_status"] == PASS_STATUS
    assert summary["active_candidate_count"] == 1
    assert summary["production_effect"] == "none"


def _report_index_payload(
    tmp_path: Path,
    *,
    overrides: dict[str, dict[str, object]] | None = None,
    omit: set[str] | None = None,
) -> dict[str, object]:
    overrides = overrides or {}
    omit = omit or set()
    reports: list[dict[str, object]] = []
    for spec in SOURCE_SPECS:
        report_id = str(spec["report_id"])
        if report_id in omit:
            continue
        payload = _source_payload(report_id)
        payload.update(overrides.get(report_id, {}))
        source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        reports.append(
            {
                "report_id": report_id,
                "latest_artifact_path": str(source_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "FRESH",
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "reports": reports,
        "production_effect": "none",
    }


def _source_payload(report_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "candidate": "candidate_a",
        "status": "PASS",
        "next_action": "continue_monthly_manual_review",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "etf_dynamic_v3_candidate_decision_ledger": {
                "final_decision": "FORMALIZE_RESEARCH_METHOD",
                "next_required_action": "continue_paper_shadow_review",
            },
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "coverage_status": "FULL_WEEK_REVIEW",
                "weekly_decision": "CONTINUE",
            },
            "etf_dynamic_v3_evidence_staleness_monitor": {
                "evidence_freshness_status": "ACCEPTABLE",
                "safe_to_continue_shadow": True,
            },
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "READY_TO_CONTINUE",
                "safe_to_continue_shadow": True,
            },
            "research_safety_boundary_audit": {
                "safety_status": "SAFETY_PASS",
                "summary": {"unsafe_signal_count": 0, "missing_metadata_count": 0},
            },
            "owner_decision_audit_log": {
                "audit_log_status": "AUDIT_LOG_PASS",
                "summary": {"included_record_count": 1},
            },
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "COST_REVIEW_PASS",
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "BASELINE_CONTROL_PASS",
            },
            "artifact_lineage_graph": {
                "lineage_status": "PASS",
                "summary": {"blocking_issue_count": 0, "warning_issue_count": 0},
            },
            "data_refresh_audit": {
                "status": "PASS",
                "validation_status": "PASS",
            },
            "data_source_fallback_policy": {
                "fallback_status": "PRIMARY_OK",
            },
            "cache_catalog": {
                "cache_integrity_status": "OK",
            },
            "pit_source_manifest": {
                "status": "PASS",
                "validation_status": "PASS",
            },
            "etf_dynamic_v3_signal_input_completeness": {
                "signal_input_status": "PASS",
            },
            "etf_dynamic_v3_paper_shadow_health": {
                "paper_shadow_health_status": "HEALTHY",
            },
        }.get(report_id, {})
    )
    return payload
