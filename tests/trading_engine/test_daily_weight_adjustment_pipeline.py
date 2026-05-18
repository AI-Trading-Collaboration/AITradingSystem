from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.reports.daily_weight_adjustment import (
    FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS,
    build_daily_weight_adjustment_summary_payload,
    render_daily_weight_adjustment_summary_report,
    write_daily_weight_adjustment_summary_report,
)


def test_complete_upstream_artifacts_generate_daily_weight_summary(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_summary_report(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["report_type"] == "daily_weight_adjustment_summary"
    assert payload["status"] == "OBSERVE_ONLY"
    assert payload["mode"] == "observe_only"
    assert payload["production_effect"] == "none"
    assert payload["candidate_count"] == 1
    assert payload["evaluable_candidate_count"] == 1
    assert payload["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["ready_for_manual_review_count"] == 1
    assert payload["blocked_count"] == 0
    assert payload["top_candidate_id"] == "weight_adjustment_candidate:2026-05-18:test"
    assert payload["main_blocked_by"] == "none"
    assert payload["missing_artifacts"] == []
    assert payload["recommendation"]["action"] == "manual_review_only"
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()


def test_missing_candidates_marks_summary_limited(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of, include_candidates=False)

    payload = build_daily_weight_adjustment_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["status"] == "LIMITED"
    assert payload["candidate_status"] == "LIMITED"
    assert payload["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert payload["ready_for_manual_review_count"] == 0
    assert payload["production_effect"] == "none"
    assert payload["main_blocked_by"] == "missing_weight_adjustment_candidates"
    assert any(
        "weight_adjustment_candidates_2026-05-18.json" in item
        for item in payload["missing_artifacts"]
    )
    assert payload["recommendation"]["action"] == "collect_missing_or_invalid_upstream_artifacts"


def test_missing_evaluation_marks_summary_limited(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of, include_evaluation=False)

    payload = build_daily_weight_adjustment_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["status"] == "LIMITED"
    assert payload["evaluation_status"] == "INSUFFICIENT_DATA"
    assert payload["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert payload["ready_for_manual_review_count"] == 0
    assert payload["production_effect"] == "none"
    assert payload["main_blocked_by"] == "missing_weight_candidate_evaluation"
    assert any(
        "weight_candidate_evaluation_2026-05-18.json" in item
        for item in payload["missing_artifacts"]
    )


def test_missing_promotion_gate_marks_summary_limited(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of, include_gate=False)

    payload = build_daily_weight_adjustment_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["status"] == "LIMITED"
    assert payload["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert payload["ready_for_manual_review_count"] == 0
    assert payload["production_effect"] == "none"
    assert payload["main_blocked_by"] == "missing_weight_promotion_gate"
    assert any(
        "weight_promotion_gate_2026-05-18.json" in item for item in payload["missing_artifacts"]
    )


def test_ready_gate_remains_manual_review_only_not_applied(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = build_daily_weight_adjustment_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["recommendation"]["action"] == "manual_review_only"
    assert payload["pipeline_contract"]["reads_existing_artifacts_only"] is True
    assert payload["pipeline_contract"]["runs_weight_adjustment_candidate_generator"] is False
    assert payload["pipeline_contract"]["runs_weight_candidate_evaluation"] is False
    assert payload["pipeline_contract"]["runs_weight_promotion_gate"] is False
    assert payload["safety_boundary"]["writes_production_profile"] is False
    assert payload["safety_boundary"]["writes_approved_profile"] is False
    assert payload["safety_boundary"]["triggers_trade"] is False


def test_forbidden_terms_do_not_appear_in_daily_summary_outputs(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_summary_report(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )
    markdown = render_daily_weight_adjustment_summary_report(payload)
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown

    for term in FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS:
        assert term not in combined


def test_summary_is_read_only_and_does_not_trigger_brokers_or_runners(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)
    production_profile = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    production_profile.parent.mkdir(parents=True, exist_ok=True)
    production_profile.write_text("version: test-production\n", encoding="utf-8")
    before = production_profile.read_text(encoding="utf-8")
    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        forbidden = (
            "ibkr",
            "paper_broker",
            "run_paper_trading_replay",
            "run_paper_trading_from_candidates",
            "run_ibkr_paper_controlled_fill",
            "run_ibkr_paper_order_lifecycle",
            "run_paperbroker_vs_ibkr_paper_comparison",
        )
        if any(term in name for term in forbidden):
            raise AssertionError(f"daily weight adjustment summary must not import {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_daily_weight_adjustment_summary_report(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )

    assert payload["production_effect"] == "none"
    assert payload["safety_boundary"]["calls_ibkr"] is False
    assert payload["safety_boundary"]["calls_paperbroker"] is False
    assert payload["safety_boundary"]["runs_paper_runner"] is False
    assert payload["safety_boundary"]["runs_replay_runner"] is False
    assert payload["safety_boundary"]["writes_production_profile"] is False
    assert production_profile.read_text(encoding="utf-8") == before


def test_dashboard_reads_daily_weight_summary_without_rerun(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)
    write_daily_weight_adjustment_summary_report(
        as_of=as_of,
        reports_dir=reports_dir,
        generated_at=_fixed_generated_at(),
    )
    _remove_upstream_weight_artifacts(reports_dir, as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=reports_dir,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["daily_weight_adjustment_summary"]
    assert summary["candidate_count"] == 1
    assert summary["evaluation_status"] == "CANDIDATE_PROMISING_BUT_LIMITED"
    assert summary["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["ready_for_manual_review_count"] == 1
    assert summary["main_blocked_by"] == "none"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert "Daily Weight Adjustment Summary" in html
    assert "daily_weight_adjustment_summary_2026-05-18.md" in html
    assert "manual_review_only" in html


def _write_daily_weight_inputs(
    reports_dir: Path,
    as_of: date,
    *,
    include_candidates: bool = True,
    include_evaluation: bool = True,
    include_gate: bool = True,
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    suffix = as_of.isoformat()
    candidate_id = f"weight_adjustment_candidate:{suffix}:test"
    if include_candidates:
        (reports_dir / f"weight_adjustment_candidates_{suffix}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "report_type": "weight_adjustment_candidates",
                    "as_of": suffix,
                    "generated_at": _fixed_generated_at().isoformat(),
                    "mode": "observe_only",
                    "production_effect": "none",
                    "status": "LIMITED",
                    "gate_status": "LIMITED",
                    "candidate_count": 1,
                    "top_candidate_id": candidate_id,
                    "summary": {
                        "candidate_count": 1,
                        "top_candidate_id": candidate_id,
                        "gate_status": "LIMITED",
                        "main_blocked_by": "manual_approval_required",
                        "production_effect": "none",
                        "mode": "observe_only",
                    },
                    "outputs": {
                        "json": str(reports_dir / f"weight_adjustment_candidates_{suffix}.json"),
                        "markdown": str(reports_dir / f"weight_adjustment_candidates_{suffix}.md"),
                    },
                    "candidates": [
                        {
                            "candidate_id": candidate_id,
                            "blocked_by": ["manual_approval_required"],
                            "required_validations": ["aits validate-data"],
                            "production_effect": "none",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (reports_dir / f"weight_adjustment_candidates_{suffix}.md").write_text(
            "# Weight Adjustment Candidate Generator\n\n- production_effect=none\n",
            encoding="utf-8",
        )
    if include_evaluation:
        (reports_dir / f"weight_candidate_evaluation_{suffix}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "report_type": "weight_candidate_evaluation",
                    "as_of": suffix,
                    "generated_at": _fixed_generated_at().isoformat(),
                    "evaluation_mode": "observe_only",
                    "production_effect": "none",
                    "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                    "summary": {
                        "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                        "candidate_count": 1,
                        "evaluable_candidate_count": 1,
                        "top_candidate_id": candidate_id,
                        "main_blocked_by": "manual_approval_required",
                        "production_effect": "none",
                        "evaluation_mode": "observe_only",
                    },
                    "outputs": {
                        "json": str(reports_dir / f"weight_candidate_evaluation_{suffix}.json"),
                        "markdown": str(reports_dir / f"weight_candidate_evaluation_{suffix}.md"),
                    },
                    "candidates": [
                        {
                            "candidate_id": candidate_id,
                            "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                            "blocked_by": ["manual_approval_required"],
                            "required_validations": [
                                "aits validate-data",
                                "manual_owner_review",
                            ],
                            "production_effect": "none",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (reports_dir / f"weight_candidate_evaluation_{suffix}.md").write_text(
            "# Weight Candidate Evaluation\n\n- production_effect=none\n",
            encoding="utf-8",
        )
    if include_gate:
        (reports_dir / f"weight_promotion_gate_{suffix}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "report_type": "weight_promotion_gate",
                    "as_of": suffix,
                    "generated_at": _fixed_generated_at().isoformat(),
                    "gate_mode": "manual_review_only",
                    "production_effect": "none",
                    "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
                    "summary": {
                        "gate_status": "READY_FOR_MANUAL_REVIEW",
                        "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
                        "candidate_count": 1,
                        "ready_for_manual_review_count": 1,
                        "blocked_count": 0,
                        "top_candidate_id": candidate_id,
                        "main_blocked_by": "none",
                        "production_effect": "none",
                        "gate_mode": "manual_review_only",
                    },
                    "outputs": {
                        "json": str(reports_dir / f"weight_promotion_gate_{suffix}.json"),
                        "markdown": str(reports_dir / f"weight_promotion_gate_{suffix}.md"),
                    },
                    "candidates": [
                        {
                            "candidate_id": candidate_id,
                            "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
                            "blocked": False,
                            "blocked_by": [],
                            "warnings": [],
                            "required_manual_review_items": [
                                "manual_owner_review",
                                "validate_data_quality_report",
                            ],
                            "recommendation": {"action": "manual_review_only"},
                            "production_effect": "none",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (reports_dir / f"weight_promotion_gate_{suffix}.md").write_text(
            "# Weight Promotion Gate\n\n- production_effect=none\n",
            encoding="utf-8",
        )


def _remove_upstream_weight_artifacts(reports_dir: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    for prefix in (
        "weight_adjustment_candidates",
        "weight_candidate_evaluation",
        "weight_promotion_gate",
    ):
        for suffix_ext in (".json", ".md"):
            path = reports_dir / f"{prefix}_{suffix}{suffix_ext}"
            if path.exists():
                path.unlink()


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / "daily_ops_run_metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "run_id": "daily-weight-adjustment-test",
                "status": "PASS",
                "project_root": str(tmp_path),
                "started_at": datetime(2026, 5, 18, 21, 0, tzinfo=UTC).isoformat(),
                "finished_at": datetime(2026, 5, 18, 21, 1, tzinfo=UTC).isoformat(),
                "visibility_cutoff": as_of.isoformat(),
                "input_visibility_status": "PASS",
                "git": {"commit": "test", "dirty": False},
                "commands": [],
                "step_results": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return metadata_path


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 18, 22, 0, tzinfo=UTC)
