from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.reports.daily_weight_adjustment import (
    FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS,
)
from scripts.run_daily_weight_adjustment_scheduler_dry_run import (
    render_scheduler_dry_run_report,
    write_daily_weight_adjustment_scheduler_dry_run_report,
)


def test_scheduler_dry_run_generates_report(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )

    assert payload["report_type"] == "daily_weight_adjustment_scheduler_dry_run"
    assert payload["mode"] == "dry_run"
    assert payload["dry_run_status"] == "PASS"
    assert payload["pipeline_status"] == "OBSERVE_ONLY"
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["candidate_count"] == 1
    assert payload["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["ready_for_manual_review_count"] == 1
    assert payload["blocked_count"] == 0
    assert payload["missing_artifacts"] == []
    assert payload["duration_seconds"] == 2.0
    assert payload["invoked_command"]["script"] == "scripts/run_daily_weight_adjustment.py"
    assert payload["invoked_command"]["invocation_mode"] == "in_process_wrapper"
    assert "--date 2026-05-18" in payload["invoked_command"]["equivalent_command"]
    artifacts = _artifacts_by_id(payload)
    assert Path(artifacts["daily_weight_adjustment_summary_json"]["path"]).exists()
    assert Path(artifacts["daily_weight_adjustment_summary_markdown"]["path"]).exists()
    assert Path(artifacts["scheduler_dry_run_json"]["path"]).exists()
    assert Path(artifacts["scheduler_dry_run_markdown"]["path"]).exists()


def test_scheduler_dry_run_missing_upstream_artifact_is_limited(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of, include_candidates=False)

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )

    assert payload["dry_run_status"] == "LIMITED"
    assert payload["pipeline_status"] == "LIMITED"
    assert payload["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert payload["ready_for_manual_review_count"] == 0
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert "pipeline_status_limited" in payload["warnings"]
    assert any(
        "weight_adjustment_candidates_2026-05-18.json" in item
        for item in payload["missing_artifacts"]
    )


def test_scheduler_dry_run_safety_flags_are_fixed(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )

    safety = payload["safety_checks"]
    assert safety["production_profile_write_attempted"] is False
    assert safety["approved_profile_write_attempted"] is False
    assert safety["ibkr_order_path_called"] is False
    assert safety["paperbroker_order_path_called"] is False
    assert safety["replay_runner_called"] is False
    assert safety["controlled_fill_runner_called"] is False
    assert safety["order_lifecycle_runner_called"] is False
    assert safety["broker_comparison_runner_called"] is False
    assert safety["dashboard_write_only_summary"] is True
    assert safety["forbidden_terms_absent"] is True
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True


def test_scheduler_dry_run_does_not_modify_profiles_or_call_order_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)
    production_profile = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    approved_profile = tmp_path / "config" / "weights" / "weight_profile_approved.yaml"
    production_profile.parent.mkdir(parents=True, exist_ok=True)
    production_profile.write_text("version: test-production\n", encoding="utf-8")
    approved_profile.write_text("version: test-approved\n", encoding="utf-8")
    before_production = production_profile.read_text(encoding="utf-8")
    before_approved = approved_profile.read_text(encoding="utf-8")
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
            raise AssertionError(f"scheduler dry-run must not import {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )

    assert production_profile.read_text(encoding="utf-8") == before_production
    assert approved_profile.read_text(encoding="utf-8") == before_approved
    assert payload["safety_checks"]["production_profile_write_attempted"] is False
    assert payload["safety_checks"]["approved_profile_write_attempted"] is False
    assert payload["safety_checks"]["ibkr_order_path_called"] is False
    assert payload["safety_checks"]["paperbroker_order_path_called"] is False
    assert payload["safety_checks"]["replay_runner_called"] is False


def test_scheduler_dry_run_forbidden_terms_do_not_appear(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )
    markdown = render_scheduler_dry_run_report(payload)
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown

    for term in FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS:
        assert term not in combined


def test_scheduler_dry_run_generated_artifacts_are_recorded(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_daily_weight_inputs(reports_dir, as_of)

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=as_of,
        reports_dir=reports_dir,
        clock=_fixed_clock(),
    )

    artifacts = _artifacts_by_id(payload)
    assert set(artifacts) == {
        "daily_weight_adjustment_summary_json",
        "daily_weight_adjustment_summary_markdown",
        "scheduler_dry_run_json",
        "scheduler_dry_run_markdown",
    }
    assert artifacts["daily_weight_adjustment_summary_json"]["role"] == "pipeline_output"
    assert artifacts["scheduler_dry_run_json"]["role"] == "scheduler_output"
    for artifact in artifacts.values():
        assert artifact["exists"] is True
        assert Path(artifact["path"]).exists()


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


def _artifacts_by_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(artifact["artifact_id"]): artifact
        for artifact in payload["generated_artifacts"]
        if isinstance(artifact, dict)
    }


def _fixed_clock() -> Any:
    started = datetime(2026, 5, 18, 22, 0, tzinfo=UTC)
    calls = {"count": 0}

    def _clock() -> datetime:
        value = started + timedelta(seconds=2 * calls["count"])
        calls["count"] += 1
        return value

    return _clock


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 18, 21, 0, tzinfo=UTC)
