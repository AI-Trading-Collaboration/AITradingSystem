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
from ai_trading_system.trading_engine.reports.weight_promotion_gate import (
    ALLOWED_PROMOTION_GATE_STATUSES,
    build_weight_promotion_gate_payload,
    render_weight_promotion_gate_report,
    write_weight_promotion_gate_report,
)

FORBIDDEN_OUTPUT_TERMS = (
    "AUTO_PROMOTE",
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
    "APPROVED",
)


def test_missing_candidate_evaluation_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of, include_evaluation=False)

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["report_type"] == "weight_promotion_gate"
    assert payload["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert payload["gate_mode"] == "manual_review_only"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["main_blocked_by"] == "missing_weight_candidate_evaluation"
    assert payload["candidates"][0]["promotion_gate_status"] == "INSUFFICIENT_DATA"
    assert "missing_weight_candidate_evaluation" in payload["candidates"][0]["blocked_by"]


def test_candidate_blocked_keeps_gate_blocked(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of, candidate_blocked=True)

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert payload["promotion_gate_status"] == "BLOCKED"
    assert candidate["promotion_gate_status"] == "BLOCKED"
    assert "candidate_blocked" in candidate["blocked_by"]


def test_poor_data_quality_blocks_promotion(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of, synthetic_snapshot_ratio=0.8)

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["promotion_gate_status"] == "BLOCKED"
    assert "synthetic_snapshot_ratio_too_high" in candidate["blocked_by"]
    assert "low_data_quality" in candidate["blocked_by"]


def test_drawdown_worse_blocks_promotion(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of, max_drawdown_delta=-0.01)

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["promotion_gate_status"] == "BLOCKED"
    assert "max_drawdown_worse" in candidate["blocked_by"]
    assert candidate["risk_delta_summary"]["max_drawdown_worse"] is True


def test_continuous_replay_missing_blocks_and_warns(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of, continuous_replay_available=False)

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["promotion_gate_status"] == "BLOCKED"
    assert "continuous_replay_missing" in candidate["blocked_by"]
    assert "continuous_replay_missing" in candidate["warnings"]


def test_insufficient_sample_blocks_promotion(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(
        reports_dir,
        as_of,
        paper_sample_count=2,
        shadow_sample_count=2,
        production_sample_count=2,
        filled_count=1,
    )

    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["promotion_gate_status"] == "BLOCKED"
    assert "insufficient_sample" in candidate["blocked_by"]
    assert "insufficient_filled_count" in candidate["blocked_by"]


def test_ready_inputs_enter_manual_review_only_status(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of)

    payload = write_weight_promotion_gate_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert payload["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["summary"]["ready_for_manual_review_count"] == 1
    assert payload["summary"]["blocked_count"] == 0
    assert candidate["promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert candidate["improvement_summary"]["stable_improvement_signal"] is True
    assert candidate["recommendation"]["action"] == "manual_review_only"
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()


def test_forbidden_terms_do_not_appear_in_outputs(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of)

    payload = write_weight_promotion_gate_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )
    markdown = render_weight_promotion_gate_report(payload)
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown

    assert payload["promotion_gate_status"] in ALLOWED_PROMOTION_GATE_STATUSES
    for candidate in payload["candidates"]:
        assert candidate["promotion_gate_status"] in ALLOWED_PROMOTION_GATE_STATUSES
    for term in FORBIDDEN_OUTPUT_TERMS:
        assert term not in combined


def test_gate_is_read_only_and_does_not_trigger_brokers_or_runners(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of)
    before = context["production_profile"].read_text(encoding="utf-8")
    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        forbidden = (
            "ibkr",
            "paper_broker",
            "run_paper_trading_replay",
            "run_paper_trading_from_candidates",
        )
        if any(term in name for term in forbidden):
            raise AssertionError(f"weight promotion gate must not import {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_weight_promotion_gate_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["production_effect"] == "none"
    assert payload["safety_boundary"]["calls_ibkr"] is False
    assert payload["safety_boundary"]["calls_paperbroker"] is False
    assert payload["safety_boundary"]["runs_paper_runner"] is False
    assert payload["safety_boundary"]["runs_replay_runner"] is False
    assert payload["safety_boundary"]["writes_production_profile"] is False
    assert context["production_profile"].read_text(encoding="utf-8") == before


def test_dashboard_reads_weight_promotion_gate_without_rerun(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_ready_inputs(reports_dir, as_of)
    before = context["production_profile"].read_text(encoding="utf-8")
    write_weight_promotion_gate_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=reports_dir,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["weight_promotion_gate"]
    assert summary["gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["candidate_count"] == 1
    assert summary["ready_for_manual_review_count"] == 1
    assert summary["blocked_count"] == 0
    assert summary["top_candidate_id"] == "weight_adjustment_candidate:2026-05-18:test"
    assert summary["main_blocked_by"] == "none"
    assert summary["production_effect"] == "none"
    assert summary["gate_mode"] == "manual_review_only"
    assert "Weight Promotion Gate" in html
    assert "weight_promotion_gate_2026-05-18.md" in html
    assert context["production_profile"].read_text(encoding="utf-8") == before


def _write_config_context(tmp_path: Path) -> dict[str, Path]:
    config_dir = tmp_path / "config"
    weights_dir = config_dir / "weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    policy = config_dir / "weight_promotion_gate_policy.yaml"
    policy.write_text(
        "\n".join(
            [
                "policy_id: weight_promotion_gate_policy",
                "version: 1",
                "status: pilot_baseline",
                "owner: system",
                "production_effect: none",
                "rationale: test policy",
                "intended_effect: test promotion gate",
                "validation_evidence: unit tests",
                "review_condition: after forward samples",
                "thresholds:",
                "  minimum_paper_signal_sample_count: 7",
                "  minimum_shadow_sample_count: 7",
                "  minimum_production_baseline_count: 7",
                "  minimum_filled_count: 3",
                "  maximum_synthetic_snapshot_ratio: 0.25",
                "  minimum_historical_ohlc_coverage: 0.70",
                "  minimum_reconciliation_pass_ratio: 0.90",
                "  minimum_max_drawdown_delta: 0.0",
                "  maximum_exposure_delta: 0.0",
                "  maximum_concentration_delta: 0.0",
                "  minimum_final_equity_delta: 0.0",
                "  minimum_improvement_windows: 2",
                "required_validations:",
                "  - aits validate-data",
                "  - manual_owner_review",
                "",
            ]
        ),
        encoding="utf-8",
    )
    governance = config_dir / "parameter_governance.yaml"
    governance.write_text(
        "\n".join(
            [
                "version: parameter_governance_test",
                "status: pilot",
                "owner: system",
                "production_effect: none",
                "parameters: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    production_profile = weights_dir / "weight_profile_current.yaml"
    production_profile.write_text(
        "\n".join(
            [
                'version: "test-production"',
                'status: "production"',
                "base_weights:",
                "  trend: 0.25",
                "  fundamentals: 0.25",
                "  macro_liquidity: 0.15",
                "  risk_sentiment: 0.15",
                "  valuation: 0.10",
                "  policy_geopolitics: 0.10",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "policy": policy,
        "governance": governance,
        "production_profile": production_profile,
    }


def _write_ready_inputs(
    reports_dir: Path,
    as_of: date,
    *,
    include_evaluation: bool = True,
    candidate_blocked: bool = False,
    paper_sample_count: int = 9,
    shadow_sample_count: int = 9,
    production_sample_count: int = 9,
    filled_count: int = 7,
    synthetic_snapshot_ratio: float = 0.0,
    historical_ohlc_coverage: float = 1.0,
    reconciliation_pass_ratio: float = 1.0,
    continuous_replay_available: bool = True,
    max_drawdown_delta: float = 0.0,
    exposure_delta: float = 0.0,
    concentration_delta: float = 0.0,
    final_equity_delta: float = 100.0,
) -> None:
    suffix = as_of.isoformat()
    candidate_id = f"weight_adjustment_candidate:{suffix}:test"
    hard_blockers = ["candidate_blocked"] if candidate_blocked else []
    candidate_blockers = ["manual_approval_required", *hard_blockers]
    _write_json(
        reports_dir / f"weight_adjustment_candidates_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "weight_adjustment_candidates",
            "as_of": suffix,
            "generated_at": _fixed_generated_at().isoformat(),
            "mode": "observe_only",
            "production_effect": "none",
            "status": "BLOCKED",
            "gate_status": "BLOCKED",
            "candidate_count": 1,
            "top_candidate_id": candidate_id,
            "outputs": {
                "json": str(reports_dir / f"weight_adjustment_candidates_{suffix}.json"),
                "markdown": str(reports_dir / f"weight_adjustment_candidates_{suffix}.md"),
            },
            "summary": {
                "candidate_count": 1,
                "top_candidate_id": candidate_id,
                "gate_status": "BLOCKED",
                "main_blocked_by": "manual_approval_required",
                "production_effect": "none",
                "mode": "observe_only",
            },
            "candidate_gate": {
                "status": "BLOCKED",
                "blocked": True,
                "blocked_by": candidate_blockers,
            },
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "generated_at": _fixed_generated_at().isoformat(),
                    "mode": "observe_only",
                    "blocked": candidate_blocked,
                    "source_profile": {"profile_id": "production_current"},
                    "target_profile": {"profile_id": "shadow_alpha_tilt_v1_small_step"},
                    "parameter_changes": [
                        {
                            "parameter_id": "base_weights.trend",
                            "from": 0.25,
                            "to": 0.27,
                            "delta": 0.02,
                        }
                    ],
                    "blocked_by": candidate_blockers,
                    "required_validations": ["aits validate-data", "manual_owner_review"],
                    "production_effect": "none",
                }
            ],
        },
    )
    (reports_dir / f"weight_adjustment_candidates_{suffix}.md").write_text(
        "# Weight Adjustment Candidate Generator\n\n- production_effect=none\n",
        encoding="utf-8",
    )
    if include_evaluation:
        _write_weight_candidate_evaluation(
            reports_dir,
            as_of,
            candidate_id=candidate_id,
            candidate_blocked=candidate_blocked,
            candidate_blockers=candidate_blockers,
            paper_sample_count=paper_sample_count,
            shadow_sample_count=shadow_sample_count,
            production_sample_count=production_sample_count,
            filled_count=filled_count,
            synthetic_snapshot_ratio=synthetic_snapshot_ratio,
            historical_ohlc_coverage=historical_ohlc_coverage,
            reconciliation_pass_ratio=reconciliation_pass_ratio,
            continuous_replay_available=continuous_replay_available,
            max_drawdown_delta=max_drawdown_delta,
            exposure_delta=exposure_delta,
            concentration_delta=concentration_delta,
            final_equity_delta=final_equity_delta,
        )
    _write_quality_inputs(
        reports_dir,
        as_of,
        paper_sample_count=paper_sample_count,
        shadow_sample_count=shadow_sample_count,
        production_sample_count=production_sample_count,
        filled_count=filled_count,
        synthetic_snapshot_ratio=synthetic_snapshot_ratio,
        historical_ohlc_coverage=historical_ohlc_coverage,
        reconciliation_pass_ratio=reconciliation_pass_ratio,
        continuous_replay_available=continuous_replay_available,
    )
    _write_json(
        reports_dir / f"daily_decision_summary_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "daily_decision_summary",
            "as_of": suffix,
            "data_gate": {"status": "PASS"},
            "warnings": [],
            "production_effect": "none",
        },
    )


def _write_weight_candidate_evaluation(
    reports_dir: Path,
    as_of: date,
    *,
    candidate_id: str,
    candidate_blocked: bool,
    candidate_blockers: list[str],
    paper_sample_count: int,
    shadow_sample_count: int,
    production_sample_count: int,
    filled_count: int,
    synthetic_snapshot_ratio: float,
    historical_ohlc_coverage: float,
    reconciliation_pass_ratio: float,
    continuous_replay_available: bool,
    max_drawdown_delta: float,
    exposure_delta: float,
    concentration_delta: float,
    final_equity_delta: float,
) -> None:
    suffix = as_of.isoformat()
    windows = {
        str(window): _evaluation_window(
            window=window,
            candidate_blockers=candidate_blockers,
            paper_sample_count=paper_sample_count,
            shadow_sample_count=shadow_sample_count,
            production_sample_count=production_sample_count,
            filled_count=filled_count,
            synthetic_snapshot_ratio=synthetic_snapshot_ratio,
            historical_ohlc_coverage=historical_ohlc_coverage,
            reconciliation_pass_ratio=reconciliation_pass_ratio,
            continuous_replay_available=continuous_replay_available,
            max_drawdown_delta=max_drawdown_delta,
            exposure_delta=exposure_delta,
            concentration_delta=concentration_delta,
            final_equity_delta=final_equity_delta,
        )
        for window in (7, 14, 30)
    }
    _write_json(
        reports_dir / f"weight_candidate_evaluation_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "weight_candidate_evaluation",
            "as_of": suffix,
            "generated_at": _fixed_generated_at().isoformat(),
            "status": "BLOCKED",
            "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
            "evaluation_mode": "observe_only",
            "production_effect": "none",
            "selected_window_days": 30,
            "outputs": {
                "json": str(reports_dir / f"weight_candidate_evaluation_{suffix}.json"),
                "markdown": str(reports_dir / f"weight_candidate_evaluation_{suffix}.md"),
            },
            "summary": {
                "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                "candidate_count": 1,
                "evaluable_candidate_count": 1,
                "blocked_candidate_count": int(candidate_blocked),
                "top_candidate_id": candidate_id,
                "main_blocked_by": "manual_approval_required",
                "production_effect": "none",
                "evaluation_mode": "observe_only",
            },
            "windows": windows,
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "source_profile": {"profile_id": "production_current"},
                    "target_profile": {"profile_id": "shadow_alpha_tilt_v1_small_step"},
                    "parameter_changes": [],
                    "required_validations": ["aits validate-data", "manual_owner_review"],
                    "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                    "blocked": candidate_blocked,
                    "blocked_by": candidate_blockers,
                    "warnings": [],
                    "scorecard": {
                        "selected_window_days": 30,
                        "selected_window": windows["30"],
                        "windows": windows,
                    },
                    "recommendation": {"action": "manual_review_only"},
                    "production_effect": "none",
                }
            ],
        },
    )
    (reports_dir / f"weight_candidate_evaluation_{suffix}.md").write_text(
        "# Weight Candidate Evaluation\n\n- production_effect=none\n",
        encoding="utf-8",
    )


def _evaluation_window(
    *,
    window: int,
    candidate_blockers: list[str],
    paper_sample_count: int,
    shadow_sample_count: int,
    production_sample_count: int,
    filled_count: int,
    synthetic_snapshot_ratio: float,
    historical_ohlc_coverage: float,
    reconciliation_pass_ratio: float,
    continuous_replay_available: bool,
    max_drawdown_delta: float,
    exposure_delta: float,
    concentration_delta: float,
    final_equity_delta: float,
) -> dict[str, Any]:
    return {
        "window_days": window,
        "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
        "blocked": bool(set(candidate_blockers) - {"manual_approval_required"}),
        "blocked_by": candidate_blockers,
        "warnings": [] if continuous_replay_available else ["continuous_replay_missing"],
        "candidate_count": 1,
        "evaluable_candidate_count": 1,
        "blocked_candidate_count": 0,
        "continuous_replay_available": continuous_replay_available,
        "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
        "historical_ohlc_coverage": historical_ohlc_coverage,
        "reconciliation_pass_ratio": reconciliation_pass_ratio,
        "paper_signal_quality_status": "OBSERVE_ONLY",
        "shadow_impact_status": "SHADOW_PROMISING_BUT_LIMITED",
        "replay_mode": "continuous_portfolio" if continuous_replay_available else "missing",
        "max_drawdown_delta": max_drawdown_delta,
        "final_equity_delta": final_equity_delta,
        "exposure_delta": exposure_delta,
        "concentration_delta": concentration_delta,
        "metrics": {
            "paper_signal_sample_count": paper_sample_count,
            "shadow_sample_count": shadow_sample_count,
            "production_baseline_count": production_sample_count,
            "filled_count": filled_count,
            "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
            "historical_ohlc_coverage": historical_ohlc_coverage,
            "reconciliation_pass_ratio": reconciliation_pass_ratio,
            "continuous_replay_available": continuous_replay_available,
            "replay_mode": "continuous_portfolio" if continuous_replay_available else "missing",
            "max_drawdown_delta": max_drawdown_delta,
            "final_equity_delta": final_equity_delta,
            "exposure_delta": exposure_delta,
            "concentration_delta": concentration_delta,
            "paper_signal_quality_status": "OBSERVE_ONLY",
            "shadow_impact_status": "SHADOW_PROMISING_BUT_LIMITED",
        },
        "production_effect": "none",
    }


def _write_quality_inputs(
    reports_dir: Path,
    as_of: date,
    *,
    paper_sample_count: int,
    shadow_sample_count: int,
    production_sample_count: int,
    filled_count: int,
    synthetic_snapshot_ratio: float,
    historical_ohlc_coverage: float,
    reconciliation_pass_ratio: float,
    continuous_replay_available: bool,
) -> None:
    suffix = as_of.isoformat()
    windows = {
        str(window): {
            "window_days": window,
            "evaluation_status": "OBSERVE_ONLY",
            "summary": {
                "sample_count": paper_sample_count,
                "filled_count": filled_count,
                "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
                "historical_ohlc_coverage": historical_ohlc_coverage,
                "reconciliation_pass_ratio": reconciliation_pass_ratio,
            },
        }
        for window in (7, 14, 30)
    }
    _write_json(
        reports_dir / f"paper_signal_quality_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "paper_signal_quality",
            "as_of": suffix,
            "evaluation_status": "OBSERVE_ONLY",
            "production_effect": "none",
            "summary": windows["30"]["summary"],
            "windows": windows,
        },
    )
    shadow_windows = {
        str(window): {
            "window_days": window,
            "impact_status": "SHADOW_PROMISING_BUT_LIMITED",
            "summary": {
                "sample_counts": {
                    "production": production_sample_count,
                    "shadow": shadow_sample_count,
                    "unknown": 0,
                },
                "main_blocked_by": "none",
            },
            "profile_comparison": {
                "production": {
                    "sample_count": production_sample_count,
                    "filled_count": filled_count,
                    "paper_pnl_total": 10.0,
                },
                "shadow": {
                    "sample_count": shadow_sample_count,
                    "filled_count": filled_count,
                    "paper_pnl_total": 20.0,
                    "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
                    "historical_ohlc_coverage": historical_ohlc_coverage,
                    "reconciliation_pass_ratio": reconciliation_pass_ratio,
                },
            },
        }
        for window in (7, 14, 30)
    }
    _write_json(
        reports_dir / f"shadow_parameter_impact_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "shadow_parameter_impact",
            "as_of": suffix,
            "impact_status": "SHADOW_PROMISING_BUT_LIMITED",
            "production_effect": "none",
            "summary": shadow_windows["30"]["summary"],
            "profile_comparison": shadow_windows["30"]["profile_comparison"],
            "continuous_replay": {
                "available": continuous_replay_available,
                "replay_mode": (
                    "continuous_portfolio" if continuous_replay_available else "daily_independent"
                ),
                "portfolio_carry_forward": continuous_replay_available,
                "profiles": {
                    "production": {
                        "available": continuous_replay_available,
                        "final_equity": 100000.0,
                        "max_drawdown_pct": -0.02,
                        "exposure_peak": 50000.0,
                        "max_position_concentration": 0.25,
                    },
                    "shadow": {
                        "available": continuous_replay_available,
                        "final_equity": 100100.0,
                        "max_drawdown_pct": -0.02,
                        "exposure_peak": 50000.0,
                        "max_position_concentration": 0.25,
                    },
                },
            },
            "windows": shadow_windows,
        },
    )


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    _write_json(
        metadata_path,
        {
            "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
            "as_of": as_of.isoformat(),
            "generated_at": _fixed_generated_at().isoformat(),
            "project_root": str(tmp_path),
            "status": "PASS",
            "started_at": _fixed_generated_at().isoformat(),
            "finished_at": _fixed_generated_at().isoformat(),
            "visibility_cutoff": "2026-05-18T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return metadata_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 18, 21, 30, tzinfo=UTC)
