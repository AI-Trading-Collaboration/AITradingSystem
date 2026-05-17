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
from ai_trading_system.trading_engine.reports.weight_candidate_evaluation import (
    ALLOWED_EVALUATION_STATUSES,
    build_weight_candidate_evaluation_payload,
    render_weight_candidate_evaluation_report,
    write_weight_candidate_evaluation_report,
)

FORBIDDEN_OUTPUT_TERMS = (
    "AUTO_PROMOTE",
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
    "APPROVED",
)


def test_missing_candidate_file_is_limited_insufficient_data(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_quality_inputs(reports_dir, as_of)

    payload = write_weight_candidate_evaluation_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["report_type"] == "weight_candidate_evaluation"
    assert payload["status"] == "LIMITED"
    assert payload["evaluation_status"] == "INSUFFICIENT_DATA"
    assert payload["production_effect"] == "none"
    assert payload["evaluation_mode"] == "observe_only"
    selected = payload["windows"]["30"]
    assert selected["candidate_count"] == 0
    assert selected["main_blocked_by"] == "missing_weight_adjustment_candidates"
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()


def test_candidate_blocked_propagates_to_evaluation_blocker(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of, blocked=True)
    _write_quality_inputs(reports_dir, as_of)

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["blocked"] is True
    assert "candidate_blocked" in candidate["blocked_by"]
    assert "manual_approval_required" in candidate["blocked_by"]
    assert candidate["evaluation_status"] == "CANDIDATE_PROMISING_BUT_LIMITED"
    assert candidate["evaluation_status"] in ALLOWED_EVALUATION_STATUSES


def test_manual_approval_required_is_always_preserved(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(
        reports_dir,
        as_of,
        blocked=False,
        blocked_by=[],
        required_validations=["manual_owner_review"],
    )
    _write_quality_inputs(reports_dir, as_of)

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert "manual_approval_required" in candidate["blocked_by"]
    assert candidate["blocked"] is True


def test_shadow_impact_insufficient_blocks_candidate(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(
        reports_dir,
        as_of,
        shadow_status="INSUFFICIENT_DATA",
        shadow_sample_count=2,
    )

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["evaluation_status"] == "INSUFFICIENT_DATA"
    assert "shadow_impact_insufficient" in candidate["blocked_by"]


def test_high_synthetic_snapshot_ratio_is_low_data_quality(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of, synthetic_snapshot_ratio=0.80)

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert payload["evaluation_status"] == "LOW_DATA_QUALITY"
    assert candidate["evaluation_status"] == "LOW_DATA_QUALITY"
    assert "synthetic_snapshot_ratio_too_high" in candidate["blocked_by"]
    assert "low_data_quality" in candidate["blocked_by"]


def test_continuous_replay_missing_warns_and_blocks(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of, continuous_replay_available=False)

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert "continuous_replay_missing" in candidate["warnings"]
    assert "continuous_replay_missing" in candidate["blocked_by"]
    assert payload["windows"]["30"]["continuous_replay_available"] is False


def test_paper_signal_quality_unreliable_blocks_candidate(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of, paper_status="UNRELIABLE")

    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    candidate = payload["candidates"][0]
    assert candidate["evaluation_status"] == "CANDIDATE_UNRELIABLE"
    assert "paper_signal_quality_unreliable" in candidate["blocked_by"]


def test_forbidden_statuses_do_not_appear_in_outputs(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of)

    payload = write_weight_candidate_evaluation_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )
    markdown = render_weight_candidate_evaluation_report(payload)
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown

    for candidate in payload["candidates"]:
        assert candidate["evaluation_status"] in ALLOWED_EVALUATION_STATUSES
    for term in FORBIDDEN_OUTPUT_TERMS:
        assert term not in combined


def test_evaluation_is_read_only_and_does_not_trigger_brokers_or_runners(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of)
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
            raise AssertionError(f"weight candidate evaluation must not import {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_weight_candidate_evaluation_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["production_effect"] == "none"
    assert payload["safety_boundary"]["calls_ibkr"] is False
    assert payload["safety_boundary"]["runs_paper_runner"] is False
    assert payload["safety_boundary"]["runs_replay_runner"] is False
    assert context["production_profile"].read_text(encoding="utf-8") == before


def test_dashboard_reads_weight_candidate_evaluation_without_rerun(
    tmp_path: Path,
) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_candidate_file(reports_dir, as_of)
    _write_quality_inputs(reports_dir, as_of)
    before = context["production_profile"].read_text(encoding="utf-8")
    write_weight_candidate_evaluation_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
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

    summary = payload["weight_candidate_evaluation"]
    assert summary["evaluation_status"] == "CANDIDATE_PROMISING_BUT_LIMITED"
    assert summary["candidate_count"] == 1
    assert summary["evaluable_candidate_count"] == 1
    assert summary["top_candidate_id"] == "weight_adjustment_candidate:2026-05-18:test"
    assert summary["main_blocked_by"] == "manual_approval_required"
    assert summary["production_effect"] == "none"
    assert "Weight Candidate Evaluation" in html
    assert "weight_candidate_evaluation_2026-05-18.md" in html
    assert context["production_profile"].read_text(encoding="utf-8") == before


def _write_config_context(tmp_path: Path) -> dict[str, Path]:
    config_dir = tmp_path / "config"
    weights_dir = config_dir / "weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    policy = config_dir / "weight_candidate_evaluation_policy.yaml"
    policy.write_text(
        "\n".join(
            [
                "policy_id: weight_candidate_evaluation_policy",
                "version: 1",
                "status: pilot_baseline",
                "owner: system",
                "production_effect: none",
                "rationale: test policy",
                "intended_effect: test evaluation",
                "validation_evidence: unit tests",
                "review_condition: after forward samples",
                "thresholds:",
                "  minimum_paper_signal_sample_count: 7",
                "  minimum_shadow_sample_count: 7",
                "  minimum_production_baseline_count: 7",
                "  maximum_synthetic_snapshot_ratio: 0.25",
                "  minimum_historical_ohlc_coverage: 0.70",
                "  minimum_reconciliation_pass_ratio: 0.90",
                "  minimum_max_drawdown_delta: 0.0",
                "  maximum_exposure_delta: 0.0",
                "  maximum_concentration_delta: 0.0",
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
    shadow_profiles = weights_dir / "shadow_weight_profiles.yaml"
    shadow_profiles.write_text(
        "\n".join(
            [
                'version: "test-shadow"',
                'status: "pilot"',
                'production_effect: "none"',
                "profiles:",
                '  - profile_id: "shadow_alpha_tilt_v1"',
                '    version: "test-alpha"',
                '    status: "shadow"',
                '    production_effect: "none"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "policy": policy,
        "governance": governance,
        "production_profile": production_profile,
        "shadow_profiles": shadow_profiles,
    }


def _write_candidate_file(
    reports_dir: Path,
    as_of: date,
    *,
    blocked: bool = True,
    blocked_by: list[str] | None = None,
    required_validations: list[str] | None = None,
) -> None:
    suffix = as_of.isoformat()
    candidate_id = f"weight_adjustment_candidate:{suffix}:test"
    blockers = ["manual_approval_required"] if blocked_by is None else blocked_by
    validations = ["aits validate-data", "manual_owner_review"]
    if required_validations is not None:
        validations = required_validations
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
                "main_blocked_by": blockers[0] if blockers else "none",
                "production_effect": "none",
                "mode": "observe_only",
            },
            "candidate_gate": {
                "status": "BLOCKED",
                "blocked": True,
                "blocked_by": blockers,
            },
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "generated_at": _fixed_generated_at().isoformat(),
                    "mode": "observe_only",
                    "blocked": blocked,
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
                    "blocked_by": blockers,
                    "required_validations": validations,
                    "production_effect": "none",
                }
            ],
        },
    )
    (reports_dir / f"weight_adjustment_candidates_{suffix}.md").write_text(
        "# Weight Adjustment Candidate Generator\n\n- production_effect=none\n",
        encoding="utf-8",
    )


def _write_quality_inputs(
    reports_dir: Path,
    as_of: date,
    *,
    paper_status: str = "OBSERVE_ONLY",
    shadow_status: str = "SHADOW_PROMISING_BUT_LIMITED",
    paper_sample_count: int = 9,
    shadow_sample_count: int = 9,
    production_sample_count: int = 9,
    synthetic_snapshot_ratio: float = 0.0,
    historical_ohlc_coverage: float = 1.0,
    reconciliation_pass_ratio: float = 1.0,
    continuous_replay_available: bool = True,
) -> None:
    suffix = as_of.isoformat()
    windows = {
        str(window): {
            "window_days": window,
            "evaluation_status": paper_status,
            "summary": {
                "sample_count": paper_sample_count,
                "candidate_count": 9,
                "generated_intents": 9,
                "filled_count": 7,
                "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
                "historical_ohlc_coverage": historical_ohlc_coverage,
                "reconciliation_pass_ratio": reconciliation_pass_ratio,
                "primary_blocked_by": "none",
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
            "evaluation_status": paper_status,
            "production_effect": "none",
            "summary": windows["30"]["summary"],
            "windows": windows,
        },
    )
    shadow_windows = {
        str(window): {
            "window_days": window,
            "impact_status": shadow_status,
            "summary": {
                "sample_counts": {
                    "production": production_sample_count,
                    "shadow": shadow_sample_count,
                    "unknown": 0,
                },
                "main_blocked_by": "none",
            },
            "profile_comparison": _profile_comparison(
                production_sample_count=production_sample_count,
                shadow_sample_count=shadow_sample_count,
                synthetic_snapshot_ratio=synthetic_snapshot_ratio,
                historical_ohlc_coverage=historical_ohlc_coverage,
                reconciliation_pass_ratio=reconciliation_pass_ratio,
            ),
        }
        for window in (7, 14, 30)
    }
    _write_json(
        reports_dir / f"shadow_parameter_impact_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "shadow_parameter_impact",
            "as_of": suffix,
            "impact_status": shadow_status,
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


def _profile_comparison(
    *,
    production_sample_count: int,
    shadow_sample_count: int,
    synthetic_snapshot_ratio: float,
    historical_ohlc_coverage: float,
    reconciliation_pass_ratio: float,
) -> dict[str, dict[str, Any]]:
    return {
        "production": {
            "sample_count": production_sample_count,
            "filled_count": 7,
            "paper_pnl_total": 10.0,
            "synthetic_snapshot_ratio": 0.0,
            "historical_ohlc_coverage": 1.0,
            "reconciliation_pass_ratio": 1.0,
        },
        "shadow": {
            "sample_count": shadow_sample_count,
            "filled_count": 7,
            "paper_pnl_total": 20.0,
            "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
            "historical_ohlc_coverage": historical_ohlc_coverage,
            "reconciliation_pass_ratio": reconciliation_pass_ratio,
        },
        "unknown": {"sample_count": 0},
    }


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
