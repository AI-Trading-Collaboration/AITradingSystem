from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.reports.weight_adjustment_candidates import (
    build_weight_adjustment_candidates_payload,
    render_weight_adjustment_candidates_report,
    write_weight_adjustment_candidates_report,
)

FORBIDDEN_OUTPUT_TERMS = ("AUTO_PROMOTE", "LIVE_READY", "SHOULD_TRADE")


def test_missing_inputs_generate_limited_blocked_candidate(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)

    payload = write_weight_adjustment_candidates_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["report_type"] == "weight_adjustment_candidates"
    assert payload["mode"] == "observe_only"
    assert payload["production_effect"] == "none"
    assert payload["gate_status"] == "LIMITED"
    assert payload["candidate_count"] == 1
    candidate = payload["candidates"][0]
    assert candidate["blocked"] is True
    assert candidate["gate_status"] == "LIMITED"
    assert "missing_daily_decision_summary" in candidate["blocked_by"]
    assert "manual_approval_required" in candidate["blocked_by"]
    assert candidate["production_effect"] == "none"
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()


def test_production_effect_is_none_for_payload_and_all_candidates(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)

    payload = build_weight_adjustment_candidates_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["production_effect"] == "none"
    assert payload["evaluation_scope"]["changes_production_parameters"] is False
    assert payload["evaluation_scope"]["triggers_trade"] is False
    assert all(candidate["production_effect"] == "none" for candidate in payload["candidates"])
    assert all(candidate["mode"] == "observe_only" for candidate in payload["candidates"])


def test_generator_does_not_modify_production_profile(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)
    before = context["production_profile"].read_text(encoding="utf-8")

    write_weight_adjustment_candidates_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    after = context["production_profile"].read_text(encoding="utf-8")
    assert after == before


def test_candidates_always_include_manual_approval_blocker(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)

    payload = build_weight_adjustment_candidates_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["gate_status"] == "BLOCKED"
    for candidate in payload["candidates"]:
        assert "manual_approval_required" in candidate["blocked_by"]
        assert candidate["blocked"] is True


def test_single_day_weight_change_does_not_exceed_policy_threshold(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)

    payload = build_weight_adjustment_candidates_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )

    threshold = payload["thresholds_snapshot"]["max_single_day_weight_delta"]
    candidate = payload["candidates"][0]
    deltas = [abs(change["delta"]) for change in candidate["parameter_changes"]]
    assert deltas
    assert max(deltas) <= threshold + 1e-12
    target_weights = candidate["target_profile"]["weights"]
    assert (
        abs(sum(target_weights.values()) - 1.0)
        <= payload["thresholds_snapshot"]["total_weight_tolerance"]
    )
    assert candidate["expected_effect"]["keeps_total_weight_balanced"] is True
    assert candidate["expected_effect"]["uses_single_day_pnl_to_increase_weight"] is False


def test_forbidden_terms_do_not_appear_in_outputs(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)

    payload = write_weight_adjustment_candidates_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        parameter_governance_path=context["governance"],
        production_profile_path=context["production_profile"],
        shadow_profiles_path=context["shadow_profiles"],
        generated_at=_fixed_generated_at(),
    )
    markdown = render_weight_adjustment_candidates_report(payload)
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown

    for term in FORBIDDEN_OUTPUT_TERMS:
        assert term not in combined


def test_dashboard_reads_candidates_without_applying_parameters(tmp_path: Path) -> None:
    context = _write_config_context(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 18)
    _write_required_inputs(reports_dir, as_of)
    before = context["production_profile"].read_text(encoding="utf-8")
    write_weight_adjustment_candidates_report(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=context["policy"],
        replay_json_path=reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
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

    summary = payload["weight_adjustment_candidates"]
    assert summary["candidate_count"] == 1
    assert summary["top_candidate_id"].startswith("weight_adjustment_candidate:2026-05-18:")
    assert summary["gate_status"] == "BLOCKED"
    assert summary["production_effect"] == "none"
    assert "Weight Adjustment Candidate" in html
    assert "weight_adjustment_candidates_2026-05-18.md" in html
    assert context["production_profile"].read_text(encoding="utf-8") == before


def _write_config_context(tmp_path: Path) -> dict[str, Path]:
    config_dir = tmp_path / "config"
    weights_dir = config_dir / "weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    policy = config_dir / "weight_adjustment_candidate_policy.yaml"
    policy.write_text(
        "\n".join(
            [
                "policy_id: weight_adjustment_candidate_policy",
                "version: 1",
                "status: pilot_baseline",
                "owner: system",
                "production_effect: none",
                "rationale: test policy",
                "intended_effect: test candidate generation",
                "validation_evidence: unit tests",
                "review_condition: after forward samples",
                "thresholds:",
                "  max_single_day_weight_delta: 0.02",
                "  target_total_weight_sum: 1.0",
                "  total_weight_tolerance: 0.000001",
                "  minimum_paper_signal_sample_count: 7",
                "  minimum_shadow_sample_count: 7",
                "  minimum_production_baseline_count: 7",
                "  maximum_synthetic_snapshot_ratio: 0.25",
                "required_validations:",
                "  - aits validate-data",
                "  - paper_signal_quality_gate",
                "  - shadow_parameter_impact_gate",
                "  - continuous_portfolio_replay",
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
                "    target_weights:",
                "      trend: 0.35",
                "      fundamentals: 0.30",
                "      macro_liquidity: 0.12",
                "      risk_sentiment: 0.11",
                "      valuation: 0.06",
                "      policy_geopolitics: 0.06",
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


def _write_required_inputs(reports_dir: Path, as_of: date) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    suffix = as_of.isoformat()
    _write_json(
        reports_dir / f"daily_decision_summary_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "daily_decision_summary",
            "as_of": suffix,
            "production_effect": "none",
            "status": "ready",
            "data_gate": {"status": "PASS", "blocking_reasons": []},
            "investment_conclusion": {"action_bias": "观察"},
        },
    )
    _write_json(
        reports_dir / f"paper_signal_quality_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "paper_signal_quality",
            "as_of": suffix,
            "evaluation_status": "OBSERVE_ONLY",
            "production_effect": "none",
            "summary": {
                "sample_count": 9,
                "primary_blocked_by": "none",
                "synthetic_snapshot_ratio": 0.0,
            },
            "evaluation_gate": {"blocked_by": []},
        },
    )
    _write_json(
        reports_dir / f"shadow_parameter_impact_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "shadow_parameter_impact",
            "as_of": suffix,
            "impact_status": "SHADOW_PROMISING_BUT_LIMITED",
            "production_effect": "none",
            "summary": {
                "sample_counts": {"production": 9, "shadow": 9, "unknown": 0},
                "main_blocked_by": "none",
            },
            "candidate_gate": {},
            "impact_gate": {"blocking_reasons": []},
            "continuous_replay": {
                "available": True,
                "replay_mode": "continuous_portfolio",
                "portfolio_carry_forward": True,
            },
        },
    )
    _write_json(
        reports_dir / "paper_trading_replay_2026-05-12_2026-05-18.json",
        {
            "schema_version": 1,
            "report_type": "paper_trading_replay",
            "start": "2026-05-12",
            "end": "2026-05-18",
            "replay_mode": "continuous_portfolio",
            "portfolio_carry_forward": True,
            "production_effect": "none",
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
