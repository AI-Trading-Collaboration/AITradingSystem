from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dynamic_v3_paper_tracking_helpers import write_market_cache

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay


def prepare_backtest_sim_environment(tmp_path: Path, monkeypatch: Any) -> dict[str, Path]:
    dynamic_root = tmp_path / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
    monkeypatch.setattr(sim, "DEFAULT_DYNAMIC_V3_RESEARCH_ROOT", dynamic_root)
    monkeypatch.setattr(replay, "DEFAULT_LATEST_POINTER_DIR", dynamic_root / "latest")
    prices_path, rates_path = write_market_cache(
        tmp_path / "market_cache",
        start="2026-05-01",
        end="2026-07-31",
    )
    position_config_path = tmp_path / "position_advisory_v1.yaml"
    position_config_path.write_text("schema_version: 1\n", encoding="utf-8")
    _write_shadow_shortlist(dynamic_root)
    config_path = _write_config(
        tmp_path,
        prices_path=prices_path,
        rates_path=rates_path,
        position_config_path=position_config_path,
    )
    return {
        "config_path": config_path,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "event_dir": tmp_path / "backtest_sim_events",
        "variant_dir": tmp_path / "backtest_sim_variants",
        "outcome_dir": tmp_path / "backtest_sim_outcome",
        "paper_dir": tmp_path / "backtest_sim_paper",
        "regime_dir": tmp_path / "backtest_sim_regime",
        "sensitivity_dir": tmp_path / "backtest_sim_sensitivity",
        "calibration_dir": tmp_path / "backtest_sim_calibration",
        "bridge_dir": tmp_path / "backtest_sim_forward_bridge",
        "interpretation_dir": tmp_path / "sim_interpretation",
        "risk_return_dir": tmp_path / "sim_risk_return",
        "defensive_validation_dir": tmp_path / "sim_defensive_validation",
        "proposal_review_dir": tmp_path / "advisory_proposal_review",
        "confirmation_plan_dir": tmp_path / "forward_confirmation_plan",
    }


def run_event_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    event = sim.generate_backtest_sim_events(
        config_path=paths["config_path"],
        output_dir=paths["event_dir"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
    )
    return {**paths, "event": event}


def run_variant_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_event_fixture(tmp_path, monkeypatch)
    variants = sim.generate_backtest_sim_variants(
        event_set_id=fixture["event"]["event_set_id"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["variant_dir"],
        generated_at=datetime(2026, 7, 31, 1, tzinfo=UTC),
    )
    return {**fixture, "variants": variants}


def run_outcome_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    outcome = sim.run_backtest_sim_outcome(
        variant_set_id=fixture["variants"]["variant_set_id"],
        variant_dir=fixture["variant_dir"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["outcome_dir"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 31, 2, tzinfo=UTC),
    )
    return {**fixture, "outcome": outcome}


def run_paper_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    paper = sim.run_backtest_sim_paper(
        variant_set_id=fixture["variants"]["variant_set_id"],
        variant="limited_adjustment",
        variant_dir=fixture["variant_dir"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["paper_dir"],
        generated_at=datetime(2026, 7, 31, 3, tzinfo=UTC),
    )
    return {**fixture, "paper": paper}


def run_regime_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    regime = sim.run_backtest_sim_regime_review(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["regime_dir"],
        generated_at=datetime(2026, 7, 31, 4, tzinfo=UTC),
    )
    return {**fixture, "regime": regime}


def run_sensitivity_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    sensitivity = sim.run_backtest_sim_sensitivity(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        variant_dir=fixture["variant_dir"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["sensitivity_dir"],
        generated_at=datetime(2026, 7, 31, 5, tzinfo=UTC),
    )
    return {**fixture, "sensitivity": sensitivity}


def run_calibration_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    regime = sim.run_backtest_sim_regime_review(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["regime_dir"],
        generated_at=datetime(2026, 7, 31, 6, tzinfo=UTC),
    )
    sensitivity = sim.run_backtest_sim_sensitivity(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        variant_dir=fixture["variant_dir"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["sensitivity_dir"],
        generated_at=datetime(2026, 7, 31, 7, tzinfo=UTC),
    )
    calibration = sim.run_backtest_sim_calibration_pack(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        sim_paper_id=fixture["paper"]["sim_paper_id"],
        regime_review_id=regime["regime_review_id"],
        sensitivity_id=sensitivity["sensitivity_id"],
        outcome_dir=fixture["outcome_dir"],
        paper_dir=fixture["paper_dir"],
        regime_dir=fixture["regime_dir"],
        sensitivity_dir=fixture["sensitivity_dir"],
        output_dir=fixture["calibration_dir"],
        generated_at=datetime(2026, 7, 31, 8, tzinfo=UTC),
    )
    return {
        **fixture,
        "regime": regime,
        "sensitivity": sensitivity,
        "calibration": calibration,
    }


def run_forward_bridge_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)
    bridge = sim.run_backtest_sim_forward_bridge(
        calibration_pack_id=fixture["calibration"]["calibration_pack_id"],
        calibration_dir=fixture["calibration_dir"],
        output_dir=fixture["bridge_dir"],
        generated_at=datetime(2026, 7, 31, 9, tzinfo=UTC),
    )
    return {**fixture, "bridge": bridge}


def run_sim_interpretation_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    interpretation = sim.run_sim_interpretation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        calibration_id=fixture["calibration"]["calibration_pack_id"],
        bridge_id=fixture["bridge"]["bridge_id"],
        outcome_dir=fixture["outcome_dir"],
        calibration_dir=fixture["calibration_dir"],
        bridge_dir=fixture["bridge_dir"],
        output_dir=fixture["interpretation_dir"],
        generated_at=datetime(2026, 7, 31, 10, tzinfo=UTC),
    )
    return {**fixture, "interpretation": interpretation}


def run_sim_risk_return_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    risk_return = sim.run_sim_risk_return(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["risk_return_dir"],
        generated_at=datetime(2026, 7, 31, 11, tzinfo=UTC),
    )
    return {**fixture, "risk_return": risk_return}


def run_sim_defensive_validation_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    defensive_validation = sim.run_sim_defensive_validation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["defensive_validation_dir"],
        generated_at=datetime(2026, 7, 31, 12, tzinfo=UTC),
    )
    return {**fixture, "defensive_validation": defensive_validation}


def run_advisory_proposal_review_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    interpretation = sim.run_sim_interpretation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        calibration_id=fixture["calibration"]["calibration_pack_id"],
        bridge_id=fixture["bridge"]["bridge_id"],
        outcome_dir=fixture["outcome_dir"],
        calibration_dir=fixture["calibration_dir"],
        bridge_dir=fixture["bridge_dir"],
        output_dir=fixture["interpretation_dir"],
        generated_at=datetime(2026, 7, 31, 10, tzinfo=UTC),
    )
    risk_return = sim.run_sim_risk_return(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["risk_return_dir"],
        generated_at=datetime(2026, 7, 31, 11, tzinfo=UTC),
    )
    defensive_validation = sim.run_sim_defensive_validation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["defensive_validation_dir"],
        generated_at=datetime(2026, 7, 31, 12, tzinfo=UTC),
    )
    proposal_review = sim.run_advisory_proposal_review(
        interpretation_id=interpretation["interpretation_id"],
        risk_return_id=risk_return["risk_return_id"],
        defensive_validation_id=defensive_validation["defensive_validation_id"],
        calibration_id=fixture["calibration"]["calibration_pack_id"],
        interpretation_dir=fixture["interpretation_dir"],
        risk_return_dir=fixture["risk_return_dir"],
        defensive_validation_dir=fixture["defensive_validation_dir"],
        calibration_dir=fixture["calibration_dir"],
        output_dir=fixture["proposal_review_dir"],
        generated_at=datetime(2026, 7, 31, 13, tzinfo=UTC),
    )
    return {
        **fixture,
        "interpretation": interpretation,
        "risk_return": risk_return,
        "defensive_validation": defensive_validation,
        "proposal_review": proposal_review,
    }


def run_forward_confirmation_plan_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_advisory_proposal_review_fixture(tmp_path, monkeypatch)
    confirmation_plan = sim.run_forward_confirmation_plan(
        proposal_review_id=fixture["proposal_review"]["proposal_review_id"],
        bridge_id=fixture["bridge"]["bridge_id"],
        proposal_review_dir=fixture["proposal_review_dir"],
        bridge_dir=fixture["bridge_dir"],
        output_dir=fixture["confirmation_plan_dir"],
        generated_at=datetime(2026, 7, 31, 14, tzinfo=UTC),
    )
    return {**fixture, "confirmation_plan": confirmation_plan}


def _write_config(
    tmp_path: Path,
    *,
    prices_path: Path,
    rates_path: Path,
    position_config_path: Path,
) -> Path:
    config_path = tmp_path / "backtest_simulation_advisory_v1.yaml"
    payload = {
        "schema_version": 1,
        "policy_metadata": {
            "policy_id": "backtest_simulation_advisory_v1_test",
            "owner": "tests",
            "version": "v1",
            "status": "test",
            "rationale": "Focused tests for non-PIT simulation artifacts.",
            "intended_effect": "Exercise backtest simulation without production mutation.",
            "validation_evidence": "Focused deterministic fixture validation.",
            "review_condition": "Test fixture only.",
        },
        "simulation": {
            "name": "dynamic_v3_backtest_simulation_advisory",
            "market_regime": "ai_after_chatgpt",
            "anchor_event": "ChatGPT public launch 2022-11-30",
            "outcome_mode": "BACKTEST_SIMULATION",
            "pit_safety_status": "SIMULATION_NOT_PIT",
            "not_for_production": True,
        },
        "date_range": {
            "start": "2026-06-01",
            "end": "2026-06-30",
            "event_frequency": "weekly",
            "event_day": "MON",
            "min_history_days_before_event": 5,
        },
        "source": {
            "shadow_shortlist_id": "shadow-shortlist-1",
            "position_advisory_config": str(position_config_path),
            "price_cache_path": str(prices_path),
            "rates_cache_path": str(rates_path),
            "use_adjusted_close": True,
        },
        "portfolio": {
            "baseline_snapshot": {"QQQ": 0.50, "SMH": 0.20, "TLT": 0.10, "CASH": 0.20},
            "cash_symbol": "CASH",
            "defensive_symbols": ["CASH", "TLT"],
            "semiconductor_symbols": ["SMH", "SOXX"],
        },
        "variants": {
            "enabled": [
                "no_trade",
                "consensus_target",
                "limited_adjustment",
                "defensive_limited_adjustment",
                "equal_weight_shadow_candidates",
            ]
        },
        "outcome_windows": {"trading_days": [1, 5, 10, 20]},
        "limits": {
            "max_single_event_total_adjustment": 0.10,
            "max_single_symbol_adjustment": 0.05,
            "min_trade_threshold": 0.01,
        },
        "consensus_policy": {
            "max_symbol_dispersion_for_high_consensus": 0.08,
            "max_average_dispersion_for_high_consensus": 0.04,
        },
        "regime_policy": {
            "qqq_symbol": "QQQ",
            "semiconductor_symbol": "SMH",
            "rates_proxy_symbol": "TLT",
            "ai_trend_qqq_60d_return_min": 0.08,
            "strong_recovery_qqq_20d_return_min": 0.04,
            "tech_drawdown_qqq_20d_return_max": -0.05,
            "semiconductor_pullback_20d_return_max": -0.06,
            "risk_off_tlt_20d_return_min": 0.03,
            "risk_off_qqq_20d_return_max": -0.03,
            "sideways_choppy_qqq_60d_abs_return_max": 0.03,
        },
        "sensitivity_policy": {
            "consensus_dispersion_thresholds": {"tight": 0.03, "base": 0.08},
            "adjustment_limit_grid": [0.05, 0.10, 0.15],
            "shortlist_top_n_grid": [1, 2],
            "event_frequency_profiles": ["weekly", "biweekly"],
            "min_available_windows_for_low_risk": 8,
            "max_regime_return_concentration_low_risk": 0.90,
            "high_risk_regime_return_concentration": 0.98,
            "max_parameter_result_spread_low_risk": 0.10,
            "high_risk_parameter_result_spread": 0.25,
        },
        "forward_confirmation": {
            "required_forward_events": 2,
            "win_rate_vs_no_trade_min": 0.55,
            "min_relative_return": 0.0,
            "avg_drawdown_delta_max": 0.0,
            "review_cadence": "weekly",
        },
        "safety": {
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "auto_policy_apply": False,
            "production_effect": "none",
            "production_candidate_generated": False,
            "require_report_label": "BACKTEST_SIMULATION_NOT_PIT",
        },
    }
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _write_shadow_shortlist(dynamic_root: Path) -> None:
    shortlist_dir = dynamic_root / "shadow_shortlist" / "shadow-shortlist-1"
    shortlist_dir.mkdir(parents=True, exist_ok=True)
    real_eval_root = dynamic_root / "real_evaluation"
    rows = []
    for rank, candidate_id in enumerate(["candidate-a", "candidate-b"], start=1):
        artifact_dir = real_eval_root / candidate_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = artifact_dir / "real_evaluation_manifest.json"
        manifest_path.write_text(
            json.dumps({"candidate_id": candidate_id}, sort_keys=True),
            encoding="utf-8",
        )
        _write_daily_weights(artifact_dir / "daily_weights.csv", candidate_id=candidate_id)
        rows.append(
            {
                "candidate_id": candidate_id,
                "shortlist_rank": rank,
                "real_evaluation_artifact_path": str(manifest_path),
            }
        )
    (shortlist_dir / "shadow_shortlist_candidates.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_daily_weights(path: Path, *, candidate_id: str) -> None:
    base = (
        {"QQQ": 0.44, "SMH": 0.26, "TLT": 0.10, "CASH": 0.20}
        if candidate_id == "candidate-a"
        else {"QQQ": 0.39, "SMH": 0.31, "SOXX": 0.10, "TLT": 0.05, "CASH": 0.15}
    )
    rows = []
    for day_value in pd.bdate_range("2026-05-01", "2026-07-31"):
        for symbol, weight in base.items():
            rows.append(
                {
                    "date": day_value.date().isoformat(),
                    "candidate_id": candidate_id,
                    "symbol": symbol,
                    "target_weight": weight,
                    "weight": weight,
                    "regime": "test_regime",
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
