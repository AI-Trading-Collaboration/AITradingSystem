from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target

TARGET_AS_OF = date(2026, 1, 5)
EVALUATION_AS_OF = date(2026, 1, 8)
BACKFILL_START = date(2022, 12, 1)
BACKFILL_END = date(2024, 2, 29)


def write_model_target_config(tmp_path: Path) -> Path:
    advisory_path = tmp_path / "position_advisory_v1.yaml"
    advisory_path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "advisory_limits:",
                "  max_single_day_total_adjustment: 0.12",
                "  max_single_symbol_adjustment: 0.06",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "model_target_portfolio_v1.yaml"
    config_path.write_text(
        f"""
schema_version: 1
model_target:
  name: test_dynamic_v3_rescue_research_model_target_v1
  mode: research_target_only
  not_official_target_weights: true
  paper_shadow_only: true
source:
  shadow_shortlist: latest
  candidate_cluster: latest
  position_advisory_config: {advisory_path.as_posix()}
  smoothed_limited_config: {system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.as_posix()}
target_methods:
  enabled:
    - static_baseline
    - no_trade_baseline
    - consensus_target
    - limited_adjustment
    - smooth_weights_3d_limited_adjustment
    - smooth_weights_5d_limited_adjustment
    - risk_capped_limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - selected_top_candidate
baseline:
  static_weights:
    QQQ: 0.50
    SMH: 0.20
    TLT: 0.10
    CASH: 0.20
method_policy:
  defensive_limited_adjustment:
    semiconductor_symbols:
      - SMH
      - SOXX
    growth_symbols:
      - QQQ
    semiconductor_reduction: 0.03
    growth_reduction: 0.02
    max_cash_weight: 0.35
  review_policy:
    preferred_method_order:
      - limited_adjustment
      - defensive_limited_adjustment
      - consensus_target
constraints:
  max_single_symbol_weight: 0.65
  max_semiconductor_weight: 0.35
  min_cash_weight: 0.05
  max_total_risk_asset_weight: 0.95
  semiconductor_symbols:
    - SMH
    - SOXX
  defensive_symbols:
    - CASH
    - TLT
safety:
  research_target_only: true
  paper_shadow_only: true
  not_official_target_weights: true
  broker_action_allowed: false
  broker_action_taken: false
  order_ticket_generated: false
  production_effect: none
  auto_apply: false
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def write_paper_shadow_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "paper_shadow_account_v1.yaml"
    config_path.write_text(
        """
schema_version: 1
paper_shadow_account:
  name: test_dynamic_v3_rescue_paper_shadow_account_v1
  mode: paper_shadow_only
  base_currency: USD
  initial_equity: 100000
  start_date: "2022-12-01"
  initial_method: static_baseline
tracking:
  target_methods:
    - static_baseline
    - no_trade_baseline
    - consensus_target
    - limited_adjustment
    - smooth_weights_3d_limited_adjustment
    - smooth_weights_5d_limited_adjustment
    - risk_capped_limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - selected_top_candidate
baseline:
  static_weights:
    QQQ: 0.50
    SMH: 0.20
    TLT: 0.10
    CASH: 0.20
safety:
  research_target_only: true
  paper_shadow_only: true
  not_official_target_weights: true
  broker_action_allowed: false
  broker_action_taken: false
  production_effect: none
  order_ticket_generated: false
  auto_apply: false
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def write_target_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    daily_dir = tmp_path / "position_advisory_daily" / "daily-1"
    daily_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        {
            "candidate_id": "candidate-a",
            "shortlist_rank": 1,
            "shortlist_score": 0.91,
            "target_weights": {
                "QQQ": 0.55,
                "SMH": 0.24,
                "SOXX": 0.04,
                "TLT": 0.05,
                "CASH": 0.12,
            },
        },
        {
            "candidate_id": "candidate-b",
            "shortlist_rank": 2,
            "shortlist_score": 0.84,
            "target_weights": {
                "QQQ": 0.49,
                "SMH": 0.18,
                "SOXX": 0.08,
                "TLT": 0.10,
                "CASH": 0.15,
            },
        },
    ]
    _write_jsonl(daily_dir / "daily_candidate_targets.jsonl", candidates)
    (daily_dir / "daily_consensus_weights.csv").write_text(
        "\n".join(
            [
                "symbol,mean_target_weight",
                "QQQ,0.52",
                "SMH,0.21",
                "SOXX,0.06",
                "TLT,0.08",
                "CASH,0.13",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        daily_dir / "daily_advisory_actions.json",
        {"daily_advisory_id": "daily-1", "consensus_status": "PASS"},
    )

    monitor_dir = tmp_path / "shadow_monitor_runs" / "monitor-1"
    monitor_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        monitor_dir / "shadow_monitor_manifest.json",
        {"monitor_run_id": "monitor-1", "status": "PASS"},
    )
    _write_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl", candidates)

    shortlist_dir = tmp_path / "shadow_shortlist" / "shortlist-1"
    shortlist_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        shortlist_dir / "shadow_shortlist_manifest.json",
        {"shadow_shortlist_id": "shortlist-1", "status": "PASS"},
    )

    drift_dir = tmp_path / "consensus_drift" / "drift-1"
    drift_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        drift_dir / "consensus_drift_summary.json",
        {"drift_id": "drift-1", "disagreement_status": "LOW_DISAGREEMENT"},
    )
    return {
        "position_advisory_daily_dir": tmp_path / "position_advisory_daily",
        "shadow_monitor_dir": tmp_path / "shadow_monitor_runs",
        "shadow_shortlist_dir": tmp_path / "shadow_shortlist",
        "consensus_drift_dir": tmp_path / "consensus_drift",
    }


def build_model_target_fixture(tmp_path: Path) -> dict[str, Any]:
    source_dirs = write_target_source_artifacts(tmp_path)
    config_path = write_model_target_config(tmp_path)
    result = system_target.generate_model_target(
        config_path=config_path,
        as_of=TARGET_AS_OF,
        output_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 5, tzinfo=UTC),
        **source_dirs,
    )
    return {"config_path": config_path, **source_dirs, **result}


def build_rebalanced_shadow_fixture(tmp_path: Path) -> dict[str, Any]:
    target = build_model_target_fixture(tmp_path)
    paper_config = write_paper_shadow_config(tmp_path)
    paper = system_target.init_paper_shadow_account(
        config_path=paper_config,
        output_dir=tmp_path / "paper_shadow",
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 5, 1, tzinfo=UTC),
    )
    rebalance = system_target.simulate_model_rebalance(
        paper_shadow_id=paper["paper_shadow_id"],
        target_id=target["target_id"],
        paper_shadow_dir=tmp_path / "paper_shadow",
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "model_rebalance",
        generated_at=datetime(2026, 1, 5, 2, tzinfo=UTC),
    )
    return {
        "target": target,
        "paper_config_path": paper_config,
        "paper": paper,
        "rebalance": rebalance,
    }


def run_performance_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = build_rebalanced_shadow_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    performance = system_target.run_paper_shadow_performance(
        paper_shadow_id=fixture["paper"]["paper_shadow_id"],
        paper_shadow_dir=tmp_path / "paper_shadow",
        output_dir=tmp_path / "paper_shadow_performance",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        as_of=EVALUATION_AS_OF,
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )
    return {
        **fixture,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "performance": performance,
    }


def run_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_performance_fixture(tmp_path)
    review = system_target.build_system_target_review_pack(
        target_id=fixture["target"]["target_id"],
        paper_shadow_id=fixture["paper"]["paper_shadow_id"],
        performance_id=fixture["performance"]["performance_id"],
        model_target_dir=tmp_path / "model_target",
        paper_shadow_dir=tmp_path / "paper_shadow",
        performance_dir=tmp_path / "paper_shadow_performance",
        output_dir=tmp_path / "system_target_review",
        generated_at=datetime(2026, 1, 8, 1, tzinfo=UTC),
    )
    return {**fixture, "review": review}


def write_paper_shadow_backfill_config(
    tmp_path: Path,
    *,
    prices_path: Path,
) -> dict[str, Any]:
    source_dirs = write_target_source_artifacts(tmp_path)
    model_config = write_model_target_config(tmp_path)
    paper_config = write_paper_shadow_config(tmp_path)
    config_path = tmp_path / "paper_shadow_backfill_v1.yaml"
    config_path.write_text(
        f"""
schema_version: 1
backfill:
  mode: BACKTEST_SIMULATION
  research_target_only: true
  paper_shadow_only: true
  not_pit_safe: true
  not_official_target_weights: true
date_range:
  start: "{BACKFILL_START.isoformat()}"
  end: "{BACKFILL_END.isoformat()}"
  rebalance_frequency: weekly
  rebalance_day: MON
  min_history_days_before_first_rebalance: 20
source:
  model_target_config: {model_config.as_posix()}
  paper_shadow_config: {paper_config.as_posix()}
  position_advisory_daily_dir: {source_dirs["position_advisory_daily_dir"].as_posix()}
  shadow_monitor_dir: {source_dirs["shadow_monitor_dir"].as_posix()}
  shadow_shortlist_dir: {source_dirs["shadow_shortlist_dir"].as_posix()}
  consensus_drift_dir: {source_dirs["consensus_drift_dir"].as_posix()}
  risk_capped_limited_config: {system_target.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH.as_posix()}
  smoothed_limited_config: {system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.as_posix()}
  price_cache_path: {prices_path.as_posix()}
target_methods:
  enabled:
    - static_baseline
    - no_trade_baseline
    - consensus_target
    - limited_adjustment
    - smooth_weights_3d_limited_adjustment
    - smooth_weights_5d_limited_adjustment
    - risk_capped_limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - selected_top_candidate
costs:
  transaction_cost_bps: 0
  slippage_bps: 0
evaluation:
  min_observations_per_window: 10
regime_policy:
  min_sample_count: 2
  risk_off_return_threshold: -0.015
  tech_drawdown_return_threshold: -0.010
  semiconductor_pullback_return_threshold: -0.012
  ai_trend_return_threshold: 0.008
  strong_recovery_return_threshold: 0.012
stability_policy:
  large_jump_threshold: 0.10
  high_jump_threshold: 0.20
  stable_max_daily_weight_change: 0.08
  unstable_max_daily_weight_change: 0.18
  moderate_annualized_turnover: 1.5
  high_annualized_turnover: 4.0
selection_policy:
  preferred_method_order:
    - limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - consensus_target
  reference_only_methods:
    - consensus_target
  preferred_method_score_tolerance: 0.10
  continue_observation_score: 0.55
  review_required_score: 0.35
  score_weights:
    return: 0.25
    drawdown: 0.25
    risk_adjusted: 0.20
    regime: 0.15
    stability: 0.15
    turnover_penalty: 0.10
safety:
  research_target_only: true
  paper_shadow_only: true
  not_official_target_weights: true
  broker_action_allowed: false
  broker_action_taken: false
  order_ticket_generated: false
  production_effect: none
  auto_apply: false
""".lstrip(),
        encoding="utf-8",
    )
    return {
        "config_path": config_path,
        "model_config_path": model_config,
        "paper_config_path": paper_config,
        **source_dirs,
    }


def run_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    backfill = system_target.run_paper_shadow_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        generated_at=datetime(2024, 3, 1, tzinfo=UTC),
    )
    return {
        **config,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "backfill": backfill,
    }


def write_weight_experiment_matrix_config(
    tmp_path: Path,
    *,
    source_backfill_id: str,
) -> Path:
    payload = yaml.safe_load(
        system_target.DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["experiment_group"]["source_backfill_id"] = source_backfill_id
    config_path = tmp_path / "weight_experiment_matrix_v1.yaml"
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return config_path


def run_experiment_matrix_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    matrix_config = write_weight_experiment_matrix_config(
        tmp_path,
        source_backfill_id=fixture["backfill"]["backfill_id"],
    )
    matrix = system_target.build_experiment_matrix(
        config_path=matrix_config,
        output_dir=tmp_path / "experiment_matrix",
        generated_at=datetime(2024, 3, 2, tzinfo=UTC),
    )
    return {**fixture, "matrix_config_path": matrix_config, "matrix": matrix}


def run_batch_experiment_fixture(
    tmp_path: Path,
    *,
    force_promote: bool = False,
) -> dict[str, Any]:
    fixture = run_experiment_matrix_fixture(tmp_path)
    batch = system_target.run_batch_experiment(
        matrix_id=fixture["matrix"]["matrix_id"],
        matrix_dir=tmp_path / "experiment_matrix",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "batch_experiment",
        price_cache_path=fixture["prices_path"],
        rates_cache_path=fixture["rates_path"],
        generated_at=datetime(2024, 3, 3, tzinfo=UTC),
    )
    if force_promote:
        force_promotable_batch_metrics(batch["batch_dir"])
    return {**fixture, "batch": batch}


def run_experiment_triage_fixture(
    tmp_path: Path,
    *,
    force_promote: bool = True,
) -> dict[str, Any]:
    fixture = run_batch_experiment_fixture(tmp_path, force_promote=force_promote)
    triage = system_target.run_experiment_triage(
        batch_id=fixture["batch"]["batch_id"],
        batch_dir=tmp_path / "batch_experiment",
        matrix_dir=tmp_path / "experiment_matrix",
        output_dir=tmp_path / "experiment_triage",
        generated_at=datetime(2024, 3, 4, tzinfo=UTC),
    )
    return {**fixture, "triage": triage}


def run_top_variant_interpretation_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_experiment_triage_fixture(tmp_path)
    interpretation = system_target.run_top_variant_interpretation(
        triage_id=fixture["triage"]["triage_id"],
        triage_dir=tmp_path / "experiment_triage",
        matrix_dir=tmp_path / "experiment_matrix",
        output_dir=tmp_path / "top_variant_interpretation",
        generated_at=datetime(2024, 3, 5, tzinfo=UTC),
    )
    return {**fixture, "interpretation": interpretation}


def run_method_promotion_plan_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_top_variant_interpretation_fixture(tmp_path)
    promotion_plan = system_target.run_method_promotion_plan(
        triage_id=fixture["triage"]["triage_id"],
        interpretation_id=fixture["interpretation"]["interpretation_id"],
        triage_dir=tmp_path / "experiment_triage",
        interpretation_dir=tmp_path / "top_variant_interpretation",
        output_dir=tmp_path / "method_promotion_plan",
        generated_at=datetime(2024, 3, 6, tzinfo=UTC),
    )
    return {**fixture, "promotion_plan": promotion_plan}


def run_smoothed_review_chain_fixture(tmp_path: Path) -> dict[str, Any]:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    smoothed = system_target.run_smoothed_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "smoothed_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )
    risk_capped = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_capped_backfill",
        paper_shadow_backfill_dir=tmp_path / "risk_source_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )
    comparison = system_target.run_smoothed_comparison(
        smoothed_backfill_id=smoothed["smoothed_backfill_id"],
        baseline_backfill_id=smoothed["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_id=risk_capped["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_comparison",
    )
    review = system_target.build_smoothed_review_pack(
        comparison_id=comparison["comparison_id"],
        smoothed_backfill_id=smoothed["smoothed_backfill_id"],
        comparison_dir=tmp_path / "smoothed_comparison",
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review",
    )
    return {
        **config,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "smoothed": smoothed,
        "risk_capped": risk_capped,
        "comparison": comparison,
        "review": review,
    }


def run_smoothed_readiness_chain_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    attribution = system_target.run_smoothed_review_attribution(
        review_id=fixture["review"]["review_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        review_dir=tmp_path / "smoothed_review",
        comparison_dir=tmp_path / "smoothed_comparison",
        backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review_attribution",
    )
    benefit_lag = system_target.run_smoothing_benefit_lag_drilldown(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_dir=tmp_path / "smoothed_backfill",
        comparison_dir=tmp_path / "smoothed_comparison",
        output_dir=tmp_path / "smoothing_benefit_lag",
    )
    regime = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
    )
    confirmation = system_target.register_smoothed_confirmation_targets(
        review_id=fixture["review"]["review_id"],
        regime_validation_id=regime["regime_validation_id"],
        review_dir=tmp_path / "smoothed_review",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )
    watch = system_target.run_smoothed_watch_pack(
        review_attribution_id=attribution["attribution_id"],
        benefit_lag_id=benefit_lag["drilldown_id"],
        regime_validation_id=regime["regime_validation_id"],
        confirmation_id=confirmation["confirmation_id"],
        attribution_dir=tmp_path / "smoothed_review_attribution",
        benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        confirmation_dir=tmp_path / "smoothed_forward_confirmation",
        output_dir=tmp_path / "smoothed_watch_pack",
    )
    gap = system_target.run_smoothed_evidence_gap_diagnosis(
        benefit_lag_id=benefit_lag["drilldown_id"],
        regime_validation_id=regime["regime_validation_id"],
        watch_pack_id=watch["watch_pack_id"],
        benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        watch_pack_dir=tmp_path / "smoothed_watch_pack",
        output_dir=tmp_path / "smoothed_evidence_gap",
    )
    churn = system_target.run_smoothed_churn_backfill(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        baseline_backfill_id=fixture["smoothed"]["source_paper_shadow_backfill"][
            "backfill_id"
        ],
        risk_capped_backfill_id=fixture["risk_capped"]["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_churn_backfill",
    )
    sideways = system_target.run_sideways_mixed_attribution(
        regime_validation_id=regime["regime_validation_id"],
        churn_id=churn["churn_id"],
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        churn_dir=tmp_path / "smoothed_churn_backfill",
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "sideways_mixed_attribution",
    )
    scorecard = system_target.run_smoothed_readiness_scorecard(
        attribution_id=attribution["attribution_id"],
        benefit_lag_id=benefit_lag["drilldown_id"],
        churn_id=churn["churn_id"],
        sideways_attribution_id=sideways["sideways_attribution_id"],
        confirmation_id=confirmation["confirmation_id"],
        attribution_dir=tmp_path / "smoothed_review_attribution",
        benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
        churn_dir=tmp_path / "smoothed_churn_backfill",
        sideways_attribution_dir=tmp_path / "sideways_mixed_attribution",
        confirmation_dir=tmp_path / "smoothed_forward_confirmation",
        output_dir=tmp_path / "smoothed_readiness_scorecard",
    )
    owner_update = system_target.run_smoothed_owner_review_update(
        scorecard_id=scorecard["scorecard_id"],
        watch_pack_id=watch["watch_pack_id"],
        scorecard_dir=tmp_path / "smoothed_readiness_scorecard",
        watch_pack_dir=tmp_path / "smoothed_watch_pack",
        output_dir=tmp_path / "smoothed_owner_review_update",
    )
    return {
        **fixture,
        "attribution": attribution,
        "benefit_lag": benefit_lag,
        "regime": regime,
        "confirmation": confirmation,
        "watch": watch,
        "gap": gap,
        "churn": churn,
        "sideways": sideways,
        "scorecard": scorecard,
        "owner_update": owner_update,
    }


def run_smoothed_promotion_chain_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_smoothed_readiness_chain_fixture(tmp_path)
    promotion_review = system_target.build_smoothed_promotion_review_pack(
        readiness_scorecard_id=fixture["scorecard"]["scorecard_id"],
        owner_update_id=fixture["owner_update"]["owner_update_id"],
        watch_pack_id=fixture["watch"]["watch_pack_id"],
        scorecard_dir=tmp_path / "smoothed_readiness_scorecard",
        owner_update_dir=tmp_path / "smoothed_owner_review_update",
        watch_pack_dir=tmp_path / "smoothed_watch_pack",
        output_dir=tmp_path / "smoothed_promotion_review",
        generated_at=datetime(2024, 3, 7, tzinfo=UTC),
    )
    gate = system_target.run_primary_research_candidate_gate(
        promotion_review_id=promotion_review["promotion_review_id"],
        promotion_review_dir=tmp_path / "smoothed_promotion_review",
        output_dir=tmp_path / "primary_research_candidate_gate",
        generated_at=datetime(2024, 3, 8, tzinfo=UTC),
    )
    binding = system_target.run_smoothed_forward_binding(
        confirmation_id=fixture["confirmation"]["confirmation_id"],
        gate_id=gate["gate_id"],
        confirmation_dir=tmp_path / "smoothed_forward_confirmation",
        gate_dir=tmp_path / "primary_research_candidate_gate",
        output_dir=tmp_path / "smoothed_forward_binding",
        generated_at=datetime(2024, 3, 9, tzinfo=UTC),
    )
    switch_plan = system_target.build_paper_shadow_primary_switch_plan(
        gate_id=gate["gate_id"],
        binding_id=binding["binding_id"],
        gate_dir=tmp_path / "primary_research_candidate_gate",
        binding_dir=tmp_path / "smoothed_forward_binding",
        output_dir=tmp_path / "paper_shadow_primary_switch",
        generated_at=datetime(2024, 3, 10, tzinfo=UTC),
    )
    owner_promotion = system_target.create_smoothed_owner_promotion_decision(
        promotion_review_id=promotion_review["promotion_review_id"],
        gate_id=gate["gate_id"],
        switch_plan_id=switch_plan["switch_plan_id"],
        promotion_review_dir=tmp_path / "smoothed_promotion_review",
        gate_dir=tmp_path / "primary_research_candidate_gate",
        switch_plan_dir=tmp_path / "paper_shadow_primary_switch",
        output_dir=tmp_path / "smoothed_owner_promotion",
        generated_at=datetime(2024, 3, 11, tzinfo=UTC),
    )
    return {
        **fixture,
        "promotion_review": promotion_review,
        "gate": gate,
        "binding": binding,
        "switch_plan": switch_plan,
        "owner_promotion": owner_promotion,
    }


def force_promotable_batch_metrics(
    batch_dir: Path,
    *,
    promote_variant: str = "sideways_choppy_hold_previous",
    reject_variant: str = "cash_buffer_15",
) -> None:
    performance_path = batch_dir / "variant_performance_metrics.jsonl"
    stability_path = batch_dir / "variant_stability_metrics.jsonl"
    regime_path = batch_dir / "variant_regime_metrics.jsonl"
    performance = _read_jsonl(performance_path)
    stability = _read_jsonl(stability_path)
    regime = _read_jsonl(regime_path)

    for row in performance:
        if row["variant_id"] == promote_variant:
            row.update(
                {
                    "relative_to_limited_adjustment": 0.05,
                    "drawdown_delta_vs_limited": 0.02,
                    "turnover_delta_vs_limited": -0.2,
                    "performance_status": "PASS",
                }
            )
        if row["variant_id"] == reject_variant:
            row.update(
                {
                    "relative_to_limited_adjustment": 0.03,
                    "drawdown_delta_vs_limited": -0.02,
                    "turnover_delta_vs_limited": 0.0,
                    "performance_status": "FAIL",
                }
            )
    for row in stability:
        if row["variant_id"] == promote_variant:
            row.update(
                {
                    "rolling_consistency_delta": "IMPROVED",
                    "stability_status": "STABLE",
                    "large_jump_count": 0,
                    "avg_rebalance_turnover": 0.0,
                }
            )
        if row["variant_id"] == reject_variant:
            row.update(
                {
                    "rolling_consistency_delta": "MIXED",
                    "stability_status": "MODERATE",
                }
            )
    for row in regime:
        if row["variant_id"] == promote_variant:
            row.update(
                {
                    "relative_to_limited_adjustment": 0.01,
                    "drawdown_delta_vs_limited": 0.01,
                    "turnover_delta_vs_limited": -0.05,
                    "regime_status": "IMPROVED",
                }
            )
        if row["variant_id"] == reject_variant:
            row.update(
                {
                    "relative_to_limited_adjustment": 0.01,
                    "drawdown_delta_vs_limited": 0.0,
                    "turnover_delta_vs_limited": 0.0,
                    "regime_status": "MIXED",
                }
            )
    _write_jsonl(performance_path, performance)
    _write_jsonl(stability_path, stability)
    _write_jsonl(regime_path, regime)


def run_rolling_eval_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    rolling = system_target.run_paper_shadow_rolling_eval(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_rolling_eval",
        generated_at=datetime(2024, 3, 1, 1, tzinfo=UTC),
    )
    return {**fixture, "rolling": rolling}


def run_regime_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    regime = system_target.run_paper_shadow_regime_review(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_regime_review",
        generated_at=datetime(2024, 3, 1, 2, tzinfo=UTC),
    )
    return {**fixture, "regime": regime}


def run_stability_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    stability = system_target.run_paper_shadow_stability(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_stability",
        generated_at=datetime(2024, 3, 1, 3, tzinfo=UTC),
    )
    return {**fixture, "stability": stability}


def run_selection_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    rolling = system_target.run_paper_shadow_rolling_eval(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_rolling_eval",
        generated_at=datetime(2024, 3, 1, 1, tzinfo=UTC),
    )
    regime = system_target.run_paper_shadow_regime_review(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_regime_review",
        generated_at=datetime(2024, 3, 1, 2, tzinfo=UTC),
    )
    stability = system_target.run_paper_shadow_stability(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "paper_shadow_stability",
        generated_at=datetime(2024, 3, 1, 3, tzinfo=UTC),
    )
    selection = system_target.run_system_target_selection_review(
        backfill_id=fixture["backfill"]["backfill_id"],
        rolling_eval_id=rolling["rolling_eval_id"],
        regime_review_id=regime["regime_review_id"],
        stability_id=stability["stability_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "paper_shadow_regime_review",
        stability_dir=tmp_path / "paper_shadow_stability",
        output_dir=tmp_path / "system_target_selection_review",
        generated_at=datetime(2024, 3, 1, 4, tzinfo=UTC),
    )
    return {
        **fixture,
        "rolling": rolling,
        "regime": regime,
        "stability": stability,
        "selection": selection,
    }


def write_market_cache(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    prices_path = root / "prices_daily.csv"
    rates_path = root / "rates_daily.csv"
    symbols = ("QQQ", "SMH", "SOXX", "TLT")
    price_lines = ["date,ticker,open,high,low,close,adj_close,volume"]
    for symbol_index, symbol in enumerate(symbols):
        level = 100.0 + symbol_index
        for day_index, day in enumerate(("2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08")):
            level *= 1.002 + symbol_index * 0.0005 + day_index * 0.0001
            price_lines.append(
                f"{day},{symbol},{level:.4f},{level * 1.01:.4f},"
                f"{level * 0.99:.4f},{level:.4f},{level:.4f},1000000"
            )
    prices_path.write_text("\n".join(price_lines) + "\n", encoding="utf-8")
    rates_path.write_text(
        "\n".join(
            [
                "date,series,value",
                "2026-01-05,FEDFUNDS,4.0",
                "2026-01-06,FEDFUNDS,4.0",
                "2026-01-07,FEDFUNDS,4.0",
                "2026-01-08,FEDFUNDS,4.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return prices_path, rates_path


def write_long_market_cache(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    prices_path = root / "prices_daily.csv"
    rates_path = root / "rates_daily.csv"
    symbols = ("QQQ", "SMH", "SOXX", "TLT")
    levels = {symbol: 100.0 + index * 5.0 for index, symbol in enumerate(symbols)}
    price_lines = ["date,ticker,open,high,low,close,adj_close,volume"]
    rate_lines = ["date,series,value"]
    current = BACKFILL_START
    day_index = 0
    while current <= BACKFILL_END:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        rates = _scenario_returns(day_index)
        for symbol in symbols:
            levels[symbol] *= 1.0 + rates[symbol]
            close = levels[symbol]
            price_lines.append(
                f"{current.isoformat()},{symbol},{close:.4f},{close * 1.01:.4f},"
                f"{close * 0.99:.4f},{close:.4f},{close:.4f},1000000"
            )
        rate_lines.append(f"{current.isoformat()},FEDFUNDS,4.0")
        current += timedelta(days=1)
        day_index += 1
    prices_path.write_text("\n".join(price_lines) + "\n", encoding="utf-8")
    rates_path.write_text("\n".join(rate_lines) + "\n", encoding="utf-8")
    return prices_path, rates_path


def report_index_for_review_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    target_dir = fixture["target"]["target_dir"]
    paper_dir = fixture["paper"]["paper_shadow_dir"]
    rebalance_dir = fixture["rebalance"]["rebalance_dir"]
    performance_dir = fixture["performance"]["performance_dir"]
    review_dir = fixture["review"]["review_dir"]
    return {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_model_target",
                "latest_artifact_path": str(target_dir / "model_target_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow",
                "latest_artifact_path": str(paper_dir / "paper_shadow_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_model_rebalance",
                "latest_artifact_path": str(rebalance_dir / "model_rebalance_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow_performance",
                "latest_artifact_path": str(
                    performance_dir / "paper_shadow_performance_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_system_target_review",
                "latest_artifact_path": str(review_dir / "system_target_review_manifest.json"),
            },
        ]
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def _scenario_returns(day_index: int) -> dict[str, float]:
    if day_index % 61 == 0:
        return {"QQQ": -0.030, "SMH": -0.040, "SOXX": -0.042, "TLT": -0.005}
    if day_index % 47 == 0:
        return {"QQQ": -0.018, "SMH": -0.030, "SOXX": -0.032, "TLT": 0.002}
    if day_index % 53 == 0:
        return {"QQQ": -0.022, "SMH": -0.010, "SOXX": -0.012, "TLT": -0.002}
    if day_index % 43 == 0:
        return {"QQQ": 0.026, "SMH": 0.034, "SOXX": 0.036, "TLT": 0.004}
    if day_index % 17 == 0:
        return {"QQQ": 0.012, "SMH": 0.016, "SOXX": 0.018, "TLT": 0.000}
    return {
        "QQQ": 0.0012,
        "SMH": 0.0015,
        "SOXX": 0.0017,
        "TLT": 0.0003,
    }
