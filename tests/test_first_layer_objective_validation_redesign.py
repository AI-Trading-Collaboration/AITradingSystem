from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_objective_validation_redesign
from ai_trading_system.cli_commands.research_trends import trends_app


def test_objective_validation_redesign_defines_terms_and_blocks_promotion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        first_layer_objective_validation_redesign,
        "validate_cached_market_data",
        lambda **_: {
            "status": "PASS",
            "passed": True,
            "checked_at": "2026-06-28T00:00:00+00:00",
            "as_of": "2026-06-26",
            "price_path": str(tmp_path / "prices.csv"),
            "rates_path": str(tmp_path / "rates.csv"),
            "secondary_prices_path": "",
            "expected_price_tickers": ["QQQ", "SPY", "SMH"],
            "expected_rate_series": [],
            "price_row_count": 100,
            "rate_row_count": 0,
            "price_checksum": "fixture",
            "rate_checksum": "fixture",
            "warning_count": 0,
            "error_count": 0,
        },
    )

    runner = (
        first_layer_objective_validation_redesign.run_first_layer_objective_validation_redesign_pack
    )
    payload = runner(
        policy_path=_policy(tmp_path),
        current_state_summary_path=_current_state(tmp_path),
        failure_taxonomy_path=_failure_taxonomy(tmp_path),
        benchmark_consistency_path=_benchmark_consistency(tmp_path),
        proxy_audit_path=_proxy_audit(tmp_path),
        prices_path=tmp_path / "prices.csv",
        rates_path=tmp_path / "rates.csv",
        marketstack_prices_path=None,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        as_of_date=date(2026, 6, 26),
    )

    terms = {row["term_id"]: row for row in payload["objective_terms"]}
    stress = payload["stress_slice_minimum_requirements"]

    assert set(terms) == set(first_layer_objective_validation_redesign.OBJECTIVE_TERM_IDS)
    assert terms["false_risk_on_cost"]["current_baseline_value"] == 3
    assert terms["false_risk_off_cost"]["current_baseline_value"] == 5
    assert terms["drawdown_warning_lead_time"]["current_baseline_value"] == 5
    assert terms["recovery_delay_days"]["current_baseline_value"] == 5
    assert terms["regime_flip_penalty"]["current_baseline_value"] == 1.25
    assert terms["benchmark_consistency_score"]["current_baseline_value"] == 0.42
    assert stress["blocked_slice_ids"] == ["2022_bear_rate_shock"]
    assert stress["stress_validation_allowed"] is False
    assert payload["proxy_replacement_status"]["true_breadth_replaced"] is False
    assert payload["summary"]["validation_ready"] is False
    assert payload["promotion_allowed"] is False
    assert Path(
        payload["artifact_paths"]["first_layer_objective_validation_redesign_json"]
    ).exists()
    assert Path(
        payload["artifact_paths"]["first_layer_objective_validation_redesign_doc"]
    ).exists()


def test_first_layer_objective_validation_redesign_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-objective-validation-redesign" in result.output


def _policy(tmp_path: Path) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(
        """
schema_version: test
policy_id: first_layer_objective_validation_policy_v1
requested_window:
  start: '2022-12-01'
  end: latest
data_quality_expected_price_tickers: [QQQ, SPY, SMH]
objective_terms:
  false_risk_on_cost:
    definition: false risk-on cost
    measurement_source: first_layer_failure_taxonomy.failure_type.false_risk_on.event_count
    direction: minimize
    validation_role: primary_downside_cost
    promotion_interpretation: diagnostic_contract_only_not_promotion
  false_risk_off_cost:
    definition: false risk-off cost
    measurement_source: first_layer_failure_taxonomy.failure_type.false_risk_off.event_count
    direction: minimize
    validation_role: missed_upside_cost
    promotion_interpretation: diagnostic_contract_only_not_promotion
  drawdown_warning_lead_time:
    definition: drawdown warning lead time
    measurement_source: policy.minimum_warning_lead_days plus late_risk_off.event_count
    direction: maximize_lead_time_and_minimize_late_events
    validation_role: drawdown_warning_objective
    promotion_interpretation: diagnostic_contract_only_not_promotion
    minimum_warning_lead_days: 5
  recovery_delay_days:
    definition: recovery delay days
    measurement_source: policy.maximum_recovery_delay_days plus late_risk_on.event_count
    direction: minimize_delay_and_late_events
    validation_role: recovery_reentry_objective
    promotion_interpretation: diagnostic_contract_only_not_promotion
    maximum_recovery_delay_days: 5
  regime_flip_penalty:
    definition: regime flip penalty
    measurement_source: first_layer_failure_taxonomy.signal_summary.regime_flip_rate
    direction: minimize
    validation_role: stability_penalty
    promotion_interpretation: diagnostic_contract_only_not_promotion
  benchmark_consistency_score:
    definition: benchmark consistency score
    measurement_source: benchmark_consistency_report.summary.benchmark_consistency_score
    direction: maximize
    validation_role: cross_benchmark_consistency_check
    promotion_interpretation: diagnostic_contract_only_not_promotion
  stress_slice_minimum_requirements:
    definition: stress slice minimum requirements
    measurement_source: first_layer_current_state_summary.regime_slices
    direction: satisfy_all_required_slices
    validation_role: stress_and_regime_coverage_gate
    promotion_interpretation: diagnostic_contract_only_not_promotion
stress_slice_minimum_requirements:
  minimum_signal_observations: 60
  required_coverage_status: SIGNAL_COVERED
  required_slice_ids:
    - 2022_bear_rate_shock
    - 2023_recovery
  primary_ai_conclusion_slice_ids:
    - 2023_recovery
  stress_validation_requires_2022_signal_coverage: true
""",
        encoding="utf-8",
    )
    return path


def _current_state(tmp_path: Path) -> Path:
    path = tmp_path / "current_state.yaml"
    path.write_text(
        """
schema_version: test
actual_signal_start: '2023-02-22'
actual_signal_end: '2026-03-27'
summary:
  actual_signal_start: '2023-02-22'
  actual_signal_end: '2026-03-27'
regime_slices:
  - slice_id: 2022_bear_rate_shock
    label: 2022 bear / rate shock
    role: stress_comparison_not_primary_ai_conclusion
    coverage_status: NO_SIGNAL_COVERAGE
    signal_observation_count: 0
  - slice_id: 2023_recovery
    label: 2023 recovery
    role: ai_after_chatgpt_recovery
    coverage_status: SIGNAL_COVERED
    signal_observation_count: 100
""",
        encoding="utf-8",
    )
    return path


def _failure_taxonomy(tmp_path: Path) -> Path:
    path = tmp_path / "failure_taxonomy.json"
    path.write_text(
        """
{
  "failure_taxonomy": [
    {
      "failure_type": "false_risk_on",
      "event_count": 3,
      "benchmark_event_counts": {"QQQ": 1, "SPY": 1, "SMH": 1}
    },
    {
      "failure_type": "false_risk_off",
      "event_count": 5,
      "benchmark_event_counts": {"QQQ": 2, "SPY": 2, "SMH": 1}
    },
    {
      "failure_type": "late_risk_off",
      "event_count": 2,
      "benchmark_event_counts": {"QQQ": 1, "SPY": 1, "SMH": 0}
    },
    {
      "failure_type": "late_risk_on",
      "event_count": 4,
      "benchmark_event_counts": {"QQQ": 2, "SPY": 1, "SMH": 1}
    }
  ],
  "signal_summary": {
    "regime_flip_count": 10,
    "regime_flip_rate_per_20_observations": 1.25
  }
}
""",
        encoding="utf-8",
    )
    return path


def _benchmark_consistency(tmp_path: Path) -> Path:
    path = tmp_path / "benchmark_consistency.json"
    path.write_text(
        """
{
  "summary": {
    "benchmark_consistency_score": 0.42,
    "benchmark_consistency_status": "CORE_BENCHMARKS_AVAILABLE",
    "core_benchmarks_available": ["QQQ", "SPY", "SMH"],
    "optional_benchmarks_missing": ["IWM", "RSP"]
  },
  "consistency_by_failure_type": []
}
""",
        encoding="utf-8",
    )
    return path


def _proxy_audit(tmp_path: Path) -> Path:
    path = tmp_path / "proxy_audit.yaml"
    path.write_text(
        """
status: FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED
summary:
  proxy_count: 2
  replacement_for_true_breadth_count: 0
  true_breadth_replaced: false
rows:
  - proxy_id: smh_to_qqq
    replacement_for_true_breadth: false
  - proxy_id: alpha_vantage_listing_status
    replacement_for_true_breadth: false
""",
        encoding="utf-8",
    )
    return path
