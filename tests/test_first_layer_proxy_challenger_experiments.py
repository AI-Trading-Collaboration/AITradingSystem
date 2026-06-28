from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_proxy_challenger_experiments
from ai_trading_system.cli_commands.research_trends import trends_app


def test_proxy_challenger_experiments_separate_validation_from_promotion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        first_layer_proxy_challenger_experiments,
        "validate_cached_market_data",
        lambda **_: {
            "status": "PASS",
            "passed": True,
            "checked_at": "2026-06-28T00:00:00+00:00",
            "as_of": "2026-06-26",
            "price_path": str(tmp_path / "prices.csv"),
            "rates_path": str(tmp_path / "rates.csv"),
            "secondary_prices_path": "",
            "expected_price_tickers": ["QQQ", "SPY", "SMH", "SOXX"],
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
        first_layer_proxy_challenger_experiments.run_first_layer_proxy_challenger_experiments_pack
    )
    payload = runner(
        policy_path=_policy(tmp_path),
        current_state_summary_path=_current_state(tmp_path),
        objective_validation_path=_objective_validation(tmp_path),
        proxy_coverage_audit_path=_proxy_audit(tmp_path),
        prices_path=tmp_path / "prices.csv",
        rates_path=tmp_path / "rates.csv",
        marketstack_prices_path=None,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        as_of_date=date(2026, 6, 26),
    )

    rows = {row["experiment_id"]: row for row in payload["experiments"]}

    assert set(rows) == set(first_layer_proxy_challenger_experiments.REQUIRED_EXPERIMENT_IDS)
    assert payload["summary"]["validation_ready_count"] == 4
    assert payload["summary"]["promotion_allowed_count"] == 0
    assert rows["baseline"]["validation_ready"] is True
    assert rows["baseline"]["promotion_allowed"] is False
    assert rows["volatility_regime"]["validation_ready"] is True
    assert rows["risk_appetite"]["validation_ready"] is True
    assert rows["equal_cap_weight_divergence"]["validation_ready"] is False
    assert rows["equal_cap_weight_divergence"]["missing_proxy_ids"] == ["rsp_to_spy", "qqqe_to_qqq"]
    assert rows["combined_proxy"]["validation_ready"] is False
    assert rows["combined_proxy"]["promotion_allowed"] is False
    assert "PROXIES_ARE_NOT_TRUE_PIT_BREADTH" in rows["risk_appetite"]["promotion_blockers"]
    assert Path(
        payload["artifact_paths"]["first_layer_proxy_challenger_experiments_json"]
    ).exists()
    assert Path(
        payload["artifact_paths"]["first_layer_proxy_challenger_experiments_doc"]
    ).exists()


def test_first_layer_proxy_challenger_experiments_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-proxy-challenger-experiments" in result.output


def _policy(tmp_path: Path) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(
        """
schema_version: test
policy_id: first_layer_proxy_challenger_experiments_policy_v1
requested_window:
  start: '2022-12-01'
  end: latest
data_quality_expected_price_tickers: [QQQ, SPY, SMH, SOXX]
validation_ready_scope: offline_challenger_experiment_only_not_promotion
experiment_definitions:
  - experiment_id: baseline
    required_proxy_ids: []
    target_objective_terms: [false_risk_on_cost, false_risk_off_cost]
    expected_signal_role: frozen_baseline_control
  - experiment_id: baseline_plus_trend_structure
    required_proxy_ids: []
    target_objective_terms: [regime_flip_penalty]
    expected_signal_role: trend_structure_diagnostic
  - experiment_id: volatility_regime
    required_proxy_ids: [volatility_compression_free_v1]
    target_objective_terms: [false_risk_on_cost, drawdown_warning_lead_time]
    expected_signal_role: stress_detection_diagnostic
  - experiment_id: risk_appetite
    required_proxy_ids: [rates_liquidity_free_v1, smh_to_qqq, soxx_to_qqq]
    target_objective_terms: [false_risk_off_cost, recovery_delay_days]
    expected_signal_role: risk_appetite_diagnostic
  - experiment_id: equal_cap_weight_divergence
    required_proxy_ids: [rsp_to_spy, qqqe_to_qqq]
    target_objective_terms: [benchmark_consistency_score]
    expected_signal_role: equal_cap_weight_divergence
  - experiment_id: combined_proxy
    required_proxy_ids:
      - volatility_compression_free_v1
      - rates_liquidity_free_v1
      - smh_to_qqq
      - soxx_to_qqq
      - rsp_to_spy
      - qqqe_to_qqq
    target_objective_terms:
      - false_risk_on_cost
      - false_risk_off_cost
      - drawdown_warning_lead_time
      - recovery_delay_days
      - regime_flip_penalty
      - benchmark_consistency_score
      - stress_slice_minimum_requirements
    expected_signal_role: combined_proxy_diagnostic
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
  failure_event_count: 1089
""",
        encoding="utf-8",
    )
    return path


def _objective_validation(tmp_path: Path) -> Path:
    path = tmp_path / "objective.yaml"
    path.write_text(
        """
status: FIRST_LAYER_OBJECTIVE_VALIDATION_REDESIGN_READY_PROMOTION_BLOCKED
summary:
  stress_validation_allowed: false
objective_terms:
  - term_id: false_risk_on_cost
    current_baseline_value: 198
  - term_id: false_risk_off_cost
    current_baseline_value: 499
  - term_id: drawdown_warning_lead_time
    current_baseline_value: 5
  - term_id: recovery_delay_days
    current_baseline_value: 5
  - term_id: regime_flip_penalty
    current_baseline_value: 1.956242
  - term_id: benchmark_consistency_score
    current_baseline_value: 0.392621
  - term_id: stress_slice_minimum_requirements
    current_baseline_value: 3/4 slices covered
""",
        encoding="utf-8",
    )
    return path


def _proxy_audit(tmp_path: Path) -> Path:
    path = tmp_path / "proxy.yaml"
    path.write_text(
        """
status: FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED
summary:
  proxy_count: 8
  true_breadth_replaced: false
rows:
  - proxy_id: rates_liquidity_free_v1
    data_available: true
    replacement_for_true_breadth: false
  - proxy_id: volatility_compression_free_v1
    data_available: true
    replacement_for_true_breadth: false
  - proxy_id: smh_to_qqq
    data_available: true
    replacement_for_true_breadth: false
  - proxy_id: soxx_to_qqq
    data_available: true
    replacement_for_true_breadth: false
  - proxy_id: rsp_to_spy
    data_available: false
    replacement_for_true_breadth: false
  - proxy_id: qqqe_to_qqq
    data_available: false
    replacement_for_true_breadth: false
""",
        encoding="utf-8",
    )
    return path
