from __future__ import annotations

from controlled_strategy_batch_helpers import (
    PROJECT_ROOT,
    TEST_AS_OF,
    TIER_SPECS,
    CliRunner,
    Path,
    _write_forward_ledger,
    _write_price_caches,
    app,
    safe_load_yaml_path,
)


def test_controlled_strategy_batch_cli_smoke(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    ledger_path = _write_forward_ledger(tmp_path)
    runner = CliRunner()
    commands = [
        [
            "research",
            "strategies",
            "value-surface-controlled-prototype",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_value"),
        ],
        [
            "research",
            "strategies",
            "value-surface-controlled-expansion",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_value_expansion"),
        ],
        [
            "research",
            "strategies",
            "utility-boundary-ranking-policy-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "research",
            "strategies",
            "regret-state-machine-controlled-prototype",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "research",
            "strategies",
            "simple-strategy-selector-pilot",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_simple"),
        ],
        [
            "research",
            "strategies",
            "gbdt-action-utility-baseline",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-pivot-review",
            "--gbdt-action-utility",
            str(tmp_path / "cli_gbdt" / "gbdt_action_utility_baseline.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-pivot-direction-selection",
            "--gbdt-pivot-review",
            str(tmp_path / "cli_gbdt" / "gbdt_pivot_review.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-value-surface-residual-diagnostic-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--gbdt-pivot-selection",
            str(tmp_path / "cli_gbdt" / "gbdt_pivot_direction_selection.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-residual-hypothesis-triage",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--residual-diagnostic",
            str(tmp_path / "cli_gbdt" / "gbdt_value_surface_residual_diagnostic_prototype.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "regret-casebook-expansion-gate",
            "--regret-state-machine",
            str(tmp_path / "cli_state" / "regret_state_machine_controlled_prototype.json"),
            "--state-transition-casebook",
            str(tmp_path / "cli_state" / "state_transition_casebook.json"),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "research",
            "strategies",
            "regret-activation-inputs-from-value-surface-failures",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--regret-casebook-expansion-gate",
            str(tmp_path / "cli_state" / "regret_casebook_expansion_gate.json"),
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "research",
            "strategies",
            "regret-casebook-activation-recheck",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--regret-activation-inputs",
            str(
                tmp_path / "cli_state" / "regret_activation_inputs_from_value_surface_failures.json"
            ),
            "--regret-casebook-expansion-gate",
            str(tmp_path / "cli_state" / "regret_casebook_expansion_gate.json"),
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "forward-evidence",
            "maturity-tracker",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "research",
            "strategies",
            "value-surface-warning-triage-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--forward-maturity",
            str(tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-controlled-walk-forward-expansion",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--warning-triage",
            str(tmp_path / "cli_warning" / "value_surface_warning_triage_review.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-failure-attribution",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--walk-forward",
            str(tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "utility-ranking-robustness-pareto-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "research",
            "strategies",
            "value-surface-utility-pareto-ranking-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "research",
            "strategies",
            "horizon-cliff-utility-ranking-stabilization-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-pareto-ranking",
            str(tmp_path / "cli_utility" / "value_surface_utility_pareto_ranking_review.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "forward-evidence",
            "daily-continuity-maturity-tracker",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--forward-maturity",
            str(tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json"),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "forward-evidence",
            "daily-continuity-review",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "forward-evidence",
            "continuity-extension",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "research",
            "strategies",
            "value-surface-direction-review",
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--forward-continuity-extension",
            str(tmp_path / "cli_maturity" / "forward_evidence_continuity_extension.json"),
            "--walk-forward",
            str(tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-value-surface-design",
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--direction-review",
            str(tmp_path / "cli_warning" / "value_surface_direction_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-loss-guardrail-fallback-policy",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-horizon-loss-attribution-matrix",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "gbdt-residual-hypothesis-regime-conditioning",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-value-surface-controlled-review",
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--guardrail-policy",
            str(tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json"),
            "--loss-matrix",
            str(tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json"),
            "--residual-regime",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_regime_conditioning.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "cost-turnover-aware-regime-conditioned-value-surface",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--guardrail-policy",
            str(tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "long-horizon-quarantine-selection-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "ai-after-chatgpt-full-regime-attribution-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--loss-matrix",
            str(tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-walk-forward-holdout",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--horizon-quarantine",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--regime-attribution",
            str(tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-v2-controlled-review",
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--horizon-quarantine",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--regime-attribution",
            str(tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"),
            "--holdout",
            str(tmp_path / "cli_warning" / "regime_conditioned_walk_forward_holdout.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-problem-contract",
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--long-horizon-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "long-horizon-quarantine-fallback-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-controlled-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--fallback-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "cost-aware-horizon-hysteresis",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--prototype",
            str(tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-holdout-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--fallback-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json"),
            "--prototype",
            str(tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json"),
            "--hysteresis",
            str(tmp_path / "cli_warning" / "cost_aware_horizon_hysteresis.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-policy-kill-diagnostic-downgrade",
            "--horizon-selector-holdout",
            str(tmp_path / "cli_warning" / "horizon_selector_holdout_review.json"),
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "benchmark-first-tail-risk-policy-contract",
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-loss-avoidance-classifier-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "conservative-horizon-risk-filter",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "benchmark-fallback-drawdown-guard-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--horizon-filter",
            str(tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-policy-family-controlled-review",
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--horizon-filter",
            str(tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json"),
            "--fallback",
            str(tmp_path / "cli_warning" / "benchmark_fallback_drawdown_guard_prototype.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-benchmark-fallback-robustness-expansion",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--fallback",
            str(tmp_path / "cli_warning" / "benchmark_fallback_drawdown_guard_prototype.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-trigger-precision-recall-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-opportunity-cost-upside-capture-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-forward-evidence-integration",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--ledger-path",
            str(tmp_path / "cli_forward_tail" / "tail_risk_ledger.jsonl"),
            "--output-root",
            str(tmp_path / "cli_forward_tail"),
            "--as-of-date",
            TEST_AS_OF.isoformat(),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-audit-universe-reconciliation",
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--precision-recall",
            str(
                tmp_path / "cli_warning" / "tail_risk_fallback_trigger_precision_recall_audit.json"
            ),
            "--opportunity-cost",
            str(tmp_path / "cli_warning" / "tail_risk_opportunity_cost_upside_capture_review.json"),
            "--forward-integration",
            str(
                tmp_path
                / "cli_forward_tail"
                / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-anti-leakage-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-threshold-sensitivity",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-regime-segmented-robustness",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-forward-maturity-scoreboard",
            "--forward-integration",
            str(
                tmp_path
                / "cli_forward_tail"
                / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
            ),
            "--as-of-date",
            TEST_AS_OF.isoformat(),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-policy-controlled-review-board",
            "--robustness",
            str(
                tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
            ),
            "--precision-recall",
            str(
                tmp_path / "cli_warning" / "tail_risk_fallback_trigger_precision_recall_audit.json"
            ),
            "--opportunity-cost",
            str(tmp_path / "cli_warning" / "tail_risk_opportunity_cost_upside_capture_review.json"),
            "--forward-integration",
            str(
                tmp_path
                / "cli_forward_tail"
                / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
            ),
            "--audit-universe-reconciliation",
            str(tmp_path / "cli_warning" / "tail_risk_fallback_audit_universe_reconciliation.json"),
            "--anti-leakage",
            str(tmp_path / "cli_warning" / "tail_risk_fallback_anti_leakage_audit.json"),
            "--sensitivity",
            str(tmp_path / "cli_warning" / "tail_risk_fallback_threshold_sensitivity.json"),
            "--regime-segmented",
            str(tmp_path / "cli_warning" / "tail_risk_fallback_regime_segmented_robustness.json"),
            "--forward-maturity-scoreboard",
            str(tmp_path / "cli_warning" / "tail_risk_fallback_forward_maturity_scoreboard.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-fallback-blocker-diagnostic",
            "--review-board",
            str(tmp_path / "cli_warning" / "tail_risk_policy_controlled_review_board.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "ops",
            "controlled-strategy-batch-review",
            "--value-surface",
            str(tmp_path / "cli_value" / "value_surface_controlled_prototype.json"),
            "--regret-state-machine",
            str(tmp_path / "cli_state" / "regret_state_machine_controlled_prototype.json"),
            "--simple-selector",
            str(tmp_path / "cli_simple" / "simple_strategy_selector_pilot.json"),
            "--gbdt-action-utility",
            str(tmp_path / "cli_gbdt" / "gbdt_action_utility_baseline.json"),
            "--output-root",
            str(tmp_path / "cli_review"),
        ],
    ]

    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
        assert "production_effect=none" in result.output

    assert (tmp_path / "cli_review" / "controlled_strategy_batch_review.json").exists()
    assert (tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_warning_triage_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "value_surface_failure_attribution.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_direction_review.json").exists()
    assert (tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json").exists()
    assert (tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json").exists()
    assert (tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json").exists()
    assert (
        tmp_path / "cli_warning" / "regime_conditioned_value_surface_controlled_review.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "cost_turnover_aware_regime_conditioned_value_surface.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "regime_conditioned_walk_forward_holdout.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_problem_contract.json").exists()
    assert (tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "cost_aware_horizon_hysteresis.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_holdout_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json").exists()
    assert (tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json").exists()
    assert (tmp_path / "cli_warning" / "benchmark_fallback_drawdown_guard_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "tail_risk_policy_family_controlled_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_benchmark_fallback_robustness_expansion.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_fallback_trigger_precision_recall_audit.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_opportunity_cost_upside_capture_review.json"
    ).exists()
    assert (
        tmp_path
        / "cli_forward_tail"
        / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_fallback_audit_universe_reconciliation.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "tail_risk_fallback_anti_leakage_audit.json").exists()
    assert (tmp_path / "cli_warning" / "tail_risk_fallback_threshold_sensitivity.json").exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_fallback_regime_segmented_robustness.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "tail_risk_fallback_forward_maturity_scoreboard.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "tail_risk_policy_controlled_review_board.json").exists()
    assert (tmp_path / "cli_warning" / "tail_risk_fallback_blocker_diagnostic.json").exists()
    assert (tmp_path / "cli_utility" / "utility_ranking_robustness_pareto_audit.json").exists()
    assert (tmp_path / "cli_utility" / "value_surface_utility_pareto_ranking_review.json").exists()
    assert (
        tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
    ).exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json").exists()
    assert (
        tmp_path / "cli_maturity" / "forward_evidence_daily_continuity_maturity_tracker.json"
    ).exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_daily_continuity_review.json").exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_continuity_extension.json").exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_pivot_direction_selection.json").exists()
    assert (
        tmp_path / "cli_gbdt" / "gbdt_value_surface_residual_diagnostic_prototype.json"
    ).exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json").exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_regime_conditioning.json").exists()
    assert (tmp_path / "cli_state" / "regret_casebook_expansion_gate.json").exists()
    assert (
        tmp_path / "cli_state" / "regret_activation_inputs_from_value_surface_failures.json"
    ).exists()
    assert (tmp_path / "cli_state" / "regret_casebook_activation_recheck.json").exists()


def test_controlled_strategy_batch_validation_tiers() -> None:
    controlled_strategy_paths = {
        "tests/test_controlled_strategy_value_surface.py",
        "tests/test_controlled_strategy_regime_horizon.py",
        "tests/test_controlled_strategy_tail_risk_policy.py",
        "tests/test_controlled_strategy_candidate_batch.py",
        "tests/test_controlled_strategy_batch.py",
        "tests/test_tail_risk_fallback_falsification_audit.py",
    }
    assert controlled_strategy_paths.issubset(set(TIER_SPECS["fast-unit"].paths))
    assert controlled_strategy_paths.issubset(set(TIER_SPECS["contract-validation"].paths))


def test_controlled_strategy_batch_registry_catalog_and_system_flow() -> None:
    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    for report_id in {
        "value_surface_controlled_prototype",
        "value_surface_controlled_expansion",
        "utility_boundary_ranking_policy_audit",
        "utility_ranking_robustness_pareto_audit",
        "value_surface_utility_pareto_ranking_review",
        "horizon_cliff_utility_ranking_stabilization_review",
        "forward_evidence_maturity_tracker",
        "forward_evidence_daily_continuity_maturity_tracker",
        "forward_evidence_daily_continuity_review",
        "forward_evidence_continuity_extension",
        "regret_state_machine_controlled_prototype",
        "simple_strategy_selector_pilot",
        "gbdt_action_utility_controlled_baseline",
        "gbdt_pivot_review",
        "gbdt_pivot_direction_selection",
        "gbdt_value_surface_residual_diagnostic_prototype",
        "gbdt_residual_hypothesis_triage",
        "regret_casebook_expansion_gate",
        "regret_activation_inputs_from_value_surface_failures",
        "regret_casebook_activation_recheck",
        "value_surface_warning_triage_review",
        "value_surface_controlled_walk_forward_expansion",
        "value_surface_failure_attribution",
        "value_surface_direction_review",
        "regime_conditioned_value_surface_design",
        "tail_loss_guardrail_fallback_policy",
        "regime_horizon_loss_attribution_matrix",
        "gbdt_residual_hypothesis_regime_conditioning",
        "regime_conditioned_value_surface_controlled_review",
        "cost_turnover_aware_regime_conditioned_value_surface",
        "long_horizon_quarantine_selection_review",
        "ai_after_chatgpt_full_regime_attribution_review",
        "regime_conditioned_walk_forward_holdout",
        "value_surface_v2_controlled_review",
        "horizon_selector_problem_contract",
        "long_horizon_quarantine_fallback_review",
        "horizon_selector_controlled_prototype",
        "cost_aware_horizon_hysteresis",
        "horizon_selector_holdout_review",
        "value_surface_policy_kill_diagnostic_downgrade",
        "benchmark_first_tail_risk_policy_contract",
        "tail_loss_avoidance_classifier_prototype",
        "conservative_horizon_risk_filter",
        "benchmark_fallback_drawdown_guard_prototype",
        "tail_risk_policy_family_controlled_review",
        "tail_risk_benchmark_fallback_robustness_expansion",
        "tail_risk_fallback_trigger_precision_recall_audit",
        "tail_risk_opportunity_cost_upside_capture_review",
        "tail_risk_forward_evidence_integration",
        "tail_risk_fallback_audit_universe_reconciliation",
        "tail_risk_fallback_anti_leakage_audit",
        "tail_risk_fallback_threshold_sensitivity",
        "tail_risk_fallback_regime_segmented_robustness",
        "tail_risk_fallback_forward_maturity_scoreboard",
        "tail_risk_policy_controlled_review_board",
        "tail_risk_fallback_blocker_diagnostic",
        "controlled_strategy_batch_review",
    }:
        assert report_id in report_ids
        assert report_ids[report_id]["artifact_selection_policy"] == "latest_available"
        assert report_ids[report_id]["required_for_daily_reading"] is False

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "value_surface_controlled_prototype.json/md" in catalog
    assert "value_surface_controlled_expansion.json/md" in catalog
    assert "value_surface_warning_triage_review.json/md" in catalog
    assert "value_surface_controlled_walk_forward_expansion.json/md" in catalog
    assert "value_surface_failure_attribution.json/md" in catalog
    assert "value_surface_direction_review.json/md" in catalog
    assert "regime_conditioned_value_surface_design.json/md" in catalog
    assert "tail_loss_guardrail_fallback_policy.json/md" in catalog
    assert "regime_horizon_loss_attribution_matrix.json/md" in catalog
    assert "gbdt_residual_hypothesis_regime_conditioning.json/md" in catalog
    assert "regime_conditioned_value_surface_controlled_review.json/md" in catalog
    assert "cost_turnover_aware_regime_conditioned_value_surface.json/md" in catalog
    assert "long_horizon_quarantine_selection_review.json/md" in catalog
    assert "ai_after_chatgpt_full_regime_attribution_review.json/md" in catalog
    assert "regime_conditioned_walk_forward_holdout.json/md" in catalog
    assert "value_surface_v2_controlled_review.json/md" in catalog
    assert "horizon_selector_problem_contract.json/md" in catalog
    assert "long_horizon_quarantine_fallback_review.json/md" in catalog
    assert "horizon_selector_controlled_prototype.json/md" in catalog
    assert "cost_aware_horizon_hysteresis.json/md" in catalog
    assert "horizon_selector_holdout_review.json/md" in catalog
    assert "value_surface_policy_kill_diagnostic_downgrade.json/md" in catalog
    assert "benchmark_first_tail_risk_policy_contract.json/md" in catalog
    assert "tail_loss_avoidance_classifier_prototype.json/md" in catalog
    assert "conservative_horizon_risk_filter.json/md" in catalog
    assert "benchmark_fallback_drawdown_guard_prototype.json/md" in catalog
    assert "tail_risk_policy_family_controlled_review.json/md" in catalog
    assert "tail_risk_benchmark_fallback_robustness_expansion.json/md" in catalog
    assert "tail_risk_fallback_trigger_precision_recall_audit.json/md" in catalog
    assert "tail_risk_opportunity_cost_upside_capture_review.json/md" in catalog
    assert "tail_risk_benchmark_fallback_forward_evidence_integration.json/md" in catalog
    assert "tail_risk_fallback_audit_universe_reconciliation.json/md" in catalog
    assert "tail_risk_fallback_anti_leakage_audit.json/md" in catalog
    assert "tail_risk_fallback_threshold_sensitivity.json/md" in catalog
    assert "tail_risk_fallback_regime_segmented_robustness.json/md" in catalog
    assert "tail_risk_fallback_forward_maturity_scoreboard.json/md" in catalog
    assert "tail_risk_policy_controlled_review_board.json/md" in catalog
    assert "tail_risk_fallback_blocker_diagnostic.json/md" in catalog
    assert "value_surface_utility_pareto_ranking_review.json/md" in catalog
    assert "horizon_cliff_utility_ranking_stabilization_review.json/md" in catalog
    assert "forward_evidence_maturity_tracker.json/md" in catalog
    assert "forward_evidence_daily_continuity_maturity_tracker.json/md" in catalog
    assert "forward_evidence_daily_continuity_review.json/md" in catalog
    assert "forward_evidence_continuity_extension.json/md" in catalog
    assert "gbdt_value_surface_residual_diagnostic_prototype.json/md" in catalog
    assert "gbdt_residual_hypothesis_triage.json/md" in catalog
    assert "regret_casebook_activation_recheck.json/md" in catalog
    assert "controlled_strategy_batch_review.json/md" in catalog
    assert "validated utility boundary" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-770～774" in system_flow
    assert "TRADING-775～779" in system_flow
    assert "TRADING-780～784" in system_flow
    assert "TRADING-785～789" in system_flow
    assert "TRADING-790～794" in system_flow
    assert "TRADING-795～799" in system_flow
    assert "TRADING-800～804" in system_flow
    assert "TRADING-805～809" in system_flow
    assert "TRADING-810～815" in system_flow
    assert "TRADING-816～820" in system_flow
    assert "TRADING-821～825" in system_flow
    assert "TRADING-826" in system_flow
    assert "aits research strategies value-surface-controlled-prototype" in system_flow
    assert "aits research strategies value-surface-controlled-expansion" in system_flow
    assert "aits research strategies value-surface-warning-triage-review" in system_flow
    assert "aits research strategies value-surface-controlled-walk-forward-expansion" in system_flow
    assert "aits research strategies value-surface-failure-attribution" in system_flow
    assert "aits research strategies value-surface-direction-review" in system_flow
    assert "aits research strategies regime-conditioned-value-surface-design" in system_flow
    assert "aits research strategies tail-loss-guardrail-fallback-policy" in system_flow
    assert "aits research strategies regime-horizon-loss-attribution-matrix" in system_flow
    assert "aits research strategies gbdt-residual-hypothesis-regime-conditioning" in system_flow
    assert (
        "aits research strategies regime-conditioned-value-surface-controlled-review" in system_flow
    )
    assert (
        "aits research strategies cost-turnover-aware-regime-conditioned-value-surface"
        in system_flow
    )
    assert "aits research strategies long-horizon-quarantine-selection-review" in system_flow
    assert "aits research strategies ai-after-chatgpt-full-regime-attribution-review" in system_flow
    assert "aits research strategies regime-conditioned-walk-forward-holdout" in system_flow
    assert "aits research strategies value-surface-v2-controlled-review" in system_flow
    assert "aits research strategies horizon-selector-problem-contract" in system_flow
    assert "aits research strategies long-horizon-quarantine-fallback-review" in system_flow
    assert "aits research strategies horizon-selector-controlled-prototype" in system_flow
    assert "aits research strategies cost-aware-horizon-hysteresis" in system_flow
    assert "aits research strategies horizon-selector-holdout-review" in system_flow
    assert "aits research strategies value-surface-policy-kill-diagnostic-downgrade" in system_flow
    assert "aits research strategies benchmark-first-tail-risk-policy-contract" in system_flow
    assert "aits research strategies tail-loss-avoidance-classifier-prototype" in system_flow
    assert "aits research strategies conservative-horizon-risk-filter" in system_flow
    assert "aits research strategies benchmark-fallback-drawdown-guard-prototype" in system_flow
    assert "aits research strategies tail-risk-policy-family-controlled-review" in system_flow
    assert (
        "aits research strategies tail-risk-benchmark-fallback-robustness-expansion" in system_flow
    )
    assert (
        "aits research strategies tail-risk-fallback-trigger-precision-recall-audit" in system_flow
    )
    assert (
        "aits research strategies tail-risk-opportunity-cost-upside-capture-review" in system_flow
    )
    assert "aits research strategies tail-risk-forward-evidence-integration" in system_flow
    assert (
        "aits research strategies tail-risk-fallback-audit-universe-reconciliation" in system_flow
    )
    assert "aits research strategies tail-risk-fallback-anti-leakage-audit" in system_flow
    assert "aits research strategies tail-risk-fallback-threshold-sensitivity" in system_flow
    assert "aits research strategies tail-risk-fallback-regime-segmented-robustness" in system_flow
    assert "aits research strategies tail-risk-fallback-forward-maturity-scoreboard" in system_flow
    assert "aits research strategies tail-risk-policy-controlled-review-board" in system_flow
    assert "aits research strategies tail-risk-fallback-blocker-diagnostic" in system_flow
    assert "aits research strategies value-surface-utility-pareto-ranking-review" in system_flow
    assert (
        "aits research strategies horizon-cliff-utility-ranking-stabilization-review" in system_flow
    )
    assert "aits forward-evidence maturity-tracker" in system_flow
    assert "aits forward-evidence daily-continuity-maturity-tracker" in system_flow
    assert "aits forward-evidence daily-continuity-review" in system_flow
    assert "aits forward-evidence continuity-extension" in system_flow
    assert (
        "aits research strategies gbdt-value-surface-residual-diagnostic-prototype" in system_flow
    )
    assert "aits research strategies gbdt-residual-hypothesis-triage" in system_flow
    assert "aits research strategies regret-casebook-activation-recheck" in system_flow
    assert "CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE" in system_flow
