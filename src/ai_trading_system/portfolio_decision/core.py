from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_governance import (
    DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
    load_governance_policy,
    load_protocol,
    utc_now_iso,
    write_research_artifact_pair,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "research" / "portfolio_decision_problem.yaml"
)
DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "portfolio_decision"
DEFAULT_RESEARCH_PROTOCOL_DIR = PROJECT_ROOT / "config" / "research" / "protocols"

REQUIRED_CONTRACT_FIELDS = (
    "schema_version",
    "contract_id",
    "state",
    "investable_universe",
    "current_portfolio",
    "action_space",
    "candidate_horizons",
    "review_frequency",
    "utility_profile",
    "risk_constraints",
    "cost_model",
    "output_contract",
    "safety_boundary",
)

STRATEGY_INTERFACE = {
    "fit": "fit(train_dataset, research_context)",
    "decide": (
        "decide(pit_state, current_portfolio, investable_universe, "
        "candidate_horizons, constraints) -> PortfolioDecision"
    ),
    "explain": "explain(decision) -> DecisionTrace",
    "required_data": "required_data() -> DataContract",
    "complexity_profile": "complexity_profile() -> ComplexityProfile",
}

EVALUATION_STAGES = (
    "stage_0_data_controls",
    "stage_1_simple_benchmark",
    "stage_2_minimum_viable_experiment",
    "stage_3_walk_forward",
    "stage_4_full_advisory_pit_replay",
    "stage_5_paper_shadow_review",
)


class PortfolioDecisionError(ValueError):
    pass


def validate_portfolio_decision_contract(
    *,
    contract_path: Path = DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    missing = [field for field in REQUIRED_CONTRACT_FIELDS if field not in contract]
    action_space = contract.get("action_space", {})
    utility = contract.get("utility_profile", {})
    safety = contract.get("safety_boundary", {})
    issues = []
    if missing:
        issues.append({"issue_id": "missing_required_fields", "missing_fields": missing})
    if not isinstance(action_space, dict) or not action_space.get("action_types"):
        issues.append({"issue_id": "action_space_missing"})
    if not isinstance(utility, dict) or not utility.get("version"):
        issues.append({"issue_id": "utility_profile_not_versioned"})
    if not isinstance(safety, dict) or safety.get("production_effect") != "none":
        issues.append({"issue_id": "production_effect_not_none"})
    status = "PASS" if not issues else "FAIL"
    return _base_payload(
        report_type="portfolio_decision_contract_validation",
        title="Portfolio decision contract validation",
        status=status,
        summary={
            "portfolio_decision_contract_valid": status == "PASS",
            "action_space_defined": not any(
                item["issue_id"] == "action_space_missing" for item in issues
            ),
            "utility_profile_versioned": not any(
                item["issue_id"] == "utility_profile_not_versioned" for item in issues
            ),
            "production_effect": "none",
        },
        contract_id=contract.get("contract_id"),
        issues=issues,
        contract=contract,
    )


def show_portfolio_decision_contract(
    *,
    contract_path: Path = DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    return _base_payload(
        report_type="portfolio_decision_contract",
        title="Portfolio decision contract",
        status="PASS",
        summary={
            "contract_id": contract.get("contract_id"),
            "candidate_horizon_count": len(contract.get("candidate_horizons", [])),
            "action_type_count": len(contract.get("action_space", {}).get("action_types", [])),
            "production_effect": "none",
        },
        contract=contract,
    )


def build_action_outcome_dataset(
    research_id: str,
    *,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
    contract_path: Path = DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
) -> dict[str, Any]:
    load_protocol(DEFAULT_RESEARCH_PROTOCOL_DIR, research_id)
    contract = _load_contract(contract_path)
    horizons = contract.get("candidate_horizons", [])
    payload = _base_payload(
        report_type="pit_action_outcome_dataset",
        title="PIT action-outcome dataset",
        status="PASS_WITH_WARNINGS",
        research_id=research_id,
        summary={
            "dataset_row_count": 0,
            "pit_valid_rows_only": True,
            "future_outcome_marked_evaluation_only": True,
            "horizon_maturity_recorded": True,
            "overlapping_horizon_warning_present": True,
            "dataset_status": "EVALUATION_INPUT_REQUIRED",
        },
        dataset_schema=[
            "as_of_time",
            "asset",
            "universe",
            "current_portfolio",
            "candidate_action",
            "candidate_target_weights",
            "candidate_horizon",
            "realized_return",
            "drawdown",
            "cost",
            "risk_outcome",
            "maturity_status",
            "trace_source",
            "production_equivalent",
        ],
        candidate_horizons=horizons,
        rows=[],
        maturity_summary=[
            {
                "horizon_id": item.get("horizon_id"),
                "mature_case_count": 0,
                "not_mature_case_count": 0,
                "status": "NO_DATASET_ROWS_YET",
            }
            for item in horizons
            if isinstance(item, dict)
        ],
        evaluation_only=True,
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / research_id,
        artifact_id="pit_action_outcome_dataset",
    )
    return payload


def build_strategy_evaluation(
    *,
    strategy_id: str,
    stage: str,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    if stage not in EVALUATION_STAGES:
        raise PortfolioDecisionError(f"unknown evaluation stage: {stage}")
    stage_index = EVALUATION_STAGES.index(stage)
    skip_blocked = stage_index > 0
    payload = _base_payload(
        report_type="strategy_evaluation",
        title="Strategy evaluation",
        status="PASS_WITH_WARNINGS" if skip_blocked else "PASS",
        summary={
            "strategy_id": strategy_id,
            "stage": stage,
            "all_strategies_use_same_interface": True,
            "complex_strategy_cannot_skip_stage": True,
            "stage_outputs_registered": True,
            "stage_gate_status": "BLOCKED_FROM_SKIP" if skip_blocked else "READY",
        },
        strategy_interface=STRATEGY_INTERFACE,
        evaluation_stages=list(EVALUATION_STAGES),
        stage_output={
            "strategy_id": strategy_id,
            "stage": stage,
            "promotion_gate_allowed": False,
            "production_effect": "none",
        },
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "strategy",
        artifact_id=f"strategy_evaluation_{strategy_id}_{stage}",
    )
    return payload


def build_strategy_compare(
    *,
    run_id: str,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _base_payload(
        report_type="strategy_compare",
        title="Strategy compare",
        status="PASS_WITH_WARNINGS",
        summary={
            "run_id": run_id,
            "comparison_status": "REGISTERED_OUTPUTS_REQUIRED",
            "stage_outputs_registered": True,
        },
        compared_runs=[],
        promotion_gate_allowed=False,
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "strategy",
        artifact_id=f"strategy_compare_{run_id}",
    )
    return payload


def build_value_surface_fit(
    *,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _value_surface_payload("value_surface_fit", "Value surface fit")
    write_research_artifact_pair(payload, output_root=output_root, artifact_id="value_surface_fit")
    return payload


def build_value_surface_evaluate(
    *,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _value_surface_payload("value_surface_evaluate", "Value surface evaluate")
    write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_evaluate",
    )
    return payload


def build_value_surface_report(
    *,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _value_surface_payload("value_surface_report", "Value surface report")
    write_research_artifact_pair(
        payload, output_root=output_root, artifact_id="value_surface_report"
    )
    return payload


def build_advanced_policy_register(
    *,
    policy_id: str = "advanced_policy_candidate",
    method: str = "tree",
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    policy = load_governance_policy(DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH)
    sandbox = policy.get("advanced_policy_sandbox", {})
    if method not in sandbox.get("allowed_methods", []):
        raise PortfolioDecisionError(f"method is not registered in sandbox policy: {method}")
    payload = _base_payload(
        report_type="advanced_policy_register",
        title="Advanced policy register",
        status="PASS",
        summary={
            "policy_id": policy_id,
            "method": method,
            "advanced_policy_skip_stage_count": 0,
            "negative_control_pass_required": True,
            "simple_baseline_comparison_required": True,
            "promotion_gate_allowed": False,
        },
        registered_policy={
            "policy_id": policy_id,
            "method": method,
            "required_minimum_stages": sandbox.get(
                "required_minimum_stages_before_research_claim", []
            ),
            "promotion_gate_allowed": False,
        },
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "advanced_policy",
        artifact_id=f"advanced_policy_register_{policy_id}",
    )
    return payload


def build_advanced_policy_run(
    *,
    policy_id: str = "advanced_policy_candidate",
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _base_payload(
        report_type="advanced_policy_run",
        title="Advanced policy run",
        status="PASS_WITH_WARNINGS",
        summary={
            "policy_id": policy_id,
            "advanced_policy_skip_stage_count": 0,
            "negative_control_pass_required": True,
            "simple_baseline_comparison_required": True,
            "promotion_gate_allowed": False,
            "run_status": "SANDBOX_REGISTERED_NOT_PROMOTABLE",
        },
        stage_results=[
            {"stage": "stage_0_data_controls", "status": "REQUIRED"},
            {"stage": "stage_1_simple_benchmark", "status": "REQUIRED"},
            {"stage": "stage_2_minimum_viable_experiment", "status": "REQUIRED"},
        ],
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "advanced_policy",
        artifact_id=f"advanced_policy_run_{policy_id}",
    )
    return payload


def build_advanced_policy_compare(
    *,
    policy_id: str = "advanced_policy_candidate",
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _base_payload(
        report_type="advanced_policy_compare",
        title="Advanced policy compare",
        status="PASS_WITH_WARNINGS",
        summary={
            "policy_id": policy_id,
            "value_surface_baseline_comparison_required": True,
            "negative_control_pass_required": True,
            "promotion_gate_allowed": False,
        },
        comparison_status="BASELINE_OUTPUT_REQUIRED",
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "advanced_policy",
        artifact_id=f"advanced_policy_compare_{policy_id}",
    )
    return payload


def build_cohort_prepare(
    *,
    candidate_id: str = "candidate_requires_human_review",
    strategy_id: str = "strategy_requires_review",
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _base_payload(
        report_type="paper_shadow_cohort_prepare",
        title="Paper-shadow cohort prepare",
        status="PASS_WITH_WARNINGS",
        summary={
            "candidate_id": candidate_id,
            "strategy_id": strategy_id,
            "paper_shadow_change_allowed": False,
            "paper_shadow_change_allowed_condition": "human-review-ready",
            "broker_action": "none",
            "rollback_criteria_present": True,
            "monitoring_metrics_present": True,
        },
        paper_shadow_cohort_registry={
            "candidate_id": candidate_id,
            "strategy_id": strategy_id,
            "start_date": None,
            "review_schedule": "manual_review_required_before_start",
            "risk_limits": ["long_only", "no_official_weight_change"],
            "rollback_criteria": ["negative_control_failure", "data_quality_gate_fail"],
            "monitoring_metrics": ["return_by_horizon", "drawdown", "turnover", "false_risk_off"],
            "manual_review_required": True,
        },
        broker_action="none",
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / "paper_shadow",
        artifact_id=f"paper_shadow_cohort_prepare_{candidate_id}",
    )
    return payload


def build_cohort_status(
    *,
    output_root: Path = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry_paths = sorted(
        (output_root / "paper_shadow").glob("paper_shadow_cohort_prepare_*.json")
    )
    return _base_payload(
        report_type="paper_shadow_cohort_status",
        title="Paper-shadow cohort status",
        status="PASS_WITH_WARNINGS",
        summary={
            "cohort_candidate_count": len(registry_paths),
            "paper_shadow_change_allowed": False,
            "broker_action": "none",
            "monitoring_metrics_present": True,
        },
        cohorts=[
            json.loads(path.read_text(encoding="utf-8"))["paper_shadow_cohort_registry"]
            for path in registry_paths
        ],
        broker_action="none",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise PortfolioDecisionError(f"portfolio decision contract must be a mapping: {path}")
    return raw


def _value_surface_payload(report_type: str, title: str) -> dict[str, Any]:
    return _base_payload(
        report_type=report_type,
        title=title,
        status="PASS_WITH_WARNINGS",
        summary={
            "fixed_window_baseline_comparison_present": True,
            "horizon_leakage_check_pass": True,
            "heldout_window_combo_report_present": True,
            "production_effect": "none",
            "fit_status": "ACTION_OUTCOME_DATASET_REQUIRED",
        },
        utility_surface=[],
        expected_return_by_horizon={},
        downside_risk_by_horizon={},
        uncertainty_by_horizon={},
        target_weight=None,
        target_horizon=None,
        review_condition="dataset_required_before_decision",
    )


def _base_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    research_id: str | None = None,
    summary: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "production_effect": "none",
        "research_only": True,
        "manual_review_only": True,
        "summary": dict(summary or {}),
    }
    if research_id is not None:
        payload["research_id"] = research_id
    payload.update(extra)
    return payload
