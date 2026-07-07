from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "valid_until_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_dynamic_strategy_valid_until_window_stale_signal_remediation_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2403", "TRADING-2405", "TRADING-2406"]
    assert payload["input_under_review"] == "valid_until_window"
    assert payload["input_type"] == "EXECUTION_SEMANTIC"
    assert payload["current_severity"] == "BLOCKING"
    assert payload["current_pit_status"] == "UNKNOWN_OR_APPROXIMATE_PIT"
    assert payload["valid_until_semantics_review_ready"] is True
    assert payload["stale_signal_risk_audit_ready"] is True
    assert payload["signal_validity_contract_plan_ready"] is True
    assert payload["growth_tilt_alignment_review_ready"] is True
    assert payload["remediation_plan_ready"] is True
    assert payload["severity_downgrade_conditions_ready"] is True
    assert payload["validation_plan_ready"] is True
    assert payload["valid_until_window_blocking_gap_resolved"] is False
    assert payload["valid_until_window_severity_downgraded"] is False
    assert payload["valid_until_window_remediation_plan_ready"] is True
    assert payload["valid_until_window_validation_plan_ready"] is True
    assert payload["candidate_search_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    current_blocker = payload["current_blocker"]
    assert current_blocker["input_id"] == "valid_until_window"
    assert current_blocker["input_type"] == "EXECUTION_SEMANTIC"
    assert current_blocker["severity"] == "BLOCKING"
    assert current_blocker["candidate_search_blocker"] is True
    assert current_blocker["blocker_resolved_in_2407"] is False

    semantics = payload["valid_until_semantics_review"]
    assert len(semantics["semantics"]) >= 6
    assert {row["semantic_id"] for row in semantics["semantics"]} >= {
        "valid_until_window",
        "no_stale_signal_carry_forward",
        "signal_to_execution_lag",
        "growth_tilt_valid_until_alignment",
    }
    risk_audit = payload["stale_signal_risk_audit"]
    assert risk_audit["blocking_risk_count"] == 4
    assert {risk["category"] for risk in risk_audit["risks"]} >= {
        "VALID_UNTIL_UNGROUNDED",
        "CARRY_FORWARD_RISK",
        "SIGNAL_TO_EXECUTION_LAG_RISK",
        "VALID_FROM_MISSING_RISK",
        "VALID_UNTIL_MISSING_RISK",
    }
    contract = payload["signal_validity_contract_plan"]
    assert "valid_until" in contract["required_fields"]
    assert "expired_signal_cannot_trigger_new_trade" in contract["invariants"]
    assert (
        contract["decision_policy"]["missing valid_until"]
        == "BLOCK_CANDIDATE_SEARCH_FOR_DEPENDENT_STRATEGY"
    )
    alignment = payload["growth_tilt_alignment_review"]
    assert alignment["growth_tilt_engine_status_from_2406"] == "BLOCKING"
    assert alignment["valid_until_window_status"] == "BLOCKING"
    assert payload["remediation_plan"]["valid_until_window_blocking_gap_resolved"] is False
    assert payload["severity_downgrade_conditions"]["downgrade_executed_in_2407"] is False
    assert payload["validation_plan"]["candidate_search_remains_blocked"] is True

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "valid_until_semantics_review_json",
        "stale_signal_risk_audit_json",
        "signal_validity_contract_plan_json",
        "severity_downgrade_conditions_json",
        "validation_plan_json",
        "markdown_path",
        "valid_until_semantics_review_markdown",
        "stale_signal_risk_audit_markdown",
        "signal_validity_contract_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "valid_until_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-valid-until-window-stale-signal-remediation-plan",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "remediation_plan_result.json").exists()
    assert (output_root / "valid_until_semantics_review.json").exists()
    assert (output_root / "stale_signal_risk_audit.json").exists()
    assert (output_root / "signal_validity_contract_plan.json").exists()
    assert (output_root / "severity_downgrade_conditions.json").exists()
    assert (output_root / "validation_plan.json").exists()


def test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_valid_until_window_stale_signal_remediation_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-valid-until-window-stale-signal-remediation-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("remediation_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2408_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_valid_until_window_stale_signal_remediation_plan" in catalog
    assert "dynamic-strategy-valid-until-window-stale-signal-remediation-plan" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "implementation_2405": root / "implementation_2405.json",
        "registry_snapshot_2405": root / "registry_snapshot_2405.json",
        "pit_matrix_2405": root / "pit_matrix_2405.json",
        "pit_gate_result_2405": root / "pit_gate_result_2405.json",
        "blocker_summary_2405": root / "blocker_summary_2405.json",
        "remediation_routes_2405": root / "remediation_routes_2405.json",
        "remediation_plan_2406": root / "remediation_plan_2406.json",
        "source_feature_inventory_2406": root / "source_feature_inventory_2406.json",
        "pit_risk_audit_2406": root / "pit_risk_audit_2406.json",
        "signal_construction_gap_analysis_2406": (
            root / "signal_construction_gap_analysis_2406.json"
        ),
        "severity_downgrade_conditions_2406": (
            root / "severity_downgrade_conditions_2406.json"
        ),
        "validation_plan_2406": root / "validation_plan_2406.json",
        "pit_matrix_2403": root / "pit_matrix_2403.json",
        "signal_construction_review_2403": root / "signal_construction_review_2403.json",
        "remediation_matrix_2403": root / "remediation_matrix_2403.json",
        "pit_input_registry_config": root / "pit_input_registry.yaml",
        "execution_policy_registry": root / "execution_policy_registry.yaml",
        "signal_validity_taxonomy": root / "signal_validity_taxonomy.yaml",
    }
    _write_json(
        paths["implementation_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "recommended_next_research_task": impl.m2405.NEXT_ROUTE,
            "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
        },
    )
    _write_json(
        paths["registry_snapshot_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_input_registry_snapshot": {"entries": [_valid_until_registry_entry()]},
        },
    )
    _write_json(
        paths["pit_matrix_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_coverage_matrix": {"pit_coverage_matrix": _pit_rows()},
        },
    )
    _write_json(
        paths["pit_gate_result_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_gate_result": {
                "candidate_search_allowed": False,
                "research_only_observation_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "blockers": [
                    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
                    "BLOCKING_GAP_VALID_UNTIL_WINDOW",
                ],
            },
        },
    )
    _write_json(
        paths["blocker_summary_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_blocker_summary": {
                "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
                "blocking_gap_details": {
                    "valid_until_window": {
                        "severity": "BLOCKING",
                        "pit_status": "UNKNOWN",
                        "pit_confidence": "LOW",
                        "candidate_search_blocker": True,
                        "observation_blocker": True,
                        "paper_shadow_blocker": True,
                        "production_blocker": True,
                        "risk_flags": [
                            "LOOKAHEAD_RISK",
                            "VALID_UNTIL_UNGROUNDED",
                            "STALE_DATA_RISK",
                        ],
                        "recommended_action": "ground validity window semantics",
                    }
                },
            },
        },
    )
    _write_json(
        paths["remediation_routes_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_remediation_routes": {
                "recommended_next_research_task": impl.m2405.NEXT_ROUTE,
                "routes": {
                    "valid_until_window": {
                        "next_task": impl.TASK_ID,
                        "severity": "BLOCKING",
                        "candidate_search_blocker": True,
                    }
                },
            },
        },
    )
    _write_json(
        paths["remediation_plan_2406"],
        {
            **_safe_doc(impl.m2406.READY_STATUS),
            "recommended_next_research_task": impl.m2406.NEXT_ROUTE,
            "current_severity": "BLOCKING",
            "growth_tilt_engine_blocking_gap_resolved": False,
            "growth_tilt_engine_severity_downgraded": False,
        },
    )
    for key in (
        "source_feature_inventory_2406",
        "pit_risk_audit_2406",
        "severity_downgrade_conditions_2406",
        "validation_plan_2406",
    ):
        _write_json(paths[key], _safe_doc(impl.m2406.READY_STATUS))
    _write_json(
        paths["signal_construction_gap_analysis_2406"],
        {
            **_safe_doc(impl.m2406.READY_STATUS),
            "signal_construction_gap_analysis": {
                "signal_construction": {
                    "horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY"
                }
            },
        },
    )
    _write_json(
        paths["pit_matrix_2403"],
        {**_safe_doc(impl.m2403.READY_STATUS), "pit_coverage_matrix": _pit_rows()},
    )
    _write_json(
        paths["signal_construction_review_2403"],
        {
            **_safe_doc(impl.m2403.READY_STATUS),
            "signal_construction_review": {
                "growth_tilt_engine": {
                    "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
                    "valid_until_rule": "validity_10d_v1 / valid_until_window family",
                },
                "source_2402_signal_gap": {
                    "valid_until_strictness": {
                        "near_expiry_signal_behavior": "NOT_SEPARATELY_VALIDATED",
                        "signal_to_execution_lag_days": 1.0,
                        "stale_signal_execution_count": 0,
                    }
                },
                "record_ready": True,
            },
        },
    )
    _write_json(
        paths["remediation_matrix_2403"],
        {
            **_safe_doc(impl.m2403.READY_STATUS),
            "prioritized_remediation_matrix": [
                {"remediation_id": "2403-SIGNAL-VALID-UNTIL", "severity": "BLOCKING"}
            ],
        },
    )
    paths["pit_input_registry_config"].write_text(
        _pit_input_registry_yaml(),
        encoding="utf-8",
    )
    paths["execution_policy_registry"].write_text(
        _execution_policy_registry_yaml(),
        encoding="utf-8",
    )
    paths["signal_validity_taxonomy"].write_text(
        _signal_validity_taxonomy_yaml(),
        encoding="utf-8",
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2405_implementation_path": paths["implementation_2405"],
        "source_2405_registry_snapshot_path": paths["registry_snapshot_2405"],
        "source_2405_pit_coverage_matrix_path": paths["pit_matrix_2405"],
        "source_2405_pit_gate_result_path": paths["pit_gate_result_2405"],
        "source_2405_blocker_summary_path": paths["blocker_summary_2405"],
        "source_2405_remediation_routes_path": paths["remediation_routes_2405"],
        "source_2406_remediation_plan_path": paths["remediation_plan_2406"],
        "source_2406_source_feature_inventory_path": paths[
            "source_feature_inventory_2406"
        ],
        "source_2406_pit_risk_audit_path": paths["pit_risk_audit_2406"],
        "source_2406_signal_construction_gap_analysis_path": paths[
            "signal_construction_gap_analysis_2406"
        ],
        "source_2406_severity_downgrade_conditions_path": paths[
            "severity_downgrade_conditions_2406"
        ],
        "source_2406_validation_plan_path": paths["validation_plan_2406"],
        "source_2403_pit_matrix_path": paths["pit_matrix_2403"],
        "source_2403_signal_construction_review_path": paths[
            "signal_construction_review_2403"
        ],
        "source_2403_remediation_matrix_path": paths["remediation_matrix_2403"],
        "pit_input_registry_path": paths["pit_input_registry_config"],
        "execution_policy_registry_path": paths["execution_policy_registry"],
        "signal_validity_taxonomy_path": paths["signal_validity_taxonomy"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "implementation_2405": "--source-2405-implementation",
        "registry_snapshot_2405": "--source-2405-registry-snapshot",
        "pit_matrix_2405": "--source-2405-pit-matrix",
        "pit_gate_result_2405": "--source-2405-pit-gate-result",
        "blocker_summary_2405": "--source-2405-blocker-summary",
        "remediation_routes_2405": "--source-2405-remediation-routes",
        "remediation_plan_2406": "--source-2406-remediation-plan",
        "source_feature_inventory_2406": "--source-2406-source-feature-inventory",
        "pit_risk_audit_2406": "--source-2406-pit-risk-audit",
        "signal_construction_gap_analysis_2406": (
            "--source-2406-signal-construction-gap-analysis"
        ),
        "severity_downgrade_conditions_2406": (
            "--source-2406-severity-downgrade-conditions"
        ),
        "validation_plan_2406": "--source-2406-validation-plan",
        "pit_matrix_2403": "--source-2403-pit-matrix",
        "signal_construction_review_2403": "--source-2403-signal-construction-review",
        "remediation_matrix_2403": "--source-2403-remediation-matrix",
        "pit_input_registry_config": "--pit-input-registry",
        "execution_policy_registry": "--execution-policy-registry",
        "signal_validity_taxonomy": "--signal-validity-taxonomy",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _pit_rows() -> list[dict[str, object]]:
    return [
        {
            "input_id": "growth_tilt_engine",
            "input_type": "SIGNAL",
            "source_artifact_or_config": "test_source",
            "point_in_time_status": "UNKNOWN",
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": "review growth tilt source features",
        },
        {
            "input_id": "valid_until_window",
            "input_type": "EXECUTION_SEMANTIC",
            "source_artifact_or_config": "test_source",
            "point_in_time_status": "UNKNOWN",
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": "ground validity window semantics",
        },
    ]


def _valid_until_registry_entry() -> dict[str, object]:
    return {
        "input_id": "valid_until_window",
        "input_type": "EXECUTION_SEMANTIC",
        "pit_status": "UNKNOWN",
        "pit_confidence": "LOW",
        "severity": "BLOCKING",
        "candidate_search_blocker": True,
        "observation_blocker": True,
        "paper_shadow_blocker": True,
        "production_blocker": True,
        "remediation_owner": "TRADING-2407",
        "risk_flags": [
            "LOOKAHEAD_RISK",
            "VALID_UNTIL_UNGROUNDED",
            "STALE_DATA_RISK",
        ],
        "recommended_action": "ground validity window semantics",
    }


def _pit_input_registry_yaml() -> str:
    return """
schema_version: dynamic_strategy_pit_input_registry.v1
entries:
  - input_id: valid_until_window
    input_type: EXECUTION_SEMANTIC
    pit_status: UNKNOWN
    pit_confidence: LOW
    severity: BLOCKING
    candidate_search_blocker: true
    observation_blocker: true
    paper_shadow_blocker: true
    production_blocker: true
    remediation_owner: TRADING-2407
    risk_flags:
      - LOOKAHEAD_RISK
      - VALID_UNTIL_UNGROUNDED
      - STALE_DATA_RISK
    recommended_action: ground validity window semantics
  - input_id: no_stale_signal_carry_forward
    input_type: EXECUTION_SEMANTIC
    pit_status: APPROXIMATE_PIT
    pit_confidence: MEDIUM
    severity: MATERIAL
    candidate_search_blocker: false
    observation_blocker: true
    paper_shadow_blocker: true
    production_blocker: true
    remediation_owner: TRADING-2407
    recommended_action: make stale-signal suppression verifiable
  - input_id: signal_to_execution_lag
    input_type: EXECUTION_SEMANTIC
    pit_status: APPROXIMATE_PIT
    pit_confidence: MEDIUM
    severity: MATERIAL
    candidate_search_blocker: false
    observation_blocker: true
    paper_shadow_blocker: true
    production_blocker: true
    remediation_owner: TRADING-2407
    recommended_action: keep lag distribution visible
"""


def _execution_policy_registry_yaml() -> str:
    return """
strategy_execution_policies:
  - strategy_id: equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1
    signal_policy:
      signal_source: close_based_vol_target
      signal_observation_time: after_market_close
      signal_effective_earliest: next_trading_day
      signal_validity_window_bdays: 10
      stale_signal_behavior: hold_previous_actual_position
    rebalance_policy:
      rebalance_frequency: monthly_plus_override
      rebalance_anchor: month_end
      allow_intramonth_rebalance: true
      execution_lag_bdays: 1
"""


def _signal_validity_taxonomy_yaml() -> str:
    return """
signal_validity_taxonomy:
  medium_signal:
    expected_half_life_days_min: 4
    expected_half_life_days_max: 10
  slow_signal:
    expected_half_life_days_min: 10
    expected_half_life_days_max: 30
strategy_signal_validity_profiles:
  limited_adjustment:
    primary_signal_class: slow_signal
    stale_after_days: 10
    near_stale_within_days: 2
    stale_action: hold_previous_position
allowed_stale_actions:
  - suppress_rebalance
  - hold_previous_position
  - fallback_to_static_baseline
  - no_trade
"""


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in impl.m2405.SAFETY_FALSE_FIELDS},
        **{field: False for field in impl.m2406.SAFETY_FALSE_FIELDS},
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
