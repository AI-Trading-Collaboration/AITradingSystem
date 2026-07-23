from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_runtime_spec_threshold_policy_approval as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_candidate_runtime_spec_threshold_policy_approval as approval,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

CANDIDATE_IDS = [
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
]
OWNER_REVIEW_PATH = Path(
    "inputs/research_reviews/growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml"
)
METRIC_CONTRACT_PATH = Path("config/research/growth_tilt_candidate_replay_metric_contract.yaml")
THRESHOLD_POLICY_PATH = Path("config/research/growth_tilt_candidate_pit_screening_policy.yaml")


def test_tracked_owner_review_records_resolved_zero_approval_disposition() -> None:
    payload = _build(_owner_review(), _metric_contract(), _threshold_policy())

    assert payload["status"] == approval.READY_STATUS
    assert payload["candidate_ids"] == CANDIDATE_IDS
    assert payload["selection_basis"] == "CONFIG_DECLARATION_ORDER"
    assert payload["performance_ranked"] is False
    assert payload["owner_decision_complete_count"] == 3
    assert payload["approved_candidate_count"] == 0
    assert payload["redefine_candidate_count"] == 2
    assert payload["withdraw_candidate_count"] == 1
    assert payload["pending_candidate_count"] == 0
    assert payload["runtime_spec_ready_count"] == 0
    assert payload["metric_contract_ready_count"] == 0
    assert payload["threshold_policy_ready_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["owner_input_gap_count"] == 0
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_NO_APPROVED
    assert payload["strict_validation_errors"] == []
    assert payload["candidate_reviews"][0]["review_status"] == (
        "REDEFINED_SECOND_OWNER_APPROVAL_REQUIRED"
    )
    assert payload["candidate_reviews"][1]["review_status"] == "WITHDRAWN"


def test_complete_a_b_contracts_are_ready_while_c_redefine_is_excluded() -> None:
    payload = _build(*_complete_inputs())

    assert payload["status"] == approval.READY_STATUS
    assert payload["approved_candidate_count"] == 2
    assert payload["redefine_candidate_count"] == 1
    assert payload["runtime_spec_ready_count"] == 2
    assert payload["metric_contract_ready_count"] == 2
    assert payload["threshold_policy_ready_count"] == 2
    assert payload["m2_eligible_candidate_ids"] == CANDIDATE_IDS[:2]
    assert payload["owner_input_gap_count"] == 0
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_READY
    c = payload["candidate_reviews"][2]
    assert c["review_status"] == "REDEFINED_SECOND_OWNER_APPROVAL_REQUIRED"
    assert c["m2_eligible"] is False


def test_config_order_is_not_a_performance_rank() -> None:
    payload = _build(*_complete_inputs())

    assert [row["selection_order"] for row in payload["candidate_reviews"]] == [1, 2, 3]
    assert all(row["performance_ranked"] is False for row in payload["candidate_reviews"])
    assert all("source_rank" not in row for row in payload["candidate_reviews"])


def test_prohibited_rank_field_is_a_strict_error() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][0]["source_rank"] = 1
    payload = _build(review, metric, policy)

    assert any(
        item.startswith("prohibited_performance_rank_field")
        for item in payload["strict_validation_errors"]
    )
    assert payload["status"] == approval.BLOCKED_STATUS


def test_candidate_identity_or_order_drift_is_a_strict_error() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_ids"] = list(reversed(review["candidate_ids"]))
    review["candidate_reviews"] = list(reversed(review["candidate_reviews"]))
    payload = _build(review, metric, policy)

    assert "candidate_identity_or_order_drift" in payload["strict_validation_errors"]
    assert payload["status"] == approval.BLOCKED_STATUS


def test_selection_semantics_are_strictly_pinned() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_selection"]["performance_ranked"] = True
    payload = _build(review, metric, policy)

    assert "candidate_selection_semantics_invalid" in payload["strict_validation_errors"]


def test_redefinition_requires_orthogonal_post_confirmation_contract() -> None:
    review, metric, policy = _complete_inputs()
    redefinition = review["candidate_reviews"][2]["redefinition"]
    redefinition["changes_trigger_timing"] = True
    payload = _build(review, metric, policy)
    c = payload["candidate_reviews"][2]

    assert c["redefinition_ready"] is False
    assert approval.CANDIDATE_REDEFINITION_INCOMPLETE in c["gap_codes"]
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_DECISION


def test_tracked_candidate_b_is_withdrawn_and_not_m2_eligible() -> None:
    payload = _build(_owner_review(), _metric_contract(), _threshold_policy())
    row = payload["candidate_reviews"][1]

    assert row["decision"] == "WITHDRAW"
    assert row["redefinition_ready"] is True
    assert row["review_status"] == "WITHDRAWN"
    assert row["m2_eligible"] is False


def test_candidate_b_redefinition_cannot_add_more_than_one_persistence_step() -> None:
    review = _owner_review()
    review["candidate_reviews"][1] = _redefined_b_review_fixture()
    review["candidate_reviews"][1]["redefinition"]["proposed_parameters"]["maximum_added_steps"] = 2
    payload = _build(review, _metric_contract(), _threshold_policy())

    assert payload["candidate_reviews"][1]["redefinition_ready"] is False
    assert (
        approval.CANDIDATE_REDEFINITION_INCOMPLETE in payload["candidate_reviews"][1]["gap_codes"]
    )


def test_candidate_a_redefinition_cannot_extend_beyond_one_active_step() -> None:
    review = _owner_review()
    review["candidate_reviews"][0]["redefinition"]["proposed_parameters"][
        "maximum_active_steps"
    ] = 2
    payload = _build(review, _metric_contract(), _threshold_policy())

    assert payload["candidate_reviews"][0]["redefinition_ready"] is False
    assert (
        approval.CANDIDATE_REDEFINITION_INCOMPLETE in payload["candidate_reviews"][0]["gap_codes"]
    )


def test_redefined_candidate_never_appears_in_approved_runtime_specs() -> None:
    payload = _build(*_complete_inputs())
    section = payload["approved_candidate_runtime_specs"]

    assert section["candidate_count"] == 2
    assert section["candidate_ids"] == CANDIDATE_IDS[:2]
    assert all(item["candidate_id"] != CANDIDATE_IDS[2] for item in section["candidate_specs"])


def test_runtime_owner_placeholder_blocks_only_the_affected_candidate() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][0]["runtime_spec"]["parameters"][
        "recovery_signal_id"
    ] = "OWNER_MUST_MAP_TO_EXISTING_SIGNAL"
    payload = _build(review, metric, policy)

    assert payload["runtime_spec_ready_count"] == 1
    assert payload["m2_eligible_candidate_ids"] == [CANDIDATE_IDS[1]]
    assert payload["status"] == approval.BLOCKED_STATUS


def test_candidate_a_cannot_change_confirmed_ramp_speed() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][0]["runtime_spec"]["parameters"][
        "confirmed_state_ramp_multiplier"
    ] = 1.5
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][0]["runtime_spec_ready"] is False


def test_candidate_b_cannot_remove_confirmation() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][1]["runtime_spec"]["parameters"][
        "remove_confirmation_entirely"
    ] = True
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][1]["runtime_spec_ready"] is False


def test_operation_type_cannot_be_inferred_from_candidate_name() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][0]["runtime_spec"][
        "operation_type"
    ] = "DEFENSIVE_SOFT_CONFIRMATION_GRACE"
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][0]["runtime_spec_ready"] is False


def test_executor_mapping_operation_must_match_runtime_operation() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][1]["executor_mapping"][
        "operation_type"
    ] = "EARLY_REENTRY_PROVISIONAL_EXPOSURE"
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][1]["executor_mapping_ready"] is False
    assert approval.EXECUTOR_MAPPING_INCOMPLETE in payload["candidate_reviews"][1]["gap_codes"]


def test_runtime_spec_requires_inventory_ready_mapping_status() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][0]["runtime_spec"][
        "baseline_mapping_status"
    ] = "BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING"
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][0]["runtime_spec_ready"] is False
    assert approval.RUNTIME_SPEC_INCOMPLETE in payload["candidate_reviews"][0]["gap_codes"]


def test_runtime_spec_requires_exact_complete_hard_veto_set() -> None:
    review, metric, policy = _complete_inputs()
    review["candidate_reviews"][1]["runtime_spec"]["hard_veto_ids"].pop()
    payload = _build(review, metric, policy)

    assert payload["candidate_reviews"][1]["runtime_spec_ready"] is False


def test_shared_metric_contract_requires_all_six_metrics_in_order() -> None:
    review, metric, policy = _complete_inputs()
    metric["metrics"].pop()
    payload = _build(review, metric, policy)

    assert (
        "metric_identity_or_order_mismatch"
        in payload["metric_contract_review_matrix"]["blocker_codes"]
    )
    assert payload["status"] == approval.BLOCKED_STATUS


def test_shared_metric_contract_requires_preregistered_relative_delta_policy() -> None:
    review, metric, policy = _complete_inputs()
    metric["relative_delta_epsilon_policy"]["use_epsilon_as_substitute_value"] = True
    payload = _build(review, metric, policy)

    assert (
        "relative_delta_epsilon_policy_unresolved"
        in payload["metric_contract_review_matrix"]["blocker_codes"]
    )


def test_empty_event_policy_must_be_explicit() -> None:
    review, metric, policy = _complete_inputs()
    metric["empty_event_policy"]["no_eligible_events"] = "ZERO_IMPROVEMENT"
    payload = _build(review, metric, policy)

    assert (
        "empty_event_policy_unresolved" in payload["metric_contract_review_matrix"]["blocker_codes"]
    )


def test_event_metric_owner_fields_are_required() -> None:
    review, metric, policy = _complete_inputs()
    target = next(item for item in metric["metrics"] if item["metric_id"] == "false_risk_off_delta")
    target["owner_fields"]["evaluation_horizon_steps"] = None
    payload = _build(review, metric, policy)
    row = _metric_row(payload, "false_risk_off_delta")

    assert row["ready"] is False
    assert "owner_fields.evaluation_horizon_steps" in row["missing_fields"]


def test_metric_runtime_provenance_is_required() -> None:
    review, metric, policy = _complete_inputs()
    metric["common_runtime_provenance_fields"].remove("numerator")
    payload = _build(review, metric, policy)

    assert (
        "metric_runtime_provenance_incomplete"
        in payload["metric_contract_review_matrix"]["blocker_codes"]
    )


def test_screening_policy_requires_owner_preregistration() -> None:
    review, metric, policy = _complete_inputs()
    policy["policy_status"] = "PENDING_OWNER_PREREGISTRATION"
    policy["owner"] = "OWNER_MUST_SET"
    payload = _build(review, metric, policy)

    blockers = payload["threshold_policy_review_matrix"]["blockers_by_candidate"][CANDIDATE_IDS[0]]
    assert "threshold_policy_not_owner_approved" in blockers
    assert "threshold_policy_owner_unresolved" in blockers


@pytest.mark.parametrize(
    ("field", "value", "expected_blocker"),
    [
        ("approved_at", None, "threshold_policy_approved_at_missing"),
        ("approved_commit", None, "threshold_policy_approved_commit_missing"),
        ("source_hash", None, "threshold_policy_source_hash_missing"),
        (
            "result_visibility_at_approval",
            "RESULTS_VISIBLE",
            "threshold_policy_result_visibility_not_none",
        ),
    ],
)
def test_screening_policy_requires_frozen_approval_provenance(
    field: str, value: Any, expected_blocker: str
) -> None:
    review, metric, policy = _complete_inputs()
    policy[field] = value
    payload = _build(review, metric, policy)

    blockers = payload["threshold_policy_review_matrix"]["blockers_by_candidate"][CANDIDATE_IDS[0]]
    assert expected_blocker in blockers


def test_screening_policy_requires_six_threshold_evaluations() -> None:
    review, metric, policy = _complete_inputs()
    candidate = policy["candidate_thresholds"][0]
    candidate["thresholds"].pop()
    payload = _build(review, metric, policy)

    blockers = payload["threshold_policy_review_matrix"]["blockers_by_candidate"][CANDIDATE_IDS[0]]
    assert "candidate_threshold_count_mismatch" in blockers
    assert "candidate_threshold_metric_set_mismatch" in blockers


def test_screening_policy_requires_minimum_five_primary_events() -> None:
    review, metric, policy = _complete_inputs()
    policy["readiness_gate"]["minimum_primary_event_count"] = 4
    payload = _build(review, metric, policy)

    blockers = payload["threshold_policy_review_matrix"]["blockers_by_candidate"][CANDIDATE_IDS[0]]
    assert "readiness_gate_minimum_primary_event_count_mismatch" in blockers


def test_threshold_unit_must_match_metric_unit() -> None:
    review, metric, policy = _complete_inputs()
    policy["common_thresholds"][0]["unit"] = "relative_fraction"
    payload = _build(review, metric, policy)
    row = _threshold_row(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert "THRESHOLD_METRIC_UNIT_MISMATCH" in row["gap_codes"]


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), float("-inf")])
def test_null_nan_and_infinite_threshold_values_fail_closed(invalid: Any) -> None:
    review, metric, policy = _complete_inputs()
    policy["common_thresholds"][0]["value"] = invalid
    payload = _build(review, metric, policy)

    assert (
        approval.THRESHOLD_VALUE_INVALID
        in _threshold_row(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")["gap_codes"]
    )
    assert payload["status"] == approval.BLOCKED_STATUS


def test_zero_and_negative_preregistered_thresholds_are_valid() -> None:
    payload = _build(*_complete_inputs())

    return_row = _threshold_row(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")
    primary_row = _threshold_row(payload, CANDIDATE_IDS[0], "missed_upside_delta")
    assert return_row["threshold_value"] == 0.0 and return_row["ready"] is True
    assert primary_row["threshold_value"] == -0.05 and primary_row["ready"] is True


def test_invalid_safety_boundary_is_a_strict_error() -> None:
    review, metric, policy = _complete_inputs()
    review["safety_boundary"]["paper_shadow_allowed"] = True
    payload = _build(review, metric, policy)

    assert "owner_review_safety_boundary_invalid" in payload["strict_validation_errors"]
    assert payload["status"] == approval.BLOCKED_STATUS


def test_no_approved_candidates_is_a_valid_mixed_decision_state() -> None:
    review, metric, policy = _complete_inputs()
    _withdraw(review["candidate_reviews"][0])
    _withdraw(review["candidate_reviews"][1])
    payload = _build(review, metric, policy)

    assert payload["status"] == approval.READY_STATUS
    assert payload["approved_candidate_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["next_route"] == approval.NEXT_ROUTE_NO_APPROVED


def test_source_schema_status_route_and_lineage_are_strict() -> None:
    source = _source()
    source["schema_version"] = "wrong.v1"
    source["status"] = "WRONG"
    source["next_route"] = "WRONG_ROUTE"
    review, metric, policy = _complete_inputs()
    payload = _build(review, metric, policy, source=source)

    assert {
        "source_2438m_schema_version_mismatch",
        "source_2438m_status_mismatch",
        "source_2438m_route_mismatch",
    }.issubset(payload["strict_validation_errors"])


def test_no_effect_boundary_is_preserved() -> None:
    payload = _build(_owner_review(), _metric_contract(), _threshold_policy())

    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_status"].startswith("NOT_APPLICABLE")
    assert payload["threshold_values_changed"] is False
    assert payload["candidate_parameters_changed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_dynamic_runner_writes_mixed_decision_artifacts(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _owner_review(), _metric_contract(), _threshold_policy())
    output_root = tmp_path / "outputs"
    docs_root = tmp_path / "docs"

    payload = impl.run_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
        source_2438m_path=paths["source"],
        owner_review_path=paths["owner_review"],
        metric_contract_path=paths["metric_contract"],
        threshold_policy_path=paths["threshold_policy"],
        requirement_doc_path=paths["requirement"],
        report_registry_path=paths["registry"],
        artifact_catalog_path=paths["catalog"],
        system_flow_path=paths["flow"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
        strict=True,
    )

    assert payload["status"] == approval.READY_STATUS
    for filename in (
        "approval_readiness_result.json",
        "candidate_runtime_spec_review_matrix.json",
        "metric_contract_review_matrix.json",
        "threshold_policy_review_matrix.json",
        "owner_review_validation.json",
        "approved_candidate_runtime_specs.json",
        "owner_action_checklist.json",
        "no_effect_boundary.json",
    ):
        assert (output_root / filename).exists()
    assert (docs_root / "growth_tilt_candidate_runtime_spec_threshold_policy_approval.md").exists()


def test_dynamic_runner_strict_mode_rejects_missing_metric_contract(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _owner_review(), _metric_contract(), _threshold_policy())

    with pytest.raises(ValueError, match="metric_contract missing"):
        impl.run_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
            source_2438m_path=paths["source"],
            owner_review_path=paths["owner_review"],
            metric_contract_path=tmp_path / "missing.yaml",
            threshold_policy_path=paths["threshold_policy"],
            requirement_doc_path=paths["requirement"],
            report_registry_path=paths["registry"],
            artifact_catalog_path=paths["catalog"],
            system_flow_path=paths["flow"],
            output_root=tmp_path / "missing_outputs",
            docs_root=tmp_path / "missing_docs",
            as_of_date=date(2026, 7, 8),
            strict=True,
        )


def test_cli_realistic_mixed_decision_run(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _owner_review(), _metric_contract(), _threshold_policy())
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-candidate-runtime-spec-threshold-policy-approval",
            "--source-2438m",
            str(paths["source"]),
            "--owner-review",
            str(paths["owner_review"]),
            "--metric-contract",
            str(paths["metric_contract"]),
            "--threshold-policy",
            str(paths["threshold_policy"]),
            "--requirement-doc",
            str(paths["requirement"]),
            "--report-registry",
            str(paths["registry"]),
            "--artifact-catalog",
            str(paths["catalog"]),
            "--system-flow",
            str(paths["flow"]),
            "--output-root",
            str(tmp_path / "cli_outputs"),
            "--docs-root",
            str(tmp_path / "cli_docs"),
            "--as-of",
            "2026-07-08",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert approval.READY_STATUS in result.output
    assert "approved_candidate_count=0" in result.output
    assert "redefine_candidate_count=2" in result.output
    assert "withdraw_candidate_count=1" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "performance_ranked=false" in result.output


def test_registry_catalog_system_flow_and_task_register_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[approval.REPORT_TYPE]

    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in approval.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in approval.REQUIRED_FLOW_REFERENCES
    )
    active_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    completed_register = Path("docs/task_register_completed.md").read_text(encoding="utf-8")
    for task_id in (
        "TRADING-2438M1A_GROWTH_TILT_OWNER_DECISION_FINALIZATION",
        "TRADING-2438M1B_GROWTH_TILT_SHARED_METRIC_AND_SCREENING_POLICY_APPROVAL",
    ):
        assert task_id not in active_register
        matching_rows = [
            line for line in completed_register.splitlines() if line.startswith(f"|{task_id}|")
        ]
        assert len(matching_rows) == 1
        assert "|P0|DROPPED|" in matching_rows[0]


def _build(
    owner_review: dict[str, Any],
    metric_contract: dict[str, Any],
    threshold_policy: dict[str, Any],
    *,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return approval.build_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
        source or _source(),
        owner_review,
        metric_contract=metric_contract,
        threshold_policy=threshold_policy,
        report_registry={"reports": [{"report_id": approval.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(approval.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(approval.REQUIRED_FLOW_REFERENCES),
        requirement_text=Path(
            "docs/requirements/TRADING-2438M1_M2_Growth_Tilt_Candidate_Research_Contract_And_PIT_Replay_Development_Plan.md"
        ).read_text(encoding="utf-8"),
        as_of="2026-07-08",
    )


def _source() -> dict[str, Any]:
    return {
        "schema_version": approval.EXPECTED_SOURCE_SCHEMA,
        "status": approval.EXPECTED_SOURCE_STATUS,
        "next_route": approval.EXPECTED_SOURCE_ROUTE,
        "run_id": "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution:2026-07-08",
        "as_of": "2026-07-08",
        "top3_candidate_ids": list(CANDIDATE_IDS),
        "candidate_replay_outcome_rechecked": True,
        "blocked_count": 3,
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review() -> dict[str, Any]:
    return copy.deepcopy(safe_load_yaml_path(OWNER_REVIEW_PATH))


def _metric_contract() -> dict[str, Any]:
    return copy.deepcopy(safe_load_yaml_path(METRIC_CONTRACT_PATH))


def _threshold_policy() -> dict[str, Any]:
    return copy.deepcopy(safe_load_yaml_path(THRESHOLD_POLICY_PATH))


def _complete_inputs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    review = _owner_review()
    review["candidate_reviews"][0] = _approved_a_review_fixture()
    review["candidate_reviews"][1] = _approved_b_review_fixture()
    a_spec = review["candidate_reviews"][0]["runtime_spec"]
    a_spec["parameters"].update(
        {
            "recovery_signal_id": "fixture_recovery_signal",
            "recovery_signal_inventory_status": "PIT_APPROVED_CALLABLE",
            "recovery_persistence_contract_id": "fixture_recovery_persistence",
            "recovery_persistence_inventory_status": "GOVERNED_BASELINE_RULE_RESOLVED",
            "lagging_soft_confirmation_id": "fixture_soft_confirmation",
            "provisional_exposure_absolute_cap": 0.05,
        }
    )
    a_spec["baseline_mapping_status"] = "READY_FOR_OWNER_PREREGISTRATION"
    a_spec["hard_veto_ids"] = list(approval.mapping_inventory.EXPECTED_VETO_IDS)
    a_spec["hard_veto_set_inventory_status"] = "COMPLETE_CALLABLE_PIT_VALID_SET"
    a_spec["governed_transition_scope_inventory_status"] = "RESOLVED"
    a_spec["qqq_equivalent_binding_inventory_status"] = "RESOLVED"
    a_spec["applicable_regime_ids"] = ["ai_after_chatgpt_full_window"]
    b_spec = review["candidate_reviews"][1]["runtime_spec"]
    b_spec["parameters"].update(
        {
            "relaxed_soft_confirmation_id": "fixture_soft_confirmation",
            "soft_confirmation_inventory_status": "EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION",
            "baseline_required_state": "fixture_baseline_state",
        }
    )
    b_spec["baseline_mapping_status"] = "READY_FOR_OWNER_PREREGISTRATION"
    b_spec["hard_veto_ids"] = list(approval.mapping_inventory.EXPECTED_VETO_IDS)
    b_spec["hard_veto_set_inventory_status"] = "COMPLETE_CALLABLE_PIT_VALID_SET"
    b_spec["governed_transition_scope_inventory_status"] = "RESOLVED"
    b_spec["qqq_equivalent_binding_inventory_status"] = "RESOLVED"
    b_spec["applicable_regime_ids"] = ["ai_after_chatgpt_full_window"]

    metric = _metric_contract()
    metric["status"] = "APPROVED"
    metric["owner"] = "fixture_policy_owner"
    metric["relative_delta_epsilon_policy"].update(
        {"status": "APPROVED", "owner": "fixture_policy_owner"}
    )
    metric["empty_event_policy"].update({"status": "APPROVED", "owner": "fixture_policy_owner"})

    policy = _threshold_policy()
    policy["policy_status"] = "APPROVED"
    policy["owner"] = "fixture_policy_owner"
    policy["approved_at"] = "2026-07-10T00:00:00+09:00"
    policy["approved_commit"] = "a" * 40
    policy["source_hash"] = "b" * 64
    policy["result_visibility_at_approval"] = "NONE"
    for candidate in policy["candidate_thresholds"]:
        candidate["approval_dependency"] = "APPROVED"
    return review, metric, policy


def _approved_a_review_fixture() -> dict[str, Any]:
    return {
        "candidate_id": "recovery_reentry_speedup_guard",
        "selection_order": 1,
        "selection_basis": "CONFIG_DECLARATION_ORDER",
        "decision": "APPROVE",
        "decision_owner": "fixture_owner",
        "decision_version": "fixture.v1",
        "decision_timestamp": "2026-07-10T00:00:00+09:00",
        "decision_rationale": "Fixture for validator coverage of the historical A operation.",
        "review_condition": "Fixture review condition.",
        "expiry_condition": "Fixture expiry condition.",
        "next_route": approval.NEXT_ROUTE_BLOCKED,
        "runtime_spec": {
            "candidate_role": "RECOVERY_REENTRY_TIMING_ACCELERATOR",
            "baseline_config_ref": (
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml"
            ),
            "baseline_mapping_status": "BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING",
            "operation_type": "EARLY_REENTRY_PROVISIONAL_EXPOSURE",
            "parameters": {
                "recovery_signal_id": "OWNER_MUST_MAP_TO_EXISTING_SIGNAL",
                "recovery_signal_inventory_status": "UNRESOLVED",
                "recovery_persistence_contract_id": "OWNER_MUST_MAP_TO_GOVERNED_RULE",
                "recovery_persistence_inventory_status": "UNRESOLVED",
                "lagging_soft_confirmation_id": "OWNER_MUST_SELECT_EXACTLY_ONE",
                "lead_steps": 1,
                "baseline_transition_state": "defensive",
                "provisional_target_state": "neutral",
                "excluded_source_state": "risk_off",
                "provisional_exposure_fraction_of_remaining_gap": 0.25,
                "provisional_exposure_absolute_cap": 0.05,
                "provisional_exposure_unit": "QQQ_EQUIVALENT_WEIGHT",
                "max_active_steps": 2,
                "confirmed_state_ramp_multiplier": 1.0,
                "target_exposure_override_allowed": False,
                "hard_veto_bypass_allowed": False,
            },
            "hard_veto_ids": ["OWNER_MUST_MAP_ALL_BASELINE_HARD_VETOES"],
            "hard_veto_set_inventory_status": "INCOMPLETE_CALLABLE_PIT_VALID_SET",
            "applicable_regime_ids": ["OWNER_MUST_SET_NON_WILDCARD_SCOPE"],
            "governed_transition_scope_inventory_status": "UNRESOLVED",
            "qqq_equivalent_binding_inventory_status": "UNRESOLVED",
            "expiry_conditions": ["maximum_active_steps_reached"],
            "rollback_conditions": ["return_to_baseline_path"],
        },
        "executor_mapping": {
            "executor_family": "GrowthTiltCandidateOverlayExecutor",
            "operation_type": "EARLY_REENTRY_PROVISIONAL_EXPOSURE",
            "planned_entrypoint": (
                "ai_trading_system.research_quality."
                "growth_tilt_candidate_overlay_executor."
                "GrowthTiltCandidateOverlayExecutor"
            ),
            "input_contract_version": "growth_tilt_candidate_overlay_input.v1",
            "output_contract_version": "growth_tilt_candidate_overlay_output.v1",
        },
        "metric_contract_ref": "growth_tilt_candidate_replay_metric_contract_v1",
        "threshold_policy_ref": "growth_tilt_candidate_pit_screening_policy_v1",
    }


def _approved_b_review_fixture() -> dict[str, Any]:
    return {
        "candidate_id": "false_risk_off_confirmation_relaxation",
        "selection_order": 2,
        "selection_basis": "CONFIG_DECLARATION_ORDER",
        "decision": "APPROVE",
        "decision_owner": "fixture_owner",
        "decision_version": "fixture.v1",
        "decision_timestamp": "2026-07-10T00:00:00+09:00",
        "decision_rationale": "Fixture for validator coverage of the historical B operation.",
        "review_condition": "Fixture review condition.",
        "expiry_condition": "Fixture expiry condition.",
        "next_route": approval.NEXT_ROUTE_BLOCKED,
        "runtime_spec": {
            "candidate_role": "DEFENSIVE_ENTRY_SOFT_CONFIRMATION_GRACE",
            "baseline_config_ref": (
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml"
            ),
            "baseline_mapping_status": "BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING",
            "operation_type": "DEFENSIVE_SOFT_CONFIRMATION_GRACE",
            "parameters": {
                "relaxed_soft_confirmation_id": "OWNER_MUST_SELECT_EXACTLY_ONE",
                "soft_confirmation_inventory_status": (
                    "UNRESOLVED_NO_ELIGIBLE_BASELINE_CONFIRMATION"
                ),
                "baseline_required_state": "neutral_or_constructive",
                "relaxation_mode": "ONE_STEP_GRACE",
                "grace_steps": 1,
                "remove_confirmation_entirely": False,
                "defensive_exposure_override_allowed": False,
                "hard_veto_bypass_allowed": False,
                "max_active_steps": 1,
            },
            "hard_veto_ids": ["OWNER_MUST_MAP_ALL_NON_BYPASSABLE_VETOES"],
            "hard_veto_set_inventory_status": "INCOMPLETE_CALLABLE_PIT_VALID_SET",
            "applicable_regime_ids": ["OWNER_MUST_SET_NON_WILDCARD_SCOPE"],
            "governed_transition_scope_inventory_status": "UNRESOLVED",
            "qqq_equivalent_binding_inventory_status": "UNRESOLVED",
            "expiry_conditions": ["grace_steps_exhausted"],
            "rollback_conditions": ["resume_baseline_defensive_rule"],
        },
        "executor_mapping": {
            "executor_family": "GrowthTiltCandidateOverlayExecutor",
            "operation_type": "DEFENSIVE_SOFT_CONFIRMATION_GRACE",
            "planned_entrypoint": (
                "ai_trading_system.research_quality."
                "growth_tilt_candidate_overlay_executor."
                "GrowthTiltCandidateOverlayExecutor"
            ),
            "input_contract_version": "growth_tilt_candidate_overlay_input.v1",
            "output_contract_version": "growth_tilt_candidate_overlay_output.v1",
        },
        "metric_contract_ref": "growth_tilt_candidate_replay_metric_contract_v1",
        "threshold_policy_ref": "growth_tilt_candidate_pit_screening_policy_v1",
    }


def _redefined_b_review_fixture() -> dict[str, Any]:
    return {
        "candidate_id": "false_risk_off_confirmation_relaxation",
        "selection_order": 2,
        "selection_basis": "CONFIG_DECLARATION_ORDER",
        "decision": "REDEFINE",
        "decision_owner": "fixture_owner",
        "decision_version": "fixture.v1",
        "decision_timestamp": "2026-07-10T00:00:00+09:00",
        "decision_rationale": "Fixture for the superseded B redefinition validator.",
        "review_condition": "Fixture review condition.",
        "expiry_condition": "Fixture expiry condition.",
        "next_route": approval.NEXT_ROUTE_DECISION,
        "redefinition": {
            "old_candidate_id": "false_risk_off_confirmation_relaxation",
            "proposed_candidate_id": "non_hard_defensive_entry_persistence_guard",
            "overlap_with": "BASELINE_NON_HARD_DEFENSIVE_ENTRY_REQUEST",
            "old_semantics_rejected_reason": ("NO_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION"),
            "new_candidate_role": "NON_HARD_DEFENSIVE_ENTRY_PERSISTENCE_GUARD",
            "changes_trigger_timing": True,
            "changes_ramp_speed": False,
            "changes_soft_component": False,
            "changes_aggregate_non_hard_request_persistence": True,
            "rationale": "Test only the aggregate non-hard request persistence axis.",
            "proposed_parameters": {
                "trigger_source": ("EXACT_BASELINE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"),
                "candidate_required_steps_expression": ("baseline_required_steps_plus_one"),
                "maximum_added_steps": 1,
                "changes_soft_component": False,
                "changes_aggregate_non_hard_request_persistence": True,
                "hard_veto_bypass_allowed": False,
                "auto_extension_allowed": False,
                "exposure_above_pre_request_baseline_allowed": False,
                "aggregate_request_contract_status": (
                    "BLOCKED_NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"
                ),
            },
            "second_owner_approval_required": True,
            "future_reopen_condition": "Only after the baseline contract exists.",
        },
    }


def _withdraw(review: dict[str, Any]) -> None:
    review["decision"] = "WITHDRAW"
    review.pop("runtime_spec", None)
    review.pop("executor_mapping", None)
    review.pop("metric_contract_ref", None)
    review.pop("threshold_policy_ref", None)


def _metric_row(payload: dict[str, Any], metric_id: str) -> dict[str, Any]:
    return next(
        item
        for item in payload["metric_contract_review_matrix"]["rows"]
        if item["metric_id"] == metric_id
    )


def _threshold_row(payload: dict[str, Any], candidate_id: str, metric_id: str) -> dict[str, Any]:
    return next(
        item
        for item in payload["threshold_policy_review_matrix"]["rows"]
        if item["candidate_id"] == candidate_id and item["metric_id"] == metric_id
    )


def _runner_sources(
    tmp_path: Path,
    owner_review: dict[str, Any],
    metric_contract: dict[str, Any],
    threshold_policy: dict[str, Any],
) -> dict[str, Path]:
    paths = {
        "source": tmp_path / "source.json",
        "owner_review": tmp_path / "owner_review.yaml",
        "metric_contract": tmp_path / "metric_contract.yaml",
        "threshold_policy": tmp_path / "threshold_policy.yaml",
        "requirement": tmp_path / "requirement.md",
        "registry": tmp_path / "registry.yaml",
        "catalog": tmp_path / "catalog.md",
        "flow": tmp_path / "flow.md",
    }
    paths["source"].write_text(json.dumps(_source()), encoding="utf-8")
    paths["owner_review"].write_text(json.dumps(owner_review), encoding="utf-8")
    paths["metric_contract"].write_text(json.dumps(metric_contract), encoding="utf-8")
    paths["threshold_policy"].write_text(json.dumps(threshold_policy), encoding="utf-8")
    paths["requirement"].write_text(
        Path(
            "docs/requirements/TRADING-2438M1_M2_Growth_Tilt_Candidate_Research_Contract_And_PIT_Replay_Development_Plan.md"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    paths["registry"].write_text(
        json.dumps({"reports": [{"report_id": approval.REPORT_TYPE}]}),
        encoding="utf-8",
    )
    paths["catalog"].write_text("\n".join(approval.REQUIRED_CATALOG_REFERENCES), encoding="utf-8")
    paths["flow"].write_text("\n".join(approval.REQUIRED_FLOW_REFERENCES), encoding="utf-8")
    return paths
