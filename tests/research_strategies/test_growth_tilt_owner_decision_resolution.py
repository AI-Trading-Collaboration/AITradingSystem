from __future__ import annotations

import copy
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_owner_decision_resolution as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_owner_decision_resolution as resolution,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

OWNER_RESOLUTION_PATH = Path(
    "inputs/research_reviews/growth_tilt_owner_decision_resolution.yaml"
)
CHANNEL_CODE_PATH = Path("src/ai_trading_system/channel_specific_first_layer_v3.py")
COMPILER_CODE_PATH = Path("src/ai_trading_system/two_layer_policy_compiler.py")
CHANNEL_PREDICTIONS_PATH = Path(
    "outputs/research_trends/channel_specific_v3/channel_composer_v3_predictions.csv"
)
THRESHOLD_POLICY_PATH = Path(
    "config/research/growth_tilt_candidate_pit_screening_policy.yaml"
)
REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2438M1D1A_Growth_Tilt_Owner_Decision_Resolution_And_"
    "Candidate_A_Reframing_Plan.md"
)


def test_owner_resolution_completes_all_decisions_with_explicit_blockers() -> None:
    payload = _build()

    assert payload["status"] == resolution.READY_STATUS
    assert payload["decision_count"] == 18
    assert payload["resolved_decision_count"] == 18
    assert payload["owner_decisions_complete"] is True
    assert payload["blocking_decision_ids"] == list(
        resolution.EXPECTED_BLOCKING_DECISION_IDS
    )
    assert payload["strict_validation_errors"] == []


def test_candidate_disposition_is_zero_approve_two_redefine_one_withdraw() -> None:
    payload = _build()
    section = payload["candidate_disposition_after_owner_resolution"]

    assert payload["approved_candidate_count"] == 0
    assert payload["redefine_candidate_count"] == 2
    assert payload["withdraw_candidate_count"] == 1
    assert payload["m2_eligible_candidate_count"] == 0
    assert [item["decision"] for item in section["candidates"]] == [
        "REDEFINE",
        "WITHDRAW",
        "REDEFINE",
    ]
    assert section["candidates"][0]["replacement_candidate_id"] == (
        "capped_recovery_permission_overlay"
    )


def test_recovery_output_is_classified_as_unscaled_score() -> None:
    payload = _build()
    checks = _checks(payload)

    assert checks["D03_UNSCALED_SCORE_EVIDENCED"]["status"] == "PASS"
    d03 = _decision(payload, "D03")
    assert d03["semantic_type"] == "UNSCALED_SCORE"


def test_missing_recovery_pit_lineage_is_blocked_not_inferred() -> None:
    payload = _build()
    check = _checks(payload)["D02_PIT_LINEAGE_BLOCKED"]

    assert check["status"] == "PASS"
    assert check["evidence"]["missing_pit_fields"] == [
        "as_of",
        "available_at",
        "known_at",
        "source_data_cutoff",
    ]
    assert _decision(payload, "D02")["status"] == "RESOLVED_BLOCKED"


def test_threshold_has_no_default_or_post_replay_tuning_escape() -> None:
    payload = _build()
    d04 = _decision(payload, "D04")

    assert d04["threshold_status"] == "BLOCKED_UNCALIBRATED_SCORE"
    assert d04["default_0_5_allowed"] is False
    assert d04["existing_pilot_0_55_approved_for_replacement"] is False
    assert d04["post_replay_tuning_allowed"] is False


def test_overlay_timing_reset_and_expiry_are_exactly_bounded() -> None:
    payload = _build()

    assert _decision(payload, "D05")["baseline_required_consecutive_steps"] is None
    assert _decision(payload, "D06")["reset_on_missing"] == "BLOCKED"
    assert _decision(payload, "D07")["same_step_application_allowed"] is False
    assert _decision(payload, "D08")["maximum_active_steps"] == 1
    assert _decision(payload, "D08")["auto_extension_allowed"] is False


def test_veto_producers_and_missing_policy_remain_explicit() -> None:
    payload = _build()

    assert _decision(payload, "D11")["current_resolution"] == (
        "BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS"
    )
    assert _decision(payload, "D12")["current_resolution"] == (
        "BLOCKED_NO_CALLABLE_PRODUCER"
    )
    assert _decision(payload, "D13")["do_not_create_for_this_candidate"] is True
    assert _decision(payload, "D14")["unresolved_component"] == "BLOCKED"
    assert _decision(payload, "D14")["candidate_specific_removal_allowed"] is False


def test_transition_contract_requires_distinct_requested_and_applied_trace() -> None:
    payload = _build()
    d15 = _decision(payload, "D15")

    assert d15["current_state_field"]
    assert d15["requested_target_state_field"] is None
    assert d15["applied_target_state_field"] is None
    assert d15["current_resolution"] == (
        "BLOCKED_NO_GOVERNED_REQUESTED_APPLIED_FIELDS"
    )
    assert _decision(payload, "D16")["candidate_request_applied_at"] == (
        "next_executable_evaluation_step"
    )


def test_transition_priority_never_lets_candidate_supersede_defense() -> None:
    payload = _build()
    d17 = _decision(payload, "D17")

    assert d17["ordered_priority"][0] == "INVALID_PIT_OR_DATA_CONTRACT_BLOCKED"
    assert d17["ordered_priority"][-1] == "EXPOSURE_OR_RISK_CAP_CLAMP"
    assert d17["candidate_may_supersede_baseline_defensive_request"] is False


def test_native_scalar_blocker_prohibits_qqq_and_tqqq_substitution() -> None:
    payload = _build()
    d18 = _decision(payload, "D18")

    assert d18["native_scalar_resolution"] == "BLOCKED_NO_GOVERNED_NATIVE_SCALAR"
    assert d18["qqq_equivalent_unit_allowed"] is False
    assert d18["tqqq_increase_allowed"] is False
    assert "one_native_transition_increment" in d18["candidate_delta_cap_formula"]


def test_m1d2_scope_allows_adapters_but_no_candidate_behavior() -> None:
    payload = _build()
    scope = payload["m1d2_adapter_scope"]

    assert scope["implementation_allowed"] is True
    assert scope["candidate_behavior_allowed"] is False
    assert scope["replay_allowed"] is False
    assert "hard_veto_aggregate_adapter" in scope["implement"]
    assert "new_baseline_recovery_persistence" in scope["prohibited"]
    assert "real_pit_replay" in scope["prohibited"]


def test_replacement_readiness_exposes_only_proven_conditions_as_ready() -> None:
    payload = _build()
    readiness = payload["replacement_a_readiness"]

    assert readiness["approval_status"] == "BLOCKED_PENDING_M1D2_AND_M1E"
    assert readiness["conditions"]["output_semantic_type_known"] is True
    assert readiness["conditions"]["next_step_timing_registered"] is True
    assert readiness["conditions"]["recovery_producer_pit_lineage_valid"] is False
    assert readiness["conditions"]["native_exposure_scalar_ready"] is False
    assert readiness["m2_eligible"] is False


def test_evidence_drift_fails_closed() -> None:
    sources = _sources()
    sources["channel_code_text"] = sources["channel_code_text"].replace(
        'frame["re_risk_allowed_probability"] = frame["do_not_de_risk_probability"]',
        'frame["re_risk_allowed_probability"] = 0.5',
    )

    payload = _build(sources)

    assert payload["status"] == resolution.BLOCKED_STATUS
    assert "owner_decision_evidence_mismatch" in payload["strict_validation_errors"]


def test_decision_identity_or_order_drift_fails_closed() -> None:
    sources = _sources()
    sources["owner_resolution"]["decisions"] = list(
        reversed(sources["owner_resolution"]["decisions"])
    )

    payload = _build(sources)

    assert "decision_identity_or_order_mismatch" in payload["strict_validation_errors"]
    assert payload["status"] == resolution.BLOCKED_STATUS


def test_safety_boundary_prohibits_replay_and_runtime_mutation() -> None:
    payload = _build()

    assert payload["replay_run"] is False
    assert payload["runtime_metrics_generated"] is False
    assert payload["candidate_behavior_implemented"] is False
    assert payload["baseline_behavior_changed"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_runner_writes_four_machine_artifacts_and_markdown(tmp_path: Path) -> None:
    payload = impl.run_growth_tilt_owner_decision_resolution(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == resolution.READY_STATUS
    for filename in (
        "growth_tilt_owner_decision_resolution.json",
        "growth_tilt_candidate_disposition_after_owner_resolution.json",
        "growth_tilt_m1d2_adapter_scope.json",
        "growth_tilt_replacement_a_readiness.json",
    ):
        assert (tmp_path / "outputs" / filename).exists()
    assert (tmp_path / "docs" / "growth_tilt_owner_decision_resolution.md").exists()


def test_runner_strict_mode_rejects_missing_owner_resolution(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="owner_resolution missing"):
        impl.run_growth_tilt_owner_decision_resolution(
            owner_resolution_path=tmp_path / "missing.yaml",
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_runs_owner_resolution_without_replay(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-owner-decision-resolution",
            "--output-root",
            str(tmp_path / "outputs"),
            "--docs-root",
            str(tmp_path / "docs"),
            "--as-of",
            "2026-07-10",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert resolution.READY_STATUS in result.output
    assert "resolved_decision_count=18" in result.output
    assert "approved_candidate_count=0" in result.output
    assert "withdraw_candidate_count=1" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_flow_and_task_register_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert entries[resolution.REPORT_TYPE]["production_effect"] == "none"
    assert entries[resolution.REPORT_TYPE]["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in resolution.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in resolution.REQUIRED_FLOW_REFERENCES
    )
    completed_register = Path("docs/task_register_completed.md").read_text(
        encoding="utf-8"
    )
    assert "TRADING-2438M1D1A_GROWTH_TILT_OWNER_DECISION_RESOLUTION" in (
        completed_register
    )


def _build(sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return resolution.build_growth_tilt_owner_decision_resolution(
        sources or _sources(),
        report_registry={"reports": [{"report_id": resolution.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(resolution.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(resolution.REQUIRED_FLOW_REFERENCES),
        requirement_text=REQUIREMENT_PATH.read_text(encoding="utf-8"),
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    with CHANNEL_PREDICTIONS_PATH.open("r", encoding="utf-8-sig") as handle:
        header = handle.readline().strip()
    return {
        "owner_resolution": copy.deepcopy(safe_load_yaml_path(OWNER_RESOLUTION_PATH)),
        "channel_code_text": CHANNEL_CODE_PATH.read_text(encoding="utf-8"),
        "compiler_code_text": COMPILER_CODE_PATH.read_text(encoding="utf-8"),
        "channel_prediction_header": header,
        "threshold_policy": copy.deepcopy(safe_load_yaml_path(THRESHOLD_POLICY_PATH)),
    }


def _decision(payload: dict[str, Any], decision_id: str) -> dict[str, Any]:
    return next(
        item for item in payload["decision_matrix"] if item["decision_id"] == decision_id
    )


def _checks(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["check_id"]: item for item in payload["evidence_validation"]}
