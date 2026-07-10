from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_replacement_candidate_contract as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_replacement_candidate_contract as contract,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

OWNER_RESOLUTION_PATH = Path(
    "inputs/research_reviews/growth_tilt_owner_decision_resolution.yaml"
)
SCREENING_POLICY_PATH = Path(
    "config/research/growth_tilt_candidate_pit_screening_policy.yaml"
)
REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2438M1D1A_Growth_Tilt_Owner_Decision_Resolution_And_"
    "Candidate_A_Reframing_Plan.md"
)


def test_real_replacement_gate_keeps_candidate_redefined_and_blocked() -> None:
    payload = _build()

    assert payload["status"] == contract.READY_STATUS
    assert payload["replacement_candidate_id"] == contract.REPLACEMENT_CANDIDATE_ID
    assert payload["disposition"] == "KEEP_REDEFINED_BLOCKED"
    assert payload["approval_prerequisites_ready"] is False
    assert payload["approved_candidate_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["strict_validation_errors"] == []


def test_prerequisite_identity_and_order_are_frozen() -> None:
    matrix = _build()["prerequisite_matrix"]

    assert [item["prerequisite_id"] for item in matrix["rows"]] == list(
        contract.EXPECTED_PREREQUISITE_IDS
    )
    assert matrix["prerequisite_count"] == 10
    assert matrix["pass_count"] == 2
    assert matrix["blocked_count"] == 8
    assert matrix["all_prerequisites_ready"] is False


def test_only_identity_and_unscaled_semantic_are_currently_ready() -> None:
    rows = {
        item["prerequisite_id"]: item for item in _build()["prerequisite_matrix"]["rows"]
    }

    assert rows["replacement_identity_and_orthogonal_role_frozen"]["status"] == "PASS"
    assert rows["recovery_output_semantic_type_known"]["status"] == "PASS"
    assert rows["recovery_producer_pit_lineage_valid"]["status"] == "BLOCKED"
    assert rows["hard_veto_aggregate_contract_ready"]["status"] == "BLOCKED"
    assert rows["native_exposure_scalar_contract_ready"]["status"] == "BLOCKED"


def test_pending_screening_policy_metadata_remains_unmodified() -> None:
    sources = _sources()
    before = copy.deepcopy(sources["screening_policy"])

    payload = _build(sources)

    assert sources["screening_policy"] == before
    assert payload["screening_policy_status"] == "PENDING_OWNER_PREREGISTRATION"
    assert payload["screening_policy_approval_metadata_complete"] is False
    assert payload["replacement_candidate_decision"]["policy_approval_metadata_written"] is False
    assert payload["policy_approval_fabricated"] is False


def test_second_owner_approval_alone_cannot_unlock_candidate() -> None:
    sources = _sources()
    sources["owner_resolution"]["replacement_candidate_second_approval"] = {
        "candidate_id": contract.REPLACEMENT_CANDIDATE_ID,
        "decision": "APPROVE",
        "owner": "fixture_owner",
        "approved_at": "2026-07-10T00:00:00+09:00",
        "source_hash": "a" * 64,
    }

    payload = _build(sources)

    assert payload["disposition"] == "KEEP_REDEFINED_BLOCKED"
    assert payload["approved_candidate_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0


def test_technical_readiness_without_second_owner_approval_stays_blocked() -> None:
    sources = _sources()
    _make_adapter_contracts_ready(sources["adapter_readiness"])
    _approve_screening_policy(sources["screening_policy"])
    _resolve_native_scalar_decision(sources["owner_resolution"])

    payload = _build(sources)

    assert payload["prerequisite_matrix"]["blocked_count"] == 1
    assert payload["prerequisite_matrix"]["blocker_codes"] == [
        "SECOND_OWNER_APPROVAL_RECORDED"
    ]
    assert payload["disposition"] == "KEEP_REDEFINED_BLOCKED"


def test_all_prerequisites_and_explicit_second_approval_allow_contract() -> None:
    sources = _sources()
    _make_adapter_contracts_ready(sources["adapter_readiness"])
    _approve_screening_policy(sources["screening_policy"])
    _resolve_native_scalar_decision(sources["owner_resolution"])
    sources["owner_resolution"]["replacement_candidate_second_approval"] = {
        "candidate_id": contract.REPLACEMENT_CANDIDATE_ID,
        "decision": "APPROVE",
        "owner": "fixture_owner",
        "approved_at": "2026-07-10T00:00:00+09:00",
        "source_hash": "a" * 64,
    }

    payload = _build(sources)

    assert payload["disposition"] == "APPROVE"
    assert payload["approval_prerequisites_ready"] is True
    assert payload["approved_candidate_count"] == 1
    assert payload["m2_eligible_candidate_count"] == 1
    assert payload["approved_candidate_runtime_spec"]["maximum_active_steps"] == 1
    assert payload["approved_executor_binding"]["operation_type"] == (
        "CAPPED_RECOVERY_PERMISSION_OVERLAY"
    )


def test_approved_spec_never_changes_baseline_persistence_veto_or_tqqq() -> None:
    sources = _fully_ready_sources()

    spec = _build(sources)["approved_candidate_runtime_spec"]

    assert spec["changes_baseline_recovery_persistence"] is False
    assert spec["changes_hard_veto_behavior"] is False
    assert spec["tqqq_increase_allowed"] is False
    assert spec["same_step_application_allowed"] is False
    assert spec["maximum_active_steps"] == 1


def test_adapter_schema_drift_blocks_source_contract() -> None:
    sources = _sources()
    sources["adapter_readiness"]["schema_version"] = "drift.v1"

    payload = _build(sources)

    assert payload["status"] == contract.BLOCKED_STATUS
    assert "adapter_readiness_schema_mismatch" in payload["strict_validation_errors"]


def test_safety_boundary_records_no_runtime_replay_or_mutation() -> None:
    payload = _build()

    assert payload["runtime_code_invoked"] is False
    assert payload["replay_run"] is False
    assert payload["runtime_metrics_generated"] is False
    assert payload["candidate_behavior_implemented"] is False
    assert payload["baseline_behavior_changed"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_runner_writes_primary_two_sections_and_markdown(tmp_path: Path) -> None:
    adapter_path = tmp_path / "adapter.json"
    adapter_path.write_text(json.dumps(_adapter_readiness_fixture()), encoding="utf-8")

    payload = impl.run_growth_tilt_replacement_candidate_contract(
        adapter_readiness_path=adapter_path,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == contract.READY_STATUS
    for filename in (
        "growth_tilt_replacement_candidate_contract.json",
        "growth_tilt_replacement_candidate_prerequisite_matrix.json",
        "growth_tilt_replacement_candidate_decision.json",
    ):
        assert (tmp_path / "outputs" / filename).exists()
    assert (tmp_path / "docs" / "growth_tilt_replacement_candidate_contract.md").exists()


def test_runner_strict_mode_rejects_missing_adapter_readiness(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="adapter_readiness missing"):
        impl.run_growth_tilt_replacement_candidate_contract(
            adapter_readiness_path=tmp_path / "missing.json",
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_runs_blocked_gate_without_replay(tmp_path: Path) -> None:
    adapter_path = tmp_path / "adapter.json"
    adapter_path.write_text(json.dumps(_adapter_readiness_fixture()), encoding="utf-8")
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-replacement-candidate-contract",
            "--adapter-readiness",
            str(adapter_path),
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
    assert contract.READY_STATUS in result.output
    assert "disposition=KEEP_REDEFINED_BLOCKED" in result.output
    assert "approved_candidate_count=0" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_flow_and_requirement_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert entries[contract.REPORT_TYPE]["production_effect"] == "none"
    assert entries[contract.REPORT_TYPE]["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in contract.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in contract.REQUIRED_FLOW_REFERENCES
    )
    requirement = REQUIREMENT_PATH.read_text(encoding="utf-8")
    assert "replacement A approved only if all prerequisites are proven" in requirement
    assert "M2 eligible becomes 1, or remains 0 with exact blocker" in requirement


def _build(sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return contract.build_growth_tilt_replacement_candidate_contract(
        sources or _sources(),
        report_registry={"reports": [{"report_id": contract.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(contract.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(contract.REQUIRED_FLOW_REFERENCES),
        requirement_text=REQUIREMENT_PATH.read_text(encoding="utf-8"),
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    return {
        "owner_resolution": copy.deepcopy(safe_load_yaml_path(OWNER_RESOLUTION_PATH)),
        "adapter_readiness": _adapter_readiness_fixture(),
        "screening_policy": copy.deepcopy(safe_load_yaml_path(SCREENING_POLICY_PATH)),
    }


def _adapter_readiness_fixture() -> dict[str, Any]:
    return {
        "schema_version": contract.EXPECTED_ADAPTER_SCHEMA,
        "status": contract.EXPECTED_ADAPTER_STATUS,
        "m2_eligible_candidate_count": 0,
        "recovery_permission_adapter": {
            "status": "BLOCKED",
            "semantic_type": "UNSCALED_SCORE",
            "missing_pit_lineage_fields": [
                "as_of",
                "available_at",
                "known_at",
                "source_data_cutoff",
            ],
            "threshold_status": "BLOCKED_UNCALIBRATED_SCORE",
        },
        "hard_veto_aggregate_adapter": {
            "status": "BLOCKED_UNRESOLVED_HARD_VETO_AGGREGATE",
            "required_component_ids": [
                "risk_off_veto",
                "volatility_veto",
                "event_risk_veto",
                "trend_break_veto",
                "tqqq_veto",
            ],
        },
        "regime_transition_trace_adapter": {"status": "BLOCKED"},
        "native_exposure_scalar_adapter": {
            "status": "BLOCKED_NO_GOVERNED_NATIVE_SCALAR",
            "binding": {},
        },
    }


def _make_adapter_contracts_ready(adapter: dict[str, Any]) -> None:
    adapter["recovery_permission_adapter"].update(
        {
            "status": "READY",
            "missing_pit_lineage_fields": [],
            "threshold_status": "APPROVED_PREREGISTERED",
        }
    )
    adapter["hard_veto_aggregate_adapter"]["status"] = "READY"
    adapter["regime_transition_trace_adapter"]["status"] = "READY"
    adapter["native_exposure_scalar_adapter"].update(
        {
            "status": "READY",
            "binding": {
                "native_scalar_id": "baseline_risk_budget_scalar",
                "unit": "RISK_BUDGET_FRACTION",
                "current_scalar_field": "current_risk",
                "requested_target_scalar_field": "requested_risk",
                "applied_target_scalar_field": "applied_risk",
                "minimum_value": 0.0,
                "maximum_value": 1.0,
                "minimum_increment": 0.05,
            },
        }
    )


def _approve_screening_policy(policy: dict[str, Any]) -> None:
    policy.update(
        {
            "policy_status": "APPROVED",
            "approved_at": "2026-07-10T00:00:00+09:00",
            "approved_commit": "a" * 40,
            "source_hash": "b" * 64,
            "result_visibility_at_approval": "NONE",
        }
    )


def _resolve_native_scalar_decision(owner_resolution: dict[str, Any]) -> None:
    d18 = next(
        item
        for item in owner_resolution["decisions"]
        if item["decision_id"] == "D18"
    )
    d18["native_scalar_resolution"] = "RESOLVED_GOVERNED_NATIVE_SCALAR"


def _fully_ready_sources() -> dict[str, Any]:
    sources = _sources()
    _make_adapter_contracts_ready(sources["adapter_readiness"])
    _approve_screening_policy(sources["screening_policy"])
    _resolve_native_scalar_decision(sources["owner_resolution"])
    sources["owner_resolution"]["replacement_candidate_second_approval"] = {
        "candidate_id": contract.REPLACEMENT_CANDIDATE_ID,
        "decision": "APPROVE",
        "owner": "fixture_owner",
        "approved_at": "2026-07-10T00:00:00+09:00",
        "source_hash": "a" * 64,
    }
    return sources
