from __future__ import annotations

import copy
import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_baseline_contract_adapters_readiness as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_baseline_contract_adapters as adapters,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

OWNER_RESOLUTION_PATH = Path(
    "inputs/research_reviews/growth_tilt_owner_decision_resolution.yaml"
)
HARD_VETO_MATRIX_PATH = Path(
    "outputs/research_strategies/growth_tilt_baseline_contract_decision_pack/"
    "growth_tilt_hard_veto_resolution_matrix.json"
)
SIGNAL_INVENTORY_PATH = Path(
    "outputs/research_strategies/growth_tilt_owner_mapping_inventory/"
    "baseline_signal_inventory.json"
)
EXPOSURE_INVENTORY_PATH = Path(
    "outputs/research_strategies/growth_tilt_owner_mapping_inventory/"
    "baseline_exposure_unit_inventory.json"
)
TRANSITION_SOURCE_PATH = Path(
    "inputs/research_reviews/growth_tilt_baseline_transition_trace_source.csv"
)
PREDICTIONS_PATH = Path(
    "outputs/research_trends/channel_specific_v3/channel_composer_v3_predictions.csv"
)
REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2438M1D1A_Growth_Tilt_Owner_Decision_Resolution_And_"
    "Candidate_A_Reframing_Plan.md"
)


def test_real_hard_veto_adapter_blocks_three_unresolved_components() -> None:
    payload = _build()
    section = payload["hard_veto_aggregate_adapter"]

    assert section["status"] == "BLOCKED_UNRESOLVED_HARD_VETO_AGGREGATE"
    assert section["resolved_component_ids"] == ["volatility_veto", "tqqq_veto"]
    assert section["unresolved_component_ids"] == [
        "risk_off_veto",
        "event_risk_veto",
        "trend_break_veto",
    ]
    assert any(
        "BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS" in item
        for item in section["blocker_codes"]
    )
    assert any(
        "BLOCKED_NO_CALLABLE_PRODUCER" in item
        for item in section["blocker_codes"]
    )


def test_unresolved_hard_veto_aggregate_materializes_blocked_not_false() -> None:
    adapter = _build()["hard_veto_aggregate_adapter"]

    trace = adapters.materialize_hard_veto_aggregate(adapter, {})

    assert trace["status"] == "BLOCKED"
    assert trace["active"] is None
    assert trace["missing_component_policy"] == "BLOCKED_NOT_FALSE"
    assert trace["candidate_component_removal_allowed"] is False


def test_resolved_hard_veto_values_materialize_active_ids() -> None:
    adapter = adapters.build_hard_veto_aggregate_adapter(_resolved_components())
    values = {
        veto_id: {
            "value": veto_id == "event_risk_veto",
            "pit_valid": True,
            "known_at": "2026-07-09",
            "available_at": "2026-07-10",
            "source_artifact_ref": f"fixture:{veto_id}",
        }
        for veto_id in adapters.EXPECTED_VETO_IDS
    }

    trace = adapters.materialize_hard_veto_aggregate(adapter, values)

    assert adapter["status"] == "READY"
    assert trace["status"] == "READY"
    assert trace["active"] is True
    assert trace["active_component_ids"] == ["event_risk_veto"]


def test_missing_runtime_hard_veto_value_blocks_aggregate() -> None:
    adapter = adapters.build_hard_veto_aggregate_adapter(_resolved_components())
    values = {
        veto_id: {
            "value": False,
            "pit_valid": True,
            "known_at": "2026-07-09",
            "available_at": "2026-07-10",
        }
        for veto_id in adapters.EXPECTED_VETO_IDS[:-1]
    }

    trace = adapters.materialize_hard_veto_aggregate(adapter, values)

    assert trace["status"] == "BLOCKED"
    assert trace["active"] is None
    assert "HARD_VETO_RUNTIME_VALUE_MISSING:tqqq_veto" in trace["blocker_codes"]


def test_unknown_hard_veto_runtime_input_is_rejected() -> None:
    adapter = adapters.build_hard_veto_aggregate_adapter(_resolved_components())
    values = {
        veto_id: {
            "value": False,
            "pit_valid": True,
            "known_at": "2026-07-09",
            "available_at": "2026-07-10",
        }
        for veto_id in adapters.EXPECTED_VETO_IDS
    }
    values["raw_vix"] = values["risk_off_veto"]

    trace = adapters.materialize_hard_veto_aggregate(adapter, values)

    assert "UNKNOWN_OR_NON_CALLABLE_HARD_VETO_VALUE" in trace["blocker_codes"]
    assert trace["active"] is None


def test_real_transition_rows_materialize_current_but_not_requested_or_applied() -> None:
    section = _build()["regime_transition_trace_adapter"]

    assert section["status"] == "BLOCKED"
    assert section["record_count"] > 0
    assert section["ready_record_count"] == 0
    assert section["used_adjacent_row_inference"] is False
    first = section["records"][0]
    assert first["current_state"] in adapters.EXPECTED_TREND_STATES
    assert first["requested_target_state"] is None
    assert first["applied_target_state"] is None


def test_complete_transition_trace_requires_next_step_application() -> None:
    section = adapters.materialize_regime_transition_trace(
        [
            {
                "date": "2026-07-09",
                "current_state": "defensive",
                "requested_target_state": "neutral",
                "applied_target_state": "neutral",
                "known_at": "2026-07-09",
                "available_at": "2026-07-09",
                "request_created_at": "2026-07-09",
                "applied_at": "2026-07-10",
            }
        ]
    )

    assert section["status"] == "READY"
    assert section["same_step_application_allowed"] is False
    assert section["ordered_priority"] == list(adapters.TRANSITION_PRIORITY)


def test_same_step_transition_application_is_blocked() -> None:
    section = adapters.materialize_regime_transition_trace(
        [
            {
                "current_state": "defensive",
                "requested_target_state": "neutral",
                "applied_target_state": "neutral",
                "request_created_at": "2026-07-10",
                "applied_at": "2026-07-10",
            }
        ]
    )

    assert section["status"] == "BLOCKED"
    assert "SAME_STEP_APPLICATION_PROHIBITED" in section["blocker_codes"]


def test_missing_native_scalar_binding_stays_exactly_blocked() -> None:
    section = adapters.build_native_exposure_scalar_adapter({})

    assert section["status"] == "BLOCKED_NO_GOVERNED_NATIVE_SCALAR"
    assert section["scalar_binding_ready"] is False
    assert section["qqq_equivalent_candidate_delta_allowed"] is False
    assert section["tqqq_increase_allowed"] is False
    assert section["candidate_delta_materialized"] is False


def test_instrument_named_scalar_substitution_is_prohibited() -> None:
    binding = _ready_scalar_binding()
    binding["native_scalar_id"] = "QQQ_equivalent_exposure"

    section = adapters.build_native_exposure_scalar_adapter(binding)

    assert section["scalar_binding_ready"] is False
    assert "INSTRUMENT_NAME_SCALAR_SUBSTITUTION_PROHIBITED" in section["blocker_codes"]


def test_ready_native_scalar_binding_materializes_three_fields() -> None:
    adapter = adapters.build_native_exposure_scalar_adapter(_ready_scalar_binding())
    trace = adapters.materialize_native_exposure_scalar_trace(
        adapter,
        {
            "current_risk": 0.4,
            "requested_risk": 0.5,
            "applied_risk": 0.45,
            "pit_valid": True,
            "known_at": "2026-07-09",
            "available_at": "2026-07-10",
            "source_artifact_ref": "fixture:scalar",
        },
    )

    assert adapter["status"] == "READY"
    assert trace["status"] == "READY"
    assert trace["current_scalar"] == 0.4
    assert trace["requested_target_scalar"] == 0.5
    assert trace["applied_target_scalar"] == 0.45


def test_native_scalar_out_of_range_fails_closed() -> None:
    adapter = adapters.build_native_exposure_scalar_adapter(_ready_scalar_binding())
    trace = adapters.materialize_native_exposure_scalar_trace(
        adapter,
        {
            "current_risk": 0.4,
            "requested_risk": 1.5,
            "applied_risk": 0.45,
            "pit_valid": True,
            "known_at": "2026-07-09",
            "available_at": "2026-07-10",
            "source_artifact_ref": "fixture:scalar",
        },
    )

    assert trace["status"] == "BLOCKED"
    assert "SCALAR_VALUE_OUT_OF_RANGE:requested_target_scalar" in trace["blocker_codes"]


def test_real_recovery_adapter_records_unscaled_score_and_two_blockers() -> None:
    section = _build()["recovery_permission_adapter"]

    assert section["semantic_type"] == "UNSCALED_SCORE"
    assert section["probability_interpretation_allowed"] is False
    assert section["missing_pit_lineage_fields"] == [
        "as_of",
        "available_at",
        "known_at",
        "source_data_cutoff",
    ]
    assert section["threshold_status"] == "BLOCKED_UNCALIBRATED_SCORE"
    assert section["default_threshold_allowed"] is False
    assert section["trigger_materialized"] is False


def test_recovery_adapter_never_emits_baseline_transition_or_persistence() -> None:
    producer = {
        "signal_id": "re_risk_allowed_probability",
        "producer_callable": True,
        "output_path": "fixture:recovery",
        "pit_lineage_valid": True,
    }
    section = adapters.build_recovery_permission_adapter(
        producer,
        prediction_header_fields=list(adapters.PIT_LINEAGE_FIELDS),
        threshold_decision={"threshold_status": "APPROVED_PREREGISTERED"},
    )

    assert section["status"] == "READY"
    assert section["trigger_materialized"] is False
    assert section["baseline_transition_emitted"] is False
    assert section["baseline_recovery_persistence_created"] is False


def test_real_readiness_has_four_blocked_adapters_and_zero_m2_candidates() -> None:
    payload = _build()

    assert payload["status"] == adapters.READY_STATUS
    assert payload["adapter_implementation_count"] == 4
    assert payload["adapter_contract_ready_count"] == 0
    assert payload["adapter_contract_blocked_count"] == 4
    assert payload["replacement_a_ready_for_m1e_approval"] is False
    assert payload["approved_candidate_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["strict_validation_errors"] == []


def test_owner_disposition_drift_blocks_source_contract() -> None:
    sources = _sources()
    sources["owner_resolution"]["candidate_disposition"]["approved_candidate_count"] = 1

    payload = _build(sources)

    assert payload["status"] == adapters.BLOCKED_STATUS
    assert "owner_candidate_disposition_mismatch" in payload["strict_validation_errors"]


def test_empty_transition_source_is_a_strict_contract_error() -> None:
    sources = _sources()
    sources["transition_rows"] = []

    payload = _build(sources)

    assert "transition_source_rows_missing" in payload["strict_validation_errors"]
    assert payload["status"] == adapters.BLOCKED_STATUS


def test_safety_boundary_records_no_runtime_or_candidate_behavior() -> None:
    payload = _build()

    assert payload["runtime_code_invoked"] is False
    assert payload["replay_run"] is False
    assert payload["runtime_metrics_generated"] is False
    assert payload["baseline_recovery_persistence_created"] is False
    assert payload["baseline_transition_behavior_changed"] is False
    assert payload["candidate_behavior_implemented"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_runner_writes_primary_four_sections_and_markdown(tmp_path: Path) -> None:
    payload = impl.run_growth_tilt_baseline_contract_adapters_readiness(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == adapters.READY_STATUS
    for filename in (
        "growth_tilt_baseline_contract_adapters_readiness.json",
        "growth_tilt_hard_veto_aggregate_adapter.json",
        "growth_tilt_regime_transition_trace_adapter.json",
        "growth_tilt_native_exposure_scalar_adapter.json",
        "growth_tilt_recovery_permission_adapter.json",
    ):
        assert (tmp_path / "outputs" / filename).exists()
    assert (tmp_path / "docs" / "growth_tilt_baseline_contract_adapters_readiness.md").exists()


def test_runner_strict_mode_rejects_missing_hard_veto_matrix(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hard_veto_matrix missing"):
        impl.run_growth_tilt_baseline_contract_adapters_readiness(
            hard_veto_matrix_path=tmp_path / "missing.json",
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_runs_adapter_readiness_without_replay(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-baseline-contract-adapters-readiness",
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
    assert adapters.READY_STATUS in result.output
    assert "adapter_implementation_count=4" in result.output
    assert "adapter_contract_blocked_count=4" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_flow_and_requirement_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert entries[adapters.REPORT_TYPE]["production_effect"] == "none"
    assert entries[adapters.REPORT_TYPE]["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in adapters.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in adapters.REQUIRED_FLOW_REFERENCES
    )
    requirement = REQUIREMENT_PATH.read_text(encoding="utf-8")
    assert "existing baseline behavior exposed through governed adapters" in requirement
    assert "No real PIT replay runs in M1D2" in requirement


def _build(sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return adapters.build_growth_tilt_baseline_contract_adapters_readiness(
        sources or _sources(),
        report_registry={"reports": [{"report_id": adapters.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(adapters.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(adapters.REQUIRED_FLOW_REFERENCES),
        requirement_text=REQUIREMENT_PATH.read_text(encoding="utf-8"),
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    with TRANSITION_SOURCE_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        transition_rows = [dict(item) for item in csv.DictReader(handle)]
    with PREDICTIONS_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        prediction_header_fields = next(csv.reader(handle))
    return {
        "owner_resolution": copy.deepcopy(safe_load_yaml_path(OWNER_RESOLUTION_PATH)),
        "hard_veto_matrix": json.loads(HARD_VETO_MATRIX_PATH.read_text(encoding="utf-8")),
        "signal_inventory": json.loads(SIGNAL_INVENTORY_PATH.read_text(encoding="utf-8")),
        "exposure_inventory": json.loads(
            EXPOSURE_INVENTORY_PATH.read_text(encoding="utf-8")
        ),
        "transition_rows": transition_rows,
        "prediction_header_fields": prediction_header_fields,
    }


def _resolved_components() -> list[dict[str, Any]]:
    return [
        {
            "veto_id": veto_id,
            "resolution_status": "RESOLVED_CALLABLE",
            "producer_callable": True,
            "producer_entrypoint": f"fixture.{veto_id}",
            "output_path": f"signal_state.{veto_id}",
            "pit_lineage_ref": f"fixture:{veto_id}:pit",
            "missing_policy": "BLOCKED_NOT_FALSE",
            "priority": "BEFORE_CANDIDATE_OVERLAY",
        }
        for veto_id in adapters.EXPECTED_VETO_IDS
    ]


def _ready_scalar_binding() -> dict[str, Any]:
    return {
        "native_scalar_id": "baseline_risk_budget_scalar",
        "unit": "RISK_BUDGET_FRACTION",
        "current_scalar_field": "current_risk",
        "requested_target_scalar_field": "requested_risk",
        "applied_target_scalar_field": "applied_risk",
        "minimum_value": 0.0,
        "maximum_value": 1.0,
        "minimum_increment": 0.05,
        "pit_lineage_ref": "fixture:native-scalar:pit",
        "owner_semantics_status": "APPROVED",
    }
