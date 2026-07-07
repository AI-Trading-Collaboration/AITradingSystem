from __future__ import annotations

from typing import Any

from ai_trading_system.research_quality.signal_as_of_contract import (
    build_signal_as_of_contract_schema,
    valid_signal_as_of_contract_example,
    validate_signal_as_of_contract,
)
from ai_trading_system.research_quality.signal_validity_contract import (
    build_signal_validity_contract_schema,
    valid_signal_validity_contract_example,
    validate_signal_validity_contract,
)
from ai_trading_system.research_quality.source_feature_traceability_contract import (
    build_source_feature_traceability_contract_schema,
    valid_source_feature_traceability_contract_example,
    validate_source_feature_traceability_contract,
)

SNAPSHOT_SCHEMA_VERSION = "signal_contract_schema_snapshot.v1"


def build_signal_contract_schema_snapshot() -> dict[str, Any]:
    signal_as_of_schema = build_signal_as_of_contract_schema()
    source_feature_schema = build_source_feature_traceability_contract_schema()
    signal_validity_schema = build_signal_validity_contract_schema()
    self_test_results = {
        "signal_as_of_contract": validate_signal_as_of_contract(
            valid_signal_as_of_contract_example()
        ),
        "source_feature_traceability_contract": (
            validate_source_feature_traceability_contract(
                valid_source_feature_traceability_contract_example()
            )
        ),
        "signal_validity_contract": validate_signal_validity_contract(
            valid_signal_validity_contract_example()
        ),
    }
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "signal_as_of_contract": _snapshot_row(signal_as_of_schema),
        "source_feature_traceability_contract": _snapshot_row(source_feature_schema),
        "signal_validity_contract": _snapshot_row(signal_validity_schema),
        "schema_validation_helpers_ready": all(
            result.get("valid") is True for result in self_test_results.values()
        ),
        "contract_snapshot_ready": True,
        "validator_self_test_results": self_test_results,
        "contract_adoption_checklist": [
            "map source features to source_feature_traceability_contract",
            "map strategy signal outputs to signal_as_of_contract",
            "map valid_from / valid_until / stale_after fields to signal_validity_contract",
            "run as-of replay validation before blocker downgrade review",
            "regenerate PIT gate before candidate search reconsideration",
            "record owner review before any downgrade from BLOCKING",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def build_pit_gate_integration_plan() -> dict[str, Any]:
    return {
        "schema_version": "signal_contract_pit_gate_integration_plan.v1",
        "current_pit_gate_source": "config/research/dynamic_strategy_pit_input_registry.yaml",
        "future_integration": [
            "source feature contracts feed PIT matrix rows",
            "signal as-of contracts feed signal PIT status",
            "signal validity contracts feed valid_until_window status",
            "replay validation feeds blocker downgrade evidence",
        ],
        "current_gate_change_in_2409": "none",
        "current_gate_result": {
            "candidate_search_allowed": False,
            "research_only_observation_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
        },
        "reconsider_candidate_search_only_after": [
            "growth_tilt_engine contract mapping completed",
            "valid_until_window contract mapping completed",
            "as-of replay validation dry run completed",
            "PIT gate regenerated",
            "owner review records blocker downgrade approval",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _snapshot_row(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_name": schema.get("schema_name"),
        "schema_version": schema.get("schema_version"),
        "schema_ready": True,
        "required_field_count": len(schema.get("required_fields", [])),
        "invariant_count": len(schema.get("invariants", [])),
        "validation_error_codes": list(schema.get("validation_error_codes", [])),
    }
