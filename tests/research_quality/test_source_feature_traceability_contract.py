from __future__ import annotations

from ai_trading_system.research_quality.source_feature_traceability_contract import (
    build_source_feature_traceability_contract_schema,
    valid_source_feature_traceability_contract_example,
    validate_source_feature_traceability_contract,
)


def test_source_feature_traceability_contract_valid_example_passes() -> None:
    schema = build_source_feature_traceability_contract_schema()
    result = validate_source_feature_traceability_contract(
        valid_source_feature_traceability_contract_example()
    )

    assert schema["schema_name"] == "source_feature_traceability_contract"
    assert "forward_window_used=false_for_TRUE_PIT_features" in schema["invariants"]
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_source_feature_true_pit_forward_window_fails() -> None:
    payload = valid_source_feature_traceability_contract_example()
    payload["pit_status"] = "TRUE_PIT"
    payload["pit_confidence"] = "HIGH"
    payload["forward_window_used"] = True

    result = validate_source_feature_traceability_contract(payload)

    assert result["valid"] is False
    assert any(
        error["code"] == "FORWARD_WINDOW_CONFLICTS_WITH_TRUE_PIT"
        for error in result["errors"]
    )


def test_source_feature_unknown_pit_confidence_consistency_checked() -> None:
    payload = valid_source_feature_traceability_contract_example()
    payload["pit_status"] = "UNKNOWN"
    payload["pit_confidence"] = "HIGH"

    result = validate_source_feature_traceability_contract(payload)

    assert result["valid"] is False
    assert any(
        error["code"] == "INVALID_PIT_CONFIDENCE_COMBINATION"
        for error in result["errors"]
    )
