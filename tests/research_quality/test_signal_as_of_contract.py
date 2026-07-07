from __future__ import annotations

from ai_trading_system.research_quality.signal_as_of_contract import (
    build_signal_as_of_contract_schema,
    valid_signal_as_of_contract_example,
    validate_signal_as_of_contract,
)


def test_signal_as_of_contract_valid_example_passes() -> None:
    schema = build_signal_as_of_contract_schema()
    result = validate_signal_as_of_contract(valid_signal_as_of_contract_example())

    assert schema["schema_name"] == "signal_as_of_contract"
    assert "source_feature_ids_non_empty" in schema["invariants"]
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_signal_as_of_contract_missing_required_field_fails() -> None:
    payload = valid_signal_as_of_contract_example()
    payload.pop("source_feature_ids")

    result = validate_signal_as_of_contract(payload)

    assert result["valid"] is False
    assert any(error["code"] == "REQUIRED_FIELD_MISSING" for error in result["errors"])


def test_signal_as_of_contract_invalid_source_cutoff_order_fails() -> None:
    payload = valid_signal_as_of_contract_example()
    payload["generated_at"] = "2026-07-07T09:00:00"
    payload["source_data_cutoff"] = "2026-07-07T21:00:00"

    result = validate_signal_as_of_contract(payload)

    assert result["valid"] is False
    assert any(error["code"] == "INVALID_DATE_ORDER" for error in result["errors"])
