from __future__ import annotations

from ai_trading_system.research_quality.signal_validity_contract import (
    build_signal_validity_contract_schema,
    valid_signal_validity_contract_example,
    validate_signal_validity_contract,
)


def test_signal_validity_contract_valid_example_passes() -> None:
    schema = build_signal_validity_contract_schema()
    result = validate_signal_validity_contract(valid_signal_validity_contract_example())

    assert schema["schema_name"] == "signal_validity_contract"
    assert "valid_until > valid_from" in schema["invariants"]
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_signal_validity_contract_valid_until_before_valid_from_fails() -> None:
    payload = valid_signal_validity_contract_example()
    payload["valid_until"] = payload["valid_from"]

    result = validate_signal_validity_contract(payload)

    assert result["valid"] is False
    assert any(error["code"] == "INVALID_DATE_ORDER" for error in result["errors"])


def test_signal_validity_contract_stale_after_after_valid_until_fails() -> None:
    payload = valid_signal_validity_contract_example()
    payload["stale_after"] = "2026-07-15T21:00:00"

    result = validate_signal_validity_contract(payload)

    assert result["valid"] is False
    assert any(
        error["field"] == "stale_after" and error["code"] == "INVALID_DATE_ORDER"
        for error in result["errors"]
    )
