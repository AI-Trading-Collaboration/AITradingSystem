from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    build_high_intensity_runtime_event_append_contract,
)


def test_runtime_event_append_contract_is_append_only() -> None:
    contract = build_high_intensity_runtime_event_append_contract()

    assert contract["append_mode"] == "append_only"
    assert contract["event_id_deterministic"] is True
    assert contract["original_event_log_mutation_allowed"] is False
    assert contract["dedup_required"] is True
    assert "broker_action" in contract["blocked_appended_fields"]
    assert contract["production_allowed"] is False
