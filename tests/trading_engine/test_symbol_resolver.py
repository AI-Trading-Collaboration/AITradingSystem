from __future__ import annotations

from ai_trading_system.trading_engine.data.symbol_resolver import (
    canonical_symbol_for,
    resolve_symbol,
    source_symbol_for,
    symbol_mapping_payload,
)


def test_symbol_resolver_maps_brk_dot_b_to_source_symbol() -> None:
    assert source_symbol_for("BRK.B") == "BRK-B"
    assert canonical_symbol_for("BRK-B") == "BRK.B"

    resolution = resolve_symbol(
        "BRK.B",
        used_by=("repair", "diagnostics", "validate_data", "portfolio_sensitivity"),
    )

    assert resolution.canonical_symbol == "BRK.B"
    assert resolution.source_symbol == "BRK-B"
    assert resolution.mapping_status == "OK"
    assert "validate_data" in resolution.used_by


def test_symbol_resolver_detects_missing_manifest_mapping() -> None:
    resolution = resolve_symbol("BRK.B", manifest_mapping={"BRK.B": {"source_symbol": "BRK.B"}})

    assert resolution.mapping_status == "SYMBOL_MAPPING_MISSING"


def test_symbol_mapping_payload_is_auditable() -> None:
    payload = symbol_mapping_payload(["GOOGL", "BRK.B"], used_by=("repair", "validate_data"))

    assert payload["GOOGL"]["source_symbol"] == "GOOGL"
    assert payload["BRK.B"]["source_symbol"] == "BRK-B"
    assert payload["BRK.B"]["used_by"] == ["repair", "validate_data"]
