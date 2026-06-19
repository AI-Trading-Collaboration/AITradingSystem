from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.etf_portfolio.weight_research_interfaces import (
    build_dependency_boundary_validation,
    build_research_layer_interface_contract,
    build_signal_diagnostics_framework_contract,
    build_signal_diagnostics_report,
)


def test_research_layer_interface_contract_freezes_five_layers() -> None:
    payload = build_research_layer_interface_contract()

    assert payload["status"] == "RESEARCH_LAYER_INTERFACE_CONTRACT_READY"
    assert [layer["layer_id"] for layer in payload["layers"]] == [
        "feature",
        "signal",
        "target",
        "execution",
        "evaluation",
    ]
    assert "target_weight" in payload["layers"][1]["forbidden_outputs"]
    assert "modified_signal" in payload["layers"][4]["forbidden_outputs"]
    assert payload["safety_boundary"]["official_target_weights"] is False


def test_dependency_boundary_validation_rejects_p0_signal_allocator_imports() -> None:
    payload = build_dependency_boundary_validation()

    assert payload["status"] == "PASS"
    assert "phase0_runner_avoids_p0_signal_allocator_imports" not in payload[
        "blocking_checks"
    ]
    assert payload["safety_boundary"]["production_effect"] == "none"


def test_signal_diagnostics_framework_contract_is_signal_only() -> None:
    payload = build_signal_diagnostics_framework_contract()

    assert payload["status"] == "SIGNAL_DIAGNOSTICS_FRAMEWORK_READY"
    assert payload["runner_contract"]["evaluates_signal_only"] is True
    assert payload["runner_contract"]["evaluates_portfolio_return"] is False
    assert "target_weight" in payload["forbidden_outputs"]


def test_signal_diagnostics_report_passes_clean_signal_fixture() -> None:
    frame = pd.DataFrame(
        [
            {
                "date": "2023-01-03",
                "symbol": "SPY",
                "signal_score": 0.1,
                "state": "NORMAL",
                "confidence": 0.8,
            },
            {
                "date": "2023-01-04",
                "symbol": "SPY",
                "signal_score": 0.2,
                "state": "NORMAL",
                "confidence": 0.8,
            },
        ]
    )

    payload = build_signal_diagnostics_report(
        frame,
        signal_artifact_id="fixture_signal",
        as_of=date(2023, 1, 5),
    )

    assert payload["status"] == "SIGNAL_DIAGNOSTICS_PASS"
    assert payload["evaluates_portfolio_return"] is False
    assert payload["metrics"]["row_count"] == 2


def test_signal_diagnostics_report_blocks_missing_required_columns() -> None:
    payload = build_signal_diagnostics_report(
        pd.DataFrame([{"date": "2023-01-03", "symbol": "SPY"}]),
        signal_artifact_id="broken_signal",
        as_of=date(2023, 1, 5),
    )

    assert payload["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED"
    assert payload["fail_closed_reason"] == "missing_required_columns_or_empty_signal"
