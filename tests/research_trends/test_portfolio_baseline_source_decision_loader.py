from __future__ import annotations

import json
from pathlib import Path

import pytest
from portfolio_baseline_source_decision_fixtures import (
    read_json,
    write_simulation_policy_fixture,
    write_source_binding_fixture,
)

from ai_trading_system.portfolio_baseline_source_decision import (
    PortfolioBaselineSourceDecisionError,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
)


def test_loader_reads_2324_source_binding_outputs(tmp_path: Path) -> None:
    source_dir = write_source_binding_fixture(tmp_path)

    payload = load_source_binding_outputs(source_dir)

    assert payload["summary"]["dry_run_readiness_status"] == (
        "SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE"
    )
    assert payload["portfolio_baseline_binding"]["portfolio_source_mode"] == (
        "synthetic_observe_only"
    )
    assert "cap_comparison" in payload["optional_payloads"]


def test_loader_reads_2323_policy_outputs(tmp_path: Path) -> None:
    simulation_dir = write_simulation_policy_fixture(tmp_path)

    payload = load_simulation_policy_outputs(simulation_dir)

    assert payload["summary"]["status"] == (
        "EXPOSURE_CAP_MECHANICS_SIMULATION_SOURCE_BLOCKED_NOT_EXECUTED"
    )
    assert payload["safety_boundary"]["simulation_executed"] is False


def test_loader_fails_when_source_binding_summary_missing(tmp_path: Path) -> None:
    source_dir = write_source_binding_fixture(tmp_path)
    (source_dir / "exposure_cap_source_binding_summary.json").unlink()

    with pytest.raises(
        PortfolioBaselineSourceDecisionError,
        match="source binding summary",
    ):
        load_source_binding_outputs(source_dir)


def test_loader_fails_closed_when_input_opens_promotion(tmp_path: Path) -> None:
    source_dir = write_source_binding_fixture(tmp_path)
    summary_path = source_dir / "exposure_cap_source_binding_summary.json"
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(PortfolioBaselineSourceDecisionError, match="promotion"):
        load_source_binding_outputs(source_dir)


def test_loader_fails_closed_when_input_opens_broker_action(tmp_path: Path) -> None:
    source_dir = write_source_binding_fixture(tmp_path)
    summary_path = source_dir / "exposure_cap_source_binding_summary.json"
    payload = read_json(summary_path)
    payload["broker_action"] = "buy"
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(PortfolioBaselineSourceDecisionError, match="broker_action"):
        load_source_binding_outputs(source_dir)
