from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    run_source_bound_static_etf_dry_run,
)


def build_exposure_cap_diagnostics_review_fixture(tmp_path: Path) -> dict[str, Path]:
    source_fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    dry_run_dir = tmp_path / "dry_run"
    run_source_bound_static_etf_dry_run(
        source_binding_dir=source_fixture["source_binding_dir"],
        baseline_decision_dir=source_fixture["baseline_decision_dir"],
        simulation_policy_dir=source_fixture["simulation_policy_dir"],
        portfolio_config_dir=source_fixture["portfolio_config_dir"],
        market_data_source=source_fixture["prices_path"],
        rates_source=source_fixture["rates_path"],
        policy_path=source_fixture["policy_path"],
        quality_as_of="2023-01-10",
        output_dir=dry_run_dir,
        docs_root=tmp_path / "dry_run_docs",
    )
    return {
        "dry_run_dir": dry_run_dir,
        "source_binding_dir": source_fixture["source_binding_dir"],
        "baseline_decision_dir": source_fixture["baseline_decision_dir"],
        "simulation_policy_dir": source_fixture["simulation_policy_dir"],
        "prices_path": source_fixture["prices_path"],
        "rates_path": source_fixture["rates_path"],
        "policy_path": source_fixture["policy_path"],
    }


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
