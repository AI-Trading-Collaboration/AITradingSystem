from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    read_json,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    STATUS,
    run_source_bound_static_etf_dry_run,
)


def test_exposure_cap_vs_no_cap_comparison_is_written_from_static_baseline(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    payload = run_source_bound_static_etf_dry_run(
        source_binding_dir=fixture["source_binding_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        portfolio_config_dir=fixture["portfolio_config_dir"],
        market_data_source=fixture["prices_path"],
        rates_source=fixture["rates_path"],
        marketstack_prices_source=None,
        policy_path=fixture["policy_path"],
        quality_as_of="2023-01-10",
        output_dir=output_dir,
        docs_root=docs_root,
    )

    assert payload["status"] == STATUS
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    comparison = read_json(output_dir / "exposure_cap_vs_no_cap_static_etf_comparison.json")
    assert comparison["portfolio_source_mode"] == "static_etf_allocation_baseline"
    assert comparison["record_count"] == payload["record_count"]
    assert comparison["cap_binding_days"] > 0
    assert comparison["interpretation_boundary"] == (
        "static_etf_baseline_dry_run_proxy_diagnostics_only"
    )
    assert (docs_root / "exposure_cap_vs_no_cap_static_etf_comparison.md").exists()
