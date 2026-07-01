from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.exposure_cap_mechanics_simulation import (
    ARTIFACT_ROLE as SOURCE_2323_ARTIFACT_ROLE,
)
from ai_trading_system.exposure_cap_mechanics_simulation import (
    STATUS as SOURCE_2323_STATUS,
)
from ai_trading_system.exposure_cap_simulation_source_binding import (
    READY_WITH_SYNTHETIC_BASELINE,
    SAFETY_FIELDS,
    STATUS,
    TASK_ID,
    ExposureCapSimulationSourceBindingError,
    load_trading_2323_source_artifacts,
    run_exposure_cap_simulation_source_binding,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_exposure_cap_source_binding_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "exposure-cap-simulation-source-binding" in result.output


def test_exposure_cap_source_binding_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/exposure_cap_simulation_source_binding_policy.yaml")
    )

    assert policy["policy_id"] == "exposure_cap_simulation_source_binding_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_source_bound_dry_run"
    assert policy["task_id"] == TASK_ID
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["source_requirements"]["required_candidate_id"] == (
        "volatility_regime_scope_narrowed_risk_cap_v1"
    )
    assert policy["source_requirements"]["required_2323_status"] == SOURCE_2323_STATUS
    assert policy["source_requirements"]["allow_synthetic_observe_only_baseline"] is True
    assert policy["dry_run_policy"]["synthetic_baseline_source_mode"] == (
        "synthetic_observe_only"
    )
    assert policy["turnover_assumption"]["real_turnover_history_bound"] is False

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_exposure_cap_source_binding_writes_synthetic_dry_run_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    _force_scope_validation_forward_observe(fixture["scope_validation_dir"])
    source_2323 = _write_trading_2323_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    payload = run_exposure_cap_simulation_source_binding(
        simulation_policy_dir=source_2323,
        risk_cap_source_dir=fixture["scope_narrowed_generator_dir"],
        scope_validation_dir=fixture["scope_validation_dir"],
        market_data_source=fixture["prices_path"],
        rates_source=fixture["rates_path"],
        marketstack_prices_source=None,
        output_dir=output_dir,
        docs_root=docs_root,
    )

    assert payload["status"] == STATUS
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert payload["dry_run_readiness_status"] == READY_WITH_SYNTHETIC_BASELINE
    assert payload["portfolio_source_mode"] == "synthetic_observe_only"
    assert payload["real_portfolio_baseline_bound"] is False
    assert payload["dry_run_result_generated"] is True
    assert payload["dry_run_record_count"] > 0
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    required = [
        "exposure_cap_source_binding_summary.json",
        "exposure_cap_source_inventory.json",
        "exposure_cap_source_gap_matrix.json",
        "risk_cap_trigger_series_binding_report.json",
        "market_data_binding_report.json",
        "portfolio_baseline_binding_report.json",
        "turnover_rebalance_assumption_report.json",
        "source_bound_dry_run_simulation_readiness.json",
        "source_bound_dry_run_safety_boundary.json",
        "exposure_cap_simulation_next_task_route.json",
        "source_bound_exposure_cap_dry_run_result.json",
        "source_bound_exposure_cap_dry_run_result.csv",
        "exposure_cap_vs_no_cap_comparison.json",
        "exposure_cap_turnover_impact_report.json",
        "exposure_cap_cooldown_impact_report.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in ("target_weights.csv", "rebalance_instruction.json", "broker_order.json"):
        assert not (output_dir / filename).exists(), filename
    assert (docs_root / "exposure_cap_simulation_source_binding_report.md").exists()
    assert (docs_root / "exposure_cap_source_inventory.md").exists()

    readiness = _read_json(output_dir / "source_bound_dry_run_simulation_readiness.json")
    assert readiness["dry_run_readiness_status"] == READY_WITH_SYNTHETIC_BASELINE
    assert readiness["full_simulation_allowed"] is False
    assert readiness["portfolio_baseline_bound"] is True
    assert readiness["real_portfolio_baseline_bound"] is False

    comparison = _read_json(output_dir / "exposure_cap_vs_no_cap_comparison.json")
    assert comparison["portfolio_source_mode"] == "synthetic_observe_only"
    assert comparison["record_count"] == payload["dry_run_record_count"]
    assert comparison["interpretation_boundary"] == (
        "proxy_diagnostics_only_synthetic_observe_baseline"
    )
    assert comparison["promotion_allowed"] is False
    assert comparison["broker_action"] == "none"

    route = _read_json(output_dir / "exposure_cap_simulation_next_task_route.json")
    assert route["next_task"] == "TRADING-2325_Portfolio_Baseline_Source_Decision"


def test_exposure_cap_source_binding_cli_rejects_wrong_mode(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    source_2323 = _write_trading_2323_source(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "exposure-cap-simulation-source-binding",
            "--simulation-policy-dir",
            str(source_2323),
            "--risk-cap-source-dir",
            str(fixture["scope_narrowed_generator_dir"]),
            "--scope-validation-dir",
            str(fixture["scope_validation_dir"]),
            "--market-data-source",
            str(fixture["prices_path"]),
            "--rates-source",
            str(fixture["rates_path"]),
            "--marketstack-prices-source",
            "",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "production",
        ],
    )

    assert result.exit_code != 0


def test_exposure_cap_source_binding_fails_closed_on_unsafe_2323(
    tmp_path: Path,
) -> None:
    source_2323 = _write_trading_2323_source(tmp_path)
    summary_path = source_2323 / "exposure_cap_mechanics_simulation_summary.json"
    payload = _read_json(summary_path)
    payload["promotion_allowed"] = True
    payload["summary"]["promotion_allowed"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ExposureCapSimulationSourceBindingError, match="promotion"):
        load_trading_2323_source_artifacts(source_2323)


def test_exposure_cap_source_binding_registry_catalog_and_flow_are_safe() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "exposure_cap_simulation_source_binding"
    )

    assert entry["command"] == "aits research trends exposure-cap-simulation-source-binding"
    assert entry["artifact_role"] == "exposure_cap_simulation_source_binding"
    assert entry["validation_status"] == STATUS
    assert entry["data_quality_gate_required"] is True
    assert entry["dry_run_readiness_status"] == READY_WITH_SYNTHETIC_BASELINE
    assert entry["portfolio_source_mode"] == "synthetic_observe_only"
    assert entry["full_simulation_allowed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "TRADING-2324 Exposure-Cap Simulation Source Binding" in catalog
    assert "exposure-cap-simulation-source-binding" in catalog
    assert "source-bound dry-run readiness" in catalog
    assert "不是 target weight" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2324" in system_flow
    assert "exposure-cap-simulation-source-binding" in system_flow
    assert "data_quality_gate_executed=true" in system_flow
    assert "SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE" in system_flow


def _write_trading_2323_source(tmp_path: Path) -> Path:
    output_dir = tmp_path / "trading_2323"
    output_dir.mkdir()
    common = {
        "schema_version": "exposure_cap_mechanics_simulation.v1",
        "report_type": "exposure_cap_mechanics_simulation",
        "artifact_role": SOURCE_2323_ARTIFACT_ROLE,
        "task_id": "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION",
        "status": SOURCE_2323_STATUS,
        "candidate_id": "volatility_regime_scope_narrowed_risk_cap_v1",
        "source_blocked_no_simulation": True,
        "simulation_executed": False,
        "simulation_result_generated": False,
        "metric_result_generated": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "target_weight_generated": False,
        "rebalance_instruction_generated": False,
        "broker_order_generated": False,
        "paper_shadow_order_generated": False,
        "production_decision_generated": False,
    }
    _write_json(
        output_dir / "exposure_cap_mechanics_simulation_summary.json",
        {**common, "summary": dict(common)},
    )
    _write_json(
        output_dir / "exposure_cap_simulation_readiness_matrix.json",
        {**common, "rows": [{"simulation_objective": "false_risk_cap_cost", **common}]},
    )
    _write_json(
        output_dir / "exposure_cap_simulation_metric_contract.json",
        {**common, "metric_count": 4, "metrics": []},
    )
    _write_json(output_dir / "exposure_cap_simulation_safety_boundary.json", common)
    return output_dir


def _force_scope_validation_forward_observe(scope_validation_dir: Path) -> None:
    path = scope_validation_dir / "scope_narrowed_state_recommendation_matrix.json"
    payload = _read_json(path)
    for row in payload["candidate_rows"]:
        if row["scope_narrowed_candidate_id"] == (
            "volatility_regime_scope_narrowed_risk_cap_v1"
        ):
            row["recommended_research_status"] = (
                "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE"
            )
            row["forward_observe_candidate_recommendation"] = True
            row["usage_specific_status"] = "RISK_CAP_SCOPE_VALIDATED_LOCAL_EDGE"
            row["sample_sufficiency_status"] = "SAMPLE_SUFFICIENT"
            row["data_quality_status"] = "PASS_WITH_WARNINGS"
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
