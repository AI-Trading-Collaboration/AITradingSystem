from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_baseline_capability_graph as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_baseline_capability_graph as graph,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

GRAPH_CONFIG_PATH = Path(
    "config/research/growth_tilt_baseline_capability_graph.yaml"
)
REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2438N_Growth_Tilt_Candidate_Family_Closure_And_"
    "Contract_First_Discovery_Pivot.md"
)


def test_real_graph_is_ready_with_zero_mutation_ready_capabilities() -> None:
    payload = _build()

    assert payload["status"] == graph.READY_ZERO_STATUS
    assert payload["node_count"] == 21
    assert payload["edge_count"] == 15
    assert payload["readiness_status_counts"] == {
        "READY": 6,
        "BLOCKED": 9,
        "DIAGNOSTIC_ONLY": 2,
        "NOT_APPLICABLE": 4,
    }
    assert payload["mutation_ready_capability_count"] == 0
    assert payload["mutation_ready_capability_ids"] == []
    assert payload["strict_validation_errors"] == []


def test_capability_identity_and_order_are_frozen() -> None:
    nodes = _build()["nodes"]

    assert [item["capability_id"] for item in nodes] == list(
        graph.EXPECTED_NODE_IDS
    )
    assert all(item["schema_version"] == graph.NODE_SCHEMA_VERSION for item in nodes)


def test_callable_but_unconsumed_is_never_mutation_ready() -> None:
    payload = _build()
    by_id = {item["capability_id"]: item for item in payload["nodes"]}

    assert payload["callable_but_unconsumed_capability_ids"] == [
        "signal_re_risk_allowed_probability",
        "expiry_rule_signal_validity",
        "replay_runner_candidate_overlay_executor",
    ]
    recovery = by_id["signal_re_risk_allowed_probability"]
    assert recovery["runtime"]["callable"] is True
    assert recovery["runtime"]["consumed_by_baseline"] is False
    assert recovery["mutation_ready"] is False
    assert "NOT_CONSUMED_BY_BASELINE" in recovery["mutation_blocker_codes"]


def test_ready_signal_contract_still_lacks_an_approved_mutation_contract() -> None:
    node = _node("signal_growth_allowed")

    assert node["capability_contract_ready"] is True
    assert node["readiness"]["status"] == "READY"
    assert node["mutation_ready"] is False
    assert "RUNNER_BINDING_MISSING" in node["mutation_blocker_codes"]
    assert "NO_APPROVED_MUTABLE_DIMENSION" in node["mutation_blocker_codes"]


def test_individual_veto_readiness_does_not_imply_aggregate_readiness() -> None:
    by_id = {item["capability_id"]: item for item in _build()["nodes"]}

    assert by_id["hard_veto_volatility"]["readiness"]["status"] == "READY"
    assert by_id["hard_veto_tqqq"]["readiness"]["status"] == "READY"
    aggregate = by_id["hard_veto_aggregate"]
    assert aggregate["readiness"]["status"] == "BLOCKED"
    assert aggregate["mutation_ready"] is False


def test_transition_native_scalar_and_ramp_remain_distinct_blockers() -> None:
    by_id = {item["capability_id"]: item for item in _build()["nodes"]}

    for capability_id in (
        "transition_regime_current_requested_applied",
        "exposure_scalar_native",
    ):
        assert by_id[capability_id]["readiness"]["status"] == "BLOCKED"
        assert by_id[capability_id]["mutation_ready"] is False
    assert by_id["ramp_rule_recovery"]["readiness"]["status"] == "NOT_APPLICABLE"
    assert by_id["ramp_rule_recovery"]["mutation_ready"] is False


def test_ready_caps_are_capabilities_not_candidate_delta_units() -> None:
    by_id = {item["capability_id"]: item for item in _build()["nodes"]}

    for capability_id in (
        "exposure_cap_qqq_equivalent",
        "exposure_cap_turnover",
    ):
        node = by_id[capability_id]
        assert node["readiness"]["status"] == "READY"
        assert node["capability_contract_ready"] is True
        assert node["mutation_ready"] is False
        assert "NO_APPROVED_MUTABLE_DIMENSION" in node["mutation_blocker_codes"]


def test_diagnostic_executor_is_not_misrepresented_as_pit_runner() -> None:
    by_id = {item["capability_id"]: item for item in _build()["nodes"]}
    executor = by_id["replay_runner_candidate_overlay_executor"]
    runner = by_id["replay_runner_growth_tilt_pit"]

    assert executor["readiness"]["status"] == "DIAGNOSTIC_ONLY"
    assert executor["replay"]["runner_binding"] is None
    assert runner["readiness"]["status"] == "BLOCKED"
    assert executor["mutation_ready"] is False
    assert runner["mutation_ready"] is False


def test_edges_are_resolved_but_do_not_create_missing_runtime_contracts() -> None:
    payload = _build()

    assert all(item["from_capability"] in graph.EXPECTED_NODE_IDS for item in payload["edges"])
    assert all(item["to_capability"] in graph.EXPECTED_NODE_IDS for item in payload["edges"])
    assert any(item["edge_ready"] is False for item in payload["edges"])
    assert payload["new_transition_created"] is False
    assert payload["new_persistence_created"] is False
    assert payload["new_veto_created"] is False
    assert payload["new_exposure_unit_created"] is False


def test_zero_mutation_ready_count_keeps_n3_and_n4_not_started() -> None:
    payload = _build()

    assert payload["n3_candidate_generation_allowed"] is False
    assert payload["n3_status"] == "NOT_STARTED_NO_MUTATION_READY_CAPABILITY"
    assert payload["n4_status"] == "NOT_STARTED_NO_CONTRACT_READY_CANDIDATE"
    assert payload["candidate_generation_run"] is False
    assert payload["candidate_count"] == 0
    assert payload["replay_run"] is False
    assert payload["next_route"] == graph.NEXT_ROUTE_ZERO


def test_graph_config_drift_blocks_instead_of_reordering_nodes() -> None:
    sources = _sources()
    sources["graph_config"]["nodes"] = list(
        reversed(sources["graph_config"]["nodes"])
    )

    payload = _build(sources)

    assert payload["status"] == graph.BLOCKED_STATUS
    assert "capability_identity_or_order_mismatch" in payload[
        "strict_validation_errors"
    ]


def test_mutation_gate_requires_all_dimensions_and_ready_dependencies() -> None:
    sources = _sources()
    node = sources["graph_config"]["nodes"][1]
    node["governance"]["mutable_dimensions"] = ["fixture_dimension"]
    node["replay"]["runner_binding"] = "fixture_runner"
    dependency_ids = {
        "hard_veto_aggregate",
        "transition_regime_current_requested_applied",
        "exposure_scalar_native",
    }
    for dependency in sources["graph_config"]["nodes"]:
        if dependency["capability_id"] in dependency_ids:
            dependency["readiness"]["status"] = "READY"

    payload = _build(sources)
    evaluated = next(
        item
        for item in payload["nodes"]
        if item["capability_id"] == "signal_growth_allowed"
    )

    assert evaluated["mutation_ready"] is True
    assert evaluated["mutation_blocker_codes"] == []
    assert payload["status"] == graph.BLOCKED_STATUS
    assert "unexpected_mutation_ready_capability_count" in payload[
        "strict_validation_errors"
    ]


def test_runner_writes_reload_verified_graph_and_markdown(tmp_path: Path) -> None:
    paths = _write_source_fixtures(tmp_path)

    payload = impl.run_growth_tilt_baseline_capability_graph(
        closure_path=paths["closure"],
        adapters_path=paths["adapters"],
        signal_inventory_path=paths["signal_inventory"],
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == graph.READY_ZERO_STATUS
    assert payload["artifact_reload_verified"] is True
    assert (
        tmp_path / "outputs" / "growth_tilt_baseline_capability_graph.json"
    ).exists()
    assert (tmp_path / "docs" / "growth_tilt_baseline_capability_graph.md").exists()


def test_runner_strict_mode_rejects_missing_graph_config(tmp_path: Path) -> None:
    paths = _write_source_fixtures(tmp_path)

    with pytest.raises(ValueError, match="graph_config missing"):
        impl.run_growth_tilt_baseline_capability_graph(
            graph_config_path=tmp_path / "missing.yaml",
            closure_path=paths["closure"],
            adapters_path=paths["adapters"],
            signal_inventory_path=paths["signal_inventory"],
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_reports_zero_mutation_ready_and_no_candidate_generation(
    tmp_path: Path,
) -> None:
    paths = _write_source_fixtures(tmp_path)
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-baseline-capability-graph",
            "--source-closure",
            str(paths["closure"]),
            "--source-adapters",
            str(paths["adapters"]),
            "--signal-inventory",
            str(paths["signal_inventory"]),
            "--output-root",
            str(tmp_path / "outputs"),
            "--docs-root",
            str(tmp_path / "docs"),
            "--as-of",
            "2026-07-10",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert graph.READY_ZERO_STATUS in result.output
    assert "mutation_ready_capability_count=0" in result.output
    assert "candidate_generation_run=false" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_flow_and_requirement_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert entries[graph.REPORT_TYPE]["production_effect"] == "none"
    assert entries[graph.REPORT_TYPE]["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in graph.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in graph.REQUIRED_FLOW_REFERENCES
    )
    requirement = REQUIREMENT_PATH.read_text(encoding="utf-8")
    assert "mutation_ready_capability_count > 0" in requirement
    assert "N2 不得新增 transition" in requirement


def _node(capability_id: str) -> dict[str, Any]:
    return next(
        item for item in _build()["nodes"] if item["capability_id"] == capability_id
    )


def _build(sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return graph.build_growth_tilt_baseline_capability_graph(
        sources or _sources(),
        report_registry={"reports": [{"report_id": graph.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(graph.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(graph.REQUIRED_FLOW_REFERENCES),
        requirement_text=REQUIREMENT_PATH.read_text(encoding="utf-8"),
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    return {
        "graph_config": copy.deepcopy(safe_load_yaml_path(GRAPH_CONFIG_PATH)),
        "closure": _closure_fixture(),
        "adapters": _adapters_fixture(),
        "signal_inventory": {
            "schema_version": graph.EXPECTED_SIGNAL_INVENTORY_SCHEMA
        },
        "base_policy": {"schema_version": graph.EXPECTED_BASE_POLICY_SCHEMA},
        "risk_veto_policy": {"schema_version": graph.EXPECTED_RISK_VETO_SCHEMA},
        "metric_contract": {
            "schema_version": graph.EXPECTED_METRIC_SCHEMA,
            "status": "PENDING_OWNER_PREREGISTRATION",
        },
        "screening_policy": {
            "schema_version": graph.EXPECTED_SCREENING_SCHEMA,
            "policy_status": "PENDING_OWNER_PREREGISTRATION",
        },
        "compiler_code_text": "QQQ_equivalent_exposure_max = 3.0\nturnover_max",
        "executor_code_text": "class GrowthTiltCandidateOverlayExecutor:",
    }


def _closure_fixture() -> dict[str, Any]:
    return {
        "schema_version": graph.EXPECTED_CLOSURE_SCHEMA,
        "status": graph.EXPECTED_CLOSURE_STATUS,
    }


def _adapters_fixture() -> dict[str, Any]:
    return {
        "schema_version": graph.EXPECTED_ADAPTER_SCHEMA,
        "adapter_contract_ready_count": 0,
        "adapter_contract_blocked_count": 4,
    }


def _write_source_fixtures(tmp_path: Path) -> dict[str, Path]:
    sources = {
        "closure": _closure_fixture(),
        "adapters": _adapters_fixture(),
        "signal_inventory": {
            "schema_version": graph.EXPECTED_SIGNAL_INVENTORY_SCHEMA
        },
    }
    paths: dict[str, Path] = {}
    for source_id, value in sources.items():
        path = tmp_path / f"{source_id}.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        paths[source_id] = path
    return paths
