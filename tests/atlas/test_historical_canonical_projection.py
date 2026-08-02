from __future__ import annotations

import copy
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas import historical_canonical_projection as projection_module
from ai_trading_system.atlas.historical_canonical_projection import (
    HistoricalCanonicalProjectionError,
    apply_historical_canonical_projection,
    build_historical_canonical_projection,
    validate_historical_canonical_projection_sources,
)
from ai_trading_system.atlas.source_projection import load_source_registry

ROOT = Path(__file__).resolve().parents[2]


def _head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_projection_is_exact_and_deterministic() -> None:
    first = build_historical_canonical_projection(repository_root=ROOT)
    second = build_historical_canonical_projection(repository_root=ROOT)
    assert first == second
    assert first.projection_counts == {
        "sources": 0,
        "nodes": 6,
        "edges": 6,
        "results": 5,
        "attributions": 5,
    }
    assert set(first.source_ref_ids) == {
        "historical-b0-baseline",
        "historical-b1-b4-attribution",
        "historical-final-branch-decision",
        "historical-monthly-program-review",
        "historical-weight-program-snapshot",
    }
    assert {item["edge_kind"] for item in first.edges} == {"CONTAINS"}
    assert {item["direction"] for item in first.attributions} == {"NEUTRAL"}
    assert {item["display_status"] for item in first.results} == {"LIMITED"}
    assert all(item["investment_facing"] is False for item in first.results)


def test_projection_applies_only_to_the_base_graph() -> None:
    base = load_source_registry(ROOT / "config" / "atlas" / "source_registry.yaml")
    projection = build_historical_canonical_projection(repository_root=ROOT)
    projected = apply_historical_canonical_projection(base, projection)
    assert projected.registry_id == "TRADING_2494_ATLAS_CANONICAL_SOURCE_COVERAGE_V1_3"
    assert len(projected.source_payloads) == 13
    assert len(projected.node_payloads) == 27
    assert len(projected.edge_payloads) == 28
    assert len(projected.result_payloads) == 13
    assert len(projected.attribution_payloads) == 17
    historical_results = [
        item for item in projected.result_payloads if item.get("source_original_status") is not None
    ]
    assert len(historical_results) == 5
    assert sum(item["raw_status"] == "PASS" for item in historical_results) == 4
    assert sum(item["raw_status"] == "LIMITED" for item in historical_results) == 1


def test_exact_typed_sources_validate_against_git() -> None:
    validation = validate_historical_canonical_projection_sources(
        repository_root=ROOT,
        evidence_exact_commit=_head(),
    )
    assert validation.status == "PASS"
    assert len(validation.checks) == 11
    assert len(validation.source_ref_ids) == 5
    assert validation.errors == ()


def test_owner_decision_tamper_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    original = projection_module._yaml_mapping

    def tampered(payload: bytes, field: str):
        loaded = original(payload, field)
        if field != "projection_policy":
            return loaded
        changed = copy.deepcopy(dict(loaded))
        changed["owner_decision"] = "owner_decision:tampered"
        return changed

    monkeypatch.setattr(projection_module, "_yaml_mapping", tampered)
    with pytest.raises(
        HistoricalCanonicalProjectionError,
        match="PROJECTION_OWNER_DECISION_MISMATCH",
    ):
        build_historical_canonical_projection(repository_root=ROOT)


def test_existing_id_collision_fails_closed() -> None:
    base = load_source_registry(ROOT / "config" / "atlas" / "source_registry.yaml")
    projection = build_historical_canonical_projection(repository_root=ROOT)
    first_node = dict(projection.nodes[0])
    first_node["node_id"] = "program-strategy-research"
    tampered = replace(projection, nodes=(first_node, *projection.nodes[1:]))
    with pytest.raises(HistoricalCanonicalProjectionError, match="PROJECTION_ID_COLLISION"):
        apply_historical_canonical_projection(base, tampered)


def test_qqq_options_review_range_is_not_projected() -> None:
    projection = build_historical_canonical_projection(repository_root=ROOT)
    serialized = repr(
        (
            projection.nodes,
            projection.edges,
            projection.results,
            projection.attributions,
        )
    )
    assert projection.excluded_task_ids == tuple(
        f"TRADING-{task_id}" for task_id in range(2481, 2494)
    )
    assert not any(task_id in serialized for task_id in projection.excluded_task_ids)
