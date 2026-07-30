from __future__ import annotations

from pathlib import Path

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.contracts import (
    AssertionKind,
    CanonicalStatus,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "f" * 40


def test_canonical_registry_builds_closed_deterministic_snapshot() -> None:
    first = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )
    second = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )
    assert first.snapshot.canonical_json_bytes() == (second.snapshot.canonical_json_bytes())
    assert first.snapshot.snapshot_id == second.snapshot.snapshot_id
    assert len(first.snapshot.sources) == 8
    assert len(first.snapshot.nodes) == 21
    assert len(first.snapshot.edges) == 22
    assert len(first.snapshot.results) == 8
    assert len(first.snapshot.attributions) == 12


def test_snapshot_keeps_research_status_separate_from_validation_pass() -> None:
    bundle = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )
    results = {item.result_id: item for item in bundle.snapshot.results}
    assert results["result-o1-v1-coverage"].raw_status is CanonicalStatus.BLOCKED
    assert results["result-o1-v2-policy"].raw_status is CanonicalStatus.NOT_DUE
    assert results["result-restart-r2"].display_status is CanonicalStatus.LIMITED
    assert results["result-qld-evaluation"].display_status is CanonicalStatus.LIMITED
    assert results["result-label-foundation"].display_status is CanonicalStatus.LIMITED
    assert results["result-atlas-contract"].raw_status is CanonicalStatus.PASS
    assert "不是策略 PASS" in results["result-atlas-contract"].reader_summary
    assert not any(item.investment_facing for item in results.values())


def test_snapshot_uses_only_explicit_assertion_kinds() -> None:
    bundle = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )
    kinds = {
        *(item.assertion_kind for item in bundle.snapshot.nodes),
        *(item.assertion_kind for item in bundle.snapshot.results),
        *(item.assertion_kind for item in bundle.snapshot.attributions),
    }
    assert kinds <= set(AssertionKind)
    assert AssertionKind.DATA_FACT in kinds
    assert AssertionKind.RULE_JUDGMENT in kinds
    assert AssertionKind.RESEARCHER_INTERPRETATION in kinds
    assert AssertionKind.OWNER_DECISION in kinds


def test_snapshot_exposes_exact_primary_and_requested_windows() -> None:
    bundle = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )
    assert bundle.primary_research_start == "2021-02-22"
    source_map = {item.source_ref_id: item for item in bundle.snapshot.sources}
    assert source_map["o1-v1-policy"].requested_end.isoformat() == "2026-07-27"
    assert source_map["o1-v1-policy"].evaluated_end.isoformat() == "2026-07-24"
    assert source_map["o1-v2-policy"].requested_end.isoformat() == "2027-01-29"
    assert source_map["o1-v2-policy"].evaluated_end.isoformat() == "2027-01-22"
    assert source_map["restart-r0-r2-closeout"].legacy_history_partial
    assert not source_map["qld-universe-evaluation"].data_quality_ready
    assert not source_map["label-foundation"].data_quality_ready
