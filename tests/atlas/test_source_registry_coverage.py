from __future__ import annotations

from collections import deque
from pathlib import Path

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.contracts import (
    CanonicalStatus,
    ExplorerSourceKind,
    ResearchNodeKind,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "f" * 40

EXPECTED_SOURCE_PATHS = {
    "config/atlas/source_registry.yaml",
    "config/research/o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml",
    "config/research/o1_relative_opportunity_capability_audit_v1.yaml",
    "docs/research/strategy_research_restart_r0_r2_closeout_2026-07-20.md",
    "docs/requirements/TRADING-2446_to_2448_Research_Restart_R0_R2.md",
    "docs/requirements/TRADING-2459_Strategy_Style_Discovery_SPY_QLD_Universe_Evaluation.md",
    "docs/requirements/TRADING-2460_Decision_Target_Capability_Audit_Label_Foundation.md",
    "docs/requirements/TRADING-2463_Decision_Target_Redesign_Preregistration.md",
}
EXPECTED_CAMPAIGN_IDS = {
    "campaign-restart-evidence-closure",
    "campaign-qld-implementation",
    "campaign-decision-target",
    "campaign-o1-current-future",
}


def _bundle():
    return build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )


def test_v1_1_covers_exact_reviewed_git_sources() -> None:
    sources = _bundle().snapshot.sources
    assert {item.source_path for item in sources} == EXPECTED_SOURCE_PATHS
    assert all(item.exact_commit == EXACT_COMMIT for item in sources)
    assert all(item.source_kind is not ExplorerSourceKind.UNVERIFIED_CONTEXT for item in sources)


def test_legacy_and_scoped_dq_boundaries_are_fail_closed() -> None:
    source_map = {item.source_ref_id: item for item in _bundle().snapshot.sources}
    for source_ref_id in ("restart-r0-r2-requirement", "restart-r0-r2-closeout"):
        source = source_map[source_ref_id]
        assert source.legacy_history_partial
        assert not source.research_context_complete
        assert not source.data_quality_ready
        assert "legacy" in source.limitation.lower() or "holdout" in source.limitation
    for source_ref_id in ("qld-universe-evaluation", "label-foundation"):
        source = source_map[source_ref_id]
        assert source.research_context_complete
        assert not source.data_quality_ready
        assert "canonical full-cache DQ" in source.limitation


def test_all_four_campaigns_are_reachable_from_program_root() -> None:
    snapshot = _bundle().snapshot
    node_map = {item.node_id: item for item in snapshot.nodes}
    campaign_ids = {
        item.node_id for item in snapshot.nodes if item.node_kind is ResearchNodeKind.CAMPAIGN
    }
    assert campaign_ids == EXPECTED_CAMPAIGN_IDS
    adjacency: dict[str, list[str]] = {}
    for edge in snapshot.edges:
        adjacency.setdefault(edge.from_node_id, []).append(edge.to_node_id)
    visited = {"program-strategy-research"}
    queue = deque(visited)
    while queue:
        node_id = queue.popleft()
        for target in adjacency.get(node_id, []):
            if target not in visited:
                visited.add(target)
                queue.append(target)
    assert EXPECTED_CAMPAIGN_IDS <= visited
    assert set(node_map) == visited


def test_reader_results_preserve_non_investment_status_boundaries() -> None:
    results = {item.result_id: item for item in _bundle().snapshot.results}
    assert not any(item.investment_facing for item in results.values())
    assert results["result-restart-r2"].display_status is CanonicalStatus.LIMITED
    assert results["result-qld-evaluation"].display_status is CanonicalStatus.LIMITED
    assert results["result-qld-owner-role"].display_status is CanonicalStatus.LIMITED
    assert results["result-label-foundation"].display_status is CanonicalStatus.LIMITED
    assert results["result-target-redesign"].display_status is CanonicalStatus.PASS
    assert "不是 capability PASS" in results["result-target-redesign"].reader_summary
    assert results["result-o1-v1-coverage"].display_status is CanonicalStatus.BLOCKED
    assert results["result-o1-v2-policy"].display_status is CanonicalStatus.NOT_DUE


def test_registry_declares_current_primary_window_only() -> None:
    bundle = _bundle()
    assert bundle.primary_research_start == "2021-02-22"
    restart = next(
        item
        for item in bundle.snapshot.results
        if item.result_id == "result-restart-r2"
    )
    assert any("2022-12-01" in limitation for limitation in restart.limitations)
