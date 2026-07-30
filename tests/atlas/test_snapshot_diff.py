from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import (
    AtlasSnapshotDiffError,
    build_snapshot_diff,
    load_snapshot_diff_bundle,
)
from ai_trading_system.contracts import (
    AssertionKind,
    CanonicalStatus,
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffSignificance,
    ResearchNodeKind,
    ResearchPathEdge,
    ResearchPathNode,
    StrategyResearchExplorerSnapshot,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "f" * 40


def _base_snapshot() -> StrategyResearchExplorerSnapshot:
    return build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    ).snapshot


def _pair() -> tuple[
    StrategyResearchExplorerSnapshot,
    StrategyResearchExplorerSnapshot,
]:
    before = _base_snapshot()
    sources = (
        replace(
            before.sources[0],
            exact_commit="e" * 40,
            as_of=before.sources[0].as_of + timedelta(days=1),
        ),
        *before.sources[1:],
    )
    nodes = (
        replace(
            before.nodes[0],
            summary=before.nodes[0].summary + " 已补充跨快照比较入口。",
        ),
        *before.nodes[1:],
    )
    edges = (
        *before.edges[:-1],
        ResearchPathEdge(
            edge_id="edge-diff-audit-link",
            edge_kind=before.edges[-1].edge_kind,
            from_node_id=before.edges[-1].from_node_id,
            to_node_id=before.edges[-1].to_node_id,
            label="跨快照复核",
        ),
    )
    results = (
        replace(
            before.results[0],
            reader_summary=before.results[0].reader_summary + " 新增差异审计。",
        ),
        *before.results[1:],
    )
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " V1.1",
        generated_at=before.generated_at + timedelta(days=1),
        sources=sources,
        nodes=nodes,
        edges=edges,
        results=results,
        attributions=before.attributions,
    )
    return before, after


def test_builder_is_deterministic_and_classifies_changes() -> None:
    before, after = _pair()
    first = build_snapshot_diff(before, after)
    second = build_snapshot_diff(before, after)
    assert first.canonical_json_bytes() == second.canonical_json_bytes()
    assert first.diff_id == second.diff_id

    changes = {(item.entity_kind, item.entity_id): item for item in first.changes}
    source_change = changes[(ExplorerDiffEntityKind.SOURCE, "atlas-registry")]
    assert source_change.change_kind is ExplorerDiffChangeKind.CHANGED
    assert source_change.significance is ExplorerDiffSignificance.LINEAGE_ONLY
    assert source_change.changed_fields == ("as_of", "exact_commit")

    node_change = changes[(ExplorerDiffEntityKind.NODE, before.nodes[0].node_id)]
    assert node_change.significance is ExplorerDiffSignificance.SEMANTIC
    assert node_change.changed_fields == ("summary",)

    result_change = changes[(ExplorerDiffEntityKind.RESULT, before.results[0].result_id)]
    assert result_change.significance is ExplorerDiffSignificance.SEMANTIC
    assert result_change.changed_fields == ("reader_summary",)

    edge_changes = [
        item for item in first.changes if item.entity_kind is ExplorerDiffEntityKind.EDGE
    ]
    assert {item.change_kind for item in edge_changes} == {
        ExplorerDiffChangeKind.ADDED,
        ExplorerDiffChangeKind.REMOVED,
    }
    assert all(item.significance is ExplorerDiffSignificance.STRUCTURAL for item in edge_changes)


def test_builder_uses_stable_ids_and_does_not_infer_rename() -> None:
    base = _base_snapshot()
    source_ids = base.nodes[0].source_ref_ids
    old_node = ResearchPathNode(
        node_id="rename-old-id",
        node_kind=ResearchNodeKind.EVIDENCE,
        title="相同展示标题",
        summary="相同展示内容",
        assertion_kind=AssertionKind.DATA_FACT,
        source_ref_ids=source_ids,
        raw_status=CanonicalStatus.LIMITED,
    )
    new_node = replace(old_node, node_id="rename-new-id")
    before = StrategyResearchExplorerSnapshot.build(
        title=base.title,
        generated_at=base.generated_at,
        sources=base.sources,
        nodes=(*base.nodes, old_node),
        edges=base.edges,
        results=base.results,
        attributions=base.attributions,
    )
    after = StrategyResearchExplorerSnapshot.build(
        title=base.title,
        generated_at=base.generated_at + timedelta(days=1),
        sources=base.sources,
        nodes=(*base.nodes, new_node),
        edges=base.edges,
        results=base.results,
        attributions=base.attributions,
    )
    changes = build_snapshot_diff(before, after).changes
    rename_rows = [item for item in changes if item.entity_id.startswith("rename-")]
    assert [(item.entity_id, item.change_kind) for item in rename_rows] == [
        ("rename-new-id", ExplorerDiffChangeKind.ADDED),
        ("rename-old-id", ExplorerDiffChangeKind.REMOVED),
    ]


def test_builder_rejects_same_snapshot() -> None:
    snapshot = _base_snapshot()
    with pytest.raises(
        AtlasSnapshotDiffError,
        match="ATLAS_DIFF_SAME_SNAPSHOT_FORBIDDEN",
    ):
        build_snapshot_diff(snapshot, snapshot)


def test_file_loader_binds_portable_input_receipt(tmp_path: Path) -> None:
    before, after = _pair()
    before_path = tmp_path / "before" / "snapshot.json"
    after_path = tmp_path / "after" / "snapshot.json"
    before_path.parent.mkdir()
    after_path.parent.mkdir()
    before_path.write_bytes(before.canonical_json_bytes())
    after_path.write_bytes(after.canonical_json_bytes())

    bundle = load_snapshot_diff_bundle(
        before_path=before_path,
        after_path=after_path,
        recorded_at=after.generated_at,
        path_root=tmp_path,
    )
    assert [item.source_path for item in bundle.input_receipt.inputs] == [
        "before/snapshot.json",
        "after/snapshot.json",
    ]
    assert bundle.input_receipt.inputs[0].snapshot_id == before.snapshot_id
    assert bundle.input_receipt.inputs[1].snapshot_id == after.snapshot_id
    assert bundle.input_receipt.to_dict()["identity_excludes"] == ["recorded_at"]
    assert not Path(bundle.input_receipt.inputs[0].source_path).is_absolute()


def test_file_loader_rejects_noncanonical_snapshot_bytes(tmp_path: Path) -> None:
    before, after = _pair()
    before_path = tmp_path / "before.json"
    after_path = tmp_path / "after.json"
    before_path.write_text(
        before.canonical_json_bytes().decode("utf-8").replace(",", ", "),
        encoding="utf-8",
    )
    after_path.write_bytes(after.canonical_json_bytes())
    with pytest.raises(
        AtlasSnapshotDiffError,
        match="ATLAS_DIFF_INPUT_NONCANONICAL_BYTES",
    ):
        load_snapshot_diff_bundle(
            before_path=before_path,
            after_path=after_path,
            recorded_at=after.generated_at,
            path_root=tmp_path,
        )
