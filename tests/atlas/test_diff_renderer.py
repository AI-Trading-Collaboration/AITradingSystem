from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from ai_trading_system.atlas.diff_renderer import (
    render_snapshot_diff_html,
    write_snapshot_diff_artifacts,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import load_snapshot_diff_bundle
from ai_trading_system.contracts import (
    ResearchPathNode,
    StrategyResearchExplorerSnapshot,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _bundle(tmp_path: Path, *, injected_summary: str = ""):
    before = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    new_node = ResearchPathNode(
        node_id="atlas-diff-reader-entry",
        node_kind=before.nodes[0].node_kind,
        title="跨快照差异入口",
        summary=injected_summary or "用通俗语言展示研究含义、结构与证据时点变化。",
        assertion_kind=before.nodes[0].assertion_kind,
        source_ref_ids=before.nodes[0].source_ref_ids,
        raw_status=before.nodes[0].raw_status,
    )
    existing_nodes = (
        (
            replace(
                before.nodes[0],
                summary=injected_summary,
            ),
            *before.nodes[1:],
        )
        if injected_summary
        else before.nodes
    )
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " V1.1",
        generated_at=before.generated_at + timedelta(days=1),
        sources=(
            replace(
                before.sources[0],
                exact_commit="e" * 40,
                as_of=before.sources[0].as_of + timedelta(days=1),
            ),
            *before.sources[1:],
        ),
        nodes=(*existing_nodes, new_node),
        edges=before.edges,
        results=(
            replace(
                before.results[0],
                reader_summary=before.results[0].reader_summary + " 研究含义已更新。",
            ),
            *before.results[1:],
        ),
        attributions=before.attributions,
    )
    before_path = tmp_path / "v1" / "snapshot.json"
    after_path = tmp_path / "v1_1" / "snapshot.json"
    before_path.parent.mkdir(parents=True)
    after_path.parent.mkdir(parents=True)
    before_path.write_bytes(before.canonical_json_bytes())
    after_path.write_bytes(after.canonical_json_bytes())
    return load_snapshot_diff_bundle(
        before_path=before_path,
        after_path=after_path,
        recorded_at=after.generated_at,
        path_root=tmp_path,
    )


def test_renderer_explains_diff_in_plain_language(tmp_path: Path) -> None:
    html = render_snapshot_diff_html(_bundle(tmp_path))
    for expected in (
        "研究变化一览",
        "只按稳定 ID 比较",
        "系统不做 rename inference",
        "研究含义变化",
        "结构新增与移除",
        "仅证据时点变化",
        "这不是策略好坏结论",
        "跨快照差异入口",
        "production_effect",
    ):
        assert expected in html
    assert "<script" not in html
    assert "<form" not in html
    assert "http://" not in html
    assert "https://" not in html
    assert 'lang="zh-CN"' in html
    assert "<details open>" in html


def test_renderer_escapes_entity_content(tmp_path: Path) -> None:
    html = render_snapshot_diff_html(
        _bundle(tmp_path, injected_summary="<script>alert('x')</script>")
    )
    assert "<script>alert" not in html
    assert "&lt;script&gt;alert" in html


def test_artifact_writer_is_byte_deterministic(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    bundle = _bundle(input_root)
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = write_snapshot_diff_artifacts(bundle, first_dir)
    second = write_snapshot_diff_artifacts(bundle, second_dir)
    assert [item.sha256 for item in first] == [item.sha256 for item in second]
    assert [item.size_bytes for item in first] == [item.size_bytes for item in second]
    for name in (
        "index.html",
        "diff.json",
        "validation.json",
        "input_receipt.json",
    ):
        assert (first_dir / name).read_bytes() == (second_dir / name).read_bytes()
