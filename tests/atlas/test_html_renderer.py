from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ai_trading_system.atlas.html_renderer import (
    render_atlas_html,
    write_atlas_artifacts,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "f" * 40


def _bundle():
    return build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )


def test_renderer_exposes_plain_language_research_sections() -> None:
    html = render_atlas_html(_bundle())
    for expected in (
        "研究主线",
        "研究全景",
        "实际结果与归因",
        "证据来源与限制",
        "金融研究术语表",
        "数据事实",
        "规则判断",
        "Owner 决策",
        "不是策略 PASS",
        "主线 A：历史研究重启与证据闭合",
        "主线 B：QLD 工具角色评估",
        "主线 C：Decision target 到 O1 capability",
        "主线 D：O1 当前结论与未来 re-entry",
        "V1.1 不声称覆盖全部历史研究",
    ):
        assert expected in html
    assert "<script" not in html
    assert "<form" not in html
    assert 'lang="zh-CN"' in html
    assert 'class="skip-link"' in html
    assert 'aria-label="页内导航"' in html


def test_renderer_escapes_registry_narrative() -> None:
    bundle = replace(_bundle(), reader_notice="<script>alert('x')</script>")
    html = render_atlas_html(bundle)
    assert "<script>alert" not in html
    assert "&lt;script&gt;alert" in html


def test_artifact_write_is_byte_deterministic(tmp_path: Path) -> None:
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = write_atlas_artifacts(_bundle(), first_dir)
    second = write_atlas_artifacts(_bundle(), second_dir)
    assert [item.sha256 for item in first] == [item.sha256 for item in second]
    assert [item.size_bytes for item in first] == [item.size_bytes for item in second]
    for name in ("index.html", "snapshot.json", "validation.json"):
        assert (first_dir / name).read_bytes() == (second_dir / name).read_bytes()
