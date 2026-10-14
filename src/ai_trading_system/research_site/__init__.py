"""Read-only, deterministic research progress page (TRADING-2568)."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .collect import SCHEMA_VERSION, TASK_PREFIXES, collect
from .markdown import markdown_to_html
from .render import render

__all__ = [
    "SCHEMA_VERSION",
    "TASK_PREFIXES",
    "collect",
    "render",
    "markdown_to_html",
    "build_site",
    "main",
]


def _write_site(repo: Path, out: Path, model: Mapping[str, Any]) -> Path:
    if out.resolve().is_relative_to(repo.resolve()):
        raise ValueError("输出目录必须在仓库之外；生成器不写仓库。")
    page = render(model)
    data = json.dumps(model, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    index = out / "index.html"
    index.write_text(page, encoding="utf-8", newline="\n")
    (out / "site.json").write_text(data, encoding="utf-8", newline="\n")
    return index


def build_site(repo: Path, out: Path, *, repo_url: str | None = None) -> Path:
    """Write a static page and its JSON projection outside the source repository."""
    return _write_site(repo, out, collect(repo, repo_url=repo_url))


def main(argv: Sequence[str] | None = None) -> int:
    """Generate the page; visible record problems do not make the command fail."""
    parser = argparse.ArgumentParser(description="从仓库研究记录生成只读的研究进展页面。")
    parser.add_argument("--repo", type=Path, default=Path("."), help="仓库目录（默认 .）")
    parser.add_argument("--out", type=Path, required=True, help="仓库之外的输出目录")
    parser.add_argument("--repo-url", help="仓库网页地址（用于记录链接）")
    args = parser.parse_args(argv)
    model = collect(args.repo, repo_url=args.repo_url)
    index = _write_site(args.repo, args.out, model)
    summary = model["summary"]
    print(
        f"研究题 {len(model['questions'])} · 进行中 {summary['active']} · "
        f"累计试验 {summary['global_trials']} · 问题 {summary['problems']} · {index}"
    )
    return 0
