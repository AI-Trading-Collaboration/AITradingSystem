"""Verified private report snapshots; output contains only labels and aggregate numbers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from ai_trading_system.research_judgments.importer import _read_chain, _sha256
from ai_trading_system.research_judgments.report import ReportParams, build_report
from ai_trading_system.research_judgments.schema import Issue
from ai_trading_system.research_judgments.settlements import FORMAT as SETTLEMENT_FORMAT
from ai_trading_system.research_judgments.settlements import _verified_document


@dataclass
class ReportResult:
    status: Literal["generated", "unchanged", "rejected"]
    issues: list[Issue]
    name: str | None
    summary: dict[str, int]


def _number(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    # Metadata can contain Markdown delimiters; never let it alter the report structure.
    return str(value).replace("\r", " ").replace("\n", " ").replace("|", "\\|")


def render_markdown(doc: dict[str, Any]) -> str:
    """Render only explicitly selected metadata/numeric fields, never imported prose."""
    lines = [f"# 判断评估报告 {doc['asof']}", ""]
    counts = doc["counts"]
    lines.append(
        "结算计数："
        + "；".join(
            f"{state}={counts[state]}" for state in ("settled", "pending", "void", "data_gap")
        )
    )
    research = doc["research"]
    lines.append(
        f"研究样本：事件数={research['n_events'] if research else 0}；"
        f"簇数={research['n_clusters'] if research else 0}；"
        "同一事件的多条判断只算 1 个样本。"
    )
    lines.extend(
        [
            "",
            "阈值政策：[research_judgment_eval.yaml]"
            "(../../../../../config/research/research_judgment_eval.yaml)（report 部分）；"
            "有希望只表示值得优先研究，不是正式结论或生产授权。",
            "思路组可以重叠；AI 参与条目只作事后对账，不重放历史 AI 判断。",
        ]
    )

    def table(title: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> None:
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| " + " | ".join(label for _, label in columns) + " |",
                "| " + " | ".join("---" for _ in columns) + " |",
            ]
        )
        for row in rows:
            lines.append("| " + " | ".join(_number(row.get(key)) for key, _ in columns) + " |")

    scores = [
        ("n_judgments", "条数"),
        ("n_events", "事件数"),
        ("n_clusters", "簇数"),
        ("hit_rate", "命中率"),
        ("hit_wilson_low", "Wilson 下界"),
        ("hit_wilson_high", "Wilson 上界"),
        ("brier_scored", "记分 Brier"),
        ("brier_original", "原始 Brier"),
        ("brier_coin", "50% Brier"),
    ]
    lifts = [
        ("brier_baseline", "留一基准 Brier"),
        ("lift_coin_mean", "相对 50% 提升"),
        ("lift_mean", "基准率提升"),
        ("lift_se", "聚类标准误"),
        ("lift_lower", "提升下界"),
        ("lift_upper", "提升上界"),
        ("lift_p", "单侧 p"),
        ("lift_first_half", "前半提升"),
        ("lift_second_half", "后半提升"),
        ("mde", "MDE"),
    ]
    statuses = {"insufficient": "样本不足", "promising": "有希望", "abandon": "可放弃"}
    methods = [{**m, "status_cn": statuses[m["status"]]} for m in doc["methods"]]
    table(
        "研究思路",
        methods,
        [("method", "思路 id"), *scores, *lifts, ("status_cn", "状态"), ("reason", "原因")],
    )
    lines.extend(
        [
            "",
            f"多重比较：m={doc['bh']['m']}；BH q={_number(doc['bh']['q'])}"
            "（逐步上升，样本不足组也计入 m）。",
        ]
    )
    table(
        "更新增益",
        methods,
        [
            ("method", "思路 id"),
            ("update_gain", "更新增益"),
            ("n_updated", "更新事件数"),
            ("n_flipped", "翻向判断数"),
        ],
    )
    calibration = [
        ("low", "概率下界"),
        ("high", "概率上界"),
        ("n", "条数"),
        ("mean_p", "平均概率"),
        ("hit_rate", "命中率"),
        ("wilson_low", "Wilson 下界"),
        ("wilson_high", "Wilson 上界"),
    ]
    table("校准表（研究，低概率已翻向）", doc["calibration"], calibration)
    for method in methods:
        table(f"校准表：{_number(method['method'])}", method["calibration"], calibration)
    for title, key, field in (
        ("标的", "by_ticker", "ticker"),
        ("AI 参与", "by_ai_mode", "aiMode"),
        ("看过系统日报", "by_saw_system", "sawSystem"),
    ):
        table(title, doc[key], [(field, title), *scores, *lifts])
    for title, key in (("练习条目", "learning"), ("全部已结算", "all_settled")):
        table(
            title,
            [doc[key]] if doc[key] else [],
            [
                *scores,
                ("update_gain", "更新增益"),
                ("n_updated", "更新事件数"),
                ("n_flipped", "翻向判断数"),
            ],
        )
    return "\n".join(lines) + "\n"


def make_report(
    *, data_dir: Path, params: ReportParams, asof: date, dry_run: bool = False
) -> ReportResult:
    """Verify chains and snapshots before computing or creating anything."""

    def rejected(code: str) -> ReportResult:
        return ReportResult("rejected", [Issue(code)], None, {})

    try:
        imports, _ = _read_chain(data_dir / "imports.jsonl")
        settlements, _ = _read_chain(data_dir / "settlements.jsonl")
    except (OSError, ValueError, RecursionError):
        return rejected("chain_broken")
    if not imports:
        return rejected("no_import")
    if not settlements:
        return rejected("no_settlement")
    latest_import, latest_settlement = imports[-1], settlements[-1]
    exports_dir = data_dir / "exports"
    try:
        imported = _verified_document(exports_dir, latest_import, "export_sha256")
        rows = imported["judgments"]
        if (
            imported.get("format") != "judgments_export.v1"
            or not isinstance(rows, list)
            or not all(isinstance(row, dict) for row in rows)
        ):
            raise ValueError("invalid imported document")
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return rejected("baseline_tampered")
    settlements_dir = data_dir / "settlements"
    try:
        settled = _verified_document(settlements_dir, latest_settlement, "file_sha256")
        settled_asof = date.fromisoformat(settled["asof"])
        outcomes = settled["outcomes"]
        if settled.get("format") != SETTLEMENT_FORMAT or not isinstance(outcomes, list):
            raise ValueError("invalid settlement document")
        research_ids = {row["id"] for row in rows if row["purpose"] == "research"}
        ids: set[str] = set()
        for outcome in outcomes:
            if not isinstance(outcome, dict):
                raise ValueError("invalid settlement row")
            id_ = outcome["id"]
            state, value = outcome["state"], outcome["outcome"]
            if (
                not isinstance(id_, str)
                or id_ in ids
                or state not in ("settled", "pending", "void", "data_gap")
                or (state == "settled" and (type(value) is not int or value not in (0, 1)))
                or (state != "settled" and value is not None)
            ):
                raise ValueError("invalid settlement outcome")
            ids.add(id_)
        # Stale snapshots may legitimately contain a different set of IDs; reject them below.
        if (
            latest_settlement["export_sha256"] == latest_import["export_sha256"]
            and ids != research_ids
        ):
            raise ValueError("incomplete settlement")
        if settled["export_sha256"] != latest_settlement["export_sha256"]:
            raise ValueError("settlement metadata mismatch")
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return rejected("chain_broken")
    if latest_settlement["export_sha256"] != latest_import["export_sha256"]:
        return rejected("settlement_stale")
    if asof < settled_asof:
        return rejected("asof_backwards")
    doc = build_report(rows, outcomes, params, asof)
    doc.update(
        export_sha256=latest_import["export_sha256"],
        settlement_sha256=latest_settlement["file_sha256"],
    )
    raw = (json.dumps(doc, sort_keys=True, ensure_ascii=False, indent=1) + "\n").encode("utf-8")
    name = f"judgment_report_{asof.isoformat()}_{_sha256(raw)[:8]}.json"
    reports_dir = data_dir / "reports"
    json_path = reports_dir / name
    md_path = json_path.with_suffix(".md")
    research = doc["research"]
    summary = {
        "methods": len(doc["methods"]),
        "events": research["n_events"] if research else 0,
        "clusters": research["n_clusters"] if research else 0,
    }
    if json_path.exists() and md_path.exists():
        return ReportResult("unchanged", [], name, summary)
    if not dry_run:
        markdown = render_markdown(doc)
        reports_dir.mkdir(parents=True, exist_ok=True)
        json_path.write_bytes(raw)
        md_path.write_bytes(markdown.encode("utf-8"))
    return ReportResult("generated", [], name, summary)
