from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, Literal

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_learning_queue import DEFAULT_DECISION_LEARNING_QUEUE_PATH
from ai_trading_system.decision_outcomes import DEFAULT_DECISION_OUTCOMES_PATH
from ai_trading_system.decision_snapshots import DEFAULT_DECISION_SNAPSHOT_DIR
from ai_trading_system.rule_experiments import DEFAULT_RULE_EXPERIMENT_LEDGER_PATH

DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_SCORES_DAILY_PATH = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv"


@dataclass(frozen=True)
class PeriodicInvestmentReviewReport:
    period: Literal["weekly", "monthly"]
    as_of: date
    since: date
    generated_at: datetime
    market_regime_id: str
    score_rows: tuple[dict[str, Any], ...]
    snapshots: tuple[dict[str, Any], ...]
    belief_states: tuple[dict[str, Any], ...]
    outcome_summary: dict[str, Any]
    learning_summary: dict[str, Any]
    rule_experiment_summary: dict[str, Any]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings or not self.score_rows or not self.snapshots:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"


def default_periodic_investment_review_report_path(
    output_dir: Path,
    period: Literal["weekly", "monthly"],
    as_of: date,
) -> Path:
    return output_dir / f"investment_{period}_review_{as_of.isoformat()}.md"


def build_periodic_investment_review_report(
    *,
    period: Literal["weekly", "monthly"],
    as_of: date,
    since: date | None = None,
    market_regime_id: str = "ai_after_chatgpt",
    scores_path: Path = DEFAULT_SCORES_DAILY_PATH,
    decision_snapshot_path: Path = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Path = DEFAULT_DECISION_OUTCOMES_PATH,
    learning_queue_path: Path = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Path = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
) -> PeriodicInvestmentReviewReport:
    if since is None:
        since = _default_since(period, as_of)
    if since > as_of:
        raise ValueError("since must not be later than as_of")

    warnings: list[str] = []
    score_rows = _load_score_rows(scores_path, since, as_of, warnings)
    snapshots = _load_snapshots(decision_snapshot_path, since, as_of, warnings)
    belief_states = _load_belief_states(snapshots, warnings)
    return PeriodicInvestmentReviewReport(
        period=period,
        as_of=as_of,
        since=since,
        generated_at=datetime.now(tz=UTC),
        market_regime_id=market_regime_id,
        score_rows=tuple(score_rows),
        snapshots=tuple(snapshots),
        belief_states=tuple(belief_states),
        outcome_summary=_outcome_summary(outcomes_path, since, as_of, warnings),
        learning_summary=_learning_summary(learning_queue_path, since, as_of, warnings),
        rule_experiment_summary=_rule_experiment_summary(rule_experiment_path, warnings),
        warnings=tuple(warnings),
    )


def render_periodic_investment_review_report(
    report: PeriodicInvestmentReviewReport,
) -> str:
    title = "周报" if report.period == "weekly" else "月报"
    lines = [
        f"# AI 产业链{title}投资复盘",
        "",
        f"- 状态：{report.status}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 复盘区间：{report.since.isoformat()} 至 {report.as_of.isoformat()}",
        f"- 市场阶段：{report.market_regime_id}",
        "- production_effect=none；本报告只做复盘和审计下钻，不改变评分、仓位、回测或执行建议。",
        f"- 决策样本数：{len(report.score_rows)}",
        f"- Decision snapshots：{len(report.snapshots)}",
        "",
        "## 本期结论是否变化",
        "",
        *(_decision_change_lines(report)),
        "",
        "## 本期仓位是否变化",
        "",
        *(_position_change_lines(report)),
        "",
        "## 改变判断的前三个证据",
        "",
        *(_top_evidence_lines(report)),
        "",
        "## 产业链节点热度/健康度变化",
        "",
        *(_industry_chain_lines(report)),
        "",
        "## Thesis / Risk / Valuation 状态",
        "",
        *(_state_lines(report)),
        "",
        "## 系统判断是否被市场验证",
        "",
        *(_outcome_lines(report)),
    ]
    if report.period == "weekly":
        lines.extend(
            [
                "",
                "## 下周最重要的观察事件",
                "",
                "- 当前周报模板未自动接入 catalyst calendar；请结合 `aits catalysts upcoming` "
                "的未来 5/20/60 天事件清单做人工复核。",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "## 本月系统校准和规则学习",
                "",
                *(_monthly_learning_lines(report)),
            ]
        )

    lines.extend(
        [
            "",
            "## 审计下钻",
            "",
            *(_trace_lines(report)),
            "",
            "## 限制说明",
            "",
            *(_warning_lines(report)),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_periodic_investment_review_report(
    report: PeriodicInvestmentReviewReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_periodic_investment_review_report(report),
        encoding="utf-8",
    )
    return output_path


def _default_since(period: Literal["weekly", "monthly"], as_of: date) -> date:
    if period == "weekly":
        return as_of - timedelta(days=6)
    return as_of.replace(day=1)


def _load_score_rows(
    path: Path,
    since: date,
    as_of: date,
    warnings: list[str],
) -> list[dict[str, Any]]:
    if not path.exists():
        warnings.append(f"scores_daily 不存在：{path}")
        return []
    frame = pd.read_csv(path)
    required = {"as_of", "component", "score"}
    if missing := required - set(frame.columns):
        warnings.append(f"scores_daily 缺少列：{', '.join(sorted(missing))}")
        return []
    frame = frame.loc[frame["component"] == "overall"].copy()
    frame["_as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.date
    frame = frame.loc[
        frame["_as_of"].notna()
        & (frame["_as_of"] >= since)
        & (frame["_as_of"] <= as_of)
    ].sort_values("_as_of")
    return [dict(row) for row in frame.drop(columns=["_as_of"]).to_dict("records")]


def _load_snapshots(
    path: Path,
    since: date,
    as_of: date,
    warnings: list[str],
) -> list[dict[str, Any]]:
    if not path.exists():
        warnings.append(f"decision snapshot 路径不存在：{path}")
        return []
    paths = (path,) if path.is_file() else tuple(sorted(path.glob("decision_snapshot_*.json")))
    snapshots: list[dict[str, Any]] = []
    for snapshot_path in paths:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        signal_date = date.fromisoformat(str(snapshot["signal_date"]))
        if since <= signal_date <= as_of:
            snapshots.append(snapshot)
    return sorted(snapshots, key=lambda item: item["signal_date"])


def _load_belief_states(
    snapshots: list[dict[str, Any]],
    warnings: list[str],
) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for snapshot in snapshots:
        ref = snapshot.get("belief_state_ref") or {}
        path_text = ref.get("path")
        if not path_text:
            continue
        path = Path(path_text)
        if not path.exists():
            warnings.append(f"belief_state 不存在：{path}")
            continue
        states.append(json.loads(path.read_text(encoding="utf-8")))
    return states


def _outcome_summary(
    path: Path,
    since: date,
    as_of: date,
    warnings: list[str],
) -> dict[str, Any]:
    if not path.exists():
        warnings.append(f"decision outcomes 不存在：{path}")
        return {"total": 0, "available": 0, "pending": 0, "missing": 0}
    frame = pd.read_csv(path)
    if "signal_date" not in frame.columns:
        warnings.append(f"decision outcomes 缺少 signal_date 列：{path}")
        return {"total": 0, "available": 0, "pending": 0, "missing": 0}
    frame["_signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.date
    frame = frame.loc[
        frame["_signal_date"].notna()
        & (frame["_signal_date"] >= since)
        & (frame["_signal_date"] <= as_of)
    ]
    status = frame["outcome_status"] if "outcome_status" in frame else pd.Series(dtype=str)
    available = frame.loc[status == "AVAILABLE"]
    return {
        "total": len(frame),
        "available": len(available),
        "pending": int((status == "PENDING").sum()),
        "missing": int((status == "MISSING_DATA").sum()),
        "average_ai_proxy_return": _mean_column(available, "ai_proxy_return"),
        "average_ai_proxy_max_drawdown": _mean_column(
            available,
            "ai_proxy_max_drawdown",
        ),
    }


def _learning_summary(
    path: Path,
    since: date,
    as_of: date,
    warnings: list[str],
) -> dict[str, Any]:
    if not path.exists():
        warnings.append(f"decision learning queue 不存在：{path}")
        return {"total": 0, "new": 0, "rule_candidate_required": 0}
    ledger = json.loads(path.read_text(encoding="utf-8"))
    items = ledger.get("items", [])
    new_count = 0
    for item in items:
        created_at = str(item.get("created_at") or item.get("signal_date") or "")[:10]
        if created_at:
            item_date = date.fromisoformat(created_at)
            if since <= item_date <= as_of:
                new_count += 1
    return {
        "total": len(items),
        "new": new_count,
        "rule_candidate_required": sum(
            1 for item in items if item.get("rule_candidate_required")
        ),
    }


def _rule_experiment_summary(path: Path, warnings: list[str]) -> dict[str, Any]:
    if not path.exists():
        warnings.append(f"rule experiment ledger 不存在：{path}")
        return {"total": 0, "pending_replay": 0, "pending_shadow": 0}
    ledger = json.loads(path.read_text(encoding="utf-8"))
    candidates = ledger.get("candidates", [])
    return {
        "total": len(candidates),
        "pending_replay": sum(
            1
            for candidate in candidates
            if (candidate.get("replay_plan") or {}).get("status") == "NOT_RUN"
        ),
        "pending_shadow": sum(
            1
            for candidate in candidates
            if (candidate.get("forward_shadow_plan") or {}).get("status") == "PENDING"
        ),
    }


def _decision_change_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    first = _first(report.score_rows)
    last = _last(report.score_rows)
    if first is None or last is None:
        return ["- 缺少本期 overall 评分，无法判断结论变化。"]
    score_delta = _float(last, "score") - _float(first, "score")
    confidence_delta = _float(last, "confidence") - _float(first, "confidence")
    return [
        (
            "- AI 产业链评分："
            f"{_float(first, 'score'):.1f} -> {_float(last, 'score'):.1f}"
            f"（变化 {score_delta:+.1f}）。"
        ),
        (
            "- 判断置信度："
            f"{_float(first, 'confidence'):.1f} -> {_float(last, 'confidence'):.1f}"
            f"（变化 {confidence_delta:+.1f}）。"
        ),
        f"- 最新结论限制：{last.get('confidence_reasons') or '无结构化记录'}",
    ]


def _position_change_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    first = _first(report.score_rows)
    last = _last(report.score_rows)
    if first is None or last is None:
        return ["- 缺少本期仓位记录。"]
    return [
        (
            "- 风险资产内最终 AI 仓位："
            f"{_format_band(first, 'final_risk_asset_ai')} -> "
            f"{_format_band(last, 'final_risk_asset_ai')}。"
        ),
        (
            "- 总资产内 AI 仓位："
            f"{_format_band(first, 'total_asset_ai')} -> "
            f"{_format_band(last, 'total_asset_ai')}。"
        ),
        f"- 最新触发 gate：{last.get('triggered_position_gates') or '无'}",
    ]


def _top_evidence_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    evidence: list[str] = []
    first_snapshot = _first(report.snapshots)
    last_snapshot = _last(report.snapshots)
    if first_snapshot and last_snapshot:
        evidence.extend(_component_delta_evidence(first_snapshot, last_snapshot))
    latest_state = _last(report.belief_states)
    if latest_state:
        confidence = latest_state.get("confidence", {})
        for reason in confidence.get("reasons", [])[:2]:
            evidence.append(f"置信度限制：{reason}")
    latest_score = _last(report.score_rows)
    if latest_score and latest_score.get("triggered_position_gates"):
        evidence.append(f"仓位闸门：{latest_score['triggered_position_gates']}")
    if not evidence:
        return ["- 缺少足够 evidence bundle / snapshot 信息，无法排序核心证据。"]
    return [f"- {item}" for item in evidence[:3]]


def _industry_chain_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    first = _first(report.belief_states)
    last = _last(report.belief_states)
    if first is None or last is None:
        return ["- 未找到本期 belief_state，产业链节点变化暂无法自动汇总。"]
    first_state = first.get("industry_chain_state", {})
    last_state = last.get("industry_chain_state", {})
    return [
        f"- 期初：{first_state.get('summary', '无摘要')}",
        f"- 期末：{last_state.get('summary', '无摘要')}",
        "- 健康度：当前基础版主要复用节点/观察池覆盖状态；完整节点健康度仍由 "
        "`CHAIN-001` 后续补强。",
    ]


def _state_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    latest = _last(report.belief_states)
    if latest is None:
        return ["- 缺少 belief_state，无法自动汇总 thesis/risk/valuation 状态。"]
    return [
        f"- Thesis：{(latest.get('thesis_state') or {}).get('summary', '无摘要')}",
        f"- Risk：{(latest.get('risk_state') or {}).get('summary', '无摘要')}",
        f"- Valuation：{(latest.get('valuation_state') or {}).get('summary', '无摘要')}",
    ]


def _outcome_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    summary = report.outcome_summary
    return [
        (
            "- Outcome 覆盖："
            f"total={summary['total']}，available={summary['available']}，"
            f"pending={summary['pending']}，missing={summary['missing']}。"
        ),
        (
            "- 平均 AI proxy return："
            f"{_format_optional_pct(summary.get('average_ai_proxy_return'))}；"
            "平均最大回撤："
            f"{_format_optional_pct(summary.get('average_ai_proxy_max_drawdown'))}。"
        ),
        "- 样本不足或 outcome pending 时，不能把本期判断升级为已验证规则。",
    ]


def _monthly_learning_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    learning = report.learning_summary
    experiments = report.rule_experiment_summary
    return [
        (
            "- Learning queue："
            f"total={learning['total']}，本期新增={learning['new']}，"
            f"需要规则候选={learning['rule_candidate_required']}。"
        ),
        (
            "- Rule experiments："
            f"total={experiments['total']}，pending replay={experiments['pending_replay']}，"
            f"pending shadow={experiments['pending_shadow']}。"
        ),
        "- 高分信号、gate 松紧和 thesis warning 提前量仍需结合 outcome 样本数解释，"
        "不能由单月报告自动改 production 规则。",
    ]


def _trace_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    refs = []
    for snapshot in report.snapshots[-5:]:
        trace = snapshot.get("trace") or {}
        refs.append(
            "- "
            f"{snapshot.get('signal_date')}: "
            f"{trace.get('overall_claim_id', 'missing_claim')} -> "
            f"`{trace.get('trace_bundle_path', 'missing_trace')}`"
        )
    if not refs:
        return ["- 本期没有可用 trace 引用。"]
    return refs


def _warning_lines(report: PeriodicInvestmentReviewReport) -> list[str]:
    if not report.warnings:
        return ["- 未发现结构性限制。"]
    return [f"- {warning}" for warning in report.warnings]


def _component_delta_evidence(
    first_snapshot: dict[str, Any],
    last_snapshot: dict[str, Any],
) -> list[str]:
    first_components = {
        item["component"]: item for item in first_snapshot.get("scores", {}).get("components", [])
    }
    last_components = {
        item["component"]: item for item in last_snapshot.get("scores", {}).get("components", [])
    }
    deltas: list[tuple[float, str]] = []
    for component, latest in last_components.items():
        if component not in first_components:
            continue
        delta = float(latest.get("score", 0.0)) - float(
            first_components[component].get("score", 0.0)
        )
        if abs(delta) > 0:
            deltas.append((abs(delta), f"{component} 分数变化 {delta:+.1f}"))
    return [item for _, item in sorted(deltas, reverse=True)]


def _format_band(row: dict[str, Any], prefix: str) -> str:
    min_value = row.get(f"{prefix}_min")
    max_value = row.get(f"{prefix}_max")
    if pd.isna(min_value) or pd.isna(max_value):
        return "n/a"
    return f"{float(min_value):.0%}-{float(max_value):.0%}"


def _format_optional_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.2%}"


def _mean_column(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(mean(values))


def _float(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _first(items):  # type: ignore[no-untyped-def]
    return items[0] if items else None


def _last(items):  # type: ignore[no-untyped-def]
    return items[-1] if items else None
