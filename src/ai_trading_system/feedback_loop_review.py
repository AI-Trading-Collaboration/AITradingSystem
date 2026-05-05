from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_outcomes import load_decision_snapshots
from ai_trading_system.market_evidence import load_market_evidence_store
from ai_trading_system.prediction_ledger import DEFAULT_PREDICTION_OUTCOMES_PATH
from ai_trading_system.rule_experiments import DEFAULT_RULE_EXPERIMENT_LEDGER_PATH

DEFAULT_FEEDBACK_LOOP_REVIEW_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass(frozen=True)
class FeedbackLoopReviewReport:
    as_of: date
    since: date
    generated_at: datetime
    market_regime_id: str
    evidence: dict[str, Any]
    decision_snapshots: dict[str, Any]
    decision_outcomes: dict[str, Any]
    prediction_outcomes: dict[str, Any]
    causal_chains: dict[str, Any]
    learning_queue: dict[str, Any]
    rule_candidates: dict[str, Any]
    task_register: dict[str, Any]

    @property
    def warning_count(self) -> int:
        sections = (
            self.evidence,
            self.decision_snapshots,
            self.decision_outcomes,
            self.prediction_outcomes,
            self.causal_chains,
            self.learning_queue,
            self.rule_candidates,
            self.task_register,
        )
        return sum(len(section.get("warnings", [])) for section in sections)

    @property
    def status(self) -> str:
        if self.warning_count:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"


def default_feedback_loop_review_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"feedback_loop_review_{as_of.isoformat()}.md"


def build_feedback_loop_review_report(
    *,
    as_of: date,
    evidence_path: Path,
    decision_snapshot_path: Path,
    outcomes_path: Path,
    causal_chain_path: Path,
    learning_queue_path: Path,
    task_register_path: Path,
    prediction_outcomes_path: Path = DEFAULT_PREDICTION_OUTCOMES_PATH,
    rule_experiment_path: Path = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    since: date | None = None,
    market_regime_id: str = "ai_after_chatgpt",
) -> FeedbackLoopReviewReport:
    since = since or as_of - timedelta(days=7)
    return FeedbackLoopReviewReport(
        as_of=as_of,
        since=since,
        generated_at=datetime.now(tz=UTC),
        market_regime_id=market_regime_id,
        evidence=_evidence_section(evidence_path, since, as_of),
        decision_snapshots=_snapshot_section(decision_snapshot_path, since, as_of),
        decision_outcomes=_outcome_section(outcomes_path),
        prediction_outcomes=_prediction_outcome_section(prediction_outcomes_path),
        causal_chains=_ledger_section(causal_chain_path, "chains"),
        learning_queue=_ledger_section(learning_queue_path, "items"),
        rule_candidates=_rule_candidate_section(rule_experiment_path),
        task_register=_task_register_section(task_register_path),
    )


def render_feedback_loop_review_report(report: FeedbackLoopReviewReport) -> str:
    lines = [
        "# 反馈闭环周期复核报告",
        "",
        f"- 状态：{report.status}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 复核日期：{report.as_of.isoformat()}",
        f"- 复核窗口：{report.since.isoformat()} 至 {report.as_of.isoformat()}",
        f"- 市场阶段：{report.market_regime_id}",
        f"- 警告数：{report.warning_count}",
        "- 治理边界：本报告只汇总复核状态；任何规则、仓位、thesis 或评分变更"
        "必须另走实验和治理流程。",
        "",
        "## 新证据",
        "",
        _count_line("证据总数", report.evidence, "total_count"),
        _count_line("窗口内新证据", report.evidence, "new_count"),
        _count_line("待人工复核", report.evidence, "pending_review_count"),
        _warning_lines(report.evidence),
        "",
        "## 决策快照",
        "",
        _count_line("快照总数", report.decision_snapshots, "total_count"),
        _count_line("窗口内快照", report.decision_snapshots, "new_count"),
        _warning_lines(report.decision_snapshots),
        "",
        "## Outcome 与校准",
        "",
        _count_line("Outcome 行数", report.decision_outcomes, "total_count"),
        _count_line("可用 outcome", report.decision_outcomes, "available_count"),
        _count_line("等待完成", report.decision_outcomes, "pending_count"),
        _count_line("缺失数据", report.decision_outcomes, "missing_count"),
        _warning_lines(report.decision_outcomes),
        "",
        "## Prediction / Shadow Outcome",
        "",
        _count_line("Prediction outcome 行数", report.prediction_outcomes, "total_count"),
        _count_line("可用 prediction outcome", report.prediction_outcomes, "available_count"),
        _count_line("等待 shadow 窗口", report.prediction_outcomes, "pending_count"),
        _count_line("缺失 prediction 数据", report.prediction_outcomes, "missing_count"),
        _count_line("Challenger 分组数", report.prediction_outcomes, "challenger_count"),
        _warning_lines(report.prediction_outcomes),
        "",
        "## 因果链",
        "",
        _count_line("因果链数量", report.causal_chains, "total_count"),
        _warning_lines(report.causal_chains),
        "",
        "## 学习队列",
        "",
        _count_line("复核项数量", report.learning_queue, "total_count"),
        _category_line(report.learning_queue),
        _warning_lines(report.learning_queue),
        "",
        "## 规则候选",
        "",
        f"- 状态：{report.rule_candidates['status']}",
        f"- 说明：{report.rule_candidates['summary']}",
        _count_line("候选规则数", report.rule_candidates, "total_count"),
        _count_line("未运行 replay", report.rule_candidates, "pending_replay_count"),
        _count_line("待前向 shadow", report.rule_candidates, "pending_shadow_count"),
        _warning_lines(report.rule_candidates),
        "",
        "## Task Register",
        "",
        _task_status_table(report.task_register),
        "",
        "## 输出解释",
        "",
        "- 可执行：无自动交易动作；本报告不直接产生调仓建议。",
        "- 需要复核：学习队列中的 `rule_candidate_required=true` 项。",
        "- 研究用途：样本不足、reconciliation 未覆盖或规则候选尚未验证的结论。",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_feedback_loop_review_report(
    report: FeedbackLoopReviewReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_feedback_loop_review_report(report), encoding="utf-8")
    return output_path


def _evidence_section(path: Path, since: date, as_of: date) -> dict[str, Any]:
    warnings: list[str] = []
    if not path.exists():
        return {
            "total_count": 0,
            "new_count": 0,
            "pending_review_count": 0,
            "warnings": [f"market evidence 路径不存在：{path}"],
        }
    store = load_market_evidence_store(path)
    warnings.extend(f"{error.path}: {error.message}" for error in store.load_errors)
    loaded = tuple(item.evidence for item in store.loaded)
    new_items = [
        item for item in loaded if since <= item.captured_at <= as_of
    ]
    pending = [
        item for item in loaded if item.manual_review_status == "pending_review"
    ]
    return {
        "total_count": len(loaded),
        "new_count": len(new_items),
        "pending_review_count": len(pending),
        "warnings": warnings,
    }


def _snapshot_section(path: Path, since: date, as_of: date) -> dict[str, Any]:
    if not path.exists():
        return {
            "total_count": 0,
            "new_count": 0,
            "warnings": [f"decision snapshot 路径不存在：{path}"],
        }
    snapshots = load_decision_snapshots(path)
    new_snapshots = [
        snapshot
        for snapshot in snapshots
        if since <= date.fromisoformat(snapshot["signal_date"]) <= as_of
    ]
    return {
        "total_count": len(snapshots),
        "new_count": len(new_snapshots),
        "warnings": [],
    }


def _outcome_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "total_count": 0,
            "available_count": 0,
            "pending_count": 0,
            "missing_count": 0,
            "warnings": [f"decision outcomes 不存在：{path}"],
        }
    outcomes = pd.read_csv(path)
    status = outcomes["outcome_status"] if "outcome_status" in outcomes else pd.Series(dtype=str)
    return {
        "total_count": len(outcomes),
        "available_count": int((status == "AVAILABLE").sum()),
        "pending_count": int((status == "PENDING").sum()),
        "missing_count": int((status == "MISSING_DATA").sum()),
        "warnings": [],
    }


def _prediction_outcome_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "total_count": 0,
            "available_count": 0,
            "pending_count": 0,
            "missing_count": 0,
            "challenger_count": 0,
            "warnings": [f"prediction outcomes 不存在：{path}"],
        }
    outcomes = pd.read_csv(path)
    status = outcomes["outcome_status"] if "outcome_status" in outcomes else pd.Series(dtype=str)
    candidate = outcomes["candidate_id"] if "candidate_id" in outcomes else pd.Series(dtype=str)
    production_effect = (
        outcomes["production_effect"] if "production_effect" in outcomes else pd.Series(dtype=str)
    )
    challenger_count = len(
        set(candidate.loc[(candidate != "production") | (production_effect == "none")])
    )
    warnings = []
    available_count = int((status == "AVAILABLE").sum())
    if available_count < 30:
        warnings.append("prediction/shadow 可用样本不足 30，不能作为 production 晋级证据。")
    return {
        "total_count": len(outcomes),
        "available_count": available_count,
        "pending_count": int((status == "PENDING").sum()),
        "missing_count": int((status == "MISSING_DATA").sum()),
        "challenger_count": challenger_count,
        "warnings": warnings,
    }


def _ledger_section(path: Path, key: str) -> dict[str, Any]:
    if not path.exists():
        return {
            "total_count": 0,
            "category_counts": {},
            "warnings": [f"ledger 不存在：{path}"],
        }
    ledger = json.loads(path.read_text(encoding="utf-8"))
    items = ledger.get(key, [])
    category_counts: dict[str, int] = {}
    for item in items:
        category = item.get("attribution_category")
        if category:
            category_counts[category] = category_counts.get(category, 0) + 1
    return {
        "total_count": len(items),
        "category_counts": category_counts,
        "warnings": [],
    }


def _rule_candidate_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "NOT_CONNECTED",
            "summary": "EXPERIMENT-001 / GOV-001 尚未实现，学习队列只能标记候选规则需求。",
            "total_count": 0,
            "pending_replay_count": 0,
            "pending_shadow_count": 0,
            "warnings": ["rule candidate 流程尚未接入。"],
        }
    ledger = json.loads(path.read_text(encoding="utf-8"))
    candidates = ledger.get("candidates", [])
    pending_replay = sum(
        1
        for candidate in candidates
        if (candidate.get("replay_plan") or {}).get("status") == "NOT_RUN"
    )
    pending_shadow = sum(
        1
        for candidate in candidates
        if (candidate.get("forward_shadow_plan") or {}).get("status") == "PENDING"
    )
    return {
        "status": "CONNECTED_PENDING_VALIDATION",
        "summary": (
            "EXPERIMENT-001 rule experiment ledger 已接入；候选规则仍需历史 replay、"
            "前向 shadow 和 GOV-001 rule card 批准，不能影响 production。"
        ),
        "total_count": len(candidates),
        "pending_replay_count": pending_replay,
        "pending_shadow_count": pending_shadow,
        "warnings": (
            ["存在候选规则尚未完成 replay/shadow 验证。"]
            if pending_replay or pending_shadow
            else []
        ),
    }


def _task_register_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "status_counts": {},
            "blocked_tasks": [],
            "warnings": [f"task register 不存在：{path}"],
        }
    status_counts: dict[str, int] = {}
    blocked_tasks: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---") or line.startswith("|ID|"):
            continue
        fields = [field.strip() for field in line.strip("|").split("|")]
        if len(fields) < 4:
            continue
        task_id = fields[0]
        status = fields[3]
        status_counts[status] = status_counts.get(status, 0) + 1
        if status.startswith("BLOCKED"):
            blocked_tasks.append(task_id)
    return {
        "status_counts": dict(sorted(status_counts.items())),
        "blocked_tasks": blocked_tasks,
        "warnings": [],
    }


def _count_line(label: str, section: dict[str, Any], key: str) -> str:
    return f"- {label}：{section.get(key, 0)}"


def _warning_lines(section: dict[str, Any]) -> str:
    warnings = section.get("warnings", [])
    if not warnings:
        return "- 警告：无"
    return "- 警告：" + "；".join(str(item) for item in warnings)


def _category_line(section: dict[str, Any]) -> str:
    counts = section.get("category_counts", {})
    if not counts:
        return "- 分类：无"
    text = "，".join(f"{key}={value}" for key, value in sorted(counts.items()))
    return f"- 分类：{text}"


def _task_status_table(section: dict[str, Any]) -> str:
    lines = [
        "| 状态 | 数量 |",
        "|---|---:|",
    ]
    for status, count in section.get("status_counts", {}).items():
        lines.append(f"| {status} | {count} |")
    blocked = section.get("blocked_tasks", [])
    lines.extend(["", f"- Blocked tasks：{', '.join(blocked) if blocked else '无'}"])
    warnings = _warning_lines(section)
    lines.append(warnings)
    return "\n".join(lines)
