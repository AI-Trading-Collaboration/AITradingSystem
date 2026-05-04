from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
DEFAULT_DECISION_LEARNING_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "decision_learning_queue.json"
)


@dataclass(frozen=True)
class DecisionLearningQueueLedger:
    schema_version: int
    generated_at: datetime
    item_count: int
    source_chain_count: int
    items: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "item_count": self.item_count,
            "source_chain_count": self.source_chain_count,
            "items": list(self.items),
        }


def default_decision_learning_queue_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"decision_learning_queue_{as_of.isoformat()}.md"


def load_decision_learning_queue(input_path: Path) -> dict[str, Any]:
    return json.loads(input_path.read_text(encoding="utf-8"))


def lookup_decision_learning_item(input_path: Path, review_id: str) -> dict[str, Any]:
    ledger = load_decision_learning_queue(input_path)
    for item in ledger.get("items", []):
        if item.get("review_id") == review_id:
            return item
    raise KeyError(f"decision learning item not found: {review_id}")


def build_decision_learning_queue(
    *,
    chains: tuple[dict[str, Any], ...],
    generated_at: datetime | None = None,
    min_available_windows: int = 1,
) -> DecisionLearningQueueLedger:
    if min_available_windows <= 0:
        raise ValueError("min_available_windows must be positive")
    generated_at = generated_at or datetime.now(tz=UTC)
    items = tuple(
        _learning_item(chain, min_available_windows=min_available_windows)
        for chain in sorted(chains, key=lambda item: item.get("signal_date", ""))
    )
    return DecisionLearningQueueLedger(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        item_count=len(items),
        source_chain_count=len(chains),
        items=items,
    )


def write_decision_learning_queue(
    ledger: DecisionLearningQueueLedger,
    output_path: Path = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(ledger.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def render_decision_learning_queue_report(
    ledger: DecisionLearningQueueLedger,
    ledger_path: Path,
) -> str:
    lines = [
        "# 决策学习队列",
        "",
        "- 状态：PASS_WITH_LIMITATIONS",
        f"- 生成时间：{ledger.generated_at.isoformat()}",
        f"- 来源因果链数：{ledger.source_chain_count}",
        f"- 复核项数：{ledger.item_count}",
        f"- 机器可读 queue：`{ledger_path}`",
        "- 治理边界：学习队列只生成复核任务和候选规则需求标记，"
        "不得自动修改 production scoring、position_gate、thesis 或日报结论。",
        "- 样本不足策略：`sample_limited` 项不得生成规则候选。",
        "",
        "## 分类摘要",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
    ]
    for category, count in _category_counts(ledger.items).items():
        lines.append(f"| {category} | {count} |")

    lines.extend(
        [
            "",
            "## 复核队列",
            "",
            "| Review | 日期 | 方向 | 分类 | 规则候选 | Next step |",
            "|---|---|---|---|---|---|",
        ]
    )
    for item in ledger.items:
        lines.append(
            "| "
            f"`{item['review_id']}` | "
            f"{item['signal_date']} | "
            f"{item['outcome_direction']} | "
            f"{item['attribution_category']} | "
            f"{'需要复核' if item['rule_candidate_required'] else '不生成'} | "
            f"{_escape_markdown_table(item['next_step'])} |"
        )
    return "\n".join(lines) + "\n"


def write_decision_learning_queue_report(
    ledger: DecisionLearningQueueLedger,
    ledger_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_decision_learning_queue_report(ledger, ledger_path),
        encoding="utf-8",
    )
    return output_path


def render_decision_learning_item_lookup(item: dict[str, Any]) -> str:
    lines = [
        f"# {item['review_id']}",
        "",
        f"- Chain：`{item['chain_id']}`",
        f"- 日期：{item['signal_date']}",
        f"- 方向：{item['outcome_direction']}",
        f"- 归因分类：{item['attribution_category']}",
        f"- 规则候选：{'需要复核' if item['rule_candidate_required'] else '不生成'}",
        f"- Owner：{item['owner']}",
        f"- Next step：{item['next_step']}",
        f"- Reason：{item['reason']}",
    ]
    return "\n".join(lines) + "\n"


def _learning_item(
    chain: dict[str, Any],
    *,
    min_available_windows: int,
) -> dict[str, Any]:
    chain_id = str(chain["chain_id"])
    signal = chain["signal_time_context"]
    post = chain["post_signal_observations"]
    windows = tuple(post.get("linked_outcome_windows") or ())
    available = tuple(window for window in windows if _is_available_window(window))
    outcome_summary = _outcome_summary(available)
    classification = _classify_chain(
        signal=signal,
        available_windows=available,
        min_available_windows=min_available_windows,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "review_id": _review_id(chain),
        "chain_id": chain_id,
        "signal_date": chain["signal_date"],
        "market_regime": chain.get("market_regime") or {},
        "linked_decision_snapshot": signal.get("linked_decision_snapshot"),
        "linked_evidence_ids": list(signal.get("linked_evidence_ids") or ()),
        "triggered_gate_ids": [
            str(gate.get("gate_id")) for gate in signal.get("triggered_gates", [])
        ],
        "affected_modules": list(signal.get("affected_modules") or ()),
        "outcome_direction": classification["outcome_direction"],
        "attribution_category": classification["attribution_category"],
        "review_status": classification["review_status"],
        "reason": classification["reason"],
        "owner": "system_review",
        "next_step": classification["next_step"],
        "rule_candidate_required": classification["rule_candidate_required"],
        "rule_candidate_policy": (
            "只有非 sample_limited 且归因为 rule_issue 的失败样本才可进入候选规则复核；"
            "进入 production 必须另走 EXPERIMENT-001 和 GOV-001。"
        ),
        "outcome_summary": outcome_summary,
        "available_window_count": len(available),
        "sample_limited": classification["attribution_category"] == "sample_limited",
    }


def _classify_chain(
    *,
    signal: dict[str, Any],
    available_windows: tuple[dict[str, Any], ...],
    min_available_windows: int,
) -> dict[str, Any]:
    if len(available_windows) < min_available_windows:
        return _classification(
            outcome_direction="pending",
            attribution_category="sample_limited",
            review_status="sample_limited",
            reason="可用 outcome 窗口不足，不能形成规则结论。",
            next_step="等待更多 outcome 窗口完成后再复核。",
            rule_candidate_required=False,
        )

    returns = [
        float(window["ai_proxy_return"])
        for window in available_windows
        if window.get("ai_proxy_return") is not None
    ]
    hit_values = [_to_bool(window.get("hit")) for window in available_windows]
    hit_values = [value for value in hit_values if value is not None]
    outcome_direction = (
        "success"
        if hit_values and all(hit_values)
        else "success"
        if returns and mean(returns) > 0
        else "failure"
    )
    data_quality_status = (signal.get("quality") or {}).get("market_data_status")
    affected_modules = signal.get("affected_modules") or []
    triggered_gates = signal.get("triggered_gates") or []

    if outcome_direction == "success":
        return _classification(
            outcome_direction=outcome_direction,
            attribution_category="rule_issue",
            review_status="open",
            reason="成功样本进入规则有效性复核，不自动强化生产规则。",
            next_step="记录成功链路，等待更多样本后再判断是否形成候选规则。",
            rule_candidate_required=False,
        )
    if data_quality_status not in {None, "PASS"}:
        return _classification(
            outcome_direction=outcome_direction,
            attribution_category="data_issue",
            review_status="open",
            reason="失败样本伴随数据质量非 PASS，优先排查数据来源和质量门覆盖。",
            next_step="复核数据质量报告、来源冲突和缓存新鲜度。",
            rule_candidate_required=False,
        )
    if any(_is_low_confidence_module(module) for module in affected_modules):
        return _classification(
            outcome_direction=outcome_direction,
            attribution_category="data_issue",
            review_status="open",
            reason="失败样本依赖低置信或手工/占位模块，优先归因为数据覆盖问题。",
            next_step="补齐对应模块的可信数据源，再重新观察同类样本。",
            rule_candidate_required=False,
        )
    if triggered_gates:
        return _classification(
            outcome_direction=outcome_direction,
            attribution_category="rule_issue",
            review_status="open",
            reason="失败样本已触发 position gate，需要复核 gate 阈值或保护力度。",
            next_step="评估是否生成 rule_candidate，并通过 replay/shadow 验证。",
            rule_candidate_required=True,
        )
    if not signal.get("linked_evidence_ids"):
        return _classification(
            outcome_direction=outcome_direction,
            attribution_category="data_issue",
            review_status="open",
            reason="失败样本缺少 evidence 引用，审计链路不足。",
            next_step="补齐 evidence bundle 或将该样本降级为不可用于规则学习。",
            rule_candidate_required=False,
        )
    return _classification(
        outcome_direction=outcome_direction,
        attribution_category="exogenous_unforecastable",
        review_status="open",
        reason="失败样本暂未发现数据或规则直接缺陷，先归为外生冲击复核。",
        next_step="人工复核同期新闻、事件和 proxy/benchmark 选择。",
        rule_candidate_required=False,
    )


def _classification(
    *,
    outcome_direction: str,
    attribution_category: str,
    review_status: str,
    reason: str,
    next_step: str,
    rule_candidate_required: bool,
) -> dict[str, Any]:
    return {
        "outcome_direction": outcome_direction,
        "attribution_category": attribution_category,
        "review_status": review_status,
        "reason": reason,
        "next_step": next_step,
        "rule_candidate_required": rule_candidate_required,
    }


def _review_id(chain: dict[str, Any]) -> str:
    chain_id = str(chain["chain_id"]).replace("decision_causal_chain:", "")
    safe_id = chain_id.replace(":", "_")
    return f"learning_review:{safe_id}"


def _is_available_window(window: dict[str, Any]) -> bool:
    status = window.get("outcome_status")
    return status == "AVAILABLE" or window.get("ai_proxy_return") is not None


def _outcome_summary(windows: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    returns = [
        float(window["ai_proxy_return"])
        for window in windows
        if window.get("ai_proxy_return") is not None
    ]
    if not returns:
        return {
            "available_window_count": len(windows),
            "mean_ai_proxy_return": None,
            "min_ai_proxy_return": None,
            "max_ai_proxy_return": None,
        }
    return {
        "available_window_count": len(windows),
        "mean_ai_proxy_return": mean(returns),
        "min_ai_proxy_return": min(returns),
        "max_ai_proxy_return": max(returns),
    }


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _is_low_confidence_module(module: dict[str, Any]) -> bool:
    return module.get("source_type") in {
        "placeholder",
        "insufficient_data",
        "manual_input",
        "partial_manual_input",
    }


def _category_counts(items: tuple[dict[str, Any], ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        category = str(item.get("attribution_category"))
        counts[category] = counts.get(category, 0) + 1
    return dict(sorted(counts.items()))


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
