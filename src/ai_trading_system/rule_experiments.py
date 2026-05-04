from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
DEFAULT_RULE_EXPERIMENT_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "rule_experiments.json"
)
DEFAULT_REPLAY_START = date(2022, 12, 1)


@dataclass(frozen=True)
class RuleExperimentLedger:
    schema_version: int
    generated_at: datetime
    candidate_count: int
    source_learning_item_count: int
    candidates: tuple[dict[str, Any], ...]

    @property
    def pending_replay_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if (candidate.get("replay_plan") or {}).get("status") == "NOT_RUN"
        )

    @property
    def pending_shadow_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if (candidate.get("forward_shadow_plan") or {}).get("status") == "PENDING"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "candidate_count": self.candidate_count,
            "source_learning_item_count": self.source_learning_item_count,
            "pending_replay_count": self.pending_replay_count,
            "pending_shadow_count": self.pending_shadow_count,
            "production_effect": "none",
            "governance_policy": (
                "Rule experiments are candidate-only. They must not modify production "
                "scoring, position_gate, thesis, daily reports, or backtests until "
                "historical replay, forward shadow review, and GOV-001 approval pass."
            ),
            "candidates": list(self.candidates),
        }


def default_rule_experiment_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"rule_experiments_{as_of.isoformat()}.md"


def load_rule_experiment_ledger(input_path: Path) -> dict[str, Any]:
    return json.loads(input_path.read_text(encoding="utf-8"))


def lookup_rule_experiment(input_path: Path, candidate_id: str) -> dict[str, Any]:
    ledger = load_rule_experiment_ledger(input_path)
    for candidate in ledger.get("candidates", []):
        if candidate.get("candidate_id") == candidate_id:
            return candidate
    raise KeyError(f"rule experiment candidate not found: {candidate_id}")


def build_rule_experiment_ledger(
    *,
    learning_items: tuple[dict[str, Any], ...],
    generated_at: datetime | None = None,
    replay_start: date = DEFAULT_REPLAY_START,
    replay_end: date | None = None,
    shadow_start: date | None = None,
    shadow_days: int = 20,
) -> RuleExperimentLedger:
    if shadow_days <= 0:
        raise ValueError("shadow_days must be positive")
    generated_at = generated_at or datetime.now(tz=UTC)
    replay_end = replay_end or generated_at.date()
    shadow_start = shadow_start or generated_at.date()
    candidates = tuple(
        _rule_candidate(
            item=item,
            replay_start=replay_start,
            replay_end=replay_end,
            shadow_start=shadow_start,
            shadow_days=shadow_days,
        )
        for item in sorted(learning_items, key=lambda value: str(value.get("review_id", "")))
        if item.get("rule_candidate_required") is True
        and item.get("attribution_category") != "sample_limited"
    )
    return RuleExperimentLedger(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        candidate_count=len(candidates),
        source_learning_item_count=len(learning_items),
        candidates=candidates,
    )


def write_rule_experiment_ledger(
    ledger: RuleExperimentLedger,
    output_path: Path = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(ledger.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_rule_experiment_report(
    ledger: RuleExperimentLedger,
    ledger_path: Path,
) -> str:
    lines = [
        "# 候选规则实验台账",
        "",
        "- 状态：PASS_WITH_LIMITATIONS",
        f"- 生成时间：{ledger.generated_at.isoformat()}",
        f"- 来源学习复核项数：{ledger.source_learning_item_count}",
        f"- 候选规则数：{ledger.candidate_count}",
        f"- 未运行历史 replay：{ledger.pending_replay_count}",
        f"- 待前向 shadow：{ledger.pending_shadow_count}",
        f"- 机器可读 ledger：`{ledger_path}`",
        "- 治理边界：本台账只登记候选规则和验证计划；未完成 replay、shadow 和 "
        "GOV-001 批准前，`production_effect` 必须保持 `none`。",
        "",
        "## 候选规则",
        "",
    ]
    if not ledger.candidates:
        lines.append("当前没有来自学习队列的规则候选。")
    else:
        lines.extend(
            [
                "| Candidate | Review | 日期 | 触发原因 | Replay | Shadow | Production |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for candidate in ledger.candidates:
            replay = candidate["replay_plan"]
            shadow = candidate["forward_shadow_plan"]
            trigger = candidate["trigger"]
            lines.append(
                "| "
                f"`{candidate['candidate_id']}` | "
                f"`{candidate['linked_learning_review_id']}` | "
                f"{candidate['signal_date']} | "
                f"{_escape_markdown_table(trigger['reason'])} | "
                f"{replay['status']} | "
                f"{shadow['status']} | "
                f"{candidate['production_effect']} |"
            )
    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告不执行历史重放，只生成 replay 输入边界和指标要求。",
            "- forward shadow 只记录候选规则如果上线会如何改变判断，不改变正式日报。",
            "- `sample_limited` 或非 `rule_candidate_required` 复核项不会生成候选规则。",
            "- 候选规则进入 production 前必须通过 `EXPERIMENT-001` replay/shadow 和 "
            "`GOV-001` rule card 批准。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_rule_experiment_report(
    ledger: RuleExperimentLedger,
    ledger_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_rule_experiment_report(ledger, ledger_path),
        encoding="utf-8",
    )
    return output_path


def render_rule_experiment_lookup(candidate: dict[str, Any]) -> str:
    replay = candidate["replay_plan"]
    shadow = candidate["forward_shadow_plan"]
    governance = candidate["governance"]
    lines = [
        f"# {candidate['candidate_id']}",
        "",
        f"- Learning review：`{candidate['linked_learning_review_id']}`",
        f"- Causal chain：`{candidate['linked_causal_chain_id']}`",
        f"- 日期：{candidate['signal_date']}",
        f"- 触发原因：{candidate['trigger']['reason']}",
        f"- 候选假设：{candidate['candidate_hypothesis']}",
        f"- Replay：{replay['status']}，窗口 {replay['start_date']} 至 {replay['end_date']}",
        f"- Shadow：{shadow['status']}，{shadow['start_date']} 至 {shadow['end_date']}",
        f"- Production effect：{candidate['production_effect']}",
        f"- Governance：{governance['approval_status']}",
    ]
    return "\n".join(lines) + "\n"


def _rule_candidate(
    *,
    item: dict[str, Any],
    replay_start: date,
    replay_end: date,
    shadow_start: date,
    shadow_days: int,
) -> dict[str, Any]:
    review_id = str(item["review_id"])
    signal_date = str(item.get("signal_date") or "")
    triggered_gate_ids = [str(gate) for gate in item.get("triggered_gate_ids", [])]
    affected_modules = list(item.get("affected_modules") or [])
    linked_evidence_ids = [str(value) for value in item.get("linked_evidence_ids", [])]
    shadow_end = shadow_start + timedelta(days=shadow_days)
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": _candidate_id(review_id),
        "linked_learning_review_id": review_id,
        "linked_causal_chain_id": item.get("chain_id"),
        "linked_decision_snapshot": item.get("linked_decision_snapshot"),
        "signal_date": signal_date,
        "market_regime": item.get("market_regime") or {},
        "trigger": {
            "outcome_direction": item.get("outcome_direction"),
            "attribution_category": item.get("attribution_category"),
            "reason": item.get("reason") or "",
            "next_step": item.get("next_step") or "",
        },
        "linked_evidence_ids": linked_evidence_ids,
        "triggered_gate_ids": triggered_gate_ids,
        "affected_modules": affected_modules,
        "candidate_hypothesis": _candidate_hypothesis(
            triggered_gate_ids=triggered_gate_ids,
            affected_modules=affected_modules,
        ),
        "candidate_rule_change": {
            "status": "DESIGN_REQUIRED",
            "scope": _candidate_scope(triggered_gate_ids, affected_modules),
            "description": item.get("next_step") or "复核是否需要候选规则。",
        },
        "replay_plan": {
            "status": "NOT_RUN",
            "start_date": replay_start.isoformat(),
            "end_date": replay_end.isoformat(),
            "market_regime": "ai_after_chatgpt",
            "required_metrics": [
                "total_return",
                "max_drawdown",
                "turnover",
                "confidence_bucket_change",
                "gate_trigger_count",
                "failure_sample_count",
            ],
            "point_in_time_policy": (
                "Replay must use the same signal-date-visible inputs as production "
                "backtests and must not use future outcomes to alter signal context."
            ),
        },
        "forward_shadow_plan": {
            "status": "PENDING",
            "start_date": shadow_start.isoformat(),
            "end_date": shadow_end.isoformat(),
            "min_observation_days": shadow_days,
            "production_effect": "none",
            "logging_requirement": (
                "Record candidate-only score/gate/position deltas beside production "
                "outputs without changing daily reports."
            ),
        },
        "sample_limitations": {
            "available_window_count": item.get("available_window_count", 0),
            "sample_limited": item.get("sample_limited", False),
            "outcome_summary": item.get("outcome_summary") or {},
        },
        "risks": [
            "候选规则可能过拟合单个失败样本。",
            "候选规则可能降低简单性或提高换手。",
            "未完成 replay/shadow 前不能解释为生产改进。",
        ],
        "rollback_conditions": [
            "历史 replay 相比 production 回撤或换手显著恶化。",
            "前向 shadow 未改善失败样本或降低结论置信度。",
            "GOV-001 rule card 未批准或 owner 拒绝上线。",
        ],
        "governance": {
            "approval_required": True,
            "approval_status": "NOT_SUBMITTED",
            "required_task": "GOV-001",
        },
        "review_status": "candidate_registered",
        "production_effect": "none",
        "approved_for_production": False,
    }


def _candidate_id(review_id: str) -> str:
    safe_id = (
        review_id.replace("learning_review:", "")
        .replace(":", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )
    return f"rule_experiment:{safe_id}"


def _candidate_scope(
    triggered_gate_ids: list[str],
    affected_modules: list[dict[str, Any]],
) -> str:
    if triggered_gate_ids:
        return "position_gate"
    modules = [str(module.get("component")) for module in affected_modules]
    if modules:
        return ",".join(sorted(set(modules)))
    return "scoring_rules"


def _candidate_hypothesis(
    triggered_gate_ids: list[str],
    affected_modules: list[dict[str, Any]],
) -> str:
    if triggered_gate_ids:
        return (
            "复核已触发的 position_gate 是否过弱、过强或触发时点错误；"
            "候选规则必须先证明能改善回撤/失败样本且不显著增加换手。"
        )
    modules = [str(module.get("component")) for module in affected_modules]
    if modules:
        return (
            "复核相关评分模块的阈值、权重或置信度扣减是否需要候选调整："
            f"{', '.join(sorted(set(modules)))}。"
        )
    return "复核是否存在未归属到模块或 gate 的规则缺口。"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
