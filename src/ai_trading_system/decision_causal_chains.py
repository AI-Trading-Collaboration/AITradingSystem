from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
DEFAULT_DECISION_CAUSAL_CHAIN_PATH = (
    PROJECT_ROOT / "data" / "processed" / "decision_causal_chains.json"
)


@dataclass(frozen=True)
class DecisionCausalChainLedger:
    schema_version: int
    generated_at: datetime
    chain_count: int
    source_snapshot_count: int
    source_outcome_count: int
    chains: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "chain_count": self.chain_count,
            "source_snapshot_count": self.source_snapshot_count,
            "source_outcome_count": self.source_outcome_count,
            "chains": list(self.chains),
        }


def default_decision_causal_chain_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"decision_causal_chains_{as_of.isoformat()}.md"


def load_decision_causal_chain_ledger(input_path: Path) -> dict[str, Any]:
    return json.loads(input_path.read_text(encoding="utf-8"))


def lookup_decision_causal_chain(input_path: Path, chain_id: str) -> dict[str, Any]:
    ledger = load_decision_causal_chain_ledger(input_path)
    for chain in ledger.get("chains", []):
        if chain.get("chain_id") == chain_id:
            return chain
    raise KeyError(f"decision causal chain not found: {chain_id}")


def build_decision_causal_chain_ledger(
    *,
    snapshots: tuple[dict[str, Any], ...],
    outcomes: pd.DataFrame | None = None,
    generated_at: datetime | None = None,
) -> DecisionCausalChainLedger:
    generated_at = generated_at or datetime.now(tz=UTC)
    sorted_snapshots = tuple(sorted(snapshots, key=lambda item: item["signal_date"]))
    outcome_frame = outcomes if outcomes is not None else pd.DataFrame()
    chains: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for snapshot in sorted_snapshots:
        chains.append(_chain_record(snapshot=snapshot, previous=previous, outcomes=outcome_frame))
        previous = snapshot
    return DecisionCausalChainLedger(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        chain_count=len(chains),
        source_snapshot_count=len(sorted_snapshots),
        source_outcome_count=0 if outcome_frame.empty else len(outcome_frame),
        chains=tuple(chains),
    )


def write_decision_causal_chain_ledger(
    ledger: DecisionCausalChainLedger,
    output_path: Path = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(ledger.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def render_decision_causal_chain_report(
    ledger: DecisionCausalChainLedger,
    ledger_path: Path,
) -> str:
    lines = [
        "# 决策因果链 ledger",
        "",
        "- 状态：PASS_WITH_LIMITATIONS",
        f"- 生成时间：{ledger.generated_at.isoformat()}",
        f"- 链条数：{ledger.chain_count}",
        f"- 来源决策快照数：{ledger.source_snapshot_count}",
        f"- 来源 outcome 行数：{ledger.source_outcome_count}",
        f"- 机器可读 ledger：`{ledger_path}`",
        "- 数据质量状态：继承自每个 `decision_snapshot.quality`；"
        "后续 outcome 只作为观察结果追加，不得覆盖 signal-time 质量状态。",
        "- 治理边界：`signal_time_context` 只保存 signal_date 当时可见信息；"
        "`post_signal_observations` 只能追加后验 outcome，不得改写当时因果解释。",
        "",
        "## 链条摘要",
        "",
        "| Chain | 日期 | 质量状态 | 总分变化 | 置信度变化 | 仓位变化 | 触发 gate | outcome |",
        "|---|---|---|---:|---:|---:|---|---|",
    ]
    for chain in ledger.chains:
        signal = chain["signal_time_context"]
        post = chain["post_signal_observations"]
        lines.append(
            "| "
            f"`{chain['chain_id']}` | "
            f"{chain['signal_date']} | "
            f"{_quality_summary(signal.get('quality', {}))} | "
            f"{_format_delta(signal['score_delta'])} | "
            f"{_format_delta(signal['confidence_delta'])} | "
            f"{_format_delta(signal['position_delta']['final_max_delta'])} | "
            f"{_gate_summary(signal['triggered_gates'])} | "
            f"{len(post['linked_outcome_windows'])} 个窗口 |"
        )
    return "\n".join(lines) + "\n"


def write_decision_causal_chain_report(
    ledger: DecisionCausalChainLedger,
    ledger_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_decision_causal_chain_report(ledger, ledger_path),
        encoding="utf-8",
    )
    return output_path


def render_decision_causal_chain_lookup(chain: dict[str, Any]) -> str:
    signal = chain["signal_time_context"]
    post = chain["post_signal_observations"]
    affected_modules = ", ".join(
        item["component"] for item in signal["affected_modules"]
    ) or "无"
    lines = [
        f"# {chain['chain_id']}",
        "",
        f"- 日期：{chain['signal_date']}",
        f"- 市场阶段：{chain['market_regime'].get('regime_id')}",
        f"- 数据质量状态：{_quality_summary(signal.get('quality', {}))}",
        f"- 决策快照：`{signal['linked_decision_snapshot']}`",
        f"- Evidence：{', '.join(signal['linked_evidence_ids']) or '无'}",
        f"- 受影响模块：{affected_modules}",
        f"- 触发 gate：{_gate_summary(signal['triggered_gates'])}",
        f"- Outcome 窗口：{len(post['linked_outcome_windows'])}",
        f"- 复核状态：{chain['review_status']}",
    ]
    return "\n".join(lines) + "\n"


def load_decision_outcomes_frame(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        return pd.DataFrame()
    return pd.read_csv(input_path)


def _chain_record(
    *,
    snapshot: dict[str, Any],
    previous: dict[str, Any] | None,
    outcomes: pd.DataFrame,
) -> dict[str, Any]:
    signal_date = snapshot["signal_date"]
    snapshot_id = snapshot["snapshot_id"]
    trace_refs = _trace_refs(snapshot)
    signal_context = {
        "linked_decision_snapshot": snapshot_id,
        "linked_belief_state": _belief_state_path(snapshot),
        "linked_trace_bundle": trace_refs["trace_bundle_path"],
        "overall_claim_id": trace_refs["overall_claim_id"],
        "linked_evidence_ids": _linked_evidence_ids(trace_refs),
        "quality": snapshot.get("quality") or {},
        "affected_modules": _affected_modules(snapshot, previous),
        "score_delta": _score_delta(snapshot, previous),
        "confidence_delta": _confidence_delta(snapshot, previous),
        "triggered_gates": _triggered_gates(snapshot),
        "position_delta": _position_delta(snapshot, previous),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "chain_id": f"decision_causal_chain:{signal_date}:overall_position",
        "signal_date": signal_date,
        "market_regime": snapshot.get("market_regime") or {},
        "chain_type": "overall_position",
        "review_status": "pending_review",
        "future_outcome_policy": (
            "Outcome rows are append-only post-signal observations and must not "
            "rewrite signal_time_context."
        ),
        "signal_time_context": signal_context,
        "post_signal_observations": {
            "append_only": True,
            "linked_outcome_windows": _outcome_windows(outcomes, snapshot_id),
            "linked_rule_candidate": None,
        },
    }


def _trace_refs(snapshot: dict[str, Any]) -> dict[str, str | None]:
    trace = snapshot.get("trace") or {}
    return {
        "trace_bundle_path": trace.get("trace_bundle_path"),
        "overall_claim_id": trace.get("overall_claim_id"),
    }


def _linked_evidence_ids(trace_refs: dict[str, str | None]) -> list[str]:
    trace_path = trace_refs.get("trace_bundle_path")
    claim_id = trace_refs.get("overall_claim_id")
    if trace_path is None or claim_id is None:
        return []
    path = Path(trace_path)
    if not path.exists():
        return []
    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    for claim in bundle.get("claims", []):
        if claim.get("claim_id") == claim_id:
            return [str(item) for item in claim.get("evidence_ids", [])]
    return []


def _affected_modules(
    snapshot: dict[str, Any],
    previous: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    previous_components = _component_index(previous)
    records: list[dict[str, Any]] = []
    for component in (snapshot.get("scores") or {}).get("components", []):
        name = str(component.get("component"))
        previous_component = previous_components.get(name)
        score_delta = (
            None
            if previous_component is None
            else _optional_float(component.get("score"))
            - _optional_float(previous_component.get("score"))
        )
        confidence_delta = (
            None
            if previous_component is None
            else _optional_float(component.get("confidence"))
            - _optional_float(previous_component.get("confidence"))
        )
        include = previous_component is None
        include = include or (score_delta is not None and abs(score_delta) >= 5.0)
        include = include or _optional_float(component.get("score")) < 50
        include = include or _optional_float(component.get("confidence")) < 0.60
        include = include or component.get("source_type") in {
            "placeholder",
            "insufficient_data",
            "manual_input",
            "partial_manual_input",
        }
        if include:
            records.append(
                {
                    "component": name,
                    "score": component.get("score"),
                    "score_delta": score_delta,
                    "confidence": component.get("confidence"),
                    "confidence_delta": confidence_delta,
                    "source_type": component.get("source_type"),
                    "reason": component.get("reason"),
                }
            )
    return records


def _score_delta(snapshot: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    if previous is None:
        return None
    current = _optional_float((snapshot.get("scores") or {}).get("overall_score"))
    prior = _optional_float((previous.get("scores") or {}).get("overall_score"))
    return current - prior


def _confidence_delta(snapshot: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    if previous is None:
        return None
    current = _optional_float((snapshot.get("scores") or {}).get("confidence_score"))
    prior = _optional_float((previous.get("scores") or {}).get("confidence_score"))
    return current - prior


def _position_delta(snapshot: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    current = _final_band(snapshot)
    prior = _final_band(previous) if previous is not None else None
    return {
        "final_min": current.get("min_position"),
        "final_max": current.get("max_position"),
        "previous_final_min": None if prior is None else prior.get("min_position"),
        "previous_final_max": None if prior is None else prior.get("max_position"),
        "final_min_delta": (
            None
            if prior is None
            else _optional_float(current.get("min_position"))
            - _optional_float(prior.get("min_position"))
        ),
        "final_max_delta": (
            None
            if prior is None
            else _optional_float(current.get("max_position"))
            - _optional_float(prior.get("max_position"))
        ),
    }


def _triggered_gates(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    gates = ((snapshot.get("positions") or {}).get("position_gates")) or []
    return [
        {
            "gate_id": gate.get("gate_id"),
            "label": gate.get("label"),
            "max_position": gate.get("max_position"),
            "reason": gate.get("reason"),
        }
        for gate in gates
        if gate.get("triggered") and gate.get("gate_id") != "score_model"
    ]


def _outcome_windows(outcomes: pd.DataFrame, snapshot_id: str) -> list[dict[str, Any]]:
    if outcomes.empty or "snapshot_id" not in outcomes.columns:
        return []
    rows = outcomes.loc[outcomes["snapshot_id"] == snapshot_id]
    windows: list[dict[str, Any]] = []
    for _, row in rows.sort_values("horizon_days").iterrows():
        windows.append(
            {
                "horizon_days": _row_value(row, "horizon_days"),
                "outcome_status": _row_value(row, "outcome_status"),
                "outcome_end_date": _row_value(row, "outcome_end_date"),
                "ai_proxy_return": _row_value(row, "ai_proxy_return"),
                "ai_proxy_max_drawdown": _row_value(row, "ai_proxy_max_drawdown"),
                "ai_proxy_realized_volatility": _row_value(
                    row,
                    "ai_proxy_realized_volatility",
                ),
                "hit": _row_value(row, "hit"),
            }
        )
    return windows


def _component_index(snapshot: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if snapshot is None:
        return {}
    return {
        str(component.get("component")): component
        for component in (snapshot.get("scores") or {}).get("components", [])
    }


def _final_band(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return {}
    return (snapshot.get("positions") or {}).get("final_risk_asset_ai_band") or {}


def _belief_state_path(snapshot: dict[str, Any]) -> str | None:
    belief_ref = snapshot.get("belief_state_ref")
    if not belief_ref:
        return None
    return belief_ref.get("path")


def _row_value(row: pd.Series, column: str) -> Any:
    if column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _optional_float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _format_delta(value: object) -> str:
    if value is None:
        return "NA"
    return f"{float(value):+.2f}"


def _gate_summary(gates: list[dict[str, Any]]) -> str:
    if not gates:
        return "无"
    return "、".join(str(gate.get("gate_id")) for gate in gates)


def _quality_summary(quality: dict[str, Any]) -> str:
    if not quality:
        return "UNKNOWN"
    status = quality.get("market_data_status") or "UNKNOWN"
    errors = quality.get("market_data_error_count", 0)
    warnings = quality.get("market_data_warning_count", 0)
    return f"{status} / 错误 {errors} / 警告 {warnings}"
