from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.parameter_replay import (
    default_parameter_replay_summary_path,
)

SCHEMA_VERSION = 1
DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "parameter_candidates.json"
)
DEFAULT_PARAMETER_CANDIDATE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass(frozen=True)
class ParameterTrial:
    trial_id: str
    source_scenario_id: str
    label: str
    category: str
    status: str
    total_return_delta_vs_base: float | None
    max_drawdown_delta_vs_base: float | None
    turnover: float | None
    material_total_return_delta: bool | None
    skipped_reason: str | None
    candidate_eligible: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "trial_id": self.trial_id,
            "source_scenario_id": self.source_scenario_id,
            "label": self.label,
            "category": self.category,
            "status": self.status,
            "total_return_delta_vs_base": self.total_return_delta_vs_base,
            "max_drawdown_delta_vs_base": self.max_drawdown_delta_vs_base,
            "turnover": self.turnover,
            "material_total_return_delta": self.material_total_return_delta,
            "skipped_reason": self.skipped_reason,
            "candidate_eligible": self.candidate_eligible,
        }


@dataclass(frozen=True)
class ParameterCandidate:
    candidate_id: str
    linked_trial_id: str
    source_scenario_id: str
    label: str
    category: str
    hypothesis: str
    total_return_delta_vs_base: float | None
    max_drawdown_delta_vs_base: float | None
    turnover: float | None
    material_total_return_delta: bool | None
    recommendation_status: str
    replay_status: str
    shadow_status: str
    governance_status: str
    production_effect: str
    next_step: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "linked_trial_id": self.linked_trial_id,
            "source_scenario_id": self.source_scenario_id,
            "label": self.label,
            "category": self.category,
            "hypothesis": self.hypothesis,
            "total_return_delta_vs_base": self.total_return_delta_vs_base,
            "max_drawdown_delta_vs_base": self.max_drawdown_delta_vs_base,
            "turnover": self.turnover,
            "material_total_return_delta": self.material_total_return_delta,
            "recommendation_status": self.recommendation_status,
            "replay_status": self.replay_status,
            "shadow_status": self.shadow_status,
            "governance_status": self.governance_status,
            "production_effect": self.production_effect,
            "next_step": self.next_step,
        }


@dataclass(frozen=True)
class ParameterCandidateLedger:
    as_of: date
    generated_at: datetime
    source_parameter_replay_path: Path
    source_backtest_summary_path: str
    source_status: str
    source_market_regime: dict[str, Any] | None
    production_effect: str
    trials: tuple[ParameterTrial, ...]
    candidates: tuple[ParameterCandidate, ...]
    warnings: tuple[str, ...]

    @property
    def trial_count(self) -> int:
        return len(self.trials)

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def ready_for_owner_review_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if candidate.recommendation_status == "READY_FOR_OWNER_REVIEW"
        )

    @property
    def needs_policy_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if candidate.recommendation_status == "NEEDS_MATERIALITY_POLICY"
        )

    @property
    def material_risk_review_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if candidate.recommendation_status == "MATERIAL_RISK_REVIEW"
        )

    @property
    def status(self) -> str:
        if self.warnings or self.needs_policy_count:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "parameter_candidate_ledger",
            "production_effect": self.production_effect,
            "status": self.status,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "source_parameter_replay_path": str(self.source_parameter_replay_path),
            "source_backtest_summary_path": self.source_backtest_summary_path,
            "source_status": self.source_status,
            "source_market_regime": self.source_market_regime,
            "trial_count": self.trial_count,
            "candidate_count": self.candidate_count,
            "ready_for_owner_review_count": self.ready_for_owner_review_count,
            "needs_policy_count": self.needs_policy_count,
            "material_risk_review_count": self.material_risk_review_count,
            "warnings": list(self.warnings),
            "trials": [trial.to_dict() for trial in self.trials],
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


def default_parameter_candidate_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"parameter_candidates_{as_of.isoformat()}.md"


def build_parameter_candidate_ledger(
    *,
    parameter_replay_summary_path: Path | None,
    as_of: date,
    generated_at: datetime | None = None,
) -> ParameterCandidateLedger:
    replay_path = parameter_replay_summary_path or default_parameter_replay_summary_path(
        PROJECT_ROOT / "outputs" / "reports",
        as_of,
    )
    payload = _read_json_object(replay_path)
    generated = generated_at or datetime.now(tz=UTC)
    trials = tuple(_trial_from_scenario(item, payload) for item in _scenario_items(payload))
    candidates = tuple(
        _candidate_from_trial(trial, scenario, payload)
        for trial, scenario in zip(trials, _scenario_items(payload), strict=True)
        if trial.candidate_eligible
    )
    warnings = _ledger_warnings(payload, trials, candidates)
    return ParameterCandidateLedger(
        as_of=as_of,
        generated_at=generated,
        source_parameter_replay_path=replay_path,
        source_backtest_summary_path=str(payload.get("source_summary_path") or "UNKNOWN"),
        source_status=str(payload.get("status") or "UNKNOWN"),
        source_market_regime=_optional_dict(payload.get("market_regime")),
        production_effect="none",
        trials=trials,
        candidates=candidates,
        warnings=warnings,
    )


def write_parameter_candidate_ledger(
    ledger: ParameterCandidateLedger,
    output_path: Path = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(ledger.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_parameter_candidate_report(
    ledger: ParameterCandidateLedger,
    ledger_path: Path,
) -> str:
    lines = [
        "# 参数候选台账",
        "",
        f"- 状态：{ledger.status}",
        "- production_effect：none",
        f"- 生成时间：{ledger.generated_at.isoformat()}",
        f"- 复核日期：{ledger.as_of.isoformat()}",
        f"- 参数 replay 摘要：`{ledger.source_parameter_replay_path}`",
        f"- 来源 backtest summary：`{ledger.source_backtest_summary_path}`",
        f"- 机器可读 ledger：`{ledger_path}`",
        f"- Trial 数：{ledger.trial_count}",
        f"- Candidate 数：{ledger.candidate_count}",
        f"- Ready for owner review：{ledger.ready_for_owner_review_count}",
        f"- Material risk review：{ledger.material_risk_review_count}",
        f"- Needs materiality policy：{ledger.needs_policy_count}",
    ]
    if ledger.source_market_regime:
        lines.append(
            "- 市场阶段："
            f"{ledger.source_market_regime.get('name', 'UNKNOWN')}"
            f"（{ledger.source_market_regime.get('regime_id', 'UNKNOWN')}）"
        )
    lines.extend(
        [
            "",
            "## 候选参数",
            "",
        ]
    )
    if not ledger.candidates:
        lines.append("当前没有可登记的参数候选；请先生成 parameter replay 摘要。")
    else:
        lines.extend(
            [
                "| Candidate | Scenario | 类别 | 收益差异 | 回撤变化 | 换手 | 状态 | 下一步 |",
                "|---|---|---|---:|---:|---:|---|---|",
            ]
        )
        for candidate in ledger.candidates:
            lines.append(_candidate_row(candidate))
    lines.extend(
        [
            "",
            "## Trial Registry",
            "",
        ]
    )
    if not ledger.trials:
        lines.append("当前没有 trial 记录。")
    else:
        lines.extend(
            [
                "| Trial | Scenario | 类别 | 状态 | 收益差异 | Candidate eligible |",
                "|---|---|---|---|---:|---|",
            ]
        )
        for trial in ledger.trials:
            lines.append(_trial_row(trial))
    lines.extend(
        [
            "",
            "## 限制与治理边界",
            "",
            (
                "- 候选台账不批准参数上线；所有候选仍需 owner review、"
                "forward shadow 和 overlay/rule card 治理。"
            ),
            "- 未配置 materiality policy 的候选只能进入观察或补政策阈值，不能作为晋级依据。",
        ]
    )
    if ledger.warnings:
        lines.extend(f"- {warning}" for warning in ledger.warnings)
    return "\n".join(lines).rstrip() + "\n"


def write_parameter_candidate_report(
    ledger: ParameterCandidateLedger,
    ledger_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_parameter_candidate_report(ledger, ledger_path),
        encoding="utf-8",
    )
    return output_path


def load_parameter_candidate_ledger(input_path: Path) -> dict[str, Any]:
    return _read_json_object(input_path)


def _trial_from_scenario(
    scenario: dict[str, Any],
    payload: dict[str, Any],
) -> ParameterTrial:
    scenario_id = str(scenario.get("scenario_id") or "unknown")
    trial_id = _trial_id(payload, scenario_id)
    skipped_reason = _optional_str(scenario.get("skipped_reason"))
    total_return_delta = _float_or_none(scenario.get("total_return_delta_vs_base"))
    candidate_eligible = skipped_reason is None and total_return_delta is not None
    return ParameterTrial(
        trial_id=trial_id,
        source_scenario_id=scenario_id,
        label=str(scenario.get("label") or scenario_id),
        category=str(scenario.get("category") or "unknown"),
        status=str(scenario.get("status") or "UNKNOWN"),
        total_return_delta_vs_base=total_return_delta,
        max_drawdown_delta_vs_base=_float_or_none(scenario.get("max_drawdown_delta_vs_base")),
        turnover=_float_or_none(scenario.get("turnover")),
        material_total_return_delta=_bool_or_none(scenario.get("material_total_return_delta")),
        skipped_reason=skipped_reason,
        candidate_eligible=candidate_eligible,
    )


def _candidate_from_trial(
    trial: ParameterTrial,
    scenario: dict[str, Any],
    payload: dict[str, Any],
) -> ParameterCandidate:
    recommendation_status = _recommendation_status(trial)
    return ParameterCandidate(
        candidate_id=_candidate_id(payload, trial.source_scenario_id),
        linked_trial_id=trial.trial_id,
        source_scenario_id=trial.source_scenario_id,
        label=trial.label,
        category=trial.category,
        hypothesis=str(scenario.get("description") or "参数复测场景，需人工补充业务假设。"),
        total_return_delta_vs_base=trial.total_return_delta_vs_base,
        max_drawdown_delta_vs_base=trial.max_drawdown_delta_vs_base,
        turnover=trial.turnover,
        material_total_return_delta=trial.material_total_return_delta,
        recommendation_status=recommendation_status,
        replay_status="COMPLETED_FROM_ROBUSTNESS_SUMMARY",
        shadow_status="NOT_STARTED",
        governance_status="NOT_APPROVED",
        production_effect="none",
        next_step=_candidate_next_step(recommendation_status),
    )


def _recommendation_status(trial: ParameterTrial) -> str:
    if trial.material_total_return_delta is True:
        if trial.total_return_delta_vs_base is not None and trial.total_return_delta_vs_base > 0:
            return "READY_FOR_OWNER_REVIEW"
        return "MATERIAL_RISK_REVIEW"
    if trial.material_total_return_delta is False:
        return "OBSERVE_ONLY"
    return "NEEDS_MATERIALITY_POLICY"


def _candidate_next_step(status: str) -> str:
    if status == "READY_FOR_OWNER_REVIEW":
        return "人工复核假设和风险；通过后只能进入 shadow 或 candidate overlay。"
    if status == "MATERIAL_RISK_REVIEW":
        return "记录为参数敏感性风险；不作为上线候选，需补窗口复测或降低结论可信度。"
    if status == "OBSERVE_ONLY":
        return "继续观察；当前收益差异未达到 material 阈值。"
    return "补齐 materiality policy 或重新生成带阈值的 parameter replay。"


def _ledger_warnings(
    payload: dict[str, Any],
    trials: tuple[ParameterTrial, ...],
    candidates: tuple[ParameterCandidate, ...],
) -> tuple[str, ...]:
    warnings = _warnings_from_payload(payload)
    if payload.get("production_effect") != "none":
        warnings.append("来源 parameter replay 未声明 production_effect=none。")
    if not trials:
        warnings.append("来源 parameter replay 没有 trial 场景。")
    if not candidates:
        warnings.append("没有可登记参数候选；请检查 parameter replay 输入。")
    if any(
        candidate.recommendation_status == "NEEDS_MATERIALITY_POLICY" for candidate in candidates
    ):
        warnings.append("存在候选缺少 materiality policy 阈值，不能进入 owner approval。")
    return tuple(warnings)


def _warnings_from_payload(payload: dict[str, Any]) -> list[str]:
    raw_warnings = payload.get("warnings", [])
    if isinstance(raw_warnings, list):
        return [str(item) for item in raw_warnings if str(item).strip()]
    if isinstance(raw_warnings, str) and raw_warnings.strip():
        return [raw_warnings]
    return []


def _scenario_items(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_items = payload.get("scenarios", [])
    if not isinstance(raw_items, list):
        return ()
    return tuple(item for item in raw_items if isinstance(item, dict))


def _trial_id(payload: dict[str, Any], scenario_id: str) -> str:
    return "parameter_trial:" + _source_token(payload) + ":" + _id_token(scenario_id)


def _candidate_id(payload: dict[str, Any], scenario_id: str) -> str:
    return "parameter_candidate:" + _source_token(payload) + ":" + _id_token(scenario_id)


def _source_token(payload: dict[str, Any]) -> str:
    start = str(payload.get("requested_start") or "unknown_start")
    end = str(payload.get("requested_end") or "unknown_end")
    return _id_token(f"{start}_{end}")


def _id_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value.strip())
    return token.strip("_") or "unknown"


def _candidate_row(candidate: ParameterCandidate) -> str:
    return (
        f"| `{candidate.candidate_id}` | `{candidate.source_scenario_id}` | "
        f"{candidate.category} | "
        f"{_format_pct(candidate.total_return_delta_vs_base, signed=True)} | "
        f"{_format_pct(candidate.max_drawdown_delta_vs_base, signed=True)} | "
        f"{_format_float(candidate.turnover)} | "
        f"{candidate.recommendation_status} | {candidate.next_step} |"
    )


def _trial_row(trial: ParameterTrial) -> str:
    return (
        f"| `{trial.trial_id}` | `{trial.source_scenario_id}` | {trial.category} | "
        f"{trial.status} | {_format_pct(trial.total_return_delta_vs_base, signed=True)} | "
        f"{trial.candidate_eligible} |"
    )


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON ledger must contain an object: {path}")
    return payload


def _optional_dict(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _format_pct(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.1%}"


def _format_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
