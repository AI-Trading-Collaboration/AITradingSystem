from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH,
    PROJECT_ROOT,
    load_backtest_validation_policy,
)

DEFAULT_PARAMETER_REPLAY_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_BACKTEST_ROBUSTNESS_DIR = PROJECT_ROOT / "outputs" / "backtests"
PARAMETER_REPLAY_CATEGORIES = frozenset(
    {
        "module_weight_perturbation",
        "rebalance_frequency",
        "cost",
        "window",
        "baseline",
    }
)


@dataclass(frozen=True)
class ParameterReplayScenario:
    scenario_id: str
    label: str
    category: str
    status: str
    total_return: float | None
    total_return_delta_vs_base: float | None
    max_drawdown: float | None
    max_drawdown_delta_vs_base: float | None
    sharpe: float | None
    turnover: float | None
    skipped_reason: str | None
    description: str
    material_total_return_delta: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "label": self.label,
            "category": self.category,
            "status": self.status,
            "total_return": self.total_return,
            "total_return_delta_vs_base": self.total_return_delta_vs_base,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_delta_vs_base": self.max_drawdown_delta_vs_base,
            "sharpe": self.sharpe,
            "turnover": self.turnover,
            "skipped_reason": self.skipped_reason,
            "description": self.description,
            "material_total_return_delta": self.material_total_return_delta,
        }


@dataclass(frozen=True)
class ParameterReplayReport:
    as_of: date
    generated_at: datetime
    source_summary_path: Path
    source_status: str
    source_report_type: str
    requested_start: str
    requested_end: str
    first_signal_date: str
    last_signal_date: str
    market_regime: dict[str, Any] | None
    data_quality_status: str
    base_dynamic: dict[str, Any]
    scenarios: tuple[ParameterReplayScenario, ...]
    warnings: tuple[str, ...]
    materiality_policy: dict[str, Any] | None
    material_total_return_delta_abs: float | None

    @property
    def status(self) -> str:
        if not self.scenarios:
            return "PASS_WITH_LIMITATIONS"
        if self.warnings:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def scenario_count(self) -> int:
        return len(self.scenarios)

    @property
    def completed_scenario_count(self) -> int:
        return sum(1 for scenario in self.scenarios if scenario.total_return is not None)

    @property
    def material_delta_count(self) -> int:
        return sum(1 for scenario in self.scenarios if scenario.material_total_return_delta is True)

    def to_summary(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "report_type": "feedback_parameter_replay",
            "production_effect": "none",
            "status": self.status,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "source_summary_path": str(self.source_summary_path),
            "source_status": self.source_status,
            "source_report_type": self.source_report_type,
            "requested_start": self.requested_start,
            "requested_end": self.requested_end,
            "first_signal_date": self.first_signal_date,
            "last_signal_date": self.last_signal_date,
            "market_regime": self.market_regime,
            "data_quality_status": self.data_quality_status,
            "base_dynamic": self.base_dynamic,
            "scenario_count": self.scenario_count,
            "completed_scenario_count": self.completed_scenario_count,
            "material_delta_count": self.material_delta_count,
            "materiality_policy": self.materiality_policy,
            "material_total_return_delta_abs": self.material_total_return_delta_abs,
            "warnings": list(self.warnings),
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
        }


def default_parameter_replay_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"parameter_replay_{as_of.isoformat()}.md"


def default_parameter_replay_summary_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"parameter_replay_{as_of.isoformat()}.json"


def latest_backtest_robustness_summary_path(output_dir: Path) -> Path | None:
    candidates = [path for path in output_dir.glob("backtest_robustness_*.json") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def build_parameter_replay_report(
    *,
    robustness_summary_path: Path,
    as_of: date,
    generated_at: datetime | None = None,
) -> ParameterReplayReport:
    payload = _read_json_object(robustness_summary_path)
    generated = generated_at or datetime.now(tz=UTC)
    base_dynamic = _dict_value(payload.get("base_dynamic"))
    base_max_drawdown = _float_or_none(base_dynamic.get("max_drawdown"))
    materiality_policy, materiality_warnings = _materiality_policy(payload)
    scenarios = tuple(
        _scenario_from_summary(
            raw_scenario,
            base_max_drawdown=base_max_drawdown,
            materiality_policy=materiality_policy,
        )
        for raw_scenario in _scenario_items(payload)
        if str(raw_scenario.get("category", "")).strip() in PARAMETER_REPLAY_CATEGORIES
    )
    warnings = _report_warnings(payload, scenarios, materiality_warnings)
    return ParameterReplayReport(
        as_of=as_of,
        generated_at=generated,
        source_summary_path=robustness_summary_path,
        source_status=str(payload.get("status") or "UNKNOWN"),
        source_report_type=str(payload.get("report_type") or "UNKNOWN"),
        requested_start=str(payload.get("requested_start") or "UNKNOWN"),
        requested_end=str(payload.get("requested_end") or "UNKNOWN"),
        first_signal_date=str(payload.get("first_signal_date") or "UNKNOWN"),
        last_signal_date=str(payload.get("last_signal_date") or "UNKNOWN"),
        market_regime=_optional_dict(payload.get("market_regime")),
        data_quality_status=str(payload.get("data_quality_status") or "UNKNOWN"),
        base_dynamic=base_dynamic,
        scenarios=scenarios,
        warnings=warnings,
        materiality_policy=materiality_policy,
        material_total_return_delta_abs=_float_or_none(
            (materiality_policy or {}).get("weight_perturbation_material_total_return_delta_abs")
        ),
    )


def render_parameter_replay_report(report: ParameterReplayReport) -> str:
    lines = [
        "# 参数 as-if replay 收益变化报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 复核日期：{report.as_of.isoformat()}",
        f"- 来源 robustness summary：`{report.source_summary_path}`",
        f"- 来源状态：{report.source_status}",
        f"- 请求区间：{report.requested_start} 至 {report.requested_end}",
        f"- 实际信号区间：{report.first_signal_date} 至 {report.last_signal_date}",
        f"- 数据质量状态：{report.data_quality_status}",
    ]
    if report.materiality_policy:
        lines.extend(
            [
                (
                    "- Materiality policy："
                    f"{report.materiality_policy.get('source', 'UNKNOWN')}"
                    f"（{report.materiality_policy.get('version', 'UNKNOWN')}，"
                    f"status={report.materiality_policy.get('status', 'UNKNOWN')}）"
                ),
                (
                    "- Material 阈值："
                    "cost drag "
                    f"{_format_pct(report.materiality_policy.get('material_cost_drag_total_return'))}；"
                    "window abs "
                    f"{_format_pct(report.materiality_policy.get('shifted_start_material_total_return_delta_abs'))}；"
                    "generic abs "
                    f"{_format_pct(report.materiality_policy.get('weight_perturbation_material_total_return_delta_abs'))}"
                ),
            ]
        )
    else:
        lines.append("- Materiality policy：未连接；Material 判定为 n/a。")
    if report.market_regime:
        lines.extend(
            [
                (
                    f"- 市场阶段：{report.market_regime.get('name', 'UNKNOWN')}"
                    f"（{report.market_regime.get('regime_id', 'UNKNOWN')}）"
                ),
                f"- 阶段默认起点：{report.market_regime.get('start_date', 'UNKNOWN')}",
                (
                    f"- 锚定事件：{report.market_regime.get('anchor_date', 'UNKNOWN')} "
                    f"{report.market_regime.get('anchor_event', 'UNKNOWN')}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## 方法边界",
            "",
            (
                "- 本报告消费已生成的 `backtest_robustness_*.json`，"
                "把其中参数相关场景接入 feedback 闭环。"
            ),
            "- 当前阶段只比较 as-if 场景相对基础策略的收益、回撤和换手变化，不自动拟合新权重。",
            (
                "- 任何参数进入 production 前仍需 replay/shadow、owner approval、"
                "overlay 或 rule card 和回滚条件。"
            ),
            "",
            "## 基础策略",
            "",
            "| 指标 | 值 |",
            "|---|---:|",
            f"| 总收益 | {_format_pct(_float_or_none(report.base_dynamic.get('total_return')))} |",
            f"| CAGR | {_format_pct(_float_or_none(report.base_dynamic.get('cagr')))} |",
            (
                "| 最大回撤 | "
                f"{_format_pct(_float_or_none(report.base_dynamic.get('max_drawdown')))} |"
            ),
            f"| Sharpe | {_format_float(_float_or_none(report.base_dynamic.get('sharpe')))} |",
            f"| 换手 | {_format_float(_float_or_none(report.base_dynamic.get('turnover')))} |",
            "",
            "## 参数复测场景",
            "",
        ]
    )
    if not report.scenarios:
        lines.append("未在 robustness summary 中找到参数相关场景；请先运行带 robustness 的回测。")
    else:
        lines.extend(
            [
                (
                    "| Scenario | 类别 | 状态 | 总收益 | 相对基础收益 | 最大回撤 | "
                    "回撤变化 | Sharpe | 换手 | Material |"
                ),
                "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for scenario in report.scenarios:
            lines.append(_scenario_row(scenario))
    lines.extend(
        [
            "",
            "## 限制与下一步",
            "",
        ]
    )
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 未发现阻断性限制；仍需保持 candidate-only 审计边界。")
    lines.append(_next_step(report))
    return "\n".join(lines).rstrip() + "\n"


def write_parameter_replay_report(
    report: ParameterReplayReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_parameter_replay_report(report), encoding="utf-8")
    return output_path


def write_parameter_replay_summary(
    report: ParameterReplayReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_summary(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _scenario_from_summary(
    raw: dict[str, Any],
    *,
    base_max_drawdown: float | None,
    materiality_policy: dict[str, Any] | None,
) -> ParameterReplayScenario:
    category = str(raw.get("category") or "unknown")
    max_drawdown = _float_or_none(raw.get("max_drawdown"))
    total_return_delta = _float_or_none(raw.get("total_return_delta_vs_base"))
    material_delta = _material_total_return_delta(
        category,
        total_return_delta,
        materiality_policy,
    )
    return ParameterReplayScenario(
        scenario_id=str(raw.get("scenario_id") or "unknown"),
        label=str(raw.get("label") or raw.get("scenario_id") or "unknown"),
        category=category,
        status=str(raw.get("status") or "UNKNOWN"),
        total_return=_float_or_none(raw.get("total_return")),
        total_return_delta_vs_base=total_return_delta,
        max_drawdown=max_drawdown,
        max_drawdown_delta_vs_base=(
            None
            if max_drawdown is None or base_max_drawdown is None
            else max_drawdown - base_max_drawdown
        ),
        sharpe=_float_or_none(raw.get("sharpe")),
        turnover=_float_or_none(raw.get("turnover")),
        skipped_reason=_optional_str(raw.get("skipped_reason")),
        description=str(raw.get("description") or ""),
        material_total_return_delta=material_delta,
    )


def _scenario_items(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_items = payload.get("scenarios", [])
    if not isinstance(raw_items, list):
        return ()
    return tuple(item for item in raw_items if isinstance(item, dict))


def _report_warnings(
    payload: dict[str, Any],
    scenarios: tuple[ParameterReplayScenario, ...],
    materiality_warnings: tuple[str, ...],
) -> tuple[str, ...]:
    warnings: list[str] = list(materiality_warnings)
    if payload.get("production_effect") != "none":
        warnings.append("来源 robustness summary 未声明 production_effect=none。")
    if str(payload.get("report_type") or "") != "backtest_robustness":
        warnings.append("来源文件不是 backtest_robustness 摘要，需人工确认输入语义。")
    if not scenarios:
        warnings.append("缺少参数相关 robustness 场景，无法比较参数复测收益变化。")
    if payload.get("data_quality_status") not in {"PASS", "PASS_WITH_WARNINGS"}:
        warnings.append("来源回测数据质量状态不是 PASS/PASS_WITH_WARNINGS。")
    remaining_gaps = payload.get("remaining_gaps", [])
    if isinstance(remaining_gaps, list) and "module_weight_perturbation" in remaining_gaps:
        warnings.append("来源 robustness summary 标记 module_weight_perturbation 仍有缺口。")
    skipped = [scenario.scenario_id for scenario in scenarios if scenario.skipped_reason]
    if skipped:
        warnings.append("存在被跳过的参数场景：" + "、".join(skipped))
    return tuple(warnings)


def _materiality_policy(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    policy = payload.get("policy")
    if isinstance(policy, dict):
        robustness = policy.get("robustness")
        raw_policy = robustness if isinstance(robustness, dict) else policy
        materiality_policy = _materiality_policy_record(
            raw_policy,
            source="backtest_robustness_summary",
            source_path=None,
            metadata=_optional_dict(payload.get("policy_metadata")),
        )
        if materiality_policy is not None:
            return materiality_policy, ()
    try:
        current_policy = load_backtest_validation_policy()
    except Exception as exc:  # pragma: no cover - exercised only for broken local config
        return None, (
            "来源 robustness summary 缺少 materiality policy，且当前 "
            f"backtest validation policy 读取失败：{exc}",
        )
    materiality_policy = _materiality_policy_record(
        current_policy.robustness.model_dump(mode="json"),
        source="current_backtest_validation_policy",
        source_path=str(DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH),
        metadata=current_policy.policy_metadata.model_dump(mode="json"),
    )
    if materiality_policy is None:
        return None, ("当前 backtest validation policy 未提供 materiality 阈值。",)
    return materiality_policy, (
        "来源 robustness summary 缺少 materiality policy；已使用当前 "
        f"`{DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH}` 做 material 判定，"
        "建议重跑 robustness 产物以固化 as-run policy。",
    )


def _materiality_policy_record(
    raw_policy: dict[str, Any],
    *,
    source: str,
    source_path: str | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any] | None:
    cost_drag = _float_or_none(raw_policy.get("material_cost_drag_total_return"))
    shifted_start = _float_or_none(raw_policy.get("shifted_start_material_total_return_delta_abs"))
    generic_delta = _float_or_none(
        raw_policy.get("weight_perturbation_material_total_return_delta_abs")
    )
    oos_delta = _float_or_none(raw_policy.get("oos_material_underperformance_total_return_delta"))
    if all(value is None for value in (cost_drag, shifted_start, generic_delta, oos_delta)):
        return None
    metadata = metadata or {}
    return {
        "source": source,
        "source_path": source_path,
        "version": metadata.get("version", "UNKNOWN"),
        "status": metadata.get("status", "UNKNOWN"),
        "metric": "total_return_delta_vs_base",
        "material_cost_drag_total_return": cost_drag,
        "shifted_start_material_total_return_delta_abs": shifted_start,
        "weight_perturbation_material_total_return_delta_abs": generic_delta,
        "oos_material_underperformance_total_return_delta": oos_delta,
    }


def _material_total_return_delta(
    category: str,
    total_return_delta: float | None,
    materiality_policy: dict[str, Any] | None,
) -> bool | None:
    if total_return_delta is None or materiality_policy is None:
        return None
    if category == "cost":
        threshold = _float_or_none(materiality_policy.get("material_cost_drag_total_return"))
        return None if threshold is None else total_return_delta < threshold
    if category == "window":
        threshold = _float_or_none(
            materiality_policy.get("shifted_start_material_total_return_delta_abs")
        )
        return None if threshold is None else abs(total_return_delta) > threshold
    threshold = _float_or_none(
        materiality_policy.get("weight_perturbation_material_total_return_delta_abs")
    )
    return None if threshold is None else abs(total_return_delta) > threshold


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON summary must contain an object: {path}")
    return payload


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _scenario_row(scenario: ParameterReplayScenario) -> str:
    material = (
        "n/a"
        if scenario.material_total_return_delta is None
        else "yes"
        if scenario.material_total_return_delta
        else "no"
    )
    return (
        f"| `{scenario.scenario_id}` | {scenario.category} | {scenario.status} | "
        f"{_format_pct(scenario.total_return)} | "
        f"{_format_pct(scenario.total_return_delta_vs_base, signed=True)} | "
        f"{_format_pct(scenario.max_drawdown)} | "
        f"{_format_pct(scenario.max_drawdown_delta_vs_base, signed=True)} | "
        f"{_format_float(scenario.sharpe)} | {_format_float(scenario.turnover)} | "
        f"{material} |"
    )


def _next_step(report: ParameterReplayReport) -> str:
    if not report.scenarios:
        return "- 下一步：先运行 `aits backtest --robustness-report`，再生成参数 replay 报告。"
    if report.material_delta_count:
        return (
            "- 下一步：对 material delta 场景做人工复核；若要推进，只能生成 candidate overlay "
            "或 shadow，不得直接改 production。"
        )
    return "- 下一步：继续积累 outcome 和 shadow 样本；当前参数 replay 不支持生产晋级。"


def _format_pct(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.1%}"


def _format_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
