from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from math import log, sqrt
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
        "signal_family_baseline",
        "same_turnover_random_strategy",
        "same_exposure_random_strategy",
        "out_of_sample_validation",
        "score_architecture_baseline",
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
    return_delta_bootstrap_ci_low: float | None
    return_delta_bootstrap_ci_high: float | None
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
            "return_delta_bootstrap_ci_low": self.return_delta_bootstrap_ci_low,
            "return_delta_bootstrap_ci_high": self.return_delta_bootstrap_ci_high,
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
    robustness_evidence: dict[str, Any]
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
            "robustness_evidence": self.robustness_evidence,
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
    robustness_evidence = _robustness_evidence(payload, materiality_policy)
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
        robustness_evidence=robustness_evidence,
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
            "## 反过拟合证据摘要",
            "",
            *_robustness_evidence_lines(report.robustness_evidence),
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
                    "回撤变化 | Sharpe | 换手 | Bootstrap CI | Material |"
                ),
                "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
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
    bootstrap_ci = _dict_value(raw.get("return_delta_bootstrap_ci_95"))
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
        return_delta_bootstrap_ci_low=_float_or_none(bootstrap_ci.get("low")),
        return_delta_bootstrap_ci_high=_float_or_none(bootstrap_ci.get("high")),
        skipped_reason=_optional_str(raw.get("skipped_reason")),
        description=str(raw.get("description") or ""),
        material_total_return_delta=material_delta,
    )


def _scenario_items(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_items = payload.get("scenarios", [])
    if not isinstance(raw_items, list):
        return ()
    return tuple(item for item in raw_items if isinstance(item, dict))


def _robustness_evidence(
    payload: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    scenarios = _scenario_items(payload)
    base_dynamic = _dict_value(payload.get("base_dynamic"))
    base_return = _float_or_none(base_dynamic.get("total_return"))
    random_scenarios = [
        scenario
        for scenario in scenarios
        if scenario.get("category") == "same_turnover_random_strategy"
        and _float_or_none(scenario.get("total_return")) is not None
    ]
    random_returns = [
        _float_or_none(scenario.get("total_return"))
        for scenario in random_scenarios
    ]
    random_returns = [value for value in random_returns if value is not None]
    random_beats_count = (
        None
        if base_return is None or not random_returns
        else sum(1 for value in random_returns if value >= base_return)
    )
    random_percentile = (
        None
        if random_beats_count is None or not random_returns
        else (len(random_returns) - random_beats_count) / len(random_returns)
    )
    same_exposure_random_scenarios = [
        scenario
        for scenario in scenarios
        if scenario.get("category") == "same_exposure_random_strategy"
        and _float_or_none(scenario.get("total_return")) is not None
    ]
    same_exposure_random_returns = [
        _float_or_none(scenario.get("total_return"))
        for scenario in same_exposure_random_scenarios
    ]
    same_exposure_random_returns = [
        value for value in same_exposure_random_returns if value is not None
    ]
    same_exposure_beats_count = (
        None
        if base_return is None or not same_exposure_random_returns
        else sum(1 for value in same_exposure_random_returns if value >= base_return)
    )
    same_exposure_percentile = (
        None
        if same_exposure_beats_count is None or not same_exposure_random_returns
        else (
            len(same_exposure_random_returns) - same_exposure_beats_count
        )
        / len(same_exposure_random_returns)
    )

    in_sample = _scenario_by_id(scenarios, "in_sample_window")
    out_sample = _scenario_by_id(scenarios, "out_of_sample_holdout")
    in_sample_return = _float_or_none((in_sample or {}).get("total_return"))
    out_sample_return = _float_or_none((out_sample or {}).get("total_return"))
    oos_degradation = (
        None
        if in_sample_return is None or out_sample_return is None
        else in_sample_return - out_sample_return
    )
    oos_threshold = _float_or_none(
        (materiality_policy or {}).get("oos_material_underperformance_total_return_delta")
    )
    min_oos_return = _float_or_none(
        (materiality_policy or {}).get("candidate_min_oos_total_return")
    )
    oos_blocked = False
    if out_sample_return is not None and min_oos_return is not None:
        oos_blocked = out_sample_return < min_oos_return
    if oos_degradation is not None and oos_threshold is not None:
        oos_blocked = oos_blocked or oos_degradation > oos_threshold

    signal_baselines = [
        scenario
        for scenario in scenarios
        if scenario.get("category") == "signal_family_baseline"
        and _float_or_none(scenario.get("total_return")) is not None
    ]
    best_signal_baseline = max(
        signal_baselines,
        key=lambda scenario: _float_or_none(scenario.get("total_return")) or -1e9,
        default=None,
    )
    best_signal_return = _float_or_none(
        (best_signal_baseline or {}).get("total_return")
    )
    best_signal_delta = (
        None
        if best_signal_return is None or base_return is None
        else best_signal_return - base_return
    )
    architecture_baselines = [
        scenario
        for scenario in scenarios
        if scenario.get("category") == "score_architecture_baseline"
        and _float_or_none(scenario.get("total_return")) is not None
    ]
    best_architecture_baseline = max(
        architecture_baselines,
        key=lambda scenario: _float_or_none(scenario.get("total_return")) or -1e9,
        default=None,
    )
    best_architecture_return = _float_or_none(
        (best_architecture_baseline or {}).get("total_return")
    )
    best_architecture_delta = (
        None
        if best_architecture_return is None or base_return is None
        else best_architecture_return - base_return
    )
    statistical_evidence = _statistical_evidence(
        scenarios,
        materiality_policy,
        base_dynamic=base_dynamic,
        oos_blocked=oos_blocked,
        oos_degradation=oos_degradation,
    )

    remaining_gaps = payload.get("remaining_gaps", [])
    if not isinstance(remaining_gaps, list):
        remaining_gaps = []
    data_quality_status = str(payload.get("data_quality_status") or "UNKNOWN")
    coverage_evidence = _coverage_evidence(payload, materiality_policy)
    sample_independence = _sample_independence_evidence(
        payload,
        materiality_policy,
        coverage_evidence=coverage_evidence,
    )
    return {
        "data_quality": {
            "status": data_quality_status,
            "passed": data_quality_status in {"PASS", "PASS_WITH_WARNINGS"},
            "data_credibility_grade": payload.get("data_credibility_grade"),
            "remaining_gaps": [str(item) for item in remaining_gaps],
        },
        "coverage": coverage_evidence,
        "sample_independence": sample_independence,
        "same_turnover_random_strategy": {
            "available": bool(random_returns),
            "random_path_count": len(random_returns),
            "random_beats_count": random_beats_count,
            "dynamic_strategy_percentile": random_percentile,
            "min_required_percentile": _float_or_none(
                (materiality_policy or {}).get("candidate_random_baseline_min_percentile")
            ),
            "max_random_beats_share": _float_or_none(
                (materiality_policy or {}).get("candidate_max_random_beats_share")
            ),
        },
        "same_exposure_random_strategy": {
            "available": bool(same_exposure_random_returns),
            "random_path_count": len(same_exposure_random_returns),
            "random_beats_count": same_exposure_beats_count,
            "dynamic_strategy_percentile": same_exposure_percentile,
        },
        "out_of_sample_validation": {
            "available": in_sample_return is not None and out_sample_return is not None,
            "in_sample_total_return": in_sample_return,
            "out_of_sample_total_return": out_sample_return,
            "oos_vs_insample_degradation": oos_degradation,
            "blocked": oos_blocked,
            "min_oos_total_return": min_oos_return,
            "material_degradation_threshold": oos_threshold,
        },
        "signal_family_baseline": {
            "available": best_signal_baseline is not None,
            "best_scenario_id": (
                None
                if best_signal_baseline is None
                else best_signal_baseline.get("scenario_id")
            ),
            "best_total_return": best_signal_return,
            "best_delta_vs_base": best_signal_delta,
            "base_beats_best_signal_family_baseline": (
                None if best_signal_delta is None else best_signal_delta <= 0
            ),
        },
        "score_architecture_baseline": {
            "available": best_architecture_baseline is not None,
            "best_scenario_id": (
                None
                if best_architecture_baseline is None
                else best_architecture_baseline.get("scenario_id")
            ),
            "best_total_return": best_architecture_return,
            "best_delta_vs_base": best_architecture_delta,
            "base_beats_best_score_architecture_baseline": (
                None
                if best_architecture_delta is None
                else best_architecture_delta <= 0
            ),
        },
        "statistical": statistical_evidence,
    }


def _coverage_evidence(
    payload: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    raw = _dict_value(payload.get("coverage_evidence"))
    components = raw.get("components")
    available = raw.get("available") is True and isinstance(components, dict) and bool(components)
    min_required = _float_or_none(
        (materiality_policy or {}).get("candidate_min_component_coverage")
    )
    max_placeholder = _float_or_none(
        (materiality_policy or {}).get("candidate_max_placeholder_share")
    )
    blocking_source_types = [
        str(item)
        for item in (materiality_policy or {}).get(
            "candidate_blocking_component_source_types",
            [],
        )
        if str(item).strip()
    ]
    minimum_coverage = _float_or_none(raw.get("minimum_component_coverage"))
    minimum_average_coverage = _float_or_none(
        raw.get("minimum_average_component_coverage")
    )
    maximum_placeholder_share = _float_or_none(raw.get("maximum_placeholder_share"))
    raw_blocking_components = raw.get("blocking_components")
    blocking_components = (
        [str(item) for item in raw_blocking_components]
        if isinstance(raw_blocking_components, list)
        else []
    )
    if not available:
        blocked = True
    else:
        blocked = bool(blocking_components)
        if min_required is not None:
            blocked = blocked or (
                minimum_coverage is not None and minimum_coverage < min_required
            )
            blocked = blocked or (
                minimum_average_coverage is not None
                and minimum_average_coverage < min_required
            )
        if max_placeholder is not None and maximum_placeholder_share is not None:
            blocked = blocked or maximum_placeholder_share > max_placeholder

    return {
        "available": available,
        "sample_count": _int_or_none(raw.get("sample_count")),
        "min_required_component_coverage": min_required,
        "max_allowed_placeholder_share": max_placeholder,
        "blocking_source_types": blocking_source_types,
        "minimum_component_coverage": minimum_coverage,
        "minimum_average_component_coverage": minimum_average_coverage,
        "maximum_placeholder_share": maximum_placeholder_share,
        "blocking_components": blocking_components,
        "blocked": blocked,
    }


def _statistical_evidence(
    scenarios: tuple[dict[str, Any], ...],
    materiality_policy: dict[str, Any] | None,
    *,
    base_dynamic: dict[str, Any],
    oos_blocked: bool,
    oos_degradation: float | None,
) -> dict[str, Any]:
    candidate_categories = {"module_weight_perturbation", "cost", "window"}
    require_bootstrap = bool(
        (materiality_policy or {}).get("candidate_require_bootstrap_ci")
    )
    min_ci_lower = _float_or_none(
        (materiality_policy or {}).get(
            "candidate_min_bootstrap_ci_lower_total_return_delta"
        )
    )
    completed_candidates = [
        scenario
        for scenario in scenarios
        if scenario.get("category") in candidate_categories
        and _float_or_none(scenario.get("total_return_delta_vs_base")) is not None
    ]
    bootstrap_records = [
        _dict_value(scenario.get("return_delta_bootstrap_ci_95"))
        for scenario in completed_candidates
    ]
    available_records = [
        record
        for record in bootstrap_records
        if _float_or_none(record.get("low")) is not None
        and _float_or_none(record.get("high")) is not None
    ]
    positive_candidates = [
        scenario
        for scenario in completed_candidates
        if (_float_or_none(scenario.get("total_return_delta_vs_base")) or 0.0) > 0
    ]
    positive_ci_crosses_threshold = 0
    for scenario in positive_candidates:
        ci = _dict_value(scenario.get("return_delta_bootstrap_ci_95"))
        ci_low = _float_or_none(ci.get("low"))
        if ci_low is not None and min_ci_lower is not None and ci_low < min_ci_lower:
            positive_ci_crosses_threshold += 1
    best_sharpe = max(
        (
            value
            for value in (
                _float_or_none(scenario.get("sharpe"))
                for scenario in completed_candidates
            )
            if value is not None
        ),
        default=None,
    )
    base_sharpe = _float_or_none(base_dynamic.get("sharpe"))
    daily_return_counts = [
        count
        for count in (
            _int_or_none(record.get("daily_return_count"))
            for record in available_records
        )
        if count is not None and count > 0
    ]
    max_daily_return_count = max(daily_return_counts, default=None)
    trial_count = max(len(completed_candidates), 1)
    deflated_sharpe_proxy = None
    deflated_sharpe_penalty = None
    if best_sharpe is not None and trial_count > 1 and max_daily_return_count:
        effective_years = max_daily_return_count / 252.0
        deflated_sharpe_penalty = sqrt(2.0 * log(trial_count)) / sqrt(effective_years)
        deflated_sharpe_proxy = best_sharpe - deflated_sharpe_penalty
    pbo_proxy = None
    if oos_degradation is not None:
        pbo_proxy = 1.0 if oos_blocked else 0.0
    return {
        "available": bool(available_records),
        "require_bootstrap_ci": require_bootstrap,
        "candidate_scenario_count": len(completed_candidates),
        "bootstrap_ci_count": len(available_records),
        "missing_bootstrap_ci_count": (
            len(completed_candidates) - len(available_records)
        ),
        "min_required_ci_lower_total_return_delta": min_ci_lower,
        "positive_candidate_ci_crosses_threshold_count": (
            positive_ci_crosses_threshold
        ),
        "deflated_sharpe_proxy": {
            "available": deflated_sharpe_proxy is not None,
            "method": "trial_count_penalty_proxy_not_formal_deflated_sharpe",
            "base_sharpe": base_sharpe,
            "best_candidate_sharpe": best_sharpe,
            "trial_count": trial_count,
            "daily_return_count": max_daily_return_count,
            "penalty": deflated_sharpe_penalty,
            "value": deflated_sharpe_proxy,
        },
        "pbo_proxy": {
            "available": pbo_proxy is not None,
            "method": "single_holdout_underperformance_proxy_not_cscv_pbo",
            "oos_blocked": oos_blocked,
            "oos_vs_insample_degradation": oos_degradation,
            "value": pbo_proxy,
        },
        "status": (
            "PASS"
            if (
                (not require_bootstrap or len(available_records) == len(completed_candidates))
                and positive_ci_crosses_threshold == 0
            )
            else "PASS_WITH_LIMITATIONS"
        ),
    }


def _sample_independence_evidence(
    payload: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
    *,
    coverage_evidence: dict[str, Any],
) -> dict[str, Any]:
    label_horizon_days = _int_or_none(
        (materiality_policy or {}).get("candidate_label_horizon_days")
    )
    embargo_days = _int_or_none((materiality_policy or {}).get("candidate_embargo_days"))
    min_windows = _int_or_none(
        (materiality_policy or {}).get("candidate_min_independent_windows")
    )
    first_signal_date = _date_or_none(payload.get("first_signal_date"))
    last_signal_date = _date_or_none(payload.get("last_signal_date"))
    signal_count = _int_or_none(coverage_evidence.get("sample_count"))
    effective_window_days = (
        None
        if label_horizon_days is None or embargo_days is None
        else label_horizon_days + embargo_days
    )
    calendar_span_days = (
        None
        if first_signal_date is None or last_signal_date is None
        else (last_signal_date - first_signal_date).days + 1
    )
    independent_windows_by_signal_count = (
        None
        if signal_count is None or effective_window_days in (None, 0)
        else signal_count // effective_window_days
    )
    independent_windows_by_calendar = (
        None
        if calendar_span_days is None or effective_window_days in (None, 0)
        else calendar_span_days // effective_window_days
    )
    estimates = [
        value
        for value in (
            independent_windows_by_signal_count,
            independent_windows_by_calendar,
        )
        if value is not None
    ]
    effective_independent_windows = min(estimates) if estimates else None
    available = effective_independent_windows is not None
    blocked = (
        True
        if not available
        else min_windows is not None and effective_independent_windows < min_windows
    )
    return {
        "available": available,
        "first_signal_date": (
            None if first_signal_date is None else first_signal_date.isoformat()
        ),
        "last_signal_date": (
            None if last_signal_date is None else last_signal_date.isoformat()
        ),
        "signal_count": signal_count,
        "calendar_span_days": calendar_span_days,
        "label_horizon_days": label_horizon_days,
        "embargo_days": embargo_days,
        "effective_window_days": effective_window_days,
        "independent_windows_by_signal_count": independent_windows_by_signal_count,
        "independent_windows_by_calendar": independent_windows_by_calendar,
        "effective_independent_windows": effective_independent_windows,
        "min_required_independent_windows": min_windows,
        "blocked": blocked,
    }


def _scenario_by_id(
    scenarios: tuple[dict[str, Any], ...],
    scenario_id: str,
) -> dict[str, Any] | None:
    for scenario in scenarios:
        if scenario.get("scenario_id") == scenario_id:
            return scenario
    return None


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
    coverage = _dict_value(payload.get("coverage_evidence"))
    if coverage.get("available") is not True:
        warnings.append("来源 robustness summary 缺少 component coverage/source veto 证据。")
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
    candidate_drawdown = _float_or_none(raw_policy.get("candidate_max_drawdown_worsening"))
    candidate_random_percentile = _float_or_none(
        raw_policy.get("candidate_random_baseline_min_percentile")
    )
    candidate_random_beats_share = _float_or_none(
        raw_policy.get("candidate_max_random_beats_share")
    )
    candidate_min_oos = _float_or_none(raw_policy.get("candidate_min_oos_total_return"))
    candidate_blocking_grades = raw_policy.get("candidate_blocking_data_credibility_grades")
    candidate_min_component_coverage = _float_or_none(
        raw_policy.get("candidate_min_component_coverage")
    )
    candidate_max_placeholder_share = _float_or_none(
        raw_policy.get("candidate_max_placeholder_share")
    )
    candidate_blocking_source_types = raw_policy.get(
        "candidate_blocking_component_source_types"
    )
    candidate_require_bootstrap_ci = raw_policy.get("candidate_require_bootstrap_ci")
    candidate_min_bootstrap_ci_lower = _float_or_none(
        raw_policy.get("candidate_min_bootstrap_ci_lower_total_return_delta")
    )
    candidate_label_horizon_days = _int_or_none(
        raw_policy.get("candidate_label_horizon_days")
    )
    candidate_embargo_days = _int_or_none(raw_policy.get("candidate_embargo_days"))
    candidate_min_independent_windows = _int_or_none(
        raw_policy.get("candidate_min_independent_windows")
    )
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
        "candidate_max_drawdown_worsening": candidate_drawdown,
        "candidate_random_baseline_min_percentile": candidate_random_percentile,
        "candidate_max_random_beats_share": candidate_random_beats_share,
        "candidate_min_oos_total_return": candidate_min_oos,
        "candidate_blocking_data_credibility_grades": (
            [str(item) for item in candidate_blocking_grades]
            if isinstance(candidate_blocking_grades, list)
            else []
        ),
        "candidate_min_component_coverage": candidate_min_component_coverage,
        "candidate_max_placeholder_share": candidate_max_placeholder_share,
        "candidate_blocking_component_source_types": (
            [str(item) for item in candidate_blocking_source_types]
            if isinstance(candidate_blocking_source_types, list)
            else []
        ),
        "candidate_require_bootstrap_ci": bool(candidate_require_bootstrap_ci),
        "candidate_min_bootstrap_ci_lower_total_return_delta": (
            candidate_min_bootstrap_ci_lower
        ),
        "candidate_label_horizon_days": candidate_label_horizon_days,
        "candidate_embargo_days": candidate_embargo_days,
        "candidate_min_independent_windows": candidate_min_independent_windows,
    }


def _material_total_return_delta(
    category: str,
    total_return_delta: float | None,
    materiality_policy: dict[str, Any] | None,
) -> bool | None:
    if total_return_delta is None or materiality_policy is None:
        return None
    if category in {
        "baseline",
        "signal_family_baseline",
        "same_turnover_random_strategy",
        "same_exposure_random_strategy",
        "out_of_sample_validation",
        "score_architecture_baseline",
    }:
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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
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
        f"{_bootstrap_ci_label(scenario)} | "
        f"{material} |"
    )


def _robustness_evidence_lines(evidence: dict[str, Any]) -> list[str]:
    data_quality = _dict_value(evidence.get("data_quality"))
    random_evidence = _dict_value(evidence.get("same_turnover_random_strategy"))
    exposure_random_evidence = _dict_value(
        evidence.get("same_exposure_random_strategy")
    )
    oos_evidence = _dict_value(evidence.get("out_of_sample_validation"))
    signal_evidence = _dict_value(evidence.get("signal_family_baseline"))
    architecture_evidence = _dict_value(evidence.get("score_architecture_baseline"))
    statistical_evidence = _dict_value(evidence.get("statistical"))
    deflated_sharpe = _dict_value(statistical_evidence.get("deflated_sharpe_proxy"))
    pbo_proxy = _dict_value(statistical_evidence.get("pbo_proxy"))
    coverage_evidence = _dict_value(evidence.get("coverage"))
    sample_evidence = _dict_value(evidence.get("sample_independence"))
    architecture_delta = _format_pct(
        _float_or_none(architecture_evidence.get("best_delta_vs_base")),
        signed=True,
    )
    return [
        (
            "- 数据质量："
            f"{data_quality.get('status', 'UNKNOWN')}；"
            f"data_credibility_grade={data_quality.get('data_credibility_grade') or 'n/a'}。"
        ),
        (
            "- Coverage veto："
            f"min coverage="
            f"{_format_pct(_float_or_none(coverage_evidence.get('minimum_component_coverage')))}；"
            "max placeholder="
            f"{_format_pct(_float_or_none(coverage_evidence.get('maximum_placeholder_share')))}；"
            f"blocked={coverage_evidence.get('blocked', 'n/a')}。"
        ),
        (
            "- 有效独立样本："
            f"{sample_evidence.get('effective_independent_windows', 'n/a')} 个窗口；"
            f"horizon={sample_evidence.get('label_horizon_days', 'n/a')}D；"
            f"embargo={sample_evidence.get('embargo_days', 'n/a')}D；"
            f"blocked={sample_evidence.get('blocked', 'n/a')}。"
        ),
        (
            "- 同换手率随机策略："
            f"{random_evidence.get('random_beats_count', 'n/a')}/"
            f"{random_evidence.get('random_path_count', 'n/a')} 条随机路径不低于基础策略；"
            "基础策略分位="
            f"{_format_pct(_float_or_none(random_evidence.get('dynamic_strategy_percentile')))}。"
        ),
        (
            "- 同 exposure 分布随机策略："
            f"{exposure_random_evidence.get('random_beats_count', 'n/a')}/"
            f"{exposure_random_evidence.get('random_path_count', 'n/a')} 条随机路径不低于基础策略；"
            "基础策略分位="
            f"{_format_pct(_float_or_none(exposure_random_evidence.get('dynamic_strategy_percentile')))}。"
        ),
        (
            "- OOS holdout："
            f"out-of-sample 总收益="
            f"{_format_pct(_float_or_none(oos_evidence.get('out_of_sample_total_return')))}；"
            "相对 in-sample 退化="
            f"{_format_pct(_float_or_none(oos_evidence.get('oos_vs_insample_degradation')))}。"
        ),
        (
            "- Signal-family baseline："
            f"best={signal_evidence.get('best_scenario_id') or 'n/a'}；"
            "best 相对基础="
            f"{_format_pct(_float_or_none(signal_evidence.get('best_delta_vs_base')), signed=True)}"
            "。"
        ),
        (
            "- Score-architecture baseline："
            f"best={architecture_evidence.get('best_scenario_id') or 'n/a'}；"
            "best 相对基础="
            f"{architecture_delta}"
            "。"
        ),
        (
            "- 统计证据："
            f"bootstrap_ci={statistical_evidence.get('bootstrap_ci_count', 'n/a')}/"
            f"{statistical_evidence.get('candidate_scenario_count', 'n/a')}；"
            f"deflated_sharpe_proxy="
            f"{_format_float(_float_or_none(deflated_sharpe.get('value')))}；"
            f"pbo_proxy={_format_float(_float_or_none(pbo_proxy.get('value')))}；"
            f"status={statistical_evidence.get('status', 'UNKNOWN')}。"
        ),
    ]


def _next_step(report: ParameterReplayReport) -> str:
    if not report.scenarios:
        return "- 下一步：先运行 `aits backtest --robustness-report`，再生成参数 replay 报告。"
    if report.material_delta_count:
        return (
            "- 下一步：对 material delta 场景做多目标候选门禁；若要推进，只能生成 "
            "candidate overlay 或 shadow，不得直接改 production。"
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


def _bootstrap_ci_label(scenario: ParameterReplayScenario) -> str:
    if (
        scenario.return_delta_bootstrap_ci_low is None
        or scenario.return_delta_bootstrap_ci_high is None
    ):
        return "n/a"
    return (
        f"{_format_pct(scenario.return_delta_bootstrap_ci_low, signed=True)} to "
        f"{_format_pct(scenario.return_delta_bootstrap_ci_high, signed=True)}"
    )
