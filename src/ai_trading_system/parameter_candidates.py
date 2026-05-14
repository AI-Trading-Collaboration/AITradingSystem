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
CANDIDATE_EVALUATION_CATEGORIES = frozenset(
    {"module_weight_perturbation", "rebalance_frequency", "cost", "window"}
)


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
    return_delta_bootstrap_ci_low: float | None
    return_delta_bootstrap_ci_high: float | None
    material_total_return_delta: bool | None
    skipped_reason: str | None
    candidate_eligible: bool
    candidate_eligible_reason: str

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
            "return_delta_bootstrap_ci_low": self.return_delta_bootstrap_ci_low,
            "return_delta_bootstrap_ci_high": self.return_delta_bootstrap_ci_high,
            "material_total_return_delta": self.material_total_return_delta,
            "skipped_reason": self.skipped_reason,
            "candidate_eligible": self.candidate_eligible,
            "candidate_eligible_reason": self.candidate_eligible_reason,
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
    data_quality_status: str
    data_credibility_grade: str | None
    coverage_min_component: float | None
    coverage_max_placeholder_share: float | None
    coverage_blocking_components: tuple[str, ...]
    effective_independent_windows: int | None
    oos_total_return: float | None
    oos_vs_insample_degradation: float | None
    random_strategy_percentile: float | None
    random_beats_count: int | None
    return_delta_bootstrap_ci_low: float | None
    return_delta_bootstrap_ci_high: float | None
    signal_family_baseline_beaten: bool | None
    score_architecture_baseline_beaten: bool | None
    veto_reasons: tuple[str, ...]
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
            "data_quality_status": self.data_quality_status,
            "data_credibility_grade": self.data_credibility_grade,
            "coverage_min_component": self.coverage_min_component,
            "coverage_max_placeholder_share": self.coverage_max_placeholder_share,
            "coverage_blocking_components": list(self.coverage_blocking_components),
            "effective_independent_windows": self.effective_independent_windows,
            "oos_total_return": self.oos_total_return,
            "oos_vs_insample_degradation": self.oos_vs_insample_degradation,
            "random_strategy_percentile": self.random_strategy_percentile,
            "random_beats_count": self.random_beats_count,
            "return_delta_bootstrap_ci_low": self.return_delta_bootstrap_ci_low,
            "return_delta_bootstrap_ci_high": self.return_delta_bootstrap_ci_high,
            "signal_family_baseline_beaten": self.signal_family_baseline_beaten,
            "score_architecture_baseline_beaten": (
                self.score_architecture_baseline_beaten
            ),
            "veto_reasons": list(self.veto_reasons),
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
    def ready_for_forward_shadow_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if candidate.recommendation_status == "READY_FOR_FORWARD_SHADOW"
        )

    @property
    def blocked_count(self) -> int:
        return sum(
            1
            for candidate in self.candidates
            if candidate.recommendation_status.startswith("BLOCKED_BY_")
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
            "ready_for_forward_shadow_count": self.ready_for_forward_shadow_count,
            "blocked_count": self.blocked_count,
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
    robustness_evidence = _dict_value(payload.get("robustness_evidence"))
    materiality_policy = _optional_dict(payload.get("materiality_policy"))
    trials = tuple(_trial_from_scenario(item, payload) for item in _scenario_items(payload))
    candidates = tuple(
        _candidate_from_trial(
            trial,
            scenario,
            payload,
            robustness_evidence=robustness_evidence,
            materiality_policy=materiality_policy,
        )
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
        f"- Ready for forward shadow：{ledger.ready_for_forward_shadow_count}",
        f"- Blocked：{ledger.blocked_count}",
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
    category = str(scenario.get("category") or "unknown")
    candidate_eligible = (
        skipped_reason is None
        and total_return_delta is not None
        and category in CANDIDATE_EVALUATION_CATEGORIES
    )
    if skipped_reason is not None:
        candidate_eligible_reason = "skipped_scenario"
    elif total_return_delta is None:
        candidate_eligible_reason = "missing_total_return_delta"
    elif category not in CANDIDATE_EVALUATION_CATEGORIES:
        candidate_eligible_reason = "robustness_evidence_only"
    else:
        candidate_eligible_reason = "eligible_for_multi_objective_gate"
    return ParameterTrial(
        trial_id=trial_id,
        source_scenario_id=scenario_id,
        label=str(scenario.get("label") or scenario_id),
        category=category,
        status=str(scenario.get("status") or "UNKNOWN"),
        total_return_delta_vs_base=total_return_delta,
        max_drawdown_delta_vs_base=_float_or_none(scenario.get("max_drawdown_delta_vs_base")),
        turnover=_float_or_none(scenario.get("turnover")),
        return_delta_bootstrap_ci_low=_float_or_none(
            scenario.get("return_delta_bootstrap_ci_low")
        ),
        return_delta_bootstrap_ci_high=_float_or_none(
            scenario.get("return_delta_bootstrap_ci_high")
        ),
        material_total_return_delta=_bool_or_none(scenario.get("material_total_return_delta")),
        skipped_reason=skipped_reason,
        candidate_eligible=candidate_eligible,
        candidate_eligible_reason=candidate_eligible_reason,
    )


def _candidate_from_trial(
    trial: ParameterTrial,
    scenario: dict[str, Any],
    payload: dict[str, Any],
    *,
    robustness_evidence: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
) -> ParameterCandidate:
    recommendation_status, veto_reasons = _recommendation_status(
        trial,
        robustness_evidence=robustness_evidence,
        materiality_policy=materiality_policy,
    )
    data_quality = _dict_value(robustness_evidence.get("data_quality"))
    random_evidence = _dict_value(
        robustness_evidence.get("same_turnover_random_strategy")
    )
    oos_evidence = _dict_value(robustness_evidence.get("out_of_sample_validation"))
    signal_evidence = _dict_value(robustness_evidence.get("signal_family_baseline"))
    architecture_evidence = _dict_value(
        robustness_evidence.get("score_architecture_baseline")
    )
    sample_evidence = _dict_value(robustness_evidence.get("sample_independence"))
    coverage_evidence = _dict_value(robustness_evidence.get("coverage"))
    raw_blocking_components = coverage_evidence.get("blocking_components")
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
        data_quality_status=str(data_quality.get("status") or "UNKNOWN"),
        data_credibility_grade=_optional_str(data_quality.get("data_credibility_grade")),
        coverage_min_component=_float_or_none(
            coverage_evidence.get("minimum_component_coverage")
        ),
        coverage_max_placeholder_share=_float_or_none(
            coverage_evidence.get("maximum_placeholder_share")
        ),
        coverage_blocking_components=(
            tuple(str(item) for item in raw_blocking_components)
            if isinstance(raw_blocking_components, list)
            else ()
        ),
        effective_independent_windows=_int_or_none(
            sample_evidence.get("effective_independent_windows")
        ),
        oos_total_return=_float_or_none(oos_evidence.get("out_of_sample_total_return")),
        oos_vs_insample_degradation=_float_or_none(
            oos_evidence.get("oos_vs_insample_degradation")
        ),
        random_strategy_percentile=_float_or_none(
            random_evidence.get("dynamic_strategy_percentile")
        ),
        random_beats_count=_int_or_none(random_evidence.get("random_beats_count")),
        return_delta_bootstrap_ci_low=trial.return_delta_bootstrap_ci_low,
        return_delta_bootstrap_ci_high=trial.return_delta_bootstrap_ci_high,
        signal_family_baseline_beaten=_bool_or_none(
            signal_evidence.get("base_beats_best_signal_family_baseline")
        ),
        score_architecture_baseline_beaten=_bool_or_none(
            architecture_evidence.get("base_beats_best_score_architecture_baseline")
        ),
        veto_reasons=veto_reasons,
        recommendation_status=recommendation_status,
        replay_status="COMPLETED_FROM_ROBUSTNESS_SUMMARY",
        shadow_status="NOT_STARTED",
        governance_status="NOT_APPROVED",
        production_effect="none",
        next_step=_candidate_next_step(recommendation_status),
    )


def _recommendation_status(
    trial: ParameterTrial,
    *,
    robustness_evidence: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
) -> tuple[str, tuple[str, ...]]:
    veto_reasons = _candidate_veto_reasons(
        trial,
        robustness_evidence=robustness_evidence,
        materiality_policy=materiality_policy,
    )
    if any(reason.startswith("data_") for reason in veto_reasons):
        return "BLOCKED_BY_DATA", veto_reasons
    if "oos_holdout_failed" in veto_reasons:
        return "BLOCKED_BY_OOS", veto_reasons
    if "random_baseline_failed" in veto_reasons:
        return "BLOCKED_BY_RANDOM_BASELINE", veto_reasons
    if "drawdown_worsened" in veto_reasons:
        return "RISK_REVIEW", veto_reasons
    if "signal_family_baseline_not_beaten" in veto_reasons:
        return "RISK_REVIEW", veto_reasons
    if "score_architecture_baseline_not_beaten" in veto_reasons:
        return "RISK_REVIEW", veto_reasons
    if "sample_independence_insufficient" in veto_reasons:
        return "READY_FOR_AS_IF_REPLAY", veto_reasons
    if "statistical_bootstrap_ci_crosses_threshold" in veto_reasons:
        return "READY_FOR_AS_IF_REPLAY", veto_reasons
    if trial.material_total_return_delta is True:
        if trial.total_return_delta_vs_base is not None and trial.total_return_delta_vs_base > 0:
            if any(reason.endswith("_missing") for reason in veto_reasons):
                return "READY_FOR_AS_IF_REPLAY", veto_reasons
            return "READY_FOR_FORWARD_SHADOW", veto_reasons
        return "MATERIAL_RISK_REVIEW", veto_reasons
    if trial.material_total_return_delta is False:
        return "OBSERVE_ONLY", veto_reasons
    return "NEEDS_MATERIALITY_POLICY", veto_reasons


def _candidate_veto_reasons(
    trial: ParameterTrial,
    *,
    robustness_evidence: dict[str, Any],
    materiality_policy: dict[str, Any] | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    data_quality = _dict_value(robustness_evidence.get("data_quality"))
    if data_quality.get("passed") is False:
        reasons.append("data_quality_failed")
    blocking_grades = {
        str(item)
        for item in (materiality_policy or {}).get(
            "candidate_blocking_data_credibility_grades",
            [],
        )
        if str(item).strip()
    }
    data_grade = _optional_str(data_quality.get("data_credibility_grade"))
    if data_grade and data_grade in blocking_grades:
        reasons.append("data_credibility_blocked")
    coverage_evidence = _dict_value(robustness_evidence.get("coverage"))
    if coverage_evidence.get("available") is not True:
        reasons.append("data_coverage_missing")
    elif coverage_evidence.get("blocked") is True:
        blocking_components = coverage_evidence.get("blocking_components")
        if isinstance(blocking_components, list) and blocking_components:
            reasons.append("data_component_coverage_blocked")
        min_required_coverage = _float_or_none(
            (materiality_policy or {}).get("candidate_min_component_coverage")
        )
        min_coverage = _float_or_none(
            coverage_evidence.get("minimum_component_coverage")
        )
        min_avg_coverage = _float_or_none(
            coverage_evidence.get("minimum_average_component_coverage")
        )
        if min_required_coverage is not None and (
            (min_coverage is not None and min_coverage < min_required_coverage)
            or (
                min_avg_coverage is not None
                and min_avg_coverage < min_required_coverage
            )
        ):
            reasons.append("data_coverage_below_threshold")
        max_placeholder_share = _float_or_none(
            (materiality_policy or {}).get("candidate_max_placeholder_share")
        )
        actual_placeholder_share = _float_or_none(
            coverage_evidence.get("maximum_placeholder_share")
        )
        if (
            max_placeholder_share is not None
            and actual_placeholder_share is not None
            and actual_placeholder_share > max_placeholder_share
        ):
            reasons.append("data_placeholder_share_exceeded")
    sample_evidence = _dict_value(robustness_evidence.get("sample_independence"))
    if sample_evidence.get("available") is not True:
        reasons.append("sample_independence_missing")
    elif sample_evidence.get("blocked") is True:
        reasons.append("sample_independence_insufficient")

    max_drawdown_worsening = _float_or_none(
        (materiality_policy or {}).get("candidate_max_drawdown_worsening")
    )
    if (
        trial.max_drawdown_delta_vs_base is not None
        and max_drawdown_worsening is not None
        and trial.max_drawdown_delta_vs_base < -max_drawdown_worsening
    ):
        reasons.append("drawdown_worsened")

    oos_evidence = _dict_value(robustness_evidence.get("out_of_sample_validation"))
    if oos_evidence.get("available") is True:
        if oos_evidence.get("blocked") is True:
            reasons.append("oos_holdout_failed")
    else:
        reasons.append("oos_evidence_missing")

    random_evidence = _dict_value(
        robustness_evidence.get("same_turnover_random_strategy")
    )
    if random_evidence.get("available") is True:
        percentile = _float_or_none(random_evidence.get("dynamic_strategy_percentile"))
        min_percentile = _float_or_none(random_evidence.get("min_required_percentile"))
        beats_count = _int_or_none(random_evidence.get("random_beats_count"))
        path_count = _int_or_none(random_evidence.get("random_path_count"))
        max_beats_share = _float_or_none(random_evidence.get("max_random_beats_share"))
        beats_share = (
            None
            if beats_count is None or path_count in (None, 0)
            else beats_count / path_count
        )
        if (
            percentile is not None
            and min_percentile is not None
            and percentile < min_percentile
        ) or (
            beats_share is not None
            and max_beats_share is not None
            and beats_share > max_beats_share
        ):
            reasons.append("random_baseline_failed")
    else:
        reasons.append("random_baseline_missing")

    signal_evidence = _dict_value(robustness_evidence.get("signal_family_baseline"))
    if signal_evidence.get("available") is True:
        if signal_evidence.get("base_beats_best_signal_family_baseline") is False:
            reasons.append("signal_family_baseline_not_beaten")
    else:
        reasons.append("signal_family_baseline_missing")
    architecture_evidence = _dict_value(
        robustness_evidence.get("score_architecture_baseline")
    )
    if architecture_evidence.get("available") is True:
        if (
            architecture_evidence.get(
                "base_beats_best_score_architecture_baseline"
            )
            is False
        ):
            reasons.append("score_architecture_baseline_not_beaten")
    else:
        reasons.append("score_architecture_baseline_missing")
    if trial.material_total_return_delta is True and (
        trial.total_return_delta_vs_base is not None
        and trial.total_return_delta_vs_base > 0
    ):
        require_bootstrap = bool(
            (materiality_policy or {}).get("candidate_require_bootstrap_ci")
        )
        min_ci_lower = _float_or_none(
            (materiality_policy or {}).get(
                "candidate_min_bootstrap_ci_lower_total_return_delta"
            )
        )
        if (
            trial.return_delta_bootstrap_ci_low is None
            or trial.return_delta_bootstrap_ci_high is None
        ):
            if require_bootstrap:
                reasons.append("statistical_evidence_missing")
        elif (
            min_ci_lower is not None
            and trial.return_delta_bootstrap_ci_low < min_ci_lower
        ):
            reasons.append("statistical_bootstrap_ci_crosses_threshold")
    return tuple(dict.fromkeys(reasons))


def _candidate_next_step(status: str) -> str:
    if status == "READY_FOR_FORWARD_SHADOW":
        return "补 owner 假设卡和 rollback 条件后进入 forward shadow，不得直接改 production。"
    if status == "READY_FOR_AS_IF_REPLAY":
        return "补齐缺失 OOS/random/baseline 证据后再判断能否进入 forward shadow。"
    if status == "BLOCKED_BY_DATA":
        return "先修复数据质量、PIT 覆盖或数据可信度，再讨论参数候选。"
    if status == "BLOCKED_BY_OOS":
        return "样本外证据阻断；记录为过拟合风险，不进入 shadow。"
    if status == "BLOCKED_BY_RANDOM_BASELINE":
        return "同换手率随机基线阻断；需要更长样本或重新提出业务假设。"
    if status == "RISK_REVIEW":
        return "收益改善伴随风险或基线问题，进入风险复核而不是晋级。"
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
    if any(candidate.recommendation_status.startswith("BLOCKED_BY_") for candidate in candidates):
        warnings.append("存在候选被数据、OOS 或随机基线 veto，不能进入 forward shadow。")
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
    next_step = candidate.next_step
    if candidate.veto_reasons:
        next_step += " Veto: " + "、".join(candidate.veto_reasons)
    return (
        f"| `{candidate.candidate_id}` | `{candidate.source_scenario_id}` | "
        f"{candidate.category} | "
        f"{_format_pct(candidate.total_return_delta_vs_base, signed=True)} | "
        f"{_format_pct(candidate.max_drawdown_delta_vs_base, signed=True)} | "
        f"{_format_float(candidate.turnover)} | "
        f"{candidate.recommendation_status} | {next_step} |"
    )


def _trial_row(trial: ParameterTrial) -> str:
    return (
        f"| `{trial.trial_id}` | `{trial.source_scenario_id}` | {trial.category} | "
        f"{trial.status} | {_format_pct(trial.total_return_delta_vs_base, signed=True)} | "
        f"{trial.candidate_eligible} ({trial.candidate_eligible_reason}) |"
    )


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON ledger must contain an object: {path}")
    return payload


def _optional_dict(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
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
