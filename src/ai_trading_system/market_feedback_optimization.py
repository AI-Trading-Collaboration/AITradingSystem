from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_causal_chains import DEFAULT_DECISION_CAUSAL_CHAIN_PATH
from ai_trading_system.decision_learning_queue import DEFAULT_DECISION_LEARNING_QUEUE_PATH
from ai_trading_system.decision_outcomes import DEFAULT_DECISION_OUTCOMES_PATH
from ai_trading_system.feedback_sample_policy import (
    DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
    FeedbackSamplePolicy,
    OutcomeSampleFloors,
    load_feedback_sample_policy,
    sample_floor_summary,
)
from ai_trading_system.parameter_candidates import (
    DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
)
from ai_trading_system.parameter_replay import default_parameter_replay_summary_path
from ai_trading_system.prediction_ledger import DEFAULT_PREDICTION_OUTCOMES_PATH
from ai_trading_system.rule_experiments import DEFAULT_RULE_EXPERIMENT_LEDGER_PATH
from ai_trading_system.weight_calibration import (
    DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    DEFAULT_EFFECTIVE_WEIGHTS_PATH,
)

DEFAULT_MARKET_FEEDBACK_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_MARKET_FEEDBACK_REPLAY_START = date(2022, 12, 1)


@dataclass(frozen=True)
class MarketFeedbackOptimizationReport:
    as_of: date
    since: date
    replay_start: date
    replay_end: date
    generated_at: datetime
    market_regime_id: str
    data_quality: dict[str, Any]
    decision_outcomes: dict[str, Any]
    prediction_outcomes: dict[str, Any]
    causal_chains: dict[str, Any]
    learning_queue: dict[str, Any]
    rule_experiments: dict[str, Any]
    parameter_replay: dict[str, Any]
    parameter_candidates: dict[str, Any]
    shadow_maturity: dict[str, Any]
    calibration_overlay: dict[str, Any]
    effective_weights: dict[str, Any]
    sample_policy_path: Path
    sample_policy: FeedbackSamplePolicy

    @property
    def warning_count(self) -> int:
        sections = (
            self.data_quality,
            self.decision_outcomes,
            self.prediction_outcomes,
            self.causal_chains,
            self.learning_queue,
            self.rule_experiments,
            self.parameter_replay,
            self.parameter_candidates,
            self.shadow_maturity,
            self.calibration_overlay,
            self.effective_weights,
        )
        return sum(len(section.get("warnings", [])) for section in sections)

    @property
    def status(self) -> str:
        if self.warning_count:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def readiness(self) -> str:
        decision_available = int(self.decision_outcomes.get("available_count", 0))
        prediction_available = int(self.prediction_outcomes.get("available_count", 0))
        pending_replay = int(self.rule_experiments.get("pending_replay_count", 0))
        pending_shadow = int(self.rule_experiments.get("pending_shadow_count", 0))
        approved_overlay_count = int(self.calibration_overlay.get("approved_count", 0))
        if (
            decision_available < self.sample_policy.decision_outcomes.reporting_floor
            or prediction_available < self.sample_policy.prediction_outcomes.reporting_floor
        ):
            return "INSUFFICIENT_REPORTING_SAMPLE"
        if decision_available < self.sample_policy.decision_outcomes.pilot_floor:
            return "INSUFFICIENT_DECISION_PILOT_SAMPLE"
        if prediction_available < self.sample_policy.prediction_outcomes.pilot_floor:
            return "INSUFFICIENT_FORWARD_SHADOW_PILOT_SAMPLE"
        if pending_replay or pending_shadow:
            return "READY_FOR_REPLAY_OR_SHADOW_REVIEW"
        if approved_overlay_count:
            return "READY_FOR_APPROVED_OVERLAY_AUDIT"
        if (
            decision_available < self.sample_policy.decision_outcomes.diagnostic_floor
            or prediction_available < self.sample_policy.prediction_outcomes.diagnostic_floor
        ):
            return "PILOT_DIAGNOSTIC_REVIEW"
        return "READY_FOR_WEIGHT_DIAGNOSTIC_REVIEW"


def default_market_feedback_optimization_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"market_feedback_optimization_{as_of.isoformat()}.md"


def build_market_feedback_optimization_report(
    *,
    as_of: date,
    since: date | None = None,
    replay_start: date = DEFAULT_MARKET_FEEDBACK_REPLAY_START,
    replay_end: date | None = None,
    market_regime_id: str = "ai_after_chatgpt",
    data_quality_report_path: Path | None = None,
    decision_outcomes_path: Path = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Path = DEFAULT_PREDICTION_OUTCOMES_PATH,
    causal_chain_path: Path = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Path = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Path = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    parameter_replay_summary_path: Path | None = None,
    parameter_candidate_ledger_path: Path = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    shadow_maturity_report_path: Path | None = None,
    calibration_overlay_path: Path = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    effective_weights_path: Path = DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    sample_policy_path: Path = DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
) -> MarketFeedbackOptimizationReport:
    since = since or as_of - timedelta(days=7)
    replay_end = replay_end or as_of
    sample_policy = load_feedback_sample_policy(sample_policy_path)
    data_quality_report_path = data_quality_report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"data_quality_{as_of.isoformat()}.md"
    )
    shadow_maturity_report_path = shadow_maturity_report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"shadow_maturity_{as_of.isoformat()}.md"
    )
    parameter_replay_summary_path = parameter_replay_summary_path or (
        default_parameter_replay_summary_path(
            PROJECT_ROOT / "outputs" / "reports",
            as_of,
        )
    )
    return MarketFeedbackOptimizationReport(
        as_of=as_of,
        since=since,
        replay_start=replay_start,
        replay_end=replay_end,
        generated_at=datetime.now(tz=UTC),
        market_regime_id=market_regime_id,
        data_quality=_markdown_artifact_section(
            data_quality_report_path,
            "data_quality_report",
        ),
        decision_outcomes=_decision_outcome_section(
            decision_outcomes_path,
            as_of=as_of,
            sample_floors=sample_policy.decision_outcomes,
        ),
        prediction_outcomes=_prediction_outcome_section(
            prediction_outcomes_path,
            as_of=as_of,
            sample_floors=sample_policy.prediction_outcomes,
        ),
        causal_chains=_json_collection_section(causal_chain_path, "chains"),
        learning_queue=_learning_queue_section(learning_queue_path),
        rule_experiments=_rule_experiment_section(rule_experiment_path),
        parameter_replay=_parameter_replay_section(parameter_replay_summary_path),
        parameter_candidates=_parameter_candidate_section(parameter_candidate_ledger_path),
        shadow_maturity=_markdown_artifact_section(
            shadow_maturity_report_path,
            "shadow_maturity_report",
        ),
        calibration_overlay=_calibration_overlay_section(calibration_overlay_path),
        effective_weights=_effective_weights_section(effective_weights_path),
        sample_policy_path=sample_policy_path,
        sample_policy=sample_policy,
    )


def render_market_feedback_optimization_report(
    report: MarketFeedbackOptimizationReport,
) -> str:
    lines = [
        "# 市场反馈优化闭环报告",
        "",
        f"- 状态：{report.status}",
        f"- Readiness：{report.readiness}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 复核日期：{report.as_of.isoformat()}",
        f"- 复核窗口：{report.since.isoformat()} 至 {report.as_of.isoformat()}",
        f"- as-if 回放窗口：{report.replay_start.isoformat()} 至 {report.replay_end.isoformat()}",
        f"- 市场阶段：{report.market_regime_id}",
        f"- 样本政策：{report.sample_policy.version}（status={report.sample_policy.status}，"
        f"review_after_reports={report.sample_policy.review_after_reports}）",
        f"- 样本政策配置：`{report.sample_policy_path}`",
        "- 生产影响：none。本报告不改 `score-daily`、`position_gate`、thesis、"
        "日报结论、回测仓位或正式规则。",
        "",
        "## 样本政策",
        "",
        "| Outcome | Reporting floor | Pilot floor | Diagnostic floor | Promotion floor |",
        "|---|---:|---:|---:|---:|",
        _sample_policy_row("Decision", report.sample_policy.decision_outcomes),
        _sample_policy_row("Prediction / shadow", report.sample_policy.prediction_outcomes),
        "",
        "- Reporting floor 只允许展示覆盖状态；pilot floor 允许启动因果链、学习队列、"
        "候选规则整理和 pilot 复盘；diagnostic floor 才允许输出权重诊断；promotion floor "
        "仍需 replay、forward shadow、owner approval 和回滚条件。",
        "",
        "## 执行频次",
        "",
        "| 频次 | 命令 | 作用 |",
        "|---|---|---|",
        "| 每周 | `aits feedback optimize-market-feedback --as-of YYYY-MM-DD` | "
        "汇总 outcome、learning queue、rule experiments、shadow maturity 和 overlay 状态，"
        "判断是否可进入复核或继续等待样本。 |",
        "| 每周上游 | `aits feedback calibrate`、`aits feedback calibrate-predictions`、"
        "`aits feedback build-causal-chain`、`aits feedback build-learning-queue`、"
        "`aits feedback build-rule-experiments` | "
        "先生成本报告依赖的后验观察、因果链和候选规则台账。 |",
        "| 每月 | 使用 `--replay-start 2022-12-01 --replay-end YYYY-MM-DD` 固定 AI regime "
        "窗口复核 | 检查权重/规则候选是否有足够 as-if 证据、是否需要 policy replay 或 "
        "owner governance。 |",
        "",
        "## 关键产物状态",
        "",
        "| 产物 | 路径 | 状态 | 样本/数量 | 警告 |",
        "|---|---|---|---:|---|",
        _artifact_row("数据质量报告", report.data_quality, "row_count"),
        _artifact_row("Decision outcomes", report.decision_outcomes, "total_count"),
        _artifact_row("Prediction outcomes", report.prediction_outcomes, "total_count"),
        _artifact_row("Decision causal chains", report.causal_chains, "total_count"),
        _artifact_row("Learning queue", report.learning_queue, "total_count"),
        _artifact_row("Rule experiments", report.rule_experiments, "total_count"),
        _artifact_row("Parameter replay", report.parameter_replay, "scenario_count"),
        _artifact_row(
            "Parameter candidates",
            report.parameter_candidates,
            "candidate_count",
        ),
        _artifact_row("Shadow maturity", report.shadow_maturity, "row_count"),
        _artifact_row("Approved overlay", report.calibration_overlay, "total_count"),
        _artifact_row("Effective weights", report.effective_weights, "matched_count"),
        "",
        "## Outcome 可校准性",
        "",
        f"- Decision outcome 可用样本：{report.decision_outcomes['available_count']} / "
        f"{_floor_summary(report.sample_policy.decision_outcomes)}",
        f"- Decision outcome 最新 signal_date：{report.decision_outcomes['latest_signal_date']}",
        f"- Prediction/shadow outcome 可用样本："
        f"{report.prediction_outcomes['available_count']} / "
        f"{_floor_summary(report.sample_policy.prediction_outcomes)}",
        f"- Prediction outcome 最新 decision_date："
        f"{report.prediction_outcomes['latest_decision_date']}",
        f"- Decision outcome horizon 覆盖：{_format_counts(report.decision_outcomes['horizons'])}",
        f"- Prediction candidate 覆盖：{report.prediction_outcomes['candidate_count']}",
        f"- 当前结论：{_readiness_explanation(report)}",
        "",
        "## 错误复盘与候选规则",
        "",
        f"- 学习队列分类：{_format_counts(report.learning_queue['category_counts'])}",
        f"- 需要候选规则复核：{report.learning_queue['rule_candidate_required_count']}",
        f"- 候选规则数：{report.rule_experiments['total_count']}",
        f"- 未运行 replay：{report.rule_experiments['pending_replay_count']}",
        f"- 待 forward shadow：{report.rule_experiments['pending_shadow_count']}",
        f"- 参数复测场景：{report.parameter_replay['scenario_count']}；"
        f"material delta：{report.parameter_replay['material_delta_count']}",
        f"- 参数候选数：{report.parameter_candidates['candidate_count']}；"
        f"trial：{report.parameter_candidates['trial_count']}；"
        f"owner review：{report.parameter_candidates['ready_for_owner_review_count']}；"
        f"risk review：{report.parameter_candidates['material_risk_review_count']}",
        "",
        "## Overlay 与生产兼容性",
        "",
        f"- Approved overlay 数：{report.calibration_overlay['approved_count']}",
        f"- Candidate / shadow-only overlay 数：{report.calibration_overlay['candidate_count']}",
        f"- 当前 effective weight 命中数：{report.effective_weights['matched_count']}",
        "- 兼容规则：候选、shadow-only、过期或未批准 overlay 不得进入 production；"
        "`approved_soft` 也只能通过既有 `apply-calibration-overlay` 和后续治理接入，"
        "不能由本报告直接改变日报仓位。",
        "",
        "## 与既有计划流程的兼容性",
        "",
        "- `aits ops daily-run` 继续负责生产日报、decision snapshot、prediction ledger、"
        "data quality 和 trace bundle；本流程只消费这些产物。",
        "- 周期性复核放在日报之后，不作为 daily-run 阻断步骤；缺少样本时输出 "
        "`PASS_WITH_LIMITATIONS` 和明确下一步。",
        "- as-if 回放默认使用 `ai_after_chatgpt` 窗口，起点为 2022-12-01；若使用更早历史，"
        "报告必须写明非默认市场阶段和原因。",
        "- 任何权重、规则或 gate 的生产变更必须先经过 replay、forward shadow、"
        "rule card / overlay approval 和回滚条件登记。",
        "",
        "## 下一步",
        "",
        _next_action_line(report),
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_market_feedback_optimization_report(
    report: MarketFeedbackOptimizationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_market_feedback_optimization_report(report),
        encoding="utf-8",
    )
    return output_path


def _markdown_artifact_section(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        return _missing_section(path, f"{label} 不存在")
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "row_count": line_count,
        "warnings": [],
    }


def _decision_outcome_section(
    path: Path,
    *,
    as_of: date,
    sample_floors: OutcomeSampleFloors,
) -> dict[str, Any]:
    if not path.exists():
        return _missing_csv_section(path, "decision outcomes 不存在", sample_floors)
    frame = pd.read_csv(path)
    status = _string_series(frame, "outcome_status")
    available_count = int((status == "AVAILABLE").sum())
    warnings = []
    latest_signal_date = _latest_date(frame, "signal_date")
    if latest_signal_date is None:
        warnings.append("decision outcomes 缺少 signal_date，无法判断是否覆盖复核日期。")
    elif latest_signal_date < as_of:
        warnings.append(
            "decision outcomes 最新 signal_date "
            f"{latest_signal_date.isoformat()} 早于复核日期 {as_of.isoformat()}，"
            "应先重跑 `aits feedback calibrate`。"
        )
    warnings.extend(_sample_floor_warnings("decision outcome", available_count, sample_floors))
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "total_count": len(frame),
        "available_count": available_count,
        "pending_count": int((status == "PENDING").sum()),
        "missing_count": int((status == "MISSING_DATA").sum()),
        "latest_signal_date": latest_signal_date.isoformat() if latest_signal_date else "UNKNOWN",
        "horizons": _horizon_counts(frame, status),
        "warnings": warnings,
    }


def _prediction_outcome_section(
    path: Path,
    *,
    as_of: date,
    sample_floors: OutcomeSampleFloors,
) -> dict[str, Any]:
    if not path.exists():
        return _missing_prediction_section(path, "prediction outcomes 不存在", sample_floors)
    frame = pd.read_csv(path)
    status = _string_series(frame, "outcome_status")
    candidate = _string_series(frame, "candidate_id")
    production_effect = _string_series(frame, "production_effect")
    available_count = int((status == "AVAILABLE").sum())
    challenger_mask = (candidate != "production") | (production_effect == "none")
    warnings = []
    latest_decision_date = _latest_date(frame, "decision_date")
    if latest_decision_date is None:
        warnings.append("prediction outcomes 缺少 decision_date，无法判断是否覆盖复核日期。")
    elif latest_decision_date < as_of:
        warnings.append(
            "prediction outcomes 最新 decision_date "
            f"{latest_decision_date.isoformat()} 早于复核日期 {as_of.isoformat()}，"
            "应先重跑 `aits feedback calibrate-predictions`。"
        )
    warnings.extend(
        _sample_floor_warnings("prediction/shadow outcome", available_count, sample_floors)
    )
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "total_count": len(frame),
        "available_count": available_count,
        "pending_count": int((status == "PENDING").sum()),
        "missing_count": int((status == "MISSING_DATA").sum()),
        "latest_decision_date": (
            latest_decision_date.isoformat() if latest_decision_date else "UNKNOWN"
        ),
        "candidate_count": int(candidate.nunique()) if not candidate.empty else 0,
        "challenger_count": int(candidate.loc[challenger_mask].nunique()),
        "warnings": warnings,
    }


def _json_collection_section(path: Path, key: str) -> dict[str, Any]:
    if not path.exists():
        return _missing_section(path, f"JSON ledger 不存在：{key}")
    ledger = _read_json(path)
    items = ledger.get(key, [])
    if not isinstance(items, list):
        return {
            "path": str(path),
            "exists": True,
            "status": "INVALID",
            "total_count": 0,
            "warnings": [f"JSON ledger 字段不是 list：{key}"],
        }
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "total_count": len(items),
        "warnings": [],
    }


def _learning_queue_section(path: Path) -> dict[str, Any]:
    base = _json_collection_section(path, "items")
    if not base.get("exists"):
        base["category_counts"] = {}
        base["rule_candidate_required_count"] = 0
        return base
    ledger = _read_json(path)
    raw_items = ledger.get("items", [])
    items = raw_items if isinstance(raw_items, list) else []
    category_counts: dict[str, int] = {}
    required_count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        category = str(item.get("attribution_category", "")).strip()
        if category:
            category_counts[category] = category_counts.get(category, 0) + 1
        if item.get("rule_candidate_required") is True:
            required_count += 1
    base["category_counts"] = dict(sorted(category_counts.items()))
    base["rule_candidate_required_count"] = required_count
    return base


def _rule_experiment_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "status": "NOT_CONNECTED",
            "total_count": 0,
            "pending_replay_count": 0,
            "pending_shadow_count": 0,
            "warnings": ["rule experiments ledger 不存在，候选规则仍停留在学习队列。"],
        }
    ledger = _read_json(path)
    candidates = ledger.get("candidates", [])
    if not isinstance(candidates, list):
        candidates = []
    pending_replay = sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict)
        and (candidate.get("replay_plan") or {}).get("status") == "NOT_RUN"
    )
    pending_shadow = sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict)
        and (candidate.get("forward_shadow_plan") or {}).get("status") == "PENDING"
    )
    warnings = []
    if pending_replay or pending_shadow:
        warnings.append("存在候选规则尚未完成 replay 或 forward shadow。")
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "total_count": len(candidates),
        "pending_replay_count": pending_replay,
        "pending_shadow_count": pending_shadow,
        "warnings": warnings,
    }


def _parameter_replay_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "status": "NOT_CONNECTED",
            "scenario_count": 0,
            "completed_scenario_count": 0,
            "material_delta_count": 0,
            "warnings": ["parameter replay 摘要不存在；尚未把参数复测收益变化接入 feedback 闭环。"],
        }
    payload = _read_json(path)
    scenarios = payload.get("scenarios", [])
    if not isinstance(scenarios, list):
        scenarios = []
    raw_warnings = payload.get("warnings", [])
    if isinstance(raw_warnings, list):
        warnings = [str(item) for item in raw_warnings if str(item).strip()]
    elif isinstance(raw_warnings, str) and raw_warnings.strip():
        warnings = [raw_warnings]
    else:
        warnings = []
    return {
        "path": str(path),
        "exists": True,
        "status": str(payload.get("status") or "CONNECTED"),
        "scenario_count": int(payload.get("scenario_count") or len(scenarios)),
        "completed_scenario_count": int(
            payload.get("completed_scenario_count")
            or sum(
                1
                for scenario in scenarios
                if isinstance(scenario, dict) and scenario.get("total_return") is not None
            )
        ),
        "material_delta_count": int(payload.get("material_delta_count") or 0),
        "warnings": warnings,
    }


def _parameter_candidate_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "status": "NOT_CONNECTED",
            "trial_count": 0,
            "candidate_count": 0,
            "ready_for_owner_review_count": 0,
            "material_risk_review_count": 0,
            "needs_policy_count": 0,
            "warnings": ["parameter candidates ledger 不存在；参数复测结果尚未进入候选台账。"],
        }
    payload = _read_json(path)
    warnings = _warnings_from_payload(payload)
    return {
        "path": str(path),
        "exists": True,
        "status": str(payload.get("status") or "CONNECTED"),
        "trial_count": int(payload.get("trial_count") or 0),
        "candidate_count": int(payload.get("candidate_count") or 0),
        "ready_for_owner_review_count": int(payload.get("ready_for_owner_review_count") or 0),
        "material_risk_review_count": int(payload.get("material_risk_review_count") or 0),
        "needs_policy_count": int(payload.get("needs_policy_count") or 0),
        "warnings": warnings,
    }


def _calibration_overlay_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "status": "NOT_CONNECTED",
            "total_count": 0,
            "approved_count": 0,
            "candidate_count": 0,
            "warnings": ["approved calibration overlay 不存在；当前没有已批准历史调权。"],
        }
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_overlays = payload.get("overlays", [])
    elif isinstance(payload, list):
        raw_overlays = payload
    else:
        raw_overlays = []
    overlays = raw_overlays if isinstance(raw_overlays, list) else []
    approved = 0
    candidate = 0
    for item in overlays:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", ""))
        if status in {"approved_soft", "approved_hard"}:
            approved += 1
        elif status:
            candidate += 1
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "total_count": len(overlays),
        "approved_count": approved,
        "candidate_count": candidate,
        "warnings": [],
    }


def _effective_weights_section(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "status": "NOT_CONNECTED",
            "matched_count": 0,
            "warnings": ["current effective weights 不存在；尚未生成 overlay 审计输出。"],
        }
    payload = _read_json(path)
    matched = payload.get("matched_overlays", [])
    matched_count = len(matched) if isinstance(matched, list) else 0
    return {
        "path": str(path),
        "exists": True,
        "status": "CONNECTED",
        "matched_count": matched_count,
        "warnings": [],
    }


def _missing_section(path: Path, warning: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": False,
        "status": "MISSING",
        "total_count": 0,
        "row_count": 0,
        "warnings": [f"{warning}：{path}"],
    }


def _missing_csv_section(
    path: Path,
    warning: str,
    sample_floors: OutcomeSampleFloors,
) -> dict[str, Any]:
    section = _missing_section(path, warning)
    section.update(
        {
            "available_count": 0,
            "pending_count": 0,
            "missing_count": 0,
            "latest_signal_date": "UNKNOWN",
            "horizons": {},
            "warnings": section["warnings"]
            + [
                "可用样本为 0，低于 reporting floor "
                f"{sample_floors.reporting_floor}，不能启动 pilot 复盘。"
            ],
        }
    )
    return section


def _missing_prediction_section(
    path: Path,
    warning: str,
    sample_floors: OutcomeSampleFloors,
) -> dict[str, Any]:
    section = _missing_csv_section(path, warning, sample_floors)
    section.update(
        {
            "candidate_count": 0,
            "challenger_count": 0,
            "latest_decision_date": "UNKNOWN",
        }
    )
    return section


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {"items": payload}


def _warnings_from_payload(payload: dict[str, Any]) -> list[str]:
    raw_warnings = payload.get("warnings", [])
    if isinstance(raw_warnings, list):
        return [str(item) for item in raw_warnings if str(item).strip()]
    if isinstance(raw_warnings, str) and raw_warnings.strip():
        return [raw_warnings]
    return []


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(dtype="string")
    return frame[column].fillna("").astype("string")


def _horizon_counts(frame: pd.DataFrame, status: pd.Series) -> dict[str, int]:
    if "horizon_days" not in frame:
        return {}
    horizons: dict[str, int] = {}
    for horizon, group in frame.groupby("horizon_days"):
        horizon_status = status.loc[group.index]
        horizons[str(horizon)] = int((horizon_status == "AVAILABLE").sum())
    return dict(sorted(horizons.items(), key=lambda item: item[0]))


def _latest_date(frame: pd.DataFrame, column: str) -> date | None:
    if column not in frame:
        return None
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    if dates.empty:
        return None
    latest = dates.max()
    return latest.date()


def _artifact_row(label: str, section: dict[str, Any], count_key: str) -> str:
    warnings = section.get("warnings", [])
    warning_text = "无" if not warnings else "<br/>".join(str(item) for item in warnings)
    return (
        f"| {label} | `{section.get('path', '')}` | {section.get('status', 'UNKNOWN')} | "
        f"{section.get(count_key, 0)} | {warning_text} |"
    )


def _sample_policy_row(label: str, floors: OutcomeSampleFloors) -> str:
    return (
        f"| {label} | {floors.reporting_floor} | {floors.pilot_floor} | "
        f"{floors.diagnostic_floor} | {floors.promotion_floor} |"
    )


def _floor_summary(floors: OutcomeSampleFloors) -> str:
    return sample_floor_summary(floors)


def _sample_floor_warnings(
    label: str,
    available_count: int,
    floors: OutcomeSampleFloors,
) -> list[str]:
    if available_count < floors.reporting_floor:
        return [
            f"{label} 可用样本 {available_count} 低于 reporting floor "
            f"{floors.reporting_floor}，只能展示缺口，不能启动 pilot 复盘。"
        ]
    if available_count < floors.pilot_floor:
        return [
            f"{label} 可用样本 {available_count} 低于 pilot floor "
            f"{floors.pilot_floor}，继续收集样本，暂不进入后续流程。"
        ]
    warnings = []
    if available_count < floors.diagnostic_floor:
        warnings.append(
            f"{label} 已达到 pilot floor {floors.pilot_floor}，但低于 diagnostic floor "
            f"{floors.diagnostic_floor}；允许启动复盘和候选整理，不允许输出正式调权结论。"
        )
    if available_count < floors.promotion_floor:
        warnings.append(
            f"{label} 低于 promotion floor {floors.promotion_floor}；不得晋级 production "
            "权重、规则或仓位 gate。"
        )
    return warnings


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "无"
    return "，".join(f"{key}={value}" for key, value in sorted(counts.items(), key=_count_sort_key))


def _count_sort_key(item: tuple[str, int]) -> tuple[int, float | str]:
    key = item[0]
    try:
        return (0, float(key))
    except ValueError:
        return (1, key)


def _readiness_explanation(report: MarketFeedbackOptimizationReport) -> str:
    if report.readiness == "INSUFFICIENT_REPORTING_SAMPLE":
        return "outcome 样本不足 reporting floor，本期只能展示缺口。"
    if report.readiness == "INSUFFICIENT_DECISION_PILOT_SAMPLE":
        return "decision outcome 样本不足 pilot floor，暂不启动后续复盘流程。"
    if report.readiness == "INSUFFICIENT_FORWARD_SHADOW_PILOT_SAMPLE":
        return "prediction/shadow outcome 样本不足 pilot floor，暂不启动候选 shadow 流程。"
    if report.readiness == "READY_FOR_REPLAY_OR_SHADOW_REVIEW":
        return (
            "已有候选规则待 replay 或 forward shadow；当前可启动 pilot 验证，但不能改 production。"
        )
    if report.readiness == "READY_FOR_APPROVED_OVERLAY_AUDIT":
        return "存在 approved overlay，应审计命中上下文、过期条件和回滚条件。"
    if report.readiness == "PILOT_DIAGNOSTIC_REVIEW":
        return "样本达到 pilot floor，可启动因果链、学习队列和候选规则整理；权重诊断仍是研究用途。"
    return "样本门槛已满足，可进入权重诊断和候选 overlay 设计复核。"


def _next_action_line(report: MarketFeedbackOptimizationReport) -> str:
    if report.readiness == "INSUFFICIENT_REPORTING_SAMPLE":
        return "- 先补齐 outcome 生成；当前样本连 reporting floor 都未达到。"
    if report.readiness == "INSUFFICIENT_DECISION_PILOT_SAMPLE":
        return "- 继续生成 decision outcome；达到 pilot floor 后再启动学习队列和候选整理。"
    if report.readiness == "INSUFFICIENT_FORWARD_SHADOW_PILOT_SAMPLE":
        return "- 继续补充 prediction/shadow outcome；达到 pilot floor 后再启动 shadow 复核。"
    if report.readiness == "READY_FOR_REPLAY_OR_SHADOW_REVIEW":
        return "- 启动候选规则的 as-if replay 或 forward shadow，保持 `production_effect=none`。"
    if report.readiness == "READY_FOR_APPROVED_OVERLAY_AUDIT":
        return "- 运行 `apply-calibration-overlay` 审计命中情况，确认是否仍在有效期内。"
    if report.readiness == "PILOT_DIAGNOSTIC_REVIEW":
        if not report.parameter_replay.get("exists"):
            return (
                "- 先运行带 robustness 的回测并生成 `feedback build-parameter-replay`，"
                "把参数复测收益变化纳入闭环；不要晋级 production。"
            )
        if not report.parameter_candidates.get("exists"):
            return (
                "- 运行 `feedback build-parameter-candidates`，把参数复测结果登记为 "
                "candidate-only trial ledger；不要晋级 production。"
            )
        return (
            "- 先跑 causal chain、learning queue 和 rule experiment 候选整理；不要晋级 production。"
        )
    return "- 设计候选 weight diagnostics，但保持 candidate-only 和 production_effect=none。"
