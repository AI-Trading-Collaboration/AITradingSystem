from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.backtest.daily import (
    DailyBacktestResult,
    build_backtest_data_credibility,
)
from ai_trading_system.backtest.lag_sensitivity import BacktestLagSensitivityReport
from ai_trading_system.backtest.robustness import BacktestRobustnessReport
from ai_trading_system.config import (
    BacktestPromotionPolicyConfig,
    load_backtest_validation_policy,
)
from ai_trading_system.feedback_sample_policy import load_feedback_sample_policy


@dataclass(frozen=True)
class ModelPromotionCheck:
    check_id: str
    status: str
    reason: str
    evidence_ref: str


@dataclass(frozen=True)
class ModelPromotionReport:
    as_of: date
    backtest_result: DailyBacktestResult
    checks: tuple[ModelPromotionCheck, ...]
    robustness_report_path: Path | None
    lag_sensitivity_report_path: Path | None
    prediction_outcomes_path: Path | None
    rule_governance_status: str
    policy_metadata: dict[str, object]
    promotion_policy: BacktestPromotionPolicyConfig
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if any(check.status == "FAIL" for check in self.checks):
            return "NOT_PROMOTABLE"
        if any(check.status == "MISSING" for check in self.checks):
            return "READY_FOR_SHADOW"
        return "READY_FOR_GOV_REVIEW"


def default_model_promotion_report_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"model_promotion_{start.isoformat()}_{end.isoformat()}.md"


def default_model_promotion_summary_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"model_promotion_{start.isoformat()}_{end.isoformat()}.json"


def build_model_promotion_report(
    *,
    result: DailyBacktestResult,
    as_of: date,
    robustness_report: BacktestRobustnessReport | None,
    robustness_report_path: Path | None,
    lag_sensitivity_report: BacktestLagSensitivityReport | None,
    lag_sensitivity_report_path: Path | None,
    prediction_outcomes_path: Path | None,
    rule_governance_status: str,
    promotion_policy: BacktestPromotionPolicyConfig | None = None,
    policy_metadata: dict[str, object] | None = None,
) -> ModelPromotionReport:
    if promotion_policy is None or policy_metadata is None:
        validation_policy = load_backtest_validation_policy()
        if promotion_policy is None:
            promotion_policy = validation_policy.promotion
        if policy_metadata is None:
            policy_metadata = validation_policy.policy_metadata.model_dump(mode="json")
    checks = [
        _data_credibility_check(result, promotion_policy),
        _robustness_check(robustness_report, robustness_report_path, promotion_policy),
        _lag_sensitivity_check(
            lag_sensitivity_report,
            lag_sensitivity_report_path,
            promotion_policy,
        ),
        _shadow_outcome_check(prediction_outcomes_path),
        _rule_governance_check(rule_governance_status, promotion_policy),
    ]
    return ModelPromotionReport(
        as_of=as_of,
        backtest_result=result,
        checks=tuple(checks),
        robustness_report_path=robustness_report_path,
        lag_sensitivity_report_path=lag_sensitivity_report_path,
        prediction_outcomes_path=prediction_outcomes_path,
        rule_governance_status=rule_governance_status,
        policy_metadata=policy_metadata,
        promotion_policy=promotion_policy,
    )


def write_model_promotion_report(report: ModelPromotionReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_model_promotion_report(report), encoding="utf-8")
    return output_path


def write_model_promotion_summary(report: ModelPromotionReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            model_promotion_summary_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return output_path


def model_promotion_summary_record(report: ModelPromotionReport) -> dict[str, object]:
    result = report.backtest_result
    credibility = build_backtest_data_credibility(result)
    return {
        "schema_version": 1,
        "report_type": "model_promotion_gate",
        "production_effect": report.production_effect,
        "status": report.status,
        "as_of": report.as_of.isoformat(),
        "requested_start": result.requested_start.isoformat(),
        "requested_end": result.requested_end.isoformat(),
        "market_regime": (
            None
            if result.market_regime is None
            else {
                "regime_id": result.market_regime.regime_id,
                "name": result.market_regime.name,
                "start_date": result.market_regime.start_date.isoformat(),
                "anchor_date": result.market_regime.anchor_date.isoformat(),
                "anchor_event": result.market_regime.anchor_event,
            }
        ),
        "backtest_data_quality": {
            "grade": credibility.grade,
            "label": credibility.label,
        },
        "policy_metadata": report.policy_metadata,
        "promotion_policy": report.promotion_policy.model_dump(mode="json"),
        "checks": [
            {
                "check_id": check.check_id,
                "status": check.status,
                "reason": check.reason,
                "evidence_ref": check.evidence_ref,
            }
            for check in report.checks
        ],
    }


def render_model_promotion_report(report: ModelPromotionReport) -> str:
    result = report.backtest_result
    credibility = build_backtest_data_credibility(result)
    lines = [
        "# 模型晋级门槛报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 请求区间：{result.requested_start.isoformat()} 至 {result.requested_end.isoformat()}",
        f"- Backtest Data Quality：{credibility.label}（{credibility.grade}）",
        f"- Policy version：{report.policy_metadata.get('version', 'unknown')}",
        f"- production_effect={report.production_effect}",
        "- 晋级路线：历史探索 -> 近似 PIT 回测 -> 稳健性/滞后敏感性 -> "
        "前向 shadow -> owner/rule card 批准 -> production rule",
    ]
    if result.market_regime is not None:
        lines.extend(
            [
                (
                    f"- 市场阶段：{result.market_regime.name}"
                    f"（{result.market_regime.regime_id}）"
                ),
                f"- 阶段默认起点：{result.market_regime.start_date.isoformat()}",
                (
                    f"- 锚定事件：{result.market_regime.anchor_date.isoformat()} "
                    f"{result.market_regime.anchor_event}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Gate 检查",
            "",
            "| Check | 状态 | Evidence | 说明 |",
            "|---|---|---|---|",
        ]
    )
    for check in report.checks:
        lines.append(
            "| "
            f"{check.check_id} | "
            f"{check.status} | "
            f"{_escape(check.evidence_ref)} | "
            f"{_escape(check.reason)} |"
        )
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- C 级历史回测只能生成研究线索，不得直接晋级或作为上线证据。",
            "- `READY_FOR_SHADOW` 表示可以进入前向观察，不表示 production rule 已批准。",
            "- `READY_FOR_GOV_REVIEW` 仍需要 `GOV-001` owner approval、promotion 记录和回滚条件。",
            "- 后验 outcome 只能追加到 prediction ledger，不得改写 signal-time 输入或因果链。",
        ]
    )
    return "\n".join(lines) + "\n"


def _data_credibility_check(
    result: DailyBacktestResult,
    policy: BacktestPromotionPolicyConfig,
) -> ModelPromotionCheck:
    credibility = build_backtest_data_credibility(result)
    if credibility.grade in policy.blocking_data_credibility_grades:
        return ModelPromotionCheck(
            check_id="data_credibility",
            status="FAIL",
            reason=(
                f"回测数据可信度为 {credibility.grade} 级，属于 policy 阻断等级；"
                "只能作为探索性诊断，不得晋级。"
            ),
            evidence_ref="backtest_report:Backtest Data Quality",
        )
    return ModelPromotionCheck(
        check_id="data_credibility",
        status="PASS",
        reason=f"回测数据可信度为 {credibility.grade} 级。",
        evidence_ref="backtest_report:Backtest Data Quality",
    )


def _robustness_check(
    report: BacktestRobustnessReport | None,
    report_path: Path | None,
    policy: BacktestPromotionPolicyConfig,
) -> ModelPromotionCheck:
    if report is None:
        return ModelPromotionCheck(
            check_id="robustness",
            status="MISSING",
            reason="缺少 robustness report；不能判断成本、权重、随机和样本外稳健性。",
            evidence_ref="missing",
        )
    completed_categories = {
        scenario.category
        for scenario in report.scenarios
        if scenario.result is not None or scenario.metrics is not None
    }
    required = set(policy.required_robustness_categories)
    missing = sorted(required - completed_categories)
    if missing:
        return ModelPromotionCheck(
            check_id="robustness",
            status="MISSING",
            reason=f"robustness 场景未完整覆盖：{', '.join(missing)}。",
            evidence_ref=_path_ref(report_path),
        )
    return ModelPromotionCheck(
        check_id="robustness",
        status="PASS",
        reason="robustness 场景已覆盖成本、基线、再平衡、权重扰动、随机和样本外验证。",
        evidence_ref=_path_ref(report_path),
    )


def _lag_sensitivity_check(
    report: BacktestLagSensitivityReport | None,
    report_path: Path | None,
    policy: BacktestPromotionPolicyConfig,
) -> ModelPromotionCheck:
    if report is None:
        return ModelPromotionCheck(
            check_id="lag_sensitivity",
            status="MISSING",
            reason="缺少 lag sensitivity report；不能判断未来函数敏感性。",
            evidence_ref="missing",
        )
    has_three_day = any(
        scenario.result is not None
        and max(scenario.feature_lag_days, scenario.universe_lag_days)
        >= policy.min_lag_sensitivity_days
        for scenario in report.scenarios
    )
    if not has_three_day:
        return ModelPromotionCheck(
            check_id="lag_sensitivity",
            status="MISSING",
            reason=(
                "缺少 "
                f"{policy.min_lag_sensitivity_days} 个交易日以上 "
                "feature/universe lag 场景。"
            ),
            evidence_ref=_path_ref(report_path),
        )
    return ModelPromotionCheck(
        check_id="lag_sensitivity",
        status="PASS",
        reason=(
            "已运行 "
            f"{policy.min_lag_sensitivity_days} 个交易日以上 "
            "feature/universe lag 场景。"
        ),
        evidence_ref=_path_ref(report_path),
    )


def _shadow_outcome_check(prediction_outcomes_path: Path | None) -> ModelPromotionCheck:
    if prediction_outcomes_path is None or not prediction_outcomes_path.exists():
        return ModelPromotionCheck(
            check_id="shadow_outcome",
            status="MISSING",
            reason="缺少 prediction/shadow outcome；只能进入前向 shadow，不能直接晋级。",
            evidence_ref="missing",
        )
    try:
        frame = pd.read_csv(prediction_outcomes_path)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        return ModelPromotionCheck(
            check_id="shadow_outcome",
            status="FAIL",
            reason=f"prediction outcome 无法读取：{exc}",
            evidence_ref=str(prediction_outcomes_path),
        )
    if "outcome_status" not in frame.columns:
        return ModelPromotionCheck(
            check_id="shadow_outcome",
            status="FAIL",
            reason="prediction outcome 缺少 outcome_status 字段。",
            evidence_ref=str(prediction_outcomes_path),
        )
    available = frame.loc[frame["outcome_status"] == "AVAILABLE"]
    promotion_floor = load_feedback_sample_policy().prediction_outcomes.promotion_floor
    if len(available) < promotion_floor:
        return ModelPromotionCheck(
            check_id="shadow_outcome",
            status="MISSING",
            reason=(
                f"可用 prediction outcome 只有 {len(available)} 行，低于 "
                f"promotion floor {promotion_floor}。"
            ),
            evidence_ref=str(prediction_outcomes_path),
        )
    return ModelPromotionCheck(
        check_id="shadow_outcome",
        status="PASS",
        reason=f"已有 {len(available)} 行可用 prediction outcome。",
        evidence_ref=str(prediction_outcomes_path),
    )


def _rule_governance_check(
    rule_governance_status: str,
    policy: BacktestPromotionPolicyConfig,
) -> ModelPromotionCheck:
    if rule_governance_status == policy.required_rule_governance_status:
        return ModelPromotionCheck(
            check_id="rule_governance",
            status="PASS",
            reason="rule card registry 校验通过；正式晋级仍需 owner approval。",
            evidence_ref="rule_cards",
        )
    return ModelPromotionCheck(
        check_id="rule_governance",
        status="FAIL",
        reason=f"rule governance 状态为 {rule_governance_status}。",
        evidence_ref="rule_cards",
    )


def _path_ref(path: Path | None) -> str:
    return "missing" if path is None else str(path)


def _escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
