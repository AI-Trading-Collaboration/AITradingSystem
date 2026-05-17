from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

WEIGHT_PROMOTION_GATE_SCHEMA_VERSION = 1
WEIGHT_PROMOTION_GATE_REPORT_TYPE = "weight_promotion_gate"
GATE_MODE_MANUAL_REVIEW_ONLY = "manual_review_only"
PRODUCTION_EFFECT_NONE = "none"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_WEIGHT_PROMOTION_GATE_POLICY_PATH = (
    REPO_ROOT / "config" / "weight_promotion_gate_policy.yaml"
)
DEFAULT_PARAMETER_GOVERNANCE_PATH = REPO_ROOT / "config" / "parameter_governance.yaml"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"

PROMO_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
PROMO_OBSERVE_ONLY = "OBSERVE_ONLY"
PROMO_BLOCKED = "BLOCKED"
PROMO_NO_CLEAR_IMPROVEMENT = "NO_CLEAR_IMPROVEMENT"
PROMO_PROMISING_LIMITED = "CANDIDATE_PROMISING_BUT_LIMITED"
PROMO_READY_FOR_MANUAL_REVIEW = "READY_FOR_MANUAL_REVIEW"
ALLOWED_PROMOTION_GATE_STATUSES = {
    PROMO_INSUFFICIENT_DATA,
    PROMO_OBSERVE_ONLY,
    PROMO_BLOCKED,
    PROMO_NO_CLEAR_IMPROVEMENT,
    PROMO_PROMISING_LIMITED,
    PROMO_READY_FOR_MANUAL_REVIEW,
}

FORBIDDEN_PROMOTION_TERMS = {
    "AUTO_PROMOTE",
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
    "APPROVED",
}

BLOCKER_MISSING_CANDIDATE_EVALUATION = "missing_weight_candidate_evaluation"
BLOCKER_MISSING_CANDIDATES = "missing_weight_adjustment_candidates"
BLOCKER_MISSING_PAPER_SIGNAL_QUALITY = "missing_paper_signal_quality"
BLOCKER_MISSING_SHADOW_IMPACT = "missing_shadow_parameter_impact"
BLOCKER_CANDIDATE_BLOCKED = "candidate_blocked"
BLOCKER_MANUAL_APPROVAL_MISSING = "manual_approval_required_missing"
BLOCKER_INSUFFICIENT_SAMPLE = "insufficient_sample"
BLOCKER_INSUFFICIENT_FILLED_COUNT = "insufficient_filled_count"
BLOCKER_LOW_DATA_QUALITY = "low_data_quality"
BLOCKER_SYNTHETIC_RATIO_HIGH = "synthetic_snapshot_ratio_too_high"
BLOCKER_OHLC_COVERAGE_LOW = "historical_ohlc_coverage_low"
BLOCKER_RECONCILIATION_UNRELIABLE = "reconciliation_unreliable"
BLOCKER_CONTINUOUS_REPLAY_MISSING = "continuous_replay_missing"
BLOCKER_MAX_DRAWDOWN_WORSE = "max_drawdown_worse"
BLOCKER_EXPOSURE_WORSE = "exposure_worse"
BLOCKER_CONCENTRATION_WORSE = "concentration_worse"
BLOCKER_SHADOW_IMPACT_INSUFFICIENT = "shadow_impact_insufficient"
BLOCKER_SHADOW_IMPACT_UNRELIABLE = "shadow_impact_unreliable"
BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE = "paper_signal_quality_unreliable"
BLOCKER_DATA_GATE_BLOCK = "data_gate_block"
BLOCKER_MAJOR_RISK_EVENT_WARNING = "major_risk_event_warning"
BLOCKER_NO_CLEAR_IMPROVEMENT = "no_clear_improvement"
BLOCKER_IMPROVEMENT_LIMITED = "improvement_signal_limited"

MANUAL_APPROVAL_MARKERS = {
    "manual_approval_required",
    "manual_owner_review",
    "manual_review_required",
}

BLOCKER_EXPLANATIONS = {
    BLOCKER_MISSING_CANDIDATE_EVALUATION: "缺少同日 weight candidate evaluation JSON。",
    BLOCKER_MISSING_CANDIDATES: "缺少同日 weight adjustment candidates JSON。",
    BLOCKER_MISSING_PAPER_SIGNAL_QUALITY: "缺少同日 paper signal quality JSON。",
    BLOCKER_MISSING_SHADOW_IMPACT: "缺少同日 shadow parameter impact JSON。",
    BLOCKER_CANDIDATE_BLOCKED: "上游 candidate 存在非人工复核原因的 blocked 状态。",
    BLOCKER_MANUAL_APPROVAL_MISSING: "candidate 未保留 manual_approval_required 约束。",
    BLOCKER_INSUFFICIENT_SAMPLE: "paper / shadow / production baseline 样本低于 policy floor。",
    BLOCKER_INSUFFICIENT_FILLED_COUNT: "filled_count 低于 policy floor。",
    BLOCKER_LOW_DATA_QUALITY: "数据质量不足以支持人工复核入口。",
    BLOCKER_SYNTHETIC_RATIO_HIGH: "synthetic snapshot ratio 高于 policy 上限。",
    BLOCKER_OHLC_COVERAGE_LOW: "historical OHLC coverage 低于 policy 下限。",
    BLOCKER_RECONCILIATION_UNRELIABLE: "reconciliation PASS 比例低于 policy 下限。",
    BLOCKER_CONTINUOUS_REPLAY_MISSING: "缺少 continuous portfolio replay 证据。",
    BLOCKER_MAX_DRAWDOWN_WORSE: "candidate max drawdown 差于 production baseline。",
    BLOCKER_EXPOSURE_WORSE: "candidate exposure 高于 production baseline。",
    BLOCKER_CONCENTRATION_WORSE: "candidate concentration 高于 production baseline。",
    BLOCKER_SHADOW_IMPACT_INSUFFICIENT: "shadow impact 证据不足。",
    BLOCKER_SHADOW_IMPACT_UNRELIABLE: "shadow impact 可靠性不足。",
    BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE: "paper signal quality 可靠性不足。",
    BLOCKER_DATA_GATE_BLOCK: "daily data gate 为 BLOCK。",
    BLOCKER_MAJOR_RISK_EVENT_WARNING: "daily summary 出现重大风险事件 warning。",
    BLOCKER_NO_CLEAR_IMPROVEMENT: "candidate 相比 production 没有清晰改善信号。",
    BLOCKER_IMPROVEMENT_LIMITED: "candidate 改善信号仍不够稳定。",
}

RECOMMENDATION_MANUAL_REVIEW = "manual_review_only"
RECOMMENDATION_CONTINUE_OBSERVATION = "continue_observation"
RECOMMENDATION_COLLECT_EVIDENCE = "collect_more_evidence"
RECOMMENDATION_DO_NOT_ADVANCE = "do_not_advance"


def default_weight_promotion_gate_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"weight_promotion_gate_{as_of.isoformat()}.json"


def build_weight_promotion_gate_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_PROMOTION_GATE_POLICY_PATH,
    weight_adjustment_candidates_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    daily_decision_summary_path: Path | None = None,
    parameter_governance_path: Path | None = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path | None = DEFAULT_PRODUCTION_PROFILE_PATH,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if selected_window_days not in {7, 14, 30}:
        raise ValueError("selected_window_days must be one of 7, 14, or 30")

    suffix = as_of.isoformat()
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_weight_promotion_gate_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    resolved_candidates_path = weight_adjustment_candidates_path or (
        reports_dir / f"weight_adjustment_candidates_{suffix}.json"
    )
    resolved_evaluation_path = weight_candidate_evaluation_path or (
        reports_dir / f"weight_candidate_evaluation_{suffix}.json"
    )
    resolved_paper_path = paper_signal_quality_path or (
        reports_dir / f"paper_signal_quality_{suffix}.json"
    )
    resolved_shadow_path = shadow_parameter_impact_path or (
        reports_dir / f"shadow_parameter_impact_{suffix}.json"
    )
    resolved_daily_path = daily_decision_summary_path or (
        reports_dir / f"daily_decision_summary_{suffix}.json"
    )
    selected_replay_path = replay_json_path or _select_latest_replay_path(reports_dir, as_of)

    policy = _load_yaml_object(policy_path)
    thresholds = _mapping(policy.get("thresholds"))
    candidates_payload = _read_json_object(resolved_candidates_path)
    evaluation_payload = _read_json_object(resolved_evaluation_path)
    paper_quality = _read_json_object(resolved_paper_path)
    shadow_impact = _read_json_object(resolved_shadow_path)
    replay_payload = _read_json_object(selected_replay_path)
    daily_summary = _read_json_object(resolved_daily_path)
    parameter_governance = (
        _load_yaml_object(parameter_governance_path) if parameter_governance_path else {}
    )
    production_profile = (
        _load_yaml_object(production_profile_path) if production_profile_path else {}
    )

    evaluation_candidates = _candidate_records(
        evaluation_payload,
        expected_report_type="weight_candidate_evaluation",
    )
    adjustment_candidates = _candidate_records(
        candidates_payload,
        expected_report_type="weight_adjustment_candidates",
    )
    adjustment_by_id = {
        _string_value(candidate.get("candidate_id")): candidate
        for candidate in adjustment_candidates
    }
    candidate_records = evaluation_candidates or adjustment_candidates
    context = _gate_context(
        evaluation_payload=evaluation_payload,
        candidates_payload=candidates_payload,
        paper_quality=paper_quality,
        shadow_impact=shadow_impact,
        replay_payload=replay_payload,
        daily_summary=daily_summary,
        thresholds=thresholds,
        selected_window_days=selected_window_days,
    )
    candidates = [
        _promotion_candidate(
            candidate=candidate,
            adjustment_candidate=adjustment_by_id.get(
                _string_value(candidate.get("candidate_id")),
                {},
            ),
            context=context,
            thresholds=thresholds,
            selected_window_days=selected_window_days,
        )
        for candidate in candidate_records
    ]
    gate_status = _overall_promotion_status(
        evaluation_payload=evaluation_payload,
        candidate_records=candidate_records,
        candidates=candidates,
        context=context,
    )
    main_blocked_by = _main_blocked_by(candidates, context)
    ready_count = sum(
        1
        for candidate in candidates
        if candidate["promotion_gate_status"] == PROMO_READY_FOR_MANUAL_REVIEW
    )
    blocked_count = sum(
        1
        for candidate in candidates
        if candidate["promotion_gate_status"] in {PROMO_BLOCKED, PROMO_INSUFFICIENT_DATA}
    )
    policy_report = _policy_report(policy, policy_path)
    payload = {
        "schema_version": WEIGHT_PROMOTION_GATE_SCHEMA_VERSION,
        "report_type": WEIGHT_PROMOTION_GATE_REPORT_TYPE,
        "generated_at": generated.isoformat(),
        "as_of": suffix,
        "market_regime": "ai_after_chatgpt",
        "promotion_gate_status": gate_status,
        "gate_status": gate_status,
        "gate_mode": GATE_MODE_MANUAL_REVIEW_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "selected_window_days": selected_window_days,
        "policy_id": policy_report["policy_id"],
        "policy_version": policy_report["version"],
        "thresholds_snapshot": policy_report["thresholds"],
        "policy": policy_report,
        "evaluation_scope": {
            "manual_review_only": True,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "changes_production_parameters": False,
            "writes_production_profile": False,
            "writes_reviewed_profile": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "runs_replay": False,
            "uses_existing_artifacts_only": True,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_ibkr": False,
            "calls_paperbroker": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_reviewed_profile": False,
            "changes_production_parameters": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "source_artifacts": {
            "reports_dir": str(reports_dir),
            "policy_path": _artifact_record(policy_path, reports_dir),
            "weight_adjustment_candidates": _artifact_record(
                resolved_candidates_path,
                reports_dir,
                expected_report_type="weight_adjustment_candidates",
                payload=candidates_payload,
            ),
            "weight_candidate_evaluation": _artifact_record(
                resolved_evaluation_path,
                reports_dir,
                expected_report_type="weight_candidate_evaluation",
                payload=evaluation_payload,
            ),
            "paper_signal_quality": _artifact_record(
                resolved_paper_path,
                reports_dir,
                expected_report_type="paper_signal_quality",
                payload=paper_quality,
            ),
            "shadow_parameter_impact": _artifact_record(
                resolved_shadow_path,
                reports_dir,
                expected_report_type="shadow_parameter_impact",
                payload=shadow_impact,
            ),
            "paper_trading_replay": _artifact_record(
                selected_replay_path,
                reports_dir,
                expected_report_type="paper_trading_replay",
                payload=replay_payload,
                optional=True,
            ),
            "daily_decision_summary": _artifact_record(
                resolved_daily_path,
                reports_dir,
                expected_report_type="daily_decision_summary",
                payload=daily_summary,
                optional=True,
            ),
            "parameter_governance": _artifact_record(
                parameter_governance_path,
                reports_dir,
                optional=True,
            ),
            "production_profile": _artifact_record(
                production_profile_path,
                reports_dir,
                optional=True,
            ),
        },
        "metadata_inputs": {
            "parameter_governance": _metadata_record(parameter_governance),
            "production_profile": _metadata_record(production_profile),
        },
        "input_status": context["input_status"],
        "summary": {
            "gate_status": gate_status,
            "promotion_gate_status": gate_status,
            "candidate_count": len(candidate_records),
            "ready_for_manual_review_count": ready_count,
            "blocked_count": blocked_count,
            "no_clear_improvement_count": sum(
                1
                for candidate in candidates
                if candidate["promotion_gate_status"] == PROMO_NO_CLEAR_IMPROVEMENT
            ),
            "promising_limited_count": sum(
                1
                for candidate in candidates
                if candidate["promotion_gate_status"] == PROMO_PROMISING_LIMITED
            ),
            "top_candidate_id": _top_candidate_id(
                candidates_payload, evaluation_payload, candidates
            ),
            "main_blocked_by": main_blocked_by,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "gate_mode": GATE_MODE_MANUAL_REVIEW_ONLY,
            "report_link": _report_href(output_md_path, reports_dir),
        },
        "gate_context": context["summary"],
        "candidates": candidates,
        "notes": [
            "本报告是 manual-review-only gate，不是自动晋级。",
            "本报告不改变 production profile，不写入人工复核后的 profile。",
            "本报告不触发 IBKR、PaperBroker、paper runner 或 replay runner。",
            "本报告不影响交易，不代表 live readiness。",
            "READY_FOR_MANUAL_REVIEW 也只表示可以准备人工复核材料。",
        ],
    }
    _assert_allowed_statuses(payload)
    _assert_forbidden_terms_absent(payload, render_weight_promotion_gate_report(payload))
    return payload


def write_weight_promotion_gate_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_PROMOTION_GATE_POLICY_PATH,
    weight_adjustment_candidates_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    daily_decision_summary_path: Path | None = None,
    parameter_governance_path: Path | None = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path | None = DEFAULT_PRODUCTION_PROFILE_PATH,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_weight_promotion_gate_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=policy_path,
        weight_adjustment_candidates_path=weight_adjustment_candidates_path,
        weight_candidate_evaluation_path=weight_candidate_evaluation_path,
        paper_signal_quality_path=paper_signal_quality_path,
        shadow_parameter_impact_path=shadow_parameter_impact_path,
        replay_json_path=replay_json_path,
        daily_decision_summary_path=daily_decision_summary_path,
        parameter_governance_path=parameter_governance_path,
        production_profile_path=production_profile_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        selected_window_days=selected_window_days,
        generated_at=generated_at,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_weight_promotion_gate_report(payload), encoding="utf-8")
    return payload


def render_weight_promotion_gate_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    thresholds = _mapping(payload.get("thresholds_snapshot"))
    candidates = _list_mappings(payload.get("candidates"))
    lines = [
        "# Weight Promotion Gate",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- promotion_gate_status：`{payload.get('promotion_gate_status')}`",
        f"- gate_mode：`{payload.get('gate_mode')}`",
        "- production_effect=none",
        "- manual-review-only gate：是",
        "- 自动晋级：否",
        "- production profile 修改：无",
        "- 交易影响：无",
        "- live readiness：否",
        "",
        "## Policy Thresholds",
        "",
        "| Threshold | Value |",
        "|---|---:|",
    ]
    for key in (
        "minimum_paper_signal_sample_count",
        "minimum_shadow_sample_count",
        "minimum_production_baseline_count",
        "minimum_filled_count",
        "maximum_synthetic_snapshot_ratio",
        "minimum_historical_ohlc_coverage",
        "minimum_reconciliation_pass_ratio",
        "minimum_max_drawdown_delta",
        "maximum_exposure_delta",
        "maximum_concentration_delta",
        "minimum_final_equity_delta",
        "minimum_improvement_windows",
    ):
        lines.append(f"| {key} | {thresholds.get(key, 'missing')} |")

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- gate_status：`{summary.get('gate_status', PROMO_INSUFFICIENT_DATA)}`",
            f"- candidate_count：{summary.get('candidate_count', 0)}",
            (
                "- ready_for_manual_review_count："
                f"{summary.get('ready_for_manual_review_count', 0)}"
            ),
            f"- blocked_count：{summary.get('blocked_count', 0)}",
            f"- top_candidate_id：`{summary.get('top_candidate_id', '')}`",
            f"- main_blocked_by：`{summary.get('main_blocked_by', 'none')}`",
            "",
            "## Candidates",
            "",
            (
                "| candidate_id | candidate_evaluation_status | promotion_gate_status | "
                "blocked_by | warnings | recommendation |"
            ),
            "|---|---|---|---|---|---|",
        ]
    )
    for candidate in candidates:
        recommendation = _mapping(candidate.get("recommendation"))
        lines.append(
            "| "
            f"`{candidate.get('candidate_id')}` | "
            f"{candidate.get('candidate_evaluation_status', '')} | "
            f"{candidate.get('promotion_gate_status', '')} | "
            f"{', '.join(_strings(candidate.get('blocked_by'))) or 'none'} | "
            f"{', '.join(_strings(candidate.get('warnings'))) or 'none'} | "
            f"{recommendation.get('action', '')} |"
        )

    for candidate in candidates:
        improvement = _mapping(candidate.get("improvement_summary"))
        risk = _mapping(candidate.get("risk_delta_summary"))
        data_quality = _mapping(candidate.get("data_quality_summary"))
        review_items = ", ".join(_strings(candidate.get("required_manual_review_items"))) or "none"
        lines.extend(
            [
                "",
                f"### {candidate.get('candidate_id')}",
                "",
                f"- improvement_summary：{_json_inline(improvement)}",
                f"- risk_delta_summary：{_json_inline(risk)}",
                f"- data_quality_summary：{_json_inline(data_quality)}",
                f"- required_manual_review_items：{review_items}",
            ]
        )

    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- 这是 manual-review-only gate，不是自动晋级流程。",
            "- 不修改 `config/weights/weight_profile_current.yaml`。",
            "- 不写入人工复核后的 profile。",
            "- 不触发 IBKR、PaperBroker、paper runner 或 replay runner。",
            "- 不影响交易，不代表 live readiness。",
            "",
        ]
    )
    return "\n".join(lines)


def _promotion_candidate(
    *,
    candidate: dict[str, Any],
    adjustment_candidate: dict[str, Any],
    context: dict[str, Any],
    thresholds: dict[str, Any],
    selected_window_days: int,
) -> dict[str, Any]:
    candidate_id = _string_value(candidate.get("candidate_id"))
    metrics = _candidate_metrics(
        candidate=candidate,
        context=context,
        selected_window_days=selected_window_days,
    )
    checks, blockers, warnings = _promotion_checks(
        candidate=candidate,
        adjustment_candidate=adjustment_candidate,
        context=context,
        metrics=metrics,
        thresholds=thresholds,
    )
    promotion_gate_status = _candidate_promotion_status(
        blockers=blockers,
        metrics=metrics,
        evaluation_exists=context["input_status"]["weight_candidate_evaluation"],
    )
    return {
        "candidate_id": candidate_id,
        "candidate_evaluation_status": metrics["candidate_evaluation_status"],
        "promotion_gate_status": promotion_gate_status,
        "gate_mode": GATE_MODE_MANUAL_REVIEW_ONLY,
        "blocked": promotion_gate_status
        in {PROMO_INSUFFICIENT_DATA, PROMO_BLOCKED, PROMO_NO_CLEAR_IMPROVEMENT},
        "blocked_by": blockers,
        "warnings": warnings,
        "improvement_summary": _improvement_summary(metrics, thresholds),
        "risk_delta_summary": _risk_delta_summary(metrics),
        "data_quality_summary": _data_quality_summary(metrics),
        "required_manual_review_items": _required_manual_review_items(blockers),
        "recommendation": _recommendation(promotion_gate_status, blockers),
        "checks": checks,
        "reason_explanations": {
            blocker: BLOCKER_EXPLANATIONS.get(blocker, blocker) for blocker in blockers
        },
        "production_effect": PRODUCTION_EFFECT_NONE,
    }


def _promotion_checks(
    *,
    candidate: dict[str, Any],
    adjustment_candidate: dict[str, Any],
    context: dict[str, Any],
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    min_paper = _int_value(thresholds.get("minimum_paper_signal_sample_count"), default=7)
    min_shadow = _int_value(thresholds.get("minimum_shadow_sample_count"), default=7)
    min_production = _int_value(thresholds.get("minimum_production_baseline_count"), default=7)
    min_filled = _int_value(thresholds.get("minimum_filled_count"), default=3)
    max_synthetic = _float_value(thresholds.get("maximum_synthetic_snapshot_ratio"), default=0.25)
    min_ohlc = _float_value(thresholds.get("minimum_historical_ohlc_coverage"), default=0.70)
    min_reconciliation = _float_value(
        thresholds.get("minimum_reconciliation_pass_ratio"),
        default=0.90,
    )
    min_drawdown_delta = _float_value(thresholds.get("minimum_max_drawdown_delta"), default=0.0)
    max_exposure_delta = _float_value(thresholds.get("maximum_exposure_delta"), default=0.0)
    max_concentration_delta = _float_value(
        thresholds.get("maximum_concentration_delta"),
        default=0.0,
    )

    checks = [
        _minimum_check(
            "weight_candidate_evaluation_exists",
            1 if context["input_status"]["weight_candidate_evaluation"] else 0,
            1,
            BLOCKER_MISSING_CANDIDATE_EVALUATION,
        ),
        _minimum_check(
            "weight_adjustment_candidates_exists",
            1 if context["input_status"]["weight_adjustment_candidates"] else 0,
            1,
            BLOCKER_MISSING_CANDIDATES,
        ),
        _minimum_check(
            "paper_signal_quality_exists",
            1 if context["input_status"]["paper_signal_quality"] else 0,
            1,
            BLOCKER_MISSING_PAPER_SIGNAL_QUALITY,
        ),
        _minimum_check(
            "shadow_parameter_impact_exists",
            1 if context["input_status"]["shadow_parameter_impact"] else 0,
            1,
            BLOCKER_MISSING_SHADOW_IMPACT,
        ),
        _manual_required_check(candidate, adjustment_candidate),
        _hard_candidate_blocked_check(candidate, adjustment_candidate),
        _minimum_check(
            "paper_signal_sample_count",
            _int_value(metrics.get("paper_signal_sample_count")),
            min_paper,
            BLOCKER_INSUFFICIENT_SAMPLE,
        ),
        _minimum_check(
            "shadow_sample_count",
            _int_value(metrics.get("shadow_sample_count")),
            min_shadow,
            BLOCKER_INSUFFICIENT_SAMPLE,
        ),
        _minimum_check(
            "production_baseline_count",
            _int_value(metrics.get("production_baseline_count")),
            min_production,
            BLOCKER_INSUFFICIENT_SAMPLE,
        ),
        _minimum_check(
            "filled_count",
            _int_value(metrics.get("filled_count")),
            min_filled,
            BLOCKER_INSUFFICIENT_FILLED_COUNT,
        ),
        _maximum_check(
            "synthetic_snapshot_ratio",
            _float_value(metrics.get("synthetic_snapshot_ratio")),
            max_synthetic,
            BLOCKER_SYNTHETIC_RATIO_HIGH,
        ),
        _minimum_ratio_check(
            "historical_ohlc_coverage",
            _float_value(metrics.get("historical_ohlc_coverage")),
            min_ohlc,
            BLOCKER_OHLC_COVERAGE_LOW,
        ),
        _minimum_ratio_check(
            "reconciliation_pass_ratio",
            _float_value(metrics.get("reconciliation_pass_ratio")),
            min_reconciliation,
            BLOCKER_RECONCILIATION_UNRELIABLE,
        ),
        _minimum_check(
            "continuous_replay_available",
            1 if metrics.get("continuous_replay_available") else 0,
            1,
            BLOCKER_CONTINUOUS_REPLAY_MISSING,
        ),
        _minimum_optional_check(
            "max_drawdown_delta",
            _optional_float(metrics.get("max_drawdown_delta")),
            min_drawdown_delta,
            BLOCKER_MAX_DRAWDOWN_WORSE,
        ),
        _maximum_optional_check(
            "exposure_delta",
            _optional_float(metrics.get("exposure_delta")),
            max_exposure_delta,
            BLOCKER_EXPOSURE_WORSE,
        ),
        _maximum_optional_check(
            "concentration_delta",
            _optional_float(metrics.get("concentration_delta")),
            max_concentration_delta,
            BLOCKER_CONCENTRATION_WORSE,
        ),
        _data_gate_check(metrics),
        _major_risk_event_check(metrics),
        _improvement_check(metrics, thresholds),
    ]
    blockers = [str(check["reason_code"]) for check in checks if check.get("status") == "FAIL"]
    paper_status = _string_value(metrics.get("paper_signal_quality_status"))
    shadow_status = _string_value(metrics.get("shadow_impact_status"))
    candidate_evaluation_status = _string_value(metrics.get("candidate_evaluation_status"))
    if paper_status == "LOW_DATA_QUALITY":
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if paper_status == "UNRELIABLE":
        blockers.append(BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE)
    if paper_status == "INSUFFICIENT_DATA":
        blockers.append(BLOCKER_INSUFFICIENT_SAMPLE)
    if shadow_status == "INSUFFICIENT_DATA":
        blockers.append(BLOCKER_SHADOW_IMPACT_INSUFFICIENT)
    if shadow_status == "LOW_DATA_QUALITY":
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if shadow_status in {"SHADOW_UNRELIABLE", "UNRELIABLE"}:
        blockers.append(BLOCKER_SHADOW_IMPACT_UNRELIABLE)
    if candidate_evaluation_status == "LOW_DATA_QUALITY":
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if candidate_evaluation_status == "CANDIDATE_UNRELIABLE":
        blockers.append(BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE)
    if BLOCKER_SYNTHETIC_RATIO_HIGH in blockers or BLOCKER_OHLC_COVERAGE_LOW in blockers:
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    blockers = _unique_strings(_canonical_blockers(blockers))
    warnings = _unique_strings(
        [
            *_strings(context.get("warnings")),
            *_strings(candidate.get("warnings")),
            *(
                [BLOCKER_CONTINUOUS_REPLAY_MISSING]
                if BLOCKER_CONTINUOUS_REPLAY_MISSING in blockers
                else []
            ),
        ]
    )
    return checks, blockers, warnings


def _candidate_promotion_status(
    *,
    blockers: list[str],
    metrics: dict[str, Any],
    evaluation_exists: bool,
) -> str:
    if not evaluation_exists:
        return PROMO_INSUFFICIENT_DATA
    blocker_set = set(blockers)
    hard_blockers = blocker_set - {
        BLOCKER_NO_CLEAR_IMPROVEMENT,
        BLOCKER_IMPROVEMENT_LIMITED,
    }
    if hard_blockers:
        return PROMO_BLOCKED
    if BLOCKER_IMPROVEMENT_LIMITED in blocker_set:
        return PROMO_PROMISING_LIMITED
    if BLOCKER_NO_CLEAR_IMPROVEMENT in blocker_set:
        return PROMO_NO_CLEAR_IMPROVEMENT
    if bool(metrics.get("stable_improvement_signal")):
        return PROMO_READY_FOR_MANUAL_REVIEW
    return PROMO_OBSERVE_ONLY


def _overall_promotion_status(
    *,
    evaluation_payload: dict[str, Any],
    candidate_records: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    context: dict[str, Any],
) -> str:
    if evaluation_payload.get("report_type") != "weight_candidate_evaluation":
        return PROMO_INSUFFICIENT_DATA
    if not candidate_records:
        return PROMO_INSUFFICIENT_DATA
    statuses = [_string_value(candidate.get("promotion_gate_status")) for candidate in candidates]
    if PROMO_READY_FOR_MANUAL_REVIEW in statuses:
        return PROMO_READY_FOR_MANUAL_REVIEW
    for status in (
        PROMO_BLOCKED,
        PROMO_INSUFFICIENT_DATA,
        PROMO_PROMISING_LIMITED,
        PROMO_NO_CLEAR_IMPROVEMENT,
        PROMO_OBSERVE_ONLY,
    ):
        if status in statuses:
            return status
    blockers = _strings(context.get("base_blockers"))
    return PROMO_BLOCKED if blockers else PROMO_OBSERVE_ONLY


def _gate_context(
    *,
    evaluation_payload: dict[str, Any],
    candidates_payload: dict[str, Any],
    paper_quality: dict[str, Any],
    shadow_impact: dict[str, Any],
    replay_payload: dict[str, Any],
    daily_summary: dict[str, Any],
    thresholds: dict[str, Any],
    selected_window_days: int,
) -> dict[str, Any]:
    evaluation_valid = evaluation_payload.get("report_type") == "weight_candidate_evaluation"
    candidates_valid = candidates_payload.get("report_type") == "weight_adjustment_candidates"
    paper_valid = paper_quality.get("report_type") == "paper_signal_quality"
    shadow_valid = shadow_impact.get("report_type") == "shadow_parameter_impact"
    input_status = {
        "weight_candidate_evaluation": evaluation_valid,
        "weight_adjustment_candidates": candidates_valid,
        "paper_signal_quality": paper_valid,
        "shadow_parameter_impact": shadow_valid,
        "paper_trading_replay": replay_payload.get("report_type") == "paper_trading_replay",
        "daily_decision_summary": daily_summary.get("report_type") == "daily_decision_summary",
    }
    selected_window = _mapping(
        _mapping(evaluation_payload.get("windows")).get(str(selected_window_days))
    )
    replay = _continuous_replay_context(
        replay_payload=replay_payload,
        shadow_impact=shadow_impact,
        evaluation_payload=evaluation_payload,
    )
    daily_data_gate = _mapping(daily_summary.get("data_gate"))
    data_gate_status = (
        _string_value(daily_data_gate.get("status"))
        or _string_value(daily_summary.get("data_gate_status"))
        or "missing"
    )
    base_blockers: list[str] = []
    if not evaluation_valid:
        base_blockers.append(BLOCKER_MISSING_CANDIDATE_EVALUATION)
    if not candidates_valid:
        base_blockers.append(BLOCKER_MISSING_CANDIDATES)
    if not paper_valid:
        base_blockers.append(BLOCKER_MISSING_PAPER_SIGNAL_QUALITY)
    if not shadow_valid:
        base_blockers.append(BLOCKER_MISSING_SHADOW_IMPACT)
    if data_gate_status == "BLOCK":
        base_blockers.append(BLOCKER_DATA_GATE_BLOCK)
    if _major_risk_warning(daily_summary):
        base_blockers.append(BLOCKER_MAJOR_RISK_EVENT_WARNING)
    return {
        "input_status": input_status,
        "selected_window": selected_window,
        "paper_quality": paper_quality,
        "shadow_impact": shadow_impact,
        "replay": replay,
        "daily_summary": daily_summary,
        "data_gate_status": data_gate_status,
        "major_risk_event_warning": _major_risk_warning(daily_summary),
        "base_blockers": _unique_strings(base_blockers),
        "warnings": _context_warnings(selected_window, replay, daily_summary),
        "thresholds": thresholds,
        "summary": {
            "selected_window_days": selected_window_days,
            "evaluation_status": _string_value(evaluation_payload.get("evaluation_status"))
            or _string_value(selected_window.get("evaluation_status"))
            or "missing",
            "candidate_count": len(
                _candidate_records(
                    evaluation_payload,
                    expected_report_type="weight_candidate_evaluation",
                )
            ),
            "continuous_replay_available": bool(replay.get("available")),
            "replay_mode": _string_value(replay.get("replay_mode")) or "missing",
            "data_gate_status": data_gate_status,
            "major_risk_event_warning": _major_risk_warning(daily_summary),
            "base_blockers": _unique_strings(base_blockers),
        },
    }


def _candidate_metrics(
    *,
    candidate: dict[str, Any],
    context: dict[str, Any],
    selected_window_days: int,
) -> dict[str, Any]:
    selected_scorecard = _selected_candidate_scorecard(candidate, selected_window_days)
    scorecard_metrics = _mapping(selected_scorecard.get("metrics"))
    selected_window = _mapping(context.get("selected_window"))
    paper_quality = _mapping(context.get("paper_quality"))
    shadow_impact = _mapping(context.get("shadow_impact"))
    paper_window = _mapping(_mapping(paper_quality.get("windows")).get(str(selected_window_days)))
    shadow_window = _mapping(_mapping(shadow_impact.get("windows")).get(str(selected_window_days)))
    paper_summary = _mapping(paper_window.get("summary")) or _mapping(paper_quality.get("summary"))
    shadow_summary = _mapping(shadow_window.get("summary")) or _mapping(
        shadow_impact.get("summary")
    )
    shadow_comparison = _mapping(shadow_window.get("profile_comparison")) or _mapping(
        shadow_impact.get("profile_comparison")
    )
    production_comparison = _mapping(shadow_comparison.get("production"))
    candidate_comparison = _mapping(shadow_comparison.get("shadow"))
    sample_counts = _mapping(shadow_summary.get("sample_counts"))
    replay = _mapping(context.get("replay"))
    replay_metrics = _replay_metrics(replay)
    candidate_evaluation_status = (
        _string_value(candidate.get("evaluation_status"))
        or _string_value(selected_scorecard.get("evaluation_status"))
        or "INSUFFICIENT_DATA"
    )
    final_equity_delta = _first_known_float(
        scorecard_metrics.get("final_equity_delta"),
        selected_scorecard.get("final_equity_delta"),
        selected_window.get("final_equity_delta"),
        replay_metrics.get("final_equity_delta"),
    )
    improvement_windows = _improvement_window_count(candidate, context, selected_window_days)
    minimum_improvement_windows = _int_value(
        _mapping(context.get("thresholds")).get("minimum_improvement_windows"),
        default=2,
    )
    has_improvement_signal = _has_improvement_signal(
        candidate_evaluation_status=candidate_evaluation_status,
        final_equity_delta=final_equity_delta,
        minimum_final_equity_delta=_float_value(
            _mapping(context.get("thresholds")).get("minimum_final_equity_delta"),
            default=0.0,
        ),
    )
    return {
        "candidate_evaluation_status": candidate_evaluation_status,
        "paper_signal_sample_count": _first_known_int(
            scorecard_metrics.get("paper_signal_sample_count"),
            selected_window.get("paper_signal_sample_count"),
            paper_summary.get("sample_count"),
        ),
        "shadow_sample_count": _first_known_int(
            scorecard_metrics.get("shadow_sample_count"),
            selected_window.get("shadow_sample_count"),
            sample_counts.get("shadow"),
            candidate_comparison.get("sample_count"),
        ),
        "production_baseline_count": _first_known_int(
            scorecard_metrics.get("production_baseline_count"),
            selected_window.get("production_baseline_count"),
            sample_counts.get("production"),
            production_comparison.get("sample_count"),
        ),
        "filled_count": _max_known_int(
            scorecard_metrics.get("filled_count"),
            selected_window.get("filled_count"),
            paper_summary.get("filled_count"),
            candidate_comparison.get("filled_count"),
        ),
        "synthetic_snapshot_ratio": _max_known(
            _optional_float(scorecard_metrics.get("synthetic_snapshot_ratio")),
            _optional_float(selected_window.get("synthetic_snapshot_ratio")),
            _optional_float(paper_summary.get("synthetic_snapshot_ratio")),
            _optional_float(candidate_comparison.get("synthetic_snapshot_ratio")),
        ),
        "historical_ohlc_coverage": _min_known(
            _optional_float(scorecard_metrics.get("historical_ohlc_coverage")),
            _optional_float(selected_window.get("historical_ohlc_coverage")),
            _optional_float(paper_summary.get("historical_ohlc_coverage")),
            _optional_float(candidate_comparison.get("historical_ohlc_coverage")),
        ),
        "reconciliation_pass_ratio": _min_known(
            _optional_float(scorecard_metrics.get("reconciliation_pass_ratio")),
            _optional_float(selected_window.get("reconciliation_pass_ratio")),
            _optional_float(paper_summary.get("reconciliation_pass_ratio")),
            _optional_float(candidate_comparison.get("reconciliation_pass_ratio")),
        ),
        "paper_signal_quality_status": (
            _string_value(scorecard_metrics.get("paper_signal_quality_status"))
            or _string_value(selected_window.get("paper_signal_quality_status"))
            or _string_value(paper_window.get("evaluation_status"))
            or _string_value(paper_quality.get("evaluation_status"))
            or "missing"
        ),
        "shadow_impact_status": (
            _string_value(scorecard_metrics.get("shadow_impact_status"))
            or _string_value(selected_window.get("shadow_impact_status"))
            or _string_value(shadow_window.get("impact_status"))
            or _string_value(shadow_impact.get("impact_status"))
            or "missing"
        ),
        "continuous_replay_available": bool(
            scorecard_metrics.get("continuous_replay_available")
            or selected_window.get("continuous_replay_available")
            or replay.get("available")
        ),
        "replay_mode": (
            _string_value(scorecard_metrics.get("replay_mode"))
            or _string_value(selected_window.get("replay_mode"))
            or _string_value(replay.get("replay_mode"))
            or "missing"
        ),
        "max_drawdown_delta": _first_known_float(
            scorecard_metrics.get("max_drawdown_delta"),
            selected_scorecard.get("max_drawdown_delta"),
            selected_window.get("max_drawdown_delta"),
            replay_metrics.get("max_drawdown_delta"),
        ),
        "final_equity_delta": final_equity_delta,
        "exposure_delta": _first_known_float(
            scorecard_metrics.get("exposure_delta"),
            selected_scorecard.get("exposure_delta"),
            selected_window.get("exposure_delta"),
            replay_metrics.get("exposure_delta"),
        ),
        "concentration_delta": _first_known_float(
            scorecard_metrics.get("concentration_delta"),
            selected_scorecard.get("concentration_delta"),
            selected_window.get("concentration_delta"),
            replay_metrics.get("concentration_delta"),
        ),
        "data_gate_status": _string_value(context.get("data_gate_status")) or "missing",
        "major_risk_event_warning": bool(context.get("major_risk_event_warning")),
        "improvement_window_count": improvement_windows,
        "minimum_improvement_windows": minimum_improvement_windows,
        "has_improvement_signal": has_improvement_signal,
        "stable_improvement_signal": (
            has_improvement_signal and improvement_windows >= minimum_improvement_windows
        ),
    }


def _selected_candidate_scorecard(
    candidate: dict[str, Any],
    selected_window_days: int,
) -> dict[str, Any]:
    scorecard = _mapping(candidate.get("scorecard"))
    selected = _mapping(scorecard.get("selected_window"))
    if selected:
        return selected
    return _mapping(_mapping(scorecard.get("windows")).get(str(selected_window_days)))


def _improvement_window_count(
    candidate: dict[str, Any],
    context: dict[str, Any],
    selected_window_days: int,
) -> int:
    scorecard = _mapping(candidate.get("scorecard"))
    windows = _mapping(scorecard.get("windows"))
    minimum_final_delta = _float_value(
        _mapping(context.get("thresholds")).get("minimum_final_equity_delta"),
        default=0.0,
    )
    if not windows:
        selected = _selected_candidate_scorecard(candidate, selected_window_days)
        status = _string_value(selected.get("evaluation_status")) or _string_value(
            candidate.get("evaluation_status")
        )
        final_delta = _first_known_float(
            _mapping(selected.get("metrics")).get("final_equity_delta"),
            selected.get("final_equity_delta"),
        )
        return int(
            _has_improvement_signal(
                candidate_evaluation_status=status,
                final_equity_delta=final_delta,
                minimum_final_equity_delta=minimum_final_delta,
            )
        )
    count = 0
    for window in _list_mappings(list(windows.values())):
        metrics = _mapping(window.get("metrics"))
        status = _string_value(window.get("evaluation_status"))
        final_delta = _first_known_float(
            metrics.get("final_equity_delta"),
            window.get("final_equity_delta"),
        )
        if _has_improvement_signal(
            candidate_evaluation_status=status,
            final_equity_delta=final_delta,
            minimum_final_equity_delta=minimum_final_delta,
        ):
            count += 1
    return count


def _has_improvement_signal(
    *,
    candidate_evaluation_status: str,
    final_equity_delta: float | None,
    minimum_final_equity_delta: float,
) -> bool:
    if candidate_evaluation_status == "CANDIDATE_PROMISING_BUT_LIMITED":
        return True
    if candidate_evaluation_status == "NO_CLEAR_IMPROVEMENT":
        return False
    return final_equity_delta is not None and final_equity_delta > minimum_final_equity_delta


def _improvement_summary(
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_evaluation_status": metrics.get("candidate_evaluation_status"),
        "final_equity_delta": metrics.get("final_equity_delta"),
        "minimum_final_equity_delta": _float_value(
            thresholds.get("minimum_final_equity_delta"),
            default=0.0,
        ),
        "improvement_window_count": metrics.get("improvement_window_count"),
        "minimum_improvement_windows": metrics.get("minimum_improvement_windows"),
        "has_improvement_signal": bool(metrics.get("has_improvement_signal")),
        "stable_improvement_signal": bool(metrics.get("stable_improvement_signal")),
        "paper_signal_quality_status": metrics.get("paper_signal_quality_status"),
        "shadow_impact_status": metrics.get("shadow_impact_status"),
    }


def _risk_delta_summary(metrics: dict[str, Any]) -> dict[str, Any]:
    max_drawdown_delta = _optional_float(metrics.get("max_drawdown_delta"))
    exposure_delta = _optional_float(metrics.get("exposure_delta"))
    concentration_delta = _optional_float(metrics.get("concentration_delta"))
    return {
        "max_drawdown_delta": max_drawdown_delta,
        "exposure_delta": exposure_delta,
        "concentration_delta": concentration_delta,
        "max_drawdown_worse": max_drawdown_delta is not None and max_drawdown_delta < 0,
        "exposure_worse": exposure_delta is not None and exposure_delta > 0,
        "concentration_worse": concentration_delta is not None and concentration_delta > 0,
    }


def _data_quality_summary(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_signal_sample_count": metrics.get("paper_signal_sample_count"),
        "shadow_sample_count": metrics.get("shadow_sample_count"),
        "production_baseline_count": metrics.get("production_baseline_count"),
        "filled_count": metrics.get("filled_count"),
        "synthetic_snapshot_ratio": metrics.get("synthetic_snapshot_ratio"),
        "historical_ohlc_coverage": metrics.get("historical_ohlc_coverage"),
        "reconciliation_pass_ratio": metrics.get("reconciliation_pass_ratio"),
        "continuous_replay_available": bool(metrics.get("continuous_replay_available")),
        "replay_mode": metrics.get("replay_mode"),
        "data_gate_status": metrics.get("data_gate_status"),
        "major_risk_event_warning": bool(metrics.get("major_risk_event_warning")),
    }


def _required_manual_review_items(blockers: list[str]) -> list[str]:
    items = [
        "review_source_and_target_weight_profile",
        "verify_data_quality_report",
        "verify_continuous_replay_evidence",
        "review_risk_delta_summary",
        "confirm_production_profile_unchanged",
        "record_owner_decision_before_any_future_change",
    ]
    if blockers:
        items.append("resolve_or_document_gate_blockers_before_review")
    return _unique_strings(items)


def _recommendation(status: str, blockers: list[str]) -> dict[str, str]:
    if status == PROMO_READY_FOR_MANUAL_REVIEW:
        return {
            "action": RECOMMENDATION_MANUAL_REVIEW,
            "rationale": "证据达到 policy gate，可准备人工复核材料；仍不改变 production profile。",
        }
    if status == PROMO_PROMISING_LIMITED:
        return {
            "action": RECOMMENDATION_CONTINUE_OBSERVATION,
            "rationale": "candidate 有改善迹象，但稳定性不足，继续观察。",
        }
    if status in {PROMO_INSUFFICIENT_DATA, PROMO_NO_CLEAR_IMPROVEMENT}:
        return {
            "action": RECOMMENDATION_COLLECT_EVIDENCE,
            "rationale": "先补足证据或等待更清晰的 production 对比改善。",
        }
    if blockers:
        return {
            "action": RECOMMENDATION_DO_NOT_ADVANCE,
            "rationale": "promotion gate 存在阻断项，不能进入人工复核。",
        }
    return {
        "action": RECOMMENDATION_CONTINUE_OBSERVATION,
        "rationale": "保持只读观察，不改变任何 production 参数。",
    }


def _manual_required_check(
    candidate: dict[str, Any],
    adjustment_candidate: dict[str, Any],
) -> dict[str, Any]:
    passed = _has_manual_approval_required(candidate) or _has_manual_approval_required(
        adjustment_candidate
    )
    return {
        "check_id": "manual_approval_required_preserved",
        "status": "PASS" if passed else "FAIL",
        "observed": passed,
        "threshold": True,
        "operator": "is",
        "reason_code": "" if passed else BLOCKER_MANUAL_APPROVAL_MISSING,
    }


def _hard_candidate_blocked_check(
    candidate: dict[str, Any],
    adjustment_candidate: dict[str, Any],
) -> dict[str, Any]:
    hard_blocked = _hard_candidate_blocked(candidate) or _hard_candidate_blocked(
        adjustment_candidate
    )
    return {
        "check_id": "candidate_not_hard_blocked",
        "status": "FAIL" if hard_blocked else "PASS",
        "observed": hard_blocked,
        "threshold": False,
        "operator": "is",
        "reason_code": BLOCKER_CANDIDATE_BLOCKED if hard_blocked else "",
    }


def _data_gate_check(metrics: dict[str, Any]) -> dict[str, Any]:
    status = _string_value(metrics.get("data_gate_status")) or "missing"
    failed = status == "BLOCK"
    return {
        "check_id": "data_gate_not_block",
        "status": "FAIL" if failed else "PASS",
        "observed": status,
        "threshold": "not BLOCK",
        "operator": "!=",
        "reason_code": BLOCKER_DATA_GATE_BLOCK if failed else "",
    }


def _major_risk_event_check(metrics: dict[str, Any]) -> dict[str, Any]:
    warning = bool(metrics.get("major_risk_event_warning"))
    return {
        "check_id": "major_risk_event_warning_absent",
        "status": "FAIL" if warning else "PASS",
        "observed": warning,
        "threshold": False,
        "operator": "is",
        "reason_code": BLOCKER_MAJOR_RISK_EVENT_WARNING if warning else "",
    }


def _improvement_check(metrics: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    has_signal = bool(metrics.get("has_improvement_signal"))
    stable_signal = bool(metrics.get("stable_improvement_signal"))
    if stable_signal:
        status = "PASS"
        reason_code = ""
    elif has_signal:
        status = "FAIL"
        reason_code = BLOCKER_IMPROVEMENT_LIMITED
    else:
        status = "FAIL"
        reason_code = BLOCKER_NO_CLEAR_IMPROVEMENT
    return {
        "check_id": "stable_improvement_signal",
        "status": status,
        "observed": metrics.get("improvement_window_count"),
        "threshold": _int_value(thresholds.get("minimum_improvement_windows"), default=2),
        "operator": ">=",
        "reason_code": reason_code,
    }


def _has_manual_approval_required(candidate: dict[str, Any]) -> bool:
    blockers = {value.lower() for value in _strings(candidate.get("blocked_by"))}
    validations = {value.lower() for value in _strings(candidate.get("required_validations"))}
    recommendation = _mapping(candidate.get("recommendation"))
    action = _string_value(recommendation.get("action")).lower()
    return bool(
        blockers & MANUAL_APPROVAL_MARKERS
        or validations & MANUAL_APPROVAL_MARKERS
        or action == RECOMMENDATION_MANUAL_REVIEW
    )


def _hard_candidate_blocked(candidate: dict[str, Any]) -> bool:
    blockers = set(_canonical_blockers(_strings(candidate.get("blocked_by"))))
    non_manual_blockers = blockers - {"manual_approval_required"}
    if BLOCKER_CANDIDATE_BLOCKED in blockers:
        return True
    if bool(candidate.get("blocked")) and non_manual_blockers:
        return True
    if bool(candidate.get("blocked")) and not blockers:
        return True
    return False


def _context_warnings(
    selected_window: dict[str, Any],
    replay: dict[str, Any],
    daily_summary: dict[str, Any],
) -> list[str]:
    warnings = [*_strings(selected_window.get("warnings"))]
    if not bool(replay.get("available")):
        warnings.append(BLOCKER_CONTINUOUS_REPLAY_MISSING)
    if _major_risk_warning(daily_summary):
        warnings.append(BLOCKER_MAJOR_RISK_EVENT_WARNING)
    return _unique_strings(warnings)


def _continuous_replay_context(
    *,
    replay_payload: dict[str, Any],
    shadow_impact: dict[str, Any],
    evaluation_payload: dict[str, Any],
) -> dict[str, Any]:
    if replay_payload.get("report_type") == "paper_trading_replay":
        return _continuous_replay_from_replay(replay_payload)
    for payload in (shadow_impact, evaluation_payload):
        direct = _mapping(payload.get("continuous_replay"))
        if direct:
            return _normalize_continuous_replay(direct)
    windows = _mapping(evaluation_payload.get("windows"))
    for key in ("30", "14", "7"):
        window = _mapping(windows.get(key))
        if window.get("continuous_replay_available"):
            return _normalize_continuous_replay(
                {
                    "available": True,
                    "replay_mode": window.get("replay_mode") or "continuous_portfolio",
                    "portfolio_carry_forward": True,
                }
            )
    return _normalize_continuous_replay({})


def _continuous_replay_from_replay(replay_payload: dict[str, Any]) -> dict[str, Any]:
    replay_mode = _string_value(replay_payload.get("replay_mode")) or "daily_independent"
    carry_forward = _bool_value(replay_payload.get("portfolio_carry_forward"))
    profiles = _mapping(replay_payload.get("profile_results")) or _mapping(
        replay_payload.get("profiles")
    )
    if not profiles:
        profiles = {
            "unknown": {
                "final_equity": replay_payload.get("final_equity"),
                "max_drawdown_pct": _max_drawdown_pct(replay_payload),
                "exposure_peak": replay_payload.get("exposure_peak"),
                "max_position_concentration": replay_payload.get("max_position_concentration"),
            }
        }
    return _normalize_continuous_replay(
        {
            "available": replay_mode == "continuous_portfolio" and carry_forward,
            "replay_mode": replay_mode,
            "portfolio_carry_forward": carry_forward,
            "profiles": profiles,
            "start": _string_value(replay_payload.get("start")),
            "end": _string_value(replay_payload.get("end")),
        }
    )


def _normalize_continuous_replay(payload: dict[str, Any]) -> dict[str, Any]:
    source_artifact = _mapping(payload.get("source_artifact"))
    return {
        "available": _bool_value(payload.get("available")),
        "replay_mode": (
            _string_value(payload.get("replay_mode"))
            or _string_value(source_artifact.get("mode"))
            or "missing"
        ),
        "portfolio_carry_forward": _bool_value(payload.get("portfolio_carry_forward")),
        "profiles": _mapping(payload.get("profiles")),
        "path": _string_value(payload.get("path")) or _string_value(source_artifact.get("path")),
        "start": _string_value(payload.get("start")),
        "end": _string_value(payload.get("end")),
    }


def _replay_metrics(replay: dict[str, Any]) -> dict[str, float | None]:
    profiles = _mapping(replay.get("profiles"))
    production = _mapping(profiles.get("production"))
    shadow = _mapping(profiles.get("shadow"))
    return {
        "final_equity_delta": _delta(shadow.get("final_equity"), production.get("final_equity")),
        "max_drawdown_delta": _delta(
            _max_drawdown_pct(shadow),
            _max_drawdown_pct(production),
        ),
        "exposure_delta": _delta(
            _first_present(shadow, ("exposure_peak", "max_exposure", "exposure")),
            _first_present(production, ("exposure_peak", "max_exposure", "exposure")),
        ),
        "concentration_delta": _delta(
            _first_present(
                shadow,
                ("max_position_concentration", "position_concentration_peak", "concentration"),
            ),
            _first_present(
                production,
                ("max_position_concentration", "position_concentration_peak", "concentration"),
            ),
        ),
    }


def _major_risk_warning(daily_summary: dict[str, Any]) -> bool:
    if not daily_summary:
        return False
    direct = daily_summary.get("major_risk_event_warning")
    if isinstance(direct, bool):
        return direct
    warnings = _strings(daily_summary.get("warnings"))
    warnings.extend(_strings(_mapping(daily_summary.get("risk_events")).get("warnings")))
    warnings.extend(_strings(_mapping(daily_summary.get("risk_summary")).get("warnings")))
    warning_text = " ".join(warnings).lower()
    return "major_risk_event" in warning_text or "major risk" in warning_text


def _candidate_records(
    payload: dict[str, Any], *, expected_report_type: str
) -> list[dict[str, Any]]:
    if payload.get("report_type") != expected_report_type:
        return []
    return _list_mappings(payload.get("candidates"))


def _main_blocked_by(candidates: list[dict[str, Any]], context: dict[str, Any]) -> str:
    counter: Counter[str] = Counter()
    for candidate in candidates:
        counter.update(_strings(candidate.get("blocked_by")))
    if counter:
        return counter.most_common(1)[0][0]
    base = _strings(context.get("base_blockers"))
    return base[0] if base else "none"


def _top_candidate_id(
    candidates_payload: dict[str, Any],
    evaluation_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str:
    ready = [
        candidate
        for candidate in candidates
        if candidate.get("promotion_gate_status") == PROMO_READY_FOR_MANUAL_REVIEW
    ]
    if ready:
        return _string_value(ready[0].get("candidate_id"))
    for payload in (evaluation_payload, candidates_payload):
        summary = _mapping(payload.get("summary"))
        top = _string_value(summary.get("top_candidate_id")) or _string_value(
            payload.get("top_candidate_id")
        )
        if top:
            return top
    return _string_value(candidates[0].get("candidate_id")) if candidates else ""


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "weight_promotion_gate_policy",
        "version": policy.get("version", "missing"),
        "status": _string_value(policy.get("status")) or "missing",
        "owner": _string_value(policy.get("owner")) or "missing",
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "path": str(policy_path),
        "rationale": _string_value(policy.get("rationale")),
        "intended_effect": _string_value(policy.get("intended_effect")),
        "validation_evidence": _string_value(policy.get("validation_evidence")),
        "review_condition": _string_value(policy.get("review_condition")),
        "thresholds": dict(_mapping(policy.get("thresholds"))),
        "required_validations": _strings(policy.get("required_validations")),
    }


def _metadata_record(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "exists": bool(payload),
        "policy_id": _string_value(payload.get("policy_id")),
        "version": payload.get("version", ""),
        "status": _string_value(payload.get("status")),
        "owner": _string_value(payload.get("owner")),
        "production_effect": _string_value(payload.get("production_effect")) or "unknown",
    }


def _artifact_record(
    path: Path | None,
    base_dir: Path,
    *,
    expected_report_type: str | None = None,
    payload: dict[str, Any] | None = None,
    optional: bool = False,
) -> dict[str, Any]:
    if path is None:
        return {
            "path": "",
            "exists": False,
            "optional": optional,
            "href": "",
            "checksum_sha256": "",
            "valid": optional,
        }
    exists = path.exists()
    checksum = _sha256(path) if exists and path.is_file() else ""
    actual_type = _string_value((payload or {}).get("report_type"))
    valid = exists and (expected_report_type is None or actual_type == expected_report_type)
    if optional and not exists:
        valid = True
    return {
        "path": str(path),
        "exists": exists,
        "optional": optional,
        "href": _report_href(path, base_dir),
        "checksum_sha256": checksum,
        "expected_report_type": expected_report_type or "",
        "actual_report_type": actual_type,
        "valid": valid,
    }


def _minimum_check(
    check_id: str,
    observed: int,
    threshold: int,
    reason_code: str,
) -> dict[str, Any]:
    passed = observed >= threshold
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if passed else reason_code,
    }


def _minimum_ratio_check(
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    passed = observed >= threshold
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if passed else reason_code,
    }


def _maximum_check(
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    passed = observed <= threshold
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": "<=",
        "reason_code": "" if passed else reason_code,
    }


def _minimum_optional_check(
    check_id: str,
    observed: float | None,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    if observed is None:
        return {
            "check_id": check_id,
            "status": "FAIL",
            "observed": None,
            "threshold": threshold,
            "operator": ">=",
            "reason_code": reason_code,
        }
    return _minimum_ratio_check(check_id, observed, threshold, reason_code)


def _maximum_optional_check(
    check_id: str,
    observed: float | None,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    if observed is None:
        return {
            "check_id": check_id,
            "status": "FAIL",
            "observed": None,
            "threshold": threshold,
            "operator": "<=",
            "reason_code": reason_code,
        }
    return _maximum_check(check_id, observed, threshold, reason_code)


def _assert_allowed_statuses(payload: dict[str, Any]) -> None:
    top_status = _string_value(payload.get("promotion_gate_status"))
    if top_status not in ALLOWED_PROMOTION_GATE_STATUSES:
        raise ValueError(f"unsupported weight promotion gate status: {top_status}")
    for candidate in _list_mappings(payload.get("candidates")):
        status = _string_value(candidate.get("promotion_gate_status"))
        if status not in ALLOWED_PROMOTION_GATE_STATUSES:
            raise ValueError(f"unsupported candidate promotion gate status: {status}")


def _assert_forbidden_terms_absent(payload: dict[str, Any], markdown: str) -> None:
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown
    for term in FORBIDDEN_PROMOTION_TERMS:
        if term in combined:
            raise ValueError(f"forbidden promotion term present in output: {term}")


def _select_latest_replay_path(reports_dir: Path, as_of: date) -> Path | None:
    selected: tuple[date, datetime, str, Path] | None = None
    for path in reports_dir.glob("paper_trading_replay_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_trading_replay":
            continue
        end = _parse_iso_date(_string_value(payload.get("end")))
        if end is None:
            match = re.search(
                r"paper_trading_replay_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})",
                path.name,
            )
            end = date.fromisoformat(match.group(2)) if match else None
        if end is None or end > as_of:
            continue
        generated = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidate = (end, generated, path.name, path)
        if selected is None or candidate > selected:
            selected = candidate
    return selected[3] if selected else None


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_href(path: Path, base_dir: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(base_dir.resolve()).as_posix()
        except (OSError, RuntimeError, ValueError):
            return path.as_posix()


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_datetime(value: str) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    return []


def _unique_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _canonical_blockers(blockers: list[str]) -> list[str]:
    canonical: list[str] = []
    for blocker in blockers:
        value = str(blocker)
        if value in {"LOW_DATA_QUALITY", "LIMITED_MARKET_DATA"}:
            canonical.append(BLOCKER_LOW_DATA_QUALITY)
        elif value in {"UNRELIABLE", "UNRELIABLE_EXECUTION_STATE"}:
            canonical.append(BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE)
        elif value in {"continuous_replay_missing", "DAILY_INDEPENDENT_ONLY"}:
            canonical.append(BLOCKER_CONTINUOUS_REPLAY_MISSING)
        elif value in {"shadow_impact_insufficient", "insufficient_shadow_sample"}:
            canonical.append(BLOCKER_SHADOW_IMPACT_INSUFFICIENT)
        elif value in {"insufficient_sample", "INSUFFICIENT_SAMPLE"}:
            canonical.append(BLOCKER_INSUFFICIENT_SAMPLE)
        elif value in {"candidate_blocked", "CANDIDATE_BLOCKED"}:
            canonical.append(BLOCKER_CANDIDATE_BLOCKED)
        else:
            canonical.append(value)
    return canonical


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_value(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_value(value: Any, *, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _optional_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_known_float(*values: Any) -> float | None:
    for value in values:
        parsed = _optional_float(value)
        if parsed is not None:
            return parsed
    return None


def _first_known_int(*values: Any) -> int:
    for value in values:
        parsed = _optional_float(value)
        if parsed is not None:
            return int(parsed)
    return 0


def _max_known_int(*values: Any) -> int:
    known = [_first_known_int(value) for value in values if _optional_float(value) is not None]
    return max(known) if known else 0


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _delta(candidate_value: Any, production_value: Any) -> float | None:
    candidate_number = _optional_float(candidate_value)
    production_number = _optional_float(production_value)
    if candidate_number is None or production_number is None:
        return None
    return candidate_number - production_number


def _max_drawdown_pct(payload: dict[str, Any]) -> float | None:
    value = _optional_float(payload.get("max_drawdown_pct"))
    if value is not None:
        return value
    return _optional_float(_mapping(payload.get("max_drawdown")).get("percent"))


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _max_known(*values: float | None) -> float:
    known = [value for value in values if value is not None]
    return max(known) if known else 0.0


def _min_known(*values: float | None) -> float:
    known = [value for value in values if value is not None]
    return min(known) if known else 0.0


def _json_inline(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
