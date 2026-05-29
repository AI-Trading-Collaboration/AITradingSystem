from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

WEIGHT_CANDIDATE_EVALUATION_SCHEMA_VERSION = 1
WEIGHT_CANDIDATE_EVALUATION_REPORT_TYPE = "weight_candidate_evaluation"
WEIGHT_CANDIDATE_EVALUATION_WINDOWS: tuple[int, ...] = (7, 14, 30)
EVALUATION_MODE_OBSERVE_ONLY = "observe_only"
PRODUCTION_EFFECT_NONE = "none"
STATUS_LIMITED = "LIMITED"
STATUS_BLOCKED = "BLOCKED"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_WEIGHT_CANDIDATE_EVALUATION_POLICY_PATH = (
    REPO_ROOT / "config" / "weight_candidate_evaluation_policy.yaml"
)
DEFAULT_PARAMETER_GOVERNANCE_PATH = REPO_ROOT / "config" / "parameter_governance.yaml"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"
DEFAULT_SHADOW_PROFILES_PATH = REPO_ROOT / "config" / "weights" / "shadow_weight_profiles.yaml"

EVAL_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
EVAL_OBSERVE_ONLY = "OBSERVE_ONLY"
EVAL_PROMISING_LIMITED = "CANDIDATE_PROMISING_BUT_LIMITED"
EVAL_NO_CLEAR_IMPROVEMENT = "NO_CLEAR_IMPROVEMENT"
EVAL_UNRELIABLE = "CANDIDATE_UNRELIABLE"
EVAL_LOW_DATA_QUALITY = "LOW_DATA_QUALITY"
ALLOWED_EVALUATION_STATUSES = {
    EVAL_INSUFFICIENT_DATA,
    EVAL_OBSERVE_ONLY,
    EVAL_PROMISING_LIMITED,
    EVAL_NO_CLEAR_IMPROVEMENT,
    EVAL_UNRELIABLE,
    EVAL_LOW_DATA_QUALITY,
}

BLOCKER_MISSING_CANDIDATES = "missing_weight_adjustment_candidates"
BLOCKER_CANDIDATE_BLOCKED = "candidate_blocked"
BLOCKER_MANUAL_APPROVAL = "manual_approval_required"
BLOCKER_INSUFFICIENT_SAMPLE = "insufficient_sample"
BLOCKER_LOW_DATA_QUALITY = "low_data_quality"
BLOCKER_SYNTHETIC_RATIO_HIGH = "synthetic_snapshot_ratio_too_high"
BLOCKER_CONTINUOUS_REPLAY_MISSING = "continuous_replay_missing"
BLOCKER_SHADOW_IMPACT_INSUFFICIENT = "shadow_impact_insufficient"
BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE = "paper_signal_quality_unreliable"
BLOCKER_RECONCILIATION_UNRELIABLE = "reconciliation_unreliable"
BLOCKER_MAX_DRAWDOWN_WORSE = "max_drawdown_worse"
BLOCKER_EXPOSURE_WORSE = "exposure_worse"
BLOCKER_CONCENTRATION_WORSE = "concentration_worse"

BLOCKER_EXPLANATIONS = {
    BLOCKER_MISSING_CANDIDATES: "缺少同日 weight adjustment candidates JSON。",
    BLOCKER_CANDIDATE_BLOCKED: "上游 candidate 仍处于 blocked 状态。",
    BLOCKER_MANUAL_APPROVAL: "候选权重评估后仍只能进入人工复核。",
    BLOCKER_INSUFFICIENT_SAMPLE: "paper / shadow / production baseline 样本低于 policy floor。",
    BLOCKER_LOW_DATA_QUALITY: "数据质量不足以支持候选权重解释。",
    BLOCKER_SYNTHETIC_RATIO_HIGH: "synthetic snapshot ratio 高于 policy 上限。",
    BLOCKER_CONTINUOUS_REPLAY_MISSING: "缺少 continuous portfolio replay 证据。",
    BLOCKER_SHADOW_IMPACT_INSUFFICIENT: "shadow impact 证据不足。",
    BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE: "paper signal quality 仍不可靠。",
    BLOCKER_RECONCILIATION_UNRELIABLE: "portfolio reconciliation PASS 比例不足。",
    BLOCKER_MAX_DRAWDOWN_WORSE: "shadow replay max drawdown 差于 production baseline。",
    BLOCKER_EXPOSURE_WORSE: "shadow replay exposure 高于 production baseline。",
    BLOCKER_CONCENTRATION_WORSE: "shadow replay concentration 高于 production baseline。",
}

RECOMMENDATION_MANUAL_REVIEW = "manual_review_only"
RECOMMENDATION_CONTINUE_OBSERVATION = "continue_observation"
RECOMMENDATION_COLLECT_EVIDENCE = "collect_more_evidence"
RECOMMENDATION_DO_NOT_ADVANCE = "do_not_advance"


def default_weight_candidate_evaluation_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"weight_candidate_evaluation_{as_of.isoformat()}.json"


def build_weight_candidate_evaluation_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_CANDIDATE_EVALUATION_POLICY_PATH,
    weight_adjustment_candidates_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    continuous_replay_summary_path: Path | None = None,
    parameter_governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_profiles_path: Path = DEFAULT_SHADOW_PROFILES_PATH,
    daily_decision_summary_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if selected_window_days not in set(WEIGHT_CANDIDATE_EVALUATION_WINDOWS):
        raise ValueError("selected_window_days must be one of 7, 14, or 30")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_weight_candidate_evaluation_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    suffix = as_of.isoformat()
    resolved_candidates_path = weight_adjustment_candidates_path or (
        reports_dir / f"weight_adjustment_candidates_{suffix}.json"
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
    paper_quality = _read_json_object(resolved_paper_path)
    shadow_impact = _read_json_object(resolved_shadow_path)
    replay_payload = _read_json_object(selected_replay_path)
    continuous_summary = _read_json_object(continuous_replay_summary_path)
    parameter_governance = _load_yaml_object(parameter_governance_path)
    production_profile = _load_yaml_object(production_profile_path)
    shadow_profiles = _load_yaml_object(shadow_profiles_path)
    daily_summary = _read_json_object(resolved_daily_path)

    candidate_records = _candidate_records(candidates_payload)
    replay = _continuous_replay_context(
        replay_payload=replay_payload,
        shadow_impact=shadow_impact,
        continuous_summary=continuous_summary,
    )
    window_contexts = {
        str(days): _window_context(
            days=days,
            candidates_payload=candidates_payload,
            candidate_records=candidate_records,
            paper_quality=paper_quality,
            shadow_impact=shadow_impact,
            replay=replay,
            thresholds=thresholds,
        )
        for days in WEIGHT_CANDIDATE_EVALUATION_WINDOWS
    }
    candidate_evaluations = _candidate_evaluations(
        candidate_records=candidate_records,
        window_contexts=window_contexts,
        selected_window_days=selected_window_days,
    )
    windows = {
        key: _window_payload(context, candidate_evaluations, key)
        for key, context in window_contexts.items()
    }
    selected_window = windows[str(selected_window_days)]
    selected_candidate_records = _selected_candidate_records(candidate_evaluations)
    evaluation_status = _overall_evaluation_status(
        candidate_count=len(candidate_records),
        selected_window=selected_window,
        candidates=selected_candidate_records,
    )
    main_blocked_by = _main_blocked_by(
        selected_window=selected_window,
        candidates=selected_candidate_records,
    )
    top_candidate_id = _top_candidate_id(candidates_payload, candidate_records)
    status = (
        STATUS_LIMITED
        if candidates_payload.get("report_type") != "weight_adjustment_candidates"
        else STATUS_BLOCKED
    )
    policy_report = _policy_report(policy, policy_path)
    payload = {
        "schema_version": WEIGHT_CANDIDATE_EVALUATION_SCHEMA_VERSION,
        "report_type": WEIGHT_CANDIDATE_EVALUATION_REPORT_TYPE,
        "generated_at": generated.isoformat(),
        "as_of": suffix,
        "market_regime": "ai_after_chatgpt",
        "status": status,
        "evaluation_status": evaluation_status,
        "evaluation_mode": EVALUATION_MODE_OBSERVE_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "selected_window_days": selected_window_days,
        "policy_id": policy_report["policy_id"],
        "policy_version": policy_report["version"],
        "thresholds_snapshot": policy_report["thresholds"],
        "policy": policy_report,
        "evaluation_scope": {
            "observe_only": True,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "changes_production_parameters": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "runs_replay": False,
            "uses_existing_artifacts_only": True,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_ibkr": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
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
            "continuous_replay_summary": _artifact_record(
                continuous_replay_summary_path,
                reports_dir,
                payload=continuous_summary,
                optional=True,
            ),
            "daily_decision_summary": _artifact_record(
                resolved_daily_path,
                reports_dir,
                expected_report_type="daily_decision_summary",
                payload=daily_summary,
                optional=True,
            ),
            "parameter_governance": _artifact_record(parameter_governance_path, reports_dir),
            "production_profile": _artifact_record(production_profile_path, reports_dir),
            "shadow_profiles": _artifact_record(shadow_profiles_path, reports_dir, optional=True),
        },
        "metadata_inputs": {
            "parameter_governance": _metadata_record(parameter_governance),
            "production_profile": _metadata_record(production_profile),
            "shadow_profiles": _shadow_profile_metadata(shadow_profiles),
        },
        "summary": {
            "evaluation_status": evaluation_status,
            "status": status,
            "candidate_count": len(candidate_records),
            "evaluable_candidate_count": selected_window["evaluable_candidate_count"],
            "blocked_candidate_count": selected_window["blocked_candidate_count"],
            "insufficient_data_count": selected_window["insufficient_data_count"],
            "low_quality_data_count": selected_window["low_quality_data_count"],
            "top_candidate_id": top_candidate_id,
            "main_blocked_by": main_blocked_by,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "evaluation_mode": EVALUATION_MODE_OBSERVE_ONLY,
            "report_link": _report_href(output_md_path, reports_dir),
        },
        "windows": windows,
        "candidates": selected_candidate_records,
        "notes": [
            "本报告只做候选权重 observe-only 评估，不是自动调参。",
            "本报告不是 production promotion，不改变 production profile。",
            "本报告不触发 IBKR、PaperBroker、paper runner 或 replay runner。",
            "本报告不影响交易或 daily dashboard 主结论。",
            "即使 candidate 看起来更好，也只能进入 manual review。",
        ],
    }
    _assert_allowed_statuses(payload)
    return payload


def write_weight_candidate_evaluation_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_CANDIDATE_EVALUATION_POLICY_PATH,
    weight_adjustment_candidates_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    continuous_replay_summary_path: Path | None = None,
    parameter_governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_profiles_path: Path = DEFAULT_SHADOW_PROFILES_PATH,
    daily_decision_summary_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    selected_window_days: int = 30,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_weight_candidate_evaluation_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=policy_path,
        weight_adjustment_candidates_path=weight_adjustment_candidates_path,
        paper_signal_quality_path=paper_signal_quality_path,
        shadow_parameter_impact_path=shadow_parameter_impact_path,
        replay_json_path=replay_json_path,
        continuous_replay_summary_path=continuous_replay_summary_path,
        parameter_governance_path=parameter_governance_path,
        production_profile_path=production_profile_path,
        shadow_profiles_path=shadow_profiles_path,
        daily_decision_summary_path=daily_decision_summary_path,
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
    md_path.write_text(render_weight_candidate_evaluation_report(payload), encoding="utf-8")
    return payload


def render_weight_candidate_evaluation_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    thresholds = _mapping(payload.get("thresholds_snapshot"))
    windows = _mapping(payload.get("windows"))
    candidates = _list_mappings(payload.get("candidates"))
    lines = [
        "# Weight Candidate Evaluation",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- evaluation_status：`{payload.get('evaluation_status')}`",
        f"- evaluation_mode：`{payload.get('evaluation_mode')}`",
        "- production_effect=none",
        "- observe-only：true",
        "- 自动调参：否",
        "- production promotion：否",
        "- production profile 修改：无",
        "- 交易或 dashboard 主结论影响：无",
        "- 更好的 candidate 也只能进入 manual review。",
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
        "maximum_synthetic_snapshot_ratio",
        "minimum_historical_ohlc_coverage",
        "minimum_reconciliation_pass_ratio",
        "minimum_max_drawdown_delta",
        "maximum_exposure_delta",
        "maximum_concentration_delta",
    ):
        lines.append(f"| {key} | {thresholds.get(key, 'missing')} |")

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- candidate_count：{summary.get('candidate_count', 0)}",
            f"- evaluable_candidate_count：{summary.get('evaluable_candidate_count', 0)}",
            f"- blocked_candidate_count：{summary.get('blocked_candidate_count', 0)}",
            f"- insufficient_data_count：{summary.get('insufficient_data_count', 0)}",
            f"- low_quality_data_count：{summary.get('low_quality_data_count', 0)}",
            f"- top_candidate_id：`{summary.get('top_candidate_id', '')}`",
            f"- main_blocked_by：`{summary.get('main_blocked_by', 'none')}`",
            "",
            "## Windows",
            "",
            (
                "| Window | Status | Candidates | Evaluable | Blocked | Insufficient | "
                "Low quality | Continuous replay | Synthetic | OHLC | Reconciliation | "
                "Replay mode | Final equity delta | Max drawdown delta | Exposure delta | "
                "Concentration delta |"
            ),
            "|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---|---:|---:|---:|---:|",
        ]
    )
    for key in ("7", "14", "30"):
        window = _mapping(windows.get(key))
        lines.append(
            "| "
            f"{key}d | "
            f"{window.get('evaluation_status', EVAL_INSUFFICIENT_DATA)} | "
            f"{window.get('candidate_count', 0)} | "
            f"{window.get('evaluable_candidate_count', 0)} | "
            f"{window.get('blocked_candidate_count', 0)} | "
            f"{window.get('insufficient_data_count', 0)} | "
            f"{window.get('low_quality_data_count', 0)} | "
            f"{window.get('continuous_replay_available', False)} | "
            f"{_format_ratio(window.get('synthetic_snapshot_ratio'))} | "
            f"{_format_ratio(window.get('historical_ohlc_coverage'))} | "
            f"{_format_ratio(window.get('reconciliation_pass_ratio'))} | "
            f"{window.get('replay_mode', 'missing')} | "
            f"{_format_optional_number(window.get('final_equity_delta'))} | "
            f"{_format_optional_number(window.get('max_drawdown_delta'))} | "
            f"{_format_optional_number(window.get('exposure_delta'))} | "
            f"{_format_optional_number(window.get('concentration_delta'))} |"
        )

    lines.extend(
        [
            "",
            "## Candidate Scorecards",
            "",
            "| candidate_id | evaluation_status | blocked_by | recommendation |",
            "|---|---|---|---|",
        ]
    )
    for candidate in candidates:
        recommendation = _mapping(candidate.get("recommendation"))
        lines.append(
            "| "
            f"`{candidate.get('candidate_id', '')}` | "
            f"{candidate.get('evaluation_status', EVAL_INSUFFICIENT_DATA)} | "
            f"{', '.join(_strings(candidate.get('blocked_by'))) or 'none'} | "
            f"{recommendation.get('action', '')} |"
        )

    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- 这是候选权重的 observe-only 评估。",
            "- 不是自动调参，不是 production promotion。",
            "- 不改变 `config/weights/weight_profile_current.yaml`。",
            "- 不触发 IBKR、PaperBroker、paper runner 或 replay runner。",
            "- 不影响交易或 daily dashboard 主结论。",
            "- 即使 candidate 看起来更好，也只能进入 manual review。",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_evaluations(
    *,
    candidate_records: list[dict[str, Any]],
    window_contexts: dict[str, dict[str, Any]],
    selected_window_days: int,
) -> list[dict[str, Any]]:
    evaluations: list[dict[str, Any]] = []
    for candidate in candidate_records:
        window_scorecards = {
            key: _evaluate_candidate_for_window(candidate, context)
            for key, context in window_contexts.items()
        }
        selected = window_scorecards[str(selected_window_days)]
        source_profile = _mapping(candidate.get("source_profile"))
        target_profile = _mapping(candidate.get("target_profile"))
        evaluations.append(
            {
                "candidate_id": _string_value(candidate.get("candidate_id")),
                "source_profile": dict(source_profile),
                "target_profile": dict(target_profile),
                "parameter_changes": _list_mappings(candidate.get("parameter_changes")),
                "required_validations": _strings(candidate.get("required_validations")),
                "evaluation_status": selected["evaluation_status"],
                "blocked": bool(selected["blocked_by"]),
                "blocked_by": selected["blocked_by"],
                "warnings": selected["warnings"],
                "scorecard": {
                    "selected_window_days": selected_window_days,
                    "selected_window": selected,
                    "windows": window_scorecards,
                },
                "recommendation": _recommendation(selected),
                "production_effect": PRODUCTION_EFFECT_NONE,
            }
        )
    return evaluations


def _evaluate_candidate_for_window(
    candidate: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    candidate_blockers = _strings(candidate.get("blocked_by"))
    blockers = [*context["base_blockers"], *candidate_blockers]
    if bool(candidate.get("blocked")):
        blockers.append(BLOCKER_CANDIDATE_BLOCKED)
    if _requires_manual_review(candidate):
        blockers.append(BLOCKER_MANUAL_APPROVAL)
    blockers = _unique_strings(_canonical_blockers(blockers))
    warnings = _unique_strings([*context["warnings"], *_candidate_warnings(candidate)])
    evaluation_status = _candidate_status(
        blockers=blockers,
        context=context,
    )
    if evaluation_status not in ALLOWED_EVALUATION_STATUSES:
        raise ValueError(f"unsupported weight candidate evaluation status: {evaluation_status}")
    return {
        "window_days": context["window_days"],
        "evaluation_status": evaluation_status,
        "blocked": bool(blockers),
        "blocked_by": blockers,
        "warnings": warnings,
        "candidate_blocked_by": candidate_blockers,
        "metrics": context["metrics"],
        "checks": context["checks"],
        "reason_explanations": {
            blocker: BLOCKER_EXPLANATIONS.get(blocker, blocker) for blocker in blockers
        },
    }


def _window_context(
    *,
    days: int,
    candidates_payload: dict[str, Any],
    candidate_records: list[dict[str, Any]],
    paper_quality: dict[str, Any],
    shadow_impact: dict[str, Any],
    replay: dict[str, Any],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    paper_window = _window_payload_from(paper_quality, str(days))
    shadow_window = _window_payload_from(shadow_impact, str(days))
    paper_summary = _mapping(paper_window.get("summary")) or _mapping(paper_quality.get("summary"))
    shadow_summary = _mapping(shadow_window.get("summary")) or _mapping(
        shadow_impact.get("summary")
    )
    shadow_comparison = _mapping(shadow_window.get("profile_comparison")) or _mapping(
        shadow_impact.get("profile_comparison")
    )
    production_shadow = _mapping(shadow_comparison.get("production"))
    candidate_shadow = _mapping(shadow_comparison.get("shadow"))
    sample_counts = _mapping(shadow_summary.get("sample_counts"))
    paper_sample_count = _int_value(paper_summary.get("sample_count"))
    production_sample_count = _int_value(
        sample_counts.get("production"),
        default=_int_value(production_shadow.get("sample_count")),
    )
    shadow_sample_count = _int_value(
        sample_counts.get("shadow"),
        default=_int_value(candidate_shadow.get("sample_count")),
    )
    paper_status = _status_from_window(
        paper_window,
        paper_quality,
        field="evaluation_status",
    )
    shadow_status = _status_from_window(
        shadow_window,
        shadow_impact,
        field="impact_status",
    )
    synthetic_ratio = _max_known(
        _optional_float(paper_summary.get("synthetic_snapshot_ratio")),
        _optional_float(candidate_shadow.get("synthetic_snapshot_ratio")),
    )
    historical_coverage = _min_known(
        _optional_float(paper_summary.get("historical_ohlc_coverage")),
        _optional_float(candidate_shadow.get("historical_ohlc_coverage")),
    )
    reconciliation_ratio = _min_known(
        _optional_float(paper_summary.get("reconciliation_pass_ratio")),
        _optional_float(candidate_shadow.get("reconciliation_pass_ratio")),
    )
    replay_metrics = _replay_metrics(replay)
    metrics = {
        "paper_signal_sample_count": paper_sample_count,
        "production_baseline_count": production_sample_count,
        "shadow_sample_count": shadow_sample_count,
        "paper_signal_quality_status": paper_status or "missing",
        "shadow_impact_status": shadow_status or "missing",
        "continuous_replay_available": bool(replay.get("available")),
        "synthetic_snapshot_ratio": synthetic_ratio,
        "historical_ohlc_coverage": historical_coverage,
        "reconciliation_pass_ratio": reconciliation_ratio,
        "replay_mode": _string_value(replay.get("replay_mode")) or "missing",
        "max_drawdown_delta": replay_metrics["max_drawdown_delta"],
        "final_equity_delta": replay_metrics["final_equity_delta"],
        "exposure_delta": replay_metrics["exposure_delta"],
        "concentration_delta": replay_metrics["concentration_delta"],
    }
    checks, blockers = _window_checks(
        candidates_payload=candidates_payload,
        candidate_records=candidate_records,
        metrics=metrics,
        thresholds=thresholds,
    )
    warnings: list[str] = []
    if not metrics["continuous_replay_available"]:
        warnings.append(BLOCKER_CONTINUOUS_REPLAY_MISSING)
    return {
        "window_days": days,
        "base_blockers": blockers,
        "warnings": warnings,
        "checks": checks,
        "metrics": metrics,
    }


def _window_checks(
    *,
    candidates_payload: dict[str, Any],
    candidate_records: list[dict[str, Any]],
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    min_paper = _int_value(thresholds.get("minimum_paper_signal_sample_count"), default=7)
    min_shadow = _int_value(thresholds.get("minimum_shadow_sample_count"), default=7)
    min_production = _int_value(thresholds.get("minimum_production_baseline_count"), default=7)
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
            "candidate_file",
            1 if candidates_payload.get("report_type") == "weight_adjustment_candidates" else 0,
            1,
            BLOCKER_MISSING_CANDIDATES,
        ),
        _minimum_check("candidate_count", len(candidate_records), 1, BLOCKER_INSUFFICIENT_SAMPLE),
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
            BLOCKER_LOW_DATA_QUALITY,
        ),
        _minimum_ratio_check(
            "reconciliation_pass_ratio",
            _float_value(metrics.get("reconciliation_pass_ratio")),
            min_reconciliation,
            BLOCKER_RECONCILIATION_UNRELIABLE,
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
    ]
    blockers = [str(check["reason_code"]) for check in checks if check.get("status") == "FAIL"]
    paper_status = _string_value(metrics.get("paper_signal_quality_status"))
    shadow_status = _string_value(metrics.get("shadow_impact_status"))
    if paper_status in {"LOW_DATA_QUALITY"}:
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if paper_status in {"UNRELIABLE"}:
        blockers.append(BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE)
    if paper_status in {"INSUFFICIENT_DATA"}:
        blockers.append(BLOCKER_INSUFFICIENT_SAMPLE)
    if shadow_status in {"INSUFFICIENT_DATA"}:
        blockers.append(BLOCKER_SHADOW_IMPACT_INSUFFICIENT)
    if shadow_status in {"LOW_DATA_QUALITY"}:
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if shadow_status in {"SHADOW_UNRELIABLE"}:
        blockers.append(BLOCKER_SHADOW_IMPACT_INSUFFICIENT)
        blockers.append(BLOCKER_RECONCILIATION_UNRELIABLE)
    if not bool(metrics.get("continuous_replay_available")):
        blockers.append(BLOCKER_CONTINUOUS_REPLAY_MISSING)
    if BLOCKER_SYNTHETIC_RATIO_HIGH in blockers:
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    return checks, _unique_strings(blockers)


def _window_payload(
    context: dict[str, Any],
    candidate_evaluations: list[dict[str, Any]],
    window_key: str,
) -> dict[str, Any]:
    window_records = [
        _mapping(_mapping(_mapping(candidate.get("scorecard")).get("windows")).get(window_key))
        for candidate in candidate_evaluations
    ]
    statuses = [_string_value(record.get("evaluation_status")) for record in window_records]
    blocker_counter: Counter[str] = Counter()
    for record in window_records:
        blocker_counter.update(_strings(record.get("blocked_by")))
    if not candidate_evaluations:
        blocker_counter.update(_strings(context.get("base_blockers")))
    metrics = _mapping(context.get("metrics"))
    return {
        "window_days": context["window_days"],
        "evaluation_status": _window_evaluation_status(statuses, context),
        "candidate_count": len(candidate_evaluations),
        "evaluable_candidate_count": sum(_is_evaluable_status(status) for status in statuses),
        "blocked_candidate_count": sum(1 for record in window_records if record.get("blocked")),
        "insufficient_data_count": statuses.count(EVAL_INSUFFICIENT_DATA),
        "low_quality_data_count": statuses.count(EVAL_LOW_DATA_QUALITY),
        "continuous_replay_available": bool(metrics.get("continuous_replay_available")),
        "synthetic_snapshot_ratio": metrics.get("synthetic_snapshot_ratio"),
        "historical_ohlc_coverage": metrics.get("historical_ohlc_coverage"),
        "reconciliation_pass_ratio": metrics.get("reconciliation_pass_ratio"),
        "paper_signal_quality_status": metrics.get("paper_signal_quality_status"),
        "shadow_impact_status": metrics.get("shadow_impact_status"),
        "replay_mode": metrics.get("replay_mode"),
        "max_drawdown_delta": metrics.get("max_drawdown_delta"),
        "final_equity_delta": metrics.get("final_equity_delta"),
        "exposure_delta": metrics.get("exposure_delta"),
        "concentration_delta": metrics.get("concentration_delta"),
        "blocked_by": [value for value, _count in blocker_counter.most_common()],
        "main_blocked_by": blocker_counter.most_common(1)[0][0] if blocker_counter else "none",
        "warnings": _strings(context.get("warnings")),
        "checks": _list_mappings(context.get("checks")),
        "production_effect": PRODUCTION_EFFECT_NONE,
    }


def _candidate_status(*, blockers: list[str], context: dict[str, Any]) -> str:
    blocker_set = set(blockers)
    metrics = _mapping(context.get("metrics"))
    if blocker_set & {BLOCKER_LOW_DATA_QUALITY, BLOCKER_SYNTHETIC_RATIO_HIGH}:
        return EVAL_LOW_DATA_QUALITY
    if blocker_set & {
        BLOCKER_MISSING_CANDIDATES,
        BLOCKER_INSUFFICIENT_SAMPLE,
        BLOCKER_SHADOW_IMPACT_INSUFFICIENT,
    }:
        return EVAL_INSUFFICIENT_DATA
    if blocker_set & {
        BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE,
        BLOCKER_RECONCILIATION_UNRELIABLE,
        BLOCKER_MAX_DRAWDOWN_WORSE,
        BLOCKER_EXPOSURE_WORSE,
        BLOCKER_CONCENTRATION_WORSE,
    }:
        return EVAL_UNRELIABLE
    shadow_status = _string_value(metrics.get("shadow_impact_status"))
    final_delta = _optional_float(metrics.get("final_equity_delta"))
    if shadow_status == "SHADOW_PROMISING_BUT_LIMITED" or (
        final_delta is not None and final_delta > 0
    ):
        return EVAL_PROMISING_LIMITED
    if shadow_status == "NO_CLEAR_IMPROVEMENT":
        return EVAL_NO_CLEAR_IMPROVEMENT
    return EVAL_OBSERVE_ONLY


def _recommendation(scorecard: dict[str, Any]) -> dict[str, str]:
    status = _string_value(scorecard.get("evaluation_status"))
    blockers = _strings(scorecard.get("blocked_by"))
    if status == EVAL_PROMISING_LIMITED:
        return {
            "action": RECOMMENDATION_MANUAL_REVIEW,
            "rationale": "候选可进入人工复核，但不能自动修改 production 参数。",
        }
    if status in {EVAL_INSUFFICIENT_DATA, EVAL_LOW_DATA_QUALITY}:
        return {
            "action": RECOMMENDATION_COLLECT_EVIDENCE,
            "rationale": "先补足样本、数据质量或 continuous replay 证据。",
        }
    if status == EVAL_UNRELIABLE:
        return {
            "action": RECOMMENDATION_DO_NOT_ADVANCE,
            "rationale": "候选被可靠性或风险恶化 gate 阻断。",
        }
    if blockers:
        return {
            "action": RECOMMENDATION_CONTINUE_OBSERVATION,
            "rationale": "候选保持 observe-only，人工复核前不进入参数变更。",
        }
    return {
        "action": RECOMMENDATION_CONTINUE_OBSERVATION,
        "rationale": "当前没有可支持参数变更的清晰改善。",
    }


def _continuous_replay_context(
    *,
    replay_payload: dict[str, Any],
    shadow_impact: dict[str, Any],
    continuous_summary: dict[str, Any],
) -> dict[str, Any]:
    if replay_payload.get("report_type") == "paper_trading_replay":
        return _continuous_replay_from_replay(replay_payload)
    for payload in (continuous_summary, shadow_impact):
        direct = _mapping(payload.get("continuous_replay"))
        if direct:
            return _normalize_continuous_replay(direct)
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
            "path": _string_value(_mapping(replay_payload.get("outputs")).get("json")),
            "start": _string_value(replay_payload.get("start")),
            "end": _string_value(replay_payload.get("end")),
        }
    )


def _normalize_continuous_replay(payload: dict[str, Any]) -> dict[str, Any]:
    source_artifact = _mapping(payload.get("source_artifact"))
    replay_mode = (
        _string_value(payload.get("replay_mode"))
        or _string_value(source_artifact.get("mode"))
        or "missing"
    )
    profiles = _mapping(payload.get("profiles"))
    return {
        "available": _bool_value(payload.get("available")),
        "replay_mode": replay_mode,
        "portfolio_carry_forward": _bool_value(payload.get("portfolio_carry_forward")),
        "profiles": profiles,
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


def _selected_candidate_records(
    candidate_evaluations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return candidate_evaluations


def _overall_evaluation_status(
    *,
    candidate_count: int,
    selected_window: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str:
    if candidate_count == 0:
        return EVAL_INSUFFICIENT_DATA
    statuses = [_string_value(candidate.get("evaluation_status")) for candidate in candidates]
    if all(status == EVAL_LOW_DATA_QUALITY for status in statuses):
        return EVAL_LOW_DATA_QUALITY
    if all(status == EVAL_INSUFFICIENT_DATA for status in statuses):
        return EVAL_INSUFFICIENT_DATA
    for status in (
        EVAL_LOW_DATA_QUALITY,
        EVAL_UNRELIABLE,
        EVAL_PROMISING_LIMITED,
        EVAL_NO_CLEAR_IMPROVEMENT,
        EVAL_OBSERVE_ONLY,
        EVAL_INSUFFICIENT_DATA,
    ):
        if status in statuses:
            return status
    return _string_value(selected_window.get("evaluation_status")) or EVAL_INSUFFICIENT_DATA


def _window_evaluation_status(statuses: list[str], context: dict[str, Any]) -> str:
    if not statuses:
        blockers = set(_strings(context.get("base_blockers")))
        if blockers & {BLOCKER_LOW_DATA_QUALITY, BLOCKER_SYNTHETIC_RATIO_HIGH}:
            return EVAL_LOW_DATA_QUALITY
        return EVAL_INSUFFICIENT_DATA
    if EVAL_LOW_DATA_QUALITY in statuses:
        return EVAL_LOW_DATA_QUALITY
    if EVAL_UNRELIABLE in statuses:
        return EVAL_UNRELIABLE
    if EVAL_PROMISING_LIMITED in statuses:
        return EVAL_PROMISING_LIMITED
    if EVAL_NO_CLEAR_IMPROVEMENT in statuses:
        return EVAL_NO_CLEAR_IMPROVEMENT
    if EVAL_OBSERVE_ONLY in statuses:
        return EVAL_OBSERVE_ONLY
    return EVAL_INSUFFICIENT_DATA


def _is_evaluable_status(status: str) -> bool:
    return status in {
        EVAL_OBSERVE_ONLY,
        EVAL_PROMISING_LIMITED,
        EVAL_NO_CLEAR_IMPROVEMENT,
    }


def _main_blocked_by(
    *,
    selected_window: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str:
    counter: Counter[str] = Counter()
    for candidate in candidates:
        counter.update(_strings(candidate.get("blocked_by")))
    if counter:
        return counter.most_common(1)[0][0]
    return _string_value(selected_window.get("main_blocked_by")) or "none"


def _top_candidate_id(
    candidates_payload: dict[str, Any],
    candidate_records: list[dict[str, Any]],
) -> str:
    summary = _mapping(candidates_payload.get("summary"))
    return (
        _string_value(summary.get("top_candidate_id"))
        or _string_value(candidates_payload.get("top_candidate_id"))
        or (_string_value(candidate_records[0].get("candidate_id")) if candidate_records else "")
    )


def _candidate_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("report_type") != "weight_adjustment_candidates":
        return []
    return _list_mappings(payload.get("candidates"))


def _requires_manual_review(candidate: dict[str, Any]) -> bool:
    blockers = set(_strings(candidate.get("blocked_by")))
    validations = {value.lower() for value in _strings(candidate.get("required_validations"))}
    return (
        BLOCKER_MANUAL_APPROVAL in blockers
        or "manual_owner_review" in validations
        or "manual_review_required" in validations
    )


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
        else:
            canonical.append(value)
    return canonical


def _candidate_warnings(candidate: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if _string_value(candidate.get("production_effect")) not in {"", PRODUCTION_EFFECT_NONE}:
        warnings.append("candidate_production_effect_not_none")
    return warnings


def _window_payload_from(payload: dict[str, Any], window_key: str) -> dict[str, Any]:
    windows = _mapping(payload.get("windows"))
    return _mapping(windows.get(window_key))


def _status_from_window(window: dict[str, Any], payload: dict[str, Any], *, field: str) -> str:
    return _string_value(window.get(field)) or _string_value(payload.get(field))


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "weight_candidate_evaluation_policy",
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


def _shadow_profile_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    profiles = _list_mappings(payload.get("profiles"))
    return {
        "exists": bool(payload),
        "version": payload.get("version", ""),
        "status": _string_value(payload.get("status")),
        "profile_count": len(profiles),
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
    return {
        "check_id": check_id,
        "status": "PASS" if observed >= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if observed >= threshold else reason_code,
    }


def _minimum_ratio_check(
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if observed >= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": ">=",
        "reason_code": "" if observed >= threshold else reason_code,
    }


def _maximum_check(
    check_id: str,
    observed: float,
    threshold: float,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if observed <= threshold else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "operator": "<=",
        "reason_code": "" if observed <= threshold else reason_code,
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
            "status": "SKIPPED",
            "observed": None,
            "threshold": threshold,
            "operator": ">=",
            "reason_code": "",
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
            "status": "SKIPPED",
            "observed": None,
            "threshold": threshold,
            "operator": "<=",
            "reason_code": "",
        }
    return _maximum_check(check_id, observed, threshold, reason_code)


def _assert_allowed_statuses(payload: dict[str, Any]) -> None:
    for candidate in _list_mappings(payload.get("candidates")):
        status = _string_value(candidate.get("evaluation_status"))
        if status not in ALLOWED_EVALUATION_STATUSES:
            raise ValueError(f"unsupported candidate evaluation status: {status}")


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


def _load_yaml_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = safe_load_yaml_path(path) or {}
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


def _format_ratio(value: Any) -> str:
    number = _optional_float(value)
    if number is None:
        return "missing"
    return f"{number:.2%}"


def _format_optional_number(value: Any) -> str:
    number = _optional_float(value)
    if number is None:
        return "missing"
    return f"{number:.4f}"
