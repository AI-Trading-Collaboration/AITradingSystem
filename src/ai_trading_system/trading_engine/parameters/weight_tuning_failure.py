from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    default_weight_tuning_candidates_path,
    default_weight_tuning_root,
    latest_weight_tuning_path,
    load_weight_tuning_payload,
    weight_tuning_payload_date,
)

WEIGHT_TUNING_FAILURE_SCHEMA_VERSION = 1
WEIGHT_TUNING_FAILURE_REPORT_TYPE = "weight_tuning_failure"
WEIGHT_TUNING_FAILURE_ALIAS_REPORT_TYPE = "weight_tuning_failure_report"

# Diagnostic severity labels only; these do not change promotion or investment thresholds.
HIGH_FAILURE_RATIO = 0.60
MEDIUM_FAILURE_RATIO = 0.30
LOW_FAILURE_RATIO = 0.15
NEAR_MISS_LIMIT = 5

PERFORMANCE_FAILURE_REASONS = {
    "no_return_improvement",
    "sharpe_not_improved",
    "max_drawdown_worse",
    "turnover_too_high",
    "cost_drag_too_high",
    "risk_adjusted_performance_worse",
}
WALK_FORWARD_FAILURE_REASONS = {
    "insufficient_validation_windows",
    "non_worse_ratio_below_threshold",
    "improvement_concentrated_in_few_windows",
    "unstable_across_regimes",
    "worst_window_breaches_guardrail",
    "walk_forward_guardrail_failed",
}
GUARDRAIL_FAILURE_REASONS = {
    "drawdown_guardrail_failed",
    "return_guardrail_failed",
    "turnover_guardrail_failed",
    "walk_forward_guardrail_failed",
    "fallback_free_tuning_guardrail_failed",
    "production_safety_guardrail_failed",
}

RAW_GUARDRAIL_REASON_MAP = {
    "max_drawdown_worse_than_baseline_by_more_than_limit": "drawdown_guardrail_failed",
    "annualized_return_underperformance_more_than_limit": "return_guardrail_failed",
    "turnover_increase_more_than_limit": "turnover_guardrail_failed",
    "non_worse_walk_forward_ratio_below_minimum": "walk_forward_guardrail_failed",
    "fallback_signal_free_tuned": "fallback_free_tuning_guardrail_failed",
    "production_effect_not_none": "production_safety_guardrail_failed",
    "auto_promotion_true": "production_safety_guardrail_failed",
    "data_gate_not_ok": "data_gate_not_ok",
    "freshness_not_ok": "freshness_not_ok",
    "insufficient_validation_windows": "insufficient_validation_windows",
}


@dataclass(frozen=True)
class WeightTuningFailureRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    debug_path: Path | None = None


def default_weight_tuning_failure_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "weight_tuning_failure"


def default_weight_tuning_failure_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_weight_tuning_failure_json_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_failure_dir(output_root, as_of) / (
        "weight_tuning_failure_summary.json"
    )


def default_weight_tuning_failure_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_failure_dir(output_root, as_of) / (
        "weight_tuning_failure_summary.md"
    )


def default_weight_tuning_failure_debug_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_failure_dir(output_root, as_of) / (
        "weight_tuning_failure_candidates_debug.json"
    )


def latest_weight_tuning_failure_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_weight_tuning_failure_root()
    candidates = sorted(root.glob("*/weight_tuning_failure_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_weight_tuning_failure_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_weight_tuning_failure_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_tuning_failure_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"weight_tuning_failure_{as_of.isoformat()}.json",
        reports_dir / f"weight_tuning_failure_{as_of.isoformat()}.md",
    )


def run_weight_tuning_failure_attribution(
    *,
    as_of: date | None = None,
    summary_path: Path | None = None,
    output_root: Path | None = None,
    debug: bool = False,
    generated_at: datetime | None = None,
) -> WeightTuningFailureRun:
    generated = generated_at or datetime.now(tz=UTC)
    root = output_root or default_weight_tuning_failure_root()
    resolved_summary_path = _resolve_summary_path(as_of=as_of, summary_path=summary_path)
    resolved_as_of = _resolve_output_date(
        as_of=as_of,
        summary_path=resolved_summary_path or summary_path,
        generated_at=generated,
    )

    if resolved_summary_path is None or not resolved_summary_path.exists():
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_summary",
            summary_path=summary_path or resolved_summary_path,
            candidates_path=None,
            output_root=root,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    summary = load_weight_tuning_payload(resolved_summary_path)
    if not summary:
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_summary",
            summary_path=resolved_summary_path,
            candidates_path=None,
            output_root=root,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    resolved_as_of = _summary_date(summary, resolved_summary_path, fallback=resolved_as_of)
    candidates_path = _resolve_candidates_path(summary, resolved_summary_path, resolved_as_of)
    if candidates_path is None or not candidates_path.exists():
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_candidates",
            summary_path=resolved_summary_path,
            candidates_path=candidates_path,
            output_root=root,
            summary=summary,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    candidates_payload = load_weight_tuning_candidates(candidates_path)
    payload = build_weight_tuning_failure_payload(
        summary,
        candidates_payload,
        summary_path=resolved_summary_path,
        candidates_path=candidates_path,
        output_root=root,
        generated_at=generated,
    )
    return _write_run(payload, root, resolved_as_of, debug=debug)


def build_weight_tuning_failure_payload(
    summary: Mapping[str, Any],
    candidates_payload: Mapping[str, Any],
    *,
    summary_path: Path,
    candidates_path: Path,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    as_of = _summary_date(summary, summary_path, fallback=generated.date())
    root = output_root or default_weight_tuning_failure_root()
    output_artifacts = _output_artifacts(root, as_of)
    candidates = _records(candidates_payload.get("candidates"))
    search = _mapping(summary.get("search"))
    metadata = _mapping(summary.get("metadata"))
    recommended = _mapping(summary.get("recommended_candidate"))
    signal_quality = _mapping(summary.get("signal_quality"))
    guardrails = _mapping(recommended.get("guardrails"))
    relative = _candidate_relative(recommended)
    guardrail_policy = _mapping(summary.get("guardrail_policy"))
    data_failures = _data_failure_reasons(summary)
    signal_failures = _signal_failure_reasons(summary)
    search_failures = _search_failure_reasons(summary, candidates)

    candidate_results = [
        _candidate_failure_analysis(
            candidate,
            summary=summary,
            guardrail_policy=guardrail_policy,
            signal_failure_reasons=signal_failures,
        )
        for candidate in candidates
    ]
    rejection_summary = _candidate_rejection_summary(summary, candidate_results)
    failure_counter = _failure_counter(
        candidate_results,
        data_failures=data_failures,
        signal_failures=signal_failures,
        search_failures=search_failures,
    )
    failure_ranking = _failure_ranking(failure_counter, rejection_summary["total_candidates"])
    near_misses = _near_miss_candidates(candidate_results, guardrail_policy)
    walk_forward_failure = _walk_forward_failure_summary(
        candidate_results,
        summary,
        guardrail_policy,
    )
    guardrail_failure = _guardrail_failure_summary(candidate_results, guardrails)
    performance_failure = _performance_failure_summary(candidate_results)
    root_cause = _root_cause(
        failure_ranking,
        data_failures=data_failures,
        signal_failures=signal_failures,
        search_failures=search_failures,
        candidate_results=candidate_results,
    )
    next_action = _recommended_next_action(root_cause)
    detail_sufficient = _candidate_level_details_sufficient(candidate_results)
    status = "NO_CANDIDATE_EXPLAINED"
    if candidates and not detail_sufficient:
        root_cause = {
            "category": "mixed",
            "confidence": "LOW",
            "summary": (
                "候选级拒绝字段不足，当前只能确认没有候选通过，无法稳定分解失败分布。"
            ),
        }
        next_action = {
            "action": "improve_weight_tuning_candidate_diagnostics",
            "suggested_task": "TRADING-059B Candidate-level Weight Tuning Diagnostics",
            "reason": "candidate-level rejection details are insufficient.",
        }

    payload = {
        "schema_version": WEIGHT_TUNING_FAILURE_SCHEMA_VERSION,
        "report_type": WEIGHT_TUNING_FAILURE_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-tuning-failure-{as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_config_modified": False,
            "source_task": "TRADING-059A",
            "market_regime": metadata.get("market_regime", "ai_after_chatgpt"),
            "market_regime_anchor": metadata.get("market_regime_anchor", "2022-11-30"),
            "requested_date_range": metadata.get("requested_date_range", {}),
        },
        "input": {
            "weight_tuning_summary": str(summary_path),
            "weight_tuning_candidates": str(candidates_path),
            "signal_snapshot": _mapping(summary.get("inputs")).get("signal_snapshot", ""),
            "portfolio_candidates_summary": _latest_artifact_path(
                "portfolio_candidates",
                as_of,
                "portfolio_candidates_summary.json",
            ),
            "portfolio_candidate_tracking_summary": _latest_artifact_path(
                "portfolio_candidate_tracking",
                as_of,
                "portfolio_candidate_tracking_summary.json",
            ),
            "market_data_freshness_summary": _latest_artifact_path(
                "data_freshness",
                as_of,
                "market_data_freshness_summary.json",
            ),
            "backtest_input_manifest": _mapping(summary.get("inputs")).get(
                "backtest_input_manifest",
                _mapping(summary.get("data_quality")).get("backtest_manifest", ""),
            ),
            "weight_tuning_config": metadata.get("config_path", ""),
            "production_parameters": _mapping(summary.get("inputs")).get(
                "baseline_parameters",
                "config/parameters/production/current.yaml",
            ),
        },
        "inputs": {
            "weight_tuning_summary": str(summary_path),
            "weight_tuning_candidates": str(candidates_path),
        },
        "output_artifacts": output_artifacts,
        "tuning_result": {
            "result": metadata.get("status", "UNKNOWN"),
            "candidate_status": recommended.get("status", "UNKNOWN"),
            "guardrail_status": guardrails.get("status", "UNKNOWN"),
            "reason": recommended.get("reason", ""),
        },
        "candidate_rejection_summary": rejection_summary,
        "data_readiness_failure": {"reasons": data_failures},
        "signal_quality_failure": {
            "reasons": signal_failures,
            "status": signal_quality.get("status", "UNKNOWN"),
            "real_signals": _strings(signal_quality.get("real_signals")),
            "proxy_signals": _strings(signal_quality.get("proxy_signals")),
            "fallback_signals": _strings(signal_quality.get("fallback_signals")),
        },
        "search_space_failure": {
            "reasons": search_failures,
            "candidates_generated": search.get("candidates_generated", len(candidates)),
            "candidates_evaluated": search.get("candidates_evaluated", len(candidates)),
            "candidates_rejected_by_constraints": search.get(
                "candidates_rejected_by_constraints",
                0,
            ),
        },
        "performance_failure": performance_failure,
        "walk_forward_failure": walk_forward_failure,
        "guardrail_failure": guardrail_failure,
        "failure_ranking": failure_ranking,
        "near_miss_candidates": near_misses,
        "root_cause": root_cause,
        "recommended_next_action": next_action,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "No valid weight candidate exists. Production parameters remain unchanged."
            ),
        },
        "diagnostic_quality": {
            "candidate_level_details_available": detail_sufficient,
            "candidate_level_detail_fields": _candidate_detail_fields(candidates),
        },
        "safety": _safety_payload(),
    }
    if not candidates:
        payload["root_cause"] = {
            "category": "search_space_too_narrow",
            "confidence": "MEDIUM",
            "summary": "没有可评估候选；需要先复核受限搜索空间和约束过滤。",
        }
        payload["recommended_next_action"] = _recommended_next_action(payload["root_cause"])
    if not failure_ranking and relative:
        payload["failure_ranking"] = _failure_ranking(
            Counter(_performance_failure_reasons(recommended)),
            rejection_summary["total_candidates"],
        )
    return payload


def load_weight_tuning_candidates(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_weight_tuning_failure_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_weight_tuning_failure_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_weight_tuning_failure_debug(
    payload: Mapping[str, Any],
    debug_path: Path,
) -> Path:
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_payload = {
        "schema_version": WEIGHT_TUNING_FAILURE_SCHEMA_VERSION,
        "report_type": "weight_tuning_failure_candidates_debug",
        "metadata": _mapping(payload.get("metadata")),
        "failure_ranking": payload.get("failure_ranking", []),
        "near_miss_candidates": payload.get("near_miss_candidates", []),
        "safety": _safety_payload(),
    }
    debug_path.write_text(
        json.dumps(debug_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return debug_path


def write_weight_tuning_failure_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": WEIGHT_TUNING_FAILURE_ALIAS_REPORT_TYPE,
        "source_report_type": WEIGHT_TUNING_FAILURE_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_weight_tuning_failure_summary(alias_payload, json_path, markdown_path)


def load_weight_tuning_failure_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_weight_tuning_failure_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != WEIGHT_TUNING_FAILURE_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        WEIGHT_TUNING_FAILURE_REPORT_TYPE,
        WEIGHT_TUNING_FAILURE_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in ("run_id", "generated_at", "status", "production_effect"):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("status") not in {"NO_CANDIDATE_EXPLAINED", "BLOCKED"}:
        issues.append("status must be NO_CANDIDATE_EXPLAINED or BLOCKED")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if metadata.get("production_config_modified") is not False:
        issues.append("production_config_modified must be false")
    safety = _mapping(payload.get("safety"))
    for key in (
        "production_write_allowed",
        "production_config_modified",
        "candidate_promotion_triggered",
        "trading_action",
    ):
        if safety.get(key) is not False:
            issues.append(f"{key} must be false")
    can_support_promotion = _mapping(payload.get("promotion_impact")).get(
        "can_support_candidate_promotion"
    )
    if can_support_promotion is not False:
        issues.append("promotion_impact must not support candidate promotion")
    if metadata.get("status") == "NO_CANDIDATE_EXPLAINED":
        if not isinstance(payload.get("candidate_rejection_summary"), dict):
            issues.append("candidate_rejection_summary must be an object")
        if not isinstance(payload.get("root_cause"), dict):
            issues.append("root_cause must be an object")
        if not isinstance(payload.get("recommended_next_action"), dict):
            issues.append("recommended_next_action must be an object")
    return issues


def weight_tuning_failure_payload_date(payload: Mapping[str, Any], source_path: Path) -> date:
    raw_as_of = str(payload.get("as_of") or "")
    try:
        return date.fromisoformat(raw_as_of)
    except ValueError:
        pass
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("weight-tuning-failure-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer weight tuning failure date from {source_path}") from exc


def render_weight_tuning_failure_explanation(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    tuning = _mapping(payload.get("tuning_result"))
    rejection = _mapping(payload.get("candidate_rejection_summary"))
    root = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    guardrail = _mapping(payload.get("guardrail_failure"))
    ranking = _records(payload.get("failure_ranking"))
    top_reason = ranking[0]["reason"] if ranking else "none"
    return "\n".join(
        [
            f"status={metadata.get('status', 'UNKNOWN')}",
            f"tuning_result={tuning.get('result', 'UNKNOWN')}",
            f"candidate_status={tuning.get('candidate_status', 'UNKNOWN')}",
            f"guardrail_status={tuning.get('guardrail_status', 'UNKNOWN')}",
            f"total_candidates={rejection.get('total_candidates', 0)}",
            f"top_failure_reason={top_reason}",
            f"root_cause_category={root.get('category', 'mixed')}",
            f"root_cause_confidence={root.get('confidence', 'LOW')}",
            f"recommended_next_action={next_action.get('action', '')}",
            f"most_common_guardrail_failure={guardrail.get('most_common_guardrail_failure', '')}",
            "production_effect=none",
            "manual_review_required=true",
            "auto_promotion=false",
            "production_config_modified=false",
        ]
    )


def render_weight_tuning_failure_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    tuning = _mapping(payload.get("tuning_result"))
    rejection = _mapping(payload.get("candidate_rejection_summary"))
    root = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    walk_forward = _mapping(payload.get("walk_forward_failure"))
    guardrail = _mapping(payload.get("guardrail_failure"))
    performance = _mapping(payload.get("performance_failure"))
    promotion = _mapping(payload.get("promotion_impact"))
    lines = [
        "# Weight Tuning Failure Attribution Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- root_cause_category: `{root.get('category', 'mixed')}`",
        f"- root_cause_confidence: `{root.get('confidence', 'LOW')}`",
        f"- summary: {root.get('summary', '')}",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Input Artifacts",
        "",
    ]
    for key, value in _mapping(payload.get("input")).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 3. Weight Tuning Result",
            "",
            f"- result: `{tuning.get('result', 'UNKNOWN')}`",
            f"- candidate_status: `{tuning.get('candidate_status', 'UNKNOWN')}`",
            f"- guardrail_status: `{tuning.get('guardrail_status', 'UNKNOWN')}`",
            f"- reason: {tuning.get('reason', '')}",
            "",
            "## 4. Candidate Rejection Summary",
            "",
        ]
    )
    for key, value in rejection.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 5. Guardrail Failure Breakdown",
            "",
            f"- failed_guardrails: `{', '.join(_strings(guardrail.get('failed_guardrails')))}`",
            "- most_common_guardrail_failure: "
            f"`{guardrail.get('most_common_guardrail_failure', '')}`",
            "",
            "## 6. Performance Failure Breakdown",
            "",
        ]
    )
    for item in _records(performance.get("reasons")):
        lines.append(
            f"- `{item.get('reason')}`: {item.get('affected_candidates', 0)} candidates"
        )
    if not _records(performance.get("reasons")):
        lines.append("- 未识别出单一 performance failure 主因。")
    lines.extend(
        [
            "",
            "## 7. Walk-forward Stability Failure",
            "",
            "- non_worse_ratio_threshold: "
            f"`{walk_forward.get('non_worse_ratio_threshold', '')}`",
            "- observed_best_non_worse_ratio: "
            f"`{walk_forward.get('observed_best_non_worse_ratio', '')}`",
            "- most_failed_windows: "
            f"`{', '.join(_strings(walk_forward.get('most_failed_windows')))}`",
            "",
            "## 8. Near-miss Candidates",
            "",
        ]
    )
    near_misses = _records(payload.get("near_miss_candidates"))
    if not near_misses:
        lines.append("- 没有足够 candidate detail 可识别 near-miss candidate。")
    for row in near_misses:
        failed = ", ".join(_strings(row.get("failed_guardrails"))) or "none"
        lines.append(
            f"- `{row.get('candidate_id')}`: failed_guardrails=`{failed}`；"
            f"why_interesting={row.get('why_interesting', '')}"
        )
    lines.extend(
        [
            "",
            "## 9. Root Cause Assessment",
            "",
            f"- category: `{root.get('category', 'mixed')}`",
            f"- confidence: `{root.get('confidence', 'LOW')}`",
            f"- summary: {root.get('summary', '')}",
            "",
            "## 10. Recommended Next Action",
            "",
            f"- action: `{next_action.get('action', '')}`",
            f"- suggested_task: `{next_action.get('suggested_task', '')}`",
            f"- reason: {next_action.get('reason', '')}",
            "",
            "## 11. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 12. Manual Review Checklist",
            "",
            "- 复核 top failure reason 是否来自 candidate-level rejection details。",
            "- 确认没有降低 guardrail 或放开 fallback signal。",
            "- 若 root cause 为 turnover/drawdown/walk-forward，先建立专项诊断任务。",
            "- 确认 `config/parameters/production/current.yaml` 未修改。",
            "",
            "## 13. Input / Output Artifacts",
            "",
        ]
    )
    for section in ("input", "output_artifacts"):
        lines.append(f"### {section}")
        for key, value in _mapping(payload.get(section)).items():
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _write_run(
    payload: dict[str, Any],
    output_root: Path,
    as_of: date,
    *,
    debug: bool,
) -> WeightTuningFailureRun:
    json_path = default_weight_tuning_failure_json_path(output_root, as_of)
    markdown_path = default_weight_tuning_failure_markdown_path(output_root, as_of)
    write_weight_tuning_failure_summary(payload, json_path, markdown_path)
    debug_path = None
    if debug:
        debug_path = default_weight_tuning_failure_debug_path(output_root, as_of)
        write_weight_tuning_failure_debug(payload, debug_path)
    return WeightTuningFailureRun(
        as_of=as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        debug_path=debug_path,
    )


def _resolve_summary_path(*, as_of: date | None, summary_path: Path | None) -> Path | None:
    if summary_path is not None:
        return summary_path
    root = default_weight_tuning_root()
    if as_of is not None:
        return root / as_of.isoformat() / "weight_tuning_summary.json"
    return latest_weight_tuning_path(root)


def _resolve_output_date(
    *,
    as_of: date | None,
    summary_path: Path | None,
    generated_at: datetime,
) -> date:
    if as_of is not None:
        return as_of
    if summary_path is not None:
        try:
            return date.fromisoformat(summary_path.parent.name)
        except ValueError:
            return generated_at.date()
    return generated_at.date()


def _resolve_candidates_path(
    summary: Mapping[str, Any],
    summary_path: Path,
    as_of: date,
) -> Path | None:
    output_artifacts = _mapping(summary.get("output_artifacts"))
    candidate_text = str(output_artifacts.get("weight_tuning_candidates") or "")
    if candidate_text:
        path = Path(candidate_text)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if path.exists():
            return path
    return default_weight_tuning_candidates_path(summary_path.parent.parent, as_of)


def _blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    reason: str,
    summary_path: Path | None,
    candidates_path: Path | None,
    output_root: Path,
    summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    output_artifacts = _output_artifacts(output_root, as_of)
    input_payload = {
        "weight_tuning_summary": "" if summary_path is None else str(summary_path),
        "weight_tuning_candidates": "" if candidates_path is None else str(candidates_path),
    }
    metadata = _mapping(_mapping(summary).get("metadata")) if summary else {}
    tuning_result = {
        "result": metadata.get("status", "UNKNOWN"),
        "candidate_status": _mapping(_mapping(summary).get("recommended_candidate")).get(
            "status",
            "UNKNOWN",
        )
        if summary
        else "UNKNOWN",
        "guardrail_status": _mapping(
            _mapping(_mapping(summary).get("recommended_candidate")).get("guardrails")
        ).get("status", "UNKNOWN")
        if summary
        else "UNKNOWN",
    }
    return {
        "schema_version": WEIGHT_TUNING_FAILURE_SCHEMA_VERSION,
        "report_type": WEIGHT_TUNING_FAILURE_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-tuning-failure-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "BLOCKED",
            "reason": reason,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_config_modified": False,
            "source_task": "TRADING-059A",
            "market_regime": metadata.get("market_regime", "ai_after_chatgpt"),
            "market_regime_anchor": metadata.get("market_regime_anchor", "2022-11-30"),
            "requested_date_range": metadata.get("requested_date_range", {}),
        },
        "input": input_payload,
        "inputs": input_payload,
        "output_artifacts": output_artifacts,
        "tuning_result": tuning_result,
        "candidate_rejection_summary": {
            "total_candidates": 0,
            "valid_after_constraints": 0,
            "rejected_by_constraints": 0,
            "rejected_by_guardrails": 0,
            "rejected_by_performance": 0,
            "rejected_by_walk_forward": 0,
            "passed_all_but_signal_quality": 0,
            "passed_all_guardrails": 0,
        },
        "failure_ranking": [],
        "near_miss_candidates": [],
        "walk_forward_failure": {
            "non_worse_ratio_threshold": "",
            "observed_best_non_worse_ratio": 0.0,
            "most_failed_windows": [],
        },
        "guardrail_failure": {"failed_guardrails": [], "most_common_guardrail_failure": None},
        "performance_failure": {"reasons": []},
        "root_cause": {
            "category": "data_insufficient",
            "confidence": "HIGH",
            "summary": f"无法读取 weight tuning failure attribution 输入：{reason}。",
        },
        "recommended_next_action": {
            "action": "restore_weight_tuning_artifacts",
            "suggested_task": "TRADING-059A Weight Tuning Failure Attribution",
            "reason": reason,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "Failure attribution is blocked, so no candidate can be promoted.",
        },
        "diagnostic_quality": {
            "candidate_level_details_available": False,
            "candidate_level_detail_fields": [],
        },
        "safety": _safety_payload(),
    }


def _output_artifacts(output_root: Path, as_of: date) -> dict[str, str]:
    return {
        "weight_tuning_failure_summary_json": str(
            default_weight_tuning_failure_json_path(output_root, as_of)
        ),
        "weight_tuning_failure_summary_md": str(
            default_weight_tuning_failure_markdown_path(output_root, as_of)
        ),
        "weight_tuning_failure_candidates_debug": str(
            default_weight_tuning_failure_debug_path(output_root, as_of)
        ),
    }


def _candidate_failure_analysis(
    candidate: Mapping[str, Any],
    *,
    summary: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
    signal_failure_reasons: Sequence[str],
) -> dict[str, Any]:
    reasons = set(_explicit_rejection_reasons(candidate))
    reasons.update(_guardrail_reasons(candidate))
    reasons.update(_performance_failure_reasons(candidate))
    reasons.update(_walk_forward_reasons(candidate, guardrail_policy))
    if "signal_quality_limited" in signal_failure_reasons and _passes_guardrails(candidate):
        reasons.add("signal_quality_limited")
    if not _candidate_has_detail(candidate):
        reasons.add("candidate_level_details_insufficient")
    return {
        "candidate": dict(candidate),
        "candidate_id": candidate.get("candidate_id", ""),
        "status": candidate.get("status", "UNKNOWN"),
        "guardrail_status": candidate.get("guardrail_status")
        or _mapping(candidate.get("guardrails")).get("status", "UNKNOWN"),
        "rejection_reasons": sorted(reasons),
        "failed_guardrails": sorted(
            reason for reason in reasons if reason in GUARDRAIL_FAILURE_REASONS
        ),
        "performance_failure_reasons": sorted(
            reason for reason in reasons if reason in PERFORMANCE_FAILURE_REASONS
        ),
        "walk_forward_failure_reasons": sorted(
            reason for reason in reasons if reason in WALK_FORWARD_FAILURE_REASONS
        ),
        "relative_metrics": _candidate_relative(candidate),
        "objective_breakdown": _mapping(candidate.get("objective_breakdown")),
        "walk_forward_summary": _candidate_walk_forward_summary(candidate, guardrail_policy),
        "detail_available": _candidate_has_detail(candidate),
        "summary_status": _mapping(summary.get("metadata")).get("status", "UNKNOWN"),
    }


def _explicit_rejection_reasons(candidate: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for reason in _strings(candidate.get("rejection_reasons")):
        reasons.append(RAW_GUARDRAIL_REASON_MAP.get(reason, reason))
    return reasons


def _guardrail_reasons(candidate: Mapping[str, Any]) -> list[str]:
    guardrails = _mapping(candidate.get("guardrails"))
    reasons = [
        RAW_GUARDRAIL_REASON_MAP.get(reason, reason)
        for reason in _strings(guardrails.get("hard_rejections"))
    ]
    if candidate.get("guardrail_status") == "FAIL" and not reasons:
        reasons.append("guardrail_failure_unspecified")
    return sorted(dict.fromkeys(reasons))


def _performance_failure_reasons(candidate: Mapping[str, Any]) -> list[str]:
    relative = _candidate_relative(candidate)
    objective = _mapping(candidate.get("objective_breakdown"))
    reasons: list[str] = []
    if _has_number(relative, "annualized_return_delta") and (
        _float(relative.get("annualized_return_delta")) <= 0.0
    ):
        reasons.append("no_return_improvement")
    sharpe_delta = _coalesce_float(
        relative.get("sharpe_ratio_delta"),
        relative.get("sharpe_delta"),
        default=None,
    )
    if sharpe_delta is not None and sharpe_delta <= 0.0:
        reasons.append("sharpe_not_improved")
    if _has_number(relative, "max_drawdown_delta") and (
        _float(relative.get("max_drawdown_delta")) < 0.0
    ):
        reasons.append("max_drawdown_worse")
    turnover_relative = _coalesce_float(
        relative.get("turnover_relative_increase"),
        _mapping(candidate.get("guardrails")).get("turnover_relative_increase"),
        default=None,
    )
    if turnover_relative is not None and "turnover_guardrail_failed" in _guardrail_reasons(
        candidate
    ):
        reasons.append("turnover_too_high")
    if _has_number(relative, "cost_drag_delta") and _float(relative.get("cost_drag_delta")) > 0.0:
        reasons.append("cost_drag_too_high")
    objective_score_is_worse = _has_number(objective, "objective_score") and (
        _float(objective.get("objective_score")) <= 0.0
    )
    if objective_score_is_worse:
        reasons.append("risk_adjusted_performance_worse")
    return sorted(dict.fromkeys(reasons))


def _walk_forward_reasons(
    candidate: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
) -> list[str]:
    summary = _candidate_walk_forward_summary(candidate, guardrail_policy)
    reasons: list[str] = []
    if summary.get("validation_window_count", 0) and summary.get(
        "validation_window_count",
        0,
    ) < _int(guardrail_policy.get("min_validation_windows"), default=0):
        reasons.append("insufficient_validation_windows")
    threshold = _float(guardrail_policy.get("min_non_worse_walk_forward_ratio"), default=0.60)
    observed = _float(summary.get("non_worse_ratio"), default=0.0)
    if threshold and observed < threshold:
        reasons.append("non_worse_ratio_below_threshold")
    if summary.get("worst_window_status") == "worse":
        reasons.append("worst_window_breaches_guardrail")
    positive_ratio_low = (
        _float(summary.get("positive_window_ratio"), default=0.0) < LOW_FAILURE_RATIO
    )
    if positive_ratio_low and summary.get("validation_window_count", 0):
        reasons.append("improvement_concentrated_in_few_windows")
    if summary.get("negative_windows", 0) and summary.get("positive_windows", 0):
        reasons.append("unstable_across_regimes")
    return sorted(dict.fromkeys(reasons))


def _candidate_rejection_summary(
    summary: Mapping[str, Any],
    candidate_results: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    search = _mapping(summary.get("search"))
    total = len(candidate_results)
    valid_after_constraints = _int(search.get("candidates_evaluated"), default=total)
    if valid_after_constraints == 0 and total:
        valid_after_constraints = total
    rejected_by_guardrails = sum(
        1 for result in candidate_results if result.get("guardrail_status") != "PASS"
    )
    rejected_by_performance = sum(
        1 for result in candidate_results if result.get("performance_failure_reasons")
    )
    rejected_by_walk_forward = sum(
        1 for result in candidate_results if result.get("walk_forward_failure_reasons")
    )
    passed_all_guardrails = sum(
        1 for result in candidate_results if result.get("guardrail_status") == "PASS"
    )
    passed_all_but_signal_quality = sum(
        1
        for result in candidate_results
        if result.get("guardrail_status") == "PASS"
        and "signal_quality_limited" in _strings(result.get("rejection_reasons"))
    )
    return {
        "total_candidates": total,
        "valid_after_constraints": valid_after_constraints,
        "rejected_by_constraints": _int(search.get("candidates_rejected_by_constraints")),
        "rejected_by_guardrails": _int(
            search.get("candidates_rejected_by_guardrails"),
            default=rejected_by_guardrails,
        ),
        "rejected_by_performance": rejected_by_performance,
        "rejected_by_walk_forward": rejected_by_walk_forward,
        "passed_all_but_signal_quality": passed_all_but_signal_quality,
        "passed_all_guardrails": passed_all_guardrails,
    }


def _failure_counter(
    candidate_results: Sequence[Mapping[str, Any]],
    *,
    data_failures: Sequence[str],
    signal_failures: Sequence[str],
    search_failures: Sequence[str],
) -> Counter[str]:
    counter: Counter[str] = Counter()
    for result in candidate_results:
        counter.update(_strings(result.get("rejection_reasons")))
    if not candidate_results:
        counter.update(data_failures)
        counter.update(signal_failures)
        counter.update(search_failures)
    return counter


def _failure_ranking(counter: Counter[str], total_candidates: int) -> list[dict[str, Any]]:
    ranking = []
    denominator = max(total_candidates, 1)
    for rank, (reason, affected) in enumerate(counter.most_common(), start=1):
        ratio = affected / denominator
        ranking.append(
            {
                "rank": rank,
                "reason": reason,
                "affected_candidates": affected,
                "affected_ratio": round(ratio, 6),
                "severity": _severity(ratio),
            }
        )
    return ranking


def _near_miss_candidates(
    candidate_results: Sequence[Mapping[str, Any]],
    guardrail_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sorted_candidates = sorted(
        candidate_results,
        key=lambda item: (
            len(_strings(item.get("failed_guardrails"))),
            len(_strings(item.get("rejection_reasons"))),
            -_float(_mapping(item.get("objective_breakdown")).get("objective_score")),
            str(item.get("candidate_id")),
        ),
    )
    rows: list[dict[str, Any]] = []
    for result in sorted_candidates[:NEAR_MISS_LIMIT]:
        candidate = _mapping(result.get("candidate"))
        rows.append(
            {
                "candidate_id": result.get("candidate_id", ""),
                "weights": candidate.get("weights", {}),
                "failed_guardrails": _strings(result.get("failed_guardrails")),
                "rejection_reasons": _strings(result.get("rejection_reasons")),
                "distance_to_pass": _distance_to_pass(result, guardrail_policy),
                "why_interesting": _near_miss_sentence(result),
            }
        )
    return rows


def _distance_to_pass(
    result: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
) -> dict[str, dict[str, float]]:
    relative = _mapping(result.get("relative_metrics"))
    distances: dict[str, dict[str, float]] = {}
    turnover = _coalesce_float(
        relative.get("turnover_relative_increase"),
        _mapping(_mapping(result.get("candidate")).get("guardrails")).get(
            "turnover_relative_increase"
        ),
        default=None,
    )
    turnover_threshold = _float(
        guardrail_policy.get("turnover_relative_increase_limit"),
        default=0.30,
    )
    if turnover is not None and turnover > turnover_threshold:
        distances["turnover_relative_increase"] = {
            "value": round(turnover, 6),
            "threshold": round(turnover_threshold, 6),
            "excess": round(turnover - turnover_threshold, 6),
        }
    non_worse = _float(relative.get("non_worse_walk_forward_ratio"), default=0.0)
    non_worse_threshold = _float(
        guardrail_policy.get("min_non_worse_walk_forward_ratio"),
        default=0.60,
    )
    if non_worse < non_worse_threshold:
        distances["non_worse_walk_forward_ratio"] = {
            "value": round(non_worse, 6),
            "threshold": round(non_worse_threshold, 6),
            "shortfall": round(non_worse_threshold - non_worse, 6),
        }
    drawdown = _coalesce_float(relative.get("max_drawdown_delta"), default=None)
    drawdown_floor = -_float(guardrail_policy.get("max_drawdown_worse_limit"), default=0.02)
    if drawdown is not None and drawdown < drawdown_floor:
        distances["max_drawdown_delta"] = {
            "value": round(drawdown, 6),
            "threshold": round(drawdown_floor, 6),
            "shortfall": round(drawdown_floor - drawdown, 6),
        }
    annualized = _coalesce_float(relative.get("annualized_return_delta"), default=None)
    return_floor = -_float(
        guardrail_policy.get("annualized_return_underperformance_limit"),
        default=0.01,
    )
    if annualized is not None and annualized < return_floor:
        distances["annualized_return_delta"] = {
            "value": round(annualized, 6),
            "threshold": round(return_floor, 6),
            "shortfall": round(return_floor - annualized, 6),
        }
    return distances


def _near_miss_sentence(result: Mapping[str, Any]) -> str:
    reasons = set(_strings(result.get("rejection_reasons")))
    relative = _mapping(result.get("relative_metrics"))
    sharpe = _coalesce_float(relative.get("sharpe_ratio_delta"), relative.get("sharpe_delta"))
    annualized = _coalesce_float(relative.get("annualized_return_delta"))
    if "turnover_guardrail_failed" in reasons and (
        (sharpe is not None and sharpe > 0.0) or (annualized is not None and annualized > 0.0)
    ):
        return "候选改善收益或 Sharpe，但 turnover 超过 guardrail。"
    if "drawdown_guardrail_failed" in reasons:
        return "候选在部分绩效指标上接近可用，但 drawdown 相对 baseline 恶化。"
    if "walk_forward_guardrail_failed" in reasons or "non_worse_ratio_below_threshold" in reasons:
        return "候选全样本表现接近可用，但 walk-forward 稳定性不足。"
    if "sharpe_not_improved" in reasons or "no_return_improvement" in reasons:
        return "候选未证明比 baseline 有足够 alpha 或风险调整收益改善。"
    return "候选是当前排序中最接近通过的 rejected 参数组合之一。"


def _walk_forward_failure_summary(
    candidate_results: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
) -> dict[str, Any]:
    threshold = _float(guardrail_policy.get("min_non_worse_walk_forward_ratio"), default=0.60)
    observed = [
        _float(_mapping(item.get("relative_metrics")).get("non_worse_walk_forward_ratio"))
        for item in candidate_results
    ]
    window_counter: Counter[str] = Counter()
    for result in candidate_results:
        windows = _records(_mapping(result.get("candidate")).get("walk_forward_windows"))
        for window in windows:
            if window.get("status") != "non_worse":
                window_counter.update([str(window.get("window_id") or "UNKNOWN")])
    if not observed:
        observed = [
            _float(
                _mapping(summary.get("walk_forward")).get("non_worse_window_ratio"),
                default=0.0,
            )
        ]
    return {
        "non_worse_ratio_threshold": threshold,
        "observed_best_non_worse_ratio": max(observed) if observed else 0.0,
        "most_failed_windows": [item[0] for item in window_counter.most_common(5)],
        "reasons": sorted(
            {
                reason
                for result in candidate_results
                for reason in _strings(result.get("walk_forward_failure_reasons"))
            }
        ),
    }


def _guardrail_failure_summary(
    candidate_results: Sequence[Mapping[str, Any]],
    recommended_guardrails: Mapping[str, Any],
) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    for result in candidate_results:
        counter.update(_strings(result.get("failed_guardrails")))
    if not counter:
        counter.update(
            RAW_GUARDRAIL_REASON_MAP.get(reason, reason)
            for reason in _strings(recommended_guardrails.get("hard_rejections"))
        )
    return {
        "failed_guardrails": sorted(counter),
        "most_common_guardrail_failure": counter.most_common(1)[0][0] if counter else None,
        "failure_counts": dict(counter),
    }


def _performance_failure_summary(
    candidate_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    for result in candidate_results:
        counter.update(_strings(result.get("performance_failure_reasons")))
    return {
        "reasons": [
            {"reason": reason, "affected_candidates": count}
            for reason, count in counter.most_common()
        ],
    }


def _root_cause(
    failure_ranking: Sequence[Mapping[str, Any]],
    *,
    data_failures: Sequence[str],
    signal_failures: Sequence[str],
    search_failures: Sequence[str],
    candidate_results: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    if data_failures:
        return {
            "category": "data_insufficient",
            "confidence": "HIGH",
            "summary": "Data readiness failure blocked reliable interpretation of weight tuning.",
        }
    if search_failures and not candidate_results:
        return {
            "category": "search_space_too_narrow",
            "confidence": "MEDIUM",
            "summary": "No valid candidates were available after restricted search constraints.",
        }
    top_reason = str(failure_ranking[0].get("reason")) if failure_ranking else ""
    top_ratio = _float(failure_ranking[0].get("affected_ratio")) if failure_ranking else 0.0
    confidence = "HIGH" if top_ratio >= HIGH_FAILURE_RATIO else "MEDIUM"
    if top_ratio < MEDIUM_FAILURE_RATIO:
        confidence = "LOW"
    category = "mixed"
    summary = "Candidate failures are distributed across multiple dimensions."
    if top_reason in {"turnover_guardrail_failed", "turnover_too_high", "cost_drag_too_high"}:
        category = "portfolio_turnover_too_high"
        summary = "Most rejected candidates are blocked by turnover or cost-drag pressure."
    elif top_reason in {"drawdown_guardrail_failed", "max_drawdown_worse"}:
        category = "drawdown_control_insufficient"
        summary = "Candidate return changes are not supported by drawdown control."
    elif top_reason in {
        "walk_forward_guardrail_failed",
        "non_worse_ratio_below_threshold",
        "improvement_concentrated_in_few_windows",
        "worst_window_breaches_guardrail",
        "unstable_across_regimes",
    }:
        category = "walk_forward_unstable"
        summary = "Candidate improvements are not stable across walk-forward windows."
    elif top_reason in {
        "no_return_improvement",
        "sharpe_not_improved",
        "risk_adjusted_performance_worse",
    }:
        category = "no_alpha_detected"
        summary = "Restricted tuning did not find risk-adjusted improvement over baseline."
    elif top_reason in {"too_few_valid_candidates", "search_space_too_narrow"}:
        category = "search_space_too_narrow"
        summary = "Search constraints leave too few valid candidates for attribution."
    elif "signal_quality_limited" in signal_failures:
        category = "signal_quality_limited"
        summary = "Current signal quality is LIMITED and constrains candidate usefulness."
    return {"category": category, "confidence": confidence, "summary": summary}


def _recommended_next_action(root_cause: Mapping[str, Any]) -> dict[str, str]:
    category = str(root_cause.get("category") or "mixed")
    mapping = {
        "portfolio_turnover_too_high": {
            "action": "review_portfolio_turnover_constraints",
            "suggested_task": "TRADING-060 Portfolio Turnover Attribution for Weight Candidates",
            "reason": "Most near-miss candidates improved metrics but failed turnover guardrails.",
        },
        "drawdown_control_insufficient": {
            "action": "upgrade_risk_signals",
            "suggested_task": "TRADING-060 Valuation Risk / Macro Risk Proxy Upgrade",
            "reason": "Candidate weights worsened drawdown beyond guardrail limits.",
        },
        "no_alpha_detected": {
            "action": "improve_signal_quality",
            "suggested_task": "TRADING-060 Signal Quality Upgrade",
            "reason": "Restricted tuning did not find risk-adjusted improvement.",
        },
        "walk_forward_unstable": {
            "action": "add_regime_aware_weight_tuning",
            "suggested_task": "TRADING-060 Walk-forward Robustness Diagnostics",
            "reason": "Candidate improvements are concentrated in unstable validation windows.",
        },
        "search_space_too_narrow": {
            "action": "expand_restricted_search_space",
            "suggested_task": "TRADING-060 Restricted Search Space Expansion",
            "reason": "Too few valid candidates were generated after constraints.",
        },
        "signal_quality_limited": {
            "action": "improve_signal_quality",
            "suggested_task": "TRADING-060 Signal Quality Upgrade",
            "reason": "Signal quality remains LIMITED and constrains tuning interpretation.",
        },
        "data_insufficient": {
            "action": "restore_data_readiness",
            "suggested_task": "TRADING-060 Data Readiness for Weight Tuning",
            "reason": "Data or input artifacts are insufficient for failure attribution.",
        },
    }
    return mapping.get(
        category,
        {
            "action": "review_mixed_failure_attribution",
            "suggested_task": "TRADING-060 Mixed Weight Tuning Failure Review",
            "reason": "No single failure mode dominates candidate rejection.",
        },
    )


def _data_failure_reasons(summary: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    data_quality = _mapping(summary.get("data_quality"))
    freshness = _mapping(summary.get("freshness"))
    inputs = _mapping(summary.get("inputs"))
    if data_quality.get("data_gate_status") not in {"OK", None, ""}:
        reasons.append("data_gate_not_ok")
    if freshness.get("status") not in {"OK", None, ""}:
        reasons.append("freshness_not_ok")
    manifest = str(
        inputs.get("backtest_input_manifest") or data_quality.get("backtest_manifest") or ""
    )
    if not manifest:
        reasons.append("missing_backtest_manifest")
    signal_snapshot = str(inputs.get("signal_snapshot") or "")
    if not signal_snapshot:
        reasons.append("missing_signal_snapshot")
    return sorted(dict.fromkeys(reasons))


def _signal_failure_reasons(summary: Mapping[str, Any]) -> list[str]:
    signal_quality = _mapping(summary.get("signal_quality"))
    constraints = _mapping(summary.get("constraints"))
    reasons: list[str] = []
    if signal_quality.get("status") == "LIMITED":
        reasons.append("signal_quality_limited")
    if not _strings(signal_quality.get("real_signals")):
        reasons.append("too_few_real_signals")
    if _strings(signal_quality.get("fallback_signals")):
        reasons.append("too_many_fallback_signals")
    if "macro_liquidity" in _strings(signal_quality.get("proxy_signals")):
        reasons.append("proxy_signal_dominates")
    if constraints.get("forbid_free_fallback_weight_tuning") is True:
        reasons.append("fallback_signals_not_tunable")
    return sorted(dict.fromkeys(reasons))


def _search_failure_reasons(
    summary: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> list[str]:
    search = _mapping(summary.get("search"))
    constraints = _mapping(summary.get("constraints"))
    scope = _mapping(summary.get("tuning_scope"))
    reasons: list[str] = []
    evaluated = _int(search.get("candidates_evaluated"), default=len(candidates))
    rejected_constraints = _int(search.get("candidates_rejected_by_constraints"))
    if evaluated == 0:
        reasons.append("too_few_valid_candidates")
        reasons.append("search_space_too_narrow")
    if rejected_constraints > evaluated:
        reasons.append("too_many_candidates_rejected_by_constraints")
    if constraints.get("max_total_l1_distance_from_baseline") is not None:
        reasons.append("l1_distance_constraint_observed")
    if _strings(scope.get("fixed_weights")):
        reasons.append("fixed_fallback_weights_limit_search")
    return sorted(dict.fromkeys(reasons))


def _candidate_walk_forward_summary(
    candidate: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
) -> dict[str, Any]:
    existing = _mapping(candidate.get("walk_forward_summary"))
    if existing:
        return dict(existing)
    windows = _records(candidate.get("walk_forward_windows"))
    positive = 0
    negative = 0
    non_worse = 0
    worst_window = ""
    worst_score = 0.0
    for window in windows:
        relative = _mapping(window.get("relative_metrics"))
        return_delta = _float(relative.get("annualized_return_delta"))
        sharpe_delta = _coalesce_float(
            relative.get("sharpe_ratio_delta"),
            relative.get("sharpe_delta"),
            default=0.0,
        )
        window_score = return_delta + float(sharpe_delta or 0.0)
        if window.get("status") == "non_worse":
            non_worse += 1
        if return_delta > 0.0 or (sharpe_delta is not None and sharpe_delta > 0.0):
            positive += 1
        if return_delta < 0.0 or (sharpe_delta is not None and sharpe_delta < 0.0):
            negative += 1
        if not worst_window or window_score < worst_score:
            worst_window = str(window.get("window_id") or "")
            worst_score = window_score
    count = len(windows)
    relative = _candidate_relative(candidate)
    non_worse_ratio = _coalesce_float(
        relative.get("non_worse_walk_forward_ratio"),
        default=(non_worse / count if count else 0.0),
    )
    return {
        "validation_window_count": count,
        "non_worse_ratio": round(float(non_worse_ratio or 0.0), 6),
        "non_worse_ratio_threshold": _float(
            guardrail_policy.get("min_non_worse_walk_forward_ratio"),
            default=0.60,
        ),
        "positive_windows": positive,
        "negative_windows": negative,
        "unstable_windows": negative,
        "positive_window_ratio": round(positive / count, 6) if count else 0.0,
        "worst_window": worst_window,
        "worst_window_status": "worse" if negative else "",
    }


def _candidate_level_details_sufficient(candidate_results: Sequence[Mapping[str, Any]]) -> bool:
    if not candidate_results:
        return False
    return any(result.get("detail_available") is True for result in candidate_results)


def _candidate_detail_fields(candidates: Sequence[Mapping[str, Any]]) -> list[str]:
    fields: set[str] = set()
    for candidate in candidates:
        for key in (
            "rejection_reasons",
            "guardrails",
            "relative_metrics",
            "walk_forward_windows",
            "walk_forward_summary",
            "objective_breakdown",
        ):
            if candidate.get(key):
                fields.add(key)
    return sorted(fields)


def _candidate_has_detail(candidate: Mapping[str, Any]) -> bool:
    return any(
        candidate.get(key)
        for key in (
            "rejection_reasons",
            "guardrails",
            "relative_metrics",
            "walk_forward_windows",
            "walk_forward_summary",
            "objective_breakdown",
        )
    )


def _passes_guardrails(candidate: Mapping[str, Any]) -> bool:
    guardrail_status = candidate.get("guardrail_status") or _mapping(
        candidate.get("guardrails")
    ).get("status")
    return guardrail_status == "PASS"


def _candidate_relative(candidate: Mapping[str, Any]) -> dict[str, Any]:
    relative = _mapping(candidate.get("relative_metrics"))
    if "sharpe_delta" in relative and "sharpe_ratio_delta" not in relative:
        relative = {**relative, "sharpe_ratio_delta": relative.get("sharpe_delta")}
    return relative


def _summary_date(summary: Mapping[str, Any], source_path: Path, *, fallback: date) -> date:
    try:
        return weight_tuning_payload_date(summary, source_path)
    except ValueError:
        try:
            return date.fromisoformat(str(summary.get("as_of") or ""))
        except ValueError:
            return fallback


def _latest_artifact_path(root_name: str, as_of: date, filename: str) -> str:
    root = PROJECT_ROOT / "artifacts" / root_name
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"*/{filename}"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return ""
    return str(max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1])


def _severity(ratio: float) -> str:
    if ratio >= HIGH_FAILURE_RATIO:
        return "HIGH"
    if ratio >= MEDIUM_FAILURE_RATIO:
        return "MEDIUM"
    return "LOW"


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "production_write_allowed": False,
        "production_config_modified": False,
        "candidate_promotion_triggered": False,
        "fallback_signals_free_tuned": False,
        "guardrails_lowered": False,
        "fake_candidate_generated": False,
        "broker_action": False,
        "trading_action": False,
    }


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _int(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coalesce_float(*values: object, default: float | None = 0.0) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _has_number(mapping: Mapping[str, Any], key: str) -> bool:
    if key not in mapping:
        return False
    try:
        float(mapping[key])
    except (TypeError, ValueError):
        return False
    return True
