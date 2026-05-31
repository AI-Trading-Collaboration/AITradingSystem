from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_PRODUCTION_PARAMETERS_PATH,
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
    _full_result_start,
    _portfolio_profile_config,
    _simulate_weight_context,
    _simulation_context,
    default_weight_tuning_candidates_path,
    latest_weight_tuning_path,
    load_weight_tuning_config,
    load_weight_tuning_payload,
    weight_tuning_payload_date,
)
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    RAW_GUARDRAIL_REASON_MAP,
    latest_weight_tuning_failure_path_on_or_before,
    load_weight_tuning_candidates,
    load_weight_tuning_failure_payload,
)
from ai_trading_system.trading_engine.portfolio_candidates import (
    DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    latest_portfolio_candidates_path_on_or_before,
    load_portfolio_candidate_config,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    latest_portfolio_sensitivity_path_on_or_before,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    signal_snapshot_frames,
)

PORTFOLIO_TURNOVER_ATTRIBUTION_SCHEMA_VERSION = 1
PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE = "portfolio_turnover_attribution"
PORTFOLIO_TURNOVER_ATTRIBUTION_ALIAS_REPORT_TYPE = "portfolio_turnover_attribution_report"

# Diagnostic-only fallback used only when the TRADING-059 guardrail policy is absent.
DEFAULT_TURNOVER_RELATIVE_INCREASE_LIMIT = 0.30
# Diagnostic thresholds below only choose explanatory labels; they never change guardrails.
HIGH_ROOT_CAUSE_RATIO = 0.60
ASSET_CONCENTRATION_SHARE = 0.45
SMALL_TRADE_RATIO_WARNING = 0.40
NEAR_MISS_LIMIT = 5
REPLAY_CANDIDATE_LIMIT = 8
WEIGHT_SEARCH_L1_DISTANCE_WARNING = 0.20
COST_DRAG_DOMINANCE_RATIO = 0.60
SMALL_TRADE_ABS_THRESHOLD = 0.01
EPSILON = 1e-12

TURNOVER_REASONS = {
    "turnover_guardrail_failed",
    "turnover_too_high",
    "cost_drag_too_high",
    "turnover_increase_more_than_limit",
}


@dataclass(frozen=True)
class PortfolioTurnoverAttributionRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    debug_path: Path | None = None


def default_portfolio_turnover_attribution_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_turnover_attribution"


def default_portfolio_turnover_attribution_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_turnover_attribution_json_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_turnover_attribution_dir(output_root, as_of)
        / "portfolio_turnover_attribution_summary.json"
    )


def default_portfolio_turnover_attribution_markdown_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_turnover_attribution_dir(output_root, as_of)
        / "portfolio_turnover_attribution_summary.md"
    )


def default_portfolio_turnover_attribution_debug_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_turnover_attribution_dir(output_root, as_of)
        / "portfolio_turnover_candidates_debug.json"
    )


def latest_portfolio_turnover_attribution_path(
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_turnover_attribution_root()
    candidates = sorted(root.glob("*/portfolio_turnover_attribution_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_turnover_attribution_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_turnover_attribution_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_turnover_attribution_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_turnover_attribution_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_turnover_attribution_{as_of.isoformat()}.md",
    )


def run_portfolio_turnover_attribution(
    *,
    as_of: date | None = None,
    weight_tuning_path: Path | None = None,
    output_root: Path | None = None,
    near_miss_only: bool = False,
    debug: bool = False,
    generated_at: datetime | None = None,
) -> PortfolioTurnoverAttributionRun:
    generated = generated_at or datetime.now(tz=UTC)
    root = output_root or default_portfolio_turnover_attribution_root()
    summary_path = _resolve_weight_tuning_summary_path(
        as_of=as_of,
        weight_tuning_path=weight_tuning_path,
    )
    resolved_as_of = _resolve_output_date(
        as_of=as_of,
        weight_tuning_path=summary_path or weight_tuning_path,
        generated_at=generated,
    )
    if summary_path is None or not summary_path.exists():
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_summary",
            weight_tuning_path=summary_path or weight_tuning_path,
            candidates_path=None,
            failure_path=None,
            output_root=root,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    summary = load_weight_tuning_payload(summary_path)
    if not summary:
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_summary",
            weight_tuning_path=summary_path,
            candidates_path=None,
            failure_path=None,
            output_root=root,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    resolved_as_of = _summary_date(summary, summary_path, fallback=resolved_as_of)
    failure_path = latest_weight_tuning_failure_path_on_or_before(resolved_as_of)
    if failure_path is None or not failure_path.exists():
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_failure_summary",
            weight_tuning_path=summary_path,
            candidates_path=None,
            failure_path=failure_path,
            output_root=root,
            summary=summary,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    candidates_path = _resolve_candidates_path(summary, summary_path, resolved_as_of)
    if candidates_path is None or not candidates_path.exists():
        payload = _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            reason="missing_weight_tuning_candidates",
            weight_tuning_path=summary_path,
            candidates_path=candidates_path,
            failure_path=failure_path,
            output_root=root,
            summary=summary,
        )
        return _write_run(payload, root, resolved_as_of, debug=debug)

    failure_payload = load_weight_tuning_failure_payload(failure_path)
    candidates_payload = load_weight_tuning_candidates(candidates_path)
    payload = build_portfolio_turnover_attribution_payload(
        summary,
        candidates_payload,
        failure_payload,
        weight_tuning_path=summary_path,
        candidates_path=candidates_path,
        failure_path=failure_path,
        output_root=root,
        near_miss_only=near_miss_only,
        generated_at=generated,
    )
    return _write_run(payload, root, resolved_as_of, debug=debug)


def build_portfolio_turnover_attribution_payload(
    summary: Mapping[str, Any],
    candidates_payload: Mapping[str, Any],
    failure_payload: Mapping[str, Any],
    *,
    weight_tuning_path: Path,
    candidates_path: Path,
    failure_path: Path,
    output_root: Path | None = None,
    near_miss_only: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    as_of = _summary_date(summary, weight_tuning_path, fallback=generated.date())
    root = output_root or default_portfolio_turnover_attribution_root()
    metadata = _mapping(summary.get("metadata"))
    inputs = _mapping(summary.get("inputs"))
    guardrail_policy = _mapping(summary.get("guardrail_policy"))
    failure_root = _mapping(failure_payload.get("root_cause"))
    failure_guardrail = _mapping(failure_payload.get("guardrail_failure"))
    candidates = _records(candidates_payload.get("candidates"))
    if not candidates:
        candidates = _records(summary.get("candidate_ranking"))
    turnover_candidates = [
        candidate for candidate in candidates if _candidate_is_turnover_rejected(candidate)
    ]
    if near_miss_only:
        turnover_candidates = _near_miss_source_candidates(
            turnover_candidates,
            failure_payload=failure_payload,
        )
    detail_sufficient = any(_candidate_has_turnover_detail(candidate) for candidate in candidates)
    status = "TURNOVER_FAILURE_EXPLAINED"
    reason = ""
    if not detail_sufficient:
        status = "LIMITED"
        reason = "insufficient_candidate_turnover_details"

    candidate_rows = [
        _candidate_turnover_row(candidate, guardrail_policy=guardrail_policy)
        for candidate in turnover_candidates
        if _candidate_has_turnover_detail(candidate)
    ]
    replay = _replay_turnover_details(
        summary=summary,
        candidate_rows=candidate_rows,
        candidates=turnover_candidates,
        as_of=as_of,
    )
    candidate_rows = _merge_replay_candidate_rows(candidate_rows, replay)
    asset_contribution = _asset_turnover_contribution(candidate_rows, replay)
    walk_forward_turnover = _walk_forward_turnover(candidate_rows, turnover_candidates)
    rebalance_attribution = _rebalance_attribution(candidate_rows, replay)
    near_miss_analysis = _near_miss_turnover_analysis(
        candidate_rows,
        failure_payload=failure_payload,
        guardrail_policy=guardrail_policy,
    )
    candidate_summary = _candidate_turnover_summary(candidate_rows)
    cost_drag = _cost_drag_attribution(candidate_rows, asset_contribution)
    root_cause = _turnover_root_cause(
        candidate_rows=candidate_rows,
        asset_contribution=asset_contribution,
        rebalance_attribution=rebalance_attribution,
        cost_drag=cost_drag,
        summary=summary,
        status=status,
    )
    next_action = _recommended_next_action(root_cause)
    diagnostics_warnings = [
        *replay.get("warnings", []),
        *([] if detail_sufficient else [reason]),
    ]
    latest_candidates_path = latest_portfolio_candidates_path_on_or_before(as_of)
    latest_sensitivity_path = latest_portfolio_sensitivity_path_on_or_before(as_of)
    input_artifacts = {
        "weight_tuning_summary": str(weight_tuning_path),
        "weight_tuning_candidates": str(candidates_path),
        "weight_tuning_failure": str(failure_path),
        "portfolio_candidates_summary": ""
        if latest_candidates_path is None
        else str(latest_candidates_path),
        "portfolio_sensitivity_summary": ""
        if latest_sensitivity_path is None
        else str(latest_sensitivity_path),
        "signal_snapshot": str(inputs.get("signal_snapshot") or ""),
        "backtest_input_manifest": str(inputs.get("backtest_input_manifest") or ""),
        "weight_tuning_config": str(
            inputs.get("weight_tuning_config")
            or metadata.get("config_path")
            or DEFAULT_WEIGHT_TUNING_CONFIG_PATH
        ),
        "portfolio_candidate_profiles": str(
            inputs.get("portfolio_candidate_profiles")
            or DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH
        ),
        "production_parameters": str(
            inputs.get("baseline_parameters") or DEFAULT_PRODUCTION_PARAMETERS_PATH
        ),
    }
    output_artifacts = _output_artifacts(root, as_of)
    return {
        "schema_version": PORTFOLIO_TURNOVER_ATTRIBUTION_SCHEMA_VERSION,
        "report_type": PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"portfolio-turnover-attribution-{as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": status,
            "reason": reason,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_config_modified": False,
            "source_task": "TRADING-060",
            "market_regime": metadata.get("market_regime", "ai_after_chatgpt"),
            "market_regime_anchor": metadata.get("market_regime_anchor", "2022-11-30"),
            "requested_date_range": metadata.get("requested_date_range", {}),
            "near_miss_only": near_miss_only,
        },
        "inputs": input_artifacts,
        "input": input_artifacts,
        "output_artifacts": output_artifacts,
        "source_failure_context": {
            "status": _mapping(failure_payload.get("metadata")).get("status", "UNKNOWN"),
            "root_cause_category": failure_root.get("category", "mixed"),
            "top_failure_reason": _top_failure_reason(failure_payload),
            "most_common_guardrail_failure": failure_guardrail.get(
                "most_common_guardrail_failure",
                "",
            ),
            "recommended_next_action": _mapping(
                failure_payload.get("recommended_next_action")
            ).get("action", ""),
        },
        "summary": {
            "root_cause_category": root_cause.get("category", "mixed"),
            "top_failure_reason": _top_failure_reason(failure_payload),
            "most_common_guardrail_failure": failure_guardrail.get(
                "most_common_guardrail_failure",
                "",
            ),
            "turnover_failed_candidates": len(candidate_rows),
            "near_miss_candidates": len(near_miss_analysis),
            "portfolio_profile": inputs.get("portfolio_profile", ""),
            "data_quality_status": _mapping(summary.get("data_quality")).get(
                "status",
                "UNKNOWN",
            ),
        },
        "candidate_turnover_summary": candidate_summary,
        "candidate_turnover_attribution": candidate_rows,
        "cost_drag_attribution": cost_drag,
        "asset_turnover_contribution": asset_contribution,
        "walk_forward_turnover": walk_forward_turnover,
        "rebalance_attribution": rebalance_attribution,
        "near_miss_turnover_analysis": near_miss_analysis,
        "root_cause": root_cause,
        "recommended_next_action": next_action,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Weight tuning produced no valid candidate; turnover attribution is "
                "diagnostic only and cannot approve promotion."
            ),
        },
        "diagnostic_quality": {
            "candidate_level_turnover_details_available": detail_sufficient,
            "asset_level_replay_available": bool(replay.get("asset_level_replay_available")),
            "replayed_candidate_count": replay.get("replayed_candidate_count", 0),
            "warnings": list(dict.fromkeys(str(item) for item in diagnostics_warnings if item)),
            "diagnostic_policy": {
                "turnover_relative_increase_limit": _turnover_limit(guardrail_policy),
                "asset_concentration_share": ASSET_CONCENTRATION_SHARE,
                "small_trade_ratio_warning": SMALL_TRADE_RATIO_WARNING,
                "small_trade_abs_threshold": SMALL_TRADE_ABS_THRESHOLD,
                "replay_candidate_limit": REPLAY_CANDIDATE_LIMIT,
            },
        },
        "safety": _safety_payload(),
    }


def write_portfolio_turnover_attribution_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_portfolio_turnover_attribution_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def write_portfolio_turnover_attribution_debug(
    payload: Mapping[str, Any],
    debug_path: Path,
) -> Path:
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_payload = {
        "schema_version": PORTFOLIO_TURNOVER_ATTRIBUTION_SCHEMA_VERSION,
        "report_type": "portfolio_turnover_candidates_debug",
        "metadata": _mapping(payload.get("metadata")),
        "candidate_turnover_attribution": payload.get("candidate_turnover_attribution", []),
        "asset_turnover_contribution": payload.get("asset_turnover_contribution", []),
        "walk_forward_turnover": payload.get("walk_forward_turnover", []),
        "rebalance_attribution": payload.get("rebalance_attribution", {}),
        "diagnostic_quality": payload.get("diagnostic_quality", {}),
        "safety": _safety_payload(),
    }
    debug_path.write_text(
        json.dumps(debug_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return debug_path


def write_portfolio_turnover_attribution_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": PORTFOLIO_TURNOVER_ATTRIBUTION_ALIAS_REPORT_TYPE,
        "source_report_type": PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_portfolio_turnover_attribution_report(
        alias_payload,
        json_path,
        markdown_path,
    )


def load_portfolio_turnover_attribution_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_portfolio_turnover_attribution_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_TURNOVER_ATTRIBUTION_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE,
        PORTFOLIO_TURNOVER_ATTRIBUTION_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in ("run_id", "generated_at", "status", "production_effect"):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("status") not in {"TURNOVER_FAILURE_EXPLAINED", "LIMITED", "BLOCKED"}:
        issues.append("status must be TURNOVER_FAILURE_EXPLAINED, LIMITED, or BLOCKED")
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
        "cost_model_modified",
        "turnover_guardrail_modified",
    ):
        if safety.get(key) is not False:
            issues.append(f"{key} must be false")
    can_support_promotion = _mapping(payload.get("promotion_impact")).get(
        "can_support_candidate_promotion"
    )
    if can_support_promotion is not False:
        issues.append("promotion_impact must not support candidate promotion")
    if metadata.get("status") in {"TURNOVER_FAILURE_EXPLAINED", "LIMITED"}:
        if not isinstance(payload.get("candidate_turnover_summary"), dict):
            issues.append("candidate_turnover_summary must be an object")
        if not isinstance(payload.get("root_cause"), dict):
            issues.append("root_cause must be an object")
        if not isinstance(payload.get("recommended_next_action"), dict):
            issues.append("recommended_next_action must be an object")
    return issues


def portfolio_turnover_attribution_payload_date(
    payload: Mapping[str, Any],
    source_path: Path,
) -> date:
    raw_as_of = str(payload.get("as_of") or "")
    try:
        return date.fromisoformat(raw_as_of)
    except ValueError:
        pass
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("portfolio-turnover-attribution-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(
            f"cannot infer portfolio turnover attribution date from {source_path}"
        ) from exc


def render_portfolio_turnover_attribution_explanation(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    candidate_summary = _mapping(payload.get("candidate_turnover_summary"))
    root = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    return "\n".join(
        [
            f"status={metadata.get('status', 'UNKNOWN')}",
            f"reason={metadata.get('reason', '')}",
            f"root_cause_category={root.get('category', 'mixed')}",
            f"root_cause_confidence={root.get('confidence', 'LOW')}",
            f"top_failure_reason={summary.get('top_failure_reason', '')}",
            "most_common_guardrail_failure="
            f"{summary.get('most_common_guardrail_failure', '')}",
            f"failed_by_turnover={candidate_summary.get('total_failed_by_turnover', 0)}",
            "avg_turnover_relative_increase="
            f"{candidate_summary.get('avg_turnover_relative_increase', 0.0)}",
            f"avg_cost_drag_delta={candidate_summary.get('avg_cost_drag_delta', 0.0)}",
            f"recommended_next_action={next_action.get('action', '')}",
            "production_effect=none",
            "manual_review_required=true",
            "auto_promotion=false",
            "production_config_modified=false",
        ]
    )


def render_portfolio_turnover_attribution_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    source_failure = _mapping(payload.get("source_failure_context"))
    candidate_summary = _mapping(payload.get("candidate_turnover_summary"))
    root = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    promotion = _mapping(payload.get("promotion_impact"))
    cost_drag = _mapping(payload.get("cost_drag_attribution"))
    rebalance = _mapping(payload.get("rebalance_attribution"))
    lines = [
        "# Portfolio Turnover Attribution Summary",
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
    for key, value in _mapping(payload.get("inputs")).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 3. Weight Tuning Failure Context",
            "",
            f"- failure_status: `{source_failure.get('status', 'UNKNOWN')}`",
            f"- failure_root_cause: `{source_failure.get('root_cause_category', 'mixed')}`",
            f"- top_failure_reason: `{summary.get('top_failure_reason', '')}`",
            "- most_common_guardrail_failure: "
            f"`{summary.get('most_common_guardrail_failure', '')}`",
            f"- portfolio_profile: `{summary.get('portfolio_profile', '')}`",
            f"- data_quality_status: `{summary.get('data_quality_status', 'UNKNOWN')}`",
            "",
            "## 4. Candidate-level Turnover Attribution",
            "",
        ]
    )
    for key, value in candidate_summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "| Candidate | Guardrail | Turnover | Baseline | Relative Increase | Cost Drag Delta |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in _records(payload.get("candidate_turnover_attribution"))[:10]:
        lines.append(
            "| "
            f"`{row.get('candidate_id', '')}` | "
            f"`{row.get('guardrail_status', '')}` | "
            f"{_format_float(row.get('turnover'))} | "
            f"{_format_float(row.get('baseline_turnover'))} | "
            f"{_format_float(row.get('turnover_relative_increase'))} | "
            f"{_format_float(_mapping(row.get('cost_drag')).get('cost_drag_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 5. Cost Drag Attribution",
            "",
            f"- avg_cost_drag_delta: `{_format_float(cost_drag.get('avg_cost_drag_delta'))}`",
            f"- max_cost_drag_delta: `{_format_float(cost_drag.get('max_cost_drag_delta'))}`",
            f"- cost_drag_dominates: `{cost_drag.get('cost_drag_dominates', False)}`",
            f"- top_cost_assets: `{', '.join(_strings(cost_drag.get('top_cost_assets')))}`",
            "",
            "## 6. Asset-level Turnover Contribution",
            "",
            "| Symbol | Turnover Contribution | Share | Rebalance Count | Avg Weight Change |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in _records(payload.get("asset_turnover_contribution"))[:10]:
        lines.append(
            "| "
            f"`{row.get('symbol', '')}` | "
            f"{_format_float(row.get('turnover_contribution'))} | "
            f"{_format_float(row.get('turnover_share'))} | "
            f"{row.get('rebalance_count', 0)} | "
            f"{_format_float(row.get('avg_weight_change'))} |"
        )
    if not _records(payload.get("asset_turnover_contribution")):
        lines.append("| `insufficient_details` | 0.0000 | 0.0000 | 0 | 0.0000 |")
    lines.extend(
        [
            "",
            "## 7. Walk-forward Turnover Contribution",
            "",
            "| Window | Candidate Turnover | Baseline Turnover | Cost Drag Delta | Guardrail |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for row in _records(payload.get("walk_forward_turnover")):
        lines.append(
            "| "
            f"`{row.get('window_id', '')}` | "
            f"{_format_float(row.get('candidate_turnover'))} | "
            f"{_format_float(row.get('baseline_turnover'))} | "
            f"{_format_float(row.get('cost_drag_delta'))} | "
            f"`{row.get('guardrail_status', '')}` |"
        )
    lines.extend(
        [
            "",
            "## 8. Rebalance Event Attribution",
            "",
            f"- rebalance_days: `{rebalance.get('rebalance_days', 0)}`",
            f"- baseline_rebalance_days: `{rebalance.get('baseline_rebalance_days', 0)}`",
            f"- extra_rebalance_days: `{rebalance.get('extra_rebalance_days', 0)}`",
            "- avg_assets_changed_per_rebalance: "
            f"`{_format_float(rebalance.get('avg_assets_changed_per_rebalance'))}`",
            "- max_assets_changed_per_rebalance: "
            f"`{rebalance.get('max_assets_changed_per_rebalance', 0)}`",
            f"- small_trade_ratio: `{_format_float(rebalance.get('small_trade_ratio'))}`",
            f"- warning: {rebalance.get('warning', '')}",
            "",
            "## 9. Near-miss Candidates",
            "",
        ]
    )
    near_misses = _records(payload.get("near_miss_turnover_analysis"))
    if not near_misses:
        lines.append("- 没有足够 turnover near-miss detail。")
    for row in near_misses:
        lines.append(
            f"- `{row.get('candidate_id', '')}`: turnover_excess="
            f"`{_format_float(row.get('turnover_excess'))}`；"
            f"cost_drag_excess=`{_format_float(row.get('cost_drag_excess'))}`；"
            f"possible_fix=`{'; '.join(_strings(row.get('possible_fix')))}`"
        )
    lines.extend(
        [
            "",
            "## 10. Root Cause Assessment",
            "",
            f"- category: `{root.get('category', 'mixed')}`",
            f"- confidence: `{root.get('confidence', 'LOW')}`",
            f"- summary: {root.get('summary', '')}",
            "",
            "## 11. Recommended Next Action",
            "",
            f"- action: `{next_action.get('action', '')}`",
            f"- suggested_task: `{next_action.get('suggested_task', '')}`",
            f"- reason: {next_action.get('reason', '')}",
            "",
            "## 12. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 13. Manual Review Checklist",
            "",
            "- 确认 turnover attribution 只解释 rejected candidates，不重新调参。",
            "- 确认没有降低 turnover guardrail 或修改 cost model。",
            "- 如果 asset-level detail 缺失，先增强 candidate turnover logging 或重放输入。",
            "- 确认 `config/parameters/production/current.yaml` 未修改。",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _write_run(
    payload: dict[str, Any],
    output_root: Path,
    as_of: date,
    *,
    debug: bool,
) -> PortfolioTurnoverAttributionRun:
    json_path = default_portfolio_turnover_attribution_json_path(output_root, as_of)
    markdown_path = default_portfolio_turnover_attribution_markdown_path(output_root, as_of)
    write_portfolio_turnover_attribution_report(payload, json_path, markdown_path)
    debug_path = None
    if debug:
        debug_path = default_portfolio_turnover_attribution_debug_path(output_root, as_of)
        write_portfolio_turnover_attribution_debug(payload, debug_path)
    return PortfolioTurnoverAttributionRun(
        as_of=as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        debug_path=debug_path,
    )


def _resolve_weight_tuning_summary_path(
    *,
    as_of: date | None,
    weight_tuning_path: Path | None,
) -> Path | None:
    if weight_tuning_path is not None:
        return weight_tuning_path
    root = PROJECT_ROOT / "artifacts" / "weight_tuning"
    if as_of is not None:
        return root / as_of.isoformat() / "weight_tuning_summary.json"
    return latest_weight_tuning_path(root)


def _resolve_output_date(
    *,
    as_of: date | None,
    weight_tuning_path: Path | None,
    generated_at: datetime,
) -> date:
    if as_of is not None:
        return as_of
    if weight_tuning_path is not None:
        try:
            return date.fromisoformat(weight_tuning_path.parent.name)
        except ValueError:
            return generated_at.date()
    return generated_at.date()


def _summary_date(summary: Mapping[str, Any], source_path: Path, *, fallback: date) -> date:
    try:
        return weight_tuning_payload_date(summary, source_path)
    except ValueError:
        pass
    raw_as_of = str(summary.get("as_of") or "")
    try:
        return date.fromisoformat(raw_as_of)
    except ValueError:
        return fallback


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
    weight_tuning_path: Path | None,
    candidates_path: Path | None,
    failure_path: Path | None,
    output_root: Path,
    summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = _mapping(_mapping(summary).get("metadata")) if summary else {}
    input_artifacts = {
        "weight_tuning_summary": "" if weight_tuning_path is None else str(weight_tuning_path),
        "weight_tuning_candidates": "" if candidates_path is None else str(candidates_path),
        "weight_tuning_failure": "" if failure_path is None else str(failure_path),
    }
    return {
        "schema_version": PORTFOLIO_TURNOVER_ATTRIBUTION_SCHEMA_VERSION,
        "report_type": PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"portfolio-turnover-attribution-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "BLOCKED",
            "reason": reason,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_config_modified": False,
            "source_task": "TRADING-060",
            "market_regime": metadata.get("market_regime", "ai_after_chatgpt"),
            "market_regime_anchor": metadata.get("market_regime_anchor", "2022-11-30"),
            "requested_date_range": metadata.get("requested_date_range", {}),
        },
        "inputs": input_artifacts,
        "input": input_artifacts,
        "output_artifacts": _output_artifacts(output_root, as_of),
        "source_failure_context": {},
        "summary": {
            "root_cause_category": "insufficient_details",
            "top_failure_reason": "",
            "most_common_guardrail_failure": "",
            "turnover_failed_candidates": 0,
            "near_miss_candidates": 0,
            "portfolio_profile": "",
            "data_quality_status": _mapping(_mapping(summary).get("data_quality")).get(
                "status",
                "UNKNOWN",
            )
            if summary
            else "UNKNOWN",
        },
        "candidate_turnover_summary": {
            "total_failed_by_turnover": 0,
            "avg_turnover_relative_increase": 0.0,
            "max_turnover_relative_increase": 0.0,
            "avg_cost_drag_delta": 0.0,
        },
        "candidate_turnover_attribution": [],
        "cost_drag_attribution": {
            "avg_cost_drag_delta": 0.0,
            "max_cost_drag_delta": 0.0,
            "cost_drag_dominates": False,
            "top_cost_assets": [],
        },
        "asset_turnover_contribution": [],
        "walk_forward_turnover": [],
        "rebalance_attribution": {},
        "near_miss_turnover_analysis": [],
        "root_cause": {
            "category": "insufficient_details",
            "confidence": "HIGH",
            "summary": f"Portfolio turnover attribution is blocked: {reason}.",
        },
        "recommended_next_action": {
            "action": "restore_turnover_attribution_inputs",
            "suggested_task": "TRADING-060 Portfolio Turnover Attribution",
            "reason": reason,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "Turnover attribution is blocked, so no candidate can be promoted.",
        },
        "diagnostic_quality": {
            "candidate_level_turnover_details_available": False,
            "asset_level_replay_available": False,
            "replayed_candidate_count": 0,
            "warnings": [reason],
            "diagnostic_policy": {
                "turnover_relative_increase_limit": DEFAULT_TURNOVER_RELATIVE_INCREASE_LIMIT,
                "asset_concentration_share": ASSET_CONCENTRATION_SHARE,
                "small_trade_ratio_warning": SMALL_TRADE_RATIO_WARNING,
                "small_trade_abs_threshold": SMALL_TRADE_ABS_THRESHOLD,
                "replay_candidate_limit": REPLAY_CANDIDATE_LIMIT,
            },
        },
        "safety": _safety_payload(),
    }


def _output_artifacts(output_root: Path, as_of: date) -> dict[str, str]:
    return {
        "portfolio_turnover_attribution_summary_json": str(
            default_portfolio_turnover_attribution_json_path(output_root, as_of)
        ),
        "portfolio_turnover_attribution_summary_md": str(
            default_portfolio_turnover_attribution_markdown_path(output_root, as_of)
        ),
        "portfolio_turnover_candidates_debug": str(
            default_portfolio_turnover_attribution_debug_path(output_root, as_of)
        ),
    }


def _candidate_is_turnover_rejected(candidate: Mapping[str, Any]) -> bool:
    reasons = set(_candidate_reasons(candidate))
    if reasons & TURNOVER_REASONS:
        return True
    relative = _mapping(candidate.get("relative_metrics"))
    guardrail_status = str(
        candidate.get("guardrail_status") or _mapping(candidate.get("guardrails")).get("status")
    )
    if _float_value(relative.get("turnover_delta")) > 0.0 and guardrail_status == "FAIL":
        return True
    return _float_value(relative.get("cost_drag_delta")) > 0.0 and (
        str(candidate.get("status") or "") == "rejected"
    )


def _candidate_reasons(candidate: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for reason in _strings(candidate.get("rejection_reasons")):
        reasons.append(RAW_GUARDRAIL_REASON_MAP.get(reason, reason))
    guardrails = _mapping(candidate.get("guardrails"))
    for reason in _strings(guardrails.get("hard_rejections")):
        reasons.append(RAW_GUARDRAIL_REASON_MAP.get(reason, reason))
    if _float_value(_mapping(candidate.get("relative_metrics")).get("cost_drag_delta")) > 0.0:
        reasons.append("cost_drag_too_high")
    return sorted(dict.fromkeys(reasons))


def _candidate_has_turnover_detail(candidate: Mapping[str, Any]) -> bool:
    relative = _mapping(candidate.get("relative_metrics"))
    metrics = _mapping(candidate.get("metrics"))
    guardrails = _mapping(candidate.get("guardrails"))
    has_core_turnover = any(
        _has_number(mapping, key)
        for mapping, key in (
            (relative, "candidate_turnover"),
            (relative, "turnover_delta"),
            (relative, "turnover_relative_increase"),
            (metrics, "turnover"),
        )
    )
    guardrail_turnover = _coalesce_float(
        guardrails.get("turnover_relative_increase"),
        default=None,
    )
    return has_core_turnover or (guardrail_turnover is not None and guardrail_turnover > 0.0)


def _candidate_turnover_row(
    candidate: Mapping[str, Any],
    *,
    guardrail_policy: Mapping[str, Any],
) -> dict[str, Any]:
    relative = _mapping(candidate.get("relative_metrics"))
    metrics = _mapping(candidate.get("metrics"))
    guardrails = _mapping(candidate.get("guardrails"))
    turnover = _coalesce_float(relative.get("candidate_turnover"), metrics.get("turnover"))
    baseline_turnover = _coalesce_float(relative.get("baseline_turnover"))
    turnover_delta = _coalesce_float(
        relative.get("turnover_delta"),
        None if turnover is None or baseline_turnover is None else turnover - baseline_turnover,
    )
    if baseline_turnover is None and turnover is not None and turnover_delta is not None:
        baseline_turnover = turnover - turnover_delta
    turnover_relative = _coalesce_float(
        relative.get("turnover_relative_increase"),
        guardrails.get("turnover_relative_increase"),
    )
    if turnover_relative is None and baseline_turnover not in {None, 0.0}:
        turnover_relative = max((turnover_delta or 0.0) / abs(baseline_turnover), 0.0)
    candidate_cost = _coalesce_float(metrics.get("estimated_cost_drag"))
    cost_delta = _coalesce_float(relative.get("cost_drag_delta"))
    baseline_cost = None
    if candidate_cost is not None and cost_delta is not None:
        baseline_cost = candidate_cost - cost_delta
    threshold = _turnover_limit(guardrail_policy)
    turnover_excess = max((turnover_relative or 0.0) - threshold, 0.0)
    return {
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "status": str(candidate.get("status") or "UNKNOWN"),
        "guardrail_status": str(
            candidate.get("guardrail_status")
            or _mapping(candidate.get("guardrails")).get("status", "UNKNOWN")
        ),
        "failed_reasons": _candidate_reasons(candidate),
        "turnover": _round_float(turnover),
        "baseline_turnover": _round_float(baseline_turnover),
        "turnover_delta": _round_float(turnover_delta),
        "turnover_relative_increase": _round_float(turnover_relative),
        "turnover_guardrail_threshold": _round_float(threshold),
        "turnover_excess": _round_float(turnover_excess),
        "cost_drag": {
            "candidate_cost_drag": _round_float(candidate_cost),
            "baseline_cost_drag": _round_float(baseline_cost),
            "cost_drag_delta": _round_float(cost_delta),
            "cost_drag_relative_increase": _round_float(
                (cost_delta or 0.0) / abs(baseline_cost)
                if baseline_cost not in {None, 0.0}
                else (1.0 if (cost_delta or 0.0) > 0.0 else 0.0)
            ),
        },
        "weights": _mapping(candidate.get("weights")),
        "l1_distance_from_baseline": _round_float(candidate.get("l1_distance_from_baseline")),
        "objective_score": _round_float(
            _mapping(candidate.get("objective_breakdown")).get("objective_score")
        ),
        "walk_forward_windows": _records(candidate.get("walk_forward_windows")),
        "asset_turnover_contribution": _candidate_asset_details(candidate),
        "rebalance_attribution": _candidate_rebalance_details(candidate),
        "score_volatility": _candidate_score_volatility(candidate),
    }


def _candidate_asset_details(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    explicit = _records(candidate.get("asset_turnover_contribution"))
    if explicit:
        return explicit
    diagnostics = _mapping(candidate.get("turnover_attribution"))
    return _records(diagnostics.get("asset_turnover_contribution"))


def _candidate_rebalance_details(candidate: Mapping[str, Any]) -> dict[str, Any]:
    explicit = _mapping(candidate.get("rebalance_attribution"))
    if explicit:
        return explicit
    diagnostics = _mapping(candidate.get("turnover_attribution"))
    return _mapping(diagnostics.get("rebalance_attribution"))


def _candidate_score_volatility(candidate: Mapping[str, Any]) -> dict[str, Any]:
    explicit = _mapping(candidate.get("score_volatility"))
    if explicit:
        return explicit
    diagnostics = _mapping(candidate.get("turnover_attribution"))
    return _mapping(diagnostics.get("score_volatility"))


def _near_miss_source_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    failure_payload: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    near_miss_ids = {
        str(item.get("candidate_id"))
        for item in _records(failure_payload.get("near_miss_candidates"))
        if item.get("candidate_id")
    }
    if not near_miss_ids:
        return list(candidates[:NEAR_MISS_LIMIT])
    return [
        candidate
        for candidate in candidates
        if str(candidate.get("candidate_id")) in near_miss_ids
    ]


def _replay_turnover_details(
    *,
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    as_of: date,
) -> dict[str, Any]:
    if not candidate_rows:
        return {
            "asset_level_replay_available": False,
            "replayed_candidate_count": 0,
            "candidate_details": {},
            "warnings": [],
        }
    try:
        replayed = _run_replay(summary=summary, candidates=candidates, as_of=as_of)
    except Exception as exc:  # pragma: no cover - exercised through CLI smoke.
        return {
            "asset_level_replay_available": False,
            "replayed_candidate_count": 0,
            "candidate_details": {},
            "warnings": [f"turnover replay unavailable: {exc}"],
        }
    return replayed


def _run_replay(
    *,
    summary: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    as_of: date,
) -> dict[str, Any]:
    inputs = _mapping(summary.get("inputs"))
    metadata = _mapping(summary.get("metadata"))
    config_path = resolve_project_path(
        str(
            inputs.get("weight_tuning_config")
            or metadata.get("config_path")
            or DEFAULT_WEIGHT_TUNING_CONFIG_PATH
        )
    )
    config = load_weight_tuning_config(config_path)
    config_inputs = _mapping(config.get("inputs"))
    shadow_config_path = resolve_project_path(
        str(
            inputs.get("shadow_backtest_config")
            or config_inputs.get("shadow_backtest_config")
            or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH
        )
    )
    portfolio_config_path = resolve_project_path(
        str(
            inputs.get("portfolio_candidate_profiles")
            or config_inputs.get("portfolio_candidate_profiles")
            or DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH
        )
    )
    baseline_path = resolve_project_path(
        str(
            inputs.get("baseline_parameters")
            or config_inputs.get("baseline_parameters")
            or DEFAULT_PRODUCTION_PARAMETERS_PATH
        )
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    portfolio_config = load_portfolio_candidate_config(portfolio_config_path)
    baseline = load_production_parameters(baseline_path)
    prices = shadow_backtest_module._read_prices(
        resolve_project_path(shadow_config.data.prices_path)
    )
    signal_snapshot_path = Path(str(inputs.get("signal_snapshot") or ""))
    if not signal_snapshot_path.is_absolute():
        signal_snapshot_path = PROJECT_ROOT / signal_snapshot_path
    signal_snapshot_payload = load_signal_snapshot_payload(signal_snapshot_path)
    if not signal_snapshot_payload:
        return {
            "asset_level_replay_available": False,
            "replayed_candidate_count": 0,
            "candidate_details": {},
            "warnings": [f"signal snapshot unavailable for replay: {signal_snapshot_path}"],
        }
    signal_frames = signal_snapshot_frames(signal_snapshot_payload)
    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, as_of)
    windows = shadow_backtest_module.generate_walk_forward_windows(
        trading_dates,
        shadow_config.walk_forward,
    )
    start = _full_result_start(windows, trading_dates, shadow_config.market_regime)
    profile_name = str(
        inputs.get("portfolio_profile")
        or config_inputs.get("default_portfolio_profile")
        or "lower_rebalance_threshold_2pct"
    )
    profile_config = _portfolio_profile_config(portfolio_config, profile_name)
    context = _simulation_context(
        profile_name=profile_name,
        profile_config=profile_config,
        prices=prices,
        baseline=baseline,
        shadow_config=shadow_config,
        signal_frames=signal_frames,
        start=start,
        end=as_of,
    )
    baseline_sim = _simulate_weight_context(
        context,
        weights=baseline.weights,
        profile_name=f"{profile_name}__baseline__turnover_attribution",
    )
    details: dict[str, Any] = {}
    selected = [
        candidate
        for candidate in candidates
        if _candidate_is_turnover_rejected(candidate) and _candidate_has_turnover_detail(candidate)
    ][:REPLAY_CANDIDATE_LIMIT]
    for candidate in selected:
        candidate_id = str(candidate.get("candidate_id") or "")
        weights = {
            key: float(value)
            for key, value in _mapping(candidate.get("weights")).items()
            if _is_number(value)
        }
        if not weights:
            continue
        candidate_sim = _simulate_weight_context(
            context,
            weights=weights,
            profile_name=f"{profile_name}__{candidate_id}__turnover_attribution",
        )
        details[candidate_id] = {
            "asset_turnover_contribution": _asset_contribution_from_frames(
                candidate_sim.actual,
                baseline_sim.actual,
            ),
            "rebalance_attribution": _rebalance_from_frames(
                candidate_sim.actual,
                baseline_sim.actual,
            ),
            "score_volatility": _score_volatility(candidate_sim.adjusted_score),
        }
    return {
        "asset_level_replay_available": bool(details),
        "replayed_candidate_count": len(details),
        "candidate_details": details,
        "warnings": [],
    }


def _merge_replay_candidate_rows(
    candidate_rows: Sequence[Mapping[str, Any]],
    replay: Mapping[str, Any],
) -> list[dict[str, Any]]:
    replay_details = _mapping(replay.get("candidate_details"))
    rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        candidate_id = str(row.get("candidate_id") or "")
        detail = _mapping(replay_details.get(candidate_id))
        merged = dict(row)
        if detail:
            if not _records(merged.get("asset_turnover_contribution")):
                merged["asset_turnover_contribution"] = _records(
                    detail.get("asset_turnover_contribution")
                )
            if not _mapping(merged.get("rebalance_attribution")):
                merged["rebalance_attribution"] = _mapping(detail.get("rebalance_attribution"))
            if not _mapping(merged.get("score_volatility")):
                merged["score_volatility"] = _mapping(detail.get("score_volatility"))
        rows.append(merged)
    return rows


def _asset_contribution_from_frames(
    candidate_actual: pd.DataFrame,
    baseline_actual: pd.DataFrame,
) -> list[dict[str, Any]]:
    if candidate_actual.empty:
        return []
    candidate_delta = candidate_actual.diff().abs().fillna(candidate_actual.abs())
    baseline_delta = baseline_actual.reindex_like(candidate_actual).diff().abs().fillna(
        baseline_actual.reindex_like(candidate_actual).abs()
    )
    contribution = candidate_delta.sum(axis=0).sort_values(ascending=False)
    total = float(contribution.sum())
    rows: list[dict[str, Any]] = []
    for symbol, value in contribution.items():
        contribution_value = float(value)
        if contribution_value <= EPSILON:
            continue
        asset_delta = candidate_delta[symbol]
        nonzero = asset_delta[asset_delta > EPSILON]
        baseline_value = float(baseline_delta.get(symbol, pd.Series(dtype=float)).sum())
        rows.append(
            {
                "symbol": str(symbol),
                "turnover_contribution": _round_float(contribution_value),
                "baseline_turnover_contribution": _round_float(baseline_value),
                "incremental_turnover_contribution": _round_float(
                    max(contribution_value - baseline_value, 0.0)
                ),
                "turnover_share": _round_float(contribution_value / total if total else 0.0),
                "rebalance_count": int((asset_delta > EPSILON).sum()),
                "avg_weight_change": _round_float(
                    float(nonzero.mean()) if not nonzero.empty else 0.0
                ),
            }
        )
    return rows


def _rebalance_from_frames(
    candidate_actual: pd.DataFrame,
    baseline_actual: pd.DataFrame,
) -> dict[str, Any]:
    if candidate_actual.empty:
        return {}
    candidate_delta = candidate_actual.diff().abs().fillna(candidate_actual.abs())
    baseline_delta = baseline_actual.reindex_like(candidate_actual).diff().abs().fillna(
        baseline_actual.reindex_like(candidate_actual).abs()
    )
    daily_candidate = candidate_delta.sum(axis=1)
    daily_baseline = baseline_delta.sum(axis=1)
    rebalance_mask = daily_candidate > EPSILON
    baseline_mask = daily_baseline > EPSILON
    changed_counts = (candidate_delta > EPSILON).sum(axis=1)
    changed_when_rebalanced = changed_counts[rebalance_mask]
    trade_values = candidate_delta.stack()
    active_trades = trade_values[trade_values > EPSILON]
    small_trade_ratio = (
        float((active_trades <= SMALL_TRADE_ABS_THRESHOLD).mean())
        if not active_trades.empty
        else 0.0
    )
    warning = ""
    if small_trade_ratio >= SMALL_TRADE_RATIO_WARNING:
        warning = (
            "Candidate generates many small rebalances; threshold or minimum trade size "
            "may be too sensitive."
        )
    return {
        "rebalance_days": int(rebalance_mask.sum()),
        "baseline_rebalance_days": int(baseline_mask.sum()),
        "extra_rebalance_days": max(int(rebalance_mask.sum()) - int(baseline_mask.sum()), 0),
        "avg_assets_changed_per_rebalance": _round_float(
            float(changed_when_rebalanced.mean()) if not changed_when_rebalanced.empty else 0.0
        ),
        "max_assets_changed_per_rebalance": int(
            changed_when_rebalanced.max() if not changed_when_rebalanced.empty else 0
        ),
        "small_trade_ratio": _round_float(small_trade_ratio),
        "warning": warning,
    }


def _score_volatility(adjusted_score: pd.DataFrame) -> dict[str, Any]:
    if adjusted_score.empty:
        return {
            "mean_abs_score_change": 0.0,
            "score_change_asset_day_ratio": 0.0,
        }
    score_delta = adjusted_score.diff().abs().iloc[1:]
    if score_delta.empty:
        return {
            "mean_abs_score_change": 0.0,
            "score_change_asset_day_ratio": 0.0,
        }
    return {
        "mean_abs_score_change": _round_float(float(score_delta.stack().mean())),
        "score_change_asset_day_ratio": _round_float(
            float((score_delta > SMALL_TRADE_ABS_THRESHOLD).stack().mean())
        ),
    }


def _asset_turnover_contribution(
    candidate_rows: Sequence[Mapping[str, Any]],
    replay: Mapping[str, Any],
) -> list[dict[str, Any]]:
    del replay
    counter: dict[str, dict[str, float | int]] = {}
    for row in candidate_rows:
        for item in _records(row.get("asset_turnover_contribution")):
            symbol = str(item.get("symbol") or "")
            if not symbol:
                continue
            bucket = counter.setdefault(
                symbol,
                {
                    "turnover_contribution": 0.0,
                    "baseline_turnover_contribution": 0.0,
                    "incremental_turnover_contribution": 0.0,
                    "rebalance_count": 0,
                    "avg_weight_change_sum": 0.0,
                    "count": 0,
                },
            )
            bucket["turnover_contribution"] = float(bucket["turnover_contribution"]) + _float_value(
                item.get("turnover_contribution")
            )
            bucket["baseline_turnover_contribution"] = float(
                bucket["baseline_turnover_contribution"]
            ) + _float_value(item.get("baseline_turnover_contribution"))
            bucket["incremental_turnover_contribution"] = float(
                bucket["incremental_turnover_contribution"]
            ) + _float_value(item.get("incremental_turnover_contribution"))
            bucket["rebalance_count"] = int(bucket["rebalance_count"]) + _int_value(
                item.get("rebalance_count")
            )
            bucket["avg_weight_change_sum"] = float(
                bucket["avg_weight_change_sum"]
            ) + _float_value(item.get("avg_weight_change"))
            bucket["count"] = int(bucket["count"]) + 1
    total = sum(float(item["turnover_contribution"]) for item in counter.values())
    rows: list[dict[str, Any]] = []
    for symbol, item in counter.items():
        count = max(int(item["count"]), 1)
        contribution = float(item["turnover_contribution"])
        rows.append(
            {
                "symbol": symbol,
                "turnover_contribution": _round_float(contribution),
                "baseline_turnover_contribution": _round_float(
                    item["baseline_turnover_contribution"]
                ),
                "incremental_turnover_contribution": _round_float(
                    item["incremental_turnover_contribution"]
                ),
                "turnover_share": _round_float(contribution / total if total else 0.0),
                "rebalance_count": int(item["rebalance_count"]),
                "avg_weight_change": _round_float(float(item["avg_weight_change_sum"]) / count),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            -_float_value(item.get("turnover_contribution")),
            str(item.get("symbol")),
        ),
    )


def _walk_forward_turnover(
    candidate_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    del candidates
    buckets: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        for window in _records(row.get("walk_forward_windows")):
            window_id = str(window.get("window_id") or "")
            if not window_id:
                continue
            relative = _mapping(window.get("relative_metrics"))
            candidate_metrics = _mapping(window.get("candidate_metrics"))
            baseline_metrics = _mapping(window.get("baseline_metrics"))
            bucket = buckets.setdefault(
                window_id,
                {
                    "window_id": window_id,
                    "candidate_turnover_values": [],
                    "baseline_turnover_values": [],
                    "cost_drag_values": [],
                    "fail_count": 0,
                    "candidate_count": 0,
                    "statuses": Counter(),
                },
            )
            bucket["candidate_turnover_values"].append(
                _coalesce_float(
                    relative.get("candidate_turnover"),
                    candidate_metrics.get("turnover"),
                    0.0,
                )
            )
            bucket["baseline_turnover_values"].append(
                _coalesce_float(
                    relative.get("baseline_turnover"),
                    baseline_metrics.get("turnover"),
                    0.0,
                )
            )
            bucket["cost_drag_values"].append(_float_value(relative.get("cost_drag_delta")))
            status = str(window.get("status") or "UNKNOWN")
            bucket["statuses"].update([status])
            bucket["candidate_count"] += 1
            if status not in {"PASS", "non_worse"}:
                bucket["fail_count"] += 1
    rows: list[dict[str, Any]] = []
    for bucket in buckets.values():
        statuses = bucket["statuses"]
        guardrail_status = "FAIL" if bucket["fail_count"] else "PASS"
        rows.append(
            {
                "window_id": bucket["window_id"],
                "candidate_turnover": _round_float(_mean(bucket["candidate_turnover_values"])),
                "baseline_turnover": _round_float(_mean(bucket["baseline_turnover_values"])),
                "turnover_delta": _round_float(
                    _mean(bucket["candidate_turnover_values"])
                    - _mean(bucket["baseline_turnover_values"])
                ),
                "cost_drag_delta": _round_float(_mean(bucket["cost_drag_values"])),
                "guardrail_status": guardrail_status,
                "candidate_count": bucket["candidate_count"],
                "failed_candidate_count": bucket["fail_count"],
                "most_common_status": statuses.most_common(1)[0][0] if statuses else "UNKNOWN",
            }
        )
    return sorted(rows, key=lambda item: str(item.get("window_id")))


def _rebalance_attribution(
    candidate_rows: Sequence[Mapping[str, Any]],
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    del replay
    rows = [_mapping(row.get("rebalance_attribution")) for row in candidate_rows]
    rows = [row for row in rows if row]
    if not rows:
        return {}
    rebalance_days = [float(_int_value(row.get("rebalance_days"))) for row in rows]
    baseline_days = [float(_int_value(row.get("baseline_rebalance_days"))) for row in rows]
    extra_days = [float(_int_value(row.get("extra_rebalance_days"))) for row in rows]
    changed_avg = [
        _float_value(row.get("avg_assets_changed_per_rebalance")) for row in rows
    ]
    changed_max = [_int_value(row.get("max_assets_changed_per_rebalance")) for row in rows]
    small_ratios = [_float_value(row.get("small_trade_ratio")) for row in rows]
    warning = ""
    if max(small_ratios or [0.0]) >= SMALL_TRADE_RATIO_WARNING:
        warning = (
            "Candidate generates many small rebalances; threshold or minimum trade size "
            "may be too sensitive."
        )
    return {
        "rebalance_days": int(round(_mean(rebalance_days))),
        "baseline_rebalance_days": int(round(_mean(baseline_days))),
        "extra_rebalance_days": int(round(_mean(extra_days))),
        "avg_assets_changed_per_rebalance": _round_float(_mean(changed_avg)),
        "max_assets_changed_per_rebalance": max(changed_max or [0]),
        "small_trade_ratio": _round_float(_mean(small_ratios)),
        "warning": warning,
    }


def _near_miss_turnover_analysis(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    failure_payload: Mapping[str, Any],
    guardrail_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    failure_near_misses = {
        str(item.get("candidate_id")): item
        for item in _records(failure_payload.get("near_miss_candidates"))
        if item.get("candidate_id")
    }
    sorted_rows = sorted(
        candidate_rows,
        key=lambda row: (
            _float_value(row.get("turnover_excess")),
            _float_value(_mapping(row.get("cost_drag")).get("cost_drag_delta")),
            _float_value(row.get("objective_score")),
        ),
        reverse=True,
    )
    threshold = _turnover_limit(guardrail_policy)
    rows: list[dict[str, Any]] = []
    for row in sorted_rows[:NEAR_MISS_LIMIT]:
        candidate_id = str(row.get("candidate_id") or "")
        source = _mapping(failure_near_misses.get(candidate_id))
        why = str(
            source.get("why_interesting")
            or "Improved or ranked candidate failed turnover guardrail."
        )
        turnover_relative = _float_value(row.get("turnover_relative_increase"))
        rows.append(
            {
                "candidate_id": candidate_id,
                "why_near_miss": why,
                "turnover_excess": _round_float(max(turnover_relative - threshold, 0.0)),
                "cost_drag_excess": _round_float(
                    _mapping(row.get("cost_drag")).get("cost_drag_delta")
                ),
                "possible_fix": _possible_fixes(row),
            }
        )
    return rows


def _possible_fixes(row: Mapping[str, Any]) -> list[str]:
    fixes = ["test turnover-control overlay before expanding the weight search space"]
    if _float_value(row.get("l1_distance_from_baseline")) >= WEIGHT_SEARCH_L1_DISTANCE_WARNING:
        fixes.append("tighten total L1 distance from baseline weights")
    if _float_value(_mapping(row.get("rebalance_attribution")).get("small_trade_ratio")) >= (
        SMALL_TRADE_RATIO_WARNING
    ):
        fixes.append("increase minimum trade size or rebalance threshold")
    if _float_value(_mapping(row.get("cost_drag")).get("cost_drag_delta")) > 0.0:
        fixes.append("review transaction cost drag before changing signal weights")
    return list(dict.fromkeys(fixes))


def _candidate_turnover_summary(candidate_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    relative = [_float_value(row.get("turnover_relative_increase")) for row in candidate_rows]
    cost_deltas = [
        _float_value(_mapping(row.get("cost_drag")).get("cost_drag_delta"))
        for row in candidate_rows
    ]
    return {
        "total_failed_by_turnover": len(candidate_rows),
        "avg_turnover_relative_increase": _round_float(_mean(relative)),
        "max_turnover_relative_increase": _round_float(max(relative or [0.0])),
        "avg_cost_drag_delta": _round_float(_mean(cost_deltas)),
        "max_cost_drag_delta": _round_float(max(cost_deltas or [0.0])),
    }


def _cost_drag_attribution(
    candidate_rows: Sequence[Mapping[str, Any]],
    asset_contribution: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    cost_deltas = [
        _float_value(_mapping(row.get("cost_drag")).get("cost_drag_delta"))
        for row in candidate_rows
    ]
    turnover_deltas = [_float_value(row.get("turnover_delta")) for row in candidate_rows]
    avg_cost = _mean(cost_deltas)
    avg_turnover_delta = _mean(turnover_deltas)
    dominance = (
        avg_cost > 0.0
        and avg_turnover_delta > 0.0
        and (avg_cost / avg_turnover_delta) >= COST_DRAG_DOMINANCE_RATIO
    )
    return {
        "avg_cost_drag_delta": _round_float(avg_cost),
        "max_cost_drag_delta": _round_float(max(cost_deltas or [0.0])),
        "avg_turnover_delta": _round_float(avg_turnover_delta),
        "cost_drag_dominates": bool(dominance),
        "top_cost_assets": [
            str(item.get("symbol"))
            for item in sorted(
                asset_contribution,
                key=lambda row: -_float_value(row.get("turnover_contribution")),
            )[:3]
            if item.get("symbol")
        ],
    }


def _turnover_root_cause(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    asset_contribution: Sequence[Mapping[str, Any]],
    rebalance_attribution: Mapping[str, Any],
    cost_drag: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: str,
) -> dict[str, str]:
    if status == "LIMITED":
        return {
            "category": "insufficient_details",
            "confidence": "HIGH",
            "summary": "Candidate-level turnover details are insufficient for full attribution.",
        }
    if not candidate_rows:
        return {
            "category": "insufficient_details",
            "confidence": "HIGH",
            "summary": "No turnover-rejected candidate was available for attribution.",
        }
    portfolio_profile = str(_mapping(summary.get("inputs")).get("portfolio_profile") or "")
    top_asset_share = (
        max((_float_value(item.get("turnover_share")) for item in asset_contribution), default=0.0)
    )
    avg_l1 = _mean([_float_value(row.get("l1_distance_from_baseline")) for row in candidate_rows])
    extra_rebalance_days = _int_value(rebalance_attribution.get("extra_rebalance_days"))
    small_trade_ratio = _float_value(rebalance_attribution.get("small_trade_ratio"))
    score_volatility = max(
        (
            _float_value(_mapping(row.get("score_volatility")).get("score_change_asset_day_ratio"))
            for row in candidate_rows
        ),
        default=0.0,
    )
    if (
        "lower_rebalance_threshold" in portfolio_profile
        and extra_rebalance_days > 0
        and small_trade_ratio >= SMALL_TRADE_RATIO_WARNING
    ):
        return {
            "category": "rebalance_threshold_too_low",
            "confidence": "HIGH",
            "summary": (
                "Rejected candidates create extra rebalance days and many small trades under "
                f"the `{portfolio_profile}` profile."
            ),
        }
    if score_volatility >= HIGH_ROOT_CAUSE_RATIO and extra_rebalance_days > 0:
        return {
            "category": "score_volatility_too_high",
            "confidence": "MEDIUM",
            "summary": (
                "Candidate weights amplify score changes that flow into frequent "
                "rebalances."
            ),
        }
    if avg_l1 >= WEIGHT_SEARCH_L1_DISTANCE_WARNING:
        return {
            "category": "weight_search_too_aggressive",
            "confidence": "MEDIUM",
            "summary": (
                "Rejected candidates are far from baseline weights, increasing "
                "turnover pressure."
            ),
        }
    if top_asset_share >= ASSET_CONCENTRATION_SHARE:
        return {
            "category": "asset_rotation_too_frequent",
            "confidence": "MEDIUM",
            "summary": "Most turnover is concentrated in a small set of assets.",
        }
    if bool(cost_drag.get("cost_drag_dominates")):
        return {
            "category": "cost_model_too_punitive",
            "confidence": "LOW",
            "summary": (
                "Cost drag is large relative to raw turnover delta; cost assumptions "
                "need review."
            ),
        }
    if extra_rebalance_days > 0:
        return {
            "category": "rebalance_threshold_too_low",
            "confidence": "MEDIUM",
            "summary": "Rejected candidates produce more rebalance days than the baseline.",
        }
    return {
        "category": "mixed",
        "confidence": "LOW",
        "summary": "Turnover and cost drag are distributed across multiple candidate dimensions.",
    }


def _recommended_next_action(root_cause: Mapping[str, Any]) -> dict[str, str]:
    category = str(root_cause.get("category") or "mixed")
    mapping = {
        "rebalance_threshold_too_low": {
            "action": "test_turnover_control_overlay",
            "suggested_task": "TRADING-061 Turnover Control Overlay for Weight Candidates",
            "reason": (
                "Near-miss candidates fail due to excessive rebalance activity under the "
                "current threshold."
            ),
        },
        "score_volatility_too_high": {
            "action": "add_score_smoothing",
            "suggested_task": "TRADING-061 Score Smoothing for Weight Tuning",
            "reason": "Candidate weights amplify score volatility and target-weight changes.",
        },
        "weight_search_too_aggressive": {
            "action": "tighten_weight_search_l1_distance",
            "suggested_task": "TRADING-061 Weight Search Stability Constraints",
            "reason": "High L1 distance from baseline increases portfolio turnover.",
        },
        "asset_rotation_too_frequent": {
            "action": "add_asset_rotation_penalty",
            "suggested_task": "TRADING-061 Asset Rotation Penalty",
            "reason": "Turnover is concentrated in a small set of AI/semiconductor assets.",
        },
        "cost_model_too_punitive": {
            "action": "review_cost_model_assumptions",
            "suggested_task": "TRADING-061 Transaction Cost Assumption Review",
            "reason": "Cost drag is a dominant failure reason while guardrails remain unchanged.",
        },
        "insufficient_details": {
            "action": "enhance_weight_tuning_candidate_turnover_logging",
            "suggested_task": "TRADING-061 Candidate Turnover Detail Logging",
            "reason": "Candidate turnover details are insufficient for full attribution.",
        },
    }
    return mapping.get(
        category,
        {
            "action": "review_mixed_turnover_failure",
            "suggested_task": "TRADING-061 Mixed Turnover Failure Review",
            "reason": "No single turnover or cost driver dominates the rejected candidates.",
        },
    )


def _top_failure_reason(failure_payload: Mapping[str, Any]) -> str:
    ranking = _records(failure_payload.get("failure_ranking"))
    if ranking:
        return str(ranking[0].get("reason") or "")
    guardrail = _mapping(failure_payload.get("guardrail_failure"))
    return str(guardrail.get("most_common_guardrail_failure") or "")


def _turnover_limit(guardrail_policy: Mapping[str, Any]) -> float:
    return _float_value(
        guardrail_policy.get("turnover_relative_increase_limit"),
        default=DEFAULT_TURNOVER_RELATIVE_INCREASE_LIMIT,
    )


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "production_write_allowed": False,
        "production_config_modified": False,
        "candidate_promotion_triggered": False,
        "broker_action": False,
        "trading_action": False,
        "turnover_guardrail_modified": False,
        "cost_model_modified": False,
        "new_weight_candidate_generated": False,
        "rejected_candidate_enabled": False,
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if item not in {None, ""}]
    return []


def _has_number(mapping: Mapping[str, Any], key: str) -> bool:
    return _coalesce_float(mapping.get(key), default=None) is not None


def _is_number(value: object) -> bool:
    return _coalesce_float(value, default=None) is not None


def _coalesce_float(*values: object, default: float | None = None) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _float_value(value: object, *, default: float = 0.0) -> float:
    result = _coalesce_float(value, default=None)
    return default if result is None else result


def _int_value(value: object, *, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _round_float(value: object, *, digits: int = 12) -> float:
    return round(_float_value(value), digits)


def _format_float(value: object) -> str:
    return f"{_float_value(value):.4f}"


def _mean(values: Sequence[float | None]) -> float:
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else 0.0
