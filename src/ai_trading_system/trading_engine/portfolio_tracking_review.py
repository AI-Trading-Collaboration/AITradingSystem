from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.parameters.parameter_loader import resolve_project_path
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    default_active_shadow_candidates_path,
    default_portfolio_candidate_tracking_root,
    load_portfolio_candidate_tracking_payload,
    portfolio_candidate_tracking_payload_date,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PORTFOLIO_TRACKING_REVIEW_SCHEMA_VERSION = 1
PORTFOLIO_TRACKING_REVIEW_REPORT_TYPE = "portfolio_tracking_review"
PORTFOLIO_TRACKING_REVIEW_ALIAS_REPORT_TYPE = "portfolio_tracking_review_report"
DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_tracking_review.yaml"
)
DEFAULT_PORTFOLIO_TRACKING_REVIEW_WINDOWS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_tracking_review_windows.yaml"
)
RECOMMENDATIONS = {
    "continue_tracking",
    "watch",
    "pause_tracking",
    "retire_candidate",
    "needs_more_data",
    "eligible_for_extended_review",
}
TRACKING_REVIEW_STAGES = {
    "initial_observation",
    "short_window_review",
    "extended_review_ready",
}
WINDOW_ALIASES = {
    "latest": "latest_day",
    "latest-day": "latest_day",
    "latest_day": "latest_day",
    "5d": "rolling_5d",
    "rolling_5d": "rolling_5d",
    "20d": "rolling_20d",
    "rolling_20d": "rolling_20d",
    "since-start": "since_tracking_start",
    "since_start": "since_tracking_start",
    "since-tracking-start": "since_tracking_start",
    "since_tracking_start": "since_tracking_start",
}
STALE_FRESHNESS_STATUSES = {"STALE", "FAILED", "SOURCE_DELAYED"}


@dataclass(frozen=True)
class PortfolioTrackingReviewRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


def default_portfolio_tracking_review_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_tracking_reviews"


def default_portfolio_tracking_review_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_tracking_review_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_tracking_review_dir(output_root, as_of)
        / "portfolio_tracking_review_summary.json"
    )


def default_portfolio_tracking_review_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_tracking_review_dir(output_root, as_of)
        / "portfolio_tracking_review_summary.md"
    )


def portfolio_tracking_review_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_tracking_review_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_tracking_review_{as_of.isoformat()}.md",
    )


def latest_portfolio_tracking_review_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_portfolio_tracking_review_root()
    candidates = sorted(root.glob("*/portfolio_tracking_review_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_tracking_review_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_tracking_review_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_tracking_review_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def load_portfolio_tracking_review_config(
    path: Path | str = DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio tracking review config must be a mapping: {path}")
    _validate_config(payload)
    return payload


def load_portfolio_tracking_review_windows_config(
    path: Path | str = DEFAULT_PORTFOLIO_TRACKING_REVIEW_WINDOWS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio tracking review windows config must be a mapping: {path}")
    _validate_tracking_window_config(payload)
    return payload


def run_portfolio_tracking_review(
    *,
    as_of: date | None = None,
    candidate_profile: str | None = None,
    window: str | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> PortfolioTrackingReviewRun:
    config = load_portfolio_tracking_review_config(config_path)
    output_root = _output_root(config, dry_run=dry_run)
    payload = build_portfolio_tracking_review_payload(
        as_of=as_of,
        candidate_profile=candidate_profile,
        window=window,
        config=config,
        config_path=Path(config_path),
        output_root=output_root,
        dry_run=dry_run,
        generated_at=generated_at,
    )
    review_date = portfolio_tracking_review_payload_date(
        payload,
        default_portfolio_tracking_review_json_path(output_root, datetime.now(tz=UTC).date()),
    )
    json_path = default_portfolio_tracking_review_json_path(output_root, review_date)
    markdown_path = default_portfolio_tracking_review_markdown_path(output_root, review_date)
    payload = {
        **payload,
        "output_artifacts": {
            "summary_json": str(json_path),
            "summary_markdown": str(markdown_path),
        },
    }
    write_portfolio_tracking_review_summary(payload, json_path, markdown_path)
    return PortfolioTrackingReviewRun(
        as_of=review_date,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_portfolio_tracking_review_payload(
    *,
    as_of: date | None,
    candidate_profile: str | None,
    window: str | None,
    config: dict[str, Any],
    config_path: Path,
    output_root: Path,
    dry_run: bool,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    tracking_root = _input_root(config, "portfolio_candidate_tracking_dir")
    resolved_as_of = as_of or _latest_tracking_date(tracking_root) or generated.date()
    review_window = _canonical_window(
        window or str(config.get("default_window") or "since_tracking_start")
    )
    active_state_path = _active_state_path(config)
    active_state = _load_json(active_state_path)
    active_record = _select_active_record(active_state, candidate_profile)
    if not active_record:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=config_path,
            output_root=output_root,
            dry_run=dry_run,
            window=review_window,
            reason="no_active_shadow_candidate",
            active_state_path=active_state_path,
            candidate_profile=candidate_profile or "",
        )
    profile_name = str(active_record.get("profile_name") or candidate_profile or "")
    candidate_id = str(active_record.get("candidate_id") or "")
    observations = _tracking_observations(
        tracking_root=tracking_root,
        as_of=resolved_as_of,
        candidate_id=candidate_id,
        profile_name=profile_name,
    )
    if not observations:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=config_path,
            output_root=output_root,
            dry_run=dry_run,
            window=review_window,
            reason="missing_tracking_summary",
            active_state_path=active_state_path,
            candidate_profile=profile_name,
            active_record=active_record,
        )
    latest = observations[-1]
    resolved_as_of = latest["date"]
    total_tracking_days = len(observations)
    selected_observations = _window_observations(observations, review_window)
    performance = _performance_review(selected_observations, active_record, review_window)
    signal_review = _signal_transmission_review(selected_observations)
    hard_rejections = _hard_rejections(latest)
    guardrails = _risk_guardrails(
        performance=performance,
        signal_review=signal_review,
        hard_rejections=hard_rejections,
        config=config,
    )
    recommendation = _recommendation(
        tracking_days=total_tracking_days,
        performance=performance,
        signal_review=signal_review,
        guardrails=guardrails,
        hard_rejections=hard_rejections,
        config=config,
    )
    metadata_status = _metadata_status(
        recommendation["status"],
        hard_rejections,
        total_tracking_days,
        config,
    )
    freshness_status = str(latest["freshness"].get("status") or "MISSING")
    data_gate_status = str(latest["data_gate"].get("status") or "UNKNOWN")
    tracking_status = str(latest["candidate"].get("tracking_status") or "UNKNOWN")
    tracking_window_progress = _tracking_window_progress(
        tracking_days=total_tracking_days,
        config=config,
        recommendation_status=str(recommendation.get("status") or ""),
        data_gate_status=data_gate_status,
        freshness_status=freshness_status,
        tracking_status=tracking_status,
    )
    supporting = _supporting_artifacts(
        active_state_path=active_state_path,
        latest=latest,
        observations=observations,
    )
    return {
        "schema_version": PORTFOLIO_TRACKING_REVIEW_SCHEMA_VERSION,
        "report_type": PORTFOLIO_TRACKING_REVIEW_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-tracking-review-{resolved_as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": metadata_status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "config_path": str(resolve_project_path(str(config_path))),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "candidate": {
            "candidate_id": candidate_id,
            "profile_name": profile_name,
            "review_status": str(latest["candidate"].get("review_status") or ""),
            "review_status_from_state": str(active_record.get("review_decision") or ""),
            "review_status_source": str(active_record.get("source_review_decision") or ""),
            "review_status_date": str(latest["candidate"].get("review_decision_date") or ""),
            "review_status_current": str(latest["candidate"].get("review_status") or ""),
            "review_status_latest_tracking": str(latest["candidate"].get("review_status") or ""),
            "tracking_status": str(latest["candidate"].get("tracking_status") or "UNKNOWN"),
            "tracking_start_date": _tracking_start_date(active_record, observations),
            "tracking_days": total_tracking_days,
            "review_status_recommendation": recommendation["status"],
        },
        "data_readiness": {
            "data_gate": data_gate_status,
            "freshness_status": freshness_status,
            "effective_data_date": latest["date_resolution"].get("effective_data_date", ""),
            "tracking_readiness": latest["freshness"].get("tracking_readiness", "unknown"),
            "quality_report": latest["data_gate"].get("manifest", ""),
        },
        "tracking_window": {
            **tracking_window_progress,
            "window": review_window,
            "requested_window": window or config.get("default_window", "since_tracking_start"),
            "available_tracking_days": total_tracking_days,
            "window_observation_days": len(selected_observations),
            "start_date": selected_observations[0]["date"].isoformat(),
            "end_date": selected_observations[-1]["date"].isoformat(),
        },
        "performance_review": performance,
        "signal_transmission_review": signal_review,
        "risk_guardrails": guardrails,
        "recommendation": recommendation,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Tracking review is advisory only. Production promotion remains disabled "
                "and requires a separate manual promotion gate."
            ),
        },
        "supporting_artifacts": supporting["artifacts"],
        "missing_supporting_artifacts": supporting["missing"],
        "safety": {
            "production_config_modified": latest["safety"].get(
                "production_config_modified",
                False,
            ),
            "production_sha256": _production_sha(config, latest),
            "candidate_promotion_enabled": False,
            "candidate_production_promotion_allowed": False,
            "production_write_allowed": False,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "data_quality_gate_lowered": False,
            **_tracking_window_safety(config),
        },
    }


def write_portfolio_tracking_review_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_portfolio_tracking_review_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_portfolio_tracking_review_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": PORTFOLIO_TRACKING_REVIEW_ALIAS_REPORT_TYPE,
        "source_report_type": PORTFOLIO_TRACKING_REVIEW_REPORT_TYPE,
    }
    json_path, markdown_path = portfolio_tracking_review_report_alias_paths(reports_dir, as_of)
    return write_portfolio_tracking_review_summary(alias_payload, json_path, markdown_path)


def load_portfolio_tracking_review_payload(path: Path) -> dict[str, Any]:
    return _load_json(path)


def validate_portfolio_tracking_review_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_TRACKING_REVIEW_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        PORTFOLIO_TRACKING_REVIEW_REPORT_TYPE,
        PORTFOLIO_TRACKING_REVIEW_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    tracking_window = _mapping(payload.get("tracking_window"))
    recommendation = _mapping(payload.get("recommendation"))
    promotion = _mapping(payload.get("promotion_impact"))
    safety = _mapping(payload.get("safety"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if recommendation.get("status") not in RECOMMENDATIONS:
        issues.append("recommendation status is invalid")
    stage = str(tracking_window.get("stage") or "")
    if stage not in TRACKING_REVIEW_STAGES:
        issues.append("tracking_window stage is invalid")
    for key in (
        "tracking_days",
        "min_days_for_short_review",
        "min_days_for_extended_review",
        "days_until_short_review",
        "days_until_extended_review",
        "can_form_short_window_conclusion",
        "can_form_extended_review_conclusion",
        "done_condition_met",
    ):
        if key not in tracking_window:
            issues.append(f"tracking_window missing {key}")
    tracking_days = _int_value(
        tracking_window.get("tracking_days"),
        default=_int_value(candidate.get("tracking_days")),
    )
    min_short = _int_value(tracking_window.get("min_days_for_short_review"), default=5)
    if (
        metadata.get("status") != "BLOCKED"
        and tracking_days > 0
        and tracking_days < min_short
        and recommendation.get("status") != "needs_more_data"
    ):
        issues.append("tracking_days below short window must be needs_more_data")
    if promotion.get("can_support_candidate_promotion") is not False:
        issues.append("tracking review must not support candidate promotion")
    if safety.get("production_write_allowed") is not False:
        issues.append("production_write_allowed must be false")
    if safety.get("candidate_promotion_enabled") is not False:
        issues.append("candidate_promotion_enabled must be false")
    if safety.get("candidate_production_promotion_allowed") is not False:
        issues.append("candidate_production_promotion_allowed must be false")
    if safety.get("production_effect") != "none":
        issues.append("safety production_effect must be none")
    if safety.get("manual_review_required") is not True:
        issues.append("safety manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        issues.append("safety auto_promotion must be false")
    if safety.get("data_quality_gate_lowered") is not False:
        issues.append("data_quality_gate_lowered must be false")
    if safety.get("forbid_backfilled_tracking_days") is not True:
        issues.append("forbid_backfilled_tracking_days must be true")
    if safety.get("forbid_synthetic_tracking_days") is not True:
        issues.append("forbid_synthetic_tracking_days must be true")
    return issues


def portfolio_tracking_review_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    if run_id.startswith("portfolio-tracking-review-"):
        try:
            return date.fromisoformat(run_id.removeprefix("portfolio-tracking-review-"))
        except ValueError:
            pass
    tracking_window = _mapping(payload.get("tracking_window"))
    raw_end = str(tracking_window.get("end_date") or "")
    if raw_end:
        try:
            return date.fromisoformat(raw_end)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"cannot infer portfolio tracking review date from {source_path}"
        raise ValueError(message) from exc


def render_portfolio_tracking_review_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    readiness = _mapping(payload.get("data_readiness"))
    window = _mapping(payload.get("tracking_window"))
    performance = _mapping(payload.get("performance_review"))
    baseline = _mapping(performance.get("baseline"))
    candidate_perf = _mapping(performance.get("candidate"))
    relative = _mapping(performance.get("relative_performance"))
    signal = _mapping(payload.get("signal_transmission_review"))
    guardrails = _mapping(payload.get("risk_guardrails"))
    recommendation = _mapping(payload.get("recommendation"))
    promotion = _mapping(payload.get("promotion_impact"))
    safety = _mapping(payload.get("safety"))
    lines = [
        "# Portfolio Tracking Review Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- recommendation: `{recommendation.get('status', 'UNKNOWN')}`",
        f"- candidate profile: `{candidate.get('profile_name', '')}`",
        f"- tracking_status: `{candidate.get('tracking_status', 'UNKNOWN')}`",
        f"- tracking_days: `{candidate.get('tracking_days', 0)}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Candidate Profile",
        "",
        f"- candidate_id: `{candidate.get('candidate_id', '')}`",
        f"- profile_name: `{candidate.get('profile_name', '')}`",
        f"- tracking_start_date: `{candidate.get('tracking_start_date', '')}`",
        "",
        "## 3. Tracking Window Progress",
        "",
    ]
    for key, value in window.items():
        lines.append(f"- `{key}`: `{value}`")
    if recommendation.get("status") == "needs_more_data":
        lines.append(f"- `needs_more_data_reason`: {recommendation.get('reason', '')}")
    lines.extend(["", "## 4. Data Readiness", ""])
    for key, value in readiness.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 5. Baseline vs Candidate Performance",
            "",
            "| Metric | Baseline | Candidate |",
            "|---|---:|---:|",
        ]
    )
    for key in (
        "daily_return",
        "cumulative_return",
        "annualized_return_if_available",
        "volatility_if_available",
        "max_drawdown",
        "current_drawdown",
        "sharpe_ratio_if_available",
        "turnover",
        "estimated_cost_drag",
    ):
        lines.append(f"| `{key}` | {baseline.get(key, '')} | {candidate_perf.get(key, '')} |")
    lines.extend(["", "## 6. Relative Performance", ""])
    for key, value in relative.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 7. Signal Transmission Review", ""])
    for key, value in signal.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 8. Risk Guardrails", ""])
    lines.append(f"- status: `{guardrails.get('status', 'UNKNOWN')}`")
    for warning in _records(guardrails.get("warnings")):
        lines.append(f"- warning: {warning}")
    for rejection in _records(guardrails.get("hard_rejections")):
        lines.append(f"- hard_rejection: `{rejection}`")
    lines.extend(
        [
            "",
            "## 9. Recommendation",
            "",
            f"- status: `{recommendation.get('status', 'UNKNOWN')}`",
            f"- reason: {recommendation.get('reason', '')}",
            f"- allowed_next_step: `{recommendation.get('allowed_next_step', '')}`",
            "",
            "## 10. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 11. Supporting Artifacts",
            "",
        ]
    )
    for key, value in _mapping(payload.get("supporting_artifacts")).items():
        if value:
            lines.append(f"- `{key}`: `{value}`")
    missing = _records(payload.get("missing_supporting_artifacts"))
    if missing:
        lines.append("")
        lines.append("missing_supporting_artifacts:")
        for item in missing:
            lines.append(f"- `{item}`")
    lines.extend(["", "## 12. Manual Review Checklist", ""])
    lines.extend(
        [
            "- Confirm review remains advisory only.",
            "- Confirm production parameters are unchanged.",
            "- Confirm no single-day performance result is used as production approval.",
            "- Confirm data gate and freshness status are acceptable before continuing tracking.",
            "",
            "## 13. Safety",
            "",
        ]
    )
    for key, value in safety.items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    output_root: Path,
    dry_run: bool,
    window: str,
    reason: str,
    active_state_path: Path,
    candidate_profile: str,
    active_record: dict[str, Any] | None = None,
) -> dict[str, Any]:
    supporting = {"active_shadow_candidates": str(active_state_path)}
    missing = [] if active_state_path.exists() else ["active_shadow_candidates"]
    if active_record:
        latest_summary = str(active_record.get("latest_summary") or "")
        if latest_summary:
            supporting["latest_tracking_summary"] = latest_summary
    return {
        "schema_version": PORTFOLIO_TRACKING_REVIEW_SCHEMA_VERSION,
        "report_type": PORTFOLIO_TRACKING_REVIEW_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-tracking-review-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "BLOCKED",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "config_path": str(resolve_project_path(str(config_path))),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "candidate": {
            "candidate_id": str((active_record or {}).get("candidate_id") or ""),
            "profile_name": candidate_profile,
            "tracking_status": str((active_record or {}).get("status") or "UNKNOWN"),
            "tracking_start_date": str((active_record or {}).get("started_at") or ""),
            "tracking_days": 0,
        },
        "data_readiness": {
            "data_gate": "UNKNOWN",
            "freshness_status": "UNKNOWN",
            "effective_data_date": "",
        },
        "tracking_window": {
            **_tracking_window_progress(
                tracking_days=0,
                config=config,
                recommendation_status="pause_tracking",
                data_gate_status="UNKNOWN",
                freshness_status="UNKNOWN",
                tracking_status=str((active_record or {}).get("status") or "UNKNOWN"),
            ),
            "window": window,
            "available_tracking_days": 0,
            "window_observation_days": 0,
            "start_date": as_of.isoformat(),
            "end_date": as_of.isoformat(),
        },
        "performance_review": _empty_performance_review(window),
        "signal_transmission_review": _empty_signal_review(),
        "risk_guardrails": {
            "status": "FAIL",
            "warnings": [reason],
            "hard_rejections": [reason],
            "policy": _mapping(config.get("tracking_guardrails")),
        },
        "recommendation": {
            "status": "pause_tracking",
            "reason": reason,
            "allowed_next_step": "resolve_tracking_review_blocker",
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "Tracking review is blocked and cannot support production promotion.",
        },
        "supporting_artifacts": supporting,
        "missing_supporting_artifacts": missing,
        "output_artifacts": {
            "summary_json": str(default_portfolio_tracking_review_json_path(output_root, as_of)),
            "summary_markdown": str(
                default_portfolio_tracking_review_markdown_path(output_root, as_of)
            ),
        },
        "safety": {
            "production_config_modified": False,
            "production_sha256": _sha256_if_exists(_production_path(config)),
            "candidate_promotion_enabled": False,
            "candidate_production_promotion_allowed": False,
            "production_write_allowed": False,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "data_quality_gate_lowered": False,
            **_tracking_window_safety(config),
        },
    }


def _tracking_observations(
    *,
    tracking_root: Path,
    as_of: date,
    candidate_id: str,
    profile_name: str,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for path in tracking_root.glob("*/portfolio_candidate_tracking_summary.json"):
        payload = load_portfolio_candidate_tracking_payload(path)
        if payload.get("report_type") != "portfolio_candidate_tracking":
            continue
        candidate = _mapping(payload.get("candidate"))
        if candidate_id and str(candidate.get("candidate_id") or "") != candidate_id:
            continue
        if (
            not candidate_id
            and profile_name
            and str(candidate.get("profile_name") or "") != profile_name
        ):
            continue
        try:
            obs_date = portfolio_candidate_tracking_payload_date(payload, path)
        except ValueError:
            continue
        if obs_date > as_of:
            continue
        observations.append(_observation_from_payload(payload, path, obs_date))
    observations.sort(key=lambda item: (item["date"], str(item["path"])))
    deduped: dict[date, dict[str, Any]] = {}
    for item in observations:
        deduped[item["date"]] = item
    return [deduped[key] for key in sorted(deduped)]


def _observation_from_payload(
    payload: dict[str, Any],
    path: Path,
    obs_date: date,
) -> dict[str, Any]:
    metrics = _mapping(payload.get("tracking_metrics"))
    raw = _mapping(metrics.get("raw_observed_metrics"))
    baseline = _mapping(raw.get("baseline"))
    candidate = _mapping(raw.get("candidate"))
    tracking_baseline = _mapping(metrics.get("baseline"))
    tracking_candidate = _mapping(metrics.get("candidate"))
    candidate_payload = _mapping(payload.get("candidate"))
    source_profiles = _source_profile_metrics(payload, candidate_payload.get("profile_name", ""))
    if source_profiles["baseline"]:
        baseline = {**source_profiles["baseline"], **baseline}
    if source_profiles["candidate"]:
        candidate = {**source_profiles["candidate"], **candidate}
    return {
        "date": obs_date,
        "path": path,
        "payload": payload,
        "metadata": _mapping(payload.get("metadata")),
        "candidate": candidate_payload,
        "date_resolution": _mapping(payload.get("date_resolution")),
        "data_gate": _mapping(payload.get("data_gate")),
        "freshness": _mapping(payload.get("market_data_freshness")),
        "risk_guardrails": _mapping(payload.get("risk_guardrails")),
        "safety": _mapping(payload.get("safety")),
        "baseline": baseline,
        "candidate_metrics": candidate,
        "tracking_baseline": tracking_baseline,
        "tracking_candidate": tracking_candidate,
        "supporting_artifacts": _mapping(payload.get("supporting_artifacts")),
        "missing_supporting_artifacts": _records(payload.get("missing_supporting_artifacts")),
    }


def _source_profile_metrics(
    tracking_payload: dict[str, Any],
    profile_name: object,
) -> dict[str, dict[str, Any]]:
    artifacts = _mapping(tracking_payload.get("supporting_artifacts"))
    candidates_path = str(artifacts.get("portfolio_candidates") or "")
    candidates_payload = (
        _load_json(resolve_project_path(candidates_path))
        if candidates_path
        else {}
    )
    baseline_profile = _mapping(candidates_payload.get("baseline"))
    candidate_profile = {}
    for profile in _records(candidates_payload.get("profiles")):
        if str(profile.get("profile_name") or "") == str(profile_name or ""):
            candidate_profile = _mapping(profile)
            break
    return {
        "baseline": _flatten_profile_metrics(baseline_profile),
        "candidate": _flatten_profile_metrics(candidate_profile),
    }


def _flatten_profile_metrics(profile: dict[str, Any]) -> dict[str, Any]:
    performance = _mapping(profile.get("performance"))
    transmission = _mapping(profile.get("signal_transmission"))
    risk = _mapping(profile.get("risk_guardrails"))
    return {
        "cumulative_return": _round_float(performance.get("cumulative_return")),
        "annualized_return": _round_float(performance.get("annualized_return")),
        "max_drawdown": _round_float(performance.get("max_drawdown")),
        "drawdown": _round_float(performance.get("max_drawdown")),
        "volatility": _round_float(performance.get("volatility")),
        "sharpe_ratio": _round_float(performance.get("sharpe_ratio")),
        "turnover": _round_float(performance.get("turnover")),
        "estimated_cost_drag": _round_float(performance.get("estimated_cost_drag")),
        "rebalance_count": _int_value(transmission.get("rebalance_days")),
        "rebalance_suppression_ratio": _round_float(
            transmission.get("rebalance_suppression_ratio")
        ),
        "mean_abs_actual_weight_delta": _round_float(
            transmission.get("mean_abs_actual_weight_delta")
        ),
        "signal_transmission_score": _round_float(
            transmission.get("target_to_actual_weight_effectiveness")
        ),
        "target_to_actual_weight_effectiveness": _round_float(
            transmission.get("target_to_actual_weight_effectiveness")
        ),
        "risk_guardrail_status": risk.get("guardrail_status", "UNKNOWN"),
    }


def _performance_review(
    observations: list[dict[str, Any]],
    active_record: dict[str, Any],
    window: str,
) -> dict[str, Any]:
    baseline = _performance_metrics(
        observations=observations,
        side="baseline",
        active_record=active_record,
    )
    candidate = _performance_metrics(
        observations=observations,
        side="candidate",
        active_record=active_record,
    )
    relative = {
        "excess_return": _round_float(
            _float_value(candidate.get("cumulative_return"))
            - _float_value(baseline.get("cumulative_return"))
        ),
        "excess_return_bps": int(
            round(
                (
                    _float_value(candidate.get("cumulative_return"))
                    - _float_value(baseline.get("cumulative_return"))
                )
                * 10000
            )
        ),
        "drawdown_delta": _round_float(
            _float_value(candidate.get("max_drawdown"))
            - _float_value(baseline.get("max_drawdown"))
        ),
        "turnover_delta": _round_float(
            _float_value(candidate.get("turnover")) - _float_value(baseline.get("turnover"))
        ),
        "cost_drag_delta": _round_float(
            _float_value(candidate.get("estimated_cost_drag"))
            - _float_value(baseline.get("estimated_cost_drag"))
        ),
        "risk_adjusted_delta": _round_float(
            _float_value(candidate.get("sharpe_ratio_if_available"))
            - _float_value(baseline.get("sharpe_ratio_if_available"))
        ),
    }
    return {
        "window": window,
        "baseline": baseline,
        "candidate": candidate,
        "relative_performance": relative,
    }


def _performance_metrics(
    *,
    observations: list[dict[str, Any]],
    side: str,
    active_record: dict[str, Any],
) -> dict[str, Any]:
    latest = observations[-1]
    start = _start_metrics(observations[0], active_record, side)
    latest_metrics = latest["baseline"] if side == "baseline" else latest["candidate_metrics"]
    daily_returns = _daily_return_series(observations, side)
    cumulative_return = _relative_change(
        latest_metrics.get("cumulative_return"),
        start.get("cumulative_return"),
    )
    window_intervals = max(len(observations) - 1, 0)
    volatility = _annualized_volatility(daily_returns)
    sharpe = _annualized_sharpe(daily_returns)
    annualized = _annualized_return(cumulative_return, window_intervals)
    return {
        "daily_return": _round_float(daily_returns[-1] if daily_returns else 0.0),
        "cumulative_return": _round_float(cumulative_return),
        "annualized_return_if_available": annualized,
        "volatility_if_available": volatility,
        "max_drawdown": _round_float(
            min(_float_value(_metrics_for(obs, side).get("max_drawdown")) for obs in observations)
        ),
        "current_drawdown": _round_float(latest_metrics.get("drawdown")),
        "sharpe_ratio_if_available": sharpe,
        "turnover": _round_float(
            _float_value(latest_metrics.get("turnover")) - _float_value(start.get("turnover"))
        ),
        "estimated_cost_drag": _round_float(
            _float_value(latest_metrics.get("estimated_cost_drag"))
            - _float_value(start.get("estimated_cost_drag"))
        ),
    }


def _signal_transmission_review(observations: list[dict[str, Any]]) -> dict[str, Any]:
    latest = observations[-1]
    baseline = latest["baseline"]
    candidate = latest["candidate_metrics"]
    baseline_rebalance = _int_value(baseline.get("rebalance_count"))
    candidate_rebalance = _int_value(candidate.get("rebalance_count"))
    baseline_score = _float_value(
        baseline.get("target_to_actual_weight_effectiveness"),
        default=_float_value(baseline.get("signal_transmission_score")),
    )
    candidate_score = _float_value(
        candidate.get("target_to_actual_weight_effectiveness"),
        default=_float_value(candidate.get("signal_transmission_score")),
    )
    rebalance_suppression_delta = _round_float(
        _float_value(candidate.get("rebalance_suppression_ratio"), default=1.0 - candidate_score)
        - _float_value(baseline.get("rebalance_suppression_ratio"), default=1.0 - baseline_score)
    )
    mean_abs_delta = _round_float(
        _float_value(candidate.get("mean_abs_actual_weight_delta"))
        - _float_value(baseline.get("mean_abs_actual_weight_delta"))
    )
    turnover_delta = _round_float(
        _float_value(candidate.get("turnover")) - _float_value(baseline.get("turnover"))
    )
    improved = candidate_score > baseline_score or candidate_rebalance > baseline_rebalance
    warning = ""
    if improved and turnover_delta > 0.0:
        warning = "Candidate improves signal transmission but increases turnover."
    return {
        "baseline_rebalance_count": baseline_rebalance,
        "candidate_rebalance_count": candidate_rebalance,
        "rebalance_suppression_delta": rebalance_suppression_delta,
        "mean_abs_weight_delta_delta": mean_abs_delta,
        "signal_to_weight_improved": improved,
        "turnover_increased": turnover_delta > 0.0,
        "warning": warning,
    }


def _risk_guardrails(
    *,
    performance: dict[str, Any],
    signal_review: dict[str, Any],
    hard_rejections: list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    guardrails = _mapping(config.get("tracking_guardrails"))
    relative = _mapping(performance.get("relative_performance"))
    baseline = _mapping(performance.get("baseline"))
    candidate = _mapping(performance.get("candidate"))
    breach_warnings = list(hard_rejections)
    warnings = list(hard_rejections)
    drawdown_worse_by = max(0.0, -_float_value(relative.get("drawdown_delta")))
    drawdown_limit = _float_value(
        guardrails.get("max_drawdown_worse_than_baseline_limit"),
        default=0.02,
    )
    if drawdown_worse_by > drawdown_limit:
        breach_warnings.append("max_drawdown_worse_than_baseline_limit")
        warnings.append("max_drawdown_worse_than_baseline_limit")
    turnover_relative = _relative_increase(
        _float_value(candidate.get("turnover")),
        _float_value(baseline.get("turnover")),
    )
    if turnover_relative > _float_value(
        guardrails.get("turnover_relative_increase_limit"),
        default=0.30,
    ):
        breach_warnings.append("turnover_relative_increase_limit")
        warnings.append("turnover_relative_increase_limit")
    cost_relative = _relative_increase(
        _float_value(candidate.get("estimated_cost_drag")),
        _float_value(baseline.get("estimated_cost_drag")),
    )
    if cost_relative > _float_value(
        guardrails.get("cost_drag_relative_increase_limit"),
        default=0.20,
    ):
        breach_warnings.append("cost_drag_relative_increase_limit")
        warnings.append("cost_drag_relative_increase_limit")
    if signal_review.get("warning"):
        warnings.append(str(signal_review["warning"]))
    warnings = list(dict.fromkeys(item for item in warnings if item))
    return {
        "status": "PASS" if not breach_warnings else "FAIL",
        "warnings": warnings,
        "hard_rejections": hard_rejections,
        "drawdown_worse_by": _round_float(drawdown_worse_by),
        "turnover_relative_increase": _round_float(turnover_relative),
        "cost_drag_relative_increase": _round_float(cost_relative),
        "policy": guardrails,
    }


def _recommendation(
    *,
    tracking_days: int,
    performance: dict[str, Any],
    signal_review: dict[str, Any],
    guardrails: dict[str, Any],
    hard_rejections: list[str],
    config: dict[str, Any],
) -> dict[str, str]:
    if hard_rejections:
        return {
            "status": "pause_tracking",
            "reason": "Tracking review blocked by hard rejection: " + ", ".join(hard_rejections),
            "allowed_next_step": "resolve_tracking_review_blocker",
        }
    guardrail_config = _mapping(config.get("tracking_guardrails"))
    min_positive = _int_value(
        guardrail_config.get("min_tracking_days_for_positive_recommendation"),
        default=5,
    )
    min_extended = _int_value(
        guardrail_config.get("min_tracking_days_for_extended_review"),
        default=20,
    )
    relative = _mapping(performance.get("relative_performance"))
    excess_return = _float_value(relative.get("excess_return"))
    drawdown_delta = _float_value(relative.get("drawdown_delta"))
    turnover_relative = _float_value(guardrails.get("turnover_relative_increase"))
    guardrail_failed = guardrails.get("status") == "FAIL"
    if tracking_days < min_positive:
        return {
            "status": "needs_more_data",
            "reason": (
                f"Only {tracking_days} tracking {_day_label(tracking_days)} "
                f"{_is_are(tracking_days)} available. At least {min_positive} valid "
                "tracking days are required before short-window review."
            ),
            "allowed_next_step": "continue_shadow_tracking",
        }
    if guardrail_failed and (excess_return < 0.0 or drawdown_delta < 0.0):
        return {
            "status": "retire_candidate",
            "reason": "Candidate underperforms baseline and breaches tracking guardrails.",
            "allowed_next_step": "retire_shadow_candidate",
        }
    if guardrail_failed:
        return {
            "status": "pause_tracking",
            "reason": "Candidate review has guardrail warnings that require manual review.",
            "allowed_next_step": "manual_risk_review",
        }
    if (
        tracking_days >= min_extended
        and excess_return > 0.0
        and drawdown_delta >= 0.0
        and turnover_relative
        <= _float_value(guardrail_config.get("turnover_relative_increase_limit"), default=0.30)
        and signal_review.get("signal_to_weight_improved") is True
    ):
        return {
            "status": "eligible_for_extended_review",
            "reason": (
                "Candidate is outperforming baseline with acceptable drawdown and turnover. "
                "This is extended-review eligibility only, not production approval."
            ),
            "allowed_next_step": "start_extended_manual_review",
        }
    return {
        "status": "continue_tracking",
        "reason": (
            "Tracking is active, data readiness is acceptable, "
            "and no guardrail breach was found."
        ),
        "allowed_next_step": "continue_shadow_tracking",
    }


def _tracking_window_progress(
    *,
    tracking_days: int,
    config: dict[str, Any],
    recommendation_status: str,
    data_gate_status: str,
    freshness_status: str,
    tracking_status: str,
) -> dict[str, Any]:
    policy = _tracking_window_policy(config)
    min_short = _int_value(policy.get("min_days_for_short_review"), default=5)
    min_extended = _int_value(policy.get("min_days_for_extended_review"), default=20)
    stage = "initial_observation"
    if tracking_days >= min_extended:
        stage = "extended_review_ready"
    elif tracking_days >= min_short:
        stage = "short_window_review"

    data_ready = data_gate_status == "OK" and freshness_status == "OK"
    tracking_active = tracking_status == "active_tracking"
    can_form_short = tracking_days >= min_short
    can_form_extended = tracking_days >= min_extended
    done_condition_met = (
        can_form_short
        and recommendation_status not in {"", "needs_more_data"}
        and data_ready
        and tracking_active
    )
    return {
        "tracking_days": tracking_days,
        "stage": stage,
        "policy_version": policy.get("version", "portfolio_tracking_review_windows_v0_1"),
        "min_days_for_short_review": min_short,
        "min_days_for_extended_review": min_extended,
        "days_until_short_review": max(0, min_short - tracking_days),
        "days_until_extended_review": max(0, min_extended - tracking_days),
        "can_form_short_window_conclusion": can_form_short,
        "can_form_extended_review_conclusion": can_form_extended,
        "done_condition_met": done_condition_met,
    }


def _tracking_window_safety(config: dict[str, Any]) -> dict[str, bool]:
    policy = _tracking_window_policy(config)
    return {
        "forbid_backfilled_tracking_days": bool(
            policy.get("forbid_backfilled_tracking_days", True)
        ),
        "forbid_synthetic_tracking_days": bool(
            policy.get("forbid_synthetic_tracking_days", True)
        ),
    }


def _tracking_window_policy(config: dict[str, Any]) -> dict[str, Any]:
    raw_policy = _mapping(config.get("tracking_window_policy"))
    policy_path = str(config.get("tracking_window_policy_path") or "").strip()
    if not raw_policy:
        raw_path = policy_path or str(
            _mapping(config.get("input")).get("tracking_window_policy_path") or ""
        ).strip()
        if raw_path:
            resolved_path = resolve_project_path(raw_path)
            if not resolved_path.exists():
                raise ValueError(f"tracking window policy config does not exist: {resolved_path}")
            raw_policy = load_portfolio_tracking_review_windows_config(resolved_path)
        elif DEFAULT_PORTFOLIO_TRACKING_REVIEW_WINDOWS_CONFIG_PATH.exists():
            raw_policy = load_portfolio_tracking_review_windows_config()

    guardrails = _mapping(config.get("tracking_guardrails"))
    windows = _mapping(raw_policy.get("tracking_windows"))
    safety = _mapping(raw_policy.get("safety"))
    min_short = _int_value(
        windows.get("min_days_for_short_review"),
        default=_int_value(
            guardrails.get("min_tracking_days_for_positive_recommendation"),
            default=5,
        ),
    )
    min_extended = _int_value(
        windows.get("min_days_for_extended_review"),
        default=_int_value(
            guardrails.get("min_tracking_days_for_extended_review"),
            default=20,
        ),
    )
    return {
        "version": raw_policy.get("version", "portfolio_tracking_review_windows_inline"),
        "min_days_for_short_review": min_short,
        "min_days_for_extended_review": min_extended,
        "forbid_backfilled_tracking_days": safety.get("forbid_backfilled_tracking_days", True),
        "forbid_synthetic_tracking_days": safety.get("forbid_synthetic_tracking_days", True),
    }


def _hard_rejections(latest: dict[str, Any]) -> list[str]:
    metadata = latest["metadata"]
    candidate = latest["candidate"]
    data_gate = latest["data_gate"]
    freshness = latest["freshness"]
    safety = latest["safety"]
    reasons: list[str] = []
    if data_gate.get("status") != "OK":
        reasons.append("data_gate_not_ok")
    if str(freshness.get("status") or "") in STALE_FRESHNESS_STATUSES:
        reasons.append("freshness_status_stale")
    if candidate.get("tracking_status") != "active_tracking":
        reasons.append("tracking_status_not_active")
    if metadata.get("production_effect") != "none":
        reasons.append("production_effect_not_none")
    if metadata.get("auto_promotion") is True:
        reasons.append("auto_promotion_true")
    if safety.get("production_config_modified") is True:
        reasons.append("production_config_modified")
    return reasons


def _supporting_artifacts(
    *,
    active_state_path: Path,
    latest: dict[str, Any],
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    artifacts = {
        "active_shadow_candidates": str(active_state_path),
        "latest_tracking_summary": str(latest["path"]),
    }
    for key, value in latest["supporting_artifacts"].items():
        if value:
            artifacts[key] = str(value)
    missing = list(latest["missing_supporting_artifacts"])
    if not active_state_path.exists():
        missing.append("active_shadow_candidates")
    if not observations:
        missing.append("portfolio_candidate_tracking_summary")
    return {"artifacts": artifacts, "missing": sorted(set(missing))}


def _window_observations(
    observations: list[dict[str, Any]],
    window: str,
) -> list[dict[str, Any]]:
    if window == "latest_day":
        return observations[-2:] if len(observations) >= 2 else observations[-1:]
    if window == "rolling_5d":
        return observations[-5:]
    if window == "rolling_20d":
        return observations[-20:]
    return observations


def _start_metrics(
    first_observation: dict[str, Any],
    active_record: dict[str, Any],
    side: str,
) -> dict[str, Any]:
    if first_observation["date"].isoformat() == str(active_record.get("started_at") or ""):
        start = _mapping(active_record.get("tracking_start_metrics"))
        state_metrics = _mapping(start.get(side))
        if state_metrics:
            return state_metrics
    return _metrics_for(first_observation, side)


def _metrics_for(observation: dict[str, Any], side: str) -> dict[str, Any]:
    return observation["baseline"] if side == "baseline" else observation["candidate_metrics"]


def _daily_return_series(observations: list[dict[str, Any]], side: str) -> list[float]:
    if not observations:
        return []
    returns: list[float] = []
    previous_metrics: dict[str, Any] | None = None
    for observation in observations:
        current = _metrics_for(observation, side)
        if previous_metrics is None:
            tracking_key = "tracking_baseline" if side == "baseline" else "tracking_candidate"
            returns.append(_float_value(_mapping(observation.get(tracking_key)).get("daily_return")))
        else:
            returns.append(
                _relative_change(
                    current.get("cumulative_return"),
                    previous_metrics.get("cumulative_return"),
                )
            )
        previous_metrics = current
    return [_round_float(item) for item in returns]


def _relative_change(current: object, previous: object) -> float:
    current_value = _float_value(current)
    previous_value = _float_value(previous)
    denominator = 1.0 + previous_value
    if denominator == 0.0:
        return 0.0
    return (1.0 + current_value) / denominator - 1.0


def _annualized_return(cumulative_return: float, intervals: int) -> float | str:
    if intervals <= 0:
        return ""
    try:
        return _round_float((1.0 + cumulative_return) ** (252.0 / intervals) - 1.0)
    except (OverflowError, ValueError):
        return ""


def _annualized_volatility(daily_returns: list[float]) -> float | str:
    cleaned = daily_returns[1:] if len(daily_returns) > 1 else []
    if len(cleaned) < 2:
        return ""
    return _round_float(statistics.pstdev(cleaned) * math.sqrt(252.0))


def _annualized_sharpe(daily_returns: list[float]) -> float | str:
    cleaned = daily_returns[1:] if len(daily_returns) > 1 else []
    if len(cleaned) < 2:
        return ""
    volatility = statistics.pstdev(cleaned)
    if volatility == 0.0:
        return ""
    return _round_float((statistics.mean(cleaned) / volatility) * math.sqrt(252.0))


def _relative_increase(current: float, baseline: float) -> float:
    denominator = abs(baseline)
    if denominator == 0.0:
        return current if current > 0.0 else 0.0
    return (current - baseline) / denominator


def _metadata_status(
    recommendation: str,
    hard_rejections: list[str],
    tracking_days: int,
    config: dict[str, Any],
) -> str:
    if hard_rejections:
        return "BLOCKED"
    min_positive = _int_value(
        _mapping(config.get("tracking_guardrails")).get(
            "min_tracking_days_for_positive_recommendation"
        ),
        default=5,
    )
    if recommendation in {"needs_more_data", "watch"} or tracking_days < min_positive:
        return "LIMITED"
    return "OK"


def _empty_performance_review(window: str) -> dict[str, Any]:
    blank = {
        "daily_return": 0.0,
        "cumulative_return": 0.0,
        "annualized_return_if_available": "",
        "volatility_if_available": "",
        "max_drawdown": 0.0,
        "current_drawdown": 0.0,
        "sharpe_ratio_if_available": "",
        "turnover": 0.0,
        "estimated_cost_drag": 0.0,
    }
    return {
        "window": window,
        "baseline": dict(blank),
        "candidate": dict(blank),
        "relative_performance": {
            "excess_return": 0.0,
            "excess_return_bps": 0,
            "drawdown_delta": 0.0,
            "turnover_delta": 0.0,
            "cost_drag_delta": 0.0,
            "risk_adjusted_delta": 0.0,
        },
    }


def _empty_signal_review() -> dict[str, Any]:
    return {
        "baseline_rebalance_count": 0,
        "candidate_rebalance_count": 0,
        "rebalance_suppression_delta": 0.0,
        "mean_abs_weight_delta_delta": 0.0,
        "signal_to_weight_improved": False,
        "turnover_increased": False,
        "warning": "",
    }


def _select_active_record(
    active_state: dict[str, Any],
    candidate_profile: str | None,
) -> dict[str, Any]:
    records = _records(active_state.get("active_candidates"))
    if candidate_profile:
        for record in records:
            if str(record.get("profile_name") or "") == candidate_profile:
                return _mapping(record)
        return {}
    for record in records:
        if str(record.get("status") or "") == "active_tracking":
            return _mapping(record)
    return _mapping(records[0]) if records else {}


def _tracking_start_date(
    active_record: dict[str, Any],
    observations: list[dict[str, Any]],
) -> str:
    raw = str(active_record.get("started_at") or "")
    if raw:
        return raw
    return observations[0]["date"].isoformat() if observations else ""


def _latest_tracking_date(root: Path) -> date | None:
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_tracking_summary.json"):
        try:
            candidates.append((date.fromisoformat(path.parent.name), path))
        except ValueError:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[0]


def _active_state_path(config: dict[str, Any]) -> Path:
    raw = _mapping(config.get("input")).get("active_shadow_candidates_path")
    if raw:
        return resolve_project_path(str(raw))
    return default_active_shadow_candidates_path(
        _input_root(config, "portfolio_candidate_tracking_dir")
    )


def _input_root(config: dict[str, Any], key: str) -> Path:
    value = _mapping(config.get("input")).get(key)
    default_map = {
        "portfolio_candidate_tracking_dir": default_portfolio_candidate_tracking_root(),
        "portfolio_candidate_reviews_dir": (
            PROJECT_ROOT / "artifacts" / "portfolio_candidate_reviews"
        ),
        "portfolio_candidates_dir": PROJECT_ROOT / "artifacts" / "portfolio_candidates",
        "market_data_freshness_dir": PROJECT_ROOT / "artifacts" / "data_freshness",
        "market_data_refresh_dir": PROJECT_ROOT / "artifacts" / "data_refresh",
        "backtest_snapshot_dir": PROJECT_ROOT / "artifacts" / "backtest_snapshots",
    }
    return resolve_project_path(str(value)) if value else default_map[key]


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    output = _mapping(config.get("output"))
    key = "dry_run_dir" if dry_run else "portfolio_tracking_reviews_dir"
    default = (
        PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_tracking_reviews"
        if dry_run
        else default_portfolio_tracking_review_root()
    )
    raw = output.get(key)
    return resolve_project_path(str(raw)) if raw else default


def _production_path(config: dict[str, Any]) -> Path:
    raw = _mapping(config.get("input")).get("production_parameters_path")
    return resolve_project_path(
        str(raw or PROJECT_ROOT / "config" / "parameters" / "production" / "current.yaml")
    )


def _production_sha(config: dict[str, Any], latest: dict[str, Any]) -> str:
    existing = str(latest["safety"].get("production_sha256") or "")
    return existing or _sha256_if_exists(_production_path(config))


def _canonical_window(value: str) -> str:
    normalized = value.strip().lower().replace(" ", "_")
    if normalized not in WINDOW_ALIASES:
        raise ValueError(
            "unsupported tracking review window: "
            f"{value}; expected latest_day, 5d, 20d, or since-start"
        )
    return WINDOW_ALIASES[normalized]


def _validate_config(config: dict[str, Any]) -> None:
    if config.get("production_effect") != "none":
        raise ValueError("portfolio tracking review production_effect must be none")
    if config.get("manual_review_required") is not True:
        raise ValueError("portfolio tracking review manual_review_required must be true")
    if config.get("auto_promotion") is not False:
        raise ValueError("portfolio tracking review auto_promotion must be false")
    if _mapping(config.get("safety")).get("production_write_allowed") is not False:
        raise ValueError("portfolio tracking review production writes must be disabled")
    configured_recommendations = {
        str(item)
        for item in _records(config.get("performance_review_recommendation"))
    }
    if not RECOMMENDATIONS.issubset(configured_recommendations):
        raise ValueError("portfolio tracking review config missing recommendation statuses")
    for key in (
        "max_drawdown_worse_than_baseline_limit",
        "turnover_relative_increase_limit",
        "cost_drag_relative_increase_limit",
        "min_tracking_days_for_positive_recommendation",
        "min_tracking_days_for_extended_review",
    ):
        if key not in _mapping(config.get("tracking_guardrails")):
            raise ValueError(f"portfolio tracking review missing guardrail: {key}")


def _validate_tracking_window_config(config: dict[str, Any]) -> None:
    windows = _mapping(config.get("tracking_windows"))
    min_short = _int_value(windows.get("min_days_for_short_review"))
    min_extended = _int_value(windows.get("min_days_for_extended_review"))
    if min_short < 1:
        raise ValueError("min_days_for_short_review must be at least 1")
    if min_extended < min_short:
        raise ValueError("min_days_for_extended_review must be >= short review minimum")
    stages = _mapping(config.get("review_stages"))
    missing_stages = TRACKING_REVIEW_STAGES - set(stages)
    if missing_stages:
        raise ValueError(
            "portfolio tracking review windows missing stages: "
            + ", ".join(sorted(missing_stages))
        )
    safety = _mapping(config.get("safety"))
    if safety.get("production_effect") != "none":
        raise ValueError("tracking window production_effect must be none")
    if safety.get("manual_review_required") is not True:
        raise ValueError("tracking window manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        raise ValueError("tracking window auto_promotion must be false")
    if safety.get("forbid_backfilled_tracking_days") is not True:
        raise ValueError("tracking window must forbid backfilled tracking days")
    if safety.get("forbid_synthetic_tracking_days") is not True:
        raise ValueError("tracking window must forbid synthetic tracking days")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_if_exists(path: Path) -> str:
    try:
        if path.exists() and path.is_file():
            return sha256_file(path)
    except OSError:
        return ""
    return ""


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _float_value(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_value(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _round_float(value: object) -> float:
    return round(_float_value(value), 6)


def _day_label(value: int) -> str:
    return "day" if value == 1 else "days"


def _is_are(value: int) -> str:
    return "is" if value == 1 else "are"
