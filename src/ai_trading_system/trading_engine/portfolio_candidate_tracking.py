from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.data_registry_consistency import (
    manifest_context_from_path,
    portfolio_sensitivity_data_gate_context,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    MARKET_DATA_FRESHNESS_STATUSES,
    default_market_data_freshness_root,
    latest_market_data_freshness_path_on_or_before,
    load_market_data_freshness_payload,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    latest_portfolio_candidate_review_decision_path,
    load_portfolio_candidate_review_payload,
    portfolio_candidate_review_payload_date,
)
from ai_trading_system.trading_engine.portfolio_candidates import (
    latest_portfolio_candidates_path_on_or_before,
    load_portfolio_candidates_payload,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import _backtest_artifact_root
from ai_trading_system.yaml_loader import safe_load_yaml_path

PORTFOLIO_CANDIDATE_TRACKING_SCHEMA_VERSION = 1
PORTFOLIO_CANDIDATE_TRACKING_REPORT_TYPE = "portfolio_candidate_tracking"
PORTFOLIO_CANDIDATE_TRACKING_ALIAS_REPORT_TYPE = "portfolio_candidate_tracking_report"
DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_candidate_tracking.yaml"
)
ELIGIBLE_REVIEW_STATUSES = {"watch", "approved_for_shadow_candidate"}
INELIGIBLE_REVIEW_STATUSES = {"pending_review", "rejected", "needs_more_data"}
SHADOW_CANDIDATE_STATUSES = {
    "not_started",
    "active_tracking",
    "tracking_blocked",
    "degraded_tracking",
    "watch",
    "paused",
    "retired",
}


@dataclass(frozen=True)
class PortfolioCandidateTrackingRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    daily_state_path: Path
    active_state_path: Path | None


def default_portfolio_candidate_tracking_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_candidate_tracking"


def default_portfolio_candidate_tracking_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_candidate_tracking_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_candidate_tracking_dir(output_root, as_of)
        / "portfolio_candidate_tracking_summary.json"
    )


def default_portfolio_candidate_tracking_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_candidate_tracking_dir(output_root, as_of)
        / "portfolio_candidate_tracking_summary.md"
    )


def default_portfolio_candidate_tracking_daily_state_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_candidate_tracking_dir(output_root, as_of)
        / "portfolio_candidate_tracking_state.json"
    )


def default_active_shadow_candidates_path(output_root: Path | None = None) -> Path:
    root = output_root or default_portfolio_candidate_tracking_root()
    return root / "state" / "active_shadow_candidates.json"


def portfolio_candidate_tracking_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_candidate_tracking_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_candidate_tracking_{as_of.isoformat()}.md",
    )


def latest_portfolio_candidate_tracking_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_portfolio_candidate_tracking_root()
    candidates = sorted(root.glob("*/portfolio_candidate_tracking_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_candidate_tracking_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_candidate_tracking_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_tracking_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def load_portfolio_candidate_tracking_config(
    path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio candidate tracking config must be a mapping: {path}")
    _validate_tracking_config(payload)
    return payload


def run_portfolio_candidate_tracking(
    *,
    as_of: date | None = None,
    review_path: Path | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> PortfolioCandidateTrackingRun:
    config = load_portfolio_candidate_tracking_config(config_path)
    output_root = _output_root(config, dry_run=dry_run)
    payload = build_portfolio_candidate_tracking_payload(
        as_of=as_of,
        review_path=review_path,
        config=config,
        config_path=Path(config_path),
        output_root=output_root,
        dry_run=dry_run,
        generated_at=generated_at,
    )
    tracking_date = portfolio_candidate_tracking_payload_date(
        payload,
        default_portfolio_candidate_tracking_json_path(output_root, datetime.now(tz=UTC).date()),
    )
    json_path = default_portfolio_candidate_tracking_json_path(output_root, tracking_date)
    markdown_path = default_portfolio_candidate_tracking_markdown_path(output_root, tracking_date)
    active_state_path = None if dry_run else default_active_shadow_candidates_path(output_root)
    daily_state_path = default_portfolio_candidate_tracking_daily_state_path(
        output_root,
        tracking_date,
    )
    payload = {
        **payload,
        "output_artifacts": {
            "summary_json": str(json_path),
            "summary_markdown": str(markdown_path),
            "daily_state": str(daily_state_path),
            "active_state": "" if active_state_path is None else str(active_state_path),
        },
    }
    state_payload = build_tracking_state_payload(
        payload,
        active_state_path=active_state_path or default_active_shadow_candidates_path(output_root),
        update_global_state=not dry_run,
    )
    write_portfolio_candidate_tracking_summary(payload, json_path, markdown_path)
    write_tracking_state(state_payload, daily_state_path)
    if active_state_path is not None:
        write_tracking_state(state_payload, active_state_path)
    return PortfolioCandidateTrackingRun(
        as_of=tracking_date,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        daily_state_path=daily_state_path,
        active_state_path=active_state_path,
    )


def build_portfolio_candidate_tracking_payload(
    *,
    as_of: date | None,
    review_path: Path | None,
    config: dict[str, Any],
    config_path: Path,
    output_root: Path,
    dry_run: bool,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    shadow_config_path = resolve_project_path(
        str(_input(config, "shadow_backtest_config_path") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    prices = shadow_backtest_module._read_prices(
        resolve_project_path(shadow_config.data.prices_path)
    )
    latest_market_data_date = shadow_backtest_module._latest_price_date(prices)
    tracking_date = as_of or latest_market_data_date or generated.date()
    data_gate = portfolio_sensitivity_data_gate_context(
        as_of=tracking_date,
        config_path=shadow_config_path,
        output_root=_backtest_artifact_root(shadow_config),
    )
    manifest_path = _path_or_none(data_gate.get("manifest"))
    manifest = manifest_context_from_path(manifest_path)
    effective_data_date = manifest.manifest_date
    selected_review_path = _resolve_review_path(config, tracking_date, review_path)
    market_data_freshness = _market_data_freshness_context(config, tracking_date)
    market_data_refresh = _market_data_refresh_context(config, tracking_date)
    decision_payload = load_portfolio_candidate_review_payload(selected_review_path)
    package_path = _source_review_package_path(decision_payload, selected_review_path)
    package_payload = load_portfolio_candidate_review_payload(package_path)
    review_date = _safe_review_date(decision_payload, selected_review_path)
    candidate = _mapping(decision_payload.get("candidate"))
    review_status = _review_status(decision_payload)
    candidate_profile = str(candidate.get("profile_name") or "")
    candidate_artifact_path = _candidate_artifact_path(decision_payload)
    candidate_artifact_date = _parent_date(candidate_artifact_path)
    candidate_hash = str(candidate.get("candidate_hash") or "")
    candidates_path = _portfolio_candidates_path(config, candidate_artifact_path, review_date)
    candidates_payload = load_portfolio_candidates_payload(candidates_path)
    supporting = _supporting_artifacts(
        config=config,
        tracking_date=tracking_date,
        selected_review_path=selected_review_path,
        package_path=package_path,
        candidate_artifact_path=candidate_artifact_path,
        candidates_path=candidates_path,
        manifest_path=manifest_path,
        market_data_freshness_path=_path_or_none(market_data_freshness.get("report")),
        market_data_refresh_path=_path_or_none(market_data_refresh.get("report")),
    )
    previous_state = _load_tracking_state(default_active_shadow_candidates_path(output_root))
    candidate_id = _candidate_id(candidate_profile, review_date, candidate_hash)
    previous_record = _state_record(previous_state, candidate_id)
    block_reasons = _tracking_block_reasons(
        review_status=review_status,
        decision_payload=decision_payload,
        package_payload=package_payload,
        candidate_artifact_path=candidate_artifact_path,
        candidate_hash=candidate_hash,
        candidates_payload=candidates_payload,
        data_gate=data_gate,
        market_data_freshness=market_data_freshness,
        previous_record=previous_record,
    )
    tracking_status = _tracking_status(
        review_status=review_status,
        block_reasons=block_reasons,
        tracking_date=tracking_date,
        effective_data_date=effective_data_date,
        market_data_freshness=market_data_freshness,
    )
    roll_forward_status = _roll_forward_status(
        block_reasons=block_reasons,
        tracking_date=tracking_date,
        review_date=review_date,
        effective_data_date=effective_data_date,
    )
    status = _metadata_status(tracking_status)
    baseline_profile = _mapping(candidates_payload.get("baseline"))
    candidate_profile_payload = _profile_payload(candidates_payload, candidate_profile)
    tracking_metrics = _tracking_metrics(
        baseline_profile=baseline_profile,
        candidate_profile=candidate_profile_payload,
        previous_record=previous_record,
        tracking_status=tracking_status,
    )
    reason = _date_resolution_reason(
        tracking_date=tracking_date,
        review_date=review_date,
        effective_data_date=effective_data_date,
        roll_forward_status=roll_forward_status,
        block_reasons=block_reasons,
    )
    production_hash = _current_production_hash(decision_payload, package_payload)
    production_config_modified = "production_config_hash_changed" in block_reasons
    return {
        "schema_version": PORTFOLIO_CANDIDATE_TRACKING_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATE_TRACKING_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-candidate-tracking-{tracking_date.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "config_path": str(resolved_config_path),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "candidate": {
            "candidate_id": candidate_id,
            "profile_name": candidate_profile,
            "review_status": review_status,
            "review_decision_date": "" if review_date is None else review_date.isoformat(),
            "candidate_artifact_date": (
                "" if candidate_artifact_date is None else candidate_artifact_date.isoformat()
            ),
            "candidate_hash": candidate_hash,
            "source_artifact": str(candidate_artifact_path),
            "tracking_status": tracking_status,
            "shadow_candidate_status": tracking_status,
        },
        "date_resolution": {
            "tracking_date": tracking_date.isoformat(),
            "effective_data_date": (
                "" if effective_data_date is None else effective_data_date.isoformat()
            ),
            "review_decision_date": "" if review_date is None else review_date.isoformat(),
            "candidate_artifact_date": (
                "" if candidate_artifact_date is None else candidate_artifact_date.isoformat()
            ),
            "latest_manifest_date": (
                "" if manifest.manifest_date is None else manifest.manifest_date.isoformat()
            ),
            "latest_market_data_date": (
                "" if latest_market_data_date is None else latest_market_data_date.isoformat()
            ),
            "roll_forward_status": roll_forward_status,
            "status": roll_forward_status,
            "reason": reason,
        },
        "data_gate": {
            "status": data_gate.get("status", "UNKNOWN"),
            "manifest": data_gate.get("manifest", ""),
            "manifest_date": (
                "" if manifest.manifest_date is None else manifest.manifest_date.isoformat()
            ),
            "latest_resolution_status": data_gate.get("latest_resolution_status", "UNKNOWN"),
            "price_cache_registry_status": data_gate.get("price_cache_registry", "UNKNOWN"),
            "symbol_mapping": data_gate.get("symbol_mapping", "UNKNOWN"),
            "reason": data_gate.get("reason", ""),
            "error_code": data_gate.get("error_code", "OK"),
        },
        "market_data_freshness": {
            "status": market_data_freshness.get("status", "MISSING"),
            "report": market_data_freshness.get("report", ""),
            "refresh_status": market_data_refresh.get("status", ""),
            "refresh_report": market_data_refresh.get("report", ""),
            "tracking_readiness": market_data_freshness.get("tracking_readiness", "unknown"),
            "tracking_status_recommendation": market_data_freshness.get(
                "tracking_status_recommendation",
                "",
            ),
            "tracking_recovery": market_data_refresh.get("candidate_tracking_status", ""),
            "suggested_action": market_data_refresh.get("suggested_action", ""),
            "reason": market_data_freshness.get("reason", ""),
        },
        "market_data_refresh": market_data_refresh,
        "daily_tracking": {
            "date": tracking_date.isoformat(),
            "effective_data_date": (
                "" if effective_data_date is None else effective_data_date.isoformat()
            ),
            **tracking_metrics.get("daily_tracking", {}),
        },
        "tracking_metrics": tracking_metrics,
        "risk_guardrails": {
            "status": _risk_status(candidate_profile_payload),
            "warnings": _risk_warnings(candidate_profile_payload),
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": _promotion_reason(tracking_status),
        },
        "date_roll_forward": {
            "roll_forward_status": roll_forward_status,
            "block_reasons": block_reasons,
            "reason": reason,
        },
        "supporting_artifacts": supporting["artifacts"],
        "missing_supporting_artifacts": supporting["missing"],
        "state": {
            "active_state_path": str(default_active_shadow_candidates_path(output_root)),
            "previous_state_found": previous_record is not None,
            "initial_observation": previous_record is None,
        },
        "safety": {
            "production_config_modified": production_config_modified,
            "production_sha256": production_hash,
            "candidate_promotion_enabled": False,
            "candidate_production_promotion_allowed": False,
            "production_write_allowed": False,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "data_quality_gate_lowered": False,
        },
    }


def build_tracking_state_payload(
    payload: dict[str, Any],
    *,
    active_state_path: Path,
    update_global_state: bool,
) -> dict[str, Any]:
    existing = _load_tracking_state(active_state_path)
    active = [
        dict(item)
        for item in _records(existing.get("active_candidates"))
        if str(item.get("status") or "") != "retired"
    ]
    candidate = _mapping(payload.get("candidate"))
    decision = _mapping(payload.get("candidate"))
    date_resolution = _mapping(payload.get("date_resolution"))
    metrics = _mapping(payload.get("tracking_metrics"))
    candidate_id = str(candidate.get("candidate_id") or "")
    if candidate_id and str(candidate.get("tracking_status")) != "not_started":
        record = {
            "candidate_id": candidate_id,
            "profile_name": candidate.get("profile_name", ""),
            "started_at": _started_at(active, candidate_id, date_resolution),
            "review_decision": decision.get("review_status", ""),
            "status": candidate.get("tracking_status", "tracking_blocked"),
            "source_review_decision": _mapping(payload.get("supporting_artifacts")).get(
                "portfolio_candidate_review_decision",
                "",
            ),
            "production_effect": "none",
            "auto_promotion": False,
            "last_tracking_date": date_resolution.get("tracking_date", ""),
            "effective_data_date": date_resolution.get("effective_data_date", ""),
            "latest_summary": _mapping(payload.get("output_artifacts")).get(
                "summary_json",
                "",
            ),
            "last_observed_metrics": metrics.get("raw_observed_metrics", {}),
            "tracking_start_metrics": _tracking_start_metrics(active, candidate_id, metrics),
        }
        replaced = False
        for index, item in enumerate(active):
            if str(item.get("candidate_id") or "") == candidate_id:
                active[index] = {**item, **record}
                replaced = True
                break
        if not replaced:
            active.append(record)
    return {
        "schema_version": PORTFOLIO_CANDIDATE_TRACKING_SCHEMA_VERSION,
        "report_type": "active_shadow_candidates",
        "metadata": {
            "generated_at": _mapping(payload.get("metadata")).get("generated_at", ""),
            "status": _mapping(payload.get("metadata")).get("status", "UNKNOWN"),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "update_global_state": update_global_state,
        },
        "active_candidates": active,
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
        },
    }


def write_portfolio_candidate_tracking_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_portfolio_candidate_tracking_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def write_tracking_state(payload: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_portfolio_candidate_tracking_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": PORTFOLIO_CANDIDATE_TRACKING_ALIAS_REPORT_TYPE,
        "source_report_type": PORTFOLIO_CANDIDATE_TRACKING_REPORT_TYPE,
    }
    json_path, markdown_path = portfolio_candidate_tracking_report_alias_paths(
        reports_dir,
        as_of,
    )
    return write_portfolio_candidate_tracking_summary(alias_payload, json_path, markdown_path)


def load_portfolio_candidate_tracking_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_portfolio_candidate_tracking_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_CANDIDATE_TRACKING_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        PORTFOLIO_CANDIDATE_TRACKING_REPORT_TYPE,
        PORTFOLIO_CANDIDATE_TRACKING_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    freshness = _mapping(payload.get("market_data_freshness"))
    promotion = _mapping(payload.get("promotion_impact"))
    safety = _mapping(payload.get("safety"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if candidate.get("tracking_status") not in SHADOW_CANDIDATE_STATUSES:
        issues.append("tracking_status is invalid")
    freshness_status = str(freshness.get("status") or "")
    if freshness_status and freshness_status not in MARKET_DATA_FRESHNESS_STATUSES | {"MISSING"}:
        issues.append("market_data_freshness status is invalid")
    if promotion.get("can_support_candidate_promotion") is not False:
        issues.append("tracking must not support candidate promotion")
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
    return issues


def portfolio_candidate_tracking_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    if run_id.startswith("portfolio-candidate-tracking-"):
        try:
            return date.fromisoformat(run_id.removeprefix("portfolio-candidate-tracking-"))
        except ValueError:
            pass
    date_resolution = _mapping(payload.get("date_resolution"))
    raw_tracking_date = str(date_resolution.get("tracking_date") or "")
    if raw_tracking_date:
        try:
            return date.fromisoformat(raw_tracking_date)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"cannot infer portfolio candidate tracking date from {source_path}"
        raise ValueError(message) from exc


def render_portfolio_candidate_tracking_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    date_resolution = _mapping(payload.get("date_resolution"))
    data_gate = _mapping(payload.get("data_gate"))
    freshness = _mapping(payload.get("market_data_freshness"))
    tracking_metrics = _mapping(payload.get("tracking_metrics"))
    baseline = _mapping(tracking_metrics.get("baseline"))
    candidate_metrics = _mapping(tracking_metrics.get("candidate"))
    risk = _mapping(payload.get("risk_guardrails"))
    promotion = _mapping(payload.get("promotion_impact"))
    safety = _mapping(payload.get("safety"))
    lines = [
        "# Portfolio Candidate Tracking Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- tracking_status: `{candidate.get('tracking_status', 'UNKNOWN')}`",
        f"- candidate profile: `{candidate.get('profile_name', '')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        "- Shadow tracking is advisory only and does not modify production parameters.",
        "",
        "## 2. Candidate Profile",
        "",
        f"- candidate_id: `{candidate.get('candidate_id', '')}`",
        f"- profile_name: `{candidate.get('profile_name', '')}`",
        f"- candidate_hash: `{candidate.get('candidate_hash', '')}`",
        f"- source_artifact: `{candidate.get('source_artifact', '')}`",
        "",
        "## 3. Review Decision",
        "",
        f"- review_status: `{candidate.get('review_status', '')}`",
        f"- review_decision_date: `{candidate.get('review_decision_date', '')}`",
        "",
        "## 4. Date Roll-forward",
        "",
    ]
    for key in (
        "tracking_date",
        "effective_data_date",
        "review_decision_date",
        "candidate_artifact_date",
        "latest_manifest_date",
        "latest_market_data_date",
        "roll_forward_status",
        "reason",
    ):
        lines.append(f"- `{key}`: `{date_resolution.get(key, '')}`")
    lines.extend(
        [
            "",
            "## 5. Data Gate",
            "",
        ]
    )
    for key in (
        "status",
        "manifest_date",
        "latest_resolution_status",
        "price_cache_registry_status",
        "symbol_mapping",
        "error_code",
        "reason",
    ):
        lines.append(f"- `{key}`: `{data_gate.get(key, '')}`")
    lines.extend(
        [
            "",
            "## 6. Market Data Freshness",
            "",
        ]
    )
    for key in (
        "status",
        "tracking_readiness",
        "tracking_status_recommendation",
        "report",
        "refresh_status",
        "refresh_report",
        "tracking_recovery",
        "suggested_action",
        "reason",
    ):
        lines.append(f"- `{key}`: `{freshness.get(key, '')}`")
    lines.extend(
        [
            "",
            "## 7. Baseline vs Candidate Tracking",
            "",
            "| Metric | Baseline | Candidate |",
            "|---|---:|---:|",
        ]
    )
    for key in (
        "daily_return",
        "cumulative_return_since_tracking_start",
        "drawdown",
        "max_drawdown_since_tracking_start",
        "turnover",
        "rebalance_count",
        "signal_transmission_score",
    ):
        lines.append(f"| `{key}` | {baseline.get(key, '')} | {candidate_metrics.get(key, '')} |")
    lines.append(
        f"| `excess_return_vs_baseline` |  | "
        f"{candidate_metrics.get('excess_return_vs_baseline', '')} |"
    )
    lines.extend(
        [
            "",
            "## 8. Risk Guardrails",
            "",
            f"- status: `{risk.get('status', 'UNKNOWN')}`",
        ]
    )
    for warning in _records(risk.get("warnings")):
        lines.append(f"- warning: {warning}")
    lines.extend(
        [
            "",
            "## 9. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 10. Manual Review Notes",
            "",
            "- Continue manual review before any future production promotion discussion.",
            "- A good tracking window alone must not update production parameters.",
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
    lines.extend(
        [
            "",
            "## 12. Safety",
            "",
        ]
    )
    for key, value in safety.items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _resolve_review_path(
    config: dict[str, Any],
    tracking_date: date,
    review_path: Path | None,
) -> Path:
    if review_path is not None:
        return resolve_project_path(str(review_path))
    root = _input_root(config, "portfolio_candidate_reviews_dir")
    path = _latest_review_decision_by_date_on_or_before(tracking_date, root)
    if path is not None:
        return path
    latest_path = latest_portfolio_candidate_review_decision_path(root)
    if latest_path is None:
        return root / tracking_date.isoformat() / "portfolio_candidate_review_decision.json"
    return latest_path


def _latest_review_decision_by_date_on_or_before(as_of: date, root: Path) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_review_decision.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _source_review_package_path(decision_payload: dict[str, Any], decision_path: Path) -> Path:
    metadata = _mapping(decision_payload.get("metadata"))
    raw = str(metadata.get("source_review_package") or "")
    if raw:
        return resolve_project_path(raw)
    return decision_path.parent / "portfolio_candidate_review_package.json"


def _safe_review_date(payload: dict[str, Any], path: Path) -> date | None:
    try:
        return portfolio_candidate_review_payload_date(payload, path)
    except ValueError:
        return _parent_date(path)


def _candidate_artifact_path(decision_payload: dict[str, Any]) -> Path:
    candidate = _mapping(decision_payload.get("candidate"))
    raw = str(candidate.get("source_artifact") or "")
    return resolve_project_path(raw) if raw else Path("")


def _portfolio_candidates_path(
    config: dict[str, Any],
    candidate_artifact_path: Path,
    review_date: date | None,
) -> Path:
    if str(candidate_artifact_path) and candidate_artifact_path.is_file():
        path = candidate_artifact_path.parent / "portfolio_candidates_summary.json"
        if path.exists():
            return path
    root = _input_root(config, "portfolio_candidates_dir")
    if review_date is not None:
        latest = latest_portfolio_candidates_path_on_or_before(review_date, root)
        if latest is not None:
            return latest
    return root / "missing" / "portfolio_candidates_summary.json"


def _supporting_artifacts(
    *,
    config: dict[str, Any],
    tracking_date: date,
    selected_review_path: Path,
    package_path: Path,
    candidate_artifact_path: Path,
    candidates_path: Path,
    manifest_path: Path | None,
    market_data_freshness_path: Path | None,
    market_data_refresh_path: Path | None,
) -> dict[str, Any]:
    artifacts = {
        "portfolio_candidate_review_decision": str(selected_review_path),
        "portfolio_candidate_review_package": str(package_path),
        "recommended_portfolio_candidate": str(candidate_artifact_path),
        "portfolio_candidates": str(candidates_path),
    }
    optional = {
        "portfolio_sensitivity": _latest_named_artifact(
            _input_root(config, "portfolio_sensitivity_dir"),
            "portfolio_sensitivity_summary.json",
            tracking_date,
        ),
        "signal_snapshot": _latest_named_artifact(
            _input_root(config, "signal_snapshot_dir"),
            "signal_snapshot.json",
            tracking_date,
        ),
        "backtest_input_manifest": manifest_path,
        "market_data_freshness": market_data_freshness_path,
        "market_data_refresh": market_data_refresh_path,
        "price_cache_registry": _path_or_none(_input(config, "price_cache_registry_path")),
        "shadow_backtest": _latest_shadow_backtest_path(config, tracking_date),
    }
    missing: list[str] = []
    for key, path in list(artifacts.items()):
        artifact_path = Path(path)
        if not path or not artifact_path.is_file():
            missing.append(key)
    for key, path in optional.items():
        if path is not None and path.exists():
            artifacts[key] = str(path)
        else:
            missing.append(key)
    return {"artifacts": artifacts, "missing": sorted(set(missing))}


def _market_data_freshness_context(config: dict[str, Any], tracking_date: date) -> dict[str, Any]:
    root = _market_data_freshness_root(config)
    path = latest_market_data_freshness_path_on_or_before(tracking_date, root)
    if path is None:
        return {
            "status": "MISSING",
            "report": "",
            "can_track": None,
            "tracking_readiness": "not_evaluated",
            "tracking_status_recommendation": "",
            "reason": (
                "Market data freshness report is missing; tracking falls back to existing "
                "data gate and date roll-forward behavior."
            ),
        }
    payload = load_market_data_freshness_payload(path)
    freshness = _mapping(payload.get("freshness"))
    readiness = _mapping(payload.get("tracking_readiness"))
    metadata = _mapping(payload.get("metadata"))
    status = str(freshness.get("status") or metadata.get("status") or "UNKNOWN")
    return {
        "status": status,
        "report": str(path),
        "can_track": readiness.get("can_track") is True,
        "tracking_readiness": str(readiness.get("readiness") or "unknown"),
        "tracking_status_recommendation": str(
            readiness.get("tracking_status_recommendation") or ""
        ),
        "reason": str(freshness.get("reason") or readiness.get("reason") or ""),
    }


def _market_data_refresh_context(config: dict[str, Any], tracking_date: date) -> dict[str, Any]:
    root = _market_data_refresh_root(config)
    path = _latest_market_data_refresh_path_on_or_before(tracking_date, root)
    if path is None:
        return {
            "status": "MISSING",
            "report": "",
            "before_freshness_status": "",
            "after_freshness_status": "",
            "candidate_tracking_status": "",
            "suggested_action": "",
        }
    payload = _load_json(path)
    metadata = _mapping(payload.get("metadata"))
    before = _mapping(payload.get("before"))
    after = _mapping(payload.get("after"))
    status = str(metadata.get("status") or "UNKNOWN")
    suggested_action = ""
    if status == "SOURCE_DELAYED":
        suggested_action = "check data source availability or rerun refresh after source update"
    elif status in {"FAILED", "BLOCKED"}:
        suggested_action = "review market data refresh report before rerunning tracking"
    return {
        "status": status,
        "report": str(path),
        "before_freshness_status": str(before.get("freshness_status") or ""),
        "after_freshness_status": str(after.get("freshness_status") or ""),
        "candidate_tracking_status": str(after.get("candidate_tracking_status") or ""),
        "suggested_action": suggested_action,
    }


def _latest_market_data_refresh_path_on_or_before(
    as_of: date,
    root: Path,
) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_refresh_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _tracking_block_reasons(
    *,
    review_status: str,
    decision_payload: dict[str, Any],
    package_payload: dict[str, Any],
    candidate_artifact_path: Path,
    candidate_hash: str,
    candidates_payload: dict[str, Any],
    data_gate: dict[str, Any],
    market_data_freshness: dict[str, Any],
    previous_record: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    if review_status not in ELIGIBLE_REVIEW_STATUSES:
        reasons.append(f"review_status_not_eligible:{review_status or 'missing'}")
    if data_gate.get("status") != "OK":
        reasons.append("data_gate_failed")
    if market_data_freshness.get("report") and market_data_freshness.get("can_track") is False:
        reasons.append(
            "market_data_freshness_not_ready:"
            + str(market_data_freshness.get("status") or "UNKNOWN")
        )
    if (
        not str(candidate_artifact_path)
        or not candidate_artifact_path.exists()
        or candidate_artifact_path.is_dir()
    ):
        reasons.append("candidate_artifact_missing")
    elif candidate_hash:
        actual_hash = _sha256_if_exists(candidate_artifact_path)
        if actual_hash and actual_hash != candidate_hash:
            reasons.append("candidate_artifact_hash_changed")
    if not candidates_payload or candidates_payload.get("report_type") != "portfolio_candidates":
        reasons.append("portfolio_candidates_summary_missing")
    if _production_hash_changed(decision_payload, package_payload):
        reasons.append("production_config_hash_changed")
    if previous_record and str(previous_record.get("status") or "") == "retired":
        reasons.append("candidate_retired")
    return list(dict.fromkeys(reasons))


def _tracking_status(
    *,
    review_status: str,
    block_reasons: list[str],
    tracking_date: date,
    effective_data_date: date | None,
    market_data_freshness: dict[str, Any],
) -> str:
    if review_status not in ELIGIBLE_REVIEW_STATUSES:
        return "not_started"
    if block_reasons:
        return "tracking_blocked"
    recommendation = str(market_data_freshness.get("tracking_status_recommendation") or "")
    if recommendation in {"active_tracking", "degraded_tracking"}:
        return recommendation
    if effective_data_date is not None and tracking_date > effective_data_date:
        return "degraded_tracking"
    return "active_tracking"


def _roll_forward_status(
    *,
    block_reasons: list[str],
    tracking_date: date,
    review_date: date | None,
    effective_data_date: date | None,
) -> str:
    if block_reasons:
        return "BLOCKED"
    if review_date is not None and review_date < tracking_date:
        return "ROLLED_FORWARD"
    if effective_data_date is not None and effective_data_date < tracking_date:
        return "ROLLED_FORWARD"
    return "OK"


def _metadata_status(tracking_status: str) -> str:
    if tracking_status == "tracking_blocked":
        return "BLOCKED"
    if tracking_status == "degraded_tracking":
        return "DEGRADED"
    if tracking_status == "not_started":
        return "NOT_STARTED"
    return "OK"


def _date_resolution_reason(
    *,
    tracking_date: date,
    review_date: date | None,
    effective_data_date: date | None,
    roll_forward_status: str,
    block_reasons: list[str],
) -> str:
    if block_reasons:
        return "Tracking blocked: " + ", ".join(block_reasons) + "."
    parts: list[str] = []
    if review_date is not None and review_date < tracking_date:
        parts.append(
            "Latest eligible review decision was rolled forward from " f"{review_date.isoformat()}."
        )
    if effective_data_date is not None and effective_data_date < tracking_date:
        parts.append(
            "Effective market data date remains "
            f"{effective_data_date.isoformat()} while tracking date is "
            f"{tracking_date.isoformat()}."
        )
    if not parts:
        parts.append(f"Tracking uses current artifacts for {tracking_date.isoformat()}.")
    if roll_forward_status == "ROLLED_FORWARD":
        return " ".join(parts)
    return parts[0]


def _tracking_metrics(
    *,
    baseline_profile: dict[str, Any],
    candidate_profile: dict[str, Any],
    previous_record: dict[str, Any] | None,
    tracking_status: str,
) -> dict[str, Any]:
    baseline_raw = _raw_metrics(baseline_profile)
    candidate_raw = _raw_metrics(candidate_profile)
    start_metrics = _mapping((previous_record or {}).get("tracking_start_metrics"))
    last_metrics = _mapping((previous_record or {}).get("last_observed_metrics"))
    baseline_start = _mapping(start_metrics.get("baseline")) or baseline_raw
    candidate_start = _mapping(start_metrics.get("candidate")) or candidate_raw
    baseline_last = _mapping(last_metrics.get("baseline")) or baseline_raw
    candidate_last = _mapping(last_metrics.get("candidate")) or candidate_raw
    baseline_tracking = _derived_tracking_metrics(
        current=baseline_raw,
        start=baseline_start,
        last=baseline_last,
    )
    candidate_tracking = _derived_tracking_metrics(
        current=candidate_raw,
        start=candidate_start,
        last=candidate_last,
    )
    candidate_tracking["excess_return_vs_baseline"] = _round_float(
        _float_value(candidate_tracking.get("cumulative_return_since_tracking_start"))
        - _float_value(baseline_tracking.get("cumulative_return_since_tracking_start"))
    )
    return {
        "status": tracking_status,
        "baseline": baseline_tracking,
        "candidate": candidate_tracking,
        "excess_vs_baseline": {
            "cumulative_return_since_tracking_start": candidate_tracking[
                "excess_return_vs_baseline"
            ],
            "daily_return": _round_float(
                _float_value(candidate_tracking.get("daily_return"))
                - _float_value(baseline_tracking.get("daily_return"))
            ),
        },
        "raw_observed_metrics": {
            "baseline": baseline_raw,
            "candidate": candidate_raw,
        },
        "metric_source": (
            "state_delta_from_portfolio_candidates_summary"
            if previous_record
            else "initial_observation_from_portfolio_candidates_summary"
        ),
        "daily_tracking": {
            "baseline": baseline_tracking,
            "candidate": candidate_tracking,
        },
    }


def _raw_metrics(profile: dict[str, Any]) -> dict[str, Any]:
    performance = _mapping(profile.get("performance"))
    transmission = _mapping(profile.get("signal_transmission"))
    risk = _mapping(profile.get("risk_guardrails"))
    return {
        "cumulative_return": _round_float(performance.get("cumulative_return")),
        "drawdown": _round_float(performance.get("max_drawdown")),
        "max_drawdown": _round_float(performance.get("max_drawdown")),
        "turnover": _round_float(performance.get("turnover")),
        "rebalance_count": _int_value(transmission.get("rebalance_days")),
        "signal_transmission_score": _round_float(
            transmission.get("target_to_actual_weight_effectiveness")
        ),
        "risk_guardrail_status": risk.get("guardrail_status", "UNKNOWN"),
    }


def _derived_tracking_metrics(
    *,
    current: dict[str, Any],
    start: dict[str, Any],
    last: dict[str, Any],
) -> dict[str, Any]:
    return {
        "daily_return": _relative_return(
            current.get("cumulative_return"),
            last.get("cumulative_return"),
        ),
        "cumulative_return_since_tracking_start": _relative_return(
            current.get("cumulative_return"),
            start.get("cumulative_return"),
        ),
        "drawdown": _round_float(current.get("drawdown")),
        "max_drawdown_since_tracking_start": _round_float(current.get("max_drawdown")),
        "turnover": _round_float(current.get("turnover")),
        "rebalance_count": _int_value(current.get("rebalance_count")),
        "signal_transmission_score": _round_float(current.get("signal_transmission_score")),
        "risk_guardrail_status": current.get("risk_guardrail_status", "UNKNOWN"),
    }


def _relative_return(current: object, previous: object) -> float:
    current_value = _float_value(current)
    previous_value = _float_value(previous)
    denominator = 1.0 + previous_value
    if denominator == 0.0:
        return 0.0
    return _round_float((1.0 + current_value) / denominator - 1.0)


def _profile_payload(payload: dict[str, Any], profile_name: str) -> dict[str, Any]:
    for profile in _records(payload.get("profiles")):
        if str(profile.get("profile_name") or "") == profile_name:
            return profile
    for profile in _records(payload.get("candidates")):
        if str(profile.get("profile_name") or "") == profile_name:
            return profile
    return {}


def _risk_status(candidate_profile: dict[str, Any]) -> str:
    return str(
        _mapping(candidate_profile.get("risk_guardrails")).get("guardrail_status") or "UNKNOWN"
    )


def _risk_warnings(candidate_profile: dict[str, Any]) -> list[str]:
    risk = _mapping(candidate_profile.get("risk_guardrails"))
    warnings = [str(risk.get("warning") or "")]
    warnings.extend(str(item) for item in _records(candidate_profile.get("warnings")))
    return [item for item in dict.fromkeys(warnings) if item]


def _promotion_reason(tracking_status: str) -> str:
    if tracking_status == "degraded_tracking":
        return (
            "Portfolio candidate tracking is degraded due to latest data roll-forward. "
            "Production promotion remains disabled."
        )
    if tracking_status == "active_tracking":
        return (
            "Portfolio candidate is being tracked in shadow mode. Production promotion "
            "remains disabled because signal quality is LIMITED."
        )
    if tracking_status == "tracking_blocked":
        return (
            "Portfolio candidate tracking is blocked. Shadow tracking is advisory only "
            "and cannot support production promotion."
        )
    return "Shadow tracking has not started; production promotion remains disabled."


def _candidate_id(profile_name: str, review_date: date | None, candidate_hash: str) -> str:
    date_text = "unknown-date" if review_date is None else review_date.isoformat()
    slug = profile_name.replace("_", "-") or "unknown-profile"
    suffix = candidate_hash[:12] if candidate_hash else "nohash"
    return f"portfolio-{slug}-{date_text}-{suffix}"


def _load_tracking_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _state_record(state: dict[str, Any], candidate_id: str) -> dict[str, Any] | None:
    for record in _records(state.get("active_candidates")):
        if str(record.get("candidate_id") or "") == candidate_id:
            return record
    return None


def _started_at(
    active: list[dict[str, Any]],
    candidate_id: str,
    date_resolution: dict[str, Any],
) -> str:
    for item in active:
        if str(item.get("candidate_id") or "") == candidate_id:
            return str(item.get("started_at") or date_resolution.get("review_decision_date") or "")
    return str(
        date_resolution.get("review_decision_date") or date_resolution.get("tracking_date") or ""
    )


def _tracking_start_metrics(
    active: list[dict[str, Any]],
    candidate_id: str,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    for item in active:
        if str(item.get("candidate_id") or "") == candidate_id:
            start = item.get("tracking_start_metrics")
            if isinstance(start, dict):
                return start
    return _mapping(metrics.get("raw_observed_metrics"))


def _production_hash_changed(
    decision_payload: dict[str, Any],
    package_payload: dict[str, Any],
) -> bool:
    rollback = _mapping(decision_payload.get("rollback_reference"))
    package_production = _mapping(package_payload.get("current_production"))
    production_path = Path(
        str(rollback.get("production_path") or package_production.get("path") or "")
    )
    expected_hash = str(
        rollback.get("production_sha256_before_review") or package_production.get("sha256") or ""
    )
    current_hash = _sha256_if_exists(production_path)
    return bool(expected_hash and current_hash and expected_hash != current_hash)


def _current_production_hash(
    decision_payload: dict[str, Any],
    package_payload: dict[str, Any],
) -> str:
    rollback = _mapping(decision_payload.get("rollback_reference"))
    package_production = _mapping(package_payload.get("current_production"))
    production_path = Path(
        str(rollback.get("production_path") or package_production.get("path") or "")
    )
    return _sha256_if_exists(production_path)


def _review_status(payload: dict[str, Any]) -> str:
    decision = _mapping(payload.get("decision"))
    metadata = _mapping(payload.get("metadata"))
    return str(decision.get("status") or metadata.get("status") or "")


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    key = "dry_run_dir" if dry_run else "portfolio_candidate_tracking_dir"
    default = (
        PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_candidate_tracking"
        if dry_run
        else default_portfolio_candidate_tracking_root()
    )
    raw = _mapping(config.get("output")).get(key)
    return resolve_project_path(str(raw)) if raw else default


def _input_root(config: dict[str, Any], key: str) -> Path:
    raw = _input(config, key)
    return resolve_project_path(str(raw)) if raw else PROJECT_ROOT / "artifacts"


def _market_data_freshness_root(config: dict[str, Any]) -> Path:
    raw = _input(config, "market_data_freshness_dir")
    return resolve_project_path(str(raw)) if raw else default_market_data_freshness_root()


def _market_data_refresh_root(config: dict[str, Any]) -> Path:
    raw = _input(config, "market_data_refresh_dir")
    return resolve_project_path(str(raw)) if raw else PROJECT_ROOT / "artifacts" / "data_refresh"


def _input(config: dict[str, Any], key: str) -> object:
    return _mapping(config.get("input")).get(key)


def _latest_named_artifact(root: Path, file_name: str, as_of: date) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"*/{file_name}"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_shadow_backtest_path(config: dict[str, Any], as_of: date) -> Path | None:
    roots = [
        _input_root(config, "shadow_backtest_dir"),
        _input_root(config, "shadow_backtest_dry_run_dir"),
    ]
    candidates: list[tuple[date, Path]] = []
    for root in roots:
        for path in root.glob("*/shadow_backtest_summary.json"):
            try:
                candidate_date = date.fromisoformat(path.parent.name)
            except ValueError:
                continue
            if candidate_date <= as_of:
                candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _path_or_none(value: object) -> Path | None:
    raw = str(value or "")
    if not raw:
        return None
    return resolve_project_path(raw)


def _parent_date(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.parent.name)
    except (AttributeError, ValueError):
        return None


def _sha256_if_exists(path: Path) -> str:
    try:
        if not path.exists() or path.is_dir():
            return ""
        return sha256_file(path)
    except OSError:
        return ""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _float_value(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _round_float(value: object) -> float:
    return round(_float_value(value), 6)


def _validate_tracking_config(config: dict[str, Any]) -> None:
    if config.get("production_effect") != "none":
        raise ValueError("portfolio candidate tracking production_effect must be none")
    if config.get("manual_review_required") is not True:
        raise ValueError("portfolio candidate tracking manual_review_required must be true")
    if config.get("auto_promotion") is not False:
        raise ValueError("portfolio candidate tracking auto_promotion must be false")
    eligible = set(str(item) for item in _records(config.get("eligible_review_status")))
    if not ELIGIBLE_REVIEW_STATUSES.issubset(eligible):
        raise ValueError("portfolio candidate tracking config missing eligible statuses")
    safety = _mapping(config.get("safety"))
    if safety.get("production_write_allowed") is not False:
        raise ValueError("portfolio candidate tracking production writes must be disabled")
    if safety.get("candidate_promotion_enabled") is not False:
        raise ValueError("portfolio candidate tracking candidate promotion must be disabled")
