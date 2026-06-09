from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    load_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    latest_market_data_freshness_path,
    load_market_data_freshness_payload,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    latest_market_data_refresh_path,
    load_market_data_refresh_payload,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import resolve_project_path
from ai_trading_system.trading_engine.signal_snapshots import (
    latest_signal_snapshot_path,
    load_signal_snapshot_payload,
    signal_snapshot_summary,
)

WEIGHT_STABILITY_READINESS_SCHEMA_VERSION = 1
WEIGHT_STABILITY_READINESS_REPORT_TYPE = "weight_stability_readiness"
WEIGHT_STABILITY_READINESS_ALIAS_REPORT_TYPE = "weight_stability_readiness_report"
WEIGHT_STABILITY_READINESS_STATUSES = {
    "READY",
    "LIMITED_READY",
    "BLOCKED",
    "RECOVERY_AVAILABLE",
    "RECOVERY_FAILED",
    "INSUFFICIENT_DATA",
    "FAILED",
}
DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "weight_tuning_v0_2_stability.yaml"
)


@dataclass(frozen=True)
class WeightStabilityReadinessRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


def default_weight_stability_readiness_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "weight_stability_readiness"


def default_weight_stability_readiness_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_weight_stability_readiness_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_weight_stability_readiness_dir(output_root, as_of)
        / "weight_stability_readiness_summary.json"
    )


def default_weight_stability_readiness_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_weight_stability_readiness_dir(output_root, as_of)
        / "weight_stability_readiness_summary.md"
    )


def latest_weight_stability_readiness_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_weight_stability_readiness_root()
    candidates = sorted(root.glob("*/weight_stability_readiness_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_weight_stability_readiness_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_weight_stability_readiness_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_stability_readiness_summary.json"):
        parsed = _parse_date(path.parent.name)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def weight_stability_readiness_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"weight_stability_readiness_{as_of.isoformat()}.json",
        reports_dir / f"weight_stability_readiness_{as_of.isoformat()}.md",
    )


def run_weight_stability_readiness(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
    dry_run: bool = False,
    recovery_mode: bool = False,
    generated_at: datetime | None = None,
) -> WeightStabilityReadinessRun:
    root = (
        PROJECT_ROOT / "outputs" / "dry_runs" / "weight_stability_readiness"
        if dry_run
        else default_weight_stability_readiness_root()
    )
    payload = build_weight_stability_readiness_payload(
        as_of=as_of,
        config_path=config_path,
        output_root=root,
        dry_run=dry_run,
        recovery_mode=recovery_mode,
        generated_at=generated_at,
    )
    resolved_as_of = weight_stability_readiness_payload_date(
        payload,
        default_weight_stability_readiness_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_weight_stability_readiness_json_path(root, resolved_as_of)
    markdown_path = default_weight_stability_readiness_markdown_path(root, resolved_as_of)
    payload["output_artifacts"] = {
        **_mapping(payload.get("output_artifacts")),
        "weight_stability_readiness_summary_json": str(json_path),
        "weight_stability_readiness_summary_md": str(markdown_path),
    }
    write_weight_stability_readiness_summary(payload, json_path, markdown_path)
    return WeightStabilityReadinessRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_weight_stability_readiness_payload(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
    output_root: Path | None = None,
    dry_run: bool = False,
    recovery_mode: bool = False,
    generated_at: datetime | None = None,
    project_root: Path | None = None,
    freshness_path: Path | None = None,
    refresh_path: Path | None = None,
    signal_snapshot_path: Path | None = None,
    backtest_diagnostics_path: Path | None = None,
    stable_tuning_path: Path | None = None,
) -> dict[str, Any]:
    root = project_root or PROJECT_ROOT
    generated = generated_at or datetime.now(tz=UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        generated = generated.replace(tzinfo=UTC)
    resolved_config = _resolve_config_path(config_path, root)
    resolved_as_of = as_of or _latest_input_date(
        root,
        freshness_path=freshness_path,
        refresh_path=refresh_path,
        signal_snapshot_path=signal_snapshot_path,
        backtest_diagnostics_path=backtest_diagnostics_path,
        stable_tuning_path=stable_tuning_path,
        default_date=generated.date(),
    )
    freshness_source = freshness_path or _input_path(
        root,
        "data_freshness",
        resolved_as_of,
        "market_data_freshness_summary.json",
        latest_when_missing=as_of is None,
        latest_func=latest_market_data_freshness_path,
    )
    refresh_source = refresh_path or _input_path(
        root,
        "data_refresh",
        resolved_as_of,
        "market_data_refresh_summary.json",
        latest_when_missing=as_of is None,
        latest_func=latest_market_data_refresh_path,
    )
    snapshot_source = signal_snapshot_path or _input_path(
        root,
        "signal_snapshots",
        resolved_as_of,
        "signal_snapshot.json",
        latest_when_missing=as_of is None,
        latest_func=latest_signal_snapshot_path,
    )
    diagnostics_source = backtest_diagnostics_path or _input_path(
        root,
        "data_quality",
        resolved_as_of,
        "backtest_input_diagnostics.json",
        latest_when_missing=as_of is None,
        latest_func=None,
    )
    stable_source = stable_tuning_path or _input_path(
        root,
        "weight_stability",
        resolved_as_of,
        "weight_stability_summary.json",
        latest_when_missing=as_of is None,
        latest_func=None,
    )
    if as_of is None and not diagnostics_source.exists():
        diagnostics_source = (
            _latest_path(
                root / "artifacts" / "data_quality",
                "backtest_input_diagnostics.json",
            )
            or diagnostics_source
        )
    if as_of is None and not stable_source.exists():
        stable_source = (
            _latest_path(
                root / "artifacts" / "weight_stability",
                "weight_stability_summary.json",
            )
            or stable_source
        )

    freshness_payload = (
        load_market_data_freshness_payload(freshness_source) if freshness_source.exists() else {}
    )
    refresh_payload = (
        load_market_data_refresh_payload(refresh_source) if refresh_source.exists() else {}
    )
    snapshot_payload = (
        load_signal_snapshot_payload(snapshot_source) if snapshot_source.exists() else {}
    )
    diagnostics_payload = (
        load_backtest_input_diagnostics(diagnostics_source) if diagnostics_source.exists() else {}
    )
    stable_payload = _load_json(stable_source) if stable_source.exists() else {}

    freshness = _freshness_readiness(freshness_payload, freshness_source)
    recover = _recover_freshness_readiness(refresh_payload, refresh_source)
    manifest = _backtest_manifest_readiness(
        diagnostics_payload,
        diagnostics_source,
        root=root,
        as_of=resolved_as_of,
    )
    price_coverage = _price_coverage_readiness(
        diagnostics_payload,
        recover_payload=refresh_payload,
    )
    signal_snapshot = _signal_snapshot_readiness(
        snapshot_payload,
        snapshot_source,
        target_date=resolved_as_of,
        effective_data_date=_parse_date(str(freshness.get("effective_data_date") or "")),
        latest_manifest_date=_parse_date(str(freshness.get("latest_manifest_date") or "")),
    )
    checks = {
        "freshness": freshness,
        "recover_freshness": recover,
        "signal_snapshot": signal_snapshot,
        "backtest_manifest": manifest,
        "price_coverage": price_coverage,
    }
    eligibility = _stable_tuning_eligibility(checks)
    output_root_resolved = output_root or (
        root / "outputs" / "dry_runs" / "weight_stability_readiness"
        if dry_run
        else root / "artifacts" / "weight_stability_readiness"
    )
    output_artifacts = {
        "weight_stability_readiness_summary_json": str(
            default_weight_stability_readiness_json_path(output_root_resolved, resolved_as_of)
        ),
        "weight_stability_readiness_summary_md": str(
            default_weight_stability_readiness_markdown_path(output_root_resolved, resolved_as_of)
        ),
    }
    previous_context = _previous_stable_tuning_context(stable_payload, stable_source)
    return {
        "schema_version": WEIGHT_STABILITY_READINESS_SCHEMA_VERSION,
        "report_type": WEIGHT_STABILITY_READINESS_REPORT_TYPE,
        "as_of": resolved_as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-stability-readiness-{resolved_as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": eligibility["status"],
            "reason": eligibility["reason"],
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "recovery_mode": recovery_mode,
            "source_task": "TRADING-061A",
            "config_path": str(resolved_config),
            "market_regime": "ai_after_chatgpt",
            "market_regime_anchor": "2022-11-30",
        },
        "input_context": {
            "stable_tuning_config": str(resolved_config),
            **previous_context,
        },
        "readiness_checks": checks,
        "stable_tuning_eligibility": eligibility,
        "blocking_errors": _blocking_errors(checks),
        "recovery_plan": _recovery_plan(checks, eligibility),
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Stable weight tuning has not entered a valid backtest because input "
                "readiness is blocked."
                if not eligibility["can_run"]
                else "Stable tuning input readiness is restored, but promotion remains disabled."
            ),
        },
        "supporting_artifacts": {
            "market_data_freshness": str(freshness_source) if freshness_source.exists() else "",
            "market_data_refresh": str(refresh_source) if refresh_source.exists() else "",
            "signal_snapshot": str(snapshot_source) if snapshot_source.exists() else "",
            "backtest_input_diagnostics": (
                str(diagnostics_source) if diagnostics_source.exists() else ""
            ),
            "backtest_input_manifest": str(
                root
                / "artifacts"
                / "backtest_snapshots"
                / resolved_as_of.isoformat()
                / "backtest_input_manifest.json"
            ),
            "previous_weight_stability": str(stable_source) if stable_source.exists() else "",
        },
        "output_artifacts": output_artifacts,
        "reader_brief": _reader_brief_sentence(eligibility, checks),
        "safety": _safety_payload(),
    }


def write_weight_stability_readiness_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_weight_stability_readiness_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def write_weight_stability_readiness_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": WEIGHT_STABILITY_READINESS_ALIAS_REPORT_TYPE,
        "source_report_type": WEIGHT_STABILITY_READINESS_REPORT_TYPE,
    }
    json_path, markdown_path = weight_stability_readiness_report_alias_paths(
        reports_dir,
        as_of,
    )
    return write_weight_stability_readiness_summary(alias_payload, json_path, markdown_path)


def load_weight_stability_readiness_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_weight_stability_readiness_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != WEIGHT_STABILITY_READINESS_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        WEIGHT_STABILITY_READINESS_REPORT_TYPE,
        WEIGHT_STABILITY_READINESS_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in ("run_id", "generated_at", "status", "production_effect"):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("status") not in WEIGHT_STABILITY_READINESS_STATUSES:
        issues.append("metadata status is invalid")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    eligibility = _mapping(payload.get("stable_tuning_eligibility"))
    if not isinstance(eligibility.get("blocking_checks"), list):
        issues.append("stable_tuning_eligibility.blocking_checks must be a list")
    if (
        eligibility.get("can_run") is True
        and eligibility.get("candidates_backtest_allowed") is not True
    ):
        issues.append("candidates_backtest_allowed must be true when can_run is true")
    if (
        eligibility.get("can_run") is False
        and eligibility.get("candidates_backtest_allowed") is not False
    ):
        issues.append("candidates_backtest_allowed must be false when can_run is false")
    checks = _mapping(payload.get("readiness_checks"))
    for key in (
        "freshness",
        "recover_freshness",
        "signal_snapshot",
        "backtest_manifest",
        "price_coverage",
    ):
        if not isinstance(checks.get(key), dict):
            issues.append(f"readiness_checks.{key} must be an object")
    safety = _mapping(payload.get("safety"))
    for key in (
        "production_write_allowed",
        "production_config_modified",
        "data_quality_gate_lowered",
        "mock_data_used",
        "synthetic_price_history_generated",
        "fallback_signals_relaxed",
        "candidate_promotion_triggered",
        "trading_action",
    ):
        if safety.get(key) is not False:
            issues.append(f"{key} must be false")
    if safety.get("production_effect") != "none":
        issues.append("safety production_effect must be none")
    if safety.get("manual_review_required") is not True:
        issues.append("safety manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        issues.append("safety auto_promotion must be false")
    return issues


def weight_stability_readiness_payload_date(
    payload: Mapping[str, Any],
    source_path: Path,
) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    if run_id.startswith("weight-stability-readiness-"):
        parsed = _parse_date(run_id.removeprefix("weight-stability-readiness-"))
        if parsed is not None:
            return parsed
    parsed = _parse_date(str(payload.get("as_of") or ""))
    if parsed is not None:
        return parsed
    parsed = _parse_date(source_path.parent.name)
    if parsed is not None:
        return parsed
    raise ValueError(f"cannot infer weight stability readiness date from {source_path}")


def render_weight_stability_readiness_explanation(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    checks = _mapping(payload.get("readiness_checks"))
    eligibility = _mapping(payload.get("stable_tuning_eligibility"))
    freshness = _mapping(checks.get("freshness"))
    signal = _mapping(checks.get("signal_snapshot"))
    manifest = _mapping(checks.get("backtest_manifest"))
    price = _mapping(checks.get("price_coverage"))
    context = _mapping(payload.get("input_context"))
    blocking = ", ".join(_strings(eligibility.get("blocking_checks"))) or "none"
    return "\n".join(
        [
            f"status={metadata.get('status', 'UNKNOWN')}",
            f"can_run={str(eligibility.get('can_run', False)).lower()}",
            f"reason={eligibility.get('reason', '')}",
            f"blocking_checks={blocking}",
            f"freshness_status={freshness.get('status', 'UNKNOWN')}",
            f"signal_snapshot_status={signal.get('status', 'UNKNOWN')}",
            f"backtest_manifest_status={manifest.get('status', 'UNKNOWN')}",
            f"price_coverage_status={price.get('status', 'UNKNOWN')}",
            "previous_stable_tuning_status="
            f"{context.get('previous_stable_tuning_status', 'UNKNOWN')}",
            f"previous_candidates_backtested={context.get('previous_candidates_backtested', 0)}",
            "production_effect=none",
            "manual_review_required=true",
            "auto_promotion=false",
        ]
    )


def render_weight_stability_readiness_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    context = _mapping(payload.get("input_context"))
    checks = _mapping(payload.get("readiness_checks"))
    eligibility = _mapping(payload.get("stable_tuning_eligibility"))
    freshness = _mapping(checks.get("freshness"))
    recover = _mapping(checks.get("recover_freshness"))
    signal = _mapping(checks.get("signal_snapshot"))
    manifest = _mapping(checks.get("backtest_manifest"))
    price = _mapping(checks.get("price_coverage"))
    lines = [
        "# Stable Weight Tuning Input Readiness Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- can_run_stable_tuning: `{eligibility.get('can_run', False)}`",
        f"- candidates_backtest_allowed: `{eligibility.get('candidates_backtest_allowed', False)}`",
        f"- reason: {eligibility.get('reason', '')}",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Previous Stable Tuning Result",
        "",
        "- previous_stable_tuning_status: "
        f"`{context.get('previous_stable_tuning_status', 'UNKNOWN')}`",
        f"- previous_candidates_backtested: `{context.get('previous_candidates_backtested', 0)}`",
        f"- previous_reason: {context.get('previous_reason', '')}",
        "",
        "## 3. Freshness Readiness",
        "",
        _definition_lines(
            freshness,
            (
                "status",
                "tracking_date",
                "effective_data_date",
                "latest_manifest_date",
                "tracking_readiness",
                "can_continue",
                "reason",
            ),
        ),
        "",
        "## 4. Recover Freshness Result",
        "",
        _definition_lines(
            recover,
            ("status", "after_freshness_status", "can_continue", "reason"),
        ),
        "",
        "## 5. Signal Snapshot Readiness",
        "",
        _definition_lines(
            signal,
            (
                "status",
                "snapshot_date",
                "real_signals",
                "proxy_signals",
                "fallback_signals",
                "missing_signals",
                "can_continue",
                "reason",
                "warning",
            ),
        ),
        "",
        "## 6. Backtest Manifest Readiness",
        "",
        _definition_lines(
            manifest,
            ("status", "can_continue", "reason", "source_artifact", "manifest_artifact"),
        ),
        "",
        "## 7. Price Coverage Readiness",
        "",
        _definition_lines(
            price,
            (
                "status",
                "can_continue",
                "reason",
                "missing_symbols",
                "high_missing_ratio_symbols",
                "special_findings",
            ),
        ),
        "",
        "## 8. Stable Tuning Eligibility",
        "",
        _definition_lines(
            eligibility,
            (
                "status",
                "can_run",
                "candidates_backtest_allowed",
                "blocking_checks",
                "reason",
            ),
        ),
        "",
        "## 9. Blocking Errors",
        "",
    ]
    blocking = _records(payload.get("blocking_errors"))
    if not blocking:
        lines.append("- 无阻塞项。")
    for item in blocking:
        lines.append(
            f"- `{item.get('check', 'UNKNOWN')}`: `{item.get('status', 'UNKNOWN')}` - "
            f"{item.get('reason', '')}"
        )
    lines.extend(["", "## 10. Recovery Plan", ""])
    for step in _records(payload.get("recovery_plan")):
        lines.append(
            f"{step.get('step', '')}. `{step.get('action', '')}` - "
            f"`{step.get('command', '')}`；{step.get('reason', '')}"
        )
    if not _records(payload.get("recovery_plan")):
        lines.append("- 无需恢复。")
    lines.extend(
        [
            "",
            "## 11. Impact on TRADING-061",
            "",
            f"- {payload.get('reader_brief', '')}",
            "",
            "## 12. Input / Output Artifacts",
            "",
        ]
    )
    for key, value in _mapping(payload.get("supporting_artifacts")).items():
        lines.append(f"- `{key}`: `{value}`")
    for key, value in _mapping(payload.get("output_artifacts")).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 13. Safety", ""])
    for key, value in _mapping(payload.get("safety")).items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _freshness_readiness(payload: Mapping[str, Any], source_path: Path) -> dict[str, Any]:
    if not payload:
        return {
            "status": "MISSING",
            "tracking_date": "",
            "effective_data_date": "",
            "latest_manifest_date": "",
            "tracking_readiness": "unknown",
            "can_continue": False,
            "reason": "freshness_summary_missing",
            "source_artifact": "",
        }
    metadata = _mapping(payload.get("metadata"))
    freshness = _mapping(payload.get("freshness"))
    data_dates = _mapping(payload.get("data_dates"))
    tracking = _mapping(payload.get("tracking_readiness"))
    status = str(freshness.get("status") or metadata.get("status") or "UNKNOWN")
    can_continue = status == "OK"
    reason = ""
    if status == "ACCEPTABLE_LAG":
        reason = "stable_tuning_requires_full_historical_readiness"
    elif status == "MISSING":
        reason = "freshness_status_missing"
    elif status == "STALE":
        reason = "freshness_status_stale"
    elif status == "FAILED":
        reason = "freshness_status_failed"
    elif not can_continue:
        reason = "freshness_status_not_ok"
    return {
        "status": status,
        "tracking_date": str(data_dates.get("tracking_date") or ""),
        "expected_data_date": str(data_dates.get("expected_data_date") or ""),
        "effective_data_date": str(data_dates.get("effective_data_date") or ""),
        "latest_manifest_date": str(data_dates.get("latest_manifest_date") or ""),
        "tracking_readiness": str(
            tracking.get("readiness") or tracking.get("tracking_status_recommendation") or "unknown"
        ),
        "can_continue": can_continue,
        "reason": reason,
        "source_artifact": str(source_path),
    }


def _recover_freshness_readiness(payload: Mapping[str, Any], source_path: Path) -> dict[str, Any]:
    if not payload:
        return {
            "status": "MISSING",
            "after_freshness_status": "",
            "remaining_limitations": [],
            "can_continue": True,
            "reason": "recover_freshness_not_run_or_not_required",
            "source_artifact": "",
        }
    metadata = _mapping(payload.get("metadata"))
    after = _mapping(payload.get("after"))
    after_status = str(after.get("freshness_status") or "")
    raw_status = str(metadata.get("status") or "UNKNOWN")
    remaining = _strings(payload.get("remaining_limitations"))
    if raw_status == "OK" and after_status and after_status != "OK":
        status = "COMPLETED_BUT_NOT_RECOVERED"
        can_continue = False
        reason = "recover_freshness_completed_but_freshness_not_ok"
    elif raw_status in {"FAILED", "BLOCKED", "PARTIAL"}:
        status = raw_status
        can_continue = False
        reason = "recover_freshness_not_successful"
    else:
        status = raw_status
        can_continue = True
        reason = ""
    return {
        "status": status,
        "after_freshness_status": after_status,
        "remaining_limitations": remaining,
        "can_continue": can_continue,
        "reason": reason,
        "source_artifact": str(source_path),
    }


def _signal_snapshot_readiness(
    payload: Mapping[str, Any],
    source_path: Path,
    *,
    target_date: date,
    effective_data_date: date | None,
    latest_manifest_date: date | None,
) -> dict[str, Any]:
    if not payload:
        return {
            "status": "MISSING",
            "snapshot_date": "",
            "real_signals": 0,
            "proxy_signals": 0,
            "fallback_signals": 0,
            "missing_signals": 0,
            "can_continue": False,
            "reason": "signal_snapshot_missing",
            "source_artifact": "",
        }
    metadata = _mapping(payload.get("metadata"))
    summary = signal_snapshot_summary(dict(payload))
    snapshot_date = _parse_date(str(metadata.get("as_of") or source_path.parent.name))
    valid_dates = {item for item in (effective_data_date, latest_manifest_date) if item is not None}
    if not valid_dates:
        valid_dates = {target_date}
    status = str(summary.get("status") or metadata.get("status") or "UNKNOWN")
    real_count = _int_value(summary.get("real_signal_count"))
    proxy_count = _int_value(summary.get("proxy_signal_count"))
    fallback_count = _int_value(summary.get("fallback_signal_count"))
    missing_count = _int_value(summary.get("missing_signal_count"))
    failed_count = _int_value(summary.get("failed_signal_count"))
    can_continue = (
        status in {"OK", "LIMITED"}
        and snapshot_date in valid_dates
        and real_count >= 2
        and missing_count == 0
        and failed_count == 0
    )
    warning = ""
    reason = ""
    display_status = status
    if snapshot_date not in valid_dates:
        display_status = "DATE_MISMATCH"
        reason = "signal_snapshot_date_mismatch"
    elif missing_count:
        reason = "signal_snapshot_missing_required_signals"
    elif failed_count:
        reason = "signal_snapshot_failed_required_signals"
    elif real_count < 2:
        reason = "signal_snapshot_real_signal_floor_not_met"
    elif status == "LIMITED":
        warning = "Signal quality is LIMITED; stable tuning remains shadow-only."
    elif status not in {"OK", "LIMITED"}:
        reason = "signal_snapshot_status_not_usable"
    return {
        "status": display_status,
        "raw_status": status,
        "snapshot_date": snapshot_date.isoformat() if snapshot_date else "",
        "required_alignment_dates": sorted(item.isoformat() for item in valid_dates),
        "real_signals": real_count,
        "proxy_signals": proxy_count,
        "fallback_signals": fallback_count,
        "missing_signals": missing_count,
        "failed_signals": failed_count,
        "real_signal_names": _strings(summary.get("real_signals")),
        "proxy_signal_names": _strings(summary.get("proxy_signals")),
        "fallback_signal_names": _strings(summary.get("neutral_fallback_signals")),
        "missing_signal_names": _strings(summary.get("missing_signals")),
        "can_continue": can_continue,
        "reason": reason,
        "warning": warning,
        "source_artifact": str(source_path),
    }


def _backtest_manifest_readiness(
    payload: Mapping[str, Any],
    source_path: Path,
    *,
    root: Path,
    as_of: date,
) -> dict[str, Any]:
    manifest_path = (
        root
        / "artifacts"
        / "backtest_snapshots"
        / as_of.isoformat()
        / "backtest_input_manifest.json"
    )
    if not payload:
        return {
            "status": "MISSING",
            "can_continue": False,
            "reason": "backtest_input_diagnostics_missing",
            "required_range": {},
            "available_range": {},
            "source_artifact": "",
            "manifest_artifact": str(manifest_path),
        }
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    checks = _mapping(payload.get("checks"))
    date_coverage = _mapping(checks.get("date_coverage"))
    status = str(summary.get("overall_status") or metadata.get("status") or "UNKNOWN")
    can_continue = bool(summary.get("can_run_shadow_backtest")) and status in {"OK", "LIMITED"}
    reason = ""
    if date_coverage.get("status") in {"INSUFFICIENT_DATA", "FAILED"}:
        reason = "date_coverage_insufficient"
    elif not manifest_path.exists():
        reason = "backtest_input_manifest_missing"
        can_continue = False
    elif not can_continue:
        reason = "backtest_input_diagnostics_failed"
    return {
        "status": status,
        "can_continue": can_continue,
        "reason": reason,
        "backtest_mode": str(summary.get("backtest_mode") or "unknown"),
        "blocking_reasons": _strings(summary.get("blocking_reasons")),
        "required_range": {
            "start": str(
                date_coverage.get("required_start_date")
                or _mapping(metadata.get("requested_date_range")).get("start")
                or ""
            ),
            "end": str(
                date_coverage.get("required_end_date")
                or _mapping(metadata.get("requested_date_range")).get("end")
                or ""
            ),
        },
        "available_range": {
            "start": str(date_coverage.get("available_start_date") or ""),
            "end": str(date_coverage.get("available_end_date") or ""),
        },
        "source_artifact": str(source_path),
        "manifest_artifact": str(manifest_path),
    }


def _price_coverage_readiness(
    payload: Mapping[str, Any],
    *,
    recover_payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not payload:
        return {
            "status": "MISSING",
            "can_continue": False,
            "reason": "backtest_input_diagnostics_missing",
            "missing_symbols": [],
            "high_missing_ratio_symbols": [],
            "special_findings": [],
        }
    checks = _mapping(payload.get("checks"))
    asset_coverage = _mapping(checks.get("asset_coverage"))
    date_coverage = _mapping(checks.get("date_coverage"))
    price_data = _mapping(checks.get("price_data"))
    assets = _records(price_data.get("assets"))
    max_missing = _float_value(price_data.get("max_allowed_missing_ratio"), default=0.0)
    high_missing = [
        str(asset.get("symbol"))
        for asset in assets
        if str(asset.get("symbol") or "")
        and (
            asset.get("status") == "FAILED"
            or _float_value(asset.get("missing_ratio"), default=0.0) > max_missing
        )
    ]
    missing_symbols = _strings(asset_coverage.get("missing_assets"))
    status = "FAILED" if high_missing or missing_symbols else str(price_data.get("status") or "OK")
    can_continue = status in {"OK", "PASS"} and not high_missing and not missing_symbols
    findings: list[str] = []
    required_start = str(date_coverage.get("required_start_date") or "")
    required_end = str(date_coverage.get("required_end_date") or "")
    available_start = str(date_coverage.get("available_start_date") or "")
    available_end = str(date_coverage.get("available_end_date") or "")
    if available_start and available_start == available_end and required_start != available_start:
        findings.append("SINGLE_DAY_PRICE_CACHE")
    if high_missing and _mapping(recover_payload.get("actions")).get("refreshed_backtest_manifest"):
        findings.append("REPAIRED_HISTORY_NOT_REGISTERED_OR_LATEST_VIEW_MISMATCH")
    if "BRK.B" in high_missing:
        findings.append("BRK_B_SYMBOL_MAPPING_REVIEW_REQUIRED")
    reason = ""
    if high_missing:
        reason = "price_missing_ratio_too_high"
    elif missing_symbols:
        reason = "required_assets_missing"
    elif not can_continue:
        reason = "price_coverage_not_ok"
    return {
        "status": status,
        "can_continue": can_continue,
        "reason": reason,
        "required_date_range": {"start": required_start, "end": required_end},
        "available_date_range": {"start": available_start, "end": available_end},
        "missing_symbols": missing_symbols,
        "high_missing_ratio_symbols": high_missing,
        "max_allowed_missing_ratio": max_missing,
        "asset_missing_ratios": {
            str(asset.get("symbol")): _float_value(asset.get("missing_ratio"), default=0.0)
            for asset in assets
            if str(asset.get("symbol") or "")
        },
        "special_findings": sorted(set(findings)),
    }


def _stable_tuning_eligibility(checks: Mapping[str, Any]) -> dict[str, Any]:
    blocking: list[str] = []
    for key in ("freshness", "signal_snapshot", "backtest_manifest", "price_coverage"):
        if _mapping(checks.get(key)).get("can_continue") is not True:
            blocking.append(key)
    recover = _mapping(checks.get("recover_freshness"))
    if recover.get("can_continue") is False:
        blocking.append("recover_freshness")
    if not blocking:
        signal_status = str(_mapping(checks.get("signal_snapshot")).get("raw_status") or "")
        status = "LIMITED_READY" if signal_status == "LIMITED" else "READY"
        return {
            "status": status,
            "can_run": True,
            "candidates_backtest_allowed": True,
            "blocking_checks": [],
            "reason": "Stable tuning inputs are ready for candidate backtest.",
        }
    price = _mapping(checks.get("price_coverage"))
    manifest = _mapping(checks.get("backtest_manifest"))
    freshness = _mapping(checks.get("freshness"))
    signal = _mapping(checks.get("signal_snapshot"))
    if recover.get("status") == "COMPLETED_BUT_NOT_RECOVERED":
        status = "RECOVERY_FAILED"
    elif price.get("reason") == "price_missing_ratio_too_high":
        status = "INSUFFICIENT_DATA"
    else:
        status = "BLOCKED"
    reason = (
        _first_text(
            price.get("reason"),
            manifest.get("reason"),
            freshness.get("reason"),
            signal.get("reason"),
            "input_readiness_blocked",
        )
        or "input_readiness_blocked"
    )
    return {
        "status": status,
        "can_run": False,
        "candidates_backtest_allowed": False,
        "blocking_checks": sorted(set(blocking)),
        "reason": reason,
    }


def _blocking_errors(checks: Mapping[str, Any]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for key, value in checks.items():
        check = _mapping(value)
        if check.get("can_continue") is False:
            errors.append(
                {
                    "check": key,
                    "status": check.get("status", "UNKNOWN"),
                    "reason": check.get("reason", ""),
                }
            )
    return errors


def _recovery_plan(
    checks: Mapping[str, Any],
    eligibility: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if eligibility.get("can_run") is True:
        return []
    steps: list[dict[str, Any]] = []
    step = 1
    freshness = _mapping(checks.get("freshness"))
    signal = _mapping(checks.get("signal_snapshot"))
    manifest = _mapping(checks.get("backtest_manifest"))
    price = _mapping(checks.get("price_coverage"))
    if freshness.get("can_continue") is not True:
        steps.append(
            _plan_step(
                step,
                "rerun_freshness_check",
                "aits data freshness --latest",
                "确认 latest tracking/effective/manifest date 与 freshness status。",
            )
        )
        step += 1
        steps.append(
            _plan_step(
                step,
                "recover_freshness",
                "aits data recover-freshness --latest",
                "尝试恢复 freshness，但不得生成 synthetic price history。",
            )
        )
        step += 1
    if signal.get("can_continue") is not True:
        steps.append(
            _plan_step(
                step,
                "build_signal_snapshot",
                "aits signals build-snapshot --latest",
                "重建与 effective data date / manifest date 对齐的 signal snapshot。",
            )
        )
        step += 1
        steps.append(
            _plan_step(
                step,
                "validate_signal_snapshot",
                "aits signals validate-snapshot --latest",
                "确认 snapshot 存在、未过期、real signal floor 满足且无 missing signals。",
            )
        )
        step += 1
    if manifest.get("can_continue") is not True or price.get("can_continue") is not True:
        steps.append(
            _plan_step(
                step,
                "inspect_price_cache_registry",
                "aits data inspect-registry --latest",
                "检查 repaired GOOGL / BRK.B / SGOV 历史是否仍注册到 primary cache。",
            )
        )
        step += 1
        steps.append(
            _plan_step(
                step,
                "reconcile_price_cache",
                "aits data reconcile-price-cache --latest",
                "把可审计 repaired history 对齐到 cache registry 和 latest manifest。",
            )
        )
        step += 1
        steps.append(
            _plan_step(
                step,
                "refresh_backtest_manifest",
                "aits data refresh-backtest-manifest --latest",
                "重新生成包含完整历史覆盖的 backtest input manifest。",
            )
        )
        step += 1
        steps.append(
            _plan_step(
                step,
                "diagnose_backtest_inputs",
                "aits data diagnose-backtest-inputs --latest",
                "确认 can_run_shadow_backtest=true 且价格覆盖通过。",
            )
        )
        step += 1
    steps.append(
        _plan_step(
            step,
            "rerun_stable_weight_tuning",
            "aits parameters tune-weights-stable --latest",
            "仅在 input readiness 恢复后重新进入 stable candidate backtest。",
        )
    )
    return steps


def _reader_brief_sentence(
    eligibility: Mapping[str, Any],
    checks: Mapping[str, Any],
) -> str:
    if eligibility.get("can_run") is True:
        return (
            "Stable weight tuning input readiness is restored. The next stable tuning "
            "run can proceed to candidate backtesting, while production promotion "
            "remains disabled."
        )
    freshness = _mapping(checks.get("freshness"))
    manifest = _mapping(checks.get("backtest_manifest"))
    price = _mapping(checks.get("price_coverage"))
    symbols = ", ".join(_strings(price.get("high_missing_ratio_symbols"))) or "unknown symbols"
    return (
        "Stable weight tuning remains blocked before backtest. Latest readiness shows "
        f"freshness={freshness.get('status', 'UNKNOWN')}, "
        f"manifest={manifest.get('status', 'UNKNOWN')}, and price coverage is blocked "
        f"for {symbols}. TRADING-061 should not be interpreted until input readiness "
        "is restored."
    )


def _previous_stable_tuning_context(
    payload: Mapping[str, Any],
    source_path: Path,
) -> dict[str, Any]:
    if not payload:
        return {
            "previous_stable_tuning_status": "MISSING",
            "previous_candidates_backtested": 0,
            "previous_reason": "",
            "previous_artifact": "",
        }
    metadata = _mapping(payload.get("metadata"))
    search = _mapping(payload.get("search_summary"))
    recommended = _mapping(payload.get("recommended_candidate"))
    return {
        "previous_stable_tuning_status": str(metadata.get("status") or "UNKNOWN"),
        "previous_candidates_backtested": _int_value(search.get("candidates_backtested")),
        "previous_reason": str(
            metadata.get("reason") or search.get("reason") or recommended.get("reason") or ""
        ),
        "previous_artifact": str(source_path),
    }


def _input_path(
    root: Path,
    directory: str,
    as_of: date,
    filename: str,
    *,
    latest_when_missing: bool,
    latest_func: Any | None,
) -> Path:
    dated = root / "artifacts" / directory / as_of.isoformat() / filename
    if dated.exists() or not latest_when_missing:
        return dated
    if latest_func is not None and root == PROJECT_ROOT:
        latest = latest_func()
        if latest is not None:
            return latest
    return _latest_path(root / "artifacts" / directory, filename) or dated


def _latest_input_date(
    root: Path,
    *,
    freshness_path: Path | None,
    refresh_path: Path | None,
    signal_snapshot_path: Path | None,
    backtest_diagnostics_path: Path | None,
    stable_tuning_path: Path | None,
    default_date: date,
) -> date:
    for path in (
        freshness_path,
        refresh_path,
        signal_snapshot_path,
        backtest_diagnostics_path,
        stable_tuning_path,
        _latest_path(root / "artifacts" / "data_freshness", "market_data_freshness_summary.json"),
        _latest_path(root / "artifacts" / "data_quality", "backtest_input_diagnostics.json"),
        _latest_path(root / "artifacts" / "signal_snapshots", "signal_snapshot.json"),
        _latest_path(root / "artifacts" / "weight_stability", "weight_stability_summary.json"),
    ):
        if path is None:
            continue
        parsed = _parse_date(path.parent.name)
        if parsed is not None:
            return parsed
    return default_date


def _latest_path(root: Path, filename: str) -> Path | None:
    candidates = sorted(root.glob(f"*/{filename}"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _resolve_config_path(path: Path | str, root: Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    if root == PROJECT_ROOT:
        return resolve_project_path(str(candidate))
    return root / candidate


def _plan_step(step: int, action: str, command: str, reason: str) -> dict[str, Any]:
    return {
        "step": step,
        "action": action,
        "command": command,
        "reason": reason,
        "auto_executed": False,
    }


def _definition_lines(payload: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    lines = []
    for key in keys:
        value = payload.get(key, "")
        if isinstance(value, list):
            value = ", ".join(str(item) for item in value) or "none"
        elif isinstance(value, dict):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines)


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "production_write_allowed": False,
        "production_config_modified": False,
        "data_quality_gate_lowered": False,
        "mock_data_used": False,
        "synthetic_price_history_generated": False,
        "fallback_signals_relaxed": False,
        "candidate_backtest_run_when_blocked": False,
        "candidate_promotion_triggered": False,
        "broker_action": False,
        "trading_action": False,
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _int_value(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_value(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _first_text(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


__all__ = [
    "DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH",
    "WEIGHT_STABILITY_READINESS_ALIAS_REPORT_TYPE",
    "WEIGHT_STABILITY_READINESS_REPORT_TYPE",
    "WEIGHT_STABILITY_READINESS_SCHEMA_VERSION",
    "WeightStabilityReadinessRun",
    "build_weight_stability_readiness_payload",
    "default_weight_stability_readiness_json_path",
    "default_weight_stability_readiness_markdown_path",
    "default_weight_stability_readiness_root",
    "latest_weight_stability_readiness_path",
    "latest_weight_stability_readiness_path_on_or_before",
    "load_weight_stability_readiness_payload",
    "render_weight_stability_readiness_explanation",
    "render_weight_stability_readiness_markdown",
    "run_weight_stability_readiness",
    "validate_weight_stability_readiness_payload",
    "weight_stability_readiness_payload_date",
    "write_weight_stability_readiness_report_alias",
    "write_weight_stability_readiness_summary",
]
