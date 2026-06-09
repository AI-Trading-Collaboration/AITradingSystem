from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.trading_engine.parameters.parameter_loader import resolve_project_path
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    latest_weight_stability_readiness_path_on_or_before,
    load_weight_stability_readiness_payload,
    run_weight_stability_readiness,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    build_weight_tuning_payload,
    calculate_weight_stability,
    estimate_turnover_prefilter,
    load_weight_tuning_config,
    validate_weight_tuning_payload,
    weight_tuning_payload_date,
)

WEIGHT_STABILITY_SCHEMA_VERSION = 1
WEIGHT_STABILITY_REPORT_TYPE = "weight_stability"
WEIGHT_STABILITY_ALIAS_REPORT_TYPE = "weight_stability_report"
WEIGHT_STABILITY_CANDIDATES_REPORT_TYPE = "stable_weight_candidates"
DEFAULT_WEIGHT_STABILITY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "weight_tuning_v0_2_stability.yaml"
)


@dataclass(frozen=True)
class WeightStabilityRun:
    as_of: date
    payload: dict[str, Any]
    candidates_payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    candidates_path: Path
    recommended_weights_path: Path | None = None
    readiness_path: Path | None = None


def default_weight_stability_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "weight_stability"


def default_weight_stability_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_weight_stability_json_path(output_root: Path, as_of: date) -> Path:
    return default_weight_stability_dir(output_root, as_of) / "weight_stability_summary.json"


def default_weight_stability_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_weight_stability_dir(output_root, as_of) / "weight_stability_summary.md"


def default_stable_weight_candidates_path(output_root: Path, as_of: date) -> Path:
    return default_weight_stability_dir(output_root, as_of) / "stable_weight_candidates.json"


def default_recommended_stable_shadow_weights_path(output_root: Path, as_of: date) -> Path:
    return (
        default_weight_stability_dir(output_root, as_of) / "recommended_stable_shadow_weights.yaml"
    )


def latest_weight_stability_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_weight_stability_root()
    candidates = sorted(root.glob("*/weight_stability_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_weight_stability_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_weight_stability_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_stability_summary.json"):
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
        reports_dir / f"weight_stability_{as_of.isoformat()}.json",
        reports_dir / f"weight_stability_{as_of.isoformat()}.md",
    )


def load_weight_stability_config(
    path: Path | str = DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
) -> dict[str, Any]:
    payload = load_weight_tuning_config(path)
    if _mapping(payload.get("metadata")).get("version") != "weight-tuning-v0.2-stability":
        raise ValueError("weight stability config version must be weight-tuning-v0.2-stability")
    if not _mapping(payload.get("stability_constraints")):
        raise ValueError("weight stability config missing stability_constraints")
    if not _mapping(payload.get("turnover_controls")):
        raise ValueError("weight stability config missing turnover_controls")
    return payload


def run_weight_stability(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
    portfolio_profile: str | None = None,
    signals: Sequence[str] | None = None,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> WeightStabilityRun:
    config = load_weight_stability_config(config_path)
    root = _output_root(config, dry_run=dry_run)
    readiness_run = run_weight_stability_readiness(
        as_of=as_of,
        config_path=config_path,
        dry_run=dry_run,
        generated_at=generated_at,
    )
    payload, candidates_payload = build_weight_stability_payload(
        as_of=as_of,
        config_path=config_path,
        portfolio_profile=portfolio_profile,
        signals=signals,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
        input_readiness_payload=readiness_run.payload,
        input_readiness_path=readiness_run.json_path,
    )
    resolved_as_of = weight_stability_payload_date(
        payload,
        default_weight_stability_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_weight_stability_json_path(root, resolved_as_of)
    markdown_path = default_weight_stability_markdown_path(root, resolved_as_of)
    candidates_path = default_stable_weight_candidates_path(root, resolved_as_of)
    recommended_path: Path | None = None
    write_weight_stability_summary(payload, json_path, markdown_path)
    write_stable_weight_candidates(candidates_payload, candidates_path)
    if _has_recommended_candidate(payload):
        recommended_path = default_recommended_stable_shadow_weights_path(root, resolved_as_of)
        write_recommended_stable_shadow_weights(payload, recommended_path)
    return WeightStabilityRun(
        as_of=resolved_as_of,
        payload=payload,
        candidates_payload=candidates_payload,
        json_path=json_path,
        markdown_path=markdown_path,
        candidates_path=candidates_path,
        recommended_weights_path=recommended_path,
        readiness_path=readiness_run.json_path,
    )


def build_weight_stability_payload(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
    portfolio_profile: str | None = None,
    signals: Sequence[str] | None = None,
    dry_run: bool = False,
    generated_at: datetime | None = None,
    output_root: Path | None = None,
    input_readiness_payload: Mapping[str, Any] | None = None,
    input_readiness_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_weight_stability_config(resolved_config_path)
    root = output_root or _output_root(config, dry_run=dry_run)
    tuning_payload, tuning_candidates = build_weight_tuning_payload(
        as_of=as_of,
        config_path=resolved_config_path,
        portfolio_profile=portfolio_profile,
        signals=signals,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
    )
    summary = _stability_summary_from_tuning(
        tuning_payload,
        tuning_candidates,
        config=config,
        config_path=resolved_config_path,
        output_root=root,
        dry_run=dry_run,
        input_readiness_payload=input_readiness_payload,
        input_readiness_path=input_readiness_path,
    )
    candidates = _stable_candidates_payload(summary, tuning_payload, tuning_candidates)
    return summary, candidates


def write_weight_stability_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_weight_stability_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_stable_weight_candidates(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_recommended_stable_shadow_weights(payload: dict[str, Any], output_path: Path) -> Path:
    recommended = _mapping(payload.get("recommended_candidate"))
    metadata = _mapping(payload.get("metadata"))
    yaml_payload = {
        "metadata": {
            "version": f"stable-shadow-weight-candidate-{payload.get('as_of', '')}",
            "source": "TRADING-061 stable weight tuning",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "signal_quality_status": _mapping(payload.get("signal_quality")).get(
                "status",
                metadata.get("status", "UNKNOWN"),
            ),
            "stability_profile": metadata.get("policy_version", ""),
        },
        "weights": {
            key: float(value) for key, value in _mapping(recommended.get("weights")).items()
        },
        "constraints": {
            "stability_status": _mapping(recommended.get("stability")).get(
                "stability_status",
                "UNKNOWN",
            ),
            "turnover_prefilter_status": _mapping(recommended.get("turnover_prefilter")).get(
                "status", "UNKNOWN"
            ),
            "production_write_allowed": False,
            "promotion_allowed": False,
        },
        "review": {
            "status": recommended.get("status", "watch"),
            "reason": recommended.get("reason", ""),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(yaml_payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return output_path


def write_weight_stability_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": WEIGHT_STABILITY_ALIAS_REPORT_TYPE,
        "source_report_type": WEIGHT_STABILITY_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_weight_stability_summary(alias_payload, json_path, markdown_path)


def load_weight_stability_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_weight_stability_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != WEIGHT_STABILITY_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        WEIGHT_STABILITY_REPORT_TYPE,
        WEIGHT_STABILITY_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in ("run_id", "generated_at", "status", "production_effect"):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if not isinstance(payload.get("search_summary"), dict):
        issues.append("search_summary must be an object")
    if not isinstance(payload.get("recommended_candidate"), dict):
        issues.append("recommended_candidate must be an object")
    safety = _mapping(payload.get("safety"))
    for key in (
        "production_config_modified",
        "production_write_allowed",
        "turnover_guardrail_modified",
        "cost_model_modified",
        "fallback_signals_free_tuned",
        "candidate_promotion_triggered",
        "trading_action",
    ):
        if safety.get(key) is not False:
            issues.append(f"{key} must be false")
    promotion_impact = _mapping(payload.get("promotion_impact"))
    if promotion_impact.get("can_support_candidate_promotion") is not False:
        issues.append("promotion_impact must not support candidate promotion")
    return issues


def weight_stability_payload_date(payload: Mapping[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("weight-stability-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(str(payload.get("as_of") or ""))
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer weight stability date from {source_path}") from exc


def render_weight_stability_explanation(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    search = _mapping(payload.get("search_summary"))
    recommended = _mapping(payload.get("recommended_candidate"))
    comparison = _mapping(payload.get("comparison_to_trading_059"))
    readiness = _mapping(payload.get("input_readiness"))
    return "\n".join(
        [
            f"status={metadata.get('status', 'UNKNOWN')}",
            f"reason={metadata.get('reason', '')}",
            f"recommended_status={recommended.get('status', 'UNKNOWN')}",
            f"candidates_generated={search.get('candidates_generated', 0)}",
            f"candidates_rejected_by_stability={search.get('candidates_rejected_by_stability', 0)}",
            "candidates_rejected_by_turnover_prefilter="
            f"{search.get('candidates_rejected_by_turnover_prefilter', 0)}",
            f"candidates_backtested={search.get('candidates_backtested', 0)}",
            f"candidates_passed_guardrails={search.get('candidates_passed_guardrails', 0)}",
            f"turnover_failures_reduced={comparison.get('turnover_failures_reduced', False)}",
            f"input_readiness_status={readiness.get('status', 'UNKNOWN')}",
            f"input_readiness_reason={readiness.get('reason', '')}",
            f"input_readiness_report={readiness.get('report', '')}",
            "production_effect=none",
            "manual_review_required=true",
            "auto_promotion=false",
        ]
    )


def render_weight_stability_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    context = _mapping(payload.get("input_context"))
    search = _mapping(payload.get("search_summary"))
    recommended = _mapping(payload.get("recommended_candidate"))
    comparison = _mapping(payload.get("comparison_to_trading_059"))
    stability = _mapping(payload.get("stability_constraints"))
    turnover = _mapping(payload.get("turnover_controls"))
    promotion_impact = _mapping(payload.get("promotion_impact"))
    readiness = _mapping(payload.get("input_readiness"))
    lines = [
        "# Weight Search Stability Summary",
        "",
        "## 1. 执行摘要",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- reason: `{metadata.get('reason', '')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- recommended_status: `{recommended.get('status', 'UNKNOWN')}`",
        f"- reason: {recommended.get('reason', '')}",
        f"- input_readiness_status: `{readiness.get('status', 'UNKNOWN')}`",
        f"- input_readiness_report: `{readiness.get('report', '')}`",
        "",
        "## 2. TRADING-060 输入背景",
        "",
        f"- previous_failure_root_cause: `{context.get('previous_failure_root_cause', '')}`",
        f"- previous_top_failure_reason: `{context.get('previous_top_failure_reason', '')}`",
        f"- previous_failed_by_turnover: `{context.get('previous_failed_by_turnover', 0)}`",
        "",
        "## 3. Stability Profile",
        "",
    ]
    for key, value in stability.items():
        if isinstance(value, dict):
            continue
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 4. Turnover Controls",
            "",
        ]
    )
    for key, value in turnover.items():
        if isinstance(value, dict):
            continue
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 5. Search Summary",
            "",
            f"- candidates_generated: `{search.get('candidates_generated', 0)}`",
            "- candidates_rejected_by_stability: "
            f"`{search.get('candidates_rejected_by_stability', 0)}`",
            "- candidates_rejected_by_turnover_prefilter: "
            f"`{search.get('candidates_rejected_by_turnover_prefilter', 0)}`",
            f"- candidates_backtested: `{search.get('candidates_backtested', 0)}`",
            f"- candidates_passed_guardrails: `{search.get('candidates_passed_guardrails', 0)}`",
            f"- reason: `{search.get('reason', '')}`",
            "",
            "## 5A. Input Readiness",
            "",
            f"- status: `{readiness.get('status', 'UNKNOWN')}`",
            f"- can_run: `{readiness.get('can_run', '')}`",
            f"- candidates_backtest_allowed: `{readiness.get('candidates_backtest_allowed', '')}`",
            f"- blocking_checks: `{', '.join(_strings(readiness.get('blocking_checks')))}`",
            f"- reason: `{readiness.get('reason', '')}`",
            f"- report: `{readiness.get('report', '')}`",
            "",
            "## 6. Recommended Candidate",
            "",
            f"- status: `{recommended.get('status', 'UNKNOWN')}`",
            f"- guardrail_status: `{recommended.get('guardrail_status', 'UNKNOWN')}`",
            "- stability_status: "
            f"`{_mapping(recommended.get('stability')).get('stability_status', '')}`",
            "- turnover_prefilter_status: "
            f"`{_mapping(recommended.get('turnover_prefilter')).get('status', '')}`",
            "",
            "| Signal | Weight |",
            "|---|---:|",
        ]
    )
    for key, value in sorted(_mapping(recommended.get("weights")).items()):
        lines.append(f"| `{key}` | {_format_float(value)} |")
    lines.extend(
        [
            "",
            "## 7. Comparison To TRADING-059",
            "",
            "- turnover_failures_reduced: "
            f"`{comparison.get('turnover_failures_reduced', False)}`",
            f"- cost_drag_improved: `{comparison.get('cost_drag_improved', False)}`",
            f"- candidate_found: `{comparison.get('candidate_found', False)}`",
            "",
            "## 8. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion_impact.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion_impact.get('reason', '')}",
            "",
            "## 9. Safety",
            "",
        ]
    )
    for key, value in _mapping(payload.get("safety")).items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _stability_summary_from_tuning(
    tuning_payload: Mapping[str, Any],
    tuning_candidates: Mapping[str, Any],
    *,
    config: Mapping[str, Any],
    config_path: Path,
    output_root: Path,
    dry_run: bool,
    input_readiness_payload: Mapping[str, Any] | None = None,
    input_readiness_path: Path | None = None,
) -> dict[str, Any]:
    as_of = weight_tuning_payload_date(
        tuning_payload,
        default_weight_stability_json_path(output_root, datetime.now(tz=UTC).date()),
    )
    metadata = _mapping(tuning_payload.get("metadata"))
    search = _mapping(tuning_payload.get("search"))
    signal_quality = _mapping(tuning_payload.get("signal_quality"))
    recommended = _mapping(tuning_payload.get("recommended_candidate"))
    evaluated_candidates = _records(tuning_candidates.get("candidates"))
    passed_guardrails = sum(
        1
        for candidate in evaluated_candidates
        if candidate.get("guardrail_status") == "PASS"
        or _mapping(candidate.get("guardrails")).get("status") == "PASS"
    )
    context = _previous_turnover_context(as_of)
    previous_failed = _int_value(context.get("previous_failed_by_turnover"))
    current_turnover_failures = sum(
        1
        for candidate in evaluated_candidates
        if "turnover_guardrail_failed" in _strings(candidate.get("rejection_reasons"))
    )
    current_cost_drag_failures = sum(
        1
        for candidate in evaluated_candidates
        if "cost_drag_too_high" in _strings(candidate.get("rejection_reasons"))
    )
    candidate_found = _candidate_found(recommended)
    status = _stability_status(metadata, signal_quality, candidate_found)
    input_readiness = _input_readiness_context(
        input_readiness_payload,
        input_readiness_path,
        as_of,
    )
    input_readiness_blocked = input_readiness.get("can_run") is False
    reason = str(metadata.get("reason") or "")
    if status == "INSUFFICIENT_DATA" and input_readiness_blocked:
        reason = "input_readiness_blocked"
    data_ready = status not in {"INSUFFICIENT_DATA", "FAILED"}
    output_artifacts = {
        "weight_stability_summary_json": str(
            default_weight_stability_json_path(output_root, as_of)
        ),
        "weight_stability_summary_md": str(
            default_weight_stability_markdown_path(output_root, as_of)
        ),
        "stable_weight_candidates": str(default_stable_weight_candidates_path(output_root, as_of)),
        "recommended_stable_shadow_weights": (
            str(default_recommended_stable_shadow_weights_path(output_root, as_of))
            if candidate_found
            else ""
        ),
    }
    payload = {
        "schema_version": WEIGHT_STABILITY_SCHEMA_VERSION,
        "report_type": WEIGHT_STABILITY_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-stability-{as_of.isoformat()}",
            "generated_at": metadata.get("generated_at", datetime.now(tz=UTC).isoformat()),
            "status": status,
            "reason": reason,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "market_regime": metadata.get("market_regime", "ai_after_chatgpt"),
            "market_regime_anchor": metadata.get("market_regime_anchor", "2022-11-30"),
            "requested_date_range": metadata.get("requested_date_range", {}),
            "config_path": str(config_path),
            "source_task": "TRADING-061",
            "policy_version": _mapping(config.get("metadata")).get(
                "version",
                "weight-tuning-v0.2-stability",
            ),
            "config_hash": _config_hash(config_path),
        },
        "inputs": _mapping(tuning_payload.get("inputs")),
        "input_artifacts": _mapping(tuning_payload.get("input_artifacts")),
        "input_context": context,
        "input_readiness": input_readiness,
        "output_artifacts": output_artifacts,
        "data_quality": _mapping(tuning_payload.get("data_quality")),
        "freshness": _mapping(tuning_payload.get("freshness")),
        "signal_quality": signal_quality,
        "baseline": _mapping(tuning_payload.get("baseline")),
        "stability_constraints": _mapping(config.get("stability_constraints")),
        "turnover_controls": _mapping(config.get("turnover_controls")),
        "objective": _mapping(config.get("objective")),
        "guardrail_policy": _mapping(config.get("guardrails")),
        "search_summary": {
            "method": _mapping(config.get("search")).get(
                "method",
                "stable_restricted_grid_search",
            ),
            "candidates_generated": search.get("candidates_generated", 0),
            "candidates_rejected_by_stability": search.get(
                "candidates_rejected_by_stability",
                0,
            ),
            "candidates_rejected_by_turnover_prefilter": search.get(
                "candidates_rejected_by_turnover_prefilter",
                0,
            ),
            "candidates_backtested": search.get("candidates_evaluated", 0),
            "candidates_rejected_by_guardrails": search.get(
                "candidates_rejected_by_guardrails",
                0,
            ),
            "candidates_passed_guardrails": passed_guardrails,
            "reason": reason,
        },
        "candidate_ranking": tuning_payload.get("candidate_ranking", []),
        "recommended_candidate": _stable_recommended_candidate(
            recommended,
            candidate_found,
            input_readiness_blocked=input_readiness_blocked,
        ),
        "comparison_to_trading_059": {
            "turnover_failures_reduced": (
                data_ready and previous_failed > 0 and current_turnover_failures < previous_failed
            ),
            "cost_drag_improved": (
                data_ready and previous_failed > 0 and current_cost_drag_failures < previous_failed
            ),
            "candidate_found": candidate_found,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Stable weight tuning has not entered valid backtest because input "
                "readiness is blocked."
                if input_readiness_blocked
                else "Stable weight tuning remains shadow-only because signal quality "
                "is LIMITED and manual review is required."
            ),
        },
        "reader_brief": _reader_brief_sentence(
            candidate_found,
            input_readiness_blocked=input_readiness_blocked,
        ),
        "source_weight_tuning_payload": tuning_payload,
        "warnings": _strings(tuning_payload.get("warnings")),
        "safety": _safety_payload(),
    }
    issues = validate_weight_tuning_payload(dict(tuning_payload))
    if issues:
        payload["warnings"] = [
            *_strings(payload.get("warnings")),
            "source weight tuning payload validation issues: " + "; ".join(issues),
        ]
    return payload


def _stable_candidates_payload(
    summary: Mapping[str, Any],
    tuning_payload: Mapping[str, Any],
    tuning_candidates: Mapping[str, Any],
) -> dict[str, Any]:
    evaluated_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in _records(tuning_candidates.get("candidates"))
    }
    diagnostics = []
    for record in _records(tuning_payload.get("candidate_diagnostics")):
        merged = dict(record)
        evaluated = evaluated_by_id.get(str(record.get("candidate_id")))
        if evaluated:
            merged.update(evaluated)
        diagnostics.append(merged)
    if not diagnostics:
        diagnostics = [dict(candidate) for candidate in evaluated_by_id.values()]
    return {
        "schema_version": WEIGHT_STABILITY_SCHEMA_VERSION,
        "report_type": WEIGHT_STABILITY_CANDIDATES_REPORT_TYPE,
        "metadata": {
            "run_id": _mapping(summary.get("metadata")).get("run_id"),
            "generated_at": _mapping(summary.get("metadata")).get("generated_at"),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "summary_artifact": _mapping(summary.get("output_artifacts")).get(
            "weight_stability_summary_json",
            "",
        ),
        "candidate_count": len(diagnostics),
        "candidates": diagnostics,
        "safety": _safety_payload(),
    }


def _stable_recommended_candidate(
    recommended: Mapping[str, Any],
    candidate_found: bool,
    *,
    input_readiness_blocked: bool = False,
) -> dict[str, Any]:
    payload = dict(recommended)
    if candidate_found:
        return payload
    payload["status"] = "no_candidate"
    payload["reason"] = (
        "input_readiness_blocked"
        if input_readiness_blocked
        else str(
            recommended.get("reason")
            or "Stable weight tuning did not find a guardrail-passing candidate."
        )
    )
    payload.setdefault("weights", {})
    payload.setdefault("stability", {})
    payload.setdefault("turnover_prefilter", {})
    return payload


def _previous_turnover_context(as_of: date) -> dict[str, Any]:
    path = _latest_turnover_attribution_path(as_of)
    if path is None:
        return {
            "previous_failure_root_cause": "MISSING",
            "previous_top_failure_reason": "",
            "previous_failed_by_turnover": 0,
            "source_artifact": "",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "previous_failure_root_cause": "UNREADABLE",
            "previous_top_failure_reason": "",
            "previous_failed_by_turnover": 0,
            "source_artifact": str(path),
        }
    summary = _mapping(payload.get("summary"))
    turnover = _mapping(payload.get("candidate_turnover_summary"))
    return {
        "previous_failure_root_cause": str(summary.get("root_cause_category") or ""),
        "previous_top_failure_reason": str(summary.get("top_failure_reason") or ""),
        "previous_failed_by_turnover": _int_value(
            turnover.get("total_failed_by_turnover"),
            default=_int_value(summary.get("turnover_failed_candidates")),
        ),
        "source_artifact": str(path),
    }


def _latest_turnover_attribution_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_turnover_attribution"
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
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _candidate_found(recommended: Mapping[str, Any]) -> bool:
    return str(recommended.get("status") or "") in {"watch", "shadow_candidate_only"}


def _has_recommended_candidate(payload: Mapping[str, Any]) -> bool:
    return _candidate_found(_mapping(payload.get("recommended_candidate")))


def _stability_status(
    metadata: Mapping[str, Any],
    signal_quality: Mapping[str, Any],
    candidate_found: bool,
) -> str:
    if metadata.get("status") in {"INSUFFICIENT_DATA", "FAILED"}:
        return str(metadata.get("status"))
    if candidate_found:
        return str(signal_quality.get("status") or "LIMITED")
    if signal_quality.get("status") in {"LIMITED", "OK"}:
        return str(signal_quality.get("status"))
    return "NO_CANDIDATE"


def _reader_brief_sentence(
    candidate_found: bool,
    *,
    input_readiness_blocked: bool = False,
) -> str:
    if input_readiness_blocked:
        return (
            "Stable weight tuning remains blocked before candidate backtest because "
            "input readiness is not satisfied. This is not evidence that stability "
            "constraints failed to find a candidate."
        )
    if candidate_found:
        return (
            "Stable weight tuning found a shadow-only candidate after adding L1 "
            "distance and turnover-aware constraints. Production promotion remains "
            "disabled because signal quality is LIMITED."
        )
    return (
        "Stable weight tuning reduced aggressive candidates but still did not find a "
        "guardrail-passing weight candidate. This suggests current real signals may "
        "not provide enough stable improvement over baseline."
    )


def _input_readiness_context(
    payload: Mapping[str, Any] | None,
    source_path: Path | None,
    as_of: date,
) -> dict[str, Any]:
    resolved_payload: Mapping[str, Any] | None = payload
    resolved_path = source_path
    if resolved_payload is None:
        resolved_path = latest_weight_stability_readiness_path_on_or_before(as_of)
        if resolved_path is not None:
            resolved_payload = load_weight_stability_readiness_payload(resolved_path)
    if not resolved_payload:
        return {
            "status": "MISSING",
            "can_run": None,
            "candidates_backtest_allowed": None,
            "blocking_checks": [],
            "reason": "",
            "report": "" if resolved_path is None else str(resolved_path),
        }
    metadata = _mapping(resolved_payload.get("metadata"))
    eligibility = _mapping(resolved_payload.get("stable_tuning_eligibility"))
    return {
        "status": str(metadata.get("status") or eligibility.get("status") or "UNKNOWN"),
        "can_run": eligibility.get("can_run"),
        "candidates_backtest_allowed": eligibility.get("candidates_backtest_allowed"),
        "blocking_checks": _strings(eligibility.get("blocking_checks")),
        "reason": str(eligibility.get("reason") or metadata.get("reason") or ""),
        "report": "" if resolved_path is None else str(resolved_path),
        "summary": str(resolved_payload.get("reader_brief") or ""),
    }


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "production_write_allowed": False,
        "production_config_modified": False,
        "turnover_guardrail_modified": False,
        "cost_model_modified": False,
        "fallback_signals_free_tuned": False,
        "candidate_promotion_triggered": False,
        "broker_action": False,
        "trading_action": False,
    }


def _output_root(config: Mapping[str, Any], *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "weight_stability"
    output = _mapping(config.get("output"))
    return resolve_project_path(
        str(output.get("weight_stability_dir") or default_weight_stability_root())
    )


def _config_hash(*paths: Path) -> str:
    from hashlib import sha256

    digest = sha256()
    for path in paths:
        if path.exists() and path.is_file():
            digest.update(str(path).encode("utf-8"))
            digest.update(sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


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


def _format_float(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"


__all__ = [
    "DEFAULT_WEIGHT_STABILITY_CONFIG_PATH",
    "WEIGHT_STABILITY_ALIAS_REPORT_TYPE",
    "WEIGHT_STABILITY_REPORT_TYPE",
    "WEIGHT_STABILITY_SCHEMA_VERSION",
    "WeightStabilityRun",
    "build_weight_stability_payload",
    "calculate_weight_stability",
    "default_recommended_stable_shadow_weights_path",
    "default_stable_weight_candidates_path",
    "default_weight_stability_json_path",
    "default_weight_stability_markdown_path",
    "default_weight_stability_root",
    "estimate_turnover_prefilter",
    "latest_weight_stability_path",
    "latest_weight_stability_path_on_or_before",
    "load_weight_stability_config",
    "load_weight_stability_payload",
    "render_weight_stability_explanation",
    "render_weight_stability_markdown",
    "run_weight_stability",
    "validate_weight_stability_payload",
    "weight_stability_payload_date",
    "write_recommended_stable_shadow_weights",
    "write_stable_weight_candidates",
    "write_weight_stability_report_alias",
    "write_weight_stability_summary",
]
