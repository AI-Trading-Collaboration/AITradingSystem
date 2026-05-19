from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

SHADOW_WEIGHT_ITERATION_SCHEMA_VERSION = "1.0"
SHADOW_WEIGHT_ITERATION_TASK_ID = "TRADING-018B"
SHADOW_WEIGHT_ITERATION_REPORT_TYPE = "daily_shadow_weight_iteration"
CURRENT_SHADOW_WEIGHTS_REPORT_TYPE = "current_shadow_weights"
MODE_SHADOW_ONLY = "shadow_only"
PRODUCTION_EFFECT_NONE = "none"
DECISION_UPDATE = "UPDATE"
DECISION_NO_UPDATE = "NO_UPDATE"
DECISION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_ERROR = "ERROR"

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DAILY_SHADOW_WEIGHT_ITERATION_POLICY_PATH = (
    REPO_ROOT / "config" / "daily_shadow_weight_iteration_policy.yaml"
)
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"

EMBEDDED_POLICY_DEFAULTS = {
    "policy_id": "daily_shadow_weight_iteration_policy",
    "version": "embedded_default",
    "status": "missing_policy_embedded_defaults",
    "owner": "system",
    "production_effect": PRODUCTION_EFFECT_NONE,
    "thresholds": {
        "max_abs_delta_per_day": 0.02,
        "max_relative_delta_per_day": 0.05,
        "minimum_adjustment_confidence": 0.60,
        "target_total_weight_sum": 1.0,
        "total_weight_tolerance": 1e-6,
        "min_weight": 0.0,
        "max_weight": 1.0,
    },
    "confidence_defaults": {
        "ready_for_manual_review": 0.80,
        "candidate_promising_but_limited": 0.70,
    },
    "conservative_default_weights": {
        "technical": 0.25,
        "fundamental": 0.25,
        "macro": 0.20,
        "policy": 0.15,
        "sentiment": 0.15,
    },
}


FALLBACK_CONSERVATIVE_DEFAULT_WEIGHTS = {
    "technical": 0.25,
    "fundamental": 0.25,
    "macro": 0.20,
    "policy": 0.15,
    "sentiment": 0.15,
}

REQUIRED_ARTIFACTS = {
    "trading_015": {
        "label": "TRADING-015",
        "filename": "weight_adjustment_candidates_{date}.json",
        "report_type": "weight_adjustment_candidates",
    },
    "trading_016": {
        "label": "TRADING-016",
        "filename": "weight_candidate_evaluation_{date}.json",
        "report_type": "weight_candidate_evaluation",
    },
    "trading_017": {
        "label": "TRADING-017",
        "filename": "weight_promotion_gate_{date}.json",
        "report_type": "weight_promotion_gate",
    },
    "trading_018": {
        "label": "TRADING-018",
        "filename": "daily_weight_adjustment_summary_{date}.json",
        "report_type": "daily_weight_adjustment_summary",
    },
}

SCHEDULER_DRY_RUN_FILENAMES = (
    "daily_weight_adjustment_scheduler_dry_run_{date}.json",
    "scheduler_dry_run_daily_weight_adjustment_{date}.json",
    "daily_weight_adjustment_dry_run_{date}.json",
)


def default_shadow_iteration_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "shadow"


def default_current_shadow_weights_path(data_root: Path) -> Path:
    return default_shadow_iteration_root(data_root) / "current_shadow_weights.json"


def default_shadow_weight_candidate_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_shadow_iteration_root(data_root)
        / "candidates"
        / f"shadow_weight_candidate_{as_of.isoformat()}.json"
    )


def build_daily_shadow_weight_iteration_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    data_root: Path = REPO_ROOT / "data",
    policy_path: Path = DEFAULT_DAILY_SHADOW_WEIGHT_ITERATION_POLICY_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    scheduler_dry_run_path: Path | None = None,
    current_state: dict[str, Any] | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    force_no_update: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_shadow_weight_candidate_json_path(
        data_root, as_of
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    policy = _load_policy(policy_path)
    thresholds = _thresholds(policy)
    current = current_state or _default_current_shadow_state(
        as_of=as_of,
        production_profile_path=production_profile_path,
        policy=policy,
        generated_at=generated,
    )
    previous_weights = _weights_from_mapping(_mapping(current.get("weights")))

    artifact_paths = _resolve_input_artifact_paths(
        reports_dir=reports_dir,
        as_of=as_of,
        scheduler_dry_run_path=scheduler_dry_run_path,
    )
    artifacts = {
        key: _artifact_record(
            path,
            reports_dir,
            expected_report_type=_string_value(spec.get("report_type")),
        )
        for key, (path, spec) in artifact_paths.items()
    }
    payloads = {key: _read_json_object(path) for key, (path, _spec) in artifact_paths.items()}
    missing = [
        _string_value(artifacts[key].get("label"))
        for key in artifacts
        if not bool(artifacts[key].get("valid"))
    ]
    safety = _scheduler_safety_checks(payloads.get("trading_018a", {}))
    confidence = _adjustment_confidence(
        daily_summary=payloads.get("trading_018", {}),
        evaluation_payload=payloads.get("trading_016", {}),
        promotion_payload=payloads.get("trading_017", {}),
        policy=policy,
    )
    raw_delta = _raw_delta_from_artifacts(
        previous_weights=previous_weights,
        daily_summary=payloads.get("trading_018", {}),
        candidate_payload=payloads.get("trading_015", {}),
    )
    decision, decision_reason = _decision(
        missing_artifacts=missing,
        safety=safety,
        daily_summary=payloads.get("trading_018", {}),
        promotion_payload=payloads.get("trading_017", {}),
        confidence=confidence,
        minimum_confidence=thresholds["minimum_adjustment_confidence"],
        raw_delta=raw_delta,
        policy_file_exists=bool(policy.get("policy_file_exists")),
        force_no_update=force_no_update,
        dry_run=dry_run,
    )
    constrained = _apply_conservative_update(
        previous_weights=previous_weights,
        raw_delta=raw_delta if decision == DECISION_UPDATE else {},
        thresholds=thresholds,
    )
    new_weights = (
        constrained["new_shadow_weights"] if decision == DECISION_UPDATE else dict(previous_weights)
    )
    proposed_delta = (
        constrained["proposed_delta"]
        if decision == DECISION_UPDATE
        else {key: 0.0 for key in previous_weights}
    )
    evidence = _evidence(
        decision=decision,
        proposed_delta=proposed_delta,
        raw_delta=raw_delta,
        daily_summary=payloads.get("trading_018", {}),
        candidate_payload=payloads.get("trading_015", {}),
    )
    payload = {
        "schema_version": SHADOW_WEIGHT_ITERATION_SCHEMA_VERSION,
        "report_type": SHADOW_WEIGHT_ITERATION_REPORT_TYPE,
        "task_id": SHADOW_WEIGHT_ITERATION_TASK_ID,
        "date": as_of.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "mode": MODE_SHADOW_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "decision": decision,
        "decision_reason": decision_reason,
        "dry_run": dry_run,
        "input_artifacts": artifacts,
        "safety_checks": safety,
        "policy": _policy_report(policy, policy_path),
        "adjustment_confidence": confidence,
        "minimum_adjustment_confidence": thresholds["minimum_adjustment_confidence"],
        "previous_shadow_weights": _rounded_weights(previous_weights),
        "raw_delta": _rounded_weights(raw_delta),
        "proposed_delta": _rounded_weights(proposed_delta),
        "new_shadow_weights": _rounded_weights(new_weights),
        "constraints_applied": constrained["constraints_applied"],
        "evidence": evidence,
        "outputs": _output_paths(
            data_root=data_root,
            as_of=as_of,
            candidate_json=output_json_path,
            candidate_md=output_md_path,
        ),
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "runs_weight_adjustment_pipeline": False,
            "runs_scheduler_dry_run": False,
            "writes_production_profile": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "audit": {
            "created_at": generated.isoformat(),
            "created_by": "scripts/run_daily_shadow_weight_iteration.py",
            "safe_for_scheduler": decision in {DECISION_UPDATE, DECISION_NO_UPDATE},
            "safe_for_production": False,
            "current_state_initialized": current_state is None,
        },
    }
    _assert_shadow_safety_invariants(payload)
    return payload


def write_daily_shadow_weight_iteration_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    data_root: Path = REPO_ROOT / "data",
    policy_path: Path = DEFAULT_DAILY_SHADOW_WEIGHT_ITERATION_POLICY_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    scheduler_dry_run_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    force_no_update: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    current_path = default_current_shadow_weights_path(data_root)
    existing_state = _read_json_object(current_path)
    current_state = (
        existing_state
        if existing_state.get("report_type") == CURRENT_SHADOW_WEIGHTS_REPORT_TYPE
        else None
    )
    candidate = build_daily_shadow_weight_iteration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        data_root=data_root,
        policy_path=policy_path,
        production_profile_path=production_profile_path,
        scheduler_dry_run_path=scheduler_dry_run_path,
        current_state=current_state,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated,
        force_no_update=force_no_update,
        dry_run=dry_run,
    )
    outputs = _mapping(candidate.get("outputs"))
    candidate_json_path = Path(str(outputs["candidate_json"]))
    candidate_md_path = Path(str(outputs["candidate_markdown"]))
    _write_json(candidate_json_path, candidate)
    _write_text(candidate_md_path, render_daily_shadow_weight_iteration_report(candidate))

    previous_state = current_state or _default_current_shadow_state(
        as_of=as_of,
        production_profile_path=production_profile_path,
        policy=_load_policy(policy_path),
        generated_at=generated,
    )
    current_state_updated = False
    history_written = False
    if candidate["decision"] == DECISION_UPDATE and not dry_run:
        final_state = _updated_current_shadow_state(
            previous_state=previous_state,
            candidate=candidate,
            as_of=as_of,
            generated_at=generated,
        )
        history_json_path = Path(str(outputs["history_json"]))
        history_md_path = Path(str(outputs["history_markdown"]))
        _write_json(history_json_path, final_state)
        _write_text(history_md_path, render_current_shadow_weights_report(final_state))
        history_written = True
        _atomic_write_json(current_path, final_state)
        current_state_updated = True
    elif current_state is None and not dry_run:
        _atomic_write_json(current_path, previous_state)

    run_log = _run_log_payload(
        candidate=candidate,
        current_state_path=current_path,
        current_state_updated=current_state_updated,
        history_written=history_written,
        generated_at=generated,
    )
    run_log_json_path = Path(str(outputs["run_log_json"]))
    run_log_md_path = Path(str(outputs["run_log_markdown"]))
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_weight_iteration_run_log(run_log))
    candidate["run_log"] = {
        "json": str(run_log_json_path),
        "markdown": str(run_log_md_path),
        "current_state_updated": current_state_updated,
        "history_written": history_written,
    }
    return candidate


def render_daily_shadow_weight_iteration_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    safety = _mapping(payload.get("safety_checks"))
    previous = _mapping(payload.get("previous_shadow_weights"))
    proposed = _mapping(payload.get("proposed_delta"))
    new_weights = _mapping(payload.get("new_shadow_weights"))
    evidence = _mapping(payload.get("evidence"))
    supporting = _list_mappings(evidence.get("supporting_signals"))
    opposing = _strings(evidence.get("opposing_signals"))
    uncertainties = _strings(evidence.get("uncertainties"))
    lines = [
        f"# 每日 Shadow 权重迭代报告 - {payload.get('date')}",
        "",
        "## 1. 运行摘要",
        "",
        f"- Decision: `{payload.get('decision')}`",
        f"- 决策原因：{payload.get('decision_reason')}",
        f"- Mode: `{payload.get('mode')}`",
        "- Production Effect: `none`",
        "- Manual Review Only: `true`",
        f"- 调整置信度：{_format_float(payload.get('adjustment_confidence', 0))}",
        "",
        "## 2. 输入 Artifacts",
        "",
        "| Artifact | Status | Path |",
        "|---|---:|---|",
    ]
    for key in ("trading_015", "trading_016", "trading_017", "trading_018", "trading_018a"):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            "| "
            f"{artifact.get('label', key)} | "
            f"{artifact.get('status', 'MISSING')} | "
            f"`{artifact.get('path', '')}` |"
        )
    lines.extend(
        [
            "",
            "## 3. Safety Checks",
            "",
            f"- Scheduler dry-run status: `{safety.get('scheduler_dry_run_status', 'MISSING')}`",
            f"- Safety status: `{safety.get('status', 'BLOCK')}`",
            f"- Blocking reasons: {', '.join(_strings(safety.get('blocking_reasons'))) or 'none'}",
            "",
            "## 4. 上一版 Shadow 权重",
            "",
            "| Weight | Value |",
            "|---|---:|",
        ]
    )
    for key, value in previous.items():
        lines.append(f"| {key} | {_format_float(value)} |")
    lines.extend(
        [
            "",
            "## 5. Proposed Changes",
            "",
            "| Weight | Previous | Delta | New |",
            "|---|---:|---:|---:|",
        ]
    )
    for key, previous_value in previous.items():
        delta = _float_value(proposed.get(key), default=0.0)
        lines.append(
            "| "
            f"{key} | {_format_float(previous_value)} | {_format_signed_float(delta)} | "
            f"{_format_float(new_weights.get(key, previous_value))} |"
        )
    lines.extend(["", "## 6. 变化原因", "", "### Supporting Evidence", ""])
    if supporting:
        for item in supporting:
            lines.append(f"- {item.get('reason', '')}")
    else:
        lines.append("- None.")
    lines.extend(["", "### Opposing Evidence", "", _bullet_list(opposing, "None.")])
    lines.extend(["", "### Uncertainty", "", _bullet_list(uncertainties, "None.")])
    lines.extend(
        [
            "",
            "## 7. Production Safety",
            "",
            "本次运行不影响 production。",
            "",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- safe_for_production: `false`",
            "",
            "## 8. 后续复核建议",
            "",
            "- 连续多日运行后，再比较 shadow 与 production 的决策差异。",
            "- TRADING-018C comparison 完成前，不讨论 promotion。",
            "",
        ]
    )
    return "\n".join(lines)


def render_current_shadow_weights_report(payload: dict[str, Any]) -> str:
    weights = _mapping(payload.get("weights"))
    audit = _mapping(payload.get("audit"))
    lines = [
        f"# 当前 Shadow 权重 - {payload.get('last_updated_date')}",
        "",
        f"- mode: `{payload.get('mode')}`",
        "- production_effect: `none`",
        "- manual_review_only: `true`",
        f"- last_decision: `{audit.get('last_decision', '')}`",
        f"- update_count: {audit.get('update_count', 0)}",
        "",
        "| Weight | Value |",
        "|---|---:|",
    ]
    for key, value in weights.items():
        lines.append(f"| {key} | {_format_float(value)} |")
    lines.append("")
    return "\n".join(lines)


def render_shadow_weight_iteration_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Shadow 权重迭代运行日志 - {payload.get('date')}",
            "",
            f"- decision: `{payload.get('decision')}`",
            f"- current_state_updated: `{payload.get('current_state_updated')}`",
            f"- history_written: `{payload.get('history_written')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            f"- candidate_json: `{payload.get('candidate_json')}`",
            f"- current_shadow_weights: `{payload.get('current_shadow_weights')}`",
            "",
        ]
    )


def _decision(
    *,
    missing_artifacts: list[str],
    safety: dict[str, Any],
    daily_summary: dict[str, Any],
    promotion_payload: dict[str, Any],
    confidence: float,
    minimum_confidence: float,
    raw_delta: dict[str, float],
    policy_file_exists: bool,
    force_no_update: bool,
    dry_run: bool,
) -> tuple[str, str]:
    if not policy_file_exists:
        return DECISION_INSUFFICIENT_DATA, "Policy file is missing; shadow update is blocked."
    if missing_artifacts:
        return (
            DECISION_INSUFFICIENT_DATA,
            "Missing required input artifacts: " + ", ".join(missing_artifacts),
        )
    if safety.get("status") != "PASS":
        return DECISION_SAFETY_BLOCKED, "Scheduler dry-run safety checks did not pass."
    if force_no_update:
        return DECISION_NO_UPDATE, "force_no_update was requested."
    if dry_run:
        return DECISION_NO_UPDATE, "dry_run was requested; current shadow state will not change."
    if not _daily_summary_allows_update(daily_summary, promotion_payload):
        return DECISION_NO_UPDATE, "Daily weight adjustment summary did not support an update."
    if confidence < minimum_confidence:
        return (
            DECISION_NO_UPDATE,
            f"Adjustment confidence {confidence:.4f} is below minimum {minimum_confidence:.4f}.",
        )
    if not any(abs(value) > 1e-12 for value in raw_delta.values()):
        return DECISION_NO_UPDATE, "No non-zero proposed weight delta was available."
    return (
        DECISION_UPDATE,
        "Sufficient input artifacts, safety checks passed, and confidence exceeded threshold.",
    )


def _daily_summary_allows_update(
    daily_summary: dict[str, Any],
    promotion_payload: dict[str, Any],
) -> bool:
    status = _string_value(daily_summary.get("status"))
    if status in {"LIMITED", "INSUFFICIENT_DATA", "ERROR"}:
        return False
    summary_blocker = _string_value(daily_summary.get("main_blocked_by"))
    if summary_blocker and summary_blocker not in {"none", "manual_approval_required"}:
        return False
    promotion_summary = _mapping(promotion_payload.get("summary"))
    gate_status = (
        _string_value(daily_summary.get("promotion_gate_status"))
        or _string_value(promotion_summary.get("promotion_gate_status"))
        or _string_value(promotion_payload.get("promotion_gate_status"))
    )
    ready_count = _int_value(daily_summary.get("ready_for_manual_review_count"), default=0)
    if ready_count <= 0:
        ready_count = _int_value(promotion_summary.get("ready_for_manual_review_count"), default=0)
    return gate_status == "READY_FOR_MANUAL_REVIEW" and ready_count > 0


def _scheduler_safety_checks(payload: dict[str, Any]) -> dict[str, Any]:
    blocking: list[str] = []
    safety = _mapping(payload.get("safety_checks"))
    scheduler_status = (
        _string_value(payload.get("scheduler_dry_run_status"))
        or _string_value(safety.get("scheduler_dry_run_status"))
        or _string_value(payload.get("status"))
        or _string_value(safety.get("status"))
        or "MISSING"
    )
    safety_status = _string_value(safety.get("status")) or scheduler_status
    if scheduler_status != "PASS" or safety_status != "PASS":
        blocking.append("scheduler_dry_run_not_pass")
    if _string_value(payload.get("production_effect")) != PRODUCTION_EFFECT_NONE:
        blocking.append("production_effect_not_none")
    if payload.get("manual_review_only") is not True:
        blocking.append("manual_review_only_not_true")
    blocking.extend(_strings(safety.get("blocking_reasons")))
    return {
        "status": "PASS" if not blocking else "BLOCK",
        "scheduler_dry_run_status": scheduler_status,
        "missing_artifacts": [] if payload else ["TRADING-018A"],
        "blocking_reasons": list(dict.fromkeys(blocking)),
        "safe_for_scheduler": not blocking,
    }


def _raw_delta_from_artifacts(
    *,
    previous_weights: dict[str, float],
    daily_summary: dict[str, Any],
    candidate_payload: dict[str, Any],
) -> dict[str, float]:
    explicit = _weights_from_mapping(_mapping(daily_summary.get("proposed_delta")))
    if explicit:
        return {key: explicit.get(key, 0.0) for key in previous_weights}
    summary = _mapping(daily_summary.get("summary"))
    explicit = _weights_from_mapping(_mapping(summary.get("proposed_delta")))
    if explicit:
        return {key: explicit.get(key, 0.0) for key in previous_weights}
    candidate = _selected_weight_candidate(
        candidate_payload,
        _string_value(daily_summary.get("top_candidate_id")),
    )
    deltas: dict[str, float] = {key: 0.0 for key in previous_weights}
    for change in _list_mappings(candidate.get("parameter_changes")):
        key = _parameter_key(change.get("parameter_id"))
        if key in deltas:
            deltas[key] = _float_value(change.get("delta"), default=0.0)
    if any(abs(value) > 1e-12 for value in deltas.values()):
        return deltas
    target_weights = _weights_from_mapping(
        _mapping(_mapping(candidate.get("target_profile")).get("weights"))
    )
    if target_weights:
        return {
            key: target_weights.get(key, previous_weights[key]) - previous_weights[key]
            for key in previous_weights
        }
    return deltas


def _selected_weight_candidate(payload: dict[str, Any], top_candidate_id: str) -> dict[str, Any]:
    candidates = _list_mappings(payload.get("candidates"))
    if top_candidate_id:
        for candidate in candidates:
            if _string_value(candidate.get("candidate_id")) == top_candidate_id:
                return candidate
    return candidates[0] if candidates else {}


def _adjustment_confidence(
    *,
    daily_summary: dict[str, Any],
    evaluation_payload: dict[str, Any],
    promotion_payload: dict[str, Any],
    policy: dict[str, Any],
) -> float:
    for record in (
        daily_summary,
        _mapping(daily_summary.get("summary")),
        _mapping(daily_summary.get("recommendation")),
    ):
        for field in (
            "adjustment_confidence",
            "confidence",
            "minimum_confidence_passed_value",
        ):
            explicit = _optional_float(record.get(field))
            if explicit is not None:
                return explicit
    defaults = _confidence_defaults(policy)
    promotion_summary = _mapping(promotion_payload.get("summary"))
    promotion_status = _string_value(
        promotion_payload.get("promotion_gate_status")
    ) or _string_value(promotion_summary.get("promotion_gate_status"))
    ready_count = _int_value(promotion_summary.get("ready_for_manual_review_count"), default=0)
    if promotion_status == "READY_FOR_MANUAL_REVIEW" and ready_count > 0:
        return defaults["ready_for_manual_review"]
    evaluation_status = _string_value(evaluation_payload.get("evaluation_status"))
    if evaluation_status == "CANDIDATE_PROMISING_BUT_LIMITED":
        return defaults["candidate_promising_but_limited"]
    return 0.0


def _apply_conservative_update(
    *,
    previous_weights: dict[str, float],
    raw_delta: dict[str, float],
    thresholds: dict[str, float],
) -> dict[str, Any]:
    allowed = {
        key: min(
            thresholds["max_abs_delta_per_day"],
            abs(previous_weights[key]) * thresholds["max_relative_delta_per_day"],
        )
        for key in previous_weights
    }
    clamped_fields: list[str] = []
    deltas: dict[str, float] = {}
    for key in previous_weights:
        raw = raw_delta.get(key, 0.0)
        cap = allowed[key]
        clamped = max(-cap, min(cap, raw))
        if abs(raw - clamped) > 1e-12:
            clamped_fields.append(key)
        deltas[key] = clamped
    deltas = _rebalance_delta_sum_to_zero(deltas=deltas, allowed=allowed)
    new_weights = {key: previous_weights[key] + deltas[key] for key in previous_weights}
    new_weights = _normalize_weights(
        new_weights,
        target_total=thresholds["target_total_weight_sum"],
        min_weight=thresholds["min_weight"],
        max_weight=thresholds["max_weight"],
    )
    proposed_delta = {key: new_weights[key] - previous_weights[key] for key in previous_weights}
    return {
        "proposed_delta": proposed_delta,
        "new_shadow_weights": new_weights,
        "constraints_applied": {
            "max_abs_delta_per_day": thresholds["max_abs_delta_per_day"],
            "max_relative_delta_per_day": thresholds["max_relative_delta_per_day"],
            "normalization_applied": True,
            "clamped_fields": clamped_fields,
            "target_total_weight_sum": thresholds["target_total_weight_sum"],
        },
    }


def _rebalance_delta_sum_to_zero(
    *,
    deltas: dict[str, float],
    allowed: dict[str, float],
) -> dict[str, float]:
    adjusted = dict(deltas)
    residual = -sum(adjusted.values())
    for _iteration in range(8):
        if abs(residual) <= 1e-12:
            break
        direction = 1.0 if residual > 0 else -1.0
        capacities: dict[str, float] = {}
        for key, value in adjusted.items():
            capacity = allowed[key] - value if direction > 0 else allowed[key] + value
            if capacity > 1e-12:
                capacities[key] = capacity
        total_capacity = sum(capacities.values())
        if total_capacity <= 1e-12:
            break
        move_total = min(abs(residual), total_capacity)
        for key, capacity in capacities.items():
            adjusted[key] += direction * move_total * capacity / total_capacity
        residual = -sum(adjusted.values())
    return adjusted


def _normalize_weights(
    weights: dict[str, float],
    *,
    target_total: float,
    min_weight: float,
    max_weight: float,
) -> dict[str, float]:
    clamped = {key: max(min_weight, min(max_weight, value)) for key, value in weights.items()}
    total = sum(clamped.values())
    if total <= 0:
        equal = target_total / len(clamped)
        return {key: equal for key in clamped}
    return {key: value * target_total / total for key, value in clamped.items()}


def _default_current_shadow_state(
    *,
    as_of: date,
    production_profile_path: Path,
    policy: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    production_profile = _load_yaml_object(production_profile_path)
    production_weights = _weights_from_mapping(_mapping(production_profile.get("base_weights")))
    if production_weights:
        initialization_source = "production_profile_snapshot"
        weights = _normalize_weights(
            production_weights,
            target_total=1.0,
            min_weight=0.0,
            max_weight=1.0,
        )
    else:
        initialization_source = "conservative_default"
        weights = _conservative_default_weights(policy)
    return {
        "schema_version": SHADOW_WEIGHT_ITERATION_SCHEMA_VERSION,
        "report_type": CURRENT_SHADOW_WEIGHTS_REPORT_TYPE,
        "mode": MODE_SHADOW_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "last_updated_date": as_of.isoformat(),
        "initialization_source": initialization_source,
        "source": {
            "created_by": SHADOW_WEIGHT_ITERATION_TASK_ID,
            "based_on_candidate": "",
            "production_profile_snapshot_path": (
                str(production_profile_path)
                if initialization_source == "production_profile_snapshot"
                else ""
            ),
        },
        "weights": _rounded_weights(weights),
        "constraints": _state_constraints(_thresholds(policy)),
        "audit": {
            "update_count": 0,
            "last_decision": "INITIALIZED",
            "last_reason": f"Initialized from {initialization_source}.",
            "initialized_at": generated_at.isoformat(),
        },
    }


def _updated_current_shadow_state(
    *,
    previous_state: dict[str, Any],
    candidate: dict[str, Any],
    as_of: date,
    generated_at: datetime,
) -> dict[str, Any]:
    audit = _mapping(previous_state.get("audit"))
    outputs = _mapping(candidate.get("outputs"))
    updated = {
        "schema_version": SHADOW_WEIGHT_ITERATION_SCHEMA_VERSION,
        "report_type": CURRENT_SHADOW_WEIGHTS_REPORT_TYPE,
        "mode": MODE_SHADOW_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "last_updated_date": as_of.isoformat(),
        "initialization_source": _string_value(previous_state.get("initialization_source")),
        "source": {
            "created_by": SHADOW_WEIGHT_ITERATION_TASK_ID,
            "based_on_candidate": Path(str(outputs.get("candidate_json", ""))).name,
        },
        "weights": _rounded_weights(_mapping(candidate.get("new_shadow_weights"))),
        "constraints": _state_constraints(_thresholds(_mapping(candidate.get("policy")))),
        "audit": {
            "update_count": _int_value(audit.get("update_count"), default=0) + 1,
            "last_decision": candidate.get("decision"),
            "last_reason": candidate.get("decision_reason"),
            "last_updated_at": generated_at.isoformat(),
        },
    }
    _assert_shadow_safety_invariants(updated)
    return updated


def _run_log_payload(
    *,
    candidate: dict[str, Any],
    current_state_path: Path,
    current_state_updated: bool,
    history_written: bool,
    generated_at: datetime,
) -> dict[str, Any]:
    outputs = _mapping(candidate.get("outputs"))
    return {
        "schema_version": SHADOW_WEIGHT_ITERATION_SCHEMA_VERSION,
        "report_type": "shadow_weight_iteration_run",
        "task_id": SHADOW_WEIGHT_ITERATION_TASK_ID,
        "date": candidate.get("date"),
        "generated_at": generated_at.isoformat(),
        "decision": candidate.get("decision"),
        "decision_reason": candidate.get("decision_reason"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "candidate_json": outputs.get("candidate_json", ""),
        "candidate_markdown": outputs.get("candidate_markdown", ""),
        "current_shadow_weights": str(current_state_path),
        "current_state_updated": current_state_updated,
        "history_written": history_written,
        "safe_for_production": False,
    }


def _resolve_input_artifact_paths(
    *,
    reports_dir: Path,
    as_of: date,
    scheduler_dry_run_path: Path | None,
) -> dict[str, tuple[Path, dict[str, Any]]]:
    suffix = as_of.isoformat()
    artifacts = {
        key: (reports_dir / _string_value(spec["filename"]).format(date=suffix), spec)
        for key, spec in REQUIRED_ARTIFACTS.items()
    }
    scheduler_path = scheduler_dry_run_path or _first_existing_path(
        [reports_dir / pattern.format(date=suffix) for pattern in SCHEDULER_DRY_RUN_FILENAMES]
    )
    artifacts["trading_018a"] = (
        scheduler_path,
        {"label": "TRADING-018A", "report_type": "daily_weight_adjustment_scheduler_dry_run"},
    )
    return artifacts


def _first_existing_path(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _artifact_record(
    path: Path,
    reports_dir: Path,
    *,
    expected_report_type: str = "",
) -> dict[str, Any]:
    payload = _read_json_object(path)
    exists = path.exists()
    report_type = _string_value(payload.get("report_type"))
    valid_report_type = True if not expected_report_type else report_type == expected_report_type
    valid = bool(exists and valid_report_type)
    return {
        "label": _artifact_label(path),
        "status": "FOUND" if valid else "MISSING",
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": exists,
        "valid": valid,
        "report_type": report_type,
        "expected_report_type": expected_report_type,
        "checksum_sha256": _sha256(path) if exists and path.is_file() else "",
    }


def _artifact_label(path: Path) -> str:
    name = path.name
    if name.startswith("weight_adjustment_candidates"):
        return "TRADING-015"
    if name.startswith("weight_candidate_evaluation"):
        return "TRADING-016"
    if name.startswith("weight_promotion_gate"):
        return "TRADING-017"
    if name.startswith("daily_weight_adjustment_summary"):
        return "TRADING-018"
    return "TRADING-018A"


def _evidence(
    *,
    decision: str,
    proposed_delta: dict[str, float],
    raw_delta: dict[str, float],
    daily_summary: dict[str, Any],
    candidate_payload: dict[str, Any],
) -> dict[str, Any]:
    supporting: list[dict[str, str]] = []
    for key, delta in proposed_delta.items():
        if abs(delta) <= 1e-12:
            continue
        direction = "上调" if delta > 0 else "下调"
        supporting.append(
            {
                "weight_key": key,
                "reason": (
                    f"{key} {direction}，因为 daily adjustment candidate 提出了保守的 "
                    "shadow-only 调整。"
                ),
                "source_artifact": Path(
                    str(_mapping(candidate_payload.get("outputs")).get("json", ""))
                    or "weight_adjustment_candidates"
                ).name,
            }
        )
    uncertainties = [
        "单日调整仍属于低样本观察，需要继续累计运行结果。",
        "任何 manual promotion 讨论前，必须先完成 TRADING-018C comparison。",
    ]
    if decision != DECISION_UPDATE:
        uncertainties.append("本次运行未修改 current shadow weights。")
    return {
        "supporting_signals": supporting,
        "opposing_signals": [],
        "uncertainties": uncertainties,
        "daily_summary_status": _string_value(daily_summary.get("status")),
        "raw_delta_available": any(abs(value) > 1e-12 for value in raw_delta.values()),
    }


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    thresholds = _thresholds(policy)
    return {
        "policy_id": _string_value(policy.get("policy_id"))
        or "daily_shadow_weight_iteration_policy",
        "version": policy.get("version", "missing"),
        "status": _string_value(policy.get("status")) or "missing",
        "owner": _string_value(policy.get("owner")) or "missing",
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "path": str(policy_path),
        "policy_file_exists": bool(policy.get("policy_file_exists")),
        "thresholds": thresholds,
        "confidence_defaults": _confidence_defaults(policy),
        "conservative_default_weights": _conservative_default_weights(policy),
    }


def _thresholds(policy: dict[str, Any]) -> dict[str, float]:
    source = _mapping(policy.get("thresholds"))
    return {
        "max_abs_delta_per_day": _float_value(source.get("max_abs_delta_per_day"), default=0.02),
        "max_relative_delta_per_day": _float_value(
            source.get("max_relative_delta_per_day"),
            default=0.05,
        ),
        "minimum_adjustment_confidence": _float_value(
            source.get("minimum_adjustment_confidence"),
            default=0.60,
        ),
        "target_total_weight_sum": _float_value(source.get("target_total_weight_sum"), default=1.0),
        "total_weight_tolerance": _float_value(source.get("total_weight_tolerance"), default=1e-6),
        "min_weight": _float_value(source.get("min_weight"), default=0.0),
        "max_weight": _float_value(source.get("max_weight"), default=1.0),
    }


def _confidence_defaults(policy: dict[str, Any]) -> dict[str, float]:
    source = _mapping(policy.get("confidence_defaults"))
    embedded = _mapping(EMBEDDED_POLICY_DEFAULTS["confidence_defaults"])
    return {
        "ready_for_manual_review": _float_value(
            source.get("ready_for_manual_review"),
            default=_float_value(embedded.get("ready_for_manual_review"), default=0.0),
        ),
        "candidate_promising_but_limited": _float_value(
            source.get("candidate_promising_but_limited"),
            default=_float_value(embedded.get("candidate_promising_but_limited"), default=0.0),
        ),
    }


def _conservative_default_weights(policy: dict[str, Any]) -> dict[str, float]:
    weights = _weights_from_mapping(_mapping(policy.get("conservative_default_weights")))
    if not weights:
        weights = dict(FALLBACK_CONSERVATIVE_DEFAULT_WEIGHTS)
    return _normalize_weights(weights, target_total=1.0, min_weight=0.0, max_weight=1.0)


def _state_constraints(thresholds: dict[str, float]) -> dict[str, Any]:
    return {
        "max_abs_delta_per_day": thresholds["max_abs_delta_per_day"],
        "max_relative_delta_per_day": thresholds["max_relative_delta_per_day"],
        "normalization_required": True,
        "min_weight": thresholds["min_weight"],
        "max_weight": thresholds["max_weight"],
    }


def _output_paths(
    *,
    data_root: Path,
    as_of: date,
    candidate_json: Path,
    candidate_md: Path,
) -> dict[str, str]:
    root = default_shadow_iteration_root(data_root)
    suffix = as_of.isoformat()
    return {
        "candidate_json": str(candidate_json),
        "candidate_markdown": str(candidate_md),
        "current_shadow_weights": str(default_current_shadow_weights_path(data_root)),
        "history_json": str(root / "history" / f"shadow_weights_{suffix}.json"),
        "history_markdown": str(root / "history" / f"shadow_weights_{suffix}.md"),
        "run_log_json": str(root / "logs" / f"shadow_weight_iteration_run_{suffix}.json"),
        "run_log_markdown": str(root / "logs" / f"shadow_weight_iteration_run_{suffix}.md"),
    }


def _assert_shadow_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("shadow weight iteration production_effect must be none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("shadow weight iteration manual_review_only must be true")


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    _write_json(tmp_path, payload)
    tmp_path.replace(path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parameter_key(value: Any) -> str:
    text = _string_value(value)
    return text.rsplit(".", 1)[-1] if text else ""


def _load_policy(path: Path) -> dict[str, Any]:
    payload = _load_yaml_object(path)
    if payload:
        payload["policy_file_exists"] = True
        return payload
    fallback = dict(EMBEDDED_POLICY_DEFAULTS)
    fallback["policy_file_exists"] = False
    return fallback


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
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _rounded_weights(weights: dict[str, Any]) -> dict[str, float]:
    return {key: round(_float_value(value, default=0.0), 10) for key, value in weights.items()}


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: Any, *, default: float) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _int_value(value: Any, *, default: int) -> int:
    try:
        if isinstance(value, bool):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


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
        return path.as_posix()


def _format_float(value: Any) -> str:
    return f"{_float_value(value, default=0.0):.4f}"


def _format_signed_float(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.4f}"


def _bullet_list(values: list[str], empty: str) -> str:
    if not values:
        return f"- {empty}"
    return "\n".join(f"- {value}" for value in values)
