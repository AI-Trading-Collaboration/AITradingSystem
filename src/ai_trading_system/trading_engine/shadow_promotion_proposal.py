from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_promotion_proposal"
RUN_REPORT_TYPE = "shadow_promotion_proposal_run"
TASK_ID = "TRADING-018D"
MODE_MANUAL_PROPOSAL = "manual_promotion_proposal_only"
PRODUCTION_EFFECT_NONE = "none"

DECISION_PROPOSE = "PROPOSE_FOR_MANUAL_REVIEW"
DECISION_CONTINUE = "CONTINUE_OBSERVATION"
DECISION_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
DECISION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_REJECT_SHADOW = "REJECT_SHADOW"
DECISION_ERROR = "ERROR"

REVIEW_SHADOW_LOOKS_BETTER = "SHADOW_LOOKS_BETTER"
REVIEW_SHADOW_LOOKS_WORSE = "SHADOW_LOOKS_WORSE"
REVIEW_SAFETY_BLOCKED = "SAFETY_BLOCKED"
REVIEW_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"

CURRENT_SHADOW_WEIGHTS_REPORT_TYPE = "current_shadow_weights"
SHADOW_ITERATION_REPORT_TYPE = "daily_shadow_weight_iteration"
MULTI_DAY_REVIEW_REPORT_TYPE = "shadow_vs_production_multi_day_review"
COMPARISON_REPORT_TYPE = "daily_shadow_vs_production_comparison"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"
DEFAULT_POLICY_PATH = REPO_ROOT / "config" / "shadow_promotion_proposal_policy.yaml"

EMBEDDED_POLICY_DEFAULTS = {
    "policy_id": "shadow_promotion_proposal_policy",
    "version": "embedded_default",
    "status": "missing_policy_embedded_defaults",
    "owner": "system_review",
    "production_effect": PRODUCTION_EFFECT_NONE,
    "manual_review_only": True,
    "thresholds": {
        "minimum_comparison_days": 5,
        "maximum_insufficient_data_days": 0,
        "maximum_safety_blocked_days": 0,
        "minimum_average_score_delta": 0.0,
        "maximum_shadow_risk_flag_delta_total": 0,
        "maximum_decision_difference_count": 2,
        "target_weight_sum": 1.0,
        "weight_sum_tolerance": 0.000001,
    },
}


def default_promotion_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "promotion"


def default_promotion_proposal_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_promotion_root(data_root)
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )


def default_promotion_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_promotion_root(data_root)
        / "logs"
        / f"shadow_promotion_proposal_run_{as_of.isoformat()}.json"
    )


def default_current_shadow_weights_path(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"


def default_comparison_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "comparison"


def build_shadow_promotion_proposal_payload(
    *,
    as_of: date,
    data_root: Path = REPO_ROOT / "data",
    lookback_days: int = 7,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    policy_path: Path = DEFAULT_POLICY_PATH,
    current_shadow_weights_path: Path | None = None,
    latest_multi_day_review_path: Path | None = None,
    shadow_iteration_candidate_path: Path | None = None,
    shadow_history_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    force_no_proposal: bool = False,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_promotion_proposal_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    current_shadow_weights_path = (
        current_shadow_weights_path or default_current_shadow_weights_path(data_root)
    )
    latest_multi_day_review_path = latest_multi_day_review_path or _latest_multi_day_review_path(
        data_root,
        as_of,
    )
    shadow_iteration_candidate_path = (
        shadow_iteration_candidate_path
        or _latest_shadow_candidate_path(
            data_root,
            as_of,
        )
    )
    shadow_history_path = shadow_history_path or _latest_shadow_history_path(data_root, as_of)

    policy = _load_policy(policy_path)
    thresholds = _policy_thresholds(policy)
    production_profile = _load_yaml_object(production_profile_path)
    current_shadow = _read_json_object(current_shadow_weights_path)
    latest_review = _read_json_object(latest_multi_day_review_path)
    shadow_candidate = _read_json_object(shadow_iteration_candidate_path)
    shadow_history = _read_json_object(shadow_history_path)

    production_weights = _rounded_weights(
        _weights_from_mapping(_mapping(production_profile.get("base_weights")))
    )
    shadow_weights = _rounded_weights(
        _weights_from_mapping(_mapping(current_shadow.get("weights")))
    )
    comparison_artifacts = _comparison_artifact_records(
        data_root=data_root,
        as_of=as_of,
        lookback_days=lookback_days,
    )

    input_artifacts = {
        "current_shadow_weights": _artifact_record(
            current_shadow_weights_path,
            expected_report_type=CURRENT_SHADOW_WEIGHTS_REPORT_TYPE,
            payload=current_shadow,
        ),
        "production_profile_snapshot": _artifact_record(
            production_profile_path,
            payload=production_profile,
            require_non_empty=True,
        ),
        "latest_multi_day_review": _artifact_record(
            latest_multi_day_review_path,
            expected_report_type=MULTI_DAY_REVIEW_REPORT_TYPE,
            payload=latest_review,
        ),
        "latest_shadow_iteration_candidate": _artifact_record(
            shadow_iteration_candidate_path,
            expected_report_type=SHADOW_ITERATION_REPORT_TYPE,
            payload=shadow_candidate,
            optional=True,
        ),
        "latest_shadow_history": _artifact_record(
            shadow_history_path,
            expected_report_type=CURRENT_SHADOW_WEIGHTS_REPORT_TYPE,
            payload=shadow_history,
            optional=True,
        ),
        "policy": _artifact_record(policy_path, payload=policy, require_non_empty=True),
        "comparison_artifacts": comparison_artifacts,
    }
    readiness = _readiness_checks(
        input_artifacts=input_artifacts,
        policy=policy,
        thresholds=thresholds,
        latest_review=latest_review,
        current_shadow=current_shadow,
        production_weights=production_weights,
        shadow_weights=shadow_weights,
        force_no_proposal=force_no_proposal,
    )
    proposal_decision = _proposal_decision(readiness)
    proposal_reason = _proposal_reason(proposal_decision, readiness)
    promotion_proposed = proposal_decision == DECISION_PROPOSE
    weight_compatibility = _weight_key_compatibility(production_weights, shadow_weights)
    proposed_weights = dict(shadow_weights) if weight_compatibility["compatible"] else {}
    proposed_delta = (
        _weight_delta(production_weights, shadow_weights)
        if weight_compatibility["compatible"]
        else {}
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "mode": MODE_MANUAL_PROPOSAL,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "promotion_proposed": promotion_proposed,
        "promotion_executed": False,
        "safe_for_production": False,
        "proposal_decision": proposal_decision,
        "proposal_reason": proposal_reason,
        "force_no_proposal": force_no_proposal,
        "policy": _policy_report(policy, policy_path),
        "input_artifacts": input_artifacts,
        "readiness_checks": readiness,
        "production_weights": production_weights,
        "shadow_weights": shadow_weights,
        "proposed_production_weights": proposed_weights,
        "proposed_delta_from_production": proposed_delta,
        "weight_key_compatibility": {
            "compatible": weight_compatibility["compatible"],
            "missing_in_shadow": weight_compatibility["missing_in_shadow"],
            "missing_in_production": weight_compatibility["missing_in_production"],
        },
        "impact_summary": _impact_summary(latest_review),
        "manual_approval": {
            "required": True,
            "approved": False,
            "approval_file_required": True,
            "approval_file_path": None,
            "approval_instructions": (
                "TRADING-018D only generates a proposal. A future explicit apply task "
                "must require a human-approved file."
            ),
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(default_promotion_run_log_json_path(data_root, as_of)),
            "run_log_markdown": str(
                default_promotion_run_log_json_path(data_root, as_of).with_suffix(".md")
            ),
        },
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_apply": False,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_production_weights": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_proposal.py",
            "created_at": generated.isoformat(),
            "safe_for_scheduler": True,
            "safe_for_production": False,
        },
    }
    _assert_safety_invariants(payload)
    return payload


def write_shadow_promotion_proposal_report(
    *,
    as_of: date,
    data_root: Path = REPO_ROOT / "data",
    lookback_days: int = 7,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    policy_path: Path = DEFAULT_POLICY_PATH,
    current_shadow_weights_path: Path | None = None,
    latest_multi_day_review_path: Path | None = None,
    shadow_iteration_candidate_path: Path | None = None,
    shadow_history_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    force_no_proposal: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=data_root,
        lookback_days=lookback_days,
        production_profile_path=production_profile_path,
        policy_path=policy_path,
        current_shadow_weights_path=current_shadow_weights_path,
        latest_multi_day_review_path=latest_multi_day_review_path,
        shadow_iteration_candidate_path=shadow_iteration_candidate_path,
        shadow_history_path=shadow_history_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated,
        force_no_proposal=force_no_proposal,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    _write_json(json_path, payload)
    _write_text(md_path, render_shadow_promotion_proposal_report(payload))

    run_log = _run_log_payload(payload=payload, generated_at=generated)
    run_log_json_path = Path(str(outputs["run_log_json"]))
    run_log_md_path = Path(str(outputs["run_log_markdown"]))
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_promotion_proposal_run_log(run_log))
    return payload


def render_shadow_promotion_proposal_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    readiness = _mapping(payload.get("readiness_checks"))
    production_weights = _mapping(payload.get("production_weights"))
    shadow_weights = _mapping(payload.get("shadow_weights"))
    delta = _mapping(payload.get("proposed_delta_from_production"))
    impact = _mapping(payload.get("impact_summary"))
    lines = [
        f"# Shadow-to-Production Promotion Proposal - {payload.get('date')}",
        "",
        "## 1. Run Summary",
        "",
        f"- Proposal Decision: `{payload.get('proposal_decision')}`",
        f"- Promotion Proposed: `{payload.get('promotion_proposed')}`",
        "- Promotion Executed: `false`",
        "- Production Effect: `none`",
        "- Manual Review Only: `true`",
        "",
        "## 2. Input Artifacts",
        "",
        "| Artifact | Status | Path |",
        "|---|---:|---|",
    ]
    for key in (
        "current_shadow_weights",
        "production_profile_snapshot",
        "latest_multi_day_review",
        "latest_shadow_iteration_candidate",
        "latest_shadow_history",
        "policy",
    ):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            f"| {key} | `{artifact.get('status', 'MISSING')}` | " f"`{artifact.get('path', '')}` |"
        )
    lines.extend(
        [
            "",
            "## 3. Readiness Checks",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ]
    )
    check_reasons = _mapping(readiness.get("check_reasons"))
    for key in (
        "multi_day_review_status",
        "minimum_history_days_status",
        "score_improvement_status",
        "risk_regression_status",
        "decision_stability_status",
        "weight_key_compatibility_status",
        "shadow_weight_sum_status",
        "safety_boundary_status",
    ):
        lines.append(
            f"| {key} | `{readiness.get(key, 'FAIL')}` | " f"{check_reasons.get(key, '')} |"
        )
    lines.extend(
        [
            "",
            "## 4. Production vs Shadow Weights",
            "",
            "| Weight Key | Production | Shadow | Delta |",
            "|---|---:|---:|---:|",
        ]
    )
    for key in sorted(set(production_weights) | set(shadow_weights)):
        lines.append(
            "| "
            f"{key} | {_format_float(production_weights.get(key))} | "
            f"{_format_float(shadow_weights.get(key))} | "
            f"{_format_signed_float(delta.get(key))} |"
        )
    lines.extend(
        [
            "",
            "## 5. Multi-day Evidence Summary",
            "",
            f"- Review decision: `{impact.get('review_decision', 'MISSING')}`",
            f"- Available comparison days: {impact.get('available_comparison_days', 0)}",
            f"- Average score delta: {_format_signed_float(impact.get('expected_score_delta'))}",
            f"- Decision difference count: {impact.get('decision_difference_count', 0)}",
            f"- Risk flag delta total: {impact.get('risk_flag_delta_total', 0)}",
            (
                "- Dominant changed weight keys: "
                f"{', '.join(_strings(impact.get('dominant_changed_weight_keys'))) or 'none'}"
            ),
            "",
            "## 6. Proposal Result",
            "",
            str(payload.get("proposal_reason") or ""),
            "",
            "## 7. Manual Approval Requirement",
            "",
            "This task does not apply shadow weights to production.",
            "",
            "A future explicit apply task must require:",
            "- human approval",
            "- approval artifact",
            "- final diff confirmation",
            "- rollback plan",
            "",
            "## 8. Safety Statement",
            "",
            "- production_effect = none",
            "- manual_review_only = true",
            "- promotion_executed = false",
            "- safe_for_production = false",
            "",
        ]
    )
    return "\n".join(lines)


def render_shadow_promotion_proposal_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Shadow Promotion Proposal Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- proposal_decision: `{payload.get('proposal_decision')}`",
            f"- promotion_proposed: `{payload.get('promotion_proposed')}`",
            "- promotion_executed: `false`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            f"- proposal_json: `{payload.get('proposal_json')}`",
            f"- proposal_markdown: `{payload.get('proposal_markdown')}`",
            "",
        ]
    )


def _readiness_checks(
    *,
    input_artifacts: dict[str, Any],
    policy: dict[str, Any],
    thresholds: dict[str, float],
    latest_review: dict[str, Any],
    current_shadow: dict[str, Any],
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
    force_no_proposal: bool,
) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    check_reasons: dict[str, str] = {}
    required_inputs = (
        "current_shadow_weights",
        "production_profile_snapshot",
        "latest_multi_day_review",
        "policy",
    )
    missing_required = [
        key
        for key in required_inputs
        if _mapping(input_artifacts.get(key)).get("valid") is not True
    ]
    input_status = "PASS" if not missing_required else "FAIL"
    if missing_required:
        blocking_reasons.append("missing_required_artifacts:" + ",".join(missing_required))
    check_reasons["input_artifacts_status"] = (
        "All required artifacts were found."
        if not missing_required
        else ", ".join(missing_required)
    )

    review_decision = _string_value(latest_review.get("review_decision")) or "MISSING"
    multi_day_review_status = "PASS" if review_decision == REVIEW_SHADOW_LOOKS_BETTER else "FAIL"
    check_reasons["multi_day_review_status"] = f"review_decision={review_decision}"

    available_days = _int_value(latest_review.get("available_comparison_days"), default=0)
    minimum_days = int(thresholds["minimum_comparison_days"])
    history_status = "PASS" if available_days >= minimum_days else "FAIL"
    check_reasons["minimum_history_days_status"] = (
        f"available_comparison_days={available_days}, required={minimum_days}"
    )

    average_delta = _float_value(latest_review.get("average_score_delta"), default=0.0)
    minimum_delta = thresholds["minimum_average_score_delta"]
    score_status = "PASS" if average_delta > minimum_delta else "FAIL"
    check_reasons["score_improvement_status"] = (
        f"average_score_delta={average_delta:.10f}, required>{minimum_delta:.10f}"
    )

    risk_delta = _int_value(latest_review.get("shadow_risk_flag_delta_total"), default=0)
    max_risk_delta = int(thresholds["maximum_shadow_risk_flag_delta_total"])
    risk_status = "PASS" if risk_delta <= max_risk_delta else "FAIL"
    check_reasons["risk_regression_status"] = (
        f"shadow_risk_flag_delta_total={risk_delta}, limit={max_risk_delta}"
    )

    decision_difference_count = _int_value(
        latest_review.get("decision_difference_count"),
        default=0,
    )
    max_decision_difference = int(thresholds["maximum_decision_difference_count"])
    stability_status = "PASS" if decision_difference_count <= max_decision_difference else "FAIL"
    check_reasons["decision_stability_status"] = (
        f"decision_difference_count={decision_difference_count}, limit={max_decision_difference}"
    )

    compatibility = _weight_key_compatibility(production_weights, shadow_weights)
    key_status = "PASS" if compatibility["compatible"] and bool(production_weights) else "FAIL"
    check_reasons["weight_key_compatibility_status"] = (
        "production_weights.keys == shadow_weights.keys"
        if key_status == "PASS"
        else (
            "missing_in_shadow="
            f"{compatibility['missing_in_shadow']}; missing_in_production="
            f"{compatibility['missing_in_production']}"
        )
    )

    shadow_sum = sum(shadow_weights.values())
    target_sum = thresholds["target_weight_sum"]
    tolerance = thresholds["weight_sum_tolerance"]
    sum_status = "PASS" if shadow_weights and abs(shadow_sum - target_sum) <= tolerance else "FAIL"
    check_reasons["shadow_weight_sum_status"] = (
        f"shadow_sum={shadow_sum:.10f}, target={target_sum:.10f}, tolerance={tolerance:.10f}"
    )

    insufficient_days = _int_value(latest_review.get("insufficient_data_days"), default=0)
    safety_days = _int_value(latest_review.get("safety_blocked_days"), default=0)
    max_insufficient = int(thresholds["maximum_insufficient_data_days"])
    max_safety = int(thresholds["maximum_safety_blocked_days"])
    boundary_reasons = _review_safety_reasons(latest_review)
    if insufficient_days > max_insufficient:
        boundary_reasons.append(
            f"insufficient_data_days={insufficient_days} exceeds {max_insufficient}"
        )
    if safety_days > max_safety:
        boundary_reasons.append(f"safety_blocked_days={safety_days} exceeds {max_safety}")
    boundary_reasons.extend(_current_shadow_safety_reasons(current_shadow))
    boundary_reasons.extend(_policy_safety_reasons(policy))
    safety_status = "PASS" if not boundary_reasons else "FAIL"
    check_reasons["safety_boundary_status"] = ", ".join(boundary_reasons) or "No safety blockers."

    if input_status != "PASS":
        blocking_reasons.extend(missing_required)
    if review_decision == REVIEW_SAFETY_BLOCKED:
        blocking_reasons.append("multi_day_review_safety_blocked")
    if insufficient_days > max_insufficient:
        blocking_reasons.append("insufficient_data_days_above_limit")
    if safety_days > max_safety:
        blocking_reasons.append("safety_blocked_days_above_limit")
    if key_status != "PASS":
        blocking_reasons.append("weight_key_mismatch")
    if sum_status != "PASS":
        blocking_reasons.append("shadow_weights_sum_invalid")
    if not bool(policy.get("policy_file_exists")):
        blocking_reasons.append("policy_file_missing")
    if force_no_proposal:
        blocking_reasons.append("force_no_proposal_requested")

    return {
        "input_artifacts_status": input_status,
        "multi_day_review_status": multi_day_review_status,
        "minimum_history_days_status": history_status,
        "score_improvement_status": score_status,
        "risk_regression_status": risk_status,
        "decision_stability_status": stability_status,
        "weight_key_compatibility_status": key_status,
        "shadow_weight_sum_status": sum_status,
        "safety_boundary_status": safety_status,
        "blocking_reasons": _dedupe(blocking_reasons),
        "check_reasons": check_reasons,
        "missing_in_shadow": compatibility["missing_in_shadow"],
        "missing_in_production": compatibility["missing_in_production"],
        "available_comparison_days": available_days,
        "insufficient_data_days": insufficient_days,
        "safety_blocked_days": safety_days,
        "average_score_delta": round(average_delta, 10),
        "shadow_risk_flag_delta_total": risk_delta,
        "decision_difference_count": decision_difference_count,
        "review_decision": review_decision,
    }


def _proposal_decision(readiness: dict[str, Any]) -> str:
    blockers = _strings(readiness.get("blocking_reasons"))
    if readiness["input_artifacts_status"] != "PASS":
        return DECISION_INSUFFICIENT_DATA
    if "insufficient_data_days_above_limit" in blockers:
        return DECISION_INSUFFICIENT_DATA
    if (
        readiness["safety_boundary_status"] != "PASS"
        or readiness["review_decision"] == REVIEW_SAFETY_BLOCKED
    ):
        return DECISION_SAFETY_BLOCKED
    if (
        readiness["minimum_history_days_status"] != "PASS"
        or readiness["review_decision"] == REVIEW_INSUFFICIENT_HISTORY
    ):
        return DECISION_INSUFFICIENT_HISTORY
    if (
        readiness["weight_key_compatibility_status"] != "PASS"
        or readiness["shadow_weight_sum_status"] != "PASS"
    ):
        return DECISION_INSUFFICIENT_DATA
    if (
        readiness["review_decision"] == REVIEW_SHADOW_LOOKS_WORSE
        or readiness["risk_regression_status"] != "PASS"
        or _float_value(readiness.get("average_score_delta"), default=0.0) < 0
    ):
        return DECISION_REJECT_SHADOW
    if "force_no_proposal_requested" in blockers:
        return DECISION_CONTINUE
    if (
        readiness["multi_day_review_status"] == "PASS"
        and readiness["score_improvement_status"] == "PASS"
        and readiness["decision_stability_status"] == "PASS"
    ):
        return DECISION_PROPOSE
    return DECISION_CONTINUE


def _proposal_reason(decision: str, readiness: dict[str, Any]) -> str:
    blockers = _strings(readiness.get("blocking_reasons"))
    if decision == DECISION_PROPOSE:
        return (
            "Shadow review history is strong enough to prepare a manual proposal, "
            "but TRADING-018D does not execute promotion."
        )
    if decision == DECISION_INSUFFICIENT_DATA:
        return "Required input artifacts or weight compatibility checks failed: " + (
            ", ".join(blockers) or "insufficient data"
        )
    if decision == DECISION_INSUFFICIENT_HISTORY:
        return (
            "Shadow comparison history is shorter than the minimum proposal window; "
            "continue observing."
        )
    if decision == DECISION_SAFETY_BLOCKED:
        return "Upstream safety or review blockers prevent a promotion proposal."
    if decision == DECISION_REJECT_SHADOW:
        return "Shadow evidence is worse or risk increased; do not propose promotion."
    if "force_no_proposal_requested" in blockers:
        return "force_no_proposal was requested; continue observation."
    return "Shadow review history is not strong enough for a promotion proposal."


def _impact_summary(latest_review: dict[str, Any]) -> dict[str, Any]:
    return {
        "expected_score_delta": round(
            _float_value(latest_review.get("average_score_delta"), default=0.0),
            10,
        ),
        "review_decision": _string_value(latest_review.get("review_decision")) or "MISSING",
        "available_comparison_days": _int_value(
            latest_review.get("available_comparison_days"),
            default=0,
        ),
        "decision_difference_count": _int_value(
            latest_review.get("decision_difference_count"),
            default=0,
        ),
        "risk_flag_delta_total": _int_value(
            latest_review.get("shadow_risk_flag_delta_total"),
            default=0,
        ),
        "dominant_changed_weight_keys": _strings(latest_review.get("dominant_changed_weight_keys")),
    }


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    run_log = {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": generated_at.isoformat(),
        "run_status": (
            "PASS"
            if payload.get("proposal_decision")
            in {DECISION_PROPOSE, DECISION_CONTINUE, DECISION_INSUFFICIENT_HISTORY}
            else "BLOCKED"
        ),
        "proposal_decision": payload.get("proposal_decision"),
        "promotion_proposed": payload.get("promotion_proposed"),
        "promotion_executed": False,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "safe_for_production": False,
        "proposal_json": outputs.get("json", ""),
        "proposal_markdown": outputs.get("markdown", ""),
        "run_log_json": outputs.get("run_log_json", ""),
        "run_log_markdown": outputs.get("run_log_markdown", ""),
        "pipeline_contract": payload.get("pipeline_contract", {}),
    }
    _assert_safety_invariants(run_log)
    return run_log


def _comparison_artifact_records(
    *,
    data_root: Path,
    as_of: date,
    lookback_days: int,
) -> list[dict[str, Any]]:
    comparison_root = default_comparison_root(data_root)
    records = []
    for day in _lookback_dates(as_of, lookback_days):
        path = comparison_root / f"daily_shadow_vs_production_{day.isoformat()}.json"
        payload = _read_json_object(path)
        record = _artifact_record(
            path,
            expected_report_type=COMPARISON_REPORT_TYPE,
            payload=payload,
            optional=True,
        )
        difference = _mapping(payload.get("difference"))
        record.update(
            {
                "date": day.isoformat(),
                "comparison_status": _string_value(payload.get("comparison_status"))
                or ("MISSING" if not path.exists() else "INVALID"),
                "score_delta": difference.get("score_delta"),
                "decision_changed": difference.get("decision_changed"),
            }
        )
        records.append(record)
    return records


def _artifact_record(
    path: Path,
    *,
    expected_report_type: str | None = None,
    payload: dict[str, Any] | None = None,
    optional: bool = False,
    require_non_empty: bool = False,
) -> dict[str, Any]:
    exists = path.exists()
    payload = payload if payload is not None else _read_json_object(path)
    report_type = _string_value(payload.get("report_type"))
    report_type_valid = True
    if expected_report_type is not None:
        report_type_valid = report_type == expected_report_type
    non_empty_valid = bool(payload) if require_non_empty else True
    valid = bool(exists and report_type_valid and non_empty_valid)
    if optional and not exists:
        valid = True
    if valid and exists:
        status = "FOUND"
    elif optional and not exists:
        status = "OPTIONAL_MISSING"
    elif exists:
        status = "INVALID"
    else:
        status = "MISSING"
    return {
        "status": status,
        "path": str(path),
        "exists": exists,
        "valid": valid,
        "optional": optional,
        "expected_report_type": expected_report_type or "",
        "report_type": report_type,
        "checksum_sha256": _sha256(path) if exists and path.is_file() else "",
        "size_bytes": path.stat().st_size if exists and path.is_file() else 0,
    }


def _review_safety_reasons(review: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if review and review.get("production_effect") != PRODUCTION_EFFECT_NONE:
        reasons.append("latest_review_production_effect_not_none")
    if review and review.get("manual_review_only") is not True:
        reasons.append("latest_review_not_manual_review_only")
    promotion = _mapping(review.get("promotion_readiness"))
    if promotion.get("ready") is True:
        reasons.append("latest_review_promotion_readiness_ready")
    contract = _mapping(review.get("pipeline_contract"))
    for field in (
        "runs_scoring_pipeline",
        "runs_comparison_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "triggers_trade",
    ):
        if contract and contract.get(field) is not False:
            reasons.append(f"latest_review_unsafe_contract:{field}")
    return reasons


def _current_shadow_safety_reasons(current_shadow: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if current_shadow and current_shadow.get("production_effect") != PRODUCTION_EFFECT_NONE:
        reasons.append("current_shadow_production_effect_not_none")
    if current_shadow and current_shadow.get("manual_review_only") is not True:
        reasons.append("current_shadow_not_manual_review_only")
    return reasons


def _policy_safety_reasons(policy: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if policy and policy.get("production_effect") != PRODUCTION_EFFECT_NONE:
        reasons.append("policy_production_effect_not_none")
    if policy and policy.get("manual_review_only") is not True:
        reasons.append("policy_not_manual_review_only")
    return reasons


def _weight_key_compatibility(
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
) -> dict[str, Any]:
    production_keys = set(production_weights)
    shadow_keys = set(shadow_weights)
    return {
        "compatible": bool(production_keys) and production_keys == shadow_keys,
        "missing_in_shadow": sorted(production_keys - shadow_keys),
        "missing_in_production": sorted(shadow_keys - production_keys),
    }


def _weight_delta(
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
) -> dict[str, float]:
    return {
        key: round(shadow_weights[key] - production_weights[key], 10)
        for key in sorted(production_weights)
    }


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "shadow_promotion_proposal_policy",
        "version": _string_value(policy.get("version")) or "missing",
        "status": _string_value(policy.get("status")) or "missing",
        "owner": _string_value(policy.get("owner")) or "missing",
        "path": str(policy_path),
        "policy_file_exists": bool(policy.get("policy_file_exists")),
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "manual_review_only": policy.get("manual_review_only") is True,
        "thresholds": _policy_thresholds(policy),
        "rationale": _string_value(policy.get("rationale")),
        "review_condition": _string_value(policy.get("review_condition")),
    }


def _policy_thresholds(policy: dict[str, Any]) -> dict[str, float]:
    source = _mapping(policy.get("thresholds"))
    defaults = _mapping(EMBEDDED_POLICY_DEFAULTS["thresholds"])
    return {
        "minimum_comparison_days": _float_value(
            source.get("minimum_comparison_days"),
            default=_float_value(defaults.get("minimum_comparison_days"), default=5.0),
        ),
        "maximum_insufficient_data_days": _float_value(
            source.get("maximum_insufficient_data_days"),
            default=0.0,
        ),
        "maximum_safety_blocked_days": _float_value(
            source.get("maximum_safety_blocked_days"),
            default=0.0,
        ),
        "minimum_average_score_delta": _float_value(
            source.get("minimum_average_score_delta"),
            default=0.0,
        ),
        "maximum_shadow_risk_flag_delta_total": _float_value(
            source.get("maximum_shadow_risk_flag_delta_total"),
            default=0.0,
        ),
        "maximum_decision_difference_count": _float_value(
            source.get("maximum_decision_difference_count"),
            default=2.0,
        ),
        "target_weight_sum": _float_value(source.get("target_weight_sum"), default=1.0),
        "weight_sum_tolerance": _float_value(source.get("weight_sum_tolerance"), default=0.000001),
    }


def _load_policy(path: Path) -> dict[str, Any]:
    payload = _load_yaml_object(path)
    if payload:
        payload["policy_file_exists"] = True
        return payload
    fallback = dict(EMBEDDED_POLICY_DEFAULTS)
    fallback["policy_file_exists"] = False
    return fallback


def _latest_multi_day_review_path(data_root: Path, as_of: date) -> Path:
    review_root = default_comparison_root(data_root) / "reviews"
    default_path = review_root / f"shadow_vs_production_review_{as_of.isoformat()}.json"
    return _latest_dated_path(
        root=review_root,
        prefix="shadow_vs_production_review_",
        as_of=as_of,
        default_path=default_path,
    )


def _latest_shadow_candidate_path(data_root: Path, as_of: date) -> Path:
    candidate_root = data_root / "derived" / "weight_iterations" / "shadow" / "candidates"
    default_path = candidate_root / f"shadow_weight_candidate_{as_of.isoformat()}.json"
    return _latest_dated_path(
        root=candidate_root,
        prefix="shadow_weight_candidate_",
        as_of=as_of,
        default_path=default_path,
    )


def _latest_shadow_history_path(data_root: Path, as_of: date) -> Path:
    history_root = data_root / "derived" / "weight_iterations" / "shadow" / "history"
    default_path = history_root / f"shadow_weights_{as_of.isoformat()}.json"
    return _latest_dated_path(
        root=history_root,
        prefix="shadow_weights_",
        as_of=as_of,
        default_path=default_path,
    )


def _latest_dated_path(*, root: Path, prefix: str, as_of: date, default_path: Path) -> Path:
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        parsed = _parse_iso_date(path.stem.removeprefix(prefix))
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _lookback_dates(as_of: date, lookback_days: int) -> list[date]:
    return [as_of - timedelta(days=offset) for offset in range(lookback_days - 1, -1, -1)]


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("shadow promotion proposal production_effect must be none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("shadow promotion proposal manual_review_only must be true")
    if payload.get("promotion_executed") is not False:
        raise ValueError("TRADING-018D cannot execute promotion")
    if payload.get("safe_for_production") is not False:
        raise ValueError("TRADING-018D must not mark output safe_for_production")
    contract = _mapping(payload.get("pipeline_contract"))
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_apply",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if field in contract and contract.get(field) is not False:
            raise ValueError(f"unsafe shadow promotion proposal contract field: {field}")


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _rounded_weights(weights: dict[str, Any]) -> dict[str, float]:
    return {key: round(_float_value(value, default=0.0), 10) for key, value in weights.items()}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_float(value: Any) -> str:
    parsed = _optional_float(value)
    return "NA" if parsed is None else f"{parsed:.4f}"


def _format_signed_float(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NA"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.4f}"
