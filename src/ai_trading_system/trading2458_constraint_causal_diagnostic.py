from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from hashlib import sha256
from itertools import combinations
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts.writer import (
    canonical_json_bytes,
    write_bytes_atomic,
    write_json_atomic,
)
from ai_trading_system.trading2453_constraint_hit_diagnosis import (
    DEFAULT_PACKAGE_ROOT,
    DEFAULT_RUN_DIR,
    Trading2453ConstraintDiagnosisError,
    build_trading2453_diagnosis,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "trading2458_constraint_causal_diagnostic.v1"
VALIDATION_SCHEMA_VERSION = "trading2458_constraint_causal_diagnostic_validation.v1"
POLICY_SCHEMA_VERSION = "trading2458_constraint_causal_diagnostic_policy.v1"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "trading2458_constraint_causal_diagnostic.yaml"
)

TARGET_AXES = (
    "constraint_buffer_bps",
    "drawdown_guard",
    "rebalance_cooldown_days",
    "rescue_intensity",
    "risk_off_confirmation_days",
    "smooth_window_days",
    "turnover_penalty",
)
EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2458:" "2026-07-25:approve_narrow_constraint_causal_diagnostic_v1"
)
SAFETY: dict[str, Any] = {
    "research_only": True,
    "manual_review_required": True,
    "current_package_reopened": False,
    "strategy_gate_changed": False,
    "candidate_universe_changed": False,
    "candidate_search_executed": False,
    "prospective_accessed": False,
    "paper_shadow_changed": False,
    "production_effect": "none",
    "broker_action": "none",
}
OUTPUT_FILENAMES = (
    "matched_contrasts.jsonl",
    "axis_diagnostic.json",
    "owner_decision_pack.json",
    "owner_decision_pack.md",
)


class Trading2458ConstraintCausalDiagnosticError(ValueError):
    """Raised when frozen evidence cannot support the governed diagnostic."""


def load_trading2458_policy(path: Path = DEFAULT_POLICY_PATH) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise Trading2458ConstraintCausalDiagnosticError("mapping policy required")
    policy = dict(payload)
    _require_exact_keys(
        policy,
        {
            "schema_version",
            "policy_id",
            "version",
            "status",
            "owner",
            "owner_decision",
            "rationale",
            "intended_effect",
            "validation_evidence",
            "review_condition",
            "expiry_condition",
            "research_window",
            "matched_contrast",
            "interpretation",
            "safety",
        },
        "policy",
    )
    expected_identity = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": "trading2458_constraint_causal_diagnostic",
        "version": "1.0.0",
        "status": "REVIEWED_NARROW_DIAGNOSTIC",
        "owner": "strategy_research_owner",
        "owner_decision": EXPECTED_OWNER_DECISION,
    }
    for field, expected in expected_identity.items():
        if policy.get(field) != expected:
            raise Trading2458ConstraintCausalDiagnosticError(
                f"unexpected policy {field}: {policy.get(field)!r}"
            )
    for field in (
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
    ):
        if not isinstance(policy[field], str) or not policy[field].strip():
            raise Trading2458ConstraintCausalDiagnosticError(f"non-empty {field} required")

    research_window = _mapping(policy["research_window"], "research_window")
    _require_exact_keys(
        research_window,
        {
            "active_primary_start",
            "historical_seen_end",
            "prospective_untouched_start",
        },
        "research_window",
    )
    if research_window != {
        "active_primary_start": "2021-02-22",
        "historical_seen_end": "2025-12-31",
        "prospective_untouched_start": "2026-07-22",
    }:
        raise Trading2458ConstraintCausalDiagnosticError("research window role drift")

    contrast = _mapping(policy["matched_contrast"], "matched_contrast")
    _require_exact_keys(
        contrast,
        {
            "unit",
            "target_axes",
            "required_fold_count",
            "minimum_pairs_per_fold",
            "minimum_nonzero_pairs_per_fold",
            "required_nonzero_direction_consistency",
            "zero_effect_classification",
            "no_pair_classification",
            "incomplete_fold_classification",
            "mixed_direction_classification",
            "identifiable_classification",
        },
        "matched_contrast",
    )
    if contrast["unit"] != "SAME_FOLD_SAME_SELECTED_TEMPLATE_ALL_OTHER_AXES_EQUAL":
        raise Trading2458ConstraintCausalDiagnosticError("matched contrast unit drift")
    if tuple(_strings(contrast["target_axes"], "target_axes")) != TARGET_AXES:
        raise Trading2458ConstraintCausalDiagnosticError("target axis set/order drift")
    _positive_int(contrast["required_fold_count"], "required_fold_count")
    _positive_int(contrast["minimum_pairs_per_fold"], "minimum_pairs_per_fold")
    _positive_int(
        contrast["minimum_nonzero_pairs_per_fold"],
        "minimum_nonzero_pairs_per_fold",
    )
    consistency = _number(
        contrast["required_nonzero_direction_consistency"],
        "required_nonzero_direction_consistency",
    )
    if not 0 < consistency <= 1:
        raise Trading2458ConstraintCausalDiagnosticError(
            "required_nonzero_direction_consistency must be in (0, 1]"
        )
    for field in (
        "zero_effect_classification",
        "no_pair_classification",
        "incomplete_fold_classification",
        "mixed_direction_classification",
        "identifiable_classification",
    ):
        if not isinstance(contrast[field], str) or not contrast[field]:
            raise Trading2458ConstraintCausalDiagnosticError(f"{field} required")

    interpretation = _mapping(policy["interpretation"], "interpretation")
    _require_exact_keys(
        interpretation,
        {
            "all_axes_common_mode_recommendation",
            "identifiable_axis_recommendation",
            "insufficient_identifiability_recommendation",
            "role_correct_gate_policy_option",
        },
        "interpretation",
    )
    if interpretation != {
        "all_axes_common_mode_recommendation": "RETIRE_CURRENT_FAMILY",
        "identifiable_axis_recommendation": "AUTHOR_NEW_HYPOTHESIS_GENERATOR",
        "insufficient_identifiability_recommendation": "INSUFFICIENT_IDENTIFIABILITY",
        "role_correct_gate_policy_option": "AUTHOR_ROLE_CORRECT_GATE_POLICY",
    }:
        raise Trading2458ConstraintCausalDiagnosticError("interpretation action set drift")
    if _mapping(policy["safety"], "safety") != SAFETY:
        raise Trading2458ConstraintCausalDiagnosticError("policy safety drift")
    return policy


def build_trading2458_diagnostic(
    *,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    policy = load_trading2458_policy(policy_path)
    try:
        prior = build_trading2453_diagnosis(
            run_dir=run_dir,
            package_root=package_root,
        )
    except Trading2453ConstraintDiagnosisError as exc:
        raise Trading2458ConstraintCausalDiagnosticError(str(exc)) from exc
    if prior["manifest"].get("status") != "PASS":
        raise Trading2458ConstraintCausalDiagnosticError("TRADING-2453 replay is not PASS")

    rows = [dict(row) for row in prior["recomputations"]]
    structure_checks = _structure_checks(rows)
    contrasts = build_matched_contrasts(rows=rows, policy=policy)
    axis_summaries = build_axis_summaries(contrasts=contrasts, policy=policy)
    template_summaries = _build_axis_template_summaries(contrasts)
    status = (
        "PASS"
        if all(structure_checks.values()) and all(row.get("status") == "PASS" for row in rows)
        else "FAIL"
    )
    conclusion = _build_conclusion(axis_summaries=axis_summaries, policy=policy)
    source_contract = {
        "trading2453_diagnosis_id": prior["manifest"]["diagnosis_id"],
        "trading2453_manifest_sha256": _canonical_sha256(prior["manifest"]),
        "trading2453_source_contract": prior["manifest"]["source_contract"],
        "trading2453_recomputation_count": len(rows),
        "trading2453_exact_match_count": sum(row.get("status") == "PASS" for row in rows),
        "trading2452_result_status": "INCOMPLETE_NO_ELIGIBLE_CANDIDATE",
        "trading2452_package_remains_closed": True,
        "policy_path": policy_path.relative_to(PROJECT_ROOT).as_posix(),
        "policy_sha256": sha256(policy_path.read_bytes()).hexdigest(),
        "owner_decision": EXPECTED_OWNER_DECISION,
    }
    diagnostic_core = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2458_constraint_causal_axis_diagnostic",
        "status": status,
        "policy": _policy_projection(policy),
        "source_contract": source_contract,
        "structure_checks": structure_checks,
        "matched_contrast_summary": {
            "pair_count": len(contrasts),
            "axis_count": len(TARGET_AXES),
            "fold_count": len({row["window_index"] for row in rows}),
            "selected_template_count": len({row.get("candidate_template") for row in rows}),
            "effect_direction_counts": _counter_records(
                row["effect"]["constraint_hit_rate_direction"] for row in contrasts
            ),
            "gate_reason_change_count": sum(
                row["effect"]["gate_reasons_changed"] for row in contrasts
            ),
        },
        "axis_summaries": axis_summaries,
        "axis_template_summaries": template_summaries,
        "conclusion": conclusion,
        "interpretation_limitations": [
            "matched contrast is an observational association, not proven investment causality",
            "selected template is an observed best-template label and may itself "
            "depend on the candidate",
            "zero within-pair rate movement can reflect common-mode saturation or "
            "insufficient metric sensitivity",
            "the closed TRADING-2452 package cannot be rerun or reinterpreted as eligible",
        ],
        "safety": dict(SAFETY),
        **SAFETY,
    }
    diagnostic_id = "trading2458-constraint-causal_" + _stable_hash(diagnostic_core, contrasts)[:20]
    diagnostic = {"diagnostic_id": diagnostic_id, **diagnostic_core}
    owner_pack = _build_owner_pack(
        diagnostic_id=diagnostic_id,
        diagnostic=diagnostic,
        policy=policy,
    )
    output_bytes = {
        "matched_contrasts.jsonl": _jsonl_bytes(contrasts),
        "axis_diagnostic.json": canonical_json_bytes(diagnostic),
        "owner_decision_pack.json": canonical_json_bytes(owner_pack),
        "owner_decision_pack.md": _render_owner_pack(owner_pack).encode("utf-8"),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2458_constraint_causal_diagnostic_manifest",
        "diagnostic_id": diagnostic_id,
        "status": status,
        "policy_binding": diagnostic["policy"],
        "source_contract": source_contract,
        "output_artifact_checksums": {
            name: sha256(content).hexdigest() for name, content in sorted(output_bytes.items())
        },
        "output_artifact_sizes": {
            name: len(content) for name, content in sorted(output_bytes.items())
        },
        "completed_stages": ["S0", "S1", "S2", "S3"],
        "original_package_reopened": False,
        "threshold_or_gate_modified": False,
        "candidate_or_search_space_modified": False,
        "prospective_holdout_accessed": False,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    return {
        "contrasts": contrasts,
        "diagnostic": diagnostic,
        "owner_pack": owner_pack,
        "manifest": manifest,
        "bytes": output_bytes,
    }


def build_matched_contrasts(
    *,
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    contrast_policy = _mapping(policy["matched_contrast"], "matched_contrast")
    axes = tuple(_strings(contrast_policy["target_axes"], "target_axes"))
    output: list[dict[str, Any]] = []
    for axis in axes:
        other_axes = tuple(candidate for candidate in axes if candidate != axis)
        grouped: defaultdict[tuple[object, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            parameters = _mapping(row.get("parameters"), "parameters")
            if set(parameters) != set(axes):
                raise Trading2458ConstraintCausalDiagnosticError(
                    f"candidate parameter axis drift: {row.get('candidate_id')}"
                )
            key = (
                row.get("window_index"),
                row.get("candidate_template"),
                *(_canonical_scalar(parameters[name]) for name in other_axes),
            )
            grouped[key].append(row)
        for key in sorted(grouped, key=_tuple_sort_key):
            group = sorted(
                grouped[key],
                key=lambda row: (
                    _scalar_sort_key(_mapping(row.get("parameters"), "parameters").get(axis)),
                    str(row.get("candidate_id")),
                ),
            )
            for left, right in combinations(group, 2):
                left_parameters = _mapping(left.get("parameters"), "parameters")
                right_parameters = _mapping(right.get("parameters"), "parameters")
                if left_parameters[axis] == right_parameters[axis]:
                    continue
                output.append(
                    _build_pair(
                        axis=axis,
                        other_axes=other_axes,
                        left=left,
                        right=right,
                    )
                )
    return sorted(
        output,
        key=lambda row: (
            str(row["axis"]),
            int(row["window_index"]),
            str(row["candidate_template"]),
            str(row["pair_id"]),
        ),
    )


def build_axis_summaries(
    *,
    contrasts: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    contrast_policy = _mapping(policy["matched_contrast"], "matched_contrast")
    axes = tuple(_strings(contrast_policy["target_axes"], "target_axes"))
    required_fold_count = _positive_int(
        contrast_policy["required_fold_count"],
        "required_fold_count",
    )
    minimum_pairs = _positive_int(
        contrast_policy["minimum_pairs_per_fold"],
        "minimum_pairs_per_fold",
    )
    minimum_nonzero = _positive_int(
        contrast_policy["minimum_nonzero_pairs_per_fold"],
        "minimum_nonzero_pairs_per_fold",
    )
    required_consistency = _number(
        contrast_policy["required_nonzero_direction_consistency"],
        "required_nonzero_direction_consistency",
    )
    summaries: list[dict[str, Any]] = []
    for axis in axes:
        axis_rows = [row for row in contrasts if row.get("axis") == axis]
        directions = [
            _mapping(row.get("effect"), "effect").get("constraint_hit_rate_direction")
            for row in axis_rows
        ]
        nonzero = [value for value in directions if value in {"POSITIVE", "NEGATIVE"}]
        fold_rows = []
        for fold in range(1, required_fold_count + 1):
            selected = [row for row in axis_rows if row.get("window_index") == fold]
            selected_nonzero = [
                row
                for row in selected
                if _mapping(row.get("effect"), "effect").get("constraint_hit_rate_direction")
                in {"POSITIVE", "NEGATIVE"}
            ]
            fold_rows.append(
                {
                    "window_index": fold,
                    "pair_count": len(selected),
                    "nonzero_pair_count": len(selected_nonzero),
                    "minimum_pair_coverage_met": len(selected) >= minimum_pairs,
                    "minimum_nonzero_coverage_met": len(selected_nonzero) >= minimum_nonzero,
                }
            )
        covered_fold_count = sum(row["minimum_pair_coverage_met"] for row in fold_rows)
        nonzero_covered_fold_count = sum(row["minimum_nonzero_coverage_met"] for row in fold_rows)
        dominant_direction = None
        direction_consistency = None
        if nonzero:
            counts = Counter(str(value) for value in nonzero)
            dominant_direction, dominant_count = sorted(
                counts.items(), key=lambda item: (-item[1], item[0])
            )[0]
            direction_consistency = dominant_count / len(nonzero)
        if not axis_rows:
            classification = contrast_policy["no_pair_classification"]
        elif covered_fold_count < required_fold_count:
            classification = contrast_policy["incomplete_fold_classification"]
        elif not nonzero:
            classification = contrast_policy["zero_effect_classification"]
        elif nonzero_covered_fold_count < required_fold_count:
            classification = contrast_policy["incomplete_fold_classification"]
        elif direction_consistency is None or direction_consistency < required_consistency:
            classification = contrast_policy["mixed_direction_classification"]
        else:
            classification = contrast_policy["identifiable_classification"]
        summaries.append(
            {
                "axis": axis,
                "classification": classification,
                "pair_count": len(axis_rows),
                "fold_coverage": fold_rows,
                "covered_fold_count": covered_fold_count,
                "nonzero_covered_fold_count": nonzero_covered_fold_count,
                "direction_counts": _counter_records(directions),
                "dominant_nonzero_direction": dominant_direction,
                "nonzero_direction_consistency": direction_consistency,
                "constraint_hit_rate_delta": _numeric_distribution(
                    [
                        _mapping(row.get("effect"), "effect").get("constraint_hit_rate_delta")
                        for row in axis_rows
                    ]
                ),
                "constraint_hits_delta": _numeric_distribution(
                    [
                        _mapping(row.get("effect"), "effect").get("constraint_hits_delta")
                        for row in axis_rows
                    ]
                ),
                "gate_reason_change_count": sum(
                    _mapping(row.get("effect"), "effect").get("gate_reasons_changed") is True
                    for row in axis_rows
                ),
                "interpretation": (
                    "exact within-template matched association only; "
                    "not proven investment causality"
                ),
            }
        )
    return summaries


def write_trading2458_diagnostic(
    *,
    output_dir: Path,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise Trading2458ConstraintCausalDiagnosticError(
            f"output directory must be absent or empty: {output_dir}"
        )
    bundle = build_trading2458_diagnostic(
        run_dir=run_dir,
        package_root=package_root,
        policy_path=policy_path,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in bundle["bytes"].items():
        write_bytes_atomic(output_dir / filename, content)
    write_json_atomic(output_dir / "diagnostic_manifest.json", bundle["manifest"])
    validation = validate_trading2458_diagnostic(
        output_dir=output_dir,
        run_dir=run_dir,
        package_root=package_root,
        policy_path=policy_path,
    )
    write_json_atomic(output_dir / "diagnostic_validation.json", validation)
    if validation["status"] != "PASS":
        raise Trading2458ConstraintCausalDiagnosticError(
            f"diagnostic validation failed: {validation['failed_check_count']}"
        )
    return {
        "status": bundle["manifest"]["status"],
        "diagnostic_id": bundle["manifest"]["diagnostic_id"],
        "output_dir": output_dir,
        "manifest": bundle["manifest"],
        "validation": validation,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_trading2458_diagnostic(
    *,
    output_dir: Path,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        expected = build_trading2458_diagnostic(
            run_dir=run_dir,
            package_root=package_root,
            policy_path=policy_path,
        )
        manifest = _load_json(output_dir / "diagnostic_manifest.json")
        base_inventory = {*OUTPUT_FILENAMES, "diagnostic_manifest.json"}
        actual_inventory = {path.name for path in output_dir.iterdir()}
        checks.append(
            _check(
                "output_inventory_exact",
                actual_inventory
                in (base_inventory, {*base_inventory, "diagnostic_validation.json"}),
            )
        )
        checks.append(_check("manifest_content_derived", manifest == expected["manifest"]))
        for filename in OUTPUT_FILENAMES:
            path = output_dir / filename
            checks.append(
                _check(
                    f"output_content_derived:{filename}",
                    path.is_file() and path.read_bytes() == expected["bytes"][filename],
                )
            )
        checks.extend(
            [
                _check("diagnostic_status_pass", manifest.get("status") == "PASS"),
                _check(
                    "all_stages_complete",
                    manifest.get("completed_stages") == ["S0", "S1", "S2", "S3"],
                ),
                _check("safety_exact", manifest.get("safety") == SAFETY),
                _check(
                    "current_package_remains_closed",
                    manifest.get("original_package_reopened") is False,
                ),
                _check(
                    "threshold_and_gate_unchanged",
                    manifest.get("threshold_or_gate_modified") is False,
                ),
                _check(
                    "candidate_and_search_unchanged",
                    manifest.get("candidate_or_search_space_modified") is False,
                ),
                _check(
                    "prospective_not_accessed",
                    manifest.get("prospective_holdout_accessed") is False,
                ),
            ]
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        checks.append(_check("content_derived_validation", False, str(exc)))
    status = "PASS" if checks and all(row["passed"] for row in checks) else "FAIL"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "report_type": "trading2458_constraint_causal_diagnostic_validation",
        "status": status,
        "failed_check_count": sum(not row["passed"] for row in checks),
        "checks": checks,
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _build_pair(
    *,
    axis: str,
    other_axes: Sequence[str],
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> dict[str, Any]:
    left_parameters = _mapping(left.get("parameters"), "parameters")
    right_parameters = _mapping(right.get("parameters"), "parameters")
    if any(left_parameters[name] != right_parameters[name] for name in other_axes):
        raise Trading2458ConstraintCausalDiagnosticError("invalid matched pair")
    left_artifact = _mapping(left.get("artifact"), "artifact")
    right_artifact = _mapping(right.get("artifact"), "artifact")
    rate_delta = _subtract_optional(
        right_artifact.get("constraint_hit_rate"),
        left_artifact.get("constraint_hit_rate"),
    )
    pair_core = {
        "schema_version": SCHEMA_VERSION,
        "axis": axis,
        "window_index": left.get("window_index"),
        "candidate_template": left.get("candidate_template"),
        "matched_other_axes": {name: left_parameters[name] for name in sorted(other_axes)},
        "left": {
            "candidate_id": left.get("candidate_id"),
            "axis_value": left_parameters[axis],
            "constraint_hit_rate": left_artifact.get("constraint_hit_rate"),
            "constraint_hits": left_artifact.get("constraint_hits"),
            "constraint_hits_delta_vs_reference": left_artifact.get(
                "constraint_hits_delta_vs_reference"
            ),
            "gate_reasons": list(left_artifact.get("gate_reasons") or []),
        },
        "right": {
            "candidate_id": right.get("candidate_id"),
            "axis_value": right_parameters[axis],
            "constraint_hit_rate": right_artifact.get("constraint_hit_rate"),
            "constraint_hits": right_artifact.get("constraint_hits"),
            "constraint_hits_delta_vs_reference": right_artifact.get(
                "constraint_hits_delta_vs_reference"
            ),
            "gate_reasons": list(right_artifact.get("gate_reasons") or []),
        },
        "effect": {
            "constraint_hit_rate_delta": rate_delta,
            "constraint_hit_rate_direction": _direction(rate_delta),
            "constraint_hits_delta": _subtract_optional(
                right_artifact.get("constraint_hits"),
                left_artifact.get("constraint_hits"),
            ),
            "constraint_delta_metric_delta": _subtract_optional(
                right_artifact.get("constraint_hits_delta_vs_reference"),
                left_artifact.get("constraint_hits_delta_vs_reference"),
            ),
            "gate_reasons_changed": list(left_artifact.get("gate_reasons") or [])
            != list(right_artifact.get("gate_reasons") or []),
        },
        "interpretation": "MATCHED_ASSOCIATION_NOT_PROVEN_CAUSALITY",
        "safety": dict(SAFETY),
    }
    return {"pair_id": "pair_" + _stable_hash(pair_core)[:20], **pair_core}


def _build_axis_template_summaries(
    contrasts: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in contrasts:
        groups[(str(row.get("axis")), str(row.get("candidate_template")))].append(row)
    output = []
    for (axis, template), rows in sorted(groups.items()):
        output.append(
            {
                "axis": axis,
                "candidate_template": template,
                "pair_count": len(rows),
                "fold_counts": [
                    {
                        "window_index": fold,
                        "pair_count": sum(row.get("window_index") == fold for row in rows),
                    }
                    for fold in range(1, 7)
                ],
                "direction_counts": _counter_records(
                    _mapping(row.get("effect"), "effect").get("constraint_hit_rate_direction")
                    for row in rows
                ),
                "constraint_hit_rate_delta": _numeric_distribution(
                    [
                        _mapping(row.get("effect"), "effect").get("constraint_hit_rate_delta")
                        for row in rows
                    ]
                ),
            }
        )
    return output


def _build_conclusion(
    *,
    axis_summaries: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    contrast = _mapping(policy["matched_contrast"], "matched_contrast")
    interpretation = _mapping(policy["interpretation"], "interpretation")
    classifications = [str(row.get("classification")) for row in axis_summaries]
    if classifications and all(
        value == contrast["zero_effect_classification"] for value in classifications
    ):
        family_classification = "CURRENT_FAMILY_COMMON_MODE_NO_CONSTRAINT_DISCRIMINATION"
        recommendation = interpretation["all_axes_common_mode_recommendation"]
    elif contrast["identifiable_classification"] in classifications:
        family_classification = "AT_LEAST_ONE_AXIS_IDENTIFIABLE_ASSOCIATION"
        recommendation = interpretation["identifiable_axis_recommendation"]
    else:
        family_classification = "CURRENT_GRID_INSUFFICIENT_IDENTIFIABILITY"
        recommendation = interpretation["insufficient_identifiability_recommendation"]
    return {
        "classification": family_classification,
        "recommended_owner_action": recommendation,
        "role_correct_gate_policy_option": interpretation["role_correct_gate_policy_option"],
        "threshold_change_supported": False,
        "same_package_rerun_supported": False,
        "candidate_expansion_executed": False,
        "causal_claim_supported": False,
    }


def _build_owner_pack(
    *,
    diagnostic_id: str,
    diagnostic: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    conclusion = _mapping(diagnostic.get("conclusion"), "conclusion")
    recommendation = str(conclusion.get("recommended_owner_action"))
    option_ids = (
        "RETIRE_CURRENT_FAMILY",
        "AUTHOR_NEW_HYPOTHESIS_GENERATOR",
        "AUTHOR_ROLE_CORRECT_GATE_POLICY",
        "INSUFFICIENT_IDENTIFIABILITY",
    )
    labels = {
        "RETIRE_CURRENT_FAMILY": "关闭当前 candidate family/generator 的约束辨识路径",
        "AUTHOR_NEW_HYPOTHESIS_GENERATOR": "另建可证伪 hypothesis 与 candidate generator",
        "AUTHOR_ROLE_CORRECT_GATE_POLICY": "另建 role-correct hard-gate policy 与新 package",
        "INSUFFICIENT_IDENTIFIABILITY": "保留负面结论并停止推断",
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2458_constraint_causal_owner_decision_pack",
        "diagnostic_id": diagnostic_id,
        "status": "READY_FOR_OWNER_REVIEW",
        "owner_decision_source": EXPECTED_OWNER_DECISION,
        "recommended_option_id": recommendation,
        "options": [
            {
                "option_id": option_id,
                "label": labels[option_id],
                "recommended": option_id == recommendation,
                "new_authorization_required": option_id
                in {
                    "AUTHOR_NEW_HYPOTHESIS_GENERATOR",
                    "AUTHOR_ROLE_CORRECT_GATE_POLICY",
                },
                "same_package_rerun_allowed": False,
                "prospective_access_allowed": False,
                "automatic_execution_allowed": False,
            }
            for option_id in option_ids
        ],
        "evidence": {
            "matched_contrast_summary": diagnostic["matched_contrast_summary"],
            "axis_summaries": diagnostic["axis_summaries"],
            "conclusion": conclusion,
        },
        "policy_version": policy["version"],
        "prohibited_actions": [
            "修改 max_constraint_hit_rate 或其他 gate threshold",
            "在已关闭 TRADING-2452 package 内重跑",
            "扩展 candidate/search space 或补造 matched pair",
            "把 matched association 声称为已证明投资因果",
            "访问 prospective holdout",
            "进入 paper-shadow、promotion、production 或 broker/order",
        ],
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _render_owner_pack(pack: Mapping[str, Any]) -> str:
    evidence = _mapping(pack.get("evidence"), "evidence")
    conclusion = _mapping(evidence.get("conclusion"), "conclusion")
    lines = [
        "# TRADING-2458 冻结 Constraint 证据窄版因果诊断",
        "",
        f"- 诊断 ID：`{pack.get('diagnostic_id')}`",
        f"- 状态：`{pack.get('status')}`",
        f"- 推荐选项：`{pack.get('recommended_option_id')}`",
        f"- 总分类：`{conclusion.get('classification')}`",
        "- 因果声明：不支持；仅为冻结设计矩阵内的 matched association。",
        "- 原 package：保持关闭，不重跑、不改 gate。",
        "- Prospective：未访问。",
        "- Production effect：`none`；broker action：`none`。",
        "",
        "## Axis 诊断",
        "",
        "| Axis | Classification | Pairs | Covered folds | Non-zero folds | Δ rate min/max |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in evidence.get("axis_summaries", []):
        typed = _mapping(row, "axis_summary")
        delta = _mapping(typed.get("constraint_hit_rate_delta"), "delta")
        lines.append(
            f"| {typed.get('axis')} | {typed.get('classification')} | "
            f"{typed.get('pair_count')} | {typed.get('covered_fold_count')} | "
            f"{typed.get('nonzero_covered_fold_count')} | "
            f"{delta.get('minimum')} / {delta.get('maximum')} |"
        )
    lines.extend(["", "## Owner 选项", ""])
    for option in pack.get("options", []):
        typed = _mapping(option, "option")
        suffix = "（推荐）" if typed.get("recommended") is True else ""
        lines.extend(
            [
                f"### {typed.get('option_id')} {suffix}",
                "",
                f"- {typed.get('label')}。",
                f"- 需要新授权：`{str(typed.get('new_authorization_required')).lower()}`。",
                "- 同 package 重跑：`false`。",
                "- Prospective access：`false`。",
                "",
            ]
        )
    lines.extend(
        [
            "## 禁止事项",
            "",
            *[f"- {item}" for item in pack.get("prohibited_actions", [])],
            "",
        ]
    )
    return "\n".join(lines)


def _structure_checks(rows: Sequence[Mapping[str, Any]]) -> dict[str, bool]:
    return {
        "evaluation_count_is_1800": len(rows) == 1800,
        "all_recomputations_exact": all(row.get("status") == "PASS" for row in rows),
        "six_folds_present": {row.get("window_index") for row in rows} == set(range(1, 7)),
        "all_candidate_axes_exact": all(
            set(_mapping(row.get("parameters"), "parameters")) == set(TARGET_AXES) for row in rows
        ),
        "all_ranges_historical_seen": all(
            str(_mapping(row.get("requested_range"), "requested_range").get("end", ""))
            < "2026-07-22"
            for row in rows
        ),
        "all_rows_rejected_under_frozen_gate": all(
            _mapping(row.get("artifact"), "artifact").get("gate") == "reject" for row in rows
        ),
    }


def _policy_projection(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": policy["schema_version"],
        "policy_id": policy["policy_id"],
        "version": policy["version"],
        "status": policy["status"],
        "owner": policy["owner"],
        "owner_decision": policy["owner_decision"],
        "matched_contrast": policy["matched_contrast"],
        "interpretation": policy["interpretation"],
    }


def _numeric_distribution(values: Sequence[object]) -> dict[str, Any]:
    present = [
        _as_float(value, "numeric_distribution.value") for value in values if value is not None
    ]
    return {
        "observation_count": len(values),
        "present_count": len(present),
        "null_count": len(values) - len(present),
        "minimum": None if not present else min(present),
        "maximum": None if not present else max(present),
        "mean": None if not present else sum(present) / len(present),
    }


def _counter_records(values: Sequence[object] | Any) -> list[dict[str, Any]]:
    counts = Counter(values)
    return [
        {"key": key, "count": count}
        for key, count in sorted(counts.items(), key=lambda item: str(item[0]))
    ]


def _subtract_optional(right: object, left: object) -> float | int | None:
    if right is None or left is None:
        return None
    result = _as_float(right, "right") - _as_float(left, "left")
    if isinstance(right, int) and isinstance(left, int):
        return int(result)
    return result


def _direction(value: object) -> str:
    if value is None:
        return "NULL"
    numeric = _as_float(value, "direction.value")
    if numeric > 0:
        return "POSITIVE"
    if numeric < 0:
        return "NEGATIVE"
    return "ZERO"


def _canonical_scalar(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _scalar_sort_key(value: object) -> tuple[str, str]:
    return (type(value).__name__, _canonical_scalar(value))


def _tuple_sort_key(values: tuple[object, ...]) -> tuple[str, ...]:
    return tuple(_canonical_scalar(value) for value in values)


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} mapping required")
    return dict(value)


def _strings(value: object, field: str) -> list[str]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes, bytearray))
        or not all(isinstance(row, str) and row for row in value)
    ):
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} string list required")
    rows = list(value)
    if len(rows) != len(set(rows)) or rows != sorted(rows):
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} must be unique and sorted")
    return rows


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} positive int required")
    return value


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} number required")
    return float(value)


def _as_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Trading2458ConstraintCausalDiagnosticError(f"{field} numeric value required")
    return float(value)


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    if actual != expected:
        raise Trading2458ConstraintCausalDiagnosticError(
            f"{field} keys mismatch: missing={sorted(expected - actual)} "
            f"unknown={sorted(actual - expected)}"
        )


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise Trading2458ConstraintCausalDiagnosticError(f"mapping JSON required: {path}")
    return dict(payload)


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_json_bytes(dict(row), indent=None) for row in rows)


def _canonical_sha256(value: object) -> str:
    return sha256(canonical_json_bytes(value, indent=None)).hexdigest()


def _stable_hash(*values: object) -> str:
    return _canonical_sha256({"values": values})


def _check(check_id: str, passed: bool, details: str | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [] if details is None else [details],
    }


__all__ = [
    "DEFAULT_POLICY_PATH",
    "OUTPUT_FILENAMES",
    "SAFETY",
    "TARGET_AXES",
    "Trading2458ConstraintCausalDiagnosticError",
    "build_axis_summaries",
    "build_matched_contrasts",
    "build_trading2458_diagnostic",
    "load_trading2458_policy",
    "validate_trading2458_diagnostic",
    "write_trading2458_diagnostic",
]
