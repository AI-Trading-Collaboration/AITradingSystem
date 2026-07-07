from __future__ import annotations

from collections.abc import Mapping
from typing import Any

REMEDIATION_PLAN_SCHEMA_VERSION = "growth_tilt_engine_contract_gap_remediation_plan.v1"
REMEDIATION_VALIDATION_SCHEMA_VERSION = (
    "growth_tilt_engine_contract_gap_remediation_validation.v1"
)
VALIDATION_DESIGN_SCHEMA_VERSION = "growth_tilt_engine_contract_gap_validation_design.v1"

ALLOWED_REMEDIATION_CATEGORIES: tuple[str, ...] = (
    "as_of_semantics_required",
    "source_traceability_required",
    "validity_dependency_required",
    "pit_gate_requirement_required",
    "upstream_artifact_reference_required",
    "ambiguous_feature_boundary_requires_owner_review",
    "non_signal_feature_exclusion_required",
    "blocked_pending_prior_remediation",
)
GAP_MAPPING_STATUSES: tuple[str, ...] = (
    "missing_as_of_semantics",
    "missing_source_traceability",
    "missing_validity_dependency",
    "ambiguous_source_feature",
    "blocked_unresolved",
)
CATEGORY_PRIORITY: dict[str, int] = {
    "source_traceability_required": 1,
    "as_of_semantics_required": 2,
    "validity_dependency_required": 3,
    "pit_gate_requirement_required": 4,
    "ambiguous_feature_boundary_requires_owner_review": 5,
    "upstream_artifact_reference_required": 6,
    "non_signal_feature_exclusion_required": 7,
    "blocked_pending_prior_remediation": 8,
}


def build_growth_tilt_contract_gap_remediation_plan(
    mapping_result: Mapping[str, Any],
) -> dict[str, Any]:
    mapping_rows = _mapping_rows(mapping_result)
    gap_rows = [row for row in mapping_rows if row.get("mapping_status") in GAP_MAPPING_STATUSES]
    ordered_items = _ordered_items(gap_rows)
    validation = validate_growth_tilt_gap_remediation_plan(
        ordered_items,
        expected_gap_count=_expected_gap_count(mapping_result),
    )
    validation_design = build_growth_tilt_gap_validation_design(ordered_items)
    return {
        "schema_version": REMEDIATION_PLAN_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_task": "TRADING-2410",
        "source_status": mapping_result.get("status"),
        "source_known_feature_count": mapping_result.get("known_source_feature_count"),
        "source_blocked_or_gap_count": mapping_result.get("blocked_or_gap_count"),
        "gap_count": len(ordered_items),
        "allowed_remediation_categories": list(ALLOWED_REMEDIATION_CATEGORIES),
        "remediation_items": ordered_items,
        "ordered_remediation_items": ordered_items,
        "validation_design": validation_design,
        "remediation_plan_validation": validation,
        "unresolved_blocker_summary": build_unresolved_blocker_summary(ordered_items),
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_remediation_item(
    mapping_row: Mapping[str, Any],
    *,
    remediation_order: int,
) -> dict[str, Any]:
    category = remediation_category(mapping_row)
    missing_dimensions = missing_contract_dimensions(mapping_row, category)
    return {
        "remediation_order": remediation_order,
        "feature_id": str(mapping_row.get("feature_id", "")).strip(),
        "source_feature_name": str(mapping_row.get("feature_name", "")).strip()
        or str(mapping_row.get("feature_id", "")).strip(),
        "current_mapping_status": mapping_row.get("mapping_status"),
        "remediation_category": category,
        "missing_contract_dimension": missing_dimensions,
        "missing_as_of_semantics": "as_of_semantics" in missing_dimensions,
        "missing_source_traceability": "source_traceability" in missing_dimensions,
        "missing_validity_dependency": "validity_dependency" in missing_dimensions,
        "pit_eligibility_risk": pit_eligibility_risk(mapping_row),
        "remediation_action": remediation_action(mapping_row, category),
        "required_upstream_artifact": required_upstream_artifact(mapping_row, category),
        "required_code_doc_config_change": required_code_doc_config_change(
            mapping_row,
            category,
        ),
        "validation_requirement": validation_requirement(mapping_row, category),
        "dependency_ordering": dependency_ordering(category),
        "blocker_impact": blocker_impact(mapping_row, category),
        "blocks_contract_ready": True,
        "blocks_pit_gate": True,
        "requires_owner_review": requires_owner_review(mapping_row, category),
        "can_be_implemented_without_fresh_market_data": True,
        "gap_resolved_in_2411": False,
        "blocker_downgraded_in_2411": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def remediation_category(mapping_row: Mapping[str, Any]) -> str:
    status = str(mapping_row.get("mapping_status", "")).strip()
    feature_id = str(mapping_row.get("feature_id", "")).lower()
    feature_type = str(mapping_row.get("feature_type", "")).lower()
    validity_dependency = str(mapping_row.get("validity_dependency", "")).lower()
    source_ref = str(mapping_row.get("upstream_artifact_or_registry_reference", "")).lower()
    reasons = " ".join(str(item) for item in _as_list(mapping_row.get("mapping_status_reasons")))
    reasons = reasons.lower()

    if status == "missing_source_traceability":
        return "source_traceability_required"
    if status == "missing_as_of_semantics":
        return "as_of_semantics_required"
    if status == "missing_validity_dependency":
        return "validity_dependency_required"
    if status == "ambiguous_source_feature":
        return "ambiguous_feature_boundary_requires_owner_review"
    if status == "excluded_non_signal_feature":
        return "non_signal_feature_exclusion_required"
    if (
        "validity" in feature_id
        or "validity" in feature_type
        or "valid_until" in validity_dependency
    ):
        return "validity_dependency_required"
    if "signal_artifact" in feature_id or "signal_artifact" in feature_type:
        return "blocked_pending_prior_remediation"
    if "missing" in source_ref or "tbd" in source_ref or "ambiguous" in reasons:
        return "upstream_artifact_reference_required"
    if status == "blocked_unresolved":
        return "blocked_pending_prior_remediation"
    return "pit_gate_requirement_required"


def missing_contract_dimensions(
    mapping_row: Mapping[str, Any],
    category: str,
) -> list[str]:
    dimensions: list[str] = []
    if category == "as_of_semantics_required":
        dimensions.append("as_of_semantics")
    if category == "source_traceability_required":
        dimensions.append("source_traceability")
    if category == "validity_dependency_required":
        dimensions.append("validity_dependency")
    if category == "pit_gate_requirement_required":
        dimensions.append("pit_gate_requirement")
    if category == "upstream_artifact_reference_required":
        dimensions.append("upstream_artifact_reference")
    if category == "ambiguous_feature_boundary_requires_owner_review":
        dimensions.append("feature_boundary")
    if category == "non_signal_feature_exclusion_required":
        dimensions.append("non_signal_exclusion")
    if category == "blocked_pending_prior_remediation":
        dimensions.append("prior_gap_remediation")

    row_validity = str(mapping_row.get("validity_dependency", "")).lower()
    if "valid_until" in row_validity and "validity_dependency" not in dimensions:
        dimensions.append("validity_dependency")
    pit_status = str(mapping_row.get("pit_eligibility", "")).upper()
    if pit_status in {"UNKNOWN", "UNKNOWN_OR_APPROXIMATE_PIT", "NOT_PIT_SAFE"}:
        if "pit_gate_requirement" not in dimensions:
            dimensions.append("pit_gate_requirement")
    return dimensions


def pit_eligibility_risk(mapping_row: Mapping[str, Any]) -> str:
    pit_status = str(mapping_row.get("pit_eligibility", "")).upper()
    if pit_status in {"UNKNOWN", "UNKNOWN_OR_APPROXIMATE_PIT"}:
        return "blocking_unknown_pit_status"
    if pit_status == "NOT_PIT_SAFE":
        return "blocking_not_pit_safe"
    if pit_status == "APPROXIMATE_PIT":
        return "material_approximate_pit_evidence"
    return "none_identified_in_2411"


def remediation_action(mapping_row: Mapping[str, Any], category: str) -> str:
    feature_id = str(mapping_row.get("feature_id", "")).strip()
    actions = {
        "source_traceability_required": (
            f"Create a source feature manifest for {feature_id} with upstream artifact, "
            "source cutoff, generated_at, checksum, and lineage fields."
        ),
        "as_of_semantics_required": (
            f"Define explicit as-of semantics for {feature_id}, including decision-time "
            "availability and no-forward-window assertions."
        ),
        "validity_dependency_required": (
            f"Bind {feature_id} to the future valid-until contract and stale-signal "
            "handling boundary before PIT gate reconsideration."
        ),
        "pit_gate_requirement_required": (
            f"Add {feature_id} to PIT gate evidence with explicit pass/fail criteria."
        ),
        "upstream_artifact_reference_required": (
            f"Replace ambiguous or missing upstream reference for {feature_id} with a "
            "reviewable artifact or registry entry."
        ),
        "ambiguous_feature_boundary_requires_owner_review": (
            f"Ask owner to confirm whether {feature_id} is a source feature, signal "
            "artifact contract, or downstream evaluation artifact."
        ),
        "non_signal_feature_exclusion_required": (
            f"Document exclusion rationale for {feature_id} and remove it from signal "
            "contract-ready scope."
        ),
        "blocked_pending_prior_remediation": (
            f"Keep {feature_id} blocked until upstream source feature and validity "
            "remediation items have passed validation."
        ),
    }
    return actions[category]


def required_upstream_artifact(mapping_row: Mapping[str, Any], category: str) -> str:
    source_ref = str(mapping_row.get("upstream_artifact_or_registry_reference", "")).strip()
    if category == "validity_dependency_required":
        return "signal_validity_contract artifact and valid_until_window remediation result"
    if category == "blocked_pending_prior_remediation":
        return "completed prerequisite source-feature remediation evidence pack"
    if category == "ambiguous_feature_boundary_requires_owner_review":
        return "owner-reviewed feature boundary decision record"
    if source_ref:
        return source_ref
    return "source feature manifest or registry entry required"


def required_code_doc_config_change(mapping_row: Mapping[str, Any], category: str) -> str:
    feature_id = str(mapping_row.get("feature_id", "")).strip()
    changes = {
        "source_traceability_required": (
            f"Add governed manifest/config reference and docs for {feature_id}; wire "
            "validator fixtures before implementation."
        ),
        "as_of_semantics_required": (
            f"Add explicit as-of contract fields and replay-test fixtures for {feature_id}."
        ),
        "validity_dependency_required": (
            f"Add validity dependency contract link for {feature_id}; do not enable "
            "execution until valid_until_window remains separately remediated."
        ),
        "pit_gate_requirement_required": (
            f"Update PIT gate precondition docs for {feature_id}; no gate downgrade in 2411."
        ),
        "upstream_artifact_reference_required": (
            f"Add registry entry or artifact pointer for {feature_id}."
        ),
        "ambiguous_feature_boundary_requires_owner_review": (
            f"Document owner boundary decision for {feature_id} before code changes."
        ),
        "non_signal_feature_exclusion_required": (
            f"Document {feature_id} as excluded from signal contract-ready scope."
        ),
        "blocked_pending_prior_remediation": (
            f"Track {feature_id} as blocked until prerequisite remediation items close."
        ),
    }
    return changes[category]


def validation_requirement(mapping_row: Mapping[str, Any], category: str) -> str:
    feature_id = str(mapping_row.get("feature_id", "")).strip()
    validations = {
        "source_traceability_required": (
            f"Focused validator must reject {feature_id} without generated_at, source "
            "cutoff, checksum, and lineage; pass only after manifest coverage exists."
        ),
        "as_of_semantics_required": (
            f"As-of contract test must prove {feature_id} is known at decision time and "
            "does not use forward windows."
        ),
        "validity_dependency_required": (
            f"Validity contract test must prove {feature_id} has valid_from, valid_until, "
            "stale_after, and carry-forward behavior before gate reconsideration."
        ),
        "pit_gate_requirement_required": (
            f"PIT gate dry run must keep {feature_id} blocking until contract evidence passes."
        ),
        "upstream_artifact_reference_required": (
            f"Artifact existence and checksum test must pass for {feature_id}."
        ),
        "ambiguous_feature_boundary_requires_owner_review": (
            f"Owner review record must classify {feature_id} before it can be contract-ready."
        ),
        "non_signal_feature_exclusion_required": (
            f"Exclusion test must prove {feature_id} cannot affect signal readiness."
        ),
        "blocked_pending_prior_remediation": (
            f"Dependency test must keep {feature_id} unresolved until prerequisite "
            "source-feature and validity items pass."
        ),
    }
    return validations[category]


def dependency_ordering(category: str) -> str:
    return {
        "source_traceability_required": "phase_1_source_traceability",
        "as_of_semantics_required": "phase_2_as_of_semantics",
        "validity_dependency_required": "phase_3_validity_dependency",
        "pit_gate_requirement_required": "phase_4_pit_gate_precondition",
        "ambiguous_feature_boundary_requires_owner_review": "phase_5_owner_boundary_review",
        "upstream_artifact_reference_required": "phase_6_upstream_artifact_reference",
        "non_signal_feature_exclusion_required": "phase_7_non_signal_exclusion",
        "blocked_pending_prior_remediation": "phase_8_blocked_summary_review",
    }[category]


def blocker_impact(mapping_row: Mapping[str, Any], category: str) -> str:
    feature_id = str(mapping_row.get("feature_id", "")).strip()
    if category == "validity_dependency_required":
        return (
            f"{feature_id} keeps growth_tilt_engine blocked and remains coupled to the "
            "valid_until_window blocker."
        )
    if category == "blocked_pending_prior_remediation":
        return (
            f"{feature_id} is a blocking summary artifact; it cannot downgrade until "
            "source feature and validity remediation evidence exists."
        )
    return f"{feature_id} blocks contract-ready and PIT gate readiness until remediated."


def requires_owner_review(mapping_row: Mapping[str, Any], category: str) -> bool:
    return category in {
        "ambiguous_feature_boundary_requires_owner_review",
        "blocked_pending_prior_remediation",
        "validity_dependency_required",
    } or str(mapping_row.get("feature_type", "")).upper() == "SIGNAL_ARTIFACT_CONTRACT"


def validate_growth_tilt_gap_remediation_plan(
    remediation_items: list[Mapping[str, Any]],
    *,
    expected_gap_count: int | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    for item in remediation_items:
        feature_id = str(item.get("feature_id"))
        category = item.get("remediation_category")
        order = item.get("remediation_order")
        if category not in ALLOWED_REMEDIATION_CATEGORIES:
            errors.append(_error(feature_id, "UNCLASSIFIED_REMEDIATION_ITEM", "invalid category"))
        if not isinstance(order, int) or order <= 0 or order in seen_orders:
            errors.append(_error(feature_id, "INVALID_REMEDIATION_ORDER", "order must be unique"))
        if isinstance(order, int):
            seen_orders.add(order)
        for field in (
            "remediation_action",
            "required_upstream_artifact",
            "required_code_doc_config_change",
            "validation_requirement",
            "dependency_ordering",
            "blocker_impact",
        ):
            if not item.get(field):
                errors.append(_error(feature_id, "REQUIRED_FIELD_MISSING", field))
        if item.get("gap_resolved_in_2411") is True:
            errors.append(_error(feature_id, "SILENT_GAP_RESOLUTION", "gap resolved in plan"))
        if item.get("blocker_downgraded_in_2411") is True:
            errors.append(
                _error(feature_id, "SILENT_BLOCKER_DOWNGRADE", "blocker downgraded in plan")
            )
        if item.get("blocks_contract_ready") is not True:
            warnings.append(
                _warning(
                    feature_id,
                    "ITEM_NOT_MARKED_CONTRACT_READY_BLOCKING",
                    "gap item should block contract ready",
                )
            )
    if expected_gap_count is not None and len(remediation_items) != expected_gap_count:
        errors.append(
            _error(
                "growth_tilt_engine",
                "GAP_COUNT_MISMATCH",
                f"expected {expected_gap_count}, got {len(remediation_items)}",
            )
        )
    return {
        "schema_version": REMEDIATION_VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "expected_gap_count": expected_gap_count,
        "remediation_item_count": len(remediation_items),
        "unclassified_remediation_item_count": sum(
            1
            for item in remediation_items
            if item.get("remediation_category") not in ALLOWED_REMEDIATION_CATEGORIES
        ),
        "silent_gap_resolution_count": sum(
            1 for item in remediation_items if item.get("gap_resolved_in_2411") is True
        ),
        "silent_blocker_downgrade_count": sum(
            1 for item in remediation_items if item.get("blocker_downgraded_in_2411") is True
        ),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_growth_tilt_gap_validation_design(
    remediation_items: list[Mapping[str, Any]],
) -> dict[str, Any]:
    categories = sorted(
        {str(item.get("remediation_category")) for item in remediation_items},
        key=lambda category: CATEGORY_PRIORITY.get(category, 999),
    )
    return {
        "schema_version": VALIDATION_DESIGN_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "validation_goal": "prove contract evidence before any blocker downgrade review",
        "covered_remediation_categories": categories,
        "validation_stages": [
            {
                "stage_id": "source_traceability_manifest_validation",
                "required_before": "as_of_semantics_remediation",
                "acceptance": (
                    "all source_traceability_required items have manifest, checksum, "
                    "generated_at, and source cutoff"
                ),
            },
            {
                "stage_id": "as_of_semantics_contract_validation",
                "required_before": "validity_dependency_remediation",
                "acceptance": (
                    "as-of rows prove decision-time availability and no forward-window usage"
                ),
            },
            {
                "stage_id": "validity_dependency_contract_validation",
                "required_before": "pit_gate_reconsideration",
                "acceptance": (
                    "valid_from, valid_until, stale_after, and carry-forward rules are explicit"
                ),
            },
            {
                "stage_id": "pit_gate_dry_run_validation",
                "required_before": "owner_downgrade_review",
                "acceptance": "PIT gate remains blocking until all contract evidence passes",
            },
        ],
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_unresolved_blocker_summary(
    remediation_items: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_engine_unresolved_blocker_summary.v1",
        "engine_id": "growth_tilt_engine",
        "remediation_item_count": len(remediation_items),
        "blocked_feature_ids": [str(item.get("feature_id")) for item in remediation_items],
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "recommended_next_task": (
            "TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _ordered_items(gap_rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(
        gap_rows,
        key=lambda row: (
            CATEGORY_PRIORITY.get(remediation_category(row), 999),
            str(row.get("feature_id", "")),
        ),
    )
    return [
        build_remediation_item(row, remediation_order=index)
        for index, row in enumerate(sorted_rows, start=1)
    ]


def _mapping_rows(mapping_result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct_rows = _as_list(mapping_result.get("mapping_rows"))
    if direct_rows:
        return [_as_mapping(row) for row in direct_rows]
    contract_mapping = _as_mapping(mapping_result.get("source_feature_contract_mapping"))
    return [_as_mapping(row) for row in _as_list(contract_mapping.get("mapping_rows"))]


def _expected_gap_count(mapping_result: Mapping[str, Any]) -> int | None:
    value = mapping_result.get("blocked_or_gap_count")
    if isinstance(value, int):
        return value
    validation = _as_mapping(mapping_result.get("contract_mapping_validation"))
    value = validation.get("blocked_or_gap_count")
    return value if isinstance(value, int) else None


def _error(feature_id: str, code: str, message: str) -> dict[str, Any]:
    return {
        "feature_id": feature_id,
        "code": code,
        "message": message,
        "severity": "ERROR",
    }


def _warning(feature_id: str, code: str, message: str) -> dict[str, Any]:
    return {
        "feature_id": feature_id,
        "code": code,
        "message": message,
        "severity": "WARNING",
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
