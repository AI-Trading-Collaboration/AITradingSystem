from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_trading_system.research_quality.source_feature_traceability_contract import (
    validate_source_feature_traceability_contract,
)

MAPPING_SCHEMA_VERSION = "growth_tilt_engine_source_feature_contract_mapping.v1"
VALIDATION_SCHEMA_VERSION = "growth_tilt_engine_source_feature_contract_mapping_validation.v1"
ALLOWED_MAPPING_STATUSES: tuple[str, ...] = (
    "mapped_contract_ready",
    "mapped_with_caveats",
    "missing_as_of_semantics",
    "missing_source_traceability",
    "missing_validity_dependency",
    "ambiguous_source_feature",
    "excluded_non_signal_feature",
    "blocked_unresolved",
)
BLOCKER_STATUSES: tuple[str, ...] = (
    "missing_as_of_semantics",
    "missing_source_traceability",
    "missing_validity_dependency",
    "ambiguous_source_feature",
    "blocked_unresolved",
)


def build_growth_tilt_source_feature_contract_mapping(
    source_feature_inventory: list[Mapping[str, Any]],
    *,
    source_feature_contract_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = [
        build_growth_tilt_feature_mapping_row(row)
        for row in source_feature_inventory
    ]
    validation = validate_growth_tilt_feature_mapping(rows)
    return {
        "schema_version": MAPPING_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "contract_schema_source": _as_mapping(source_feature_contract_schema).get(
            "schema_version",
            "source_feature_traceability_contract.v1",
        ),
        "known_source_feature_count": len(rows),
        "mapping_statuses_allowed": list(ALLOWED_MAPPING_STATUSES),
        "mapping_rows": rows,
        "contract_mapping_validation": validation,
        "unresolved_gap_summary": build_unresolved_gap_summary(rows),
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_growth_tilt_feature_mapping_row(source_feature: Mapping[str, Any]) -> dict[str, Any]:
    feature_id = str(source_feature.get("feature_id", "")).strip()
    contract_payload = _source_feature_contract_payload(source_feature)
    validation_result = validate_source_feature_traceability_contract(contract_payload)
    status, reasons = classify_growth_tilt_source_feature(source_feature, validation_result)
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "feature_type": source_feature.get("feature_type"),
        "source_system": _source_system(source_feature.get("source_config_or_artifact")),
        "upstream_artifact_or_registry_reference": source_feature.get(
            "source_config_or_artifact"
        ),
        "contract_payload": contract_payload,
        "contract_validation_result": validation_result,
        "mapping_status": status,
        "mapping_status_reasons": reasons,
        "as_of_semantics": source_feature.get("as_of_handling"),
        "pit_eligibility": source_feature.get("pit_status"),
        "source_snapshot_requirement": _source_snapshot_requirement(source_feature),
        "traceability_status": _traceability_status(source_feature),
        "validity_dependency": _validity_dependency(source_feature),
        "blocking_reason_if_unresolved": reasons if status in BLOCKER_STATUSES else [],
        "recommended_action": source_feature.get("recommended_action"),
        "used_by_growth_tilt_engine": (
            source_feature.get("used_by_growth_tilt_engine") is True
        ),
    }


def classify_growth_tilt_source_feature(
    source_feature: Mapping[str, Any],
    validation_result: Mapping[str, Any],
) -> tuple[str, list[str]]:
    feature_id = str(source_feature.get("feature_id", "")).strip()
    feature_type = str(source_feature.get("feature_type", "")).strip()
    severity = str(source_feature.get("severity", "")).strip()
    as_of_text = str(source_feature.get("as_of_handling", "")).lower()
    generated_text = str(source_feature.get("generated_at_handling", "")).lower()
    source_ref = str(source_feature.get("source_config_or_artifact", "")).lower()
    recommended_action = str(source_feature.get("recommended_action", "")).lower()
    pit_status = str(source_feature.get("pit_status", "")).strip()

    if source_feature.get("used_by_growth_tilt_engine") is False:
        return "excluded_non_signal_feature", ["feature is not used by growth_tilt_engine"]
    if severity == "BLOCKING":
        return "blocked_unresolved", [f"{feature_id} remains BLOCKING in 2406 inventory"]
    if "missing" in as_of_text or "missing" in recommended_action:
        return "missing_as_of_semantics", ["as-of semantics are missing"]
    if (
        "missing standalone" in source_ref
        or "tbd" in source_ref
        or feature_type == "SIGNAL_ARTIFACT_CONTRACT"
    ):
        return "ambiguous_source_feature", ["source feature identity or artifact is ambiguous"]
    if (
        "validity" in feature_id
        or "validity" in feature_type.lower()
        or "valid-until" in recommended_action
        or "valid_until" in recommended_action
    ):
        return "missing_validity_dependency", ["validity dependency is not grounded"]
    if (
        "not normalized" in generated_text
        or "no feature manifest" in generated_text
        or "no standalone generated_at" in generated_text
        or "lineage is missing" in as_of_text
    ):
        return "missing_source_traceability", [
            "source traceability or generated_at lineage is missing"
        ]
    if validation_result.get("valid") is not True:
        return "missing_source_traceability", ["source feature contract validator failed"]
    if pit_status == "TRUE_PIT":
        return "mapped_contract_ready", ["source feature validates against contract"]
    return "mapped_with_caveats", [
        "source feature validates against contract but PIT evidence still has caveats"
    ]


def validate_growth_tilt_feature_mapping(
    mapping_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for row in mapping_rows:
        feature_id = str(row.get("feature_id"))
        status = row.get("mapping_status")
        if status not in ALLOWED_MAPPING_STATUSES:
            errors.append(_error(feature_id, "UNCLASSIFIED_FEATURE", "invalid status"))
        if status == "mapped_contract_ready":
            payload = _as_mapping(row.get("contract_payload"))
            if payload.get("pit_status") != "TRUE_PIT":
                errors.append(
                    _error(
                        feature_id,
                        "READY_STATUS_WITHOUT_PIT_ELIGIBILITY",
                        "contract-ready feature must be PIT eligible",
                    )
                )
            if _as_mapping(row.get("contract_validation_result")).get("valid") is not True:
                errors.append(
                    _error(
                        feature_id,
                        "READY_STATUS_WITH_FAILED_VALIDATION",
                        "contract-ready feature must pass validator",
                    )
                )
        if status in {"missing_as_of_semantics", "missing_source_traceability"}:
            warnings.append(
                _warning(
                    feature_id,
                    "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
                    "feature remains mapped only to a remediation gap",
                )
            )
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "feature_count": len(mapping_rows),
        "unclassified_feature_count": sum(
            1
            for row in mapping_rows
            if row.get("mapping_status") not in ALLOWED_MAPPING_STATUSES
        ),
        "contract_ready_count": sum(
            1 for row in mapping_rows if row.get("mapping_status") == "mapped_contract_ready"
        ),
        "blocked_or_gap_count": sum(
            1 for row in mapping_rows if row.get("mapping_status") in BLOCKER_STATUSES
        ),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_unresolved_gap_summary(mapping_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    by_status = {status: 0 for status in ALLOWED_MAPPING_STATUSES}
    unresolved_features: list[str] = []
    for row in mapping_rows:
        status = str(row.get("mapping_status"))
        if status in by_status:
            by_status[status] += 1
        if status in BLOCKER_STATUSES:
            unresolved_features.append(str(row.get("feature_id")))
    return {
        "schema_version": "growth_tilt_engine_contract_gap_summary.v1",
        "engine_id": "growth_tilt_engine",
        "status_counts": by_status,
        "unresolved_feature_ids": unresolved_features,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "recommended_next_task": (
            "TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_feature_contract_payload(source_feature: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature_id": source_feature.get("feature_id"),
        "feature_family": "growth_tilt_engine",
        "source_config": source_feature.get("source_config_or_artifact"),
        "source_data": source_feature.get("source_config_or_artifact"),
        "as_of_handling": _as_of_handling_enum(source_feature.get("as_of_handling")),
        "generated_at_handling": _generated_at_handling_enum(
            source_feature.get("generated_at_handling")
        ),
        "lookback_window": source_feature.get("lookback_window"),
        "forward_window_used": _forward_window_used(source_feature.get("forward_window_used")),
        "pit_status": _pit_status_enum(source_feature.get("pit_status")),
        "pit_confidence": _pit_confidence_enum(source_feature.get("pit_confidence")),
        "risk_flags": _risk_flags(source_feature),
        "severity": source_feature.get("severity"),
        "explicit_reason": source_feature.get("recommended_action"),
    }


def _as_of_handling_enum(value: Any) -> str:
    text = str(value).lower()
    if "explicit" in text:
        return "EXPLICIT_AS_OF"
    if "missing" in text or "unknown" in text:
        return "UNKNOWN"
    if "derived" in text or "historical" in text or "trailing" in text:
        return "DERIVED_FROM_SOURCE_CUTOFF"
    if "after_market_close" in text:
        return "EXPLICIT_AS_OF"
    return "APPROXIMATE"


def _generated_at_handling_enum(value: Any) -> str:
    text = str(value).lower()
    if "explicit" in text:
        return "EXPLICIT_GENERATED_AT"
    if "missing" in text or "no standalone generated_at" in text:
        return "UNKNOWN"
    if "pipeline" in text or "research run" in text or "provider/cache" in text:
        return "DERIVED_FROM_PIPELINE_RUN"
    if "not normalized" in text or "no feature manifest" in text:
        return "APPROXIMATE"
    return "APPROXIMATE"


def _pit_status_enum(value: Any) -> str:
    text = str(value)
    if text in {"TRUE_PIT", "APPROXIMATE_PIT", "NOT_PIT_SAFE", "UNKNOWN", "NOT_APPLICABLE"}:
        return text
    if text == "UNKNOWN_OR_APPROXIMATE_PIT":
        return "UNKNOWN"
    return "UNKNOWN"


def _pit_confidence_enum(value: Any) -> str:
    text = str(value)
    if text in {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}:
        return text
    return "UNKNOWN"


def _forward_window_used(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).lower()
    return not (
        "none" in text
        or "not applicable" in text
        or text in {"", "0", "false", "null"}
    )


def _risk_flags(source_feature: Mapping[str, Any]) -> list[str]:
    text = " ".join(
        str(source_feature.get(key, ""))
        for key in ("revision_or_backfill_risk", "recommended_action", "as_of_handling")
    ).upper()
    flags: list[str] = []
    for needle, flag in (
        ("LOOKAHEAD", "LOOKAHEAD_RISK"),
        ("REVISION", "REVISION_RISK"),
        ("BACKFILL", "BACKFILL_RISK"),
        ("STALE", "STALE_DATA_RISK"),
        ("MISSING", "MISSING_DATA_RISK"),
        ("REGIME", "REGIME_CONFIRMATION_RISK"),
        ("VALID_UNTIL", "VALID_UNTIL_UNGROUNDED"),
        ("VALID-UNTIL", "VALID_UNTIL_UNGROUNDED"),
        ("THRESHOLD", "THRESHOLD_UNCALIBRATED"),
    ):
        if needle in text and flag not in flags:
            flags.append(flag)
    return flags


def _source_system(source_config_or_artifact: Any) -> str:
    text = str(source_config_or_artifact)
    if text.startswith("config/"):
        return "governed_config"
    if text.startswith("data/"):
        return "cached_data_artifact"
    if "missing" in text.lower():
        return "missing_artifact"
    return "derived_research_artifact"


def _source_snapshot_requirement(source_feature: Mapping[str, Any]) -> str:
    if str(source_feature.get("source_config_or_artifact", "")).startswith("config/"):
        return "record config path, policy version, checksum, and generated_at"
    return "record upstream artifact id, source_data_cutoff, generated_at, and checksum"


def _traceability_status(source_feature: Mapping[str, Any]) -> str:
    generated = str(source_feature.get("generated_at_handling", "")).lower()
    as_of = str(source_feature.get("as_of_handling", "")).lower()
    if "missing" in generated or "missing" in as_of:
        return "missing"
    if "not normalized" in generated or "lineage is missing" in as_of:
        return "partial"
    return "mapped_with_caveats"


def _validity_dependency(source_feature: Mapping[str, Any]) -> str:
    text = " ".join(
        str(source_feature.get(key, ""))
        for key in ("feature_id", "feature_type", "recommended_action", "source_config_or_artifact")
    ).lower()
    if "validity" in text or "valid_until" in text or "valid-until" in text:
        return "depends_on_valid_until_window_contract"
    return "none_identified_in_2410"


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
