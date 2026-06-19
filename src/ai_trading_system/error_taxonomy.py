"""Central structured error taxonomy for audit-facing workflows."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class ErrorCategory(StrEnum):
    """Stable error categories for CLI, report, and run-manifest boundaries."""

    INPUT_MISSING = "INPUT_MISSING"
    INPUT_STALE = "INPUT_STALE"
    SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"
    CONFIG_INVALID = "CONFIG_INVALID"
    ARTIFACT_NOT_FOUND = "ARTIFACT_NOT_FOUND"
    REPORT_SOURCE_INCOMPLETE = "REPORT_SOURCE_INCOMPLETE"
    RESEARCH_GATE_BLOCKED = "RESEARCH_GATE_BLOCKED"
    SAFETY_BOUNDARY_BLOCKED = "SAFETY_BOUNDARY_BLOCKED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True)
class ErrorCategoryDefinition:
    code: ErrorCategory
    summary: str
    default_next_action: str
    operator_fixable: bool


ERROR_CATEGORY_DEFINITIONS: Mapping[ErrorCategory, ErrorCategoryDefinition] = {
    ErrorCategory.INPUT_MISSING: ErrorCategoryDefinition(
        code=ErrorCategory.INPUT_MISSING,
        summary="A required input file, record, field, or environment value is absent.",
        default_next_action="locate_or_generate_required_input_without_fabrication",
        operator_fixable=True,
    ),
    ErrorCategory.INPUT_STALE: ErrorCategoryDefinition(
        code=ErrorCategory.INPUT_STALE,
        summary="An input exists but is older than the policy or market-calendar window allows.",
        default_next_action="refresh_or_reselect_valid_as_of_input",
        operator_fixable=True,
    ),
    ErrorCategory.SCHEMA_INCOMPATIBLE: ErrorCategoryDefinition(
        code=ErrorCategory.SCHEMA_INCOMPATIBLE,
        summary="An input or artifact does not match the expected schema contract.",
        default_next_action="migrate_or_regenerate_with_compatible_schema",
        operator_fixable=False,
    ),
    ErrorCategory.CONFIG_INVALID: ErrorCategoryDefinition(
        code=ErrorCategory.CONFIG_INVALID,
        summary="Configuration is missing required governance metadata or contains invalid values.",
        default_next_action="repair_config_and_rerun_validation",
        operator_fixable=False,
    ),
    ErrorCategory.ARTIFACT_NOT_FOUND: ErrorCategoryDefinition(
        code=ErrorCategory.ARTIFACT_NOT_FOUND,
        summary="A referenced artifact id or path cannot be resolved.",
        default_next_action="query_report_index_or_latest_pointer_before_rerun",
        operator_fixable=True,
    ),
    ErrorCategory.REPORT_SOURCE_INCOMPLETE: ErrorCategoryDefinition(
        code=ErrorCategory.REPORT_SOURCE_INCOMPLETE,
        summary="A report can render, but required source evidence is incomplete.",
        default_next_action="restore_source_artifacts_or_keep_report_limited",
        operator_fixable=True,
    ),
    ErrorCategory.RESEARCH_GATE_BLOCKED: ErrorCategoryDefinition(
        code=ErrorCategory.RESEARCH_GATE_BLOCKED,
        summary="A research gate intentionally blocks continuation or promotion.",
        default_next_action="return_to_hypothesis_or_owner_review_without_gate_relaxation",
        operator_fixable=False,
    ),
    ErrorCategory.SAFETY_BOUNDARY_BLOCKED: ErrorCategoryDefinition(
        code=ErrorCategory.SAFETY_BOUNDARY_BLOCKED,
        summary="A safety boundary forbids the requested action or output interpretation.",
        default_next_action="stop_and_review_safety_boundary_before_any_continuation",
        operator_fixable=False,
    ),
    ErrorCategory.INTERNAL_ERROR: ErrorCategoryDefinition(
        code=ErrorCategory.INTERNAL_ERROR,
        summary="Unexpected implementation failure after inputs, config, and safety checks pass.",
        default_next_action="fix_code_path_and_add_regression_test",
        operator_fixable=False,
    ),
}

ERROR_CATEGORY_CODES: tuple[str, ...] = tuple(category.value for category in ErrorCategory)

REQUIRED_LOG_FIELDS: tuple[str, ...] = (
    "run_id",
    "candidate_id",
    "stage",
    "artifact_id",
    "elapsed_seconds",
    "status",
    "next_action",
)


def normalize_error_category(value: str | ErrorCategory) -> ErrorCategory:
    """Return a valid ErrorCategory or raise ValueError with an auditable message."""

    if isinstance(value, ErrorCategory):
        return value
    try:
        return ErrorCategory(value)
    except ValueError as exc:
        allowed = ", ".join(ERROR_CATEGORY_CODES)
        raise ValueError(f"Unknown error category {value!r}; expected one of: {allowed}") from exc


def error_category_definition(value: str | ErrorCategory) -> ErrorCategoryDefinition:
    return ERROR_CATEGORY_DEFINITIONS[normalize_error_category(value)]


def build_error_record(
    *,
    category: str | ErrorCategory,
    message: str,
    stage: str,
    status: str,
    next_action: str | None = None,
    artifact_id: str | None = None,
    run_id: str | None = None,
    candidate_id: str | None = None,
    elapsed_seconds: float | None = None,
) -> dict[str, object]:
    definition = error_category_definition(category)
    return {
        "error_category": definition.code.value,
        "message": message,
        "run_id": run_id or "UNKNOWN",
        "candidate_id": candidate_id or "UNKNOWN",
        "stage": stage,
        "artifact_id": artifact_id or "UNKNOWN",
        "elapsed_seconds": elapsed_seconds,
        "status": status,
        "next_action": next_action or definition.default_next_action,
        "operator_fixable": definition.operator_fixable,
    }
