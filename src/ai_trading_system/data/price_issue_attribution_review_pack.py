from __future__ import annotations

import ast
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.data.quality_issue_attribution_inventory import (
    load_and_validate_attribution_readiness_inventory,
)
from ai_trading_system.platform.artifacts import (
    load_strict_json_path,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PACK_SCHEMA_VERSION = "data_quality_price_non_market_session_attribution_review_pack.v1"
VALIDATION_SCHEMA_VERSION = "data_quality_price_non_market_session_attribution_review_validation.v1"
PROPOSAL_SCHEMA_VERSION = "data_quality_price_non_market_session_attribution_review_proposal.v1"
TASK_ID = "DATA-GOV-002C2P_PRICE_NON_MARKET_SESSION_ATTRIBUTION_SOURCE_OWNER_REVIEW_PACK"
PARENT_OWNER_DECISION_ID = (
    "owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1"
)

DEFAULT_INVENTORY_PATH = "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json"
DEFAULT_PROPOSAL_PATH = "config/data_quality/price_non_market_session_attribution_review_v1.yaml"
DEFAULT_QUALITY_SOURCE_PATH = "src/ai_trading_system/data/quality.py"
DEFAULT_DQ_POLICY_PATH = "config/data_quality.yaml"
DEFAULT_CALENDAR_SOURCE_PATH = "src/ai_trading_system/trading_calendar.py"
DEFAULT_SPECIAL_CLOSURE_LOADER_PATH = "src/ai_trading_system/us_equity_special_closure_policy.py"
DEFAULT_SPECIAL_CLOSURE_POLICY_PATH = "config/data/us_equity_special_closure_registry.yaml"
DEFAULT_JSON_PATH = "inputs/data_quality/price_non_market_session_attribution_review_pack_v1.json"
DEFAULT_MARKDOWN_PATH = "docs/data_quality/price_non_market_session_attribution_review_pack_v1.md"
DEFAULT_VALIDATION_PATH = (
    "inputs/data_quality/price_non_market_session_attribution_review_pack_v1.validation.json"
)

EXPECTED_SITE_ID = "dq_issue_site_312625a26da21428b763"
EXPECTED_ISSUE_CODE = "prices_non_market_session_date"
EXPECTED_EMITTER_FUNCTION = "_check_price_market_calendar_dates"
EXPECTED_CALENDAR_FUNCTION = "is_us_equity_trading_day"
CANONICAL_SOURCE_ROLE = "primary_market_prices"
SCOPE_TAXONOMY = "DISTINCT_NON_SESSION_DATE_ROW_SET"
PACK_STATUS = "SOURCE_OWNER_DECISION_PENDING"
PROPOSAL_STATUS = "PROPOSED_FOR_SOURCE_OWNER_REVIEW"
SOURCE_OWNER_DECISION_PENDING = "PENDING_SOURCE_OWNER_DECISION"
CONTRACT_WAVE_DISPOSITION = "CONTRACT_WAVE_CANDIDATE"

EXPECTED_REQUIRED_REVIEW_DIMENSIONS = [
    "affected_price_tickers",
    "affected_rate_series",
    "affected_source_roles",
    "affected_window",
    "affected_fields",
    "affected_rows",
]
AUTHORITY_FALSE_FIELDS = (
    "review_pack_is_authorization",
    "source_owner_decision_recorded",
    "runtime_contract_change_authorized",
    "new_issue_isolation_authorized",
    "window_or_row_level_isolation_authorized",
    "capability_policy_change_authorized",
    "consumer_migration_authorized",
    "message_or_sample_scope_inference_allowed",
)
EXPECTED_PROPOSAL_FIELDS = {
    "schema_version",
    "proposal_id",
    "proposal_version",
    "status",
    "task_id",
    "parent_owner_decision_id",
    "proposal_owner",
    "required_source_owner",
    "canonical_source_role",
    "candidate_source_domain",
    "review_condition",
    "window_or_row_level_isolation_authorized",
    "new_issue_isolation_authorized",
    "runtime_contract_change_authorized",
    "capability_policy_change_authorized",
    "consumer_migration_authorized",
    "candidate",
    "production_effect",
    "broker_action",
}
EXPECTED_CANDIDATE_FIELDS = {
    "site_id",
    "issue_code",
    "emitter_function",
    "severity_contract",
    "scope_taxonomy",
    "predicate_id",
    "current_rows_semantics",
    "current_sample_semantics",
    "affected_price_tickers_rule",
    "affected_rate_series",
    "affected_source_roles",
    "affected_date_rule",
    "affected_window_rule",
    "affected_fields",
    "row_identity_fields",
    "affected_rows_rule",
    "trigger_row_dependencies",
    "policy_dependencies",
    "attribution_completeness_requirements",
    "incomplete_when",
    "false_isolation_risks",
    "required_runtime_contract_tests",
    "proposed_contract_wave_disposition",
    "source_owner_decision",
    "source_owner_questions",
}
EXPECTED_CANDIDATE_EXACT_VALUES: dict[str, Any] = {
    "site_id": EXPECTED_SITE_ID,
    "issue_code": EXPECTED_ISSUE_CODE,
    "emitter_function": EXPECTED_EMITTER_FUNCTION,
    "severity_contract": "CALLER_SUPPLIED",
    "scope_taxonomy": SCOPE_TAXONOMY,
    "predicate_id": "OBSERVED_DATE_IS_NOT_A_REVIEWED_US_EQUITY_TRADING_SESSION",
    "current_rows_semantics": "DISTINCT_NON_SESSION_DATE_COUNT",
    "current_sample_semantics": "FIRST_TEN_DISTINCT_NON_SESSION_DATES",
    "affected_price_tickers_rule": ("DISTINCT_NORMALIZED_NON_EMPTY_TICKERS_FROM_ALL_TRIGGER_ROWS"),
    "affected_rate_series": [],
    "affected_source_roles": [CANONICAL_SOURCE_ROLE],
    "affected_date_rule": "DISTINCT_NON_SESSION_DATES_WITHIN_REQUESTED_WINDOW",
    "affected_window_rule": "REQUESTED_WINDOW_INCLUSIVE",
    "affected_fields": ["date"],
    "row_identity_fields": ["source_ordinal", "canonical_row_digest"],
    "affected_rows_rule": ("ALL_TRIGGER_ROWS_WITH_SOURCE_ORDINAL_AND_CANONICAL_ROW_DIGEST"),
    "trigger_row_dependencies": ["ALL_ROWS_WITH_NON_SESSION_DATE_WITHIN_REQUESTED_WINDOW"],
    "proposed_contract_wave_disposition": CONTRACT_WAVE_DISPOSITION,
    "source_owner_decision": SOURCE_OWNER_DECISION_PENDING,
}


class PriceIssueAttributionReviewError(ValueError):
    pass


@dataclass(frozen=True)
class ReviewPackPaths:
    repo_root: Path
    inventory_path: str = DEFAULT_INVENTORY_PATH
    proposal_path: str = DEFAULT_PROPOSAL_PATH
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH
    calendar_source_path: str = DEFAULT_CALENDAR_SOURCE_PATH
    special_closure_loader_path: str = DEFAULT_SPECIAL_CLOSURE_LOADER_PATH
    special_closure_policy_path: str = DEFAULT_SPECIAL_CLOSURE_POLICY_PATH

    def resolve(self, relative_path: str) -> Path:
        if not relative_path or "\\" in relative_path:
            raise PriceIssueAttributionReviewError(
                f"path must be normalized repository-relative POSIX: {relative_path}"
            )
        root = self.repo_root.resolve()
        candidate = Path(relative_path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise PriceIssueAttributionReviewError(f"path escapes repository: {relative_path}")
        resolved = (root / candidate).resolve()
        if root != resolved and root not in resolved.parents:
            raise PriceIssueAttributionReviewError(f"path escapes repository: {relative_path}")
        if not resolved.is_file():
            raise PriceIssueAttributionReviewError(f"required file missing: {relative_path}")
        return resolved


def build_price_issue_attribution_review_pack(
    *,
    repo_root: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
    calendar_source_path: str = DEFAULT_CALENDAR_SOURCE_PATH,
    special_closure_loader_path: str = DEFAULT_SPECIAL_CLOSURE_LOADER_PATH,
    special_closure_policy_path: str = DEFAULT_SPECIAL_CLOSURE_POLICY_PATH,
) -> dict[str, Any]:
    paths = ReviewPackPaths(
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
        calendar_source_path=calendar_source_path,
        special_closure_loader_path=special_closure_loader_path,
        special_closure_policy_path=special_closure_policy_path,
    )
    inventory = _load_current_inventory(paths)
    proposal = _load_and_validate_proposal(paths)
    dq_policy = _load_and_validate_dq_policy(paths)
    inventory_site = _exact_inventory_site(inventory)
    quality_ast_hash = _function_ast_hash(
        paths.resolve(paths.quality_source_path),
        EXPECTED_EMITTER_FUNCTION,
    )
    calendar_ast_hash = _function_ast_hash(
        paths.resolve(paths.calendar_source_path),
        EXPECTED_CALENDAR_FUNCTION,
    )
    candidate = _build_candidate(
        _mapping(proposal.get("candidate"), "proposal candidate"),
        inventory_site=inventory_site,
        quality_ast_hash=quality_ast_hash,
        calendar_ast_hash=calendar_ast_hash,
        dq_policy=dq_policy,
    )
    bindings = [
        _file_binding(paths, paths.inventory_path, role="c1_readiness_inventory"),
        _file_binding(paths, paths.proposal_path, role="source_owner_review_proposal"),
        _file_binding(paths, paths.quality_source_path, role="canonical_dq_source"),
        _file_binding(paths, paths.dq_policy_path, role="reviewed_dq_policy"),
        _file_binding(paths, paths.calendar_source_path, role="calendar_runtime_source"),
        _file_binding(
            paths,
            paths.special_closure_loader_path,
            role="special_closure_policy_loader",
        ),
        _file_binding(
            paths,
            paths.special_closure_policy_path,
            role="reviewed_special_closure_policy",
        ),
    ]
    authority = {field: False for field in AUTHORITY_FALSE_FIELDS}
    payload_without_id: dict[str, Any] = {
        "schema_version": PACK_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "parent_owner_decision_id": PARENT_OWNER_DECISION_ID,
        "status": PACK_STATUS,
        "authority": authority,
        "input_bindings": bindings,
        "c1_inventory_id": _text(inventory.get("inventory_id"), "inventory_id"),
        "proposal": {
            key: _json_value(value) for key, value in proposal.items() if key != "candidate"
        },
        "dq_policy_governance": _dq_policy_governance(dq_policy),
        "price_policy_snapshot": _json_value(_mapping(dq_policy.get("prices"), "DQ policy prices")),
        "calendar_authority": {
            "calendar_function": EXPECTED_CALENDAR_FUNCTION,
            "calendar_function_ast_sha256": calendar_ast_hash,
            "special_closure_policy_bound": True,
        },
        "summary": {
            "candidate_site_count": 1,
            "distinct_non_session_date_row_set_site_count": 1,
            "caller_supplied_severity_site_count": 1,
            "pending_source_owner_decision_count": 1,
            "current_rows_value_is_distinct_date_count": True,
            "current_rows_value_is_source_row_count": False,
            "complete_trigger_row_identity_required": True,
            "runtime_attribution_implemented_site_count": 0,
            "new_issue_isolation_authorized_site_count": 0,
        },
        "candidate": candidate,
        "contract_wave_recommendation": {
            "status": "READY_FOR_SOURCE_OWNER_DECISION",
            "candidate_issue_code": EXPECTED_ISSUE_CODE,
            "preserve_existing_instrument_only_rule_until_separate_approval": True,
            "price_ticker_attribution_must_be_complete_and_non_empty": True,
            "trigger_date_set_must_be_complete_and_non_empty": True,
            "trigger_row_identity_must_be_complete": True,
            "unknown_or_incomplete_attribution_remains_global": True,
            "window_or_row_level_isolation_authorized": False,
            "runtime_schema_change_required": True,
            "capability_classifier_change_required": True,
            "source_owner_decision_required": True,
            "serial_contract_wave_required_after_approval": True,
            "contract_wave_started": False,
            "consumer_migration_authorized": False,
        },
        "safety": {
            "data_quality_behavior_changed": False,
            "data_quality_policy_changed": False,
            "data_quality_issue_schema_changed": False,
            "capability_policy_or_classifier_changed": False,
            "new_issue_isolation_authorized": False,
            "window_or_row_level_isolation_authorized": False,
            "consumer_migration_executed": False,
            "cached_data_read": False,
            "cached_data_mutated": False,
            "strategy_logic_changed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    review_pack_id = (
        "dq_price_issue_attribution_review_"
        + sha256(_canonical_json_bytes(payload_without_id)).hexdigest()[:24]
    )
    return {
        "schema_version": PACK_SCHEMA_VERSION,
        "review_pack_id": review_pack_id,
        **{key: value for key, value in payload_without_id.items() if key != "schema_version"},
    }


def validate_price_issue_attribution_review_pack(
    payload: dict[str, Any],
    *,
    repo_root: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
    calendar_source_path: str = DEFAULT_CALENDAR_SOURCE_PATH,
    special_closure_loader_path: str = DEFAULT_SPECIAL_CLOSURE_LOADER_PATH,
    special_closure_policy_path: str = DEFAULT_SPECIAL_CLOSURE_POLICY_PATH,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        expected = build_price_issue_attribution_review_pack(
            repo_root=repo_root,
            inventory_path=inventory_path,
            proposal_path=proposal_path,
            quality_source_path=quality_source_path,
            dq_policy_path=dq_policy_path,
            calendar_source_path=calendar_source_path,
            special_closure_loader_path=special_closure_loader_path,
            special_closure_policy_path=special_closure_policy_path,
        )
    except (OSError, SyntaxError, TypeError, ValueError) as exc:
        return _validation_payload(
            status="FAIL",
            review_pack_id=str(payload.get("review_pack_id", "")),
            expected_review_pack_id=None,
            errors=[f"rebuild_failed:{type(exc).__name__}:{exc}"],
        )

    if payload != expected:
        errors.append("review_pack_content_mismatch")
    if payload.get("review_pack_id") != expected.get("review_pack_id"):
        errors.append("review_pack_id_mismatch")
    if payload.get("status") != PACK_STATUS:
        errors.append("review_pack_status_invalid")
    authority = payload.get("authority")
    if not isinstance(authority, Mapping) or any(
        authority.get(field) is not False for field in AUTHORITY_FALSE_FIELDS
    ):
        errors.append("authority_boundary_invalid")
    summary = payload.get("summary")
    if not isinstance(summary, Mapping) or any(
        (
            summary.get("candidate_site_count") != 1,
            summary.get("pending_source_owner_decision_count") != 1,
            summary.get("current_rows_value_is_distinct_date_count") is not True,
            summary.get("current_rows_value_is_source_row_count") is not False,
            summary.get("new_issue_isolation_authorized_site_count") != 0,
        )
    ):
        errors.append("summary_boundary_invalid")
    safety = payload.get("safety")
    if not isinstance(safety, Mapping) or any(
        (
            safety.get("production_effect") != "none",
            safety.get("broker_action") != "none",
            safety.get("new_issue_isolation_authorized") is not False,
            safety.get("window_or_row_level_isolation_authorized") is not False,
        )
    ):
        errors.append("safety_boundary_invalid")
    unique_errors = sorted(set(errors))
    return _validation_payload(
        status="PASS" if not unique_errors else "FAIL",
        review_pack_id=str(payload.get("review_pack_id", "")),
        expected_review_pack_id=str(expected.get("review_pack_id", "")),
        errors=unique_errors,
    )


def load_and_validate_price_issue_attribution_review_pack(
    *,
    repo_root: Path,
    pack_path: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
    calendar_source_path: str = DEFAULT_CALENDAR_SOURCE_PATH,
    special_closure_loader_path: str = DEFAULT_SPECIAL_CLOSURE_LOADER_PATH,
    special_closure_policy_path: str = DEFAULT_SPECIAL_CLOSURE_POLICY_PATH,
) -> dict[str, Any]:
    raw = load_strict_json_path(pack_path)
    if not isinstance(raw, dict):
        return _validation_payload(
            status="FAIL",
            review_pack_id="",
            expected_review_pack_id=None,
            errors=["review_pack_root_not_object"],
        )
    return validate_price_issue_attribution_review_pack(
        raw,
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
        calendar_source_path=calendar_source_path,
        special_closure_loader_path=special_closure_loader_path,
        special_closure_policy_path=special_closure_policy_path,
    )


def render_price_issue_attribution_review_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"), "review summary")
    candidate = _mapping(payload.get("candidate"), "review candidate")
    incomplete_when = _string_items(
        candidate.get("incomplete_when"),
        "candidate incomplete_when",
    )
    questions = _string_items(
        candidate.get("source_owner_questions"),
        "candidate source_owner_questions",
    )
    lines = [
        "# DATA-GOV-002C2P Price Non-Market-Session Attribution Source-Owner Review Pack",
        "",
        f"- Review pack ID：`{payload.get('review_pack_id')}`",
        f"- 状态：`{payload.get('status')}`",
        f"- Exact site count：`{summary.get('candidate_site_count')}`",
        (f"- Exact site：`{candidate.get('site_id')} / {candidate.get('issue_code')}`"),
        "- Source-owner decision：`PENDING_SOURCE_OWNER_DECISION`",
        "- 当前新增 runtime/schema/isolation/consumer 授权：`0`",
        "- Production effect：`none`；broker action：`none`",
        "",
        "## 工程结论",
        "",
        (
            "本 pack 只把一个已有 instrument-level pilot site 的六维归因建议整理成可审计"
            "评审输入。它不是 source-owner 批准记录，也不修改 `DataQualityIssue`、full/scoped "
            "DQ、capability classifier 或任何 consumer。"
        ),
        "",
        (
            "现有 issue 的 `rows` 是 requested window 内 distinct non-session date 数，"
            "不是触发 source row 数；`sample` 也只有前 10 个 distinct dates，不能作为完整 scope。"
        ),
        "",
        "## Proposed six-dimensional attribution",
        "",
        "|Dimension|Proposed rule|",
        "|---|---|",
        (f"|Price tickers|`{candidate.get('affected_price_tickers_rule')}`|"),
        "|Rate series|`[]`|",
        (
            "|Source roles|"
            f"`{', '.join(_string_items(candidate.get('affected_source_roles'), 'source roles'))}`|"
        ),
        f"|Dates|`{candidate.get('affected_date_rule')}`|",
        (
            "|Fields|"
            f"`{', '.join(_string_items(candidate.get('affected_fields'), 'affected fields'))}`|"
        ),
        f"|Rows|`{candidate.get('affected_rows_rule')}`|",
        "",
        "## Fail-closed incomplete conditions",
        "",
    ]
    lines.extend(f"- `{condition}`" for condition in incomplete_when)
    lines.extend(["", "## Price source-owner questions", ""])
    lines.extend(f"- {question}" for question in questions)
    lines.extend(
        [
            "",
            "## 后续边界",
            "",
            "- 当前 decision slot 仍为 `PENDING_SOURCE_OWNER_DECISION`。",
            "- 未完整归因时继续保持 `GLOBAL_OR_UNKNOWN_SCOPE`。",
            "- window/row-level isolation 仍未授权。",
            "- 获批后仍须另建最小 serial C3 runtime contract wave；本 pack 不自动启动 C3。",
            "- 不迁移 daily/periodic/research consumer，不生成权重、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def write_price_issue_attribution_review_artifacts(
    *,
    repo_root: Path,
    json_path: Path,
    markdown_path: Path,
    validation_path: Path,
) -> dict[str, Any]:
    pack = build_price_issue_attribution_review_pack(repo_root=repo_root)
    validation = validate_price_issue_attribution_review_pack(
        pack,
        repo_root=repo_root,
    )
    if validation.get("status") != "PASS":
        raise PriceIssueAttributionReviewError(
            f"refusing to write invalid review pack: {validation.get('errors')}"
        )
    write_json_atomic(json_path, pack)
    write_json_atomic(validation_path, validation)
    write_markdown_atomic(
        markdown_path,
        render_price_issue_attribution_review_markdown(pack),
    )
    return {
        "pack": pack,
        "validation": validation,
        "paths": {
            "json": json_path.as_posix(),
            "validation": validation_path.as_posix(),
            "markdown": markdown_path.as_posix(),
        },
    }


def _load_current_inventory(paths: ReviewPackPaths) -> dict[str, Any]:
    inventory_resolved = paths.resolve(paths.inventory_path)
    raw = load_strict_json_path(inventory_resolved)
    if not isinstance(raw, dict):
        raise PriceIssueAttributionReviewError("C1 inventory root must be an object")
    validation = load_and_validate_attribution_readiness_inventory(
        repo_root=paths.repo_root,
        inventory_path=inventory_resolved,
    )
    if validation.get("status") != "PASS":
        raise PriceIssueAttributionReviewError(
            f"C1 inventory is not current: {validation.get('errors')}"
        )
    return raw


def _load_and_validate_proposal(paths: ReviewPackPaths) -> dict[str, Any]:
    proposal = _mapping(
        safe_load_yaml_path(paths.resolve(paths.proposal_path)),
        "review proposal",
    )
    if set(proposal) != EXPECTED_PROPOSAL_FIELDS:
        raise PriceIssueAttributionReviewError(
            "proposal fields mismatch: "
            f"missing={sorted(EXPECTED_PROPOSAL_FIELDS - set(proposal))} "
            f"extra={sorted(set(proposal) - EXPECTED_PROPOSAL_FIELDS)}"
        )
    exact_values = {
        "schema_version": PROPOSAL_SCHEMA_VERSION,
        "status": PROPOSAL_STATUS,
        "task_id": TASK_ID,
        "parent_owner_decision_id": PARENT_OWNER_DECISION_ID,
        "canonical_source_role": CANONICAL_SOURCE_ROLE,
        "candidate_source_domain": "prices",
        "production_effect": "none",
        "broker_action": "none",
    }
    for field, expected in exact_values.items():
        if proposal.get(field) != expected:
            raise PriceIssueAttributionReviewError(f"proposal {field} must equal {expected}")
    for field in (
        "window_or_row_level_isolation_authorized",
        "new_issue_isolation_authorized",
        "runtime_contract_change_authorized",
        "capability_policy_change_authorized",
        "consumer_migration_authorized",
    ):
        if proposal.get(field) is not False:
            raise PriceIssueAttributionReviewError(f"proposal {field} must be false")
    for field in (
        "proposal_id",
        "proposal_version",
        "proposal_owner",
        "required_source_owner",
        "review_condition",
    ):
        _text(proposal.get(field), f"proposal {field}")
    _validate_proposal_candidate(_mapping(proposal.get("candidate"), "proposal candidate"))
    return dict(proposal)


def _validate_proposal_candidate(candidate: Mapping[str, Any]) -> None:
    if set(candidate) != EXPECTED_CANDIDATE_FIELDS:
        raise PriceIssueAttributionReviewError(
            "candidate fields mismatch: "
            f"missing={sorted(EXPECTED_CANDIDATE_FIELDS - set(candidate))} "
            f"extra={sorted(set(candidate) - EXPECTED_CANDIDATE_FIELDS)}"
        )
    for field, expected in EXPECTED_CANDIDATE_EXACT_VALUES.items():
        if candidate.get(field) != expected:
            raise PriceIssueAttributionReviewError(f"candidate {field} must equal {expected}")
    for field in (
        "policy_dependencies",
        "attribution_completeness_requirements",
        "incomplete_when",
        "false_isolation_risks",
        "required_runtime_contract_tests",
        "source_owner_questions",
    ):
        _string_items(candidate.get(field), f"candidate {field}")


def _load_and_validate_dq_policy(paths: ReviewPackPaths) -> dict[str, Any]:
    policy = _mapping(
        safe_load_yaml_path(paths.resolve(paths.dq_policy_path)),
        "DQ policy",
    )
    governance = _mapping(policy.get("governance"), "DQ policy governance")
    if (
        governance.get("policy_id") != "DATA_QUALITY_CACHE_GATE"
        or governance.get("status") != "REVIEWED"
        or not _text(governance.get("owner"), "DQ policy owner")
    ):
        raise PriceIssueAttributionReviewError(
            "DQ policy must be the reviewed canonical cache gate"
        )
    _mapping(policy.get("prices"), "DQ policy prices")
    return dict(policy)


def _exact_inventory_site(inventory: Mapping[str, Any]) -> Mapping[str, Any]:
    matches: list[Mapping[str, Any]] = []
    for item in _sequence(inventory.get("sites"), "C1 inventory sites"):
        site = _mapping(item, "C1 inventory site")
        if site.get("site_id") == EXPECTED_SITE_ID:
            matches.append(site)
    if len(matches) != 1:
        raise PriceIssueAttributionReviewError(
            f"C1 inventory must contain one exact site: {EXPECTED_SITE_ID}"
        )
    site = matches[0]
    exact_values: dict[str, Any] = {
        "static_code": EXPECTED_ISSUE_CODE,
        "code_kind": "STATIC_LITERAL",
        "enclosing_function": EXPECTED_EMITTER_FUNCTION,
        "source_path": DEFAULT_QUALITY_SOURCE_PATH,
        "severity_expression": "severity",
        "source_expression": "source",
        "owner_review_status": "EXISTING_OWNER_REVIEWED_PILOT",
        "scope_status": "EXISTING_POLICY_AUTHORIZED_INSTRUMENT_SCOPE",
        "existing_policy_authorized": True,
        "phase_c_migration_eligible": False,
        "legacy_affected_instruments_present": True,
        "message_or_sample_scope_inference_allowed": False,
        "required_review_dimensions": EXPECTED_REQUIRED_REVIEW_DIMENSIONS,
    }
    for field, expected in exact_values.items():
        if site.get(field) != expected:
            raise PriceIssueAttributionReviewError(
                f"C1 inventory mismatch for {EXPECTED_ISSUE_CODE}: {field}"
            )
    return site


def _build_candidate(
    proposal_candidate: Mapping[str, Any],
    *,
    inventory_site: Mapping[str, Any],
    quality_ast_hash: str,
    calendar_ast_hash: str,
    dq_policy: Mapping[str, Any],
) -> dict[str, Any]:
    for dependency in _string_items(
        proposal_candidate.get("policy_dependencies"),
        "candidate policy_dependencies",
    ):
        _resolve_policy_dependency(dq_policy, dependency)
    return {
        **{key: _json_value(value) for key, value in proposal_candidate.items()},
        "source_path": DEFAULT_QUALITY_SOURCE_PATH,
        "source_line": inventory_site.get("line"),
        "emitter_function_ast_sha256": quality_ast_hash,
        "calendar_function_ast_sha256": calendar_ast_hash,
        "c1_scope_status": inventory_site.get("scope_status"),
        "c1_owner_review_status": inventory_site.get("owner_review_status"),
        "c1_existing_policy_authorized": True,
        "c1_phase_c_migration_eligible": False,
        "runtime_attribution_implemented": False,
        "new_issue_isolation_authorized": False,
        "window_or_row_level_isolation_authorized": False,
        "message_or_sample_scope_inference_allowed": False,
    }


def _function_ast_hash(source_path: Path, function_name: str) -> str:
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=source_path.as_posix(),
        )
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise PriceIssueAttributionReviewError(
            f"cannot parse bound source {source_path.name}: {exc}"
        ) from exc
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name
    ]
    if len(matches) != 1:
        raise PriceIssueAttributionReviewError(
            f"bound source must contain one function {function_name}"
        )
    try:
        canonical_dump = ast.dump(
            matches[0],
            annotate_fields=True,
            include_attributes=False,
            show_empty=True,
        )
    except TypeError:
        # Python <3.13 always emitted empty AST fields and has no show_empty
        # argument. Newer runtimes add an empty PEP 695 type_params field;
        # omit it so the reviewed function authority is version-stable.
        canonical_dump = ast.dump(
            matches[0],
            annotate_fields=True,
            include_attributes=False,
        )
    material = canonical_dump.replace(", type_params=[]", "").encode("utf-8")
    return sha256(material).hexdigest()


def _resolve_policy_dependency(policy: Mapping[str, Any], dependency: str) -> Any:
    current: Any = policy
    for part in dependency.split("."):
        if not isinstance(current, Mapping) or part not in current:
            raise PriceIssueAttributionReviewError(
                f"proposal references missing DQ policy field: {dependency}"
            )
        current = current[part]
    return current


def _dq_policy_governance(policy: Mapping[str, Any]) -> dict[str, Any]:
    governance = _mapping(policy.get("governance"), "DQ policy governance")
    return {
        "policy_id": _text(governance.get("policy_id"), "DQ policy id"),
        "policy_version": _text(
            governance.get("policy_version"),
            "DQ policy version",
        ),
        "status": _text(governance.get("status"), "DQ policy status"),
        "owner": _text(governance.get("owner"), "DQ policy owner"),
        "role": _text(governance.get("role"), "DQ policy role"),
        "reviewed_at": _json_value(governance.get("reviewed_at")),
        "review_condition": _text(
            governance.get("review_condition"),
            "DQ policy review condition",
        ),
    }


def _file_binding(
    paths: ReviewPackPaths,
    relative_path: str,
    *,
    role: str,
) -> dict[str, Any]:
    content = paths.resolve(relative_path).read_bytes()
    return {
        "role": role,
        "path": relative_path,
        "sha256": sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _validation_payload(
    *,
    status: str,
    review_pack_id: str,
    expected_review_pack_id: str | None,
    errors: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": status,
        "task_id": TASK_ID,
        "review_pack_id": review_pack_id,
        "expected_review_pack_id": expected_review_pack_id,
        "error_count": len(errors),
        "errors": errors,
        "checks": {
            "content_derived_rebuild": status == "PASS",
            "c1_inventory_current_and_exact_site": status == "PASS",
            "source_policy_and_calendar_bindings": status == "PASS",
            "distinct_date_and_trigger_row_semantics_separated": status == "PASS",
            "six_dimension_proposal_complete": status == "PASS",
            "source_owner_decision_pending": status == "PASS",
            "no_new_runtime_or_isolation_authority": status == "PASS",
            "safety_boundary": status == "PASS",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PriceIssueAttributionReviewError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise PriceIssueAttributionReviewError(f"{label} keys must be strings")
    return value


def _sequence(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise PriceIssueAttributionReviewError(f"{label} must be a list")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise PriceIssueAttributionReviewError(f"{label} must be non-empty normalized text")
    return value


def _string_items(value: object, label: str) -> tuple[str, ...]:
    items = _sequence(value, label)
    normalized = tuple(_text(item, label) for item in items)
    if not normalized or len(normalized) != len(set(normalized)):
        raise PriceIssueAttributionReviewError(f"{label} must contain unique normalized text")
    return normalized


def _json_value(value: object) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PriceIssueAttributionReviewError("non-finite number is not valid review evidence")
        return value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PriceIssueAttributionReviewError(
                    "review evidence mapping keys must be strings"
                )
            result[key] = _json_value(item)
        return result
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    raise PriceIssueAttributionReviewError(
        f"unsupported review evidence value: {type(value).__name__}"
    )


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PriceIssueAttributionReviewError(
            f"review payload is not canonical JSON: {exc}"
        ) from exc


__all__ = [
    "DEFAULT_CALENDAR_SOURCE_PATH",
    "DEFAULT_DQ_POLICY_PATH",
    "DEFAULT_INVENTORY_PATH",
    "DEFAULT_JSON_PATH",
    "DEFAULT_MARKDOWN_PATH",
    "DEFAULT_PROPOSAL_PATH",
    "DEFAULT_QUALITY_SOURCE_PATH",
    "DEFAULT_SPECIAL_CLOSURE_LOADER_PATH",
    "DEFAULT_SPECIAL_CLOSURE_POLICY_PATH",
    "DEFAULT_VALIDATION_PATH",
    "EXPECTED_ISSUE_CODE",
    "EXPECTED_SITE_ID",
    "PACK_SCHEMA_VERSION",
    "PriceIssueAttributionReviewError",
    "build_price_issue_attribution_review_pack",
    "load_and_validate_price_issue_attribution_review_pack",
    "render_price_issue_attribution_review_markdown",
    "validate_price_issue_attribution_review_pack",
    "write_price_issue_attribution_review_artifacts",
]
