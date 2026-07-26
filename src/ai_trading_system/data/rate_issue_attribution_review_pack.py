from __future__ import annotations

import ast
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.data.quality_issue_attribution_inventory import (
    OWNER_REVIEW_REQUIRED,
    load_and_validate_attribution_readiness_inventory,
)
from ai_trading_system.platform.artifacts import (
    load_strict_json_path,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PACK_SCHEMA_VERSION = "data_quality_rate_issue_attribution_review_pack.v1"
VALIDATION_SCHEMA_VERSION = "data_quality_rate_issue_attribution_review_validation.v1"
PROPOSAL_SCHEMA_VERSION = "data_quality_rate_row_issue_attribution_review_proposal.v1"
TASK_ID = "DATA-GOV-002C2_RATE_ROW_ISSUE_ATTRIBUTION_SOURCE_OWNER_REVIEW_PACK"
PARENT_OWNER_DECISION_ID = (
    "owner_decision:DATA-GOV-002:2026-07-26:" "approve_long_term_capability_receipt_engineering_v1"
)
DEFAULT_INVENTORY_PATH = "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json"
DEFAULT_PROPOSAL_PATH = "config/data_quality/rate_row_issue_attribution_review_v1.yaml"
DEFAULT_QUALITY_SOURCE_PATH = "src/ai_trading_system/data/quality.py"
DEFAULT_DQ_POLICY_PATH = "config/data_quality.yaml"
DEFAULT_JSON_PATH = "inputs/data_quality/rate_issue_attribution_review_pack_v1.json"
DEFAULT_MARKDOWN_PATH = "docs/data_quality/rate_issue_attribution_review_pack_v1.md"
DEFAULT_VALIDATION_PATH = (
    "inputs/data_quality/rate_issue_attribution_review_pack_v1.validation.json"
)

SOURCE_OWNER_DECISION_PENDING = "PENDING_SOURCE_OWNER_DECISION"
PACK_STATUS = "SOURCE_OWNER_DECISION_PENDING"
PROPOSAL_STATUS = "PROPOSED_FOR_SOURCE_OWNER_REVIEW"
CANONICAL_SOURCE_ROLE = "primary_macro_rates"
INITIAL_ISOLATION_RULE = "ALL_AFFECTED_RATE_SERIES_OUTSIDE_REQUIRED_SCOPE"
SINGLE_ROW = "SINGLE_SOURCE_ROW"
ROW_PAIR = "CURRENT_AND_PREVIOUS_VALID_OBSERVATION"

# These exact sites are the reviewed hand-off boundary from C1. Keeping the
# mapping named prevents a proposal edit from silently widening the first batch.
EXPECTED_SITE_BY_CODE = {
    "rates_invalid_date": "dq_issue_site_0e7f3d74bfa489801c83",
    "rates_invalid_value": "dq_issue_site_f337897b3d0d0b8e2842",
    "rates_non_finite_value": "dq_issue_site_dcc6dcab7a17c225b404",
    "rates_out_of_range": "dq_issue_site_6421117ee905a6da1438",
    "rates_extreme_daily_change": "dq_issue_site_85549de0f1e9ab739a74",
    "rates_suspicious_daily_change": "dq_issue_site_df1c184d09e3c55d3e71",
}
EXPECTED_TAXONOMY_BY_FUNCTION = {
    "_validate_rates": SINGLE_ROW,
    "_check_rate_ranges": SINGLE_ROW,
    "_check_rate_moves": ROW_PAIR,
}
EXPECTED_ROW_DEPENDENCIES = {
    SINGLE_ROW: ("TRIGGER_ROW",),
    ROW_PAIR: ("PREVIOUS_VALID_SAME_SERIES_ROW", "TRIGGER_ROW"),
}
EXPECTED_SEVERITY_BY_CODE = {
    "rates_invalid_date": "ERROR",
    "rates_invalid_value": "ERROR",
    "rates_non_finite_value": "ERROR",
    "rates_out_of_range": "ERROR",
    "rates_extreme_daily_change": "ERROR",
    "rates_suspicious_daily_change": "WARNING",
}
EXPECTED_CANDIDATE_FIELDS = {
    "site_id",
    "issue_code",
    "emitter_function",
    "severity",
    "scope_taxonomy",
    "predicate_id",
    "defect_fields",
    "identity_fields",
    "derived_fields",
    "row_dependencies",
    "affected_price_tickers",
    "affected_rate_series_rule",
    "affected_source_roles",
    "affected_window_rule",
    "affected_rows_rule",
    "attribution_completeness_requirements",
    "incomplete_when",
    "policy_dependencies",
    "proposed_contract_wave_disposition",
    "source_owner_decision",
    "source_owner_questions",
}
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
    "recommended_initial_isolation_rule",
    "window_or_row_level_isolation_authorized",
    "new_issue_isolation_authorized",
    "runtime_contract_change_authorized",
    "consumer_migration_authorized",
    "review_condition",
    "candidates",
    "production_effect",
    "broker_action",
}
ALLOWED_RATE_FIELDS = {"date", "series", "value"}
ALLOWED_DERIVED_FIELDS = {"_change"}


class RateIssueAttributionReviewError(ValueError):
    pass


@dataclass(frozen=True)
class ReviewPackPaths:
    repo_root: Path
    inventory_path: str = DEFAULT_INVENTORY_PATH
    proposal_path: str = DEFAULT_PROPOSAL_PATH
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH

    def resolve(self, relative_path: str) -> Path:
        if not relative_path or "\\" in relative_path:
            raise RateIssueAttributionReviewError(
                f"path must be normalized repository-relative POSIX: {relative_path}"
            )
        root = self.repo_root.resolve()
        candidate = Path(relative_path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise RateIssueAttributionReviewError(f"path escapes repository: {relative_path}")
        resolved = (root / candidate).resolve()
        if root != resolved and root not in resolved.parents:
            raise RateIssueAttributionReviewError(f"path escapes repository: {relative_path}")
        if not resolved.is_file():
            raise RateIssueAttributionReviewError(f"required file missing: {relative_path}")
        return resolved


def build_rate_issue_attribution_review_pack(
    *,
    repo_root: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
) -> dict[str, Any]:
    paths = ReviewPackPaths(
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
    )
    inventory = _load_current_inventory(paths)
    proposal = _load_and_validate_proposal(paths)
    dq_policy = _load_and_validate_dq_policy(paths)
    function_hashes = _function_ast_hashes(
        paths.resolve(paths.quality_source_path),
        expected_functions=set(EXPECTED_TAXONOMY_BY_FUNCTION),
    )
    inventory_sites = _inventory_sites_by_id(inventory)
    candidates = [
        _build_candidate(
            _mapping(candidate, "proposal candidate"),
            inventory_sites=inventory_sites,
            function_hashes=function_hashes,
            dq_policy=dq_policy,
        )
        for candidate in _sequence(proposal["candidates"], "proposal candidates")
    ]
    candidates.sort(key=lambda item: str(item["issue_code"]))

    taxonomy_counts = Counter(str(item["scope_taxonomy"]) for item in candidates)
    severity_counts = Counter(str(item["severity"]) for item in candidates)
    decision_counts = Counter(str(item["source_owner_decision"]) for item in candidates)
    disposition_counts = Counter(
        str(item["proposed_contract_wave_disposition"]) for item in candidates
    )
    summary = {
        "candidate_site_count": len(candidates),
        "single_source_row_site_count": taxonomy_counts[SINGLE_ROW],
        "current_and_previous_observation_site_count": taxonomy_counts[ROW_PAIR],
        "error_site_count": severity_counts["ERROR"],
        "warning_site_count": severity_counts["WARNING"],
        "pending_source_owner_decision_count": decision_counts[SOURCE_OWNER_DECISION_PENDING],
        "contract_wave_candidate_count": disposition_counts["CONTRACT_WAVE_CANDIDATE"],
        "runtime_attribution_implemented_site_count": 0,
        "new_issue_isolation_authorized_site_count": 0,
    }
    bindings = [
        _file_binding(paths, paths.inventory_path, role="c1_readiness_inventory"),
        _file_binding(paths, paths.proposal_path, role="source_owner_review_proposal"),
        _file_binding(paths, paths.quality_source_path, role="canonical_dq_source"),
        _file_binding(paths, paths.dq_policy_path, role="reviewed_dq_policy"),
    ]
    payload_without_id: dict[str, Any] = {
        "schema_version": PACK_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "parent_owner_decision_id": PARENT_OWNER_DECISION_ID,
        "status": PACK_STATUS,
        "authority": {
            "review_pack_is_authorization": False,
            "source_owner_decision_recorded": False,
            "new_issue_isolation_authorized": False,
            "runtime_contract_change_authorized": False,
            "capability_policy_change_authorized": False,
            "consumer_migration_authorized": False,
            "message_or_sample_scope_inference_allowed": False,
        },
        "input_bindings": bindings,
        "c1_inventory_id": _text(inventory.get("inventory_id"), "inventory_id"),
        "proposal": {
            key: _json_value(value) for key, value in proposal.items() if key != "candidates"
        },
        "dq_policy_governance": _dq_policy_governance(dq_policy),
        "rate_policy_snapshot": _json_value(_mapping(dq_policy.get("rates"), "DQ policy rates")),
        "summary": summary,
        "candidates": candidates,
        "contract_wave_recommendation": {
            "status": "READY_FOR_SOURCE_OWNER_DECISION",
            "recommended_initial_isolation_rule": INITIAL_ISOLATION_RULE,
            "candidate_issue_codes": sorted(EXPECTED_SITE_BY_CODE),
            "series_attribution_must_be_complete_and_non_empty": True,
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
            "consumer_migration_executed": False,
            "cached_data_read": False,
            "cached_data_mutated": False,
            "strategy_logic_changed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    pack_id = (
        "dq_rate_issue_attribution_review_"
        + sha256(_canonical_json_bytes(payload_without_id)).hexdigest()[:24]
    )
    return {
        "schema_version": PACK_SCHEMA_VERSION,
        "review_pack_id": pack_id,
        **{key: value for key, value in payload_without_id.items() if key != "schema_version"},
    }


def validate_rate_issue_attribution_review_pack(
    payload: dict[str, Any],
    *,
    repo_root: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        expected = build_rate_issue_attribution_review_pack(
            repo_root=repo_root,
            inventory_path=inventory_path,
            proposal_path=proposal_path,
            quality_source_path=quality_source_path,
            dq_policy_path=dq_policy_path,
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
    if payload.get("review_pack_id") != expected["review_pack_id"]:
        errors.append("review_pack_id_mismatch")
    if payload.get("status") != PACK_STATUS:
        errors.append("review_pack_status_invalid")
    authority = payload.get("authority")
    if not isinstance(authority, Mapping) or any(
        authority.get(field) is not False
        for field in (
            "review_pack_is_authorization",
            "source_owner_decision_recorded",
            "new_issue_isolation_authorized",
            "runtime_contract_change_authorized",
            "capability_policy_change_authorized",
            "consumer_migration_authorized",
            "message_or_sample_scope_inference_allowed",
        )
    ):
        errors.append("authority_boundary_invalid")
    summary = payload.get("summary")
    if not isinstance(summary, Mapping) or (
        summary.get("candidate_site_count") != len(EXPECTED_SITE_BY_CODE)
        or summary.get("pending_source_owner_decision_count") != len(EXPECTED_SITE_BY_CODE)
        or summary.get("new_issue_isolation_authorized_site_count") != 0
    ):
        errors.append("summary_boundary_invalid")
    safety = payload.get("safety")
    if (
        not isinstance(safety, Mapping)
        or safety.get("production_effect") != "none"
        or safety.get("broker_action") != "none"
        or safety.get("new_issue_isolation_authorized") is not False
    ):
        errors.append("safety_boundary_invalid")

    unique_errors = sorted(set(errors))
    return _validation_payload(
        status="PASS" if not unique_errors else "FAIL",
        review_pack_id=str(payload.get("review_pack_id", "")),
        expected_review_pack_id=str(expected["review_pack_id"]),
        errors=unique_errors,
    )


def load_and_validate_rate_issue_attribution_review_pack(
    *,
    repo_root: Path,
    pack_path: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
) -> dict[str, Any]:
    raw = load_strict_json_path(pack_path)
    if not isinstance(raw, dict):
        return _validation_payload(
            status="FAIL",
            review_pack_id="",
            expected_review_pack_id=None,
            errors=["review_pack_root_not_object"],
        )
    return validate_rate_issue_attribution_review_pack(
        raw,
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
    )


def render_rate_issue_attribution_review_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"), "review summary")
    recommendation = _mapping(
        payload.get("contract_wave_recommendation"),
        "contract wave recommendation",
    )
    candidates = [
        _mapping(item, "review candidate")
        for item in _sequence(payload.get("candidates"), "review candidates")
    ]
    lines = [
        "# DATA-GOV-002C2 Rate Row Issue Attribution Source-Owner Review Pack",
        "",
        f"- Review pack ID：`{payload.get('review_pack_id')}`",
        f"- 状态：`{payload.get('status')}`",
        f"- 候选 site：`{summary.get('candidate_site_count')}`",
        (
            "- Scope taxonomy：single-row="
            f"`{summary.get('single_source_row_site_count')}`，row-pair="
            f"`{summary.get('current_and_previous_observation_site_count')}`"
        ),
        (
            "- Source-owner decisions pending："
            f"`{summary.get('pending_source_owner_decision_count')}`"
        ),
        "- 当前新增隔离授权：`0`",
        "- Production effect：`none`；broker action：`none`",
        "",
        "## 工程结论",
        "",
        (
            "本 pack 建议 source owner 逐项审查以下 6 个 site。它不是审批记录，不修改 "
            "`DataQualityIssue`、capability classifier 或任何 full/scoped DQ 结果。"
        ),
        "",
        (
            "若后续逐项获批，C3 的首个保守规则建议为 "
            f"`{recommendation.get('recommended_initial_isolation_rule')}`。"
            "只有 affected rate series 完整、非空，且与 consumer required rate series "
            "完全不相交时才可讨论隔离；window/row-level isolation 仍未授权。"
        ),
        "",
        "## Exact candidates",
        "",
        "|Issue code|Site ID|Severity|Scope taxonomy|Affected window rule|Disposition|Decision|",
        "|---|---|---|---|---|---|---|",
    ]
    for candidate in candidates:
        lines.append(
            "|"
            f"`{candidate.get('issue_code')}`|"
            f"`{candidate.get('site_id')}`|"
            f"`{candidate.get('severity')}`|"
            f"`{candidate.get('scope_taxonomy')}`|"
            f"`{candidate.get('affected_window_rule')}`|"
            f"`{candidate.get('proposed_contract_wave_disposition')}`|"
            f"`{candidate.get('source_owner_decision')}`|"
        )

    lines.extend(["", "## 逐项 source-owner questions", ""])
    for candidate in candidates:
        row_dependencies = ", ".join(
            _string_items(
                candidate.get("row_dependencies"),
                "row dependencies",
            )
        )
        defect_fields = ", ".join(_string_items(candidate.get("defect_fields"), "defect fields"))
        incomplete_when = ", ".join(
            _string_items(candidate.get("incomplete_when"), "incomplete when")
        )
        lines.extend(
            [
                f"### `{candidate.get('issue_code')}`",
                "",
                f"- Predicate：`{candidate.get('predicate_id')}`",
                f"- Row dependencies：`{row_dependencies}`",
                f"- Defect fields：`{defect_fields}`",
                f"- Incomplete when：`{incomplete_when}`",
            ]
        )
        for question in _string_items(
            candidate.get("source_owner_questions"),
            "source owner questions",
        ):
            lines.append(f"- 待决定：{question}")
        lines.append("")

    lines.extend(
        [
            "## 后续边界",
            "",
            "- 所有 decision slots 仍为 `PENDING_SOURCE_OWNER_DECISION`。",
            "- 未完整归因的 issue 继续保持 `GLOBAL_OR_UNKNOWN_SCOPE`。",
            "- 获批后仍必须另建 C3 serial contract wave；本 pack 不自动启动 C3。",
            (
                "- 不迁移 daily/periodic consumer，不生成 strategy、weight、"
                "production 或 broker action。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def write_rate_issue_attribution_review_artifacts(
    *,
    repo_root: Path,
    json_path: Path,
    markdown_path: Path,
    validation_path: Path,
    inventory_path: str = DEFAULT_INVENTORY_PATH,
    proposal_path: str = DEFAULT_PROPOSAL_PATH,
    quality_source_path: str = DEFAULT_QUALITY_SOURCE_PATH,
    dq_policy_path: str = DEFAULT_DQ_POLICY_PATH,
) -> dict[str, Any]:
    pack = build_rate_issue_attribution_review_pack(
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
    )
    validation = validate_rate_issue_attribution_review_pack(
        pack,
        repo_root=repo_root,
        inventory_path=inventory_path,
        proposal_path=proposal_path,
        quality_source_path=quality_source_path,
        dq_policy_path=dq_policy_path,
    )
    if validation["status"] != "PASS":
        raise RateIssueAttributionReviewError(
            f"refusing to write invalid review pack: {validation['errors']}"
        )
    write_json_atomic(json_path, pack)
    write_json_atomic(validation_path, validation)
    write_markdown_atomic(
        markdown_path,
        render_rate_issue_attribution_review_markdown(pack),
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
        raise RateIssueAttributionReviewError("C1 inventory root must be an object")
    validation = load_and_validate_attribution_readiness_inventory(
        repo_root=paths.repo_root,
        inventory_path=inventory_resolved,
    )
    if validation.get("status") != "PASS":
        raise RateIssueAttributionReviewError(
            f"C1 inventory is not current: {validation.get('errors')}"
        )
    return raw


def _load_and_validate_proposal(paths: ReviewPackPaths) -> dict[str, Any]:
    raw = safe_load_yaml_path(paths.resolve(paths.proposal_path))
    proposal = _mapping(raw, "review proposal")
    if set(proposal) != EXPECTED_PROPOSAL_FIELDS:
        raise RateIssueAttributionReviewError(
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
        "candidate_source_domain": "rates",
        "recommended_initial_isolation_rule": INITIAL_ISOLATION_RULE,
        "production_effect": "none",
        "broker_action": "none",
    }
    for field, expected in exact_values.items():
        if proposal.get(field) != expected:
            raise RateIssueAttributionReviewError(f"proposal {field} must equal {expected}")
    for field in (
        "window_or_row_level_isolation_authorized",
        "new_issue_isolation_authorized",
        "runtime_contract_change_authorized",
        "consumer_migration_authorized",
    ):
        if proposal.get(field) is not False:
            raise RateIssueAttributionReviewError(f"proposal {field} must be false")
    for field in (
        "proposal_id",
        "proposal_version",
        "proposal_owner",
        "required_source_owner",
        "review_condition",
    ):
        _text(proposal.get(field), f"proposal {field}")

    candidates = [
        _mapping(item, "proposal candidate")
        for item in _sequence(proposal.get("candidates"), "proposal candidates")
    ]
    if len(candidates) != len(EXPECTED_SITE_BY_CODE):
        raise RateIssueAttributionReviewError(
            f"proposal must contain exactly {len(EXPECTED_SITE_BY_CODE)} candidates"
        )
    seen_codes: set[str] = set()
    seen_sites: set[str] = set()
    for candidate in candidates:
        _validate_proposal_candidate(candidate)
        code = _text(candidate.get("issue_code"), "candidate issue_code")
        site_id = _text(candidate.get("site_id"), "candidate site_id")
        if code in seen_codes or site_id in seen_sites:
            raise RateIssueAttributionReviewError("proposal candidate identity duplicated")
        seen_codes.add(code)
        seen_sites.add(site_id)
    if seen_codes != set(EXPECTED_SITE_BY_CODE):
        raise RateIssueAttributionReviewError(
            f"proposal issue codes mismatch: {sorted(seen_codes)}"
        )
    if seen_sites != set(EXPECTED_SITE_BY_CODE.values()):
        raise RateIssueAttributionReviewError(f"proposal site ids mismatch: {sorted(seen_sites)}")
    return dict(proposal)


def _validate_proposal_candidate(candidate: Mapping[str, Any]) -> None:
    if set(candidate) != EXPECTED_CANDIDATE_FIELDS:
        raise RateIssueAttributionReviewError(
            "candidate fields mismatch: "
            f"missing={sorted(EXPECTED_CANDIDATE_FIELDS - set(candidate))} "
            f"extra={sorted(set(candidate) - EXPECTED_CANDIDATE_FIELDS)}"
        )
    code = _text(candidate.get("issue_code"), "candidate issue_code")
    site_id = _text(candidate.get("site_id"), "candidate site_id")
    if EXPECTED_SITE_BY_CODE.get(code) != site_id:
        raise RateIssueAttributionReviewError(f"candidate site/code mismatch: {site_id}/{code}")
    function_name = _text(
        candidate.get("emitter_function"),
        "candidate emitter_function",
    )
    taxonomy = _text(candidate.get("scope_taxonomy"), "candidate scope_taxonomy")
    if EXPECTED_TAXONOMY_BY_FUNCTION.get(function_name) != taxonomy:
        raise RateIssueAttributionReviewError(f"candidate taxonomy/function mismatch: {code}")
    if candidate.get("severity") != EXPECTED_SEVERITY_BY_CODE[code]:
        raise RateIssueAttributionReviewError(f"candidate severity mismatch: {code}")
    if candidate.get("row_dependencies") != list(EXPECTED_ROW_DEPENDENCIES[taxonomy]):
        raise RateIssueAttributionReviewError(f"candidate row dependency mismatch: {code}")
    if candidate.get("affected_price_tickers") != []:
        raise RateIssueAttributionReviewError(
            f"candidate affected_price_tickers must be exact empty: {code}"
        )
    if candidate.get("affected_source_roles") != [CANONICAL_SOURCE_ROLE]:
        raise RateIssueAttributionReviewError(f"candidate source role mismatch: {code}")
    if (
        candidate.get("affected_rate_series_rule")
        != "DISTINCT_NORMALIZED_NON_EMPTY_SERIES_FROM_TRIGGER_ROWS"
    ):
        raise RateIssueAttributionReviewError(f"candidate rate series rule mismatch: {code}")
    if candidate.get("source_owner_decision") != SOURCE_OWNER_DECISION_PENDING:
        raise RateIssueAttributionReviewError(
            f"candidate source owner decision must remain pending: {code}"
        )
    if candidate.get("proposed_contract_wave_disposition") != "CONTRACT_WAVE_CANDIDATE":
        raise RateIssueAttributionReviewError(f"candidate disposition unsupported: {code}")
    _text(candidate.get("predicate_id"), f"{code} predicate_id")
    _text(candidate.get("affected_window_rule"), f"{code} affected_window_rule")
    _text(candidate.get("affected_rows_rule"), f"{code} affected_rows_rule")
    defect_fields = set(_string_items(candidate.get("defect_fields"), "defect_fields"))
    identity_fields = set(_string_items(candidate.get("identity_fields"), "identity_fields"))
    derived_fields = set(
        _string_items(
            candidate.get("derived_fields"),
            "derived_fields",
            allow_empty=True,
        )
    )
    if not defect_fields.issubset(ALLOWED_RATE_FIELDS):
        raise RateIssueAttributionReviewError(f"candidate defect fields invalid: {code}")
    if not identity_fields.issubset(ALLOWED_RATE_FIELDS):
        raise RateIssueAttributionReviewError(f"candidate identity fields invalid: {code}")
    if not derived_fields.issubset(ALLOWED_DERIVED_FIELDS):
        raise RateIssueAttributionReviewError(f"candidate derived fields invalid: {code}")
    for field in (
        "attribution_completeness_requirements",
        "incomplete_when",
        "source_owner_questions",
    ):
        _string_items(candidate.get(field), f"{code} {field}")
    _string_items(
        candidate.get("policy_dependencies"),
        f"{code} policy_dependencies",
        allow_empty=True,
    )


def _load_and_validate_dq_policy(paths: ReviewPackPaths) -> dict[str, Any]:
    raw = safe_load_yaml_path(paths.resolve(paths.dq_policy_path))
    policy = _mapping(raw, "DQ policy")
    governance = _mapping(policy.get("governance"), "DQ policy governance")
    if (
        governance.get("policy_id") != "DATA_QUALITY_CACHE_GATE"
        or governance.get("status") != "REVIEWED"
        or not _text(governance.get("owner"), "DQ policy owner")
    ):
        raise RateIssueAttributionReviewError("DQ policy must be the reviewed canonical cache gate")
    rates = _mapping(policy.get("rates"), "DQ policy rates")
    for required in (
        "min_plausible_value",
        "max_plausible_value",
        "suspicious_daily_change_abs",
        "extreme_daily_change_abs",
        "consistency_start_date",
        "series_overrides",
    ):
        if required not in rates:
            raise RateIssueAttributionReviewError(f"DQ rate policy field missing: {required}")
    return dict(policy)


def _build_candidate(
    proposal_candidate: Mapping[str, Any],
    *,
    inventory_sites: Mapping[str, Mapping[str, Any]],
    function_hashes: Mapping[str, str],
    dq_policy: Mapping[str, Any],
) -> dict[str, Any]:
    site_id = _text(proposal_candidate.get("site_id"), "candidate site_id")
    issue_code = _text(proposal_candidate.get("issue_code"), "candidate issue_code")
    inventory_site = inventory_sites.get(site_id)
    if inventory_site is None:
        raise RateIssueAttributionReviewError(f"candidate site absent from C1 inventory: {site_id}")
    emitter_function = _text(
        proposal_candidate.get("emitter_function"),
        "candidate emitter_function",
    )
    expected_severity_expression = f"Severity.{proposal_candidate.get('severity')}"
    exact_expectations = {
        "static_code": issue_code,
        "code_kind": "STATIC_LITERAL",
        "enclosing_function": emitter_function,
        "source_path": DEFAULT_QUALITY_SOURCE_PATH,
        "severity_expression": expected_severity_expression,
        "owner_review_status": OWNER_REVIEW_REQUIRED,
        "scope_status": "GLOBAL_OR_UNKNOWN_SCOPE",
    }
    for field, expected in exact_expectations.items():
        if inventory_site.get(field) != expected:
            raise RateIssueAttributionReviewError(
                f"C1 inventory mismatch for {issue_code}: {field}"
            )
    if (
        inventory_site.get("phase_c_migration_eligible") is not False
        or inventory_site.get("existing_policy_authorized") is not False
        or inventory_site.get("message_or_sample_scope_inference_allowed") is not False
    ):
        raise RateIssueAttributionReviewError(f"C1 authority boundary invalid for {issue_code}")
    if emitter_function not in function_hashes:
        raise RateIssueAttributionReviewError(
            f"emitter function missing from source: {emitter_function}"
        )
    for dependency in _string_items(
        proposal_candidate.get("policy_dependencies"),
        f"{issue_code} policy_dependencies",
        allow_empty=True,
    ):
        _resolve_policy_dependency(dq_policy, dependency)
    return {
        **{key: _json_value(value) for key, value in proposal_candidate.items()},
        "source_path": DEFAULT_QUALITY_SOURCE_PATH,
        "source_line": inventory_site.get("line"),
        "emitter_function_ast_sha256": function_hashes[emitter_function],
        "c1_scope_status": inventory_site.get("scope_status"),
        "c1_owner_review_status": inventory_site.get("owner_review_status"),
        "c1_phase_c_migration_eligible": False,
        "runtime_attribution_implemented": False,
        "new_issue_isolation_authorized": False,
        "message_or_sample_scope_inference_allowed": False,
    }


def _inventory_sites_by_id(
    inventory: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for item in _sequence(inventory.get("sites"), "C1 inventory sites"):
        site = _mapping(item, "C1 inventory site")
        site_id = _text(site.get("site_id"), "C1 site_id")
        if site_id in result:
            raise RateIssueAttributionReviewError(f"C1 inventory site duplicated: {site_id}")
        result[site_id] = site
    return result


def _function_ast_hashes(
    source_path: Path,
    *,
    expected_functions: set[str],
) -> dict[str, str]:
    try:
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=source_path.as_posix())
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise RateIssueAttributionReviewError(f"cannot parse canonical DQ source: {exc}") from exc
    result: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name not in expected_functions:
                continue
            if node.name in result:
                raise RateIssueAttributionReviewError(f"canonical function duplicated: {node.name}")
            material = ast.dump(
                node,
                annotate_fields=True,
                include_attributes=False,
            ).encode("utf-8")
            result[node.name] = sha256(material).hexdigest()
    if set(result) != expected_functions:
        raise RateIssueAttributionReviewError(
            "canonical function set mismatch: "
            f"missing={sorted(expected_functions - set(result))}"
        )
    return result


def _resolve_policy_dependency(policy: Mapping[str, Any], dependency: str) -> Any:
    current: Any = policy
    for part in dependency.split("."):
        if not isinstance(current, Mapping) or part not in current:
            raise RateIssueAttributionReviewError(
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
            "c1_inventory_current_and_exact_sites": status == "PASS",
            "source_and_policy_bindings": status == "PASS",
            "single_row_and_row_pair_taxonomy": status == "PASS",
            "all_source_owner_decisions_pending": status == "PASS",
            "no_new_runtime_or_isolation_authority": status == "PASS",
            "safety_boundary": status == "PASS",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RateIssueAttributionReviewError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise RateIssueAttributionReviewError(f"{label} keys must be strings")
    return value


def _sequence(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise RateIssueAttributionReviewError(f"{label} must be a list")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RateIssueAttributionReviewError(f"{label} must be non-empty normalized text")
    return value


def _string_items(
    value: object,
    label: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    items = _sequence(value, label)
    normalized = tuple(_text(item, label) for item in items)
    if (not normalized and not allow_empty) or len(normalized) != len(set(normalized)):
        raise RateIssueAttributionReviewError(f"{label} must contain unique normalized text")
    return normalized


def _json_value(value: object) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RateIssueAttributionReviewError("non-finite number is not valid review evidence")
        return value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise RateIssueAttributionReviewError(
                    "review evidence mapping keys must be strings"
                )
            result[key] = _json_value(item)
        return result
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    raise RateIssueAttributionReviewError(
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
        raise RateIssueAttributionReviewError(
            f"review payload is not canonical JSON: {exc}"
        ) from exc


__all__ = [
    "DEFAULT_DQ_POLICY_PATH",
    "DEFAULT_INVENTORY_PATH",
    "DEFAULT_JSON_PATH",
    "DEFAULT_MARKDOWN_PATH",
    "DEFAULT_PROPOSAL_PATH",
    "DEFAULT_QUALITY_SOURCE_PATH",
    "DEFAULT_VALIDATION_PATH",
    "EXPECTED_SITE_BY_CODE",
    "PACK_SCHEMA_VERSION",
    "RateIssueAttributionReviewError",
    "build_rate_issue_attribution_review_pack",
    "load_and_validate_rate_issue_attribution_review_pack",
    "render_rate_issue_attribution_review_markdown",
    "validate_rate_issue_attribution_review_pack",
    "write_rate_issue_attribution_review_artifacts",
]
