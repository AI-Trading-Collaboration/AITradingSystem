from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.research_context import (
    ResearchContextError,
    ResearchEvaluationContext,
)
from ai_trading_system.contracts.research_lifecycle import (
    ResearchLifecycleError,
    ResearchPreregistration,
)
from ai_trading_system.platform.artifacts.writer import (
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.research_campaign import CampaignSpec
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "dynamic_v3_clean_selection_preregistration_gate.v1"
REPORT_TYPE = "dynamic_v3_clean_selection_preregistration_gate"
MANIFEST_TYPE = "dynamic_v3_clean_selection_preregistration_gate_manifest"
VALIDATION_TYPE = "dynamic_v3_clean_selection_preregistration_gate_validation"

DEFAULT_CLEAN_SELECTION_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_v3_clean_selection_preregistration_policy.yaml"
)
DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_ops" / "strategy_restart" / "clean_selection_gate"
)

REPORT_FILENAME = "dynamic_v3_clean_selection_preregistration_gate.json"
MARKDOWN_FILENAME = "dynamic_v3_clean_selection_preregistration_gate.md"
MANIFEST_FILENAME = "dynamic_v3_clean_selection_preregistration_gate_manifest.json"

SAFETY = {
    "clean_run_unblocked": False,
    "unbiased_oos_claim_allowed": False,
    "candidate_expansion_allowed": False,
    "new_parameter_search_allowed": False,
    "evaluator_execution_allowed": False,
    "locked_holdout_access_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
    "production_effect": "none",
    "broker_action": "none",
}


class DynamicV3CleanSelectionGateError(ValueError):
    """Raised when the S0 eligibility artifact itself cannot be read or written."""


def build_dynamic_v3_clean_selection_preregistration_gate(
    *,
    r2_manifest_path: Path,
    policy_path: Path = DEFAULT_CLEAN_SELECTION_POLICY_PATH,
    preregistration_path: Path | None = None,
    research_context_path: Path | None = None,
    campaign_spec_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the S0 eligibility decision without running a research evaluator."""

    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3CleanSelectionGateError("generated_at must be timezone-aware")

    policy = _load_mapping_yaml(policy_path)
    r2_manifest = _load_mapping_json(r2_manifest_path)
    r2_commitments = _mapping(r2_manifest.get("input_commitments"))
    r2_decision_path = r2_manifest_path.with_name("strategy_research_restart_r2_decision.json")
    r2_decision = _load_mapping_json_optional(r2_decision_path)

    r1_manifest_path = _commitment_path(r2_commitments, "walk_forward_manifest")
    r1_report_path = _commitment_path(r2_commitments, "walk_forward_report")
    r1_manifest = _load_mapping_json_optional(r1_manifest_path)
    r1_report = _load_mapping_json_optional(r1_report_path)
    source_contract = _mapping(r1_manifest.get("clean_selection_contract"))

    resolved_preregistration_path = preregistration_path or _optional_contract_path(
        source_contract, "preregistration_path", r1_manifest_path
    )
    resolved_context_path = research_context_path or _optional_contract_path(
        source_contract, "research_context_path", r1_manifest_path
    )
    resolved_campaign_path = campaign_spec_path or _optional_contract_path(
        source_contract, "campaign_spec_path", r1_manifest_path
    )
    preregistration_raw = _load_mapping_document_optional(resolved_preregistration_path)
    context_raw = _load_mapping_document_optional(resolved_context_path)
    campaign_raw = _load_mapping_document_optional(resolved_campaign_path)

    checks: list[dict[str, Any]] = []
    drift_reasons = _source_drift_reasons(
        policy_path=policy_path,
        r2_manifest_path=r2_manifest_path,
        r2_manifest=r2_manifest,
        r2_decision_path=r2_decision_path,
        r1_manifest_path=r1_manifest_path,
        r1_report_path=r1_report_path,
        r1_manifest=r1_manifest,
        source_contract=source_contract,
        context_raw=context_raw,
    )
    checks.append(_check("source_and_policy_checksums_fresh", not drift_reasons, drift_reasons))

    qualification = _mapping(policy.get("qualification"))
    origin = str(
        source_contract.get(
            "candidate_universe_origin",
            r1_report.get("source_candidate_selection_method", ""),
        )
    )
    contaminated_origins = _text_set(qualification.get("contaminated_source_origins"))
    selected_after_visibility = source_contract.get("selected_after_result_visibility") is True
    contaminated = (
        origin in contaminated_origins
        or selected_after_visibility
        or r1_report.get("source_selection_contamination") is True
    )
    checks.append(
        _check(
            "source_origin_uncontaminated",
            not contaminated,
            [
                f"candidate_universe_origin={origin or 'missing'}",
                f"source_top_n={r1_manifest.get('top_n')}",
                f"selected_after_result_visibility={selected_after_visibility}",
            ],
        )
    )

    preregistration, preregistration_error = _parse_preregistration(preregistration_raw)
    context, context_error = _parse_context(context_raw)
    campaign, campaign_error = _parse_campaign(campaign_raw)
    incomplete_reasons = _incomplete_reasons(
        policy=policy,
        r2_manifest=r2_manifest,
        r2_decision=r2_decision,
        source_contract=source_contract,
        r1_report=r1_report,
        preregistration_raw=preregistration_raw,
        preregistration=preregistration,
        preregistration_error=preregistration_error,
        context=context,
        context_error=context_error,
        campaign=campaign,
        campaign_error=campaign_error,
    )
    checks.append(
        _check("canonical_preregistration_complete", not incomplete_reasons, incomplete_reasons)
    )

    visibility_reasons = _visibility_reasons(
        policy=policy,
        source_contract=source_contract,
        preregistration_raw=preregistration_raw,
        preregistration=preregistration,
    )
    checks.append(
        _check("results_not_visible_at_freeze", not visibility_reasons, visibility_reasons)
    )

    overlap_details = _holdout_overlap_details(r1_report)
    checks.append(_check("locked_holdout_isolated", not overlap_details, overlap_details))

    eligibility_status = _select_outcome(
        drift=bool(drift_reasons),
        contaminated=contaminated,
        incomplete=bool(incomplete_reasons),
        visible=bool(visibility_reasons),
        overlap=bool(overlap_details),
    )
    input_paths = {
        "policy": policy_path,
        "r2_manifest": r2_manifest_path,
        "r2_decision": r2_decision_path,
        "r1_walk_forward_manifest": r1_manifest_path,
        "r1_walk_forward_report": r1_report_path,
        "preregistration": resolved_preregistration_path,
        "research_context": resolved_context_path,
        "campaign_spec": resolved_campaign_path,
    }
    input_commitments = {
        name: _commitment(path) for name, path in input_paths.items() if path is not None
    }
    gate_id = (
        "clean-selection-gate_"
        + _stable_id(
            eligibility_status,
            {name: value.get("sha256") for name, value in input_commitments.items()},
            generated.isoformat(),
        )[:16]
    )
    canonical_summary = {
        "preregistration_id": (
            None if preregistration is None else preregistration.preregistration_id
        ),
        "research_context_id": None if context is None else context.context_id,
        "campaign_id": None if campaign is None else campaign.campaign_id,
        "result_visibility": preregistration_raw.get("result_visibility"),
        "freeze_at": preregistration_raw.get("frozen_at"),
        "candidate_universe_id": source_contract.get("candidate_universe_id"),
        "candidate_universe_origin": origin or None,
        "selection_rule_id": preregistration_raw.get("selection_rule_id"),
        "metric_ids": preregistration_raw.get("metric_ids", []),
        "policy_ref_ids": preregistration_raw.get("policy_ref_ids", []),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "gate_id": gate_id,
        "status": eligibility_status,
        "eligibility_status": eligibility_status,
        "source_decision": {
            "r2_decision_id": r2_manifest.get("decision_id"),
            "r2_decision": r2_decision.get("decision", r2_manifest.get("decision")),
            "walk_forward_id": r2_manifest.get("walk_forward_id"),
            "source_candidate_selection_method": r1_report.get("source_candidate_selection_method"),
            "source_selection_contamination": r1_report.get("source_selection_contamination"),
            "source_top_n": r1_manifest.get("top_n"),
        },
        "canonical_contracts": canonical_summary,
        "window_holdout_analysis": {
            "window_count": len(_records(r1_report.get("windows"))),
            "locked_holdout": r1_report.get("locked_holdout"),
            "overlap_count": len(overlap_details),
            "overlaps": overlap_details,
        },
        "checks": checks,
        "failed_checks": [item["check_id"] for item in checks if not item["passed"]],
        "next_responsible_party": (
            "research_owner_new_preregistration"
            if eligibility_status != "ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN"
            else "research_owner_explicit_clean_run_authorization"
        ),
        "input_commitments": input_commitments,
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_dynamic_v3_clean_selection_preregistration_gate(
    report: Mapping[str, Any],
    *,
    output_root: Path = DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / REPORT_FILENAME
    markdown_path = output_root / MARKDOWN_FILENAME
    manifest_path = output_root / MANIFEST_FILENAME
    write_json_atomic(report_path, dict(report))
    write_markdown_atomic(
        markdown_path,
        render_dynamic_v3_clean_selection_preregistration_gate(report),
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": MANIFEST_TYPE,
        "gate_id": report.get("gate_id"),
        "eligibility_status": report.get("eligibility_status"),
        "generated_at": report.get("generated_at"),
        "build_arguments": _build_arguments_from_report(report),
        "input_commitments": report.get("input_commitments", {}),
        "output_artifact_checksums": {
            REPORT_FILENAME: _file_sha256(report_path),
            MARKDOWN_FILENAME: _file_sha256(markdown_path),
        },
        "safety": dict(SAFETY),
    }
    write_json_atomic(manifest_path, manifest)
    return {
        "gate_id": report.get("gate_id"),
        "status": report.get("eligibility_status"),
        "report_path": report_path,
        "markdown_path": markdown_path,
        "manifest_path": manifest_path,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_dynamic_v3_clean_selection_preregistration_gate(
    *,
    output_root: Path = DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT,
) -> dict[str, Any]:
    report_path = output_root / REPORT_FILENAME
    markdown_path = output_root / MARKDOWN_FILENAME
    manifest_path = output_root / MANIFEST_FILENAME
    checks: list[dict[str, Any]] = []
    try:
        report = _load_mapping_json(report_path)
        manifest = _load_mapping_json(manifest_path)
        checks.extend(
            [
                _check("report_schema", report.get("schema_version") == SCHEMA_VERSION),
                _check("manifest_schema", manifest.get("schema_version") == SCHEMA_VERSION),
                _check("manifest_type", manifest.get("report_type") == MANIFEST_TYPE),
                _check("gate_id_matches", report.get("gate_id") == manifest.get("gate_id")),
                _check("safety_boundary", report.get("safety") == SAFETY),
                _check(
                    "manifest_safety_boundary",
                    manifest.get("safety") == SAFETY,
                ),
            ]
        )
        output_checksums = _mapping(manifest.get("output_artifact_checksums"))
        checks.extend(
            [
                _check(
                    f"output_checksum:{REPORT_FILENAME}",
                    output_checksums.get(REPORT_FILENAME) == _file_sha256_optional(report_path),
                ),
                _check(
                    f"output_checksum:{MARKDOWN_FILENAME}",
                    output_checksums.get(MARKDOWN_FILENAME) == _file_sha256_optional(markdown_path),
                ),
                _check(
                    "input_commitments_fresh",
                    _commitments_fresh(_mapping(manifest.get("input_commitments"))),
                ),
            ]
        )
        args = _mapping(manifest.get("build_arguments"))
        recomputed = build_dynamic_v3_clean_selection_preregistration_gate(
            r2_manifest_path=Path(str(args.get("r2_manifest_path", ""))),
            policy_path=Path(str(args.get("policy_path", ""))),
            preregistration_path=_path_or_none(args.get("preregistration_path")),
            research_context_path=_path_or_none(args.get("research_context_path")),
            campaign_spec_path=_path_or_none(args.get("campaign_spec_path")),
            generated_at=datetime.fromisoformat(str(report.get("generated_at"))),
        )
        checks.extend(
            [
                _check("report_content_recomputed", _json_equivalent(report, recomputed)),
                _check(
                    "markdown_content_recomputed",
                    markdown_path.is_file()
                    and markdown_path.read_text(encoding="utf-8")
                    == render_dynamic_v3_clean_selection_preregistration_gate(recomputed),
                ),
            ]
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        checks.append(_check("artifact_read_and_recompute", False, [str(exc)]))
        report = {}
    passed = all(item["passed"] for item in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_TYPE,
        "gate_id": report.get("gate_id"),
        "eligibility_status": report.get("eligibility_status"),
        "status": "PASS" if passed else "FAIL",
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
        "broker_action": "none",
    }


def render_dynamic_v3_clean_selection_preregistration_gate(
    report: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic v3 无污染选择预注册资格门",
        "",
        f"- gate_id: `{report.get('gate_id')}`",
        f"- eligibility_status: `{report.get('eligibility_status')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- clean_run_unblocked: `{report.get('clean_run_unblocked')}`",
        f"- unbiased_oos_claim_allowed: `{report.get('unbiased_oos_claim_allowed')}`",
        f"- candidate_expansion_allowed: `{report.get('candidate_expansion_allowed')}`",
        f"- new_parameter_search_allowed: `{report.get('new_parameter_search_allowed')}`",
        f"- production_effect: `{report.get('production_effect')}`",
        f"- broker_action: `{report.get('broker_action')}`",
        "",
        "## 资格检查",
        "",
    ]
    for item in _records(report.get("checks")):
        state = "PASS" if item.get("passed") is True else "FAIL"
        details = "; ".join(str(value) for value in item.get("details", [])) or "none"
        lines.append(f"- `{item.get('check_id')}`: **{state}** — {details}")
    source = _mapping(report.get("source_decision"))
    contracts = _mapping(report.get("canonical_contracts"))
    windows = _mapping(report.get("window_holdout_analysis"))
    lines.extend(
        [
            "",
            "## 来源与窗口",
            "",
            f"- R2 decision: `{source.get('r2_decision')}`",
            f"- source method: `{source.get('source_candidate_selection_method')}`",
            f"- source top-N: `{source.get('source_top_n')}`",
            f"- candidate universe origin: `{contracts.get('candidate_universe_origin')}`",
            f"- result visibility: `{contracts.get('result_visibility')}`",
            f"- locked-holdout overlap count: `{windows.get('overlap_count')}`",
            "",
            "## 解释边界",
            "",
            "该 artifact 只判定 future clean run 的输入资格；它不运行 evaluator、不访问 locked "
            "holdout，也不授权 candidate expansion、paper-shadow、production 或 "
            "broker/order 行为。",
            "",
        ]
    )
    return "\n".join(lines)


def _source_drift_reasons(
    *,
    policy_path: Path,
    r2_manifest_path: Path,
    r2_manifest: Mapping[str, Any],
    r2_decision_path: Path,
    r1_manifest_path: Path,
    r1_report_path: Path,
    r1_manifest: Mapping[str, Any],
    source_contract: Mapping[str, Any],
    context_raw: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not policy_path.is_file():
        reasons.append("policy_missing")
    if not r2_manifest_path.is_file():
        reasons.append("r2_manifest_missing")
    r2_outputs = _mapping(r2_manifest.get("output_artifact_checksums"))
    if r2_outputs.get(r2_decision_path.name) != _file_sha256_optional(r2_decision_path):
        reasons.append("r2_decision_checksum_drift")
    r2_commitments = _mapping(r2_manifest.get("input_commitments"))
    for name, path in (
        ("walk_forward_manifest", r1_manifest_path),
        ("walk_forward_report", r1_report_path),
    ):
        expected = _mapping(r2_commitments.get(name)).get("sha256")
        if expected != _file_sha256_optional(path):
            reasons.append(f"r2_commitment_drift:{name}")
    source_artifacts = _mapping(r1_manifest.get("source_artifacts"))
    source_checksums = _mapping(r1_manifest.get("source_checksums"))
    for path_key, raw_path in source_artifacts.items():
        name = str(path_key).removesuffix("_path")
        expected = source_checksums.get(f"{name}_sha256")
        path = _resolve_path(str(raw_path), base=r1_manifest_path.parent)
        if expected != _file_sha256_optional(path):
            reasons.append(f"r1_source_checksum_drift:{name}")
    for prefix in ("candidate_universe", "selection_rule"):
        raw_path = source_contract.get(f"{prefix}_path")
        expected = source_contract.get(f"{prefix}_sha256")
        if raw_path and expected != _file_sha256_optional(
            _resolve_path(str(raw_path), base=r1_manifest_path.parent)
        ):
            reasons.append(f"clean_source_checksum_drift:{prefix}")
    for item in _records(context_raw.get("policy_refs")):
        raw_path = item.get("path")
        expected = item.get("sha256")
        if raw_path and expected != _file_sha256_optional(
            _resolve_path(str(raw_path), base=PROJECT_ROOT)
        ):
            reasons.append(f"policy_ref_checksum_drift:{item.get('policy_id')}")
    return sorted(set(reasons))


def _incomplete_reasons(
    *,
    policy: Mapping[str, Any],
    r2_manifest: Mapping[str, Any],
    r2_decision: Mapping[str, Any],
    source_contract: Mapping[str, Any],
    r1_report: Mapping[str, Any],
    preregistration_raw: Mapping[str, Any],
    preregistration: ResearchPreregistration | None,
    preregistration_error: str | None,
    context: ResearchEvaluationContext | None,
    context_error: str | None,
    campaign: CampaignSpec | None,
    campaign_error: str | None,
) -> list[str]:
    qualification = _mapping(policy.get("qualification"))
    reasons: list[str] = []
    if policy.get("schema_version") != ("dynamic_v3_clean_selection_preregistration_policy.v1"):
        reasons.append("gate_policy_schema_mismatch")
    if policy.get("status") != "reviewed_pilot_baseline":
        reasons.append("gate_policy_status_mismatch")
    if _mapping(policy.get("safety_boundary")) != SAFETY:
        reasons.append("gate_policy_safety_boundary_mismatch")
    if qualification.get("ordered_outcomes") != [
        "BLOCKED_SOURCE_DRIFT",
        "BLOCKED_CONTAMINATED_LEGACY_SOURCE",
        "BLOCKED_INCOMPLETE_PREREGISTRATION",
        "BLOCKED_RESULT_VISIBILITY",
        "BLOCKED_HOLDOUT_OVERLAP",
        "ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN",
    ]:
        reasons.append("gate_policy_outcome_order_mismatch")
    if qualification.get("freeze_must_precede_first_selected_result") is not True:
        reasons.append("gate_policy_freeze_chronology_disabled")
    if qualification.get("train_and_test_must_not_overlap_locked_holdout") is not True:
        reasons.append("gate_policy_holdout_isolation_disabled")
    if r2_decision.get("status") != "PASS":
        reasons.append("r2_validated_decision_missing")
    if r2_decision.get("decision") != r2_manifest.get("decision"):
        reasons.append("r2_decision_manifest_mismatch")
    if (
        r2_decision.get("candidate_expansion_allowed") is not False
        or r2_decision.get("new_parameter_search_allowed") is not False
        or r2_decision.get("production_effect") != "none"
        or r2_decision.get("broker_action") != "none"
    ):
        reasons.append("r2_safety_boundary_invalid")
    required_prereg = _text_set(qualification.get("required_preregistration_fields"))
    for field in sorted(required_prereg):
        value = preregistration_raw.get(field)
        if value is None or value == "" or value == []:
            reasons.append(f"preregistration_field_missing:{field}")
    required_source = _text_set(qualification.get("required_source_contract_fields"))
    for field in sorted(required_source):
        value = source_contract.get(field)
        if value is None or value == "" or value == []:
            reasons.append(f"source_contract_field_missing:{field}")
    if not _records(r1_report.get("windows")):
        reasons.append("window_catalog_missing")
    locked = _mapping(r1_report.get("locked_holdout"))
    if not locked.get("start") or not locked.get("end"):
        reasons.append("locked_holdout_missing")
    required_visibility = str(qualification.get("required_result_visibility", "NONE"))
    if (
        preregistration_error
        and preregistration_raw.get("result_visibility") == required_visibility
    ):
        reasons.append(f"canonical_preregistration_invalid:{preregistration_error}")
    if context_error:
        reasons.append(f"canonical_research_context_invalid:{context_error}")
    if campaign_error:
        reasons.append(f"canonical_campaign_spec_invalid:{campaign_error}")
    origin = str(source_contract.get("candidate_universe_origin", ""))
    if origin not in _text_set(qualification.get("allowed_source_origins")):
        reasons.append(f"candidate_universe_origin_not_allowed:{origin or 'missing'}")
    schemas = _mapping(qualification.get("required_canonical_schemas"))
    if preregistration_raw.get("schema_version") != schemas.get("preregistration"):
        reasons.append("preregistration_schema_mismatch")
    if context is not None and context.to_dict().get("schema_version") != schemas.get(
        "research_context"
    ):
        reasons.append("research_context_schema_mismatch")
    if campaign is not None and campaign.schema_version != schemas.get("campaign_spec"):
        reasons.append("campaign_schema_mismatch")
    if preregistration is not None and context is not None:
        if preregistration.research_context_id != context.context_id:
            reasons.append("preregistration_context_id_mismatch")
        context_policy_ids = {item.policy_id for item in context.policy_refs}
        missing_policy_ids = sorted(set(preregistration.policy_ref_ids) - context_policy_ids)
        reasons.extend(f"preregistration_policy_ref_missing:{item}" for item in missing_policy_ids)
    if preregistration is not None:
        if preregistration.candidate_id != source_contract.get("candidate_universe_id"):
            reasons.append("preregistration_candidate_universe_mismatch")
        if preregistration.selection_rule_sha256 != source_contract.get("selection_rule_sha256"):
            reasons.append("preregistration_selection_rule_checksum_mismatch")
    if preregistration is not None and campaign is not None:
        if campaign.metadata.get("clean_selection_preregistration_id") != (
            preregistration.preregistration_id
        ):
            reasons.append("campaign_preregistration_id_mismatch")
        if (
            context is not None
            and campaign.metadata.get("research_context_id") != context.context_id
        ):
            reasons.append("campaign_research_context_id_mismatch")
        if campaign.owner_authorized_holdout:
            reasons.append("campaign_holdout_already_authorized")
        if (
            campaign.safety.paper_shadow_allowed
            or campaign.safety.official_target_weights
            or campaign.safety.production_effect != "none"
            or campaign.safety.broker_effect != "none"
            or campaign.safety.order_effect != "none"
        ):
            reasons.append("campaign_safety_boundary_invalid")
    return sorted(set(reasons))


def _visibility_reasons(
    *,
    policy: Mapping[str, Any],
    source_contract: Mapping[str, Any],
    preregistration_raw: Mapping[str, Any],
    preregistration: ResearchPreregistration | None,
) -> list[str]:
    reasons: list[str] = []
    required = str(_mapping(policy.get("qualification")).get("required_result_visibility", "NONE"))
    if preregistration_raw.get("result_visibility") != required:
        reasons.append(f"result_visibility={preregistration_raw.get('result_visibility')}")
    frozen_at = _datetime_optional(preregistration_raw.get("frozen_at"))
    first_result = _datetime_optional(source_contract.get("first_selected_result_at"))
    if frozen_at is not None and first_result is not None and frozen_at >= first_result:
        reasons.append("freeze_not_before_first_selected_result")
    if preregistration is not None and preregistration.result_visibility.value != required:
        reasons.append("canonical_result_visibility_mismatch")
    return sorted(set(reasons))


def _holdout_overlap_details(report: Mapping[str, Any]) -> list[str]:
    locked = _mapping(report.get("locked_holdout"))
    holdout = _date_range_optional(locked.get("start"), locked.get("end"))
    if holdout is None:
        return []
    details: list[str] = []
    for index, window in enumerate(_records(report.get("windows")), start=1):
        window_id = window.get("window_index", index)
        for phase in ("train", "test"):
            selected = _date_range_optional(
                window.get(f"effective_{phase}_start", window.get(f"{phase}_start")),
                window.get(f"effective_{phase}_end", window.get(f"{phase}_end")),
            )
            if selected is not None and _ranges_overlap(selected, holdout):
                details.append(
                    f"window={window_id}:{phase}:{selected[0].isoformat()}.."
                    f"{selected[1].isoformat()}"
                )
    return details


def _select_outcome(
    *, drift: bool, contaminated: bool, incomplete: bool, visible: bool, overlap: bool
) -> str:
    if drift:
        return "BLOCKED_SOURCE_DRIFT"
    if contaminated:
        return "BLOCKED_CONTAMINATED_LEGACY_SOURCE"
    if incomplete:
        return "BLOCKED_INCOMPLETE_PREREGISTRATION"
    if visible:
        return "BLOCKED_RESULT_VISIBILITY"
    if overlap:
        return "BLOCKED_HOLDOUT_OVERLAP"
    return "ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN"


def _parse_preregistration(
    payload: Mapping[str, Any],
) -> tuple[ResearchPreregistration | None, str | None]:
    try:
        return ResearchPreregistration.from_dict(payload), None
    except (ResearchLifecycleError, ValueError, TypeError) as exc:
        return None, str(exc)


def _parse_context(
    payload: Mapping[str, Any],
) -> tuple[ResearchEvaluationContext | None, str | None]:
    try:
        return ResearchEvaluationContext.from_dict(payload), None
    except (ResearchContextError, ValueError, TypeError) as exc:
        return None, str(exc)


def _parse_campaign(
    payload: Mapping[str, Any],
) -> tuple[CampaignSpec | None, str | None]:
    try:
        return CampaignSpec.model_validate(payload), None
    except (ValidationError, ValueError, TypeError) as exc:
        return None, str(exc)


def _build_arguments_from_report(report: Mapping[str, Any]) -> dict[str, str | None]:
    commitments = _mapping(report.get("input_commitments"))

    def path(name: str) -> str | None:
        value = _mapping(commitments.get(name)).get("path")
        return None if value is None else str(value)

    return {
        "r2_manifest_path": path("r2_manifest"),
        "policy_path": path("policy"),
        "preregistration_path": path("preregistration"),
        "research_context_path": path("research_context"),
        "campaign_spec_path": path("campaign_spec"),
    }


def _commitments_fresh(commitments: Mapping[str, Any]) -> bool:
    if not commitments:
        return False
    for raw in commitments.values():
        item = _mapping(raw)
        path = Path(str(item.get("path", "")))
        if item.get("sha256") != _file_sha256_optional(path):
            return False
    return True


def _commitment(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _file_sha256_optional(path),
        "size": path.stat().st_size if path.is_file() else None,
    }


def _commitment_path(commitments: Mapping[str, Any], name: str) -> Path:
    return Path(str(_mapping(commitments.get(name)).get("path", "")))


def _optional_contract_path(
    contract: Mapping[str, Any], name: str, source_path: Path
) -> Path | None:
    value = contract.get(name)
    if value is None or not str(value).strip():
        return None
    return _resolve_path(str(value), base=source_path.parent)


def _resolve_path(value: str, *, base: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    local = base / path
    if local.exists():
        return local
    return PROJECT_ROOT / path


def _path_or_none(value: object) -> Path | None:
    return None if value is None or not str(value).strip() else Path(str(value))


def _load_mapping_yaml(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise DynamicV3CleanSelectionGateError(f"mapping YAML required: {path}")
    return dict(payload)


def _load_mapping_document_optional(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    if path.suffix.lower() in {".yaml", ".yml"}:
        payload = safe_load_yaml_path(path)
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _load_mapping_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise DynamicV3CleanSelectionGateError(f"mapping JSON required: {path}")
    return dict(payload)


def _load_mapping_json_optional(path: Path) -> dict[str, Any]:
    try:
        return _load_mapping_json(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text_set(value: object) -> set[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return set()
    return {str(item) for item in value if str(item).strip()}


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _file_sha256_optional(path: Path) -> str | None:
    try:
        return _file_sha256(path) if path.is_file() else None
    except OSError:
        return None


def _stable_id(*values: object) -> str:
    encoded = json.dumps(
        values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _check(check_id: str, passed: bool, details: Sequence[object] = ()) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [str(value) for value in details],
    }


def _datetime_optional(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        result = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return result if result.tzinfo is not None and result.utcoffset() is not None else None


def _date_range_optional(start: object, end: object) -> tuple[date, date] | None:
    try:
        first = date.fromisoformat(str(start))
        last = date.fromisoformat(str(end))
    except ValueError:
        return None
    return (first, last) if first <= last else None


def _ranges_overlap(left: tuple[date, date], right: tuple[date, date]) -> bool:
    return max(left[0], right[0]) <= min(left[1], right[1])


def _json_equivalent(left: object, right: object) -> bool:
    return json.dumps(left, ensure_ascii=False, sort_keys=True) == json.dumps(
        right, ensure_ascii=False, sort_keys=True
    )


__all__ = [
    "DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT",
    "DEFAULT_CLEAN_SELECTION_POLICY_PATH",
    "DynamicV3CleanSelectionGateError",
    "build_dynamic_v3_clean_selection_preregistration_gate",
    "render_dynamic_v3_clean_selection_preregistration_gate",
    "validate_dynamic_v3_clean_selection_preregistration_gate",
    "write_dynamic_v3_clean_selection_preregistration_gate",
]
