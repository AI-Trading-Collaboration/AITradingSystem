from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT"
REPORT_TYPE = "event_calendar_data_feasibility_audit"
ARTIFACT_ROLE = "event_calendar_feasibility_audit"
MODE = "feasibility_audit"
STATUS = "EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "event_calendar_feasibility_policy.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_EVENT_FAMILIES = {
    "FOMC",
    "CPI",
    "PCE",
    "NFP",
    "NVDA_EARNINGS",
    "AI_MEGA_CAP_EARNINGS",
    "TSM_MONTHLY_REVENUE",
    "SEMICONDUCTOR_EARNINGS_WINDOW",
}
REQUIRED_USE_CASES = {
    "pre_event_no_add",
    "post_event_confirmation_window",
    "manual_review_trigger",
    "earnings_cluster_risk",
}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "static_feasibility_audit": True,
    "event_rows_downloaded": False,
    "event_calendar_cache_written": False,
    "gating_signal_generated": False,
    "event_outcome_prediction_allowed": False,
    "candidate_generation_allowed": False,
    "candidate_artifact_generated": False,
    "candidate_signal_series_generated": False,
    "prediction_artifact_generated": False,
    "actual_path_validation_executed": False,
    "scope_review_executed": False,
    "forward_observe_started": False,
    "paper_shadow_allowed": False,
    "promotion_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class EventCalendarFeasibilityAuditError(ValueError):
    pass


def run_event_calendar_data_feasibility_audit(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise EventCalendarFeasibilityAuditError(
            "event calendar feasibility audit only supports feasibility_audit mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    common = _common_payload(generated_at=generated_at, policy_path=policy_path)
    source_rows = build_event_calendar_source_inventory(policy)
    known_at_rows = build_event_calendar_known_at_requirement_matrix(
        policy=policy,
        source_rows=source_rows,
    )
    use_case_rows = build_event_calendar_gating_use_case_matrix(
        policy=policy,
        source_rows=source_rows,
    )
    manual_review_contract = build_event_calendar_manual_review_trigger_contract(
        policy=policy,
        generated_at=generated_at,
    )
    validation_route = build_event_calendar_validation_route(
        source_rows=source_rows,
        use_case_rows=use_case_rows,
    )
    safety_boundary = build_event_calendar_safety_boundary(
        generated_at=generated_at,
        source_rows=source_rows,
        use_case_rows=use_case_rows,
    )
    summary = build_event_calendar_feasibility_summary(
        common=common,
        policy=policy,
        source_rows=source_rows,
        known_at_rows=known_at_rows,
        use_case_rows=use_case_rows,
        validation_route=validation_route,
    )
    paths = write_event_calendar_feasibility_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        source_rows=source_rows,
        known_at_rows=known_at_rows,
        use_case_rows=use_case_rows,
        manual_review_contract=manual_review_contract,
        validation_route=validation_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "source_inventory": source_rows,
            "known_at_requirement_matrix": known_at_rows,
            "gating_use_case_matrix": use_case_rows,
            "manual_review_trigger_contract": manual_review_contract,
            "validation_route": validation_route,
        }
    )


def build_event_calendar_source_inventory(
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_id, source_value in mapping(policy.get("event_sources")).items():
        source = mapping(source_value)
        row = {
            "source_id": str(source_id),
            "event_family": str(source.get("event_family", "")),
            "provider_name": str(source.get("provider_name", "")),
            "provider_class": str(source.get("provider_class", "")),
            "endpoint_or_file": str(source.get("endpoint_or_file", "")),
            "request_parameters": str(source.get("request_parameters", "")),
            "source_status": str(source.get("source_status", "")),
            "pit_status": str(source.get("pit_status", "")),
            "feasibility_grade": str(source.get("feasibility_grade", "")),
            "required_timestamps": _joined(source.get("required_timestamps")),
            "source_manifest_required": True,
            "manifest_fields_required": _joined(
                mapping(policy.get("source_manifest_requirements")).get("fields")
            ),
            "download_timestamp": "not_downloaded_static_feasibility_audit",
            "row_count": 0,
            "checksum": "",
            "event_rows_downloaded": False,
            "schema_validation_required": True,
            "known_at_policy_required": True,
            "generator_poc_ready": False,
            "runtime_gating_ready": False,
            "major_risks": _joined(source.get("major_risks")),
            "recommended_usage": str(source.get("recommended_usage", "")),
            "blocker": _source_blocker(source),
            **SAFETY_FIELDS,
        }
        rows.append(clean_for_yaml(row))
    return sorted(rows, key=lambda row: str(row["source_id"]))


def build_event_calendar_known_at_requirement_matrix(
    *,
    policy: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    manifest_fields = _joined(mapping(policy.get("source_manifest_requirements")).get("fields"))
    rows: list[dict[str, Any]] = []
    for row in source_rows:
        rows.append(
            clean_for_yaml(
                {
                    "source_id": row["source_id"],
                    "event_family": row["event_family"],
                    "required_timestamp_fields": row["required_timestamps"],
                    "required_manifest_fields": manifest_fields,
                    "required_schema_checks": (
                        "schema,completeness,freshness,duplicates,"
                        "known_at_before_decision,timezone_policy,checksum"
                    ),
                    "known_at_gate_status": "BLOCKED_PENDING_SOURCE_MANIFEST",
                    "pit_status": row["pit_status"],
                    "allowed_current_usage": "source_feasibility_audit_only",
                    "allowed_after_manifest": "diagnostic_event_gating_poc_only",
                    "blocked_usage": (
                        "promotion,paper_shadow,production,broker,"
                        "event_outcome_prediction,position_sizing"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_calendar_gating_use_case_matrix(
    *,
    policy: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    family_source_status = {
        str(row["event_family"]): str(row["source_status"]) for row in source_rows
    }
    rows: list[dict[str, Any]] = []
    for use_case_id, use_case_value in mapping(policy.get("gating_use_cases")).items():
        use_case = mapping(use_case_value)
        required_families = _strings(use_case.get("required_event_families"))
        missing_families = [
            family for family in required_families if family not in family_source_status
        ]
        blocked_families = [
            family
            for family in required_families
            if family_source_status.get(family) != "PIT_READY_APPROVED"
        ]
        rows.append(
            clean_for_yaml(
                {
                    "use_case_id": str(use_case_id),
                    "use_case": str(use_case.get("use_case", use_case_id)),
                    "intended_effect": str(use_case.get("intended_effect", "")),
                    "required_event_families": ",".join(required_families),
                    "missing_event_families": ",".join(missing_families),
                    "blocked_event_families": ",".join(blocked_families),
                    "runtime_status": str(use_case.get("runtime_status", "")),
                    "owner_review_required": bool(use_case.get("owner_review_required")),
                    "source_audit_required": True,
                    "gating_generator_ready": False,
                    "manual_review_contract_only": True,
                    "allowed_current_usage": "design_and_source_feasibility_only",
                    "blocked_usage": (
                        "runtime_no_add_gate,return_prediction,position_sizing,"
                        "promotion,paper_shadow,production,broker"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return sorted(rows, key=lambda row: str(row["use_case_id"]))


def build_event_calendar_manual_review_trigger_contract(
    *,
    policy: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    manifest_fields = _strings(mapping(policy.get("source_manifest_requirements")).get("fields"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_trigger_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "MANUAL_REVIEW_TRIGGER_CONTRACT_DRAFT_SOURCE_AUDIT_REQUIRED",
            "generated_at": generated_at.isoformat(),
            "contract_role": "source_feasibility_contract_only",
            "manual_review_only": True,
            "automatic_no_add_allowed": False,
            "automatic_position_change_allowed": False,
            "event_outcome_prediction_allowed": False,
            "required_event_row_fields": [
                "event_id",
                "event_family",
                "event_date",
                "event_time",
                "source_published_time",
                "known_at",
                "available_at",
                "provider_name",
                "source_url_or_file",
                "source_manifest_id",
            ],
            "required_source_manifest_fields": manifest_fields,
            "allowed_trigger_actions": ["manual_review_only"],
            "blocked_trigger_actions": [
                "auto_trade",
                "auto_no_add_runtime",
                "position_sizing",
                "broker_order",
                "paper_shadow_enrollment",
                "production_report_decision",
            ],
            "next_task": "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC",
            **SAFETY_FIELDS,
        }
    )


def build_event_calendar_validation_route(
    *,
    source_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in source_rows:
        rows.append(
            clean_for_yaml(
                {
                    "route_id": f"{source['source_id']}_source_route",
                    "route_type": "event_source",
                    "event_family": source["event_family"],
                    "readiness_status": "SOURCE_AUDIT_REQUIRED",
                    "required_before_trading_2319": (
                        "source_manifest,event_row_schema,known_at_timestamp,"
                        "available_at_timestamp,checksum"
                    ),
                    "allowed_next_step": (
                        "TRADING-2319_GENERATOR_POC_AFTER_SOURCE_MANIFEST"
                    ),
                    "blocked_validation": (
                        "event_gating_validation,promotion,paper_shadow,"
                        "production,broker"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    for use_case in use_case_rows:
        rows.append(
            clean_for_yaml(
                {
                    "route_id": f"{use_case['use_case_id']}_use_case_route",
                    "route_type": "gating_use_case",
                    "event_family": "",
                    "readiness_status": use_case["runtime_status"],
                    "required_before_trading_2319": (
                        "all_required_event_families_manifested,"
                        "manual_review_contract_owner_review"
                    ),
                    "allowed_next_step": (
                        "TRADING-2319_GENERATOR_POC_AFTER_OWNER_REVIEW"
                    ),
                    "blocked_validation": (
                        "runtime_no_add_gate,event_gating_validation,promotion,"
                        "paper_shadow,production,broker"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_calendar_safety_boundary(
    *,
    generated_at: datetime,
    source_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "event_source_count": len(source_rows),
            "gating_use_case_count": len(use_case_rows),
            "does_not_read_market_cache": True,
            "does_not_download_external_event_rows": True,
            "does_not_write_event_calendar_cache": True,
            "does_not_generate_event_gating_signal": True,
            "does_not_predict_event_outcome": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Static source feasibility audit does not consume cached market "
                "data. Future event-row ingestion, scoring, validation, reports "
                "or backtests must run their source schema gate and aits "
                "validate-data when cached market data is consumed."
            ),
            "allowed_next_step": "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC",
            **SAFETY_FIELDS,
        }
    )


def build_event_calendar_feasibility_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
    known_at_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    validation_route: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    primary_source_count = sum(
        1 for row in source_rows if "primary" in str(row.get("provider_class", ""))
    )
    source_audit_required_count = sum(
        1 for row in source_rows if row.get("source_status") != "PIT_READY_APPROVED"
    )
    pit_ready_source_count = sum(
        1 for row in source_rows if row.get("pit_status") == "PIT_READY_APPROVED"
    )
    source_blocked_count = sum(
        1 for row in source_rows if "BLOCKED" in str(row.get("pit_status", ""))
    )
    source_manifest = mapping(policy.get("source_manifest_requirements"))
    return clean_for_yaml(
        {
            **dict(common),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_manifest_required": bool(source_manifest.get("required")),
            "event_source_count": len(source_rows),
            "primary_source_count": primary_source_count,
            "source_audit_required_count": source_audit_required_count,
            "pit_ready_source_count": pit_ready_source_count,
            "source_blocked_count": source_blocked_count,
            "known_at_requirement_row_count": len(known_at_rows),
            "gating_use_case_count": len(use_case_rows),
            "validation_route_row_count": len(validation_route),
            "event_families": [row["event_family"] for row in source_rows],
            "gating_use_cases": [row["use_case_id"] for row in use_case_rows],
            "generator_poc_ready_now": False,
            "event_rows_downloaded": False,
            "event_calendar_cache_written": False,
            "recommended_next_task": (
                "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC_AFTER_SOURCE_AUDIT"
            ),
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_event_calendar_feasibility_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
    known_at_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    manual_review_contract: Mapping[str, Any],
    validation_route: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "event_calendar_data_feasibility_summary.json",
        "source_inventory_json": output_dir / "event_calendar_source_inventory.json",
        "source_inventory_csv": output_dir / "event_calendar_source_inventory.csv",
        "known_at_json": output_dir / "event_calendar_known_at_requirement_matrix.json",
        "known_at_csv": output_dir / "event_calendar_known_at_requirement_matrix.csv",
        "use_case_json": output_dir / "event_calendar_gating_use_case_matrix.json",
        "use_case_csv": output_dir / "event_calendar_gating_use_case_matrix.csv",
        "manual_review_contract": output_dir
        / "event_calendar_manual_review_trigger_contract.json",
        "validation_route_json": output_dir / "event_calendar_validation_route.json",
        "validation_route_csv": output_dir / "event_calendar_validation_route.csv",
        "safety_boundary": output_dir / "event_calendar_safety_boundary.json",
        "report_doc": docs_root / "event_calendar_data_feasibility_audit.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["source_inventory_json"], {**dict(common), "rows": source_rows})
    write_csv_rows(paths["source_inventory_csv"], source_rows)
    write_json(paths["known_at_json"], {**dict(common), "rows": known_at_rows})
    write_csv_rows(paths["known_at_csv"], known_at_rows)
    write_json(paths["use_case_json"], {**dict(common), "rows": use_case_rows})
    write_csv_rows(paths["use_case_csv"], use_case_rows)
    write_json(paths["manual_review_contract"], dict(manual_review_contract))
    write_json(paths["validation_route_json"], {**dict(common), "rows": validation_route})
    write_csv_rows(paths["validation_route_csv"], validation_route)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            source_rows=source_rows,
            use_case_rows=use_case_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Event Calendar Data Feasibility Audit",
            "",
            "TRADING-2318 只审计 event calendar source 的 PIT / known-at 可行性。"
            "它不下载事件 rows，不生成 gating signal，不预测事件结果，不进入仓位、"
            "paper-shadow、production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- event_source_count: `{summary['event_source_count']}`",
            f"- source_audit_required_count: `{summary['source_audit_required_count']}`",
            f"- pit_ready_source_count: `{summary['pit_ready_source_count']}`",
            f"- source_blocked_count: `{summary['source_blocked_count']}`",
            f"- gating_use_case_count: `{summary['gating_use_case_count']}`",
            f"- generator_poc_ready_now: `{summary['generator_poc_ready_now']}`",
            f"- recommended_next_task: `{summary['recommended_next_task']}`",
            "- gating_signal_generated: `False`",
            "- event_outcome_prediction_allowed: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Source Inventory",
            "",
            "|source_id|event_family|provider_class|pit_status|recommended_usage|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['source_id']}`|`{row['event_family']}`|"
                    f"`{row['provider_class']}`|`{row['pit_status']}`|"
                    f"{row['recommended_usage']}|"
                )
                for row in source_rows
            ],
            "",
            "## Gating Use Cases",
            "",
            "|use_case_id|runtime_status|blocked_event_families|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['use_case_id']}`|`{row['runtime_status']}`|"
                    f"{row['blocked_event_families']}|"
                )
                for row in use_case_rows
            ],
            "",
            "## Boundary",
            "",
            "当前所有 use case 都是 source feasibility / design only。TRADING-2319 之前必须"
            "先补 provider-specific source manifest、event row schema validation、"
            "known-at / available-at timestamp、row count 和 checksum。任何 no-add、"
            "manual review、post-event confirmation、scoring、report、paper-shadow、production "
            "或 broker 使用都需要独立任务和 owner review。",
            "",
        ]
    )


def _common_payload(*, generated_at: datetime, policy_path: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Event Calendar Data Feasibility Audit",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "policy_path": str(policy_path),
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "actual_requested_date_range": "static_feasibility_audit",
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "Static source feasibility audit; no cached market or event rows are read. "
            "Future data-dependent event workflows must run source schema validation "
            "and aits validate-data when cached market data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EventCalendarFeasibilityAuditError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise EventCalendarFeasibilityAuditError(f"policy must be object: {path}")
    return payload


def _validate_policy(policy: Mapping[str, Any]) -> None:
    required_fields = (
        "policy_id",
        "version",
        "status",
        "owner",
        "task_id",
        "market_regime",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
        "data_quality",
        "source_manifest_requirements",
        "event_sources",
        "gating_use_cases",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise EventCalendarFeasibilityAuditError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "event_calendar_feasibility_policy":
        raise EventCalendarFeasibilityAuditError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise EventCalendarFeasibilityAuditError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise EventCalendarFeasibilityAuditError("policy market_regime mismatch")
    data_quality = mapping(policy.get("data_quality"))
    if data_quality.get("status") != DATA_QUALITY_STATUS:
        raise EventCalendarFeasibilityAuditError(
            f"policy data_quality.status must be {DATA_QUALITY_STATUS}"
        )
    manifest = mapping(policy.get("source_manifest_requirements"))
    if manifest.get("required") is not True:
        raise EventCalendarFeasibilityAuditError("source manifest must be required")
    manifest_fields = set(_strings(manifest.get("fields")))
    required_manifest_fields = {
        "provider_name",
        "provider_class",
        "endpoint_or_file",
        "request_parameters",
        "event_time",
        "source_published_time",
        "known_at",
        "available_at",
        "download_timestamp",
        "row_count",
        "checksum",
    }
    if not required_manifest_fields.issubset(manifest_fields):
        raise EventCalendarFeasibilityAuditError("source manifest fields incomplete")

    source_families = {
        str(mapping(source).get("event_family"))
        for source in mapping(policy.get("event_sources")).values()
    }
    if source_families != REQUIRED_EVENT_FAMILIES:
        raise EventCalendarFeasibilityAuditError("event source families mismatch")
    if set(mapping(policy.get("gating_use_cases"))) != REQUIRED_USE_CASES:
        raise EventCalendarFeasibilityAuditError("gating use cases mismatch")
    for source_id, source_value in mapping(policy.get("event_sources")).items():
        source = mapping(source_value)
        for field in (
            "provider_name",
            "provider_class",
            "endpoint_or_file",
            "request_parameters",
            "source_status",
            "pit_status",
            "feasibility_grade",
            "required_timestamps",
            "major_risks",
            "recommended_usage",
        ):
            if not source.get(field):
                raise EventCalendarFeasibilityAuditError(
                    f"event source {source_id} missing {field}"
                )
    for use_case_id, use_case_value in mapping(policy.get("gating_use_cases")).items():
        use_case = mapping(use_case_value)
        required_families = set(_strings(use_case.get("required_event_families")))
        if not required_families or not required_families.issubset(REQUIRED_EVENT_FAMILIES):
            raise EventCalendarFeasibilityAuditError(
                f"use case {use_case_id} has invalid required_event_families"
            )
        if use_case.get("owner_review_required") is not True:
            raise EventCalendarFeasibilityAuditError(
                f"use case {use_case_id} must require owner review"
            )
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise EventCalendarFeasibilityAuditError(
                f"policy safety.{field} must be {expected}"
            )


def _source_blocker(source: Mapping[str, Any]) -> str:
    pit_status = str(source.get("pit_status", ""))
    if "BLOCKED" in pit_status:
        return pit_status.lower()
    return "source_manifest_and_known_at_timestamp_required"


def _joined(value: object) -> str:
    return ",".join(_strings(value))


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value]
    return []


__all__ = [
    "ARTIFACT_ROLE",
    "DATA_QUALITY_STATUS",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "REPORT_TYPE",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "EventCalendarFeasibilityAuditError",
    "build_event_calendar_gating_use_case_matrix",
    "build_event_calendar_known_at_requirement_matrix",
    "build_event_calendar_source_inventory",
    "build_event_calendar_validation_route",
    "run_event_calendar_data_feasibility_audit",
]
