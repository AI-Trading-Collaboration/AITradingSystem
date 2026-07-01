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
    records,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC"
SOURCE_TASK_ID = "TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT"
REPORT_TYPE = "event_calendar_gating_generator_poc"
ARTIFACT_ROLE = "event_calendar_gating_generator_poc_source_blocked"
MODE = "generator_poc"
STATUS = "EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL"
SOURCE_STATUS = "EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_GENERATOR"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "event_calendar_gating_generator_policy.yaml"
)
DEFAULT_FEASIBILITY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "event_calendar_data_feasibility_audit"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_USE_CASES = {
    "pre_event_no_add",
    "post_event_confirmation_window",
    "manual_review_trigger",
    "earnings_cluster_risk",
}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "source_blocked_no_generation": True,
    "event_rows_consumed": False,
    "event_rows_downloaded": False,
    "event_calendar_cache_written": False,
    "gating_generator_executed": False,
    "gating_signal_generated": False,
    "event_gating_signal_series_generated": False,
    "event_outcome_prediction_allowed": False,
    "trading_direction_prediction_allowed": False,
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


class EventCalendarGatingGeneratorPocError(ValueError):
    pass


def run_event_calendar_gating_generator_poc(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    feasibility_dir: Path = DEFAULT_FEASIBILITY_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise EventCalendarGatingGeneratorPocError(
            "event calendar gating generator POC only supports generator_poc mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = load_trading_2318_feasibility_artifacts(feasibility_dir)
    _validate_trading_2318_source(source, policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    common = _common_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        feasibility_dir=feasibility_dir,
        source_summary=source["summary"],
    )
    blocker_rows = build_event_gating_source_blocker_report(source=source)
    readiness_rows = build_event_gating_use_case_readiness_matrix(
        policy=policy,
        source=source,
    )
    inactive_signal_spec = build_event_gating_signal_spec(policy=policy, source=source)
    manual_review_contract = build_event_gating_manual_review_contract(
        source=source,
        generated_at=generated_at,
    )
    validation_summary = build_event_gating_generator_validation_summary(
        blocker_rows=blocker_rows,
        readiness_rows=readiness_rows,
        inactive_signal_spec=inactive_signal_spec,
        source=source,
    )
    safety_boundary = build_event_gating_generator_safety_boundary(
        generated_at=generated_at,
        blocker_rows=blocker_rows,
        readiness_rows=readiness_rows,
    )
    summary = build_event_gating_generator_summary(
        common=common,
        policy=policy,
        source=source,
        blocker_rows=blocker_rows,
        readiness_rows=readiness_rows,
        validation_summary=validation_summary,
    )
    paths = write_event_gating_generator_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        inactive_signal_spec=inactive_signal_spec,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        manual_review_contract=manual_review_contract,
        validation_summary=validation_summary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "event_gating_signal_spec": inactive_signal_spec,
            "use_case_readiness_rows": readiness_rows,
            "source_blocker_rows": blocker_rows,
            "manual_review_trigger_contract": manual_review_contract,
            "validation_summary": validation_summary,
        }
    )


def load_trading_2318_feasibility_artifacts(feasibility_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": feasibility_dir / "event_calendar_data_feasibility_summary.json",
        "source_inventory": feasibility_dir / "event_calendar_source_inventory.json",
        "use_case_matrix": feasibility_dir / "event_calendar_gating_use_case_matrix.json",
        "manual_review_contract": feasibility_dir
        / "event_calendar_manual_review_trigger_contract.json",
        "safety_boundary": feasibility_dir / "event_calendar_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise EventCalendarGatingGeneratorPocError(
            "TRADING-2319 requires TRADING-2318 feasibility outputs: "
            + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def build_event_gating_source_blocker_report(
    *,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in records(mapping(source.get("source_inventory")).get("rows")):
        source_status = str(row.get("source_status", ""))
        pit_status = str(row.get("pit_status", ""))
        rows.append(
            clean_for_yaml(
                {
                    "source_id": row.get("source_id", ""),
                    "event_family": row.get("event_family", ""),
                    "provider_name": row.get("provider_name", ""),
                    "provider_class": row.get("provider_class", ""),
                    "source_status": source_status,
                    "pit_status": pit_status,
                    "blocker_status": "SOURCE_BLOCKED_NO_GENERATOR",
                    "blocker": row.get("blocker", ""),
                    "required_timestamps": row.get("required_timestamps", ""),
                    "required_before_generator": (
                        "provider_specific_source_manifest,event_row_schema,"
                        "known_at_timestamp,available_at_timestamp,row_count,checksum"
                    ),
                    "event_rows_downloaded": False,
                    "gating_signal_generated": False,
                    "reconsideration_condition": (
                        "Rerun TRADING-2318 and TRADING-2319 after this source has "
                        "row-level manifest and known-at validation."
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_gating_use_case_readiness_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy_use_cases = mapping(policy.get("required_use_cases"))
    source_rows = {
        str(row.get("use_case_id")): row
        for row in records(mapping(source.get("use_case_matrix")).get("rows"))
    }
    rows: list[dict[str, Any]] = []
    for use_case_id in sorted(policy_use_cases):
        definition = mapping(policy_use_cases.get(use_case_id))
        source_row = mapping(source_rows.get(use_case_id))
        rows.append(
            clean_for_yaml(
                {
                    "use_case_id": use_case_id,
                    "output_role": definition.get("output_role", ""),
                    "intended_effect": definition.get("intended_effect", ""),
                    "source_runtime_status": source_row.get("runtime_status", ""),
                    "readiness_status": "SOURCE_BLOCKED_NO_GENERATOR",
                    "blocked_event_families": source_row.get("blocked_event_families", ""),
                    "required_before_activation": _joined(
                        definition.get("required_before_activation")
                    ),
                    "event_rows_available": False,
                    "generator_ready": False,
                    "manual_review_contract_only": True,
                    "allowed_current_usage": "source_blocked_design_package_only",
                    "blocked_usage": (
                        "runtime_no_add_gate,post_event_confirmation_signal,"
                        "trading_direction_prediction,position_sizing,promotion,"
                        "paper_shadow,production,broker"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_gating_signal_spec(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    spec = mapping(policy.get("inactive_signal_spec"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.inactive_signal_spec.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY",
            "spec_status": spec.get("spec_status", "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "source_pit_ready_source_count": _summary_value(
                source["summary"], "pit_ready_source_count"
            ),
            "required_event_row_fields": _strings(spec.get("required_event_row_fields")),
            "required_output_fields_after_unblock": _strings(
                spec.get("required_output_fields_after_unblock")
            ),
            "blocked_actions": _strings(spec.get("blocked_actions")),
            "use_cases": sorted(mapping(policy.get("required_use_cases"))),
            "event_rows_consumed": False,
            "event_rows_downloaded": False,
            "event_gating_signal_series_generated": False,
            "executable_signal_ready": False,
            "next_unblock_condition": (
                "All required event families need provider-specific source manifests, "
                "event row schema validation and known_at/available_at timestamps."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_manual_review_contract(
    *,
    source: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    source_contract = mapping(source.get("manual_review_contract"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "SOURCE_BLOCKED_MANUAL_REVIEW_CONTRACT_CARRY_FORWARD",
            "generated_at": generated_at.isoformat(),
            "source_contract_status": source_contract.get("status", ""),
            "source_contract_role": source_contract.get("contract_role", ""),
            "manual_review_only": True,
            "automatic_no_add_allowed": False,
            "automatic_position_change_allowed": False,
            "event_outcome_prediction_allowed": False,
            "required_event_row_fields": source_contract.get("required_event_row_fields", []),
            "allowed_trigger_actions": ["manual_review_only_after_source_unblocked"],
            "blocked_trigger_actions": source_contract.get("blocked_trigger_actions", []),
            "contract_activation_status": "BLOCKED_PENDING_EVENT_SOURCE_MANIFEST",
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_generator_validation_summary(
    *,
    blocker_rows: Sequence[Mapping[str, Any]],
    readiness_rows: Sequence[Mapping[str, Any]],
    inactive_signal_spec: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    checks = [
        {
            "check_id": "source_status_is_expected",
            "status": "PASS",
            "detail": str(_summary_value(source["summary"], "status")),
        },
        {
            "check_id": "pit_ready_source_count_zero_blocks_generator",
            "status": "PASS",
            "detail": str(_summary_value(source["summary"], "pit_ready_source_count")),
        },
        {
            "check_id": "use_cases_all_source_blocked",
            "status": "PASS"
            if all(
                row.get("readiness_status") == "SOURCE_BLOCKED_NO_GENERATOR"
                for row in readiness_rows
            )
            else "FAIL",
            "detail": str(len(readiness_rows)),
        },
        {
            "check_id": "inactive_signal_spec_not_executable",
            "status": "PASS"
            if inactive_signal_spec.get("executable_signal_ready") is False
            else "FAIL",
            "detail": str(inactive_signal_spec.get("spec_status", "")),
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.validation_summary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "PASS_SOURCE_BLOCKED_EXPECTED",
            "source_blocker_count": len(blocker_rows),
            "use_case_readiness_count": len(readiness_rows),
            "check_count": len(checks),
            "failed_check_count": sum(1 for row in checks if row["status"] != "PASS"),
            "checks": checks,
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_generator_safety_boundary(
    *,
    generated_at: datetime,
    blocker_rows: Sequence[Mapping[str, Any]],
    readiness_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "source_blocker_count": len(blocker_rows),
            "use_case_readiness_count": len(readiness_rows),
            "does_not_read_market_cache": True,
            "does_not_download_external_event_rows": True,
            "does_not_write_event_calendar_cache": True,
            "does_not_generate_event_gating_signal": True,
            "does_not_generate_signal_series": True,
            "does_not_predict_event_outcome": True,
            "does_not_predict_trading_direction": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Source-blocked static generator POC consumes only TRADING-2318 "
                "artifacts. Future event-row ingestion, scoring, validation, reports "
                "or backtests must run source schema validation and aits validate-data "
                "when cached market data is consumed."
            ),
            "allowed_next_step": (
                "provide_event_source_manifest_then_rerun_trading_2318_and_2319"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_generator_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    blocker_rows: Sequence[Mapping[str, Any]],
    readiness_rows: Sequence[Mapping[str, Any]],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            **dict(common),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "source_data_quality_status": _summary_value(
                source["summary"], "data_quality_status"
            ),
            "source_pit_ready_source_count": _summary_value(
                source["summary"], "pit_ready_source_count"
            ),
            "source_audit_required_count": _summary_value(
                source["summary"], "source_audit_required_count"
            ),
            "source_blocked_count": _summary_value(source["summary"], "source_blocked_count"),
            "source_blocker_count": len(blocker_rows),
            "use_case_readiness_count": len(readiness_rows),
            "blocked_use_case_count": sum(
                1
                for row in readiness_rows
                if row.get("readiness_status") == "SOURCE_BLOCKED_NO_GENERATOR"
            ),
            "validation_status": validation_summary.get("status", ""),
            "generator_poc_cli_implemented": True,
            "executable_generator_ready": False,
            "signal_spec_status": "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY",
            "recommended_next_action": (
                "PROVIDE_EVENT_SOURCE_MANIFEST_AND_RERUN_TRADING_2318_BEFORE_GENERATOR"
            ),
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_event_gating_generator_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    inactive_signal_spec: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    manual_review_contract: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "event_calendar_gating_generator_summary.json",
        "signal_spec": output_dir / "event_gating_signal_spec.json",
        "use_case_readiness_json": output_dir
        / "event_gating_use_case_readiness_matrix.json",
        "use_case_readiness_csv": output_dir
        / "event_gating_use_case_readiness_matrix.csv",
        "source_blocker_json": output_dir / "event_gating_source_blocker_report.json",
        "source_blocker_csv": output_dir / "event_gating_source_blocker_report.csv",
        "manual_review_contract": output_dir
        / "event_gating_manual_review_trigger_contract.json",
        "validation_summary": output_dir / "event_gating_generator_validation_summary.json",
        "safety_boundary": output_dir / "event_gating_generator_safety_boundary.json",
        "report_doc": docs_root / "event_calendar_gating_generator_poc.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["signal_spec"], dict(inactive_signal_spec))
    write_json(paths["use_case_readiness_json"], {**dict(common), "rows": readiness_rows})
    write_csv_rows(paths["use_case_readiness_csv"], readiness_rows)
    write_json(paths["source_blocker_json"], {**dict(common), "rows": blocker_rows})
    write_csv_rows(paths["source_blocker_csv"], blocker_rows)
    write_json(paths["manual_review_contract"], dict(manual_review_contract))
    write_json(paths["validation_summary"], dict(validation_summary))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            readiness_rows=readiness_rows,
            blocker_rows=blocker_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Event Calendar Gating Generator POC",
            "",
            "TRADING-2319 承接 TRADING-2318，但当前没有 PIT-ready event source。"
            "本报告是 source-blocked generator POC package，不下载 event rows，不生成 "
            "gating signal，不预测事件结果或交易方向，不进入仓位、paper-shadow、"
            "production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- source_pit_ready_source_count: `{summary['source_pit_ready_source_count']}`",
            f"- source_blocker_count: `{summary['source_blocker_count']}`",
            f"- use_case_readiness_count: `{summary['use_case_readiness_count']}`",
            f"- blocked_use_case_count: `{summary['blocked_use_case_count']}`",
            f"- executable_generator_ready: `{summary['executable_generator_ready']}`",
            f"- signal_spec_status: `{summary['signal_spec_status']}`",
            "- event_rows_consumed: `False`",
            "- gating_signal_generated: `False`",
            "- event_gating_signal_series_generated: `False`",
            "- event_outcome_prediction_allowed: `False`",
            "- trading_direction_prediction_allowed: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Use-Case Readiness",
            "",
            "|use_case_id|readiness_status|blocked_event_families|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['use_case_id']}`|`{row['readiness_status']}`|"
                    f"{row['blocked_event_families']}|"
                )
                for row in readiness_rows
            ],
            "",
            "## Source Blockers",
            "",
            "|source_id|event_family|pit_status|blocker_status|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['source_id']}`|`{row['event_family']}`|"
                    f"`{row['pit_status']}`|`{row['blocker_status']}`|"
                )
                for row in blocker_rows
            ],
            "",
            "## Boundary",
            "",
            "退出 source-blocked 状态的条件是补齐 provider-specific source manifest、"
            "event row schema、known_at / available_at timestamp、row count 和 checksum，"
            "然后重新运行 TRADING-2318 和 TRADING-2319。当前不得把 inactive spec "
            "接入 no-add、manual review、post-event confirmation、scoring、report、"
            "paper-shadow、production 或 broker workflow。",
            "",
        ]
    )


def _common_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    feasibility_dir: Path,
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Event Calendar Gating Generator POC",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "policy_path": str(policy_path),
        "feasibility_dir": str(feasibility_dir),
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "actual_requested_date_range": "source_blocked_static_generator_poc",
        "data_quality_status": DATA_QUALITY_STATUS,
        "source_data_quality_status": _summary_value(source_summary, "data_quality_status"),
        "data_quality_requirement": (
            "Source-blocked static generator POC; no cached market data or event "
            "rows are read. Future data-dependent event workflows must run source "
            "schema validation and aits validate-data when cached market data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _validate_trading_2318_source(
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = source["summary"]
    source_dependency = mapping(policy.get("source_dependency"))
    if _summary_value(summary, "task_id") != source_dependency.get("required_task_id"):
        raise EventCalendarGatingGeneratorPocError("TRADING-2318 source task_id mismatch")
    if _summary_value(summary, "status") != source_dependency.get("required_status"):
        raise EventCalendarGatingGeneratorPocError("TRADING-2318 source status mismatch")
    if _summary_value(summary, "data_quality_status") != source_dependency.get(
        "required_data_quality_status"
    ):
        raise EventCalendarGatingGeneratorPocError(
            "TRADING-2318 source data_quality_status mismatch"
        )
    min_ready = int(source_dependency.get("minimum_pit_ready_sources_for_executable_generator", 1))
    pit_ready = int(_summary_value(summary, "pit_ready_source_count") or 0)
    if pit_ready >= min_ready:
        raise EventCalendarGatingGeneratorPocError(
            "TRADING-2318 has PIT-ready sources; use an executable generator policy instead"
        )
    if source_dependency.get("allow_source_blocked_package") is not True:
        raise EventCalendarGatingGeneratorPocError("source-blocked package is not allowed")
    for field in (
        "event_rows_downloaded",
        "event_calendar_cache_written",
        "gating_signal_generated",
        "event_outcome_prediction_allowed",
        "candidate_artifact_generated",
        "actual_path_validation_executed",
        "paper_shadow_allowed",
        "production_allowed",
    ):
        if _summary_value(summary, field) not in {False, None}:
            raise EventCalendarGatingGeneratorPocError(
                f"TRADING-2318 source safety.{field} must be false"
            )
    source_rows = records(mapping(source.get("source_inventory")).get("rows"))
    use_case_rows = records(mapping(source.get("use_case_matrix")).get("rows"))
    if not source_rows:
        raise EventCalendarGatingGeneratorPocError("TRADING-2318 source inventory empty")
    if {str(row.get("use_case_id")) for row in use_case_rows} != REQUIRED_USE_CASES:
        raise EventCalendarGatingGeneratorPocError("TRADING-2318 use cases mismatch")


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
        "source_dependency",
        "required_use_cases",
        "inactive_signal_spec",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise EventCalendarGatingGeneratorPocError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "event_calendar_gating_generator_policy":
        raise EventCalendarGatingGeneratorPocError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise EventCalendarGatingGeneratorPocError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise EventCalendarGatingGeneratorPocError("policy market_regime mismatch")
    if set(mapping(policy.get("required_use_cases"))) != REQUIRED_USE_CASES:
        raise EventCalendarGatingGeneratorPocError("policy required use cases mismatch")
    signal_spec = mapping(policy.get("inactive_signal_spec"))
    for key in (
        "required_event_row_fields",
        "required_output_fields_after_unblock",
        "blocked_actions",
    ):
        if not _strings(signal_spec.get(key)):
            raise EventCalendarGatingGeneratorPocError(f"inactive_signal_spec.{key} required")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise EventCalendarGatingGeneratorPocError(
                f"policy safety.{field} must be {expected}"
            )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EventCalendarGatingGeneratorPocError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise EventCalendarGatingGeneratorPocError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EventCalendarGatingGeneratorPocError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise EventCalendarGatingGeneratorPocError(f"JSON must be object: {path}")
    return payload


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    return mapping(payload.get("summary")).get(key)


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
    "DEFAULT_FEASIBILITY_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "REPORT_TYPE",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "EventCalendarGatingGeneratorPocError",
    "build_event_gating_source_blocker_report",
    "build_event_gating_signal_spec",
    "build_event_gating_use_case_readiness_matrix",
    "load_trading_2318_feasibility_artifacts",
    "run_event_calendar_gating_generator_poc",
]
