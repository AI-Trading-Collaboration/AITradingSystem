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

TASK_ID = "TRADING-2320_EVENT_GATING_VALIDATION"
SOURCE_TASK_ID = "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC"
REPORT_TYPE = "event_gating_validation"
ARTIFACT_ROLE = "event_gating_validation_source_blocked"
MODE = "validation_readiness"
STATUS = "EVENT_GATING_VALIDATION_SOURCE_BLOCKED_NOT_EXECUTED"
SOURCE_STATUS = "EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL"
SOURCE_DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_GENERATOR"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_VALIDATION"
SOURCE_SIGNAL_SPEC_STATUS = "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
OBJECTIVE_STATUS = "SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED"

DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "event_gating_validation_policy.yaml"
DEFAULT_GENERATOR_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "event_calendar_gating_generator_poc"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_OBJECTIVES = {
    "pre_event_false_risk_on",
    "event_window_overtrading",
    "earnings_cluster_exposure_risk",
}

REQUIRED_USE_CASES = {
    "pre_event_no_add",
    "post_event_confirmation_window",
    "manual_review_trigger",
    "earnings_cluster_risk",
}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "source_blocked_no_validation": True,
    "event_rows_consumed": False,
    "event_rows_downloaded": False,
    "event_calendar_cache_written": False,
    "gating_signal_consumed": False,
    "event_gating_signal_series_consumed": False,
    "market_data_consumed": False,
    "turnover_records_consumed": False,
    "exposure_records_consumed": False,
    "event_gating_validation_executed": False,
    "validation_result_generated": False,
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


class EventGatingValidationError(ValueError):
    pass


def run_event_gating_validation(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    generator_dir: Path = DEFAULT_GENERATOR_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise EventGatingValidationError(
            "event gating validation only supports validation_readiness mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = load_trading_2319_gating_generator_artifacts(generator_dir)
    _validate_trading_2319_source(source, policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    common = _common_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        generator_dir=generator_dir,
        source_summary=source["summary"],
    )
    readiness_rows = build_event_gating_validation_readiness_matrix(
        policy=policy,
        source=source,
    )
    data_requirement_rows = build_event_gating_validation_data_requirement_matrix(
        policy=policy,
        source=source,
    )
    blocker_rows = build_event_gating_validation_blocker_report(
        readiness_rows=readiness_rows,
        data_requirement_rows=data_requirement_rows,
        source=source,
    )
    metric_contract = build_event_gating_validation_metric_contract(
        policy=policy,
        source=source,
        readiness_rows=readiness_rows,
    )
    safety_boundary = build_event_gating_validation_safety_boundary(
        generated_at=generated_at,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        data_requirement_rows=data_requirement_rows,
    )
    summary = build_event_gating_validation_summary(
        common=common,
        policy=policy,
        source=source,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        data_requirement_rows=data_requirement_rows,
        metric_contract=metric_contract,
    )
    paths = write_event_gating_validation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        metric_contract=metric_contract,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        data_requirement_rows=data_requirement_rows,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "metric_contract": metric_contract,
            "readiness_rows": readiness_rows,
            "blocker_rows": blocker_rows,
            "data_requirement_rows": data_requirement_rows,
            "safety_boundary": safety_boundary,
        }
    )


def load_trading_2319_gating_generator_artifacts(generator_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": generator_dir / "event_calendar_gating_generator_summary.json",
        "signal_spec": generator_dir / "event_gating_signal_spec.json",
        "use_case_readiness": generator_dir / "event_gating_use_case_readiness_matrix.json",
        "source_blocker": generator_dir / "event_gating_source_blocker_report.json",
        "manual_review_contract": generator_dir
        / "event_gating_manual_review_trigger_contract.json",
        "validation_summary": generator_dir / "event_gating_generator_validation_summary.json",
        "safety_boundary": generator_dir / "event_gating_generator_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise EventGatingValidationError(
            "TRADING-2320 requires TRADING-2319 generator POC outputs: "
            + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def build_event_gating_validation_readiness_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_use_cases = {
        str(row.get("use_case_id")): row
        for row in records(mapping(source.get("use_case_readiness")).get("rows"))
    }
    rows: list[dict[str, Any]] = []
    for objective_id in sorted(REQUIRED_OBJECTIVES):
        objective = mapping(mapping(policy.get("validation_objectives")).get(objective_id))
        required_use_cases = _strings(objective.get("required_gating_use_cases"))
        use_case_statuses = {
            use_case_id: str(mapping(source_use_cases.get(use_case_id)).get("readiness_status"))
            for use_case_id in required_use_cases
        }
        rows.append(
            clean_for_yaml(
                {
                    "validation_objective": objective_id,
                    "target_question": objective.get("target_question", ""),
                    "readiness_status": OBJECTIVE_STATUS,
                    "validation_ready": False,
                    "validation_executed": False,
                    "effect_claim_allowed": False,
                    "required_gating_use_cases": ",".join(required_use_cases),
                    "source_use_case_statuses": use_case_statuses,
                    "required_data": ",".join(_strings(objective.get("required_data"))),
                    "blocked_reason": objective.get("blocked_reason", ""),
                    "required_before_validation": (
                        "pit_event_rows_with_known_at,executable_gating_signal_series,"
                        "event_window_outcomes,turnover_records,exposure_records,"
                        "cached_data_quality_gate"
                    ),
                    "allowed_current_usage": "source_blocked_validation_readiness_only",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_gating_validation_data_requirement_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_summary = mapping(source.get("summary"))
    rows: list[dict[str, Any]] = []
    for objective_id in sorted(REQUIRED_OBJECTIVES):
        objective = mapping(mapping(policy.get("validation_objectives")).get(objective_id))
        for requirement_id in _strings(objective.get("required_data")):
            rows.append(
                clean_for_yaml(
                    {
                        "validation_objective": objective_id,
                        "requirement_id": requirement_id,
                        "requirement_status": "MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED",
                        "current_source_status": _summary_value(source_summary, "status"),
                        "source_event_rows_consumed": _summary_value(
                            source_summary,
                            "event_rows_consumed",
                        ),
                        "source_gating_signal_generated": _summary_value(
                            source_summary,
                            "gating_signal_generated",
                        ),
                        "source_signal_series_generated": _summary_value(
                            source_summary,
                            "event_gating_signal_series_generated",
                        ),
                        "validation_consumed": False,
                        "blocked_reason": objective.get("blocked_reason", ""),
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_event_gating_validation_blocker_report(
    *,
    readiness_rows: Sequence[Mapping[str, Any]],
    data_requirement_rows: Sequence[Mapping[str, Any]],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    upstream_source_blockers = records(mapping(source.get("source_blocker")).get("rows"))
    rows: list[dict[str, Any]] = []
    for row in readiness_rows:
        rows.append(
            clean_for_yaml(
                {
                    "blocker_scope": "validation_objective",
                    "validation_objective": row.get("validation_objective", ""),
                    "blocker_status": OBJECTIVE_STATUS,
                    "blocker": row.get("blocked_reason", ""),
                    "upstream_source_status": _summary_value(source["summary"], "status"),
                    "upstream_source_blocker_count": len(upstream_source_blockers),
                    "required_before_unblock": row.get("required_before_validation", ""),
                    **SAFETY_FIELDS,
                }
            )
        )
    for row in data_requirement_rows:
        rows.append(
            clean_for_yaml(
                {
                    "blocker_scope": "data_requirement",
                    "validation_objective": row.get("validation_objective", ""),
                    "requirement_id": row.get("requirement_id", ""),
                    "blocker_status": OBJECTIVE_STATUS,
                    "blocker": row.get("blocked_reason", ""),
                    "upstream_source_status": _summary_value(source["summary"], "status"),
                    "upstream_source_blocker_count": len(upstream_source_blockers),
                    "required_before_unblock": row.get("requirement_id", ""),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_event_gating_validation_metric_contract(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contract = mapping(policy.get("metric_contract"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.metric_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": contract.get("contract_status", "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "source_signal_spec_status": _summary_value(source["signal_spec"], "spec_status"),
            "validation_objectives": sorted(REQUIRED_OBJECTIVES),
            "validation_objective_count": len(readiness_rows),
            "comparison_windows": _strings(contract.get("comparison_windows")),
            "required_output_fields_after_unblock": _strings(
                contract.get("required_output_fields_after_unblock")
            ),
            "blocked_actions": _strings(contract.get("blocked_actions")),
            "metric_result_generated": False,
            "effect_claim_generated": False,
            "executable_validation_ready": False,
            "next_unblock_condition": (
                "Rerun TRADING-2318 and TRADING-2319 with provider-specific "
                "event rows, known-at validation and executable gating signal series."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_validation_safety_boundary(
    *,
    generated_at: datetime,
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    data_requirement_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "validation_objective_count": len(readiness_rows),
            "blocked_objective_count": len(readiness_rows),
            "blocker_count": len(blocker_rows),
            "data_requirement_count": len(data_requirement_rows),
            "does_not_read_market_cache": True,
            "does_not_read_event_rows": True,
            "does_not_read_turnover_records": True,
            "does_not_read_exposure_records": True,
            "does_not_execute_event_gating_validation": True,
            "does_not_generate_effect_claim": True,
            "does_not_predict_event_outcome": True,
            "does_not_predict_trading_direction": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Source-blocked static validation package consumes only TRADING-2319 "
                "artifacts. Future event validation must run provider source schema "
                "validation and aits validate-data when cached market data is consumed."
            ),
            "allowed_next_step": (
                "provide_event_source_manifest_and_executable_gating_signal_before_validation"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_event_gating_validation_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    data_requirement_rows: Sequence[Mapping[str, Any]],
    metric_contract: Mapping[str, Any],
) -> dict[str, Any]:
    source_summary = mapping(source.get("summary"))
    return clean_for_yaml(
        {
            **dict(common),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source_summary, "status"),
            "source_data_quality_status": _summary_value(
                source_summary,
                "data_quality_status",
            ),
            "source_signal_spec_status": _summary_value(source["signal_spec"], "spec_status"),
            "source_gating_signal_generated": _summary_value(
                source_summary,
                "gating_signal_generated",
            ),
            "source_event_gating_signal_series_generated": _summary_value(
                source_summary,
                "event_gating_signal_series_generated",
            ),
            "source_event_rows_consumed": _summary_value(source_summary, "event_rows_consumed"),
            "validation_objective_count": len(readiness_rows),
            "blocked_objective_count": sum(
                1 for row in readiness_rows if row.get("readiness_status") == OBJECTIVE_STATUS
            ),
            "blocker_count": len(blocker_rows),
            "data_requirement_count": len(data_requirement_rows),
            "metric_contract_status": metric_contract.get("status", ""),
            "validation_status": "PASS_SOURCE_BLOCKED_EXPECTED",
            "event_gating_validation_cli_implemented": True,
            "executable_validation_ready": False,
            "recommended_next_action": (
                "PROVIDE_EVENT_SOURCE_MANIFEST_AND_EXECUTABLE_GATING_SIGNAL_BEFORE_VALIDATION"
            ),
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_event_gating_validation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    metric_contract: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    data_requirement_rows: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "event_gating_validation_summary.json",
        "metric_contract": output_dir / "event_gating_validation_metric_contract.json",
        "readiness_json": output_dir / "event_gating_validation_readiness_matrix.json",
        "readiness_csv": output_dir / "event_gating_validation_readiness_matrix.csv",
        "blocker_json": output_dir / "event_gating_validation_blocker_report.json",
        "blocker_csv": output_dir / "event_gating_validation_blocker_report.csv",
        "data_requirement_json": output_dir
        / "event_gating_validation_data_requirement_matrix.json",
        "data_requirement_csv": output_dir
        / "event_gating_validation_data_requirement_matrix.csv",
        "safety_boundary": output_dir / "event_gating_validation_safety_boundary.json",
        "report_doc": docs_root / "event_gating_validation.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["metric_contract"], dict(metric_contract))
    write_json(paths["readiness_json"], {**dict(common), "rows": readiness_rows})
    write_csv_rows(paths["readiness_csv"], readiness_rows)
    write_json(paths["blocker_json"], {**dict(common), "rows": blocker_rows})
    write_csv_rows(paths["blocker_csv"], blocker_rows)
    write_json(paths["data_requirement_json"], {**dict(common), "rows": data_requirement_rows})
    write_csv_rows(paths["data_requirement_csv"], data_requirement_rows)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            readiness_rows=readiness_rows,
            data_requirement_rows=data_requirement_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    data_requirement_rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Event Gating Validation",
            "",
            "TRADING-2320 承接 TRADING-2319，但当前没有 executable event gating "
            "signal 或 event gating signal series。本报告是 source-blocked validation "
            "readiness package，不读取 event rows、market data、turnover records 或 "
            "exposure records，不执行 event gating validation，不生成效果结论，不进入 "
            "paper-shadow、production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- source_signal_spec_status: `{summary['source_signal_spec_status']}`",
            f"- validation_objective_count: `{summary['validation_objective_count']}`",
            f"- blocked_objective_count: `{summary['blocked_objective_count']}`",
            f"- data_requirement_count: `{summary['data_requirement_count']}`",
            f"- executable_validation_ready: `{summary['executable_validation_ready']}`",
            "- event_rows_consumed: `False`",
            "- gating_signal_consumed: `False`",
            "- event_gating_signal_series_consumed: `False`",
            "- market_data_consumed: `False`",
            "- event_gating_validation_executed: `False`",
            "- validation_result_generated: `False`",
            "- event_outcome_prediction_allowed: `False`",
            "- trading_direction_prediction_allowed: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Objective Readiness",
            "",
            "|validation_objective|readiness_status|blocked_reason|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['validation_objective']}`|`{row['readiness_status']}`|"
                    f"{row['blocked_reason']}|"
                )
                for row in readiness_rows
            ],
            "",
            "## Data Requirements",
            "",
            "|validation_objective|requirement_id|requirement_status|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['validation_objective']}`|`{row['requirement_id']}`|"
                    f"`{row['requirement_status']}`|"
                )
                for row in data_requirement_rows
            ],
            "",
            "## Boundary",
            "",
            "退出 source-blocked validation 状态的条件是补齐 provider-specific event "
            "source manifest、PIT event rows、known_at / available_at timestamp、"
            "executable gating signal series、event-window outcome / turnover / exposure "
            "records，并在读取 cached market data 的 validation workflow 中执行 "
            "`aits validate-data` 或同源 data-quality gate。当前不得把本报告解读为 "
            "event gating 有效性结论。",
            "",
        ]
    )


def _common_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    generator_dir: Path,
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Event Gating Validation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "policy_path": str(policy_path),
        "generator_dir": str(generator_dir),
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "actual_requested_date_range": "source_blocked_static_validation_package",
        "data_quality_status": DATA_QUALITY_STATUS,
        "source_data_quality_status": _summary_value(source_summary, "data_quality_status"),
        "data_quality_requirement": (
            "Source-blocked static validation package; no cached market data, event "
            "rows, turnover records or exposure records are read. Future event "
            "validation must run provider source schema validation and aits "
            "validate-data when cached market data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _validate_trading_2319_source(
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = source["summary"]
    source_dependency = mapping(policy.get("source_dependency"))
    if _summary_value(summary, "task_id") != source_dependency.get("required_task_id"):
        raise EventGatingValidationError("TRADING-2319 source task_id mismatch")
    if _summary_value(summary, "status") != source_dependency.get("required_status"):
        raise EventGatingValidationError("TRADING-2319 source status mismatch")
    if _summary_value(summary, "data_quality_status") != source_dependency.get(
        "required_data_quality_status"
    ):
        raise EventGatingValidationError("TRADING-2319 source data_quality_status mismatch")
    if _summary_value(source["signal_spec"], "spec_status") != source_dependency.get(
        "required_signal_spec_status"
    ):
        raise EventGatingValidationError("TRADING-2319 signal_spec status mismatch")
    if source_dependency.get("allow_source_blocked_package") is not True:
        raise EventGatingValidationError("source-blocked validation package is not allowed")
    if _summary_value(summary, "gating_signal_generated") is not False:
        raise EventGatingValidationError(
            "TRADING-2319 generated a gating signal; use executable validation policy"
        )
    if _summary_value(summary, "event_gating_signal_series_generated") is not False:
        raise EventGatingValidationError(
            "TRADING-2319 generated a signal series; use executable validation policy"
        )
    for field in (
        "event_rows_consumed",
        "event_rows_downloaded",
        "event_calendar_cache_written",
        "event_outcome_prediction_allowed",
        "trading_direction_prediction_allowed",
        "candidate_artifact_generated",
        "actual_path_validation_executed",
        "paper_shadow_allowed",
        "production_allowed",
    ):
        if _summary_value(summary, field) not in {False, None}:
            raise EventGatingValidationError(
                f"TRADING-2319 source safety.{field} must be false"
            )
    if _summary_value(source["signal_spec"], "executable_signal_ready") is not False:
        raise EventGatingValidationError("TRADING-2319 signal spec must be inactive")
    use_case_rows = records(mapping(source.get("use_case_readiness")).get("rows"))
    if {str(row.get("use_case_id")) for row in use_case_rows} != REQUIRED_USE_CASES:
        raise EventGatingValidationError("TRADING-2319 use cases mismatch")
    if any(row.get("readiness_status") != "SOURCE_BLOCKED_NO_GENERATOR" for row in use_case_rows):
        raise EventGatingValidationError(
            "TRADING-2319 use cases are no longer source-blocked; use executable validation"
        )
    if not records(mapping(source.get("source_blocker")).get("rows")):
        raise EventGatingValidationError("TRADING-2319 source blocker report empty")


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
        "validation_objectives",
        "metric_contract",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise EventGatingValidationError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "event_gating_validation_policy":
        raise EventGatingValidationError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise EventGatingValidationError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise EventGatingValidationError("policy market_regime mismatch")
    objectives = mapping(policy.get("validation_objectives"))
    if set(objectives) != REQUIRED_OBJECTIVES:
        raise EventGatingValidationError("policy validation objectives mismatch")
    for objective_id in REQUIRED_OBJECTIVES:
        objective = mapping(objectives.get(objective_id))
        if not _strings(objective.get("required_gating_use_cases")):
            raise EventGatingValidationError(f"{objective_id} required_gating_use_cases empty")
        if not _strings(objective.get("required_data")):
            raise EventGatingValidationError(f"{objective_id} required_data empty")
        if not objective.get("blocked_reason"):
            raise EventGatingValidationError(f"{objective_id} blocked_reason required")
    contract = mapping(policy.get("metric_contract"))
    for key in (
        "comparison_windows",
        "required_output_fields_after_unblock",
        "blocked_actions",
    ):
        if not _strings(contract.get(key)):
            raise EventGatingValidationError(f"metric_contract.{key} required")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise EventGatingValidationError(
                f"policy safety.{field} must be {expected}"
            )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EventGatingValidationError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise EventGatingValidationError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EventGatingValidationError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise EventGatingValidationError(f"JSON must be object: {path}")
    return payload


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    return mapping(payload.get("summary")).get(key)


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
    "DEFAULT_GENERATOR_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "OBJECTIVE_STATUS",
    "REPORT_TYPE",
    "REQUIRED_OBJECTIVES",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "EventGatingValidationError",
    "build_event_gating_validation_blocker_report",
    "build_event_gating_validation_data_requirement_matrix",
    "build_event_gating_validation_metric_contract",
    "build_event_gating_validation_readiness_matrix",
    "load_trading_2319_gating_generator_artifacts",
    "run_event_gating_validation",
]
