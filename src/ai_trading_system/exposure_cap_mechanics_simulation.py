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
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    RISK_CAP_CANDIDATE_ID,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SIGNAL_VALIDITY_AGING_ROOT,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    REQUIRED_LIFECYCLE_FIELDS as SOURCE_LIFECYCLE_FIELDS,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    STATUS as SOURCE_STATUS,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    TASK_ID as SOURCE_TASK_ID,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION"
REPORT_TYPE = "exposure_cap_mechanics_simulation"
ARTIFACT_ROLE = "exposure_cap_mechanics_simulation_source_blocked"
MODE = "simulation_readiness"
STATUS = "EXPOSURE_CAP_MECHANICS_SIMULATION_SOURCE_BLOCKED_NOT_EXECUTED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_SIMULATION"
OBJECTIVE_STATUS = "SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "exposure_cap_mechanics_simulation_policy.yaml"
)
DEFAULT_SOURCE_ROOT = DEFAULT_SIGNAL_VALIDITY_AGING_ROOT
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_SIMULATION_OBJECTIVES = (
    "max_exposure_change_after_risk_cap_trigger",
    "cooldown_turnover_effect",
    "release_restore_after_risk_cap_clear",
    "false_risk_cap_cost",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "source_blocked_no_simulation": True,
    "simulation_runtime_started": False,
    "simulation_executed": False,
    "simulation_result_generated": False,
    "metric_result_generated": False,
    "market_data_consumed": False,
    "cached_market_data_consumed": False,
    "runtime_records_consumed": False,
    "portfolio_weights_read": False,
    "portfolio_weights_written": False,
    "portfolio_exposure_history_consumed": False,
    "turnover_records_consumed": False,
    "post_trigger_return_outcomes_consumed": False,
    "release_restore_records_consumed": False,
    "target_weight_generated": False,
    "max_exposure_number_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
    "execution_runtime_started": False,
    "forward_observe_started": False,
    "runtime_started": False,
    "aging_runtime_started": False,
    "signal_validity_runtime_started": False,
    "validity_runtime_executable": False,
    "exposure_cap_policy_executable": False,
    "cooldown_turnover_effect_claim_generated": False,
    "false_risk_cap_cost_claim_generated": False,
    "release_restore_effect_claim_generated": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class ExposureCapMechanicsSimulationError(ValueError):
    pass


def run_exposure_cap_mechanics_simulation(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    source_dir: Path = DEFAULT_SOURCE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise ExposureCapMechanicsSimulationError(
            "exposure-cap mechanics simulation only supports simulation_readiness mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = load_trading_2322_signal_validity_aging_runtime_artifacts(source_dir)
    _validate_trading_2322_source(source, policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    common = _common_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        source_dir=source_dir,
        source_summary=source["summary"],
    )
    readiness_rows = build_exposure_cap_simulation_readiness_matrix(
        policy=policy,
        source=source,
    )
    input_requirement_rows = build_exposure_cap_simulation_input_requirement_matrix(
        policy=policy,
        source=source,
    )
    blocker_rows = build_exposure_cap_simulation_blocker_report(
        readiness_rows=readiness_rows,
        input_requirement_rows=input_requirement_rows,
        source=source,
    )
    metric_contract = build_exposure_cap_simulation_metric_contract(
        policy=policy,
        source=source,
        readiness_rows=readiness_rows,
    )
    safety_boundary = build_exposure_cap_simulation_safety_boundary(
        generated_at=generated_at,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        input_requirement_rows=input_requirement_rows,
    )
    summary = build_exposure_cap_mechanics_simulation_summary(
        common=common,
        policy=policy,
        source=source,
        readiness_rows=readiness_rows,
        blocker_rows=blocker_rows,
        input_requirement_rows=input_requirement_rows,
        metric_contract=metric_contract,
    )
    paths = write_exposure_cap_mechanics_simulation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        metric_contract=metric_contract,
        readiness_rows=readiness_rows,
        input_requirement_rows=input_requirement_rows,
        blocker_rows=blocker_rows,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "metric_contract": metric_contract,
            "readiness_rows": readiness_rows,
            "input_requirement_rows": input_requirement_rows,
            "blocker_rows": blocker_rows,
            "safety_boundary": safety_boundary,
        }
    )


def load_trading_2322_signal_validity_aging_runtime_artifacts(
    source_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": source_dir / "signal_validity_aging_runtime_design_summary.json",
        "lifecycle_contract": source_dir / "signal_validity_lifecycle_contract.json",
        "aging_rules": source_dir / "signal_validity_aging_rule_matrix.json",
        "trigger_aging": source_dir / "signal_validity_trigger_aging_state_matrix.json",
        "release_restore": source_dir / "signal_validity_release_restore_rule_matrix.json",
        "runtime_schema": source_dir / "signal_validity_runtime_record_schema.json",
        "safety_boundary": source_dir / "signal_validity_aging_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ExposureCapMechanicsSimulationError(
            "TRADING-2323 requires TRADING-2322 signal validity / aging outputs: "
            + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def build_exposure_cap_simulation_readiness_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    objectives = mapping(policy.get("simulation_objectives"))
    source_summary = source["summary"]
    rows: list[dict[str, Any]] = []
    for objective_id in REQUIRED_SIMULATION_OBJECTIVES:
        objective = mapping(objectives.get(objective_id))
        rows.append(
            clean_for_yaml(
                {
                    "simulation_objective": objective_id,
                    "target_question": objective.get("target_question", ""),
                    "readiness_status": OBJECTIVE_STATUS,
                    "simulation_ready": False,
                    "simulation_executed": False,
                    "effect_claim_allowed": False,
                    "required_runtime_inputs": ",".join(
                        _strings(objective.get("required_runtime_inputs"))
                    ),
                    "blocked_reason": objective.get("blocked_reason", ""),
                    "source_task_id": SOURCE_TASK_ID,
                    "source_status": _summary_value(source_summary, "status"),
                    "source_design_only": _summary_value(source_summary, "design_only"),
                    "source_runtime_started": _summary_value(
                        source_summary,
                        "runtime_started",
                    ),
                    "source_aging_runtime_started": _summary_value(
                        source_summary,
                        "aging_runtime_started",
                    ),
                    "required_before_simulation": (
                        "runtime_observe_records,calibrated_cap_multiplier_policy,"
                        "portfolio_exposure_history,turnover_history,"
                        "post_trigger_outcomes,release_restore_decisions,"
                        "cached_data_quality_gate"
                    ),
                    "allowed_current_usage": "source_blocked_simulation_readiness_only",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_exposure_cap_simulation_input_requirement_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    objectives = mapping(policy.get("simulation_objectives"))
    source_summary = source["summary"]
    rows: list[dict[str, Any]] = []
    for objective_id in REQUIRED_SIMULATION_OBJECTIVES:
        objective = mapping(objectives.get(objective_id))
        for requirement_id in _strings(objective.get("required_runtime_inputs")):
            rows.append(
                clean_for_yaml(
                    {
                        "simulation_objective": objective_id,
                        "requirement_id": requirement_id,
                        "requirement_status": "MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED",
                        "current_source_status": _summary_value(
                            source_summary,
                            "status",
                        ),
                        "source_runtime_started": _summary_value(
                            source_summary,
                            "runtime_started",
                        ),
                        "source_aging_runtime_started": _summary_value(
                            source_summary,
                            "aging_runtime_started",
                        ),
                        "source_runtime_schema_required_field_count": len(
                            _strings(source["runtime_schema"].get("required_fields"))
                        ),
                        "simulation_consumed": False,
                        "blocked_reason": objective.get("blocked_reason", ""),
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_exposure_cap_simulation_blocker_report(
    *,
    readiness_rows: Sequence[Mapping[str, Any]],
    input_requirement_rows: Sequence[Mapping[str, Any]],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in readiness_rows:
        rows.append(
            clean_for_yaml(
                {
                    "blocker_scope": "simulation_objective",
                    "simulation_objective": row.get("simulation_objective", ""),
                    "blocker_status": OBJECTIVE_STATUS,
                    "blocker": row.get("blocked_reason", ""),
                    "upstream_source_status": _summary_value(
                        source["summary"],
                        "status",
                    ),
                    "required_before_unblock": row.get(
                        "required_before_simulation",
                        "",
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    for row in input_requirement_rows:
        rows.append(
            clean_for_yaml(
                {
                    "blocker_scope": "input_requirement",
                    "simulation_objective": row.get("simulation_objective", ""),
                    "requirement_id": row.get("requirement_id", ""),
                    "blocker_status": OBJECTIVE_STATUS,
                    "blocker": row.get("blocked_reason", ""),
                    "upstream_source_status": _summary_value(
                        source["summary"],
                        "status",
                    ),
                    "required_before_unblock": row.get("requirement_id", ""),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_exposure_cap_simulation_metric_contract(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contract = mapping(policy.get("metric_contract"))
    metrics = mapping(contract.get("metrics"))
    metric_rows: list[dict[str, Any]] = []
    for metric_id in sorted(metrics):
        metric = mapping(metrics.get(metric_id))
        metric_rows.append(
            clean_for_yaml(
                {
                    "metric_id": metric_id,
                    "simulation_objective": metric.get("simulation_objective", ""),
                    "metric_definition": metric.get("metric_definition", ""),
                    "required_output_fields_after_unblock": _strings(
                        metric.get("required_output_fields_after_unblock")
                    ),
                    "result_generated_now": False,
                    "effect_claim_generated": False,
                    "metric_ready": False,
                }
            )
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.metric_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": contract.get("contract_status", ""),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "source_artifact_role": _summary_value(source["summary"], "artifact_role"),
            "simulation_objectives": list(REQUIRED_SIMULATION_OBJECTIVES),
            "simulation_objective_count": len(readiness_rows),
            "metric_count": len(metric_rows),
            "metrics": metric_rows,
            "blocked_actions": _strings(contract.get("blocked_actions")),
            "metric_result_generated": False,
            "effect_claim_generated": False,
            "executable_simulation_ready": False,
            "next_unblock_condition": (
                "Provide validated runtime observe records, calibrated cap multiplier "
                "policy, portfolio exposure history, turnover history, post-trigger "
                "outcomes and release / restore decision records."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_simulation_safety_boundary(
    *,
    generated_at: datetime,
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    input_requirement_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "simulation_objective_count": len(readiness_rows),
            "blocked_objective_count": len(readiness_rows),
            "blocker_count": len(blocker_rows),
            "input_requirement_count": len(input_requirement_rows),
            "does_not_read_market_cache": True,
            "does_not_read_runtime_records": True,
            "does_not_read_portfolio_weights": True,
            "does_not_read_portfolio_exposure_history": True,
            "does_not_read_turnover_records": True,
            "does_not_read_post_trigger_outcomes": True,
            "does_not_execute_exposure_cap_simulation": True,
            "does_not_generate_simulation_result": True,
            "does_not_generate_effect_claim": True,
            "does_not_generate_target_weights": True,
            "does_not_generate_max_exposure_number": True,
            "does_not_generate_rebalance_instruction": True,
            "does_not_generate_broker_order": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Source-blocked static simulation readiness package consumes only "
                "TRADING-2322 design artifacts. Future exposure-cap simulation must "
                "run aits validate-data or the same validation code path when cached "
                "market, runtime, portfolio or turnover data is consumed."
            ),
            "allowed_next_step": (
                "provide_runtime_exposure_turnover_outcome_sources_before_simulation"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_mechanics_simulation_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    input_requirement_rows: Sequence[Mapping[str, Any]],
    metric_contract: Mapping[str, Any],
) -> dict[str, Any]:
    source_summary = source["summary"]
    return clean_for_yaml(
        {
            **dict(common),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source_summary, "status"),
            "source_artifact_role": _summary_value(source_summary, "artifact_role"),
            "source_data_quality_status": _summary_value(
                source_summary,
                "data_quality_status",
            ),
            "source_design_only": _summary_value(source_summary, "design_only"),
            "source_runtime_started": _summary_value(source_summary, "runtime_started"),
            "source_aging_runtime_started": _summary_value(
                source_summary,
                "aging_runtime_started",
            ),
            "source_signal_validity_runtime_started": _summary_value(
                source_summary,
                "signal_validity_runtime_started",
            ),
            "source_execution_runtime_started": _summary_value(
                source_summary,
                "execution_runtime_started",
            ),
            "source_forward_observe_started": _summary_value(
                source_summary,
                "forward_observe_started",
            ),
            "source_lifecycle_field_count": _summary_value(
                source_summary,
                "lifecycle_field_count",
            ),
            "source_aging_rule_count": _summary_value(
                source_summary,
                "aging_rule_count",
            ),
            "source_trigger_aging_state_count": _summary_value(
                source_summary,
                "trigger_aging_state_count",
            ),
            "source_release_restore_rule_count": _summary_value(
                source_summary,
                "release_restore_rule_count",
            ),
            "source_runtime_schema_required_field_count": len(
                _strings(source["runtime_schema"].get("required_fields"))
            ),
            "simulation_objective_count": len(readiness_rows),
            "blocked_objective_count": sum(
                1
                for row in readiness_rows
                if row.get("readiness_status") == OBJECTIVE_STATUS
            ),
            "input_requirement_count": len(input_requirement_rows),
            "blocker_count": len(blocker_rows),
            "metric_count": metric_contract.get("metric_count", 0),
            "metric_contract_status": metric_contract.get("status", ""),
            "simulation_readiness_status": "PASS_SOURCE_BLOCKED_EXPECTED",
            "exposure_cap_mechanics_simulation_cli_implemented": True,
            "executable_simulation_ready": False,
            "recommended_next_action": (
                "PROVIDE_RUNTIME_EXPOSURE_TURNOVER_OUTCOME_SOURCES_BEFORE_SIMULATION"
            ),
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_exposure_cap_mechanics_simulation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    metric_contract: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    input_requirement_rows: Sequence[Mapping[str, Any]],
    blocker_rows: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "exposure_cap_mechanics_simulation_summary.json",
        "metric_contract": output_dir / "exposure_cap_simulation_metric_contract.json",
        "readiness_json": output_dir / "exposure_cap_simulation_readiness_matrix.json",
        "readiness_csv": output_dir / "exposure_cap_simulation_readiness_matrix.csv",
        "input_requirement_json": output_dir
        / "exposure_cap_simulation_input_requirement_matrix.json",
        "input_requirement_csv": output_dir
        / "exposure_cap_simulation_input_requirement_matrix.csv",
        "blocker_json": output_dir / "exposure_cap_simulation_blocker_report.json",
        "blocker_csv": output_dir / "exposure_cap_simulation_blocker_report.csv",
        "safety_boundary": output_dir / "exposure_cap_simulation_safety_boundary.json",
        "report_doc": docs_root / "exposure_cap_mechanics_simulation.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["metric_contract"], dict(metric_contract))
    write_json(paths["readiness_json"], {**dict(common), "rows": readiness_rows})
    write_csv_rows(paths["readiness_csv"], readiness_rows)
    write_json(
        paths["input_requirement_json"],
        {**dict(common), "rows": input_requirement_rows},
    )
    write_csv_rows(paths["input_requirement_csv"], input_requirement_rows)
    write_json(paths["blocker_json"], {**dict(common), "rows": blocker_rows})
    write_csv_rows(paths["blocker_csv"], blocker_rows)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            readiness_rows=readiness_rows,
            input_requirement_rows=input_requirement_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    readiness_rows: Sequence[Mapping[str, Any]],
    input_requirement_rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Mechanics Simulation",
            "",
            "TRADING-2323 承接 TRADING-2322，但当前上游仍是 design-only signal "
            "validity / aging runtime contract。本报告是 source-blocked simulation "
            "readiness package，不读取 runtime records、portfolio exposure history、"
            "turnover records 或 post-trigger outcomes，不执行 exposure-cap simulation，"
            "不生成 max exposure delta、turnover effect、restore lag 或 false risk-cap "
            "cost 结论，不进入 paper-shadow、production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- source_design_only: `{summary['source_design_only']}`",
            f"- source_runtime_started: `{summary['source_runtime_started']}`",
            f"- simulation_objective_count: `{summary['simulation_objective_count']}`",
            f"- blocked_objective_count: `{summary['blocked_objective_count']}`",
            f"- input_requirement_count: `{summary['input_requirement_count']}`",
            f"- blocker_count: `{summary['blocker_count']}`",
            f"- executable_simulation_ready: `{summary['executable_simulation_ready']}`",
            "- simulation_executed: `False`",
            "- simulation_result_generated: `False`",
            "- runtime_records_consumed: `False`",
            "- portfolio_exposure_history_consumed: `False`",
            "- turnover_records_consumed: `False`",
            "- target_weight_generated: `False`",
            "- max_exposure_number_generated: `False`",
            "- broker_order_generated: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Simulation Readiness",
            "",
            "|simulation_objective|readiness_status|blocked_reason|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['simulation_objective']}`|"
                    f"`{row['readiness_status']}`|{_one_line(row['blocked_reason'])}|"
                )
                for row in readiness_rows
            ],
            "",
            "## Input Requirements",
            "",
            "|simulation_objective|requirement_id|requirement_status|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['simulation_objective']}`|`{row['requirement_id']}`|"
                    f"`{row['requirement_status']}`|"
                )
                for row in input_requirement_rows
            ],
            "",
            "## Boundary",
            "",
            "退出 source-blocked simulation 状态的条件是补齐 runtime observe records、"
            "calibrated cap multiplier policy、validated portfolio exposure history、"
            "turnover / trade intent history、post-trigger outcomes 和 release / restore "
            "decision records，并在读取 cached market/runtime/portfolio data 的 simulation "
            "workflow 中执行 `aits validate-data` 或同源 data-quality gate。当前不得把本报告"
            "解读为 exposure-cap mechanics simulation result。",
            "",
        ]
    )


def _common_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    source_dir: Path,
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Exposure-Cap Mechanics Simulation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "policy_path": str(policy_path),
        "source_dir": str(source_dir),
        "candidate_id": RISK_CAP_CANDIDATE_ID,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "actual_requested_date_range": (
            "source_blocked_static_simulation_readiness_package"
        ),
        "data_quality_status": DATA_QUALITY_STATUS,
        "source_data_quality_status": _summary_value(
            source_summary,
            "data_quality_status",
        ),
        "data_quality_requirement": (
            "Source-blocked static simulation readiness package; no cached market "
            "data, runtime observe records, portfolio exposure history, turnover "
            "records or post-trigger outcomes are read. Future exposure-cap "
            "simulation must run aits validate-data when cached data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _validate_trading_2322_source(
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = source["summary"]
    dependency = mapping(policy.get("source_dependency"))
    expected = {
        "task_id": dependency.get("required_task_id"),
        "status": dependency.get("required_status"),
        "artifact_role": dependency.get("required_artifact_role"),
        "data_quality_status": dependency.get("required_data_quality_status"),
        "broker_action": dependency.get("required_broker_action"),
        "candidate_id": dependency.get("required_candidate_id"),
    }
    for field, expected_value in expected.items():
        if _summary_value(summary, field) != expected_value:
            raise ExposureCapMechanicsSimulationError(
                f"TRADING-2322 source {field} mismatch: "
                f"{_summary_value(summary, field)}"
            )
    for field in (
        "design_only",
        "runtime_started",
        "aging_runtime_started",
        "signal_validity_runtime_started",
        "execution_runtime_started",
        "forward_observe_started",
    ):
        expected_value = dependency.get(f"required_{field}")
        if _summary_value(summary, field) is not expected_value:
            raise ExposureCapMechanicsSimulationError(
                f"TRADING-2322 {field} mismatch"
            )
    count_expectations = {
        "lifecycle_field_count": "required_lifecycle_field_count",
        "aging_rule_count": "required_aging_rule_count",
        "trigger_aging_state_count": "required_trigger_aging_state_count",
        "release_restore_rule_count": "required_release_restore_rule_count",
    }
    for source_field, dependency_field in count_expectations.items():
        if _summary_value(summary, source_field) != dependency.get(dependency_field):
            raise ExposureCapMechanicsSimulationError(
                f"TRADING-2322 {source_field} mismatch"
            )
    if dependency.get("allow_design_only_source_blocked_simulation") is not True:
        raise ExposureCapMechanicsSimulationError(
            "source-blocked simulation package is not allowed"
        )
    for key, payload in source.items():
        _validate_source_safety(f"source.{key}", payload)
    lifecycle_fields = {
        str(row.get("field_id"))
        for row in records(source["lifecycle_contract"].get("lifecycle_fields"))
    }
    if lifecycle_fields != set(SOURCE_LIFECYCLE_FIELDS):
        raise ExposureCapMechanicsSimulationError(
            "TRADING-2322 lifecycle contract mismatch"
        )
    if len(records(mapping(source["aging_rules"]).get("rows"))) != 5:
        raise ExposureCapMechanicsSimulationError("TRADING-2322 aging rules missing")
    if len(records(mapping(source["trigger_aging"]).get("rows"))) != 4:
        raise ExposureCapMechanicsSimulationError("TRADING-2322 trigger states missing")
    if len(records(mapping(source["release_restore"]).get("rows"))) != 4:
        raise ExposureCapMechanicsSimulationError("TRADING-2322 release rules missing")
    if _strings(source["runtime_schema"].get("allowed_action_values")) != [
        "observe_only_design_contract"
    ]:
        raise ExposureCapMechanicsSimulationError(
            "TRADING-2322 runtime schema action mismatch"
        )


def _validate_source_safety(name: str, payload: Mapping[str, Any]) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise ExposureCapMechanicsSimulationError(f"{name} opens promotion_allowed")
        if item.get("paper_shadow_allowed") is True:
            raise ExposureCapMechanicsSimulationError(
                f"{name} opens paper_shadow_allowed"
            )
        if item.get("production_allowed") is True:
            raise ExposureCapMechanicsSimulationError(f"{name} opens production_allowed")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise ExposureCapMechanicsSimulationError(f"{name} opens broker_action")
        for forbidden in (
            "runtime_started",
            "forward_observe_started",
            "execution_runtime_started",
            "aging_runtime_started",
            "signal_validity_runtime_started",
            "portfolio_weights_read",
            "portfolio_weights_written",
            "target_weight_generated",
            "max_exposure_number_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
            "validity_runtime_executable",
            "aging_rule_executable",
            "release_restore_rule_executable",
        ):
            if item.get(forbidden) is True:
                raise ExposureCapMechanicsSimulationError(
                    f"{name} opens {forbidden}"
                )


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
        "simulation_objectives",
        "required_runtime_inputs",
        "metric_contract",
        "blocker_catalog",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise ExposureCapMechanicsSimulationError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "exposure_cap_mechanics_simulation_policy":
        raise ExposureCapMechanicsSimulationError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise ExposureCapMechanicsSimulationError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise ExposureCapMechanicsSimulationError("policy market_regime mismatch")
    dependency = mapping(policy.get("source_dependency"))
    if dependency.get("required_task_id") != SOURCE_TASK_ID:
        raise ExposureCapMechanicsSimulationError("policy source task mismatch")
    if dependency.get("required_status") != SOURCE_STATUS:
        raise ExposureCapMechanicsSimulationError("policy source status mismatch")
    objectives = mapping(policy.get("simulation_objectives"))
    if set(objectives) != set(REQUIRED_SIMULATION_OBJECTIVES):
        raise ExposureCapMechanicsSimulationError("policy objectives mismatch")
    for objective_id in REQUIRED_SIMULATION_OBJECTIVES:
        objective = mapping(objectives.get(objective_id))
        if not _strings(objective.get("required_runtime_inputs")):
            raise ExposureCapMechanicsSimulationError(
                f"{objective_id} requires runtime inputs"
            )
        if not objective.get("blocked_reason"):
            raise ExposureCapMechanicsSimulationError(
                f"{objective_id} requires blocked_reason"
            )
    contract = mapping(policy.get("metric_contract"))
    if contract.get("contract_status") != "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY":
        raise ExposureCapMechanicsSimulationError("metric contract status mismatch")
    metrics = mapping(contract.get("metrics"))
    if set(mapping(metric).get("simulation_objective") for metric in metrics.values()) != set(
        REQUIRED_SIMULATION_OBJECTIVES
    ):
        raise ExposureCapMechanicsSimulationError("metric objective coverage mismatch")
    for metric_id, metric in metrics.items():
        if mapping(metric).get("result_generated_now") is not False:
            raise ExposureCapMechanicsSimulationError(
                f"{metric_id} result_generated_now must be false"
            )
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise ExposureCapMechanicsSimulationError(
                f"policy safety.{field} must be {expected}"
            )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ExposureCapMechanicsSimulationError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise ExposureCapMechanicsSimulationError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ExposureCapMechanicsSimulationError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ExposureCapMechanicsSimulationError(f"JSON must be object: {path}")
    return payload


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        found.append(value)
        for child in value.values():
            found.extend(_walk_mappings(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_mappings(child))
    return found


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    return mapping(payload.get("summary")).get(key)


def _one_line(value: object) -> str:
    return " ".join(str(value).split())


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
    "DEFAULT_SOURCE_ROOT",
    "MODE",
    "OBJECTIVE_STATUS",
    "REPORT_TYPE",
    "REQUIRED_SIMULATION_OBJECTIVES",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "ExposureCapMechanicsSimulationError",
    "build_exposure_cap_mechanics_simulation_summary",
    "build_exposure_cap_simulation_blocker_report",
    "build_exposure_cap_simulation_input_requirement_matrix",
    "build_exposure_cap_simulation_metric_contract",
    "build_exposure_cap_simulation_readiness_matrix",
    "load_trading_2322_signal_validity_aging_runtime_artifacts",
    "run_exposure_cap_mechanics_simulation",
]
