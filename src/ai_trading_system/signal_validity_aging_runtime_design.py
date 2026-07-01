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
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RISK_CAP_COOLDOWN_DECAY_ROOT,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    REQUIRED_STATES as SOURCE_EXECUTION_STATES,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    STATUS as SOURCE_STATUS,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    TASK_ID as SOURCE_RISK_CAP_TASK_ID,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    RISK_CAP_CANDIDATE_ID,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2322_SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN"
SOURCE_TASK_ID = SOURCE_RISK_CAP_TASK_ID
REPORT_TYPE = "signal_validity_aging_runtime_design"
ARTIFACT_ROLE = "signal_validity_aging_runtime_design_only"
MODE = "design_only"
STATUS = "SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_DESIGN_ONLY_SIGNAL_VALIDITY_AGING"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "signal_validity_aging_runtime_design_policy.yaml"
)
DEFAULT_SOURCE_ROOT = DEFAULT_RISK_CAP_COOLDOWN_DECAY_ROOT
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_LIFECYCLE_FIELDS = (
    "valid_from",
    "valid_until",
    "decay",
    "staleness",
    "trigger_aging",
    "release_restore_rule",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "design_only": True,
    "aging_runtime_started": False,
    "signal_validity_runtime_started": False,
    "execution_runtime_started": False,
    "forward_observe_started": False,
    "runtime_started": False,
    "portfolio_weights_read": False,
    "portfolio_weights_written": False,
    "target_weight_generated": False,
    "max_exposure_number_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
    "validity_runtime_executable": False,
    "aging_rule_executable": False,
    "release_restore_rule_executable": False,
    "manual_review_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class SignalValidityAgingRuntimeDesignError(ValueError):
    pass


def run_signal_validity_aging_runtime_design(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    source_dir: Path = DEFAULT_SOURCE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise SignalValidityAgingRuntimeDesignError(
            "signal validity / aging runtime design only supports design_only mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = load_trading_2321_risk_cap_cooldown_decay_design_artifacts(source_dir)
    _validate_trading_2321_source(source, policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    common = _common_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        source_dir=source_dir,
        source_summary=source["summary"],
    )
    lifecycle_contract = build_signal_validity_lifecycle_contract(
        policy=policy,
        source=source,
    )
    aging_rows = build_signal_validity_aging_rule_matrix(
        policy=policy,
        source=source,
    )
    trigger_aging_rows = build_signal_validity_trigger_aging_state_matrix(
        policy=policy,
        source=source,
    )
    release_restore_rows = build_signal_validity_release_restore_rule_matrix(
        policy=policy,
        source=source,
    )
    runtime_schema = build_signal_validity_runtime_record_schema(
        policy=policy,
        source=source,
    )
    safety_boundary = build_signal_validity_aging_safety_boundary(
        generated_at=generated_at,
        lifecycle_contract=lifecycle_contract,
        aging_rows=aging_rows,
        trigger_aging_rows=trigger_aging_rows,
        release_restore_rows=release_restore_rows,
    )
    summary = build_signal_validity_aging_runtime_design_summary(
        common=common,
        policy=policy,
        source=source,
        lifecycle_contract=lifecycle_contract,
        aging_rows=aging_rows,
        trigger_aging_rows=trigger_aging_rows,
        release_restore_rows=release_restore_rows,
        runtime_schema=runtime_schema,
    )
    paths = write_signal_validity_aging_runtime_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        lifecycle_contract=lifecycle_contract,
        aging_rows=aging_rows,
        trigger_aging_rows=trigger_aging_rows,
        release_restore_rows=release_restore_rows,
        runtime_schema=runtime_schema,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "lifecycle_contract": lifecycle_contract,
            "aging_rule_rows": aging_rows,
            "trigger_aging_state_rows": trigger_aging_rows,
            "release_restore_rule_rows": release_restore_rows,
            "runtime_record_schema": runtime_schema,
            "safety_boundary": safety_boundary,
        }
    )


def load_trading_2321_risk_cap_cooldown_decay_design_artifacts(
    source_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": source_dir / "risk_cap_cooldown_decay_design_summary.json",
        "state_contract": source_dir / "risk_cap_execution_state_contract.json",
        "cooldown_rules": source_dir / "risk_cap_cooldown_decay_rule_matrix.json",
        "exposure_states": source_dir / "risk_cap_exposure_cap_state_matrix.json",
        "manual_review_contract": source_dir / "risk_cap_manual_review_mode_contract.json",
        "transition_matrix": source_dir / "risk_cap_execution_transition_matrix.json",
        "safety_boundary": source_dir / "risk_cap_cooldown_decay_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise SignalValidityAgingRuntimeDesignError(
            "TRADING-2322 requires TRADING-2321 cooldown / decay outputs: "
            + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def build_signal_validity_lifecycle_contract(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    field_config = mapping(policy.get("required_lifecycle_fields"))
    fields: list[dict[str, Any]] = []
    for field_id in REQUIRED_LIFECYCLE_FIELDS:
        definition = mapping(field_config.get(field_id))
        fields.append(
            clean_for_yaml(
                {
                    "field_id": field_id,
                    "field_role": definition.get("field_role", ""),
                    "source": definition.get("source", ""),
                    "allowed_current_effect": definition.get("allowed_current_effect", ""),
                    "forbidden_effects": _strings(definition.get("forbidden_effects")),
                    "source_task_id": _summary_value(source["summary"], "task_id"),
                    "source_status": _summary_value(source["summary"], "status"),
                    "runtime_field_executable_now": False,
                    "portfolio_effect": "none",
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.lifecycle_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "lifecycle_field_count": len(fields),
            "lifecycle_fields": fields,
            "allowed_current_usage": "signal_validity_aging_schema_contract_only",
            "forbidden_outputs": _forbidden_runtime_outputs(policy),
            **SAFETY_FIELDS,
        }
    )


def build_signal_validity_aging_rule_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checkpoints = ",".join(_strings(policy.get("aging_checkpoints")))
    rows: list[dict[str, Any]] = []
    for rule in records(policy.get("aging_rules")):
        rows.append(
            clean_for_yaml(
                {
                    "rule_id": rule.get("rule_id", ""),
                    "lifecycle_field": rule.get("lifecycle_field", ""),
                    "trigger_state": rule.get("trigger_state", ""),
                    "evidence_required": ",".join(_strings(rule.get("evidence_required"))),
                    "aging_checkpoints": checkpoints,
                    "rule_status": "DESIGN_ONLY_NOT_EXECUTABLE",
                    "source_state_count": _summary_value(source["summary"], "state_count"),
                    "validity_duration_defined": False,
                    "decay_multiplier_defined": False,
                    "staleness_threshold_defined": False,
                    "automatic_action_allowed": False,
                    "target_weight_generated": False,
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_signal_validity_trigger_aging_state_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_states = {
        str(row.get("state_id"))
        for row in records(source["state_contract"].get("states"))
    }
    for state in records(policy.get("trigger_aging_states")):
        rows.append(
            clean_for_yaml(
                {
                    "state_id": state.get("state_id", ""),
                    "state_role": state.get("state_role", ""),
                    "aging_status": state.get("aging_status", ""),
                    "source_execution_states": ",".join(sorted(source_states)),
                    "state_executable_now": False,
                    "automatic_transition_allowed": False,
                    "portfolio_effect": "none",
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_signal_validity_release_restore_rule_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_states = {
        str(row.get("state_id"))
        for row in records(source["state_contract"].get("states"))
    }
    rows: list[dict[str, Any]] = []
    for rule in records(policy.get("release_restore_rules")):
        rows.append(
            clean_for_yaml(
                {
                    "rule_id": rule.get("rule_id", ""),
                    "source_state": rule.get("source_state", ""),
                    "source_state_in_2321_contract": rule.get("source_state") in source_states,
                    "restore_target_state": rule.get("restore_target_state", ""),
                    "release_allowed_now": rule.get("release_allowed_now", False),
                    "owner_review_required": rule.get("owner_review_required", True),
                    "rule_status": "DESIGN_ONLY_NOT_EXECUTABLE",
                    "automatic_restore_allowed": False,
                    "exposure_restore_instruction_generated": False,
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_signal_validity_runtime_record_schema(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    schema = mapping(policy.get("runtime_record_schema"))
    required_fields = _strings(schema.get("required_fields"))
    forbidden_fields = _strings(schema.get("forbidden_fields"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.runtime_record_schema.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "SIGNAL_VALIDITY_RUNTIME_RECORD_SCHEMA_DESIGN_ONLY",
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "allowed_action_values": _strings(schema.get("allowed_action_values")),
            "required_fields": required_fields,
            "forbidden_fields": forbidden_fields,
            "lifecycle_fields_required": list(REQUIRED_LIFECYCLE_FIELDS),
            "record_written_now": False,
            "runtime_schema_executable_now": False,
            "blocked_actions": [
                "runtime_record_write",
                "target_weight_change",
                "rebalance_instruction",
                "broker_order",
                "paper_shadow_order",
                "production_decision",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_signal_validity_aging_safety_boundary(
    *,
    generated_at: datetime,
    lifecycle_contract: Mapping[str, Any],
    aging_rows: Sequence[Mapping[str, Any]],
    trigger_aging_rows: Sequence[Mapping[str, Any]],
    release_restore_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "lifecycle_field_count": lifecycle_contract.get("lifecycle_field_count", 0),
            "aging_rule_count": len(aging_rows),
            "trigger_aging_state_count": len(trigger_aging_rows),
            "release_restore_rule_count": len(release_restore_rows),
            "does_not_start_aging_runtime": True,
            "does_not_write_runtime_records": True,
            "does_not_read_portfolio_weights": True,
            "does_not_write_portfolio_weights": True,
            "does_not_generate_target_weights": True,
            "does_not_generate_rebalance_instruction": True,
            "does_not_generate_broker_order": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Design-only signal validity / aging runtime package consumes only "
                "TRADING-2321 static artifacts. Future runtime, simulation, daily "
                "report, portfolio or backtest workflows must run aits validate-data "
                "or the same validation code path before consuming cached market data."
            ),
            "allowed_next_step": (
                "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION_AFTER_OWNER_REVIEW"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_signal_validity_aging_runtime_design_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    lifecycle_contract: Mapping[str, Any],
    aging_rows: Sequence[Mapping[str, Any]],
    trigger_aging_rows: Sequence[Mapping[str, Any]],
    release_restore_rows: Sequence[Mapping[str, Any]],
    runtime_schema: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            **dict(common),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_task_id": SOURCE_TASK_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "source_data_quality_status": _summary_value(
                source["summary"],
                "data_quality_status",
            ),
            "source_runtime_started": _summary_value(source["summary"], "runtime_started"),
            "source_execution_runtime_started": _summary_value(
                source["summary"],
                "execution_runtime_started",
            ),
            "source_forward_observe_started": _summary_value(
                source["summary"],
                "forward_observe_started",
            ),
            "source_state_count": _summary_value(source["summary"], "state_count"),
            "lifecycle_field_count": lifecycle_contract.get("lifecycle_field_count", 0),
            "aging_rule_count": len(aging_rows),
            "trigger_aging_state_count": len(trigger_aging_rows),
            "release_restore_rule_count": len(release_restore_rows),
            "runtime_schema_required_field_count": len(
                _strings(runtime_schema.get("required_fields"))
            ),
            "signal_validity_aging_runtime_design_cli_implemented": True,
            "lifecycle_contract_generated": True,
            "runtime_record_schema_generated": True,
            "executable_runtime_policy_ready": False,
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_signal_validity_aging_runtime_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    lifecycle_contract: Mapping[str, Any],
    aging_rows: Sequence[Mapping[str, Any]],
    trigger_aging_rows: Sequence[Mapping[str, Any]],
    release_restore_rows: Sequence[Mapping[str, Any]],
    runtime_schema: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "signal_validity_aging_runtime_design_summary.json",
        "lifecycle_contract": output_dir / "signal_validity_lifecycle_contract.json",
        "aging_rule_matrix_json": output_dir / "signal_validity_aging_rule_matrix.json",
        "aging_rule_matrix_csv": output_dir / "signal_validity_aging_rule_matrix.csv",
        "trigger_aging_json": output_dir
        / "signal_validity_trigger_aging_state_matrix.json",
        "trigger_aging_csv": output_dir
        / "signal_validity_trigger_aging_state_matrix.csv",
        "release_restore_json": output_dir
        / "signal_validity_release_restore_rule_matrix.json",
        "release_restore_csv": output_dir
        / "signal_validity_release_restore_rule_matrix.csv",
        "runtime_record_schema": output_dir / "signal_validity_runtime_record_schema.json",
        "safety_boundary": output_dir / "signal_validity_aging_safety_boundary.json",
        "report_doc": docs_root / "signal_validity_aging_runtime_design.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["lifecycle_contract"], dict(lifecycle_contract))
    write_json(paths["aging_rule_matrix_json"], {**dict(common), "rows": aging_rows})
    write_csv_rows(paths["aging_rule_matrix_csv"], aging_rows)
    write_json(paths["trigger_aging_json"], {**dict(common), "rows": trigger_aging_rows})
    write_csv_rows(paths["trigger_aging_csv"], trigger_aging_rows)
    write_json(
        paths["release_restore_json"],
        {**dict(common), "rows": release_restore_rows},
    )
    write_csv_rows(paths["release_restore_csv"], release_restore_rows)
    write_json(paths["runtime_record_schema"], dict(runtime_schema))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            lifecycle_contract=lifecycle_contract,
            aging_rows=aging_rows,
            trigger_aging_rows=trigger_aging_rows,
            release_restore_rows=release_restore_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    lifecycle_contract: Mapping[str, Any],
    aging_rows: Sequence[Mapping[str, Any]],
    trigger_aging_rows: Sequence[Mapping[str, Any]],
    release_restore_rows: Sequence[Mapping[str, Any]],
) -> str:
    lifecycle_fields = records(lifecycle_contract.get("lifecycle_fields"))
    return "\n".join(
        [
            "# Signal Validity / Aging Runtime Design",
            "",
            "TRADING-2322 承接 TRADING-2321 design-only execution mechanics。"
            "本报告只定义 signal validity / trigger aging runtime schema contract，"
            "不写入 runtime records，不读取或写入 portfolio weights，不生成 target "
            "weight、rebalance instruction 或 broker order，不启动 paper-shadow、"
            "production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- source_runtime_started: `{summary['source_runtime_started']}`",
            f"- source_execution_runtime_started: `{summary['source_execution_runtime_started']}`",
            f"- lifecycle_field_count: `{summary['lifecycle_field_count']}`",
            f"- aging_rule_count: `{summary['aging_rule_count']}`",
            f"- trigger_aging_state_count: `{summary['trigger_aging_state_count']}`",
            f"- release_restore_rule_count: `{summary['release_restore_rule_count']}`",
            "- aging_runtime_started: `False`",
            "- target_weight_generated: `False`",
            "- rebalance_instruction_generated: `False`",
            "- broker_order_generated: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Lifecycle Fields",
            "",
            "|field_id|field_role|allowed_current_effect|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['field_id']}`|{row['field_role']}|"
                    f"{row['allowed_current_effect']}|"
                )
                for row in lifecycle_fields
            ],
            "",
            "## Aging Rules",
            "",
            "|rule_id|lifecycle_field|trigger_state|rule_status|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['rule_id']}`|`{row['lifecycle_field']}`|"
                    f"`{row['trigger_state']}`|`{row['rule_status']}`|"
                )
                for row in aging_rows
            ],
            "",
            "## Trigger Aging States",
            "",
            "|state_id|state_role|aging_status|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['state_id']}`|{row['state_role']}|"
                    f"`{row['aging_status']}`|"
                )
                for row in trigger_aging_rows
            ],
            "",
            "## Release / Restore Rules",
            "",
            "|rule_id|source_state|restore_target_state|release_allowed_now|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['rule_id']}`|`{row['source_state']}`|"
                    f"`{row['restore_target_state']}`|`{row['release_allowed_now']}`|"
                )
                for row in release_restore_rows
            ],
            "",
            "## Boundary",
            "",
            "当前 validity duration、valid_until expiry action、decay multiplier、"
            "staleness threshold 和 release / restore threshold 均未校准为可执行 "
            "policy。后续若进入 TRADING-2323 simulation 或 runtime implementation，"
            "必须重新执行 data-quality gate、owner review 和 broker / production safety review。",
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
        "title": "Signal Validity / Aging Runtime Design",
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
        "actual_requested_date_range": "design_only_signal_validity_aging_contract",
        "data_quality_status": DATA_QUALITY_STATUS,
        "source_data_quality_status": _summary_value(
            source_summary,
            "data_quality_status",
        ),
        "data_quality_requirement": (
            "Design-only signal validity / aging runtime package; no cached market "
            "data, runtime observe records or portfolio data are read. Future runtime, "
            "simulation, scoring, reports or backtests must run aits validate-data "
            "when cached data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _validate_trading_2321_source(
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = source["summary"]
    dependency = mapping(policy.get("source_dependency"))
    expected = {
        "task_id": dependency.get("required_task_id"),
        "status": dependency.get("required_status"),
        "artifact_role": dependency.get("required_artifact_role"),
        "broker_action": dependency.get("required_broker_action"),
        "candidate_id": dependency.get("required_candidate_id"),
    }
    for field, expected_value in expected.items():
        if _summary_value(summary, field) != expected_value:
            raise SignalValidityAgingRuntimeDesignError(
                f"TRADING-2321 source {field} mismatch: {_summary_value(summary, field)}"
            )
    for field in (
        "design_only",
        "runtime_started",
        "execution_runtime_started",
        "forward_observe_started",
    ):
        expected_value = dependency.get(f"required_{field}")
        if _summary_value(summary, field) is not expected_value:
            raise SignalValidityAgingRuntimeDesignError(
                f"TRADING-2321 {field} mismatch"
            )
    if _summary_value(summary, "state_count") != dependency.get("required_state_count"):
        raise SignalValidityAgingRuntimeDesignError("TRADING-2321 state_count mismatch")
    for key, payload in source.items():
        _validate_source_safety(f"source.{key}", payload)
    state_ids = {
        str(row.get("state_id"))
        for row in records(source["state_contract"].get("states"))
    }
    if state_ids != SOURCE_EXECUTION_STATES:
        raise SignalValidityAgingRuntimeDesignError("TRADING-2321 state contract mismatch")
    if len(records(mapping(source.get("cooldown_rules")).get("rows"))) != 3:
        raise SignalValidityAgingRuntimeDesignError("TRADING-2321 cooldown rules missing")
    if len(records(mapping(source.get("transition_matrix")).get("rows"))) != 5:
        raise SignalValidityAgingRuntimeDesignError("TRADING-2321 transition matrix missing")


def _validate_source_safety(name: str, payload: Mapping[str, Any]) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise SignalValidityAgingRuntimeDesignError(f"{name} opens promotion_allowed")
        if item.get("paper_shadow_allowed") is True:
            raise SignalValidityAgingRuntimeDesignError(f"{name} opens paper_shadow_allowed")
        if item.get("production_allowed") is True:
            raise SignalValidityAgingRuntimeDesignError(f"{name} opens production_allowed")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise SignalValidityAgingRuntimeDesignError(f"{name} opens broker_action")
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
            "cooldown_state_executable",
            "decay_rule_executable",
            "exposure_cap_rule_executable",
        ):
            if item.get(forbidden) is True:
                raise SignalValidityAgingRuntimeDesignError(f"{name} opens {forbidden}")


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
        "required_lifecycle_fields",
        "aging_checkpoints",
        "aging_rules",
        "trigger_aging_states",
        "release_restore_rules",
        "runtime_record_schema",
        "calibration_policy",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise SignalValidityAgingRuntimeDesignError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "signal_validity_aging_runtime_design_policy":
        raise SignalValidityAgingRuntimeDesignError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise SignalValidityAgingRuntimeDesignError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise SignalValidityAgingRuntimeDesignError("policy market_regime mismatch")
    dependency = mapping(policy.get("source_dependency"))
    if dependency.get("required_task_id") != SOURCE_TASK_ID:
        raise SignalValidityAgingRuntimeDesignError("policy source task mismatch")
    if dependency.get("required_status") != SOURCE_STATUS:
        raise SignalValidityAgingRuntimeDesignError("policy source status mismatch")
    if tuple(mapping(policy.get("required_lifecycle_fields"))) != REQUIRED_LIFECYCLE_FIELDS:
        raise SignalValidityAgingRuntimeDesignError("policy lifecycle fields mismatch")
    if _strings(policy.get("aging_checkpoints")) != ["5d", "10d", "20d"]:
        raise SignalValidityAgingRuntimeDesignError("policy aging checkpoints mismatch")
    if not records(policy.get("aging_rules")):
        raise SignalValidityAgingRuntimeDesignError("policy aging rules required")
    if not records(policy.get("trigger_aging_states")):
        raise SignalValidityAgingRuntimeDesignError("policy trigger aging states required")
    if not records(policy.get("release_restore_rules")):
        raise SignalValidityAgingRuntimeDesignError("policy release rules required")
    calibration = mapping(policy.get("calibration_policy"))
    for field in (
        "validity_duration_defined",
        "decay_multiplier_defined",
        "staleness_threshold_defined",
        "release_threshold_defined",
    ):
        if calibration.get(field) is not False:
            raise SignalValidityAgingRuntimeDesignError(f"{field} must be false")
    schema = mapping(policy.get("runtime_record_schema"))
    required_schema_fields = set(_strings(schema.get("required_fields")))
    if not set(REQUIRED_LIFECYCLE_FIELDS).issubset(required_schema_fields):
        raise SignalValidityAgingRuntimeDesignError("runtime schema missing lifecycle fields")
    if _strings(schema.get("allowed_action_values")) != ["observe_only_design_contract"]:
        raise SignalValidityAgingRuntimeDesignError("runtime schema action mismatch")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise SignalValidityAgingRuntimeDesignError(
                f"policy safety.{field} must be {expected}"
            )


def _forbidden_runtime_outputs(policy: Mapping[str, Any]) -> list[str]:
    schema = mapping(policy.get("runtime_record_schema"))
    return _strings(schema.get("forbidden_fields"))


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SignalValidityAgingRuntimeDesignError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise SignalValidityAgingRuntimeDesignError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SignalValidityAgingRuntimeDesignError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SignalValidityAgingRuntimeDesignError(f"JSON must be object: {path}")
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
    "REPORT_TYPE",
    "REQUIRED_LIFECYCLE_FIELDS",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "SignalValidityAgingRuntimeDesignError",
    "build_signal_validity_aging_rule_matrix",
    "build_signal_validity_lifecycle_contract",
    "build_signal_validity_release_restore_rule_matrix",
    "build_signal_validity_runtime_record_schema",
    "build_signal_validity_trigger_aging_state_matrix",
    "load_trading_2321_risk_cap_cooldown_decay_design_artifacts",
    "run_signal_validity_aging_runtime_design",
]
