from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
)
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
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2321_RISK_CAP_COOLDOWN_DECAY_DESIGN"
SOURCE_TASK_ID = "TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN"
REPORT_TYPE = "risk_cap_cooldown_decay_design"
ARTIFACT_ROLE = "risk_cap_cooldown_decay_design_only"
MODE = "design_only"
STATUS = "RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED"
SOURCE_STATUS = "FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_DESIGN_ONLY_EXECUTION_MECHANICS"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_cap_cooldown_decay_design_policy.yaml"
)
DEFAULT_SOURCE_ROOT = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_STATES = {
    "no_add_mode",
    "reduced_max_exposure_mode",
    "manual_review_mode",
    "cooldown_mode",
}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "design_only": True,
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
    "cooldown_state_executable": False,
    "decay_rule_executable": False,
    "exposure_cap_rule_executable": False,
    "manual_review_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class RiskCapCooldownDecayDesignError(ValueError):
    pass


def run_risk_cap_cooldown_decay_design(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    source_dir: Path = DEFAULT_SOURCE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise RiskCapCooldownDecayDesignError(
            "risk-cap cooldown / decay design only supports design_only mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = load_trading_2294_forward_observe_plan_artifacts(source_dir)
    _validate_trading_2294_source(source, policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    common = _common_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        source_dir=source_dir,
        source_summary=source["summary"],
    )
    state_contract = build_risk_cap_execution_state_contract(policy=policy, source=source)
    rule_rows = build_risk_cap_cooldown_decay_rule_matrix(policy=policy, source=source)
    exposure_rows = build_risk_cap_exposure_cap_state_matrix(policy=policy, source=source)
    manual_review_contract = build_risk_cap_manual_review_mode_contract(
        policy=policy,
        source=source,
    )
    transition_rows = build_risk_cap_execution_transition_matrix(
        policy=policy,
        source=source,
    )
    safety_boundary = build_risk_cap_cooldown_decay_safety_boundary(
        generated_at=generated_at,
        state_contract=state_contract,
        rule_rows=rule_rows,
        exposure_rows=exposure_rows,
        transition_rows=transition_rows,
    )
    summary = build_risk_cap_cooldown_decay_design_summary(
        common=common,
        policy=policy,
        source=source,
        state_contract=state_contract,
        rule_rows=rule_rows,
        exposure_rows=exposure_rows,
        transition_rows=transition_rows,
    )
    paths = write_risk_cap_cooldown_decay_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        state_contract=state_contract,
        rule_rows=rule_rows,
        exposure_rows=exposure_rows,
        manual_review_contract=manual_review_contract,
        transition_rows=transition_rows,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "execution_state_contract": state_contract,
            "cooldown_decay_rule_rows": rule_rows,
            "exposure_cap_state_rows": exposure_rows,
            "manual_review_mode_contract": manual_review_contract,
            "execution_transition_rows": transition_rows,
            "safety_boundary": safety_boundary,
        }
    )


def load_trading_2294_forward_observe_plan_artifacts(source_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": source_dir / "forward_observe_evidence_accumulation_plan_summary.json",
        "runtime_contract": source_dir / "forward_observe_runtime_contract.json",
        "daily_schema": source_dir / "risk_cap_daily_observe_record_schema.json",
        "followup_schema": source_dir / "risk_cap_trigger_followup_schema.json",
        "storage_layout": source_dir / "forward_observe_storage_layout.json",
        "observation_policy": source_dir / "forward_observe_minimum_observation_policy.json",
        "decision_matrix": source_dir / "forward_observe_evidence_decision_matrix.json",
        "safety_boundary": source_dir / "forward_observe_runtime_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise RiskCapCooldownDecayDesignError(
            "TRADING-2321 requires TRADING-2294 observe plan outputs: "
            + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def build_risk_cap_execution_state_contract(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    states: list[dict[str, Any]] = []
    for state_id in sorted(REQUIRED_STATES):
        definition = mapping(mapping(policy.get("required_execution_states")).get(state_id))
        states.append(
            clean_for_yaml(
                {
                    "state_id": state_id,
                    "state_role": definition.get("state_role", ""),
                    "activation_source": definition.get("activation_source", ""),
                    "allowed_current_effect": definition.get("allowed_current_effect", ""),
                    "forbidden_effects": _strings(definition.get("forbidden_effects")),
                    "source_observe_mode": _summary_value(source["summary"], "observe_mode"),
                    "source_runtime_started": _summary_value(
                        source["summary"],
                        "runtime_started",
                    ),
                    "state_executable_now": False,
                    "portfolio_effect": "none",
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.execution_state_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "state_count": len(states),
            "states": states,
            "forbidden_outputs": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_order",
                "paper_shadow_order",
                "production_decision",
            ],
            "allowed_current_usage": "design_only_execution_state_contract",
            **SAFETY_FIELDS,
        }
    )


def build_risk_cap_cooldown_decay_rule_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    checkpoints = _strings(policy.get("cooldown_decay_checkpoints"))
    source_horizons = ",".join(_strings(_summary_value(source["summary"], "horizons")))
    for checkpoint in checkpoints:
        rows.append(
            clean_for_yaml(
                {
                    "rule_id": f"cooldown_decay_checkpoint_{checkpoint}",
                    "checkpoint": checkpoint,
                    "source_horizons": source_horizons,
                    "state_id": "cooldown_mode",
                    "rule_status": "DESIGN_ONLY_NOT_EXECUTABLE",
                    "activation_condition": "risk_cap_trigger_cleared_after_active_state",
                    "review_condition": "followup_window_review_before_release",
                    "allowed_current_effect": "cooldown_decay_design_contract_only",
                    "release_automatic": False,
                    "cap_multiplier_defined": False,
                    "target_weight_generated": False,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_risk_cap_exposure_cap_state_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    cap_policy = mapping(policy.get("cap_calibration_policy"))
    rows: list[dict[str, Any]] = []
    for state_id in sorted(REQUIRED_STATES):
        rows.append(
            clean_for_yaml(
                {
                    "state_id": state_id,
                    "cap_scope": _cap_scope(state_id),
                    "cap_multiplier_defined": cap_policy.get("cap_multiplier_defined", False),
                    "cap_multiplier_status": cap_policy.get("cap_multiplier_status", ""),
                    "calibration_task": cap_policy.get("calibration_task", ""),
                    "source_candidate_id": _summary_value(source["summary"], "candidate_id"),
                    "max_exposure_number_generated": False,
                    "portfolio_weights_written": False,
                    "rebalance_instruction_generated": False,
                    "allowed_current_effect": "design_only_no_executable_cap",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_risk_cap_manual_review_mode_contract(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    review_policy = mapping(policy.get("manual_review_policy"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_mode_contract.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": "RISK_CAP_MANUAL_REVIEW_MODE_CONTRACT_DESIGN_ONLY",
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "source_status": _summary_value(source["summary"], "status"),
            "manual_review_only": review_policy.get("manual_review_only", True),
            "automatic_approval_allowed": review_policy.get(
                "automatic_approval_allowed",
                False,
            ),
            "owner_approval_required_before_runtime": review_policy.get(
                "owner_approval_required_before_runtime",
                True,
            ),
            "review_reasons": _strings(review_policy.get("review_reasons")),
            "allowed_current_effect": "manual_review_contract_only",
            "blocked_actions": [
                "automatic_trade_approval",
                "target_weight_change",
                "rebalance_instruction",
                "broker_order",
                "paper_shadow_order",
                "production_decision",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_risk_cap_execution_transition_matrix(
    *,
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in records(policy.get("transition_rules")):
        rows.append(
            clean_for_yaml(
                {
                    "rule_id": rule.get("rule_id", ""),
                    "from_state": rule.get("from_state", ""),
                    "to_state": rule.get("to_state", ""),
                    "evidence_required": ",".join(_strings(rule.get("evidence_required"))),
                    "transition_status": "DESIGN_ONLY_NOT_EXECUTABLE",
                    "source_runtime_started": _summary_value(
                        source["summary"],
                        "runtime_started",
                    ),
                    "automatic_transition_allowed": False,
                    "portfolio_effect": "none",
                    "broker_action": "none",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_risk_cap_cooldown_decay_safety_boundary(
    *,
    generated_at: datetime,
    state_contract: Mapping[str, Any],
    rule_rows: Sequence[Mapping[str, Any]],
    exposure_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "state_count": state_contract.get("state_count", 0),
            "cooldown_decay_rule_count": len(rule_rows),
            "exposure_cap_state_count": len(exposure_rows),
            "transition_count": len(transition_rows),
            "does_not_read_portfolio_weights": True,
            "does_not_write_portfolio_weights": True,
            "does_not_generate_target_weights": True,
            "does_not_generate_rebalance_instruction": True,
            "does_not_generate_broker_order": True,
            "does_not_start_forward_observe_runtime": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_requirement": (
                "Design-only execution mechanics package consumes only TRADING-2294 "
                "artifacts. Future runtime, simulation, daily report, portfolio or "
                "backtest workflows must run aits validate-data or the same validation "
                "code path before consuming cached market data."
            ),
            "allowed_next_step": (
                "TRADING-2322_SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_OR_"
                "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION_AFTER_OWNER_REVIEW"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_risk_cap_cooldown_decay_design_summary(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    source: Mapping[str, Any],
    state_contract: Mapping[str, Any],
    rule_rows: Sequence[Mapping[str, Any]],
    exposure_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
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
                "source_data_quality_status",
            ),
            "source_observe_mode": _summary_value(source["summary"], "observe_mode"),
            "source_runtime_started": _summary_value(source["summary"], "runtime_started"),
            "source_forward_observe_started": _summary_value(
                source["summary"],
                "forward_observe_started",
            ),
            "source_daily_report_integration": _summary_value(
                source["summary"],
                "daily_report_integration",
            ),
            "state_count": state_contract.get("state_count", 0),
            "cooldown_decay_rule_count": len(rule_rows),
            "exposure_cap_state_count": len(exposure_rows),
            "transition_count": len(transition_rows),
            "risk_cap_cooldown_decay_design_cli_implemented": True,
            "execution_state_contract_generated": True,
            "manual_review_mode_contract_generated": True,
            "executable_execution_policy_ready": False,
            "selected_market_regime": MARKET_REGIME,
            **SAFETY_FIELDS,
        }
    )


def write_risk_cap_cooldown_decay_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    state_contract: Mapping[str, Any],
    rule_rows: Sequence[Mapping[str, Any]],
    exposure_rows: Sequence[Mapping[str, Any]],
    manual_review_contract: Mapping[str, Any],
    transition_rows: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "risk_cap_cooldown_decay_design_summary.json",
        "state_contract": output_dir / "risk_cap_execution_state_contract.json",
        "rule_matrix_json": output_dir / "risk_cap_cooldown_decay_rule_matrix.json",
        "rule_matrix_csv": output_dir / "risk_cap_cooldown_decay_rule_matrix.csv",
        "exposure_cap_json": output_dir / "risk_cap_exposure_cap_state_matrix.json",
        "exposure_cap_csv": output_dir / "risk_cap_exposure_cap_state_matrix.csv",
        "manual_review_contract": output_dir / "risk_cap_manual_review_mode_contract.json",
        "transition_matrix_json": output_dir / "risk_cap_execution_transition_matrix.json",
        "transition_matrix_csv": output_dir / "risk_cap_execution_transition_matrix.csv",
        "safety_boundary": output_dir / "risk_cap_cooldown_decay_safety_boundary.json",
        "report_doc": docs_root / "risk_cap_cooldown_decay_design.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["state_contract"], dict(state_contract))
    write_json(paths["rule_matrix_json"], {**dict(common), "rows": rule_rows})
    write_csv_rows(paths["rule_matrix_csv"], rule_rows)
    write_json(paths["exposure_cap_json"], {**dict(common), "rows": exposure_rows})
    write_csv_rows(paths["exposure_cap_csv"], exposure_rows)
    write_json(paths["manual_review_contract"], dict(manual_review_contract))
    write_json(paths["transition_matrix_json"], {**dict(common), "rows": transition_rows})
    write_csv_rows(paths["transition_matrix_csv"], transition_rows)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            state_contract=state_contract,
            rule_rows=rule_rows,
            exposure_rows=exposure_rows,
            transition_rows=transition_rows,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_report(
    *,
    summary: Mapping[str, Any],
    state_contract: Mapping[str, Any],
    rule_rows: Sequence[Mapping[str, Any]],
    exposure_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
) -> str:
    states = records(state_contract.get("states"))
    return "\n".join(
        [
            "# Risk-Cap Cooldown / Decay Design",
            "",
            "TRADING-2321 承接 TRADING-2294 observe-only evidence plan。"
            "本报告只定义 risk-cap trigger 后的 design-only execution mechanics，"
            "不读取或写入 portfolio weights，不生成 target weight、rebalance "
            "instruction 或 broker order，不启动 paper-shadow、production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- source_observe_mode: `{summary['source_observe_mode']}`",
            f"- source_runtime_started: `{summary['source_runtime_started']}`",
            f"- state_count: `{summary['state_count']}`",
            f"- cooldown_decay_rule_count: `{summary['cooldown_decay_rule_count']}`",
            f"- exposure_cap_state_count: `{summary['exposure_cap_state_count']}`",
            f"- transition_count: `{summary['transition_count']}`",
            "- target_weight_generated: `False`",
            "- rebalance_instruction_generated: `False`",
            "- broker_order_generated: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Execution States",
            "",
            "|state_id|state_role|allowed_current_effect|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['state_id']}`|{row['state_role']}|"
                    f"{row['allowed_current_effect']}|"
                )
                for row in states
            ],
            "",
            "## Cooldown / Decay Rules",
            "",
            "|rule_id|checkpoint|rule_status|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['rule_id']}`|`{row['checkpoint']}`|"
                    f"`{row['rule_status']}`|"
                )
                for row in rule_rows
            ],
            "",
            "## Exposure Cap States",
            "",
            "|state_id|cap_scope|cap_multiplier_status|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['state_id']}`|{row['cap_scope']}|"
                    f"`{row['cap_multiplier_status']}`|"
                )
                for row in exposure_rows
            ],
            "",
            "## Transitions",
            "",
            "|rule_id|from_state|to_state|transition_status|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['rule_id']}`|`{row['from_state']}`|`{row['to_state']}`|"
                    f"`{row['transition_status']}`|"
                )
                for row in transition_rows
            ],
            "",
            "## Boundary",
            "",
            "当前 cap multiplier、cooldown duration 和 release threshold 均未校准为"
            "可执行 policy。后续若进入 TRADING-2323 simulation 或 runtime implementation，"
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
        "title": "Risk-Cap Cooldown / Decay Design",
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
        "actual_requested_date_range": "design_only_execution_mechanics_contract",
        "data_quality_status": DATA_QUALITY_STATUS,
        "source_data_quality_status": _summary_value(
            source_summary,
            "source_data_quality_status",
        ),
        "data_quality_requirement": (
            "Design-only execution mechanics package; no cached market data or "
            "portfolio data are read. Future runtime, simulation, scoring, reports "
            "or backtests must run aits validate-data when cached data is consumed."
        ),
        **SAFETY_FIELDS,
    }


def _validate_trading_2294_source(
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = source["summary"]
    dependency = mapping(policy.get("source_dependency"))
    expected = {
        "task_id": dependency.get("required_task_id"),
        "status": dependency.get("required_status"),
        "observe_mode": dependency.get("required_observe_mode"),
        "daily_report_integration": dependency.get("required_daily_report_integration"),
        "weekly_report_integration": dependency.get("required_weekly_report_integration"),
        "broker_action": dependency.get("required_broker_action"),
        "candidate_id": dependency.get("required_candidate_id"),
    }
    for field, expected_value in expected.items():
        if _summary_value(summary, field) != expected_value:
            raise RiskCapCooldownDecayDesignError(
                f"TRADING-2294 source {field} mismatch: {_summary_value(summary, field)}"
            )
    if _summary_value(summary, "runtime_started") is not dependency.get(
        "required_runtime_started"
    ):
        raise RiskCapCooldownDecayDesignError("TRADING-2294 runtime_started mismatch")
    if _summary_value(summary, "forward_observe_started") is not dependency.get(
        "required_forward_observe_started"
    ):
        raise RiskCapCooldownDecayDesignError(
            "TRADING-2294 forward_observe_started mismatch"
        )
    for key, payload in source.items():
        _validate_source_safety(f"source.{key}", payload)
    if source["daily_schema"].get("allowed_action_values") != ["observe_only"]:
        raise RiskCapCooldownDecayDesignError("TRADING-2294 daily schema is not observe_only")
    if not records(mapping(source.get("decision_matrix")).get("rows")):
        raise RiskCapCooldownDecayDesignError("TRADING-2294 decision matrix empty")


def _validate_source_safety(name: str, payload: Mapping[str, Any]) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise RiskCapCooldownDecayDesignError(f"{name} opens promotion_allowed")
        if item.get("paper_shadow_allowed") is True:
            raise RiskCapCooldownDecayDesignError(f"{name} opens paper_shadow_allowed")
        if item.get("production_allowed") is True:
            raise RiskCapCooldownDecayDesignError(f"{name} opens production_allowed")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise RiskCapCooldownDecayDesignError(f"{name} opens broker_action")
        if item.get("runtime_started") is True:
            raise RiskCapCooldownDecayDesignError(f"{name} opens runtime_started")
        if item.get("forward_observe_started") is True:
            raise RiskCapCooldownDecayDesignError(f"{name} opens forward_observe_started")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise RiskCapCooldownDecayDesignError(f"{name} opens {forbidden}")


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
        "required_execution_states",
        "cooldown_decay_checkpoints",
        "transition_rules",
        "cap_calibration_policy",
        "manual_review_policy",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise RiskCapCooldownDecayDesignError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "risk_cap_cooldown_decay_design_policy":
        raise RiskCapCooldownDecayDesignError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise RiskCapCooldownDecayDesignError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise RiskCapCooldownDecayDesignError("policy market_regime mismatch")
    if set(mapping(policy.get("required_execution_states"))) != REQUIRED_STATES:
        raise RiskCapCooldownDecayDesignError("policy execution states mismatch")
    if not _strings(policy.get("cooldown_decay_checkpoints")):
        raise RiskCapCooldownDecayDesignError("policy cooldown checkpoints required")
    if not records(policy.get("transition_rules")):
        raise RiskCapCooldownDecayDesignError("policy transition rules required")
    if mapping(policy.get("cap_calibration_policy")).get("cap_multiplier_defined") is not False:
        raise RiskCapCooldownDecayDesignError("cap multiplier must not be defined")
    if mapping(policy.get("manual_review_policy")).get("manual_review_only") is not True:
        raise RiskCapCooldownDecayDesignError("manual_review_only must be true")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise RiskCapCooldownDecayDesignError(
                f"policy safety.{field} must be {expected}"
            )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RiskCapCooldownDecayDesignError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise RiskCapCooldownDecayDesignError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RiskCapCooldownDecayDesignError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RiskCapCooldownDecayDesignError(f"JSON must be object: {path}")
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


def _cap_scope(state_id: str) -> str:
    if state_id == "no_add_mode":
        return "block_new_additions_only_design"
    if state_id == "reduced_max_exposure_mode":
        return "future_max_exposure_cap_design_requires_calibration"
    if state_id == "cooldown_mode":
        return "hold_release_until_review_checkpoint_design"
    return "manual_review_no_numeric_cap"


__all__ = [
    "ARTIFACT_ROLE",
    "DATA_QUALITY_STATUS",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_SOURCE_ROOT",
    "MODE",
    "REPORT_TYPE",
    "REQUIRED_STATES",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "RiskCapCooldownDecayDesignError",
    "build_risk_cap_cooldown_decay_rule_matrix",
    "build_risk_cap_execution_state_contract",
    "build_risk_cap_execution_transition_matrix",
    "build_risk_cap_exposure_cap_state_matrix",
    "load_trading_2294_forward_observe_plan_artifacts",
    "run_risk_cap_cooldown_decay_design",
]
