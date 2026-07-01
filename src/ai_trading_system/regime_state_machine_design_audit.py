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
    strings,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2315_REGIME_STATE_MACHINE_DESIGN_AUDIT"
REPORT_TYPE = "regime_state_machine_design_audit"
ARTIFACT_ROLE = "regime_state_machine_design_audit"
MODE = "design_audit"
STATUS = "REGIME_STATE_MACHINE_DESIGN_AUDIT_READY_DIAGNOSTIC_ONLY"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_STATIC_DESIGN_AUDIT"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "regime_state_machine_design_policy.yaml"
)

EXPECTED_LABELS = (
    "uptrend",
    "late_uptrend",
    "drawdown",
    "panic",
    "rebound",
    "failed_rebound",
    "range_bound",
    "high_volatility",
    "low_volatility",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "diagnostic_only": True,
    "design_audit_only": True,
    "candidate_signal_generated": False,
    "regime_label_series_generated": False,
    "generator_implemented": False,
    "actual_path_validation_executed": False,
    "scope_review_executed": False,
    "forward_observe_started": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class RegimeStateMachineDesignAuditError(ValueError):
    pass


def run_regime_state_machine_design_audit(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise RegimeStateMachineDesignAuditError(
            "regime state machine design audit only supports design_audit mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    label_rows = build_regime_label_taxonomy(policy)
    transition_rows = build_transition_rule_matrix(policy)
    guardrail_rows = build_anti_lookahead_guardrail_matrix(policy)
    use_case_rows = build_candidate_segmentation_use_case_matrix(policy)
    generator_route = build_label_generator_poc_route(
        label_rows=label_rows,
        transition_rows=transition_rows,
        guardrail_rows=guardrail_rows,
        use_case_rows=use_case_rows,
    )
    safety_boundary = build_safety_boundary()
    summary = _summary_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        label_rows=label_rows,
        transition_rows=transition_rows,
        guardrail_rows=guardrail_rows,
        use_case_rows=use_case_rows,
        generator_route=generator_route,
    )
    common = _common_payload(generated_at=generated_at, mode=mode)
    paths = write_regime_state_machine_design_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        label_rows=label_rows,
        transition_rows=transition_rows,
        guardrail_rows=guardrail_rows,
        use_case_rows=use_case_rows,
        generator_route=generator_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "label_rows": label_rows,
            "transition_rows": transition_rows,
            "guardrail_rows": guardrail_rows,
            "candidate_segmentation_use_case_rows": use_case_rows,
            "generator_route": generator_route,
        }
    )


def build_regime_label_taxonomy(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    taxonomy = mapping(policy.get("label_taxonomy"))
    rows: list[dict[str, Any]] = []
    for label_id in EXPECTED_LABELS:
        definition = mapping(taxonomy.get(label_id))
        rows.append(
            clean_for_yaml(
                {
                    "label_id": label_id,
                    "regime_group": definition.get("regime_group", ""),
                    "diagnostic_interpretation": definition.get(
                        "diagnostic_interpretation", ""
                    ),
                    "allowed_usage": strings(definition.get("allowed_usage")),
                    "blocked_usage": strings(definition.get("blocked_usage")),
                    "pit_requirement": definition.get("pit_requirement", ""),
                    "lookahead_risk": definition.get("lookahead_risk", ""),
                    "runtime_generation_allowed": False,
                    "generator_poc_required": True,
                    "label_series_generated": False,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_transition_rule_matrix(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    transition_design = mapping(policy.get("transition_design"))
    allowed_modes = strings(transition_design.get("allowed_transition_modes"))
    disallowed_modes = strings(transition_design.get("disallowed_transition_modes"))
    rows: list[dict[str, Any]] = []
    for row in records(transition_design.get("transition_rows")):
        rows.append(
            clean_for_yaml(
                {
                    "transition_id": row.get("transition_id", ""),
                    "from_state": row.get("from_state", ""),
                    "to_state": row.get("to_state", ""),
                    "trigger_family": row.get("trigger_family", ""),
                    "allowed_confirmation": row.get("allowed_confirmation", ""),
                    "blocked_confirmation": row.get("blocked_confirmation", ""),
                    "allowed_transition_modes": allowed_modes,
                    "disallowed_transition_modes": disallowed_modes,
                    "transition_runtime_implemented": False,
                    "requires_trading_2316_generator_poc": True,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_anti_lookahead_guardrail_matrix(
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in records(policy.get("guardrails")):
        rows.append(
            clean_for_yaml(
                {
                    "guardrail_id": row.get("guardrail_id", ""),
                    "blocked_failure_mode": row.get("blocked_failure_mode", ""),
                    "required_control": row.get("required_control", ""),
                    "runtime_enforced_now": False,
                    "must_be_enforced_before_label_generation": True,
                    "failure_action": "fail_closed_before_trading_2316_label_series",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_candidate_segmentation_use_case_matrix(
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    use_cases = mapping(policy.get("candidate_segmentation_use_cases"))
    rows: list[dict[str, Any]] = []
    for use_case_id, definition_value in use_cases.items():
        definition = mapping(definition_value)
        rows.append(
            clean_for_yaml(
                {
                    "use_case_id": str(use_case_id),
                    "candidate_family": definition.get("candidate_family", ""),
                    "allowed_usage": definition.get("allowed_usage", ""),
                    "blocked_usage": definition.get("blocked_usage", ""),
                    "segmentation_ready_now": False,
                    "requires_label_generator_poc": True,
                    "allowed_next_step": "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC",
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_label_generator_poc_route(
    *,
    label_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.generator_poc_route.v1",
            "task_id": TASK_ID,
            "next_task": "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC",
            "route_status": "READY_FOR_DIAGNOSTIC_POC_DESIGN_ONLY",
            "label_count": len(label_rows),
            "transition_rule_count": len(transition_rows),
            "guardrail_count": len(guardrail_rows),
            "candidate_segmentation_use_case_count": len(use_case_rows),
            "required_before_trading_2316": [
                "define_input_feature_contract",
                "define_known_at_or_lag_policy",
                "run_cached_data_quality_gate_if_cached_data_is_read",
                "write_label_series_schema",
                "write_label_versioning_contract",
                "fail_closed_on_missing_inputs",
            ],
            "blocked_until_trading_2316": [
                "regime_label_series",
                "candidate_segmentation_metrics",
                "actual_path_validation_by_regime",
                "daily_report_integration",
                "portfolio_or_broker_effect",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_safety_boundary() -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "status": STATUS,
        "data_quality_status": DATA_QUALITY_STATUS,
        "does_not_read_cached_market_data": True,
        "does_not_generate_regime_label_series": True,
        "does_not_generate_candidate_signal": True,
        "does_not_run_backtest": True,
        "does_not_run_actual_path_validation": True,
        "does_not_start_forward_observe": True,
        "does_not_allow_direct_strategy_signal": True,
        "does_not_allow_position_sizing": True,
        "does_not_allow_broker_action": True,
        **SAFETY_FIELDS,
    }


def write_regime_state_machine_design_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    label_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    generator_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "regime_state_machine_design_audit_summary.json",
        "label_taxonomy_json": output_dir / "regime_label_taxonomy.json",
        "label_taxonomy_csv": output_dir / "regime_label_taxonomy.csv",
        "transition_rules_json": output_dir / "regime_transition_rule_matrix.json",
        "transition_rules_csv": output_dir / "regime_transition_rule_matrix.csv",
        "guardrails_json": output_dir
        / "regime_anti_lookahead_guardrail_matrix.json",
        "guardrails_csv": output_dir / "regime_anti_lookahead_guardrail_matrix.csv",
        "use_cases_json": output_dir
        / "regime_candidate_segmentation_use_case_matrix.json",
        "use_cases_csv": output_dir
        / "regime_candidate_segmentation_use_case_matrix.csv",
        "generator_route": output_dir / "regime_label_generator_poc_route.json",
        "safety_boundary": output_dir / "regime_state_machine_safety_boundary.json",
        "report_doc": docs_root / "regime_state_machine_design_audit.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["label_taxonomy_json"], {**dict(common), "rows": label_rows})
    write_csv_rows(paths["label_taxonomy_csv"], label_rows)
    write_json(paths["transition_rules_json"], {**dict(common), "rows": transition_rows})
    write_csv_rows(paths["transition_rules_csv"], transition_rows)
    write_json(paths["guardrails_json"], {**dict(common), "rows": guardrail_rows})
    write_csv_rows(paths["guardrails_csv"], guardrail_rows)
    write_json(paths["use_cases_json"], {**dict(common), "rows": use_case_rows})
    write_csv_rows(paths["use_cases_csv"], use_case_rows)
    write_json(paths["generator_route"], {**dict(common), "generator_route": generator_route})
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            label_rows=label_rows,
            transition_rows=transition_rows,
            guardrail_rows=guardrail_rows,
            use_case_rows=use_case_rows,
            generator_route=generator_route,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _summary_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    label_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    generator_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "title": "Regime State Machine Design Audit",
            "task_id": TASK_ID,
            "status": STATUS,
            "artifact_role": ARTIFACT_ROLE,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": "owner_static_design_audit",
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_rationale": (
                "static design audit; no cached market or macro data read"
            ),
            "policy_path": str(policy_path),
            "label_count": len(label_rows),
            "label_ids": [row["label_id"] for row in label_rows],
            "transition_rule_count": len(transition_rows),
            "guardrail_count": len(guardrail_rows),
            "candidate_segmentation_use_case_count": len(use_case_rows),
            "candidate_segmentation_use_cases": [
                row["use_case_id"] for row in use_case_rows
            ],
            "next_task": generator_route["next_task"],
            "generator_route_status": generator_route["route_status"],
            **SAFETY_FIELDS,
        }
    )


def _common_payload(*, generated_at: datetime, mode: str) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Regime State Machine Design Audit",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "owner_static_design_audit",
        "data_quality_status": DATA_QUALITY_STATUS,
        **SAFETY_FIELDS,
    }


def _render_report(
    *,
    summary: Mapping[str, Any],
    label_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    generator_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Regime State Machine Design Audit",
            "",
            "TRADING-2315 只定义 diagnostic-only regime state machine design，不生成 "
            "label series 或交易信号。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- label_count: `{summary['label_count']}`",
            f"- transition_rule_count: `{summary['transition_rule_count']}`",
            f"- guardrail_count: `{summary['guardrail_count']}`",
            (
                "- candidate_segmentation_use_case_count: "
                f"`{summary['candidate_segmentation_use_case_count']}`"
            ),
            f"- next_task: `{summary['next_task']}`",
            "- candidate_signal_generated: `False`",
            "- regime_label_series_generated: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "- dynamic_promotion_status: `BLOCKED`",
            "",
            "## Label Taxonomy",
            "",
            "|label_id|regime_group|pit_requirement|lookahead_risk|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['label_id']}`|`{row['regime_group']}`|"
                    f"`{row['pit_requirement']}`|`{row['lookahead_risk']}`|"
                )
                for row in label_rows
            ],
            "",
            "## Transition Design",
            "",
            "|transition_id|from_state|to_state|blocked_confirmation|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['transition_id']}`|`{row['from_state']}`|"
                    f"`{row['to_state']}`|`{row['blocked_confirmation']}`|"
                )
                for row in transition_rows
            ],
            "",
            "## Anti-Lookahead Guardrails",
            "",
            "|guardrail_id|blocked_failure_mode|required_control|",
            "|---|---|---|",
            *[
                (
                    f"|`{row['guardrail_id']}`|`{row['blocked_failure_mode']}`|"
                    f"`{row['required_control']}`|"
                )
                for row in guardrail_rows
            ],
            "",
            "## Candidate Segmentation Use Cases",
            "",
            "|use_case_id|candidate_family|allowed_usage|blocked_usage|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['use_case_id']}`|`{row['candidate_family']}`|"
                    f"`{row['allowed_usage']}`|`{row['blocked_usage']}`|"
                )
                for row in use_case_rows
            ],
            "",
            "## TRADING-2316 Route",
            "",
            f"- route_status: `{generator_route['route_status']}`",
            "- required_before_trading_2316: `{}`".format(
                ",".join(generator_route["required_before_trading_2316"])
            ),
            "- blocked_until_trading_2316: `{}`".format(
                ",".join(generator_route["blocked_until_trading_2316"])
            ),
            "",
            "## Safety",
            "",
            "本报告不读取 cached market / macro data，不生成 regime label series，不运行 "
            "backtest / actual-path validation，不写入 daily scoring、portfolio weights、"
            "forward observe runtime、production report path 或 broker path。任何后续 "
            "TRADING-2316 label generation 必须重新审计 PIT / known-at contract 和 data "
            "quality gate。",
            "",
        ]
    )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RegimeStateMachineDesignAuditError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise RegimeStateMachineDesignAuditError(f"policy file must be object: {path}")
    return payload


def _validate_policy(policy: Mapping[str, Any]) -> None:
    required_fields = (
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
        "data_quality",
        "label_taxonomy",
        "transition_design",
        "guardrails",
        "candidate_segmentation_use_cases",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise RegimeStateMachineDesignAuditError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "regime_state_machine_design_policy":
        raise RegimeStateMachineDesignAuditError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise RegimeStateMachineDesignAuditError("policy task_id mismatch")
    data_quality = mapping(policy.get("data_quality"))
    if data_quality.get("status") != DATA_QUALITY_STATUS:
        raise RegimeStateMachineDesignAuditError(
            f"policy data_quality.status must be {DATA_QUALITY_STATUS}"
        )
    taxonomy = mapping(policy.get("label_taxonomy"))
    missing_labels = [label_id for label_id in EXPECTED_LABELS if label_id not in taxonomy]
    if missing_labels:
        raise RegimeStateMachineDesignAuditError(
            f"policy missing label taxonomy rows: {missing_labels}"
        )
    if len(records(mapping(policy.get("transition_design")).get("transition_rows"))) == 0:
        raise RegimeStateMachineDesignAuditError("policy transition rows are required")
    if len(records(policy.get("guardrails"))) == 0:
        raise RegimeStateMachineDesignAuditError("policy guardrails are required")
    if not mapping(policy.get("candidate_segmentation_use_cases")):
        raise RegimeStateMachineDesignAuditError(
            "policy candidate segmentation use cases are required"
        )
    safety = mapping(policy.get("safety"))
    for field, expected in SAFETY_FIELDS.items():
        if safety.get(field) != expected:
            raise RegimeStateMachineDesignAuditError(
                f"policy safety.{field} must be {expected}"
            )


__all__ = [
    "ARTIFACT_ROLE",
    "DATA_QUALITY_STATUS",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "EXPECTED_LABELS",
    "MODE",
    "STATUS",
    "RegimeStateMachineDesignAuditError",
    "build_anti_lookahead_guardrail_matrix",
    "build_candidate_segmentation_use_case_matrix",
    "build_regime_label_taxonomy",
    "build_transition_rule_matrix",
    "run_regime_state_machine_design_audit",
]
