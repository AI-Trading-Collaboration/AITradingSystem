from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as m2431,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_text_document_or_missing_flag as _load_text_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
    write_section_json_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.research_quality import growth_tilt_candidate_gauntlet_harness as harness
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2432"
TASK_REGISTER_ID = "TRADING-2432_GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS"
REPORT_TYPE = harness.REPORT_TYPE
SCHEMA_VERSION = harness.SCHEMA_VERSION
READY_STATUS = harness.READY_STATUS
BLOCKED_STATUS = harness.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_CANDIDATE_GAUNTLET_HARNESS_PRIOR_ARTIFACTS_CONFIG_DOCS_"
    "ONLY_NO_FRESH_MARKET_DATA_NO_BATCH_SCREEN"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "read_fresh_cached_market_data",
    "run_candidate_batch_screen",
    "run_candidate_gauntlet",
    "run_historical_screen",
    "run_pit_replay",
    "run_backtest",
    "run_scoring",
    "generate_daily_report",
    "generate_real_signal",
    "backfill_real_outcome",
    "bind_real_outcome",
    "generate_trading_advice",
    "generate_actionable_allocation_change",
    "generate_broker_order",
    "modify_actual_portfolio_weights",
    "enable_paper_shadow",
    "enable_paper_shadow_schedule",
    "create_scheduled_task",
    "enable_production",
    "call_broker_api",
    "send_order",
    "set_new_investment_threshold_values",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "candidate_search_resumed",
    "candidate_batch_screen_run",
    "candidate_gauntlet_run",
    "observation_enabled",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_shadow_approved",
    "paper_shadow_schedule_enabled",
    "paper_shadow_daily_job_run",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "outcome_store_mutated",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "order_generated",
    "broker_order_generated",
    "daily_report_generated",
    "daily_report_run",
    "new_feature_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "allocation_change_generated",
    "recommendation_generated",
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "new_strategy_backtest_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
    "portfolio_weight_mutated",
    "actual_portfolio_weights_modified",
    "automatic_execution_allowed",
    "new_investment_threshold_values_set",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_MATRIX_PATH = (
    m2431.DEFAULT_OUTPUT_ROOT / "existing_candidate_evidence_matrix_result.json"
)
DEFAULT_CANDIDATE_SET_PATH = (
    PROJECT_ROOT / "research" / "configs" / "growth_tilt" / "candidate_set_2432.yaml"
)
DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_existing_candidate_evidence_matrix.md"
)
DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_TABLE_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_existing_candidate_evidence_matrix_table.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_candidate_gauntlet_harness(
    *,
    source_2431_existing_candidate_evidence_matrix_path: Path = (
        DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_MATRIX_PATH
    ),
    candidate_set_path: Path = DEFAULT_CANDIDATE_SET_PATH,
    existing_candidate_evidence_matrix_doc_path: Path = (
        DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_DOC_PATH
    ),
    existing_candidate_evidence_matrix_table_doc_path: Path = (
        DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_TABLE_DOC_PATH
    ),
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2431_existing_candidate_evidence_matrix": _load_json_document(
            source_2431_existing_candidate_evidence_matrix_path
        ),
        "candidate_set": _load_yaml_document(candidate_set_path),
        "existing_candidate_evidence_matrix_doc": _load_text_document(
            existing_candidate_evidence_matrix_doc_path
        ),
        "existing_candidate_evidence_matrix_table_doc": _load_text_document(
            existing_candidate_evidence_matrix_table_doc_path
        ),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_validation_errors = _source_validation_errors(sources)
    if source_validation_errors:
        payload = _blocked_payload(
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
        )
    else:
        research_doc_texts = {
            name: _as_mapping(document).get("text", "")
            for name, document in sources.items()
            if name.endswith("_doc")
        }
        payload = harness.build_growth_tilt_candidate_gauntlet_harness(
            _as_mapping(sources["source_2431_existing_candidate_evidence_matrix"]),
            _as_mapping(sources["candidate_set"]),
            report_registry=_as_mapping(sources["report_registry"]),
            artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                "text",
                "",
            ),
            system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
            research_doc_texts=research_doc_texts,
        )
        payload = _with_runtime_metadata(
            payload,
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
            candidate_set_path=candidate_set_path,
        )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_yaml_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return safe_load_yaml_path(path)


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, document in sources.items():
        if isinstance(document, Mapping) and document.get("_missing") is True:
            errors.append(f"{name} missing: {document.get('_path')}")
    return errors


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
    candidate_set_path: Path,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date) if as_of_date else enriched.get("as_of"),
            "generated_at": utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "candidate_set_path": str(candidate_set_path),
            "data_quality_gate_executed": False,
            "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
            "manual_review_required": True,
            "manual_review_only": True,
            "observe_only": True,
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    for field in SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _blocked_payload(
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
) -> dict[str, Any]:
    gaps = [
        {
            "requirement_id": "source_artifact_availability",
            "classification": "candidate_gauntlet_harness_source_gap",
            "gap": (
                "Required TRADING-2431 artifact, candidate-set config, registry, "
                "catalog, system flow, or research doc is missing."
            ),
            "evidence": {"source_validation_errors": list(source_validation_errors)},
            "production_effect": "none",
            "broker_action": "none",
        }
    ]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": BLOCKED_STATUS,
        "readiness_status": BLOCKED_STATUS,
        "source_2431_ready": False,
        "candidate_set_ready": False,
        "candidate_set_id": "",
        "harness_ready": False,
        "baseline_ready": False,
        "metrics_ready": False,
        "kill_criteria_ready": False,
        "promotion_criteria_ready": False,
        "regime_slices_ready": False,
        "parameter_plateau_check_ready": False,
        "ablation_output_ready": False,
        "candidate_group_count": 0,
        "candidates_tested": 0,
        "contract_gap_count": len(gaps),
        "contract_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "growth_tilt_candidate_gauntlet_result": {
            "candidate_set_id": "",
            "candidates_tested": 0,
            "harness_ready": False,
            "baseline_ready": False,
            "metrics_ready": False,
            "kill_criteria_ready": False,
            "promotion_criteria_ready": False,
            "regime_slices_ready": False,
            "parameter_plateau_check_ready": False,
            "ablation_output_ready": False,
            "next_route": harness.BLOCKED_ROUTE,
        },
        "candidate_set_snapshot": {
            "schema_version": harness.CANDIDATE_SET_SNAPSHOT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_set_ready": False,
            "candidate_group_count": 0,
            "candidates_tested": 0,
            "candidate_groups": [],
        },
        "baseline_contract": {
            "schema_version": harness.BASELINE_CONTRACT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "baseline_ready": False,
        },
        "metric_contract": {
            "schema_version": harness.METRIC_CONTRACT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "metrics_ready": False,
        },
        "criteria_contract": {
            "schema_version": harness.CRITERIA_CONTRACT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "kill_criteria_ready": False,
            "promotion_criteria_ready": False,
        },
        "regime_plateau_ablation_contract": {
            "schema_version": harness.REGIME_PLATEAU_ABLATION_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "parameter_plateau_check_ready": False,
            "regime_slices_ready": False,
            "ablation_output_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": harness.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": harness.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required source artifacts or candidate-set config are missing; "
            "gauntlet harness cannot silently pass."
        ),
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
        candidate_set_path=DEFAULT_CANDIDATE_SET_PATH,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "candidate_gauntlet_result.json"
    candidate_set_json_path = output_root / "candidate_set_snapshot.json"
    baseline_json_path = output_root / "gauntlet_baseline_contract.json"
    metric_json_path = output_root / "gauntlet_metric_contract.json"
    criteria_json_path = output_root / "gauntlet_criteria_contract.json"
    regime_json_path = output_root / "regime_plateau_ablation_contract.json"
    no_effect_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_candidate_gauntlet_harness.md"
    candidate_set_markdown_path = docs_root / "growth_tilt_candidate_set_2432.md"
    baseline_markdown_path = (
        docs_root / "growth_tilt_candidate_gauntlet_baseline_contract.md"
    )
    metric_markdown_path = docs_root / "growth_tilt_candidate_gauntlet_metric_contract.md"
    criteria_markdown_path = (
        docs_root / "growth_tilt_candidate_gauntlet_criteria_contract.md"
    )
    regime_markdown_path = (
        docs_root / "growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract.md"
    )
    no_effect_markdown_path = (
        docs_root / "growth_tilt_candidate_gauntlet_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2433_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_set_snapshot_json": str(candidate_set_json_path),
        "baseline_contract_json": str(baseline_json_path),
        "metric_contract_json": str(metric_json_path),
        "criteria_contract_json": str(criteria_json_path),
        "regime_plateau_ablation_contract_json": str(regime_json_path),
        "no_effect_boundary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "candidate_set_snapshot_markdown": str(candidate_set_markdown_path),
        "baseline_contract_markdown": str(baseline_markdown_path),
        "metric_contract_markdown": str(metric_markdown_path),
        "criteria_contract_markdown": str(criteria_markdown_path),
        "regime_plateau_ablation_contract_markdown": str(regime_markdown_path),
        "no_effect_boundary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        candidate_set_json_path,
        "growth_tilt_candidate_set_2432",
        harness.CANDIDATE_SET_SNAPSHOT_SCHEMA_VERSION,
        payload,
        "candidate_set_snapshot",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        baseline_json_path,
        "growth_tilt_candidate_gauntlet_baseline_contract",
        harness.BASELINE_CONTRACT_SCHEMA_VERSION,
        payload,
        "baseline_contract",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        metric_json_path,
        "growth_tilt_candidate_gauntlet_metric_contract",
        harness.METRIC_CONTRACT_SCHEMA_VERSION,
        payload,
        "metric_contract",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        criteria_json_path,
        "growth_tilt_candidate_gauntlet_criteria_contract",
        harness.CRITERIA_CONTRACT_SCHEMA_VERSION,
        payload,
        "criteria_contract",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        regime_json_path,
        "growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract",
        harness.REGIME_PLATEAU_ABLATION_SCHEMA_VERSION,
        payload,
        "regime_plateau_ablation_contract",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_candidate_gauntlet_no_effect_boundary",
        harness.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        candidate_set_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Set 2432",
            payload.get("candidate_set_snapshot"),
        ),
    )
    write_markdown_artifact(
        baseline_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Gauntlet Baseline Contract",
            payload.get("baseline_contract"),
        ),
    )
    write_markdown_artifact(
        metric_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Gauntlet Metric Contract",
            payload.get("metric_contract"),
        ),
    )
    write_markdown_artifact(
        criteria_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Gauntlet Criteria Contract",
            payload.get("criteria_contract"),
        ),
    )
    write_markdown_artifact(
        regime_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Gauntlet Regime Plateau Ablation Contract",
            payload.get("regime_plateau_ablation_contract"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Gauntlet No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "candidate_set_id": payload.get("candidate_set_id"),
        "candidates_tested": payload.get("candidates_tested"),
        "harness_ready": payload.get("harness_ready"),
        "baseline_ready": payload.get("baseline_ready"),
        "metrics_ready": payload.get("metrics_ready"),
        "kill_criteria_ready": payload.get("kill_criteria_ready"),
        "promotion_criteria_ready": payload.get("promotion_criteria_ready"),
        "regime_slices_ready": payload.get("regime_slices_ready"),
        "parameter_plateau_check_ready": payload.get("parameter_plateau_check_ready"),
        "ablation_output_ready": payload.get("ablation_output_ready"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Candidate Gauntlet Harness",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- candidate set id：`{payload.get('candidate_set_id')}`",
            f"- candidates tested：`{payload.get('candidates_tested')}`",
            f"- harness ready：`{payload.get('harness_ready')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2432 只建立 batch gauntlet harness contract，不执行真实 "
            "candidate batch screen、historical screen、PIT replay、backtest 或 "
            "scoring。默认不批准 paper-shadow、schedule、production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Set Snapshot",
            "",
            "```json",
            _json_block(payload.get("candidate_set_snapshot", {})),
            "```",
            "",
            "## Criteria Contract",
            "",
            "```json",
            _json_block(payload.get("criteria_contract", {})),
            "```",
            "",
            "## No-Effect Boundary",
            "",
            "```json",
            _json_block(payload.get("no_effect_boundary", {})),
            "```",
            "",
        ]
    )


def _render_section_markdown(title: str, section: object) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            "```json",
            _json_block(section if isinstance(section, Mapping) else {}),
            "```",
            "",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2433 Route",
            "",
            "- source task：`TRADING-2432`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2433 should run the false risk-off / missed upside batch "
            "screen using the harness contract. TRADING-2432 does not execute "
            "the batch screen, PIT replay, market-data experiment, backtest, "
            "scoring, paper-shadow, production, broker, new signal, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
