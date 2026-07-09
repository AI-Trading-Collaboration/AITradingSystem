from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as m2438b,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_remaining_candidate_pit_replay_blocker_closure as m2438h,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure as m2438f,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure as m2438i,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as m2438d,  # noqa: E501
)
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
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
from ai_trading_system.research_quality import (
    growth_tilt_persistent_candidate_pit_replay_blocker_escalation as escalation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438J"
TASK_REGISTER_ID = (
    "TRADING-2438J_GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_"
    "BLOCKER_ESCALATION"
)
REPORT_TYPE = escalation.REPORT_TYPE
SCHEMA_VERSION = escalation.SCHEMA_VERSION
READY_STATUS = escalation.READY_STATUS
BLOCKED_STATUS = escalation.BLOCKED_STATUS

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "pit_replay_executed",
    "backtest_run",
    "scoring_run",
    "daily_report_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "forward_aging_observation_started",
    "forward_aging_observation_written",
    "candidate_tracking_started",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "outcome_store_mutated",
    "paper_shadow_candidate_found",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_shadow_approved",
    "paper_shadow_schedule_enabled",
    "paper_shadow_daily_job_run",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "broker_order_generated",
    "daily_report_generated",
    "new_feature_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "portfolio_weight_mutated",
    "automatic_execution_allowed",
    "forward_aging_handoff_ready",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438I_BLOCKED_RECHECK_PATH = (
    m2438i.DEFAULT_OUTPUT_ROOT / "recheck_after_remaining_blocker_closure_result.json"
)
DEFAULT_PERSISTENT_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438i.DEFAULT_OUTPUT_ROOT / "persistent_candidate_replay_blocker_summary.json"
)
DEFAULT_SOURCE_2438H_REMAINING_BLOCKER_CLOSURE_PATH = (
    m2438h.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH = (
    m2438f.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "candidate_replay_output_records.json"
)
DEFAULT_SOURCE_2438B_ENGINE_BLOCKER_CLOSURE_PATH = (
    m2438b.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_SOURCE_2438I_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure.md"
)
DEFAULT_PERSISTENT_BLOCKER_SUMMARY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_persistent_candidate_replay_blocker_summary.md"
)
DEFAULT_SOURCE_2438H_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_remaining_candidate_pit_replay_blocker_closure.md"
)
DEFAULT_SOURCE_2438F_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_level_pit_replay_blocker_closure.md"
)
DEFAULT_SOURCE_2438D_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.md"
)
DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_replay_output_records.md"
)
DEFAULT_SOURCE_2438B_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_pit_replay_engine_blocker_closure.md"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_persistent_candidate_pit_replay_blocker_escalation(
    *,
    source_2438i_blocked_recheck_path: Path = DEFAULT_SOURCE_2438I_BLOCKED_RECHECK_PATH,
    persistent_candidate_replay_blocker_summary_path: Path = (
        DEFAULT_PERSISTENT_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH
    ),
    source_2438h_remaining_blocker_closure_path: Path = (
        DEFAULT_SOURCE_2438H_REMAINING_BLOCKER_CLOSURE_PATH
    ),
    source_2438f_candidate_level_blocker_closure_path: Path = (
        DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH
    ),
    source_2438d_output_closure_path: Path = DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH,
    candidate_replay_output_records_path: Path = (
        DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH
    ),
    source_2438b_engine_blocker_closure_path: Path = (
        DEFAULT_SOURCE_2438B_ENGINE_BLOCKER_CLOSURE_PATH
    ),
    source_2438i_doc_path: Path = DEFAULT_SOURCE_2438I_DOC_PATH,
    persistent_blocker_summary_doc_path: Path = (
        DEFAULT_PERSISTENT_BLOCKER_SUMMARY_DOC_PATH
    ),
    source_2438h_doc_path: Path = DEFAULT_SOURCE_2438H_DOC_PATH,
    source_2438f_doc_path: Path = DEFAULT_SOURCE_2438F_DOC_PATH,
    source_2438d_doc_path: Path = DEFAULT_SOURCE_2438D_DOC_PATH,
    candidate_output_records_doc_path: Path = DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    source_2438b_doc_path: Path = DEFAULT_SOURCE_2438B_DOC_PATH,
    requirement_doc_path: Path = DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    data_quality_summary_path: Path | None = None,
    data_quality_output_path: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2438i_blocked_recheck": _load_json_document(
            source_2438i_blocked_recheck_path
        ),
        "persistent_candidate_replay_blocker_summary": _load_json_document(
            persistent_candidate_replay_blocker_summary_path
        ),
        "source_2438h_remaining_blocker_closure": _load_json_document(
            source_2438h_remaining_blocker_closure_path
        ),
        "source_2438f_candidate_level_blocker_closure": _load_json_document(
            source_2438f_candidate_level_blocker_closure_path
        ),
        "source_2438d_output_closure": _load_json_document(
            source_2438d_output_closure_path
        ),
        "candidate_replay_output_records": _load_json_document(
            candidate_replay_output_records_path
        ),
        "source_2438b_engine_blocker_closure": _load_json_document(
            source_2438b_engine_blocker_closure_path
        ),
        "source_2438i_doc": _load_text_document(source_2438i_doc_path),
        "persistent_blocker_summary_doc": _load_text_document(
            persistent_blocker_summary_doc_path
        ),
        "source_2438h_doc": _load_text_document(source_2438h_doc_path),
        "source_2438f_doc": _load_text_document(source_2438f_doc_path),
        "source_2438d_doc": _load_text_document(source_2438d_doc_path),
        "candidate_output_records_doc": _load_text_document(
            candidate_output_records_doc_path
        ),
        "source_2438b_doc": _load_text_document(source_2438b_doc_path),
        "requirement_doc": _load_text_document(requirement_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_validation_errors = _source_validation_errors(sources)
    if data_quality_summary_path is not None:
        data_quality_summary = _load_data_quality_summary(data_quality_summary_path)
    else:
        data_quality_summary = _run_data_quality_gate(
            prices_path=prices_path,
            rates_path=rates_path,
            as_of_date=as_of_date,
            output_path=data_quality_output_path,
        )
    research_doc_texts = {
        name: _as_mapping(document).get("text", "")
        for name, document in sources.items()
        if name.endswith("_doc")
    }
    payload = (
        escalation.build_growth_tilt_persistent_candidate_pit_replay_blocker_escalation(
            _as_mapping(sources["source_2438i_blocked_recheck"]),
            _as_mapping(sources["persistent_candidate_replay_blocker_summary"]),
            _as_mapping(sources["source_2438h_remaining_blocker_closure"]),
            _as_mapping(sources["source_2438f_candidate_level_blocker_closure"]),
            _as_mapping(sources["source_2438d_output_closure"]),
            _as_mapping(sources["candidate_replay_output_records"]),
            _as_mapping(sources["source_2438b_engine_blocker_closure"]),
            data_quality_summary,
            report_registry=_as_mapping(sources["report_registry"]),
            artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                "text",
                "",
            ),
            system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
            research_doc_texts=research_doc_texts,
            as_of=str(as_of_date) if as_of_date else None,
        )
    )
    payload = _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_yaml_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return safe_load_yaml_path(path)


def _load_data_quality_summary(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {
            "data_quality_gate_executed": False,
            "data_quality_gate_passed": False,
            "data_quality_status": "MISSING",
            "data_quality_report_path": str(path),
        }
    return _as_mapping(json.loads(path.read_text(encoding="utf-8")))


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    output_path: Path | None,
) -> dict[str, Any]:
    universe = load_universe()
    quality_config = load_data_quality()
    resolved_as_of = as_of_date or date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        resolved_as_of,
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=False,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=resolved_as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(report, report_path)
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": report.passed,
        "data_quality_status": report.status,
        "data_quality_report_path": str(report_path),
        "data_quality_as_of": str(report.as_of),
        "data_quality_error_count": report.error_count,
        "data_quality_warning_count": report.warning_count,
        "data_quality_info_count": report.info_count,
    }


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_PRICES_PATH.resolve()
    except OSError:
        return prices_path == DEFAULT_PRICES_PATH


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    return [
        f"{name} missing: {document.get('_path')}"
        for name, document in sources.items()
        if isinstance(document, Mapping) and document.get("_missing") is True
    ]


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
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
            "manual_review_required": True,
            "manual_review_only": True,
            "observe_only": True,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    for field in SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "escalation_result.json"
    matrix_path = output_root / "candidate_persistent_blocker_root_cause_matrix.json"
    repeated_path = output_root / "repeated_closure_failure_summary.json"
    remediation_path = output_root / "recommended_remediation_route.json"
    no_forward_path = output_root / "no_forward_aging_safety_decision.json"
    markdown_path = (
        docs_root / "growth_tilt_persistent_candidate_pit_replay_blocker_escalation.md"
    )
    matrix_markdown_path = (
        docs_root / "growth_tilt_candidate_persistent_blocker_root_cause_matrix.md"
    )
    repeated_markdown_path = (
        docs_root / "growth_tilt_repeated_closure_failure_summary.md"
    )
    remediation_markdown_path = (
        docs_root / "growth_tilt_persistent_blocker_recommended_remediation_route.md"
    )
    no_forward_markdown_path = docs_root / "growth_tilt_no_forward_aging_safety_decision.md"
    route_markdown_path = docs_root / "dynamic_strategy_2438K_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_persistent_blocker_root_cause_matrix_json": str(matrix_path),
        "repeated_closure_failure_summary_json": str(repeated_path),
        "recommended_remediation_route_json": str(remediation_path),
        "no_forward_aging_safety_decision_json": str(no_forward_path),
        "markdown_path": str(markdown_path),
        "candidate_persistent_blocker_root_cause_matrix_markdown": str(
            matrix_markdown_path
        ),
        "repeated_closure_failure_summary_markdown": str(repeated_markdown_path),
        "recommended_remediation_route_markdown": str(remediation_markdown_path),
        "no_forward_aging_safety_decision_markdown": str(no_forward_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(matrix_path, "candidate_persistent_blocker_root_cause_matrix", payload)
    _write_section(repeated_path, "repeated_closure_failure_summary", payload)
    _write_section(remediation_path, "recommended_remediation_route", payload)
    _write_section(no_forward_path, "no_forward_aging_safety_decision", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Persistent Blocker Root-Cause Matrix",
            payload.get("candidate_persistent_blocker_root_cause_matrix"),
        ),
    )
    write_markdown_artifact(
        repeated_markdown_path,
        _render_section_markdown(
            "Growth Tilt Repeated Closure Failure Summary",
            payload.get("repeated_closure_failure_summary"),
        ),
    )
    write_markdown_artifact(
        remediation_markdown_path,
        _render_section_markdown(
            "Growth Tilt Persistent Blocker Recommended Remediation Route",
            payload.get("recommended_remediation_route"),
        ),
    )
    write_markdown_artifact(
        no_forward_markdown_path,
        _render_section_markdown(
            "Growth Tilt No Forward-Aging Safety Decision",
            payload.get("no_forward_aging_safety_decision"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _write_section(path: Path, section_name: str, payload: Mapping[str, Any]) -> None:
    section = _as_mapping(payload.get(section_name))
    schema_version = str(section.get("schema_version") or SCHEMA_VERSION)
    write_section_json_artifact(
        path,
        f"growth_tilt_{section_name}",
        schema_version,
        payload,
        section_name,
        task_id=TASK_ID,
    )


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "prior_status": payload.get("prior_status"),
        "data_quality_status": payload.get("data_quality_status"),
        "source_2438i_blocked_recheck_ready": payload.get(
            "source_2438i_blocked_recheck_ready"
        ),
        "persistent_blocker_escalation_required": payload.get(
            "persistent_blocker_escalation_required"
        ),
        "closure_history_confirmed": payload.get("closure_history_confirmed"),
        "candidate_replay_pass_count": payload.get("candidate_replay_pass_count"),
        "candidate_replay_fail_count": payload.get("candidate_replay_fail_count"),
        "candidate_replay_blocked_count": payload.get(
            "candidate_replay_blocked_count"
        ),
        "persistent_blocked_candidate_count": payload.get(
            "persistent_blocked_candidate_count"
        ),
        "forward_aging_handoff_ready": payload.get("forward_aging_handoff_ready"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Persistent Candidate PIT Replay Blocker Escalation",
            "",
            f"- task_id: `{TASK_ID}`",
            f"- status: `{payload.get('status')}`",
            f"- data quality status: `{payload.get('data_quality_status')}`",
            f"- source 2438I blocked recheck ready: "
            f"`{payload.get('source_2438i_blocked_recheck_ready')}`",
            f"- pass / fail / blocked: `{payload.get('candidate_replay_pass_count')}` / "
            f"`{payload.get('candidate_replay_fail_count')}` / "
            f"`{payload.get('candidate_replay_blocked_count')}`",
            f"- persistent blocked candidate count: "
            f"`{payload.get('persistent_blocked_candidate_count')}`",
            f"- closure history confirmed: `{payload.get('closure_history_confirmed')}`",
            f"- forward-aging handoff ready: `{payload.get('forward_aging_handoff_ready')}`",
            f"- next route: `{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438J 只升级 2438I 之后仍然存在的 candidate PIT replay "
            "BLOCKED 证据，对 2438B / 2438D / 2438F / 2438H 多次 closure READY "
            "后仍 pass/fail/blocked=`0/0/3` 的状态做 root-cause 分类。"
            "`ESCALATION_READY` 只表示根因分类 artifact 完整，不表示 replay PASS、"
            "FAIL、NO_PASSING_CANDIDATE、forward-aging ready、paper-shadow candidate "
            "或 production / broker ready。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Root-Cause Matrix",
            "",
            "```json",
            _json_block(
                payload.get("candidate_persistent_blocker_root_cause_matrix", {})
            ),
            "```",
            "",
            "## Repeated Closure Failure",
            "",
            "```json",
            _json_block(payload.get("repeated_closure_failure_summary", {})),
            "```",
            "",
            "## Recommended Remediation Route",
            "",
            "```json",
            _json_block(payload.get("recommended_remediation_route", {})),
            "```",
            "",
            "## No Forward-Aging Safety Decision",
            "",
            "```json",
            _json_block(payload.get("no_forward_aging_safety_decision", {})),
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
            "# Dynamic Strategy TRADING-2438K Route",
            "",
            "- source task: `TRADING-2438J`",
            f"- source status: `{payload.get('status')}`",
            f"- next route: `{payload.get('recommended_next_research_task')}`",
            "",
            "2438J 不启动 forward-aging、paper-shadow、production 或 broker。"
            "下一步必须先处理 persistent candidate PIT replay blocker root cause；"
            "若 escalation evidence 不完整，则先进入 manual review。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
