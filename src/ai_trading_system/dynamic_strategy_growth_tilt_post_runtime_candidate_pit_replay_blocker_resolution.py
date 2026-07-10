from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as m2438b,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation as m2438l,  # noqa: E501
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
    growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as resolution,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438M"
TASK_REGISTER_ID = (
    "TRADING-2438M_GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION"
)
REPORT_TYPE = resolution.REPORT_TYPE
SCHEMA_VERSION = resolution.SCHEMA_VERSION
READY_STATUS = resolution.READY_STATUS
PARTIAL_STATUS = resolution.PARTIAL_STATUS
BLOCKED_STATUS = resolution.BLOCKED_STATUS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438L_PATH = (
    m2438l.DEFAULT_OUTPUT_ROOT / "recheck_after_runtime_remediation_result.json"
)
DEFAULT_CANDIDATE_CONFIG_PATH = (
    PROJECT_ROOT
    / "research"
    / "configs"
    / "growth_tilt"
    / "false_risk_off_missed_upside_2433.yaml"
)
DEFAULT_ENGINE_CONTRACT_PATH = m2438b.DEFAULT_OUTPUT_ROOT / "pit_replay_engine_contract.json"
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_Resolution.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "historical_screen_run",
    "pit_replay_run",
    "backtest_run",
    "scoring_run",
    "daily_report_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "forward_aging_observation_started",
    "forward_aging_observation_written",
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
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "broker_order_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "portfolio_weight_mutated",
    "automatic_execution_allowed",
)


def run_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
    *,
    source_2438l_path: Path = DEFAULT_SOURCE_2438L_PATH,
    candidate_config_path: Path = DEFAULT_CANDIDATE_CONFIG_PATH,
    engine_contract_path: Path = DEFAULT_ENGINE_CONTRACT_PATH,
    runtime_evaluation_input_path: Path | None = None,
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
    source_run_id: str | None = None,
    candidate_limit: int = 3,
    strict: bool = False,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2438l": _load_json_document(source_2438l_path),
        "candidate_config": _load_yaml_document(candidate_config_path),
        "engine_contract": _load_json_document(engine_contract_path),
        "requirement_doc": _load_text_document(requirement_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    if runtime_evaluation_input_path is None:
        runtime_inputs: Mapping[str, Any] = {}
    else:
        runtime_inputs = _as_mapping(_load_structured_document(runtime_evaluation_input_path))
        sources["runtime_evaluation_input"] = runtime_inputs
    source_errors = _source_validation_errors(sources)
    resolved_as_of = as_of_date or date.today()
    if data_quality_summary_path is not None:
        data_quality_summary = _load_data_quality_summary(data_quality_summary_path)
    else:
        data_quality_summary = _run_data_quality_gate(
            prices_path=prices_path,
            rates_path=rates_path,
            as_of_date=resolved_as_of,
            output_path=data_quality_output_path,
        )
    source_documents = [
        (source_2438l_path, sources["source_2438l"]),
        (candidate_config_path, sources["candidate_config"]),
        (engine_contract_path, sources["engine_contract"]),
        (requirement_doc_path, sources["requirement_doc"]),
        (report_registry_path, sources["report_registry"]),
        (artifact_catalog_path, sources["artifact_catalog"]),
        (system_flow_path, sources["system_flow"]),
    ]
    if runtime_evaluation_input_path is not None:
        source_documents.append(
            (runtime_evaluation_input_path, sources["runtime_evaluation_input"])
        )
    payload = resolution.build_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
        _as_mapping(sources["source_2438l"]),
        _as_mapping(sources["candidate_config"]),
        _as_mapping(sources["engine_contract"]),
        runtime_inputs,
        data_quality_summary,
        source_artifacts=_source_artifact_records(source_documents),
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=str(
            _as_mapping(sources["artifact_catalog"]).get("text", "")
        ),
        system_flow_text=str(_as_mapping(sources["system_flow"]).get("text", "")),
        requirement_text=str(_as_mapping(sources["requirement_doc"]).get("text", "")),
        as_of=str(resolved_as_of),
        candidate_limit=candidate_limit,
        source_run_id=source_run_id,
    )
    payload = _with_runtime_metadata(
        payload,
        source_validation_errors=source_errors,
        as_of_date=resolved_as_of,
    )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    strict_errors = source_errors + [
        str(item) for item in payload.get("strict_validation_errors", [])
    ]
    if strict and strict_errors:
        raise ValueError("; ".join(strict_errors))
    return payload


def _load_yaml_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return safe_load_yaml_path(path)


def _load_structured_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
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
    as_of_date: date,
    output_path: Path | None,
) -> dict[str, Any]:
    universe = load_universe()
    quality_config = load_data_quality()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        as_of_date,
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
        as_of=as_of_date,
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


def _source_artifact_records(
    source_documents: list[tuple[Path, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "path": str(path.resolve()),
            "sha256": _sha256(path),
            "schema_version": _as_mapping(document).get("schema_version"),
        }
        for path, document in source_documents
        if path.exists()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date),
            "generated_at": utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "task_register_id": TASK_REGISTER_ID,
            "manual_review_required": True,
            "manual_review_only": True,
            "observe_only": True,
            "candidate_only": True,
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
    sections = {
        "growth_tilt_candidate_runtime_stage_trace": (
            "growth_tilt_candidate_runtime_stage_trace.json"
        ),
        "growth_tilt_candidate_runtime_metric_materialization": (
            "growth_tilt_candidate_runtime_metric_materialization.json"
        ),
        "growth_tilt_candidate_runtime_threshold_evaluations": (
            "growth_tilt_candidate_runtime_threshold_evaluations.json"
        ),
        "growth_tilt_candidate_runtime_blocker_matrix": (
            "growth_tilt_candidate_runtime_blocker_matrix.json"
        ),
        "growth_tilt_candidate_runtime_provenance": (
            "growth_tilt_candidate_runtime_provenance.json"
        ),
    }
    primary_path = (
        output_root / "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.json"
    )
    markdown_path = (
        docs_root / "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.md"
    )
    artifact_paths = {
        "json_path": str(primary_path),
        "markdown_path": str(markdown_path),
        **{section: str(output_root / filename) for section, filename in sections.items()},
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(primary_path, payload)
    for section_name, filename in sections.items():
        section = _as_mapping(payload.get(section_name))
        write_section_json_artifact(
            output_root / filename,
            section_name,
            str(section.get("schema_version") or SCHEMA_VERSION),
            payload,
            section_name,
            task_id=TASK_ID,
        )
    write_markdown_artifact(markdown_path, _render_markdown(payload))


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "market_regime": payload.get("market_regime"),
        "candidate_count": payload.get("candidate_count"),
        "runtime_invoked_candidate_count": payload.get(
            "runtime_invoked_candidate_count"
        ),
        "computed_runtime_metric_count": payload.get(
            "computed_runtime_metric_count"
        ),
        "null_runtime_metric_count": payload.get("null_runtime_metric_count"),
        "completed_threshold_evaluation_count": payload.get(
            "completed_threshold_evaluation_count"
        ),
        "missing_threshold_evaluation_count": payload.get(
            "missing_threshold_evaluation_count"
        ),
        "pass_fail_blocked": [
            payload.get("pass_count"),
            payload.get("fail_count"),
            payload.get("blocked_count"),
        ],
        "unresolved_blocker_count": payload.get("unresolved_blocker_count"),
        "next_route": payload.get("recommended_next_research_task"),
        "data_quality_status": payload.get("data_quality_status"),
    }
    candidate_summary = [
        {
            "candidate_id": item.get("candidate_id"),
            "source_rank": item.get("source_rank"),
            "first_failed_stage": item.get("first_failed_stage"),
            "runtime_executable": _as_mapping(item.get("runtime_contract")).get(
                "executable"
            ),
            "outcome_status": _as_mapping(item.get("candidate_replay_outcome")).get(
                "status"
            ),
            "blocker_codes": _as_mapping(item.get("candidate_replay_outcome")).get(
                "blocker_codes"
            ),
        }
        for item in payload.get("candidate_results", [])
        if isinstance(item, Mapping)
    ]
    return "\n".join(
        [
            "# Growth Tilt Post-Runtime Candidate PIT Replay Blocker Resolution",
            "",
            f"- task_id: `{TASK_ID}`",
            f"- status: `{payload.get('status')}`",
            f"- market regime: `{payload.get('market_regime')}`",
            f"- requested date: `{payload.get('as_of')}`",
            f"- data quality: `{payload.get('data_quality_status')}`",
            f"- pass / fail / blocked: `{payload.get('pass_count')}` / "
            f"`{payload.get('fail_count')}` / `{payload.get('blocked_count')}`",
            f"- next route: `{payload.get('recommended_next_research_task')}`",
            "",
            "本报告只解析 validation-only candidate runtime evidence。当前若缺少受审的 "
            "candidate executable spec、真实 replay output 或 threshold policy，结果必须保持 "
            "BLOCKED；不得把 null 转为 0，不得从候选名称推断参数，不得把静态 threshold "
            "contract 计为 runtime evaluation。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Stage Summary",
            "",
            "```json",
            _json_block(candidate_summary),
            "```",
            "",
            "## Blocker Taxonomy Counts",
            "",
            "```json",
            _json_block(payload.get("blockers_by_code", {})),
            "```",
            "",
            "完整 stage trace、metric materialization、threshold evaluations、blocker "
            "records 和 provenance 见同目录 supporting JSON artifacts。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
