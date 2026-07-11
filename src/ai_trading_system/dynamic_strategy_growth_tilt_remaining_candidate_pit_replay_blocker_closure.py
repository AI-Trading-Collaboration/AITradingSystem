from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure as m2438f,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure as m2438g,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as m2438d,
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
from ai_trading_system.research_framework.runtime_metadata import (
    PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS,
    with_pit_replay_observe_only_runtime_metadata,
)
from ai_trading_system.research_quality import (
    growth_tilt_remaining_candidate_pit_replay_blocker_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438H"
TASK_REGISTER_ID = (
    "TRADING-2438H_GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_"
    "BLOCKER_CLOSURE"
)
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_STATUS = closure.BLOCKED_STATUS

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438G_BLOCKED_RECHECK_PATH = (
    m2438g.DEFAULT_OUTPUT_ROOT / "recheck_after_candidate_blocker_closure_result.json"
)
DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH = (
    m2438f.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "candidate_replay_output_records.json"
)
DEFAULT_REMAINING_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438g.DEFAULT_OUTPUT_ROOT / "remaining_candidate_replay_blocker_summary.json"
)
DEFAULT_SOURCE_2438G_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.md"
)
DEFAULT_SOURCE_2438F_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_level_pit_replay_blocker_closure.md"
)
DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_replay_output_records.md"
)
DEFAULT_REMAINING_BLOCKER_SUMMARY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_remaining_candidate_replay_blocker_summary.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_remaining_candidate_pit_replay_blocker_closure(
    *,
    source_2438g_blocked_recheck_path: Path = (
        DEFAULT_SOURCE_2438G_BLOCKED_RECHECK_PATH
    ),
    source_2438f_candidate_level_blocker_closure_path: Path = (
        DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH
    ),
    candidate_replay_output_records_path: Path = (
        DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH
    ),
    remaining_candidate_replay_blocker_summary_path: Path = (
        DEFAULT_REMAINING_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH
    ),
    source_2438g_doc_path: Path = DEFAULT_SOURCE_2438G_DOC_PATH,
    source_2438f_doc_path: Path = DEFAULT_SOURCE_2438F_DOC_PATH,
    candidate_output_records_doc_path: Path = DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    remaining_blocker_summary_doc_path: Path = DEFAULT_REMAINING_BLOCKER_SUMMARY_DOC_PATH,
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
        "source_2438g_blocked_recheck": _load_json_document(
            source_2438g_blocked_recheck_path
        ),
        "source_2438f_candidate_level_blocker_closure": _load_json_document(
            source_2438f_candidate_level_blocker_closure_path
        ),
        "candidate_replay_output_records": _load_json_document(
            candidate_replay_output_records_path
        ),
        "remaining_candidate_replay_blocker_summary": _load_json_document(
            remaining_candidate_replay_blocker_summary_path
        ),
        "source_2438g_doc": _load_text_document(source_2438g_doc_path),
        "source_2438f_doc": _load_text_document(source_2438f_doc_path),
        "candidate_output_records_doc": _load_text_document(
            candidate_output_records_doc_path
        ),
        "remaining_blocker_summary_doc": _load_text_document(
            remaining_blocker_summary_doc_path
        ),
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
    payload = closure.build_growth_tilt_remaining_candidate_pit_replay_blocker_closure(
        _as_mapping(sources["source_2438g_blocked_recheck"]),
        _as_mapping(sources["source_2438f_candidate_level_blocker_closure"]),
        _as_mapping(sources["candidate_replay_output_records"]),
        _as_mapping(sources["remaining_candidate_replay_blocker_summary"]),
        data_quality_summary,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get("text", ""),
        system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
        research_doc_texts=research_doc_texts,
        as_of=str(as_of_date) if as_of_date else None,
    )
    payload = with_pit_replay_observe_only_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
        task_register_id=TASK_REGISTER_ID,
        report_type=REPORT_TYPE,
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


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "blocker_closure_result.json"
    closure_records_path = output_root / "remaining_candidate_blocker_closure_records.json"
    before_after_path = output_root / "remaining_candidate_blocker_before_after_matrix.json"
    handoff_path = output_root / "replay_recheck_readiness_handoff.json"
    unresolved_path = output_root / "unresolved_remaining_candidate_blocker_summary.json"
    boundary_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_remaining_candidate_pit_replay_blocker_closure.md"
    closure_records_markdown_path = (
        docs_root / "growth_tilt_remaining_candidate_replay_blocker_closure_records.md"
    )
    before_after_markdown_path = (
        docs_root / "growth_tilt_remaining_candidate_replay_blocker_before_after.md"
    )
    handoff_markdown_path = docs_root / "growth_tilt_replay_recheck_readiness_handoff.md"
    unresolved_markdown_path = (
        docs_root / "growth_tilt_unresolved_remaining_candidate_replay_blockers.md"
    )
    boundary_markdown_path = (
        docs_root
        / "growth_tilt_remaining_candidate_pit_replay_blocker_closure_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438I_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "remaining_candidate_blocker_closure_records_json": str(closure_records_path),
        "remaining_candidate_blocker_before_after_matrix_json": str(before_after_path),
        "replay_recheck_readiness_handoff_json": str(handoff_path),
        "unresolved_remaining_candidate_blocker_summary_json": str(unresolved_path),
        "no_effect_boundary_json": str(boundary_path),
        "markdown_path": str(markdown_path),
        "remaining_candidate_blocker_closure_records_markdown": str(
            closure_records_markdown_path
        ),
        "remaining_candidate_blocker_before_after_matrix_markdown": str(
            before_after_markdown_path
        ),
        "replay_recheck_readiness_handoff_markdown": str(handoff_markdown_path),
        "unresolved_remaining_candidate_blocker_summary_markdown": str(
            unresolved_markdown_path
        ),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(
        closure_records_path,
        "remaining_candidate_blocker_closure_records",
        payload,
    )
    _write_section(
        before_after_path,
        "remaining_candidate_blocker_before_after_matrix",
        payload,
    )
    _write_section(handoff_path, "replay_recheck_readiness_handoff", payload)
    _write_section(
        unresolved_path,
        "unresolved_remaining_candidate_blocker_summary",
        payload,
    )
    _write_section(boundary_path, "no_effect_boundary", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        closure_records_markdown_path,
        _render_section_markdown(
            "Growth Tilt Remaining Candidate Replay Blocker Closure Records",
            payload.get("remaining_candidate_blocker_closure_records"),
        ),
    )
    write_markdown_artifact(
        before_after_markdown_path,
        _render_section_markdown(
            "Growth Tilt Remaining Candidate Replay Blocker Before After",
            payload.get("remaining_candidate_blocker_before_after_matrix"),
        ),
    )
    write_markdown_artifact(
        handoff_markdown_path,
        _render_section_markdown(
            "Growth Tilt Replay Recheck Readiness Handoff",
            payload.get("replay_recheck_readiness_handoff"),
        ),
    )
    write_markdown_artifact(
        unresolved_markdown_path,
        _render_section_markdown(
            "Growth Tilt Unresolved Remaining Candidate Replay Blockers",
            payload.get("unresolved_remaining_candidate_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Remaining Candidate PIT Replay Blocker Closure No-Effect Boundary",
            payload.get("no_effect_boundary"),
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
        "prior_candidate_replay_pass_count": payload.get(
            "prior_candidate_replay_pass_count"
        ),
        "prior_candidate_replay_fail_count": payload.get(
            "prior_candidate_replay_fail_count"
        ),
        "prior_candidate_replay_blocked_count": payload.get(
            "prior_candidate_replay_blocked_count"
        ),
        "remaining_candidate_blocker_count_before": payload.get(
            "remaining_candidate_blocker_count_before"
        ),
        "remaining_candidate_blocker_count_after": payload.get(
            "remaining_candidate_blocker_count_after"
        ),
        "candidate_recheckable_after_closure_count": payload.get(
            "candidate_recheckable_after_closure_count"
        ),
        "replay_recheck_handoff_ready": payload.get("replay_recheck_handoff_ready"),
        "forward_aging_handoff_ready": payload.get("forward_aging_handoff_ready"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Remaining Candidate PIT Replay Blocker Closure",
            "",
            f"- task_id: `{TASK_ID}`",
            f"- status: `{payload.get('status')}`",
            f"- data quality status: `{payload.get('data_quality_status')}`",
            f"- remaining blockers before / after: "
            f"`{payload.get('remaining_candidate_blocker_count_before')}` / "
            f"`{payload.get('remaining_candidate_blocker_count_after')}`",
            f"- replay recheck handoff ready: "
            f"`{payload.get('replay_recheck_handoff_ready')}`",
            f"- pass / fail / blocked remains: "
            f"`{payload.get('candidate_replay_pass_count')}` / "
            f"`{payload.get('candidate_replay_fail_count')}` / "
            f"`{payload.get('candidate_replay_blocked_count')}`",
            f"- next route: `{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438H 只关闭 remaining candidate PIT replay blockers 并生成 "
            "2438I recheck handoff。`READY` 不表示 candidate replay PASS / FAIL，"
            "也不表示 forward-aging 或 paper-shadow ready；closure records 的 "
            "`replay_outcome_after_closure` 固定为 `NOT_RECHECKED`。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Closure Records",
            "",
            "```json",
            _json_block(payload.get("remaining_candidate_blocker_closure_records", {})),
            "```",
            "",
            "## Replay Recheck Handoff",
            "",
            "```json",
            _json_block(payload.get("replay_recheck_readiness_handoff", {})),
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
    status = payload.get("status")
    next_route = payload.get("recommended_next_research_task")
    if status == READY_STATUS:
        body = (
            "三个 remaining candidate replay blockers 均已 closure-ready。下一步"
            "进入 2438I 独立 recheck；2438H 本身没有生成 PASS / FAIL、"
            "forward-aging candidate 或 paper-shadow candidate。"
        )
    else:
        body = (
            "仍有 remaining candidate replay blocker closure requirement 未满足。"
            "下一步继续 2438I continuation route，不能进入 forward-aging、"
            "paper-shadow、production 或 broker。"
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438I Route",
            "",
            "- source task: `TRADING-2438H`",
            f"- source status: `{status}`",
            f"- next route: `{next_route}`",
            "",
            body,
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
