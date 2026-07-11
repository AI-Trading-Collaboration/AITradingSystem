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
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck as m2438c,
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
    growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure as recheck,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438E"
TASK_REGISTER_ID = (
    "TRADING-2438E_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE"
)
REPORT_TYPE = recheck.REPORT_TYPE
SCHEMA_VERSION = recheck.SCHEMA_VERSION
READY_STATUS = recheck.READY_STATUS
BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS = (
    recheck.BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS
)
NO_PASSING_CANDIDATE_STATUS = recheck.NO_PASSING_CANDIDATE_STATUS
BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS = (
    recheck.BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS
)

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "candidate_replay_output_records.json"
)
DEFAULT_SOURCE_2438C_RECHECK_PATH = (
    m2438c.DEFAULT_OUTPUT_ROOT / "pit_replay_recheck_result.json"
)
DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH = (
    m2438b.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
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
DEFAULT_SOURCE_2438C_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_top3_candidate_pit_replay_recheck.md"
)
DEFAULT_SOURCE_2438B_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_pit_replay_engine_blocker_closure.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure(
    *,
    source_2438d_output_closure_path: Path = DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH,
    candidate_replay_output_records_path: Path = (
        DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH
    ),
    source_2438c_recheck_path: Path = DEFAULT_SOURCE_2438C_RECHECK_PATH,
    source_2438b_blocker_closure_path: Path = DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH,
    source_2438d_doc_path: Path = DEFAULT_SOURCE_2438D_DOC_PATH,
    candidate_output_records_doc_path: Path = DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    source_2438c_doc_path: Path = DEFAULT_SOURCE_2438C_DOC_PATH,
    source_2438b_doc_path: Path = DEFAULT_SOURCE_2438B_DOC_PATH,
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
        "source_2438d_output_closure": _load_json_document(
            source_2438d_output_closure_path
        ),
        "candidate_replay_output_records": _load_json_document(
            candidate_replay_output_records_path
        ),
        "source_2438c_recheck": _load_json_document(source_2438c_recheck_path),
        "source_2438b_blocker_closure": _load_json_document(
            source_2438b_blocker_closure_path
        ),
        "source_2438d_doc": _load_text_document(source_2438d_doc_path),
        "candidate_output_records_doc": _load_text_document(
            candidate_output_records_doc_path
        ),
        "source_2438c_doc": _load_text_document(source_2438c_doc_path),
        "source_2438b_doc": _load_text_document(source_2438b_doc_path),
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
        recheck.build_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure(
            _as_mapping(sources["source_2438d_output_closure"]),
            _as_mapping(sources["candidate_replay_output_records"]),
            _as_mapping(sources["source_2438c_recheck"]),
            _as_mapping(sources["source_2438b_blocker_closure"]),
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
    json_path = output_root / "recheck_after_output_closure_result.json"
    blocker_path = output_root / "candidate_level_blocker_summary.json"
    matrix_path = output_root / "pass_fail_blocked_decision_matrix.json"
    handoff_path = output_root / "forward_aging_handoff_gate.json"
    boundary_path = output_root / "no_effect_boundary.json"
    markdown_path = (
        docs_root / "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure.md"
    )
    blocker_markdown_path = (
        docs_root / "growth_tilt_candidate_level_replay_blocker_summary.md"
    )
    matrix_markdown_path = (
        docs_root / "growth_tilt_candidate_pass_fail_blocked_decision_matrix.md"
    )
    handoff_markdown_path = (
        docs_root / "growth_tilt_candidate_forward_aging_handoff_gate.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_candidate_recheck_after_output_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438F_or_2439A_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_level_blocker_summary_json": str(blocker_path),
        "pass_fail_blocked_decision_matrix_json": str(matrix_path),
        "forward_aging_handoff_gate_json": str(handoff_path),
        "no_effect_boundary_json": str(boundary_path),
        "markdown_path": str(markdown_path),
        "candidate_level_blocker_summary_markdown": str(blocker_markdown_path),
        "pass_fail_blocked_decision_matrix_markdown": str(matrix_markdown_path),
        "forward_aging_handoff_gate_markdown": str(handoff_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(blocker_path, "candidate_level_blocker_summary", payload)
    _write_section(matrix_path, "pass_fail_blocked_decision_matrix", payload)
    _write_section(handoff_path, "forward_aging_handoff_gate", payload)
    _write_section(boundary_path, "no_effect_boundary", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        blocker_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate-Level Replay Blocker Summary",
            payload.get("candidate_level_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Pass Fail Blocked Decision Matrix",
            payload.get("pass_fail_blocked_decision_matrix"),
        ),
    )
    write_markdown_artifact(
        handoff_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Forward-Aging Handoff Gate",
            payload.get("forward_aging_handoff_gate"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Recheck After Output No-Effect Boundary",
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
        "candidate_replay_outputs_complete": payload.get(
            "candidate_replay_outputs_complete"
        ),
        "candidate_replay_output_record_count": payload.get(
            "candidate_replay_output_record_count"
        ),
        "pit_replay_recheck_after_output_closure_ready": payload.get(
            "pit_replay_recheck_after_output_closure_ready"
        ),
        "candidate_replay_pass_count": payload.get("candidate_replay_pass_count"),
        "candidate_replay_fail_count": payload.get("candidate_replay_fail_count"),
        "candidate_replay_blocked_count": payload.get(
            "candidate_replay_blocked_count"
        ),
        "candidate_level_blocker_count": payload.get("candidate_level_blocker_count"),
        "forward_aging_handoff_ready": payload.get("forward_aging_handoff_ready"),
        "forward_aging_candidate_count": payload.get("forward_aging_candidate_count"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Top-3 Candidate PIT Replay Recheck After Output Closure",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- output records complete：`{payload.get('candidate_replay_outputs_complete')}`",
            f"- pass / fail / blocked：`{payload.get('candidate_replay_pass_count')}` / "
            f"`{payload.get('candidate_replay_fail_count')}` / "
            f"`{payload.get('candidate_replay_blocked_count')}`",
            f"- candidate-level blocker count：`{payload.get('candidate_level_blocker_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438E 在 2438D output completeness READY 后复核 candidate "
            "level replay status。当前若 candidate 仍为 BLOCKED，必须 route 到 "
            "2438F blocker closure；不得把 BLOCKED 误标为 FAIL 或 no-candidate，"
            "不得启用 paper-shadow / production / broker。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate-Level Blockers",
            "",
            "```json",
            _json_block(payload.get("candidate_level_blocker_summary", {})),
            "```",
            "",
            "## Decision Matrix",
            "",
            "```json",
            _json_block(payload.get("pass_fail_blocked_decision_matrix", {})),
            "```",
            "",
            "## Forward-Aging Handoff Gate",
            "",
            "```json",
            _json_block(payload.get("forward_aging_handoff_gate", {})),
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
            "至少 1 个 candidate 为 PASS，且没有 candidate 保持 BLOCKED。下一步"
            "进入 2439A forward-aging candidate pack rebuild；paper-shadow 仍未启用。"
        )
    elif status == NO_PASSING_CANDIDATE_STATUS:
        body = (
            "所有 candidate output 均可判定且无 BLOCKED，但没有 PASS candidate。下一步"
            "进入 no-passing-candidate evidence review；这不是 2440 promotion review 的"
            " no-candidate。"
        )
    elif status == BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS:
        body = (
            "candidate output 已完整，但至少 1 个 candidate 仍存在 candidate-level "
            "PIT replay blocker。下一步必须先关闭这些 blocker。"
        )
    else:
        body = (
            "candidate output records 仍不足以执行 after-output-closure recheck。"
            "需要先回补 output completeness evidence。"
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438F Or 2439A Route",
            "",
            "- source task：`TRADING-2438E`",
            f"- source status：`{status}`",
            f"- next route：`{next_route}`",
            "",
            body,
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
