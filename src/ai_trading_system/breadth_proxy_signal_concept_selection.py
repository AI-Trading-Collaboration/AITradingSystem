from __future__ import annotations

import json
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
    write_csv_rows,
    write_json,
    write_markdown,
)

DEFAULT_DIAGNOSTICS_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "current_constituents_breadth_proxy_diagnostics"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "breadth_proxy_signal_concept_selection"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2304_BREADTH_PROXY_SIGNAL_CONCEPT_SELECTION"
REPORT_TYPE = "breadth_proxy_signal_concept_selection"
MODE = "breadth_proxy_signal_concept_selection"
STATUS = "BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_SELECTION"
ARTIFACT_ROLE = "breadth_proxy_signal_concept_selection"

REQUIRED_SOURCE_STATUS = "CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED"
REQUIRED_SOURCE_TASK_ID = "TRADING-2303_CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_ONLY"

SELECTION_CRITERIA = (
    "distribution_discrimination_status",
    "neutrality_status",
    "asset_concentration_status",
    "trend_fragility_explanation_status",
    "price_trend_overlap_status",
    "bias_acceptability_status",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "selection_status": "source_blocked_no_selection",
    "selected_for_poc": False,
    "advance_to_generator_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "generator_implemented": False,
    "candidate_generation_allowed": False,
    "actual_path_validation_executed": False,
    "candidate_artifact_generated": False,
    "candidate_signal_series_generated": False,
    "prediction_artifact_generated": False,
    "forward_observe_runtime_started": False,
    "runtime_started": False,
}


class BreadthProxySignalConceptSelectionError(ValueError):
    pass


def run_breadth_proxy_signal_concept_selection(
    *,
    diagnostics_dir: Path = DEFAULT_DIAGNOSTICS_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise BreadthProxySignalConceptSelectionError(
            f"breadth proxy signal concept selection only supports {MODE}"
        )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_breadth_proxy_signal_concept_selection_artifacts(
        diagnostics_dir=diagnostics_dir,
        generated_at=generated_at,
    )
    artifact_paths = write_breadth_proxy_signal_concept_selection_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def build_breadth_proxy_signal_concept_selection_artifacts(
    *,
    diagnostics_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    source = load_trading_2303_diagnostics_artifacts(diagnostics_dir)
    signal_rows = _records(source["signal_distribution"].get("rows"))
    scorecard_rows = build_signal_concept_scorecard(
        signal_rows=signal_rows,
        source_summary=source["summary"],
        source_next_step=source["next_step"],
        source_bias_warning=source["bias_warning"],
    )
    selected = build_selected_breadth_signal_concepts(scorecard_rows=scorecard_rows)
    rejected = build_rejected_breadth_signal_concepts(scorecard_rows=scorecard_rows)
    safety_boundary = build_signal_selection_safety_boundary(source=source)
    common = _common_payload(
        diagnostics_dir=diagnostics_dir,
        generated_at=generated_at,
        source_summary=source["summary"],
    )
    summary = build_selection_summary(
        common=common,
        scorecard_rows=scorecard_rows,
        selected=selected,
        rejected=rejected,
        source_summary=source["summary"],
    )
    docs = build_signal_selection_docs(
        summary=summary,
        scorecard_rows=scorecard_rows,
        selected=selected,
        rejected=rejected,
        safety_boundary=safety_boundary,
    )
    return {
        "summary": summary,
        "scorecard": {**common, "rows": scorecard_rows},
        "selected_concepts": {**common, **selected},
        "rejected_concepts": {**common, **rejected},
        "safety_boundary": {**common, **safety_boundary},
        "docs": docs,
    }


def load_trading_2303_diagnostics_artifacts(diagnostics_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": diagnostics_dir / "breadth_proxy_diagnostics_summary.json",
        "signal_distribution": diagnostics_dir / "breadth_proxy_signal_distribution_matrix.json",
        "asset_horizon": diagnostics_dir / "breadth_proxy_asset_horizon_drilldown.json",
        "bias_warning": diagnostics_dir / "breadth_proxy_bias_warning_report.json",
        "next_step": diagnostics_dir / "breadth_proxy_next_step_recommendation.json",
        "safety_boundary": diagnostics_dir / "breadth_proxy_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2304 requires TRADING-2303 diagnostics outputs: "
            + ", ".join(missing)
        )
    source = {key: _read_json(path) for key, path in paths.items()}
    _validate_trading_2303_source(source)
    return source


def build_signal_concept_scorecard(
    *,
    signal_rows: Sequence[Mapping[str, Any]],
    source_summary: Mapping[str, Any],
    source_next_step: Mapping[str, Any],
    source_bias_warning: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not signal_rows:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2303 signal distribution matrix has no signal rows"
        )
    rows: list[dict[str, Any]] = []
    source_snapshot_status = _nested_summary_value(
        source_summary,
        "source_snapshot_status",
        default="unknown",
    )
    for row in signal_rows:
        signal_name = str(row.get("signal_name", "")).strip()
        if not signal_name:
            raise BreadthProxySignalConceptSelectionError(
                "TRADING-2303 signal distribution row missing signal_name"
            )
        scorecard = {
            "signal_name": signal_name,
            "source_distribution_status": row.get("distribution_status"),
            "source_diagnostics_grade": row.get("diagnostics_grade"),
            "source_snapshot_status": source_snapshot_status,
            "selection_decision": "REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION",
            "selection_ready": False,
            "selected_for_poc": False,
            "eligible_for_trading_2305": False,
            "source_blocker": "CURRENT_CONSTITUENTS_SNAPSHOT_MISSING",
            "reason_not_selected": (
                "TRADING-2303 did not compute signal distribution because frozen "
                "current constituents snapshots are missing."
            ),
            "source_next_action": source_next_step.get("recommendation_status"),
            "bias_warning_status": source_bias_warning.get("warning_status"),
            "reconsideration_condition": (
                "Rerun TRADING-2303 after frozen current constituent snapshots and "
                "constituent price coverage audit exist, then rerun TRADING-2304."
            ),
            **SAFETY_FIELDS,
        }
        scorecard.update(
            {criterion: "NOT_EVALUATED_SOURCE_BLOCKED" for criterion in SELECTION_CRITERIA}
        )
        rows.append(scorecard)
    return rows


def build_selected_breadth_signal_concepts(
    *,
    scorecard_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected_rows = [dict(row) for row in scorecard_rows if row.get("selected_for_poc") is True]
    return {
        "selection_status": "source_blocked_no_selection",
        "selected_concept_count": len(selected_rows),
        "rows": selected_rows,
        "advance_to_generator_allowed": False,
        "recommended_next_task": "TRADING-2303_CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_ONLY",
        "recommended_action": "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT_BEFORE_SELECTION",
        "selection_blocker": "CURRENT_CONSTITUENTS_SNAPSHOT_MISSING",
        **SAFETY_FIELDS,
    }


def build_rejected_breadth_signal_concepts(
    *,
    scorecard_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rejected_rows = [
        {
            "signal_name": row["signal_name"],
            "rejection_type": row["selection_decision"],
            "source_blocker": row["source_blocker"],
            "reason_not_selected": row["reason_not_selected"],
            "reconsideration_condition": row["reconsideration_condition"],
            "not_a_signal_quality_rejection": True,
            **SAFETY_FIELDS,
        }
        for row in scorecard_rows
        if row.get("selected_for_poc") is not True
    ]
    return {
        "selection_status": "source_blocked_no_selection",
        "rejected_concept_count": len(rejected_rows),
        "rows": rejected_rows,
        "advance_to_generator_allowed": False,
        "rejection_scope_note": (
            "Concepts are rejected from TRADING-2305 now because source diagnostics "
            "are missing, not because the concepts failed a measured distribution test."
        ),
        **SAFETY_FIELDS,
    }


def build_signal_selection_safety_boundary(
    *,
    source: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "boundary_status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED",
        "source_status": source["summary"].get("status"),
        "diagnostics_only": True,
        "source_blocked_default": True,
        "does_not_select_concepts_without_distribution": True,
        "does_not_advance_to_generator": True,
        "does_not_generate_signal_series": True,
        "does_not_generate_candidate_bound_artifacts": True,
        "does_not_run_actual_path_validation": True,
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "This source-blocked selection command does not consume cached market "
            "or macro data. Future constituent-price-dependent selection evidence "
            "must run aits validate-data or the same validation code path first."
        ),
        **SAFETY_FIELDS,
    }


def build_selection_summary(
    *,
    common: Mapping[str, Any],
    scorecard_rows: Sequence[Mapping[str, Any]],
    selected: Mapping[str, Any],
    rejected: Mapping[str, Any],
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(common),
        "summary": {
            "source_status": source_summary.get("status"),
            "source_snapshot_status": _nested_summary_value(
                source_summary,
                "source_snapshot_status",
                default="unknown",
            ),
            "signal_concept_count": len(scorecard_rows),
            "selected_concept_count": selected["selected_concept_count"],
            "rejected_concept_count": rejected["rejected_concept_count"],
            "advance_to_generator_allowed": False,
            "recommended_action": "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT_BEFORE_SELECTION",
            "data_quality_status": DATA_QUALITY_STATUS,
        },
        "source_task_id": source_summary.get("task_id"),
        "source_status": source_summary.get("status"),
        "source_recommended_next_action": source_summary.get("recommended_next_action"),
        "selected_concept_count": selected["selected_concept_count"],
        "rejected_concept_count": rejected["rejected_concept_count"],
        "advance_to_generator_allowed": False,
        "recommended_next_action": "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT_BEFORE_SELECTION",
        **SAFETY_FIELDS,
    }


def write_breadth_proxy_signal_concept_selection_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "breadth_proxy_signal_selection_summary.json",
        "scorecard_json": output_dir / "breadth_signal_concept_scorecard.json",
        "scorecard_csv": output_dir / "breadth_signal_concept_scorecard.csv",
        "selected_concepts": output_dir / "selected_breadth_signal_concepts.json",
        "rejected_concepts": output_dir / "rejected_breadth_signal_concepts.json",
        "safety_boundary": output_dir / "breadth_proxy_signal_selection_safety_boundary.json",
        "selection_report_doc": docs_root / "breadth_proxy_signal_selection_report.md",
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["scorecard_json"], artifacts["scorecard"])
    write_csv_rows(paths["scorecard_csv"], artifacts["scorecard"]["rows"])
    write_json(paths["selected_concepts"], artifacts["selected_concepts"])
    write_json(paths["rejected_concepts"], artifacts["rejected_concepts"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])
    write_markdown(paths["selection_report_doc"], artifacts["docs"]["selection_report"])
    return {key: str(path) for key, path in paths.items()}


def build_signal_selection_docs(
    *,
    summary: Mapping[str, Any],
    scorecard_rows: Sequence[Mapping[str, Any]],
    selected: Mapping[str, Any],
    rejected: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    report = "\n".join(
        [
            "# Breadth Proxy Signal Concept Selection",
            "",
            "TRADING-2304 只做 diagnostics-only signal concept selection。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- selected_concept_count: `{summary['selected_concept_count']}`",
            f"- rejected_concept_count: `{summary['rejected_concept_count']}`",
            (
                "- advance_to_generator_allowed: "
                f"`{summary['advance_to_generator_allowed']}`"
            ),
            f"- recommended_next_action: `{summary['recommended_next_action']}`",
            "",
            "## Selection Result",
            "",
            "当前没有 signal concept 被选入 TRADING-2305。原因不是 signal quality "
            "失败，而是 TRADING-2303 source-blocked，无法计算分布、neutrality、"
            "asset concentration、trend fragility 或 bias acceptance evidence。",
            "",
            "## Scorecard",
            "",
            "|signal_name|selection_decision|distribution|neutrality|bias|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['signal_name']}`|`{row['selection_decision']}`|"
                    f"`{row['distribution_discrimination_status']}`|"
                    f"`{row['neutrality_status']}`|"
                    f"`{row['bias_acceptability_status']}`|"
                )
                for row in scorecard_rows
            ],
            "",
            "## Selected Concepts",
            "",
            f"- selected_concept_count: `{selected['selected_concept_count']}`",
            "",
            "## Rejected Concepts",
            "",
            f"- rejected_concept_count: `{rejected['rejected_concept_count']}`",
            f"- rejection_scope_note: {rejected['rejection_scope_note']}",
            "",
            "## Safety",
            "",
            _safety_sentence(safety_boundary),
            "",
            "本报告不得用于 candidate generation、actual-path validation、promotion、"
            "paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return {"selection_report": report}


def _validate_trading_2303_source(source: Mapping[str, Mapping[str, Any]]) -> None:
    summary = source["summary"]
    if summary.get("status") != REQUIRED_SOURCE_STATUS:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2304 currently requires source-blocked TRADING-2303 diagnostics, "
            f"got {summary.get('status')}"
        )
    if summary.get("task_id") != REQUIRED_SOURCE_TASK_ID:
        raise BreadthProxySignalConceptSelectionError(
            f"TRADING-2303 task_id must be {REQUIRED_SOURCE_TASK_ID}"
        )
    if summary.get("signal_distribution_computed") is not False:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2304 source-blocked selection requires signal_distribution_computed=false"
        )
    if summary.get("candidate_generation_allowed_now") is not False:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2303 must not allow candidate generation"
        )
    next_step = source["next_step"]
    if next_step.get("recommended_next_task") != TASK_ID:
        raise BreadthProxySignalConceptSelectionError(
            f"TRADING-2303 next step must point to {TASK_ID}"
        )
    if next_step.get("do_not_advance_to_generator") is not True:
        raise BreadthProxySignalConceptSelectionError(
            "TRADING-2303 must block advancement to generator"
        )
    for artifact_name, payload in source.items():
        _validate_safety_fields(payload, artifact_name=artifact_name)


def _validate_safety_fields(
    payload: Mapping[str, Any],
    *,
    artifact_name: str,
) -> None:
    for field, expected in (
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("actual_path_validation_executed", False),
        ("candidate_artifact_generated", False),
        ("candidate_signal_series_generated", False),
    ):
        if payload.get(field, expected) != expected:
            raise BreadthProxySignalConceptSelectionError(
                f"TRADING-2303 {artifact_name} unsafe field {field}={payload.get(field)}"
            )


def _common_payload(
    *,
    diagnostics_dir: Path,
    generated_at: datetime,
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "Breadth Proxy Signal Concept Selection",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "source_blocked_signal_concept_selection",
        "source_diagnostics_dir": str(diagnostics_dir),
        "source_task_id": source_summary.get("task_id"),
        "source_status": source_summary.get("status"),
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "No cached market/macro data is consumed while TRADING-2303 signal "
            "distributions are source-blocked. Future data-dependent concept "
            "selection must run aits validate-data or the same validation code path."
        ),
        **SAFETY_FIELDS,
    }


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise BreadthProxySignalConceptSelectionError(f"JSON artifact must be object: {path}")
    return payload


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _nested_summary_value(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: str,
) -> str:
    summary = payload.get("summary")
    if isinstance(summary, Mapping) and summary.get(key) is not None:
        return str(summary[key])
    if payload.get(key) is not None:
        return str(payload[key])
    return default


def _safety_sentence(payload: Mapping[str, Any]) -> str:
    return (
        f"selection_status=`{payload['selection_status']}`, "
        f"advance_to_generator_allowed=`{payload['advance_to_generator_allowed']}`, "
        f"promotion_allowed=`{payload['promotion_allowed']}`, "
        f"paper_shadow_allowed=`{payload['paper_shadow_allowed']}`, "
        f"production_allowed=`{payload['production_allowed']}`, "
        f"broker_action=`{payload['broker_action']}`, "
        f"candidate_artifact_generated=`{payload['candidate_artifact_generated']}`, "
        f"actual_path_validation_executed=`{payload['actual_path_validation_executed']}`."
    )


__all__ = [
    "DEFAULT_DIAGNOSTICS_ROOT",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "BreadthProxySignalConceptSelectionError",
    "build_breadth_proxy_signal_concept_selection_artifacts",
    "build_rejected_breadth_signal_concepts",
    "build_selected_breadth_signal_concepts",
    "build_signal_concept_scorecard",
    "build_signal_selection_safety_boundary",
    "load_trading_2303_diagnostics_artifacts",
    "run_breadth_proxy_signal_concept_selection",
]
