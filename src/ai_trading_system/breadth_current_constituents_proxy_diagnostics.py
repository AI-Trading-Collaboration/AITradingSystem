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

DEFAULT_FEASIBILITY_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "breadth_participation_candidate_family_feasibility_audit"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "current_constituents_breadth_proxy_diagnostics"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2303_CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_ONLY"
REPORT_TYPE = "current_constituents_breadth_proxy_diagnostics"
MODE = "current_constituents_proxy_diagnostics"
STATUS = "CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_DIAGNOSTICS"
ARTIFACT_ROLE = "current_constituents_breadth_proxy_diagnostics"

REQUIRED_FEASIBILITY_STATUS = "BREADTH_FEASIBILITY_AUDIT_READY_PROXY_ONLY"
REQUIRED_NEXT_ACTION = "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only"
DEFAULT_TARGET_ETFS = ("QQQ", "SPY", "SMH")
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")
DEFAULT_SIGNAL_CONCEPTS = (
    "breadth_participation_score",
    "advance_decline_participation_score",
    "constituent_momentum_breadth_score",
    "new_high_new_low_proxy_score",
    "mega_cap_concentration_risk_score",
    "sector_leadership_diffusion_score",
    "trend_fragility_score",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "pit_status": "current_constituents_proxy_only",
    "strict_pit_ready": False,
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
    "forward_observe_started": False,
    "forward_observe_runtime_started": False,
    "runtime_started": False,
}


class CurrentConstituentsProxyDiagnosticsError(ValueError):
    pass


def run_current_constituents_breadth_proxy_diagnostics(
    *,
    feasibility_dir: Path = DEFAULT_FEASIBILITY_ROOT,
    current_constituents_dir: Path | None = None,
    target_etfs: str | Sequence[str] = DEFAULT_TARGET_ETFS,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    horizons: str | Sequence[str] = DEFAULT_HORIZONS,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    request = _validated_request(
        target_etfs=target_etfs,
        target_assets=target_assets,
        horizons=horizons,
        mode=mode,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_current_constituents_breadth_proxy_diagnostics_artifacts(
        feasibility_dir=feasibility_dir,
        current_constituents_dir=current_constituents_dir,
        target_etfs=request["target_etfs"],
        target_assets=request["target_assets"],
        horizons=request["horizons"],
        generated_at=generated_at,
    )
    artifact_paths = write_current_constituents_breadth_proxy_diagnostics_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def build_current_constituents_breadth_proxy_diagnostics_artifacts(
    *,
    feasibility_dir: Path,
    current_constituents_dir: Path | None,
    target_etfs: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
) -> dict[str, Any]:
    feasibility = load_trading_2302_feasibility_artifacts(feasibility_dir)
    snapshot_rows = build_snapshot_source_coverage_matrix(
        target_etfs=target_etfs,
        current_constituents_dir=current_constituents_dir,
    )
    concepts = _signal_concepts_from_feasibility(feasibility["signal_concepts"])
    source_blocked = any(not row["snapshot_found"] for row in snapshot_rows)
    common = _common_payload(
        target_etfs=target_etfs,
        target_assets=target_assets,
        horizons=horizons,
        generated_at=generated_at,
        feasibility_dir=feasibility_dir,
        current_constituents_dir=current_constituents_dir,
        source_blocked=source_blocked,
    )
    signal_rows = build_signal_distribution_matrix(signal_concepts=concepts)
    drilldown_rows = build_asset_horizon_drilldown(
        target_assets=target_assets,
        target_etfs=target_etfs,
        horizons=horizons,
        snapshot_rows=snapshot_rows,
    )
    bias_warning = build_bias_warning_report(
        target_etfs=target_etfs,
        feasibility=feasibility,
        snapshot_rows=snapshot_rows,
    )
    next_step = build_next_step_recommendation(
        source_blocked=source_blocked,
        snapshot_rows=snapshot_rows,
    )
    safety_boundary = build_proxy_diagnostics_safety_boundary()
    summary = build_diagnostics_summary(
        common=common,
        signal_rows=signal_rows,
        drilldown_rows=drilldown_rows,
        snapshot_rows=snapshot_rows,
        next_step=next_step,
        source_summary=feasibility["summary"],
    )
    docs = build_proxy_diagnostics_docs(
        summary=summary,
        signal_rows=signal_rows,
        drilldown_rows=drilldown_rows,
        snapshot_rows=snapshot_rows,
        bias_warning=bias_warning,
        next_step=next_step,
        safety_boundary=safety_boundary,
    )
    return {
        "summary": summary,
        "source_coverage_matrix": {**common, "rows": snapshot_rows},
        "signal_distribution_matrix": {**common, "rows": signal_rows},
        "asset_horizon_drilldown": {**common, "rows": drilldown_rows},
        "bias_warning_report": {**common, **bias_warning},
        "next_step_recommendation": {**common, **next_step},
        "safety_boundary": {**common, **safety_boundary},
        "docs": docs,
    }


def load_trading_2302_feasibility_artifacts(feasibility_dir: Path) -> dict[str, Any]:
    summary_path = feasibility_dir / "breadth_participation_feasibility_summary.json"
    concepts_path = feasibility_dir / "breadth_candidate_signal_concept_matrix.json"
    proxy_risk_path = feasibility_dir / "current_constituents_proxy_risk_matrix.json"
    task_route_path = feasibility_dir / "breadth_2303_task_route.json"
    missing = [
        str(path)
        for path in (summary_path, concepts_path, proxy_risk_path, task_route_path)
        if not path.exists()
    ]
    if missing:
        raise CurrentConstituentsProxyDiagnosticsError(
            "TRADING-2303 requires TRADING-2302 feasibility outputs: "
            + ", ".join(missing)
        )

    summary = _read_json(summary_path)
    concepts = _read_json(concepts_path)
    proxy_risk = _read_json(proxy_risk_path)
    task_route = _read_json(task_route_path)
    _validate_trading_2302_source(summary=summary, task_route=task_route)
    return {
        "summary": summary,
        "signal_concepts": concepts,
        "proxy_risk": proxy_risk,
        "task_route": task_route,
    }


def build_snapshot_source_coverage_matrix(
    *,
    target_etfs: Sequence[str],
    current_constituents_dir: Path | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for etf in target_etfs:
        snapshot_path = _find_snapshot_path(current_constituents_dir, etf)
        rows.append(
            {
                "target_etf": etf,
                "current_constituents_dir": str(current_constituents_dir or ""),
                "snapshot_found": snapshot_path is not None,
                "snapshot_path": str(snapshot_path or ""),
                "source_status": (
                    "SNAPSHOT_PRESENT_NOT_USED_FOR_SIGNAL_GENERATION"
                    if snapshot_path
                    else "MISSING_CURRENT_CONSTITUENTS_SNAPSHOT"
                ),
                "constituent_count": "not_computed_source_blocked",
                "weight_coverage": "not_computed_source_blocked",
                "constituent_price_coverage": "not_computed_source_blocked",
                "coverage_audit_status": "NOT_EXECUTED_SNAPSHOT_MISSING",
                "diagnostics_allowed": snapshot_path is not None,
                "candidate_generation_allowed": False,
                "actual_path_validation_allowed": False,
                "required_before_computation": (
                    "frozen_current_constituent_snapshot; source/provider record; "
                    "download timestamp; row count; checksum; constituent price coverage audit"
                ),
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_signal_distribution_matrix(
    *,
    signal_concepts: Sequence[str],
) -> list[dict[str, Any]]:
    return [
        {
            "signal_name": signal,
            "distribution_status": "NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING",
            "diagnostics_grade": "source_blocked",
            "sample_count": 0,
            "non_null_count": 0,
            "distinct_value_count": 0,
            "min": None,
            "median": None,
            "max": None,
            "neutral_share": None,
            "extreme_share": None,
            "reason_not_computed": (
                "No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH."
            ),
            "allowed_next_use": "source_due_diligence_only",
            **SAFETY_FIELDS,
        }
        for signal in signal_concepts
    ]


def build_asset_horizon_drilldown(
    *,
    target_assets: Sequence[str],
    target_etfs: Sequence[str],
    horizons: Sequence[str],
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_etf = {str(row["target_etf"]): row for row in snapshot_rows}
    rows: list[dict[str, Any]] = []
    for asset in target_assets:
        for etf in target_etfs:
            source = by_etf[etf]
            for horizon in horizons:
                rows.append(
                    {
                        "target_asset": asset,
                        "target_etf": etf,
                        "horizon": horizon,
                        "diagnostic_status": (
                            "BLOCKED_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING"
                            if not source["snapshot_found"]
                            else "BLOCKED_COMPUTATION_NOT_IMPLEMENTED_IN_SOURCE_AUDIT"
                        ),
                        "record_count": 0,
                        "source_coverage_status": source["source_status"],
                        "snapshot_found": source["snapshot_found"],
                        "signal_value_available": False,
                        "future_return_evaluation_available": False,
                        "data_quality_status": DATA_QUALITY_STATUS,
                        "allowed_validation": "none_source_blocked",
                        **SAFETY_FIELDS,
                    }
                )
    return rows


def build_bias_warning_report(
    *,
    target_etfs: Sequence[str],
    feasibility: Mapping[str, Any],
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "warning_status": "CURRENT_CONSTITUENTS_PROXY_HIGH_BIAS_SOURCE_BLOCKED",
        "target_etfs": list(target_etfs),
        "source_snapshot_status": _overall_snapshot_status(snapshot_rows),
        "inherited_trading_2302_status": feasibility["summary"].get("status"),
        "bias_warnings": [
            {
                "bias_type": "survivorship_bias",
                "severity": "high",
                "behavioral_impact": (
                    "Current winners and surviving constituents can overstate historical "
                    "participation quality."
                ),
                "risk": "invalid historical breadth conclusion if backfilled",
                "exit_condition": "historical constituent membership with known-at timestamps",
            },
            {
                "bias_type": "lookahead_bias",
                "severity": "high",
                "behavioral_impact": (
                    "A current membership snapshot can reveal future index membership "
                    "relative to older dates."
                ),
                "risk": "false confidence in actual-path validation",
                "exit_condition": "PIT constituent source or forward-only frozen snapshot log",
            },
            {
                "bias_type": "mega_cap_concentration",
                "severity": "moderate_high",
                "behavioral_impact": (
                    "Cap-weighted ETF trend can remain strong while equal member "
                    "participation weakens."
                ),
                "risk": "fragility warning may be overstated or understated without weights",
                "exit_condition": "audited current or historical weights source",
            },
        ],
        "acceptable_for_diagnostics_only": True,
        "acceptable_for_candidate_generation": False,
        "acceptable_for_actual_path_validation": False,
        "acceptable_for_promotion": False,
        **SAFETY_FIELDS,
    }


def build_next_step_recommendation(
    *,
    source_blocked: bool,
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    missing = [str(row["target_etf"]) for row in snapshot_rows if not row["snapshot_found"]]
    return {
        "recommendation_status": (
            "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT"
            if source_blocked
            else "RUN_COVERAGE_AUDIT_BEFORE_SIGNAL_COMPUTATION"
        ),
        "recommended_next_task": "TRADING-2304_BREADTH_PROXY_SIGNAL_CONCEPT_SELECTION",
        "recommended_action": (
            "Request frozen QQQ / SPY / SMH current constituents snapshots with provider, "
            "download timestamp, row count, checksum, and terms before computing diagnostics."
            if source_blocked
            else "Audit snapshot schema and constituent price coverage before concept selection."
        ),
        "missing_snapshot_etfs": missing,
        "owner_input_required": source_blocked,
        "data_source_investment_decision_required": source_blocked,
        "do_not_advance_to_generator": True,
        "do_not_advance_to_actual_path_validation": True,
        "exit_condition_for_source_blocker": (
            "Frozen current constituents snapshot plus constituent price coverage audit "
            "for every target ETF."
        ),
        **SAFETY_FIELDS,
    }


def build_proxy_diagnostics_safety_boundary() -> dict[str, Any]:
    return {
        "boundary_status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED",
        "diagnostics_only": True,
        "source_blocked_default": True,
        "does_not_read_market_cache": True,
        "does_not_download_external_data": True,
        "does_not_generate_signal_series": True,
        "does_not_generate_candidate_bound_artifacts": True,
        "does_not_run_actual_path_validation": True,
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "This source-blocked static diagnostics command does not consume cached market "
            "or macro data. Any future constituent-price-dependent computation must run "
            "aits validate-data or the same validation code path first and expose the result."
        ),
        **SAFETY_FIELDS,
    }


def build_diagnostics_summary(
    *,
    common: Mapping[str, Any],
    signal_rows: Sequence[Mapping[str, Any]],
    drilldown_rows: Sequence[Mapping[str, Any]],
    snapshot_rows: Sequence[Mapping[str, Any]],
    next_step: Mapping[str, Any],
    source_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(common),
        "summary": {
            "source_snapshot_status": _overall_snapshot_status(snapshot_rows),
            "signal_concept_count": len(signal_rows),
            "computable_signal_concept_count": 0,
            "asset_horizon_row_count": len(drilldown_rows),
            "missing_snapshot_count": sum(1 for row in snapshot_rows if not row["snapshot_found"]),
            "recommended_action": next_step["recommendation_status"],
            "data_quality_status": DATA_QUALITY_STATUS,
        },
        "inherited_trading_2302_status": source_summary.get("status"),
        "inherited_trading_2302_recommended_next_action": source_summary.get(
            "recommended_next_action"
        ),
        "current_constituents_snapshot_available": not any(
            not row["snapshot_found"] for row in snapshot_rows
        ),
        "signal_distribution_computed": False,
        "asset_horizon_drilldown_computed": False,
        "candidate_generation_allowed_now": False,
        "recommended_next_action": next_step["recommendation_status"],
        **SAFETY_FIELDS,
    }


def write_current_constituents_breadth_proxy_diagnostics_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "breadth_proxy_diagnostics_summary.json",
        "source_coverage_json": output_dir / "breadth_proxy_source_coverage_matrix.json",
        "source_coverage_csv": output_dir / "breadth_proxy_source_coverage_matrix.csv",
        "signal_distribution_json": output_dir
        / "breadth_proxy_signal_distribution_matrix.json",
        "signal_distribution_csv": output_dir
        / "breadth_proxy_signal_distribution_matrix.csv",
        "asset_horizon_json": output_dir / "breadth_proxy_asset_horizon_drilldown.json",
        "asset_horizon_csv": output_dir / "breadth_proxy_asset_horizon_drilldown.csv",
        "bias_warning_report": output_dir / "breadth_proxy_bias_warning_report.json",
        "next_step_recommendation": output_dir / "breadth_proxy_next_step_recommendation.json",
        "safety_boundary": output_dir / "breadth_proxy_safety_boundary.json",
        "diagnostics_report_doc": docs_root
        / "current_constituents_breadth_proxy_diagnostics_report.md",
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["source_coverage_json"], artifacts["source_coverage_matrix"])
    write_csv_rows(
        paths["source_coverage_csv"],
        artifacts["source_coverage_matrix"]["rows"],
    )
    write_json(paths["signal_distribution_json"], artifacts["signal_distribution_matrix"])
    write_csv_rows(
        paths["signal_distribution_csv"],
        artifacts["signal_distribution_matrix"]["rows"],
    )
    write_json(paths["asset_horizon_json"], artifacts["asset_horizon_drilldown"])
    write_csv_rows(paths["asset_horizon_csv"], artifacts["asset_horizon_drilldown"]["rows"])
    write_json(paths["bias_warning_report"], artifacts["bias_warning_report"])
    write_json(paths["next_step_recommendation"], artifacts["next_step_recommendation"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])
    write_markdown(paths["diagnostics_report_doc"], artifacts["docs"]["diagnostics_report"])
    return {key: str(path) for key, path in paths.items()}


def build_proxy_diagnostics_docs(
    *,
    summary: Mapping[str, Any],
    signal_rows: Sequence[Mapping[str, Any]],
    drilldown_rows: Sequence[Mapping[str, Any]],
    snapshot_rows: Sequence[Mapping[str, Any]],
    bias_warning: Mapping[str, Any],
    next_step: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    report = "\n".join(
        [
            "# Current Constituents Breadth Proxy Diagnostics",
            "",
            "TRADING-2303 只生成 current constituents proxy diagnostics-only 包。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_snapshot_status: `{summary['summary']['source_snapshot_status']}`",
            f"- recommended_next_action: `{summary['recommended_next_action']}`",
            "",
            "## Source Coverage",
            "",
            "|target_etf|snapshot_found|source_status|required_before_computation|",
            "|---|---:|---|---|",
            *[
                (
                    f"|`{row['target_etf']}`|{row['snapshot_found']}|"
                    f"`{row['source_status']}`|{row['required_before_computation']}|"
                )
                for row in snapshot_rows
            ],
            "",
            "## Signal Distribution",
            "",
            "|signal_name|distribution_status|diagnostics_grade|reason|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['signal_name']}`|`{row['distribution_status']}`|"
                    f"`{row['diagnostics_grade']}`|{row['reason_not_computed']}|"
                )
                for row in signal_rows
            ],
            "",
            "## Asset / Horizon Drilldown",
            "",
            f"- row_count: `{len(drilldown_rows)}`",
            "- all rows remain source-blocked until frozen current constituent snapshots exist.",
            "",
            "## Bias Warning",
            "",
            f"- warning_status: `{bias_warning['warning_status']}`",
            *[
                (
                    f"- `{item['bias_type']}`: severity=`{item['severity']}`, "
                    f"risk={item['risk']}"
                )
                for item in bias_warning["bias_warnings"]
            ],
            "",
            "## Next Step",
            "",
            f"- recommendation_status: `{next_step['recommendation_status']}`",
            f"- owner_input_required: `{next_step['owner_input_required']}`",
            (
                "- exit_condition_for_source_blocker: "
                f"{next_step['exit_condition_for_source_blocker']}"
            ),
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
    return {"diagnostics_report": report}


def _validated_request(
    *,
    target_etfs: str | Sequence[str],
    target_assets: str | Sequence[str],
    horizons: str | Sequence[str],
    mode: str,
) -> dict[str, Any]:
    if mode != MODE:
        raise CurrentConstituentsProxyDiagnosticsError(
            f"current constituents proxy diagnostics only supports {MODE}"
        )
    etfs = _parse_list(target_etfs)
    assets = _parse_list(target_assets)
    parsed_horizons = _parse_list(horizons, uppercase=False)
    if not etfs:
        raise CurrentConstituentsProxyDiagnosticsError("--target-etfs is required")
    if not assets:
        raise CurrentConstituentsProxyDiagnosticsError("--target-assets is required")
    if not parsed_horizons:
        raise CurrentConstituentsProxyDiagnosticsError("--horizons is required")
    return {"target_etfs": etfs, "target_assets": assets, "horizons": parsed_horizons}


def _validate_trading_2302_source(
    *,
    summary: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> None:
    if summary.get("status") != REQUIRED_FEASIBILITY_STATUS:
        raise CurrentConstituentsProxyDiagnosticsError(
            "TRADING-2302 status must be "
            f"{REQUIRED_FEASIBILITY_STATUS}, got {summary.get('status')}"
        )
    if summary.get("recommended_next_action") != REQUIRED_NEXT_ACTION:
        raise CurrentConstituentsProxyDiagnosticsError(
            "TRADING-2302 recommended_next_action must be "
            f"{REQUIRED_NEXT_ACTION}, got {summary.get('recommended_next_action')}"
        )
    if task_route.get("next_task") != REQUIRED_NEXT_ACTION:
        raise CurrentConstituentsProxyDiagnosticsError(
            "TRADING-2302 task route must point to "
            f"{REQUIRED_NEXT_ACTION}, got {task_route.get('next_task')}"
        )
    for field, expected in (
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("actual_path_validation_executed", False),
        ("candidate_artifact_generated", False),
    ):
        if summary.get(field, expected) != expected:
            raise CurrentConstituentsProxyDiagnosticsError(
                f"TRADING-2302 summary unsafe field {field}={summary.get(field)}"
            )


def _common_payload(
    *,
    target_etfs: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    feasibility_dir: Path,
    current_constituents_dir: Path | None,
    source_blocked: bool,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "Current Constituents Breadth Proxy Diagnostics",
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
        "actual_requested_date_range": "source_blocked_current_snapshot_diagnostics",
        "target_etfs": list(target_etfs),
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "source_blocked": source_blocked,
        "feasibility_dir": str(feasibility_dir),
        "current_constituents_dir": str(current_constituents_dir or ""),
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "No cached market/macro data is consumed while current constituents snapshots "
            "are missing. Future constituent-price computation must run aits validate-data "
            "or the same validation code path first."
        ),
        **SAFETY_FIELDS,
    }


def _signal_concepts_from_feasibility(payload: Mapping[str, Any]) -> list[str]:
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise CurrentConstituentsProxyDiagnosticsError(
            "TRADING-2302 signal concept matrix rows must be a list"
        )
    concepts = [
        str(row["signal_name"])
        for row in rows
        if isinstance(row, Mapping) and row.get("signal_name")
    ]
    if not concepts:
        return list(DEFAULT_SIGNAL_CONCEPTS)
    return concepts


def _find_snapshot_path(current_constituents_dir: Path | None, etf: str) -> Path | None:
    if current_constituents_dir is None or not current_constituents_dir.exists():
        return None
    candidates = (
        current_constituents_dir / f"{etf}_current_constituents.csv",
        current_constituents_dir / f"{etf}_current_constituents.json",
        current_constituents_dir / f"{etf}.csv",
        current_constituents_dir / f"{etf}.json",
        current_constituents_dir / f"{etf.lower()}_current_constituents.csv",
        current_constituents_dir / f"{etf.lower()}_current_constituents.json",
    )
    for path in candidates:
        if path.exists():
            return path
    return None


def _overall_snapshot_status(snapshot_rows: Sequence[Mapping[str, Any]]) -> str:
    missing = [row for row in snapshot_rows if not row["snapshot_found"]]
    if len(missing) == len(snapshot_rows):
        return "ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING"
    if missing:
        return "PARTIAL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING"
    return "ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_PRESENT_COVERAGE_NOT_AUDITED"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CurrentConstituentsProxyDiagnosticsError(f"JSON artifact must be object: {path}")
    return payload


def _parse_list(value: str | Sequence[str], *, uppercase: bool = True) -> list[str]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    cleaned = [part.strip() for part in parts if part.strip()]
    if uppercase:
        return [part.upper() for part in cleaned]
    return cleaned


def _safety_sentence(payload: Mapping[str, Any]) -> str:
    return (
        f"pit_status=`{payload['pit_status']}`, "
        f"strict_pit_ready=`{payload['strict_pit_ready']}`, "
        f"promotion_allowed=`{payload['promotion_allowed']}`, "
        f"paper_shadow_allowed=`{payload['paper_shadow_allowed']}`, "
        f"production_allowed=`{payload['production_allowed']}`, "
        f"broker_action=`{payload['broker_action']}`, "
        f"candidate_artifact_generated=`{payload['candidate_artifact_generated']}`, "
        f"actual_path_validation_executed=`{payload['actual_path_validation_executed']}`."
    )


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_FEASIBILITY_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "CurrentConstituentsProxyDiagnosticsError",
    "build_asset_horizon_drilldown",
    "build_current_constituents_breadth_proxy_diagnostics_artifacts",
    "build_proxy_diagnostics_safety_boundary",
    "build_signal_distribution_matrix",
    "build_snapshot_source_coverage_matrix",
    "load_trading_2302_feasibility_artifacts",
    "run_current_constituents_breadth_proxy_diagnostics",
]
