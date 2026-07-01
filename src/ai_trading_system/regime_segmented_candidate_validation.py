from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    data_quality_payload,
    mapping,
    max_price_date,
    rate,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2317_REGIME_SEGMENTED_CANDIDATE_VALIDATION"
REPORT_TYPE = "regime_segmented_candidate_validation"
ARTIFACT_ROLE = "regime_segmented_candidate_validation_diagnostic"
MODE = "diagnostic_validation"
STATUS = "REGIME_SEGMENTED_CANDIDATE_VALIDATION_READY_DIAGNOSTIC_ONLY"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "regime_segmented_candidate_validation_policy.yaml"
)
DEFAULT_LABEL_SERIES_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "regime_label_generator_diagnostic_poc"
    / "regime_label_series.csv"
)
DEFAULT_LABEL_SUMMARY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "regime_label_generator_diagnostic_poc"
    / "regime_label_generation_summary.json"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

PRIMARY_AXIS = "primary_trend_regime"
VOLATILITY_AXIS = "volatility_overlay"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "diagnostic_only": True,
    "segmentation_only": True,
    "actual_path_validation_consumed": True,
    "new_actual_path_validation_executed": False,
    "candidate_signal_generated": False,
    "candidate_artifact_generated": False,
    "direct_strategy_signal_allowed": False,
    "existing_candidate_verdict_changed": False,
    "forward_observe_started": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "portfolio_effect": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class RegimeSegmentedCandidateValidationError(ValueError):
    pass


def run_regime_segmented_candidate_validation(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    label_series_path: Path = DEFAULT_LABEL_SERIES_PATH,
    label_summary_path: Path = DEFAULT_LABEL_SUMMARY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise RegimeSegmentedCandidateValidationError(
            "regime segmented candidate validation only supports diagnostic_validation mode"
        )

    policy = _load_policy(policy_path)
    _validate_policy(policy)
    resolved_quality_as_of = _parse_optional_date(quality_as_of) or max_price_date(
        prices_path
    )
    quality_report, quality_report_path = _run_data_quality_gate(
        policy=policy,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise RegimeSegmentedCandidateValidationError(
            f"cached data quality gate failed: {quality_report.status}"
        )

    label_summary = _load_json(label_summary_path)
    _validate_label_summary(label_summary, policy)
    label_frame = _load_label_frame(label_series_path)

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    family_blockers: list[dict[str, Any]] = []
    performance_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []

    candidate_sources = mapping(policy.get("candidate_sources"))
    for family_name, source_value in candidate_sources.items():
        source = mapping(source_value)
        if source.get("source_type") == "source_blocked_diagnostics_selection":
            blocker = _build_source_blocked_family_row(
                family_name=str(family_name),
                source=source,
                policy=policy,
            )
            family_blockers.append(blocker)
            continue

        source_blocker, family_performance, family_coverage = _segment_actual_path_family(
            family_name=str(family_name),
            source=source,
            policy=policy,
            label_frame=label_frame,
        )
        family_blockers.append(source_blocker)
        performance_rows.extend(family_performance)
        coverage_rows.extend(family_coverage)

    interpretation_rows = build_interpretation_matrix(
        performance_rows=performance_rows,
        family_blocker_rows=family_blockers,
        policy=policy,
    )
    safety_boundary = build_safety_boundary(
        generated_at=generated_at,
        data_quality_status=quality_report.status,
        performance_row_count=len(performance_rows),
        blocker_row_count=len(family_blockers),
    )
    requested_range = _label_requested_range(label_summary)
    summary = _summary_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        label_series_path=label_series_path,
        label_summary_path=label_summary_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        label_summary=label_summary,
        requested_range=requested_range,
        performance_rows=performance_rows,
        coverage_rows=coverage_rows,
        family_blocker_rows=family_blockers,
        interpretation_rows=interpretation_rows,
        policy=policy,
    )
    common = _common_payload(
        generated_at=generated_at,
        data_quality_status=quality_report.status,
        requested_range=requested_range,
    )
    paths = write_regime_segmented_candidate_validation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        performance_rows=performance_rows,
        coverage_rows=coverage_rows,
        family_blocker_rows=family_blockers,
        interpretation_rows=interpretation_rows,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "performance_rows": performance_rows,
            "coverage_rows": coverage_rows,
            "family_blocker_rows": family_blockers,
            "interpretation_rows": interpretation_rows,
        }
    )


def build_interpretation_matrix(
    *,
    performance_rows: Sequence[Mapping[str, Any]],
    family_blocker_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family_names = [str(item.get("family_name", "")) for item in family_blocker_rows]
    for family_name in family_names:
        family_rows = [
            row for row in performance_rows if str(row.get("family_name")) == family_name
        ]
        blocker = next(
            (
                row
                for row in family_blocker_rows
                if str(row.get("family_name")) == family_name
            ),
            {},
        )
        if not family_rows:
            rows.append(
                clean_for_yaml(
                    {
                        "family_name": family_name,
                        "family_id": blocker.get("family_id", family_name),
                        "segmentable": False,
                        "interpretation_status": blocker.get(
                            "blocker_status", "SEGMENTATION_SOURCE_BLOCKED"
                        ),
                        "primary_interpretable_segment_count": 0,
                        "volatility_interpretable_segment_count": 0,
                        "best_primary_segment_by_avg_forward_return": "",
                        "worst_primary_segment_by_avg_max_drawdown": "",
                        "diagnostic_interpretation": blocker.get("blocker_reason", ""),
                        "scope_change_recommended": False,
                        "existing_candidate_verdict_changed": False,
                        **SAFETY_FIELDS,
                    }
                )
            )
            continue

        primary_rows = [
            row
            for row in family_rows
            if row.get("label_axis") == PRIMARY_AXIS
            and row.get("interpretation_status")
            == _interpretation_label(policy, "interpretable")
        ]
        volatility_rows = [
            row
            for row in family_rows
            if row.get("label_axis") == VOLATILITY_AXIS
            and row.get("interpretation_status")
            == _interpretation_label(policy, "interpretable")
        ]
        best_primary = _best_segment(primary_rows, "avg_forward_return", reverse=True)
        worst_primary = _best_segment(primary_rows, "avg_max_drawdown", reverse=False)
        rows.append(
            clean_for_yaml(
                {
                    "family_name": family_name,
                    "family_id": blocker.get("family_id", family_name),
                    "segmentable": True,
                    "interpretation_status": "SEGMENT_DIAGNOSTIC_SUMMARY_ONLY",
                    "primary_interpretable_segment_count": len(primary_rows),
                    "volatility_interpretable_segment_count": len(volatility_rows),
                    "best_primary_segment_by_avg_forward_return": best_primary,
                    "worst_primary_segment_by_avg_max_drawdown": worst_primary,
                    "diagnostic_interpretation": (
                        "Segment metrics describe prior actual-path evidence by "
                        "regime label; they do not change candidate verdicts."
                    ),
                    "scope_change_recommended": False,
                    "existing_candidate_verdict_changed": False,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_safety_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
    performance_row_count: int,
    blocker_row_count: int,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "performance_row_count": performance_row_count,
            "family_blocker_row_count": blocker_row_count,
            "does_read_cached_market_data": True,
            "data_quality_gate_required": True,
            "does_consume_regime_label_series": True,
            "does_consume_prior_actual_path_validation": True,
            "does_generate_new_candidate_signal": False,
            "does_change_existing_candidate_verdict": False,
            "does_allow_position_sizing": False,
            "does_allow_broker_action": False,
            "allowed_next_step": "TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT",
            **SAFETY_FIELDS,
        }
    )


def write_regime_segmented_candidate_validation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    performance_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    family_blocker_rows: Sequence[Mapping[str, Any]],
    interpretation_rows: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "regime_segmented_candidate_validation_summary.json",
        "performance_json": output_dir
        / "regime_segmented_candidate_performance_matrix.json",
        "performance_csv": output_dir
        / "regime_segmented_candidate_performance_matrix.csv",
        "coverage_json": output_dir / "regime_segmented_candidate_coverage_matrix.json",
        "coverage_csv": output_dir / "regime_segmented_candidate_coverage_matrix.csv",
        "family_blocker_json": output_dir
        / "regime_segmented_family_blocker_matrix.json",
        "family_blocker_csv": output_dir / "regime_segmented_family_blocker_matrix.csv",
        "interpretation_json": output_dir / "regime_segmented_interpretation_matrix.json",
        "interpretation_csv": output_dir / "regime_segmented_interpretation_matrix.csv",
        "safety_boundary": output_dir
        / "regime_segmented_candidate_validation_safety_boundary.json",
        "report_doc": docs_root / "regime_segmented_candidate_validation.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["performance_json"], {**dict(common), "rows": performance_rows})
    write_csv_rows(paths["performance_csv"], performance_rows)
    write_json(paths["coverage_json"], {**dict(common), "rows": coverage_rows})
    write_csv_rows(paths["coverage_csv"], coverage_rows)
    write_json(paths["family_blocker_json"], {**dict(common), "rows": family_blocker_rows})
    write_csv_rows(paths["family_blocker_csv"], family_blocker_rows)
    write_json(paths["interpretation_json"], {**dict(common), "rows": interpretation_rows})
    write_csv_rows(paths["interpretation_csv"], interpretation_rows)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(paths["report_doc"], _render_report(summary=summary))
    return {key: str(path) for key, path in paths.items()}


def _segment_actual_path_family(
    *,
    family_name: str,
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
    label_frame: pd.DataFrame,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    summary = _load_json(_policy_path(source, "source_summary"))
    source_status = _status(summary)
    allowed_statuses = set(_strings(source.get("source_statuses_allowed")))
    if source_status not in allowed_statuses:
        raise RegimeSegmentedCandidateValidationError(
            f"{family_name} source status {source_status} not allowed"
        )
    matrix_path = _policy_path(source, "source_matrix")
    frame = pd.read_csv(matrix_path)
    candidate_ids = _strings(source.get("candidate_ids"))
    if candidate_ids:
        frame = frame.loc[frame["candidate_id"].astype(str).isin(set(candidate_ids))].copy()
    if frame.empty:
        raise RegimeSegmentedCandidateValidationError(
            f"{family_name} source matrix has no segmentable rows"
        )

    joined = _join_with_labels(frame, label_frame, source)
    family_id = str(source.get("family_id", family_name))
    coverage_rows = _coverage_rows(
        family_name=family_name,
        family_id=family_id,
        source_status=source_status,
        joined=joined,
    )
    performance_rows = _performance_rows(
        family_name=family_name,
        family_id=family_id,
        source_status=source_status,
        joined=joined,
        source=source,
        policy=policy,
    )
    label_unmatched_count = int(joined["primary_regime_label"].isna().sum())
    blocker = clean_for_yaml(
        {
            "family_name": family_name,
            "family_id": family_id,
            "source_type": source.get("source_type", ""),
            "source_status": source_status,
            "segmentable": True,
            "blocker_status": "SEGMENTATION_AVAILABLE_DIAGNOSTIC_ONLY",
            "blocker_reason": "",
            "source_record_count": int(len(frame)),
            "validation_eligible_count": _bool_count(frame.get("validation_eligible")),
            "label_unmatched_count": label_unmatched_count,
            "label_match_rate": round_float(
                1.0 - label_unmatched_count / len(joined) if len(joined) else 0.0
            ),
            "source_matrix": str(matrix_path),
            **SAFETY_FIELDS,
        }
    )
    return blocker, performance_rows, coverage_rows


def _build_source_blocked_family_row(
    *,
    family_name: str,
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _load_json(_policy_path(source, "source_summary"))
    source_status = _status(summary)
    allowed_statuses = set(_strings(source.get("source_statuses_allowed")))
    if source_status not in allowed_statuses:
        raise RegimeSegmentedCandidateValidationError(
            f"{family_name} source status {source_status} not allowed"
        )
    scorecard_path = _policy_path(source, "source_scorecard")
    scorecard = pd.read_csv(scorecard_path)
    summary_block = mapping(summary.get("summary"))
    return clean_for_yaml(
        {
            "family_name": family_name,
            "family_id": source.get("family_id", family_name),
            "source_type": source.get("source_type", ""),
            "source_status": source_status,
            "segmentable": False,
            "blocker_status": _interpretation_label(policy, "source_blocked"),
            "blocker_reason": summary_block.get(
                "source_status", "source blocked before candidate generation"
            ),
            "source_record_count": int(len(scorecard)),
            "validation_eligible_count": 0,
            "label_unmatched_count": 0,
            "label_match_rate": 0.0,
            "selected_concept_count": summary.get(
                "selected_concept_count", summary_block.get("selected_concept_count", 0)
            ),
            "rejected_concept_count": summary.get(
                "rejected_concept_count", summary_block.get("rejected_concept_count", 0)
            ),
            "source_scorecard": str(scorecard_path),
            **SAFETY_FIELDS,
        }
    )


def _join_with_labels(
    frame: pd.DataFrame,
    label_frame: pd.DataFrame,
    source: Mapping[str, Any],
) -> pd.DataFrame:
    target_asset_column = str(source.get("target_asset_column", "target_asset"))
    source_date_column = str(source.get("source_date_column", "source_date"))
    if target_asset_column not in frame:
        raise RegimeSegmentedCandidateValidationError(
            f"source matrix missing target asset column: {target_asset_column}"
        )
    if source_date_column not in frame:
        raise RegimeSegmentedCandidateValidationError(
            f"source matrix missing source date column: {source_date_column}"
        )
    work = frame.copy()
    work["_segment_date"] = pd.to_datetime(
        work[source_date_column], errors="coerce", utc=True
    ).dt.date.astype(str)
    work["_segment_ticker"] = work[target_asset_column].astype(str)
    labels = label_frame.rename(columns={"date": "_segment_date", "ticker": "_segment_ticker"})
    return work.merge(labels, on=["_segment_date", "_segment_ticker"], how="left")


def _coverage_rows(
    *,
    family_name: str,
    family_id: str,
    source_status: str,
    joined: pd.DataFrame,
) -> list[dict[str, Any]]:
    group_columns = ["candidate_id", "target_asset", "horizon"]
    rows: list[dict[str, Any]] = []
    for group_key, group in joined.groupby(group_columns, dropna=False):
        candidate_id, target_asset, horizon = [str(item) for item in group_key]
        label_matched = group["primary_regime_label"].notna()
        rows.append(
            clean_for_yaml(
                {
                    "family_name": family_name,
                    "family_id": family_id,
                    "source_status": source_status,
                    "candidate_id": candidate_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "source_record_count": int(len(group)),
                    "validation_eligible_count": _bool_count(group.get("validation_eligible")),
                    "label_matched_count": int(label_matched.sum()),
                    "label_unmatched_count": int((~label_matched).sum()),
                    "label_match_rate": rate(int(label_matched.sum()), int(len(group))),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def _performance_rows(
    *,
    family_name: str,
    family_id: str,
    source_status: str,
    joined: pd.DataFrame,
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    threshold = int(_threshold_value(policy, "segment_min_eligible_records"))
    axis_specs = (
        (PRIMARY_AXIS, "primary_regime_label"),
        (VOLATILITY_AXIS, "volatility_regime_label"),
    )
    for label_axis, label_column in axis_specs:
        grouped = joined.loc[joined[label_column].notna()].copy()
        if grouped.empty:
            continue
        for group_key, group in grouped.groupby(
            ["candidate_id", "target_asset", "horizon", label_column],
            dropna=False,
        ):
            candidate_id, target_asset, horizon, regime_label = [
                str(item) for item in group_key
            ]
            eligible = _eligible_group(group)
            eligible_count = len(eligible)
            interpretation_status = (
                _interpretation_label(policy, "interpretable")
                if eligible_count >= threshold
                else _interpretation_label(policy, "sparse")
            )
            rows.append(
                clean_for_yaml(
                    {
                        "family_name": family_name,
                        "family_id": family_id,
                        "source_status": source_status,
                        "candidate_id": candidate_id,
                        "target_asset": target_asset,
                        "horizon": horizon,
                        "label_axis": label_axis,
                        "regime_label": regime_label,
                        "source_record_count": int(len(group)),
                        "validation_eligible_count": int(eligible_count),
                        "segment_min_eligible_records": threshold,
                        "interpretation_status": interpretation_status,
                        "avg_forward_return": _mean_or_none(
                            eligible, str(source.get("forward_return_column", ""))
                        ),
                        "median_forward_return": _median_or_none(
                            eligible, str(source.get("forward_return_column", ""))
                        ),
                        "avg_max_drawdown": _mean_or_none(
                            eligible, str(source.get("max_drawdown_column", ""))
                        ),
                        "worst_max_drawdown": _min_or_none(
                            eligible, str(source.get("max_drawdown_column", ""))
                        ),
                        "stress_event_rate": _mean_or_none(
                            eligible, str(source.get("stress_event_column", ""))
                        ),
                        "avg_signal_value": _mean_or_none(eligible, "signal_value"),
                        "avg_signal_confidence": _mean_or_none(
                            eligible, "signal_confidence"
                        ),
                        "avg_relative_return": _mean_or_none(
                            eligible, str(source.get("relative_return_column", ""))
                        ),
                        "avg_family_forward_return": _mean_or_none(
                            eligible,
                            str(source.get("family_average_forward_return_column", "")),
                        ),
                        "avg_family_worst_drawdown": _mean_or_none(
                            eligible,
                            str(source.get("family_worst_drawdown_column", "")),
                        ),
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def _load_label_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RegimeSegmentedCandidateValidationError(f"label series missing: {path}")
    labels = pd.read_csv(path)
    required = {"date", "ticker", "label_axis", "regime_label"}
    missing = required - set(labels.columns)
    if missing:
        raise RegimeSegmentedCandidateValidationError(
            f"label series missing columns: {sorted(missing)}"
        )
    pivot = labels.pivot_table(
        index=["date", "ticker"],
        columns="label_axis",
        values="regime_label",
        aggfunc="last",
    ).reset_index()
    required_axes = {PRIMARY_AXIS, VOLATILITY_AXIS}
    missing_axes = required_axes - set(pivot.columns)
    if missing_axes:
        raise RegimeSegmentedCandidateValidationError(
            f"label series missing axes: {sorted(missing_axes)}"
        )
    return pivot.rename(
        columns={
            PRIMARY_AXIS: "primary_regime_label",
            VOLATILITY_AXIS: "volatility_regime_label",
        }
    )


def _summary_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    label_series_path: Path,
    label_summary_path: Path,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    label_summary: Mapping[str, Any],
    requested_range: str,
    performance_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    family_blocker_rows: Sequence[Mapping[str, Any]],
    interpretation_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    segmentable_families = [
        row["family_name"] for row in family_blocker_rows if row.get("segmentable") is True
    ]
    blocked_families = [
        row["family_name"] for row in family_blocker_rows if row.get("segmentable") is False
    ]
    data_quality = data_quality_payload(
        quality_report,
        prices_path,
        rates_path,
        marketstack_prices_path,
    )
    data_quality["required_command"] = "aits validate-data"
    data_quality["report_path"] = str(quality_report_path)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "Regime-Segmented Candidate Validation",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": requested_range,
            "data_quality_status": quality_report.status,
            "data_quality_gate": data_quality,
            "policy_path": str(policy_path),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "label_series_path": str(label_series_path),
            "label_summary_path": str(label_summary_path),
            "label_source_status": _status(label_summary),
            "label_source_data_quality_status": _summary_value(
                label_summary, "data_quality_status"
            ),
            "performance_row_count": len(performance_rows),
            "coverage_row_count": len(coverage_rows),
            "family_blocker_row_count": len(family_blocker_rows),
            "interpretation_row_count": len(interpretation_rows),
            "segmentable_family_count": len(segmentable_families),
            "blocked_family_count": len(blocked_families),
            "segmentable_families": segmentable_families,
            "blocked_families": blocked_families,
            "candidate_family_count": len(family_blocker_rows),
            "regime_label_axes": [PRIMARY_AXIS, VOLATILITY_AXIS],
            "breadth_proxy_status": next(
                (
                    row.get("blocker_status")
                    for row in family_blocker_rows
                    if row.get("family_name") == "breadth_proxy"
                ),
                "",
            ),
            "scope_change_recommended": False,
            "existing_candidate_verdict_changed": False,
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    data_quality_status: str,
    requested_range: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Regime-Segmented Candidate Validation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": requested_range,
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }


def _render_report(*, summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Regime-Segmented Candidate Validation",
            "",
            "TRADING-2317 用 TRADING-2316 的 regime labels 重新解释已有候选 "
            "actual-path evidence。它不生成新 signal，不改变候选 verdict，不进入仓位、"
            "paper-shadow、production 或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- label_source_status: `{summary['label_source_status']}`",
            f"- performance_row_count: `{summary['performance_row_count']}`",
            f"- coverage_row_count: `{summary['coverage_row_count']}`",
            f"- family_blocker_row_count: `{summary['family_blocker_row_count']}`",
            f"- segmentable_families: `{','.join(summary['segmentable_families'])}`",
            f"- blocked_families: `{','.join(summary['blocked_families'])}`",
            "- candidate_signal_generated: `False`",
            "- existing_candidate_verdict_changed: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Boundary",
            "",
            "Segment metrics are diagnostic only. Breadth proxy remains source-blocked "
            "because current constituent snapshots are missing. Any future use in report "
            "integration, forward observe, scope review, paper-shadow, production or broker "
            "paths requires a separate owner-reviewed task and quality gate.",
            "",
        ]
    )


def _run_data_quality_gate(
    *,
    policy: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    data_quality = mapping(policy.get("data_quality"))
    expected_tickers = _strings(data_quality.get("required_price_tickers"))
    universe = load_universe()
    secondary_path = (
        marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=quality_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, quality_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def _validate_label_summary(
    label_summary: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    label_source = mapping(policy.get("label_source"))
    required_status = str(label_source.get("required_status", ""))
    if _status(label_summary) != required_status:
        raise RegimeSegmentedCandidateValidationError(
            f"label summary status must be {required_status}"
        )
    allowed_quality = set(_strings(label_source.get("required_data_quality_statuses")))
    label_quality = str(_summary_value(label_summary, "data_quality_status"))
    if label_quality not in allowed_quality:
        raise RegimeSegmentedCandidateValidationError(
            f"label summary data quality status {label_quality} not allowed"
        )


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
        "data_quality",
        "label_source",
        "candidate_sources",
        "threshold_governance",
        "thresholds",
        "interpretation_labels",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise RegimeSegmentedCandidateValidationError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "regime_segmented_candidate_validation_policy":
        raise RegimeSegmentedCandidateValidationError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise RegimeSegmentedCandidateValidationError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise RegimeSegmentedCandidateValidationError("policy market_regime mismatch")
    for field in (
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
    ):
        if not mapping(policy.get("threshold_governance")).get(field):
            raise RegimeSegmentedCandidateValidationError(
                f"threshold_governance.{field} is required"
            )
    for threshold_id in ("segment_min_eligible_records", "segment_min_candidate_count"):
        definition = mapping(mapping(policy.get("thresholds")).get(threshold_id))
        if "value" not in definition or not definition.get("rationale"):
            raise RegimeSegmentedCandidateValidationError(
                f"threshold {threshold_id} requires value and rationale"
            )
    expected_families = {
        "volatility_risk_cap",
        "breadth_proxy",
        "ai_leadership",
        "liquidity_pressure",
    }
    if set(mapping(policy.get("candidate_sources"))) != expected_families:
        raise RegimeSegmentedCandidateValidationError("candidate source families mismatch")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise RegimeSegmentedCandidateValidationError(
                f"policy safety.{field} must be {expected}"
            )


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RegimeSegmentedCandidateValidationError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise RegimeSegmentedCandidateValidationError(f"policy file must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RegimeSegmentedCandidateValidationError(f"required JSON missing: {path}")
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RegimeSegmentedCandidateValidationError(f"JSON must be object: {path}")
    return payload


def _policy_path(source: Mapping[str, Any], key: str) -> Path:
    raw = str(source.get(key, ""))
    if not raw:
        raise RegimeSegmentedCandidateValidationError(f"source missing path: {key}")
    path = Path(raw)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _eligible_group(frame: pd.DataFrame) -> pd.DataFrame:
    if "validation_eligible" not in frame:
        return frame
    return frame.loc[frame["validation_eligible"].map(_truthy)].copy()


def _mean_or_none(frame: pd.DataFrame, column: str) -> float | None:
    if not column or column not in frame or frame.empty:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return round_float(float(values.mean()))


def _median_or_none(frame: pd.DataFrame, column: str) -> float | None:
    if not column or column not in frame or frame.empty:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return round_float(float(values.median()))


def _min_or_none(frame: pd.DataFrame, column: str) -> float | None:
    if not column or column not in frame or frame.empty:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return round_float(float(values.min()))


def _bool_count(series: object) -> int:
    if series is None or not isinstance(series, pd.Series):
        return 0
    return int(series.map(_truthy).sum())


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _best_segment(
    rows: Sequence[Mapping[str, Any]],
    metric: str,
    *,
    reverse: bool,
) -> str:
    candidates = [
        row
        for row in rows
        if row.get(metric) is not None and str(row.get("regime_label", ""))
    ]
    if not candidates:
        return ""
    best = sorted(candidates, key=lambda row: to_float(row.get(metric)), reverse=reverse)[0]
    return str(best.get("regime_label", ""))


def _threshold_value(policy: Mapping[str, Any], threshold_id: str) -> float:
    definition = mapping(mapping(policy.get("thresholds")).get(threshold_id))
    if "value" not in definition:
        raise RegimeSegmentedCandidateValidationError(
            f"policy missing threshold value: {threshold_id}"
        )
    return to_float(definition["value"])


def _interpretation_label(policy: Mapping[str, Any], key: str) -> str:
    labels = mapping(policy.get("interpretation_labels"))
    value = labels.get(key)
    if not value:
        raise RegimeSegmentedCandidateValidationError(
            f"policy missing interpretation label: {key}"
        )
    return str(value)


def _status(payload: Mapping[str, Any]) -> str:
    value = payload.get("status")
    if value:
        return str(value)
    summary = mapping(payload.get("summary"))
    return str(summary.get("status", ""))


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    return mapping(payload.get("summary")).get(key)


def _label_requested_range(label_summary: Mapping[str, Any]) -> str:
    value = _summary_value(label_summary, "actual_requested_date_range")
    return str(value or f"{DEFAULT_BACKTEST_START}..latest")


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value]
    return []


def _parse_optional_date(raw: str | None) -> date | None:
    if raw is None or not str(raw).strip():
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError as exc:
        raise RegimeSegmentedCandidateValidationError(
            f"date must be YYYY-MM-DD: {raw}"
        ) from exc


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


__all__ = [
    "ARTIFACT_ROLE",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_LABEL_SERIES_PATH",
    "DEFAULT_LABEL_SUMMARY_PATH",
    "DEFAULT_MARKETSTACK_PRICES_PATH",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_PRICES_PATH",
    "DEFAULT_RATES_PATH",
    "MODE",
    "PRIMARY_AXIS",
    "REPORT_TYPE",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "VOLATILITY_AXIS",
    "RegimeSegmentedCandidateValidationError",
    "build_interpretation_matrix",
    "build_safety_boundary",
    "run_regime_segmented_candidate_validation",
]
