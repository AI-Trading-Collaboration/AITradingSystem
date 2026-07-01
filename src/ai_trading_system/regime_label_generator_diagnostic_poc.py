from __future__ import annotations

import math
from collections import Counter
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
    load_adjusted_price_matrix,
    mapping,
    max_price_date,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.regime_state_machine_design_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_DESIGN_POLICY_PATH,
)
from ai_trading_system.regime_state_machine_design_audit import (
    EXPECTED_LABELS,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC"
REPORT_TYPE = "regime_label_generator_diagnostic_poc"
ARTIFACT_ROLE = "regime_label_series_diagnostic_poc"
MODE = "diagnostic_poc"
STATUS = "REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY"

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "regime_label_generator_policy.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_SYMBOLS = ("QQQ", "SMH", "SPY")
PRIMARY_AXIS = "primary_trend_regime"
VOLATILITY_AXIS = "volatility_overlay"
NORMAL_VOLATILITY_LABEL = "normal_volatility"

PRIMARY_LABELS = (
    "uptrend",
    "late_uptrend",
    "drawdown",
    "panic",
    "rebound",
    "failed_rebound",
    "range_bound",
)
VOLATILITY_LABELS = ("high_volatility", "low_volatility", NORMAL_VOLATILITY_LABEL)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "diagnostic_only": True,
    "segmentation_only": True,
    "generator_implemented": True,
    "regime_label_series_generated": True,
    "candidate_signal_generated": False,
    "candidate_artifact_generated": False,
    "actual_path_validation_executed": False,
    "scope_review_executed": False,
    "forward_observe_started": False,
    "direct_strategy_signal_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "portfolio_effect": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class RegimeLabelGeneratorDiagnosticPocError(ValueError):
    pass


def run_regime_label_generator_diagnostic_poc(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    design_policy_path: Path = DEFAULT_DESIGN_POLICY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise RegimeLabelGeneratorDiagnosticPocError(
            "regime label generator diagnostic POC only supports diagnostic_poc mode"
        )

    policy = _load_policy(policy_path)
    design_policy = _load_policy(design_policy_path)
    _validate_policy(policy)
    _validate_design_policy(design_policy)

    requested_start = _parse_optional_date(start_date) or date.fromisoformat(
        DEFAULT_BACKTEST_START
    )
    requested_end = _parse_optional_date(end_date)
    resolved_quality_as_of = (
        _parse_optional_date(quality_as_of) or requested_end or max_price_date(prices_path)
    )

    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_symbols=_required_symbols(policy),
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise RegimeLabelGeneratorDiagnosticPocError(
            f"cached data quality gate failed: {quality_report.status}"
        )

    price_matrix = load_adjusted_price_matrix(prices_path, _required_symbols(policy))
    latest_common_date = _latest_common_price_date(price_matrix, _required_symbols(policy))
    actual_end = min(requested_end or resolved_quality_as_of, latest_common_date)
    if actual_end < requested_start:
        raise RegimeLabelGeneratorDiagnosticPocError(
            "requested end date is before requested start date"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    label_rows = build_regime_label_series(
        price_matrix=price_matrix,
        policy=policy,
        start_date=requested_start,
        end_date=actual_end,
    )
    if not label_rows:
        raise RegimeLabelGeneratorDiagnosticPocError(
            "no PIT-eligible regime label rows were generated"
        )

    distribution_rows = build_label_distribution_matrix(label_rows, policy)
    transition_rows = build_label_transition_matrix(label_rows)
    pit_policy = build_regime_label_pit_policy(
        policy=policy,
        design_policy=design_policy,
        generated_at=generated_at,
        prices_path=prices_path,
        rates_path=rates_path,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        requested_start=requested_start,
        actual_end=actual_end,
    )
    safety_boundary = build_safety_boundary(
        generated_at=generated_at,
        data_quality_status=quality_report.status,
        label_row_count=len(label_rows),
    )
    summary = _summary_payload(
        generated_at=generated_at,
        policy_path=policy_path,
        design_policy_path=design_policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        requested_start=requested_start,
        actual_end=actual_end,
        label_rows=label_rows,
        distribution_rows=distribution_rows,
        transition_rows=transition_rows,
        policy=policy,
    )
    common = _common_payload(
        generated_at=generated_at,
        data_quality_status=quality_report.status,
        requested_start=requested_start,
        actual_end=actual_end,
    )
    paths = write_regime_label_generator_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        label_rows=label_rows,
        distribution_rows=distribution_rows,
        transition_rows=transition_rows,
        pit_policy=pit_policy,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "label_row_count": len(label_rows),
            "distribution_rows": distribution_rows,
            "transition_rows": transition_rows,
            "pit_policy": pit_policy,
        }
    )


def build_regime_label_series(
    *,
    price_matrix: pd.DataFrame,
    policy: Mapping[str, Any],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    symbols = _required_symbols(policy)
    thresholds = _threshold_values(policy)
    known_at = mapping(policy.get("known_at_policy"))
    label_version = _label_version(policy)
    dates = _eligible_dates(
        price_matrix=price_matrix,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        minimum_history_days=int(thresholds["minimum_history_days"]),
    )
    rows: list[dict[str, Any]] = []
    for ticker in symbols:
        series = price_matrix[ticker]
        for ts in dates:
            position = price_matrix.index.get_loc(ts)
            if not isinstance(position, int):
                continue
            features = _features_for_position(series, position, thresholds)
            if not features:
                continue
            primary_label = _primary_label(features, thresholds)
            volatility_label = _volatility_label(features, thresholds)
            for axis, label in (
                (PRIMARY_AXIS, primary_label),
                (VOLATILITY_AXIS, volatility_label),
            ):
                rows.append(
                    clean_for_yaml(
                        {
                            "date": ts.date().isoformat(),
                            "ticker": ticker,
                            "label_axis": axis,
                            "regime_label": label,
                            "label_version": label_version,
                            "known_at_policy": known_at.get("policy_id", ""),
                            "known_at": known_at.get("label_known_at", ""),
                            "feature_lag_days": int(known_at.get("feature_lag_days", 0)),
                            "pit_policy_status": known_at.get("pit_policy_status", ""),
                            "short_return": round_float(features["short_return"]),
                            "long_return": round_float(features["long_return"]),
                            "drawdown_from_trailing_high": round_float(
                                features["drawdown_from_trailing_high"]
                            ),
                            "rebound_from_trailing_low": round_float(
                                features["rebound_from_trailing_low"]
                            ),
                            "annualized_realized_volatility": round_float(
                                features["annualized_realized_volatility"]
                            ),
                            "source_price": round_float(features["source_price"]),
                            "data_feature_scope": "trailing_adjusted_close_only",
                            "future_outcome_used": False,
                            "hindsight_relabeling_allowed": False,
                            "candidate_signal_generated": False,
                            "direct_strategy_signal_allowed": False,
                            "promotion_allowed": False,
                            "paper_shadow_allowed": False,
                            "production_allowed": False,
                            "broker_action": "none",
                        }
                    )
                )
    return rows


def build_label_distribution_matrix(
    label_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    counts: Counter[tuple[str, str, str]] = Counter()
    totals: Counter[tuple[str, str]] = Counter()
    first_dates: dict[tuple[str, str, str], str] = {}
    last_dates: dict[tuple[str, str, str], str] = {}
    for row in label_rows:
        ticker = str(row["ticker"])
        axis = str(row["label_axis"])
        label = str(row["regime_label"])
        key = (ticker, axis, label)
        counts[key] += 1
        totals[(ticker, axis)] += 1
        first_dates.setdefault(key, str(row["date"]))
        last_dates[key] = str(row["date"])

    for ticker in _required_symbols(policy):
        for axis, labels in (
            (PRIMARY_AXIS, PRIMARY_LABELS),
            (VOLATILITY_AXIS, VOLATILITY_LABELS),
        ):
            total = totals[(ticker, axis)]
            for label in labels:
                key = (ticker, axis, label)
                count = counts[key]
                rows.append(
                    clean_for_yaml(
                        {
                            "ticker": ticker,
                            "label_axis": axis,
                            "regime_label": label,
                            "label_count": count,
                            "axis_total_count": total,
                            "label_share": round_float(count / total if total else 0.0),
                            "first_date": first_dates.get(key, ""),
                            "last_date": last_dates.get(key, ""),
                            **SAFETY_FIELDS,
                        }
                    )
                )
    return rows


def build_label_transition_matrix(
    label_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in label_rows:
        key = (str(row["ticker"]), str(row["label_axis"]))
        by_key.setdefault(key, []).append(row)

    transition_counts: Counter[tuple[str, str, str, str]] = Counter()
    first_dates: dict[tuple[str, str, str, str], str] = {}
    last_dates: dict[tuple[str, str, str, str], str] = {}
    for (ticker, axis), rows in by_key.items():
        ordered = sorted(rows, key=lambda item: str(item["date"]))
        for previous, current in zip(ordered, ordered[1:], strict=False):
            from_label = str(previous["regime_label"])
            to_label = str(current["regime_label"])
            key = (ticker, axis, from_label, to_label)
            transition_counts[key] += 1
            first_dates.setdefault(key, str(current["date"]))
            last_dates[key] = str(current["date"])

    return [
        clean_for_yaml(
            {
                "ticker": ticker,
                "label_axis": axis,
                "from_regime_label": from_label,
                "to_regime_label": to_label,
                "transition_count": count,
                "first_transition_date": first_dates[key],
                "last_transition_date": last_dates[key],
                "same_state_persistence": from_label == to_label,
                "future_confirmation_used": False,
                "hindsight_relabeling_allowed": False,
                **SAFETY_FIELDS,
            }
        )
        for key, count in sorted(transition_counts.items())
        for ticker, axis, from_label, to_label in [key]
    ]


def build_regime_label_pit_policy(
    *,
    policy: Mapping[str, Any],
    design_policy: Mapping[str, Any],
    generated_at: datetime,
    prices_path: Path,
    rates_path: Path,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    requested_start: date,
    actual_end: date,
) -> dict[str, Any]:
    known_at = mapping(policy.get("known_at_policy"))
    thresholds = mapping(policy.get("thresholds"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.pit_policy.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": _date_range_label(requested_start, actual_end),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "source_design_policy_id": design_policy.get("policy_id", ""),
            "source_design_policy_version": design_policy.get("version", ""),
            "label_version": _label_version(policy),
            "known_at_policy": known_at,
            "threshold_values": {
                key: mapping(value).get("value") for key, value in thresholds.items()
            },
            "required_controls": [
                "validate_data_cache_before_label_generation",
                "trailing_adjusted_close_features_only",
                "no_future_return_or_drawdown",
                "no_future_volatility_window",
                "no_hindsight_episode_relabeling",
                "immutable_daily_label_records_with_label_version",
                "missing_input_fail_closed",
            ],
            "blocked_failure_modes": [
                "future_return_defines_runtime_state",
                "future_drawdown_defines_runtime_state",
                "full_episode_peak_trough_rewrites_labels",
                "regime_label_used_as_direct_strategy_signal",
                "regime_segment_result_treated_as_promotion_evidence",
            ],
            "input_paths": {
                "prices": str(prices_path),
                "rates": str(rates_path),
                "data_quality_report": str(quality_report_path),
            },
            "data_quality_gate": {
                "required_command": "aits validate-data",
                "status": quality_report.status,
                "passed": quality_report.passed,
                "as_of": quality_report.as_of.isoformat(),
                "report_path": str(quality_report_path),
            },
            **SAFETY_FIELDS,
        }
    )


def build_safety_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
    label_row_count: int,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "label_row_count": label_row_count,
            "does_read_cached_market_data": True,
            "data_quality_gate_required": True,
            "does_generate_regime_label_series": True,
            "does_generate_candidate_signal": False,
            "does_run_backtest": False,
            "does_run_actual_path_validation": False,
            "does_start_forward_observe": False,
            "does_allow_direct_strategy_signal": False,
            "does_allow_position_sizing": False,
            "does_allow_broker_action": False,
            "allowed_next_step": "TRADING-2317_REGIME_SEGMENTED_CANDIDATE_VALIDATION",
            **SAFETY_FIELDS,
        }
    )


def write_regime_label_generator_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    label_rows: Sequence[Mapping[str, Any]],
    distribution_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    pit_policy: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "regime_label_generation_summary.json",
        "label_series": output_dir / "regime_label_series.csv",
        "pit_policy": output_dir / "regime_label_pit_policy.json",
        "distribution_json": output_dir / "regime_label_distribution_matrix.json",
        "distribution_csv": output_dir / "regime_label_distribution_matrix.csv",
        "transition_json": output_dir / "regime_label_transition_matrix.json",
        "transition_csv": output_dir / "regime_label_transition_matrix.csv",
        "safety_boundary": output_dir / "regime_label_generation_safety_boundary.json",
        "report_doc": docs_root / "regime_label_generator_diagnostic_poc.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_csv_rows(paths["label_series"], label_rows)
    write_json(paths["pit_policy"], dict(pit_policy))
    write_json(paths["distribution_json"], {**dict(common), "rows": distribution_rows})
    write_csv_rows(paths["distribution_csv"], distribution_rows)
    write_json(paths["transition_json"], {**dict(common), "rows": transition_rows})
    write_csv_rows(paths["transition_csv"], transition_rows)
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(paths["report_doc"], _render_report(summary=summary))
    return {key: str(path) for key, path in paths.items()}


def _summary_payload(
    *,
    generated_at: datetime,
    policy_path: Path,
    design_policy_path: Path,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    requested_start: date,
    actual_end: date,
    label_rows: Sequence[Mapping[str, Any]],
    distribution_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    observed_labels = sorted({str(row["regime_label"]) for row in label_rows})
    primary_labels = sorted(
        {
            str(row["regime_label"])
            for row in label_rows
            if row["label_axis"] == PRIMARY_AXIS
        }
    )
    volatility_labels = sorted(
        {
            str(row["regime_label"])
            for row in label_rows
            if row["label_axis"] == VOLATILITY_AXIS
        }
    )
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
            "title": "Regime Label Generator Diagnostic POC",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": _date_range_label(requested_start, actual_end),
            "requested_start": requested_start.isoformat(),
            "actual_end": actual_end.isoformat(),
            "data_quality_status": quality_report.status,
            "data_quality_gate": data_quality,
            "policy_path": str(policy_path),
            "design_policy_path": str(design_policy_path),
            "policy_id": policy.get("policy_id", ""),
            "policy_version": policy.get("version", ""),
            "label_version": _label_version(policy),
            "required_symbols": list(_required_symbols(policy)),
            "label_axis_count": 2,
            "label_axes": [PRIMARY_AXIS, VOLATILITY_AXIS],
            "label_row_count": len(label_rows),
            "distribution_row_count": len(distribution_rows),
            "transition_row_count": len(transition_rows),
            "taxonomy_label_ids": list(EXPECTED_LABELS),
            "observed_label_ids": observed_labels,
            "observed_primary_label_ids": primary_labels,
            "observed_volatility_label_ids": volatility_labels,
            "overlay_neutral_label": NORMAL_VOLATILITY_LABEL,
            "segmentation_ready": True,
            "segmentation_ready_scope": "diagnostic_only_validation_segmentation",
            "candidate_signal_generated": False,
            "candidate_artifact_generated": False,
            "actual_path_validation_executed": False,
            "next_task": "TRADING-2317_REGIME_SEGMENTED_CANDIDATE_VALIDATION",
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    data_quality_status: str,
    requested_start: date,
    actual_end: date,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Regime Label Generator Diagnostic POC",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": _date_range_label(requested_start, actual_end),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }


def _render_report(*, summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Regime Label Generator Diagnostic POC",
            "",
            "TRADING-2316 生成 diagnostic-only regime label series，供后续 validation "
            "segmentation 使用；它不是交易信号，也不进入仓位、日报生产建议或 broker path。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- policy_version: `{summary['policy_version']}`",
            f"- label_row_count: `{summary['label_row_count']}`",
            f"- distribution_row_count: `{summary['distribution_row_count']}`",
            f"- transition_row_count: `{summary['transition_row_count']}`",
            f"- observed_label_ids: `{','.join(summary['observed_label_ids'])}`",
            "- segmentation_ready_scope: `diagnostic_only_validation_segmentation`",
            "- candidate_signal_generated: `False`",
            "- candidate_artifact_generated: `False`",
            "- actual_path_validation_executed: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## PIT Boundary",
            "",
            "Labels only use trailing adjusted-close features known after each market "
            "close. The POC blocks future return, future drawdown, future volatility, "
            "final peak/trough and hindsight episode relabeling. `normal_volatility` "
            "is an overlay neutral state, not a TRADING-2315 taxonomy label.",
            "",
            "## Data Quality",
            "",
            "Cached data quality is enforced through the same validation code path as "
            "`aits validate-data`; the generated summary and PIT policy link the data "
            "quality report and disclose the status.",
            "",
            "## Safety",
            "",
            "本产物只能作为 TRADING-2317 segmentation input。任何 candidate validation、"
            "scope review、forward observe、report integration、paper-shadow、production "
            "或 broker 使用都需要独立任务、独立质量门禁和 owner review。",
            "",
        ]
    )


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    required_symbols: Sequence[str],
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    universe = load_universe()
    secondary_path = (
        marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(required_symbols),
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


def _features_for_position(
    series: pd.Series,
    position: int,
    thresholds: Mapping[str, float],
) -> dict[str, float]:
    current_price = to_float(series.iloc[position])
    if current_price <= 0.0:
        return {}
    short_return = _rolling_return(
        series,
        position,
        int(thresholds["short_return_lookback_days"]),
    )
    long_return = _rolling_return(
        series,
        position,
        int(thresholds["long_return_lookback_days"]),
    )
    drawdown = _drawdown_from_trailing_high(
        series,
        position,
        int(thresholds["drawdown_lookback_days"]),
    )
    rebound = _rebound_from_trailing_low(
        series,
        position,
        int(thresholds["rebound_lookback_days"]),
    )
    volatility = _annualized_volatility(
        series,
        position,
        int(thresholds["volatility_lookback_days"]),
    )
    if None in (short_return, long_return, drawdown, rebound, volatility):
        return {}
    return {
        "source_price": current_price,
        "short_return": float(short_return),
        "long_return": float(long_return),
        "drawdown_from_trailing_high": float(drawdown),
        "rebound_from_trailing_low": float(rebound),
        "annualized_realized_volatility": float(volatility),
    }


def _primary_label(features: Mapping[str, float], thresholds: Mapping[str, float]) -> str:
    short_return = features["short_return"]
    long_return = features["long_return"]
    drawdown = features["drawdown_from_trailing_high"]
    rebound = features["rebound_from_trailing_low"]
    volatility = features["annualized_realized_volatility"]

    if (
        drawdown <= thresholds["panic_drawdown_threshold"]
        and volatility >= thresholds["panic_volatility_threshold"]
    ):
        return "panic"
    if drawdown <= thresholds["drawdown_threshold"] and short_return < 0.0:
        return "drawdown"
    if (
        rebound >= thresholds["rebound_return_threshold"]
        and short_return >= 0.0
        and drawdown <= thresholds["rebound_prior_drawdown_threshold"]
    ):
        return "rebound"
    if (
        rebound >= thresholds["failed_rebound_min_rebound"]
        and short_return < 0.0
        and drawdown <= thresholds["failed_rebound_drawdown_threshold"]
    ):
        return "failed_rebound"
    if (
        long_return >= thresholds["long_uptrend_min_return"]
        and short_return >= thresholds["short_uptrend_min_return"]
        and drawdown > thresholds["late_uptrend_drawdown_threshold"]
    ):
        return "uptrend"
    if long_return >= thresholds["long_uptrend_min_return"]:
        return "late_uptrend"
    if (
        abs(long_return) <= thresholds["range_long_abs_return_threshold"]
        and drawdown > thresholds["range_drawdown_floor"]
    ):
        return "range_bound"
    if short_return >= 0.0 and long_return >= 0.0:
        return "uptrend"
    if drawdown <= thresholds["drawdown_threshold"]:
        return "drawdown"
    return "range_bound"


def _volatility_label(
    features: Mapping[str, float],
    thresholds: Mapping[str, float],
) -> str:
    volatility = features["annualized_realized_volatility"]
    if volatility >= thresholds["high_volatility_annualized_threshold"]:
        return "high_volatility"
    if volatility <= thresholds["low_volatility_annualized_threshold"]:
        return "low_volatility"
    return NORMAL_VOLATILITY_LABEL


def _rolling_return(series: pd.Series, position: int, lookback_days: int) -> float | None:
    if position < lookback_days:
        return None
    previous = to_float(series.iloc[position - lookback_days])
    current = to_float(series.iloc[position])
    if previous <= 0.0 or current <= 0.0:
        return None
    return current / previous - 1.0


def _drawdown_from_trailing_high(
    series: pd.Series,
    position: int,
    lookback_days: int,
) -> float | None:
    if position + 1 < lookback_days:
        return None
    window = series.iloc[position - lookback_days + 1 : position + 1].dropna()
    if window.empty:
        return None
    high = to_float(window.max())
    current = to_float(series.iloc[position])
    if high <= 0.0 or current <= 0.0:
        return None
    return current / high - 1.0


def _rebound_from_trailing_low(
    series: pd.Series,
    position: int,
    lookback_days: int,
) -> float | None:
    if position + 1 < lookback_days:
        return None
    window = series.iloc[position - lookback_days + 1 : position + 1].dropna()
    if window.empty:
        return None
    low = to_float(window.min())
    current = to_float(series.iloc[position])
    if low <= 0.0 or current <= 0.0:
        return None
    return current / low - 1.0


def _annualized_volatility(
    series: pd.Series,
    position: int,
    lookback_days: int,
) -> float | None:
    if position < lookback_days:
        return None
    returns = series.pct_change().iloc[position - lookback_days + 1 : position + 1]
    returns = pd.to_numeric(returns, errors="coerce").dropna()
    if len(returns) < lookback_days - 1:
        return None
    return float(returns.std(ddof=0) * math.sqrt(252.0))


def _eligible_dates(
    *,
    price_matrix: pd.DataFrame,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    minimum_history_days: int,
) -> list[pd.Timestamp]:
    clean = price_matrix.loc[:, list(symbols)].dropna(how="any")
    dates: list[pd.Timestamp] = []
    for ts in clean.index:
        if ts.date() < start_date or ts.date() > end_date:
            continue
        position = price_matrix.index.get_loc(ts)
        if isinstance(position, int) and position >= minimum_history_days:
            dates.append(ts)
    return dates


def _latest_common_price_date(
    price_matrix: pd.DataFrame,
    symbols: Sequence[str],
) -> date:
    clean = price_matrix.loc[:, list(symbols)].dropna(how="any")
    if clean.empty:
        raise RegimeLabelGeneratorDiagnosticPocError(
            f"price cache has no common dates for required symbols: {list(symbols)}"
        )
    return pd.Timestamp(clean.index.max()).date()


def _threshold_values(policy: Mapping[str, Any]) -> dict[str, float]:
    required = (
        "minimum_history_days",
        "short_return_lookback_days",
        "long_return_lookback_days",
        "drawdown_lookback_days",
        "rebound_lookback_days",
        "volatility_lookback_days",
        "short_uptrend_min_return",
        "long_uptrend_min_return",
        "late_uptrend_drawdown_threshold",
        "drawdown_threshold",
        "panic_drawdown_threshold",
        "panic_volatility_threshold",
        "rebound_return_threshold",
        "rebound_prior_drawdown_threshold",
        "failed_rebound_min_rebound",
        "failed_rebound_drawdown_threshold",
        "range_long_abs_return_threshold",
        "range_drawdown_floor",
        "high_volatility_annualized_threshold",
        "low_volatility_annualized_threshold",
    )
    thresholds = mapping(policy.get("thresholds"))
    values: dict[str, float] = {}
    missing: list[str] = []
    for key in required:
        definition = mapping(thresholds.get(key))
        if "value" not in definition or not definition.get("rationale"):
            missing.append(key)
            continue
        values[key] = to_float(definition["value"])
    if missing:
        raise RegimeLabelGeneratorDiagnosticPocError(
            f"policy thresholds missing value/rationale: {missing}"
        )
    return values


def _required_symbols(policy: Mapping[str, Any]) -> tuple[str, ...]:
    inputs = mapping(policy.get("inputs"))
    raw_symbols = inputs.get("required_symbols", REQUIRED_SYMBOLS)
    if not isinstance(raw_symbols, Sequence) or isinstance(raw_symbols, str):
        raise RegimeLabelGeneratorDiagnosticPocError(
            "policy inputs.required_symbols must be a list"
        )
    return tuple(str(symbol) for symbol in raw_symbols)


def _label_version(policy: Mapping[str, Any]) -> str:
    return f"{policy.get('policy_id', 'regime_label_generator_policy')}:{policy.get('version', '')}"


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RegimeLabelGeneratorDiagnosticPocError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise RegimeLabelGeneratorDiagnosticPocError(f"policy file must be object: {path}")
    return payload


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
        "inputs",
        "known_at_policy",
        "label_axes",
        "threshold_governance",
        "thresholds",
        "safety",
    )
    missing = [field for field in required_fields if not policy.get(field)]
    if missing:
        raise RegimeLabelGeneratorDiagnosticPocError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "regime_label_generator_policy":
        raise RegimeLabelGeneratorDiagnosticPocError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise RegimeLabelGeneratorDiagnosticPocError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise RegimeLabelGeneratorDiagnosticPocError("policy market_regime mismatch")
    if set(_required_symbols(policy)) != set(REQUIRED_SYMBOLS):
        raise RegimeLabelGeneratorDiagnosticPocError("policy required_symbols mismatch")
    axes = mapping(policy.get("label_axes"))
    if set(axes) != {PRIMARY_AXIS, VOLATILITY_AXIS}:
        raise RegimeLabelGeneratorDiagnosticPocError("policy label_axes mismatch")
    primary_axis = mapping(axes.get(PRIMARY_AXIS))
    volatility_axis = mapping(axes.get(VOLATILITY_AXIS))
    if set(primary_axis.get("allowed_labels", [])) != set(PRIMARY_LABELS):
        raise RegimeLabelGeneratorDiagnosticPocError("primary axis labels mismatch")
    if set(volatility_axis.get("allowed_labels", [])) != {
        "high_volatility",
        "low_volatility",
    }:
        raise RegimeLabelGeneratorDiagnosticPocError("volatility axis labels mismatch")
    if volatility_axis.get("neutral_label") != NORMAL_VOLATILITY_LABEL:
        raise RegimeLabelGeneratorDiagnosticPocError("volatility neutral label mismatch")
    governance = mapping(policy.get("threshold_governance"))
    for field in (
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
    ):
        if not governance.get(field):
            raise RegimeLabelGeneratorDiagnosticPocError(
                f"threshold_governance.{field} is required"
            )
    _threshold_values(policy)
    known_at = mapping(policy.get("known_at_policy"))
    if known_at.get("pit_policy_status") != "PIT_APPROX_TRAILING_CLOSE_ONLY":
        raise RegimeLabelGeneratorDiagnosticPocError(
            "known_at_policy.pit_policy_status mismatch"
        )
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise RegimeLabelGeneratorDiagnosticPocError(
                f"policy safety.{field} must be {expected}"
            )


def _validate_design_policy(policy: Mapping[str, Any]) -> None:
    if policy.get("policy_id") != "regime_state_machine_design_policy":
        raise RegimeLabelGeneratorDiagnosticPocError(
            "design policy must be regime_state_machine_design_policy"
        )
    taxonomy = mapping(policy.get("label_taxonomy"))
    if set(taxonomy) != set(EXPECTED_LABELS):
        raise RegimeLabelGeneratorDiagnosticPocError(
            "design policy label taxonomy must match expected labels"
        )
    safety = mapping(policy.get("safety"))
    if safety.get("candidate_signal_generated") is not False:
        raise RegimeLabelGeneratorDiagnosticPocError(
            "design policy must block candidate signals"
        )
    if safety.get("promotion_allowed") is not False:
        raise RegimeLabelGeneratorDiagnosticPocError("design policy must block promotion")


def _parse_optional_date(raw: str | None) -> date | None:
    if raw is None or not str(raw).strip():
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError as exc:
        raise RegimeLabelGeneratorDiagnosticPocError(
            f"date must be YYYY-MM-DD: {raw}"
        ) from exc


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _date_range_label(start_date: date, end_date: date) -> str:
    return f"{start_date.isoformat()}..{end_date.isoformat()}"


__all__ = [
    "ARTIFACT_ROLE",
    "DEFAULT_DESIGN_POLICY_PATH",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_PRICES_PATH",
    "DEFAULT_RATES_PATH",
    "DEFAULT_MARKETSTACK_PRICES_PATH",
    "MODE",
    "NORMAL_VOLATILITY_LABEL",
    "PRIMARY_AXIS",
    "PRIMARY_LABELS",
    "REPORT_TYPE",
    "REQUIRED_SYMBOLS",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "VOLATILITY_AXIS",
    "VOLATILITY_LABELS",
    "RegimeLabelGeneratorDiagnosticPocError",
    "build_label_distribution_matrix",
    "build_label_transition_matrix",
    "build_regime_label_pit_policy",
    "build_regime_label_series",
    "build_safety_boundary",
    "run_regime_label_generator_diagnostic_poc",
]
