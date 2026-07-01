from __future__ import annotations

import csv
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
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
from ai_trading_system.liquidity_rates_generator_poc import (
    BLOCKED_CANDIDATES,
    DEFAULT_CANDIDATES,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_GENERATOR_ROOT,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    STATUS as SOURCE_GENERATOR_STATUS,
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
    mapping,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION"
SOURCE_TASK_ID = "TRADING-2312_LIQUIDITY_RATES_PRESSURE_GENERATOR_POC"
REPORT_TYPE = "liquidity_rates_actual_path_validation"
ARTIFACT_ROLE = "liquidity_rates_actual_path_validation"
MODE = "actual_path_validation"
CANDIDATE_FAMILY = "liquidity_rates_pressure"

STATUS_CONTINUE_RESEARCH = "LIQUIDITY_RATES_VALIDATED_CONTINUE_RESEARCH"
STATUS_INCONCLUSIVE = "LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE"
STATUS_REJECT_RECOMMENDED = "LIQUIDITY_RATES_REJECT_RECOMMENDED"
ALLOWED_STATUSES = {
    STATUS_CONTINUE_RESEARCH,
    STATUS_INCONCLUSIVE,
    STATUS_REJECT_RECOMMENDED,
}

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "liquidity_rates_actual_path_validation_policy.yaml"
)
DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("10d", "20d", "1m")
REQUIRED_PRICE_SYMBOLS = ("QQQ", "SMH", "TLT", "SHY")

OBJECTIVE_VALUATION_PRESSURE = "qqq_smh_valuation_pressure"
OBJECTIVE_DURATION_DRAWDOWN = "high_duration_asset_drawdown"
OBJECTIVE_EXPOSURE_CAP = "risk_on_exposure_cap"
OBJECTIVES = (
    OBJECTIVE_VALUATION_PRESSURE,
    OBJECTIVE_DURATION_DRAWDOWN,
    OBJECTIVE_EXPOSURE_CAP,
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "actual_path_validation_executed": True,
    "scope_review_ready": False,
    "partial_rates_only_validation": True,
    "liquidity_headwind_validation_executed": False,
    "full_liquidity_pressure_validation_ready": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "paper_shadow_recommendation_allowed": False,
    "promotion_eligible": False,
}


class LiquidityRatesActualPathValidationError(ValueError):
    pass


def run_liquidity_rates_actual_path_validation(
    *,
    generator_dir: Path = DEFAULT_GENERATOR_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
    target_assets: Sequence[str] | str = DEFAULT_TARGET_ASSETS,
    horizons: Sequence[str] | str = DEFAULT_HORIZONS,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    quality_as_of: str | date | None = None,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise LiquidityRatesActualPathValidationError(
            "liquidity / rates actual-path validation only supports "
            "actual_path_validation mode"
        )
    policy = _load_policy(policy_path)
    candidate_ids = _parse_candidates(candidates)
    asset_ids = _parse_list(target_assets, uppercase=True)
    horizon_ids = _parse_list(horizons)
    _validate_policy(policy)
    source_summary = _validate_generator_source(generator_dir, candidate_ids)
    resolved_start = _resolve_date(
        start_date,
        default=_date_from_text(str(source_summary.get("requested_start_date")))
        or date.fromisoformat(DEFAULT_BACKTEST_START),
    )
    resolved_end = _resolve_date(
        end_date,
        default=_date_from_text(str(source_summary.get("requested_end_date")))
        or _latest_common_price_date(prices_path, REQUIRED_PRICE_SYMBOLS),
    )
    resolved_quality_as_of = _resolve_date(
        quality_as_of,
        default=_date_from_text(str(mapping(source_summary.get("data_quality")).get("as_of")))
        or resolved_end,
    )
    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_symbols=_required_price_symbols(policy, asset_ids),
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise LiquidityRatesActualPathValidationError(
            f"TRADING-2313 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    price_matrix = _load_price_matrix(prices_path, REQUIRED_PRICE_SYMBOLS)
    candidate_frames = _load_candidate_signal_frames(
        generator_dir=generator_dir,
        candidates=candidate_ids,
        horizons=horizon_ids,
        target_assets=asset_ids,
        start_date=resolved_start,
        end_date=resolved_end,
    )
    actual_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    for frame in candidate_frames.values():
        for record in frame.to_dict("records"):
            actual_path = _actual_path(record, price_matrix)
            actual_rows.append(_actual_path_row(record, actual_path))
            outcome_rows.append(_prediction_outcome_row(record, actual_path, policy))
    objective_rows = [
        _objective_row(objective_id, outcome_rows, policy)
        for objective_id in OBJECTIVES
    ]
    scorecards = [
        _candidate_scorecard(candidate_id, outcome_rows, objective_rows, policy)
        for candidate_id in candidate_ids
    ]
    state_rows = [_state_recommendation_row(row) for row in scorecards]
    horizon_rows = [
        _horizon_coverage_row(horizon, outcome_rows, policy) for horizon in horizon_ids
    ]
    status = _family_status(scorecards, objective_rows, policy)
    summary = _summary_payload(
        status=status,
        generated_at=generated_at,
        generator_dir=generator_dir,
        policy_path=policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        candidates=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        start_date=resolved_start,
        end_date=resolved_end,
        source_summary=source_summary,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        actual_rows=actual_rows,
        outcome_rows=outcome_rows,
        objective_rows=objective_rows,
        scorecards=scorecards,
        horizon_rows=horizon_rows,
    )
    common = _common_payload(summary=summary, generated_at=generated_at, mode=mode)
    paths = _write_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        actual_rows=actual_rows,
        outcome_rows=outcome_rows,
        scorecards=scorecards,
        objective_rows=objective_rows,
        state_rows=state_rows,
        horizon_rows=horizon_rows,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "candidate_scorecards": scorecards,
            "objective_rows": objective_rows,
            "state_recommendations": state_rows,
            "horizon_coverage": horizon_rows,
        }
    )


def _load_candidate_signal_frames(
    *,
    generator_dir: Path,
    candidates: Sequence[str],
    horizons: Sequence[str],
    target_assets: Sequence[str],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    validator = CandidateSignalBindingValidator()
    for candidate_id in candidates:
        candidate_dir = generator_dir / candidate_id
        spec_path = candidate_dir / "candidate_signal_spec.json"
        series_path = candidate_dir / "candidate_signal_series.csv"
        prediction_path = candidate_dir / "candidate_prediction_artifact.json"
        generation_summary_path = candidate_dir / "generation_summary.json"
        validation_summary_path = candidate_dir / "validation_summary.json"
        missing = [
            str(path)
            for path in (
                spec_path,
                series_path,
                prediction_path,
                generation_summary_path,
                validation_summary_path,
            )
            if not path.exists()
        ]
        if missing:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: missing TRADING-2312 artifact(s): {missing}"
            )
        spec = _read_json(spec_path)
        prediction = _read_json(prediction_path)
        generation_summary = _read_json(generation_summary_path)
        validation_summary = _read_json(validation_summary_path)
        if spec.get("candidate_id") != candidate_id:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: signal spec candidate_id mismatch"
            )
        if validation_summary.get("status") != "PASS":
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: TRADING-2312 validation_summary is not PASS"
            )
        if generation_summary.get("validation_status") != "PASS":
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: TRADING-2312 generation_summary validation_status is not PASS"
            )
        spec_result = validator.validate_candidate_signal_spec(spec)
        if spec_result.errors:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: invalid candidate_signal_spec: {spec_result.errors}"
            )
        prediction_result = validator.validate_candidate_bound_prediction_artifact(
            prediction
        )
        if prediction_result.errors:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: invalid candidate_prediction_artifact: "
                f"{prediction_result.errors[:20]}"
            )
        rows = _read_signal_series(series_path)
        series_result = validator.validate_candidate_bound_signal_series(rows)
        if series_result.errors:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: invalid candidate_signal_series: "
                f"{series_result.errors[:20]}"
            )
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: empty signal series"
            )
        frame = frame.loc[
            frame["horizon"].astype(str).isin(set(horizons))
            & frame["target_asset"].astype(str).isin(set(target_assets))
        ].copy()
        frame["_source_date"] = pd.to_datetime(frame["source_date"], errors="coerce")
        frame = frame.loc[
            frame["_source_date"].dt.date.ge(start_date)
            & frame["_source_date"].dt.date.le(end_date)
        ].copy()
        if frame.empty:
            raise LiquidityRatesActualPathValidationError(
                f"{candidate_id}: no signal rows after date / asset / horizon filters"
            )
        frames[candidate_id] = frame
    return frames


def _actual_path(record: Mapping[str, Any], price_matrix: pd.DataFrame) -> dict[str, Any]:
    target_asset = str(record.get("target_asset") or "")
    horizon = str(record.get("horizon") or "")
    horizon_days = _parse_horizon_days(horizon)
    decision_ts = _timestamp(record.get("decision_timestamp"))
    if decision_ts is None:
        return _ineligible_actual_path("missing_decision_timestamp")
    decision_position = _first_position_on_or_after(
        price_matrix.index,
        pd.Timestamp(decision_ts.date()),
    )
    if decision_position is None:
        return _ineligible_actual_path("missing_decision_price")
    end_position = decision_position + horizon_days
    if end_position >= len(price_matrix.index):
        return _ineligible_actual_path("incomplete_future_window")
    decision_date = price_matrix.index[decision_position]
    horizon_end_date = price_matrix.index[end_position]
    qqq_path = _asset_path(price_matrix, "QQQ", decision_position, end_position)
    smh_path = _asset_path(price_matrix, "SMH", decision_position, end_position)
    tlt_path = _asset_path(price_matrix, "TLT", decision_position, end_position)
    shy_path = _asset_path(price_matrix, "SHY", decision_position, end_position)
    target_path = _asset_path(price_matrix, target_asset, decision_position, end_position)
    validation_eligible = all(
        path.get("validation_eligible")
        for path in (target_path, qqq_path, smh_path, tlt_path, shy_path)
    )
    status = "complete" if validation_eligible else "partial_price_coverage"
    qqq_forward = _optional_float(qqq_path.get("forward_return"))
    smh_forward = _optional_float(smh_path.get("forward_return"))
    target_forward = _optional_float(target_path.get("forward_return"))
    qqq_drawdown = _optional_float(qqq_path.get("max_drawdown"))
    smh_drawdown = _optional_float(smh_path.get("max_drawdown"))
    target_drawdown = _optional_float(target_path.get("max_drawdown"))
    qqq_smh_avg_forward = _average_values([qqq_forward, smh_forward])
    qqq_smh_worst_drawdown = _min_values([qqq_drawdown, smh_drawdown])
    return clean_for_yaml(
        {
            "actual_path_status": status,
            "validation_eligible": validation_eligible,
            "data_quality_warning": not validation_eligible,
            "decision_price_date": decision_date.date().isoformat(),
            "horizon_end_date": horizon_end_date.date().isoformat(),
            "target_forward_return": target_forward,
            "target_max_drawdown": target_drawdown,
            "qqq_forward_return": qqq_forward,
            "qqq_max_drawdown": qqq_drawdown,
            "smh_forward_return": smh_forward,
            "smh_max_drawdown": smh_drawdown,
            "qqq_smh_average_forward_return": qqq_smh_avg_forward,
            "qqq_smh_worst_drawdown": qqq_smh_worst_drawdown,
            "tlt_forward_return": tlt_path.get("forward_return"),
            "tlt_max_drawdown": tlt_path.get("max_drawdown"),
            "shy_forward_return": shy_path.get("forward_return"),
            "shy_max_drawdown": shy_path.get("max_drawdown"),
            "tlt_minus_shy_forward_return": _delta_or_none(
                tlt_path.get("forward_return"),
                shy_path.get("forward_return"),
            ),
            "data_coverage_ratio": round_float(
                min(
                    to_float(path.get("coverage_ratio"))
                    for path in (target_path, qqq_path, smh_path, tlt_path, shy_path)
                )
            ),
        }
    )


def _asset_path(
    price_matrix: pd.DataFrame,
    asset: str,
    start_position: int,
    end_position: int,
) -> dict[str, Any]:
    if asset not in price_matrix.columns:
        return {
            "validation_eligible": False,
            "coverage_ratio": 0.0,
            "forward_return": None,
            "max_drawdown": None,
        }
    window = price_matrix[asset].iloc[start_position : end_position + 1]
    observed = window.dropna()
    coverage_ratio = len(observed) / len(window) if len(window) else 0.0
    start_price = _finite_price(price_matrix[asset].iloc[start_position])
    end_price = _finite_price(price_matrix[asset].iloc[end_position])
    eligible = coverage_ratio >= 1.0 and start_price is not None and end_price is not None
    if not eligible or start_price is None:
        return {
            "validation_eligible": False,
            "coverage_ratio": round_float(coverage_ratio),
            "forward_return": None,
            "max_drawdown": None,
        }
    path = observed.astype(float) / start_price - 1.0
    return {
        "validation_eligible": True,
        "coverage_ratio": round_float(coverage_ratio),
        "forward_return": round_float(end_price / start_price - 1.0),
        "max_drawdown": round_float(float(path.min())) if not path.empty else 0.0,
    }


def _prediction_outcome_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    objective_scores = {
        OBJECTIVE_VALUATION_PRESSURE: _score_valuation_pressure(record, actual_path, policy),
        OBJECTIVE_DURATION_DRAWDOWN: _score_duration_drawdown(record, actual_path, policy),
        OBJECTIVE_EXPOSURE_CAP: _score_exposure_cap(record, actual_path, policy),
    }
    applicable_scores = [score for score in objective_scores.values() if score is not None]
    combined = sum(applicable_scores) / len(applicable_scores) if applicable_scores else 0.0
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "candidate_id": record.get("candidate_id"),
            "target_asset": record.get("target_asset"),
            "horizon": record.get("horizon"),
            "source_date": record.get("source_date"),
            "decision_timestamp": record.get("decision_timestamp"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            "actual_path_status": actual_path.get("actual_path_status"),
            "validation_eligible": actual_path.get("validation_eligible"),
            "data_quality_warning": actual_path.get("data_quality_warning"),
            "target_forward_return": actual_path.get("target_forward_return"),
            "target_max_drawdown": actual_path.get("target_max_drawdown"),
            "qqq_forward_return": actual_path.get("qqq_forward_return"),
            "qqq_max_drawdown": actual_path.get("qqq_max_drawdown"),
            "smh_forward_return": actual_path.get("smh_forward_return"),
            "smh_max_drawdown": actual_path.get("smh_max_drawdown"),
            "qqq_smh_average_forward_return": actual_path.get(
                "qqq_smh_average_forward_return"
            ),
            "qqq_smh_worst_drawdown": actual_path.get("qqq_smh_worst_drawdown"),
            "tlt_forward_return": actual_path.get("tlt_forward_return"),
            "tlt_max_drawdown": actual_path.get("tlt_max_drawdown"),
            "shy_forward_return": actual_path.get("shy_forward_return"),
            "shy_max_drawdown": actual_path.get("shy_max_drawdown"),
            "tlt_minus_shy_forward_return": actual_path.get(
                "tlt_minus_shy_forward_return"
            ),
            "valuation_pressure_score": _score_value(
                objective_scores[OBJECTIVE_VALUATION_PRESSURE]
            ),
            "duration_drawdown_score": _score_value(
                objective_scores[OBJECTIVE_DURATION_DRAWDOWN]
            ),
            "exposure_cap_score": _score_value(
                objective_scores[OBJECTIVE_EXPOSURE_CAP]
            ),
            "combined_alignment_score": round_float(combined),
            **SAFETY_FIELDS,
        }
    )


def _actual_path_row(record: Mapping[str, Any], actual_path: Mapping[str, Any]) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "candidate_id": record.get("candidate_id"),
            "candidate_family": record.get("candidate_family"),
            "target_asset": record.get("target_asset"),
            "horizon": record.get("horizon"),
            "source_date": record.get("source_date"),
            "decision_timestamp": record.get("decision_timestamp"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            **dict(actual_path),
            **SAFETY_FIELDS,
        }
    )


def _score_valuation_pressure(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    pressure = _pressure_sign(record)
    if pressure == 0:
        return 0.0
    return_pressure = _policy_float(policy, "return_pressure_threshold")
    return_relief = _policy_float(policy, "return_relief_threshold")
    drawdown_pressure = _policy_float(policy, "drawdown_pressure_threshold")
    target_forward = _optional_float(actual_path.get("target_forward_return"))
    target_drawdown = _optional_float(actual_path.get("target_max_drawdown"))
    observed_pressure = (
        _lte(target_forward, return_pressure)
        or _lte(target_drawdown, drawdown_pressure)
    )
    observed_relief = _gte(target_forward, return_relief) and not _lte(
        target_drawdown,
        drawdown_pressure,
    )
    outcome = _pressure_outcome(observed_pressure, observed_relief)
    return _alignment_score(pressure, outcome)


def _score_duration_drawdown(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    pressure = _pressure_sign(record)
    if pressure == 0:
        return 0.0
    return_pressure = _policy_float(policy, "return_pressure_threshold")
    return_relief = _policy_float(policy, "return_relief_threshold")
    drawdown_pressure = _policy_float(policy, "drawdown_pressure_threshold")
    tlt_forward = _optional_float(actual_path.get("tlt_forward_return"))
    tlt_drawdown = _optional_float(actual_path.get("tlt_max_drawdown"))
    observed_pressure = _lte(tlt_forward, return_pressure) or _lte(
        tlt_drawdown,
        drawdown_pressure,
    )
    observed_relief = _gte(tlt_forward, return_relief) and not _lte(
        tlt_drawdown,
        drawdown_pressure,
    )
    outcome = _pressure_outcome(observed_pressure, observed_relief)
    return _alignment_score(pressure, outcome)


def _score_exposure_cap(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    pressure = _pressure_sign(record)
    if pressure == 0:
        return 0.0
    false_risk_on = _policy_float(policy, "false_risk_on_drawdown_threshold")
    drawdown_pressure = _policy_float(policy, "drawdown_pressure_threshold")
    target_drawdown = _optional_float(actual_path.get("target_max_drawdown"))
    severe_drawdown = _lte(target_drawdown, false_risk_on)
    moderate_drawdown = _lte(target_drawdown, drawdown_pressure)
    if pressure < 0:
        if severe_drawdown:
            return -1.0
        return 1.0
    if severe_drawdown or moderate_drawdown:
        return 1.0
    return -0.25


def _objective_row(
    objective_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    score_field = {
        OBJECTIVE_VALUATION_PRESSURE: "valuation_pressure_score",
        OBJECTIVE_DURATION_DRAWDOWN: "duration_drawdown_score",
        OBJECTIVE_EXPOSURE_CAP: "exposure_cap_score",
    }[objective_id]
    rows = [
        row
        for row in outcome_rows
        if bool(row.get("validation_eligible")) and row.get(score_field) not in (None, "")
    ]
    scores = [to_float(row.get(score_field)) for row in rows]
    average_score = sum(scores) / len(scores) if scores else 0.0
    eligible_count = len(rows)
    min_records = _policy_int(policy, "minimum_objective_records")
    pass_threshold = _policy_float(policy, "objective_pass_alignment_score")
    if eligible_count < min_records:
        status = "INSUFFICIENT_SAMPLE"
    elif average_score >= pass_threshold:
        status = "PASS"
    elif average_score <= -pass_threshold:
        status = "FAIL"
    else:
        status = "INCONCLUSIVE_OR_WEAK"
    return clean_for_yaml(
        {
            "objective_id": objective_id,
            "score_field": score_field,
            "eligible_record_count": eligible_count,
            "minimum_required_records": min_records,
            "average_alignment_score": round_float(average_score),
            "positive_alignment_count": sum(1 for score in scores if score > 0.0),
            "negative_alignment_count": sum(1 for score in scores if score < 0.0),
            "neutral_alignment_count": sum(1 for score in scores if score == 0.0),
            "objective_status": status,
            **SAFETY_FIELDS,
        }
    )


def _candidate_scorecard(
    candidate_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        row
        for row in outcome_rows
        if row.get("candidate_id") == candidate_id and bool(row.get("validation_eligible"))
    ]
    average_alignment = _average(rows, "combined_alignment_score")
    eligible_count = len(rows)
    min_records = _policy_int(policy, "minimum_candidate_records")
    continue_threshold = _policy_float(policy, "candidate_continue_alignment_score")
    reject_threshold = _policy_float(policy, "candidate_reject_alignment_score")
    if eligible_count < min_records:
        status = STATUS_INCONCLUSIVE
        reason = "INSUFFICIENT_SAMPLE"
    elif average_alignment is not None and average_alignment >= continue_threshold:
        status = STATUS_CONTINUE_RESEARCH
        reason = "POSITIVE_ACTUAL_PATH_ALIGNMENT"
    elif average_alignment is not None and average_alignment <= reject_threshold:
        status = STATUS_REJECT_RECOMMENDED
        reason = "NEGATIVE_ACTUAL_PATH_ALIGNMENT"
    else:
        status = STATUS_INCONCLUSIVE
        reason = "WEAK_OR_MIXED_ALIGNMENT"
    signal_counts = Counter(str(row.get("signal_direction") or "") for row in rows)
    horizon_counts = Counter(str(row.get("horizon") or "") for row in rows)
    return clean_for_yaml(
        {
            "candidate_id": candidate_id,
            "validation_eligible_record_count": eligible_count,
            "minimum_required_records": min_records,
            "average_alignment_score": average_alignment,
            "positive_alignment_count": sum(
                1 for row in rows if to_float(row.get("combined_alignment_score")) > 0.0
            ),
            "negative_alignment_count": sum(
                1 for row in rows if to_float(row.get("combined_alignment_score")) < 0.0
            ),
            "signal_direction_counts": dict(signal_counts),
            "horizon_counts": dict(horizon_counts),
            "candidate_validation_status": status,
            "state_recommendation": status,
            "state_recommendation_reason": reason,
            "objective_statuses": {
                str(row.get("objective_id")): row.get("objective_status")
                for row in objective_rows
            },
            **SAFETY_FIELDS,
        }
    )


def _horizon_coverage_row(
    horizon: str,
    outcome_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        row
        for row in outcome_rows
        if row.get("horizon") == horizon and bool(row.get("validation_eligible"))
    ]
    avg_alignment = _average(rows, "combined_alignment_score")
    min_records = _policy_int(policy, "minimum_objective_records")
    status = "PASS" if len(rows) >= min_records else "INSUFFICIENT_SAMPLE"
    return clean_for_yaml(
        {
            "horizon": horizon,
            "eligible_record_count": len(rows),
            "minimum_required_records": min_records,
            "average_alignment_score": avg_alignment,
            "horizon_status": status,
            **SAFETY_FIELDS,
        }
    )


def _state_recommendation_row(scorecard: Mapping[str, Any]) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "candidate_id": scorecard.get("candidate_id"),
            "state_recommendation": scorecard.get("state_recommendation"),
            "state_recommendation_reason": scorecard.get("state_recommendation_reason"),
            "scope_review_ready": False,
            "next_required_task": "TRADING-2314_LIQUIDITY_RATES_SCOPE_REVIEW",
            **SAFETY_FIELDS,
        }
    )


def _family_status(
    scorecards: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> str:
    objectives_passed = sum(
        1 for row in objective_rows if row.get("objective_status") == "PASS"
    )
    candidates_continue = sum(
        1
        for row in scorecards
        if row.get("candidate_validation_status") == STATUS_CONTINUE_RESEARCH
    )
    candidates_rejected = sum(
        1
        for row in scorecards
        if row.get("candidate_validation_status") == STATUS_REJECT_RECOMMENDED
    )
    if (
        objectives_passed >= _policy_int(policy, "continue_research_min_objectives_passed")
        and candidates_continue > 0
    ):
        return STATUS_CONTINUE_RESEARCH
    if candidates_rejected >= _policy_int(policy, "reject_recommended_min_candidates_rejected"):
        return STATUS_REJECT_RECOMMENDED
    return STATUS_INCONCLUSIVE


def _summary_payload(
    *,
    status: str,
    generated_at: datetime,
    generator_dir: Path,
    policy_path: Path,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    candidates: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    start_date: date,
    end_date: date,
    source_summary: Mapping[str, Any],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    actual_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    eligible_count = sum(1 for row in actual_rows if row.get("validation_eligible"))
    recommendation_counts = Counter(
        str(row.get("candidate_validation_status")) for row in scorecards
    )
    objective_counts = Counter(str(row.get("objective_status")) for row in objective_rows)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "Liquidity / Rates Actual-Path Validation",
            "task_id": TASK_ID,
            "source_task_id": SOURCE_TASK_ID,
            "status": status,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": f"{start_date.isoformat()}..{end_date.isoformat()}",
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "source_generator_dir": str(generator_dir),
            "source_generator_status": source_summary.get("status"),
            "source_generator_data_quality_status": source_summary.get("data_quality_status"),
            "source_generated_date_range": source_summary.get("generated_source_date_range"),
            "policy_path": str(policy_path),
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "candidate_ids": list(candidates),
            "blocked_candidate_ids": list(BLOCKED_CANDIDATES),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "actual_path_record_count": len(actual_rows),
            "prediction_outcome_record_count": len(outcome_rows),
            "validation_eligible_record_count": eligible_count,
            "validation_ineligible_record_count": len(actual_rows) - eligible_count,
            "candidate_recommendation_counts": dict(recommendation_counts),
            "objective_status_counts": dict(objective_counts),
            "horizon_rows": list(horizon_rows),
            "data_quality": _data_quality_payload(quality_report, quality_report_path),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "source_data_quality_status": quality_report.status,
            "source_gap_exclusion": {
                "liquidity_headwind_proxy_v1": (
                    "blocked by TRADING-2311 / TRADING-2312 source gap; no "
                    "validation rows generated"
                )
            },
            "allowed_statuses": sorted(ALLOWED_STATUSES),
            "next_task": "TRADING-2314_LIQUIDITY_RATES_SCOPE_REVIEW",
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    summary: Mapping[str, Any],
    generated_at: datetime,
    mode: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Liquidity / Rates Actual-Path Validation",
        "task_id": TASK_ID,
        "status": summary["status"],
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        **SAFETY_FIELDS,
    }


def _write_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    actual_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "liquidity_rates_actual_path_validation_summary.json",
        "actual_path_matrix_json": output_dir
        / "liquidity_rates_actual_path_matrix.json",
        "actual_path_matrix_csv": output_dir
        / "liquidity_rates_actual_path_matrix.csv",
        "prediction_outcome_matrix_json": output_dir
        / "liquidity_rates_prediction_outcome_matrix.json",
        "prediction_outcome_matrix_csv": output_dir
        / "liquidity_rates_prediction_outcome_matrix.csv",
        "candidate_scorecard": output_dir / "liquidity_rates_candidate_scorecard.json",
        "objective_coverage": output_dir
        / "liquidity_rates_objective_coverage_matrix.json",
        "state_recommendation": output_dir
        / "liquidity_rates_state_recommendation_matrix.json",
        "horizon_coverage": output_dir / "liquidity_rates_horizon_coverage_matrix.json",
        "safety_boundary": output_dir
        / "liquidity_rates_actual_path_safety_boundary.json",
        "report_doc": docs_root / "liquidity_rates_actual_path_validation.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["actual_path_matrix_json"], {**dict(common), "rows": list(actual_rows)})
    write_csv_rows(paths["actual_path_matrix_csv"], actual_rows)
    write_json(
        paths["prediction_outcome_matrix_json"],
        {**dict(common), "rows": list(outcome_rows)},
    )
    write_csv_rows(paths["prediction_outcome_matrix_csv"], outcome_rows)
    write_json(paths["candidate_scorecard"], {**dict(common), "candidate_scorecards": scorecards})
    write_json(paths["objective_coverage"], {**dict(common), "objective_rows": objective_rows})
    write_json(
        paths["state_recommendation"],
        {**dict(common), "candidate_rows": state_rows},
    )
    write_json(paths["horizon_coverage"], {**dict(common), "horizon_rows": horizon_rows})
    write_json(paths["safety_boundary"], _safety_boundary(summary))
    write_markdown(
        paths["report_doc"],
        _render_report(summary, scorecards, objective_rows, horizon_rows),
    )
    return {key: str(path) for key, path in paths.items()}


def _safety_boundary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": summary["status"],
        "data_quality_status": summary["data_quality_status"],
        "data_quality_report_path": summary["data_quality_report_path"],
        "does_not_modify_generator_artifacts": True,
        "does_not_run_scope_review": True,
        "does_not_validate_liquidity_headwind_proxy": True,
        "full_liquidity_pressure_validation_ready": False,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        "next_required_task": "TRADING-2314_LIQUIDITY_RATES_SCOPE_REVIEW",
        **SAFETY_FIELDS,
    }


def _render_report(
    summary: Mapping[str, Any],
    scorecards: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
) -> str:
    candidate_lines = [
        "|candidate_id|eligible|avg_alignment|status|",
        "|---|---:|---:|---|",
    ]
    for row in scorecards:
        candidate_lines.append(
            "|`{}`|{}|{}|`{}`|".format(
                row.get("candidate_id"),
                row.get("validation_eligible_record_count"),
                row.get("average_alignment_score"),
                row.get("candidate_validation_status"),
            )
        )
    objective_lines = [
        "|objective_id|eligible|avg_alignment|status|",
        "|---|---:|---:|---|",
    ]
    for row in objective_rows:
        objective_lines.append(
            "|`{}`|{}|{}|`{}`|".format(
                row.get("objective_id"),
                row.get("eligible_record_count"),
                row.get("average_alignment_score"),
                row.get("objective_status"),
            )
        )
    horizon_lines = [
        "|horizon|eligible|avg_alignment|status|",
        "|---|---:|---:|---|",
    ]
    for row in horizon_rows:
        horizon_lines.append(
            "|`{}`|{}|{}|`{}`|".format(
                row.get("horizon"),
                row.get("eligible_record_count"),
                row.get("average_alignment_score"),
                row.get("horizon_status"),
            )
        )
    return "\n".join(
        [
            "# Liquidity / Rates Actual-Path Validation",
            "",
            "TRADING-2313 对 TRADING-2312 partial rates-only candidate-bound artifacts "
            "执行 research-only actual-path validation。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_report_path: `{summary['data_quality_report_path']}`",
            "- partial_rates_only_validation: `True`",
            "- liquidity_headwind_validation_executed: `False`",
            "- full_liquidity_pressure_validation_ready: `False`",
            "- scope_review_ready: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "- dynamic_promotion_status: `BLOCKED`",
            "",
            "## Candidate Scorecards",
            "",
            *candidate_lines,
            "",
            "## Objective Coverage",
            "",
            *objective_lines,
            "",
            "## Horizon Coverage",
            "",
            *horizon_lines,
            "",
            "## Safety",
            "",
            "`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 没有 "
            "TRADING-2312 signal series，本报告不得为该 route 生成 validation rows。"
            "本报告不修改 generator artifacts，不执行 scope review，不允许 promotion、"
            "paper-shadow、production 或 broker action。",
            "",
        ]
    )


def _validate_generator_source(generator_dir: Path, candidates: Sequence[str]) -> dict[str, Any]:
    summary_path = generator_dir / "liquidity_rates_pressure_generator_poc_summary.json"
    blocked_path = generator_dir / "blocked_liquidity_rates_candidate_report.json"
    if not summary_path.exists():
        raise LiquidityRatesActualPathValidationError(
            f"TRADING-2312 generator summary missing: {summary_path}"
        )
    if not blocked_path.exists():
        raise LiquidityRatesActualPathValidationError(
            f"TRADING-2312 blocked candidate report missing: {blocked_path}"
        )
    summary = _read_json(summary_path)
    blocked = _read_json(blocked_path)
    if summary.get("status") != SOURCE_GENERATOR_STATUS:
        raise LiquidityRatesActualPathValidationError(
            "TRADING-2312 generator summary is not ready for actual-path validation"
        )
    expected_false_fields = (
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "actual_path_validation_executed",
        "liquidity_headwind_generator_implemented",
        "full_liquidity_pressure_poc_ready",
    )
    for field in expected_false_fields:
        if summary.get(field) is not False:
            raise LiquidityRatesActualPathValidationError(
                f"TRADING-2312 source safety field {field} is not false"
            )
    if summary.get("partial_rates_only_generator_poc") is not True:
        raise LiquidityRatesActualPathValidationError(
            "TRADING-2312 source is not marked partial_rates_only_generator_poc"
        )
    requested_blocked = sorted(set(candidates) & set(BLOCKED_CANDIDATES))
    if requested_blocked:
        raise LiquidityRatesActualPathValidationError(
            "blocked by TRADING-2311 / TRADING-2312 source gap; no validation "
            f"workaround is allowed for candidates: {requested_blocked}"
        )
    missing = sorted(set(candidates) - set(_strings(summary.get("candidates"))))
    if missing:
        raise LiquidityRatesActualPathValidationError(
            f"TRADING-2312 summary missing requested candidates: {missing}"
        )
    if "liquidity_headwind_proxy_v1" not in _strings(blocked.get("blocked_candidates")):
        raise LiquidityRatesActualPathValidationError(
            "TRADING-2312 blocked report does not preserve liquidity_headwind source gap"
        )
    return summary


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


def _data_quality_payload(report: DataQualityReport, report_path: Path) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "report_path": str(report_path),
    }


def _load_price_matrix(path: Path, assets: Sequence[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    missing = {"date", "ticker", "adj_close"} - set(frame.columns)
    if missing:
        raise LiquidityRatesActualPathValidationError(
            f"price cache missing required columns: {sorted(missing)}"
        )
    frame = frame.loc[frame["ticker"].astype(str).isin(set(assets))].copy()
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    pivot = frame.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last")
    pivot = pivot.sort_index()
    for asset in assets:
        if asset not in pivot.columns:
            pivot[asset] = pd.NA
    return pivot.reindex(columns=list(assets))


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LiquidityRatesActualPathValidationError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise LiquidityRatesActualPathValidationError(f"policy file must be object: {path}")
    return payload


def _validate_policy(policy: Mapping[str, Any]) -> None:
    required = (
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
        "source_boundary",
        "validation_thresholds",
        "status_rule",
        "safety",
    )
    missing = [field for field in required if not policy.get(field)]
    if missing:
        raise LiquidityRatesActualPathValidationError(f"policy missing fields: {missing}")
    allowed = set(_strings(mapping(policy.get("status_rule")).get("allowed_statuses")))
    if allowed != ALLOWED_STATUSES:
        raise LiquidityRatesActualPathValidationError(
            f"policy allowed_statuses must match {sorted(ALLOWED_STATUSES)}"
        )
    safety = mapping(policy.get("safety"))
    for field, expected in SAFETY_FIELDS.items():
        if field == "promotion_eligible":
            continue
        if safety.get(field) != expected:
            raise LiquidityRatesActualPathValidationError(
                f"policy safety.{field} must be {expected}"
            )
    for key in (
        "minimum_candidate_records",
        "minimum_objective_records",
        "return_pressure_threshold",
        "return_relief_threshold",
        "drawdown_pressure_threshold",
        "false_risk_on_drawdown_threshold",
        "objective_pass_alignment_score",
        "candidate_continue_alignment_score",
        "candidate_reject_alignment_score",
        "continue_research_min_objectives_passed",
        "reject_recommended_min_candidates_rejected",
    ):
        _policy_float(policy, key)


def _required_price_symbols(policy: Mapping[str, Any], target_assets: Sequence[str]) -> list[str]:
    data_quality = mapping(policy.get("data_quality"))
    symbols = _strings(data_quality.get("required_price_symbols"))
    for asset in target_assets:
        if asset not in symbols:
            symbols.append(asset)
    return symbols


def _policy_float(policy: Mapping[str, Any], key: str) -> float:
    threshold = mapping(mapping(policy.get("validation_thresholds")).get(key))
    if "value" not in threshold:
        raise LiquidityRatesActualPathValidationError(f"missing policy threshold: {key}")
    value = threshold.get("value")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise LiquidityRatesActualPathValidationError(
            f"invalid policy threshold: {key}"
        ) from exc
    if not math.isfinite(parsed):
        raise LiquidityRatesActualPathValidationError(f"invalid policy threshold: {key}")
    return parsed


def _policy_int(policy: Mapping[str, Any], key: str) -> int:
    value = int(_policy_float(policy, key))
    if value <= 0:
        raise LiquidityRatesActualPathValidationError(
            f"policy threshold must be positive: {key}"
        )
    return value


def _read_signal_series(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    for row in rows:
        for key in ("provenance", "source_prediction_flags"):
            if isinstance(row.get(key), str) and str(row[key]).strip().startswith("{"):
                row[key] = json.loads(str(row[key]))
        for key in (
            "promotion_eligible",
            "promotion_allowed",
            "paper_shadow_allowed",
            "production_allowed",
            "permanently_inconclusive_override_allowed",
        ):
            row[key] = _bool_value(row.get(key))
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise LiquidityRatesActualPathValidationError(f"{path}: expected JSON object")
    return payload


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _first_position_on_or_after(index: pd.DatetimeIndex, ts: pd.Timestamp) -> int | None:
    position = int(index.searchsorted(ts, side="left"))
    if position >= len(index):
        return None
    return position


def _latest_common_price_date(prices_path: Path, assets: Sequence[str]) -> date:
    matrix = _load_price_matrix(prices_path, assets)
    clean = matrix.dropna(how="any")
    if clean.empty:
        raise LiquidityRatesActualPathValidationError(
            f"no common price date for assets: {list(assets)}"
        )
    return pd.Timestamp(clean.index.max()).date()


def _resolve_date(value: str | date | None, *, default: date) -> date:
    if value is None or value == "":
        return default
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise LiquidityRatesActualPathValidationError(
            f"date must use YYYY-MM-DD: {value}"
        ) from exc


def _date_from_text(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _timestamp(value: object) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)


def _parse_candidates(value: str | Sequence[str]) -> tuple[str, ...]:
    candidates = _parse_list(value)
    blocked = sorted(set(candidates) & set(BLOCKED_CANDIDATES))
    if blocked:
        raise LiquidityRatesActualPathValidationError(
            "blocked by TRADING-2311 / TRADING-2312 source gap; no actual-path "
            f"validation workaround is allowed for candidates: {blocked}"
        )
    unsupported = sorted(set(candidates) - set(DEFAULT_CANDIDATES))
    if unsupported:
        raise LiquidityRatesActualPathValidationError(
            f"unsupported TRADING-2313 candidates: {unsupported}"
        )
    return candidates


def _parse_list(
    value: str | Sequence[str],
    *,
    uppercase: bool = False,
) -> tuple[str, ...]:
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    else:
        raw = [str(item).strip() for item in value]
    parsed = [item.upper() if uppercase else item for item in raw if item]
    if not parsed:
        raise LiquidityRatesActualPathValidationError("list option must be non-empty")
    return tuple(parsed)


def _parse_horizon_days(value: str) -> int:
    text = str(value).strip().lower()
    if text.endswith("d"):
        return int(text[:-1])
    if text.endswith("m"):
        return int(text[:-1]) * 30
    raise LiquidityRatesActualPathValidationError(f"unsupported horizon: {value}")


def _strings(value: Any) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _bool_value(value: object) -> bool | object:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return value


def _record_id(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("candidate_id") or ""),
            str(record.get("target_asset") or ""),
            str(record.get("horizon") or ""),
            str(record.get("source_date") or ""),
            str(record.get("signal_name") or ""),
        ]
    )


def _ineligible_actual_path(reason: str) -> dict[str, Any]:
    return {
        "actual_path_status": reason,
        "validation_eligible": False,
        "data_quality_warning": True,
        "decision_price_date": "",
        "horizon_end_date": "",
        "target_forward_return": None,
        "target_max_drawdown": None,
        "qqq_forward_return": None,
        "qqq_max_drawdown": None,
        "smh_forward_return": None,
        "smh_max_drawdown": None,
        "qqq_smh_average_forward_return": None,
        "qqq_smh_worst_drawdown": None,
        "tlt_forward_return": None,
        "tlt_max_drawdown": None,
        "shy_forward_return": None,
        "shy_max_drawdown": None,
        "tlt_minus_shy_forward_return": None,
        "data_coverage_ratio": 0.0,
    }


def _pressure_sign(record: Mapping[str, Any]) -> int:
    direction = str(record.get("signal_direction") or "").strip().lower()
    if direction == "risk_off":
        return 1
    if direction == "risk_on":
        return -1
    value = _optional_float(record.get("signal_value"))
    if value is None:
        return 0
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _pressure_outcome(observed_pressure: bool, observed_relief: bool) -> int:
    if observed_pressure and not observed_relief:
        return 1
    if observed_relief and not observed_pressure:
        return -1
    return 0


def _alignment_score(pressure: int, outcome: int) -> float:
    if pressure == 0 or outcome == 0:
        return 0.0
    return 1.0 if pressure == outcome else -1.0


def _eligible(actual_path: Mapping[str, Any]) -> bool:
    return bool(actual_path.get("validation_eligible"))


def _score_value(value: float | None) -> float | None:
    return round_float(value) if value is not None else None


def _average(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    values = [_optional_float(row.get(field)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round_float(sum(clean) / len(clean))


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _finite_price(value: object) -> float | None:
    number = _optional_float(value)
    if number is None or number <= 0.0:
        return None
    return number


def _average_values(values: Sequence[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round_float(sum(clean) / len(clean))


def _min_values(values: Sequence[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round_float(min(clean))


def _delta_or_none(left: object, right: object) -> float | None:
    left_value = _optional_float(left)
    right_value = _optional_float(right)
    if left_value is None or right_value is None:
        return None
    return round_float(left_value - right_value)


def _lte(left: float | None, right: float) -> bool:
    return left is not None and left <= right


def _gte(left: float | None, right: float) -> bool:
    return left is not None and left >= right


__all__ = [
    "ALLOWED_STATUSES",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_GENERATOR_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "STATUS_CONTINUE_RESEARCH",
    "STATUS_INCONCLUSIVE",
    "STATUS_REJECT_RECOMMENDED",
    "LiquidityRatesActualPathValidationError",
    "run_liquidity_rates_actual_path_validation",
]
