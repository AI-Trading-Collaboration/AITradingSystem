from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_signal_generator import (
    REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
    candidate_artifact_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    clean_for_yaml,
    round_float,
    to_float,
    validate_cached_market_data,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.regenerated_candidate_generator_common import (
    parse_csv_list,
    parse_horizon_days,
)

DEFAULT_INPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_candidate_generators_regenerated"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "regenerated_candidate_actual_path_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2285_REGENERATED_CANDIDATE_ACTUAL_PATH_VALIDATION"
STATUS = "REGENERATED_CANDIDATE_ACTUAL_PATH_EVIDENCE_READY_PROMOTION_BLOCKED"
MODE = "actual_path_validation"
ARTIFACT_ROLE = "regenerated_candidate_actual_path_validation"
DEFAULT_CANDIDATES = (
    "baseline_plus_trend_structure",
    "risk_appetite",
    "volatility_regime",
)

# Research-only pilot thresholds for TRADING-2285 evidence classification.
# They seed TRADING-2286 review and are not promotion, paper-shadow, or production gates.
MINIMUM_DATA_COVERAGE_RATIO = 0.8
MIN_ELIGIBLE_RECORDS_FOR_RECOMMENDATION = 30
POSITIVE_RETURN_THRESHOLD = 0.005
NEGATIVE_RETURN_THRESHOLD = -0.005
STRONG_POSITIVE_RETURN_THRESHOLD = 0.02
NEUTRAL_RETURN_BAND = 0.01
MAX_ALLOWED_DRAWDOWN_THRESHOLD = -0.04
DRAWDOWN_STRESS_THRESHOLD = -0.03
MILD_DRAWDOWN_THRESHOLD = -0.02
DOWNSIDE_TAIL_RETURN_THRESHOLD = -0.04
DOWNSIDE_TAIL_DRAWDOWN_THRESHOLD = -0.05
UPSIDE_BREAKOUT_RETURN_THRESHOLD = 0.04
UPSIDE_BREAKOUT_RUNUP_THRESHOLD = 0.05
STRESS_REALIZED_VOLATILITY_THRESHOLD = 0.35
VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD = 0.25
VOLATILITY_COMPRESSION_ABSOLUTE_THRESHOLD = 0.18
HIGH_CONFIDENCE_NEUTRAL_THRESHOLD = 0.6
OWNER_REVIEW_ALIGNMENT_RATE_THRESHOLD = 0.58
OWNER_REVIEW_WEIGHTED_SCORE_THRESHOLD = 0.2
REJECT_ALIGNMENT_RATE_THRESHOLD = 0.35
REJECT_WEIGHTED_SCORE_THRESHOLD = -0.1
FALSE_RISK_COST_REVIEW_THRESHOLD = 0.08
ANNUALIZATION_TRADING_DAYS = 252.0


class RegeneratedCandidateActualPathValidationError(ValueError):
    pass


@dataclass(frozen=True)
class RegeneratedCandidateArtifacts:
    candidate_id: str
    candidate_dir: Path
    signal_spec: dict[str, Any]
    signal_series: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    artifact_paths: dict[str, Path]
    input_artifact_validation_status: str

    @property
    def prediction_records(self) -> list[dict[str, Any]]:
        records = self.prediction_artifact.get("prediction_records")
        return [dict(row) for row in records] if isinstance(records, list) else []


def run_regenerated_candidate_actual_path_validation(
    *,
    input_dir: Path = DEFAULT_INPUT_ROOT,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
    target_assets: Sequence[str] | str = ("QQQ", "SPY", "SMH"),
    horizons: Sequence[str] | str = ("5d", "10d", "20d"),
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise RegeneratedCandidateActualPathValidationError(
            "regenerated candidate actual-path validation only supports "
            "actual_path_validation mode"
        )
    candidate_ids = _normalize_list(candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=None,
        expected_price_tickers=asset_ids,
        expected_rate_series=(),
    )
    price_matrix = load_actual_price_matrix(prices_path, asset_ids)
    artifacts = load_regenerated_candidate_artifacts(input_dir, candidate_ids)

    actual_path_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    for artifact_bundle in artifacts.values():
        for record in artifact_bundle.prediction_records:
            if str(record.get("target_asset")) not in asset_ids:
                continue
            if str(record.get("horizon")) not in horizon_ids:
                continue
            actual_path = calculate_actual_path(record, price_matrix)
            actual_row = _actual_path_row(record, actual_path)
            alignment = classify_outcome_alignment(record, actual_path)
            outcome_row = _prediction_outcome_row(record, actual_path, alignment)
            actual_path_rows.append(actual_row)
            outcome_rows.append(outcome_row)
            error_rows.append(_error_attribution_row(outcome_row))

    scorecards = [
        build_candidate_scorecard(candidate_id, _candidate_rows(outcome_rows, candidate_id))
        for candidate_id in candidate_ids
    ]
    data_quality_rows = [
        build_candidate_data_quality_row(
            candidate_id,
            _candidate_rows(actual_path_rows, candidate_id),
            artifacts[candidate_id].input_artifact_validation_status,
        )
        for candidate_id in candidate_ids
    ]
    state_rows = [
        {
            "candidate_id": row["candidate_id"],
            "recommended_research_status": row["recommended_research_status"],
            "owner_review_required": row["owner_review_required"],
            **_safety_fields(),
            **trading_2281_boundary_fields(),
        }
        for row in scorecards
    ]
    summary = _summary(
        candidate_ids=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        generated_at=generated_at,
        actual_path_rows=actual_path_rows,
        outcome_rows=outcome_rows,
        scorecards=scorecards,
        data_quality_rows=data_quality_rows,
        data_quality=data_quality,
        input_dir=input_dir,
        prices_path=prices_path,
    )
    common = _common_payload(generated_at=generated_at, summary=summary, mode=mode)
    paths = _artifact_paths(output_dir=output_dir, docs_root=docs_root)

    write_json(paths["summary"], {**common, "summary": summary})
    write_json(paths["actual_path_matrix_json"], {**common, "rows": actual_path_rows})
    write_csv_rows(paths["actual_path_matrix_csv"], actual_path_rows)
    write_json(paths["prediction_outcome_matrix_json"], {**common, "rows": outcome_rows})
    write_csv_rows(paths["prediction_outcome_matrix_csv"], outcome_rows)
    write_json(paths["scorecard"], {**common, "candidate_scorecards": scorecards})
    write_json(paths["error_attribution_seed"], {**common, "error_rows": error_rows})
    write_json(paths["data_quality_report"], {**common, "candidate_rows": data_quality_rows})
    write_json(paths["state_recommendation_matrix"], {**common, "candidate_rows": state_rows})
    write_markdown(paths["validation_report_doc"], _render_validation_report(summary, scorecards))
    write_markdown(paths["prediction_outcome_doc"], _render_prediction_outcome_doc(summary))
    write_markdown(paths["error_attribution_doc"], _render_error_attribution_doc(summary))

    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "candidate_scorecards": scorecards,
            "candidate_data_quality": data_quality_rows,
            "candidate_state_recommendations": state_rows,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "input_artifact_count": len(artifacts),
            "actual_path_record_count": len(actual_path_rows),
            "prediction_outcome_record_count": len(outcome_rows),
        }
    )


def load_regenerated_candidate_artifacts(
    input_dir: Path,
    candidates: Sequence[str] | str,
) -> dict[str, RegeneratedCandidateArtifacts]:
    candidate_ids = _normalize_list(candidates)
    loaded: dict[str, RegeneratedCandidateArtifacts] = {}
    for candidate_id in candidate_ids:
        candidate_dir = input_dir / candidate_id
        paths = {
            "signal_spec": candidate_dir / "candidate_signal_spec.json",
            "signal_series": candidate_dir / "candidate_signal_series.csv",
            "prediction_artifact": candidate_dir / "candidate_prediction_artifact.json",
            "generation_summary": candidate_dir / "generation_summary.json",
            "validation_summary": candidate_dir / "validation_summary.json",
        }
        missing = [str(path) for path in paths.values() if not path.exists()]
        if missing:
            raise RegeneratedCandidateActualPathValidationError(
                f"{candidate_id}: missing regenerated artifact(s): {missing}"
            )
        signal_spec = _read_json(paths["signal_spec"])
        signal_series = _read_signal_series_csv(paths["signal_series"])
        prediction_artifact = _read_json(paths["prediction_artifact"])
        generation_summary = _read_json(paths["generation_summary"])
        validation_summary = _read_json(paths["validation_summary"])
        input_status = validate_regenerated_candidate_artifact_bundle(
            candidate_id=candidate_id,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            validation_summary=validation_summary,
        )
        loaded[candidate_id] = RegeneratedCandidateArtifacts(
            candidate_id=candidate_id,
            candidate_dir=candidate_dir,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            generation_summary=generation_summary,
            validation_summary=validation_summary,
            artifact_paths=paths,
            input_artifact_validation_status=input_status,
        )
    return loaded


def validate_regenerated_candidate_artifact_bundle(
    *,
    candidate_id: str,
    signal_spec: Mapping[str, Any],
    signal_series: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
) -> str:
    validator = CandidateSignalBindingValidator()
    results = [
        validator.validate_candidate_signal_spec(signal_spec),
        validator.validate_candidate_bound_signal_series(signal_series),
        validator.validate_candidate_bound_prediction_artifact(prediction_artifact),
    ]
    errors = [error for result in results for error in result.errors]
    if str(signal_spec.get("candidate_id")) != candidate_id:
        errors.append(f"{candidate_id}: signal spec candidate_id mismatch")
    if str(prediction_artifact.get("candidate_id")) != candidate_id:
        errors.append(f"{candidate_id}: prediction artifact candidate_id mismatch")
    if prediction_artifact.get("artifact_role") != REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE:
        errors.append(f"{candidate_id}: prediction artifact role is not regenerated executable")
    if str(validation_summary.get("status")) != "PASS":
        errors.append(f"{candidate_id}: prior validation_summary status is not PASS")
    errors.extend(_input_safety_errors(candidate_id, prediction_artifact))
    for index, record in enumerate(prediction_artifact.get("prediction_records", [])):
        if isinstance(record, Mapping):
            errors.extend(
                _input_safety_errors(
                    f"{candidate_id}.prediction_records[{index}]",
                    record,
                )
            )
        else:
            errors.append(f"{candidate_id}.prediction_records[{index}]: record is not object")
    if errors:
        raise RegeneratedCandidateActualPathValidationError("; ".join(errors))
    return "PASS"


def load_actual_price_matrix(path: Path, target_assets: Sequence[str] | str) -> pd.DataFrame:
    assets = _normalize_list(target_assets)
    frame = pd.read_csv(path, parse_dates=["date"])
    missing = {"date", "ticker"} - set(frame.columns)
    if missing:
        raise RegeneratedCandidateActualPathValidationError(
            f"price cache missing required columns: {sorted(missing)}"
        )
    price_column = _price_column(frame)
    frame = frame.loc[frame["ticker"].astype(str).isin(assets)].copy()
    frame[price_column] = pd.to_numeric(frame[price_column], errors="coerce")
    pivot = frame.pivot_table(index="date", columns="ticker", values=price_column, aggfunc="last")
    pivot = pivot.sort_index()
    for asset in assets:
        if asset not in pivot.columns:
            pivot[asset] = pd.NA
    return pivot.reindex(columns=assets)


def calculate_actual_path(
    record: Mapping[str, Any],
    price_matrix: pd.DataFrame,
    *,
    minimum_data_coverage_ratio: float = MINIMUM_DATA_COVERAGE_RATIO,
) -> dict[str, Any]:
    target_asset = str(record.get("target_asset") or "")
    horizon = str(record.get("horizon") or "")
    horizon_days = parse_horizon_days(horizon)
    decision_ts = _timestamp(record.get("decision_timestamp"))
    if target_asset not in price_matrix.columns:
        return _ineligible_actual_path("missing_target_asset")
    if decision_ts is None:
        return _ineligible_actual_path("missing_decision_timestamp")
    decision_date = pd.Timestamp(decision_ts.date())
    if decision_date not in price_matrix.index:
        return _ineligible_actual_path("missing_decision_price")
    position = price_matrix.index.get_loc(decision_date)
    if not isinstance(position, int):
        return _ineligible_actual_path("missing_decision_price")
    decision_price = _finite_price(price_matrix[target_asset].iloc[position])
    if decision_price is None:
        return _ineligible_actual_path("missing_decision_price")
    end_position = position + horizon_days
    if end_position >= len(price_matrix.index):
        return {
            **_ineligible_actual_path("incomplete_future_window"),
            "decision_price": round_float(decision_price),
            "data_coverage_ratio": round_float(
                max(0.0, (len(price_matrix.index) - position) / (horizon_days + 1))
            ),
        }
    window = price_matrix[target_asset].iloc[position : end_position + 1]
    observed = window.dropna()
    coverage_ratio = len(observed) / (horizon_days + 1)
    horizon_end_price = _finite_price(price_matrix[target_asset].iloc[end_position])
    actual_path_status = "complete"
    validation_eligible = True
    data_quality_warning = False
    if len(observed) < len(window):
        actual_path_status = "partial_price_coverage"
        data_quality_warning = True
    if coverage_ratio < minimum_data_coverage_ratio or horizon_end_price is None:
        validation_eligible = False
    if not validation_eligible and actual_path_status == "complete":
        actual_path_status = "partial_price_coverage"
        data_quality_warning = True
    path = observed.astype(float) / decision_price - 1.0
    returns = observed.astype(float).pct_change().dropna()
    forward_return = (
        horizon_end_price / decision_price - 1.0 if horizon_end_price is not None else None
    )
    max_drawdown = float(path.min()) if not path.empty else None
    max_runup = float(path.max()) if not path.empty else None
    realized_vol = (
        float(returns.std()) * math.sqrt(ANNUALIZATION_TRADING_DAYS)
        if len(returns) >= 2
        else 0.0
    )
    trailing_vol = _trailing_realized_volatility(price_matrix[target_asset], position)
    stress_event = (
        _lte(max_drawdown, DRAWDOWN_STRESS_THRESHOLD)
        or _lte(forward_return, DOWNSIDE_TAIL_RETURN_THRESHOLD)
        or realized_vol >= STRESS_REALIZED_VOLATILITY_THRESHOLD
    )
    return clean_for_yaml(
        {
            "actual_path_status": actual_path_status,
            "validation_eligible": validation_eligible,
            "data_quality_warning": data_quality_warning,
            "decision_price": round_float(decision_price),
            "horizon_end_price": _round_or_none(horizon_end_price),
            "forward_return": _round_or_none(forward_return),
            "max_drawdown_during_horizon": _round_or_none(max_drawdown),
            "max_runup_during_horizon": _round_or_none(max_runup),
            "realized_volatility": round_float(realized_vol),
            "rolling_volatility_baseline": _round_or_none(trailing_vol),
            "downside_tail_event": _lte(forward_return, DOWNSIDE_TAIL_RETURN_THRESHOLD)
            or _lte(max_drawdown, DOWNSIDE_TAIL_DRAWDOWN_THRESHOLD),
            "upside_breakout_event": _gte(forward_return, UPSIDE_BREAKOUT_RETURN_THRESHOLD)
            or _gte(max_runup, UPSIDE_BREAKOUT_RUNUP_THRESHOLD),
            "stress_event": stress_event,
            "data_coverage_ratio": round_float(coverage_ratio),
            "expected_price_observation_count": horizon_days + 1,
            "observed_price_observation_count": int(len(observed)),
        }
    )


def classify_outcome_alignment(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(actual_path.get("validation_eligible")):
        return _alignment("data_quality_inconclusive", 0.0, "data_quality_inconclusive")
    direction = str(record.get("signal_direction") or "neutral")
    confidence = to_float(record.get("signal_confidence"))
    forward_return = _optional_float(actual_path.get("forward_return"))
    max_drawdown = _optional_float(actual_path.get("max_drawdown_during_horizon"))
    realized_vol = to_float(actual_path.get("realized_volatility"))
    stress_event = bool(actual_path.get("stress_event"))
    if direction in {"risk_on", "trend_confirming"}:
        if _lte(forward_return, NEGATIVE_RETURN_THRESHOLD) or stress_event:
            return _alignment("false_risk_on", -1.0, "false_risk_on")
        if _gt(forward_return, POSITIVE_RETURN_THRESHOLD) and _gt(
            max_drawdown, MAX_ALLOWED_DRAWDOWN_THRESHOLD
        ):
            return _alignment("positive_alignment", 1.0, "no_error")
        if _gt(forward_return, 0.0) and not stress_event:
            return _alignment("partial_alignment", 0.5, "no_error")
        return _alignment("neutral_inconclusive", 0.0, "no_error")
    if direction in {"risk_off", "trend_weakening", "volatility_expansion"}:
        if _gt(forward_return, STRONG_POSITIVE_RETURN_THRESHOLD) and _gt(
            max_drawdown, MILD_DRAWDOWN_THRESHOLD
        ):
            return _alignment("false_risk_off", -1.0, "false_risk_off")
        if (
            _lte(max_drawdown, DRAWDOWN_STRESS_THRESHOLD)
            or _lte(forward_return, NEGATIVE_RETURN_THRESHOLD)
            or stress_event
            or (
                direction == "volatility_expansion"
                and realized_vol >= VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD
            )
        ):
            return _alignment("positive_alignment", 1.0, "no_error")
        if realized_vol >= VOLATILITY_COMPRESSION_ABSOLUTE_THRESHOLD:
            return _alignment("partial_alignment", 0.5, "no_error")
        return _alignment("neutral_inconclusive", 0.0, "no_error")
    if direction == "volatility_compression":
        baseline = _optional_float(actual_path.get("rolling_volatility_baseline"))
        volatility_ceiling = (
            baseline
            if baseline is not None and baseline > 0.0
            else VOLATILITY_COMPRESSION_ABSOLUTE_THRESHOLD
        )
        if realized_vol <= volatility_ceiling and not stress_event:
            return _alignment("positive_alignment", 1.0, "no_error")
        if stress_event or realized_vol >= STRESS_REALIZED_VOLATILITY_THRESHOLD:
            return _alignment("volatility_misclassification", -1.0, "volatility_misclassification")
        return _alignment("neutral_inconclusive", 0.0, "no_error")
    if abs(to_float(forward_return)) <= NEUTRAL_RETURN_BAND and not stress_event:
        return _alignment("positive_alignment", 1.0, "no_error")
    if (
        confidence >= HIGH_CONFIDENCE_NEUTRAL_THRESHOLD
        and (abs(to_float(forward_return)) > STRONG_POSITIVE_RETURN_THRESHOLD or stress_event)
    ):
        return _alignment("neutral_misclassification", -1.0, "neutral_misclassification")
    return _alignment("neutral_inconclusive", 0.0, "no_error")


def build_candidate_scorecard(
    candidate_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    record_count = len(outcome_rows)
    eligible = [row for row in outcome_rows if bool(row.get("validation_eligible"))]
    warning_count = sum(1 for row in outcome_rows if bool(row.get("data_quality_warning")))
    false_risk_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
    false_risk_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
    scores = [to_float(row.get("alignment_score")) for row in eligible]
    confidence_weights = [max(to_float(row.get("signal_confidence")), 0.0) for row in eligible]
    positive_count = sum(1 for row in eligible if to_float(row.get("alignment_score")) > 0.0)
    alignment_rate = positive_count / len(eligible) if eligible else 0.0
    weighted_alignment_score = sum(scores) / len(scores) if scores else 0.0
    confidence_weighted = (
        sum(score * weight for score, weight in zip(scores, confidence_weights, strict=True))
        / sum(confidence_weights)
        if confidence_weights and sum(confidence_weights) > 0.0
        else 0.0
    )
    avg_return_by_direction = _average_by(outcome_rows, "signal_direction", "actual_forward_return")
    avg_drawdown_by_direction = _average_by(outcome_rows, "signal_direction", "actual_max_drawdown")
    avg_vol_by_direction = _average_by(
        outcome_rows,
        "signal_direction",
        "actual_realized_volatility",
    )
    horizon_scores = _average_by(eligible, "horizon", "alignment_score")
    best_horizon = max(horizon_scores, key=horizon_scores.get) if horizon_scores else ""
    worst_horizon = min(horizon_scores, key=horizon_scores.get) if horizon_scores else ""
    false_risk_on_cost = _error_cost(false_risk_on)
    false_risk_off_cost = _error_cost(false_risk_off)
    recommended = _recommended_research_status(
        eligible_count=len(eligible),
        warning_count=warning_count,
        record_count=record_count,
        alignment_rate=alignment_rate,
        confidence_weighted_alignment_score=confidence_weighted,
        false_risk_on_cost=false_risk_on_cost,
        false_risk_off_cost=false_risk_off_cost,
        horizon_scores=horizon_scores,
    )
    owner_review_required = recommended == "ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"
    return clean_for_yaml(
        {
            "candidate_id": candidate_id,
            "record_count": record_count,
            "validation_eligible_record_count": len(eligible),
            "validation_ineligible_record_count": record_count - len(eligible),
            "data_quality_warning_count": warning_count,
            "alignment_rate": round_float(alignment_rate),
            "weighted_alignment_score": round_float(weighted_alignment_score),
            "confidence_weighted_alignment_score": round_float(confidence_weighted),
            "false_risk_on_count": len(false_risk_on),
            "false_risk_off_count": len(false_risk_off),
            "false_risk_on_cost": round_float(false_risk_on_cost),
            "false_risk_off_cost": round_float(false_risk_off_cost),
            "average_forward_return_by_direction": avg_return_by_direction,
            "average_max_drawdown_by_direction": avg_drawdown_by_direction,
            "average_realized_volatility_by_direction": avg_vol_by_direction,
            "best_horizon": best_horizon,
            "worst_horizon": worst_horizon,
            "regime_dependency_notes": (
                "ai_after_chatgpt actual-path evidence; no promotion interpretation"
            ),
            "recommended_research_status": recommended,
            "owner_review_required": owner_review_required,
            **_safety_fields(),
            **trading_2281_boundary_fields(),
        }
    )


def build_candidate_data_quality_row(
    candidate_id: str,
    actual_path_rows: Sequence[Mapping[str, Any]],
    input_artifact_validation_status: str,
) -> dict[str, Any]:
    status_counts = Counter(str(row.get("actual_path_status")) for row in actual_path_rows)
    missing_target_asset_count = status_counts["missing_target_asset"]
    warning_count = (
        status_counts["missing_decision_price"]
        + status_counts["incomplete_future_window"]
        + status_counts["partial_price_coverage"]
    )
    error_count = missing_target_asset_count
    data_quality_status = (
        "FAIL"
        if error_count > 0 or input_artifact_validation_status != "PASS"
        else "PASS_WITH_WARNINGS"
        if warning_count > 0
        else "PASS"
    )
    return {
        "candidate_id": candidate_id,
        "input_artifact_validation_status": input_artifact_validation_status,
        "price_data_coverage_status": data_quality_status,
        "missing_decision_price_count": status_counts["missing_decision_price"],
        "partial_future_window_count": status_counts["partial_price_coverage"],
        "incomplete_horizon_count": status_counts["incomplete_future_window"],
        "missing_target_asset_count": missing_target_asset_count,
        "data_quality_warning_count": warning_count,
        "data_quality_error_count": error_count,
        "data_quality_status": data_quality_status,
    }


def _actual_path_row(record: Mapping[str, Any], actual_path: Mapping[str, Any]) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "candidate_id": record.get("candidate_id"),
            "candidate_family": record.get("candidate_family"),
            "target_asset": record.get("target_asset"),
            "horizon": record.get("horizon"),
            "decision_timestamp": record.get("decision_timestamp"),
            "valid_from": record.get("valid_from"),
            "valid_until": record.get("valid_until"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            **dict(actual_path),
            **_safety_fields(),
        }
    )


def _prediction_outcome_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    alignment: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "candidate_id": record.get("candidate_id"),
            "target_asset": record.get("target_asset"),
            "decision_timestamp": record.get("decision_timestamp"),
            "horizon": record.get("horizon"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            "actual_path_status": actual_path.get("actual_path_status"),
            "validation_eligible": actual_path.get("validation_eligible"),
            "data_quality_warning": actual_path.get("data_quality_warning"),
            "actual_forward_return": actual_path.get("forward_return"),
            "actual_max_drawdown": actual_path.get("max_drawdown_during_horizon"),
            "actual_max_runup": actual_path.get("max_runup_during_horizon"),
            "actual_realized_volatility": actual_path.get("realized_volatility"),
            "alignment_label": alignment.get("alignment_label"),
            "alignment_score": alignment.get("alignment_score"),
            "error_type": alignment.get("error_type"),
            "dominant_observed_driver": _dominant_observed_driver(actual_path),
            **_safety_fields(),
        }
    )


def _error_attribution_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "candidate_id": row.get("candidate_id"),
            "record_id": row.get("record_id"),
            "target_asset": row.get("target_asset"),
            "decision_timestamp": row.get("decision_timestamp"),
            "horizon": row.get("horizon"),
            "signal_direction": row.get("signal_direction"),
            "signal_value": row.get("signal_value"),
            "signal_confidence": row.get("signal_confidence"),
            "actual_forward_return": row.get("actual_forward_return"),
            "actual_max_drawdown": row.get("actual_max_drawdown"),
            "actual_realized_volatility": row.get("actual_realized_volatility"),
            "alignment_label": row.get("alignment_label"),
            "error_type": row.get("error_type"),
            "dominant_observed_driver": row.get("dominant_observed_driver"),
            "owner_review_note": _owner_review_note(row),
            **_safety_fields(),
        }
    )


def _summary(
    *,
    candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    actual_path_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
    input_dir: Path,
    prices_path: Path,
) -> dict[str, Any]:
    eligible_count = sum(1 for row in actual_path_rows if bool(row.get("validation_eligible")))
    status_counts = Counter(str(row.get("actual_path_status")) for row in actual_path_rows)
    recommendation_counts = Counter(
        str(row.get("recommended_research_status")) for row in scorecards
    )
    output_data_quality_status = (
        "FAIL"
        if any(row.get("data_quality_status") == "FAIL" for row in data_quality_rows)
        else "PASS_WITH_WARNINGS"
        if any(row.get("data_quality_status") == "PASS_WITH_WARNINGS" for row in data_quality_rows)
        else "PASS"
    )
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "input_dir": str(input_dir),
            "prices_path": str(prices_path),
            "candidate_count": len(candidate_ids),
            "candidate_ids": list(candidate_ids),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "actual_path_record_count": len(actual_path_rows),
            "prediction_outcome_record_count": len(outcome_rows),
            "validation_eligible_record_count": eligible_count,
            "validation_ineligible_record_count": len(actual_path_rows) - eligible_count,
            "actual_path_status_counts": dict(status_counts),
            "candidate_recommendation_counts": dict(recommendation_counts),
            "data_quality_status": output_data_quality_status,
            "source_data_quality_status": data_quality.get("status"),
            "source_data_quality_error_count": data_quality.get("error_count"),
            "source_data_quality_warning_count": data_quality.get("warning_count"),
            "artifact_role": ARTIFACT_ROLE,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "next_task": (
                "TRADING-2286_Regenerated_Candidate_Risk_Attribution_And_"
                "Owner_Review_Package"
            ),
            **trading_2281_boundary_fields(),
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    summary: Mapping[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "schema_version": "regenerated_candidate_actual_path_validation.v1",
        "report_type": "regenerated_candidate_actual_path_validation",
        "artifact_role": ARTIFACT_ROLE,
        "title": "Regenerated Candidate Actual-Path Validation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        "summary_status": summary.get("status"),
        **_safety_fields(),
        "owner_review_required": False,
        **trading_2281_boundary_fields(),
    }


def _artifact_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "regenerated_candidate_actual_path_validation_summary.json",
        "actual_path_matrix_json": output_dir / "regenerated_candidate_actual_path_matrix.json",
        "actual_path_matrix_csv": output_dir / "regenerated_candidate_actual_path_matrix.csv",
        "prediction_outcome_matrix_json": output_dir / "candidate_prediction_outcome_matrix.json",
        "prediction_outcome_matrix_csv": output_dir / "candidate_prediction_outcome_matrix.csv",
        "scorecard": output_dir / "candidate_validation_scorecard.json",
        "error_attribution_seed": output_dir / "candidate_error_attribution_seed.json",
        "data_quality_report": output_dir / "candidate_data_quality_report.json",
        "state_recommendation_matrix": output_dir / "candidate_state_recommendation_matrix.json",
        "validation_report_doc": (
            docs_root / "regenerated_candidate_actual_path_validation_report.md"
        ),
        "prediction_outcome_doc": docs_root / "regenerated_candidate_prediction_outcome_summary.md",
        "error_attribution_doc": docs_root / "regenerated_candidate_error_attribution_seed.md",
    }


def _input_safety_errors(scope: str, payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    required_fields = (
        "candidate_id",
        "source_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "horizon",
        "provenance",
    )
    for field in required_fields:
        if payload.get(field) in (None, ""):
            errors.append(f"{scope}: missing {field}")
    if _bool(payload.get("promotion_allowed")) is not False:
        errors.append(f"{scope}: promotion_allowed must be false")
    if _bool(payload.get("paper_shadow_allowed")) is not False:
        errors.append(f"{scope}: paper_shadow_allowed must be false")
    if _bool(payload.get("production_allowed")) is not False:
        errors.append(f"{scope}: production_allowed must be false")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action=none required")
    return errors


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RegeneratedCandidateActualPathValidationError(f"{path}: expected JSON object")
    return payload


def _read_signal_series_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    for row in rows:
        for key in ("provenance", "source_prediction_flags"):
            if isinstance(row.get(key), str) and row[key].strip().startswith("{"):
                row[key] = json.loads(row[key])
    return rows


def _render_validation_report(
    summary: Mapping[str, Any],
    scorecards: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Regenerated Candidate Actual-Path Validation Report",
        "",
        "最后更新：2026-06-30",
        "",
        "## Summary",
        "",
        f"- status: `{summary.get('status')}`",
        f"- candidate_count: `{summary.get('candidate_count')}`",
        f"- actual_path_record_count: `{summary.get('actual_path_record_count')}`",
        f"- validation_eligible_record_count: `{summary.get('validation_eligible_record_count')}`",
        f"- data_quality_status: `{summary.get('data_quality_status')}`",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## Candidate Scorecards",
        "",
        "|candidate_id|eligible|alignment_rate|confidence_weighted|recommended_status|",
        "|---|---:|---:|---:|---|",
    ]
    for row in scorecards:
        lines.append(
            "|`{}`|{}|{}|{}|`{}`|".format(
                row.get("candidate_id"),
                row.get("validation_eligible_record_count"),
                row.get("alignment_rate"),
                row.get("confidence_weighted_alignment_score"),
                row.get("recommended_research_status"),
            )
        )
    lines.extend(
        [
            "",
            "本报告只生成 regenerated candidate actual-path evidence；不代表 owner "
            "approval、promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_prediction_outcome_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Regenerated Candidate Prediction / Outcome Summary",
            "",
            "最后更新：2026-06-30",
            "",
            "- prediction_outcome_record_count: "
            f"`{summary.get('prediction_outcome_record_count')}`",
            f"- actual_path_status_counts: `{summary.get('actual_path_status_counts')}`",
            "- candidate_recommendation_counts: "
            f"`{summary.get('candidate_recommendation_counts')}`",
            "- promotion_allowed: `false`",
            "- paper_shadow_allowed: `false`",
            "- production_allowed: `false`",
            "- broker_action: `none`",
            "",
        ]
    )


def _render_error_attribution_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Regenerated Candidate Error Attribution Seed",
            "",
            "最后更新：2026-06-30",
            "",
            "本文件说明 TRADING-2285 只生成 error attribution seed，完整 risk "
            "attribution 和 owner review package 由 TRADING-2286 承接。",
            "",
            f"- actual_path_record_count: `{summary.get('actual_path_record_count')}`",
            "- validation_eligible_record_count: "
            f"`{summary.get('validation_eligible_record_count')}`",
            "- promotion_allowed: `false`",
            "- paper_shadow_allowed: `false`",
            "- production_allowed: `false`",
            "- broker_action: `none`",
            "",
        ]
    )


def _candidate_rows(rows: Sequence[Mapping[str, Any]], candidate_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("candidate_id")) == candidate_id]


def _average_by(
    rows: Sequence[Mapping[str, Any]],
    group_key: str,
    value_key: str,
) -> dict[str, float]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = _optional_float(row.get(value_key))
        if value is not None:
            grouped[str(row.get(group_key))].append(value)
    return {
        key: round_float(sum(values) / len(values))
        for key, values in grouped.items()
        if values
    }


def _recommended_research_status(
    *,
    eligible_count: int,
    warning_count: int,
    record_count: int,
    alignment_rate: float,
    confidence_weighted_alignment_score: float,
    false_risk_on_cost: float,
    false_risk_off_cost: float,
    horizon_scores: Mapping[str, float],
) -> str:
    if record_count == 0:
        return "DATA_QUALITY_BLOCKED"
    if eligible_count < MIN_ELIGIBLE_RECORDS_FOR_RECOMMENDATION:
        return "ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
    if warning_count / max(record_count, 1) > (1.0 - MINIMUM_DATA_COVERAGE_RATIO):
        return "ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
    if (
        alignment_rate >= OWNER_REVIEW_ALIGNMENT_RATE_THRESHOLD
        and confidence_weighted_alignment_score >= OWNER_REVIEW_WEIGHTED_SCORE_THRESHOLD
        and false_risk_on_cost <= FALSE_RISK_COST_REVIEW_THRESHOLD
        and false_risk_off_cost <= FALSE_RISK_COST_REVIEW_THRESHOLD
        and len(horizon_scores) >= 2
    ):
        return "ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"
    if (
        alignment_rate <= REJECT_ALIGNMENT_RATE_THRESHOLD
        or confidence_weighted_alignment_score <= REJECT_WEIGHTED_SCORE_THRESHOLD
    ):
        return "ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    return "ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"


def _error_cost(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        abs(to_float(row.get("actual_forward_return")))
        + abs(to_float(row.get("actual_max_drawdown")))
        for row in rows
    )


def _dominant_observed_driver(actual_path: Mapping[str, Any]) -> str:
    if not bool(actual_path.get("validation_eligible")):
        return "data_gap"
    forward_return = _optional_float(actual_path.get("forward_return"))
    max_drawdown = _optional_float(actual_path.get("max_drawdown_during_horizon"))
    realized_vol = to_float(actual_path.get("realized_volatility"))
    if _gte(forward_return, UPSIDE_BREAKOUT_RETURN_THRESHOLD):
        return "sharp_rebound"
    if _lte(max_drawdown, DOWNSIDE_TAIL_DRAWDOWN_THRESHOLD):
        return "drawdown_event"
    if realized_vol >= VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD:
        return "volatility_expansion"
    if realized_vol <= VOLATILITY_COMPRESSION_ABSOLUTE_THRESHOLD:
        return "volatility_compression"
    if _gt(forward_return, POSITIVE_RETURN_THRESHOLD):
        return "trend_continuation"
    if _lt(forward_return, NEGATIVE_RETURN_THRESHOLD):
        return "trend_reversal"
    return "noisy_path"


def _owner_review_note(row: Mapping[str, Any]) -> str:
    error_type = str(row.get("error_type") or "")
    if error_type == "no_error":
        return "No initial error attribution required."
    if error_type == "data_quality_inconclusive":
        return "Actual-path evidence is ineligible or coverage-limited; review data coverage first."
    return f"Initial TRADING-2285 seed classification: {error_type}."


def _record_id(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("candidate_id")),
            str(record.get("target_asset")),
            str(record.get("horizon")),
            str(record.get("signal_name")),
            str(record.get("decision_timestamp")),
            str(record.get("source_row_index", "")),
        ]
    )


def _alignment(label: str, score: float, error_type: str) -> dict[str, Any]:
    return {
        "alignment_label": label,
        "alignment_score": score,
        "error_type": error_type,
    }


def _ineligible_actual_path(status: str) -> dict[str, Any]:
    return {
        "actual_path_status": status,
        "validation_eligible": False,
        "data_quality_warning": status != "missing_target_asset",
        "decision_price": None,
        "horizon_end_price": None,
        "forward_return": None,
        "max_drawdown_during_horizon": None,
        "max_runup_during_horizon": None,
        "realized_volatility": None,
        "rolling_volatility_baseline": None,
        "downside_tail_event": False,
        "upside_breakout_event": False,
        "stress_event": False,
        "data_coverage_ratio": 0.0,
        "expected_price_observation_count": None,
        "observed_price_observation_count": 0,
    }


def _trailing_realized_volatility(series: pd.Series, position: int) -> float | None:
    start = max(0, position - 20)
    window = series.iloc[start : position + 1].dropna()
    if len(window) < 5:
        return None
    returns = window.astype(float).pct_change().dropna()
    if len(returns) < 2:
        return None
    return float(returns.std()) * math.sqrt(ANNUALIZATION_TRADING_DAYS)


def _price_column(frame: pd.DataFrame) -> str:
    for column in ("adj_close", "adjusted_close", "close"):
        if column in frame.columns:
            return column
    raise RegeneratedCandidateActualPathValidationError(
        "price cache missing adj_close, adjusted_close, or close column"
    )


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _finite_price(value: Any) -> float | None:
    parsed = _optional_float(value)
    if parsed is None or parsed <= 0.0:
        return None
    return parsed


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _round_or_none(value: Any) -> float | None:
    parsed = _optional_float(value)
    return round_float(parsed) if parsed is not None else None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    if not parsed:
        raise RegeneratedCandidateActualPathValidationError(
            "comma-separated value must be non-empty"
        )
    return parsed


def _safety_fields() -> dict[str, Any]:
    return {
        **candidate_artifact_safety_fields(),
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
    }


def _lt(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed < right


def _lte(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed <= right


def _gt(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed > right


def _gte(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed >= right
