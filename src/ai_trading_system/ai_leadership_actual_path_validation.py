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

from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_CANDIDATES,
    FULL_UNIVERSE_BLOCKER,
)
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
from ai_trading_system.regenerated_candidate_generator_common import parse_horizon_days
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION"
REPORT_TYPE = "ai_leadership_actual_path_validation"
ARTIFACT_ROLE = "ai_leadership_actual_path_validation"
MODE = "actual_path_validation"
CANDIDATE_FAMILY = "ai_semiconductor_leadership"
SOURCE_GENERATOR_STATUS = "AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC_READY_VALIDATION_BLOCKED"

STATUS_CONTINUE_RESEARCH = "AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH"
STATUS_INCONCLUSIVE = "AI_LEADERSHIP_VALIDATED_INCONCLUSIVE"
STATUS_REJECT_RECOMMENDED = "AI_LEADERSHIP_REJECT_RECOMMENDED"
ALLOWED_STATUSES = {
    STATUS_CONTINUE_RESEARCH,
    STATUS_INCONCLUSIVE,
    STATUS_REJECT_RECOMMENDED,
}

DEFAULT_GENERATOR_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "ai_semiconductor_leadership_generator_poc"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "ai_leadership_actual_path_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "ai_leadership_actual_path_validation_policy.yaml"
)
DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

OBJECTIVE_SMH_RELATIVE_RETURN = "smh_future_relative_return"
OBJECTIVE_DRAWDOWN_RISK = "qqq_smh_drawdown_risk"
OBJECTIVE_WEAKENING_WINDOWS = "ai_leadership_weakening_windows"
OBJECTIVE_SMH_OVERWEIGHT_RISK = "smh_overweight_risk"
OBJECTIVES = (
    OBJECTIVE_SMH_RELATIVE_RETURN,
    OBJECTIVE_DRAWDOWN_RISK,
    OBJECTIVE_WEAKENING_WINDOWS,
    OBJECTIVE_SMH_OVERWEIGHT_RISK,
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "actual_path_validation_executed": True,
    "scope_review_ready": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "paper_shadow_recommendation_allowed": False,
    "promotion_eligible": False,
}


class AILeadershipActualPathValidationError(ValueError):
    pass


def run_ai_leadership_actual_path_validation(
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
        raise AILeadershipActualPathValidationError(
            "AI leadership actual-path validation only supports actual_path_validation mode"
        )
    policy = _load_policy(policy_path)
    candidate_ids = _parse_list(candidates)
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
        or _latest_common_price_date(prices_path, asset_ids),
    )
    resolved_quality_as_of = _resolve_date(
        quality_as_of,
        default=_date_from_text(str(mapping(source_summary.get("data_quality")).get("as_of")))
        or resolved_end,
    )
    required_symbols = _required_price_symbols(policy, asset_ids)
    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_symbols=required_symbols,
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise AILeadershipActualPathValidationError(
            f"TRADING-2309 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    price_matrix = _load_price_matrix(prices_path, asset_ids)
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
        _objective_row(objective_id, outcome_rows, policy) for objective_id in OBJECTIVES
    ]
    scorecards = [
        _candidate_scorecard(candidate_id, outcome_rows, objective_rows, policy)
        for candidate_id in candidate_ids
    ]
    state_rows = [
        _state_recommendation_row(scorecard) for scorecard in scorecards
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
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        actual_rows=actual_rows,
        outcome_rows=outcome_rows,
        objective_rows=objective_rows,
        scorecards=scorecards,
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
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "candidate_scorecards": scorecards,
            "objective_rows": objective_rows,
            "state_recommendations": state_rows,
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
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: missing TRADING-2308 artifact(s): {missing}"
            )
        spec = _read_json(spec_path)
        generation_summary = _read_json(generation_summary_path)
        validation_summary = _read_json(validation_summary_path)
        if spec.get("candidate_id") != candidate_id:
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: signal spec candidate_id mismatch"
            )
        if validation_summary.get("status") != "PASS":
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: TRADING-2308 validation_summary is not PASS"
            )
        if generation_summary.get("validation_status") != "PASS":
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: TRADING-2308 generation_summary validation_status is not PASS"
            )
        spec_result = validator.validate_candidate_signal_spec(spec)
        if spec_result.errors:
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: invalid candidate_signal_spec: {spec_result.errors}"
            )
        rows = _read_signal_series(series_path)
        series_result = validator.validate_candidate_bound_signal_series(rows)
        if series_result.errors:
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: invalid candidate_signal_series: {series_result.errors[:20]}"
            )
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise AILeadershipActualPathValidationError(f"{candidate_id}: empty signal series")
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
            raise AILeadershipActualPathValidationError(
                f"{candidate_id}: no signal rows after date / asset / horizon filters"
            )
        frames[candidate_id] = frame
    return frames


def _actual_path(record: Mapping[str, Any], price_matrix: pd.DataFrame) -> dict[str, Any]:
    target_asset = str(record.get("target_asset") or "")
    horizon = str(record.get("horizon") or "")
    horizon_days = parse_horizon_days(horizon)
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
    target_path = _asset_path(price_matrix, target_asset, decision_position, end_position)
    validation_eligible = all(
        path.get("validation_eligible") for path in (qqq_path, smh_path, target_path)
    )
    status = "complete" if validation_eligible else "partial_price_coverage"
    smh_forward = _optional_float(smh_path.get("forward_return"))
    qqq_forward = _optional_float(qqq_path.get("forward_return"))
    smh_relative = (
        smh_forward - qqq_forward if smh_forward is not None and qqq_forward is not None else None
    )
    return clean_for_yaml(
        {
            "actual_path_status": status,
            "validation_eligible": validation_eligible,
            "data_quality_warning": not validation_eligible,
            "decision_price_date": decision_date.date().isoformat(),
            "horizon_end_date": horizon_end_date.date().isoformat(),
            "target_forward_return": target_path.get("forward_return"),
            "target_max_drawdown": target_path.get("max_drawdown"),
            "qqq_forward_return": qqq_path.get("forward_return"),
            "qqq_max_drawdown": qqq_path.get("max_drawdown"),
            "smh_forward_return": smh_path.get("forward_return"),
            "smh_max_drawdown": smh_path.get("max_drawdown"),
            "smh_relative_forward_return": _round_or_none(smh_relative),
            "data_coverage_ratio": round_float(
                min(
                    to_float(qqq_path.get("coverage_ratio")),
                    to_float(smh_path.get("coverage_ratio")),
                    to_float(target_path.get("coverage_ratio")),
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
        OBJECTIVE_SMH_RELATIVE_RETURN: _score_smh_relative(record, actual_path, policy),
        OBJECTIVE_DRAWDOWN_RISK: _score_drawdown_risk(record, actual_path, policy),
        OBJECTIVE_WEAKENING_WINDOWS: _score_weakening(record, actual_path, policy),
        OBJECTIVE_SMH_OVERWEIGHT_RISK: _score_smh_overweight(record, actual_path, policy),
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
            "smh_relative_forward_return": actual_path.get("smh_relative_forward_return"),
            "smh_relative_return_score": _score_value(
                objective_scores[OBJECTIVE_SMH_RELATIVE_RETURN]
            ),
            "drawdown_risk_score": _score_value(objective_scores[OBJECTIVE_DRAWDOWN_RISK]),
            "weakening_window_score": _score_value(
                objective_scores[OBJECTIVE_WEAKENING_WINDOWS]
            ),
            "smh_overweight_risk_score": _score_value(
                objective_scores[OBJECTIVE_SMH_OVERWEIGHT_RISK]
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


def _objective_row(
    objective_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    score_field = {
        OBJECTIVE_SMH_RELATIVE_RETURN: "smh_relative_return_score",
        OBJECTIVE_DRAWDOWN_RISK: "drawdown_risk_score",
        OBJECTIVE_WEAKENING_WINDOWS: "weakening_window_score",
        OBJECTIVE_SMH_OVERWEIGHT_RISK: "smh_overweight_risk_score",
    }[objective_id]
    rows = [
        row
        for row in outcome_rows
        if bool(row.get("validation_eligible")) and row.get(score_field) not in (None, "")
    ]
    scores = [to_float(row.get(score_field)) for row in rows]
    average_score = sum(scores) / len(scores) if scores else 0.0
    pass_threshold = _policy_float(policy, "objective_pass_alignment_score")
    minimum_records = _policy_int(policy, "minimum_objective_records")
    objective_status = (
        "PASS"
        if len(rows) >= minimum_records and average_score >= pass_threshold
        else "SAMPLE_BLOCKED"
        if len(rows) < minimum_records
        else "INCONCLUSIVE_OR_WEAK"
    )
    return clean_for_yaml(
        {
            "objective_id": objective_id,
            "score_field": score_field,
            "eligible_record_count": len(rows),
            "minimum_record_count": minimum_records,
            "average_alignment_score": round_float(average_score),
            "pass_alignment_score": pass_threshold,
            "objective_status": objective_status,
            "average_smh_relative_forward_return": _average(
                rows, "smh_relative_forward_return"
            ),
            "average_qqq_max_drawdown": _average(rows, "qqq_max_drawdown"),
            "average_smh_max_drawdown": _average(rows, "smh_max_drawdown"),
            **SAFETY_FIELDS,
        }
    )


def _candidate_scorecard(
    candidate_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [row for row in outcome_rows if row.get("candidate_id") == candidate_id]
    eligible = [row for row in rows if bool(row.get("validation_eligible"))]
    scores = [to_float(row.get("combined_alignment_score")) for row in eligible]
    average_score = sum(scores) / len(scores) if scores else 0.0
    minimum_records = _policy_int(policy, "minimum_candidate_records")
    continue_threshold = _policy_float(policy, "candidate_continue_alignment_score")
    reject_threshold = _policy_float(policy, "candidate_reject_alignment_score")
    passed_objectives = sum(
        1 for row in objective_rows if row.get("objective_status") == "PASS"
    )
    if len(eligible) < minimum_records:
        status = STATUS_INCONCLUSIVE
    elif average_score <= reject_threshold:
        status = STATUS_REJECT_RECOMMENDED
    elif average_score >= continue_threshold:
        status = STATUS_CONTINUE_RESEARCH
    else:
        status = STATUS_INCONCLUSIVE
    return clean_for_yaml(
        {
            "candidate_id": candidate_id,
            "record_count": len(rows),
            "validation_eligible_record_count": len(eligible),
            "validation_ineligible_record_count": len(rows) - len(eligible),
            "minimum_record_count": minimum_records,
            "average_alignment_score": round_float(average_score),
            "continue_alignment_score": continue_threshold,
            "reject_alignment_score": reject_threshold,
            "passed_objective_count": passed_objectives,
            "average_smh_relative_forward_return": _average(
                eligible, "smh_relative_forward_return"
            ),
            "average_target_forward_return": _average(eligible, "target_forward_return"),
            "average_target_max_drawdown": _average(eligible, "target_max_drawdown"),
            "average_qqq_max_drawdown": _average(eligible, "qqq_max_drawdown"),
            "average_smh_max_drawdown": _average(eligible, "smh_max_drawdown"),
            "candidate_validation_status": status,
            **SAFETY_FIELDS,
        }
    )


def _state_recommendation_row(scorecard: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": scorecard.get("candidate_id"),
        "recommended_research_status": scorecard.get("candidate_validation_status"),
        "scope_review_ready": False,
        **SAFETY_FIELDS,
    }


def _family_status(
    scorecards: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> str:
    passed_objectives = sum(1 for row in objective_rows if row.get("objective_status") == "PASS")
    continued_candidates = sum(
        1
        for row in scorecards
        if row.get("candidate_validation_status") == STATUS_CONTINUE_RESEARCH
    )
    rejected_candidates = sum(
        1
        for row in scorecards
        if row.get("candidate_validation_status") == STATUS_REJECT_RECOMMENDED
    )
    if (
        passed_objectives >= _policy_int(policy, "continue_research_min_objectives_passed")
        and continued_candidates >= 1
    ):
        return STATUS_CONTINUE_RESEARCH
    if rejected_candidates >= _policy_int(policy, "reject_recommended_min_candidates_rejected"):
        return STATUS_REJECT_RECOMMENDED
    return STATUS_INCONCLUSIVE


def _score_smh_relative(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    if str(record.get("target_asset")) != "SMH":
        return None
    relative_return = _optional_float(actual_path.get("smh_relative_forward_return"))
    return _directional_relative_score(str(record.get("signal_direction")), relative_return, policy)


def _score_drawdown_risk(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    direction = str(record.get("signal_direction"))
    target_drawdown = _optional_float(actual_path.get("target_max_drawdown"))
    qqq_drawdown = _optional_float(actual_path.get("qqq_max_drawdown"))
    smh_drawdown = _optional_float(actual_path.get("smh_max_drawdown"))
    worst_drawdown = min(
        value for value in (target_drawdown, qqq_drawdown, smh_drawdown) if value is not None
    )
    warning = _policy_float(policy, "drawdown_warning_threshold")
    false_risk_on = _policy_float(policy, "false_risk_on_drawdown_threshold")
    if direction == "risk_off":
        return 1.0 if worst_drawdown <= warning else -0.5
    if direction == "risk_on":
        return -1.0 if worst_drawdown <= false_risk_on else 0.5
    return 0.5 if worst_drawdown > warning else -0.5


def _score_weakening(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    signal_name = str(record.get("signal_name") or "")
    direction = str(record.get("signal_direction") or "")
    if "weakening" not in signal_name and direction != "risk_off":
        return None
    relative_return = _optional_float(actual_path.get("smh_relative_forward_return"))
    smh_drawdown = _optional_float(actual_path.get("smh_max_drawdown"))
    negative_threshold = _policy_float(policy, "relative_return_negative_threshold")
    drawdown_threshold = _policy_float(policy, "drawdown_warning_threshold")
    if direction == "risk_off":
        if _lte(relative_return, negative_threshold) or _lte(smh_drawdown, drawdown_threshold):
            return 1.0
        return -1.0
    if direction == "neutral":
        return 0.0
    if _lte(relative_return, negative_threshold) or _lte(smh_drawdown, drawdown_threshold):
        return -1.0
    return 0.5


def _score_smh_overweight(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> float | None:
    if not _eligible(actual_path):
        return None
    signal_name = str(record.get("signal_name") or "")
    target_asset = str(record.get("target_asset") or "")
    if target_asset != "SMH":
        return None
    if "overweight" not in signal_name and "confirmation" not in signal_name:
        return None
    relative_return = _optional_float(actual_path.get("smh_relative_forward_return"))
    smh_drawdown = _optional_float(actual_path.get("smh_max_drawdown"))
    direction = str(record.get("signal_direction") or "")
    positive_threshold = _policy_float(policy, "relative_return_positive_threshold")
    negative_threshold = _policy_float(policy, "relative_return_negative_threshold")
    false_drawdown = _policy_float(policy, "false_risk_on_drawdown_threshold")
    if direction == "risk_on":
        if _gte(relative_return, positive_threshold) and _gt(smh_drawdown, false_drawdown):
            return 1.0
        if _lte(relative_return, negative_threshold) or _lte(smh_drawdown, false_drawdown):
            return -1.0
        return 0.0
    if direction == "risk_off":
        if _lte(relative_return, negative_threshold) or _lte(smh_drawdown, false_drawdown):
            return 0.5
        return -0.5
    return 0.0


def _directional_relative_score(
    direction: str,
    relative_return: float | None,
    policy: Mapping[str, Any],
) -> float | None:
    if relative_return is None:
        return None
    positive = _policy_float(policy, "relative_return_positive_threshold")
    negative = _policy_float(policy, "relative_return_negative_threshold")
    if direction == "risk_on":
        if relative_return >= positive:
            return 1.0
        if relative_return <= negative:
            return -1.0
        return 0.0
    if direction == "risk_off":
        if relative_return <= negative:
            return 1.0
        if relative_return >= positive:
            return -1.0
        return 0.0
    if negative < relative_return < positive:
        return 0.5
    return -0.5


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
    quality_report: DataQualityReport,
    quality_report_path: Path,
    actual_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    recommendation_counts = Counter(
        str(row.get("candidate_validation_status")) for row in scorecards
    )
    objective_counts = Counter(str(row.get("objective_status")) for row in objective_rows)
    eligible_count = sum(1 for row in actual_rows if bool(row.get("validation_eligible")))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "title": "AI / 半导体 Leadership Actual-Path Validation",
            "task_id": TASK_ID,
            "status": status,
            "artifact_role": ARTIFACT_ROLE,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "actual_requested_date_range": f"{start_date.isoformat()}..{end_date.isoformat()}",
            "source_generator_dir": str(generator_dir),
            "policy_path": str(policy_path),
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "candidate_ids": list(candidates),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "actual_path_record_count": len(actual_rows),
            "prediction_outcome_record_count": len(outcome_rows),
            "validation_eligible_record_count": eligible_count,
            "validation_ineligible_record_count": len(actual_rows) - eligible_count,
            "candidate_recommendation_counts": dict(recommendation_counts),
            "objective_status_counts": dict(objective_counts),
            "data_quality": _data_quality_payload(quality_report, quality_report_path),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "source_data_quality_status": quality_report.status,
            "full_universe_validation_blocker_out_of_scope": FULL_UNIVERSE_BLOCKER,
            "allowed_statuses": sorted(ALLOWED_STATUSES),
            "next_task": "TRADING-2310_AI_LEADERSHIP_SCOPE_REVIEW",
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
        "title": "AI / 半导体 Leadership Actual-Path Validation",
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
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "ai_leadership_actual_path_validation_summary.json",
        "actual_path_matrix_json": output_dir / "ai_leadership_actual_path_matrix.json",
        "actual_path_matrix_csv": output_dir / "ai_leadership_actual_path_matrix.csv",
        "prediction_outcome_matrix_json": output_dir
        / "ai_leadership_prediction_outcome_matrix.json",
        "prediction_outcome_matrix_csv": output_dir
        / "ai_leadership_prediction_outcome_matrix.csv",
        "candidate_scorecard": output_dir / "ai_leadership_candidate_scorecard.json",
        "objective_coverage": output_dir / "ai_leadership_objective_coverage_matrix.json",
        "state_recommendation": output_dir / "ai_leadership_state_recommendation_matrix.json",
        "safety_boundary": output_dir / "ai_leadership_actual_path_safety_boundary.json",
        "report_doc": docs_root / "ai_semiconductor_leadership_actual_path_validation.md",
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
    write_json(paths["safety_boundary"], _safety_boundary(summary))
    write_markdown(paths["report_doc"], _render_report(summary, scorecards, objective_rows))
    return {key: str(path) for key, path in paths.items()}


def _safety_boundary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": summary["status"],
        "data_quality_status": summary["data_quality_status"],
        "data_quality_report_path": summary["data_quality_report_path"],
        "full_universe_readiness_claimed": False,
        "does_not_modify_generator_artifacts": True,
        "does_not_run_scope_review": True,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        "next_required_task": "TRADING-2310_AI_LEADERSHIP_SCOPE_REVIEW",
        **SAFETY_FIELDS,
    }


def _render_report(
    summary: Mapping[str, Any],
    scorecards: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
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
    return "\n".join(
        [
            "# AI / 半导体 Leadership Actual-Path Validation",
            "",
            "TRADING-2309 对 TRADING-2308 candidate-bound price-proxy artifacts "
            "执行 research-only actual-path validation。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_report_path: `{summary['data_quality_report_path']}`",
            "- full_universe_readiness_claimed: `False`",
            f"- full_universe_validation_blocker_out_of_scope: `{FULL_UNIVERSE_BLOCKER}`",
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
            "## Safety",
            "",
            "本报告只验证 actual-path evidence，不修改 TRADING-2308 generator artifacts，"
            "不执行 scope review，不允许 promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )


def _validate_generator_source(generator_dir: Path, candidates: Sequence[str]) -> dict[str, Any]:
    summary_path = generator_dir / "ai_semiconductor_leadership_generator_poc_summary.json"
    if not summary_path.exists():
        raise AILeadershipActualPathValidationError(
            f"TRADING-2308 generator summary missing: {summary_path}"
        )
    summary = _read_json(summary_path)
    if summary.get("status") != SOURCE_GENERATOR_STATUS:
        raise AILeadershipActualPathValidationError(
            "TRADING-2308 generator summary is not ready for actual-path validation"
        )
    if summary.get("promotion_allowed") is not False:
        raise AILeadershipActualPathValidationError(
            "TRADING-2308 source unexpectedly allows promotion"
        )
    if summary.get("actual_path_validation_executed") is not False:
        raise AILeadershipActualPathValidationError(
            "TRADING-2308 source should not have executed actual-path validation"
        )
    missing = sorted(set(candidates) - set(_strings(summary.get("candidates"))))
    if missing:
        raise AILeadershipActualPathValidationError(
            f"TRADING-2308 summary missing requested candidates: {missing}"
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
        raise AILeadershipActualPathValidationError(
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
        raise AILeadershipActualPathValidationError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise AILeadershipActualPathValidationError(f"policy file must be object: {path}")
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
        "validation_thresholds",
        "status_rule",
        "safety",
    )
    missing = [field for field in required if not policy.get(field)]
    if missing:
        raise AILeadershipActualPathValidationError(f"policy missing fields: {missing}")
    allowed = set(_strings(mapping(policy.get("status_rule")).get("allowed_statuses")))
    if allowed != ALLOWED_STATUSES:
        raise AILeadershipActualPathValidationError(
            f"policy allowed_statuses must match {sorted(ALLOWED_STATUSES)}"
        )
    safety = mapping(policy.get("safety"))
    for field, expected in SAFETY_FIELDS.items():
        if field == "promotion_eligible":
            continue
        if safety.get(field) != expected:
            raise AILeadershipActualPathValidationError(
                f"policy safety.{field} must be {expected}"
            )


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
        raise AILeadershipActualPathValidationError(f"missing policy threshold: {key}")
    value = threshold.get("value")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise AILeadershipActualPathValidationError(
            f"invalid policy threshold: {key}"
        ) from exc
    if not math.isfinite(parsed):
        raise AILeadershipActualPathValidationError(f"invalid policy threshold: {key}")
    return parsed


def _policy_int(policy: Mapping[str, Any], key: str) -> int:
    value = int(_policy_float(policy, key))
    if value <= 0:
        raise AILeadershipActualPathValidationError(f"policy threshold must be positive: {key}")
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
        raise AILeadershipActualPathValidationError(f"{path}: expected JSON object")
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
        raise AILeadershipActualPathValidationError(
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
        raise AILeadershipActualPathValidationError(
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


def _parse_list(value: str | Sequence[str], *, uppercase: bool = False) -> tuple[str, ...]:
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    else:
        raw = [str(item).strip() for item in value]
    parsed = [item.upper() if uppercase else item for item in raw if item]
    if not parsed:
        raise AILeadershipActualPathValidationError("list option must be non-empty")
    return tuple(parsed)


def _strings(value: Any) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
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
        "smh_relative_forward_return": None,
        "data_coverage_ratio": 0.0,
    }


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


def _round_or_none(value: float | None) -> float | None:
    return round_float(value) if value is not None else None


def _lte(left: float | None, right: float) -> bool:
    return left is not None and left <= right


def _gte(left: float | None, right: float) -> bool:
    return left is not None and left >= right


def _gt(left: float | None, right: float) -> bool:
    return left is not None and left > right


__all__ = [
    "DEFAULT_GENERATOR_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "run_ai_leadership_actual_path_validation",
]
