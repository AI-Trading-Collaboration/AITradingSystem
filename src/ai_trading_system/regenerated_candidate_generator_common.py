from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pandas as pd

from ai_trading_system.candidate_signal_binding_schema import (
    CandidateArtifactProvenance,
    CandidateBoundPredictionArtifact,
    CandidateBoundSignalRecord,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    REGENERATED_CANDIDATE_GENERATOR_MODE,
    REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
    CandidateGeneratorContext,
    CandidateGeneratorError,
    CandidateSignalSpec,
    candidate_artifact_safety_fields,
    generator_operation_safety_fields,
    regenerated_executable_candidate_artifact_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    load_adjusted_price_matrix,
    round_float,
    to_float,
)

REGENERATED_CANDIDATE_FAMILY = "first_layer_executable_candidate"

# Pilot normalization constants for non-promotable regenerated candidate artifacts only.
# TRADING-2285 must validate or recalibrate before any investment interpretation.
MIN_HISTORY_DAYS = 21
LOW_CONFIDENCE = 0.2
BASE_CONFIDENCE = 0.62
OPTIONAL_INPUT_CONFIDENCE_PENALTY = 0.06
CONFIDENCE_FLOOR_WITH_OPTIONAL_MISSING = 0.34
NEUTRAL_BAND = 0.1
RETURN_SCORE_SCALE = 0.08
RELATIVE_STRENGTH_SCORE_SCALE = 0.05
DRAWDOWN_SCORE_SCALE = 0.12
MOVING_AVERAGE_SCORE_SCALE = 0.04
VOLATILITY_SCORE_SCALE = 0.18
VOLATILITY_EXPANSION_SCALE = 0.08
VIX_STRESS_LEVEL = 25.0
VIX_STRESS_SCALE = 15.0
ANNUALIZATION_TRADING_DAYS = 252.0


@dataclass(frozen=True)
class RegeneratedCandidateProvenance(CandidateArtifactProvenance):
    proxy_input_used: bool = False
    proxy_input_reason: str = ""
    proxy_limitations: tuple[str, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    missing_input_policy: str = "neutral_signal_with_low_confidence"
    vix_available: bool | None = None
    volatility_proxy_mode: str = ""


@dataclass(frozen=True)
class SignalComputation:
    signal_name: str
    signal_value: float
    signal_direction: str
    signal_confidence: float
    source_state: str
    missing_inputs: tuple[str, ...] = ()
    proxy_input_used: bool = False
    proxy_input_reason: str = ""
    proxy_limitations: tuple[str, ...] = ()
    vix_available: bool | None = None
    volatility_proxy_mode: str = ""


class PriceDerivedRegeneratedCandidateGenerator:
    candidate_id: str
    generator_version: str
    model_or_rule_version: str
    required_inputs: tuple[str, ...]
    optional_input_tickers: tuple[str, ...] = ()
    output_signal_names: tuple[str, ...]
    signal_direction_mapping: dict[str, str]
    pit_policy = "strict_pit"

    @property
    def generator_id(self) -> str:
        return self.candidate_id

    @property
    def candidate_family(self) -> str:
        return REGENERATED_CANDIDATE_FAMILY

    def build_signal_spec(
        self,
        context: CandidateGeneratorContext,
    ) -> CandidateSignalSpec:
        self._assert_context(context)
        return CandidateSignalSpec(
            candidate_id=context.candidate_id,
            candidate_family=context.candidate_family,
            generator_id=self.generator_id,
            generator_version=self.generator_version,
            signal_spec_version=context.signal_spec_version,
            prediction_schema_version=context.prediction_schema_version,
            target_asset=context.target_asset,
            supported_horizons=tuple(parse_csv_list(context.horizon)),
            required_inputs=self.required_inputs,
            output_signal_names=self.output_signal_names,
            signal_direction_mapping=self.signal_direction_mapping,
            validity_rule=(
                "valid_from equals decision_timestamp; valid_until equals "
                "decision_timestamp plus horizon calendar days"
            ),
            pit_policy=self.pit_policy,
            **generator_operation_safety_fields(),
        )

    def generate_signal_series(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
    ) -> list[CandidateBoundSignalRecord]:
        self._assert_context(context)
        target_assets = parse_csv_list(context.target_asset)
        horizons = parse_csv_list(context.horizon)
        required_tickers = tuple(
            dict.fromkeys((*target_assets, "QQQ", "SPY", "SMH", *self.optional_input_tickers))
        )
        price_matrix = load_adjusted_price_matrix(context.source_paths[0], required_tickers)
        dates = _generation_dates(price_matrix, context)
        if not dates:
            raise CandidateGeneratorError(f"{self.candidate_id} generated no source dates")

        records: list[CandidateBoundSignalRecord] = []
        source_path = context.source_paths[0]
        source_hash = context.source_hashes[0]
        source_row_index = 0
        for current_ts in dates:
            for target_asset in target_assets:
                computations = self.compute_signals(price_matrix, target_asset, current_ts)
                for horizon in horizons:
                    horizon_days = parse_horizon_days(horizon)
                    as_of = datetime.combine(current_ts.date(), time(21, 0), tzinfo=UTC)
                    decision = as_of + timedelta(days=1)
                    valid_until = decision + timedelta(days=horizon_days)
                    for computation in computations:
                        source_row_index += 1
                        records.append(
                            CandidateBoundSignalRecord(
                                candidate_id=context.candidate_id,
                                candidate_family=context.candidate_family,
                                source_experiment_id=signal_spec.generator_id,
                                source_artifact_id=source_path.stem,
                                source_artifact_path=str(source_path),
                                source_artifact_hash=source_hash,
                                signal_spec_version=context.signal_spec_version,
                                prediction_schema_version=context.prediction_schema_version,
                                generated_at=context.generated_at.isoformat(),
                                as_of_timestamp=as_of.isoformat(),
                                decision_timestamp=decision.isoformat(),
                                target_asset=target_asset,
                                horizon=horizon,
                                signal_name=computation.signal_name,
                                signal_value=computation.signal_value,
                                signal_direction=computation.signal_direction,
                                signal_confidence=computation.signal_confidence,
                                valid_from=decision.isoformat(),
                                valid_until=valid_until.isoformat(),
                                input_snapshot_hash=context.input_snapshot_hash,
                                feature_snapshot_hash=context.feature_snapshot_hash,
                                model_or_rule_version=self.model_or_rule_version,
                                provenance=_provenance(context, computation, self.pit_policy),
                                **candidate_artifact_safety_fields(),
                                source_row_index=source_row_index,
                                source_date=current_ts.date().isoformat(),
                                source_trend_state=computation.source_state,
                                source_confidence=computation.signal_confidence,
                                source_prediction_flags={
                                    "regenerated_candidate_artifact": True,
                                    "actual_path_validation_ready": False,
                                    "missing_input_neutralized": bool(computation.missing_inputs),
                                    "proxy_input_used": computation.proxy_input_used,
                                },
                            )
                        )
        return records

    def generate_prediction_artifact(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
        signal_records: list[CandidateBoundSignalRecord],
    ) -> dict[str, Any]:
        if not signal_records:
            raise CandidateGeneratorError("prediction artifact requires signal records")
        latest = signal_records[-1]
        source_path = context.source_paths[0]
        source_hash = context.source_hashes[0]
        prediction_records = [_prediction_record(record) for record in signal_records]
        artifact = CandidateBoundPredictionArtifact(
            artifact_id=f"{context.candidate_id}_regenerated_prediction_artifact",
            artifact_role=REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
            candidate_id=context.candidate_id,
            candidate_family=context.candidate_family,
            source_experiment_id=signal_spec.generator_id,
            source_artifact_id=source_path.stem,
            source_artifact_path=str(source_path),
            source_artifact_hash=source_hash,
            signal_spec_version=context.signal_spec_version,
            prediction_schema_version=context.prediction_schema_version,
            generated_at=context.generated_at.isoformat(),
            as_of_timestamp=latest.as_of_timestamp,
            decision_timestamp=latest.decision_timestamp,
            target_asset=latest.target_asset,
            horizon=latest.horizon,
            signal_name=latest.signal_name,
            signal_value=latest.signal_value,
            signal_direction=latest.signal_direction,
            signal_confidence=latest.signal_confidence,
            valid_from=latest.valid_from,
            valid_until=latest.valid_until,
            input_snapshot_hash=context.input_snapshot_hash,
            feature_snapshot_hash=context.feature_snapshot_hash,
            model_or_rule_version=self.model_or_rule_version,
            provenance=_provenance(context, _latest_computation(latest), self.pit_policy),
            prediction_records=prediction_records,
            source_schema_status="candidate_bound",
            **regenerated_executable_candidate_artifact_safety_fields(),
        )
        payload = artifact.to_dict()
        payload["schema_version"] = context.prediction_schema_version
        payload["record_count"] = len(prediction_records)
        payload["generation_mode"] = context.mode
        payload["candidate_binding_method"] = "native_candidate_id"
        payload["target_assets"] = parse_csv_list(context.target_asset)
        payload["horizons"] = parse_csv_list(context.horizon)
        payload["actual_path_validation_blocker"] = (
            "TRADING-2285_Regenerated_Candidate_Actual_Path_Validation"
        )
        payload["dynamic_promotion_status"] = "BLOCKED"
        payload.update(trading_2281_boundary_fields())
        return clean_for_yaml(payload)

    def compute_signals(
        self,
        price_matrix: pd.DataFrame,
        target_asset: str,
        current_ts: pd.Timestamp,
    ) -> list[SignalComputation]:
        raise NotImplementedError

    def _assert_context(self, context: CandidateGeneratorContext) -> None:
        if context.candidate_id != self.candidate_id:
            raise CandidateGeneratorError(
                f"{self.candidate_id} generator received {context.candidate_id}"
            )
        if context.candidate_family != REGENERATED_CANDIDATE_FAMILY:
            raise CandidateGeneratorError(
                f"{self.candidate_id} requires {REGENERATED_CANDIDATE_FAMILY} family"
            )
        if context.mode != REGENERATED_CANDIDATE_GENERATOR_MODE:
            raise CandidateGeneratorError(
                f"{self.candidate_id} only supports regenerated_candidate_artifacts mode"
            )


def parse_csv_list(value: str) -> tuple[str, ...]:
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    if not parsed:
        raise CandidateGeneratorError("comma-separated value must be non-empty")
    return parsed


def parse_horizon_days(horizon: str) -> int:
    text = horizon.strip().lower()
    if not text.endswith("d"):
        raise CandidateGeneratorError("horizon must use day suffix, e.g. 10d")
    try:
        days = int(text[:-1])
    except ValueError as exc:
        raise CandidateGeneratorError("horizon day count must be an integer") from exc
    if days <= 0:
        raise CandidateGeneratorError("horizon day count must be positive")
    return days


def trend_direction(value: float) -> str:
    if value > NEUTRAL_BAND:
        return "trend_confirming"
    if value < -NEUTRAL_BAND:
        return "trend_weakening"
    return "neutral"


def risk_direction(value: float) -> str:
    if value > NEUTRAL_BAND:
        return "risk_on"
    if value < -NEUTRAL_BAND:
        return "risk_off"
    return "neutral"


def volatility_direction(value: float) -> str:
    if value > NEUTRAL_BAND:
        return "volatility_compression"
    if value < -NEUTRAL_BAND:
        return "volatility_expansion"
    return "neutral"


def clamp_score(value: float) -> float:
    return round_float(max(-1.0, min(1.0, to_float(value))))


def confidence_for(
    *,
    history_count: int,
    missing_inputs: tuple[str, ...] = (),
) -> float:
    if history_count < MIN_HISTORY_DAYS:
        return LOW_CONFIDENCE
    confidence = BASE_CONFIDENCE - (
        len(missing_inputs) * OPTIONAL_INPUT_CONFIDENCE_PENALTY
    )
    if missing_inputs:
        confidence = max(CONFIDENCE_FLOOR_WITH_OPTIONAL_MISSING, confidence)
    return round_float(max(LOW_CONFIDENCE, min(1.0, confidence)))


def missing_tickers(price_matrix: pd.DataFrame, tickers: tuple[str, ...]) -> tuple[str, ...]:
    missing: list[str] = []
    for ticker in tickers:
        if ticker not in price_matrix.columns or price_matrix[ticker].dropna().empty:
            missing.append(ticker)
    return tuple(missing)


def price_at(price_matrix: pd.DataFrame, ticker: str, current_ts: pd.Timestamp) -> float | None:
    if ticker not in price_matrix.columns or current_ts not in price_matrix.index:
        return None
    value = to_float(price_matrix.at[current_ts, ticker], default=float("nan"))
    if pd.isna(value) or value <= 0.0:
        return None
    return value


def rolling_return(
    price_matrix: pd.DataFrame,
    ticker: str,
    current_ts: pd.Timestamp,
    lookback_days: int,
) -> float | None:
    if ticker not in price_matrix.columns or current_ts not in price_matrix.index:
        return None
    position = price_matrix.index.get_loc(current_ts)
    if not isinstance(position, int) or position < lookback_days:
        return None
    current = price_at(price_matrix, ticker, current_ts)
    previous = to_float(price_matrix[ticker].iloc[position - lookback_days], default=0.0)
    if current is None or previous <= 0.0:
        return None
    return current / previous - 1.0


def moving_average_gap(
    price_matrix: pd.DataFrame,
    ticker: str,
    current_ts: pd.Timestamp,
    lookback_days: int,
) -> float | None:
    if ticker not in price_matrix.columns or current_ts not in price_matrix.index:
        return None
    position = price_matrix.index.get_loc(current_ts)
    if not isinstance(position, int) or position + 1 < lookback_days:
        return None
    window = price_matrix[ticker].iloc[position + 1 - lookback_days : position + 1].dropna()
    current = price_at(price_matrix, ticker, current_ts)
    if current is None or window.empty:
        return None
    average = to_float(window.mean())
    if average <= 0.0:
        return None
    return current / average - 1.0


def rolling_drawdown(
    price_matrix: pd.DataFrame,
    ticker: str,
    current_ts: pd.Timestamp,
    lookback_days: int,
) -> float | None:
    if ticker not in price_matrix.columns or current_ts not in price_matrix.index:
        return None
    position = price_matrix.index.get_loc(current_ts)
    if not isinstance(position, int) or position + 1 < lookback_days:
        return None
    window = price_matrix[ticker].iloc[position + 1 - lookback_days : position + 1].dropna()
    current = price_at(price_matrix, ticker, current_ts)
    if current is None or window.empty:
        return None
    peak = to_float(window.max())
    if peak <= 0.0:
        return None
    return current / peak - 1.0


def realized_volatility(
    price_matrix: pd.DataFrame,
    ticker: str,
    current_ts: pd.Timestamp,
    lookback_days: int,
) -> float | None:
    if ticker not in price_matrix.columns or current_ts not in price_matrix.index:
        return None
    position = price_matrix.index.get_loc(current_ts)
    if not isinstance(position, int) or position + 1 < lookback_days:
        return None
    window = price_matrix[ticker].iloc[position + 1 - lookback_days : position + 1].dropna()
    if len(window) < lookback_days:
        return None
    returns = window.pct_change().dropna()
    if returns.empty:
        return None
    return to_float(returns.std()) * (ANNUALIZATION_TRADING_DAYS**0.5)


def history_count(price_matrix: pd.DataFrame, current_ts: pd.Timestamp) -> int:
    position = price_matrix.index.get_loc(current_ts)
    return int(position) + 1 if isinstance(position, int) else 0


def neutral_signal(
    signal_name: str,
    *,
    direction: str = "neutral",
    missing_inputs: tuple[str, ...],
    source_state: str = "insufficient_data_neutral",
) -> SignalComputation:
    return SignalComputation(
        signal_name=signal_name,
        signal_value=0.0,
        signal_direction=direction,
        signal_confidence=LOW_CONFIDENCE,
        source_state=source_state,
        missing_inputs=missing_inputs,
    )


def _generation_dates(
    price_matrix: pd.DataFrame,
    context: CandidateGeneratorContext,
) -> list[pd.Timestamp]:
    start = pd.Timestamp(context.start_date)
    end = pd.Timestamp(context.end_date)
    return [
        pd.Timestamp(item)
        for item in price_matrix.index
        if start <= pd.Timestamp(item) <= end
    ]


def _provenance(
    context: CandidateGeneratorContext,
    computation: SignalComputation,
    pit_policy: str,
) -> RegeneratedCandidateProvenance:
    return RegeneratedCandidateProvenance(
        source_paths=[str(path) for path in context.source_paths],
        source_hashes=list(context.source_hashes),
        regeneration_mode="deterministic_regeneration",
        pit_policy=pit_policy,
        candidate_binding_method="native_candidate_id",
        source_schema_status="candidate_bound",
        promotion_eligible=False,
        proxy_input_used=computation.proxy_input_used,
        proxy_input_reason=computation.proxy_input_reason,
        proxy_limitations=computation.proxy_limitations,
        missing_inputs=computation.missing_inputs,
        missing_input_policy="neutral_signal_with_low_confidence"
        if computation.missing_inputs
        else "fail_closed",
        vix_available=computation.vix_available,
        volatility_proxy_mode=computation.volatility_proxy_mode,
    )


def _prediction_record(record: CandidateBoundSignalRecord) -> dict[str, Any]:
    payload = record.to_dict()
    payload["prediction_fields"] = {
        "candidate_signal": record.signal_name,
        "signal_value": record.signal_value,
        "signal_direction": record.signal_direction,
        "signal_confidence": record.signal_confidence,
        "regenerated_candidate_artifact": True,
        "actual_path_validation_ready": False,
    }
    return clean_for_yaml(payload)


def _latest_computation(record: CandidateBoundSignalRecord) -> SignalComputation:
    provenance = record.provenance.to_dict()
    return SignalComputation(
        signal_name=record.signal_name,
        signal_value=record.signal_value,
        signal_direction=record.signal_direction,
        signal_confidence=record.signal_confidence,
        source_state=record.source_trend_state,
        missing_inputs=tuple(provenance.get("missing_inputs") or ()),
        proxy_input_used=bool(provenance.get("proxy_input_used")),
        proxy_input_reason=str(provenance.get("proxy_input_reason") or ""),
        proxy_limitations=tuple(provenance.get("proxy_limitations") or ()),
        vix_available=provenance.get("vix_available"),
        volatility_proxy_mode=str(provenance.get("volatility_proxy_mode") or ""),
    )
