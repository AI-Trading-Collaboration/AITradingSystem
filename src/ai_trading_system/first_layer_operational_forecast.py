from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal, Self, cast

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_policy_calibration import (
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
)
from ai_trading_system.platform.artifacts import sha256_path, write_bytes_atomic
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.upper_state_label_feature_reset import (
    MODEL_SPECS,
    V3_FEATURE_COLUMNS,
    _binary_confidence,
    _compose_confidence,
    _compose_state,
    _compute_feature_frame,
    _score_rows,
    build_upper_state_action_value_matrix_v2,
    build_upper_state_labels_v2,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_OPERATIONAL_FORECAST_POLICY_PATH = Path(
    "config/research/first_layer_operational_forecast_producer_v1.yaml"
)

_SHA256_CHARS = frozenset("0123456789abcdef")
_PRIMARY_START = date(2021, 2, 22)
_PRIMARY_END = date(2025, 12, 2)
_PRIMARY_SESSION_COUNT = 1202
_EXPECTED_MODEL_IDS = tuple(MODEL_SPECS)
_ALLOWED_STATES = ("constructive", "defensive", "neutral", "risk_off", "risk_on")


class OperationalForecastError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in _SHA256_CHARS for character in value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


class EvaluationWindow(_StrictModel):
    calendar: Literal["XNYS"]
    start: date
    end: date
    expected_session_count: int
    research_window_id: Literal["exact_three_asset_validated"]

    @model_validator(mode="after")
    def validate_primary_window(self) -> Self:
        if (self.start, self.end, self.expected_session_count) != (
            _PRIMARY_START,
            _PRIMARY_END,
            _PRIMARY_SESSION_COUNT,
        ):
            raise ValueError("primary evaluation window drifted")
        return self


class CashReferencePolicy(_StrictModel):
    proxy_asset: Literal["SHY"]
    exact_asset: Literal["SGOV"]
    exact_asset_start: date
    transition_rule: Literal["RETURN_SPLICE_PROXY_UNTIL_FIRST_VALID_EXACT_RETURN"]
    proxy_role: Literal["TRAINING_INITIALIZATION_ONLY"]
    proxy_allowed_on_or_after_evaluation_start: Literal[False]


class TrainingHistoryPolicy(_StrictModel):
    start: date
    feature_warmup_sessions: int
    required_price_assets: tuple[str, ...]
    required_rate_series: tuple[str, ...]
    cash_reference: CashReferencePolicy

    @model_validator(mode="after")
    def validate_training_history(self) -> Self:
        if self.start != date(2018, 1, 2) or self.feature_warmup_sessions != 126:
            raise ValueError("training history or feature warmup drifted")
        if self.required_price_assets != ("QQQ", "TQQQ", "SHY", "SGOV"):
            raise ValueError("required price assets drifted")
        if self.required_rate_series != ("DGS10", "DGS2", "DTWEXBGS"):
            raise ValueError("required rate series drifted")
        if self.cash_reference.exact_asset_start != date(2020, 5, 28):
            raise ValueError("SGOV exact start drifted")
        return self


class WalkForwardPolicy(_StrictModel):
    mode: Literal["ROLLING_FIXED"]
    train_window_sessions: int
    label_horizon_sessions: int
    refit_step_sessions: int
    label_maturity_rule: Literal["LABEL_END_SESSION_ON_OR_BEFORE_FIT_SESSION"]
    fold_selection_rule: Literal["MOST_RECENT_FIT_EXACTLY_ONCE_PER_EVALUATION_SESSION"]
    terminal_emission_rule: Literal["FEATURES_ONLY_NO_FORWARD_LABEL_JOIN"]

    @model_validator(mode="after")
    def validate_frozen_walk_forward(self) -> Self:
        if (
            self.train_window_sessions,
            self.label_horizon_sessions,
            self.refit_step_sessions,
        ) != (504, 20, 21):
            raise ValueError("walk-forward values drifted")
        return self


class ModelContract(_StrictModel):
    model_id: Literal["first_layer_composer_v2"]
    model_version: Literal["first_layer_composer_v2"]
    model_specs_sha256: str
    required_model_ids: tuple[str, ...]
    allowed_states: tuple[str, ...]

    @field_validator("model_specs_sha256")
    @classmethod
    def validate_model_hash(cls, value: str) -> str:
        return _sha256(value, "model_specs_sha256")

    @model_validator(mode="after")
    def validate_model_contract(self) -> Self:
        if self.required_model_ids != _EXPECTED_MODEL_IDS:
            raise ValueError("required model ids drifted")
        if self.allowed_states != _ALLOWED_STATES:
            raise ValueError("allowed trend states drifted")
        return self


class AuthorityBinding(_StrictModel):
    path: str
    sha256: str

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _sha256(value, "authority sha256")


class AuthorityPolicy(_StrictModel):
    threshold_policy: AuthorityBinding
    composer_policy: AuthorityBinding
    label_taxonomy: AuthorityBinding
    action_value_policy: AuthorityBinding
    frozen_probe_registry: AuthorityBinding


class OutputContract(_StrictModel):
    date_semantics: Literal["COMPLETED_XNYS_FEATURE_SESSION"]
    known_at_semantics: Literal["SAME_COMPLETED_XNYS_SESSION"]
    available_at_semantics: Literal["SAME_COMPLETED_XNYS_SESSION"]
    decision_at_semantics: Literal["NEXT_VALID_XNYS_SESSION"]
    exactly_one_row_per_evaluation_session: Literal[True]
    forward_label_columns_allowed: Literal[False]
    cross_session_fill_allowed: Literal[False]


class SafetyPolicy(_StrictModel):
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    real_cache_materialization_authorized_in_this_wave: Literal[False]
    quantconnect_project_mutation_allowed: Literal[False]
    quantconnect_backtest_allowed: Literal[False]
    provider_query_allowed: Literal[False]
    raw_option_payload_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class OperationalForecastProducerPolicy(_StrictModel):
    schema_version: Literal["first_layer_operational_forecast_producer_policy.v1"]
    policy_id: Literal["first_layer_operational_forecast_producer_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_APPROVED_NON_EXECUTABLE_DATA_RESEARCH"]
    owner: Literal["project_owner"]
    owner_decision_id: str
    policy_metadata: Mapping[str, str]
    evaluation_window: EvaluationWindow
    training_history: TrainingHistoryPolicy
    walk_forward: WalkForwardPolicy
    model_contract: ModelContract
    authorities: AuthorityPolicy
    output_contract: OutputContract
    safety: SafetyPolicy


@dataclass(frozen=True)
class LoadedOperationalForecastPolicy:
    policy: OperationalForecastProducerPolicy
    path: Path
    file_sha256: str
    authority_payloads: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class OperationalForecastResult:
    predictions: pd.DataFrame
    fit_audit: pd.DataFrame
    receipt: Mapping[str, Any]


def load_operational_forecast_policy(
    path: Path = DEFAULT_OPERATIONAL_FORECAST_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedOperationalForecastPolicy:
    resolved = _bound_file(path, project_root)
    try:
        content = resolved.read_bytes()
        raw = safe_load_yaml_path(resolved)
        if not isinstance(raw, dict):
            raise TypeError("policy root must be a mapping")
        policy = OperationalForecastProducerPolicy.model_validate(raw, strict=False)
        if _model_specs_sha256() != policy.model_contract.model_specs_sha256:
            raise ValueError("MODEL_SPECS code authority drifted")
        payloads: dict[str, Mapping[str, Any]] = {}
        for name, binding in (
            ("threshold_policy", policy.authorities.threshold_policy),
            ("composer_policy", policy.authorities.composer_policy),
            ("label_taxonomy", policy.authorities.label_taxonomy),
            ("action_value_policy", policy.authorities.action_value_policy),
            ("frozen_probe_registry", policy.authorities.frozen_probe_registry),
        ):
            authority_path = _bound_file(Path(binding.path), project_root)
            if sha256_path(authority_path) != binding.sha256:
                raise ValueError(f"authority drifted: {binding.path}")
            authority = safe_load_yaml_path(authority_path)
            if not isinstance(authority, dict):
                raise TypeError(f"authority root must be a mapping: {binding.path}")
            payloads[name] = authority
    except (OSError, TypeError, ValueError) as exc:
        raise OperationalForecastError("OPERATIONAL_FORECAST_POLICY_INVALID", str(exc)) from exc
    return LoadedOperationalForecastPolicy(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(content).hexdigest(),
        authority_payloads=payloads,
    )


def build_operational_forecast_series(
    *,
    loaded_policy: LoadedOperationalForecastPolicy,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    data_quality_status: str,
    data_quality_identity_sha256: str,
) -> OperationalForecastResult:
    if data_quality_status != "PASS":
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_DQ_NOT_PASS", f"observed={data_quality_status}"
        )
    try:
        dq_identity = _sha256(data_quality_identity_sha256, "data_quality_identity_sha256")
    except ValueError as exc:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_DQ_IDENTITY_INVALID", str(exc)
        ) from exc

    policy = loaded_policy.policy
    evaluation_sessions = _xnys_sessions(
        policy.evaluation_window.start, policy.evaluation_window.end
    )
    if len(evaluation_sessions) != policy.evaluation_window.expected_session_count:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_CALENDAR_DRIFT",
            "expected="
            f"{policy.evaluation_window.expected_session_count}; "
            f"observed={len(evaluation_sessions)}",
        )
    price_frame = _normalize_frame(prices, "prices")
    rate_frame = _normalize_frame(rates, "rates")
    model_prices, cash_source = _build_model_prices(
        price_frame=price_frame,
        policy=policy,
        evaluation_sessions=evaluation_sessions,
    )
    _require_rate_coverage(rate_frame, policy, pd.DatetimeIndex(model_prices.index))

    features = _compute_feature_frame(model_prices, rate_frame).copy()
    features["feature_warmup_complete"] = np.arange(len(features)) >= (
        policy.training_history.feature_warmup_sessions - 1
    )
    features["cash_reference_source"] = cash_source.reindex(features.index)
    labels = _build_training_labels(loaded_policy, model_prices)
    training = _build_training_table(
        features=features,
        labels=labels,
        sessions=tuple(timestamp.date() for timestamp in model_prices.index),
        label_horizon=policy.walk_forward.label_horizon_sessions,
        exact_cash_start=policy.training_history.cash_reference.exact_asset_start,
    )
    composer = loaded_policy.authority_payloads["composer_policy"]
    threshold_policy = loaded_policy.authority_payloads["threshold_policy"]
    positive_quantile = float(
        _mapping(threshold_policy.get("threshold_selection")).get(
            "positive_score_quantile", 0.65
        )
    )
    floors = _mapping(threshold_policy.get("positive_sample_floor"))
    model_scores = {
        model_id: _score_rows(features, _mapping(spec.get("feature_weights")))
        for model_id, spec in MODEL_SPECS.items()
    }
    training_scores = {
        model_id: model_scores[model_id].reindex(pd.to_datetime(training["date"])).to_numpy()
        for model_id in MODEL_SPECS
    }
    train_window = policy.walk_forward.train_window_sessions
    step = policy.walk_forward.refit_step_sessions
    predictions: list[dict[str, Any]] = []
    fit_rows: list[dict[str, Any]] = []

    for block_start in range(0, len(evaluation_sessions), step):
        fit_session = evaluation_sessions[block_start]
        fit_id = f"operational_wf_{block_start // step:04d}"
        eligible_mask = (
            training["label_available_at"].map(date.fromisoformat) <= fit_session
        ) & training["feature_warmup_complete"].astype(bool)
        eligible_positions = np.flatnonzero(eligible_mask.to_numpy())
        if len(eligible_positions) < train_window:
            raise OperationalForecastError(
                "OPERATIONAL_FORECAST_TRAINING_HISTORY_INSUFFICIENT",
                f"fit_session={fit_session.isoformat()}; required={train_window}; "
                f"observed={len(eligible_positions)}",
            )
        selected_positions = eligible_positions[-train_window:]
        selected = training.iloc[selected_positions]
        thresholds: dict[str, float] = {}
        for model_id, spec in MODEL_SPECS.items():
            label_column = str(spec["label_column"])
            floor = int(floors.get(model_id, 0))
            scores = pd.Series(training_scores[model_id][selected_positions], dtype=float)
            positive_mask = selected[label_column].astype(bool).reset_index(drop=True)
            positives = scores.loc[positive_mask]
            threshold = math.inf if len(positives) < floor else float(
                positives.quantile(positive_quantile)
            )
            thresholds[model_id] = threshold
            fit_rows.append(
                {
                    "fit_id": fit_id,
                    "model_id": model_id,
                    "fit_session": fit_session.isoformat(),
                    "train_start": str(selected["date"].iloc[0]),
                    "train_end": str(selected["date"].iloc[-1]),
                    "latest_label_available_at": str(selected["label_available_at"].iloc[-1]),
                    "train_sample_count": len(selected),
                    "positive_train_sample_count": len(positives),
                    "positive_sample_floor": floor,
                    "threshold": threshold,
                    "sample_status": "PASS" if not math.isinf(threshold) else "SAMPLE_INSUFFICIENT",
                    "proxy_training_sample_count": int(
                        selected["cash_proxy_in_label_horizon"].astype(bool).sum()
                    ),
                    "label_maturity_pass": bool(
                        selected["label_available_at"].map(date.fromisoformat).max()
                        <= fit_session
                    ),
                }
            )
        threshold_hash = _canonical_sha256(
            {model_id: _json_number(value) for model_id, value in thresholds.items()}
        )
        block_sessions = evaluation_sessions[block_start : block_start + step]
        for session in block_sessions:
            timestamp = pd.Timestamp(session)
            row = cast(pd.Series, features.loc[timestamp])
            state_input: dict[str, Any] = {}
            for model_id, spec in MODEL_SPECS.items():
                prefix = str(spec["label_column"]).removesuffix("_label")
                score = float(model_scores[model_id].loc[timestamp])
                threshold = thresholds[model_id]
                state_input[f"{prefix}_pred"] = bool(score >= threshold)
                confidence = _binary_confidence(score, threshold)
                state_input[f"{prefix}_model_confidence"] = confidence
            state = _compose_state(state_input)
            rule = _mapping(_mapping(composer.get("rules")).get(state))
            predictions.append(
                {
                    "research_window_id": policy.evaluation_window.research_window_id,
                    "requested_start": policy.evaluation_window.start.isoformat(),
                    "actual_start": policy.evaluation_window.start.isoformat(),
                    "actual_portfolio_start": policy.evaluation_window.start.isoformat(),
                    "end": policy.evaluation_window.end.isoformat(),
                    "window_role": "primary_validated",
                    "data_quality_contract": "operational_forecast_extended_training_dq_required",
                    "exact_or_proxy": "exact_evaluation_with_training_only_proxy_initialization",
                    "date": session.isoformat(),
                    "model_id": policy.model_contract.model_id,
                    "trend_state": state,
                    "confidence": _compose_confidence(state_input, state),
                    "expected_horizon_days": policy.walk_forward.label_horizon_sessions,
                    "validity_days": int(rule.get("validity_days", 10)),
                    "decay_profile": str(rule.get("decay_profile", "medium")),
                    "feature_snapshot_hash": _feature_snapshot_hash(session, row),
                    "threshold_snapshot_hash": threshold_hash,
                    "model_version": policy.model_contract.model_version,
                    "producer_version": policy.policy_id,
                    "fit_id": fit_id,
                    "known_at": session.isoformat(),
                    "available_at": session.isoformat(),
                    "decision_at": _next_xnys_session(session).isoformat(),
                    "training_sample_count": train_window,
                    "latest_mature_target_available_at": max(
                        fit_row["latest_label_available_at"]
                        for fit_row in fit_rows
                        if fit_row["fit_id"] == fit_id
                    ),
                    "cash_reference_source": str(row["cash_reference_source"]),
                    "target_path_metrics_used_for_pass": False,
                    **SAFETY_BOUNDARY,
                }
            )

    prediction_frame = pd.DataFrame(predictions)
    fit_audit = pd.DataFrame(fit_rows)
    _validate_predictions(prediction_frame, evaluation_sessions, policy)
    receipt = {
        "schema_version": "first_layer_operational_forecast_receipt.v1",
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_file_sha256": loaded_policy.file_sha256,
        "data_quality_status": data_quality_status,
        "data_quality_identity_sha256": dq_identity,
        "evaluation_start": evaluation_sessions[0].isoformat(),
        "evaluation_end": evaluation_sessions[-1].isoformat(),
        "expected_session_count": len(evaluation_sessions),
        "prediction_row_count": len(prediction_frame),
        "unique_session_count": int(prediction_frame["date"].nunique()),
        "fit_count": int(fit_audit["fit_id"].nunique()),
        "all_fit_label_maturity_pass": bool(fit_audit["label_maturity_pass"].all()),
        "terminal_prediction_emitted": bool(
            prediction_frame["date"].iloc[-1] == policy.evaluation_window.end.isoformat()
        ),
        "evaluation_proxy_row_count": int(
            prediction_frame["cash_reference_source"].ne("SGOV").sum()
        ),
        "forward_label_columns_present": any(
            "label" in str(column).lower() for column in prediction_frame.columns
        ),
        "admission_status": "PASS",
        "quantconnect_status": "NOT_AUTHORIZED_NOT_RUN",
        "orders": 0,
        "fills": 0,
        "positions": 0,
        "production_effect": "none",
        "broker_action": "none",
    }
    return OperationalForecastResult(
        predictions=prediction_frame,
        fit_audit=fit_audit,
        receipt=receipt,
    )


def write_operational_forecast_artifacts(
    result: OperationalForecastResult,
    *,
    output_root: Path,
) -> tuple[Path, Path, Path]:
    if output_root.exists():
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_OUTPUT_EXISTS", str(output_root)
        )
    output_root.mkdir(parents=True, exist_ok=False)
    predictions_path = output_root / "first_layer_composer_v2_operational_predictions.csv"
    fit_audit_path = output_root / "operational_forecast_fit_audit.csv"
    receipt_path = output_root / "operational_forecast_receipt.json"
    write_bytes_atomic(
        predictions_path,
        result.predictions.to_csv(index=False, lineterminator="\n").encode("utf-8"),
    )
    write_bytes_atomic(
        fit_audit_path,
        result.fit_audit.to_csv(index=False, lineterminator="\n").encode("utf-8"),
    )
    write_bytes_atomic(
        receipt_path,
        (json.dumps(result.receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        ),
    )
    return predictions_path, fit_audit_path, receipt_path


def _build_model_prices(
    *,
    price_frame: pd.DataFrame,
    policy: OperationalForecastProducerPolicy,
    evaluation_sessions: Sequence[date],
) -> tuple[pd.DataFrame, pd.Series]:
    history_sessions = _xnys_sessions(
        policy.training_history.start, policy.evaluation_window.end
    )
    index = pd.DatetimeIndex(history_sessions)
    required = set(policy.training_history.required_price_assets)
    missing_columns = sorted(required - set(price_frame.columns))
    if missing_columns:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_PRICE_SCHEMA_INCOMPLETE", str(missing_columns)
        )
    scoped = price_frame.reindex(index)
    for asset in ("QQQ", "TQQQ", "SHY"):
        missing = int(scoped[asset].isna().sum())
        if missing:
            raise OperationalForecastError(
                "OPERATIONAL_FORECAST_TRAINING_PRICE_COVERAGE_INCOMPLETE",
                f"asset={asset}; missing={missing}",
            )
    exact_start = pd.Timestamp(policy.training_history.cash_reference.exact_asset_start)
    exact_mask = scoped.index >= exact_start
    if scoped.loc[exact_mask, "SGOV"].isna().any():
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_SGOV_EXACT_COVERAGE_INCOMPLETE",
            f"start={exact_start.date().isoformat()}",
        )
    proxy_return = scoped["SHY"].pct_change(fill_method=None)
    exact_return = scoped["SGOV"].pct_change(fill_method=None)
    use_exact = exact_mask & exact_return.notna()
    cash_return = proxy_return.where(~use_exact, exact_return).fillna(0.0)
    cash_level = (1.0 + cash_return).cumprod()
    source = pd.Series(np.where(use_exact, "SGOV", "SHY"), index=scoped.index)
    evaluation_index = pd.DatetimeIndex(list(evaluation_sessions))
    if source.reindex(evaluation_index).ne("SGOV").any():
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_PROXY_LEAKED_INTO_EVALUATION", "SHY present in evaluation"
        )
    model_prices = pd.DataFrame(
        {"QQQ": scoped["QQQ"], "SGOV": cash_level, "TQQQ": scoped["TQQQ"]},
        index=scoped.index,
    )
    return model_prices, source


def _require_rate_coverage(
    rate_frame: pd.DataFrame,
    policy: OperationalForecastProducerPolicy,
    index: pd.DatetimeIndex,
) -> None:
    required = set(policy.training_history.required_rate_series)
    missing_columns = sorted(required - set(rate_frame.columns))
    if missing_columns:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_RATE_SCHEMA_INCOMPLETE", str(missing_columns)
        )
    scoped = rate_frame.reindex(index).ffill()
    if scoped[list(required)].isna().any().any():
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_RATE_COVERAGE_INCOMPLETE", "required rate values missing"
        )


def _build_training_labels(
    loaded_policy: LoadedOperationalForecastPolicy,
    model_prices: pd.DataFrame,
) -> pd.DataFrame:
    policy = loaded_policy.policy
    window = {
        "research_window_id": "operational_forecast_training_history_v1",
        "start": policy.training_history.start.isoformat(),
        "requested_start": policy.training_history.start.isoformat(),
        "actual_start": policy.training_history.start.isoformat(),
        "actual_portfolio_start": policy.training_history.start.isoformat(),
        "end": policy.evaluation_window.end.isoformat(),
        "role": "training_only_initialization",
        "data_quality_contract": "explicit_shy_sgov_cash_reference_proxy_training_only",
        "exact_or_proxy": "mixed_training_only",
    }
    score_policy = dict(loaded_policy.authority_payloads["action_value_policy"])
    score_policy["horizons"] = [policy.walk_forward.label_horizon_sessions]
    action_value = build_upper_state_action_value_matrix_v2(
        windows=[window],
        prices=model_prices,
        probe_registry=loaded_policy.authority_payloads["frozen_probe_registry"],
        score_policy=score_policy,
    )
    labels = build_upper_state_labels_v2(
        action_value=action_value,
        taxonomy=loaded_policy.authority_payloads["label_taxonomy"],
        score_policy=score_policy,
    )
    return labels.loc[
        labels["horizon_days"].astype(int) == policy.walk_forward.label_horizon_sessions
    ].copy()


def _build_training_table(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    sessions: tuple[date, ...],
    label_horizon: int,
    exact_cash_start: date,
) -> pd.DataFrame:
    feature_rows = features.reset_index(names="timestamp")
    feature_rows["date"] = feature_rows["timestamp"].dt.date.astype(str)
    merged = feature_rows.merge(labels, on="date", how="inner", suffixes=("", "_label"))
    position = {session: idx for idx, session in enumerate(sessions)}
    maturity: list[str] = []
    proxy_in_horizon: list[bool] = []
    for value in merged["date"]:
        session = date.fromisoformat(str(value))
        end_position = position[session] + label_horizon
        if end_position >= len(sessions):
            raise OperationalForecastError(
                "OPERATIONAL_FORECAST_LABEL_MATURITY_OUT_OF_RANGE", str(value)
            )
        label_end = sessions[end_position]
        maturity.append(label_end.isoformat())
        proxy_in_horizon.append(session < exact_cash_start)
    merged["label_available_at"] = maturity
    merged["cash_proxy_in_label_horizon"] = proxy_in_horizon
    return merged.sort_values("date", kind="stable").reset_index(drop=True)


def _validate_predictions(
    frame: pd.DataFrame,
    sessions: Sequence[date],
    policy: OperationalForecastProducerPolicy,
) -> None:
    expected = tuple(session.isoformat() for session in sessions)
    observed = tuple(frame["date"].astype(str))
    if observed != expected or len(frame) != len(set(observed)):
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_SESSION_COVERAGE_INVALID",
            f"rows={len(frame)}; unique={len(set(observed))}",
        )
    if not set(frame["trend_state"].astype(str)) <= set(policy.model_contract.allowed_states):
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_STATE_INVALID", "unknown trend_state"
        )
    if any("label" in str(column).lower() for column in frame.columns):
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_FORWARD_LABEL_COLUMN_PRESENT", "prediction output"
        )
    expected_decisions = tuple(_next_xnys_session(session).isoformat() for session in sessions)
    if tuple(frame["decision_at"].astype(str)) != expected_decisions:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_NEXT_XNYS_INVALID", "decision_at mismatch"
        )


def _normalize_frame(frame: pd.DataFrame, role: str) -> pd.DataFrame:
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_INDEX_INVALID", f"{role} index must be DatetimeIndex"
        )
    normalized = frame.copy()
    normalized.index = pd.DatetimeIndex(normalized.index).tz_localize(None).normalize()
    if normalized.index.has_duplicates or not normalized.index.is_monotonic_increasing:
        raise OperationalForecastError(
            "OPERATIONAL_FORECAST_INDEX_INVALID", f"{role} index duplicate or unsorted"
        )
    return normalized


def _xnys_sessions(start: date, end: date) -> tuple[date, ...]:
    values: list[date] = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


def _next_xnys_session(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _feature_snapshot_hash(session: date, row: pd.Series) -> str:
    return _canonical_sha256(
        {
            "date": session.isoformat(),
            "features": {column: round(float(row[column]), 8) for column in V3_FEATURE_COLUMNS},
        }
    )


def _model_specs_sha256() -> str:
    content = (
        json.dumps(MODEL_SPECS, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _canonical_sha256(value: object) -> str:
    content = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _json_number(value: float) -> float | str:
    return "Infinity" if math.isinf(value) else round(value, GRID_ROUND_DIGITS)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _bound_file(path: Path, project_root: Path) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path escapes project root: {path}") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"path must be a non-symlink regular file: {path}")
    return resolved
