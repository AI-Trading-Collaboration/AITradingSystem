from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, Self, cast

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_operational_forecast import (
    LoadedOperationalForecastPolicy,
    OperationalForecastError,
    OperationalForecastProducerPolicy,
    _build_training_table,
    _canonical_sha256,
    _feature_snapshot_hash,
    _json_number,
    _mapping,
    _next_xnys_session,
    _normalize_frame,
    _require_rate_coverage,
    _xnys_sessions,
    load_operational_forecast_policy,
)
from ai_trading_system.first_layer_policy_calibration import SAFETY_BOUNDARY
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.upper_state_label_feature_reset import (
    MODEL_SPECS,
    _binary_confidence,
    _compose_confidence,
    _compose_state,
    _compute_feature_frame,
    _score_rows,
    build_upper_state_action_value_matrix_v2,
    build_upper_state_labels_v2,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_CURRENT_SESSION_PRODUCER_POLICY_PATH = Path(
    "config/research/first_layer_composer_v2_current_session_producer_v1.yaml"
)

_SHA256_CHARS = frozenset("0123456789abcdef")
_HISTORICAL_CUTOFF = date(2025, 12, 2)


class CurrentSessionProducerError(ValueError):
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


class PolicyMetadata(_StrictModel):
    rationale: str
    intended_effect: str
    validation_evidence: str
    review_condition: str


class AuthorityBinding(_StrictModel):
    path: str
    sha256: str

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return _sha256(value, "authority sha256")


class AuthorityBindings(_StrictModel):
    frozen_operational_policy: AuthorityBinding
    prospective_oos_preregistration: AuthorityBinding


class SessionContract(_StrictModel):
    calendar: Literal["XNYS"]
    historical_cutoff: date
    feature_session_role: Literal["CALLER_SUPPLIED_COMPLETED_XNYS_SESSION"]
    feature_session_must_follow_historical_cutoff: Literal[True]
    input_rows_after_feature_session_allowed: Literal[False]
    future_label_access_allowed: Literal[False]
    label_maturity_rule: Literal["LABEL_END_SESSION_ON_OR_BEFORE_FEATURE_SESSION"]
    output_row_count: Literal[1]
    decision_at_semantics: Literal["NEXT_VALID_XNYS_SESSION"]

    @model_validator(mode="after")
    def validate_cutoff(self) -> Self:
        if self.historical_cutoff != _HISTORICAL_CUTOFF:
            raise ValueError("historical cutoff drifted")
        return self


class FitContract(_StrictModel):
    source: Literal["INHERIT_FROZEN_OPERATIONAL_POLICY_EXACTLY"]
    refit_rule: Literal["FIT_ON_EACH_REQUESTED_FEATURE_SESSION"]
    cross_session_state_reuse_allowed: Literal[False]


class OutputContract(_StrictModel):
    schema_version: Literal["first_layer_composer_v2_current_session_preview.v1"]
    readiness_status: Literal["SAFE_PREVIEW_READY"]
    observation_identity_preview_required: Literal[True]
    observation_write_allowed: Literal[False]
    artifact_write_allowed: Literal[False]
    automatic_capture_allowed: Literal[False]
    forward_label_columns_allowed: Literal[False]


class CaptureBoundary(_StrictModel):
    prospective_start_frozen: Literal[False]
    exact_manifest_replay_completed: Literal[False]
    canonical_dq_run_completed: Literal[False]
    exact_capture_authorization_present: Literal[False]
    first_real_observation_allowed: Literal[False]


class SafetyPolicy(_StrictModel):
    scope: Literal["NON_EXECUTABLE_RESULT_BLIND_PREVIEW"]
    market_data_read_by_producer: Literal[False]
    real_cache_materialization_allowed: Literal[False]
    data_download_allowed: Literal[False]
    cache_mutation_allowed: Literal[False]
    provider_query_allowed: Literal[False]
    quantconnect_allowed: Literal[False]
    option_backtest_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class CurrentSessionProducerPolicy(_StrictModel):
    schema_version: Literal["first_layer_composer_v2_current_session_producer_policy.v1"]
    policy_id: Literal["first_layer_composer_v2_current_session_producer_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_APPROVED_RESULT_BLIND_IMPLEMENTATION_ONLY"]
    owner: Literal["project_owner"]
    owner_decision_id: str
    policy_metadata: PolicyMetadata
    authority_bindings: AuthorityBindings
    session_contract: SessionContract
    fit_contract: FitContract
    output_contract: OutputContract
    capture_boundary: CaptureBoundary
    safety: SafetyPolicy


@dataclass(frozen=True)
class LoadedCurrentSessionProducerPolicy:
    policy: CurrentSessionProducerPolicy
    path: Path
    file_sha256: str
    frozen_operational_policy: LoadedOperationalForecastPolicy
    prospective_preregistration: Mapping[str, Any]


@dataclass(frozen=True)
class CurrentSessionPreviewResult:
    preview: Mapping[str, Any]
    fit_audit: pd.DataFrame
    receipt: Mapping[str, Any]


def load_current_session_producer_policy(
    path: Path = DEFAULT_CURRENT_SESSION_PRODUCER_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedCurrentSessionProducerPolicy:
    try:
        resolved = _bound_file(path, project_root)
        content = resolved.read_bytes()
        raw = safe_load_yaml_path(resolved)
        if not isinstance(raw, dict):
            raise TypeError("policy root must be a mapping")
        policy = CurrentSessionProducerPolicy.model_validate(raw, strict=False)
        frozen_path = _validate_authority(
            policy.authority_bindings.frozen_operational_policy,
            project_root=project_root,
        )
        preregistration_path = _validate_authority(
            policy.authority_bindings.prospective_oos_preregistration,
            project_root=project_root,
        )
        frozen = load_operational_forecast_policy(
            Path(policy.authority_bindings.frozen_operational_policy.path),
            project_root=project_root,
        )
        preregistration_raw = safe_load_yaml_path(preregistration_path)
        if not isinstance(preregistration_raw, dict):
            raise TypeError("prospective preregistration root must be a mapping")
        _validate_result_blind_preregistration(preregistration_raw)
        if frozen.path != frozen_path:
            raise ValueError("frozen operational policy path binding drifted")
    except (OSError, TypeError, ValueError, OperationalForecastError) as exc:
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_POLICY_INVALID", str(exc)
        ) from exc
    return LoadedCurrentSessionProducerPolicy(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(content).hexdigest(),
        frozen_operational_policy=frozen,
        prospective_preregistration=preregistration_raw,
    )


def build_current_session_preview(
    *,
    loaded_policy: LoadedCurrentSessionProducerPolicy,
    feature_session: date,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    data_quality_status: str,
    dq_receipt_sha256: str,
    source_sha256: str,
) -> CurrentSessionPreviewResult:
    if data_quality_status != "PASS":
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_DQ_NOT_PASS", f"observed={data_quality_status}"
        )
    try:
        dq_identity = _sha256(dq_receipt_sha256, "dq_receipt_sha256")
        source_identity = _sha256(source_sha256, "source_sha256")
    except ValueError as exc:
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_IDENTITY_INVALID", str(exc)
        ) from exc

    cutoff = loaded_policy.policy.session_contract.historical_cutoff
    if feature_session <= cutoff:
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_NOT_PROSPECTIVE",
            f"feature_session={feature_session.isoformat()}; cutoff={cutoff.isoformat()}",
        )
    if not is_us_equity_trading_day(feature_session):
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_FEATURE_SESSION_INVALID",
            feature_session.isoformat(),
        )

    price_frame = _normalize_current_visible_frame(prices, "prices", feature_session)
    rate_frame = _normalize_current_visible_frame(rates, "rates", feature_session)
    frozen = loaded_policy.frozen_operational_policy
    base_policy = frozen.policy
    try:
        model_prices, cash_source = _build_current_model_prices(
            price_frame=price_frame,
            policy=base_policy,
            evaluation_sessions=(feature_session,),
            feature_session=feature_session,
        )
        _require_rate_coverage(rate_frame, base_policy, pd.DatetimeIndex(model_prices.index))
        features = _compute_feature_frame(model_prices, rate_frame).copy()
        features["feature_warmup_complete"] = np.arange(len(features)) >= (
            base_policy.training_history.feature_warmup_sessions - 1
        )
        features["cash_reference_source"] = cash_source.reindex(features.index)
        labels = _build_current_training_labels(
            frozen,
            model_prices,
            feature_session=feature_session,
        )
        training = _build_training_table(
            features=features,
            labels=labels,
            sessions=tuple(timestamp.date() for timestamp in model_prices.index),
            label_horizon=base_policy.walk_forward.label_horizon_sessions,
            exact_cash_start=base_policy.training_history.cash_reference.exact_asset_start,
        )
        model_scores = {
            model_id: _score_rows(features, _mapping(spec.get("feature_weights")))
            for model_id, spec in MODEL_SPECS.items()
        }
        training_scores = {
            model_id: model_scores[model_id].reindex(pd.to_datetime(training["date"])).to_numpy()
            for model_id in MODEL_SPECS
        }
        threshold_policy = frozen.authority_payloads["threshold_policy"]
        positive_quantile = float(
            _mapping(threshold_policy.get("threshold_selection")).get(
                "positive_score_quantile", 0.65
            )
        )
        positive_sample_floors = _mapping(_mapping(threshold_policy).get("positive_sample_floor"))
        fit_id = f"prospective_current_session_{feature_session:%Y%m%d}"
        thresholds, fit_rows = _fit_current_model_thresholds(
            fit_session=feature_session,
            fit_id=fit_id,
            training=training,
            training_scores=training_scores,
            train_window=base_policy.walk_forward.train_window_sessions,
            positive_quantile=positive_quantile,
            positive_sample_floors=positive_sample_floors,
        )
    except OperationalForecastError as exc:
        raise CurrentSessionProducerError(exc.reason_code, exc.detail) from exc

    timestamp = pd.Timestamp(feature_session)
    feature_row = cast(pd.Series, features.loc[timestamp])
    state_input: dict[str, Any] = {}
    for model_id, spec in MODEL_SPECS.items():
        prefix = str(spec["label_column"]).removesuffix("_label")
        score = float(model_scores[model_id].loc[timestamp])
        threshold = thresholds[model_id]
        state_input[f"{prefix}_pred"] = bool(score >= threshold)
        state_input[f"{prefix}_model_confidence"] = _binary_confidence(score, threshold)
    state = _compose_state(state_input)
    composer = frozen.authority_payloads["composer_policy"]
    rule = _mapping(_mapping(composer.get("rules")).get(state))
    confidence = _compose_confidence(state_input, state)
    decision_date = _next_xnys_session(feature_session)
    feature_snapshot_sha256 = _feature_snapshot_hash(feature_session, feature_row)
    threshold_snapshot_sha256 = _canonical_sha256(
        {model_id: _json_number(value) for model_id, value in thresholds.items()}
    )
    action = _action_for_state(loaded_policy.prospective_preregistration, state)
    signal_payload = {
        "schema_version": "first_layer_composer_v2_current_session_signal.v1",
        "feature_session": feature_session.isoformat(),
        "decision_date": decision_date.isoformat(),
        "trend_state": state,
        "confidence": confidence,
        "action": action,
        "feature_snapshot_sha256": feature_snapshot_sha256,
        "threshold_snapshot_sha256": threshold_snapshot_sha256,
        "model_sha256": base_policy.model_contract.model_specs_sha256,
    }
    observation_identity_preview = {
        "feature_snapshot_sha256": feature_snapshot_sha256,
        "signal_sha256": _canonical_sha256(signal_payload),
        "model_sha256": base_policy.model_contract.model_specs_sha256,
        "policy_sha256": loaded_policy.file_sha256,
        "dq_receipt_sha256": dq_identity,
        "source_sha256": source_identity,
    }
    latest_mature_target = max(str(row["latest_label_available_at"]) for row in fit_rows)
    preview = {
        "schema_version": loaded_policy.policy.output_contract.schema_version,
        "producer_id": loaded_policy.policy.policy_id,
        "producer_version": loaded_policy.policy.policy_version,
        "status": loaded_policy.policy.output_contract.readiness_status,
        "feature_session": feature_session.isoformat(),
        "decision_date": decision_date.isoformat(),
        "trend_state": state,
        "confidence": confidence,
        "action": action,
        "validity_days": int(rule.get("validity_days", 10)),
        "decay_profile": str(rule.get("decay_profile", "medium")),
        "fit_id": fit_id,
        "training_sample_count": base_policy.walk_forward.train_window_sessions,
        "latest_mature_target_available_at": latest_mature_target,
        "cash_reference_source": str(feature_row["cash_reference_source"]),
        "threshold_snapshot_sha256": threshold_snapshot_sha256,
        "observation_identity_preview": observation_identity_preview,
        "forward_label_columns_present": False,
        **SAFETY_BOUNDARY,
    }
    receipt = {
        "schema_version": "first_layer_composer_v2_current_session_preview_receipt.v1",
        "status": "SAFE_PREVIEW_READY",
        "policy_id": loaded_policy.policy.policy_id,
        "policy_file_sha256": loaded_policy.file_sha256,
        "frozen_operational_policy_sha256": frozen.file_sha256,
        "feature_session": feature_session.isoformat(),
        "decision_date": decision_date.isoformat(),
        "output_row_count": 1,
        "fit_model_count": len(fit_rows),
        "training_sample_count": base_policy.walk_forward.train_window_sessions,
        "latest_mature_target_available_at": latest_mature_target,
        "label_maturity_pass": date.fromisoformat(latest_mature_target) <= feature_session,
        "input_max_price_date": price_frame.index.max().date().isoformat(),
        "input_max_rate_date": rate_frame.index.max().date().isoformat(),
        "future_input_row_count": 0,
        "caller_supplied_data_quality_status": data_quality_status,
        "canonical_dq_run_count": 0,
        "market_data_read_count": 0,
        "prospective_capture_count": 0,
        "observation_write_count": 0,
        "maturity_update_count": 0,
        "data_download_count": 0,
        "cache_mutation_count": 0,
        "provider_action_count": 0,
        "quantconnect_action_count": 0,
        "option_backtest_count": 0,
        "orders": 0,
        "fills": 0,
        "positions": 0,
        "paper_allowed": False,
        "live_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return CurrentSessionPreviewResult(
        preview=preview,
        fit_audit=pd.DataFrame(fit_rows),
        receipt=receipt,
    )


def _build_current_model_prices(
    *,
    price_frame: pd.DataFrame,
    policy: OperationalForecastProducerPolicy,
    evaluation_sessions: tuple[date, ...],
    feature_session: date,
) -> tuple[pd.DataFrame, pd.Series]:
    history_sessions = _xnys_sessions(policy.training_history.start, feature_session)
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
            "OPERATIONAL_FORECAST_PROXY_LEAKED_INTO_EVALUATION",
            "SHY present in evaluation",
        )
    model_prices = pd.DataFrame(
        {"QQQ": scoped["QQQ"], "SGOV": cash_level, "TQQQ": scoped["TQQQ"]},
        index=scoped.index,
    )
    return model_prices, source


def _build_current_training_labels(
    loaded_policy: LoadedOperationalForecastPolicy,
    model_prices: pd.DataFrame,
    *,
    feature_session: date,
) -> pd.DataFrame:
    policy = loaded_policy.policy
    window = {
        "research_window_id": "prospective_current_session_training_history_v1",
        "start": policy.training_history.start.isoformat(),
        "requested_start": policy.training_history.start.isoformat(),
        "actual_start": policy.training_history.start.isoformat(),
        "actual_portfolio_start": policy.training_history.start.isoformat(),
        "end": feature_session.isoformat(),
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


def _fit_current_model_thresholds(
    *,
    fit_session: date,
    fit_id: str,
    training: pd.DataFrame,
    training_scores: Mapping[str, np.ndarray],
    train_window: int,
    positive_quantile: float,
    positive_sample_floors: Mapping[str, Any],
) -> tuple[dict[str, float], list[dict[str, Any]]]:
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
    fit_rows: list[dict[str, Any]] = []
    for model_id, spec in MODEL_SPECS.items():
        label_column = str(spec["label_column"])
        floor = int(positive_sample_floors.get(model_id, 0))
        scores = pd.Series(training_scores[model_id][selected_positions], dtype=float)
        positive_mask = selected[label_column].astype(bool).reset_index(drop=True)
        positives = scores.loc[positive_mask]
        threshold = (
            math.inf if len(positives) < floor else float(positives.quantile(positive_quantile))
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
                "sample_status": ("PASS" if not math.isinf(threshold) else "SAMPLE_INSUFFICIENT"),
                "proxy_training_sample_count": int(
                    selected["cash_proxy_in_label_horizon"].astype(bool).sum()
                ),
                "label_maturity_pass": bool(
                    selected["label_available_at"].map(date.fromisoformat).max() <= fit_session
                ),
            }
        )
    return thresholds, fit_rows


def _normalize_current_visible_frame(
    frame: pd.DataFrame,
    role: str,
    feature_session: date,
) -> pd.DataFrame:
    normalized = _normalize_frame(frame, role)
    future_rows = normalized.index > pd.Timestamp(feature_session)
    if future_rows.any():
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_FUTURE_INPUT_PRESENT",
            f"role={role}; rows={int(future_rows.sum())}",
        )
    if normalized.empty or normalized.index.max().date() != feature_session:
        raise CurrentSessionProducerError(
            "CURRENT_SESSION_PRODUCER_TARGET_INPUT_MISSING",
            f"role={role}; feature_session={feature_session.isoformat()}",
        )
    return normalized


def _action_for_state(preregistration: Mapping[str, Any], state: str) -> str:
    freeze = _mapping(preregistration.get("freeze_contract"))
    long_states = {str(value) for value in freeze.get("long_states", ())}
    flat_states = {str(value) for value in freeze.get("flat_states", ())}
    if state in long_states:
        return "LONG_QQQ"
    if state in flat_states:
        return "FLAT_CASH"
    raise CurrentSessionProducerError("CURRENT_SESSION_PRODUCER_STATE_MAPPING_INVALID", state)


def _validate_result_blind_preregistration(payload: Mapping[str, Any]) -> None:
    freeze = _mapping(payload.get("freeze_contract"))
    envelope = _mapping(payload.get("run_envelope"))
    if payload.get("policy_status") != "RESULT_BLIND_CONTRACT_NOT_YET_CAPTURING":
        raise ValueError("prospective preregistration is not result blind")
    if freeze.get("prospective_start_date") is not None:
        raise ValueError("prospective start is unexpectedly frozen")
    if envelope.get("prospective_captures") != 0:
        raise ValueError("prospective capture envelope drifted")


def _validate_authority(binding: AuthorityBinding, *, project_root: Path) -> Path:
    resolved = _bound_file(Path(binding.path), project_root)
    if sha256_path(resolved) != binding.sha256:
        raise ValueError(f"authority drifted: {binding.path}")
    return resolved


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
