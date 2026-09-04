from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

import ai_trading_system.first_layer_composer_v2_current_session_producer as producer
from ai_trading_system.first_layer_composer_v2_current_session_producer import (
    CurrentSessionPreviewResult,
    CurrentSessionProducerError,
    build_current_session_preview,
    load_current_session_producer_policy,
)
from ai_trading_system.first_layer_composer_v2_prospective_oos import (
    ActivatedObservationContract,
    create_observation,
)
from ai_trading_system.first_layer_operational_forecast import (
    _next_xnys_session,
    _xnys_sessions,
)

_FEATURE_SESSION = date(2026, 9, 4)


def test_policy_inherits_frozen_model_and_keeps_capture_closed() -> None:
    loaded = load_current_session_producer_policy()
    policy = loaded.policy
    frozen = loaded.frozen_operational_policy.policy

    assert policy.session_contract.historical_cutoff == date(2025, 12, 2)
    assert policy.output_contract.readiness_status == "SAFE_PREVIEW_READY"
    assert policy.output_contract.observation_write_allowed is False
    assert policy.capture_boundary.first_real_observation_allowed is False
    assert frozen.walk_forward.train_window_sessions == 504
    assert frozen.walk_forward.label_horizon_sessions == 20
    assert frozen.walk_forward.refit_step_sessions == 21
    assert frozen.model_contract.model_id == "first_layer_composer_v2"


def test_current_session_preview_is_single_row_current_visible_and_mature_label_only(
    current_session_result: CurrentSessionPreviewResult,
) -> None:
    preview = current_session_result.preview
    receipt = current_session_result.receipt
    audit = current_session_result.fit_audit

    assert preview["status"] == "SAFE_PREVIEW_READY"
    assert preview["feature_session"] == "2026-09-04"
    assert preview["decision_date"] == "2026-09-08"
    assert preview["action"] in {"LONG_QQQ", "FLAT_CASH"}
    assert preview["forward_label_columns_present"] is False
    assert receipt["output_row_count"] == 1
    assert receipt["input_max_price_date"] == receipt["feature_session"]
    assert receipt["input_max_rate_date"] == receipt["feature_session"]
    assert receipt["fit_model_count"] == 4
    assert len(audit) == 4
    assert set(audit["train_sample_count"]) == {504}
    assert audit["label_maturity_pass"].all()
    assert all(
        date.fromisoformat(value) <= _FEATURE_SESSION
        for value in audit["latest_label_available_at"]
    )


def test_preview_exposes_complete_deterministic_observation_identity(
    current_session_result: CurrentSessionPreviewResult,
) -> None:
    identities = current_session_result.preview["observation_identity_preview"]

    assert set(identities) == {
        "feature_snapshot_sha256",
        "signal_sha256",
        "model_sha256",
        "policy_sha256",
        "dq_receipt_sha256",
        "source_sha256",
    }
    assert all(
        isinstance(value, str) and len(value) == 64 and set(value) <= set("0123456789abcdef")
        for value in identities.values()
    )


def test_preview_identity_is_compatible_with_append_only_observation_contract(
    current_session_result: CurrentSessionPreviewResult,
) -> None:
    preview = current_session_result.preview
    decision_date = date.fromisoformat(str(preview["decision_date"]))
    contract = ActivatedObservationContract(
        policy_id="synthetic_compatibility_only",
        policy_sha256="c" * 64,
        freeze_commit="d" * 40,
        prospective_start=decision_date,
    )

    observation = create_observation(
        contract,
        decision_date=decision_date,
        trend_state=str(preview["trend_state"]),
        identities=preview["observation_identity_preview"],
        dq_status="PASS",
    )

    assert observation["decision_date"] == preview["decision_date"]
    assert observation["action"] == preview["action"]
    assert observation["matured_outcomes"] == {}


def test_future_input_row_is_rejected_before_fitting() -> None:
    prices, rates = _synthetic_inputs()
    future = _next_xnys_session(_FEATURE_SESSION)
    prices.loc[pd.Timestamp(future)] = prices.iloc[-1]

    with pytest.raises(CurrentSessionProducerError) as raised:
        _build(prices, rates)

    assert raised.value.reason_code == "CURRENT_SESSION_PRODUCER_FUTURE_INPUT_PRESENT"


def test_non_trading_or_historical_feature_session_is_rejected() -> None:
    prices, rates = _synthetic_inputs()

    with pytest.raises(CurrentSessionProducerError) as weekend:
        _build(prices, rates, feature_session=date(2026, 9, 5))
    assert weekend.value.reason_code == "CURRENT_SESSION_PRODUCER_FEATURE_SESSION_INVALID"

    with pytest.raises(CurrentSessionProducerError) as historical:
        _build(prices, rates, feature_session=date(2025, 12, 2))
    assert historical.value.reason_code == "CURRENT_SESSION_PRODUCER_NOT_PROSPECTIVE"


def test_non_pass_dq_and_invalid_identity_fail_closed_before_model_work() -> None:
    prices, rates = _synthetic_inputs()
    loaded = load_current_session_producer_policy()

    with pytest.raises(CurrentSessionProducerError) as dq_error:
        build_current_session_preview(
            loaded_policy=loaded,
            feature_session=_FEATURE_SESSION,
            prices=prices,
            rates=rates,
            data_quality_status="PASS_WITH_WARNINGS",
            dq_receipt_sha256="a" * 64,
            source_sha256="b" * 64,
        )
    assert dq_error.value.reason_code == "CURRENT_SESSION_PRODUCER_DQ_NOT_PASS"

    with pytest.raises(CurrentSessionProducerError) as identity_error:
        build_current_session_preview(
            loaded_policy=loaded,
            feature_session=_FEATURE_SESSION,
            prices=prices,
            rates=rates,
            data_quality_status="PASS",
            dq_receipt_sha256="not-a-hash",
            source_sha256="b" * 64,
        )
    assert identity_error.value.reason_code == "CURRENT_SESSION_PRODUCER_IDENTITY_INVALID"


def test_insufficient_history_fails_closed() -> None:
    prices, rates = _synthetic_inputs(start=date(2020, 1, 2))

    with pytest.raises(CurrentSessionProducerError) as raised:
        _build(prices, rates)

    assert raised.value.reason_code in {
        "OPERATIONAL_FORECAST_TRAINING_PRICE_COVERAGE_INCOMPLETE",
        "OPERATIONAL_FORECAST_TRAINING_HISTORY_INSUFFICIENT",
    }


def test_preview_has_no_writer_and_all_forbidden_action_counts_are_zero(
    current_session_result: CurrentSessionPreviewResult,
) -> None:
    receipt = current_session_result.receipt

    assert not hasattr(producer, "write_current_session_preview")
    assert receipt["canonical_dq_run_count"] == 0
    assert receipt["market_data_read_count"] == 0
    assert receipt["prospective_capture_count"] == 0
    assert receipt["observation_write_count"] == 0
    assert receipt["maturity_update_count"] == 0
    assert receipt["data_download_count"] == 0
    assert receipt["cache_mutation_count"] == 0
    assert receipt["provider_action_count"] == 0
    assert receipt["quantconnect_action_count"] == 0
    assert receipt["option_backtest_count"] == 0
    assert receipt["orders"] == receipt["fills"] == receipt["positions"] == 0
    assert receipt["production_effect"] == "none"
    assert receipt["broker_action"] == "none"


@pytest.fixture(scope="module")
def current_session_result() -> CurrentSessionPreviewResult:
    prices, rates = _synthetic_inputs()
    return _build(prices, rates)


def _build(
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    *,
    feature_session: date = _FEATURE_SESSION,
) -> CurrentSessionPreviewResult:
    return build_current_session_preview(
        loaded_policy=load_current_session_producer_policy(),
        feature_session=feature_session,
        prices=prices,
        rates=rates,
        data_quality_status="PASS",
        dq_receipt_sha256="a" * 64,
        source_sha256="b" * 64,
    )


def _synthetic_inputs(
    *,
    start: date = date(2018, 1, 2),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sessions = _xnys_sessions(start, _FEATURE_SESSION)
    index = pd.DatetimeIndex(sessions)
    ordinal = np.arange(len(index), dtype=float)

    def levels(drift: float, cycle: float) -> np.ndarray:
        returns = drift + 0.0015 * np.sin(ordinal / cycle)
        return 100.0 * np.cumprod(1.0 + returns)

    prices = pd.DataFrame(
        {
            "QQQ": levels(0.00045, 17.0),
            "TQQQ": levels(0.00110, 13.0),
            "SHY": levels(0.00005, 29.0),
            "SGOV": levels(0.00004, 31.0),
        },
        index=index,
    )
    prices.loc[prices.index < pd.Timestamp("2020-05-28"), "SGOV"] = np.nan
    rates = pd.DataFrame(
        {
            "DGS10": 2.5 + 0.4 * np.sin(ordinal / 43.0),
            "DGS2": 1.8 + 0.3 * np.sin(ordinal / 37.0),
            "DTWEXBGS": 100.0 + 2.0 * np.sin(ordinal / 51.0),
        },
        index=index,
    )
    return prices, rates
