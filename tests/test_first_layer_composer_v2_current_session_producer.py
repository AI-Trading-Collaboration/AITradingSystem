from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest

import ai_trading_system.first_layer_composer_v2_current_session_producer as producer
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_composer_v2_current_session_producer import (
    CurrentSessionPreviewResult,
    CurrentSessionProducerError,
    build_current_session_preview,
    build_known_snapshot_preview,
    load_current_session_producer_policy,
)
from ai_trading_system.first_layer_composer_v2_prospective_oos import (
    ActivatedObservationContract,
    create_observation,
)
from ai_trading_system.first_layer_operational_forecast import (
    _canonical_sha256,
    _next_xnys_session,
    _xnys_sessions,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

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


def test_known_snapshot_same_day_preserves_frozen_computation_and_fit(
    current_session_result: CurrentSessionPreviewResult,
    known_snapshot_result: CurrentSessionPreviewResult,
) -> None:
    old = current_session_result
    new = known_snapshot_result
    identity_keys = {"schema_version", "producer_id", "observation_identity_preview"}
    for key, value in old.preview.items():
        if key not in identity_keys:
            assert new.preview[key] == value, key
    pd.testing.assert_frame_equal(old.fit_audit, new.fit_audit)
    old_ids = old.preview["observation_identity_preview"]
    new_ids = new.preview["observation_identity_preview"]
    assert new_ids["feature_snapshot_sha256"] == old_ids["feature_snapshot_sha256"]
    assert new_ids["signal_sha256"] != old_ids["signal_sha256"]
    assert new_ids["policy_sha256"] == hashlib.sha256(_input_policy_bytes()).hexdigest()
    assert new.receipt["schema_version"] == (
        "first_layer_composer_v2_known_snapshot_preview_receipt.v1"
    )
    assert new.preview["schema_version"] == "first_layer_composer_v2_known_snapshot_preview.v1"
    assert new.receipt["frozen_current_session_policy_sha256"] == old_ids["policy_sha256"]


def test_known_snapshot_same_day_discloses_no_carry_and_no_capture_authority(
    known_snapshot_result: CurrentSessionPreviewResult,
) -> None:
    for container in (known_snapshot_result.preview, known_snapshot_result.receipt):
        assert container["status"] == "SAFE_PREVIEW_READY"
        assert container["historical_training_access"] == {
            "accessed": True,
            "role": "CURRENT_REVISION_PREQUENTIAL_TRAINING",
            "label_construction_input_start": "2018-01-02",
            "label_construction_input_end": "2026-09-04",
            "fit_sample_count": 504,
            "label_horizon_xnys_sessions": 20,
            "historical_provider_available_at": "NOT_ESTABLISHED",
        }
        assert container["historical_pit_claim_allowed"] is False
        assert container["future_observation_outcome_access"] is False
        assert container["pure_preview_grants_capture_authority"] is False
        assert container["actual_input_capture_time_established"] is False
        assert "R" not in container and "captured_at" not in container
        for row in container["rate_disclosures"]:
            assert row["raw_last_valid_observation_date"] == "2026-09-04"
            assert row["effective_carry_source_date"] == "2026-09-04"
            assert row["raw_lag_calendar_days"] == row["effective_lag_calendar_days"] == 0
            assert row["carry_applied"] is False
            assert row["input_content_sha256"] == container["normalized_rates_sha256"]
    receipt = known_snapshot_result.receipt
    for key, value in receipt.items():
        if key.endswith("_count") and key not in {
            "output_row_count",
            "fit_model_count",
            "training_sample_count",
        }:
            assert value == 0, key
    assert receipt["orders"] == receipt["fills"] == receipt["positions"] == 0


def test_known_snapshot_accepts_real_earlier_rates_without_changing_inputs() -> None:
    prices, rates = _synthetic_inputs()
    rates = rates.iloc[:-1].copy()
    original_prices, original_rates = prices.copy(deep=True), rates.copy(deep=True)
    with pytest.raises(CurrentSessionProducerError) as old:
        _build(prices, rates)
    assert old.value.reason_code == "CURRENT_SESSION_PRODUCER_TARGET_INPUT_MISSING"
    result = _build_known(prices, rates)
    assert result.receipt["input_max_rate_date"] == "2026-09-03"
    assert result.receipt["input_max_price_date"] == "2026-09-04"
    assert result.fit_audit["label_maturity_pass"].all()
    for row in result.receipt["rate_disclosures"]:
        assert row["raw_last_valid_observation_date"] == "2026-09-03"
        assert row["effective_carry_source_date"] == "2026-09-03"
        assert row["raw_lag_calendar_days"] == row["effective_lag_calendar_days"] == 1
        assert row["carry_applied"] is True
    pd.testing.assert_frame_equal(prices, original_prices)
    pd.testing.assert_frame_equal(rates, original_rates)


def test_known_snapshot_tracks_per_series_raw_and_effective_sources() -> None:
    prices, rates = _synthetic_inputs()
    rates.loc[rates.index > pd.Timestamp("2026-08-28"), "DGS10"] = np.nan
    rates.loc[pd.Timestamp("2026-08-30")] = [4.0, np.nan, np.nan]
    rates.loc[pd.Timestamp("2026-09-04"), "DGS2"] = np.nan
    rates = rates.sort_index()
    original = rates.copy(deep=True)
    result = _build_known(prices, rates)
    rows = {row["series"]: row for row in result.receipt["rate_disclosures"]}
    assert rows["DGS10"]["raw_last_valid_observation_date"] == "2026-08-30"
    assert rows["DGS10"]["effective_carry_source_date"] == "2026-08-28"
    assert rows["DGS10"]["raw_lag_calendar_days"] == 5
    assert rows["DGS10"]["effective_lag_calendar_days"] == 7
    assert rows["DGS2"]["effective_carry_source_date"] == "2026-09-03"
    assert rows["DTWEXBGS"]["carry_applied"] is False
    without_off_calendar = _build_known(prices, rates.drop(pd.Timestamp("2026-08-30")))
    assert result.preview["threshold_snapshot_sha256"] == (
        without_off_calendar.preview["threshold_snapshot_sha256"]
    )
    assert result.preview["observation_identity_preview"]["feature_snapshot_sha256"] == (
        without_off_calendar.preview["observation_identity_preview"]["feature_snapshot_sha256"]
    )
    assert result.receipt["normalized_rates_sha256"] != (
        without_off_calendar.receipt["normalized_rates_sha256"]
    )
    pd.testing.assert_frame_equal(rates, original)


def test_known_snapshot_rate_identity_binds_full_normalized_input(
    known_snapshot_result: CurrentSessionPreviewResult,
) -> None:
    _, rates = _synthetic_inputs()
    expected = _canonical_sha256(
        {
            "schema_version": "composer_known_snapshot_normalized_rates.v1",
            "columns": list(rates.columns),
            "dates": [value.date().isoformat() for value in rates.index],
            "values": rates.to_numpy().tolist(),
        }
    )
    assert known_snapshot_result.receipt["normalized_rates_sha256"] == expected


@pytest.mark.parametrize("role", ["prices", "rates"])
def test_known_snapshot_future_inputs_rejected_before_model_work(
    role: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    prices, rates = _synthetic_inputs()
    frame = prices if role == "prices" else rates
    frame.loc[pd.Timestamp(_next_xnys_session(_FEATURE_SESSION))] = frame.iloc[-1]
    _forbid_known_model_work(monkeypatch)
    with pytest.raises(CurrentSessionProducerError) as raised:
        _build_known(prices, rates)
    assert raised.value.reason_code == "CURRENT_SESSION_PRODUCER_FUTURE_INPUT_PRESENT"


@pytest.mark.parametrize(
    ("problem", "expected"),
    [
        ("empty_rates", "CURRENT_SESSION_PRODUCER_TARGET_INPUT_MISSING"),
        ("missing_price_f", "CURRENT_SESSION_PRODUCER_TARGET_INPUT_MISSING"),
        ("missing_series", "OPERATIONAL_FORECAST_RATE_SCHEMA_INCOMPLETE"),
        ("initial_rate_gap", "OPERATIONAL_FORECAST_RATE_COVERAGE_INCOMPLETE"),
        ("all_missing_series", "OPERATIONAL_FORECAST_RATE_COVERAGE_INCOMPLETE"),
        ("infinite_rate", "KNOWN_SNAPSHOT_PRODUCER_INPUT_VALUES_INVALID"),
        ("infinite_price", "KNOWN_SNAPSHOT_PRODUCER_INPUT_VALUES_INVALID"),
        ("text_rate", "KNOWN_SNAPSHOT_PRODUCER_INPUT_VALUES_INVALID"),
        ("duplicate_rate_column", "KNOWN_SNAPSHOT_PRODUCER_INPUT_SCHEMA_INVALID"),
        ("duplicate_date", "OPERATIONAL_FORECAST_INDEX_INVALID"),
    ],
)
def test_known_snapshot_invalid_inputs_rejected_before_model_work(
    problem: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    prices, rates = _synthetic_inputs()
    if problem == "empty_rates":
        rates = rates.iloc[:0]
    elif problem == "missing_price_f":
        prices = prices.iloc[:-1]
    elif problem == "missing_series":
        rates = rates.drop(columns="DGS2")
    elif problem == "initial_rate_gap":
        rates.iloc[0, 0] = np.nan
    elif problem == "all_missing_series":
        rates["DGS2"] = np.nan
    elif problem == "infinite_rate":
        rates.iloc[-1, 0] = np.inf
    elif problem == "infinite_price":
        prices.iloc[-1, 0] = -np.inf
    elif problem == "text_rate":
        rates["DGS2"] = rates["DGS2"].astype(str)
    elif problem == "duplicate_rate_column":
        rates = pd.concat([rates, rates[["DGS2"]]], axis=1)
    elif problem == "duplicate_date":
        rates = pd.concat([rates, rates.iloc[[-1]]])
    _forbid_known_model_work(monkeypatch)
    with pytest.raises(CurrentSessionProducerError) as raised:
        _build_known(prices, rates)
    assert raised.value.reason_code == expected


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"data_quality_status": "PASS_WITH_WARNINGS"}, "CURRENT_SESSION_PRODUCER_DQ_NOT_PASS"),
        ({"dq_receipt_sha256": "invalid"}, "CURRENT_SESSION_PRODUCER_IDENTITY_INVALID"),
        ({"source_sha256": "B" * 64}, "CURRENT_SESSION_PRODUCER_IDENTITY_INVALID"),
        ({"feature_session": date(2026, 9, 5)}, "CURRENT_SESSION_PRODUCER_FEATURE_SESSION_INVALID"),
        ({"feature_session": date(2025, 12, 2)}, "CURRENT_SESSION_PRODUCER_NOT_PROSPECTIVE"),
    ],
)
def test_known_snapshot_invalid_request_rejected_before_model_work(
    kwargs: dict[str, Any], expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    prices, rates = _synthetic_inputs()
    _forbid_known_model_work(monkeypatch)
    with pytest.raises(CurrentSessionProducerError) as raised:
        _build_known(prices, rates, **kwargs)
    assert raised.value.reason_code == expected


@pytest.mark.parametrize("problem", ["bytes", "loaded_policy", "fit_rule"])
def test_known_snapshot_policy_drift_rejected_before_model_work(
    problem: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    loaded = load_current_session_producer_policy()
    content = _input_policy_bytes()
    if problem == "bytes":
        content += b"\n"
    elif problem == "loaded_policy":
        loaded = replace(loaded, file_sha256="a" * 64)
    else:
        frozen = loaded.frozen_operational_policy
        walk_forward = frozen.policy.walk_forward.model_copy(update={"train_window_sessions": 503})
        loaded = replace(
            loaded,
            frozen_operational_policy=replace(
                frozen, policy=frozen.policy.model_copy(update={"walk_forward": walk_forward})
            ),
        )
    prices, rates = _synthetic_inputs()
    _forbid_known_model_work(monkeypatch)
    with pytest.raises(CurrentSessionProducerError) as raised:
        _build_known(prices, rates, loaded_policy=loaded, input_policy_content=content)
    assert raised.value.reason_code == "KNOWN_SNAPSHOT_PRODUCER_INPUT_POLICY_INVALID"


@pytest.mark.parametrize("problem", ["extra", "missing", "type", "series", "disclosures"])
def test_known_snapshot_policy_shape_is_strict(problem: str) -> None:
    raw = load_strict_yaml_text(_input_policy_bytes().decode("utf-8"))
    if problem == "extra":
        raw["allow_stale"] = True
    elif problem == "missing":
        del raw["information_set"]
    elif problem == "type":
        raw["training_sample_count"] = "504"
    elif problem == "series":
        raw["rate_series"] = ["DGS10", "DGS2"]
    else:
        raw["required_rate_disclosures"] = []
    with pytest.raises(ValueError):
        producer._KnownSnapshotInputPolicy.model_validate(raw, strict=True)


@pytest.fixture(scope="module")
def known_snapshot_result() -> CurrentSessionPreviewResult:
    prices, rates = _synthetic_inputs()
    return _build_known(prices, rates)


def _input_policy_bytes() -> bytes:
    return (
        PROJECT_ROOT / "config/research/first_layer_composer_v2_known_snapshot_input_v1.yaml"
    ).read_bytes()


def _build_known(
    prices: pd.DataFrame, rates: pd.DataFrame, **overrides: Any
) -> CurrentSessionPreviewResult:
    kwargs = {
        "loaded_policy": load_current_session_producer_policy(),
        "input_policy_content": _input_policy_bytes(),
        "feature_session": _FEATURE_SESSION,
        "prices": prices,
        "rates": rates,
        "data_quality_status": "PASS",
        "dq_receipt_sha256": "a" * 64,
        "source_sha256": "b" * 64,
        **overrides,
    }
    return build_known_snapshot_preview(**kwargs)


def _forbid_known_model_work(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(**kwargs: Any) -> CurrentSessionPreviewResult:
        pytest.fail("invalid known-snapshot input reached model work")

    monkeypatch.setattr(producer, "_build_normalized_session_preview", forbidden)


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
