from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.first_layer_operational_forecast import (
    OperationalForecastError,
    OperationalForecastResult,
    _next_xnys_session,
    _xnys_sessions,
    build_operational_forecast_series,
    load_operational_forecast_policy,
)
from ai_trading_system.qqq_options_research.exact_signal_package_admission import (
    audit_first_layer_signal_source,
    load_exact_signal_package_admission_policy,
)


def test_policy_freezes_generic_producer_without_options_redesign() -> None:
    loaded = load_operational_forecast_policy()
    policy = loaded.policy

    assert policy.evaluation_window.expected_session_count == 1202
    assert policy.walk_forward.train_window_sessions == 504
    assert policy.walk_forward.label_horizon_sessions == 20
    assert policy.walk_forward.refit_step_sessions == 21
    assert policy.model_contract.model_id == "first_layer_composer_v2"
    assert policy.training_history.cash_reference.proxy_role == (
        "TRAINING_INITIALIZATION_ONLY"
    )
    assert policy.safety.real_cache_materialization_authorized_in_this_wave is False
    assert policy.safety.quantconnect_backtest_allowed is False
    assert policy.safety.orders == policy.safety.fills == policy.safety.positions == 0


def test_operational_producer_emits_exact_unique_label_free_terminal_series(
    operational_result: OperationalForecastResult,
) -> None:
    result = operational_result
    predictions = result.predictions
    assert len(predictions) == predictions["date"].nunique() == 1202
    assert predictions["date"].iloc[0] == "2021-02-22"
    assert predictions["date"].iloc[-1] == "2025-12-02"
    assert predictions["decision_at"].iloc[-1] == "2025-12-03"
    assert not any("label" in column.lower() for column in predictions.columns)
    assert set(predictions["cash_reference_source"]) == {"SGOV"}
    assert result.receipt["terminal_prediction_emitted"] is True
    assert result.receipt["evaluation_proxy_row_count"] == 0
    assert result.receipt["quantconnect_status"] == "NOT_AUTHORIZED_NOT_RUN"


def test_each_fit_uses_504_mature_labels_and_each_session_uses_one_fit(
    operational_result: OperationalForecastResult,
) -> None:
    result = operational_result
    audit = result.fit_audit
    assert set(audit["train_sample_count"]) == {504}
    assert audit["label_maturity_pass"].all()
    assert all(
        date.fromisoformat(available) <= date.fromisoformat(fit)
        for available, fit in zip(
            audit["latest_label_available_at"], audit["fit_session"], strict=True
        )
    )
    assert result.predictions.groupby("date")["fit_id"].nunique().eq(1).all()
    assert result.predictions["fit_id"].nunique() == 58
    first_fit_proxy_count = audit.loc[
        audit["fit_id"] == "operational_wf_0000", "proxy_training_sample_count"
    ].min()
    assert first_fit_proxy_count > 0


def test_operational_output_passes_existing_exact_source_admission(
    operational_result: OperationalForecastResult,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "first_layer_composer_v2_predictions.csv"
    operational_result.predictions.to_csv(source_path, index=False)

    audit = audit_first_layer_signal_source(
        loaded_policy=load_exact_signal_package_admission_policy(),
        source_path=source_path,
        project_root=tmp_path,
    )

    assert audit.admission_status == "PASS"
    assert audit.unique_session_count == 1202
    assert audit.blocker_codes == ()


def test_decision_at_is_exact_next_xnys_across_holidays() -> None:
    assert _next_xnys_session(date(2021, 7, 2)) == date(2021, 7, 6)
    assert _next_xnys_session(date(2021, 11, 24)) == date(2021, 11, 26)


def test_insufficient_pre_evaluation_history_fails_closed() -> None:
    loaded = load_operational_forecast_policy()
    prices, rates = _synthetic_inputs(start=date(2019, 7, 1))

    with pytest.raises(OperationalForecastError) as raised:
        build_operational_forecast_series(
            loaded_policy=loaded,
            prices=prices,
            rates=rates,
            data_quality_status="PASS",
            data_quality_identity_sha256="c" * 64,
        )

    assert raised.value.reason_code in {
        "OPERATIONAL_FORECAST_TRAINING_PRICE_COVERAGE_INCOMPLETE",
        "OPERATIONAL_FORECAST_TRAINING_HISTORY_INSUFFICIENT",
    }


def test_non_pass_data_quality_is_rejected_before_model_work() -> None:
    loaded = load_operational_forecast_policy()
    prices, rates = _synthetic_inputs()

    with pytest.raises(
        OperationalForecastError, match="OPERATIONAL_FORECAST_DQ_NOT_PASS"
    ):
        build_operational_forecast_series(
            loaded_policy=loaded,
            prices=prices,
            rates=rates,
            data_quality_status="FAIL",
            data_quality_identity_sha256="d" * 64,
        )


@pytest.fixture(scope="module")
def operational_result() -> OperationalForecastResult:
    loaded = load_operational_forecast_policy()
    prices, rates = _synthetic_inputs()
    return build_operational_forecast_series(
        loaded_policy=loaded,
        prices=prices,
        rates=rates,
        data_quality_status="PASS",
        data_quality_identity_sha256="a" * 64,
    )


def _synthetic_inputs(
    *, start: date = date(2018, 1, 2)
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sessions = _xnys_sessions(start, date(2025, 12, 2))
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
