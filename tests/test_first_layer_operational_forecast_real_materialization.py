from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.first_layer_operational_forecast_real_materialization import (
    RealMaterializationError,
    _normalized_signals,
    _project_scope,
    load_real_materialization_policy,
    run_real_operational_forecast_materialization,
)


def test_real_materialization_policy_freezes_segmented_dq_and_safety() -> None:
    loaded = load_real_materialization_policy()

    assert loaded.policy.authorization_state == "EXACT_PREAUTHORIZED"
    assert loaded.policy.policy_version == "1.2.0"
    assert loaded.policy.authorities.xnys_special_closure_policy.path == (
        "config/data/us_equity_special_closure_registry.yaml"
    )
    assert loaded.policy.authorities.signal_export_policy.path.endswith(
        "qqq_options_signal_export_v2.yaml"
    )
    assert loaded.policy.authorities.project_adapter_policy.path.endswith(
        "qc_qqq_options_project_adapter_contract_v2.yaml"
    )
    assert tuple(scope.scope_id for scope in loaded.policy.dq_scopes) == (
        "training_proxy_history",
        "exact_sgov_history",
        "primary_evaluation",
    )
    assert loaded.policy.dq_scopes[0].expected_price_tickers == ("QQQ", "SHY", "TQQQ")
    assert loaded.policy.dq_scopes[1].requested_start == date(2020, 5, 28)
    assert tuple(scope.require_secondary_prices for scope in loaded.policy.dq_scopes) == (
        True,
        False,
        True,
    )
    assert loaded.policy.producer_execution.expected_session_count == 1202
    assert loaded.policy.producer_execution.package_run_id.endswith("_v3")
    assert loaded.policy.safety.real_cache_materialization_allowed is True
    assert loaded.policy.safety.quantconnect_backtest_allowed_in_this_materialization_wave is False
    assert loaded.policy.safety.maximum_quantconnect_backtests_in_this_materialization_wave == 0
    assert loaded.policy.safety.orders_outside_qc_simulation == 0
    assert loaded.policy.safety.broker_action == "none"


def test_scope_projection_preserves_real_listing_boundary_without_fill() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2020-05-27", "2020-05-28", "2020-05-29", "2020-05-28"],
            "ticker": ["SGOV", "SGOV", "SGOV", "QQQ"],
            "adj_close": [99.0, 100.0, 100.1, 200.0],
        }
    )

    projected = _project_scope(
        frame,
        identity_column="ticker",
        identities=("SGOV",),
        start=date(2020, 5, 28),
        end=date(2020, 5, 29),
        role="prices",
    )

    assert projected[["ticker", "date"]].to_dict(orient="records") == [
        {"ticker": "SGOV", "date": "2020-05-28"},
        {"ticker": "SGOV", "date": "2020-05-29"},
    ]


def test_scope_projection_rejects_duplicate_identity_session() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2021-02-22", "2021-02-22"],
            "ticker": ["QQQ", "QQQ"],
            "adj_close": [300.0, 301.0],
        }
    )

    with pytest.raises(RealMaterializationError) as exc_info:
        _project_scope(
            frame,
            identity_column="ticker",
            identities=("QQQ",),
            start=date(2021, 2, 22),
            end=date(2021, 2, 22),
            role="prices",
        )

    assert exc_info.value.reason_code == "REAL_MATERIALIZATION_SOURCE_DUPLICATE_KEY"


def test_normalized_signal_mapping_uses_reviewed_close_and_exact_actions() -> None:
    predictions = pd.DataFrame(
        {
            "date": ["2025-11-28", "2025-12-01"],
            "trend_state": ["risk_on", "defensive"],
        }
    )
    mapping = {
        "constructive": "LONG_CALL",
        "defensive": "FLAT",
        "neutral": "FLAT",
        "risk_off": "FLAT",
        "risk_on": "LONG_CALL",
    }

    signals = _normalized_signals(predictions, mapping=mapping)

    assert tuple(item.signal for item in signals) == ("LONG_CALL", "FLAT")
    assert all(item.source_data_cutoff_utc < item.generated_at_utc for item in signals)
    assert all(
        item.generated_at_utc - item.source_data_cutoff_utc == pd.Timedelta(minutes=1)
        for item in signals
    )


def test_normalized_signal_mapping_rejects_unknown_state() -> None:
    predictions = pd.DataFrame(
        {"date": ["2025-12-01"], "trend_state": ["unknown_state"]}
    )

    with pytest.raises(RealMaterializationError) as exc_info:
        _normalized_signals(predictions, mapping={"risk_on": "LONG_CALL"})

    assert exc_info.value.reason_code == "REAL_MATERIALIZATION_STATE_INVALID"


def test_real_run_rejects_invalid_code_identity_before_any_output(tmp_path: Path) -> None:
    loaded = load_real_materialization_policy()

    with pytest.raises(RealMaterializationError) as exc_info:
        run_real_operational_forecast_materialization(
            loaded_policy=loaded,
            repository_code_sha="not-a-sha",
            output_root=tmp_path / "must-not-exist",
        )

    assert exc_info.value.reason_code == "REAL_MATERIALIZATION_CODE_IDENTITY_INVALID"
    assert not (tmp_path / "must-not-exist").exists()


def test_real_run_rejects_output_override_outside_frozen_policy(tmp_path: Path) -> None:
    loaded = load_real_materialization_policy()

    with pytest.raises(RealMaterializationError) as exc_info:
        run_real_operational_forecast_materialization(
            loaded_policy=loaded,
            repository_code_sha="a" * 40,
            output_root=tmp_path / "different-output",
        )

    assert exc_info.value.reason_code == "REAL_MATERIALIZATION_OUTPUT_OUTSIDE_REPOSITORY"
    assert not (tmp_path / "different-output").exists()
