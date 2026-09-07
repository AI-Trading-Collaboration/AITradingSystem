"""Pure synthetic arithmetic/parser tests; no actual provenance or DQ claim."""

from __future__ import annotations

import builtins
import hashlib
import io
import json
import socket
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from test_named_data_quality_execution_contract import _dispatch, _receipt

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    NamedArtifactBinding,
    NamedEqualRiskPriceScope,
)
from ai_trading_system.contracts.named_simple_baseline_preview import (
    FROZEN_PREVIEW_CANDIDATES,
    NamedSimpleBaselinePreview,
)
from ai_trading_system.simple_baseline_named_preview import (
    FROZEN_PREVIEW_REGISTRY_SHA256,
    NamedSimpleBaselinePreviewError,
    _candidate_previews,
    _lookback_at_target,
    _parse_registry,
    _price_matrix,
    build_named_simple_baseline_preview,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _dynamic_candidate_strategies,
    _target_weight_frame,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def registry() -> bytes:
    return (ROOT / EQUAL_RISK_PRICE_REGISTRY_PATH).read_bytes()


def _prices(n: int = 500, *, end: str | None = None) -> pd.DataFrame:
    index = (
        pd.bdate_range(end=end, periods=n) if end else pd.bdate_range(start="2021-02-22", periods=n)
    )
    step = np.arange(n)
    return pd.DataFrame(
        {
            "QQQ": 100 * np.exp(0.0005 * step + 0.01 * np.sin(step / 7)),
            "SGOV": 100 * np.exp(0.0001 * step + 0.0001 * np.cos(step / 3)),
            "TQQQ": 100 * np.exp(0.001 * step + 0.03 * np.sin(step / 7)),
        },
        index=index,
    )


def _scope(content: bytes, as_of: date) -> NamedEqualRiskPriceScope:
    return NamedEqualRiskPriceScope(
        as_of=as_of,
        requested_window=DataQualityDateWindow(date(2021, 2, 22), as_of),
        registry_binding=NamedArtifactBinding(
            "EXECUTION",
            EQUAL_RISK_PRICE_REGISTRY_PATH,
            hashlib.sha256(content).hexdigest(),
            len(content),
        ),
    )


def _csv(prices: pd.DataFrame) -> bytes:
    return (
        prices.rename_axis("date")
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="adj_close")
        .to_csv(index=False)
        .encode()
    )


def _forbid(*args: object, **kwargs: object) -> None:
    raise AssertionError("consumer performed forbidden I/O")


def test_manifest_has_exact_reviewed_additions_and_unchanged_dependencies() -> None:
    old = json.loads((ROOT / EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH).read_bytes())
    content = (ROOT / FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH).read_bytes()
    new = json.loads(content)
    assert hashlib.sha256(content).hexdigest() == FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256
    additions = [
        {
            "module_name": "ai_trading_system.contracts.named_simple_baseline_preview",
            "source_path": "src/ai_trading_system/contracts/named_simple_baseline_preview.py",
            "is_package": False,
        },
        {
            "module_name": "ai_trading_system.simple_baseline_named_preview",
            "source_path": "src/ai_trading_system/simple_baseline_named_preview.py",
            "is_package": False,
        },
    ]
    assert new == {
        **old,
        "modules": sorted(old["modules"] + additions, key=lambda row: row["module_name"]),
    }
    assert len(new["modules"]) == 59 and len(new["policy_dependencies"]) == 8


@pytest.mark.parametrize("end", ["2023-01-02", "2023-01-17", "2023-02-01"])
def test_five_complete_targets_match_original_full_matrix_without_io(
    registry: bytes, end: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, prices = _parse_registry(registry), _prices(end=end)
    definitions = {
        row["strategy_id"]: row
        for row in [*config["strategies"], *_dynamic_candidate_strategies(config)]
    }
    expected = {
        key: _target_weight_frame(value, prices, config).iloc[-1].to_dict()
        for key, value in definitions.items()
    }
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", _forbid)
        guard.setattr(io, "open", _forbid)
        guard.setattr(socket, "socket", _forbid)
        actual = _candidate_previews(
            prices, config, next_session=prices.index[-1].date() + timedelta(days=1)
        )
        assert (
            tuple(
                (row.candidate_id, row.candidate_role, row.registry_strategy_id) for row in actual
            )
            == FROZEN_PREVIEW_CANDIDATES
        )
        for row in actual:
            assert dict(row.target_weights) == expected[row.registry_strategy_id]
    equal = actual[0]
    rebalance_index = prices.index.get_loc(pd.Timestamp(equal.target_rebalance_session))
    # Independent 60-return/ddof=1/lag=1 calculation at actual month-first rebalance.
    history = prices[["QQQ", "SGOV"]].iloc[rebalance_index - 61 : rebalance_index]
    vol = history.pct_change(fill_method=None).dropna().std(ddof=1)
    qqq = float((1 / vol.QQQ) / ((1 / vol.QQQ) + (1 / vol.SGOV)))
    assert dict(equal.target_weights)["QQQ"] == pytest.approx(min(0.9, max(0.1, qqq)))
    assert equal.signal_price_through == prices.index[rebalance_index - 1].date()
    assert actual[-1].signal_price_through == prices.index[-2].date()
    assert all(row.signal_price_through is None for row in actual[1:4])


@pytest.mark.parametrize(
    "candidate,rows,ready",
    [
        ("equal_risk_qqq_sgov", 61, False),
        ("equal_risk_qqq_sgov", 62, True),
        ("dyn_tqqq_capped_trend", 272, False),
        ("dyn_tqqq_capped_trend", 273, True),
    ],
)
def test_algorithm_dependency_boundaries(
    registry: bytes, candidate: str, rows: int, ready: bool
) -> None:
    prices, config = _prices(rows, end="2024-01-01"), _parse_registry(registry)
    if ready:
        assert _lookback_at_target(candidate, prices, config)[0] == rows
    else:
        with pytest.raises(NamedSimpleBaselinePreviewError, match="LOOKBACK_INCOMPLETE"):
            _lookback_at_target(candidate, prices, config)


def test_midmonth_history_does_not_reinitialize_monthly_strategy(registry: bytes) -> None:
    prices = _prices(70, end="2024-01-15")
    with pytest.raises(NamedSimpleBaselinePreviewError, match="LOOKBACK_INCOMPLETE"):
        _lookback_at_target("equal_risk_qqq_sgov", prices, _parse_registry(registry))


def test_real_zero_vol_is_original_equal_weight_not_insufficient_history(registry: bytes) -> None:
    prices = _prices()
    prices.loc[:, :] = 100
    rows = _candidate_previews(prices, _parse_registry(registry), next_session=date(2024, 1, 1))
    assert rows[0].zero_volatility_initialization
    assert dict(rows[0].target_weights) == {"QQQ": 0.5, "SGOV": 0.5}
    # Original comparisons are inclusive; equal MAs and zero vol satisfy risk-on.
    assert dict(rows[-1].target_weights) == {"QQQ": 0.0, "SGOV": 0.67, "TQQQ": 0.33}


def test_current_price_cannot_change_lagged_current_target(registry: bytes) -> None:
    prices, config = _prices(end="2024-01-01"), _parse_registry(registry)
    baseline = _candidate_previews(prices, config, next_session=date(2024, 1, 2))
    prices.iloc[-1] *= 2
    assert _candidate_previews(prices, config, next_session=date(2024, 1, 2)) == baseline


@pytest.mark.parametrize("direction,expected_tqqq", [(1, 0.33), (-1, 0.0)])
def test_dynamic_challenger_exercises_both_original_states(
    registry: bytes, direction: int, expected_tqqq: float
) -> None:
    prices = _prices()
    prices["QQQ"] = 100 + direction * 0.01 * np.arange(len(prices))
    result = _candidate_previews(prices, _parse_registry(registry), next_session=date(2024, 1, 1))
    expected = (
        {"QQQ": 0.0, "SGOV": 0.67, "TQQQ": 0.33}
        if direction == 1
        else {"QQQ": 0.2, "SGOV": 0.8, "TQQQ": 0.0}
    )
    assert dict(result[-1].target_weights) == expected
    assert expected["TQQQ"] == expected_tqqq


def test_verifier_calendar_witness_excludes_reviewed_special_closure() -> None:
    from ai_trading_system.data.named_quality_execution import _preview_calendar_witness

    receipt = _receipt()
    end = date(2025, 1, 8)
    request = replace(
        receipt.request,
        source_manifest_path=FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
        scope=replace(
            receipt.request.scope,
            as_of=end,
            requested_window=DataQualityDateWindow(date(2021, 2, 22), end),
        ),
    )
    sessions, next_session = _preview_calendar_witness(request)
    assert sessions[0] == date(2021, 2, 22) and sessions[-1] == end
    assert next_session == date(2025, 1, 10)
    next_request = replace(
        request,
        scope=replace(
            request.scope,
            as_of=next_session,
            requested_window=DataQualityDateWindow(date(2021, 2, 22), next_session),
        ),
    )
    assert date(2025, 1, 9) not in _preview_calendar_witness(next_request)[0]


def test_overflowing_dynamic_ma_cannot_silently_select_risk_off(registry: bytes) -> None:
    prices = _prices()
    prices["QQQ"] = 1e306 * np.exp(-0.001 * np.arange(len(prices)))
    assert np.isfinite(prices.to_numpy()).all()
    assert np.isfinite(prices.pct_change(fill_method=None).iloc[1:].to_numpy()).all()
    with pytest.raises(NamedSimpleBaselinePreviewError, match="CALCULATION_NONFINITE"):
        _candidate_previews(prices, _parse_registry(registry), next_session=date(2024, 1, 1))


def test_equal_risk_interior_weight_holds_month_first_until_next_month(registry: bytes) -> None:
    config = _parse_registry(registry)
    prices = _prices(550, end="2023-02-15")
    steps = np.arange(len(prices))
    prices["SGOV"] = 100 * np.exp(0.0001 * steps + 0.007 * np.cos(steps / 7))
    month_start = prices.index.get_loc(pd.Timestamp("2023-01-02"))
    start_prices = prices.iloc[: month_start + 1]
    first = _candidate_previews(start_prices, config, next_session=date(2023, 1, 3))[0]
    weight = dict(first.target_weights)["QQQ"]
    assert 0.1 < weight < 0.9
    history = prices[["QQQ", "SGOV"]].iloc[month_start - 61 : month_start]
    vol = history.pct_change(fill_method=None).dropna().std(ddof=1)
    assert weight == pytest.approx(float(vol.SGOV / (vol.QQQ + vol.SGOV)))
    # New high SGOV variability inside January must not reset its monthly target.
    later = prices.index > pd.Timestamp("2023-01-02")
    prices.loc[later, "SGOV"] *= np.exp(0.03 * np.sin(steps[later]))
    mid = _candidate_previews(prices.loc[:"2023-01-18"], config, next_session=date(2023, 1, 19))[0]
    assert mid.target_weights == first.target_weights
    assert mid.signal_price_through == first.signal_price_through
    following = _candidate_previews(
        prices.loc[:"2023-02-01"], config, next_session=date(2023, 2, 2)
    )[0]
    assert following.target_weights != first.target_weights
    assert following.signal_price_through == date(2023, 1, 31)


@pytest.mark.parametrize(
    "damage",
    [
        "utf8",
        "duplicate",
        "cycle",
        "mixed_universe",
        "wrong_universe",
        "alias",
        "role",
        "id_collision",
        "negative_weight",
        "nonfinite_weight",
        "giant_integer_weight",
        "sum",
        "window",
        "safety",
        "frequency",
        "lookback",
        "comment_only",
    ],
)
def test_registry_rejects_every_unreviewed_complete_version(registry: bytes, damage: str) -> None:
    config = yaml.safe_load(registry)
    policy = config["research_policy"]
    if damage == "utf8":
        content = b"\xff"
    elif damage == "duplicate":
        content = b"a: 1\na: 2\n"
    elif damage == "cycle":
        content = b"a: &a [*a]\n"
    else:
        if damage == "mixed_universe":
            policy["required_price_tickers"] = ["QQQ", 1, "SGOV"]
        elif damage == "wrong_universe":
            policy["required_rate_series"] = None
        elif damage == "alias":
            policy["forward_aging"]["public_strategy_aliases"]["100_qqq"] = "missing"
        elif damage == "role":
            policy["forward_aging"]["candidate_freeze"]["primary"] = ["qqq_50_sgov_50"]
        elif damage == "id_collision":
            policy["dynamic_policy_search"]["candidate_allocations"][0]["policy_id"] = config[
                "strategies"
            ][0]["strategy_id"]
        elif damage in {"negative_weight", "nonfinite_weight", "giant_integer_weight", "sum"}:
            config["strategies"][0]["target_weights"] = {
                "QQQ": {
                    "negative_weight": -0.1,
                    "nonfinite_weight": float("nan"),
                    "giant_integer_weight": 10**1000,
                    "sum": 0.2,
                }[damage]
            }
        elif damage == "window":
            config["market_regime"]["default_backtest_start"] = "2022-12-01"
        elif damage == "safety":
            config["safety_boundary"]["production_allowed"] = True
        elif damage == "frequency":
            config["strategies"][0]["rebalance_frequency"] = "daily"
        elif damage == "lookback":
            policy["moving_average_windows"]["long"] = 199
        content = (
            yaml.safe_dump(config).encode() if damage != "comment_only" else registry + b"#x\n"
        )
    with pytest.raises(NamedSimpleBaselinePreviewError) as error:
        _parse_registry(content)
    assert error.value.code.startswith("NAMED_PREVIEW_")
    assert hashlib.sha256(registry).hexdigest() == FROZEN_PREVIEW_REGISTRY_SHA256


@pytest.mark.parametrize(
    "damage",
    [
        "utf8",
        "unclosed_quote",
        "duplicate_header",
        "missing_header",
        "duplicate_key",
        "lowercase",
        "date_shape",
        "missing_asset",
        "missing_day",
        "missing_last",
        "future",
        "weekend",
        "nan",
        "zero",
        "negative",
        "infinity",
        "ratio_overflow",
    ],
)
def test_price_parser_never_repairs_bad_inputs(registry: bytes, damage: str) -> None:
    prices = _prices(10)
    content = _csv(prices)
    rows = list(__import__("csv").DictReader(io.StringIO(content.decode())))
    if damage == "utf8":
        content = b"\xff"
    elif damage == "unclosed_quote":
        content = b'date,ticker,adj_close\n"unfinished'
    elif damage == "duplicate_header":
        content = content.replace(b"adj_close", b"ticker", 1)
    elif damage == "missing_header":
        content = content.replace(b"adj_close", b"price", 1)
    else:
        if damage == "duplicate_key":
            rows.append(rows[0])
        elif damage == "lowercase":
            rows[0]["ticker"] = "qqq"
        elif damage == "date_shape":
            rows[0]["date"] = "20210222"
        elif damage == "missing_asset":
            rows = [row for row in rows if row["ticker"] != "SGOV"]
        elif damage in {"missing_day", "missing_last"}:
            missing = prices.index[-1 if damage == "missing_last" else 2].date().isoformat()
            rows = [row for row in rows if row["date"] != missing]
        elif damage in {"future", "weekend"}:
            rows.append({**rows[0], "date": "2021-03-08" if damage == "future" else "2021-02-27"})
        elif damage == "ratio_overflow":
            rows[0]["adj_close"], rows[1]["adj_close"] = "1e-308", "1e308"
        else:
            rows[0]["adj_close"] = {"nan": "nan", "zero": "0", "negative": "-1", "infinity": "inf"}[
                damage
            ]
        content = pd.DataFrame(rows).to_csv(index=False).encode()
    with pytest.raises(NamedSimpleBaselinePreviewError) as error:
        _price_matrix(
            content,
            scope=_scope(registry, prices.index[-1].date()),
            sessions=tuple(stamp.date() for stamp in prices.index),
        )
    assert error.value.code.startswith("NAMED_PREVIEW_")


def test_parser_discloses_exclusions_and_preserves_exact_grid(registry: bytes) -> None:
    prices = _prices(10)
    content = _csv(prices) + b"2021-02-19,QQQ,99\n2021-02-22,SPY,99\n"
    actual, counts = _price_matrix(
        content,
        scope=_scope(registry, prices.index[-1].date()),
        sessions=tuple(stamp.date() for stamp in prices.index),
    )
    assert counts == (32, 30, 1, 1)
    np.testing.assert_allclose(actual[prices.columns].to_numpy(), prices.to_numpy())


def _result(registry: bytes) -> NamedSimpleBaselinePreview:
    receipt = _receipt()
    scope = _scope(registry, receipt.request.scope.as_of)
    execution = replace(
        receipt.execution,
        source_manifest_path=FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    )
    request = replace(
        receipt.request,
        source_manifest_path=execution.source_manifest_path,
        source_manifest_sha256=execution.source_manifest_sha256,
        scope=replace(receipt.request.scope, expected_rate_series=scope.guard_rate_series),
    )
    receipt = replace(
        receipt,
        request=request,
        execution=execution,
        execution_dependencies=receipt.execution_dependencies + (scope.registry_binding,),
    )
    prices = _prices(end=scope.as_of.isoformat())
    candidates = _candidate_previews(
        prices, _parse_registry(registry), next_session=scope.as_of + timedelta(days=1)
    )
    return NamedSimpleBaselinePreview(
        scope,
        receipt,
        _dispatch(receipt),
        candidates,
        1500,
        1500,
        0,
        0,
        (
            ("python_implementation", "synthetic"),
            ("python_version", "synthetic"),
            ("pandas_version", "synthetic"),
            ("numpy_version", "synthetic"),
        ),
    )


def test_result_is_canonical_description_never_a_seal(registry: bytes) -> None:
    result = _result(registry)
    payload = json.loads(result.canonical_bytes)
    assert result.canonical_bytes == result.canonical_bytes
    assert payload["status"] == "FIVE_CANDIDATE_PREVIEW_READY"
    for key in (
        "dispatch_allowed",
        "consumer_cutover_allowed",
        "returns_computed",
        "observation_created",
        "outcome_read",
        "dq_validation_executed",
    ):
        assert payload[key] is False
    assert payload["pit_status"] == payload["oos_status"] == "NOT_ESTABLISHED"
    for value in (object(), result, result.receipt, payload):
        with pytest.raises(NamedSimpleBaselinePreviewError, match="VERIFIED_INPUTS_REQUIRED"):
            build_named_simple_baseline_preview(value, required_scope=result.scope)


@pytest.mark.parametrize(
    "damage",
    [
        "scope",
        "dispatch",
        "mutable_env",
        "duplicate_env",
        "mutable_candidates",
        "partial_candidates",
        "counts",
    ],
)
def test_result_rejects_conflicting_or_mutable_evidence(registry: bytes, damage: str) -> None:
    result = _result(registry)
    changes = {
        "scope": {
            "scope": replace(
                result.scope,
                registry_binding=replace(result.scope.registry_binding, sha256="0" * 64),
            )
        },
        "dispatch": {
            "successful_dispatch": replace(result.successful_dispatch, execution_pid=9876)
        },
        "mutable_env": {
            "calculation_environment": tuple(list(p) for p in result.calculation_environment)
        },
        "duplicate_env": {"calculation_environment": (result.calculation_environment[0],) * 4},
        "mutable_candidates": {"candidates": list(result.candidates)},
        "partial_candidates": {"candidates": result.candidates[:-1]},
        "counts": {"source_row_count": 1501},
    }[damage]
    with pytest.raises(ValueError):
        replace(result, **changes)


@pytest.mark.parametrize(
    "damage", ["mutable_pair", "datetime", "frequency", "static_signal", "nonfinite", "sum"]
)
def test_candidate_rejects_mutable_or_invalid_semantics(registry: bytes, damage: str) -> None:
    row = _result(registry).candidates[1]
    changes = {
        "mutable_pair": {"target_weights": tuple(list(pair) for pair in row.target_weights)},
        "datetime": {"target_session": datetime.combine(row.target_session, datetime.min.time())},
        "frequency": {"rebalance_frequency": "daily"},
        "static_signal": {"signal_price_through": row.target_rebalance_session - timedelta(days=1)},
        "nonfinite": {"target_weights": (("QQQ", float("nan")),)},
        "sum": {"target_weights": (("QQQ", 0.2),)},
    }[damage]
    with pytest.raises(ValueError):
        replace(row, **changes)
