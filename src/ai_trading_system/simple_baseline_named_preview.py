"""Five frozen targets from process-local verified bytes, with no consumer I/O.

All calculations reuse the existing portfolio-control functions. The verifier,
not this module, supplies the captured registry and canonical calendar witness.
See TRADING-2564_S2c2_Five_Candidate_Read_Only_Preview_V1.md.
"""

from __future__ import annotations

import csv
import hashlib
import io
import math
import platform
import re
from collections.abc import Mapping
from datetime import date
from typing import Any, NoReturn, cast

import numpy as np
import pandas as pd

from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_TICKERS,
    EQUAL_RISK_PRIMARY_START,
    NamedEqualRiskPriceScope,
    VerifiedNamedInputs,
)
from ai_trading_system.contracts.named_simple_baseline_preview import (
    FROZEN_PREVIEW_CANDIDATES,
    NamedSimpleBaselinePreview,
    SimpleBaselineCandidatePreview,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _dynamic_candidate_strategies,
    _realized_vol,
    _target_weight_frame,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

# Reviewed complete registry identity, not another tunable strategy policy.
# Any change requires an explicit new consumer version/review (§4 requirement).
FROZEN_PREVIEW_REGISTRY_SHA256 = "a4487d4477e7066943c3894e690d1dc9dbcb18bc28919eaa8cfb15371ef79843"
_TICKER = re.compile(r"[A-Z][A-Z0-9.-]*")


class NamedSimpleBaselinePreviewError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"{code}: {detail}")


def _fail(code: str, detail: str) -> NoReturn:
    raise NamedSimpleBaselinePreviewError(code, detail)


def _mapping(value: object, label: str) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", label)
    return value


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if type(value) is not list or not value:
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", label)
    return [_mapping(row, label) for row in value]


def _weights(value: object, label: str) -> None:
    weights = _mapping(value, label)
    if not weights or any(
        ticker not in EQUAL_RISK_PRICE_TICKERS
        or type(weight) not in {float, int}
        or not 0 <= weight <= 1
        or not math.isfinite(weight)
        for ticker, weight in weights.items()
    ):
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", label + " finite nonnegative weights required")
    # Floating-point summation invariant, not permission to renormalize weights.
    if abs(math.fsum(weights.values()) - 1) > math.ulp(1.0) * len(weights):
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", label + " weights must sum to one")


def _parse_registry(content: bytes) -> dict[str, Any]:
    if type(content) is not bytes:
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", "captured bytes required")
    try:
        config = _mapping(load_strict_yaml_text(content.decode("utf-8")), "registry")
    except (UnicodeError, ValueError) as exc:
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", str(exc))
    policy = _mapping(config.get("research_policy"), "research_policy")
    forward = _mapping(policy.get("forward_aging"), "forward_aging")
    freeze = _mapping(forward.get("candidate_freeze"), "candidate_freeze")
    expected_groups = {
        "primary": ["equal_risk_qqq_sgov"],
        "static_comparators": ["qqq_50_sgov_50", "qqq_60_sgov_40", "100_qqq"],
        "challenger": ["dyn_tqqq_capped_trend"],
    }
    if any(freeze.get(key) != values for key, values in expected_groups.items()) or forward.get(
        "public_strategy_aliases"
    ) != {"100_qqq": "qqq_100_static"}:
        _fail("NAMED_PREVIEW_CANDIDATES_CHANGED", "fixed groups, order and unique alias required")

    def exact_universe(value: object, expected: tuple[str, ...]) -> bool:
        return (
            type(value) is list
            and all(type(item) is str for item in value)
            and sorted(value) == sorted(expected)
        )

    if (
        not exact_universe(policy.get("required_price_tickers"), EQUAL_RISK_PRICE_TICKERS)
        or not exact_universe(policy.get("required_rate_series"), EQUAL_RISK_GUARD_RATE_SERIES)
        or _mapping(config.get("market_regime"), "market_regime").get("default_backtest_start")
        != EQUAL_RISK_PRIMARY_START
    ):
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", "full asset/rate universe and primary window")
    safety = _mapping(config.get("safety_boundary"), "safety_boundary")
    if any(
        safety.get(key) != value or type(safety.get(key)) is not type(value)
        for key, value in {
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
            "research_only": True,
            "observe_only": True,
        }.items()
    ):
        _fail("NAMED_PREVIEW_REGISTRY_INVALID", "frozen safety fields")
    strategies = _rows(config.get("strategies"), "strategies")
    search = _mapping(policy.get("dynamic_policy_search"), "dynamic_policy_search")
    allocations = _rows(search.get("candidate_allocations"), "candidate_allocations")
    ids = [row.get("strategy_id") for row in strategies] + [
        row.get("policy_id") for row in allocations
    ]
    if any(type(value) is not str or not value for value in ids) or len(set(ids)) != len(ids):
        _fail("NAMED_PREVIEW_CANDIDATE_ID_COLLISION", "unique static and dynamic IDs required")
    if not {item[2] for item in FROZEN_PREVIEW_CANDIDATES}.issubset(ids):
        _fail("NAMED_PREVIEW_CANDIDATES_CHANGED", "missing frozen strategy definition")
    for row in strategies:
        _weights(row.get("target_weights"), str(row["strategy_id"]))
    for row in allocations:
        for key in ("risk_on_weights", "risk_off_weights"):
            _weights(row.get(key), str(row["policy_id"]) + "." + key)
    if hashlib.sha256(content).hexdigest() != FROZEN_PREVIEW_REGISTRY_SHA256:
        _fail("NAMED_PREVIEW_REGISTRY_VERSION_CHANGED", "complete reviewed registry bytes required")
    return config


def _price_matrix(
    content: bytes, *, scope: NamedEqualRiskPriceScope, sessions: tuple[date, ...]
) -> tuple[pd.DataFrame, tuple[int, int, int, int]]:
    try:
        return _parse_price_matrix(content, scope=scope, sessions=sessions)
    except (UnicodeError, csv.Error) as exc:
        _fail("NAMED_PREVIEW_CSV_ROW_INVALID", str(exc))


def _parse_price_matrix(
    content: bytes, *, scope: NamedEqualRiskPriceScope, sessions: tuple[date, ...]
) -> tuple[pd.DataFrame, tuple[int, int, int, int]]:
    if (
        type(content) is not bytes
        or type(sessions) is not tuple
        or not sessions
        or any(type(session) is not date for session in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[0] != scope.requested_window.start
        or sessions[-1] != scope.as_of
    ):
        _fail("NAMED_PREVIEW_INPUT_INVALID", "full immutable primary-session witness required")
    reader = csv.reader(io.StringIO(content.decode("utf-8"), newline=""), strict=True)
    header = next(reader, None)
    if (
        header is None
        or not header
        or any(not key or key != key.strip() for key in header)
        or len(set(header)) != len(header)
        or not {"date", "ticker", "adj_close"}.issubset(header)
    ):
        _fail(
            "NAMED_PREVIEW_CSV_HEADER_INVALID", "unique columns and date/ticker/adj_close required"
        )
    date_column, ticker_column, price_column = (
        header.index(k) for k in ("date", "ticker", "adj_close")
    )
    session_set = set(sessions)
    source_keys: set[tuple[date, str]] = set()
    consumed: list[tuple[date, str, float]] = []
    before_count = other_count = source_count = 0
    for row in reader:
        source_count += 1
        if len(row) != len(header):
            _fail("NAMED_PREVIEW_CSV_ROW_INVALID", f"row {source_count}: column count")
        raw_date, ticker = row[date_column], row[ticker_column]
        try:
            observed = date.fromisoformat(raw_date)
            price = float(row[price_column])
        except (ValueError, OverflowError) as exc:
            _fail("NAMED_PREVIEW_CSV_ROW_INVALID", f"row {source_count}: {exc}")
        if raw_date != observed.isoformat() or _TICKER.fullmatch(ticker) is None:
            _fail("NAMED_PREVIEW_NONCANONICAL_KEY", f"row {source_count}: {raw_date}/{ticker}")
        key = (observed, ticker)
        if key in source_keys:
            _fail("NAMED_PREVIEW_DUPLICATE_PRICE_KEY", f"{observed}/{ticker}")
        source_keys.add(key)
        if not math.isfinite(price) or price <= 0:
            _fail("NAMED_PREVIEW_PRICE_INVALID", f"{observed}/{ticker}")
        if observed > scope.as_of:
            _fail("NAMED_PREVIEW_FUTURE_PRICE", f"{observed}/{ticker}")
        if observed < scope.requested_window.start:
            before_count += 1
        elif ticker not in scope.expected_price_tickers:
            other_count += 1
        elif observed not in session_set:
            _fail("NAMED_PREVIEW_NON_SESSION_PRICE", f"{observed}/{ticker}")
        else:
            consumed.append((observed, ticker, price))
    expected_keys = {
        (session, ticker) for session in sessions for ticker in scope.expected_price_tickers
    }
    actual_keys = {(session, ticker) for session, ticker, _ in consumed}
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        _fail("NAMED_PREVIEW_PRICE_COVERAGE_INCOMPLETE", str(missing[:1]))
    frame = pd.DataFrame(consumed, columns=["date", "ticker", "adj_close"])
    frame["date"] = pd.to_datetime(frame["date"])
    matrix = frame.pivot(index="date", columns="ticker", values="adj_close").reindex(
        index=pd.DatetimeIndex(sessions), columns=list(scope.expected_price_tickers)
    )
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        changes = matrix.pct_change(fill_method=None).iloc[1:].to_numpy()
    if not np.isfinite(changes).all():
        _fail("NAMED_PREVIEW_CALCULATION_NONFINITE", "price changes exceed finite arithmetic")
    return matrix, (source_count, len(consumed), before_count, other_count)


def _lookback_at_target(
    candidate_id: str, prices: pd.DataFrame, config: Mapping[str, Any]
) -> tuple[int, int, bool]:
    """Return required rows, actual rebalance index, and original zero-vol state."""
    policy = config["research_policy"]
    target_index = len(prices) - 1
    if candidate_id == "dyn_tqqq_capped_trend":
        # The rolling quantile consumes already-lagged volatility: w + q + 1
        # prices. Counts derive from algorithm dependencies, not sample policy.
        required = max(
            policy["moving_average_windows"]["short"] + 1,
            policy["moving_average_windows"]["long"] + 1,
            policy["realized_vol_windows"]["short"] + policy["rolling_high_windows"]["long"] + 1,
            policy["rolling_high_windows"]["medium"] + 1,
        )
    else:
        period = prices.index[-1].to_period("M")
        target_index = next(
            index for index, stamp in enumerate(prices.index) if stamp.to_period("M") == period
        )
        required = (
            policy["realized_vol_windows"]["medium"] + 2
            if candidate_id == "equal_risk_qqq_sgov"
            else 1
        )
    if target_index + 1 < required:
        _fail(
            "NAMED_PREVIEW_LOOKBACK_INCOMPLETE",
            f"{candidate_id}: rebalance={prices.index[target_index].date()}, "
            f"available_prices={target_index + 1}, required_prices={required}",
        )
    zero_vol = False
    if candidate_id == "equal_risk_qqq_sgov":
        vols = tuple(
            float(
                _realized_vol(
                    prices[ticker],
                    policy["realized_vol_windows"]["medium"],
                    policy["annualization_trading_days"],
                ).iloc[target_index]
            )
            for ticker in ("QQQ", "SGOV")
        )
        if not all(math.isfinite(vol) and vol >= 0 for vol in vols):
            _fail("NAMED_PREVIEW_CALCULATION_NONFINITE", candidate_id)
        zero_vol = any(vol == 0 for vol in vols)
    elif candidate_id == "dyn_tqqq_capped_trend":
        vol = _realized_vol(
            prices["QQQ"],
            policy["realized_vol_windows"]["short"],
            policy["annualization_trading_days"],
        )
        if not np.isfinite(vol.iloc[-policy["rolling_high_windows"]["long"] :].to_numpy()).all():
            _fail("NAMED_PREVIEW_CALCULATION_NONFINITE", candidate_id)
        # Mirror the original operands solely to reject arithmetic failure before
        # NaN comparisons could silently select risk-off. This is not a new signal.
        qqq = prices["QQQ"]
        ma_short = qqq.rolling(policy["moving_average_windows"]["short"]).mean().shift(1)
        ma_long = qqq.rolling(policy["moving_average_windows"]["long"]).mean().shift(1)
        close = qqq.shift(1)
        high = qqq.rolling(policy["rolling_high_windows"]["medium"]).max().shift(1)
        cutoff = policy["dynamic_policy_search"]["volatility_percentile_thresholds"][-1]
        threshold = vol.rolling(policy["rolling_high_windows"]["long"]).quantile(cutoff)
        operands = (ma_short, ma_long, close, high, close / high - 1, vol, threshold)
        if not all(math.isfinite(float(value.iloc[-1])) for value in operands):
            _fail("NAMED_PREVIEW_CALCULATION_NONFINITE", candidate_id)
    return required, target_index, zero_vol


def _candidate_previews(
    prices: pd.DataFrame, config: Mapping[str, Any], *, next_session: date
) -> tuple[SimpleBaselineCandidatePreview, ...]:
    definitions = {
        row["strategy_id"]: row
        for row in [*config["strategies"], *_dynamic_candidate_strategies(config)]
    }
    # Check every candidate first: no partially-ready five-candidate result.
    readiness = {
        candidate_id: _lookback_at_target(candidate_id, prices, config)
        for candidate_id, _, _ in FROZEN_PREVIEW_CANDIDATES
    }
    result = []
    for candidate_id, role, strategy_id in FROZEN_PREVIEW_CANDIDATES:
        strategy = definitions[strategy_id]
        required, rebalance_index, zero_vol = readiness[candidate_id]
        frame = _target_weight_frame(strategy, prices, config)
        signal_date = (
            prices.index[rebalance_index - 1].date()
            if candidate_id in {"equal_risk_qqq_sgov", "dyn_tqqq_capped_trend"}
            else None
        )
        result.append(
            SimpleBaselineCandidatePreview(
                candidate_id=candidate_id,
                candidate_role=role,
                registry_strategy_id=strategy_id,
                rebalance_frequency=strategy["rebalance_frequency"],
                target_session=prices.index[-1].date(),
                target_rebalance_session=prices.index[rebalance_index].date(),
                signal_price_through=signal_date,
                legacy_applied_session=next_session,
                target_weights=tuple(
                    sorted(
                        (cast(str, ticker), float(value))
                        for ticker, value in frame.iloc[-1].items()
                    )
                ),
                required_lookback_price_rows=required,
                available_at_rebalance_price_rows=rebalance_index + 1,
                zero_volatility_initialization=zero_vol,
            )
        )
    return tuple(result)


def build_named_simple_baseline_preview(
    verified: VerifiedNamedInputs, *, required_scope: NamedEqualRiskPriceScope
) -> NamedSimpleBaselinePreview:
    """Consume only same-context sealed inputs; perform no I/O or DQ execution."""
    if type(verified) is not VerifiedNamedInputs:
        _fail("NAMED_PREVIEW_VERIFIED_INPUTS_REQUIRED", "same-process verifier seal required")
    prices, registry, sessions, next_session = verified.inputs_for_five_candidate_preview(
        required_scope=required_scope
    )
    config = _parse_registry(registry)
    matrix, counts = _price_matrix(prices, scope=required_scope, sessions=sessions)
    candidates = _candidate_previews(matrix, config, next_session=next_session)
    return NamedSimpleBaselinePreview(
        scope=required_scope,
        receipt=verified.receipt,
        successful_dispatch=verified.successful_dispatch,
        candidates=candidates,
        source_row_count=counts[0],
        consumed_row_count=counts[1],
        excluded_before_window_row_count=counts[2],
        excluded_other_ticker_row_count=counts[3],
        calculation_environment=(
            ("python_implementation", platform.python_implementation()),
            ("python_version", platform.python_version()),
            ("pandas_version", pd.__version__),
            ("numpy_version", np.__version__),
        ),
    )
