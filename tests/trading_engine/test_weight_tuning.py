from __future__ import annotations

import pytest

from ai_trading_system.trading_engine.parameters.weight_tuning import (
    generate_restricted_grid_candidates,
    load_weight_tuning_config,
)
from trading_engine.weight_tuning_helpers import BASELINE_WEIGHTS


def test_restricted_grid_preserves_fixed_fallbacks_and_caps() -> None:
    config = load_weight_tuning_config()

    candidates, rejected = generate_restricted_grid_candidates(config, BASELINE_WEIGHTS)

    assert candidates
    assert len(candidates) <= config["search"]["max_candidates"]
    assert rejected >= 0
    for candidate in candidates:
        weights = candidate["weights"]
        assert sum(weights.values()) == pytest.approx(1.0)
        assert weights["earnings_quality"] == pytest.approx(0.05)
        assert weights["event_risk"] == pytest.approx(0.05)
        assert 0.05 <= weights["valuation_risk"] <= 0.15
        assert candidate["l1_distance_from_baseline"] <= 0.50
        assert candidate["fallback_signals_free_tuned"] is False


def test_restricted_grid_rejects_free_fallback_signal_selection() -> None:
    config = load_weight_tuning_config()

    with pytest.raises(ValueError, match="fixed fallback signals"):
        generate_restricted_grid_candidates(
            config,
            BASELINE_WEIGHTS,
            selected_signals=("earnings_quality",),
        )
