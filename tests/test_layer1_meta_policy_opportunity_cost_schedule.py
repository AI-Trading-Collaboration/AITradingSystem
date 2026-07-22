from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system import layer1_simple_rule_meta_policy as policy


def test_opportunity_component_return_schedule_is_exact_for_nan_tail_and_missing_component() -> (
    None
):
    context = _context(include_qqq=True, include_nan=True)
    path = _path(context)
    schedule = policy._build_opportunity_component_return_schedule(context, path)

    assert policy._opportunity_costs_from_component_return_schedule(
        context,
        path,
        schedule,
    ) == policy._opportunity_costs(context, path)
    assert schedule.rows[-1].available_days == 0

    missing_context = _context(include_qqq=False, include_nan=True)
    missing_path = _path(missing_context)
    missing_schedule = policy._build_opportunity_component_return_schedule(
        missing_context,
        missing_path,
    )
    assert policy._opportunity_costs_from_component_return_schedule(
        missing_context,
        missing_path,
        missing_schedule,
    ) == policy._opportunity_costs(missing_context, missing_path)
    assert all(row.available_days == 0 for row in missing_schedule.rows)


@pytest.mark.parametrize(
    "mismatch",
    ["identity", "panel_identity", "shape", "date", "column", "return_column"],
)
def test_opportunity_component_return_schedule_mismatch_falls_back(
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    context = _context(include_qqq=True, include_nan=False)
    path = _path(context)
    schedule = policy._build_opportunity_component_return_schedule(context, path)
    candidate_context = context
    candidate_path = path
    if mismatch == "identity":
        candidate_context = dict(context)
    elif mismatch == "panel_identity":
        context["panel"] = context["panel"].copy()
    elif mismatch == "shape":
        panel = context["panel"]
        panel.loc[len(panel)] = panel.iloc[-1]
    elif mismatch == "date":
        candidate_path = dict(path)
        candidate_path.pop(next(iter(candidate_path)))
    elif mismatch == "column":
        context["panel"].rename(columns={"net_return": "return"}, inplace=True)
    elif mismatch == "return_column":
        context["panel"].loc[
            context["panel"]["strategy_id"] == "100_qqq",
            "strategy_id",
        ] = "renamed_qqq"

    expected = {"missed_rebound_cost": -1.0, "late_risk_off_cost": -2.0}
    calls = 0

    def fallback(
        fallback_context: object,
        fallback_path: object,
    ) -> dict[str, float]:
        nonlocal calls
        calls += 1
        assert fallback_context is candidate_context
        assert fallback_path is candidate_path
        return expected

    monkeypatch.setattr(policy, "_opportunity_costs", fallback)

    assert (
        policy._opportunity_costs_from_component_return_schedule(
            candidate_context,
            candidate_path,
            schedule,
        )
        == expected
    )
    assert calls == 1


def test_soft_blend_constrained_search_reuses_one_bounded_schedule_for_full_grid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = _context(include_qqq=True, include_nan=True)
    registry = policy._load_registry(policy.DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH)
    real_future_component_return: Callable[..., tuple[float, int]] = (
        policy._future_component_return
    )
    calls = 0

    def counted_future_component_return(*args: object, **kwargs: object) -> tuple[float, int]:
        nonlocal calls
        calls += 1
        return real_future_component_return(*args, **kwargs)

    monkeypatch.setattr(policy, "_future_component_return", counted_future_component_return)
    monkeypatch.setattr(policy, "_build_context", lambda **_kwargs: context)

    payload = policy.run_layer1_selector_soft_blend_constrained_search(
        output_root=tmp_path,
    )

    assert len(payload["soft_blend_constrained_search_rows"]) == 243
    assert calls == len(_dates(context)) * 2 - 1
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["summary"]["data_quality_status"] == "PASS"
    assert registry["evaluation_policy"]


def test_soft_blend_constrained_search_full_grid_matches_legacy_rows_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(include_qqq=True, include_nan=True)
    registry = policy._load_registry(policy.DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH)
    optimized_rows = policy._soft_blend_constrained_search_rows(context, registry)
    monkeypatch.setattr(
        policy,
        "_opportunity_component_return_schedule_matches",
        lambda *_args: False,
    )

    legacy_rows = policy._soft_blend_constrained_search_rows(context, registry)

    assert len(optimized_rows) == 243
    assert optimized_rows == legacy_rows


def _context(*, include_qqq: bool, include_nan: bool) -> dict[str, object]:
    dates = [f"2024-01-{day:02d}" for day in range(2, 9)]
    rows = []
    for index, day in enumerate(dates):
        rows.append(
            {
                "date": day,
                "strategy_id": "equal_risk_qqq_sgov",
                "net_return": float("nan") if include_nan and index == 4 else 0.001 * index,
            }
        )
        if include_qqq:
            rows.append(
                {
                    "date": day,
                    "strategy_id": "100_qqq",
                    "net_return": float("nan")
                    if include_nan and index == 2
                    else 0.002 * index,
                }
            )
    return {
        "panel": pd.DataFrame(rows),
        "config": {"research_policy": {"cost_assumption": {"base_cost_bps": 5.0}}},
        "data_quality_passed": True,
        "data_quality_status": "PASS",
        "source_artifacts": {},
    }


def _dates(context: dict[str, object]) -> list[str]:
    panel = context["panel"]
    assert isinstance(panel, pd.DataFrame)
    return sorted(str(value) for value in panel["date"].unique())


def _path(context: dict[str, object]) -> policy.BlendPath:
    return {
        day: {
            "100_qqq": 0.2 + index * 0.1,
            "equal_risk_qqq_sgov": 0.8 - index * 0.1,
        }
        for index, day in enumerate(_dates(context))
    }
