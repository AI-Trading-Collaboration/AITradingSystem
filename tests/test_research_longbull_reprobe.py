"""Acceptance tests for the long-bull cost re-probe with a two-leg planted rule (TRADING-2565-66).

Synthetic data only: no snapshot is read, no real rule is run. The probe measures a ruler with
planted rules (owner_decision:TRADING-2565:2026-10-25:longbull_reprobe, charter p6 sec. 15) and
reuses TRADING-2565-61's cycle_power and TRADING-2565-63's longbull_power unchanged.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lp
from ai_trading_system.research_probes import longbull_reprobe as rp

SOURCE = Path(rp.__file__).read_text(encoding="utf-8")
CYCLE_POWER_SHA256 = "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc"
LONGBULL_POWER_SHA256 = "0fe7a2ecb0f9d9a8e276dce0b2b08f89be322d253d4d9b4029fcd7dc2230e904"


def _frames(
    seed: int = 1, vol: float = 0.012, end: str = "2012-06-29"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ken French shaped frames: decimal returns, Softw empty before 1965-07, rows past 2010."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", end)
    common = rng.normal(0.0006, vol, len(days))
    data = {
        name: common + rng.normal(0.0, extra, len(days))
        for name, extra in zip(cp.COLUMNS, (0.006, 0.009, 0.012), strict=True)
    }
    industries = pd.DataFrame(data, index=days)
    industries.loc[industries.index < pd.Timestamp("1965-07-01"), "Softw"] = np.nan
    industries["Food"] = 0.0
    excess = 0.7 * common + rng.normal(0.0, vol * 0.4, len(days))
    factors = pd.DataFrame(
        {"Mkt-RF": excess - 0.0001, "SMB": 0.0, "RF": np.full(len(days), 0.0001)}, index=days
    )
    return industries, factors


INDUSTRIES, FACTORS = _frames()
TECH = cp.market_frame(INDUSTRIES, FACTORS)
US = lp.us_frame(FACTORS)
FOUND, TAIL = cp.cycles(TECH)
US_FOUND, US_TAIL = cp.cycles(US)
KEYS = ["0", "3", "6", "9", "12"]


# ----- constants: the requirement's sections 2 to 4, fixed before any run -------------------------
def test_constants() -> None:
    assert rp.RUN_ID == "TRADING-2565-66"
    assert rp.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-25:longbull_reprobe"
    assert (rp.MONTHS, rp.HISTORIES, rp.DRAWS) == (984, 500, 1000)
    # The same seed as TRADING-2565-61 and -63: the same 500 tech histories and US histories.
    assert rp.SEED == lp.SEED == cp.SEED == 20261024
    assert rp.LAGS == (0, 3, 6, 9, 12)
    assert rp.FALSE_RATE == 0.35
    assert (rp.FAST_BACK, rp.HALF, cp.EXIT_MONTHS) == (2, 0.5, 1)
    assert rp.TERMS == ("reentry", "false", "full")
    assert (rp.MOVE, rp.BAND, rp.IDENTITY) == (0.003, 0.0036, 1e-9)
    assert lp.TARGET_SD == 0.0124 and rp.FIRST == 0
    assert (rp.DECOMPOSE, rp.DECOMPOSE_SUMMARY, rp.DECOMPOSE_REPORT) == (
        "decompose.jsonl",
        "decompose.json",
        "decompose.md",
    )
    assert (lp.CALIBRATION, lp.JOURNAL, lp.SUMMARY, lp.REPORT) == (
        "calibration.jsonl",
        "histories.jsonl",
        "summary.json",
        "report.md",
    )
    assert rp.CALIBRATION_KEYS == ("history", "drawn", "slow_bears", "long_bulls", "sd", "error")
    assert [lp.rate_key(lag) for lag in rp.LAGS] == KEYS


def test_cycle_power_and_longbull_power_are_unchanged() -> None:
    for module, digest in ((cp, CYCLE_POWER_SHA256), (lp, LONGBULL_POWER_SHA256)):
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest


# ----- the two-leg planted rule (requirement sec. 2) ----------------------------------------------
NO_FALSE = np.ones(39)


def test_plant_legs_by_hand() -> None:
    # One slow bear, peak in month 5 and trough in month 12, 40 months, no false exits.
    plant = rp.plant_legs([(5, 12)], 40, NO_FALSE, 6)
    expected = np.ones(40)
    expected[7:14] = 0.0  # both legs out from peak + 2; the fast leg back at trough + 2 = 14
    expected[14:19] = 0.5  # the slow leg back at trough + R + 1 = 19
    np.testing.assert_array_equal(plant.targets, expected)
    lag = np.zeros(40, dtype=bool)
    lag[13:19] = True  # after the trough, until both legs are back
    np.testing.assert_array_equal(plant.lag, lag)
    assert not plant.false.any()
    # R = 0: the slow leg is back at trough + 1, one month before the fast leg.
    plant = rp.plant_legs([(5, 12)], 40, NO_FALSE, 0)
    assert list(plant.targets[6:15]) == [1.0, 0, 0, 0, 0, 0, 0, 0.5, 1.0]
    assert list(np.flatnonzero(plant.lag)) == [13]
    # Two bears; a lag that runs past the end of the history is cut there.
    plant = rp.plant_legs([(2, 6), (20, 30)], 40, NO_FALSE, 12)
    assert plant.targets[3] == 1.0 and plant.targets[4] == 0.0
    assert plant.targets[8] == 0.5 and plant.targets[19] == 1.0
    assert plant.targets[32] == 0.5 and plant.targets[39] == 0.5
    assert list(np.flatnonzero(plant.lag)) == [*range(7, 19), *range(31, 40)]


def test_exposure_takes_only_zero_half_and_one() -> None:
    rng = np.random.default_rng(3)
    for lag in rp.LAGS:
        plant = rp.plant_legs([(10, 30), (80, 95), (150, 170)], 240, rng.random(239), lag)
        assert set(np.unique(plant.targets)) <= {0.0, 0.5, 1.0}
        assert (plant.targets[12:31] == 0.0).all()  # both legs out between peak + 2 and the trough


def test_false_exits_move_only_the_fast_leg_for_one_month() -> None:
    uniforms = np.ones(39)
    uniforms[2] = 0.1  # decided at the end of month 2: out in month 3
    uniforms[9] = 0.1  # month 10 is inside the bear: the fast leg is out already
    uniforms[15] = 0.34  # month 16: the slow leg is out after the trough, the fast leg is in
    uniforms[25] = 0.35  # not below q: no false exit
    plant = rp.plant_legs([(5, 12)], 40, uniforms, 6)
    assert list(np.flatnonzero(plant.false)) == [3, 16]
    assert plant.targets[3] == 0.5 and plant.targets[4] == 1.0  # one month, half the exposure
    assert plant.targets[16] == 0.0 and plant.targets[17] == 0.5
    assert plant.targets[10] == 0.0 and plant.targets[26] == 1.0
    # q = 0: no false exits at all.
    none = rp.plant_legs([(5, 12)], 40, np.zeros(39), 6, rate=0.0)
    assert not none.false.any()
    np.testing.assert_array_equal(none.targets, rp.plant_legs([(5, 12)], 40, NO_FALSE, 6).targets)
    with pytest.raises(ValueError, match="count - 1"):
        rp.plant_legs([(5, 12)], 40, np.ones(40), 6)
    with pytest.raises(ValueError, match="negative"):
        rp.plant_legs([(5, 12)], 40, NO_FALSE, -1)


def test_the_legs_read_no_returns() -> None:
    parameters = list(inspect.signature(rp.plant_legs).parameters)
    assert parameters == ["bears", "count", "uniforms", "lag", "rate"]
    # Same peaks, troughs and random numbers under two different return series: the same plant.
    rng = np.random.default_rng(4)
    made = cp.history(TECH, FOUND, rng, 360)
    part = cp.market(made.frame, "tech_ew")
    level = cp.month_level(part.risky, part.month)
    uniforms = rng.random(len(level) - 1)
    bears = cp.slow_bears(level)
    first = rp.plant_legs(bears, len(level), uniforms, 6)
    other = cp.market(cp.history(US, US_FOUND, np.random.default_rng(5), 360).frame, "tech_ew")
    assert not np.array_equal(other.risky[:100], part.risky[:100])
    second = rp.plant_legs(bears, len(level), uniforms, 6)
    np.testing.assert_array_equal(first.targets, second.targets)


def test_day_terms_put_the_reentry_first() -> None:
    uniforms = np.ones(39)
    uniforms[15] = 0.1  # a false exit inside the slow leg's lag counts as re-entry
    uniforms[25] = 0.1
    plant = rp.plant_legs([(5, 12)], 40, uniforms, 6)
    month = np.repeat(np.arange(40), 3)
    kind = rp.day_terms(plant, month)
    assert kind.shape == month.shape
    by_month = kind[::3]
    assert list(np.flatnonzero(by_month == 0)) == list(range(13, 19))
    assert list(np.flatnonzero(by_month == 1)) == [26]
    assert by_month[0] == 2 and by_month[30] == 2


# ----- the decomposition (requirement sec. 3) -----------------------------------------------------
def _market(seed: int = 6, months: int = 240) -> tuple[cp.Market, pd.Series, np.random.Generator]:
    rng = np.random.default_rng(seed)
    made = cp.history(US, US_FOUND, rng, months)
    part = cp.market(made.frame, "tech_ew")
    return part, cp.month_level(part.risky, part.month), rng


def test_three_terms_sum_to_the_exact_reading() -> None:
    part, level, rng = _market()
    bears, bulls = cp.slow_bears(level), lp.long_bulls(level)
    assert len(bulls) >= 2
    plant = rp.plant_legs(bears, len(level), rng.random(len(level) - 1), 6)
    parts, exposure = rp.split(part.risky, part.cash, part.month, bulls, plant)
    gross, held = cp.simulate(part.risky, part.cash, part.month, plant.targets)
    assert exposure == pytest.approx(held.mean(), abs=1e-15)
    reference, base = cp.simulate(part.risky, part.cash, part.month, np.full(len(level), exposure))
    difference = np.log(gross) - np.log(reference)
    np.testing.assert_allclose(parts.reading, lp.readings(difference, part.month, bulls))
    np.testing.assert_allclose(sum(parts.terms.values()), parts.reading, rtol=0, atol=1e-12)
    kind = rp.day_terms(plant, part.month)
    for i, name in enumerate(rp.TERMS):
        by_hand = lp.readings(np.where(kind == i, difference, 0.0), part.month, bulls)
        np.testing.assert_allclose(parts.terms[name], by_hand, rtol=0, atol=1e-15)
    # The first-order version: daily (exposure - benchmark exposure) x (risky - cash), per year.
    first = lp.readings((held - base) * (part.risky - part.cash), part.month, bulls)
    np.testing.assert_allclose(parts.first_order, first, rtol=0, atol=1e-15)
    simple = lp.simple_differences(np.log(gross), np.log(reference), part.month, bulls)
    np.testing.assert_allclose(parts.simple, simple, rtol=0, atol=1e-15)
    assert parts.spans == bulls


def test_a_us_history_by_hand() -> None:
    record = rp.us_history(US, US_FOUND, 99, 2, months=360)
    rng = np.random.default_rng([99, 2, 1])
    made = cp.history(US, US_FOUND, rng, 360)
    part = cp.market(made.frame, "tech_ew")
    level = cp.month_level(part.risky, part.month)
    bears, bulls = cp.slow_bears(level), lp.long_bulls(level)
    uniforms = rng.random(len(level) - 1)
    assert set(record) == {*rp.CALIBRATION_KEYS, "parts"}
    assert (record["history"], record["drawn"], record["error"]) == (2, list(made.drawn), None)
    assert (record["slow_bears"], record["long_bulls"]) == (len(bears), len(bulls))
    assert list(record["sd"]) == KEYS and list(record["parts"]) == KEYS
    for lag in rp.LAGS:
        key = lp.rate_key(lag)
        parts, _ = rp.split(
            part.risky,
            part.cash,
            part.month,
            bulls,
            rp.plant_legs(bears, len(level), uniforms, lag),
        )
        item = record["parts"][key]
        assert set(item) == {"reentry", "false", "full", "reading", "simple", "gap", "identity"}
        assert item["simple"] == record["sd"][key] == pytest.approx(lp.sample_sd(parts.simple))
        assert item["reentry"] == pytest.approx(lp.sample_sd(parts.terms["reentry"]))
        assert item["reading"] == pytest.approx(lp.sample_sd(parts.reading))
        gaps = np.abs(parts.first_order - parts.reading)
        assert item["gap"] == pytest.approx(float(np.median(gaps)))
        assert 0 <= item["identity"] <= 1e-12


def test_the_us_histories_are_those_of_trading_2565_63() -> None:
    for index in (0, 3):
        ours = rp.us_history(US, US_FOUND, rp.SEED, index, months=360)
        theirs = lp.calibration_history(US, US_FOUND, rp.SEED, index, months=360)
        assert rp.same_draws(ours, theirs)
        assert {k: ours[k] for k in ("drawn", "slow_bears", "long_bulls")} == {
            k: theirs[k] for k in ("drawn", "slow_bears", "long_bulls")
        }


def test_same_draws_compares_cycles_and_both_counts() -> None:
    ours = {"drawn": ["c1", "c2"], "slow_bears": {"tech_ew": 3}, "long_bulls": {"tech_ew": 4}}
    assert rp.same_draws(ours, {**ours, "readings": {}})
    for key, value in (
        ("drawn", ["c2", "c1"]),
        ("slow_bears", {"tech_ew": 2}),
        ("long_bulls", {"tech_ew": 5}),
    ):
        assert not rp.same_draws(ours, {**ours, key: value})


def test_first_rows_by_hand() -> None:
    rows = rp.first_rows(US, US_FOUND, 7, months=360)
    made = rp.us_parts(US, US_FOUND, 7, rp.FIRST, months=360)
    counts = {key: len(parts.spans) for key, parts in made.parts.items()}
    assert [row["lag"] for row in rows] == [k for k in KEYS for _ in range(counts[k])]
    row = rows[0]
    parts = made.parts["0"]
    assert (row["segment"], row["first"], row["last"]) == (1, *parts.spans[0])
    assert row["years"] == (parts.spans[0][1] - parts.spans[0][0] + 1) / 12
    assert row["sum"] == pytest.approx(row["reentry"] + row["false"] + row["full"])
    assert row["sum"] == pytest.approx(row["reading"], abs=1e-12)
    assert row["gap"] == pytest.approx(row["first_order"] - row["reading"])
    assert row["reading"] == pytest.approx(float(parts.reading[0]))


def _table(**overrides: dict[str, float]) -> dict[str, dict[str, Any]]:
    base = {"reentry": 0.004, "false": 0.003, "full": 0.008, "simple": 0.02}
    return {
        k: {"histories": 9, **base, "reading": 0.01, "gap": 0.001, **overrides.get(k, {})}
        for k in KEYS
    }


def _records(residual: float = 0.0) -> list[dict[str, Any]]:
    return [{"parts": {k: {"identity": residual if k == "6" else 0.0} for k in KEYS}}]


def test_check_reads_the_three_items_as_written() -> None:
    table = _table(
        **{
            "0": {"reentry": 0.002, "full": 0.009},
            "6": {"reentry": 0.010, "simple": 0.0124 + 0.0036},
            "12": {"reentry": 0.012, "simple": 0.0124 - 0.0036},
        }
    )
    # (a): at the selected R the largest term is re-entry; it moves 0.012 - 0.002 >= 0.003.
    checks = rp.check(table, 6, _records())
    assert checks["a"]["term"] == "reentry"
    assert checks["a"]["spread"] == pytest.approx(0.010)
    assert checks["a"]["holds"]
    # (b): the band 1.24 +- 0.36 points includes both ends.
    assert checks["b"] == {"lags": ["6", "12"], "holds": True}
    assert checks["c"] == {"residual": 0.0, "holds": True}
    # At R = 0 the largest term is the full one, which does not move enough.
    checks = rp.check(table, 0, _records())
    assert checks["a"]["term"] == "full"
    assert checks["a"]["spread"] == pytest.approx(0.001)
    assert not checks["a"]["holds"]
    # A move of exactly 0.30 points holds; just under does not.
    exact = _table(**{"6": {"full": 0.011}})
    assert rp.check(exact, 6, _records())["a"]["holds"]
    under = _table(**{"6": {"full": 0.011 - 1e-6}})
    assert not rp.check(under, 6, _records())["a"]["holds"]
    outside = _table(**{"3": {"simple": 0.0124 + 0.0036 + 1e-6}})
    assert rp.check(outside, 6, _records())["b"] == {"lags": [], "holds": False}
    # (b) fails when every R is outside the band; (c) fails above 1e-9.
    assert rp.check(_table(), 6, _records())["b"] == {"lags": [], "holds": False}
    assert rp.check(_table(), 6, _records(1e-9))["c"]["holds"]
    assert not rp.check(_table(), 6, _records(2e-9))["c"]["holds"]
    # No selected R: (a) cannot hold.
    assert rp.check(_table(), None, _records())["a"] == {
        "term": None,
        "spread": None,
        "holds": False,
    }


def test_summarize_takes_medians_and_selects_r_like_longbull_power() -> None:
    def record(
        long_bulls: int, simple: dict[str, float | None], gap: float, scale: float = 1.0
    ) -> dict[str, Any]:
        parts = {
            k: {
                "reentry": 0.001 * (i + 1) * scale if simple[k] is not None else None,
                "false": 0.002 * scale if simple[k] is not None else None,
                "full": 0.003 * scale if simple[k] is not None else None,
                "reading": 0.004 * scale if simple[k] is not None else None,
                "simple": simple[k],
                "gap": gap,
                "identity": 0.0,
            }
            for i, k in enumerate(KEYS)
        }
        return {
            "history": 0,
            "drawn": [],
            "slow_bears": 9,
            "long_bulls": long_bulls,
            "sd": simple,
            "parts": parts,
            "error": None,
        }

    near = {"0": 0.020, "3": 0.016, "6": 0.0124 + 0.002, "9": 0.0124 - 0.002, "12": 0.008}
    records = [
        record(2, near, 0.004),
        record(1, dict.fromkeys(KEYS), 0.002),
        record(5, near, 0.006, scale=2.0),
        record(3, near, 0.020, scale=6.0),
    ]
    result = rp.summarize(records, [{"lag": "0"}])
    # The same calibration as TRADING-2565-63: medians over histories with at least two segments;
    # equally near takes the smaller R.
    assert result["selected"] == 6
    assert (
        result["selected"]
        == lp.calibrate([{k: r[k] for k in rp.CALIBRATION_KEYS} for r in records], rates=rp.LAGS)[
            "selected"
        ]
    )
    assert result["histories"] == 4
    assert result["lags"]["3"]["histories"] == 3
    assert result["lags"]["3"]["simple"] == pytest.approx(0.016)
    # Medians, not means, of the three judged histories (scales 1, 2 and 6).
    assert result["lags"]["3"]["reentry"] == pytest.approx(0.004)
    assert result["lags"]["3"]["full"] == pytest.approx(0.006)
    assert result["lags"]["3"]["reading"] == pytest.approx(0.008)
    assert result["lags"]["3"]["gap"] == pytest.approx(0.005)  # the median over all histories
    assert result["first_history"] == [{"lag": "0"}]
    assert set(result) == {"histories", "lags", "selected", "checks", "all_hold", "first_history"}
    assert result["all_hold"] == all(c["holds"] for c in result["checks"].values())


# ----- the tech histories (requirement sec. 1 and 2) ----------------------------------------------
def test_the_tech_histories_are_those_of_trading_2565_61_and_63() -> None:
    ours = rp.evaluate_history(TECH, FOUND, rp.SEED, 3, months=240, draws=20)
    theirs = lp.evaluate_history(TECH, FOUND, rp.SEED, 3, months=240, draws=20)
    assert rp.same_draws(ours, theirs)
    for key in ("drawn", "days", "slow_bears", "long_bulls", "segments"):
        assert ours[key] == theirs[key], key
    plant_base = cp.evaluate_history(
        TECH, FOUND, rp.SEED, 3, months=240, draws=20, blocks=(21,), confidences=(0.95,)
    )
    assert ours["slow_bears"] == plant_base["slow_bears"]


@pytest.mark.parametrize(("index", "months"), [(4, 360), (0, 240)])
def test_a_tech_history_by_hand(index: int, months: int) -> None:
    record = rp.evaluate_history(
        TECH, FOUND, 99, index, months=months, draws=50, confidences=(0.99,)
    )
    rng = np.random.default_rng([99, index])
    made = cp.history(TECH, FOUND, rng, months)
    pooled: dict[str, dict[str, list[float]]] = {v: {k: [] for k in KEYS} for v in cp.VARIANTS}
    for name in cp.MARKETS:
        part = cp.market(made.frame, name)
        if not len(part.rows):
            continue
        level = cp.month_level(part.risky, part.month)
        bears, bulls = cp.slow_bears(level), lp.long_bulls(level)
        uniforms = rng.random(len(level) - 1)
        variant = "tech_ew" if name == "tech_ew" else "three"
        for lag in rp.LAGS:
            plant = rp.plant_legs(bears, len(level), uniforms, lag)
            gross, held = cp.simulate(part.risky, part.cash, part.month, plant.targets)
            exposure = float(held.mean())
            assert record["matched_exposure"][lp.rate_key(lag)][name] == pytest.approx(exposure)
            reference, _ = cp.simulate(
                part.risky, part.cash, part.month, np.full(len(level), exposure)
            )
            values = lp.readings(np.log(gross) - np.log(reference), part.month, bulls)
            pooled[variant][lp.rate_key(lag)].extend(values.tolist())
    for variant in cp.VARIANTS:
        assert record["segments"][variant] == len(pooled[variant]["0"])
        for key in KEYS:
            np.testing.assert_allclose(record["readings"][variant][key], pooled[variant][key])
            if pooled[variant][key]:
                assert record["point"][variant][key] == pytest.approx(np.mean(pooled[variant][key]))
    picks = {
        v: lp.boot_picks(len(pooled[v]["0"]), 50, rng) if len(pooled[v]["0"]) >= 2 else None
        for v in cp.VARIANTS
    }
    for variant in cp.VARIANTS:
        for key in KEYS:
            expected = lp.ruler_bounds(np.asarray(pooled[variant][key]), picks[variant], (0.99,))
            assert record["bounds"][variant][key] == expected


# ----- the runs -----------------------------------------------------------------------------------
SMALL: dict[str, Any] = {"histories": 4, "draws": 40, "months": 360}


def _run(tech: pd.DataFrame = TECH, us: pd.DataFrame = US, **kwargs: Any) -> dict[str, Any]:
    # The synthetic market does not pass the decompose checks; widen the two lines to run step 2.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(rp, "MOVE", 0.0)
        patch.setattr(rp, "BAND", 1.0)
        return rp.run_probe(tech, us, **{**SMALL, **kwargs})


@pytest.fixture(scope="module")
def small() -> dict[str, Any]:
    return _run()


@pytest.fixture(scope="module")
def decomposed() -> dict[str, Any]:
    return rp.decompose_probe(US, histories=4, months=360)


def test_decompose_probe_on_a_small_synthetic_run(
    decomposed: dict[str, Any], tmp_path: Path
) -> None:
    result = rp.decompose_probe(US, histories=4, months=360, out_dir=tmp_path)
    assert json.dumps(result, sort_keys=True) == json.dumps(decomposed, sort_keys=True)
    assert (result["run_id"], result["owner_decision"], result["step"]) == (
        rp.RUN_ID,
        rp.OWNER_DECISION,
        "decompose",
    )
    assert result["cycles"] == len(US_FOUND) and result["errors"] == []
    item = result["decompose"]
    assert list(item["lags"]) == KEYS and item["histories"] == 4
    assert set(item["checks"]) == {"a", "b", "c"} and item["checks"]["c"]["holds"]
    assert item["first_history"] == rp.first_rows(US, US_FOUND, rp.SEED, months=360)
    lines = (tmp_path / "decompose.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["history"] for line in lines] == [0, 1, 2, 3]
    assert set(json.loads(lines[0])) == {"history", "drawn", "long_bulls", "parts", "error"}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["decompose.jsonl"]


def test_run_probe_on_a_small_synthetic_run(small: dict[str, Any], tmp_path: Path) -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(rp, "MOVE", 0.0)
        patch.setattr(rp, "BAND", 1.0)
        result = rp.run_probe(TECH, US, out_dir=tmp_path, **SMALL)
    assert json.dumps(result, sort_keys=True) == json.dumps(small, sort_keys=True)
    assert (result["run_id"], result["snapshot_id"], result["step"]) == (
        rp.RUN_ID,
        cp.SNAPSHOT_ID,
        "run",
    )
    assert len(result["cycles"]["tech_ew"]) == len(FOUND)
    assert result["cycles"]["us_market"] == len(US_FOUND)
    assert result["tail"]["tech_ew"] == {"first": TAIL[0], "last": "2010-12", "months": len(TAIL)}
    assert result["errors"] == {"calibration": [], "histories": []}
    assert result["same_as_previous"] == {"calibration": True, "histories": True}
    assert set(result["calibration"]["rates"]) == set(KEYS)
    assert result["calibration"]["selected"] in rp.LAGS
    assert result["decompose"]["selected"] == result["calibration"]["selected"]
    assert result["decompose"]["all_hold"]
    for name, rows in (("calibration.jsonl", 4), ("decompose.jsonl", 4), ("histories.jsonl", 4)):
        lines = (tmp_path / name).read_text(encoding="utf-8").splitlines()
        assert [json.loads(line)["history"] for line in lines] == list(range(rows))
    for variant in cp.VARIANTS:
        item = result["variants"][variant]
        assert item["reading"]["verdict"] in {"ready", "large_only", "below_50", "none_eligible"}
        # All five R are simulated and tabled; the selected one is judged.
        assert list(item["tables"]) == KEYS
        assert all(len(t["rows"]) == 6 for t in item["tables"].values())
        selected = lp.rate_key(result["calibration"]["selected"])
        assert item["reading"] == lp.choose(item["tables"][selected]["rows"])
    assert result["parameters"]["lags"] == list(rp.LAGS)
    assert result["parameters"]["false_rate"] == 0.35
    assert set(result["long_bull_counts"]) == {"tech_ew", "Hardw", "Chips", "Softw", "three"}
    assert set(result["real_long_bulls"]) == {"tech_ew", "Hardw", "Chips", "Softw", "us_market"}


def test_the_us_market_prints_no_cost(small: dict[str, Any], tmp_path: Path) -> None:
    _run(out_dir=tmp_path)
    line = json.loads((tmp_path / "calibration.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert set(line) == set(rp.CALIBRATION_KEYS)
    line = json.loads((tmp_path / "decompose.jsonl").read_text(encoding="utf-8").splitlines()[0])
    for item in line["parts"].values():
        assert set(item) == {"reentry", "false", "full", "reading", "simple", "gap", "identity"}
    assert set(small["calibration"]) == {"rates", "selected", "long_bulls", "slow_bears"}
    for row in small["decompose"]["lags"].values():
        assert set(row) == {"histories", "reentry", "false", "full", "reading", "simple", "gap"}
    text = rp.render(small)
    section = text.split("## 定标（美股大盘，只用来选 R）")[1].split("## ")[0]
    assert "c₀" not in section
    assert "us_market" not in "".join(
        line for line in text.splitlines() if line.startswith("## ") and "·" in line
    )


def test_run_stops_when_the_decomposition_does_not_hold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no tech history is evaluated")

    monkeypatch.setattr(rp, "evaluate_history", never)
    monkeypatch.setattr(rp, "MOVE", 1.0)  # no term moves by 100 points: (a) fails
    result = rp.run_probe(TECH, US, out_dir=tmp_path, **SMALL)
    assert not result["decompose"]["checks"]["a"]["holds"]
    assert result["errors"] == {"calibration": [], "histories": []}
    assert (tmp_path / "histories.jsonl").read_text(encoding="utf-8") == ""
    assert not result["decompose"]["all_hold"]
    assert result["calibration"]["selected"] in rp.LAGS
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}
    assert result["same_as_previous"] == {"calibration": True, "histories": None}
    text = rp.render(result)
    assert "拆解的三件没有都成立，第 2 步没有跑科技行业。" in text.splitlines()
    assert "- 科技行业：抽到的周期、慢熊段数、长牛段数逐条相同：–（没有比）" in text.splitlines()


def test_the_same_seed_gives_the_same_bytes(
    small: dict[str, Any], decomposed: dict[str, Any], tmp_path: Path
) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    rp.write_result(small, first)
    rp.write_result(_run(), second)
    rp.write_decompose(decomposed, first)
    rp.write_decompose(rp.decompose_probe(US, histories=4, months=360), second)
    for name in ("summary.json", "report.md", "decompose.json", "decompose.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name
    other = _run(seed=rp.SEED + 1)
    assert json.dumps(other["variants"], sort_keys=True) != json.dumps(
        small["variants"], sort_keys=True
    )


def test_nothing_after_2010_reaches_the_probe(
    small: dict[str, Any], decomposed: dict[str, Any]
) -> None:
    industries, factors = INDUSTRIES.copy(), FACTORS.copy()
    after = pd.DatetimeIndex(industries.index) > pd.Timestamp("2010-12-31")
    industries.loc[after, list(cp.COLUMNS)] = 5.0
    factors.loc[pd.DatetimeIndex(factors.index) > pd.Timestamp("2010-12-31"), ["Mkt-RF", "RF"]] = (
        np.nan
    )
    tech, us = cp.market_frame(industries, factors), lp.us_frame(factors)
    assert json.dumps(_run(tech, us), sort_keys=True) == json.dumps(small, sort_keys=True)
    again = rp.decompose_probe(us, histories=4, months=360)
    assert json.dumps(again, sort_keys=True) == json.dumps(decomposed, sort_keys=True)


def test_any_failed_history_makes_the_run_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    real_tech, real_us = rp.evaluate_history, rp.us_history

    def flaky(frame: pd.DataFrame, found: Any, seed: int, index: int, **kwargs: Any) -> Any:
        if index == 1:
            raise RuntimeError("boom")
        return real_tech(frame, found, seed, index, **kwargs)

    monkeypatch.setattr(rp, "evaluate_history", flaky)
    result = _run()
    assert result["errors"] == {
        "calibration": [],
        "histories": [{"history": 1, "error": "RuntimeError: boom"}],
    }
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}

    def flaky_us(frame: pd.DataFrame, found: Any, seed: int, index: int, **kwargs: Any) -> Any:
        if index == 2:
            raise ValueError("bad\nsecond line")
        return real_us(frame, found, seed, index, **kwargs)

    monkeypatch.setattr(rp, "evaluate_history", real_tech)
    monkeypatch.setattr(rp, "us_history", flaky_us)
    result = _run()
    assert result["errors"]["calibration"] == [{"history": 2, "error": "ValueError: bad"}]
    assert result["decompose"] is None
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}
    decomposed = rp.decompose_probe(US, histories=4, months=360)
    assert decomposed["errors"] == [{"history": 2, "error": "ValueError: bad"}]
    assert decomposed["decompose"] is None
    assert "拆解没有结果（周期不足或历史出错），不能跑第 2 步。" in rp.render_decompose(decomposed)


@pytest.mark.parametrize("calm_market", ["tech", "us"])
def test_fewer_than_eight_cycles_runs_nothing(
    monkeypatch: pytest.MonkeyPatch, calm_market: str, tmp_path: Path
) -> None:
    industries, factors = _frames(vol=0.003)
    tech = cp.market_frame(industries, factors) if calm_market == "tech" else TECH
    us = lp.us_frame(factors) if calm_market == "us" else US
    assert len(cp.cycles(tech if calm_market == "tech" else us)[0]) < 8

    def never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no history is evaluated")

    monkeypatch.setattr(rp, "evaluate_history", never)
    monkeypatch.setattr(rp, "us_history", never)
    result = rp.run_probe(tech, us, **SMALL)
    assert result["calibration"] is None and result["decompose"] is None
    assert result["errors"] == {"calibration": [], "histories": []}
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}
    assert "incomplete" in rp.render(result)
    if calm_market == "us":
        again = rp.decompose_probe(us, histories=4, months=360, out_dir=tmp_path)
        assert again["decompose"] is None and again["errors"] == []
        assert (tmp_path / "decompose.jsonl").read_text(encoding="utf-8") == ""


# ----- reports and commands -----------------------------------------------------------------------
def test_decompose_report_lines(decomposed: dict[str, Any]) -> None:
    text = rp.render_decompose(decomposed)
    lines = text.splitlines()
    for line in (
        "# 长牛代价重测：第 1 步拆解（TRADING-2565-66）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-25:longbull_reprobe`"
        "（第 6 期章程第 15 节）。",
        "- 数据：信号快照 `sig-7ad8aa00b736` 的 Mkt-RF 和 RF，只读到 2010-12-31；"
        "只用美股大盘的合成历史，不读科技行业。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 埋的规则：两条腿各管一半敞口；慢熊里两条腿都在峰后第 2 个月退出，"
        "快腿在谷底后第 2 个月回来，慢腿在谷底后第 R + 1 个月回来；其余月份快腿每月以概率 "
        "q = 0.35 假退出一个月。腿的状态只看峰、谷和随机数，不看收益。",
        "- 第 0 条历史的逐段数是埋的规则在一条合成历史上的数，不是任何真实规则的代价。",
        "### 读法（跑之前写死）",
        "### 每个 R 的段间标准差中位（4 条历史，点）",
        "### 第 0 条历史的逐段拆解（埋的规则，点/年）",
        "production_effect=none；broker_action=none。",
    ):
        assert line in lines, line
    verdict = (
        "三件都成立：可以跑第 2 步（run）。"
        if decomposed["decompose"]["all_hold"]
        else "有一件不成立：停下，不跑第 2 步，回来改规格。"
    )
    assert verdict in lines
    assert any(line.startswith("| (a) | 选中的 R 上段间标准差中位最大的那一项") for line in lines)
    assert any("1.24 ± 0.36 点" in line and line.startswith("| (b) |") for line in lines)
    assert any(line.startswith("| (c) | 三项之和等于精确读数") for line in lines)
    table = [line for line in lines if line.startswith("| ") and " | 是 |" in line]
    assert len(table) == 1  # the selected R, once
    rows = decomposed["decompose"]["first_history"]
    assert sum(line.startswith(f"| {rows[0]['lag']} | 1 | ") for line in lines) == 1


def test_report_lines(small: dict[str, Any]) -> None:
    text = rp.render(small)
    selected = lp.rate_key(small["calibration"]["selected"])
    lines = text.splitlines()
    for line in (
        "# 长牛代价重测：两条腿的埋规则（TRADING-2565-66）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-25:longbull_reprobe`"
        "（第 6 期章程第 15 节）。",
        "- 数据：信号快照 `sig-7ad8aa00b736`，Hardw、Chips、Softw、Mkt-RF 和 RF，"
        "只读到 2010-12-31。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 读数、档位、两把尺子和读法都照 TRADING-2565-63；档位和「下界 > −1.67」都是对数点。",
        "## 判定",
        f"选中的 R：{selected}（慢腿在谷底后第 R + 1 个月回来）；q = 0.35。",
        "## 定标（美股大盘，只用来选 R）",
        "## 和 TRADING-2565-63 同一批历史（只印）",
        "- 美股大盘：抽到的周期、慢熊段数、长牛段数逐条相同：True",
        "- 科技行业：抽到的周期、慢熊段数、长牛段数逐条相同：True",
        "## 周期和真实的长牛段（只印）",
        "## 每条历史的长牛段个数",
        "## 每条历史重新划出的慢熊段数（和 TRADING-2565-61、-63 同一批历史）",
        "## 附录：第 1 步的拆解",
        "production_effect=none；broker_action=none。",
    ):
        assert line in lines, line
    for variant in cp.VARIANTS:
        assert f"## {variant} · R = {selected}（判的口径）" in lines
        for key in KEYS:
            if key != selected:
                assert f"## {variant} · R = {key}（只印）" in lines
    assert any(line.startswith("选中的 R 在拆解表里的那一行") for line in lines)
    assert any("下界 > −1.67" in line and "| 合格 |" in line for line in lines)
    # The appendix carries step 1's reading rules and both tables as they are.
    appendix = "\n".join(rp.decompose_lines(small["decompose"]))
    assert appendix in text.split("## 附录：第 1 步的拆解")[1]


def test_commands_write_their_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rp, "load_frames", lambda: (TECH, US))
    monkeypatch.setattr(rp, "HISTORIES", 2)
    monkeypatch.setattr(rp, "DRAWS", 30)
    monkeypatch.setattr(rp, "MONTHS", 240)
    monkeypatch.setattr(rp, "MOVE", 1.0)
    out = tmp_path / "decompose"
    result = CliRunner().invoke(rp.app, ["decompose", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out.iterdir()) == [
        "decompose.json",
        "decompose.jsonl",
        "decompose.md",
    ]
    summary = json.loads((out / "decompose.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["histories"] == 2 and summary["parameters"]["months"] == 240
    assert "longbull_reprobe: calibration 2/2" in result.output
    assert "longbull_reprobe: 拆解有一件不成立" in result.output
    monkeypatch.setattr(rp, "MOVE", 0.0)
    monkeypatch.setattr(rp, "BAND", 1.0)
    out = tmp_path / "run"
    result = CliRunner().invoke(rp.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out.iterdir()) == [
        "calibration.jsonl",
        "decompose.jsonl",
        "histories.jsonl",
        "report.md",
        "summary.json",
    ]
    assert len((out / "histories.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["histories"] == 2 and summary["parameters"]["draws"] == 30
    assert "longbull_reprobe: histories 2/2" in result.output
    assert "longbull_reprobe: tech_ew 判定" in result.output
    assert "longbull_reprobe: three 判定" in result.output


@pytest.mark.parametrize("command", ["decompose", "run"])
def test_commands_report_a_load_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
) -> None:
    def broken() -> tuple[pd.DataFrame, pd.DataFrame]:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(rp, "load_frames", broken)
    result = CliRunner().invoke(rp.app, [command, "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "longbull_reprobe: 数据载入失败：signal snapshot is unchecked or failed" in result.output
    assert not (tmp_path / "out").exists()


def test_the_probe_reuses_both_modules_and_runs_no_real_rule() -> None:
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    assert "from ai_trading_system.research_probes import cycle_power as cp" in imports
    assert "from ai_trading_system.research_probes import longbull_power as lp" in imports
    for banned in (
        "research_screens",
        "signal_strategy",
        "fixed_strategy",
        "research_bench",
        "research_data",
        "forward_ledger",
        "qqq",
    ):
        assert not any(banned in line for line in imports), banned
    for copied in (
        "def history",
        "def cycles",
        "def plant_targets",
        "def simulate",
        "def long_bulls",
        "def readings",
        "def simple_differences",
        "def t_critical",
        "def ruler_rows",
        "def choose",
        "def calibrate",
        "def us_frame",
    ):
        assert copied not in SOURCE, copied
    assert "2011" not in SOURCE and "load_snapshot" not in SOURCE
