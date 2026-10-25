"""Acceptance tests for the planted rule on the real US path (TRADING-2565-68).

Synthetic data only: no snapshot is read, no real rule is run. The probe puts TRADING-2565-66's
two-leg planted rule on the single real US market path (owner_decision:TRADING-2565:2026-10-25:
longbull_realpath, charter p6 sec. 16) and reuses cycle_power, longbull_power and
longbull_reprobe unchanged.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lp
from ai_trading_system.research_probes import longbull_realpath as rr
from ai_trading_system.research_probes import longbull_reprobe as rp

SOURCE = Path(rr.__file__).read_text(encoding="utf-8")
UNCHANGED = {
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
    lp: "0fe7a2ecb0f9d9a8e276dce0b2b08f89be322d253d4d9b4029fcd7dc2230e904",
    rp: "3592f15ac24a1637ec0889b81ffd70eb117c3c98d3ccdecf30645b4fdd6df6ae",
}
KEYS = ["0", "3", "6", "9", "12"]


def _factors(seed: int = 1, vol: float = 0.012) -> pd.DataFrame:
    """Ken French shaped factors: decimal returns, rows before 1929 and past 2010."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", "2012-06-29")
    common = rng.normal(0.0006, vol, len(days))
    excess = 0.7 * common + rng.normal(0.0, vol * 0.4, len(days))
    return pd.DataFrame(
        {"Mkt-RF": excess - 0.0001, "SMB": 0.0, "RF": np.full(len(days), 0.0001)}, index=days
    )


FACTORS = _factors()
US = lp.us_frame(FACTORS)
PATH = rr.real_path(US)
FOUND = rr.span_months(PATH.level, PATH.bulls)


# ----- constants: charter p6 sec. 16 and the spec's sections 2 and 3, fixed before the run -------
def test_constants() -> None:
    assert rr.RUN_ID == "TRADING-2565-68"
    assert rr.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-25:longbull_realpath"
    assert (rr.SETS, rr.SEED) == (500, 20261024) and rr.SEED == lp.SEED
    assert rr.LAGS == rp.LAGS == (0, 3, 6, 9, 12)
    assert rr.SELECTED == 3
    assert (rr.QUIET, rr.NOISY) == (0.016, 0.019)
    assert rr.QUIET == pytest.approx(lp.TARGET_SD + rp.BAND)
    assert (rr.SUMMARY, rr.REPORT) == ("realpath.json", "realpath.md")
    assert rr.LONG_BULLS == (
        ("1933-03", "1937-02"),
        ("1942-05", "1946-05"),
        ("1947-06", "1961-12"),
        ("1962-07", "1968-11"),
        ("1974-10", "1987-08"),
        ("1987-12", "2000-03"),
        ("2002-10", "2007-10"),
    )
    assert (rr.F0001_EXPOSURE, rr.F0001_BULL_EXPOSURE) == (0.656, 0.747)
    assert rr.SLACK == 1e-12
    assert rr.VERDICTS == ("path_quiet", "both", "plant_noisier", "incomplete")


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_and_never_touches_tech() -> None:
    for line in (
        "from ai_trading_system.research_probes import cycle_power as cp",
        "from ai_trading_system.research_probes import longbull_power as lp",
        "from ai_trading_system.research_probes import longbull_reprobe as rp",
    ):
        assert line in SOURCE
    for name in (
        "def plant_legs",
        "def split",
        "def simulate",
        "def readings",
        "def simple_differences",
        "def long_bulls",
        "def slow_bears",
        "def month_level",
        "def us_frame",
        "def sample_sd",
    ):
        assert name not in SOURCE
    for text in ("2011", "market_frame", "INDUSTRIES", "load_frame", "Hardw", "tech_ew"):
        assert text not in SOURCE
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    for banned in ("research_bench", "strategies", "screen", "forward", "qqq"):
        assert not any(banned in line for line in imports), banned
    assert SOURCE.count("load_snapshot") == 1


# ----- the real path ------------------------------------------------------------------------------
def test_real_path_uses_the_shared_segmentation() -> None:
    assert len(PATH.level) == 984 and PATH.level.index[0] == cp.FIRST_MONTH
    assert len(PATH.risky) == len(PATH.cash) == len(PATH.month) == len(US)
    assert PATH.month[0] == 0 and PATH.month[-1] == 983
    np.testing.assert_array_equal(PATH.risky, US[cp.TECH].to_numpy(dtype=float))
    np.testing.assert_array_equal(PATH.cash, US[cp.CASH].to_numpy(dtype=float))
    level = cp.month_level(PATH.risky, PATH.month)
    pd.testing.assert_series_equal(PATH.level, level)
    assert PATH.bears == cp.slow_bears(level) and PATH.bulls == lp.long_bulls(level)
    assert len(PATH.bears) >= 2 and len(PATH.bulls) >= 2
    assert FOUND == [
        (str(level.index[first]), str(level.index[last])) for first, last in PATH.bulls
    ]


def test_real_path_must_start_in_1929_01() -> None:
    with pytest.raises(ValueError, match="the path must start in 1929-01"):
        rr.real_path(US.loc[US.index >= pd.Timestamp("1929-02-01")])
    with pytest.raises(ValueError, match="the path must start in 1929-01"):
        rr.real_path(US.iloc[:0])


def test_uniforms_and_inside() -> None:
    np.testing.assert_array_equal(
        rr.uniforms_for(7, 3, 11), np.random.default_rng([7, 3, 2]).random(10)
    )
    assert len(rr.uniforms_for(rr.SEED, 0, 984)) == 983
    month = np.array([0, 0, 1, 2, 3, 3, 4, 5])
    assert rr.inside(month, [(1, 2), (5, 5)]).tolist() == [0, 0, 1, 1, 0, 0, 0, 1]
    assert not rr.inside(month, []).any()


# ----- one set of false exits --------------------------------------------------------------------
def test_one_set_is_the_reprobe_split_on_the_real_path() -> None:
    uniforms = rr.uniforms_for(rr.SEED, 4, len(PATH.level))
    item = rr.one_set(PATH, 3, uniforms)
    plant = rp.plant_legs(PATH.bears, len(PATH.level), uniforms, 3)
    parts, exposure = rp.split(PATH.risky, PATH.cash, PATH.month, PATH.bulls, plant)
    _, held = cp.simulate(PATH.risky, PATH.cash, PATH.month, plant.targets)
    assert set(item) == {
        "simple",
        "reading",
        "reentry",
        "false",
        "full",
        "exposure",
        "bull_exposure",
        "segment_simple",
        "segment_exposure",
    }
    assert item["simple"] == lp.sample_sd(parts.simple)
    assert item["reading"] == lp.sample_sd(parts.reading)
    for name in rp.TERMS:
        assert item[name] == lp.sample_sd(parts.terms[name])
    assert item["exposure"] == exposure == pytest.approx(held.mean())
    bull = rr.inside(PATH.month, PATH.bulls)
    assert item["bull_exposure"] == pytest.approx(held[bull].mean())
    assert item["segment_simple"] == pytest.approx(list(parts.simple))
    assert len(item["segment_exposure"]) == len(PATH.bulls)
    first, last = PATH.bulls[1]
    days = (PATH.month >= first) & (PATH.month <= last)
    assert item["segment_exposure"][1] == pytest.approx(held[days].mean())
    json.dumps(item, allow_nan=False)


def test_one_set_without_false_exits_is_full_in_the_bulls() -> None:
    item = rr.one_set(PATH, 0, np.zeros(len(PATH.level) - 1), rate=0.0)
    plant = rp.plant_legs(PATH.bears, len(PATH.level), np.zeros(len(PATH.level) - 1), 0, 0.0)
    assert not plant.false.any()
    # Away from the month after each trough the targets in a long bull are 1.
    assert item["bull_exposure"] > 0.95
    assert item["bull_exposure"] > item["exposure"]


def test_one_set_rejects_bad_uniforms() -> None:
    with pytest.raises(ValueError, match="uniforms must have count - 1 entries"):
        rr.one_set(PATH, 3, np.zeros(5))


# ----- summaries and the reading rule -------------------------------------------------------------
def _item(simple: float | None, exposure: float = 0.6, bull: float | None = 0.8) -> dict[str, Any]:
    return {
        "simple": simple,
        "reading": None if simple is None else simple * 0.9,
        "reentry": 0.001,
        "false": 0.002,
        "full": None if simple is None else simple / 2,
        "exposure": exposure,
        "bull_exposure": bull,
        "segment_simple": [],
        "segment_exposure": [],
    }


def test_summarize_lag_by_hand() -> None:
    values = [0.0100, 0.0124, 0.0130, 0.0160, 0.0200, 0.0300]
    items = [_item(v, exposure=0.6 + i / 100) for i, v in enumerate(values)]
    items.append(_item(None, bull=None))
    row = rr.summarize_lag(items)
    assert set(row) == {
        "sets",
        "median",
        "q25",
        "q75",
        "p05",
        "p95",
        "at_most_target",
        "at_most_band",
        "reading",
        "reentry",
        "false",
        "full",
        "exposure",
        "bull_exposure",
        "gap",
    }
    assert row["sets"] == 7
    assert row["median"] == pytest.approx(np.median(values))
    for key, q in (("q25", 0.25), ("q75", 0.75), ("p05", 0.05), ("p95", 0.95)):
        assert row[key] == pytest.approx(np.quantile(values, q))
    # Both lines are inclusive: 1.24 and 1.60 points themselves count.
    assert (row["at_most_target"], row["at_most_band"]) == (2, 4)
    assert row["reading"] == pytest.approx(np.median([v * 0.9 for v in values]))
    assert row["full"] == pytest.approx(np.median([v / 2 for v in values]))
    assert (row["reentry"], row["false"]) == (0.001, 0.002)
    exposures = [0.6 + i / 100 for i in range(6)] + [0.6]
    assert row["exposure"] == pytest.approx(np.mean(exposures))
    assert row["bull_exposure"] == pytest.approx(0.8)
    assert row["gap"] == pytest.approx(np.mean([0.8 - e for e in exposures[:6]]))


def test_summarize_lag_with_nothing_to_count() -> None:
    row = rr.summarize_lag([_item(None, bull=None)])
    assert row["sets"] == 1 and row["median"] is None and row["p95"] is None
    assert (row["at_most_target"], row["at_most_band"]) == (0, 0)
    assert row["gap"] is None and row["bull_exposure"] is None


@pytest.mark.parametrize(
    ("median", "matches", "key"),
    [
        (0.0124, True, "path_quiet"),
        (0.016, True, "path_quiet"),
        (0.016 + 1e-6, True, "both"),
        (0.019 - 1e-6, True, "both"),
        (0.019, True, "plant_noisier"),
        (0.03, True, "plant_noisier"),
        (0.0124, False, "incomplete"),
        (None, True, "incomplete"),
    ],
)
def test_verdict_lines(median: float | None, matches: bool, key: str) -> None:
    item = None if median is None else {"median": median}
    read = rr.verdict(item, matches)
    assert read["key"] == key
    assert read["median"] == median
    if median is None:
        assert read["factor"] is None
    else:
        assert read["factor"] == pytest.approx(0.0124 / median)


def test_verdict_reads_the_lines_at_call_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rr, "QUIET", 0.02)
    assert rr.verdict({"median": 0.017}, True)["key"] == "path_quiet"
    monkeypatch.setattr(rr, "QUIET", 0.016)
    monkeypatch.setattr(rr, "NOISY", 0.017)
    assert rr.verdict({"median": 0.017}, True)["key"] == "plant_noisier"


# ----- the probe ----------------------------------------------------------------------------------
SMALL = rr.probe(US, sets=3, expected=FOUND)


def test_probe_keys_and_parameters() -> None:
    assert set(SMALL) == {
        "run_id",
        "owner_decision",
        "snapshot_id",
        "parameters",
        "path",
        "f0001",
        "lags",
        "no_false",
        "segments",
        "verdict",
    }
    assert (SMALL["run_id"], SMALL["owner_decision"], SMALL["snapshot_id"]) == (
        rr.RUN_ID,
        rr.OWNER_DECISION,
        "sig-7ad8aa00b736",
    )
    assert SMALL["parameters"] == {
        "sets": 3,
        "seed": 20261024,
        "lags": [0, 3, 6, 9, 12],
        "selected": 3,
        "false_rate": 0.35,
        "fast_back": 2,
        "target_sd": 0.0124,
        "band": 0.0036,
        "quiet": 0.016,
        "noisy": 0.019,
    }
    assert SMALL["path"] == {
        "months": 984,
        "days": len(US),
        "slow_bears": len(PATH.bears),
        "long_bulls": [list(span) for span in FOUND],
        "matches": True,
    }
    assert SMALL["f0001"] == {"sd": 0.0124, "exposure": 0.656, "bull_exposure": 0.747}
    json.dumps(SMALL, allow_nan=False)


def test_probe_tables_follow_the_sets() -> None:
    assert list(SMALL["lags"]) == KEYS and list(SMALL["no_false"]) == KEYS
    for lag, key in zip(rr.LAGS, KEYS, strict=True):
        items = [
            rr.one_set(PATH, lag, rr.uniforms_for(rr.SEED, i, len(PATH.level))) for i in range(3)
        ]
        assert SMALL["lags"][key] == rr.summarize_lag(items)
        plain = rr.one_set(PATH, lag, np.ones(len(PATH.level) - 1), rate=0.0)
        assert SMALL["no_false"][key] == {
            "simple": plain["simple"],
            "exposure": plain["exposure"],
            "bull_exposure": plain["bull_exposure"],
            "gap": plain["bull_exposure"] - plain["exposure"],
        }
    assert SMALL["verdict"] == rr.verdict(SMALL["lags"]["3"], True)


def test_probe_segment_rows_at_the_selected_lag() -> None:
    items = [rr.one_set(PATH, 3, rr.uniforms_for(rr.SEED, i, len(PATH.level))) for i in range(3)]
    excess = lp.simple_differences(
        np.log1p(PATH.risky), np.log1p(PATH.cash), PATH.month, PATH.bulls
    )
    whole = np.mean([item["exposure"] for item in items])
    rows = SMALL["segments"]
    assert len(rows) == len(PATH.bulls)
    for i, row in enumerate(rows):
        first, last = PATH.bulls[i]
        exposure = np.mean([item["segment_exposure"][i] for item in items])
        assert row == pytest.approx(
            {
                "segment": i + 1,
                "start": FOUND[i][0],
                "end": FOUND[i][1],
                "months": last - first + 1,
                "excess": excess[i],
                "exposure": exposure,
                "simple": np.mean([item["segment_simple"][i] for item in items]),
                "product": (exposure - whole) * excess[i],
            }
        )


def test_probe_is_deterministic_and_reads_sets_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert rr.probe(US, sets=3, expected=FOUND) == SMALL
    monkeypatch.setattr(rr, "SETS", 2)
    result = rr.probe(US, expected=FOUND)
    assert result["parameters"]["sets"] == 2
    assert all(row["sets"] == 2 for row in result["lags"].values())


def test_probe_reports_progress() -> None:
    seen: list[tuple[str, int, int]] = []
    rr.probe(US, sets=2, lags=(3,), expected=FOUND, progress=lambda *a: seen.append(a))
    assert seen == [("sets", 1, 2), ("sets", 2, 2)]


def test_probe_against_other_segments_is_incomplete() -> None:
    result = rr.probe(US, sets=2, lags=(3,))
    assert result["path"]["matches"] is False
    assert result["verdict"]["key"] == "incomplete"
    assert result["lags"]["3"]["sets"] == 2  # still printed
    assert result["verdict"]["median"] == result["lags"]["3"]["median"]


def test_probe_without_the_selected_lag_is_incomplete() -> None:
    result = rr.probe(US, sets=2, lags=(0, 6), expected=FOUND)
    assert list(result["lags"]) == ["0", "6"]
    assert result["segments"] == []
    assert result["verdict"] == {"key": "incomplete", "median": None, "factor": None}


def test_probe_with_too_few_long_bulls() -> None:
    # A path that only goes down for the first half has fewer than two long bulls.
    falling = US.copy()
    half = len(falling) // 2
    falling.iloc[:half, falling.columns.get_loc(cp.TECH)] = -0.002
    falling.iloc[half:, falling.columns.get_loc(cp.TECH)] = 0.0
    path = rr.real_path(falling)
    assert len(path.bulls) < lp.MIN_SEGMENTS
    result = rr.probe(falling, sets=2, expected=rr.span_months(path.level, path.bulls))
    assert all(row is None for row in result["lags"].values())
    assert all(row is None for row in result["no_false"].values())
    assert result["segments"] == []
    assert result["verdict"]["key"] == "incomplete"
    json.dumps(result, allow_nan=False)


# ----- report, files and command ------------------------------------------------------------------
def test_render_sentences() -> None:
    text = rr.render(SMALL)
    lines = text.splitlines()
    for line in (
        "# 长牛代价：埋的规则放到真实美股大盘路径上（TRADING-2565-68）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-25:longbull_realpath`，第 6 期章程第 16 节。",
        "- 数据：信号快照 `sig-7ad8aa00b736` 里 `ff_factors_daily` 的 Mkt-RF 和 RF，"
        "切到 2010-12-31；只用美股大盘这一条真实路径，不抽合成历史，不用科技行业。",
        "- 只用 TRADING-2565-66 的两条腿埋规则；F0001、fast_slow 和别的已有配置都没有跑。"
        "不进台账、不算试验，k 不动。",
        "- 快腿假退出的随机数抽 3 组（种子 20261024），每组五个 R 共用；q = 0.35。",
        "## 判定（跑之前写死）",
        "| 选中的 R | 简单年化差段间标准差（3 组的中位，点） | 线 | 判定 |",
        "## 每个 R（点；敞口是 3 组的平均）",
        "真实 F0001（TRADING-2565-34 印的，只印）：整条平均敞口 0.656，"
        "长牛平均敞口 0.747，差 0.091。",
        "## 没有假退出（q = 0，只印）",
        "## 选中的 R = 3：逐段（3 组的平均，只印）",
        "一阶乘积 =（段内平均敞口 − 整条平均敞口）× 段的超额年化收益。",
        "production_effect=none；broker_action=none。",
    ):
        assert line in lines, line
    read = SMALL["verdict"]
    assert f"判定：{rr.VERDICT_TEXT[read['key']]}。" in lines
    assert any(
        line.startswith("- 路径：984 个月、")
        and line.endswith("和 TRADING-2565-34 的七个长牛段逐段一致。")
        for line in lines
    )
    assert any(
        line.startswith("| 3 | ")
        and "≤ 1.60 路径偏安静；≥ 1.90 埋的规则太吵；之间两者都有" in line
        and line.endswith(f"| {read['key']} |")
        for line in lines
    )
    assert f"比值 1.24 ÷ 中位 = {read['factor']:.2f}（只印）。" in text
    row = SMALL["lags"]["3"]
    assert any(
        line.startswith(f"| 3 | {row['median'] * 100:.2f} | {row['q25'] * 100:.2f} | ")
        for line in lines
    )
    first = SMALL["segments"][0]
    assert any(
        line.startswith(f"| 1 | {first['start']} | {first['end']} | {first['months']} | ")
        for line in lines
    )
    assert text.endswith("production_effect=none；broker_action=none。\n")


def test_render_verdict_texts() -> None:
    assert rr.VERDICT_TEXT == {
        "path_quiet": "路径偏安静：埋的规则在这条路径上也只有 F0001 那么吵",
        "both": "两者都有：埋的规则比 F0001 吵，这条路径也偏安静",
        "plant_noisier": "埋的规则太吵：同一条路径上它比 F0001 吵得多",
        "incomplete": "没判：长牛段和 TRADING-2565-34 对不上，或者没有可算的组",
    }


def test_render_empty_tables() -> None:
    result = rr.probe(US, sets=2, lags=(0, 6))
    text = rr.render(result)
    assert "和 TRADING-2565-34 的七个长牛段对不上。" in text
    assert "| 3 | – | ≤ 1.60 路径偏安静；≥ 1.90 埋的规则太吵；之间两者都有 | incomplete |" in text
    assert "比值 1.24 ÷ 中位 = –（只印）。" in text


def test_write_result(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b"
    rr.write_result(SMALL, out)
    assert sorted(p.name for p in out.iterdir()) == ["realpath.json", "realpath.md"]
    raw = (out / "realpath.json").read_text(encoding="utf-8")
    assert raw.endswith("}\n")
    assert raw == json.dumps(SMALL, ensure_ascii=False, indent=1, sort_keys=True) + "\n"
    assert (out / "realpath.md").read_text(encoding="utf-8") == rr.render(SMALL)
    broken = {**SMALL, "verdict": {**SMALL["verdict"], "median": math.nan}}
    with pytest.raises(ValueError):
        rr.write_result(broken, tmp_path / "nan")


class _Frames(dict[str, pd.DataFrame]):
    def __getitem__(self, key: str) -> pd.DataFrame:
        if key != cp.FACTORS:
            raise AssertionError(f"read {key}")
        return super().__getitem__(key)


def test_load_us_takes_only_the_factors(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Path, str]] = []

    class Snapshot:
        frames = _Frames({cp.FACTORS: FACTORS, cp.INDUSTRIES: pd.DataFrame()})

    def fake(root: Path, snapshot_id: str) -> Snapshot:
        calls.append((root, snapshot_id))
        return Snapshot()

    monkeypatch.setattr(rr.signal_sources, "load_snapshot", fake)
    pd.testing.assert_frame_equal(rr.load_us(Path("root")), US)
    assert calls == [(Path("root"), "sig-7ad8aa00b736")]


def test_command_writes_its_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rr, "load_us", lambda: US)
    monkeypatch.setattr(rr, "SETS", 2)
    out = tmp_path / "run"
    result = CliRunner().invoke(rr.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out.iterdir()) == ["realpath.json", "realpath.md"]
    summary = json.loads((out / "realpath.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["sets"] == 2
    lines = result.output.splitlines()
    assert f"longbull_realpath: 数据已载入并截到 2010-12-31；us_market {len(US)} 天" in lines
    assert "longbull_realpath: sets 2/2" in lines
    # The synthetic path's long bulls are not the real ones.
    assert "longbull_realpath: 长牛段和 TRADING-2565-34 对不上" in lines
    median = summary["verdict"]["median"]
    assert f"longbull_realpath: R = 3 中位 {median * 100:.2f} 点，判定 incomplete" in lines
    assert lines[-1] == f"longbull_realpath: 写入 {out / 'realpath.json'} 和 {out / 'realpath.md'}"


def test_command_stops_when_the_data_do_not_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def broken() -> pd.DataFrame:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(rr, "load_us", broken)
    out = tmp_path / "out"
    result = CliRunner().invoke(rr.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert (
        "longbull_realpath: 数据载入失败：signal snapshot is unchecked or failed" in result.output
    )
    assert not out.exists()
