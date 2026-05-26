from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_evaluation import run_sec_pit_evaluation
from ai_trading_system.fundamentals.sec_pit_panel import SEC_PIT_FEATURE_PANEL_COLUMNS


def test_sec_pit_evaluation_classifies_shadow_candidate(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = run_sec_pit_evaluation(
        start=date(2023, 1, 2),
        end=date(2023, 1, 4),
        feature_panel_path=paths["feature_panel"],
        universe_path=paths["sec_companies"],
        benchmark="QQQ",
        output_dir=tmp_path / "outputs",
        prices_path=paths["prices"],
        rates_path=paths["rates"],
        market_universe_path=paths["market_universe"],
        data_quality_config_path=paths["data_quality"],
        quality_as_of=date(2023, 1, 5),
        policy_path=paths["policy"],
        market_regimes_path=paths["market_regimes"],
    )

    assert artifacts.status == "PASS"
    assert artifacts.feature_contributions_path is not None
    contributions = pd.read_csv(artifacts.feature_contributions_path)
    gross_margin = contributions.loc[contributions["feature_id"] == "gross_margin"].iloc[0]
    assert gross_margin["classification"] == "SHADOW_CANDIDATE"
    assert gross_margin["mean_daily_rank_ic"] == 1.0
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["summary"]["strategy_judgment_status"] == "POSITIVE_SHADOW_EVIDENCE"
    assert payload["data_quality"]["status"] == "PASS"


def test_sec_pit_evaluation_blocks_future_available_time(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, future_available_time=True)

    artifacts = run_sec_pit_evaluation(
        start=date(2023, 1, 2),
        end=date(2023, 1, 4),
        feature_panel_path=paths["feature_panel"],
        universe_path=paths["sec_companies"],
        benchmark="QQQ",
        output_dir=tmp_path / "outputs",
        prices_path=paths["prices"],
        rates_path=paths["rates"],
        market_universe_path=paths["market_universe"],
        data_quality_config_path=paths["data_quality"],
        quality_as_of=date(2023, 1, 5),
        policy_path=paths["policy"],
        market_regimes_path=paths["market_regimes"],
    )

    assert artifacts.status == "SAFETY_BLOCKED"
    assert artifacts.feature_contributions_path is None
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["pit_safety"]["future_available_time_violation_count"] == 1
    assert "future availability leakage" in payload["blocking_reason"]


def test_sec_pit_evaluation_data_quality_gate_stops_on_failure(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, omit_price_ticker="AMD")

    artifacts = run_sec_pit_evaluation(
        start=date(2023, 1, 2),
        end=date(2023, 1, 4),
        feature_panel_path=paths["feature_panel"],
        universe_path=paths["sec_companies"],
        benchmark="QQQ",
        output_dir=tmp_path / "outputs",
        prices_path=paths["prices"],
        rates_path=paths["rates"],
        market_universe_path=paths["market_universe"],
        data_quality_config_path=paths["data_quality"],
        quality_as_of=date(2023, 1, 5),
        policy_path=paths["policy"],
        market_regimes_path=paths["market_regimes"],
    )

    assert artifacts.status == "DATA_QUALITY_FAILED"
    assert artifacts.feature_contributions_path is None
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["data_quality"]["status"] == "FAIL"


def test_sec_pit_evaluate_cli_writes_artifacts(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "evaluate",
            "--start",
            "2023-01-02",
            "--end",
            "2023-01-04",
            "--feature-panel",
            str(paths["feature_panel"]),
            "--universe",
            str(paths["sec_companies"]),
            "--benchmark",
            "QQQ",
            "--output-dir",
            str(tmp_path / "outputs"),
            "--prices-path",
            str(paths["prices"]),
            "--rates-path",
            str(paths["rates"]),
            "--market-universe-path",
            str(paths["market_universe"]),
            "--data-quality-config-path",
            str(paths["data_quality"]),
            "--quality-as-of",
            "2023-01-05",
            "--policy-path",
            str(paths["policy"]),
            "--market-regimes-path",
            str(paths["market_regimes"]),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "outputs" / "sec_pit_evaluation_2023-01-02_2023-01-04.json").exists()
    assert (tmp_path / "outputs" / "sec_pit_evaluation_2023-01-02_2023-01-04.md").exists()
    assert (
        tmp_path / "outputs" / "sec_pit_feature_contributions_2023-01-02_2023-01-04.csv"
    ).exists()
    assert (tmp_path / "outputs" / "sec_pit_shadow_candidates_2023-01-02_2023-01-04.csv").exists()
    assert "SEC PIT evaluation status: PASS" in result.output


def _write_inputs(
    tmp_path: Path,
    *,
    future_available_time: bool = False,
    omit_price_ticker: str | None = None,
) -> dict[str, Path]:
    paths = {
        "sec_companies": tmp_path / "sec_companies.yaml",
        "market_universe": tmp_path / "universe.yaml",
        "data_quality": tmp_path / "data_quality.yaml",
        "market_regimes": tmp_path / "market_regimes.yaml",
        "policy": tmp_path / "sec_pit_evaluation_policy.yaml",
        "prices": tmp_path / "prices_daily.csv",
        "rates": tmp_path / "rates_daily.csv",
        "feature_panel": tmp_path / "sec_pit_feature_panel.csv",
    }
    _write_sec_companies(paths["sec_companies"])
    _write_market_universe(paths["market_universe"])
    _write_data_quality(paths["data_quality"])
    _write_market_regimes(paths["market_regimes"])
    _write_policy(paths["policy"])
    _write_prices(paths["prices"], omit_ticker=omit_price_ticker)
    _write_rates(paths["rates"])
    _write_feature_panel(paths["feature_panel"], future_available_time=future_available_time)
    return paths


def _write_sec_companies(path: Path) -> None:
    path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
  - ticker: MSFT
    cik: "0000789019"
    company_name: Microsoft Corporation
  - ticker: AMD
    cik: "0000002488"
    company_name: Advanced Micro Devices, Inc.
""".lstrip(),
        encoding="utf-8",
    )


def _write_market_universe(path: Path) -> None:
    path.write_text(
        """
market:
  decision_frequency: daily
  benchmarks: [QQQ]
  defensive: []
macro:
  volatility: []
  rates: [DGS10]
  currency: []
ai_chain:
  core_watchlist: [NVDA, MSFT, AMD]
scoring_weights:
  trend: 25
  fundamentals: 25
""".lstrip(),
        encoding="utf-8",
    )


def _write_data_quality(path: Path) -> None:
    path.write_text(
        """
prices:
  max_stale_calendar_days: 10
  suspicious_daily_return_abs: 0.20
  extreme_daily_return_abs: 0.50
  suspicious_adjustment_ratio_change_abs: 0.25
  consistency_start_date: 2023-01-01
rates:
  max_stale_calendar_days: 10
  min_plausible_value: -1.0
  max_plausible_value: 25.0
  suspicious_daily_change_abs: 0.75
  extreme_daily_change_abs: 2.0
  consistency_start_date: 2023-01-01
""".lstrip(),
        encoding="utf-8",
    )


def _write_market_regimes(path: Path) -> None:
    path.write_text(
        """
default_backtest_regime: ai_after_chatgpt
regimes:
  - regime_id: ai_after_chatgpt
    name: ChatGPT 后 AI 主线行情
    start_date: 2022-12-01
    anchor_date: 2022-11-30
    anchor_event: ChatGPT 公开发布
    description: Unit test regime.
    primary: true
""".lstrip(),
        encoding="utf-8",
    )


def _write_policy(path: Path) -> None:
    path.write_text(
        """
sec_pit_evaluation:
  policy_version: sec_pit_evaluation.test
  owner: test
  status: pilot_baseline
  rationale: Unit test policy.
  review_condition: Unit test review.
  forward_return_trading_days: 1
  min_cross_sectional_tickers: 3
  research_only_min_observations: 3
  shadow_candidate_min_observations: 6
  shadow_candidate_min_months: 1
  shadow_candidate_min_ticker_coverage: 0.8
  shadow_candidate_min_decision_day_coverage: 0.8
  shadow_candidate_min_abs_mean_rank_ic: 0.5
  shadow_candidate_min_rank_ic_sign_stability: 1.0
  shadow_candidate_min_directional_spread: 0.0
  top_bottom_quantile: 0.34
  min_shadow_candidate_count_for_positive_loop: 1
""".lstrip(),
        encoding="utf-8",
    )


def _write_prices(path: Path, *, omit_ticker: str | None = None) -> None:
    dates = pd.to_datetime(["2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"])
    returns = {
        "NVDA": 0.02,
        "MSFT": 0.01,
        "AMD": -0.01,
        "QQQ": 0.00,
    }
    rows: list[dict[str, object]] = []
    for ticker, daily_return in returns.items():
        if omit_ticker == ticker:
            continue
        price = 100.0
        for day_index, item in enumerate(dates):
            if day_index:
                price *= 1.0 + daily_return
            rows.append(
                {
                    "date": item.date().isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rates(path: Path) -> None:
    rows = [
        {"date": item, "series": "DGS10", "value": 4.0}
        for item in ("2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05")
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_feature_panel(path: Path, *, future_available_time: bool) -> None:
    values = {"NVDA": 0.70, "MSFT": 0.55, "AMD": 0.40}
    records = []
    for decision_date in ("2023-01-02", "2023-01-03", "2023-01-04"):
        for ticker, feature_value in values.items():
            available_time = f"{decision_date}T00:00:00+00:00"
            if future_available_time and decision_date == "2023-01-02" and ticker == "NVDA":
                available_time = "2023-01-03T00:00:00+00:00"
            records.append(
                {
                    "decision_date": decision_date,
                    "ticker": ticker,
                    "feature_id": "gross_margin",
                    "feature_value": feature_value,
                    "feature_unit": "ratio",
                    "input_metric_ids": "gross_profit,revenue",
                    "input_accession_numbers": "a,b",
                    "input_available_times_utc": f"{available_time},{available_time}",
                    "max_input_available_time_utc": available_time,
                    "pit_data_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                    "confidence_level": "high",
                    "confidence_reason": "unit test",
                    "period_type": "quarterly",
                    "period_end": "2022-12-31",
                    "input_metric_units": "USD,USD",
                }
            )
    pd.DataFrame(records, columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS)).to_csv(path, index=False)
