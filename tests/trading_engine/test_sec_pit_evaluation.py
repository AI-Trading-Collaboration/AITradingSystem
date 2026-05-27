from __future__ import annotations

import builtins
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_evaluation import (
    FEATURE_EFFECTIVENESS_COLUMNS,
    SHADOW_CANDIDATE_WEIGHT_COLUMNS,
    SIGNAL_ATTRIBUTION_COLUMNS,
    _forward_max_drawdown,
    run_sec_pit_evaluation,
)


def test_sec_pit_evaluation_excludes_future_available_time_rows(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, future_available_time=True)

    artifacts = _run(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    attribution = pd.read_csv(artifacts.signal_attribution_path)
    assert summary["data_coverage"]["pit_violation_count"] == 1
    assert summary["data_coverage"]["valid_rows"] == summary["data_coverage"]["input_rows"] - 1
    assert not (
        (attribution["decision_date"] == "2023-01-03")
        & (attribution["ticker"] == "NVDA")
        & (attribution["feature_id"] == "gross_margin")
    ).any()


def test_sec_pit_evaluation_excludes_missing_available_time_rows(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, missing_available_time=True)

    artifacts = _run(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    assert summary["data_coverage"]["missing_available_time"] == 1
    assert summary["data_coverage"]["valid_rows"] == summary["data_coverage"]["input_rows"] - 1


def test_sec_pit_evaluation_downgrades_missing_accession_number(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, missing_accession_feature=True)

    artifacts = _run(paths, tmp_path)

    effectiveness = pd.read_csv(artifacts.feature_effectiveness_path)
    weak = effectiveness.loc[effectiveness["feature_id"] == "weak_provenance"].iloc[0]
    assert weak["data_quality_score"] < 0.8
    assert weak["recommendation"] != "PROMOTE_TO_SHADOW"


def test_sec_pit_feature_effectiveness_schema_is_stable(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = _run(paths, tmp_path)

    effectiveness = pd.read_csv(artifacts.feature_effectiveness_path)
    assert tuple(effectiveness.columns) == FEATURE_EFFECTIVENESS_COLUMNS
    promoted = effectiveness.loc[effectiveness["feature_id"] == "gross_margin"].iloc[0]
    assert promoted["recommendation"] == "PROMOTE_TO_SHADOW"
    assert promoted["rank_ic_20d"] > 0


def test_sec_pit_signal_attribution_schema_is_stable(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = _run(paths, tmp_path)

    attribution = pd.read_csv(artifacts.signal_attribution_path)
    assert tuple(attribution.columns) == SIGNAL_ATTRIBUTION_COLUMNS
    assert set(attribution["signal_direction"]) == {"POSITIVE"}
    assert attribution["contribution"].notna().any()


def test_sec_pit_forward_max_drawdown_is_never_positive() -> None:
    rising = _forward_max_drawdown(pd.Series([100.0, 101.0, 102.0, 103.0]).to_numpy(), 3)
    falling = _forward_max_drawdown(pd.Series([100.0, 105.0, 95.0, 110.0]).to_numpy(), 3)

    assert rising[0] == 0.0
    assert round(float(falling[0]), 6) == -0.05


def test_sec_pit_shadow_candidate_weights_are_observe_only(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = _run(paths, tmp_path)

    weights = pd.read_csv(artifacts.shadow_candidate_weights_path)
    assert tuple(weights.columns) == SHADOW_CANDIDATE_WEIGHT_COLUMNS
    assert weights["manual_review_required"].all()
    assert set(weights["production_effect"]) == {"none"}
    assert (weights["current_weight"] == 0.0).all()


def test_sec_pit_markdown_summary_is_generated(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = _run(paths, tmp_path)

    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "# SEC PIT Cognitive Evaluation Summary" in markdown
    assert "## PIT Safety Checks" in markdown
    assert "## Manual Review Checklist" in markdown
    assert "B_RECONSTRUCTED_SEC_FILING_PIT" in markdown


def test_sec_pit_evaluate_cli_writes_requested_artifacts(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "evaluate",
            "--start",
            "2023-01-02",
            "--end",
            "2023-01-20",
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
            "2023-04-07",
            "--policy-path",
            str(paths["policy"]),
            "--market-regimes-path",
            str(paths["market_regimes"]),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "outputs" / "sec_pit_evaluation_summary_2023-01-20.json").exists()
    assert (tmp_path / "outputs" / "sec_pit_evaluation_summary_2023-01-20.md").exists()
    assert (tmp_path / "outputs" / "sec_pit_feature_effectiveness_2023-01-20.csv").exists()
    assert (tmp_path / "outputs" / "sec_pit_signal_attribution_2023-01-20.csv").exists()
    assert (tmp_path / "outputs" / "sec_pit_shadow_candidate_weights_2023-01-20.csv").exists()
    assert "Feature effectiveness:" in result.output


def test_sec_pit_evaluation_insufficient_sample_is_not_promoted(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, decision_days=2)

    artifacts = _run(paths, tmp_path, end=date(2023, 1, 4))

    effectiveness = pd.read_csv(artifacts.feature_effectiveness_path)
    assert set(effectiveness["recommendation"]).issubset(
        {"EXCLUDE_INSUFFICIENT_DATA", "KEEP_RESEARCH_ONLY"}
    )


def test_sec_pit_evaluation_repeated_runs_keep_csv_outputs_deterministic(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    first = _run(paths, tmp_path / "first")
    second = _run(paths, tmp_path / "second")

    assert first.feature_effectiveness_path.read_text(encoding="utf-8") == (
        second.feature_effectiveness_path.read_text(encoding="utf-8")
    )
    assert first.shadow_candidate_weights_path.read_text(encoding="utf-8") == (
        second.shadow_candidate_weights_path.read_text(encoding="utf-8")
    )


def test_daily_task_dashboard_sec_pit_evaluation_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.daily_task_dashboard import (
        build_daily_task_dashboard_payload,
        build_daily_task_dashboard_report,
        render_daily_task_dashboard,
    )

    as_of = date(2023, 1, 20)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_sec_pit_evaluation_summary_artifact(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "run_sec_pit_evaluation",
            "ai_trading_system.fundamentals.sec_pit_evaluation",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import SEC PIT evaluator: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_evaluation_summary"]
    assert summary["exists"] is True
    assert summary["latest_evaluation_date"] == "2023-01-20"
    assert summary["universe_size"] == 5
    assert summary["feature_count"] == 1
    assert summary["promote_to_shadow_count"] == 1
    assert summary["research_only_count"] == 0
    assert summary["excluded_count"] == 0
    assert summary["pit_safety_status"] == "PASS"
    assert "SEC PIT Evaluation Summary" in html
    assert "gross_margin" in html


def _run(
    paths: dict[str, Path],
    tmp_path: Path,
    *,
    end: date = date(2023, 1, 20),
) -> Any:
    return run_sec_pit_evaluation(
        start=date(2023, 1, 2),
        end=end,
        feature_panel_path=paths["feature_panel"],
        universe_path=paths["sec_companies"],
        benchmark="QQQ",
        output_dir=tmp_path / "outputs",
        prices_path=paths["prices"],
        rates_path=paths["rates"],
        market_universe_path=paths["market_universe"],
        data_quality_config_path=paths["data_quality"],
        quality_as_of=date(2023, 4, 7),
        policy_path=paths["policy"],
        market_regimes_path=paths["market_regimes"],
    )


def _write_inputs(
    tmp_path: Path,
    *,
    decision_days: int = 15,
    future_available_time: bool = False,
    missing_available_time: bool = False,
    missing_accession_feature: bool = False,
) -> dict[str, Path]:
    paths = {
        "sec_companies": tmp_path / "sec_companies.yaml",
        "market_universe": tmp_path / "universe.yaml",
        "data_quality": tmp_path / "data_quality.yaml",
        "market_regimes": tmp_path / "market_regimes.yaml",
        "policy": tmp_path / "sec_pit_evaluation.yaml",
        "prices": tmp_path / "prices_daily.csv",
        "rates": tmp_path / "rates_daily.csv",
        "feature_panel": tmp_path / "sec_pit_feature_panel.csv",
    }
    _write_sec_companies(paths["sec_companies"])
    _write_market_universe(paths["market_universe"])
    _write_data_quality(paths["data_quality"])
    _write_market_regimes(paths["market_regimes"])
    _write_policy(paths["policy"])
    dates = [date(2023, 1, 2) + timedelta(days=offset) for offset in range(96)]
    _write_prices(paths["prices"], dates)
    _write_rates(paths["rates"], dates)
    _write_feature_panel(
        paths["feature_panel"],
        dates[:decision_days],
        future_available_time=future_available_time,
        missing_available_time=missing_available_time,
        missing_accession_feature=missing_accession_feature,
    )
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
  - ticker: AVGO
    cik: "0001730168"
    company_name: Broadcom Inc.
  - ticker: GOOGL
    cik: "0001652044"
    company_name: Alphabet Inc.
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
  core_watchlist: [NVDA, MSFT, AMD, AVGO, GOOGL]
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
  max_stale_calendar_days: 120
  suspicious_daily_return_abs: 0.20
  extreme_daily_return_abs: 0.50
  suspicious_adjustment_ratio_change_abs: 0.25
  consistency_start_date: 2023-01-01
rates:
  max_stale_calendar_days: 120
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
  min_coverage_ratio: 0.6
  min_valid_ticker_count: 5
  min_sample_count: 30
  min_abs_rank_ic_20d: 0.03
  min_stability_score: 0.5
  min_pit_quality_score: 0.8
  winsorize_lower_quantile: 0.01
  winsorize_upper_quantile: 0.99
  top_quantile: 0.20
  max_abs_shadow_weight: 0.05
  pit_quality_weights:
    pit_grade: 0.25
    accession_number: 0.25
    accepted_datetime: 0.15
    filed_date: 0.15
    raw_sha256: 0.20
""".lstrip(),
        encoding="utf-8",
    )


def _write_prices(path: Path, dates: list[date]) -> None:
    daily_returns = {
        "NVDA": 0.0040,
        "MSFT": 0.0030,
        "AMD": 0.0020,
        "AVGO": 0.0010,
        "GOOGL": 0.0005,
        "QQQ": 0.0010,
    }
    rows: list[dict[str, object]] = []
    for ticker, daily_return in daily_returns.items():
        price = 100.0
        for index, item in enumerate(dates):
            if index:
                price *= 1.0 + daily_return
            rows.append(
                {
                    "date": item.isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price * 1.001,
                    "low": price * 0.999,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rates(path: Path, dates: list[date]) -> None:
    rows = [{"date": item.isoformat(), "series": "DGS10", "value": 4.0} for item in dates]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_feature_panel(
    path: Path,
    decision_dates: list[date],
    *,
    future_available_time: bool,
    missing_available_time: bool,
    missing_accession_feature: bool,
) -> None:
    values = {
        "NVDA": 0.80,
        "MSFT": 0.65,
        "AMD": 0.50,
        "AVGO": 0.35,
        "GOOGL": 0.20,
    }
    cik_by_ticker = {
        "NVDA": "0001045810",
        "MSFT": "0000789019",
        "AMD": "0000002488",
        "AVGO": "0001730168",
        "GOOGL": "0001652044",
    }
    records: list[dict[str, object]] = []
    feature_ids = ["gross_margin"]
    if missing_accession_feature:
        feature_ids.append("weak_provenance")
    for decision_date in decision_dates:
        for ticker, feature_value in values.items():
            for feature_id in feature_ids:
                available_time = f"{decision_date.isoformat()}T00:00:00+00:00"
                accession_number = f"{cik_by_ticker[ticker]}-23-000001"
                if future_available_time and decision_date == date(2023, 1, 3) and ticker == "NVDA":
                    available_time = "2023-02-01T00:00:00+00:00"
                if (
                    missing_available_time
                    and decision_date == date(2023, 1, 3)
                    and ticker == "MSFT"
                ):
                    available_time = ""
                if feature_id == "weak_provenance":
                    accession_number = ""
                records.append(
                    {
                        "decision_date": decision_date.isoformat(),
                        "ticker": ticker,
                        "cik": cik_by_ticker[ticker],
                        "feature_id": feature_id,
                        "metric_id": "gross_profit,revenue",
                        "period": "2022Q4",
                        "available_time": available_time,
                        "feature_value": feature_value,
                        "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                        "accession_number": accession_number,
                        "form": "10-Q",
                        "source_concept": "us-gaap:GrossProfit,us-gaap:Revenue",
                        "raw_sha256": "a" * 64,
                        "accepted_datetime": "2023-01-01T00:00:00+00:00",
                        "filed_date": "2023-01-01",
                    }
                )
    pd.DataFrame(records).to_csv(path, index=False)


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_metadata_{as_of.isoformat()}.json"
    payload = {
        "run_id": "unit-test",
        "status": "PASS",
        "project_root": str(tmp_path),
        "started_at": "2023-01-20T00:00:00Z",
        "finished_at": "2023-01-20T00:01:00Z",
        "commands": [],
        "step_results": [],
        "git": {"commit": "abc123", "dirty": False},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_sec_pit_evaluation_summary_artifact(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_evaluation"
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / f"sec_pit_evaluation_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_evaluation_summary_{as_of.isoformat()}.md"
    payload = {
        "report_type": "sec_pit_cognitive_evaluation",
        "status": "PASS",
        "generated_at": "2023-01-20T00:00:00Z",
        "end_date": as_of.isoformat(),
        "universe_size": 5,
        "feature_count": 1,
        "data_coverage": {
            "pit_violation_count": 0,
            "missing_available_time": 0,
            "excluded_rows": 0,
        },
        "recommendations": {
            "promote_to_shadow": 1,
            "keep_research_only": 0,
            "downweight": 0,
            "exclude_insufficient_data": 0,
        },
        "top_features": [
            {
                "feature_id": "gross_margin",
                "metric_id": "gross_profit,revenue",
                "rank_ic_20d": 1.0,
                "recommendation": "PROMOTE_TO_SHADOW",
            }
        ],
        "output_artifacts": {
            "summary_markdown": str(markdown_path),
        },
        "production_effect": "none",
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text("# SEC PIT Cognitive Evaluation Summary\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
