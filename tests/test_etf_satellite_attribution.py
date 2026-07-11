from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.data import standardize_price_frame
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.satellite import load_satellite_universe_config
from ai_trading_system.etf_portfolio.satellite_attribution import (
    SatelliteAttributionError,
    build_ai_confirmation_interaction_attribution,
    build_eligibility_bucket_forward_return_analysis,
    build_fallback_to_etf_attribution,
    build_role_group_level_attribution,
    build_satellite_attribution_dataset,
    build_satellite_attribution_evidence_scorecard,
    build_satellite_attribution_report,
    build_satellite_attribution_validation_report,
    build_satellite_risk_attribution,
    build_satellite_score_attribution,
    build_stock_vs_benchmark_attribution,
    load_satellite_replacement_report_payloads,
    render_satellite_attribution_report_markdown,
    satellite_score_bucket,
    validate_satellite_attribution_dataset,
    validate_satellite_attribution_report,
    write_satellite_attribution_dataset,
    write_satellite_attribution_report,
    write_satellite_attribution_validation_report,
)
from ai_trading_system.reports.report_index import load_report_registry


def test_satellite_attribution_dataset_builds_evaluation_records() -> None:
    dataset = _dataset()

    assert dataset["schema_version"] == "satellite_attribution_dataset_v1"
    assert dataset["record_count"] == len(_satellite_reports()) * 2 * 4
    assert dataset["evaluation_only"] is True
    assert dataset["observe_only"] is True
    assert dataset["candidate_only"] is True
    assert dataset["production_effect"] == "none"
    assert dataset["broker_action"] == "none"
    record = next(row for row in dataset["records"] if row["ticker"] == "NVDA")
    assert record["decision_date"] == "2026-03-02"
    assert record["eligibility_date"] == "2026-03-02"
    assert record["replacement_plan_date"] == "2026-03-02"
    assert record["forward_window"] == "1D"
    assert record["sample_available"] is True
    assert record["stock_forward_return"] is not None
    assert record["benchmark_forward_return"] is not None
    assert record["stock_minus_benchmark_forward_return"] is not None
    assert record["replacement_minus_ETF_forward_return"] is not None
    assert record["group"] != "unknown"
    assert record["evaluation_only"] is True
    assert "target_weights" not in record
    validate_satellite_attribution_dataset(dataset)


def test_satellite_attribution_dataset_marks_insufficient_forward_window() -> None:
    report = _satellite_report("2026-04-30", eligible_score=82.0, fallback_score=38.0)
    dataset = build_satellite_attribution_dataset(
        satellite_reports=[report],
        prices=_make_prices(days=90),
        evaluation_as_of_date=date(2026, 5, 4),
        universe_config=load_satellite_universe_config(),
        data_quality_status="PASS",
    )

    row_60d = next(record for record in dataset["records"] if record["forward_window"] == "60D")
    assert row_60d["sample_available"] is False
    assert row_60d["evaluation_only"] is True
    assert (
        "insufficient" in row_60d["insufficient_data_reason"]
        or "after" in row_60d["insufficient_data_reason"]
    )


def test_satellite_score_bucket_assignment() -> None:
    assert satellite_score_bucket(20.0) == "reject"
    assert satellite_score_bucket(35.0) == "weak"
    assert satellite_score_bucket(50.0) == "neutral"
    assert satellite_score_bucket(70.0) == "candidate"
    assert satellite_score_bucket(90.0) == "strong_candidate"
    assert satellite_score_bucket(None) == "unknown"


def test_satellite_attribution_analysis_sections() -> None:
    dataset = _dataset()

    eligibility = build_eligibility_bucket_forward_return_analysis(dataset)
    stock = build_stock_vs_benchmark_attribution(dataset)
    fallback = build_fallback_to_etf_attribution(dataset)
    score = build_satellite_score_attribution(dataset)
    risk = build_satellite_risk_attribution(dataset)
    role = build_role_group_level_attribution(dataset)
    ai = build_ai_confirmation_interaction_attribution(dataset)
    scorecard = build_satellite_attribution_evidence_scorecard(
        dataset_payload=dataset,
        eligibility_bucket_analysis=eligibility,
        stock_vs_benchmark_attribution=stock,
        fallback_attribution=fallback,
        score_attribution=score,
        risk_attribution=risk,
        role_group_attribution=role,
        ai_interaction_attribution=ai,
    )

    assert eligibility["report_type"] == "satellite_eligibility_bucket_analysis"
    assert any(row["eligibility_bucket"] == "eligible" for row in eligibility["buckets"])
    nvda = next(row for row in stock["stocks"] if row["ticker"] == "NVDA")
    amd = next(row for row in stock["stocks"] if row["ticker"] == "AMD")
    assert nvda["mean_stock_minus_benchmark"] > 0
    assert amd["mean_stock_minus_benchmark"] < 0
    assert fallback["fallback_saved_loss_rate"] is not None
    assert any(row["score_bucket"] == "strong_candidate" for row in score["score_buckets"])
    assert risk["risk_adjusted_alpha"] is not None
    assert role["best_role"] != "unknown"
    assert any(row["dimension"] == "AIConfirmationScore" for row in ai["interactions"])
    assert scorecard["overall_status"] in {
        "useful_candidate_overlay_policy",
        "ETF_first_fallback_validated",
        "needs_more_data",
        "too_risky_or_noisy",
        "tighten_constraints_recommended",
        "blocked_by_data_quality",
    }
    assert scorecard["production_effect"] == "none"


def test_satellite_attribution_report_writes_json_and_markdown(tmp_path: Path) -> None:
    report = build_satellite_attribution_report(
        _dataset(),
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    validate_satellite_attribution_report(report)
    paths = write_satellite_attribution_report(report, output_dir=tmp_path / "reports")
    dataset_paths = write_satellite_attribution_dataset(
        report["dataset"],
        output_dir=tmp_path / "datasets",
    )

    assert json.loads(paths["json"].read_text(encoding="utf-8"))["evaluation_only"] is True
    assert "Satellite Replacement Forward Attribution Review" in paths["markdown"].read_text(
        encoding="utf-8"
    )
    assert "production_effect=none" in render_satellite_attribution_report_markdown(report)
    assert dataset_paths["csv"].exists()


def test_satellite_attribution_rejects_unsafe_payload() -> None:
    dataset = _dataset()
    dataset["records"][0]["production_effect"] = "apply_weights"

    with pytest.raises(SatelliteAttributionError, match="UNSAFE_PRODUCTION_EFFECT"):
        validate_satellite_attribution_dataset(dataset)


def test_satellite_attribution_validation_gate_passes_and_fails_registry(tmp_path: Path) -> None:
    passing = build_satellite_attribution_validation_report(
        report_registry=load_report_registry(),
        reader_brief_available=True,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    assert passing["status"] == "PASS"
    assert passing["failed_check_count"] == 0
    assert {
        "dataset_builder_available",
        "eligibility_bucket_analysis_available",
        "stock_vs_benchmark_attribution_available",
        "fallback_attribution_available",
        "score_attribution_available",
        "risk_attribution_available",
        "role_group_attribution_available",
        "AI_interaction_attribution_available",
        "evidence_scorecard_available",
        "report_generator_available",
        "reader_brief_integration_available",
        "report_registry_integration_available",
        "forward_returns_evaluation_only",
    }.issubset({check["check_id"] for check in passing["checks"]})
    paths = write_satellite_attribution_validation_report(
        passing,
        output_dir=tmp_path / "validation",
    )
    assert paths["json"].exists()
    assert "Satellite Attribution Validation Gate" in paths["markdown"].read_text(encoding="utf-8")

    failing = build_satellite_attribution_validation_report(
        report_registry={"reports": []},
        reader_brief_available=True,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    assert failing["status"] == "FAIL"
    assert any(
        check["check_id"] == "report_registry_integration_available" and check["status"] == "FAIL"
        for check in failing["checks"]
    )


def test_load_satellite_reports_filters_by_date(tmp_path: Path) -> None:
    report_dir = tmp_path / "satellite_reports"
    report_dir.mkdir()
    for report in [
        _satellite_report("2026-02-02", eligible_score=82.0, fallback_score=40.0),
        _satellite_report("2026-03-02", eligible_score=85.0, fallback_score=38.0),
        _satellite_report("2026-07-02", eligible_score=88.0, fallback_score=35.0),
    ]:
        path = report_dir / f"satellite_replacement_report_{report['date']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")

    loaded = load_satellite_replacement_report_payloads(
        report_dir,
        as_of=date(2026, 6, 30),
        start=date(2026, 3, 1),
    )

    assert [item["date"] for item in loaded] == ["2026-03-02"]
    assert loaded[0]["source_report_path"].endswith("satellite_replacement_report_2026-03-02.json")


def test_satellite_attribution_cli_build_report_and_validate(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    report_dir = tmp_path / "satellite_reports"
    ai_dir = tmp_path / "ai_reports"
    report_dir.mkdir()
    ai_dir.mkdir()
    _standardized_prices().to_csv(prices_path, index=False)
    for report in _satellite_reports():
        path = report_dir / f"satellite_replacement_report_{report['date']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")
    for report in _ai_reports():
        path = ai_dir / f"ai_confirmation_report_{report['date']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")

    runner = CliRunner()
    build_result = runner.invoke(
        etf_app,
        [
            "satellite-attribution",
            "build",
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-06-30",
            "--satellite-report-dir",
            str(report_dir),
            "--ai-confirmation-report-dir",
            str(ai_dir),
            "--output-dir",
            str(tmp_path / "datasets"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert build_result.exit_code == 0, build_result.output
    assert "evaluation_only=true" in build_result.output
    assert list((tmp_path / "datasets").glob("satellite_attribution_dataset_*.json"))

    report_result = runner.invoke(
        etf_app,
        [
            "satellite-attribution",
            "report",
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-06-30",
            "--satellite-report-dir",
            str(report_dir),
            "--ai-confirmation-report-dir",
            str(ai_dir),
            "--dataset-output-dir",
            str(tmp_path / "report_datasets"),
            "--output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert report_result.exit_code == 0, report_result.output
    assert "Satellite attribution report JSON" in report_result.output
    assert list((tmp_path / "reports").glob("satellite_attribution_report_*.json"))

    validate_result = runner.invoke(
        etf_app,
        [
            "satellite-attribution",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert validate_result.exit_code == 0, validate_result.output
    assert "status=PASS" in validate_result.output
    assert list((tmp_path / "validation").glob("satellite_attribution_validation_*.json"))


def test_satellite_attribution_cli_build_fails_closed_on_invalid_prices(tmp_path: Path) -> None:
    prices_path = tmp_path / "invalid_prices.csv"
    prices_path.write_text("date,symbol,close\n2026-06-30,SPY,not-a-number\n", encoding="utf-8")
    output_dir = tmp_path / "datasets"

    result = CliRunner().invoke(
        etf_app,
        [
            "satellite-attribution",
            "build",
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-06-30",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code != 0
    assert not list(output_dir.glob("satellite_attribution_dataset_*.json"))


def _dataset() -> dict[str, object]:
    return build_satellite_attribution_dataset(
        satellite_reports=_satellite_reports(),
        prices=_make_prices(),
        evaluation_as_of_date=date(2026, 6, 30),
        universe_config=load_satellite_universe_config(),
        ai_confirmation_reports=_ai_reports(),
        data_quality_status="PASS",
    )


def _standardized_prices() -> pd.DataFrame:
    config = load_etf_config_bundle()
    prices, _ = standardize_price_frame(
        _make_prices(),
        assets=config.assets,
        source_name="fixture",
    )
    return prices


def _make_prices(days: int = 150) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2026-01-02", periods=days)
    slopes = {
        "SPY": 0.05,
        "QQQ": 0.10,
        "SMH": 0.12,
        "SOXX": 0.11,
        "NVDA": 0.30,
        "AMD": -0.03,
        "MSFT": 0.16,
    }
    for index, current_date in enumerate(dates):
        for symbol, slope in slopes.items():
            price = 100.0 + index * slope
            if symbol == "NVDA" and index >= 65:
                price += 4.0
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "fixture",
                    "created_at": "2026-06-30T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _satellite_reports() -> list[dict[str, object]]:
    return [
        _satellite_report("2026-03-02", eligible_score=86.0, fallback_score=38.0),
        _satellite_report("2026-03-16", eligible_score=83.0, fallback_score=40.0),
        _satellite_report("2026-03-30", eligible_score=88.0, fallback_score=42.0),
    ]


def _satellite_report(
    decision_date: str,
    *,
    eligible_score: float,
    fallback_score: float,
) -> dict[str, object]:
    payload = {
        "schema_version": "satellite_replacement_report_v1",
        "report_type": "satellite_replacement_report",
        "date": decision_date,
        "market_regime": "growth_leadership",
        "source_report_path": f"satellite_replacement_report_{decision_date}.json",
        "replacement_eligibility": [
            _eligibility_record(
                decision_date,
                ticker="NVDA",
                status="eligible",
                score=eligible_score,
                fallback_to_etf=False,
                blockers=[],
            ),
            _eligibility_record(
                decision_date,
                ticker="AMD",
                status="fallback_to_etf",
                score=fallback_score,
                fallback_to_etf=True,
                blockers=["LOW_RELATIVE_STRENGTH"],
            ),
        ],
        "satellite_candidate_scores": [
            _score_record("NVDA", eligible_score, event_risk=10.0),
            _score_record("AMD", fallback_score, event_risk=25.0),
        ],
        "stock_vs_etf_features": [
            {"ticker": "NVDA", "benchmark_etf": "SMH"},
            {"ticker": "AMD", "benchmark_etf": "SMH"},
        ],
        "replacement_plan": {
            "date": decision_date,
            "replacement_plan_id": f"satellite-replacement-{decision_date}",
            "satellite_allocations": [
                {
                    "ticker": "NVDA",
                    "benchmark_etf": "SMH",
                    "sleeve": "semiconductor",
                    "allocation": 0.03,
                    "reason_codes": ["SATELLITE_REPLACEMENT_ELIGIBLE"],
                }
            ],
            "fallback_positions": [
                {
                    "ticker": "AMD",
                    "benchmark_etf": "SMH",
                    "sleeve": "semiconductor",
                    "reason_codes": ["LOW_RELATIVE_STRENGTH"],
                }
            ],
        },
        "ai_confirmation_context": {"score_value": 76.0, "event_risk_score": 15.0},
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    return deepcopy(payload)


def _eligibility_record(
    decision_date: str,
    *,
    ticker: str,
    status: str,
    score: float,
    fallback_to_etf: bool,
    blockers: list[str],
) -> dict[str, object]:
    return {
        "date": decision_date,
        "ticker": ticker,
        "benchmark_etf": "SMH",
        "sleeve": "semiconductor",
        "role": "ai_accelerator",
        "status": status,
        "score_value": score,
        "score_band": satellite_score_bucket(score),
        "fallback_to_etf": fallback_to_etf,
        "blockers": blockers,
        "reason_codes": blockers or ["SATELLITE_REPLACEMENT_ELIGIBLE"],
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _score_record(ticker: str, score: float, *, event_risk: float) -> dict[str, object]:
    return {
        "ticker": ticker,
        "benchmark_etf": "SMH",
        "score_value": score,
        "score_band": satellite_score_bucket(score),
        "component_scores": {
            "relative_strength_score": score,
            "trend_score": max(0.0, min(100.0, score - 3.0)),
            "drawdown_risk_score": max(0.0, min(100.0, score - 5.0)),
            "event_risk_adjusted_score": 100.0 - event_risk,
            "ai_confirmation_support_score": 76.0,
        },
        "event_risk_score": event_risk,
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _ai_reports() -> list[dict[str, object]]:
    return [
        _ai_report("2026-03-02", 72.0, semi=78.0, mega=70.0, event_risk=10.0),
        _ai_report("2026-03-16", 76.0, semi=82.0, mega=74.0, event_risk=15.0),
        _ai_report("2026-03-30", 82.0, semi=86.0, mega=78.0, event_risk=20.0),
    ]


def _ai_report(
    score_date: str,
    score: float,
    *,
    semi: float,
    mega: float,
    event_risk: float,
) -> dict[str, object]:
    return {
        "schema_version": "ai_confirmation_report_v1",
        "report_type": "ai_confirmation_report",
        "date": score_date,
        "source_report_path": f"ai_confirmation_report_{score_date}.json",
        "AIConfirmationScore": {
            "score_value": score,
            "component_scores": {
                "semiconductor_breadth": semi,
                "mega_cap_ai": mega,
                "ai_relative_strength": semi,
                "event_risk_adjustment": 100.0 - event_risk,
            },
        },
        "event_risk_overlay": {"event_risk_score": event_risk},
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
