from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.ai_attribution import (
    AIAttributionError,
    build_ai_attribution_dataset,
    build_ai_attribution_evidence_scorecard,
    build_ai_attribution_report,
    build_ai_attribution_validation_report,
    build_ai_score_bucket_forward_return_analysis,
    build_component_level_attribution,
    build_event_risk_attribution,
    build_redundancy_diagnostics,
    build_regime_conditional_attribution,
    event_risk_bucket,
    load_ai_confirmation_report_payloads,
    render_ai_attribution_report_markdown,
    score_bucket,
    validate_ai_attribution_dataset,
    validate_ai_attribution_report,
    write_ai_attribution_dataset,
    write_ai_attribution_report,
    write_ai_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.data import standardize_price_frame
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.reports.report_index import load_report_registry


def test_ai_attribution_dataset_builds_forward_evaluation_records() -> None:
    prices = _make_prices()
    reports = _ai_reports()

    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=date(2026, 6, 30),
        data_quality_status="PASS",
    )

    assert dataset["schema_version"] == "ai_attribution_dataset_v1"
    assert dataset["record_count"] == len(reports) * 4
    assert dataset["evaluation_only"] is True
    assert dataset["observe_only"] is True
    assert dataset["candidate_only"] is True
    assert dataset["production_effect"] == "none"
    assert dataset["broker_action"] == "none"
    record = dataset["records"][0]
    assert record["score_date"] == "2026-03-02"
    assert record["evaluation_as_of_date"] == "2026-06-30"
    assert record["forward_window"] == "1D"
    assert record["evaluation_only"] is True
    assert record["sample_available"] is True
    assert record["QQQ_forward_return"] is not None
    assert record["SMH_minus_QQQ_forward_return"] is not None
    assert record["QQQ_minus_SPY_forward_return"] is not None
    assert record["max_drawdown_forward"] <= 0
    assert "target_weights" not in record
    validate_ai_attribution_dataset(dataset)


def test_ai_attribution_dataset_marks_insufficient_forward_window() -> None:
    prices = _make_prices(days=90)
    report = _ai_report("2026-04-30", 86.0, event_risk=20.0)

    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=[report],
        prices=prices,
        evaluation_as_of_date=date(2026, 5, 4),
        data_quality_status="PASS",
    )

    row_60d = next(record for record in dataset["records"] if record["forward_window"] == "60D")
    assert row_60d["sample_available"] is False
    assert row_60d["evaluation_only"] is True
    assert "insufficient" in row_60d["insufficient_data_reason"] or "after" in row_60d[
        "insufficient_data_reason"
    ]


def test_score_and_event_risk_bucket_assignment() -> None:
    assert score_bucket(20.0) == "negative"
    assert score_bucket(35.0) == "weak"
    assert score_bucket(50.0) == "neutral"
    assert score_bucket(70.0) == "confirm"
    assert score_bucket(90.0) == "strong_confirm"
    assert event_risk_bucket(20.0) == "low"
    assert event_risk_bucket(45.0) == "medium"
    assert event_risk_bucket(70.0) == "high"
    assert event_risk_bucket(90.0) == "critical"


def test_bucket_component_regime_event_and_redundancy_analysis() -> None:
    dataset = _dataset()

    bucket = build_ai_score_bucket_forward_return_analysis(dataset)
    component = build_component_level_attribution(dataset)
    regime = build_regime_conditional_attribution(dataset)
    event = build_event_risk_attribution(dataset)
    redundancy = build_redundancy_diagnostics(dataset)
    scorecard = build_ai_attribution_evidence_scorecard(
        dataset_payload=dataset,
        bucket_analysis=bucket,
        component_attribution=component,
        regime_attribution=regime,
        event_risk_attribution=event,
        redundancy_diagnostics=redundancy,
    )

    assert bucket["report_type"] == "ai_attribution_score_bucket_analysis"
    assert any(row["score_bucket"] == "strong_confirm" for row in bucket["buckets"])
    semi = next(
        item for item in component["components"] if item["component"] == "SemiconductorBreadthScore"
    )
    smh_target = next(target for target in semi["targets"] if target["target"] == "SMH_minus_QQQ")
    assert smh_target["sample_count"] > 0
    assert smh_target["rank_correlation_with_forward_return"] is not None
    assert any(row["regime"] == "growth_leadership" for row in regime["regime_bucket_metrics"])
    assert {row["event_risk_bucket"] for row in event["event_risk_bucket_metrics"]} >= {
        "low",
        "high",
    }
    assert redundancy["redundancy_band"] in {"low", "medium", "high", "unknown_insufficient_data"}
    assert scorecard["overall_status"] in {
        "useful_candidate_overlay_factor",
        "reporting_only",
        "needs_more_data",
        "noisy_or_redundant",
        "blocked_by_data_quality",
    }
    assert scorecard["manual_review_recommendation"]
    assert scorecard["production_effect"] == "none"


def test_ai_attribution_report_writes_json_and_markdown(tmp_path: Path) -> None:
    report = build_ai_attribution_report(
        _dataset(),
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    validate_ai_attribution_report(report)
    paths = write_ai_attribution_report(report, output_dir=tmp_path / "reports")
    dataset_paths = write_ai_attribution_dataset(
        report["dataset"],
        output_dir=tmp_path / "datasets",
    )

    assert json.loads(paths["json"].read_text(encoding="utf-8"))["evaluation_only"] is True
    assert "AI Confirmation Forward Attribution Review" in paths["markdown"].read_text(
        encoding="utf-8"
    )
    assert "production_effect=none" in render_ai_attribution_report_markdown(report)
    assert dataset_paths["csv"].exists()


def test_ai_attribution_rejects_unsafe_payload() -> None:
    dataset = _dataset()
    dataset["records"][0]["production_effect"] = "apply_weights"

    with pytest.raises(AIAttributionError, match="UNSAFE_PRODUCTION_EFFECT"):
        validate_ai_attribution_dataset(dataset)


def test_ai_attribution_validation_gate_passes_and_fails_registry(tmp_path: Path) -> None:
    passing = build_ai_attribution_validation_report(
        report_registry=load_report_registry(),
        reader_brief_available=True,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    assert passing["status"] == "PASS"
    assert passing["failed_check_count"] == 0
    assert {
        "dataset_builder_available",
        "score_bucket_analysis_available",
        "component_attribution_available",
        "regime_attribution_available",
        "event_risk_attribution_available",
        "redundancy_diagnostics_available",
        "evidence_scorecard_available",
        "report_generator_available",
        "reader_brief_integration_available",
        "report_registry_integration_available",
        "forward_returns_evaluation_only",
    }.issubset({check["check_id"] for check in passing["checks"]})
    paths = write_ai_attribution_validation_report(passing, output_dir=tmp_path / "validation")
    assert paths["json"].exists()
    assert "AI Attribution Validation Gate" in paths["markdown"].read_text(encoding="utf-8")

    failing = build_ai_attribution_validation_report(
        report_registry={"reports": []},
        reader_brief_available=True,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    assert failing["status"] == "FAIL"
    assert any(
        check["check_id"] == "report_registry_integration_available"
        and check["status"] == "FAIL"
        for check in failing["checks"]
    )


def test_load_ai_confirmation_reports_filters_by_date(tmp_path: Path) -> None:
    report_dir = tmp_path / "ai_confirmation"
    report_dir.mkdir()
    for report in [
        _ai_report("2026-02-02", 45.0, event_risk=10.0),
        _ai_report("2026-03-02", 65.0, event_risk=10.0),
        _ai_report("2026-07-02", 85.0, event_risk=10.0),
    ]:
        path = report_dir / f"ai_confirmation_report_{report['date']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")

    loaded = load_ai_confirmation_report_payloads(
        report_dir,
        as_of=date(2026, 6, 30),
        start=date(2026, 3, 1),
    )

    assert [item["date"] for item in loaded] == ["2026-03-02"]
    assert loaded[0]["source_report_path"].endswith("ai_confirmation_report_2026-03-02.json")


def test_ai_attribution_cli_build_report_and_validate(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    report_dir = tmp_path / "ai_confirmation_reports"
    report_dir.mkdir()
    _standardized_prices().to_csv(prices_path, index=False)
    for report in _ai_reports():
        path = report_dir / f"ai_confirmation_report_{report['date']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")

    runner = CliRunner()
    build_result = runner.invoke(
        etf_app,
        [
            "ai-attribution",
            "build",
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-06-30",
            "--ai-confirmation-report-dir",
            str(report_dir),
            "--output-dir",
            str(tmp_path / "datasets"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert build_result.exit_code == 0, build_result.output
    assert "evaluation_only=true" in build_result.output
    assert list((tmp_path / "datasets").glob("ai_attribution_dataset_*.json"))

    report_result = runner.invoke(
        etf_app,
        [
            "ai-attribution",
            "report",
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-06-30",
            "--ai-confirmation-report-dir",
            str(report_dir),
            "--dataset-output-dir",
            str(tmp_path / "report_datasets"),
            "--output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert report_result.exit_code == 0, report_result.output
    assert "AI attribution report JSON" in report_result.output
    assert list((tmp_path / "reports").glob("ai_attribution_report_*.json"))

    validate_result = runner.invoke(
        etf_app,
        [
            "ai-attribution",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert validate_result.exit_code == 0, validate_result.output
    assert "status=PASS" in validate_result.output
    assert list((tmp_path / "validation").glob("ai_attribution_validation_*.json"))


def _dataset() -> dict[str, object]:
    return build_ai_attribution_dataset(
        ai_confirmation_reports=_ai_reports(),
        prices=_make_prices(),
        evaluation_as_of_date=date(2026, 6, 30),
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
    slopes = {"SPY": 0.05, "QQQ": 0.10, "SMH": 0.18, "SOXX": 0.17}
    for index, current_date in enumerate(dates):
        for symbol, slope in slopes.items():
            bonus = 3.0 if symbol in {"SMH", "SOXX"} and index >= 65 else 0.0
            price = 100.0 + index * slope + bonus
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


def _ai_reports() -> list[dict[str, object]]:
    return [
        _ai_report("2026-03-02", 35.0, event_risk=10.0, regime="neutral"),
        _ai_report("2026-03-16", 42.0, event_risk=12.0, regime="neutral"),
        _ai_report("2026-03-30", 55.0, event_risk=15.0, regime="neutral"),
        _ai_report("2026-04-13", 67.0, event_risk=20.0, regime="growth_leadership"),
        _ai_report("2026-04-27", 74.0, event_risk=25.0, regime="growth_leadership"),
        _ai_report("2026-05-11", 84.0, event_risk=70.0, regime="growth_leadership"),
        _ai_report("2026-05-25", 91.0, event_risk=82.0, regime="growth_leadership"),
    ]


def _ai_report(
    score_date: str,
    score: float,
    *,
    event_risk: float,
    regime: str = "growth_leadership",
) -> dict[str, object]:
    payload = {
        "schema_version": "ai_confirmation_report_v1",
        "report_type": "ai_confirmation_report",
        "date": score_date,
        "market_regime": regime,
        "source_report_path": f"ai_confirmation_report_{score_date}.json",
        "AIConfirmationScore": {
            "score_name": "AIConfirmationScore",
            "score_value": score,
            "score_band": score_bucket(score),
            "action_hint": "supports_neutral_ai_exposure",
            "component_scores": {
                "semiconductor_breadth": score,
                "mega_cap_ai": min(100.0, score + 4.0),
                "ai_relative_strength": min(100.0, score + 2.0),
                "event_risk_adjustment": 100.0 - event_risk,
                "data_coverage": 95.0,
            },
            "data_coverage_ratio": 0.95,
            "observe_only": True,
            "candidate_only": True,
            "production_effect": "none",
            "broker_action": "none",
            "manual_review_required": True,
        },
        "component_scores": {
            "semiconductor_breadth": score,
            "mega_cap_ai": min(100.0, score + 4.0),
            "ai_relative_strength": min(100.0, score + 2.0),
            "event_risk_adjustment": 100.0 - event_risk,
            "data_coverage": 95.0,
        },
        "event_risk_overlay": {
            "event_risk_score": event_risk,
            "risk_band": event_risk_bucket(event_risk),
            "observe_only": True,
            "candidate_only": True,
            "production_effect": "none",
            "broker_action": "none",
            "manual_review_required": True,
        },
        "data_coverage": {
            "composite_data_coverage_ratio": 0.95,
        },
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    return deepcopy(payload)
