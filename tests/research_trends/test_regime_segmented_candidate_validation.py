from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.config import configured_rate_series, load_universe
from ai_trading_system.regime_segmented_candidate_validation import (
    PRIMARY_AXIS,
    SAFETY_FIELDS,
    STATUS,
    VOLATILITY_AXIS,
    RegimeSegmentedCandidateValidationError,
    run_regime_segmented_candidate_validation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_regime_segmented_candidate_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "regime-segmented-candidate-validation" in result.output


def test_regime_segmented_candidate_validation_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/regime_segmented_candidate_validation_policy.yaml")
    )

    assert policy["policy_id"] == "regime_segmented_candidate_validation_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == "TRADING-2317_REGIME_SEGMENTED_CANDIDATE_VALIDATION"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["data_quality"]["required_command"] == "aits validate-data"
    assert policy["label_source"]["required_status"] == (
        "REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY"
    )
    assert set(policy["candidate_sources"]) == {
        "volatility_risk_cap",
        "breadth_proxy",
        "ai_leadership",
        "liquidity_pressure",
    }
    assert policy["threshold_governance"]["validation_evidence"]
    assert policy["threshold_governance"]["review_condition"]
    assert policy["threshold_governance"]["expiry_condition"]
    assert all(
        "value" in item and item["rationale"]
        for item in policy["thresholds"].values()
    )

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_regime_segmented_candidate_validation_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = _write_fixture_pack(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-segmented-candidate-validation",
            "--policy",
            str(fixture["policy_path"]),
            "--label-series",
            str(fixture["label_series_path"]),
            "--label-summary",
            str(fixture["label_summary_path"]),
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--quality-as-of",
            "2023-06-30",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "regime_segmented_candidate_validation_summary.json",
        "regime_segmented_candidate_performance_matrix.json",
        "regime_segmented_candidate_performance_matrix.csv",
        "regime_segmented_candidate_coverage_matrix.json",
        "regime_segmented_candidate_coverage_matrix.csv",
        "regime_segmented_family_blocker_matrix.json",
        "regime_segmented_family_blocker_matrix.csv",
        "regime_segmented_interpretation_matrix.json",
        "regime_segmented_interpretation_matrix.csv",
        "regime_segmented_candidate_validation_safety_boundary.json",
        "data_quality_2023-06-30.md",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "regime_segmented_candidate_validation.md").exists()

    summary_payload = json.loads(
        (output_dir / "regime_segmented_candidate_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["actual_requested_date_range"] == "2022-12-01..2023-06-30"
    assert summary["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["data_quality_gate"]["required_command"] == "aits validate-data"
    assert summary["label_source_status"] == (
        "REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY"
    )
    assert summary["performance_row_count"] > 0
    assert summary["coverage_row_count"] == 5
    assert summary["family_blocker_row_count"] == 4
    assert summary["segmentable_families"] == [
        "volatility_risk_cap",
        "ai_leadership",
        "liquidity_pressure",
    ]
    assert summary["blocked_families"] == ["breadth_proxy"]
    assert summary["breadth_proxy_status"] == "SEGMENTATION_SOURCE_BLOCKED"
    assert summary["candidate_signal_generated"] is False
    assert summary["candidate_artifact_generated"] is False
    assert summary["new_actual_path_validation_executed"] is False
    assert summary["existing_candidate_verdict_changed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    performance = pd.read_csv(
        output_dir / "regime_segmented_candidate_performance_matrix.csv"
    )
    assert set(performance["family_name"]) == {
        "volatility_risk_cap",
        "ai_leadership",
        "liquidity_pressure",
    }
    assert set(performance["label_axis"]) == {PRIMARY_AXIS, VOLATILITY_AXIS}
    assert performance["candidate_signal_generated"].eq(False).all()
    assert performance["promotion_allowed"].eq(False).all()

    blockers = pd.read_csv(output_dir / "regime_segmented_family_blocker_matrix.csv")
    breadth = blockers.loc[blockers["family_name"] == "breadth_proxy"].iloc[0]
    assert not bool(breadth["segmentable"])
    assert breadth["blocker_status"] == "SEGMENTATION_SOURCE_BLOCKED"

    safety = json.loads(
        (output_dir / "regime_segmented_candidate_validation_safety_boundary.json")
        .read_text(encoding="utf-8")
    )
    assert safety["does_read_cached_market_data"] is True
    assert safety["data_quality_gate_required"] is True
    assert safety["does_consume_regime_label_series"] is True
    assert safety["does_generate_new_candidate_signal"] is False
    assert safety["does_change_existing_candidate_verdict"] is False
    assert safety["does_allow_broker_action"] is False


def test_regime_segmented_candidate_validation_fails_closed_on_data_quality_error(
    tmp_path: Path,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_price_fixture(prices_path)
    output_dir = tmp_path / "out"

    with pytest.raises(RegimeSegmentedCandidateValidationError, match="data quality"):
        run_regime_segmented_candidate_validation(
            prices_path=prices_path,
            rates_path=tmp_path / "missing_rates.csv",
            marketstack_prices_path=None,
            quality_as_of="2023-06-30",
            output_dir=output_dir,
            docs_root=tmp_path / "docs",
        )

    assert (output_dir / "data_quality_2023-06-30.md").exists()
    assert not (output_dir / "regime_segmented_candidate_performance_matrix.csv").exists()


def test_regime_segmented_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "regime_segmented_candidate_validation"
    )

    assert entry["command"] == "aits research trends regime-segmented-candidate-validation"
    assert entry["artifact_role"] == "regime_segmented_candidate_validation_diagnostic"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["validation_status"] == STATUS
    assert entry["diagnostic_only"] is True
    assert entry["segmentation_only"] is True
    assert entry["actual_path_validation_consumed"] is True
    assert entry["new_actual_path_validation_executed"] is False
    assert entry["candidate_signal_generated"] is False
    assert entry["candidate_artifact_generated"] is False
    assert entry["existing_candidate_verdict_changed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"
    assert entry["blocked_families"] == ["breadth_proxy"]
    assert set(entry["segmentable_families"]) == {
        "volatility_risk_cap",
        "ai_leadership",
        "liquidity_pressure",
    }

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "regime_segmented_candidate_validation" in catalog
    assert STATUS in catalog
    assert "breadth proxy 因 source blocked" in catalog
    assert "不是新 actual-path validation" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2317" in system_flow
    assert "regime-segmented-candidate-validation" in system_flow
    assert "validate_data_cache" in system_flow
    assert "new_actual_path_validation_executed=false" in system_flow


def test_regime_segmented_candidate_validation_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    fixture = _write_fixture_pack(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-segmented-candidate-validation",
            "--policy",
            str(fixture["policy_path"]),
            "--label-series",
            str(fixture["label_series_path"]),
            "--label-summary",
            str(fixture["label_summary_path"]),
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--quality-as-of",
            "2023-06-30",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def _write_fixture_pack(tmp_path: Path) -> dict[str, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    _write_price_fixture(prices_path)
    _write_rate_fixture(rates_path)

    dates = [ts.date().isoformat() for ts in pd.bdate_range("2023-01-03", periods=45)]
    label_series_path = tmp_path / "regime_label_series.csv"
    label_summary_path = tmp_path / "regime_label_generation_summary.json"
    _write_label_fixture(label_series_path, label_summary_path, dates)

    source_root = tmp_path / "sources"
    risk_matrix = source_root / "risk" / "matrix.csv"
    risk_summary = source_root / "risk" / "summary.json"
    ai_matrix = source_root / "ai" / "matrix.csv"
    ai_summary = source_root / "ai" / "summary.json"
    liquidity_matrix = source_root / "liquidity" / "matrix.csv"
    liquidity_summary = source_root / "liquidity" / "summary.json"
    breadth_scorecard = source_root / "breadth" / "scorecard.csv"
    breadth_summary = source_root / "breadth" / "summary.json"

    _write_risk_actual_path_fixture(risk_matrix, risk_summary, dates)
    _write_ai_actual_path_fixture(ai_matrix, ai_summary, dates)
    _write_liquidity_actual_path_fixture(
        liquidity_matrix,
        liquidity_summary,
        dates,
    )
    _write_breadth_blocked_fixture(breadth_scorecard, breadth_summary)

    policy = safe_load_yaml_path(
        Path("config/research/regime_segmented_candidate_validation_policy.yaml")
    )
    policy["candidate_sources"]["volatility_risk_cap"]["source_matrix"] = str(
        risk_matrix
    )
    policy["candidate_sources"]["volatility_risk_cap"]["source_summary"] = str(
        risk_summary
    )
    policy["candidate_sources"]["ai_leadership"]["source_matrix"] = str(ai_matrix)
    policy["candidate_sources"]["ai_leadership"]["source_summary"] = str(ai_summary)
    policy["candidate_sources"]["liquidity_pressure"]["source_matrix"] = str(
        liquidity_matrix
    )
    policy["candidate_sources"]["liquidity_pressure"]["source_summary"] = str(
        liquidity_summary
    )
    policy["candidate_sources"]["breadth_proxy"]["source_scorecard"] = str(
        breadth_scorecard
    )
    policy["candidate_sources"]["breadth_proxy"]["source_summary"] = str(
        breadth_summary
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    return {
        "policy_path": policy_path,
        "label_series_path": label_series_path,
        "label_summary_path": label_summary_path,
        "prices_path": prices_path,
        "rates_path": rates_path,
    }


def _write_label_fixture(
    label_series_path: Path,
    label_summary_path: Path,
    dates: list[str],
) -> None:
    rows: list[dict[str, object]] = []
    for ticker in ("SPY", "QQQ", "SMH"):
        for index, day in enumerate(dates):
            primary = "uptrend" if index < 35 else "drawdown"
            volatility = "normal_volatility" if index < 30 else "high_volatility"
            rows.extend(
                [
                    {
                        "date": day,
                        "ticker": ticker,
                        "label_axis": PRIMARY_AXIS,
                        "regime_label": primary,
                    },
                    {
                        "date": day,
                        "ticker": ticker,
                        "label_axis": VOLATILITY_AXIS,
                        "regime_label": volatility,
                    },
                ]
            )
    label_series_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(label_series_path, index=False)
    label_summary_path.write_text(
        json.dumps(
            {
                "status": "REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY",
                "data_quality_status": "PASS_WITH_WARNINGS",
                "actual_requested_date_range": "2022-12-01..2023-06-30",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_risk_actual_path_fixture(
    matrix_path: Path,
    summary_path: Path,
    dates: list[str],
) -> None:
    rows = [
        {
            "candidate_id": "volatility_regime_scope_narrowed_risk_cap_v1",
            "target_asset": "SPY",
            "horizon": "5d",
            "decision_timestamp": day,
            "validation_eligible": True,
            "forward_return": 0.01 - index * 0.0001,
            "max_drawdown_during_horizon": -0.02 - index * 0.0001,
            "realized_volatility": 0.15 + index * 0.001,
            "stress_event": index % 11 == 0,
        }
        for index, day in enumerate(dates)
    ]
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(matrix_path, index=False)
    summary_path.write_text(
        json.dumps(
            {"status": "SCOPE_NARROWED_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED"},
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_ai_actual_path_fixture(
    matrix_path: Path,
    summary_path: Path,
    dates: list[str],
) -> None:
    rows: list[dict[str, object]] = []
    for target_asset in ("QQQ", "SMH"):
        for index, day in enumerate(dates):
            rows.append(
                {
                    "candidate_id": "ai_semiconductor_leadership_quality_v1",
                    "candidate_family": "ai_leadership",
                    "target_asset": target_asset,
                    "horizon": "10d",
                    "source_date": day,
                    "signal_name": "ai_leadership_quality",
                    "signal_direction": "positive",
                    "signal_value": 0.5 + index * 0.001,
                    "signal_confidence": 0.6,
                    "validation_eligible": True,
                    "target_forward_return": 0.012 + index * 0.0001,
                    "target_max_drawdown": -0.015,
                    "smh_relative_forward_return": 0.002,
                }
            )
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(matrix_path, index=False)
    summary_path.write_text(
        json.dumps({"status": "AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH"}, indent=2),
        encoding="utf-8",
    )


def _write_liquidity_actual_path_fixture(
    matrix_path: Path,
    summary_path: Path,
    dates: list[str],
) -> None:
    rows: list[dict[str, object]] = []
    for target_asset in ("QQQ", "SMH"):
        for index, day in enumerate(dates):
            rows.append(
                {
                    "candidate_id": "duration_pressure_proxy_v1",
                    "candidate_family": "liquidity_pressure",
                    "target_asset": target_asset,
                    "horizon": "20d",
                    "source_date": day,
                    "signal_name": "duration_pressure",
                    "signal_direction": "negative",
                    "signal_value": -0.25,
                    "signal_confidence": 0.55,
                    "validation_eligible": True,
                    "target_forward_return": 0.006 - index * 0.00005,
                    "target_max_drawdown": -0.018,
                    "qqq_smh_average_forward_return": 0.005,
                    "qqq_smh_worst_drawdown": -0.02,
                }
            )
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(matrix_path, index=False)
    summary_path.write_text(
        json.dumps({"status": "LIQUIDITY_RATES_VALIDATED_CONTINUE_RESEARCH"}, indent=2),
        encoding="utf-8",
    )


def _write_breadth_blocked_fixture(
    scorecard_path: Path,
    summary_path: Path,
) -> None:
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "concept_id": f"breadth_proxy_{index}",
                "selection_status": "SOURCE_BLOCKED",
            }
            for index in range(7)
        ]
    ).to_csv(scorecard_path, index=False)
    summary_path.write_text(
        json.dumps(
            {
                "status": "BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION",
                "summary": {
                    "source_status": "CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED",
                    "selected_concept_count": 0,
                    "rejected_concept_count": 7,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_price_fixture(path: Path) -> None:
    dates = pd.bdate_range("2021-07-01", "2023-06-30")
    rows: list[dict[str, object]] = []
    multipliers = {"QQQ": 1.0, "SMH": 1.15, "SPY": 0.9, "TLT": 0.75, "SHY": 0.5}
    for ticker, multiplier in multipliers.items():
        price = 100.0 * multiplier
        for index, ts in enumerate(dates):
            daily_return = 0.001 if index % 9 else -0.0015
            price = round(price * (1.0 + daily_return), 4)
            rows.append(
                {
                    "date": ts.date().isoformat(),
                    "ticker": ticker,
                    "open": round(price * 0.999, 4),
                    "high": round(price * 1.003, 4),
                    "low": round(price * 0.997, 4),
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000 + index,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rate_fixture(path: Path) -> None:
    dates = pd.bdate_range("2021-07-01", "2023-06-30")
    rows: list[dict[str, object]] = []
    for series in configured_rate_series(load_universe()):
        for index, ts in enumerate(dates):
            rows.append(
                {
                    "date": ts.date().isoformat(),
                    "series": series,
                    "value": round(2.0 + index * 0.0001, 4),
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
