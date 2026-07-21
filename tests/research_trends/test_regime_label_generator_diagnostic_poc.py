from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.config import configured_rate_series, load_universe
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    NORMAL_VOLATILITY_LABEL,
    PRIMARY_AXIS,
    REQUIRED_SYMBOLS,
    SAFETY_FIELDS,
    STATUS,
    VOLATILITY_AXIS,
    RegimeLabelGeneratorDiagnosticPocError,
    run_regime_label_generator_diagnostic_poc,
)
from ai_trading_system.regime_state_machine_design_audit import EXPECTED_LABELS
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_regime_label_generator_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "regime-label-generator-diagnostic-poc" in result.output


def test_regime_label_generator_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/regime_label_generator_policy.yaml")
    )

    assert policy["policy_id"] == "regime_label_generator_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC"
    assert policy["market_regime"] == "unified_primary_2021"
    assert policy["known_at_policy"]["pit_policy_status"] == (
        "PIT_APPROX_TRAILING_CLOSE_ONLY"
    )
    assert policy["inputs"]["required_symbols"] == list(REQUIRED_SYMBOLS)
    assert set(policy["label_axes"][PRIMARY_AXIS]["allowed_labels"]) == {
        "uptrend",
        "late_uptrend",
        "drawdown",
        "panic",
        "rebound",
        "failed_rebound",
        "range_bound",
    }
    assert set(policy["label_axes"][VOLATILITY_AXIS]["allowed_labels"]) == {
        "high_volatility",
        "low_volatility",
    }
    assert policy["label_axes"][VOLATILITY_AXIS]["neutral_label"] == (
        NORMAL_VOLATILITY_LABEL
    )
    assert policy["threshold_governance"]["validation_evidence"]
    assert policy["threshold_governance"]["review_condition"]
    assert policy["threshold_governance"]["expiry_condition"]
    assert all("value" in item and item["rationale"] for item in policy["thresholds"].values())

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_regime_label_generator_cli_writes_outputs(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    _write_price_fixture(prices_path)
    _write_rate_fixture(rates_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-label-generator-diagnostic-poc",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--quality-as-of",
            "2023-06-30",
            "--start-date",
            "2022-12-01",
            "--end-date",
            "2023-06-30",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "regime_label_generation_summary.json",
        "regime_label_series.csv",
        "regime_label_pit_policy.json",
        "regime_label_distribution_matrix.json",
        "regime_label_distribution_matrix.csv",
        "regime_label_transition_matrix.json",
        "regime_label_transition_matrix.csv",
        "regime_label_generation_safety_boundary.json",
        "data_quality_2023-06-30.md",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "regime_label_generator_diagnostic_poc.md").exists()

    summary_payload = json.loads(
        (output_dir / "regime_label_generation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["actual_requested_date_range"] == "2022-12-01..2023-06-30"
    assert summary["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["data_quality_gate"]["required_command"] == "aits validate-data"
    assert summary["policy_id"] == "regime_label_generator_policy"
    assert summary["policy_version"] == "v1"
    assert summary["label_row_count"] > 0
    assert summary["label_axis_count"] == 2
    assert set(summary["label_axes"]) == {PRIMARY_AXIS, VOLATILITY_AXIS}
    assert summary["segmentation_ready"] is True
    assert summary["candidate_signal_generated"] is False
    assert summary["candidate_artifact_generated"] is False
    assert summary["actual_path_validation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    labels = pd.read_csv(output_dir / "regime_label_series.csv")
    assert set(labels["ticker"]) == set(REQUIRED_SYMBOLS)
    assert set(labels["label_axis"]) == {PRIMARY_AXIS, VOLATILITY_AXIS}
    assert set(labels.loc[labels["label_axis"] == PRIMARY_AXIS, "regime_label"]).issubset(
        set(EXPECTED_LABELS)
    )
    assert labels["future_outcome_used"].eq(False).all()
    assert labels["hindsight_relabeling_allowed"].eq(False).all()
    assert labels["candidate_signal_generated"].eq(False).all()
    assert labels["promotion_allowed"].eq(False).all()

    pit_policy = json.loads(
        (output_dir / "regime_label_pit_policy.json").read_text(encoding="utf-8")
    )
    assert pit_policy["data_quality_gate"]["required_command"] == "aits validate-data"
    assert "no_future_return_or_drawdown" in pit_policy["required_controls"]
    assert "no_hindsight_episode_relabeling" in pit_policy["required_controls"]
    assert pit_policy["candidate_signal_generated"] is False

    safety = json.loads(
        (output_dir / "regime_label_generation_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_read_cached_market_data"] is True
    assert safety["data_quality_gate_required"] is True
    assert safety["does_generate_regime_label_series"] is True
    assert safety["does_generate_candidate_signal"] is False
    assert safety["does_allow_direct_strategy_signal"] is False
    assert safety["does_allow_broker_action"] is False


def test_regime_label_generator_fails_closed_on_data_quality_error(
    tmp_path: Path,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_price_fixture(prices_path)
    output_dir = tmp_path / "out"

    with pytest.raises(RegimeLabelGeneratorDiagnosticPocError, match="data quality"):
        run_regime_label_generator_diagnostic_poc(
            prices_path=prices_path,
            rates_path=tmp_path / "missing_rates.csv",
            marketstack_prices_path=None,
            quality_as_of="2023-06-30",
            start_date="2022-12-01",
            end_date="2023-06-30",
            output_dir=output_dir,
            docs_root=tmp_path / "docs",
        )

    assert (output_dir / "data_quality_2023-06-30.md").exists()
    assert not (output_dir / "regime_label_series.csv").exists()


def test_regime_label_generator_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "regime_label_generator_diagnostic_poc"
    )

    assert entry["command"] == "aits research trends regime-label-generator-diagnostic-poc"
    assert entry["artifact_role"] == "regime_label_series_diagnostic_poc"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["validation_status"] == STATUS
    assert entry["diagnostic_only"] is True
    assert entry["segmentation_only"] is True
    assert entry["generator_implemented"] is True
    assert entry["regime_label_series_generated"] is True
    assert entry["candidate_signal_generated"] is False
    assert entry["candidate_artifact_generated"] is False
    assert entry["actual_path_validation_executed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"
    assert set(entry["label_ids"]) == set(EXPECTED_LABELS)
    assert entry["overlay_neutral_label"] == NORMAL_VOLATILITY_LABEL

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "regime_label_generator_diagnostic_poc" in catalog
    assert STATUS in catalog
    assert "不是 candidate signal" in catalog
    assert NORMAL_VOLATILITY_LABEL in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2316" in system_flow
    assert "regime-label-generator-diagnostic-poc" in system_flow
    assert "validate_data_cache" in system_flow
    assert "candidate_signal_generated=false" in system_flow


def test_regime_label_generator_rejects_wrong_mode(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    _write_price_fixture(prices_path)
    _write_rate_fixture(rates_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-label-generator-diagnostic-poc",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
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


def _write_price_fixture(path: Path) -> None:
    dates = pd.bdate_range("2021-07-01", "2023-06-30")
    rows: list[dict[str, object]] = []
    multipliers = {"QQQ": 1.0, "SMH": 1.15, "SPY": 0.9}
    for ticker, multiplier in multipliers.items():
        price = 100.0 * multiplier
        for index, ts in enumerate(dates):
            if index < 130:
                daily_return = 0.001
            elif index < 210:
                daily_return = -0.002
            elif index < 270:
                daily_return = 0.003
            elif index < 350:
                daily_return = -0.001
            else:
                daily_return = 0.0015
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
