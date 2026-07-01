from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.liquidity_rates_generator_poc import (
    BLOCKED_CANDIDATES,
    DEFAULT_CANDIDATES,
    STATUS,
    run_liquidity_rates_pressure_generator_poc,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "TLT", "SHY")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")


def test_liquidity_rates_generator_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "liquidity-rates-pressure-generator-poc" in result.output


def test_liquidity_rates_generator_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/liquidity_rates_pressure_generator_policy.yaml")
    )

    assert policy["policy_id"] == "liquidity_rates_pressure_generator_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert set(policy["candidate_policy"]) == set(DEFAULT_CANDIDATES)
    assert set(policy["source_gap_policy"]["blocked_candidates"]) == set(BLOCKED_CANDIDATES)
    assert policy["safety"]["partial_rates_only_generator_poc"] is True
    assert policy["safety"]["full_liquidity_pressure_poc_ready"] is False
    assert policy["safety"]["liquidity_headwind_generator_implemented"] is False
    assert policy["safety"]["promotion_allowed"] is False
    assert policy["safety"]["paper_shadow_allowed"] is False
    assert policy["safety"]["production_allowed"] is False
    assert policy["safety"]["broker_action"] == "none"

    for threshold in policy["signal_policy"].values():
        assert "value" in threshold
        assert threshold["rationale"]


def test_liquidity_rates_generator_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-generator-poc",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--feasibility-dir",
            str(fixture["feasibility_dir"]),
            "--start-date",
            "2026-05-15",
            "--end-date",
            "2026-06-29",
            "--quality-as-of",
            "2026-06-29",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    summary = json.loads(
        (output_dir / "liquidity_rates_pressure_generator_poc_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["status"] == STATUS
    assert summary["data_quality_status"] == "PASS"
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["actual_requested_date_range"] == "2026-05-15..2026-06-29"
    assert summary["summary"]["candidate_count"] == 2
    assert summary["summary"]["blocked_candidate_count"] == 1
    assert summary["summary"]["validation_status"] == "PASS"
    assert summary["partial_rates_only_generator_poc"] is True
    assert summary["full_liquidity_pressure_poc_ready"] is False
    assert summary["liquidity_headwind_generator_implemented"] is False
    assert summary["actual_path_validation_ready"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    for candidate_id in DEFAULT_CANDIDATES:
        candidate_dir = output_dir / candidate_id
        assert (candidate_dir / "candidate_signal_spec.json").exists()
        assert (candidate_dir / "candidate_signal_series.csv").exists()
        assert (candidate_dir / "candidate_prediction_artifact.json").exists()
        assert (candidate_dir / "generation_summary.json").exists()
        validation = json.loads(
            (candidate_dir / "validation_summary.json").read_text(encoding="utf-8")
        )
        assert validation["status"] == "PASS"
        series = pd.read_csv(candidate_dir / "candidate_signal_series.csv")
        assert not series.empty
        assert set(series["candidate_id"]) == {candidate_id}
        assert set(series["market_regime"]) == {"ai_after_chatgpt"}

    assert not (output_dir / "liquidity_headwind_proxy_v1").exists()
    blocked = json.loads(
        (output_dir / "blocked_liquidity_rates_candidate_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert blocked["status"] == "LIQUIDITY_HEADWIND_SOURCE_GAP_BLOCKED"
    assert blocked["rows"][0]["candidate_signal_series_generated"] is False
    assert "UUP_or_DXY_price_proxy" in blocked["rows"][0]["missing_inputs"]

    safety = json.loads(
        (output_dir / "liquidity_rates_generator_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_generate_liquidity_headwind_candidate"] is True
    assert safety["does_not_run_actual_path_validation"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"
    assert (docs_root / "liquidity_rates_pressure_generator_poc.md").exists()


def test_liquidity_rates_generator_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path)

    payload = run_liquidity_rates_pressure_generator_poc(
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        feasibility_dir=fixture["feasibility_dir"],
        start_date="2026-05-15",
        end_date="2026-06-29",
        quality_as_of="2026-06-29",
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    candidate_paths = payload["artifact_paths"]["candidates"]
    assert all(
        isinstance(path, str)
        for paths in candidate_paths.values()
        for path in paths.values()
    )


def test_liquidity_rates_generator_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-generator-poc",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_liquidity_rates_generator_rejects_blocked_liquidity_headwind_candidate(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path)
    output_dir = tmp_path / "out"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-generator-poc",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--feasibility-dir",
            str(fixture["feasibility_dir"]),
            "--candidates",
            "liquidity_headwind_proxy_v1",
            "--quality-as-of",
            "2026-06-29",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
    assert "blocked by TRADING-2311 source gap" in str(result.exception)
    assert not (output_dir / "liquidity_rates_pressure_generator_poc_summary.json").exists()


def test_liquidity_rates_generator_fails_closed_missing_required_anchor(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path, symbols=REQUIRED_SYMBOLS[:-1])
    output_dir = tmp_path / "out"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-generator-poc",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--feasibility-dir",
            str(fixture["feasibility_dir"]),
            "--quality-as-of",
            "2026-06-29",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
    assert (output_dir / "data_quality_2026-06-29.md").exists()
    assert not (output_dir / "liquidity_rates_pressure_generator_poc_summary.json").exists()


def test_liquidity_rates_generator_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "liquidity_rates_pressure_generator_poc"
    )

    assert entry["command"] == "aits research trends liquidity-rates-pressure-generator-poc"
    assert entry["artifact_role"] == "liquidity_rates_pressure_generator_poc"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["generator_implemented"] is True
    assert entry["partial_rates_only_generator_poc"] is True
    assert entry["full_liquidity_pressure_poc_ready"] is False
    assert entry["liquidity_headwind_generator_implemented"] is False
    assert entry["candidate_artifact_generated"] is True
    assert entry["candidate_signal_series_generated"] is True
    assert entry["actual_path_validation_executed"] is False
    assert entry["actual_path_validation_ready"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "liquidity_rates_pressure_generator_poc" in catalog
    assert STATUS in catalog
    assert "liquidity_headwind_generator_implemented=false" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2312" in system_flow
    assert "liquidity-rates-pressure-generator-poc" in system_flow
    assert "validate_data_cache" in system_flow
    assert "liquidity_headwind_proxy_v1" in system_flow


def _write_generator_fixture(
    tmp_path: Path,
    *,
    symbols: tuple[str, ...] = REQUIRED_SYMBOLS,
) -> dict[str, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    feasibility_dir = tmp_path / "feasibility"
    feasibility_dir.mkdir(parents=True, exist_ok=True)
    _write_feasibility_fixture(feasibility_dir)

    dates = pd.bdate_range("2026-04-01", "2026-06-29")
    price_rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(symbols):
        base = 90.0 + symbol_index * 12.0
        drift = 0.0007 + symbol_index * 0.0001
        for day_index, value_date in enumerate(dates):
            close = round(base * ((1.0 + drift) ** day_index), 4)
            price_rows.append(
                {
                    "date": value_date.date().isoformat(),
                    "ticker": symbol,
                    "open": round(close * 0.999, 4),
                    "high": round(close * 1.002, 4),
                    "low": round(close * 0.998, 4),
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000 + symbol_index * 1000 + day_index,
                }
            )
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)

    rate_rows: list[dict[str, object]] = []
    for series_index, series in enumerate(RATE_SERIES):
        base = 4.0 + series_index * 0.25
        if series == "DTWEXBGS":
            base = 120.0
        for day_index, value_date in enumerate(dates):
            rate_rows.append(
                {
                    "date": value_date.date().isoformat(),
                    "series": series,
                    "value": round(base + day_index * 0.001, 4),
                }
            )
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)

    return {
        "prices": prices_path,
        "rates": rates_path,
        "feasibility_dir": feasibility_dir,
    }


def _write_feasibility_fixture(feasibility_dir: Path) -> None:
    summary = {
        "summary": {
            "status": "LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY",
            "data_quality_status": "PASS",
            "actual_requested_date_range": "2026-05-15..2026-06-29",
            "partial_poc_possible": True,
            "full_liquidity_pressure_poc_ready": False,
            "promotion_allowed": False,
            "missing_price_proxy_symbols": ["IEF", "UUP", "HYG", "LQD"],
            "missing_macro_series": ["DFII10", "T10YIE", "SOFR"],
        }
    }
    design = {
        "blocked_full_scope": [
            "liquidity_headwind_proxy_v1_requires_UUP_or_DXY_and_HYG_LQD",
            "real_rate_proxy_requires_DFII10_or_equivalent_real_rate_source",
        ],
        "recommended_partial_scope": [
            "duration_pressure_proxy_v1_using_TLT_SHY_DGS10_DGS2",
            "rates_pressure_exposure_cap_modifier_v1_using_TLT_DGS10_DGS2",
        ],
    }
    (feasibility_dir / "liquidity_rates_data_feasibility_summary.json").write_text(
        json.dumps(summary),
        encoding="utf-8",
    )
    (feasibility_dir / "liquidity_pressure_candidate_design_sketch.json").write_text(
        json.dumps(design),
        encoding="utf-8",
    )
