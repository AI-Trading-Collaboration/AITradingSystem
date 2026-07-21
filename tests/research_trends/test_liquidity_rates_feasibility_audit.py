from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.liquidity_rates_feasibility_audit import (
    STATUS,
    build_macro_rates_coverage_matrix,
    build_price_proxy_coverage_matrix,
    run_liquidity_rates_pressure_feasibility_audit,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "TLT", "SHY")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")


def test_liquidity_rates_feasibility_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "liquidity-rates-pressure-feasibility-audit" in result.output


def test_liquidity_rates_proxy_inventory_records_source_gaps(
    tmp_path: Path,
) -> None:
    fixture = _write_liquidity_rates_fixture(tmp_path)
    price_rows = build_price_proxy_coverage_matrix(
        pd.read_csv(fixture["prices"]),
        fixture["prices"],
    )
    macro_rows = build_macro_rates_coverage_matrix(
        pd.read_csv(fixture["rates"]),
        fixture["rates"],
    )

    by_symbol = {row["symbol"]: row for row in price_rows}
    by_series = {row["series"]: row for row in macro_rows}

    assert by_symbol["TLT"]["source_status"] == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    assert by_symbol["SHY"]["source_status"] == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    assert by_symbol["IEF"]["source_status"] == "SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE"
    assert by_symbol["UUP"]["source_status"] == "SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE"
    assert by_symbol["HYG"]["source_status"] == "SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE"
    assert by_symbol["LQD"]["source_status"] == "SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE"
    assert by_series["DGS10"]["source_status"] == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    assert by_series["DGS2"]["source_status"] == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    assert by_series["DFII10"]["source_status"] == "SOURCE_GAP_MISSING_LOCAL_RATE_CACHE"
    assert all(row["promotion_allowed"] is False for row in price_rows + macro_rows)
    assert all(row["paper_shadow_allowed"] is False for row in price_rows + macro_rows)
    assert all(row["production_allowed"] is False for row in price_rows + macro_rows)
    assert all(row["broker_action"] == "none" for row in price_rows + macro_rows)


def test_liquidity_rates_feasibility_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = _write_liquidity_rates_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-feasibility-audit",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--quality-as-of",
            "2026-06-29",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "liquidity_rates_data_feasibility_summary.json",
        "rates_proxy_inventory.json",
        "rates_proxy_inventory.csv",
        "liquidity_rates_price_proxy_coverage_matrix.json",
        "liquidity_rates_price_proxy_coverage_matrix.csv",
        "macro_rates_coverage_matrix.json",
        "macro_rates_coverage_matrix.csv",
        "liquidity_pressure_candidate_design_sketch.json",
        "liquidity_rates_validation_route.json",
        "liquidity_rates_safety_boundary.json",
        "data_quality_2026-06-29.md",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "liquidity_rates_data_feasibility_audit.md").exists()

    summary_payload = json.loads(
        (output_dir / "liquidity_rates_data_feasibility_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["actual_requested_date_range"] == "2022-12-01..2026-06-29"
    assert summary["data_quality_status"] == "PASS"
    assert summary["partial_poc_possible"] is True
    assert summary["full_liquidity_pressure_poc_ready"] is False
    assert set(summary["missing_price_proxy_symbols"]) == {"IEF", "UUP", "HYG", "LQD"}
    assert set(summary["missing_macro_series"]) == {"DFII10", "T10YIE", "SOFR"}
    assert summary["generator_implemented"] is False
    assert summary["candidate_artifact_generated"] is False
    assert summary["actual_path_validation_executed"] is False
    assert summary["scope_review_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["dynamic_promotion_status"] == "BLOCKED"

    inventory_rows = json.loads(
        (output_dir / "rates_proxy_inventory.json").read_text(encoding="utf-8")
    )["rows"]
    by_input = {row["input_id"]: row for row in inventory_rows}
    assert by_input["duration_pressure_proxy"]["missing_dependencies"] == []
    assert by_input["credit_liquidity_proxy"]["missing_dependencies"] == ["HYG", "LQD"]
    assert by_input["real_rate_proxy"]["missing_dependencies"] == ["DFII10", "T10YIE"]
    assert all(row["generator_ready"] is False for row in inventory_rows)

    safety = json.loads(
        (output_dir / "liquidity_rates_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_generate_candidate_artifacts"] is True
    assert safety["does_not_run_actual_path_validation"] is True
    assert safety["does_not_run_scope_review"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"


def test_liquidity_rates_feasibility_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    fixture = _write_liquidity_rates_fixture(tmp_path)

    payload = run_liquidity_rates_pressure_feasibility_audit(
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        quality_as_of="2026-06-29",
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_liquidity_rates_feasibility_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-feasibility-audit",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_liquidity_rates_feasibility_fails_closed_missing_required_anchor(
    tmp_path: Path,
) -> None:
    fixture = _write_liquidity_rates_fixture(tmp_path, symbols=REQUIRED_SYMBOLS[:-1])
    output_dir = tmp_path / "out"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-pressure-feasibility-audit",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
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
    assert not (output_dir / "liquidity_rates_data_feasibility_summary.json").exists()


def test_liquidity_rates_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "liquidity_rates_data_feasibility_audit"
    )

    assert entry["command"] == (
        "aits research trends liquidity-rates-pressure-feasibility-audit"
    )
    assert entry["artifact_role"] == "liquidity_rates_feasibility_audit"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["generator_implemented"] is False
    assert entry["candidate_artifact_generated"] is False
    assert entry["actual_path_validation_executed"] is False
    assert entry["scope_review_executed"] is False
    assert entry["partial_poc_possible"] is True
    assert entry["full_liquidity_pressure_poc_ready"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "liquidity_rates_data_feasibility_audit" in catalog
    assert STATUS in catalog
    assert "full_liquidity_pressure_poc_ready=false" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2311" in system_flow
    assert "liquidity-rates-pressure-feasibility-audit" in system_flow
    assert "validate_data_cache" in system_flow
    assert "missing IEF / UUP / HYG / LQD" in system_flow


def _write_liquidity_rates_fixture(
    tmp_path: Path,
    *,
    symbols: tuple[str, ...] = REQUIRED_SYMBOLS,
) -> dict[str, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    dates = pd.bdate_range("2026-04-01", "2026-06-29")

    price_rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(symbols):
        base = 90.0 + symbol_index * 15.0
        drift = 0.0008 + symbol_index * 0.0001
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

    return {"prices": prices_path, "rates": rates_path}
