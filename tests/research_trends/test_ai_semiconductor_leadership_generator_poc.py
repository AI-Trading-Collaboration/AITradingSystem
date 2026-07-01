from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_CANDIDATES,
    FULL_UNIVERSE_BLOCKER,
    STATUS,
    run_ai_semiconductor_leadership_generator_poc,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "NVDA", "AMD", "TSM", "AVGO", "ASML")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")


def test_ai_semiconductor_leadership_generator_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "ai-semiconductor-leadership-generator-poc" in result.output


def test_ai_semiconductor_leadership_generator_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/ai_semiconductor_leadership_generator_policy.yaml")
    )

    assert policy["policy_id"] == "ai_semiconductor_leadership_generator_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert set(policy["candidate_policy"]) == set(DEFAULT_CANDIDATES)
    assert policy["safety"]["promotion_allowed"] is False
    assert policy["safety"]["paper_shadow_allowed"] is False
    assert policy["safety"]["production_allowed"] is False
    assert policy["safety"]["broker_action"] == "none"


def test_ai_semiconductor_leadership_generator_registry_and_catalog_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "ai_semiconductor_leadership_generator_poc"
    )

    assert entry["command"] == "aits research trends ai-semiconductor-leadership-generator-poc"
    assert entry["artifact_role"] == "ai_semiconductor_leadership_generator_poc"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["generator_implemented"] is True
    assert entry["actual_path_validation_executed"] is False
    assert entry["actual_path_validation_ready"] is False
    assert entry["candidate_artifact_generated"] is True
    assert entry["candidate_signal_series_generated"] is True
    assert entry["candidate_prediction_artifact_generated"] is True
    assert entry["full_universe_readiness_claimed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "ai_semiconductor_leadership_generator_poc" in catalog
    assert "AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC_READY_VALIDATION_BLOCKED" in catalog
    assert "full_universe_readiness_claimed=false" in catalog


def test_ai_semiconductor_leadership_generator_cli_writes_outputs(
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
            "ai-semiconductor-leadership-generator-poc",
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
    summary_path = output_dir / "ai_semiconductor_leadership_generator_poc_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["status"] == STATUS
    assert summary["data_quality_status"] == "PASS"
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["actual_requested_date_range"] == "2026-05-15..2026-06-29"
    assert summary["full_universe_validation_blocker_out_of_scope"] == FULL_UNIVERSE_BLOCKER
    assert summary["summary"]["candidate_count"] == 3
    assert summary["summary"]["validation_status"] == "PASS"
    assert summary["summary"]["actual_path_validation_ready"] is False
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
        assert validation["promotion_allowed"] is False
        series = pd.read_csv(candidate_dir / "candidate_signal_series.csv")
        assert not series.empty
        assert set(series["candidate_id"]) == {candidate_id}

    safety = json.loads(
        (output_dir / "ai_leadership_generator_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["full_universe_readiness_claimed"] is False
    assert safety["does_not_run_actual_path_validation"] is True
    assert (docs_root / "ai_semiconductor_leadership_generator_poc.md").exists()


def test_ai_semiconductor_leadership_generator_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path)

    payload = run_ai_semiconductor_leadership_generator_poc(
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


def test_ai_semiconductor_leadership_generator_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-semiconductor-leadership-generator-poc",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_ai_semiconductor_leadership_generator_fails_closed_missing_required_symbol(
    tmp_path: Path,
) -> None:
    fixture = _write_generator_fixture(tmp_path, symbols=REQUIRED_SYMBOLS[:-1])
    output_dir = tmp_path / "out"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-semiconductor-leadership-generator-poc",
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
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
    assert (output_dir / "data_quality_2026-06-29.md").exists()
    assert not (output_dir / "ai_semiconductor_leadership_generator_poc_summary.json").exists()


def _write_generator_fixture(
    tmp_path: Path,
    *,
    symbols: tuple[str, ...] = REQUIRED_SYMBOLS,
) -> dict[str, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    feasibility_dir = tmp_path / "feasibility"
    feasibility_dir.mkdir(parents=True, exist_ok=True)
    (feasibility_dir / "ai_semiconductor_leadership_feasibility_summary.json").write_text(
        json.dumps(
            {
                "status": (
                    "AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY"
                ),
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )

    dates = pd.bdate_range("2026-04-01", "2026-06-29")
    price_rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(symbols):
        base = 100.0 + symbol_index * 8.0
        drift = 0.001 + symbol_index * 0.0001
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
        base = 4.0 + series_index * 0.5
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
