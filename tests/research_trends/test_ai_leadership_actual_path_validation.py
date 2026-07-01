from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.ai_leadership_actual_path_validation import (
    ALLOWED_STATUSES,
    STATUS_CONTINUE_RESEARCH,
    STATUS_INCONCLUSIVE,
    STATUS_REJECT_RECOMMENDED,
    run_ai_leadership_actual_path_validation,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_CANDIDATES,
    FULL_UNIVERSE_BLOCKER,
    run_ai_semiconductor_leadership_generator_poc,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    STATUS as GENERATOR_STATUS,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "NVDA", "AMD", "TSM", "AVGO", "ASML")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")


def test_ai_leadership_actual_path_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "ai-leadership-actual-path-validation" in result.output


def test_ai_leadership_actual_path_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/ai_leadership_actual_path_validation_policy.yaml")
    )

    assert policy["policy_id"] == "ai_leadership_actual_path_validation_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert set(policy["status_rule"]["allowed_statuses"]) == ALLOWED_STATUSES
    assert set(policy["data_quality"]["required_price_symbols"]) == set(REQUIRED_SYMBOLS)
    assert policy["data_quality"]["full_universe_blocker_out_of_scope"]["ticker"] == "ASX"

    required_thresholds = {
        "minimum_candidate_records",
        "minimum_objective_records",
        "relative_return_positive_threshold",
        "relative_return_negative_threshold",
        "drawdown_warning_threshold",
        "false_risk_on_drawdown_threshold",
        "objective_pass_alignment_score",
        "candidate_continue_alignment_score",
        "candidate_reject_alignment_score",
        "continue_research_min_objectives_passed",
        "reject_recommended_min_candidates_rejected",
    }
    assert set(policy["validation_thresholds"]) == required_thresholds
    for threshold in policy["validation_thresholds"].values():
        assert "value" in threshold
        assert threshold["rationale"]

    assert policy["safety"]["research_only"] is True
    assert policy["safety"]["actual_path_validation_executed"] is True
    assert policy["safety"]["scope_review_ready"] is False
    assert policy["safety"]["promotion_allowed"] is False
    assert policy["safety"]["paper_shadow_allowed"] is False
    assert policy["safety"]["production_allowed"] is False
    assert policy["safety"]["broker_action"] == "none"
    assert policy["safety"]["dynamic_promotion_status"] == "BLOCKED"


def test_ai_leadership_actual_path_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "ai_leadership_actual_path_validation"
    )

    assert entry["command"] == "aits research trends ai-leadership-actual-path-validation"
    assert entry["artifact_role"] == "ai_leadership_actual_path_validation"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["actual_path_validation_executed"] is True
    assert entry["scope_review_ready"] is False
    assert entry["generator_implemented"] is False
    assert entry["candidate_artifact_generated"] is False
    assert entry["candidate_signal_series_generated"] is False
    assert entry["full_universe_readiness_claimed"] is False
    assert set(entry["allowed_statuses"]) == ALLOWED_STATUSES
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "ai_leadership_actual_path_validation" in catalog
    assert STATUS_CONTINUE_RESEARCH in catalog
    assert "scope_review_ready=false" in catalog
    assert "dynamic_promotion_status=BLOCKED" in catalog
    assert "不是 scope review ready" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2309" in system_flow
    assert "ai-leadership-actual-path-validation" in system_flow
    assert "validate_data_cache" in system_flow
    assert "dynamic_promotion_status=BLOCKED" in system_flow


def test_ai_leadership_actual_path_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = _write_actual_path_fixture(tmp_path)
    output_dir = tmp_path / "actual_path"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-actual-path-validation",
            "--prices-path",
            str(fixture["prices"]),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--generator-dir",
            str(fixture["generator_dir"]),
            "--quality-as-of",
            "2026-07-31",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output

    summary_path = output_dir / "ai_leadership_actual_path_validation_summary.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary = summary_payload["summary"]
    assert summary_payload["status"] in ALLOWED_STATUSES
    assert summary["status"] in ALLOWED_STATUSES
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["actual_requested_date_range"] == "2026-05-15..2026-06-29"
    assert summary["data_quality_status"] == "PASS"
    assert summary["full_universe_validation_blocker_out_of_scope"] == FULL_UNIVERSE_BLOCKER
    assert summary["actual_path_record_count"] > 0
    assert summary["validation_eligible_record_count"] > 0
    assert summary["actual_path_validation_executed"] is True
    assert summary["scope_review_ready"] is False
    assert summary["dynamic_promotion_status"] == "BLOCKED"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    scorecard = json.loads(
        (output_dir / "ai_leadership_candidate_scorecard.json").read_text(
            encoding="utf-8"
        )
    )
    scorecards = scorecard["candidate_scorecards"]
    assert {row["candidate_id"] for row in scorecards} == set(DEFAULT_CANDIDATES)
    assert all(row["candidate_validation_status"] in ALLOWED_STATUSES for row in scorecards)
    assert all(row["scope_review_ready"] is False for row in scorecards)
    assert all(row["dynamic_promotion_status"] == "BLOCKED" for row in scorecards)

    objectives = json.loads(
        (output_dir / "ai_leadership_objective_coverage_matrix.json").read_text(
            encoding="utf-8"
        )
    )["objective_rows"]
    assert {row["objective_id"] for row in objectives} == {
        "smh_future_relative_return",
        "qqq_smh_drawdown_risk",
        "ai_leadership_weakening_windows",
        "smh_overweight_risk",
    }

    safety = json.loads(
        (output_dir / "ai_leadership_actual_path_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_modify_generator_artifacts"] is True
    assert safety["does_not_run_scope_review"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"
    assert (docs_root / "ai_semiconductor_leadership_actual_path_validation.md").exists()
    assert (output_dir / "ai_leadership_actual_path_matrix.csv").exists()
    assert (output_dir / "ai_leadership_prediction_outcome_matrix.csv").exists()


def test_ai_leadership_actual_path_direct_payload_has_string_paths(tmp_path: Path) -> None:
    fixture = _write_actual_path_fixture(tmp_path)

    payload = run_ai_leadership_actual_path_validation(
        generator_dir=fixture["generator_dir"],
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        quality_as_of="2026-07-31",
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] in ALLOWED_STATUSES
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_ai_leadership_actual_path_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-actual-path-validation",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_ai_leadership_actual_path_fails_closed_missing_required_symbol(
    tmp_path: Path,
) -> None:
    fixture = _write_actual_path_fixture(tmp_path)
    output_dir = tmp_path / "missing_symbol_out"
    prices_missing_asml = tmp_path / "prices_missing_asml.csv"
    prices = pd.read_csv(fixture["prices"])
    prices.loc[prices["ticker"] != "ASML"].to_csv(prices_missing_asml, index=False)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-actual-path-validation",
            "--prices-path",
            str(prices_missing_asml),
            "--rates-path",
            str(fixture["rates"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--generator-dir",
            str(fixture["generator_dir"]),
            "--quality-as-of",
            "2026-07-31",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
    assert (output_dir / "data_quality_2026-07-31.md").exists()
    assert not (output_dir / "ai_leadership_actual_path_validation_summary.json").exists()


def test_ai_leadership_actual_path_status_constants_match_policy() -> None:
    assert ALLOWED_STATUSES == {
        STATUS_CONTINUE_RESEARCH,
        STATUS_INCONCLUSIVE,
        STATUS_REJECT_RECOMMENDED,
    }


def _write_actual_path_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = _write_generator_fixture(tmp_path)
    generator_dir = tmp_path / "generator"
    generator_payload = run_ai_semiconductor_leadership_generator_poc(
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        feasibility_dir=fixture["feasibility_dir"],
        start_date="2026-05-15",
        end_date="2026-06-29",
        quality_as_of="2026-07-31",
        output_dir=generator_dir,
        docs_root=tmp_path / "generator_docs",
    )
    assert generator_payload["status"] == GENERATOR_STATUS
    return {
        "prices": fixture["prices"],
        "rates": fixture["rates"],
        "generator_dir": generator_dir,
    }


def _write_generator_fixture(tmp_path: Path) -> dict[str, Path]:
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

    dates = pd.bdate_range("2026-04-01", "2026-07-31")
    price_rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(REQUIRED_SYMBOLS):
        base = 100.0 + symbol_index * 8.0
        drift = 0.001 + symbol_index * 0.00015
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
