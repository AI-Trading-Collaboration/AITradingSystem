from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.ai_leadership_scope_review import (
    ALLOWED_STATUSES,
    STATUS_INCONCLUSIVE,
    STATUS_READY_RESEARCH_ONLY,
    STATUS_REJECT_RECOMMENDED,
    run_ai_leadership_scope_review,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    FULL_UNIVERSE_BLOCKER,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "NVDA", "AMD", "TSM", "AVGO", "ASML")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")
CANDIDATES = (
    "smh_relative_strength_leadership_v1",
    "ai_semiconductor_leadership_quality_v1",
    "ai_core_basket_leadership_v1",
)


def test_ai_leadership_scope_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "ai-leadership-scope-review" in result.output


def test_ai_leadership_scope_review_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/ai_leadership_scope_review_policy.yaml")
    )

    assert policy["policy_id"] == "ai_leadership_scope_review_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["market_regime"] == "unified_primary_2021"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert set(policy["status_rule"]["allowed_statuses"]) == ALLOWED_STATUSES
    assert set(policy["data_quality"]["required_price_symbols"]) == set(REQUIRED_SYMBOLS)
    assert policy["scope_options"]["owner_review_horizons"] == ["10d", "20d"]
    assert set(policy["scope_options"]["use_cases"]) == {
        "confirmation_only",
        "exposure_cap_modifier",
        "standalone_alpha",
    }

    for threshold in policy["scope_thresholds"].values():
        assert "value" in threshold
        assert threshold["rationale"]

    assert policy["safety"]["research_only"] is True
    assert policy["safety"]["actual_path_validation_consumed"] is True
    assert policy["safety"]["scope_review_executed"] is True
    assert policy["safety"]["forward_observe_started"] is False
    assert policy["safety"]["promotion_allowed"] is False
    assert policy["safety"]["paper_shadow_allowed"] is False
    assert policy["safety"]["production_allowed"] is False
    assert policy["safety"]["broker_action"] == "none"
    assert policy["safety"]["dynamic_promotion_status"] == "BLOCKED"


def test_ai_leadership_scope_review_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "ai_leadership_scope_review"
    )

    assert entry["command"] == "aits research trends ai-leadership-scope-review"
    assert entry["artifact_role"] == "ai_leadership_scope_review"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["actual_path_validation_consumed"] is True
    assert entry["scope_review_executed"] is True
    assert entry["forward_observe_started"] is False
    assert entry["owner_approval_required_before_forward_observe"] is True
    assert entry["recommended_asset_scope"] == "QQQ_PLUS_SMH_RESEARCH_ONLY"
    assert set(entry["recommended_use_cases"]) == {
        "confirmation_only",
        "exposure_cap_modifier",
    }
    assert set(entry["allowed_statuses"]) == ALLOWED_STATUSES
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "ai_leadership_scope_review" in catalog
    assert STATUS_READY_RESEARCH_ONLY in catalog
    assert "QQQ_PLUS_SMH_RESEARCH_ONLY" in catalog
    assert "forward observe start" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2310" in system_flow
    assert "ai-leadership-scope-review" in system_flow
    assert "validate_data_cache" in system_flow
    assert "owner_approval_required_before_forward_observe=true" in system_flow


def test_ai_leadership_scope_review_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = _write_scope_review_fixture(tmp_path)
    output_dir = tmp_path / "scope_review"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-scope-review",
            "--validation-dir",
            str(fixture["validation_dir"]),
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
    summary_payload = json.loads(
        (output_dir / "ai_leadership_scope_review_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS_READY_RESEARCH_ONLY
    assert summary["status"] == STATUS_READY_RESEARCH_ONLY
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["actual_requested_date_range"] == "2026-05-15..2026-06-29"
    assert summary["data_quality_status"] == "PASS"
    assert summary["full_universe_validation_blocker_out_of_scope"] == FULL_UNIVERSE_BLOCKER
    assert summary["recommended_asset_scope"] == "QQQ_PLUS_SMH_RESEARCH_ONLY"
    assert summary["preferred_owner_review_horizons"] == ["10d"]
    assert summary["diagnostic_owner_review_horizons"] == ["20d"]
    assert set(summary["recommended_use_cases"]) == {
        "confirmation_only",
        "exposure_cap_modifier",
    }
    assert "standalone_alpha" in summary["not_recommended_as"]
    assert summary["scope_review_executed"] is True
    assert summary["forward_observe_started"] is False
    assert summary["dynamic_promotion_status"] == "BLOCKED"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    asset_rows = json.loads(
        (output_dir / "ai_leadership_asset_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_asset = {row["scope_id"]: row for row in asset_rows}
    assert by_asset["smh_only"]["scope_decision"] == "DIAGNOSTIC_ONLY"
    assert by_asset["qqq_plus_smh"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"

    horizon_rows = json.loads(
        (output_dir / "ai_leadership_horizon_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_horizon = {row["scope_id"]: row for row in horizon_rows}
    assert by_horizon["10d"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"
    assert by_horizon["20d"]["scope_decision"] == "DIAGNOSTIC_ONLY"

    use_case_rows = json.loads(
        (output_dir / "ai_leadership_use_case_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_use_case = {row["scope_id"]: row for row in use_case_rows}
    assert by_use_case["confirmation_only"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"
    assert by_use_case["exposure_cap_modifier"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"
    assert by_use_case["standalone_alpha"]["scope_decision"] == "REJECT_CURRENT_SCOPE"

    safety = json.loads(
        (output_dir / "ai_leadership_scope_review_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_modify_generator_artifacts"] is True
    assert safety["does_not_modify_actual_path_validation_artifacts"] is True
    assert safety["does_not_start_forward_observe"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"
    assert (docs_root / "ai_semiconductor_leadership_scope_review.md").exists()


def test_ai_leadership_scope_review_direct_payload_has_string_paths(tmp_path: Path) -> None:
    fixture = _write_scope_review_fixture(tmp_path)

    payload = run_ai_leadership_scope_review(
        validation_dir=fixture["validation_dir"],
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        quality_as_of="2026-06-29",
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS_READY_RESEARCH_ONLY
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_ai_leadership_scope_review_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-scope-review",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_ai_leadership_scope_review_fails_closed_missing_required_symbol(
    tmp_path: Path,
) -> None:
    fixture = _write_scope_review_fixture(tmp_path)
    output_dir = tmp_path / "missing_symbol_out"
    prices_missing_asml = tmp_path / "prices_missing_asml.csv"
    prices = pd.read_csv(fixture["prices"])
    prices.loc[prices["ticker"] != "ASML"].to_csv(prices_missing_asml, index=False)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-leadership-scope-review",
            "--validation-dir",
            str(fixture["validation_dir"]),
            "--prices-path",
            str(prices_missing_asml),
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
    assert not (output_dir / "ai_leadership_scope_review_summary.json").exists()


def test_ai_leadership_scope_review_status_constants_match_policy() -> None:
    assert ALLOWED_STATUSES == {
        STATUS_READY_RESEARCH_ONLY,
        STATUS_INCONCLUSIVE,
        STATUS_REJECT_RECOMMENDED,
    }


def _write_scope_review_fixture(tmp_path: Path) -> dict[str, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    validation_dir = tmp_path / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    _write_price_and_rate_cache(prices_path, rates_path)
    outcome_rows = _outcome_rows()
    candidate_rows = [
        {
            "candidate_id": candidate_id,
            "validation_eligible_record_count": 420,
            "average_alignment_score": 0.04,
            "candidate_validation_status": "AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "dynamic_promotion_status": "BLOCKED",
        }
        for candidate_id in CANDIDATES
    ]
    objective_rows = [
        _objective_row("smh_future_relative_return", "INCONCLUSIVE_OR_WEAK", -0.1),
        _objective_row("qqq_smh_drawdown_risk", "PASS", 0.1),
        _objective_row("ai_leadership_weakening_windows", "INCONCLUSIVE_OR_WEAK", 0.02),
        _objective_row("smh_overweight_risk", "PASS", 0.06),
    ]
    source_summary = {
        "schema_version": "ai_leadership_actual_path_validation.summary.v1",
        "report_type": "ai_leadership_actual_path_validation",
        "task_id": "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION",
        "status": "AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH",
        "market_regime": "unified_primary_2021",
        "selected_market_regime": "unified_primary_2021",
        "requested_start_date": "2026-05-15",
        "requested_end_date": "2026-06-29",
        "actual_requested_date_range": "2026-05-15..2026-06-29",
        "validation_eligible_record_count": len(outcome_rows),
        "data_quality": {"status": "PASS", "as_of": "2026-06-29"},
        "data_quality_status": "PASS",
        "actual_path_validation_executed": True,
        "scope_review_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    (validation_dir / "ai_leadership_actual_path_validation_summary.json").write_text(
        json.dumps(
            {
                **source_summary,
                "summary": source_summary,
            }
        ),
        encoding="utf-8",
    )
    (validation_dir / "ai_leadership_prediction_outcome_matrix.json").write_text(
        json.dumps({"rows": outcome_rows}),
        encoding="utf-8",
    )
    (validation_dir / "ai_leadership_candidate_scorecard.json").write_text(
        json.dumps({"candidate_scorecards": candidate_rows}),
        encoding="utf-8",
    )
    (validation_dir / "ai_leadership_objective_coverage_matrix.json").write_text(
        json.dumps({"objective_rows": objective_rows}),
        encoding="utf-8",
    )
    return {
        "prices": prices_path,
        "rates": rates_path,
        "validation_dir": validation_dir,
    }


def _outcome_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2026-05-15", periods=35)
    signal_names = {
        "smh_relative_strength_leadership_v1": "smh_vs_qqq_relative_strength_score",
        "ai_semiconductor_leadership_quality_v1": (
            "ai_semiconductor_leadership_quality_score"
        ),
        "ai_core_basket_leadership_v1": "ai_core_basket_vs_qqq_score",
    }
    for value_date in dates:
        for candidate_id in CANDIDATES:
            for target_asset in ("QQQ", "SMH"):
                for horizon in ("5d", "10d", "20d"):
                    rows.append(
                        _outcome_row(
                            candidate_id=candidate_id,
                            signal_name=signal_names[candidate_id],
                            source_date=value_date.date().isoformat(),
                            target_asset=target_asset,
                            horizon=horizon,
                        )
                    )
    return rows


def _outcome_row(
    *,
    candidate_id: str,
    signal_name: str,
    source_date: str,
    target_asset: str,
    horizon: str,
) -> dict[str, object]:
    score_by_scope = {
        ("QQQ", "5d"): 0.1,
        ("QQQ", "10d"): 0.08,
        ("QQQ", "20d"): 0.02,
        ("SMH", "5d"): 0.03,
        ("SMH", "10d"): 0.02,
        ("SMH", "20d"): 0.0,
    }
    combined = score_by_scope[(target_asset, horizon)]
    smh_relative_score = -0.1 if target_asset == "SMH" else None
    return {
        "candidate_id": candidate_id,
        "target_asset": target_asset,
        "horizon": horizon,
        "source_date": source_date,
        "decision_timestamp": f"{source_date}T21:00:00+00:00",
        "signal_name": signal_name,
        "validation_eligible": True,
        "combined_alignment_score": combined,
        "smh_relative_return_score": smh_relative_score,
        "drawdown_risk_score": 0.1,
        "weakening_window_score": 0.02,
        "smh_overweight_risk_score": 0.06 if target_asset == "SMH" else None,
        "target_forward_return": 0.02 if target_asset == "SMH" else 0.01,
        "target_max_drawdown": -0.03 if target_asset == "SMH" else -0.02,
        "smh_relative_forward_return": 0.01,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _objective_row(objective_id: str, status: str, score: float) -> dict[str, object]:
    return {
        "objective_id": objective_id,
        "eligible_record_count": 420,
        "average_alignment_score": score,
        "objective_status": status,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _write_price_and_rate_cache(prices_path: Path, rates_path: Path) -> None:
    dates = pd.bdate_range("2026-04-01", "2026-06-29")
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
