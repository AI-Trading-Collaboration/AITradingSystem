from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.liquidity_rates_actual_path_validation import (
    STATUS_CONTINUE_RESEARCH as SOURCE_STATUS_CONTINUE_RESEARCH,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    STATUS_INCONCLUSIVE as SOURCE_STATUS_INCONCLUSIVE,
)
from ai_trading_system.liquidity_rates_scope_review import (
    ALLOWED_STATUSES,
    STATUS_DIAGNOSTIC_ONLY,
    STATUS_READY_RESEARCH_ONLY,
    STATUS_REJECT_RECOMMENDED,
    run_liquidity_rates_scope_review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_SYMBOLS = ("QQQ", "SMH", "TLT", "SHY")
RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")
CANDIDATES = (
    "duration_pressure_proxy_v1",
    "rates_pressure_exposure_cap_modifier_v1",
)
BLOCKED_CANDIDATE = "liquidity_headwind_proxy_v1"
SCOPE_RESULT = "DIAGNOSTIC_ONLY_WITH_LIMITED_RISK_CAP_RESEARCH_CANDIDATE"


def test_liquidity_rates_scope_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "liquidity-rates-scope-review" in result.output


def test_liquidity_rates_scope_review_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/liquidity_rates_scope_review_policy.yaml")
    )

    assert policy["policy_id"] == "liquidity_rates_scope_review_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["market_regime"] == "unified_primary_2021"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert set(policy["status_rule"]["allowed_statuses"]) == ALLOWED_STATUSES
    assert set(policy["data_quality"]["required_price_symbols"]) == set(REQUIRED_SYMBOLS)
    assert set(policy["source_boundary"]["allowed_source_statuses"]) == {
        SOURCE_STATUS_CONTINUE_RESEARCH,
        SOURCE_STATUS_INCONCLUSIVE,
    }
    assert set(policy["source_boundary"]["generated_candidates"]) == set(CANDIDATES)
    assert BLOCKED_CANDIDATE in policy["source_boundary"]["blocked_candidates"]
    assert policy["scope_options"]["owner_review_horizons"] == ["10d", "20d", "1m"]
    assert set(policy["scope_options"]["use_cases"]) == {
        "risk_cap_modifier",
        "no_add_gate",
        "max_exposure_limiter",
        "diagnostic_only",
    }

    required_thresholds = {
        "minimum_candidate_records",
        "minimum_horizon_records",
        "minimum_use_case_records",
        "candidate_keep_alignment_score",
        "candidate_reject_alignment_score",
        "horizon_keep_alignment_score",
        "horizon_reject_alignment_score",
        "use_case_keep_alignment_score",
        "use_case_reject_alignment_score",
        "exposure_cap_support_score",
        "diagnostic_keep_min_records",
        "minimum_objective_pass_count_for_ready",
    }
    assert set(policy["scope_thresholds"]) == required_thresholds
    for threshold in policy["scope_thresholds"].values():
        assert "value" in threshold
        assert threshold["rationale"]

    assert policy["safety"]["research_only"] is True
    assert policy["safety"]["actual_path_validation_consumed"] is True
    assert policy["safety"]["scope_review_executed"] is True
    assert policy["safety"]["forward_observe_started"] is False
    assert policy["safety"]["owner_approval_required_before_forward_observe"] is True
    assert policy["safety"]["partial_rates_only_scope_review"] is True
    assert policy["safety"]["liquidity_headwind_scope_review_executed"] is False
    assert policy["safety"]["full_liquidity_pressure_scope_ready"] is False
    assert policy["safety"]["promotion_allowed"] is False
    assert policy["safety"]["paper_shadow_allowed"] is False
    assert policy["safety"]["production_allowed"] is False
    assert policy["safety"]["broker_action"] == "none"
    assert policy["safety"]["dynamic_promotion_status"] == "BLOCKED"


def test_liquidity_rates_scope_review_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "liquidity_rates_scope_review"
    )

    assert entry["command"] == "aits research trends liquidity-rates-scope-review"
    assert entry["artifact_role"] == "liquidity_rates_scope_review"
    assert entry["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert entry["actual_path_validation_consumed"] is True
    assert entry["scope_review_executed"] is True
    assert entry["forward_observe_started"] is False
    assert entry["owner_approval_required_before_forward_observe"] is True
    assert entry["partial_rates_only_scope_review"] is True
    assert entry["liquidity_headwind_scope_review_executed"] is False
    assert entry["full_liquidity_pressure_scope_ready"] is False
    assert entry["validation_status"] == STATUS_DIAGNOSTIC_ONLY
    assert entry["source_validation_status"] == SOURCE_STATUS_INCONCLUSIVE
    assert entry["scope_review_result"] == SCOPE_RESULT
    assert set(entry["recommended_candidate_ids"]) == {"duration_pressure_proxy_v1"}
    assert set(entry["diagnostic_candidate_ids"]) == {
        "rates_pressure_exposure_cap_modifier_v1"
    }
    assert set(entry["recommended_use_cases"]) == {
        "risk_cap_modifier",
        "max_exposure_limiter",
        "diagnostic_only",
    }
    assert entry["rejected_use_cases"] == ["no_add_gate"]
    assert entry["blocked_candidate_ids"] == [BLOCKED_CANDIDATE]
    assert set(entry["allowed_statuses"]) == ALLOWED_STATUSES
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "liquidity_rates_scope_review" in catalog
    assert STATUS_DIAGNOSTIC_ONLY in catalog
    assert SCOPE_RESULT in catalog
    assert "UUP/HYG/LQD source gap" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2314" in system_flow
    assert "liquidity-rates-scope-review" in system_flow
    assert "validate_data_cache" in system_flow
    assert "owner_approval_required_before_forward_observe=true" in system_flow


def test_liquidity_rates_scope_review_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = _write_scope_review_fixture(tmp_path)
    output_dir = tmp_path / "scope_review"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-scope-review",
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
        (output_dir / "liquidity_rates_scope_review_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS_DIAGNOSTIC_ONLY
    assert summary["status"] == STATUS_DIAGNOSTIC_ONLY
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["actual_requested_date_range"] == "2026-05-15..2026-06-29"
    assert summary["data_quality_status"] == "PASS"
    assert summary["source_status"] == SOURCE_STATUS_INCONCLUSIVE
    assert summary["scope_review_result"] == SCOPE_RESULT
    assert summary["recommended_candidate_ids"] == ["duration_pressure_proxy_v1"]
    assert summary["diagnostic_candidate_ids"] == [
        "rates_pressure_exposure_cap_modifier_v1"
    ]
    assert summary["preferred_owner_review_horizons"] == ["10d"]
    assert summary["diagnostic_owner_review_horizons"] == ["20d", "1m"]
    assert set(summary["recommended_use_cases"]) == {
        "risk_cap_modifier",
        "max_exposure_limiter",
        "diagnostic_only",
    }
    assert "no_add_gate" in summary["not_recommended_as"]
    assert "scope_ready_research_only" in summary["not_recommended_as"]
    assert summary["partial_rates_only_scope_review"] is True
    assert summary["liquidity_headwind_scope_review_executed"] is False
    assert summary["full_liquidity_pressure_scope_ready"] is False
    assert summary["forward_observe_started"] is False
    assert summary["dynamic_promotion_status"] == "BLOCKED"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    candidate_rows = json.loads(
        (output_dir / "liquidity_rates_candidate_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_candidate = {row["scope_id"]: row for row in candidate_rows}
    assert by_candidate["duration_pressure_proxy_v1"]["scope_decision"] == (
        "KEEP_RESEARCH_SCOPE"
    )
    assert by_candidate["rates_pressure_exposure_cap_modifier_v1"][
        "scope_decision"
    ] == "DIAGNOSTIC_ONLY"
    assert BLOCKED_CANDIDATE not in by_candidate

    horizon_rows = json.loads(
        (output_dir / "liquidity_rates_horizon_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_horizon = {row["scope_id"]: row for row in horizon_rows}
    assert by_horizon["10d"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"
    assert by_horizon["20d"]["scope_decision"] == "DIAGNOSTIC_ONLY"
    assert by_horizon["1m"]["scope_decision"] == "DIAGNOSTIC_ONLY"

    use_case_rows = json.loads(
        (output_dir / "liquidity_rates_use_case_scope_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    by_use_case = {row["scope_id"]: row for row in use_case_rows}
    assert by_use_case["risk_cap_modifier"]["scope_decision"] == (
        "KEEP_RESEARCH_SCOPE"
    )
    assert by_use_case["max_exposure_limiter"]["scope_decision"] == (
        "KEEP_RESEARCH_SCOPE"
    )
    assert by_use_case["diagnostic_only"]["scope_decision"] == "KEEP_RESEARCH_SCOPE"
    assert by_use_case["no_add_gate"]["scope_decision"] == "REJECT_CURRENT_SCOPE"

    recommended = json.loads(
        (output_dir / "liquidity_rates_recommended_scope.json").read_text(
            encoding="utf-8"
        )
    )["recommended_scope"]
    assert recommended["source_gap_exclusion"][BLOCKED_CANDIDATE]
    assert recommended["liquidity_headwind_scope_review_executed"] is False

    safety = json.loads(
        (output_dir / "liquidity_rates_scope_review_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_modify_generator_artifacts"] is True
    assert safety["does_not_modify_actual_path_validation_artifacts"] is True
    assert safety["does_not_start_forward_observe"] is True
    assert safety["does_not_create_liquidity_headwind_scope"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"
    assert (docs_root / "liquidity_rates_scope_review.md").exists()


def test_liquidity_rates_scope_review_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    fixture = _write_scope_review_fixture(tmp_path)

    payload = run_liquidity_rates_scope_review(
        validation_dir=fixture["validation_dir"],
        prices_path=fixture["prices"],
        rates_path=fixture["rates"],
        marketstack_prices_path=tmp_path / "missing_marketstack.csv",
        quality_as_of="2026-06-29",
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS_DIAGNOSTIC_ONLY
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_liquidity_rates_scope_review_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-scope-review",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_liquidity_rates_scope_review_fails_closed_missing_required_anchor(
    tmp_path: Path,
) -> None:
    fixture = _write_scope_review_fixture(tmp_path)
    output_dir = tmp_path / "missing_symbol_out"
    prices_missing_shy = tmp_path / "prices_missing_shy.csv"
    prices = pd.read_csv(fixture["prices"])
    prices.loc[prices["ticker"] != "SHY"].to_csv(prices_missing_shy, index=False)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-scope-review",
            "--validation-dir",
            str(fixture["validation_dir"]),
            "--prices-path",
            str(prices_missing_shy),
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
    assert not (output_dir / "liquidity_rates_scope_review_summary.json").exists()


def test_liquidity_rates_scope_review_preserves_liquidity_headwind_blocker(
    tmp_path: Path,
) -> None:
    fixture = _write_scope_review_fixture(tmp_path)
    source_summary_path = (
        fixture["validation_dir"] / "liquidity_rates_actual_path_validation_summary.json"
    )
    payload = json.loads(source_summary_path.read_text(encoding="utf-8"))
    payload["summary"]["blocked_candidate_ids"] = []
    source_summary_path.write_text(json.dumps(payload), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "liquidity-rates-scope-review",
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
            str(tmp_path / "out"),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
    assert "source gap" in str(result.exception)


def test_liquidity_rates_scope_review_status_constants_match_policy() -> None:
    assert ALLOWED_STATUSES == {
        STATUS_READY_RESEARCH_ONLY,
        STATUS_DIAGNOSTIC_ONLY,
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
        _candidate_row(
            candidate_id="duration_pressure_proxy_v1",
            status=SOURCE_STATUS_CONTINUE_RESEARCH,
            score=0.04,
        ),
        _candidate_row(
            candidate_id="rates_pressure_exposure_cap_modifier_v1",
            status=SOURCE_STATUS_INCONCLUSIVE,
            score=0.02,
        ),
    ]
    objective_rows = [
        _objective_row("qqq_smh_valuation_pressure", "INCONCLUSIVE_OR_WEAK", -0.06),
        _objective_row("high_duration_asset_drawdown", "FAIL", -0.06),
        _objective_row("risk_on_exposure_cap", "PASS", 0.2),
    ]
    horizon_rows = [
        _horizon_row("10d", "PASS", 0.05),
        _horizon_row("20d", "INCONCLUSIVE_OR_WEAK", 0.02),
        _horizon_row("1m", "INCONCLUSIVE_OR_WEAK", 0.01),
    ]
    source_summary = {
        "schema_version": "liquidity_rates_actual_path_validation.summary.v1",
        "report_type": "liquidity_rates_actual_path_validation",
        "task_id": "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION",
        "status": SOURCE_STATUS_INCONCLUSIVE,
        "market_regime": "unified_primary_2021",
        "selected_market_regime": "unified_primary_2021",
        "requested_start_date": "2026-05-15",
        "requested_end_date": "2026-06-29",
        "actual_requested_date_range": "2026-05-15..2026-06-29",
        "validation_eligible_record_count": len(outcome_rows),
        "candidate_ids": list(CANDIDATES),
        "blocked_candidate_ids": [BLOCKED_CANDIDATE],
        "data_quality": {"status": "PASS", "as_of": "2026-06-29"},
        "data_quality_status": "PASS",
        "actual_path_validation_executed": True,
        "scope_review_ready": False,
        "partial_rates_only_validation": True,
        "liquidity_headwind_validation_executed": False,
        "full_liquidity_pressure_validation_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    (validation_dir / "liquidity_rates_actual_path_validation_summary.json").write_text(
        json.dumps({**source_summary, "summary": source_summary}),
        encoding="utf-8",
    )
    (validation_dir / "liquidity_rates_prediction_outcome_matrix.json").write_text(
        json.dumps({"rows": outcome_rows}),
        encoding="utf-8",
    )
    (validation_dir / "liquidity_rates_candidate_scorecard.json").write_text(
        json.dumps({"candidate_scorecards": candidate_rows}),
        encoding="utf-8",
    )
    (validation_dir / "liquidity_rates_objective_coverage_matrix.json").write_text(
        json.dumps({"objective_rows": objective_rows}),
        encoding="utf-8",
    )
    (validation_dir / "liquidity_rates_horizon_coverage_matrix.json").write_text(
        json.dumps({"horizon_rows": horizon_rows}),
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
    for value_date in dates:
        source_date = value_date.date().isoformat()
        for candidate_id, signal_name in (
            (
                "duration_pressure_proxy_v1",
                "duration_pressure_valuation_pressure_yield_curve_score",
            ),
            (
                "rates_pressure_exposure_cap_modifier_v1",
                "rates_pressure_exposure_cap_modifier_score",
            ),
        ):
            for target_asset in ("QQQ", "SMH"):
                for horizon in ("10d", "20d", "1m"):
                    rows.append(
                        _outcome_row(
                            candidate_id=candidate_id,
                            signal_name=signal_name,
                            source_date=source_date,
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
    is_rates_candidate = candidate_id == "rates_pressure_exposure_cap_modifier_v1"
    return {
        "candidate_id": candidate_id,
        "target_asset": target_asset,
        "horizon": horizon,
        "source_date": source_date,
        "decision_timestamp": f"{source_date}T21:00:00+00:00",
        "signal_name": signal_name,
        "validation_eligible": True,
        "combined_alignment_score": 0.02 if is_rates_candidate else 0.04,
        "valuation_pressure_score": None if is_rates_candidate else -0.06,
        "duration_drawdown_score": None if is_rates_candidate else -0.06,
        "exposure_cap_score": 0.2 if is_rates_candidate else 0.12,
        "target_forward_return": 0.01,
        "target_max_drawdown": -0.02,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _candidate_row(candidate_id: str, status: str, score: float) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "validation_eligible_record_count": 420,
        "average_alignment_score": score,
        "candidate_validation_status": status,
        "scope_review_ready": False,
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


def _horizon_row(horizon: str, status: str, score: float) -> dict[str, object]:
    return {
        "horizon": horizon,
        "eligible_record_count": 140,
        "average_alignment_score": score,
        "horizon_status": status,
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
        base = 90.0 + symbol_index * 12.0
        drift = 0.0007 + symbol_index * 0.0002
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
