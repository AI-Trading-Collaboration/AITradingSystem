from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.satellite import (
    SATELLITE_SAFETY,
    SatelliteUniverseConfig,
    build_etf_replacement_plan,
    build_replacement_eligibility_gate,
    build_satellite_candidate_scores,
    build_satellite_policy_validation_report,
    build_satellite_relative_strength_features,
    build_satellite_replacement_report,
    build_satellite_shadow_portfolio_experiment,
    load_satellite_policy_config,
    load_satellite_universe_config,
    render_satellite_policy_validation_markdown,
    render_satellite_replacement_report_markdown,
    satellite_price_symbols,
    satellite_score_band,
    stock_benchmark_mapping,
    validate_satellite_data_availability,
    write_satellite_policy_validation_report,
    write_satellite_replacement_report,
)
from ai_trading_system.reports.reader_brief import (
    _etf_satellite_replacement_summary,
    render_reader_brief_html,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUN_DATE = date(2026, 6, 1)


def test_satellite_universe_config_loads_and_validates_metadata() -> None:
    config = load_satellite_universe_config()

    assert config.policy_metadata.version == "satellite_universe_v0_1"
    assert config.safety.model_dump(mode="json") == SATELLITE_SAFETY
    assert "NVDA" in {stock.ticker for stock in config.satellite_universe}
    for stock in config.satellite_universe:
        assert stock.ticker == stock.ticker.upper()
        assert stock.name
        assert stock.group
        assert stock.role
        assert stock.benchmark_etf in config.allowed_benchmarks
        assert stock.sleeve
        assert 0.0 < stock.max_single_name_weight <= 1.0
        assert 0.0 <= stock.min_data_coverage <= 1.0
        assert stock.event_risk_group


def test_satellite_mapping_and_data_availability_rules() -> None:
    config = load_satellite_universe_config()

    assert stock_benchmark_mapping(config, "NVDA")["benchmark_etf"] == "SMH"
    assert stock_benchmark_mapping(config, "AMD")["benchmark_etf"] == "SMH"
    assert stock_benchmark_mapping(config, "TSM")["benchmark_etf"] == "SMH"
    assert stock_benchmark_mapping(config, "MSFT")["benchmark_etf"] == "QQQ"
    with pytest.raises(KeyError, match="unknown satellite ticker"):
        stock_benchmark_mapping(config, "UNKNOWN")

    available = set(satellite_price_symbols(config)) - {"ASML"}
    availability = validate_satellite_data_availability(config, available)

    assert availability["status"] == "PASS_WITH_WARNINGS"
    assert any("ASML:missing_optional:ASML" == item for item in availability["warnings"])
    assert not availability["errors"]


def test_satellite_duplicate_disabled_and_invalid_benchmark_handling() -> None:
    raw = _raw_satellite_config()
    duplicate = deepcopy(raw["satellite_universe"][0])
    duplicate["optional"] = True
    duplicate["role"] = "duplicate_optional_role"
    raw["satellite_universe"].append(duplicate)
    raw["satellite_universe"][1]["enabled"] = False
    config = SatelliteUniverseConfig.model_validate(raw)

    with pytest.raises(KeyError, match="disabled"):
        stock_benchmark_mapping(config, raw["satellite_universe"][1]["ticker"])
    first = stock_benchmark_mapping(config, "NVDA")
    second = stock_benchmark_mapping(config, "NVDA")
    assert first == second

    raw_bad = _raw_satellite_config()
    raw_bad["satellite_universe"][0]["benchmark_etf"] = "BAD"
    with pytest.raises(ValueError, match="invalid benchmark ETF"):
        SatelliteUniverseConfig.model_validate(raw_bad)


def test_satellite_relative_strength_features_are_no_lookahead() -> None:
    config = load_satellite_universe_config()
    prices = _satellite_prices({"NVDA": 0.60, "SMH": 0.20, "QQQ": 0.15})
    features = build_satellite_relative_strength_features(
        prices,
        universe_config=config,
        run_date=RUN_DATE,
    )
    future_prices = pd.concat(
        [
            prices,
            pd.DataFrame(
                [
                    {
                        "date": "2026-06-10",
                        "symbol": "NVDA",
                        "adj_close": 10000.0,
                        "close": 10000.0,
                        "open": 10000.0,
                        "high": 10000.0,
                        "low": 10000.0,
                        "volume": 1_000_000,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    future_features = build_satellite_relative_strength_features(
        future_prices,
        universe_config=config,
        run_date=RUN_DATE,
    )
    nvda = _by_ticker(features, "NVDA")
    future_nvda = _by_ticker(future_features, "NVDA")

    assert nvda["relative_return_60d"] > 0
    assert nvda["stock_above_50d_ma"] is True
    assert nvda["relative_price_above_50d_ma"] is True
    assert nvda["stock_drawdown_from_60d_high"] <= 0
    assert nvda["relative_volatility"] is not None
    assert nvda["feature_date"] == RUN_DATE.isoformat()
    assert nvda["earliest_execution_date"] > nvda["score_date"]
    assert future_nvda["relative_return_60d"] == pytest.approx(nvda["relative_return_60d"])


def test_satellite_score_gate_and_plan_respect_fallback_and_constraints() -> None:
    universe = load_satellite_universe_config()
    policy = load_satellite_policy_config()
    prices = _satellite_prices({"NVDA": 0.60, "AMD": 0.05, "SMH": 0.20, "QQQ": 0.15})
    features = build_satellite_relative_strength_features(
        prices,
        universe_config=universe,
        run_date=RUN_DATE,
    )
    scores = build_satellite_candidate_scores(
        features,
        universe_config=universe,
        policy_config=policy,
        run_date=RUN_DATE,
        ai_confirmation_payload=_ai_payload(score=70.0, event_risk=10.0),
    )
    gates = build_replacement_eligibility_gate(
        scores,
        universe_config=universe,
        policy_config=policy,
        base_weights=_base_weights(),
    )
    plan = build_etf_replacement_plan(
        run_date=RUN_DATE,
        base_weights=_base_weights(),
        eligibility_records=gates,
        universe_config=universe,
        policy_config=policy,
    )

    assert _by_ticker(scores, "NVDA")["score_value"] > _by_ticker(scores, "AMD")["score_value"]
    assert _by_ticker(gates, "NVDA")["status"] in {"eligible", "watch"}
    assert _by_ticker(gates, "AMD")["fallback_to_etf"] is True
    assert plan["candidate_weights"] == plan["shadow_weights"]
    assert plan["candidate_weights"] == plan["hypothetical_weights"]
    assert "target_weights" not in plan
    assert sum(plan["candidate_weights"].values()) == pytest.approx(1.0)
    assert plan["candidate_weights"]["SMH"] <= plan["base_weights"]["SMH"]
    assert plan["total_replaced_weight"] <= policy.risk_constraints.max_total_satellite_weight


def test_satellite_gate_blocks_high_drawdown_event_risk_and_unsafe_payload() -> None:
    universe = load_satellite_universe_config()
    policy = load_satellite_policy_config()
    prices = _satellite_prices({"NVDA": 0.60, "SMH": 0.20})
    features = build_satellite_relative_strength_features(
        prices,
        universe_config=universe,
        run_date=RUN_DATE,
    )
    scores = build_satellite_candidate_scores(
        features,
        universe_config=universe,
        policy_config=policy,
        run_date=RUN_DATE,
        ai_confirmation_payload=_ai_payload(score=20.0, event_risk=90.0),
    )
    nvda = _by_ticker(scores, "NVDA")
    nvda["stock_drawdown_from_60d_high"] = -0.30
    nvda["production_effect"] = "target_weights"
    gate = build_replacement_eligibility_gate(
        [nvda],
        universe_config=universe,
        policy_config=policy,
        base_weights=_base_weights(),
    )[0]

    assert gate["status"] == "blocked"
    assert "HIGH_DRAWDOWN" in gate["blockers"]
    assert "HIGH_EVENT_RISK" in gate["blockers"]
    assert "AI_CONFIRMATION_WEAK" in gate["blockers"]
    assert "UNSAFE_PRODUCTION_EFFECT" in gate["blockers"]


def test_satellite_report_shadow_experiment_and_markdown() -> None:
    universe = load_satellite_universe_config()
    policy = load_satellite_policy_config()
    report = build_satellite_replacement_report(
        prices=_satellite_prices({"NVDA": 0.60, "SMH": 0.20, "QQQ": 0.15}),
        universe_config=universe,
        policy_config=policy,
        run_date=RUN_DATE,
        data_quality_status="PASS",
        data_quality_report="reports/etf_portfolio/data_quality_2026-06-01.md",
        base_weights=_base_weights(),
        ai_confirmation_payload=_ai_payload(score=70.0, event_risk=10.0),
        market_regime="ai_after_chatgpt",
        requested_date_range={"start": "2025-09-01", "end": RUN_DATE.isoformat()},
    )
    experiment = build_satellite_shadow_portfolio_experiment(
        run_date=RUN_DATE,
        replacement_plan=report["replacement_plan"],
        universe_config=universe,
        base_candidate_id="satellite_replacement_v1",
    )
    markdown = render_satellite_replacement_report_markdown(report)

    assert report["schema_version"] == "satellite_replacement_report_v1"
    assert report["observe_only"] is True
    assert report["candidate_only"] is True
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"
    assert report["manual_review_required"] is True
    assert report["replacement_plan"]["fallback_to_etf"] is True
    assert experiment["after_candidate_weights"] == experiment["candidate_weights"]
    assert "future returns are evaluation-only" in experiment["decision_input_usage"]
    assert "# Satellite Replacement Report" in markdown
    assert "## Replacement Plan Summary" in markdown
    assert "observe_only=true" in markdown


def test_satellite_report_writer_and_cli(tmp_path: Path) -> None:
    universe = load_satellite_universe_config()
    policy = load_satellite_policy_config()
    report = build_satellite_replacement_report(
        prices=_satellite_prices({"NVDA": 0.60, "SMH": 0.20, "QQQ": 0.15}),
        universe_config=universe,
        policy_config=policy,
        run_date=RUN_DATE,
        data_quality_status="PASS",
        base_weights=_base_weights(),
        ai_confirmation_payload=_ai_payload(score=70.0, event_risk=10.0),
    )
    json_path = tmp_path / "satellite_replacement_report_2026-06-01.json"
    markdown_path = tmp_path / "satellite_replacement_report_2026-06-01.md"
    write_satellite_replacement_report(report, json_path=json_path, markdown_path=markdown_path)

    assert json.loads(json_path.read_text(encoding="utf-8"))["candidate_only"] is True
    assert "candidate-only" in markdown_path.read_text(encoding="utf-8")

    prices_path = tmp_path / "prices.csv"
    _satellite_prices({"NVDA": 0.60, "SMH": 0.20, "QQQ": 0.15}).to_csv(
        prices_path,
        index=False,
    )
    output_dir = tmp_path / "reports"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "satellite",
            "report",
            "--prices-path",
            str(prices_path),
            "--date",
            RUN_DATE.isoformat(),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "satellite_replacement_report_2026-06-01.json").exists()
    assert "production_effect=none" in result.output


def test_satellite_validation_gate_passes_and_fails_missing_registry(tmp_path: Path) -> None:
    validation = build_satellite_policy_validation_report(
        universe_config=load_satellite_universe_config(),
        policy_config=load_satellite_policy_config(),
        report_registry=load_report_registry(DEFAULT_REPORT_REGISTRY_PATH),
        reader_brief_available=True,
        generated_at="2026-06-01T00:00:00+00:00",
    )

    assert validation["status"] == "PASS"
    assert validation["safe_for_shadow_replacement"] is True
    assert validation["production_weights_mutated"] is False
    assert {check["check_id"] for check in validation["checks"]} >= {
        "stock_to_etf_mapping_valid",
        "relative_strength_features_available",
        "candidate_score_available",
        "replacement_eligibility_gate_available",
        "replacement_plan_generator_available",
        "satellite_shadow_experiment_available",
        "reader_brief_section_available",
        "report_registry_integration",
    }

    missing_registry = build_satellite_policy_validation_report(
        universe_config=load_satellite_universe_config(),
        policy_config=load_satellite_policy_config(),
        report_registry={"reports": []},
        reader_brief_available=True,
    )
    assert missing_registry["status"] == "FAIL"
    assert any("REPORT_REGISTRY_MISSING" in item for item in missing_registry["blockers"])

    json_path = tmp_path / "validation.json"
    markdown_path = tmp_path / "validation.md"
    write_satellite_policy_validation_report(
        validation,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "PASS"
    assert "# Satellite Policy Validation Gate" in render_satellite_policy_validation_markdown(
        validation
    )


def test_satellite_validation_cli_outputs_json_and_markdown(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "satellite",
            "validate",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert list(tmp_path.glob("satellite_validation_*.json"))
    assert list(tmp_path.glob("satellite_validation_*.md"))


def test_reader_brief_satellite_summary_and_section() -> None:
    report = build_satellite_replacement_report(
        prices=_satellite_prices({"NVDA": 0.60, "SMH": 0.20, "QQQ": 0.15}),
        universe_config=load_satellite_universe_config(),
        policy_config=load_satellite_policy_config(),
        run_date=RUN_DATE,
        data_quality_status="PASS",
        base_weights=_base_weights(),
        ai_confirmation_payload=_ai_payload(score=70.0, event_risk=10.0),
    )
    summary = _etf_satellite_replacement_summary(
        {"reports": [{"report_id": "etf_satellite_replacement_report", "latest_artifact_path": ""}]}
    )
    payload = _minimal_reader_payload(report)
    html = render_reader_brief_html(payload)

    assert summary["availability"] == "MISSING"
    assert "Satellite Replacement" in html
    assert "candidate-only replacement" in html or "Default ETF exposure" in html


def test_satellite_score_band_mapping() -> None:
    policy = load_satellite_policy_config()

    assert satellite_score_band(82.0, policy) == "strong"
    assert satellite_score_band(72.0, policy) == "eligible"
    assert satellite_score_band(56.0, policy) == "watch"
    assert satellite_score_band(10.0, policy) == "fallback"


def _raw_satellite_config() -> dict[str, object]:
    return safe_load_yaml_path(Path("config/etf_portfolio/satellite_universe.yaml"))


def _by_ticker(rows: list[dict[str, object]], ticker: str) -> dict[str, object]:
    for row in rows:
        if row.get("ticker") == ticker:
            return row
    raise AssertionError(f"missing ticker row: {ticker}")


def _base_weights() -> dict[str, float]:
    return {"SPY": 0.25, "QQQ": 0.45, "SMH": 0.20, "SOXX": 0.0, "CASH": 0.10}


def _ai_payload(*, score: float, event_risk: float) -> dict[str, object]:
    return {
        "report_type": "ai_confirmation_report",
        "AIConfirmationScore": {
            "score_value": score,
            "score_band": "confirm" if score >= 65.0 else "weak",
            "action_hint": "supports_neutral_ai_exposure",
        },
        "event_risk_overlay": {
            "event_risk_score": event_risk,
            "risk_band": "high" if event_risk >= 75.0 else "low",
        },
    }


def _satellite_prices(slopes: dict[str, float]) -> pd.DataFrame:
    universe = load_satellite_universe_config()
    symbols = set(satellite_price_symbols(universe)) | {"SPY", "SOXX", "CASH"}
    rows: list[dict[str, object]] = []
    start = pd.Timestamp("2025-09-01")
    for day_index in range((pd.Timestamp(RUN_DATE) - start).days + 1):
        current_date = (start + pd.Timedelta(days=day_index)).date()
        for symbol in sorted(symbols):
            base = 80.0 + (sum(ord(char) for char in symbol) % 29)
            slope = slopes.get(symbol, 0.22)
            if symbol == "CASH":
                base = 1.0
                slope = 0.0
            price = base + slope * day_index
            rows.append(
                {
                    "date": current_date.isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000 + day_index,
                    "source": "fixture",
                    "created_at": "2026-06-01T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _minimal_reader_payload(report: dict[str, object]) -> dict[str, object]:
    return {
        "as_of": RUN_DATE.isoformat(),
        "status": "OK",
        "production_effect": "none",
        "status_panel": {},
        "run_context": {},
        "narrative_executive_summary": {},
        "action_checklist": [],
        "executive_summary": {},
        "executive_decision": {},
        "market_situation_snapshot": {},
        "score_to_position_funnel": {"steps": []},
        "score_change_attribution_summary": {},
        "score_change_narrative": {},
        "report_index_summary": {},
        "missing_limited_artifact_impact": {},
        "task_cadence_calendar": {},
        "documentation_contract_summary": {},
        "contribution_summary": {},
        "component_score_explainability": {"components": []},
        "binding_gate_ladder": {"gates": []},
        "data_quality_pit_safety": {},
        "backtest_shadow_governance": {},
        "parameter_shadow_review": {},
        "etf_backtest_summary": {},
        "etf_calibration_experiments": {},
        "etf_forward_simulation": {},
        "etf_ai_confirmation": {},
        "etf_satellite_replacement": {
            "availability": "AVAILABLE",
            "status": "CANDIDATE_REPLACEMENT_AVAILABLE",
            "summary_sentence": "Satellite Replacement: candidate-only replacement available.",
            "eligible_stocks": ", ".join(str(item) for item in report["eligible_stocks"]),
            "watchlist": ", ".join(str(item) for item in report["watchlist"]),
            "fallback_to_etf": ", ".join(str(item) for item in report["fallback_to_etf_stocks"]),
            "proposed_candidate_replacement": "SMH -5.0%, NVDA +5.0%",
            "main_reason": "relative strength above benchmark ETF",
            "main_blocker": "none",
            "safety_status": "observe_only=true; candidate_only=true; production_effect=none",
            "detail_report": "",
            "production_effect": "none",
            "broker_action": "none",
        },
        "manual_review_queue": {"items": [], "groups": []},
        "report_navigation": [],
        "report_navigation_groups": {"groups": []},
        "appendix_links": [],
    }
