from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.contracts import CanonicalStatus, DataQualityEvidence
from ai_trading_system.research_framework import (
    ExperimentRunRequest,
    resolve_experiment_spec,
    run_experiment,
)
from ai_trading_system.research_framework.plugins.leveraged_exposure_instrument_evaluation import (
    BLOCKED_STATUS,
    ELIGIBLE_STATUS,
    build_evaluation_payload,
    leveraged_exposure_instrument_registry,
    render_evaluation_markdown,
    validate_evaluation_payload,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    PROJECT_ROOT / "config/research/experiments/leveraged_exposure_instrument_evaluation.yaml"
)
POLICY_PATH = PROJECT_ROOT / "config/research/strategy_style_discovery_universe_v1.yaml"
COST_POLICY_PATH = PROJECT_ROOT / "config/research/transaction_cost_model.yaml"
DATA_QUALITY_POLICY_PATH = PROJECT_ROOT / "config/data_quality.yaml"
AS_OF = date(2026, 7, 21)
GENERATED_AT = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


def test_content_derived_evaluation_is_deterministic_and_tamper_evident(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)

    first = build_evaluation_payload(sources, as_of=AS_OF)
    second = build_evaluation_payload(copy.deepcopy(sources), as_of=AS_OF)

    assert first == second
    assert first["status"] == ELIGIBLE_STATUS
    assert first["validation_status"] == "PASS"
    assert first["official_action_universe_changed"] is False
    assert first["official_primary_action_universe_changed"] is False
    assert first["role_limited_implementation_universe_changed"] is True
    assert first["qld_role_limited_2x_implementation_approved"] is True
    assert first["qld_automatic_execution_allowed"] is False
    assert first["next_route"] == (
        "govern_forward_shadow_and_non_automatic_implementation_selector"
    )
    role_decision = first["evaluation"]["owner_role_decision"]
    assert role_decision["independent_trend_model_required"] is True
    assert role_decision["trusted_nasdaq_uptrend_required"] is True
    assert role_decision["portfolio_target_qqq_equivalent_exposure_near_2x_required"] is True
    assert role_decision["risk_gate_pass_required"] is True
    assert role_decision["qld_as_trend_signal_allowed"] is False
    assert role_decision["automatic_instrument_selection_allowed"] is False
    assert validate_evaluation_payload(first, sources, as_of=AS_OF) == ()

    tampered = copy.deepcopy(first)
    tampered["evaluation"]["evaluated_range"]["common_price_sessions"] = 999
    assert validate_evaluation_payload(tampered, sources, as_of=AS_OF) == (
        "EVALUATION_CONTENT_MISMATCH",
    )


def test_prospective_source_row_fails_closed_before_cutoff_filter(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)
    package = copy.deepcopy(sources["instrument_panel_package"])
    panel_path = Path(package["panel"]["path"])
    panel = pd.read_csv(panel_path)
    prospective = panel.iloc[-1].copy()
    prospective["date"] = "2026-07-22"
    panel = pd.concat([panel, prospective.to_frame().T], ignore_index=True)
    panel.to_csv(panel_path, index=False, lineterminator="\n")
    package["panel"] = {
        "path": str(panel_path),
        "sha256": hashlib.sha256(panel_path.read_bytes()).hexdigest(),
        "size_bytes": panel_path.stat().st_size,
        "row_count": len(panel),
    }
    sources["instrument_panel_package"] = package

    payload = build_evaluation_payload(sources, as_of=AS_OF)

    assert payload["status"] == BLOCKED_STATUS
    assert payload["strict_validation_errors"] == ["PROSPECTIVE_VALUE_ENTERED_SOURCE_PANEL"]


def test_role_limited_implementation_policy_fails_closed_on_automatic_use(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)
    policy = copy.deepcopy(sources["universe_policy"])
    policy["role_limited_2x_implementation_policy"]["automatic_instrument_selection_allowed"] = True
    sources["universe_policy"] = policy

    payload = build_evaluation_payload(sources, as_of=AS_OF)

    assert payload["status"] == BLOCKED_STATUS
    assert payload["strict_validation_errors"] == ["ROLE_LIMITED_2X_IMPLEMENTATION_POLICY_INVALID"]


def test_generic_runner_attaches_required_data_quality_to_envelope_and_ledger(
    tmp_path: Path,
) -> None:
    sources, paths = _sources(tmp_path)
    result = run_experiment(
        resolved_spec=resolve_experiment_spec(SPEC_PATH),
        plugins=leveraged_exposure_instrument_registry(),
        request=ExperimentRunRequest(
            project_root=PROJECT_ROOT,
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of=AS_OF,
            input_overrides=paths,
            strict=True,
            generated_at=GENERATED_AT,
        ),
    )

    assert result.payload == {
        **result.payload,
        "status": ELIGIBLE_STATUS,
    }
    assert result.envelope.status is CanonicalStatus.PASS
    assert result.envelope.data_quality_required is True
    assert result.envelope.data_quality is not None
    assert result.envelope.data_quality.ready is True
    ledger_entry = result.ledger.entry("evaluate_and_render")
    assert ledger_entry.status is CanonicalStatus.PASS
    assert ledger_entry.data_quality == result.envelope.data_quality
    assert result.output_paths["reader_markdown"].read_text(encoding="utf-8") == (
        render_evaluation_markdown(result.payload)
    )


def test_experiment_contract_preserves_non_automatic_implementation_boundary() -> None:
    spec = resolve_experiment_spec(SPEC_PATH).value

    assert spec.data_quality_required is True
    assert spec.investment_facing_envelope is False
    assert spec.production_effect.value == "none"
    assert spec.broker_action == "none"
    assert spec.canonical_status(ELIGIBLE_STATUS) is CanonicalStatus.PASS


def test_spy_qld_roles_and_governed_report_surfaces_are_explicit() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)
    decision = safe_load_yaml_path(PROJECT_ROOT / "config/research/portfolio_decision_problem.yaml")
    protocol = safe_load_yaml_path(
        PROJECT_ROOT / "config/research/protocols/portfolio_decision_problem_v1.yaml"
    )
    registry = safe_load_yaml_path(PROJECT_ROOT / "config/report_registry.yaml")

    assert policy["universes"]["reference_and_regime_control"] == ["SPY", "QQQ"]
    assert policy["universes"]["role_limited_2x_implementation_instrument"] == ["QLD"]
    assert policy["universes"]["role_limited_implementation_universe_change_approved"] is True
    assert policy["universes"]["qld_signal_input_allowed"] is False
    assert policy["universes"]["official_action_universe_change_allowed"] is False
    assert decision["universe_roles"]["reference_and_regime_control"]["tickers"] == [
        "SPY",
        "QQQ",
    ]
    assert (
        decision["universe_roles"]["reference_and_regime_control"]["action_weight_eligible"]
        is False
    )
    role = decision["universe_roles"]["role_limited_implementation_instruments"]
    assert role["tickers"] == ["QLD"]
    assert role["conditional_execution_implementation_eligible"] is True
    assert role["direct_action_candidate_eligible"] is False
    assert role["signal_input_eligible"] is False
    assert role["automatic_instrument_selection_allowed"] is False
    assert (
        protocol["safety_boundary"]["qld_role_limited_2x_implementation_consideration_allowed"]
        is True
    )
    assert protocol["safety_boundary"]["qld_action_universe_change_allowed"] is False
    assert protocol["safety_boundary"]["qld_signal_use_allowed"] is False

    report = next(
        item
        for item in registry["reports"]
        if item["report_id"] == "leveraged_exposure_instrument_evaluation"
    )
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"
    assert report["required_for_daily_reading"] is False
    catalog = (PROJECT_ROOT / "docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = (PROJECT_ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    assert "leveraged_exposure_instrument_evaluation.v2" in catalog
    assert "TRADING-2459" in system_flow
    assert "Canonical full-cache DQ = FAIL" in system_flow


def _sources(tmp_path: Path) -> tuple[dict[str, object], dict[str, Path]]:
    policy = safe_load_yaml_path(POLICY_PATH)
    panel_path = tmp_path / "instrument_panel.csv"
    panel = _panel()
    panel.to_csv(panel_path, index=False, lineterminator="\n")
    rates_path = tmp_path / "rates_snapshot.csv"
    rates = pd.DataFrame(
        {
            "date": sorted(panel["date"].unique()),
            "series": "DGS3MO",
            "value": 5.0,
        }
    )
    rates.to_csv(rates_path, index=False, lineterminator="\n")
    quality_path = tmp_path / "data_quality_report.md"
    quality_path.write_text("# test quality\n", encoding="utf-8")
    quality_sha = hashlib.sha256(quality_path.read_bytes()).hexdigest()
    request_cache_path = tmp_path / "qld_request_cache.body"
    request_cache_path.write_bytes(b"test qld source response")
    evidence = DataQualityEvidence(
        contract_id="test_instrument_panel",
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v2",
        status="PASS",
        passed=True,
        checked_at=GENERATED_AT,
        as_of=AS_OF,
        report_path=str(quality_path),
        report_sha256=quality_sha,
        checked_input_count=2,
    )
    package = {
        "schema_version": "leveraged_exposure_instrument_panel.v1",
        "as_of": AS_OF.isoformat(),
        "requested_start": "2021-02-22",
        "historical_seen_end": AS_OF.isoformat(),
        "prospective_source_rows_observed": True,
        "prospective_values_used_in_evaluation": False,
        "scoped_data_quality_exception": policy["scoped_data_quality_exception"],
        "scoped_warning_resolution": policy["scoped_warning_resolution"],
        "canonical_full_cache_status": "FAIL",
        "canonical_full_cache_pass_claimed": False,
        "panel": {
            "path": str(panel_path),
            "sha256": hashlib.sha256(panel_path.read_bytes()).hexdigest(),
            "size_bytes": panel_path.stat().st_size,
            "row_count": len(panel),
        },
        "rates": {
            "path": str(rates_path),
            "sha256": hashlib.sha256(rates_path.read_bytes()).hexdigest(),
            "size_bytes": rates_path.stat().st_size,
            "row_count": len(rates),
        },
        "data_quality_report": {
            "path": str(quality_path),
            "sha256": quality_sha,
            "size_bytes": quality_path.stat().st_size,
        },
        "data_quality_evidence": evidence.to_dict(),
        "external_request_cache_commitments": [
            {
                "path": str(request_cache_path),
                "sha256": hashlib.sha256(request_cache_path.read_bytes()).hexdigest(),
                "size_bytes": request_cache_path.stat().st_size,
            }
        ],
    }
    requirement = tmp_path / "requirement.md"
    requirement.write_text("research-only; no action-universe mutation\n", encoding="utf-8")
    package_path = tmp_path / "instrument_panel_package.json"
    package_path.write_text(
        json.dumps(package, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    paths = {
        "universe_policy": POLICY_PATH,
        "instrument_panel_package": package_path,
        "transaction_cost_policy": COST_POLICY_PATH,
        "data_quality_policy": DATA_QUALITY_POLICY_PATH,
        "requirement_text": requirement,
    }
    return {
        "universe_policy": policy,
        "instrument_panel_package": package,
        "transaction_cost_policy": safe_load_yaml_path(COST_POLICY_PATH),
        "data_quality_policy": safe_load_yaml_path(DATA_QUALITY_POLICY_PATH),
        "requirement_text": requirement.read_text(encoding="utf-8"),
    }, paths


def _panel() -> pd.DataFrame:
    dates = pd.DatetimeIndex(
        [
            value
            for value in pd.date_range("2021-02-22", AS_OF.isoformat(), freq="D")
            if is_us_equity_trading_day(value.date())
        ]
    )
    qqq_returns = [
        0.0,
        *([0.01, -0.007, 0.006, -0.003, 0.008] * len(dates))[: len(dates) - 1],
    ]
    spy_returns = [value * 0.8 for value in qqq_returns]
    qld_returns = [value * 2.0 for value in qqq_returns]
    tqqq_returns = [value * 3.0 for value in qqq_returns]
    sgov_returns = [0.0, *([0.00005] * (len(dates) - 1))]
    rows: list[dict[str, object]] = []
    for ticker, returns in (
        ("SPY", spy_returns),
        ("QQQ", qqq_returns),
        ("SGOV", sgov_returns),
        ("QLD", qld_returns),
        ("TQQQ", tqqq_returns),
    ):
        price = 100.0
        for session, asset_return in zip(dates, returns, strict=True):
            price *= 1.0 + asset_return
            rows.append(
                {
                    "date": session.date().isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)
