from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as retest
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "event_append_enabled",
    "event_append_approved",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "production_enabled",
    "broker_action_enabled",
    "daily_report_generated",
)


def test_dynamic_strategy_component_recombination_candidate_retest_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "recombination"
    docs_root = tmp_path / "docs" / "research"

    payload = retest.run_dynamic_strategy_component_recombination_candidate_retest(
        **_source_kwargs(source_paths),
        prices_path=tmp_path / "prices.parquet",
        marketstack_prices_path=tmp_path / "marketstack.parquet",
        rates_path=tmp_path / "rates.parquet",
        simple_config_path=tmp_path / "simple.yaml",
        policy_registry_path=tmp_path / "policies.yaml",
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
        start_date=date(2022, 12, 1),
    )

    assert payload["status"] == retest.READY_STATUS
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_passed"] is True
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_recombination_retest"] is True
    assert payload["primary_execution_cadence"] == retest.PRIMARY_EXECUTION_CADENCE
    assert set(payload["recombination_candidates_tested"]) == set(
        retest.RECOMBINATION_CANDIDATES
    )
    assert payload["reference_candidates"]
    assert payload["recombination_retest_ready"] is True
    assert payload["candidate_ranking_ready"] is True
    assert payload["component_evidence_matrix_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["best_recombination_candidate"] in retest.RECOMBINATION_CANDIDATES
    assert payload["best_recombination_decision"] in {
        retest.DECISION_OBSERVATION_PREVIEW,
        retest.DECISION_OWNER_REVIEW,
        retest.DECISION_CONTINUE_OPTIMIZATION,
        retest.DECISION_COMPONENT_VALUE_ONLY,
        retest.DECISION_REJECT,
    }
    assert payload["recommended_next_research_task"] == retest.NEXT_ROUTE

    ranking = payload["recombination_candidate_ranking"]
    assert len(ranking) == len(retest.RECOMBINATION_CANDIDATES)
    assert ranking[0]["rank"] == 1
    assert ranking[0]["decision"] == retest.DECISION_OBSERVATION_PREVIEW
    assert payload["decision_update"]["research_only_observation_preview_exists"] is True
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "candidate_ranking_json",
        "component_evidence_matrix_json",
        "decision_update_json",
        "markdown_path",
        "candidate_ranking_markdown",
        "component_evidence_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_component_recombination_candidate_retest_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "recombination_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-component-recombination-candidate-retest",
            *_source_args(source_paths),
            "--prices-path",
            str(tmp_path / "prices.parquet"),
            "--marketstack-prices-path",
            str(tmp_path / "marketstack.parquet"),
            "--rates-path",
            str(tmp_path / "rates.parquet"),
            "--simple-config-path",
            str(tmp_path / "simple.yaml"),
            "--policy-registry-path",
            str(tmp_path / "policies.yaml"),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert retest.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "recombination_retest_result.json").exists()
    assert (output_root / "recombination_candidate_ranking.json").exists()
    assert (output_root / "component_evidence_matrix.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_component_recombination_candidate_retest_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_component_recombination_candidate_retest"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-component-recombination-candidate-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("recombination_retest_result.json" in item for item in entry["artifact_globs"])
    assert any(
        "component_evidence_matrix.json" in item for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_component_recombination_candidate_retest" in catalog
    assert "dynamic-strategy-component-recombination-candidate-retest" in system_flow
    assert (
        "TRADING-2396_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST"
        in task_register
    )


def _patch_runtime(monkeypatch) -> None:
    monkeypatch.setattr(retest, "_load_registry", lambda path: {})
    monkeypatch.setattr(
        retest,
        "_data_quality_gate",
        lambda **kwargs: {
            "passed": True,
            "status": "PASS",
            "as_of": "2026-07-07",
            "checked_at": "2026-07-07T00:00:00+00:00",
            "report_path": "outputs/data_quality/cache_quality_report.json",
            "price_checksum": "fixture-price",
            "rate_checksum": "fixture-rate",
            "price_row_count": 120,
            "rate_row_count": 120,
            "issues": [],
        },
    )
    monkeypatch.setattr(retest, "_load_execution_price_matrix", _fake_prices)
    monkeypatch.setattr(retest, "_load_policy_registry", lambda path: {})
    monkeypatch.setattr(retest, "_policies_by_id", lambda registry: {})
    monkeypatch.setattr(retest, "_run_recombination_candidate_retest", _fake_retest)


def _fake_prices(*args, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "QQQ": [100.0, 101.0, 102.0],
            "TQQQ": [100.0, 103.0, 106.0],
            "SGOV": [100.0, 100.1, 100.2],
        },
        index=pd.to_datetime(["2022-12-01", "2022-12-02", "2022-12-05"]),
    )


def _fake_retest(*, prices: pd.DataFrame, policies: dict[str, object]) -> dict[str, object]:
    del prices, policies
    full_rows = [
        _row("static_baseline", 0.10, -0.18, 0.0, 0.20, 0, 0.20, 0.0, 0.0),
        _row(retest.m2386.RANKING_TOP_CANDIDATE, 0.17, -0.24, 0.07, 3.00, 4, 1.60, 1.0, 0.0),
        _row(retest.m2386.BASE_CANDIDATE_ID, 0.12, -0.16, 0.02, 0.90, 2, 0.70, 0.70, 0.70),
        _row(
            retest.m2386.BEST_LOWER_TURNOVER_VARIANT,
            0.12,
            -0.17,
            0.02,
            1.00,
            2,
            0.80,
            0.70,
            0.67,
        ),
        _row(retest.m2386.BEST_GUARDED_VARIANT, 0.13, -0.15, 0.03, 0.85, 1, 0.75, 0.76, 0.72),
    ]
    candidate_rows = [
        _row(
            "growth_tilt_lower_turnover_guarded_v1",
            0.158,
            -0.19,
            0.058,
            1.80,
            0,
            0.80,
            0.929,
            0.40,
        ),
        _row("growth_tilt_turnover_budgeted_v1", 0.150, -0.20, 0.050, 2.10, 0, 0.90, 0.882, 0.30),
        _row("growth_tilt_valid_until_strict_v1", 0.145, -0.20, 0.045, 2.40, 0, 1.00, 0.853, 0.20),
        _row(
            "growth_tilt_turnover_budgeted_valid_until_strict_v1",
            0.152,
            -0.195,
            0.052,
            2.00,
            0,
            0.85,
            0.894,
            0.33,
        ),
        _row(
            "growth_tilt_lower_turnover_guarded_transfer_v1",
            0.148,
            -0.18,
            0.048,
            1.30,
            0,
            0.70,
            0.871,
            0.57,
        ),
        _row(
            "growth_tilt_conservative_guarded_v1",
            0.125,
            -0.14,
            0.025,
            0.80,
            0,
            0.50,
            0.735,
            0.73,
        ),
    ]
    full_rows.extend(candidate_rows)
    cost_rows: list[dict[str, object]] = []
    for row in candidate_rows:
        candidate_id = str(row["candidate_id"])
        cost_rows.extend(
            [
                _cost_row(candidate_id, "realistic", 0.050),
                _cost_row(candidate_id, "conservative", 0.032),
                _cost_row(candidate_id, "harsh", 0.015),
            ]
        )
    slice_rows = [
        {
            "candidate_id": candidate_id,
            "slice_id": f"slice_{index}",
            "slice_passed": index < 3,
        }
        for candidate_id in retest.RECOMBINATION_CANDIDATES
        for index in range(4)
    ]
    return {
        "recombination_retest_result": full_rows,
        "reference_candidate_result": full_rows[:5],
        "time_slice_matrix": slice_rows,
        "regime_slice_matrix": slice_rows,
        "cost_stress_result": cost_rows,
        "cadence_comparison_result": [],
    }


def _row(
    candidate_id: str,
    annualized_return: float,
    max_drawdown: float,
    dynamic_gap: float,
    turnover: float,
    stale_count: int,
    max_monthly_turnover: float,
    retention: float,
    turnover_reduction: float,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "performance_metrics": {
            "total_return": annualized_return,
            "annualized_return": annualized_return,
            "max_drawdown": max_drawdown,
            "volatility": 0.20,
            "sharpe_or_sortino_if_available": annualized_return / 0.20,
            "downside_capture": 0.90,
            "upside_capture": annualized_return / 0.10,
        },
        "relative_metrics": {
            "dynamic_vs_static_gap": dynamic_gap,
            "cost_adjusted_dynamic_vs_static_gap": dynamic_gap,
            "candidate_vs_raw_growth_tilt_gap": annualized_return - 0.17,
            "candidate_vs_lower_turnover_gap": annualized_return - 0.12,
            "candidate_vs_guarded_turnover_gap": annualized_return - 0.13,
            "return_retention_vs_raw_growth_tilt": retention,
            "turnover_reduction_vs_raw_growth_tilt": turnover_reduction,
            "drawdown_gap_vs_static": 0.18 - abs(max_drawdown),
        },
        "execution_metrics": {
            "turnover": turnover,
            "rebalance_count": 6,
            "average_holding_days": 40,
            "max_monthly_turnover": max_monthly_turnover,
            "signal_to_execution_lag_days": stale_count / 2,
            "stale_signal_execution_count": stale_count,
            "cooldown_block_count": 1,
            "constraint_hit_count": 0,
        },
    }


def _cost_row(candidate_id: str, scenario_id: str, dynamic_gap: float) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "scenario_id": scenario_id,
        "performance_metrics": {
            "annualized_return": 0.10 + dynamic_gap,
            "max_drawdown": -0.20,
            "upside_capture": 1.0,
        },
        "relative_metrics": {
            "dynamic_vs_static_gap": dynamic_gap,
            "cost_adjusted_dynamic_vs_static_gap": dynamic_gap,
        },
        "execution_metrics": {
            "turnover": 1.0,
            "max_monthly_turnover": 0.8,
            "stale_signal_execution_count": 0,
        },
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_recombination_candidate_plan_2395_path": paths["plan_2395"],
        "source_candidate_definitions_2395_path": paths["definitions_2395"],
        "source_retest_plan_2396_path": paths["retest_plan_2396"],
        "source_acceptance_criteria_2395_path": paths["acceptance_2395"],
        "source_owner_review_decision_2394_path": paths["owner_2394"],
        "source_component_recombination_decision_2394_path": paths[
            "recombination_2394"
        ],
        "source_ablation_retest_result_2393_path": paths["ablation_2393"],
        "source_component_attribution_matrix_2393_path": paths["matrix_2393"],
        "source_expanded_candidate_retest_2386_path": paths["expanded_retest_2386"],
        "source_expanded_candidate_ranking_2386_path": paths["expanded_ranking_2386"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "plan_2395": "--source-recombination-candidate-plan-2395",
        "definitions_2395": "--source-candidate-definitions-2395",
        "retest_plan_2396": "--source-retest-plan-2396",
        "acceptance_2395": "--source-acceptance-criteria-2395",
        "owner_2394": "--source-owner-review-decision-2394",
        "recombination_2394": "--source-component-recombination-decision-2394",
        "ablation_2393": "--source-ablation-retest-result-2393",
        "matrix_2393": "--source-component-attribution-matrix-2393",
        "expanded_retest_2386": "--source-expanded-candidate-retest-2386",
        "expanded_ranking_2386": "--source-expanded-candidate-ranking-2386",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    ranking_rows = _expanded_ranking_rows()
    payloads = {
        "plan_2395": _source(
            retest.m2395.READY_STATUS,
            recommended_next_research_task=retest.m2395.NEXT_ROUTE,
            recombination_candidate_plan_ready=True,
            retest_plan_2396_ready=True,
            planned_recombination_candidates=list(retest.RECOMBINATION_CANDIDATES),
        ),
        "definitions_2395": _source(
            retest.m2395.READY_STATUS,
            recombination_candidate_definitions=[
                {"candidate_id": candidate_id, "components": ["growth_tilt_engine"]}
                for candidate_id in retest.RECOMBINATION_CANDIDATES
            ],
        ),
        "retest_plan_2396": _source(
            retest.m2395.READY_STATUS,
            retest_plan_2396={
                "next_task": retest.m2395.NEXT_ROUTE,
                "execution_cadence": {
                    "primary": retest.PRIMARY_EXECUTION_CADENCE,
                    "monthly_rebalance": {"allowed_for_primary_decision": False},
                },
                "planned_recombination_candidates": list(
                    retest.RECOMBINATION_CANDIDATES
                ),
            },
        ),
        "acceptance_2395": _source(
            retest.m2395.READY_STATUS,
            recombination_acceptance_criteria={"schema_version": "fixture.v1"},
        ),
        "owner_2394": _source(
            retest.m2394.READY_STATUS,
            owner_decision=retest.m2394.OWNER_DECISION,
            recombination_plan_approved=True,
            recommended_next_research_task=retest.m2394.NEXT_ROUTE,
        ),
        "recombination_2394": _source(
            retest.m2394.READY_STATUS,
            component_recombination_decision={"record_ready": True},
        ),
        "ablation_2393": _source(
            retest.m2393.READY_STATUS,
            data_quality_gate_executed=True,
            best_reusable_component=retest.m2395.RETURN_ENGINE_COMPONENT,
            recommended_next_research_task=retest.m2393.NEXT_ROUTE,
        ),
        "matrix_2393": _source(
            retest.m2393.READY_STATUS,
            component_attribution_matrix=[
                {
                    "component_name": retest.m2395.RETURN_ENGINE_COMPONENT,
                    "recommended_component_decision": retest.m2393.COMPONENT_DECISION_REUSABLE,
                },
                {
                    "component_name": retest.m2395.LOWER_TURNOVER_GUARDRAIL,
                    "recommended_component_decision": retest.m2393.COMPONENT_DECISION_GUARDRAIL,
                },
                {
                    "component_name": retest.m2395.GUARDED_TURNOVER_TRANSFER,
                    "recommended_component_decision": retest.m2393.COMPONENT_DECISION_OWNER_REVIEW,
                },
            ],
        ),
        "expanded_retest_2386": _source(
            retest.m2386.READY_STATUS,
            data_quality_gate_executed=True,
            primary_execution_cadence=retest.PRIMARY_EXECUTION_CADENCE,
        ),
        "expanded_ranking_2386": _source(
            retest.m2386.READY_STATUS,
            expanded_candidate_ranking=ranking_rows,
        ),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _source(status: str, **updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": status,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }
    payload.update(updates)
    return payload


def _expanded_ranking_rows() -> list[dict[str, object]]:
    candidates = [
        retest.m2386.RANKING_TOP_CANDIDATE,
        retest.m2386.BASE_CANDIDATE_ID,
        retest.m2386.BEST_LOWER_TURNOVER_VARIANT,
        retest.m2386.BEST_GUARDED_VARIANT,
    ]
    return [
        {
            "candidate_id": candidate_id,
            "decision": "OWNER_REVIEW_REQUIRED",
            "dynamic_vs_static_gap": 0.02,
        }
        for candidate_id in candidates
    ]
