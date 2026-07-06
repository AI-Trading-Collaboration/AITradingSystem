from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as retest
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


def test_dynamic_strategy_component_attribution_targeted_ablation_retest_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "component_ablation"
    docs_root = tmp_path / "docs" / "research"

    payload = retest.run_dynamic_strategy_component_attribution_targeted_ablation_retest(
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
    assert payload["source_ready_for_ablation_retest"] is True
    assert payload["ablation_retest_run"] is True
    assert payload["ablation_retest_ready"] is True
    assert payload["component_attribution_matrix_ready"] is True
    assert payload["reusable_component_decision_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["primary_execution_cadence"] == retest.PRIMARY_EXECUTION_CADENCE
    assert set(payload["components_tested"]) == set(retest.COMPONENTS_TESTED)
    assert set(payload["ablation_candidates_tested"]) == set(retest.ABLATION_CANDIDATES)
    assert payload["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert payload["recommended_next_research_task"] == retest.NEXT_ROUTE
    assert payload["best_reusable_component"] not in (None, "TBD")

    matrix = {row["component_name"]: row for row in payload["component_attribution_matrix"]}
    assert set(retest.COMPONENTS_TESTED).issubset(matrix)
    assert matrix["turnover_budgeting"]["test_candidate"] == (
        "growth_tilt_plus_turnover_budget"
    )
    assert matrix["valid_until_strictness"]["test_candidate"] == (
        "growth_tilt_plus_valid_until_strict"
    )
    assert matrix["growth_tilt_engine"]["recommended_component_decision"] == (
        retest.COMPONENT_DECISION_REUSABLE
    )
    assert matrix["lower_turnover_guardrail"]["candidate_level_approval"] is False

    assert payload["candidate_auto_accept_approved"] is False
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
        "component_attribution_matrix_json",
        "reusable_component_decision_json",
        "decision_update_json",
        "markdown_path",
        "ablation_result_markdown",
        "reusable_component_decision_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_component_attribution_targeted_ablation_retest_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "component_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-component-attribution-targeted-ablation-retest",
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
    assert (output_root / "ablation_retest_result.json").exists()
    assert (output_root / "component_attribution_matrix.json").exists()
    assert (output_root / "reusable_component_decision.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_component_attribution_targeted_ablation_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_component_attribution_targeted_ablation_retest"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-component-attribution-targeted-ablation-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("ablation_retest_result.json" in item for item in entry["artifact_globs"])
    assert any(
        "component_attribution_matrix.json" in item for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_component_attribution_targeted_ablation_retest" in catalog
    assert "dynamic-strategy-component-attribution-targeted-ablation-retest" in (
        system_flow
    )
    assert (
        "TRADING-2393_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST"
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
    monkeypatch.setattr(retest, "_run_targeted_ablation_retest", _fake_retest)


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
        _row("static_baseline", 0.10, -0.18, 0.0, 0.20, 0, 0.20),
        _row(retest.RANKING_TOP_CANDIDATE, 0.17, -0.24, 0.07, 3.00, 4, 1.60),
        _row(retest.BASE_CANDIDATE_ID, 0.12, -0.16, 0.02, 0.90, 2, 0.70),
        _row(retest.BEST_LOWER_TURNOVER_VARIANT, 0.12, -0.17, 0.02, 1.00, 2, 0.80),
        _row(retest.BEST_GUARDED_VARIANT, 0.13, -0.15, 0.03, 0.85, 1, 0.75),
        _row("growth_tilt_only_reference", 0.16, -0.23, 0.06, 3.00, 4, 1.55),
        _row("growth_tilt_plus_turnover_budget", 0.155, -0.21, 0.055, 2.35, 4, 0.90),
        _row("growth_tilt_plus_valid_until_strict", 0.152, -0.205, 0.052, 2.80, 1, 1.05),
        _row(
            "growth_tilt_plus_turnover_budget_and_valid_until",
            0.153,
            -0.20,
            0.053,
            2.20,
            1,
            0.85,
        ),
        _row("lower_turnover_without_cooldown", 0.115, -0.155, 0.015, 0.88, 2, 0.70),
        _row(
            "lower_turnover_plus_growth_tilt_component",
            0.145,
            -0.145,
            0.045,
            0.95,
            1,
            0.80,
        ),
    ]
    cost_rows: list[dict[str, object]] = []
    for candidate_id in retest.ABLATION_CANDIDATES:
        cost_rows.extend(
            [
                _cost_row(candidate_id, "realistic", 0.040),
                _cost_row(candidate_id, "conservative", 0.025),
                _cost_row(candidate_id, "harsh", 0.005),
            ]
        )
    slice_rows = [
        {
            "candidate_id": candidate_id,
            "slice_id": f"slice_{index}",
            "slice_passed": index % 2 == 0,
        }
        for candidate_id in retest.ABLATION_CANDIDATES
        for index in range(4)
    ]
    return {
        "ablation_retest_result": full_rows,
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
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "performance_metrics": {
            "annualized_return": annualized_return,
            "max_drawdown": max_drawdown,
            "upside_capture": annualized_return / 0.10,
        },
        "relative_metrics": {
            "dynamic_vs_static_gap": dynamic_gap,
            "cost_adjusted_dynamic_vs_static_gap": dynamic_gap,
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
        "source_candidate_ranking_2365_path": paths["candidate_ranking_2365"],
        "source_sensitivity_result_2366_path": paths["sensitivity_result_2366"],
        "source_expanded_candidate_retest_2386_path": paths[
            "expanded_candidate_retest_2386"
        ],
        "source_expanded_candidate_ranking_2386_path": paths[
            "expanded_candidate_ranking_2386"
        ],
        "source_reclassification_result_2390_path": paths[
            "reclassification_result_2390"
        ],
        "source_owner_review_decision_2391_path": paths["owner_review_decision_2391"],
        "source_component_attribution_plan_2392_path": paths[
            "component_attribution_plan_2392"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "candidate_ranking_2365": "--source-candidate-ranking-2365",
        "sensitivity_result_2366": "--source-sensitivity-result-2366",
        "expanded_candidate_retest_2386": "--source-expanded-candidate-retest-2386",
        "expanded_candidate_ranking_2386": "--source-expanded-candidate-ranking-2386",
        "reclassification_result_2390": "--source-reclassification-result-2390",
        "owner_review_decision_2391": "--source-owner-review-decision-2391",
        "component_attribution_plan_2392": "--source-component-attribution-plan-2392",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    ranking_rows = _ranking_rows()
    payloads = {
        "candidate_ranking_2365": _source(
            retest.SOURCE_2365_READY_STATUS,
            candidate_ranking=[
                {
                    "candidate_id": retest.RANKING_TOP_CANDIDATE,
                    "decision": "OWNER_REVIEW_REQUIRED",
                }
            ],
        ),
        "sensitivity_result_2366": _source(
            retest.SOURCE_2366_READY_STATUS,
            top_candidate_from_2365=retest.RANKING_TOP_CANDIDATE,
        ),
        "expanded_candidate_retest_2386": _source(
            retest.SOURCE_2386_READY_STATUS,
            primary_execution_cadence=retest.PRIMARY_EXECUTION_CADENCE,
            data_quality_gate_executed=True,
            best_candidate_after_expanded_screening=retest.RANKING_TOP_CANDIDATE,
            expanded_candidate_ranking=ranking_rows,
        ),
        "expanded_candidate_ranking_2386": _source(
            retest.SOURCE_2386_READY_STATUS,
            expanded_candidate_ranking=ranking_rows,
        ),
        "reclassification_result_2390": _source(
            retest.SOURCE_2390_READY_STATUS,
            current_best_candidate=retest.RANKING_TOP_CANDIDATE,
        ),
        "owner_review_decision_2391": _source(
            retest.SOURCE_2391_READY_STATUS,
            owner_decision=retest.SOURCE_2391_OWNER_DECISION,
            research_only_observation_approved=False,
        ),
        "component_attribution_plan_2392": _source(
            retest.SOURCE_2392_READY_STATUS,
            recommended_next_research_task=retest.SOURCE_2392_EXPECTED_ROUTE,
            targeted_ablation_retest_plan_ready=True,
            components_to_attribute=list(retest.COMPONENTS_TESTED),
            targeted_ablation_retest_plan={
                "ablation_test_candidates": [
                    {"candidate_id": candidate_id}
                    for candidate_id in retest.ABLATION_CANDIDATES
                ],
            },
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


def _ranking_rows() -> list[dict[str, object]]:
    candidates = [
        retest.RANKING_TOP_CANDIDATE,
        retest.BASE_CANDIDATE_ID,
        retest.BEST_LOWER_TURNOVER_VARIANT,
        retest.BEST_GUARDED_VARIANT,
        "dynamic_turnover_budgeted_growth_tilt_v1",
        "dynamic_valid_until_expiry_strict_v1",
    ]
    return [
        {
            "candidate_id": candidate_id,
            "decision": "OWNER_REVIEW_REQUIRED",
            "dynamic_vs_static_gap": 0.02,
        }
        for candidate_id in candidates
    ]
