from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest as retest,
)
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


def test_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "targeted_retest"
    docs_root = tmp_path / "docs" / "research"

    payload = (
        retest.run_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest(
            **_source_kwargs(source_paths),
            prices_path=tmp_path / "prices.parquet",
            marketstack_prices_path=tmp_path / "marketstack.parquet",
            rates_path=tmp_path / "rates.parquet",
            simple_config_path=tmp_path / "simple.yaml",
            policy_registry_path=tmp_path / "policies.yaml",
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=date(2026, 7, 5),
            start_date=date(2022, 12, 1),
        )
    )

    assert payload["status"] == retest.READY_STATUS
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_passed"] is True
    assert payload["data_quality_gate_command"] == "aits validate-data --as-of 2026-07-05"
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_targeted_gate_evidence_retest"] is True
    assert payload["candidate_under_review"] == retest.CANDIDATE_UNDER_REVIEW
    assert payload["primary_execution_cadence"] == retest.PRIMARY_EXECUTION_CADENCE
    assert set(payload["targeted_variants_tested"]) == set(
        retest.TARGETED_VARIANT_IDS
    )
    assert payload["targeted_retest_ready"] is True
    assert payload["variant_ranking_ready"] is True
    assert payload["gate_evidence_matrix_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["best_targeted_variant"] in retest.TARGETED_VARIANT_IDS
    assert payload["best_targeted_variant_decision"] in {
        retest.DECISION_OBSERVATION_PREVIEW,
        retest.DECISION_OWNER_REVIEW,
        retest.DECISION_CONTINUE_TARGETED_IMPROVEMENT,
        retest.DECISION_COMPONENT_VALUE_ONLY,
        retest.DECISION_REJECT,
    }
    assert isinstance(payload["observation_preview_candidates_count"], int)
    assert payload["recommended_next_research_task"] == retest.NEXT_ROUTE

    ranking = payload["targeted_variant_ranking"]
    assert len(ranking) == len(retest.TARGETED_VARIANT_IDS)
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
        "targeted_variant_ranking_json",
        "gate_evidence_matrix_json",
        "decision_update_json",
        "markdown_path",
        "targeted_variant_ranking_markdown",
        "gate_evidence_matrix_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime(monkeypatch)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "targeted_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest",
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
            "2026-07-05",
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
    assert (output_root / "targeted_gate_evidence_retest_result.json").exists()
    assert (output_root / "targeted_variant_ranking.json").exists()
    assert (output_root / "gate_evidence_matrix.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_targeted_gate_evidence_retest_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[
        "dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest"
    ]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "targeted_gate_evidence_retest_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("gate_evidence_matrix.json" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest" in catalog
    assert (
        "dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest"
        in system_flow
    )
    assert retest.TASK_REGISTER_ID in task_register


def _patch_runtime(monkeypatch) -> None:
    monkeypatch.setattr(retest, "_load_registry", lambda path: {})
    monkeypatch.setattr(
        retest,
        "_data_quality_gate",
        lambda **kwargs: {
            "passed": True,
            "status": "PASS",
            "as_of": "2026-07-05",
            "checked_at": "2026-07-05T00:00:00+00:00",
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
    monkeypatch.setattr(retest, "_run_targeted_gate_evidence_retest", _fake_retest)


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
        _row(
            retest.m2396.m2386.RANKING_TOP_CANDIDATE,
            0.17,
            -0.24,
            0.07,
            2.50,
            3,
            1.60,
            1.0,
            0.0,
        ),
        _row(retest.CANDIDATE_UNDER_REVIEW, 0.14, -0.17, 0.04, 1.20, 0, 0.70, 0.82, 0.52),
        _row(
            retest.m2396.m2386.BEST_LOWER_TURNOVER_VARIANT,
            0.12,
            -0.16,
            0.02,
            0.80,
            0,
            0.50,
            0.70,
            0.68,
        ),
    ]
    for index, candidate_id in enumerate(retest.TARGETED_VARIANT_IDS):
        annual = 0.155 - index * 0.004
        drawdown = -0.16 - index * 0.005
        turnover = 1.00 + index * 0.08
        full_rows.append(
            _row(
                candidate_id,
                annual,
                drawdown,
                annual - 0.10,
                turnover,
                0,
                0.65,
                annual / 0.17,
                (2.50 - turnover) / 2.50,
                base_annual=0.14,
                static_drawdown=0.18,
                base_turnover=1.20,
                base_drawdown=0.17,
            )
        )
    cost_rows: list[dict[str, object]] = []
    for candidate_id in retest.TARGETED_VARIANT_IDS:
        cost_rows.extend(
            [
                _cost_row(candidate_id, "base", 0.060),
                _cost_row(candidate_id, "realistic", 0.050),
                _cost_row(candidate_id, "conservative", 0.030),
                _cost_row(candidate_id, "harsh", 0.010),
            ]
        )
    slice_rows = [
        {
            "candidate_id": candidate_id,
            "slice_id": f"slice_{index}",
            "slice_passed": index < 3,
        }
        for candidate_id in retest.TARGETED_VARIANT_IDS
        for index in range(4)
    ]
    return {
        "targeted_gate_evidence_retest_result": full_rows,
        "reference_candidate_result": full_rows[:4],
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
    *,
    base_annual: float = 0.14,
    static_drawdown: float = 0.18,
    base_turnover: float = 1.20,
    base_drawdown: float = 0.17,
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
            "candidate_vs_base_recombination_gap": annualized_return - base_annual,
            "return_retention_vs_raw_growth_tilt": retention,
            "return_retention_vs_base_recombination": annualized_return / base_annual,
            "turnover_reduction_vs_raw_growth_tilt": turnover_reduction,
            "turnover_change_vs_base_recombination": turnover - base_turnover,
            "drawdown_gap_vs_static": static_drawdown - abs(max_drawdown),
            "drawdown_improvement_vs_base": base_drawdown - abs(max_drawdown),
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
        "source_gate_evidence_plan_result_2398_path": paths["plan_2398"],
        "source_gate_evidence_gap_summary_2398_path": paths["gap_2398"],
        "source_targeted_improvement_plan_2398_path": paths["targeted_plan_2398"],
        "source_retest_plan_2399_2398_path": paths["retest_plan_2399"],
        "source_next_route_2398_path": paths["next_route_2398"],
        "source_owner_review_decision_2397_path": paths["owner_2397"],
        "source_recombination_retest_result_2396_path": paths["retest_2396"],
        "source_recombination_candidate_ranking_2396_path": paths["ranking_2396"],
        "source_component_evidence_matrix_2396_path": paths["evidence_2396"],
        "source_decision_update_2396_path": paths["decision_2396"],
        "source_recombination_candidate_plan_2395_path": paths["plan_2395"],
        "source_candidate_definitions_2395_path": paths["definitions_2395"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "plan_2398": "--source-gate-evidence-plan-result-2398",
        "gap_2398": "--source-gate-evidence-gap-summary-2398",
        "targeted_plan_2398": "--source-targeted-improvement-plan-2398",
        "retest_plan_2399": "--source-retest-plan-2399-2398",
        "next_route_2398": "--source-next-route-2398",
        "owner_2397": "--source-owner-review-decision-2397",
        "retest_2396": "--source-recombination-retest-result-2396",
        "ranking_2396": "--source-recombination-candidate-ranking-2396",
        "evidence_2396": "--source-component-evidence-matrix-2396",
        "decision_2396": "--source-decision-update-2396",
        "plan_2395": "--source-recombination-candidate-plan-2395",
        "definitions_2395": "--source-candidate-definitions-2395",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    best = retest.CANDIDATE_UNDER_REVIEW
    paths = {
        "plan_2398": root / "gate_evidence_plan_result.json",
        "gap_2398": root / "gate_evidence_gap_summary.json",
        "targeted_plan_2398": root / "targeted_improvement_plan.json",
        "retest_plan_2399": root / "retest_plan_2399.json",
        "next_route_2398": root / "next_route_2398.json",
        "owner_2397": root / "owner_review_decision_2397.json",
        "retest_2396": root / "recombination_retest_result.json",
        "ranking_2396": root / "recombination_candidate_ranking.json",
        "evidence_2396": root / "component_evidence_matrix.json",
        "decision_2396": root / "decision_update.json",
        "plan_2395": root / "recombination_candidate_plan.json",
        "definitions_2395": root / "candidate_definitions.json",
    }
    _write_json(
        paths["plan_2398"],
        {
            **_safe_doc(retest.m2398.READY_STATUS),
            "candidate_under_review": best,
            "decision_from_2396": retest.m2398.EXPECTED_DECISION_FROM_2396,
            "owner_decision_from_2397": retest.m2398.OWNER_DECISION_FROM_2397,
            "planned_targeted_variants": list(retest.TARGETED_VARIANT_IDS),
            "recommended_next_research_task": retest.m2398.NEXT_ROUTE,
            "gate_evidence_gap_summary": {"gap_areas": {"time_slice_evidence_gap": {}}},
        },
    )
    _write_json(paths["gap_2398"], {**_safe_doc(retest.m2398.READY_STATUS)})
    _write_json(
        paths["targeted_plan_2398"],
        {
            **_safe_doc(retest.m2398.READY_STATUS),
            "targeted_improvement_plan": {
                "record_ready": True,
                "targeted_variants": [
                    {"candidate_id": candidate_id}
                    for candidate_id in retest.TARGETED_VARIANT_IDS
                ],
            },
        },
    )
    _write_json(
        paths["retest_plan_2399"],
        {
            **_safe_doc(retest.m2398.READY_STATUS),
            "retest_plan_2399": {
                "record_ready": True,
                "primary_execution_cadence": retest.PRIMARY_EXECUTION_CADENCE,
                "monthly_rebalance": {"allowed_for_primary_decision": False},
                "required_2399_candidates": {
                    "targeted_variants": list(retest.TARGETED_VARIANT_IDS),
                    "reference": [best],
                },
                "recommended_next_research_task": retest.m2398.NEXT_ROUTE,
            },
        },
    )
    _write_json(
        paths["next_route_2398"],
        {
            **_safe_doc(retest.m2398.READY_STATUS),
            "recommended_next_research_task": retest.m2398.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["owner_2397"],
        {
            **_safe_doc(retest.m2397.READY_STATUS),
            "owner_decision": retest.m2398.OWNER_DECISION_FROM_2397,
            "best_recombination_candidate": best,
            "research_only_observation_approved": False,
        },
    )
    _write_json(
        paths["retest_2396"],
        {
            **_safe_doc(retest.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "best_recombination_decision": retest.m2398.EXPECTED_DECISION_FROM_2396,
            "data_quality_gate_executed": True,
        },
    )
    _write_json(
        paths["ranking_2396"],
        {
            **_safe_doc(retest.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "recombination_candidate_ranking": [
                {
                    "candidate_id": best,
                    "decision": retest.m2398.EXPECTED_DECISION_FROM_2396,
                }
            ],
        },
    )
    _write_json(
        paths["evidence_2396"],
        {
            **_safe_doc(retest.m2396.READY_STATUS),
            "component_evidence_matrix": [{"candidate_id": best}],
        },
    )
    _write_json(
        paths["decision_2396"],
        {
            **_safe_doc(retest.m2396.READY_STATUS),
            "decision_update": {"best_recombination_candidate": best},
        },
    )
    _write_json(
        paths["plan_2395"],
        {
            **_safe_doc(retest.m2395.READY_STATUS),
            "recommended_next_research_task": retest.m2395.NEXT_ROUTE,
            "planned_recombination_candidates": [best],
        },
    )
    _write_json(
        paths["definitions_2395"],
        {
            **_safe_doc(retest.m2395.READY_STATUS),
            "recombination_candidate_definitions": [{"candidate_id": best}],
        },
    )
    return paths


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in retest.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
