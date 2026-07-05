from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    FUSION_CANDIDATES,
    NEXT_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_candidate_optimization_divergence_review,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ROBUSTNESS_TOP = "dynamic_regime_overlay_v0_4_lower_turnover"
SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2367_READY = (
    "DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY"
)
SOURCE_2374_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_CHECKPOINT_READY"
)


def test_dynamic_strategy_candidate_optimization_divergence_review_builder(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "optimization"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_candidate_optimization_divergence_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_cadence_matrix_path=source_paths["cadence_matrix"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_matrix_path=source_paths["sensitivity_matrix"],
        source_decision_update_path=source_paths["decision_update"],
        source_owner_review_gate_path=source_paths["owner_review_gate"],
        source_candidate_owner_review_comparison_path=source_paths[
            "candidate_owner_review_comparison"
        ],
        source_owner_reassessment_path=source_paths["owner_reassessment"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of,
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_optimization_review"] is True
    assert payload["data_quality_gate_executed"] is True
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["monthly_rebalance"]["allowed_for_primary_ranking"] is False
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == ROBUSTNESS_TOP
    assert payload["ranking_robustness_divergence_detected"] is True
    assert payload["optimization_review_ready"] is True
    assert payload["divergence_explanation_ready"] is True
    assert payload["fusion_candidates_generated"] is True
    assert payload["candidate_decision_update_ready"] is True
    assert payload["best_candidate_after_optimization"]
    assert payload["recommended_decision_after_optimization"] in {
        "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION",
        "OWNER_REVIEW_REQUIRED",
        "CONTINUE_OPTIMIZATION",
        "REJECT_FOR_NOW",
        "DEPRECATED_BY_DIVERGENCE_REVIEW",
    }
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    matrix = payload["optimization_matrix"]
    assert matrix
    assert {"base", "realistic", "conservative", "harsh"} <= {
        row["scenario_id"] for row in matrix
    }
    assert set(FUSION_CANDIDATES) <= {row["candidate_id"] for row in matrix}
    dynamic_row = next(
        row for row in matrix if row["candidate_id"] in FUSION_CANDIDATES
    )
    assert dynamic_row["execution_cadence"] == "valid_until_window"
    assert "cost_adjusted_return" in dynamic_row["cost_metrics"]
    assert "turnover" in dynamic_row["turnover_metrics"]
    assert "cooldown_block_count" in dynamic_row["cooldown_metrics"]

    decision = payload["candidate_decision_update"]
    assert decision["decision_update_ready"] is True
    assert decision["recommended_next_research_task"] == NEXT_ROUTE
    assert decision["monthly_rebalance_allowed_for_primary_ranking"] is False
    assert decision["paper_shadow_enabled"] is False
    assert {item["candidate_id"] for item in decision["candidate_decisions"]} >= {
        RANKING_TOP,
        ROBUSTNESS_TOP,
        *FUSION_CANDIDATES,
    }

    explanation = payload["divergence_explanation"]
    assert explanation["divergence_explanation_ready"] is True
    assert explanation["valid_until_window_remains_default"] is True
    assert explanation["paper_shadow_remains_disabled"] is True

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "outcome_binding_enabled",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "optimization_matrix_json",
        "candidate_decision_update_json",
        "markdown_path",
        "divergence_markdown",
        "optimization_matrix_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_candidate_optimization_divergence_review_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "optimization_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-candidate-optimization-divergence-review",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-cadence-matrix",
            str(source_paths["cadence_matrix"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-matrix",
            str(source_paths["sensitivity_matrix"]),
            "--source-decision-update",
            str(source_paths["decision_update"]),
            "--source-owner-review-gate",
            str(source_paths["owner_review_gate"]),
            "--source-candidate-owner-review-comparison",
            str(source_paths["candidate_owner_review_comparison"]),
            "--source-owner-reassessment",
            str(source_paths["owner_reassessment"]),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "divergence_review_result.json").exists()
    assert (output_root / "optimization_matrix.json").exists()
    assert (output_root / "candidate_decision_update.json").exists()


def test_dynamic_strategy_candidate_optimization_divergence_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_candidate_optimization_divergence_review"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-candidate-optimization-divergence-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("divergence_review_result.json" in item for item in entry["artifact_globs"])
    assert any("candidate_decision_update.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_candidate_optimization_divergence_review" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-candidate-optimization-divergence-review" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2375_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
        "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/TRADING-2375_Dynamic_Strategy_Candidate_Optimization_And_Ranking_Robustness_Divergence_Review.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "event_retest": _event_retest(),
        "candidate_ranking": _candidate_ranking_document(),
        "cadence_matrix": _cadence_matrix_document(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_matrix": _sensitivity_matrix_document(),
        "decision_update": _decision_update_document(),
        "owner_review_gate": _owner_review_gate(),
        "candidate_owner_review_comparison": _candidate_owner_review_comparison(),
        "owner_reassessment": _owner_reassessment(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = source_root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _candidate_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": RANKING_TOP,
            "rank": 1,
            "decision": "OWNER_REVIEW_REQUIRED",
            "cost_adjusted_return": 0.214462,
            "dynamic_vs_static_gap": 0.021905,
            "turnover": 1.964574,
            "rebalance_count": 65,
            "average_holding_days": 13.938,
            "max_drawdown": -0.183495,
            "false_risk_off_count": 15,
            "missed_upside_count": 174,
            "primary_execution_cadence": "valid_until_window",
        },
        {
            "candidate_id": ROBUSTNESS_TOP,
            "rank": 2,
            "decision": "CONTINUE_RESEARCH",
            "cost_adjusted_return": 0.195375,
            "dynamic_vs_static_gap": 0.002818,
            "turnover": 2.04,
            "rebalance_count": 19,
            "average_holding_days": 47.222,
            "max_drawdown": -0.122632,
            "false_risk_off_count": 3,
            "missed_upside_count": 33,
            "primary_execution_cadence": "valid_until_window",
        },
    ]


def _robustness_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": ROBUSTNESS_TOP,
            "robust_rank": 1,
            "source_rank_2365": 2,
            "robustness_score": 11,
            "realistic_cost_adjusted_return": 0.194762,
            "realistic_dynamic_vs_static_gap": 0.002205,
            "conservative_dynamic_vs_static_gap": 0.001524,
            "harsh_dynamic_vs_static_gap": 0.000843,
            "realistic_max_drawdown": -0.122866,
            "realistic_turnover": 2.04,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
            "turnover_acceptable": False,
            "decision_update": "OWNER_REVIEW_REQUIRED",
            "fragility_reason": "turnover 超出 sensitivity cap",
            "cooldown_fragility": "NOT_SEVERE",
        },
        {
            "candidate_id": RANKING_TOP,
            "robust_rank": 2,
            "source_rank_2365": 1,
            "robustness_score": 10,
            "realistic_cost_adjusted_return": 0.213859,
            "realistic_dynamic_vs_static_gap": 0.021302,
            "conservative_dynamic_vs_static_gap": 0.020633,
            "harsh_dynamic_vs_static_gap": 0.019964,
            "realistic_max_drawdown": -0.183642,
            "realistic_turnover": 1.964574,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
            "turnover_acceptable": False,
            "decision_update": "OWNER_REVIEW_REQUIRED",
            "fragility_reason": "turnover 超出 sensitivity cap；max drawdown 相对 static 明显恶化",
            "cooldown_fragility": "NOT_SEVERE",
        },
    ]


def _event_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "as_of": "2025-10-30",
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
        "cadence_comparison_matrix": [],
        "data_quality": {"status": "PASS_WITH_WARNINGS", "error_count": 0},
        "requested_date_range": {"start": "2022-12-01", "end": "2025-10-30"},
    }


def _candidate_ranking_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _cadence_matrix_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "cadence_comparison_matrix": [],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": _robustness_rows(),
        "decision_update": _decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_matrix_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "sensitivity_matrix": [],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": _decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _decision_update_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "source_top_candidate": RANKING_TOP,
        "top_candidate_after_sensitivity": ROBUSTNESS_TOP,
        "top_candidate_decision_after_sensitivity": "OWNER_REVIEW_REQUIRED",
        "robustness_ranking": _robustness_rows(),
        "recommended_next_research_task": (
            "TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate"
        ),
    }


def _owner_review_gate() -> dict[str, object]:
    return {
        "status": SOURCE_2367_READY,
        "primary_execution_cadence": "valid_until_window",
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": ROBUSTNESS_TOP,
        "ranking_robustness_divergence_detected": True,
        "candidate_review_comparison": _review_rows(),
        "recommended_gate_candidate": ROBUSTNESS_TOP,
        "recommended_gate_decision": "OWNER_REVIEW_REQUIRED",
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_owner_review_comparison() -> dict[str, object]:
    return {
        "status": SOURCE_2367_READY,
        "candidate_review_comparison": _review_rows(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _review_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": ROBUSTNESS_TOP,
            "roles": ["robustness_top_from_2366", "current_dynamic_default"],
            "ranking_rank_from_2365": 2,
            "robustness_rank": 1,
            "dynamic_vs_static_gap": 0.002205,
            "max_drawdown": -0.122866,
            "turnover": 2.04,
            "average_holding_days": 47.222,
            "fragility_reason": "turnover 超出 sensitivity cap",
        },
        {
            "candidate_id": RANKING_TOP,
            "roles": ["ranking_top_from_2365"],
            "ranking_rank_from_2365": 1,
            "robustness_rank": 2,
            "dynamic_vs_static_gap": 0.021302,
            "max_drawdown": -0.183642,
            "turnover": 1.964574,
            "average_holding_days": 13.938,
            "fragility_reason": "turnover 超出 sensitivity cap；max drawdown 相对 static 明显恶化",
        },
    ]


def _owner_reassessment() -> dict[str, object]:
    return {
        "status": SOURCE_2374_READY,
        "final_route": "OWNER_REASSESSMENT_REQUIRED_BEFORE_TRADING_2375",
        "continue_linear_observation_tasks": False,
        "trading_2375_auto_created": False,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": ROBUSTNESS_TOP,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "production_enabled": False,
        "production_approved": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_execution_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0007 + 0.0018 * math.sin(day_index / 19.0)
        if 80 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 190:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= max(0.01, 1.0 + qqq_return * 3.0 - 0.00025)
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)

    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(
            f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n"
        )
        rate_rows.append(
            f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n"
        )

    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result
