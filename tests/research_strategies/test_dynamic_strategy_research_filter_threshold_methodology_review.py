from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RANKING_TOP = review.RANKING_TOP_CANDIDATE
LOWER_TURNOVER = review.BASE_CANDIDATE_ID
COOLDOWN_BALANCED = review.BEST_LOWER_TURNOVER_VARIANT
GUARDED_TURNOVER = "equal_risk_growth_tilt_guarded_turnover_v1"
TURNOVER_BUDGETED = "dynamic_turnover_budgeted_growth_tilt_v1"
VALID_UNTIL_STRICT = "dynamic_valid_until_expiry_strict_v1"


def test_dynamic_strategy_research_filter_threshold_methodology_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "threshold_review"
    docs_root = tmp_path / "docs" / "research"

    payload = review.run_dynamic_strategy_research_filter_threshold_methodology_review(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_tasks"] == list(review.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_threshold_methodology_review"] is True
    assert payload["threshold_methodology_review_ready"] is True
    assert payload["threshold_inventory_ready"] is True
    assert payload["gate_taxonomy_ready"] is True
    assert payload["candidate_threshold_outcome_matrix_ready"] is True
    assert payload["recommended_gate_policy_proposal_ready"] is True
    assert payload["research_only_vs_paper_shadow_gate_separated"] is True
    assert (
        payload["current_gate_may_be_too_strict_for_research_only_observation"]
        is True
    )
    assert payload["reference_candidate_policy_recommendation"] == (
        review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert payload["thresholds_requiring_statistical_calibration"] == list(
        review.THRESHOLDS_REQUIRING_STATISTICAL_CALIBRATION
    )
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE

    matrix = {
        row["candidate_id"]: row
        for row in payload["candidate_threshold_outcome_matrix"]
    }
    assert set(matrix) == set(review.REQUIRED_CANDIDATES)
    assert matrix[RANKING_TOP]["likely_reclassification_under_calibrated_gate"] == (
        "OWNER_REVIEW_REQUIRED"
    )
    assert matrix[RANKING_TOP]["reference_candidate"] is True
    assert matrix[TURNOVER_BUDGETED]["candidate_value_type"] == (
        "turnover_budget_component_value"
    )
    assert matrix[VALID_UNTIL_STRICT]["candidate_value_type"] == (
        "valid_until_component_value"
    )

    inventory = payload["threshold_inventory"]
    assert (
        inventory["slice_stability_thresholds"]["time_slice_pass_rate"][
            "current_threshold"
        ]
        == review.OBSERVATION_TIME_SLICE_PASS_RATE_MIN
    )
    assert (
        inventory["reference_candidate_thresholds"]["proposed_reference_policy"][
            "recommended_policy"
        ]
        == review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert payload["gate_taxonomy"]["research_only_observation_gate"][
        "owner_review_allowed"
    ] is True
    assert payload["recommended_gate_policy_proposal"]["policy_update_applied"] is False
    assert payload["recommended_gate_policy_proposal"]["rules_mutated"] is False

    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == review.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["observation_approved"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in review.SAFETY_FALSE_FIELDS:
        assert payload[key] is False
    for key in (
        "json_path",
        "threshold_inventory_json",
        "gate_taxonomy_json",
        "candidate_threshold_outcome_matrix_json",
        "recommended_gate_policy_proposal_json",
        "markdown_path",
        "threshold_inventory_markdown",
        "gate_taxonomy_markdown",
        "candidate_threshold_outcome_matrix_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_filter_threshold_methodology_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "threshold_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-filter-threshold-methodology-review",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-06",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert review.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "threshold_methodology_review_result.json").exists()
    assert (output_root / "threshold_inventory.json").exists()
    assert (output_root / "gate_taxonomy.json").exists()
    assert (output_root / "candidate_threshold_outcome_matrix.json").exists()
    assert (output_root / "recommended_gate_policy_proposal.json").exists()


def test_dynamic_strategy_research_filter_threshold_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[
        "dynamic_strategy_research_filter_threshold_methodology_review"
    ]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-filter-threshold-methodology-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "threshold_methodology_review_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("recommended_gate_policy_proposal.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_research_filter_threshold_methodology_review" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-research-filter-threshold-methodology-review" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2388_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_cadence_audit_path": paths["cadence_audit"],
        "source_event_retest_path": paths["event_retest"],
        "source_candidate_ranking_path": paths["candidate_ranking"],
        "source_sensitivity_result_path": paths["sensitivity_result"],
        "source_sensitivity_decision_update_path": paths[
            "sensitivity_decision_update"
        ],
        "source_divergence_review_path": paths["divergence_review"],
        "source_divergence_decision_update_path": paths[
            "divergence_decision_update"
        ],
        "source_targeted_retest_path": paths["targeted_retest"],
        "source_targeted_decision_update_path": paths["targeted_decision_update"],
        "source_variant_retest_path": paths["variant_retest"],
        "source_optimized_variant_ranking_path": paths["optimized_variant_ranking"],
        "source_variant_decision_update_path": paths["variant_decision_update"],
        "source_guarded_variant_retest_path": paths["guarded_variant_retest"],
        "source_guarded_variant_ranking_path": paths["guarded_variant_ranking"],
        "source_guarded_decision_update_path": paths["guarded_decision_update"],
        "source_expanded_candidate_retest_path": paths["expanded_candidate_retest"],
        "source_expanded_candidate_ranking_path": paths[
            "expanded_candidate_ranking"
        ],
        "source_expanded_decision_update_path": paths["expanded_decision_update"],
        "source_gate_calibration_review_path": paths["gate_calibration_review"],
        "source_gate_policy_review_path": paths["gate_policy_review"],
        "source_candidate_reclassification_preview_path": paths[
            "candidate_reclassification_preview"
        ],
        "source_recommended_policy_update_path": paths["recommended_policy_update"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "cadence_audit": "--source-cadence-audit",
        "event_retest": "--source-event-retest",
        "candidate_ranking": "--source-candidate-ranking",
        "sensitivity_result": "--source-sensitivity-result",
        "sensitivity_decision_update": "--source-sensitivity-decision-update",
        "divergence_review": "--source-divergence-review",
        "divergence_decision_update": "--source-divergence-decision-update",
        "targeted_retest": "--source-targeted-retest",
        "targeted_decision_update": "--source-targeted-decision-update",
        "variant_retest": "--source-variant-retest",
        "optimized_variant_ranking": "--source-optimized-variant-ranking",
        "variant_decision_update": "--source-variant-decision-update",
        "guarded_variant_retest": "--source-guarded-variant-retest",
        "guarded_variant_ranking": "--source-guarded-variant-ranking",
        "guarded_decision_update": "--source-guarded-decision-update",
        "expanded_candidate_retest": "--source-expanded-candidate-retest",
        "expanded_candidate_ranking": "--source-expanded-candidate-ranking",
        "expanded_decision_update": "--source-expanded-decision-update",
        "gate_calibration_review": "--source-gate-calibration-review",
        "gate_policy_review": "--source-gate-policy-review",
        "candidate_reclassification_preview": (
            "--source-candidate-reclassification-preview"
        ),
        "recommended_policy_update": "--source-recommended-policy-update",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "cadence_audit": _source(
            review.SOURCE_2364_READY_STATUS,
            data_quality_gate_executed=True,
        ),
        "event_retest": _source(
            review.SOURCE_2365_READY_STATUS,
            recommended_next_research_task=(
                "TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_"
                "Cooldown_Sensitivity_Analysis"
            ),
        ),
        "candidate_ranking": _source(
            review.SOURCE_2365_READY_STATUS,
            candidate_ranking=[
                {"candidate_id": RANKING_TOP, "rank": 1},
                {"candidate_id": LOWER_TURNOVER, "rank": 2},
            ],
        ),
        "sensitivity_result": _source(
            review.SOURCE_2366_READY_STATUS,
            sensitivity_matrix=[{"candidate_id": RANKING_TOP, "scenario_id": "base"}],
        ),
        "sensitivity_decision_update": _source(
            review.SOURCE_2366_READY_STATUS,
            decision_update={"decision_update_ready": True},
        ),
        "divergence_review": _source(
            review.SOURCE_2375_READY_STATUS,
            recommended_next_research_task=(
                "TRADING-2376_Dynamic_Strategy_Optimized_Candidate_"
                "Targeted_Retest"
            ),
        ),
        "divergence_decision_update": _source(review.SOURCE_2375_READY_STATUS),
        "targeted_retest": _source(
            review.SOURCE_2376_READY_STATUS,
            primary_candidate=LOWER_TURNOVER,
            candidate_decision_after_targeted_retest="CONTINUE_OPTIMIZATION",
        ),
        "targeted_decision_update": _source(review.SOURCE_2376_READY_STATUS),
        "variant_retest": _source(
            review.SOURCE_2379_READY_STATUS,
            best_variant_after_retest=COOLDOWN_BALANCED,
            best_variant_decision="CONTINUE_OPTIMIZATION",
            candidate_ready_for_research_only_observation=False,
        ),
        "optimized_variant_ranking": _source(review.SOURCE_2379_READY_STATUS),
        "variant_decision_update": _source(review.SOURCE_2379_READY_STATUS),
        "guarded_variant_retest": _source(
            review.SOURCE_2383_READY_STATUS,
            best_guarded_variant=GUARDED_TURNOVER,
            best_guarded_variant_decision="CONTINUE_OPTIMIZATION",
            candidate_ready_for_research_only_observation=False,
        ),
        "guarded_variant_ranking": _source(review.SOURCE_2383_READY_STATUS),
        "guarded_decision_update": _source(review.SOURCE_2383_READY_STATUS),
        "expanded_candidate_retest": _source(
            review.SOURCE_2386_READY_STATUS,
            best_candidate_after_expanded_screening=RANKING_TOP,
            best_candidate_decision="CONTINUE_OPTIMIZATION",
            candidate_ready_for_research_only_observation=False,
            expanded_candidate_ranking=_ranking_rows(),
        ),
        "expanded_candidate_ranking": _source(
            review.SOURCE_2386_READY_STATUS,
            expanded_candidate_ranking=_ranking_rows(),
        ),
        "expanded_decision_update": _source(
            review.SOURCE_2386_READY_STATUS,
            decision_update={
                "decision_update_ready": True,
                "best_candidate_after_expanded_screening": RANKING_TOP,
                "best_candidate_decision": "CONTINUE_OPTIMIZATION",
            },
        ),
        "gate_calibration_review": _source(
            review.SOURCE_2387_READY_STATUS,
            recommended_next_research_task=(
                "TRADING-2388_Dynamic_Strategy_Research_Filter_"
                "Threshold_Methodology_Review"
            ),
            observation_approved=False,
            policy_update_applied=False,
            reference_candidate_policy_recommendation=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            candidate_reclassification_preview=_preview_rows(),
        ),
        "gate_policy_review": _source(review.SOURCE_2387_READY_STATUS),
        "candidate_reclassification_preview": _source(
            review.SOURCE_2387_READY_STATUS,
            candidate_reclassification_preview=_preview_rows(),
        ),
        "recommended_policy_update": _source(
            review.SOURCE_2387_READY_STATUS,
            policy_update_applied=False,
            rules_mutated=False,
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
        **{field: False for field in review.SAFETY_FALSE_FIELDS},
    }
    payload.update(updates)
    return payload


def _ranking_rows() -> list[dict[str, object]]:
    return [
        _ranking_row(
            RANKING_TOP,
            rank=1,
            candidate_type="reference_candidate",
            static_gap=0.021302,
            guarded_gap=0.000682,
            time_slice=0.0,
            regime_slice=0.0,
            drawdown_gap=0.043574,
        ),
        _ranking_row(
            LOWER_TURNOVER,
            rank=2,
            candidate_type="reference_candidate",
            static_gap=0.002205,
            guarded_gap=-0.018415,
            time_slice=0.428571,
            regime_slice=0.0,
            drawdown_gap=-0.006,
        ),
        _ranking_row(
            COOLDOWN_BALANCED,
            rank=3,
            candidate_type="reference_candidate",
            static_gap=0.003,
            guarded_gap=-0.017,
            time_slice=0.428571,
            regime_slice=0.0,
            drawdown_gap=-0.004,
        ),
        _ranking_row(
            GUARDED_TURNOVER,
            rank=4,
            candidate_type="reference_candidate",
            static_gap=0.02062,
            guarded_gap=0.0,
            time_slice=0.0,
            regime_slice=0.0,
            drawdown_gap=0.036251,
        ),
        _ranking_row(
            TURNOVER_BUDGETED,
            rank=5,
            candidate_type="new_candidate",
            static_gap=0.006941,
            guarded_gap=-0.013679,
            time_slice=0.428571,
            regime_slice=0.0,
            drawdown_gap=-0.000389,
        ),
        _ranking_row(
            VALID_UNTIL_STRICT,
            rank=6,
            candidate_type="new_candidate",
            static_gap=0.007195,
            guarded_gap=-0.013425,
            time_slice=0.428571,
            regime_slice=0.0,
            drawdown_gap=-0.005479,
        ),
    ]


def _ranking_row(
    candidate_id: str,
    *,
    rank: int,
    candidate_type: str,
    static_gap: float,
    guarded_gap: float,
    time_slice: float,
    regime_slice: float,
    drawdown_gap: float,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "rank": rank,
        "candidate_type": candidate_type,
        "decision": "CONTINUE_OPTIMIZATION",
        "dynamic_vs_static_gap": static_gap,
        "realistic_cost_passed": True,
        "conservative_cost_passed": True,
        "harsh_cost_passed": True,
        "turnover_budget_passed": True,
        "time_slice_pass_rate": time_slice,
        "regime_slice_pass_rate": regime_slice,
        "drawdown_not_materially_worse": drawdown_gap <= review.DRAWDOWN_WORSE_TOLERANCE,
        "drawdown_gap_vs_static": drawdown_gap,
        "candidate_vs_guarded_ranking_top_gap": guarded_gap,
    }


def _preview_rows() -> dict[str, dict[str, object]]:
    return {
        RANKING_TOP: {
            "current_blockers": [
                "reference_candidate_hard_block",
                "drawdown_not_materially_worse=false",
            ],
            "preview_decision_under_calibrated_gate": "OWNER_REVIEW_REQUIRED",
            "auto_accept_allowed": False,
            "owner_review_allowed": True,
        },
        TURNOVER_BUDGETED: {
            "current_blockers": ["regime_slice_pass_rate_below_acceptance"],
            "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
            "auto_accept_allowed": False,
            "owner_review_allowed": False,
        },
        VALID_UNTIL_STRICT: {
            "current_blockers": ["regime_slice_pass_rate_below_acceptance"],
            "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
            "auto_accept_allowed": False,
            "owner_review_allowed": False,
        },
    }
