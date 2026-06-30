from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    run_candidate_generator_confidence_scaling_refinement_plan,
)
from ai_trading_system.first_layer_candidate_generator_runtime import (
    validate_candidate_generation_bundle,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    run_first_layer_candidate_generators_regenerate,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGenerationBundle,
    CandidateGeneratorContext,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    run_refined_candidate_actual_path_validation,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    run_refined_candidate_generators_regenerate,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    run_regenerated_candidate_actual_path_validation,
)
from ai_trading_system.regenerated_candidate_generator_common import (
    REGENERATED_CANDIDATE_FAMILY,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    run_regenerated_candidate_inconclusive_diagnostics,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)

REFINED_REVIEW_CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1",
    "risk_appetite_refined_confidence_v1",
    "volatility_regime_refined_confidence_v1",
)


def write_price_fixture(tmp_path: Path, *, include_vix: bool = True) -> Path:
    path = tmp_path / "prices_daily.csv"
    dates = pd.bdate_range("2022-12-01", "2023-02-15")
    rows: list[dict[str, object]] = []
    tickers = {
        "QQQ": 100.0,
        "SPY": 90.0,
        "SMH": 80.0,
        "TLT": 110.0,
    }
    if include_vix:
        tickers["^VIX"] = 20.0
    for index, current in enumerate(dates):
        for ticker, base in tickers.items():
            if ticker == "^VIX":
                close = base + (index % 6)
                adj_close = close
            elif ticker == "TLT":
                adj_close = base * (1.0 - (index * 0.0004))
                close = adj_close
            else:
                adj_close = base * (1.0 + (index * 0.002) + ((index % 5) * 0.0005))
                close = adj_close
            rows.append(
                {
                    "date": current.date().isoformat(),
                    "ticker": ticker,
                    "symbol": ticker,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": adj_close,
                    "volume": 1000,
                    "source": "pytest_fixture",
                    "updated_at": "2026-06-29T00:00:00Z",
                    "source_symbol": ticker,
                    "canonical_symbol": ticker,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def write_rates_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "rates_daily.csv"
    rows: list[dict[str, object]] = []
    for index, current in enumerate(pd.bdate_range("2022-12-01", "2023-02-15")):
        rows.append(
            {
                "date": current.date().isoformat(),
                "series": "DGS10",
                "value": 3.5 + index * 0.001,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_regenerated_artifact_fixture(tmp_path: Path) -> dict[str, Path]:
    price_path = write_price_fixture(tmp_path)
    rates_path = write_rates_fixture(tmp_path)
    output_dir = tmp_path / "regenerated"
    run_first_layer_candidate_generators_regenerate(
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        start_date=date(2023, 1, 3),
        end_date=date(2023, 1, 10),
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="regenerated_candidate_artifacts",
        prices_path=price_path,
        rates_path=rates_path,
        marketstack_prices_path=None,
    )
    return {
        "input_dir": output_dir,
        "prices_path": price_path,
        "rates_path": rates_path,
    }


def build_regenerated_actual_path_validation_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    output_dir = tmp_path / "actual_path_validation"
    run_regenerated_candidate_actual_path_validation(
        input_dir=fixture["input_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="actual_path_validation",
        prices_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        marketstack_prices_path=None,
        docs_root=tmp_path / "actual_path_docs",
    )
    return {
        **fixture,
        "validation_dir": output_dir,
        "generator_dir": fixture["input_dir"],
    }


def build_regenerated_inconclusive_diagnostics_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    output_dir = tmp_path / "inconclusive_diagnostics"
    run_regenerated_candidate_inconclusive_diagnostics(
        validation_dir=fixture["validation_dir"],
        generator_dir=fixture["generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="inconclusive_diagnostics",
        docs_root=tmp_path / "diagnostics_docs",
    )
    return {
        **fixture,
        "diagnostics_dir": output_dir,
    }


def build_confidence_scaling_refinement_plan_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    output_dir = tmp_path / "confidence_scaling_refinement_plan"
    run_candidate_generator_confidence_scaling_refinement_plan(
        diagnostics_dir=fixture["diagnostics_dir"],
        validation_dir=fixture["validation_dir"],
        generator_dir=fixture["generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refinement_plan",
        docs_root=tmp_path / "confidence_scaling_docs",
    )
    return {
        **fixture,
        "refinement_plan_dir": output_dir,
        "original_generator_dir": fixture["generator_dir"],
    }


def build_refined_candidate_regeneration_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    output_dir = tmp_path / "refined_candidate_generators_regenerated"
    run_refined_candidate_generators_regenerate(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refined_regeneration",
        docs_root=tmp_path / "refined_generation_docs",
    )
    return {
        **fixture,
        "refined_generator_dir": output_dir,
    }


def build_refined_candidate_actual_path_validation_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    output_dir = tmp_path / "refined_candidate_actual_path_validation"
    run_refined_candidate_actual_path_validation(
        refined_generator_dir=fixture["refined_generator_dir"],
        original_validation_dir=fixture["validation_dir"],
        refinement_plan_dir=fixture["refinement_plan_dir"],
        candidates=(
            "baseline_plus_trend_structure_refined_confidence_v1,"
            "risk_appetite_refined_confidence_v1,"
            "volatility_regime_refined_confidence_v1"
        ),
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refined_actual_path_validation",
        prices_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        marketstack_prices_path=None,
        docs_root=tmp_path / "refined_actual_path_docs",
    )
    return {
        **fixture,
        "refined_validation_dir": output_dir,
    }


def build_refined_scope_review_input_fixture(tmp_path: Path) -> dict[str, Path]:
    refined_validation_dir = tmp_path / "refined_validation"
    refined_generator_dir = tmp_path / "refined_generator"
    refinement_plan_dir = tmp_path / "refinement_plan"
    refined_validation_dir.mkdir(parents=True)
    refined_generator_dir.mkdir(parents=True)
    refinement_plan_dir.mkdir(parents=True)

    def safe_payload(**extra: object) -> dict[str, object]:
        return {
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "broker_action_recommendation_allowed": False,
            **extra,
        }

    candidates = list(REFINED_REVIEW_CANDIDATES)
    original_by_refined = {
        "baseline_plus_trend_structure_refined_confidence_v1": ("baseline_plus_trend_structure"),
        "risk_appetite_refined_confidence_v1": "risk_appetite",
        "volatility_regime_refined_confidence_v1": "volatility_regime",
    }
    scorecards = []
    state_rows = []
    data_quality_rows = []
    comparison_rows = []
    guardrail_rows = []
    high_rows = []
    false_cost_rows = []
    for refined_id in candidates:
        original_id = original_by_refined[refined_id]
        reject = "risk_appetite" in refined_id
        scorecards.append(
            safe_payload(
                refined_candidate_id=refined_id,
                candidate_id=refined_id,
                original_candidate_id=original_id,
                record_count=1200,
                validation_eligible_record_count=1200,
                alignment_rate=0.45 if not reject else 0.35,
                weighted_alignment_score=-0.05,
                confidence_weighted_alignment_score=-0.02 if not reject else -0.10,
                high_conviction_record_count=1100 if not reject else 0,
                high_conviction_eligible_record_count=1100 if not reject else 0,
                high_conviction_alignment_rate=0.58 if not reject else 0.0,
                high_conviction_confidence_weighted_alignment_score=0.12 if not reject else 0.0,
                false_risk_on_cost=1.0,
                false_risk_off_cost=1.0,
                guardrail_status="PASS_WITH_WARNINGS",
                recommended_research_status=(
                    "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
                    if reject
                    else "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
                ),
                owner_review_candidate_recommendation=False,
            )
        )
        state_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                original_candidate_id=original_id,
                recommended_research_status=(
                    "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
                    if reject
                    else "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
                ),
                guardrail_status="PASS_WITH_WARNINGS",
                data_quality_status="PASS_WITH_WARNINGS",
                comparison_label="REFINED_WORSE"
                if reject
                else "REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED",
                owner_review_candidate_recommendation=False,
            )
        )
        data_quality_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                candidate_id=refined_id,
                original_candidate_id=original_id,
                data_quality_status="PASS_WITH_WARNINGS",
                data_quality_error_count=0,
                data_quality_warning_count=1,
            )
        )
        comparison_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                original_candidate_id=original_id,
                comparison_label="REFINED_WORSE"
                if reject
                else "REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED",
                confidence_weighted_score_delta=-0.01 if not reject else -0.08,
                false_risk_on_cost_delta=-0.5,
                false_risk_off_cost_delta=-0.5,
                guardrail_status="PASS_WITH_WARNINGS",
            )
        )
        guardrail_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                original_candidate_id=original_id,
                guardrail_status="PASS_WITH_WARNINGS",
                validation_status="PASS",
            )
        )
        high_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                original_candidate_id=original_id,
                target_asset="QQQ",
                horizon="5d",
                high_conviction_eligible_count=1100 if not reject else 0,
                high_conviction_alignment_rate=0.58 if not reject else 0.0,
                non_high_conviction_alignment_rate=0.40,
                high_vs_non_high_alignment_delta=0.18 if not reject else 0.0,
                high_conviction_false_risk_on_cost=1.0,
                high_conviction_false_risk_off_cost=1.0,
            )
        )
        false_cost_rows.append(
            safe_payload(
                refined_candidate_id=refined_id,
                original_candidate_id=original_id,
                target_asset="QQQ",
                horizon="5d",
                eligible_record_count=1200,
                false_risk_on_cost=1.0,
                false_risk_off_cost=1.0,
            )
        )

    def write_json_fixture(path: Path, payload: dict[str, object]) -> None:
        path.write_text(json.dumps(payload), encoding="utf-8")

    write_json_fixture(
        refined_validation_dir / "refined_candidate_actual_path_validation_summary.json",
        safe_payload(
            summary={
                "actual_path_record_count": 3600,
                "validation_eligible_record_count": 3600,
                "data_quality_status": "PASS_WITH_WARNINGS",
            }
        ),
    )
    write_json_fixture(
        refined_validation_dir / "refined_candidate_validation_scorecard.json",
        safe_payload(candidate_scorecards=scorecards),
    )
    write_json_fixture(
        refined_validation_dir / "refined_high_conviction_outcome_drilldown.json",
        safe_payload(rows=high_rows),
    )
    write_json_fixture(
        refined_validation_dir / "refined_false_signal_cost_matrix.json",
        safe_payload(rows=false_cost_rows),
    )
    write_json_fixture(
        refined_validation_dir / "refined_guardrail_validation_matrix.json",
        safe_payload(rows=guardrail_rows),
    )
    write_json_fixture(
        refined_validation_dir / "original_vs_refined_actual_path_comparison.json",
        safe_payload(rows=comparison_rows),
    )
    write_json_fixture(
        refined_validation_dir / "refined_candidate_state_recommendation_matrix.json",
        safe_payload(candidate_rows=state_rows),
    )
    write_json_fixture(
        refined_validation_dir / "refined_candidate_data_quality_report.json",
        safe_payload(candidate_rows=data_quality_rows),
    )
    pd.DataFrame(
        [
            safe_payload(
                record_id=f"{refined_id}|QQQ|5d|{index}",
                refined_candidate_id=refined_id,
                original_candidate_id=original_by_refined[refined_id],
                candidate_id=refined_id,
                target_asset="QQQ",
                decision_timestamp="2023-01-03T21:00:00+00:00",
                horizon="5d",
                signal_name="pytest_signal",
                signal_direction="trend_confirming"
                if "baseline" in refined_id
                else "risk_off"
                if "volatility" in refined_id
                else "risk_on",
                signal_value=0.8,
                signal_confidence=0.8,
                refined_signal_value=0.8,
                refined_signal_confidence=0.8,
                high_conviction_flag="risk_appetite" not in refined_id,
                actual_path_status="complete",
                validation_eligible=True,
                data_quality_warning=False,
                actual_forward_return=0.02,
                actual_max_drawdown=-0.01,
                actual_max_runup=0.02,
                actual_realized_volatility=0.15,
                alignment_label="aligned",
                alignment_score=1.0 if "risk_appetite" not in refined_id else -1.0,
                error_type="none" if "risk_appetite" not in refined_id else "false_risk_on",
                dominant_observed_driver="sharp_rebound",
                promotion_eligible=False,
                permanently_inconclusive_override_allowed=False,
            )
            for refined_id in candidates
            for index in range(3)
        ]
    ).to_csv(
        refined_validation_dir / "refined_candidate_prediction_outcome_matrix.csv", index=False
    )

    for filename in (
        "refined_regeneration_run_summary.json",
        "refined_regeneration_validation_summary.json",
        "refined_original_vs_refined_delta_summary.json",
    ):
        write_json_fixture(refined_generator_dir / filename, safe_payload(rows=[]))
    for refined_id in candidates:
        candidate_dir = refined_generator_dir / refined_id
        candidate_dir.mkdir()
        for filename in (
            "refined_candidate_signal_spec.json",
            "refined_candidate_prediction_artifact.json",
            "refined_generation_summary.json",
            "refined_validation_summary.json",
            "refined_parameter_application_report.json",
            "refined_original_vs_refined_delta.json",
        ):
            write_json_fixture(
                candidate_dir / filename, safe_payload(refined_candidate_id=refined_id)
            )
        pd.DataFrame([{"refined_candidate_id": refined_id}]).to_csv(
            candidate_dir / "refined_candidate_signal_series.csv",
            index=False,
        )

    for filename in (
        "confidence_scaling_refinement_summary.json",
        "candidate_confidence_scaling_proposal_matrix.json",
        "candidate_confidence_scaling_parameter_grid.json",
        "candidate_guardrail_matrix.json",
        "candidate_expected_risk_impact_matrix.json",
        "candidate_2288_implementation_plan.json",
    ):
        write_json_fixture(refinement_plan_dir / filename, safe_payload(rows=[]))

    return {
        "refined_validation_dir": refined_validation_dir,
        "refined_generator_dir": refined_generator_dir,
        "refinement_plan_dir": refinement_plan_dir,
    }


def build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path: Path) -> dict[str, Path]:
    scope_review_dir = tmp_path / "scope_review"
    refined_generator_dir = tmp_path / "refined_generator"
    refined_validation_dir = tmp_path / "refined_validation"
    scope_review_dir.mkdir(parents=True)
    refined_generator_dir.mkdir(parents=True)
    refined_validation_dir.mkdir(parents=True)

    def safe_payload(**extra: object) -> dict[str, object]:
        return {
            "artifact_role": "pytest_fixture",
            "promotion_eligible": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "owner_review_required": False,
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "broker_action_recommendation_allowed": False,
            **extra,
        }

    def write_json_fixture(path: Path, payload: dict[str, object]) -> None:
        path.write_text(json.dumps(payload), encoding="utf-8")

    baseline = "baseline_plus_trend_structure_refined_confidence_v1"
    volatility = "volatility_regime_refined_confidence_v1"
    risk = "risk_appetite_refined_confidence_v1"
    scope_rows = [
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            recommended_scope_action="SCOPE_NARROW_AND_REGENERATE",
            usage_recommendation="confirmation_only",
            candidate_status_after_2289="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            scope_review_status="LOCAL_EDGE_PRESENT",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_KEEP_ONLY",
            kept_assets=[],
            dropped_assets=["QQQ", "SPY", "SMH"],
            kept_horizons=["5d"],
            dropped_horizons=["10d", "20d"],
            kept_directions=["risk_on", "risk_off", "trend_confirming", "trend_weakening"],
            dropped_directions=["neutral"],
            kept_regimes=["uptrend"],
            next_task_recommendation="TRADING-2291_Scope_Narrowed_Candidate_Regeneration",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            recommended_scope_action="SCOPE_NARROW_AND_REGENERATE",
            usage_recommendation="risk_cap_only",
            candidate_status_after_2289="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            scope_review_status="LOCAL_EDGE_WEAK",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_KEEP_ONLY",
            kept_assets=["SPY"],
            dropped_assets=["QQQ", "SMH"],
            kept_horizons=["5d"],
            dropped_horizons=["10d", "20d"],
            kept_directions=["risk_off", "volatility_compression", "volatility_expansion"],
            dropped_directions=["neutral"],
            kept_regimes=["high_volatility"],
            next_task_recommendation="TRADING-2291_Scope_Narrowed_Candidate_Regeneration",
        ),
        safe_payload(
            refined_candidate_id=risk,
            original_candidate_id="risk_appetite",
            recommended_scope_action="REJECT_CURRENT_FORM",
            usage_recommendation="reject",
            candidate_status_after_2289="REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED",
            scope_review_status="LOCAL_EDGE_NOT_FOUND",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_INCONCLUSIVE",
            kept_assets=["QQQ"],
            dropped_assets=["SMH"],
            kept_horizons=["5d"],
            dropped_horizons=[],
            kept_directions=["risk_on"],
            dropped_directions=[],
            kept_regimes=[],
            next_task_recommendation="TRADING-2291_Archive_Rejected_Candidate_Current_Form",
        ),
    ]
    direction_rows = [
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            signal_direction="trend_confirming",
            direction_scope_label="DIRECTION_CONFIRMATION_ONLY",
        ),
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            signal_direction="risk_on",
            direction_scope_label="DIRECTION_KEEP",
        ),
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            signal_direction="neutral",
            direction_scope_label="DIRECTION_DROP",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            signal_direction="volatility_expansion",
            direction_scope_label="DIRECTION_RISK_CAP_ONLY",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            signal_direction="risk_off",
            direction_scope_label="DIRECTION_RISK_CAP_ONLY",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            signal_direction="volatility_compression",
            direction_scope_label="DIRECTION_KEEP",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            signal_direction="risk_on",
            direction_scope_label="DIRECTION_DROP",
        ),
    ]
    high_rows = [
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_KEEP_ONLY",
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_KEEP_ONLY",
        ),
        safe_payload(
            refined_candidate_id=risk,
            original_candidate_id="risk_appetite",
            high_conviction_scope_label="HIGH_CONVICTION_SCOPE_INCONCLUSIVE",
        ),
    ]
    false_cost_rows = []
    for refined_id, original_id, directions in [
        (baseline, "baseline_plus_trend_structure", ["trend_confirming", "risk_on", "neutral"]),
        (
            volatility,
            "volatility_regime",
            ["volatility_expansion", "risk_off", "volatility_compression", "risk_on"],
        ),
    ]:
        for dimension, values in {
            "asset": ["QQQ", "SPY", "SMH"],
            "horizon": ["5d", "10d", "20d"],
            "direction": directions,
            "high_conviction": ["high_conviction_only"],
        }.items():
            for value in values:
                false_cost_rows.append(
                    safe_payload(
                        refined_candidate_id=refined_id,
                        original_candidate_id=original_id,
                        scope_dimension=dimension,
                        scope_value=value,
                        false_cost_label="FALSE_COST_ACCEPTABLE",
                    )
                )
    for filename, payload in {
        "local_edge_scope_review_summary.json": safe_payload(summary={}),
        "candidate_scope_narrowing_recommendation_matrix.json": safe_payload(rows=scope_rows),
        "candidate_direction_scope_matrix.json": safe_payload(rows=direction_rows),
        "candidate_high_conviction_scope_matrix.json": safe_payload(rows=high_rows),
        "candidate_false_cost_scope_matrix.json": safe_payload(rows=false_cost_rows),
        "risk_appetite_reject_record.json": safe_payload(
            refined_candidate_id=risk,
            original_candidate_id="risk_appetite",
            archive_scope="current_form",
        ),
        "candidate_next_task_recommendation_matrix.json": safe_payload(rows=[]),
        "candidate_scope_review_decision_summary.json": safe_payload(rows=scope_rows),
    }.items():
        write_json_fixture(scope_review_dir / filename, payload)

    state_rows = [
        safe_payload(
            refined_candidate_id=baseline,
            original_candidate_id="baseline_plus_trend_structure",
            recommended_research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            owner_review_candidate_recommendation=False,
        ),
        safe_payload(
            refined_candidate_id=volatility,
            original_candidate_id="volatility_regime",
            recommended_research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            owner_review_candidate_recommendation=False,
        ),
        safe_payload(
            refined_candidate_id=risk,
            original_candidate_id="risk_appetite",
            recommended_research_status="REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED",
            owner_review_candidate_recommendation=False,
        ),
    ]
    for filename, payload in {
        "refined_candidate_actual_path_validation_summary.json": safe_payload(summary={}),
        "refined_candidate_validation_scorecard.json": safe_payload(candidate_scorecards=[]),
        "refined_high_conviction_outcome_drilldown.json": safe_payload(rows=[]),
        "refined_false_signal_cost_matrix.json": safe_payload(rows=[]),
        "refined_guardrail_validation_matrix.json": safe_payload(rows=[]),
        "original_vs_refined_actual_path_comparison.json": safe_payload(rows=[]),
        "refined_candidate_state_recommendation_matrix.json": safe_payload(
            candidate_rows=state_rows
        ),
        "refined_candidate_data_quality_report.json": safe_payload(candidate_rows=[]),
    }.items():
        write_json_fixture(refined_validation_dir / filename, payload)

    for filename in (
        "refined_regeneration_run_summary.json",
        "refined_regeneration_validation_summary.json",
        "refined_original_vs_refined_delta_summary.json",
    ):
        write_json_fixture(refined_generator_dir / filename, safe_payload(rows=[]))

    def provenance() -> dict[str, object]:
        return {
            "source_paths": ["pytest_source.csv"],
            "source_hashes": ["pytest_hash"],
            "regeneration_mode": "deterministic_refined_regeneration",
            "pit_policy": "strict_pit",
            "candidate_binding_method": "native_candidate_id",
            "source_schema_status": "candidate_bound",
            "promotion_eligible": False,
        }

    def make_records(refined_id: str, original_id: str) -> list[dict[str, object]]:
        directions = (
            ["trend_confirming", "neutral", "risk_on", "trend_confirming"]
            if "baseline" in refined_id
            else ["volatility_expansion", "volatility_compression", "risk_on", "risk_off"]
        )
        assets = (
            ["QQQ", "QQQ", "QQQ", "SPY"]
            if "baseline" in refined_id
            else ["SPY", "SPY", "SPY", "SPY"]
        )
        highs = [True, True, False, True]
        rows = []
        for index, direction in enumerate(directions):
            rows.append(
                {
                    "candidate_id": refined_id,
                    "candidate_family": "first_layer_executable_candidate",
                    "source_experiment_id": f"{refined_id}_generator",
                    "source_artifact_id": refined_id,
                    "source_artifact_path": "pytest_source.csv",
                    "source_artifact_hash": f"{refined_id}_hash",
                    "signal_spec_version": "first_layer_candidate_signal_spec.v1",
                    "prediction_schema_version": "candidate_bound_prediction_artifact.v1",
                    "generated_at": "2026-06-30T00:00:00+00:00",
                    "as_of_timestamp": "2023-01-03T21:00:00+00:00",
                    "decision_timestamp": "2023-01-04T21:00:00+00:00",
                    "target_asset": assets[index],
                    "horizon": "5d" if index != 1 else "10d",
                    "signal_name": f"{direction}_score",
                    "signal_value": (
                        0.6
                        if direction not in {"risk_off", "trend_weakening"}
                        else -0.6
                    ),
                    "signal_direction": direction,
                    "signal_confidence": 0.8,
                    "valid_from": "2023-01-04T21:00:00+00:00",
                    "valid_until": "2023-01-09T21:00:00+00:00",
                    "input_snapshot_hash": "input_hash",
                    "feature_snapshot_hash": "feature_hash",
                    "model_or_rule_version": f"{original_id}.refined.v1",
                    "provenance": provenance(),
                    "promotion_eligible": False,
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    "permanently_inconclusive_override_allowed": False,
                    "source_row_index": index,
                    "source_date": "2023-01-03",
                    "source_trend_state": direction,
                    "source_confidence": 0.8,
                    "source_prediction_flags": {"high_conviction_flag": highs[index]},
                    "original_candidate_id": original_id,
                    "refined_candidate_id": refined_id,
                    "refinement_source_task": "TRADING-2287",
                    "refinement_task_id": (
                        "TRADING-2288_REFINED_CANDIDATE_REGENERATION_WITH_ADJUSTED_CONFIDENCE_SCALING"
                    ),
                    "refinement_version": "refined_confidence_v1",
                    "high_conviction_flag": highs[index],
                    "refined_signal_value": 0.6,
                    "refined_signal_confidence": 0.8,
                    "actual_path_validation_ready": False,
                    "actual_path_validation_executed": False,
                }
            )
        return rows

    def write_refined_candidate(refined_id: str, original_id: str) -> None:
        candidate_dir = refined_generator_dir / refined_id
        candidate_dir.mkdir()
        records = make_records(refined_id, original_id)
        signal_spec = safe_payload(
            schema_version="first_layer_candidate_signal_spec.v1",
            candidate_id=refined_id,
            candidate_family="first_layer_executable_candidate",
            generator_id=f"{refined_id}_generator",
            signal_spec_version="first_layer_candidate_signal_spec.v1",
            prediction_schema_version="candidate_bound_prediction_artifact.v1",
            target_asset="QQQ,SPY,SMH",
            supported_horizons=["5d", "10d", "20d"],
            output_signal_names=["pytest_signal"],
            required_inputs=["pytest_input"],
            signal_direction_mapping={"pytest": "risk_on"},
            pit_policy="strict_pit",
            original_candidate_id=original_id,
            refined_candidate_id=refined_id,
        )
        artifact = safe_payload(
            schema_version="candidate_bound_prediction_artifact.v1",
            artifact_id=f"{refined_id}_prediction_artifact",
            artifact_role="refined_regenerated_executable_candidate_artifact",
            candidate_id=refined_id,
            candidate_family="first_layer_executable_candidate",
            original_candidate_id=original_id,
            refined_candidate_id=refined_id,
            source_experiment_id=f"{refined_id}_generator",
            source_artifact_id=refined_id,
            source_artifact_path="pytest_source.csv",
            source_artifact_hash=f"{refined_id}_hash",
            signal_spec_version="first_layer_candidate_signal_spec.v1",
            prediction_schema_version="candidate_bound_prediction_artifact.v1",
            generated_at="2026-06-30T00:00:00+00:00",
            as_of_timestamp="2023-01-03T21:00:00+00:00",
            decision_timestamp="2023-01-04T21:00:00+00:00",
            target_asset=records[-1]["target_asset"],
            horizon=records[-1]["horizon"],
            signal_name=records[-1]["signal_name"],
            signal_value=records[-1]["signal_value"],
            signal_direction=records[-1]["signal_direction"],
            signal_confidence=records[-1]["signal_confidence"],
            valid_from="2023-01-04T21:00:00+00:00",
            valid_until="2023-01-09T21:00:00+00:00",
            input_snapshot_hash="input_hash",
            feature_snapshot_hash="feature_hash",
            model_or_rule_version=f"{original_id}.refined.v1",
            provenance=provenance(),
            prediction_records=records,
            historical_executable_artifact=True,
            actual_path_validation_ready=False,
            actual_path_validation_executed=False,
            permanently_inconclusive_override_allowed=False,
        )
        write_json_fixture(candidate_dir / "refined_candidate_signal_spec.json", signal_spec)
        write_json_fixture(candidate_dir / "refined_candidate_prediction_artifact.json", artifact)
        write_json_fixture(candidate_dir / "refined_generation_summary.json", safe_payload())
        write_json_fixture(
            candidate_dir / "refined_validation_summary.json",
            safe_payload(status="PASS"),
        )
        write_json_fixture(
            candidate_dir / "refined_parameter_application_report.json",
            safe_payload(),
        )
        write_json_fixture(candidate_dir / "refined_original_vs_refined_delta.json", safe_payload())
        csv_rows = []
        for row in records:
            csv_row = dict(row)
            csv_row["provenance"] = json.dumps(csv_row["provenance"])
            csv_row["source_prediction_flags"] = json.dumps(csv_row["source_prediction_flags"])
            csv_rows.append(csv_row)
        pd.DataFrame(csv_rows).to_csv(
            candidate_dir / "refined_candidate_signal_series.csv",
            index=False,
        )

    write_refined_candidate(baseline, "baseline_plus_trend_structure")
    write_refined_candidate(volatility, "volatility_regime")
    write_refined_candidate(risk, "risk_appetite")
    return {
        "scope_review_dir": scope_review_dir,
        "refined_generator_dir": refined_generator_dir,
        "refined_validation_dir": refined_validation_dir,
    }


def build_scope_narrowed_candidate_actual_path_validation_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    inputs = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    output_dir = tmp_path / "scope_narrowed_generator"
    run_scope_narrowed_candidate_generators_regenerate(
        scope_review_dir=inputs["scope_review_dir"],
        refined_generator_dir=inputs["refined_generator_dir"],
        refined_validation_dir=inputs["refined_validation_dir"],
        include_candidates=(
            "baseline_plus_trend_structure_refined_confidence_v1,"
            "volatility_regime_refined_confidence_v1"
        ),
        archive_candidates="risk_appetite_refined_confidence_v1",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="scope_narrowed_regeneration",
        docs_root=tmp_path / "scope_narrowed_docs",
    )
    return {
        **inputs,
        "scope_narrowed_generator_dir": output_dir,
        "prices_path": write_price_fixture(tmp_path),
        "rates_path": write_rates_fixture(tmp_path),
    }


def regenerated_context(
    tmp_path: Path,
    *,
    candidate_id: str,
    price_path: Path,
) -> CandidateGeneratorContext:
    return CandidateGeneratorContext(
        candidate_id=candidate_id,
        candidate_family=REGENERATED_CANDIDATE_FAMILY,
        target_asset="QQQ,SPY,SMH",
        start_date=date(2023, 1, 16),
        end_date=date(2023, 1, 20),
        horizon="5d,10d,20d",
        output_dir=tmp_path / candidate_id,
        mode="regenerated_candidate_artifacts",
        generated_at=datetime(2026, 6, 29, tzinfo=UTC),
        signal_spec_version="first_layer_candidate_signal_spec.v1",
        prediction_schema_version="candidate_bound_prediction_artifact.v1",
        input_snapshot_hash=f"{candidate_id}_input_hash",
        feature_snapshot_hash=f"{candidate_id}_feature_hash",
        source_paths=(price_path, Path(__file__)),
        source_hashes=(f"{candidate_id}_price_hash", f"{candidate_id}_source_hash"),
    )


def build_and_validate_bundle(generator: object, context: CandidateGeneratorContext):
    spec = generator.build_signal_spec(context)
    records = generator.generate_signal_series(context, spec)
    artifact = generator.generate_prediction_artifact(context, spec, records)
    bundle = CandidateGenerationBundle(
        context=context,
        signal_spec=spec,
        signal_records=records,
        prediction_artifact=artifact,
    )
    validation = validate_candidate_generation_bundle(
        bundle,
        task_id="TRADING-2284_TREND_RISK_VOLATILITY_EXECUTABLE_CANDIDATE_GENERATORS",
    )
    return spec, records, artifact, validation


def assert_common_candidate_contract(
    *,
    candidate_id: str,
    records: list[object],
    artifact: dict[str, object],
    validation: dict[str, object],
) -> None:
    assert validation["status"] == "PASS", validation["errors"]
    assert records
    payload = records[0].to_dict()
    assert payload["candidate_id"] == candidate_id
    assert payload["target_asset"] in {"QQQ", "SPY", "SMH"}
    assert payload["horizon"] in {"5d", "10d", "20d"}
    assert payload["source_artifact_hash"]
    assert payload["provenance"]["regeneration_mode"] == "deterministic_regeneration"
    assert -1.0 <= payload["signal_value"] <= 1.0
    assert 0.0 <= payload["signal_confidence"] <= 1.0
    assert payload["promotion_eligible"] is False
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert artifact["candidate_id"] == candidate_id
    assert artifact["artifact_role"] == "regenerated_executable_candidate_artifact"
    assert artifact["historical_executable_artifact"] is True
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["promotion_eligible"] is False
    assert artifact["promotion_allowed"] is False
    assert artifact["paper_shadow_allowed"] is False
    assert artifact["production_allowed"] is False
    assert artifact["broker_action"] == "none"
