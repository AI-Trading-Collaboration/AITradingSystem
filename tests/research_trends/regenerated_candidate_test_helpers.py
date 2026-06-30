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
