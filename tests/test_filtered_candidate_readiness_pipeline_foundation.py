from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_filtered_candidate_readiness_helpers import (
    run_filtered_candidate_evidence_fixture,
)
from dynamic_v3_weight_batch_search_helpers import (
    run_filtered_candidate_promotion_review_fixture,
)

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_pipeline as upstream,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness_pipeline as readiness,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_real_eb1_eb2_eb3_chain_is_reproducible_and_fail_closed(tmp_path: Path) -> None:
    source = run_filtered_candidate_promotion_review_fixture(tmp_path)
    comparison = source["filtered_vs_original_comparison"]
    review = source["filtered_candidate_promotion_review"]

    evidence = readiness.run_filtered_candidate_evidence(
        filtered_comparison_id=comparison["comparison_id"],
        promotion_review_id=review["filtered_review_id"],
        comparison_dir=tmp_path / "filtered_vs_original_comparison",
        promotion_review_dir=tmp_path / "filtered_candidate_promotion_review",
        output_dir=tmp_path / "filtered_candidate_evidence",
        generated_at=datetime(2026, 4, 8, tzinfo=UTC),
    )
    spec = readiness.review_median_regime_filter_spec(
        output_dir=tmp_path / "median_regime_filter_spec",
        generated_at=datetime(2026, 4, 8, 1, tzinfo=UTC),
    )
    stress = readiness.run_filtered_candidate_stress_backfill(
        spec_id=spec["spec_id"],
        spec_dir=tmp_path / "median_regime_filter_spec",
        output_dir=tmp_path / "filtered_candidate_stress_backfill",
        generated_at=datetime(2026, 4, 9, tzinfo=UTC),
    )
    mismatch = readiness.run_drawdown_mismatch_reduction(
        stress_backfill_id=stress["stress_backfill_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        output_dir=tmp_path / "drawdown_mismatch_reduction",
        generated_at=datetime(2026, 4, 10, tzinfo=UTC),
    )
    flip = readiness.run_flip_rotation_reduction(
        stress_backfill_id=stress["stress_backfill_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        output_dir=tmp_path / "flip_rotation_reduction",
        generated_at=datetime(2026, 4, 11, tzinfo=UTC),
    )
    ab = readiness.run_filtered_candidate_ab_review(
        stress_backfill_id=stress["stress_backfill_id"],
        mismatch_reduction_id=mismatch["reduction_id"],
        flip_reduction_id=flip["flip_reduction_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        mismatch_reduction_dir=tmp_path / "drawdown_mismatch_reduction",
        flip_reduction_dir=tmp_path / "flip_rotation_reduction",
        output_dir=tmp_path / "filtered_candidate_ab_review",
        generated_at=datetime(2026, 4, 12, tzinfo=UTC),
    )
    confirmation = readiness.register_signal_gate_confirmation(
        ab_review_id=ab["ab_review_id"],
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        output_dir=tmp_path / "signal_gate_confirmation",
        generated_at=datetime(2026, 4, 13, tzinfo=UTC),
    )
    formalization = readiness.run_filtered_formalization_readiness(
        ab_review_id=ab["ab_review_id"],
        confirmation_id=confirmation["confirmation_id"],
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        confirmation_dir=tmp_path / "signal_gate_confirmation",
        output_dir=tmp_path / "filtered_formalization_readiness",
        generated_at=datetime(2026, 4, 14, tzinfo=UTC),
    )
    owner = readiness.build_owner_filtered_candidate_review(
        readiness_id=formalization["readiness_id"],
        readiness_dir=tmp_path / "filtered_formalization_readiness",
        output_dir=tmp_path / "owner_filtered_candidate_review",
        generated_at=datetime(2026, 4, 15, tzinfo=UTC),
    )
    decision = readiness.run_filtered_next_decision(
        owner_review_id=owner["owner_review_id"],
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        output_dir=tmp_path / "filtered_next_decision",
        generated_at=datetime(2026, 4, 16, tzinfo=UTC),
    )

    validations = [
        readiness.validate_filtered_candidate_evidence_artifact(
            evidence_id=evidence["evidence_id"],
            output_dir=tmp_path / "filtered_candidate_evidence",
        ),
        readiness.validate_median_regime_filter_spec_artifact(
            spec_id=spec["spec_id"], output_dir=tmp_path / "median_regime_filter_spec"
        ),
        readiness.validate_filtered_candidate_stress_backfill_artifact(
            stress_backfill_id=stress["stress_backfill_id"],
            output_dir=tmp_path / "filtered_candidate_stress_backfill",
        ),
        readiness.validate_drawdown_mismatch_reduction_artifact(
            reduction_id=mismatch["reduction_id"],
            output_dir=tmp_path / "drawdown_mismatch_reduction",
        ),
        readiness.validate_flip_rotation_reduction_artifact(
            flip_reduction_id=flip["flip_reduction_id"],
            output_dir=tmp_path / "flip_rotation_reduction",
        ),
        readiness.validate_filtered_candidate_ab_review_artifact(
            ab_review_id=ab["ab_review_id"],
            output_dir=tmp_path / "filtered_candidate_ab_review",
        ),
        readiness.validate_signal_gate_confirmation_artifact(
            confirmation_id=confirmation["confirmation_id"],
            output_dir=tmp_path / "signal_gate_confirmation",
        ),
        readiness.validate_filtered_formalization_readiness_artifact(
            readiness_id=formalization["readiness_id"],
            output_dir=tmp_path / "filtered_formalization_readiness",
        ),
        readiness.validate_owner_filtered_candidate_review_artifact(
            owner_review_id=owner["owner_review_id"],
            output_dir=tmp_path / "owner_filtered_candidate_review",
        ),
        readiness.validate_filtered_next_decision_artifact(
            decision_id=decision["decision_id"],
            output_dir=tmp_path / "filtered_next_decision",
        ),
    ]

    assert {item["status"] for item in validations} == {"PASS"}
    assert evidence["filtered_candidate_evidence_summary"]["evidence_status"] == (
        "INSUFFICIENT_DATA"
    )
    assert stress["stress_window_metrics"] == []
    assert ab["ab_method_comparison"] == []
    assert confirmation["signal_gate_confirmation_targets"]["targets"] == []
    assert formalization["formalization_readiness_decision"]["decision"] == ("INSUFFICIENT_DATA")
    assert decision["filtered_next_decision"]["decision"] == "COLLECT_DATED_EVIDENCE"
    assert decision["next_task_plan"]["next_tasks"] == []


def test_eb3_validation_rejects_bound_source_tamper(tmp_path: Path, monkeypatch) -> None:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path, monkeypatch)
    evidence = fixture["filtered_candidate_evidence"]
    source_path = (
        tmp_path
        / "filtered_vs_original_comparison"
        / fixture["filtered_vs_original_comparison"]["comparison_id"]
        / "filtered_improvement_summary.json"
    )
    source_path.write_text(source_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    validation = readiness.validate_filtered_candidate_evidence_artifact(
        evidence_id=evidence["evidence_id"], output_dir=tmp_path / "filtered_candidate_evidence"
    )

    assert validation["status"] == "FAIL"


def test_eb3_validation_rejects_output_and_snapshot_tamper(tmp_path: Path, monkeypatch) -> None:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path, monkeypatch)
    evidence = fixture["filtered_candidate_evidence"]
    root = Path(evidence["evidence_dir"])
    summary_path = root / "filtered_candidate_evidence_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["evidence_status"] = "PROMISING"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        readiness.validate_filtered_candidate_evidence_artifact(
            evidence_id=evidence["evidence_id"],
            output_dir=tmp_path / "filtered_candidate_evidence",
        )["status"]
        == "FAIL"
    )

    snapshot_path = root / "filtered_candidate_evidence_input_snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["schema_version"] = "filtered_candidate_evidence_input_snapshot.v1"
    snapshot_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        readiness.validate_filtered_candidate_evidence_artifact(
            evidence_id=evidence["evidence_id"],
            output_dir=tmp_path / "filtered_candidate_evidence",
        )["status"]
        == "FAIL"
    )


def test_eb3_rejects_cross_lineage_and_pre_source_chronology(tmp_path: Path, monkeypatch) -> None:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path, monkeypatch)
    comparison = fixture["filtered_vs_original_comparison"]
    review = fixture["filtered_candidate_promotion_review"]
    original_reader = readiness._validated_promotion_review
    mismatched = dict(
        original_reader(
            review["filtered_review_id"], tmp_path / "filtered_candidate_promotion_review"
        )
    )
    mismatched["source_ledger_id"] = "candidate-signal-ledger_other"
    monkeypatch.setattr(readiness, "_validated_promotion_review", lambda *_: mismatched)

    with pytest.raises(
        readiness.DynamicV3FilteredCandidateReadinessPipelineError,
        match="source source_ledger_id lineage mismatch",
    ):
        readiness.run_filtered_candidate_evidence(
            filtered_comparison_id=comparison["comparison_id"],
            promotion_review_id=review["filtered_review_id"],
            comparison_dir=tmp_path / "filtered_vs_original_comparison",
            promotion_review_dir=tmp_path / "filtered_candidate_promotion_review",
            output_dir=tmp_path / "cross_lineage_evidence",
            generated_at=datetime(2024, 4, 9, tzinfo=UTC),
        )

    monkeypatch.setattr(readiness, "_validated_promotion_review", original_reader)
    with pytest.raises(readiness.DynamicV3FilteredCandidateReadinessPipelineError):
        readiness.run_filtered_candidate_evidence(
            filtered_comparison_id=comparison["comparison_id"],
            promotion_review_id=review["filtered_review_id"],
            comparison_dir=tmp_path / "filtered_vs_original_comparison",
            promotion_review_dir=tmp_path / "filtered_candidate_promotion_review",
            output_dir=tmp_path / "chronology_evidence",
            generated_at=datetime(2024, 4, 6, tzinfo=UTC),
        )


def test_canonical_signal_persistence_filter_id_is_evaluated() -> None:
    policy = {"signal_quality_policy": {"persistence_days": 3}}

    assert (
        upstream._event_matches_filter({"persistence_days": 2}, "signal_persistence_filter", policy)
        is True
    )
    assert (
        upstream._event_matches_filter({"persistence_days": 4}, "signal_persistence_filter", policy)
        is False
    )
    assert (
        upstream._event_matches_filter(
            {"persistence_days": 2}, "signal_persistence_3d_filter", policy
        )
        is None
    )
