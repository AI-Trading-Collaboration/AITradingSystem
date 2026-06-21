from __future__ import annotations

import json

from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    Path,
    _assert_safety,
    _run_tail_risk_falsification_inputs,
    _run_tail_risk_review_board_inputs,
    run_tail_risk_fallback_anti_leakage_audit,
    run_tail_risk_fallback_audit_universe_reconciliation,
    run_tail_risk_fallback_forward_maturity_scoreboard,
    run_tail_risk_fallback_regime_segmented_robustness,
    run_tail_risk_fallback_threshold_sensitivity,
    run_tail_risk_policy_controlled_review_board,
)


def test_tail_risk_fallback_audit_universe_reconciliation_counts(tmp_path: Path) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_fallback_audit_universe_reconciliation(
        robustness_path=paths["robustness"],
        precision_recall_path=paths["precision"],
        opportunity_cost_path=paths["opportunity"],
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "reconciliation",
    )

    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-821"
    assert payload["status"] in {"RECONCILED", "PARTIALLY_RECONCILED"}
    counts = {
        (row["source_task"], row["count_name"]): row
        for row in payload["count_reconciliation_summary"]
    }
    assert ("TRADING-816", "fallback_trigger_count") in counts
    assert ("TRADING-817", "TP_plus_FP") in counts
    assert ("TRADING-818", "benchmark_upside_case_count") in counts
    assert ("TRADING-819", "fallback_trigger_count") in counts
    assert all(not row["is_comparable_to_other_count"] for row in counts.values())


def test_tail_risk_fallback_reconciliation_missing_denominator_blocks_reconciled(
    tmp_path: Path,
) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    broken = json.loads(paths["robustness"].read_text(encoding="utf-8"))
    broken["original_metric"].pop("case_count", None)
    broken_path = tmp_path / "broken_robustness.json"
    broken_path.write_text(json.dumps(broken), encoding="utf-8")

    payload = run_tail_risk_fallback_audit_universe_reconciliation(
        robustness_path=broken_path,
        precision_recall_path=paths["precision"],
        opportunity_cost_path=paths["opportunity"],
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "reconciliation_broken",
    )

    assert payload["status"] == "INCOMPLETE"
    assert payload["summary"]["controlled_review_status"] == "CONTROLLED_RESEARCH_BLOCKED"
    assert any(
        item["missing_field_name"] == "sample_count_total"
        for item in payload["missing_field_records"]
    )


def test_tail_risk_fallback_anti_leakage_flags_label_coupling(tmp_path: Path) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_fallback_anti_leakage_audit(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "anti_leakage",
    )

    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-822"
    assert payload["status"] == "ANTI_LEAKAGE_BLOCKED"
    assert payload["label_trigger_overlap_audit"]["coupling_risk"] in {"HIGH", "CRITICAL"}
    assert any(
        blocker["blocker"] == "trigger_label_same_source_without_independent_validation"
        for blocker in payload["blockers"]
    )
    assert any(row["pit_status"] == "unknown" for row in payload["pit_revision_audit"])


def test_tail_risk_fallback_sensitivity_covers_perturbation_families(tmp_path: Path) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_fallback_threshold_sensitivity(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "sensitivity",
    )

    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-823"
    assert any(row["variant_id"] == "baseline" for row in payload["variant_results"])
    coverage = payload["perturbation_coverage"]
    assert coverage["threshold"]
    assert coverage["lag"]
    assert coverage["horizon"]
    assert coverage["benchmark"]
    assert coverage["cost"]
    if payload["stability_summary"]["cliff_detected"]:
        assert payload["status"] == "SENSITIVITY_FRAGILE"


def test_tail_risk_fallback_regime_segmented_outputs_required_segments(tmp_path: Path) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_fallback_regime_segmented_robustness(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "regime",
    )

    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-824"
    segment_types = {row["segment_type"] for row in payload["segment_results"]}
    assert {"calendar", "volatility", "trend"} <= segment_types
    assert payload["segment_unavailable"]
    assert all(row["reason"] for row in payload["segment_unavailable"])
    assert payload["status"] in {
        "REGIME_ROBUST",
        "REGIME_WARNING",
        "REGIME_CONCENTRATED",
        "INSUFFICIENT_SEGMENT_EVIDENCE",
    }


def test_tail_risk_fallback_forward_maturity_excludes_pending_records(
    tmp_path: Path,
) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_fallback_forward_maturity_scoreboard(
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "scoreboard",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-825"
    assert payload["status"] == "FORWARD_PENDING"
    assert payload["scoreboard"]["matured_record_count"] == 0
    assert (
        payload["scoreboard"]["pending_record_count"]
        == payload["scoreboard"]["forward_record_count"]
    )
    assert payload["promotion_readiness_assessment"] != "PROMOTION_READY"


def test_tail_risk_controlled_review_board_reads_falsification_reports(
    tmp_path: Path,
) -> None:
    paths = _run_tail_risk_falsification_inputs(tmp_path)
    payload = run_tail_risk_policy_controlled_review_board(
        robustness_path=paths["robustness"],
        precision_recall_path=paths["precision"],
        opportunity_cost_path=paths["opportunity"],
        forward_integration_path=paths["forward"],
        audit_universe_reconciliation_path=paths["reconciliation"],
        anti_leakage_path=paths["anti_leakage"],
        sensitivity_path=paths["sensitivity"],
        regime_segmented_path=paths["regime"],
        forward_maturity_scoreboard_path=paths["scoreboard"],
        output_root=tmp_path / "board",
    )

    _assert_safety(payload)
    assert payload["summary"]["anti_leakage_status"] == "ANTI_LEAKAGE_BLOCKED"
    assert payload["review_decision"]["decision"] == "CONTROLLED_RESEARCH_BLOCKED"
    assert payload["review_decision"]["promotion_gate_allowed"] is False
