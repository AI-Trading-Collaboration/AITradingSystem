from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from ai_trading_system.etf_portfolio import dynamic_v3_parameter_research as legacy
from ai_trading_system.trading2453_constraint_hit_diagnosis import (
    DEFAULT_PACKAGE_ROOT,
    DEFAULT_POLICY_PATH,
    DEFAULT_RUN_DIR,
    DEFAULT_RUN_ID,
    SAFETY,
    Trading2453ConstraintDiagnosisError,
    build_trading2453_diagnosis,
    numeric_distribution_preserving_null,
    recompute_constraint_row,
    validate_trading2453_diagnosis,
    write_trading2453_diagnosis,
)


def test_s0_s3_recomputes_classifies_and_builds_owner_options() -> None:
    bundle = build_trading2453_diagnosis()
    report = bundle["attribution"]
    summary = report["recomputation_summary"]

    assert bundle["manifest"]["status"] == "PASS"
    assert bundle["manifest"]["safety"] == SAFETY
    assert summary["evaluation_count"] == 1800
    assert summary["exact_match_count"] == 1800
    assert summary["mismatch_count"] == 0
    assert summary["rejected_count"] == 1800
    assert summary["null_selection_score_count"] == 1800
    assert all(summary["structure_checks"].values())

    by_fold = {row["key"]: row for row in report["aggregations"]["by_fold"]}
    assert set(by_fold) == {1, 2, 3, 4, 5, 6}
    assert all(row["evaluation_count"] == 300 for row in by_fold.values())
    assert by_fold[6]["constraint_reason_counts"] == {
        "constraint_hit_rate_exceeds_policy": 300,
        "constraint_hits_delta_exceeds_policy": 96,
    }
    by_constraint_type = {row["key"]: row for row in report["aggregations"]["by_constraint_type"]}
    assert by_constraint_type["constraint_hit_rate_exceeds_policy"]["evaluation_count"] == 1800
    assert by_constraint_type["constraint_hits_delta_exceeds_policy"]["fold_counts"] == [
        {"key": 6, "evaluation_count": 96}
    ]
    assert (
        by_constraint_type["constraint_hits_delta_exceeds_policy"][
            "constraint_hits_delta_vs_reference"
        ]["null_count"]
        == 0
    )
    assert report["aggregations"]["concentration"] == {
        "candidate_template_unique_count": 3,
        "candidate_template_max_share": 1602 / 1800,
        "candidate_template_hhi": (1602 / 1800) ** 2 + (102 / 1800) ** 2 + (96 / 1800) ** 2,
        "policy_hash_unique_count": 300,
        "policy_hash_max_share": 6 / 1800,
        "policy_hash_hhi": 300 * (6 / 1800) ** 2,
    }
    semantic_audit = report["s2_semantic_audit"]
    assert semantic_audit["status"] == "COMPLETE"
    assert semantic_audit["primary_classification"] == "POLICY_ROLE_MISMATCH_REQUIRES_OWNER_REVIEW"
    assert semantic_audit["production_path_modified"] is False
    assert semantic_audit["calculation_assessment"]["classification"] == "CALCULATION_MATCH"
    best_design = semantic_audit["best_candidate_design_assessment"]
    assert best_design["classification"] == "DESIGNED_TEMPLATE_SELECTION_NOT_IMPLEMENTATION_DEFECT"
    assert best_design["frozen_template_count"] == 4
    assert best_design["best_candidate_filter"] == "group == dynamic_v0_3_rescue"
    assert best_design["source_references"][0]["symbols"] == [
        "_materialized_policy_set",
        "_best_v0_3_candidate",
    ]
    policy_role = semantic_audit["policy_role_assessment"]
    assert policy_role["classification"] == "POLICY_ROLE_MISMATCH_REQUIRES_OWNER_REVIEW"
    assert policy_role["frozen_result_is_correct_under_policy"] is True
    assert policy_role["policy_comment_explicitly_not_promotion_gate"] is True
    assert policy_role["threshold"] == 0.65
    discrimination = semantic_audit["constraint_outcome_discrimination_assessment"]
    assert discrimination["classification"] == (
        "LOW_DISCRIMINATION_STRUCTURAL_DEGENERATION_NOT_CODE_BUG"
    )
    assert discrimination["folds_with_uniform_constraint_hit_rate"] == [1, 2, 3, 4, 5]
    assert discrimination["best_template_counts"] == [
        {
            "candidate_template": "dynamic_regime_overlay_v0_3a_constraint_smooth",
            "evaluation_count": 96,
        },
        {
            "candidate_template": "dynamic_regime_overlay_v0_3b_drawdown_guarded",
            "evaluation_count": 1602,
        },
        {
            "candidate_template": "dynamic_regime_overlay_v0_3d_emergency_only_guarded",
            "evaluation_count": 102,
        },
    ]
    assert discrimination["gate_reason_combo_counts"] == [
        {
            "gate_reasons": ["constraint_hit_rate_exceeds_policy"],
            "evaluation_count": 1704,
        },
        {
            "gate_reasons": [
                "constraint_hit_rate_exceeds_policy",
                "constraint_hits_delta_exceeds_policy",
            ],
            "evaluation_count": 96,
        },
    ]
    owner_pack = bundle["owner_review_pack"]
    assert owner_pack["default_decision"] == "KILL_PAUSE"
    assert owner_pack["recommended_option_id"] == "A_KEEP_KILL_AND_CLOSE_CURRENT_PACKAGE"
    assert [option["option_id"] for option in owner_pack["options"]] == [
        "A_KEEP_KILL_AND_CLOSE_CURRENT_PACKAGE",
        "B_NEW_REVIEWED_GATE_AND_NEW_PREREGISTRATION",
        "C_AUTHORIZED_TEMPLATE_AXIS_CAUSAL_DIAGNOSTIC",
    ]
    assert owner_pack["options"][0]["recommended"] is True
    assert owner_pack["options"][1]["same_package_replay_allowed"] is False
    assert owner_pack["options"][2]["new_authorization_required"] is True
    assert len(owner_pack["evidence"]["fold_constraint_summary"]) == 6
    assert owner_pack["evidence"]["fold_constraint_summary"][0]["constraint_hit_rate"] == {
        "observation_count": 300,
        "present_count": 300,
        "null_count": 0,
        "minimum": 0.816631,
        "maximum": 0.816631,
        "mean": 0.816631,
    }
    assert "直接放宽 max_constraint_hit_rate=0.65" in owner_pack["prohibited_actions"]
    assert bundle["manifest"]["completed_stages"] == ["S0", "S1", "S2", "S3"]
    assert bundle["manifest"]["original_trading2452_result_status_changed"] is False
    assert bundle["manifest"]["prospective_holdout_accessed"] is False
    assert bundle["manifest"]["thresholds_modified"] is False


@pytest.mark.parametrize("target", ["attribution_json", "owner_markdown"])
def test_content_derived_validator_detects_output_tamper(tmp_path: Path, target: str) -> None:
    output_dir = tmp_path / "diagnosis"
    result = write_trading2453_diagnosis(output_dir=output_dir)

    assert result["validation"]["status"] == "PASS"
    if target == "attribution_json":
        payload = json.loads(
            (output_dir / "constraint_hit_attribution.json").read_text(encoding="utf-8")
        )
        payload["recomputation_summary"]["evaluation_count"] = 1799
        (output_dir / "constraint_hit_attribution.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        markdown_path = output_dir / "owner_review_pack.md"
        markdown_path.write_text(
            markdown_path.read_text(encoding="utf-8") + "tampered\n",
            encoding="utf-8",
        )

    validation = validate_trading2453_diagnosis(output_dir=output_dir)

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1


def test_validator_rejects_extra_stale_output_file(tmp_path: Path) -> None:
    output_dir = tmp_path / "diagnosis"
    write_trading2453_diagnosis(output_dir=output_dir)
    (output_dir / "stale.json").write_text("{}\n", encoding="utf-8")

    validation = validate_trading2453_diagnosis(output_dir=output_dir)

    assert validation["status"] == "FAIL"
    inventory_check = next(
        check for check in validation["checks"] if check["check_id"] == "output_inventory_exact"
    )
    assert inventory_check["passed"] is False


def test_writer_rejects_nonempty_output_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "diagnosis"
    output_dir.mkdir()
    (output_dir / "stale.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(Trading2453ConstraintDiagnosisError, match="absent or empty"):
        write_trading2453_diagnosis(output_dir=output_dir)


def test_frozen_train_input_tamper_fails_closed(tmp_path: Path) -> None:
    run_dir = tmp_path / DEFAULT_RUN_ID
    package_root = tmp_path / "package"
    shutil.copytree(DEFAULT_RUN_DIR, run_dir)
    shutil.copytree(DEFAULT_PACKAGE_ROOT, package_root)
    train_path = run_dir / "train_evaluations.jsonl"
    train_path.write_bytes(train_path.read_bytes() + b"\n")

    with pytest.raises(Trading2453ConstraintDiagnosisError, match="frozen run input drift"):
        build_trading2453_diagnosis(run_dir=run_dir, package_root=package_root)


def test_row_recomputation_detects_gate_reason_semantic_tamper() -> None:
    row = json.loads(
        DEFAULT_RUN_DIR.joinpath("train_evaluations.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    candidates = json.loads(
        DEFAULT_PACKAGE_ROOT.joinpath("candidate_universe.json").read_text(encoding="utf-8")
    )["candidates"]
    candidate = next(item for item in candidates if item["candidate_id"] == row["candidate_id"])
    config = legacy.load_parameter_sweep_config(DEFAULT_POLICY_PATH)
    row["gate_reasons"] = []

    recomputed = recompute_constraint_row(row=row, candidate=candidate, config=config)

    assert recomputed["status"] == "FAIL"
    assert recomputed["exact_match"]["gate_reasons_exact_match"] is False
    assert recomputed["recomputed"]["gate_reasons"] == ["constraint_hit_rate_exceeds_policy"]


def test_numeric_distribution_preserves_null_instead_of_coercing_to_zero() -> None:
    distribution = numeric_distribution_preserving_null([None, 0, 1])

    assert distribution == {
        "observation_count": 3,
        "present_count": 2,
        "null_count": 1,
        "minimum": 0.0,
        "maximum": 1.0,
        "mean": 0.5,
    }
