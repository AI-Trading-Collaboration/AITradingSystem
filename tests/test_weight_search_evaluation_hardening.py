from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_weight_batch_search_helpers import run_weight_adaptive_branch_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation


def _tamper_and_restore(path: Path, validate) -> None:  # noqa: ANN001
    original = path.read_bytes()
    try:
        path.write_bytes(original + b" ")
        assert validate()["status"] == "FAIL"
    finally:
        path.write_bytes(original)


def _tamper_snapshot_schema(path: Path, validate) -> None:  # noqa: ANN001
    original = path.read_bytes()
    try:
        payload = json.loads(original.decode("utf-8"))
        payload["schema_version"] = "tampered.v0"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        assert validate()["status"] == "FAIL"
    finally:
        path.write_bytes(original)


def test_weight_search_evaluation_chain_rebuilds_all_views_and_binds_expansion(tmp_path) -> None:
    fixture = run_weight_adaptive_branch_fixture(tmp_path)
    scorecard = fixture["scorecard"]
    robustness = fixture["robustness"]
    branch = fixture["branch"]
    score_root = Path(scorecard["scorecard_dir"])
    robust_root = Path(robustness["robustness_dir"])
    branch_root = Path(branch["branch_dir"])

    def score_validate() -> dict[str, object]:
        return evaluation.validate_weight_scorecard_artifact(
            scorecard_id=scorecard["scorecard_id"],
            output_dir=tmp_path / "weight_scorecard",
        )

    def robust_validate() -> dict[str, object]:
        return evaluation.validate_weight_robustness_review_artifact(
            robustness_id=robustness["robustness_id"],
            output_dir=tmp_path / "weight_robustness_review",
        )

    def branch_validate() -> dict[str, object]:
        return evaluation.validate_weight_adaptive_branch_artifact(
            branch_id=branch["branch_id"],
            output_dir=tmp_path / "weight_adaptive_branch",
        )

    assert score_validate()["status"] == "PASS"
    assert robust_validate()["status"] == "PASS"
    assert branch_validate()["status"] == "PASS"

    for name in evaluation.SCORECARD_VIEWS:
        _tamper_and_restore(score_root / name, score_validate)
    for name in evaluation.ROBUSTNESS_VIEWS:
        _tamper_and_restore(robust_root / name, robust_validate)
    for name in evaluation.ADAPTIVE_VIEWS:
        _tamper_and_restore(branch_root / name, branch_validate)

    _tamper_snapshot_schema(score_root / "weight_scorecard_input_snapshot.json", score_validate)
    _tamper_snapshot_schema(
        robust_root / "weight_robustness_review_input_snapshot.json", robust_validate
    )
    _tamper_snapshot_schema(
        branch_root / "weight_adaptive_branch_input_snapshot.json", branch_validate
    )

    adaptive_snapshot_path = branch_root / "weight_adaptive_branch_input_snapshot.json"
    original_adaptive_snapshot = adaptive_snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original_adaptive_snapshot.decode("utf-8"))
        snapshot["robustness_source"]["artifact_id"] = "cross-lineage-robustness"
        adaptive_snapshot_path.write_text(
            json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        assert branch_validate()["status"] == "FAIL"
    finally:
        adaptive_snapshot_path.write_bytes(original_adaptive_snapshot)

    expanded = evaluation.build_weight_expanded_search(
        branch_id=branch["branch_id"],
        branch_dir=tmp_path / "weight_adaptive_branch",
        search_space_dir=tmp_path / "weight_search_space",
        output_dir=tmp_path / "weight_expanded_search",
        generated_at=datetime(2024, 3, 7, tzinfo=UTC),
    )
    branch_decision_path = branch_root / "branch_decision.json"
    original_branch_decision = branch_decision_path.read_bytes()
    try:
        branch_decision_path.write_bytes(original_branch_decision + b" ")
        with pytest.raises(ValueError):
            evaluation.run_weight_expanded_search(
                expanded_matrix_id=expanded["matrix_id"],
                expanded_matrix_dir=tmp_path / "weight_expanded_search",
                baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
                output_dir=tmp_path / "expanded_weight_batch_backfill",
                price_cache_path=fixture["prices_path"],
                rates_cache_path=fixture["rates_path"],
            )
    finally:
        branch_decision_path.write_bytes(original_branch_decision)

    assert branch_validate()["status"] == "PASS"
