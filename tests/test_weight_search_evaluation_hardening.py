from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_weight_batch_search_helpers import run_weight_adaptive_branch_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_validation_scope as validation_scope,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


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


@with_artifact_validation_session
def test_weight_search_evaluation_chain_rebuilds_all_views_and_binds_expansion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    backfill_id = fixture["weight_backfill"]["batch_backfill_id"]
    backfill_dir = tmp_path / "weight_batch_backfill"
    scorecard_id = scorecard["scorecard_id"]
    scorecard_dir = tmp_path / "weight_scorecard"
    adapter_calls = {"backfill": 0, "scorecard": 0}

    def counted_backfill_validator(**_: object) -> dict[str, object]:
        adapter_calls["backfill"] += 1
        return {"status": "PASS"}

    def counted_scorecard_validator(**_: object) -> dict[str, object]:
        adapter_calls["scorecard"] += 1
        return {"status": "PASS"}

    with monkeypatch.context() as cache_patch:
        cache_patch.setattr(
            evaluation.foundation,
            "validate_weight_batch_backfill_artifact",
            counted_backfill_validator,
        )
        cache_patch.setattr(
            evaluation,
            "validate_weight_scorecard_artifact",
            counted_scorecard_validator,
        )
        for _ in range(2):
            evaluation._validated_backfill(backfill_id, backfill_dir)
            evaluation._validated_scorecard(scorecard_id, scorecard_dir)
        assert adapter_calls == {"backfill": 1, "scorecard": 1}

        def assert_backfill_scope_change_revalidates(path: Path) -> None:
            original = path.read_bytes()
            before = adapter_calls["backfill"]
            try:
                path.write_bytes(original + b"\n")
                evaluation._validated_backfill(backfill_id, backfill_dir)
                assert adapter_calls["backfill"] == before + 1
            finally:
                path.write_bytes(original)
            restored = adapter_calls["backfill"]
            evaluation._validated_backfill(backfill_id, backfill_dir)
            assert adapter_calls["backfill"] == restored

        for dependency_path in (
            Path(fixture["position_advisory_daily_dir"])
            / "daily-1"
            / "daily_advisory_manifest.json",
            Path(fixture["weight_search_config_path"]),
            Path(fixture["prices_path"]),
            Path(fixture["rates_path"]),
        ):
            assert_backfill_scope_change_revalidates(dependency_path)

    fail_calls = 0

    def failing_backfill_validator(**_: object) -> dict[str, object]:
        nonlocal fail_calls
        fail_calls += 1
        return {"status": "FAIL"}

    with monkeypatch.context() as fail_patch:
        fail_patch.setattr(
            evaluation.foundation,
            "validate_weight_batch_backfill_artifact",
            failing_backfill_validator,
        )
        for _ in range(2):
            with pytest.raises(ValueError, match="source backfill validation failed"):
                evaluation._validated_backfill(backfill_id, backfill_dir)
    assert fail_calls == 2

    exception_calls = 0

    def exploding_backfill_validator(**_: object) -> dict[str, object]:
        nonlocal exception_calls
        exception_calls += 1
        raise RuntimeError("evaluation validator exception")

    with monkeypatch.context() as exception_patch:
        exception_patch.setattr(
            evaluation.foundation,
            "validate_weight_batch_backfill_artifact",
            exploding_backfill_validator,
        )
        for _ in range(2):
            with pytest.raises(RuntimeError, match="evaluation validator exception"):
                evaluation._validated_backfill(backfill_id, backfill_dir)
    assert exception_calls == 2

    bypass_calls = 0

    def bypass_validator(**_: object) -> dict[str, object]:
        nonlocal bypass_calls
        bypass_calls += 1
        return {"status": "PASS"}

    backfill_snapshot_path = (
        Path(fixture["weight_backfill"]["backfill_dir"])
        / "weight_batch_backfill_input_snapshot.json"
    )
    original_backfill_snapshot = backfill_snapshot_path.read_bytes()
    try:
        malformed_snapshot = json.loads(original_backfill_snapshot.decode("utf-8"))
        malformed_snapshot["price_source"]["path"] = "relative-cache.csv"
        backfill_snapshot_path.write_text(
            json.dumps(malformed_snapshot, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for _ in range(2):
            validation_scope.validate_upstream_with_hardened_scope(
                validator=bypass_validator,
                validator_key="backfill_id",
                artifact_id=backfill_id,
                output_dir=backfill_dir,
                snapshot_name="weight_batch_backfill_input_snapshot.json",
            )
        assert bypass_calls == 2
    finally:
        backfill_snapshot_path.write_bytes(original_backfill_snapshot)

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
