from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from dynamic_v3_position_readiness_helpers import (
    position_advisory_config,
    shadow_shortlist_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW,
    POSITION_ADVISORY_TARGET_ONLY,
    DynamicV3ParameterResearchError,
    run_position_advisory,
    validate_position_advisory_artifact,
)


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    return {
        str(check["check_id"])
        for check in payload["checks"]  # type: ignore[index]
        if check["passed"] is False  # type: ignore[index]
    }


def test_position_advisory_target_only_without_snapshot(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    result = run_position_advisory(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        config_path=position_advisory_config(tmp_path),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_advisory",
    )

    assert result["manifest"]["position_advisory_status"] == POSITION_ADVISORY_TARGET_ONLY
    assert result["manifest"]["broker_action_allowed"] is False
    assert result["manifest"]["owner_approval_required"] is True
    assert (
        validate_position_advisory_artifact(
            advisory_id=result["advisory_id"],
            output_dir=tmp_path / "position_advisory",
        )["status"]
        == "PASS"
    )


def test_position_advisory_with_snapshot_requires_manual_review(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    snapshot = tmp_path / "current_portfolio_snapshot.yaml"
    snapshot.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "as_of": "2026-06-07",
                "positions": [
                    {"symbol": "QQQ", "weight": 0.50},
                    {"symbol": "SMH", "weight": 0.20},
                    {"symbol": "CASH", "weight": 0.30},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = run_position_advisory(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        config_path=position_advisory_config(tmp_path),
        portfolio_snapshot_path=snapshot,
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_advisory",
    )

    assert (
        result["manifest"]["position_advisory_status"] == POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
    )
    assert result["advisory_actions"]["broker_action_allowed"] is False
    assert result["advisory_actions"]["owner_approval_required"] is True
    assert result["candidate_position_deltas"]


def test_position_advisory_rejects_invalid_shadow_shortlist_before_write(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    shadow_dir = tmp_path / "shadow_shortlist" / fixture["shadow"]["shadow_shortlist_id"]
    candidates_path = shadow_dir / "shadow_shortlist_candidates.jsonl"
    rows = [json.loads(line) for line in candidates_path.read_text(encoding="utf-8").splitlines()]
    rows[0]["monitoring_start_after_owner_review"] = False
    candidates_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )

    with pytest.raises(
        DynamicV3ParameterResearchError,
        match="shadow shortlist validation must PASS",
    ):
        run_position_advisory(
            shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
            config_path=position_advisory_config(tmp_path),
            shadow_shortlist_dir=tmp_path / "shadow_shortlist",
            output_dir=tmp_path / "position_advisory",
        )
    assert not (tmp_path / "position_advisory").exists()


def test_position_advisory_rejects_invalid_candidate_weight_sum_before_write(
    tmp_path: Path,
) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    candidate = fixture["shadow"]["candidates"][0]
    source_path = Path(candidate["real_evaluation_artifact_path"])
    weight_path = source_path.parent / "daily_weights.csv"
    weight_path.write_text(
        weight_path.read_text(encoding="utf-8").replace(",0.05", ",0.50"),
        encoding="utf-8",
    )

    with pytest.raises(
        DynamicV3ParameterResearchError,
        match="complete target-weight paths",
    ):
        run_position_advisory(
            shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
            config_path=position_advisory_config(tmp_path),
            shadow_shortlist_dir=tmp_path / "shadow_shortlist",
            output_dir=tmp_path / "position_advisory",
        )
    assert not (tmp_path / "position_advisory").exists()


def test_position_advisory_validator_detects_output_and_source_drift(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    result = run_position_advisory(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        config_path=position_advisory_config(tmp_path),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_advisory",
    )
    advisory_dir = Path(result["advisory_dir"])
    actions_path = advisory_dir / "advisory_actions.json"
    actions = json.loads(actions_path.read_text(encoding="utf-8"))
    actions["recommended_action"] = "small_adjustment"
    actions_path.write_text(json.dumps(actions, indent=2, sort_keys=True), encoding="utf-8")

    output_validation = validate_position_advisory_artifact(
        advisory_id=result["advisory_id"],
        output_dir=tmp_path / "position_advisory",
    )
    assert output_validation["status"] == "FAIL"
    assert "advisory_actions_content_matches" in _failed_check_ids(output_validation)
    assert "output_checksum_matches:advisory_actions.json" in _failed_check_ids(output_validation)

    weight_path = Path(
        result["manifest"]["source_candidate_weight_artifacts"][0]["path"]
    )
    weight_path.write_text(weight_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    source_validation = validate_position_advisory_artifact(
        advisory_id=result["advisory_id"],
        output_dir=tmp_path / "position_advisory",
    )
    assert source_validation["status"] == "FAIL"
    assert "source_candidate_weight_checksums_match" in _failed_check_ids(source_validation)
