from __future__ import annotations

from pathlib import Path

import yaml
from dynamic_v3_position_readiness_helpers import (
    position_advisory_config,
    shadow_shortlist_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW,
    POSITION_ADVISORY_TARGET_ONLY,
    run_position_advisory,
    validate_position_advisory_artifact,
)


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
