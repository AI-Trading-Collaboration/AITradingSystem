from __future__ import annotations

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_hypothesis_backlog_generates_failure_taxonomy_and_priority_summary(tmp_path) -> None:
    result = system_target.build_hypothesis_backlog(
        output_dir=tmp_path / "hypothesis_backlog",
    )

    manifest = result["manifest"]
    taxonomy = result["failure_mode_taxonomy"]
    hypotheses = result["hypotheses"]
    priority = result["hypothesis_priority_summary"]

    assert manifest["status"] == "PASS"
    assert manifest["failure_modes_count"] >= len(system_target.DEFAULT_FAILURE_MODES)
    assert manifest["hypotheses_count"] == len(hypotheses)
    assert set(system_target.DEFAULT_FAILURE_MODES).issubset(
        {row["id"] for row in taxonomy["failure_modes"]}
    )
    assert all(row["target_failure_modes"] for row in hypotheses)
    assert all(row["promotion_eligible"] is False for row in hypotheses)
    assert priority["recommended_for_experiment_matrix"]
    assert manifest["experiment_only"] is True
    assert manifest["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"

    validation = system_target.validate_hypothesis_backlog_artifact(
        backlog_id=result["backlog_id"],
        output_dir=tmp_path / "hypothesis_backlog",
    )
    assert validation["status"] == "PASS"
