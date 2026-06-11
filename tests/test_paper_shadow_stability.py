from __future__ import annotations

from dynamic_v3_system_target_helpers import run_stability_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_stability_writes_jump_and_turnover_diagnostics(tmp_path) -> None:
    fixture = run_stability_fixture(tmp_path)
    stability = fixture["stability"]
    metrics = stability["method_stability_metrics"]
    turnover = stability["turnover_diagnostics"]

    assert stability["manifest"]["status"] == "PASS"
    assert {row["target_method"] for row in metrics} == set(system_target.TARGET_METHODS)
    assert {row["target_method"] for row in turnover["methods"]} == set(
        system_target.TARGET_METHODS
    )
    assert all(row["stability_status"] in {"STABLE", "MODERATE", "UNSTABLE"} for row in metrics)
    assert all(row["broker_action_allowed"] is False for row in metrics)
    assert all(row["broker_action_taken"] is False for row in stability["weight_path_jump_events"])

    validation = system_target.validate_paper_shadow_stability_artifact(
        stability_id=stability["stability_id"],
        output_dir=tmp_path / "paper_shadow_stability",
    )
    assert validation["status"] == "PASS"
