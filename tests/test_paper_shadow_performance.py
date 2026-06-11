from __future__ import annotations

from dynamic_v3_system_target_helpers import EVALUATION_AS_OF, TARGET_AS_OF, run_performance_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_performance_runs_after_data_quality_gate(tmp_path) -> None:
    fixture = run_performance_fixture(tmp_path)
    performance = fixture["performance"]
    summary = performance["method_performance_summary"]

    assert summary["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["performance_start_date"] == TARGET_AS_OF.isoformat()
    assert summary["evaluation_as_of"] == EVALUATION_AS_OF.isoformat()
    assert summary["return_observation_count"] > 0
    assert summary["best_return_method"] != "INSUFFICIENT_DATA"
    assert performance["manifest"]["broker_action_taken"] is False

    validation = system_target.validate_paper_shadow_performance_artifact(
        performance_id=performance["performance_id"],
        output_dir=tmp_path / "paper_shadow_performance",
    )
    assert validation["status"] == "PASS"
