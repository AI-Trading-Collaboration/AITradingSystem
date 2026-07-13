from __future__ import annotations

from dynamic_v3_system_target_helpers import run_rolling_eval_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_rolling_eval_outputs_required_windows(tmp_path) -> None:
    fixture = run_rolling_eval_fixture(tmp_path)
    rolling = fixture["rolling"]
    inventory = rolling["rolling_window_inventory"]
    metrics = rolling["rolling_method_metrics"]
    stability = rolling["rolling_rank_stability"]

    window_types = {row["window_type"] for row in inventory["windows"]}
    assert {"full", "yearly", "rolling_3m", "rolling_6m", "rolling_12m"}.issubset(window_types)
    assert rolling["manifest"]["status"] == "PASS"
    assert rolling["manifest"]["market_regime"] == "ai_after_chatgpt"
    assert (
        rolling["manifest"]["input_snapshot_schema"]
        == "paper_shadow_rolling_eval_input_snapshot.v2"
    )
    assert len(metrics) >= len(system_target.TARGET_METHODS)
    assert all(row["broker_action_allowed"] is False for row in metrics)
    assert {row["target_method"] for row in stability["methods"]} == set(
        system_target.TARGET_METHODS
    )

    validation = system_target.validate_paper_shadow_rolling_eval_artifact(
        rolling_eval_id=rolling["rolling_eval_id"],
        output_dir=tmp_path / "paper_shadow_rolling_eval",
    )
    assert validation["status"] == "PASS"


def test_paper_shadow_rolling_eval_validation_rejects_derived_view_tamper(tmp_path) -> None:
    fixture = run_rolling_eval_fixture(tmp_path)
    rolling = fixture["rolling"]
    metrics_path = rolling["rolling_eval_dir"] / "rolling_method_metrics.jsonl"
    metrics_path.write_text(
        metrics_path.read_text(encoding="utf-8") + "{}\n",
        encoding="utf-8",
    )

    validation = system_target.validate_paper_shadow_rolling_eval_artifact(
        rolling_eval_id=rolling["rolling_eval_id"],
        output_dir=tmp_path / "paper_shadow_rolling_eval",
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
