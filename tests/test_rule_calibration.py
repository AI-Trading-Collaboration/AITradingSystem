from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_RULE_CALIBRATION_POLICY_PATH,
    DynamicV3HistoricalReplayError,
    run_backfill_repair,
    run_replay_diagnosis,
    run_rule_calibration,
    run_variant_comparison,
    validate_rule_calibration_artifact,
)


def test_rule_calibration_generates_manual_only_policy_proposals(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    diagnosis = run_replay_diagnosis(
        inventory_id=chain["inventory"]["inventory_id"],
        replay_id=chain["replay"]["replay_id"],
        backfill_id=chain["backfill"]["backfill_id"],
        sim_id=chain["sim"]["sim_id"],
        review_id=chain["review"]["review_id"],
        inventory_dir=paths["inventory_dir"],
        replay_dir=paths["historical_replay_dir"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        review_dir=paths["performance_review_dir"],
        output_dir=paths["diagnosis_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )
    repair = run_backfill_repair(
        backfill_id=chain["backfill"]["backfill_id"],
        diagnosis_id=diagnosis["diagnosis_id"],
        backfill_dir=paths["backfill_dir"],
        diagnosis_dir=paths["diagnosis_dir"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_repair_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )
    comparison = run_variant_comparison(
        backfill_id=chain["backfill"]["backfill_id"],
        repair_id=repair["repair_id"],
        backfill_dir=paths["backfill_dir"],
        repair_dir=paths["backfill_repair_dir"],
        output_dir=paths["variant_comparison_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )
    calibration_policy_path = tmp_path / "rule_calibration_v1.yaml"
    calibration_policy_path.write_bytes(DEFAULT_RULE_CALIBRATION_POLICY_PATH.read_bytes())
    target_policy_path = tmp_path / "position_advisory_v1.yaml"
    target_policy_path.write_bytes(DEFAULT_POSITION_ADVISORY_CONFIG_PATH.read_bytes())

    calibration = run_rule_calibration(
        comparison_id=comparison["comparison_id"],
        comparison_dir=paths["variant_comparison_dir"],
        output_dir=paths["rule_calibration_dir"],
        policy_path=calibration_policy_path,
        target_policy_path=target_policy_path,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    proposals = calibration["proposed_policy_adjustments"]["proposals"]
    actions = calibration["evidence_collection_actions"]["actions"]
    safety = calibration["calibration_safety_checks"]
    assert proposals == []
    assert actions[0]["action_type"] == "require_more_forward_data"
    assert actions[0]["policy_change_allowed"] is False
    assert calibration["manifest"]["status"] == "INSUFFICIENT_DATA"
    assert calibration["manifest"]["proposal_count"] == 0
    assert safety["auto_apply"] is False
    assert safety["broker_action_allowed"] is False
    assert safety["owner_approval_required"] is True
    assert calibration["manifest"]["baseline_config_mutated"] is False

    validation = validate_rule_calibration_artifact(
        calibration_id=calibration["calibration_id"],
        output_dir=paths["rule_calibration_dir"],
    )
    failed_checks = [check for check in validation["checks"] if not check["passed"]]
    assert not failed_checks, failed_checks
    assert validation["status"] == "PASS", validation

    with pytest.raises(
        DynamicV3HistoricalReplayError,
        match="must not precede source comparison",
    ):
        run_rule_calibration(
            comparison_id=comparison["comparison_id"],
            comparison_dir=paths["variant_comparison_dir"],
            output_dir=paths["rule_calibration_dir"],
            policy_path=calibration_policy_path,
            target_policy_path=target_policy_path,
            generated_at=datetime(2026, 7, 20, tzinfo=UTC),
        )

    tamper_cases = (
        (calibration["calibration_dir"] / "rule_calibration_source_snapshot.json", b"\n"),
        (calibration_policy_path, b"\n"),
        (target_policy_path, b"\n"),
        (comparison["comparison_dir"] / "variant_comparison_report.md", b"\n"),
        (calibration["calibration_dir"] / "advisory_rule_diagnostics.json", b"\n"),
        (calibration["calibration_dir"] / "rule_calibration_report.md", b"\n"),
    )
    for path, suffix in tamper_cases:
        original = path.read_bytes()
        tampered_bytes = (
            b"{}\n" if path.name == "advisory_rule_diagnostics.json" else original + suffix
        )
        path.write_bytes(tampered_bytes)
        tampered = validate_rule_calibration_artifact(
            calibration_id=calibration["calibration_id"],
            output_dir=paths["rule_calibration_dir"],
        )
        assert tampered["status"] == "FAIL", path
        path.write_bytes(original)
