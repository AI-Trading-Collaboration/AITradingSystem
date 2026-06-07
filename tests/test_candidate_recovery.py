from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    apply_evidence_gate_policy,
    rebuild_observe_pool_from_recovery,
    run_candidate_recovery,
    run_evidence_summary,
    validate_candidate_recovery_artifact,
    validate_observe_pool_artifact,
)


def test_candidate_recovery_rebuilds_manual_review_observe_pool(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    run_evidence_summary(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "evidence_summary",
        candidate_attribution_dir=tmp_path / "missing_attribution",
        overfit_dir=tmp_path / "missing_overfit",
        data_provenance_dir=tmp_path / "missing_provenance",
        window_audit_dir=tmp_path / "missing_window",
    )
    policy = apply_evidence_gate_policy(
        sweep_id=sweep["sweep_id"],
        policy_path=DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
        sweep_output_dir=sweep["sweep_output_dir"],
        evidence_summary_dir=tmp_path / "evidence_summary",
        regime_coverage_dir=tmp_path / "missing_regime",
        output_dir=tmp_path / "gate_policy",
    )

    recovery = run_candidate_recovery(
        sweep_id=sweep["sweep_id"],
        policy_run_id=policy["policy_run_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        gate_policy_dir=tmp_path / "gate_policy",
        output_dir=tmp_path / "candidate_recovery",
    )
    pool = rebuild_observe_pool_from_recovery(
        recovery_id=recovery["recovery_id"],
        recovery_dir=tmp_path / "candidate_recovery",
        output_dir=tmp_path / "observe_pool",
    )

    assert recovery["manifest"]["recovered_candidate_count"] > 0
    assert recovery["manifest"]["manual_review_required_count"] == recovery["manifest"][
        "recovered_candidate_count"
    ]
    assert pool["manifest"]["observe_candidate_count"] == recovery["manifest"][
        "recovered_candidate_count"
    ]
    assert (
        validate_candidate_recovery_artifact(
            recovery_id=recovery["recovery_id"],
            output_dir=tmp_path / "candidate_recovery",
        )["status"]
        == "PASS"
    )
    assert (
        validate_observe_pool_artifact(
            pool_id=pool["pool_id"],
            output_dir=tmp_path / "observe_pool",
        )["status"]
        == "PASS"
    )
