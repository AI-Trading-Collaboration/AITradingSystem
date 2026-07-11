from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
from dynamic_v3_position_readiness_helpers import (
    position_advisory_config,
    shadow_shortlist_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    POSITION_ADVISORY_SNAPSHOT_DELTA,
    POSITION_ADVISORY_TARGET_ONLY,
    DynamicV3ParameterResearchError,
    create_owner_review,
    run_consensus_drift,
    run_position_advisory_daily,
    run_shadow_shortlist_monitor,
    validate_position_advisory_daily_artifact,
)
from ai_trading_system.reports import reader_brief


def test_position_advisory_daily_target_only_and_snapshot_delta(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    config_path = position_advisory_config(tmp_path)

    target_only = run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "position_advisory_daily",
    )
    assert target_only["daily_advisory_actions"]["mode"] == POSITION_ADVISORY_TARGET_ONLY
    assert target_only["daily_advisory_actions"]["broker_action_allowed"] is False

    snapshot = tmp_path / "snapshot.yaml"
    snapshot.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "as_of": "2026-06-07",
                "base_currency": "USD",
                "account_type": "manual_snapshot",
                "source": "manual",
                "total_equity": 100000.0,
                "cash": {"symbol": "CASH", "weight": 0.20, "value": 20000.0},
                "positions": [
                    {"symbol": "QQQ", "weight": 0.50, "value": 50000.0, "currency": "USD"},
                    {"symbol": "SMH", "weight": 0.20, "value": 20000.0, "currency": "USD"},
                    {"symbol": "TLT", "weight": 0.10, "value": 10000.0, "currency": "USD"},
                ],
                "metadata": {"owner_reviewed": True, "broker_imported": False},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    snapshot_delta = run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=config_path,
        portfolio_snapshot_path=snapshot,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "position_advisory_daily",
    )

    assert snapshot_delta["daily_advisory_actions"]["mode"] == POSITION_ADVISORY_SNAPSHOT_DELTA
    assert snapshot_delta["daily_advisory_actions"]["owner_approval_required"] is True
    assert (
        validate_position_advisory_daily_artifact(
            daily_advisory_id=snapshot_delta["daily_advisory_id"],
            output_dir=tmp_path / "position_advisory_daily",
        )["status"]
        == "PASS"
    )


def test_high_disagreement_forces_daily_manual_review(tmp_path: Path) -> None:
    monitor_dir = _write_high_disagreement_monitor(tmp_path)
    config_path = position_advisory_config(tmp_path)
    drift = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
    )

    advisory = run_position_advisory_daily(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        consensus_drift_dir=tmp_path / "consensus_drift",
        output_dir=tmp_path / "position_advisory_daily",
    )

    assert monitor_dir.exists()
    assert drift["summary"]["disagreement_status"] == "HIGH_DISAGREEMENT"
    assert advisory["daily_advisory_actions"]["recommended_action"] == "manual_review"
    assert all(
        row["candidate_agreement_ratio"] == 0.0
        for row in advisory["daily_consensus_weights"]
    )


def test_daily_advisory_rejects_invalid_monitor_and_future_snapshot_before_write(
    tmp_path: Path,
) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    monitor_manifest_path = (
        Path(monitor["monitor_run_dir"]) / "shadow_monitor_manifest.json"
    )
    monitor_manifest = json.loads(monitor_manifest_path.read_text(encoding="utf-8"))
    monitor_manifest["broker_action_allowed"] = True
    monitor_manifest_path.write_text(json.dumps(monitor_manifest), encoding="utf-8")
    with pytest.raises(
        DynamicV3ParameterResearchError,
        match="shadow monitor validation must PASS",
    ):
        run_position_advisory_daily(
            shadow_monitor_run_id=monitor["monitor_run_id"],
            config_path=position_advisory_config(tmp_path),
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            output_dir=tmp_path / "position_advisory_daily",
        )
    assert not (tmp_path / "position_advisory_daily").exists()

    valid_root = tmp_path / "valid_case"
    valid_root.mkdir()
    valid_fixture = shadow_shortlist_fixture(valid_root)
    valid_monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=valid_fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=valid_root / "shadow_shortlist",
        output_dir=valid_root / "shadow_monitor_runs",
    )
    future_snapshot = _write_snapshot(valid_root / "future_snapshot.yaml", as_of="2026-06-08")
    with pytest.raises(
        DynamicV3ParameterResearchError,
        match="snapshot as_of cannot be later",
    ):
        run_position_advisory_daily(
            shadow_monitor_run_id=valid_monitor["monitor_run_id"],
            config_path=position_advisory_config(valid_root),
            portfolio_snapshot_path=future_snapshot,
            shadow_monitor_run_dir=valid_root / "shadow_monitor_runs",
            output_dir=valid_root / "position_advisory_daily",
        )
    assert not (valid_root / "position_advisory_daily").exists()


def test_daily_advisory_selects_semantic_latest_same_chain_drift_not_mtime(
    tmp_path: Path,
) -> None:
    _write_high_disagreement_monitor(tmp_path)
    config_path = position_advisory_config(tmp_path)
    older = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )
    newer = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 11, tzinfo=UTC),
    )
    os.utime(Path(older["drift_dir"]), (2_000_000_000, 2_000_000_000))

    advisory = run_position_advisory_daily(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        consensus_drift_dir=tmp_path / "consensus_drift",
        output_dir=tmp_path / "position_advisory_daily",
        generated_at=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )
    assert advisory["manifest"]["source_consensus_drift_id"] == newer["drift_id"]
    assert advisory["manifest"]["consensus_drift_evidence_status"] == "CORROBORATED"


def test_daily_advisory_rejects_tampered_same_chain_drift_before_write(
    tmp_path: Path,
) -> None:
    _write_high_disagreement_monitor(tmp_path)
    config_path = position_advisory_config(tmp_path)
    drift = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )
    summary_path = Path(drift["drift_dir"]) / "consensus_drift_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["disagreement_status"] = "CONSENSUS"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    with pytest.raises(
        DynamicV3ParameterResearchError,
        match="latest same-chain consensus drift evidence is invalid",
    ):
        run_position_advisory_daily(
            shadow_monitor_run_id="monitor-high",
            config_path=config_path,
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            consensus_drift_dir=tmp_path / "consensus_drift",
            output_dir=tmp_path / "position_advisory_daily",
            generated_at=datetime(2026, 6, 7, 11, tzinfo=UTC),
        )
    assert not (tmp_path / "position_advisory_daily").exists()


def test_daily_advisory_validator_detects_output_and_source_drift(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    advisory = run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "position_advisory_daily",
    )
    actions_path = Path(advisory["daily_advisory_dir"]) / "daily_advisory_actions.json"
    actions = json.loads(actions_path.read_text(encoding="utf-8"))
    actions["recommended_action"] = "small_adjustment_review_only"
    actions_path.write_text(json.dumps(actions), encoding="utf-8")
    output_validation = validate_position_advisory_daily_artifact(
        daily_advisory_id=advisory["daily_advisory_id"],
        output_dir=tmp_path / "position_advisory_daily",
    )
    assert output_validation["status"] == "FAIL"
    assert "advisory_actions_content_matches" in _failed_check_ids(output_validation)

    monitor_report = Path(monitor["monitor_run_dir"]) / "shadow_monitor_report.md"
    monitor_report.write_text(
        monitor_report.read_text(encoding="utf-8") + "\nsource drift\n",
        encoding="utf-8",
    )
    source_validation = validate_position_advisory_daily_artifact(
        daily_advisory_id=advisory["daily_advisory_id"],
        output_dir=tmp_path / "position_advisory_daily",
    )
    assert source_validation["status"] == "FAIL"
    assert "source_checksums_match" in _failed_check_ids(source_validation)


def test_position_advisory_daily_flows_into_reader_brief(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    config_path = position_advisory_config(tmp_path)
    drift = run_consensus_drift(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
    )
    advisory = run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        consensus_drift_dir=tmp_path / "consensus_drift",
        output_dir=tmp_path / "position_advisory_daily",
    )
    review = create_owner_review(
        daily_advisory_id=advisory["daily_advisory_id"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    leaderboard_path = _write_minimal_leaderboard(tmp_path)

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(
        {
            "reports": [
                _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
                _report_record(
                    "etf_dynamic_v3_shadow_monitor_run",
                    tmp_path
                    / "shadow_monitor_runs"
                    / monitor["monitor_run_id"]
                    / "shadow_monitor_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_position_advisory_daily",
                    tmp_path
                    / "position_advisory_daily"
                    / advisory["daily_advisory_id"]
                    / "daily_advisory_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_consensus_drift",
                    tmp_path
                    / "consensus_drift"
                    / drift["drift_id"]
                    / "consensus_drift_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_owner_review",
                    tmp_path / "owner_review_journal" / "latest_owner_review.json",
                ),
            ]
        }
    )

    assert summary["shadow_monitor_run_active_count"] == monitor["summary"]["active_count"]
    assert summary["position_advisory_daily_mode"] == POSITION_ADVISORY_TARGET_ONLY
    assert summary["consensus_drift_disagreement_status"] == "CONSENSUS"
    assert summary["owner_review_decision"] == review["review"]["owner_decision"]
    assert summary["position_advisory_daily_broker_action_allowed"] is False


def _write_high_disagreement_monitor(tmp_path: Path) -> Path:
    monitor_dir = tmp_path / "shadow_monitor_runs" / "monitor-high"
    monitor_dir.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "monitor_run_id": "monitor-high",
        "shadow_shortlist_id": "shadow-high",
        "as_of": "2026-06-07",
        "generated_at": "2026-06-07T09:00:00+00:00",
        "status": "PASS",
        "candidate_count": 2,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
    }
    rows = [
        {
            "candidate_id": "candidate-a",
            "as_of": "2026-06-07",
            "cluster_id": "cluster-1",
            "cluster_label": "risk_on",
            "target_weights": {"QQQ": 0.90, "CASH": 0.10},
            "manual_review_required": True,
            "monitoring_status": "active",
        },
        {
            "candidate_id": "candidate-b",
            "as_of": "2026-06-07",
            "cluster_id": "cluster-2",
            "cluster_label": "risk_off",
            "target_weights": {"QQQ": 0.10, "CASH": 0.90},
            "manual_review_required": True,
            "monitoring_status": "active",
        },
    ]
    (monitor_dir / "shadow_monitor_manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    (monitor_dir / "shadow_candidate_daily_results.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    (monitor_dir / "shadow_candidate_weekly_summary.jsonl").write_text("", encoding="utf-8")
    (monitor_dir / "shadow_monitor_summary.json").write_text(
        json.dumps(
            {
                "monitor_run_id": "monitor-high",
                "broker_action_allowed": False,
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    (monitor_dir / "shadow_monitor_report.md").write_text("# Monitor\n", encoding="utf-8")
    (monitor_dir / "reader_brief_section.md").write_text("## Monitor\n", encoding="utf-8")
    return monitor_dir


def _write_snapshot(path: Path, *, as_of: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "as_of": as_of,
                "base_currency": "USD",
                "account_type": "manual_snapshot",
                "source": "manual",
                "total_equity": 100000.0,
                "cash": {"symbol": "CASH", "weight": 0.20, "value": 20000.0},
                "positions": [
                    {
                        "symbol": "QQQ",
                        "weight": 0.80,
                        "value": 80000.0,
                        "currency": "USD",
                    }
                ],
                "metadata": {"owner_reviewed": True, "broker_imported": False},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    return {
        str(check["check_id"])
        for check in payload["checks"]  # type: ignore[index]
        if check["passed"] is False  # type: ignore[index]
    }


def _write_minimal_leaderboard(tmp_path: Path) -> Path:
    leaderboard_dir = tmp_path / "leaderboard"
    leaderboard_dir.mkdir(parents=True)
    leaderboard_path = leaderboard_dir / "leaderboard.json"
    leaderboard_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "completed",
                "candidate_count": 2,
                "evaluator_mode": "real_dynamic_v3_rescue",
                "evaluator_version": "test",
                "metrics_source": "real_evaluation_artifact",
                "not_for_investment_decision": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
                "top_eligible_candidates": [
                    {"candidate_id": "candidate-a", "gate": "OBSERVE_ONLY", "score": 1.0}
                ],
                "most_common_reject_reasons": [],
                "recommended_next_actions": ["owner_review_required"],
                "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
            }
        ),
        encoding="utf-8",
    )
    return leaderboard_path


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-08",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
