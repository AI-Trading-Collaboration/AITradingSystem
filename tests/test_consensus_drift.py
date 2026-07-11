from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from dynamic_v3_position_readiness_helpers import position_advisory_config

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DynamicV3ParameterResearchError,
    run_consensus_drift,
    validate_consensus_drift_artifact,
)


def test_consensus_drift_high_disagreement(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-high",
        as_of="2026-06-07",
        generated_at="2026-06-07T09:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.90, "CASH": 0.10}),
            ("candidate-b", {"QQQ": 0.10, "CASH": 0.90}),
        ],
    )

    result = run_consensus_drift(
        shadow_monitor_run_id="monitor-high",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )

    assert result["summary"]["disagreement_status"] == "HIGH_DISAGREEMENT"
    assert result["summary"]["min_candidate_agreement_ratio"] == 0.0
    assert result["summary"]["position_advisory_implication"] == "manual_review_required"
    assert result["manifest"]["source_monitor_validation_status"] == "PASS"
    assert result["manifest"]["policy_id"] == "dynamic_v3_rescue_position_advisory_v1"
    assert (
        validate_consensus_drift_artifact(
            drift_id=result["drift_id"],
            output_dir=tmp_path / "consensus_drift",
        )["status"]
        == "PASS"
    )


def test_consensus_drift_moderate_and_insufficient_require_manual_review(
    tmp_path: Path,
) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-moderate",
        as_of="2026-06-07",
        generated_at="2026-06-07T09:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.70, "TLT": 0.00, "CASH": 0.30}),
            ("candidate-b", {"QQQ": 0.45, "TLT": 0.25, "CASH": 0.30}),
        ],
    )
    moderate = run_consensus_drift(
        shadow_monitor_run_id="monitor-moderate",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )
    assert moderate["summary"]["disagreement_status"] == "MODERATE_DISAGREEMENT"
    assert moderate["summary"]["position_advisory_implication"] == "manual_review_required"

    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-single",
        as_of="2026-06-07",
        generated_at="2026-06-07T09:30:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.70, "CASH": 0.30})],
        shadow_shortlist_id="shadow-single",
    )
    insufficient = run_consensus_drift(
        shadow_monitor_run_id="monitor-single",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )
    assert insufficient["summary"]["disagreement_status"] == "INSUFFICIENT_DATA"
    assert insufficient["summary"]["position_advisory_implication"] == (
        "manual_review_required"
    )


def test_consensus_drift_rejects_invalid_or_future_monitor_before_write(
    tmp_path: Path,
) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-invalid",
        as_of="2026-06-07",
        generated_at="2026-06-07T09:00:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.70, "CASH": 0.20})],
    )
    config_path = position_advisory_config(tmp_path)
    with pytest.raises(DynamicV3ParameterResearchError, match="portfolio invariants"):
        run_consensus_drift(
            shadow_monitor_run_id="monitor-invalid",
            config_path=config_path,
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            output_dir=tmp_path / "consensus_drift",
            generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
        )
    assert not (tmp_path / "consensus_drift").exists()

    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-future",
        as_of="2026-06-07",
        generated_at="2026-06-07T11:00:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.70, "CASH": 0.30})],
        shadow_shortlist_id="shadow-future",
    )
    with pytest.raises(DynamicV3ParameterResearchError, match="later than consensus drift cutoff"):
        run_consensus_drift(
            shadow_monitor_run_id="monitor-future",
            config_path=config_path,
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            output_dir=tmp_path / "consensus_drift",
            generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
        )
    assert not (tmp_path / "consensus_drift").exists()


def test_consensus_drift_rejects_invalid_policy_threshold_before_write(
    tmp_path: Path,
) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-current",
        as_of="2026-06-08",
        generated_at="2026-06-08T10:00:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.60, "CASH": 0.40})],
    )
    config_path = position_advisory_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["consensus"]["max_symbol_dispersion"] = "not-a-number"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    with pytest.raises(DynamicV3ParameterResearchError, match="must be numeric"):
        run_consensus_drift(
            shadow_monitor_run_id="monitor-current",
            config_path=config_path,
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            output_dir=tmp_path / "consensus_drift",
            generated_at=datetime(2026, 6, 8, 11, tzinfo=UTC),
        )
    assert not (tmp_path / "consensus_drift").exists()


def test_consensus_drift_selects_semantic_previous_monitor_and_union_delta(
    tmp_path: Path,
) -> None:
    older = _write_monitor(
        tmp_path,
        monitor_run_id="monitor-older",
        as_of="2026-06-06",
        generated_at="2026-06-06T10:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.20, "CASH": 0.80}),
            ("candidate-b", {"QQQ": 0.20, "CASH": 0.80}),
        ],
    )
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-previous",
        as_of="2026-06-07",
        generated_at="2026-06-07T10:00:00+00:00",
        weights=[
            ("candidate-a", {"SMH": 0.50, "CASH": 0.50}),
            ("candidate-b", {"SMH": 0.50, "CASH": 0.50}),
        ],
    )
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-current",
        as_of="2026-06-08",
        generated_at="2026-06-08T10:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.50, "CASH": 0.50}),
            ("candidate-b", {"QQQ": 0.50, "CASH": 0.50}),
        ],
    )
    future_mtime = datetime(2026, 6, 9, tzinfo=UTC).timestamp()
    os.utime(older, (future_mtime, future_mtime))

    result = run_consensus_drift(
        shadow_monitor_run_id="monitor-current",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 8, 11, tzinfo=UTC),
    )
    change = result["summary"]["daily_consensus_change_vs_previous"]
    assert result["manifest"]["source_previous_shadow_monitor_run_id"] == "monitor-previous"
    assert change["source_shadow_monitor_run_id"] == "monitor-previous"
    assert change["candidate_set_status"] == "UNCHANGED"
    assert change["symbol_count"] == 3
    assert change["max_mean_weight_delta"] == 0.5


def test_consensus_drift_rejects_invalid_latest_previous_before_write(
    tmp_path: Path,
) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-previous-invalid",
        as_of="2026-06-07",
        generated_at="2026-06-07T10:00:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.60, "CASH": 0.30})],
    )
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-current",
        as_of="2026-06-08",
        generated_at="2026-06-08T10:00:00+00:00",
        weights=[("candidate-a", {"QQQ": 0.60, "CASH": 0.40})],
    )
    with pytest.raises(DynamicV3ParameterResearchError, match="portfolio invariants"):
        run_consensus_drift(
            shadow_monitor_run_id="monitor-current",
            config_path=position_advisory_config(tmp_path),
            shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
            output_dir=tmp_path / "consensus_drift",
            generated_at=datetime(2026, 6, 8, 11, tzinfo=UTC),
        )
    assert not (tmp_path / "consensus_drift").exists()


def test_consensus_drift_validator_ignores_previous_monitor_generated_after_cutoff(
    tmp_path: Path,
) -> None:
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-current",
        as_of="2026-06-08",
        generated_at="2026-06-08T10:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.60, "CASH": 0.40}),
            ("candidate-b", {"QQQ": 0.55, "CASH": 0.45}),
        ],
    )
    result = run_consensus_drift(
        shadow_monitor_run_id="monitor-current",
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 8, 11, tzinfo=UTC),
    )
    _write_monitor(
        tmp_path,
        monitor_run_id="monitor-created-later",
        as_of="2026-06-07",
        generated_at="2026-06-08T12:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.50, "CASH": 0.50}),
            ("candidate-b", {"QQQ": 0.50, "CASH": 0.50}),
        ],
    )
    assert (
        validate_consensus_drift_artifact(
            drift_id=result["drift_id"],
            output_dir=tmp_path / "consensus_drift",
        )["status"]
        == "PASS"
    )


def test_consensus_drift_validator_detects_output_and_source_tampering(
    tmp_path: Path,
) -> None:
    current = _write_monitor(
        tmp_path,
        monitor_run_id="monitor-current",
        as_of="2026-06-08",
        generated_at="2026-06-08T10:00:00+00:00",
        weights=[
            ("candidate-a", {"QQQ": 0.60, "CASH": 0.40}),
            ("candidate-b", {"QQQ": 0.55, "CASH": 0.45}),
        ],
    )
    config_path = position_advisory_config(tmp_path)
    first = run_consensus_drift(
        shadow_monitor_run_id="monitor-current",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 8, 11, tzinfo=UTC),
    )
    pairwise_path = Path(first["drift_dir"]) / "candidate_pairwise_disagreement.csv"
    pairwise_path.write_text("tampered\n", encoding="utf-8")
    output_validation = validate_consensus_drift_artifact(
        drift_id=first["drift_id"],
        output_dir=tmp_path / "consensus_drift",
    )
    assert output_validation["status"] == "FAIL"
    assert "pairwise_content_matches" in _failed_check_ids(output_validation)

    second = run_consensus_drift(
        shadow_monitor_run_id="monitor-current",
        config_path=config_path,
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "consensus_drift",
        generated_at=datetime(2026, 6, 8, 12, tzinfo=UTC),
    )
    daily_path = current / "shadow_candidate_daily_results.jsonl"
    rows = [json.loads(line) for line in daily_path.read_text(encoding="utf-8").splitlines()]
    rows[0]["target_weights"] = {"QQQ": 0.50, "CASH": 0.50}
    daily_path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    source_validation = validate_consensus_drift_artifact(
        drift_id=second["drift_id"],
        output_dir=tmp_path / "consensus_drift",
    )
    assert source_validation["status"] == "FAIL"
    assert "source_checksums_match" in _failed_check_ids(source_validation)


def _write_monitor(
    tmp_path: Path,
    *,
    monitor_run_id: str,
    as_of: str,
    generated_at: str,
    weights: list[tuple[str, dict[str, float]]],
    shadow_shortlist_id: str = "shadow-shared",
) -> Path:
    monitor_dir = tmp_path / "shadow_monitor_runs" / monitor_run_id
    monitor_dir.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "monitor_run_id": monitor_run_id,
        "shadow_shortlist_id": shadow_shortlist_id,
        "as_of": as_of,
        "generated_at": generated_at,
        "status": "PASS",
        "candidate_count": len(weights),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
    }
    rows = [
        {
            "candidate_id": candidate_id,
            "cluster_id": f"cluster-{index}",
            "cluster_label": "test",
            "as_of": as_of,
            "target_weights": target_weights,
            "manual_review_required": True,
            "monitoring_status": "active",
        }
        for index, (candidate_id, target_weights) in enumerate(weights, start=1)
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
                "monitor_run_id": monitor_run_id,
                "broker_action_allowed": False,
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    (monitor_dir / "shadow_monitor_report.md").write_text("# Monitor\n", encoding="utf-8")
    (monitor_dir / "reader_brief_section.md").write_text("## Monitor\n", encoding="utf-8")
    return monitor_dir


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    return {
        str(check["check_id"])
        for check in payload["checks"]  # type: ignore[index]
        if check["passed"] is False  # type: ignore[index]
    }
