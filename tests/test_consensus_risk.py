from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def _bundle(files: dict[str, Any]) -> dict[str, Any]:
    return {"files": {name: {"content": content} for name, content in files.items()}}


def test_consensus_risk_keeps_missing_metrics_null_and_forbids_default_execution(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    output_dir = tmp_path / "consensus_risk"

    result = accumulation.run_consensus_risk_review(
        output_dir=output_dir,
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        historical_replay_dir=tmp_path / "historical_replay",
        backfill_dir=tmp_path / "backfilled_outcome",
        repair_dir=tmp_path / "backfill_repair",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    assert result["manifest"]["consensus_target_risk"] == "INSUFFICIENT_DATA"
    assert result["manifest"]["consensus_target_default_execution_recommended"] is False
    assert result["consensus_exposure_summary"]["sample_count"] == 0
    assert result["consensus_exposure_summary"]["risk_asset_exposure"] == {
        "min": None,
        "mean": None,
        "max": None,
    }
    assert all(
        row["avg_drawdown"] is None
        and row["max_drawdown"] is None
        and row["drawdown_delta_vs_no_trade"] is None
        for row in result["consensus_drawdown_risk"]["window_results"]
    )
    assert result["consensus_turnover_risk"]["avg_turnover"] is None
    assert result["consensus_turnover_risk"]["max_turnover"] is None
    assert (
        accumulation.validate_consensus_risk_artifact(
            risk_id=result["risk_id"], output_dir=output_dir
        )["status"]
        == "PASS"
    )

    report_path = Path(result["risk_dir"]) / "consensus_risk_report.md"
    original = report_path.read_text(encoding="utf-8")
    report_path.write_text(original + "tampered\n", encoding="utf-8")
    assert (
        accumulation.validate_consensus_risk_artifact(
            risk_id=result["risk_id"], output_dir=output_dir
        )["status"]
        == "FAIL"
    )
    report_path.write_text(original, encoding="utf-8")


def test_consensus_risk_uses_distinct_dates_pairs_and_reviewed_policy() -> None:
    weights = {"CASH": 0.15, "QQQ": 0.5, "SMH": 0.2, "SOXX": 0.15}
    events = []
    outcome_rows = []
    for index in range(5):
        event_id = f"event-{index}"
        as_of = f"2026-06-{index + 1:02d}"
        events.append(
            {
                "replay_event_id": event_id,
                "as_of": as_of,
                "variants": [
                    {
                        "variant": "consensus_target",
                        "weights": weights,
                        "turnover": 0.3,
                    }
                ],
            }
        )
        for window in (5, 20):
            common = {
                "replay_event_id": event_id,
                "as_of": as_of,
                "window_days": window,
                "outcome_status": "AVAILABLE",
            }
            outcome_rows.extend(
                [
                    {
                        **common,
                        "variant": "consensus_target",
                        "max_drawdown": -0.01,
                    },
                    {**common, "variant": "no_trade", "max_drawdown": -0.015},
                ]
            )
    snapshot = {
        "generated_cutoff": "2026-07-01T00:00:00+00:00",
        "daily_sources": [],
        "historical_replay_sources": [_bundle({"replay_events.jsonl": events})],
        "outcome_source_type": "backfill",
        "outcome_sources": [_bundle({"replay_outcome_windows.jsonl": outcome_rows})],
    }
    policy = accumulation._load_consensus_risk_policy(
        accumulation.DEFAULT_CONSENSUS_RISK_POLICY_PATH
    )

    samples, coverage, pairs = accumulation._consensus_risk_views_from_snapshot(snapshot)
    exposure = accumulation._consensus_exposure_summary_v2(samples, policy, coverage)
    drawdown = accumulation._consensus_drawdown_risk_v2(pairs, policy)
    turnover = accumulation._consensus_turnover_risk_v2(samples, policy)

    assert len(samples) == 5
    assert coverage["paired_outcome_sample_count"] == 10
    assert turnover["sample_count"] == 5
    assert turnover["avg_turnover"] == pytest.approx(0.3)
    assert all(
        row["risk_status"] == "PASS"
        for row in drawdown["window_results"]
        if row["required_for_pass"]
    )
    assert accumulation._consensus_overall_risk_status_v2(
        exposure, drawdown, turnover, policy
    ) == "PASS"


def test_consensus_risk_merges_equal_daily_replay_date_and_rejects_conflict() -> None:
    csv_text = "symbol,median_target_weight\nCASH,0.2\nQQQ,0.8\n"
    daily = _bundle(
        {
            "daily_advisory_manifest.json": {
                "daily_advisory_id": "daily-1",
                "as_of": "2026-06-01",
            },
            "daily_consensus_weights.csv": csv_text,
            "daily_position_deltas.jsonl": [{"deltas": {"CASH": -0.1, "QQQ": 0.1}}],
        }
    )
    replay_event = {
        "replay_event_id": "event-1",
        "as_of": "2026-06-01",
        "variants": [
            {
                "variant": "consensus_target",
                "weights": {"CASH": 0.2, "QQQ": 0.8},
                "turnover": 0.2,
            }
        ],
    }
    snapshot = {
        "generated_cutoff": "2026-07-01T00:00:00+00:00",
        "daily_sources": [daily],
        "historical_replay_sources": [
            _bundle({"replay_events.jsonl": [replay_event]})
        ],
        "outcome_source_type": "backfill",
        "outcome_sources": [],
    }

    samples, coverage, _ = accumulation._consensus_risk_views_from_snapshot(snapshot)
    assert len(samples) == 1
    assert samples[0]["source_mode"] == "DAILY_AND_HISTORICAL_REPLAY"
    assert coverage["merged_daily_replay_date_count"] == 1

    replay_event["variants"][0]["weights"] = {"CASH": 0.1, "QQQ": 0.9}
    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation._consensus_risk_views_from_snapshot(snapshot)
