from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def _bundle(filename: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"files": {filename: {"content": rows}}}


def test_limited_vs_notrade_keeps_missing_metrics_null_and_does_not_apply_policy(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    output_dir = tmp_path / "limited_vs_notrade"
    result = accumulation.run_limited_vs_notrade_evaluation(
        output_dir=output_dir,
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        backfill_dir=tmp_path / "backfilled_outcome",
        repair_dir=tmp_path / "backfill_repair",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    metrics = result["window_comparison_metrics"]
    assert result["manifest"]["available_count"] == 0
    assert result["manifest"]["paired_sample_count"] == 0
    assert metrics["overall_recommendation"] == "insufficient_data"
    assert all(row["confidence"] == "INSUFFICIENT_DATA" for row in metrics["by_window"])
    assert all(row["avg_relative_return"] is None for row in metrics["by_window"])
    assert all(row["median_relative_return"] is None for row in metrics["by_window"])
    assert all(row["win_rate"] is None for row in metrics["by_window"])
    assert result["regime_breakdown"]["status"] == "UNAVAILABLE"
    assert all(
        row["avg_relative_return"] is None
        for row in result["regime_breakdown"]["by_regime"].values()
    )
    assert result["manifest"]["auto_policy_apply"] is False
    assert (
        accumulation.validate_limited_vs_notrade_artifact(
            focus_id=result["focus_id"], output_dir=output_dir
        )["status"]
        == "PASS"
    )

    focus_dir = Path(result["focus_dir"])
    metrics_path = focus_dir / "window_comparison_metrics.json"
    original = metrics_path.read_text(encoding="utf-8")
    tampered = json.loads(original)
    tampered["by_window"][0]["avg_relative_return"] = 0.0
    metrics_path.write_text(json.dumps(tampered, sort_keys=True), encoding="utf-8")
    assert (
        accumulation.validate_limited_vs_notrade_artifact(
            focus_id=result["focus_id"], output_dir=output_dir
        )["status"]
        == "FAIL"
    )
    metrics_path.write_text(original, encoding="utf-8")
    assert (
        accumulation.validate_limited_vs_notrade_artifact(
            focus_id=result["focus_id"], output_dir=output_dir
        )["status"]
        == "PASS"
    )


def test_limited_vs_notrade_uses_strict_pairs_and_policy_threshold() -> None:
    rows: list[dict[str, Any]] = []
    for index in range(5):
        common = {
            "replay_event_id": f"event-{index}",
            "window_days": 5,
            "as_of": f"2026-06-{index + 1:02d}",
            "outcome_status": "AVAILABLE",
            "regime": "ai_trend",
        }
        rows.extend(
            [
                {
                    **common,
                    "variant": "limited_adjustment",
                    "return": 0.03 + index / 100,
                    "max_drawdown": -0.01,
                    "turnover": 0.1,
                },
                {
                    **common,
                    "variant": "no_trade",
                    "return": 0.01 + index / 100,
                    "max_drawdown": -0.02,
                    "turnover": 0.0,
                },
            ]
        )
    rows.append(
        {
            "replay_event_id": "unpaired-event",
            "window_days": 5,
            "as_of": "2026-06-30",
            "outcome_status": "AVAILABLE",
            "regime": "ai_trend",
            "variant": "limited_adjustment",
            "return": 0.05,
        }
    )
    snapshot = {
        "generated_cutoff": "2026-07-01T00:00:00+00:00",
        "forward_sources": [],
        "historical_source_type": "repair",
        "historical_sources": [_bundle("repaired_outcome_windows.jsonl", rows)],
    }
    policy = accumulation._load_limited_vs_notrade_policy(
        accumulation.DEFAULT_LIMITED_VS_NOTRADE_POLICY_PATH
    )

    samples, coverage = accumulation._limited_vs_notrade_views_from_snapshot(snapshot)
    metrics = accumulation._limited_window_metrics_v2(samples, policy)
    five_day = next(row for row in metrics if row["window_days"] == 5)

    assert coverage == {
        "source_row_count": 11,
        "paired_sample_count": 5,
        "unpaired_source_row_count": 1,
        "available_paired_sample_count": 5,
        "production_effect": "none",
    }
    assert five_day["avg_relative_return"] == pytest.approx(0.02)
    assert five_day["confidence"] == "MEDIUM"
    assert accumulation._limited_overall_recommendation_v2(metrics, policy) == (
        "support_limited_adjustment"
    )
    regime = accumulation._limited_regime_breakdown_v2(samples, policy)
    assert regime["by_regime"]["ai_trend"]["available_count"] == 5
    assert regime["by_regime"]["ai_trend"]["avg_relative_return"] == pytest.approx(0.02)


@pytest.mark.parametrize("invalid_kind", ["duplicate", "unknown_variant", "unknown_status"])
def test_limited_vs_notrade_rejects_ambiguous_historical_rows(invalid_kind: str) -> None:
    row = {
        "replay_event_id": "event-1",
        "window_days": 5,
        "variant": "limited_adjustment",
        "outcome_status": "AVAILABLE",
        "return": 0.02,
    }
    rows = [row, {**row, "variant": "no_trade", "return": 0.01}]
    if invalid_kind == "duplicate":
        rows.append(dict(row))
    elif invalid_kind == "unknown_variant":
        rows[0]["variant"] = "paper_portfolio"
    else:
        rows[0]["outcome_status"] = "UNKNOWN"
    snapshot = {
        "generated_cutoff": "2026-07-01T00:00:00+00:00",
        "forward_sources": [],
        "historical_source_type": "repair",
        "historical_sources": [_bundle("repaired_outcome_windows.jsonl", rows)],
    }

    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation._limited_vs_notrade_views_from_snapshot(snapshot)
