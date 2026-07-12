from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_outcome_loop_helpers import run_rolling_refresh_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_evidence_trend_marks_single_refresh_as_insufficient_history(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    run_rolling_refresh_fixture(tmp_path, monkeypatch)

    result = accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    summary = result["confidence_trend_summary"]
    assert summary["trend_status"] == "INSUFFICIENT_HISTORY"
    assert summary["available_sample_growth"] is None
    assert summary["confidence_change"] == "NO_CHANGE"
    assert summary["next_action"] == "continue_tracking"
    assert summary["policy_id"] == "dynamic_v3_evidence_trend_v1"
    assert result["evidence_trend_timeseries"][0]["sample_scope"] == (
        "post_dashboard_full_forward_state"
    )
    assert (
        accumulation.validate_evidence_trend_artifact(
            trend_id=result["trend_id"],
            output_dir=tmp_path / "evidence_trend",
        )["status"]
        == "PASS"
    )


def test_evidence_trend_excludes_rolled_back_and_legacy_refreshes(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    refresh_root = tmp_path / "rolling_evidence_refresh"
    rolled = refresh_root / "rolled-back-refresh"
    rolled.mkdir()
    _write_json(
        rolled / "rolling_refresh_transaction.json",
        {"status": "ROLLED_BACK", "outcome_update_id": "rolled-update"},
    )
    legacy = refresh_root / "legacy-refresh"
    legacy.mkdir()
    _write_json(
        legacy / "rolling_refresh_manifest.json",
        {
            "refresh_id": "legacy-refresh",
            "outcome_update_id": "legacy-update",
            "generated_at": "2026-06-09T00:00:00+00:00",
            "as_of": "2026-06-09",
            "broker_action_allowed": False,
            "broker_action_taken": False,
        },
    )
    _write_json(legacy / "refreshed_artifacts.json", {})
    _write_json(legacy / "evidence_delta_summary.json", {})
    (legacy / "rolling_evidence_refresh_report.md").write_text("legacy\n", encoding="utf-8")
    (legacy / "reader_brief_section.md").write_text("legacy\n", encoding="utf-8")

    result = accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=refresh_root,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    assert len(result["evidence_trend_timeseries"]) == 1
    assert result["evidence_trend_timeseries"][0]["refresh_id"] == (
        fixture["refresh"]["refresh_id"]
    )
    assert {row["reason"] for row in result["source_snapshot"]["excluded_refreshes"]} == {
        "ROLLED_BACK",
        "LEGACY_UNSNAPSHOTTED",
    }

    transaction_path = rolled / "rolling_refresh_transaction.json"
    _write_json(
        transaction_path,
        {"status": "COMMITTED", "outcome_update_id": "rolled-update"},
    )
    validation = accumulation.validate_evidence_trend_artifact(
        trend_id=result["trend_id"],
        output_dir=tmp_path / "evidence_trend",
    )
    assert validation["status"] == "FAIL"


def test_evidence_trend_blocks_invalid_committed_refresh(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    refresh_root = tmp_path / "rolling_evidence_refresh"
    shutil.copytree(fixture["refresh"]["refresh_dir"], refresh_root / "invalid-copy")

    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="committed rolling refresh validation failed",
    ):
        accumulation.run_evidence_trend(
            output_dir=tmp_path / "evidence_trend",
            rolling_refresh_dir=refresh_root,
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )
    assert not (tmp_path / "evidence_trend").exists()


def test_evidence_trend_summary_uses_reviewed_full_state_policy() -> None:
    policy = accumulation._load_evidence_trend_policy(  # noqa: SLF001
        accumulation.DEFAULT_EVIDENCE_TREND_POLICY_PATH
    )
    rows = [
        {
            "forward_available": 1,
            "limited_vs_notrade_confidence": "LOW",
            "limited_vs_notrade_avg_relative_return": None,
            "consensus_target_risk": None,
        },
        {
            "forward_available": 3,
            "limited_vs_notrade_confidence": "MEDIUM",
            "limited_vs_notrade_avg_relative_return": 0.01,
            "consensus_target_risk": "INSUFFICIENT_DATA",
        },
    ]

    summary = accumulation._confidence_trend_summary(rows, policy)  # noqa: SLF001

    assert summary["available_sample_growth"] == 2
    assert summary["confidence_change"] == "IMPROVED"
    assert summary["trend_status"] == "IMPROVING"
    assert summary["limited_vs_notrade_signal"] == "EARLY_POSITIVE"
    assert summary["next_action"] == "continue_tracking"


@pytest.mark.parametrize(
    ("target", "mutate"),
    [
        (
            "confidence_trend_summary.json",
            lambda payload: {**payload, "trend_status": "TAMPERED"},
        ),
        (
            "evidence_trend_source_snapshot.json",
            lambda payload: {**payload, "generated_at": "2020-01-01T00:00:00+00:00"},
        ),
    ],
)
def test_evidence_trend_validator_rejects_output_and_snapshot_tamper(
    tmp_path: Path,
    monkeypatch: Any,
    target: str,
    mutate: Any,
) -> None:
    run_rolling_refresh_fixture(tmp_path, monkeypatch)
    result = accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    path = result["trend_dir"] / target
    payload = json.loads(path.read_text(encoding="utf-8"))
    _write_json(path, mutate(payload))

    assert accumulation.validate_evidence_trend_artifact(
        trend_id=result["trend_id"], output_dir=tmp_path / "evidence_trend"
    )["status"] == "FAIL"


def test_evidence_trend_validator_rejects_policy_and_live_refresh_tamper(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)
    policy_path = tmp_path / "evidence_trend_policy.yaml"
    policy_path.write_text(
        accumulation.DEFAULT_EVIDENCE_TREND_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    result = accumulation.run_evidence_trend(
        output_dir=tmp_path / "evidence_trend",
        rolling_refresh_dir=tmp_path / "rolling_evidence_refresh",
        policy_path=policy_path,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    policy_path.write_text(policy_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert accumulation.validate_evidence_trend_artifact(
        trend_id=result["trend_id"], output_dir=tmp_path / "evidence_trend"
    )["status"] == "FAIL"

    policy_path.write_text(
        accumulation.DEFAULT_EVIDENCE_TREND_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    refresh_report = fixture["refresh"]["refresh_dir"] / "rolling_evidence_refresh_report.md"
    refresh_report.write_text(
        refresh_report.read_text(encoding="utf-8") + "tampered\n",
        encoding="utf-8",
    )
    assert accumulation.validate_evidence_trend_artifact(
        trend_id=result["trend_id"], output_dir=tmp_path / "evidence_trend"
    )["status"] == "FAIL"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
