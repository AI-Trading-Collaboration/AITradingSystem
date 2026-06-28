from __future__ import annotations

import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.vendor_adapters import norgate_partial_evidence_review as review
from ai_trading_system.vendor_adapters.norgate_partial_evidence_review import (
    run_norgate_2y_partial_evidence_review,
)


def test_partial_evidence_review_explains_none_without_unlocking_gates(
    tmp_path: Path,
) -> None:
    feature_path, coverage_path, local_signal_path, conclusion_path = _write_2267_artifacts(
        tmp_path
    )
    dates = pd.bdate_range("2024-06-28", periods=190)
    benchmark_frames = {
        symbol: pd.DataFrame({"Close": [100.0 for _ in dates]}, index=dates)
        for symbol in ("QQQ", "SPY", "SMH")
    }

    final = run_norgate_2y_partial_evidence_review(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        policy_path=review.DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
        coverage_path=coverage_path,
        feature_csv_path=feature_path,
        local_signal_path=local_signal_path,
        prior_conclusion_path=conclusion_path,
        benchmark_price_frames=benchmark_frames,
    )

    assert final["status"] == "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_READY"
    assert final["local_signal_evidence_reason"] == "no_incremental_value"
    assert final["trial_2y_feature_value"] == "weak"
    assert final["full_history_needed_for_final_answer"] is True
    assert final["purchase_platinum_recommendation"] == "yes"
    assert final["purchase_rationale"] == "stress_window_required"
    assert final["primary_window_validated"] is False
    assert final["model_ready_for_2021_primary_window"] is False
    assert final["reopen_gate_allowed"] is False
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"
    assert final["purchase_allowed_without_owner_approval"] is False
    assert (tmp_path / "docs" / "norgate_2y_partial_evidence_review.md").exists()
    assert (tmp_path / "docs" / "norgate_platinum_decision_memo.md").exists()
    assert (tmp_path / "outputs" / "norgate_2y_benchmark_consistency.csv").exists()


def test_partial_evidence_review_artifacts_have_audit_metadata(tmp_path: Path) -> None:
    feature_path, coverage_path, local_signal_path, conclusion_path = _write_2267_artifacts(
        tmp_path
    )
    dates = pd.bdate_range("2024-06-28", periods=190)
    benchmark_frames = {
        symbol: pd.DataFrame({"Close": [100.0 for _ in dates]}, index=dates)
        for symbol in ("QQQ", "SPY", "SMH")
    }

    run_norgate_2y_partial_evidence_review(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        policy_path=review.DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
        coverage_path=coverage_path,
        feature_csv_path=feature_path,
        local_signal_path=local_signal_path,
        prior_conclusion_path=conclusion_path,
        benchmark_price_frames=benchmark_frames,
    )

    schema = load_research_audit_metadata_schema()
    for path in (
        tmp_path / "inputs" / "norgate_2y_partial_evidence_review.yaml",
        tmp_path / "inputs" / "norgate_platinum_decision_memo.yaml",
        tmp_path / "inputs" / "norgate_2y_partial_evidence_conclusion_matrix.yaml",
    ):
        artifact = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert artifact["promotion_allowed"] is False
        assert artifact["broker_action"] == "none"


def test_partial_evidence_review_fails_closed_when_2267_artifacts_missing(
    tmp_path: Path,
) -> None:
    final = run_norgate_2y_partial_evidence_review(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        policy_path=review.DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
        coverage_path=tmp_path / "missing_coverage.json",
        feature_csv_path=tmp_path / "missing_features.csv",
        local_signal_path=tmp_path / "missing_signal.json",
        prior_conclusion_path=tmp_path / "missing_conclusion.json",
        benchmark_price_frames={},
    )

    assert final["status"] == "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_BLOCKED"
    assert final["local_signal_evidence_reason"] == "inconclusive"
    assert final["purchase_platinum_recommendation"] == "defer"
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False


def test_partial_evidence_review_cli_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["data", "norgate", "partial-evidence-review", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "partial evidence review" in result.output.lower()


def _write_2267_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    dates = pd.bdate_range("2024-06-28", periods=180)
    rows: list[dict[str, Any]] = []
    for index, day in enumerate(dates):
        pct50 = 0.5 + 0.35 * _sin_like(index)
        previous = 0.5 + 0.35 * _sin_like(index - 20) if index >= 20 else None
        rows.append(
            {
                "date": day.date().isoformat(),
                "pct_above_ma20": 0.5 + 0.3 * _cos_like(index),
                "pct_above_ma50": pct50,
                "pct_above_ma200": 0.5 + 0.25 * _sin_like(index + 7),
                "equal_weight_return": 0.0,
                "cap_weight_proxy_return": 0.0,
                "advance_decline_proxy": 0.0,
                "breadth_momentum": pct50 - previous if previous is not None else None,
            }
        )
    feature_path = tmp_path / "norgate_trial_breadth_feature_report_2y.csv"
    pd.DataFrame(rows).to_csv(feature_path, index=False)
    coverage_path = tmp_path / "coverage.json"
    _write_json(
        coverage_path,
        {
            "earliest_price_date": "2024-06-28",
            "latest_price_date": "2025-03-06",
            "member_day_coverage_ratio": 1.0,
            "missing_price_ratio": 0.0,
            "failed_join_count": 0,
        },
    )
    local_signal_path = tmp_path / "local_signal.json"
    _write_json(local_signal_path, _local_signal_payload())
    conclusion_path = tmp_path / "conclusion.json"
    _write_json(
        conclusion_path,
        {
            "source_engineering_useful": True,
            "source_feature_useful_2y": "weak",
            "purchase_platinum_evidence_strength": "moderate",
            "engineering_validated": True,
            "feature_numeric_validated": True,
            "local_signal_evidence": "none",
            "primary_window_start": "2021-02-22",
        },
    )
    return feature_path, coverage_path, local_signal_path, conclusion_path


def _local_signal_payload() -> dict[str, Any]:
    return {
        "breadth_bucket_vs_forward_return": [
            {"breadth_bucket": "low", "sample_count": 60},
            {"breadth_bucket": "mid", "sample_count": 60},
            {"breadth_bucket": "high", "sample_count": 60},
        ],
        "breadth_deterioration_vs_future_drawdown": [
            {"breadth_deterioration": False, "sample_count": 120},
            {"breadth_deterioration": True, "sample_count": 60},
        ],
        "baseline_comparison": {
            "baseline_first_layer_proxy": {
                "risk_off_day_count": 40,
                "future_drawdown_event_capture_ratio": 0.4,
            },
            "baseline_plus_breadth": {
                "risk_off_day_count": 90,
                "future_drawdown_event_capture_ratio": 0.41,
            },
            "false_risk_off_delta": 0.15,
            "false_risk_on_delta": 0.06,
        },
    }


def _sin_like(index: int) -> float:
    return math.sin(index / 9)


def _cos_like(index: int) -> float:
    return math.cos(index / 11)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
