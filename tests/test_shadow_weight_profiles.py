from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.prediction_ledger import load_prediction_ledger
from ai_trading_system.shadow_weight_profiles import (
    build_shadow_weight_profile_run_report,
    load_shadow_weight_profile_manifest,
)


def test_default_shadow_weight_profile_manifest_is_isolated() -> None:
    manifest, source_profile, source_path = load_shadow_weight_profile_manifest()

    assert manifest.production_effect == "none"
    assert source_path.name == "weight_profile_current.yaml"
    assert len(manifest.profiles) >= 3
    for profile in manifest.profiles:
        assert profile.production_effect == "none"
        assert profile.status == "shadow"
        assert set(profile.target_weights) == set(source_profile.base_weights)
        assert sum(profile.target_weights.values()) == pytest.approx(1.0)


def test_shadow_weight_profile_report_compares_against_production(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(tmp_path)
    manifest_path = _write_manifest(tmp_path)

    report = build_shadow_weight_profile_run_report(
        as_of=date(2026, 5, 14),
        decision_snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        generated_at=datetime.fromisoformat("2026-05-14T12:00:00+00:00"),
    )

    assert report.status == "PASS"
    assert report.production_effect == "none"
    assert len(report.observations) == 1
    observation = report.observations[0]
    assert observation.profile_id == "shadow_test_alpha"
    assert observation.production_score == 70.0
    assert observation.shadow_score != observation.production_score
    assert observation.shadow_final_band["max_position"] == 0.4


def test_shadow_weight_profiles_cli_writes_observations_and_optional_predictions(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(tmp_path)
    trace_path = tmp_path / "trace.json"
    features_path = tmp_path / "features.csv"
    quality_path = tmp_path / "quality.md"
    manifest_path = _write_manifest(tmp_path)
    observation_path = tmp_path / "shadow_observations.csv"
    prediction_ledger_path = tmp_path / "shadow_prediction_ledger.csv"
    report_path = tmp_path / "shadow_weight_profiles.md"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["trace"] = {"trace_bundle_path": str(trace_path)}
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    trace_path.write_text(
        json.dumps(
            {
                "run_manifest": {"run_id": "run:test:2026-05-14"},
                "dataset_refs": [
                    {
                        "dataset_type": "processed_feature_cache",
                        "path": str(features_path),
                    }
                ],
                "quality_refs": [{"report_path": str(quality_path)}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "run-shadow-weight-profiles",
            "--manifest-path",
            str(manifest_path),
            "--decision-snapshot-path",
            str(snapshot_path),
            "--observation-ledger-path",
            str(observation_path),
            "--prediction-ledger-path",
            str(prediction_ledger_path),
            "--as-of",
            "2026-05-14",
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "Shadow weight profile 状态：PASS" in result.output
    observations = pd.read_csv(observation_path)
    assert observations.loc[0, "profile_id"] == "shadow_test_alpha"
    rows = load_prediction_ledger(prediction_ledger_path)
    assert len(rows) == 1
    assert rows[0]["candidate_id"].startswith("shadow_weight_profile:shadow_test_alpha")
    assert rows[0]["production_effect"] == "none"
    assert (
        rows[0]["execution_assumption"]
        == "shadow_weight_profile_no_order_no_position_change"
    )
    assert report_path.exists()


def _write_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "shadow_weight_profiles.yaml"
    manifest_path.write_text(
        """
version: shadow_weight_profiles_test
status: pilot
owner: system
production_effect: none
source_weight_profile_path: config/weights/weight_profile_current.yaml
label_horizon_days: 20
rationale: test manifest
review_after_reports: 3
profiles:
  - profile_id: shadow_test_alpha
    version: v1
    status: shadow
    owner: system
    production_effect: none
    rationale: test profile
    review_after_reports: 3
    target_weights:
      trend: 0.30
      fundamentals: 0.30
      macro_liquidity: 0.125
      risk_sentiment: 0.125
      valuation: 0.075
      policy_geopolitics: 0.075
""".lstrip(),
        encoding="utf-8",
    )
    return manifest_path


def _write_snapshot(tmp_path: Path) -> Path:
    snapshot_path = tmp_path / "decision_snapshot_2026-05-14.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "signal_date": "2026-05-14",
                "generated_at": "2026-05-14T21:00:00+00:00",
                "market_regime": {"regime_id": "ai_after_chatgpt"},
                "scores": {
                    "overall_score": 70.0,
                    "components": [
                        {"component": "trend", "score": 90.0},
                        {"component": "fundamentals", "score": 80.0},
                        {"component": "macro_liquidity", "score": 40.0},
                        {"component": "risk_sentiment", "score": 50.0},
                        {"component": "valuation", "score": 35.0},
                        {"component": "policy_geopolitics", "score": 60.0},
                    ],
                },
                "positions": {
                    "model_risk_asset_ai_band": {
                        "label": "偏重仓",
                        "min_position": 0.6,
                        "max_position": 0.8,
                    },
                    "final_risk_asset_ai_band": {
                        "label": "偏重仓/仓位受限",
                        "min_position": 0.4,
                        "max_position": 0.4,
                    },
                    "position_gates": [
                        {
                            "gate_id": "score_model",
                            "label": "评分模型仓位",
                            "max_position": 0.8,
                        },
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "max_position": 0.4,
                        },
                    ],
                },
                "rule_versions": {"rules": [{"rule_id": "scoring.weighted_score.v1"}]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return snapshot_path
