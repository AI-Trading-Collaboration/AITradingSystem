from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.weight_calibration_profiling import (
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    WeightCalibrationProfilingError,
    WeightCalibrationRuntimeProfiler,
    build_cache_timing_breakdown,
    build_regime_mask_precomputation_assessment,
    build_vectorization_audit_report,
    build_weight_calibration_candidate_hotspot_table,
    build_weight_calibration_profiling_report,
    build_weight_calibration_profiling_validation_report,
    build_worker_timing_breakdown,
    load_weight_calibration_profiling_policy_config,
    normalize_weight_calibration_profile_mode,
    run_with_optional_cprofile,
    validate_weight_calibration_profiling_validation_report,
    write_cprofile_artifacts,
    write_weight_calibration_candidate_hotspot_table,
    write_weight_calibration_profiling_report,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_weight_calibration_profiling_policy_loads_and_rejects_invalid_mode(
    tmp_path: Path,
) -> None:
    policy = load_weight_calibration_profiling_policy_config()

    settings = policy.weight_calibration_profiling
    assert policy.schema_version == "etf_weight_calibration_profiling_policy_v1"
    assert settings.default_mode == "summary"
    assert settings.modes["summary"].step_timing is True
    assert settings.modes["detailed"].candidate_timing is True
    assert settings.modes["cprofile"].cprofile is True
    assert settings.safety.production_effect == "none"
    assert normalize_weight_calibration_profile_mode("c-profile", policy=policy) == "cprofile"
    with pytest.raises(WeightCalibrationProfilingError, match="invalid profiling mode"):
        normalize_weight_calibration_profile_mode("turbo", policy=policy)

    raw = _raw_policy()
    raw["weight_calibration_profiling"]["safety"]["production_effect"] = "apply_weights"
    path = _write_json_policy(raw, tmp_path / "unsafe_profiling_policy.yaml")
    with pytest.raises(WeightCalibrationProfilingError, match="production_effect"):
        load_weight_calibration_profiling_policy_config(path)


def test_step_profiler_records_slow_and_failed_steps() -> None:
    policy = load_weight_calibration_profiling_policy_config()
    profiler = WeightCalibrationRuntimeProfiler(mode="summary", policy=policy)
    profiler.record_step("candidate_backtest", duration_seconds=61.0)
    with pytest.raises(RuntimeError):
        with profiler.profile_step("failed_step"):
            raise RuntimeError("boom")

    records = profiler.records()
    assert records[0]["warning_if_slow"] is True
    assert records[1]["status"] == "failed"
    assert profiler.summary()["slowest_steps"][0]["step_id"] == "candidate_backtest"


def test_candidate_hotspot_table_outputs_json_csv_markdown(tmp_path: Path) -> None:
    payload = build_weight_calibration_candidate_hotspot_table(_diagnostics_payload(), top_n=2)
    paths = write_weight_calibration_candidate_hotspot_table(
        payload,
        output_dir=tmp_path / "profiling",
    )

    assert payload["hotspots"][0]["candidate_id"] == "weight_set_0002"
    assert payload["hotspots"][0]["cache_status"] == "miss_written"
    assert paths["json"].exists()
    assert "weight_set_0002" in paths["csv"].read_text(encoding="utf-8")
    assert "ETF Weight Calibration Candidate Hotspots" in paths[
        "markdown"
    ].read_text(encoding="utf-8")


def test_cprofile_mode_creates_top_function_artifacts(tmp_path: Path) -> None:
    result, profiler = run_with_optional_cprofile(_sample_profiled_function, enabled=True)
    assert result == 30
    assert profiler is not None

    paths = write_cprofile_artifacts(profiler, output_dir=tmp_path / "profile", top_n=5)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))

    assert paths["stats"].exists()
    assert payload["functions"]
    assert "ETF Weight Calibration cProfile Top Functions" in paths[
        "markdown"
    ].read_text(encoding="utf-8")


def test_cache_worker_vectorization_and_regime_assessments() -> None:
    diagnostics = _diagnostics_payload()
    cache = build_cache_timing_breakdown(
        diagnostics["profiling"]["cache_events"],
        slow_entry_seconds=0.5,
    )
    worker = build_worker_timing_breakdown(diagnostics["profiling"]["candidate_timings"])
    audit = build_vectorization_audit_report(diagnostics)
    regime = build_regime_mask_precomputation_assessment(diagnostics)

    candidate_cache = next(
        row for row in cache["cache_layers"] if row["cache_layer"] == "candidate_backtest"
    )
    assert candidate_cache["hit_count"] == 1
    assert candidate_cache["miss_count"] == 1
    assert worker["workers"][0]["worker_id"] == "main"
    assert any(
        row["recommended_action"] == "already_vectorized" for row in audit["areas"]
    )
    assert any(row["recommend_precompute"] is True for row in regime["regimes"])
    assert all(row["native_extension_needed"] is False for row in audit["areas"])


def test_profiling_report_generator_and_validation_cli(tmp_path: Path) -> None:
    policy = load_weight_calibration_profiling_policy_config()
    report = build_weight_calibration_profiling_report(
        _diagnostics_payload(),
        policy=policy,
        profile_mode="detailed",
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    paths = write_weight_calibration_profiling_report(
        report,
        output_dir=tmp_path / "profiling",
    )

    assert report["production_effect"] == "none"
    assert report["candidate_hotspots"]["hotspots"]
    assert report["optimization_recommendations"]
    assert "native_extension_not_recommended" in {
        row["category"] for row in report["optimization_recommendations"]
    }
    assert paths["json"].exists()
    assert "ETF Weight Calibration Profiling Report" in paths[
        "markdown"
    ].read_text(encoding="utf-8")

    validation = build_weight_calibration_profiling_validation_report(
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    validate_weight_calibration_profiling_validation_report(validation)
    assert validation["status"] == "PASS"

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "profiling-validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert result.exit_code == 0, result.output
    assert "ETF weight calibration profiling validation" in result.output
    assert "status=PASS" in result.output
    assert list((tmp_path / "validation").glob("*.json"))
    assert list((tmp_path / "validation").glob("*.md"))


def test_profiling_validation_fails_unsafe_policy(tmp_path: Path) -> None:
    raw = _raw_policy()
    raw["weight_calibration_profiling"]["safety"]["broker_action"] = "place_order"
    unsafe_path = _write_json_policy(raw, tmp_path / "unsafe_policy.yaml")

    payload = build_weight_calibration_profiling_validation_report(
        policy_config_path=unsafe_path,
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    assert payload["failed_check_count"] >= 1


def _diagnostics_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_weight_search_diagnostics_v1",
        "report_type": "etf_weight_search_diagnostics",
        "run_manifest": {"run_id": "profile-test-run"},
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": "PASS",
        "candidate_observation_count": 2,
        "preset_results": [
            {
                "search_id": "etf_initial_weight_search_v1",
                "preset_id": "ai_cycle_recent",
                "regime_failure_summary": {"risk_on": 0},
            }
        ],
        "profiling": {
            "total_runtime_seconds": 10.0,
            "step_timings": [
                {
                    "step_id": "candidate_backtest",
                    "duration_seconds": 8.0,
                    "status": "completed",
                    "warning_if_slow": False,
                },
                {
                    "step_id": "cache_write",
                    "duration_seconds": 1.0,
                    "status": "completed",
                    "warning_if_slow": False,
                },
            ],
            "candidate_timings": [
                {
                    "candidate_id": "weight_set_0001",
                    "search_id": "etf_initial_weight_search_v1",
                    "preset_id": "ai_cycle_recent",
                    "weights_hash": "hash-1",
                    "backtest_seconds": 0.0,
                    "regime_seconds": 0.0,
                    "overfit_seconds": 0.1,
                    "total_candidate_seconds": 0.1,
                    "cache_status": "hit",
                    "worker_id": "main",
                    "rank": 2,
                    "readiness_status": "needs_manual_review",
                    "overfit_risk": "medium",
                    "status": "completed",
                },
                {
                    "candidate_id": "weight_set_0002",
                    "search_id": "etf_initial_weight_search_v1",
                    "preset_id": "ai_cycle_recent",
                    "weights_hash": "hash-2",
                    "backtest_seconds": 4.0,
                    "regime_seconds": 1.0,
                    "overfit_seconds": 0.2,
                    "total_candidate_seconds": 5.2,
                    "cache_status": "miss_written",
                    "worker_id": "pid:2",
                    "rank": 1,
                    "readiness_status": "shadow_ready",
                    "overfit_risk": "low",
                    "status": "completed",
                },
            ],
            "cache_events": [
                {
                    "cache_layer": "candidate_backtest",
                    "cache_status": "hit",
                    "duration_seconds": 0.01,
                },
                {
                    "cache_layer": "candidate_backtest",
                    "cache_status": "miss",
                    "duration_seconds": 0.02,
                },
                {
                    "cache_layer": "candidate_backtest",
                    "cache_status": "write",
                    "duration_seconds": 0.03,
                    "serialization_seconds": 0.03,
                },
            ],
            "worker_timing": {
                "workers": [
                    {
                        "worker_id": "main",
                        "assigned_candidate_count": 1,
                        "completed_candidate_count": 1,
                        "failed_candidate_count": 0,
                        "runtime_seconds": 0.1,
                        "mean_candidate_seconds": 0.1,
                        "max_candidate_seconds": 0.1,
                        "cache_hit_count": 1,
                        "cache_miss_count": 0,
                    }
                ],
                "worker_count": 1,
            },
            "regime_mask_timings": [
                {
                    "regime_id": "risk_on",
                    "date_range": {"start": "2022-12-01", "end": "2026-06-03"},
                    "mask_build_seconds": 0.25,
                    "mask_reuse_count": 3,
                    "candidate_count": 2,
                }
            ],
        },
        "safety": {
            "observe_only": True,
            "candidate_only": True,
            "production_effect": "none",
            "broker_action": "none",
            "manual_review_required": True,
        },
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _sample_profiled_function() -> int:
    return sum(index * index for index in range(5))


def _raw_policy() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _write_json_policy(raw: dict[str, object], path: Path) -> Path:
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return path
