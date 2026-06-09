from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.weight_calibration_cache import (
    DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION,
    WeightCalibrationCacheError,
    build_price_returns_matrix_cache_lookup,
    build_price_returns_matrix_cache_payload,
    build_weight_calibration_cache_key,
    build_weight_calibration_cache_manifest,
    build_weight_calibration_cache_parallel_validation_report,
    build_weight_calibration_diagnostics_run_manifest,
    build_weight_calibration_performance_report,
    load_weight_calibration_cache_policy_config,
    load_weight_calibration_json_cache_entry,
    resolve_weight_calibration_worker_count,
    run_weight_calibration_parallel_tasks,
    validate_weight_calibration_cache_manifest,
    validate_weight_calibration_cache_parallel_validation_report,
    validate_weight_calibration_diagnostics_run_manifest,
    validate_weight_calibration_performance_report,
    weight_calibration_input_hash,
    write_price_returns_matrix_cache_entry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_weight_calibration_cache_policy_loads_default() -> None:
    policy = load_weight_calibration_cache_policy_config()

    settings = policy.weight_calibration_cache
    assert policy.schema_version == "etf_weight_calibration_cache_policy_v1"
    assert settings.cache_root == "data/cache/weight_calibration"
    assert settings.default_mode == "read_write"
    assert settings.enabled_layers.price_returns_matrix is True
    assert settings.parallel.default_workers == "auto"
    assert settings.parallel.max_workers == 8
    assert settings.safety.production_effect == "none"
    assert (
        resolve_weight_calibration_worker_count("auto", policy=policy, available_cpu_count=16) == 8
    )


def test_weight_calibration_cache_policy_unsafe_safety_fails(tmp_path: Path) -> None:
    raw = _raw_policy()
    raw["weight_calibration_cache"]["safety"]["production_effect"] = "apply_weights"
    path = _write_policy(raw, tmp_path / "unsafe_cache_policy.yaml")

    with pytest.raises(WeightCalibrationCacheError, match="production_effect"):
        load_weight_calibration_cache_policy_config(path)


def test_weight_calibration_cache_key_is_deterministic_and_sensitive() -> None:
    inputs = _candidate_backtest_inputs()

    first = build_weight_calibration_cache_key("candidate_backtest", inputs)
    second = build_weight_calibration_cache_key(
        "candidate-backtest",
        dict(reversed(inputs.items())),
    )
    changed_data = build_weight_calibration_cache_key(
        "candidate_backtest",
        {**inputs, "data_hash": "changed"},
    )
    changed_config = build_weight_calibration_cache_key(
        "candidate_backtest",
        {**inputs, "source_config_hash": "changed"},
    )
    changed_weights = build_weight_calibration_cache_key(
        "candidate_backtest",
        {**inputs, "candidate_weights_hash": "changed"},
    )

    assert first == second
    assert first != changed_data
    assert first != changed_config
    assert first != changed_weights
    with pytest.raises(WeightCalibrationCacheError, match="data_hash"):
        build_weight_calibration_cache_key(
            "candidate_backtest",
            {key: value for key, value in inputs.items() if key != "data_hash"},
        )


def test_weight_calibration_cache_manifest_validates_safety_and_schema() -> None:
    cache_key = build_weight_calibration_cache_key(
        "diagnostics_aggregation",
        _diagnostics_aggregation_inputs(),
    )
    manifest = build_weight_calibration_cache_manifest(
        cache_key=cache_key,
        cache_layer="diagnostics_aggregation",
        source_config_hash="cfg",
        data_hash="data",
        model_version="model",
        engine_version="engine",
        input_summary={"search_ids": ["search"]},
    )

    validated = validate_weight_calibration_cache_manifest(
        manifest,
        expected_cache_key=cache_key,
        expected_cache_layer="diagnostics_aggregation",
        expected_source_config_hash="cfg",
        expected_data_hash="data",
        expected_engine_version="engine",
    )

    assert validated.schema_version == WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION
    unsafe = deepcopy(manifest)
    unsafe["safety"]["broker_action"] = "place_order"
    with pytest.raises(WeightCalibrationCacheError, match="broker_action"):
        validate_weight_calibration_cache_manifest(unsafe)
    wrong_schema = deepcopy(manifest)
    wrong_schema["schema_version"] = "old"
    with pytest.raises(WeightCalibrationCacheError, match="schema_version"):
        validate_weight_calibration_cache_manifest(wrong_schema)


def test_price_returns_matrix_cache_writes_and_loads(tmp_path: Path) -> None:
    prices = pd.DataFrame(
        [
            {"date": "2026-06-01", "symbol": "SPY", "adj_close": 100.0},
            {"date": "2026-06-02", "symbol": "SPY", "adj_close": 101.0},
            {"date": "2026-06-03", "symbol": "SPY", "adj_close": 102.0},
            {"date": "2026-06-01", "symbol": "QQQ", "adj_close": 200.0},
            {"date": "2026-06-02", "symbol": "QQQ", "adj_close": 198.0},
            {"date": "2026-06-03", "symbol": "QQQ", "adj_close": 201.0},
        ]
    )
    lookup = build_price_returns_matrix_cache_lookup(
        prices,
        asset_universe=["SPY", "QQQ"],
        start=date(2026, 6, 1),
        end=date(2026, 6, 2),
        data_source="fixture",
        source_config_hash="cfg",
    )
    changed_range = build_price_returns_matrix_cache_lookup(
        prices,
        asset_universe=["SPY", "QQQ"],
        start=date(2026, 6, 1),
        end=date(2026, 6, 3),
        data_source="fixture",
        source_config_hash="cfg",
    )
    payload = build_price_returns_matrix_cache_payload(
        prices,
        asset_universe=["SPY", "QQQ"],
        start=date(2026, 6, 1),
        end=date(2026, 6, 2),
        data_source="fixture",
        source_config_hash="cfg",
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )

    assert lookup["cache_key"] == payload["cache_key"]
    assert changed_range["cache_key"] != payload["cache_key"]
    paths = write_price_returns_matrix_cache_entry(payload, cache_root=tmp_path / "cache")
    loaded = load_weight_calibration_json_cache_entry(
        cache_root=tmp_path / "cache",
        cache_layer="price_returns_matrix",
        cache_key=payload["cache_key"],
        expected_source_config_hash="cfg",
        expected_data_hash=payload["data_hash"],
        expected_engine_version="etf_weight_calibration_price_returns_matrix_cache_v1",
    )

    assert paths["payload"].exists()
    assert loaded is not None
    assert loaded["payload"]["returns_matrix"][1]["SPY"] == pytest.approx(0.01)
    assert loaded["manifest"]["safety"]["production_effect"] == "none"


def test_parallel_runner_preserves_order_and_captures_exceptions() -> None:
    result = run_weight_calibration_parallel_tasks(
        [
            {"task_id": "first", "value": 1},
            {"task_id": "bad", "value": "bad"},
            {"task_id": "second", "value": 2},
        ],
        _double_worker,
        workers=1,
    )

    assert result["status"] == "PARTIAL"
    assert [item["task_id"] for item in result["results"]] == ["first", "second"]
    assert result["exceptions"][0]["task_id"] == "bad"
    assert result["production_effect"] == "none"


def test_run_manifest_and_performance_report_validate() -> None:
    manifest = build_weight_calibration_diagnostics_run_manifest(
        run_id="run-1",
        status="completed",
        config_hash="cfg",
        data_hash="data",
        planned_steps=["a", "b"],
        completed_steps=["a", "b"],
        started_at=datetime(2026, 6, 4, tzinfo=UTC),
        completed_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    performance = build_weight_calibration_performance_report(
        run_id="run-1",
        total_runtime_seconds=3.0,
        step_runtime_seconds={"a": 1.0, "b": 2.0},
        worker_count=1,
        cache_mode="read-write",
        cache_events=[
            {"cache_layer": "diagnostics_aggregation", "cache_status": "hit"},
            {"cache_layer": "candidate_backtest", "cache_status": "miss"},
            {"cache_layer": "diagnostics_aggregation", "cache_status": "write"},
        ],
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )

    validate_weight_calibration_diagnostics_run_manifest(manifest)
    validate_weight_calibration_performance_report(performance)
    assert performance["cache_hit_rate"] == 0.5
    assert performance["slowest_step"] == "b"


def test_cache_parallel_validation_gate_passes_and_fails_unsafe_policy(
    tmp_path: Path,
) -> None:
    payload = build_weight_calibration_cache_parallel_validation_report(
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    validate_weight_calibration_cache_parallel_validation_report(payload)
    assert payload["status"] == "PASS"
    assert payload["failed_check_count"] == 0

    raw = _raw_policy()
    raw["weight_calibration_cache"]["safety"]["broker_action"] = "place_order"
    unsafe_path = _write_policy(raw, tmp_path / "unsafe_policy.yaml")
    failed = build_weight_calibration_cache_parallel_validation_report(
        policy_config_path=unsafe_path,
        generated_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    assert failed["status"] == "FAIL"
    assert failed["failed_check_count"] >= 1


def test_weight_calibration_performance_validate_cli_writes_report(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "performance-validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight calibration cache / parallel validation" in result.output
    assert "status=PASS" in result.output
    assert list((tmp_path / "validation").glob("*.json"))
    assert list((tmp_path / "validation").glob("*.md"))


def _candidate_backtest_inputs() -> dict[str, object]:
    return {
        "source_config_hash": "cfg",
        "data_hash": "data",
        "engine_version": "engine",
        "candidate_weights_hash": weight_calibration_input_hash({"SPY": 0.5, "QQQ": 0.5}),
        "date_range": {"start": "2022-12-01", "end": "2026-06-02"},
        "returns_matrix_hash": "returns",
        "backtest_engine_version": "backtest",
        "transaction_cost_config_hash": "cost",
        "rebalance_policy_hash": "rebalance",
        "benchmark_set_hash": "benchmarks",
    }


def _diagnostics_aggregation_inputs() -> dict[str, object]:
    return {
        "source_config_hash": "cfg",
        "data_hash": "data",
        "engine_version": "engine",
        "diagnostics_config_hash": "diagnostics",
        "input_search_run_ids": ["run-1"],
        "input_candidate_result_hashes": ["candidate-1"],
        "aggregation_engine_version": "aggregation",
    }


def _raw_policy() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _write_policy(raw: dict[str, object], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return path


def _double_worker(task: dict[str, object]) -> dict[str, object]:
    if task["value"] == "bad":
        raise ValueError("bad task")
    return {"task_id": task["task_id"], "value": int(task["value"]) * 2}
