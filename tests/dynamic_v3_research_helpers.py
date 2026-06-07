from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
    WEIGHT_PATH_COMPLETE,
    run_parameter_sweep,
)


def tiny_config_path(tmp_path: Path) -> Path:
    raw = yaml.safe_load(DEFAULT_PARAMETER_SWEEP_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["run"]["max_candidates"] = 12
    raw["data"]["quality_status"] = "PASS"
    raw["data"]["manifest_hash"] = "tiny_medium_real_test_manifest"
    raw["parameter_space"] = {
        "rescue_intensity": {"values": [0.50, 0.75, 1.00]},
        "smooth_window_days": {"values": [3, 10]},
        "constraint_buffer_bps": {"values": [0, 50]},
        "turnover_penalty": {"values": [0.20]},
        "risk_off_confirmation_days": {"values": [1]},
        "rebalance_cooldown_days": {"values": [10]},
        "drawdown_guard": {"values": ["hard"]},
    }
    raw["hard_constraints"]["max_drawdown_degradation_pp"] = 0.01
    raw["hard_constraints"]["max_dynamic_vs_static_gap"] = 0.21
    raw["hard_constraints"]["noise_floor_improvement"] = 0.005
    path = tmp_path / "parameter_sweep_tiny.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def prepared_real_like_sweep(tmp_path: Path) -> dict[str, Any]:
    sweep_output_dir = tmp_path / "sweeps"
    result = run_parameter_sweep(
        config_path=tiny_config_path(tmp_path), output_dir=sweep_output_dir
    )
    sweep_id = result["sweep_id"]
    sweep_dir = sweep_output_dir / sweep_id
    results_path = sweep_dir / "candidate_results.jsonl"
    rows = [
        json.loads(line)
        for line in results_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    patched_rows = []
    for row in rows:
        candidate_id = row["candidate_id"]
        real_dir = sweep_dir / "real_evaluation" / candidate_id
        real_dir.mkdir(parents=True, exist_ok=True)
        real_path = real_dir / "real_evaluation.json"
        real_path.write_text(
            json.dumps(
                {
                    "report_type": "etf_dynamic_v3_real_evaluation_report",
                    "dynamic_v3_real_evaluation_report_id": f"eval_{candidate_id}",
                    "candidate_id": candidate_id,
                    "requested_range": {"start": "2022-12-01", "end": "2026-06-04"},
                    "daily_path_summary": {
                        "first_signal_date": "2022-12-01",
                        "last_signal_date": "2026-06-04",
                        "row_count": 2,
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (real_dir / "weight_path_metadata.json").write_text(
            json.dumps(
                {
                    "candidate_id": candidate_id,
                    "evaluation_id": f"eval_{candidate_id}",
                    "has_daily_weights": True,
                    "attribution_completeness": WEIGHT_PATH_COMPLETE,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(
            [
                {
                    "date": "2022-12-01",
                    "SPY": 0.30,
                    "QQQ": 0.35,
                    "SMH": 0.20,
                    "SOXX": 0.10,
                    "CASH": 0.05,
                },
                {
                    "date": "2026-06-04",
                    "SPY": 0.28,
                    "QQQ": 0.37,
                    "SMH": 0.18,
                    "SOXX": 0.12,
                    "CASH": 0.05,
                },
            ]
        ).to_csv(real_dir / "daily_weights.csv", index=False)
        row["evaluator_mode"] = EVALUATOR_REAL_DYNAMIC_V3_RESCUE
        row["evaluator_version"] = "real_dynamic_v3_rescue_v1"
        row["metrics_source"] = "real_evaluation_artifact"
        row["not_for_investment_decision"] = False
        row["real_evaluation_artifact_path"] = str(real_path)
        row["data_quality"] = {
            "status": "PASS",
            "report_path": str(tmp_path / "data_quality.md"),
            "source": "validate_data_cache",
        }
        row["metrics"]["data_quality"] = "PASS"
        row["metrics"]["date_range_status"] = "PASS"
        row["metrics"]["weight_path_status"] = WEIGHT_PATH_COMPLETE
        row["metrics"]["overfit_status"] = "LOW_RISK"
        row["backtest_window"] = {
            "date_range_status": "PASS",
            "requested_start": "2022-12-01",
            "requested_end": "2026-06-04",
            "earliest_actual_evaluation_start": "2022-12-01",
        }
        row["weight_path_metadata"] = {
            "candidate_id": candidate_id,
            "metadata_path": str(real_dir / "weight_path_metadata.json"),
            "has_daily_weights": True,
            "attribution_completeness": WEIGHT_PATH_COMPLETE,
        }
        patched_rows.append(row)
    results_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in patched_rows) + "\n",
        encoding="utf-8",
    )
    manifest_path = sweep_dir / "sweep_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        {
            "profile": "medium_real",
            "evaluator_mode": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
            "evaluator_version": "real_dynamic_v3_rescue_v1",
            "not_for_investment_decision": False,
            "data_quality": {"status": "PASS", "source": "validate_data_cache"},
            "backtest_window": {"date_range_status": "PASS"},
        }
    )
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return {"sweep_id": sweep_id, "sweep_dir": sweep_dir, "sweep_output_dir": sweep_output_dir}


def write_candidate_evidence(tmp_path: Path, sweep: dict[str, Any]) -> dict[str, Path]:
    attribution_dir = tmp_path / "candidate_attribution"
    overfit_dir = tmp_path / "overfit"
    for row in _candidate_rows(sweep["sweep_dir"]):
        candidate_id = row["candidate_id"]
        candidate_attr_dir = attribution_dir / candidate_id
        candidate_attr_dir.mkdir(parents=True, exist_ok=True)
        (candidate_attr_dir / "attribution_manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_id": candidate_id,
                    "source_sweep_id": sweep["sweep_id"],
                    "status": WEIGHT_PATH_COMPLETE,
                    "attribution_completeness": WEIGHT_PATH_COMPLETE,
                    "safety": {"production_candidate_generated": False},
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        candidate_overfit_dir = overfit_dir / f"overfit_{candidate_id}"
        candidate_overfit_dir.mkdir(parents=True, exist_ok=True)
        (candidate_overfit_dir / "overfit_manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_id": candidate_id,
                    "source_sweep_id": sweep["sweep_id"],
                    "overfit_id": f"overfit_{candidate_id}",
                    "overfit_status": "LOW_RISK",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    provenance_dir = tmp_path / "data_provenance"
    provenance_dir.mkdir()
    (provenance_dir / "price_cache_provenance_report.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "provenance_status": "ORIGINAL_OR_VENDOR",
                "download_manifest_status": "AVAILABLE",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    window_dir = tmp_path / "window_audit" / "window_ok"
    window_dir.mkdir(parents=True)
    (window_dir / "window_audit_manifest.json").write_text(
        json.dumps(
            {
                "window_audit_id": "window_ok",
                "status": "PASS",
                "date_range_status": "PASS",
                "promotion_blocking": False,
                "promotion_blocking_count": 0,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {
        "candidate_attribution_dir": attribution_dir,
        "overfit_dir": overfit_dir,
        "data_provenance_dir": provenance_dir,
        "window_audit_dir": tmp_path / "window_audit",
    }


def write_regime_price_cache(tmp_path: Path) -> Path:
    dates = pd.bdate_range("2022-12-01", "2026-06-04")
    rows = []
    for ticker, drift, shock_day in [
        ("QQQ", 0.001, 180),
        ("SMH", 0.0012, 220),
        ("SOXX", 0.0011, 220),
    ]:
        level = 100.0
        for idx, day in enumerate(dates):
            level *= 1.0 + drift
            if shock_day <= idx < shock_day + 20:
                level *= 0.99
            if shock_day + 20 <= idx < shock_day + 45:
                level *= 1.006
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "open": level,
                    "high": level * 1.01,
                    "low": level * 0.99,
                    "close": level,
                    "adj_close": level,
                    "volume": 1_000_000,
                }
            )
    path = tmp_path / "prices_daily.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def top_candidate_id(sweep: dict[str, Any]) -> str:
    rows = [row for row in _candidate_rows(sweep["sweep_dir"]) if row.get("score") is not None]
    rows.sort(key=lambda row: row["score"], reverse=True)
    return rows[0]["candidate_id"]


def _candidate_rows(sweep_dir: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (sweep_dir / "candidate_results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
