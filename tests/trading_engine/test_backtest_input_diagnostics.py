from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    REQUIRED_SIGNAL_SNAPSHOTS,
    run_backtest_input_diagnostics,
)

ALL_ASSETS = ("QQQ", "SMH", "NVDA", "TSM", "MSFT", "GOOGL", "BRK.B", "SGOV")


def test_diagnostics_reports_required_asset_missing(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, missing_assets=("BRK.B", "SGOV"))

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    asset_coverage = run.payload["checks"]["asset_coverage"]
    assert run.payload["summary"]["overall_status"] == "FAILED"
    assert asset_coverage["status"] == "FAILED"
    assert asset_coverage["missing_assets"] == ["BRK.B", "SGOV"]


def test_diagnostics_reports_insufficient_history(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, days=4, min_history_days=8)

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    date_coverage = run.payload["checks"]["date_coverage"]
    assert date_coverage["status"] == "INSUFFICIENT_DATA"
    assert date_coverage["missing_start_gap_days"] > 0
    assert run.payload["summary"]["can_run_shadow_backtest"] is False


def test_diagnostics_reports_stale_cache(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        downloaded_at="2026-05-20T00:00:00+00:00",
        generated_at=datetime(2026, 5, 29, tzinfo=UTC),
    )

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    freshness = run.payload["checks"]["cache_freshness"]
    price_item = next(item for item in freshness["items"] if item["name"] == "price_data")
    assert freshness["status"] == "STALE"
    assert price_item["status"] == "STALE"
    assert run.payload["summary"]["overall_status"] == "FAILED"


def test_diagnostics_reports_missing_signal_snapshot_as_limited(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, write_signal_snapshots=False)

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    signals = run.payload["checks"]["signal_snapshots"]
    assert signals["status"] == "LIMITED"
    assert "macro_liquidity" in signals["missing_signals"]
    assert signals["fallback_mode"] == "price_only_shadow_backtest"
    assert run.payload["summary"]["overall_status"] == "LIMITED"
    assert run.payload["summary"]["backtest_mode"] == "price_only_shadow_backtest"
    assert run.payload["summary"]["can_run_shadow_backtest"] is True
    assert run.payload["summary"]["can_promote_candidate"] is False


def test_diagnostics_reports_price_missing_ratio_too_high(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, sparse_assets=("TSM",))

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    price_data = run.payload["checks"]["price_data"]
    tsm = next(asset for asset in price_data["assets"] if asset["symbol"] == "TSM")
    assert price_data["status"] == "LIMITED"
    assert tsm["status"] == "FAILED"
    assert tsm["missing_ratio"] > 0.02


def test_diagnostics_all_data_ok(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path)

    run = run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        generated_at=fixture["generated_at"],
    )

    assert run.payload["summary"]["overall_status"] == "OK"
    assert run.payload["summary"]["can_run_shadow_backtest"] is True
    assert run.payload["summary"]["can_promote_candidate"] is True
    assert run.payload["summary"]["backtest_mode"] == "full_signal_backtest"
    assert run.payload["checks"]["asset_coverage"]["status"] == "OK"
    assert run.payload["checks"]["signal_snapshots"]["status"] == "OK"


def test_dashboard_reads_latest_backtest_data_quality(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_diagnostic_artifact(tmp_path, as_of, status="FAILED")
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["backtest_data_quality"]
    assert card["exists"] is True
    assert card["overall_status"] == "FAILED"
    assert card["asset_coverage_status"] == "FAILED"
    assert card["can_run_shadow_backtest"] is False
    assert card["can_promote_candidate"] is False
    assert "Backtest Data Quality" in html
    assert "Data Quality" in html


def test_reader_brief_displays_parameter_shadow_data_quality_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    as_of = date(2026, 5, 29)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_diagnostic_artifact(tmp_path, as_of, status="FAILED")
    _write_shadow_summary(tmp_path, as_of, data_quality_status="FAILED")
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["data_quality_status"] == "FAILED"
    assert "required price history is missing for BRK.B" in review["data_quality_summary"]


def _write_fixture(
    tmp_path: Path,
    *,
    days: int = 12,
    min_history_days: int = 8,
    missing_assets: tuple[str, ...] = (),
    sparse_assets: tuple[str, ...] = (),
    write_signal_snapshots: bool = True,
    downloaded_at: str = "2026-05-29T00:00:00+00:00",
    generated_at: datetime = datetime(2026, 5, 29, tzinfo=UTC),
) -> dict[str, object]:
    as_of = date(2026, 5, 29)
    start = as_of - timedelta(days=days - 1)
    dates = tuple(start + timedelta(days=offset) for offset in range(days))
    data_dir = tmp_path / "data" / "raw"
    config_dir = tmp_path / "config"
    output_root = tmp_path / "artifacts"
    signal_dir = tmp_path / "artifacts" / "signal_snapshots" / as_of.isoformat()
    prices_path = data_dir / "prices_daily.csv"
    rates_path = data_dir / "rates_daily.csv"
    secondary_prices_path = data_dir / "prices_marketstack_daily.csv"
    manifest_path = data_dir / "download_manifest.csv"
    baseline_path = config_dir / "parameters" / "production" / "current.yaml"
    promotion_path = config_dir / "parameters" / "promotion" / "promotion_rules.yaml"
    config_path = config_dir / "parameters" / "shadow" / "shadow_backtest.yaml"

    _write_prices(prices_path, dates, missing_assets=missing_assets, sparse_assets=sparse_assets)
    _write_prices(
        secondary_prices_path,
        dates,
        missing_assets=missing_assets,
        sparse_assets=sparse_assets,
    )
    _write_rates(rates_path, dates)
    _write_manifest(manifest_path, [prices_path, rates_path, secondary_prices_path], downloaded_at)
    _write_yaml(baseline_path, _baseline_payload())
    _write_yaml(promotion_path, {"version": "promotion-test"})
    if write_signal_snapshots:
        signal_dir.mkdir(parents=True, exist_ok=True)
        for signal in REQUIRED_SIGNAL_SNAPSHOTS:
            (signal_dir / f"{signal}.json").write_text("{}", encoding="utf-8")
    _write_yaml(
        config_path,
        _shadow_config_payload(
            prices_path=prices_path,
            rates_path=rates_path,
            secondary_prices_path=secondary_prices_path,
            manifest_path=manifest_path,
            baseline_path=baseline_path,
            promotion_path=promotion_path,
            signal_dir=signal_dir.parent,
            output_root=output_root,
            min_history_days=min_history_days,
        ),
    )
    return {
        "as_of": as_of,
        "config_path": config_path,
        "output_root": output_root,
        "generated_at": generated_at,
    }


def _write_prices(
    path: Path,
    dates: tuple[date, ...],
    *,
    missing_assets: tuple[str, ...],
    sparse_assets: tuple[str, ...],
) -> None:
    rows: list[dict[str, object]] = []
    for offset, current in enumerate(dates):
        for asset_index, asset in enumerate(ALL_ASSETS):
            if asset in missing_assets:
                continue
            if asset in sparse_assets and offset % 3:
                continue
            close = 100.0 + asset_index + offset * 0.1
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": asset,
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000 + offset,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rates(path: Path, dates: tuple[date, ...]) -> None:
    rows = [
        {"date": current.isoformat(), "series": series, "value": 4.0}
        for current in dates
        for series in ("DGS2", "DGS10", "DTWEXBGS")
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_manifest(path: Path, files: list[Path], downloaded_at: str) -> None:
    rows = [
        {
            "downloaded_at": downloaded_at,
            "source_id": file_path.stem,
            "provider": "unit_test",
            "endpoint": "fixture",
            "request_parameters": "{}",
            "output_path": str(file_path),
            "row_count": len(pd.read_csv(file_path)),
            "checksum_sha256": _sha256(file_path),
        }
        for file_path in files
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _baseline_payload() -> dict[str, object]:
    return {
        "version": "production-test",
        "created_at": "2026-05-29T00:00:00+00:00",
        "owner": "tests",
        "status": "pilot_baseline",
        "production_effect": "production",
        "rationale": "unit test baseline",
        "asset_universe": {"core": list(ALL_ASSETS) + ["CASH"]},
        "decision_frequency": "daily",
        "rebalance_frequency": "weekly",
        "risk_profile": "balanced_growth",
        "weights": {
            "macro_liquidity": 0.20,
            "trend_momentum": 0.25,
            "sector_strength": 0.20,
            "earnings_quality": 0.15,
            "valuation_risk": 0.10,
            "event_risk": 0.10,
        },
        "position_limits": {
            "max_single_asset_weight": 0.30,
            "max_sector_weight": 0.60,
            "min_cash_weight": 0.05,
        },
    }


def _shadow_config_payload(
    *,
    prices_path: Path,
    rates_path: Path,
    secondary_prices_path: Path,
    manifest_path: Path,
    baseline_path: Path,
    promotion_path: Path,
    signal_dir: Path,
    output_root: Path,
    min_history_days: int,
) -> dict[str, object]:
    return {
        "version": "shadow-test",
        "owner": "tests",
        "status": "pilot",
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "observe_only": True,
        "rationale": "test",
        "intended_effect": "test",
        "validation_evidence": "tests",
        "review_condition": "test",
        "market_regime": {
            "id": "ai_after_chatgpt",
            "anchor_event": "ChatGPT public launch",
            "anchor_date": "2022-11-30",
            "default_backtest_start": "2022-12-01",
        },
        "data": {
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "download_manifest_path": str(manifest_path),
            "secondary_prices_path": str(secondary_prices_path),
            "data_quality_report_dir": str(output_root / "reports"),
            "signal_snapshot_dir": str(signal_dir),
        },
        "baseline_parameters_path": str(baseline_path),
        "promotion_rules_path": str(promotion_path),
        "output": {
            "shadow_backtest_dir": str(output_root / "shadow_backtest"),
            "shadow_parameters_dir": str(output_root / "shadow_parameters"),
            "candidate_parameters_dir": str(output_root / "candidate_parameters"),
            "parameter_promotion_dir": str(output_root / "parameter_promotion"),
            "report_alias_dir": str(output_root / "reports"),
        },
        "walk_forward": {
            "train_window_days": 3,
            "validation_window_days": 2,
            "step_days": 2,
            "min_history_days": min_history_days,
        },
        "backtest_frequency": "daily",
        "rebalance_frequency": "weekly",
        "signal_evaluation_frequency": "daily",
        "transaction_cost": {
            "commission_bps": 1,
            "slippage_bps": 5,
            "fx_cost_bps": 0,
            "tax_model": "ignored_for_test",
        },
        "search": {
            "algorithm": "bounded_grid",
            "max_candidates": 8,
            "hard_gate_tuning": {"enabled": False, "reason": "test"},
            "search_space": {
                "macro_liquidity": {"min": 0.20, "max": 0.20, "step": 0.05},
                "trend_momentum": {"min": 0.25, "max": 0.25, "step": 0.05},
                "sector_strength": {"min": 0.20, "max": 0.20, "step": 0.05},
                "earnings_quality": {"min": 0.15, "max": 0.15, "step": 0.05},
                "valuation_risk": {"min": 0.10, "max": 0.10, "step": 0.05},
                "event_risk": {"min": 0.10, "max": 0.10, "step": 0.05},
            },
            "constraints": {
                "total_weight_sum": 1.0,
                "max_single_weight": 0.35,
                "min_single_weight": 0.05,
                "max_daily_parameter_delta": 0.05,
                "max_weekly_parameter_delta": 0.10,
            },
            "parameter_change_guardrails": {
                "max_abs_change_per_weight": 0.10,
                "max_total_l1_change": 0.30,
                "require_reason_for_each_change": True,
            },
        },
        "data_quality_rules": {
            "insufficient_history": {"min_days": min_history_days, "status": "INSUFFICIENT_DATA"},
            "missing_price_data": {"max_missing_ratio": 0.02, "status": "LIMITED"},
            "missing_required_asset": {"status": "FAILED"},
            "missing_signal_snapshot": {"status": "LIMITED"},
            "cache_freshness": {
                "max_age_days": {"price_data": 3, "signal_snapshot": 3, "macro_data": 7}
            },
        },
        "point_in_time_status": {
            "price_data": "OK",
            "fundamental_data": "LIMITED",
            "news_data": "NOT_AVAILABLE",
            "macro_data": "LIMITED",
        },
    }


def _write_diagnostic_artifact(tmp_path: Path, as_of: date, *, status: str) -> Path:
    path = tmp_path / "artifacts" / "data_quality" / as_of.isoformat()
    path.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "report_type": "backtest_input_diagnostics",
        "metadata": {
            "run_id": f"backtest-input-diagnostics-{as_of.isoformat()}",
            "production_effect": "none",
        },
        "summary": {
            "overall_status": status,
            "blocking_errors": 1 if status == "FAILED" else 0,
            "warnings": 0,
            "asset_coverage_status": "FAILED" if status == "FAILED" else "OK",
            "date_coverage_status": "OK",
            "price_data_status": "OK",
            "signal_snapshots_status": "OK",
            "backtest_mode": "blocked" if status == "FAILED" else "full_signal_backtest",
            "can_run_shadow_backtest": status == "OK",
            "can_promote_candidate": status == "OK",
            "blocking_reasons": ["Missing required price history for BRK.B."],
        },
        "checks": {
            "asset_coverage": {
                "status": "FAILED" if status == "FAILED" else "OK",
                "missing_assets": ["BRK.B"] if status == "FAILED" else [],
            },
            "date_coverage": {"status": "OK"},
            "price_data": {"status": "OK", "assets": []},
            "signal_snapshots": {"status": "OK", "missing_signals": []},
            "cache_freshness": {"status": "OK", "items": []},
        },
        "repair_plan": {"status": "AVAILABLE", "steps": []},
    }
    json_path = path / "backtest_input_diagnostics.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    (path / "backtest_input_diagnostics.md").write_text(
        "# Backtest Input Diagnostics\n", encoding="utf-8"
    )
    return json_path


def _write_shadow_summary(tmp_path: Path, as_of: date, *, data_quality_status: str) -> Path:
    diagnostic_path = (
        tmp_path
        / "artifacts"
        / "data_quality"
        / as_of.isoformat()
        / "backtest_input_diagnostics.json"
    )
    path = tmp_path / "artifacts" / "shadow_backtest" / as_of.isoformat()
    path.mkdir(parents=True, exist_ok=True)
    summary_path = path / "shadow_backtest_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "shadow_parameter_backtest",
                "metadata": {
                    "run_id": f"shadow-backtest-{as_of.isoformat()}",
                    "generated_at": "2026-05-29T00:00:00+00:00",
                    "status": "DEGRADED",
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                    "backtest_mode": "blocked",
                    "baseline_parameter_version": "production-test",
                    "candidate_parameter_version": "shadow-test",
                },
                "data_quality": {
                    "status": data_quality_status,
                    "overall_status": data_quality_status,
                    "price_data_status": "OK",
                    "signal_snapshots_status": "OK",
                    "backtest_mode": "blocked",
                    "diagnostic_report": str(diagnostic_path),
                    "blocking_errors": 1,
                    "can_run_shadow_backtest": False,
                    "can_promote_candidate": False,
                },
                "relative_comparison": {},
                "promotion_decision": {
                    "status": "rejected",
                    "reason": "data quality failed",
                    "hard_rejections": ["data_quality_status_red"],
                    "manual_review_items": [],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return summary_path


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / "outputs" / "reports" / f"daily_ops_metadata_{as_of.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "run_id": f"daily-ops-{as_of.isoformat()}",
                "status": "PASS",
                "project_root": str(tmp_path),
                "started_at": "2026-05-29T00:00:00+00:00",
                "finished_at": "2026-05-29T00:01:00+00:00",
                "commands": [],
                "step_results": [],
                "git": {"commit": "test", "dirty": False},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_decision_snapshot(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"decision_snapshot_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": f"decision_snapshot:{as_of.isoformat()}",
                "signal_date": as_of.isoformat(),
                "market_regime": {"regime_id": "ai_after_chatgpt"},
                "scores": {"overall_score": 70, "confidence_score": 60, "components": []},
                "positions": {
                    "final_risk_asset_ai_band": {"min_position": 0.2, "max_position": 0.4},
                    "position_gates": [],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
