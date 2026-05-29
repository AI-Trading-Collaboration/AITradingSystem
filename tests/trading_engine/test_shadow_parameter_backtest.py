from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    validate_shadow_backtest_payload,
)


def test_shadow_parameter_backtest_writes_observe_only_artifacts(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )

    assert run.artifacts is not None
    assert run.artifacts.summary_json.exists()
    assert run.artifacts.summary_markdown.exists()
    assert run.artifacts.shadow_parameters_json.exists()
    assert run.artifacts.candidate_parameters_json.exists()
    assert run.artifacts.promotion_json.exists()
    assert run.payload["data_quality"]["status"] == "OK"
    assert run.payload["metadata"]["status"] == "OK"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["metadata"]["manual_review_required"] is True
    assert run.payload["metadata"]["auto_promotion"] is False
    assert run.payload["metadata"]["market_regime"] == "ai_after_chatgpt"
    assert run.payload["walk_forward_windows"]
    assert validate_shadow_backtest_payload(run.payload) == []

    shadow_snapshot = json.loads(run.artifacts.shadow_parameters_json.read_text(encoding="utf-8"))
    candidate_snapshot = json.loads(
        run.artifacts.candidate_parameters_json.read_text(encoding="utf-8")
    )
    assert shadow_snapshot["report_type"] == "shadow_parameters"
    assert candidate_snapshot["report_type"] == "candidate_parameters"
    assert shadow_snapshot["parameters"]["weights"]
    assert candidate_snapshot["parameters"]["weights"]
    assert fixture["baseline_path"].read_text(encoding="utf-8") == fixture["baseline_text"]


def test_shadow_parameter_backtest_rejects_insufficient_history(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=7, min_history_days=10)

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )

    decision = run.payload["promotion_decision"]
    assert run.payload["metadata"]["status"] == "DEGRADED"
    assert run.payload["data_quality"]["status"] == "INSUFFICIENT_DATA"
    assert decision["status"] == "rejected"
    assert "insufficient_data" in decision["hard_rejections"]
    assert run.payload["candidate_parameters"]["version"] == "no-shadow-candidate"


def test_shadow_parameter_backtest_reports_failed_missing_baseline(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(
        tmp_path,
        days=16,
        min_history_days=8,
        missing_baseline=True,
    )

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )

    assert run.payload["metadata"]["status"] == "FAILED"
    assert run.payload["data_quality"]["status"] == "FAILED"
    assert run.payload["promotion_decision"]["status"] == "rejected"
    assert (
        "missing_required_input_artifacts" in run.payload["promotion_decision"]["hard_rejections"]
    )
    assert run.artifacts is not None
    assert run.artifacts.summary_json.exists()


def test_shadow_parameter_backtest_dry_run_does_not_write_formal_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    assert run.artifacts is not None
    assert "outputs\\dry_runs\\shadow_backtest" in str(run.artifacts.summary_json) or (
        "outputs/dry_runs/shadow_backtest" in str(run.artifacts.summary_json)
    )
    formal_summary = (
        fixture["formal_shadow_backtest_dir"]
        / fixture["as_of"].isoformat()
        / "shadow_backtest_summary.json"
    )
    assert not formal_summary.exists()
    assert run.artifacts.summary_json.exists()
    assert run.payload["metadata"]["dry_run"] is True


def test_dashboard_reads_shadow_parameter_backtest_card(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_shadow_summary_artifact(tmp_path, as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )

    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["shadow_parameter_backtest"]
    assert card["exists"] is True
    assert card["baseline_version"] == "production-test"
    assert card["candidate_version"] == "shadow-test"
    assert card["promotion_status"] == "watch"
    assert card["manual_review_required"] is True
    assert "Shadow Parameter Backtest" in html
    assert "production_effect" in html


def _write_shadow_backtest_fixture(
    tmp_path: Path,
    *,
    days: int,
    min_history_days: int,
    missing_baseline: bool = False,
) -> dict[str, object]:
    as_of = date(2026, 1, 1) + timedelta(days=days - 1)
    data_dir = tmp_path / "data"
    config_dir = tmp_path / "config"
    output_dir = tmp_path / "artifacts"
    reports_dir = tmp_path / "outputs" / "reports"
    prices_path = data_dir / "prices_daily.csv"
    rates_path = data_dir / "rates_daily.csv"
    secondary_prices_path = data_dir / "secondary_prices_daily.csv"
    manifest_path = data_dir / "download_manifest.csv"
    baseline_path = config_dir / "parameters" / "production" / "current.yaml"
    promotion_path = config_dir / "parameters" / "promotion" / "promotion_rules.yaml"
    config_path = config_dir / "parameters" / "shadow" / "shadow_backtest.yaml"
    dates = tuple(date(2026, 1, 1) + timedelta(days=offset) for offset in range(days))

    _write_prices(prices_path, dates)
    _write_prices(secondary_prices_path, dates)
    _write_rates(rates_path, dates)
    _write_manifest(manifest_path, [prices_path, rates_path, secondary_prices_path])
    baseline_text = yaml.safe_dump(_baseline_payload(), sort_keys=False, allow_unicode=True)
    if not missing_baseline:
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(baseline_text, encoding="utf-8")
    promotion_path.parent.mkdir(parents=True, exist_ok=True)
    promotion_path.write_text(
        yaml.safe_dump(_promotion_rules_payload(), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            _shadow_config_payload(
                prices_path=prices_path,
                rates_path=rates_path,
                secondary_prices_path=secondary_prices_path,
                manifest_path=manifest_path,
                baseline_path=baseline_path,
                promotion_path=promotion_path,
                output_dir=output_dir,
                reports_dir=reports_dir,
                min_history_days=min_history_days,
            ),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return {
        "as_of": as_of,
        "config_path": config_path,
        "baseline_path": baseline_path,
        "baseline_text": baseline_text,
        "formal_shadow_backtest_dir": output_dir / "shadow_backtest",
    }


def _baseline_payload() -> dict[str, object]:
    return {
        "version": "production-test",
        "created_at": "2026-05-29T00:00:00+09:00",
        "owner": "tests",
        "status": "pilot_baseline",
        "production_effect": "production",
        "rationale": "unit test baseline",
        "asset_universe": {"core": ["QQQ", "NVDA", "CASH"]},
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
        "hard_gates": {
            "max_drawdown_alert": {"enabled": True, "threshold": -0.12},
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
    output_dir: Path,
    reports_dir: Path,
    min_history_days: int,
) -> dict[str, object]:
    return {
        "version": "shadow-backtest-test",
        "owner": "tests",
        "status": "pilot",
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "observe_only": True,
        "rationale": "test shadow backtest infrastructure",
        "intended_effect": "validate observe-only parameter backtest",
        "validation_evidence": "unit tests",
        "review_condition": "test review",
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
            "data_quality_report_dir": str(reports_dir),
        },
        "baseline_parameters_path": str(baseline_path),
        "promotion_rules_path": str(promotion_path),
        "output": {
            "shadow_backtest_dir": str(output_dir / "shadow_backtest"),
            "shadow_parameters_dir": str(output_dir / "shadow_parameters"),
            "candidate_parameters_dir": str(output_dir / "candidate_parameters"),
            "parameter_promotion_dir": str(output_dir / "parameter_promotion"),
            "report_alias_dir": str(reports_dir),
        },
        "walk_forward": {
            "train_window_days": 5,
            "validation_window_days": 3,
            "step_days": 4,
            "min_history_days": min_history_days,
        },
        "backtest_frequency": "daily",
        "rebalance_frequency": "weekly",
        "signal_evaluation_frequency": "daily",
        "transaction_cost": {
            "commission_bps": 1,
            "slippage_bps": 5,
            "fx_cost_bps": 0,
            "tax_model": "ignored_for_v0_1",
        },
        "search": {
            "algorithm": "bounded_grid",
            "max_candidates": 32,
            "hard_gate_tuning": {
                "enabled": False,
                "reason": "avoid overfitting in v0.1",
            },
            "search_space": {
                "macro_liquidity": {"min": 0.15, "max": 0.25, "step": 0.05},
                "trend_momentum": {"min": 0.20, "max": 0.30, "step": 0.05},
                "sector_strength": {"min": 0.15, "max": 0.25, "step": 0.05},
                "earnings_quality": {"min": 0.10, "max": 0.20, "step": 0.05},
                "valuation_risk": {"min": 0.05, "max": 0.15, "step": 0.05},
                "event_risk": {"min": 0.05, "max": 0.15, "step": 0.05},
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
            "insufficient_history": {
                "min_days": min_history_days,
                "status": "INSUFFICIENT_DATA",
            },
            "missing_price_data": {"max_missing_ratio": 0.02, "status": "LIMITED"},
            "missing_required_asset": {"status": "FAILED"},
            "missing_signal_snapshot": {"status": "LIMITED"},
        },
        "point_in_time_status": {
            "price_data": "OK",
            "fundamental_data": "LIMITED",
            "news_data": "NOT_AVAILABLE",
            "macro_data": "LIMITED",
        },
    }


def _promotion_rules_payload() -> dict[str, object]:
    return {
        "version": "promotion-test",
        "owner": "tests",
        "status": "pilot",
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "observe_only": True,
        "rationale": "test promotion rules",
        "intended_effect": "test conservative promotion",
        "validation_evidence": "unit tests",
        "review_condition": "test review",
        "promotion_status": ["rejected", "watch", "candidate", "manual_review_required"],
        "promotion_criteria": {
            "max_drawdown": {"must_be_less_or_equal_to_baseline": True},
            "annualized_return": {"min_relative_improvement": 0.02},
            "sharpe_ratio": {"min_relative_improvement": 0.05},
            "turnover": {"max_relative_increase": 0.20},
            "recent_period": {"must_not_underperform_baseline_by_more_than": 0.03},
            "stability": {"min_passing_windows_ratio": 0.60},
            "explainability": {"all_major_changes_require_reason": True},
        },
        "hard_rejection_rules": [
            "max_drawdown_worse_than_baseline_by_more_than_5_percent",
            "turnover_more_than_50_percent_above_baseline",
            "performance_only_improves_in_one_window",
            "parameter_change_without_explanation",
            "missing_required_input_artifacts",
            "insufficient_data",
            "data_quality_status_red",
        ],
    }


def _write_prices(path: Path, dates: tuple[date, ...]) -> None:
    rows: list[dict[str, object]] = []
    for offset, current in enumerate(dates):
        for ticker, base, drift in (("QQQ", 100.0, 0.12), ("NVDA", 90.0, 0.16)):
            close = base + offset * drift
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": ticker,
                    "open": round(close * 0.998, 4),
                    "high": round(close * 1.004, 4),
                    "low": round(close * 0.996, 4),
                    "close": round(close, 4),
                    "adj_close": round(close, 4),
                    "volume": 1_000_000 + offset,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rates(path: Path, dates: tuple[date, ...]) -> None:
    rows: list[dict[str, object]] = []
    for offset, current in enumerate(dates):
        rows.extend(
            [
                {"date": current.isoformat(), "series": "DGS2", "value": 4.0 + offset * 0.001},
                {"date": current.isoformat(), "series": "DGS10", "value": 4.4 + offset * 0.001},
                {
                    "date": current.isoformat(),
                    "series": "DTWEXBGS",
                    "value": 120.0 + offset * 0.01,
                },
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_manifest(path: Path, files: list[Path]) -> None:
    rows = [
        {
            "downloaded_at": "2026-01-20T00:00:00+00:00",
            "source_id": file_path.stem,
            "provider": "unit_test",
            "endpoint": "local_fixture",
            "request_parameters": "{}",
            "output_path": str(file_path),
            "row_count": len(pd.read_csv(file_path)),
            "checksum_sha256": _sha256(file_path),
        }
        for file_path in files
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_shadow_summary_artifact(tmp_path: Path, as_of: date) -> Path:
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
                    "status": "OK",
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                    "baseline_parameter_version": "production-test",
                    "candidate_parameter_version": "shadow-test",
                },
                "data_quality": {"status": "OK"},
                "relative_comparison": {
                    "annualized_return_delta": 0.02,
                    "max_drawdown_delta": 0.01,
                    "sharpe_ratio_delta": 0.05,
                    "turnover_delta": 0.10,
                },
                "promotion_decision": {
                    "status": "watch",
                    "reason": "Needs more validation windows.",
                    "hard_rejections": [],
                    "manual_review_items": ["criterion_failed:stability"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return summary_path


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / "outputs" / "reports" / f"daily_ops_metadata_{as_of}.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
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
            indent=2,
        ),
        encoding="utf-8",
    )
    return metadata_path
