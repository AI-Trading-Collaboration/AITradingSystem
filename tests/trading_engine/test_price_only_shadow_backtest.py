from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.promotion_rules import PromotionDecision


def test_price_only_mode_caps_candidate_promotion() -> None:
    decision = PromotionDecision(
        status="candidate",
        reason="candidate criteria passed",
        hard_rejections=(),
        manual_review_items=(),
        criteria_results={"annualized_return": True},
    )

    constrained = shadow_backtest._apply_backtest_mode_promotion_constraints(
        decision,
        backtest_mode="price_only_shadow_backtest",
    )

    assert constrained.status == "rejected"
    assert "signal snapshot is missing" in constrained.reason
    assert "price_only_shadow_backtest_signal_snapshot_missing" in (constrained.manual_review_items)


def test_dashboard_displays_price_only_mode_and_disabled_promotion(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_price_only_shadow_summary(tmp_path, as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["shadow_parameter_backtest"]
    assert card["backtest_mode"] == "price_only_shadow_backtest"
    assert card["promotion_eligibility"] == "Disabled"
    assert "Price-only" in html
    assert "Disabled" in html


def test_reader_brief_displays_limited_signal_price_only_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    as_of = date(2026, 5, 29)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_price_only_diagnostic(tmp_path, as_of)
    _write_price_only_shadow_summary(tmp_path, as_of)
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["backtest_mode"] == "price_only_shadow_backtest"
    assert review["promotion_eligibility"] == "Disabled"
    assert "can now run in price-only mode" in review["data_quality_summary"]
    assert "candidate promotion is rejected" in review["data_quality_summary"]


def _write_price_only_diagnostic(tmp_path: Path, as_of: date) -> Path:
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
            "overall_status": "LIMITED",
            "asset_coverage_status": "OK",
            "date_coverage_status": "OK",
            "price_data_status": "OK",
            "signal_snapshots_status": "MISSING",
            "backtest_mode": "price_only_shadow_backtest",
            "blocking_errors": 0,
            "warnings": 1,
            "can_run_shadow_backtest": True,
            "can_promote_candidate": False,
            "blocking_reasons": [],
        },
    }
    json_path = path / "backtest_input_diagnostics.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return json_path


def _write_price_only_shadow_summary(tmp_path: Path, as_of: date) -> Path:
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
                    "status": "LIMITED",
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                    "backtest_mode": "price_only_shadow_backtest",
                    "baseline_parameter_version": "production-test",
                    "candidate_parameter_version": "shadow-test",
                },
                "data_quality": {
                    "status": "LIMITED",
                    "overall_status": "LIMITED",
                    "price_data_status": "OK",
                    "signal_snapshots_status": "MISSING",
                    "backtest_mode": "price_only_shadow_backtest",
                    "diagnostic_report": str(diagnostic_path),
                    "blocking_errors": 0,
                    "can_run_shadow_backtest": True,
                    "can_promote_candidate": False,
                },
                "relative_comparison": {},
                "promotion_decision": {
                    "status": "rejected",
                    "reason": (
                        "Price-only shadow backtest completed, but signal snapshot is "
                        "missing. Candidate promotion is rejected until full signal inputs "
                        "are available."
                    ),
                    "hard_rejections": [],
                    "manual_review_items": ["price_only_shadow_backtest_signal_snapshot_missing"],
                },
                "promotion_constraints": {
                    "max_promotion_status": "rejected",
                    "allow_candidate": False,
                    "allow_production_promotion": False,
                    "manual_review_required": True,
                    "reason": "signal_snapshot_missing",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return summary_path


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / "outputs" / "reports" / f"daily_ops_metadata_{as_of}.json"
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
