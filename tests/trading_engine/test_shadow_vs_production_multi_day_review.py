from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.reports.daily_shadow_vs_production_multi_day_review import (
    build_shadow_vs_production_multi_day_review_payload,
    write_shadow_vs_production_multi_day_review_report,
)


def test_multi_day_review_insufficient_history_with_less_than_three_days(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    _write_comparison(context["data_root"], as_of, score_delta=1.0)
    _write_comparison(context["data_root"], as_of - timedelta(days=1), score_delta=2.0)

    payload = build_shadow_vs_production_multi_day_review_payload(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert payload["review_decision"] == "INSUFFICIENT_HISTORY"
    assert payload["available_comparison_days"] == 2


def test_multi_day_review_shadow_score_better_without_risk_increase(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    for offset, delta in enumerate((1.0, 2.0, 3.0)):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset), score_delta=delta)

    payload = build_shadow_vs_production_multi_day_review_payload(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert payload["review_decision"] == "SHADOW_LOOKS_BETTER"
    assert payload["average_score_delta"] == 2.0
    assert payload["shadow_risk_flag_delta_total"] == 0
    assert payload["dominant_changed_weight_keys"] == ["trend"]


def test_multi_day_review_shadow_score_worse(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    for offset, delta in enumerate((-1.0, -2.0, -3.0)):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset), score_delta=delta)

    payload = build_shadow_vs_production_multi_day_review_payload(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert payload["review_decision"] == "SHADOW_LOOKS_WORSE"
    assert payload["shadow_worse_score_days"] == 3


def test_multi_day_review_safety_blocked_days_take_precedence(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    _write_comparison(context["data_root"], as_of, score_delta=2.0, safety_blocked=True)
    _write_comparison(
        context["data_root"],
        as_of - timedelta(days=1),
        score_delta=2.0,
        safety_blocked=True,
    )
    _write_comparison(context["data_root"], as_of - timedelta(days=2), score_delta=2.0)
    _write_comparison(context["data_root"], as_of - timedelta(days=3), score_delta=2.0)
    _write_comparison(context["data_root"], as_of - timedelta(days=4), score_delta=2.0)

    payload = build_shadow_vs_production_multi_day_review_payload(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert payload["review_decision"] == "SAFETY_BLOCKED"
    assert payload["safety_blocked_days"] == 2


def test_multi_day_review_records_missing_comparison_days_and_writes_outputs(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    for offset in (0, 2, 4):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset), score_delta=1.0)

    payload = write_shadow_vs_production_multi_day_review_report(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert payload["available_comparison_days"] == 3
    assert "2026-05-18" in payload["missing_comparison_days"]
    assert "2026-05-16" in payload["missing_comparison_days"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()


def test_multi_day_review_does_not_write_production_profile(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    for offset in range(3):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset), score_delta=1.0)
    profile_path = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("version: test\nbase_weights:\n  trend: 1.0\n", encoding="utf-8")
    before = profile_path.read_text(encoding="utf-8")

    payload = write_shadow_vs_production_multi_day_review_report(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_review(payload)
    assert profile_path.read_text(encoding="utf-8") == before
    assert payload["pipeline_contract"]["writes_production_profile"] is False


def test_dashboard_reads_multi_day_review_artifact_without_rerun(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    for offset in range(3):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset), score_delta=1.0)
    review = write_shadow_vs_production_multi_day_review_report(
        as_of=as_of,
        lookback_days=7,
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )
    _remove_daily_comparison_inputs(context["data_root"])
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["shadow_vs_production_multi_day_review"]
    assert summary["review_decision"] == "SHADOW_LOOKS_BETTER"
    assert summary["lookback_days"] == 7
    assert summary["available_comparison_days"] == 3
    assert summary["average_score_delta"] == "+1.00"
    assert summary["decision_difference_count"] == 0
    assert summary["promotion_ready"] is False
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["latest_review_markdown_path"] == review["outputs"]["markdown"]
    assert "Shadow vs Production Multi-day Review" in html
    assert "shadow_vs_production_review_2026-05-19.md" in html
    assert not list(
        (context["data_root"] / "derived" / "weight_iterations" / "comparison").glob(
            "daily_shadow_vs_production_*.json"
        )
    )


def _write_context(tmp_path: Path) -> dict[str, Path]:
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    return {"data_root": data_root, "reports_dir": reports_dir}


def _write_comparison(
    data_root: Path,
    as_of: date,
    *,
    score_delta: float,
    production_decision: str = "中性",
    shadow_decision: str = "中性",
    risk_delta: int = 0,
    safety_blocked: bool = False,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "comparison"
        / f"daily_shadow_vs_production_{as_of.isoformat()}.json"
    )
    production_flags = [{"gate_id": "score_model", "triggered": True}]
    shadow_flags = [{"gate_id": "score_model", "triggered": True}]
    if risk_delta > 0:
        shadow_flags.append({"gate_id": "valuation", "triggered": True})
    elif risk_delta < 0:
        production_flags.append({"gate_id": "valuation", "triggered": True})
    payload = {
        "schema_version": 1,
        "report_type": "daily_shadow_vs_production_comparison",
        "task_id": "TRADING-018C",
        "as_of": as_of.isoformat(),
        "mode": "offline_comparison",
        "production_effect": "none",
        "manual_review_only": True,
        "comparison_status": "COMPARISON_AVAILABLE",
        "input_validation": {"blocking_reasons": [], "warnings": []},
        "shadow_iteration": {
            "decision": "SAFETY_BLOCKED" if safety_blocked else "UPDATE",
        },
        "production": {
            "decision": production_decision,
            "score": 60.0,
            "risk_flags": production_flags,
        },
        "shadow": {
            "decision": shadow_decision,
            "score": 60.0 + score_delta,
            "risk_flags": shadow_flags,
        },
        "difference": {
            "score_delta": score_delta,
            "decision_changed": production_decision != shadow_decision,
            "risk_flags_changed": risk_delta != 0,
            "weight_deltas": [{"component": "trend", "weight_delta": 0.10}],
            "contribution_deltas": [{"component": "trend", "weight_delta": 0.10}],
        },
        "outputs": {
            "json": str(path),
            "markdown": str(path.with_suffix(".md")),
        },
        "pipeline_contract": {
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "triggers_trade": False,
        },
    }
    _write_json(path, payload)
    path.with_suffix(".md").write_text("# Shadow vs Production Comparison\n", encoding="utf-8")


def _remove_daily_comparison_inputs(data_root: Path) -> None:
    comparison_root = data_root / "derived" / "weight_iterations" / "comparison"
    for path in comparison_root.glob("daily_shadow_vs_production_*.json"):
        path.unlink()


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    _write_json(
        metadata_path,
        {
            "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
            "as_of": as_of.isoformat(),
            "generated_at": _fixed_generated_at().isoformat(),
            "project_root": str(tmp_path),
            "status": "PASS",
            "started_at": _fixed_generated_at().isoformat(),
            "finished_at": _fixed_generated_at().isoformat(),
            "visibility_cutoff": "2026-05-19T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return metadata_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _assert_safe_review(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["promotion_readiness"]["ready"] is False
    assert payload["pipeline_contract"]["runs_scoring_pipeline"] is False
    assert payload["pipeline_contract"]["runs_broker_runner"] is False
    assert payload["pipeline_contract"]["runs_replay_runner"] is False
    assert payload["audit"]["safe_for_production"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 19, 8, 0, tzinfo=UTC)
