from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.shadow_promotion_proposal import (
    build_shadow_promotion_proposal_payload,
    write_shadow_promotion_proposal_report,
)


def test_promotion_proposal_missing_current_shadow_weights_is_insufficient_data(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_review(context["data_root"], as_of)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "INSUFFICIENT_DATA"
    assert payload["promotion_proposed"] is False
    assert payload["input_artifacts"]["current_shadow_weights"]["status"] == "MISSING"


def test_promotion_proposal_missing_latest_review_is_insufficient_data(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["latest_multi_day_review"]["status"] == "MISSING"


def test_promotion_proposal_requires_minimum_history_days(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of, available_days=4)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "INSUFFICIENT_HISTORY"
    assert payload["readiness_checks"]["minimum_history_days_status"] == "FAIL"


def test_promotion_proposal_proposes_for_manual_review_when_all_conditions_pass(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(
        context["data_root"],
        weights={
            "technical": 0.26,
            "fundamental": 0.24,
            "macro": 0.20,
            "policy": 0.15,
            "sentiment": 0.15,
        },
    )
    _write_review(context["data_root"], as_of, decision_difference_count=2)
    for offset in range(5):
        _write_comparison(context["data_root"], as_of - timedelta(days=offset))

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "PROPOSE_FOR_MANUAL_REVIEW"
    assert payload["promotion_proposed"] is True
    assert payload["promotion_executed"] is False
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["proposed_delta_from_production"] == {
        "fundamental": -0.01,
        "macro": 0.0,
        "policy": 0.0,
        "sentiment": 0.0,
        "technical": 0.01,
    }


def test_promotion_proposal_continues_when_average_score_delta_is_not_positive(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of, average_score_delta=0.0)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "CONTINUE_OBSERVATION"
    assert payload["readiness_checks"]["score_improvement_status"] == "FAIL"


def test_promotion_proposal_rejects_shadow_when_risk_increases(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of, risk_delta=1)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "REJECT_SHADOW"
    assert payload["readiness_checks"]["risk_regression_status"] == "FAIL"


def test_promotion_proposal_safety_blocks_when_review_has_safety_blocked_days(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of, safety_blocked_days=1)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "SAFETY_BLOCKED"
    assert "safety_blocked_days_above_limit" in payload["readiness_checks"]["blocking_reasons"]


def test_promotion_proposal_blocks_weight_key_mismatch(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(
        context["data_root"],
        weights={
            "technical": 0.50,
            "fundamental": 0.25,
            "macro": 0.25,
        },
    )
    _write_review(context["data_root"], as_of)

    payload = build_shadow_promotion_proposal_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert payload["proposal_decision"] == "INSUFFICIENT_DATA"
    assert payload["readiness_checks"]["weight_key_compatibility_status"] == "FAIL"
    assert payload["weight_key_compatibility"]["missing_in_shadow"] == ["policy", "sentiment"]
    assert payload["weight_key_compatibility"]["missing_in_production"] == []


def test_promotion_proposal_write_does_not_modify_production_profile(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    before = production_path.read_text(encoding="utf-8")
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of)

    payload = write_shadow_promotion_proposal_report(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_proposal(payload)
    assert production_path.read_text(encoding="utf-8") == before
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()


def test_dashboard_reads_shadow_promotion_proposal_artifact_without_rerun(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_review(context["data_root"], as_of)
    proposal = write_shadow_promotion_proposal_report(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )
    _remove_shadow_promotion_inputs(context["data_root"])
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_shadow_weight_iteration",
            "run_daily_shadow_vs_production_comparison",
            "run_shadow_vs_production_multi_day_review",
            "run_shadow_promotion_proposal",
            "ai_trading_system.trading_engine.shadow_promotion_proposal",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["shadow_promotion_proposal"]
    assert summary["proposal_decision"] == "PROPOSE_FOR_MANUAL_REVIEW"
    assert summary["promotion_proposed"] is True
    assert summary["promotion_executed"] is False
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["average_score_delta"] == "+0.02"
    assert summary["risk_flag_delta_total"] == 0
    assert summary["available_comparison_days"] == 5
    assert summary["latest_proposal_markdown_path"] == proposal["outputs"]["markdown"]
    assert "Shadow Promotion Proposal" in html
    assert "shadow_promotion_proposal_2026-05-19.md" in html
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


def _write_production_profile(tmp_path: Path) -> Path:
    path = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                'version: "test"',
                'status: "production"',
                "base_weights:",
                "  technical: 0.25",
                "  fundamental: 0.25",
                "  macro: 0.20",
                "  policy: 0.15",
                "  sentiment: 0.15",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_current_shadow_weights(
    data_root: Path,
    *,
    weights: dict[str, float] | None = None,
) -> None:
    path = data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "report_type": "current_shadow_weights",
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "last_updated_date": "2026-05-19",
            "weights": weights
            or {
                "technical": 0.26,
                "fundamental": 0.24,
                "macro": 0.20,
                "policy": 0.15,
                "sentiment": 0.15,
            },
            "audit": {"last_decision": "UPDATE"},
        },
    )


def _write_review(
    data_root: Path,
    as_of: date,
    *,
    review_decision: str = "SHADOW_LOOKS_BETTER",
    available_days: int = 5,
    insufficient_data_days: int = 0,
    safety_blocked_days: int = 0,
    average_score_delta: float = 0.024,
    risk_delta: int = 0,
    decision_difference_count: int = 2,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "comparison"
        / "reviews"
        / f"shadow_vs_production_review_{as_of.isoformat()}.json"
    )
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "report_type": "shadow_vs_production_multi_day_review",
            "task_id": "TRADING-018C2",
            "date": as_of.isoformat(),
            "lookback_days": 7,
            "production_effect": "none",
            "manual_review_only": True,
            "review_decision": review_decision,
            "available_comparison_days": available_days,
            "missing_comparison_days": [],
            "insufficient_data_days": insufficient_data_days,
            "safety_blocked_days": safety_blocked_days,
            "decision_difference_count": decision_difference_count,
            "average_score_delta": average_score_delta,
            "shadow_risk_flag_delta_total": risk_delta,
            "dominant_changed_weight_keys": ["technical", "fundamental"],
            "promotion_readiness": {"ready": False},
            "outputs": {
                "json": str(path),
                "markdown": str(path.with_suffix(".md")),
            },
            "pipeline_contract": {
                "runs_scoring_pipeline": False,
                "runs_comparison_pipeline": False,
                "runs_broker_runner": False,
                "runs_paper_runner": False,
                "runs_replay_runner": False,
                "writes_production_profile": False,
                "writes_approved_profile": False,
                "promotes_shadow_to_production": False,
                "triggers_trade": False,
            },
        },
    )
    path.with_suffix(".md").write_text("# Multi-day Review\n", encoding="utf-8")


def _write_comparison(data_root: Path, as_of: date) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "comparison"
        / f"daily_shadow_vs_production_{as_of.isoformat()}.json"
    )
    _write_json(
        path,
        {
            "schema_version": 1,
            "report_type": "daily_shadow_vs_production_comparison",
            "task_id": "TRADING-018C",
            "as_of": as_of.isoformat(),
            "production_effect": "none",
            "manual_review_only": True,
            "comparison_status": "COMPARISON_AVAILABLE",
            "difference": {
                "score_delta": 0.024,
                "decision_changed": False,
            },
        },
    )


def _remove_shadow_promotion_inputs(data_root: Path) -> None:
    shadow_root = data_root / "derived" / "weight_iterations" / "shadow"
    current_path = shadow_root / "current_shadow_weights.json"
    if current_path.exists():
        current_path.unlink()
    review_root = data_root / "derived" / "weight_iterations" / "comparison" / "reviews"
    for path in review_root.glob("shadow_vs_production_review_*.json"):
        path.unlink()
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


def _assert_safe_proposal(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["promotion_executed"] is False
    assert payload["safe_for_production"] is False
    assert payload["pipeline_contract"]["runs_scoring_pipeline"] is False
    assert payload["pipeline_contract"]["runs_broker_runner"] is False
    assert payload["pipeline_contract"]["runs_replay_runner"] is False
    assert payload["pipeline_contract"]["writes_production_profile"] is False
    assert payload["pipeline_contract"]["promotes_shadow_to_production"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 19, 8, 0, tzinfo=UTC)
