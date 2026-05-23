from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.parameter_governance_daily_digest import (
    build_parameter_governance_daily_digest_payload,
    render_parameter_governance_daily_digest_markdown,
    write_parameter_governance_daily_digest,
)


@pytest.mark.parametrize(
    ("governance_state", "action_required", "action_level", "expected"),
    [
        ("ROLLBACK_COMPLETED", False, "NONE", "OK"),
        ("SAFE_OBSERVATION", False, "NONE", "OK"),
        ("SHADOW_LEARNING", False, "WATCH", "WATCH"),
        ("APPLIED_NEEDS_MONITORING", False, "WATCH", "WATCH"),
        ("PREFLIGHT_READY", True, "REVIEW_REQUIRED", "ACTION_REQUIRED"),
        ("PREFLIGHT_READY", True, "APPROVAL_REQUIRED", "ACTION_REQUIRED"),
        ("SAFETY_ANOMALY", True, "URGENT", "URGENT"),
    ],
)
def test_daily_digest_status_mapping(
    tmp_path: Path,
    governance_state: str,
    action_required: bool,
    action_level: str,
    expected: str,
) -> None:
    context = _write_context(tmp_path)
    critical = ["lifecycle_audit:safety_anomaly"] if governance_state == "SAFETY_ANOMALY" else []
    _write_summary(
        context,
        overrides={
            "governance_state": governance_state,
            "action_required": action_required,
            "action_level": action_level,
            "audit_findings": {
                "critical_findings": critical,
                "warnings": [],
                "notes": [],
            },
            "safety_boundary_audit": {
                "status": "FAIL" if governance_state == "SAFETY_ANOMALY" else "PASS",
                "latest_lifecycle_has_safety_anomaly": governance_state == "SAFETY_ANOMALY",
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
                "production_effect_from_governance": "none",
                "blocking_reasons": critical,
            },
        },
    )

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == expected
    assert (
        payload["summary_level"]
        == {
            "OK": "NORMAL",
            "WATCH": "WATCH",
            "ACTION_REQUIRED": "ACTION",
            "URGENT": "URGENT",
        }[expected]
    )


def test_daily_digest_missing_summary_is_input_missing(tmp_path: Path) -> None:
    context = _write_context(tmp_path)

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == "INPUT_MISSING"
    assert payload["summary_level"] == "UNKNOWN"


def test_daily_digest_invalid_json_is_input_invalid(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    context["summary_path"].parent.mkdir(parents=True, exist_ok=True)
    context["summary_path"].write_text("{not json", encoding="utf-8")

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == "INPUT_INVALID"


def test_daily_digest_invalid_task_id_is_input_invalid(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context, overrides={"task_id": "TRADING-018F"})

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == "INPUT_INVALID"
    assert payload["safety_validation"]["summary_task_id_valid"] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"production_effect": "profile_updated"},
        {"broker_execution": True},
        {"replay_execution": True},
        {"trading_execution": True},
    ],
)
def test_daily_digest_blocks_invalid_summary_safety_fields(
    tmp_path: Path,
    overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path)
    _write_summary(context, overrides=overrides)

    payload = _build_digest(context)
    markdown = render_parameter_governance_daily_digest_markdown(payload)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == "SAFETY_BLOCKED"
    assert payload["summary_level"] == "UNKNOWN"
    assert payload["safety_validation"]["status"] == "FAIL"
    assert "Digest Safety Blocked" in markdown


def test_daily_digest_markdown_ok_contains_daily_sections(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)

    payload = _build_digest(context)
    markdown = render_parameter_governance_daily_digest_markdown(payload)

    _assert_digest_invariants(payload)
    assert payload["digest_status"] == "OK"
    assert "Today's Status" in markdown
    assert "Safety Check" in markdown
    assert "Pending Actions" in markdown
    assert "Suggested Next Steps" in markdown
    assert "Continue observation." in markdown


def test_daily_digest_markdown_banners(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "governance_state": "SAFETY_ANOMALY",
            "action_required": True,
            "action_level": "URGENT",
            "audit_findings": {
                "critical_findings": ["critical safety finding"],
                "warnings": [],
                "notes": [],
            },
            "safety_boundary_audit": {
                "status": "FAIL",
                "latest_lifecycle_has_safety_anomaly": True,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
                "production_effect_from_governance": "none",
                "blocking_reasons": ["critical safety finding"],
            },
        },
    )
    urgent = _build_digest(context)
    assert "URGENT: Manual Attention Required" in render_parameter_governance_daily_digest_markdown(
        urgent
    )

    _write_summary(
        context,
        overrides={
            "governance_state": "PREFLIGHT_READY",
            "action_required": True,
            "action_level": "APPROVAL_REQUIRED",
            "pending_items": {
                "pending_proposal_review": False,
                "pending_preflight": False,
                "pending_apply": True,
                "pending_rollback": False,
                "pending_lifecycle_audit": False,
            },
        },
    )
    action = _build_digest(context)
    action_markdown = render_parameter_governance_daily_digest_markdown(action)
    assert "Action Required" in action_markdown
    assert "| Apply | `true` |" in action_markdown
    _assert_digest_invariants(urgent)
    _assert_digest_invariants(action)


def test_daily_digest_markdown_shows_critical_alerts(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "governance_state": "SAFETY_ANOMALY",
            "action_required": True,
            "action_level": "URGENT",
            "audit_findings": {
                "critical_findings": ["critical finding present"],
                "warnings": [],
                "notes": [],
            },
            "safety_boundary_audit": {
                "status": "FAIL",
                "latest_lifecycle_has_safety_anomaly": True,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
                "production_effect_from_governance": "none",
                "blocking_reasons": ["critical finding present"],
            },
        },
    )

    payload = _build_digest(context)
    markdown = render_parameter_governance_daily_digest_markdown(payload)

    _assert_digest_invariants(payload)
    assert "critical finding present" in markdown
    assert payload["alerts"]["critical"] == ["critical finding present"]


def test_daily_digest_weight_snapshot_extracts_deltas(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    weights = payload["weight_snapshot"]
    assert weights["production_weights_available"] is True
    assert weights["shadow_weights_available"] is True
    assert weights["largest_delta_key"] == "technical"
    assert weights["largest_delta_value"] == 0.01
    assert weights["delta_summary"][:2] == [
        {
            "weight_key": "technical",
            "production": 0.25,
            "shadow": 0.26,
            "delta": 0.01,
        },
        {
            "weight_key": "fundamental",
            "production": 0.25,
            "shadow": 0.24,
            "delta": -0.01,
        },
    ]


def test_daily_digest_missing_shadow_weights_warns_without_crashing(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "shadow_state": {
                "status": "MISSING",
                "weights": {},
                "weights_sum_valid": False,
                "delta_from_production": {},
            }
        },
    )

    payload = _build_digest(context)

    _assert_digest_invariants(payload)
    assert payload["weight_snapshot"]["shadow_weights_available"] is False
    assert any("Shadow weights are missing" in item for item in payload["alerts"]["warnings"])


def test_daily_digest_writes_outputs_and_run_log(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)

    payload = write_parameter_governance_daily_digest(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_digest_invariants(payload)
    assert Path(payload["output_artifacts"]["json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log_markdown"]["path"]).exists()


def test_dashboard_reads_daily_digest_without_triggering_pipelines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)
    digest = write_parameter_governance_daily_digest(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )
    context["summary_path"].unlink()
    metadata_path = _write_dashboard_metadata(tmp_path, context["as_of"])

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
            "run_shadow_promotion_apply_preflight",
            "run_shadow_promotion_apply",
            "run_shadow_promotion_rollback",
            "run_shadow_promotion_lifecycle_audit",
            "run_parameter_governance_summary",
            "render_parameter_governance_web_view",
            "run_parameter_governance_daily_digest",
            "ai_trading_system.trading_engine.parameter_governance_summary",
            "ai_trading_system.trading_engine.parameter_governance_web_view",
            "ai_trading_system.trading_engine.parameter_governance_daily_digest",
            "ai_trading_system.scoring",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=context["as_of"],
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    _assert_digest_invariants(digest)
    dashboard_summary = payload["parameter_governance_daily_digest"]
    assert dashboard_summary["digest_status"] == "OK"
    assert dashboard_summary["summary_level"] == "NORMAL"
    assert dashboard_summary["headline"] == digest["headline"]
    assert dashboard_summary["governance_state"] == "ROLLBACK_COMPLETED"
    assert dashboard_summary["action_required"] is False
    assert dashboard_summary["action_level"] == "NONE"
    assert dashboard_summary["safety_boundary_status"] == "PASS"
    assert dashboard_summary["pending_apply"] is False
    assert dashboard_summary["pending_rollback"] is False
    assert dashboard_summary["critical_alert_count"] == 0
    assert dashboard_summary["warning_count"] == 0
    assert dashboard_summary["latest_digest_markdown_path"] == (
        digest["output_artifacts"]["markdown"]["path"]
    )
    assert dashboard_summary["production_effect"] == "none"
    assert dashboard_summary["manual_review_only"] is True
    assert dashboard_summary["digest_only"] is True
    assert dashboard_summary["governance_only"] is True
    assert dashboard_summary["apply_executed_by_digest"] is False
    assert dashboard_summary["rollback_executed_by_digest"] is False
    assert dashboard_summary["broker_execution"] is False
    assert dashboard_summary["replay_execution"] is False
    assert dashboard_summary["trading_execution"] is False
    assert "Parameter Governance Daily Digest" in html


def _build_digest(context: dict[str, Any]) -> dict[str, Any]:
    payload = build_parameter_governance_daily_digest_payload(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )
    _assert_digest_invariants(payload)
    return payload


def _write_context(tmp_path: Path) -> dict[str, Any]:
    as_of = date(2026, 5, 23)
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    summary_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "governance"
        / f"parameter_governance_summary_{as_of.isoformat()}.json"
    )
    reports_dir.mkdir(parents=True, exist_ok=True)
    return {
        "as_of": as_of,
        "data_root": data_root,
        "reports_dir": reports_dir,
        "summary_path": summary_path,
    }


def _write_summary(
    context: dict[str, Any],
    *,
    overrides: dict[str, Any] | None = None,
) -> None:
    payload = _valid_summary(context)
    payload.update(overrides or {})
    _write_json(context["summary_path"], payload)


def _valid_summary(context: dict[str, Any]) -> dict[str, Any]:
    suffix = context["as_of"].isoformat()
    return {
        "schema_version": "1.0",
        "report_type": "parameter_governance_summary",
        "task_id": "TRADING-019",
        "date": suffix,
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "parameter_governance_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "governance_only": True,
        "apply_executed_by_governance": False,
        "rollback_executed_by_governance": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "governance_state": "ROLLBACK_COMPLETED",
        "governance_reason": "Latest rollback result and lifecycle audit indicate rollback.",
        "action_required": False,
        "action_level": "NONE",
        "recommended_action": "Continue observation.",
        "input_artifacts": _input_artifacts(context),
        "production_state": {
            "status": "AVAILABLE",
            "weights": _production_weights(),
            "weights_sum_valid": True,
        },
        "shadow_state": {
            "status": "AVAILABLE",
            "weights": _shadow_weights(),
            "weights_sum_valid": True,
            "delta_from_production": _expected_delta(),
        },
        "shadow_vs_production_review": {
            "status": "AVAILABLE",
            "review_decision": "SHADOW_LOOKS_BETTER",
            "available_comparison_days": 7,
            "average_score_delta": 0.024,
            "decision_difference_count": 2,
            "risk_flag_delta_total": 0,
        },
        "promotion_status": {
            "proposal_status": "FOUND",
            "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
            "promotion_proposed": True,
            "preflight_status": "FOUND",
            "preflight_decision": "PASS",
            "apply_status": "FOUND",
            "apply_decision": "APPLIED",
            "apply_executed": True,
            "rollback_status": "FOUND",
            "rollback_decision": "ROLLED_BACK",
            "rollback_executed": True,
            "lifecycle_status": "FOUND",
            "lifecycle_decision": "COMPLETE_WITH_ROLLBACK",
        },
        "pending_items": {
            "pending_proposal_review": False,
            "pending_preflight": False,
            "pending_apply": False,
            "pending_rollback": False,
            "pending_lifecycle_audit": False,
        },
        "safety_boundary_audit": {
            "status": "PASS",
            "latest_lifecycle_has_safety_anomaly": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "production_effect_from_governance": "none",
            "blocking_reasons": [],
        },
        "audit_findings": {
            "critical_findings": [],
            "warnings": [],
            "notes": ["Latest lifecycle audit indicates rollback completed successfully."],
        },
        "outputs": {
            "json": str(context["summary_path"]),
            "markdown": str(context["summary_path"].with_suffix(".md")),
        },
    }


def _input_artifacts(context: dict[str, Any]) -> dict[str, Any]:
    base = context["data_root"] / "derived" / "weight_iterations"
    return {
        "latest_multi_day_review": _artifact(
            base / "comparison" / "reviews" / "shadow_vs_production_review_2026-05-23.json"
        ),
        "latest_lifecycle_audit": _artifact(
            base / "promotion" / "audit" / "shadow_promotion_lifecycle_audit_2026-05-23.json"
        ),
    }


def _artifact(path: Path) -> dict[str, Any]:
    return {"status": "FOUND", "path": str(path), "exists": True, "sha256": "abc"}


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    _write_json(
        path,
        {
            "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
            "as_of": as_of.isoformat(),
            "generated_at": _fixed_generated_at().isoformat(),
            "project_root": str(tmp_path),
            "status": "PASS",
            "started_at": _fixed_generated_at().isoformat(),
            "finished_at": _fixed_generated_at().isoformat(),
            "visibility_cutoff": "2026-05-23T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return path


def _production_weights() -> dict[str, float]:
    return {
        "technical": 0.25,
        "fundamental": 0.25,
        "macro": 0.20,
        "policy": 0.15,
        "sentiment": 0.15,
    }


def _shadow_weights() -> dict[str, float]:
    return {
        "technical": 0.26,
        "fundamental": 0.24,
        "macro": 0.20,
        "policy": 0.15,
        "sentiment": 0.15,
    }


def _expected_delta() -> dict[str, float]:
    return {
        key: round(_shadow_weights()[key] - _production_weights()[key], 10)
        for key in _production_weights()
    }


def _assert_digest_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["digest_only"] is True
    assert payload["governance_only"] is True
    assert payload["apply_executed_by_digest"] is False
    assert payload["rollback_executed_by_digest"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["safe_for_scheduler"] is True


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
