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
from ai_trading_system.trading_engine.parameter_governance_web_view import (
    write_parameter_governance_web_view,
)


def test_parameter_governance_web_view_renders_valid_summary(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)

    metadata = _render(context)

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] == "RENDERED"
    html_path = Path(metadata["output_artifacts"]["html"]["path"])
    metadata_path = Path(metadata["output_artifacts"]["metadata"]["path"])
    assert html_path.exists()
    assert metadata_path.exists()
    html = html_path.read_text(encoding="utf-8")
    assert "ROLLBACK_COMPLETED" in html
    assert "NONE" in html
    assert "Continue observation." in html
    assert "Production vs Shadow Weights" in html
    assert "technical" in html
    assert "+0.0100" in html
    assert "Shadow Review Status" in html
    assert "Promotion Lifecycle Timeline" in html
    assert "pending_apply" in html
    assert "Safety Boundary Audit" in html
    assert "Artifact Links / Paths" in html
    assert metadata["render_summary"]["governance_state"] == "ROLLBACK_COMPLETED"
    assert metadata["render_summary"]["action_required"] is False
    assert metadata["render_summary"]["safety_boundary_status"] == "PASS"


@pytest.mark.parametrize(
    "overrides",
    [
        {"production_effect": "profile_updated"},
        {"governance_only": False},
        {"broker_execution": True},
        {"replay_execution": True},
        {"trading_execution": True},
    ],
)
def test_parameter_governance_web_view_safety_block(
    tmp_path: Path,
    overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path)
    _write_summary(context, overrides=overrides)

    metadata = _render(context)

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] == "SAFETY_BLOCKED"
    assert metadata["safety_validation"]["status"] == "FAIL"
    html = Path(metadata["output_artifacts"]["html"]["path"]).read_text(encoding="utf-8")
    assert "Web view render blocked because governance summary safety fields are invalid." in html


def test_parameter_governance_web_view_missing_input(tmp_path: Path) -> None:
    context = _write_context(tmp_path)

    metadata = _render(context)

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] == "INPUT_MISSING"
    assert Path(metadata["output_artifacts"]["html"]["path"]).exists()
    assert Path(metadata["output_artifacts"]["metadata"]["path"]).exists()


def test_parameter_governance_web_view_invalid_json(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    context["summary_path"].parent.mkdir(parents=True, exist_ok=True)
    context["summary_path"].write_text("{not json", encoding="utf-8")

    metadata = _render(context)

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] == "INPUT_INVALID"
    assert metadata["safety_validation"]["status"] == "FAIL"


def test_parameter_governance_web_view_invalid_task_id(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(context, overrides={"task_id": "TRADING-018F"})

    metadata = _render(context)

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] in {"INPUT_INVALID", "SAFETY_BLOCKED"}
    assert metadata["safety_validation"]["summary_task_id_valid"] is False


def test_parameter_governance_web_view_escapes_json_strings(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "audit_findings": {
                "critical_findings": ["bad <script>alert(1)</script>"],
                "warnings": ["path contains <unsafe&value>"],
                "notes": [],
            },
            "input_artifacts": {
                **_input_artifacts(context),
                "latest_multi_day_review": {
                    "status": "FOUND",
                    "path": "data/<review&unsafe>.json",
                    "exists": True,
                    "sha256": "abc",
                },
            },
        },
    )

    metadata = _render(context)
    html = Path(metadata["output_artifacts"]["html"]["path"]).read_text(encoding="utf-8")

    _assert_web_view_invariants(metadata)
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<script>alert(1)</script>" not in html
    assert "data/&lt;review&amp;unsafe&gt;.json" in html


def test_parameter_governance_web_view_status_rendering(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "governance_state": "SAFETY_ANOMALY",
            "action_required": True,
            "action_level": "URGENT",
            "recommended_action": "Investigate safety anomaly.",
            "shadow_state": {
                "status": "MISSING",
                "weights": {},
                "weights_sum_valid": False,
                "delta_from_production": {},
            },
            "pending_items": {
                "pending_proposal_review": False,
                "pending_preflight": False,
                "pending_apply": True,
                "pending_rollback": False,
                "pending_lifecycle_audit": False,
            },
            "audit_findings": {
                "critical_findings": ["lifecycle_audit:safety_anomaly"],
                "warnings": ["production_shadow_weight_keys_mismatch"],
                "notes": [],
            },
            "safety_boundary_audit": {
                "status": "FAIL",
                "latest_lifecycle_has_safety_anomaly": True,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
                "production_effect_from_governance": "none",
                "blocking_reasons": ["lifecycle_audit:safety_anomaly"],
            },
        },
    )

    metadata = _render(context)
    html = Path(metadata["output_artifacts"]["html"]["path"]).read_text(encoding="utf-8")

    _assert_web_view_invariants(metadata)
    assert metadata["render_decision"] == "RENDERED"
    assert "URGENT: Safety Anomaly Detected" in html
    assert "lifecycle_audit:safety_anomaly" in html
    assert "Manual approval/apply may be required." in html
    assert "NOT_AVAILABLE" in html


def test_parameter_governance_web_view_weight_key_mismatch_warning(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_summary(
        context,
        overrides={
            "shadow_state": {
                "status": "AVAILABLE",
                "weights": {"technical": 0.27, "quality": 0.73},
                "weights_sum_valid": True,
                "delta_from_production": {"technical": 0.02, "quality": 0.73},
            }
        },
    )

    metadata = _render(context)
    html = Path(metadata["output_artifacts"]["html"]["path"]).read_text(encoding="utf-8")

    _assert_web_view_invariants(metadata)
    assert "Weight key mismatch warning" in html


def test_dashboard_reads_web_view_metadata_without_triggering_pipelines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_context(tmp_path)
    _write_summary(context)
    metadata = _render(context)
    context["summary_path"].unlink()
    dashboard_metadata = _write_dashboard_metadata(tmp_path, context["as_of"])

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
            "ai_trading_system.trading_engine.parameter_governance_summary",
            "ai_trading_system.trading_engine.parameter_governance_web_view",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=context["as_of"],
        metadata_path=dashboard_metadata,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    _assert_web_view_invariants(metadata)
    dashboard_summary = payload["parameter_governance_web_view"]
    assert dashboard_summary["render_decision"] == "RENDERED"
    assert dashboard_summary["governance_state"] == "ROLLBACK_COMPLETED"
    assert dashboard_summary["action_required"] is False
    assert dashboard_summary["action_level"] == "NONE"
    assert dashboard_summary["safety_boundary_status"] == "PASS"
    assert dashboard_summary["critical_findings_count"] == 0
    assert dashboard_summary["warnings_count"] == 0
    assert dashboard_summary["latest_web_view_html_path"] == (
        metadata["output_artifacts"]["html"]["path"]
    )
    assert dashboard_summary["latest_render_metadata_path"] == (
        metadata["output_artifacts"]["metadata"]["path"]
    )
    assert dashboard_summary["production_effect"] == "none"
    assert dashboard_summary["manual_review_only"] is True
    assert dashboard_summary["governance_only"] is True
    assert dashboard_summary["web_view_only"] is True
    assert dashboard_summary["apply_executed_by_web_view"] is False
    assert dashboard_summary["rollback_executed_by_web_view"] is False
    assert dashboard_summary["broker_execution"] is False
    assert dashboard_summary["replay_execution"] is False
    assert dashboard_summary["trading_execution"] is False
    assert "Parameter Governance Web View" in html


def _render(context: dict[str, Any]) -> dict[str, Any]:
    metadata = write_parameter_governance_web_view(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )
    _assert_web_view_invariants(metadata)
    return metadata


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
        "latest_promotion_proposal": _artifact(
            base / "promotion" / "proposals" / "shadow_promotion_proposal_2026-05-23.json"
        ),
        "latest_apply_preflight": _artifact(
            base / "promotion" / "preflight" / "shadow_promotion_apply_preflight_2026-05-23.json"
        ),
        "latest_apply_result": _artifact(
            base / "promotion" / "apply" / "shadow_promotion_apply_result_2026-05-23.json"
        ),
        "latest_rollback_result": _artifact(
            base
            / "promotion"
            / "rollback_results"
            / "shadow_promotion_rollback_result_2026-05-23.json"
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


def _assert_web_view_invariants(metadata: dict[str, Any]) -> None:
    assert metadata["production_effect"] == "none"
    assert metadata["manual_review_only"] is True
    assert metadata["governance_only"] is True
    assert metadata["web_view_only"] is True
    assert metadata["apply_executed_by_web_view"] is False
    assert metadata["rollback_executed_by_web_view"] is False
    assert metadata["broker_execution"] is False
    assert metadata["replay_execution"] is False
    assert metadata["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
