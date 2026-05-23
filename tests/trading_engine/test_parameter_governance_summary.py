from __future__ import annotations

import builtins
import hashlib
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
from ai_trading_system.trading_engine.parameter_governance_summary import (
    build_parameter_governance_summary_payload,
    write_parameter_governance_summary_report,
)


def test_governance_summary_shadow_learning_with_production_and_shadow(tmp_path: Path) -> None:
    context = _write_context(tmp_path)

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "SHADOW_LEARNING"
    assert payload["action_level"] == "WATCH"
    assert payload["production_state"]["weights"] == _production_weights()
    assert payload["shadow_state"]["weights"] == _shadow_weights()
    assert payload["shadow_state"]["delta_from_production"] == _expected_delta()


def test_governance_summary_shadow_review_ready(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_review(context["data_root"])

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "SHADOW_REVIEW_READY"
    assert payload["action_required"] is True
    assert payload["action_level"] == "REVIEW_REQUIRED"


def test_governance_summary_proposal_pending_review(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_proposal(context["data_root"])

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "PROPOSAL_PENDING_REVIEW"
    assert payload["pending_items"]["pending_preflight"] is True
    assert payload["action_level"] == "REVIEW_REQUIRED"


def test_governance_summary_preflight_ready(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_proposal(context["data_root"])
    _write_preflight(context["data_root"])

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "PREFLIGHT_READY"
    assert payload["pending_items"]["pending_apply"] is True
    assert payload["action_level"] == "APPROVAL_REQUIRED"


def test_governance_summary_applied_needs_monitoring(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_apply(context["data_root"], snapshot_exists=True)

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "APPLIED_NEEDS_MONITORING"
    assert payload["pending_items"]["pending_rollback"] is True
    assert payload["pending_items"]["pending_lifecycle_audit"] is True
    assert payload["action_level"] in {"WATCH", "ROLLBACK_REVIEW_REQUIRED"}


def test_governance_summary_rollback_completed(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_apply(context["data_root"], snapshot_exists=True)
    _write_rollback(context["data_root"])
    _write_lifecycle(context["data_root"], decision="COMPLETE_WITH_ROLLBACK")

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "ROLLBACK_COMPLETED"
    assert payload["action_required"] is False
    assert payload["action_level"] == "NONE"
    assert payload["safety_boundary_audit"]["status"] == "PASS"


@pytest.mark.parametrize(
    ("writer", "expected_reason"),
    [
        (
            lambda data_root: _write_lifecycle(data_root, decision="SAFETY_ANOMALY"),
            "lifecycle_audit:lifecycle_decision_safety_anomaly",
        ),
        (
            lambda data_root: _write_review(data_root, overrides={"broker_execution": True}),
            "latest_multi_day_review:broker_execution_true",
        ),
        (
            lambda data_root: _write_proposal(data_root, overrides={"replay_execution": True}),
            "latest_promotion_proposal:replay_execution_true",
        ),
        (
            lambda data_root: _write_preflight(data_root, overrides={"trading_execution": True}),
            "latest_apply_preflight:trading_execution_true",
        ),
        (
            lambda data_root: _write_apply(data_root, snapshot_exists=False),
            "latest_apply_result:applied_but_rollback_snapshot_missing",
        ),
        (
            lambda data_root: _write_rollback(
                data_root,
                overrides={"post_rollback_validation": {"status": "FAIL"}},
            ),
            "latest_rollback_result:post_rollback_validation_not_pass",
        ),
    ],
)
def test_governance_summary_safety_anomalies(
    tmp_path: Path,
    writer: Any,
    expected_reason: str,
) -> None:
    context = _write_context(tmp_path)
    writer(context["data_root"])

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "SAFETY_ANOMALY"
    assert payload["action_required"] is True
    assert payload["action_level"] == "URGENT"
    assert expected_reason in payload["audit_findings"]["critical_findings"]
    assert payload["safety_boundary_audit"]["status"] == "FAIL"


def test_governance_summary_pending_lifecycle_audit_when_event_newer_than_audit(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_apply(context["data_root"], as_of=date(2026, 5, 23), snapshot_exists=True)
    _write_lifecycle(
        context["data_root"],
        as_of=date(2026, 5, 22),
        decision="COMPLETE_APPLIED_NO_ROLLBACK",
    )

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["pending_items"]["pending_lifecycle_audit"] is True


def test_governance_summary_weight_warnings_for_invalid_sum_and_key_mismatch(
    tmp_path: Path,
) -> None:
    context = _write_context(
        tmp_path,
        shadow_weights={"technical": 0.6, "fundamental": 0.2, "macro": 0.1},
    )

    payload = _build_summary(context)

    _assert_governance_invariants(payload)
    assert payload["shadow_state"]["weights_sum_valid"] is False
    assert "shadow_weights_sum_invalid" in payload["audit_findings"]["warnings"]
    assert "production_shadow_weight_keys_mismatch" in payload["audit_findings"]["warnings"]


def test_governance_summary_write_outputs_and_markdown(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_apply(context["data_root"], snapshot_exists=True)
    _write_rollback(context["data_root"])
    _write_lifecycle(context["data_root"], decision="COMPLETE_WITH_ROLLBACK")

    payload = write_parameter_governance_summary_report(
        as_of=context["as_of"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        shadow_weights_file=context["shadow_weights"],
        generated_at=_fixed_generated_at(),
    )

    _assert_governance_invariants(payload)
    assert payload["governance_state"] == "ROLLBACK_COMPLETED"
    assert Path(payload["outputs"]["json"]).exists()
    markdown_path = Path(payload["outputs"]["markdown"])
    assert markdown_path.exists()
    assert "Parameter Governance Summary" in markdown_path.read_text(encoding="utf-8")
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()


def test_governance_dashboard_reads_summary_artifact_without_triggering_pipelines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_context(tmp_path)
    summary = write_parameter_governance_summary_report(
        as_of=context["as_of"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        shadow_weights_file=context["shadow_weights"],
        generated_at=_fixed_generated_at(),
    )
    _remove_governance_inputs(context)
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
            "ai_trading_system.trading_engine.parameter_governance_summary",
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

    dashboard_summary = payload["parameter_governance_summary"]
    assert dashboard_summary["governance_state"] == summary["governance_state"]
    assert dashboard_summary["action_required"] is summary["action_required"]
    assert dashboard_summary["action_level"] == summary["action_level"]
    assert dashboard_summary["review_decision"] == "MISSING"
    assert dashboard_summary["safety_boundary_status"] == "PASS"
    assert dashboard_summary["critical_findings_count"] == 0
    assert dashboard_summary["warnings_count"] == 0
    assert dashboard_summary["latest_summary_markdown_path"] == summary["outputs"]["markdown"]
    assert "Parameter Governance Summary" in html
    assert "parameter_governance_summary_2026-05-23.md" in html
    _assert_governance_invariants(summary)


def _build_summary(context: dict[str, Any]) -> dict[str, Any]:
    payload = build_parameter_governance_summary_payload(
        as_of=context["as_of"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        shadow_weights_file=context["shadow_weights"],
        generated_at=_fixed_generated_at(),
    )
    _assert_governance_invariants(payload)
    return payload


def _write_context(
    tmp_path: Path,
    *,
    shadow_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    as_of = date(2026, 5, 23)
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    production_profile = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    shadow_path = (
        data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"
    )
    reports_dir.mkdir(parents=True, exist_ok=True)
    production_profile.parent.mkdir(parents=True, exist_ok=True)
    production_profile.write_text(
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
    _write_json(
        shadow_path,
        {
            "schema_version": "1.0",
            "report_type": "current_shadow_weights",
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "last_updated_date": as_of.isoformat(),
            "weights": shadow_weights or _shadow_weights(),
        },
    )
    return {
        "as_of": as_of,
        "data_root": data_root,
        "reports_dir": reports_dir,
        "production_profile": production_profile,
        "shadow_weights": shadow_path,
    }


def _write_review(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    overrides: dict[str, Any] | None = None,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "comparison"
        / "reviews"
        / f"shadow_vs_production_review_{as_of.isoformat()}.json"
    )
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_vs_production_multi_day_review",
        "task_id": "TRADING-018C2",
        "date": as_of.isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "review_decision": "SHADOW_LOOKS_BETTER",
        "available_comparison_days": 7,
        "average_score_delta": 0.024,
        "decision_difference_count": 2,
        "shadow_risk_flag_delta_total": 0,
    }
    payload.update(overrides or {})
    _write_json(path, payload)


def _write_proposal(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    overrides: dict[str, Any] | None = None,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_proposal",
        "task_id": "TRADING-018D",
        "date": as_of.isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_proposed": True,
        "promotion_executed": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
    }
    payload.update(overrides or {})
    _write_json(path, payload)


def _write_preflight(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    overrides: dict[str, Any] | None = None,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "preflight"
        / f"shadow_promotion_apply_preflight_{as_of.isoformat()}.json"
    )
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_preflight",
        "task_id": "TRADING-018E1",
        "date": as_of.isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_executed": False,
        "apply_executed": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "preflight_decision": "PASS",
    }
    payload.update(overrides or {})
    _write_json(path, payload)


def _write_apply(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    snapshot_exists: bool,
    overrides: dict[str, Any] | None = None,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "apply"
        / f"shadow_promotion_apply_result_{as_of.isoformat()}.json"
    )
    snapshot_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback"
        / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
    )
    snapshot_sha = ""
    if snapshot_exists:
        _write_json(snapshot_path, {"weights": _production_weights()})
        snapshot_sha = _sha256(snapshot_path)
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_result",
        "task_id": "TRADING-018E2",
        "date": as_of.isoformat(),
        "production_effect": "profile_updated_only_if_apply_executed",
        "manual_review_only": True,
        "promotion_executed": True,
        "apply_executed": True,
        "safe_for_scheduler": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "apply_decision": "APPLIED",
        "rollback": {
            "snapshot_created": snapshot_exists,
            "snapshot_path": str(snapshot_path),
            "snapshot_sha256": snapshot_sha,
            "snapshot_file_sha256": snapshot_sha,
        },
        "post_apply_validation": {"status": "PASS"},
    }
    payload.update(overrides or {})
    _write_json(path, payload)


def _write_rollback(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    overrides: dict[str, Any] | None = None,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback_results"
        / f"shadow_promotion_rollback_result_{as_of.isoformat()}.json"
    )
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_rollback_result",
        "task_id": "TRADING-018E3",
        "date": as_of.isoformat(),
        "production_effect": "profile_rolled_back_only_if_rollback_executed",
        "manual_review_only": True,
        "rollback_executed": True,
        "safe_for_scheduler": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "rollback_decision": "ROLLED_BACK",
        "post_rollback_validation": {"status": "PASS"},
    }
    payload.update(overrides or {})
    _write_json(path, payload)


def _write_lifecycle(
    data_root: Path,
    *,
    as_of: date = date(2026, 5, 23),
    decision: str,
) -> None:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "audit"
        / f"shadow_promotion_lifecycle_audit_{as_of.isoformat()}.json"
    )
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "report_type": "shadow_promotion_lifecycle_audit",
            "task_id": "TRADING-018F",
            "date": as_of.isoformat(),
            "production_effect": "none",
            "manual_review_only": True,
            "audit_only": True,
            "apply_executed_by_audit": False,
            "rollback_executed_by_audit": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "lifecycle_decision": decision,
        },
    )


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


def _remove_governance_inputs(context: dict[str, Any]) -> None:
    for key in ("production_profile", "shadow_weights"):
        path = context[key]
        if path.exists():
            path.unlink()


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


def _assert_governance_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["governance_only"] is True
    assert payload["apply_executed_by_governance"] is False
    assert payload["rollback_executed_by_governance"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["safe_for_scheduler"] is True


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
