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
from ai_trading_system.trading_engine.shadow_promotion_lifecycle_audit import (
    write_shadow_promotion_lifecycle_audit_report,
)


@pytest.mark.parametrize(
    ("stages", "expected_decision"),
    [
        (("proposal",), "PROPOSAL_ONLY"),
        (("proposal", "preflight"), "PREFLIGHT_ONLY"),
        (("proposal", "preflight", "apply"), "COMPLETE_APPLIED_NO_ROLLBACK"),
        (("proposal", "preflight", "apply", "rollback"), "COMPLETE_WITH_ROLLBACK"),
    ],
)
def test_lifecycle_audit_artifact_coverage_decisions(
    tmp_path: Path,
    stages: tuple[str, ...],
    expected_decision: str,
) -> None:
    context = _write_context(tmp_path, stages=stages)

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == expected_decision
    assert payload["artifact_chain"]["status"] == "PASS"
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()
    if expected_decision == "COMPLETE_APPLIED_NO_ROLLBACK":
        assert payload["weight_lifecycle"]["production_weights_after_rollback"] is None
        assert any(
            "rollback result was not found" in item
            for item in payload["audit_findings"]["warnings"]
        )


def test_lifecycle_audit_missing_proposal_with_apply_is_incomplete(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    context["proposal_path"].unlink()

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == "INCOMPLETE_ARTIFACTS"
    assert payload["input_artifacts"]["proposal"]["status"] == "MISSING"


def test_lifecycle_audit_apply_result_blocked(tmp_path: Path) -> None:
    context = _write_context(
        tmp_path,
        stages=("proposal", "preflight", "apply"),
        apply_overrides={
            "apply_decision": "DANGER_FLAG_MISSING",
            "apply_executed": False,
            "promotion_executed": False,
            "production_effect": "none",
        },
        apply_rollback_overrides={"snapshot_created": False, "snapshot_sha256": ""},
    )

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == "APPLY_FAILED_OR_BLOCKED"
    assert payload["apply_summary"]["apply_executed"] is False


@pytest.mark.parametrize(
    "rollback_overrides",
    [
        {
            "rollback_decision": "DANGER_FLAG_MISSING",
            "rollback_executed": False,
            "production_effect": "none",
        },
        {
            "rollback_decision": "ROLLED_BACK",
            "rollback_executed": False,
            "production_effect": "none",
        },
    ],
)
def test_lifecycle_audit_rollback_result_blocked(
    tmp_path: Path,
    rollback_overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path, rollback_overrides=rollback_overrides)

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == "ROLLBACK_FAILED_OR_BLOCKED"
    assert payload["rollback_summary"]["rollback_executed"] is False


@pytest.mark.parametrize(
    ("context_kwargs", "expected_reason"),
    [
        (
            {"preflight_proposal_ref_overrides": {"sha256": "bad"}},
            "proposal_to_preflight:sha256_mismatch",
        ),
        (
            {"apply_preflight_ref_overrides": {"sha256": "bad"}},
            "preflight_to_apply:sha256_mismatch",
        ),
        (
            {"rollback_apply_ref_overrides": {"sha256": "bad"}},
            "apply_to_rollback:sha256_mismatch",
        ),
        (
            {"rollback_snapshot_ref_overrides": {"sha256": "bad"}},
            "apply_to_rollback:rollback_snapshot_sha256_mismatch",
        ),
    ],
)
def test_lifecycle_audit_artifact_chain_mismatch_is_safety_anomaly(
    tmp_path: Path,
    context_kwargs: dict[str, Any],
    expected_reason: str,
) -> None:
    context = _write_context(tmp_path, **context_kwargs)

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == "SAFETY_ANOMALY"
    assert expected_reason in payload["audit_findings"]["critical_findings"]


@pytest.mark.parametrize(
    ("context_kwargs", "expected_reason"),
    [
        ({"apply_overrides": {"broker_execution": True}}, "apply_result:broker_execution_true"),
        ({"preflight_overrides": {"replay_execution": True}}, "preflight:replay_execution_true"),
        ({"proposal_overrides": {"trading_execution": True}}, "proposal:trading_execution_true"),
        (
            {"preflight_overrides": {"production_effect": "profile_updated"}},
            "preflight:production_effect_not_none",
        ),
        (
            {"proposal_overrides": {"promotion_executed": True}},
            "proposal:promotion_executed_true",
        ),
        (
            {"preflight_overrides": {"apply_executed": True}},
            "preflight:apply_executed_true",
        ),
        (
            {
                "stages": ("proposal", "preflight", "apply"),
                "apply_rollback_overrides": {"snapshot_created": False, "snapshot_sha256": ""},
            },
            "apply_result:applied_but_rollback_snapshot_missing",
        ),
        (
            {
                "rollback_overrides": {
                    "rollback_decision": "DANGER_FLAG_MISSING",
                    "rollback_executed": True,
                }
            },
            "rollback_result:rollback_executed_true_but_decision_not_rolled_back",
        ),
        (
            {"rollback_post_overrides": {"status": "FAIL"}},
            "rollback_result:post_rollback_validation_not_pass",
        ),
    ],
)
def test_lifecycle_audit_safety_boundary_anomalies(
    tmp_path: Path,
    context_kwargs: dict[str, Any],
    expected_reason: str,
) -> None:
    context = _write_context(tmp_path, **context_kwargs)

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    assert payload["lifecycle_decision"] == "SAFETY_ANOMALY"
    assert expected_reason in payload["audit_findings"]["critical_findings"]
    assert payload["safety_boundary_audit"]["status"] == "FAIL"


def test_lifecycle_audit_extracts_weight_lifecycle(tmp_path: Path) -> None:
    context = _write_context(tmp_path)

    payload = _run_audit(context)

    _assert_audit_invariants(payload)
    weights = payload["weight_lifecycle"]
    assert weights["production_weights_before_apply"] == _production_weights()
    assert weights["production_weights_after_apply"] == _shadow_weights()
    assert weights["production_weights_after_rollback"] == _production_weights()
    assert weights["apply_delta"] == {
        "fundamental": -0.01,
        "macro": 0.0,
        "policy": 0.0,
        "sentiment": 0.0,
        "technical": 0.01,
    }
    assert weights["rollback_delta"] == {
        "fundamental": 0.01,
        "macro": 0.0,
        "policy": 0.0,
        "sentiment": 0.0,
        "technical": -0.01,
    }
    assert weights["net_delta_after_lifecycle"] == {
        "fundamental": 0.0,
        "macro": 0.0,
        "policy": 0.0,
        "sentiment": 0.0,
        "technical": 0.0,
    }


def test_lifecycle_dashboard_reads_audit_artifact_without_triggering_pipelines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_context(tmp_path)
    audit_payload = _run_audit(context)
    _remove_source_artifacts(context)
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
            "ai_trading_system.trading_engine.shadow_promotion_lifecycle_audit",
            "ai_trading_system.trading_engine.shadow_promotion_rollback",
            "ai_trading_system.trading_engine.shadow_promotion_apply",
            "ai_trading_system.trading_engine.shadow_promotion_apply_preflight",
            "ai_trading_system.trading_engine.shadow_promotion_proposal",
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
    dashboard_payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = dashboard_payload["shadow_promotion_lifecycle_audit"]
    assert summary["lifecycle_decision"] == "COMPLETE_WITH_ROLLBACK"
    assert summary["promotion_date"] == context["promotion_date"].isoformat()
    assert summary["proposal_status"] == "FOUND"
    assert summary["preflight_status"] == "FOUND"
    assert summary["apply_status"] == "FOUND"
    assert summary["rollback_status"] == "FOUND"
    assert summary["safety_boundary_status"] == "PASS"
    assert summary["critical_findings_count"] == 0
    assert summary["warnings_count"] == 0
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert summary["latest_audit_markdown_path"] == audit_payload["outputs"]["markdown"]
    assert "Shadow Promotion Lifecycle Audit" in html
    assert "shadow_promotion_lifecycle_audit_2026-05-23.md" in html
    _assert_audit_invariants(audit_payload)


def _run_audit(context: dict[str, Any]) -> dict[str, Any]:
    return write_shadow_promotion_lifecycle_audit_report(
        as_of=context["as_of"],
        promotion_date=context["promotion_date"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )


def _write_context(
    tmp_path: Path,
    *,
    stages: tuple[str, ...] = ("proposal", "preflight", "apply", "rollback"),
    proposal_overrides: dict[str, Any] | None = None,
    preflight_overrides: dict[str, Any] | None = None,
    preflight_proposal_ref_overrides: dict[str, Any] | None = None,
    apply_overrides: dict[str, Any] | None = None,
    apply_preflight_ref_overrides: dict[str, Any] | None = None,
    apply_rollback_overrides: dict[str, Any] | None = None,
    rollback_overrides: dict[str, Any] | None = None,
    rollback_apply_ref_overrides: dict[str, Any] | None = None,
    rollback_snapshot_ref_overrides: dict[str, Any] | None = None,
    rollback_post_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    as_of = date(2026, 5, 23)
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    proposal_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )
    preflight_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "preflight"
        / f"shadow_promotion_apply_preflight_{as_of.isoformat()}.json"
    )
    apply_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "apply"
        / f"shadow_promotion_apply_result_{as_of.isoformat()}.json"
    )
    rollback_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback_results"
        / f"shadow_promotion_rollback_result_{as_of.isoformat()}.json"
    )
    snapshot_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback"
        / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
    )
    target_before_sha = "target-profile-before-sha"
    target_after_sha = "target-profile-after-sha"

    if "proposal" in stages:
        _write_json(proposal_path, _proposal_payload(as_of, proposal_overrides))
    if "preflight" in stages:
        proposal_ref = {
            "status": "FOUND",
            "path": str(proposal_path),
            "sha256": _sha256(proposal_path) if proposal_path.exists() else "missing",
        }
        proposal_ref.update(preflight_proposal_ref_overrides or {})
        _write_json(
            preflight_path,
            _preflight_payload(
                as_of,
                proposal_ref=proposal_ref,
                target_before_sha=target_before_sha,
                overrides=preflight_overrides,
            ),
        )
    if "apply" in stages:
        _write_json(snapshot_path, {"weights": _production_weights()})
        snapshot_sha = _sha256(snapshot_path)
        preflight_ref = {
            "status": "FOUND",
            "path": str(preflight_path),
            "sha256": _sha256(preflight_path) if preflight_path.exists() else "missing",
        }
        preflight_ref.update(apply_preflight_ref_overrides or {})
        rollback_snapshot = {
            "snapshot_created": True,
            "snapshot_path": str(snapshot_path),
            "snapshot_sha256": snapshot_sha,
            "snapshot_file_sha256": snapshot_sha,
            "snapshot_sha256_path": str(snapshot_path.with_suffix(".sha256")),
            "rollback_supported": True,
        }
        rollback_snapshot.update(apply_rollback_overrides or {})
        _write_json(
            apply_path,
            _apply_payload(
                as_of,
                preflight_ref=preflight_ref,
                target_before_sha=target_before_sha,
                target_after_sha=target_after_sha,
                rollback_snapshot=rollback_snapshot,
                overrides=apply_overrides,
            ),
        )
    if "rollback" in stages:
        snapshot_sha = _sha256(snapshot_path)
        apply_ref = {
            "status": "FOUND",
            "path": str(apply_path),
            "sha256": _sha256(apply_path) if apply_path.exists() else "missing",
        }
        apply_ref.update(rollback_apply_ref_overrides or {})
        snapshot_ref = {
            "status": "FOUND",
            "path": str(snapshot_path),
            "sha256": snapshot_sha,
        }
        snapshot_ref.update(rollback_snapshot_ref_overrides or {})
        post_rollback = {"status": "PASS", "blocking_reasons": []}
        post_rollback.update(rollback_post_overrides or {})
        _write_json(
            rollback_path,
            _rollback_payload(
                as_of,
                apply_ref=apply_ref,
                snapshot_ref=snapshot_ref,
                target_after_sha=target_after_sha,
                overrides=rollback_overrides,
                post_rollback=post_rollback,
            ),
        )
    return {
        "as_of": as_of,
        "promotion_date": as_of,
        "data_root": data_root,
        "reports_dir": reports_dir,
        "proposal_path": proposal_path,
        "preflight_path": preflight_path,
        "apply_path": apply_path,
        "rollback_path": rollback_path,
        "snapshot_path": snapshot_path,
    }


def _proposal_payload(as_of: date, overrides: dict[str, Any] | None) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_proposal",
        "task_id": "TRADING-018D",
        "date": as_of.isoformat(),
        "mode": "manual_promotion_proposal_only",
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_proposed": True,
        "promotion_executed": False,
        "safe_for_production": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
        "production_weights": _production_weights(),
        "proposed_production_weights": _shadow_weights(),
    }
    payload.update(overrides or {})
    return payload


def _preflight_payload(
    as_of: date,
    *,
    proposal_ref: dict[str, Any],
    target_before_sha: str,
    overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_preflight",
        "task_id": "TRADING-018E1",
        "date": as_of.isoformat(),
        "mode": "approved_apply_preflight_only",
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_executed": False,
        "apply_executed": False,
        "preflight_only": True,
        "safe_for_production": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "preflight_decision": "PASS",
        "input_artifacts": {"promotion_proposal": proposal_ref},
        "proposal_validation": {
            "status": "PASS",
            "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
        },
        "diff_preview": {
            "target_profile_sha256_before": target_before_sha,
            "production_weights_before": _production_weights(),
            "production_weights_after_preview": _shadow_weights(),
            "delta": _apply_delta(),
        },
    }
    payload.update(overrides or {})
    return payload


def _apply_payload(
    as_of: date,
    *,
    preflight_ref: dict[str, Any],
    target_before_sha: str,
    target_after_sha: str,
    rollback_snapshot: dict[str, Any],
    overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_result",
        "task_id": "TRADING-018E2",
        "date": as_of.isoformat(),
        "mode": "explicit_approved_apply",
        "production_effect": "profile_updated_only_if_apply_executed",
        "manual_review_only": True,
        "promotion_executed": True,
        "apply_executed": True,
        "safe_for_scheduler": False,
        "safe_for_production": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "apply_decision": "APPLIED",
        "input_artifacts": {"preflight": preflight_ref},
        "preflight_validation": {
            "status": "PASS",
            "preflight_decision": "PASS",
        },
        "target_profile_validation": {
            "status": "PASS",
            "path": "config/weights/weight_profile_current.yaml",
            "sha256_expected_from_preflight": target_before_sha,
            "sha256_after": target_after_sha,
        },
        "diff_applied": {
            "production_weights_before": _production_weights(),
            "production_weights_after": _shadow_weights(),
            "delta": _apply_delta(),
        },
        "rollback": rollback_snapshot,
        "post_apply_validation": {"status": "PASS", "blocking_reasons": []},
    }
    payload.update(overrides or {})
    return payload


def _rollback_payload(
    as_of: date,
    *,
    apply_ref: dict[str, Any],
    snapshot_ref: dict[str, Any],
    target_after_sha: str,
    overrides: dict[str, Any] | None,
    post_rollback: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_rollback_result",
        "task_id": "TRADING-018E3",
        "date": as_of.isoformat(),
        "mode": "explicit_approved_rollback",
        "production_effect": "profile_rolled_back_only_if_rollback_executed",
        "manual_review_only": True,
        "rollback_executed": True,
        "safe_for_scheduler": False,
        "safe_for_production": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "rollback_decision": "ROLLED_BACK",
        "input_artifacts": {
            "apply_result": apply_ref,
            "rollback_snapshot": snapshot_ref,
        },
        "apply_result_validation": {
            "status": "PASS",
            "apply_decision": "APPLIED",
        },
        "target_profile_validation": {
            "status": "PASS",
            "sha256_expected_current": target_after_sha,
            "sha256_after_rollback": "target-profile-rollback-sha",
        },
        "rollback_applied": {
            "production_weights_before_rollback": _shadow_weights(),
            "production_weights_after_rollback": _production_weights(),
            "delta": _rollback_delta(),
        },
        "post_rollback_validation": post_rollback,
    }
    payload.update(overrides or {})
    return payload


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


def _remove_source_artifacts(context: dict[str, Any]) -> None:
    for key in ("proposal_path", "preflight_path", "apply_path", "rollback_path", "snapshot_path"):
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


def _apply_delta() -> dict[str, float]:
    return {
        key: round(_shadow_weights()[key] - _production_weights()[key], 10)
        for key in _production_weights()
    }


def _rollback_delta() -> dict[str, float]:
    return {
        key: round(_production_weights()[key] - _shadow_weights()[key], 10)
        for key in _production_weights()
    }


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _assert_audit_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["audit_only"] is True
    assert payload["apply_executed_by_audit"] is False
    assert payload["rollback_executed_by_audit"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["safe_for_scheduler"] is True


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
