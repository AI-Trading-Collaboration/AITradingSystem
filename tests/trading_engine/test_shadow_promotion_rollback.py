from __future__ import annotations

import builtins
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

import ai_trading_system.trading_engine.shadow_promotion_rollback as rollback_module
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.shadow_promotion_rollback import (
    write_shadow_promotion_rollback_report,
)


def test_rollback_missing_apply_result_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["apply_result_path"].unlink()

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "INSUFFICIENT_DATA"
    assert payload["rollback_executed"] is False
    assert payload["production_effect"] == "none"
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])


def test_rollback_missing_approval_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["approval_path"].unlink()

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["rollback_approval"]["status"] == "MISSING"


def test_rollback_missing_snapshot_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["rollback_snapshot_path"].unlink()

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["rollback_snapshot"]["status"] == "MISSING"


def test_rollback_missing_target_profile_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["target_path"].unlink()

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["target_profile_before_rollback"]["status"] == "MISSING"


def test_rollback_requires_danger_flag_and_leaves_profile_unchanged(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)

    payload = _run_rollback(context, danger_flag_provided=False)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "DANGER_FLAG_MISSING"
    assert payload["rollback_executed"] is False
    assert payload["production_effect"] == "none"
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])
    assert Path(payload["outputs"]["json"]).exists()


@pytest.mark.parametrize(
    ("approval_overrides", "safety_overrides", "expected_reason"),
    [
        ({"approved": False}, None, "approved"),
        ({"approval_type": "shadow_promotion_apply"}, None, "approval_type"),
        (None, {"rollback_authorized": False}, "rollback_authorized"),
        (
            None,
            {"production_modification_authorized": False},
            "production_modification_authorized",
        ),
        (None, {"scheduler_execution_forbidden": False}, "scheduler_execution_forbidden"),
        (None, {"broker_execution_forbidden": False}, "broker_execution_forbidden"),
        (None, {"replay_execution_forbidden": False}, "replay_execution_forbidden"),
        (None, {"trading_execution_forbidden": False}, "trading_execution_forbidden"),
    ],
)
def test_rollback_rejects_invalid_approval(
    tmp_path: Path,
    approval_overrides: dict[str, Any] | None,
    safety_overrides: dict[str, Any] | None,
    expected_reason: str,
) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_overrides=approval_overrides,
        safety_overrides=safety_overrides,
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "APPROVAL_INVALID"
    assert expected_reason in payload["approval_validation"]["blocking_reasons"]
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])


@pytest.mark.parametrize(
    ("apply_result_overrides", "expected_reason"),
    [
        ({"apply_decision": "BLOCKED"}, "apply_decision"),
        ({"apply_executed": False}, "apply_executed"),
        ({"promotion_executed": False}, "promotion_executed"),
        ({"post_apply_validation": {"status": "FAIL"}}, "post_apply_validation_status"),
        ({"rollback": {}}, "rollback_snapshot_created"),
    ],
)
def test_rollback_rejects_invalid_apply_result(
    tmp_path: Path,
    apply_result_overrides: dict[str, Any],
    expected_reason: str,
) -> None:
    context = _write_valid_context(tmp_path, apply_result_overrides=apply_result_overrides)

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "APPLY_RESULT_INVALID"
    assert expected_reason in payload["apply_result_validation"]["blocking_reasons"]
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])


def test_rollback_rejects_approval_apply_hash_mismatch(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_apply_overrides={"apply_result_sha256": "bad"},
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "APPROVAL_INVALID"
    assert "apply_result_hash_match" in payload["approval_validation"]["blocking_reasons"]


def test_rollback_rejects_approval_snapshot_hash_mismatch(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_snapshot_overrides={"snapshot_sha256": "bad"},
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "APPROVAL_INVALID"
    assert "rollback_snapshot_hash_match" in payload["approval_validation"]["blocking_reasons"]


def test_rollback_blocks_when_approval_expected_current_hash_mismatches(
    tmp_path: Path,
) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_target_overrides={"expected_current_profile_sha256": "bad"},
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "TARGET_PROFILE_CHANGED"
    assert payload["rollback_executed"] is False


def test_rollback_rejects_snapshot_hash_mismatch_against_apply_result(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    _write_json(
        context["rollback_snapshot_path"],
        _production_profile(weights={**_production_weights(), "technical": 0.24}),
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "ROLLBACK_SNAPSHOT_INVALID"
    assert (
        "rollback_snapshot_hash_matches_apply_result"
        in payload["rollback_snapshot_validation"]["blocking_reasons"]
    )


def test_rollback_rejects_snapshot_with_invalid_weight_sum(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    _rewrite_apply_and_approval_for_snapshot(
        context,
        snapshot_weights={**_production_weights(), "technical": 0.35},
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "ROLLBACK_SNAPSHOT_INVALID"
    assert "weights_sum_valid" in payload["rollback_snapshot_validation"]["blocking_reasons"]


def test_rollback_rejects_snapshot_with_invalid_weight_keys(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    _rewrite_apply_and_approval_for_snapshot(
        context,
        snapshot_weights={"technical": 0.50, "fundamental": 0.50},
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "ROLLBACK_SNAPSHOT_INVALID"
    assert (
        "weight_keys_match_current" in payload["rollback_snapshot_validation"]["blocking_reasons"]
    )


def test_rollback_blocks_when_current_profile_changed_after_apply(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    _write_json(
        context["target_path"],
        _production_profile(weights={**_shadow_weights(), "technical": 0.25}),
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "TARGET_PROFILE_CHANGED"
    assert payload["rollback_executed"] is False


def test_rollback_rejects_target_profile_path_mismatch(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_target_overrides={
            "target_profile_path": str(tmp_path / "config" / "weights" / "other.json"),
        },
    )

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "TARGET_PROFILE_MISMATCH"
    assert "target_profile_path" in payload["target_profile_validation"]["blocking_reasons"]


def test_rollback_creates_current_snapshot_before_successful_restore(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    before_profile = json.loads(context["target_path"].read_text(encoding="utf-8"))

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "ROLLED_BACK"
    current_snapshot_path = Path(payload["current_snapshot"]["snapshot_path"])
    current_snapshot_sha_path = Path(payload["current_snapshot"]["snapshot_sha256_path"])
    assert current_snapshot_path.exists()
    assert current_snapshot_sha_path.exists()
    assert json.loads(current_snapshot_path.read_text(encoding="utf-8")) == before_profile
    assert (
        current_snapshot_sha_path.read_text(encoding="utf-8").strip()
        == context["target_sha_after_apply"]
    )


def test_rollback_blocks_when_current_snapshot_creation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def fail_snapshot(**_: Any) -> dict[str, Any]:
        raise OSError("snapshot denied")

    monkeypatch.setattr(rollback_module, "_create_current_snapshot", fail_snapshot)

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "CURRENT_SNAPSHOT_FAILED"
    assert payload["rollback_executed"] is False
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])


def test_successful_rollback_restores_weights_only_and_writes_outputs(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    before_profile = json.loads(context["target_path"].read_text(encoding="utf-8"))

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "ROLLED_BACK"
    assert payload["rollback_executed"] is True
    assert payload["production_effect"] == "profile_rolled_back_only_if_rollback_executed"
    assert payload["post_rollback_validation"]["status"] == "PASS"
    profile_after = json.loads(context["target_path"].read_text(encoding="utf-8"))
    assert profile_after["weights"] == _production_weights()
    assert profile_after["broker"] == before_profile["broker"]
    assert profile_after["risk_limits"] == before_profile["risk_limits"]
    assert payload["rollback_applied"]["changed_weight_keys"] == ["fundamental", "technical"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()


def test_rollback_reports_write_failed_when_atomic_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def fail_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        raise OSError(f"write denied: {path}:{payload}:{write_mode}")

    monkeypatch.setattr(rollback_module, "_atomic_write_profile", fail_write)

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "WRITE_FAILED"
    assert payload["rollback_executed"] is False
    assert context["target_sha_after_apply"] == _sha256(context["target_path"])


def test_rollback_reports_post_validation_failed_for_weight_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def corrupt_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        broken = dict(payload)
        broken["weights"] = _shadow_weights()
        _write_json(path, broken)

    monkeypatch.setattr(rollback_module, "_atomic_write_profile", corrupt_write)

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "POST_ROLLBACK_VALIDATION_FAILED"
    assert payload["rollback_executed"] is True
    assert (
        "weights_match_rollback_snapshot" in payload["post_rollback_validation"]["blocking_reasons"]
    )


def test_rollback_reports_post_validation_failed_for_non_allowed_field_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def corrupt_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        broken = dict(payload)
        broken["broker"] = {"enabled": True}
        _write_json(path, broken)

    monkeypatch.setattr(rollback_module, "_atomic_write_profile", corrupt_write)

    payload = _run_rollback(context)

    _assert_no_trading_execution(payload)
    assert payload["rollback_decision"] == "POST_ROLLBACK_VALIDATION_FAILED"
    assert payload["rollback_executed"] is True
    assert "only_allowed_fields_changed" in payload["post_rollback_validation"]["blocking_reasons"]


def test_dashboard_reads_rollback_artifact_without_triggering_rollback_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)
    payload = _run_rollback(context)
    _remove_rollback_inputs(context)
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

    summary = dashboard_payload["shadow_promotion_rollback"]
    assert summary["rollback_decision"] == "ROLLED_BACK"
    assert summary["rollback_executed"] is True
    assert summary["production_effect"] == "profile_rolled_back_only_if_rollback_executed"
    assert summary["changed_weight_keys"] == ["fundamental", "technical"]
    assert summary["current_snapshot_path"] == payload["current_snapshot"]["snapshot_path"]
    assert (
        summary["rollback_snapshot_path"] == payload["input_artifacts"]["rollback_snapshot"]["path"]
    )
    assert summary["post_rollback_validation_status"] == "PASS"
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Shadow Promotion Rollback Result" in html
    assert "shadow_promotion_rollback_result_2026-05-23.md" in html
    assert not context["apply_result_path"].exists()
    assert not context["approval_path"].exists()
    assert not context["rollback_snapshot_path"].exists()


def _run_rollback(
    context: dict[str, Any],
    *,
    danger_flag_provided: bool = True,
) -> dict[str, Any]:
    return write_shadow_promotion_rollback_report(
        as_of=context["as_of"],
        data_root=context["data_root"],
        apply_result_file=context["apply_result_path"],
        rollback_approval_file=context["approval_path"],
        target_profile_path=context["target_path"],
        danger_flag_provided=danger_flag_provided,
        generated_at=_fixed_generated_at(),
    )


def _write_valid_context(
    tmp_path: Path,
    *,
    apply_result_overrides: dict[str, Any] | None = None,
    approval_overrides: dict[str, Any] | None = None,
    approval_apply_overrides: dict[str, Any] | None = None,
    approval_snapshot_overrides: dict[str, Any] | None = None,
    approval_target_overrides: dict[str, Any] | None = None,
    safety_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    as_of = date(2026, 5, 23)
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    target_path = tmp_path / "config" / "weights" / "production_profile.json"
    _write_json(target_path, _production_profile(weights=_shadow_weights()))
    target_sha_after_apply = _sha256(target_path)
    rollback_snapshot_path = _write_rollback_snapshot(data_root, as_of)
    apply_result_path = _write_apply_result(
        data_root,
        as_of,
        target_path=target_path,
        target_sha_after_apply=target_sha_after_apply,
        rollback_snapshot_path=rollback_snapshot_path,
        apply_result_overrides=apply_result_overrides,
    )
    approval_path = _write_rollback_approval(
        data_root,
        as_of,
        apply_result_path=apply_result_path,
        rollback_snapshot_path=rollback_snapshot_path,
        target_path=target_path,
        target_sha_after_apply=target_sha_after_apply,
        approval_overrides=approval_overrides,
        apply_overrides=approval_apply_overrides,
        snapshot_overrides=approval_snapshot_overrides,
        target_overrides=approval_target_overrides,
        safety_overrides=safety_overrides,
    )
    return {
        "as_of": as_of,
        "data_root": data_root,
        "reports_dir": reports_dir,
        "target_path": target_path,
        "target_sha_after_apply": target_sha_after_apply,
        "rollback_snapshot_path": rollback_snapshot_path,
        "rollback_snapshot_sha256": _sha256(rollback_snapshot_path),
        "apply_result_path": apply_result_path,
        "approval_path": approval_path,
    }


def _write_rollback_snapshot(data_root: Path, as_of: date) -> Path:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback"
        / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
    )
    _write_json(path, _production_profile(weights=_production_weights()))
    _write_text(path.with_suffix(".sha256"), _sha256(path) + "\n")
    return path


def _write_apply_result(
    data_root: Path,
    as_of: date,
    *,
    target_path: Path,
    target_sha_after_apply: str,
    rollback_snapshot_path: Path,
    apply_result_overrides: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "apply"
        / f"shadow_promotion_apply_result_{as_of.isoformat()}.json"
    )
    rollback_snapshot_sha = _sha256(rollback_snapshot_path)
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_result",
        "task_id": "TRADING-018E2",
        "date": as_of.isoformat(),
        "generated_at": _fixed_generated_at().isoformat(),
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
        "target_profile_validation": {
            "status": "PASS",
            "path": str(target_path),
            "sha256_after": target_sha_after_apply,
        },
        "rollback": {
            "snapshot_created": True,
            "snapshot_path": str(rollback_snapshot_path),
            "snapshot_sha256": rollback_snapshot_sha,
            "snapshot_file_sha256": rollback_snapshot_sha,
            "snapshot_sha256_path": str(rollback_snapshot_path.with_suffix(".sha256")),
            "rollback_supported": True,
        },
        "post_apply_validation": {"status": "PASS", "blocking_reasons": []},
        "audit": {
            "created_by": "scripts/run_shadow_promotion_apply.py",
            "target_profile_path": str(target_path),
            "target_profile_sha256_after": target_sha_after_apply,
        },
    }
    payload.update(apply_result_overrides or {})
    _write_json(path, payload)
    return path


def _write_rollback_approval(
    data_root: Path,
    as_of: date,
    *,
    apply_result_path: Path,
    rollback_snapshot_path: Path,
    target_path: Path,
    target_sha_after_apply: str,
    approval_overrides: dict[str, Any] | None = None,
    apply_overrides: dict[str, Any] | None = None,
    snapshot_overrides: dict[str, Any] | None = None,
    target_overrides: dict[str, Any] | None = None,
    safety_overrides: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "manual_approvals"
        / f"shadow_promotion_rollback_approval_{as_of.isoformat()}.json"
    )
    apply_result = {
        "apply_date": as_of.isoformat(),
        "apply_result_file": str(apply_result_path),
        "apply_result_sha256": _sha256(apply_result_path),
        "apply_decision": "APPLIED",
        "apply_executed": True,
    }
    apply_result.update(apply_overrides or {})
    rollback_snapshot = {
        "snapshot_file": str(rollback_snapshot_path),
        "snapshot_sha256": _sha256(rollback_snapshot_path),
    }
    rollback_snapshot.update(snapshot_overrides or {})
    target = {
        "target_profile_name": "production",
        "target_profile_path": str(target_path),
        "expected_current_profile_sha256": target_sha_after_apply,
        "expected_rollback_profile_sha256": _sha256(rollback_snapshot_path),
    }
    target.update(target_overrides or {})
    safety = {
        "rollback_authorized": True,
        "production_modification_authorized": True,
        "weights_only_restore": True,
        "current_snapshot_required": True,
        "manual_command_required": True,
        "scheduler_execution_forbidden": True,
        "broker_execution_forbidden": True,
        "replay_execution_forbidden": True,
        "trading_execution_forbidden": True,
    }
    safety.update(safety_overrides or {})
    payload = {
        "schema_version": "1.0",
        "approval_type": "shadow_promotion_rollback",
        "approved": True,
        "approved_by": "manual_user",
        "approved_at": "2026-05-23T00:00:00Z",
        "apply_result": apply_result,
        "rollback_snapshot": rollback_snapshot,
        "target": target,
        "rollback_scope": {
            "allowed_fields": ["weights"],
            "forbidden_fields": [
                "broker",
                "execution",
                "replay",
                "scheduler",
                "risk_limits",
                "api_keys",
                "account",
                "credentials",
            ],
        },
        "approval_statement": (
            "I manually reviewed the apply result and rollback snapshot. "
            "I understand this command will restore the target production profile "
            "to the rollback snapshot."
        ),
        "safety_acknowledgement": safety,
    }
    payload.update(approval_overrides or {})
    _write_json(path, payload)
    return path


def _rewrite_apply_and_approval_for_snapshot(
    context: dict[str, Any],
    *,
    snapshot_weights: dict[str, float],
) -> None:
    _write_json(
        context["rollback_snapshot_path"],
        _production_profile(weights=snapshot_weights),
    )
    _write_text(
        context["rollback_snapshot_path"].with_suffix(".sha256"),
        _sha256(context["rollback_snapshot_path"]) + "\n",
    )
    _write_apply_result(
        context["data_root"],
        context["as_of"],
        target_path=context["target_path"],
        target_sha_after_apply=context["target_sha_after_apply"],
        rollback_snapshot_path=context["rollback_snapshot_path"],
    )
    _write_rollback_approval(
        context["data_root"],
        context["as_of"],
        apply_result_path=context["apply_result_path"],
        rollback_snapshot_path=context["rollback_snapshot_path"],
        target_path=context["target_path"],
        target_sha_after_apply=context["target_sha_after_apply"],
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


def _remove_rollback_inputs(context: dict[str, Any]) -> None:
    for path in (
        context["apply_result_path"],
        context["approval_path"],
        context["rollback_snapshot_path"],
    ):
        if path.exists():
            path.unlink()


def _production_profile(*, weights: dict[str, float] | None = None) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "profile_name": "production",
        "status": "production",
        "environment": "production",
        "weights": weights or _production_weights(),
        "broker": {"enabled": False},
        "risk_limits": {"max_position": 0.20},
        "metadata": {"owner": "test"},
    }


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


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _assert_no_trading_execution(payload: dict[str, Any]) -> None:
    assert payload["manual_review_only"] is True
    assert payload["safe_for_scheduler"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
