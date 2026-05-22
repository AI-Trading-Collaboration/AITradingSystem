from __future__ import annotations

import builtins
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

import ai_trading_system.trading_engine.shadow_promotion_apply as apply_module
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.shadow_promotion_apply import (
    write_shadow_promotion_apply_report,
)


def test_apply_missing_preflight_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["preflight_path"].unlink()

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "INSUFFICIENT_DATA"
    assert payload["apply_executed"] is False
    assert payload["production_effect"] == "none"
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_missing_approval_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["approval_path"].unlink()

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["apply_approval"]["status"] == "MISSING"


def test_apply_missing_target_profile_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    context["target_path"].unlink()

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["target_profile_before"]["status"] == "MISSING"


def test_apply_requires_danger_flag_and_leaves_profile_unchanged(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)

    payload = _run_apply(context, danger_flag_provided=False)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "DANGER_FLAG_MISSING"
    assert payload["apply_executed"] is False
    assert payload["promotion_executed"] is False
    assert payload["production_effect"] == "none"
    assert context["target_sha_before"] == _sha256(context["target_path"])
    assert Path(payload["outputs"]["json"]).exists()


@pytest.mark.parametrize(
    ("approval_overrides", "safety_overrides", "target_overrides", "expected_reason"),
    [
        ({"approved": False}, None, None, "approved"),
        ({"approval_type": "shadow_promotion_apply_preflight"}, None, None, "approval_type"),
        (None, {"apply_authorized": False}, None, "apply_authorized"),
        (
            None,
            {"production_modification_authorized": False},
            None,
            "production_modification_authorized",
        ),
        (None, {"scheduler_execution_forbidden": False}, None, "scheduler_execution_forbidden"),
        (None, {"broker_execution_forbidden": False}, None, "broker_execution_forbidden"),
        (None, {"replay_execution_forbidden": False}, None, "replay_execution_forbidden"),
        (None, {"trading_execution_forbidden": False}, None, "trading_execution_forbidden"),
        (None, None, {"expected_target_profile_sha256": "bad"}, "expected_target_profile_sha256"),
    ],
)
def test_apply_rejects_invalid_approval(
    tmp_path: Path,
    approval_overrides: dict[str, Any] | None,
    safety_overrides: dict[str, Any] | None,
    target_overrides: dict[str, Any] | None,
    expected_reason: str,
) -> None:
    context = _write_valid_context(
        tmp_path,
        approval_overrides=approval_overrides,
        safety_overrides=safety_overrides,
        target_overrides=target_overrides,
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "APPROVAL_INVALID"
    assert expected_reason in payload["approval_validation"]["blocking_reasons"]
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_rejects_approval_preflight_hash_mismatch(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path, approval_preflight_overrides={"preflight_sha256": "bad"}
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "APPROVAL_INVALID"
    assert "preflight_hash_match" in payload["approval_validation"]["blocking_reasons"]


@pytest.mark.parametrize(
    ("preflight_overrides", "diff_overrides", "expected_reason"),
    [
        ({"preflight_decision": "WARNING"}, None, "preflight_decision"),
        ({"apply_executed": True}, None, "apply_executed"),
        ({"production_effect": "profile_updated"}, None, "production_effect"),
        (None, {}, "diff_preview"),
        (None, {"production_weights_after_preview": None}, "production_weights_after_preview"),
    ],
)
def test_apply_rejects_invalid_preflight(
    tmp_path: Path,
    preflight_overrides: dict[str, Any] | None,
    diff_overrides: dict[str, Any] | None,
    expected_reason: str,
) -> None:
    context = _write_valid_context(
        tmp_path,
        preflight_overrides=preflight_overrides,
        diff_overrides=diff_overrides,
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "PREFLIGHT_INVALID"
    assert expected_reason in payload["preflight_validation"]["blocking_reasons"]
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_blocks_when_target_profile_changed_after_preflight(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    _write_json(
        context["target_path"],
        _production_profile(weights={**_production_weights(), "technical": 0.24}),
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "TARGET_PROFILE_CHANGED"
    assert payload["apply_executed"] is False


def test_apply_rejects_target_profile_path_mismatch(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path,
        target_overrides={
            "target_profile_path": str(tmp_path / "config" / "weights" / "other.json"),
        },
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "TARGET_PROFILE_MISMATCH"
    assert "target_profile_path" in payload["target_profile_validation"]["blocking_reasons"]


def test_apply_rejects_weight_key_mismatch_before_write(tmp_path: Path) -> None:
    context = _write_valid_context(
        tmp_path,
        expected_weights={"technical": 0.50, "fundamental": 0.50},
    )

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "TARGET_PROFILE_MISMATCH"
    assert payload["apply_executed"] is False
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_creates_rollback_before_successful_profile_write(tmp_path: Path) -> None:
    context = _write_valid_context(tmp_path)
    before_profile = json.loads(context["target_path"].read_text(encoding="utf-8"))

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "APPLIED"
    assert payload["apply_executed"] is True
    assert payload["promotion_executed"] is True
    assert payload["production_effect"] == "profile_updated_only_if_apply_executed"
    assert payload["post_apply_validation"]["status"] == "PASS"
    profile_after = json.loads(context["target_path"].read_text(encoding="utf-8"))
    assert profile_after["weights"] == _shadow_weights()
    assert profile_after["broker"] == before_profile["broker"]
    assert profile_after["risk_limits"] == before_profile["risk_limits"]
    rollback_path = Path(payload["rollback"]["snapshot_path"])
    rollback_sha_path = Path(payload["rollback"]["snapshot_sha256_path"])
    assert rollback_path.exists()
    assert rollback_sha_path.exists()
    assert json.loads(rollback_path.read_text(encoding="utf-8")) == before_profile
    assert rollback_sha_path.read_text(encoding="utf-8").strip() == context["target_sha_before"]
    assert payload["rollback"]["snapshot_sha256"] == context["target_sha_before"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()


def test_apply_blocks_when_rollback_snapshot_creation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def fail_snapshot(**_: Any) -> dict[str, Any]:
        raise OSError("snapshot denied")

    monkeypatch.setattr(apply_module, "_create_rollback_snapshot", fail_snapshot)

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "ROLLBACK_SNAPSHOT_FAILED"
    assert payload["apply_executed"] is False
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_reports_write_failed_when_atomic_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def fail_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        raise OSError(f"write denied: {path}:{payload}:{write_mode}")

    monkeypatch.setattr(apply_module, "_atomic_write_profile", fail_write)

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "WRITE_FAILED"
    assert payload["apply_executed"] is False
    assert context["target_sha_before"] == _sha256(context["target_path"])


def test_apply_reports_post_apply_validation_failed_for_weight_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def corrupt_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        broken = dict(payload)
        broken["weights"] = {**_shadow_weights(), "technical": 0.25, "fundamental": 0.25}
        _write_json(path, broken)

    monkeypatch.setattr(apply_module, "_atomic_write_profile", corrupt_write)

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "POST_APPLY_VALIDATION_FAILED"
    assert payload["apply_executed"] is True
    assert "weights_match_expected" in payload["post_apply_validation"]["blocking_reasons"]


def test_apply_reports_post_apply_validation_failed_for_non_allowed_field_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)

    def corrupt_write(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
        broken = dict(payload)
        broken["broker"] = {"enabled": True}
        _write_json(path, broken)

    monkeypatch.setattr(apply_module, "_atomic_write_profile", corrupt_write)

    payload = _run_apply(context)

    _assert_no_trading_execution(payload)
    assert payload["apply_decision"] == "POST_APPLY_VALIDATION_FAILED"
    assert payload["apply_executed"] is True
    assert "only_allowed_fields_changed" in payload["post_apply_validation"]["blocking_reasons"]


def test_dashboard_reads_apply_artifact_without_triggering_apply_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _write_valid_context(tmp_path)
    payload = _run_apply(context)
    _remove_apply_inputs(context)
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

    summary = dashboard_payload["shadow_promotion_apply"]
    assert summary["apply_decision"] == "APPLIED"
    assert summary["apply_executed"] is True
    assert summary["promotion_executed"] is True
    assert summary["production_effect"] == "profile_updated_only_if_apply_executed"
    assert summary["changed_weight_keys"] == ["fundamental", "technical"]
    assert summary["rollback_snapshot_path"] == payload["rollback"]["snapshot_path"]
    assert summary["post_apply_validation_status"] == "PASS"
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Shadow Promotion Apply Result" in html
    assert "shadow_promotion_apply_result_2026-05-20.md" in html
    assert not context["preflight_path"].exists()
    assert not context["approval_path"].exists()
    assert not context["proposal_path"].exists()


def _run_apply(
    context: dict[str, Any],
    *,
    danger_flag_provided: bool = True,
) -> dict[str, Any]:
    return write_shadow_promotion_apply_report(
        as_of=context["as_of"],
        data_root=context["data_root"],
        preflight_file=context["preflight_path"],
        apply_approval_file=context["approval_path"],
        target_profile_path=context["target_path"],
        proposal_file=context["proposal_path"],
        danger_flag_provided=danger_flag_provided,
        generated_at=_fixed_generated_at(),
    )


def _write_valid_context(
    tmp_path: Path,
    *,
    expected_weights: dict[str, float] | None = None,
    preflight_overrides: dict[str, Any] | None = None,
    diff_overrides: dict[str, Any] | None = None,
    approval_overrides: dict[str, Any] | None = None,
    approval_preflight_overrides: dict[str, Any] | None = None,
    safety_overrides: dict[str, Any] | None = None,
    target_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    as_of = date(2026, 5, 20)
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    target_path = tmp_path / "config" / "weights" / "production_profile.json"
    _write_json(target_path, _production_profile())
    target_sha = _sha256(target_path)
    proposal_path = _write_proposal(data_root, as_of, weights=expected_weights or _shadow_weights())
    preflight_path = _write_preflight(
        data_root,
        as_of,
        target_path=target_path,
        target_sha256=target_sha,
        proposal_path=proposal_path,
        expected_weights=expected_weights or _shadow_weights(),
        preflight_overrides=preflight_overrides,
        diff_overrides=diff_overrides,
    )
    approval_path = _write_apply_approval(
        data_root,
        as_of,
        preflight_path=preflight_path,
        proposal_path=proposal_path,
        target_path=target_path,
        target_sha256=target_sha,
        approval_overrides=approval_overrides,
        preflight_overrides=approval_preflight_overrides,
        safety_overrides=safety_overrides,
        target_overrides=target_overrides,
    )
    return {
        "as_of": as_of,
        "data_root": data_root,
        "reports_dir": reports_dir,
        "target_path": target_path,
        "target_sha_before": target_sha,
        "proposal_path": proposal_path,
        "preflight_path": preflight_path,
        "approval_path": approval_path,
    }


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


def _write_proposal(data_root: Path, as_of: date, *, weights: dict[str, float]) -> Path:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )
    _write_json(
        path,
        {
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
            "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
            "proposed_production_weights": weights,
        },
    )
    return path


def _write_preflight(
    data_root: Path,
    as_of: date,
    *,
    target_path: Path,
    target_sha256: str,
    proposal_path: Path,
    expected_weights: dict[str, float],
    preflight_overrides: dict[str, Any] | None = None,
    diff_overrides: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "preflight"
        / f"shadow_promotion_apply_preflight_{as_of.isoformat()}.json"
    )
    delta = {
        key: round(expected_weights.get(key, 0.0) - _production_weights().get(key, 0.0), 10)
        for key in sorted(set(expected_weights) | set(_production_weights()))
    }
    diff_preview: dict[str, Any] = {
        "target_profile_path": str(target_path),
        "target_profile_sha256_before": target_sha256,
        "changed_weight_keys": [key for key, value in delta.items() if abs(value) > 0.000001],
        "production_weights_before": _production_weights(),
        "production_weights_after_preview": expected_weights,
        "delta": delta,
    }
    if diff_overrides is not None:
        if diff_overrides:
            diff_preview.update(
                {key: value for key, value in diff_overrides.items() if value is not None}
            )
            for key, value in diff_overrides.items():
                if value is None and key in diff_preview:
                    del diff_preview[key]
        else:
            diff_preview = {}
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_apply_preflight",
        "task_id": "TRADING-018E1",
        "date": as_of.isoformat(),
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "approved_apply_preflight_only",
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_executed": False,
        "apply_executed": False,
        "preflight_only": True,
        "safe_for_production": False,
        "preflight_decision": "PASS",
        "input_artifacts": {
            "promotion_proposal": {
                "status": "FOUND",
                "path": str(proposal_path),
                "sha256": _sha256(proposal_path),
            },
            "production_profile": {
                "status": "FOUND",
                "path": str(target_path),
                "sha256": target_sha256,
            },
        },
        "diff_preview": diff_preview,
        "rollback_plan": {
            "required": True,
            "target_profile_path": str(target_path),
            "target_profile_sha256_before": target_sha256,
            "rollback_snapshot_path_preview": str(
                data_root
                / "derived"
                / "weight_iterations"
                / "promotion"
                / "rollback"
                / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
            ),
        },
        "pipeline_contract": _safe_preflight_contract(),
    }
    payload.update(preflight_overrides or {})
    _write_json(path, payload)
    return path


def _write_apply_approval(
    data_root: Path,
    as_of: date,
    *,
    preflight_path: Path,
    proposal_path: Path,
    target_path: Path,
    target_sha256: str,
    approval_overrides: dict[str, Any] | None = None,
    preflight_overrides: dict[str, Any] | None = None,
    safety_overrides: dict[str, Any] | None = None,
    target_overrides: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root / "manual_approvals" / f"shadow_promotion_apply_approval_{as_of.isoformat()}.json"
    )
    preflight = {
        "preflight_date": as_of.isoformat(),
        "preflight_file": str(preflight_path),
        "preflight_sha256": _sha256(preflight_path),
        "preflight_decision": "PASS",
    }
    preflight.update(preflight_overrides or {})
    target = {
        "target_profile_name": "production",
        "target_profile_path": str(target_path),
        "expected_target_profile_sha256": target_sha256,
    }
    target.update(target_overrides or {})
    safety = {
        "apply_authorized": True,
        "production_modification_authorized": True,
        "weights_only_update": True,
        "rollback_required": True,
        "manual_command_required": True,
        "scheduler_execution_forbidden": True,
        "broker_execution_forbidden": True,
        "replay_execution_forbidden": True,
        "trading_execution_forbidden": True,
    }
    safety.update(safety_overrides or {})
    payload = {
        "schema_version": "1.0",
        "approval_type": "shadow_promotion_apply",
        "approved": True,
        "approved_by": "manual_user",
        "approved_at": "2026-05-20T00:00:00Z",
        "preflight": preflight,
        "proposal": {
            "proposal_date": as_of.isoformat(),
            "proposal_file": str(proposal_path),
            "proposal_sha256": _sha256(proposal_path),
            "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
            "promotion_proposed": True,
        },
        "target": target,
        "apply_scope": {
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
            "I manually reviewed the promotion proposal and preflight report. "
            "I understand this command will modify the target production profile weights only."
        ),
        "safety_acknowledgement": safety,
    }
    payload.update(approval_overrides or {})
    _write_json(path, payload)
    return path


def _safe_preflight_contract() -> dict[str, Any]:
    return {
        "runs_shadow_iteration_pipeline": False,
        "runs_comparison_pipeline": False,
        "runs_multi_day_review_pipeline": False,
        "runs_promotion_proposal_pipeline": False,
        "runs_promotion_apply": False,
        "runs_scoring_pipeline": False,
        "runs_broker_runner": False,
        "runs_paper_runner": False,
        "runs_replay_runner": False,
        "writes_production_profile": False,
        "writes_production_weights": False,
        "writes_approved_profile": False,
        "promotes_shadow_to_production": False,
        "triggers_trade": False,
    }


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
            "visibility_cutoff": "2026-05-20T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return path


def _remove_apply_inputs(context: dict[str, Any]) -> None:
    for path in (
        context["preflight_path"],
        context["approval_path"],
        context["proposal_path"],
    ):
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


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 20, tzinfo=UTC)


def _assert_no_trading_execution(payload: dict[str, Any]) -> None:
    assert payload["manual_review_only"] is True
    assert payload["safe_for_scheduler"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
