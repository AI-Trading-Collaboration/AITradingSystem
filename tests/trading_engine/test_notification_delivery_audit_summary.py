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
from ai_trading_system.trading_engine.notification_delivery_audit_summary import (
    _compute_draft_dispatch_hash,
    write_notification_delivery_audit_summary,
)


def test_valid_030_031_034_generates_pass(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "PASS"
    assert payload["notification_lifecycle_status"] == "DRAFT_READY"
    assert payload["artifact_chain"]["status"] == "PASS"
    assert payload["artifact_chain"]["draft_hash_match"] is True
    assert payload["artifact_chain"]["dispatch_latest_match"] is True
    assert payload["external_side_effect_audit"]["status"] == "PASS"


def test_missing_030_is_incomplete(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_preflight(data_root, as_of)
    _write_dispatch(data_root, as_of)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "INCOMPLETE"
    assert payload["notification_lifecycle_status"] == "INCOMPLETE"


def test_missing_031_is_incomplete_and_draft_only(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_dispatch(data_root, as_of)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "INCOMPLETE"
    assert payload["notification_lifecycle_status"] == "DRAFT_ONLY"


def test_missing_034_without_allow_missing_dispatch_is_incomplete(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "INCOMPLETE"
    assert payload["notification_lifecycle_status"] == "PREFLIGHT_READY"


def test_missing_034_with_allow_missing_dispatch_is_pass_with_warnings(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = _run_audit(data_root, as_of, allow_missing_dispatch=True)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "PASS_WITH_WARNINGS"
    assert payload["notification_lifecycle_status"] == "PREFLIGHT_READY"
    assert payload["alerts"]["warnings"]


def test_invalid_json_is_error(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    path = _draft_path(data_root, as_of)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    payload = _run_audit(data_root, as_of, allow_missing_dispatch=True)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "ERROR"
    assert payload["notification_lifecycle_status"] == "UNKNOWN"
    assert payload["alerts"]["critical"]


def test_030_plus_031_pass_maps_to_preflight_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = _run_audit(data_root, as_of, allow_missing_dispatch=True)

    _assert_audit_invariants(payload)
    assert payload["notification_lifecycle_status"] == "PREFLIGHT_READY"


@pytest.mark.parametrize(
    ("dispatch_status", "expected_lifecycle"),
    [
        ("DRAFT_READY", "DRAFT_READY"),
        ("BLOCKED", "BLOCKED"),
        ("APPROVAL_MISMATCH", "APPROVAL_MISMATCH"),
    ],
)
def test_dispatch_status_maps_to_lifecycle(
    tmp_path: Path,
    dispatch_status: str,
    expected_lifecycle: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of, dispatch_status=dispatch_status)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["notification_lifecycle_status"] == expected_lifecycle


def test_safety_blocked_dispatch_maps_to_safety_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of, dispatch_status="SAFETY_BLOCKED")

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "SAFETY_BLOCKED"
    assert payload["notification_lifecycle_status"] == "SAFETY_BLOCKED"


def test_preflight_references_wrong_draft_hash_is_mismatch(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of, draft_sha256="bad")
    _write_dispatch(data_root, as_of)

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "MISMATCH"
    assert payload["artifact_chain"]["draft_to_preflight_match"] is False


def test_dispatch_references_wrong_preflight_hash_is_mismatch(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)
    _write_dispatch(data_root, as_of, preflight_sha256="bad")

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "MISMATCH"
    assert payload["artifact_chain"]["preflight_to_dispatch_match"] is False


def test_dispatch_draft_hash_mismatch_is_mismatch_and_approval_mismatch(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of, dispatch_hash="sha256:bad")

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "MISMATCH"
    assert payload["notification_lifecycle_status"] == "APPROVAL_MISMATCH"
    assert payload["artifact_chain"]["dispatch_hash_stable"] is False


def test_latest_json_points_to_different_dispatch_is_mismatch(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)
    dispatch = _write_dispatch(data_root, as_of, latest_dispatch_path="data/other.json")

    payload = write_notification_delivery_audit_summary(
        as_of=as_of,
        data_root=data_root,
        dispatch_file=Path(dispatch["output_artifacts"]["draft_dispatch_json"]["path"]),
        generated_at=_fixed_generated_at(),
    )

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "MISMATCH"
    assert payload["artifact_chain"]["dispatch_latest_match"] is False


def test_latest_missing_with_explicit_dispatch_is_pass_with_warnings(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)
    dispatch = _write_dispatch(data_root, as_of, write_latest=False)

    payload = write_notification_delivery_audit_summary(
        as_of=as_of,
        data_root=data_root,
        dispatch_file=Path(dispatch["output_artifacts"]["draft_dispatch_json"]["path"]),
        generated_at=_fixed_generated_at(),
    )

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "PASS_WITH_WARNINGS"
    assert payload["artifact_chain"]["dispatch_latest_match"] is False


@pytest.mark.parametrize(
    "field",
    [
        "email_sent",
        "gmail_draft_created",
        "gmail_draft_modified",
        "slack_sent",
        "discord_sent",
        "webhook_called",
        "mobile_push_sent",
        "broker_execution",
        "replay_execution",
        "trading_execution",
    ],
)
def test_side_effect_or_execution_true_in_any_artifact_safety_blocks(
    tmp_path: Path,
    field: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of, dispatch_overrides={field: True})

    payload = _run_audit(data_root, as_of)

    _assert_audit_invariants(payload)
    assert payload["audit_status"] == "SAFETY_BLOCKED"
    assert payload["external_side_effect_audit"]["status"] == "FAIL"
    assert payload["safety_validation"]["status"] == "FAIL"


def test_pass_markdown_contains_required_sections(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_valid_chain(data_root, as_of)

    payload = _run_audit(data_root, as_of)
    markdown = Path(payload["output_artifacts"]["audit_markdown"]["path"]).read_text(
        encoding="utf-8"
    )

    _assert_audit_invariants(payload)
    assert "## 1. Audit Summary" in markdown
    assert "## 2. Artifact Chain" in markdown
    assert "## 6. External Side Effect Audit" in markdown


@pytest.mark.parametrize(
    ("status_builder", "expected_banner"),
    [
        ("safety", "## Notification Delivery Audit Safety Blocked"),
        ("mismatch", "## Notification Delivery Audit Mismatch"),
        ("incomplete", "## Notification Delivery Audit Incomplete"),
    ],
)
def test_markdown_status_banners(
    tmp_path: Path,
    status_builder: str,
    expected_banner: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    if status_builder == "safety":
        _write_valid_chain(data_root, as_of, dispatch_overrides={"webhook_called": True})
    elif status_builder == "mismatch":
        _write_valid_chain(data_root, as_of, dispatch_hash="sha256:bad")
    else:
        _write_notification_draft(data_root, as_of)

    payload = _run_audit(data_root, as_of, allow_missing_dispatch=status_builder == "incomplete")
    markdown = Path(payload["output_artifacts"]["audit_markdown"]["path"]).read_text(
        encoding="utf-8"
    )

    _assert_audit_invariants(payload)
    assert expected_banner in markdown


def test_dashboard_reads_trading_035_artifact_without_triggering_upstream(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    data_root = tmp_path / "data"
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)
    _write_valid_chain(data_root, as_of)
    audit = _run_audit(data_root, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_notification_delivery_audit_summary",
            "generate_operator_brief_notification_draft",
            "run_operator_brief_notification_delivery_preflight",
            "run_operator_brief_notification_draft_dispatch",
            "ai_trading_system.trading_engine.notification_delivery_audit_summary",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight",
            "ai_trading_system.trading_engine.operator_brief_notification_draft_dispatch",
            "smtplib",
            "slack_sdk",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import TRADING-035 execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    summary = payload["notification_delivery_audit_summary"]
    assert summary["audit_status"] == "PASS"
    assert summary["notification_lifecycle_status"] == "DRAFT_READY"
    assert summary["summary_level"] == "NORMAL"
    assert summary["draft_status"] == "GENERATED"
    assert summary["preflight_status"] == "PASS"
    assert summary["dispatch_status"] == "DRAFT_READY"
    assert summary["draft_hash_match"] is True
    assert summary["latest_json_match"] is True
    assert summary["external_side_effect_audit_status"] == "PASS"
    assert summary["critical_alert_count"] == 0
    assert summary["warning_count"] == 0
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["notification_delivery_audit_only"] is True
    assert summary["read_only"] is True
    assert summary["notification_draft_executed_by_audit"] is False
    assert summary["delivery_preflight_executed_by_audit"] is False
    assert summary["draft_dispatch_executed_by_audit"] is False
    assert summary["operator_brief_executed_by_audit"] is False
    assert summary["pipelines_executed_by_audit"] is False
    assert summary["data_downloaded_by_audit"] is False
    assert summary["apply_executed_by_audit"] is False
    assert summary["rollback_executed_by_audit"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Notification Delivery Audit Summary" in html
    assert "latest_json_match" in html
    assert audit["output_artifacts"]["audit_markdown"]["path"] in html


def _run_audit(
    data_root: Path,
    as_of: date,
    *,
    allow_missing_dispatch: bool = False,
) -> dict[str, Any]:
    return write_notification_delivery_audit_summary(
        as_of=as_of,
        data_root=data_root,
        allow_missing_dispatch=allow_missing_dispatch,
        generated_at=_fixed_generated_at(),
    )


def _write_valid_chain(
    data_root: Path,
    as_of: date,
    *,
    dispatch_status: str = "DRAFT_READY",
    dispatch_hash: str | None = None,
    dispatch_overrides: dict[str, Any] | None = None,
) -> None:
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)
    _write_dispatch(
        data_root,
        as_of,
        dispatch_status=dispatch_status,
        dispatch_hash=dispatch_hash,
        dispatch_overrides=dispatch_overrides,
    )


def _write_notification_draft(
    data_root: Path,
    as_of: date,
    *,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    root = data_root / "derived" / "operator_briefs" / "notifications"
    json_path = _draft_path(data_root, as_of)
    markdown_path = json_path.with_suffix(".md")
    email_path = root / "email" / f"operator_brief_email_draft_{suffix}.md"
    chat_path = root / "chat" / f"operator_brief_chat_draft_{suffix}.md"
    mobile_path = root / "mobile" / f"operator_brief_mobile_summary_{suffix}.md"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_draft",
        "task_id": "TRADING-030",
        "date": suffix,
        "mode": "operator_brief_notification_draft_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "slack_sent": False,
        "discord_sent": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_notification_draft": False,
        "pipelines_executed_by_notification_draft": False,
        "data_downloaded_by_notification_draft": False,
        "apply_executed_by_notification_draft": False,
        "rollback_executed_by_notification_draft": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "draft_status": "GENERATED",
        "notification_severity": "NORMAL",
        "headline": "Notification drafts are ready for review.",
        "draft_outputs": {
            "email_draft": {"path": str(email_path)},
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "summary_markdown": {"path": str(markdown_path)},
        },
        "safety_validation": {"status": "PASS", "blocking_reasons": []},
        "alerts": {"critical": [], "warnings": [], "notes": []},
    }
    if overrides:
        payload.update(overrides)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    email_path.parent.mkdir(parents=True, exist_ok=True)
    chat_path.parent.mkdir(parents=True, exist_ok=True)
    mobile_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft\n", encoding="utf-8")
    email_path.write_text("# Email Draft\nReady.\n", encoding="utf-8")
    chat_path.write_text("# Chat Draft\nReady.\n", encoding="utf-8")
    mobile_path.write_text("Ready.\n", encoding="utf-8")
    return payload


def _write_preflight(
    data_root: Path,
    as_of: date,
    *,
    draft_sha256: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    root = data_root / "derived" / "operator_briefs" / "notifications" / "delivery_preflight"
    json_path = root / f"operator_brief_notification_delivery_preflight_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    run_log_json = (
        root / "logs" / f"operator_brief_notification_delivery_preflight_run_{suffix}.json"
    )
    draft = _draft_path(data_root, as_of)
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_delivery_preflight",
        "task_id": "TRADING-031",
        "date": suffix,
        "mode": "operator_brief_notification_delivery_preflight_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_delivery_preflight_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_delivery_preflight": False,
        "notification_draft_executed_by_delivery_preflight": False,
        "pipelines_executed_by_delivery_preflight": False,
        "data_downloaded_by_delivery_preflight": False,
        "apply_executed_by_delivery_preflight": False,
        "rollback_executed_by_delivery_preflight": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "preflight_status": "PASS",
        "delivery_readiness": "READY_FOR_MANUAL_REVIEW",
        "notification_severity": "NORMAL",
        "input_artifacts": {
            "notification_draft_metadata": {
                "status": "FOUND",
                "path": str(draft),
                "sha256": draft_sha256 or (_sha256(draft) if draft.exists() else ""),
            }
        },
        "approval_validation": {"approval_required": False},
        "channel_readiness": {
            "email": {"status": "READY_FOR_MANUAL_REVIEW"},
            "chat": {"status": "READY_FOR_MANUAL_REVIEW"},
            "mobile": {"status": "READY_FOR_MANUAL_REVIEW"},
        },
        "safety_validation": {"status": "PASS", "blocking_reasons": []},
        "alerts": {
            "critical": [],
            "warnings": [],
            "notes": ["Delivery preflight is read-only and did not send any notification."],
        },
        "output_artifacts": {
            "preflight_json": {"path": str(json_path)},
            "preflight_markdown": {"path": str(markdown_path)},
            "run_log_json": {"path": str(run_log_json)},
            "run_log_markdown": {"path": str(run_log_json.with_suffix(".md"))},
        },
    }
    if overrides:
        payload.update(overrides)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    run_log_json.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Delivery Preflight\n", encoding="utf-8")
    run_log_json.write_text(json.dumps({"run_status": "COMPLETED"}), encoding="utf-8")
    return payload


def _write_dispatch(
    data_root: Path,
    as_of: date,
    *,
    dispatch_status: str = "DRAFT_READY",
    preflight_sha256: str | None = None,
    dispatch_hash: str | None = None,
    dispatch_overrides: dict[str, Any] | None = None,
    latest_dispatch_path: str | None = None,
    write_latest: bool = True,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    root = data_root / "derived" / "operator_briefs" / "notifications" / "draft_dispatch"
    json_path = root / f"operator_brief_notification_draft_dispatch_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_path = root / "latest.json"
    latest_md = root / "latest.md"
    run_log = root / "run.log"
    preflight = (
        data_root
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_preflight"
        / f"operator_brief_notification_delivery_preflight_{suffix}.json"
    )
    draft = _draft_path(data_root, as_of)
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_draft_dispatch",
        "task_id": "TRADING-034",
        "date": suffix,
        "mode": "draft_dispatch",
        "production_effect": "none",
        "manual_review_only": True,
        "draft_dispatch_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "smtp_called": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_draft_dispatch": False,
        "notification_draft_executed_by_draft_dispatch": False,
        "delivery_preflight_executed_by_draft_dispatch": False,
        "dispatch_preview_executed_by_draft_dispatch": False,
        "approval_gate_executed_by_draft_dispatch": False,
        "pipelines_executed_by_draft_dispatch": False,
        "data_downloaded_by_draft_dispatch": False,
        "apply_executed_by_draft_dispatch": False,
        "rollback_executed_by_draft_dispatch": False,
        "operator_brief_executed_by_dispatch": False,
        "pipelines_executed_by_dispatch": False,
        "data_downloaded_by_dispatch": False,
        "apply_executed_by_dispatch": False,
        "rollback_executed_by_dispatch": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-034",
            "task_name": "Operator Brief Notification Draft Dispatch",
            "run_date": suffix,
            "generated_at": "2026-05-24T00:00:00Z",
            "mode": "draft_dispatch",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "input_refs": {
            "delivery_preflight_artifact": {
                "path": str(preflight),
                "status": "FOUND",
                "sha256": preflight_sha256 or (_sha256(preflight) if preflight.exists() else ""),
            },
            "notification_draft_metadata": {
                "path": str(draft),
                "status": "FOUND",
                "sha256": _sha256(draft) if draft.exists() else "",
            },
        },
        "approval_gate_summary": {
            "approval_gate_status": "APPROVED",
            "allowed_to_enter_dispatch": True,
            "dispatch_preview_hash": "sha256:preview",
            "current_dispatch_preview_hash": "sha256:preview",
        },
        "dispatch_preview_summary": {
            "final_status": "WOULD_SEND",
            "dispatch_status": "WOULD_SEND",
            "human_action_required": True,
            "channel_count": 2,
            "would_send_channel_count": 1,
        },
        "draft": {
            "draft_status": dispatch_status,
            "draft_id": "",
            "channel_count": 2,
            "draft_ready_channel_count": 1 if dispatch_status == "DRAFT_READY" else 0,
            "channels": [
                {
                    "channel_type": "email",
                    "target_ref": "o***@example.com",
                    "enabled": True,
                    "draft_ready": dispatch_status == "DRAFT_READY",
                    "reason": "email channel ready.",
                }
            ],
            "message": {
                "subject": "[Trading System] Daily Operator Brief - OK - 2026-05-24",
                "body_markdown": "# Operator Brief\nReady.\n",
                "body_length": 24,
                "contains_markdown": True,
            },
        },
        "hashes": {
            "dispatch_preview_hash": "sha256:preview",
            "approval_gate_dispatch_preview_hash": "sha256:preview",
            "draft_hash": "",
            "hash_algorithm": "sha256",
            "hash_scope": "canonical_draft_dispatch_json",
        },
        "decision": {
            "final_status": dispatch_status,
            "ready_for_actual_dispatch": dispatch_status == "DRAFT_READY",
            "human_action_required": True,
            "next_recommended_action": "Review this local draft dispatch artifact.",
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "approval_gate_required": True,
            "approval_gate_passed": dispatch_status == "DRAFT_READY",
            "sensitive_content_flags": [],
        },
        "reasons": [],
        "warnings": [],
        "output_artifacts": {
            "draft_dispatch_json": {
                "path": latest_dispatch_path
                or f"data/derived/operator_briefs/notifications/draft_dispatch/{json_path.name}"
            },
            "draft_dispatch_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/draft_dispatch/"
                    f"{markdown_path.name}"
                )
            },
            "latest_json": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/latest.json"
            },
            "latest_markdown": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/latest.md"
            },
            "run_log": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/run.log"
            },
        },
    }
    if dispatch_overrides:
        payload.update(dispatch_overrides)
    payload["hashes"]["draft_hash"] = dispatch_hash or _compute_draft_dispatch_hash(payload)
    payload["draft"]["draft_id"] = f"local-draft-{payload['hashes']['draft_hash'][-16:]}"
    root.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    json_path.write_text(text, encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft Dispatch\n", encoding="utf-8")
    if write_latest:
        latest_path.write_text(text, encoding="utf-8")
        latest_md.write_text("# Operator Brief Notification Draft Dispatch\n", encoding="utf-8")
    run_log.write_text("final_status=DRAFT_READY\n", encoding="utf-8")
    return payload


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_run_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "run_id": f"daily_ops_run:{as_of.isoformat()}",
                "status": "PASS",
                "project_root": str(tmp_path),
                "commands": [],
                "step_results": [],
                "git": {"dirty": False, "commit": "abc"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _draft_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "operator_briefs"
        / "notifications"
        / f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )


def _sha256(path: Path) -> str:
    digest = __import__("hashlib").sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_audit_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["notification_delivery_audit_only"] is True
    assert payload["read_only"] is True
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["notification_draft_executed_by_audit"] is False
    assert payload["delivery_preflight_executed_by_audit"] is False
    assert payload["draft_dispatch_executed_by_audit"] is False
    assert payload["operator_brief_executed_by_audit"] is False
    assert payload["pipelines_executed_by_audit"] is False
    assert payload["data_downloaded_by_audit"] is False
    assert payload["apply_executed_by_audit"] is False
    assert payload["rollback_executed_by_audit"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, tzinfo=UTC)
