from __future__ import annotations

import builtins
import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.operator_brief_notification_draft_dispatch import (
    compute_dispatch_preview_hash,
    compute_draft_dispatch_hash,
    write_operator_brief_notification_draft_dispatch,
)


def test_draft_dispatch_approved_would_send_generates_draft_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash=compute_dispatch_preview_hash(preview),
    )

    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "DRAFT_READY"
    assert payload["decision"]["ready_for_actual_dispatch"] is True
    assert payload["approval_gate_summary"]["approval_gate_status"] == "APPROVED"
    assert payload["draft"]["channel_count"] == 2
    assert payload["draft"]["draft_ready_channel_count"] == 1
    assert payload["draft"]["message"]["subject"] == (
        "[Trading System] Daily Operator Brief - OK - 2026-05-24"
    )
    assert payload["draft"]["message"]["body_markdown"] == "# Operator Brief\nReady.\n"
    assert payload["hashes"]["draft_hash"].startswith("sha256:")
    assert Path(payload["output_artifacts"]["draft_dispatch_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["draft_dispatch_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log"]["path"]).exists()


def test_draft_dispatch_approval_required_status(tmp_path: Path) -> None:
    payload = _build_with_gate_status(tmp_path, "APPROVAL_REQUIRED")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "APPROVAL_REQUIRED"
    assert payload["decision"]["ready_for_actual_dispatch"] is False


def test_draft_dispatch_approval_expired_status(tmp_path: Path) -> None:
    payload = _build_with_gate_status(tmp_path, "APPROVAL_EXPIRED")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "APPROVAL_EXPIRED"


def test_draft_dispatch_approval_mismatch_status(tmp_path: Path) -> None:
    payload = _build_with_gate_status(tmp_path, "APPROVAL_MISMATCH")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "APPROVAL_MISMATCH"


def test_draft_dispatch_safety_blocked_status(tmp_path: Path) -> None:
    payload = _build_with_gate_status(tmp_path, "SAFETY_BLOCKED")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "SAFETY_BLOCKED"


def test_draft_dispatch_blocked_status(tmp_path: Path) -> None:
    payload = _build_with_gate_status(tmp_path, "BLOCKED")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"


def test_draft_dispatch_noop_status(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of, final_status="NOOP")
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="NOOP",
        preview_hash=compute_dispatch_preview_hash(preview),
    )

    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "NOOP"
    assert payload["decision"]["human_action_required"] is False


def test_draft_dispatch_missing_approval_gate_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of)

    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert payload["input_refs"]["approval_gate_artifact"]["status"] == "MISSING"


def test_draft_dispatch_missing_preview_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash="sha256:" + ("1" * 64),
    )

    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert payload["input_refs"]["dispatch_preview_artifact"]["status"] == "MISSING"


def test_draft_dispatch_hash_mismatch_blocks_ready_draft(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash="sha256:" + ("0" * 64),
    )

    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "APPROVAL_MISMATCH"
    assert payload["decision"]["ready_for_actual_dispatch"] is False


def test_draft_dispatch_missing_subject_is_blocked(tmp_path: Path) -> None:
    payload = _build_approved_with_preview(tmp_path, subject="")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert any("subject" in reason.lower() for reason in payload["reasons"])


def test_draft_dispatch_missing_body_is_blocked(tmp_path: Path) -> None:
    payload = _build_approved_with_preview(tmp_path, body="")

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert any("body" in reason.lower() for reason in payload["reasons"])


def test_draft_dispatch_no_enabled_channel_is_blocked(tmp_path: Path) -> None:
    payload = _build_approved_with_preview(
        tmp_path,
        channels=[
            {
                "channel_id": "email",
                "channel_type": "email",
                "target_ref": "o***@example.com",
                "enabled": False,
                "would_send": False,
                "reason": "disabled.",
            }
        ],
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert payload["draft"]["draft_ready_channel_count"] == 0


def test_draft_dispatch_masks_unmasked_target_and_safety_blocks(tmp_path: Path) -> None:
    payload = _build_approved_with_preview(
        tmp_path,
        target_ref="operator@example.com",
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "SAFETY_BLOCKED"
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "operator@example.com" not in serialized
    assert "o***@example.com" in serialized


def test_draft_dispatch_schema_and_markdown_sections(tmp_path: Path) -> None:
    payload = _build_approved_with_preview(tmp_path)

    _assert_invariants(payload)
    assert set(payload) >= {
        "metadata",
        "input_refs",
        "approval_gate_summary",
        "draft",
        "hashes",
        "decision",
        "safety",
        "reasons",
        "warnings",
    }
    markdown = Path(payload["output_artifacts"]["draft_dispatch_markdown"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "## Final Decision" in markdown
    assert "## Draft Summary" in markdown
    assert "## Message Draft" in markdown
    assert "## Safety" in markdown


def test_draft_dispatch_hash_ignores_generated_at_only(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash=compute_dispatch_preview_hash(preview),
    )

    first = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=datetime(2026, 5, 24, 0, 0, tzinfo=UTC),
    )
    second = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=datetime(2026, 5, 24, 12, 30, tzinfo=UTC),
    )

    assert first["hashes"]["draft_hash"] == second["hashes"]["draft_hash"]
    assert compute_draft_dispatch_hash(first) == compute_draft_dispatch_hash(second)


def test_draft_dispatch_hash_changes_for_subject_body_channel_and_gate(tmp_path: Path) -> None:
    base = _build_approved_with_preview(tmp_path / "base")
    subject_changed = _build_approved_with_preview(tmp_path / "subject", subject="Changed")
    body_changed = _build_approved_with_preview(tmp_path / "body", body="# Changed\n")
    channel_changed = _build_approved_with_preview(
        tmp_path / "channel",
        channels=[
            {
                "channel_id": "chat",
                "channel_type": "file",
                "target_ref": "operator-chat-channel",
                "enabled": True,
                "would_send": True,
                "reason": "chat channel ready.",
            }
        ],
    )
    gate_changed = _build_with_gate_status(tmp_path / "gate", "APPROVAL_REQUIRED")

    base_hash = base["hashes"]["draft_hash"]
    assert subject_changed["hashes"]["draft_hash"] != base_hash
    assert body_changed["hashes"]["draft_hash"] != base_hash
    assert channel_changed["hashes"]["draft_hash"] != base_hash
    assert gate_changed["hashes"]["draft_hash"] != base_hash


def test_draft_dispatch_cli_generates_artifacts(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash=compute_dispatch_preview_hash(preview),
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_operator_brief_notification_draft_dispatch.py",
            "--date",
            as_of.isoformat(),
            "--data-root",
            str(data_root),
        ],
        check=False,
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "PASS: operator brief notification draft dispatch generated" in result.stdout
    output_root = data_root / "derived" / "operator_briefs" / "notifications" / "draft_dispatch"
    assert (output_root / f"operator_brief_notification_draft_dispatch_{as_of}.json").exists()
    assert (output_root / f"operator_brief_notification_draft_dispatch_{as_of}.md").exists()
    assert (output_root / "latest.json").exists()
    assert (output_root / "latest.md").exists()
    assert (output_root / "run.log").exists()


def test_draft_dispatch_never_imports_external_send_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash=compute_dispatch_preview_hash(preview),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "smtplib",
            "slack_sdk",
            "telegram",
            "discord",
            "gmail",
            "webhook",
            "requests",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"draft dispatch must not import send path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "DRAFT_READY"


def _build_with_gate_status(tmp_path: Path, gate_status: str) -> dict[str, Any]:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview_status = "NOOP" if gate_status == "NOOP" else "WOULD_SEND"
    preview = _write_dispatch_preview(data_root, as_of, final_status=preview_status)
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status=gate_status,
        preview_hash=compute_dispatch_preview_hash(preview),
    )
    return write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )


def _build_approved_with_preview(
    tmp_path: Path,
    *,
    subject: str = "[Trading System] Daily Operator Brief - OK - 2026-05-24",
    body: str = "# Operator Brief\nReady.\n",
    target_ref: str = "o***@example.com",
    channels: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(
        data_root,
        as_of,
        subject=subject,
        body=body,
        target_ref=target_ref,
        channels=channels,
    )
    _write_approval_gate(
        data_root,
        as_of,
        approval_gate_status="APPROVED",
        preview_hash=compute_dispatch_preview_hash(preview),
    )
    return write_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )


def _write_dispatch_preview(
    data_root: Path,
    as_of: date,
    *,
    final_status: str = "WOULD_SEND",
    subject: str = "[Trading System] Daily Operator Brief - OK - 2026-05-24",
    body: str = "# Operator Brief\nReady.\n",
    target_ref: str = "o***@example.com",
    channels: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications" / "dispatch_preview"
    json_path = root / f"operator_brief_notification_dispatch_preview_{as_of.isoformat()}.json"
    latest_path = root / "latest.json"
    payload = _dispatch_preview_payload(
        as_of,
        final_status=final_status,
        subject=subject,
        body=body,
        target_ref=target_ref,
        channels=channels,
    )
    root.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    json_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return payload


def _dispatch_preview_payload(
    as_of: date,
    *,
    final_status: str,
    subject: str,
    body: str,
    target_ref: str,
    channels: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    would_send = final_status == "WOULD_SEND"
    default_channels = [
        {
            "channel_id": "email",
            "channel_type": "email",
            "target_ref": target_ref,
            "enabled": True,
            "would_send": would_send,
            "reason": "email channel is enabled for dry-run preview only.",
        },
        {
            "channel_id": "chat",
            "channel_type": "file",
            "target_ref": "operator-chat-channel",
            "enabled": True,
            "would_send": False,
            "reason": "chat requires approval before real dispatch.",
        },
    ]
    message: dict[str, Any] = {
        "title_preview": "Daily Trading System Operator Brief - 2026-05-24",
        "body_length": len(body),
        "contains_markdown": True,
    }
    if subject:
        message["subject_preview"] = subject
    if body:
        message["body_markdown"] = body
        message["body_excerpt"] = body[:500]
    return {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_dispatch_preview",
        "task_id": "TRADING-032",
        "date": as_of.isoformat(),
        "mode": "dry_run",
        "production_effect": "none",
        "manual_review_only": True,
        "dispatch_preview_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_dispatch_preview": False,
        "notification_draft_executed_by_dispatch_preview": False,
        "delivery_preflight_executed_by_dispatch_preview": False,
        "pipelines_executed_by_dispatch_preview": False,
        "data_downloaded_by_dispatch_preview": False,
        "apply_executed_by_dispatch_preview": False,
        "rollback_executed_by_dispatch_preview": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-032",
            "task_name": "Operator Brief Notification Dry-run Dispatch Preview",
            "run_date": as_of.isoformat(),
            "generated_at": "2026-05-24T00:00:00Z",
            "preview_generated_at": "2026-05-24T00:00:00Z",
            "mode": "dry_run",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "dispatch_preview": {
            "dispatch_status": final_status,
            "channels": channels if channels is not None else default_channels,
            "message": message,
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "sensitive_content_flags": [],
        },
        "decision": {
            "final_status": final_status,
            "human_action_required": final_status != "NOOP",
            "next_recommended_action": "Review dispatch preview.",
        },
        "output_artifacts": {
            "run_log": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/run.log"
            }
        },
    }


def _write_approval_gate(
    data_root: Path,
    as_of: date,
    *,
    approval_gate_status: str,
    preview_hash: str,
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications" / "approval_gate"
    json_path = root / f"operator_brief_notification_approval_gate_{as_of.isoformat()}.json"
    latest_path = root / "latest.json"
    allowed = approval_gate_status == "APPROVED"
    payload = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_approval_gate",
        "task_id": "TRADING-033",
        "date": as_of.isoformat(),
        "mode": "approval_gate",
        "production_effect": "none",
        "manual_review_only": True,
        "approval_gate_only": True,
        "read_only": True,
        "metadata": {
            "task_id": "TRADING-033",
            "task_name": "Operator Brief Notification Approval Gate",
            "run_date": as_of.isoformat(),
            "generated_at": "2026-05-24T00:00:00Z",
            "mode": "approval_gate",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "dispatch_preview_summary": {
            "final_status": "WOULD_SEND" if approval_gate_status != "NOOP" else "NOOP",
            "human_action_required": approval_gate_status != "NOOP",
            "channel_count": 2,
            "would_send_channel_count": 1,
        },
        "hashes": {
            "dispatch_preview_hash": preview_hash,
            "hash_algorithm": "sha256",
            "hash_scope": "canonical_dispatch_preview_json",
        },
        "decision": {
            "approval_gate_status": approval_gate_status,
            "allowed_to_enter_dispatch": allowed,
            "human_action_required": not allowed and approval_gate_status != "NOOP",
            "next_recommended_action": "Review approval gate.",
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
        },
        "reasons": [],
        "warnings": [],
    }
    root.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    json_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return payload


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["draft_dispatch_only"] is True
    assert payload["read_only"] is True
    assert payload["external_side_effects"] is False
    assert payload["network_access_required"] is False
    assert payload["secrets_required"] is False
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["smtp_called"] is False
    assert payload["slack_sent"] is False
    assert payload["telegram_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["operator_brief_executed_by_draft_dispatch"] is False
    assert payload["notification_draft_executed_by_draft_dispatch"] is False
    assert payload["delivery_preflight_executed_by_draft_dispatch"] is False
    assert payload["dispatch_preview_executed_by_draft_dispatch"] is False
    assert payload["approval_gate_executed_by_draft_dispatch"] is False
    assert payload["pipelines_executed_by_draft_dispatch"] is False
    assert payload["data_downloaded_by_draft_dispatch"] is False
    assert payload["apply_executed_by_draft_dispatch"] is False
    assert payload["rollback_executed_by_draft_dispatch"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["metadata"]["task_id"] == "TRADING-034"
    assert payload["metadata"]["manual_review_only"] is True
    assert "sent_at" not in json.dumps(payload, ensure_ascii=False).lower()


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, tzinfo=UTC)
