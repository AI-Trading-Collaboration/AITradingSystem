from __future__ import annotations

import builtins
import json
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.operator_brief_notification_approval_gate import (
    compute_dispatch_preview_hash,
    write_operator_brief_notification_approval_gate,
)


def test_approval_gate_would_send_without_marker_requires_approval(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "APPROVAL_REQUIRED"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False
    assert payload["approval_marker_summary"]["exists"] is False
    assert payload["hashes"]["dispatch_preview_hash"].startswith("sha256:")


def test_approval_gate_would_send_with_valid_marker_is_approved(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
    _write_approval_marker(
        data_root,
        preview_hash=compute_dispatch_preview_hash(preview),
        expires_at="2026-05-25T12:00:00Z",
    )

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "APPROVED"
    assert payload["decision"]["allowed_to_enter_dispatch"] is True
    assert payload["approval_marker_summary"]["hash_matches"] is True
    assert payload["approval_marker_summary"]["expired"] is False
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "operator@example.com" not in serialized
    assert "o***@example.com" in serialized
    assert Path(payload["output_artifacts"]["approval_gate_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["approval_gate_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log"]["path"]).exists()


def test_approval_gate_expired_marker_is_expired(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
    _write_approval_marker(
        data_root,
        preview_hash=compute_dispatch_preview_hash(preview),
        expires_at="2026-05-24T11:59:59Z",
    )

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "APPROVAL_EXPIRED"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False
    assert payload["approval_marker_summary"]["hash_matches"] is True
    assert payload["approval_marker_summary"]["expired"] is True


def test_approval_gate_hash_mismatch_blocks_old_approval(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
    _write_approval_marker(
        data_root,
        preview_hash="sha256:" + ("0" * 64),
        expires_at="2026-05-25T12:00:00Z",
    )

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "APPROVAL_MISMATCH"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False
    assert payload["approval_marker_summary"]["hash_matches"] is False


def test_approval_gate_needs_approval_from_preview_requires_approval(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="NEEDS_APPROVAL")

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["dispatch_preview_summary"]["final_status"] == "NEEDS_APPROVAL"
    assert payload["decision"]["approval_gate_status"] == "APPROVAL_REQUIRED"


def test_approval_gate_safety_blocked_preview_is_safety_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(
        data_root,
        as_of,
        final_status="SAFETY_BLOCKED",
        sensitive_flags=["body contains private key material."],
    )
    _write_approval_marker(
        data_root,
        preview_hash=compute_dispatch_preview_hash(preview),
        expires_at="2026-05-25T12:00:00Z",
    )

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "SAFETY_BLOCKED"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False
    assert payload["safety"]["safety_blocked_from_preview"] is True
    assert any("cannot override" in reason for reason in payload["reasons"])


def test_approval_gate_blocked_preview_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="BLOCKED")

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "BLOCKED"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False


def test_approval_gate_noop_preview_is_noop(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="NOOP")

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "NOOP"
    assert payload["decision"]["allowed_to_enter_dispatch"] is False
    assert payload["decision"]["human_action_required"] is False


def test_approval_gate_missing_preview_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "BLOCKED"
    assert payload["input_refs"]["dispatch_preview_artifact"]["status"] == "MISSING"


def test_approval_gate_invalid_marker_json_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
    marker_path = (
        data_root
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "approval_gate"
        / "approval_marker.json"
    )
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text("{not json", encoding="utf-8")

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "BLOCKED"
    assert payload["input_refs"]["approval_marker"]["status"] == "INVALID"


def test_dispatch_preview_hash_ignores_generated_at_only() -> None:
    as_of = date(2026, 5, 24)
    preview = _dispatch_preview_payload(as_of, final_status="WOULD_SEND")
    changed = deepcopy(preview)
    changed["metadata"]["generated_at"] = "2026-05-24T12:34:56Z"
    changed["metadata"]["preview_generated_at"] = "2026-05-24T12:34:56Z"
    changed["audit"]["created_at"] = "2026-05-24T12:34:56Z"

    assert compute_dispatch_preview_hash(preview) == compute_dispatch_preview_hash(changed)


def test_dispatch_preview_hash_changes_for_message_channel_and_status() -> None:
    as_of = date(2026, 5, 24)
    preview = _dispatch_preview_payload(as_of, final_status="WOULD_SEND")

    message_changed = deepcopy(preview)
    message_changed["dispatch_preview"]["message"]["subject_preview"] = "Changed subject"
    channel_changed = deepcopy(preview)
    channel_changed["dispatch_preview"]["channels"][0]["target_ref"] = "operator-chat-channel"
    status_changed = deepcopy(preview)
    status_changed["decision"]["final_status"] = "NEEDS_APPROVAL"
    status_changed["dispatch_preview"]["dispatch_status"] = "NEEDS_APPROVAL"

    base_hash = compute_dispatch_preview_hash(preview)
    assert compute_dispatch_preview_hash(message_changed) != base_hash
    assert compute_dispatch_preview_hash(channel_changed) != base_hash
    assert compute_dispatch_preview_hash(status_changed) != base_hash


def test_approval_gate_markdown_contains_required_sections(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    preview = _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
    _write_approval_marker(
        data_root,
        preview_hash=compute_dispatch_preview_hash(preview),
        expires_at="2026-05-25T12:00:00Z",
    )

    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    markdown = Path(payload["output_artifacts"]["approval_gate_markdown"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "## Final Decision" in markdown
    assert "## Approval Marker" in markdown
    assert "## Hash" in markdown
    assert "## Safety" in markdown


def test_approval_gate_cli_generates_artifacts(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_operator_brief_notification_approval_gate.py",
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
    assert "LIMITED: operator brief notification approval gate generated" in result.stdout
    output_root = data_root / "derived" / "operator_briefs" / "notifications" / "approval_gate"
    assert (output_root / f"operator_brief_notification_approval_gate_{as_of}.json").exists()
    assert (output_root / f"operator_brief_notification_approval_gate_{as_of}.md").exists()
    assert (output_root / "latest.json").exists()
    assert (output_root / "latest.md").exists()
    assert (output_root / "run.log").exists()


def test_approval_gate_never_imports_external_send_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_dispatch_preview(data_root, as_of, final_status="WOULD_SEND")
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
            raise AssertionError(f"approval gate must not import send path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_now(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["approval_gate_status"] == "APPROVAL_REQUIRED"


def _write_dispatch_preview(
    data_root: Path,
    as_of: date,
    *,
    final_status: str,
    sensitive_flags: list[str] | None = None,
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications" / "dispatch_preview"
    json_path = root / f"operator_brief_notification_dispatch_preview_{as_of.isoformat()}.json"
    latest_path = root / "latest.json"
    payload = _dispatch_preview_payload(
        as_of,
        final_status=final_status,
        sensitive_flags=sensitive_flags,
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
    sensitive_flags: list[str] | None = None,
) -> dict[str, Any]:
    would_send = final_status == "WOULD_SEND"
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
        "input_refs": {
            "preflight_artifact": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/"
                    f"operator_brief_notification_delivery_preflight_{as_of.isoformat()}.json"
                ),
                "status": "FOUND",
            }
        },
        "preflight_summary": {
            "status": "PASS" if would_send else final_status,
            "allowed_to_dispatch": would_send,
            "reasons": [],
            "warnings": [],
        },
        "dispatch_preview": {
            "dispatch_status": final_status,
            "channels": [
                {
                    "channel_id": "email",
                    "channel_type": "email",
                    "target_ref": "o***@example.com",
                    "enabled": True,
                    "would_send": would_send,
                    "reason": "email channel is enabled for dry-run preview only.",
                }
            ],
            "message": {
                "subject_preview": "[Trading System] Daily Operator Brief - OK - 2026-05-24",
                "title_preview": "Daily Trading System Operator Brief - 2026-05-24",
                "body_excerpt": "Ready for manual review.",
                "body_length": 24,
                "contains_markdown": True,
            },
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "sensitive_content_flags": sensitive_flags or [],
        },
        "decision": {
            "final_status": final_status,
            "human_action_required": final_status != "NOOP",
            "next_recommended_action": "Review dispatch preview.",
        },
        "audit": {
            "created_by": "scripts/run_operator_brief_notification_dispatch_preview.py",
            "created_at": "2026-05-24T00:00:00Z",
            "read_only": True,
        },
        "output_artifacts": {
            "run_log": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/run.log"
            }
        },
    }


def _write_approval_marker(
    data_root: Path,
    *,
    preview_hash: str,
    expires_at: str,
    approved: bool = True,
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications" / "approval_gate"
    marker_path = root / "approval_marker.json"
    payload = {
        "task_id": "TRADING-033",
        "approval_type": "manual",
        "approved": approved,
        "approved_by": "operator@example.com",
        "approved_at": "2026-05-24T12:00:00Z",
        "expires_at": expires_at,
        "preview_hash": preview_hash,
        "approval_note": "Reviewed by operator@example.com.",
    }
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["approval_gate_only"] is True
    assert payload["read_only"] is True
    assert payload["external_side_effects"] is False
    assert payload["network_access_required"] is False
    assert payload["secrets_required"] is False
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["slack_sent"] is False
    assert payload["telegram_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["operator_brief_executed_by_approval_gate"] is False
    assert payload["notification_draft_executed_by_approval_gate"] is False
    assert payload["delivery_preflight_executed_by_approval_gate"] is False
    assert payload["dispatch_preview_executed_by_approval_gate"] is False
    assert payload["pipelines_executed_by_approval_gate"] is False
    assert payload["data_downloaded_by_approval_gate"] is False
    assert payload["apply_executed_by_approval_gate"] is False
    assert payload["rollback_executed_by_approval_gate"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["metadata"]["task_id"] == "TRADING-033"
    assert payload["metadata"]["manual_review_only"] is True
    assert set(payload) >= {
        "metadata",
        "input_refs",
        "dispatch_preview_summary",
        "approval_marker_summary",
        "hashes",
        "decision",
        "safety",
        "reasons",
        "warnings",
    }


def _fixed_now() -> datetime:
    return datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
