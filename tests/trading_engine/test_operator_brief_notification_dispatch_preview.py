from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.operator_brief_notification_dispatch_preview import (
    write_operator_brief_notification_dispatch_preview,
)


def test_dispatch_preview_pass_preflight_generates_would_send(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_summary"]["status"] == "PASS"
    assert payload["dispatch_preview"]["dispatch_status"] == "WOULD_SEND"
    assert payload["decision"]["final_status"] == "WOULD_SEND"
    assert payload["preflight_summary"]["allowed_to_dispatch"] is True
    assert len(payload["dispatch_preview"]["channels"]) == 3
    assert (
        sum(1 for channel in payload["dispatch_preview"]["channels"] if channel["would_send"]) == 3
    )
    assert payload["dispatch_preview"]["message"]["subject_preview"] == (
        "[Trading System] Daily Operator Brief - OK - 2026-05-24"
    )
    assert Path(payload["output_artifacts"]["dispatch_preview_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["dispatch_preview_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["latest_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log"]["path"]).exists()


def test_dispatch_preview_needs_approval_from_preflight(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of, brief_status="ACTION_REQUIRED")
    _write_notification_draft(data_root, as_of, severity="ACTION")
    _write_preflight(
        data_root,
        as_of,
        preflight_status="PASS_WITH_WARNINGS",
        delivery_readiness="NEEDS_APPROVAL",
        notification_severity="ACTION",
        approval_required=True,
    )

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_summary"]["status"] == "NEEDS_APPROVAL"
    assert payload["decision"]["final_status"] == "NEEDS_APPROVAL"
    assert all(not channel["would_send"] for channel in payload["dispatch_preview"]["channels"])
    assert payload["decision"]["human_action_required"] is True


def test_dispatch_preview_safety_blocked_from_preflight(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(
        data_root,
        as_of,
        preflight_status="SAFETY_BLOCKED",
        delivery_readiness="SAFETY_BLOCKED",
        critical=["Preflight detected unredacted token."],
    )

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_summary"]["status"] == "SAFETY_BLOCKED"
    assert payload["decision"]["final_status"] == "SAFETY_BLOCKED"
    assert all(not channel["would_send"] for channel in payload["dispatch_preview"]["channels"])


def test_dispatch_preview_blocked_from_preflight(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(
        data_root,
        as_of,
        preflight_status="BLOCKED",
        delivery_readiness="BLOCKED",
        critical=["Email draft is missing."],
    )

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_summary"]["status"] == "BLOCKED"
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert any("preflight" in reason.lower() for reason in payload["preflight_summary"]["reasons"])


def test_dispatch_preview_missing_operator_brief_is_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "BLOCKED"
    assert payload["input_refs"]["operator_brief_json"]["status"] == "MISSING"


def test_dispatch_preview_notification_disabled_is_noop(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(
        data_root,
        as_of,
        notification_enabled=False,
        channels_enabled=False,
    )

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "NOOP"
    assert payload["decision"]["human_action_required"] is False
    assert all(not channel["would_send"] for channel in payload["dispatch_preview"]["channels"])


def test_dispatch_preview_masks_recipients_and_limits_body_excerpt(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    long_body = "Subject: Long body\n\n" + ("Line with detail for review.\n" * 80)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of, email_content=long_body)
    _write_preflight(data_root, as_of, target_ref="operator@example.com")

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    serialized = json.dumps(payload, ensure_ascii=False)
    excerpt = payload["dispatch_preview"]["message"]["body_excerpt"]
    assert "operator@example.com" not in serialized
    assert "o***@example.com" in serialized
    assert len(excerpt) <= 500
    assert excerpt.endswith("...")


def test_dispatch_preview_markdown_contains_manual_review_sections(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    markdown = Path(payload["output_artifacts"]["dispatch_preview_markdown"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "## Final Decision" in markdown
    assert "## Preflight Summary" in markdown
    assert "## Message Preview" in markdown
    assert "## Safety" in markdown


def test_dispatch_preview_never_imports_external_send_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)
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
            raise AssertionError(f"dispatch preview must not import send path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "WOULD_SEND"


def test_dispatch_preview_blocks_path_traversal_input(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    outside_preflight = tmp_path.parent / "operator_brief_notification_preflight_outside.json"
    outside_preflight.write_text("{}", encoding="utf-8")
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        input_preflight_file=outside_preflight,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"]["final_status"] == "SAFETY_BLOCKED"
    assert any("escapes repo root" in flag for flag in payload["safety"]["sensitive_content_flags"])


def test_dispatch_preview_run_log_is_generated(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
    _write_notification_draft(data_root, as_of)
    _write_preflight(data_root, as_of)

    payload = write_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    run_log = Path(payload["output_artifacts"]["run_log"]["path"])
    assert run_log.exists()
    text = run_log.read_text(encoding="utf-8")
    assert "final_status=WOULD_SEND" in text
    assert "email_sent=false" in text


def _write_operator_brief(
    data_root: Path,
    as_of: date,
    *,
    brief_status: str = "OK",
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs"
    json_path = root / f"daily_trading_system_operator_brief_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_trading_system_operator_brief",
        "task_id": "TRADING-022",
        "date": as_of.isoformat(),
        "mode": "daily_trading_system_operator_brief_only",
        "production_effect": "none",
        "manual_review_only": True,
        "operator_brief_only": True,
        "read_only": True,
        "brief_status": brief_status,
        "headline": "Daily operator brief is ready for review.",
        "recommended_next_steps": ["Review the operator brief."],
        "alerts": {"critical": [], "warnings": [], "notes": []},
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(
        f"# Daily Trading System Operator Brief - {as_of.isoformat()}\n\nReady.\n",
        encoding="utf-8",
    )
    return payload


def _write_notification_draft(
    data_root: Path,
    as_of: date,
    *,
    severity: str = "NORMAL",
    email_content: str = "# Email Draft\nReady for manual review.\n",
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications"
    json_path = root / f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    email_path = root / "email" / f"operator_brief_email_draft_{as_of.isoformat()}.md"
    chat_path = root / "chat" / f"operator_brief_chat_draft_{as_of.isoformat()}.md"
    mobile_path = root / "mobile" / f"operator_brief_mobile_summary_{as_of.isoformat()}.md"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_draft",
        "task_id": "TRADING-030",
        "date": as_of.isoformat(),
        "mode": "operator_brief_notification_draft_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "draft_status": "GENERATED",
        "notification_severity": severity,
        "draft_outputs": {
            "email_draft": {
                "path": f"data/derived/operator_briefs/notifications/email/{email_path.name}",
                "subject": f"[Trading System] Daily Operator Brief - OK - {as_of.isoformat()}",
            },
            "chat_draft": {
                "path": f"data/derived/operator_briefs/notifications/chat/{chat_path.name}"
            },
            "mobile_summary": {
                "path": f"data/derived/operator_briefs/notifications/mobile/{mobile_path.name}"
            },
            "summary_markdown": {
                "path": f"data/derived/operator_briefs/notifications/{markdown_path.name}"
            },
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    email_path.parent.mkdir(parents=True, exist_ok=True)
    chat_path.parent.mkdir(parents=True, exist_ok=True)
    mobile_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft\n", encoding="utf-8")
    email_path.write_text(email_content, encoding="utf-8")
    chat_path.write_text("# Chat Draft\nReady.\n", encoding="utf-8")
    mobile_path.write_text("Mobile summary ready.\n", encoding="utf-8")
    return payload


def _write_preflight(
    data_root: Path,
    as_of: date,
    *,
    preflight_status: str = "PASS",
    delivery_readiness: str = "READY_FOR_MANUAL_REVIEW",
    notification_severity: str = "NORMAL",
    approval_required: bool = False,
    critical: list[str] | None = None,
    target_ref: str = "operator@example.com",
    notification_enabled: bool | None = None,
    channels_enabled: bool = True,
) -> dict[str, Any]:
    root = data_root / "derived" / "operator_briefs" / "notifications" / "delivery_preflight"
    json_path = root / f"operator_brief_notification_delivery_preflight_{as_of.isoformat()}.json"
    draft_metadata_path = (
        "data/derived/operator_briefs/notifications/"
        f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )
    channel_status = "READY_FOR_MANUAL_REVIEW" if channels_enabled else "BLOCKED"
    blocking_reasons = [] if channels_enabled else ["channel is disabled in channel config."]
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_delivery_preflight",
        "task_id": "TRADING-031",
        "date": as_of.isoformat(),
        "mode": "operator_brief_notification_delivery_preflight_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_delivery_preflight_only": True,
        "read_only": True,
        "preflight_status": preflight_status,
        "delivery_readiness": delivery_readiness,
        "notification_severity": notification_severity,
        "notification_enabled": notification_enabled,
        "approval_validation": {
            "status": "PASS_WITH_WARNINGS" if approval_required else "PASS",
            "approval_required": approval_required,
            "approval_policy_available": False,
            "approval_reason": (
                "ACTION severity requires delivery approval."
                if approval_required
                else "NORMAL severity does not require delivery approval."
            ),
            "blocking_reasons": [],
            "warnings": [],
        },
        "channel_readiness": {
            "email": {
                "status": channel_status,
                "draft_available": True,
                "enabled": channels_enabled,
                "target_ref": target_ref,
                "manual_send_only": True,
                "blocking_reasons": blocking_reasons,
                "warnings": [],
            },
            "chat": {
                "status": channel_status,
                "draft_available": True,
                "enabled": channels_enabled,
                "target_ref": "#operator-brief",
                "manual_send_only": True,
                "blocking_reasons": blocking_reasons,
                "warnings": [],
            },
            "mobile": {
                "status": channel_status,
                "draft_available": True,
                "enabled": channels_enabled,
                "target_ref": "operator-mobile-review-target",
                "manual_send_only": True,
                "blocking_reasons": blocking_reasons,
                "warnings": [],
            },
        },
        "input_artifacts": {
            "notification_draft_metadata": {
                "status": "FOUND",
                "path": draft_metadata_path,
            }
        },
        "alerts": {
            "critical": critical or [],
            "warnings": [],
            "notes": ["Delivery preflight is read-only and did not send any notification."],
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["dispatch_preview_only"] is True
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
    assert payload["operator_brief_executed_by_dispatch_preview"] is False
    assert payload["notification_draft_executed_by_dispatch_preview"] is False
    assert payload["delivery_preflight_executed_by_dispatch_preview"] is False
    assert payload["pipelines_executed_by_dispatch_preview"] is False
    assert payload["data_downloaded_by_dispatch_preview"] is False
    assert payload["apply_executed_by_dispatch_preview"] is False
    assert payload["rollback_executed_by_dispatch_preview"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["metadata"]["task_id"] == "TRADING-032"
    assert payload["metadata"]["manual_review_only"] is True
    assert set(payload) >= {
        "metadata",
        "input_refs",
        "preflight_summary",
        "dispatch_preview",
        "safety",
        "decision",
    }


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, tzinfo=UTC)
