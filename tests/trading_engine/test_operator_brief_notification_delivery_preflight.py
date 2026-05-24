from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight import (
    write_operator_brief_notification_delivery_preflight,
)


def test_delivery_preflight_valid_notification_draft_passes(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "PASS"
    assert payload["delivery_readiness"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["notification_severity"] == "NORMAL"
    assert payload["draft_validation"]["status"] == "PASS"
    assert payload["draft_validation"]["redaction_confirmed"] is True
    assert payload["input_artifacts"]["notification_draft_metadata"]["status"] == "FOUND"
    assert Path(payload["output_artifacts"]["preflight_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["preflight_markdown"]["path"]).exists()


def test_delivery_preflight_missing_metadata_is_input_missing(tmp_path: Path) -> None:
    payload = write_operator_brief_notification_delivery_preflight(
        as_of=date(2026, 5, 24),
        data_root=tmp_path / "data",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "INPUT_MISSING"
    assert payload["delivery_readiness"] == "UNKNOWN"


def test_delivery_preflight_invalid_json_is_input_invalid(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    path = _notification_metadata_path(data_root, date(2026, 5, 24))
    path.parent.mkdir(parents=True)
    path.write_text("{not json", encoding="utf-8")

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=date(2026, 5, 24),
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "INPUT_INVALID"
    assert payload["delivery_readiness"] == "UNKNOWN"


def test_delivery_preflight_wrong_task_id_is_input_invalid(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, metadata_overrides={"task_id": "TRADING-022"})

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "INPUT_INVALID"
    assert payload["safety_validation"]["notification_metadata_task_id_valid"] is False


def test_delivery_preflight_metadata_safety_invalid_is_safety_blocked(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, metadata_overrides={"email_sent": True})

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "SAFETY_BLOCKED"
    assert payload["delivery_readiness"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["status"] == "FAIL"


@pytest.mark.parametrize(
    "draft_key",
    ["email_draft", "chat_draft", "mobile_summary"],
)
def test_delivery_preflight_missing_required_draft_is_blocked(
    tmp_path: Path,
    draft_key: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    draft = _write_notification_draft(data_root, as_of)
    Path(draft["draft_outputs"][draft_key]["path"]).unlink()

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "BLOCKED"
    assert payload["delivery_readiness"] == "BLOCKED"
    assert payload["draft_validation"]["blocking_reasons"]


@pytest.mark.parametrize(
    "draft_key",
    ["email_draft", "chat_draft", "mobile_summary"],
)
def test_delivery_preflight_empty_required_draft_is_blocked(
    tmp_path: Path,
    draft_key: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    draft = _write_notification_draft(data_root, as_of)
    Path(draft["draft_outputs"][draft_key]["path"]).write_text("", encoding="utf-8")

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "BLOCKED"
    assert payload["draft_validation"]["blocking_reasons"]


@pytest.mark.parametrize(
    "content",
    [
        "api_key=[REDACTED]\nready\n",
        "password=[REDACTED]\nready\n",
        "token=[REDACTED]\nready\n",
    ],
)
def test_delivery_preflight_redacted_sensitive_assignment_passes(
    tmp_path: Path,
    content: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, email_content=content)

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "PASS"
    assert payload["draft_validation"]["sensitive_content_detected"] is False


@pytest.mark.parametrize(
    "content",
    [
        "api_key=abc123\nready\n",
        "password=rawpass\nready\n",
        "token=rawtoken\nready\n",
    ],
)
def test_delivery_preflight_unredacted_sensitive_assignment_safety_blocks(
    tmp_path: Path,
    content: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, email_content=content)

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "SAFETY_BLOCKED"
    assert payload["delivery_readiness"] == "SAFETY_BLOCKED"
    assert payload["draft_validation"]["sensitive_content_detected"] is True
    assert payload["alerts"]["critical"]


@pytest.mark.parametrize(
    ("severity", "expected_status", "expected_readiness"),
    [
        ("NORMAL", "PASS", "READY_FOR_MANUAL_REVIEW"),
        ("WATCH", "PASS", "READY_FOR_MANUAL_REVIEW"),
        ("ACTION", "PASS_WITH_WARNINGS", "NEEDS_APPROVAL"),
        ("URGENT", "PASS_WITH_WARNINGS", "NEEDS_APPROVAL"),
        ("BLOCKED", "BLOCKED", "BLOCKED"),
    ],
)
def test_delivery_preflight_severity_and_approval_mapping(
    tmp_path: Path,
    severity: str,
    expected_status: str,
    expected_readiness: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, severity=severity)

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == expected_status
    assert payload["delivery_readiness"] == expected_readiness
    if severity in {"ACTION", "URGENT"}:
        assert payload["approval_validation"]["approval_required"] is True


def test_delivery_preflight_urgent_markdown_contains_needs_approval_banner(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of, severity="URGENT")

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    markdown = Path(payload["output_artifacts"]["preflight_markdown"]["path"]).read_text(
        encoding="utf-8"
    )
    _assert_invariants(payload)
    assert payload["delivery_readiness"] == "NEEDS_APPROVAL"
    assert "## Notification Delivery Needs Approval" in markdown


def test_delivery_preflight_missing_channel_config_is_manual_review_only(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["channel_readiness"]["email"]["manual_send_only"] is True
    assert payload["channel_readiness"]["chat"]["manual_send_only"] is True
    assert payload["channel_readiness"]["mobile"]["manual_send_only"] is True
    assert payload["channel_readiness"]["email"]["can_send_automatically"] is False


@pytest.mark.parametrize(
    "channel_payload",
    [
        {"schema_version": "1.0", "auto_send_allowed": True},
        {"schema_version": "1.0", "email": {"enabled": True, "manual_send_only": False}},
    ],
)
def test_delivery_preflight_unsafe_channel_config_safety_blocks(
    tmp_path: Path,
    channel_payload: dict[str, Any],
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    channel_config = tmp_path / "channel.json"
    channel_config.write_text(
        json.dumps(channel_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        channel_config_file=channel_config,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["preflight_status"] == "SAFETY_BLOCKED"
    assert payload["delivery_readiness"] == "SAFETY_BLOCKED"


def test_delivery_preflight_webhook_url_in_channel_config_warns_without_leaking(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
    webhook_url = "https://hooks.example.test/raw-secret"
    channel_config = tmp_path / "channel.json"
    channel_config.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "chat": {
                    "enabled": True,
                    "manual_send_only": True,
                    "webhook_url": webhook_url,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        channel_config_file=channel_config,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    serialized = json.dumps(payload, ensure_ascii=False)
    assert payload["preflight_status"] == "PASS"
    assert any("Webhook URL" in item for item in payload["alerts"]["warnings"])
    assert webhook_url not in serialized


def test_delivery_preflight_never_sends_notifications_or_imports_execution_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_notification_draft(data_root, as_of)
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
            "discord",
            "gmail",
            "webhook",
            "run_daily_trading_system_operator_brief",
            "generate_operator_brief_notification_draft",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"delivery preflight must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False


def _write_notification_draft(
    data_root: Path,
    as_of: date,
    *,
    severity: str = "NORMAL",
    email_content: str = "# Email Draft\nReady for manual review.\n",
    chat_content: str = "# Chat Draft\nReady for manual review.\n",
    mobile_content: str = "Trading System OK - no manual action required.\n",
    metadata_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    notification_root = data_root / "derived" / "operator_briefs" / "notifications"
    json_path = notification_root / f"operator_brief_notification_draft_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    email_path = notification_root / "email" / f"operator_brief_email_draft_{suffix}.md"
    chat_path = notification_root / "chat" / f"operator_brief_chat_draft_{suffix}.md"
    mobile_path = notification_root / "mobile" / f"operator_brief_mobile_summary_{suffix}.md"
    draft_status = "SAFETY_BLOCKED" if severity == "BLOCKED" else "GENERATED"
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
        "draft_status": draft_status,
        "notification_severity": severity,
        "headline": "Notification drafts are ready for review.",
        "draft_outputs": {
            "email_draft": {"path": str(email_path)},
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "summary_markdown": {"path": str(markdown_path)},
        },
        "safety_validation": {
            "status": "PASS",
            "blocking_reasons": [],
        },
    }
    if metadata_overrides:
        payload.update(metadata_overrides)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    email_path.parent.mkdir(parents=True, exist_ok=True)
    chat_path.parent.mkdir(parents=True, exist_ok=True)
    mobile_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft\n", encoding="utf-8")
    email_path.write_text(email_content, encoding="utf-8")
    chat_path.write_text(chat_content, encoding="utf-8")
    mobile_path.write_text(mobile_content, encoding="utf-8")
    return payload


def _notification_metadata_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "operator_briefs"
        / "notifications"
        / f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["notification_delivery_preflight_only"] is True
    assert payload["read_only"] is True
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["operator_brief_executed_by_delivery_preflight"] is False
    assert payload["notification_draft_executed_by_delivery_preflight"] is False
    assert payload["pipelines_executed_by_delivery_preflight"] is False
    assert payload["data_downloaded_by_delivery_preflight"] is False
    assert payload["apply_executed_by_delivery_preflight"] is False
    assert payload["rollback_executed_by_delivery_preflight"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, tzinfo=UTC)
