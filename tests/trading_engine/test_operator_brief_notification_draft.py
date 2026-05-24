from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.operator_brief_notification_draft import (
    write_operator_brief_notification_draft,
)


def test_notification_draft_valid_operator_brief_ok_generates_all_outputs(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "GENERATED"
    assert payload["notification_severity"] == "NORMAL"
    assert payload["source_snapshot"]["brief_status"] == "OK"
    outputs = payload["draft_outputs"]
    assert Path(outputs["email_draft"]["path"]).exists()
    assert Path(outputs["chat_draft"]["path"]).exists()
    assert Path(outputs["mobile_summary"]["path"]).exists()
    assert Path(outputs["summary_markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["metadata_json"]["path"]).exists()
    assert outputs["email_draft"]["subject"] == (
        "[Trading System] Daily Operator Brief - OK - 2026-05-24"
    )


def test_notification_draft_missing_operator_brief_is_input_missing(tmp_path: Path) -> None:
    payload = write_operator_brief_notification_draft(
        as_of=date(2026, 5, 24),
        data_root=tmp_path / "data",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "INPUT_MISSING"
    assert payload["notification_severity"] == "UNKNOWN"
    assert payload["input_artifacts"]["operator_brief"]["status"] == "MISSING"


def test_notification_draft_invalid_json_is_input_invalid(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    path = (
        data_root
        / "derived"
        / "operator_briefs"
        / ("daily_trading_system_operator_brief_2026-05-24.json")
    )
    path.parent.mkdir(parents=True)
    path.write_text("{not json", encoding="utf-8")

    payload = write_operator_brief_notification_draft(
        as_of=date(2026, 5, 24),
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "INPUT_INVALID"
    assert payload["notification_severity"] == "UNKNOWN"


def test_notification_draft_wrong_task_id_is_input_invalid(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of, overrides={"task_id": "TRADING-020"})

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "INPUT_INVALID"
    assert payload["safety_validation"]["operator_brief_task_id_valid"] is False


def test_notification_draft_operator_brief_safety_invalid_is_blocked(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of, overrides={"broker_execution": True})

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "SAFETY_BLOCKED"
    assert payload["notification_severity"] == "BLOCKED"
    assert payload["safety_validation"]["status"] == "FAIL"
    email = Path(payload["draft_outputs"]["email_draft"]["path"]).read_text(encoding="utf-8")
    chat = Path(payload["draft_outputs"]["chat_draft"]["path"]).read_text(encoding="utf-8")
    assert "## Notification Draft Safety Blocked" in email
    assert "Notification Draft Safety Blocked" in chat


@pytest.mark.parametrize(
    ("brief_status", "expected_severity"),
    [
        ("OK", "NORMAL"),
        ("WATCH", "WATCH"),
        ("ACTION_REQUIRED", "ACTION"),
        ("URGENT", "URGENT"),
        ("SAFETY_BLOCKED", "BLOCKED"),
    ],
)
def test_notification_draft_severity_mapping(
    tmp_path: Path,
    brief_status: str,
    expected_severity: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of, brief_status=brief_status)

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["notification_severity"] == expected_severity


@pytest.mark.parametrize(
    ("brief_status", "expected_text"),
    [
        ("URGENT", "## URGENT: Manual Attention Required"),
        ("ACTION_REQUIRED", "## Action Required"),
        ("SAFETY_BLOCKED", "## Notification Draft Safety Blocked"),
    ],
)
def test_notification_draft_email_banners(
    tmp_path: Path,
    brief_status: str,
    expected_text: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of, brief_status=brief_status)

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    email = Path(payload["draft_outputs"]["email_draft"]["path"]).read_text(encoding="utf-8")
    assert expected_text in email


def test_notification_draft_never_sends_notifications(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(data_root, as_of)
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
            "run_daily_trading_system_operator_brief",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"notification draft must not import send path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["mobile_push_sent"] is False


@pytest.mark.parametrize(
    ("headline", "alerts", "secret_value"),
    [
        ("Trading system OK api_key=abc123", {"notes": []}, "abc123"),
        ("Trading system OK", {"notes": ["operator password=hunter2"]}, "hunter2"),
    ],
)
def test_notification_draft_redacts_sensitive_fields(
    tmp_path: Path,
    headline: str,
    alerts: dict[str, list[str]],
    secret_value: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 24)
    _write_operator_brief(
        data_root,
        as_of,
        headline=headline,
        overrides={"alerts": {"critical": [], "warnings": [], **alerts}},
    )

    payload = write_operator_brief_notification_draft(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["draft_status"] == "GENERATED_WITH_WARNINGS"
    assert any("Redacted sensitive field" in item for item in payload["alerts"]["warnings"])
    for output in payload["draft_outputs"].values():
        path = Path(output["path"])
        if path.exists():
            assert secret_value not in path.read_text(encoding="utf-8")
    metadata = Path(payload["output_artifacts"]["metadata_json"]["path"]).read_text(
        encoding="utf-8"
    )
    assert secret_value not in metadata


def _write_operator_brief(
    data_root: Path,
    as_of: date,
    *,
    brief_status: str = "OK",
    headline: str = "Trading system status is stable. No immediate manual action is required.",
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    path = (
        data_root
        / "derived"
        / "operator_briefs"
        / (f"daily_trading_system_operator_brief_{suffix}.json")
    )
    markdown_path = path.with_suffix(".md")
    summary_level = {
        "OK": "NORMAL",
        "WATCH": "WATCH",
        "ACTION_REQUIRED": "ACTION",
        "URGENT": "URGENT",
        "SAFETY_BLOCKED": "UNKNOWN",
    }.get(brief_status, "UNKNOWN")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_trading_system_operator_brief",
        "task_id": "TRADING-022",
        "date": suffix,
        "mode": "daily_trading_system_operator_brief_only",
        "production_effect": "none",
        "manual_review_only": True,
        "operator_brief_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "brief_status": brief_status,
        "summary_level": summary_level,
        "headline": headline,
        "system_snapshot": {
            "overall_system_status": brief_status,
            "can_trust_outputs_today": brief_status in ("OK", "WATCH"),
            "manual_action_required": brief_status in ("ACTION_REQUIRED", "URGENT"),
        },
        "parameter_governance": {"status": "OK", "digest_status": "OK"},
        "pipeline_health": {"status": "OK", "health_status": "OK"},
        "data_freshness": {"status": "OK", "freshness_status": "OK"},
        "alerts": {"critical": [], "warnings": [], "notes": []},
        "recommended_next_steps": ["Continue observation."],
        "output_artifacts": {
            "json": {"path": str(path)},
            "markdown": {"path": str(markdown_path)},
        },
    }
    if overrides:
        payload.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Daily Trading System Operator Brief\n", encoding="utf-8")
    return payload


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["notification_draft_only"] is True
    assert payload["read_only"] is True
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["operator_brief_executed_by_notification_draft"] is False
    assert payload["pipelines_executed_by_notification_draft"] is False
    assert payload["data_downloaded_by_notification_draft"] is False
    assert payload["apply_executed_by_notification_draft"] is False
    assert payload["rollback_executed_by_notification_draft"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, tzinfo=UTC)
