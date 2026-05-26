from __future__ import annotations

import builtins
import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.retry_candidate_queue import (
    render_retry_candidate_queue_markdown,
    should_fail_cli,
    write_retry_candidate_queue,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_retry_candidate_queue.py"


def test_pass_classification_generates_empty_queue(tmp_path: Path) -> None:
    source = _write_classification_report(tmp_path, overall_status="PASS")

    payload = _run_queue(tmp_path, source)

    _assert_safety_invariants(payload)
    assert payload["source_classification"]["overall_status"] == "PASS"
    assert payload["queue_summary"] == {
        "queue_status": "EMPTY",
        "total_candidates": 0,
        "approved_candidates": 0,
        "blocked_candidates": 0,
        "manual_review_required": False,
        "has_retryable_candidates": False,
        "safe_to_execute_retry": False,
    }
    assert payload["candidate_queue"] == []
    assert payload["blocked_items"] == []


def test_retryable_transient_failure_generates_pending_approval_candidate(
    tmp_path: Path,
) -> None:
    source = _write_classification_report(
        tmp_path,
        overall_status="WARN",
        highest_severity="WARN",
        category="TRANSIENT_DELIVERY_FAILURE",
        reason="temporary smtp timeout",
    )

    payload = _run_queue(tmp_path, source)

    assert payload["queue_summary"]["queue_status"] == "PENDING_APPROVAL"
    assert payload["queue_summary"]["total_candidates"] == 1
    assert payload["queue_summary"]["manual_review_required"] is True
    assert payload["queue_summary"]["safe_to_execute_retry"] is False
    candidate = payload["candidate_queue"][0]
    assert candidate["candidate_id"] == "retry_candidate_2026-05-26_001"
    assert candidate["source_category"] == "TRANSIENT_DELIVERY_FAILURE"
    assert candidate["source_item_id"] == "delivery_failure_001"
    assert candidate["retryable"] is True
    assert candidate["requires_manual_review"] is True
    assert candidate["approval_required"] is True
    assert candidate["retry_status"] == "PENDING_APPROVAL"
    assert candidate["retry_blockers"] == []
    assert payload["approval_gate"]["retry_execution_allowed"] is False


def test_configuration_failure_generates_blocked_item(tmp_path: Path) -> None:
    payload = _queue_for_category(
        tmp_path,
        category="CONFIGURATION_FAILURE",
        reason="channel config missing",
    )

    assert payload["queue_summary"]["queue_status"] == "BLOCKED"
    _assert_single_blocked_item(payload, "CONFIGURATION_FAILURE")


def test_content_mismatch_generates_blocked_item(tmp_path: Path) -> None:
    payload = _queue_for_category(
        tmp_path,
        category="CONTENT_MISMATCH",
        reason="draft hash mismatch",
    )

    assert payload["queue_summary"]["queue_status"] == "BLOCKED"
    _assert_single_blocked_item(payload, "CONTENT_MISMATCH")


def test_missing_artifact_generates_blocked_item(tmp_path: Path) -> None:
    payload = _queue_for_category(
        tmp_path,
        category="MISSING_ARTIFACT",
        reason="dispatch artifact missing",
    )

    assert payload["queue_summary"]["queue_status"] == "BLOCKED"
    _assert_single_blocked_item(payload, "MISSING_ARTIFACT")


def test_unknown_failure_generates_blocked_item(tmp_path: Path) -> None:
    payload = _queue_for_category(
        tmp_path,
        category="UNKNOWN",
        reason="unexpected audit status",
        highest_severity="WARN",
        overall_status="UNKNOWN",
    )

    assert payload["queue_summary"]["queue_status"] == "BLOCKED"
    _assert_single_blocked_item(payload, "UNKNOWN")


def test_safety_blocked_generates_safety_blocked_queue(tmp_path: Path) -> None:
    source = _write_classification_report(
        tmp_path,
        overall_status="CRITICAL",
        highest_severity="CRITICAL",
        category="SAFETY_BLOCKED",
        reason="webhook_called=true",
    )

    payload = _run_queue(tmp_path, source)
    markdown = render_retry_candidate_queue_markdown(payload)

    assert payload["queue_summary"]["queue_status"] == "SAFETY_BLOCKED"
    assert payload["queue_summary"]["manual_review_required"] is True
    assert payload["queue_summary"]["safe_to_execute_retry"] is False
    assert payload["approval_gate"]["retry_execution_allowed"] is False
    assert should_fail_cli(payload, fail_on_safety_blocked=True) is True
    assert "CRITICAL: Safety blocked. Retry execution is not allowed." in markdown


def test_missing_source_classification_report_is_source_unavailable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "notification_delivery_failure_classification_2026-05-26.json"

    payload = _run_queue(tmp_path, source)

    assert payload["source_classification"]["source_available"] is False
    assert payload["source_classification"]["source_parse_status"] == "MISSING"
    assert payload["queue_summary"]["queue_status"] == "SOURCE_UNAVAILABLE"
    assert payload["queue_summary"]["manual_review_required"] is True
    assert payload["queue_summary"]["safe_to_execute_retry"] is False


def test_malformed_source_json_is_source_unavailable(tmp_path: Path) -> None:
    source = tmp_path / "notification_delivery_failure_classification_2026-05-26.json"
    source.write_text("{not-json", encoding="utf-8")

    payload = _run_queue(tmp_path, source)

    assert payload["source_classification"]["source_available"] is True
    assert payload["source_classification"]["source_parse_status"] == "MALFORMED_JSON"
    assert payload["queue_summary"]["queue_status"] == "SOURCE_UNAVAILABLE"
    assert payload["candidate_queue"] == []
    assert payload["blocked_items"] == []


def test_candidate_id_generation_is_deterministic(tmp_path: Path) -> None:
    source = _write_classification_report(
        tmp_path,
        category="TRANSIENT_DELIVERY_FAILURE",
        item_count=2,
    )

    first = _run_queue(tmp_path, source)
    second = _run_queue(tmp_path, source)

    assert [item["candidate_id"] for item in first["candidate_queue"]] == [
        "retry_candidate_2026-05-26_001",
        "retry_candidate_2026-05-26_002",
    ]
    assert first["candidate_queue"] == second["candidate_queue"]


def test_json_contains_required_metadata_and_safety_invariants(tmp_path: Path) -> None:
    source = _write_classification_report(tmp_path, overall_status="PASS")

    payload = _run_queue(tmp_path, source)

    _assert_safety_invariants(payload)
    assert payload["metadata"]["mode"] == "read_only"
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_only"] is True
    assert payload["safety_invariants"] == {
        "read_only": True,
        "no_external_delivery": True,
        "no_retry_execution": True,
        "no_state_mutation": True,
        "no_production_parameter_change": True,
        "dashboard_read_only": True,
    }


def test_markdown_contains_status_banner_and_approval_gate(tmp_path: Path) -> None:
    source = _write_classification_report(
        tmp_path,
        category="TRANSIENT_DELIVERY_FAILURE",
        reason="temporary network timeout",
    )

    payload = _run_queue(tmp_path, source)
    markdown = Path(
        payload["output_artifacts"]["retry_candidate_queue_markdown"]["path"]
    ).read_text(encoding="utf-8")

    assert "PENDING_APPROVAL: Retry candidates exist but require manual approval." in markdown
    assert "## Approval Gate" in markdown
    assert "## Safety Invariants" in markdown


def test_dashboard_reads_existing_queue_json_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 26)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)
    queue = _write_queue_artifact(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_retry_candidate_queue",
            "ai_trading_system.trading_engine.retry_candidate_queue",
            "run_notification_delivery_failure_classification",
            "ai_trading_system.trading_engine.notification_delivery_failure_classification",
            "run_notification_delivery_audit_summary",
            "ai_trading_system.trading_engine.notification_delivery_audit_summary",
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
            raise AssertionError(f"dashboard must not import queue or retry path: {name}")
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

    summary = payload["retry_candidate_queue"]
    assert summary["queue_status"] == "PENDING_APPROVAL"
    assert summary["total_candidates"] == 1
    assert summary["blocked_candidates"] == 0
    assert summary["manual_review_required"] is True
    assert summary["has_retryable_candidates"] is True
    assert summary["safe_to_execute_retry"] is False
    assert summary["approval_status"] == "NOT_REQUESTED"
    assert summary["retry_execution_allowed"] is False
    assert summary["source_classification_status"] == "WARN"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["retry_candidate_queue_only"] is True
    assert "Retry Candidate Queue" in html
    assert "queue_status" in html
    assert queue["output_artifacts"]["retry_candidate_queue_markdown"]["path"] in html


def test_dashboard_handles_missing_queue_report_gracefully(tmp_path: Path) -> None:
    as_of = date(2026, 5, 26)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    summary = payload["retry_candidate_queue"]
    assert summary["exists"] is False
    assert summary["queue_status"] == "MISSING"
    assert summary["safe_to_execute_retry"] is False
    assert "No retry candidate queue report available." in html


def test_cli_writes_json_markdown_and_log_with_explicit_classification_report(
    tmp_path: Path,
) -> None:
    source = _write_classification_report(tmp_path, overall_status="PASS")
    output_dir = tmp_path / "queue"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--classification-report",
            str(source),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    json_path = output_dir / "retry_candidate_queue_2026-05-26.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    assert json_path.exists()
    assert markdown_path.exists()
    assert log_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["source_classification"]["classification_report_path"] == str(source)
    assert "Retry Candidate Queue: EMPTY" in result.stdout


def test_cli_supports_custom_output_dir_and_as_of_date(tmp_path: Path) -> None:
    source = _write_classification_report(tmp_path, as_of=date(2026, 5, 25))
    output_dir = tmp_path / "custom-output"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--classification-report",
            str(source),
            "--output-dir",
            str(output_dir),
            "--as-of-date",
            "2026-05-26",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "retry_candidate_queue_2026-05-26.json").exists()


def test_cli_fail_on_safety_blocked_returns_non_zero(tmp_path: Path) -> None:
    source = _write_classification_report(
        tmp_path,
        overall_status="CRITICAL",
        highest_severity="CRITICAL",
        category="SAFETY_BLOCKED",
        reason="external delivery side effect",
    )
    output_dir = tmp_path / "queue"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--classification-report",
            str(source),
            "--output-dir",
            str(output_dir),
            "--fail-on-safety-blocked",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert (output_dir / "retry_candidate_queue_2026-05-26.json").exists()


def _run_queue(tmp_path: Path, source: Path) -> dict[str, Any]:
    return write_retry_candidate_queue(
        classification_report_path=source,
        output_dir=tmp_path / "queue",
        generated_at=_fixed_generated_at(),
    )


def _queue_for_category(
    tmp_path: Path,
    *,
    category: str,
    reason: str,
    overall_status: str = "ERROR",
    highest_severity: str = "ERROR",
) -> dict[str, Any]:
    source = _write_classification_report(
        tmp_path,
        overall_status=overall_status,
        highest_severity=highest_severity,
        category=category,
        reason=reason,
    )
    return _run_queue(tmp_path, source)


def _assert_single_blocked_item(payload: dict[str, Any], category: str) -> None:
    assert payload["queue_summary"]["total_candidates"] == 0
    assert payload["queue_summary"]["blocked_candidates"] == 1
    assert payload["queue_summary"]["manual_review_required"] is True
    assert payload["queue_summary"]["safe_to_execute_retry"] is False
    blocked = payload["blocked_items"][0]
    assert blocked["source_category"] == category
    assert blocked["retryable"] is False
    assert blocked["requires_manual_review"] is True
    assert blocked["approval_required"] is True
    assert blocked["retry_status"] == "BLOCKED"
    assert blocked["retry_blockers"]


def _write_classification_report(
    root: Path,
    *,
    as_of: date = date(2026, 5, 26),
    overall_status: str = "WARN",
    highest_severity: str = "WARN",
    category: str | None = None,
    reason: str = "temporary timeout",
    item_count: int = 1,
) -> Path:
    path = root / f"notification_delivery_failure_classification_{as_of.isoformat()}.json"
    categories = _empty_categories()
    if category is not None:
        record = categories[category]
        record["count"] = item_count
        record["items"] = [
            {
                "source_audit_status": "PASS_WITH_WARNINGS",
                "source_audit_path": "audit.json",
                "reason": reason,
                "details": [reason],
            }
            for _ in range(item_count)
        ]
    total_findings = sum(int(record["count"]) for record in categories.values())
    payload = {
        "schema_version": "1.0",
        "report_type": "notification_delivery_failure_classification",
        "task_id": "TRADING-036",
        "date": as_of.isoformat(),
        "mode": "read_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_delivery_failure_classification_only": True,
        "read_only": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "external_delivery_executed": False,
        "retry_executed": False,
        "delivery_state_mutated": False,
        "production_parameters_modified": False,
        "metadata": {
            "task_id": "TRADING-036",
            "generated_at": "2026-05-26T00:00:00Z",
            "mode": "read_only",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "source_audit": {
            "task_id": "TRADING-035",
            "audit_summary_path": "audit.json",
            "audit_status": "PASS" if category is None else "PASS_WITH_WARNINGS",
            "source_available": True,
            "source_parse_status": "OK",
        },
        "classification_summary": {
            "overall_status": overall_status,
            "total_findings": total_findings,
            "total_failures": total_findings,
            "highest_severity": highest_severity,
            "requires_manual_review": category is not None
            and category != "TRANSIENT_DELIVERY_FAILURE",
            "safe_to_retry": category == "TRANSIENT_DELIVERY_FAILURE",
            "blocks_notification_chain": category is not None,
        },
        "failure_categories": categories,
        "retry_readiness": {
            "safe_to_retry": category == "TRANSIENT_DELIVERY_FAILURE",
            "retry_mode": (
                "READY_FOR_RETRY" if category == "TRANSIENT_DELIVERY_FAILURE" else "NOT_APPLICABLE"
            ),
            "retry_candidates": [category] if category == "TRANSIENT_DELIVERY_FAILURE" else [],
            "retry_blockers": [] if category == "TRANSIENT_DELIVERY_FAILURE" else [reason],
            "manual_review_required_for": (
                [] if category in (None, "TRANSIENT_DELIVERY_FAILURE") else [category]
            ),
        },
        "recommended_actions": ["Review source classification."],
        "safety_invariants": {
            "read_only": True,
            "no_external_delivery": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "classification_json": {"path": str(path)},
            "classification_markdown": {"path": str(path.with_suffix(".md"))},
            "run_log": {"path": str(path.with_suffix(".log"))},
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    path.with_suffix(".md").write_text(
        "# Notification Delivery Failure Classification\n",
        encoding="utf-8",
    )
    path.with_suffix(".log").write_text("overall_status=PASS\n", encoding="utf-8")
    return path


def _empty_categories() -> dict[str, dict[str, Any]]:
    return {
        "TRANSIENT_DELIVERY_FAILURE": {
            "count": 0,
            "severity": "WARN",
            "retryable": True,
            "requires_manual_review": False,
            "items": [],
        },
        "CONFIGURATION_FAILURE": {
            "count": 0,
            "severity": "ERROR",
            "retryable": False,
            "requires_manual_review": True,
            "items": [],
        },
        "SAFETY_BLOCKED": {
            "count": 0,
            "severity": "CRITICAL",
            "retryable": False,
            "requires_manual_review": True,
            "items": [],
        },
        "CONTENT_MISMATCH": {
            "count": 0,
            "severity": "ERROR",
            "retryable": False,
            "requires_manual_review": True,
            "items": [],
        },
        "MISSING_ARTIFACT": {
            "count": 0,
            "severity": "ERROR",
            "retryable": False,
            "requires_manual_review": True,
            "items": [],
        },
        "UNKNOWN": {
            "count": 0,
            "severity": "WARN",
            "retryable": False,
            "requires_manual_review": True,
            "items": [],
        },
    }


def _write_queue_artifact(tmp_path: Path, as_of: date) -> dict[str, Any]:
    root = tmp_path / "outputs" / "retry_candidate_queue"
    json_path = root / f"retry_candidate_queue_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    payload = {
        "schema_version": "1.0",
        "report_type": "retry_candidate_queue",
        "task_id": "TRADING-037",
        "date": as_of.isoformat(),
        "mode": "read_only",
        "production_effect": "none",
        "manual_review_only": True,
        "retry_candidate_queue_only": True,
        "read_only": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "external_delivery_executed": False,
        "retry_executed": False,
        "delivery_state_mutated": False,
        "approval_state_modified": False,
        "metadata": {
            "task_id": "TRADING-037",
            "generated_at": "2026-05-26T00:00:00Z",
            "mode": "read_only",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "source_classification": {
            "task_id": "TRADING-036",
            "classification_report_path": "classification.json",
            "overall_status": "WARN",
            "highest_severity": "WARN",
            "source_available": True,
            "source_parse_status": "OK",
        },
        "queue_summary": {
            "queue_status": "PENDING_APPROVAL",
            "total_candidates": 1,
            "approved_candidates": 0,
            "blocked_candidates": 0,
            "manual_review_required": True,
            "has_retryable_candidates": True,
            "safe_to_execute_retry": False,
        },
        "candidate_queue": [
            {
                "candidate_id": "retry_candidate_2026-05-26_001",
                "source_category": "TRANSIENT_DELIVERY_FAILURE",
                "source_item_id": "delivery_failure_001",
                "retry_status": "PENDING_APPROVAL",
            }
        ],
        "blocked_items": [],
        "approval_gate": {
            "approval_required": True,
            "approval_status": "NOT_REQUESTED",
            "approved_by": None,
            "approved_at": None,
            "approval_note": None,
            "retry_execution_allowed": False,
        },
        "recommended_actions": ["Review candidate."],
        "safety_invariants": {
            "read_only": True,
            "no_external_delivery": True,
            "no_retry_execution": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "retry_candidate_queue_json": {"path": str(json_path)},
            "retry_candidate_queue_markdown": {"path": str(markdown_path)},
            "run_log": {"path": str(log_path)},
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text("# Retry Candidate Queue / Manual Approval Gate\n", encoding="utf-8")
    log_path.write_text("queue_status=PENDING_APPROVAL\n", encoding="utf-8")
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


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["retry_candidate_queue_only"] is True
    assert payload["read_only"] is True
    assert payload["email_sent"] is False
    assert payload["gmail_draft_created"] is False
    assert payload["gmail_draft_modified"] is False
    assert payload["slack_sent"] is False
    assert payload["discord_sent"] is False
    assert payload["webhook_called"] is False
    assert payload["mobile_push_sent"] is False
    assert payload["external_delivery_executed"] is False
    assert payload["retry_executed"] is False
    assert payload["delivery_state_mutated"] is False
    assert payload["approval_state_modified"] is False
    assert payload["production_parameters_modified"] is False
    assert payload["notification_delivery_failure_classification_executed_by_queue"] is False
    assert payload["notification_delivery_audit_executed_by_queue"] is False
    assert payload["notification_draft_executed_by_queue"] is False
    assert payload["delivery_preflight_executed_by_queue"] is False
    assert payload["draft_dispatch_executed_by_queue"] is False
    assert payload["operator_brief_executed_by_queue"] is False
    assert payload["pipelines_executed_by_queue"] is False
    assert payload["data_downloaded_by_queue"] is False
    assert payload["apply_executed_by_queue"] is False
    assert payload["rollback_executed_by_queue"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 26, tzinfo=UTC)
