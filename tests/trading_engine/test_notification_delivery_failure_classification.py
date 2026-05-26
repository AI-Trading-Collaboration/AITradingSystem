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
from ai_trading_system.trading_engine.notification_delivery_failure_classification import (
    render_notification_delivery_failure_classification_markdown,
    should_fail_cli,
    write_notification_delivery_failure_classification,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_notification_delivery_failure_classification.py"


def test_pass_audit_summary_generates_pass_classification(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PASS")

    payload = _run_classifier(tmp_path, source)

    _assert_safety_invariants(payload)
    assert payload["source_audit"]["audit_status"] == "PASS"
    assert payload["classification_summary"] == {
        "overall_status": "PASS",
        "total_findings": 0,
        "total_failures": 0,
        "highest_severity": "NONE",
        "requires_manual_review": False,
        "safe_to_retry": False,
        "blocks_notification_chain": False,
    }
    assert payload["retry_readiness"]["retry_mode"] == "NOT_APPLICABLE"


def test_incomplete_audit_summary_maps_to_missing_artifact(tmp_path: Path) -> None:
    source = _write_audit_summary(
        tmp_path,
        "INCOMPLETE",
        artifact_chain={"blocking_reasons": ["dispatch artifact missing"], "warnings": []},
        input_artifacts={
            "dispatch_artifact": {
                "status": "MISSING",
                "path": "missing.json",
                "error": "JSON input not found",
            }
        },
    )

    payload = _run_classifier(tmp_path, source)

    _assert_safety_invariants(payload)
    assert payload["classification_summary"]["overall_status"] == "ERROR"
    assert payload["classification_summary"]["requires_manual_review"] is True
    assert payload["classification_summary"]["safe_to_retry"] is False
    assert payload["classification_summary"]["blocks_notification_chain"] is True
    assert payload["failure_categories"]["MISSING_ARTIFACT"]["count"] == 1
    assert payload["failure_categories"]["CONFIGURATION_FAILURE"]["count"] == 0


def test_incomplete_channel_config_maps_to_configuration_failure(tmp_path: Path) -> None:
    source = _write_audit_summary(
        tmp_path,
        "INCOMPLETE",
        alerts={"critical": [], "warnings": ["channel config missing for notification target"]},
    )

    payload = _run_classifier(tmp_path, source)

    assert payload["failure_categories"]["CONFIGURATION_FAILURE"]["count"] == 1
    assert payload["failure_categories"]["MISSING_ARTIFACT"]["count"] == 0
    assert payload["retry_readiness"]["safe_to_retry"] is False


def test_mismatch_audit_summary_maps_to_content_mismatch(tmp_path: Path) -> None:
    source = _write_audit_summary(
        tmp_path,
        "MISMATCH",
        artifact_chain={"blocking_reasons": ["draft hash mismatch"], "warnings": []},
    )

    payload = _run_classifier(tmp_path, source)

    assert payload["classification_summary"]["overall_status"] == "ERROR"
    assert payload["failure_categories"]["CONTENT_MISMATCH"]["count"] == 1
    assert payload["retry_readiness"]["manual_review_required_for"] == ["CONTENT_MISMATCH"]


def test_safety_blocked_audit_summary_maps_to_critical(tmp_path: Path) -> None:
    source = _write_audit_summary(
        tmp_path,
        "SAFETY_BLOCKED",
        safety_validation={"blocking_reasons": ["webhook_called=true"]},
    )

    payload = _run_classifier(tmp_path, source)
    markdown = render_notification_delivery_failure_classification_markdown(payload)

    assert payload["classification_summary"]["overall_status"] == "CRITICAL"
    assert payload["classification_summary"]["highest_severity"] == "CRITICAL"
    assert payload["failure_categories"]["SAFETY_BLOCKED"]["count"] == 1
    assert payload["retry_readiness"]["retry_mode"] == "SAFETY_BLOCKED"
    assert should_fail_cli(payload, fail_on_critical=True) is True
    assert "CRITICAL: Safety blocked. Do not retry or deliver." in markdown
    assert "SAFETY BLOCKED: manual review required before any retry or delivery action." in markdown


def test_missing_source_audit_artifact_maps_to_missing_artifact(tmp_path: Path) -> None:
    source = tmp_path / "missing_audit.json"

    payload = _run_classifier(tmp_path, source)

    assert payload["source_audit"]["source_available"] is False
    assert payload["source_audit"]["source_parse_status"] == "MISSING"
    assert payload["classification_summary"]["overall_status"] == "ERROR"
    assert payload["failure_categories"]["MISSING_ARTIFACT"]["count"] == 1
    assert payload["classification_summary"]["requires_manual_review"] is True


def test_malformed_source_json_maps_to_unknown_manual_review(tmp_path: Path) -> None:
    source = tmp_path / "notification_delivery_audit_summary_2026-05-26.json"
    source.write_text("{not-json", encoding="utf-8")

    payload = _run_classifier(tmp_path, source)

    assert payload["source_audit"]["source_available"] is True
    assert payload["source_audit"]["source_parse_status"] == "MALFORMED_JSON"
    assert payload["classification_summary"]["overall_status"] == "UNKNOWN"
    assert payload["failure_categories"]["UNKNOWN"]["count"] == 1
    assert payload["classification_summary"]["requires_manual_review"] is True


def test_unknown_audit_status_maps_to_unknown(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PARTIAL")

    payload = _run_classifier(tmp_path, source)

    assert payload["classification_summary"]["overall_status"] == "UNKNOWN"
    assert payload["classification_summary"]["highest_severity"] == "WARN"
    assert payload["failure_categories"]["UNKNOWN"]["count"] == 1
    assert payload["classification_summary"]["blocks_notification_chain"] is True


def test_markdown_contains_pass_banner_and_retry_section(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PASS")

    payload = _run_classifier(tmp_path, source)
    markdown = Path(payload["output_artifacts"]["classification_markdown"]["path"]).read_text(
        encoding="utf-8"
    )

    assert "PASS: No notification delivery failures detected." in markdown
    assert "## Retry Readiness" in markdown
    assert "## Safety Invariants" in markdown


def test_json_contains_required_metadata_and_safety_invariants(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PASS")

    payload = _run_classifier(tmp_path, source)

    _assert_safety_invariants(payload)
    assert payload["metadata"]["mode"] == "read_only"
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_only"] is True
    assert payload["safety_invariants"] == {
        "read_only": True,
        "no_external_delivery": True,
        "no_state_mutation": True,
        "no_production_parameter_change": True,
        "dashboard_read_only": True,
    }


def test_default_latest_source_reads_trading_035_data_path(tmp_path: Path) -> None:
    audit_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "notifications" / "delivery_audit"
    )
    older = _write_audit_summary(
        audit_root,
        "MISMATCH",
        as_of=date(2026, 5, 25),
        artifact_chain={"blocking_reasons": ["old mismatch"], "warnings": []},
    )
    latest = _write_audit_summary(audit_root, "PASS", as_of=date(2026, 5, 26))

    payload = write_notification_delivery_failure_classification(
        project_root=tmp_path,
        output_dir=tmp_path / "out",
        generated_at=_fixed_generated_at(),
    )

    assert older.exists()
    assert payload["source_audit"]["audit_summary_path"] == str(latest)
    assert payload["classification_summary"]["overall_status"] == "PASS"


def test_cli_writes_json_markdown_and_log_with_explicit_audit_summary(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PASS")
    output_dir = tmp_path / "classification"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--audit-summary",
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
    json_path = output_dir / "notification_delivery_failure_classification_2026-05-26.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    assert json_path.exists()
    assert markdown_path.exists()
    assert log_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["source_audit"]["audit_summary_path"] == str(source)
    assert "Notification Delivery Failure Classification: PASS" in result.stdout


def test_cli_supports_custom_output_dir_and_as_of_date(tmp_path: Path) -> None:
    source = _write_audit_summary(tmp_path, "PASS", as_of=date(2026, 5, 25))
    output_dir = tmp_path / "custom-output"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--audit-summary",
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
    assert (output_dir / "notification_delivery_failure_classification_2026-05-26.json").exists()


def test_cli_fail_on_critical_returns_non_zero(tmp_path: Path) -> None:
    source = _write_audit_summary(
        tmp_path,
        "SAFETY_BLOCKED",
        safety_validation={"blocking_reasons": ["external delivery side effect"]},
    )
    output_dir = tmp_path / "classification"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--audit-summary",
            str(source),
            "--output-dir",
            str(output_dir),
            "--fail-on-critical",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert (output_dir / "notification_delivery_failure_classification_2026-05-26.json").exists()


def test_dashboard_reads_existing_classification_json_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 26)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)
    classification = _write_classification_artifact(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
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
            raise AssertionError(f"dashboard must not import classifier or delivery path: {name}")
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

    summary = payload["notification_delivery_failure_classification"]
    assert summary["overall_status"] == "ERROR"
    assert summary["highest_severity"] == "ERROR"
    assert summary["total_failures"] == 1
    assert summary["requires_manual_review"] is True
    assert summary["safe_to_retry"] is False
    assert summary["blocks_notification_chain"] is True
    assert summary["source_audit_status"] == "MISMATCH"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["read_only"] is True
    assert summary["notification_delivery_failure_classification_only"] is True
    assert "Notification Delivery Failure Classification" in html
    assert "overall_status" in html
    assert classification["output_artifacts"]["classification_markdown"]["path"] in html


def test_dashboard_handles_missing_classification_report_gracefully(tmp_path: Path) -> None:
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

    summary = payload["notification_delivery_failure_classification"]
    assert summary["exists"] is False
    assert summary["overall_status"] == "MISSING"
    assert summary["safe_to_retry"] is False
    assert "No notification delivery failure classification report available." in html


def _run_classifier(tmp_path: Path, source: Path) -> dict[str, Any]:
    return write_notification_delivery_failure_classification(
        audit_summary_path=source,
        output_dir=tmp_path / "classification",
        generated_at=_fixed_generated_at(),
    )


def _write_audit_summary(
    root: Path,
    status: str,
    *,
    as_of: date = date(2026, 5, 26),
    alerts: dict[str, list[str]] | None = None,
    artifact_chain: dict[str, list[str]] | None = None,
    safety_validation: dict[str, list[str]] | None = None,
    input_artifacts: dict[str, Any] | None = None,
) -> Path:
    path = root / f"notification_delivery_audit_summary_{as_of.isoformat()}.json"
    payload = {
        "schema_version": "1.0",
        "report_type": "notification_delivery_audit_summary",
        "task_id": "TRADING-035",
        "date": as_of.isoformat(),
        "mode": "notification_delivery_audit_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_delivery_audit_only": True,
        "read_only": True,
        "audit_status": status,
        "notification_lifecycle_status": "DRAFT_READY" if status == "PASS" else "UNKNOWN",
        "artifact_chain": artifact_chain or {"blocking_reasons": [], "warnings": []},
        "safety_validation": safety_validation or {"blocking_reasons": []},
        "external_side_effect_audit": {"status": "PASS", "blocking_reasons": []},
        "alerts": alerts or {"critical": [], "warnings": [], "notes": []},
        "input_artifacts": input_artifacts or {},
        "audit": {"created_at": "2026-05-26T00:00:00Z"},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_classification_artifact(tmp_path: Path, as_of: date) -> dict[str, Any]:
    root = tmp_path / "outputs" / "notification_delivery_failure_classification"
    json_path = root / f"notification_delivery_failure_classification_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
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
        "retry_executed": False,
        "delivery_state_mutated": False,
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
            "audit_status": "MISMATCH",
            "source_available": True,
            "source_parse_status": "OK",
        },
        "classification_summary": {
            "overall_status": "ERROR",
            "total_findings": 1,
            "total_failures": 1,
            "highest_severity": "ERROR",
            "requires_manual_review": True,
            "safe_to_retry": False,
            "blocks_notification_chain": True,
        },
        "failure_categories": {},
        "retry_readiness": {
            "safe_to_retry": False,
            "retry_mode": "BLOCKED_PENDING_MANUAL_REVIEW",
            "retry_candidates": [],
            "retry_blockers": ["draft hash mismatch"],
            "manual_review_required_for": ["CONTENT_MISMATCH"],
        },
        "recommended_actions": ["Review mismatch."],
        "safety_invariants": {
            "read_only": True,
            "no_external_delivery": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "classification_json": {"path": str(json_path)},
            "classification_markdown": {"path": str(markdown_path)},
            "run_log": {"path": str(log_path)},
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text("# Notification Delivery Failure Classification\n", encoding="utf-8")
    log_path.write_text("overall_status=ERROR\n", encoding="utf-8")
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
    assert payload["notification_delivery_failure_classification_only"] is True
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
    assert payload["production_parameters_modified"] is False
    assert payload["notification_delivery_audit_executed_by_classifier"] is False
    assert payload["notification_draft_executed_by_classifier"] is False
    assert payload["delivery_preflight_executed_by_classifier"] is False
    assert payload["draft_dispatch_executed_by_classifier"] is False
    assert payload["operator_brief_executed_by_classifier"] is False
    assert payload["pipelines_executed_by_classifier"] is False
    assert payload["data_downloaded_by_classifier"] is False
    assert payload["apply_executed_by_classifier"] is False
    assert payload["rollback_executed_by_classifier"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 26, tzinfo=UTC)
