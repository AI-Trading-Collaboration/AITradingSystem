from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.retry_execution_dry_run import (
    render_retry_execution_dry_run_markdown,
    should_fail_cli,
    write_retry_execution_dry_run,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_retry_execution_dry_run.py"


def test_missing_queue_report_is_source_queue_unavailable(tmp_path: Path) -> None:
    source = tmp_path / "retry_candidate_queue_2026-05-26.json"

    payload = _run_dry_run(tmp_path, source)

    assert payload["source_queue"]["source_available"] is False
    assert payload["source_queue"]["source_parse_status"] == "MISSING"
    assert payload["dry_run_summary"]["dry_run_status"] == "SOURCE_QUEUE_UNAVAILABLE"
    assert payload["dry_run_summary"]["real_retry_allowed"] is False
    assert payload["simulated_retry_actions"] == []


def test_malformed_queue_json_is_source_queue_unavailable(tmp_path: Path) -> None:
    source = tmp_path / "retry_candidate_queue_2026-05-26.json"
    source.write_text("{not-json", encoding="utf-8")

    payload = _run_dry_run(tmp_path, source)

    assert payload["source_queue"]["source_available"] is True
    assert payload["source_queue"]["source_parse_status"] == "MALFORMED_JSON"
    assert payload["dry_run_summary"]["dry_run_status"] == "SOURCE_QUEUE_UNAVAILABLE"
    assert payload["simulated_retry_actions"] == []


def test_empty_queue_has_nothing_to_dry_run(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path, queue_status="EMPTY", candidate_count=0)

    payload = _run_dry_run(tmp_path, source)

    assert payload["source_queue"]["queue_status"] == "EMPTY"
    assert payload["dry_run_summary"]["dry_run_status"] == "NOTHING_TO_DRY_RUN"
    assert payload["dry_run_summary"]["total_candidates"] == 0
    assert payload["simulated_retry_actions"] == []


def test_pending_queue_without_approval_waits_for_manual_approval(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path)

    payload = _run_dry_run(tmp_path, source)

    assert payload["dry_run_summary"]["dry_run_status"] == "WAITING_FOR_MANUAL_APPROVAL"
    assert payload["approval_record"]["approval_record_available"] is False
    assert payload["approval_record"]["approved_candidate_count"] == 0
    assert payload["approval_record"]["unapproved_candidate_count"] == 1
    assert payload["dry_run_summary"]["approved_for_dry_run"] == 0
    assert payload["simulated_retry_actions"] == []


def test_valid_approval_record_generates_ready_dry_run_action(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path)
    approval = _write_approval_record(
        tmp_path,
        approved_candidate_ids=["retry_candidate_2026-05-26_001"],
    )

    payload = _run_dry_run(tmp_path, source, approval)
    markdown = render_retry_execution_dry_run_markdown(payload)

    _assert_safety_invariants(payload)
    assert payload["metadata"]["mode"] == "dry_run_only"
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_only"] is True
    assert payload["dry_run_summary"]["dry_run_status"] == "READY_FOR_DRY_RUN"
    assert payload["dry_run_summary"]["approved_for_dry_run"] == 1
    assert payload["dry_run_summary"]["simulated_retry_actions"] == 1
    action = payload["simulated_retry_actions"][0]
    assert action["candidate_id"] == "retry_candidate_2026-05-26_001"
    assert action["dry_run_action_id"] == "dry_run_retry_2026-05-26_001"
    assert action["would_retry"] is True
    assert action["actual_retry_executed"] is False
    assert action["external_delivery_executed"] is False
    assert action["state_mutation_executed"] is False
    assert "READY_FOR_DRY_RUN" in markdown
    assert "## Approval Record" in markdown
    assert "## Simulated Retry Actions" in markdown


def test_approval_candidate_id_mismatch_blocks_dry_run(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path)
    approval = _write_approval_record(
        tmp_path,
        approved_candidate_ids=["retry_candidate_2026-05-26_999"],
    )

    payload = _run_dry_run(tmp_path, source, approval)

    assert payload["dry_run_summary"]["dry_run_status"] == "APPROVAL_MISMATCH"
    assert payload["dry_run_summary"]["blocked_from_dry_run"] > 0
    assert payload["simulated_retry_actions"] == []
    assert payload["dry_run_summary"]["real_retry_allowed"] is False
    assert should_fail_cli(payload, fail_on_approval_mismatch=True) is True


def test_rejected_candidate_becomes_blocked_item(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path)
    approval = _write_approval_record(
        tmp_path,
        rejected_candidate_ids=["retry_candidate_2026-05-26_001"],
    )

    payload = _run_dry_run(tmp_path, source, approval)

    assert payload["dry_run_summary"]["dry_run_status"] == "WAITING_FOR_MANUAL_APPROVAL"
    assert payload["dry_run_summary"]["simulated_retry_actions"] == 0
    assert payload["simulated_retry_actions"] == []
    blocked = payload["blocked_items"][0]
    assert blocked["candidate_id"] == "retry_candidate_2026-05-26_001"
    assert blocked["approval_status"] == "REJECTED"


def test_safety_blocked_queue_blocks_retry_dry_run(tmp_path: Path) -> None:
    source = _write_queue_report(
        tmp_path,
        queue_status="SAFETY_BLOCKED",
        candidate_count=0,
        blocked_item_count=1,
    )

    payload = _run_dry_run(tmp_path, source)
    markdown = render_retry_execution_dry_run_markdown(payload)

    assert payload["dry_run_summary"]["dry_run_status"] == "SAFETY_BLOCKED"
    assert payload["simulated_retry_actions"] == []
    assert should_fail_cli(payload, fail_on_safety_blocked=True) is True
    assert "CRITICAL: Safety blocked. Retry dry-run is not allowed." in markdown


def test_dry_run_action_id_generation_is_deterministic(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path, candidate_count=2)
    approval = _write_approval_record(
        tmp_path,
        approved_candidate_ids=[
            "retry_candidate_2026-05-26_001",
            "retry_candidate_2026-05-26_002",
        ],
    )

    first = _run_dry_run(tmp_path, source, approval)
    second = _run_dry_run(tmp_path, source, approval)

    assert [item["dry_run_action_id"] for item in first["simulated_retry_actions"]] == [
        "dry_run_retry_2026-05-26_001",
        "dry_run_retry_2026-05-26_002",
    ]
    assert first["simulated_retry_actions"] == second["simulated_retry_actions"]


def test_cli_writes_json_markdown_and_log_with_explicit_inputs(tmp_path: Path) -> None:
    source = _write_queue_report(tmp_path)
    approval = _write_approval_record(
        tmp_path,
        approved_candidate_ids=["retry_candidate_2026-05-26_001"],
    )
    output_dir = tmp_path / "dry-run"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--queue-report",
            str(source),
            "--approval-record",
            str(approval),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    json_path = output_dir / "retry_execution_dry_run_2026-05-26.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    assert json_path.exists()
    assert markdown_path.exists()
    assert log_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["source_queue"]["queue_report_path"] == str(source)
    assert payload["approval_record"]["approval_record_path"] == str(approval)
    assert "Retry Execution Dry Run: READY_FOR_DRY_RUN" in result.stdout


def test_cli_fail_flags_return_non_zero(tmp_path: Path) -> None:
    safety_source = _write_queue_report(
        tmp_path / "safety",
        queue_status="SAFETY_BLOCKED",
        candidate_count=0,
        blocked_item_count=1,
    )
    mismatch_source = _write_queue_report(tmp_path / "mismatch")
    mismatch_approval = _write_approval_record(
        tmp_path / "mismatch",
        approved_candidate_ids=["retry_candidate_2026-05-26_999"],
    )

    safety_result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--queue-report",
            str(safety_source),
            "--output-dir",
            str(tmp_path / "safety-output"),
            "--fail-on-safety-blocked",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    mismatch_result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--queue-report",
            str(mismatch_source),
            "--approval-record",
            str(mismatch_approval),
            "--output-dir",
            str(tmp_path / "mismatch-output"),
            "--fail-on-approval-mismatch",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert safety_result.returncode == 2
    assert mismatch_result.returncode == 2


def _run_dry_run(
    tmp_path: Path,
    source: Path,
    approval: Path | None = None,
) -> dict[str, Any]:
    return write_retry_execution_dry_run(
        queue_report_path=source,
        approval_record_path=approval,
        output_dir=tmp_path / "dry-run-output",
        generated_at=_fixed_generated_at(),
    )


def _write_queue_report(
    root: Path,
    *,
    as_of: date = date(2026, 5, 26),
    queue_status: str = "PENDING_APPROVAL",
    candidate_count: int = 1,
    blocked_item_count: int = 0,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"retry_candidate_queue_{as_of.isoformat()}.json"
    candidates = [
        {
            "candidate_id": f"retry_candidate_{as_of.isoformat()}_{index:03d}",
            "source_category": "TRANSIENT_DELIVERY_FAILURE",
            "source_item_id": f"delivery_failure_{index:03d}",
            "severity": "WARN",
            "retryable": True,
            "requires_manual_review": True,
            "approval_required": True,
            "retry_status": "PENDING_APPROVAL",
            "retry_reason": "temporary smtp timeout",
            "retry_blockers": [],
            "source_evidence": {
                "classification_report_path": "classification.json",
                "failure_category": "TRANSIENT_DELIVERY_FAILURE",
                "source_audit_status": "PASS_WITH_WARNINGS",
            },
        }
        for index in range(1, candidate_count + 1)
    ]
    blocked_items = [
        {
            "blocked_item_id": f"retry_blocked_{as_of.isoformat()}_{index:03d}",
            "source_category": "SAFETY_BLOCKED",
            "retry_status": "BLOCKED",
            "block_reason": "Safety blocked by source queue.",
        }
        for index in range(1, blocked_item_count + 1)
    ]
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
            "queue_status": queue_status,
            "total_candidates": candidate_count,
            "approved_candidates": 0,
            "blocked_candidates": blocked_item_count,
            "manual_review_required": queue_status != "EMPTY",
            "has_retryable_candidates": candidate_count > 0,
            "safe_to_execute_retry": False,
        },
        "candidate_queue": candidates,
        "blocked_items": blocked_items,
        "approval_gate": {
            "approval_required": True,
            "approval_status": "NOT_REQUESTED",
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
            "retry_candidate_queue_json": {"path": str(path)},
            "retry_candidate_queue_markdown": {"path": str(path.with_suffix(".md"))},
            "run_log": {"path": str(path.with_suffix(".log"))},
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    path.with_suffix(".md").write_text("# Retry Candidate Queue\n", encoding="utf-8")
    path.with_suffix(".log").write_text("queue_status=PENDING_APPROVAL\n", encoding="utf-8")
    return path


def _write_approval_record(
    root: Path,
    *,
    as_of: date = date(2026, 5, 26),
    approved_candidate_ids: list[str] | None = None,
    rejected_candidate_ids: list[str] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"manual_retry_approval_{as_of.isoformat()}.json"
    approved = approved_candidate_ids or []
    rejected = rejected_candidate_ids or []
    payload = {
        "metadata": {
            "task_id": "TRADING-038",
            "approval_record_type": "manual_retry_approval",
            "created_by": "manual_operator",
            "created_at": "2026-05-26T09:00:00+09:00",
            "schema_version": "1.0",
        },
        "approval_scope": {
            "source_task_id": "TRADING-037",
            "source_queue_path": (
                "outputs/retry_candidate_queue/retry_candidate_queue_2026-05-26.json"
            ),
        },
        "approved_candidates": [
            {
                "candidate_id": candidate_id,
                "approval_status": "APPROVED_FOR_DRY_RUN",
                "approval_note": "Approved for dry-run only.",
            }
            for candidate_id in approved
        ],
        "rejected_candidates": [
            {
                "candidate_id": candidate_id,
                "approval_status": "REJECTED",
                "rejection_reason": "Requires configuration review before retry.",
            }
            for candidate_id in rejected
        ],
        "safety_constraints": {
            "dry_run_only": True,
            "real_retry_allowed": False,
            "external_delivery_allowed": False,
            "production_state_mutation_allowed": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["retry_execution_dry_run_only"] is True
    assert payload["dry_run_only"] is True
    assert payload["read_only"] is True
    assert payload["approval_record_modified"] is False
    assert payload["approval_state_modified"] is False
    assert payload["retry_executed"] is False
    assert payload["actual_retry_executed"] is False
    assert payload["external_delivery_executed"] is False
    assert payload["delivery_state_mutated"] is False
    assert payload["state_mutation_executed"] is False
    assert payload["production_parameters_modified"] is False
    assert payload["safety_invariants"] == {
        "dry_run_only": True,
        "no_external_delivery": True,
        "no_retry_execution": True,
        "no_state_mutation": True,
        "no_production_parameter_change": True,
        "approval_record_is_input_only": True,
        "dashboard_read_only": True,
    }


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 26, tzinfo=UTC)
