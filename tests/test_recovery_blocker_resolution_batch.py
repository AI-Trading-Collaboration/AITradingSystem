from __future__ import annotations

import json
from pathlib import Path

from test_recovery_triage_reports import (
    RUN_DATE,
    _blocked_recovery_pack,
    _recovery_report_index_payload,
    _report_index_warning_payload,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.normal_paper_shadow_observation_clock import (
    OBSERVATION_NOT_STARTED,
    build_normal_paper_shadow_observation_clock_payload,
    validate_normal_paper_shadow_observation_clock_payload,
)
from ai_trading_system.reports.post_recovery_governance_pack import (
    POST_RECOVERY_BLOCKED,
    build_post_recovery_governance_pack_payload,
    validate_post_recovery_governance_pack_payload,
)
from ai_trading_system.reports.recovery_triage import (
    PASS_WITH_WARNINGS_STATUS,
    build_recovery_blocker_triage_payload,
    build_recovery_owner_action_map_payload,
    build_recovery_pack_source_depth_audit_payload,
    build_report_index_warning_triage_payload,
)
from ai_trading_system.reports.remaining_blocker_resolution_ledger import (
    build_remaining_blocker_resolution_ledger_payload,
    validate_remaining_blocker_resolution_ledger_payload,
)
from ai_trading_system.reports.report_index_warning_cleanup import (
    build_report_index_warning_cleanup_payload,
    validate_report_index_warning_cleanup_payload,
)


def test_remaining_blocker_resolution_ledger_preserves_all_boundaries(
    tmp_path: Path,
) -> None:
    recovery_pack, blocker_triage, warning_triage, source_audit, action_map = (
        _batch_source_payloads(tmp_path)
    )

    payload = build_remaining_blocker_resolution_ledger_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_triage_payload=blocker_triage,
        warning_triage_payload=warning_triage,
        source_depth_audit_payload=source_audit,
        owner_action_map_payload=action_map,
    )
    validation = validate_remaining_blocker_resolution_ledger_payload(payload)

    assert payload["summary"]["blocker_count"] == 9
    assert payload["summary"]["warning_count"] == 10
    assert payload["summary"]["normal_paper_shadow_may_resume"] is False
    assert payload["summary"]["extended_shadow_remains_forbidden"] is True
    assert payload["summary"]["live_trading_remains_forbidden"] is True
    assert all(row["blocks_live_trading"] is False for row in payload["blocker_resolution_ledger"])
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_report_index_warning_cleanup_keeps_visible_warnings_unwaived(
    tmp_path: Path,
) -> None:
    warning_index = _report_index_warning_payload(tmp_path)
    warning_triage = build_report_index_warning_triage_payload(
        as_of=RUN_DATE,
        report_index_payload=warning_index,
    )

    payload = build_report_index_warning_cleanup_payload(
        as_of=RUN_DATE,
        report_index_warning_triage_payload=warning_triage,
        report_index_payload=warning_index,
    )
    validation = validate_report_index_warning_cleanup_payload(payload)

    assert payload["summary"]["remaining_unwaived_count"] == 9
    assert payload["summary"]["silent_waiver_count"] == 0
    assert payload["summary"]["owner_review_count"] >= 1
    assert all(row["waiver_action"] == "not_applied" for row in payload["warning_cleanup_rows"])
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_normal_observation_clock_does_not_start_when_gate_blocked(
    tmp_path: Path,
) -> None:
    report_index = _combined_report_index_payload(tmp_path)

    payload = build_normal_paper_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_normal_paper_shadow_observation_clock_payload(payload)

    assert payload["normal_observation_clock_status"] == OBSERVATION_NOT_STARTED
    assert payload["summary"]["normal_paper_shadow_may_resume"] is False
    assert payload["summary"]["current_count"] == 0
    assert payload["summary"]["extended_shadow_remains_forbidden"] is True
    assert payload["summary"]["live_trading_remains_forbidden"] is True
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_post_recovery_governance_pack_reports_blocked_state(
    tmp_path: Path,
) -> None:
    recovery_pack, blocker_triage, warning_triage, source_audit, action_map = (
        _batch_source_payloads(tmp_path)
    )
    ledger = build_remaining_blocker_resolution_ledger_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_triage_payload=blocker_triage,
        warning_triage_payload=warning_triage,
        source_depth_audit_payload=source_audit,
        owner_action_map_payload=action_map,
    )
    warning_cleanup = build_report_index_warning_cleanup_payload(
        as_of=RUN_DATE,
        report_index_warning_triage_payload=warning_triage,
        report_index_payload=_combined_report_index_payload(tmp_path),
    )
    normal_clock = build_normal_paper_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_combined_report_index_payload(tmp_path),
        project_root=tmp_path,
    )

    payload = build_post_recovery_governance_pack_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_ledger_payload=ledger,
        warning_cleanup_payload=warning_cleanup,
        normal_observation_clock_payload=normal_clock,
        owner_decision_audit_log_payload=_owner_log_payload(),
    )
    validation = validate_post_recovery_governance_pack_payload(payload)

    assert payload["post_recovery_status"] == POST_RECOVERY_BLOCKED
    assert payload["summary"]["remaining_blocker_count"] == 9
    assert payload["summary"]["normal_paper_shadow_may_resume"] is False
    assert payload["summary"]["extended_shadow_remains_forbidden"] is True
    assert payload["summary"]["live_trading_remains_forbidden"] is True
    assert payload["summary"]["latest_owner_action"] == "hold"
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_recovery_blocker_resolution_cli_writes_outputs(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    report_index = _combined_report_index_payload(tmp_path)
    _write_json(reports_dir / "report_index_2026-06-17.json", report_index)
    recovery_pack, blocker_triage, warning_triage, source_audit, action_map = (
        _batch_source_payloads(tmp_path, report_index_payload=report_index)
    )
    _write_json(
        reports_dir / "research_governance_recovery_pack_2026-06-17.json",
        recovery_pack,
    )
    _write_json(reports_dir / "recovery_blocker_triage_2026-06-17.json", blocker_triage)
    _write_json(reports_dir / "report_index_warning_triage_2026-06-17.json", warning_triage)
    _write_json(
        reports_dir / "recovery_pack_source_depth_audit_2026-06-17.json",
        source_audit,
    )
    _write_json(reports_dir / "recovery_owner_action_map_2026-06-17.json", action_map)

    runner = CliRunner()
    commands = [
        ["reports", "remaining-blocker-resolution-ledger", "--as-of", RUN_DATE.isoformat()],
        ["reports", "validate-remaining-blocker-resolution-ledger", "--latest"],
        ["reports", "report-index-warning-cleanup", "--as-of", RUN_DATE.isoformat()],
        ["reports", "validate-report-index-warning-cleanup", "--latest"],
        [
            "reports",
            "normal-paper-shadow-observation-clock",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
        ],
        ["reports", "validate-normal-paper-shadow-observation-clock", "--latest"],
        ["reports", "post-recovery-governance-pack", "--as-of", RUN_DATE.isoformat()],
        ["reports", "validate-post-recovery-governance-pack", "--latest"],
    ]
    for command in commands:
        result = runner.invoke(app, [*command, "--reports-dir", str(reports_dir)])
        assert result.exit_code == 0, result.output

    post_recovery = json.loads(
        (reports_dir / "post_recovery_governance_pack_2026-06-17.json").read_text(
            encoding="utf-8"
        )
    )
    assert post_recovery["post_recovery_status"] == POST_RECOVERY_BLOCKED
    assert post_recovery["summary"]["live_trading_remains_forbidden"] is True


def _batch_source_payloads(
    tmp_path: Path,
    *,
    report_index_payload: dict[str, object] | None = None,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    report_index = report_index_payload or _combined_report_index_payload(tmp_path)
    recovery_pack = _blocked_recovery_pack(tmp_path, report_index_payload=report_index)
    blocker_triage = build_recovery_blocker_triage_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
    )
    warning_triage = build_report_index_warning_triage_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
    )
    source_audit = build_recovery_pack_source_depth_audit_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    action_map = build_recovery_owner_action_map_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_triage_payload=blocker_triage,
        report_index_warning_triage_payload=warning_triage,
        source_depth_audit_payload=source_audit,
    )
    return recovery_pack, blocker_triage, warning_triage, source_audit, action_map


def _combined_report_index_payload(tmp_path: Path) -> dict[str, object]:
    recovery_index = _recovery_report_index_payload(tmp_path)
    warning_index = _report_index_warning_payload(tmp_path)
    recovery_index["reports"] = [
        *list(recovery_index["reports"]),  # type: ignore[index]
        *list(warning_index["reports"]),  # type: ignore[index]
    ]
    recovery_index["warnings"] = warning_index["warnings"]
    recovery_index["visibility_audit"] = warning_index["visibility_audit"]
    recovery_index["status"] = "PASS_WITH_WARNINGS"
    recovery_index["summary"] = {
        **dict(recovery_index["summary"]),  # type: ignore[arg-type]
        "report_count": len(recovery_index["reports"]),  # type: ignore[arg-type]
        "unwaived_warning_count": 9,
        "stale_count": 9,
    }
    return recovery_index


def _owner_log_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "owner_decision_audit_log",
        "as_of": RUN_DATE.isoformat(),
        "audit_log_status": "AUDIT_LOG_PASS",
        "status": "AUDIT_LOG_PASS",
        "production_effect": "none",
        "summary": {
            "latest_owner_action": "hold",
            "latest_decision_id": "TRADING-413_owner_hold_2026-06-17",
        },
        "input_artifacts": {"owner_decision_audit_log": "data/governance/audit.jsonl"},
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
