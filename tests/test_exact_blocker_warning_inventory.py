from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from test_recovery_blocker_resolution_batch import (
    _batch_source_payloads,
    _combined_report_index_payload,
    _owner_log_payload,
)
from test_recovery_triage_reports import RUN_DATE
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.exact_blocker_warning_inventory import (
    build_exact_blocker_warning_inventory_payload,
    validate_exact_blocker_warning_inventory_payload,
)
from ai_trading_system.reports.normal_paper_shadow_observation_clock import (
    build_normal_paper_shadow_observation_clock_payload,
)
from ai_trading_system.reports.post_recovery_governance_pack import (
    build_post_recovery_governance_pack_payload,
)
from ai_trading_system.reports.remaining_blocker_resolution_ledger import (
    build_remaining_blocker_resolution_ledger_payload,
)
from ai_trading_system.reports.report_index_warning_cleanup import (
    build_report_index_warning_cleanup_payload,
)


def test_exact_blocker_warning_inventory_is_non_aggregated(tmp_path: Path) -> None:
    source = _exact_inventory_sources(tmp_path)

    payload = build_exact_blocker_warning_inventory_payload(
        as_of=RUN_DATE,
        blocker_triage_payload=source["blocker_triage"],
        post_recovery_pack_payload=source["post_recovery_pack"],
        source_depth_audit_payload=source["source_depth_audit"],
        report_index_warning_triage_payload=source["warning_triage"],
        normal_observation_clock_payload=source["normal_clock"],
        owner_action_map_payload=source["owner_action_map"],
    )
    validation = validate_exact_blocker_warning_inventory_payload(payload)

    assert payload["summary"]["blocker_count"] == 9
    assert payload["summary"]["warning_count"] == 10
    assert payload["summary"]["report_index_warning_count"] == 9
    assert payload["summary"]["normal_paper_shadow_may_resume"] is False
    assert payload["summary"]["extended_shadow_remains_forbidden"] is True
    assert payload["summary"]["live_trading_remains_forbidden"] is True
    assert {row["blocker_kind"] for row in payload["blocker_inventory"]} >= {
        "metric",
        "owner",
        "observation-clock",
        "snapshot",
    }
    assert all(row["source_artifact_path"] for row in payload["blocker_inventory"])
    assert all(row["upstream_artifact_dependency"] for row in payload["blocker_inventory"])
    assert all(row["exact_next_action_required"] for row in payload["blocker_inventory"])
    assert all(row["blocks_normal_paper_shadow"] is True for row in payload["blocker_inventory"])
    assert all(row["blocks_extended_shadow"] is True for row in payload["blocker_inventory"])
    assert all(row["blocks_live_trading"] is False for row in payload["blocker_inventory"])
    assert {row["warning_scope"] for row in payload["warning_inventory"]} >= {
        "data-quality",
        "owner-review",
        "report-index",
    }
    assert all(row["waivable"] is False for row in payload["warning_inventory"])
    assert any(row["needs_data_regeneration"] for row in payload["warning_inventory"])
    assert any(row["needs_owner_review"] for row in payload["warning_inventory"])
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"


def test_exact_blocker_warning_inventory_accepts_reduced_current_source_counts(
    tmp_path: Path,
) -> None:
    source = _exact_inventory_sources(tmp_path)
    post_recovery_pack = deepcopy(source["post_recovery_pack"])
    post_recovery_pack["remaining_blockers"] = post_recovery_pack["remaining_blockers"][:-1]
    post_recovery_pack["remaining_warnings"] = post_recovery_pack["remaining_warnings"][:-1]
    post_recovery_pack["summary"]["remaining_blocker_count"] = 8
    post_recovery_pack["summary"]["remaining_warning_count"] = 9

    payload = build_exact_blocker_warning_inventory_payload(
        as_of=RUN_DATE,
        blocker_triage_payload=source["blocker_triage"],
        post_recovery_pack_payload=post_recovery_pack,
        source_depth_audit_payload=source["source_depth_audit"],
        report_index_warning_triage_payload=source["warning_triage"],
        normal_observation_clock_payload=source["normal_clock"],
        owner_action_map_payload=source["owner_action_map"],
    )
    validation = validate_exact_blocker_warning_inventory_payload(payload)

    assert payload["summary"]["blocker_count"] == 8
    assert payload["summary"]["warning_count"] == 9
    assert payload["summary"]["source_post_recovery_blocker_count"] == 8
    assert payload["summary"]["source_post_recovery_warning_count"] == 9
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"
    checks = {row["check_id"]: row["status"] for row in validation["checks"]}
    assert checks["source_recovery_counts_match"] == "PASS"


def test_exact_blocker_warning_inventory_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    source = _exact_inventory_sources(tmp_path)
    _write_json(reports_dir / "recovery_blocker_triage_2026-06-17.json", source["blocker_triage"])
    _write_json(
        reports_dir / "post_recovery_governance_pack_2026-06-17.json",
        source["post_recovery_pack"],
    )
    _write_json(
        reports_dir / "recovery_pack_source_depth_audit_2026-06-17.json",
        source["source_depth_audit"],
    )
    _write_json(
        reports_dir / "report_index_warning_triage_2026-06-17.json",
        source["warning_triage"],
    )
    _write_json(
        reports_dir / "normal_paper_shadow_observation_clock_2026-06-17.json",
        source["normal_clock"],
    )
    _write_json(
        reports_dir / "recovery_owner_action_map_2026-06-17.json",
        source["owner_action_map"],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reports",
            "exact-blocker-warning-inventory",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-exact-blocker-warning-inventory",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation.exit_code == 0, validation.output

    payload = json.loads(
        (reports_dir / "exact_blocker_warning_inventory_2026-06-17.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["summary"]["blocker_count"] == 9
    assert payload["reader_brief"]["key_result"] == "EXACT_INVENTORY_BLOCKED"
    assert (
        reports_dir / "exact_blocker_warning_inventory_validation_2026-06-17.json"
    ).exists()


def _exact_inventory_sources(tmp_path: Path) -> dict[str, dict[str, object]]:
    report_index = _combined_report_index_payload(tmp_path)
    recovery_pack, blocker_triage, warning_triage, source_depth_audit, owner_action_map = (
        _batch_source_payloads(tmp_path, report_index_payload=report_index)
    )
    ledger = build_remaining_blocker_resolution_ledger_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_triage_payload=blocker_triage,
        warning_triage_payload=warning_triage,
        source_depth_audit_payload=source_depth_audit,
        owner_action_map_payload=owner_action_map,
    )
    warning_cleanup = build_report_index_warning_cleanup_payload(
        as_of=RUN_DATE,
        report_index_warning_triage_payload=warning_triage,
        report_index_payload=report_index,
    )
    normal_clock = build_normal_paper_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    post_recovery = build_post_recovery_governance_pack_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        blocker_ledger_payload=ledger,
        warning_cleanup_payload=warning_cleanup,
        normal_observation_clock_payload=normal_clock,
        owner_decision_audit_log_payload=_owner_log_payload(),
    )
    return {
        "blocker_triage": blocker_triage,
        "post_recovery_pack": post_recovery,
        "source_depth_audit": source_depth_audit,
        "warning_triage": warning_triage,
        "normal_clock": normal_clock,
        "owner_action_map": owner_action_map,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
