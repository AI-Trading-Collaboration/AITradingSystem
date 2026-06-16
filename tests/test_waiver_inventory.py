from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.waiver_inventory import (
    build_waiver_inventory_payload,
    render_waiver_inventory_markdown,
    validate_waiver_inventory_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_waiver_inventory_passes_current_waiver(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    waiver_path = _write_waivers(tmp_path)

    payload = build_waiver_inventory_payload(
        as_of=RUN_DATE,
        waiver_path=waiver_path,
        registry_path=registry_path,
    )
    validation = validate_waiver_inventory_payload(payload)
    markdown = render_waiver_inventory_markdown(payload)

    assert payload["inventory_status"] == "PASS"
    assert payload["summary"]["configured_waiver_count"] == 1
    assert payload["summary"]["active_waiver_count"] == 1
    assert payload["summary"]["expired_waiver_count"] == 0
    assert payload["waivers"][0]["affected_report_registry_entry"] == "optional_missing"
    assert validation["validation_status"] == "PASS"
    assert "Report Index Waiver Inventory" in markdown


def test_waiver_inventory_fails_expired_waiver(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    waiver_path = _write_waivers(tmp_path, expires_at="2026-05-03")

    payload = build_waiver_inventory_payload(
        as_of=RUN_DATE,
        waiver_path=waiver_path,
        registry_path=registry_path,
    )
    validation = validate_waiver_inventory_payload(payload)

    assert payload["inventory_status"] == "FAIL"
    assert payload["summary"]["expired_waiver_count"] == 1
    assert any(issue["issue_id"] == "expired_waiver" for issue in payload["blocking_issues"])
    assert validation["validation_status"] == "FAIL"


def test_waiver_inventory_cli_writes_report_and_validation(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    waiver_path = _write_waivers(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    inventory_json = reports_dir / "report_index_waiver_inventory_2026-05-04.json"
    inventory_md = reports_dir / "report_index_waiver_inventory_2026-05-04.md"
    validation_json = reports_dir / "report_index_waiver_inventory_validation_2026-05-04.json"
    validation_md = reports_dir / "report_index_waiver_inventory_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "waiver-inventory",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(waiver_path),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-waiver-inventory",
            "--source-json-path",
            str(inventory_json),
            "--reports-dir",
            str(reports_dir),
        ],
    )

    assert validation.exit_code == 0, validation.output
    assert inventory_md.exists()
    assert validation_md.exists()
    assert json.loads(inventory_json.read_text(encoding="utf-8"))["status"] == "PASS"
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"]["waiver_inventory"] == str(inventory_json)


def _write_registry(tmp_path: Path) -> Path:
    registry: dict[str, Any] = {
        "schema_version": 1,
        "policy_version": "test_report_registry_v1",
        "policy_metadata": {
            "owner": "test",
            "status": "test",
            "rationale": "test",
            "intended_effect": "test",
            "validation_evidence": "test",
            "review_condition": "test",
        },
        "defaults": {
            "production_effect": "none",
            "missing_status": "MISSING",
            "stale_status": "STALE",
        },
        "reports": [
            {
                "report_id": "optional_missing",
                "title": "Optional Missing",
                "group": "governance",
                "cadence": "daily",
                "audience": "test",
                "owner": "system",
                "command": "test command",
                "artifact_globs": ["outputs/reports/optional_missing_*.json"],
                "freshness_sla_days": 1,
                "freshness_rationale": "test rationale",
                "owner_action": "test_action",
                "include_in_reader_brief": True,
                "include_in_daily_task_dashboard": False,
                "required_for_daily_reading": False,
            }
        ],
    }
    path = tmp_path / "report_registry.yaml"
    path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return path


def _write_waivers(tmp_path: Path, *, expires_at: str = "2026-12-31") -> Path:
    payload = {
        "schema_version": 1,
        "policy_id": "test_report_index_visibility_waivers",
        "policy_metadata": {"owner": "test", "status": "test"},
        "waivers": [
            {
                "waiver_id": "optional_missing_waiver",
                "issue_status": "MISSING",
                "report_id": "optional_missing",
                "owner": "test",
                "created_at": "2026-05-01",
                "expires_at": expires_at,
                "review_status": "approved_active",
                "linked_task_id": "TRADING-TEST",
                "reason": "test reason",
                "accepted_impact": "test impact",
                "validation_coverage": "test validation",
                "exit_condition": "test exit",
            }
        ],
    }
    path = tmp_path / "report_index_visibility_waivers.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path
