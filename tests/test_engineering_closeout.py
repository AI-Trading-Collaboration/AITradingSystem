from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.engineering_closeout import (
    build_engineering_surface_inventory_payload,
    render_engineering_surface_inventory_markdown,
    validate_engineering_surface_inventory_payload,
)

RUN_DATE = date(2026, 6, 19)


def test_engineering_surface_inventory_covers_required_surfaces(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path)

    payload = build_engineering_surface_inventory_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )
    validation = validate_engineering_surface_inventory_payload(payload)
    markdown = render_engineering_surface_inventory_markdown(payload)
    surface_types = {item["surface_type"] for item in payload["surfaces"]}

    assert payload["report_type"] == "engineering_surface_inventory"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["unknown_surface_count"] == 0
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"
    assert "Engineering Surface Inventory" in markdown
    assert {
        "cli_top_level",
        "cli_command",
        "report_registry_entry",
        "artifact_catalog_family",
        "configuration_file",
        "operations_entry_point",
        "documentation_family",
    }.issubset(surface_types)


def test_engineering_surface_inventory_validation_blocks_unknown_classification(
    tmp_path: Path,
) -> None:
    registry_path = _write_project(tmp_path)
    payload = build_engineering_surface_inventory_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )
    payload["surfaces"][0]["classification"] = "UNKNOWN"

    validation = validate_engineering_surface_inventory_payload(payload)

    issue_ids = {issue["issue_id"] for issue in validation["blocking_issues"]}
    assert validation["validation_status"] == "FAIL"
    assert "surface_classifications_known" in issue_ids


def test_engineering_surface_inventory_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    registry_path = _write_project(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    inventory_json = reports_dir / "engineering_surface_inventory_2026-06-19.json"
    inventory_md = reports_dir / "engineering_surface_inventory_2026-06-19.md"
    validation_json = reports_dir / "engineering_surface_inventory_validation_2026-06-19.json"
    validation_md = reports_dir / "engineering_surface_inventory_validation_2026-06-19.md"
    runner = CliRunner()

    run = runner.invoke(
        app,
        [
            "reports",
            "engineering-surface-inventory",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-engineering-surface-inventory",
            "--source-json-path",
            str(inventory_json),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert validation.exit_code == 0, validation.output
    assert inventory_json.exists()
    assert inventory_md.exists()
    assert validation_json.exists()
    assert validation_md.exists()
    payload = json.loads(inventory_json.read_text(encoding="utf-8"))
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert payload["report_type"] == "engineering_surface_inventory"
    assert validation_payload["input_artifacts"]["engineering_surface_inventory"] == str(
        inventory_json
    )


def _write_project(tmp_path: Path) -> Path:
    cli_dir = tmp_path / "src" / "ai_trading_system"
    commands_dir = cli_dir / "cli_commands"
    commands_dir.mkdir(parents=True)
    (cli_dir / "cli.py").write_text(
        "import typer\n"
        "from ai_trading_system.cli_commands.reports import reports_app\n"
        "app = typer.Typer()\n"
        "app.add_typer(reports_app, name='reports')\n"
        "app.add_typer(typer.Typer(), name='run')\n",
        encoding="utf-8",
    )
    (commands_dir / "reports.py").write_text(
        "import typer\n"
        "reports_app = typer.Typer()\n"
        "@reports_app.command('index')\n"
        "def index_command():\n"
        "    pass\n",
        encoding="utf-8",
    )
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "scheduled_tasks.yaml").write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_version": "test",
                "tasks": {"daily_trading_day": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry_path = tmp_path / "config" / "report_registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(_registry(), sort_keys=False),
        encoding="utf-8",
    )
    docs_dir = tmp_path / "docs"
    (docs_dir / "operations").mkdir(parents=True)
    (docs_dir / "requirements").mkdir()
    (docs_dir / "artifact_catalog.md").write_text(
        "|Artifact|Generator|Inputs|Schema|Consumers|Production|Notes|\n"
        "|---|---|---|---|---|---|---|\n"
        "|`outputs/reports/report_index_YYYY-MM-DD.json`|"
        "`aits reports index`|registry|schema_version=1|Reader Brief|否|read-only|\n",
        encoding="utf-8",
    )
    (docs_dir / "system_flow.md").write_text("# System Flow\n", encoding="utf-8")
    (docs_dir / "task_register.md").write_text("# Tasks\n", encoding="utf-8")
    (docs_dir / "operations" / "operations_runbook.md").write_text(
        "# Runbook\n",
        encoding="utf-8",
    )
    (docs_dir / "requirements" / "TRADING-999_Test.md").write_text(
        "# Requirement\n",
        encoding="utf-8",
    )
    return registry_path


def _registry() -> dict[str, Any]:
    return {
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
                "report_id": "report_index",
                "title": "Report Index",
                "group": "governance",
                "cadence": "daily",
                "audience": "operator",
                "owner": "system",
                "command": "aits reports index",
                "artifact_globs": ["outputs/reports/report_index_*.json"],
                "freshness_sla_days": 1,
                "freshness_rationale": "test",
                "owner_action": "review",
                "include_in_reader_brief": True,
                "include_in_daily_task_dashboard": False,
                "required_for_daily_reading": False,
            }
        ],
    }
