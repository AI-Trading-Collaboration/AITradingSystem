from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.documentation_contract import (
    build_documentation_contract_payload,
    render_documentation_contract_report,
)


def test_documentation_contract_passes_when_registry_reports_have_catalog_rows(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path, "example_report")
    catalog_path = _write_catalog(
        tmp_path,
        [
            (
                "`outputs/reports/example_report_YYYY-MM-DD.json`",
                "`aits reports example`",
                "`source.json`",
                "`schema_version=1`、`report_type=example_report`、`status`、`production_effect=none`",
                "Reader Brief",
                "否，`production_effect=none`",
                "只读报告，不修改 production。",
            )
        ],
    )

    payload = build_documentation_contract_payload(
        as_of=date(2026, 5, 28),
        registry_path=registry_path,
        artifact_catalog_path=catalog_path,
    )
    markdown = render_documentation_contract_report(payload)

    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["catalog_documented_count"] == 1
    assert payload["reports"][0]["catalog_status"] == "DOCUMENTED"
    assert payload["reports"][0]["command_documented"] is True
    assert "Generated Catalog" in markdown
    assert "example_report" in markdown


def test_documentation_contract_fails_when_catalog_row_is_missing(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path, "missing_report")
    catalog_path = _write_catalog(tmp_path, [])

    payload = build_documentation_contract_payload(
        as_of=date(2026, 5, 28),
        registry_path=registry_path,
        artifact_catalog_path=catalog_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["summary"]["missing_catalog_count"] == 1
    assert payload["issues"][0]["code"] == "missing_artifact_catalog_row"


def test_default_documentation_contract_covers_current_report_registry() -> None:
    payload = build_documentation_contract_payload(as_of=date(2026, 5, 28))

    assert payload["status"] == "PASS"
    assert payload["summary"]["missing_catalog_count"] == 0
    assert payload["summary"]["error_count"] == 0
    assert any(report["report_id"] == "documentation_contract" for report in payload["reports"])
    assert any(report["report_id"] == "etf_portfolio_brief" for report in payload["reports"])
    assert any(report["report_id"] == "etf_backtest_summary" for report in payload["reports"])


def test_docs_report_contract_cli_writes_outputs_and_fails_on_contract_error(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path, "missing_report")
    catalog_path = _write_catalog(tmp_path, [])
    output_path = tmp_path / "documentation_contract.md"
    json_output_path = tmp_path / "documentation_contract.json"

    result = CliRunner().invoke(
        app,
        [
            "docs",
            "report-contract",
            "--as-of",
            "2026-05-28",
            "--registry-path",
            str(registry_path),
            "--artifact-catalog-path",
            str(catalog_path),
            "--output-path",
            str(output_path),
            "--json-output-path",
            str(json_output_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 1
    assert "Documentation contract：FAIL" in result.output
    assert output_path.exists()
    assert json_output_path.exists()
    payload = json.loads(json_output_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "documentation_contract"
    assert payload["issues"][0]["code"] == "missing_artifact_catalog_row"


def _write_registry(tmp_path: Path, report_id: str) -> Path:
    registry: dict[str, Any] = {
        "schema_version": 1,
        "policy_version": "test_report_registry_v1",
        "defaults": {
            "production_effect": "none",
            "missing_status": "MISSING",
            "stale_status": "STALE",
        },
        "reports": [
            {
                "report_id": report_id,
                "title": "Example Report",
                "group": "daily",
                "cadence": "daily",
                "audience": "test",
                "owner": "system",
                "command": "aits reports example",
                "artifact_globs": [f"outputs/reports/{report_id}_*.json"],
                "freshness_sla_days": 1,
                "freshness_rationale": "test rationale",
                "owner_action": "review_if_missing",
                "include_in_reader_brief": True,
                "include_in_daily_task_dashboard": False,
                "required_for_daily_reading": False,
            }
        ],
    }
    path = tmp_path / "report_registry.yaml"
    path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return path


def _write_catalog(tmp_path: Path, rows: list[tuple[str, ...]]) -> Path:
    lines = [
        "# 产物目录",
        "",
        "| Artifact | 由谁生成 | 上游输入 | 关键字段或内容 | 下游使用 | "
        "是否影响 production | 常见误解 |",
        "|---|---|---|---|---|---|---|",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    path = tmp_path / "artifact_catalog.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
