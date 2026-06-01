from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
    render_report_index_html,
)


def test_default_report_registry_loads() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)

    assert registry["schema_version"] == 1
    assert registry["policy_version"] == "report_registry_v1"
    assert any(item["report_id"] == "reader_brief" for item in registry["reports"])
    assert any(item["report_id"] == "etf_portfolio_brief" for item in registry["reports"])
    assert any(item["report_id"] == "etf_backtest_summary" for item in registry["reports"])
    assert any(item["report_id"] == "etf_experiment_comparison" for item in registry["reports"])
    assert any(
        item["report_id"] == "etf_experiment_candidate_selection"
        for item in registry["reports"]
    )
    assert any(item["report_id"] == "etf_experiment_weekly_review" for item in registry["reports"])
    assert any(item["report_id"] == "etf_experiment_validation" for item in registry["reports"])
    assert any(item["report_id"] == "etf_ai_confirmation_report" for item in registry["reports"])
    assert any(item["report_id"] == "etf_ai_confirmation_overlay" for item in registry["reports"])
    assert all("freshness_rationale" in item for item in registry["reports"])


def test_report_index_classifies_latest_artifacts_and_freshness(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    backtests_dir = tmp_path / "outputs" / "backtests"
    reports_dir.mkdir(parents=True)
    backtests_dir.mkdir(parents=True)
    (reports_dir / "daily_score_2026-05-04.md").write_text("# Daily Score\n", encoding="utf-8")
    _write_json(
        reports_dir / "evidence_dashboard_2026-05-01.json",
        {"report_type": "evidence_dashboard", "status": "PASS", "production_effect": "none"},
    )
    (backtests_dir / "backtest_2026-02-01_2026-05-04.md").write_text(
        "# Backtest\n",
        encoding="utf-8",
    )
    (backtests_dir / "backtest_robustness_2026-05-04_2026-05-04.md").write_text(
        "# Robustness\n",
        encoding="utf-8",
    )

    payload = build_report_index_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
        registry_path=registry_path,
    )
    html = render_report_index_html(payload)
    reports = {item["report_id"]: item for item in payload["reports"]}

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["report_count"] == 5
    assert reports["daily_score"]["freshness_status"] == "FRESH"
    assert reports["evidence_dashboard"]["freshness_status"] == "STALE"
    assert reports["missing_required"]["freshness_status"] == "MISSING"
    assert reports["missing_required"]["required_for_daily_reading"] is True
    assert reports["backtest_daily"]["latest_artifact_name"] == "backtest_2026-02-01_2026-05-04.md"
    assert not reports["backtest_daily"]["latest_artifact_name"].startswith("backtest_robustness")
    assert any("missing_required_required_missing" in item for item in payload["warnings"])
    assert "Report Registry & Cadence Calendar" in html


def test_reports_index_cli_writes_html_and_json(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    (tmp_path / "outputs" / "reports").mkdir(parents=True)
    (tmp_path / "outputs" / "reports" / "daily_score_2026-05-04.md").write_text(
        "# Daily Score\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "report_index_2026-05-04.html"
    json_output_path = tmp_path / "report_index_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "index",
            "--as-of",
            "2026-05-04",
            "--registry-path",
            str(registry_path),
            "--project-root",
            str(tmp_path),
            "--output-path",
            str(output_path),
            "--json-output-path",
            str(json_output_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Report index：PASS_WITH_WARNINGS" in result.output
    assert "只读扫描" in result.output
    assert output_path.exists()
    assert json_output_path.exists()
    payload = json.loads(json_output_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "report_index"
    assert payload["summary"]["report_count"] == 5


def test_report_index_extracts_date_from_etf_backtest_run_directory(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    backtest_dir = (
        tmp_path
        / "reports"
        / "etf_portfolio"
        / "backtests"
        / "etf-backtest-20260531T124140Z"
    )
    backtest_dir.mkdir(parents=True)
    (backtest_dir / "summary.md").write_text("# ETF Backtest\n", encoding="utf-8")

    payload = build_report_index_payload(
        as_of=date(2026, 5, 31),
        project_root=tmp_path,
        registry_path=registry_path,
    )
    reports = {item["report_id"]: item for item in payload["reports"]}

    assert reports["etf_backtest_summary"]["artifact_date"] == "2026-05-31"
    assert reports["etf_backtest_summary"]["freshness_status"] == "FRESH"
    assert reports["etf_backtest_summary"]["latest_artifact_name"] == "summary.md"


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
            _registry_entry(
                "daily_score",
                "Daily Score",
                "outputs/reports/daily_score_*.md",
                freshness_sla_days=1,
                required=True,
            ),
            _registry_entry(
                "evidence_dashboard",
                "Evidence Dashboard",
                "outputs/reports/evidence_dashboard_*.json",
                freshness_sla_days=1,
            ),
            _registry_entry(
                "missing_required",
                "Missing Required",
                "outputs/reports/missing_required_*.json",
                freshness_sla_days=1,
                required=True,
            ),
            _registry_entry(
                "backtest_daily",
                "Backtest Daily",
                "outputs/backtests/backtest_*.md",
                freshness_sla_days=90,
            ),
            _registry_entry(
                "etf_backtest_summary",
                "ETF Backtest Summary",
                "reports/etf_portfolio/backtests/*/summary.md",
                freshness_sla_days=30,
            ),
        ],
    }
    path = tmp_path / "report_registry.yaml"
    path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return path


def _registry_entry(
    report_id: str,
    title: str,
    artifact_glob: str,
    *,
    freshness_sla_days: int,
    required: bool = False,
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "daily",
        "cadence": "daily",
        "audience": "test",
        "owner": "system",
        "command": "test command",
        "artifact_globs": [artifact_glob],
        "freshness_sla_days": freshness_sla_days,
        "freshness_rationale": "test rationale",
        "owner_action": "test_action",
        "include_in_reader_brief": True,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": required,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
