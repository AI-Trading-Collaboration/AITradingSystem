from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_index_visibility_waivers,
    load_report_registry,
    render_report_index_html,
)


def test_default_report_registry_loads() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)

    assert registry["schema_version"] == 1
    assert registry["policy_version"] == "report_registry_v1"
    assert all(
        isinstance(item.get("freshness_sla_days"), int)
        and not isinstance(item.get("freshness_sla_days"), bool)
        and item["freshness_sla_days"] >= 0
        for item in registry["reports"]
    )
    assert any(item["report_id"] == "reader_brief" for item in registry["reports"])
    assert any(item["report_id"] == "etf_portfolio_brief" for item in registry["reports"])
    assert any(item["report_id"] == "etf_backtest_summary" for item in registry["reports"])
    assert any(item["report_id"] == "etf_experiment_comparison" for item in registry["reports"])
    assert any(
        item["report_id"] == "etf_experiment_candidate_selection" for item in registry["reports"]
    )
    assert any(item["report_id"] == "etf_experiment_weekly_review" for item in registry["reports"])
    assert any(item["report_id"] == "etf_experiment_validation" for item in registry["reports"])
    assert any(item["report_id"] == "etf_ai_confirmation_report" for item in registry["reports"])
    assert any(item["report_id"] == "etf_ai_confirmation_overlay" for item in registry["reports"])
    assert any(
        item["report_id"] == "etf_ai_confirmation_validation" for item in registry["reports"]
    )
    assert any(
        item["report_id"] == "etf_weight_dual_track_calibration_report"
        for item in registry["reports"]
    )
    assert any(item["report_id"] == "etf_operations_health_report" for item in registry["reports"])
    assert any(item["report_id"] == "etf_operations_validation" for item in registry["reports"])
    assert any(
        item["report_id"] == "etf_strategy_evidence_dashboard" for item in registry["reports"]
    )
    assert any(
        item["report_id"] == "etf_strategy_evidence_dashboard_validation"
        for item in registry["reports"]
    )
    assert any(item["report_id"] == "etf_dynamic_robustness_report" for item in registry["reports"])
    assert any(
        item["report_id"] == "etf_dynamic_robustness_validation" for item in registry["reports"]
    )
    assert any(
        item["report_id"] == "etf_dynamic_shadow_review_package" for item in registry["reports"]
    )
    assert any(
        item["report_id"] == "etf_dynamic_shadow_weekly_review" for item in registry["reports"]
    )
    assert any(item["report_id"] == "etf_dynamic_shadow_validation" for item in registry["reports"])
    assert all("freshness_rationale" in item for item in registry["reports"])
    assert registry["defaults"]["freshness_basis_by_cadence"]["daily"] == (
        "us_equity_trading_days"
    )
    dynamic_v3_leaderboard = next(
        item
        for item in registry["reports"]
        if item["report_id"] == "etf_dynamic_v3_parameter_sweep_leaderboard"
    )
    assert dynamic_v3_leaderboard["artifact_selection_policy"] == "latest_available"


def test_default_report_index_visibility_waivers_load() -> None:
    policy = load_report_index_visibility_waivers(DEFAULT_REPORT_INDEX_WAIVER_PATH)

    assert policy["schema_version"] == 1
    assert policy["policy_id"] == "report_index_visibility_waivers_v1"
    assert all(item.get("created_at") for item in policy["waivers"])
    assert all(item.get("expires_at") for item in policy["waivers"])
    assert all(item.get("review_status") == "approved_active" for item in policy["waivers"])
    assert all(item.get("linked_task_id") for item in policy["waivers"])
    assert any(
        "etf_dynamic_shadow_weekly_review" in item.get("report_ids", [])
        for item in policy["waivers"]
    )


@pytest.mark.parametrize("value", [None, "missing"])
def test_report_registry_rejects_missing_freshness_sla_days(tmp_path: Path, value: object) -> None:
    registry_path = _write_registry(tmp_path)
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if value == "missing":
        registry["reports"][0].pop("freshness_sla_days")
    else:
        registry["reports"][0]["freshness_sla_days"] = value
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="freshness_sla_days"):
        load_report_registry(registry_path)


def test_report_registry_rejects_unknown_artifact_selection_policy(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["reports"][0]["artifact_selection_policy"] = "future_artifacts_for_everyone"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="artifact_selection_policy"):
        load_report_registry(registry_path)


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


def test_report_index_latest_available_policy_can_select_after_as_of_artifact(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path)
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["reports"][0]["artifact_selection_policy"] = "latest_available"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "daily_score_2026-05-06.md").write_text("# Daily Score\n", encoding="utf-8")
    _write_json(
        reports_dir / "evidence_dashboard_2026-05-06.json",
        {"report_type": "evidence_dashboard", "status": "PASS", "production_effect": "none"},
    )

    payload = build_report_index_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
        registry_path=registry_path,
    )
    reports = {item["report_id"]: item for item in payload["reports"]}

    assert reports["daily_score"]["latest_artifact_name"] == "daily_score_2026-05-06.md"
    assert reports["daily_score"]["artifact_date"] == "2026-05-06"
    assert reports["daily_score"]["artifact_selection_policy"] == "latest_available"
    assert reports["daily_score"]["artifact_temporal_relation"] == "AFTER_AS_OF"
    assert reports["daily_score"]["artifact_after_as_of"] is True
    assert reports["daily_score"]["age_days"] == 0
    assert reports["daily_score"]["freshness_status"] == "FRESH"
    assert reports["evidence_dashboard"]["freshness_status"] == "MISSING"
    assert reports["evidence_dashboard"]["artifact_selection_policy"] == "as_of_or_unknown"


def test_report_index_can_use_us_equity_trading_day_freshness(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path)
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["defaults"]["freshness_basis_by_cadence"] = {"daily": "us_equity_trading_days"}
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "daily_score_2026-06-12.md").write_text("# Daily Score\n", encoding="utf-8")

    payload = build_report_index_payload(
        as_of=date(2026, 6, 15),
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=None,
    )
    reports = {item["report_id"]: item for item in payload["reports"]}

    assert reports["daily_score"]["freshness_basis"] == "us_equity_trading_days"
    assert reports["daily_score"]["age_days"] == 1
    assert reports["daily_score"]["freshness_status"] == "FRESH"
    assert "us_equity_trading_days" in payload["visibility_audit"]["freshness_basis_values"]


def test_report_index_explicit_waivers_clear_optional_visibility_warnings(
    tmp_path: Path,
) -> None:
    registry_path = _write_custom_registry(
        tmp_path,
        [
            _registry_entry(
                "optional_missing",
                "Optional Missing",
                "outputs/reports/optional_missing_*.json",
                freshness_sla_days=1,
            ),
            _registry_entry(
                "optional_stale",
                "Optional Stale",
                "outputs/reports/optional_stale_*.json",
                freshness_sla_days=1,
            ),
        ],
    )
    _write_json(
        tmp_path / "outputs" / "reports" / "optional_stale_2026-05-01.json",
        {"report_type": "optional_stale", "status": "PASS", "production_effect": "none"},
    )
    waiver_path = _write_waivers(
        tmp_path,
        [
            {
                "waiver_id": "optional_missing_waiver",
                "issue_status": "MISSING",
                "report_id": "optional_missing",
            },
            {
                "waiver_id": "optional_stale_waiver",
                "issue_status": "STALE",
                "report_id": "optional_stale",
            },
        ],
    )

    payload = build_report_index_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    reports = {item["report_id"]: item for item in payload["reports"]}

    assert payload["status"] == "PASS_WITH_EXPLICIT_WAIVERS"
    assert payload["warnings"] == []
    assert payload["summary"]["explicit_waiver_count"] == 2
    assert payload["visibility_audit"]["audit_status"] == "PASS"
    assert reports["optional_missing"]["visibility_status"] == "WAIVED"
    assert reports["optional_stale"]["visibility_status"] == "WAIVED"
    assert reports["optional_stale"]["freshness_status"] == "STALE"


def test_report_index_does_not_waive_required_missing_artifacts(tmp_path: Path) -> None:
    registry_path = _write_custom_registry(
        tmp_path,
        [
            _registry_entry(
                "required_missing",
                "Required Missing",
                "outputs/reports/required_missing_*.json",
                freshness_sla_days=1,
                required=True,
            ),
        ],
    )
    waiver_path = _write_waivers(
        tmp_path,
        [
            {
                "waiver_id": "required_missing_waiver",
                "issue_status": "MISSING",
                "report_id": "required_missing",
            },
        ],
    )

    payload = build_report_index_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["explicit_waiver_count"] == 0
    assert payload["warnings"] == ["required_missing_required_missing"]


def test_report_index_expired_waiver_does_not_clear_visibility_warning(
    tmp_path: Path,
) -> None:
    registry_path = _write_custom_registry(
        tmp_path,
        [
            _registry_entry(
                "optional_missing",
                "Optional Missing",
                "outputs/reports/optional_missing_*.json",
                freshness_sla_days=1,
            ),
        ],
    )
    waiver_path = _write_waivers(
        tmp_path,
        [
            {
                "waiver_id": "expired_missing_waiver",
                "issue_status": "MISSING",
                "report_id": "optional_missing",
                "expires_at": "2026-05-03",
            },
        ],
    )

    payload = build_report_index_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["explicit_waiver_count"] == 0
    assert payload["summary"]["expired_waiver_count"] == 1
    assert payload["visibility_audit"]["expired_waiver_ids"] == ["expired_missing_waiver"]
    assert payload["warnings"] == ["optional_missing_missing:"]


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
        tmp_path / "reports" / "etf_portfolio" / "backtests" / "etf-backtest-20260531T124140Z"
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


def _write_custom_registry(tmp_path: Path, entries: list[dict[str, Any]]) -> Path:
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
        "reports": entries,
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


def _write_waivers(tmp_path: Path, waivers: list[dict[str, str]]) -> Path:
    normalized = []
    for waiver in waivers:
        item = {
            "owner": "test",
            "created_at": "2026-05-01",
            "expires_at": "2026-12-31",
            "review_status": "approved_active",
            "linked_task_id": "TRADING-TEST",
            "reason": "test reason",
            "accepted_impact": "test impact",
            "validation_coverage": "test validation",
            "exit_condition": "test exit",
        }
        item.update(waiver)
        normalized.append(item)
    path = tmp_path / "report_index_visibility_waivers.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_id": "test_report_index_visibility_waivers",
                "policy_metadata": {"owner": "test", "status": "test"},
                "waivers": normalized,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
