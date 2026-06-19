from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.engineering_stage_b_readiness import (
    build_engineering_stage_b_readiness_payload,
    validate_engineering_stage_b_readiness_payload,
)

RUN_DATE = date(2026, 6, 19)


def test_stage_b_readiness_reports_warning_backlog_without_blocking(tmp_path: Path) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("current_report",))
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(
        reports_dir / "current_report_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "current_report",
            "status": "PASS",
            "production_effect": "none",
        },
    )

    payload = build_engineering_stage_b_readiness_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=None,
    )
    validation = validate_engineering_stage_b_readiness_payload(payload)

    assert payload["readiness_status"] == "ENGINEERING_STAGE_B_READY_WITH_LIMITATIONS"
    assert payload["summary"]["report_index_unwaived_issue_count"] == 0
    assert payload["summary"]["missing_validation_tier_count"] == 1
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"


def test_stage_b_readiness_blocks_unwaived_report_index_issue(tmp_path: Path) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("missing_report",))

    payload = build_engineering_stage_b_readiness_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=None,
    )
    validation = validate_engineering_stage_b_readiness_payload(payload)

    assert payload["readiness_status"] == "ENGINEERING_STAGE_B_BLOCKED"
    assert payload["summary"]["report_index_unwaived_issue_count"] == 1
    assert validation["validation_status"] == "FAIL"


def test_stage_b_readiness_recognizes_central_error_taxonomy(tmp_path: Path) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("current_report",))
    _write_error_taxonomy(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(
        reports_dir / "current_report_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "current_report",
            "status": "PASS",
            "production_effect": "none",
        },
    )

    payload = build_engineering_stage_b_readiness_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=None,
    )

    assert payload["summary"]["missing_error_category_count"] == 0
    assert payload["summary"]["error_taxonomy_central_module_exists"] is True
    assert "error_taxonomy_not_centralized" not in {
        issue["issue_id"] for issue in payload["warning_issues"]
    }


def test_stage_b_readiness_accepts_config_contract_registry_coverage(
    tmp_path: Path,
) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("current_report",))
    config_dir = tmp_path / "config"
    legacy_config = config_dir / "legacy_without_inline_metadata.yaml"
    legacy_config.write_text("enabled: true\n", encoding="utf-8")
    _write_config_contract_registry(tmp_path, paths=("config/legacy_without_inline_metadata.yaml",))
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(
        reports_dir / "current_report_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "current_report",
            "status": "PASS",
            "production_effect": "none",
        },
    )

    payload = build_engineering_stage_b_readiness_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=None,
    )

    legacy_record = next(
        record
        for record in payload["config_schema_records"]
        if record["path"].endswith("legacy_without_inline_metadata.yaml")
    )
    assert legacy_record["contract_registry_present"] is True
    assert payload["summary"]["config_contract_registry_present_count"] == 1
    assert payload["summary"]["config_without_schema_or_policy_metadata_count"] == 0


def test_stage_b_readiness_tracks_pre_policy_run_manifests_as_legacy(
    tmp_path: Path,
) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("current_report",))
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(
        reports_dir / "current_report_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "current_report",
            "status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        tmp_path
        / "outputs"
        / "runs"
        / "daily"
        / "20260618T000000Z"
        / "as_of_2026-06-18__legacy"
        / "manifest.json",
        {
            "generated_at": "2026-06-18T00:00:00+00:00",
            "status": "PASS",
        },
    )

    payload = build_engineering_stage_b_readiness_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=None,
    )

    assert payload["summary"]["run_manifest_count"] == 1
    assert payload["summary"]["current_policy_run_manifest_count"] == 0
    assert payload["summary"]["legacy_pre_policy_run_manifest_count"] == 1
    assert payload["summary"]["run_manifest_missing_required_field_count"] == 0
    assert payload["summary"]["legacy_run_manifest_missing_required_field_count"] > 0
    assert "run_manifest_missing_required_fields" not in {
        issue["issue_id"] for issue in payload["warning_issues"]
    }


def test_stage_b_readiness_cli_writes_report_and_validation(tmp_path: Path) -> None:
    registry_path, policy_path = _write_project(tmp_path, report_ids=("current_report",))
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(
        reports_dir / "current_report_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "current_report",
            "status": "PASS",
            "production_effect": "none",
        },
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "engineering-stage-b-readiness",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--policy-path",
            str(policy_path),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(_write_waiver_policy(tmp_path)),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    readiness_json = reports_dir / "engineering_stage_b_readiness_2026-06-19.json"
    assert readiness_json.exists()

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-engineering-stage-b-readiness",
            "--source-json-path",
            str(readiness_json),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert validation.exit_code == 0, validation.output
    validation_json = reports_dir / "engineering_stage_b_readiness_validation_2026-06-19.json"
    assert validation_json.exists()
    payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert payload["validation_status"] == "PASS_WITH_WARNINGS"


def _write_project(tmp_path: Path, *, report_ids: tuple[str, ...]) -> tuple[Path, Path]:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    (tmp_path / "src" / "ai_trading_system").mkdir(parents=True)
    scripts_dir.joinpath("run_validation_tier.py").write_text(
        'TIER_SPECS = {"fast-unit": TierSpec(), "contract-validation": TierSpec()}\n',
        encoding="utf-8",
    )
    registry_path = config_dir / "report_registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(_registry(report_ids), sort_keys=False),
        encoding="utf-8",
    )
    policy_path = config_dir / "engineering_closeout_policy.yaml"
    policy_path.write_text(
        yaml.safe_dump(_policy(), sort_keys=False),
        encoding="utf-8",
    )
    return registry_path, policy_path


def _registry(report_ids: tuple[str, ...]) -> dict[str, Any]:
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
                "report_id": report_id,
                "title": report_id,
                "group": "governance",
                "cadence": "daily",
                "audience": "test",
                "owner": "system",
                "command": f"aits reports latest --report-id {report_id}",
                "artifact_globs": [f"outputs/reports/{report_id}_*.json"],
                "freshness_sla_days": 1,
                "freshness_rationale": "test",
                "owner_action": "review",
                "include_in_reader_brief": True,
                "include_in_daily_task_dashboard": False,
                "required_for_daily_reading": False,
            }
            for report_id in report_ids
        ],
    }


def _policy() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "policy_id": "test_engineering_closeout_policy",
        "policy_version": "2026-06-19",
        "policy_metadata": {"owner": "test", "status": "test"},
        "stage_b": {
            "required_artifact_fields": [
                "schema_version",
                "report_type",
                "status",
                "production_effect",
            ],
            "required_validation_tiers": ["fast-unit", "reproducibility"],
            "required_error_categories": ["INPUT_MISSING", "INTERNAL_ERROR"],
        },
    }


def _write_waiver_policy(tmp_path: Path) -> Path:
    waiver_path = tmp_path / "config" / "report_index_visibility_waivers.yaml"
    waiver_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_id": "test_waivers",
                "policy_metadata": {"owner": "test", "status": "test"},
                "waivers": [],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return waiver_path


def _write_config_contract_registry(tmp_path: Path, *, paths: tuple[str, ...]) -> Path:
    registry_path = tmp_path / "config" / "config_contract_registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_id": "test_config_contract_registry",
                "policy_version": "test",
                "policy_metadata": {
                    "owner": "test",
                    "status": "test",
                    "rationale": "test",
                    "intended_effect": "test",
                    "validation_evidence": "test",
                    "review_condition": "test",
                },
                "config_contracts": [{"path": path} for path in paths],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return registry_path


def _write_error_taxonomy(tmp_path: Path) -> None:
    taxonomy_path = tmp_path / "src" / "ai_trading_system" / "error_taxonomy.py"
    taxonomy_path.write_text(
        "\n".join(
            [
                'ERROR_CATEGORY_CODES = ("INPUT_MISSING", "INTERNAL_ERROR")',
                'REQUIRED_LOG_FIELDS = ("run_id", "status", "next_action")',
            ]
        ),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
