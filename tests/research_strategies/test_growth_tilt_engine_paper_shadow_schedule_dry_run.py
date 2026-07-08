from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_schedule_dry_run as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_paper_shadow_schedule_dry_run as schedule_dry_run,
)


def test_paper_shadow_schedule_dry_run_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == schedule_dry_run.READY_STATUS
    assert payload["paper_shadow_dry_run_wiring_ready"] is True
    assert payload["schedule_hook_verified_disabled"] is True
    assert payload["runtime_boundary_verified"] is True
    assert payload["manual_review_handoff_wired"] is True
    assert payload["prior_no_effect_audit_ready"] is True
    assert payload["paper_shadow_schedule_dry_run_ready"] is True
    assert payload["schedule_dry_run_gap_count"] == 0
    assert payload["schedule_boundary_checklist_ready"] is True
    assert payload["schedule_no_effect_audit_ready"] is True
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["recommended_next_research_task"] == schedule_dry_run.NEXT_ROUTE_READY


def test_paper_shadow_schedule_dry_run_blocks_prior_not_ready() -> None:
    sources = _source_documents()
    sources["dry_run_wiring_result_2425"]["dry_run_wiring_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert payload["paper_shadow_schedule_dry_run_ready"] is False
    assert "prior_dry_run_wiring_ready" in payload["schedule_dry_run_gap_ids"]
    assert payload["recommended_next_research_task"] == schedule_dry_run.NEXT_ROUTE_BLOCKED


def test_paper_shadow_schedule_dry_run_blocks_schedule_hook_enabled() -> None:
    sources = _source_documents()
    section = sources["schedule_hook_disabled_verification_2425"][
        "schedule_hook_disabled_verification"
    ]
    section["schedule_hook_verified_disabled"] = False
    section["scheduler_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert "schedule_hook_remains_disabled" in payload["schedule_dry_run_gap_ids"]
    assert "paper_shadow_schedule_disabled" in payload["schedule_dry_run_gap_ids"]


def test_paper_shadow_schedule_dry_run_blocks_runtime_or_production_enabled() -> None:
    sources = _source_documents()
    runtime = sources["runtime_boundary_manifest_2425"]["runtime_boundary_manifest"]
    runtime["paper_shadow_enabled"] = True
    runtime["production_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert "runtime_boundary_remains_disabled" in payload["schedule_dry_run_gap_ids"]
    assert "paper_shadow_runtime_disabled" in payload["schedule_dry_run_gap_ids"]
    assert "production_disabled" in payload["schedule_dry_run_gap_ids"]


def test_paper_shadow_schedule_dry_run_blocks_missing_manual_handoff() -> None:
    sources = _source_documents()
    handoff = sources["manual_review_handoff_wiring_plan_2425"][
        "manual_review_handoff_wiring_plan"
    ]
    handoff["manual_review_handoff_wired"] = False

    payload = _build_payload(sources)

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert "manual_review_handoff_resolves" in payload["schedule_dry_run_gap_ids"]


def test_paper_shadow_schedule_dry_run_blocks_failed_boundary_check() -> None:
    payload = _build_payload(
        _source_documents(),
        schedule_boundary_checklist=[
            {"check_id": "daily_job_not_run", "passed": False}
        ],
    )

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert "schedule_boundary_checklist_passes" in payload["schedule_dry_run_gap_ids"]
    assert payload["schedule_boundary_checklist_ready"] is False


def test_paper_shadow_schedule_dry_run_ready_has_no_effect_outputs() -> None:
    payload = _build_payload(_source_documents())

    assert payload["paper_shadow_schedule_dry_run_ready"] is True
    assert payload["schedule_hook_invoked"] is False
    assert payload["schedule_state_mutated"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["scheduled_task_created"] is False
    assert payload["paper_shadow_daily_job_run"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["backtest_run"] is False
    assert payload["scoring_run"] is False
    assert payload["daily_report_run"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["broker_order_generated"] is False


def test_paper_shadow_schedule_dry_run_blocks_missing_registration() -> None:
    sources = _source_documents()

    payload = schedule_dry_run.build_growth_tilt_engine_paper_shadow_schedule_dry_run(
        sources["dry_run_wiring_result_2425"],
        sources["schedule_hook_disabled_verification_2425"],
        sources["runtime_boundary_manifest_2425"],
        sources["manual_review_handoff_wiring_plan_2425"],
        sources["dry_run_no_effect_audit_summary_2425"],
        report_registry={"reports": []},
        artifact_catalog_text="",
        system_flow_text="",
        research_doc_texts={"source_2425_research_doc": ""},
    )

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert "report_registry_registered" in payload["schedule_dry_run_gap_ids"]
    assert "artifact_catalog_registered" in payload["schedule_dry_run_gap_ids"]
    assert "system_flow_registered" in payload["schedule_dry_run_gap_ids"]
    assert "research_docs_registered" in payload["schedule_dry_run_gap_ids"]


def test_paper_shadow_schedule_dry_run_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "schedule_dry_run"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_paper_shadow_schedule_dry_run(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == schedule_dry_run.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["paper_shadow_schedule_dry_run_ready"] is True
    assert payload["schedule_dry_run_gap_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "schedule_boundary_checklist_json",
        "schedule_no_effect_audit_summary_json",
        "markdown_path",
        "schedule_boundary_checklist_markdown",
        "schedule_no_effect_audit_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_paper_shadow_schedule_dry_run_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "schedule_dry_run_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-paper-shadow-schedule-dry-run",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "260"},
        terminal_width=260,
    )

    assert result.exit_code == 0, result.output
    assert schedule_dry_run.READY_STATUS in result.output
    assert "paper_shadow_dry_run_wiring_ready=true" in result.output
    assert "schedule_hook_verified_disabled=true" in result.output
    assert "runtime_boundary_verified=true" in result.output
    assert "manual_review_handoff_wired=true" in result.output
    assert "paper_shadow_schedule_dry_run_ready=true" in result.output
    assert "schedule_dry_run_gap_count=0" in result.output
    assert "schedule_boundary_checklist_ready=true" in result.output
    assert "schedule_no_effect_audit_ready=true" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "paper_shadow_schedule_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "generated_signal=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={schedule_dry_run.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "schedule_dry_run_result.json").exists()


def test_paper_shadow_schedule_dry_run_missing_prior_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["dry_run_wiring_result_2425"].unlink()

    payload = impl.run_growth_tilt_engine_paper_shadow_schedule_dry_run(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == schedule_dry_run.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["paper_shadow_schedule_dry_run_started"] is False
    assert payload["paper_shadow_schedule_dry_run_ready"] is False
    assert payload["schedule_dry_run_gap_count"] == 1
    assert payload["missing_schedule_evidence_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == schedule_dry_run.NEXT_ROUTE_BLOCKED


def test_paper_shadow_schedule_dry_run_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[schedule_dry_run.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-paper-shadow-schedule-dry-run"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("schedule_dry_run_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2427_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert schedule_dry_run.REPORT_TYPE in catalog
    assert "growth-tilt-engine-paper-shadow-schedule-dry-run" in system_flow
    assert schedule_dry_run.READY_STATUS in system_flow
    assert schedule_dry_run.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, dict],
    **kwargs: object,
) -> dict[str, object]:
    return schedule_dry_run.build_growth_tilt_engine_paper_shadow_schedule_dry_run(
        sources["dry_run_wiring_result_2425"],
        sources["schedule_hook_disabled_verification_2425"],
        sources["runtime_boundary_manifest_2425"],
        sources["manual_review_handoff_wiring_plan_2425"],
        sources["dry_run_no_effect_audit_summary_2425"],
        report_registry=_report_registry(),
        artifact_catalog_text=_artifact_catalog_text(),
        system_flow_text=_system_flow_text(),
        research_doc_texts=_research_doc_texts(),
        **kwargs,
    )


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "dry_run_wiring_result_2425": root / "dry_run_wiring_2425.json",
        "schedule_hook_disabled_verification_2425": root / "schedule_hook_2425.json",
        "runtime_boundary_manifest_2425": root / "runtime_boundary_2425.json",
        "manual_review_handoff_wiring_plan_2425": root / "manual_handoff_2425.json",
        "dry_run_no_effect_audit_summary_2425": root / "no_effect_2425.json",
        "research_doc_2425": root / "research_doc_2425.md",
        "schedule_hook_doc_2425": root / "schedule_hook_doc_2425.md",
        "runtime_boundary_doc_2425": root / "runtime_boundary_doc_2425.md",
        "manual_review_doc_2425": root / "manual_review_doc_2425.md",
        "no_effect_audit_doc_2425": root / "no_effect_audit_doc_2425.md",
        "route_doc_2425": root / "route_doc_2425.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    docs = _research_doc_texts()
    for key, path in paths.items():
        if key == "report_registry":
            path.write_text(_report_registry_yaml(), encoding="utf-8")
        elif key == "artifact_catalog":
            path.write_text(_artifact_catalog_text(), encoding="utf-8")
        elif key == "system_flow":
            path.write_text(_system_flow_text(), encoding="utf-8")
        elif key in docs:
            path.write_text(docs[key], encoding="utf-8")
        else:
            path.write_text(json.dumps(sources[key], indent=2), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2425_dry_run_wiring_result_path": paths[
            "dry_run_wiring_result_2425"
        ],
        "source_2425_schedule_hook_disabled_verification_path": paths[
            "schedule_hook_disabled_verification_2425"
        ],
        "source_2425_runtime_boundary_manifest_path": paths[
            "runtime_boundary_manifest_2425"
        ],
        "source_2425_manual_review_handoff_wiring_plan_path": paths[
            "manual_review_handoff_wiring_plan_2425"
        ],
        "source_2425_dry_run_no_effect_audit_summary_path": paths[
            "dry_run_no_effect_audit_summary_2425"
        ],
        "source_2425_research_doc_path": paths["research_doc_2425"],
        "source_2425_schedule_hook_doc_path": paths["schedule_hook_doc_2425"],
        "source_2425_runtime_boundary_doc_path": paths["runtime_boundary_doc_2425"],
        "source_2425_manual_review_doc_path": paths["manual_review_doc_2425"],
        "source_2425_no_effect_audit_doc_path": paths["no_effect_audit_doc_2425"],
        "source_2425_route_doc_path": paths["route_doc_2425"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        ("--source-2425-dry-run-wiring-result", "dry_run_wiring_result_2425"),
        (
            "--source-2425-schedule-hook-disabled-verification",
            "schedule_hook_disabled_verification_2425",
        ),
        ("--source-2425-runtime-boundary-manifest", "runtime_boundary_manifest_2425"),
        (
            "--source-2425-manual-review-handoff-wiring-plan",
            "manual_review_handoff_wiring_plan_2425",
        ),
        (
            "--source-2425-dry-run-no-effect-audit-summary",
            "dry_run_no_effect_audit_summary_2425",
        ),
        ("--source-2425-research-doc", "research_doc_2425"),
        ("--source-2425-schedule-hook-doc", "schedule_hook_doc_2425"),
        ("--source-2425-runtime-boundary-doc", "runtime_boundary_doc_2425"),
        ("--source-2425-manual-review-doc", "manual_review_doc_2425"),
        ("--source-2425-no-effect-audit-doc", "no_effect_audit_doc_2425"),
        ("--source-2425-route-doc", "route_doc_2425"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
        ("--system-flow", "system_flow"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    return {
        "dry_run_wiring_result_2425": _dry_run_wiring_result_2425(),
        "schedule_hook_disabled_verification_2425": {
            "schedule_hook_disabled_verification": _schedule_hook_verification()
        },
        "runtime_boundary_manifest_2425": {
            "runtime_boundary_manifest": _runtime_boundary_manifest()
        },
        "manual_review_handoff_wiring_plan_2425": {
            "manual_review_handoff_wiring_plan": _manual_review_handoff()
        },
        "dry_run_no_effect_audit_summary_2425": {
            "dry_run_no_effect_audit_summary": _prior_no_effect_audit()
        },
    }


def _dry_run_wiring_result_2425() -> dict[str, object]:
    return {
        "status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "readiness_status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "contract_ready": True,
        "contract_ready_count": 1,
        "contract_gap_count": 0,
        "paper_shadow_dry_run_wiring_completed": True,
        "dry_run_wiring_ready": True,
        "dry_run_wiring_gap_count": 0,
        "schedule_hook_verified_disabled": True,
        "manual_review_handoff_wired": True,
        "no_effect_audit_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "broker_action": "none",
        "order_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "recommended_next_research_task": schedule_dry_run.EXPECTED_PRIOR_NEXT_ROUTE,
    }


def _schedule_hook_verification() -> dict[str, object]:
    return {
        "status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "schedule_hook_verified_disabled": True,
        "paper_shadow_schedule_enabled": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "paper_shadow_daily_job_run": False,
        "cron_or_windows_task_created": False,
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_boundary_manifest() -> dict[str, object]:
    return {
        "status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _manual_review_handoff() -> dict[str, object]:
    return {
        "status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "manual_review_required": True,
        "manual_review_handoff_wired": True,
        "automatic_execution_allowed": False,
        "next_route": schedule_dry_run.EXPECTED_PRIOR_NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _prior_no_effect_audit() -> dict[str, object]:
    return {
        "status": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "no_effect_audit_ready": True,
        "dry_run_wiring_gap_count": 0,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2425": schedule_dry_run.PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS,
        "schedule_hook_doc_2425": "schedule hook disabled",
        "runtime_boundary_doc_2425": "runtime boundary disabled",
        "manual_review_doc_2425": "manual review handoff wired",
        "no_effect_audit_doc_2425": "no effect audit ready",
        "route_doc_2425": schedule_dry_run.EXPECTED_PRIOR_NEXT_ROUTE,
    }


def _report_registry() -> dict[str, list[dict[str, object]]]:
    return {
        "reports": [
            {
                "report_id": report_id,
                "command": _command_for_report(report_id),
                "artifact_globs": [],
                "artifact_selection_policy": "latest_available",
                "required_for_daily_reading": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for report_id in schedule_dry_run.REQUIRED_REPORT_IDS
        ]
    }


def _report_registry_yaml() -> str:
    rows = "\n".join(
        f"  - report_id: {report_id}\n"
        f"    command: {_command_for_report(report_id)}\n"
        "    artifact_globs: []\n"
        "    artifact_selection_policy: latest_available\n"
        "    required_for_daily_reading: false\n"
        "    production_effect: none\n"
        "    broker_action: none"
        for report_id in schedule_dry_run.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(schedule_dry_run.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(schedule_dry_run.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        schedule_dry_run.REPORT_TYPE: (
            "aits research strategies "
            "growth-tilt-engine-paper-shadow-schedule-dry-run"
        ),
        "growth_tilt_engine_paper_shadow_dry_run_wiring": (
            "aits research strategies growth-tilt-engine-paper-shadow-dry-run-wiring"
        ),
    }[report_id]
