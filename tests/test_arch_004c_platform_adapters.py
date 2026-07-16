from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.contracts import (
    ArtifactLifecycle,
    EntrypointRef,
    ReaderTier,
    ReportAudience,
    WorkflowCadence,
)
from ai_trading_system.core import ProductionEffect
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
    write_data_quality_report,
)
from ai_trading_system.legacy.platform_contract_adapters import (
    PlatformContractAdapterError,
    data_quality_report_to_evidence,
    report_registry_entry_to_spec,
    scheduled_task_to_workflow_spec,
)
from ai_trading_system.reports.report_index import load_report_registry
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config


def test_scheduled_validate_data_adapter_preserves_legacy_command_and_safety() -> None:
    config = load_scheduled_tasks_config()
    task = config.tasks_by_id()["daily_validate_data"]

    spec = scheduled_task_to_workflow_spec(
        task,
        config=config,
        entrypoint=EntrypointRef(
            "ai_trading_system.cli_commands.data",
            "validate_data",
        ),
        owner="operations",
        timezone="Asia/Tokyo",
        quality_gate_required=False,
        expected_artifact_types=("data_quality_report",),
    )

    assert spec.workflow_id == "scheduled_task:daily_validate_data"
    assert spec.cadence is WorkflowCadence.DAILY
    assert spec.due_policy_id == "scheduled_tasks_v2:daily_trading_day"
    assert spec.steps[0].step_id == task.daily_plan_step_id
    assert " ".join(spec.steps[0].legacy_command) == task.command
    assert spec.steps[0].entrypoint.display == ("ai_trading_system.cli_commands.data:validate_data")
    assert spec.steps[0].production_effect is ProductionEffect.NONE


def test_daily_score_report_adapter_preserves_registry_contract() -> None:
    registry = load_report_registry()
    entry = next(item for item in registry["reports"] if item["report_id"] == "daily_score")

    spec = report_registry_entry_to_spec(
        entry,
        canonical_source=EntrypointRef("ai_trading_system.scoring", "load_daily_score"),
        section_provider=EntrypointRef("ai_trading_system.reports.sections", "daily_score_section"),
        view_model=EntrypointRef("ai_trading_system.reports.view_models", "daily_score_view_model"),
        renderer=EntrypointRef("ai_trading_system.reports.renderers", "render_daily_score"),
        reader_tier=ReaderTier.OWNER_DAILY_BRIEF,
        actionable=True,
    )

    assert spec.report_id == entry["report_id"]
    assert spec.title == entry["title"]
    assert spec.owner == entry["owner"]
    assert spec.artifact_globs == tuple(entry["artifact_globs"])
    assert spec.freshness_sla_days == entry["freshness_sla_days"]
    assert spec.owner_action == entry["owner_action"]
    assert spec.audience is ReportAudience.INVESTOR
    assert spec.cadence is WorkflowCadence.DAILY
    assert spec.lifecycle is ArtifactLifecycle.CURRENT
    assert spec.production_effect is ProductionEffect.NONE
    assert spec.canonical_source != spec.renderer


def test_report_adapter_rejects_unknown_audience_without_inference() -> None:
    registry = load_report_registry()
    source = next(item for item in registry["reports"] if item["report_id"] == "daily_score")
    entry = {**source, "audience": "executive-ish"}

    with pytest.raises(PlatformContractAdapterError) as error:
        report_registry_entry_to_spec(
            entry,
            canonical_source=EntrypointRef("a", "source"),
            section_provider=EntrypointRef("b", "section"),
            view_model=EntrypointRef("c", "view"),
            renderer=EntrypointRef("d", "render"),
            reader_tier=ReaderTier.OWNER_DAILY_BRIEF,
            actionable=True,
        )
    assert error.value.code == "UNKNOWN_REPORT_AUDIENCE"


def test_data_quality_report_adapter_is_authoritative_and_fail_closed(
    tmp_path: Path,
) -> None:
    report = _quality_report()
    report_path = write_data_quality_report(report, tmp_path / "data_quality.md")

    evidence = data_quality_report_to_evidence(
        report,
        report_path=report_path,
        contract_id="cached_market_macro_validation",
        policy_id="data_quality",
        policy_version="data_quality.v1",
    )

    assert evidence.status == report.status == "PASS_WITH_WARNINGS"
    assert evidence.passed is True
    assert evidence.warning_count == 1
    assert evidence.checked_input_count == 4
    assert evidence.assert_ready() is evidence

    failed = _quality_report(
        issues=(DataQualityIssue(Severity.ERROR, "prices_missing", "missing"),)
    )
    failed_path = write_data_quality_report(failed, tmp_path / "failed.md")
    failed_evidence = data_quality_report_to_evidence(
        failed,
        report_path=failed_path,
        contract_id="cached_market_macro_validation",
        policy_id="data_quality",
        policy_version="data_quality.v1",
    )
    assert failed_evidence.passed is False
    assert failed_evidence.blocking_issues == ("prices_missing",)
    with pytest.raises(ValueError, match="DATA_QUALITY_NOT_PASSED"):
        failed_evidence.assert_ready()


def _quality_report(
    *,
    issues: tuple[DataQualityIssue, ...] = (
        DataQualityIssue(Severity.WARNING, "prices_stale_warning", "warning"),
    ),
) -> DataQualityReport:
    return DataQualityReport(
        checked_at=datetime(2026, 7, 10, 22, 0, tzinfo=UTC),
        as_of=date(2026, 7, 10),
        price_summary=DataFileSummary(Path("prices.csv"), exists=True, rows=2),
        rate_summary=DataFileSummary(Path("rates.csv"), exists=True, rows=2),
        expected_price_tickers=("QQQ", "TQQQ"),
        expected_rate_series=("DGS2", "DGS10"),
        issues=issues,
    )
