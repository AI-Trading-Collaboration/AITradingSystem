from __future__ import annotations

import shlex
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import TypeVar

from ai_trading_system.contracts.artifact_envelope import ArtifactLifecycle
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.report_spec import ReaderTier, ReportAudience, ReportSpec
from ai_trading_system.contracts.workflow import (
    EntrypointRef,
    FailurePropagation,
    WorkflowCadence,
    WorkflowSpec,
    WorkflowStepSpec,
)
from ai_trading_system.core.production_effect import ProductionEffect
from ai_trading_system.data.quality import DataQualityReport, Severity
from ai_trading_system.scheduled_tasks import ScheduledTask, ScheduledTasksConfig

MappedT = TypeVar("MappedT")


class PlatformContractAdapterError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


_SCHEDULE_CADENCE_MAP = {
    "daily_trading_day": WorkflowCadence.DAILY,
    "weekly": WorkflowCadence.WEEKLY,
    "biweekly": WorkflowCadence.BIWEEKLY,
    "monthly": WorkflowCadence.MONTHLY,
    "ad_hoc_research": WorkflowCadence.MANUAL,
}
_REPORT_CADENCE_MAP = {
    "daily": WorkflowCadence.DAILY,
    "weekly": WorkflowCadence.WEEKLY,
    "biweekly": WorkflowCadence.BIWEEKLY,
    "monthly": WorkflowCadence.MONTHLY,
    "ad_hoc": WorkflowCadence.MANUAL,
}
_AUDIENCE_MAP = {item.value: item for item in ReportAudience}
_LEGACY_PRODUCTION_EFFECT_MAP = {
    "none": ProductionEffect.NONE,
    "local_cache_write": ProductionEffect.NONE,
    "local_report_write": ProductionEffect.NONE,
}
_VISIBILITY_LIFECYCLE_MAP = {
    "current": ArtifactLifecycle.CURRENT,
    "legacy_optional": ArtifactLifecycle.SUPERSEDED,
    "deprecated_optional": ArtifactLifecycle.DEPRECATED,
    "archived_optional": ArtifactLifecycle.ARCHIVED,
}


def data_quality_report_to_evidence(
    report: DataQualityReport,
    *,
    report_path: Path,
    contract_id: str,
    policy_id: str,
    policy_version: str,
) -> DataQualityEvidence:
    report_exists = report_path.is_file()
    blocking_issues = tuple(
        issue.code for issue in report.issues if issue.severity is Severity.ERROR
    )
    return DataQualityEvidence(
        contract_id=contract_id,
        policy_id=policy_id,
        policy_version=policy_version,
        status=report.status,
        passed=report.passed,
        checked_at=report.checked_at,
        as_of=report.as_of,
        report_path=str(report_path) if report_exists else None,
        report_sha256=(sha256(report_path.read_bytes()).hexdigest() if report_exists else None),
        error_count=report.error_count,
        warning_count=report.warning_count,
        checked_input_count=len(report.expected_price_tickers) + len(report.expected_rate_series),
        blocking_issues=blocking_issues,
    )


def scheduled_task_to_workflow_spec(
    task: ScheduledTask,
    *,
    config: ScheduledTasksConfig,
    entrypoint: EntrypointRef,
    owner: str,
    timezone: str,
    quality_gate_required: bool,
    expected_artifact_types: tuple[str, ...],
) -> WorkflowSpec:
    cadence = _required_mapping(
        _SCHEDULE_CADENCE_MAP,
        task.cadence,
        code="UNKNOWN_SCHEDULE_CADENCE",
    )
    production_effect = _required_mapping(
        _LEGACY_PRODUCTION_EFFECT_MAP,
        task.production_effect,
        code="UNKNOWN_LEGACY_PRODUCTION_EFFECT",
    )
    command = tuple(shlex.split(task.command, posix=False))
    step = WorkflowStepSpec(
        step_id=task.daily_plan_step_id or task.task_id,
        entrypoint=entrypoint,
        expected_artifact_types=expected_artifact_types,
        quality_gate_required=quality_gate_required,
        idempotent=True,
        failure_propagation=FailurePropagation.BLOCK_DEPENDENTS,
        production_effect=production_effect,
        legacy_command=command,
    )
    return WorkflowSpec(
        workflow_id=f"scheduled_task:{task.task_id}",
        owner=owner,
        cadence=cadence,
        timezone=timezone,
        due_policy_id=f"{config.policy_version}:{task.cadence}",
        trading_calendar="XNYS" if cadence is not WorkflowCadence.MANUAL else None,
        steps=(step,),
    )


def report_registry_entry_to_spec(
    entry: Mapping[str, object],
    *,
    canonical_source: EntrypointRef,
    section_provider: EntrypointRef,
    view_model: EntrypointRef,
    renderer: EntrypointRef,
    reader_tier: ReaderTier,
    actionable: bool,
) -> ReportSpec:
    report_id = _required_text(entry, "report_id")
    artifact_globs_raw = entry.get("artifact_globs")
    if not isinstance(artifact_globs_raw, list):
        raise PlatformContractAdapterError(
            "INVALID_REPORT_ARTIFACT_GLOBS", f"{report_id}: artifact_globs"
        )
    freshness = entry.get("freshness_sla_days")
    if not isinstance(freshness, int) or isinstance(freshness, bool):
        raise PlatformContractAdapterError(
            "INVALID_REPORT_FRESHNESS_SLA", f"{report_id}: {freshness!r}"
        )
    cadence = _required_mapping(
        _REPORT_CADENCE_MAP,
        _required_text(entry, "cadence"),
        code="UNKNOWN_REPORT_CADENCE",
    )
    audience = _required_mapping(
        _AUDIENCE_MAP,
        _required_text(entry, "audience"),
        code="UNKNOWN_REPORT_AUDIENCE",
    )
    lifecycle = _required_mapping(
        _VISIBILITY_LIFECYCLE_MAP,
        str(entry.get("visibility_policy") or "current"),
        code="UNKNOWN_REPORT_VISIBILITY_POLICY",
    )
    production_effect = ProductionEffect.parse(
        str(entry.get("production_effect") or ProductionEffect.NONE.value)
    )
    return ReportSpec(
        report_id=report_id,
        title=_required_text(entry, "title"),
        owner=_required_text(entry, "owner"),
        audience=audience,
        reader_tier=reader_tier,
        cadence=cadence,
        canonical_source=canonical_source,
        section_provider=section_provider,
        view_model=view_model,
        renderer=renderer,
        artifact_globs=tuple(str(item) for item in artifact_globs_raw),
        freshness_sla_days=freshness,
        owner_action=_required_text(entry, "owner_action"),
        actionable=actionable,
        lifecycle=lifecycle,
        production_effect=production_effect,
    )


def _required_mapping(mapping: Mapping[str, MappedT], value: str, *, code: str) -> MappedT:
    if value not in mapping:
        raise PlatformContractAdapterError(code, value)
    return mapping[value]


def _required_text(payload: Mapping[str, object], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise PlatformContractAdapterError("REQUIRED_LEGACY_FIELD_EMPTY", field)
    return value
