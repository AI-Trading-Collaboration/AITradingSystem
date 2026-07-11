from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsContractError,
    OperationsDueContext,
    OperationsDuePolicy,
    OperationsDueResolution,
    OperationsShadowPlan,
    build_operations_shadow_plan,
    resolve_operations_due,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import WorkflowCadence, WorkflowSpec
from ai_trading_system.legacy.scheduled_tasks_adapter import (
    LegacyScheduledWorkflowBinding,
    assess_daily_shadow_parity,
    assess_scheduled_cadence,
)
from ai_trading_system.ops_daily import (
    build_daily_ops_plan,
    daily_ops_shadow_path_for_plan,
    render_daily_ops_plan,
    write_daily_ops_plan,
    write_daily_ops_shadow_plan,
)
from ai_trading_system.scheduled_tasks import (
    ScheduledCadence,
    ScheduledTask,
    load_scheduled_tasks_config,
)


def _weekly_policy() -> OperationsDuePolicy:
    return OperationsDuePolicy(
        policy_id="weekly_last_completed_trading_day_v1",
        owner="system_operations",
        version="1.0.0",
        cadence=WorkflowCadence.WEEKLY,
        rule=DueRule.PERIOD_END,
        requires_trading_day=True,
        requires_completed_daily=True,
        requires_data_quality=True,
        requires_artifacts=True,
        requires_owner_gate=False,
    )


def test_due_resolution_requires_visible_daily_dq_and_artifact_evidence() -> None:
    resolution = resolve_operations_due(
        workflow_id="scheduled_weekly",
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 10),
            is_trading_day=True,
            is_period_end=True,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
            data_quality_evidence_id="dq_20260710",
            required_artifacts_ready=True,
            source_artifact_ids=("daily_20260710",),
        ),
    )

    assert resolution.status is CanonicalStatus.DUE
    assert resolution.reason_codes == ("DUE_POLICY_SATISFIED",)
    assert OperationsDueResolution.from_dict(resolution.to_dict()) == resolution


def test_due_resolution_fails_closed_when_quality_evidence_id_is_missing() -> None:
    resolution = resolve_operations_due(
        workflow_id="scheduled_weekly",
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 10),
            is_trading_day=True,
            is_period_end=True,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
            required_artifacts_ready=True,
            source_artifact_ids=("daily_20260710",),
        ),
    )

    assert resolution.status is CanonicalStatus.BLOCKED
    assert resolution.reason_codes == ("DATA_QUALITY_EVIDENCE_MISSING",)


def test_periodic_not_due_is_distinct_from_blocked() -> None:
    resolution = resolve_operations_due(
        workflow_id="scheduled_weekly",
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 8),
            is_trading_day=True,
            is_period_end=False,
        ),
    )

    assert resolution.status is CanonicalStatus.NOT_DUE
    assert resolution.reason_codes == ("NOT_PERIOD_END",)


def test_manual_due_requires_trigger_and_owner_decision_evidence() -> None:
    policy = OperationsDuePolicy(
        policy_id="manual_owner_trigger_v1",
        owner="system_operations",
        version="1.0.0",
        cadence=WorkflowCadence.MANUAL,
        rule=DueRule.EXPLICIT_TRIGGER,
        requires_trading_day=False,
        requires_completed_daily=False,
        requires_data_quality=False,
        requires_artifacts=False,
        requires_owner_gate=True,
    )
    missing_owner = resolve_operations_due(
        workflow_id="scheduled_ad_hoc_research",
        policy=policy,
        context=OperationsDueContext(as_of=date(2026, 7, 11), explicit_trigger=True),
    )
    approved = resolve_operations_due(
        workflow_id="scheduled_ad_hoc_research",
        policy=policy,
        context=OperationsDueContext(
            as_of=date(2026, 7, 11),
            explicit_trigger=True,
            owner_gate_approved=True,
            owner_decision_id="owner_decision_1",
        ),
    )

    assert missing_owner.status is CanonicalStatus.BLOCKED
    assert "OWNER_GATE_STATUS_MISSING" in missing_owner.reason_codes
    assert approved.status is CanonicalStatus.DUE


def test_biweekly_due_uses_explicit_anchor_and_interval() -> None:
    policy = OperationsDuePolicy(
        policy_id="biweekly_anchor_v1",
        owner="system_operations",
        version="1.0.0",
        cadence=WorkflowCadence.BIWEEKLY,
        rule=DueRule.INTERVAL_WEEKS,
        requires_trading_day=True,
        requires_completed_daily=False,
        requires_data_quality=False,
        requires_artifacts=False,
        requires_owner_gate=False,
        anchor_date=date(2026, 7, 10),
        interval_weeks=2,
    )

    due = resolve_operations_due(
        workflow_id="scheduled_biweekly",
        policy=policy,
        context=OperationsDueContext(
            as_of=date(2026, 7, 24), is_trading_day=True, is_period_end=True
        ),
    )
    not_due = resolve_operations_due(
        workflow_id="scheduled_biweekly",
        policy=policy,
        context=OperationsDueContext(
            as_of=date(2026, 7, 17), is_trading_day=True, is_period_end=True
        ),
    )

    assert due.status is CanonicalStatus.DUE
    assert not_due.status is CanonicalStatus.NOT_DUE
    assert not_due.reason_codes == ("INTERVAL_NOT_ELAPSED",)


def test_invalid_due_policy_rule_and_cadence_is_rejected() -> None:
    with pytest.raises(OperationsContractError, match="DUE_POLICY_CADENCE_RULE_MISMATCH"):
        OperationsDuePolicy(
            policy_id="bad",
            owner="ops",
            version="1",
            cadence=WorkflowCadence.MONTHLY,
            rule=DueRule.DAILY_TRIGGER,
            requires_trading_day=False,
            requires_completed_daily=False,
            requires_data_quality=False,
            requires_artifacts=False,
            requires_owner_gate=False,
        )


def test_legacy_schedule_requires_explicit_binding() -> None:
    cadence = load_scheduled_tasks_config().cadence("weekly")

    assessment = assess_scheduled_cadence(cadence, binding=None)

    assert assessment.status is CanonicalStatus.BLOCKED
    assert assessment.workflow_spec is None
    assert assessment.blocker_codes == (
        "DUE_POLICY_BINDING_MISSING",
        "OPERATIONS_OWNER_BINDING_MISSING",
        "TIMEZONE_BINDING_MISSING",
    )


def test_all_registered_tasks_are_inventoryable_via_explicit_bindings() -> None:
    config = load_scheduled_tasks_config()
    policies = {
        "daily_trading_day": "daily_trigger_v1",
        "weekly": "weekly_period_end_v1",
        "biweekly": "biweekly_anchor_v1",
        "monthly": "monthly_period_end_v1",
        "ad_hoc_research": "manual_owner_trigger_v1",
    }
    total = 0
    for cadence in config.cadences:
        assessment = assess_scheduled_cadence(
            cadence,
            binding=LegacyScheduledWorkflowBinding(
                owner="system_operations",
                timezone="America/New_York",
                due_policy_id=policies[cadence.cadence_id],
                trading_calendar=("XNYS" if cadence.cadence_id != "ad_hoc_research" else None),
                preserve_sequential_order=cadence.cadence_id == "daily_trading_day",
            ),
        )
        assert assessment.status is CanonicalStatus.PASS
        assert assessment.workflow_spec is not None
        assert (
            WorkflowSpec.from_dict(assessment.workflow_spec.to_dict()) == assessment.workflow_spec
        )
        total += len(assessment.workflow_spec.steps)

    assert total == 78


def test_daily_adapter_preserves_order_and_legacy_commands_without_enabling_runtime() -> None:
    cadence = load_scheduled_tasks_config().cadence("daily_trading_day")
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="daily_trigger_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=True,
        ),
    )
    assert assessment.workflow_spec is not None
    steps = assessment.workflow_spec.steps
    assert [step.step_id for step in steps] == [task.daily_plan_step_id for task in cadence.tasks]
    assert steps[0].dependencies == ()
    assert steps[1].dependencies == (steps[0].step_id,)
    assert " ".join(steps[0].legacy_command) == cadence.tasks[0].command
    assert all(step.entrypoint.callable_name == "dispatch_scheduled_task" for step in steps)


def test_due_resolution_initializes_additive_non_executing_shadow_ledger() -> None:
    cadence = load_scheduled_tasks_config().cadence("weekly")
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="weekly_last_completed_trading_day_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=False,
        ),
    )
    assert assessment.workflow_spec is not None
    resolution = resolve_operations_due(
        workflow_id=assessment.workflow_spec.workflow_id,
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 10),
            is_trading_day=True,
            is_period_end=True,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
            data_quality_evidence_id="dq_20260710",
            required_artifacts_ready=True,
            source_artifact_ids=("daily_20260710",),
        ),
    )

    plan = build_operations_shadow_plan(
        spec=assessment.workflow_spec,
        due_resolution=resolution,
        run_id="shadow_weekly_20260710",
        created_at=datetime(2026, 7, 11, tzinfo=UTC),
    )

    assert plan.execution_enabled is False
    assert {entry.status for entry in plan.run_ledger.entries} == {CanonicalStatus.DUE}
    assert OperationsShadowPlan.from_dict(plan.to_dict()) == plan


def test_blocked_due_resolution_propagates_reasons_to_every_shadow_ledger_step() -> None:
    cadence = load_scheduled_tasks_config().cadence("weekly")
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="weekly_last_completed_trading_day_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=False,
        ),
    )
    assert assessment.workflow_spec is not None
    resolution = resolve_operations_due(
        workflow_id=assessment.workflow_spec.workflow_id,
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 10), is_trading_day=True, is_period_end=True
        ),
    )

    plan = build_operations_shadow_plan(
        spec=assessment.workflow_spec,
        due_resolution=resolution,
        run_id="shadow_weekly_blocked_20260710",
        created_at=datetime(2026, 7, 11, tzinfo=UTC),
    )

    assert resolution.status is CanonicalStatus.BLOCKED
    assert {entry.status for entry in plan.run_ledger.entries} == {CanonicalStatus.BLOCKED}
    assert all(entry.blocker_codes == resolution.reason_codes for entry in plan.run_ledger.entries)


@pytest.mark.parametrize("as_of", [date(2026, 5, 6), date(2026, 5, 7)])
def test_daily_shadow_plan_matches_existing_trading_day_plan(as_of: date) -> None:
    cadence = load_scheduled_tasks_config().cadence("daily_trading_day")
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="daily_trigger_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=True,
            is_trading_day=True,
        ),
    )
    assert assessment.workflow_spec is not None
    legacy_plan = build_daily_ops_plan(as_of=as_of, skip_risk_event_openai_precheck=True)

    parity = assess_daily_shadow_parity(
        cadence=cadence,
        workflow_spec=assessment.workflow_spec,
        observed_step_ids=tuple(step.step_id for step in legacy_plan.steps),
        observed_commands=tuple(step.command for step in legacy_plan.steps),
        observed_enabled=tuple(step.enabled for step in legacy_plan.steps),
        is_trading_day=legacy_plan.market_session.is_trading_day,
    )

    assert parity.status is CanonicalStatus.PASS
    assert parity.blocker_codes == ()
    assert parity.legacy_only_step_ids == ()


@pytest.mark.parametrize("as_of", [date(2026, 5, 9), date(2026, 5, 10)])
def test_daily_shadow_plan_matches_existing_closed_market_plan(as_of: date) -> None:
    cadence = load_scheduled_tasks_config().cadence("daily_trading_day")
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="daily_trigger_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=True,
            is_trading_day=False,
        ),
    )
    assert assessment.workflow_spec is not None
    legacy_plan = build_daily_ops_plan(as_of=as_of, skip_risk_event_openai_precheck=True)

    parity = assess_daily_shadow_parity(
        cadence=cadence,
        workflow_spec=assessment.workflow_spec,
        observed_step_ids=tuple(step.step_id for step in legacy_plan.steps),
        observed_commands=tuple(step.command for step in legacy_plan.steps),
        observed_enabled=tuple(step.enabled for step in legacy_plan.steps),
        is_trading_day=legacy_plan.market_session.is_trading_day,
    )

    assert legacy_plan.market_session.is_trading_day is False
    assert parity.status is CanonicalStatus.PASS
    assert parity.blocker_codes == ()
    assert parity.legacy_only_step_ids == ()


def test_unknown_legacy_production_effect_is_blocked_not_defaulted() -> None:
    task = ScheduledTask(
        task_id="unsafe_unknown",
        title="unknown",
        command="aits unknown",
        cadence="weekly",
        production_effect="mystery_write",
    )
    assessment = assess_scheduled_cadence(
        ScheduledCadence(cadence_id="weekly", description="", tasks=(task,)),
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="weekly_period_end_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=False,
        ),
    )

    assert assessment.status is CanonicalStatus.BLOCKED
    assert assessment.blocker_codes == ("UNKNOWN_LEGACY_PRODUCTION_EFFECT:unsafe_unknown",)
    assert assessment.legacy_production_effects == (("unsafe_unknown", "mystery_write", "BLOCKED"),)


def test_due_resolution_id_detects_tampering() -> None:
    resolution = resolve_operations_due(
        workflow_id="scheduled_weekly",
        policy=_weekly_policy(),
        context=OperationsDueContext(
            as_of=date(2026, 7, 8), is_trading_day=True, is_period_end=False
        ),
    )
    payload = resolution.to_dict()
    payload["reason_codes"] = ["DUE_POLICY_SATISFIED"]

    with pytest.raises(OperationsContractError, match="DUE_RESOLUTION_ID_MISMATCH"):
        OperationsDueResolution.from_dict(payload)


@pytest.mark.parametrize("as_of", [date(2026, 5, 6), date(2026, 5, 10)])
def test_daily_plan_writes_additive_deterministic_non_executing_shadow_sidecar(
    tmp_path: Path, as_of: date
) -> None:
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    markdown_path = tmp_path / f"daily_ops_plan_{as_of.isoformat()}.md"
    expected_markdown = (
        render_daily_ops_plan(plan, env={}).replace("\n", os.linesep).encode("utf-8")
    )

    write_daily_ops_plan(plan, markdown_path, env={})
    shadow_path = daily_ops_shadow_path_for_plan(markdown_path)
    write_daily_ops_shadow_plan(plan, shadow_path)
    first_bytes = shadow_path.read_bytes()
    write_daily_ops_shadow_plan(plan, shadow_path)

    payload = json.loads(first_bytes)
    assert markdown_path.read_bytes() == expected_markdown
    assert shadow_path.read_bytes() == first_bytes
    assert payload["schema_version"] == "daily_operations_shadow.v1"
    assert payload["commands_executed"] is False
    assert payload["non_daily_dispatch_enabled"] is False
    assert payload["production_effect"] == "none"
    assert payload["parity"]["status"] == "PASS"
    assert payload["shadow_plan"]["execution_enabled"] is False
    assert payload["shadow_plan"]["due_resolution"]["status"] == "DUE"
