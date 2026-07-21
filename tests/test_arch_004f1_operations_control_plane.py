from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsContractError,
    OperationsDueContext,
    OperationsDuePolicy,
    OperationsDueResolution,
    OperationsRunDecision,
    OperationsShadowPlan,
    PeriodicOperationsPlan,
    build_operations_shadow_plan,
    resolve_operations_due,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import (
    EntrypointRef,
    RunLedger,
    WorkflowCadence,
    WorkflowSpec,
    WorkflowStepSpec,
)
from ai_trading_system.legacy.periodic_operations_adapter import (
    build_periodic_operations_plan,
)
from ai_trading_system.legacy.scheduled_tasks_adapter import (
    LegacyScheduledWorkflowBinding,
    assess_daily_shadow_parity,
    assess_scheduled_cadence,
    build_daily_schedule_workflow_spec,
)
from ai_trading_system.ops_daily import (
    build_daily_ops_plan,
    daily_ops_shadow_path_for_plan,
    render_daily_ops_plan,
    run_daily_ops_plan,
    run_daily_ops_plan_controlled,
    write_daily_ops_plan,
    write_daily_ops_shadow_plan,
)
from ai_trading_system.platform.operations import (
    OperationsRunControl,
    OperationsRuntimeControlError,
    OperationsRuntimeControlPolicy,
    PeriodicOperationsControlError,
    build_periodic_due_contexts_from_daily,
    dispatch_periodic_operations_plan,
    load_operations_runtime_control_policy,
    load_periodic_operations_control_policy,
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


def _runtime_policy(
    *,
    max_run_attempts: int = 2,
    daily_cut_in: bool = False,
    non_daily_dispatch: bool = False,
) -> OperationsRuntimeControlPolicy:
    return OperationsRuntimeControlPolicy(
        policy_id="test_runtime_control_v1",
        owner="test",
        version="1.0.0",
        lock_ttl_seconds=60,
        max_run_attempts=max_run_attempts,
        resume_idempotent_steps=True,
        legacy_daily_executor_cut_in_enabled=daily_cut_in,
        non_daily_dispatch_enabled=non_daily_dispatch,
    )


def _daily_env() -> dict[str, str]:
    return {
        "FMP_API_KEY": "present",
        "MARKETSTACK_API_KEY": "present",
        "SEC_USER_AGENT": "AITradingSystem test@example.com",
        "OPENAI_API_KEY": "",
    }


def _write_daily_pass_status_artifacts(plan) -> None:
    indexes = {
        "official_policy_sources": (2,),
        "validate_data": (0,),
        "pit_snapshots_fetch_fmp_forward": (2,),
        "pit_snapshots_build_manifest": (1,),
        "pit_snapshots_validate": (0,),
        "sec_metrics": (0, 2),
        "sec_metrics_validation": (0,),
        "valuation_snapshots": (2, 3),
        "score_daily": (2, 4),
        "pipeline_health": (0,),
        "secret_hygiene": (0,),
    }
    for step in plan.steps:
        for index in indexes.get(step.step_id, ()):
            path = step.produced_paths[index]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Test\n\n- 状态：PASS\n", encoding="utf-8")


def _execution_state_path(state_root: Path) -> Path:
    return next(
        path for path in state_root.glob("*.json") if not path.name.endswith(".run_ledger.json")
    )


def _execution_ledger_path(state_root: Path) -> Path:
    return next(state_root.glob("*.run_ledger.json"))


def _runtime_spec(*, first_idempotent: bool = True, second_max_attempts: int = 1) -> WorkflowSpec:
    first = WorkflowStepSpec(
        step_id="first",
        entrypoint=EntrypointRef(module="tests.fake", callable_name="first"),
        idempotent=first_idempotent,
        lock_key=None if first_idempotent else "synthetic:first",
        max_attempts=1,
    )
    second = WorkflowStepSpec(
        step_id="second",
        entrypoint=EntrypointRef(module="tests.fake", callable_name="second"),
        dependencies=("first",),
        idempotent=True,
        max_attempts=second_max_attempts,
    )
    return WorkflowSpec(
        workflow_id="synthetic_runtime",
        owner="test",
        cadence=WorkflowCadence.DAILY,
        timezone="UTC",
        steps=(first, second),
        due_policy_id="test_due_v1",
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


def test_periodic_policy_covers_all_non_daily_cadences_without_dispatch() -> None:
    policy = load_periodic_operations_control_policy()

    assert policy.policy_id == "periodic_operations_control_v1"
    assert policy.unified_external_trigger == "aits ops daily-run"
    assert policy.automatic_command_dispatch_enabled is False
    assert {item.cadence_id for item in policy.cadence_controls} == {
        "weekly",
        "biweekly",
        "monthly",
        "ad_hoc_research",
    }
    assert all(item.due_policy.requires_completed_daily for item in policy.cadence_controls)
    assert all(item.due_policy.requires_data_quality for item in policy.cadence_controls)
    assert all(item.due_policy.requires_artifacts for item in policy.cadence_controls)
    assert all(item.due_policy.requires_owner_gate for item in policy.cadence_controls)


def test_periodic_plan_accounts_for_all_41_tasks_and_round_trips() -> None:
    as_of = date(2026, 7, 10)
    generated_at = datetime(2026, 7, 11, tzinfo=UTC)
    contexts = build_periodic_due_contexts_from_daily(
        as_of=as_of,
        daily_status=CanonicalStatus.PASS,
        data_quality_status=CanonicalStatus.PASS,
    )

    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=generated_at,
        contexts=contexts,
    )

    assert len(plan.entries) == 41
    assert PeriodicOperationsPlan.from_dict(plan.to_dict()) == plan
    assert plan.automatic_command_dispatch_enabled is False
    assert all(entry.command_executed is False for entry in plan.entries)
    assert all(len(entry.shadow_plan.run_ledger.entries) == 1 for entry in plan.entries)
    assert all(
        entry.shadow_plan.run_ledger.entries[0].step_id == entry.task_id for entry in plan.entries
    )
    statuses = {
        cadence_id: {
            entry.shadow_plan.due_resolution.status
            for entry in plan.entries
            if entry.cadence_id == cadence_id
        }
        for cadence_id in {entry.cadence_id for entry in plan.entries}
    }
    assert statuses["weekly"] == {CanonicalStatus.BLOCKED}
    assert statuses["biweekly"] == {CanonicalStatus.BLOCKED}
    assert statuses["monthly"] == {CanonicalStatus.NOT_DUE}
    assert statuses["ad_hoc_research"] == {CanonicalStatus.NOT_DUE}
    weekly_reasons = next(
        entry.shadow_plan.due_resolution.reason_codes
        for entry in plan.entries
        if entry.cadence_id == "weekly"
    )
    assert "DATA_QUALITY_EVIDENCE_MISSING" in weekly_reasons
    assert "REQUIRED_ARTIFACT_STATUS_MISSING" in weekly_reasons
    assert "OWNER_GATE_STATUS_MISSING" in weekly_reasons


@pytest.mark.parametrize(
    ("as_of", "due_cadences"),
    [
        (date(2026, 7, 2), {"weekly"}),
        (date(2026, 7, 24), {"weekly", "biweekly"}),
        (date(2026, 7, 31), {"weekly", "monthly"}),
    ],
)
def test_periodic_plan_uses_us_market_period_end_and_reviewed_biweekly_anchor(
    as_of: date, due_cadences: set[str]
) -> None:
    contexts = build_periodic_due_contexts_from_daily(
        as_of=as_of,
        daily_status=CanonicalStatus.PASS,
        data_quality_status=CanonicalStatus.PASS,
        data_quality_evidence_id=f"dq:{as_of.isoformat()}",
        required_artifacts_ready=True,
        source_artifact_ids=(f"daily:{as_of.isoformat()}",),
        owner_gate_approved=True,
        owner_decision_id=f"owner:{as_of.isoformat()}",
    )
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
        contexts=contexts,
    )

    statuses = {
        cadence_id: {
            entry.shadow_plan.due_resolution.status
            for entry in plan.entries
            if entry.cadence_id == cadence_id
        }
        for cadence_id in {entry.cadence_id for entry in plan.entries}
    }
    for cadence_id in {"weekly", "biweekly", "monthly"}:
        expected = CanonicalStatus.DUE if cadence_id in due_cadences else CanonicalStatus.NOT_DUE
        assert statuses[cadence_id] == {expected}
    assert statuses["ad_hoc_research"] == {CanonicalStatus.NOT_DUE}


def test_periodic_ad_hoc_requires_explicit_trigger_and_all_evidence() -> None:
    as_of = date(2026, 7, 10)
    contexts = build_periodic_due_contexts_from_daily(
        as_of=as_of,
        daily_status=CanonicalStatus.PASS,
        data_quality_status=CanonicalStatus.PASS,
        data_quality_evidence_id="dq:2026-07-10",
        required_artifacts_ready=True,
        source_artifact_ids=("daily:2026-07-10",),
        owner_gate_approved=True,
        owner_decision_id="owner:explicit-research",
        explicit_trigger=True,
    )
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        contexts=contexts,
    )

    assert {
        entry.shadow_plan.due_resolution.status
        for entry in plan.entries
        if entry.cadence_id == "ad_hoc_research"
    } == {CanonicalStatus.DUE}
    assert all(
        entry.command_executed is False
        for entry in plan.entries
        if entry.cadence_id == "ad_hoc_research"
    )


def test_periodic_manual_dispatch_uses_runtime_control_and_blocks_duplicate(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 10)
    policy = load_periodic_operations_control_policy()
    contexts = build_periodic_due_contexts_from_daily(
        as_of=as_of,
        daily_status=CanonicalStatus.PASS,
        data_quality_status=CanonicalStatus.PASS,
        data_quality_evidence_id="dq:2026-07-10",
        required_artifacts_ready=True,
        source_artifact_ids=("daily:2026-07-10",),
        owner_gate_approved=True,
        owner_decision_id="owner:weekly",
    )
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        contexts=contexts,
        policy=policy,
    )
    control = OperationsRunControl(
        root=tmp_path / "periodic_control",
        policy=_runtime_policy(non_daily_dispatch=True),
    )
    calls: list[tuple[str, ...]] = []

    def runner(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    first = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=("weekly_backtest",),
        control=control,
        policy=policy,
        runner=runner,
        project_root=tmp_path,
        manual_invocation=True,
    )
    duplicate = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=("weekly_backtest",),
        control=control,
        policy=policy,
        runner=runner,
        project_root=tmp_path,
        manual_invocation=True,
    )

    assert first[0].status is CanonicalStatus.PASS
    assert first[0].command == ("aits", "backtest", "--regime", "unified_primary_2021")
    assert duplicate[0].status is CanonicalStatus.PASS
    assert len(calls) == 1
    state_root = tmp_path / "periodic_control" / "states"
    state = json.loads(_execution_state_path(state_root).read_text(encoding="utf-8"))
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert state["status"] == "PASS"
    assert ledger.entry("weekly_backtest").status is CanonicalStatus.PASS


def test_periodic_dispatch_requires_runtime_flag_and_explicit_manual_invocation(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 10)
    policy = load_periodic_operations_control_policy()
    contexts = build_periodic_due_contexts_from_daily(
        as_of=as_of,
        daily_status=CanonicalStatus.PASS,
        data_quality_status=CanonicalStatus.PASS,
        data_quality_evidence_id="dq",
        required_artifacts_ready=True,
        source_artifact_ids=("daily",),
        owner_gate_approved=True,
        owner_decision_id="owner",
    )
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        contexts=contexts,
        policy=policy,
    )

    with pytest.raises(PeriodicOperationsControlError, match="DISPATCH_DISABLED"):
        dispatch_periodic_operations_plan(
            plan,
            selected_task_ids=("weekly_backtest",),
            control=OperationsRunControl(
                root=tmp_path / "disabled",
                policy=_runtime_policy(non_daily_dispatch=False),
            ),
            policy=policy,
            runner=lambda command, **kwargs: None,
            manual_invocation=True,
        )
    with pytest.raises(PeriodicOperationsControlError, match="MANUAL_INVOCATION_REQUIRED"):
        dispatch_periodic_operations_plan(
            plan,
            selected_task_ids=("weekly_backtest",),
            control=OperationsRunControl(
                root=tmp_path / "manual",
                policy=_runtime_policy(non_daily_dispatch=True),
            ),
            policy=policy,
            runner=lambda command, **kwargs: None,
        )


def test_periodic_dispatch_blocks_manual_checkpoint_and_unresolved_placeholders(
    tmp_path: Path,
) -> None:
    policy = load_periodic_operations_control_policy()

    def full_contexts(as_of: date, *, explicit_trigger: bool):
        return build_periodic_due_contexts_from_daily(
            as_of=as_of,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
            data_quality_evidence_id="dq",
            required_artifacts_ready=True,
            source_artifact_ids=("daily",),
            owner_gate_approved=True,
            owner_decision_id="owner",
            explicit_trigger=explicit_trigger,
        )

    monthly_plan = build_periodic_operations_plan(
        as_of=date(2026, 7, 31),
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
        contexts=full_contexts(date(2026, 7, 31), explicit_trigger=False),
        policy=policy,
    )
    ad_hoc_plan = build_periodic_operations_plan(
        as_of=date(2026, 7, 10),
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        contexts=full_contexts(date(2026, 7, 10), explicit_trigger=True),
        policy=policy,
    )
    control = OperationsRunControl(
        root=tmp_path / "periodic",
        policy=_runtime_policy(non_daily_dispatch=True),
    )
    calls: list[tuple[str, ...]] = []

    def runner(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monthly = dispatch_periodic_operations_plan(
        monthly_plan,
        selected_task_ids=("monthly_artifact_catalog_review",),
        control=control,
        policy=policy,
        runner=runner,
        manual_invocation=True,
    )
    ad_hoc = dispatch_periodic_operations_plan(
        ad_hoc_plan,
        selected_task_ids=(
            "ad_hoc_dynamic_v3_rescue_injection_audit",
            "ad_hoc_dynamic_v3_rescue_candidate_attribution",
        ),
        control=control,
        policy=policy,
        runner=runner,
        manual_invocation=True,
    )

    assert monthly[0].status is CanonicalStatus.BLOCKED
    assert monthly[0].blocker_codes == ("COMMAND_PREFIX_NOT_ALLOWED",)
    assert ad_hoc[0].status is CanonicalStatus.BLOCKED
    assert "UNRESOLVED_BRACE_PLACEHOLDER" in ad_hoc[0].blocker_codes
    assert ad_hoc[1].status is CanonicalStatus.BLOCKED
    assert "UNRESOLVED_ANGLE_PLACEHOLDER" in ad_hoc[1].blocker_codes
    assert calls == []


def test_periodic_dispatch_returns_skipped_for_not_due_task_without_acquiring_lock(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 9)
    policy = load_periodic_operations_control_policy()
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 9, tzinfo=UTC),
        contexts=build_periodic_due_contexts_from_daily(
            as_of=as_of,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
        ),
        policy=policy,
    )
    control = OperationsRunControl(
        root=tmp_path / "periodic",
        policy=_runtime_policy(non_daily_dispatch=True),
    )

    result = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=("weekly_backtest",),
        control=control,
        policy=policy,
        runner=lambda command, **kwargs: None,
        manual_invocation=True,
    )

    assert result[0].status is CanonicalStatus.SKIPPED
    assert result[0].blocker_codes == ("NOT_PERIOD_END",)
    assert not (tmp_path / "periodic" / "locks").exists()


def test_periodic_dispatch_failure_is_terminal_and_retry_exhausted(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 10)
    policy = load_periodic_operations_control_policy()
    plan = build_periodic_operations_plan(
        as_of=as_of,
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        contexts=build_periodic_due_contexts_from_daily(
            as_of=as_of,
            daily_status=CanonicalStatus.PASS,
            data_quality_status=CanonicalStatus.PASS,
            data_quality_evidence_id="dq",
            required_artifacts_ready=True,
            source_artifact_ids=("daily",),
            owner_gate_approved=True,
            owner_decision_id="owner",
        ),
        policy=policy,
    )
    control = OperationsRunControl(
        root=tmp_path / "periodic",
        policy=_runtime_policy(non_daily_dispatch=True),
    )

    def failing_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

    failed = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=("weekly_backtest",),
        control=control,
        policy=policy,
        runner=failing_runner,
        manual_invocation=True,
    )
    retry = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=("weekly_backtest",),
        control=control,
        policy=policy,
        runner=failing_runner,
        manual_invocation=True,
    )

    assert failed[0].status is CanonicalStatus.FAILED
    assert retry[0].status is CanonicalStatus.BLOCKED
    assert retry[0].blocker_codes == ("STEP_ATTEMPT_BUDGET_EXHAUSTED:weekly_backtest",)
    state_root = tmp_path / "periodic" / "states"
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert ledger.entry("weekly_backtest").status is CanonicalStatus.FAILED


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
    assert payload["runtime_control_policy"]["policy_id"] == "operations_runtime_control_v1"
    assert payload["runtime_control_policy"]["legacy_daily_executor_cut_in_enabled"] is True
    assert payload["runtime_control_policy"]["non_daily_dispatch_enabled"] is True


def test_runtime_control_policy_enables_daily_and_explicit_non_daily_cut_in() -> None:
    policy = load_operations_runtime_control_policy()

    assert policy.policy_id == "operations_runtime_control_v1"
    assert policy.max_run_attempts == 2
    assert policy.resume_idempotent_steps is True
    assert policy.legacy_daily_executor_cut_in_enabled is True
    assert policy.non_daily_dispatch_enabled is True


def test_runtime_control_blocks_concurrent_workflow_date_acquisition(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)

    first = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    second = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)

    assert first.resolution.decision is OperationsRunDecision.START_NEW
    assert first.lease is not None
    assert second.resolution.decision is OperationsRunDecision.BLOCKED_CONCURRENT
    assert second.lease is None
    first.lease.release()


def test_runtime_control_recovers_only_expired_lock(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)
    first = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert first.lease is not None

    recovered = control.acquire(
        spec=spec,
        as_of=date(2026, 7, 11),
        run_id="run_2",
        now=now + timedelta(seconds=61),
    )

    assert recovered.resolution.decision is OperationsRunDecision.RESUME
    assert recovered.resolution.attempt == 2
    assert recovered.lease is not None
    recovered.lease.release()


def test_runtime_control_completed_idempotency_key_is_not_reexecuted(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)
    acquired = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert acquired.lease is not None
    acquired.lease.start_step("first", at=now)
    acquired.lease.pass_step("first", at=now)
    acquired.lease.start_step("second", at=now)
    acquired.lease.pass_step("second", at=now)
    acquired.lease.finish(CanonicalStatus.PASS, at=now)

    duplicate = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)

    assert duplicate.resolution.decision is OperationsRunDecision.ALREADY_COMPLETE
    assert duplicate.resolution.resume_completed_step_ids == ("first", "second")
    assert duplicate.lease is None


def test_runtime_control_resumes_only_remaining_idempotent_steps(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)
    first = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert first.lease is not None
    first.lease.start_step("first", at=now)
    first.lease.pass_step("first", at=now)
    first.lease.release()

    resumed = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)

    assert resumed.resolution.decision is OperationsRunDecision.RESUME
    assert resumed.resolution.resume_completed_step_ids == ("first",)
    assert resumed.lease is not None
    resumed.lease.start_step("second", at=now)
    resumed.lease.pass_step("second", at=now)
    resumed.lease.finish(CanonicalStatus.PASS, at=now)


def test_runtime_control_blocks_non_idempotent_partial_resume(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec(first_idempotent=False)
    now = datetime(2026, 7, 11, tzinfo=UTC)
    first = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert first.lease is not None
    first.lease.start_step("first", at=now)
    first.lease.pass_step("first", at=now)
    first.lease.release()

    blocked = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_UNSAFE_RESUME
    assert blocked.resolution.blocker_codes == ("NON_IDEMPOTENT_RESUME:first",)
    assert blocked.lease is None


def test_runtime_control_blocks_after_attempt_budget_is_exhausted(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy(max_run_attempts=2))
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)
    first = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert first.lease is not None
    first.lease.release()
    second = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)
    assert second.lease is not None
    second.lease.release()

    blocked = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_3", now=now)

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED
    assert blocked.resolution.blocker_codes == ("RUN_ATTEMPT_BUDGET_EXHAUSTED",)


def test_runtime_control_consumes_step_retry_budget_from_workflow_spec(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec(second_max_attempts=2)
    now = datetime(2026, 7, 11, tzinfo=UTC)
    acquired = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert acquired.lease is not None
    acquired.lease.start_step("first", at=now)
    acquired.lease.pass_step("first", at=now)
    acquired.lease.start_step("second", at=now)

    assert acquired.lease.fail_step("second", retryable=True, blocker_code="TRANSIENT", at=now)
    acquired.lease.start_step("second", at=now)
    acquired.lease.pass_step("second", at=now)
    assert acquired.lease.state.step_attempt_count("second") == 2
    acquired.lease.finish(CanonicalStatus.PASS, at=now)


def test_runtime_control_exhausted_step_attempt_cannot_resume_after_crash(tmp_path: Path) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec(second_max_attempts=1)
    now = datetime(2026, 7, 11, tzinfo=UTC)
    acquired = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert acquired.lease is not None
    acquired.lease.start_step("first", at=now)
    acquired.lease.pass_step("first", at=now)
    acquired.lease.start_step("second", at=now)
    acquired.lease.release()

    blocked = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_2", now=now)

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED
    assert blocked.resolution.blocker_codes == ("STEP_ATTEMPT_BUDGET_EXHAUSTED:second",)


def test_runtime_control_state_writes_are_atomic_and_owner_release_is_enforced(
    tmp_path: Path,
) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    spec = _runtime_spec()
    now = datetime(2026, 7, 11, tzinfo=UTC)
    acquired = control.acquire(spec=spec, as_of=date(2026, 7, 11), run_id="run_1", now=now)
    assert acquired.lease is not None
    state_path = _execution_state_path(tmp_path / "states")
    ledger_path = _execution_ledger_path(tmp_path / "states")
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "RUNNING"
    assert (
        RunLedger.from_dict(json.loads(ledger_path.read_text(encoding="utf-8")))
        .entry("first")
        .status
        is CanonicalStatus.DUE
    )
    assert list(tmp_path.rglob("*.tmp")) == []

    owner_path = next((tmp_path / "locks").glob("*/owner.json"))
    owner_payload = json.loads(owner_path.read_text(encoding="utf-8"))
    owner_payload["owner_run_id"] = "different_owner"
    owner_path.write_text(json.dumps(owner_payload), encoding="utf-8")
    with pytest.raises(OperationsRuntimeControlError, match="LOCK_OWNER_MISMATCH"):
        acquired.lease.release()


@pytest.mark.parametrize("as_of", [date(2026, 5, 6), date(2026, 5, 10)])
def test_controlled_daily_executor_writes_terminal_state_and_blocks_duplicate(
    tmp_path: Path, as_of: date
) -> None:
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    calls: list[tuple[str, ...]] = []

    def runner(command, **kwargs):
        calls.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="controlled_1",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )
    first_call_count = len(calls)
    duplicate = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="controlled_2",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )

    state_root = tmp_path / "control" / "states"
    state_path = _execution_state_path(state_root)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert report.status in {"PASS", "PASS_WITH_SKIPS"}
    assert state["status"] == "PASS"
    assert state["current_step_id"] is None
    assert set(state["completed_step_ids"]) | set(state["skipped_step_ids"]) == {
        step.step_id for step in plan.steps
    }
    expected_ledger_statuses = {
        step_id: CanonicalStatus.PASS for step_id in state["completed_step_ids"]
    } | {step_id: CanonicalStatus.SKIPPED for step_id in state["skipped_step_ids"]}
    assert {entry.step_id: entry.status for entry in ledger.entries} == expected_ledger_statuses
    assert duplicate.status == "RUN_CONTROL_ALREADY_COMPLETE"
    assert len(calls) == first_call_count


def test_controlled_daily_executor_resumes_without_repeating_completed_step(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(daily_cut_in=True),
    )
    cadence = load_scheduled_tasks_config().cadence("daily_trading_day")
    spec = build_daily_schedule_workflow_spec(cadence=cadence, is_trading_day=True)
    prior = control.acquire(spec=spec, as_of=as_of, run_id="prior")
    assert prior.lease is not None
    prior.lease.start_step("download_data")
    prior.lease.pass_step("download_data")
    prior.lease.release()
    calls: list[tuple[str, ...]] = []

    def runner(command, **kwargs):
        calls.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="resumed",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )

    first = next(result for result in report.step_results if result.step_id == "download_data")
    assert first.status == "SKIPPED"
    assert "Canonical resume" in str(first.skip_reason)
    assert not any("download-data" in " ".join(command) for command in calls)
    state_root = tmp_path / "control" / "states"
    state_path = _execution_state_path(state_root)
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "PASS"
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert ledger.entry("download_data").status is CanonicalStatus.PASS
    assert all(
        entry.status in {CanonicalStatus.PASS, CanonicalStatus.SKIPPED} for entry in ledger.entries
    )


def test_controlled_daily_executor_blocks_concurrent_trigger_before_runner(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(daily_cut_in=True),
    )
    spec = build_daily_schedule_workflow_spec(
        cadence=load_scheduled_tasks_config().cadence("daily_trading_day"),
        is_trading_day=True,
    )
    active = control.acquire(spec=spec, as_of=as_of, run_id="active")
    assert active.lease is not None
    calls: list[tuple[str, ...]] = []

    def runner(command, **kwargs):
        calls.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    blocked = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="blocked",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )

    assert blocked.status == "RUN_CONTROL_BLOCKED_CONCURRENT"
    assert calls == []
    active.lease.release()


def test_controlled_daily_executor_allows_one_configured_download_retry_then_blocks(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(daily_cut_in=True),
    )

    def failing_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

    failed = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=failing_runner,
        run_id="failed",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    retry = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=failing_runner,
        run_id="retry",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    exhausted = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=failing_runner,
        run_id="exhausted",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )

    assert failed.status == "FAIL"
    assert retry.status == "FAIL"
    assert exhausted.status == "RUN_CONTROL_BLOCKED_RETRY_EXHAUSTED"
    assert exhausted.failed_step.error == "RUN_ATTEMPT_BUDGET_EXHAUSTED"
    state = json.loads(_execution_state_path(tmp_path / "control" / "states").read_text())
    attempts = {row["step_id"]: row["attempts"] for row in state["step_attempts"]}
    assert attempts["download_data"] == 2


@pytest.mark.parametrize("as_of", [date(2026, 5, 6), date(2026, 5, 10)])
def test_controlled_daily_executor_preserves_legacy_step_result_contract(
    tmp_path: Path, as_of: date
) -> None:
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)

    def runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    common = {
        "project_root": tmp_path,
        "env": _daily_env(),
        "runner": runner,
        "visibility_check_date": as_of,
        "visibility_latest_completed_trading_day": as_of,
    }
    legacy = run_daily_ops_plan(plan, run_id="legacy", **common)
    controlled = run_daily_ops_plan_controlled(
        plan,
        run_id="controlled",
        run_control_root=tmp_path / "control",
        **common,
    )

    def contract(report):
        return (
            report.status,
            tuple(
                (
                    result.step_id,
                    result.status,
                    result.command,
                    result.return_code,
                    result.blocks_downstream,
                    result.skip_reason,
                    result.error,
                )
                for result in report.step_results
            ),
        )

    assert contract(controlled) == contract(legacy)


def test_controlled_daily_executor_keeps_validate_data_fail_closed_boundary(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    calls: list[str] = []

    def runner(command, **kwargs):
        command_text = " ".join(command)
        calls.append(command_text)
        return subprocess.CompletedProcess(
            command,
            1 if "validate-data" in command_text else 0,
            stdout="",
            stderr="data invalid" if "validate-data" in command_text else "",
        )

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="dq_failed",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )

    assert report.status == "FAIL"
    assert report.failed_step is not None
    assert report.failed_step.step_id == "validate_data"
    assert not any("score-daily" in command for command in calls)
    state_root = tmp_path / "control" / "states"
    state_path = _execution_state_path(state_root)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "FAILED"
    assert state["completed_step_ids"] == ["download_data"]
    assert {row["step_id"]: row["attempts"] for row in state["step_attempts"]} == {
        "download_data": 1,
        "validate_data": 1,
    }
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert ledger.entry("download_data").status is CanonicalStatus.PASS
    assert ledger.entry("validate_data").status is CanonicalStatus.FAILED
    assert ledger.entry("score_daily").status is CanonicalStatus.BLOCKED
