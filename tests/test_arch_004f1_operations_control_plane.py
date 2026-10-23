from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsContractError,
    OperationsDueContext,
    OperationsDuePolicy,
    OperationsDueResolution,
    OperationsRecoveryRequest,
    OperationsRunDecision,
    OperationsShadowPlan,
    OperationsSourceRetryRequest,
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
from ai_trading_system.platform.artifacts import sha256_path
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
    terminal_recovery: bool = False,
    terminal_recovery_allowed_from_step_ids: tuple[str, ...] = (),
    source_retry_from_step_ids: tuple[str, ...] = (),
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
        terminal_recovery_enabled=terminal_recovery,
        terminal_recovery_requires_release_change=True,
        terminal_recovery_allowed_from_step_ids=(terminal_recovery_allowed_from_step_ids),
        source_retry_enabled=bool(source_retry_from_step_ids),
        source_retry_from_step_ids=source_retry_from_step_ids,
    )


def _daily_env() -> dict[str, str]:
    return {
        "FMP_API_KEY": "present",
        "MARKETSTACK_API_KEY": "present",
        "SEC_USER_AGENT": "AITradingSystem test@example.com",
        "OPENAI_API_KEY": "",
    }


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        (
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=AITS Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            *args,
        ),
        capture_output=True,
        text=True,
        check=True,
        env={
            name: value
            for name, value in os.environ.items()
            if name not in {"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}
        },
    )
    return completed.stdout.strip()


def _runtime_checkout(tmp_path: Path) -> tuple[Path, str]:
    """A clean runtime checkout whose HEAD is origin/main (OPS-082 decision e)."""

    root = tmp_path / "runtime_checkout"
    if (root / ".git").exists():
        return root, _git(root, "rev-parse", "HEAD")
    root.mkdir(parents=True)
    _git(root, "init", "-q")
    (root / "README.md").write_text("runtime release\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "-q", "-m", "release")
    head = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/remotes/origin/main", head)
    return root, head


def _recovery_request(
    tmp_path: Path,
    *,
    parent_run_id: str,
    as_of: date,
    recovery_from_step_id: str = "second",
    parent_release_commit: str = "a" * 40,
) -> OperationsRecoveryRequest:
    manifest_path = tmp_path / "parent_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_run_manifest",
                "run_id": parent_run_id,
                "as_of": as_of.isoformat(),
                "git_commit": parent_release_commit,
                "status": "FAILED",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    runtime_root, runtime_head = _runtime_checkout(tmp_path)
    return OperationsRecoveryRequest(
        parent_run_id=parent_run_id,
        recovery_from_step_id=recovery_from_step_id,
        reason_code="OPS_071_TEST_REMEDIATION",
        parent_manifest_path=str(manifest_path),
        parent_manifest_sha256=sha256_path(manifest_path),
        parent_release_commit=parent_release_commit,
        current_release_commit=runtime_head,
        runtime_checkout_root=str(runtime_root),
        runtime_origin_main_commit=runtime_head,
        requested_at=datetime(2026, 7, 28, tzinfo=UTC),
    )


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
        if step.step_id == "capture_daily_inputs":
            manifest_path = step.produced_paths[0]
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "daily_input_capture_manifest.v1",
                        "as_of": plan.as_of.isoformat(),
                        "status": "CAPTURED",
                        "component_results": [
                            {"component_id": component_id, "status": "PASS"}
                            for component_id in (
                                "market_macro",
                                "fmp_forward_pit",
                                "sec_companyfacts",
                                "fmp_valuation",
                                "official_policy_sources",
                            )
                        ],
                    }
                ),
                encoding="utf-8",
            )
            path = step.produced_paths[2]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": "daily_input_capture_validation.v1",
                        "as_of": plan.as_of.isoformat(),
                        "production_effect": "none",
                        "status": "PASS",
                    }
                ),
                encoding="utf-8",
            )
        for index in indexes.get(step.step_id, ()):
            path = step.produced_paths[index]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Test\n\n- 状态：PASS\n", encoding="utf-8")
        if step.step_id == "report_quality_gate":
            path = step.produced_paths[0]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "report_type": "report_quality_gate",
                        "as_of": plan.as_of.isoformat(),
                        "production_effect": "none",
                        "status": "PASS",
                        "report_quality_status": "PASS",
                    }
                ),
                encoding="utf-8",
            )
        elif step.step_id == "validate_reader_brief":
            path = step.produced_paths[0]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "report_type": "reader_brief_quality",
                        "as_of": plan.as_of.isoformat(),
                        "production_effect": "none",
                        "status": "OK",
                    }
                ),
                encoding="utf-8",
            )


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


def test_periodic_plan_accounts_for_all_27_tasks_and_round_trips() -> None:
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

    assert len(plan.entries) == 27
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
    assert ledger.run_status is CanonicalStatus.PASS
    assert ledger.run_blocker_codes == ()
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
    # No registered ad hoc task carries an unresolved placeholder since the dynamic-v3 rescue
    # tasks retired (GOV-008 P4 block 2), so inject templates that do.
    placeholder_templates = {
        "ad_hoc_sec_pit_diagnostics": "aits sec-pit diagnose-run --run-id {run_id}",
        "ad_hoc_sec_pit_candidate_review": (
            "aits sec-pit review-candidates --candidate-id <candidate_id>"
        ),
    }
    ad_hoc_plan = replace(
        ad_hoc_plan,
        entries=tuple(
            replace(entry, command_template=placeholder_templates[entry.task_id])
            if entry.task_id in placeholder_templates
            else entry
            for entry in ad_hoc_plan.entries
        ),
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
            "ad_hoc_sec_pit_diagnostics",
            "ad_hoc_sec_pit_candidate_review",
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

    assert total == 58


def test_daily_adapter_preserves_order_explicit_dag_and_legacy_commands() -> None:
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
    assert steps[1].dependencies == ()
    assert next(step for step in steps if step.step_id == "validate_data").dependencies == (
        "capture_daily_inputs",
        "download_data",
    )
    assert next(step for step in steps if step.step_id == "score_daily").dependencies == (
        "official_policy_sources",
        "pit_snapshots_validate",
        "sec_metrics_validation",
        "validate_data",
        "valuation_snapshots",
    )
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
    assert payload["workflow_spec"]["semantic_revision"] == (
        "scheduled=scheduled_tasks_v8;capture=daily_input_capture_v7"
    )
    assert plan.workflow_semantic_revision == payload["workflow_spec"]["semantic_revision"]
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
    assert policy.terminal_recovery_enabled is True
    assert policy.terminal_recovery_requires_release_change is True
    assert "artifact_lineage" in policy.terminal_recovery_allowed_from_step_ids


def test_runtime_control_terminal_recovery_archives_parent_and_replays_boundary(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 27)
    now = datetime(2026, 7, 28, tzinfo=UTC)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("second",),
        ),
    )
    spec = _runtime_spec(second_max_attempts=1)
    parent = control.acquire(spec=spec, as_of=as_of, run_id="parent", now=now)
    assert parent.lease is not None
    parent.lease.start_step("first", at=now)
    parent.lease.pass_step("first", at=now)
    parent.lease.start_step("second", at=now)
    parent.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("DAILY_STEP_FAILED:second",),
        at=now,
    )
    state_path = control.root / "states" / f"{parent.resolution.idempotency_key}.json"
    parent_state_bytes = state_path.read_bytes()

    request = _recovery_request(
        tmp_path,
        parent_run_id="parent",
        as_of=as_of,
    )
    recovered = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="child",
        now=now + timedelta(minutes=1),
        recovery_request=request,
    )

    assert recovered.resolution.decision is OperationsRunDecision.RECOVERY
    assert recovered.resolution.resume_completed_step_ids == ("first",)
    assert recovered.resolution.recovery_parent_run_id == "parent"
    assert recovered.resolution.recovery_from_step_id == "second"
    assert recovered.lease is not None
    assert recovered.lease.state.step_attempt_count("second") == 0
    receipt_path = Path(str(recovered.resolution.recovery_receipt_path))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    archive_path = Path(receipt["parent_state_archive_path"])
    assert archive_path.read_bytes() == parent_state_bytes
    assert receipt["parent_state_sha256"] == hashlib.sha256(parent_state_bytes).hexdigest()
    assert receipt["replay_step_ids"] == ["second"]
    assert receipt["reused_completed_step_ids"] == ["first"]

    recovered.lease.start_step("second", at=now + timedelta(minutes=1))
    recovered.lease.pass_step("second", at=now + timedelta(minutes=1))
    recovered.lease.finish(CanonicalStatus.PASS, at=now + timedelta(minutes=1))
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_runtime_control_terminal_failure_requires_explicit_recovery(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 27)
    now = datetime(2026, 7, 28, tzinfo=UTC)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("second",),
        ),
    )
    spec = _runtime_spec(second_max_attempts=1)
    parent = control.acquire(spec=spec, as_of=as_of, run_id="parent", now=now)
    assert parent.lease is not None
    parent.lease.start_step("first", at=now)
    parent.lease.pass_step("first", at=now)
    parent.lease.start_step("second", at=now)
    parent.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("DAILY_STEP_FAILED:second",),
        at=now,
    )

    blocked = control.acquire(spec=spec, as_of=as_of, run_id="child", now=now)

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_TERMINAL_RECOVERY_REQUIRED
    assert blocked.resolution.blocker_codes == ("EXPLICIT_TERMINAL_RECOVERY_REQUEST_REQUIRED",)
    assert blocked.lease is None


def _failed_parent_control(tmp_path: Path) -> tuple[OperationsRunControl, WorkflowSpec]:
    as_of = date(2026, 7, 27)
    now = datetime(2026, 7, 28, tzinfo=UTC)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("second",),
        ),
    )
    spec = _runtime_spec(second_max_attempts=1)
    parent = control.acquire(spec=spec, as_of=as_of, run_id="parent", now=now)
    assert parent.lease is not None
    parent.lease.start_step("first", at=now)
    parent.lease.pass_step("first", at=now)
    parent.lease.start_step("second", at=now)
    parent.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("DAILY_STEP_FAILED:second",),
        at=now,
    )
    return control, spec


@pytest.mark.parametrize(
    ("tamper", "expected_code"),
    [
        pytest.param("untracked", "RECOVERY_RUNTIME_CHECKOUT_DIRTY", id="untracked-file"),
        pytest.param("modified", "RECOVERY_RUNTIME_CHECKOUT_DIRTY", id="modified-file"),
        pytest.param(
            "local_commit",
            "RECOVERY_RUNTIME_CHECKOUT_HEAD_NOT_ON_ORIGIN_MAIN",
            id="head-not-on-origin-main",
        ),
        pytest.param(
            "head_moved", "RECOVERY_CURRENT_RELEASE_MISMATCH", id="head-moved-after-request"
        ),
    ],
)
def test_runtime_control_recovery_rejects_unverifiable_runtime_checkout(
    tmp_path: Path, tamper: str, expected_code: str
) -> None:
    """OPS-082 decision (e) replaces the deployment receipt with the runtime checkout itself.

    Equivalent of the old tampered-receipt test: the release identity the recovery binds to must
    still hold when the run control re-reads it, and it holds only for a clean checkout whose
    HEAD is reachable from origin/main.
    """

    control, spec = _failed_parent_control(tmp_path)
    request = _recovery_request(tmp_path, parent_run_id="parent", as_of=date(2026, 7, 27))
    runtime_root = Path(request.runtime_checkout_root)
    if tamper == "untracked":
        (runtime_root / "stray.txt").write_text("not committed\n", encoding="utf-8")
    elif tamper == "modified":
        (runtime_root / "README.md").write_text("edited in place\n", encoding="utf-8")
    elif tamper == "local_commit":
        (runtime_root / "local.txt").write_text("local only\n", encoding="utf-8")
        _git(runtime_root, "add", "local.txt")
        _git(runtime_root, "commit", "-q", "-m", "local commit not on origin/main")
        request = replace(request, current_release_commit=_git(runtime_root, "rev-parse", "HEAD"))
    else:
        (runtime_root / "next.txt").write_text("next release\n", encoding="utf-8")
        _git(runtime_root, "add", "next.txt")
        _git(runtime_root, "commit", "-q", "-m", "next release")
        _git(runtime_root, "update-ref", "refs/remotes/origin/main", "HEAD")

    blocked = control.acquire(
        spec=spec,
        as_of=date(2026, 7, 27),
        run_id="child",
        now=datetime(2026, 7, 28, tzinfo=UTC),
        recovery_request=request,
    )

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_INVALID_RECOVERY
    assert blocked.resolution.blocker_codes == (expected_code,)
    assert blocked.lease is None
    assert not (control.root / "recovery").exists()


def test_runtime_control_recovery_rejects_non_idempotent_replay(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 27)
    now = datetime(2026, 7, 28, tzinfo=UTC)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("first",),
        ),
    )
    spec = _runtime_spec(first_idempotent=False, second_max_attempts=1)
    parent = control.acquire(spec=spec, as_of=as_of, run_id="parent", now=now)
    assert parent.lease is not None
    parent.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("MANUAL_TERMINAL_FAILURE",),
        at=now,
    )
    request = _recovery_request(
        tmp_path,
        parent_run_id="parent",
        as_of=as_of,
        recovery_from_step_id="first",
    )

    blocked = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="child",
        now=now,
        recovery_request=request,
    )

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_INVALID_RECOVERY
    assert blocked.resolution.blocker_codes == ("RECOVERY_REPLAY_NON_IDEMPOTENT",)
    assert blocked.lease is None


def _source_retry_spec() -> WorkflowSpec:
    def step(step_id: str, *dependencies: str) -> WorkflowStepSpec:
        return WorkflowStepSpec(
            step_id=step_id,
            entrypoint=EntrypointRef(module="tests.fake", callable_name=step_id),
            dependencies=dependencies,
            idempotent=True,
            max_attempts=1,
        )

    return WorkflowSpec(
        workflow_id="synthetic_runtime",
        owner="test",
        cadence=WorkflowCadence.DAILY,
        timezone="UTC",
        steps=(step("capture"), step("other", "capture"), step("report", "other")),
        due_policy_id="test_due_v1",
    )


def _source_retry_request(
    tmp_path: Path,
    *,
    parent_run_id: str = "parent",
    as_of: date = date(2026, 7, 27),
    retry_from_step_id: str = "capture",
) -> OperationsSourceRetryRequest:
    manifest_path = tmp_path / "capture_manifest.json"
    manifest_path.write_text(
        json.dumps({"as_of": as_of.isoformat(), "status": "PARTIAL_CAPTURE"}),
        encoding="utf-8",
    )
    return OperationsSourceRetryRequest(
        parent_run_id=parent_run_id,
        retry_from_step_id=retry_from_step_id,
        reason_code="OPS_085_TEST",
        capture_manifest_path=str(manifest_path),
        capture_manifest_sha256=sha256_path(manifest_path),
        retry_components=(("market_macro", "REQUEST_FAILED"),),
        requested_at=datetime(2026, 7, 28, 8, 30, tzinfo=UTC),
    )


def _failed_source_parent(
    tmp_path: Path,
    *,
    max_run_attempts: int = 2,
    source_retry: bool = True,
) -> tuple[OperationsRunControl, WorkflowSpec]:
    """Capture closed and `other` passed; `report` failed on the missing source."""
    now = datetime(2026, 7, 28, tzinfo=UTC)
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            max_run_attempts=max_run_attempts,
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("report",),
            source_retry_from_step_ids=("capture",) if source_retry else (),
        ),
    )
    spec = _source_retry_spec()
    parent = control.acquire(spec=spec, as_of=date(2026, 7, 27), run_id="parent", now=now)
    assert parent.lease is not None
    for step_id in ("capture", "other"):
        parent.lease.start_step(step_id, at=now)
        parent.lease.pass_step(step_id, at=now)
    parent.lease.start_step("report", at=now)
    parent.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("DAILY_STEP_FAILED:report",),
        at=now,
    )
    return control, spec


def test_runtime_control_source_retry_replays_capture_and_unpassed_steps_once(
    tmp_path: Path,
) -> None:
    """OPS-085: one automatic retry from capture, no release change, parent evidence archived."""
    as_of = date(2026, 7, 27)
    later = datetime(2026, 7, 28, 8, 30, tzinfo=UTC)
    control, spec = _failed_source_parent(tmp_path)
    key = operations_key = control.read_execution_state(spec=spec, as_of=as_of).idempotency_key
    state_path = control.root / "states" / f"{key}.json"
    parent_state_bytes = state_path.read_bytes()

    retried = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="rescue",
        now=later,
        source_retry=_source_retry_request(tmp_path),
    )

    assert retried.resolution.decision is OperationsRunDecision.SOURCE_RETRY
    assert retried.resolution.attempt == 2
    assert retried.resolution.resume_completed_step_ids == ()
    assert retried.resolution.recovery_parent_run_id == "parent"
    assert retried.resolution.recovery_from_step_id == "capture"
    assert retried.lease is not None
    assert all(
        retried.lease.state.step_attempt_count(step_id) == 0
        for step_id in ("capture", "other", "report")
    )
    receipt_path = Path(str(retried.resolution.recovery_receipt_path))
    assert receipt_path.parent.parent.name == operations_key
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["schema_version"] == "operations_source_retry_receipt.v1"
    assert receipt["replay_step_ids"] == ["capture", "other", "report"]
    assert receipt["reused_completed_step_ids"] == []
    assert receipt["owner_decision"].startswith("owner_decision:OPS-085:")
    assert receipt["request"]["retry_components"] == [
        {"component_id": "market_macro", "blocker_code": "REQUEST_FAILED"}
    ]
    assert Path(receipt["parent_state_archive_path"]).read_bytes() == parent_state_bytes

    for step_id in ("capture", "other", "report"):
        retried.lease.start_step(step_id, at=later)
        retried.lease.pass_step(step_id, at=later)
    retried.lease.finish(CanonicalStatus.PASS, at=later)
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "PASS"

    again = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="again",
        now=later,
        source_retry=_source_retry_request(tmp_path, parent_run_id="rescue"),
    )
    assert again.resolution.decision is OperationsRunDecision.ALREADY_COMPLETE


def test_runtime_control_source_retry_happens_at_most_once(tmp_path: Path) -> None:
    as_of = date(2026, 7, 27)
    later = datetime(2026, 7, 28, 8, 30, tzinfo=UTC)
    control, spec = _failed_source_parent(tmp_path, max_run_attempts=3)
    retried = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="rescue",
        now=later,
        source_retry=_source_retry_request(tmp_path),
    )
    assert retried.lease is not None
    retried.lease.start_step("capture", at=later)
    retried.lease.finish(
        CanonicalStatus.FAILED,
        blocker_codes=("DAILY_STEP_FAILED:capture",),
        at=later,
    )

    second = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="second_rescue",
        now=later,
        source_retry=_source_retry_request(tmp_path, parent_run_id="rescue"),
    )
    ordinary = control.acquire(spec=spec, as_of=as_of, run_id="ordinary", now=later)

    assert second.resolution.decision is OperationsRunDecision.BLOCKED_INVALID_RECOVERY
    assert second.resolution.blocker_codes == ("SOURCE_RETRY_ALREADY_USED",)
    assert second.lease is None
    assert ordinary.resolution.decision is (
        OperationsRunDecision.BLOCKED_TERMINAL_RECOVERY_REQUIRED
    )


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        pytest.param("policy_disabled", "SOURCE_RETRY_NOT_ELIGIBLE", id="policy-disabled"),
        pytest.param("parent_mismatch", "SOURCE_RETRY_PARENT_RUN_MISMATCH", id="parent-mismatch"),
        pytest.param("step_not_allowed", "SOURCE_RETRY_FROM_STEP_NOT_ALLOWED", id="other-step"),
        pytest.param(
            "manifest_changed",
            "SOURCE_RETRY_CAPTURE_MANIFEST_INVALID_SHA256_MISMATCH",
            id="manifest-changed",
        ),
        pytest.param("with_recovery", "SOURCE_RETRY_NOT_ELIGIBLE", id="with-recovery-request"),
    ],
)
def test_runtime_control_source_retry_rejects_unbound_requests(
    tmp_path: Path, case: str, expected_code: str
) -> None:
    as_of = date(2026, 7, 27)
    control, spec = _failed_source_parent(tmp_path, source_retry=case != "policy_disabled")
    state_path = (
        control.root
        / "states"
        / f"{control.read_execution_state(spec=spec, as_of=as_of).idempotency_key}.json"
    )
    parent_state_bytes = state_path.read_bytes()
    request = _source_retry_request(
        tmp_path,
        parent_run_id="someone_else" if case == "parent_mismatch" else "parent",
        retry_from_step_id="other" if case == "step_not_allowed" else "capture",
    )
    if case == "manifest_changed":
        Path(request.capture_manifest_path).write_text("{}", encoding="utf-8")
    recovery = (
        _recovery_request(tmp_path, parent_run_id="parent", as_of=as_of)
        if case == "with_recovery"
        else None
    )

    blocked = control.acquire(
        spec=spec,
        as_of=as_of,
        run_id="rescue",
        source_retry=request,
        recovery_request=recovery,
    )

    assert blocked.resolution.decision is OperationsRunDecision.BLOCKED_INVALID_RECOVERY
    assert blocked.resolution.blocker_codes == (expected_code,)
    assert blocked.lease is None
    assert state_path.read_bytes() == parent_state_bytes
    assert not (control.root / "source_retry").exists()


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


def test_runtime_control_policy_revision_creates_new_key_without_mutating_exhausted_state(
    tmp_path: Path,
) -> None:
    control = OperationsRunControl(root=tmp_path, policy=_runtime_policy())
    as_of = date(2026, 7, 24)
    now = datetime(2026, 7, 25, tzinfo=UTC)
    old_spec = replace(
        _runtime_spec(second_max_attempts=1),
        semantic_revision="scheduled=scheduled_tasks_v4;capture=daily_input_capture_v2",
    )
    old = control.acquire(spec=old_spec, as_of=as_of, run_id="old", now=now)
    assert old.lease is not None
    old.lease.start_step("first", at=now)
    old.lease.pass_step("first", at=now)
    old.lease.start_step("second", at=now)
    old.lease.release()

    exhausted = control.acquire(spec=old_spec, as_of=as_of, run_id="old_retry", now=now)
    assert exhausted.resolution.decision is OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED
    old_state_path = tmp_path / "states" / f"{old.resolution.idempotency_key}.json"
    old_state_bytes = old_state_path.read_bytes()

    new_spec = replace(
        old_spec,
        semantic_revision="scheduled=scheduled_tasks_v4;capture=daily_input_capture_v3",
    )
    fresh = control.acquire(spec=new_spec, as_of=as_of, run_id="new", now=now)

    assert fresh.resolution.decision is OperationsRunDecision.START_NEW
    assert fresh.lease is not None
    assert fresh.resolution.idempotency_key != old.resolution.idempotency_key
    assert old_state_path.read_bytes() == old_state_bytes
    assert (tmp_path / "states" / f"{fresh.resolution.idempotency_key}.json").exists()
    fresh.lease.release()


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


def test_controlled_daily_executor_holds_lease_until_completion_callback(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    control_root = tmp_path / "control"
    observed: dict[str, object] = {}

    def runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    def completion_callback(report) -> None:
        state = json.loads(
            _execution_state_path(control_root / "states").read_text(encoding="utf-8")
        )
        observed["report_status"] = report.status
        observed["state_status"] = state["status"]
        observed["lock_count"] = len(tuple((control_root / "locks").glob("*/owner.json")))

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="completion_pass",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=control_root,
        completion_callback=completion_callback,
    )

    state = json.loads(_execution_state_path(control_root / "states").read_text(encoding="utf-8"))
    assert report.status == "PASS"
    assert observed == {
        "report_status": "PASS",
        "state_status": "RUNNING",
        "lock_count": 1,
    }
    assert state["status"] == "PASS"
    assert tuple((control_root / "locks").glob("*/owner.json")) == ()


def test_controlled_daily_executor_finalization_failure_cannot_write_terminal_pass(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    control_root = tmp_path / "control"

    def runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    class FinalizationFailure(RuntimeError):
        code = "FINAL_READER_BRIEF_QUALITY_FAILED"

    def completion_callback(report) -> None:
        raise FinalizationFailure("final Reader Brief quality status=FAILED")

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="completion_failed",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=control_root,
        completion_callback=completion_callback,
    )

    state = json.loads(_execution_state_path(control_root / "states").read_text(encoding="utf-8"))
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(control_root / "states").read_text(encoding="utf-8"))
    )
    assert report.status == "FAIL_FINALIZATION"
    assert report.failed_step is not None
    assert report.failed_step.step_id == "canonical_finalization"
    assert "FINAL_READER_BRIEF_QUALITY_FAILED" in (report.failed_step.error or "")
    assert report.failed_step.diagnostic_path is not None
    assert report.failed_step.diagnostic_path.exists()
    assert state["status"] == "FAILED"
    assert state["blocker_codes"] == ["DAILY_STEP_FAILED:canonical_finalization"]
    assert ledger.run_status is CanonicalStatus.FAILED
    assert ledger.run_blocker_codes == ("DAILY_STEP_FAILED:canonical_finalization",)
    assert all(
        entry.status in {CanonicalStatus.PASS, CanonicalStatus.SKIPPED} for entry in ledger.entries
    )
    assert tuple((control_root / "locks").glob("*/owner.json")) == ()


def test_controlled_daily_executor_compensates_terminal_pass_write_failure(
    tmp_path: Path,
    monkeypatch,
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
    original_write_state = control._write_state
    terminal_failures: list[str] = []

    def fail_pass_state_write(state, spec) -> None:
        if state.status is CanonicalStatus.PASS:
            raise OSError("injected terminal PASS state write failure")
        original_write_state(state, spec)

    class TerminalCompensation(RuntimeError):
        pass

    def terminal_failure_callback(report, exc: Exception) -> None:
        terminal_failures.append(f"{report.status}:{type(exc).__name__}")
        raise TerminalCompensation("bundle/evidence downgraded before propagation")

    monkeypatch.setattr(control, "_write_state", fail_pass_state_write)

    with pytest.raises(TerminalCompensation):
        run_daily_ops_plan_controlled(
            plan,
            project_root=tmp_path,
            env=_daily_env(),
            runner=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout="", stderr=""
            ),
            run_id="terminal_pass_write_failed",
            visibility_check_date=as_of,
            visibility_latest_completed_trading_day=as_of,
            runtime_control=control,
            completion_callback=lambda report: None,
            terminal_failure_callback=terminal_failure_callback,
        )

    state_root = tmp_path / "control" / "states"
    state = json.loads(_execution_state_path(state_root).read_text(encoding="utf-8"))
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert terminal_failures == ["PASS:OSError"]
    assert state["status"] == "FAILED"
    assert state["blocker_codes"] == ["UNHANDLED_DAILY_EXECUTOR_EXCEPTION"]
    assert ledger.run_status is CanonicalStatus.FAILED
    assert ledger.run_blocker_codes == ("UNHANDLED_DAILY_EXECUTOR_EXCEPTION",)
    assert tuple((control.root / "locks").glob("*/owner.json")) == ()


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
    spec = build_daily_schedule_workflow_spec(
        cadence=cadence,
        is_trading_day=True,
        semantic_revision=plan.workflow_semantic_revision,
    )
    prior = control.acquire(spec=spec, as_of=as_of, run_id="prior")
    assert prior.lease is not None
    prior.lease.start_step("capture_daily_inputs")
    prior.lease.pass_step("capture_daily_inputs")
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

    first = next(
        result for result in report.step_results if result.step_id == "capture_daily_inputs"
    )
    assert first.status == "SKIPPED"
    assert "Canonical resume" in str(first.skip_reason)
    assert not any("capture-daily-inputs" in " ".join(command) for command in calls)
    state_root = tmp_path / "control" / "states"
    state_path = _execution_state_path(state_root)
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "PASS"
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert ledger.entry("capture_daily_inputs").status is CanonicalStatus.PASS
    assert all(
        entry.status in {CanonicalStatus.PASS, CanonicalStatus.SKIPPED} for entry in ledger.entries
    )


def test_controlled_daily_executor_recovery_replays_only_report_tail(
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
        policy=_runtime_policy(
            daily_cut_in=True,
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("artifact_lineage",),
        ),
    )

    def failing_lineage_runner(command, **kwargs):
        return subprocess.CompletedProcess(
            command,
            1 if "validate-artifact-lineage" in command else 0,
            stdout="",
            stderr="lineage failed",
        )

    failed = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=failing_lineage_runner,
        run_id="parent",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    assert failed.status == "FAIL"
    assert failed.failed_step is not None
    assert failed.failed_step.step_id == "validate_artifact_lineage"

    request = _recovery_request(
        tmp_path,
        parent_run_id="parent",
        as_of=as_of,
        recovery_from_step_id="artifact_lineage",
    )
    recovery_calls: list[tuple[str, ...]] = []

    def passing_runner(command, **kwargs):
        recovery_calls.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    recovered = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=passing_runner,
        run_id="child",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
        recovery_request=request,
    )

    assert recovered.status == "PASS_WITH_SKIPS"
    assert any("artifact-lineage" in command for command in recovery_calls)
    assert not any("capture-daily-inputs" in command for command in recovery_calls)
    assert not any("score-daily" in command for command in recovery_calls)
    assert any(
        result.step_id == "capture_daily_inputs"
        and str(result.skip_reason).startswith("Canonical recovery")
        for result in recovered.step_results
    )


def test_controlled_daily_executor_source_retry_reruns_capture_after_late_source(
    tmp_path: Path,
) -> None:
    """OPS-085: the 2026-10-12 shape (late VIX, pipeline_health FAIL) recovers at 17:30."""
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    capture_step = next(step for step in plan.steps if step.step_id == "capture_daily_inputs")
    captured_manifest = json.loads(capture_step.produced_paths[0].read_text(encoding="utf-8"))
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            daily_cut_in=True,
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("artifact_lineage",),
            source_retry_from_step_ids=("capture_daily_inputs",),
        ),
    )
    calls: list[str] = []

    def write_capture(*, market_macro_passes: bool) -> None:
        manifest = json.loads(json.dumps(captured_manifest))
        if not market_macro_passes:
            manifest["status"] = "PARTIAL_CAPTURE"
            market = next(
                row
                for row in manifest["component_results"]
                if row["component_id"] == "market_macro"
            )
            market.update(
                status="FAIL",
                blocker_code="REQUEST_FAILED",
                error_summary="Cboe VIX response had no rows for 2026-05-06 to 2026-05-06",
            )
        capture_step.produced_paths[0].write_text(json.dumps(manifest), encoding="utf-8")

    def runner(command, **kwargs):
        text = " ".join(command)
        calls.append(text)
        if "capture-daily-inputs" in text:
            write_capture(market_macro_passes="--rescue-session" in text)
        health_failed = " ops health " in f" {text} " and "--rescue-session" not in " ".join(calls)
        return subprocess.CompletedProcess(command, 1 if health_failed else 0, stdout="", stderr="")

    morning = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="primary",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    assert morning.status == "FAIL"
    assert morning.source_retry is None
    assert any("extract-sec-metrics" in call for call in calls)
    assert not any("score-daily" in call for call in calls)

    calls.clear()
    rescue = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="rescue",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
        source_retry_requested=True,
    )

    assert rescue.status in {"PASS", "PASS_WITH_SKIPS"}
    assert rescue.source_retry == "RETRIED"
    assert any("capture-daily-inputs" in call and "--rescue-session" in call for call in calls)
    assert any("score-daily" in call for call in calls)
    assert any("validate-data" in call for call in calls)
    # Every step after capture reruns on the new manifest; the morning's PIT capture is reused
    # inside capture itself (tests/test_daily_input_capture.py), not fetched again.
    assert any("extract-sec-metrics" in call for call in calls)
    state = json.loads(_execution_state_path(tmp_path / "control" / "states").read_text())
    assert state["status"] == "PASS"
    assert state["attempt"] == 2

    calls.clear()
    third = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="third",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
        source_retry_requested=True,
    )
    assert third.status == "RUN_CONTROL_ALREADY_COMPLETE"
    assert third.source_retry == "NOT_ELIGIBLE:NO_FAILED_RUN"
    assert calls == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        pytest.param("quota", "NOT_ELIGIBLE:COMPONENT_NOT_RETRYABLE:market_macro:", id="quota"),
        pytest.param("pit", "NOT_ELIGIBLE:COMPONENT_NOT_RETRYABLE:fmp_forward_pit:", id="pit"),
        pytest.param("step", "NOT_ELIGIBLE:OTHER_STEP_FAILED:sec_metrics", id="other-step"),
    ],
)
def test_controlled_daily_executor_source_retry_leaves_other_failures_to_people(
    tmp_path: Path, failure: str, expected: str
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    capture_step = next(step for step in plan.steps if step.step_id == "capture_daily_inputs")
    manifest = json.loads(capture_step.produced_paths[0].read_text(encoding="utf-8"))
    manifest["status"] = "PARTIAL_CAPTURE"
    failed_component, blocker = {
        "quota": ("market_macro", "PROVIDER_QUOTA_EXHAUSTED"),
        "pit": ("fmp_forward_pit", "PROVIDER_UNAVAILABLE"),
        "step": ("market_macro", "REQUEST_FAILED"),
    }[failure]
    next(row for row in manifest["component_results"] if row["component_id"] == failed_component)[
        "status"
    ] = "FAIL"
    next(row for row in manifest["component_results"] if row["component_id"] == failed_component)[
        "blocker_code"
    ] = blocker
    capture_step.produced_paths[0].write_text(json.dumps(manifest), encoding="utf-8")
    control = OperationsRunControl(
        root=tmp_path / "control",
        policy=_runtime_policy(
            daily_cut_in=True,
            terminal_recovery=True,
            terminal_recovery_allowed_from_step_ids=("artifact_lineage",),
            source_retry_from_step_ids=("capture_daily_inputs",),
        ),
    )
    calls: list[str] = []

    def runner(command, **kwargs):
        text = " ".join(command)
        calls.append(text)
        failed = failure == "step" and "extract-sec-metrics" in text
        return subprocess.CompletedProcess(command, 1 if failed else 0, stdout="", stderr="")

    run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="primary",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    calls.clear()
    rescue = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="rescue",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
        source_retry_requested=True,
    )

    assert rescue.status == "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED"
    assert str(rescue.source_retry).startswith(expected)
    assert calls == []
    assert not (tmp_path / "control" / "source_retry").exists()


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
        semantic_revision=plan.workflow_semantic_revision,
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


def test_controlled_daily_executor_blocks_rerun_after_capture_attempt_budget_is_exhausted(
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
    exhausted = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=failing_runner,
        run_id="retry",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        runtime_control=control,
    )
    assert failed.status == "FAIL"
    assert exhausted.status == "RUN_CONTROL_BLOCKED_RETRY_EXHAUSTED"
    assert {item for item in (exhausted.failed_step.error or "").split(",")} == {
        "STEP_ATTEMPT_BUDGET_EXHAUSTED:capture_daily_inputs",
        "STEP_ATTEMPT_BUDGET_EXHAUSTED:pipeline_health",
        "STEP_ATTEMPT_BUDGET_EXHAUSTED:secret_hygiene",
    }
    state = json.loads(_execution_state_path(tmp_path / "control" / "states").read_text())
    attempts = {row["step_id"]: row["attempts"] for row in state["step_attempts"]}
    assert attempts["capture_daily_inputs"] == 1


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
    _write_daily_pass_status_artifacts(plan)
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
    assert "capture_daily_inputs" in state["completed_step_ids"]
    assert "pit_snapshots_validate" in state["completed_step_ids"]
    assert "sec_metrics_validation" in state["completed_step_ids"]
    assert "pipeline_health" in state["completed_step_ids"]
    assert "secret_hygiene" in state["completed_step_ids"]
    attempts = {row["step_id"]: row["attempts"] for row in state["step_attempts"]}
    assert attempts["capture_daily_inputs"] == 1
    assert attempts["validate_data"] == 1
    assert attempts["pit_snapshots_build_manifest"] == 1
    assert attempts["sec_metrics"] == 1
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    assert ledger.entry("capture_daily_inputs").status is CanonicalStatus.PASS
    assert ledger.entry("validate_data").status is CanonicalStatus.FAILED
    assert ledger.entry("score_daily").status is CanonicalStatus.SKIPPED


def test_controlled_daily_executor_records_partial_capture_branch_isolation(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 6)
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    _write_daily_pass_status_artifacts(plan)
    capture_step = next(step for step in plan.steps if step.step_id == "capture_daily_inputs")
    manifest = json.loads(capture_step.produced_paths[0].read_text(encoding="utf-8"))
    manifest["status"] = "PARTIAL_CAPTURE"
    next(row for row in manifest["component_results"] if row["component_id"] == "fmp_forward_pit")[
        "status"
    ] = "FAIL"
    capture_step.produced_paths[0].write_text(json.dumps(manifest), encoding="utf-8")
    calls: list[str] = []

    def runner(command, **kwargs):
        calls.append(" ".join(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan_controlled(
        plan,
        project_root=tmp_path,
        env=_daily_env(),
        runner=runner,
        run_id="partial_capture",
        visibility_check_date=as_of,
        visibility_latest_completed_trading_day=as_of,
        run_control_root=tmp_path / "control",
    )

    state_root = tmp_path / "control" / "states"
    state = json.loads(_execution_state_path(state_root).read_text(encoding="utf-8"))
    ledger = RunLedger.from_dict(
        json.loads(_execution_ledger_path(state_root).read_text(encoding="utf-8"))
    )
    result_by_id = {result.step_id: result for result in report.step_results}
    assert report.status == "BLOCKED_DEPENDENCY"
    assert state["status"] == "BLOCKED"
    assert "capture_daily_inputs" in state["completed_step_ids"]
    assert "validate_data" in state["completed_step_ids"]
    assert "sec_metrics_validation" in state["completed_step_ids"]
    assert "pipeline_health" in state["completed_step_ids"]
    assert "pit_snapshots_build_manifest" in state["skipped_step_ids"]
    assert result_by_id["capture_daily_inputs"].status == "LIMITED"
    assert result_by_id["pit_snapshots_build_manifest"].status == "BLOCKED"
    assert result_by_id["score_daily"].status == "BLOCKED"
    assert not any("pit-snapshots build-manifest" in command for command in calls)
    assert any("validate-data" in command for command in calls)
    assert any("extract-sec-metrics" in command for command in calls)
    assert ledger.entry("capture_daily_inputs").status is CanonicalStatus.PASS
    assert ledger.entry("pit_snapshots_build_manifest").status is CanonicalStatus.SKIPPED
    assert ledger.entry("score_daily").status is CanonicalStatus.SKIPPED
    assert ledger.run_status is CanonicalStatus.BLOCKED
