from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.contracts.deprecation import (
    DeprecationContractError,
    DeprecationRecord,
    RemovalGateEvidence,
    SurfaceLifecycle,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.platform.architecture.deprecation import (
    DeprecationArchitectureError,
    assert_frozen_deprecation_inventory,
    load_deprecation_policy,
    scan_deprecation_inventory,
)

AT = datetime(2026, 7, 11, 5, 0, tzinfo=UTC)


def test_g0_policy_freezes_lifecycle_targets_and_removal_safety() -> None:
    policy = load_deprecation_policy()

    assert policy.policy_id == "arch_004g_deprecation_policy_v1"
    assert policy.states == tuple(SurfaceLifecycle)
    assert len(policy.targets) == 9
    assert len(policy.required_gate_ids) == 12
    assert policy.permanent_dual_track_allowed is False
    assert policy.g0_runtime_removal_allowed is False
    assert policy.artifact_retention_separate_from_code_removal is True
    assert policy.unknown_reachability_is_removal_ready is False
    assert all(item.owner and item.replacement for item in policy.targets)
    assert all(item.compatibility_window and item.sunset_condition for item in policy.targets)
    assert all(item.usage_evidence_ref for item in policy.targets)
    assert all(item.lifecycle is not SurfaceLifecycle.REMOVED for item in policy.targets)
    assert policy.production_effect == "none"
    assert policy.broker_action == "none"


def test_g0_inventory_is_deterministic_and_blocks_every_removal() -> None:
    inventory = scan_deprecation_inventory(load_deprecation_policy())
    surfaces = {item.surface_id: item for item in inventory.surfaces}

    assert inventory.python_module_count == 876
    assert inventory.python_test_file_count == 1114
    assert inventory.direct_writer_baseline_count == 894
    assert inventory.direct_writer_current_count == 858
    assert inventory.direct_writer_violation_count == 0
    assert inventory.legacy_adapter_file_count == 7
    assert inventory.dynamic_strategy_wrapper_file_count == 99
    assert inventory.research_quality_matching_wrapper_count == 48
    assert dict(inventory.lifecycle_counts) == {"ACTIVE": 6, "DEPRECATED": 3}
    assert inventory.removal_ready_count == 0
    assert len(inventory.surfaces) == 9
    assert surfaces["etf_portfolio_cli_god_module"].line_count == 24761
    assert surfaces["etf_portfolio_cli_god_module"].top_level_function_count == 727
    assert surfaces["etf_portfolio_cli_god_module"].cli_command_decorator_count == 688
    assert surfaces["reader_brief_legacy_builder_renderer"].line_count == 29050
    assert surfaces["dynamic_strategy_task_wrappers"].file_count == 99
    assert surfaces["dynamic_strategy_task_wrappers"].line_count == 88315
    assert surfaces["dynamic_strategy_task_wrappers"].top_level_function_count == 2114
    assert all(not item.removal_ready for item in inventory.surfaces)
    assert all(len(item.open_gate_ids) == 12 for item in inventory.surfaces)
    assert_frozen_deprecation_inventory(inventory)


def test_g0_inventory_drift_fails_closed() -> None:
    inventory = scan_deprecation_inventory(load_deprecation_policy())
    drifted = replace(inventory, direct_writer_current_count=892)

    with pytest.raises(DeprecationArchitectureError, match="DEPRECATION_INVENTORY_DRIFT"):
        assert_frozen_deprecation_inventory(drifted)


def test_g0_policy_rejects_missing_replacement(tmp_path: Path) -> None:
    source = Path("config/architecture/arch_004g_deprecation_policy.yaml").read_text(
        encoding="utf-8"
    )
    invalid = source.replace(
        "replacement: ai_trading_system.platform.artifacts",
        'replacement: ""',
        1,
    )
    path = tmp_path / "invalid.yaml"
    path.write_text(invalid, encoding="utf-8")

    with pytest.raises(DeprecationArchitectureError, match="DEPRECATION_TARGET_FIELD_REQUIRED"):
        load_deprecation_policy(path)


def test_deprecation_record_requires_ordered_transition_and_all_removal_gates() -> None:
    active = _record(SurfaceLifecycle.ACTIVE)

    with pytest.raises(DeprecationContractError, match="DEPRECATION_TRANSITION_INVALID"):
        active.transition(SurfaceLifecycle.FROZEN)

    deprecated = active.transition(SurfaceLifecycle.DEPRECATED)
    frozen = deprecated.transition(SurfaceLifecycle.FROZEN)
    with pytest.raises(
        DeprecationContractError,
        match="DEPRECATION_REMOVED_WITH_OPEN_GATES|DEPRECATION_REMOVAL_GATES_OPEN",
    ):
        frozen.transition(SurfaceLifecycle.REMOVED)

    evidence = tuple(
        RemovalGateEvidence(
            gate_id=gate_id,
            status=CanonicalStatus.PASS,
            evidence_refs=(f"artifact:{gate_id}",),
            checked_at=AT,
        )
        for gate_id in frozen.required_gate_ids
    )
    removed = frozen.transition(SurfaceLifecycle.REMOVED, gate_evidence=evidence)

    assert removed.removal_ready is True
    assert removed.open_gate_ids == ()
    assert DeprecationRecord.from_dict(removed.to_dict()) == removed
    assert removed.to_dict()["production_effect"] == "none"


def test_deprecation_record_does_not_accept_blocked_gate_as_removal_ready() -> None:
    frozen = _record(SurfaceLifecycle.FROZEN)
    evidence = (
        RemovalGateEvidence(
            gate_id="replacement_resolved",
            status=CanonicalStatus.PASS,
            evidence_refs=("artifact:replacement",),
            checked_at=AT,
        ),
        RemovalGateEvidence(
            gate_id="owner_signoff",
            status=CanonicalStatus.BLOCKED,
            evidence_refs=("artifact:owner_pending",),
            checked_at=AT,
        ),
    )

    with pytest.raises(DeprecationContractError, match="DEPRECATION_REMOVED_WITH_OPEN_GATES"):
        frozen.transition(SurfaceLifecycle.REMOVED, gate_evidence=evidence)


def _record(lifecycle: SurfaceLifecycle) -> DeprecationRecord:
    return DeprecationRecord(
        surface_id="legacy:example",
        owner="architecture_governance",
        source_path="src/legacy/example.py",
        replacement="ai_trading_system.platform.example",
        lifecycle=lifecycle,
        compatibility_window="through_parity",
        sunset_condition="all_callers_migrated",
        usage_evidence_ref="artifact:reachability",
        required_gate_ids=("replacement_resolved", "owner_signoff"),
    )
