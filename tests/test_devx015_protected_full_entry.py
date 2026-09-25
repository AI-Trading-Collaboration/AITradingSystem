"""DEVX-015 owner decision 2026-09-24: restricted worker scope is tests and Full.

When the registered host control declares ``PROTECTED_WORKER_REQUIRED`` the
ordinary Full entry must refuse before consuming the Full claim; only the
installed protected launcher may dispatch. Absent or ``COORDINATOR_JOB`` keeps
the existing coordinator Job model. Registry transport is the existing test seam;
fence, host binding and control-state validation run for real.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_arch_005_integration_publication_fence import TASK_ID, _acquire, _fence
from test_arch_005_integration_publication_fence import publication_checkout as publication_checkout
from test_devx015_workflow_coordination import _anchor_control, _registered_control

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.workflow_coordination import (
    CONTROL_STATE_NAME,
    FULL_EXECUTION_POLICY_KEY,
    full_execution_policy,
)


class _ReadinessReached(RuntimeError):
    """Sentinel: admission passed the protected-launcher gate without a claim."""


def test_full_execution_policy_defaults_to_coordinator_job_and_rejects_unknown() -> None:
    assert full_execution_policy({}) == "COORDINATOR_JOB"
    assert full_execution_policy({FULL_EXECUTION_POLICY_KEY: "COORDINATOR_JOB"}) == (
        "COORDINATOR_JOB"
    )
    assert full_execution_policy({FULL_EXECUTION_POLICY_KEY: "PROTECTED_WORKER_REQUIRED"}) == (
        "PROTECTED_WORKER_REQUIRED"
    )
    with pytest.raises(ParallelControlError, match="WORKFLOW_CONTROL_FULL_EXECUTION_POLICY"):
        full_execution_policy({FULL_EXECUTION_POLICY_KEY: "ANY_WORKER"})


def _declare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, root: Path, policy: str | None):
    fence = _fence(root)
    _repo, control, _policy, state = _registered_control(
        tmp_path, monkeypatch, repository=root, lease_policy=fence.guard.lease_policy
    )
    state["resource_markers"] = {
        "full": fence.policy.exclusive_validation_resource,
        "publication": fence.policy.exclusive_publication_resource,
    }
    if policy is not None:
        state[FULL_EXECUTION_POLICY_KEY] = policy
    locator_path = next(root.glob(".git/aits-workflow-control.v1.json"))
    locator = json.loads(locator_path.read_text())
    locator["resource_markers"] = state["resource_markers"]
    locator_path.write_text(json.dumps(locator), encoding="utf-8")
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    _anchor_control(root, control, monkeypatch)
    return _fence(root)


def test_control_state_rejects_invalid_full_execution_policy(
    publication_checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The registered store resolves and validates control state when the fence
    # is constructed, so an invalid declaration fails before any lease access.
    with pytest.raises(ParallelControlError, match="WORKFLOW_CONTROL_FULL_EXECUTION_POLICY"):
        _declare(tmp_path, monkeypatch, publication_checkout, "ANY_WORKER")


@pytest.mark.parametrize(
    ("policy", "protected", "refused"),
    [
        ("PROTECTED_WORKER_REQUIRED", False, True),
        ("PROTECTED_WORKER_REQUIRED", True, False),
        ("COORDINATOR_JOB", False, False),
        (None, False, False),
    ],
)
def test_ordinary_full_entry_refuses_before_claim_when_worker_protection_declared(
    publication_checkout: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    policy: str | None,
    protected: bool,
    refused: bool,
) -> None:
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        PublicationFenceError,
    )
    from scripts import run_validation_tier as tier

    root = publication_checkout
    fence = _declare(tmp_path, monkeypatch, root, policy)
    acquired = _acquire(fence, root, transaction_id="protected-full-entry")
    transaction = root / str(acquired["transaction_path"])
    for phase in (
        "TASK_SOURCE_PRE_WRITE",
        "GENERATED_REBUILD_PRE",
        "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE",
        "FORMAL_VALIDATION_PRE",
    ):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor="integration-coordinator",
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    before = fence.replay(transaction)
    # Unrelated admission gates around the new check; fence and host binding stay real.
    monkeypatch.setattr(tier, "_full_task_commitment", lambda *_: {"fragment_sha256": "0" * 64})

    def readiness(*_args, **_kwargs):
        raise _ReadinessReached

    monkeypatch.setattr(tier, "check_full_readiness", readiness)
    args = tier.parse_args(["full", "--publication-transaction", str(transaction)])
    call = dict(
        repo_root=root,
        validation_provenance={"task_id": TASK_ID},
        full_run_id="must-not-dispatch",
        protected_launcher=protected,
    )
    if refused:
        with pytest.raises(PublicationFenceError, match="FULL_PROTECTED_LAUNCHER_REQUIRED"):
            tier._validate_publication_transaction_for_full(args, **call)
    else:
        with pytest.raises(_ReadinessReached):
            tier._validate_publication_transaction_for_full(args, **call)
    assert fence.replay(transaction) == before
    assert not (transaction.parent / "full_dispatch_claim.json").exists()
