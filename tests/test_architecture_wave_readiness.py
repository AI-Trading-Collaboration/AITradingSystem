from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture import wave_readiness
from ai_trading_system.platform.architecture.task_registry_shadow import (
    ACTIVE_REGISTER_PATH,
    COMPLETED_REGISTER_PATH,
    SHADOW_FRAGMENT_SCHEMA_VERSION,
    SHADOW_REGISTRY_ROOT,
)
from ai_trading_system.platform.architecture.wave_readiness import (
    POLICY_SCHEMA_VERSION,
    WaveReadinessError,
    assert_worktree_guard,
    calculate_evidence_checksum,
    get_worktree_dirty_paths,
    git_blob_bytes,
    git_blob_sha256,
    git_commit_exists,
    git_commit_tree,
    git_is_ancestor,
    git_resolve_ref,
    load_strict_json_text,
    load_strict_yaml_text,
    validate_wave_readiness_policy,
)

_B = "1" * 40
_C = "2" * 40
_TREE_B = "3" * 40
_TREE_C = "4" * 40
_SHA = "a" * 64


def _manifest(
    *,
    change_id: str,
    task_id: str,
    lane_role: str,
    owned_paths: list[str],
    shared_paths: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "change_manifest.v1",
        "change_id": change_id,
        "task_id": task_id,
        "lane_role": lane_role,
        "base_commit": _C,
        "owner": (
            "architecture_coordinator" if lane_role == "COORDINATOR" else f"{change_id}_owner"
        ),
        "production_effect": "none",
        "owned_paths": owned_paths,
        "shared_paths": shared_paths,
        "module_ids": (
            []
            if lane_role == "COORDINATOR"
            else [f"ai_trading_system.{change_id.replace('-', '_')}"]
        ),
        "contract_claims": [],
        "required_validation_tiers": ["focused"],
    }


def _source_binding(
    binding_id: str,
    *,
    kind: str,
    role: str,
    path: str,
    schema: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    return {
        "binding_id": binding_id,
        "binding_kind": kind,
        "commit_role": role,
        "path": path,
        "blob_sha256": _SHA,
        "schema_version": schema,
        "status": status,
    }


def _task_binding(task_id: str, requirement: str, suffix: str) -> dict[str, Any]:
    fragment = f"{SHADOW_REGISTRY_ROOT}/active/{suffix[:2]}/" f"{suffix * (64 // len(suffix))}.yaml"
    return {
        "task_id": task_id,
        "source_register_path": "docs/task-register-fixture.md",
        "source_register_blob_sha256": _SHA,
        "row_sha256": _SHA,
        "priority": "P0",
        "status": "IN_PROGRESS",
        "shadow_fragment_path": fragment,
        "shadow_fragment_blob_sha256": _SHA,
        "shadow_fragment_schema_version": SHADOW_FRAGMENT_SCHEMA_VERSION,
        "shadow_fragment_checksum": _SHA,
        "requirement_paths": [requirement],
    }


def _policy() -> dict[str, Any]:
    coordinator_path = "docs/shared-integration.md"
    generated_paths = {
        "aggregate": ("inputs/architecture/arch_004e_aggregate_shadow_index.yaml"),
        "baseline": "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "module": "inputs/architecture/arch_004e_module_manifest.yaml",
        "shadow-index": "inputs/architecture/arch_005_task_shadow_index.yaml",
        "test": "inputs/architecture/arch_004e_test_manifest.yaml",
    }
    requirements = {
        "ARCH-004G3_REPORTING_NATIVE_MIGRATION": "docs/requirements/arch-g3.md",
        "ARCH-004W14_D0B2_G3_PARALLEL_READINESS": ("docs/requirements/wave14.md"),
        "DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE": ("docs/requirements/data-gov.md"),
    }
    source_bindings = [
        _source_binding(
            "aggregate",
            kind="GENERATED_STATE",
            role="LANE_BASE",
            path=generated_paths["aggregate"],
            schema="arch_004e_aggregate_shadow_index.v1",
            status="SHADOW_COMPATIBILITY_PASS",
        ),
        _source_binding(
            "baseline",
            kind="GENERATED_STATE",
            role="LANE_BASE",
            path=generated_paths["baseline"],
            schema="arch_005_task_registry_baseline.v1",
            status="PASS",
        ),
        _source_binding(
            "module",
            kind="GENERATED_STATE",
            role="LANE_BASE",
            path=generated_paths["module"],
            schema="arch_004e_module_manifest.v1",
            status="PASS",
        ),
        _source_binding(
            "ownership",
            kind="SOURCE",
            role="LANE_BASE",
            path="config/architecture/devex-ownership.yaml",
        ),
        _source_binding(
            "req-arch-g",
            kind="REQUIREMENT",
            role="LANE_BASE",
            path=requirements["ARCH-004G3_REPORTING_NATIVE_MIGRATION"],
        ),
        _source_binding(
            "req-data",
            kind="REQUIREMENT",
            role="LANE_BASE",
            path=requirements["DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE"],
        ),
        _source_binding(
            "req-wave",
            kind="REQUIREMENT",
            role="LANE_BASE",
            path=requirements["ARCH-004W14_D0B2_G3_PARALLEL_READINESS"],
        ),
        _source_binding(
            "shadow-index",
            kind="GENERATED_STATE",
            role="LANE_BASE",
            path=generated_paths["shadow-index"],
            schema="arch_005_task_shadow_index.v1",
            status="PASS",
        ),
        _source_binding(
            "source-closeout",
            kind="SOURCE",
            role="SOURCE_WAVE",
            path="inputs/architecture/wave13-closeout.json",
            schema="wave13_closeout.v1",
            status="PASS",
        ),
        _source_binding(
            "test",
            kind="GENERATED_STATE",
            role="LANE_BASE",
            path=generated_paths["test"],
            schema="arch_004e_test_manifest.v1",
            status="PASS",
        ),
    ]
    tasks = [
        _task_binding(
            "ARCH-004G3_REPORTING_NATIVE_MIGRATION",
            requirements["ARCH-004G3_REPORTING_NATIVE_MIGRATION"],
            "b",
        ),
        _task_binding(
            "ARCH-004W14_D0B2_G3_PARALLEL_READINESS",
            requirements["ARCH-004W14_D0B2_G3_PARALLEL_READINESS"],
            "c",
        ),
        _task_binding(
            "DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE",
            requirements["DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE"],
            "d",
        ),
    ]
    manifests = [
        _manifest(
            change_id="wave14-coordinator",
            task_id="ARCH-004W14_D0B2_G3_PARALLEL_READINESS",
            lane_role="COORDINATOR",
            owned_paths=["config/architecture/fragments/reports/wave14-integration.yaml"],
            shared_paths=[coordinator_path],
        ),
        _manifest(
            change_id="wave14-d0b2",
            task_id="DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE",
            lane_role="DOMAIN",
            owned_paths=["src/ai_trading_system/data/publication.py"],
            shared_paths=[],
        ),
        _manifest(
            change_id="wave14-g3",
            task_id="ARCH-004G3_REPORTING_NATIVE_MIGRATION",
            lane_role="DOMAIN",
            owned_paths=["src/ai_trading_system/platform/reporting/native.py"],
            shared_paths=[],
        ),
    ]
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "wave": "ARCH-004-WAVE14",
        "gate": "ARCH-004-WAVE14-S0",
        "status": "SCOPE_FROZEN_NOT_DISPATCHED",
        "owner": "architecture_coordinator",
        "source_wave": {
            "wave_id": "ARCH-004-WAVE13",
            "commit": _B,
            "tree_sha1": _TREE_B,
        },
        "lane_base": {
            "commit": _C,
            "tree_sha1": _TREE_C,
            "branch": "main",
            "remote_ref": "origin/main",
        },
        "max_parallel_domain_lanes": 2,
        "selected_domains": [
            {
                "domain_id": "D0B2",
                "change_id": "wave14-d0b2",
                "task_id": "DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE",
            },
            {
                "domain_id": "G3",
                "change_id": "wave14-g3",
                "task_id": "ARCH-004G3_REPORTING_NATIVE_MIGRATION",
            },
        ],
        "source_bindings": source_bindings,
        "task_bindings": tasks,
        "generated_state": {
            "ownership_policy_path": ("config/architecture/devex-ownership.yaml"),
            "module_manifest_path": generated_paths["module"],
            "test_manifest_path": generated_paths["test"],
            "aggregate_index_path": generated_paths["aggregate"],
            "task_baseline_path": generated_paths["baseline"],
            "task_shadow_index_path": generated_paths["shadow-index"],
            "task_fragment_root": SHADOW_REGISTRY_ROOT,
        },
        "change_manifests": manifests,
        "coordinator_only_paths": [coordinator_path],
        "worktree_guard": {
            "known_unrelated_paths": ["docs/research/unrelated.md"],
            "pending_output_paths": [
                "config/architecture/wave14-readiness.yaml",
                "inputs/architecture/wave14-readiness.json",
            ],
        },
        "lease_authority": {
            "kind": "NONE_AT_S0_MANUAL_ASSIGNMENT",
            "lease_namespace_created": False,
            "lease_acquisition_allowed": False,
            "active_shared_path_lease_count": 0,
        },
        "assignment_control": {
            "mode": "MANUAL_COORDINATOR_AFTER_CARRIER_PUSH",
            "authority": "architecture_coordinator",
            "worker_assignment_allowed_after_s0_pass": True,
            "carrier_commit_push_required": True,
            "automatic_command_dispatch": False,
            "automatic_merge": False,
        },
        "safety": {
            "dispatch_allowed": False,
            "automatic_command_dispatch": False,
            "lease_acquisition_allowed": False,
            "automatic_merge_allowed": False,
            "consumer_cutover_allowed": False,
            "task_registry_mutation_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _run_git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repo(root: Path) -> tuple[str, str]:
    _run_git(root, "init", "-b", "main")
    _run_git(root, "config", "user.email", "test@example.invalid")
    _run_git(root, "config", "user.name", "Wave Readiness Test")
    tracked = root / "tracked.txt"
    tracked.write_text("B\n", encoding="utf-8")
    replay_dependency = root / "src/replay_dependency.py"
    replay_dependency.parent.mkdir()
    replay_dependency.write_text("VALUE = 1\n", encoding="utf-8")
    _run_git(root, "add", "tracked.txt", "src/replay_dependency.py")
    _run_git(root, "commit", "-m", "B")
    commit_b = _run_git(root, "rev-parse", "HEAD")
    tracked.write_text("C\n", encoding="utf-8")
    _run_git(root, "commit", "-am", "C")
    return commit_b, _run_git(root, "rev-parse", "HEAD")


def test_policy_schema_accepts_exact_two_domain_manual_plan() -> None:
    validate_wave_readiness_policy(_policy())


@pytest.mark.parametrize(
    ("mutator", "code"),
    [
        (
            lambda policy: policy.update({"unknown": True}),
            "POLICY_FIELDS",
        ),
        (
            lambda policy: policy["lease_authority"].update({"lease_acquisition_allowed": True}),
            "LEASE_AUTHORITY_UNSAFE",
        ),
        (
            lambda policy: policy["assignment_control"].update(
                {"automatic_command_dispatch": True}
            ),
            "ASSIGNMENT_CONTROL_UNSAFE",
        ),
        (
            lambda policy: policy["safety"].update({"consumer_cutover_allowed": True}),
            "SAFETY_BOUNDARY",
        ),
        (
            lambda policy: policy["change_manifests"][2].update(
                {"owned_paths": ["src/ai_trading_system/data/publication.py"]}
            ),
            "LANE_PLAN_SHAPE",
        ),
        (
            lambda policy: policy.update({"status": "DRAFT"}),
            "POLICY_STATUS",
        ),
        (
            lambda policy: policy["lane_base"].update({"remote_ref": "HEAD"}),
            "REMOTE_REF_INVALID",
        ),
        (
            lambda policy: policy["source_wave"].update({"commit": _C}),
            "SOURCE_LANE_COMMIT_IDENTITY",
        ),
        (
            lambda policy: (
                policy["selected_domains"][0].update(
                    {
                        "task_id": "ARCH-004G3_REPORTING_NATIVE_MIGRATION",
                    }
                ),
                policy["selected_domains"][1].update(
                    {
                        "task_id": ("DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE"),
                    }
                ),
            ),
            "SELECTED_CHANGE_TASK_PAIR",
        ),
        (
            lambda policy: policy["worktree_guard"].update(
                {
                    "known_unrelated_paths": [
                        "src/ai_trading_system/platform/architecture/wave_readiness.py"
                    ]
                }
            ),
            "WORKTREE_KNOWN_UNRELATED_FAMILY",
        ),
        (
            lambda policy: policy["worktree_guard"].update(
                {
                    "pending_output_paths": [
                        "inputs/architecture/wave14-readiness.json",
                        "scripts/architecture_wave_readiness.py",
                    ]
                }
            ),
            "WORKTREE_PENDING_OUTPUT_FAMILY",
        ),
        (
            lambda policy: policy["worktree_guard"].update(
                {
                    "pending_output_paths": [
                        "config/architecture/devex_ownership_policy.yaml",
                        "inputs/architecture/wave14-readiness.json",
                    ]
                }
            ),
            "WORKTREE_PENDING_OUTPUT_FAMILY",
        ),
        (
            lambda policy: policy["worktree_guard"].update(
                {
                    "pending_output_paths": [
                        "docs/task_register.md",
                        "inputs/architecture/wave14-readiness.json",
                    ]
                }
            ),
            "WORKTREE_PENDING_OUTPUT_FAMILY",
        ),
        (
            lambda policy: policy["change_manifests"][0]["owned_paths"].append(
                "inputs/architecture/wave14-readiness.json"
            ),
            "WORKTREE_ALLOWLIST_PROTECTED_PATH",
        ),
    ],
)
def test_policy_fails_closed_for_unknown_authority_and_conflict(mutator: Any, code: str) -> None:
    policy = _policy()
    mutator(policy)
    with pytest.raises(WaveReadinessError) as caught:
        validate_wave_readiness_policy(policy)
    assert caught.value.code == code


@pytest.mark.parametrize(
    "text",
    [
        "root:\n  value: 1\n  value: 2\n",
        "value: .nan\n",
        "value: 1e999\n",
    ],
)
def test_strict_yaml_rejects_duplicate_and_non_finite(text: str) -> None:
    with pytest.raises(WaveReadinessError):
        load_strict_yaml_text(text)


def test_strict_yaml_rejects_cyclic_alias_as_typed_error() -> None:
    with pytest.raises(WaveReadinessError) as caught:
        load_strict_yaml_text("root: &root\n  self: *root\n")
    assert caught.value.code == "YAML_CYCLIC_ALIAS"


@pytest.mark.parametrize(
    "text",
    [
        '{"root":{"value":1,"value":2}}',
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":1e999}',
    ],
)
def test_strict_json_rejects_duplicate_and_non_finite(text: str) -> None:
    with pytest.raises(WaveReadinessError):
        load_strict_json_text(text)


def test_checksum_is_canonical_and_tamper_sensitive() -> None:
    first = {"b": [2, 1], "a": {"x": True}}
    second = {"a": {"x": True}, "b": [2, 1]}
    checksum = calculate_evidence_checksum(first)
    assert checksum == calculate_evidence_checksum(second)
    assert checksum != calculate_evidence_checksum({"a": {"x": False}, "b": [2, 1]})
    assert len(checksum) == 64


def test_git_helpers_bind_blob_tree_refs_and_ancestry(tmp_path: Path) -> None:
    commit_b, commit_c = _init_repo(tmp_path)
    assert git_commit_exists(tmp_path, commit_b)
    assert not git_commit_exists(tmp_path, "f" * 40)
    assert git_resolve_ref(tmp_path, "HEAD") == commit_c
    assert len(git_commit_tree(tmp_path, commit_c)) == 40
    assert git_is_ancestor(tmp_path, commit_b, commit_c)
    assert not git_is_ancestor(tmp_path, commit_c, commit_b)
    assert git_blob_bytes(tmp_path, commit_b, "tracked.txt") == b"B\n"
    assert git_blob_sha256(tmp_path, commit_b, "tracked.txt") == hashlib.sha256(b"B\n").hexdigest()


def test_carrier_requires_pushed_proper_descendant_and_tracked_bytes(
    tmp_path: Path,
) -> None:
    commit_b, commit_c = _init_repo(tmp_path)
    _run_git(
        tmp_path,
        "update-ref",
        "refs/remotes/origin/main",
        commit_c,
    )
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
    assert caught.value.code == "CARRIER_COMMIT_REQUIRED"

    policy_path = tmp_path / "config/architecture/wave-readiness.yaml"
    evidence_path = tmp_path / "inputs/architecture/wave-readiness.json"
    policy_path.parent.mkdir(parents=True)
    evidence_path.parent.mkdir(parents=True)
    policy_path.write_text("status: fixture\n", encoding="utf-8")
    evidence_path.write_text('{"status":"fixture"}\n', encoding="utf-8")
    _run_git(tmp_path, "add", "config", "inputs")
    _run_git(tmp_path, "commit", "-m", "D carrier")
    commit_d = _run_git(tmp_path, "rev-parse", "HEAD")
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
    assert caught.value.code == "CARRIER_PUSH_DRIFT"
    _run_git(
        tmp_path,
        "update-ref",
        "refs/remotes/origin/main",
        commit_d,
    )
    assert (
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
        == commit_d
    )
    carrier_paths = {
        "config/architecture/wave-readiness.yaml",
        "inputs/architecture/wave-readiness.json",
    }
    wave_readiness._validate_carrier_diff_scope(
        project_root=tmp_path,
        lane_commit=commit_c,
        head=commit_d,
        allowed_paths=carrier_paths,
    )
    policy_path.write_text("status: local-drift\n", encoding="utf-8")
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._assert_carrier_blob_matches(
            project_root=tmp_path,
            head=commit_d,
            path="config/architecture/wave-readiness.yaml",
            local_bytes=policy_path.read_bytes(),
            label="POLICY",
        )
    assert caught.value.code == "CARRIER_POLICY_BLOB_DRIFT"
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._assert_carrier_blob_matches(
            project_root=tmp_path,
            head=commit_d,
            path="inputs/architecture/untracked-readiness.json",
            local_bytes=b"{}\n",
            label="EVIDENCE",
        )
    assert caught.value.code == "CARRIER_EVIDENCE_UNTRACKED"
    policy_path.write_text("status: fixture\n", encoding="utf-8")
    later_doc = tmp_path / "docs/later.md"
    later_doc.parent.mkdir()
    later_doc.write_text("later unrelated commit\n", encoding="utf-8")
    _run_git(tmp_path, "add", "docs/later.md")
    _run_git(tmp_path, "commit", "-m", "E unrelated descendant")
    commit_e = _run_git(tmp_path, "rev-parse", "HEAD")
    assert (
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
        == commit_e
    )
    assert (
        wave_readiness._locate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            head=commit_e,
        )
        == commit_d
    )
    wave_readiness._validate_carrier_diff_scope(
        project_root=tmp_path,
        lane_commit=commit_c,
        head=commit_d,
        allowed_paths=carrier_paths,
    )
    _run_git(
        tmp_path,
        "update-ref",
        "refs/remotes/origin/main",
        commit_b,
    )
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
    assert caught.value.code == "CARRIER_PUSH_DRIFT"

    _run_git(tmp_path, "checkout", "-b", "remote-divergence", commit_d)
    divergent_doc = tmp_path / "docs/remote-divergence.md"
    divergent_doc.parent.mkdir()
    divergent_doc.write_text("remote-only successor\n", encoding="utf-8")
    _run_git(tmp_path, "add", "docs/remote-divergence.md")
    _run_git(tmp_path, "commit", "-m", "remote divergent successor")
    divergent_remote = _run_git(tmp_path, "rev-parse", "HEAD")
    _run_git(
        tmp_path,
        "update-ref",
        "refs/remotes/origin/main",
        divergent_remote,
    )
    _run_git(tmp_path, "checkout", "main")
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_carrier_commit(
            project_root=tmp_path,
            lane_commit=commit_c,
            remote_ref="origin/main",
        )
    assert caught.value.code == "CARRIER_PUSH_DRIFT"


def test_carrier_replay_binds_dependencies_to_d_not_local_descendant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, commit_c = _init_repo(tmp_path)
    _run_git(tmp_path, "config", "core.autocrlf", "false")
    policy_path = tmp_path / "config/architecture/wave-readiness.yaml"
    evidence_path = tmp_path / "inputs/architecture/wave-readiness.json"
    policy_path.parent.mkdir(parents=True)
    evidence_path.parent.mkdir(parents=True)
    policy_path.write_text("status: fixture\n", encoding="utf-8")
    evidence_bytes = b'{"fixture":"carrier"}\n'
    evidence_path.write_bytes(evidence_bytes)
    _run_git(tmp_path, "add", "config", "inputs")
    _run_git(tmp_path, "commit", "-m", "D carrier")
    commit_d = _run_git(tmp_path, "rev-parse", "HEAD")
    _run_git(
        tmp_path,
        "update-ref",
        "refs/remotes/origin/main",
        commit_d,
    )

    replay_dependency = tmp_path / "src/replay_dependency.py"
    replay_dependency.write_text("VALUE = 2\n", encoding="utf-8")
    _run_git(tmp_path, "commit", "-am", "E local validator descendant")
    commit_e = _run_git(tmp_path, "rev-parse", "HEAD")
    assert git_is_ancestor(tmp_path, commit_d, commit_e)

    policy_portable = "config/architecture/wave-readiness.yaml"
    evidence_portable = "inputs/architecture/wave-readiness.json"
    policy: dict[str, Any] = {
        "lane_base": {
            "commit": commit_c,
            "remote_ref": "origin/main",
        },
        "worktree_guard": {
            "pending_output_paths": [policy_portable, evidence_portable],
        },
    }
    evidence: dict[str, Any] = {
        "policy_binding": {
            "path": policy_portable,
        },
    }
    monkeypatch.setattr(
        wave_readiness,
        "_REPLAY_DEPENDENCY_PATHS",
        frozenset({"src/replay_dependency.py"}),
    )
    monkeypatch.setattr(
        wave_readiness,
        "canonical_evidence_bytes",
        lambda _: evidence_bytes,
    )

    wave_readiness._validate_carrier_state(
        project_root=tmp_path,
        policy=policy,
        policy_path=policy_path,
        evidence_path=evidence_path,
        evidence=evidence,
    )


def test_carrier_dependency_change_in_d_is_rejected(tmp_path: Path) -> None:
    _, commit_c = _init_repo(tmp_path)
    dependency_path = tmp_path / "src/replay_dependency.py"
    dependency_path.write_text("VALUE = 2\n", encoding="utf-8")
    policy_path = tmp_path / "config/architecture/wave-readiness.yaml"
    evidence_path = tmp_path / "inputs/architecture/wave-readiness.json"
    policy_path.parent.mkdir(parents=True)
    evidence_path.parent.mkdir(parents=True)
    policy_path.write_text("status: fixture\n", encoding="utf-8")
    evidence_path.write_text('{"status":"fixture"}\n', encoding="utf-8")
    _run_git(tmp_path, "add", "src", "config", "inputs")
    _run_git(tmp_path, "commit", "-m", "unsafe D dependency change")
    commit_d = _run_git(tmp_path, "rev-parse", "HEAD")
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_carrier_dependency_blobs(
            project_root=tmp_path,
            lane_commit=commit_c,
            head=commit_d,
            dependency_paths=["src/replay_dependency.py"],
        )
    assert caught.value.code == "CARRIER_REPLAY_DEPENDENCY_DRIFT"


def test_worktree_guard_allows_declared_paths_without_hashing_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_repo(tmp_path)
    tracked = tmp_path / "docs/research/unrelated.md"
    tracked.parent.mkdir(parents=True)
    tracked.write_text("original\n", encoding="utf-8")
    _run_git(tmp_path, "add", tracked.relative_to(tmp_path).as_posix())
    _run_git(tmp_path, "commit", "-m", "track unrelated research note")
    tracked.write_text("user bytes that must not be read\n", encoding="utf-8")
    guard = {
        "known_unrelated_paths": ["docs/research/unrelated.md"],
        "pending_output_paths": [
            "config/architecture/wave14-readiness.yaml",
            "inputs/architecture/wave14-readiness.json",
        ],
    }
    assert get_worktree_dirty_paths(tmp_path) == ("docs/research/unrelated.md",)
    git_calls: list[tuple[str, ...]] = []
    original_git_process = wave_readiness._git_process

    def recording_git_process(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
        git_calls.append(args)
        return original_git_process(root, *args)

    monkeypatch.setattr(wave_readiness, "_git_process", recording_git_process)
    result = assert_worktree_guard(project_root=tmp_path, guard=guard)
    assert result["known_unrelated_path_bytes_read"] is False
    assert "sha256" not in json.dumps(result)
    status_call = next(call for call in git_calls if call and call[0] == "status")
    assert "--" in status_call
    assert "." in status_call
    assert ":(top,literal,exclude)docs/research/unrelated.md" in status_call
    (tmp_path / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(WaveReadinessError) as caught:
        assert_worktree_guard(project_root=tmp_path, guard=guard)
    assert caught.value.code == "WORKTREE_UNEXPECTED_PATHS"


def test_worktree_guard_excludes_known_path_as_literal_pathspec(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    known = tmp_path / "docs/research/note[1].md"
    unexpected = tmp_path / "docs/research/note1.md"
    known.parent.mkdir(parents=True)
    known.write_text("known original\n", encoding="utf-8")
    unexpected.write_text("unexpected original\n", encoding="utf-8")
    _run_git(tmp_path, "add", "docs/research/note[1].md", "docs/research/note1.md")
    _run_git(tmp_path, "commit", "-m", "track literal pathspec fixtures")
    known.write_text("known user bytes\n", encoding="utf-8")
    unexpected.write_text("unexpected user bytes\n", encoding="utf-8")

    with pytest.raises(WaveReadinessError) as caught:
        assert_worktree_guard(
            project_root=tmp_path,
            guard={
                "known_unrelated_paths": ["docs/research/note[1].md"],
                "pending_output_paths": [
                    "config/architecture/wave14-readiness.yaml",
                    "inputs/architecture/wave14-readiness.json",
                ],
            },
        )
    assert caught.value.code == "WORKTREE_UNEXPECTED_PATHS"
    assert caught.value.message == "docs/research/note1.md"


def test_policy_input_is_not_mutated_and_validation_is_deterministic() -> None:
    policy = _policy()
    original = copy.deepcopy(policy)
    validate_wave_readiness_policy(policy)
    validate_wave_readiness_policy(policy)
    assert policy == original


def test_commit_snapshot_archives_allowlisted_paths_without_unrelated_document(
    tmp_path: Path,
) -> None:
    _init_repo(tmp_path)
    unrelated = tmp_path / "docs/research/unrelated.md"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("committed bytes that must not enter the snapshot\n", encoding="utf-8")
    _run_git(tmp_path, "add", "docs/research/unrelated.md")
    _run_git(tmp_path, "commit", "-m", "track unrelated research note")
    commit = _run_git(tmp_path, "rev-parse", "HEAD")

    with wave_readiness._git_commit_snapshot(
        tmp_path,
        commit,
        archive_paths=("src",),
        forbidden_paths=("docs/research/unrelated.md",),
    ) as snapshot:
        assert (snapshot / "src/replay_dependency.py").read_text(encoding="utf-8") == (
            "VALUE = 1\n"
        )
        assert not (snapshot / "docs").exists()


def test_snapshot_archive_paths_use_only_replay_consumed_roots(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    policy = _policy()
    ownership_path = tmp_path / str(policy["generated_state"]["ownership_policy_path"])
    ownership_path.parent.mkdir(parents=True)
    ownership_path.write_text(
        "aggregate_targets:\n"
        "  - target_id: fixture\n"
        "    current_path: docs/current.md\n"
        "    fragment_root: config/architecture/fragments/reports\n",
        encoding="utf-8",
    )
    current_path = tmp_path / "docs/current.md"
    current_path.parent.mkdir(parents=True)
    current_path.write_text("current\n", encoding="utf-8")
    _run_git(
        tmp_path,
        "add",
        ownership_path.relative_to(tmp_path).as_posix(),
        current_path.relative_to(tmp_path).as_posix(),
    )
    _run_git(tmp_path, "commit", "-m", "add snapshot ownership fixture")
    lane_commit = _run_git(tmp_path, "rev-parse", "HEAD")

    paths = wave_readiness._snapshot_archive_paths(
        project_root=tmp_path,
        lane_commit=lane_commit,
        policy=policy,
    )

    assert "src/ai_trading_system" in paths
    assert "tests" in paths
    assert "scripts" in paths
    assert "src" not in paths
    assert "config/architecture/fragments" in paths
    assert "config/architecture/fragments/reports" not in paths
    assert "config/architecture/devex-ownership.yaml" in paths
    assert "docs/current.md" in paths
    assert ACTIVE_REGISTER_PATH in paths
    assert COMPLETED_REGISTER_PATH in paths
    assert not any(
        left != right and right.startswith(f"{left}/") for left in paths for right in paths
    )


def test_archive_extraction_rejects_declared_forbidden_path_before_write(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "forbidden.tar"
    with tarfile.open(archive_path, mode="w:") as archive:
        member = tarfile.TarInfo("DOCS/RESEARCH/UNRELATED.MD")
        member.size = 0
        archive.addfile(member)
    destination = tmp_path / "snapshot"
    destination.mkdir()
    with tarfile.open(archive_path, mode="r:") as archive:
        with pytest.raises(WaveReadinessError) as caught:
            wave_readiness._extract_validated_archive(
                archive,
                destination,
                forbidden_paths=("docs/research/unrelated.md",),
            )
    assert caught.value.code == "GIT_ARCHIVE_FORBIDDEN_PATH"
    assert list(destination.iterdir()) == []


def test_snapshot_path_separation_is_case_insensitive() -> None:
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_snapshot_path_separation(
            archive_paths=("DOCS/RESEARCH",),
            excluded_paths=("docs/research/unrelated.md",),
        )
    assert caught.value.code == "GIT_ARCHIVE_EXCLUDED_PATH_COLLISION"


@pytest.mark.parametrize(
    ("name", "member_type", "linkname", "code"),
    [
        ("../escape.txt", tarfile.REGTYPE, "", "PATH_INVALID"),
        (
            "safe-link",
            tarfile.SYMTYPE,
            "../../escape.txt",
            "GIT_ARCHIVE_ENTRY_TYPE",
        ),
        ("device", tarfile.CHRTYPE, "", "GIT_ARCHIVE_ENTRY_TYPE"),
    ],
)
def test_archive_extraction_rejects_unsafe_members_before_write(
    tmp_path: Path,
    name: str,
    member_type: bytes,
    linkname: str,
    code: str,
) -> None:
    archive_path = tmp_path / "unsafe.tar"
    with tarfile.open(archive_path, mode="w:") as archive:
        member = tarfile.TarInfo(name)
        member.type = member_type
        member.linkname = linkname
        member.size = 0
        archive.addfile(member)
    destination = tmp_path / "snapshot"
    destination.mkdir()
    with tarfile.open(archive_path, mode="r:") as archive:
        with pytest.raises(WaveReadinessError) as caught:
            wave_readiness._extract_validated_archive(archive, destination)
    assert caught.value.code == code
    assert list(destination.iterdir()) == []


def test_task_fragment_replay_rejects_orphan_yaml(tmp_path: Path) -> None:
    fragment_root = tmp_path / SHADOW_REGISTRY_ROOT
    expected = fragment_root / "active/aa/expected.yaml"
    orphan = fragment_root / "active/bb/orphan.yaml"
    expected.parent.mkdir(parents=True)
    orphan.parent.mkdir(parents=True)
    expected.write_text("schema_version: fixture\n", encoding="utf-8")
    orphan.write_text("schema_version: orphan\n", encoding="utf-8")
    with pytest.raises(WaveReadinessError) as caught:
        wave_readiness._validate_task_fragment_path_set(
            snapshot_root=tmp_path,
            fragment_root=fragment_root,
            expected_paths={f"{SHADOW_REGISTRY_ROOT}/active/aa/expected.yaml"},
        )
    assert caught.value.code == "TASK_FRAGMENT_PATH_SET"
    assert "orphan.yaml" in caught.value.message
