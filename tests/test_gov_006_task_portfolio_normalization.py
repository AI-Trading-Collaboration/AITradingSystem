from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture.task_portfolio_normalization import (
    AUTHORIZATION_SCOPE,
    DECISION_REVIEW_SCOPE,
    POLICY_SCHEMA_VERSION,
    TaskPortfolioNormalizationError,
    build_normalization_decision_manifest,
    load_normalization_policy,
    validate_normalization_decision_manifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_POLICY_PATH = PROJECT_ROOT / "config/governance/gov_006_wave1_normalization.yaml"


def _row(task_id: str, status: str) -> str:
    return (
        f"|{task_id}|Governance / fixture|P1|{status}|fixture owner|"
        "next step|own acceptance complete|fixture evidence|\n"
    )


def _git(root: Path, *args: str, input_text: str | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        input=input_text,
    )
    return completed.stdout.strip()


def _head(root: Path) -> str:
    return _git(root, "rev-parse", "HEAD")


def _project(tmp_path: Path) -> Path:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "task_register.md").write_text(
        "# Active\n\n"
        "## 当前任务\n\n"
        + _row("TASK-A", "VALIDATING")
        + "\n## 当前推进补充\n\n"
        + _row("TASK-B", "BLOCKED_OWNER_INPUT")
        + "\n## 暂缓任务\n\n"
        + _row("TASK-D", "DEFERRED"),
        encoding="utf-8",
    )
    (docs / "task_register_completed.md").write_text(
        "# Completed\n\n" + _row("TASK-C", "DONE"),
        encoding="utf-8",
    )
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "gov-006-test@example.invalid")
    _git(tmp_path, "config", "user.name", "GOV-006 Test")
    _git(tmp_path, "config", "core.autocrlf", "false")
    _git(tmp_path, "add", "docs/task_register.md", "docs/task_register_completed.md")
    _git(tmp_path, "commit", "-q", "-m", "fixture register baseline")
    return tmp_path


def _policy() -> dict[str, Any]:
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": "gov_006_wave1_normalization",
        "wave_id": "GOV-006-WAVE1-TEST",
        "version": "2.0.0",
        "status": "GOVERNANCE_COORDINATOR_REVIEWED_WAVE1",
        "authorization": {
            "authorized_by": "project_owner",
            "scope": AUTHORIZATION_SCOPE,
            "ref": "owner_instruction_test",
            "exact_decisions_approved": False,
        },
        "decision_review": {
            "reviewer": "governance_coordinator",
            "scope": DECISION_REVIEW_SCOPE,
        },
        "rationale": "fixture decisions are reviewed by the governance coordinator",
        "required_validation": ["task-register-consistency"],
        "decisions": [
            {
                "task_id": "TASK-A",
                "expected_source_status": "VALIDATING",
                "target_status": "DONE",
                "reason_code": "OWN_ACCEPTANCE_COMPLETE",
                "own_acceptance_claim": "fixture implementation complete",
                "successors": [],
                "remaining_work": [],
            },
            {
                "task_id": "TASK-B",
                "expected_source_status": "BLOCKED_OWNER_INPUT",
                "target_status": "DROPPED",
                "reason_code": "SUPERSEDED_BY_TASK_C",
                "own_acceptance_claim": "fixture route closed",
                "successors": [
                    {
                        "task_id": "TASK-C",
                        "evidence_role": "terminal_closure",
                        "expected_source": "completed",
                        "expected_status": ["DONE"],
                    }
                ],
                "remaining_work": ["future evidence requires a new task"],
            },
        ],
    }


def _write_policy(root: Path, policy: dict[str, Any]) -> Path:
    path = root / "config/governance/gov_006_wave1_normalization.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(policy, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _build(root: Path, policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return build_normalization_decision_manifest(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )


def _validate(
    payload: dict[str, Any],
    *,
    root: Path,
    policy: dict[str, Any],
    policy_path: Path,
) -> None:
    validate_normalization_decision_manifest(
        payload,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )


def _refresh_manifest_hashes(payload: dict[str, Any]) -> None:
    material = copy.deepcopy(payload)
    material.pop("manifest_id", None)
    material.pop("manifest_sha256", None)
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    manifest_sha256 = hashlib.sha256(encoded).hexdigest()
    payload["manifest_sha256"] = manifest_sha256
    payload["manifest_id"] = f"gov_006_decision_manifest_{manifest_sha256[:20]}"


def test_builds_deterministic_dry_run_with_bound_policy_and_typed_evidence(
    tmp_path: Path,
) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)

    first = _build(root, policy, policy_path)
    second = _build(root, policy, policy_path)

    assert first == second
    assert first["status"] == "DRY_RUN_READY"
    assert first["decision_count"] == 2
    assert first["source"]["base_commit"] == _head(root)
    assert first["policy"]["raw_sha256"] == hashlib.sha256(policy_path.read_bytes()).hexdigest()
    assert len(first["policy"]["canonical_semantic_sha256"]) == 64
    assert first["policy"]["authorization"]["exact_decisions_approved"] is False
    assert first["policy"]["decision_review"]["reviewer"] == "governance_coordinator"
    assert len(first["manifest_sha256"]) == 64
    assert first["manifest_id"].endswith(first["manifest_sha256"][:20])
    inventory = first["before_inventory"]
    assert inventory["inventory_scope"] == "all_legacy_task_rows"
    assert inventory["active_task_count"] == 3
    assert inventory["active_task_count_formula"] == "1 + 1 + 1 = 3"
    assert [item["category"] for item in inventory["active_section_counts"]] == [
        "main",
        "supplemental",
        "deferred",
    ]
    assert first["apply_boundary"]["automatic_apply_allowed"] is False
    assert first["safety"]["production_effect"] == "none"
    dropped = first["decisions"][1]
    successor = dropped["successor_refs"][0]
    assert successor["task_id"] == "TASK-C"
    assert successor["source"] == "completed"
    assert successor["status"] == "DONE"
    assert successor["evidence_role"] == "terminal_closure"
    assert successor["expected_status"] == ["DONE"]
    assert len(dropped["own_acceptance_refs"][0]["sha256"]) == 64


def test_source_status_drift_fails_closed(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    active = root / "docs/task_register.md"
    active.write_text(
        active.read_text(encoding="utf-8").replace("VALIDATING", "READY"),
        encoding="utf-8",
    )

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == "SOURCE_STATUS_DRIFT"


def test_duplicate_task_across_partitions_fails_closed(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    completed = root / "docs/task_register_completed.md"
    completed.write_text(
        completed.read_text(encoding="utf-8") + _row("TASK-A", "DONE"),
        encoding="utf-8",
    )

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == "DUPLICATE_TASK_ID"


def test_manifest_tamper_and_live_register_drift_fail_closed(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    payload = _build(root, policy, policy_path)
    tampered = copy.deepcopy(payload)
    tampered["decisions"][0]["target_status"] = "DROPPED"

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(tampered, root=root, policy=policy, policy_path=policy_path)
    assert error.value.code == "MANIFEST_SHA256"

    active = root / "docs/task_register.md"
    active.write_text(active.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(payload, root=root, policy=policy, policy_path=policy_path)
    assert error.value.code == "MANIFEST_SOURCE_DRIFT"


def test_policy_raw_and_semantic_drift_are_bound(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    payload = _build(root, policy, policy_path)

    policy_path.write_text(
        policy_path.read_text(encoding="utf-8") + "# raw-byte drift\n",
        encoding="utf-8",
    )
    same_semantics = load_normalization_policy(policy_path)
    raw_drift = _build(root, same_semantics, policy_path)
    assert raw_drift["policy"]["raw_sha256"] != payload["policy"]["raw_sha256"]
    assert (
        raw_drift["policy"]["canonical_semantic_sha256"]
        == payload["policy"]["canonical_semantic_sha256"]
    )
    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(payload, root=root, policy=same_semantics, policy_path=policy_path)
    assert error.value.code == "MANIFEST_SOURCE_DRIFT"

    changed = _policy()
    changed["rationale"] = "semantic policy drift"
    _write_policy(root, changed)
    semantic_drift = _build(root, changed, policy_path)
    assert (
        semantic_drift["policy"]["canonical_semantic_sha256"]
        != payload["policy"]["canonical_semantic_sha256"]
    )
    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(payload, root=root, policy=changed, policy_path=policy_path)
    assert error.value.code == "MANIFEST_SOURCE_DRIFT"


def test_governance_policy_loader_rejects_duplicate_keys(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        "schema_version: gov_006_portfolio_normalization_policy.v2\n"
        "policy_id: first\n"
        "policy_id: second\n",
        encoding="utf-8",
    )

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        load_normalization_policy(path)

    assert error.value.code == "POLICY_DUPLICATE_KEY"


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ({"task_id": "TASK-X"}, "SUCCESSOR_TASK_MISSING"),
        ({"evidence_role": "completed_consumer"}, "SUPERSEDED_WITHOUT_TERMINAL_CLOSURE"),
        ({"expected_source": "active"}, "SUCCESSOR_SOURCE_DRIFT"),
        ({"expected_status": ["DROPPED"]}, "SUCCESSOR_STATUS_DRIFT"),
    ],
)
def test_successor_contract_fails_closed(
    tmp_path: Path, mutation: dict[str, Any], expected_code: str
) -> None:
    root = _project(tmp_path)
    policy = _policy()
    successor = policy["decisions"][1]["successors"][0]
    successor.update(mutation)
    policy_path = _write_policy(root, policy)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == expected_code


def test_done_decision_cannot_hide_remaining_work(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy["decisions"][0]["remaining_work"] = ["not actually complete"]
    policy_path = _write_policy(root, policy)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == "DONE_REMAINING_WORK"


def test_superseded_reason_requires_dropped_target(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy["decisions"][1]["target_status"] = "DONE"
    policy["decisions"][1]["remaining_work"] = []
    policy_path = _write_policy(root, policy)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == "SUPERSEDED_TARGET"


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        ("owner_exact_approval", "POLICY_OWNER_EXACT_DECISION_APPROVAL"),
        ("legacy_approved_by", "SCHEMA_KEYS"),
    ],
)
def test_policy_metadata_schema_rejects_overclaimed_owner_approval(
    tmp_path: Path, mutate: str, expected_code: str
) -> None:
    root = _project(tmp_path)
    policy = _policy()
    if mutate == "owner_exact_approval":
        policy["authorization"]["exact_decisions_approved"] = True
    else:
        policy["approved_by"] = "project_owner"
    policy_path = _write_policy(root, policy)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == expected_code


def test_descendant_commit_validates_but_non_ancestor_fails(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    payload = _build(root, policy, policy_path)
    base_commit = payload["source"]["base_commit"]

    (root / "descendant.txt").write_text("artifact commit\n", encoding="utf-8")
    _git(root, "add", "descendant.txt")
    _git(root, "commit", "-q", "-m", "descendant artifact commit")
    assert _head(root) != base_commit
    _validate(payload, root=root, policy=policy, policy_path=policy_path)

    tree = _git(root, "rev-parse", "HEAD^{tree}")
    unrelated_commit = _git(root, "commit-tree", tree, "-m", "unrelated root")
    _git(root, "checkout", "--detach", "-q", unrelated_commit)
    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(payload, root=root, policy=policy, policy_path=policy_path)
    assert error.value.code == "BASE_COMMIT_NOT_ANCESTOR"


def test_unknown_base_commit_fails_closed_even_with_consistent_manifest_hash(
    tmp_path: Path,
) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy_path = _write_policy(root, policy)
    payload = _build(root, policy, policy_path)
    payload["source"]["base_commit"] = "f" * 40
    _refresh_manifest_hashes(payload)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _validate(payload, root=root, policy=policy, policy_path=policy_path)

    assert error.value.code == "BASE_COMMIT_UNKNOWN"


def test_non_terminal_target_is_rejected(tmp_path: Path) -> None:
    root = _project(tmp_path)
    policy = _policy()
    policy["decisions"][0]["target_status"] = "BASELINE_DONE"
    policy_path = _write_policy(root, policy)

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build(root, policy, policy_path)

    assert error.value.code == "NON_TERMINAL_TARGET"


def test_real_wave1_policy_builds_exactly_thirty_typed_decisions() -> None:
    policy = load_normalization_policy(REAL_POLICY_PATH)

    payload = build_normalization_decision_manifest(
        project_root=PROJECT_ROOT,
        policy=policy,
        policy_path=REAL_POLICY_PATH,
    )

    assert payload["decision_count"] == 30
    decision_ids = {decision["task_id"] for decision in payload["decisions"]}
    assert sum(decision["target_status"] == "DONE" for decision in payload["decisions"]) == 18
    assert sum(decision["target_status"] == "DROPPED" for decision in payload["decisions"]) == 12
    assert "TRADING-1087_EXTERNAL_REQUEST_INCREMENTAL_REFRESH_GUARDRAILS" not in decision_ids
    assert "TRADING-1088_MARKETSTACK_MISSED_DAY_TAIL_CATCH_UP" not in decision_ids
    assert payload["before_inventory"]["inventory_scope"] == "all_legacy_task_rows"
    assert (
        payload["before_inventory"]["active_section_count_sum"]
        == payload["before_inventory"]["active_task_count"]
    )
    assert all(
        {"source", "status", "evidence_role"} <= set(successor)
        for decision in payload["decisions"]
        for successor in decision["successor_refs"]
    )
