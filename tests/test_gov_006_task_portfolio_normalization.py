from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture.task_portfolio_normalization import (
    APPLIED_CLOSEOUT_SCHEMA_VERSION,
    AUTHORIZATION_SCOPE,
    DECISION_REVIEW_SCOPE,
    POLICY_SCHEMA_VERSION,
    TaskPortfolioNormalizationError,
    build_normalization_applied_closeout,
    build_normalization_decision_manifest,
    load_normalization_policy,
    validate_historical_normalization_decision_manifest,
    validate_normalization_applied_closeout,
    validate_normalization_decision_manifest,
)
from scripts import governance_task_portfolio_normalization as normalization_control

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_POLICY_PATH = PROJECT_ROOT / "config/governance/gov_006_wave1_normalization.yaml"
REAL_MANIFEST_PATH = PROJECT_ROOT / "inputs/governance/gov_006_wave1_decision_manifest.json"
REAL_APPLIED_CLOSEOUT_PATH = PROJECT_ROOT / "inputs/governance/gov_006_wave1_applied_closeout.json"


def _row(task_id: str, status: str) -> str:
    return (
        f"|{task_id}|Governance / fixture|P1|{status}|fixture owner|"
        "next step|own acceptance complete|fixture evidence|\n"
    )


def _applied_fixture_row(
    *,
    task_id: str,
    actual_status: str,
    target_status: str,
    reason_code: str,
    manifest_id: str,
    mutation: str = "none",
) -> str:
    suffix_reason = f"{reason_code}_TAMPERED" if mutation == "audit_reason" else reason_code
    suffix_manifest = (
        "gov_006_decision_manifest_tampered" if mutation == "audit_manifest" else manifest_id
    )
    suffix_effect = "enabled" if mutation == "audit_effect" else "none"
    suffix = (
        f" 2026-07-23: GOV-006 N1 coordinator review 后按 {suffix_reason} "
        f"转 {target_status} 并归档；manifest={suffix_manifest}；"
        f"production_effect={suffix_effect}。"
    )
    if mutation == "audit_duplicate":
        suffix += suffix
    line = _row(task_id, actual_status)
    if mutation == "rewritten_field":
        line = line.replace(
            "own acceptance complete",
            "rewritten acceptance",
        )
    return f"{line[:-2]}{suffix}|\n"


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
    docs.mkdir(parents=True)
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


def _policy(*, wave_id: str = "GOV-006-WAVE1-TEST") -> dict[str, Any]:
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": "gov_006_wave1_normalization",
        "wave_id": wave_id,
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


def _prepare_applied_project(
    tmp_path: Path,
    *,
    mutation: str = "none",
    wave_id: str = "GOV-006-WAVE1-TEST",
) -> tuple[Path, dict[str, Any], Path, Path, str, dict[str, Any]]:
    root = _project(tmp_path)
    policy = _policy(wave_id=wave_id)
    policy_path = _write_policy(root, policy)
    _git(root, "add", str(policy_path.relative_to(root)))
    _git(root, "commit", "-q", "-m", "freeze normalization policy")
    dry_run = _build(root, policy, policy_path)
    manifest_path = root / "inputs/governance/gov_006_wave1_decision_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(dry_run, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _git(root, "add", str(manifest_path.relative_to(root)))
    _git(root, "commit", "-q", "-m", "freeze dry-run decision manifest")

    active_path = root / "docs/task_register.md"
    completed_path = root / "docs/task_register_completed.md"
    active_lines = active_path.read_text(encoding="utf-8").splitlines(keepends=True)
    moved_ids = {"TASK-A", "TASK-B"}
    if mutation == "missing_move":
        moved_ids.remove("TASK-B")
    rendered_active_lines: list[str] = []
    for line in active_lines:
        moved_task = next(
            (task_id for task_id in moved_ids if line.startswith(f"|{task_id}|")),
            None,
        )
        if moved_task is None:
            rendered_active_lines.append(line)
            continue
        if mutation == "physical_line_removal":
            continue
        line_ending = "\n" if line.endswith("\n") else ""
        if mutation == "nonblank_vacated" and moved_task == "TASK-A":
            rendered_active_lines.append(f"<!-- GOV-006 vacated -->{line_ending}")
        else:
            rendered_active_lines.append(line_ending)
    active_text = "".join(rendered_active_lines)
    if mutation == "untargeted_status":
        active_text = active_text.replace(
            _row("TASK-D", "DEFERRED"),
            _row("TASK-D", "READY"),
        )
    if mutation == "task_set":
        active_text = active_text.replace(_row("TASK-D", "DEFERRED"), "")
    active_path.write_text(active_text, encoding="utf-8")

    completed_text = completed_path.read_text(encoding="utf-8")
    decision_by_id = {str(decision["task_id"]): decision for decision in dry_run["decisions"]}
    first_decision = decision_by_id["TASK-A"]
    first_mutation = (
        mutation
        if mutation
        in {
            "audit_reason",
            "audit_manifest",
            "audit_effect",
            "audit_duplicate",
            "rewritten_field",
        }
        else "none"
    )
    completed_text += _applied_fixture_row(
        task_id="TASK-A",
        actual_status="DONE",
        target_status=str(first_decision["target_status"]),
        reason_code=str(first_decision["reason_code"]),
        manifest_id=str(dry_run["manifest_id"]),
        mutation=first_mutation,
    )
    if mutation != "missing_move":
        second_decision = decision_by_id["TASK-B"]
        completed_text += _applied_fixture_row(
            task_id="TASK-B",
            actual_status=("DONE" if mutation == "wrong_target_status" else "DROPPED"),
            target_status=str(second_decision["target_status"]),
            reason_code=str(second_decision["reason_code"]),
            manifest_id=str(dry_run["manifest_id"]),
        )
    if mutation == "duplicate":
        completed_text += _row("TASK-C", "DONE")
    completed_path.write_text(completed_text, encoding="utf-8")
    _git(
        root,
        "add",
        str(active_path.relative_to(root)),
        str(completed_path.relative_to(root)),
    )
    _git(root, "commit", "-q", "-m", "apply reviewed terminal decisions")
    return root, policy, policy_path, manifest_path, _head(root), dry_run


def _build_applied(
    *,
    root: Path,
    policy: dict[str, Any],
    policy_path: Path,
    manifest_path: Path,
    application_commit: str,
) -> dict[str, Any]:
    return build_normalization_applied_closeout(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        decision_manifest_path=manifest_path,
        application_commit=application_commit,
    )


def _validate_applied(
    payload: dict[str, Any],
    *,
    root: Path,
    policy: dict[str, Any],
    policy_path: Path,
    manifest_path: Path,
) -> None:
    validate_normalization_applied_closeout(
        payload,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        decision_manifest_path=manifest_path,
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


def _refresh_applied_hashes(payload: dict[str, Any]) -> None:
    material = copy.deepcopy(payload)
    material.pop("closeout_id", None)
    material.pop("closeout_sha256", None)
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    closeout_sha256 = hashlib.sha256(encoded).hexdigest()
    payload["closeout_sha256"] = closeout_sha256
    payload["closeout_id"] = f"gov_006_applied_closeout_{closeout_sha256[:20]}"


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


def test_applied_closeout_replays_historical_dry_run_and_binds_application_commit(
    tmp_path: Path,
) -> None:
    root, policy, policy_path, manifest_path, application_commit, dry_run = (
        _prepare_applied_project(tmp_path)
    )

    validate_historical_normalization_decision_manifest(
        dry_run,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )
    with pytest.raises(TaskPortfolioNormalizationError) as live_error:
        _validate(
            dry_run,
            root=root,
            policy=policy,
            policy_path=policy_path,
        )
    assert live_error.value.code == "DECISION_TASK_NOT_ACTIVE"

    closeout = _build_applied(
        root=root,
        policy=policy,
        policy_path=policy_path,
        manifest_path=manifest_path,
        application_commit=application_commit,
    )
    _validate_applied(
        closeout,
        root=root,
        policy=policy,
        policy_path=policy_path,
        manifest_path=manifest_path,
    )

    assert closeout["schema_version"] == APPLIED_CLOSEOUT_SCHEMA_VERSION
    assert closeout["historical_dry_run"]["commit_bound_replay"] == "PASS"
    assert closeout["lineage"]["application_commit"] == application_commit
    assert closeout["before_inventory"]["active_task_count"] == 3
    assert closeout["before_inventory"]["completed_task_count"] == 1
    assert closeout["after_inventory"]["active_task_count"] == 1
    assert closeout["after_inventory"]["completed_task_count"] == 3
    assert closeout["after_inventory"]["total_task_count"] == 4
    assert closeout["before_inventory"]["completed_status_counts"] == {"DONE": 1}
    assert closeout["after_inventory"]["completed_status_counts"] == {
        "DONE": 2,
        "DROPPED": 1,
    }
    assert closeout["after_inventory"]["completed_priority_counts"] == {"P1": 3}
    assert closeout["application"]["decision_count"] == 2
    assert closeout["application"]["target_status_counts"] == {
        "DONE": 1,
        "DROPPED": 1,
    }
    assert {row["task_id"] for row in closeout["application"]["applied_decisions"]} == {
        "TASK-A",
        "TASK-B",
    }
    line_churn = closeout["application"]["line_churn"]
    assert (
        line_churn["before_active_physical_line_count"]
        == line_churn["after_active_physical_line_count"]
    )
    assert line_churn["physical_line_count_preserved"] is True
    assert line_churn["vacated_source_line_count"] == 2
    assert line_churn["vacated_source_lines"] == [5, 9]
    assert line_churn["vacated_source_lines_preserved_blank"] is True
    assert closeout["safety"]["production_effect"] == "none"
    assert closeout["safety"]["broker_action"] == "none"

    (root / "descendant.txt").write_text("closeout descendant\n", encoding="utf-8")
    _git(root, "add", "descendant.txt")
    _git(root, "commit", "-q", "-m", "descendant validation commit")
    _validate_applied(
        closeout,
        root=root,
        policy=policy,
        policy_path=policy_path,
        manifest_path=manifest_path,
    )


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing_move", "APPLIED_DECISION_NOT_COMPLETED"),
        ("wrong_target_status", "APPLIED_TARGET_STATUS_DRIFT"),
        (
            "untargeted_status",
            "UNTARGETED_PARTITION_PRIORITY_STATUS_DRIFT",
        ),
        ("duplicate", "DUPLICATE_TASK_ID"),
        ("task_set", "APPLIED_TASK_SET_DRIFT"),
        ("rewritten_field", "APPLIED_NON_STATUS_FIELD_DRIFT"),
        ("audit_reason", "APPLIED_AUDIT_NOTE_DRIFT"),
        ("audit_manifest", "APPLIED_AUDIT_NOTE_DRIFT"),
        ("audit_effect", "APPLIED_AUDIT_NOTE_DRIFT"),
        ("audit_duplicate", "APPLIED_AUDIT_NOTE_DRIFT"),
        ("physical_line_removal", "APPLIED_ACTIVE_LINE_COUNT_DRIFT"),
        ("nonblank_vacated", "APPLIED_VACATED_LINE_NOT_BLANK"),
    ],
)
def test_applied_closeout_rejects_incomplete_or_expanded_application_scope(
    tmp_path: Path,
    mutation: str,
    expected_code: str,
) -> None:
    root, policy, policy_path, manifest_path, application_commit, _ = _prepare_applied_project(
        tmp_path, mutation=mutation
    )

    with pytest.raises(TaskPortfolioNormalizationError) as error:
        _build_applied(
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
            application_commit=application_commit,
        )

    assert error.value.code == expected_code


def test_applied_closeout_rejects_unknown_and_non_ancestor_application_commits(
    tmp_path: Path,
) -> None:
    root, policy, policy_path, manifest_path, _, _ = _prepare_applied_project(tmp_path)

    with pytest.raises(TaskPortfolioNormalizationError) as unknown:
        _build_applied(
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
            application_commit="f" * 40,
        )
    assert unknown.value.code == "BASE_COMMIT_UNKNOWN"

    tree = _git(root, "rev-parse", "HEAD^{tree}")
    unrelated_commit = _git(root, "commit-tree", tree, "-m", "unrelated root")
    with pytest.raises(TaskPortfolioNormalizationError) as non_ancestor:
        _build_applied(
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
            application_commit=unrelated_commit,
        )
    assert non_ancestor.value.code == "BASE_COMMIT_NOT_ANCESTOR"


def test_applied_closeout_binds_dry_run_policy_application_blobs_and_safety(
    tmp_path: Path,
) -> None:
    root, policy, policy_path, manifest_path, application_commit, dry_run = (
        _prepare_applied_project(tmp_path)
    )
    closeout = _build_applied(
        root=root,
        policy=policy,
        policy_path=policy_path,
        manifest_path=manifest_path,
        application_commit=application_commit,
    )

    tampered = copy.deepcopy(closeout)
    tampered["source_hashes"]["after"]["active_sha256"] = "0" * 64
    _refresh_applied_hashes(tampered)
    with pytest.raises(TaskPortfolioNormalizationError) as application_drift:
        _validate_applied(
            tampered,
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
        )
    assert application_drift.value.code == "APPLIED_CLOSEOUT_SOURCE_DRIFT"

    unsafe = copy.deepcopy(closeout)
    unsafe["safety"]["production_effect"] = "enabled"
    with pytest.raises(TaskPortfolioNormalizationError) as checksum:
        _validate_applied(
            unsafe,
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
        )
    assert checksum.value.code == "APPLIED_CLOSEOUT_SHA256"
    _refresh_applied_hashes(unsafe)
    with pytest.raises(TaskPortfolioNormalizationError) as safety_drift:
        _validate_applied(
            unsafe,
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
        )
    assert safety_drift.value.code == "APPLIED_CLOSEOUT_SOURCE_DRIFT"

    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8").replace(
            "fixture decisions are reviewed by the governance coordinator",
            "future live policy semantics",
        ),
        encoding="utf-8",
    )
    future_policy = load_normalization_policy(policy_path)
    validate_historical_normalization_decision_manifest(
        dry_run,
        project_root=root,
        policy=future_policy,
        policy_path=policy_path,
    )
    _validate_applied(
        closeout,
        root=root,
        policy=future_policy,
        policy_path=policy_path,
        manifest_path=manifest_path,
    )


def test_applied_closeout_cli_generates_and_validates_commit_bound_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _, policy_path, manifest_path, application_commit, _ = _prepare_applied_project(tmp_path)
    applied_path = root / "inputs/governance/gov_006_wave1_applied_closeout.json"
    monkeypatch.setattr(normalization_control, "PROJECT_ROOT", root)
    monkeypatch.setattr(normalization_control, "POLICY_PATH", policy_path)
    monkeypatch.setattr(normalization_control, "MANIFEST_PATH", manifest_path)
    monkeypatch.setattr(
        normalization_control,
        "APPLIED_CLOSEOUT_PATH",
        applied_path,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "governance_task_portfolio_normalization.py",
            "generate-applied",
            "--application-commit",
            application_commit,
        ],
    )

    assert normalization_control.main() == 0
    generated = json.loads(applied_path.read_text(encoding="utf-8"))
    assert generated["lineage"]["application_commit"] == application_commit
    assert '"mode": "APPLIED_CLOSEOUT"' in capsys.readouterr().out

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "governance_task_portfolio_normalization.py",
            "validate-applied",
        ],
    )
    assert normalization_control.main() == 0
    assert '"status": "PASS"' in capsys.readouterr().out


def test_applied_closeout_accepts_future_live_policy_and_rejects_non_thirty_real_wave(
    tmp_path: Path,
) -> None:
    root, policy, policy_path, manifest_path, application_commit, _ = _prepare_applied_project(
        tmp_path,
        wave_id="GOV-006-WAVE1-HIGH-CONFIDENCE",
    )
    with pytest.raises(TaskPortfolioNormalizationError) as wrong_count:
        _build_applied(
            root=root,
            policy=policy,
            policy_path=policy_path,
            manifest_path=manifest_path,
            application_commit=application_commit,
        )
    assert wrong_count.value.code == "REAL_WAVE_DECISION_COUNT"

    root2, policy2, policy_path2, manifest_path2, application_commit2, _ = _prepare_applied_project(
        tmp_path / "policy-drift"
    )
    policy_path2.write_text(
        policy_path2.read_text(encoding="utf-8") + "# policy drift\n",
        encoding="utf-8",
    )
    changed_policy = load_normalization_policy(policy_path2)
    future_replay = _build_applied(
        root=root2,
        policy=changed_policy,
        policy_path=policy_path2,
        manifest_path=manifest_path2,
        application_commit=application_commit2,
    )
    assert future_replay["lineage"]["application_commit"] == application_commit2


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
    payload = json.loads(REAL_MANIFEST_PATH.read_text(encoding="utf-8"))
    validate_historical_normalization_decision_manifest(
        payload,
        project_root=PROJECT_ROOT,
        policy=policy,
        policy_path=REAL_POLICY_PATH,
    )
    if REAL_APPLIED_CLOSEOUT_PATH.exists():
        closeout = json.loads(REAL_APPLIED_CLOSEOUT_PATH.read_text(encoding="utf-8"))
        validate_normalization_applied_closeout(
            closeout,
            project_root=PROJECT_ROOT,
            policy=policy,
            policy_path=REAL_POLICY_PATH,
            decision_manifest_path=REAL_MANIFEST_PATH,
        )
        assert closeout["application"]["decision_count"] == 30
        assert closeout["after_inventory"]["active_task_count"] == 405
        assert closeout["after_inventory"]["completed_task_count"] == 487
        assert closeout["after_inventory"]["completed_status_counts"] == {
            "DONE": 475,
            "DROPPED": 12,
        }
        assert closeout["application"]["line_churn"]["vacated_source_line_count"] == 30
        assert closeout["application"]["line_churn"]["vacated_source_lines_preserved_blank"] is True

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
