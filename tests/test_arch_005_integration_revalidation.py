from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.integration_revalidation import (
    INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION,
    IntegrationRevalidationError,
    IntegrationRevalidationPolicy,
    build_integration_revalidation_plan,
    load_integration_revalidation_policy,
    validate_integration_revalidation_plan,
)

ROOT = Path(__file__).resolve().parents[1]
CLI_PATH = ROOT / "scripts" / "architecture_arch005_integration_revalidation.py"
EXCLUDED_PATH = "docs/research/growth_tilt_owner_diagnosis_pack.md"


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _write(repo: Path, relative: str, text: str) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "--all")
    _git(
        repo,
        "-c",
        "user.name=Codex Test",
        "-c",
        "user.email=codex@example.invalid",
        "commit",
        "-m",
        message,
    )
    return _git(repo, "rev-parse", "HEAD")


def _repository(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _write(repo, "src/task.py", "BASE = 1\n")
    _write(repo, "docs/main.md", "base\n")
    _write(repo, "registry/development_tasks_shadow/index.json", "{}\n")
    _write(repo, "src/ai_trading_system/data/schema.py", "SCHEMA = 1\n")
    _write(repo, "old_name.py", "VALUE = 1\n")
    _write(repo, EXCLUDED_PATH, "user owned\n")
    _write(repo, ".gitignore", "outputs/\n")
    return repo, _commit(repo, "base")


def _histories(
    repo: Path,
    base: str,
    *,
    lane_changes: dict[str, str | None],
    main_changes: dict[str, str | None],
) -> tuple[str, str]:
    _git(repo, "switch", "-c", "lane", base)
    for path, content in lane_changes.items():
        if content is None:
            (repo / path).unlink()
        else:
            _write(repo, path, content)
    lane = _commit(repo, "lane")
    _git(repo, "switch", "main")
    for path, content in main_changes.items():
        if content is None:
            (repo / path).unlink()
        else:
            _write(repo, path, content)
    main = _commit(repo, "main")
    return lane, main


def _policy() -> IntegrationRevalidationPolicy:
    return IntegrationRevalidationPolicy(
        known_unrelated_exclusions=(EXCLUDED_PATH,),
        coordinator_refreshable_scopes=("registry/development_tasks_shadow",),
        contract_sensitive_scopes=("src/ai_trading_system/data",),
        final_validation_tiers=(
            "architecture-fitness",
            "contract-validation",
            "full",
        ),
    )


def _manifest(
    base: str,
    *,
    owned_paths: list[str],
    shared_paths: list[str] | None = None,
    contract_claims: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "change_manifest.v1",
        "change_id": "devx-006-test",
        "task_id": "DEVX-006_BASE_DRIFT_AWARE_INTEGRATION_AND_REVALIDATION",
        "lane_role": "COORDINATOR",
        "base_commit": base,
        "owner": "test-owner",
        "production_effect": "none",
        "owned_paths": owned_paths,
        "shared_paths": shared_paths or [],
        "module_ids": ["integration-revalidation"],
        "contract_claims": contract_claims or [],
        "required_validation_tiers": ["focused"],
    }


def test_unrelated_drift_reuses_frozen_lane_and_allows_one_candidate(
    tmp_path: Path,
) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={"docs/main.md": "mainline\n"},
    )

    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["src/task.py"]),
        policy=_policy(),
    )

    assert plan["schema_version"] == INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION
    assert plan["decision"] == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    assert plan["candidate_creation_allowed"] is True
    assert plan["task_branch_rebuild_required"] is False
    assert plan["lane_focused_evidence_reuse_allowed"] is True
    assert plan["overlaps"] == []
    assert plan["path_classifications"] == [
        {
            "history": "MAINLINE",
            "path": "docs/main.md",
            "classification": "MAINLINE_UNRELATED",
            "reason": "latest-main drift does not overlap task delta",
        },
        {
            "history": "TASK",
            "path": "src/task.py",
            "classification": "TASK_ONLY",
            "reason": "changed only on frozen task lane",
        },
    ]
    assert plan["automatic_git_mutation_allowed"] is False


def test_reviewed_shared_overlap_is_refreshed_only_on_final_tree(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    shared = "registry/development_tasks_shadow/index.json"
    lane, main = _histories(
        repo,
        base,
        lane_changes={shared: '{"lane": true}\n'},
        main_changes={shared: '{"main": true}\n'},
    )

    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=[], shared_paths=[shared]),
        policy=_policy(),
    )

    assert plan["decision"] == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    assert plan["overlaps"] == [
        {
            "task_path": shared,
            "mainline_path": shared,
            "classification": "COORDINATOR_REFRESH",
            "reason": (
                "discard lane bytes and rebuild reviewed shared view on final tree"
            ),
        }
    ]


def test_domain_overlap_requires_one_coordinator_reconciliation(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={"src/task.py": "MAIN = 1\n"},
    )
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["src/task.py"]),
        policy=_policy(),
    )
    assert plan["decision"] == "RECONCILIATION_REQUIRED"
    assert plan["required_next_stage"] == "COORDINATOR_RECONCILIATION"
    assert plan["task_branch_rebuild_required"] is False
    assert plan["candidate_creation_allowed"] is False
    assert plan["reviewed_reconciliation_required"] is True


def test_contract_sensitive_path_or_claim_requires_serial_wave(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    contract_path = "src/ai_trading_system/data/schema.py"
    lane, main = _histories(
        repo,
        base,
        lane_changes={contract_path: "SCHEMA = 2\n"},
        main_changes={contract_path: "SCHEMA = 3\n"},
    )
    manifest = _manifest(
        base,
        owned_paths=[contract_path],
        contract_claims=[
            {"contract_id": "dq-receipt", "version": "1.0.0", "access": "WRITE"}
        ],
    )
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=manifest,
        policy=_policy(),
        mainline_contract_claims=[
            {"contract_id": "dq-receipt", "version": "2.0.0", "access": "READ"}
        ],
    )
    assert plan["decision"] == "SERIAL_CONTRACT_WAVE_REQUIRED"
    assert plan["task_branch_rebuild_required"] is True
    assert plan["overlaps"][0]["classification"] == "CONTRACT_SENSITIVE_OVERLAP"
    assert plan["contract_conflicts"][0]["code"] == "CONTRACT_VERSION_CONFLICT"


def test_undeclared_task_path_blocks_candidate(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n", "src/extra.py": "EXTRA = 1\n"},
        main_changes={"docs/main.md": "mainline\n"},
    )
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["src/task.py"]),
        policy=_policy(),
    )
    assert plan["decision"] == "BLOCKED"
    assert plan["undeclared_task_paths"] == ["src/extra.py"]
    assert {"code": "UNDECLARED_TASK_PATH", "detail": "src/extra.py"} in plan[
        "blockers"
    ]


def test_rename_expands_old_and_new_paths(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    _git(repo, "switch", "-c", "lane", base)
    _git(repo, "mv", "old_name.py", "new_name.py")
    lane = _commit(repo, "rename")
    _git(repo, "switch", "main")
    _write(repo, "docs/main.md", "mainline\n")
    main = _commit(repo, "main")

    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["old_name.py", "new_name.py"]),
        policy=_policy(),
    )
    assert plan["decision"] == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    assert plan["task_delta"] == [
        {"status": "R100", "paths": ["old_name.py", "new_name.py"]}
    ]


def test_wrong_ancestry_and_dirty_repository_fail_closed(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={"docs/main.md": "mainline\n"},
    )
    _write(repo, "untracked.txt", "dirty\n")
    dirty = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["src/task.py"]),
        policy=_policy(),
    )
    assert dirty["decision"] == "BLOCKED"
    assert {"code": "REPOSITORY_DIRTY", "detail": "untracked.txt"} in dirty["blockers"]

    (repo / "untracked.txt").unlink()
    _git(repo, "switch", "--orphan", "unrelated")
    _write(repo, "orphan.py", "ORPHAN = 1\n")
    unrelated = _commit(repo, "unrelated")
    _git(repo, "switch", "main")
    wrong = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=unrelated,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["orphan.py"]),
        policy=_policy(),
    )
    assert wrong["decision"] == "BLOCKED"
    assert any(row["code"] == "BASE_NOT_LANE_ANCESTOR" for row in wrong["blockers"])


def test_known_unrelated_path_is_excluded_before_status_and_diff(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={
            "docs/main.md": "mainline\n",
            EXCLUDED_PATH: "committed owner update\n",
        },
    )
    _write(repo, EXCLUDED_PATH, "dirty owner update\n")
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=_manifest(base, owned_paths=["src/task.py"]),
        policy=_policy(),
    )
    assert plan["decision"] == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    assert plan["dirty_paths"] == []
    assert EXCLUDED_PATH not in json.dumps(plan["mainline_delta"])


def test_validator_rebuilds_repository_facts_and_rejects_tamper(
    tmp_path: Path,
) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={"docs/main.md": "mainline\n"},
    )
    manifest = _manifest(base, owned_paths=["src/task.py"])
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=manifest,
        policy=_policy(),
    )
    validate_integration_revalidation_plan(
        plan,
        repository=repo,
        manifest=manifest,
        policy=_policy(),
    )
    tampered = copy.deepcopy(plan)
    tampered["candidate_creation_allowed"] = False
    with pytest.raises(IntegrationRevalidationError, match="PLAN_CHECKSUM"):
        validate_integration_revalidation_plan(
            tampered,
            repository=repo,
            manifest=manifest,
            policy=_policy(),
        )


def test_reviewed_project_policy_is_strict_and_keeps_zero_read_exclusion() -> None:
    policy = load_integration_revalidation_policy()
    assert policy.known_unrelated_exclusions == (EXCLUDED_PATH,)
    assert "registry/development_tasks_shadow" in policy.coordinator_refreshable_scopes
    assert "src/ai_trading_system/data" in policy.contract_sensitive_scopes
    assert policy.final_validation_tiers == (
        "architecture-fitness",
        "contract-validation",
        "full",
        "integration",
        "reproducibility",
    )


def test_policy_duplicate_key_fails_closed(tmp_path: Path) -> None:
    policy_path = tmp_path / "duplicate.yaml"
    policy_path.write_text(
        "\n".join(
            [
                "schema_version: arch_005_integration_revalidation_policy.v1",
                "known_unrelated_exclusions:",
                f"  - {EXCLUDED_PATH}",
                "coordinator_refreshable_scopes: []",
                "coordinator_refreshable_scopes: []",
                "contract_sensitive_scopes: []",
                "final_validation_tiers:",
                "  - full",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "POLICY_YAML_DUPLICATE_KEY"
    assert caught.value.message == "coordinator_refreshable_scopes"


def test_policy_non_string_key_keeps_typed_line_detail(tmp_path: Path) -> None:
    policy_path = tmp_path / "non-string-key.yaml"
    policy_path.write_text(
        "1: value\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "POLICY_YAML_NON_STRING_KEY"
    assert caught.value.message == "line=1"


def test_policy_merge_key_remains_unflattened_and_fails_as_read_error(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "merge.yaml"
    policy_path.write_text(
        "base: &base\n"
        "  schema_version: arch_005_integration_revalidation_policy.v1\n"
        "<<: *base\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "POLICY_READ_FAILED"
    assert "tag:yaml.org,2002:merge" in caught.value.message
    assert "line 3, column 1" in caught.value.message


def test_policy_non_finite_value_remains_parser_accepted(tmp_path: Path) -> None:
    policy_path = tmp_path / "non-finite.yaml"
    policy_path.write_text(
        "schema_version: arch_005_integration_revalidation_policy.v1\n"
        "known_unrelated_exclusions:\n"
        f"  - {EXCLUDED_PATH}\n"
        "coordinator_refreshable_scopes: []\n"
        "contract_sensitive_scopes: []\n"
        "final_validation_tiers:\n"
        "  - .nan\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "IDENTIFIER"
    assert caught.value.message == "final_validation_tiers"


def test_policy_cyclic_alias_keeps_read_error_boundary(tmp_path: Path) -> None:
    policy_path = tmp_path / "cycle.yaml"
    policy_path.write_text(
        "schema_version: arch_005_integration_revalidation_policy.v1\n"
        "known_unrelated_exclusions: &items\n"
        "  self: *items\n"
        "coordinator_refreshable_scopes: []\n"
        "contract_sensitive_scopes: []\n"
        "final_validation_tiers:\n"
        "  - full\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "POLICY_READ_FAILED"
    assert "recursive" in caught.value.message
    assert "line 2, column 29" in caught.value.message


def test_policy_cyclic_sequence_remains_parser_accepted_then_schema_rejected(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "sequence-cycle.yaml"
    policy_path.write_text(
        "schema_version: arch_005_integration_revalidation_policy.v1\n"
        "known_unrelated_exclusions: &items\n"
        "  - *items\n"
        "coordinator_refreshable_scopes: []\n"
        "contract_sensitive_scopes: []\n"
        "final_validation_tiers:\n"
        "  - full\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "PATH_TYPE"
    assert caught.value.message == "path must be a string"


@pytest.mark.parametrize(
    ("name", "text", "expected_detail"),
    [
        (
            "malformed",
            "root: [\n",
            "expected the node content, but found '<stream end>'",
        ),
        (
            "unsafe-tag",
            "value: !!python/object:builtins.object {}\n",
            "tag:yaml.org,2002:python/object:builtins.object",
        ),
    ],
)
def test_policy_invalid_yaml_keeps_read_error_detail(
    tmp_path: Path,
    name: str,
    text: str,
    expected_detail: str,
) -> None:
    policy_path = tmp_path / f"{name}.yaml"
    policy_path.write_text(text, encoding="utf-8")
    with pytest.raises(IntegrationRevalidationError) as caught:
        load_integration_revalidation_policy(policy_path)
    assert caught.value.code == "POLICY_READ_FAILED"
    assert expected_detail in caught.value.message


def test_policy_read_and_utf8_failures_keep_read_error_boundary(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing.yaml"
    with pytest.raises(IntegrationRevalidationError) as missing:
        load_integration_revalidation_policy(missing_path)
    assert missing.value.code == "POLICY_READ_FAILED"
    assert missing_path.name in missing.value.message

    invalid_utf8_path = tmp_path / "invalid-utf8.yaml"
    invalid_utf8_path.write_bytes(b"\xff")
    with pytest.raises(IntegrationRevalidationError) as invalid_utf8:
        load_integration_revalidation_policy(invalid_utf8_path)
    assert invalid_utf8.value.code == "POLICY_READ_FAILED"
    assert "utf-8" in invalid_utf8.value.message.lower()


def test_cli_plan_and_validate_use_ignored_read_only_evidence(tmp_path: Path) -> None:
    repo, base = _repository(tmp_path)
    lane, main = _histories(
        repo,
        base,
        lane_changes={"src/task.py": "LANE = 1\n"},
        main_changes={"docs/main.md": "mainline\n"},
    )
    manifest_path = repo / "outputs" / "change_manifest.json"
    plan_path = repo / "outputs" / "integration_plan.json"
    _write(
        repo,
        "outputs/change_manifest.json",
        json.dumps(_manifest(base, owned_paths=["src/task.py"])),
    )
    plan_run = subprocess.run(
        [
            sys.executable,
            str(CLI_PATH),
            "plan",
            "--repository",
            str(repo),
            "--manifest",
            str(manifest_path),
            "--frozen-base",
            base,
            "--lane-head",
            lane,
            "--latest-main",
            main,
            "--output",
            str(plan_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert plan_run.returncode == 0, plan_run.stdout + plan_run.stderr
    assert json.loads(plan_run.stdout)["decision"] == (
        "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    )
    validate_run = subprocess.run(
        [
            sys.executable,
            str(CLI_PATH),
            "validate",
            "--repository",
            str(repo),
            "--manifest",
            str(manifest_path),
            "--plan",
            str(plan_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert validate_run.returncode == 0, validate_run.stdout + validate_run.stderr
    assert json.loads(validate_run.stdout)["status"] == "PASS"
