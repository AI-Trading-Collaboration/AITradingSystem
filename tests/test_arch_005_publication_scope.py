"""DEVX-016 S3: publication scope policy loading and the derivation of one candidate's scope."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.publication_scope import (
    APPROVED_STATUS,
    POLICY_SCHEMA_VERSION,
    PublicationScopeError,
    changed_paths_between,
    derive_publication_scope,
    load_publication_scope_policy,
)

GENERATORS = ("canonical-task-source", "architecture-manifests")
TIERS = ("architecture", "contract", "full")


def _policy_text(**overrides: str) -> str:
    fields = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": "TEST-PUBLICATION-SCOPE",
        "version": "1.0.0",
        "status": APPROVED_STATUS,
        "owner": "Project Owner",
        "approval_ref": "owner_decision:test",
        "rationale": "test fixture",
        "review_condition": "when the fence policy changes",
    }
    fields.update(overrides)
    head = "\n".join(f"{key}: {value}" for key, value in fields.items())
    return head + """
shared_paths:
  - docs/system_flow.md
  - inputs/architecture
  - tests/test_shared_pinned.py
allowed_owned_prefixes:
  - docs/requirements/
  - src/
  - tests/
  - scripts/
forbidden_paths:
  - AGENTS.md
  - outputs/
  - .git/
  - docs/research/user_owned.md
resource_paths:
  - outputs/architecture/integration_revalidation
limits:
  max_hours_since_full_end: 2.0
  min_free_disk_gb: 100
  max_generator_rounds: 3
"""


def _write_policy(tmp_path: Path, text: str | None = None) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(text if text is not None else _policy_text(), encoding="utf-8", newline="\n")
    return path


def test_a_reviewed_policy_loads_and_binds_its_own_bytes(tmp_path: Path) -> None:
    path = _write_policy(tmp_path)
    policy = load_publication_scope_policy(path)
    assert policy.status == APPROVED_STATUS
    assert policy.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert policy.shared_paths == (
        "docs/system_flow.md",
        "inputs/architecture",
        "tests/test_shared_pinned.py",
    )
    assert policy.allowed_owned_prefixes[0] == "docs/requirements/"
    assert (policy.max_hours_since_full_end, policy.min_free_disk_gb) == (2.0, 100.0)
    assert policy.max_generator_rounds == 3


@pytest.mark.parametrize(
    "overrides,code",
    [
        ({"status": "PROPOSED"}, "PUBLICATION_SCOPE_POLICY_STATUS"),
        ({"schema_version": "other.v9"}, "PUBLICATION_SCOPE_POLICY_SCHEMA"),
        ({"owner": '""'}, "PUBLICATION_SCOPE_POLICY_FIELD"),
        ({"approval_ref": '""'}, "PUBLICATION_SCOPE_POLICY_FIELD"),
    ],
)
def test_an_unreviewed_or_incomplete_policy_is_refused(
    tmp_path: Path, overrides: dict[str, str], code: str
) -> None:
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, _policy_text(**overrides)))
    assert raised.value.code == code


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("  - src/\n", "PUBLICATION_SCOPE_PATH_DUPLICATE"),  # appended to allowed prefixes below
        ("  - Docs/System_Flow.md\n", "PUBLICATION_SCOPE_PATH_DUPLICATE"),  # case-insensitive twin
        ("  - ../escape.md\n", "PUBLICATION_SCOPE_PATH_INVALID"),
        ("  - /absolute.md\n", "PUBLICATION_SCOPE_PATH_INVALID"),
        ("  - back\\\\slash.md\n", "PUBLICATION_SCOPE_PATH_INVALID"),
    ],
)
def test_malformed_or_duplicate_entries_are_refused(
    tmp_path: Path, mutation: str, code: str
) -> None:
    text = _policy_text()
    anchor = "allowed_owned_prefixes:\n" if mutation == "  - src/\n" else "shared_paths:\n"
    text = text.replace(anchor, anchor + mutation, 1)
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, text))
    assert raised.value.code == code


def test_a_prefix_must_end_with_a_slash_and_a_shared_path_must_not_be_forbidden(
    tmp_path: Path,
) -> None:
    text = _policy_text().replace("  - src/\n", "  - src\n", 1)
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, text))
    assert raised.value.code == "PUBLICATION_SCOPE_PREFIX_INVALID"
    text = _policy_text().replace("  - docs/system_flow.md\n", "  - outputs/anything.json\n", 1)
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, text))
    assert raised.value.code == "PUBLICATION_SCOPE_POLICY_OVERLAP"


def _policy(tmp_path: Path):  # type: ignore[no-untyped-def]
    return load_publication_scope_policy(_write_policy(tmp_path))


def test_derivation_splits_owned_from_shared_and_orders_deterministically(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    diff = [
        "tests/test_new.py",
        "docs/system_flow.md",
        "src/pkg/b.py",
        "inputs/architecture/x.yaml",
        "src/pkg/a.py",
        "tests/test_shared_pinned.py",
        "docs/requirements/DEVX-016.md",
        "src/pkg/a.py",  # duplicates collapse
    ]
    scope = derive_publication_scope(
        policy, diff_paths=diff, generator_ids=GENERATORS, required_validation_tiers=TIERS
    )
    assert scope.owned_paths == (
        "docs/requirements/DEVX-016.md",
        "src/pkg/a.py",
        "src/pkg/b.py",
        "tests/test_new.py",
    )
    assert scope.shared_diff_paths == (
        "docs/system_flow.md",
        "inputs/architecture/x.yaml",
        "tests/test_shared_pinned.py",
    )
    assert scope.generator_ids == GENERATORS and scope.required_validation_tiers == TIERS
    reordered = derive_publication_scope(
        policy,
        diff_paths=list(reversed(diff)),
        generator_ids=GENERATORS,
        required_validation_tiers=TIERS,
    )
    assert reordered.to_dict() == scope.to_dict()
    body = scope.to_dict()
    assert body["policy_sha256"] == policy.sha256
    assert body["production_effect"] == "none" and body["broker_action"] == "none"


@pytest.mark.parametrize(
    "path,code",
    [
        ("AGENTS.md", "PUBLICATION_SCOPE_PATH_FORBIDDEN"),
        ("outputs/validation_runtime/x.json", "PUBLICATION_SCOPE_PATH_FORBIDDEN"),
        (".git/config", "PUBLICATION_SCOPE_PATH_FORBIDDEN"),
        ("DOCS/research/USER_OWNED.md", "PUBLICATION_SCOPE_PATH_FORBIDDEN"),  # case-insensitive
        ("registry/other/file.yaml", "PUBLICATION_SCOPE_PATH_NOT_ALLOWED"),
        ("config/anything.yaml", "PUBLICATION_SCOPE_PATH_NOT_ALLOWED"),
    ],
)
def test_a_path_outside_the_policy_fails_before_any_transaction(
    tmp_path: Path, path: str, code: str
) -> None:
    with pytest.raises(PublicationScopeError) as raised:
        derive_publication_scope(
            _policy(tmp_path),
            diff_paths=["src/pkg/a.py", path],
            generator_ids=GENERATORS,
            required_validation_tiers=TIERS,
        )
    assert raised.value.code == code
    assert path.casefold() in raised.value.message.casefold()


def test_the_registered_known_unrelated_exclusions_are_forbidden_in_addition(
    tmp_path: Path,
) -> None:
    with pytest.raises(PublicationScopeError) as raised:
        derive_publication_scope(
            _policy(tmp_path),
            diff_paths=["src/pkg/a.py", "docs/requirements/owner_private.md"],
            generator_ids=GENERATORS,
            required_validation_tiers=TIERS,
            extra_forbidden=("docs/requirements/owner_private.md",),
        )
    assert raised.value.code == "PUBLICATION_SCOPE_PATH_FORBIDDEN"


def test_an_empty_diff_and_case_colliding_paths_are_refused(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    with pytest.raises(PublicationScopeError) as raised:
        derive_publication_scope(
            policy, diff_paths=[], generator_ids=GENERATORS, required_validation_tiers=TIERS
        )
    assert raised.value.code == "PUBLICATION_SCOPE_DIFF_EMPTY"
    with pytest.raises(PublicationScopeError) as raised:
        derive_publication_scope(
            policy,
            diff_paths=["src/Pkg/a.py", "src/pkg/a.py"],
            generator_ids=GENERATORS,
            required_validation_tiers=TIERS,
        )
    assert raised.value.code == "PUBLICATION_SCOPE_PATH_DUPLICATE"


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()


def test_the_commit_range_diff_lists_additions_changes_and_deletions(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "s3@example.com")
    _git(repository, "config", "user.name", "S3 Test")
    (repository / "kept.txt").write_text("1\n", encoding="utf-8")
    (repository / "changed.txt").write_text("1\n", encoding="utf-8")
    (repository / "removed.txt").write_text("1\n", encoding="utf-8")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD")
    (repository / "changed.txt").write_text("2\n", encoding="utf-8")
    (repository / "removed.txt").unlink()
    (repository / "docs").mkdir()
    (repository / "docs" / "中文.md").write_text("x\n", encoding="utf-8")
    _git(repository, "add", "-A")
    _git(repository, "commit", "-m", "candidate")
    head = _git(repository, "rev-parse", "HEAD")
    assert changed_paths_between(repository, base, head) == (
        "changed.txt",
        "docs/中文.md",
        "removed.txt",
    )
    with pytest.raises(PublicationScopeError) as raised:
        changed_paths_between(repository, base, "0" * 40)
    assert raised.value.code == "PUBLICATION_SCOPE_DIFF_UNAVAILABLE"


@pytest.mark.parametrize(
    "old,new,code",
    [
        (
            "  max_hours_since_full_end: 2.0\n",
            "  max_hours_since_full_end: 0\n",
            "PUBLICATION_SCOPE_POLICY_RANGE",
        ),
        (
            "  max_hours_since_full_end: 2.0\n",
            "  max_hours_since_full_end: 48\n",
            "PUBLICATION_SCOPE_POLICY_RANGE",
        ),
        (
            "  max_hours_since_full_end: 2.0\n",
            "  max_hours_since_full_end: soon\n",
            "PUBLICATION_SCOPE_POLICY_FIELD",
        ),
        ("  min_free_disk_gb: 100\n", "  min_free_disk_gb: 0\n", "PUBLICATION_SCOPE_POLICY_RANGE"),
        (
            "  max_generator_rounds: 3\n",
            "  max_generator_rounds: 2.5\n",
            "PUBLICATION_SCOPE_POLICY_FIELD",
        ),
        (
            "  max_generator_rounds: 3\n",
            "  max_generator_rounds: 99\n",
            "PUBLICATION_SCOPE_POLICY_RANGE",
        ),
        (
            "  max_generator_rounds: 3\n",
            "  max_generator_rounds: true\n",
            "PUBLICATION_SCOPE_POLICY_FIELD",
        ),
    ],
)
def test_the_decision_limits_are_validated_policy_fields(
    tmp_path: Path, old: str, new: str, code: str
) -> None:
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, _policy_text().replace(old, new)))
    assert raised.value.code == code


def test_a_policy_without_limits_is_refused(tmp_path: Path) -> None:
    text = _policy_text()
    text = text[: text.index("limits:")]
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, text))
    assert raised.value.code == "PUBLICATION_SCOPE_POLICY_FIELD"


@pytest.mark.parametrize(
    "resource",
    [
        "src/not_an_output",
        "outputs/validation_runtime",
        "outputs/architecture/arch_005_integration_publication_fence/publication.resource",
    ],
)
def test_resource_paths_must_be_outputs_the_fence_does_not_add_itself(
    tmp_path: Path, resource: str
) -> None:
    text = _policy_text().replace(
        "  - outputs/architecture/integration_revalidation\n", f"  - {resource}\n"
    )
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(_write_policy(tmp_path, text))
    assert raised.value.code == "PUBLICATION_SCOPE_RESOURCE_INVALID"


def test_the_derived_scope_carries_the_declared_resources(tmp_path: Path) -> None:
    policy = load_publication_scope_policy(_write_policy(tmp_path))
    scope = derive_publication_scope(
        policy, diff_paths=["src/a.py"], generator_ids=GENERATORS, required_validation_tiers=TIERS
    )
    assert scope.resource_paths == ("outputs/architecture/integration_revalidation",)
    assert scope.to_dict()["resource_paths"] == ["outputs/architecture/integration_revalidation"]


def test_a_proposed_policy_loads_only_when_explicitly_allowed(tmp_path: Path) -> None:
    path = _write_policy(tmp_path, _policy_text(status="PROPOSED_PENDING_OWNER_REVIEW"))
    with pytest.raises(PublicationScopeError) as raised:
        load_publication_scope_policy(path)
    assert raised.value.code == "PUBLICATION_SCOPE_POLICY_STATUS"
    proposed = load_publication_scope_policy(path, allow_proposed=True)
    assert proposed.status == "PROPOSED_PENDING_OWNER_REVIEW"
    path.write_text(_policy_text(status="DRAFT"), encoding="utf-8", newline="\n")
    with pytest.raises(PublicationScopeError):
        load_publication_scope_policy(path, allow_proposed=True)
