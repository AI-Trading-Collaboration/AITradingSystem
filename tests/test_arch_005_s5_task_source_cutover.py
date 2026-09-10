from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

import scripts.architecture_arch005_task_source as task_source_cli
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture import task_registry_canonical as canonical
from ai_trading_system.platform.architecture.task_registry_canonical import (
    CANONICAL_SOURCE,
    GENERATED_BANNER,
    CanonicalTaskRegistryError,
    build_consumer_inventory,
    canonical_task_register_view_path,
    run_rollback_rehearsal,
    validate_canonical_fragment,
    validate_canonical_registry,
)


def test_task_source_mutation_binds_exact_publication_phase_and_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class StubFence:
        def __init__(self, *, project_root: Path) -> None:
            assert project_root == PROJECT_ROOT

        def validate(self, transaction: Path, **kwargs: object) -> None:
            calls.append({"transaction": transaction, **kwargs})

    monkeypatch.setattr(task_source_cli, "IntegrationPublicationFence", StubFence)
    args = Namespace(
        command="update",
        task_id="DEVX-009",
        publication_transaction=Path("outputs/publication/transaction.json"),
    )

    task_source_cli._require_publication_transaction(args)

    assert calls == [
        {
            "transaction": Path("outputs/publication/transaction.json"),
            "exact_phase": "TASK_SOURCE_PRE_WRITE",
            "task_id": "DEVX-009",
        }
    ]


def test_task_source_validate_command_remains_read_only_without_publication_fence() -> None:
    task_source_cli._require_publication_transaction(Namespace(command="validate"))


def test_repository_canonical_registry_is_active_and_self_hosted() -> None:
    registry = validate_canonical_registry(project_root=PROJECT_ROOT)

    assert registry.index["status"] == "PASS"
    assert registry.index["source_of_truth"] == CANONICAL_SOURCE
    assert registry.index["cutover_performed"] is True
    assert registry.index["legacy_markdown_writable"] is False
    # Preserve the exact integration-base identities, not a total that rejects
    # legitimate canonical registrations or hides an equal-count replacement.
    baseline_commit = "6498d030079370f6fc6dd2edf3b71dd0505ed57e"
    baseline_index = canonical._canonical_blob_mapping(
        canonical._canonical_git_blob(
            PROJECT_ROOT, baseline_commit, canonical.CANONICAL_INDEX_PATH
        ),
        canonical.CANONICAL_INDEX_PATH,
        generated=True,
    )
    canonical._verify_checksum(baseline_index, "index_checksum", "INDEX_CHECKSUM")
    baseline_ids = [record["task_id"] for record in baseline_index["fragments"]]
    assert len(set(baseline_ids)) == len(baseline_ids) == baseline_index["task_count"]
    _assert_preserved_task_identities(
        baseline_ids=set(baseline_ids),
        current_ids=[
            fragment["stable_task_identity"]["task_id"] for fragment in registry.fragments
        ],
        declared_count=registry.index["task_count"],
    )
    assert registry.index["missing_task_count"] == 0
    assert registry.index["duplicate_task_count"] == 0
    assert registry.index["governance_cycle_count"] >= 2
    assert registry.index["manual_row_move_workflow_enabled"] is False


def _assert_preserved_task_identities(
    *, baseline_ids: set[str], current_ids: list[str], declared_count: int
) -> None:
    identities = set(current_ids)
    assert type(declared_count) is int
    assert len(identities) == len(current_ids) == declared_count
    assert baseline_ids <= identities
    assert {
        "OPS-081_SCHEDULER_BUSINESS_CONTRACT_DECOUPLING",
        "DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2",
    } <= identities


@pytest.mark.parametrize("mutation", ["addition", "missing", "replacement", "duplicate", "count"])
def test_preserved_task_identities_allow_additions_but_reject_loss(mutation: str) -> None:
    baseline_ids = {"BASELINE-A", "OPS-081_SCHEDULER_BUSINESS_CONTRACT_DECOUPLING"}
    current_ids = sorted(baseline_ids) + ["DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2"]
    if mutation == "addition":
        current_ids.append("LEGITIMATE-NEW-TASK")
        _assert_preserved_task_identities(
            baseline_ids=baseline_ids, current_ids=current_ids, declared_count=len(current_ids)
        )
        return
    if mutation == "missing":
        current_ids.remove("BASELINE-A")
    elif mutation == "replacement":
        current_ids[current_ids.index("BASELINE-A")] = "UNRELATED-REPLACEMENT"
    elif mutation == "duplicate":
        current_ids.append("BASELINE-A")
    declared_count = len(current_ids) + (1 if mutation == "count" else 0)
    with pytest.raises(AssertionError):
        _assert_preserved_task_identities(
            baseline_ids=baseline_ids, current_ids=current_ids, declared_count=declared_count
        )


def test_generated_views_are_validated_do_not_edit_projections() -> None:
    registry = validate_canonical_registry(project_root=PROJECT_ROOT)
    active = canonical_task_register_view_path(PROJECT_ROOT, "active")
    completed = canonical_task_register_view_path(PROJECT_ROOT, "completed")

    assert active.read_text(encoding="utf-8").startswith(GENERATED_BANNER)
    assert completed.read_text(encoding="utf-8").startswith(GENERATED_BANNER)
    assert len(registry.projected_rows("active")) == registry.index["active_task_count"]
    assert len(registry.projected_rows("completed")) == registry.index["completed_task_count"]
    with pytest.raises(CanonicalTaskRegistryError, match="PARTITION_INVALID"):
        canonical_task_register_view_path(PROJECT_ROOT, "unknown")


def test_final_import_preserves_ambiguous_legacy_row_bytes_in_view() -> None:
    registry = validate_canonical_registry(project_root=PROJECT_ROOT)
    fragment = next(
        item
        for item in registry.fragments
        if (item.get("legacy_import_evidence") or {}).get("ambiguous_unescaped_pipe_boundaries")
    )
    evidence = fragment["legacy_import_evidence"]
    partition = "completed" if fragment["projection"]["terminal"] else "active"
    view = canonical_task_register_view_path(PROJECT_ROOT, partition).read_text(encoding="utf-8")

    assert evidence["cell_count"] > 8
    assert evidence["raw_line"] in view


def test_fragment_validation_fails_closed_on_event_fork_and_reordering() -> None:
    registry = validate_canonical_registry(project_root=PROJECT_ROOT)
    original = registry.fragment("ARCH-005S5_CANONICAL_TASK_SOURCE_CUTOVER")
    assert len(original["events"]) >= 2

    forked = copy.deepcopy(original)
    forked["events"][-1]["previous_state_event_id"] = "task-event-fork"
    with pytest.raises(CanonicalTaskRegistryError, match="EVENT_CHAIN"):
        validate_canonical_fragment(forked)

    reordered = copy.deepcopy(original)
    reordered["events"] = list(reversed(reordered["events"]))
    with pytest.raises(CanonicalTaskRegistryError, match="EVENT_GENESIS|EVENT_CHAIN"):
        validate_canonical_fragment(reordered)


def test_fragment_validation_fails_closed_on_event_time_and_terminal_exit() -> None:
    registry = validate_canonical_registry(project_root=PROJECT_ROOT)
    original = registry.fragment("ARCH-005S5_CANONICAL_TASK_SOURCE_CUTOVER")

    invalid_time = copy.deepcopy(original)
    invalid_time["events"][-1]["occurred_at"] = "2026-08-09T12:00:00"
    invalid_time["events"][-1]["event_id"] = _event_id(invalid_time["events"][-1])
    with pytest.raises(CanonicalTaskRegistryError, match="EVENT_OCCURRED_AT_TIMEZONE"):
        validate_canonical_fragment(invalid_time)

    terminal_exit = copy.deepcopy(original)
    illegal_exit = copy.deepcopy(terminal_exit["events"][-1])
    illegal_exit["occurred_at"] = "2026-08-10T02:11:00+09:00"
    illegal_exit["change_id"] = "test-illegal-terminal-exit"
    illegal_exit["previous_state_event_id"] = terminal_exit["last_event_id"]
    illegal_exit["from_status"] = "DONE"
    illegal_exit["to_status"] = "VALIDATING"
    illegal_exit["event_id"] = _event_id(illegal_exit)
    terminal_exit["events"].append(illegal_exit)
    terminal_exit["last_event_id"] = illegal_exit["event_id"]
    with pytest.raises(CanonicalTaskRegistryError, match="EVENT_STATUS_TRANSITION"):
        validate_canonical_fragment(terminal_exit)


def test_consumer_inventory_has_no_manual_runtime_reader_or_writer() -> None:
    inventory = build_consumer_inventory(PROJECT_ROOT)

    assert inventory["status"] == "PASS"
    assert inventory["manual_semantic_runtime_consumer_count"] == 0
    assert inventory["manual_writer_count"] == 0


def test_rollback_rehearsal_never_reverts_canonical_authority(tmp_path: Path) -> None:
    payload = run_rollback_rehearsal(project_root=PROJECT_ROOT, output_root=tmp_path)

    assert payload["status"] == "PASS"
    assert payload["source_of_truth_reverted"] is False
    assert payload["canonical_event_loss_count"] == 0
    assert {item["partition"] for item in payload["views"]} == {"active", "completed"}
    assert (tmp_path / "rollback_rehearsal.yaml").is_file()


def _event_id(event: dict[str, object]) -> str:
    payload = {key: value for key, value in event.items() if key != "event_id"}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"task-event-{hashlib.sha256(encoded).hexdigest()[:32]}"


def _frozen_git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _frozen_fixture(root: Path, status: str = "IN_PROGRESS") -> tuple[dict, dict]:
    """A real Git repository with two independently sealed canonical tasks."""
    _frozen_git(root, "init")
    _frozen_git(root, "config", "user.email", "fixture@example.invalid")
    _frozen_git(root, "config", "user.name", "Canonical reader fixture")
    policy_path = root / canonical.POLICY_PATH
    policy_path.parent.mkdir(parents=True)
    policy_path.write_bytes((PROJECT_ROOT / canonical.POLICY_PATH).read_bytes())
    records = []
    fragments = {}
    previous = canonical._chain_genesis()
    for order, task_id in enumerate(("TASK-A", "TASK-B"), start=1):
        cells = [task_id, "fixture", "P1", status, "coordinator", "none", "checks", "note"]
        event = {
            "schema_version": "task_event.v1",
            "task_id": task_id,
            "event_type": "TASK_REGISTERED",
            "occurred_at": "2026-09-10T01:00:00+00:00",
            "actor": "fixture",
            "change_id": "fixture-register",
            "base_commit": "a" * 40,
            "previous_state_event_id": None,
            "from_status": None,
            "to_status": status,
            "payload": {"legacy_projection": cells},
            "evidence_refs": ["fixture"],
        }
        event["event_id"] = canonical._canonical_event_id(event)
        fragment = {
            "schema_version": canonical.CANONICAL_FRAGMENT_SCHEMA,
            "source_of_truth": CANONICAL_SOURCE,
            "stable_task_identity": {
                "task_id": task_id,
                "task_id_sha256": hashlib.sha256(task_id.encode()).hexdigest(),
            },
            "task_record": canonical._task_record_from_cells(cells, prior={}),
            "events": [event],
            "projection": canonical._projection_from_cells(cells),
            "last_event_id": event["event_id"],
        }
        fragment["fragment_checksum"] = canonical._payload_checksum(fragment, "fragment_checksum")
        canonical.validate_canonical_fragment(fragment)
        path = canonical._canonical_fragment_path(task_id)
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(canonical._yaml_bytes(fragment))
        core = {
            "task_id": task_id,
            "path": path,
            "file_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            "fragment_checksum": fragment["fragment_checksum"],
            "partition": "completed" if status == "DONE" else "active",
            "order": order,
        }
        chain = canonical._entry_chain(previous, core)
        records.append({**core, "previous_entry_sha256": previous, "entry_sha256": chain})
        previous = chain
        fragments[task_id] = fragment
    index = {
        "schema_version": canonical.CANONICAL_INDEX_SCHEMA,
        "status": "PASS",
        "source_of_truth": CANONICAL_SOURCE,
        "cutover_performed": True,
        "legacy_markdown_writable": False,
        "fragment_root": canonical.CANONICAL_FRAGMENT_ROOT,
        "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
        "task_count": 2,
        "fragment_count": 2,
        "active_task_count": 0 if status == "DONE" else 2,
        "completed_task_count": 2 if status == "DONE" else 0,
        "missing_task_count": 0,
        "duplicate_task_count": 0,
        "chain_genesis_sha256": canonical._chain_genesis(),
        "final_chain_sha256": previous,
        "fragments": records,
    }
    _frozen_save_index(root, index)
    return index, fragments


def _frozen_save_index(root: Path, index: dict) -> None:
    index["index_checksum"] = canonical._payload_checksum(index, "index_checksum")
    path = root / canonical.CANONICAL_INDEX_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical._yaml_bytes(index))


def _frozen_commit(root: Path) -> str:
    _frozen_git(root, "add", "--all")
    _frozen_git(root, "commit", "-m", "synthetic canonical fixture")
    return _frozen_git(root, "rev-parse", "HEAD")


@pytest.mark.parametrize("status", ["IN_PROGRESS", "DONE"])
def test_frozen_canonical_reader_exact_commit_and_task_isolation(
    tmp_path: Path, status: str
) -> None:
    index, _ = _frozen_fixture(tmp_path, status)
    commit = _frozen_commit(tmp_path)
    # Neither working-copy corruption nor absent generated views/other blobs is
    # authority for this historical, single-task query.
    (tmp_path / canonical.CANONICAL_INDEX_PATH).write_text("corrupt working copy")
    result = canonical.read_canonical_task_at_commit(
        project_root=tmp_path,
        commit=commit,
        task_id="TASK-A",
    )
    assert result["source_commit"] == commit
    assert result["task_id"] == "TASK-A"
    assert result["status"] == status
    assert result["is_terminal"] is (status == "DONE")
    assert result["fragment_sha256"] == index["fragments"][0]["file_sha256"]
    assert result["index_checksum"] == index["index_checksum"]
    assert _frozen_git(tmp_path, "rev-parse", "HEAD") == commit


def test_frozen_canonical_reader_missing_task_is_distinct_from_missing_blob(tmp_path: Path) -> None:
    _frozen_fixture(tmp_path)
    (tmp_path / canonical._canonical_fragment_path("TASK-B")).unlink()
    commit = _frozen_commit(tmp_path)
    assert (
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path,
            commit=commit,
            task_id="TASK-A",
        )["task_id"]
        == "TASK-A"
    )
    for task_id, code in (
        ("TASK-C", "CANONICAL_TASK_NOT_FOUND"),
        ("TASK-B", "CANONICAL_GIT_BLOB_MISSING"),
    ):
        with pytest.raises(CanonicalTaskRegistryError) as error:
            canonical.read_canonical_task_at_commit(
                project_root=tmp_path, commit=commit, task_id=task_id
            )
        assert error.value.code == code


@pytest.mark.parametrize("revision", ["HEAD", "a" * 7, "a" * 40, "A" * 40])
def test_frozen_canonical_reader_rejects_nonexact_revision(tmp_path: Path, revision: str) -> None:
    _frozen_fixture(tmp_path)
    _frozen_commit(tmp_path)
    with pytest.raises(CanonicalTaskRegistryError):
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=revision, task_id="TASK-A"
        )


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("duplicate", "INDEX_DUPLICATE_TASK"),
        ("count", "INDEX_COUNT"),
        ("path", "INDEX_PATH_BINDING"),
        ("chain", "INDEX_CHAIN"),
        ("file_hash", "INDEX_FILE_HASH"),
        ("checksum", "INDEX_CHECKSUM"),
        ("authority", "INDEX_AUTHORITY"),
        ("partition", "INDEX_PARTITION"),
    ],
)
def test_frozen_canonical_reader_rejects_index_corruption(
    tmp_path: Path, mutation: str, code: str
) -> None:
    index, _ = _frozen_fixture(tmp_path)
    if mutation == "duplicate":
        index["fragments"].append(copy.deepcopy(index["fragments"][0]))
    elif mutation == "count":
        index["task_count"] = True
    elif mutation == "path":
        index["fragments"][0]["path"] = "../outside.yaml"
    elif mutation == "chain":
        index["final_chain_sha256"] = "f" * 64
    elif mutation == "authority":
        index["source_of_truth"] = "LEGACY_MARKDOWN_ONLY"
    elif mutation == "partition":
        index["fragments"][0]["partition"] = "unknown"
    elif mutation == "file_hash":
        index["fragments"][0]["file_sha256"] = "f" * 64
        previous = canonical._chain_genesis()
        for record in index["fragments"]:
            record["previous_entry_sha256"] = previous
            record["entry_sha256"] = canonical._entry_chain(
                previous, canonical._record_core(record)
            )
            previous = record["entry_sha256"]
        index["final_chain_sha256"] = previous
    _frozen_save_index(tmp_path, index)
    if mutation == "checksum":
        path = tmp_path / canonical.CANONICAL_INDEX_PATH
        index["index_checksum"] = "f" * 64
        path.write_bytes(canonical._yaml_bytes(index))
    commit = _frozen_commit(tmp_path)
    with pytest.raises(CanonicalTaskRegistryError) as error:
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=commit, task_id="TASK-A"
        )
    assert error.value.code == code


def test_frozen_canonical_reader_rejects_git_symlink_blob(tmp_path: Path) -> None:
    _frozen_fixture(tmp_path)
    _frozen_commit(tmp_path)
    path = canonical._canonical_fragment_path("TASK-A")
    blob = _frozen_git(tmp_path, "rev-parse", f"HEAD:{path}")
    # Set a real Git symlink mode without needing Windows symlink privileges.
    _frozen_git(tmp_path, "update-index", "--cacheinfo", f"120000,{blob},{path}")
    _frozen_git(tmp_path, "commit", "-m", "nonregular canonical blob")
    commit = _frozen_git(tmp_path, "rev-parse", "HEAD")
    with pytest.raises(CanonicalTaskRegistryError, match="CANONICAL_GIT_REGULAR_BLOB"):
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=commit, task_id="TASK-A"
        )


def test_frozen_canonical_reader_ignores_replacement_objects(tmp_path: Path) -> None:
    index, _ = _frozen_fixture(tmp_path)
    original = _frozen_commit(tmp_path)
    original_blob = _frozen_git(tmp_path, "rev-parse", f"HEAD:{canonical.CANONICAL_INDEX_PATH}")
    index["source_of_truth"] = "CORRUPT"
    _frozen_save_index(tmp_path, index)
    replacement = _frozen_commit(tmp_path)
    replacement_blob = _frozen_git(tmp_path, "rev-parse", f"HEAD:{canonical.CANONICAL_INDEX_PATH}")
    _frozen_git(tmp_path, "replace", original_blob, replacement_blob)
    assert (
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=original, task_id="TASK-A"
        )["source_commit"]
        == original
    )
    with pytest.raises(CanonicalTaskRegistryError, match="INDEX_AUTHORITY"):
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=replacement, task_id="TASK-A"
        )


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("event", "EVENT_ID_HASH"),
        ("projection", "TASK_RECORD_PROJECTION"),
        ("fragment", "FRAGMENT_CHECKSUM"),
        ("identity", "TASK_ID_HASH"),
    ],
)
def test_frozen_canonical_reader_rejects_fragment_corruption(
    tmp_path: Path, mutation: str, code: str
) -> None:
    index, fragments = _frozen_fixture(tmp_path)
    fragment = fragments["TASK-A"]
    if mutation == "event":
        fragment["events"][0]["actor"] = "tampered"
    elif mutation == "projection":
        fragment["task_record"]["next_owner"] = "tampered"
    elif mutation == "identity":
        fragment["stable_task_identity"]["task_id_sha256"] = "f" * 64
    fragment["fragment_checksum"] = canonical._payload_checksum(fragment, "fragment_checksum")
    if mutation == "fragment":
        fragment["fragment_checksum"] = "f" * 64
    raw = canonical._yaml_bytes(fragment)
    (tmp_path / canonical._canonical_fragment_path("TASK-A")).write_bytes(raw)
    index["fragments"][0].update(
        file_sha256=hashlib.sha256(raw).hexdigest(), fragment_checksum=fragment["fragment_checksum"]
    )
    previous = canonical._chain_genesis()
    for record in index["fragments"]:
        record["previous_entry_sha256"] = previous
        record["entry_sha256"] = canonical._entry_chain(previous, canonical._record_core(record))
        previous = record["entry_sha256"]
    index["final_chain_sha256"] = previous
    _frozen_save_index(tmp_path, index)
    commit = _frozen_commit(tmp_path)
    with pytest.raises(CanonicalTaskRegistryError) as error:
        canonical.read_canonical_task_at_commit(
            project_root=tmp_path, commit=commit, task_id="TASK-A"
        )
    assert error.value.code == code
