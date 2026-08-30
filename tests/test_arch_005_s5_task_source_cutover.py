from __future__ import annotations

import copy
import hashlib
import json
from argparse import Namespace
from pathlib import Path

import pytest

import scripts.architecture_arch005_task_source as task_source_cli
from ai_trading_system.config import PROJECT_ROOT
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
    assert registry.index["task_count"] == len(registry.fragments) == 1036
    assert registry.index["missing_task_count"] == 0
    assert registry.index["duplicate_task_count"] == 0
    assert registry.index["governance_cycle_count"] >= 2
    assert registry.index["manual_row_move_workflow_enabled"] is False


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
