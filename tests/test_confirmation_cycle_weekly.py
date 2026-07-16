from __future__ import annotations

import json
import os
import select
import signal
import threading
from collections import OrderedDict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_confirmation_cycle_helpers import register_targets_fixture, write_progress_sources
from dynamic_v3_outcome_loop_helpers import build_ready_outcome_update_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_confirmation_cycle as confirmation_cycle
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    run_confirmation_cycle_weekly,
    validate_confirmation_cycle_weekly_artifact,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def _forward_plan_scope_cache_test_entry(
    key: tuple[str, ...],
) -> tuple[Any, int]:
    value = (("parent-only",), (), ())
    return value, confirmation_cycle._forward_plan_scope_cache_retained_bytes(key, value)


def _tuple_tree_contains_path(value: object) -> bool:
    if isinstance(value, Path):
        return True
    return isinstance(value, tuple) and any(_tuple_tree_contains_path(item) for item in value)


def _mapping_with_fields(value: object, fields: set[str]) -> dict[str, Any]:
    stack = [value]
    while stack:
        candidate = stack.pop()
        if isinstance(candidate, dict):
            if fields <= set(candidate):
                return candidate
            stack.extend(candidate.values())
        elif isinstance(candidate, list):
            stack.extend(candidate)
    raise AssertionError(f"snapshot mapping missing fields: {sorted(fields)}")


def test_forward_plan_scope_cache_pid_guard_discards_inherited_state() -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK
    stale_lock = threading.Lock()
    stale_lock.acquire()
    key = ("parent-only",)
    entry = _forward_plan_scope_cache_test_entry(key)
    try:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = OrderedDict({key: entry})
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = entry[1]
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = os.getpid() + 1
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = stale_lock

        confirmation_cycle._ensure_forward_plan_scope_cache_owner()

        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID == os.getpid()
        assert not confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == 0
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK is not stale_lock
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK.acquire(timeout=1.0)
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK.release()
    finally:
        stale_lock.release()
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


def test_forward_plan_scope_cache_reset_clears_retained_bytes() -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK
    stale_lock = threading.Lock()
    key = ("parent-only",)
    entry = _forward_plan_scope_cache_test_entry(key)
    try:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = OrderedDict({key: entry})
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = entry[1]
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = stale_lock

        confirmation_cycle._reset_forward_plan_scope_cache_after_fork()

        assert not confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == 0
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID == os.getpid()
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK is not stale_lock
    finally:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


def test_forward_plan_scope_cache_serializes_paths_and_evicts_by_retained_bytes(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK
    confirmation_cycle._reset_forward_plan_scope_cache_after_fork()
    first_key = ("first",)
    second_key = ("second",)
    third_key = ("third",)
    first_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=(tmp_path / "first",),
        metadata_paths=(tmp_path / "first-metadata",),
        discover_bound_paths=False,
    )
    second_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=(tmp_path / "second",),
        discover_bound_paths=False,
    )
    third_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=(tmp_path / "third",),
        discover_bound_paths=False,
    )
    try:
        assert confirmation_cycle._forward_plan_scope_cache_put(first_key, first_scope)
        assert confirmation_cycle._forward_plan_scope_cache_get(first_key) == first_scope
        assert confirmation_cycle._forward_plan_scope_cache_put(second_key, second_scope)
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == sum(
            retained_bytes
            for _, retained_bytes in confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE.values()
        )
        for key, entry in confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE.items():
            assert not _tuple_tree_contains_path(key)
            assert not _tuple_tree_contains_path(entry)
            assert entry[1] == confirmation_cycle._forward_plan_scope_cache_retained_bytes(
                key,
                entry[0],
            )

        third_value = confirmation_cycle._forward_plan_scope_cache_value(third_scope)
        assert third_value is not None
        third_retained_bytes = confirmation_cycle._forward_plan_scope_cache_retained_bytes(
            third_key,
            third_value,
        )
        monkeypatch.setattr(
            confirmation_cycle,
            "_FORWARD_PLAN_SCOPE_CACHE_MAX_BYTES",
            confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES + third_retained_bytes - 1,
        )

        assert confirmation_cycle._forward_plan_scope_cache_put(third_key, third_scope)
        assert tuple(confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE) == (
            second_key,
            third_key,
        )
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == sum(
            retained_bytes
            for _, retained_bytes in confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE.values()
        )
    finally:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


@pytest.mark.parametrize(
    ("limit_name", "limit_value", "rejected_paths"),
    (
        ("_FORWARD_PLAN_SCOPE_PATH_MAX_UTF8_BYTES", 4, (Path("12345"),)),
        (
            "_FORWARD_PLAN_SCOPE_TOTAL_UTF8_BYTES",
            15,
            (Path("alpha"), Path("beta")),
        ),
        (
            "_FORWARD_PLAN_SCOPE_PATH_MAX_COMPONENTS",
            1,
            (Path("one") / "two",),
        ),
        (
            "_FORWARD_PLAN_SCOPE_TOTAL_COMPONENTS",
            5,
            (Path("a"), Path("b"), Path("c")),
        ),
    ),
)
def test_forward_plan_scope_cache_rejects_path_budget_without_promotion(
    monkeypatch: Any,
    limit_name: str,
    limit_value: int,
    rejected_paths: tuple[Path, ...],
) -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK

    def lexical_resolve(path: Path, *, strict: bool = False) -> Path:
        del strict
        return path

    monkeypatch.setattr(Path, "resolve", lexical_resolve)
    confirmation_cycle._reset_forward_plan_scope_cache_after_fork()
    target_key = ("target", limit_name)
    recent_key = ("recent", limit_name)
    cached_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=(Path("cached"),),
        discover_bound_paths=False,
    )
    recent_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=(Path("recent"),),
        discover_bound_paths=False,
    )
    rejected_scope = confirmation_cycle.ArtifactFingerprintScope(
        paths=rejected_paths,
        discover_bound_paths=False,
    )
    try:
        assert confirmation_cycle._forward_plan_scope_cache_put(target_key, cached_scope)
        assert confirmation_cycle._forward_plan_scope_cache_put(recent_key, recent_scope)
        order_before = tuple(confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE)
        target_entry_before = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE[target_key]
        retained_bytes_before = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
        assert confirmation_cycle._forward_plan_scope_cache_value(rejected_scope) is not None
        monkeypatch.setattr(confirmation_cycle, limit_name, limit_value)

        assert confirmation_cycle._forward_plan_scope_cache_value(rejected_scope) is None
        assert not confirmation_cycle._forward_plan_scope_cache_put(
            target_key,
            rejected_scope,
        )
        assert tuple(confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE) == order_before
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE[target_key] == target_entry_before
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == retained_bytes_before
    finally:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


@pytest.mark.parametrize(
    "budget_case",
    (
        "single_utf8_bytes",
        "total_utf8_bytes",
        "single_components",
        "total_components",
    ),
)
def test_forward_plan_resolver_rejects_snapshot_path_budget_before_io_and_falls_back(
    tmp_path: Path,
    monkeypatch: Any,
    budget_case: str,
) -> None:
    confirmation_plan_id = f"budget-{budget_case}"
    plan_dir = tmp_path / confirmation_plan_id
    plan_dir.mkdir()
    snapshot_path = plan_dir / "forward_confirmation_plan_input_snapshot.json"
    resolved_snapshot_path = snapshot_path.resolve(strict=False)
    observed_base_paths = {
        str(plan_dir),
        str(snapshot_path),
        str(resolved_snapshot_path),
    }
    safe_path = tmp_path / f"safe-{budget_case}.json"
    offending_path_text: str
    if budget_case == "single_utf8_bytes":
        path_limit = max(len(path.encode("utf-8")) for path in observed_base_paths) + 8
        offending_path_text = "x" * (path_limit + 1)
        monkeypatch.setattr(
            confirmation_cycle,
            "_FORWARD_PLAN_SCOPE_PATH_MAX_UTF8_BYTES",
            path_limit,
        )
    elif budget_case == "total_utf8_bytes":
        offending_path_text = str(tmp_path / "offending-total-bytes.json")
        total_limit = sum(len(path.encode("utf-8")) for path in observed_base_paths)
        total_limit += len(str(safe_path).encode("utf-8"))
        total_limit += len(offending_path_text.encode("utf-8")) - 1
        monkeypatch.setattr(
            confirmation_cycle,
            "_FORWARD_PLAN_SCOPE_TOTAL_UTF8_BYTES",
            total_limit,
        )
    elif budget_case == "single_components":
        path_limit = max(len(Path(path).parts) for path in observed_base_paths) + 1
        offending_path_text = "/".join("x" for _ in range(path_limit + 1))
        monkeypatch.setattr(
            confirmation_cycle,
            "_FORWARD_PLAN_SCOPE_PATH_MAX_COMPONENTS",
            path_limit,
        )
    else:
        offending_path_text = str(tmp_path / "offending-total-components.json")
        total_limit = sum(len(Path(path).parts) for path in observed_base_paths)
        total_limit += len(safe_path.parts)
        total_limit += len(Path(offending_path_text).parts) - 1
        monkeypatch.setattr(
            confirmation_cycle,
            "_FORWARD_PLAN_SCOPE_TOTAL_COMPONENTS",
            total_limit,
        )
    payload = {
        "report_type": "etf_dynamic_v3_forward_confirmation_plan_input_snapshot",
        "schema_version": (
            confirmation_cycle.backtest_simulation.FORWARD_CONFIRMATION_PLAN_SNAPSHOT_SCHEMA_VERSION
        ),
    }
    if budget_case.startswith("total_"):
        payload["safe_path"] = str(safe_path)
    payload["offending_path"] = offending_path_text
    snapshot_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    real_resolve = Path.resolve
    real_stat = Path.stat
    resolved_paths: list[str] = []
    stat_paths: list[str] = []

    def tracked_resolve(path: Path, *args: Any, **kwargs: Any) -> Path:
        resolved_paths.append(str(path))
        return real_resolve(path, *args, **kwargs)

    def tracked_stat(path: Path, *args: Any, **kwargs: Any) -> os.stat_result:
        stat_paths.append(str(path))
        return real_stat(path, *args, **kwargs)

    validator_calls = 0

    def counted_validator(**kwargs: Any) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {
            "status": "PASS",
            "confirmation_plan_id": kwargs["confirmation_plan_id"],
        }

    monkeypatch.setattr(Path, "resolve", tracked_resolve)
    monkeypatch.setattr(Path, "stat", tracked_stat)
    monkeypatch.setattr(
        confirmation_cycle,
        "validate_forward_confirmation_plan_artifact",
        counted_validator,
    )

    result = confirmation_cycle._validated_forward_confirmation_plan(
        confirmation_plan_id=confirmation_plan_id,
        output_dir=tmp_path,
    )

    assert result["status"] == "PASS"
    assert validator_calls == 1
    assert str(snapshot_path) in resolved_paths
    assert str(snapshot_path) in stat_paths
    assert offending_path_text not in resolved_paths
    assert offending_path_text not in stat_paths


@pytest.mark.parametrize(
    "invalid_live_topology",
    (
        "thirty_third_inventory_entry",
        "nested_inventory_directory",
        "missing_explicit_became_directory",
    ),
)
def test_forward_plan_scope_cache_hit_discards_invalid_live_topology_and_falls_back(
    tmp_path: Path,
    monkeypatch: Any,
    invalid_live_topology: str,
) -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK
    confirmation_cycle._reset_forward_plan_scope_cache_after_fork()
    confirmation_plan_id = f"live-precondition-{invalid_live_topology}"
    plan_dir = tmp_path / confirmation_plan_id
    plan_dir.mkdir()
    (plan_dir / "forward_confirmation_plan_input_snapshot.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    inventory_root = tmp_path / f"inventory-{invalid_live_topology}"
    future_explicit_path = tmp_path / "future-explicit-path"
    if invalid_live_topology == "thirty_third_inventory_entry":
        inventory_root.mkdir()
        for index in range(confirmation_cycle._FORWARD_PLAN_MAX_FILES_PER_DIR + 1):
            (inventory_root / f"entry-{index:02d}.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
    elif invalid_live_topology == "nested_inventory_directory":
        inventory_root.mkdir()
        (inventory_root / "nested").mkdir()
    if invalid_live_topology == "missing_explicit_became_directory":
        scope = confirmation_cycle.ArtifactFingerprintScope(
            paths=(future_explicit_path,),
            discover_bound_paths=False,
        )
    else:
        scope = confirmation_cycle.ArtifactFingerprintScope(
            inventories=(
                confirmation_cycle.ArtifactFingerprintInventory(
                    root=inventory_root,
                    patterns=("*",),
                ),
            ),
            discover_bound_paths=False,
        )

    real_cache_get = confirmation_cycle._forward_plan_scope_cache_get
    inserted_key: tuple[str, ...] | None = None
    inserted_bytes = 0

    def injecting_cache_get(key: tuple[str, ...]) -> Any:
        nonlocal inserted_bytes
        nonlocal inserted_key

        assert inserted_key is None
        assert confirmation_cycle._forward_plan_scope_cache_put(key, scope)
        inserted_key = key
        inserted_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
        if invalid_live_topology == "missing_explicit_became_directory":
            assert not future_explicit_path.exists()
            future_explicit_path.mkdir()
        return real_cache_get(key)

    snapshot_parser_calls = 0

    def forbidden_snapshot_parser(path: Path | None) -> dict[str, Any] | None:
        nonlocal snapshot_parser_calls
        snapshot_parser_calls += 1
        raise AssertionError(f"cache-hit fallback unexpectedly parsed snapshot: {path}")

    validator_calls = 0

    def counted_validator(**kwargs: Any) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {
            "status": "PASS",
            "confirmation_plan_id": kwargs["confirmation_plan_id"],
        }

    real_dependency_scope = confirmation_cycle._forward_plan_dependency_scope
    resolver_results: list[Any] = []

    def tracked_dependency_scope(plan_path: Path) -> Any:
        result = real_dependency_scope(plan_path)
        resolver_results.append(result)
        return result

    monkeypatch.setattr(
        confirmation_cycle,
        "_forward_plan_scope_cache_get",
        injecting_cache_get,
    )
    monkeypatch.setattr(
        confirmation_cycle,
        "_forward_plan_dependency_scope",
        tracked_dependency_scope,
    )
    monkeypatch.setattr(confirmation_cycle, "_read_optional_json", forbidden_snapshot_parser)
    monkeypatch.setattr(
        confirmation_cycle,
        "validate_forward_confirmation_plan_artifact",
        counted_validator,
    )
    try:
        result = confirmation_cycle._validated_forward_confirmation_plan(
            confirmation_plan_id=confirmation_plan_id,
            output_dir=tmp_path,
        )

        assert result["status"] == "PASS"
        assert validator_calls == 1
        assert resolver_results == [None]
        assert snapshot_parser_calls == 0
        assert inserted_key is not None
        assert inserted_bytes > 0
        assert inserted_key not in confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == 0
    finally:
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


@pytest.mark.skipif(
    os.name == "nt" or not hasattr(os, "fork") or not hasattr(os, "register_at_fork"),
    reason="requires POSIX fork callbacks",
)
def test_forward_plan_scope_cache_fork_child_rebuilds_locked_parent_state() -> None:
    original_cache = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
    original_bytes = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES
    original_pid = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID
    original_lock = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK
    stale_lock = threading.Lock()
    lock_held = threading.Event()
    release_lock = threading.Event()
    child_pid: int | None = None
    read_fd = -1
    write_fd = -1
    key = ("parent-only",)
    entry = _forward_plan_scope_cache_test_entry(key)

    def hold_stale_lock() -> None:
        with stale_lock:
            lock_held.set()
            release_lock.wait(timeout=10.0)

    holder = threading.Thread(target=hold_stale_lock, daemon=True)
    holder.start()
    try:
        assert lock_held.wait(timeout=2.0)
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = OrderedDict({key: entry})
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = entry[1]
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = os.getpid()
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = stale_lock
        read_fd, write_fd = os.pipe()

        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_fd)
            try:
                callback_owner_reset = (
                    confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID == os.getpid()
                )
                callback_cache_cleared = not confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
                callback_bytes_cleared = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == 0
                callback_lock_rebuilt = (
                    confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK is not stale_lock
                )
                confirmation_cycle._ensure_forward_plan_scope_cache_owner()
                guard_owner_matches = (
                    confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID == os.getpid()
                )
                guard_cache_empty = not confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
                guard_bytes_zero = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == 0
                child_lock_acquired = confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK.acquire(
                    timeout=1.0
                )
                if child_lock_acquired:
                    confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK.release()
                checks = (
                    callback_owner_reset,
                    callback_cache_cleared,
                    callback_bytes_cleared,
                    callback_lock_rebuilt,
                    guard_owner_matches,
                    guard_cache_empty,
                    guard_bytes_zero,
                    child_lock_acquired,
                )
                os.write(write_fd, "".join("1" if check else "0" for check in checks).encode())
            except BaseException:
                os.write(write_fd, b"ERROR")
            finally:
                os.close(write_fd)
                os._exit(0)

        os.close(write_fd)
        write_fd = -1
        readable, _, _ = select.select([read_fd], [], [], 5.0)
        if not readable:
            os.kill(child_pid, getattr(signal, "SIGKILL", signal.SIGTERM))
            os.waitpid(child_pid, 0)
            child_pid = None
            pytest.fail("fork child deadlocked on inherited forward Plan scope cache state")
        payload = os.read(read_fd, 64)
        waited_pid, wait_status = os.waitpid(child_pid, 0)
        child_pid = None

        assert waited_pid > 0
        assert os.waitstatus_to_exitcode(wait_status) == 0
        assert payload == b"11111111"
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK is stale_lock
        assert key in confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE
        assert confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES == entry[1]
    finally:
        if child_pid is not None:
            try:
                os.kill(child_pid, getattr(signal, "SIGKILL", signal.SIGTERM))
            except ProcessLookupError:
                pass
            try:
                os.waitpid(child_pid, 0)
            except ChildProcessError:
                pass
        if read_fd >= 0:
            os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)
        release_lock.set()
        holder.join(timeout=2.0)
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE = original_cache
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_BYTES = original_bytes
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_PID = original_pid
        confirmation_cycle._FORWARD_PLAN_SCOPE_CACHE_LOCK = original_lock


@with_artifact_validation_session
def test_confirmation_cycle_weekly_dry_run_skips_outcome_update(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    real_plan_validator = confirmation_cycle.validate_forward_confirmation_plan_artifact
    real_scope_snapshot_reader = confirmation_cycle._read_optional_json
    plan_validation_calls = 0
    scope_snapshot_reads = 0

    def counted_plan_validator(**kwargs: Any) -> dict[str, Any]:
        nonlocal plan_validation_calls
        plan_validation_calls += 1
        return real_plan_validator(**kwargs)

    monkeypatch.setattr(
        confirmation_cycle,
        "validate_forward_confirmation_plan_artifact",
        counted_plan_validator,
    )

    def counted_scope_snapshot_reader(path: Path | None) -> dict[str, Any] | None:
        nonlocal scope_snapshot_reads
        if path is not None and path.name == "forward_confirmation_plan_input_snapshot.json":
            scope_snapshot_reads += 1
        return real_scope_snapshot_reader(path)

    monkeypatch.setattr(
        confirmation_cycle,
        "_read_optional_json",
        counted_scope_snapshot_reader,
    )
    registry_fixture = register_targets_fixture(tmp_path / "confirmation")
    plan_scope = confirmation_cycle._forward_plan_dependency_scope(
        Path(registry_fixture["confirmation_plan"]["confirmation_plan_dir"])
    )
    assert plan_scope is not None
    assert len(plan_scope.inventories) == 12
    assert len(plan_scope.paths) >= 11
    assert plan_scope.discover_bound_paths is False
    etf_config_root = confirmation_cycle.PROJECT_ROOT / "config" / "etf_portfolio"
    price_path = registry_fixture["prices_path"]
    required_fixed_paths = {
        confirmation_cycle.PROJECT_ROOT / "config" / "universe.yaml",
        confirmation_cycle.PROJECT_ROOT / "config" / "data_quality.yaml",
        *(
            etf_config_root / name
            for name in (
                "assets.yaml",
                "strategy.yaml",
                "risk.yaml",
                "backtest.yaml",
                "p1.yaml",
                "p2.yaml",
            )
        ),
        confirmation_cycle.backtest_simulation.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT
        / "shadow_shortlist"
        / "shadow-shortlist-1"
        / "shadow_shortlist_candidates.jsonl",
        price_path.parent / "download_manifests" / "prices_daily_download_manifest.json",
        price_path.with_name("prices_marketstack_daily.csv"),
    }
    assert required_fixed_paths <= set(plan_scope.paths)
    assert (
        Path(registry_fixture["confirmation_plan"]["input_snapshot"]["policy_bundle"]["path"])
        in plan_scope.paths
    )
    assert registry_fixture["prices_path"] in plan_scope.paths
    assert registry_fixture["rates_path"] in plan_scope.paths
    assert registry_fixture["position_config_path"] in plan_scope.paths
    assert registry_fixture["prices_path"] in plan_scope.metadata_paths
    assert registry_fixture["rates_path"] in plan_scope.metadata_paths
    write_progress_sources(registry_fixture)
    outcome_root = tmp_path / "outcome"
    outcome_root.mkdir()
    outcome_fixture = build_ready_outcome_update_fixture(outcome_root, monkeypatch)

    result = run_confirmation_cycle_weekly(
        week_ending=date(2026, 7, 31),
        execute_ready_updates=False,
        registry_id=registry_fixture["registry"]["registry_id"],
        output_dir=tmp_path / "weekly",
        outcome_due_dir=tmp_path / "weekly_due",
        outcome_update_review_dir=tmp_path / "weekly_update_review",
        outcome_update_dir=tmp_path / "weekly_update",
        rolling_refresh_dir=tmp_path / "weekly_refresh",
        evidence_trend_dir=tmp_path / "weekly_trend",
        forward_decision_dir=tmp_path / "weekly_decision",
        registry_dir=registry_fixture["registry_dir"],
        progress_dir=tmp_path / "weekly_progress",
        evaluation_dir=tmp_path / "weekly_evaluation",
        rule_cycle_dir=tmp_path / "weekly_rule_cycle",
        queue_dir=tmp_path / "weekly_queue",
        rule_owner_decision_journal_path=registry_fixture["journal_path"],
        dashboard_dir=tmp_path / "weekly_dashboard",
        pressure_tag_dir=tmp_path / "pressure_tag",
        advisory_outcome_dir=outcome_fixture["outcome"]["outcome_dir"].parent,
        limited_vs_notrade_dir=registry_fixture["limited_dir"],
        consensus_risk_dir=registry_fixture["consensus_dir"],
        prices_path=outcome_fixture["prices_path"],
        rates_path=outcome_fixture["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
    )

    steps = {row["step"]: row for row in result["weekly_cycle_steps"]["steps"]}
    summary = result["weekly_cycle_summary"]
    assert result["manifest"]["dry_run"] is True
    assert steps["outcome_update"]["status"] == "SKIPPED"
    assert steps["outcome_update"]["reason"] == "execute_ready_updates_false"
    assert summary["updated_windows"] == 0
    assert summary["ready_for_evaluation"] == 0
    assert summary["rule_review_recommendation"] == "continue_tracking"
    assert (
        validate_confirmation_cycle_weekly_artifact(
            weekly_cycle_id=result["weekly_cycle_id"],
            output_dir=tmp_path / "weekly",
        )["status"]
        == "PASS"
    )
    assert plan_validation_calls == 1

    metadata_paths = (
        registry_fixture["prices_path"],
        registry_fixture["rates_path"],
    )
    metadata_bytes = {path: path.read_bytes() for path in metadata_paths}
    metadata_stats = {path: path.stat() for path in metadata_paths}
    try:
        for expected_calls, path in enumerate(metadata_paths, start=2):
            path_stat = metadata_stats[path]
            os.utime(
                path,
                ns=(
                    path_stat.st_atime_ns,
                    path_stat.st_mtime_ns + expected_calls * 1_000_000_000,
                ),
            )
            assert path.read_bytes() == metadata_bytes[path]
            assert (
                confirmation_cycle.validate_confirmation_targets_artifact(
                    registry_id=registry_fixture["registry"]["registry_id"],
                    output_dir=registry_fixture["registry_dir"],
                )["status"]
                == "FAIL"
            )
            assert plan_validation_calls == expected_calls
    finally:
        for path in metadata_paths:
            path_stat = metadata_stats[path]
            os.utime(path, ns=(path_stat.st_atime_ns, path_stat.st_mtime_ns))
            assert path.read_bytes() == metadata_bytes[path]

    deep_source_path = (
        Path(registry_fixture["calibration"]["calibration_pack_dir"]) / "reader_brief_section.md"
    )
    deep_source_bytes = deep_source_path.read_bytes()
    try:
        deep_source_path.write_text("tampered\n", encoding="utf-8")
        assert (
            confirmation_cycle.validate_confirmation_targets_artifact(
                registry_id=registry_fixture["registry"]["registry_id"],
                output_dir=registry_fixture["registry_dir"],
            )["status"]
            == "FAIL"
        )
        assert plan_validation_calls == 4
    finally:
        deep_source_path.write_bytes(deep_source_bytes)
    assert (
        confirmation_cycle.validate_confirmation_targets_artifact(
            registry_id=registry_fixture["registry"]["registry_id"],
            output_dir=registry_fixture["registry_dir"],
        )["status"]
        == "PASS"
    )

    summary_path = Path(result["weekly_cycle_dir"]) / "weekly_cycle_summary.json"
    summary_path.write_text("{}\n", encoding="utf-8")
    assert (
        validate_confirmation_cycle_weekly_artifact(
            weekly_cycle_id=result["weekly_cycle_id"],
            output_dir=tmp_path / "weekly",
        )["status"]
        == "FAIL"
    )
    assert plan_validation_calls == 4
    assert scope_snapshot_reads == 1

    plan_dir = Path(registry_fixture["confirmation_plan"]["confirmation_plan_dir"])
    plan_snapshot_path = plan_dir / "forward_confirmation_plan_input_snapshot.json"
    original_plan_snapshot = plan_snapshot_path.read_bytes()
    known_relative_fields = {
        "position_advisory_config",
        "price_cache_path",
        "rates_cache_path",
    }
    relative_values = {
        "position_advisory_config": "config/etf_portfolio/relative-position.yaml",
        "price_cache_path": "data/cache/relative-prices.csv",
        "rates_cache_path": "data/cache/relative-rates.csv",
    }
    try:
        plan_snapshot = json.loads(original_plan_snapshot)
        relative_source = _mapping_with_fields(plan_snapshot, known_relative_fields)
        relative_source.update(relative_values)
        plan_snapshot_path.write_text(
            json.dumps(plan_snapshot, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        relative_scope = confirmation_cycle._forward_plan_dependency_scope(plan_dir)
        assert relative_scope is not None
        expected_project_paths = {
            confirmation_cycle.backtest_simulation._resolve_project_path(Path(path_text))
            for path_text in relative_values.values()
        }
        assert expected_project_paths <= set(relative_scope.paths)

        relative_source["unexpected_cache_path"] = "config/relative-unknown.json"
        plan_snapshot_path.write_text(
            json.dumps(plan_snapshot, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        assert confirmation_cycle._forward_plan_dependency_scope(plan_dir) is None
    finally:
        plan_snapshot_path.write_bytes(original_plan_snapshot)


@with_artifact_validation_session
def test_forward_plan_cache_bypasses_malformed_dependency_snapshot(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    confirmation_plan_id = "malformed-plan"
    plan_dir = tmp_path / confirmation_plan_id
    plan_dir.mkdir()
    (plan_dir / "forward_confirmation_plan_input_snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": "forward_confirmation_plan_input_snapshot.v3",
                "report_type": "etf_dynamic_v3_forward_confirmation_plan_input_snapshot",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    calls = 0

    def counted_validator(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "confirmation_plan_id": kwargs["confirmation_plan_id"]}

    monkeypatch.setattr(
        confirmation_cycle,
        "validate_forward_confirmation_plan_artifact",
        counted_validator,
    )
    for _ in range(2):
        result = confirmation_cycle._validated_forward_confirmation_plan(
            confirmation_plan_id=confirmation_plan_id,
            output_dir=tmp_path,
        )

    assert calls == 2
    assert result["status"] == "PASS"
