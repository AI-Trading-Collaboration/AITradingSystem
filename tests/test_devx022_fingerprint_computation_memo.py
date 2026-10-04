"""DEVX-022 S6: validation_session shares path observations only inside ONE fingerprint computation.

Kept in its own file so the historically hash-pinned tests/test_artifact_validation_session.py
is not modified by this change.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.artifacts import validation_session as validation_session_module
from ai_trading_system.platform.artifacts.validation_session import ArtifactFingerprintScope


def _deep_artifact_with_bound_files(tmp_path: Path, count: int, *, missing: bool = False) -> Path:
    """Artifact whose JSON binds ``count`` files that share one deep ancestor chain."""
    artifact_root = tmp_path / "artifact-memo"
    artifact_root.mkdir()
    shared = tmp_path / "level1" / "level2" / "level3" / "level4"
    shared.mkdir(parents=True)
    bindings: dict[str, Any] = {}
    for index in range(count):
        source = shared / f"source-{index}.csv"
        source.write_text(f"row-{index}", encoding="utf-8")
        bindings[f"source_{index}"] = {
            "path": str(source.resolve()),
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        }
    if missing:
        bindings["absent"] = {
            "path": str((shared / "absent.csv").resolve()),
            "sha256": hashlib.sha256(b"absent").hexdigest(),
        }
    (artifact_root / "input_snapshot.json").write_text(
        json.dumps(bindings, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact_root


@pytest.mark.parametrize("missing", [False, True])
def test_fingerprint_computation_memo_leaves_the_fingerprint_bytes_unchanged(
    tmp_path: Path,
    missing: bool,
) -> None:
    artifact_root = _deep_artifact_with_bound_files(tmp_path, 6, missing=missing)
    compatibility = validation_session_module._compatibility_artifact_fingerprint
    hardened = validation_session_module.artifact_fingerprint
    scope = ArtifactFingerprintScope(
        paths=(artifact_root / "input_snapshot.json",),
        discover_bound_paths=True,
    )

    # ``__wrapped__`` runs the same function outside any computation scope (no memo): the
    # reference behaviour before the memo existed.
    assert compatibility(artifact_root) == compatibility.__wrapped__(artifact_root)
    assert hardened(artifact_root, scope=scope) == hardened.__wrapped__(
        artifact_root, scope=scope
    )


def test_ancestor_link_checks_are_shared_inside_one_computation_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = _deep_artifact_with_bound_files(tmp_path, 12)
    compatibility = validation_session_module._compatibility_artifact_fingerprint
    real_is_symlink = Path.is_symlink
    observed: list[str] = []

    def spy(self: Path) -> bool:
        observed.append(str(self))
        return real_is_symlink(self)

    monkeypatch.setattr(Path, "is_symlink", spy)
    compatibility.__wrapped__(artifact_root)
    unshared = len(observed)
    observed.clear()
    compatibility(artifact_root)
    shared = list(observed)
    observed.clear()
    compatibility(artifact_root)
    second_computation = list(observed)

    # Each distinct entry/ancestor is observed once per computation, not once per bound path.
    assert len(shared) == len(set(shared))
    assert len(shared) * 3 < unshared
    # Nothing survives the computation: the next one observes everything again.
    assert second_computation == shared
    assert validation_session_module._FINGERPRINT_COMPUTATION.get() is None


def test_resolved_is_memoized_only_while_a_computation_is_active(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    target = tmp_path / "x.txt"
    target.write_text("x", encoding="utf-8")
    real_resolve = Path.resolve
    calls = 0

    def spy(self: Path, strict: bool = False) -> Path:
        nonlocal calls
        calls += 1
        return real_resolve(self, strict=strict)

    monkeypatch.setattr(Path, "resolve", spy)
    resolved = validation_session_module._resolved
    resolved(target)
    resolved(target)
    assert calls == 2  # no memo outside a computation

    token = validation_session_module._FINGERPRINT_COMPUTATION.set(
        validation_session_module._FingerprintComputationMemo()
    )
    try:
        first = resolved(target)
        second = resolved(target)
    finally:
        validation_session_module._FINGERPRINT_COMPUTATION.reset(token)
    assert calls == 3 and first == second

    resolved(target)
    assert calls == 4  # the memo ended with the computation


def test_link_swapped_in_after_a_clean_computation_is_still_detected(tmp_path: Path) -> None:
    artifact_root = _deep_artifact_with_bound_files(tmp_path, 3)
    compatibility = validation_session_module._compatibility_artifact_fingerprint
    clean = compatibility(artifact_root)
    assert clean == compatibility(artifact_root)

    level2 = tmp_path / "level1" / "level2"
    moved = tmp_path / "moved-level2"
    level2.rename(moved)
    try:
        level2.symlink_to(moved, target_is_directory=True)
    except OSError as exc:
        moved.rename(level2)
        pytest.skip(f"directory symlink unavailable: {exc}")

    with pytest.raises(validation_session_module._UncacheableFingerprintScope):
        compatibility(artifact_root)
    # The failed computation also released its memo.
    assert validation_session_module._FINGERPRINT_COMPUTATION.get() is None


def _reference_windows_change_token(path: Path) -> int | None:
    """The pre-S6 implementation: rebuilds the structure class and kernel32 binding per call."""
    import ctypes
    from ctypes import wintypes

    class FileBasicInfo(ctypes.Structure):
        _fields_ = (
            ("creation_time", ctypes.c_longlong),
            ("last_access_time", ctypes.c_longlong),
            ("last_write_time", ctypes.c_longlong),
            ("change_time", ctypes.c_longlong),
            ("file_attributes", wintypes.DWORD),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    get_information = kernel32.GetFileInformationByHandleEx
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    get_information.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    handle = create_file(str(path), 0x0080, 0x0001 | 0x0002 | 0x0004, None, 3, 0x02000000, None)
    if handle == wintypes.HANDLE(-1).value:
        return None
    try:
        info = FileBasicInfo()
        if not get_information(handle, 0, ctypes.byref(info), ctypes.sizeof(info)):
            return None
        return int(info.change_time)
    finally:
        close_handle(handle)


@pytest.mark.skipif(os.name != "nt", reason="Windows change token")
def test_platform_change_token_matches_the_per_call_reference_and_binds_once(
    tmp_path: Path,
) -> None:
    present = tmp_path / "present.txt"
    present.write_text("a", encoding="utf-8")
    missing = tmp_path / "missing.txt"
    token = validation_session_module._platform_change_token

    assert token(present) == _reference_windows_change_token(present)
    assert token(missing) is None and _reference_windows_change_token(missing) is None
    before = token(present)
    present.write_text("b", encoding="utf-8")
    after = token(present)
    assert after == _reference_windows_change_token(present)
    assert isinstance(before, int) and isinstance(after, int) and after >= before
    api = validation_session_module._windows_change_token_api
    assert api() is api()
