"""Capture hold (protocol v3): ownership, drift and retained-proof behaviour on a real git repo."""

from __future__ import annotations

import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.data import capture_hold
from ai_trading_system.data.capture_hold import (
    CaptureHold,
    CaptureHoldError,
    acquire_capture_hold,
    recheck_capture_hold,
    release_capture_hold,
    release_capture_hold_by_id,
    restore_capture_hold,
    verify_retained_capture_hold_proof,
)

PATHS = ("outputs/architecture/capture_a",)


def _git(root: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-c", "user.email=t@example.com", "-c", "user.name=t", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path.resolve()
    _git(root, "init", "-q")
    (root / "src").mkdir()
    (root / "src" / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    return root, _git(root, "rev-parse", "HEAD")


def _acquire(
    root: Path, commit: str, paths: tuple[str, ...] = PATHS, ttl: int = 600
) -> CaptureHold:
    return acquire_capture_hold(
        execution_root=root,
        candidate_commit=commit,
        required_paths=paths,
        actor="tester",
        ttl_seconds=ttl,
    )


def _code(exc: pytest.ExceptionInfo[CaptureHoldError]) -> str:
    return exc.value.code


def test_acquire_recheck_and_retained_proof_round_trip(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    proof = recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    assert proof["status"] == "PASS"
    assert proof["provenance"]["commit"] == commit and proof["provenance"]["code_modified"] is False
    assert proof["hold_acquired_or_mutated"] is False
    verify_retained_capture_hold_proof(
        proof,
        execution_root=root,
        candidate_commit=commit,
        required_paths=PATHS,
        source_hold_id=hold.hold_id,
        checked_at=capture_hold.parse_utc_datetime(proof["checked_at"]),
    )


def test_restore_by_id_rebuilds_a_handle_that_rechecks(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    restored = restore_capture_hold(
        execution_root=root, hold_id=hold.hold_id, candidate_commit=commit, required_paths=PATHS
    )
    assert restored.hold_id == hold.hold_id
    recheck_capture_hold(restored, candidate_commit=commit, required_paths=PATHS)


@pytest.mark.parametrize(
    "other",
    [
        ("outputs/architecture/capture_a",),
        ("outputs/architecture/capture_a/sub",),
        ("outputs/architecture",),
        ("OUTPUTS/Architecture/CAPTURE_A",),
    ],
)
def test_overlapping_paths_conflict_even_with_case_or_nesting(
    repo: tuple[Path, str], other: tuple[str, ...]
) -> None:
    root, commit = repo
    _acquire(root, commit)
    with pytest.raises(CaptureHoldError) as exc:
        _acquire(root, commit, other)
    assert _code(exc) == "CAPTURE_HOLD_PATH_CONFLICT"


def test_disjoint_paths_can_be_held_together(repo: tuple[Path, str]) -> None:
    root, commit = repo
    _acquire(root, commit)
    other = _acquire(root, commit, ("outputs/architecture/capture_b",))
    recheck_capture_hold(
        other, candidate_commit=commit, required_paths=("outputs/architecture/capture_b",)
    )


def test_release_frees_paths_and_deactivates_the_old_hold(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    release_capture_hold(hold)
    with pytest.raises(CaptureHoldError) as exc:
        recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    assert _code(exc) == "CAPTURE_HOLD_REQUIRED"
    with pytest.raises(CaptureHoldError) as inactive:
        restore_capture_hold(
            execution_root=root, hold_id=hold.hold_id, candidate_commit=commit, required_paths=PATHS
        )
    assert _code(inactive) == "CAPTURE_HOLD_INACTIVE"
    _acquire(root, commit)


def test_operator_release_by_id_clears_a_crashed_holder(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    release_capture_hold_by_id(execution_root=root, hold_id=hold.hold_id)
    _acquire(root, commit)


def test_expired_hold_neither_rechecks_nor_blocks(
    repo: tuple[Path, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, commit = repo
    hold = _acquire(root, commit, ttl=60)
    later = hold.expires_at + timedelta(seconds=1)
    monkeypatch.setattr(capture_hold, "_now", lambda: later)
    with pytest.raises(CaptureHoldError) as exc:
        recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    assert _code(exc) == "CAPTURE_HOLD_INACTIVE"
    _acquire(root, commit)


def test_new_commit_after_acquisition_is_drift(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    (root / "src" / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "commit", "-q", "-am", "change")
    with pytest.raises(CaptureHoldError) as exc:
        recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    assert _code(exc) == "CAPTURE_HOLD_COMMIT_DRIFT"


def test_uncommitted_code_change_blocks_acquire_and_recheck(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    (root / "src" / "module.py").write_text("VALUE = 3\n", encoding="utf-8")
    with pytest.raises(CaptureHoldError) as exc:
        recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    assert _code(exc) == "CAPTURE_HOLD_CODE_MODIFIED"
    release_capture_hold_by_id(execution_root=root, hold_id=hold.hold_id)
    with pytest.raises(CaptureHoldError) as again:
        _acquire(root, commit)
    assert _code(again) == "CAPTURE_HOLD_CODE_MODIFIED"


def test_acquire_for_another_commit_is_refused(repo: tuple[Path, str]) -> None:
    root, _ = repo
    with pytest.raises(CaptureHoldError) as exc:
        _acquire(root, "0" * 40)
    assert _code(exc) == "CAPTURE_HOLD_COMMIT_DRIFT"


def test_handle_built_by_the_caller_grants_nothing(repo: tuple[Path, str]) -> None:
    root, commit = repo
    real = _acquire(root, commit)
    forged_record = {**real.record, "hold_id": "hold-" + "0" * 20}
    forged = CaptureHold(root, forged_record, b"{}")
    with pytest.raises(CaptureHoldError) as exc:
        recheck_capture_hold(forged, candidate_commit=commit, required_paths=PATHS)
    assert _code(exc) == "CAPTURE_HOLD_INACTIVE"
    with pytest.raises(CaptureHoldError) as widened:
        recheck_capture_hold(real, candidate_commit=commit, required_paths=("outputs/other",))
    assert _code(widened) == "CAPTURE_HOLD_SCOPE_INVALID"


@pytest.mark.parametrize("ttl", [0, -1, capture_hold.MAX_HOLD_SECONDS + 1])
def test_ttl_outside_the_bound_is_rejected(repo: tuple[Path, str], ttl: int) -> None:
    root, commit = repo
    with pytest.raises(CaptureHoldError) as exc:
        _acquire(root, commit, ttl=ttl)
    assert _code(exc) == "CAPTURE_HOLD_TTL_INVALID"


@pytest.mark.parametrize("paths", [(), ("/abs",), ("a/../b",), ("a", "A")])
def test_invalid_paths_are_rejected(repo: tuple[Path, str], paths: tuple[str, ...]) -> None:
    root, commit = repo
    with pytest.raises(Exception):  # noqa: B017 - EventBinding path syntax or the hold error
        _acquire(root, commit, paths)


def _verify(proof: dict[str, Any], root: Path, commit: str, hold_id: str, **changes: Any) -> None:
    arguments: dict[str, Any] = {
        "execution_root": root,
        "candidate_commit": commit,
        "required_paths": PATHS,
        "source_hold_id": hold_id,
        "checked_at": capture_hold.parse_utc_datetime(proof["checked_at"]),
    }
    arguments.update(changes)
    verify_retained_capture_hold_proof(proof, **arguments)


def test_retained_proof_rejects_tampering(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    proof = recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    _verify(proof, root, commit, hold.hold_id)
    bad_bytes = {**proof, "hold_record_bytes_hex": proof["hold_record_bytes_hex"][:-2] + "00"}
    bad_record = {**proof, "active_hold": {**proof["active_hold"], "actor": "someone-else"}}
    dirty_code = {**proof, "provenance": {**proof["provenance"], "code_modified": True}}
    other_commit = {**proof, "provenance": {**proof["provenance"], "commit": "1" * 40}}
    wrong_effect = {**proof, "production_effect": "live"}
    for tampered in (bad_bytes, bad_record, dirty_code, other_commit, wrong_effect):
        with pytest.raises(CaptureHoldError):
            _verify(tampered, root, commit, hold.hold_id)


def test_retained_proof_rejects_wrong_binding(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    proof = recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    with pytest.raises(CaptureHoldError):
        _verify(proof, root, commit, "hold-" + "f" * 20)
    with pytest.raises(CaptureHoldError):
        _verify(proof, root, "2" * 40, hold.hold_id)
    with pytest.raises(CaptureHoldError) as outside:
        _verify(proof, root, commit, hold.hold_id, required_paths=("outputs/other",))
    assert _code(outside) == "CAPTURE_HOLD_SCOPE_INVALID"
    late = hold.expires_at + timedelta(seconds=1)
    forged = {**proof, "checked_at": late.isoformat()}
    with pytest.raises(CaptureHoldError):
        _verify(forged, root, commit, hold.hold_id, checked_at=late)


def test_retained_proof_survives_release_and_expiry(repo: tuple[Path, str]) -> None:
    root, commit = repo
    hold = _acquire(root, commit)
    proof = recheck_capture_hold(hold, candidate_commit=commit, required_paths=PATHS)
    release_capture_hold(hold)
    _verify(proof, root, commit, hold.hold_id)


def _cli() -> Any:
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "scripts" / "capture_hold.py"
    spec = importlib.util.spec_from_file_location("capture_hold_cli", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_operator_cli_acquires_lists_and_releases(
    repo: tuple[Path, str], capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    root, commit = repo
    cli = _cli()
    base = ["--root", str(root)]
    acquire = ["acquire", "--candidate-commit", commit, "--path", PATHS[0], "--actor", "t"]
    assert cli.main([*base, *acquire, "--ttl-minutes", "30"]) == 0
    acquired = json.loads(capsys.readouterr().out)
    assert acquired["status"] == "ACQUIRED" and acquired["required_paths"] == list(PATHS)
    assert cli.main([*base, "status"]) == 0
    rows = json.loads(capsys.readouterr().out)["holds"]
    assert [(row["hold_id"], row["released"]) for row in rows] == [(acquired["hold_id"], False)]
    assert cli.main([*base, "release", "--hold-id", acquired["hold_id"]]) == 0
    capsys.readouterr()
    assert cli.main([*base, "status"]) == 0
    assert json.loads(capsys.readouterr().out)["holds"][0]["released"] is True


def test_operator_cli_reports_blocked_without_a_traceback(
    repo: tuple[Path, str], capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    root, _ = repo
    arguments = ["--root", str(root), "acquire", "--candidate-commit", "0" * 40]
    arguments += ["--path", PATHS[0], "--actor", "t", "--ttl-minutes", "30"]
    assert _cli().main(arguments) == 2
    blocked = json.loads(capsys.readouterr().out)
    assert blocked["status"] == "BLOCKED" and "CAPTURE_HOLD_COMMIT_DRIFT" in blocked["reason"]
