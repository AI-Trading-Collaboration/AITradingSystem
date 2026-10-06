"""DEVX-021 section 9: the publication validators accept every ORIG_HEAD baseline git can produce.

Git (2.45.1.windows.1, reproduced in an isolated repository) writes ``ORIG_HEAD.lock`` at the
``reference-transaction prepared`` stage as follows:

* ORIG_HEAD absent or different from the old main: 41 bytes, the old main plus LF; the lock becomes
  ORIG_HEAD at commit.
* ORIG_HEAD already equal to the old main: an EMPTY lock; the existing ORIG_HEAD file is kept as is.

v25 was rejected by the real repository because the validator demanded the first form only.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from ai_trading_system.platform.architecture.workflow_integration import (
    publication_orig_head_baseline_is_expected_main,
    publication_orig_head_final_records,
    publication_orig_head_prepared_is_legal,
)

MAIN = "a" * 40
OTHER = "c" * 40
GIT_DIR = "D:/repo/.git"
ORIG_HEAD_PATH = GIT_DIR + "/ORIG_HEAD"
LOCK_PATH = GIT_DIR + "/ORIG_HEAD.lock"
MAIN_LINE = (MAIN + "\n").encode("ascii")


def _record(content: bytes | None, *, path: str, identity: tuple[int, int]) -> dict[str, Any]:
    """Shape of ``_local_publication_metadata(path, contents=True)``."""
    if content is None:
        return {"path": path, "identity": None, "sha256": None, "size": None}
    return {
        "path": path,
        "identity": list(identity),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
        "bytes_hex": content.hex(),
    }


def _plan(baseline: bytes | None) -> dict[str, Any]:
    return {"orig_head": _record(baseline, path=ORIG_HEAD_PATH, identity=(7, 100))}


def _lock(content: bytes | None) -> dict[str, Any]:
    return _record(content, path=LOCK_PATH, identity=(7, 200))


REQUEST = {"expected_main_sha": MAIN, "candidate_sha": "b" * 40}

ABSENT = None
EQUAL = MAIN_LINE
DIFFERENT = (OTHER + "\n").encode("ascii")


@pytest.mark.parametrize(
    "baseline, expected",
    [
        (ABSENT, False),
        (EQUAL, True),
        (DIFFERENT, False),
        (MAIN.encode("ascii"), False),  # no trailing LF: not the form git writes
    ],
)
def test_baseline_is_expected_main_only_for_an_existing_file_holding_main_and_lf(
    baseline: bytes | None, expected: bool
) -> None:
    assert publication_orig_head_baseline_is_expected_main(_plan(baseline), REQUEST) is expected


@pytest.mark.parametrize(
    "baseline, lock, legal",
    [
        # Absent or different baseline: only the main-plus-LF lock git writes is legal.
        (ABSENT, MAIN_LINE, True),
        (ABSENT, b"", False),
        (ABSENT, (OTHER + "\n").encode("ascii"), False),
        (DIFFERENT, MAIN_LINE, True),
        (DIFFERENT, b"", False),
        (DIFFERENT, (OTHER + "\n").encode("ascii"), False),
        # Baseline already equal to main: git leaves an empty lock; the former form stays legal.
        (EQUAL, b"", True),
        (EQUAL, MAIN_LINE, True),
        (EQUAL, (OTHER + "\n").encode("ascii"), False),
        # Tampered or malformed content is never legal, whatever the baseline.
        (EQUAL, MAIN.encode("ascii"), False),
        (EQUAL, MAIN_LINE + b"\n", False),
        (ABSENT, b" " + MAIN_LINE, False),
        (EQUAL, b"\x00", False),
    ],
)
def test_prepared_lock_legality_follows_the_recorded_baseline(
    baseline: bytes | None, lock: bytes, legal: bool
) -> None:
    assert (
        publication_orig_head_prepared_is_legal(_plan(baseline), REQUEST, _lock(lock)) is legal
    )


@pytest.mark.parametrize("baseline", [ABSENT, EQUAL, DIFFERENT])
def test_a_missing_prepared_lock_is_never_legal(baseline: bytes | None) -> None:
    assert (
        publication_orig_head_prepared_is_legal(_plan(baseline), REQUEST, _lock(None)) is False
    )


def test_an_empty_lock_is_not_a_permit_when_the_baseline_was_not_already_main() -> None:
    # The relaxation is bounded by the plan recorded before git ran, not by what the lock says.
    for baseline in (ABSENT, DIFFERENT):
        plan = _plan(baseline)
        assert publication_orig_head_prepared_is_legal(plan, REQUEST, _lock(b"")) is False


def test_final_records_for_an_equal_baseline_are_only_the_original_file() -> None:
    plan = _plan(EQUAL)
    records = publication_orig_head_final_records(plan, REQUEST, _lock(b""))
    assert records == [plan["orig_head"]]
    # The empty lock record is never a legal final ORIG_HEAD: git removes it, it is not renamed.
    assert all(record["size"] != 0 for record in records)


@pytest.mark.parametrize("baseline", [ABSENT, DIFFERENT])
def test_final_records_for_a_normal_lock_are_the_lock_content_at_the_orig_head_path(
    baseline: bytes | None,
) -> None:
    plan = _plan(baseline)
    lock = _lock(MAIN_LINE)
    records = publication_orig_head_final_records(plan, REQUEST, lock)
    assert records == [{**lock, "path": ORIG_HEAD_PATH}]
    # The original file is not a legal final state once git has written the lock.
    assert plan["orig_head"] not in records


def test_final_records_for_an_equal_baseline_with_the_former_lock_form_follow_the_lock() -> None:
    plan = _plan(EQUAL)
    lock = _lock(MAIN_LINE)
    assert publication_orig_head_final_records(plan, REQUEST, lock) == [
        {**lock, "path": ORIG_HEAD_PATH}
    ]


@pytest.mark.parametrize("baseline", [ABSENT, EQUAL, DIFFERENT])
def test_final_records_without_a_prepared_lock_are_empty(baseline: bytes | None) -> None:
    assert publication_orig_head_final_records(_plan(baseline), REQUEST, None) == []
