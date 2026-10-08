"""DEVX-016 P4-1: the generic active-ledger check, its mutation classes and the real repository."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
from compat_ledger_support import (
    STRICT_AFTER,
    Checkout,
    Sections,
    evaluate,
    policy,
    section,
    sha,
    structure_codes,
)

from ai_trading_system.platform.architecture.compat_ledger import (
    DEFAULT_POLICY_PATH,
    CompatLedgerError,
    FileSystemHasher,
    build_active_ledger,
    evaluate_repository,
    ledger_mismatches,
    load_ledger_policy,
    merged_sections,
)
from ai_trading_system.platform.architecture.compatibility_authority import (
    load_compatibility_authority,
)

ROOT = Path(__file__).resolve().parents[1]
RETIRE = "s_retire"


def codes(report: Any) -> set[str]:
    return {violation.code for violation in report.violations}


def base(tmp_path: Path) -> tuple[Checkout, Sections]:
    """Three files pinned by three sections; the last two are after the strict cutoff."""
    checkout = Checkout(tmp_path)
    for name in ("a.py", "b.py", "c.py"):
        checkout.write(name, f"{name} v1\n")
    sections: Sections = [
        ("s_old", section([checkout.record("a.py"), checkout.record("b.py")])),
        (STRICT_AFTER, section([checkout.record("c.py")], production_effect="none")),
    ]
    return checkout, sections


# ------------------------------------------------------------------ live drift (mutation classes)


def test_a_clean_authority_has_no_violations(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    report = evaluate(checkout, sections)
    assert report.ok and report.active_record_count == 3 and report.mismatch_count == 0


def test_m01_a_changed_pinned_file_is_live_drift(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    checkout.write("a.py", "tampered\n")
    report = evaluate(checkout, sections)
    assert codes(report) == {"LEDGER_LIVE_DRIFT"}
    assert [v.subject for v in report.violations] == ["a.py"]


def test_m02_a_deleted_pinned_file_is_missing(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    checkout.delete("b.py")
    assert codes(evaluate(checkout, sections)) == {"LEDGER_LIVE_MISSING"}


def test_m03_a_line_ending_only_change_is_invisible_to_an_lf_normalized_record(
    tmp_path: Path,
) -> None:
    checkout = Checkout(tmp_path)
    checkout.write("x.txt", "one\ntwo\n")
    sections: Sections = [("s0", section([checkout.record("x.txt", lf=True)]))]
    checkout.write("x.txt", "one\r\ntwo\r\n")
    assert evaluate(checkout, sections).ok


def test_m04_a_line_ending_change_is_drift_for_a_raw_hash_record(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    checkout.write("x.txt", "one\ntwo\n")
    sections: Sections = [("s0", section([checkout.record("x.txt")]))]
    checkout.write("x.txt", "one\r\ntwo\r\n")
    assert codes(evaluate(checkout, sections)) == {"LEDGER_LIVE_DRIFT"}


def test_m05_a_later_section_that_relists_the_new_content_supersedes_the_old_record(
    tmp_path: Path,
) -> None:
    checkout, sections = base(tmp_path)
    checkout.write("a.py", "a.py v2\n")
    sections.append(("s_new", section([checkout.record("a.py")])))
    report = evaluate(checkout, sections)
    assert report.ok and report.active_record_count == 3


def test_m06_the_last_record_wins_even_if_an_older_one_still_matches(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    stale = {"path": "a.py", "sha256": sha(b"never was")}
    sections.append(("s_new", section([stale])))  # re-listed with a hash the file never had
    report = evaluate(checkout, sections)
    assert codes(report) == {"LEDGER_LIVE_DRIFT"} and report.violations[0].detail.endswith("s_new")


def test_m07_a_historical_record_never_becomes_the_active_one(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    ghost = {"path": "a.py", "sha256": sha(b"ghost"), "historical_phase_x_hash": True}
    sections.append(("s_new", section([ghost])))
    assert evaluate(checkout, sections).ok  # the earlier, matching record stays active


def test_m08_a_removal_pops_the_record_so_the_path_is_no_longer_pinned(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    sections.append(("s_new", section([], removed_live_source_paths=["a.py"])))
    checkout.write("a.py", "anything now\n")
    report = evaluate(checkout, sections)
    assert report.ok and report.active_record_count == 2
    checkout.delete("a.py")
    assert evaluate(checkout, sections).ok


def test_m09_superseded_source_paths_pop_like_removals(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    sections.append(("s_new", section([], superseded_source_paths=["b.py"])))
    checkout.write("b.py", "different\n")
    assert evaluate(checkout, sections).ok


def test_m10_a_declared_normalization_migration_is_accepted_and_a_tampered_file_is_not(
    tmp_path: Path,
) -> None:
    crlf = b"one\r\ntwo\r\n"
    checkout = Checkout(tmp_path)
    checkout.write("x.txt", crlf)
    old = {"path": "x.txt", "sha256": sha(crlf)}  # captured before LF normalization
    migrated = checkout.record("x.txt", lf=True, previous_worktree_sha256=old["sha256"])
    sections: Sections = [("s0", section([migrated])), ("s1", section([old]))]
    checkout.write("x.txt", b"one\ntwo\n")  # the worktree was normalized to LF afterwards
    assert evaluate(checkout, sections).ok  # the old raw record is satisfied through the migration
    checkout.write("x.txt", b"tampered\n")
    assert codes(evaluate(checkout, sections)) == {"LEDGER_LIVE_DRIFT"}


# ------------------------------------------------------------------ the reviewed policy


def drifted_with_retirement(tmp_path: Path) -> tuple[Checkout, Sections]:
    checkout, sections = base(tmp_path)
    checkout.write("a.py", "changed after the record\n")
    sections.append((RETIRE, section([], superseded_live_source_paths=["a.py"])))
    return checkout, sections


def test_m11_a_path_retired_by_a_listed_section_is_allowed(tmp_path: Path) -> None:
    checkout, sections = drifted_with_retirement(tmp_path)
    report = evaluate(checkout, sections, policy(retiring=[RETIRE]))
    assert report.ok and report.retired == ("a.py",)


def test_m12_the_same_drift_is_a_violation_when_the_retiring_section_is_not_listed(
    tmp_path: Path,
) -> None:
    checkout, sections = drifted_with_retirement(tmp_path)
    assert codes(evaluate(checkout, sections)) == {"LEDGER_LIVE_DRIFT"}


def test_m13_a_retirement_that_relists_the_path_does_not_excuse_drift(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    checkout.write("a.py", "v2\n")
    stale_relist = {"path": "a.py", "sha256": sha(b"not the file")}
    sections.append((RETIRE, section([stale_relist], superseded_live_source_paths=["a.py"])))
    report = evaluate(checkout, sections, policy(retiring=[RETIRE]))
    assert "LEDGER_LIVE_DRIFT" in codes(report)


def test_m14_a_retirement_that_comes_before_the_record_does_not_excuse_drift(
    tmp_path: Path,
) -> None:
    checkout = Checkout(tmp_path)
    checkout.write("a.py", "v1\n")
    sections: Sections = [
        (RETIRE, section([], superseded_live_source_paths=["a.py"])),
        ("s_after", section([checkout.record("a.py")])),
    ]
    checkout.write("a.py", "v2\n")
    report = evaluate(checkout, sections, policy(retiring=[RETIRE]))
    assert "LEDGER_LIVE_DRIFT" in codes(report)


def test_m15_a_volatile_generated_path_may_drift_or_match(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    rules = policy(volatile=["a.py"])
    assert evaluate(checkout, sections, rules).ok
    checkout.write("a.py", "regenerated\n")
    report = evaluate(checkout, sections, rules)
    assert report.ok and report.volatile == ("a.py",)


def test_m16_an_acknowledged_stale_record_is_allowed_while_it_still_drifts(
    tmp_path: Path,
) -> None:
    checkout, sections = base(tmp_path)
    checkout.write("b.py", "changed later\n")
    report = evaluate(checkout, sections, policy(acknowledged=["b.py"]))
    assert report.ok and report.acknowledged == ("b.py",)


def test_m17_an_acknowledgement_that_matches_again_is_obsolete(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    report = evaluate(checkout, sections, policy(acknowledged=["b.py"]))
    assert codes(report) == {"LEDGER_POLICY_ACKNOWLEDGEMENT_OBSOLETE"}


def test_m18_policy_entries_that_refer_to_nothing_are_violations(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    rules = policy(
        retiring=["s_missing", "s_old"], volatile=["ghost.txt"], acknowledged=["ghost2.txt"]
    )
    assert codes(evaluate(checkout, sections, rules)) == {
        "LEDGER_POLICY_UNKNOWN_RETIRING_SECTION",
        "LEDGER_POLICY_STALE_RETIRING_SECTION",
        "LEDGER_POLICY_STALE_VOLATILE",
        "LEDGER_POLICY_STALE_ACKNOWLEDGEMENT",
    }


def test_m19_a_path_the_policy_does_not_name_is_never_excused(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    checkout.write("a.py", "x\n")
    checkout.write("b.py", "y\n")
    report = evaluate(checkout, sections, policy(volatile=["a.py"]))
    assert [v.subject for v in report.violations] == ["b.py"]


# ------------------------------------------------------------------ record shape and structure


@pytest.mark.parametrize(
    "bad",
    [
        {"path": "a.py", "sha256": "xyz"},
        {"path": "a.py", "sha256": "A" * 64},
        {"path": "..\\a.py", "sha256": "a" * 64},
        {"path": "../a.py", "sha256": "a" * 64},
        {"path": "/abs/a.py", "sha256": "a" * 64},
        {"path": "C:/a.py", "sha256": "a" * 64},
        {"path": "a.py", "sha256": "a" * 64, "hash_normalization": "crlf"},
        {"path": "a.py", "sha256": "a" * 64, "previous_worktree_sha256": "short"},
        {"path": 7, "sha256": "a" * 64},
    ],
)
def test_m20_a_malformed_record_is_reported_not_trusted(
    tmp_path: Path, bad: dict[str, Any]
) -> None:
    checkout, sections = base(tmp_path)
    sections.append(("s_new", section([bad])))
    assert "LEDGER_RECORD_MALFORMED" in codes(evaluate(checkout, sections))


def test_m21_rows_that_are_not_hash_records_are_ignored(tmp_path: Path) -> None:
    checkout, sections = base(tmp_path)
    sections.append(("s_new", section([{"note": "just text"}, {"path": "p"}])))
    assert evaluate(checkout, sections).ok


def strict(**fields: Any) -> Sections:
    """A cutoff section followed by one generated section carrying `fields`."""
    return [(STRICT_AFTER, section([])), ("s_gen", fields)]


def test_s01_the_strict_zone_rejects_duplicate_and_unsorted_sources() -> None:
    rows = [{"path": "b", "sha256": "0" * 64}, {"path": "a", "sha256": "0" * 64}]
    assert structure_codes(strict(sources=rows)) == {"LEDGER_SECTION_SOURCES_UNSORTED"}
    assert structure_codes(strict(sources=rows[:1] + rows[:1])) == {
        "LEDGER_SECTION_SOURCES_DUPLICATE"
    }


def test_s02_the_strict_zone_rejects_unsorted_declared_lists() -> None:
    for key in (
        "source_delta_paths",
        "new_source_paths",
        "removed_live_source_paths",
        "superseded_live_source_paths",
    ):
        assert "LEDGER_SECTION_LIST_UNSORTED" in structure_codes(strict(**{key: ["b", "a"]}))


def declared(**overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "source_delta_paths": ["a", "n"],
        "new_source_paths": ["n"],
        "removed_live_source_paths": [],
        "superseded_live_source_paths": ["a"],
        "sources": [{"path": "a", "sha256": "0" * 64}, {"path": "n", "sha256": "1" * 64}],
    }
    fields.update(overrides)
    return fields


def prior(*paths: str) -> Sections:
    rows = [{"path": p, "sha256": "2" * 64} for p in paths]
    return [(STRICT_AFTER, section(rows))]


def test_s03_a_consistent_generated_section_passes() -> None:
    assert structure_codes([*prior("a"), ("s_gen", declared())]) == set()


def test_s04_the_delta_must_equal_superseded_plus_new_minus_removed() -> None:
    assert "LEDGER_SECTION_DELTA_ALGEBRA" in structure_codes(
        [*prior("a"), ("s_gen", declared(source_delta_paths=["a"]))]
    )


def test_s05_the_sources_must_equal_the_delta() -> None:
    rows = [{"path": "a", "sha256": "0" * 64}]
    assert "LEDGER_SECTION_SOURCES_NE_DELTA" in structure_codes(
        [*prior("a"), ("s_gen", declared(sources=rows))]
    )


def test_s06_a_superseded_path_must_be_relisted() -> None:
    fields = declared(superseded_live_source_paths=["a", "z"], source_delta_paths=["a", "n", "z"])
    assert "LEDGER_SECTION_SUPERSEDED_NOT_RELISTED" in structure_codes(
        [*prior("a", "z"), ("s_gen", fields)]
    )


def test_s07_a_listed_source_must_be_superseded_or_new() -> None:
    extra = {"path": "q", "sha256": "3" * 64}
    fields = declared(sources=[*declared()["sources"], extra])
    assert "LEDGER_SECTION_SOURCES_BEYOND_DECLARED" in structure_codes(
        [*prior("a"), ("s_gen", fields)]
    )


def test_s08_a_new_path_must_not_already_be_active() -> None:
    assert "LEDGER_SECTION_NEW_PATH_ACTIVE" in structure_codes(
        [*prior("a", "n"), ("s_gen", declared())]
    )


def test_s09_a_removed_path_must_have_been_active() -> None:
    fields = declared(removed_live_source_paths=["ghost"], source_delta_paths=["a", "n"])
    codes_found = structure_codes([*prior("a"), ("s_gen", fields)])
    assert "LEDGER_SECTION_REMOVED_NOT_ACTIVE" in codes_found


def test_s10_history_before_the_cutoff_is_not_re_litigated() -> None:
    sections: Sections = [
        ("s_hist", declared(source_delta_paths=["z"], new_source_paths=["z", "a"])),
        (STRICT_AFTER, section([])),
    ]
    assert structure_codes(sections) == set()


@pytest.mark.parametrize("zone", ["history", "strict"])
def test_s11_the_safety_flags_are_checked_in_every_zone(zone: str) -> None:
    def build(fields: dict[str, Any]) -> Sections:
        history: Sections = [("s_hist", fields), (STRICT_AFTER, section([]))]
        generated: Sections = [(STRICT_AFTER, section([])), ("s_gen", fields)]
        return history if zone == "history" else generated

    assert structure_codes(build({"production_effect": "live"})) == {"LEDGER_SAFETY_FLAG"}
    assert structure_codes(build({"safety": {"broker_action": "order"}})) == {"LEDGER_SAFETY_FLAG"}
    assert structure_codes(build({"safety": {"production_effect": "none", "x": 1}})) == set()
    assert structure_codes(build({"supersession": {"historical_hashes_rewritten": True}})) == {
        "LEDGER_HISTORY_REWRITTEN"
    }
    assert structure_codes(build({"supersession": {"historical_hashes_rewritten": False}})) == set()


def test_s12_an_unknown_strict_cutoff_is_refused() -> None:
    with pytest.raises(CompatLedgerError) as raised:
        structure_codes([("s0", section([]))], strict_after="nope")
    assert raised.value.code == "COMPAT_LEDGER_STRICT_SECTION_UNKNOWN"


# ------------------------------------------------------------------ the policy file


def test_p01_the_shipped_policy_loads_and_names_the_measured_exceptions() -> None:
    rules = load_ledger_policy(ROOT / DEFAULT_POLICY_PATH)
    assert rules.status == "PROPOSED_PENDING_OWNER_REVIEW" and rules.version == "1.0.0"
    assert [e.path for e in rules.retiring_sections] == [
        "phase_arch_005_s5_canonical_task_source_cutover"
    ]
    volatile = [e.path for e in rules.volatile_generated_paths]
    assert len(volatile) == 2 and all((ROOT / path).is_file() for path in volatile)
    assert "inputs/architecture/arch_005_task_registry_index.yaml" in volatile
    assert len(rules.acknowledged_stale_records) == 6 and len(rules.sha256) == 64


def write_policy(tmp_path: Path, **changes: Any) -> Path:
    text = (ROOT / DEFAULT_POLICY_PATH).read_text(encoding="utf-8")
    import yaml

    payload = yaml.safe_load(text)
    for key, value in changes.items():
        if value is None and key in payload and key != "approval_ref":
            del payload[key]
        else:
            payload[key] = value
    target = tmp_path / "policy.yaml"
    target.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    return target


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"schema_version": "other"}, "COMPAT_LEDGER_POLICY_SCHEMA"),
        ({"status": "DRAFT"}, "COMPAT_LEDGER_POLICY_STATUS"),
        (
            {"status": "OWNER_APPROVED_ENFORCED", "approval_ref": None},
            "COMPAT_LEDGER_POLICY_APPROVAL",
        ),
        ({"rationale": " "}, "COMPAT_LEDGER_POLICY_FIELD"),
        ({"retiring_sections": []}, "COMPAT_LEDGER_POLICY_FIELD"),
        (
            {
                "retiring_sections": [
                    {"section_id": "s", "reason": "r"},
                    {"section_id": "s", "reason": "r"},
                ]
            },
            "COMPAT_LEDGER_POLICY_DUPLICATE",
        ),
        (
            {"volatile_generated_paths": [{"path": "/abs", "reason": "r"}]},
            "COMPAT_LEDGER_POLICY_PATH",
        ),
        (
            {"volatile_generated_paths": [{"path": "a/", "reason": "r"}]},
            "COMPAT_LEDGER_POLICY_PATH",
        ),
        ({"volatile_generated_paths": [{"path": "a", "why": "r"}]}, "COMPAT_LEDGER_POLICY_ENTRY"),
        ({"unknown_field": 1}, "COMPAT_LEDGER_POLICY_FIELDS"),
    ],
)
def test_p02_a_malformed_policy_is_refused(
    tmp_path: Path, changes: dict[str, Any], code: str
) -> None:
    path = write_policy(tmp_path, **changes)
    with pytest.raises(CompatLedgerError) as raised:
        load_ledger_policy(path)
    assert raised.value.code == code


def test_p03_a_path_cannot_be_both_volatile_and_acknowledged(tmp_path: Path) -> None:
    volatile = load_ledger_policy(ROOT / DEFAULT_POLICY_PATH).volatile_generated_paths[0].path
    clash = [{"path": volatile, "recorded_by": "s", "reason": "r"}]
    path = write_policy(tmp_path, acknowledged_stale_records=clash)
    with pytest.raises(CompatLedgerError) as raised:
        load_ledger_policy(path)
    assert raised.value.code == "COMPAT_LEDGER_POLICY_OVERLAP"


def test_p04_the_acknowledged_list_may_be_empty_once_everything_is_adopted(tmp_path: Path) -> None:
    path = write_policy(tmp_path, acknowledged_stale_records=[])
    assert load_ledger_policy(path).acknowledged_stale_records == ()


# ------------------------------------------------------------------ the real repository


@pytest.fixture(scope="module")
def real_sections() -> Any:
    return merged_sections(load_compatibility_authority(ROOT))


def test_r01_the_real_ledger_has_the_measured_shape(real_sections: Any) -> None:
    ledger = build_active_ledger(real_sections)
    assert len(real_sections) >= 325 and not ledger.malformed
    assert len(ledger.records) >= 1_900  # floors, not pins: new waves only add


def test_r02_the_real_repository_has_no_unexplained_drift_and_no_stale_policy_entries() -> None:
    report = evaluate_repository(
        ROOT, merged=load_compatibility_authority(ROOT), policy_path=DEFAULT_POLICY_PATH
    )
    assert report.ok, [(v.code, v.subject, v.detail) for v in report.violations][:20]
    assert report.acknowledged == tuple(sorted(report.acknowledged))
    assert len(report.retired) >= 375  # the task shadow registry the S5 cutover retired


def test_r03_the_live_hasher_agrees_with_a_direct_read_for_one_real_record(
    real_sections: Any,
) -> None:
    ledger = build_active_ledger(real_sections)
    hasher = FileSystemHasher(ROOT)
    mismatched = {m.record.path for m in ledger_mismatches(ledger, hasher)}
    matching = next(p for p in sorted(ledger.records) if p not in mismatched)
    record = ledger.records[matching]
    payload = (ROOT / matching).read_bytes()
    if record.normalization == "git_eol_lf":
        payload = payload.replace(b"\r\n", b"\n")
    assert record.sha256 == sha(payload)


def test_r04_mutating_the_real_authority_in_memory_is_caught(real_sections: Any) -> None:
    """Tamper one real record's hash and one real file's expectation: both become drift."""
    sections = copy.deepcopy(real_sections)
    ledger = build_active_ledger(sections)
    mismatched = {m.record.path for m in ledger_mismatches(ledger, FileSystemHasher(ROOT))}
    victim = next(
        p
        for p, r in sorted(ledger.records.items())
        if p not in mismatched and r.order == len(sections) - 1
    )
    for _, body in sections:
        for row in body.get("sources") or []:
            if isinstance(row, dict) and row.get("path") == victim:
                row["sha256"] = "f" * 64
    after = {
        m.record.path
        for m in ledger_mismatches(build_active_ledger(sections), FileSystemHasher(ROOT))
    }
    assert after - mismatched == {victim}
