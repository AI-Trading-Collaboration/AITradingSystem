from __future__ import annotations

import hashlib
import json
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ai_trading_system.platform.architecture import validation_readiness as subject
from scripts import validation_readiness as cli

CANDIDATE = "a" * 40


@pytest.fixture(autouse=True)
def _synthetic_inspection_code_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Synthetic roots substitute code identity, never the production guard."""
    monkeypatch.setattr(subject, "_inspection_code_root", lambda: tmp_path.resolve())


def _write_binding(root: Path, relative: str, raw: bytes = b"retained") -> dict[str, Any]:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    return {
        "path": relative,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def _checkers(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> None:
    # Aggregate fixtures have no Git checkout. Identity is tested separately
    # with explicit synthetic Git responses; production never skips this gate.
    monkeypatch.setattr(subject, "_inspection_code_identity", lambda root, candidate: None)
    checks = {name: (lambda root, candidate: {"status": "PASS"}) for name in subject.CHECKER_IDS}
    checks.update(overrides)
    monkeypatch.setattr(subject, "_checkers", lambda: checks)


def _codes(result: dict[str, Any]) -> set[str]:
    return {row["code"] for row in result["blockers"]}


def test_aggregate_pass_is_read_only_and_does_not_authorize_research(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _checkers(monkeypatch)
    before = list(tmp_path.iterdir())
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "PASS"
    assert result["full_dispatch_ready"] is True
    assert result["candidate_sha"] == CANDIDATE
    assert result["elapsed_seconds"] >= 0
    assert all(row["elapsed_seconds"] >= 0 for row in result["checks"])
    assert [row["checker_id"] for row in result["checks"]] == list(subject.CHECKER_IDS)
    for key in (
        "dispatch_performed",
        "research_dispatch_allowed",
        "dq_validation_executed",
        "artifacts_written",
    ):
        assert result[key] is False
    assert list(tmp_path.iterdir()) == before


def test_aggregate_collects_all_failures_without_dispatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    visited = []

    def blocked(root: Path, candidate: str) -> dict[str, Any]:
        visited.append(root)
        raise subject.ValidationReadinessError("SYNTHETIC_MISSING", "fixture")

    _checkers(monkeypatch, canonical_tasks=blocked, atlas_final_binding=blocked)
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert result["full_dispatch_ready"] is False
    assert len(visited) == len(result["blockers"]) == 2
    assert len(result["checks"]) == len(subject.CHECKER_IDS)
    assert _codes(result) == {"SYNTHETIC_MISSING"}


def test_unknown_checker_or_bad_checker_result_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _checkers(monkeypatch, surprise=lambda root, candidate: {"status": "PASS"})
    assert "READINESS_CHECKER_INVENTORY_INVALID" in _codes(
        subject.check_full_readiness(tmp_path, CANDIDATE)
    )
    _checkers(monkeypatch, canonical_tasks=lambda root, candidate: {"status": "FAIL"})
    assert "READINESS_CHECK_RESULT_INVALID" in _codes(
        subject.check_full_readiness(tmp_path, CANDIDATE)
    )
    _checkers(monkeypatch, canonical_tasks=lambda root, candidate: {})
    assert "READINESS_CHECK_RESULT_INVALID" in _codes(
        subject.check_full_readiness(tmp_path, CANDIDATE)
    )


@pytest.mark.parametrize(
    "relative",
    [
        "../outside",
        "/outside",
        "C:/outside",
        "outputs/../escape",
        "outputs\\x",
        "data/raw/x",
        "docs/research/private.md",
    ],
)
def test_paths_reject_escape_and_out_of_scope_without_reading(
    tmp_path: Path, relative: str
) -> None:
    with pytest.raises(subject.ValidationReadinessError, match="READINESS_PATH"):
        subject._regular_file(tmp_path, relative)


def test_bindings_detect_missing_tamper_and_duplicate_without_mutation(tmp_path: Path) -> None:
    first = _write_binding(tmp_path, "outputs/evidence/a.json")
    second = {**first, "path": "outputs/evidence/missing.json"}
    evidence = subject._EvidenceCheck(tmp_path)
    evidence.bindings([first, first, second])
    assert {item["code"] for item in evidence.blockers} == {
        "READINESS_DUPLICATE_DEPENDENCY",
        "READINESS_DEPENDENCY_MISSING",
    }
    path = tmp_path / first["path"]
    path.write_bytes(b"tampered")
    with pytest.raises(subject.ValidationReadinessError, match="HASH_MISMATCH"):
        subject._binding_bytes(tmp_path, first)
    assert path.read_bytes() == b"tampered"


def test_reparse_ancestor_rejected_before_content_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    binding = _write_binding(tmp_path, "outputs/evidence/a.json")
    original = Path.lstat
    intercepted = tmp_path / "outputs"

    def lstat(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == intercepted:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR, st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT
            )
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(subject.ValidationReadinessError, match="REPARSE_PATH"):
        subject._binding_bytes(tmp_path, binding)


def test_reparse_repository_root_stops_before_any_checker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original = Path.lstat

    def lstat(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == tmp_path:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR, st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT
            )
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(subject, "_checkers", lambda: pytest.fail("checker must not start"))
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert _codes(result) == {"READINESS_REPARSE_PATH"}
    assert result["checks"] == []


def test_foreign_inspection_code_root_stops_before_any_checker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    foreign = tmp_path / "foreign"
    monkeypatch.setattr(subject, "_inspection_code_root", lambda: foreign)
    monkeypatch.setattr(subject, "_checkers", lambda: pytest.fail("checker must not start"))
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert result["full_dispatch_ready"] is False
    assert _codes(result) == {"READINESS_INSPECTION_ROOT_MISMATCH"}
    assert result["inspection_code_root"] == foreign.as_posix()
    assert result["target_root"] == tmp_path.as_posix()
    assert result["checks"] == []


def test_size_and_json_contracts_fail_closed(tmp_path: Path) -> None:
    row = _write_binding(tmp_path, "outputs/fixture.json")
    with pytest.raises(subject.ValidationReadinessError, match="SIZE_MISMATCH"):
        subject._binding_bytes(tmp_path, {**row, "size_bytes": True})
    with pytest.raises(subject.ValidationReadinessError, match="DIGEST_INVALID"):
        subject._binding_bytes(tmp_path, {**row, "sha256": "bad"})
    with pytest.raises(subject.ValidationReadinessError, match="DUPLICATE_KEY"):
        subject._json_mapping(b'{"a":1,"a":2}', "fixture")
    with pytest.raises(subject.ValidationReadinessError, match="NONFINITE"):
        subject._json_mapping(b'{"a":NaN}', "fixture")


def test_candidate_and_committed_source_require_exact_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_binding(tmp_path, "config/research/fixture.yaml", b"evidence_bindings: []\n")
    commands: list[tuple[str, ...]] = []

    def git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, stdout=CANDIDATE, stderr="")

    monkeypatch.setattr(subject, "_git", git)
    assert subject._candidate_identity(tmp_path, CANDIDATE)["candidate_sha"] == CANDIDATE
    subject._committed_yaml(tmp_path, CANDIDATE, "config/research/fixture.yaml")
    assert commands[-1] == (
        "diff",
        "--quiet",
        "--no-ext-diff",
        "--no-textconv",
        CANDIDATE,
        "--",
        "config/research/fixture.yaml",
    )
    monkeypatch.setattr(
        subject, "_git", lambda *args: subprocess.CompletedProcess([], 1, stdout="")
    )
    with pytest.raises(subject.ValidationReadinessError, match="AUTHORITY_NOT_COMMITTED"):
        subject._committed_yaml(tmp_path, CANDIDATE, "config/research/fixture.yaml")
    with pytest.raises(subject.ValidationReadinessError, match="CANDIDATE_MISMATCH"):
        subject._candidate_identity(tmp_path, CANDIDATE)


@pytest.mark.parametrize(
    "candidate",
    [
        "",
        "a" * 39,
        "a" * 41,
        "A" * 40,
        "g" * 40,
        "--output=synthetic-only",
        "a" * 40 + "\n",
        None,
        0,
    ],
)
def test_invalid_candidate_stops_before_git_or_any_checker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, candidate: Any
) -> None:
    monkeypatch.setattr(subject, "_git", lambda *args: pytest.fail("Git must not start"))
    monkeypatch.setattr(subject, "_checkers", lambda: pytest.fail("checker must not start"))
    monkeypatch.setattr(
        subject, "_inspection_code_identity", lambda *args: pytest.fail("identity must not start")
    )
    result = subject.check_full_readiness(tmp_path, candidate)
    assert result["status"] == "BLOCKED"
    assert result["full_dispatch_ready"] is False
    assert _codes(result) == {"READINESS_CANDIDATE_INVALID"}
    assert result["checks"] == []
    assert list(tmp_path.iterdir()) == []


def test_committed_yaml_rejects_invalid_candidate_before_file_or_git_access(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(subject, "_git", lambda *args: pytest.fail("Git must not start"))
    monkeypatch.setattr(subject, "_regular_file", lambda *args: pytest.fail("read must not start"))
    with pytest.raises(subject.ValidationReadinessError, match="READINESS_CANDIDATE_INVALID"):
        subject._committed_yaml(tmp_path, "--output=synthetic-only", "config/research/fixture.yaml")


def test_inspection_code_identity_uses_only_fixed_literal_read_only_git_checks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[tuple[str, ...]] = []

    def git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        return subprocess.CompletedProcess(
            args, 0, stdout=CANDIDATE if args == ("rev-parse", "HEAD") else "", stderr=""
        )

    monkeypatch.setattr(subject, "_git", git)
    subject._inspection_code_identity(tmp_path, CANDIDATE)
    paths = (
        ":(literal)src",
        ":(literal)scripts/run_validation_tier.py",
        ":(literal)scripts/validation_readiness.py",
    )
    assert commands == [
        ("rev-parse", "HEAD"),
        (
            "cat-file",
            "-e",
            f"{CANDIDATE}:src/ai_trading_system/platform/architecture/validation_readiness.py",
        ),
        ("cat-file", "-e", f"{CANDIDATE}:scripts/run_validation_tier.py"),
        ("cat-file", "-e", f"{CANDIDATE}:scripts/validation_readiness.py"),
        ("diff", "--quiet", "--no-ext-diff", "--no-textconv", CANDIDATE, "--", *paths),
        ("diff", "--cached", "--quiet", "--no-ext-diff", "--no-textconv", CANDIDATE, "--", *paths),
        ("ls-files", "--others", "--exclude-standard", "-z", "--", *paths),
    ]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "failure,expected_code",
    [
        ("head", "READINESS_CANDIDATE_MISMATCH"),
        ("own-module", "READINESS_INSPECTION_CODE_NOT_COMMITTED"),
        ("runner", "READINESS_INSPECTION_CODE_NOT_COMMITTED"),
        ("script", "READINESS_INSPECTION_CODE_NOT_COMMITTED"),
        ("working-tree", "READINESS_INSPECTION_CODE_DIRTY"),
        ("index", "READINESS_INSPECTION_CODE_DIRTY"),
        ("untracked", "READINESS_INSPECTION_CODE_UNTRACKED"),
        ("diff-error", "READINESS_INSPECTION_CODE_CHECK_FAILED"),
        ("index-error", "READINESS_INSPECTION_CODE_CHECK_FAILED"),
        ("inventory-error", "READINESS_INSPECTION_CODE_CHECK_FAILED"),
    ],
)
def test_uncommitted_inspection_code_stops_before_canonical_checkers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str, expected_code: str
) -> None:
    commands: list[tuple[str, ...]] = []
    missing_files = {
        "own-module": "src/ai_trading_system/platform/architecture/validation_readiness.py",
        "runner": "scripts/run_validation_tier.py",
        "script": "scripts/validation_readiness.py",
    }

    def git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        code = 0
        stdout = ""
        if args == ("rev-parse", "HEAD"):
            stdout = "b" * 40 if failure == "head" else CANDIDATE
        elif args[0] == "cat-file" and args[-1] == f"{CANDIDATE}:{missing_files.get(failure)}":
            code = 1
        elif args[0] == "diff":
            staged = "--cached" in args
            if failure == ("index" if staged else "working-tree"):
                code = 1
            if failure == ("index-error" if staged else "diff-error"):
                code = 128
        elif args[0] == "ls-files":
            if failure == "untracked":
                stdout = "src/synthetic_extra_validator.py\0"
            if failure == "inventory-error":
                code = 128
        return subprocess.CompletedProcess(args, code, stdout=stdout, stderr="")

    monkeypatch.setattr(subject, "_git", git)
    monkeypatch.setattr(subject, "_checkers", lambda: pytest.fail("checker must not start"))
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert result["full_dispatch_ready"] is False
    assert _codes(result) == {expected_code}
    assert result["checks"] == []
    if failure == "index":
        # Candidate/worktree equality alone would miss an index change that
        # was canceled in the working tree; the separate staged check blocks.
        assert [command[0] for command in commands].count("diff") == 2
        assert commands[-1][:2] == ("diff", "--cached")
    assert list(tmp_path.iterdir()) == []


def test_missing_registered_identity_cannot_skip_source_identity_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        subject,
        "_checkers",
        lambda: {
            name: (lambda *args: pytest.fail("canonical checker must not start"))
            for name in subject.CHECKER_IDS
            if name != "candidate_identity"
        },
    )
    monkeypatch.setattr(
        subject,
        "_git",
        lambda root, *args: subprocess.CompletedProcess(args, 0, stdout="b" * 40, stderr=""),
    )
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert _codes(result) == {"READINESS_CANDIDATE_MISMATCH"}
    assert result["checks"] == []


@pytest.mark.parametrize(
    "failure", [OSError("synthetic git failure"), subprocess.SubprocessError()]
)
def test_source_identity_git_exception_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: Exception
) -> None:
    def git(*args: Any) -> subprocess.CompletedProcess[str]:
        raise failure

    monkeypatch.setattr(subject, "_git", git)
    monkeypatch.setattr(subject, "_checkers", lambda: pytest.fail("checker must not start"))
    result = subject.check_full_readiness(tmp_path, CANDIDATE)
    assert result["status"] == "BLOCKED"
    assert _codes(result) == {"READINESS_CHECK_FAILED"}
    assert result["checks"] == []


def test_generated_adapters_preserve_read_only_validator_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from ai_trading_system.platform.architecture import (
        compatibility_authority,
        devex,
        report_catalog_flow_authority,
        task_registry_canonical,
    )

    monkeypatch.setattr(
        task_registry_canonical,
        "validate_canonical_registry",
        lambda **kwargs: SimpleNamespace(index={"status": "FAIL", "task_count": 1}),
    )
    monkeypatch.setattr(
        devex,
        "build_architecture_fitness",
        lambda **kwargs: {"status": "FAIL", "violations": [{"rule_id": "module_manifest_fresh"}]},
    )
    monkeypatch.setattr(
        report_catalog_flow_authority,
        "validate_repository_authority",
        lambda root: {"status": "FAIL"},
    )
    monkeypatch.setattr(
        compatibility_authority, "validate_repository_authority", lambda root: {"status": "FAIL"}
    )
    adapters = (
        (subject._canonical_tasks, "TASK_REGISTRY_INVALID"),
        (subject._architecture_generated, "ARCHITECTURE_STALE"),
        (subject._report_flow, "REPORT_FLOW_INVALID"),
        (subject._compatibility, "COMPATIBILITY_INVALID"),
    )
    for adapter, expected in adapters:
        with pytest.raises(subject.ValidationReadinessError, match=expected):
            adapter(tmp_path, CANDIDATE)
    assert list(tmp_path.iterdir()) == []


def test_retained_inventory_follows_only_bound_receipt_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    row = _write_binding(tmp_path, "outputs/research/retained.json")
    admissions = {
        path: {"evidence_bindings": [{"path": row["path"], "file_sha256": row["sha256"]}]}
        for path in subject.RESULT_ADMISSIONS
    }
    admissions[subject.O1_POLICY] = {"isolated_dq_evidence": {"gate": row}}
    package_root = "outputs/qqq_options/synthetic"
    daily = _write_binding(tmp_path, f"{package_root}/daily_signals/one.json")
    receipt = {
        "daily_signal_artifacts": [
            {
                "relative_path": "daily_signals/one.json",
                "sha256": daily["sha256"],
                "byte_count": daily["size_bytes"],
            }
        ],
        "source_artifact": {"locator": row["path"], "sha256": row["sha256"]},
    }
    package = {"root": package_root}
    for name in ("package_receipt", "signal_index", "run_manifest"):
        payload = receipt if name == "package_receipt" else {}
        binding = _write_binding(
            tmp_path, f"{package_root}/{name}.json", json.dumps(payload).encode()
        )
        package[f"{name}_sha256"] = binding["sha256"]
    admissions[subject.SIGNAL_POLICY] = {
        "authority_bindings": [{"path": row["path"], "file_sha256": row["sha256"]}],
        "signal_package": package,
    }
    monkeypatch.setattr(subject, "_committed_yaml", lambda root, candidate, path: admissions[path])
    result = subject._retained_evidence(tmp_path, CANDIDATE)
    assert result["blockers"] == []
    assert result["verified_dependency_count"] == 5
    (tmp_path / daily["path"]).unlink()
    blocked = subject._retained_evidence(tmp_path, CANDIDATE)
    assert [item["code"] for item in blocked["blockers"]] == ["READINESS_DEPENDENCY_MISSING"]


@pytest.mark.parametrize("fresh", [True, False])
def test_atlas_requires_live_validation_and_exact_final_commit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fresh: bool
) -> None:
    from ai_trading_system.atlas import page_effectiveness as atlas
    from ai_trading_system.contracts.strategy_research_page_effectiveness import (
        PageFreshnessStatus,
        StrategyResearchPageEffectivenessManifest,
    )

    _write_binding(tmp_path, "outputs/atlas/page_effectiveness.json", b"{}")
    manifest = SimpleNamespace(
        repository_commit=CANDIDATE if fresh else "b" * 40,
        source_snapshot_commit=CANDIDATE if fresh else "b" * 40,
        freshness_status=PageFreshnessStatus.CURRENT,
        source_artifacts=(),
        rendered_artifacts=(),
        content_sha256="c" * 64,
    )
    monkeypatch.setattr(
        atlas,
        "load_page_effectiveness_policy",
        lambda **kwargs: SimpleNamespace(manifest_path="outputs/atlas/page_effectiveness.json"),
    )
    monkeypatch.setattr(
        StrategyResearchPageEffectivenessManifest, "from_json_bytes", lambda raw: manifest
    )
    monkeypatch.setattr(
        atlas,
        "validate_page_effectiveness_manifest",
        lambda **kwargs: SimpleNamespace(
            status="PASS", errors=(), freshness_status=PageFreshnessStatus.CURRENT
        ),
    )
    if fresh:
        assert subject._atlas_final_binding(tmp_path, CANDIDATE)["freshness_status"] == "CURRENT"
    else:
        with pytest.raises(subject.ValidationReadinessError, match="ATLAS_CANDIDATE_MISMATCH"):
            subject._atlas_final_binding(tmp_path, CANDIDATE)
    (tmp_path / "outputs/atlas/page_effectiveness.json").unlink()
    with pytest.raises(subject.ValidationReadinessError, match="DEPENDENCY_MISSING"):
        subject._atlas_final_binding(tmp_path, CANDIDATE)


@pytest.mark.parametrize("status,expected_exit", [("PASS", 0), ("BLOCKED", 2)])
def test_cli_prints_only_and_uses_supplied_repository(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    status: str,
    expected_exit: int,
) -> None:
    observed = []

    def check(root: Path, candidate: str) -> dict[str, Any]:
        observed.append((root, candidate))
        return {"status": status}

    monkeypatch.setattr(cli, "check_full_readiness", check)
    assert (
        cli.main(["--repository-root", str(tmp_path), "--candidate-sha", CANDIDATE])
        == expected_exit
    )
    assert json.loads(capsys.readouterr().out) == {"status": status}
    assert observed == [(tmp_path, CANDIDATE)]
    assert list(tmp_path.iterdir()) == []
