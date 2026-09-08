from __future__ import annotations

import builtins
import copy
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from scripts import refresh_partial_duration_profile as refresher
from scripts import run_validation_tier as runner
from scripts.pytest_runtime_profile import (
    DurationProfile,
    build_runtime_profile,
    load_duration_profile,
)

DURATION_MANIFEST_PATH = "inputs/architecture/arch_004g2_full_duration_profile.yaml"
FULL_TEST_MANIFEST_PATH = "inputs/architecture/arch_004e_test_manifest.yaml"


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class CapturedProfileFixture:
    """Synthetic source bytes reusable in a tiny Git source-binding repository."""

    profile: dict[str, Any]
    profile_bytes: bytes
    duration_manifest_bytes: bytes
    full_test_manifest_bytes: bytes
    configured_manifest_path: str
    validation_provenance: dict[str, Any]
    test_files: tuple[str, ...]

    def validation_arguments(self) -> dict[str, Any]:
        return {
            "duration_manifest": runner.CapturedDurationManifest(
                content=self.duration_manifest_bytes,
                normalized_locator=self.configured_manifest_path,
            ),
            "full_test_manifest_bytes": self.full_test_manifest_bytes,
            "expected_validation_provenance": copy.deepcopy(self.validation_provenance),
            "pytest_exitstatus": 0,
            "expected_worker_count": 16,
            "expected_dist": "loadfile",
            "formal_selection_eligible": True,
        }


def build_captured_profile_fixture(
    *,
    configured_manifest_path: str = "X:/retired/source/" + DURATION_MANIFEST_PATH,
) -> CapturedProfileFixture:
    """Build complete, finite telemetry without reading any working-tree manifest.

    Sixteen workers each execute two nodes in one file. Equal-weight first files
    and their node names deliberately have non-lexical order; the final file is
    absent from the original seed, so complete telemetry is not confused with a
    COMPLETE duration policy. Every timestamp and duration is synthetic.
    """
    test_files = (
        "tests/test_zeta.py",
        "tests/test_alpha.py",
        *(f"tests/test_{index:02d}.py" for index in range(2, 16)),
    )
    observed_seconds = {
        path: float(16 - max(0, index - 1)) for index, path in enumerate(test_files[:-1])
    }
    manifest: dict[str, Any] = {
        "schema_version": "arch_004g2_full_duration_profile.v1",
        "profile_id": "synthetic_original_partial_seed",
        "status": "PARTIAL_SEED",
        "owner": "validation_operations",
        "version": 1,
        "source": {
            "artifact_path": "outputs/validation_runtime/synthetic_prior/test_runtime_summary.json",
            "artifact_sha256": "1" * 64,
            "tier": "full",
            "workers": 16,
            "dist": "loadfile",
        },
        "partial_seed": {
            "enabled": True,
            "source_duration_row_count": len(observed_seconds),
            "aggregated_file_count": len(observed_seconds),
        },
        "review": {
            "stable_improvement_claimed": False,
            "conditions": ["Synthetic original seed; no measured speedup claim."],
        },
        "files": [
            {"path": path, "observed_seconds": seconds}
            for path, seconds in observed_seconds.items()
        ],
    }
    duration_manifest_bytes = _json_bytes(manifest)
    duration_profile = DurationProfile(
        configured_path=configured_manifest_path,
        manifest_sha256=hashlib.sha256(duration_manifest_bytes).hexdigest(),
        schema_version=manifest["schema_version"],
        profile_id=manifest["profile_id"],
        owner=manifest["owner"],
        version=manifest["version"],
        manifest_status="PARTIAL_SEED",
        partial_seed=True,
        complete_profile=False,
        source_tier="full",
        source_artifact_path=manifest["source"]["artifact_path"],
        source_artifact_sha256=manifest["source"]["artifact_sha256"],
        source_workers=16,
        source_dist="loadfile",
        observed_seconds=observed_seconds,
        file_node_counts={},
        source_node_count=None,
        source_file_count=None,
        source_collection_ordered_sha256=None,
        source_collection_set_sha256=None,
        source_file_set_sha256=None,
        source_file_rows_sha256=None,
        expected_scheduled_ordered_sha256=None,
        source_file_duration_total_seconds=None,
        valid=True,
        fallback_reason=None,
    )
    provenance = {
        "schema_version": "validation_trigger_provenance.v1",
        "status": "PASS",
        "required_for_tier": True,
        "trigger_reason": "formal_performance_profile",
        "task_id": "TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1",
        "boundary_id": "synthetic-duration-refresh",
        "parent_run": None,
        "envelope_source": "environment",
        "field_sources": {
            "trigger_reason": "environment",
            "task_id": "environment",
            "boundary_id": "environment",
            "parent_run": "unset",
        },
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": [],
    }
    nodeids = [f"{path}::{name}" for path in test_files for name in ("test_zeta", "test_alpha")]
    phase_reports = []
    for file_index, path in enumerate(test_files):
        # Dyadic values retain exact equality in both phase and file totals.
        phase_seconds = 1.0 if file_index < 2 else (file_index + 1) / 8.0
        for node_index, name in enumerate(("test_zeta", "test_alpha")):
            node_start = 100.0 + node_index * 8.0
            for phase_index, phase in enumerate(("setup", "call", "teardown")):
                start = node_start + phase_index * phase_seconds
                stop = start + phase_seconds
                phase_reports.append(
                    {
                        "nodeid": f"{path}::{name}",
                        "phase": phase,
                        "worker_id": f"gw{file_index}",
                        "start": start,
                        "stop": stop,
                        "duration": stop - start,
                        "outcome": "passed",
                    }
                )
    profile = build_runtime_profile(
        collections={f"gw{index}": nodeids for index in range(16)},
        phase_reports=phase_reports,
        duration_profile=duration_profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=90.0,
        ended_at=120.0,
        validation_provenance=provenance,
    )
    return CapturedProfileFixture(
        profile=profile,
        profile_bytes=_json_bytes(profile),
        duration_manifest_bytes=duration_manifest_bytes,
        full_test_manifest_bytes=_json_bytes(
            {
                "status": "PASS",
                "test_count": len(test_files),
                "tests": [{"path": path, "file_role": "test"} for path in test_files],
            }
        ),
        configured_manifest_path=configured_manifest_path,
        validation_provenance=provenance,
        test_files=test_files,
    )


@pytest.fixture
def captured_profile() -> CapturedProfileFixture:
    return build_captured_profile_fixture()


def _assert_rejected(payload: dict[str, Any], reason: str) -> None:
    assert payload["profile_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert reason in " ".join(payload["warnings"])


def test_captured_profile_validates_original_bytes_and_complete_telemetry(
    captured_profile: CapturedProfileFixture,
) -> None:
    result = runner.validate_captured_runtime_profile(
        captured_profile.profile_bytes, **captured_profile.validation_arguments()
    )

    assert result == captured_profile.profile
    for field in (
        "profile_status",
        "telemetry_status",
        "performance_evidence_status",
        "validation_provenance_binding_status",
    ):
        assert result[field] == "PASS"
    assert len(result["nodes"]) == 32
    assert len(result["workers"]) == 16
    assert [row["path"] for row in result["files"]] == list(captured_profile.test_files)
    assert result["scheduler"]["manifest_status"] == "PARTIAL_SEED"
    assert result["scheduler"]["complete_profile"] is False
    assert result["scheduler"]["matched_tracked_file_count"] == 15
    assert result["stable_full_improvement_claimed"] is False


def test_captured_profile_never_reopens_or_resolves_historical_paths(
    captured_profile: CapturedProfileFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments = captured_profile.validation_arguments()

    def forbidden(*args: object, **kwargs: object) -> Any:
        raise AssertionError("Historical validation attempted filesystem access")

    with monkeypatch.context() as guard:
        for name in ("read_bytes", "read_text", "open", "stat", "resolve"):
            guard.setattr(Path, name, forbidden)
        guard.setattr(builtins, "open", forbidden)
        guard.setattr(os, "open", forbidden)
        guard.setattr(os, "stat", forbidden)
        result = runner.validate_captured_runtime_profile(
            captured_profile.profile_bytes, **arguments
        )
    assert result == captured_profile.profile


def test_live_relative_manifest_locator_is_preserved_and_captured_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = build_captured_profile_fixture(configured_manifest_path=DURATION_MANIFEST_PATH)
    manifest_path = tmp_path / DURATION_MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(fixture.duration_manifest_bytes)
    monkeypatch.chdir(tmp_path)
    reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def counted_read_bytes(path: Path) -> bytes:
        reads.append(path)
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)
    result = runner._read_runtime_profile_payload(
        tmp_path / "unopened-profile.json",
        raw_bytes=fixture.profile_bytes,
        pytest_exitstatus=0,
        expected_worker_count=16,
        expected_dist="loadfile",
        formal_selection_eligible=True,
        duration_profile_path=Path(DURATION_MANIFEST_PATH),
        expected_test_files=set(fixture.test_files),
        expected_validation_provenance=fixture.validation_provenance,
    )

    assert result == fixture.profile
    assert result["scheduler"]["configured_manifest_path"] == DURATION_MANIFEST_PATH
    assert fixture.profile_bytes == _json_bytes(result)
    assert reads == [manifest_path.resolve()]


@pytest.mark.parametrize(
    "argument",
    (
        "duration_manifest",
        "full_test_manifest_bytes",
        "expected_validation_provenance",
        "pytest_exitstatus",
        "expected_worker_count",
        "expected_dist",
        "formal_selection_eligible",
    ),
)
def test_historical_expected_arguments_cannot_be_omitted(
    captured_profile: CapturedProfileFixture, argument: str
) -> None:
    arguments = captured_profile.validation_arguments()
    del arguments[argument]
    with pytest.raises(TypeError, match=argument):
        runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("duration_manifest", None),
        ("duration_manifest", {}),
        ("full_test_manifest_bytes", None),
        ("full_test_manifest_bytes", b""),
        ("full_test_manifest_bytes", "{}"),
        ("expected_validation_provenance", None),
        ("expected_validation_provenance", {}),
        ("expected_validation_provenance", []),
        ("pytest_exitstatus", None),
        ("pytest_exitstatus", True),
        ("pytest_exitstatus", False),
        ("pytest_exitstatus", 0.0),
        ("pytest_exitstatus", -1),
        ("expected_worker_count", None),
        ("expected_worker_count", True),
        ("expected_worker_count", 16.0),
        ("expected_worker_count", 0),
        ("expected_dist", None),
        ("expected_dist", 16),
        ("expected_dist", " "),
        ("formal_selection_eligible", None),
        ("formal_selection_eligible", 1),
        ("formal_selection_eligible", "true"),
    ],
)
def test_historical_expected_arguments_reject_none_or_wrong_types(
    captured_profile: CapturedProfileFixture, argument: str, value: object
) -> None:
    arguments = captured_profile.validation_arguments()
    arguments[argument] = value
    with pytest.raises(ValueError):
        runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)


@pytest.mark.parametrize("raw_bytes", (None, "{}", b"", bytearray(b"{}")))
def test_historical_profile_requires_nonempty_immutable_bytes(
    captured_profile: CapturedProfileFixture, raw_bytes: object
) -> None:
    with pytest.raises(ValueError, match="historical runtime profile bytes are required"):
        runner.validate_captured_runtime_profile(
            raw_bytes, **captured_profile.validation_arguments()
        )


@pytest.mark.parametrize(
    "locator",
    (
        "\\retired\\source\\manifest.yaml",
        "X:relative/manifest.yaml",
        "relative/manifest.yaml",
        "X:/retired/../manifest.yaml",
        "X:/retired/./manifest.yaml",
    ),
)
def test_historical_capture_rejects_ambiguous_locators(
    captured_profile: CapturedProfileFixture, locator: str
) -> None:
    with pytest.raises(ValueError):
        runner.CapturedDurationManifest(captured_profile.duration_manifest_bytes, locator)


@pytest.mark.parametrize(
    ("injected", "reason"),
    [
        (b'"profile_status":"PASS",', "duplicate JSON key"),
        (b'"untrusted_extra":NaN,', "non-finite JSON constant"),
        (b'"untrusted_extra":Infinity,', "non-finite JSON constant"),
        (b'"untrusted_extra":-Infinity,', "non-finite JSON constant"),
    ],
)
def test_captured_json_rejects_duplicate_keys_and_nonfinite_values(
    captured_profile: CapturedProfileFixture, injected: bytes, reason: str
) -> None:
    raw_bytes = b"{" + injected + captured_profile.profile_bytes[1:]
    result = runner.validate_captured_runtime_profile(
        raw_bytes, **captured_profile.validation_arguments()
    )
    _assert_rejected(result, reason)


@pytest.mark.parametrize("exitstatus", (False, True))
def test_captured_json_rejects_boolean_pytest_exitstatus(
    captured_profile: CapturedProfileFixture, exitstatus: bool
) -> None:
    profile = copy.deepcopy(captured_profile.profile)
    profile["pytest_exitstatus"] = exitstatus
    result = runner.validate_captured_runtime_profile(
        _json_bytes(profile), **captured_profile.validation_arguments()
    )
    _assert_rejected(result, "pytest_exitstatus is invalid or mismatched")


@pytest.mark.parametrize(
    ("section", "index", "field", "value", "reason"),
    [
        ("files", 0, "duration_seconds", 999.0, "file aggregate is not node-derived"),
        ("files", 0, "node_count", 3, "file aggregate is not node-derived"),
        ("workers", 0, "busy_seconds", 999.0, "worker aggregate is not node-derived"),
        ("nodes", 0, "duration_seconds", 999.0, "node timing is not phase-derived"),
    ],
)
def test_captured_profile_rejects_tampered_derived_aggregates(
    captured_profile: CapturedProfileFixture,
    section: str,
    index: int,
    field: str,
    value: object,
    reason: str,
) -> None:
    profile = copy.deepcopy(captured_profile.profile)
    profile[section][index][field] = value
    result = runner.validate_captured_runtime_profile(
        _json_bytes(profile), **captured_profile.validation_arguments()
    )
    _assert_rejected(result, reason)


def test_captured_profile_rejects_forged_duration_order_proof(
    captured_profile: CapturedProfileFixture,
) -> None:
    profile = copy.deepcopy(captured_profile.profile)
    profile["scheduler"]["expected_ordered_sha256"] = "0" * 64
    result = runner.validate_captured_runtime_profile(
        _json_bytes(profile), **captured_profile.validation_arguments()
    )
    _assert_rejected(result, "duration-order evidence is not reproducible")


def test_captured_profile_rejects_reordered_node_telemetry(
    captured_profile: CapturedProfileFixture,
) -> None:
    profile = copy.deepcopy(captured_profile.profile)
    profile["nodes"][0], profile["nodes"][1] = profile["nodes"][1], profile["nodes"][0]
    result = runner.validate_captured_runtime_profile(
        _json_bytes(profile), **captured_profile.validation_arguments()
    )
    _assert_rejected(result, "node rows do not preserve collection order")


def test_captured_profile_rejects_another_valid_provenance_envelope(
    captured_profile: CapturedProfileFixture,
) -> None:
    arguments = captured_profile.validation_arguments()
    arguments["expected_validation_provenance"]["boundary_id"] = "another-valid-boundary"
    result = runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)
    _assert_rejected(result, "validation provenance does not match runner envelope")


def test_captured_profile_rejects_incomplete_full_manifest_coverage(
    captured_profile: CapturedProfileFixture,
) -> None:
    arguments = captured_profile.validation_arguments()
    manifest = json.loads(captured_profile.full_test_manifest_bytes)
    manifest["tests"].append({"path": "tests/test_missing.py", "file_role": "test"})
    manifest["test_count"] += 1
    arguments["full_test_manifest_bytes"] = _json_bytes(manifest)
    result = runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)
    _assert_rejected(result, "collected file set does not match the full test manifest")


def test_captured_profile_binds_original_manifest_bytes_not_equivalent_json(
    captured_profile: CapturedProfileFixture,
) -> None:
    arguments = captured_profile.validation_arguments()
    arguments["duration_manifest"] = runner.CapturedDurationManifest(
        captured_profile.duration_manifest_bytes + b"\n",
        captured_profile.configured_manifest_path,
    )
    result = runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)
    _assert_rejected(result, "manifest_sha256")


def test_captured_profile_does_not_admit_telemetry_failure_sentinel(
    captured_profile: CapturedProfileFixture,
) -> None:
    sentinel = runner._runtime_profile_failure_payload(
        reason="synthetic missing telemetry", pytest_exitstatus=0
    )
    result = runner.validate_captured_runtime_profile(
        _json_bytes(sentinel), **captured_profile.validation_arguments()
    )
    assert result["profile_status"] == "FAIL"
    assert result["performance_evidence_status"] == "FAIL"


def test_captured_profile_false_telemetry_flag_cannot_bypass_aggregate_checks(
    captured_profile: CapturedProfileFixture,
) -> None:
    profile = copy.deepcopy(captured_profile.profile)
    profile["telemetry"]["complete"] = False
    profile["files"][0]["duration_seconds"] = 999.0
    result = runner.validate_captured_runtime_profile(
        _json_bytes(profile), **captured_profile.validation_arguments()
    )
    assert result["profile_status"] == "FAIL"
    assert result["performance_evidence_status"] == "FAIL"


@dataclass(frozen=True)
class SourceFixture:
    root: Path
    source_commit: str
    captured: CapturedProfileFixture
    profile_path: Path
    summary_path: Path
    profile: dict[str, Any]
    summary: dict[str, Any]

    def expected_kwargs(self) -> dict[str, Any]:
        return {
            "source_profile_path": self.profile_path,
            "source_summary_path": self.summary_path,
            "profile_id": "synthetic_refreshed_partial_seed",
            "version": 2,
            "expected_nodes": 32,
            "expected_files": 16,
        }

    def write_summary(self) -> None:
        self.summary_path.write_bytes(_json_bytes(self.summary))

    def write_profile(self, *, update_projection: bool = True) -> None:
        """Re-sign a tampered profile so tests exercise replay beyond its hash."""
        content = _json_bytes(self.profile)
        self.profile_path.write_bytes(content)
        self.summary["output_artifacts"][0].update(
            {"sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content)}
        )
        if update_projection:
            self.summary.update(
                runner._summarize_runtime_profile(self.profile, final_path=self.profile_path)
            )
        self.write_summary()


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        [
            "git",
            "-c",
            "core.autocrlf=false",
            "-c",
            "commit.gpgsign=false",
            "-c",
            f"core.hooksPath={root / '.no-hooks'}",
            "-c",
            "user.name=Duration Refresh Test",
            "-c",
            "user.email=duration-refresh@example.invalid",
            "-C",
            str(root),
            *arguments,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def build_source_fixture(tmp_path: Path) -> SourceFixture:
    """Create one pytest-owned Git source commit with both original manifests."""
    root = tmp_path.resolve()
    captured = build_captured_profile_fixture(
        configured_manifest_path=str(root / DURATION_MANIFEST_PATH)
    )
    for relative, content in (
        (DURATION_MANIFEST_PATH, captured.duration_manifest_bytes),
        (FULL_TEST_MANIFEST_PATH, captured.full_test_manifest_bytes),
        *(
            (path, b"def test_zeta():\n    pass\n\ndef test_alpha():\n    pass\n")
            for path in captured.test_files
        ),
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    _git(root, "init", "--quiet")
    _git(root, "add", "--", DURATION_MANIFEST_PATH, FULL_TEST_MANIFEST_PATH, *captured.test_files)
    _git(root, "commit", "--quiet", "--no-verify", "-m", "Synthetic original Full inputs")
    commit = _git(root, "rev-parse", "HEAD")
    artifact_dir = root / "outputs/validation_runtime/synthetic_source"
    artifact_dir.mkdir(parents=True)
    profile_path = artifact_dir / "test_runtime_profile.json"
    summary_path = artifact_dir / "test_runtime_summary.json"
    profile_path.write_bytes(captured.profile_bytes)
    provenance = copy.deepcopy(captured.validation_provenance)
    transaction_id = "synthetic-duration-refresh-source"
    summary = {
        "schema_version": 1,
        "report_type": "test_runtime_summary",
        "tier": "full",
        "requested_tier": "full",
        "resolved_tier": "full",
        "suite_family": "full_pytest",
        "print_only": False,
        "benchmark_mode": False,
        "extra_pytest_args": [],
        "pytest_output_captured": True,
        "status": "PASS",
        "exit_code": 0,
        "validation_provenance_status": "PASS",
        "workers": "16",
        "dist": "loadfile",
        "production_effect": "none",
        "strategy_logic_changed": False,
        "cached_data_mutated": False,
        "production_state_mutated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "git_commit": commit,
        "environment_summary": {"working_directory": str(root)},
        "summary_path": str(summary_path),
        "artifact_dir": str(artifact_dir),
        "validation_provenance": provenance,
        "publication_transaction": {
            "schema_version": "integration_publication_fence.v1",
            "status": "PASS",
            "phase": "FULL_DISPATCHED",
            "candidate_sha": commit,
            "expected_main_sha": commit,
            "task_id": provenance["task_id"],
            "transaction_id": transaction_id,
            "transaction_sha256": "2" * 64,
            "transaction_path": (
                "outputs/architecture/arch_005_integration_publication_fence/transactions/"
                f"{transaction_id}/transaction.json"
            ),
            "production_effect": "none",
            "broker_action": "none",
            "pre_dispatch_readiness": {
                "schema_version": "full_validation_readiness.v1",
                "status": "PASS",
                "candidate_sha": commit,
                "target_root": str(root),
                "inspection_code_root": str(root),
                "full_dispatch_ready": True,
                "blockers": [],
                "production_effect": "none",
                "broker_action": "none",
                "research_dispatch_allowed": False,
                "dq_validation_executed": False,
            },
        },
        "command": runner.build_command(
            "full", python_executable="python", repo_root=root, workers="16", dist="loadfile"
        ),
        "safety_boundary": {
            "production_effect": "none",
            "strategy_logic_changed": False,
            "cached_data_mutated": False,
            "production_state_mutated": False,
            "broker_action_allowed": False,
            "broker_action_taken": False,
        },
        "input_artifacts": [
            {
                "path": DURATION_MANIFEST_PATH,
                "exists": True,
                "sha256": hashlib.sha256(captured.duration_manifest_bytes).hexdigest(),
                "size_bytes": len(captured.duration_manifest_bytes),
            }
        ],
        "input_checksums": {
            DURATION_MANIFEST_PATH: hashlib.sha256(captured.duration_manifest_bytes).hexdigest()
        },
        "output_artifacts": [
            {
                "path": str(profile_path),
                "exists": True,
                "sha256": hashlib.sha256(captured.profile_bytes).hexdigest(),
                "size_bytes": len(captured.profile_bytes),
            }
        ],
        "started_at_utc": "1970-01-01T00:01:20+00:00",
        "ended_at_utc": "1970-01-01T00:02:10+00:00",
        "elapsed_seconds": 50.0,
        **runner._summarize_runtime_profile(captured.profile, final_path=profile_path),
    }
    fixture = SourceFixture(
        root=root,
        source_commit=commit,
        captured=captured,
        profile_path=profile_path,
        summary_path=summary_path,
        profile=copy.deepcopy(captured.profile),
        summary=summary,
    )
    fixture.write_summary()
    return fixture


@pytest.fixture
def source_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SourceFixture:
    fixture = build_source_fixture(tmp_path)
    monkeypatch.setattr(refresher, "PROJECT_ROOT", fixture.root)
    return fixture


def test_historical_expected_provenance_rejects_integer_boolean_substitution(
    captured_profile: CapturedProfileFixture,
) -> None:
    arguments = captured_profile.validation_arguments()
    arguments["expected_validation_provenance"]["required_for_tier"] = 1
    with pytest.raises(ValueError, match="historical expected provenance is invalid"):
        runner.validate_captured_runtime_profile(captured_profile.profile_bytes, **arguments)


def test_refresh_replays_git_source_and_keeps_all_files_with_stable_ties(
    source_fixture: SourceFixture,
) -> None:
    result = refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())

    expected_rows = [
        {
            "path": row["path"],
            "node_count": row["node_count"],
            "observed_seconds": row["duration_seconds"],
        }
        for row in sorted(source_fixture.profile["files"], key=lambda row: -row["duration_seconds"])
    ]
    assert result["status"] == "PARTIAL_SEED"
    assert result["files"] == expected_rows
    assert result["files"][0]["path"] == source_fixture.captured.test_files[-1]
    assert sum(row["node_count"] for row in result["files"]) == 32
    tied_paths = [row["path"] for row in result["files"] if row["observed_seconds"] == 6.0]
    assert tied_paths == ["tests/test_zeta.py", "tests/test_alpha.py", "tests/test_07.py"]
    assert result["review"]["stable_improvement_claimed"] is False
    proof = result["source"]["strict_replay"]
    assert proof["source_git_commit"] == source_fixture.source_commit
    assert proof["technical_validation_state"] == "PASS"
    assert (
        proof["duration_manifest_sha256"]
        == hashlib.sha256(source_fixture.captured.duration_manifest_bytes).hexdigest()
    )
    assert (
        proof["full_test_manifest_sha256"]
        == hashlib.sha256(source_fixture.captured.full_test_manifest_bytes).hexdigest()
    )


def test_refresh_uses_original_git_blobs_after_current_manifests_are_replaced(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_result = refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())
    current_paths = {
        source_fixture.root / DURATION_MANIFEST_PATH,
        source_fixture.root / FULL_TEST_MANIFEST_PATH,
    }
    for path in current_paths:
        path.write_bytes(b"current working tree has a different manifest\n")
    original_read_bytes = Path.read_bytes

    def reject_current_manifest_read(path: Path) -> bytes:
        if path in current_paths:
            raise AssertionError("Refresh must retrieve the original Git blob")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_current_manifest_read)
    result = refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())
    assert result == original_result


def test_refresh_captures_each_runtime_artifact_once(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def counted_read_bytes(path: Path) -> bytes:
        reads.append(path)
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)
    result = refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())
    assert result["source"]["strict_replay"]["technical_validation_state"] == "PASS"
    assert reads == [source_fixture.profile_path, source_fixture.summary_path]


def _set_nested(payload: dict[str, Any], path: tuple[str | int, ...], value: object) -> None:
    target = payload
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value


@pytest.mark.parametrize(
    ("path", "value", "reason"),
    [
        (("schema_version",), True, "runtime summary schema_version"),
        (("report_type",), "other_report", "runtime summary report_type"),
        (("tier",), "impact", "runtime summary tier"),
        (("requested_tier",), "impact", "runtime summary requested_tier"),
        (("resolved_tier",), "impact", "runtime summary resolved_tier"),
        (("suite_family",), "focused_pytest", "runtime summary suite_family"),
        (("print_only",), True, "runtime summary print_only"),
        (("benchmark_mode",), True, "runtime summary benchmark_mode"),
        (("extra_pytest_args",), ["-k", "selected"], "runtime summary extra_pytest_args"),
        (("pytest_output_captured",), False, "runtime summary pytest_output_captured"),
        (("status",), "FAIL", "runtime summary status"),
        (("exit_code",), False, "runtime summary exit_code"),
        (("workers",), True, "runtime summary workers must equal 16"),
        (("runtime_profile_status",), "FAIL", "runtime summary runtime_profile_status"),
        (("formal_full_selection_eligible",), False, "formal_full_selection_eligible"),
        (("cached_data_mutated",), True, "runtime summary cached_data_mutated"),
        (("production_state_mutated",), True, "runtime summary production_state_mutated"),
        (("broker_action_allowed",), True, "runtime summary broker_action_allowed"),
        (("safety_boundary", "broker_action_taken"), True, "source Full safety_boundary"),
        (("publication_transaction",), None, "publication_transaction must be a mapping"),
        (("publication_transaction", "status"), "FAIL", "source publication status"),
        (("publication_transaction", "phase"), "FORMAL_VALIDATION_PRE", "source publication phase"),
        (
            ("publication_transaction", "candidate_sha"),
            "3" * 40,
            "source publication candidate_sha",
        ),
        (("publication_transaction", "task_id"), "ANOTHER-TASK", "source publication task_id"),
        (("publication_transaction", "pre_dispatch_readiness"), None, "pre_dispatch_readiness"),
        (
            ("publication_transaction", "pre_dispatch_readiness", "status"),
            "FAIL",
            "source readiness status",
        ),
        (
            ("publication_transaction", "pre_dispatch_readiness", "candidate_sha"),
            "3" * 40,
            "source readiness candidate_sha",
        ),
        (
            ("publication_transaction", "pre_dispatch_readiness", "full_dispatch_ready"),
            1,
            "source readiness full_dispatch_ready",
        ),
        (
            ("publication_transaction", "pre_dispatch_readiness", "target_root"),
            "X:/another-source",
            "source readiness target_root",
        ),
        (
            ("publication_transaction", "pre_dispatch_readiness", "inspection_code_root"),
            "X:/another-source",
            "source readiness inspection_code_root",
        ),
        (
            ("environment_summary", "working_directory"),
            "X:/another-source",
            "source original working directory",
        ),
        (("runtime_profile_summary", "node_count"), True, "runtime_profile_summary.node_count"),
        (("runtime_profile_summary", "warning_count"), 1, "runtime_profile_summary.warning_count"),
        (("input_artifacts", 0, "sha256"), "0" * 64, "original manifest sha256"),
        (("input_artifacts", 0, "size_bytes"), 1, "original manifest size_bytes"),
        (("input_artifacts", 0, "exists"), 1, "original manifest exists"),
        (("input_checksums", DURATION_MANIFEST_PATH), "0" * 64, "original manifest checksum"),
        (("output_artifacts", 0, "sha256"), "0" * 64, "runtime profile inventory sha256"),
        (("output_artifacts", 0, "size_bytes"), 1, "runtime profile inventory size_bytes"),
        (("output_artifacts", 0, "exists"), 1, "runtime profile inventory exists"),
        (
            ("summary_path",),
            "outputs/another/test_runtime_summary.json",
            "runtime summary summary_path",
        ),
        (("artifact_dir",), "outputs/another", "runtime summary artifact_dir"),
        (
            ("runtime_profile_path",),
            "outputs/another/test_runtime_profile.json",
            "runtime summary runtime_profile_path",
        ),
        (("started_at_utc",), "1970-01-01T00:01:31+00:00", "summary UTC window"),
        (("ended_at_utc",), "1970-01-01T00:01:59+00:00", "summary UTC window"),
        (("started_at_utc",), "1970-01-01T00:01:20", "must include the UTC timezone"),
        (("elapsed_seconds",), 0, "summary elapsed_seconds must be positive and finite"),
        (("elapsed_seconds",), True, "summary elapsed_seconds must be positive and finite"),
    ],
)
def test_refresh_rejects_inconsistent_summary_evidence(
    source_fixture: SourceFixture,
    path: tuple[str | int, ...],
    value: object,
    reason: str,
) -> None:
    _set_nested(source_fixture.summary, path, value)
    source_fixture.write_summary()
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


@pytest.mark.parametrize("field", ("input_artifacts", "output_artifacts"))
def test_refresh_rejects_duplicate_inventory_entries(
    source_fixture: SourceFixture, field: str
) -> None:
    source_fixture.summary[field].append(copy.deepcopy(source_fixture.summary[field][0]))
    source_fixture.write_summary()
    reason = (
        "inventory original duration manifest exactly once"
        if field == "input_artifacts"
        else ("inventory the exact runtime profile once")
    )
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def test_refresh_rejects_valid_but_different_profile_provenance(
    source_fixture: SourceFixture,
) -> None:
    source_fixture.profile["validation_provenance"]["boundary_id"] = "different-profile-boundary"
    source_fixture.write_profile()
    with pytest.raises(ValueError, match="validation provenance does not match runner envelope"):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def test_refresh_rejects_a_filtered_recorded_command(source_fixture: SourceFixture) -> None:
    source_fixture.summary["command"].extend(["-k", "one_node"])
    source_fixture.write_summary()
    with pytest.raises(ValueError, match="source Full command"):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


@pytest.mark.parametrize(
    ("section", "field", "value", "reason"),
    [
        ("files", "duration_seconds", 999.0, "file aggregate is not node-derived"),
        ("workers", "busy_seconds", 999.0, "worker aggregate is not node-derived"),
        ("nodes", "duration_seconds", 999.0, "node timing is not phase-derived"),
    ],
)
def test_refresh_rejects_resigned_aggregate_tampering(
    source_fixture: SourceFixture, section: str, field: str, value: object, reason: str
) -> None:
    source_fixture.profile[section][0][field] = value
    source_fixture.write_profile()
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def test_refresh_rejects_resigned_equal_duration_file_order_tampering(
    source_fixture: SourceFixture,
) -> None:
    rows = source_fixture.profile["files"]
    assert rows[0]["duration_seconds"] == rows[1]["duration_seconds"]
    rows[0], rows[1] = rows[1], rows[0]
    source_fixture.write_profile()
    with pytest.raises(ValueError, match="file.*order"):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def test_refresh_rejects_zero_file_duration_even_with_complete_valid_telemetry(
    source_fixture: SourceFixture,
) -> None:
    zero_file = source_fixture.captured.test_files[0]
    reports = []
    for node in source_fixture.profile["nodes"]:
        for phase in node["phases"]:
            zero = node["file"] == zero_file
            start = node["start_epoch_seconds"] if zero else phase["start_epoch_seconds"]
            stop = start if zero else phase["stop_epoch_seconds"]
            reports.append(
                {
                    "nodeid": node["nodeid"],
                    "phase": phase["phase"],
                    "worker_id": phase["worker_id"],
                    "start": start,
                    "stop": stop,
                    "duration": stop - start,
                    "outcome": phase["outcome"],
                }
            )
    profile = build_runtime_profile(
        collections={
            f"gw{index}": source_fixture.profile["collection"]["nodeids"] for index in range(16)
        },
        phase_reports=reports,
        duration_profile=load_duration_profile(source_fixture.root / DURATION_MANIFEST_PATH),
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=90.0,
        ended_at=120.0,
        validation_provenance=source_fixture.captured.validation_provenance,
    )
    source_fixture.profile.clear()
    source_fixture.profile.update(profile)
    source_fixture.write_profile()
    validated = runner.validate_captured_runtime_profile(
        source_fixture.profile_path.read_bytes(), **source_fixture.captured.validation_arguments()
    )
    assert validated["profile_status"] == "PASS"
    assert validated["performance_evidence_status"] == "PASS"
    with pytest.raises(ValueError, match="duration_seconds is invalid for tests/test_zeta.py"):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


@pytest.mark.parametrize("artifact", ("profile", "summary"))
@pytest.mark.parametrize("invalid", ("duplicate", "nonfinite"))
def test_refresh_rejects_noncanonical_json(
    source_fixture: SourceFixture, artifact: str, invalid: str
) -> None:
    path = source_fixture.profile_path if artifact == "profile" else source_fixture.summary_path
    duplicate_key = b'"profile_status":"PASS",' if artifact == "profile" else b'"status":"PASS",'
    injected = duplicate_key if invalid == "duplicate" else b'"elapsed_seconds":NaN,'
    path.write_bytes(b"{" + injected + path.read_bytes()[1:])
    reason = "duplicate JSON key" if invalid == "duplicate" else "non-finite JSON constant"
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def _bind_source_commit(fixture: SourceFixture, commit: str) -> None:
    fixture.summary["git_commit"] = commit
    fixture.summary["publication_transaction"]["candidate_sha"] = commit
    fixture.summary["publication_transaction"]["pre_dispatch_readiness"]["candidate_sha"] = commit
    fixture.write_summary()


@pytest.mark.parametrize("identity", ("branch_name", "unknown_sha", "tree_object"))
def test_refresh_requires_an_existing_exact_commit_object(
    source_fixture: SourceFixture, identity: str
) -> None:
    values = {
        "branch_name": "HEAD",
        "unknown_sha": "0" * 40,
        "tree_object": _git(source_fixture.root, "rev-parse", "HEAD^{tree}"),
    }
    _bind_source_commit(source_fixture, values[identity])
    reason = (
        "exact lowercase SHA" if identity == "branch_name" else "must resolve to a commit object"
    )
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


@pytest.mark.parametrize("relative_path", (DURATION_MANIFEST_PATH, FULL_TEST_MANIFEST_PATH))
def test_refresh_rejects_missing_original_manifest_blob(
    source_fixture: SourceFixture, relative_path: str
) -> None:
    _git(source_fixture.root, "rm", "--quiet", "--", relative_path)
    _git(source_fixture.root, "commit", "--quiet", "--no-verify", "-m", "Missing source contract")
    _bind_source_commit(source_fixture, _git(source_fixture.root, "rev-parse", "HEAD"))
    with pytest.raises(ValueError, match="original source manifest is unavailable"):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


def test_refresh_replays_full_file_coverage_from_the_source_git_manifest(
    source_fixture: SourceFixture,
) -> None:
    manifest = json.loads(source_fixture.captured.full_test_manifest_bytes)
    manifest["tests"].pop()
    manifest["test_count"] -= 1
    (source_fixture.root / FULL_TEST_MANIFEST_PATH).write_bytes(_json_bytes(manifest))
    _git(source_fixture.root, "add", "--", FULL_TEST_MANIFEST_PATH)
    _git(source_fixture.root, "commit", "--quiet", "--no-verify", "-m", "Changed Full test scope")
    _bind_source_commit(source_fixture, _git(source_fixture.root, "rev-parse", "HEAD"))
    with pytest.raises(
        ValueError, match="collected file set does not match the full test manifest"
    ):
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())


@pytest.mark.parametrize("layout", ("wrong_filename", "non_sibling", "outside_project_root"))
def test_refresh_requires_canonical_source_artifact_locations(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    arguments = source_fixture.expected_kwargs()
    if layout == "outside_project_root":
        monkeypatch.setattr(refresher, "PROJECT_ROOT", source_fixture.root / "another_root")
        reason = "source artifact must remain inside project root"
    else:
        if layout == "wrong_filename":
            path = source_fixture.summary_path.with_name("renamed_summary.json")
            reason = "source artifact must use canonical filename"
        else:
            path = source_fixture.root / "other_artifacts/test_runtime_summary.json"
            path.parent.mkdir()
            reason = "source summary and profile must be canonical siblings"
        path.write_bytes(source_fixture.summary_path.read_bytes())
        arguments["source_summary_path"] = path
    with pytest.raises(ValueError, match=reason):
        refresher.build_partial_duration_manifest(**arguments)


def _source_file_snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if ".git" not in path.relative_to(root).parts and path.is_file()
    }


def _cli_arguments(fixture: SourceFixture, *, output: Path, write: bool = False) -> list[str]:
    return [
        "refresh_partial_duration_profile.py",
        "--source-profile",
        str(fixture.profile_path),
        "--source-summary",
        str(fixture.summary_path),
        "--profile-id",
        "synthetic_refreshed_partial_seed",
        "--version",
        "2",
        "--expected-nodes",
        "32",
        "--expected-files",
        "16",
        "--output",
        str(output),
        *(["--write"] if write else []),
    ]


def test_refresh_cli_defaults_to_dry_run_without_any_file_write(
    source_fixture: SourceFixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = _source_file_snapshot(source_fixture.root)
    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(source_fixture, output=source_fixture.root / DURATION_MANIFEST_PATH),
    )

    def forbidden_write(*args: object, **kwargs: object) -> None:
        raise AssertionError("Dry-run must not invoke the writer")

    monkeypatch.setattr(refresher, "_atomic_write", forbidden_write)
    assert refresher.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "DRY_RUN"
    assert result["file_count"] == 16
    assert _source_file_snapshot(source_fixture.root) == before


def test_refresh_cli_invalid_admission_never_writes(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_fixture.profile["files"][0]["duration_seconds"] = 999.0
    source_fixture.write_profile()
    before = _source_file_snapshot(source_fixture.root)
    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(
            source_fixture, output=source_fixture.root / DURATION_MANIFEST_PATH, write=True
        ),
    )
    writes: list[object] = []
    monkeypatch.setattr(refresher, "_atomic_write", lambda *arguments: writes.append(arguments))
    with pytest.raises(ValueError, match="file aggregate is not node-derived"):
        refresher.main()
    assert writes == []
    assert _source_file_snapshot(source_fixture.root) == before


@pytest.mark.parametrize("alias", (False, True))
def test_refresh_cli_write_changes_only_the_canonical_manifest(
    source_fixture: SourceFixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    alias: bool,
) -> None:
    before = _source_file_snapshot(source_fixture.root)
    relative_output = (
        "inputs/architecture/../architecture/arch_004g2_full_duration_profile.yaml"
        if alias
        else DURATION_MANIFEST_PATH
    )
    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(source_fixture, output=source_fixture.root / relative_output, write=True),
    )
    assert refresher.main() == 0
    result = json.loads(capsys.readouterr().out)
    after = _source_file_snapshot(source_fixture.root)
    assert before.keys() == after.keys()
    assert [path for path in before if before[path] != after[path]] == [DURATION_MANIFEST_PATH]
    assert result["mode"] == "WRITE"
    assert result["output"] == DURATION_MANIFEST_PATH
    assert result["output_sha256"] == hashlib.sha256(after[DURATION_MANIFEST_PATH]).hexdigest()
    assert load_duration_profile(source_fixture.root / DURATION_MANIFEST_PATH).valid is True
    # The newly written seed must not alter admission of the immutable prior source.
    repeated = refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())
    assert repeated["source"]["strict_replay"]["source_git_commit"] == source_fixture.source_commit


def test_refresh_cli_resolved_alias_writes_canonical_file_and_preserves_alias_bytes(
    source_fixture: SourceFixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    canonical_path = source_fixture.root / DURATION_MANIFEST_PATH
    alias_path = source_fixture.root / "duration_manifest_alias.yaml"
    alias_bytes = b"alias bytes must survive the canonical publication\n"
    alias_path.write_bytes(alias_bytes)
    before = _source_file_snapshot(source_fixture.root)
    expected_bytes = refresher._serialized_yaml(
        refresher.build_partial_duration_manifest(**source_fixture.expected_kwargs())
    )
    original_resolve = Path.resolve

    def resolve_alias(path: Path, strict: bool = False) -> Path:
        # Model only this alias identity without requiring Windows symlink rights.
        # Reads and the real atomic writer still operate on two distinct files.
        if path == alias_path:
            return canonical_path
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve_alias)
    monkeypatch.setattr(sys, "argv", _cli_arguments(source_fixture, output=alias_path, write=True))
    assert refresher.main() == 0
    result = json.loads(capsys.readouterr().out)
    after = _source_file_snapshot(source_fixture.root)

    assert canonical_path.read_bytes() == expected_bytes
    assert alias_path.read_bytes() == alias_bytes
    assert before.keys() == after.keys()
    assert [path for path in before if before[path] != after[path]] == [DURATION_MANIFEST_PATH]
    assert result["output"] == DURATION_MANIFEST_PATH
    assert result["output_sha256"] == hashlib.sha256(expected_bytes).hexdigest()


@pytest.mark.parametrize(
    "relative_output",
    (
        FULL_TEST_MANIFEST_PATH,
        "outputs/new_seed.yaml",
        "outputs/validation_runtime/synthetic_source/test_runtime_profile.json",
    ),
)
def test_refresh_cli_rejects_unapproved_write_targets(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch, relative_output: str
) -> None:
    before = _source_file_snapshot(source_fixture.root)
    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(source_fixture, output=source_fixture.root / relative_output, write=True),
    )
    with pytest.raises(
        ValueError, match="write target must be the governed Full duration manifest"
    ):
        refresher.main()
    assert _source_file_snapshot(source_fixture.root) == before


def test_refresh_cli_failed_atomic_replace_preserves_original_files(
    source_fixture: SourceFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _source_file_snapshot(source_fixture.root)
    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(
            source_fixture, output=source_fixture.root / DURATION_MANIFEST_PATH, write=True
        ),
    )

    def fail_replace(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic replace failure")

    monkeypatch.setattr(refresher.os, "replace", fail_replace)
    with pytest.raises(OSError, match="synthetic replace failure"):
        refresher.main()
    assert _source_file_snapshot(source_fixture.root) == before
