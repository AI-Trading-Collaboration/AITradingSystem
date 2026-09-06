"""Committed actual-checkout E2E; every market-like byte is a synthetic fixture.

This file deliberately fails without an explicitly associated live coordinator
fence/lease. It does not create a copied source checkout, infer local task paths,
skip dirty/uncommitted code, or acquire authority from environment variables.
Dispatch only after the source commit and formal candidate binding. Ordinary
CLI RUN/VERIFY are tested separately from the fixed, non-production TEST_PROBE.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from named_data_quality_support import (
    AS_OF,
    BOOTSTRAP_PATH,
    LEASE_ENV,
    PRICE_BYTES,
    RATE_BYTES,
    ROOT,
    SOURCE_MANIFEST_PATH,
    START,
    TRANSACTION_ENV,
    NamedExecutionFixture,
    ParentDispatchResult,
    build_actual_candidate_fixture,
    dispatch_actual_candidate_child,
)

from ai_trading_system.contracts.named_data_quality_execution import (
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQSuccessfulDispatchBinding,
)

# Reviewed finite source-closure/schema facts, not statistical acceptance rules.
EXPECTED_MODULE_COUNT = 55
CURRENT_CALENDAR_PATH = "config/data/us_equity_special_closure_registry.yaml"
HISTORICAL_CALENDAR_PATH = "config/data/archive/us_equity_special_closure_registry_1_0_0.yaml"
EXPECTED_DEPENDENCIES = {
    "config/data_quality.yaml",
    CURRENT_CALENDAR_PATH,
    HISTORICAL_CALENDAR_PATH,
    "config/data_quality/price_non_market_session_attribution_decision_v1.yaml",
    "inputs/data_quality/price_non_market_session_attribution_review_pack_v1.json",
    "config/data_quality/rate_row_issue_attribution_decision_v1.yaml",
    "inputs/data_quality/rate_issue_attribution_review_pack_v1.json",
}


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _read_receipt(
    fixture: NamedExecutionFixture, run: ParentDispatchResult
) -> NamedDQExecutionReceipt:
    content = (fixture.evidence_root / run.child_result["receipt_path"]).read_bytes()
    assert _sha(content) == run.child_result["receipt_sha256"]
    receipt = NamedDQExecutionReceipt.from_json_bytes(content)
    assert receipt.canonical_bytes == content
    assert receipt.request == fixture.request
    assert receipt.receipt_id == run.child_result["receipt_id"]
    return receipt


def _read_bound(binding: dict[str, Any]) -> bytes:
    content = Path(binding["path"]).read_bytes()
    assert _sha(content) == binding["sha256"]
    assert len(content) == binding["size_bytes"]
    return content


def _assert_parent(result: ParentDispatchResult, *, calls: int) -> None:
    parent = result.parent_receipt
    assert json.loads(result.parent_receipt_path.read_bytes()) == parent
    assert parent["schema_version"] == "named_data_quality_parent_dispatch.v1"
    assert parent["profile"] == "ACTUAL_CANDIDATE_SYNTHETIC_E2E"
    assert parent["status"] == "PASS"
    assert parent["terminal_state"] == "EXITED"
    assert parent["failure"] is None
    launch = parent["launch_audit"]
    assert launch["schema_version"] == "named_data_quality_python_launch.v1"
    assert launch["semantics"] == "PARENT_INTERPRETER_LAUNCH_SELECTION_ONLY"
    assert launch["implementation"] == sys.implementation.name == "cpython"
    assert launch["platform"] == sys.platform
    assert Path(launch["logical_executable"]) == Path(sys.executable)
    assert Path(launch["base_executable"]) == Path(sys._base_executable)
    assert Path(launch["logical_prefix"]) == Path(sys.prefix)
    assert Path(launch["base_prefix"]) == Path(sys.base_prefix)
    windows_venv = sys.platform == "win32" and Path(sys.prefix) != Path(sys.base_prefix)
    assert launch["profile"] == (
        "CPYTHON_WINDOWS_DIRECT_VENV" if windows_venv else "CPYTHON_DIRECT"
    )
    assert launch["windows_redirector_bypassed"] is windows_venv
    assert Path(launch["os_executable"]) == Path(
        sys._base_executable if windows_venv else sys.executable
    )
    assert parent["command"][:5] == [launch["os_executable"], "-I", "-B", "-X", "utf8"]
    assert launch["child_environment_overrides"] == (
        {"__PYVENV_LAUNCHER__": sys.executable} if windows_venv else {}
    )
    assert all(
        key.upper() in {"PYTHONEXECUTABLE", "__PYVENV_LAUNCHER__"}
        for key in launch["child_environment_removed_keys"]
    )
    assert launch["parent_environment_mutated"] is False
    assert launch["dispatch_authority_granted"] is False
    assert parent["source_checkout_copied"] is False
    assert parent["synthetic_inputs_only"] is True
    assert parent["real_market_dq_call_count"] == 0
    assert parent["lease_acquired_or_mutated"] is False
    assert parent["verified_input_seal_exported"] is False
    assert parent["dispatch_allowed"] is False
    assert parent["production_effect"] == parent["broker_action"] == "none"
    assert parent["observed_canonical_dq_call_count"] == calls
    assert parent["counter_observation_state"] == "KNOWN"
    assert result.child_result["canonical_dq_call_count"] == calls
    assert json.loads(_read_bound(parent["child_stdout"])) == result.child_result
    request = json.loads(_read_bound(parent["request"]))
    assert request["candidate_commit"] == parent["candidate_commit"]
    assert request["roots"]["execution_root"] == ROOT.as_posix()
    for name in ("pre_dispatch_proof", "post_dispatch_proof"):
        proof = json.loads(_read_bound(parent[name]))
        assert proof["schema_version"] == "named_dq_existing_parent_proof.v1"
        assert proof["status"] == "PASS"
        assert proof["candidate_binding_role"] == "FORMAL_CANDIDATE"
        assert proof["execution_root"] == parent["execution_root"] == ROOT.as_posix()
        assert proof["candidate_commit"] == parent["candidate_commit"]
        assert proof["request_sha256"] == parent["request"]["sha256"]
        assert proof["environment_inputs"] == parent["environment_inputs"]
        assert proof["environment_inputs"][LEASE_ENV] == parent["source_lease_id"]
        assert proof["environment_inputs"][TRANSACTION_ENV]
        assert proof["active_lease"]["lease_id"] == parent["source_lease_id"]
        assert proof["active_lease"]["state"] == "ACTIVE"
        assert datetime.fromisoformat(proof["checked_at"]) < datetime.fromisoformat(
            proof["active_lease"]["expires_at"]
        )
        assert proof["fence_binding"]["status"] == "PASS"
        assert proof["lease_replay"]["status"] == "PASS"
        assert proof["publication_replay"]["status"] == "PASS"
        assert proof["lease_acquired_or_mutated"] is False
        assert proof["dispatch_authority_granted_by_environment"] is False
        transaction_bytes = Path(proof["transaction"]["path"]).read_bytes()
        assert _sha(transaction_bytes) == proof["transaction"]["raw_file_sha256"]
        assert (
            json.loads(transaction_bytes)["transaction_sha256"]
            == proof["transaction"]["internal_transaction_sha256"]
        )
        binding_bytes = (
            json.dumps(proof["fence_binding"], ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")
        assert _sha(binding_bytes) == proof["fence_binding_sha256"]
        for event in ("lease_head_event", "publication_head_event"):
            assert _sha(Path(proof[event]["path"]).read_bytes()) == proof[event]["raw_file_sha256"]
    if result.returncode == 0:
        assert result.child_result["process_id"] == parent["child_pid"]
        assert result.child_result["source_lease_id"] == parent["source_lease_id"]
        assert result.child_result["verified_input_seal_exported"] is False
        assert datetime.fromisoformat(parent["spawn_requested_at"]) <= datetime.fromisoformat(
            result.child_result["child_started_at"]
        )
        assert datetime.fromisoformat(
            result.child_result["child_terminal_checked_at"]
        ) <= datetime.fromisoformat(parent["terminal_observed_at"])


def _pass_run(fixture: NamedExecutionFixture) -> ParentDispatchResult:
    result = dispatch_actual_candidate_child(fixture)
    assert result.returncode == 0, result.child_result
    assert result.child_result["status"] == "PASS", result.child_result
    _assert_parent(result, calls=1)
    return result


def _fixture_tree(root: Path) -> dict[str, str]:
    """Only this test's synthetic publication/evidence, never repository search."""
    return {
        path.relative_to(root).as_posix(): _sha(path.read_bytes())
        for path in root.rglob("*")
        if path.is_file()
    }


def _relocated_member(fixture: NamedExecutionFixture, original: Path) -> Path:
    relative = original.relative_to(fixture.source_root / "data/raw")
    return fixture.publication_root / relative


def test_actual_cli_run_once_then_independent_zero_dq_verifier(tmp_path: Path) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    run = _pass_run(fixture)
    receipt = _read_receipt(fixture, run)
    publication_before = _fixture_tree(fixture.publication_root)
    evidence_before = _fixture_tree(fixture.evidence_root)
    verified = dispatch_actual_candidate_child(fixture, operation="verify", successful_run=run)
    assert verified.returncode == 0, verified.child_result
    assert verified.child_result["status"] == "PASS"
    _assert_parent(verified, calls=0)
    assert run.parent_receipt["child_entrypoint_profile"] == "PRODUCTION_CLI"
    assert verified.parent_receipt["child_entrypoint_profile"] == "PRODUCTION_CLI"
    assert run.parent_receipt["child_pid"] != verified.parent_receipt["child_pid"]
    assert verified.child_result["original_dq_pid"] == receipt.execution_observation.execution_pid
    assert verified.child_result["verifier_pid"] == verified.parent_receipt["child_pid"]
    assert verified.child_result["input_sha256"] == {
        "prices": _sha(PRICE_BYTES),
        "rates": _sha(RATE_BYTES),
    }
    assert _fixture_tree(fixture.publication_root) == publication_before
    assert _fixture_tree(fixture.evidence_root) == evidence_before
    assert verified.run_dispatch_path is verified.run_dispatch_sha256 is None

    execution = receipt.execution
    assert Path(execution.python_executable) == Path(
        run.parent_receipt["launch_audit"]["logical_executable"]
    )
    assert execution.source_kind == "GIT_COMMIT_BYTES_COMPILED"
    assert execution.execution_root == ROOT.as_posix()
    assert execution.candidate_commit == fixture.request.candidate_commit
    manifest_bytes = (ROOT / SOURCE_MANIFEST_PATH).read_bytes()
    manifest = json.loads(manifest_bytes)
    assert execution.source_manifest_sha256 == _sha(manifest_bytes)
    assert execution.bootstrap_sha256 == _sha((ROOT / BOOTSTRAP_PATH).read_bytes())
    assert len(execution.modules) == len(manifest["modules"]) == EXPECTED_MODULE_COUNT
    expected_modules = {row["module_name"]: row for row in manifest["modules"]}
    for compiled in execution.modules:
        assert expected_modules[compiled.module_name] == {
            "module_name": compiled.module_name,
            "source_path": compiled.source_path,
            "is_package": compiled.is_package,
        }
        source_bytes = (ROOT / compiled.source_path).read_bytes()
        assert (compiled.sha256, compiled.size_bytes) == (_sha(source_bytes), len(source_bytes))
        # Git's raw blob identity, no filters/textconv or project dynamic import.
        blob_algorithm = "sha1" if len(compiled.git_blob_id) == 40 else "sha256"
        blob = f"blob {len(source_bytes)}\0".encode("ascii") + source_bytes
        assert hashlib.new(blob_algorithm, blob).hexdigest() == compiled.git_blob_id
    assert sum(module.is_package for module in execution.modules) == 7

    dependencies = {item.relative_path: item for item in receipt.execution_dependencies}
    assert set(dependencies) == set(manifest["policy_dependencies"]) == EXPECTED_DEPENDENCIES
    for path, binding in dependencies.items():
        content = (ROOT / path).read_bytes()
        assert (binding.sha256, binding.size_bytes) == (_sha(content), len(content))
    assert receipt.calendar.special_closure_policy_version == "1.1.0"
    assert receipt.calendar_policy == dependencies[CURRENT_CALENDAR_PATH]
    assert receipt.calendar.special_closure_policy_sha256 == receipt.calendar_policy.sha256
    assert dependencies[HISTORICAL_CALENDAR_PATH].sha256 == (
        "c0469a17a775df2dcde503c254c22db0cc7d8ad6e3a5884f2ed43c88e4dfbda4"
    )
    assert receipt.calendar_policy.sha256 != dependencies[HISTORICAL_CALENDAR_PATH].sha256
    assert (
        receipt.price_consistency_start_date
        == receipt.rate_consistency_start_date
        == date(2021, 2, 22)
    )
    assert receipt.evaluated_window.start == START
    assert receipt.evaluated_window.end == AS_OF

    original_manifest = fixture.publication.manifest_path.read_bytes()
    assert (fixture.publication_root / receipt.manifest.member.relative_path).read_bytes() == (
        original_manifest
    )
    assert receipt.manifest.member.sha256 == _sha(original_manifest)
    original_rows = list(csv.DictReader(io.StringIO(original_manifest.decode("utf-8"))))
    assert [dict(row.fields) for row in receipt.manifest.rows] == original_rows
    for item in receipt.inputs:
        assert (fixture.publication_root / item.member.relative_path).read_bytes() == (
            fixture.input_bytes[item.role]
        )
        assert item.member.sha256 == verified.child_result["input_sha256"][item.role]
        assert item.original_output_path == original_rows[item.manifest_row_ordinal]["output_path"]
        assert (
            item.manifest_row_sha256 == receipt.manifest.rows[item.manifest_row_ordinal].row_sha256
        )

    assert run.run_dispatch_path is not None
    proof_bytes = (ROOT / run.run_dispatch_path).read_bytes()
    assert _sha(proof_bytes) == run.run_dispatch_sha256
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(proof_bytes)
    proof.assert_matches_receipt(receipt, receipt_path=run.child_result["receipt_path"])
    assert proof.execution_identity_sha256 == execution.stable_identity_sha256
    assert proof.execution_pid == run.parent_receipt["child_pid"]
    assert proof.parent_receipt.sha256 == _sha(run.parent_receipt_path.read_bytes())
    assert proof.proof_semantics == "TRUSTED_COORDINATOR_CORRELATION_ONLY"


def test_real_seal_fixed_test_probe_uses_exact_context_without_export(tmp_path: Path) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    run = _pass_run(fixture)
    receipt = _read_receipt(fixture, run)
    publication_before = _fixture_tree(fixture.publication_root)
    evidence_before = _fixture_tree(fixture.evidence_root)
    probe = dispatch_actual_candidate_child(
        fixture, operation="verify", successful_run=run, test_probe=True
    )
    assert probe.returncode == 0, probe.child_result
    _assert_parent(probe, calls=0)
    assert probe.child_result["status"] == "PASS"
    assert probe.child_result["profile"] == "TEST_PROBE"
    assert probe.parent_receipt["child_entrypoint_profile"] == "TEST_PROBE"
    assert len(probe.parent_receipt["fixed_test_probe_sha256"]) == 64
    assert probe.child_result["probe_checks"] == {
        "physical_module_origins": True,
        "verifier_code_origin": True,
        "pickle_rejected": True,
        "expanded_scope_rejected": True,
        "closed_context_accessor_rejected": True,
    }
    assert probe.child_result["actual_compiled_module_count"] == EXPECTED_MODULE_COUNT
    assert (
        probe.child_result["execution_identity_sha256"] == receipt.execution.stable_identity_sha256
    )
    assert probe.child_result["context_provenance_kind"] == "GIT_COMMIT_BYTES_COMPILED"
    assert probe.child_result["verifier_pid"] != run.parent_receipt["child_pid"]
    assert probe.child_result["input_sha256"] == {
        role: _sha(b) for role, b in fixture.input_bytes.items()
    }
    assert probe.run_dispatch_path is probe.run_dispatch_sha256 is None
    assert _fixture_tree(fixture.publication_root) == publication_before
    assert _fixture_tree(fixture.evidence_root) == evidence_before


@pytest.mark.parametrize(
    ("last_rate", "status", "issue_code"),
    [
        ("5.2", "PASS_WITH_WARNINGS", "rates_suspicious_daily_change"),
        ("7.2", "FAIL", "rates_extreme_daily_change"),
    ],
)
def test_actual_canonical_warn_fail_retains_run_proof_but_never_strict_seal(
    tmp_path: Path, last_rate: str, status: str, issue_code: str
) -> None:
    # Fixed synthetic rows exercise the existing 0.75 / 2.00 reviewed thresholds;
    # no policy, validator, or report is replaced to manufacture these statuses.
    rates = f"date,series,value\n2026-09-02,DGS10,4.2\n2026-09-03,DGS10,{last_rate}\n".encode()
    fixture = build_actual_candidate_fixture(tmp_path, rates_content=rates)
    run = dispatch_actual_candidate_child(fixture)
    assert run.returncode == 0, run.child_result
    assert run.child_result["status"] == status, run.child_result
    _assert_parent(run, calls=1)
    receipt = _read_receipt(fixture, run)
    assert receipt.report.status == status
    assert issue_code in receipt.report.issue_codes
    assert run.run_dispatch_path is not None
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(
        (ROOT / run.run_dispatch_path).read_bytes()
    )
    proof.assert_matches_receipt(receipt, receipt_path=run.child_result["receipt_path"])
    verified = dispatch_actual_candidate_child(fixture, operation="verify", successful_run=run)
    assert verified.returncode == 2, verified.child_result
    assert verified.child_result["status"] == "BLOCKED"
    assert verified.child_result["reason_code"] == "NAMED_DQ_STRICT_PASS_REQUIRED"
    _assert_parent(verified, calls=0)
    assert verified.run_dispatch_path is None
    assert not list(fixture.evidence_root.rglob("*verified*"))


@pytest.mark.parametrize("damage", ["missing_prices", "changed_prices", "changed_manifest"])
def test_missing_or_changed_immutable_member_blocks_before_dq(tmp_path: Path, damage: str) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    original = (
        fixture.publication.manifest_path
        if damage == "changed_manifest"
        else fixture.publication.prices_path
    )
    member = _relocated_member(fixture, original)
    assert member.is_relative_to(fixture.publication_root)
    if damage == "missing_prices":
        member.unlink()  # Only a newly created synthetic immutable member.
    else:
        member.write_bytes(member.read_bytes() + b"synthetic tampering\n")
    blocked = dispatch_actual_candidate_child(fixture)
    assert blocked.returncode == 2, blocked.child_result
    assert blocked.child_result["status"] == "BLOCKED"
    _assert_parent(blocked, calls=0)
    assert blocked.run_dispatch_path is blocked.run_dispatch_sha256 is None
    assert not _fixture_tree(fixture.evidence_root)


@pytest.mark.parametrize("damage", ["receipt", "dispatch_proof", "report"])
def test_receipt_dispatch_proof_and_report_bytes_tamper_fail_closed(
    tmp_path: Path, damage: str
) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    run = _pass_run(fixture)
    receipt = _read_receipt(fixture, run)
    if damage == "report":
        report = fixture.evidence_root / receipt.report.path
        report.write_bytes(report.read_bytes() + b"\n")
        blocked = dispatch_actual_candidate_child(fixture, operation="verify", successful_run=run)
        assert blocked.returncode == 2, blocked.child_result
        assert blocked.child_result["reason_code"] == "NAMED_DQ_REPORT_SHA_MISMATCH"
        _assert_parent(blocked, calls=0)
        return
    if damage == "receipt":
        path = fixture.evidence_root / run.child_result["receipt_path"]
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        assert run.run_dispatch_path is not None
        original = ROOT / run.run_dispatch_path
        # Preserve the immutable original runtime evidence. This separately named
        # adversarial copy claims its original digest and must fail before spawn.
        damaged = original.with_name("adversarial_changed_dispatch_proof.json")
        with damaged.open("xb") as stream:
            stream.write(original.read_bytes() + b"\n")
        run = replace(run, run_dispatch_path=damaged.relative_to(ROOT).as_posix())
    with pytest.raises(pytest.fail.Exception, match="NAMED_PARENT_SUCCESSFUL_RUN_REQUIRED"):
        dispatch_actual_candidate_child(fixture, operation="verify", successful_run=run)


def test_parent_rejects_failed_run_even_when_original_pass_receipt_remains(tmp_path: Path) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    run = _pass_run(fixture)
    receipt = _read_receipt(fixture, run)
    assert receipt.report.status == "PASS"
    unsuccessful = [
        replace(run, returncode=2),
        replace(run, returncode=False),
        replace(run, run_dispatch_path=None, run_dispatch_sha256=None),
        replace(run, parent_receipt={**run.parent_receipt, "terminal_state": "TIMED_OUT"}),
        replace(run, parent_receipt={**run.parent_receipt, "status": "BLOCKED"}),
    ]
    for failed_run in unsuccessful:
        with pytest.raises(pytest.fail.Exception, match="NAMED_PARENT_SUCCESSFUL_RUN_REQUIRED"):
            dispatch_actual_candidate_child(fixture, operation="verify", successful_run=failed_run)

    # A separately preserved adversarial parent/proof chain has internally valid
    # hashes but an observed failed postguard. The original successful evidence
    # remains untouched; a PASS DQ receipt cannot override this failed terminal.
    post = json.loads(_read_bound(run.parent_receipt["post_dispatch_proof"]))
    post["status"] = "BLOCKED"
    damaged_post_bytes = (json.dumps(post, sort_keys=True, indent=2) + "\n").encode()
    damaged_post_path = run.parent_receipt_path.with_name("adversarial_blocked_postguard.json")
    with damaged_post_path.open("xb") as stream:
        stream.write(damaged_post_bytes)
    damaged_parent = {
        **run.parent_receipt,
        "post_dispatch_proof": {
            "path": damaged_post_path.as_posix(),
            "sha256": _sha(damaged_post_bytes),
            "size_bytes": len(damaged_post_bytes),
        },
    }
    damaged_parent_bytes = (json.dumps(damaged_parent, sort_keys=True, indent=2) + "\n").encode()
    damaged_parent_path = run.parent_receipt_path.with_name("adversarial_postguard_parent.json")
    with damaged_parent_path.open("xb") as stream:
        stream.write(damaged_parent_bytes)
    assert run.run_dispatch_path is not None
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(
        (ROOT / run.run_dispatch_path).read_bytes()
    )
    damaged_proof = replace(
        proof,
        parent_receipt=NamedArtifactBinding(
            root_role="EXECUTION",
            relative_path=damaged_parent_path.relative_to(ROOT).as_posix(),
            sha256=_sha(damaged_parent_bytes),
            size_bytes=len(damaged_parent_bytes),
        ),
    )
    damaged_proof_path = run.parent_receipt_path.with_name("adversarial_postguard_dispatch.json")
    with damaged_proof_path.open("xb") as stream:
        stream.write(damaged_proof.canonical_bytes)
    failed_postguard_run = replace(
        run,
        parent_receipt=damaged_parent,
        parent_receipt_path=damaged_parent_path,
        run_dispatch_path=damaged_proof_path.relative_to(ROOT).as_posix(),
        run_dispatch_sha256=damaged_proof.canonical_sha256,
    )
    with pytest.raises(pytest.fail.Exception, match="ORIGINAL_POSTGUARD_OR_CHILD_MISMATCH"):
        dispatch_actual_candidate_child(
            fixture, operation="verify", successful_run=failed_postguard_run
        )
    with pytest.raises(pytest.fail.Exception, match="NAMED_PARENT_SUCCESSFUL_RUN_REQUIRED"):
        dispatch_actual_candidate_child(
            fixture,
            operation="verify",
            receipt_path=run.child_result["receipt_path"],
            receipt_sha256=run.child_result["receipt_sha256"],
        )
    assert _read_receipt(fixture, run) == receipt


def test_missing_explicit_parent_association_is_typed_failure_not_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = build_actual_candidate_fixture(tmp_path)
    monkeypatch.delenv(TRANSACTION_ENV, raising=False)
    monkeypatch.delenv(LEASE_ENV, raising=False)
    with pytest.raises(pytest.fail.Exception, match="NAMED_PARENT_ASSOCIATION_REQUIRED"):
        dispatch_actual_candidate_child(fixture)
    assert not _fixture_tree(fixture.evidence_root)
