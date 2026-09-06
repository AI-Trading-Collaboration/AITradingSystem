"""Pure synthetic contract tests; no actual Git provenance or canonical DQ claim.

The source-committed bootstrap end-to-end tests separately prove real loaded
code and positive verifier consumption.  This file deliberately uses the
TEST_ONLY context factory, which cannot mint a production capability.
"""

from __future__ import annotations

import ast
import hashlib
import json
import pickle
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from inspect import Parameter, signature
from pathlib import Path
from typing import cast

import pytest

from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_attribution import DataQualityCalendarBinding
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    DataQualityImplementationSourceBinding,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    canonical_json_value,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQInputBinding,
    NamedDQRoots,
    NamedDQScope,
    NamedDQSuccessfulDispatchBinding,
    NamedExecutionObservation,
    NamedManifestBinding,
    NamedManifestRowBinding,
    NamedPublicationBinding,
    NamedSnapshotSelector,
    VerifiedNamedInputs,
    _verified_named_inputs_from_receipt,
)
from ai_trading_system.contracts.named_execution_context import (
    GitCompiledModuleBinding,
    NamedExecutionContext,
    NamedExecutionContextError,
    NamedExecutionIdentity,
    _freeze_named_execution_context,
    _initialize_test_named_execution_context,
    close_named_execution_context,
    require_named_execution_context,
)
from ai_trading_system.contracts.status import PolicyRole

_SHA = "a" * 64
_OTHER_SHA = "b" * 64
_COMMIT = "1" * 40
_NOW = datetime(2026, 9, 5, 1, tzinfo=UTC)
_WINDOW = DataQualityDateWindow(date(2021, 2, 22), date(2026, 9, 3))
_TEST_FILE = "tests/test_named_data_quality_execution_contract.py"


def _identity() -> NamedExecutionIdentity:
    modules = (
        GitCompiledModuleBinding(
            "ai_trading_system", "src/ai_trading_system/__init__.py", _COMMIT, _SHA, 0, True
        ),
        GitCompiledModuleBinding(
            "ai_trading_system.stub", "src/ai_trading_system/stub.py", _COMMIT, _SHA, 1, False
        ),
    )
    return NamedExecutionIdentity(
        execution_root="D:/synthetic/execution",
        candidate_commit=_COMMIT,
        source_manifest_path="config/synthetic_source_manifest.json",
        source_manifest_sha256=_SHA,
        bootstrap_path="scripts/run_named_data_quality.py",
        bootstrap_sha256=_SHA,
        python_executable="D:/synthetic/python.exe",
        python_version="synthetic-contract-only",
        git_executable="D:/synthetic/git.exe",
        git_version="synthetic-contract-only",
        modules=modules,
    )


def _scope() -> NamedDQScope:
    return NamedDQScope(
        as_of=_WINDOW.end,
        requested_window=_WINDOW,
        expected_price_tickers=("QQQ", "SGOV", "TQQQ"),
        expected_rate_series=("DGS3MO",),
        input_roles=("prices", "rates"),
        require_secondary_prices=False,
    )


def _request() -> NamedDQExecutionRequest:
    identity = _identity()
    return NamedDQExecutionRequest(
        roots=NamedDQRoots(
            "D:/synthetic/source",
            "D:/synthetic/publication",
            identity.execution_root,
            "D:/synthetic/evidence",
        ),
        selector=NamedSnapshotSelector("pointer_one", _SHA, "transaction_one", _SHA),
        scope=_scope(),
        source_output_relative_path="download/export",
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        candidate_commit=identity.candidate_commit,
        source_manifest_path=identity.source_manifest_path,
        source_manifest_sha256=identity.source_manifest_sha256,
    )


def _artifact(
    path: str, *, root: str = "PUBLICATION", content: bytes | None = None
) -> NamedArtifactBinding:
    return NamedArtifactBinding(
        root,
        path,
        _SHA if content is None else hashlib.sha256(content).hexdigest(),
        1 if content is None else len(content),
    )


def _row(ordinal: int, role: str) -> NamedManifestRowBinding:
    # Empty cells, whitespace and embedded newlines remain raw strings.
    values = (
        ("output_path", f"D:/synthetic/source/download/export/{role}.csv"),
        ("provider", " synthetic \n provider "),
        ("optional", ""),
    )
    checksum = hashlib.sha256(canonical_json_value(dict(values)).encode("utf-8")).hexdigest()
    return NamedManifestRowBinding(ordinal, values, checksum, "manifest_record_" + checksum)


def _receipt() -> NamedDQExecutionReceipt:
    request = _request()
    rows = (_row(0, "prices"), _row(1, "rates"))
    manifest = NamedManifestBinding(
        _artifact("members/manifest.csv"), tuple(key for key, _ in rows[0].fields), rows
    )
    inputs = tuple(
        NamedDQInputBinding(
            role=role,
            schema_id="prices_daily.v1" if role == "prices" else "rates_daily.v1",
            source_role="primary_market_prices" if role == "prices" else "primary_macro_rates",
            member=_artifact(f"members/{role}.csv", content=role.encode()),
            row_count=1,
            observed_min_date=date(2018, 1, 2),
            observed_max_date=_WINDOW.end,
            manifest_row_ordinal=index,
            manifest_row_sha256=rows[index].row_sha256,
            original_output_path=dict(rows[index].fields)["output_path"],
            source_relative_path=f"download/export/{role}.csv",
        )
        for index, role in enumerate(("prices", "rates"))
    )
    calendar_policy = _artifact("config/calendar.yaml", root="EXECUTION")
    report = DataQualityReportBinding("named/reports/report.md", _SHA, 1, "PASS", 0, 0, 0)
    return NamedDQExecutionReceipt(
        run_id="synthetic_contract_only",
        contract_id="synthetic_dq",
        request=request,
        started_at=_NOW,
        checked_at=_NOW,
        ended_at=_NOW,
        evaluated_window=_WINDOW,
        publication=NamedPublicationBinding(
            dataset_id="download_composite",
            snapshot_id="snapshot_one",
            generation=1,
            pointer_id=request.selector.pointer_id,
            pointer=_artifact("pointer_history/download_composite/pointer_one.json"),
            transaction_id=request.selector.transaction_id,
            transaction=_artifact("transactions/one.json"),
            snapshot_manifest_id="manifest_one",
            snapshot_manifest=_artifact("manifests/one.json"),
            source_event_id="event_one",
            source_event=_artifact("source_events/one.json"),
            transaction_window=_WINDOW,
            anchor_dataset_id="download_composite",
            anchor_pointer_id=request.selector.pointer_id,
            anchor_generation=1,
            anchor_pointer=_artifact("current/download_composite.json"),
        ),
        manifest=manifest,
        inputs=inputs,
        policy=DataQualityPolicyBinding(
            "synthetic_policy",
            "1.0",
            "REVIEWED",
            "contract_test",
            PolicyRole.DATA_QUALITY,
            request.policy_path,
            _SHA,
        ),
        validator=DataQualityValidatorBinding(
            "synthetic_validator",
            "1.0",
            "ai_trading_system.stub:validate_data_cache",
            (DataQualityImplementationSourceBinding("src/ai_trading_system/stub.py", _SHA),),
        ),
        execution=_identity(),
        execution_observation=NamedExecutionObservation(1234, "synthetic_lease", _NOW, _NOW, _NOW),
        execution_dependencies=(_artifact(request.policy_path, root="EXECUTION"), calendar_policy),
        calendar=DataQualityCalendarBinding(
            "XNYS", "synthetic", "synthetic", _SHA, "synthetic_closures", "1", _SHA
        ),
        calendar_policy=calendar_policy,
        invocation=(DataQualityInvocationParameter.from_value("synthetic_contract_only", True),),
        report=report,
        data_quality_evidence=DataQualityEvidence(
            "synthetic_dq",
            "synthetic_policy",
            "1.0",
            "PASS",
            True,
            _NOW,
            _WINDOW.end,
            report.path,
            report.sha256,
            checked_input_count=2,
        ),
        price_consistency_start_date=_WINDOW.start,
        rate_consistency_start_date=_WINDOW.start,
    )


def _receipt_path(receipt: NamedDQExecutionReceipt) -> str:
    return f"named_data_quality/executions/{receipt.receipt_id}/receipt.json"


def _dispatch(receipt: NamedDQExecutionReceipt) -> NamedDQSuccessfulDispatchBinding:
    # Declaration fixture only: no child, parent guard or actual files exist.
    return NamedDQSuccessfulDispatchBinding(
        receipt=_artifact(_receipt_path(receipt), root="EVIDENCE", content=receipt.canonical_bytes),
        receipt_id=receipt.receipt_id,
        request_id=receipt.request.request_id,
        request_sha256=receipt.request.canonical_sha256,
        execution_identity_sha256=receipt.execution.stable_identity_sha256,
        candidate_commit=receipt.execution.candidate_commit,
        execution_root=receipt.execution.execution_root,
        execution_pid=receipt.execution_observation.execution_pid,
        source_lease_id=receipt.execution_observation.source_lease_id,
        child_started_at=_NOW - timedelta(seconds=1),
        child_terminal_checked_at=_NOW + timedelta(seconds=1),
        parent_postchecked_at=_NOW + timedelta(seconds=2),
        parent_receipt=_artifact(
            "outputs/validation_runtime/synthetic-parent-receipt.json",
            root="EXECUTION",
            content=b"synthetic contract declaration only",
        ),
    )


def test_named_request_and_receipt_round_trip_do_not_issue_capabilities() -> None:
    request, receipt = _request(), _receipt()
    assert NamedDQExecutionRequest.from_json_bytes(request.canonical_bytes) == request
    assert NamedDQExecutionReceipt.from_json_bytes(receipt.canonical_bytes) == receipt
    assert request.canonical_sha256 == hashlib.sha256(request.canonical_bytes).hexdigest()
    assert receipt.canonical_sha256 == hashlib.sha256(receipt.canonical_bytes).hexdigest()
    with pytest.raises(ValueError, match="not canonical"):
        NamedDQExecutionReceipt.from_json_bytes(receipt.canonical_bytes + b"\n")
    assert receipt.data_quality_evidence.checked_input_count == 2
    assert receipt.inputs[0].observed_min_date == date(2018, 1, 2)
    assert receipt.request.scope.requested_window.start == date(2021, 2, 22)
    assert not isinstance(receipt, VerifiedNamedInputs)
    with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_REQUIRED"):
        _verified_named_inputs_from_receipt(
            receipt,
            receipt_path=_receipt_path(receipt),
            successful_dispatch=_dispatch(receipt),
            captured_inputs=(("prices", b"prices"), ("rates", b"rates")),
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "data_quality_execution_receipt.v1"),
        ("receipt_id", "wrong"),
        ("already_verified", True),
        ("dispatch_allowed", True),
        ("production_effect", "paper"),
    ],
)
def test_receipt_rejects_old_schema_tamper_and_authority_fields(field: str, value: object) -> None:
    payload = _receipt().to_dict()
    payload[field] = value
    with pytest.raises(ValueError):
        NamedDQExecutionReceipt.from_dict(payload)


@pytest.mark.parametrize(
    "content", [b'{"schema_version":"x","schema_version":"y"}', b'{"x":NaN}', b'{"x":Infinity}']
)
def test_json_duplicate_and_nonfinite_fields_are_rejected(content: bytes) -> None:
    with pytest.raises(ValueError):
        NamedDQExecutionRequest.from_json_bytes(content)


@pytest.mark.parametrize(
    "path",
    [
        "../output",
        "data/../output",
        "/absolute",
        "D:/absolute",
        "data\\raw",
        "data//raw",
        "data/./raw",
    ],
)
def test_request_requires_explicit_normalized_source_output(path: str) -> None:
    with pytest.raises(ValueError):
        replace(_request(), source_output_relative_path=path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_root", "relative"),
        ("publication_root", "D:/a/../b"),
        ("execution_root", "D:\\a"),
        ("evidence_root", "D:/a/"),
    ],
)
def test_four_roots_have_explicit_normalized_identity(field: str, value: str) -> None:
    with pytest.raises(ValueError):
        replace(_request().roots, **{field: value})


def test_scope_requires_exact_asof_and_never_promotes_full_file_history() -> None:
    current = _scope()
    assert current.covers(
        replace(
            current,
            requested_window=DataQualityDateWindow(date(2022, 1, 1), _WINDOW.end),
            expected_price_tickers=("QQQ",),
        )
    )
    assert not current.covers(
        replace(current, requested_window=DataQualityDateWindow(date(2018, 1, 2), _WINDOW.end))
    )
    assert not current.covers(replace(current, as_of=date(2026, 9, 4)))
    assert not current.covers(replace(current, expected_price_tickers=("UNREVIEWED",)))
    with pytest.raises(ValueError):
        replace(current, input_roles=("prices", "rates", "rates"))
    with pytest.raises(ValueError):
        replace(current, require_secondary_prices=True)
    with pytest.raises(ValueError):
        replace(current, as_of=cast(date, "2026-09-03"))
    with pytest.raises(ValueError):
        replace(current, require_secondary_prices=cast(bool, 1))


def test_full_manifest_raw_cells_and_ordinal_identity_survive_round_trip() -> None:
    manifest = _receipt().manifest
    assert NamedManifestBinding.from_dict(manifest.to_dict()) == manifest
    assert dict(manifest.rows[0].fields)["provider"] == " synthetic \n provider "
    assert dict(manifest.rows[0].fields)["optional"] == ""
    with pytest.raises(ValueError):
        replace(manifest.rows[0], fields=manifest.rows[0].fields[:-1])
    with pytest.raises(ValueError):
        replace(manifest, rows=(manifest.rows[1], manifest.rows[0]))
    with pytest.raises(ValueError):
        replace(manifest, rows=(manifest.rows[0], replace(manifest.rows[0], source_ordinal=1)))


def test_named_snapshot_can_be_historical_member_of_newer_observed_anchor() -> None:
    selected = _receipt().publication
    later = replace(
        selected,
        anchor_generation=2,
        anchor_pointer_id="pointer_two",
        anchor_pointer=replace(selected.anchor_pointer, sha256=_OTHER_SHA),
    )
    assert later.generation == 1 and later.anchor_generation == 2
    with pytest.raises(ValueError):
        replace(selected, anchor_generation=2)
    with pytest.raises(ValueError):
        replace(selected, anchor_generation=0)


def test_receipt_rejects_cross_bound_identity_and_counts() -> None:
    receipt = _receipt()
    with pytest.raises(ValueError):
        replace(receipt, inputs=(receipt.inputs[0], receipt.inputs[0]))
    with pytest.raises(ValueError):
        replace(
            receipt,
            inputs=(replace(receipt.inputs[0], original_output_path="other"), receipt.inputs[1]),
        )
    with pytest.raises(ValueError):
        replace(
            receipt,
            inputs=(
                replace(receipt.inputs[0], source_relative_path="other/prices.csv"),
                receipt.inputs[1],
            ),
        )
    with pytest.raises(ValueError):
        replace(
            receipt,
            data_quality_evidence=replace(receipt.data_quality_evidence, checked_input_count=3),
        )
    with pytest.raises(ValueError):
        replace(receipt, execution=replace(receipt.execution, execution_root="D:/copied/source"))
    with pytest.raises(ValueError):
        replace(receipt, execution_dependencies=receipt.execution_dependencies[:1])
    with pytest.raises(ValueError):
        replace(
            receipt, calendar=replace(receipt.calendar, special_closure_policy_sha256=_OTHER_SHA)
        )


def test_execution_identity_requires_real_package_shape_and_exact_schema() -> None:
    identity = _identity()
    assert NamedExecutionIdentity.from_dict(identity.to_dict()) == identity
    assert (
        replace(identity, candidate_commit="2" * 40).stable_identity_sha256
        != identity.stable_identity_sha256
    )
    with pytest.raises(ValueError):
        replace(identity, modules=identity.modules[1:])
    with pytest.raises(ValueError):
        replace(identity, modules=identity.modules + (identity.modules[0],))
    with pytest.raises(ValueError):
        replace(identity.modules[1], source_path="src/ai_trading_system/other.py")
    with pytest.raises(ValueError):
        NamedExecutionIdentity.from_dict({**identity.to_dict(), "already_verified": True})


def test_test_only_context_never_asserts_real_provenance_or_mints_seal() -> None:
    receipt = _receipt()
    handle = _initialize_test_named_execution_context(receipt.execution)
    try:
        receipt = replace(
            receipt,
            execution_observation=replace(
                receipt.execution_observation, execution_pid=handle.process_id + 1
            ),
        )
        assert handle.provenance_kind == "SYNTHETIC_CONTRACT_TEST_ONLY"
        assert handle.process_id != receipt.execution_observation.execution_pid
        _freeze_named_execution_context(handle, loaded_modules=receipt.execution.modules)
        with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_NOT_PROVEN"):
            require_named_execution_context()
        with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_NOT_PROVEN"):
            _verified_named_inputs_from_receipt(
                receipt,
                receipt_path=_receipt_path(receipt),
                successful_dispatch=_dispatch(receipt),
                captured_inputs=(("prices", b"prices"), ("rates", b"rates")),
            )
        with pytest.raises(TypeError, match="cannot be serialized"):
            pickle.dumps(handle)
        with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_ALREADY_ACTIVE"):
            _initialize_test_named_execution_context(receipt.execution)
    finally:
        close_named_execution_context(handle)
    with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_REQUIRED"):
        require_named_execution_context()


def test_context_exact_loaded_set_and_foreign_pid_are_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = _identity()
    handle = _initialize_test_named_execution_context(identity)
    try:
        with pytest.raises(NamedExecutionContextError, match="NAMED_LOADED_SET_MISMATCH"):
            _freeze_named_execution_context(handle, loaded_modules=identity.modules[:-1])
        _freeze_named_execution_context(handle, loaded_modules=identity.modules)
        with monkeypatch.context() as changed:
            changed.setattr(
                "ai_trading_system.contracts.named_execution_context.os.getpid",
                lambda: handle.process_id + 1,
            )
            with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_PROCESS_MISMATCH"):
                _freeze_named_execution_context(handle, loaded_modules=identity.modules)
            with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_PROCESS_MISMATCH"):
                close_named_execution_context(handle)
    finally:
        close_named_execution_context(handle)
    replacement = _initialize_test_named_execution_context(identity)
    try:
        with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_PROCESS_MISMATCH"):
            _freeze_named_execution_context(handle, loaded_modules=identity.modules)
    finally:
        close_named_execution_context(replacement)


def test_direct_context_and_verified_constructor_cannot_accept_forged_seal() -> None:
    with pytest.raises(NamedExecutionContextError, match="NAMED_CONTEXT_SEAL_REQUIRED"):
        NamedExecutionContext(_identity(), _seal=object(), test_only=False)
    with pytest.raises(ValueError, match="verifier seal"):
        receipt = _receipt()
        VerifiedNamedInputs(
            receipt,
            (),
            receipt_path=_receipt_path(receipt),
            successful_dispatch=_dispatch(receipt),
            _seal=object(),
        )
    assert not hasattr(VerifiedNamedInputs, "from_dict")
    assert not hasattr(VerifiedNamedInputs, "from_json_bytes")


def test_verified_serialization_is_unconditionally_disabled() -> None:
    # An uninitialized object is NOT a capability; exercising only the class's
    # unconditional serialization rejection does not assert synthetic provenance.
    uninitialized = object.__new__(VerifiedNamedInputs)
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(uninitialized)
    with pytest.raises(TypeError, match="cannot be serialized"):
        uninitialized.__reduce__()


@pytest.mark.parametrize("status", ["PASS_WITH_WARNINGS", "FAIL"])
def test_non_strict_outcomes_remain_receipts_not_verified_inputs(status: str) -> None:
    original = _receipt()
    errors, warnings = (1, 0) if status == "FAIL" else (0, 1)
    issues = ("SYNTHETIC_ISSUE",)
    blockers = issues if errors else ()
    report = replace(
        original.report,
        status=status,
        error_count=errors,
        warning_count=warnings,
        issue_codes=issues,
        blocking_issue_codes=blockers,
    )
    evidence = replace(
        original.data_quality_evidence,
        status=status,
        passed=not errors,
        error_count=errors,
        warning_count=warnings,
        blocking_issues=blockers,
    )
    receipt = replace(original, report=report, data_quality_evidence=evidence)
    assert NamedDQExecutionReceipt.from_json_bytes(receipt.canonical_bytes) == receipt
    _dispatch(receipt).assert_matches_receipt(receipt, receipt_path=_receipt_path(receipt))
    assert receipt.report.status != "PASS"
    assert not isinstance(receipt, VerifiedNamedInputs)


def test_successful_dispatch_is_independent_strict_dto_not_lease_or_capability() -> None:
    receipt = _receipt()
    original_bytes = receipt.canonical_bytes
    binding = _dispatch(receipt)
    restored = NamedDQSuccessfulDispatchBinding.from_json_bytes(binding.canonical_bytes)
    assert restored == binding
    assert binding.canonical_sha256 == hashlib.sha256(binding.canonical_bytes).hexdigest()
    restored.assert_matches_receipt(receipt, receipt_path=_receipt_path(receipt))
    assert receipt.canonical_bytes == original_bytes
    assert "successful_dispatch" not in receipt.to_dict()
    assert "source_lease_id" not in receipt.request.to_dict()
    assert "parent_receipt" not in receipt.request.to_dict()
    assert binding.proof_semantics == "TRUSTED_COORDINATOR_CORRELATION_ONLY"
    assert not binding.dispatch_allowed
    assert not isinstance(binding, (NamedExecutionContext, VerifiedNamedInputs))
    with pytest.raises(ValueError, match="not canonical"):
        NamedDQSuccessfulDispatchBinding.from_json_bytes(binding.canonical_bytes + b"\n")


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "data_quality_execution_receipt.v1"),
        ("dispatch_binding_id", "forged"),
        ("already_verified", True),
        ("child_exit_code", 1),
        ("child_exit_code", False),
        ("postguard_status", "FAIL"),
        ("proof_semantics", "SIGNED_LEASE_AUTHORITY"),
        ("dispatch_allowed", True),
        ("production_effect", "paper"),
        ("broker_action", "order"),
    ],
)
def test_dispatch_rejects_failed_terminal_and_authority_claims(field: str, value: object) -> None:
    payload = _dispatch(_receipt()).to_dict()
    payload[field] = value
    with pytest.raises(ValueError):
        NamedDQSuccessfulDispatchBinding.from_dict(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("receipt_id", "other_receipt"),
        ("request_id", "other_request"),
        ("request_sha256", _OTHER_SHA),
        ("execution_identity_sha256", _OTHER_SHA),
        ("candidate_commit", "2" * 40),
        ("execution_root", "D:/synthetic/copied-root"),
        ("execution_pid", 5678),
        ("source_lease_id", "other_active_verifier_lease"),
    ],
)
def test_dispatch_must_match_original_identity_not_current_verifier(
    field: str, value: object
) -> None:
    receipt = _receipt()
    payload = _dispatch(receipt).to_dict()
    payload.pop("dispatch_binding_id")
    payload[field] = value
    payload["dispatch_binding_id"] = (
        "named_dq_dispatch_"
        + hashlib.sha256(canonical_json_value(payload).encode("utf-8")).hexdigest()
    )
    binding = NamedDQSuccessfulDispatchBinding.from_dict(payload)
    with pytest.raises(ValueError, match="does not match"):
        binding.assert_matches_receipt(receipt, receipt_path=_receipt_path(receipt))


def test_dispatch_artifact_root_locator_hash_and_size_are_bound() -> None:
    receipt = _receipt()
    binding = _dispatch(receipt)
    for artifact in (
        replace(binding.receipt, relative_path="different/receipt.json"),
        replace(binding.receipt, sha256=_OTHER_SHA),
        replace(binding.receipt, size_bytes=binding.receipt.size_bytes + 1),
    ):
        with pytest.raises(ValueError, match="does not match"):
            replace(binding, receipt=artifact).assert_matches_receipt(
                receipt, receipt_path=_receipt_path(receipt)
            )
    with pytest.raises(ValueError):
        replace(binding, receipt=replace(binding.receipt, root_role="EXECUTION"))
    with pytest.raises(ValueError):
        replace(binding, parent_receipt=replace(binding.parent_receipt, root_role="EVIDENCE"))
    with pytest.raises(ValueError):
        binding.assert_matches_receipt(receipt, receipt_path="../receipt.json")


def test_dispatch_encloses_receipt_and_final_child_check_before_parent_postguard() -> None:
    receipt = _receipt()
    binding = _dispatch(receipt)
    for changed in (
        replace(binding, child_started_at=_NOW + timedelta(microseconds=1)),
        replace(binding, child_terminal_checked_at=_NOW - timedelta(microseconds=1)),
    ):
        with pytest.raises(ValueError, match="does not enclose"):
            changed.assert_matches_receipt(receipt, receipt_path=_receipt_path(receipt))
    with pytest.raises(ValueError, match="terminal chronology"):
        replace(
            binding,
            parent_postchecked_at=binding.child_terminal_checked_at - timedelta(microseconds=1),
        )


def test_seal_factory_rejects_mismatched_dispatch_before_context_access() -> None:
    receipt = _receipt()
    parameters = signature(_verified_named_inputs_from_receipt).parameters
    assert parameters["successful_dispatch"].default is Parameter.empty
    assert parameters["receipt_path"].default is Parameter.empty
    with pytest.raises(ValueError, match="does not match"):
        _verified_named_inputs_from_receipt(
            receipt,
            receipt_path="unbound/receipt.json",
            successful_dispatch=_dispatch(receipt),
            captured_inputs=(("prices", b"prices"), ("rates", b"rates")),
        )


def test_new_private_factory_imports_and_calls_have_narrow_owner_allowlists() -> None:
    root = Path(__file__).resolve().parents[1]
    context_owner = "src/ai_trading_system/contracts/named_execution_context.py"
    seal_owner = "src/ai_trading_system/contracts/named_data_quality_execution.py"
    allowed = {
        "_initialize_named_execution_context": {context_owner, "scripts/run_named_data_quality.py"},
        "_initialize_test_named_execution_context": {context_owner, _TEST_FILE},
        "_initialize_context": {context_owner},
        "_CONTEXT_SEAL": {context_owner},
        "_verified_named_inputs_from_receipt": {
            seal_owner,
            "src/ai_trading_system/data/named_quality_execution.py",
            _TEST_FILE,
        },
        "_VERIFIED_NAMED_SEAL": {seal_owner},
    }
    for folder in ("src", "scripts", "tests"):
        for path in (root / folder).rglob("*.py"):
            relative = path.relative_to(root).as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=relative)
            for node in ast.walk(tree):
                names: set[str] = set()
                if isinstance(node, ast.ImportFrom):
                    names = {alias.name for alias in node.names}
                    if node.module in {
                        "ai_trading_system.contracts.named_execution_context",
                        "ai_trading_system.contracts.named_data_quality_execution",
                    }:
                        assert "*" not in names, f"{relative}: wildcard private-boundary import"
                elif isinstance(node, ast.Name):
                    names = {node.id}
                elif isinstance(node, ast.Attribute):
                    names = {node.attr}
                elif (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "getattr"
                    and len(node.args) > 1
                    and isinstance(node.args[1], ast.Constant)
                    and isinstance(node.args[1].value, str)
                ):
                    names = {node.args[1].value}
                for name in names & allowed.keys():
                    assert relative in allowed[name], f"{relative}: unauthorized {name} reference"


def test_nested_unknown_field_and_noncanonical_types_are_not_dropped() -> None:
    request = _request()
    payload = json.loads(request.canonical_bytes)
    payload["scope"]["allow_training_history"] = True
    with pytest.raises(ValueError):
        NamedDQExecutionRequest.from_dict(payload)
    payload = json.loads(request.canonical_bytes)
    payload["scope"]["require_secondary_prices"] = 1
    with pytest.raises(ValueError):
        NamedDQExecutionRequest.from_dict(payload)
