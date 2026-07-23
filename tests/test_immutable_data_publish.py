from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from ai_trading_system.contracts import ArtifactEnvelope, ArtifactPointer, DataQualityEvidence
from ai_trading_system.data import immutable_publish as publish_module
from ai_trading_system.data.immutable_publish import (
    CONSUMER_CUTOVER_LIMITATION,
    CRASH_DURABILITY_VERIFIED_LIMITATION,
    DATA_QUALITY_REPORT_SCHEMA_VERSION,
    DQ_EXECUTION_PROVENANCE_LIMITATION,
    FILESYSTEM_SECURITY_PROFILE,
    FILESYSTEM_SECURITY_PROFILE_LIMITATION,
    SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION,
    SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE,
    SAME_PRINCIPAL_POST_ACK_LIMITATION,
    STORE_ACL_VERIFIED_LIMITATION,
    TRUSTED_WRITER_PRINCIPAL_LIMITATION,
    CurrentPointerPrecondition,
    DataPublicationConflictError,
    DataPublicationError,
    DataPublicationIntegrityError,
    SnapshotPublishRequest,
    SourceEventProvenance,
    publish_immutable_snapshot,
    validate_current_snapshot,
)
from ai_trading_system.platform.artifacts import (
    ArtifactWriteError,
    canonical_json_bytes,
    sha256_bytes,
    sha256_path,
)

AS_OF = date(2026, 7, 22)
COVERAGE_START = date(2021, 2, 22)
GENERATED_AT = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
DATASET_ID = "validated_prices"
PAYLOAD_TYPE = "csv"
PAYLOAD_SCHEMA = "validated_prices.v1"


def test_publish_stages_separate_immutable_evidence_then_moves_current(tmp_path: Path) -> None:
    payload = b"date,ticker,close\n2026-07-22,QQQ,555.00\n"
    request, external_report = _case(
        tmp_path,
        payload,
        run_id="run-001",
        generated_at=GENERATED_AT,
    )

    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
        current_precondition=CurrentPointerPrecondition(expected_sha256=None),
    )
    current = validate_current_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )

    assert result.current_pointer_changed is True
    assert current == result.snapshot
    assert current.generation == 1
    assert current.payload_path.read_bytes().startswith(b"date,ticker")
    assert current.payload_path.parent.parent.name == DATASET_ID
    assert current.source_event_path.parent.name == DATASET_ID
    assert current.manifest_path.parent.name == DATASET_ID
    assert current.source_event_path != current.manifest_path
    source_event = json.loads(current.source_event_path.read_text(encoding="utf-8"))
    manifest = json.loads(current.manifest_path.read_text(encoding="utf-8"))
    assert source_event["run_id"] == manifest["run_id"] == "run-001"
    assert source_event["snapshot"]["sha256"] == sha256_path(current.payload_path)
    assert manifest["artifact_envelope"]["envelope_id"] == current.envelope.envelope_id
    assert manifest["filesystem_security_profile"] == FILESYSTEM_SECURITY_PROFILE
    assert manifest["trusted_writer_principal_required"] is True
    assert (
        manifest["same_principal_adversarial_mutation_resistance"]
        == SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
    )
    assert manifest["store_acl_verified"] is False
    assert manifest["consumer_cutover_allowed"] is False
    assert manifest["crash_durability_verified"] is False
    assert manifest["same_principal_post_ack_mutation_protection"] is False
    assert result.filesystem_security_profile == FILESYSTEM_SECURITY_PROFILE
    assert result.trusted_writer_principal_required is True
    assert (
        result.same_principal_adversarial_mutation_resistance
        == SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
    )
    assert result.store_acl_verified is False
    assert result.crash_durability_verified is False
    assert result.same_principal_post_ack_mutation_protection is False
    assert FILESYSTEM_SECURITY_PROFILE_LIMITATION in current.envelope.limitations
    assert TRUSTED_WRITER_PRINCIPAL_LIMITATION in current.envelope.limitations
    assert SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION in current.envelope.limitations
    assert STORE_ACL_VERIFIED_LIMITATION in current.envelope.limitations
    assert CRASH_DURABILITY_VERIFIED_LIMITATION in current.envelope.limitations
    assert SAME_PRINCIPAL_POST_ACK_LIMITATION in current.envelope.limitations
    quality = current.envelope.data_quality
    assert quality is not None
    assert manifest["quality_binding"]["data_quality_evidence_id"] == quality.evidence_id
    assert quality.report_path is not None
    assert quality.report_path.startswith("quality_reports/validated_prices/")
    frozen_report = tmp_path / "store" / Path(*PurePosixPath(quality.report_path).parts)
    assert frozen_report.read_bytes() == external_report.read_bytes()
    history = tmp_path / "store" / "pointer_history" / DATASET_ID / f"{current.pointer_id}.json"
    lock = tmp_path / "store" / "locks" / f"{DATASET_ID}.lock"
    for immutable_file in (
        current.payload_path,
        current.source_event_path,
        current.manifest_path,
        frozen_report,
        history,
        current.pointer_path,
        lock,
    ):
        assert immutable_file.stat().st_nlink == 1
    assert list((tmp_path / "store" / "staging").rglob("*")) == [
        tmp_path / "store" / "staging" / DATASET_ID
    ]


def test_pre_commit_validator_runs_after_snapshot_validation_before_pointer_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"date,ticker,close\n2026-07-22,QQQ,555.00\n"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-pre-commit-order",
        generated_at=GENERATED_AT,
    )
    current_path = tmp_path / "store/current" / f"{DATASET_ID}.json"
    events: list[str] = []
    original_validate = publish_module._validated_snapshot
    original_commit = publish_module._commit_current_atomic

    def record_validation(*args: Any, **kwargs: Any):
        result = original_validate(*args, **kwargs)
        events.append("validated")
        return result

    def record_commit(*args: Any, **kwargs: Any):
        events.append("commit")
        return original_commit(*args, **kwargs)

    def validate_external_precondition() -> None:
        assert events == ["validated"]
        assert not current_path.exists()
        events.append("pre_commit_validator")

    monkeypatch.setattr(publish_module, "_validated_snapshot", record_validation)
    monkeypatch.setattr(publish_module, "_commit_current_atomic", record_commit)

    publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
        pre_commit_validator=validate_external_precondition,
    )

    assert events == ["validated", "pre_commit_validator", "commit"]
    assert current_path.is_file()


def test_pre_commit_validator_failure_keeps_current_pointer_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"date,ticker,close\n2026-07-22,QQQ,555.00\n"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-pre-commit-rejected",
        generated_at=GENERATED_AT,
    )
    commit_calls = 0
    original_commit = publish_module._commit_current_atomic

    def record_commit(*args: Any, **kwargs: Any):
        nonlocal commit_calls
        commit_calls += 1
        return original_commit(*args, **kwargs)

    def reject_candidate() -> None:
        raise RuntimeError("external precondition changed")

    monkeypatch.setattr(publish_module, "_commit_current_atomic", record_commit)

    with pytest.raises(RuntimeError, match="external precondition changed"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
            pre_commit_validator=reject_candidate,
        )

    assert commit_calls == 0
    assert not (tmp_path / "store/current" / f"{DATASET_ID}.json").exists()


@pytest.mark.parametrize(
    ("report_overrides", "match"),
    (
        ({"schema_version": "unreviewed.v1"}, "SCHEMA_VERSION_UNSUPPORTED"),
        ({"policy_version": "unreviewed.v2"}, "DQ_REPORT_EVIDENCE_MISMATCH"),
        ({"coverage_start": "2022-12-01"}, "DQ_REPORT_COVERAGE_MISMATCH"),
        ({"status": "FAIL", "passed": False}, "DQ_REPORT_EVIDENCE_MISMATCH"),
        ({"checked_input_count": 2}, "DQ_REPORT_EVIDENCE_MISMATCH"),
    ),
)
def test_strict_dq_report_rejects_schema_window_status_and_input_count(
    tmp_path: Path,
    report_overrides: Mapping[str, object],
    match: str,
) -> None:
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-invalid-report",
        generated_at=GENERATED_AT,
        report_overrides=report_overrides,
    )

    with pytest.raises(DataPublicationIntegrityError, match=match):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )
    assert not (tmp_path / "store" / "current" / f"{DATASET_ID}.json").exists()


def test_strict_dq_report_rejects_wrong_candidate_payload(tmp_path: Path) -> None:
    request, _ = _case(
        tmp_path,
        b"payload-that-was-checked",
        run_id="run-wrong-payload",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationIntegrityError, match="DQ_REPORT_SNAPSHOT_MISMATCH"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=b"different-payload",
        )


def test_strict_dq_report_rejects_wrong_candidate_schema(tmp_path: Path) -> None:
    payload = b"schema-bound-payload"
    wrong_snapshot = _snapshot_pointer(payload, schema_version="validated_prices.v0")
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-wrong-candidate-schema",
        generated_at=GENERATED_AT,
        report_overrides={"evaluated_snapshot": wrong_snapshot.to_dict()},
    )

    with pytest.raises(DataPublicationIntegrityError, match="DQ_REPORT_SNAPSHOT_MISMATCH"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )


def test_strict_dq_report_rejects_caller_only_pass_claim(tmp_path: Path) -> None:
    report = tmp_path / "dq" / "caller-claim.json"
    report.parent.mkdir(parents=True)
    report.write_bytes(canonical_json_bytes({"status": "PASS"}))
    request = _request(
        tmp_path,
        report.relative_to(tmp_path),
        run_id="run-caller-claim",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationIntegrityError, match="SCHEMA_FIELDS_INVALID"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=b"unvalidated",
        )


def test_external_dq_report_can_be_deleted_after_publish(tmp_path: Path) -> None:
    payload = b"trusted"
    request, report = _case(
        tmp_path,
        payload,
        run_id="run-frozen-dq",
        generated_at=GENERATED_AT,
    )
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )
    report.unlink()

    current = validate_current_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path / "evidence-no-longer-present",
        dataset_id=DATASET_ID,
    )
    assert current.pointer_sha256 == result.snapshot.pointer_sha256


def test_dq_checksum_failure_keeps_previous_current_pointer(tmp_path: Path) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"first",
        run_id="run-001",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"first",
    )
    pointer_before = first.snapshot.pointer_path.read_bytes()
    second_request, second_report = _case(
        tmp_path,
        b"second",
        run_id="run-002",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    second_report.write_bytes(b"tampered")

    with pytest.raises(DataPublicationIntegrityError, match="DQ_REPORT_INVALID"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=second_request,
            payload=b"second",
        )

    assert first.snapshot.pointer_path.read_bytes() == pointer_before
    assert (
        validate_current_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        ).pointer_sha256
        == first.snapshot.pointer_sha256
    )


def test_atomic_pointer_write_failure_preserves_previous_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"first",
        run_id="run-001",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"first",
    )
    pointer_before = first.snapshot.pointer_path.read_bytes()
    second_request, _ = _case(
        tmp_path,
        b"second",
        run_id="run-002",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    original = publish_module._replace_bound_temporary

    def fail_current_write(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        if target.parent.name == "current":
            raise ArtifactWriteError("ATOMIC_ARTIFACT_WRITE_FAILED", target, "injected")
        original(binding, temporary_name, target, source_descriptor, **kwargs)

    monkeypatch.setattr(publish_module, "_replace_bound_temporary", fail_current_write)
    with pytest.raises(ArtifactWriteError, match="ATOMIC_ARTIFACT_WRITE_FAILED"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=second_request,
            payload=b"second",
        )

    assert first.snapshot.pointer_path.read_bytes() == pointer_before


def test_publish_has_no_fallible_post_commit_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"single-commit"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-single-commit",
        generated_at=GENERATED_AT,
    )

    def forbidden_public_post_commit_validation(**_kwargs: object) -> object:
        raise AssertionError("public validator must not run after the current replace")

    original_replace = publish_module._replace_bound_temporary
    original_verify = publish_module._verify_raw
    current_replaced = False

    def observe_current_replace(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal current_replaced
        original_replace(binding, temporary_name, target, source_descriptor, **kwargs)
        if target.parent.name == "current":
            current_replaced = True

    def forbid_filesystem_check_after_commit(*args: Any, **kwargs: Any) -> None:
        if current_replaced:
            raise AssertionError("filesystem validation ran after the current replace")
        original_verify(*args, **kwargs)

    monkeypatch.setattr(
        publish_module,
        "validate_current_snapshot",
        forbidden_public_post_commit_validation,
    )
    monkeypatch.setattr(publish_module, "_replace_bound_temporary", observe_current_replace)
    monkeypatch.setattr(publish_module, "_verify_raw", forbid_filesystem_check_after_commit)
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )
    assert current_replaced is True
    assert result.snapshot.pointer_path.is_file()


def test_unlock_failure_after_commit_returns_warning_and_valid_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"committed-before-unlock"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-unlock-warning",
        generated_at=GENERATED_AT,
    )

    def fail_unlock(_handle: object) -> None:
        raise OSError("injected unlock failure")

    monkeypatch.setattr(publish_module, "_unlock", fail_unlock)
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )

    assert result.current_pointer_changed is True
    assert result.post_commit_cleanup_status == "PASS_WITH_WARNINGS"
    assert len(result.post_commit_cleanup_warnings) == 1
    assert "DATASET_UNLOCK_FAILED" in result.post_commit_cleanup_warnings[0]
    assert result.dq_execution_provenance_verified is False
    assert result.consumer_cutover_allowed is False
    current = validate_current_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )
    assert current.pointer_sha256 == result.snapshot.pointer_sha256


def test_stale_cas_cannot_replace_current(tmp_path: Path) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"first",
        run_id="run-001",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"first",
    )
    second_request, _ = _case(
        tmp_path,
        b"second",
        run_id="run-002",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    second = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=second_request,
        payload=b"second",
        current_precondition=CurrentPointerPrecondition(
            expected_sha256=first.snapshot.pointer_sha256
        ),
    )
    pointer_before = second.snapshot.pointer_path.read_bytes()
    third_request, _ = _case(
        tmp_path,
        b"third",
        run_id="run-003",
        generated_at=GENERATED_AT + timedelta(minutes=2),
    )

    with pytest.raises(DataPublicationConflictError, match="CURRENT_POINTER_CAS_MISMATCH"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=third_request,
            payload=b"third",
            current_precondition=CurrentPointerPrecondition(
                expected_sha256=first.snapshot.pointer_sha256
            ),
        )

    assert second.snapshot.pointer_path.read_bytes() == pointer_before


@pytest.mark.parametrize(
    ("stale_as_of", "stale_generated_at", "match"),
    (
        (
            AS_OF - timedelta(days=1),
            GENERATED_AT + timedelta(minutes=2),
            "CURRENT_AS_OF_REGRESSION",
        ),
        (AS_OF, GENERATED_AT - timedelta(minutes=1), "CURRENT_GENERATED_AT_REGRESSION"),
    ),
)
def test_monotonic_gate_rejects_stale_as_of_and_generated_at(
    tmp_path: Path,
    stale_as_of: date,
    stale_generated_at: datetime,
    match: str,
) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"current",
        run_id="run-current",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"current",
    )
    pointer_before = first.snapshot.pointer_path.read_bytes()
    stale_request, _ = _case(
        tmp_path,
        b"stale",
        run_id=f"run-stale-{match}",
        generated_at=stale_generated_at,
        as_of=stale_as_of,
        coverage_end=stale_as_of,
    )

    with pytest.raises(DataPublicationConflictError, match=match):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=stale_request,
            payload=b"stale",
        )
    assert first.snapshot.pointer_path.read_bytes() == pointer_before


def test_reverse_concurrency_keeps_latest_candidate_current(tmp_path: Path) -> None:
    store = tmp_path / "store"
    cases = {
        index: _case(
            tmp_path,
            f"payload-{index}".encode(),
            run_id=f"run-{index:03d}",
            generated_at=GENERATED_AT + timedelta(minutes=index),
        )[0]
        for index in range(1, 5)
    }

    def publish(index: int) -> str:
        # Submit newest first and delay progressively older candidates. The lock
        # must serialize them, while the monotonic gate rejects stale followers.
        time.sleep((4 - index) * 0.02)
        try:
            publish_immutable_snapshot(
                store_root=store,
                evidence_root=tmp_path,
                request=cases[index],
                payload=f"payload-{index}".encode(),
            )
        except DataPublicationConflictError:
            return "CONFLICT"
        return "PUBLISHED"

    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = tuple(pool.map(publish, (4, 3, 2, 1)))

    current = validate_current_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )
    assert outcomes[0] == "PUBLISHED"
    assert outcomes.count("PUBLISHED") == 1
    assert current.run_id == "run-004"
    assert current.envelope.generated_at == GENERATED_AT + timedelta(minutes=4)
    assert current.payload_path.read_bytes() == b"payload-4"


def test_full_history_chain_detects_oldest_generation_tamper(tmp_path: Path) -> None:
    store = tmp_path / "store"
    publications = []
    for index in range(1, 4):
        payload = f"payload-{index}".encode()
        request, _ = _case(
            tmp_path,
            payload,
            run_id=f"run-{index:03d}",
            generated_at=GENERATED_AT + timedelta(minutes=index),
        )
        publications.append(
            publish_immutable_snapshot(
                store_root=store,
                evidence_root=tmp_path,
                request=request,
                payload=payload,
            )
        )
    oldest_id = publications[0].snapshot.pointer_id
    oldest_history = store / "pointer_history" / DATASET_ID / f"{oldest_id}.json"
    oldest_history.write_bytes(b"{}\n")

    with pytest.raises(DataPublicationIntegrityError):
        validate_current_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        )


def test_validator_fails_closed_after_immutable_payload_tamper(tmp_path: Path) -> None:
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-001",
        generated_at=GENERATED_AT,
    )
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )
    result.snapshot.payload_path.write_bytes(b"tampered")

    with pytest.raises(DataPublicationIntegrityError, match="SNAPSHOT_PAYLOAD_INVALID"):
        validate_current_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        )


def test_hard_link_install_never_overwrites_existing_content(tmp_path: Path) -> None:
    payload = b"trusted"
    store = tmp_path / "store"
    target = store / Path(*PurePosixPath(_snapshot_pointer(payload).path).parts)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"preexisting-conflict")
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-hard-link-conflict",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationConflictError, match="IMMUTABLE_OBJECT_CONFLICT"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )
    assert target.read_bytes() == b"preexisting-conflict"
    assert not (store / "current" / f"{DATASET_ID}.json").exists()


def test_matching_content_snapshot_hardlink_is_rejected(tmp_path: Path) -> None:
    payload = b"matching-but-externally-linked"
    store = tmp_path / "store"
    external = tmp_path / "external-payload.csv"
    external.write_bytes(payload)
    target = store / Path(*PurePosixPath(_snapshot_pointer(payload).path).parts)
    target.parent.mkdir(parents=True)
    os.link(external, target)
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-matching-hard-link",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_MULTIPLE_LINKS"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )
    assert external.read_bytes() == payload
    assert external.stat().st_nlink == 2
    assert not (store / "current" / f"{DATASET_ID}.json").exists()


@pytest.mark.parametrize("target_kind", ("current", "history", "artifact", "dq"))
def test_validator_rejects_multilink_current_history_artifact_and_dq(
    tmp_path: Path,
    target_kind: str,
) -> None:
    payload = b"published-before-external-hardlink"
    store = tmp_path / "store"
    request, _ = _case(
        tmp_path,
        payload,
        run_id=f"run-multilink-{target_kind}",
        generated_at=GENERATED_AT,
    )
    result = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )
    quality = result.snapshot.envelope.data_quality
    assert quality is not None and quality.report_path is not None
    targets = {
        "current": result.snapshot.pointer_path,
        "history": store / "pointer_history" / DATASET_ID / f"{result.snapshot.pointer_id}.json",
        "artifact": result.snapshot.payload_path,
        "dq": store / Path(*PurePosixPath(quality.report_path).parts),
    }
    target = targets[target_kind]
    external_link = tmp_path / f"external-{target_kind}.bin"
    os.link(target, external_link)

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_MULTIPLE_LINKS"):
        validate_current_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        )
    assert target.stat().st_nlink == 2
    assert external_link.stat().st_nlink == 2


def test_preexisting_lock_hardlink_cannot_modify_external_file(tmp_path: Path) -> None:
    store = tmp_path / "store"
    lock = store / "locks" / f"{DATASET_ID}.lock"
    external = tmp_path / "external-lock-target.bin"
    external.write_bytes(b"")
    lock.parent.mkdir(parents=True)
    os.link(external, lock)
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-lock-hard-link",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationError, match="DATASET_LOCK_FAILED"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )
    assert external.read_bytes() == b""
    assert external.stat().st_nlink == 2


def test_final_temp_hardlink_first_publish_rolls_back_without_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"first-generation-hardlink-race"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-final-hardlink-first",
        generated_at=GENERATED_AT,
    )
    external = tmp_path / "external-final-first.json"
    original = publish_module._replace_bound_temporary
    injected = False

    def inject_hardlink_then_replace(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        original(binding, temporary_name, target, source_descriptor, **kwargs)
        if target.parent.name == "current" and not injected:
            os.link(target, external)
            injected = True

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        inject_hardlink_then_replace,
    )
    with pytest.raises(
        DataPublicationIntegrityError,
        match="ATOMIC_COMMIT_ROLLED_BACK",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )

    assert injected is True
    assert exc_info.value.commit_state == "ROLLED_BACK"
    assert external.is_file()
    assert external.stat().st_nlink == 1
    assert not (tmp_path / "store" / "current" / f"{DATASET_ID}.json").exists()


def test_final_temp_hardlink_precommit_rejection_preserves_previous_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "store"
    first_payload = b"previous-canonical"
    first_request, _ = _case(
        tmp_path,
        first_payload,
        run_id="run-final-hardlink-previous",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=first_payload,
    )
    previous_pointer_bytes = first.snapshot.pointer_path.read_bytes()
    candidate_payload = b"candidate-hardlink-race"
    candidate_request, _ = _case(
        tmp_path,
        candidate_payload,
        run_id="run-final-hardlink-candidate",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    external = tmp_path / "external-final-candidate.json"
    original = publish_module._replace_bound_temporary
    injected = False

    def inject_hardlink_then_replace(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        if target.parent.name == "current" and not injected:
            os.link(target.parent / temporary_name, external)
            injected = True
        original(binding, temporary_name, target, source_descriptor, **kwargs)

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        inject_hardlink_then_replace,
    )
    with pytest.raises(
        DataPublicationIntegrityError,
        match="ATOMIC_COMMIT_ATTESTATION_FAILED",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=candidate_request,
            payload=candidate_payload,
        )

    assert injected is True
    assert exc_info.value.commit_state == "NOT_REPLACED"
    assert first.snapshot.pointer_path.read_bytes() == previous_pointer_bytes
    restored = validate_current_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )
    assert restored.pointer_sha256 == first.snapshot.pointer_sha256
    external.write_bytes(b"mutated-external-candidate")
    assert first.snapshot.pointer_path.read_bytes() == previous_pointer_bytes
    assert (
        validate_current_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        ).pointer_sha256
        == first.snapshot.pointer_sha256
    )


@pytest.mark.parametrize("has_previous", (False, True))
def test_hardlink_between_post_attestation_and_hash_never_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    has_previous: bool,
) -> None:
    store = tmp_path / "store"
    previous_bytes: bytes | None = None
    previous_sha: str | None = None
    if has_previous:
        previous_request, _ = _case(
            tmp_path,
            b"hash-window-previous",
            run_id="run-hash-window-previous",
            generated_at=GENERATED_AT,
        )
        previous = publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=previous_request,
            payload=b"hash-window-previous",
        )
        previous_bytes = previous.snapshot.pointer_path.read_bytes()
        previous_sha = previous.snapshot.pointer_sha256

    candidate_request, _ = _case(
        tmp_path,
        b"hash-window-candidate",
        run_id=f"run-hash-window-candidate-{has_previous}",
        generated_at=GENERATED_AT + timedelta(minutes=1 if has_previous else 0),
    )
    external = tmp_path / f"external-hash-window-{has_previous}.json"
    original = publish_module._attest_atomic_descriptor
    injected = False

    def inject_after_target_attestation(
        binding: Any,
        path: Path,
        descriptor: int,
        code: str,
    ) -> os.stat_result:
        nonlocal injected
        metadata = original(binding, path, descriptor, code)
        if path.parent.name == "current" and path.name == f"{DATASET_ID}.json" and not injected:
            os.link(path, external)
            injected = True
        return metadata

    monkeypatch.setattr(
        publish_module,
        "_attest_atomic_descriptor",
        inject_after_target_attestation,
    )
    with pytest.raises(
        DataPublicationIntegrityError,
        match="ATOMIC_COMMIT_ROLLED_BACK",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=candidate_request,
            payload=b"hash-window-candidate",
        )

    assert injected is True
    assert exc_info.value.commit_state == "ROLLED_BACK"
    assert external.is_file()
    assert external.stat().st_nlink == 1
    current_path = store / "current" / f"{DATASET_ID}.json"
    if previous_bytes is None:
        assert not current_path.exists()
    else:
        assert current_path.read_bytes() == previous_bytes
        assert (
            validate_current_snapshot(
                store_root=store,
                evidence_root=tmp_path,
                dataset_id=DATASET_ID,
            ).pointer_sha256
            == previous_sha
        )


@pytest.mark.parametrize("has_previous", (False, True))
def test_final_current_precondition_rebind_blocks_same_principal_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    has_previous: bool,
) -> None:
    store = tmp_path / "store"
    if has_previous:
        previous_request, _ = _case(
            tmp_path,
            b"rebind-previous",
            run_id="run-rebind-previous",
            generated_at=GENERATED_AT,
        )
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=previous_request,
            payload=b"rebind-previous",
        )

    candidate_request, _ = _case(
        tmp_path,
        b"rebind-candidate",
        run_id=f"run-rebind-candidate-{has_previous}",
        generated_at=GENERATED_AT + timedelta(minutes=1 if has_previous else 0),
    )
    current_path = store / "current" / f"{DATASET_ID}.json"
    concurrent_bytes = f"same-principal-state-{has_previous}".encode()
    original = publish_module._attest_atomic_descriptor
    injected = False

    def change_current_after_final_source_attestation(
        binding: Any,
        path: Path,
        descriptor: int,
        code: str,
    ) -> os.stat_result:
        nonlocal injected
        metadata = original(binding, path, descriptor, code)
        if (
            path.parent.name == "current"
            and path.name.startswith(f".{DATASET_ID}.json.")
            and path.name.endswith(".tmp")
            and not injected
        ):
            current_path.write_bytes(concurrent_bytes)
            injected = True
        return metadata

    monkeypatch.setattr(
        publish_module,
        "_attest_atomic_descriptor",
        change_current_after_final_source_attestation,
    )
    with pytest.raises(
        DataPublicationConflictError,
        match="CURRENT_CHANGED_BEFORE_COMMIT",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=candidate_request,
            payload=b"rebind-candidate",
        )

    assert injected is True
    assert exc_info.value.commit_state == "NOT_REPLACED"
    assert current_path.read_bytes() == concurrent_bytes


def test_committed_current_descriptor_close_failure_returns_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"current-close-warning"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-current-close-warning",
        generated_at=GENERATED_AT,
    )
    original_attest = publish_module._attest_atomic_descriptor
    original_close = os.close
    target_descriptor: int | None = None
    injected = False

    def capture_current_descriptor(
        binding: Any,
        path: Path,
        descriptor: int,
        code: str,
    ) -> os.stat_result:
        nonlocal target_descriptor
        metadata = original_attest(binding, path, descriptor, code)
        if path.parent.name == "current" and path.name == f"{DATASET_ID}.json":
            target_descriptor = descriptor
        return metadata

    def fail_first_current_close(descriptor: int) -> None:
        nonlocal injected
        if descriptor == target_descriptor and not injected:
            injected = True
            raise OSError("injected current descriptor close failure")
        original_close(descriptor)

    monkeypatch.setattr(
        publish_module,
        "_attest_atomic_descriptor",
        capture_current_descriptor,
    )
    monkeypatch.setattr(os, "close", fail_first_current_close)
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )

    assert injected is True
    assert result.post_commit_cleanup_status == "PASS_WITH_WARNINGS"
    assert any(
        "CURRENT_DESCRIPTOR_CLOSE_FAILED" in warning
        for warning in result.post_commit_cleanup_warnings
    )
    assert (
        validate_current_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        ).pointer_sha256
        == result.snapshot.pointer_sha256
    )


def test_non_current_descriptor_close_failure_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"stage-close-failure"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-stage-close-failure",
        generated_at=GENERATED_AT,
    )
    original_attest = publish_module._attest_atomic_descriptor
    original_close = os.close
    target_descriptor: int | None = None
    injected = False

    def capture_stage_descriptor(
        binding: Any,
        path: Path,
        descriptor: int,
        code: str,
    ) -> os.stat_result:
        nonlocal target_descriptor
        metadata = original_attest(binding, path, descriptor, code)
        if path.name == "payload.bin":
            target_descriptor = descriptor
        return metadata

    def fail_stage_close(descriptor: int) -> None:
        nonlocal injected
        if descriptor == target_descriptor and not injected:
            injected = True
            raise OSError("injected stage descriptor close failure")
        original_close(descriptor)

    monkeypatch.setattr(
        publish_module,
        "_attest_atomic_descriptor",
        capture_stage_descriptor,
    )
    monkeypatch.setattr(os, "close", fail_stage_close)
    try:
        with pytest.raises(ArtifactWriteError, match="ATOMIC_DESCRIPTOR_CLOSE_FAILED"):
            publish_immutable_snapshot(
                store_root=tmp_path / "store",
                evidence_root=tmp_path,
                request=request,
                payload=payload,
            )
    finally:
        if injected and target_descriptor is not None:
            original_close(target_descriptor)

    assert injected is True


def test_stage_temp_hardlink_never_returns_multilink_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"stage-hardlink-race"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-stage-hardlink-race",
        generated_at=GENERATED_AT,
    )
    external = tmp_path / "external-stage-candidate.bin"
    original = publish_module._replace_bound_temporary
    injected = False

    def inject_hardlink_then_replace(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        original(binding, temporary_name, target, source_descriptor, **kwargs)
        if target.name == "payload.bin" and not injected:
            os.link(target, external)
            injected = True

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        inject_hardlink_then_replace,
    )
    with pytest.raises(
        DataPublicationIntegrityError,
        match="ATOMIC_COMMIT_ROLLED_BACK",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )

    assert injected is True
    assert exc_info.value.commit_state == "ROLLED_BACK"
    assert external.read_bytes() == payload
    assert external.stat().st_nlink == 1
    assert not (tmp_path / "store" / "current" / f"{DATASET_ID}.json").exists()


def test_post_replace_attestation_exception_restores_previous_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"attestation-previous",
        run_id="run-attestation-previous",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"attestation-previous",
    )
    previous_bytes = first.snapshot.pointer_path.read_bytes()
    candidate_request, _ = _case(
        tmp_path,
        b"attestation-candidate",
        run_id="run-attestation-candidate",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    original = publish_module._attest_atomic_descriptor
    injected = False

    def fail_post_replace_attestation(
        binding: Any,
        path: Path,
        descriptor: int,
        code: str,
    ) -> os.stat_result:
        nonlocal injected
        if path.parent.name == "current" and path.name == f"{DATASET_ID}.json" and not injected:
            injected = True
            raise OSError("injected post-replace metadata failure")
        return original(binding, path, descriptor, code)

    monkeypatch.setattr(
        publish_module,
        "_attest_atomic_descriptor",
        fail_post_replace_attestation,
    )
    with pytest.raises(
        DataPublicationIntegrityError, match="ATOMIC_COMMIT_ROLLED_BACK"
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=candidate_request,
            payload=b"attestation-candidate",
        )

    assert injected is True
    assert exc_info.value.commit_state == "ROLLED_BACK"
    assert first.snapshot.pointer_path.read_bytes() == previous_bytes
    assert (
        validate_current_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            dataset_id=DATASET_ID,
        ).pointer_sha256
        == first.snapshot.pointer_sha256
    )


def test_restore_failure_reports_indeterminate_commit_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "store"
    first_request, _ = _case(
        tmp_path,
        b"indeterminate-previous",
        run_id="run-indeterminate-previous",
        generated_at=GENERATED_AT,
    )
    first = publish_immutable_snapshot(
        store_root=store,
        evidence_root=tmp_path,
        request=first_request,
        payload=b"indeterminate-previous",
    )
    previous_bytes = first.snapshot.pointer_path.read_bytes()
    candidate_request, _ = _case(
        tmp_path,
        b"indeterminate-candidate",
        run_id="run-indeterminate-candidate",
        generated_at=GENERATED_AT + timedelta(minutes=1),
    )
    external = tmp_path / "external-indeterminate.json"
    original_replace = publish_module._replace_bound_temporary
    original_write = publish_module._write_bytes_atomic_bound
    injected = False

    def add_post_replace_hardlink(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        original_replace(binding, temporary_name, target, source_descriptor, **kwargs)
        if target.parent.name == "current" and not injected:
            os.link(target, external)
            injected = True

    def fail_previous_pointer_restore(
        binding: Any,
        path: Path,
        content: bytes,
        **kwargs: Any,
    ) -> object:
        if path.parent.name == "current" and content == previous_bytes and not kwargs:
            raise ArtifactWriteError("ATOMIC_ARTIFACT_WRITE_FAILED", path, "restore injected")
        return original_write(binding, path, content, **kwargs)

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        add_post_replace_hardlink,
    )
    monkeypatch.setattr(
        publish_module,
        "_write_bytes_atomic_bound",
        fail_previous_pointer_restore,
    )
    with pytest.raises(
        DataPublicationIntegrityError,
        match="ATOMIC_COMMIT_ROLLBACK_INDETERMINATE",
    ) as exc_info:
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=candidate_request,
            payload=b"indeterminate-candidate",
        )

    assert injected is True
    assert exc_info.value.commit_state == "INDETERMINATE"


@pytest.mark.skipif(os.name != "nt", reason="Windows junction/share semantics")
def test_stage_junction_race_is_blocked_before_out_of_root_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-stage-junction-race",
        generated_at=GENERATED_AT,
    )
    outside = tmp_path / "outside-stage"
    outside.mkdir()
    original = publish_module._replace_bound_temporary
    attempted = False
    blocked = False

    def attempt_junction_then_write(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal attempted, blocked
        if target.name == "payload.bin" and not attempted:
            attempted = True
            blocked = _windows_junction_swap_was_blocked(target.parent, outside)
        original(binding, temporary_name, target, source_descriptor, **kwargs)

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        attempt_junction_then_write,
    )
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )

    assert attempted is True
    assert blocked is True
    assert result.current_pointer_changed is True
    current = validate_current_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )
    assert current.pointer_sha256 == result.snapshot.pointer_sha256
    assert list(outside.rglob("*")) == []


@pytest.mark.skipif(os.name != "nt", reason="Windows junction/share semantics")
def test_current_junction_race_is_blocked_before_out_of_root_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"trusted-current"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-current-junction-race",
        generated_at=GENERATED_AT,
    )
    outside = tmp_path / "outside-current-race"
    outside.mkdir()
    original = publish_module._replace_bound_temporary
    attempted = False
    blocked = False

    def attempt_junction_then_write(
        binding: Any,
        temporary_name: str,
        target: Path,
        source_descriptor: int,
        **kwargs: Any,
    ) -> None:
        nonlocal attempted, blocked
        if target.parent.name == "current" and not attempted:
            attempted = True
            blocked = _windows_junction_swap_was_blocked(target.parent, outside)
        original(binding, temporary_name, target, source_descriptor, **kwargs)

    monkeypatch.setattr(
        publish_module,
        "_replace_bound_temporary",
        attempt_junction_then_write,
    )
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )

    assert attempted is True
    assert blocked is True
    assert result.current_pointer_changed is True
    current = validate_current_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        dataset_id=DATASET_ID,
    )
    assert current.pointer_sha256 == result.snapshot.pointer_sha256
    assert list(outside.rglob("*")) == []


@pytest.mark.skipif(os.name != "nt", reason="Windows junction/share semantics")
def test_link_target_junction_race_is_blocked_before_out_of_root_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"trusted-link"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-link-junction-race",
        generated_at=GENERATED_AT,
    )
    outside = tmp_path / "outside-link"
    outside.mkdir()
    original = os.link
    attempted = False
    blocked = False

    def attempt_junction_then_link(
        source: Path,
        target: Path,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        nonlocal attempted, blocked
        if not attempted:
            attempted = True
            blocked = _windows_junction_swap_was_blocked(target.parent, outside)
        original(source, target, *args, **kwargs)

    monkeypatch.setattr(os, "link", attempt_junction_then_link)
    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )

    assert attempted is True
    assert blocked is True
    assert result.current_pointer_changed is True
    assert list(outside.rglob("*")) == []


def test_caller_selected_policy_stays_unverified_and_cannot_allow_cutover(
    tmp_path: Path,
) -> None:
    payload = b"structurally-valid-caller-policy"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-caller-policy",
        generated_at=GENERATED_AT,
        policy_id="caller_selected_policy",
        policy_version="caller_selected_policy.v99",
    )

    result = publish_immutable_snapshot(
        store_root=tmp_path / "store",
        evidence_root=tmp_path,
        request=request,
        payload=payload,
    )
    manifest = json.loads(result.snapshot.manifest_path.read_text(encoding="utf-8"))

    assert result.dq_execution_provenance_verified is False
    assert result.filesystem_security_profile == FILESYSTEM_SECURITY_PROFILE
    assert result.trusted_writer_principal_required is True
    assert (
        result.same_principal_adversarial_mutation_resistance
        == SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
    )
    assert result.store_acl_verified is False
    assert result.consumer_cutover_allowed is False
    assert result.crash_durability_verified is False
    assert result.same_principal_post_ack_mutation_protection is False
    assert manifest["dq_execution_provenance_verified"] is False
    assert manifest["filesystem_security_profile"] == FILESYSTEM_SECURITY_PROFILE
    assert manifest["trusted_writer_principal_required"] is True
    assert (
        manifest["same_principal_adversarial_mutation_resistance"]
        == SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
    )
    assert manifest["store_acl_verified"] is False
    assert manifest["consumer_cutover_allowed"] is False
    assert manifest["crash_durability_verified"] is False
    assert manifest["same_principal_post_ack_mutation_protection"] is False
    assert DQ_EXECUTION_PROVENANCE_LIMITATION in result.snapshot.envelope.limitations
    assert FILESYSTEM_SECURITY_PROFILE_LIMITATION in result.snapshot.envelope.limitations
    assert TRUSTED_WRITER_PRINCIPAL_LIMITATION in result.snapshot.envelope.limitations
    assert SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION in result.snapshot.envelope.limitations
    assert STORE_ACL_VERIFIED_LIMITATION in result.snapshot.envelope.limitations
    assert CONSUMER_CUTOVER_LIMITATION in result.snapshot.envelope.limitations
    assert CRASH_DURABILITY_VERIFIED_LIMITATION in result.snapshot.envelope.limitations
    assert SAME_PRINCIPAL_POST_ACK_LIMITATION in result.snapshot.envelope.limitations
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, dq_execution_provenance_verified=True)
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, consumer_cutover_allowed=True)
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, same_principal_post_ack_mutation_protection=True)
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, filesystem_security_profile="unreviewed.v1")
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, trusted_writer_principal_required=False)
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, same_principal_adversarial_mutation_resistance="GUARANTEED")
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, store_acl_verified=True)
    with pytest.raises(ValueError, match="cannot verify DQ execution provenance"):
        replace(result, crash_durability_verified=True)


def test_manifest_and_envelope_security_boundary_are_strict(tmp_path: Path) -> None:
    payload = b"strict-security-boundary"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-strict-security-boundary",
        generated_at=GENERATED_AT,
    )
    built = publish_module._build(request, payload, tmp_path, None)
    base_manifest = dict(built.manifest_payload)
    quality_payload = json.loads(built.quality_report_bytes)
    raw_envelope = base_manifest["artifact_envelope"]
    assert isinstance(raw_envelope, Mapping)
    envelope = ArtifactEnvelope.from_dict(raw_envelope)

    def validate(manifest: Mapping[str, object]) -> None:
        publish_module._validate_manifest(
            manifest,
            DATASET_ID,
            str(base_manifest["snapshot_id"]),
            str(base_manifest["source_event_id"]),
            str(base_manifest["run_id"]),
            built.snapshot,
            built.source,
            None,
            quality_report_payload=quality_payload,
        )

    invalid_fields: tuple[tuple[str, object], ...] = (
        ("dq_execution_provenance_verified", True),
        ("filesystem_security_profile", "unreviewed.v1"),
        ("trusted_writer_principal_required", False),
        ("same_principal_adversarial_mutation_resistance", "GUARANTEED"),
        ("store_acl_verified", True),
        ("consumer_cutover_allowed", True),
        ("crash_durability_verified", True),
        ("same_principal_post_ack_mutation_protection", True),
    )
    for field, invalid_value in invalid_fields:
        mutated = dict(base_manifest)
        mutated[field] = invalid_value
        with pytest.raises(
            DataPublicationIntegrityError,
            match="D0A_GOVERNANCE_BOUNDARY_INVALID",
        ):
            validate(mutated)

    limitation_matrix = (
        (DQ_EXECUTION_PROVENANCE_LIMITATION, "dq_execution_provenance_verified=true"),
        (FILESYSTEM_SECURITY_PROFILE_LIMITATION, "filesystem_security_profile=unreviewed.v1"),
        (TRUSTED_WRITER_PRINCIPAL_LIMITATION, "trusted_writer_principal_required=false"),
        (
            SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION,
            "same_principal_adversarial_mutation_resistance=GUARANTEED",
        ),
        (STORE_ACL_VERIFIED_LIMITATION, "store_acl_verified=true"),
        (CONSUMER_CUTOVER_LIMITATION, "consumer_cutover_allowed=true"),
        (CRASH_DURABILITY_VERIFIED_LIMITATION, "crash_durability_verified=true"),
        (
            SAME_PRINCIPAL_POST_ACK_LIMITATION,
            "same_principal_post_ack_mutation_protection=true",
        ),
    )
    for expected, contradictory in limitation_matrix:
        missing = dict(base_manifest)
        missing["artifact_envelope"] = replace(
            envelope,
            limitations=tuple(item for item in envelope.limitations if item != expected),
        ).to_dict()
        with pytest.raises(
            DataPublicationIntegrityError,
            match="D0A_GOVERNANCE_BOUNDARY_INVALID",
        ):
            validate(missing)

        contradicted = dict(base_manifest)
        contradicted["artifact_envelope"] = replace(
            envelope,
            limitations=(*envelope.limitations, contradictory),
        ).to_dict()
        with pytest.raises(
            DataPublicationIntegrityError,
            match="D0A_GOVERNANCE_BOUNDARY_INVALID",
        ):
            validate(contradicted)

        duplicated = dict(base_manifest)
        duplicated_envelope = dict(raw_envelope)
        duplicated_envelope["limitations"] = [*envelope.limitations, expected]
        duplicated["artifact_envelope"] = duplicated_envelope
        with pytest.raises(
            DataPublicationIntegrityError,
            match="D0A_GOVERNANCE_BOUNDARY_INVALID",
        ):
            validate(duplicated)

        malformed = dict(base_manifest)
        malformed_envelope = dict(raw_envelope)
        malformed_envelope["limitations"] = [
            (
                item
                if item != expected
                else f"{expected.partition('=')[0]}:{expected.partition('=')[2]}"
            )
            for item in envelope.limitations
        ]
        malformed["artifact_envelope"] = malformed_envelope
        with pytest.raises(
            DataPublicationIntegrityError,
            match="D0A_GOVERNANCE_BOUNDARY_INVALID",
        ):
            validate(malformed)

    publish_module._validate_governed_limitations(
        [*envelope.limitations, "unrelated_security_note=allowed"]
    )


def test_lock_initializes_first_byte_only_after_os_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "store" / "locks" / f"{DATASET_ID}.lock"
    lock_path.parent.mkdir(parents=True)
    observed_sizes: list[int] = []

    def observe_lock(handle: Any) -> None:
        handle.seek(0, 2)
        observed_sizes.append(handle.tell())

    monkeypatch.setattr(publish_module, "_try_lock", observe_lock)
    monkeypatch.setattr(publish_module, "_unlock", lambda _handle: None)

    with publish_module._file_lock(
        lock_path,
        root=tmp_path / "store",
        timeout_seconds=1.0,
        poll_seconds=0.01,
    ):
        assert lock_path.read_bytes() == b"\0"

    assert observed_sizes == [0]


def test_timeout_parameters_reject_non_numeric_values(tmp_path: Path) -> None:
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-timeout-type",
        generated_at=GENERATED_AT,
    )
    with pytest.raises(ValueError, match="lock_timeout_seconds"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
            lock_timeout_seconds="1",  # type: ignore[arg-type]
        )


def test_sanitation_attestation_must_be_explicit(tmp_path: Path) -> None:
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-unsanitized",
        generated_at=GENERATED_AT,
    )
    request = replace(
        request,
        source_event=replace(request.source_event, response_headers_sanitized=False),
    )
    with pytest.raises(DataPublicationIntegrityError, match="UNSANITIZED_RESPONSE_HEADERS"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )


def test_absolute_dq_report_path_is_rejected_as_nonportable(tmp_path: Path) -> None:
    payload = b"trusted"
    request, report = _case(
        tmp_path,
        payload,
        run_id="run-absolute-report",
        generated_at=GENERATED_AT,
    )
    request = replace(
        request,
        data_quality=replace(request.data_quality, report_path=str(report.resolve())),
    )
    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_PATH_INVALID"):
        publish_immutable_snapshot(
            store_root=tmp_path / "store",
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
def test_posix_in_root_symlink_alias_is_rejected(tmp_path: Path) -> None:
    store = tmp_path / "store"
    target = store / "current-target"
    store.mkdir()
    target.mkdir()
    (store / "current").symlink_to(target, target_is_directory=True)
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-posix-link",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_PATH_REPARSE_POINT"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics")
def test_windows_external_junction_is_rejected(tmp_path: Path) -> None:
    store = tmp_path / "store"
    outside = tmp_path / "outside-current"
    store.mkdir()
    outside.mkdir()
    junction = store / "current"
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.skip(f"junction creation unavailable: {completed.stderr}")
    payload = b"trusted"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="run-windows-junction",
        generated_at=GENERATED_AT,
    )

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_PATH_REPARSE_POINT"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
        )
    assert not (outside / f"{DATASET_ID}.json").exists()


def _windows_junction_swap_was_blocked(parent: Path, outside: Path) -> bool:
    displaced = parent.with_name(f"{parent.name}_displaced")
    try:
        parent.rename(displaced)
    except OSError as exc:
        if getattr(exc, "winerror", None) not in {5, 32}:
            raise
        return True
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(parent), str(outside)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(f"junction race setup failed: {completed.stderr}")
    return False


def _case(
    root: Path,
    payload: bytes,
    *,
    run_id: str,
    generated_at: datetime,
    as_of: date = AS_OF,
    coverage_start: date = COVERAGE_START,
    coverage_end: date = AS_OF,
    report_overrides: Mapping[str, object] | None = None,
    policy_id: str = "data_quality",
    policy_version: str = "data_quality.v1",
) -> tuple[SnapshotPublishRequest, Path]:
    report_relative = _write_quality_report(
        root,
        payload,
        name=run_id,
        generated_at=generated_at,
        as_of=as_of,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        overrides=report_overrides,
        policy_id=policy_id,
        policy_version=policy_version,
    )
    return (
        _request(
            root,
            report_relative,
            run_id=run_id,
            generated_at=generated_at,
            as_of=as_of,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            policy_id=policy_id,
            policy_version=policy_version,
        ),
        root / report_relative,
    )


def _write_quality_report(
    root: Path,
    payload: bytes,
    *,
    name: str,
    generated_at: datetime,
    as_of: date,
    coverage_start: date,
    coverage_end: date,
    overrides: Mapping[str, object] | None = None,
    policy_id: str = "data_quality",
    policy_version: str = "data_quality.v1",
) -> Path:
    snapshot = _snapshot_pointer(payload)
    report: dict[str, object] = {
        "schema_version": DATA_QUALITY_REPORT_SCHEMA_VERSION,
        "contract_id": "validated_prices_dq",
        "policy_id": policy_id,
        "policy_version": policy_version,
        "status": "PASS",
        "passed": True,
        "checked_at": (generated_at - timedelta(minutes=5)).isoformat(),
        "as_of": as_of.isoformat(),
        "coverage_start": coverage_start.isoformat(),
        "coverage_end": coverage_end.isoformat(),
        "checked_input_count": 1,
        "error_count": 0,
        "warning_count": 0,
        "blocking_issues": [],
        "evaluated_snapshot": snapshot.to_dict(),
        "production_effect": "none",
    }
    if overrides is not None:
        report.update(overrides)
    relative = Path("dq") / f"{name}.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(report))
    return relative


def _snapshot_pointer(
    payload: bytes,
    *,
    schema_version: str = PAYLOAD_SCHEMA,
) -> ArtifactPointer:
    content_sha = sha256_bytes(payload)
    return ArtifactPointer(
        path=(
            PurePosixPath("snapshots") / DATASET_ID / content_sha / f"payload.{PAYLOAD_TYPE}"
        ).as_posix(),
        artifact_type=PAYLOAD_TYPE,
        sha256=content_sha,
        size_bytes=len(payload),
        schema_version=schema_version,
    )


def _request(
    evidence_root: Path,
    report_relative: Path,
    *,
    run_id: str,
    generated_at: datetime,
    as_of: date = AS_OF,
    coverage_start: date = COVERAGE_START,
    coverage_end: date = AS_OF,
    policy_id: str = "data_quality",
    policy_version: str = "data_quality.v1",
) -> SnapshotPublishRequest:
    report_path = evidence_root / report_relative
    evidence = DataQualityEvidence(
        contract_id="validated_prices_dq",
        policy_id=policy_id,
        policy_version=policy_version,
        status="PASS",
        passed=True,
        checked_at=generated_at - timedelta(minutes=5),
        as_of=as_of,
        report_path=report_relative.as_posix(),
        report_sha256=sha256_path(report_path),
        checked_input_count=1,
    )
    return SnapshotPublishRequest(
        dataset_id=DATASET_ID,
        run_id=run_id,
        producer="tests.data_foundation",
        owner="data_platform",
        as_of=as_of,
        generated_at=generated_at,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        payload_artifact_type=PAYLOAD_TYPE,
        payload_schema_version=PAYLOAD_SCHEMA,
        data_quality_report_schema_version=DATA_QUALITY_REPORT_SCHEMA_VERSION,
        source_event=SourceEventProvenance(
            source_id="prices_primary",
            provider_name="test-provider",
            endpoint="https://example.invalid/prices",
            request_parameters={"start": coverage_start.isoformat(), "end": as_of.isoformat()},
            downloaded_at=generated_at - timedelta(minutes=10),
            row_count=1,
            source_role="primary",
            response_headers_sanitized=True,
        ),
        data_quality=evidence,
    )
