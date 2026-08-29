from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.qqq_options_capability_admission import (
    evaluate_qc_qqq_options_capability_admission,
)
from ai_trading_system.qqq_options_research.contracts import (
    DailySignalRecord,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    LocalCachedDataGateDeclaration,
)
from ai_trading_system.qqq_options_research.qc_project_adapter import (
    DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
    QCProjectAdapterContractError,
    QCProjectAdapterDescriptor,
    build_qc_qqq_options_project_adapter_descriptor,
    load_qc_qqq_options_project_adapter_policy,
    load_qqq_options_signal_package_for_qc,
)
from ai_trading_system.qqq_options_research.qc_project_adapter_v2 import (
    load_qc_qqq_options_project_adapter_policy as load_qc_qqq_options_project_adapter_policy_v2,
)
from ai_trading_system.qqq_options_research.signal_package import (
    QQQOptionsSignalIndex,
    QQQOptionsSignalPackage,
    QQQOptionsSignalPackageReceipt,
    SignalArtifactDigest,
    SignalSourceArtifact,
    load_qqq_options_signal_export_policy,
    write_qqq_options_signal_package,
)

_REPOSITORY_SHA = "a" * 40
_SOURCE_SHA = "b" * 64
_DQ_RECEIPT_SHA = "c" * 64
_DQ_REPORT_SHA = "d" * 64


def test_adapter_v2_binds_signal_export_v2_without_cloud_authority() -> None:
    loaded = load_qc_qqq_options_project_adapter_policy_v2(
        Path("config/research/qc_qqq_options_project_adapter_contract_v2.yaml")
    )

    assert loaded.policy.policy_id == "qc_qqq_options_project_adapter_contract_v2"
    assert loaded.policy.policy_version == "2.0.0"
    assert loaded.policy.signal_export_policy_sha256 == (
        "d6cae89234380794eae841c71a69c1cf9bde237d3a0d5f4c74081f99c3b0dac9"
    )
    assert loaded.policy.safety.cloud_run_authorized is False


@dataclass(frozen=True)
class _AdapterContext:
    package_root: Path
    capability_receipt_path: Path


def _sha_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _artifact(relative_path: str, content: bytes) -> SignalArtifactDigest:
    return SignalArtifactDigest(
        relative_path=relative_path,
        sha256=_sha_bytes(content),
        byte_count=len(content),
    )


def _safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def _make_package(
    output_root: Path,
    *,
    research_start: date = date(2021, 2, 22),
    engine_identity_status: str = "UNKNOWN",
    engine_identity: str | None = None,
) -> Path:
    loaded = load_qqq_options_signal_export_policy()
    policy = loaded.policy
    run_id = f"adapter-fixture-{research_start.isoformat()}"
    created_date = research_start + timedelta(days=1)
    created_at = datetime(
        created_date.year, created_date.month, created_date.day, 12, 0, tzinfo=UTC
    )
    source_pairs = tuple(
        sorted(
            (
                ("qqq.options.local_dq_report", _DQ_REPORT_SHA),
                ("qqq.options.local_dq_execution_receipt", _DQ_RECEIPT_SHA),
                ("qqq.options.signal_export_policy", loaded.policy_sha256),
                ("qqq.signal.source:fixture", _SOURCE_SHA),
            )
        )
    )
    signal = DailySignalRecord.seal(
        schema_name="daily_signal",
        schema_version="1.0.0",
        run_id=run_id,
        record_id=f"signal-{research_start.isoformat()}",
        created_at_utc=created_at,
        producer_version="test-fixture-v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=policy.shared_contract_sha256,
        source_ids=tuple(key for key, _ in source_pairs),
        source_checksums=tuple(value for _, value in source_pairs),
        requested_start=research_start,
        requested_end=research_start,
        evaluated_start=research_start,
        evaluated_end=research_start,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="PASS",
        pit_status="PASS",
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="qqq-options-adapter-fixture-lineage",
        safety=_safety(),
        signal_session=research_start,
        signal_as_of_utc=datetime(
            research_start.year,
            research_start.month,
            research_start.day,
            21,
            0,
            tzinfo=UTC,
        ),
        generated_at_utc=datetime(
            research_start.year,
            research_start.month,
            research_start.day,
            22,
            0,
            tzinfo=UTC,
        ),
        earliest_effective_session=research_start + timedelta(days=1),
        signal="FLAT",
        signal_source_sha256=_SOURCE_SHA,
    )
    daily_artifact = _artifact(
        f"daily_signals/{research_start.isoformat()}.json", signal.canonical_bytes
    )
    index = QQQOptionsSignalIndex.seal(run_id=run_id, artifacts=(daily_artifact,))
    index_artifact = _artifact("signal_index.json", index.canonical_bytes)
    manifest_pairs = tuple(sorted((*source_pairs, ("qqq.signal.index", index_artifact.sha256))))
    manifest = RunManifestRecord.seal(
        schema_name="run_manifest",
        schema_version="1.0.0",
        run_id=run_id,
        record_id=f"manifest-{run_id}",
        created_at_utc=created_at,
        producer_version="test-fixture-v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=policy.shared_contract_sha256,
        source_ids=tuple(key for key, _ in manifest_pairs),
        source_checksums=tuple(value for _, value in manifest_pairs),
        requested_start=research_start,
        requested_end=research_start,
        evaluated_start=research_start,
        evaluated_end=research_start,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="PASS",
        pit_status="PASS",
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="qqq-options-adapter-fixture-lineage",
        safety=_safety(),
        underlying="QQQ",
        initial_cash_usd=Decimal("100000"),
        account_currency="USD",
        account_type="CASH",
        signal_resolution="DAILY",
        execution_resolution="MINUTE",
        signal_artifact_sha256=index_artifact.sha256,
        engine_identity_status=engine_identity_status,
        engine_identity=engine_identity,
        evidence_admission_decision="CAPABILITY_OR_LICENSE_BLOCKED",
    )
    manifest_artifact = _artifact("run_manifest.json", manifest.canonical_bytes)
    source_artifact = SignalSourceArtifact(
        artifact_id="adapter-fixture-source",
        locator="inputs/adapter-fixture.json",
        sha256=_SOURCE_SHA,
        byte_count=100,
        export_classification="EXPORT_ALLOWED_DERIVED",
    )
    dq_receipt_artifact = SignalSourceArtifact(
        artifact_id="canonical-data-quality-execution-receipt",
        locator="outputs/validation/adapter-fixture-dq.json",
        sha256=_DQ_RECEIPT_SHA,
        byte_count=200,
        export_classification="EXPORT_ALLOWED_DERIVED",
    )
    receipt = QQQOptionsSignalPackageReceipt.seal(
        schema_version="qqq_options_signal_package_receipt.v1",
        run_id=run_id,
        created_at_utc=created_at,
        producer_version="test-fixture-v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        shared_contract_sha256=policy.shared_contract_sha256,
        shared_policy_sha256=policy.shared_policy_sha256,
        dq_pit_policy_sha256=policy.dq_pit_policy_sha256,
        calendar_id=policy.calendar_id,
        calendar_policy_id=policy.calendar_policy_id,
        calendar_policy_version=policy.calendar_policy_version,
        calendar_policy_sha256=policy.calendar_policy_sha256,
        research_window_role="PRIMARY",
        research_window_authority=None,
        source_artifact=source_artifact,
        local_dq_execution_receipt=dq_receipt_artifact,
        local_cached_data_gate=LocalCachedDataGateDeclaration(
            status="PASS",
            scope="CACHED_MARKET_MACRO",
            as_of_utc=created_at,
            report_locator="outputs/validation/adapter-fixture-report.json",
            report_sha256=_DQ_REPORT_SHA,
        ),
        run_manifest_artifact=manifest_artifact,
        signal_index_artifact=index_artifact,
        daily_signal_artifacts=(daily_artifact,),
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        export_classification="EXPORT_ALLOWED_DERIVED",
        safety=_safety(),
    )
    package = QQQOptionsSignalPackage(
        policy_sha256=loaded.policy_sha256,
        daily_signals=(signal,),
        signal_index=index,
        run_manifest=manifest,
        receipt=receipt,
    )
    return write_qqq_options_signal_package(package, output_root=output_root)


@pytest.fixture
def adapter_context(tmp_path: Path) -> _AdapterContext:
    package_root = _make_package(tmp_path / "packages")
    capability = evaluate_qc_qqq_options_capability_admission(
        output_root=tmp_path / "capability"
    )
    return _AdapterContext(
        package_root=package_root,
        capability_receipt_path=capability.receipt_path,
    )


def _build(context: _AdapterContext) -> QCProjectAdapterDescriptor:
    return build_qc_qqq_options_project_adapter_descriptor(
        signal_package_root=context.package_root,
        capability_receipt_path=context.capability_receipt_path,
    )


def _reseal_manifest_with_drift(
    package_root: Path, *, producer_version: str
) -> None:
    manifest = RunManifestRecord.from_json_bytes(
        (package_root / "run_manifest.json").read_bytes()
    )
    manifest_payload = manifest.model_dump(exclude={"content_sha256"})
    manifest_payload["producer_version"] = producer_version
    drifted_manifest = RunManifestRecord.seal(**manifest_payload)
    (package_root / "run_manifest.json").write_bytes(drifted_manifest.canonical_bytes)

    receipt = QQQOptionsSignalPackageReceipt.from_json_bytes(
        (package_root / "package_receipt.json").read_bytes()
    )
    receipt_payload = receipt.model_dump(exclude={"content_sha256"})
    receipt_payload["run_manifest_artifact"] = _artifact(
        "run_manifest.json", drifted_manifest.canonical_bytes
    )
    drifted_receipt = QQQOptionsSignalPackageReceipt.seal(**receipt_payload)
    (package_root / "package_receipt.json").write_bytes(drifted_receipt.canonical_bytes)


def test_policy_freezes_predecessors_subscription_and_external_safety() -> None:
    loaded = load_qc_qqq_options_project_adapter_policy()
    policy = loaded.policy
    assert policy.primary_research_start == date(2021, 2, 22)
    assert policy.approved_non_primary_authority_count == 0
    assert policy.legacy_non_default_start == date(2022, 12, 1)
    assert policy.legacy_non_default_start_is_default is False
    assert policy.subscription.underlying == "QQQ"
    assert policy.subscription.underlying_data_normalization == "RAW"
    assert policy.subscription.option_resolution == "MINUTE"
    assert policy.project_file_boundary.maximum_file_bytes == 32768
    assert policy.project_file_boundary.object_store_allowed is False
    assert policy.project_file_boundary.api_allowed is False
    assert policy.project_file_boundary.cli_allowed is False
    assert policy.safety.cloud_run_authorized is False
    assert policy.decision == "QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN"


def test_loader_strictly_replays_the_exact_signal_package(
    adapter_context: _AdapterContext,
) -> None:
    loaded = load_qqq_options_signal_package_for_qc(adapter_context.package_root)
    assert loaded.package.run_manifest.underlying == "QQQ"
    assert loaded.package.run_manifest.requested_start == date(2021, 2, 22)
    assert loaded.package.receipt.option_event_dq_status == "NOT_EVALUATED"
    assert loaded.package.files.keys() == loaded.file_sha256s.keys()
    for relative_path, content in loaded.package.files.items():
        assert _sha_bytes(content) == loaded.file_sha256s[relative_path]
        assert len(content) == loaded.file_byte_counts[relative_path]


def test_descriptor_binds_package_capability_and_no_cloud_authority(
    adapter_context: _AdapterContext,
) -> None:
    descriptor = _build(adapter_context)
    assert descriptor.daily_signal_count == 1
    assert descriptor.requested_start == date(2021, 2, 22)
    assert descriptor.evaluated_start == date(2021, 2, 22)
    assert descriptor.capability_decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert descriptor.capability_blocking_reason_codes
    assert descriptor.capability_bounded_pilot_preparation_allowed is False
    assert descriptor.input_admission_status == "UNKNOWN_REQUIRES_PLATFORM_EVIDENCE"
    assert descriptor.cloud_run_authorized is False
    assert descriptor.safety.external_platform_action_allowed is False
    assert descriptor.decision == "QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN"


def test_descriptor_is_canonical_content_bound_and_below_free_file_limit(
    adapter_context: _AdapterContext,
) -> None:
    descriptor = _build(adapter_context)
    replay = QCProjectAdapterDescriptor.from_json_bytes(descriptor.canonical_bytes)
    assert replay == descriptor
    assert replay.compute_content_sha256() == descriptor.content_sha256
    assert descriptor.canonical_sha256 == _sha_bytes(descriptor.canonical_bytes)
    assert len(descriptor.canonical_bytes) <= 32768
    with pytest.raises(
        QCProjectAdapterContractError, match="QC_PROJECT_ADAPTER_HASH_CALLER_SUPPLIED"
    ):
        QCProjectAdapterDescriptor.seal(
            **descriptor.model_dump(exclude={"content_sha256"}),
            content_sha256="0" * 64,
        )


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("extra", "QC_PROJECT_ADAPTER_PACKAGE_INVENTORY_MISMATCH"),
        ("missing", "QC_PROJECT_ADAPTER_PACKAGE_INVENTORY_MISMATCH"),
        ("receipt", "QC_PROJECT_ADAPTER_PACKAGE_RECEIPT_INVALID"),
        ("index", "QC_PROJECT_ADAPTER_ARTIFACT_BINDING_MISMATCH"),
        ("manifest", "QC_PROJECT_ADAPTER_ARTIFACT_BINDING_MISMATCH"),
        ("daily", "QC_PROJECT_ADAPTER_ARTIFACT_BINDING_MISMATCH"),
    ],
)
def test_package_inventory_and_tamper_fail_closed(
    adapter_context: _AdapterContext,
    tmp_path: Path,
    mutation: str,
    expected_code: str,
) -> None:
    target = tmp_path / f"tampered-{mutation}"
    shutil.copytree(adapter_context.package_root, target)
    if mutation == "extra":
        (target / "unexpected.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "missing":
        (target / "run_manifest.json").unlink()
    elif mutation == "receipt":
        (target / "package_receipt.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "index":
        (target / "signal_index.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "manifest":
        (target / "run_manifest.json").write_bytes(b"{}\n")
    else:
        daily = next((target / "daily_signals").iterdir())
        daily.write_bytes(b"{}\n")
    with pytest.raises(QCProjectAdapterContractError, match=expected_code):
        load_qqq_options_signal_package_for_qc(target)


def test_primary_start_cannot_drift_to_legacy_date(tmp_path: Path) -> None:
    package_root = _make_package(
        tmp_path / "legacy-packages", research_start=date(2022, 12, 1)
    )
    with pytest.raises(
        QCProjectAdapterContractError, match="QC_PROJECT_ADAPTER_PRIMARY_START_MISMATCH"
    ):
        load_qqq_options_signal_package_for_qc(package_root)


def test_manifest_cannot_pretend_engine_identity(tmp_path: Path) -> None:
    package_root = _make_package(
        tmp_path / "engine-packages",
        engine_identity_status="CONFIRMED",
        engine_identity="LEAN-fixture",
    )
    with pytest.raises(
        QCProjectAdapterContractError,
        match="QC_PROJECT_ADAPTER_ENGINE_IDENTITY_PRETENDED",
    ):
        load_qqq_options_signal_package_for_qc(package_root)


def test_resealed_manifest_receipt_semantic_drift_fails_closed(tmp_path: Path) -> None:
    package_root = _make_package(tmp_path / "semantic-drift-packages")
    _reseal_manifest_with_drift(package_root, producer_version="drifted-producer-v2")
    with pytest.raises(
        QCProjectAdapterContractError,
        match="QC_PROJECT_ADAPTER_MANIFEST_RECEIPT_SEMANTIC_MISMATCH",
    ):
        load_qqq_options_signal_package_for_qc(package_root)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("project_file_boundary", "maximum_file_bytes"), 32769),
        (("project_file_boundary", "object_store_allowed"), True),
        (("project_file_boundary", "api_allowed"), True),
        (("project_file_boundary", "cli_allowed"), True),
        (("project_file_boundary", "remote_http_allowed"), True),
        (("legacy_non_default_start_is_default",), True),
        (("approved_non_primary_authority_count",), 1),
    ],
)
def test_unsafe_or_unreviewed_policy_drift_fails_closed(
    tmp_path: Path, path: tuple[str, ...], value: Any
) -> None:
    payload = yaml.safe_load(
        DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH.read_text(encoding="utf-8")
    )
    cursor = payload
    for part in path[:-1]:
        cursor = cursor[part]
    cursor[path[-1]] = value
    policy_path = tmp_path / "drifted-policy.yaml"
    policy_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    with pytest.raises(
        QCProjectAdapterContractError, match="QC_PROJECT_ADAPTER_POLICY_INVALID"
    ):
        load_qc_qqq_options_project_adapter_policy(policy_path)


def test_capability_receipt_tamper_cannot_enter_descriptor(
    adapter_context: _AdapterContext,
    tmp_path: Path,
) -> None:
    payload = json.loads(adapter_context.capability_receipt_path.read_text(encoding="utf-8"))
    payload["decision"] = "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"
    tampered = tmp_path / adapter_context.capability_receipt_path.name
    tampered.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="QC_CAPABILITY_RECEIPT"):
        build_qc_qqq_options_project_adapter_descriptor(
            signal_package_root=adapter_context.package_root,
            capability_receipt_path=tampered,
        )


def test_descriptor_rejects_noncanonical_and_semantic_tamper(
    adapter_context: _AdapterContext,
) -> None:
    descriptor = _build(adapter_context)
    payload = json.loads(descriptor.canonical_bytes)
    payload["cloud_run_authorized"] = True
    with pytest.raises(
        QCProjectAdapterContractError, match="QC_PROJECT_ADAPTER_DESCRIPTOR_INVALID"
    ):
        QCProjectAdapterDescriptor.from_json_bytes(
            (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
        )
    compact = json.dumps(json.loads(descriptor.canonical_bytes), sort_keys=True).encode(
        "utf-8"
    )
    with pytest.raises(
        QCProjectAdapterContractError,
        match="QC_PROJECT_ADAPTER_DESCRIPTOR_NOT_CANONICAL",
    ):
        QCProjectAdapterDescriptor.from_json_bytes(compact)


def test_signal_package_root_symlink_is_prohibited(
    adapter_context: _AdapterContext, tmp_path: Path
) -> None:
    link = tmp_path / "package-link"
    try:
        link.symlink_to(adapter_context.package_root, target_is_directory=True)
    except OSError:
        pytest.skip("platform does not permit creating a directory symlink")
    with pytest.raises(
        QCProjectAdapterContractError, match="QC_PROJECT_ADAPTER_PACKAGE_ROOT_INVALID"
    ):
        load_qqq_options_signal_package_for_qc(link)
