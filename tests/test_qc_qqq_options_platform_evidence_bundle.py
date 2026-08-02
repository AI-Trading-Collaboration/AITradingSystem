from __future__ import annotations

import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.contracts.qc_qqq_options_capability_admission import (
    QCCapabilityEvidence,
    QCCapabilityEvidenceItem,
    QCFieldExportEvidence,
)
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_capability_admission import (
    evaluate_qc_qqq_options_capability_admission,
    load_qc_qqq_options_capability_policy,
)
from ai_trading_system.qqq_options_research.contracts import EvidenceArtifact
from ai_trading_system.qqq_options_research.platform_evidence_bundle import (
    QCManualEvidenceArtifactIndex,
    QCManualEvidenceAttestation,
    QCManualEvidenceBundleMetadata,
    QCPlatformEvidenceBundleContractError,
    build_qc_qqq_options_platform_evidence_bundle_descriptor,
    build_qc_qqq_options_platform_evidence_manifest,
    load_qc_qqq_options_manual_evidence_bundle,
    load_qc_qqq_options_platform_evidence_bundle_policy,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = (
    PROJECT_ROOT
    / "config/research/qc_qqq_options_platform_evidence_manual_bundle_v1.yaml"
)
CAPTURED_AT = datetime(2026, 8, 2, 11, 30, tzinfo=UTC)
COLLECTED_AT = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
CLOSED_AT = datetime(2026, 8, 2, 12, 1, tzinfo=UTC)


def test_tracked_policy_is_offline_default_and_descriptor_is_sealed() -> None:
    loaded = load_qc_qqq_options_platform_evidence_bundle_policy()
    descriptor = build_qc_qqq_options_platform_evidence_bundle_descriptor()

    assert loaded.policy.collection_authorized is False
    assert loaded.policy.owner_authorization_token == (
        "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    )
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start == date(2022, 12, 1)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert descriptor.owner_authorization_token_status == "NOT_GRANTED"
    assert descriptor.default_disposition == "MANUAL_COLLECTION_INCOMPLETE"
    assert descriptor == type(descriptor).from_json_bytes(descriptor.canonical_bytes)
    assert descriptor.safety.external_platform_action_allowed is False


def test_tracked_policy_blocks_loading_before_package_facts_are_read(tmp_path: Path) -> None:
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        load_qc_qqq_options_manual_evidence_bundle(
            tmp_path / "absent",
            capability_receipt_path=tmp_path / "arbitrary.json",
        )

    assert raised.value.code == "MANUAL_COLLECTION_INCOMPLETE"
    assert "does not authorize" in raised.value.message


def test_complete_synthetic_bundle_is_ready_and_deterministic(tmp_path: Path) -> None:
    fixture = _build_bundle(tmp_path)

    first = _load(fixture)
    second = _load(fixture)

    assert first.validation.disposition == (
        "MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION"
    )
    assert first.validation.canonical_bytes == second.validation.canonical_bytes
    assert first.platform_manifest == second.platform_manifest
    assert first.platform_manifest.dq_status == "NOT_EVALUATED"
    assert first.platform_manifest.pit_status == "NOT_EVALUATED"
    assert first.platform_manifest.raw_option_rows_included is False
    assert first.platform_manifest.account_or_broker_identifiers_included is False
    assert tuple(item.artifact_id for item in first.platform_manifest.artifacts) == (
        "logs",
        "orders_csv",
        "platform_ui",
        "project_files",
        "report_pdf",
        "results_json",
        "trades_csv",
    )
    assert first.collector_attestation.attested_by != (
        first.reviewer_attestation.attested_by
    )


@pytest.mark.parametrize(
    "relative",
    (
        "artifact_index.json",
        "artifacts/logs.txt",
        "artifacts/platform_ui.png",
        "attestations/independent_reviewer.json",
    ),
)
def test_missing_mandatory_entry_is_typed_incomplete(
    tmp_path: Path, relative: str
) -> None:
    fixture = _build_bundle(tmp_path)
    (fixture["root"] / relative).unlink()

    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(fixture)

    assert raised.value.code == "MANUAL_COLLECTION_INCOMPLETE"


def test_unexpected_entry_and_noncanonical_control_are_invalid(tmp_path: Path) -> None:
    extra = _build_bundle(tmp_path / "extra")
    (extra["root"] / "unexpected.txt").write_text("not allowed\n", encoding="utf-8")
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(extra)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"

    noncanonical = _build_bundle(tmp_path / "noncanonical")
    index_path = noncanonical["root"] / "artifact_index.json"
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    index_path.write_text(json.dumps(payload, indent=4) + "\n", encoding="utf-8")
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(noncanonical)
    assert raised.value.code == "QC_PLATFORM_EVIDENCE_RECORD_NOT_CANONICAL"


def test_artifact_hash_byte_count_and_security_marker_fail_closed(tmp_path: Path) -> None:
    tampered = _build_bundle(tmp_path / "tampered")
    with (tampered["root"] / "artifacts/logs.txt").open("ab") as stream:
        stream.write(b"changed\n")
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(tampered)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "checksum or byte count" in raised.value.message

    prohibited = _build_bundle(tmp_path / "prohibited", prohibited_marker=True)
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(prohibited)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "option_chain_rows" in raised.value.message


def test_manifest_is_rebuilt_and_forged_bytes_cannot_claim_pass(tmp_path: Path) -> None:
    fixture = _build_bundle(tmp_path)
    manifest_path = fixture["root"] / "platform_evidence_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["tier_status"] = "UNKNOWN"
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(fixture)

    assert raised.value.code == "MANUAL_COLLECTION_INVALID"


def test_capability_receipt_is_reconstructed_not_trusted(tmp_path: Path) -> None:
    fixture = _build_bundle(tmp_path)
    receipt_path = fixture["receipt_path"]
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload["confirmed_item_count"] = payload["required_item_count"] - 1
    receipt_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(fixture)

    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "canonical facts" in raised.value.message


@pytest.mark.parametrize(
    ("field", "status", "expected_code"),
    (
        ("tier_status", "UNKNOWN", "MANUAL_COLLECTION_INCOMPLETE"),
        ("engine_identity_status", "UNKNOWN", "MANUAL_COLLECTION_INCOMPLETE"),
        ("license_status", "UNKNOWN", "MANUAL_COLLECTION_INCOMPLETE"),
        ("tier_status", "CONTRADICTED", "MANUAL_COLLECTION_INVALID"),
        ("engine_identity_status", "CONTRADICTED", "MANUAL_COLLECTION_INVALID"),
        ("license_status", "CONTRADICTED", "MANUAL_COLLECTION_INVALID"),
    ),
)
def test_unknown_is_incomplete_and_contradicted_is_invalid(
    tmp_path: Path, field: str, status: str, expected_code: str
) -> None:
    fixture = _build_bundle(tmp_path, statuses={field: status})

    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(fixture)

    assert raised.value.code == expected_code


def test_primary_window_is_exact_and_2022_12_01_never_becomes_default(
    tmp_path: Path,
) -> None:
    primary = _build_bundle(tmp_path / "primary")
    loaded = _load(primary)
    assert loaded.metadata.requested_start == date(2021, 2, 22)
    assert loaded.metadata.evaluated_start == date(2021, 2, 22)

    legacy = _build_bundle(
        tmp_path / "legacy",
        requested_start=date(2022, 12, 1),
        evaluated_start=date(2022, 12, 1),
    )
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(legacy)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "2021-02-22" in raised.value.message


def test_unreviewed_non_primary_window_and_same_person_review_are_invalid(
    tmp_path: Path,
) -> None:
    same_person = _build_bundle(tmp_path / "same", same_reviewer=True)
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(same_person)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "different people" in raised.value.message

    policy_path = _write_active_policy(tmp_path / "policy.yaml")
    policy_text = policy_path.read_text(encoding="utf-8").replace(
        "primary_research_start: 2021-02-22",
        "primary_research_start: 2022-12-01",
    )
    policy_path.write_text(policy_text, encoding="utf-8")
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        load_qc_qqq_options_platform_evidence_bundle_policy(policy_path)
    assert raised.value.code == "QC_PLATFORM_EVIDENCE_POLICY_INVALID"


def test_project_archive_traversal_and_binary_shape_are_invalid(tmp_path: Path) -> None:
    traversal = _build_bundle(tmp_path / "traversal", zip_traversal=True)
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(traversal)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "path traversal" in raised.value.message

    bad_png = _build_bundle(tmp_path / "bad_png")
    png_path = bad_png["root"] / "artifacts/platform_ui.png"
    png_path.write_bytes(b"not-a-png")
    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        _load(bad_png)
    assert raised.value.code == "MANUAL_COLLECTION_INVALID"


def test_policy_hash_and_adapter_mapping_authority_fail_closed(tmp_path: Path) -> None:
    policy_path = _write_active_policy(tmp_path / "policy.yaml")
    text = policy_path.read_text(encoding="utf-8").replace(
        "adapter_policy_sha256: b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616",
        f"adapter_policy_sha256: f{'0' * 63}",
    )
    policy_path.write_text(text, encoding="utf-8")

    with pytest.raises(QCPlatformEvidenceBundleContractError) as raised:
        load_qc_qqq_options_platform_evidence_bundle_policy(policy_path)

    assert raised.value.code == "MANUAL_COLLECTION_INVALID"
    assert "inherited policy bytes drifted" in raised.value.message


def test_index_order_and_seal_are_canonical_contracts() -> None:
    artifacts = (
        EvidenceArtifact(
            artifact_id="z",
            locator="z",
            sha256="a" * 64,
            byte_count=1,
            export_classification="EXPORT_ALLOWED_DERIVED",
        ),
        EvidenceArtifact(
            artifact_id="a",
            locator="a",
            sha256="b" * 64,
            byte_count=1,
            export_classification="EXPORT_ALLOWED_DERIVED",
        ),
    )
    with pytest.raises(ValidationError, match="sorted and unique"):
        QCManualEvidenceArtifactIndex.seal(
            schema_version="qc_qqq_options_manual_evidence_artifact_index.v1",
            bundle_id="bundle",
            artifacts=artifacts,
        )


def _load(fixture: dict[str, Path]):
    return load_qc_qqq_options_manual_evidence_bundle(
        fixture["root"],
        capability_receipt_path=fixture["receipt_path"],
        capability_evidence_path=fixture["evidence_path"],
        policy_path=fixture["policy_path"],
    )


def _build_bundle(
    tmp_path: Path,
    *,
    statuses: dict[str, str] | None = None,
    requested_start: date = date(2021, 2, 22),
    evaluated_start: date = date(2021, 2, 22),
    prohibited_marker: bool = False,
    same_reviewer: bool = False,
    zip_traversal: bool = False,
) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    policy_path = _write_active_policy(tmp_path / "active_policy.yaml")
    evidence_path = tmp_path / "capability_evidence.json"
    evidence_path.write_bytes(_fully_confirmed_evidence().canonical_bytes)
    receipt_result = evaluate_qc_qqq_options_capability_admission(
        evidence_path=evidence_path,
        output_root=tmp_path / "capability_receipts",
    )
    receipt_path = receipt_result.receipt_path
    receipt = receipt_result.receipt

    root = tmp_path / "bundle"
    (root / "artifacts").mkdir(parents=True)
    (root / "attestations").mkdir()
    project_zip = _zip_bytes("../escape.py" if zip_traversal else "main.py")
    artifact_bytes = {
        "artifacts/logs.txt": (
            b"Algorithm completed with derived metrics.\n"
            + (b"option_chain_rows=1\n" if prohibited_marker else b"")
        ),
        "artifacts/orders.csv": b"time,symbol,status\n2021-02-22T15:31:00Z,QQQ,filled\n",
        "artifacts/platform_ui.png": b"\x89PNG\r\n\x1a\nsynthetic-fixture",
        "artifacts/project_files.zip": project_zip,
        "artifacts/report.pdf": b"%PDF-1.4\n% synthetic fixture\n",
        "artifacts/results.json": b'{"result":"derived_fixture"}\n',
        "artifacts/trades.csv": b"time,symbol,quantity\n2021-02-22T15:31:00Z,QQQ,1\n",
    }
    for relative, content in artifact_bytes.items():
        (root / relative).write_bytes(content)

    artifacts = tuple(
        EvidenceArtifact(
            artifact_id=rule_id,
            locator=relative,
            sha256=_sha256_bytes(artifact_bytes[relative]),
            byte_count=len(artifact_bytes[relative]),
            export_classification="EXPORT_ALLOWED_DERIVED",
        )
        for rule_id, relative in (
            ("logs", "artifacts/logs.txt"),
            ("orders_csv", "artifacts/orders.csv"),
            ("platform_ui", "artifacts/platform_ui.png"),
            ("project_files", "artifacts/project_files.zip"),
            ("report_pdf", "artifacts/report.pdf"),
            ("results_json", "artifacts/results.json"),
            ("trades_csv", "artifacts/trades.csv"),
        )
    )
    index = QCManualEvidenceArtifactIndex.seal(
        schema_version="qc_qqq_options_manual_evidence_artifact_index.v1",
        bundle_id="bundle_fixture_001",
        artifacts=artifacts,
    )
    index_path = root / "artifact_index.json"
    index_path.write_bytes(index.canonical_bytes)

    loaded_policy = load_qc_qqq_options_platform_evidence_bundle_policy(policy_path)
    end = max(requested_start, evaluated_start) + timedelta(days=1)
    engine_values = {
        "adapter_descriptor_sha256": "a" * 64,
        "algorithm_language": "Python",
        "backtest_id": "backtest_fixture_001",
        "evaluated_end": end.isoformat(),
        "evaluated_start": evaluated_start.isoformat(),
        "lean_engine_identity": "Lean-v2.5.0-fixture",
        "project_id": "project_fixture_001",
        "repository_code_sha": "d" * 40,
        "requested_end": end.isoformat(),
        "requested_start": requested_start.isoformat(),
        "resource_runtime_telemetry": "B-MICRO synthetic fixture",
    }
    observed_statuses = {
        "tier_status": "CONFIRMED",
        "engine_identity_status": "CONFIRMED",
        "license_status": "CONFIRMED",
        **(statuses or {}),
    }
    metadata = QCManualEvidenceBundleMetadata.seal(
        schema_version="qc_qqq_options_manual_evidence_bundle_metadata.v1",
        bundle_id="bundle_fixture_001",
        run_id="run_fixture_001",
        collected_at_utc=COLLECTED_AT,
        bundle_closed_at_utc=CLOSED_AT,
        collected_by="collector_fixture",
        producer_version="1.0.0",
        repository_code_sha="d" * 40,
        policy_id=loaded_policy.policy.policy_id,
        policy_version=loaded_policy.policy.policy_version,
        policy_sha256=loaded_policy.policy_sha256,
        capability_receipt_id=receipt.receipt_id,
        capability_receipt_sha256=sha256_path(receipt_path),
        capability_policy_sha256=receipt.policy_sha256,
        capability_evidence_sha256=receipt.evidence_sha256,
        capability_decision=receipt.decision,
        shared_contract_sha256=loaded_policy.policy.shared_contract_sha256,
        shared_policy_sha256=loaded_policy.policy.shared_policy_sha256,
        dq_pit_policy_sha256=loaded_policy.policy.dq_pit_policy_sha256,
        adapter_policy_sha256=loaded_policy.policy.adapter_policy_sha256,
        adapter_descriptor_sha256="a" * 64,
        artifact_index_sha256=sha256_path(index_path),
        project_id="project_fixture_001",
        backtest_id="backtest_fixture_001",
        lean_engine_identity="Lean-v2.5.0-fixture",
        algorithm_language="Python",
        resource_runtime_telemetry="B-MICRO synthetic fixture",
        requested_start=requested_start,
        requested_end=end,
        evaluated_start=evaluated_start,
        evaluated_end=end,
        research_window_role="PRIMARY",
        reviewed_non_primary_authority_id=None,
        dq_caveat=None,
        tier_status=observed_statuses["tier_status"],
        engine_identity_status=observed_statuses["engine_identity_status"],
        license_status=observed_statuses["license_status"],
        license_review_authority_id=(
            "owner_license_review_001"
            if observed_statuses["license_status"] == "CONFIRMED"
            else None
        ),
        engine_identity_fields=tuple(
            {"field_name": name, "value": value}
            for name, value in sorted(engine_values.items())
        ),
        lineage_id="lineage_fixture_001",
        limitations=("SYNTHETIC_FIXTURE_ONLY",),
        data_quality_gate_required=False,
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        raw_option_rows_included=False,
        account_or_broker_identifiers_included=False,
        secrets_included=False,
    )
    metadata_path = root / "bundle_metadata.json"
    metadata_path.write_bytes(metadata.canonical_bytes)

    manifest = build_qc_qqq_options_platform_evidence_manifest(
        metadata=metadata,
        artifact_index=index,
        policy=loaded_policy.policy,
        policy_sha256=loaded_policy.policy_sha256,
        capability_receipt_sha256=sha256_path(receipt_path),
        bundle_metadata_sha256=sha256_path(metadata_path),
        artifact_index_sha256=sha256_path(index_path),
    )
    manifest_path = root / "platform_evidence_manifest.json"
    manifest_path.write_bytes(manifest.canonical_bytes)

    common_attestation = {
        "schema_version": "qc_qqq_options_manual_evidence_attestation.v1",
        "bundle_id": metadata.bundle_id,
        "bundle_metadata_sha256": sha256_path(metadata_path),
        "artifact_index_sha256": sha256_path(index_path),
        "platform_evidence_manifest_sha256": sha256_path(manifest_path),
        "capability_receipt_sha256": sha256_path(receipt_path),
        "inventory_reviewed": True,
        "checksums_reviewed": True,
        "platform_tier_reviewed": True,
        "engine_identity_reviewed": True,
        "license_reviewed": True,
        "no_raw_option_rows_confirmed": True,
        "no_secrets_confirmed": True,
        "no_account_identifiers_confirmed": True,
        "no_broker_identifiers_confirmed": True,
    }
    collector = QCManualEvidenceAttestation.seal(
        **common_attestation,
        attestation_id="collector_attestation_fixture_001",
        role="COLLECTOR",
        attested_by="collector_fixture",
        attested_at_utc=CLOSED_AT + timedelta(minutes=1),
        collector_attestation_sha256=None,
    )
    collector_path = root / "attestations/collector.json"
    collector_path.write_bytes(collector.canonical_bytes)
    reviewer = QCManualEvidenceAttestation.seal(
        **common_attestation,
        attestation_id="reviewer_attestation_fixture_001",
        role="INDEPENDENT_REVIEWER",
        attested_by="collector_fixture" if same_reviewer else "reviewer_fixture",
        attested_at_utc=CLOSED_AT + timedelta(minutes=2),
        collector_attestation_sha256=sha256_path(collector_path),
    )
    (root / "attestations/independent_reviewer.json").write_bytes(
        reviewer.canonical_bytes
    )
    return {
        "root": root,
        "policy_path": policy_path,
        "evidence_path": evidence_path,
        "receipt_path": receipt_path,
    }


def _write_active_policy(path: Path) -> Path:
    text = POLICY_PATH.read_text(encoding="utf-8")
    text = text.replace(
        "status: REVIEWED_OFFLINE_CONTRACT_BASELINE", "status: OWNER_REVIEWED_ACTIVE"
    ).replace("collection_authorized: false", "collection_authorized: true", 1)
    text = text.replace(
        "owner_authorization_token: NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        "owner_authorization_token: owner_decision:TRADING-2492:synthetic_fixture_only",
    )
    path.write_text(text, encoding="utf-8")
    return path


def _fully_confirmed_evidence() -> QCCapabilityEvidence:
    policy = load_qc_qqq_options_capability_policy()
    return QCCapabilityEvidence(
        schema_version="qc_qqq_options_capability_evidence.v1",
        probe_id="trading_2489_confirmed_fixture_v1",
        platform="QuantConnect",
        captured_at=CAPTURED_AT,
        external_action_authorized=True,
        owner_authorization_id=policy.required_owner_authorization_id,
        items=tuple(
            QCCapabilityEvidenceItem(
                item_id=rule.item_id,
                status="CONFIRMED",
                source_kind=rule.allowed_source_kinds[0],
                source_locator=f"https://evidence.example/{rule.item_id}",
                recorded_at=CAPTURED_AT,
                recorded_by="independent_reviewer",
                summary=f"Confirmed synthetic fixture for {rule.item_id}.",
            )
            for rule in policy.item_rules
        ),
        field_exports=tuple(
            QCFieldExportEvidence(
                field_id=rule.field_id,
                status="CONFIRMED",
                export_classification=rule.required_classification,
                source_kind=rule.allowed_source_kinds[0],
                source_locator=f"https://evidence.example/{rule.field_id}",
                recorded_at=CAPTURED_AT,
                recorded_by="independent_reviewer",
                summary=f"Confirmed synthetic fixture for {rule.field_id}.",
                raw_rows_embedded=False,
            )
            for rule in policy.field_export_rules
        ),
        raw_options_data_included=False,
        investment_metrics_included=False,
        account_or_broker_identifiers_included=False,
        safety=policy.safety,
    )


def _zip_bytes(name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, "# synthetic fixture\n")
    return buffer.getvalue()


def _sha256_bytes(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()
