from __future__ import annotations

import copy
import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import ai_trading_system.data.o1_relative_opportunity_dq_candidate as dq_module
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data.o1_relative_opportunity_dq_candidate import (
    O1RelativeOpportunityDqError,
    materialize_and_validate_o1_candidate,
    resume_existing_o1_candidate,
    validate_o1_dq_gate,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_POLICY_PATH = (
    PROJECT_ROOT / "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)


def test_public_runner_stops_at_strict_dq_and_writes_fail_closed_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _install_orchestration_fixture(tmp_path, monkeypatch, warning_count=0)

    result = materialize_and_validate_o1_candidate(
        source_project_root=fixture.source_root,
        output_root=fixture.output_root,
        project_root=fixture.project_root,
        generated_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
        audit_policy_path=fixture.audit_policy_path,
    )

    validate_o1_dq_gate(result.gate)
    assert result.gate_path.read_bytes() == canonical_json_bytes(result.gate)
    assert result.gate["fresh_data_quality"]["status"] == "PASS"
    assert result.gate["fresh_data_quality"]["evaluated_end"] == "2026-07-24"
    assert result.gate["claim_boundary"] == {
        "source_workspace_mutated": False,
        "daily_consumer_dispatched": False,
        "coverage_audit_executed": False,
        "model_training_executed": False,
        "new_o1_result_read": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    assert fixture.calls == ["materialize", "canonical_dq", "verify_dq"]


def test_public_runner_fails_closed_on_dq_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _install_orchestration_fixture(tmp_path, monkeypatch, warning_count=1)

    with pytest.raises(O1RelativeOpportunityDqError) as exc:
        materialize_and_validate_o1_candidate(
            source_project_root=fixture.source_root,
            output_root=fixture.output_root,
            project_root=fixture.project_root,
            generated_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
            audit_policy_path=fixture.audit_policy_path,
        )

    assert exc.value.code == "O1_DQ_NOT_STRICT_PASS"
    assert not (fixture.output_root / "o1_dq_gate.json").exists()
    assert fixture.calls == ["materialize", "canonical_dq", "verify_dq"]


def test_resume_verifies_existing_candidate_without_rematerializing_or_rerunning_dq(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _install_orchestration_fixture(tmp_path, monkeypatch, warning_count=0)
    candidate = fixture.output_root / "candidate_project"
    candidate.mkdir(parents=True)
    copy_path = (
        candidate
        / "outputs/data_foundation_consumer_migration"
        / "consumer_copy_1234567890abcdef1234567890abcdef"
        / "copy_manifest.json"
    )
    copy_path.parent.mkdir(parents=True)
    selected_publication = {
        "transaction_id": fixture.publication.transaction_id,
        "transaction_sha256": fixture.publication.transaction_manifest_sha256,
        "discovery_pointer_sha256": fixture.publication.discovery_pointer_sha256,
        "requested_start": fixture.publication.requested_start.isoformat(),
        "requested_end": fixture.publication.requested_end.isoformat(),
        "artifact_sha256": {},
        "manifest_sha256": "6" * 64,
        "manifest_row_count": 3,
        "legacy_projection_verified": True,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    fixture.publication.artifact_sha256 = {}
    fixture.publication.manifest_sha256 = "6" * 64
    fixture.publication.manifest_row_count = 3
    copy_manifest = {
        "copy_manifest_id": "consumer_copy_1234567890abcdef1234567890abcdef",
        "source_project_root": fixture.source_root.as_posix(),
        "historical_receipt_id": fixture.migration.historical.receipt_id,
        "historical_authorization_id": fixture.migration.historical.authorization_id,
        "selected_publication": selected_publication,
        "all_objects_checksum_verified": True,
    }
    copy_path.write_bytes(canonical_json_bytes(copy_manifest))
    receipt_path = (
        candidate / "outputs/data_quality/executions" / fixture.receipt.receipt_id / "receipt.json"
    )
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(b"{}\n")
    monkeypatch.setattr(dq_module, "validate_candidate_copy_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        dq_module,
        "resolve_download_publication",
        lambda **kwargs: fixture.publication,
    )

    result = resume_existing_o1_candidate(
        source_project_root=fixture.source_root,
        output_root=fixture.output_root,
        project_root=fixture.project_root,
        generated_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
        audit_policy_path=fixture.audit_policy_path,
    )

    assert result.gate["candidate_workspace"]["recovery_mode"] == (
        "VERIFIED_EXISTING_CANDIDATE_AFTER_SUMMARY_INTERRUPTION"
    )
    assert result.gate["claim_boundary"]["daily_consumer_dispatched"] is False
    assert fixture.calls == ["verify_dq"]


def test_wrong_source_workspace_is_rejected_before_materialization(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    expected_source = tmp_path / "expected-source"
    actual_source = tmp_path / "actual-source"
    expected_source.mkdir()
    actual_source.mkdir()
    audit_path = _write_audit_policy(project_root, expected_source)
    output_root = project_root / "outputs/validation_runtime/candidate"

    with pytest.raises(O1RelativeOpportunityDqError) as exc:
        materialize_and_validate_o1_candidate(
            source_project_root=actual_source,
            output_root=output_root,
            project_root=project_root,
            generated_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
            audit_policy_path=audit_path.relative_to(project_root),
        )

    assert exc.value.code == "O1_DQ_SOURCE_WORKSPACE_MISMATCH"
    assert not output_root.exists()


def test_candidate_member_tamper_is_rejected(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    member_path = candidate / "data/raw/prices_daily.csv"
    member_path.parent.mkdir(parents=True)
    member_path.write_bytes(b"observed")
    expected = {
        role: {
            "sha256": hashlib.sha256(b"observed").hexdigest(),
            "size_bytes": len(b"observed"),
        }
        for role in ("prices", "rates", "secondary_prices")
    }
    for _role, relative in dq_module._MEMBER_PATHS.items():
        path = candidate / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"observed")
    expected["rates"]["sha256"] = hashlib.sha256(b"different").hexdigest()

    with pytest.raises(O1RelativeOpportunityDqError) as exc:
        dq_module._verify_candidate_objects(candidate, expected)

    assert exc.value.code == "O1_DQ_MEMBER_TAMPERED"


def test_gate_id_detects_summary_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _install_orchestration_fixture(tmp_path, monkeypatch, warning_count=0)
    result = materialize_and_validate_o1_candidate(
        source_project_root=fixture.source_root,
        output_root=fixture.output_root,
        project_root=fixture.project_root,
        generated_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
        audit_policy_path=fixture.audit_policy_path,
    )
    tampered = copy.deepcopy(result.gate)
    tampered["candidate_workspace"]["retained_for_same_store_coverage"] = False

    with pytest.raises(O1RelativeOpportunityDqError) as exc:
        validate_o1_dq_gate(tampered)

    assert exc.value.code == "O1_DQ_GATE_ID_MISMATCH"


def _install_orchestration_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    warning_count: int,
) -> SimpleNamespace:
    project_root = tmp_path / "project"
    project_root.mkdir()
    source_root = tmp_path / "source"
    source_root.mkdir()
    audit_path = _write_audit_policy(project_root, source_root)
    acl_path = project_root / "config/data/data_foundation_acl.yaml"
    acl_path.parent.mkdir(parents=True)
    acl_path.write_text("{}\n", encoding="utf-8")
    output_root = project_root / "outputs/validation_runtime/o1"
    calls: list[str] = []

    historical = SimpleNamespace(
        as_of=date(2026, 7, 27),
        receipt_id="dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4",
        receipt_path="outputs/data_quality/executions/receipt.json",
        receipt_sha256="6a4319f15f65a06345f08965c04cada01083d00a478e06febfdfd21f5ef56a58",
        authorization_id="dq_consumer_authorization_fe8360fab72bb976f3b799dba3a7bb933561cc34fa47b3ad7040a9e5fe5fcc02",
        authorization_path="outputs/data_quality/consumer_authorizations/attestation.json",
        authorization_sha256="6f87e196aa97ee53e17c79aad46634e615305ccb0d89c915275cab42c9fa9ec3",
        publication_transaction_id="download_txn_80b403268d6023acaf33b0608630b908",
        publication_transaction_sha256="9ed6e7ec705633bec21e032a25f48ca93fd7ef0ead899bbe857b0f30591d7778",
        publication_discovery_pointer_sha256="65f652f79ed07c0cc074dc1cc09fe444fa912fa21580df6ebb2eee340926199f",
    )
    migration = SimpleNamespace(
        historical=historical,
        capabilities=SimpleNamespace(
            d0c_bundle_path="d0c.json",
            d0c_bundle_sha256="0" * 64,
            d0d_bundle_path="d0d.json",
            d0d_bundle_sha256="1" * 64,
        ),
        required_input_roles=("prices", "rates", "secondary_prices"),
        accepted_data_quality_statuses=("PASS",),
        execution_profile_id="daily_default.v1",
        allowed_output_parent="outputs/validation_runtime",
        source_publication_dir="data/raw",
        candidate_publication_dir="data/raw",
        publication_store_dir=".download_publications",
    )
    monkeypatch.setattr(
        dq_module,
        "load_consumer_migration_policy",
        lambda *args, **kwargs: migration,
    )
    monkeypatch.setattr(dq_module, "_validate_capability_evidence", lambda *args: None)
    monkeypatch.setattr(dq_module, "load_acl_policy", lambda path: object())
    inventory = {"sha256": "2" * 64, "file_count": 5, "size_bytes": 42}
    monkeypatch.setattr(dq_module, "_source_inventory", lambda *args: inventory)
    monkeypatch.setattr(
        dq_module,
        "_verify_candidate_objects",
        lambda *args: {
            role: {
                "path": relative,
                "sha256": "3" * 64,
                "size_bytes": 1,
                "verified": True,
            }
            for role, relative in dq_module._MEMBER_PATHS.items()
        },
    )
    monkeypatch.setattr(dq_module, "load_universe", lambda path: object())
    monkeypatch.setattr(dq_module, "configured_price_tickers", lambda universe: ["QQQ", "SGOV"])
    monkeypatch.setattr(dq_module, "configured_rate_series", lambda universe: ["DGS3MO"])

    publication = SimpleNamespace(
        transaction_id=historical.publication_transaction_id,
        transaction_manifest_sha256=historical.publication_transaction_sha256,
        discovery_pointer_sha256=historical.publication_discovery_pointer_sha256,
        requested_start=date(2021, 2, 22),
        requested_end=date(2026, 7, 27),
        legacy_projection_verified=True,
        consumer_cutover_allowed=False,
        production_effect="none",
        artifact_sha256={},
        manifest_sha256="6" * 64,
        manifest_row_count=3,
    )

    def fake_materialize(**kwargs: object) -> SimpleNamespace:
        calls.append("materialize")
        output_root.mkdir(parents=True)
        candidate = output_root / "candidate_project"
        candidate.mkdir()
        copy_path = output_root / "copy_manifest.json"
        copy_path.write_bytes(b"{}\n")
        return SimpleNamespace(
            candidate_project_root=candidate,
            copy_manifest_path=copy_path,
            copy_manifest={
                "copy_manifest_id": "consumer_copy_1234567890abcdef1234567890abcdef",
                "all_objects_checksum_verified": True,
            },
            publication=publication,
            historical_receipt=SimpleNamespace(receipt_id=historical.receipt_id),
            historical_authorization=SimpleNamespace(authorization_id=historical.authorization_id),
        )

    monkeypatch.setattr(dq_module, "materialize_isolated_candidate", fake_materialize)
    receipt = SimpleNamespace(
        receipt_id="dq_execution_" + "4" * 64,
        as_of=date(2026, 7, 27),
        requested_window=DataQualityDateWindow(date(2021, 2, 22), date(2026, 7, 27)),
        evaluated_window=DataQualityDateWindow(date(2021, 2, 22), date(2026, 7, 24)),
        invocation=(
            SimpleNamespace(
                name="execution_profile_id",
                value_json='"daily_default.v1"',
            ),
        ),
        report=SimpleNamespace(
            status="PASS",
            error_count=0,
            warning_count=warning_count,
        ),
        dq_execution_provenance_verified=True,
    )

    def fake_run(request: object, *, project_root: Path) -> SimpleNamespace:
        calls.append("canonical_dq")
        receipt_path = project_root / "outputs/data_quality/executions/receipt.json"
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_bytes(b"{}\n")
        return SimpleNamespace(
            receipt=receipt,
            receipt_path=receipt_path,
            report=SimpleNamespace(error_count=0, warning_count=warning_count),
        )

    def fake_verify(*args: object, **kwargs: object) -> SimpleNamespace:
        calls.append("verify_dq")
        return SimpleNamespace(
            status="PASS",
            receipt_sha256="5" * 64,
            receipt=receipt,
        )

    monkeypatch.setattr(dq_module, "run_canonical_data_quality_execution", fake_run)
    monkeypatch.setattr(dq_module, "verify_data_quality_execution_receipt", fake_verify)
    return SimpleNamespace(
        project_root=project_root,
        source_root=source_root,
        output_root=output_root,
        audit_policy_path=audit_path.relative_to(project_root),
        calls=calls,
        migration=migration,
        publication=publication,
        receipt=receipt,
    )


def _write_audit_policy(project_root: Path, source_root: Path) -> Path:
    payload = yaml.safe_load(AUDIT_POLICY_PATH.read_text(encoding="utf-8"))
    payload["status"] = "OWNER_APPROVED_SERIAL_CONTRACT_FROZEN_DATA_GATES_PENDING"
    payload["execution_binding"]["real_coverage_read_allowed_now"] = False
    payload["data_contract"]["recovery"]["source_workspace_path"] = source_root.as_posix()
    audit_path = project_root / "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
    audit_path.parent.mkdir(parents=True)
    audit_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return audit_path
