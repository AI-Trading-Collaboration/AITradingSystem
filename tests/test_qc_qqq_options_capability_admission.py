from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.contracts.qc_qqq_options_capability_admission import (
    QCCapabilityAdmissionContractError,
    QCCapabilityEvidence,
    QCCapabilityEvidenceItem,
    QCFieldExportEvidence,
)
from ai_trading_system.qqq_options_capability_admission import (
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    evaluate_qc_qqq_options_capability_admission,
    load_qc_qqq_options_capability_evidence,
    load_qc_qqq_options_capability_policy,
    verify_qc_qqq_options_capability_admission_receipt,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAPTURED_AT = datetime(2026, 8, 2, tzinfo=UTC)


def test_public_docs_template_is_deterministic_and_remains_blocked(tmp_path: Path) -> None:
    first = evaluate_qc_qqq_options_capability_admission(output_root=tmp_path)
    first_bytes = first.receipt_path.read_bytes()
    second = evaluate_qc_qqq_options_capability_admission(output_root=tmp_path)

    assert first.receipt == second.receipt
    assert first_bytes == second.receipt_path.read_bytes()
    assert first.receipt.decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert first.receipt.bounded_pilot_preparation_allowed is False
    assert "OWNER_AUTHORIZATION_MISSING_OR_MISMATCH" in (first.receipt.blocking_reason_codes)
    assert first.receipt.confirmed_item_count == 6
    assert first.receipt.required_item_count == 21
    assert first.receipt.confirmed_field_count == 3
    assert first.receipt.required_field_count == 12
    assert first.receipt.safety.production_effect == "none"
    assert first.receipt.safety.broker_action == "none"
    assert first.receipt.safety.bounded_cloud_pilot_authorized is False
    assert verify_qc_qqq_options_capability_admission_receipt(first.receipt_path) == first.receipt


def test_complete_authorized_evidence_allows_bounded_pilot_preparation_only(
    tmp_path: Path,
) -> None:
    evidence_path = _write_fully_confirmed_evidence(tmp_path / "evidence.json")
    result = evaluate_qc_qqq_options_capability_admission(
        evidence_path=evidence_path,
        output_root=tmp_path / "receipts",
    )

    assert result.receipt.decision == "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"
    assert result.receipt.blocking_reason_codes == ()
    assert result.receipt.bounded_pilot_preparation_allowed is True
    assert result.receipt.confirmed_item_count == result.receipt.required_item_count
    assert result.receipt.confirmed_field_count == result.receipt.required_field_count
    assert result.receipt.safety.bounded_cloud_pilot_authorized is False
    assert result.receipt.safety.strategy_execution_allowed is False
    assert (
        verify_qc_qqq_options_capability_admission_receipt(
            result.receipt_path,
            evidence_path=evidence_path,
        )
        == result.receipt
    )


def test_unknown_contradicted_and_license_mismatch_are_typed_blockers(
    tmp_path: Path,
) -> None:
    evidence = _fully_confirmed_evidence()
    payload = evidence.model_dump(mode="python")
    item = next(row for row in payload["items"] if row["item_id"] == "project_identity")
    item["status"] = "CONTRADICTED"
    item["exit_condition"] = "Create an authorized content-bound project."
    field = next(row for row in payload["field_exports"] if row["field_id"] == "order_summary")
    field["status"] = "UNKNOWN"
    field["export_classification"] = "UNKNOWN_REQUIRES_LICENSE_REVIEW"
    field["exit_condition"] = "Complete license review."
    changed = QCCapabilityEvidence.model_validate(payload)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(changed.canonical_bytes)

    receipt = evaluate_qc_qqq_options_capability_admission(
        evidence_path=evidence_path,
        output_root=tmp_path / "receipts",
    ).receipt

    assert receipt.decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert "REQUIRED_ITEM_CONTRADICTED:project_identity" in receipt.blocking_reason_codes
    assert "REQUIRED_FIELD_UNKNOWN:order_summary" in receipt.blocking_reason_codes
    assert "FIELD_EXPORT_CLASSIFICATION_MISMATCH:order_summary" in receipt.blocking_reason_codes


def test_missing_extra_and_wrong_source_authority_fail_closed(tmp_path: Path) -> None:
    evidence = _fully_confirmed_evidence()
    payload = evidence.model_dump(mode="python")
    payload["items"] = [row for row in payload["items"] if row["item_id"] != "backtest_identity"]
    account = next(row for row in payload["items"] if row["item_id"] == "account_entitlement")
    account["source_kind"] = "OFFICIAL_PUBLIC_DOCS"
    payload["field_exports"] = [
        *payload["field_exports"],
        {
            "field_id": "undeclared_export",
            "status": "CONFIRMED",
            "export_classification": "EXPORT_ALLOWED_DERIVED",
            "source_kind": "LICENSE_REVIEW",
            "source_locator": "https://evidence.example/undeclared_export",
            "recorded_at": CAPTURED_AT,
            "recorded_by": "independent_reviewer",
            "summary": "Deliberately undeclared test field.",
            "exit_condition": None,
            "raw_rows_embedded": False,
        },
    ]
    changed = QCCapabilityEvidence.model_validate(payload)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(changed.canonical_bytes)

    receipt = evaluate_qc_qqq_options_capability_admission(
        evidence_path=evidence_path,
        output_root=tmp_path / "receipts",
    ).receipt

    assert "REQUIRED_ITEM_MISSING:backtest_identity" in receipt.blocking_reason_codes
    assert "ITEM_SOURCE_NOT_ALLOWED:account_entitlement" in receipt.blocking_reason_codes
    assert "UNDECLARED_FIELD:undeclared_export" in receipt.blocking_reason_codes


def test_unsafe_duplicate_and_noncanonical_evidence_are_rejected(tmp_path: Path) -> None:
    evidence = _fully_confirmed_evidence()
    payload = evidence.model_dump(mode="python")
    payload["raw_options_data_included"] = True
    with pytest.raises(ValidationError, match="prohibited content"):
        QCCapabilityEvidence.model_validate(payload)

    payload = evidence.model_dump(mode="python")
    payload["items"] = [*payload["items"], payload["items"][0]]
    with pytest.raises(ValidationError, match="unique item ids"):
        QCCapabilityEvidence.model_validate(payload)

    noncanonical_path = tmp_path / "noncanonical.json"
    noncanonical_path.write_text(
        json.dumps(evidence.semantic_payload(), ensure_ascii=False, indent=4) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        QCCapabilityAdmissionContractError,
        match="QC_CAPABILITY_EVIDENCE_NOT_CANONICAL",
    ):
        load_qc_qqq_options_capability_evidence(noncanonical_path)


def test_policy_evidence_receipt_and_filename_tamper_fail_closed(tmp_path: Path) -> None:
    policy_path = tmp_path / "policy.yaml"
    evidence_path = tmp_path / "evidence.json"
    policy_path.write_bytes(
        (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH).read_bytes()
    )
    evidence_path.write_bytes(
        (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH).read_bytes()
    )
    result = evaluate_qc_qqq_options_capability_admission(
        policy_path=policy_path,
        evidence_path=evidence_path,
        output_root=tmp_path / "receipts",
    )
    original_receipt = result.receipt_path.read_bytes()

    payload = json.loads(original_receipt)
    payload["policy_version"] = "9.9.9"
    result.receipt_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        QCCapabilityAdmissionContractError,
        match="QC_CAPABILITY_RECEIPT_ID_MISMATCH",
    ):
        verify_qc_qqq_options_capability_admission_receipt(
            result.receipt_path,
            policy_path=policy_path,
            evidence_path=evidence_path,
        )

    result.receipt_path.write_bytes(original_receipt)
    policy_path.write_bytes(policy_path.read_bytes() + b"\n")
    with pytest.raises(
        QCCapabilityAdmissionContractError,
        match="QC_CAPABILITY_RECEIPT_BINDING_MISMATCH",
    ):
        verify_qc_qqq_options_capability_admission_receipt(
            result.receipt_path,
            policy_path=policy_path,
            evidence_path=evidence_path,
        )

    renamed = result.receipt_path.with_name("wrong-name.json")
    renamed.write_bytes(original_receipt)
    with pytest.raises(
        QCCapabilityAdmissionContractError,
        match="QC_CAPABILITY_RECEIPT_PATH_MISMATCH",
    ):
        verify_qc_qqq_options_capability_admission_receipt(
            renamed,
            policy_path=PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
            evidence_path=evidence_path,
        )


def _write_fully_confirmed_evidence(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_fully_confirmed_evidence().canonical_bytes)
    return path


def _fully_confirmed_evidence() -> QCCapabilityEvidence:
    policy = load_qc_qqq_options_capability_policy()
    items = tuple(
        QCCapabilityEvidenceItem(
            item_id=rule.item_id,
            status="CONFIRMED",
            source_kind=rule.allowed_source_kinds[0],
            source_locator=f"https://evidence.example/{rule.item_id}",
            recorded_at=CAPTURED_AT,
            recorded_by="independent_reviewer",
            summary=f"Confirmed fixture for {rule.item_id}.",
        )
        for rule in policy.item_rules
    )
    fields = tuple(
        QCFieldExportEvidence(
            field_id=rule.field_id,
            status="CONFIRMED",
            export_classification=rule.required_classification,
            source_kind=rule.allowed_source_kinds[0],
            source_locator=f"https://evidence.example/{rule.field_id}",
            recorded_at=CAPTURED_AT,
            recorded_by="independent_reviewer",
            summary=f"Confirmed fixture for {rule.field_id}.",
            raw_rows_embedded=False,
        )
        for rule in policy.field_export_rules
    )
    return QCCapabilityEvidence(
        schema_version="qc_qqq_options_capability_evidence.v1",
        probe_id="trading_2480_confirmed_fixture_v1",
        platform="QuantConnect",
        captured_at=CAPTURED_AT,
        external_action_authorized=True,
        owner_authorization_id=policy.required_owner_authorization_id,
        items=items,
        field_exports=fields,
        raw_options_data_included=False,
        investment_metrics_included=False,
        account_or_broker_identifiers_included=False,
        safety=policy.safety,
    )
