from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.qc_qqq_options_capability_admission import (
    QCCapabilityAdmissionContractError,
    QCCapabilityAdmissionPolicy,
    QCCapabilityAdmissionReceipt,
    QCCapabilityEvidence,
)
from ai_trading_system.platform.artifacts import sha256_path, write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH = Path(
    "config/research/qc_qqq_options_capability_admission_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH = Path(
    "inputs/external_validation/qc_qqq_options_capability_evidence.template.json"
)


@dataclass(frozen=True)
class QCCapabilityAdmissionBuildResult:
    receipt: QCCapabilityAdmissionReceipt
    receipt_path: Path


def load_qc_qqq_options_capability_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityAdmissionPolicy:
    resolved = _resolve(path, project_root=project_root)
    try:
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        return QCCapabilityAdmissionPolicy.model_validate(payload)
    except (OSError, TypeError, ValueError) as exc:
        raise QCCapabilityAdmissionContractError(
            "QC_CAPABILITY_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc


def load_qc_qqq_options_capability_evidence(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityEvidence:
    resolved = _resolve(path, project_root=project_root)
    try:
        return QCCapabilityEvidence.from_json_bytes(resolved.read_bytes())
    except OSError as exc:
        raise QCCapabilityAdmissionContractError(
            "QC_CAPABILITY_EVIDENCE_READ_FAILED", f"{resolved}: {exc}"
        ) from exc


def evaluate_qc_qqq_options_capability_admission(
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    evidence_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    output_root: Path,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityAdmissionBuildResult:
    resolved_policy = _resolve(policy_path, project_root=project_root)
    resolved_evidence = _resolve(evidence_path, project_root=project_root)
    policy = load_qc_qqq_options_capability_policy(
        resolved_policy,
        project_root=project_root,
    )
    evidence = load_qc_qqq_options_capability_evidence(
        resolved_evidence,
        project_root=project_root,
    )
    receipt = _build_receipt(
        policy=policy,
        policy_sha256=sha256_path(resolved_policy),
        evidence=evidence,
        evidence_sha256=sha256_path(resolved_evidence),
    )
    resolved_output = _resolve(output_root, project_root=project_root)
    receipt_path = resolved_output / f"{receipt.receipt_id}.json"
    write_bytes_atomic(receipt_path, receipt.canonical_bytes)
    return QCCapabilityAdmissionBuildResult(receipt=receipt, receipt_path=receipt_path)


def verify_qc_qqq_options_capability_admission_receipt(
    receipt_path: Path,
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    evidence_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityAdmissionReceipt:
    resolved_receipt = _resolve(receipt_path, project_root=project_root)
    try:
        receipt = QCCapabilityAdmissionReceipt.from_json_bytes(resolved_receipt.read_bytes())
    except OSError as exc:
        raise QCCapabilityAdmissionContractError(
            "QC_CAPABILITY_RECEIPT_READ_FAILED", f"{resolved_receipt}: {exc}"
        ) from exc

    resolved_policy = _resolve(policy_path, project_root=project_root)
    resolved_evidence = _resolve(evidence_path, project_root=project_root)
    policy = load_qc_qqq_options_capability_policy(
        resolved_policy,
        project_root=project_root,
    )
    evidence = load_qc_qqq_options_capability_evidence(
        resolved_evidence,
        project_root=project_root,
    )
    expected = _build_receipt(
        policy=policy,
        policy_sha256=sha256_path(resolved_policy),
        evidence=evidence,
        evidence_sha256=sha256_path(resolved_evidence),
    )
    if receipt != expected:
        raise QCCapabilityAdmissionContractError(
            "QC_CAPABILITY_RECEIPT_BINDING_MISMATCH",
            "receipt does not match the bound policy and evidence bytes",
        )
    if resolved_receipt.name != f"{receipt.receipt_id}.json":
        raise QCCapabilityAdmissionContractError(
            "QC_CAPABILITY_RECEIPT_PATH_MISMATCH",
            "receipt filename does not match its content-derived id",
        )
    return receipt


def _build_receipt(
    *,
    policy: QCCapabilityAdmissionPolicy,
    policy_sha256: str,
    evidence: QCCapabilityEvidence,
    evidence_sha256: str,
) -> QCCapabilityAdmissionReceipt:
    blockers: set[str] = set()
    if evidence.platform != policy.platform:
        blockers.add("PLATFORM_MISMATCH")
    if (
        not evidence.external_action_authorized
        or evidence.owner_authorization_id != policy.required_owner_authorization_id
    ):
        blockers.add("OWNER_AUTHORIZATION_MISSING_OR_MISMATCH")

    allowed_global_sources = set(policy.allowed_evidence_source_kinds)
    item_by_id = {item.item_id: item for item in evidence.items}
    item_rules = {item.item_id: item for item in policy.item_rules}
    for item_id in sorted(item_rules.keys() - item_by_id.keys()):
        blockers.add(f"REQUIRED_ITEM_MISSING:{item_id}")
    for item_id in sorted(item_by_id.keys() - item_rules.keys()):
        blockers.add(f"UNDECLARED_ITEM:{item_id}")

    confirmed_item_count = 0
    for item_id in sorted(item_rules.keys() & item_by_id.keys()):
        item = item_by_id[item_id]
        rule = item_rules[item_id]
        source_allowed = item.source_kind in allowed_global_sources and item.source_kind in set(
            rule.allowed_source_kinds
        )
        if not source_allowed:
            blockers.add(f"ITEM_SOURCE_NOT_ALLOWED:{item_id}")
        if item.status != "CONFIRMED":
            blockers.add(f"REQUIRED_ITEM_{item.status}:{item_id}")
        if item.status == "CONFIRMED" and source_allowed:
            confirmed_item_count += 1

    export_by_id = {item.field_id: item for item in evidence.field_exports}
    export_rules = {item.field_id: item for item in policy.field_export_rules}
    for field_id in sorted(export_rules.keys() - export_by_id.keys()):
        blockers.add(f"REQUIRED_FIELD_MISSING:{field_id}")
    for field_id in sorted(export_by_id.keys() - export_rules.keys()):
        blockers.add(f"UNDECLARED_FIELD:{field_id}")

    confirmed_field_count = 0
    for field_id in sorted(export_rules.keys() & export_by_id.keys()):
        field = export_by_id[field_id]
        field_rule = export_rules[field_id]
        source_allowed = field.source_kind in allowed_global_sources and field.source_kind in set(
            field_rule.allowed_source_kinds
        )
        classification_matches = field.export_classification == field_rule.required_classification
        if not source_allowed:
            blockers.add(f"FIELD_SOURCE_NOT_ALLOWED:{field_id}")
        if field.status != "CONFIRMED":
            blockers.add(f"REQUIRED_FIELD_{field.status}:{field_id}")
        if not classification_matches:
            blockers.add(f"FIELD_EXPORT_CLASSIFICATION_MISMATCH:{field_id}")
        if field.status == "CONFIRMED" and source_allowed and classification_matches:
            confirmed_field_count += 1

    admitted = not blockers
    return QCCapabilityAdmissionReceipt(
        schema_version="qc_qqq_options_capability_admission_receipt.v1",
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        evidence_probe_id=evidence.probe_id,
        evidence_sha256=evidence_sha256,
        evaluated_at=evidence.captured_at,
        decision=(policy.confirmed_decision if admitted else policy.blocked_decision),
        blocking_reason_codes=tuple(sorted(blockers)),
        confirmed_item_count=confirmed_item_count,
        required_item_count=len(policy.item_rules),
        confirmed_field_count=confirmed_field_count,
        required_field_count=len(policy.field_export_rules),
        bounded_pilot_preparation_allowed=admitted,
        safety=policy.safety,
    )


def _resolve(path: Path, *, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path
