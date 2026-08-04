from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.qc_qqq_options_capability_discovery_evidence import (
    QCCapabilityDiscoveryEvidence,
    QCCapabilityDiscoveryEvidenceContractError,
)
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_capability_discovery_authorization import (
    QCCapabilityDiscoveryAuthorizationLoadResult,
    load_qc_qqq_options_capability_discovery_authorization,
)

DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH = Path(
    "inputs/external_validation/"
    "qc_qqq_options_capability_discovery_evidence_20260804.json"
)


@dataclass(frozen=True)
class QCCapabilityDiscoveryEvidenceLoadResult:
    evidence: QCCapabilityDiscoveryEvidence
    evidence_path: Path
    evidence_file_sha256: str
    evidence_canonical_sha256: str
    authorization: QCCapabilityDiscoveryAuthorizationLoadResult


def load_qc_qqq_options_capability_discovery_evidence(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityDiscoveryEvidenceLoadResult:
    resolved_root = project_root.resolve()
    resolved_evidence = resolved_root / path
    try:
        resolved_evidence = _require_bound_regular_file(
            path,
            project_root=resolved_root,
            field="capability-discovery evidence",
        )
        evidence = QCCapabilityDiscoveryEvidence.from_json_bytes(
            resolved_evidence.read_bytes()
        )
        authorization = load_qc_qqq_options_capability_discovery_authorization(
            Path(evidence.authorization_policy_path),
            project_root=resolved_root,
        )
        if (
            evidence.authorization_policy_sha256
            != authorization.authorization_policy_sha256
        ):
            raise ValueError("authorization policy SHA-256 mismatch")
        if (
            evidence.authorization_canonical_sha256
            != authorization.authorization_canonical_sha256
        ):
            raise ValueError("authorization canonical SHA-256 mismatch")
        if (
            evidence.owner_authorization_id
            != authorization.authorization.owner_authorization_id
        ):
            raise ValueError("Owner authorization identity mismatch")
        if evidence.run_role != authorization.authorization.run_role:
            raise ValueError("capability-discovery run role mismatch")
        if evidence.collector_id != authorization.authorization.actors.collector_id:
            raise ValueError("collector identity mismatch")
        if (
            evidence.independent_reviewer_id
            != authorization.authorization.actors.independent_reviewer_id
        ):
            raise ValueError("independent reviewer identity mismatch")
        if (
            evidence.requested_start
            != authorization.authorization.scope.requested_start
            or evidence.requested_end
            != authorization.authorization.scope.requested_end
        ):
            raise ValueError("requested range exceeds authorization")
    except QCCapabilityDiscoveryEvidenceContractError:
        raise
    except (OSError, ValueError) as exc:
        raise QCCapabilityDiscoveryEvidenceContractError(
            "QC_CAPABILITY_DISCOVERY_EVIDENCE_BINDING_INVALID",
            f"{resolved_evidence}: {exc}",
        ) from exc

    return QCCapabilityDiscoveryEvidenceLoadResult(
        evidence=evidence,
        evidence_path=resolved_evidence,
        evidence_file_sha256=sha256_path(resolved_evidence),
        evidence_canonical_sha256=evidence.canonical_sha256,
        authorization=authorization,
    )


def _require_bound_regular_file(
    path: Path,
    *,
    project_root: Path,
    field: str,
) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the project root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved
