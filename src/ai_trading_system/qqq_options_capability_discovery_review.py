from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.qc_qqq_options_capability_discovery_review import (
    QCCapabilityDiscoveryReview,
    QCCapabilityDiscoveryReviewContractError,
)
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_capability_discovery_evidence import (
    QCCapabilityDiscoveryEvidenceLoadResult,
    load_qc_qqq_options_capability_discovery_evidence,
)

DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH = Path(
    "inputs/external_validation/qc_qqq_options_capability_discovery_review_20260804.json"
)


@dataclass(frozen=True)
class QCCapabilityDiscoveryReviewLoadResult:
    review: QCCapabilityDiscoveryReview
    review_path: Path
    review_file_sha256: str
    review_canonical_sha256: str
    evidence: QCCapabilityDiscoveryEvidenceLoadResult


def load_qc_qqq_options_capability_discovery_review(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCCapabilityDiscoveryReviewLoadResult:
    resolved_root = project_root.resolve()
    resolved_review = resolved_root / path
    try:
        resolved_review = _require_bound_regular_file(
            path,
            project_root=resolved_root,
            field="capability-discovery review",
        )
        review = QCCapabilityDiscoveryReview.from_json_bytes(resolved_review.read_bytes())
        evidence = load_qc_qqq_options_capability_discovery_evidence(
            Path(review.evidence_path),
            project_root=resolved_root,
        )
        if review.evidence_file_sha256 != evidence.evidence_file_sha256:
            raise ValueError("reviewed evidence file SHA-256 mismatch")
        if review.evidence_semantic_sha256 != evidence.evidence.content_sha256:
            raise ValueError("reviewed evidence semantic SHA-256 mismatch")
        if review.collector_id != evidence.evidence.collector_id:
            raise ValueError("review collector identity mismatch")
        if review.reviewer_id != evidence.evidence.independent_reviewer_id:
            raise ValueError("independent reviewer identity mismatch")
        if review.project_id != evidence.evidence.project_id:
            raise ValueError("review project identity mismatch")
        if review.backtest_id != evidence.evidence.backtest_id:
            raise ValueError("review backtest identity mismatch")
        if review.page_assertions.project_code_sha256 != evidence.evidence.project_code_sha256:
            raise ValueError("reviewed project-code SHA-256 mismatch")
        if review.page_assertions.build_id != evidence.evidence.build_id:
            raise ValueError("reviewed build identity mismatch")
        if review.page_assertions.cloud_compute != evidence.evidence.cloud_compute:
            raise ValueError("reviewed cloud compute mismatch")
        if review.page_assertions.account_tier != evidence.evidence.account_tier:
            raise ValueError("reviewed account tier mismatch")
        if review.page_assertions.deployment_seconds != evidence.evidence.deployment_seconds:
            raise ValueError("reviewed deployment duration mismatch")
    except QCCapabilityDiscoveryReviewContractError:
        raise
    except (OSError, ValueError) as exc:
        raise QCCapabilityDiscoveryReviewContractError(
            "QC_CAPABILITY_DISCOVERY_REVIEW_BINDING_INVALID",
            f"{resolved_review}: {exc}",
        ) from exc

    return QCCapabilityDiscoveryReviewLoadResult(
        review=review,
        review_path=resolved_review,
        review_file_sha256=sha256_path(resolved_review),
        review_canonical_sha256=review.canonical_sha256,
        evidence=evidence,
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
