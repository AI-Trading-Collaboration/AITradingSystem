from __future__ import annotations

import copy
import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.license_export_due_diligence import (
    LicenseAssessmentAxis,
)
from ai_trading_system.qqq_options_research.license_export_owner_review import (
    DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
    EXPECTED_AXIS_RECOMMENDATIONS,
    EXPECTED_EVIDENCE,
    EXPECTED_LISTING_FACTS,
    EXPECTED_OWNER_REVIEW_CHECKS,
    QCQQQOptionsLicenseExportOwnerReviewContractError,
    QCQQQOptionsLicenseExportOwnerReviewPolicy,
    QCQQQOptionsLicenseExportOwnerReviewProposal,
    build_qc_qqq_options_license_export_owner_review_proposal,
    load_qc_qqq_options_license_export_owner_review_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

_REPOSITORY_CODE_SHA = "1883c91535cb18bd8b9ef1efbd6b1d6be1fe5a0b"
_CREATED_AT = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
_PREDECESSOR_PATH = Path(
    "inputs/external_validation/qc_qqq_options_license_export_due_diligence_report_20260807.json"
)
_PREDECESSOR_FILE_SHA256 = "5e8063754bae6e9e4cb3cca02dacd064e3ce368a1cdba9612df707a83ed48e80"


def _policy_payload() -> dict[str, object]:
    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH
    )
    assert isinstance(payload, dict)
    return copy.deepcopy(payload)


def _proposal(
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportOwnerReviewProposal:
    return build_qc_qqq_options_license_export_owner_review_proposal(
        record_id="qc_qqq_options_license_export_owner_review_proposal_20260807_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha=_REPOSITORY_CODE_SHA,
        project_root=project_root,
    )


def _copy_authority(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    for relative in (
        DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
        _PREDECESSOR_PATH,
    ):
        source = PROJECT_ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return root


def _write_policy(root: Path, payload: dict[str, object]) -> None:
    path = root / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
        newline="\n",
    )


def test_policy_loads_and_exact_binds_predecessor() -> None:
    loaded = load_qc_qqq_options_license_export_owner_review_policy()

    assert loaded.policy.predecessor_task_id == (
        "TRADING-2497_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_V1"
    )
    assert loaded.policy.predecessor_report_file_sha256 == _PREDECESSOR_FILE_SHA256
    assert loaded.policy_file_sha256
    assert loaded.policy_canonical_sha256
    assert loaded.evidence_set_sha256


def test_manual_evidence_is_hash_only_exact_and_contains_no_raw_rows() -> None:
    policy = load_qc_qqq_options_license_export_owner_review_policy().policy

    observed = tuple(
        (item.evidence_id, item.byte_count, item.sha256, item.role)
        for item in policy.manual_evidence
    )
    assert observed == EXPECTED_EVIDENCE
    assert all(
        item.retention_status == "HASH_ONLY_EXTERNAL_FILE_NOT_RETAINED_IN_REPOSITORY"
        for item in policy.manual_evidence
    )
    assert all(item.contains_raw_option_rows is False for item in policy.manual_evidence)


def test_listing_price_mapping_is_exact_and_observation_only() -> None:
    policy = load_qc_qqq_options_license_export_owner_review_policy().policy

    observed = tuple(
        (item.listing_id, item.display_value, item.evidence_ids) for item in policy.listing_facts
    )
    assert observed == EXPECTED_LISTING_FACTS
    assert (
        dict((item.listing_id, item.display_value) for item in policy.listing_facts)[
            "BULK_HOUR_DOWNLOAD"
        ]
        == "$14,400"
    )
    assert (
        dict((item.listing_id, item.display_value) for item in policy.listing_facts)[
            "BULK_DAILY_DOWNLOAD"
        ]
        == "$12,000"
    )
    assert all(
        item.interpretation == "OBSERVATION_ONLY_NOT_PURCHASE_OR_BUDGET_AUTHORITY"
        for item in policy.listing_facts
    )


def test_axis_recommendations_preserve_primary_and_shared_blockers() -> None:
    proposal = _proposal()
    observed = tuple((item.axis_id, item.recommendation) for item in proposal.axis_recommendations)

    assert observed == EXPECTED_AXIS_RECOMMENDATIONS
    recommendations = {item.axis_id: item.recommendation for item in proposal.axis_recommendations}
    assert (
        str(recommendations[LicenseAssessmentAxis.PRIMARY_WINDOW_HISTORICAL_RETENTION])
        == "NO_GO_NOT_TESTED_ACCOUNT_SPECIFIC"
    )
    assert proposal.aggregate_recommendation == (
        "NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"
    )
    assert proposal.primary_window_status == "NOT_TESTED_ACCOUNT_SPECIFIC"


def test_proposal_requires_owner_review_and_cannot_self_sign() -> None:
    proposal = _proposal()

    assert proposal.owner_review_checks == EXPECTED_OWNER_REVIEW_CHECKS
    assert proposal.owner_review_completed is False
    assert proposal.owner_attestation_present is False
    assert proposal.legal_opinion_provided is False
    assert proposal.safety.proposal_only is True
    assert proposal.safety.owner_signature_present is False


def test_proposal_preserves_primary_window_and_bounded_tested_session() -> None:
    proposal = _proposal()

    assert proposal.primary_research_window_start == date(2021, 2, 22)
    assert proposal.tested_session == date(2025, 12, 2)
    assert proposal.primary_window_status == "NOT_TESTED_ACCOUNT_SPECIFIC"


def test_proposal_preserves_no_external_action_and_no_purchase_boundary() -> None:
    safety = _proposal().safety

    assert safety.external_platform_action_authorized is False
    assert safety.quantconnect_login_authorized is False
    assert safety.project_mutation_authorized is False
    assert safety.cloud_backtest_authorized is False
    assert safety.api_cli_http_object_store_authorized is False
    assert safety.raw_options_download_authorized is False
    assert safety.purchase_or_subscription_authorized is False
    assert safety.range_expansion_authorized is False
    assert safety.paid_tier_upgrade_authorized is False
    assert safety.investment_interpretation_allowed is False
    assert safety.broker_action == "none"


def test_proposal_is_deterministic_canonical_and_replayable() -> None:
    first = _proposal()
    second = _proposal()

    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.canonical_sha256 == second.canonical_sha256
    assert (
        QCQQQOptionsLicenseExportOwnerReviewProposal.from_json_bytes(first.canonical_bytes) == first
    )


def test_proposal_rejects_noncanonical_and_semantic_tamper() -> None:
    proposal = _proposal()
    decoded = json.loads(proposal.canonical_bytes)
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsLicenseExportOwnerReviewProposal.from_json_bytes(
            json.dumps(decoded, sort_keys=False).encode("utf-8")
        )

    decoded["listing_facts"][0]["display_value"] = "Paid"
    tampered = (json.dumps(decoded, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    with pytest.raises(ValidationError, match="content_sha256 does not match"):
        QCQQQOptionsLicenseExportOwnerReviewProposal.from_json_bytes(tampered)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda payload: payload["manual_evidence"].reverse(), "manual evidence inventory"),
        (
            lambda payload: payload["manual_evidence"][0].update(byte_count=1),
            "manual evidence inventory",
        ),
        (
            lambda payload: payload["manual_evidence"][0].update(sha256="0" * 64),
            "manual evidence inventory",
        ),
    ),
)
def test_policy_rejects_evidence_reorder_byte_count_and_hash_tamper(
    mutation: object, message: str
) -> None:
    payload = _policy_payload()
    assert callable(mutation)
    mutation(payload)  # type: ignore[operator]

    with pytest.raises(ValidationError, match=message):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)


def test_policy_rejects_price_mapping_swap() -> None:
    payload = _policy_payload()
    facts = payload["listing_facts"]
    assert isinstance(facts, list)
    facts[7]["display_value"], facts[8]["display_value"] = (
        facts[8]["display_value"],
        facts[7]["display_value"],
    )

    with pytest.raises(ValidationError, match="price mapping drifted"):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)


def test_policy_rejects_entitlement_or_primary_window_promotion() -> None:
    payload = _policy_payload()
    recommendations = payload["axis_recommendations"]
    assert isinstance(recommendations, list)
    recommendations[1]["recommendation"] = "CONDITIONAL_GO_PUBLIC_FREE_AND_ACCOUNT_FREE"
    with pytest.raises(ValidationError, match="axis recommendation inventory"):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)

    payload = _policy_payload()
    payload["primary_research_window_start"] = "2022-12-01"
    with pytest.raises(ValidationError, match="must remain 2021-02-22"):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    (
        "external_platform_action_authorized",
        "raw_options_download_authorized",
        "purchase_or_subscription_authorized",
        "range_expansion_authorized",
        "paid_tier_upgrade_authorized",
    ),
)
def test_policy_rejects_external_purchase_download_or_expansion_authority(
    field: str,
) -> None:
    payload = _policy_payload()
    safety = payload["safety"]
    assert isinstance(safety, dict)
    safety[field] = True

    with pytest.raises(ValidationError, match="literal_error"):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)


def test_policy_rejects_retained_source_path_or_page_copy_field() -> None:
    payload = _policy_payload()
    evidence = payload["manual_evidence"]
    assert isinstance(evidence, list)
    evidence[0]["source_path"] = "C:/owner/screenshot.png"

    with pytest.raises(ValidationError, match="extra_forbidden"):
        QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)


def test_predecessor_report_exact_bytes_remain_unchanged() -> None:
    raw = (PROJECT_ROOT / _PREDECESSOR_PATH).read_bytes()

    assert hashlib.sha256(raw).hexdigest() == _PREDECESSOR_FILE_SHA256


def test_predecessor_report_hash_tamper_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    predecessor = root / _PREDECESSOR_PATH
    predecessor.write_bytes(predecessor.read_bytes() + b" ")

    with pytest.raises(
        QCQQQOptionsLicenseExportOwnerReviewContractError,
        match="predecessor report file SHA-256 mismatch",
    ):
        load_qc_qqq_options_license_export_owner_review_policy(project_root=root)


def test_policy_path_escape_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    payload = _policy_payload()
    payload["predecessor_report_relative_path"] = "../outside.json"
    _write_policy(root, payload)

    with pytest.raises(
        QCQQQOptionsLicenseExportOwnerReviewContractError,
        match="repository-relative",
    ):
        load_qc_qqq_options_license_export_owner_review_policy(project_root=root)
