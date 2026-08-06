from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.license_export_due_diligence import (
    DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH,
    EXPECTED_AXIS_STATUSES,
    EXPECTED_CLAIM_IDS,
    EXPECTED_SOURCE_IDS,
    EXPECTED_SOURCE_URLS,
    LicenseAssessmentAxis,
    LicenseAssessmentStatus,
    LicenseClaimClassification,
    QCQQQOptionsLicenseExportDueDiligenceContractError,
    QCQQQOptionsLicenseExportDueDiligencePolicy,
    QCQQQOptionsLicenseExportDueDiligenceReport,
    build_qc_qqq_options_license_export_due_diligence_report,
    load_qc_qqq_options_license_export_due_diligence_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

_BASE_SHA = "967d3524876b34c11ee8235b2913ba841cf94b36"
_CREATED_AT = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
_AUTHORITY_PATHS = (
    "inputs/external_validation/qc_qqq_options_owner_stage_gate_owner_attestation_20260806.json",
    "inputs/external_validation/qc_qqq_options_owner_stage_gate_signoff_20260806.json",
)


def _report(*, project_root: Path = PROJECT_ROOT) -> QCQQQOptionsLicenseExportDueDiligenceReport:
    return build_qc_qqq_options_license_export_due_diligence_report(
        record_id="qc_qqq_options_license_export_due_diligence_report_20260807_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha=_BASE_SHA,
        project_root=project_root,
    )


def _copy_project_authority(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    paths = (
        DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH.as_posix(),
    ) + _AUTHORITY_PATHS
    for relative in paths:
        source = PROJECT_ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return root


def test_policy_loads_and_exact_binds_trading_2493_authority() -> None:
    loaded = load_qc_qqq_options_license_export_due_diligence_policy()

    assert tuple(item.authority_id for item in loaded.policy.authority_bindings) == (
        "TRADING_2493_OWNER_ATTESTATION",
        "TRADING_2493_SIGNOFF",
    )
    assert loaded.policy_sha256
    assert loaded.policy_canonical_sha256
    assert loaded.authority_set_sha256


def test_official_source_inventory_is_exact_metadata_only() -> None:
    policy = load_qc_qqq_options_license_export_due_diligence_policy().policy

    assert tuple(item.source_id for item in policy.sources) == EXPECTED_SOURCE_IDS
    assert tuple(item.url for item in policy.sources) == EXPECTED_SOURCE_URLS
    assert all(item.retrieved_on == date(2026, 8, 7) for item in policy.sources)
    assert all(
        item.capture_mode == "PUBLIC_REFERENCE_METADATA_ONLY_NO_PAGE_COPY"
        for item in policy.sources
    )
    assert all(
        item.source_content_checksum_status == "NOT_CAPTURED_AUTOMATION_PROHIBITED"
        for item in policy.sources
    )
    assert all(item.source_content_sha256 is None for item in policy.sources)
    assert all(
        item.manual_review_status == "PENDING_MANUAL_OWNER_REVIEW" for item in policy.sources
    )


def test_claims_separate_fact_inference_and_unknown() -> None:
    policy = load_qc_qqq_options_license_export_due_diligence_policy().policy

    assert tuple(item.claim_id for item in policy.claims) == EXPECTED_CLAIM_IDS
    classifications = {item.classification for item in policy.claims}
    assert classifications == {
        LicenseClaimClassification.DOCUMENTED_FACT,
        LicenseClaimClassification.CONSERVATIVE_INFERENCE,
        LicenseClaimClassification.EXPLICIT_UNKNOWN,
    }
    assert all(item.owner and item.exit_condition for item in policy.claims)


def test_assessment_axes_are_exact_and_aggregate_remains_no_go() -> None:
    report = _report()

    assert tuple((item.axis_id, item.status) for item in report.assessments) == (
        EXPECTED_AXIS_STATUSES
    )
    assert report.aggregate_decision == "LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"
    assert report.owner_review_status == "PENDING_MANUAL_OWNER_REVIEW"
    assert report.legal_opinion_provided is False


def test_conditional_cloud_and_derived_exports_cannot_promote_entitlement() -> None:
    report = _report()
    statuses = {item.axis_id: item.status for item in report.assessments}

    assert statuses[LicenseAssessmentAxis.FREE_CLOUD_DATA_CLASS_ACCESS] == (
        LicenseAssessmentStatus.PUBLIC_DOCS_CONDITIONAL_SUPPORT
    )
    assert statuses[LicenseAssessmentAxis.DERIVED_BACKTEST_RESULT_EXPORT] == (
        LicenseAssessmentStatus.CONDITIONAL_DOCUMENTED_UI_EXPORT_ONLY
    )
    assert statuses[LicenseAssessmentAxis.QQQ_OPTIONS_ACCOUNT_ENTITLEMENT] == (
        LicenseAssessmentStatus.UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED
    )
    assert report.aggregate_decision == "LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"


def test_provider_start_does_not_promote_primary_window_retention() -> None:
    report = _report()
    statuses = {item.axis_id: item.status for item in report.assessments}

    assert report.primary_research_window_start == date(2021, 2, 22)
    assert report.research_run_performed is False
    assert report.requested_range is None
    assert report.evaluated_range is None
    assert statuses[LicenseAssessmentAxis.PRIMARY_WINDOW_HISTORICAL_RETENTION] == (
        LicenseAssessmentStatus.UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED
    )


def test_raw_download_redistribution_and_api_cli_remain_no_go() -> None:
    report = _report()
    statuses = {item.axis_id: item.status for item in report.assessments}

    assert statuses[LicenseAssessmentAxis.RAW_OPTIONS_LOCAL_DOWNLOAD] == (
        LicenseAssessmentStatus.NO_GO_SEPARATE_DOWNLOAD_LICENSE_REQUIRED
    )
    assert statuses[LicenseAssessmentAxis.RAW_OPTIONS_REDISTRIBUTION] == (
        LicenseAssessmentStatus.NO_GO_PROHIBITED
    )
    assert statuses[LicenseAssessmentAxis.API_CLI_ACCESS] == (
        LicenseAssessmentStatus.NO_GO_CURRENT_FREE_TIER
    )


def test_report_preserves_dq_pit_and_no_external_action_safety() -> None:
    report = _report()

    assert report.option_event_dq_status == "NOT_EVALUATED"
    assert report.option_event_pit_status == "NOT_EVALUATED"
    assert report.safety.quantconnect_login_performed is False
    assert report.safety.cloud_backtest_performed is False
    assert report.safety.project_mutation_performed is False
    assert report.safety.api_cli_http_object_store_used is False
    assert report.safety.raw_options_data_downloaded is False
    assert report.safety.range_expansion_allowed is False
    assert report.safety.paid_tier_upgrade_authorized is False
    assert report.safety.investment_interpretation_allowed is False
    assert report.safety.broker_action == "none"


def test_report_is_deterministic_canonical_and_replayable() -> None:
    first = _report()
    second = _report()

    assert first.canonical_bytes == second.canonical_bytes
    assert first.canonical_sha256 == second.canonical_sha256
    assert first.content_sha256 == second.content_sha256
    assert (
        QCQQQOptionsLicenseExportDueDiligenceReport.from_json_bytes(first.canonical_bytes) == first
    )


def test_report_rejects_noncanonical_and_semantic_hash_tamper() -> None:
    report = _report()
    decoded = json.loads(report.canonical_bytes)
    noncanonical = json.dumps(decoded, sort_keys=False).encode("utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsLicenseExportDueDiligenceReport.from_json_bytes(noncanonical)

    decoded["content_sha256"] = "0" * 64
    tampered = (
        json.dumps(
            decoded,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    with pytest.raises(ValueError, match="semantic content SHA-256 mismatch"):
        QCQQQOptionsLicenseExportDueDiligenceReport.from_json_bytes(tampered)


def test_forged_aggregate_or_source_checksum_fails_closed() -> None:
    payload = _report().model_dump(mode="json")
    payload["aggregate_decision"] = "LICENSE_EXPORT_PASS"
    with pytest.raises(ValidationError, match="literal_error"):
        QCQQQOptionsLicenseExportDueDiligenceReport.model_validate(payload)

    payload = _report().model_dump(mode="json")
    payload["sources"][0]["source_content_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="none_required"):
        QCQQQOptionsLicenseExportDueDiligenceReport.model_validate(payload)


def test_policy_rejects_source_reorder_unknown_host_and_missing_reviewer() -> None:
    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    )
    assert isinstance(payload, dict)
    payload["sources"] = list(reversed(payload["sources"]))
    with pytest.raises(ValidationError, match="source inventory or URL drifted"):
        QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)

    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    )
    assert isinstance(payload, dict)
    payload["sources"][0]["url"] = "https://example.com/licensing"
    with pytest.raises(ValidationError, match="official QuantConnect HTTPS host"):
        QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)

    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    )
    assert isinstance(payload, dict)
    del payload["sources"][0]["manual_reviewer"]
    with pytest.raises(ValidationError, match="Field required"):
        QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)


def test_policy_rejects_account_entitlement_pass_and_primary_window_promotion() -> None:
    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    )
    assert isinstance(payload, dict)
    payload["assessments"][1]["status"] = "PUBLIC_DOCS_CONDITIONAL_SUPPORT"
    with pytest.raises(ValidationError, match="assessment inventory or status drifted"):
        QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)

    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    )
    assert isinstance(payload, dict)
    payload["primary_research_window_start"] = "2022-12-01"
    with pytest.raises(ValidationError, match="must remain 2021-02-22"):
        QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)


def test_policy_authority_hash_tamper_fails_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    signoff = root / _AUTHORITY_PATHS[1]
    signoff.write_bytes(signoff.read_bytes() + b" ")

    with pytest.raises(
        QCQQQOptionsLicenseExportDueDiligenceContractError,
        match="TRADING_2493_SIGNOFF SHA-256 mismatch",
    ):
        load_qc_qqq_options_license_export_due_diligence_policy(project_root=root)


def test_policy_path_escape_and_extra_field_fail_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH
    original = policy_path.read_text(encoding="utf-8")
    escaped = original.replace(
        _AUTHORITY_PATHS[0],
        "../outside.json",
    )
    policy_path.write_text(escaped, encoding="utf-8", newline="\n")
    with pytest.raises(QCQQQOptionsLicenseExportDueDiligenceContractError, match="normalized"):
        load_qc_qqq_options_license_export_due_diligence_policy(project_root=root)

    policy_path.write_text(original + "unexpected: true\n", encoding="utf-8", newline="\n")
    with pytest.raises(QCQQQOptionsLicenseExportDueDiligenceContractError, match="extra_forbidden"):
        load_qc_qqq_options_license_export_due_diligence_policy(project_root=root)


def test_symlink_authority_fails_closed_when_supported(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    signoff = root / _AUTHORITY_PATHS[1]
    target = signoff.with_name("signoff_target.json")
    signoff.replace(target)
    try:
        signoff.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(QCQQQOptionsLicenseExportDueDiligenceContractError, match="symlink"):
        load_qc_qqq_options_license_export_due_diligence_policy(project_root=root)


def test_predecessor_signed_no_go_is_preserved_in_report() -> None:
    report = _report()

    assert report.predecessor_owner_attestation_file_sha256 == (
        "9b1592289b579dacb0608aeb18d73aac940ad92795484c2377f7f6e8ba2f4aa6"
    )
    assert report.predecessor_signoff_file_sha256 == (
        "dd9c9332d57e48de7541ca316a4b64594b1ecf03f0910551f1e63a4e60174d02"
    )
    assert report.predecessor_signoff_status == "SIGNED_NO_GO"
    assert report.predecessor_aggregate_decision == "NO_GO_KEEP_BLOCKED"
    assert report.predecessor_license_export_axis == "NO_GO"
