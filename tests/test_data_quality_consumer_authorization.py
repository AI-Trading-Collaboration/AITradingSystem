from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_consumer_authorization import (
    DataQualityConsumerAuthorizationAttestation,
    DataQualityConsumerAuthorizationContractError,
    VerifiedDataQualityConsumerAuthorization,
)
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    DataQualityExecutionReceipt,
    DataQualityImplementationSourceBinding,
    DataQualityInputBinding,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    VerifiedDataQualityPreflight,
    _build_verified_data_quality_preflight,
)
from ai_trading_system.contracts.status import PolicyRole
from ai_trading_system.data.download_publication import ValidatedDownloadPublication
from ai_trading_system.data.quality_consumer_authorization import (
    DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    DataQualityConsumerAuthorizationError,
    build_data_quality_consumer_authorization_attestation,
    load_reviewed_data_quality_consumer_authorization_policy,
    verify_data_quality_consumer_authorization,
    write_data_quality_consumer_authorization_attestation,
)

AS_OF = date(2026, 7, 23)
START = date(2021, 2, 22)
AUTHORIZED_AT = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def test_reviewed_profile_is_exact_single_consumer_strict_pass_policy() -> None:
    policy = load_reviewed_data_quality_consumer_authorization_policy()

    assert policy.consumer_id == "daily_score_daily"
    assert policy.consumer_version == "1.0.0"
    assert policy.accepted_data_quality_statuses == ("PASS",)
    assert policy.required_input_roles == ("prices", "rates", "secondary_prices")
    assert policy.authorization_ttl_hours == 24
    assert policy.publication_output_dir == "data/raw"


def test_attestation_round_trip_binds_receipt_publication_and_keeps_legacy_false(
    tmp_path: Path,
) -> None:
    root = _project_root(tmp_path)
    preflight = _preflight()
    publication = _publication(root)
    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)

    attestation = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=AUTHORIZED_AT,
        project_root=root,
    )
    parsed = DataQualityConsumerAuthorizationAttestation.from_json_bytes(
        attestation.canonical_bytes
    )

    assert parsed == attestation
    assert attestation.authorization_id.startswith("dq_consumer_authorization_")
    assert attestation.consumer_id == "daily_score_daily"
    assert attestation.receipt_id == preflight.receipt_id
    assert attestation.receipt_status == "PASS"
    assert attestation.publication_transaction_id == publication.transaction_id
    assert attestation.expires_at == AUTHORIZED_AT + timedelta(hours=24)
    assert attestation.consumer_dispatch_authorized is True
    assert attestation.generic_consumer_cutover_allowed is False
    assert attestation.automatic_non_daily_dispatch is False
    assert preflight.receipt.consumer_cutover_allowed is False
    assert publication.consumer_cutover_allowed is False


def test_write_and_verify_returns_unforgeable_consumer_capability(tmp_path: Path) -> None:
    root = _project_root(tmp_path)
    preflight = _preflight()
    publication = _publication(root)
    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)
    attestation = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=AUTHORIZED_AT,
        project_root=root,
    )
    path = write_data_quality_consumer_authorization_attestation(attestation, project_root=root)
    receipt_calls: list[tuple[Path, date, tuple[str, ...]]] = []
    publication_calls: list[Path] = []

    def receipt_verifier(
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path,
    ) -> VerifiedDataQualityPreflight:
        receipt_calls.append((receipt_path, expected_as_of, expected_input_roles))
        assert project_root == root
        assert expected_policy_path == Path("config/data_quality.yaml")
        return preflight

    def publication_resolver(*, output_dir: Path) -> ValidatedDownloadPublication:
        publication_calls.append(output_dir)
        return publication

    verified = verify_data_quality_consumer_authorization(
        path,
        expected_consumer_id="daily_score_daily",
        expected_consumer_version="1.0.0",
        expected_as_of=AS_OF,
        expected_data_quality_policy_path=Path("config/data_quality.yaml"),
        receipt_verifier=receipt_verifier,
        publication_resolver=publication_resolver,
        now=AUTHORIZED_AT + timedelta(hours=1),
        project_root=root,
    )

    assert isinstance(verified, VerifiedDataQualityConsumerAuthorization)
    assert verified.consumer_id == "daily_score_daily"
    assert verified.authorization_id == attestation.authorization_id
    assert receipt_calls == [
        (
            Path(preflight.receipt_path),
            AS_OF,
            ("prices", "rates", "secondary_prices"),
        )
    ]
    assert publication_calls == [root / "data/raw"]
    with pytest.raises(
        DataQualityConsumerAuthorizationContractError,
        match="DQ_CONSUMER_AUTHORIZATION_NOT_VERIFIED",
    ):
        VerifiedDataQualityConsumerAuthorization(
            attestation=attestation,
            preflight=preflight,
            verified_at=AUTHORIZED_AT,
            _verification_seal=object(),
        )


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("expired", "DQ_CONSUMER_AUTHORIZATION_EXPIRED"),
        ("consumer", "DQ_CONSUMER_MISMATCH"),
        ("as_of", "DQ_AS_OF_MISMATCH"),
        ("profile", "DQ_CONSUMER_PROFILE_MISMATCH"),
        ("publication", "DQ_CONSUMER_AUTHORIZATION_LINEAGE_MISMATCH"),
        ("receipt", "DQ_CONSUMER_AUTHORIZATION_LINEAGE_MISMATCH"),
    ],
)
def test_verifier_fails_closed_for_expiry_identity_profile_and_lineage_drift(
    tmp_path: Path,
    case: str,
    expected_code: str,
) -> None:
    root = _project_root(tmp_path)
    preflight = _preflight()
    publication = _publication(root)
    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)
    attestation = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=AUTHORIZED_AT,
        project_root=root,
    )
    path = write_data_quality_consumer_authorization_attestation(attestation, project_root=root)
    expected_consumer_id = "another_consumer" if case == "consumer" else "daily_score_daily"
    expected_consumer_version = "2.0.0" if case == "profile" else "1.0.0"
    expected_as_of = AS_OF - timedelta(days=1) if case == "as_of" else AS_OF
    now = (
        AUTHORIZED_AT + timedelta(hours=24)
        if case == "expired"
        else AUTHORIZED_AT + timedelta(hours=1)
    )
    live_preflight = _preflight(report_sha="e" * 64) if case == "receipt" else preflight
    live_publication = (
        replace(publication, discovery_pointer_sha256="e" * 64)
        if case == "publication"
        else publication
    )

    with pytest.raises(DataQualityConsumerAuthorizationError) as error:
        verify_data_quality_consumer_authorization(
            path,
            expected_consumer_id=expected_consumer_id,
            expected_consumer_version=expected_consumer_version,
            expected_as_of=expected_as_of,
            expected_data_quality_policy_path=Path("config/data_quality.yaml"),
            receipt_verifier=lambda *args, **kwargs: live_preflight,
            publication_resolver=lambda **kwargs: live_publication,
            now=now,
            project_root=root,
        )

    assert error.value.code == expected_code


def test_warning_receipt_and_publication_source_mismatch_fail_before_authorization(
    tmp_path: Path,
) -> None:
    root = _project_root(tmp_path)
    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)
    warning = _preflight(status="PASS_WITH_WARNINGS", warning_count=1)

    with pytest.raises(DataQualityConsumerAuthorizationError) as warning_error:
        build_data_quality_consumer_authorization_attestation(
            policy=policy,
            preflight=warning,
            publication=_publication(root),
            authorized_at=AUTHORIZED_AT,
            project_root=root,
        )
    assert warning_error.value.code == "DQ_WARNING_NOT_ALLOWED"

    mismatched = replace(
        _publication(root),
        legacy_prices_path=root / "data/raw/different_prices.csv",
    )
    with pytest.raises(DataQualityConsumerAuthorizationError) as source_error:
        build_data_quality_consumer_authorization_attestation(
            policy=policy,
            preflight=_preflight(),
            publication=mismatched,
            authorized_at=AUTHORIZED_AT,
            project_root=root,
        )
    assert source_error.value.code == "DQ_PUBLICATION_SOURCE_MISMATCH"


def test_attestation_tamper_and_policy_byte_drift_are_rejected(tmp_path: Path) -> None:
    root = _project_root(tmp_path)
    preflight = _preflight()
    publication = _publication(root)
    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)
    attestation = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=AUTHORIZED_AT,
        project_root=root,
    )
    path = write_data_quality_consumer_authorization_attestation(attestation, project_root=root)
    original = path.read_bytes()
    path.write_bytes(original.replace(b'"receipt_status": "PASS"', b'"receipt_status": "FAIL"'))

    with pytest.raises(DataQualityConsumerAuthorizationError) as tamper_error:
        verify_data_quality_consumer_authorization(
            path,
            expected_consumer_id="daily_score_daily",
            expected_consumer_version="1.0.0",
            expected_as_of=AS_OF,
            expected_data_quality_policy_path=Path("config/data_quality.yaml"),
            receipt_verifier=lambda *args, **kwargs: preflight,
            publication_resolver=lambda **kwargs: publication,
            now=AUTHORIZED_AT + timedelta(hours=1),
            project_root=root,
        )
    assert tamper_error.value.code in {
        "DQ_WARNING_NOT_ALLOWED",
        "DQ_CONSUMER_AUTHORIZATION_ID_MISMATCH",
    }

    path.write_bytes(original)
    policy_path = root / DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8") + "\n# reviewed byte drift\n",
        encoding="utf-8",
    )
    with pytest.raises(DataQualityConsumerAuthorizationError) as policy_error:
        verify_data_quality_consumer_authorization(
            path,
            expected_consumer_id="daily_score_daily",
            expected_consumer_version="1.0.0",
            expected_as_of=AS_OF,
            expected_data_quality_policy_path=Path("config/data_quality.yaml"),
            receipt_verifier=lambda *args, **kwargs: preflight,
            publication_resolver=lambda **kwargs: publication,
            now=AUTHORIZED_AT + timedelta(hours=1),
            project_root=root,
        )
    assert policy_error.value.code == "DQ_CONSUMER_AUTHORIZATION_LINEAGE_MISMATCH"


def _project_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    source = Path(DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH)
    target = root / source
    target.parent.mkdir(parents=True)
    target.write_bytes(source.read_bytes())
    return root


def _preflight(
    *,
    status: str = "PASS",
    warning_count: int = 0,
    report_sha: str = SHA_C,
) -> VerifiedDataQualityPreflight:
    receipt = _receipt(
        status=status,
        warning_count=warning_count,
        report_sha=report_sha,
    )
    return _build_verified_data_quality_preflight(
        receipt=receipt,
        receipt_path=f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json",
        receipt_sha256=receipt.canonical_sha256,
        receipt_size_bytes=len(receipt.canonical_bytes),
        verified_at=datetime(2026, 7, 23, 10, 0, tzinfo=UTC),
    )


def _receipt(
    *,
    status: str,
    warning_count: int,
    report_sha: str,
) -> DataQualityExecutionReceipt:
    policy = DataQualityPolicyBinding(
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v1",
        status="REVIEWED",
        owner="data_platform_owner",
        role=PolicyRole.DATA_QUALITY,
        path="config/data_quality.yaml",
        sha256=SHA_A,
    )
    report = DataQualityReportBinding(
        path="outputs/data_quality/reports/report/data_quality_report.md",
        sha256=report_sha,
        size_bytes=123,
        status=status,
        error_count=0,
        warning_count=warning_count,
        info_count=0,
        issue_codes=() if warning_count == 0 else ("DQ_WARNING",),
        blocking_issue_codes=(),
    )
    evidence = DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        status=status,
        passed=True,
        checked_at=datetime(2026, 7, 23, 9, 0, tzinfo=UTC),
        as_of=AS_OF,
        report_path=report.path,
        report_sha256=report.sha256,
        warning_count=warning_count,
        checked_input_count=3,
    )
    inputs = tuple(
        DataQualityInputBinding(
            role=role,
            path=f"data/raw/{filename}",
            exists=True,
            schema_id=f"{role}.v1",
            source_role=f"source_{role}",
            sha256=digest,
            size_bytes=100,
            row_count=10,
            manifest_path="data/raw/download_manifest.csv",
            manifest_sha256=SHA_D,
            matched_source_ids=(source,),
            matched_record_refs=(f"manifest_record_{digest[:16]}",),
        )
        for role, filename, digest, source in (
            ("prices", "prices_daily.csv", SHA_A, "stooq"),
            ("rates", "rates_daily.csv", SHA_B, "fred"),
            (
                "secondary_prices",
                "prices_marketstack_daily.csv",
                SHA_C,
                "marketstack",
            ),
        )
    )
    return DataQualityExecutionReceipt(
        run_id="dq-run-wave15",
        contract_id=evidence.contract_id,
        started_at=datetime(2026, 7, 23, 8, 59, tzinfo=UTC),
        ended_at=datetime(2026, 7, 23, 9, 1, tzinfo=UTC),
        checked_at=evidence.checked_at,
        as_of=AS_OF,
        requested_window=DataQualityDateWindow(START, AS_OF),
        evaluated_window=DataQualityDateWindow(START, AS_OF),
        policy=policy,
        validator=DataQualityValidatorBinding(
            validator_id="aits.validate-data",
            validator_version="quality_execution.v1",
            entrypoint=(
                "ai_trading_system.data.quality_execution:" "run_canonical_data_quality_execution"
            ),
            implementation_sources=(
                DataQualityImplementationSourceBinding(
                    path="src/ai_trading_system/data/quality_execution.py",
                    sha256=SHA_B,
                ),
            ),
        ),
        invocation=(
            DataQualityInvocationParameter.from_value("execution_profile_id", "daily_default.v1"),
            DataQualityInvocationParameter.from_value(
                "requested_window", {"start": START.isoformat(), "end": AS_OF.isoformat()}
            ),
        ),
        inputs=inputs,
        report=report,
        data_quality_evidence=evidence,
    )


def _publication(root: Path) -> ValidatedDownloadPublication:
    raw = root / "data/raw"
    transaction = raw / ".download_publications/generations/txn/transaction.json"
    pointer = raw / ".download_publications/current/download_composite.json"
    return ValidatedDownloadPublication(
        transaction_id="download_publication_" + "f" * 64,
        transaction_manifest_path=transaction,
        transaction_manifest_sha256=SHA_A,
        discovery_pointer_path=pointer,
        discovery_pointer_sha256=SHA_B,
        prices_path=raw / ".download_publications/members/txn/prices_daily.csv",
        rates_path=raw / ".download_publications/members/txn/rates_daily.csv",
        manifest_path=raw / ".download_publications/members/txn/download_manifest.csv",
        secondary_prices_path=(
            raw / ".download_publications/members/txn/prices_marketstack_daily.csv"
        ),
        legacy_prices_path=raw / "prices_daily.csv",
        legacy_rates_path=raw / "rates_daily.csv",
        legacy_manifest_path=raw / "download_manifest.csv",
        legacy_secondary_prices_path=raw / "prices_marketstack_daily.csv",
        requested_start=START,
        requested_end=AS_OF,
        artifact_sha256={
            "prices": SHA_A,
            "rates": SHA_B,
            "secondary_prices": SHA_C,
        },
        artifact_row_count={"prices": 10, "rates": 10, "secondary_prices": 10},
        manifest_sha256=SHA_D,
        manifest_row_count=3,
        legacy_projection_verified=True,
        consumer_cutover_allowed=False,
        production_effect="none",
    )
