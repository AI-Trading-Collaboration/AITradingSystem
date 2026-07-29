from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.data import foundation_consumer_migration as migration
from ai_trading_system.data.immutable_publish import (
    PUBLICATION_DURABILITY_PROTOCOL_VERSION,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes


def _copy_manifest(candidate_root: Path) -> dict[str, object]:
    candidate_file = candidate_root / "data" / "raw" / "prices_daily.csv"
    candidate_file.parent.mkdir(parents=True)
    content = b"ticker,date,close\nQQQ,2026-07-27,100\n"
    candidate_file.write_bytes(content)
    body: dict[str, object] = {
        "schema_version": migration.CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION,
        "generated_at": "2026-07-29T04:54:00+00:00",
        "source_project_root": "D:/source-runtime",
        "candidate_project_root": candidate_root.resolve().as_posix(),
        "candidate_data_root": (candidate_root / "data" / "raw").resolve().as_posix(),
        "selected_publication": {"transaction_id": "download_txn_test"},
        "historical_receipt_id": "dq_execution_test",
        "historical_authorization_id": "dq_consumer_authorization_test",
        "durability_protocol_version": PUBLICATION_DURABILITY_PROTOCOL_VERSION,
        "filesystem_profile": {"supported": True},
        "copied_objects": [
            {
                "source_path": "generated:data/raw/prices_daily.csv",
                "candidate_path": "data/raw/prices_daily.csv",
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
            }
        ],
        "copy_count": 1,
        "all_objects_checksum_verified": True,
        "production_effect": "none",
        "broker_action": "none",
    }
    digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    return {"copy_manifest_id": f"consumer_copy_{digest[:32]}", **body}


def test_reviewed_policy_is_exactly_daily_score_and_non_production() -> None:
    policy = migration.load_consumer_migration_policy(
        project_root=Path.cwd(),
    )

    assert policy.consumer_id == "daily_score_daily"
    assert policy.consumer_version == "1.0.0"
    assert policy.execution_profile_id == "daily_default.v1"
    assert policy.accepted_data_quality_statuses == ("PASS",)
    assert policy.required_input_roles == ("prices", "rates", "secondary_prices")
    assert policy.retained_for_revalidation is True


def test_reviewed_policy_rejects_warning_acceptance(tmp_path: Path) -> None:
    relative = Path("config/data/data_foundation_consumer_migration.yaml")
    target = tmp_path / relative
    target.parent.mkdir(parents=True)
    raw = relative.read_text(encoding="utf-8")
    target.write_text(
        raw.replace(
            "accepted_data_quality_statuses: [PASS]",
            "accepted_data_quality_statuses: [PASS, WARNING]",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_POLICY_INVALID",
    ):
        migration.load_consumer_migration_policy(project_root=tmp_path)


def test_publication_store_relative_normalizes_exact_store_prefix() -> None:
    policy = migration.load_consumer_migration_policy(project_root=Path.cwd())

    assert (
        migration._publication_store_relative(
            ".download_publications/transactions/download_txn_test/transaction.json",
            policy=policy,
            field="immutable_path",
        )
        == "transactions/download_txn_test/transaction.json"
    )

    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID",
    ):
        migration._publication_store_relative(
            "transactions/download_txn_test/transaction.json",
            policy=policy,
            field="immutable_path",
        )


def test_candidate_copy_manifest_detects_candidate_byte_tamper(
    tmp_path: Path,
) -> None:
    candidate_root = tmp_path / "candidate"
    manifest = _copy_manifest(candidate_root)
    migration.validate_candidate_copy_manifest(
        manifest,
        candidate_project_root=candidate_root,
    )

    (candidate_root / "data" / "raw" / "prices_daily.csv").write_text(
        "tampered",
        encoding="utf-8",
    )

    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_COPY_TAMPERED",
    ):
        migration.validate_candidate_copy_manifest(
            manifest,
            candidate_project_root=candidate_root,
        )


def test_historical_row_lineage_rejects_duplicate_keys() -> None:
    raw = (
        b"ticker,date,close\n"
        b"QQQ,2026-07-27,100\n"
        b"QQQ,2026-07-27,101\n"
    )

    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT",
    ):
        migration._artifact_row_keys(raw, role="prices")


def test_verified_capability_cannot_be_forged(tmp_path: Path) -> None:
    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_CAPABILITY_FORGED",
    ):
        migration.VerifiedConsumerMigration(
            _token=object(),
            attestation_id="consumer_migration_test",
            consumer_id="daily_score_daily",
            consumer_version="1.0.0",
            candidate_project_root=tmp_path,
            candidate_data_root=tmp_path / "data" / "raw",
            receipt_id="dq_execution_test",
            authorization_id="dq_consumer_authorization_test",
            verified_at=datetime(2026, 7, 29, tzinfo=UTC),
        )


def test_dispatch_invokes_runner_once_for_exact_verified_consumer(
    tmp_path: Path,
) -> None:
    verified = migration.VerifiedConsumerMigration(
        _token=migration._VERIFIED_TOKEN,
        attestation_id="consumer_migration_test",
        consumer_id="daily_score_daily",
        consumer_version="1.0.0",
        candidate_project_root=tmp_path,
        candidate_data_root=tmp_path / "data" / "raw",
        receipt_id="dq_execution_test",
        authorization_id="dq_consumer_authorization_test",
        verified_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    calls: list[str] = []

    def _runner(capability: migration.VerifiedConsumerMigration) -> str:
        calls.append(capability.attestation_id)
        return "ok"

    result = migration.dispatch_isolated_daily_score_consumer(
        verified,
        runner=_runner,
    )

    assert result == "ok"
    assert calls == ["consumer_migration_test"]


def test_dispatch_blocks_wrong_consumer_before_runner(tmp_path: Path) -> None:
    verified = migration.VerifiedConsumerMigration(
        _token=migration._VERIFIED_TOKEN,
        attestation_id="consumer_migration_test",
        consumer_id="weekly_backtest",
        consumer_version="1.0.0",
        candidate_project_root=tmp_path,
        candidate_data_root=tmp_path / "data" / "raw",
        receipt_id="dq_execution_test",
        authorization_id="dq_consumer_authorization_test",
        verified_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    calls: list[str] = []

    with pytest.raises(
        migration.DataFoundationConsumerMigrationError,
        match="CONSUMER_MIGRATION_CONSUMER_MISMATCH",
    ):
        migration.dispatch_isolated_daily_score_consumer(
            verified,
            runner=lambda capability: calls.append(capability.attestation_id),
        )

    assert calls == []
