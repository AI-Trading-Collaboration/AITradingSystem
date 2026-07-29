from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Final, NoReturn, TypeVar

import yaml

from ai_trading_system.config import (
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
)
from ai_trading_system.contracts.data_quality_consumer_authorization import (
    DataQualityConsumerAuthorizationAttestation,
    DataQualityConsumerAuthorizationContractError,
)
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityDateWindow,
    DataQualityExecutionContractError,
    DataQualityExecutionReceipt,
)
from ai_trading_system.data.access_control import (
    StoreAclPolicy,
    apply_isolated_store_acl,
    build_acl_attestation,
    load_acl_policy,
    validate_acl_attestation,
    validate_acl_rehearsal_bundle,
)
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadReplayInputCandidate,
    DownloadSourceBinding,
    ValidatedDownloadPublication,
    publish_download_transaction,
    resolve_download_publication,
)
from ai_trading_system.data.durability import (
    probe_filesystem_durability,
    validate_durability_attestation,
)
from ai_trading_system.data.immutable_publish import (
    PUBLICATION_DURABILITY_PROTOCOL_VERSION,
    DataPublicationError,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality_consumer_authorization import (
    build_data_quality_consumer_authorization_attestation,
    load_reviewed_data_quality_consumer_authorization_policy,
    verify_data_quality_consumer_authorization,
    write_data_quality_consumer_authorization_attestation,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    run_canonical_data_quality_execution,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes
from ai_trading_system.yaml_loader import safe_load_yaml_text

CONSUMER_MIGRATION_POLICY_SCHEMA_VERSION: Final = (
    "data_foundation_consumer_migration_policy.v1"
)
CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION: Final = (
    "data_foundation_consumer_migration_copy_manifest.v1"
)
CONSUMER_MIGRATION_ATTESTATION_SCHEMA_VERSION: Final = (
    "data_foundation_consumer_migration_attestation.v1"
)
CONSUMER_MIGRATION_BUNDLE_SCHEMA_VERSION: Final = (
    "data_foundation_consumer_migration_bundle.v1"
)
CONSUMER_REHEARSAL_RECEIPT_SCHEMA_VERSION: Final = (
    "data_foundation_consumer_read_rehearsal.v1"
)

DEFAULT_POLICY_PATH: Final = Path("config/data/data_foundation_consumer_migration.yaml")
DEFAULT_ACL_POLICY_PATH: Final = Path("config/data/data_foundation_acl.yaml")
DEFAULT_DQ_POLICY_PATH: Final = Path("config/data_quality.yaml")
DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH: Final = Path(
    "config/data_quality/arch_004_wave15_daily_score_consumer_authorization.yaml"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_D0C_BUNDLE_SCHEMA_VERSION = "data_foundation_d0c_rehearsal_bundle.v1"
_D0C_BUNDLE_ID_PREFIX = "data_foundation_d0c_bundle_"
_SELECTED_POINTER_RELATIVE = (
    ".download_publications/current/download_composite.json"
)
_HISTORY_ROOT_RELATIVE = (
    ".download_publications/pointer_history/download_composite"
)
_CANDIDATE_SCAFFOLD_PATHS = (
    "config/data/us_equity_special_closure_registry.yaml",
    "config/data_quality.yaml",
    "config/data_quality/arch_004_wave15_daily_score_consumer_authorization.yaml",
    "config/universe.yaml",
    "src/ai_trading_system/data/immutable_publish.py",
    "src/ai_trading_system/data/quality.py",
    "src/ai_trading_system/data/quality_execution.py",
    "src/ai_trading_system/trading_calendar.py",
    "src/ai_trading_system/us_equity_special_closure_policy.py",
)
_MIGRATION_VALIDATOR_SOURCE_PATHS = (
    "src/ai_trading_system/data/access_control.py",
    "src/ai_trading_system/data/durability.py",
    "src/ai_trading_system/data/foundation_consumer_migration.py",
    "src/ai_trading_system/data/immutable_publish.py",
    "src/ai_trading_system/data/quality_consumer_authorization.py",
    "src/ai_trading_system/data/quality_execution.py",
)


class DataFoundationConsumerMigrationError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


@dataclass(frozen=True)
class HistoricalSourceAcceptance:
    as_of: date
    receipt_id: str
    receipt_path: str
    receipt_sha256: str
    authorization_id: str
    authorization_path: str
    authorization_sha256: str
    publication_transaction_id: str
    publication_transaction_sha256: str
    publication_discovery_pointer_sha256: str


@dataclass(frozen=True)
class CapabilityEvidence:
    d0c_bundle_path: str
    d0c_bundle_id: str
    d0c_bundle_sha256: str
    d0d_bundle_path: str
    d0d_bundle_id: str
    d0d_bundle_sha256: str


@dataclass(frozen=True)
class ConsumerMigrationPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    rationale: str
    review_condition: str
    consumer_id: str
    consumer_version: str
    execution_profile_id: str
    accepted_data_quality_statuses: tuple[str, ...]
    required_input_roles: tuple[str, ...]
    historical: HistoricalSourceAcceptance
    capabilities: CapabilityEvidence
    allowed_output_parent: str
    source_publication_dir: str
    candidate_publication_dir: str
    publication_store_dir: str
    legacy_projection_files: tuple[str, ...]
    retained_for_revalidation: bool
    policy_path: str
    policy_sha256: str


@dataclass(frozen=True)
class CandidateMaterialization:
    candidate_project_root: Path
    candidate_data_root: Path
    copy_manifest: Mapping[str, object]
    copy_manifest_path: Path
    publication: ValidatedDownloadPublication
    historical_receipt: DataQualityExecutionReceipt
    historical_authorization: DataQualityConsumerAuthorizationAttestation


_VERIFIED_TOKEN = object()


@dataclass(frozen=True)
class VerifiedConsumerMigration:
    _token: object
    attestation_id: str
    consumer_id: str
    consumer_version: str
    candidate_project_root: Path
    candidate_data_root: Path
    receipt_id: str
    authorization_id: str
    verified_at: datetime

    def __post_init__(self) -> None:
        if self._token is not _VERIFIED_TOKEN:
            raise DataFoundationConsumerMigrationError(
                "CONSUMER_MIGRATION_CAPABILITY_FORGED",
                "verified capability can only be returned by the canonical verifier",
            )


T = TypeVar("T")


def load_consumer_migration_policy(
    path: Path = DEFAULT_POLICY_PATH,
    *,
    project_root: Path,
) -> ConsumerMigrationPolicy:
    root = project_root.resolve()
    relative = _repo_relative(root, path)
    raw = _read_secure(root, relative, "CONSUMER_MIGRATION_POLICY_MISSING")
    try:
        value = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_POLICY_INVALID",
            str(exc),
            path=root / Path(relative),
        ) from exc
    payload = _mapping(value, "policy")
    _exact_fields(
        payload,
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "status",
            "owner",
            "rationale",
            "review_condition",
            "consumer",
            "historical_source_acceptance",
            "capability_evidence",
            "candidate",
            "claim_boundary",
        },
        "CONSUMER_MIGRATION_POLICY_INVALID",
    )
    if (
        payload.get("schema_version") != CONSUMER_MIGRATION_POLICY_SCHEMA_VERSION
        or payload.get("status") != "REVIEWED"
    ):
        _fail("CONSUMER_MIGRATION_POLICY_INVALID", "schema/status mismatch")
    consumer = _mapping(payload.get("consumer"), "consumer")
    _exact_fields(
        consumer,
        {
            "consumer_id",
            "consumer_version",
            "execution_profile_id",
            "accepted_data_quality_statuses",
            "required_input_roles",
        },
        "CONSUMER_MIGRATION_POLICY_INVALID",
    )
    accepted = _strings(
        consumer.get("accepted_data_quality_statuses"),
        "accepted_data_quality_statuses",
    )
    required_roles = _strings(consumer.get("required_input_roles"), "required_input_roles")
    if (
        accepted != ("PASS",)
        or required_roles != ("prices", "rates", "secondary_prices")
        or consumer.get("consumer_id") != "daily_score_daily"
        or consumer.get("consumer_version") != "1.0.0"
        or consumer.get("execution_profile_id")
        != DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
    ):
        _fail("CONSUMER_MIGRATION_POLICY_INVALID", "consumer scope widened")
    historical = _historical_policy(payload.get("historical_source_acceptance"))
    capabilities = _capability_policy(payload.get("capability_evidence"))
    candidate = _mapping(payload.get("candidate"), "candidate")
    _exact_fields(
        candidate,
        {
            "allowed_output_parent",
            "source_publication_dir",
            "candidate_publication_dir",
            "publication_store_dir",
            "legacy_projection_files",
            "retained_for_revalidation",
        },
        "CONSUMER_MIGRATION_POLICY_INVALID",
    )
    legacy = _strings(candidate.get("legacy_projection_files"), "legacy_projection_files")
    if legacy != (
        "download_manifest.csv",
        "prices_daily.csv",
        "prices_marketstack_daily.csv",
        "rates_daily.csv",
    ):
        _fail("CONSUMER_MIGRATION_POLICY_INVALID", "legacy projection set drift")
    boundary = _mapping(payload.get("claim_boundary"), "claim_boundary")
    expected_boundary = {
        "historical_false_fields_rewritten": False,
        "generic_consumer_cutover_allowed": False,
        "automatic_non_daily_dispatch": False,
        "qld_automatic_selection_enabled": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if boundary != expected_boundary:
        _fail("CONSUMER_MIGRATION_POLICY_INVALID", "claim boundary widened")
    return ConsumerMigrationPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        policy_version=_text(payload.get("policy_version"), "policy_version"),
        status="REVIEWED",
        owner=_text(payload.get("owner"), "owner"),
        rationale=_text(payload.get("rationale"), "rationale"),
        review_condition=_text(payload.get("review_condition"), "review_condition"),
        consumer_id="daily_score_daily",
        consumer_version="1.0.0",
        execution_profile_id=DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        accepted_data_quality_statuses=accepted,
        required_input_roles=required_roles,
        historical=historical,
        capabilities=capabilities,
        allowed_output_parent=_portable_path(
            candidate.get("allowed_output_parent"),
            "allowed_output_parent",
        ),
        source_publication_dir=_portable_path(
            candidate.get("source_publication_dir"),
            "source_publication_dir",
        ),
        candidate_publication_dir=_portable_path(
            candidate.get("candidate_publication_dir"),
            "candidate_publication_dir",
        ),
        publication_store_dir=_portable_path(
            candidate.get("publication_store_dir"),
            "publication_store_dir",
        ),
        legacy_projection_files=legacy,
        retained_for_revalidation=_boolean(
            candidate.get("retained_for_revalidation"),
            "retained_for_revalidation",
        ),
        policy_path=relative,
        policy_sha256=hashlib.sha256(raw).hexdigest(),
    )


def materialize_isolated_candidate(
    *,
    source_project_root: Path,
    output_root: Path,
    project_root: Path,
    policy: ConsumerMigrationPolicy,
    acl_policy: StoreAclPolicy,
    generated_at: datetime,
) -> CandidateMaterialization:
    _aware_datetime(generated_at, "generated_at")
    root = project_root.resolve()
    source_root = _resolved_plain_directory(source_project_root)
    output = _prepare_output_root(
        output_root,
        root / Path(policy.allowed_output_parent),
    )
    candidate_root = output / "candidate_project"
    candidate_root.mkdir()
    for relative in _CANDIDATE_SCAFFOLD_PATHS:
        raw = _read_secure(root, relative, "CONSUMER_MIGRATION_SCAFFOLD_MISSING")
        _write(candidate_root, relative, raw)

    candidate_data_root = candidate_root / Path(policy.candidate_publication_dir)
    candidate_data_root.mkdir(parents=True)
    apply_isolated_store_acl(
        candidate_data_root,
        allowed_parent=candidate_root,
        policy=acl_policy,
    )

    historical_receipt, historical_authorization = _validate_historical_acceptance(
        source_root,
        policy,
    )
    source_data_root = source_root / Path(policy.source_publication_dir)
    selected_pointer_path, selected_pointer_raw, pointer = _select_historical_pointer(
        source_data_root,
        policy,
    )
    transaction = _selected_transaction(source_data_root, pointer, policy)
    _validate_transaction_against_historical(
        transaction,
        historical_receipt=historical_receipt,
        historical_authorization=historical_authorization,
        policy=policy,
    )

    candidate_publication = _publish_candidate_from_historical(
        source_data_root=source_data_root,
        candidate_data_root=candidate_data_root,
        transaction=transaction,
        selected_pointer_path=selected_pointer_path,
        selected_pointer_raw=selected_pointer_raw,
        policy=policy,
        published_at=generated_at,
    )
    _require_candidate_publication(
        candidate_publication,
        policy=policy,
        historical_receipt=historical_receipt,
    )
    copied = [
        _copy_record(
            source_path=f"generated:{path.relative_to(candidate_root).as_posix()}",
            candidate_path=path.relative_to(candidate_root).as_posix(),
            raw=path.read_bytes(),
        )
        for path in _regular_files(candidate_data_root)
    ]

    profile = probe_filesystem_durability(candidate_data_root)
    if not profile.supported:
        _fail(
            "CONSUMER_MIGRATION_DURABILITY_UNSUPPORTED",
            f"{profile.system}/{profile.filesystem}/{profile.storage_scope}",
            path=candidate_data_root,
        )
    copy_body: dict[str, object] = {
        "schema_version": CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION,
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "source_project_root": source_root.as_posix(),
        "candidate_project_root": candidate_root.resolve(strict=True).as_posix(),
        "candidate_data_root": candidate_data_root.resolve(strict=True).as_posix(),
        "selected_publication": _publication_binding(candidate_publication, candidate_root),
        "historical_receipt_id": historical_receipt.receipt_id,
        "historical_authorization_id": historical_authorization.authorization_id,
        "durability_protocol_version": PUBLICATION_DURABILITY_PROTOCOL_VERSION,
        "filesystem_profile": profile.to_dict(),
        "copied_objects": sorted(copied, key=lambda item: str(item["candidate_path"])),
        "copy_count": len(copied),
        "all_objects_checksum_verified": True,
        "production_effect": "none",
        "broker_action": "none",
    }
    copy_manifest = {
        "copy_manifest_id": f"consumer_copy_{_digest(copy_body)[:32]}",
        **copy_body,
    }
    raw = canonical_json_bytes(copy_manifest)
    relative = (
        "outputs/data_foundation_consumer_migration/"
        f"{copy_manifest['copy_manifest_id']}/copy_manifest.json"
    )
    copy_manifest_path = _write(candidate_root, relative, raw)
    validate_candidate_copy_manifest(
        copy_manifest,
        candidate_project_root=candidate_root,
    )
    return CandidateMaterialization(
        candidate_project_root=candidate_root,
        candidate_data_root=candidate_data_root,
        copy_manifest=copy_manifest,
        copy_manifest_path=copy_manifest_path,
        publication=candidate_publication,
        historical_receipt=historical_receipt,
        historical_authorization=historical_authorization,
    )


def validate_candidate_copy_manifest(
    payload: Mapping[str, object],
    *,
    candidate_project_root: Path,
) -> None:
    manifest = dict(payload)
    _exact_fields(
        manifest,
        {
            "copy_manifest_id",
            "schema_version",
            "generated_at",
            "source_project_root",
            "candidate_project_root",
            "candidate_data_root",
            "selected_publication",
            "historical_receipt_id",
            "historical_authorization_id",
            "durability_protocol_version",
            "filesystem_profile",
            "copied_objects",
            "copy_count",
            "all_objects_checksum_verified",
            "production_effect",
            "broker_action",
        },
        "CONSUMER_MIGRATION_COPY_INVALID",
    )
    supplied_id = _text(manifest.pop("copy_manifest_id"), "copy_manifest_id")
    if supplied_id != f"consumer_copy_{_digest(manifest)[:32]}":
        _fail("CONSUMER_MIGRATION_COPY_TAMPERED", "copy manifest id mismatch")
    if (
        manifest.get("schema_version")
        != CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION
        or manifest.get("durability_protocol_version")
        != PUBLICATION_DURABILITY_PROTOCOL_VERSION
        or manifest.get("all_objects_checksum_verified") is not True
        or manifest.get("production_effect") != "none"
        or manifest.get("broker_action") != "none"
    ):
        _fail("CONSUMER_MIGRATION_COPY_INVALID", "copy claim boundary invalid")
    _aware_datetime(manifest.get("generated_at"), "generated_at")
    root = candidate_project_root.resolve(strict=True)
    if manifest.get("candidate_project_root") != root.as_posix():
        _fail("CONSUMER_MIGRATION_STORE_IDENTITY_MISMATCH", "candidate root mismatch")
    records = _sequence(manifest.get("copied_objects"), "copied_objects")
    if manifest.get("copy_count") != len(records) or not records:
        _fail("CONSUMER_MIGRATION_COPY_INVALID", "copy count mismatch")
    observed_paths: set[str] = set()
    for item in records:
        record = _mapping(item, "copied_objects[]")
        _exact_fields(
            record,
            {
                "source_path",
                "candidate_path",
                "sha256",
                "size_bytes",
            },
            "CONSUMER_MIGRATION_COPY_INVALID",
        )
        candidate_path = _portable_path(record.get("candidate_path"), "candidate_path")
        if candidate_path in observed_paths:
            _fail("CONSUMER_MIGRATION_COPY_INVALID", "duplicate candidate path")
        observed_paths.add(candidate_path)
        raw = _read_secure(root, candidate_path, "CONSUMER_MIGRATION_COPY_MISSING")
        if (
            hashlib.sha256(raw).hexdigest()
            != _sha(record.get("sha256"), "sha256")
            or len(raw) != _integer(record.get("size_bytes"), "size_bytes", minimum=1)
        ):
            _fail(
                "CONSUMER_MIGRATION_COPY_TAMPERED",
                candidate_path,
                path=root / Path(candidate_path),
            )


def run_isolated_consumer_migration_rehearsal(
    *,
    source_project_root: Path,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    policy_path: Path = DEFAULT_POLICY_PATH,
    acl_policy_path: Path = DEFAULT_ACL_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DQ_POLICY_PATH,
    consumer_authorization_policy_path: Path = DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
) -> dict[str, object]:
    root = project_root.resolve()
    policy = load_consumer_migration_policy(policy_path, project_root=root)
    acl_policy = load_acl_policy(root / acl_policy_path)
    _validate_capability_evidence(policy, project_root=root)
    materialized = materialize_isolated_candidate(
        source_project_root=source_project_root,
        output_root=output_root,
        project_root=root,
        policy=policy,
        acl_policy=acl_policy,
        generated_at=generated_at,
    )
    candidate = materialized.candidate_project_root
    universe = _load_candidate_universe(candidate)
    as_of = policy.historical.as_of
    request = CanonicalDataQualityExecutionRequest(
        as_of=as_of,
        requested_window=DataQualityDateWindow(date(2021, 2, 22), as_of),
        prices_path=candidate / "data/raw/prices_daily.csv",
        rates_path=candidate / "data/raw/rates_daily.csv",
        manifest_path=candidate / "data/raw/download_manifest.csv",
        expected_price_tickers=tuple(configured_price_tickers(universe)),
        expected_rate_series=tuple(configured_rate_series(universe)),
        execution_profile_id=policy.execution_profile_id,
        secondary_prices_path=candidate / "data/raw/prices_marketstack_daily.csv",
        require_secondary_prices=True,
        policy_path=data_quality_policy_path,
    )
    execution = run_canonical_data_quality_execution(request, project_root=candidate)
    preflight = verify_data_quality_execution_receipt(
        execution.receipt_path,
        expected_as_of=as_of,
        expected_policy_path=data_quality_policy_path,
        expected_input_roles=policy.required_input_roles,
        project_root=candidate,
    )
    if (
        preflight.status not in policy.accepted_data_quality_statuses
        or execution.report.error_count != 0
        or execution.report.warning_count != 0
    ):
        _fail(
            "CONSUMER_MIGRATION_DQ_NOT_STRICT_PASS",
            (
                f"status={preflight.status} errors={execution.report.error_count} "
                f"warnings={execution.report.warning_count}"
            ),
        )
    consumer_policy = load_reviewed_data_quality_consumer_authorization_policy(
        consumer_authorization_policy_path,
        project_root=candidate,
    )
    authorization = build_data_quality_consumer_authorization_attestation(
        policy=consumer_policy,
        preflight=preflight,
        publication=materialized.publication,
        authorized_at=generated_at,
        project_root=candidate,
    )
    authorization_path = write_data_quality_consumer_authorization_attestation(
        authorization,
        project_root=candidate,
    )
    verified_authorization = verify_data_quality_consumer_authorization(
        authorization_path,
        expected_consumer_id=policy.consumer_id,
        expected_consumer_version=policy.consumer_version,
        expected_as_of=as_of,
        expected_data_quality_policy_path=data_quality_policy_path,
        policy_path=consumer_authorization_policy_path,
        now=generated_at + timedelta(seconds=1),
        project_root=candidate,
    )
    acl_attestation = build_acl_attestation(
        materialized.candidate_data_root,
        policy=acl_policy,
        generated_at=generated_at,
    )
    validate_acl_attestation(
        acl_attestation,
        store_root=materialized.candidate_data_root,
        policy=acl_policy,
    )
    body = _migration_attestation_body(
        policy=policy,
        materialized=materialized,
        generated_at=generated_at,
        execution_receipt=execution.receipt,
        authorization=authorization,
        authorization_path=authorization_path,
        acl_attestation=acl_attestation,
        project_root=root,
    )
    attestation = {
        "attestation_id": f"consumer_migration_{_digest(body)[:32]}",
        **body,
    }
    attestation_raw = canonical_json_bytes(attestation)
    attestation_path = _write(
        output_root,
        "migration_attestation.json",
        attestation_raw,
    )
    verified = verify_consumer_migration_attestation(
        attestation_path,
        project_root=root,
        policy_path=policy_path,
        acl_policy_path=acl_policy_path,
        data_quality_policy_path=data_quality_policy_path,
        consumer_authorization_policy_path=consumer_authorization_policy_path,
        now=generated_at + timedelta(seconds=2),
    )
    read_receipt = dispatch_isolated_daily_score_consumer(
        verified,
        runner=_read_rehearsal_runner,
    )
    read_raw = canonical_json_bytes(read_receipt)
    read_path = _write(output_root, "consumer_read_rehearsal.json", read_raw)
    bundle_body: dict[str, object] = {
        "schema_version": CONSUMER_MIGRATION_BUNDLE_SCHEMA_VERSION,
        "status": "PASS",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_sha256": policy.policy_sha256,
        "consumer_id": policy.consumer_id,
        "consumer_version": policy.consumer_version,
        "candidate_project_root": candidate.resolve(strict=True).as_posix(),
        "candidate_data_root": materialized.candidate_data_root.resolve(strict=True).as_posix(),
        "copy_manifest": _artifact_pointer(
            materialized.copy_manifest_path,
            artifact_id=_text(
                materialized.copy_manifest.get("copy_manifest_id"),
                "copy_manifest_id",
            ),
            schema_version=CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION,
        ),
        "migration_attestation": _artifact_pointer(
            attestation_path,
            artifact_id=_text(attestation.get("attestation_id"), "attestation_id"),
            schema_version=CONSUMER_MIGRATION_ATTESTATION_SCHEMA_VERSION,
        ),
        "consumer_read_rehearsal": _artifact_pointer(
            read_path,
            artifact_id=_text(read_receipt.get("rehearsal_id"), "rehearsal_id"),
            schema_version=CONSUMER_REHEARSAL_RECEIPT_SCHEMA_VERSION,
        ),
        "candidate_retained_for_revalidation": policy.retained_for_revalidation,
        "claim_boundary": _claim_boundary(),
    }
    bundle = {
        "bundle_id": f"data_foundation_consumer_migration_bundle_{_digest(bundle_body)[:32]}",
        **bundle_body,
    }
    bundle_path = _write(
        output_root,
        "rehearsal_bundle.json",
        canonical_json_bytes(bundle),
    )
    validate_consumer_migration_bundle(
        bundle_path,
        project_root=root,
        policy_path=policy_path,
        acl_policy_path=acl_policy_path,
        data_quality_policy_path=data_quality_policy_path,
        consumer_authorization_policy_path=consumer_authorization_policy_path,
        now=generated_at + timedelta(seconds=3),
    )
    if verified_authorization.authorization_id != authorization.authorization_id:
        _fail("CONSUMER_MIGRATION_AUTHORIZATION_MISMATCH", "verified authorization drift")
    return bundle


def verify_consumer_migration_attestation(
    attestation_path: Path,
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_POLICY_PATH,
    acl_policy_path: Path = DEFAULT_ACL_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DQ_POLICY_PATH,
    consumer_authorization_policy_path: Path = DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    now: datetime | None = None,
) -> VerifiedConsumerMigration:
    root = project_root.resolve()
    policy = load_consumer_migration_policy(policy_path, project_root=root)
    raw = attestation_path.resolve(strict=True).read_bytes()
    payload = _canonical_mapping(raw, "CONSUMER_MIGRATION_ATTESTATION_INVALID")
    _validate_attestation_shape(payload)
    supplied_id = _text(payload.get("attestation_id"), "attestation_id")
    body = dict(payload)
    body.pop("attestation_id")
    if supplied_id != f"consumer_migration_{_digest(body)[:32]}":
        _fail("CONSUMER_MIGRATION_ATTESTATION_TAMPERED", "attestation id mismatch")
    if (
        payload.get("policy_id") != policy.policy_id
        or payload.get("policy_version") != policy.policy_version
        or payload.get("policy_sha256") != policy.policy_sha256
        or payload.get("consumer_id") != policy.consumer_id
        or payload.get("consumer_version") != policy.consumer_version
        or payload.get("claim_boundary") != _claim_boundary()
    ):
        _fail("CONSUMER_MIGRATION_ATTESTATION_INVALID", "policy or claim boundary drift")
    candidate = _mapping(payload.get("candidate"), "candidate")
    candidate_root = Path(_text(candidate.get("project_root"), "candidate.project_root"))
    candidate_data_root = Path(_text(candidate.get("data_root"), "candidate.data_root"))
    if (
        not candidate_root.is_absolute()
        or candidate_root.resolve(strict=True) != candidate_root
        or candidate_data_root.resolve(strict=True) != candidate_data_root
        or candidate_data_root
        != candidate_root / Path(policy.candidate_publication_dir)
    ):
        _fail("CONSUMER_MIGRATION_STORE_IDENTITY_MISMATCH", "candidate path drift")
    copy_binding = _mapping(payload.get("copy_manifest"), "copy_manifest")
    copy_path = candidate_root / Path(
        _portable_path(copy_binding.get("path"), "copy_manifest.path")
    )
    copy_raw = _read_secure(
        candidate_root,
        _repo_relative(candidate_root, copy_path),
        "CONSUMER_MIGRATION_COPY_MISSING",
    )
    if (
        hashlib.sha256(copy_raw).hexdigest()
        != _sha(copy_binding.get("sha256"), "copy_manifest.sha256")
        or len(copy_raw)
        != _integer(copy_binding.get("size_bytes"), "copy_manifest.size_bytes", minimum=1)
    ):
        _fail("CONSUMER_MIGRATION_COPY_TAMPERED", "copy manifest binding mismatch")
    copy_manifest = _canonical_mapping(copy_raw, "CONSUMER_MIGRATION_COPY_INVALID")
    validate_candidate_copy_manifest(
        copy_manifest,
        candidate_project_root=candidate_root,
    )
    publication = resolve_download_publication(output_dir=candidate_data_root)
    if (
        _publication_binding(publication, candidate_root)
        != candidate.get("publication")
        or publication.requested_end != policy.historical.as_of
        or not publication.legacy_projection_verified
        or publication.consumer_cutover_allowed
        or publication.production_effect != "none"
    ):
        _fail(
            "CONSUMER_MIGRATION_PUBLICATION_MISMATCH",
            publication.transaction_id,
        )
    dq = _mapping(payload.get("data_quality"), "data_quality")
    receipt_path = candidate_root / Path(
        _portable_path(dq.get("receipt_path"), "data_quality.receipt_path")
    )
    preflight = verify_data_quality_execution_receipt(
        receipt_path,
        expected_as_of=policy.historical.as_of,
        expected_policy_path=data_quality_policy_path,
        expected_input_roles=policy.required_input_roles,
        project_root=candidate_root,
    )
    if (
        preflight.receipt_id != dq.get("receipt_id")
        or preflight.receipt_sha256 != dq.get("receipt_sha256")
        or preflight.status != "PASS"
        or dq.get("error_count") != 0
        or dq.get("warning_count") != 0
    ):
        _fail("CONSUMER_MIGRATION_DQ_NOT_STRICT_PASS", "receipt binding mismatch")
    authorization = _mapping(payload.get("consumer_authorization"), "consumer_authorization")
    authorization_path = candidate_root / Path(
        _portable_path(
            authorization.get("path"),
            "consumer_authorization.path",
        )
    )
    verified_authorization = verify_data_quality_consumer_authorization(
        authorization_path,
        expected_consumer_id=policy.consumer_id,
        expected_consumer_version=policy.consumer_version,
        expected_as_of=policy.historical.as_of,
        expected_data_quality_policy_path=data_quality_policy_path,
        policy_path=consumer_authorization_policy_path,
        now=now,
        project_root=candidate_root,
    )
    authorization_raw = authorization_path.read_bytes()
    if (
        verified_authorization.authorization_id
        != authorization.get("authorization_id")
        or hashlib.sha256(authorization_raw).hexdigest()
        != authorization.get("sha256")
        or len(authorization_raw) != authorization.get("size_bytes")
        or verified_authorization.attestation.expires_at.astimezone(UTC).isoformat()
        != authorization.get("expires_at")
    ):
        _fail("CONSUMER_MIGRATION_AUTHORIZATION_MISMATCH", "authorization id mismatch")
    acl_policy = load_acl_policy(root / acl_policy_path)
    acl_payload = _mapping(payload.get("acl"), "acl")
    acl_attestation = _mapping(acl_payload.get("attestation"), "acl.attestation")
    validate_acl_attestation(
        acl_attestation,
        store_root=candidate_data_root,
        policy=acl_policy,
    )
    if (
        acl_attestation.get("attestation_id") != acl_payload.get("attestation_id")
        or acl_attestation.get("store_identity") != acl_payload.get("store_identity")
    ):
        _fail("CONSUMER_MIGRATION_ACL_MISMATCH", "ACL binding mismatch")
    _validate_capability_evidence(policy, project_root=root)
    durability = _mapping(payload.get("durability"), "durability")
    profile = probe_filesystem_durability(candidate_data_root)
    if (
        not profile.supported
        or durability.get("protocol_version")
        != PUBLICATION_DURABILITY_PROTOCOL_VERSION
        or durability.get("filesystem_profile") != profile.to_dict()
        or durability.get("candidate_copy_verified") is not True
    ):
        _fail("CONSUMER_MIGRATION_DURABILITY_MISMATCH", "durability binding mismatch")
    observed_store_identity = _store_identity(
        candidate_data_root,
        publication=publication,
        copy_manifest_sha256=hashlib.sha256(copy_raw).hexdigest(),
        acl_attestation=acl_attestation,
    )
    if observed_store_identity != candidate.get("store_identity"):
        _fail("CONSUMER_MIGRATION_STORE_IDENTITY_MISMATCH", "store identity mismatch")
    if payload.get("validator_sources") != _validator_sources(root):
        _fail(
            "CONSUMER_MIGRATION_VALIDATOR_SOURCE_DRIFT",
            "validator source checksum mismatch",
        )
    verified_at = (now or datetime.now(tz=UTC)).astimezone(UTC)
    return VerifiedConsumerMigration(
        _token=_VERIFIED_TOKEN,
        attestation_id=supplied_id,
        consumer_id=policy.consumer_id,
        consumer_version=policy.consumer_version,
        candidate_project_root=candidate_root,
        candidate_data_root=candidate_data_root,
        receipt_id=preflight.receipt_id,
        authorization_id=verified_authorization.authorization_id,
        verified_at=verified_at,
    )


def dispatch_isolated_daily_score_consumer(
    verified: VerifiedConsumerMigration,
    *,
    runner: Callable[[VerifiedConsumerMigration], T],
) -> T:
    if (
        not isinstance(verified, VerifiedConsumerMigration)
        or verified._token is not _VERIFIED_TOKEN
    ):
        _fail("CONSUMER_MIGRATION_CAPABILITY_FORGED", "verified migration required")
    if (
        verified.consumer_id != "daily_score_daily"
        or verified.consumer_version != "1.0.0"
    ):
        _fail("CONSUMER_MIGRATION_CONSUMER_MISMATCH", verified.consumer_id)
    return runner(verified)


def validate_consumer_migration_bundle(
    bundle_path: Path,
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_POLICY_PATH,
    acl_policy_path: Path = DEFAULT_ACL_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DQ_POLICY_PATH,
    consumer_authorization_policy_path: Path = DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    now: datetime | None = None,
) -> dict[str, object]:
    raw = bundle_path.resolve(strict=True).read_bytes()
    bundle = _canonical_mapping(raw, "CONSUMER_MIGRATION_BUNDLE_INVALID")
    expected_fields = {
        "bundle_id",
        "schema_version",
        "status",
        "generated_at",
        "policy_id",
        "policy_version",
        "policy_sha256",
        "consumer_id",
        "consumer_version",
        "candidate_project_root",
        "candidate_data_root",
        "copy_manifest",
        "migration_attestation",
        "consumer_read_rehearsal",
        "candidate_retained_for_revalidation",
        "claim_boundary",
    }
    _exact_fields(bundle, expected_fields, "CONSUMER_MIGRATION_BUNDLE_INVALID")
    body = dict(bundle)
    supplied_id = _text(body.pop("bundle_id"), "bundle_id")
    if supplied_id != f"data_foundation_consumer_migration_bundle_{_digest(body)[:32]}":
        _fail("CONSUMER_MIGRATION_BUNDLE_TAMPERED", "bundle id mismatch")
    if (
        bundle.get("schema_version") != CONSUMER_MIGRATION_BUNDLE_SCHEMA_VERSION
        or bundle.get("status") != "PASS"
        or bundle.get("claim_boundary") != _claim_boundary()
        or bundle.get("candidate_retained_for_revalidation") is not True
    ):
        _fail("CONSUMER_MIGRATION_BUNDLE_INVALID", "bundle claim boundary invalid")
    root = bundle_path.resolve(strict=True).parent
    attestation_binding = _mapping(
        bundle.get("migration_attestation"),
        "migration_attestation",
    )
    attestation_path = root / Path(
        _portable_path(attestation_binding.get("path"), "migration_attestation.path")
    )
    attestation_raw = attestation_path.read_bytes()
    if (
        hashlib.sha256(attestation_raw).hexdigest()
        != _sha(attestation_binding.get("sha256"), "migration_attestation.sha256")
        or len(attestation_raw)
        != _integer(
            attestation_binding.get("size_bytes"),
            "migration_attestation.size_bytes",
            minimum=1,
        )
    ):
        _fail("CONSUMER_MIGRATION_BUNDLE_TAMPERED", "attestation pointer mismatch")
    verified = verify_consumer_migration_attestation(
        attestation_path,
        project_root=project_root,
        policy_path=policy_path,
        acl_policy_path=acl_policy_path,
        data_quality_policy_path=data_quality_policy_path,
        consumer_authorization_policy_path=consumer_authorization_policy_path,
        now=now,
    )
    if verified.attestation_id != attestation_binding.get("artifact_id"):
        _fail("CONSUMER_MIGRATION_BUNDLE_TAMPERED", "attestation id mismatch")
    read_binding = _mapping(
        bundle.get("consumer_read_rehearsal"),
        "consumer_read_rehearsal",
    )
    read_path = root / Path(
        _portable_path(read_binding.get("path"), "consumer_read_rehearsal.path")
    )
    read_raw = read_path.read_bytes()
    read_receipt = _canonical_mapping(
        read_raw,
        "CONSUMER_MIGRATION_REHEARSAL_INVALID",
    )
    if (
        hashlib.sha256(read_raw).hexdigest()
        != _sha(read_binding.get("sha256"), "consumer_read_rehearsal.sha256")
        or read_receipt.get("rehearsal_id") != read_binding.get("artifact_id")
        or read_receipt.get("migration_attestation_id") != verified.attestation_id
        or read_receipt.get("schema_version")
        != CONSUMER_REHEARSAL_RECEIPT_SCHEMA_VERSION
        or read_receipt.get("status") != "PASS"
        or read_receipt.get("production_effect") != "none"
        or read_receipt.get("broker_action") != "none"
    ):
        _fail("CONSUMER_MIGRATION_REHEARSAL_INVALID", "read receipt binding mismatch")
    return dict(bundle)


def _migration_attestation_body(
    *,
    policy: ConsumerMigrationPolicy,
    materialized: CandidateMaterialization,
    generated_at: datetime,
    execution_receipt: DataQualityExecutionReceipt,
    authorization: DataQualityConsumerAuthorizationAttestation,
    authorization_path: Path,
    acl_attestation: Mapping[str, object],
    project_root: Path,
) -> dict[str, object]:
    candidate = materialized.candidate_project_root
    copy_raw = materialized.copy_manifest_path.read_bytes()
    copy_sha = hashlib.sha256(copy_raw).hexdigest()
    receipt_path = candidate / Path(
        f"outputs/data_quality/executions/{execution_receipt.receipt_id}/receipt.json"
    )
    receipt_raw = receipt_path.read_bytes()
    authorization_raw = authorization_path.read_bytes()
    store_identity = _store_identity(
        materialized.candidate_data_root,
        publication=materialized.publication,
        copy_manifest_sha256=copy_sha,
        acl_attestation=acl_attestation,
    )
    return {
        "schema_version": CONSUMER_MIGRATION_ATTESTATION_SCHEMA_VERSION,
        "status": "PASS",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_path": policy.policy_path,
        "policy_sha256": policy.policy_sha256,
        "consumer_id": policy.consumer_id,
        "consumer_version": policy.consumer_version,
        "candidate": {
            "project_root": candidate.resolve(strict=True).as_posix(),
            "data_root": materialized.candidate_data_root.resolve(strict=True).as_posix(),
            "store_identity": store_identity,
            "publication": _publication_binding(materialized.publication, candidate),
        },
        "copy_manifest": {
            "path": _repo_relative(candidate, materialized.copy_manifest_path),
            "copy_manifest_id": materialized.copy_manifest["copy_manifest_id"],
            "sha256": copy_sha,
            "size_bytes": len(copy_raw),
        },
        "data_quality": {
            "receipt_id": execution_receipt.receipt_id,
            "receipt_path": _repo_relative(candidate, receipt_path),
            "receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
            "receipt_size_bytes": len(receipt_raw),
            "status": execution_receipt.report.status,
            "error_count": execution_receipt.report.error_count,
            "warning_count": execution_receipt.report.warning_count,
        },
        "durability": {
            "protocol_version": PUBLICATION_DURABILITY_PROTOCOL_VERSION,
            "filesystem_profile": probe_filesystem_durability(
                materialized.candidate_data_root
            ).to_dict(),
            "d0c_bundle_id": policy.capabilities.d0c_bundle_id,
            "d0c_bundle_sha256": policy.capabilities.d0c_bundle_sha256,
            "candidate_copy_verified": True,
        },
        "acl": {
            "d0d_bundle_id": policy.capabilities.d0d_bundle_id,
            "d0d_bundle_sha256": policy.capabilities.d0d_bundle_sha256,
            "attestation_id": acl_attestation["attestation_id"],
            "store_identity": acl_attestation["store_identity"],
            "attestation": dict(acl_attestation),
        },
        "consumer_authorization": {
            "authorization_id": authorization.authorization_id,
            "path": _repo_relative(candidate, authorization_path),
            "sha256": hashlib.sha256(authorization_raw).hexdigest(),
            "size_bytes": len(authorization_raw),
            "expires_at": authorization.expires_at.astimezone(UTC).isoformat(),
        },
        "historical_source_acceptance": {
            "receipt_id": policy.historical.receipt_id,
            "authorization_id": policy.historical.authorization_id,
            "publication_transaction_id": policy.historical.publication_transaction_id,
            "publication_discovery_pointer_sha256": (
                policy.historical.publication_discovery_pointer_sha256
            ),
            "historical_authorization_expiry_reused": False,
        },
        "validator_sources": _validator_sources(project_root),
        "claim_boundary": _claim_boundary(),
    }


def _validate_attestation_shape(payload: Mapping[str, object]) -> None:
    _exact_fields(
        payload,
        {
            "attestation_id",
            "schema_version",
            "status",
            "generated_at",
            "policy_id",
            "policy_version",
            "policy_path",
            "policy_sha256",
            "consumer_id",
            "consumer_version",
            "candidate",
            "copy_manifest",
            "data_quality",
            "durability",
            "acl",
            "consumer_authorization",
            "historical_source_acceptance",
            "validator_sources",
            "claim_boundary",
        },
        "CONSUMER_MIGRATION_ATTESTATION_INVALID",
    )
    if (
        payload.get("schema_version")
        != CONSUMER_MIGRATION_ATTESTATION_SCHEMA_VERSION
        or payload.get("status") != "PASS"
    ):
        _fail("CONSUMER_MIGRATION_ATTESTATION_INVALID", "schema/status mismatch")
    _aware_datetime(payload.get("generated_at"), "generated_at")


def _validate_historical_acceptance(
    source_root: Path,
    policy: ConsumerMigrationPolicy,
) -> tuple[DataQualityExecutionReceipt, DataQualityConsumerAuthorizationAttestation]:
    historical = policy.historical
    receipt_raw = _read_secure(
        source_root,
        historical.receipt_path,
        "CONSUMER_MIGRATION_HISTORICAL_RECEIPT_MISSING",
    )
    if hashlib.sha256(receipt_raw).hexdigest() != historical.receipt_sha256:
        _fail("CONSUMER_MIGRATION_HISTORICAL_RECEIPT_DRIFT", historical.receipt_path)
    try:
        receipt = DataQualityExecutionReceipt.from_json_bytes(receipt_raw)
    except DataQualityExecutionContractError as exc:
        raise DataFoundationConsumerMigrationError(exc.code, exc.message) from exc
    if (
        receipt.receipt_id != historical.receipt_id
        or receipt.as_of != historical.as_of
        or receipt.report.status != "PASS"
        or receipt.report.error_count != 0
        or receipt.report.warning_count != 0
        or tuple(sorted(item.role for item in receipt.inputs))
        != ("prices", "rates", "secondary_prices")
    ):
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_RECEIPT_INVALID",
            historical.receipt_id,
        )
    authorization_raw = _read_secure(
        source_root,
        historical.authorization_path,
        "CONSUMER_MIGRATION_HISTORICAL_AUTHORIZATION_MISSING",
    )
    if hashlib.sha256(authorization_raw).hexdigest() != historical.authorization_sha256:
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_AUTHORIZATION_DRIFT",
            historical.authorization_path,
        )
    try:
        authorization = DataQualityConsumerAuthorizationAttestation.from_json_bytes(
            authorization_raw
        )
    except DataQualityConsumerAuthorizationContractError as exc:
        raise DataFoundationConsumerMigrationError(exc.code, exc.message) from exc
    if (
        authorization.authorization_id != historical.authorization_id
        or authorization.receipt_id != historical.receipt_id
        or authorization.receipt_sha256 != historical.receipt_sha256
        or authorization.publication_transaction_id
        != historical.publication_transaction_id
        or authorization.publication_transaction_sha256
        != historical.publication_transaction_sha256
        or authorization.publication_discovery_pointer_sha256
        != historical.publication_discovery_pointer_sha256
        or authorization.consumer_id != policy.consumer_id
        or authorization.consumer_version != policy.consumer_version
        or authorization.receipt_status != "PASS"
    ):
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_AUTHORIZATION_INVALID",
            historical.authorization_id,
        )
    return receipt, authorization


def _select_historical_pointer(
    source_data_root: Path,
    policy: ConsumerMigrationPolicy,
) -> tuple[Path, bytes, dict[str, object]]:
    history_root = source_data_root / Path(_HISTORY_ROOT_RELATIVE)
    for path in _regular_files(history_root):
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != (
            policy.historical.publication_discovery_pointer_sha256
        ):
            continue
        pointer = _canonical_mapping(
            raw,
            "CONSUMER_MIGRATION_HISTORICAL_POINTER_INVALID",
        )
        if (
            pointer.get("run_id") != policy.historical.publication_transaction_id
            or _mapping(pointer.get("snapshot"), "snapshot").get("sha256")
            != policy.historical.publication_transaction_sha256
        ):
            _fail(
                "CONSUMER_MIGRATION_HISTORICAL_POINTER_INVALID",
                path.as_posix(),
            )
        return path, raw, pointer
    _fail(
        "CONSUMER_MIGRATION_HISTORICAL_POINTER_MISSING",
        policy.historical.publication_discovery_pointer_sha256,
        path=history_root,
    )


def _selected_transaction(
    source_data_root: Path,
    pointer: Mapping[str, object],
    policy: ConsumerMigrationPolicy,
) -> dict[str, object]:
    snapshot = _mapping(pointer.get("snapshot"), "snapshot")
    relative = _portable_path(snapshot.get("path"), "snapshot.path")
    raw = _read_secure(
        source_data_root / Path(policy.publication_store_dir),
        relative,
        "CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_MISSING",
    )
    if hashlib.sha256(raw).hexdigest() != (
        policy.historical.publication_transaction_sha256
    ):
        _fail("CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_DRIFT", relative)
    transaction = _canonical_mapping(
        raw,
        "CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID",
    )
    if transaction.get("transaction_id") != policy.historical.publication_transaction_id:
        _fail("CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID", relative)
    return transaction


def _validate_transaction_against_historical(
    transaction: Mapping[str, object],
    *,
    historical_receipt: DataQualityExecutionReceipt,
    historical_authorization: DataQualityConsumerAuthorizationAttestation,
    policy: ConsumerMigrationPolicy,
) -> None:
    artifact_by_role = {
        _text(_mapping(item, "artifacts[]").get("role"), "role"): _mapping(
            item,
            "artifacts[]",
        )
        for item in _sequence(transaction.get("artifacts"), "artifacts")
    }
    receipt_by_role = {item.role: item for item in historical_receipt.inputs}
    if set(artifact_by_role) != set(policy.required_input_roles):
        _fail("CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID", "artifact roles drift")
    for role in policy.required_input_roles:
        artifact = artifact_by_role[role]
        receipt = receipt_by_role[role]
        if (
            artifact.get("sha256") != receipt.sha256
            or artifact.get("row_count") != receipt.row_count
            or artifact.get("legacy_path") != PurePosixPath(receipt.path).name
        ):
            _fail(
                "CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID",
                f"role={role}",
            )
    manifest = _mapping(transaction.get("download_manifest"), "download_manifest")
    receipt_manifest_hashes = {item.manifest_sha256 for item in historical_receipt.inputs}
    if (
        receipt_manifest_hashes != {manifest.get("sha256")}
        or historical_authorization.receipt_id != historical_receipt.receipt_id
    ):
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID",
            "manifest/receipt binding drift",
        )


def _publish_candidate_from_historical(
    *,
    source_data_root: Path,
    candidate_data_root: Path,
    transaction: Mapping[str, object],
    selected_pointer_path: Path,
    selected_pointer_raw: bytes,
    policy: ConsumerMigrationPolicy,
    published_at: datetime,
) -> ValidatedDownloadPublication:
    store_root = source_data_root / Path(policy.publication_store_dir)
    manifest = _mapping(transaction.get("download_manifest"), "download_manifest")
    manifest_sha = _sha(manifest.get("sha256"), "download_manifest.sha256")
    manifest_size = _integer(
        manifest.get("size_bytes"),
        "download_manifest.size_bytes",
        minimum=1,
    )
    manifest_rows = _integer(
        manifest.get("row_count"),
        "download_manifest.row_count",
        minimum=1,
    )
    artifacts: list[DownloadArtifactCandidate] = []
    sources: list[DownloadSourceBinding] = []
    for item in _sequence(transaction.get("artifacts"), "artifacts"):
        artifact = _mapping(item, "artifacts[]")
        role = _text(artifact.get("role"), "role")
        source_relative = _publication_store_relative(
            artifact.get("immutable_path"),
            policy=policy,
            field="immutable_path",
        )
        legacy_name = _portable_path(artifact.get("legacy_path"), "legacy_path")
        if legacy_name not in policy.legacy_projection_files:
            _fail("CONSUMER_MIGRATION_COPY_INVALID", f"unexpected legacy path={legacy_name}")
        raw = _read_secure(
            store_root,
            source_relative,
            "CONSUMER_MIGRATION_HISTORICAL_MEMBER_MISSING",
        )
        if (
            hashlib.sha256(raw).hexdigest() != _sha(artifact.get("sha256"), "sha256")
            or len(raw) != _integer(artifact.get("size_bytes"), "size_bytes", minimum=1)
        ):
            _fail("CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT", source_relative)
        row_count = _integer(artifact.get("row_count"), "row_count", minimum=1)
        row_keys = _artifact_row_keys(raw, role=role)
        if len(row_keys) != row_count:
            _fail(
                "CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT",
                f"role={role} row_count mismatch",
            )
        event_id = f"{role}:legacy_local_cache_import:data_gov_001_d0e"
        artifacts.append(
            DownloadArtifactCandidate(
                role=role,
                filename=legacy_name,
                content=raw,
                row_count=row_count,
                source_event_ids=(event_id,),
            )
        )
        sources.append(
            DownloadSourceBinding(
                source_event_id=event_id,
                artifact_role=role,
                source_kind="LEGACY_LOCAL_CACHE_IMPORT",
                source_id="data_gov_001_d0e_historical_publication_import",
                provider="AITradingSystem historical canonical publication",
                endpoint=(
                    f"immutable_transaction:"
                    f"{policy.historical.publication_transaction_id}"
                ),
                request_parameters={
                    "cache_relative_path": legacy_name,
                    "cache_sha256": hashlib.sha256(raw).hexdigest(),
                    "cache_size_bytes": len(raw),
                    "cache_row_count": row_count,
                    "manifest_relative_path": "download_manifest.csv",
                    "manifest_sha256": manifest_sha,
                    "manifest_size_bytes": manifest_size,
                    "manifest_row_count": manifest_rows,
                    "manifest_binding_status": "MATCHED",
                    "raw_provider_provenance": False,
                    "origin_lineage_complete": False,
                    "origin_status": "OPAQUE_LEGACY",
                    "data_quality_provenance": False,
                    "historical_publication_transaction_id": (
                        policy.historical.publication_transaction_id
                    ),
                    "historical_publication_transaction_sha256": (
                        policy.historical.publication_transaction_sha256
                    ),
                    "historical_discovery_pointer_path": (
                        selected_pointer_path.as_posix()
                    ),
                    "historical_discovery_pointer_sha256": hashlib.sha256(
                        selected_pointer_raw
                    ).hexdigest(),
                },
                winning_row_count=row_count,
                allocation_mode="REMAINDER",
                winning_row_keys=row_keys,
                replay_inputs=(
                    DownloadReplayInputCandidate(
                        input_role="legacy_local_cache_bytes",
                        filename=legacy_name,
                        content=raw,
                        row_count=row_count,
                    ),
                ),
            )
        )
    requested_window = _mapping(transaction.get("requested_window"), "requested_window")
    publish_download_transaction(
        output_dir=candidate_data_root,
        requested_start=_date_value(requested_window.get("start"), "requested_window.start"),
        requested_end=_date_value(requested_window.get("end"), "requested_window.end"),
        artifacts=tuple(artifacts),
        source_bindings=tuple(sources),
        published_at=published_at,
    )
    return resolve_download_publication(output_dir=candidate_data_root)


def _artifact_row_keys(
    raw: bytes,
    *,
    role: str,
) -> tuple[tuple[str, str], ...]:
    key_field = "series" if role == "rates" else "ticker"
    try:
        reader = csv.DictReader(
            io.StringIO(raw.decode("utf-8-sig"), newline="")
        )
        if not {key_field, "date"}.issubset(reader.fieldnames or ()):
            _fail(
                "CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT",
                f"role={role} row-key columns missing",
            )
        keys = tuple(
            (
                _text(row.get(key_field), f"{role}.{key_field}"),
                _text(row.get("date"), f"{role}.date"),
            )
            for row in reader
        )
    except (UnicodeDecodeError, csv.Error) as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT",
            f"role={role}: {exc}",
        ) from exc
    if len(keys) != len(set(keys)):
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_MEMBER_DRIFT",
            f"role={role} duplicate row key",
        )
    return keys


def _copy_record(
    *,
    source_path: str,
    candidate_path: str,
    raw: bytes,
) -> dict[str, object]:
    return {
        "source_path": source_path,
        "candidate_path": candidate_path,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def _require_candidate_publication(
    publication: ValidatedDownloadPublication,
    *,
    policy: ConsumerMigrationPolicy,
    historical_receipt: DataQualityExecutionReceipt,
) -> None:
    expected_hashes = {
        item.role: item.sha256 for item in historical_receipt.inputs
    }
    expected_rows = {
        item.role: item.row_count for item in historical_receipt.inputs
    }
    if (
        publication.requested_end != policy.historical.as_of
        or dict(publication.artifact_sha256) != expected_hashes
        or dict(publication.artifact_row_count) != expected_rows
        or not publication.legacy_projection_verified
        or publication.consumer_cutover_allowed
        or publication.production_effect != "none"
    ):
        _fail(
            "CONSUMER_MIGRATION_PUBLICATION_MISMATCH",
            publication.transaction_id,
        )


def _validate_capability_evidence(
    policy: ConsumerMigrationPolicy,
    *,
    project_root: Path,
) -> None:
    root = project_root.resolve()
    d0c_path = root / Path(policy.capabilities.d0c_bundle_path)
    d0c_raw = _read_secure(
        root,
        policy.capabilities.d0c_bundle_path,
        "CONSUMER_MIGRATION_D0C_EVIDENCE_MISSING",
    )
    if hashlib.sha256(d0c_raw).hexdigest() != policy.capabilities.d0c_bundle_sha256:
        _fail("CONSUMER_MIGRATION_D0C_EVIDENCE_DRIFT", d0c_path.as_posix())
    d0c = _canonical_mapping(d0c_raw, "CONSUMER_MIGRATION_D0C_EVIDENCE_INVALID")
    body = dict(d0c)
    supplied_id = _text(body.pop("bundle_id"), "bundle_id")
    if (
        supplied_id != policy.capabilities.d0c_bundle_id
        or supplied_id != f"{_D0C_BUNDLE_ID_PREFIX}{_digest(body)[:32]}"
        or d0c.get("schema_version") != _D0C_BUNDLE_SCHEMA_VERSION
        or d0c.get("status") != "PASS"
        or d0c.get("fixture_scope") != "ISOLATED_REHEARSAL_ONLY"
        or d0c.get("store_acl_verified") is not False
        or d0c.get("consumer_cutover_allowed") is not False
        or d0c.get("production_effect") != "none"
    ):
        _fail("CONSUMER_MIGRATION_D0C_EVIDENCE_INVALID", supplied_id)
    bindings = _sequence(d0c.get("artifact_bindings"), "artifact_bindings")
    observed: dict[str, dict[str, object]] = {}
    for value in bindings:
        binding = _mapping(value, "artifact_bindings[]")
        path = _portable_path(binding.get("path"), "artifact_bindings.path")
        raw = _read_secure(
            d0c_path.parent,
            path,
            "CONSUMER_MIGRATION_D0C_ARTIFACT_MISSING",
        )
        if (
            hashlib.sha256(raw).hexdigest() != _sha(binding.get("sha256"), "sha256")
            or len(raw) != _integer(binding.get("size_bytes"), "size_bytes", minimum=1)
        ):
            _fail("CONSUMER_MIGRATION_D0C_ARTIFACT_DRIFT", path)
        observed[path] = _canonical_mapping(
            raw,
            "CONSUMER_MIGRATION_D0C_ARTIFACT_INVALID",
        )
    durability = observed.get("durability_attestation.json")
    if durability is None:
        _fail("CONSUMER_MIGRATION_D0C_ARTIFACT_MISSING", "durability_attestation.json")
    validate_durability_attestation(durability)
    if durability.get("attestation_id") != d0c.get("durability_attestation_id"):
        _fail("CONSUMER_MIGRATION_D0C_EVIDENCE_INVALID", "attestation id mismatch")

    d0d_path = root / Path(policy.capabilities.d0d_bundle_path)
    d0d_raw = _read_secure(
        root,
        policy.capabilities.d0d_bundle_path,
        "CONSUMER_MIGRATION_D0D_EVIDENCE_MISSING",
    )
    if hashlib.sha256(d0d_raw).hexdigest() != policy.capabilities.d0d_bundle_sha256:
        _fail("CONSUMER_MIGRATION_D0D_EVIDENCE_DRIFT", d0d_path.as_posix())
    d0d = validate_acl_rehearsal_bundle(
        d0d_path,
        policy_path=root / DEFAULT_ACL_POLICY_PATH,
    )
    if d0d.get("bundle_id") != policy.capabilities.d0d_bundle_id:
        _fail("CONSUMER_MIGRATION_D0D_EVIDENCE_INVALID", "bundle id mismatch")


def _read_rehearsal_runner(
    verified: VerifiedConsumerMigration,
) -> dict[str, object]:
    import csv

    prices_path = verified.candidate_data_root / "prices_daily.csv"
    observed: dict[str, dict[str, str]] = {}
    with prices_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("ticker", ""))
            if ticker in {"QQQ", "SGOV", "TQQQ"}:
                current = observed.get(ticker)
                if current is None or str(row.get("date", "")) > current["date"]:
                    observed[ticker] = {
                        "date": str(row.get("date", "")),
                        "adj_close": str(row.get("adj_close", "")),
                    }
    if set(observed) != {"QQQ", "SGOV", "TQQQ"}:
        _fail("CONSUMER_MIGRATION_REHEARSAL_INVALID", "required ticker read missing")
    body: dict[str, object] = {
        "schema_version": CONSUMER_REHEARSAL_RECEIPT_SCHEMA_VERSION,
        "status": "PASS",
        "consumer_id": verified.consumer_id,
        "consumer_version": verified.consumer_version,
        "migration_attestation_id": verified.attestation_id,
        "receipt_id": verified.receipt_id,
        "authorization_id": verified.authorization_id,
        "candidate_data_root": verified.candidate_data_root.as_posix(),
        "required_ticker_latest_rows": dict(sorted(observed.items())),
        "runner_calls": 1,
        "downstream_artifacts": 0,
        "investment_score_computed": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {"rehearsal_id": f"consumer_read_{_digest(body)[:32]}", **body}


def _store_identity(
    data_root: Path,
    *,
    publication: ValidatedDownloadPublication,
    copy_manifest_sha256: str,
    acl_attestation: Mapping[str, object],
) -> str:
    root = data_root.resolve(strict=True)
    metadata = root.stat()
    body = {
        "resolved_data_root": root.as_posix(),
        "device": int(metadata.st_dev),
        "inode": int(metadata.st_ino),
        "publication_transaction_id": publication.transaction_id,
        "publication_transaction_sha256": publication.transaction_manifest_sha256,
        "publication_discovery_pointer_sha256": publication.discovery_pointer_sha256,
        "copy_manifest_sha256": copy_manifest_sha256,
        "acl_store_identity": acl_attestation.get("store_identity"),
        "acl_security_descriptor_sha256": _mapping(
            acl_attestation.get("acl_snapshot"),
            "acl_snapshot",
        ).get("security_descriptor_sha256"),
    }
    return f"consumer_store_{_digest(body)[:32]}"


def _publication_binding(
    publication: ValidatedDownloadPublication,
    project_root: Path,
) -> dict[str, object]:
    return {
        "transaction_id": publication.transaction_id,
        "transaction_path": _repo_relative(
            project_root,
            publication.transaction_manifest_path,
        ),
        "transaction_sha256": publication.transaction_manifest_sha256,
        "discovery_pointer_path": _repo_relative(
            project_root,
            publication.discovery_pointer_path,
        ),
        "discovery_pointer_sha256": publication.discovery_pointer_sha256,
        "requested_start": publication.requested_start.isoformat(),
        "requested_end": publication.requested_end.isoformat(),
        "artifact_sha256": dict(sorted(publication.artifact_sha256.items())),
        "artifact_row_count": dict(sorted(publication.artifact_row_count.items())),
        "manifest_sha256": publication.manifest_sha256,
        "manifest_row_count": publication.manifest_row_count,
        "legacy_projection_verified": publication.legacy_projection_verified,
        "consumer_cutover_allowed": publication.consumer_cutover_allowed,
        "production_effect": publication.production_effect,
    }


def _artifact_pointer(
    path: Path,
    *,
    artifact_id: str,
    schema_version: str,
) -> dict[str, object]:
    raw = path.read_bytes()
    return {
        "path": path.name,
        "artifact_id": artifact_id,
        "schema_version": schema_version,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def _validator_sources(project_root: Path) -> list[dict[str, object]]:
    root = project_root.resolve()
    records: list[dict[str, object]] = []
    for relative in _MIGRATION_VALIDATOR_SOURCE_PATHS:
        raw = _read_secure(
            root,
            relative,
            "CONSUMER_MIGRATION_VALIDATOR_SOURCE_MISSING",
        )
        records.append({"path": relative, "sha256": hashlib.sha256(raw).hexdigest()})
    return records


def _load_candidate_universe(candidate_root: Path) -> UniverseConfig:
    raw = _read_secure(
        candidate_root,
        "config/universe.yaml",
        "CONSUMER_MIGRATION_SCAFFOLD_MISSING",
    )
    try:
        return UniverseConfig.model_validate(safe_load_yaml_text(raw.decode("utf-8")))
    except Exception as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_UNIVERSE_INVALID",
            str(exc),
        ) from exc


def _historical_policy(value: object) -> HistoricalSourceAcceptance:
    payload = _mapping(value, "historical_source_acceptance")
    expected = {
        "as_of",
        "receipt_id",
        "receipt_path",
        "receipt_sha256",
        "authorization_id",
        "authorization_path",
        "authorization_sha256",
        "publication_transaction_id",
        "publication_transaction_sha256",
        "publication_discovery_pointer_sha256",
    }
    _exact_fields(payload, expected, "CONSUMER_MIGRATION_POLICY_INVALID")
    return HistoricalSourceAcceptance(
        as_of=_date_value(payload.get("as_of"), "as_of"),
        receipt_id=_text(payload.get("receipt_id"), "receipt_id"),
        receipt_path=_portable_path(payload.get("receipt_path"), "receipt_path"),
        receipt_sha256=_sha(payload.get("receipt_sha256"), "receipt_sha256"),
        authorization_id=_text(payload.get("authorization_id"), "authorization_id"),
        authorization_path=_portable_path(
            payload.get("authorization_path"),
            "authorization_path",
        ),
        authorization_sha256=_sha(
            payload.get("authorization_sha256"),
            "authorization_sha256",
        ),
        publication_transaction_id=_text(
            payload.get("publication_transaction_id"),
            "publication_transaction_id",
        ),
        publication_transaction_sha256=_sha(
            payload.get("publication_transaction_sha256"),
            "publication_transaction_sha256",
        ),
        publication_discovery_pointer_sha256=_sha(
            payload.get("publication_discovery_pointer_sha256"),
            "publication_discovery_pointer_sha256",
        ),
    )


def _capability_policy(value: object) -> CapabilityEvidence:
    payload = _mapping(value, "capability_evidence")
    expected = {
        "d0c_bundle_path",
        "d0c_bundle_id",
        "d0c_bundle_sha256",
        "d0d_bundle_path",
        "d0d_bundle_id",
        "d0d_bundle_sha256",
    }
    _exact_fields(payload, expected, "CONSUMER_MIGRATION_POLICY_INVALID")
    return CapabilityEvidence(
        d0c_bundle_path=_portable_path(payload.get("d0c_bundle_path"), "d0c_bundle_path"),
        d0c_bundle_id=_text(payload.get("d0c_bundle_id"), "d0c_bundle_id"),
        d0c_bundle_sha256=_sha(payload.get("d0c_bundle_sha256"), "d0c_bundle_sha256"),
        d0d_bundle_path=_portable_path(payload.get("d0d_bundle_path"), "d0d_bundle_path"),
        d0d_bundle_id=_text(payload.get("d0d_bundle_id"), "d0d_bundle_id"),
        d0d_bundle_sha256=_sha(payload.get("d0d_bundle_sha256"), "d0d_bundle_sha256"),
    )


def _claim_boundary() -> dict[str, object]:
    return {
        "historical_false_fields_rewritten": False,
        "generic_consumer_cutover_allowed": False,
        "automatic_non_daily_dispatch": False,
        "qld_automatic_selection_enabled": False,
        "production_weights_written": False,
        "active_shadow_weights_written": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _prepare_output_root(output_root: Path, allowed_parent: Path) -> Path:
    parent = _resolved_plain_directory(allowed_parent)
    candidate = output_root.resolve(strict=False)
    if candidate == parent or not candidate.is_relative_to(parent):
        _fail(
            "CONSUMER_MIGRATION_OUTPUT_SCOPE_INVALID",
            "output root must be a child of the reviewed output parent",
            path=candidate,
        )
    if candidate.exists():
        if not candidate.is_dir() or any(candidate.iterdir()):
            _fail(
                "CONSUMER_MIGRATION_OUTPUT_NOT_EMPTY",
                "output root must be missing or empty",
                path=candidate,
            )
    else:
        candidate.mkdir()
    return candidate.resolve(strict=True)


def _regular_files(root: Path) -> tuple[Path, ...]:
    resolved = _resolved_plain_directory(root)
    files: list[Path] = []
    for current_root, directory_names, file_names in os.walk(resolved):
        directory_names.sort()
        file_names.sort()
        current = Path(current_root)
        for name in directory_names:
            path = current / name
            metadata = path.lstat()
            if not stat.S_ISDIR(metadata.st_mode) or _is_reparse(metadata):
                _fail("CONSUMER_MIGRATION_SOURCE_PATH_INVALID", path.as_posix(), path=path)
        for name in file_names:
            path = current / name
            metadata = path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or _is_reparse(metadata)
            ):
                _fail("CONSUMER_MIGRATION_SOURCE_PATH_INVALID", path.as_posix(), path=path)
            files.append(path)
    return tuple(files)


def _is_reparse(metadata: os.stat_result) -> bool:
    return bool(getattr(metadata, "st_file_attributes", 0) & 0x400)


def _resolved_plain_directory(path: Path) -> Path:
    try:
        if path.is_symlink():
            _fail("CONSUMER_MIGRATION_SOURCE_PATH_INVALID", "symlink root", path=path)
        root = path.resolve(strict=True)
        metadata = root.stat()
    except OSError as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_SOURCE_PATH_INVALID",
            str(exc),
            path=path,
        ) from exc
    if not root.is_dir() or _is_reparse(metadata):
        _fail("CONSUMER_MIGRATION_SOURCE_PATH_INVALID", "plain directory required", path=root)
    return root


def _write(root: Path, relative: str, raw: bytes) -> Path:
    try:
        result = write_contained_artifact_bytes(
            root=root,
            relative_path=_portable_path(relative, "relative_path"),
            content=raw,
            immutable=True,
        )
    except DataPublicationError as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_WRITE_FAILED",
            str(exc),
            path=getattr(exc, "path", None),
        ) from exc
    if result.sha256 != hashlib.sha256(raw).hexdigest() or result.size_bytes != len(raw):
        _fail("CONSUMER_MIGRATION_WRITE_FAILED", "durable writer result mismatch")
    return result.path


def _read_secure(root: Path, relative: str, missing_code: str) -> bytes:
    try:
        return read_contained_artifact_bytes(
            root=root,
            relative_path=_portable_path(relative, "relative_path"),
        )
    except DataPublicationError as exc:
        code = (
            missing_code
            if exc.code
            in {
                "CONTAINED_ARTIFACT_MISSING",
                "ARTIFACT_BOUND_DIRECTORY_FAILED",
            }
            else "CONSUMER_MIGRATION_SOURCE_PATH_INVALID"
        )
        raise DataFoundationConsumerMigrationError(
            code,
            str(exc),
            path=getattr(exc, "path", None),
        ) from exc


def _canonical_mapping(raw: bytes, code: str) -> dict[str, object]:
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=lambda item: (_raise_nonfinite(item)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataFoundationConsumerMigrationError(code, str(exc)) from exc
    payload = _mapping(value, "json")
    if canonical_json_bytes(payload) != raw:
        _fail(code, "JSON bytes are not canonical")
    return payload


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise DataFoundationConsumerMigrationError(
                "CONSUMER_MIGRATION_JSON_DUPLICATE_KEY",
                key,
            )
        value[key] = item
    return value


def _raise_nonfinite(value: str) -> NoReturn:
    raise DataFoundationConsumerMigrationError(
        "CONSUMER_MIGRATION_JSON_NONFINITE",
        value,
    )


def _repo_relative(root: Path, path: Path) -> str:
    candidate = path if path.is_absolute() else root / path
    try:
        return candidate.resolve(strict=False).relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise DataFoundationConsumerMigrationError(
            "CONSUMER_MIGRATION_PATH_ESCAPE",
            path.as_posix(),
        ) from exc


def _publication_store_relative(
    value: object,
    *,
    policy: ConsumerMigrationPolicy,
    field: str,
) -> str:
    relative = _portable_path(value, field)
    prefix = f"{policy.publication_store_dir}/"
    if not relative.startswith(prefix):
        _fail(
            "CONSUMER_MIGRATION_HISTORICAL_TRANSACTION_INVALID",
            f"{field} is outside publication store",
        )
    return relative.removeprefix(prefix)


def _portable_path(value: object, field: str) -> str:
    text = _text(value, field)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or "\\" in text
        or str(path) != text
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        _fail("CONSUMER_MIGRATION_PATH_INVALID", f"{field}={text}")
    return text


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be mapping")
    return dict(value)


def _sequence(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be list")
    return value


def _exact_fields(
    payload: Mapping[str, object],
    expected: set[str],
    code: str,
) -> None:
    if set(payload) != expected:
        _fail(
            code,
            f"fields expected={sorted(expected)} actual={sorted(payload)}",
        )


def _strings(value: object, field: str) -> tuple[str, ...]:
    values = _sequence(value, field)
    result = tuple(_text(item, f"{field}[]") for item in values)
    if len(result) != len(set(result)):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"duplicate {field}")
    return result


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be non-empty text")
    return value


def _sha(value: object, field: str) -> str:
    text = _text(value, field)
    if not _SHA256_RE.fullmatch(text):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be SHA-256")
    return text


def _integer(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail(
            "CONSUMER_MIGRATION_FIELDS_INVALID",
            f"{field} must be integer >= {minimum}",
        )
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be boolean")
    return value


def _date_value(value: object, field: str) -> date:
    if isinstance(value, datetime):
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be date")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise DataFoundationConsumerMigrationError(
                "CONSUMER_MIGRATION_FIELDS_INVALID",
                f"{field} must be ISO date",
            ) from exc
    _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be date")


def _aware_datetime(value: object, field: str) -> datetime:
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise DataFoundationConsumerMigrationError(
                "CONSUMER_MIGRATION_FIELDS_INVALID",
                f"{field} must be ISO datetime",
            ) from exc
    else:
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} must be datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("CONSUMER_MIGRATION_FIELDS_INVALID", f"{field} needs timezone")
    return parsed.astimezone(UTC)


def _digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _fail(
    code: str,
    message: str,
    *,
    path: Path | None = None,
) -> NoReturn:
    raise DataFoundationConsumerMigrationError(code, message, path=path)


__all__ = [
    "CONSUMER_MIGRATION_ATTESTATION_SCHEMA_VERSION",
    "CONSUMER_MIGRATION_BUNDLE_SCHEMA_VERSION",
    "CONSUMER_MIGRATION_COPY_MANIFEST_SCHEMA_VERSION",
    "CONSUMER_MIGRATION_POLICY_SCHEMA_VERSION",
    "CONSUMER_REHEARSAL_RECEIPT_SCHEMA_VERSION",
    "ConsumerMigrationPolicy",
    "DataFoundationConsumerMigrationError",
    "VerifiedConsumerMigration",
    "dispatch_isolated_daily_score_consumer",
    "load_consumer_migration_policy",
    "materialize_isolated_candidate",
    "run_isolated_consumer_migration_rehearsal",
    "validate_candidate_copy_manifest",
    "validate_consumer_migration_bundle",
    "verify_consumer_migration_attestation",
]
