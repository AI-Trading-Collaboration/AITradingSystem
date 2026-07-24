from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, NoReturn, Protocol, cast

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_consumer_authorization import (
    DataQualityConsumerAuthorizationAttestation,
    DataQualityConsumerAuthorizationContractError,
    VerifiedDataQualityConsumerAuthorization,
    _build_verified_data_quality_consumer_authorization,
    canonical_sha256,
)
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityExecutionContractError,
    DataQualityInputBinding,
    DataQualityInvocationParameter,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.data.download_publication import (
    DownloadPublicationError,
    ValidatedDownloadPublication,
    resolve_download_publication,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality_execution import (
    DataQualityExecutionError,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.yaml_loader import safe_load_yaml_text

DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH: Final = Path(
    "config/data_quality/arch_004_wave15_daily_score_consumer_authorization.yaml"
)
DEFAULT_CONSUMER_AUTHORIZATION_ROOT: Final = Path("outputs/data_quality/consumer_authorizations")
DAILY_SCORE_CONSUMER_ID: Final = "daily_score_daily"
DAILY_SCORE_CONSUMER_VERSION: Final = "1.0.0"
DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN: Final = (
    f"{DAILY_SCORE_CONSUMER_ID}@{DAILY_SCORE_CONSUMER_VERSION}"
)


class DataQualityConsumerAuthorizationError(DataQualityConsumerAuthorizationContractError):
    """Stable fail-closed data-domain error for the Wave15 authorization chain."""


@dataclass(frozen=True)
class ReviewedDataQualityConsumerAuthorizationPolicy:
    policy_id: str
    policy_version: str
    owner: str
    owner_decision_id: str
    path: str
    sha256: str
    consumer_id: str
    consumer_version: str
    execution_profile_id: str
    publication_output_dir: str
    authorization_ttl_hours: int
    accepted_data_quality_statuses: tuple[str, ...]
    required_input_roles: tuple[str, ...]


class ReceiptVerifier(Protocol):
    def __call__(
        self,
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: Sequence[str],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight: ...


class PublicationResolver(Protocol):
    def __call__(self, *, output_dir: Path) -> ValidatedDownloadPublication: ...


def load_reviewed_data_quality_consumer_authorization_policy(
    path: Path = DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> ReviewedDataQualityConsumerAuthorizationPolicy:
    root = project_root.resolve()
    relative = _repo_relative(root, path)
    content = _read_secure(root, relative, "DQ_CONSUMER_POLICY_MISSING")
    try:
        payload = safe_load_yaml_text(content.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise DataQualityConsumerAuthorizationError("DQ_CONSUMER_POLICY_INVALID", str(exc)) from exc
    raw = _mapping(payload, "policy")
    _exact_fields(
        raw,
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "owner",
            "status",
            "owner_decision_id",
            "profile",
            "governance",
            "safety",
        },
        "policy",
    )
    if raw["schema_version"] != "data_quality_consumer_authorization_policy.v1":
        _fail("DQ_CONSUMER_POLICY_INVALID", "unsupported schema")
    if raw["status"] != "REVIEWED":
        _fail("DQ_CONSUMER_POLICY_NOT_REVIEWED", str(raw["status"]))
    profile = _mapping(raw["profile"], "profile")
    _exact_fields(
        profile,
        {
            "consumer_id",
            "consumer_version",
            "execution_profile_id",
            "publication_output_dir",
            "authorization_ttl_hours",
            "accepted_data_quality_statuses",
            "required_input_roles",
            "require_download_publication_companion",
            "require_legacy_projection_verified",
            "require_exact_as_of",
            "require_exact_requested_window",
            "require_exact_evaluated_window",
            "reversible_authorization",
        },
        "profile",
    )
    required_true = (
        "require_download_publication_companion",
        "require_legacy_projection_verified",
        "require_exact_as_of",
        "require_exact_requested_window",
        "require_exact_evaluated_window",
        "reversible_authorization",
    )
    if any(profile[item] is not True for item in required_true):
        _fail("DQ_CONSUMER_POLICY_INVALID", "required strict profile flag is not true")
    safety = _mapping(raw["safety"], "safety")
    _exact_fields(
        safety,
        {
            "generic_consumer_cutover_allowed",
            "automatic_command_dispatch",
            "automatic_non_daily_dispatch",
            "authorized_consumer_ids",
            "real_periodic_operation_executed",
            "provider_refresh_executed",
            "production_effect",
            "broker_action",
        },
        "safety",
    )
    if (
        safety["generic_consumer_cutover_allowed"] is not False
        or safety["automatic_command_dispatch"] is not False
        or safety["automatic_non_daily_dispatch"] is not False
        or safety["real_periodic_operation_executed"] is not False
        or safety["provider_refresh_executed"] is not False
        or safety["production_effect"] != "none"
        or safety["broker_action"] != "none"
    ):
        _fail("DQ_CONSUMER_SCOPE_INVALID", "policy safety boundary expanded")
    consumer_id = _text(profile["consumer_id"], "profile.consumer_id")
    if _text_tuple(safety["authorized_consumer_ids"], "authorized_consumer_ids") != (consumer_id,):
        _fail("DQ_CONSUMER_SCOPE_INVALID", "authorized consumer set must be exact singleton")
    accepted = _text_tuple(
        profile["accepted_data_quality_statuses"], "accepted_data_quality_statuses"
    )
    if accepted != ("PASS",):
        _fail("DQ_WARNING_NOT_ALLOWED", f"accepted={accepted}")
    roles = tuple(sorted(_text_tuple(profile["required_input_roles"], "required_input_roles")))
    if len(roles) != len(set(roles)):
        _fail("DQ_INPUT_SET_MISMATCH", "required input roles are duplicated")
    ttl = _positive_integer(profile["authorization_ttl_hours"], "authorization_ttl_hours")
    return ReviewedDataQualityConsumerAuthorizationPolicy(
        policy_id=_text(raw["policy_id"], "policy_id"),
        policy_version=_text(raw["policy_version"], "policy_version"),
        owner=_text(raw["owner"], "owner"),
        owner_decision_id=_text(raw["owner_decision_id"], "owner_decision_id"),
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        consumer_id=consumer_id,
        consumer_version=_text(profile["consumer_version"], "profile.consumer_version"),
        execution_profile_id=_text(profile["execution_profile_id"], "profile.execution_profile_id"),
        publication_output_dir=_portable_path(
            profile["publication_output_dir"], "profile.publication_output_dir"
        ),
        authorization_ttl_hours=ttl,
        accepted_data_quality_statuses=accepted,
        required_input_roles=roles,
    )


def build_data_quality_consumer_authorization_attestation(
    *,
    policy: ReviewedDataQualityConsumerAuthorizationPolicy,
    preflight: VerifiedDataQualityPreflight,
    publication: ValidatedDownloadPublication,
    authorized_at: datetime,
    project_root: Path = PROJECT_ROOT,
) -> DataQualityConsumerAuthorizationAttestation:
    root = project_root.resolve()
    _aware_datetime(authorized_at, "authorized_at")
    try:
        preflight.assert_strict_passed()
    except (DataQualityExecutionError, DataQualityExecutionContractError) as exc:
        raise DataQualityConsumerAuthorizationError(exc.code, exc.message) from exc
    if preflight.status != "PASS":
        _fail("DQ_WARNING_NOT_ALLOWED", f"status={preflight.status}")
    receipt = preflight.receipt
    if receipt.as_of != receipt.requested_window.end:
        _fail("DQ_AS_OF_MISMATCH", "receipt as_of differs from requested window end")
    if tuple(sorted(item.role for item in receipt.inputs)) != policy.required_input_roles:
        _fail("DQ_INPUT_SET_MISMATCH", "receipt roles differ from reviewed consumer profile")
    _verify_execution_profile(receipt.invocation, policy.execution_profile_id)
    _verify_publication_projection(root, receipt.inputs, publication)
    receipt_lineage = canonical_sha256(
        {
            "requested_window": receipt.requested_window.to_dict(),
            "evaluated_window": receipt.evaluated_window.to_dict(),
            "policy": receipt.policy.to_dict(),
            "validator": receipt.validator.to_dict(),
            "invocation": [item.to_dict() for item in receipt.invocation],
            "inputs": [item.to_dict() for item in receipt.inputs],
            "report": receipt.report.to_dict(),
        }
    )
    publication_lineage = canonical_sha256(_publication_lineage(root, publication))
    return DataQualityConsumerAuthorizationAttestation(
        consumer_id=policy.consumer_id,
        consumer_version=policy.consumer_version,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_path=policy.path,
        policy_sha256=policy.sha256,
        owner_decision_id=policy.owner_decision_id,
        authorized_at=authorized_at.astimezone(UTC),
        expires_at=(
            authorized_at.astimezone(UTC) + timedelta(hours=policy.authorization_ttl_hours)
        ),
        as_of=receipt.as_of,
        requested_window=receipt.requested_window,
        evaluated_window=receipt.evaluated_window,
        receipt_id=preflight.receipt_id,
        receipt_path=preflight.receipt_path,
        receipt_sha256=preflight.receipt_sha256,
        receipt_size_bytes=preflight.receipt_size_bytes,
        receipt_status=preflight.status,
        receipt_lineage_sha256=receipt_lineage,
        input_roles=policy.required_input_roles,
        publication_transaction_id=publication.transaction_id,
        publication_transaction_path=_repo_relative(root, publication.transaction_manifest_path),
        publication_transaction_sha256=publication.transaction_manifest_sha256,
        publication_discovery_pointer_path=_repo_relative(root, publication.discovery_pointer_path),
        publication_discovery_pointer_sha256=publication.discovery_pointer_sha256,
        publication_requested_start=publication.requested_start,
        publication_requested_end=publication.requested_end,
        publication_lineage_sha256=publication_lineage,
    )


def default_data_quality_consumer_authorization_path(
    authorization_id: str,
    *,
    root: Path = DEFAULT_CONSUMER_AUTHORIZATION_ROOT,
) -> Path:
    _text(authorization_id, "authorization_id")
    return root / authorization_id / "attestation.json"


def write_data_quality_consumer_authorization_attestation(
    attestation: DataQualityConsumerAuthorizationAttestation,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    root = project_root.resolve()
    relative = default_data_quality_consumer_authorization_path(
        attestation.authorization_id
    ).as_posix()
    try:
        result = write_contained_artifact_bytes(
            root=root,
            relative_path=relative,
            content=attestation.canonical_bytes,
            immutable=True,
        )
    except DataPublicationError as exc:
        code = (
            "DQ_CONSUMER_AUTHORIZATION_ID_MISMATCH"
            if exc.code == "IMMUTABLE_ARTIFACT_COLLISION"
            else "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID"
        )
        raise DataQualityConsumerAuthorizationError(code, str(exc)) from exc
    if result.sha256 != attestation.canonical_sha256 or result.size_bytes != len(
        attestation.canonical_bytes
    ):
        _fail("DQ_CONSUMER_AUTHORIZATION_ID_MISMATCH", "write attestation mismatch")
    return result.path


def verify_data_quality_consumer_authorization(
    attestation_path: Path,
    *,
    expected_consumer_id: str,
    expected_consumer_version: str,
    expected_as_of: date,
    expected_data_quality_policy_path: Path,
    policy_path: Path = DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    receipt_verifier: ReceiptVerifier = verify_data_quality_execution_receipt,
    publication_resolver: PublicationResolver = resolve_download_publication,
    now: datetime | None = None,
    project_root: Path = PROJECT_ROOT,
) -> VerifiedDataQualityConsumerAuthorization:
    root = project_root.resolve()
    policy = load_reviewed_data_quality_consumer_authorization_policy(
        policy_path, project_root=root
    )
    if expected_consumer_id != policy.consumer_id:
        _fail(
            "DQ_CONSUMER_MISMATCH",
            expected_consumer_id,
        )
    if expected_consumer_version != policy.consumer_version:
        _fail("DQ_CONSUMER_PROFILE_MISMATCH", expected_consumer_version)
    relative = _repo_relative(root, attestation_path)
    content = _read_secure(root, relative, "DQ_CONSUMER_AUTHORIZATION_MISSING")
    try:
        attestation = DataQualityConsumerAuthorizationAttestation.from_json_bytes(content)
    except DataQualityConsumerAuthorizationContractError as exc:
        raise DataQualityConsumerAuthorizationError(exc.code, exc.message) from exc
    expected_path = default_data_quality_consumer_authorization_path(
        attestation.authorization_id
    ).as_posix()
    if relative != expected_path:
        _fail(
            "DQ_CONSUMER_AUTHORIZATION_ID_MISMATCH",
            f"expected={expected_path} actual={relative}",
        )
    if attestation.consumer_id != policy.consumer_id:
        _fail("DQ_CONSUMER_MISMATCH", attestation.consumer_id)
    if attestation.consumer_version != policy.consumer_version:
        _fail("DQ_CONSUMER_PROFILE_MISMATCH", attestation.consumer_version)
    if attestation.as_of != expected_as_of:
        _fail("DQ_AS_OF_MISMATCH", attestation.as_of.isoformat())
    verified_at = (now or datetime.now(tz=UTC)).astimezone(UTC)
    if verified_at >= attestation.expires_at:
        _fail("DQ_CONSUMER_AUTHORIZATION_EXPIRED", attestation.authorization_id)
    try:
        preflight = receipt_verifier(
            Path(attestation.receipt_path),
            expected_as_of=expected_as_of,
            expected_policy_path=expected_data_quality_policy_path,
            expected_input_roles=policy.required_input_roles,
            project_root=root,
        )
        publication = publication_resolver(output_dir=root / Path(policy.publication_output_dir))
    except (DataQualityExecutionError, DownloadPublicationError, OSError) as exc:
        code = getattr(exc, "code", "DQ_CONSUMER_AUTHORIZATION_NOT_VERIFIED")
        raise DataQualityConsumerAuthorizationError(str(code), str(exc)) from exc
    rebuilt = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=attestation.authorized_at,
        project_root=root,
    )
    if content != rebuilt.canonical_bytes:
        _fail(
            "DQ_CONSUMER_AUTHORIZATION_LINEAGE_MISMATCH",
            "live receipt/publication/profile projection differs",
        )
    try:
        return _build_verified_data_quality_consumer_authorization(
            attestation=attestation,
            preflight=preflight,
            verified_at=verified_at,
        )
    except DataQualityConsumerAuthorizationContractError as exc:
        raise DataQualityConsumerAuthorizationError(exc.code, exc.message) from exc


def _verify_execution_profile(
    invocation: Sequence[DataQualityInvocationParameter], expected_profile_id: str
) -> None:
    values = {item.name: item.value_json for item in invocation}
    if values.get("execution_profile_id") != f'"{expected_profile_id}"':
        _fail("DQ_CONSUMER_PROFILE_MISMATCH", str(values.get("execution_profile_id")))


def _verify_publication_projection(
    root: Path,
    inputs: Sequence[DataQualityInputBinding],
    publication: ValidatedDownloadPublication,
) -> None:
    if (
        not publication.legacy_projection_verified
        or publication.consumer_cutover_allowed
        or publication.production_effect != "none"
    ):
        _fail("DQ_PUBLICATION_NOT_VERIFIED", publication.transaction_id)
    expected_paths = {
        "prices": _repo_relative(root, publication.legacy_prices_path),
        "rates": _repo_relative(root, publication.legacy_rates_path),
    }
    if publication.legacy_secondary_prices_path is not None:
        expected_paths["secondary_prices"] = _repo_relative(
            root, publication.legacy_secondary_prices_path
        )
    expected_manifest = _repo_relative(root, publication.legacy_manifest_path)
    observed_roles: set[str] = set()
    for item in inputs:
        if item.role not in expected_paths:
            _fail("DQ_INPUT_SET_MISMATCH", f"unexpected role={item.role}")
        if item.path != expected_paths[item.role]:
            _fail("DQ_PUBLICATION_SOURCE_MISMATCH", f"role={item.role}")
        if item.manifest_path != expected_manifest:
            _fail("DQ_MANIFEST_SHA_MISMATCH", f"role={item.role}")
        observed_roles.add(item.role)
    if observed_roles != set(expected_paths):
        _fail("DQ_INPUT_SET_MISMATCH", "publication/receipt role sets differ")


def _publication_lineage(
    root: Path, publication: ValidatedDownloadPublication
) -> dict[str, object]:
    return {
        "transaction_id": publication.transaction_id,
        "transaction_manifest_path": _repo_relative(root, publication.transaction_manifest_path),
        "transaction_manifest_sha256": publication.transaction_manifest_sha256,
        "discovery_pointer_path": _repo_relative(root, publication.discovery_pointer_path),
        "discovery_pointer_sha256": publication.discovery_pointer_sha256,
        "requested_start": publication.requested_start.isoformat(),
        "requested_end": publication.requested_end.isoformat(),
        "artifacts": dict(sorted(publication.artifact_sha256.items())),
        "artifact_row_count": dict(sorted(publication.artifact_row_count.items())),
        "manifest_sha256": publication.manifest_sha256,
        "manifest_row_count": publication.manifest_row_count,
        "legacy_projection_verified": publication.legacy_projection_verified,
        "consumer_cutover_allowed": publication.consumer_cutover_allowed,
        "production_effect": publication.production_effect,
    }


def _repo_relative(root: Path, value: Path) -> str:
    candidate = value if value.is_absolute() else root / value
    absolute = candidate.resolve(strict=False)
    try:
        relative = absolute.relative_to(root)
    except ValueError as exc:
        raise DataQualityConsumerAuthorizationError(
            "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
            f"path outside project root: {value}",
        ) from exc
    return _portable_path(relative.as_posix(), "path")


def _portable_path(value: object, field: str) -> str:
    text = _text(value, field)
    candidate = Path(text)
    if (
        candidate.is_absolute()
        or "\\" in text
        or text != candidate.as_posix()
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"invalid {field}")
    return text


def _read_secure(root: Path, relative: str, missing_code: str) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative)
    except DataPublicationError as exc:
        code = (
            missing_code
            if exc.code in {"CONTAINED_ARTIFACT_MISSING", "ARTIFACT_BOUND_DIRECTORY_FAILED"}
            else "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID"
        )
        raise DataQualityConsumerAuthorizationError(code, str(exc)) from exc


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail("DQ_CONSUMER_POLICY_INVALID", f"{field} must be mapping")
    return dict(cast(Mapping[str, object], value))


def _exact_fields(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(payload) != expected:
        _fail(
            "DQ_CONSUMER_POLICY_INVALID",
            f"{field} fields expected={sorted(expected)} actual={sorted(payload)}",
        )


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("DQ_CONSUMER_POLICY_INVALID", f"{field} must be non-empty text")
    return value


def _text_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        _fail("DQ_CONSUMER_POLICY_INVALID", f"{field} must be list")
    return tuple(_text(item, f"{field}[]") for item in value)


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail("DQ_CONSUMER_POLICY_INVALID", f"{field} must be positive integer")
    return value


def _aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must include timezone")
    return value


def _fail(code: str, message: str) -> NoReturn:
    raise DataQualityConsumerAuthorizationError(code, message)


__all__ = [
    "DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH",
    "DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN",
    "DAILY_SCORE_CONSUMER_ID",
    "DAILY_SCORE_CONSUMER_VERSION",
    "DataQualityConsumerAuthorizationError",
    "ReviewedDataQualityConsumerAuthorizationPolicy",
    "build_data_quality_consumer_authorization_attestation",
    "default_data_quality_consumer_authorization_path",
    "load_reviewed_data_quality_consumer_authorization_policy",
    "verify_data_quality_consumer_authorization",
    "write_data_quality_consumer_authorization_attestation",
]
