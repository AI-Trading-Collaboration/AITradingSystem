from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_capability import (
    ConsumerDataCapabilityDependency,
    ConsumerDataCapabilityDiscoveryPointer,
    ConsumerDataCapabilityPolicy,
    ConsumerDataCapabilityReceipt,
    DataQualityCapabilityContractError,
    VerifiedConsumerDataCapabilityPreflight,
    _build_verified_consumer_data_capability_preflight,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality_capability import (
    verify_consumer_data_capability_receipt,
)
from ai_trading_system.yaml_loader import safe_load_yaml_text


@dataclass(frozen=True)
class PublishedConsumerDataCapabilityDiscovery:
    pointer_path: Path
    pointer: ConsumerDataCapabilityDiscoveryPointer
    retained_receipt_path: Path
    receipt: ConsumerDataCapabilityReceipt


def load_consumer_data_capability_dependency(
    path: Path,
    *,
    project_root: Path = PROJECT_ROOT,
) -> ConsumerDataCapabilityDependency:
    root = _resolved_root(project_root)
    relative_path = _relative_path(root, path)
    content = _read_secure(root, relative_path)
    try:
        payload = safe_load_yaml_text(content.decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise TypeError("consumer dependency root must be a mapping")
        return ConsumerDataCapabilityDependency.model_validate(dict(payload))
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_INVALID",
            f"{relative_path}: {exc}",
        ) from exc


def build_consumer_data_capability_dependency(
    *,
    capability_policy_path: Path,
    data_quality_policy_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> ConsumerDataCapabilityDependency:
    root = _resolved_root(project_root)
    capability_relative = _relative_path(root, capability_policy_path)
    data_quality_relative = _relative_path(root, data_quality_policy_path)
    capability_bytes = _read_secure(root, capability_relative)
    data_quality_bytes = _read_secure(root, data_quality_relative)
    try:
        payload = safe_load_yaml_text(capability_bytes.decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise TypeError("capability policy root must be a mapping")
        policy = ConsumerDataCapabilityPolicy.model_validate(dict(payload))
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_POLICY_INVALID",
            f"{capability_relative}: {exc}",
        ) from exc
    return ConsumerDataCapabilityDependency(
        schema_version="data_quality_consumer_dependency.v1",
        consumer_id=policy.consumer_id,
        consumer_version=policy.consumer_version,
        capability_id=policy.capability_id,
        capability_version=policy.capability_version,
        capability_policy_path=capability_relative,
        capability_policy_sha256=hashlib.sha256(capability_bytes).hexdigest(),
        data_quality_policy_path=data_quality_relative,
        data_quality_policy_sha256=hashlib.sha256(data_quality_bytes).hexdigest(),
        accepted_receipt_schema_version="data_quality_consumer_capability_receipt.v1",
        strict_pass_required=True,
        cross_consumer_reuse_allowed=False,
        daily_operation_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def consumer_data_capability_discovery_path(
    dependency: ConsumerDataCapabilityDependency,
    as_of: date,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    _require_dependency(dependency)
    _require_date(as_of, "as_of")
    root = _resolved_root(project_root)
    return root / Path(_discovery_relative_path(dependency, as_of))


def publish_consumer_data_capability_discovery(
    *,
    dependency: ConsumerDataCapabilityDependency,
    source_receipt_path: Path,
    project_root: Path = PROJECT_ROOT,
    published_at: datetime | None = None,
) -> PublishedConsumerDataCapabilityDiscovery:
    root = _resolved_root(project_root)
    _verify_dependency_files(root, dependency)
    source_relative = _relative_path(root, source_receipt_path)
    receipt, receipt_bytes = _verify_receipt_bytes(
        root,
        source_relative,
        dependency=dependency,
    )
    retained_relative = _retained_receipt_relative_path(receipt.receipt_id)
    _write_secure(root, retained_relative, receipt_bytes, immutable=True)
    checked_published_at = _utc_datetime(
        published_at or datetime.now(UTC),
        "published_at",
    )
    if checked_published_at < receipt.generated_at:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_CHRONOLOGY_INVALID",
            "published_at cannot precede receipt generation",
        )
    pointer = ConsumerDataCapabilityDiscoveryPointer(
        schema_version="data_quality_consumer_capability_discovery_pointer.v1",
        dependency_id=dependency.dependency_id,
        consumer_id=dependency.consumer_id,
        consumer_version=dependency.consumer_version,
        capability_id=dependency.capability_id,
        capability_version=dependency.capability_version,
        as_of=receipt.as_of,
        published_at=checked_published_at,
        receipt_id=receipt.receipt_id,
        receipt_path=retained_relative,
        receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        receipt_size_bytes=len(receipt_bytes),
    )
    pointer_relative = _discovery_relative_path(dependency, receipt.as_of)
    if _read_secure_if_present(root, pointer_relative) is not None:
        current = _load_discovery(root, dependency=dependency, as_of=receipt.as_of)
        if receipt.generated_at < current.receipt.generated_at:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_DISCOVERY_REGRESSION",
                (
                    f"candidate={receipt.generated_at.isoformat()} "
                    f"current={current.receipt.generated_at.isoformat()}"
                ),
            )
        if (
            receipt.generated_at == current.receipt.generated_at
            and receipt.receipt_id != current.receipt.receipt_id
        ):
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_DISCOVERY_COLLISION",
                "same generated_at maps to different receipts",
            )
    _write_secure(root, pointer_relative, pointer.canonical_bytes, immutable=False)
    return _load_discovery(root, dependency=dependency, as_of=receipt.as_of)


def verify_consumer_data_capability_preflight(
    *,
    dependency: ConsumerDataCapabilityDependency,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    verified_at: datetime | None = None,
) -> VerifiedConsumerDataCapabilityPreflight:
    root = _resolved_root(project_root)
    discovered = _load_discovery(root, dependency=dependency, as_of=as_of)
    checked_at = _utc_datetime(verified_at or datetime.now(UTC), "verified_at")
    if checked_at < discovered.pointer.published_at:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_CHRONOLOGY_INVALID",
            "verified_at cannot precede pointer publication",
        )
    pointer_bytes = _read_secure(
        root,
        _relative_path(root, discovered.pointer_path),
    )
    receipt_bytes = _read_secure(
        root,
        _relative_path(root, discovered.retained_receipt_path),
    )
    return _build_verified_consumer_data_capability_preflight(
        dependency=dependency,
        receipt=discovered.receipt,
        pointer_path=_relative_path(root, discovered.pointer_path),
        pointer_sha256=hashlib.sha256(pointer_bytes).hexdigest(),
        receipt_path=_relative_path(root, discovered.retained_receipt_path),
        receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        receipt_size_bytes=len(receipt_bytes),
        verified_at=checked_at,
    )


def read_verified_consumer_data_capability_input(
    *,
    preflight: VerifiedConsumerDataCapabilityPreflight,
    role: str,
    project_root: Path = PROJECT_ROOT,
) -> bytes:
    if not isinstance(preflight, VerifiedConsumerDataCapabilityPreflight):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_NOT_VERIFIED",
            "sealed verified preflight is required",
        )
    checked_role = str(role).strip()
    if not checked_role:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_INPUT_ROLE_INVALID",
            "materialized input role is required",
        )
    matches = [
        binding for binding in preflight.receipt.materialized_inputs if binding.role == checked_role
    ]
    if len(matches) != 1:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_INPUT_ROLE_INVALID",
            checked_role,
        )
    binding = matches[0]
    root = _resolved_root(project_root)
    content = _read_secure(root, _relative_path(root, Path(binding.path)))
    if hashlib.sha256(content).hexdigest() != binding.sha256 or len(content) != binding.size_bytes:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
            checked_role,
        )
    return content


def _load_discovery(
    root: Path,
    *,
    dependency: ConsumerDataCapabilityDependency,
    as_of: date,
) -> PublishedConsumerDataCapabilityDiscovery:
    _require_dependency(dependency)
    checked_as_of = _require_date(as_of, "as_of")
    _verify_dependency_files(root, dependency)
    pointer_relative = _discovery_relative_path(dependency, checked_as_of)
    pointer_path = root / Path(pointer_relative)
    pointer_bytes = _read_secure(root, pointer_relative)
    pointer = ConsumerDataCapabilityDiscoveryPointer.from_json_bytes(pointer_bytes)
    if (
        pointer.dependency_id != dependency.dependency_id
        or pointer.consumer_id != dependency.consumer_id
        or pointer.consumer_version != dependency.consumer_version
        or pointer.capability_id != dependency.capability_id
        or pointer.capability_version != dependency.capability_version
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH",
            pointer.receipt_id,
        )
    if pointer.as_of != checked_as_of:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_AS_OF_MISMATCH",
            f"expected={checked_as_of.isoformat()} actual={pointer.as_of.isoformat()}",
        )
    expected_receipt_relative = _retained_receipt_relative_path(pointer.receipt_id)
    if pointer.receipt_path != expected_receipt_relative:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_RECEIPT_PATH_MISMATCH",
            pointer.receipt_path,
        )
    receipt_bytes = _read_secure(root, pointer.receipt_path)
    if (
        hashlib.sha256(receipt_bytes).hexdigest() != pointer.receipt_sha256
        or len(receipt_bytes) != pointer.receipt_size_bytes
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
            "discovered receipt bytes differ from pointer",
        )
    receipt, verified_bytes = _verify_receipt_bytes(
        root,
        pointer.receipt_path,
        dependency=dependency,
    )
    if verified_bytes != receipt_bytes or receipt.receipt_id != pointer.receipt_id:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_RECEIPT_ID_MISMATCH",
            pointer.receipt_id,
        )
    if receipt.as_of != checked_as_of:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_AS_OF_MISMATCH",
            receipt.receipt_id,
        )
    if pointer.published_at < receipt.generated_at:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_CHRONOLOGY_INVALID",
            "pointer publication precedes receipt generation",
        )
    return PublishedConsumerDataCapabilityDiscovery(
        pointer_path=pointer_path,
        pointer=pointer,
        retained_receipt_path=root / Path(pointer.receipt_path),
        receipt=receipt,
    )


def _verify_receipt_bytes(
    root: Path,
    relative_path: str,
    *,
    dependency: ConsumerDataCapabilityDependency,
) -> tuple[ConsumerDataCapabilityReceipt, bytes]:
    receipt_bytes = _read_secure(root, relative_path)
    receipt = ConsumerDataCapabilityReceipt.from_json_bytes(receipt_bytes)
    _assert_dependency_receipt(dependency, receipt)
    captured_bindings = _capture_receipt_bindings(root, receipt)
    verified = verify_consumer_data_capability_receipt(
        root / Path(relative_path),
        capability_policy_path=root / Path(dependency.capability_policy_path),
        data_quality_policy_path=root / Path(dependency.data_quality_policy_path),
    )
    if verified != receipt:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_RECEIPT_ID_MISMATCH",
            receipt.receipt_id,
        )
    if _read_secure(root, relative_path) != receipt_bytes:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
            "receipt changed during verification",
        )
    if _capture_receipt_bindings(root, receipt) != captured_bindings:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
            "bound input or report changed during verification",
        )
    if (
        not receipt.capability_passed
        or receipt.scoped_quality.status != "PASS"
        or receipt.unisolated_global_error_codes
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_STRICT_PASS_REQUIRED",
            receipt.receipt_id,
        )
    return receipt, receipt_bytes


def _capture_receipt_bindings(
    root: Path,
    receipt: ConsumerDataCapabilityReceipt,
) -> tuple[tuple[str, str, str, int], ...]:
    bindings = (
        *receipt.canonical_inputs,
        *receipt.materialized_inputs,
        receipt.full_quality.report,
        receipt.scoped_quality.report,
    )
    captured: list[tuple[str, str, str, int]] = []
    for binding in bindings:
        relative = _relative_path(root, Path(binding.path))
        content = _read_secure(root, relative)
        observed_sha = hashlib.sha256(content).hexdigest()
        if observed_sha != binding.sha256 or len(content) != binding.size_bytes:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
                binding.role,
            )
        captured.append((binding.role, relative, observed_sha, len(content)))
    return tuple(sorted(captured))


def _verify_dependency_files(
    root: Path,
    dependency: ConsumerDataCapabilityDependency,
) -> None:
    _require_dependency(dependency)
    capability_bytes = _read_secure(root, dependency.capability_policy_path)
    data_quality_bytes = _read_secure(root, dependency.data_quality_policy_path)
    if (
        hashlib.sha256(capability_bytes).hexdigest() != dependency.capability_policy_sha256
        or hashlib.sha256(data_quality_bytes).hexdigest() != dependency.data_quality_policy_sha256
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH",
            "policy bytes differ from dependency declaration",
        )
    try:
        payload = safe_load_yaml_text(capability_bytes.decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise TypeError("capability policy root must be a mapping")
        policy = ConsumerDataCapabilityPolicy.model_validate(dict(payload))
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_POLICY_INVALID",
            str(exc),
        ) from exc
    if (
        policy.consumer_id != dependency.consumer_id
        or policy.consumer_version != dependency.consumer_version
        or policy.capability_id != dependency.capability_id
        or policy.capability_version != dependency.capability_version
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH",
            dependency.dependency_id,
        )


def _assert_dependency_receipt(
    dependency: ConsumerDataCapabilityDependency,
    receipt: ConsumerDataCapabilityReceipt,
) -> None:
    if (
        receipt.schema_version != dependency.accepted_receipt_schema_version
        or receipt.consumer_id != dependency.consumer_id
        or receipt.consumer_version != dependency.consumer_version
        or receipt.capability_id != dependency.capability_id
        or receipt.capability_version != dependency.capability_version
        or receipt.policy_sha256 != dependency.capability_policy_sha256
        or receipt.data_quality_policy_sha256 != dependency.data_quality_policy_sha256
        or receipt.cross_consumer_reuse_allowed != dependency.cross_consumer_reuse_allowed
        or receipt.daily_operation_authorized != dependency.daily_operation_authorized
        or receipt.production_effect != dependency.production_effect
        or receipt.broker_action != dependency.broker_action
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH",
            receipt.receipt_id,
        )


def _discovery_relative_path(
    dependency: ConsumerDataCapabilityDependency,
    as_of: date,
) -> str:
    return (
        "outputs/data_quality/capabilities/discovery/"
        f"{dependency.consumer_id}/{dependency.consumer_version}/"
        f"{as_of.isoformat()}/current.json"
    )


def _retained_receipt_relative_path(receipt_id: str) -> str:
    return f"outputs/data_quality/capabilities/receipts/{receipt_id}.json"


def _resolved_root(project_root: Path) -> Path:
    try:
        return project_root.resolve(strict=True)
    except OSError as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_ROOT_INVALID",
            str(exc),
        ) from exc


def _relative_path(root: Path, path: Path) -> str:
    candidate = path if path.is_absolute() else root / path
    absolute = Path(os.path.abspath(candidate))
    try:
        relative = absolute.relative_to(root)
    except ValueError as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_PATH_OUTSIDE_ROOT",
            str(path),
        ) from exc
    if any(part in {"", ".", ".."} for part in relative.parts) or not relative.parts:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_PATH_INVALID",
            str(path),
        )
    return relative.as_posix()


def _read_secure(root: Path, relative_path: str) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SECURE_READ_FAILED",
            f"{relative_path}: {exc}",
        ) from exc


def _read_secure_if_present(root: Path, relative_path: str) -> bytes | None:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        if exc.code in {
            "CONTAINED_ARTIFACT_MISSING",
            "ARTIFACT_BOUND_DIRECTORY_FAILED",
        }:
            return None
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SECURE_READ_FAILED",
            f"{relative_path}: {exc}",
        ) from exc


def _write_secure(
    root: Path,
    relative_path: str,
    content: bytes,
    *,
    immutable: bool,
) -> None:
    try:
        result = write_contained_artifact_bytes(
            root=root,
            relative_path=relative_path,
            content=content,
            immutable=immutable,
        )
    except DataPublicationError as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SECURE_WRITE_FAILED",
            f"{relative_path}: {exc}",
        ) from exc
    if result.sha256 != hashlib.sha256(content).hexdigest() or result.size_bytes != len(content):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH",
            relative_path,
        )


def _require_dependency(
    dependency: ConsumerDataCapabilityDependency,
) -> ConsumerDataCapabilityDependency:
    if not isinstance(dependency, ConsumerDataCapabilityDependency):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH",
            "typed consumer dependency is required",
        )
    return dependency


def _require_date(value: date, field: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_AS_OF_MISMATCH",
            f"{field} must be a date",
        )
    return value


def _utc_datetime(value: datetime, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_CHRONOLOGY_INVALID",
            f"{field} must be UTC",
        )
    return value


__all__ = [
    "PublishedConsumerDataCapabilityDiscovery",
    "build_consumer_data_capability_dependency",
    "consumer_data_capability_discovery_path",
    "load_consumer_data_capability_dependency",
    "publish_consumer_data_capability_discovery",
    "read_verified_consumer_data_capability_input",
    "verify_consumer_data_capability_preflight",
]
