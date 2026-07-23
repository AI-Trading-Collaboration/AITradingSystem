from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import NoReturn

from ai_trading_system.contracts import ArtifactPointer, DataQualityEvidence
from ai_trading_system.data.immutable_publish import (
    DATA_QUALITY_REPORT_SCHEMA_VERSION,
    SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    CurrentPointerPrecondition,
    DataPublicationError,
    SnapshotPublishRequest,
    SourceEventProvenance,
    ValidatedCurrentSnapshot,
    publish_immutable_snapshot,
    read_contained_artifact_bytes,
    validate_current_snapshot,
    write_contained_artifact_bytes,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes, sha256_bytes

DOWNLOAD_PUBLICATION_SCHEMA_VERSION = "download_publication_transaction.v1"
DOWNLOAD_DISCOVERY_SCHEMA_VERSION = "data_current_pointer.v1"
DOWNLOAD_PUBLICATION_CONTRACT_VERSION = "v1"

_PUBLICATION_ROOT = ".download_publications"
_DATASET_ID = "download_composite"
_CURRENT_POINTER = f"{_PUBLICATION_ROOT}/current/{_DATASET_ID}.json"
_PAYLOAD_ARTIFACT_TYPE = "json"
_STRUCTURAL_DQ_CONTRACT_ID = "download_publication_structure"
_STRUCTURAL_DQ_POLICY_ID = "download_publication_structure"
_STRUCTURAL_DQ_POLICY_VERSION = "download_publication_structure.v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TRANSACTION_ID_RE = re.compile(r"^download_txn_[0-9a-f]{32}$")
_ARTIFACT_FILENAMES = {
    "prices": "prices_daily.csv",
    "rates": "rates_daily.csv",
    "secondary_prices": "prices_marketstack_daily.csv",
}
_LEGACY_BOOTSTRAP_PATHS = (
    "prices_daily.csv",
    "prices_marketstack_daily.csv",
    "download_manifest.csv",
)
_REQUIRED_COLUMNS = {
    "prices": ("date", "ticker", "open", "high", "low", "close", "adj_close", "volume"),
    "rates": ("date", "series", "value"),
    "secondary_prices": (
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ),
}
_MANIFEST_COLUMNS = (
    "downloaded_at",
    "source_id",
    "provider",
    "endpoint",
    "request_parameters",
    "output_path",
    "row_count",
    "checksum_sha256",
)
_SEMANTIC_CLAIMS = (
    "ATOMIC_COMPOSITE_PUBLICATION",
    "EXACT_ARTIFACT_SOURCE_BINDING",
    "FAIL_CLOSED_ZERO_DOWNSTREAM",
    "NO_CONSUMER_CUTOVER",
)
_SOURCE_BINDING_SCOPE = "NORMALIZED_FINAL_ROW_KEY_TO_IMMEDIATE_SOURCE_EVENT"
_RAW_RESPONSE_BINDING = "NOT_AVAILABLE"
_REMAINDER = "REMAINDER"
_EXPLICIT_KEYS = "EXPLICIT_KEYS"
_LIVE_PROVIDER = "LIVE_PROVIDER"
_CANONICAL_PREDECESSOR_REUSE = "CANONICAL_PREDECESSOR_REUSE"
_LEGACY_LOCAL_CACHE_IMPORT = "LEGACY_LOCAL_CACHE_IMPORT"
_SOURCE_KINDS = {
    _LIVE_PROVIDER,
    _CANONICAL_PREDECESSOR_REUSE,
    _LEGACY_LOCAL_CACHE_IMPORT,
}
_ROW_KEY_COLUMNS = {
    "prices": ("ticker", "date"),
    "rates": ("series", "date"),
    "secondary_prices": ("ticker", "date"),
}
_CURRENT_POINTER_FIELDS = {
    "pointer_id",
    "schema_version",
    "dataset_id",
    "snapshot_id",
    "manifest_id",
    "source_event_id",
    "run_id",
    "generation",
    "published_at",
    "manifest",
    "snapshot",
    "source_event",
    "data_quality_evidence_id",
    "previous_pointer_id",
    "previous_pointer_sha256",
    "production_effect",
}
_ARTIFACT_POINTER_FIELDS = {
    "path",
    "artifact_type",
    "sha256",
    "size_bytes",
    "schema_version",
}
_ATOMICITY_SCOPE = "IMMUTABLE_GENERATION_DISCOVERY_POINTER_ONLY"
_LEGACY_PROJECTION_ROLE = "COMPATIBILITY_ONLY"
_LEGACY_PROJECTION_ATOMICITY = "NOT_GUARANTEED"
_VALIDATION_SCOPE = "STRUCTURAL_PUBLICATION_ONLY"


class DownloadPublicationError(RuntimeError):
    """Typed fail-closed boundary for a download publication transaction."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: Path | None = None,
        commit_state: str = "NOT_COMMITTED",
    ) -> None:
        if commit_state not in {
            "NOT_COMMITTED",
            "POINTER_COMMITTED",
            "POINTER_COMMITTED_PROJECTION_FAILED",
            "INDETERMINATE",
        }:
            raise ValueError("invalid commit_state")
        self.code = code
        self.message = message
        self.path = path
        self.commit_state = commit_state
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location} ({commit_state}): {message}")


class DownloadPublicationIntegrityError(DownloadPublicationError):
    pass


class DownloadLegacyProjectionError(DownloadPublicationError):
    pass


@dataclass(frozen=True)
class DownloadArtifactCandidate:
    role: str
    filename: str
    content: bytes
    row_count: int
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class DownloadReplayInputCandidate:
    input_role: str
    filename: str
    content: bytes
    row_count: int


@dataclass(frozen=True)
class DownloadSourceBinding:
    source_event_id: str
    artifact_role: str
    source_kind: str
    source_id: str
    provider: str
    endpoint: str
    request_parameters: Mapping[str, object]
    winning_row_count: int
    allocation_mode: str
    winning_row_keys: tuple[tuple[str, str], ...]
    replay_inputs: tuple[DownloadReplayInputCandidate, ...] = ()


@dataclass(frozen=True)
class DownloadLegacyFilePrecondition:
    relative_path: str
    expected_exists: bool
    expected_content: bytes | None
    expected_sha256: str | None
    expected_size_bytes: int | None


@dataclass(frozen=True)
class DownloadLegacyBootstrapPrecondition:
    members: tuple[DownloadLegacyFilePrecondition, ...]


@dataclass(frozen=True)
class ValidatedDownloadPublication:
    transaction_id: str
    transaction_manifest_path: Path
    transaction_manifest_sha256: str
    discovery_pointer_path: Path
    discovery_pointer_sha256: str
    prices_path: Path
    rates_path: Path
    manifest_path: Path
    secondary_prices_path: Path | None
    legacy_prices_path: Path
    legacy_rates_path: Path
    legacy_manifest_path: Path
    legacy_secondary_prices_path: Path | None
    requested_start: date
    requested_end: date
    artifact_sha256: Mapping[str, str]
    artifact_row_count: Mapping[str, int]
    manifest_sha256: str
    manifest_row_count: int
    legacy_projection_verified: bool
    atomicity_scope: str = _ATOMICITY_SCOPE
    legacy_projection_role: str = _LEGACY_PROJECTION_ROLE
    legacy_projection_atomicity: str = _LEGACY_PROJECTION_ATOMICITY
    consumer_cutover_allowed: bool = False
    production_effect: str = "none"


def publish_download_transaction(
    *,
    output_dir: Path,
    requested_start: date,
    requested_end: date,
    artifacts: Sequence[DownloadArtifactCandidate],
    source_bindings: Sequence[DownloadSourceBinding],
    published_at: datetime | None = None,
    legacy_bootstrap_precondition: DownloadLegacyBootstrapPrecondition | None = None,
) -> ValidatedDownloadPublication:
    """Stage, validate and publish one immutable composite download transaction.

    Compatibility files remain available for legacy readers, but the atomic
    discovery pointer is the only authority that claims the entire set committed.
    No consumer cutover is authorized by this function.
    """

    if requested_start > requested_end:
        _fail("DOWNLOAD_WINDOW_INVALID", "requested_start must be <= requested_end")
    observed_at = published_at or datetime.now(tz=UTC)
    _utc_datetime(observed_at, "published_at")
    root = _prepare_root(output_dir, create=True)
    normalized_artifacts = _validated_artifacts(artifacts)
    normalized_sources = _validated_source_bindings(source_bindings, normalized_artifacts)
    normalized_legacy_precondition = _validated_legacy_bootstrap_precondition(
        legacy_bootstrap_precondition
    )

    try:
        return _publish_with_canonical_snapshot(
            root=root,
            requested_start=requested_start,
            requested_end=requested_end,
            artifacts=normalized_artifacts,
            source_bindings=normalized_sources,
            published_at=observed_at,
            legacy_bootstrap_precondition=normalized_legacy_precondition,
        )
    except DownloadPublicationError:
        raise
    except DataPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PUBLICATION_STORAGE_INVALID",
            str(exc),
            path=getattr(exc, "path", None),
        ) from exc
    except OSError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PUBLICATION_IO_FAILED",
            str(exc),
            path=root,
        ) from exc


def resolve_download_publication(*, output_dir: Path) -> ValidatedDownloadPublication:
    """Resolve the public D0A current pointer and revalidate every immutable member."""

    root = _prepare_root(output_dir, create=False)
    resolved, _, _ = _resolve_validated_generation(root)
    return resolved


def resolve_download_publication_if_present(
    *,
    output_dir: Path,
) -> ValidatedDownloadPublication | None:
    """Return canonical current, or None only when its pointer truly does not exist."""

    if not isinstance(output_dir, Path):
        raise TypeError("output_dir must be Path")
    if not os.path.lexists(output_dir):
        return None
    root = _prepare_root(output_dir, create=False)
    if not os.path.lexists(root / Path(_CURRENT_POINTER)):
        return None
    resolved, _, _ = _resolve_validated_generation(root)
    return resolved


def _resolve_validated_generation(
    root: Path,
) -> tuple[ValidatedDownloadPublication, str, bytes]:
    current = _validated_current_snapshot(root)
    outer_previous_pointer_sha256, outer_manifest_raw = _locked_outer_publication(
        root,
        current,
    )
    transaction_relative = current.payload_path.relative_to(root).as_posix()
    transaction_raw = _read_required(
        root,
        transaction_relative,
        "DOWNLOAD_TRANSACTION_MISSING",
    )
    transaction = _strict_canonical_json(
        transaction_raw,
        schema=DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
        code="DOWNLOAD_TRANSACTION_INVALID",
    )
    resolved, manifest_raw = _validate_transaction(
        root=root,
        transaction=transaction,
        transaction_raw=transaction_raw,
        pointer_path=current.pointer_path,
        transaction_path=current.payload_path,
        current=current,
        outer_previous_pointer_sha256=outer_previous_pointer_sha256,
        outer_manifest_raw=outer_manifest_raw,
    )
    return (
        _resolved_with_projection_state(
            resolved,
            verified=_legacy_projection_matches(
                root=root,
                resolved=resolved,
                validated_manifest_raw=manifest_raw,
            ),
        ),
        current.pointer_sha256,
        manifest_raw,
    )


def _validated_current_snapshot(root: Path) -> ValidatedCurrentSnapshot:
    try:
        return validate_current_snapshot(
            store_root=root / _PUBLICATION_ROOT,
            evidence_root=root,
            dataset_id=_DATASET_ID,
        )
    except DataPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_DISCOVERY_INVALID",
            str(exc),
            path=getattr(exc, "path", root / Path(_CURRENT_POINTER)),
        ) from exc


def _locked_outer_publication(
    root: Path,
    current: ValidatedCurrentSnapshot,
) -> tuple[str | None, bytes]:
    pointer_relative = current.pointer_path.relative_to(root).as_posix()
    pointer_raw = _read_required(
        root,
        pointer_relative,
        "DOWNLOAD_OUTER_POINTER_CHANGED",
    )
    if _sha256(pointer_raw) != current.pointer_sha256:
        _fail(
            "DOWNLOAD_OUTER_POINTER_CHANGED",
            "current pointer changed after public validation",
        )
    pointer = _strict_canonical_json(
        pointer_raw,
        schema=DOWNLOAD_DISCOVERY_SCHEMA_VERSION,
        code="DOWNLOAD_OUTER_POINTER_INVALID",
    )
    _exact_fields(
        pointer,
        _CURRENT_POINTER_FIELDS,
        "DOWNLOAD_OUTER_POINTER_INVALID",
    )
    expected_identity = {
        "pointer_id": current.pointer_id,
        "dataset_id": current.dataset_id,
        "snapshot_id": current.snapshot_id,
        "manifest_id": current.manifest_id,
        "source_event_id": current.source_event_id,
        "run_id": current.run_id,
        "generation": current.generation,
    }
    if any(pointer.get(field) != value for field, value in expected_identity.items()):
        _fail(
            "DOWNLOAD_OUTER_POINTER_INVALID",
            "current pointer identity differs from public validation",
        )

    manifest_pointer = _mapping(pointer.get("manifest"), "outer.manifest")
    _exact_fields(
        manifest_pointer,
        _ARTIFACT_POINTER_FIELDS,
        "DOWNLOAD_OUTER_POINTER_INVALID",
    )
    manifest_path = _portable_relative(
        manifest_pointer.get("path"),
        "outer.manifest.path",
    )
    manifest_relative = (PurePosixPath(_PUBLICATION_ROOT) / PurePosixPath(manifest_path)).as_posix()
    if (root / Path(manifest_relative)).resolve(strict=False) != current.manifest_path.resolve(
        strict=False
    ):
        _fail(
            "DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
            "outer manifest path differs from public validation",
        )
    if (
        manifest_pointer.get("artifact_type") != "json"
        or manifest_pointer.get("schema_version") != SNAPSHOT_MANIFEST_SCHEMA_VERSION
    ):
        _fail(
            "DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
            "outer manifest type/schema mismatch",
        )
    manifest_digest = _digest(
        manifest_pointer.get("sha256"),
        "outer.manifest.sha256",
    )
    manifest_size = _integer(
        manifest_pointer.get("size_bytes"),
        "outer.manifest.size_bytes",
    )
    outer_manifest_raw = _read_required(
        root,
        manifest_relative,
        "DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
    )
    _verify_bytes(
        outer_manifest_raw,
        digest=manifest_digest,
        size=manifest_size,
        code="DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
    )

    generation = _integer(pointer.get("generation"), "outer.generation")
    previous_pointer_sha256 = pointer.get("previous_pointer_sha256")
    previous_pointer_id = pointer.get("previous_pointer_id")
    if generation == 1:
        if previous_pointer_id is not None or previous_pointer_sha256 is not None:
            _fail(
                "DOWNLOAD_OUTER_POINTER_INVALID",
                "generation 1 cannot declare a predecessor",
            )
        return None, outer_manifest_raw
    _text(previous_pointer_id, "outer.previous_pointer_id")
    return (
        _digest(
            previous_pointer_sha256,
            "outer.previous_pointer_sha256",
        ),
        outer_manifest_raw,
    )


def _validated_legacy_bootstrap_precondition(
    value: DownloadLegacyBootstrapPrecondition | None,
) -> DownloadLegacyBootstrapPrecondition | None:
    if value is None:
        return None
    if not isinstance(value, DownloadLegacyBootstrapPrecondition):
        raise TypeError("legacy_bootstrap_precondition must be DownloadLegacyBootstrapPrecondition")
    if type(value.members) is not tuple:
        _fail(
            "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
            "members must be a tuple",
        )
    normalized: list[DownloadLegacyFilePrecondition] = []
    for member in value.members:
        if not isinstance(member, DownloadLegacyFilePrecondition):
            _fail(
                "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                "all members must be DownloadLegacyFilePrecondition",
            )
        if member.relative_path not in _LEGACY_BOOTSTRAP_PATHS:
            _fail(
                "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                member.relative_path,
            )
        if type(member.expected_exists) is not bool:
            _fail(
                "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                f"{member.relative_path}: expected_exists must be bool",
            )
        if member.expected_exists:
            if not isinstance(member.expected_content, bytes):
                _fail(
                    "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                    f"{member.relative_path}: expected content is missing",
                )
            expected_digest = _sha256(member.expected_content)
            if (
                not isinstance(member.expected_sha256, str)
                or _SHA256_RE.fullmatch(member.expected_sha256) is None
                or type(member.expected_size_bytes) is not int
                or member.expected_sha256 != expected_digest
                or member.expected_size_bytes != len(member.expected_content)
            ):
                _fail(
                    "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                    f"{member.relative_path}: digest or size does not bind exact content",
                )
        elif (
            member.expected_content is not None
            or member.expected_sha256 is not None
            or member.expected_size_bytes is not None
        ):
            _fail(
                "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
                f"{member.relative_path}: absent member cannot bind content",
            )
        normalized.append(
            DownloadLegacyFilePrecondition(
                relative_path=member.relative_path,
                expected_exists=member.expected_exists,
                expected_content=member.expected_content,
                expected_sha256=member.expected_sha256,
                expected_size_bytes=member.expected_size_bytes,
            )
        )
    if tuple(member.relative_path for member in normalized) != _LEGACY_BOOTSTRAP_PATHS:
        _fail(
            "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_INVALID",
            "members must contain each fixed legacy path exactly once in canonical order",
        )
    return DownloadLegacyBootstrapPrecondition(members=tuple(normalized))


def _capture_legacy_bootstrap_precondition(
    root: Path,
) -> DownloadLegacyBootstrapPrecondition:
    members: list[DownloadLegacyFilePrecondition] = []
    for relative_path in _LEGACY_BOOTSTRAP_PATHS:
        content = _read_legacy_bootstrap_member(root, relative_path)
        members.append(
            DownloadLegacyFilePrecondition(
                relative_path=relative_path,
                expected_exists=content is not None,
                expected_content=content,
                expected_sha256=None if content is None else _sha256(content),
                expected_size_bytes=None if content is None else len(content),
            )
        )
    return DownloadLegacyBootstrapPrecondition(members=tuple(members))


def _assert_legacy_bootstrap_precondition(
    *,
    root: Path,
    precondition: DownloadLegacyBootstrapPrecondition,
) -> None:
    for member in precondition.members:
        observed = _read_legacy_bootstrap_member(root, member.relative_path)
        if (
            (observed is not None) != member.expected_exists
            or observed != member.expected_content
            or (None if observed is None else _sha256(observed)) != member.expected_sha256
            or (None if observed is None else len(observed)) != member.expected_size_bytes
        ):
            _fail(
                "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH",
                f"{member.relative_path} changed after exact bootstrap capture",
            )


def _read_legacy_bootstrap_member(root: Path, relative_path: str) -> bytes | None:
    path = root / relative_path
    if not os.path.lexists(path):
        return None
    try:
        return read_contained_artifact_bytes(
            root=root,
            relative_path=relative_path,
        )
    except DataPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH",
            str(exc),
            path=getattr(exc, "path", path),
        ) from exc


def _legacy_bootstrap_member(
    precondition: DownloadLegacyBootstrapPrecondition,
    relative_path: str,
) -> DownloadLegacyFilePrecondition:
    for member in precondition.members:
        if member.relative_path == relative_path:
            return member
    raise AssertionError(f"missing normalized legacy bootstrap member: {relative_path}")


def _publication_base(
    root: Path,
    *,
    legacy_bootstrap_precondition: DownloadLegacyBootstrapPrecondition | None,
) -> tuple[
    str | None,
    bytes | None,
    DownloadLegacyBootstrapPrecondition | None,
]:
    pointer_path = root / Path(_CURRENT_POINTER)
    if not os.path.lexists(pointer_path):
        effective_precondition = (
            legacy_bootstrap_precondition or _capture_legacy_bootstrap_precondition(root)
        )
        _assert_legacy_bootstrap_precondition(
            root=root,
            precondition=effective_precondition,
        )
        manifest_member = _legacy_bootstrap_member(
            effective_precondition,
            "download_manifest.csv",
        )
        return None, manifest_member.expected_content, effective_precondition
    if legacy_bootstrap_precondition is not None:
        _fail(
            "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH",
            "canonical current appeared after legacy bootstrap capture",
        )
    _, pointer_sha256, manifest_raw = _resolve_validated_generation(root)
    return pointer_sha256, manifest_raw, None


def _publish_with_canonical_snapshot(
    *,
    root: Path,
    requested_start: date,
    requested_end: date,
    artifacts: tuple[DownloadArtifactCandidate, ...],
    source_bindings: tuple[DownloadSourceBinding, ...],
    published_at: datetime,
    legacy_bootstrap_precondition: DownloadLegacyBootstrapPrecondition | None,
) -> ValidatedDownloadPublication:
    (
        base_pointer_sha256,
        previous_manifest,
        effective_legacy_precondition,
    ) = _publication_base(
        root,
        legacy_bootstrap_precondition=legacy_bootstrap_precondition,
    )
    predecessor_pointer_sha256 = _validate_current_predecessor_bindings(
        root=root,
        source_bindings=source_bindings,
    )
    if predecessor_pointer_sha256 is not None and predecessor_pointer_sha256 != base_pointer_sha256:
        _fail(
            "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
            "current pointer changed while binding canonical predecessor",
        )
    artifact_identity = tuple(_artifact_identity(item) for item in artifacts)
    source_payloads = tuple(_source_payload(item) for item in source_bindings)
    identity = {
        "base_pointer_sha256": base_pointer_sha256,
        "published_at": published_at.isoformat(),
        "requested_window": {
            "start": requested_start.isoformat(),
            "end": requested_end.isoformat(),
        },
        "artifacts": list(artifact_identity),
        "source_event_records": list(source_payloads),
        "semantic_claims": list(_SEMANTIC_CLAIMS),
        "source_binding_scope": _SOURCE_BINDING_SCOPE,
        "raw_response_binding": _RAW_RESPONSE_BINDING,
        "validation_scope": _VALIDATION_SCOPE,
        "dq_execution_provenance_verified": False,
        "atomicity_scope": _ATOMICITY_SCOPE,
        "legacy_projection_role": _LEGACY_PROJECTION_ROLE,
        "legacy_projection_atomicity": _LEGACY_PROJECTION_ATOMICITY,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    identity_sha256 = _canonical_sha256(identity)
    transaction_id = f"download_txn_{identity_sha256[:32]}"

    legacy_records = _legacy_manifest_records(
        root=root,
        artifacts=artifacts,
        source_bindings=source_bindings,
        transaction_id=transaction_id,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
    )
    legacy_manifest_raw = _append_manifest(previous_manifest, legacy_records)
    legacy_manifest_rows = _csv_shape(
        legacy_manifest_raw,
        required_columns=_MANIFEST_COLUMNS,
        code="DOWNLOAD_MANIFEST_INVALID",
    )[1]
    _validate_current_legacy_manifest_semantics(
        root=root,
        manifest_raw=legacy_manifest_raw,
        transaction_id=transaction_id,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
        artifacts=artifact_identity,
        source_payloads=source_payloads,
    )

    artifact_payloads = [
        {
            **record,
            "immutable_path": (
                PurePosixPath(_PUBLICATION_ROOT)
                / "members"
                / str(record["sha256"])
                / str(record["filename"])
            ).as_posix(),
            "legacy_path": str(record["filename"]),
        }
        for record in artifact_identity
    ]
    legacy_manifest_ref = {
        "path": (
            PurePosixPath(_PUBLICATION_ROOT)
            / "members"
            / _sha256(legacy_manifest_raw)
            / "download_manifest.csv"
        ).as_posix(),
        "legacy_path": "download_manifest.csv",
        "sha256": _sha256(legacy_manifest_raw),
        "size_bytes": len(legacy_manifest_raw),
        "row_count": legacy_manifest_rows,
    }
    transaction = {
        "schema_version": DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
        "contract_version": DOWNLOAD_PUBLICATION_CONTRACT_VERSION,
        "transaction_id": transaction_id,
        "identity_sha256": identity_sha256,
        "base_pointer_sha256": base_pointer_sha256,
        "published_at": published_at.isoformat(),
        "discovery_schema_version": DOWNLOAD_DISCOVERY_SCHEMA_VERSION,
        "requested_window": identity["requested_window"],
        "artifacts": artifact_payloads,
        "source_event_records": list(source_payloads),
        "download_manifest": legacy_manifest_ref,
        "semantic_claims": list(_SEMANTIC_CLAIMS),
        "source_binding_scope": _SOURCE_BINDING_SCOPE,
        "raw_response_binding": _RAW_RESPONSE_BINDING,
        "validation_scope": _VALIDATION_SCOPE,
        "dq_execution_provenance_verified": False,
        "atomicity_scope": _ATOMICITY_SCOPE,
        "legacy_projection_role": _LEGACY_PROJECTION_ROLE,
        "legacy_projection_atomicity": _LEGACY_PROJECTION_ATOMICITY,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    transaction_raw = _canonical_bytes(transaction)
    for item in artifacts:
        member_path = (
            PurePosixPath(_PUBLICATION_ROOT) / "members" / _sha256(item.content) / item.filename
        ).as_posix()
        _write(root, member_path, item.content, immutable=True)
        if _read_required(root, member_path, "DOWNLOAD_ARTIFACT_MISSING") != item.content:
            _fail("DOWNLOAD_ARTIFACT_BINDING_MISMATCH", member_path)
    for source in source_bindings:
        for replay_input in source.replay_inputs:
            replay_path = _replay_input_path(replay_input)
            _write(root, replay_path, replay_input.content, immutable=True)
            if (
                _read_required(root, replay_path, "DOWNLOAD_REPLAY_INPUT_MISSING")
                != replay_input.content
            ):
                _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", replay_path)
    manifest_member_path = str(legacy_manifest_ref["path"])
    _write(root, manifest_member_path, legacy_manifest_raw, immutable=True)
    if (
        _read_required(root, manifest_member_path, "DOWNLOAD_MANIFEST_MISSING")
        != legacy_manifest_raw
    ):
        _fail("DOWNLOAD_MANIFEST_BINDING_MISMATCH", manifest_member_path)

    evidence = _write_structural_quality_evidence(
        root=root,
        transaction_id=transaction_id,
        payload=transaction_raw,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
    )
    request = SnapshotPublishRequest(
        dataset_id=_DATASET_ID,
        run_id=transaction_id,
        producer="ai_trading_system.data.download_publication",
        owner="data_platform",
        as_of=requested_end,
        generated_at=published_at,
        coverage_start=requested_start,
        coverage_end=requested_end,
        payload_artifact_type=_PAYLOAD_ARTIFACT_TYPE,
        payload_schema_version=DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
        data_quality_report_schema_version=DATA_QUALITY_REPORT_SCHEMA_VERSION,
        source_event=SourceEventProvenance(
            source_id="download_composite_publication",
            provider_name="AITradingSystem download publication",
            endpoint=DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
            request_parameters={
                "transaction_id": transaction_id,
                "requested_start": requested_start.isoformat(),
                "requested_end": requested_end.isoformat(),
                "member_count": (
                    len(artifacts)
                    + 1
                    + sum(len(source.replay_inputs) for source in source_bindings)
                ),
            },
            downloaded_at=published_at,
            row_count=1,
            source_role="composite_publication_manifest",
            response_headers_sanitized=True,
        ),
        data_quality=evidence,
    )
    pre_commit_validator: Callable[[], None] | None = None
    if effective_legacy_precondition is not None:
        bound_precondition = effective_legacy_precondition

        def validate_legacy_bootstrap_precondition() -> None:
            _assert_legacy_bootstrap_precondition(
                root=root,
                precondition=bound_precondition,
            )

        pre_commit_validator = validate_legacy_bootstrap_precondition
    try:
        publish_result = publish_immutable_snapshot(
            store_root=root / _PUBLICATION_ROOT,
            evidence_root=root,
            request=request,
            payload=transaction_raw,
            current_precondition=CurrentPointerPrecondition(expected_sha256=base_pointer_sha256),
            pre_commit_validator=pre_commit_validator,
        )
    except DataPublicationError as exc:
        commit_state = (
            "INDETERMINATE"
            if getattr(exc, "commit_state", "NOT_REPLACED") == "INDETERMINATE"
            else "NOT_COMMITTED"
        )
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_DISCOVERY_COMMIT_FAILED",
            str(exc),
            path=getattr(exc, "path", root / Path(_CURRENT_POINTER)),
            commit_state=commit_state,
        ) from exc

    try:
        canonical, observed_pointer_sha256, _ = _resolve_validated_generation(root)
    except Exception as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_POST_COMMIT_ATTESTATION_FAILED",
            str(exc),
            path=root / Path(_CURRENT_POINTER),
            commit_state="POINTER_COMMITTED",
        ) from exc
    if (
        observed_pointer_sha256 != publish_result.snapshot.pointer_sha256
        or canonical.transaction_id != transaction_id
    ):
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_DISCOVERY_SUPERSEDED_BEFORE_PROJECTION",
            (
                f"published_pointer={publish_result.snapshot.pointer_sha256}; "
                f"observed_pointer={observed_pointer_sha256}; "
                f"published_transaction={transaction_id}; "
                f"observed_transaction={canonical.transaction_id}"
            ),
            path=root / Path(_CURRENT_POINTER),
            commit_state="POINTER_COMMITTED",
        )
    compatibility_candidates = {item.filename: item.content for item in artifacts} | {
        "download_manifest.csv": legacy_manifest_raw
    }
    written: list[str] = []
    retired: list[str] = []
    try:
        if "prices_marketstack_daily.csv" not in compatibility_candidates:
            _retire_compatibility_artifact(root, "prices_marketstack_daily.csv")
            retired.append("prices_marketstack_daily.csv")
        for relative, content in compatibility_candidates.items():
            _write_compatibility_artifact(root, relative, content)
            written.append(relative)
        projected = resolve_download_publication(output_dir=root)
        if not projected.legacy_projection_verified:
            _fail("DOWNLOAD_LEGACY_PROJECTION_BINDING_MISMATCH", transaction_id)
        return projected
    except Exception as exc:
        raise DownloadLegacyProjectionError(
            "DOWNLOAD_LEGACY_PROJECTION_FAILED",
            (
                f"canonical_transaction={canonical.transaction_id}; "
                f"retired={retired}; projected={written}; cause={exc}"
            ),
            path=root,
            commit_state="POINTER_COMMITTED_PROJECTION_FAILED",
        ) from exc


def _validated_artifacts(
    artifacts: Sequence[DownloadArtifactCandidate],
) -> tuple[DownloadArtifactCandidate, ...]:
    if not isinstance(artifacts, Sequence):
        raise TypeError("artifacts must be a sequence")
    items = tuple(artifacts)
    roles = [item.role for item in items if isinstance(item, DownloadArtifactCandidate)]
    if len(roles) != len(items):
        raise TypeError("all artifacts must be DownloadArtifactCandidate")
    if set(roles) not in ({"prices", "rates"}, {"prices", "rates", "secondary_prices"}):
        _fail(
            "DOWNLOAD_ARTIFACT_SET_MISMATCH",
            f"unexpected artifact roles: {sorted(roles)}",
        )
    if len(roles) != len(set(roles)):
        _fail("DOWNLOAD_ARTIFACT_SET_MISMATCH", "artifact roles must be unique")
    validated: list[DownloadArtifactCandidate] = []
    for item in items:
        expected_filename = _ARTIFACT_FILENAMES.get(item.role)
        if expected_filename != item.filename:
            _fail(
                "DOWNLOAD_ARTIFACT_PATH_MISMATCH",
                f"role={item.role} filename={item.filename}",
            )
        if not isinstance(item.content, bytes):
            raise TypeError("artifact content must be bytes")
        if type(item.row_count) is not int or item.row_count < 0:
            _fail("DOWNLOAD_ARTIFACT_ROW_COUNT_MISMATCH", item.role)
        if (
            type(item.source_event_ids) is not tuple
            or not item.source_event_ids
            or any(
                not isinstance(value, str) or not _valid_text(value)
                for value in item.source_event_ids
            )
        ):
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.role)
        if len(item.source_event_ids) != len(set(item.source_event_ids)):
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.role)
        row_keys = _artifact_row_keys(
            item.content,
            role=item.role,
            code="DOWNLOAD_ARTIFACT_SCHEMA_MISMATCH",
        )
        if len(row_keys) != item.row_count:
            _fail(
                "DOWNLOAD_ARTIFACT_ROW_COUNT_MISMATCH",
                f"role={item.role} expected={item.row_count} observed={len(row_keys)}",
            )
        validated.append(item)
    return tuple(sorted(validated, key=lambda item: item.role))


def _write_structural_quality_evidence(
    *,
    root: Path,
    transaction_id: str,
    payload: bytes,
    requested_start: date,
    requested_end: date,
    published_at: datetime,
) -> DataQualityEvidence:
    snapshot = ArtifactPointer(
        path=(
            PurePosixPath("snapshots")
            / _DATASET_ID
            / sha256_bytes(payload)
            / f"payload.{_PAYLOAD_ARTIFACT_TYPE}"
        ).as_posix(),
        artifact_type=_PAYLOAD_ARTIFACT_TYPE,
        sha256=sha256_bytes(payload),
        size_bytes=len(payload),
        schema_version=DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
    )
    report = {
        "schema_version": DATA_QUALITY_REPORT_SCHEMA_VERSION,
        "contract_id": _STRUCTURAL_DQ_CONTRACT_ID,
        "policy_id": _STRUCTURAL_DQ_POLICY_ID,
        "policy_version": _STRUCTURAL_DQ_POLICY_VERSION,
        "status": "PASS",
        "passed": True,
        "checked_at": published_at.isoformat(),
        "as_of": requested_end.isoformat(),
        "coverage_start": requested_start.isoformat(),
        "coverage_end": requested_end.isoformat(),
        "checked_input_count": 1,
        "error_count": 0,
        "warning_count": 0,
        "blocking_issues": [],
        "evaluated_snapshot": snapshot.to_dict(),
        "production_effect": "none",
    }
    report_raw = _canonical_bytes(report)
    report_relative = (
        PurePosixPath(_PUBLICATION_ROOT)
        / "evidence"
        / transaction_id
        / "structural_quality_report.json"
    ).as_posix()
    _write(root, report_relative, report_raw, immutable=True)
    if _read_required(root, report_relative, "DOWNLOAD_STRUCTURAL_EVIDENCE_MISSING") != report_raw:
        _fail("DOWNLOAD_STRUCTURAL_EVIDENCE_BINDING_MISMATCH", transaction_id)
    return DataQualityEvidence(
        contract_id=_STRUCTURAL_DQ_CONTRACT_ID,
        policy_id=_STRUCTURAL_DQ_POLICY_ID,
        policy_version=_STRUCTURAL_DQ_POLICY_VERSION,
        status="PASS",
        passed=True,
        checked_at=published_at,
        as_of=requested_end,
        report_path=report_relative,
        report_sha256=_sha256(report_raw),
        checked_input_count=1,
    )


def _validated_source_bindings(
    source_bindings: Sequence[DownloadSourceBinding],
    artifacts: tuple[DownloadArtifactCandidate, ...],
) -> tuple[DownloadSourceBinding, ...]:
    if not isinstance(source_bindings, Sequence):
        raise TypeError("source_bindings must be a sequence")
    items = tuple(source_bindings)
    if any(not isinstance(item, DownloadSourceBinding) for item in items):
        raise TypeError("all source bindings must be DownloadSourceBinding")
    artifact_by_role = {item.role: item for item in artifacts}
    identities: set[str] = set()
    sources_by_role: dict[str, list[DownloadSourceBinding]] = {}
    normalized_items: list[DownloadSourceBinding] = []
    for item in items:
        if item.artifact_role not in artifact_by_role:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.artifact_role)
        for value, field in (
            (item.source_event_id, "source_event_id"),
            (item.source_id, "source_id"),
            (item.provider, "provider"),
            (item.endpoint, "endpoint"),
        ):
            if not _valid_text(value):
                _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", field)
        if item.source_event_id in identities:
            _fail(
                "DOWNLOAD_SOURCE_BINDING_MISMATCH",
                f"duplicate source event {item.source_event_id}",
            )
        identities.add(item.source_event_id)
        if item.source_kind not in _SOURCE_KINDS:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.source_event_id)
        if type(item.winning_row_count) is not int or item.winning_row_count < 0:
            _fail("DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH", item.source_event_id)
        if item.allocation_mode not in {_REMAINDER, _EXPLICIT_KEYS}:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.source_event_id)
        if type(item.winning_row_keys) is not tuple:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.source_event_id)
        normalized_keys = tuple(
            _validated_row_key(key, role=item.artifact_role) for key in item.winning_row_keys
        )
        if (
            normalized_keys != item.winning_row_keys
            or normalized_keys != tuple(sorted(set(normalized_keys)))
            or item.winning_row_count != len(normalized_keys)
        ):
            _fail("DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH", item.source_event_id)
        request_parameters = _json_mapping(item.request_parameters)
        replay_inputs = _validated_replay_inputs(
            item.replay_inputs,
            source_event_id=item.source_event_id,
        )
        _validated_source_kind_parameters(
            source_kind=item.source_kind,
            artifact_role=item.artifact_role,
            request_parameters=request_parameters,
            replay_inputs=replay_inputs,
            source_event_id=item.source_event_id,
        )
        normalized_item = DownloadSourceBinding(
            source_event_id=item.source_event_id,
            artifact_role=item.artifact_role,
            source_kind=item.source_kind,
            source_id=item.source_id,
            provider=item.provider,
            endpoint=item.endpoint,
            request_parameters=request_parameters,
            winning_row_count=item.winning_row_count,
            allocation_mode=item.allocation_mode,
            winning_row_keys=normalized_keys,
            replay_inputs=replay_inputs,
        )
        normalized_items.append(normalized_item)
        sources_by_role.setdefault(item.artifact_role, []).append(normalized_item)
    for role, artifact in artifact_by_role.items():
        role_sources = sources_by_role.get(role, [])
        if {item.source_event_id for item in role_sources} != set(artifact.source_event_ids):
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", role)
        remainder_sources = [item for item in role_sources if item.allocation_mode == _REMAINDER]
        if len(remainder_sources) != 1:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: remainder count")
        final_keys = set(
            _artifact_row_keys(
                artifact.content,
                role=role,
                code="DOWNLOAD_ARTIFACT_SCHEMA_MISMATCH",
            )
        )
        allocated: set[tuple[str, str]] = set()
        for item in role_sources:
            winning = set(item.winning_row_keys)
            if not winning.issubset(final_keys) or allocated.intersection(winning):
                _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", item.source_event_id)
            allocated.update(winning)
        if allocated != final_keys:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: incomplete partition")
        expected_remainder = min(
            role_sources,
            key=lambda item: (-item.winning_row_count, item.source_event_id),
        )
        if remainder_sources[0].source_event_id != expected_remainder.source_event_id:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: unstable remainder")
        if artifact.row_count > 0 and remainder_sources[0].winning_row_count == 0:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: empty remainder")
    return tuple(sorted(normalized_items, key=lambda item: item.source_event_id))


def _validate_current_predecessor_bindings(
    *,
    root: Path,
    source_bindings: tuple[DownloadSourceBinding, ...],
) -> str | None:
    canonical_sources = tuple(
        source for source in source_bindings if source.source_kind == _CANONICAL_PREDECESSOR_REUSE
    )
    if not canonical_sources:
        return None
    if not os.path.lexists(root / Path(_CURRENT_POINTER)):
        _fail(
            "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
            "canonical predecessor source requires a current publication",
        )
    try:
        predecessor = resolve_download_publication(output_dir=root)
    except DownloadPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
            str(exc),
            path=exc.path,
        ) from exc
    transaction_path = predecessor.transaction_manifest_path.relative_to(root).as_posix()
    artifact_paths = {
        "prices": predecessor.prices_path.relative_to(root).as_posix(),
        "rates": predecessor.rates_path.relative_to(root).as_posix(),
    }
    if predecessor.secondary_prices_path is not None:
        artifact_paths["secondary_prices"] = predecessor.secondary_prices_path.relative_to(
            root
        ).as_posix()
    for source in canonical_sources:
        parameters = source.request_parameters
        role = source.artifact_role
        if (
            parameters.get("predecessor_transaction_id") != predecessor.transaction_id
            or parameters.get("predecessor_transaction_path") != transaction_path
            or parameters.get("predecessor_transaction_sha256")
            != predecessor.transaction_manifest_sha256
            or parameters.get("predecessor_discovery_pointer_sha256")
            != predecessor.discovery_pointer_sha256
            or parameters.get("predecessor_artifact_role") != role
            or parameters.get("predecessor_artifact_path") != artifact_paths.get(role)
            or parameters.get("predecessor_artifact_sha256")
            != predecessor.artifact_sha256.get(role)
            or parameters.get("predecessor_artifact_row_count")
            != predecessor.artifact_row_count.get(role)
        ):
            _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source.source_event_id)
    return predecessor.discovery_pointer_sha256


def _artifact_identity(item: DownloadArtifactCandidate) -> dict[str, object]:
    return {
        "role": item.role,
        "filename": item.filename,
        "sha256": _sha256(item.content),
        "size_bytes": len(item.content),
        "row_count": item.row_count,
        "row_key_columns": list(_ROW_KEY_COLUMNS[item.role]),
        "source_event_ids": list(sorted(item.source_event_ids)),
    }


def _source_payload(item: DownloadSourceBinding) -> dict[str, object]:
    request_parameters = _json_mapping(item.request_parameters)
    explicit_row_keys = (
        [list(key) for key in item.winning_row_keys]
        if item.allocation_mode == _EXPLICIT_KEYS
        else []
    )
    return {
        "source_event_id": item.source_event_id,
        "artifact_role": item.artifact_role,
        "source_kind": item.source_kind,
        "source_id": item.source_id,
        "provider": item.provider,
        "endpoint": item.endpoint,
        "request_parameters": request_parameters,
        "request_parameters_sha256": _canonical_sha256(request_parameters),
        "allocation_mode": item.allocation_mode,
        "explicit_row_keys": explicit_row_keys,
        "winning_row_count": item.winning_row_count,
        "winning_row_keys_sha256": _row_keys_sha256(item.winning_row_keys),
        "replay_inputs": [
            _replay_input_payload(replay_input)
            for replay_input in sorted(item.replay_inputs, key=lambda value: value.input_role)
        ],
    }


def _validated_replay_inputs(
    value: object,
    *,
    source_event_id: str,
) -> tuple[DownloadReplayInputCandidate, ...]:
    if type(value) is not tuple:
        _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
    inputs = tuple(value)
    if any(not isinstance(item, DownloadReplayInputCandidate) for item in inputs):
        _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
    roles: set[str] = set()
    for item in inputs:
        if not _valid_text(item.input_role) or item.input_role in roles:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
        roles.add(item.input_role)
        _replay_input_filename(item.filename)
        if not isinstance(item.content, bytes):
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", item.input_role)
        if type(item.row_count) is not int or item.row_count < 0:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", item.input_role)
    return tuple(sorted(inputs, key=lambda item: item.input_role))


def _validated_source_kind_parameters(
    *,
    source_kind: str,
    artifact_role: str,
    request_parameters: Mapping[str, object],
    replay_inputs: tuple[DownloadReplayInputCandidate, ...],
    source_event_id: str,
) -> None:
    _validated_source_kind_payload(
        source_kind=source_kind,
        artifact_role=artifact_role,
        request_parameters=request_parameters,
        replay_inputs=[_replay_input_payload(item) for item in replay_inputs],
        source_event_id=source_event_id,
    )


def _validated_source_kind_payload(
    *,
    source_kind: str,
    artifact_role: str,
    request_parameters: Mapping[str, object],
    replay_inputs: Sequence[Mapping[str, object]],
    source_event_id: str,
) -> None:
    if source_kind == _LIVE_PROVIDER:
        if replay_inputs:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
        return
    if source_kind == _CANONICAL_PREDECESSOR_REUSE:
        if replay_inputs:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
        required = {
            "predecessor_transaction_id",
            "predecessor_transaction_path",
            "predecessor_transaction_sha256",
            "predecessor_discovery_pointer_sha256",
            "predecessor_artifact_role",
            "predecessor_artifact_path",
            "predecessor_artifact_sha256",
            "predecessor_artifact_row_count",
            "lineage_scope",
            "raw_provider_provenance",
            "origin_lineage_complete",
            "origin_status",
            "data_quality_provenance",
        }
        if not required.issubset(request_parameters):
            _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
        _transaction_id(request_parameters.get("predecessor_transaction_id"))
        _portable_relative(
            request_parameters.get("predecessor_transaction_path"),
            "predecessor_transaction_path",
        )
        _digest(
            request_parameters.get("predecessor_transaction_sha256"),
            "predecessor_transaction_sha256",
        )
        _digest(
            request_parameters.get("predecessor_discovery_pointer_sha256"),
            "predecessor_discovery_pointer_sha256",
        )
        if request_parameters.get("predecessor_artifact_role") != artifact_role:
            _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
        _portable_relative(
            request_parameters.get("predecessor_artifact_path"),
            "predecessor_artifact_path",
        )
        _digest(
            request_parameters.get("predecessor_artifact_sha256"),
            "predecessor_artifact_sha256",
        )
        _integer(
            request_parameters.get("predecessor_artifact_row_count"),
            "predecessor_artifact_row_count",
        )
        if (
            request_parameters.get("lineage_scope") != "IMMEDIATE_PREDECESSOR_ONLY"
            or request_parameters.get("raw_provider_provenance") is not False
            or request_parameters.get("origin_lineage_complete") is not False
            or request_parameters.get("origin_status") != "CANONICAL_IMMEDIATE_PREDECESSOR"
            or request_parameters.get("data_quality_provenance") is not False
        ):
            _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
        return
    if source_kind != _LEGACY_LOCAL_CACHE_IMPORT:
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_event_id)
    required = {
        "cache_relative_path",
        "cache_sha256",
        "cache_size_bytes",
        "cache_row_count",
        "manifest_relative_path",
        "manifest_sha256",
        "manifest_size_bytes",
        "manifest_row_count",
        "manifest_binding_status",
        "raw_provider_provenance",
        "origin_lineage_complete",
        "origin_status",
        "data_quality_provenance",
    }
    if not required.issubset(request_parameters) or len(replay_inputs) != 1:
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    cache_relative_path = _portable_relative(
        request_parameters.get("cache_relative_path"),
        "cache_relative_path",
    )
    if PurePosixPath(cache_relative_path).name != cache_relative_path:
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    cache_digest = _digest(request_parameters.get("cache_sha256"), "cache_sha256")
    cache_size = _integer(request_parameters.get("cache_size_bytes"), "cache_size_bytes")
    cache_rows = _integer(request_parameters.get("cache_row_count"), "cache_row_count")
    replay_input = replay_inputs[0]
    if (
        replay_input.get("input_role") != "legacy_local_cache_bytes"
        or replay_input.get("sha256") != cache_digest
        or replay_input.get("size_bytes") != cache_size
        or replay_input.get("row_count") != cache_rows
    ):
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    if request_parameters.get("manifest_relative_path") != "download_manifest.csv":
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    manifest_status = request_parameters.get("manifest_binding_status")
    if manifest_status not in {
        "MISSING",
        "UNREADABLE",
        "NO_PATH_MATCH",
        "CHECKSUM_MISMATCH",
        "ROW_COUNT_MISMATCH",
        "MATCHED",
    }:
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    manifest_digest = request_parameters.get("manifest_sha256")
    manifest_size = request_parameters.get("manifest_size_bytes")
    manifest_rows = request_parameters.get("manifest_row_count")
    if manifest_status == "MISSING":
        if manifest_digest is not None or manifest_size is not None or manifest_rows is not None:
            _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)
    else:
        _digest(manifest_digest, "manifest_sha256")
        _integer(manifest_size, "manifest_size_bytes")
        if manifest_rows is not None:
            _integer(manifest_rows, "manifest_row_count")
    if (
        request_parameters.get("raw_provider_provenance") is not False
        or request_parameters.get("origin_lineage_complete") is not False
        or request_parameters.get("origin_status") != "OPAQUE_LEGACY"
        or request_parameters.get("data_quality_provenance") is not False
    ):
        _fail("DOWNLOAD_LEGACY_IMPORT_BINDING_MISMATCH", source_event_id)


def _replay_input_payload(item: DownloadReplayInputCandidate) -> dict[str, object]:
    return {
        "input_role": item.input_role,
        "filename": item.filename,
        "path": _replay_input_path(item),
        "sha256": _sha256(item.content),
        "size_bytes": len(item.content),
        "row_count": item.row_count,
    }


def _replay_input_path(item: DownloadReplayInputCandidate) -> str:
    return (
        PurePosixPath(_PUBLICATION_ROOT) / "source_inputs" / _sha256(item.content) / item.filename
    ).as_posix()


def _replay_input_filename(value: object) -> str:
    filename = _text(value, "replay_input.filename")
    path = PurePosixPath(filename)
    if path.name != filename or filename in {".", ".."} or "\\" in filename:
        _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", filename)
    return filename


def _legacy_manifest_records(
    *,
    root: Path,
    artifacts: tuple[DownloadArtifactCandidate, ...],
    source_bindings: tuple[DownloadSourceBinding, ...],
    transaction_id: str,
    requested_start: date,
    requested_end: date,
    published_at: datetime,
) -> tuple[dict[str, object], ...]:
    return _legacy_manifest_records_from_payloads(
        root=root,
        artifacts=tuple(_artifact_identity(item) for item in artifacts),
        source_payloads=tuple(_source_payload(item) for item in source_bindings),
        transaction_id=transaction_id,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
    )


def _legacy_manifest_records_from_payloads(
    *,
    root: Path,
    artifacts: Sequence[Mapping[str, object]],
    source_payloads: Sequence[Mapping[str, object]],
    transaction_id: str,
    requested_start: date,
    requested_end: date,
    published_at: datetime,
) -> tuple[dict[str, object], ...]:
    sources_by_role: dict[str, list[Mapping[str, object]]] = {}
    for item in source_payloads:
        role = _text(item.get("artifact_role"), "source.artifact_role")
        sources_by_role.setdefault(role, []).append(item)
    records: list[dict[str, object]] = []
    for artifact in artifacts:
        role = _text(artifact.get("role"), "artifact.role")
        filename = _text(artifact.get("filename"), "artifact.filename")
        artifact_digest = _digest(artifact.get("sha256"), "artifact.sha256")
        artifact_rows = _integer(artifact.get("row_count"), "artifact.row_count")
        sources = sorted(
            sources_by_role.get(role, ()),
            key=lambda item: _text(item.get("source_id"), "source.source_id"),
        )
        if not sources:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", role)
        if len(sources) == 1:
            source_id = _text(sources[0].get("source_id"), "source.source_id")
            provider = _text(sources[0].get("provider"), "source.provider")
            endpoint = _text(sources[0].get("endpoint"), "source.endpoint")
            request_parameters = _json_mapping(
                _mapping(sources[0].get("request_parameters"), "source.request_parameters")
            )
        else:
            source_id = f"composite_{role}_publication"
            provider = "Composite download publication"
            endpoint = DOWNLOAD_PUBLICATION_SCHEMA_VERSION
            request_parameters = {}
        request_parameters.update(
            {
                "publication_transaction_id": transaction_id,
                "publication_schema_version": DOWNLOAD_PUBLICATION_SCHEMA_VERSION,
                "artifact_role": role,
                "composite_source_binding": True,
                "source_binding_scope": _SOURCE_BINDING_SCOPE,
                "raw_response_binding": _RAW_RESPONSE_BINDING,
                "row_key_columns": list(_ROW_KEY_COLUMNS[role]),
                "requested_window": {
                    "start": requested_start.isoformat(),
                    "end": requested_end.isoformat(),
                },
                "source_events": [
                    {
                        "source_event_id": _text(
                            item.get("source_event_id"),
                            "source.source_event_id",
                        ),
                        "source_kind": _text(item.get("source_kind"), "source.source_kind"),
                        "source_id": _text(item.get("source_id"), "source.source_id"),
                        "provider": _text(item.get("provider"), "source.provider"),
                        "endpoint": _text(item.get("endpoint"), "source.endpoint"),
                        "allocation_mode": _text(
                            item.get("allocation_mode"),
                            "source.allocation_mode",
                        ),
                        "winning_row_count": _integer(
                            item.get("winning_row_count"),
                            "source.winning_row_count",
                        ),
                        "winning_row_keys_sha256": _digest(
                            item.get("winning_row_keys_sha256"),
                            "source.winning_row_keys_sha256",
                        ),
                        "request_parameters_sha256": _digest(
                            item.get("request_parameters_sha256"),
                            "source.request_parameters_sha256",
                        ),
                        "replay_inputs": [
                            {
                                "input_role": _text(
                                    replay_input.get("input_role"),
                                    "replay_input.input_role",
                                ),
                                "sha256": _digest(
                                    replay_input.get("sha256"),
                                    "replay_input.sha256",
                                ),
                                "size_bytes": _integer(
                                    replay_input.get("size_bytes"),
                                    "replay_input.size_bytes",
                                ),
                                "row_count": _integer(
                                    replay_input.get("row_count"),
                                    "replay_input.row_count",
                                ),
                            }
                            for replay_input in sorted(
                                (
                                    _mapping(raw, "source.replay_input")
                                    for raw in _mapping_list(
                                        item.get("replay_inputs"),
                                        "source.replay_inputs",
                                    )
                                ),
                                key=lambda value: _text(
                                    value.get("input_role"),
                                    "replay_input.input_role",
                                ),
                            )
                        ],
                    }
                    for item in sources
                ],
            }
        )
        records.append(
            {
                "downloaded_at": published_at.isoformat(),
                "source_id": source_id,
                "provider": provider,
                "endpoint": endpoint,
                "request_parameters": json.dumps(
                    request_parameters,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
                "output_path": str(root / filename),
                "row_count": artifact_rows,
                "checksum_sha256": artifact_digest,
            }
        )
    return tuple(records)


def _append_manifest(
    previous: bytes | None,
    records: tuple[dict[str, object], ...],
) -> bytes:
    fieldnames = list(_MANIFEST_COLUMNS)
    existing: list[dict[str, str]] = []
    if previous is not None:
        try:
            handle = io.StringIO(previous.decode("utf-8-sig"), newline="")
            reader = csv.DictReader(handle)
            observed_fields = list(reader.fieldnames or ())
            if (
                not observed_fields
                or len(observed_fields) != len(set(observed_fields))
                or not set(_MANIFEST_COLUMNS).issubset(observed_fields)
            ):
                _fail("DOWNLOAD_MANIFEST_INVALID", "invalid or duplicate manifest columns")
            fieldnames = observed_fields
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    _fail("DOWNLOAD_MANIFEST_INVALID", "malformed manifest row")
                existing.append({field: str(row[field]) for field in fieldnames})
        except (UnicodeError, csv.Error) as exc:
            raise DownloadPublicationIntegrityError(
                "DOWNLOAD_MANIFEST_INVALID",
                str(exc),
            ) from exc
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(existing)
    for record in records:
        writer.writerow({field: record.get(field, "") for field in fieldnames})
    return output.getvalue().encode("utf-8")


def _validate_current_legacy_manifest_semantics(
    *,
    root: Path,
    manifest_raw: bytes,
    transaction_id: str,
    requested_start: date,
    requested_end: date,
    published_at: datetime,
    artifacts: Sequence[Mapping[str, object]],
    source_payloads: Sequence[Mapping[str, object]],
) -> None:
    expected = _legacy_manifest_records_from_payloads(
        root=root,
        artifacts=artifacts,
        source_payloads=source_payloads,
        transaction_id=transaction_id,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
    )
    header, rows = _legacy_manifest_rows(manifest_raw)
    if len(rows) < len(expected):
        _fail(
            "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
            "manifest does not contain a complete current-generation suffix",
        )
    prefix = rows[: len(rows) - len(expected)]
    suffix = rows[len(rows) - len(expected) :]
    extra_columns = tuple(field for field in header if field not in _MANIFEST_COLUMNS)
    observed_roles: list[str] = []
    for index, (observed, expected_record) in enumerate(zip(suffix, expected, strict=True)):
        expected_row = {
            field: "" if expected_record.get(field) is None else str(expected_record.get(field, ""))
            for field in _MANIFEST_COLUMNS
        }
        if any(observed.get(field) != value for field, value in expected_row.items()):
            _fail(
                "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
                f"current suffix row {index} differs from canonical transaction bindings",
            )
        if any(observed.get(field) for field in extra_columns):
            _fail(
                "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
                f"current suffix row {index} has non-empty legacy extension columns",
            )
        parameters = _manifest_request_parameters(
            observed["request_parameters"],
            strict=True,
        )
        if parameters is None:
            _fail(
                "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
                f"current suffix row {index} has invalid request parameters",
            )
        if parameters.get("publication_transaction_id") != transaction_id:
            _fail(
                "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
                f"current suffix row {index} has a different transaction",
            )
        observed_roles.append(_text(parameters.get("artifact_role"), "manifest.artifact_role"))
    expected_roles = [_text(artifact.get("role"), "artifact.role") for artifact in artifacts]
    if observed_roles != expected_roles or len(observed_roles) != len(set(observed_roles)):
        _fail(
            "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
            "current suffix must contain exactly one row per artifact role",
        )
    for row in prefix:
        parameters = _manifest_request_parameters(
            row.get("request_parameters", ""),
            strict=False,
        )
        if (
            parameters is not None
            and parameters.get("publication_transaction_id") == transaction_id
        ):
            _fail(
                "DOWNLOAD_MANIFEST_CURRENT_TRANSACTION_DUPLICATE",
                transaction_id,
            )


def _legacy_manifest_rows(
    raw: bytes,
) -> tuple[tuple[str, ...], tuple[dict[str, str], ...]]:
    try:
        handle = io.StringIO(raw.decode("utf-8-sig"), newline="")
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        if (
            not header
            or len(header) != len(set(header))
            or not set(_MANIFEST_COLUMNS).issubset(header)
        ):
            _fail(
                "DOWNLOAD_MANIFEST_INVALID",
                f"required={sorted(_MANIFEST_COLUMNS)} observed={list(header)}",
            )
        rows: list[dict[str, str]] = []
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                _fail("DOWNLOAD_MANIFEST_INVALID", "malformed manifest row")
            rows.append({field: str(row[field]) for field in header})
    except (UnicodeError, csv.Error) as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_MANIFEST_INVALID",
            str(exc),
        ) from exc
    return header, tuple(rows)


def _manifest_request_parameters(
    raw: str,
    *,
    strict: bool,
) -> dict[str, object] | None:
    def reject_constant(value: str) -> NoReturn:
        raise ValueError(f"non-finite JSON value: {value}")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        payload: dict[str, object] = {}
        for key, value in pairs:
            if key in payload:
                raise ValueError(f"duplicate JSON key: {key}")
            payload[key] = value
        return payload

    try:
        payload = json.loads(
            raw,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
        if not isinstance(payload, dict):
            raise ValueError("request_parameters must be a JSON object")
        _json_compatible(payload, "manifest.request_parameters")
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if strict and canonical != raw:
            raise ValueError("request_parameters are not canonical JSON")
        return payload
    except (TypeError, ValueError) as exc:
        if not strict:
            return None
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
            str(exc),
        ) from exc


def _validate_transaction(
    *,
    root: Path,
    transaction: Mapping[str, object],
    transaction_raw: bytes,
    pointer_path: Path,
    transaction_path: Path,
    current: ValidatedCurrentSnapshot,
    outer_previous_pointer_sha256: str | None,
    outer_manifest_raw: bytes,
) -> tuple[ValidatedDownloadPublication, bytes]:
    _exact_fields(
        transaction,
        {
            "schema_version",
            "contract_version",
            "transaction_id",
            "identity_sha256",
            "base_pointer_sha256",
            "published_at",
            "discovery_schema_version",
            "requested_window",
            "artifacts",
            "source_event_records",
            "download_manifest",
            "semantic_claims",
            "source_binding_scope",
            "raw_response_binding",
            "validation_scope",
            "dq_execution_provenance_verified",
            "atomicity_scope",
            "legacy_projection_role",
            "legacy_projection_atomicity",
            "consumer_cutover_allowed",
            "production_effect",
        },
        "DOWNLOAD_TRANSACTION_INVALID",
    )
    if transaction.get("contract_version") != DOWNLOAD_PUBLICATION_CONTRACT_VERSION:
        _fail("DOWNLOAD_TRANSACTION_INVALID", "unsupported contract_version")
    if transaction.get("discovery_schema_version") != DOWNLOAD_DISCOVERY_SCHEMA_VERSION:
        _fail("DOWNLOAD_TRANSACTION_INVALID", "unsupported discovery_schema_version")
    transaction_id = _transaction_id(transaction.get("transaction_id"))
    base_pointer_sha256 = transaction.get("base_pointer_sha256")
    if base_pointer_sha256 is not None:
        base_pointer_sha256 = _digest(base_pointer_sha256, "base_pointer_sha256")
    if base_pointer_sha256 != outer_previous_pointer_sha256:
        _fail(
            "DOWNLOAD_BASE_POINTER_BINDING_MISMATCH",
            "transaction base_pointer_sha256 differs from outer predecessor",
        )
    published_at = _utc_datetime_value(transaction.get("published_at"), "published_at")
    _safety(transaction)
    _atomicity_boundary(transaction)
    if transaction.get("semantic_claims") != list(_SEMANTIC_CLAIMS):
        _fail("DOWNLOAD_TRANSACTION_INVALID", "semantic claims mismatch")
    if (
        transaction.get("source_binding_scope") != _SOURCE_BINDING_SCOPE
        or transaction.get("raw_response_binding") != _RAW_RESPONSE_BINDING
    ):
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", "source binding scope mismatch")
    if (
        transaction.get("validation_scope") != _VALIDATION_SCOPE
        or transaction.get("dq_execution_provenance_verified") is not False
    ):
        _fail("DOWNLOAD_TRANSACTION_INVALID", "validation scope mismatch")
    window = _mapping(transaction.get("requested_window"), "requested_window")
    _exact_fields(window, {"start", "end"}, "DOWNLOAD_WINDOW_INVALID")
    requested_start = _date_value(window.get("start"), "requested_window.start")
    requested_end = _date_value(window.get("end"), "requested_window.end")
    if requested_start > requested_end:
        _fail("DOWNLOAD_WINDOW_INVALID", "start must be <= end")
    outer_manifest = _strict_canonical_json(
        outer_manifest_raw,
        schema=SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        code="DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
    )
    outer_quality = _mapping(outer_manifest.get("quality_binding"), "outer.quality_binding")
    outer_coverage = (
        _date_value(outer_quality.get("coverage_start"), "outer.coverage_start"),
        _date_value(outer_quality.get("coverage_end"), "outer.coverage_end"),
    )
    if (
        current.run_id != transaction_id
        or current.envelope.run_id != transaction_id
        or current.envelope.payload.sha256 != _sha256(transaction_raw)
        or current.envelope.payload.size_bytes != len(transaction_raw)
        or current.envelope.payload.schema_version != DOWNLOAD_PUBLICATION_SCHEMA_VERSION
        or current.envelope.as_of != requested_end
        or current.envelope.generated_at != published_at
        or outer_coverage != (requested_start, requested_end)
    ):
        _fail(
            "DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
            "transaction bytes/run/as_of/generated_at differ from validated outer publication",
        )

    artifacts_value = transaction.get("artifacts")
    if not isinstance(artifacts_value, list):
        _fail("DOWNLOAD_TRANSACTION_INVALID", "artifacts must be a list")
    artifact_payloads: list[dict[str, object]] = []
    artifact_sha256: dict[str, str] = {}
    artifact_row_count: dict[str, int] = {}
    artifact_paths: dict[str, Path] = {}
    artifact_sources: dict[str, set[str]] = {}
    artifact_row_keys: dict[str, tuple[tuple[str, str], ...]] = {}
    for raw in artifacts_value:
        artifact = _mapping(raw, "artifact")
        _exact_fields(
            artifact,
            {
                "role",
                "filename",
                "sha256",
                "size_bytes",
                "row_count",
                "row_key_columns",
                "source_event_ids",
                "immutable_path",
                "legacy_path",
            },
            "DOWNLOAD_TRANSACTION_INVALID",
        )
        role = _text(artifact.get("role"), "artifact.role")
        filename = _text(artifact.get("filename"), "artifact.filename")
        if _ARTIFACT_FILENAMES.get(role) != filename:
            _fail("DOWNLOAD_ARTIFACT_PATH_MISMATCH", role)
        immutable_path = _portable_relative(
            artifact.get("immutable_path"),
            "artifact.immutable_path",
        )
        expected_immutable = (
            PurePosixPath(_PUBLICATION_ROOT) / "members" / str(artifact.get("sha256")) / filename
        ).as_posix()
        if immutable_path != expected_immutable:
            _fail("DOWNLOAD_ARTIFACT_PATH_MISMATCH", immutable_path)
        if artifact.get("legacy_path") != filename:
            _fail("DOWNLOAD_ARTIFACT_PATH_MISMATCH", str(artifact.get("legacy_path")))
        digest = _digest(artifact.get("sha256"), "artifact.sha256")
        size = _integer(artifact.get("size_bytes"), "artifact.size_bytes")
        rows = _integer(artifact.get("row_count"), "artifact.row_count")
        row_key_columns = _ordered_string_list(
            artifact.get("row_key_columns"),
            "artifact.row_key_columns",
        )
        if row_key_columns != _ROW_KEY_COLUMNS[role]:
            _fail("DOWNLOAD_ARTIFACT_SCHEMA_MISMATCH", role)
        source_event_ids = _string_list(
            artifact.get("source_event_ids"),
            "artifact.source_event_ids",
        )
        if not source_event_ids or len(source_event_ids) != len(set(source_event_ids)):
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", role)
        immutable_raw = _read_required(root, immutable_path, "DOWNLOAD_ARTIFACT_MISSING")
        _verify_bytes(
            immutable_raw,
            digest=digest,
            size=size,
            code="DOWNLOAD_ARTIFACT_BINDING_MISMATCH",
        )
        observed_keys = _artifact_row_keys(
            immutable_raw,
            role=role,
            code="DOWNLOAD_ARTIFACT_SCHEMA_MISMATCH",
        )
        if len(observed_keys) != rows:
            _fail("DOWNLOAD_ARTIFACT_ROW_COUNT_MISMATCH", role)
        if role in artifact_paths:
            _fail("DOWNLOAD_ARTIFACT_SET_MISMATCH", role)
        artifact_paths[role] = root / Path(immutable_path)
        artifact_sha256[role] = digest
        artifact_row_count[role] = rows
        artifact_sources[role] = set(source_event_ids)
        artifact_row_keys[role] = observed_keys
        artifact_payloads.append(
            {
                "role": role,
                "filename": filename,
                "sha256": digest,
                "size_bytes": size,
                "row_count": rows,
                "row_key_columns": list(row_key_columns),
                "source_event_ids": list(source_event_ids),
            }
        )
    if set(artifact_paths) not in (
        {"prices", "rates"},
        {"prices", "rates", "secondary_prices"},
    ):
        _fail("DOWNLOAD_ARTIFACT_SET_MISMATCH", str(sorted(artifact_paths)))
    artifact_payloads.sort(key=lambda item: str(item["role"]))

    source_payloads = _validated_transaction_sources(
        transaction.get("source_event_records"),
        root=root,
        base_pointer_sha256=base_pointer_sha256,
        artifact_sources=artifact_sources,
        artifact_row_keys=artifact_row_keys,
    )
    manifest = _mapping(transaction.get("download_manifest"), "download_manifest")
    _exact_fields(
        manifest,
        {"path", "legacy_path", "sha256", "size_bytes", "row_count"},
        "DOWNLOAD_MANIFEST_INVALID",
    )
    manifest_path = _portable_relative(manifest.get("path"), "download_manifest.path")
    expected_manifest = (
        PurePosixPath(_PUBLICATION_ROOT)
        / "members"
        / str(manifest.get("sha256"))
        / "download_manifest.csv"
    ).as_posix()
    if manifest_path != expected_manifest or manifest.get("legacy_path") != "download_manifest.csv":
        _fail("DOWNLOAD_MANIFEST_BINDING_MISMATCH", manifest_path)
    manifest_digest = _digest(manifest.get("sha256"), "download_manifest.sha256")
    manifest_size = _integer(manifest.get("size_bytes"), "download_manifest.size_bytes")
    manifest_rows = _integer(manifest.get("row_count"), "download_manifest.row_count")
    manifest_raw = _read_required(root, manifest_path, "DOWNLOAD_MANIFEST_MISSING")
    _verify_bytes(
        manifest_raw,
        digest=manifest_digest,
        size=manifest_size,
        code="DOWNLOAD_MANIFEST_BINDING_MISMATCH",
    )
    _, observed_manifest_rows = _csv_shape(
        manifest_raw,
        required_columns=_MANIFEST_COLUMNS,
        code="DOWNLOAD_MANIFEST_INVALID",
    )
    if observed_manifest_rows != manifest_rows:
        _fail("DOWNLOAD_MANIFEST_ROW_COUNT_MISMATCH", manifest_path)
    _validate_current_legacy_manifest_semantics(
        root=root,
        manifest_raw=manifest_raw,
        transaction_id=transaction_id,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
        artifacts=artifact_payloads,
        source_payloads=source_payloads,
    )
    identity = {
        "base_pointer_sha256": base_pointer_sha256,
        "published_at": transaction["published_at"],
        "requested_window": dict(window),
        "artifacts": artifact_payloads,
        "source_event_records": source_payloads,
        "semantic_claims": list(_SEMANTIC_CLAIMS),
        "source_binding_scope": _SOURCE_BINDING_SCOPE,
        "raw_response_binding": _RAW_RESPONSE_BINDING,
        "validation_scope": _VALIDATION_SCOPE,
        "dq_execution_provenance_verified": False,
        "atomicity_scope": _ATOMICITY_SCOPE,
        "legacy_projection_role": _LEGACY_PROJECTION_ROLE,
        "legacy_projection_atomicity": _LEGACY_PROJECTION_ATOMICITY,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    identity_sha256 = _canonical_sha256(identity)
    if transaction.get("identity_sha256") != identity_sha256:
        _fail("DOWNLOAD_TRANSACTION_ID_MISMATCH", "identity_sha256 mismatch")
    if transaction_id != f"download_txn_{identity_sha256[:32]}":
        _fail("DOWNLOAD_TRANSACTION_ID_MISMATCH", transaction_id)

    return (
        ValidatedDownloadPublication(
            transaction_id=transaction_id,
            transaction_manifest_path=transaction_path,
            transaction_manifest_sha256=_sha256(transaction_raw),
            discovery_pointer_path=pointer_path,
            discovery_pointer_sha256=current.pointer_sha256,
            prices_path=artifact_paths["prices"],
            rates_path=artifact_paths["rates"],
            manifest_path=root / Path(manifest_path),
            secondary_prices_path=artifact_paths.get("secondary_prices"),
            legacy_prices_path=root / "prices_daily.csv",
            legacy_rates_path=root / "rates_daily.csv",
            legacy_manifest_path=root / "download_manifest.csv",
            legacy_secondary_prices_path=(
                root / "prices_marketstack_daily.csv"
                if "secondary_prices" in artifact_paths
                else None
            ),
            requested_start=requested_start,
            requested_end=requested_end,
            artifact_sha256=dict(sorted(artifact_sha256.items())),
            artifact_row_count=dict(sorted(artifact_row_count.items())),
            manifest_sha256=manifest_digest,
            manifest_row_count=manifest_rows,
            legacy_projection_verified=False,
        ),
        manifest_raw,
    )


def _validated_transaction_sources(
    value: object,
    *,
    root: Path,
    base_pointer_sha256: str | None,
    artifact_sources: Mapping[str, set[str]],
    artifact_row_keys: Mapping[str, tuple[tuple[str, str], ...]],
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", "source_event_records must be a list")
    observed_sources: dict[str, set[str]] = {}
    identities: set[str] = set()
    normalized: list[dict[str, object]] = []
    partitions: dict[
        str,
        list[tuple[str, str, int, str, tuple[tuple[str, str], ...]]],
    ] = {}
    for raw in value:
        source = _mapping(raw, "source_binding")
        _exact_fields(
            source,
            {
                "source_event_id",
                "artifact_role",
                "source_kind",
                "source_id",
                "provider",
                "endpoint",
                "request_parameters",
                "request_parameters_sha256",
                "allocation_mode",
                "explicit_row_keys",
                "winning_row_count",
                "winning_row_keys_sha256",
                "replay_inputs",
            },
            "DOWNLOAD_SOURCE_BINDING_MISMATCH",
        )
        role = _text(source.get("artifact_role"), "source.artifact_role")
        source_kind = _text(source.get("source_kind"), "source.source_kind")
        if source_kind not in _SOURCE_KINDS:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_kind)
        source_id = _text(source.get("source_id"), "source.source_id")
        source_event_id = _text(source.get("source_event_id"), "source.source_event_id")
        if role not in artifact_row_keys or source_event_id in identities:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_event_id)
        identities.add(source_event_id)
        provider = _text(source.get("provider"), "source.provider")
        endpoint = _text(source.get("endpoint"), "source.endpoint")
        request_parameters = _mapping(
            source.get("request_parameters"),
            "source.request_parameters",
        )
        _json_compatible(request_parameters, "source.request_parameters")
        request_sha = _digest(
            source.get("request_parameters_sha256"),
            "source.request_parameters_sha256",
        )
        if request_sha != _canonical_sha256(request_parameters):
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_event_id)
        allocation_mode = _text(
            source.get("allocation_mode"),
            "source.allocation_mode",
        )
        if allocation_mode not in {_REMAINDER, _EXPLICIT_KEYS}:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_event_id)
        explicit_row_keys = _transaction_row_keys(
            source.get("explicit_row_keys"),
            role=role,
        )
        if allocation_mode == _REMAINDER and explicit_row_keys:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", source_event_id)
        rows = _integer(
            source.get("winning_row_count"),
            "source.winning_row_count",
        )
        winning_digest = _digest(
            source.get("winning_row_keys_sha256"),
            "source.winning_row_keys_sha256",
        )
        replay_inputs = _validated_transaction_replay_inputs(
            source.get("replay_inputs"),
            root=root,
            source_event_id=source_event_id,
        )
        _validated_source_kind_payload(
            source_kind=source_kind,
            artifact_role=role,
            request_parameters=request_parameters,
            replay_inputs=replay_inputs,
            source_event_id=source_event_id,
        )
        if source_kind == _CANONICAL_PREDECESSOR_REUSE:
            _validate_resolved_predecessor_binding(
                root=root,
                artifact_role=role,
                request_parameters=request_parameters,
                base_pointer_sha256=base_pointer_sha256,
                source_event_id=source_event_id,
            )
        observed_sources.setdefault(role, set()).add(source_event_id)
        partitions.setdefault(role, []).append(
            (
                source_event_id,
                allocation_mode,
                rows,
                winning_digest,
                explicit_row_keys,
            )
        )
        normalized.append(
            {
                "source_event_id": source_event_id,
                "artifact_role": role,
                "source_kind": source_kind,
                "source_id": source_id,
                "provider": provider,
                "endpoint": endpoint,
                "request_parameters": dict(request_parameters),
                "request_parameters_sha256": request_sha,
                "allocation_mode": allocation_mode,
                "explicit_row_keys": [list(key) for key in explicit_row_keys],
                "winning_row_count": rows,
                "winning_row_keys_sha256": winning_digest,
                "replay_inputs": replay_inputs,
            }
        )
    if {role: sources for role, sources in observed_sources.items()} != {
        role: set(sources) for role, sources in artifact_sources.items()
    }:
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", "artifact/source sets differ")
    for role, final_values in artifact_row_keys.items():
        final_keys = set(final_values)
        role_partitions = partitions.get(role, [])
        remainder = [item for item in role_partitions if item[1] == _REMAINDER]
        if len(remainder) != 1:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: remainder count")
        allocated: set[tuple[str, str]] = set()
        winning_counts: dict[str, int] = {}
        for event_id, mode, rows, digest, explicit_keys in role_partitions:
            if mode == _REMAINDER:
                continue
            winning = set(explicit_keys)
            if not winning.issubset(final_keys) or allocated.intersection(winning):
                _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", event_id)
            if rows != len(winning) or digest != _row_keys_sha256(explicit_keys):
                _fail("DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH", event_id)
            allocated.update(winning)
            winning_counts[event_id] = len(winning)
        remainder_keys = tuple(sorted(final_keys - allocated))
        remainder_event_id, _, remainder_rows, remainder_digest, _ = remainder[0]
        if (
            remainder_rows != len(remainder_keys)
            or remainder_digest != _row_keys_sha256(remainder_keys)
            or (final_keys and not remainder_keys)
        ):
            _fail("DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH", remainder_event_id)
        winning_counts[remainder_event_id] = len(remainder_keys)
        expected_remainder = min(
            winning_counts,
            key=lambda event_id: (-winning_counts[event_id], event_id),
        )
        if remainder_event_id != expected_remainder:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: unstable remainder")
    normalized.sort(key=lambda item: str(item["source_event_id"]))
    return normalized


def _validated_transaction_replay_inputs(
    value: object,
    *,
    root: Path,
    source_event_id: str,
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
    normalized: list[dict[str, object]] = []
    roles: set[str] = set()
    for raw in value:
        replay_input = _mapping(raw, "replay_input")
        _exact_fields(
            replay_input,
            {
                "input_role",
                "filename",
                "path",
                "sha256",
                "size_bytes",
                "row_count",
            },
            "DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH",
        )
        input_role = _text(replay_input.get("input_role"), "replay_input.input_role")
        if input_role in roles:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
        roles.add(input_role)
        filename = _replay_input_filename(replay_input.get("filename"))
        digest = _digest(replay_input.get("sha256"), "replay_input.sha256")
        size = _integer(replay_input.get("size_bytes"), "replay_input.size_bytes")
        rows = _integer(replay_input.get("row_count"), "replay_input.row_count")
        path = _portable_relative(replay_input.get("path"), "replay_input.path")
        expected_path = (
            PurePosixPath(_PUBLICATION_ROOT) / "source_inputs" / digest / filename
        ).as_posix()
        if path != expected_path:
            _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", input_role)
        content = _read_required(root, path, "DOWNLOAD_REPLAY_INPUT_MISSING")
        _verify_bytes(
            content,
            digest=digest,
            size=size,
            code="DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH",
        )
        normalized.append(
            {
                "input_role": input_role,
                "filename": filename,
                "path": path,
                "sha256": digest,
                "size_bytes": size,
                "row_count": rows,
            }
        )
    normalized.sort(key=lambda item: str(item["input_role"]))
    if normalized != value:
        _fail("DOWNLOAD_REPLAY_INPUT_BINDING_MISMATCH", source_event_id)
    return normalized


def _validate_resolved_predecessor_binding(
    *,
    root: Path,
    artifact_role: str,
    request_parameters: Mapping[str, object],
    base_pointer_sha256: str | None,
    source_event_id: str,
) -> None:
    if (
        base_pointer_sha256 is None
        or request_parameters.get("predecessor_discovery_pointer_sha256") != base_pointer_sha256
    ):
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    transaction_path = _portable_relative(
        request_parameters.get("predecessor_transaction_path"),
        "predecessor_transaction_path",
    )
    transaction_digest = _digest(
        request_parameters.get("predecessor_transaction_sha256"),
        "predecessor_transaction_sha256",
    )
    expected_transaction_path = (
        PurePosixPath(_PUBLICATION_ROOT)
        / "snapshots"
        / _DATASET_ID
        / transaction_digest
        / "payload.json"
    ).as_posix()
    if transaction_path != expected_transaction_path:
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    transaction_raw = _read_required(
        root,
        transaction_path,
        "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
    )
    _verify_bytes(
        transaction_raw,
        digest=transaction_digest,
        size=len(transaction_raw),
        code="DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
    )
    try:
        predecessor_value = json.loads(transaction_raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
            source_event_id,
        ) from exc
    if (
        not isinstance(predecessor_value, Mapping)
        or _canonical_bytes(predecessor_value) != transaction_raw
        or predecessor_value.get("schema_version") != DOWNLOAD_PUBLICATION_SCHEMA_VERSION
        or predecessor_value.get("transaction_id")
        != request_parameters.get("predecessor_transaction_id")
    ):
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    artifact_digest = _digest(
        request_parameters.get("predecessor_artifact_sha256"),
        "predecessor_artifact_sha256",
    )
    artifact_rows = _integer(
        request_parameters.get("predecessor_artifact_row_count"),
        "predecessor_artifact_row_count",
    )
    artifact_path = _portable_relative(
        request_parameters.get("predecessor_artifact_path"),
        "predecessor_artifact_path",
    )
    filename = _ARTIFACT_FILENAMES[artifact_role]
    expected_artifact_path = (
        PurePosixPath(_PUBLICATION_ROOT) / "members" / artifact_digest / filename
    ).as_posix()
    if artifact_path != expected_artifact_path:
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    artifact_raw = _read_required(
        root,
        artifact_path,
        "DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
    )
    if (
        _sha256(artifact_raw) != artifact_digest
        or len(
            _artifact_row_keys(
                artifact_raw,
                role=artifact_role,
                code="DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
            )
        )
        != artifact_rows
    ):
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    predecessor_artifacts = predecessor_value.get("artifacts")
    if not isinstance(predecessor_artifacts, list):
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)
    matching = [
        item
        for item in predecessor_artifacts
        if isinstance(item, Mapping) and item.get("role") == artifact_role
    ]
    if len(matching) != 1 or (
        matching[0].get("sha256"),
        matching[0].get("row_count"),
        matching[0].get("immutable_path"),
    ) != (artifact_digest, artifact_rows, artifact_path):
        _fail("DOWNLOAD_PREDECESSOR_BINDING_MISMATCH", source_event_id)


def _write_compatibility_artifact(root: Path, relative_path: str, content: bytes) -> None:
    """Atomic leaf replacement hook for the non-atomic compatibility projection."""

    _write(root, relative_path, content, immutable=False)


def _retire_compatibility_artifact(root: Path, relative_path: str) -> None:
    """Remove one root-contained fixed-path projection that is absent canonically."""

    normalized = _portable_relative(relative_path, "legacy_retirement.path")
    target = root / Path(normalized)
    if target.parent != root:
        _fail("DOWNLOAD_ARTIFACT_PATH_MISMATCH", normalized)
    if not os.path.lexists(target):
        return
    target.unlink()
    if os.path.lexists(target):
        _fail("DOWNLOAD_LEGACY_RETIREMENT_FAILED", normalized)


def _prepare_root(output_dir: Path, *, create: bool) -> Path:
    if not isinstance(output_dir, Path):
        raise TypeError("output_dir must be Path")
    if create:
        output_dir.mkdir(parents=True, exist_ok=True)
    try:
        root = output_dir.resolve(strict=True)
    except OSError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PUBLICATION_ROOT_INVALID",
            str(exc),
            path=output_dir,
        ) from exc
    if not root.is_dir():
        _fail("DOWNLOAD_PUBLICATION_ROOT_INVALID", str(root))
    return root


def _write(root: Path, relative_path: str, content: bytes, *, immutable: bool) -> None:
    try:
        write_contained_artifact_bytes(
            root=root,
            relative_path=relative_path,
            content=content,
            immutable=immutable,
        )
    except DataPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_PUBLICATION_STORAGE_INVALID",
            str(exc),
            path=getattr(exc, "path", root / Path(relative_path)),
        ) from exc


def _read_required(root: Path, relative_path: str, code: str) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        raise DownloadPublicationIntegrityError(
            code,
            str(exc),
            path=getattr(exc, "path", root / Path(relative_path)),
        ) from exc


def _read_optional(root: Path, relative_path: str) -> bytes | None:
    path = root / Path(relative_path)
    if not os.path.lexists(path):
        return None
    return _read_required(root, relative_path, "DOWNLOAD_PUBLICATION_INPUT_INVALID")


def _strict_canonical_json(
    raw: bytes,
    *,
    schema: str,
    code: str,
) -> dict[str, object]:
    def reject_constant(value: str) -> NoReturn:
        raise ValueError(f"non-finite JSON value: {value}")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        payload: dict[str, object] = {}
        for key, value in pairs:
            if key in payload:
                raise ValueError(f"duplicate JSON key: {key}")
            payload[key] = value
        return payload

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
    except (UnicodeError, ValueError) as exc:
        raise DownloadPublicationIntegrityError(code, str(exc)) from exc
    if not isinstance(payload, dict):
        _fail(code, "payload must be a JSON object")
    if payload.get("schema_version") != schema:
        _fail(code, f"unsupported schema_version={payload.get('schema_version')!r}")
    _json_compatible(payload, "payload")
    if _canonical_bytes(payload) != raw:
        _fail(code, "payload bytes are not canonical")
    return payload


def _verify_bytes(raw: bytes, *, digest: str, size: int, code: str) -> None:
    if len(raw) != size or _sha256(raw) != digest:
        _fail(code, f"expected_sha={digest} expected_size={size}")


def _csv_shape(
    raw: bytes,
    *,
    required_columns: Sequence[str],
    code: str,
) -> tuple[tuple[str, ...], int]:
    try:
        handle = io.StringIO(raw.decode("utf-8-sig"), newline="")
        reader = csv.reader(handle)
        header = tuple(next(reader, ()))
        rows = sum(1 for _ in reader)
    except (UnicodeError, csv.Error) as exc:
        raise DownloadPublicationIntegrityError(code, str(exc)) from exc
    if not header or len(header) != len(set(header)) or not set(required_columns).issubset(header):
        _fail(code, f"required={sorted(required_columns)} observed={list(header)}")
    return header, rows


def _artifact_row_keys(
    raw: bytes,
    *,
    role: str,
    code: str,
) -> tuple[tuple[str, str], ...]:
    try:
        handle = io.StringIO(raw.decode("utf-8-sig"), newline="")
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        if (
            not header
            or len(header) != len(set(header))
            or not set(_REQUIRED_COLUMNS[role]).issubset(header)
        ):
            _fail(code, f"invalid artifact columns for {role}")
        keys: list[tuple[str, str]] = []
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                _fail(code, f"malformed artifact row for {role}")
            columns = _ROW_KEY_COLUMNS[role]
            key = _validated_row_key(
                (str(row[columns[0]]), str(row[columns[1]])),
                role=role,
                code=code,
            )
            keys.append(key)
    except (UnicodeError, csv.Error) as exc:
        raise DownloadPublicationIntegrityError(code, str(exc)) from exc
    if len(keys) != len(set(keys)):
        _fail("DOWNLOAD_ARTIFACT_KEY_DUPLICATE", role)
    return tuple(sorted(keys))


def _transaction_row_keys(
    value: object,
    *,
    role: str,
) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: explicit_row_keys")
    keys: list[tuple[str, str]] = []
    for raw in value:
        if not isinstance(raw, list) or len(raw) != 2:
            _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: explicit row key")
        keys.append(
            _validated_row_key(
                (raw[0], raw[1]),
                role=role,
                code="DOWNLOAD_SOURCE_BINDING_MISMATCH",
            )
        )
    normalized = tuple(keys)
    if normalized != tuple(sorted(set(normalized))):
        _fail("DOWNLOAD_SOURCE_BINDING_MISMATCH", f"{role}: explicit row key order")
    return normalized


def _validated_row_key(
    value: object,
    *,
    role: str,
    code: str = "DOWNLOAD_SOURCE_BINDING_MISMATCH",
) -> tuple[str, str]:
    if type(value) is not tuple or len(value) != 2:
        _fail(code, f"{role}: row key must be a two-item tuple")
    identity, raw_date = value
    if (
        not isinstance(identity, str)
        or not isinstance(raw_date, str)
        or not _valid_text(identity)
        or not _valid_text(raw_date)
    ):
        _fail(code, f"{role}: row key contains invalid text")
    try:
        parsed_date = date.fromisoformat(raw_date)
    except ValueError as exc:
        raise DownloadPublicationIntegrityError(code, f"{role}: invalid row key date") from exc
    if parsed_date.isoformat() != raw_date:
        _fail(code, f"{role}: row key date is not canonical")
    return identity, raw_date


def _row_keys_sha256(values: Sequence[tuple[str, str]]) -> str:
    return _canonical_sha256([list(value) for value in sorted(values)])


def _canonical_bytes(payload: object) -> bytes:
    _json_compatible(payload, "payload")
    return canonical_json_bytes(payload)


def _canonical_sha256(payload: object) -> str:
    return _sha256(_canonical_bytes(payload))


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json_mapping(value: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(value, Mapping):
        _fail("DOWNLOAD_JSON_FIELD_INVALID", "mapping must be a Mapping")
    payload = dict(value)
    _json_compatible(payload, "mapping")
    # Round-trip removes custom Mapping implementations while preserving strict types.
    normalized = json.loads(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )
    if not isinstance(normalized, dict):
        raise AssertionError("JSON mapping normalization must return dict")
    return normalized


def _json_compatible(value: object, field: str) -> None:
    if value is None or isinstance(value, str | bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            _fail("DOWNLOAD_NON_FINITE_VALUE", field)
        return
    if isinstance(value, list | tuple):
        for index, item in enumerate(value):
            _json_compatible(item, f"{field}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not _valid_text(key):
                _fail("DOWNLOAD_JSON_FIELD_INVALID", field)
            _json_compatible(item, f"{field}.{key}")
        return
    _fail("DOWNLOAD_JSON_FIELD_INVALID", f"{field}: {type(value).__name__}")


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        _fail("DOWNLOAD_FIELD_INVALID", f"{field} must be a mapping")
    return dict(value)


def _mapping_list(value: object, field: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        _fail("DOWNLOAD_FIELD_INVALID", f"{field} must be a list")
    return tuple(_mapping(item, f"{field}[]") for item in value)


def _exact_fields(
    payload: Mapping[str, object],
    expected: set[str],
    code: str,
) -> None:
    if set(payload) != expected:
        _fail(
            code,
            f"fields mismatch missing={sorted(expected - set(payload))} "
            f"unknown={sorted(set(payload) - expected)}",
        )


def _transaction_id(value: object) -> str:
    result = _text(value, "transaction_id")
    if not _TRANSACTION_ID_RE.fullmatch(result):
        _fail("DOWNLOAD_TRANSACTION_ID_MISMATCH", result)
    return result


def _digest(value: object, field: str) -> str:
    result = _text(value, field)
    if not _SHA256_RE.fullmatch(result):
        _fail("DOWNLOAD_FIELD_INVALID", field)
    return result


def _integer(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        _fail("DOWNLOAD_FIELD_INVALID", field)
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not _valid_text(value):
        _fail("DOWNLOAD_FIELD_INVALID", field)
    return value


def _string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not _valid_text(item) for item in value):
        _fail("DOWNLOAD_FIELD_INVALID", field)
    result = tuple(value)
    if list(result) != sorted(result):
        _fail("DOWNLOAD_FIELD_INVALID", f"{field} must be sorted")
    return result


def _ordered_string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not _valid_text(item) for item in value
    ):
        _fail("DOWNLOAD_FIELD_INVALID", field)
    return tuple(value)


def _portable_relative(value: object, field: str) -> str:
    result = _text(value, field)
    path = PurePosixPath(result)
    if (
        path.is_absolute()
        or result != path.as_posix()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or "\\" in result
    ):
        _fail("DOWNLOAD_FIELD_INVALID", field)
    return result


def _date_value(value: object, field: str) -> date:
    text = _text(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_WINDOW_INVALID",
            f"{field}: {exc}",
        ) from exc
    if parsed.isoformat() != text:
        _fail("DOWNLOAD_WINDOW_INVALID", field)
    return parsed


def _utc_datetime(value: datetime, field: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        _fail("DOWNLOAD_CHRONOLOGY_INVALID", f"{field} must be UTC")


def _utc_datetime_value(value: object, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DownloadPublicationIntegrityError(
            "DOWNLOAD_CHRONOLOGY_INVALID",
            f"{field}: {exc}",
        ) from exc
    _utc_datetime(parsed, field)
    if parsed.isoformat() != text:
        _fail("DOWNLOAD_CHRONOLOGY_INVALID", f"{field} must be canonical")
    return parsed


def _safety(payload: Mapping[str, object]) -> None:
    if payload.get("consumer_cutover_allowed") is not False:
        _fail("DOWNLOAD_CONSUMER_CUTOVER_INVALID", "consumer cutover must remain false")
    if payload.get("production_effect") != "none":
        _fail("DOWNLOAD_PRODUCTION_EFFECT_INVALID", "production_effect must be none")


def _atomicity_boundary(payload: Mapping[str, object]) -> None:
    expected = {
        "atomicity_scope": _ATOMICITY_SCOPE,
        "legacy_projection_role": _LEGACY_PROJECTION_ROLE,
        "legacy_projection_atomicity": _LEGACY_PROJECTION_ATOMICITY,
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            _fail("DOWNLOAD_ATOMICITY_SCOPE_INVALID", field)


def _legacy_projection_matches(
    *,
    root: Path,
    resolved: ValidatedDownloadPublication,
    validated_manifest_raw: bytes | None = None,
) -> bool:
    pairs: list[tuple[Path, Path]] = [
        (resolved.prices_path, resolved.legacy_prices_path),
        (resolved.rates_path, resolved.legacy_rates_path),
        (resolved.manifest_path, resolved.legacy_manifest_path),
    ]
    if (
        resolved.secondary_prices_path is not None
        and resolved.legacy_secondary_prices_path is not None
    ):
        pairs.append(
            (
                resolved.secondary_prices_path,
                resolved.legacy_secondary_prices_path,
            )
        )
    elif os.path.lexists(root / "prices_marketstack_daily.csv"):
        return False
    for immutable, legacy in pairs:
        immutable_relative = immutable.relative_to(root).as_posix()
        legacy_relative = legacy.relative_to(root).as_posix()
        immutable_raw = (
            validated_manifest_raw
            if immutable == resolved.manifest_path and validated_manifest_raw is not None
            else _read_required(
                root,
                immutable_relative,
                "DOWNLOAD_ARTIFACT_MISSING",
            )
        )
        try:
            legacy_raw = _read_optional(root, legacy_relative)
        except DownloadPublicationError:
            return False
        if legacy_raw != immutable_raw:
            return False
    return True


def _resolved_with_projection_state(
    value: ValidatedDownloadPublication,
    *,
    verified: bool,
) -> ValidatedDownloadPublication:
    return replace(value, legacy_projection_verified=verified)


def _valid_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _fail(code: str, message: str) -> NoReturn:
    raise DownloadPublicationIntegrityError(code, message)
