from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePath
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "historical_portable_source_archive_policy.v1"
MANIFEST_SCHEMA_VERSION = "historical_portable_source_archive_manifest.v1"
ARCHIVE_LOCATOR_KIND = "project_relative_content_archive"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "historical_portable_source_archive_policy.yaml"
)
DEFAULT_MANIFEST_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research"
    / "legacy_lineage"
    / "trading2449_r0_r1_r2_historical_source_archive.v1.json"
)

REQUIRED_ARCHIVE_CONTRACT = {
    "schema_version": MANIFEST_SCHEMA_VERSION,
    "locator_kind": ARCHIVE_LOCATOR_KIND,
    "archive_root": "inputs/research/legacy_lineage/source_archive",
    "require_manifest_content_id": True,
    "require_policy_hash_binding": True,
    "require_sidecar_id_binding": True,
    "require_sidecar_sha256_binding": True,
    "require_source_binding_id": True,
    "require_source_sha256": True,
    "require_source_size": True,
    "require_git_provenance": True,
}
REQUIRED_RESOLUTION_CONTRACT = {
    "explicit_opt_in_required": True,
    "unlisted_source_action": "use_frozen_sidecar_locator",
    "archive_binding_mismatch_action": "fail_closed",
    "missing_archive_source_action": "fail_closed",
    "archive_tamper_action": "fail_closed",
    "reject_absolute_locator": True,
    "reject_parent_traversal": True,
    "require_locator_under_project_root": True,
    "active_locator_supersession_requires_exact_manifest_binding": True,
    "supported_legacy_locator_dispositions": [
        "historical_path_must_match_when_present",
        "active_locator_superseded_by_window_migration",
    ],
}
SAFETY = {
    "canonical_sidecar_rewrite_allowed": False,
    "immutable_legacy_artifact_rewrite_allowed": False,
    "active_config_overwrite_allowed": False,
    "research_recalculation_allowed": False,
    "candidate_generation_allowed": False,
    "parameter_search_allowed": False,
    "promotion_change_allowed": False,
    "production_effect": "none",
    "broker_action": "none",
}
REQUIRED_FAILURE_REASONS = {
    "ARCHIVE_POLICY_SCHEMA_INVALID",
    "ARCHIVE_MANIFEST_MISSING",
    "ARCHIVE_MANIFEST_SCHEMA_INVALID",
    "ARCHIVE_CONTENT_ID_MISMATCH",
    "ARCHIVE_POLICY_BINDING_MISMATCH",
    "ARCHIVE_SIDECAR_BINDING_MISMATCH",
    "ARCHIVE_SOURCE_BINDING_MISMATCH",
    "ARCHIVE_SOURCE_MISSING",
    "ARCHIVE_SOURCE_TAMPERED",
    "ARCHIVE_LOCATOR_PATH_TRAVERSAL",
    "ARCHIVE_LOCATOR_OUTSIDE_PROJECT_ROOT",
    "ARCHIVE_ACTIVE_LOCATOR_CONFLICT",
}
_GIT_HEX_LENGTH = 40
_SHA256_HEX_LENGTH = 64


class HistoricalPortableSourceArchiveError(ValueError):
    """A stable fail-closed historical archive resolution failure."""

    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


@dataclass(frozen=True)
class HistoricalArchiveResolution:
    binding_id: str
    path: Path
    legacy_locator_disposition: str
    evidence: dict[str, Any]


class HistoricalPortableSourceArchive:
    """Resolve reviewed historical source overlays bound to one frozen sidecar."""

    def __init__(
        self,
        *,
        manifest_path: Path,
        expected_sidecar_id: str,
        expected_sidecar_sha256: str,
        frozen_source_records: Sequence[Mapping[str, Any]],
        project_root: Path = PROJECT_ROOT,
        policy_path: Path = DEFAULT_POLICY_PATH,
    ) -> None:
        self.manifest_path = manifest_path.resolve()
        self.project_root = project_root.resolve()
        self.policy_path = policy_path.resolve()
        self.policy = load_historical_portable_source_archive_policy(self.policy_path)
        self.manifest = _load_manifest(self.manifest_path)
        self._frozen_records = _frozen_record_index(frozen_source_records)
        self._validate_contract(
            expected_sidecar_id=expected_sidecar_id,
            expected_sidecar_sha256=expected_sidecar_sha256,
        )
        self._records = {
            str(record["binding_id"]): record for record in _records(self.manifest.get("sources"))
        }

    def resolve(self, frozen_record: Mapping[str, Any]) -> HistoricalArchiveResolution | None:
        binding_id = str(frozen_record.get("binding_id", ""))
        archived = self._records.get(binding_id)
        if archived is None:
            return None
        if not _source_binding_matches(archived, frozen_record):
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_SOURCE_BINDING_MISMATCH",
                f"archive source binding disagrees with frozen sidecar for {binding_id}",
            )
        disposition = str(archived.get("legacy_locator_disposition", ""))
        self._validate_disposition(archived, disposition=disposition)
        path = _archive_locator_path(
            archived,
            project_root=self.project_root,
            expected_sidecar_id=str(
                _mapping(self.manifest.get("sidecar_binding")).get("sidecar_id", "")
            ),
        )
        if not path.is_file():
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_SOURCE_MISSING", f"archive source missing for {binding_id}"
            )
        expected_hash = str(archived.get("sha256", ""))
        expected_size = int(archived.get("size", -1))
        if _file_sha256(path) != expected_hash or path.stat().st_size != expected_size:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_SOURCE_TAMPERED", f"archive source bytes drifted for {binding_id}"
            )
        return HistoricalArchiveResolution(
            binding_id=binding_id,
            path=path,
            legacy_locator_disposition=disposition,
            evidence={
                "archive_id": self.manifest.get("archive_id"),
                "archive_manifest_path": str(self.manifest_path),
                "archive_manifest_sha256": _file_sha256(self.manifest_path),
                "archive_policy_id": self.policy.get("policy_id"),
                "archive_policy_sha256": _file_sha256(self.policy_path),
                "archive_locator_kind": ARCHIVE_LOCATOR_KIND,
                "archive_locator_path": _mapping(archived.get("archive_locator")).get("path"),
                "legacy_locator_disposition": disposition,
                "provenance": _mapping(archived.get("provenance")),
            },
        )

    def evidence(self) -> dict[str, Any]:
        sidecar = _mapping(self.manifest.get("sidecar_binding"))
        return {
            "schema_version": "historical_portable_source_archive_resolution.v1",
            "status": "PASS",
            "archive_id": self.manifest.get("archive_id"),
            "archive_manifest_path": str(self.manifest_path),
            "archive_manifest_sha256": _file_sha256(self.manifest_path),
            "policy_id": self.policy.get("policy_id"),
            "policy_version": self.policy.get("policy_version"),
            "policy_sha256": _file_sha256(self.policy_path),
            "sidecar_id": sidecar.get("sidecar_id"),
            "sidecar_sha256": sidecar.get("sha256"),
            "source_binding_count": len(self._records),
            "production_effect": "none",
            "broker_action": "none",
        }

    def _validate_contract(self, *, expected_sidecar_id: str, expected_sidecar_sha256: str) -> None:
        if self.manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_MANIFEST_SCHEMA_INVALID", "unexpected archive manifest schema_version"
            )
        if _mapping(self.manifest.get("safety")) != SAFETY:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive safety boundary changed"
            )
        policy_binding = _mapping(self.manifest.get("policy_binding"))
        if (
            policy_binding.get("policy_id") != self.policy.get("policy_id")
            or policy_binding.get("policy_version") != self.policy.get("policy_version")
            or policy_binding.get("policy_sha256") != _file_sha256(self.policy_path)
        ):
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_POLICY_BINDING_MISMATCH", "archive policy binding is stale or invalid"
            )
        sidecar_binding = _mapping(self.manifest.get("sidecar_binding"))
        if (
            sidecar_binding.get("sidecar_id") != expected_sidecar_id
            or sidecar_binding.get("sha256") != expected_sidecar_sha256
        ):
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_SIDECAR_BINDING_MISMATCH",
                "archive manifest is not bound to the selected frozen sidecar",
            )
        _validated_relative_path(
            str(sidecar_binding.get("path", "")),
            project_root=self.project_root,
            traversal_reason="ARCHIVE_MANIFEST_SCHEMA_INVALID",
            outside_reason="ARCHIVE_MANIFEST_SCHEMA_INVALID",
        )
        sources = _records(self.manifest.get("sources"))
        if not sources:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive sources is empty"
            )
        binding_ids = [str(record.get("binding_id", "")) for record in sources]
        locator_paths = [
            str(_mapping(record.get("archive_locator")).get("path", "")) for record in sources
        ]
        if (
            any(not value for value in binding_ids)
            or len(binding_ids) != len(set(binding_ids))
            or len(locator_paths) != len(set(locator_paths))
        ):
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive source bindings are not unique"
            )
        for record in sources:
            _validate_source_record(
                record,
                project_root=self.project_root,
                expected_sidecar_id=expected_sidecar_id,
            )
            self._validate_disposition(
                record,
                disposition=str(record.get("legacy_locator_disposition", "")),
            )
            frozen = self._frozen_records.get(str(record.get("binding_id", "")))
            if frozen is None or not _source_binding_matches(record, frozen):
                raise HistoricalPortableSourceArchiveError(
                    "ARCHIVE_SOURCE_BINDING_MISMATCH",
                    "archive source is absent from or disagrees with the frozen sidecar",
                )
        unsigned_manifest = dict(self.manifest)
        unsigned_manifest.pop("archive_id", None)
        expected_id = _archive_id(unsigned_manifest)
        if self.manifest.get("archive_id") != expected_id:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_CONTENT_ID_MISMATCH", "archive source commitments changed"
            )

    def _validate_disposition(self, record: Mapping[str, Any], *, disposition: str) -> None:
        legacy_path = Path(str(record.get("legacy_path", "")))
        frozen_path = str(_mapping(record.get("frozen_locator")).get("path", ""))
        normalized_legacy = str(record.get("legacy_path", "")).replace("\\", "/")
        if disposition == "historical_path_must_match_when_present":
            return
        if disposition != "active_locator_superseded_by_window_migration":
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_MANIFEST_SCHEMA_INVALID", "unsupported legacy locator disposition"
            )
        if legacy_path.is_absolute() or normalized_legacy != frozen_path:
            raise HistoricalPortableSourceArchiveError(
                "ARCHIVE_ACTIVE_LOCATOR_CONFLICT",
                "active-locator supersession is not bound to the exact relative legacy locator",
            )


def load_historical_portable_source_archive_policy(
    path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    try:
        payload = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError) as exc:
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_POLICY_SCHEMA_INVALID", f"archive policy cannot be loaded: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_POLICY_SCHEMA_INVALID", "archive policy must be a mapping"
        )
    policy = dict(payload)
    if (
        policy.get("schema_version") != POLICY_SCHEMA_VERSION
        or policy.get("policy_id") != "historical_portable_source_archive_v1"
        or policy.get("policy_version") != 1
        or policy.get("status") != "owner_approved_portability_contract"
        or not str(policy.get("owner", "")).strip()
        or _mapping(policy.get("archive")) != REQUIRED_ARCHIVE_CONTRACT
        or _mapping(policy.get("resolution")) != REQUIRED_RESOLUTION_CONTRACT
        or _mapping(policy.get("safety")) != SAFETY
    ):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_POLICY_SCHEMA_INVALID", "archive policy contract changed"
        )
    validation = _mapping(policy.get("validation"))
    reasons = validation.get("required_failure_reasons")
    if (
        not isinstance(reasons, list)
        or not all(isinstance(item, str) for item in reasons)
        or len(reasons) != len(set(reasons))
        or set(reasons) != REQUIRED_FAILURE_REASONS
        or not str(validation.get("review_condition", "")).strip()
    ):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_POLICY_SCHEMA_INVALID", "archive validation contract changed"
        )
    return policy


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_MISSING", f"archive manifest missing: {path}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive manifest is not valid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive manifest must be a mapping"
        )
    return dict(payload)


def _validate_source_record(
    record: Mapping[str, Any], *, project_root: Path, expected_sidecar_id: str
) -> None:
    if (
        not str(record.get("binding_id", "")).strip()
        or not str(record.get("legacy_path", "")).strip()
    ):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive binding_id and legacy_path are required"
        )
    digest = str(record.get("sha256", ""))
    if not _is_lower_hex(digest, _SHA256_HEX_LENGTH):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive source sha256 is invalid"
        )
    if not isinstance(record.get("size"), int) or int(record["size"]) < 0:
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive source size is invalid"
        )
    frozen_locator = _mapping(record.get("frozen_locator"))
    if frozen_locator.get("kind") != "project_relative_content":
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "frozen locator kind is invalid"
        )
    _validated_relative_path(
        str(frozen_locator.get("path", "")),
        project_root=project_root,
        traversal_reason="ARCHIVE_MANIFEST_SCHEMA_INVALID",
        outside_reason="ARCHIVE_MANIFEST_SCHEMA_INVALID",
    )
    _archive_locator_path(
        record,
        project_root=project_root,
        expected_sidecar_id=expected_sidecar_id,
    )
    provenance = _mapping(record.get("provenance"))
    required_commits = (
        "source_commit",
        "sidecar_freeze_commit",
        "last_pre_migration_commit",
        "active_window_migration_commit",
    )
    if (
        any(
            not _is_lower_hex(str(provenance.get(key, "")), _GIT_HEX_LENGTH)
            for key in required_commits
        )
        or not _is_lower_hex(str(provenance.get("source_git_blob", "")), _GIT_HEX_LENGTH)
        or provenance.get("recovery_source") != "trusted_git_object_and_exact_historical_worktree"
    ):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "archive Git provenance is invalid"
        )


def _archive_locator_path(
    record: Mapping[str, Any], *, project_root: Path, expected_sidecar_id: str
) -> Path:
    locator = _mapping(record.get("archive_locator"))
    if locator.get("kind") != ARCHIVE_LOCATOR_KIND:
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_MANIFEST_SCHEMA_INVALID", "unsupported archive locator kind"
        )
    raw = str(locator.get("path", ""))
    resolved = _validated_relative_path(
        raw,
        project_root=project_root,
        traversal_reason="ARCHIVE_LOCATOR_PATH_TRAVERSAL",
        outside_reason="ARCHIVE_LOCATOR_OUTSIDE_PROJECT_ROOT",
    )
    expected_prefix = (
        f"{REQUIRED_ARCHIVE_CONTRACT['archive_root']}/{expected_sidecar_id}/"
        f"{record.get('sha256')}/"
    )
    if not raw.startswith(expected_prefix):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_SOURCE_BINDING_MISMATCH",
            "archive locator is not content-addressed under the bound sidecar",
        )
    return resolved


def _validated_relative_path(
    raw: str,
    *,
    project_root: Path,
    traversal_reason: str,
    outside_reason: str,
) -> Path:
    pure = PurePath(raw)
    if (
        not raw
        or pure.is_absolute()
        or bool(pure.anchor)
        or bool(pure.root)
        or Path(raw).is_absolute()
    ):
        raise HistoricalPortableSourceArchiveError(traversal_reason, "locator must be relative")
    if any(part in {"", ".", ".."} for part in pure.parts):
        raise HistoricalPortableSourceArchiveError(traversal_reason, f"unsafe locator: {raw}")
    root = project_root.resolve()
    resolved = (root / Path(*pure.parts)).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise HistoricalPortableSourceArchiveError(
            outside_reason, f"locator escaped project root: {raw}"
        ) from exc
    return resolved


def _archive_id(unsigned_manifest: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        unsigned_manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = sha256(encoded).hexdigest()
    return "historical-source-archive_" + digest[:20]


def _frozen_record_index(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    normalized = [dict(record) for record in records]
    binding_ids = [str(record.get("binding_id", "")) for record in normalized]
    if (
        not normalized
        or any(not binding_id for binding_id in binding_ids)
        or len(binding_ids) != len(set(binding_ids))
    ):
        raise HistoricalPortableSourceArchiveError(
            "ARCHIVE_SIDECAR_BINDING_MISMATCH",
            "frozen sidecar source bindings are empty, invalid, or duplicated",
        )
    return dict(zip(binding_ids, normalized, strict=True))


def _source_binding_matches(
    archive_record: Mapping[str, Any], frozen_record: Mapping[str, Any]
) -> bool:
    return (
        archive_record.get("legacy_path") == frozen_record.get("legacy_path")
        and _mapping(archive_record.get("frozen_locator")) == _mapping(frozen_record.get("locator"))
        and archive_record.get("sha256") == frozen_record.get("sha256")
        and archive_record.get("size") == frozen_record.get("size")
    )


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_lower_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


__all__ = [
    "ARCHIVE_LOCATOR_KIND",
    "DEFAULT_MANIFEST_PATH",
    "DEFAULT_POLICY_PATH",
    "HistoricalArchiveResolution",
    "HistoricalPortableSourceArchive",
    "HistoricalPortableSourceArchiveError",
    "MANIFEST_SCHEMA_VERSION",
    "POLICY_SCHEMA_VERSION",
    "load_historical_portable_source_archive_policy",
]
