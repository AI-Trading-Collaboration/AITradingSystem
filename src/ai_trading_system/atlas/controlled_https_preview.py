from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import urlsplit

import yaml

from ai_trading_system.platform.artifacts import canonical_json_bytes, write_bytes_atomic

CONTROLLED_PREVIEW_POLICY_SCHEMA = "atlas_controlled_https_preview_policy.v1"
CONTROLLED_PREVIEW_MANIFEST_SCHEMA = "atlas_controlled_https_preview_manifest.v1"
CONTROLLED_PREVIEW_REPLAY_SCHEMA = "atlas_controlled_https_preview_replay.v1"
CONTROLLED_PREVIEW_WRITER_VERSION = "atlas_controlled_https_preview_writer.v1"
DEFAULT_CONTROLLED_PREVIEW_POLICY_PATH = (
    "config/atlas/controlled_https_preview_policy.yaml"
)
PENDING_OWNER_DECISION = "PENDING_OWNER_DECISION"
APPROVED_LOCAL_BUNDLE = "APPROVED_LOCAL_BUNDLE"
TASK_ID = "TRADING-2526_ATLAS_ACCESSIBLE_RESEARCH_DRILLDOWN_AND_AUDIT_LINKAGE_V1"

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
_NETWORK_SCHEME_RE = re.compile(
    r"(?i)(?:https?|wss?|ftp|file|data|javascript):|(?<!:)//"
)
_LOCAL_NETWORK_RE = re.compile(
    r"(?i)(?:localhost|127(?:\.\d{1,3}){3}|\[?::1\]?|"
    r"10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|"
    r"172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2}|"
    r"169\.254(?:\.\d{1,3}){2})"
)
_HTML_REFERENCE_RE = re.compile(
    r"(?is)(?:src|href|action|poster)\s*=\s*(['\"])(.*?)\1"
)
_CSS_REFERENCE_RE = re.compile(r"(?is)url\(\s*(['\"]?)(.*?)\1\s*\)")
_DYNAMIC_NETWORK_API_RE = re.compile(
    r"(?i)\b(?:fetch|WebSocket|EventSource|XMLHttpRequest|sendBeacon)\b"
)
_TEXT_SUFFIXES = frozenset({".css", ".html", ".js", ".json", ".svg"})

_EXPECTED_POLICY_FIELDS = {
    "schema_version",
    "policy_id",
    "policy_version",
    "status",
    "owner",
    "task_id",
    "writer_version",
    "owner_decision_slots",
    "asset_policy",
    "security",
    "safety",
}
_EXPECTED_DECISION_SLOTS = {
    "hosting_provider",
    "https_origin",
    "private_access_mode",
    "ttl_hours",
    "cost_ceiling_usd",
    "retention_days",
    "browser_matrix",
    "viewport_matrix",
    "assistive_technology_matrix",
    "cleanup_authority",
}
_EXPECTED_ASSET_POLICY_FIELDS = {
    "required_entrypoint",
    "manifest_name",
    "allowed_media_types",
}
_EXPECTED_SECURITY = {
    "exact_allowlist_required": True,
    "exact_response_bytes_required": True,
    "regular_files_only": True,
    "symlinks_forbidden": True,
    "directory_listing_forbidden": True,
    "external_network_references_forbidden": True,
    "local_network_references_forbidden": True,
    "runtime_injection_forbidden": True,
    "authenticated_private_origin_required": True,
    "public_share_forbidden": True,
    "indexing_forbidden": True,
    "expired_or_unauthorized_fail_closed": True,
}
_EXPECTED_SAFETY_FIELDS = {
    "local_bundle_generation_authorized",
    "external_deployment_authorized",
    "browser_automation_authorized",
    "market_data_inclusion_authorized",
    "credentials_inclusion_authorized",
    "production_effect",
    "broker_action",
}


class ControlledHttpsPreviewError(ValueError):
    """Typed fail-closed error for the local controlled-preview contract."""


@dataclass(frozen=True)
class ControlledHttpsPreviewPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    task_id: str
    writer_version: str
    policy_sha256: str
    owner_decision_slots: Mapping[str, object]
    required_entrypoint: str
    manifest_name: str
    allowed_media_types: Mapping[str, str]
    security: Mapping[str, object]
    safety: Mapping[str, object]

    def __post_init__(self) -> None:
        if self.task_id != TASK_ID:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_TASK_ID_INVALID")
        if self.writer_version != CONTROLLED_PREVIEW_WRITER_VERSION:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_WRITER_VERSION_INVALID")
        if not _SHA256_RE.fullmatch(self.policy_sha256):
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_SHA256_INVALID")
        if set(self.owner_decision_slots) != _EXPECTED_DECISION_SLOTS:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_DECISION_SLOTS_INVALID")
        if _normal_relative_path(self.required_entrypoint) != self.required_entrypoint:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_ENTRYPOINT_INVALID")
        if _normal_relative_path(self.manifest_name) != self.manifest_name:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_MANIFEST_NAME_INVALID")
        if self.required_entrypoint == self.manifest_name:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_MANIFEST_COLLIDES")
        if not self.allowed_media_types:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_MEDIA_TYPES_EMPTY")
        for suffix, media_type in self.allowed_media_types.items():
            if (
                not isinstance(suffix, str)
                or suffix != suffix.lower()
                or not suffix.startswith(".")
                or not isinstance(media_type, str)
                or not media_type.strip()
            ):
                raise ControlledHttpsPreviewError("PREVIEW_POLICY_MEDIA_TYPE_INVALID")
        if dict(self.security) != _EXPECTED_SECURITY:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_SECURITY_INVALID")
        if set(self.safety) != _EXPECTED_SAFETY_FIELDS:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_SAFETY_FIELDS_INVALID")
        if self.safety.get("production_effect") != "none":
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_PRODUCTION_EFFECT_INVALID")
        if self.safety.get("broker_action") != "none":
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_BROKER_ACTION_INVALID")
        for field in (
            "external_deployment_authorized",
            "browser_automation_authorized",
            "market_data_inclusion_authorized",
            "credentials_inclusion_authorized",
        ):
            if self.safety.get(field) is not False:
                raise ControlledHttpsPreviewError(f"PREVIEW_POLICY_SAFETY_NOT_FALSE:{field}")
        if self.status == PENDING_OWNER_DECISION:
            if set(self.owner_decision_slots.values()) != {PENDING_OWNER_DECISION}:
                raise ControlledHttpsPreviewError("PREVIEW_POLICY_OWNER_DECISION_PREEMPTED")
            if self.safety.get("local_bundle_generation_authorized") is not False:
                raise ControlledHttpsPreviewError(
                    "PREVIEW_POLICY_PENDING_LOCAL_BUNDLE_AUTHORIZED"
                )
        elif self.status == APPROVED_LOCAL_BUNDLE:
            if self.safety.get("local_bundle_generation_authorized") is not True:
                raise ControlledHttpsPreviewError(
                    "PREVIEW_POLICY_LOCAL_BUNDLE_NOT_AUTHORIZED"
                )
            _validated_decisions(self.owner_decision_slots)
        else:
            raise ControlledHttpsPreviewError("PREVIEW_POLICY_STATUS_INVALID")

    @property
    def ready_for_local_bundle(self) -> bool:
        return self.status == APPROVED_LOCAL_BUNDLE


@dataclass(frozen=True)
class PreviewAssetIdentity:
    path: str
    media_type: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if _normal_relative_path(self.path) != self.path:
            raise ControlledHttpsPreviewError("PREVIEW_ASSET_PATH_INVALID")
        if not self.media_type.strip():
            raise ControlledHttpsPreviewError("PREVIEW_ASSET_MEDIA_TYPE_INVALID")
        if self.size_bytes < 0:
            raise ControlledHttpsPreviewError("PREVIEW_ASSET_SIZE_INVALID")
        if not _SHA256_RE.fullmatch(self.sha256):
            raise ControlledHttpsPreviewError("PREVIEW_ASSET_SHA256_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "media_type": self.media_type,
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class ControlledHttpsPreviewManifest:
    task_id: str
    source_commit: str
    source_generator: str
    source_generator_version: str
    policy_id: str
    policy_version: str
    policy_sha256: str
    writer_version: str
    generated_at_utc: datetime
    expires_at_utc: datetime
    hosting_provider: str
    https_origin: str
    private_access_mode: str
    ttl_hours: int
    cost_ceiling_usd: str
    retention_days: int
    browser_matrix: tuple[str, ...]
    viewport_matrix: tuple[str, ...]
    assistive_technology_matrix: tuple[str, ...]
    cleanup_authority: str
    entrypoint: str
    manifest_name: str
    assets: tuple[PreviewAssetIdentity, ...]
    asset_set_sha256: str
    authorization_state: Literal["NOT_AUTHORIZED", "EXACT_PREAUTHORIZED"]
    authorization_ref: str | None
    production_effect: Literal["none"] = "none"
    broker_action: Literal["none"] = "none"

    def __post_init__(self) -> None:
        if self.task_id != TASK_ID:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_TASK_ID_INVALID")
        if not _COMMIT_RE.fullmatch(self.source_commit):
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_SOURCE_COMMIT_INVALID")
        if not _SHA256_RE.fullmatch(self.policy_sha256):
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_POLICY_SHA_INVALID")
        if self.writer_version != CONTROLLED_PREVIEW_WRITER_VERSION:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_WRITER_INVALID")
        generated = _utc(self.generated_at_utc, "generated_at_utc")
        expires = _utc(self.expires_at_utc, "expires_at_utc")
        if expires != generated + timedelta(hours=self.ttl_hours):
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_TTL_BINDING_INVALID")
        if not _valid_https_origin(self.https_origin):
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_HTTPS_ORIGIN_INVALID")
        if not self.assets or tuple(sorted(self.assets, key=lambda item: item.path)) != self.assets:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ASSET_ORDER_INVALID")
        if len({item.path for item in self.assets}) != len(self.assets):
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_DUPLICATE_ASSET")
        if self.entrypoint not in {item.path for item in self.assets}:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ENTRYPOINT_MISSING")
        if self.manifest_name in {item.path for item in self.assets}:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_SELF_REFERENCE")
        if _asset_set_sha256(self.assets) != self.asset_set_sha256:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ASSET_SET_SHA_INVALID")
        if self.authorization_state == "NOT_AUTHORIZED":
            if self.authorization_ref is not None:
                raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_AUTH_REF_UNEXPECTED")
        elif not self.authorization_ref:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_AUTH_REF_MISSING")

    def to_dict(self) -> dict[str, object]:
        return {
            "asset_set_sha256": self.asset_set_sha256,
            "assets": [item.to_dict() for item in self.assets],
            "assistive_technology_matrix": list(self.assistive_technology_matrix),
            "authorization_ref": self.authorization_ref,
            "authorization_state": self.authorization_state,
            "broker_action": self.broker_action,
            "browser_matrix": list(self.browser_matrix),
            "cleanup_authority": self.cleanup_authority,
            "cost_ceiling_usd": self.cost_ceiling_usd,
            "entrypoint": self.entrypoint,
            "expires_at_utc": _format_utc(self.expires_at_utc),
            "generated_at_utc": _format_utc(self.generated_at_utc),
            "hosting_provider": self.hosting_provider,
            "https_origin": self.https_origin,
            "manifest_name": self.manifest_name,
            "policy_id": self.policy_id,
            "policy_sha256": self.policy_sha256,
            "policy_version": self.policy_version,
            "private_access_mode": self.private_access_mode,
            "production_effect": self.production_effect,
            "retention_days": self.retention_days,
            "schema_version": CONTROLLED_PREVIEW_MANIFEST_SCHEMA,
            "source_commit": self.source_commit,
            "source_generator": self.source_generator,
            "source_generator_version": self.source_generator_version,
            "task_id": self.task_id,
            "ttl_hours": self.ttl_hours,
            "viewport_matrix": list(self.viewport_matrix),
            "writer_version": self.writer_version,
        }

    def canonical_json_bytes(self) -> bytes:
        return cast(bytes, canonical_json_bytes(self.to_dict()))

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes()).hexdigest()


@dataclass(frozen=True)
class ExternalPreviewAuthorization:
    authorization_ref: str
    authorization_state: Literal["EXACT_PREAUTHORIZED"]
    task_id: str
    source_commit: str
    asset_set_sha256: str
    https_origin: str
    authorized_at_utc: datetime
    expires_at_utc: datetime

    def __post_init__(self) -> None:
        if not self.authorization_ref.strip():
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_REF_INVALID")
        if self.task_id != TASK_ID:
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_TASK_ID_INVALID")
        if not _COMMIT_RE.fullmatch(self.source_commit):
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_SOURCE_COMMIT_INVALID")
        if not _SHA256_RE.fullmatch(self.asset_set_sha256):
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_ASSET_SET_SHA_INVALID")
        if not _valid_https_origin(self.https_origin):
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_ORIGIN_INVALID")
        if _utc(self.expires_at_utc, "expires_at_utc") <= _utc(
            self.authorized_at_utc, "authorized_at_utc"
        ):
            raise ControlledHttpsPreviewError("PREVIEW_AUTH_WINDOW_INVALID")


@dataclass(frozen=True)
class PreviewReplayReceipt:
    manifest_sha256: str
    asset_set_sha256: str
    checked_at_utc: datetime
    checked_file_count: int
    endpoint_origin: str | None
    status: Literal["PASS"] = "PASS"

    def to_dict(self) -> dict[str, object]:
        return {
            "asset_set_sha256": self.asset_set_sha256,
            "checked_at_utc": _format_utc(self.checked_at_utc),
            "checked_file_count": self.checked_file_count,
            "endpoint_origin": self.endpoint_origin,
            "manifest_sha256": self.manifest_sha256,
            "schema_version": CONTROLLED_PREVIEW_REPLAY_SCHEMA,
            "status": self.status,
        }


def load_controlled_https_preview_policy(
    *,
    repository_root: Path,
    policy_path: str = DEFAULT_CONTROLLED_PREVIEW_POLICY_PATH,
) -> ControlledHttpsPreviewPolicy:
    root = repository_root.resolve()
    normalized = _normal_relative_path(policy_path)
    selected = (root / normalized).resolve()
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_PATH_OUTSIDE_REPOSITORY") from exc
    raw = selected.read_bytes()
    try:
        payload = _mapping(yaml.safe_load(raw.decode("utf-8")), "policy")
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_YAML_INVALID") from exc
    if set(payload) != _EXPECTED_POLICY_FIELDS:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_FIELDS_INVALID")
    if payload.get("schema_version") != CONTROLLED_PREVIEW_POLICY_SCHEMA:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_SCHEMA_INVALID")
    decisions = _mapping(payload["owner_decision_slots"], "owner_decision_slots")
    asset_policy = _mapping(payload["asset_policy"], "asset_policy")
    if set(asset_policy) != _EXPECTED_ASSET_POLICY_FIELDS:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_ASSET_FIELDS_INVALID")
    media_types = _mapping(asset_policy["allowed_media_types"], "allowed_media_types")
    security = _mapping(payload["security"], "security")
    safety = _mapping(payload["safety"], "safety")
    return ControlledHttpsPreviewPolicy(
        policy_id=_required_text(payload, "policy_id"),
        policy_version=_required_text(payload, "policy_version"),
        status=_required_text(payload, "status"),
        owner=_required_text(payload, "owner"),
        task_id=_required_text(payload, "task_id"),
        writer_version=_required_text(payload, "writer_version"),
        policy_sha256=hashlib.sha256(raw).hexdigest(),
        owner_decision_slots={str(key): value for key, value in decisions.items()},
        required_entrypoint=_required_text(asset_policy, "required_entrypoint"),
        manifest_name=_required_text(asset_policy, "manifest_name"),
        allowed_media_types={str(key): str(value) for key, value in media_types.items()},
        security={str(key): value for key, value in security.items()},
        safety={str(key): value for key, value in safety.items()},
    )


def build_controlled_preview_manifest(
    *,
    source_directory: Path,
    allowlisted_paths: Sequence[str],
    policy: ControlledHttpsPreviewPolicy,
    source_commit: str,
    source_generator: str,
    source_generator_version: str,
    generated_at_utc: datetime,
) -> ControlledHttpsPreviewManifest:
    decisions = _require_local_bundle_ready(policy)
    if not _COMMIT_RE.fullmatch(source_commit):
        raise ControlledHttpsPreviewError("PREVIEW_SOURCE_COMMIT_INVALID")
    generated = _utc(generated_at_utc, "generated_at_utc")
    normalized_paths = tuple(sorted(_exact_allowlist(allowlisted_paths)))
    if policy.required_entrypoint not in normalized_paths:
        raise ControlledHttpsPreviewError("PREVIEW_ENTRYPOINT_NOT_ALLOWLISTED")
    if policy.manifest_name in normalized_paths:
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_MUST_NOT_BE_ALLOWLISTED")
    source_root = _regular_directory(source_directory, "PREVIEW_SOURCE_DIRECTORY_INVALID")
    assets = tuple(
        _asset_identity(source_root, path, policy.allowed_media_types)
        for path in normalized_paths
    )
    _validate_local_references(source_root, assets)
    ttl_hours = _positive_int(decisions["ttl_hours"], "ttl_hours")
    return ControlledHttpsPreviewManifest(
        task_id=policy.task_id,
        source_commit=source_commit,
        source_generator=_nonempty_string(source_generator, "source_generator"),
        source_generator_version=_nonempty_string(
            source_generator_version, "source_generator_version"
        ),
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy.policy_sha256,
        writer_version=policy.writer_version,
        generated_at_utc=generated,
        expires_at_utc=generated + timedelta(hours=ttl_hours),
        hosting_provider=str(decisions["hosting_provider"]),
        https_origin=str(decisions["https_origin"]),
        private_access_mode=str(decisions["private_access_mode"]),
        ttl_hours=ttl_hours,
        cost_ceiling_usd=str(_nonnegative_decimal(decisions["cost_ceiling_usd"])),
        retention_days=_nonnegative_int(decisions["retention_days"], "retention_days"),
        browser_matrix=_string_tuple(decisions["browser_matrix"], "browser_matrix"),
        viewport_matrix=_string_tuple(decisions["viewport_matrix"], "viewport_matrix"),
        assistive_technology_matrix=_string_tuple(
            decisions["assistive_technology_matrix"],
            "assistive_technology_matrix",
        ),
        cleanup_authority=str(decisions["cleanup_authority"]),
        entrypoint=policy.required_entrypoint,
        manifest_name=policy.manifest_name,
        assets=assets,
        asset_set_sha256=_asset_set_sha256(assets),
        authorization_state="NOT_AUTHORIZED",
        authorization_ref=None,
    )


def write_controlled_preview_bundle(
    *,
    source_directory: Path,
    output_directory: Path,
    allowlisted_paths: Sequence[str],
    policy: ControlledHttpsPreviewPolicy,
    source_commit: str,
    source_generator: str,
    source_generator_version: str,
    generated_at_utc: datetime,
) -> ControlledHttpsPreviewManifest:
    manifest = build_controlled_preview_manifest(
        source_directory=source_directory,
        allowlisted_paths=allowlisted_paths,
        policy=policy,
        source_commit=source_commit,
        source_generator=source_generator,
        source_generator_version=source_generator_version,
        generated_at_utc=generated_at_utc,
    )
    source_root = source_directory.resolve()
    output_root = output_directory.resolve()
    if (
        source_root == output_root
        or source_root in output_root.parents
        or output_root in source_root.parents
    ):
        raise ControlledHttpsPreviewError("PREVIEW_OUTPUT_SOURCE_OVERLAP")
    if output_root.exists():
        raise ControlledHttpsPreviewError("PREVIEW_OUTPUT_ALREADY_EXISTS")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent))
    try:
        for asset in manifest.assets:
            payload = _read_regular_file(source_root, asset.path)
            if len(payload) != asset.size_bytes or _sha256(payload) != asset.sha256:
                raise ControlledHttpsPreviewError(
                    f"PREVIEW_SOURCE_CHANGED_DURING_WRITE:{asset.path}"
                )
            write_bytes_atomic(staging / asset.path, payload)
        write_bytes_atomic(staging / manifest.manifest_name, manifest.canonical_json_bytes())
        os.replace(staging, output_root)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def load_controlled_preview_manifest(path: Path) -> ControlledHttpsPreviewManifest:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_JSON_INVALID") from exc
    if not isinstance(payload, Mapping):
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_MAPPING_REQUIRED")
    manifest = _manifest_from_mapping(payload)
    if manifest.canonical_json_bytes() != raw:
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_NONCANONICAL")
    return manifest


def replay_controlled_preview_bundle(
    *, bundle_directory: Path, checked_at_utc: datetime
) -> PreviewReplayReceipt:
    root = _regular_directory(bundle_directory, "PREVIEW_BUNDLE_DIRECTORY_INVALID")
    manifest_candidates = [
        path for path in root.iterdir() if path.name.endswith("preview_manifest.json")
    ]
    if len(manifest_candidates) != 1:
        raise ControlledHttpsPreviewError("PREVIEW_BUNDLE_MANIFEST_COUNT_INVALID")
    manifest = load_controlled_preview_manifest(manifest_candidates[0])
    if manifest_candidates[0].name != manifest.manifest_name:
        raise ControlledHttpsPreviewError("PREVIEW_BUNDLE_MANIFEST_NAME_MISMATCH")
    checked_at = _utc(checked_at_utc, "checked_at_utc")
    if checked_at > manifest.expires_at_utc:
        raise ControlledHttpsPreviewError("PREVIEW_BUNDLE_EXPIRED")
    expected_paths = {item.path for item in manifest.assets} | {manifest.manifest_name}
    actual_paths = _bundle_file_inventory(root)
    if actual_paths != expected_paths:
        raise ControlledHttpsPreviewError("PREVIEW_BUNDLE_FILE_SET_MISMATCH")
    for asset in manifest.assets:
        payload = _read_regular_file(root, asset.path)
        if len(payload) != asset.size_bytes or _sha256(payload) != asset.sha256:
            raise ControlledHttpsPreviewError(f"PREVIEW_BUNDLE_ASSET_DRIFT:{asset.path}")
    _validate_local_references(root, manifest.assets)
    return PreviewReplayReceipt(
        manifest_sha256=manifest.content_sha256,
        asset_set_sha256=manifest.asset_set_sha256,
        checked_at_utc=checked_at,
        checked_file_count=len(actual_paths),
        endpoint_origin=None,
    )


def bind_external_preview_authorization(
    *,
    manifest: ControlledHttpsPreviewManifest,
    authorization: ExternalPreviewAuthorization,
) -> ControlledHttpsPreviewManifest:
    if manifest.authorization_state != "NOT_AUTHORIZED":
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ALREADY_AUTHORIZED")
    if (
        authorization.task_id != manifest.task_id
        or authorization.source_commit != manifest.source_commit
        or authorization.asset_set_sha256 != manifest.asset_set_sha256
        or authorization.https_origin != manifest.https_origin
        or authorization.expires_at_utc > manifest.expires_at_utc
    ):
        raise ControlledHttpsPreviewError("PREVIEW_AUTHORIZATION_BINDING_MISMATCH")
    return replace(
        manifest,
        authorization_state="EXACT_PREAUTHORIZED",
        authorization_ref=authorization.authorization_ref,
    )


def validate_https_endpoint_bytes(
    *,
    manifest: ControlledHttpsPreviewManifest,
    authorization: ExternalPreviewAuthorization,
    endpoint_origin: str,
    response_bytes_by_path: Mapping[str, bytes],
    checked_at_utc: datetime,
) -> PreviewReplayReceipt:
    if manifest.authorization_state != "EXACT_PREAUTHORIZED":
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_NOT_AUTHORIZED")
    if manifest.authorization_ref != authorization.authorization_ref:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_AUTH_REF_MISMATCH")
    rebound = bind_external_preview_authorization(
        manifest=replace(
            manifest,
            authorization_state="NOT_AUTHORIZED",
            authorization_ref=None,
        ),
        authorization=authorization,
    )
    if rebound != manifest:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_AUTH_BINDING_INVALID")
    normalized_origin = _normalized_https_origin(endpoint_origin)
    if normalized_origin != manifest.https_origin:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_ORIGIN_MISMATCH")
    checked_at = _utc(checked_at_utc, "checked_at_utc")
    if checked_at < authorization.authorized_at_utc:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_AUTH_NOT_YET_VALID")
    if checked_at > authorization.expires_at_utc or checked_at > manifest.expires_at_utc:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_EXPIRED")
    expected_paths = {item.path for item in manifest.assets} | {manifest.manifest_name}
    normalized_responses = {
        _normal_relative_path(path): payload for path, payload in response_bytes_by_path.items()
    }
    if set(normalized_responses) != expected_paths:
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_FILE_SET_MISMATCH")
    identities = {item.path: item for item in manifest.assets}
    for path, identity in identities.items():
        payload = normalized_responses[path]
        if len(payload) != identity.size_bytes or _sha256(payload) != identity.sha256:
            raise ControlledHttpsPreviewError(f"PREVIEW_ENDPOINT_BYTE_DRIFT:{path}")
    if normalized_responses[manifest.manifest_name] != manifest.canonical_json_bytes():
        raise ControlledHttpsPreviewError("PREVIEW_ENDPOINT_MANIFEST_BYTE_DRIFT")
    return PreviewReplayReceipt(
        manifest_sha256=manifest.content_sha256,
        asset_set_sha256=manifest.asset_set_sha256,
        checked_at_utc=checked_at,
        checked_file_count=len(normalized_responses),
        endpoint_origin=normalized_origin,
    )


def _manifest_from_mapping(payload: Mapping[str, object]) -> ControlledHttpsPreviewManifest:
    expected = set(ControlledHttpsPreviewManifest.__dataclass_fields__) | {"schema_version"}
    if (
        set(payload) != expected
        or payload.get("schema_version") != CONTROLLED_PREVIEW_MANIFEST_SCHEMA
    ):
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_FIELDS_INVALID")
    raw_assets = payload["assets"]
    if not isinstance(raw_assets, list):
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ASSETS_LIST_REQUIRED")
    assets: list[PreviewAssetIdentity] = []
    for item in raw_assets:
        mapping = _mapping(item, "asset")
        if set(mapping) != {"path", "media_type", "size_bytes", "sha256"}:
            raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_ASSET_FIELDS_INVALID")
        assets.append(
            PreviewAssetIdentity(
                path=_required_text(mapping, "path"),
                media_type=_required_text(mapping, "media_type"),
                size_bytes=_int_value(mapping["size_bytes"], "size_bytes"),
                sha256=_required_text(mapping, "sha256"),
            )
        )
    authorization_state = _required_text(payload, "authorization_state")
    if authorization_state not in {"NOT_AUTHORIZED", "EXACT_PREAUTHORIZED"}:
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_AUTH_STATE_INVALID")
    authorization_ref_value = payload["authorization_ref"]
    if authorization_ref_value is not None and not isinstance(authorization_ref_value, str):
        raise ControlledHttpsPreviewError("PREVIEW_MANIFEST_AUTH_REF_INVALID")
    return ControlledHttpsPreviewManifest(
        task_id=_required_text(payload, "task_id"),
        source_commit=_required_text(payload, "source_commit"),
        source_generator=_required_text(payload, "source_generator"),
        source_generator_version=_required_text(payload, "source_generator_version"),
        policy_id=_required_text(payload, "policy_id"),
        policy_version=_required_text(payload, "policy_version"),
        policy_sha256=_required_text(payload, "policy_sha256"),
        writer_version=_required_text(payload, "writer_version"),
        generated_at_utc=_parse_utc(_required_text(payload, "generated_at_utc")),
        expires_at_utc=_parse_utc(_required_text(payload, "expires_at_utc")),
        hosting_provider=_required_text(payload, "hosting_provider"),
        https_origin=_required_text(payload, "https_origin"),
        private_access_mode=_required_text(payload, "private_access_mode"),
        ttl_hours=_int_value(payload["ttl_hours"], "ttl_hours"),
        cost_ceiling_usd=_required_text(payload, "cost_ceiling_usd"),
        retention_days=_int_value(payload["retention_days"], "retention_days"),
        browser_matrix=_string_tuple(payload["browser_matrix"], "browser_matrix"),
        viewport_matrix=_string_tuple(payload["viewport_matrix"], "viewport_matrix"),
        assistive_technology_matrix=_string_tuple(
            payload["assistive_technology_matrix"], "assistive_technology_matrix"
        ),
        cleanup_authority=_required_text(payload, "cleanup_authority"),
        entrypoint=_required_text(payload, "entrypoint"),
        manifest_name=_required_text(payload, "manifest_name"),
        assets=tuple(assets),
        asset_set_sha256=_required_text(payload, "asset_set_sha256"),
        authorization_state=authorization_state,  # type: ignore[arg-type]
        authorization_ref=authorization_ref_value,
        production_effect=_required_text(payload, "production_effect"),  # type: ignore[arg-type]
        broker_action=_required_text(payload, "broker_action"),  # type: ignore[arg-type]
    )


def _asset_identity(
    root: Path, path: str, allowed_media_types: Mapping[str, str]
) -> PreviewAssetIdentity:
    payload = _read_regular_file(root, path)
    suffix = PurePosixPath(path).suffix.lower()
    media_type = allowed_media_types.get(suffix)
    if media_type is None:
        raise ControlledHttpsPreviewError(f"PREVIEW_ASSET_MEDIA_TYPE_NOT_ALLOWED:{path}")
    return PreviewAssetIdentity(
        path=path,
        media_type=media_type,
        size_bytes=len(payload),
        sha256=_sha256(payload),
    )


def _validate_local_references(root: Path, assets: Sequence[PreviewAssetIdentity]) -> None:
    allowlist = {item.path for item in assets}
    for asset in assets:
        if PurePosixPath(asset.path).suffix.lower() not in _TEXT_SUFFIXES:
            continue
        payload = _read_regular_file(root, asset.path)
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ControlledHttpsPreviewError(
                f"PREVIEW_TEXT_ASSET_UTF8_INVALID:{asset.path}"
            ) from exc
        if _NETWORK_SCHEME_RE.search(text):
            raise ControlledHttpsPreviewError(
                f"PREVIEW_EXTERNAL_REFERENCE_FORBIDDEN:{asset.path}"
            )
        if _LOCAL_NETWORK_RE.search(text):
            raise ControlledHttpsPreviewError(
                f"PREVIEW_LOCAL_NETWORK_REFERENCE_FORBIDDEN:{asset.path}"
            )
        if _DYNAMIC_NETWORK_API_RE.search(text):
            raise ControlledHttpsPreviewError(
                f"PREVIEW_DYNAMIC_NETWORK_API_FORBIDDEN:{asset.path}"
            )
        references = [match[1] for match in _HTML_REFERENCE_RE.findall(text)]
        references.extend(match[1] for match in _CSS_REFERENCE_RE.findall(text))
        for reference in references:
            _validate_reference(asset.path, reference.strip(), allowlist)


def _validate_reference(source_path: str, reference: str, allowlist: set[str]) -> None:
    if not reference or reference.startswith("#"):
        return
    parsed = urlsplit(reference)
    if parsed.scheme or parsed.netloc:
        raise ControlledHttpsPreviewError(
            f"PREVIEW_EXTERNAL_REFERENCE_FORBIDDEN:{source_path}:{reference}"
        )
    raw_path = parsed.path
    if not raw_path:
        return
    parent = PurePosixPath(source_path).parent
    combined = parent / raw_path
    normalized = _normal_relative_path(combined.as_posix())
    if normalized not in allowlist:
        raise ControlledHttpsPreviewError(
            f"PREVIEW_REFERENCE_NOT_ALLOWLISTED:{source_path}:{reference}"
        )


def _bundle_file_inventory(root: Path) -> set[str]:
    paths: set[str] = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise ControlledHttpsPreviewError(f"PREVIEW_BUNDLE_SYMLINK_FORBIDDEN:{relative}")
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ControlledHttpsPreviewError(f"PREVIEW_BUNDLE_NONREGULAR_FILE:{relative}")
        paths.add(_normal_relative_path(relative))
    return paths


def _read_regular_file(root: Path, relative_path: str) -> bytes:
    normalized = _normal_relative_path(relative_path)
    current = root
    for part in PurePosixPath(normalized).parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError as exc:
            raise ControlledHttpsPreviewError(
                f"PREVIEW_ASSET_MISSING:{normalized}"
            ) from exc
        if stat.S_ISLNK(mode):
            raise ControlledHttpsPreviewError(
                f"PREVIEW_ASSET_SYMLINK_FORBIDDEN:{normalized}"
            )
    if not stat.S_ISREG(current.lstat().st_mode):
        raise ControlledHttpsPreviewError(f"PREVIEW_ASSET_NOT_REGULAR:{normalized}")
    before = current.stat()
    payload = current.read_bytes()
    after = current.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or len(payload) != after.st_size
    ):
        raise ControlledHttpsPreviewError(f"PREVIEW_ASSET_CHANGED_DURING_READ:{normalized}")
    return payload


def _regular_directory(path: Path, code: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise ControlledHttpsPreviewError(code)
    return path.resolve()


def _exact_allowlist(paths: Sequence[str]) -> set[str]:
    normalized = {_normal_relative_path(path) for path in paths}
    if not normalized or len(normalized) != len(paths):
        raise ControlledHttpsPreviewError("PREVIEW_ALLOWLIST_INVALID")
    return normalized


def _normal_relative_path(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ControlledHttpsPreviewError("PREVIEW_RELATIVE_PATH_INVALID")
    path = PurePosixPath(value)
    if path.is_absolute() or value.startswith("/") or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise ControlledHttpsPreviewError("PREVIEW_RELATIVE_PATH_INVALID")
    normalized = path.as_posix()
    if normalized != value or re.match(r"^[A-Za-z]:", normalized):
        raise ControlledHttpsPreviewError("PREVIEW_RELATIVE_PATH_INVALID")
    return normalized


def _validated_decisions(decisions: Mapping[str, object]) -> Mapping[str, object]:
    if set(decisions) != _EXPECTED_DECISION_SLOTS:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_DECISION_SLOTS_INVALID")
    for field in ("hosting_provider", "private_access_mode", "cleanup_authority"):
        _nonempty_string(decisions[field], field)
    origin = _nonempty_string(decisions["https_origin"], "https_origin")
    if not _valid_https_origin(origin):
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_HTTPS_ORIGIN_INVALID")
    if decisions["private_access_mode"] != "PRIVATE_AUTHENTICATED":
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_PRIVATE_ACCESS_INVALID")
    _positive_int(decisions["ttl_hours"], "ttl_hours")
    _nonnegative_decimal(decisions["cost_ceiling_usd"])
    _nonnegative_int(decisions["retention_days"], "retention_days")
    _string_tuple(decisions["browser_matrix"], "browser_matrix")
    _string_tuple(decisions["viewport_matrix"], "viewport_matrix")
    _string_tuple(
        decisions["assistive_technology_matrix"], "assistive_technology_matrix"
    )
    return decisions


def _require_local_bundle_ready(
    policy: ControlledHttpsPreviewPolicy,
) -> Mapping[str, object]:
    if not policy.ready_for_local_bundle:
        raise ControlledHttpsPreviewError("PREVIEW_POLICY_OWNER_DECISION_PENDING")
    return _validated_decisions(policy.owner_decision_slots)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ControlledHttpsPreviewError(f"PREVIEW_MAPPING_REQUIRED:{field}")
    return value


def _required_text(mapping: Mapping[str, object], field: str) -> str:
    if field not in mapping:
        raise ControlledHttpsPreviewError(f"PREVIEW_FIELD_MISSING:{field}")
    return _nonempty_string(mapping[field], field)


def _nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ControlledHttpsPreviewError(f"PREVIEW_STRING_REQUIRED:{field}")
    return value.strip()


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ControlledHttpsPreviewError(f"PREVIEW_LIST_REQUIRED:{field}")
    result = tuple(_nonempty_string(item, field) for item in value)
    if not result or len(result) != len(set(result)):
        raise ControlledHttpsPreviewError(f"PREVIEW_LIST_INVALID:{field}")
    return result


def _int_value(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ControlledHttpsPreviewError(f"PREVIEW_INTEGER_REQUIRED:{field}")
    return value


def _positive_int(value: object, field: str) -> int:
    result = _int_value(value, field)
    if result <= 0:
        raise ControlledHttpsPreviewError(f"PREVIEW_POSITIVE_INTEGER_REQUIRED:{field}")
    return result


def _nonnegative_int(value: object, field: str) -> int:
    result = _int_value(value, field)
    if result < 0:
        raise ControlledHttpsPreviewError(f"PREVIEW_NONNEGATIVE_INTEGER_REQUIRED:{field}")
    return result


def _nonnegative_decimal(value: object) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ControlledHttpsPreviewError("PREVIEW_COST_CEILING_INVALID") from exc
    if not result.is_finite() or result < 0:
        raise ControlledHttpsPreviewError("PREVIEW_COST_CEILING_INVALID")
    return result


def _valid_https_origin(value: str) -> bool:
    try:
        return _normalized_https_origin(value) == value
    except ControlledHttpsPreviewError:
        return False


def _normalized_https_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or _LOCAL_NETWORK_RE.search(parsed.hostname)
    ):
        raise ControlledHttpsPreviewError("PREVIEW_HTTPS_ORIGIN_INVALID")
    port = "" if parsed.port is None else f":{parsed.port}"
    return f"https://{parsed.hostname.lower()}{port}"


def _asset_set_sha256(assets: Sequence[PreviewAssetIdentity]) -> str:
    payload = [item.to_dict() for item in assets]
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ControlledHttpsPreviewError(f"PREVIEW_TIMEZONE_REQUIRED:{field}")
    return value.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return _utc(value, "timestamp").strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ControlledHttpsPreviewError("PREVIEW_TIMESTAMP_INVALID") from exc
    return _utc(parsed, "timestamp")


__all__ = [
    "APPROVED_LOCAL_BUNDLE",
    "CONTROLLED_PREVIEW_MANIFEST_SCHEMA",
    "CONTROLLED_PREVIEW_POLICY_SCHEMA",
    "CONTROLLED_PREVIEW_REPLAY_SCHEMA",
    "CONTROLLED_PREVIEW_WRITER_VERSION",
    "ControlledHttpsPreviewError",
    "ControlledHttpsPreviewManifest",
    "ControlledHttpsPreviewPolicy",
    "DEFAULT_CONTROLLED_PREVIEW_POLICY_PATH",
    "ExternalPreviewAuthorization",
    "PENDING_OWNER_DECISION",
    "PreviewAssetIdentity",
    "PreviewReplayReceipt",
    "bind_external_preview_authorization",
    "build_controlled_preview_manifest",
    "load_controlled_https_preview_policy",
    "load_controlled_preview_manifest",
    "replay_controlled_preview_bundle",
    "validate_https_endpoint_bytes",
    "write_controlled_preview_bundle",
]
