from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

CHANGE_MANIFEST_SCHEMA_VERSION = "change_manifest.v1"
VALIDATION_EVIDENCE_SCHEMA_VERSION = "validation_evidence.v1"
LANE_PLAN_SCHEMA_VERSION = "lane_plan.v1"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ParallelControlError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class LaneRole(StrEnum):
    DOMAIN = "DOMAIN"
    COORDINATOR = "COORDINATOR"


class ContractAccess(StrEnum):
    READ = "READ"
    WRITE = "WRITE"


@dataclass(frozen=True, order=True)
class ContractClaim:
    contract_id: str
    version: str
    access: ContractAccess

    def to_dict(self) -> dict[str, str]:
        return {
            "contract_id": self.contract_id,
            "version": self.version,
            "access": self.access.value,
        }


@dataclass(frozen=True)
class ChangeManifest:
    change_id: str
    task_id: str
    lane_role: LaneRole
    base_commit: str
    owner: str
    production_effect: str
    owned_paths: tuple[str, ...]
    shared_paths: tuple[str, ...]
    module_ids: tuple[str, ...]
    contract_claims: tuple[ContractClaim, ...]
    required_validation_tiers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": CHANGE_MANIFEST_SCHEMA_VERSION,
            "change_id": self.change_id,
            "task_id": self.task_id,
            "lane_role": self.lane_role.value,
            "base_commit": self.base_commit,
            "owner": self.owner,
            "production_effect": self.production_effect,
            "owned_paths": list(self.owned_paths),
            "shared_paths": list(self.shared_paths),
            "module_ids": list(self.module_ids),
            "contract_claims": [claim.to_dict() for claim in self.contract_claims],
            "required_validation_tiers": list(self.required_validation_tiers),
        }

    @property
    def sha256(self) -> str:
        return _canonical_sha256(self.to_dict())


@dataclass(frozen=True)
class ValidationEvidence:
    evidence_id: str
    change_id: str
    tier: str
    status: str
    artifact_path: str
    artifact_sha256: str
    base_commit: str
    change_manifest_sha256: str
    production_effect: str

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": VALIDATION_EVIDENCE_SCHEMA_VERSION,
            "evidence_id": self.evidence_id,
            "change_id": self.change_id,
            "tier": self.tier,
            "status": self.status,
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "base_commit": self.base_commit,
            "change_manifest_sha256": self.change_manifest_sha256,
            "production_effect": self.production_effect,
        }


@dataclass(frozen=True, order=True)
class ChangeConflict:
    code: str
    change_ids: tuple[str, str]
    resource: str
    serializable: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "change_ids": list(self.change_ids),
            "resource": self.resource,
            "serializable": self.serializable,
        }


@dataclass(frozen=True, order=True)
class ControlIssue:
    code: str
    change_ids: tuple[str, ...]
    resource: str
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "change_ids": list(self.change_ids),
            "resource": self.resource,
            "message": self.message,
        }


@dataclass(frozen=True)
class LanePlan:
    status: str
    current_base_commit: str
    max_parallel_domain_lanes: int
    manifest_sha256: tuple[tuple[str, str], ...]
    waves: tuple[dict[str, object], ...]
    conflicts: tuple[ChangeConflict, ...]
    blocking_issues: tuple[ControlIssue, ...]

    def _body(self) -> dict[str, object]:
        return {
            "schema_version": LANE_PLAN_SCHEMA_VERSION,
            "status": self.status,
            "current_base_commit": self.current_base_commit,
            "max_parallel_domain_lanes": self.max_parallel_domain_lanes,
            "manifest_sha256": [
                {"change_id": change_id, "sha256": digest}
                for change_id, digest in self.manifest_sha256
            ],
            "waves": list(self.waves),
            "conflicts": [conflict.to_dict() for conflict in self.conflicts],
            "blocking_issues": [issue.to_dict() for issue in self.blocking_issues],
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_registry_mutated": False,
            "production_effect": "none",
        }

    @property
    def plan_id(self) -> str:
        return f"lane-plan-{_canonical_sha256(self._body())[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": self.plan_id, **self._body()}


@dataclass(frozen=True)
class EvidenceBindingResult:
    status: str
    change_id: str
    change_manifest_sha256: str
    required_tiers: tuple[str, ...]
    bound_tiers: tuple[str, ...]
    issues: tuple[ControlIssue, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "validation_evidence_binding.v1",
            "status": self.status,
            "change_id": self.change_id,
            "change_manifest_sha256": self.change_manifest_sha256,
            "required_tiers": list(self.required_tiers),
            "bound_tiers": list(self.bound_tiers),
            "issues": [issue.to_dict() for issue in self.issues],
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_registry_mutated": False,
            "production_effect": "none",
        }


def parse_change_manifest(payload: Mapping[str, Any]) -> ChangeManifest:
    expected = {
        "schema_version",
        "change_id",
        "task_id",
        "lane_role",
        "base_commit",
        "owner",
        "production_effect",
        "owned_paths",
        "shared_paths",
        "module_ids",
        "contract_claims",
        "required_validation_tiers",
    }
    _require_exact_keys(payload, expected, "CHANGE_MANIFEST_FIELDS")
    if payload["schema_version"] != CHANGE_MANIFEST_SCHEMA_VERSION:
        raise ParallelControlError(
            "CHANGE_MANIFEST_SCHEMA",
            f"expected {CHANGE_MANIFEST_SCHEMA_VERSION}",
        )
    change_id = _identifier(payload["change_id"], "change_id")
    task_id = _identifier(payload["task_id"], "task_id")
    owner = _required_text(payload["owner"], "owner")
    base_commit = _commit(payload["base_commit"], "base_commit")
    try:
        lane_role = LaneRole(str(payload["lane_role"]))
    except ValueError as exc:
        raise ParallelControlError("LANE_ROLE", "lane_role must be DOMAIN or COORDINATOR") from exc
    if payload["production_effect"] != "none":
        raise ParallelControlError(
            "UNSAFE_PRODUCTION_EFFECT",
            "pre-bootstrap change manifests require production_effect=none",
        )
    owned_paths = _unique_sorted_paths(payload["owned_paths"], "owned_paths")
    shared_paths = _unique_sorted_paths(payload["shared_paths"], "shared_paths")
    overlap = sorted(set(owned_paths) & set(shared_paths))
    if overlap:
        raise ParallelControlError(
            "INTRA_MANIFEST_PATH_OVERLAP",
            f"paths cannot be both owned and shared: {overlap}",
        )
    if not owned_paths and not shared_paths:
        raise ParallelControlError("EMPTY_CHANGE_SCOPE", "at least one changed path is required")
    module_ids = _unique_sorted_identifiers(payload["module_ids"], "module_ids")
    claims = _contract_claims(payload["contract_claims"])
    tiers = _unique_sorted_identifiers(
        payload["required_validation_tiers"],
        "required_validation_tiers",
    )
    if not tiers:
        raise ParallelControlError(
            "MISSING_VALIDATION_REQUIREMENTS",
            "required_validation_tiers cannot be empty",
        )
    return ChangeManifest(
        change_id=change_id,
        task_id=task_id,
        lane_role=lane_role,
        base_commit=base_commit,
        owner=owner,
        production_effect="none",
        owned_paths=owned_paths,
        shared_paths=shared_paths,
        module_ids=module_ids,
        contract_claims=claims,
        required_validation_tiers=tiers,
    )


def parse_validation_evidence(payload: Mapping[str, Any]) -> ValidationEvidence:
    expected = {
        "schema_version",
        "evidence_id",
        "change_id",
        "tier",
        "status",
        "artifact_path",
        "artifact_sha256",
        "base_commit",
        "change_manifest_sha256",
        "production_effect",
    }
    _require_exact_keys(payload, expected, "VALIDATION_EVIDENCE_FIELDS")
    if payload["schema_version"] != VALIDATION_EVIDENCE_SCHEMA_VERSION:
        raise ParallelControlError(
            "VALIDATION_EVIDENCE_SCHEMA",
            f"expected {VALIDATION_EVIDENCE_SCHEMA_VERSION}",
        )
    status = str(payload["status"])
    if status not in {"PASS", "FAIL"}:
        raise ParallelControlError("VALIDATION_EVIDENCE_STATUS", "status must be PASS or FAIL")
    if payload["production_effect"] != "none":
        raise ParallelControlError(
            "UNSAFE_PRODUCTION_EFFECT",
            "pre-bootstrap validation evidence requires production_effect=none",
        )
    return ValidationEvidence(
        evidence_id=_identifier(payload["evidence_id"], "evidence_id"),
        change_id=_identifier(payload["change_id"], "change_id"),
        tier=_identifier(payload["tier"], "tier"),
        status=status,
        artifact_path=_portable_path(payload["artifact_path"], "artifact_path"),
        artifact_sha256=_sha256(payload["artifact_sha256"], "artifact_sha256"),
        base_commit=_commit(payload["base_commit"], "base_commit"),
        change_manifest_sha256=_sha256(
            payload["change_manifest_sha256"],
            "change_manifest_sha256",
        ),
        production_effect="none",
    )


def detect_change_conflicts(manifests: Sequence[ChangeManifest]) -> tuple[ChangeConflict, ...]:
    conflicts: set[ChangeConflict] = set()
    ordered = sorted(manifests, key=lambda manifest: manifest.change_id)
    for index, first in enumerate(ordered):
        for second in ordered[index + 1 :]:
            pair = (first.change_id, second.change_id)
            for path in sorted(set(first.owned_paths) & set(second.owned_paths)):
                conflicts.add(ChangeConflict("OWNED_PATH_OVERLAP", pair, path, True))
            owned_shared = (set(first.owned_paths) & set(second.shared_paths)) | (
                set(first.shared_paths) & set(second.owned_paths)
            )
            for path in sorted(owned_shared):
                conflicts.add(ChangeConflict("OWNED_SHARED_PATH_OVERLAP", pair, path, False))
            for path in sorted(set(first.shared_paths) & set(second.shared_paths)):
                conflicts.add(ChangeConflict("SHARED_PATH_OVERLAP", pair, path, False))
            for module_id in sorted(set(first.module_ids) & set(second.module_ids)):
                conflicts.add(ChangeConflict("MODULE_CONFLICT", pair, module_id, True))
            first_claims = {claim.contract_id: claim for claim in first.contract_claims}
            second_claims = {claim.contract_id: claim for claim in second.contract_claims}
            for contract_id in sorted(set(first_claims) & set(second_claims)):
                left = first_claims[contract_id]
                right = second_claims[contract_id]
                if left.version != right.version:
                    conflicts.add(
                        ChangeConflict("CONTRACT_VERSION_CONFLICT", pair, contract_id, False)
                    )
                elif ContractAccess.WRITE in {left.access, right.access}:
                    conflicts.add(
                        ChangeConflict("CONTRACT_ACCESS_CONFLICT", pair, contract_id, True)
                    )
    return tuple(sorted(conflicts))


def build_deterministic_lane_plan(
    manifests: Sequence[ChangeManifest],
    *,
    current_base_commit: str,
    coordinator_only_paths: Sequence[str],
    max_parallel_domain_lanes: int,
) -> LanePlan:
    current_base = _commit(current_base_commit, "current_base_commit")
    if (
        isinstance(max_parallel_domain_lanes, bool)
        or not isinstance(max_parallel_domain_lanes, int)
        or max_parallel_domain_lanes < 1
    ):
        raise ParallelControlError(
            "LANE_CAPACITY",
            "max_parallel_domain_lanes must be a positive integer",
        )
    coordinator_paths = set(
        _unique_sorted_paths(coordinator_only_paths, "coordinator_only_paths")
    )
    ordered = sorted(manifests, key=lambda manifest: manifest.change_id)
    issues: set[ControlIssue] = set()
    if not ordered:
        issues.add(
            ControlIssue(
                "EMPTY_LANE_PLAN",
                (),
                "change_manifests",
                "at least one change manifest is required",
            )
        )
    seen_ids: set[str] = set()
    for manifest in ordered:
        if manifest.change_id in seen_ids:
            issues.add(
                ControlIssue(
                    "DUPLICATE_CHANGE_ID",
                    (manifest.change_id,),
                    manifest.change_id,
                    "change_id must be unique in a lane plan",
                )
            )
        seen_ids.add(manifest.change_id)
        if manifest.base_commit != current_base:
            issues.add(
                ControlIssue(
                    "BASE_DRIFT",
                    (manifest.change_id,),
                    manifest.base_commit,
                    f"expected current base {current_base}",
                )
            )
        if manifest.lane_role is LaneRole.DOMAIN and manifest.shared_paths:
            issues.add(
                ControlIssue(
                    "DOMAIN_SHARED_PATH_CLAIM",
                    (manifest.change_id,),
                    ",".join(manifest.shared_paths),
                    "domain lanes cannot edit shared paths",
                )
            )
        for path in manifest.owned_paths:
            if path in coordinator_paths:
                issues.add(
                    ControlIssue(
                        "COORDINATOR_ONLY_PATH_VIOLATION",
                        (manifest.change_id,),
                        path,
                        "coordinator-only paths must be declared as shared by the coordinator",
                    )
                )
        for path in manifest.shared_paths:
            if manifest.lane_role is not LaneRole.COORDINATOR or path not in coordinator_paths:
                issues.add(
                    ControlIssue(
                        "UNKNOWN_OR_UNAUTHORIZED_SHARED_PATH",
                        (manifest.change_id,),
                        path,
                        "shared paths require a coordinator manifest and reviewed policy entry",
                    )
                )
    coordinators = [manifest for manifest in ordered if manifest.lane_role is LaneRole.COORDINATOR]
    if len(coordinators) > 1:
        issues.add(
            ControlIssue(
                "MULTIPLE_COORDINATORS",
                tuple(manifest.change_id for manifest in coordinators),
                "integration-coordinator",
                "a pre-bootstrap plan accepts at most one coordinator manifest",
            )
        )
    conflicts = detect_change_conflicts(ordered)
    for conflict in conflicts:
        if not conflict.serializable:
            issues.add(
                ControlIssue(
                    conflict.code,
                    conflict.change_ids,
                    conflict.resource,
                    "conflict cannot be made safe by lane serialization",
                )
            )
    manifest_hashes = tuple((manifest.change_id, manifest.sha256) for manifest in ordered)
    if issues:
        return LanePlan(
            status="BLOCKED",
            current_base_commit=current_base,
            max_parallel_domain_lanes=max_parallel_domain_lanes,
            manifest_sha256=manifest_hashes,
            waves=(),
            conflicts=conflicts,
            blocking_issues=tuple(sorted(issues)),
        )
    domain = [manifest for manifest in ordered if manifest.lane_role is LaneRole.DOMAIN]
    serial_edges = {
        frozenset(conflict.change_ids)
        for conflict in conflicts
        if conflict.serializable
    }
    pending = list(domain)
    waves: list[dict[str, object]] = []
    while pending:
        selected: list[ChangeManifest] = []
        for manifest in pending:
            if len(selected) >= max_parallel_domain_lanes:
                break
            if any(
                frozenset((manifest.change_id, existing.change_id)) in serial_edges
                for existing in selected
            ):
                continue
            selected.append(manifest)
        selected_ids = {manifest.change_id for manifest in selected}
        pending = [manifest for manifest in pending if manifest.change_id not in selected_ids]
        waves.append(
            {
                "wave_id": f"domain-wave-{len(waves) + 1:03d}",
                "kind": "DOMAIN",
                "assignments": [
                    {
                        "lane_id": f"domain-{index:02d}",
                        "change_id": manifest.change_id,
                        "manifest_sha256": manifest.sha256,
                    }
                    for index, manifest in enumerate(selected, start=1)
                ],
            }
        )
    if coordinators:
        coordinator = coordinators[0]
        waves.append(
            {
                "wave_id": f"integration-wave-{len(waves) + 1:03d}",
                "kind": "COORDINATOR",
                "assignments": [
                    {
                        "lane_id": "integration-coordinator",
                        "change_id": coordinator.change_id,
                        "manifest_sha256": coordinator.sha256,
                    }
                ],
            }
        )
    return LanePlan(
        status="PASS",
        current_base_commit=current_base,
        max_parallel_domain_lanes=max_parallel_domain_lanes,
        manifest_sha256=manifest_hashes,
        waves=tuple(waves),
        conflicts=conflicts,
        blocking_issues=(),
    )


def validate_evidence_binding(
    manifest: ChangeManifest,
    evidence: Sequence[ValidationEvidence],
    *,
    project_root: Path,
) -> EvidenceBindingResult:
    root = project_root.resolve()
    issues: set[ControlIssue] = set()
    by_tier: dict[str, ValidationEvidence] = {}
    seen_evidence_ids: set[str] = set()
    required = set(manifest.required_validation_tiers)
    for record in sorted(evidence, key=lambda item: (item.tier, item.evidence_id)):
        if record.evidence_id in seen_evidence_ids:
            issues.add(
                ControlIssue(
                    "DUPLICATE_EVIDENCE_ID",
                    (manifest.change_id,),
                    record.evidence_id,
                    "evidence_id must be unique",
                )
            )
        seen_evidence_ids.add(record.evidence_id)
        if record.tier in by_tier:
            issues.add(
                ControlIssue(
                    "DUPLICATE_VALIDATION_TIER",
                    (manifest.change_id,),
                    record.tier,
                    "one evidence record is allowed per required tier",
                )
            )
        else:
            by_tier[record.tier] = record
        if record.tier not in required:
            issues.add(
                ControlIssue(
                    "UNDECLARED_VALIDATION_TIER",
                    (manifest.change_id,),
                    record.tier,
                    "evidence tier was not declared by the change manifest",
                )
            )
        if record.change_id != manifest.change_id:
            issues.add(_binding_issue(manifest, "CHANGE_ID_BINDING", record.change_id))
        if record.base_commit != manifest.base_commit:
            issues.add(_binding_issue(manifest, "BASE_COMMIT_BINDING", record.base_commit))
        if record.change_manifest_sha256 != manifest.sha256:
            issues.add(
                _binding_issue(
                    manifest,
                    "CHANGE_MANIFEST_SHA256_BINDING",
                    record.change_manifest_sha256,
                )
            )
        if record.status != "PASS":
            issues.add(_binding_issue(manifest, "VALIDATION_STATUS", record.tier))
        candidate = (root / record.artifact_path).resolve()
        if not candidate.is_relative_to(root):
            issues.add(_binding_issue(manifest, "ARTIFACT_CONTAINMENT", record.artifact_path))
        elif not candidate.is_file():
            issues.add(_binding_issue(manifest, "ARTIFACT_MISSING", record.artifact_path))
        else:
            actual = hashlib.sha256(candidate.read_bytes()).hexdigest()
            if actual != record.artifact_sha256:
                issues.add(_binding_issue(manifest, "ARTIFACT_SHA256", record.artifact_path))
    for tier in sorted(required - set(by_tier)):
        issues.add(
            ControlIssue(
                "MISSING_REQUIRED_VALIDATION_TIER",
                (manifest.change_id,),
                tier,
                "required validation evidence is missing",
            )
        )
    bound = tuple(
        sorted(
            tier
            for tier, record in by_tier.items()
            if tier in required
            and record.status == "PASS"
            and record.change_id == manifest.change_id
            and record.base_commit == manifest.base_commit
            and record.change_manifest_sha256 == manifest.sha256
        )
    )
    ordered_issues = tuple(sorted(issues))
    return EvidenceBindingResult(
        status="PASS" if not ordered_issues else "FAIL",
        change_id=manifest.change_id,
        change_manifest_sha256=manifest.sha256,
        required_tiers=manifest.required_validation_tiers,
        bound_tiers=bound if not ordered_issues else (),
        issues=ordered_issues,
    )


def _binding_issue(manifest: ChangeManifest, code: str, resource: str) -> ControlIssue:
    return ControlIssue(
        code,
        (manifest.change_id,),
        resource,
        "validation evidence does not match the declared change",
    )


def _contract_claims(value: object) -> tuple[ContractClaim, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("CONTRACT_CLAIMS_TYPE", "contract_claims must be a list")
    claims: list[ContractClaim] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, Mapping):
            raise ParallelControlError("CONTRACT_CLAIM_TYPE", "contract claims must be mappings")
        _require_exact_keys(raw, {"contract_id", "version", "access"}, "CONTRACT_CLAIM_FIELDS")
        contract_id = _identifier(raw["contract_id"], "contract_id")
        if contract_id in seen:
            raise ParallelControlError(
                "DUPLICATE_CONTRACT_CLAIM",
                f"duplicate contract claim: {contract_id}",
            )
        seen.add(contract_id)
        try:
            access = ContractAccess(str(raw["access"]))
        except ValueError as exc:
            raise ParallelControlError(
                "CONTRACT_ACCESS",
                "contract access must be READ or WRITE",
            ) from exc
        claims.append(
            ContractClaim(
                contract_id=contract_id,
                version=_identifier(raw["version"], "contract version"),
                access=access,
            )
        )
    return tuple(sorted(claims))


def _unique_sorted_paths(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("PATH_LIST_TYPE", f"{field} must be a list")
    paths = [_portable_path(item, field) for item in value]
    if len(paths) != len(set(paths)):
        raise ParallelControlError("DUPLICATE_PATH", f"{field} contains duplicate paths")
    return tuple(sorted(paths))


def _portable_path(value: object, field: str) -> str:
    text = _required_text(value, field)
    if "\\" in text or text.endswith("/"):
        raise ParallelControlError("NON_CANONICAL_PATH", f"{field} must use canonical POSIX syntax")
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or ":" in path.parts[0]
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ParallelControlError("UNSAFE_PATH", f"{field} must be repository relative")
    if str(path) != text:
        raise ParallelControlError(
            "NON_CANONICAL_PATH",
            f"{field} must not be normalized implicitly",
        )
    return text


def _unique_sorted_identifiers(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("IDENTIFIER_LIST_TYPE", f"{field} must be a list")
    identifiers = [_identifier(item, field) for item in value]
    if len(identifiers) != len(set(identifiers)):
        raise ParallelControlError("DUPLICATE_IDENTIFIER", f"{field} contains duplicates")
    return tuple(sorted(identifiers))


def _identifier(value: object, field: str) -> str:
    text = _required_text(value, field)
    if _IDENTIFIER_RE.fullmatch(text) is None:
        raise ParallelControlError("INVALID_IDENTIFIER", f"invalid {field}: {text}")
    return text


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ParallelControlError("REQUIRED_TEXT", f"{field} must be non-empty trimmed text")
    return value


def _commit(value: object, field: str) -> str:
    text = _required_text(value, field)
    if _COMMIT_RE.fullmatch(text) is None:
        raise ParallelControlError("INVALID_COMMIT", f"{field} must be a lowercase 40-hex commit")
    return text


def _sha256(value: object, field: str) -> str:
    text = _required_text(value, field)
    if _SHA256_RE.fullmatch(text) is None:
        raise ParallelControlError("INVALID_SHA256", f"{field} must be lowercase SHA-256")
    return text


def _require_exact_keys(payload: Mapping[str, Any], expected: set[str], code: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise ParallelControlError(
            code,
            f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}",
        )


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
