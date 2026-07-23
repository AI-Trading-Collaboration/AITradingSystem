from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import re
import subprocess
import tarfile
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from yaml.nodes import MappingNode

from ai_trading_system.platform.architecture.bootstrap_handoff import (
    REQUIRED_VALIDATION_TIERS,
    BootstrapHandoffError,
    validate_bootstrap_handoff,
)
from ai_trading_system.platform.architecture.devex import (
    build_module_manifest,
    build_test_manifest,
)
from ai_trading_system.platform.architecture.parallel_control import (
    ChangeManifest,
    LaneRole,
    ParallelControlError,
    build_deterministic_lane_plan,
    detect_change_conflicts,
    parse_change_manifest,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    ParallelControlPolicy,
    TaskControlRecord,
    load_parallel_control_policy,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    PilotSpec,
    build_shadow_scheduler_decision,
)
from ai_trading_system.platform.architecture.wave_readiness import (
    WaveReadinessError,
    extract_validated_archive,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes, write_json_atomic

POLICY_SCHEMA_VERSION = "arch_004_g2_5_readiness_policy.v1"
OWNERSHIP_SNAPSHOT_SCHEMA_VERSION = "arch_004_g2_5_ownership_snapshot.v1"
FRAGMENT_PREVIEW_SCHEMA_VERSION = "arch_004_g2_5_fragment_preview.v1"
REHEARSAL_SCHEMA_VERSION = "arch_004_g2_5_readiness_rehearsal.v1"
VALIDATION_BUNDLE_SCHEMA_VERSION = "arch_005_bootstrap_validation_bundle.v1"
DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_POLICY_PATH = Path("config/architecture/arch_004_g2_5_readiness.yaml")
DEFAULT_EVIDENCE_PATH = Path("inputs/architecture/arch_004g2_5_parallel_readiness.json")
GENERATOR_SOURCE_PATH = Path(
    "src/ai_trading_system/platform/architecture/arch_004_g2_5_readiness.py"
)
_CARRIER_SNAPSHOT_PATHS = (
    "config",
    "docs/artifact_catalog.md",
    "docs/system_flow.md",
    "inputs/architecture",
    "src/ai_trading_system",
    "tests",
)
DEFAULT_HANDOFF_PATH = Path("inputs/architecture/arch_005_bootstrap_handoff.yaml")
DEFAULT_VALIDATION_BUNDLE_PATH = Path(
    "inputs/architecture/arch_005_bootstrap_validation_bundle.json"
)
CANONICAL_MERGE_ORDER = (
    "contract",
    "adapter",
    "domain_migration",
    "tests_and_fragments",
    "shared_wiring_and_docs",
    "generated_aggregate",
    "compatibility_removal",
)
EXPECTED_DOMAIN_IDS = {"G3_REPORTING", "G4_OPERATIONS", "G5_RESEARCH_WRAPPER"}
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DOMAIN_KEYS = {
    "domain_id",
    "task_id",
    "change_id",
    "owner",
    "priority",
    "batch",
    "sequence",
    "owned_paths",
    "shared_paths_requested",
    "module_ids",
    "contract_claims",
    "required_validation_tiers",
    "removal_targets",
    "source_inventory",
    "fragments",
}
_SAFETY_FALSE_FIELDS = {
    "task_registry_mutation_allowed",
    "aggregate_source_of_truth_write_allowed",
    "dispatch_allowed",
    "lease_acquisition_allowed",
    "automatic_merge_allowed",
    "strategy_logic_change_allowed",
    "strategy_threshold_change_allowed",
    "data_quality_change_allowed",
    "pit_change_allowed",
    "backtest_change_allowed",
    "paper_shadow_change_allowed",
}


class G25ReadinessError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)  # type: ignore[no-untyped-call]
        try:
            duplicate = key in result
        except TypeError as exc:
            raise G25ReadinessError(
                "YAML_UNHASHABLE_KEY", f"line={key_node.start_mark.line + 1}"
            ) from exc
        if duplicate:
            raise G25ReadinessError(
                "YAML_DUPLICATE_KEY",
                f"key={key!r} line={key_node.start_mark.line + 1}",
            )
        result[key] = loader.construct_object(  # type: ignore[no-untyped-call]
            value_node, deep=deep
        )
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class G25ReadinessPolicy:
    gate_id: str
    status: str
    owner: str
    owner_resume_ref: str
    source_phase: str
    source_handoff_path: str
    source_validation_bundle_path: str
    source_validation_bundle_sha256: str
    migration_matrix_path: str
    parallel_control_policy_path: str
    ownership_policy_path: str
    max_active_domain_workers: int
    reviewed_domain_owners: tuple[str, ...]
    gate_requirement_refs: tuple[str, ...]
    gate_acceptance_criteria: tuple[str, ...]
    coordinator: Mapping[str, Any]
    aggregate_targets: tuple[Mapping[str, Any], ...]
    merge_order: tuple[str, ...]
    domains: tuple[Mapping[str, Any], ...]
    safety: Mapping[str, Any]
    path: Path
    sha256: str

    def domain_by_change_id(self) -> dict[str, Mapping[str, Any]]:
        return {_text(row["change_id"], "change_id"): row for row in self.domains}


def load_g2_5_readiness_policy(path: Path = DEFAULT_POLICY_PATH) -> G25ReadinessPolicy:
    raw = _mapping(_load_unique_yaml_path(path, "policy"), "policy")
    _exact(
        raw,
        {
            "schema_version",
            "gate_id",
            "status",
            "owner",
            "owner_resume_ref",
            "source_phase",
            "source_handoff_path",
            "source_validation_bundle_path",
            "source_validation_bundle_sha256",
            "migration_matrix_path",
            "parallel_control_policy_path",
            "ownership_policy_path",
            "max_active_domain_workers",
            "reviewed_domain_owners",
            "gate_requirement_refs",
            "gate_acceptance_criteria",
            "coordinator",
            "aggregate_targets",
            "merge_order",
            "domains",
            "safety",
        },
        "POLICY_FIELDS",
    )
    if raw["schema_version"] != POLICY_SCHEMA_VERSION:
        raise G25ReadinessError("POLICY_SCHEMA", str(raw["schema_version"]))
    domains = tuple(
        sorted(_maps(raw["domains"], "domains"), key=lambda row: _integer(row["sequence"]))
    )
    policy = G25ReadinessPolicy(
        gate_id=_text(raw["gate_id"], "gate_id"),
        status=_text(raw["status"], "status"),
        owner=_text(raw["owner"], "owner"),
        owner_resume_ref=_text(raw["owner_resume_ref"], "owner_resume_ref"),
        source_phase=_text(raw["source_phase"], "source_phase"),
        source_handoff_path=_portable(raw["source_handoff_path"], "source_handoff_path"),
        source_validation_bundle_path=_portable(
            raw["source_validation_bundle_path"], "source_validation_bundle_path"
        ),
        source_validation_bundle_sha256=_sha256_text(
            raw["source_validation_bundle_sha256"], "source_validation_bundle_sha256"
        ),
        migration_matrix_path=_portable(raw["migration_matrix_path"], "migration_matrix_path"),
        parallel_control_policy_path=_portable(
            raw["parallel_control_policy_path"], "parallel_control_policy_path"
        ),
        ownership_policy_path=_portable(raw["ownership_policy_path"], "ownership_policy_path"),
        max_active_domain_workers=_integer(raw["max_active_domain_workers"]),
        reviewed_domain_owners=_strings(raw["reviewed_domain_owners"]),
        gate_requirement_refs=_strings(raw["gate_requirement_refs"]),
        gate_acceptance_criteria=_strings(raw["gate_acceptance_criteria"]),
        coordinator=_mapping(raw["coordinator"], "coordinator"),
        aggregate_targets=_maps(raw["aggregate_targets"], "aggregate_targets"),
        merge_order=_strings(raw["merge_order"], preserve_order=True),
        domains=domains,
        safety=_mapping(raw["safety"], "safety"),
        path=path,
        sha256=_sha256_path(path),
    )
    _validate_policy(policy)
    return policy


def write_source_validation_bundle(
    *,
    project_root: Path,
    handoff_path: Path,
    output_path: Path,
) -> Path:
    root = project_root.resolve()
    resolved_handoff = handoff_path if handoff_path.is_absolute() else root / handoff_path
    handoff = _mapping(_load_unique_yaml_path(resolved_handoff, "handoff"), "handoff")
    tiers = _mapping(handoff.get("validation_artifacts"), "handoff.validation_artifacts")
    if set(tiers) != set(REQUIRED_VALIDATION_TIERS):
        raise G25ReadinessError("VALIDATION_BUNDLE_TIER_SET", str(sorted(tiers)))
    rows: list[dict[str, str]] = []
    for tier in REQUIRED_VALIDATION_TIERS:
        record = _mapping(tiers[tier], f"handoff.validation_artifacts.{tier}")
        _exact(
            record,
            {"tier", "status", "artifact_path", "artifact_sha256"},
            "VALIDATION_BUNDLE_HANDOFF_FIELDS",
        )
        original_path = _portable(record["artifact_path"], "artifact_path")
        content = _repo_file(root, original_path).read_bytes()
        expected_sha = _sha256_text(record["artifact_sha256"], "artifact_sha256")
        actual_sha = hashlib.sha256(content).hexdigest()
        if actual_sha != expected_sha:
            raise G25ReadinessError(
                "VALIDATION_BUNDLE_SOURCE_HASH_DRIFT",
                f"{original_path}:{expected_sha}->{actual_sha}",
            )
        rows.append(
            {
                "tier": tier,
                "original_path": original_path,
                "sha256": actual_sha,
                "content_base64": base64.b64encode(content).decode("ascii"),
            }
        )
    payload: dict[str, object] = {
        "schema_version": VALIDATION_BUNDLE_SCHEMA_VERSION,
        "source_handoff_path": _relative(resolved_handoff, root),
        "source_handoff_sha256": _sha256_path(resolved_handoff),
        "source_handoff_checksum": handoff["handoff_checksum"],
        "artifact_count": len(rows),
        "artifacts": rows,
        "production_effect": "none",
        "broker_action": "none",
    }
    resolved_output = output_path if output_path.is_absolute() else root / output_path
    write_json_atomic(resolved_output, payload, sort_keys=True)
    return resolved_output


def load_source_validation_bundle(
    *,
    path: Path,
    expected_sha256: str,
    handoff: Mapping[str, Any],
    handoff_path: Path,
    project_root: Path,
) -> dict[str, bytes]:
    root = project_root.resolve()
    expected_bundle_sha = _sha256_text(expected_sha256, "source_validation_bundle_sha256")
    actual_bundle_sha = _sha256_path(path)
    if actual_bundle_sha != expected_bundle_sha:
        raise G25ReadinessError(
            "VALIDATION_BUNDLE_FILE_HASH_DRIFT",
            f"expected={expected_bundle_sha} actual={actual_bundle_sha}",
        )
    payload = _mapping(_load_unique_json_path(path, "validation_bundle"), "validation_bundle")
    _exact(
        payload,
        {
            "schema_version",
            "source_handoff_path",
            "source_handoff_sha256",
            "source_handoff_checksum",
            "artifact_count",
            "artifacts",
            "production_effect",
            "broker_action",
        },
        "VALIDATION_BUNDLE_FIELDS",
    )
    if payload["schema_version"] != VALIDATION_BUNDLE_SCHEMA_VERSION:
        raise G25ReadinessError("VALIDATION_BUNDLE_SCHEMA", str(payload["schema_version"]))
    expected_handoff_path = _relative(handoff_path, root)
    if (
        payload["source_handoff_path"] != expected_handoff_path
        or payload["source_handoff_sha256"] != _sha256_path(handoff_path)
        or payload["source_handoff_checksum"] != handoff.get("handoff_checksum")
    ):
        raise G25ReadinessError("VALIDATION_BUNDLE_HANDOFF_DRIFT", expected_handoff_path)
    if payload["production_effect"] != "none" or payload["broker_action"] != "none":
        raise G25ReadinessError("VALIDATION_BUNDLE_UNSAFE_EFFECT", str(path))
    rows = _maps(payload["artifacts"], "validation_bundle.artifacts")
    if payload["artifact_count"] != len(rows):
        raise G25ReadinessError("VALIDATION_BUNDLE_COUNT_DRIFT", str(payload["artifact_count"]))
    handoff_tiers = _mapping(handoff.get("validation_artifacts"), "handoff.validation_artifacts")
    if set(handoff_tiers) != set(REQUIRED_VALIDATION_TIERS):
        raise G25ReadinessError("VALIDATION_BUNDLE_HANDOFF_TIER_SET", str(sorted(handoff_tiers)))
    decoded: dict[str, bytes] = {}
    seen_tiers: set[str] = set()
    for row in rows:
        _exact(
            row,
            {"tier", "original_path", "sha256", "content_base64"},
            "VALIDATION_BUNDLE_ARTIFACT_FIELDS",
        )
        tier = _text(row["tier"], "bundle.tier")
        if tier in seen_tiers:
            raise G25ReadinessError("VALIDATION_BUNDLE_DUPLICATE_TIER", tier)
        seen_tiers.add(tier)
        if tier not in REQUIRED_VALIDATION_TIERS:
            raise G25ReadinessError("VALIDATION_BUNDLE_UNKNOWN_TIER", tier)
        source = _mapping(handoff_tiers[tier], f"handoff.validation_artifacts.{tier}")
        original_path = _portable(row["original_path"], "bundle.original_path")
        if original_path in decoded:
            raise G25ReadinessError("VALIDATION_BUNDLE_DUPLICATE_PATH", original_path)
        if original_path != source.get("artifact_path"):
            raise G25ReadinessError("VALIDATION_BUNDLE_PATH_DRIFT", tier)
        expected_content_sha = _sha256_text(row["sha256"], "bundle.sha256")
        if expected_content_sha != source.get("artifact_sha256"):
            raise G25ReadinessError("VALIDATION_BUNDLE_HANDOFF_HASH_DRIFT", tier)
        try:
            content = base64.b64decode(
                _text(row["content_base64"], "bundle.content_base64"), validate=True
            )
        except (binascii.Error, ValueError) as exc:
            raise G25ReadinessError("VALIDATION_BUNDLE_BASE64", tier) from exc
        actual_content_sha = hashlib.sha256(content).hexdigest()
        if actual_content_sha != expected_content_sha:
            raise G25ReadinessError(
                "VALIDATION_BUNDLE_CONTENT_HASH_DRIFT",
                f"{tier}:{expected_content_sha}->{actual_content_sha}",
            )
        decoded[original_path] = content
    if seen_tiers != set(REQUIRED_VALIDATION_TIERS):
        raise G25ReadinessError(
            "VALIDATION_BUNDLE_TIER_SET",
            f"expected={list(REQUIRED_VALIDATION_TIERS)} actual={sorted(seen_tiers)}",
        )
    return dict(sorted(decoded.items()))


def policy_fragments(policy: G25ReadinessPolicy) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []
    for domain in policy.domains:
        owner = _text(domain["owner"], "owner")
        for raw in _maps(domain["fragments"], "fragments"):
            _exact(raw, {"fragment_id", "fragment_kind", "target_id", "path"}, "FRAGMENT_FIELDS")
            rows.append(
                {
                    "fragment_id": _text(raw["fragment_id"], "fragment_id"),
                    "fragment_kind": _text(raw["fragment_kind"], "fragment_kind"),
                    "target_id": _text(raw["target_id"], "target_id"),
                    "path": _portable(raw["path"], "fragment.path"),
                    "owner": owner,
                    "production_effect": "none",
                    "generated_source_of_truth_active": False,
                }
            )
    return tuple(sorted(rows, key=_fragment_key))


def build_ownership_snapshot(
    *, project_root: Path, policy: G25ReadinessPolicy, current_base_commit: str
) -> dict[str, object]:
    root = project_root.resolve()
    base = _commit(current_base_commit)
    ownership_path = _repo_path(root, policy.ownership_policy_path)
    module_rows = _maps(
        build_module_manifest(project_root=root, policy_path=ownership_path)["modules"],
        "modules",
    )
    test_rows = _maps(
        build_test_manifest(project_root=root, policy_path=ownership_path)["tests"],
        "tests",
    )
    owners = {str(row["path"]): str(row["owner_profile"]) for row in (*module_rows, *test_rows)}
    domain_rows: list[dict[str, object]] = []
    for domain in policy.domains:
        manifest = _domain_manifest(domain, base)
        source_rows: list[dict[str, object]] = []
        for source in _maps(domain["source_inventory"], "source_inventory"):
            _exact(
                source,
                {"path", "role", "current_owner_profile", "canonical_owner_profile"},
                "SOURCE_INVENTORY_FIELDS",
            )
            path = _portable(source["path"], "source.path")
            actual = owners.get(path)
            expected = _text(source["current_owner_profile"], "current_owner_profile")
            if actual is None:
                raise G25ReadinessError("SOURCE_OWNER_UNKNOWN", path)
            canonical = _text(source["canonical_owner_profile"], "canonical_owner_profile")
            if actual not in {expected, canonical}:
                raise G25ReadinessError(
                    "SOURCE_OWNER_DRIFT",
                    f"{path}:allowed={sorted({expected, canonical})} actual={actual}",
                )
            source_rows.append(
                {
                    "path": path,
                    "role": _text(source["role"], "source.role"),
                    "current_owner_profile": actual,
                    "canonical_owner_profile": canonical,
                    "ownership_transition_required": actual != canonical,
                    "sha256": _sha256_path(_repo_file(root, path)),
                }
            )
        domain_rows.append(
            {
                "domain_id": domain["domain_id"],
                "task_id": domain["task_id"],
                "change_id": domain["change_id"],
                "owner": domain["owner"],
                "priority": domain["priority"],
                "batch": domain["batch"],
                "sequence": domain["sequence"],
                "owned_paths": list(manifest.owned_paths),
                "shared_paths_requested": list(_paths(domain["shared_paths_requested"])),
                "module_ids": list(manifest.module_ids),
                "contract_claims": [claim.to_dict() for claim in manifest.contract_claims],
                "generated_outputs": [
                    row["path"]
                    for row in policy_fragments(policy)
                    if row["owner"] == domain["owner"]
                ],
                "removal_targets": list(_maps(domain["removal_targets"], "removal_targets")),
                "required_validation_tiers": list(manifest.required_validation_tiers),
                "source_inventory": source_rows,
            }
        )
    body: dict[str, object] = {
        "schema_version": OWNERSHIP_SNAPSHOT_SCHEMA_VERSION,
        "status": "PASS",
        "gate_id": policy.gate_id,
        "source_phase": policy.source_phase,
        "source_base_commit": base,
        "max_active_domain_workers": policy.max_active_domain_workers,
        "domain_count": len(domain_rows),
        "unknown_owner_count": 0,
        "owned_path_overlap_count": 0,
        "shared_path_worker_owner_count": 0,
        "coordinator_only_paths": list(_paths(policy.coordinator["coordinator_only_paths"])),
        "domains": domain_rows,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {"snapshot_id": f"g2-5-ownership-{_digest(body)[:20]}", **body}


def build_fragment_preview(
    *,
    project_root: Path,
    policy: G25ReadinessPolicy,
    fragments: Sequence[Mapping[str, object]] | None = None,
    require_complete: bool = False,
) -> dict[str, object]:
    root = project_root.resolve()
    expected = policy_fragments(policy)
    candidates = list(fragments if fragments is not None else expected)
    owners = {_text(row["owner"], "owner"): row for row in policy.domains}
    targets = {_text(row["target_id"], "target_id"): row for row in policy.aggregate_targets}
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    ordered: list[dict[str, object]] = []
    for raw in sorted(candidates, key=_fragment_key):
        _exact(
            raw,
            {
                "fragment_id",
                "fragment_kind",
                "target_id",
                "path",
                "owner",
                "production_effect",
                "generated_source_of_truth_active",
            },
            "FRAGMENT_PREVIEW_FIELDS",
        )
        row = dict(raw)
        fragment_id = _text(row["fragment_id"], "fragment_id")
        path = _portable(row["path"], "fragment.path")
        if fragment_id in seen_ids:
            raise G25ReadinessError("DUPLICATE_FRAGMENT_ID", fragment_id)
        if path in seen_paths:
            raise G25ReadinessError("DUPLICATE_FRAGMENT_PATH", path)
        seen_ids.add(fragment_id)
        seen_paths.add(path)
        domain = owners.get(_text(row["owner"], "fragment.owner"))
        if domain is None:
            raise G25ReadinessError("UNKNOWN_FRAGMENT_OWNER", str(row["owner"]))
        target = targets.get(_text(row["target_id"], "target_id"))
        if target is None:
            raise G25ReadinessError("UNKNOWN_FRAGMENT_TARGET", str(row["target_id"]))
        if path not in _paths(domain["owned_paths"]):
            raise G25ReadinessError("UNKNOWN_FRAGMENT_PATH", path)
        root_prefix = _portable(target["fragment_root"], "fragment_root") + "/"
        if not path.startswith(root_prefix):
            raise G25ReadinessError("FRAGMENT_TARGET_PATH_MISMATCH", fragment_id)
        if row["production_effect"] != "none":
            raise G25ReadinessError("UNSAFE_FRAGMENT_EFFECT", fragment_id)
        if row["generated_source_of_truth_active"] is not False:
            raise G25ReadinessError("FRAGMENT_SOURCE_CUTOVER_FORBIDDEN", fragment_id)
        ordered.append(row)
    if require_complete:
        expected_by_id = {str(row["fragment_id"]): row for row in expected}
        actual_by_id = {str(row["fragment_id"]): row for row in ordered}
        missing = sorted(set(expected_by_id) - set(actual_by_id))
        extra = sorted(set(actual_by_id) - set(expected_by_id))
        if missing or extra:
            raise G25ReadinessError(
                "FRAGMENT_SET_INCOMPLETE",
                f"missing={missing} extra={extra}",
            )
        drifted = sorted(
            fragment_id
            for fragment_id in expected_by_id
            if dict(expected_by_id[fragment_id]) != dict(actual_by_id[fragment_id])
        )
        if drifted:
            raise G25ReadinessError("FRAGMENT_DEFINITION_DRIFT", ",".join(drifted))
    target_rows: list[dict[str, object]] = []
    for target_id, target in sorted(targets.items()):
        selected = [row for row in ordered if row["target_id"] == target_id]
        current_path = _portable(target["current_path"], "current_path")
        target_rows.append(
            {
                "target_id": target_id,
                "current_path": current_path,
                "current_sha256": _sha256_path(_repo_file(root, current_path)),
                "preview_fragment_ids": [row["fragment_id"] for row in selected],
                "preview_fragment_paths": [row["path"] for row in selected],
                "source_of_truth_diff_status": "UNCHANGED_SHADOW_PREVIEW",
                "source_of_truth_write_performed": False,
            }
        )
    body: dict[str, object] = {
        "schema_version": FRAGMENT_PREVIEW_SCHEMA_VERSION,
        "status": "PASS",
        "fragment_count": len(ordered),
        "duplicate_fragment_count": 0,
        "unknown_owner_count": 0,
        "unknown_target_count": 0,
        "unsafe_effect_count": 0,
        "fragment_order": ordered,
        "source_of_truth_diffs": target_rows,
        "aggregate_source_of_truth_changed": False,
        "aggregate_write_performed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {"preview_id": f"g2-5-fragment-{_digest(body)[:20]}", **body}


def evaluate_architecture_batch(
    manifests: Sequence[ChangeManifest],
    *,
    coordinator: ChangeManifest,
    policy: G25ReadinessPolicy,
    current_base_commit: str,
) -> dict[str, object]:
    issues: set[tuple[str, str]] = set()
    expected_domains = policy.domain_by_change_id()
    actual = {row.change_id: row for row in manifests}
    if len(actual) != len(manifests) or set(actual) != set(expected_domains):
        issues.add(("ARCHITECTURE_DOMAIN_SET_DRIFT", ",".join(sorted(actual))))
    coordinator_paths = _paths(policy.coordinator["coordinator_only_paths"])
    expected_coordinator = _coordinator_manifest(policy, current_base_commit)
    coordinator_checks = {
        "COORDINATOR_LANE_ROLE_MISMATCH": coordinator.lane_role is not LaneRole.COORDINATOR,
        "COORDINATOR_CHANGE_BINDING_MISMATCH": (
            coordinator.change_id != expected_coordinator.change_id
        ),
        "COORDINATOR_TASK_BINDING_MISMATCH": coordinator.task_id != expected_coordinator.task_id,
        "COORDINATOR_BASE_DRIFT": coordinator.base_commit != expected_coordinator.base_commit,
        "COORDINATOR_OWNER_MISMATCH": coordinator.owner != expected_coordinator.owner,
        "COORDINATOR_OWNED_PATH_SCOPE_DRIFT": (
            coordinator.owned_paths != expected_coordinator.owned_paths
        ),
        "COORDINATOR_SHARED_PATH_SCOPE_DRIFT": (
            coordinator.shared_paths != expected_coordinator.shared_paths
        ),
        "COORDINATOR_MODULE_SCOPE_DRIFT": (
            coordinator.module_ids != expected_coordinator.module_ids
        ),
        "COORDINATOR_CONTRACT_SCOPE_DRIFT": (
            coordinator.contract_claims != expected_coordinator.contract_claims
        ),
        "COORDINATOR_VALIDATION_TIER_SCOPE_DRIFT": (
            coordinator.required_validation_tiers != expected_coordinator.required_validation_tiers
        ),
    }
    issues.update(
        (code, coordinator.change_id) for code, failed in coordinator_checks.items() if failed
    )
    for change_id, manifest in actual.items():
        domain = expected_domains.get(change_id)
        if domain is None:
            issues.add(("UNKNOWN_ARCHITECTURE_CHANGE", change_id))
            continue
        expected = _domain_manifest(domain, current_base_commit)
        checks = {
            "DOMAIN_LANE_ROLE_MISMATCH": manifest.lane_role is not LaneRole.DOMAIN,
            "TASK_BINDING_MISMATCH": manifest.task_id != expected.task_id,
            "UNKNOWN_DOMAIN_OWNER": manifest.owner not in policy.reviewed_domain_owners,
            "OWNERSHIP_MISMATCH": manifest.owner != expected.owner,
            "MODULE_SCOPE_DRIFT": manifest.module_ids != expected.module_ids,
            "CONTRACT_SCOPE_DRIFT": manifest.contract_claims != expected.contract_claims,
            "VALIDATION_TIER_SCOPE_DRIFT": (
                manifest.required_validation_tiers != expected.required_validation_tiers
            ),
        }
        issues.update((code, change_id) for code, failed in checks.items() if failed)
        for path in sorted(set(manifest.owned_paths) - set(expected.owned_paths)):
            issues.add(("UNKNOWN_OWNED_PATH", path))
        for path in sorted(set(expected.owned_paths) - set(manifest.owned_paths)):
            issues.add(("OWNERSHIP_SCOPE_DRIFT", path))
        issues.update(
            ("COORDINATOR_ONLY_PATH_VIOLATION", path)
            for path in manifest.owned_paths
            if path in coordinator_paths
        )
        issues.update(("DOMAIN_SHARED_PATH_CLAIM", path) for path in manifest.shared_paths)
        issues.update(
            ("UNKNOWN_MODULE", module_id)
            for module_id in manifest.module_ids
            if module_id not in expected.module_ids
        )
    for conflict in detect_change_conflicts(tuple(actual.values())):
        issues.add((conflict.code, conflict.resource))
    plan = build_deterministic_lane_plan(
        [*manifests, coordinator],
        current_base_commit=current_base_commit,
        coordinator_only_paths=list(coordinator_paths),
        max_parallel_domain_lanes=policy.max_active_domain_workers,
    )
    issues.update((row.code, row.resource) for row in plan.blocking_issues)
    if plan.status == "PASS":
        for wave in plan.waves:
            assignments = wave.get("assignments")
            if (
                wave.get("kind") == "DOMAIN"
                and isinstance(assignments, list)
                and len(assignments) > policy.max_active_domain_workers
            ):
                issues.add(("DOMAIN_WORKER_CAPACITY_EXCEEDED", str(len(assignments))))
        if not plan.waves or plan.waves[-1].get("kind") != "COORDINATOR":
            issues.add(("COORDINATOR_NOT_FINAL", "lane_plan"))
    rows = [{"code": code, "resource": resource} for code, resource in sorted(issues)]
    return {
        "status": "PASS" if not rows and plan.status == "PASS" else "BLOCKED",
        "reason_codes": sorted({str(row["code"]) for row in rows}),
        "issues": rows,
        "lane_plan": plan.to_dict(),
        "conflicting_manifests_rejected": True,
        "dispatch_allowed": False,
        "lease_acquisition_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_g2_5_readiness_evidence(
    *,
    project_root: Path,
    current_base_commit: str,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, object]:
    root = project_root.resolve()
    supplied = _commit(current_base_commit)
    head = _git_head(root)
    if supplied != head:
        raise G25ReadinessError("HEAD_DRIFT", f"supplied={supplied} actual={head}")
    return _build_g2_5_readiness_evidence(
        project_root=root,
        source_base_commit=head,
        policy_path=policy_path,
    )


def _build_g2_5_readiness_evidence(
    *,
    project_root: Path,
    source_base_commit: str,
    policy_path: Path,
    git_project_root: Path | None = None,
) -> dict[str, object]:
    root = project_root.resolve()
    git_root = (git_project_root or root).resolve()
    base = _commit(source_base_commit)
    resolved_policy = policy_path if policy_path.is_absolute() else root / policy_path
    policy = load_g2_5_readiness_policy(resolved_policy)
    manifests = tuple(_domain_manifest(row, base) for row in policy.domains)
    coordinator = _coordinator_manifest(policy, base)
    batch = evaluate_architecture_batch(
        manifests,
        coordinator=coordinator,
        policy=policy,
        current_base_commit=base,
    )
    if batch["status"] != "PASS":
        raise G25ReadinessError("NON_CONFLICTING_FIXTURE_BLOCKED", str(batch["reason_codes"]))
    control_policy_path = _repo_file(root, policy.parallel_control_policy_path)
    control_policy = load_parallel_control_policy(control_policy_path)
    if control_policy.max_parallel_domain_lanes != policy.max_active_domain_workers:
        raise G25ReadinessError("CONTROL_PLANE_CAPACITY_DRIFT", control_policy.policy_version)
    generator_path = _repo_file(root, GENERATOR_SOURCE_PATH.as_posix())
    body: dict[str, object] = {
        "schema_version": REHEARSAL_SCHEMA_VERSION,
        "status": "PASS",
        "gate_id": policy.gate_id,
        "policy": {
            "path": _relative(resolved_policy, root),
            "sha256": policy.sha256,
            "status": policy.status,
            "owner": policy.owner,
            "owner_resume_ref": policy.owner_resume_ref,
        },
        "parallel_control_policy": {
            "path": policy.parallel_control_policy_path,
            "sha256": _sha256_path(control_policy_path),
            "policy_version": control_policy.policy_version,
            "status": control_policy.status,
            "source_of_truth": control_policy.source_of_truth,
        },
        "generator_source": {
            "path": GENERATOR_SOURCE_PATH.as_posix(),
            "sha256": _sha256_path(generator_path),
        },
        "source_gate": _source_gate(root, policy, base, git_project_root=git_root),
        "source_base_commit": base,
        "ownership_snapshot": build_ownership_snapshot(
            project_root=root, policy=policy, current_base_commit=base
        ),
        "change_manifests": [row.to_dict() for row in (*manifests, coordinator)],
        "non_conflicting_lane_plan": batch,
        "two_active_domain_worker_fixture": _scheduler_fixture(
            policy, manifests, base, control_policy
        ),
        "fail_closed_rehearsals": _negative_rehearsals(policy, manifests, coordinator, base),
        "fragment_preview": build_fragment_preview(
            project_root=root,
            policy=policy,
            require_complete=True,
        ),
        "merge_order": list(policy.merge_order),
        "next_domain_batches": [
            {
                "batch": batch_id,
                "domain_ids": [
                    row["domain_id"] for row in policy.domains if row["batch"] == batch_id
                ],
                "active_domain_worker_count": sum(
                    row["batch"] == batch_id for row in policy.domains
                ),
            }
            for batch_id in sorted({_integer(row["batch"]) for row in policy.domains})
        ],
        "source_of_truth": "LEGACY_MARKDOWN_ONLY",
        "dispatch_allowed": False,
        "lease_acquisition_allowed": False,
        "automatic_merge_allowed": False,
        "aggregate_source_of_truth_changed": False,
        "task_registry_mutated": False,
        "strategy_logic_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["report_checksum"] = _digest(body)
    return body


def validate_g2_5_readiness_evidence(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    current_base_commit: str,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> None:
    root = project_root.resolve()
    supplied_head = _commit(current_base_commit)
    actual_head = _git_head(root)
    if supplied_head != actual_head:
        raise G25ReadinessError("HEAD_DRIFT", f"supplied={supplied_head} actual={actual_head}")
    if payload.get("schema_version") != REHEARSAL_SCHEMA_VERSION:
        raise G25ReadinessError("EVIDENCE_SCHEMA", str(payload.get("schema_version")))
    body = {key: value for key, value in payload.items() if key != "report_checksum"}
    if payload.get("report_checksum") != _digest(body):
        raise G25ReadinessError("EVIDENCE_CHECKSUM_DRIFT", "report_checksum")
    source_base = _commit(payload.get("source_base_commit"))
    if not _git_commit_exists(root, source_base):
        raise G25ReadinessError("SOURCE_BASE_UNKNOWN", source_base)
    if not _git_is_ancestor(root, source_base, actual_head):
        raise G25ReadinessError(
            "SOURCE_BASE_NOT_ANCESTOR", f"source={source_base} head={actual_head}"
        )
    evidence_path = DEFAULT_EVIDENCE_PATH.as_posix()
    carrier: str | None = None
    carrier_blob: bytes | None = None
    carrier_blob_error: G25ReadinessError | None = None
    if source_base != actual_head:
        carrier = _first_parent_direct_child(
            project_root=root,
            source_base_commit=source_base,
            current_head_commit=actual_head,
        )
        try:
            carrier_blob = _git_blob(root, carrier, evidence_path)
        except G25ReadinessError as exc:
            if exc.code != "GIT_BLOB_UNAVAILABLE":
                raise
            carrier_blob_error = exc
        if carrier_blob == canonical_json_bytes(dict(payload)):
            resolved_policy = policy_path if policy_path.is_absolute() else root / policy_path
            policy_portable = _relative(resolved_policy, root)
            with _g2_5_carrier_snapshot(root, carrier) as snapshot_root:
                expected = _build_g2_5_readiness_evidence(
                    project_root=snapshot_root,
                    source_base_commit=source_base,
                    policy_path=snapshot_root / policy_portable,
                    git_project_root=root,
                )
            if dict(payload) != expected:
                raise G25ReadinessError(
                    "EVIDENCE_REPRODUCIBILITY_DRIFT",
                    f"carrier snapshot={carrier}",
                )
            return
    expected = _build_g2_5_readiness_evidence(
        project_root=root,
        source_base_commit=source_base,
        policy_path=policy_path,
    )
    if dict(payload) == expected:
        return
    if carrier is None:
        carrier = _first_parent_direct_child(
            project_root=root,
            source_base_commit=source_base,
            current_head_commit=actual_head,
        )
    if carrier_blob_error is not None:
        raise carrier_blob_error
    if carrier_blob != canonical_json_bytes(dict(payload)):
        raise G25ReadinessError(
            "EVIDENCE_CARRIER_BLOB_DRIFT",
            f"carrier={carrier} path={evidence_path}",
        )
    raise G25ReadinessError(
        "EVIDENCE_REPRODUCIBILITY_DRIFT",
        f"carrier snapshot={carrier}",
    )


def write_g2_5_readiness_evidence(
    path: Path,
    *,
    project_root: Path,
    current_base_commit: str,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> Path:
    payload = build_g2_5_readiness_evidence(
        project_root=project_root,
        current_base_commit=current_base_commit,
        policy_path=policy_path,
    )
    write_json_atomic(path, payload, sort_keys=True)
    validate_g2_5_readiness_evidence(
        _mapping(json.loads(path.read_text(encoding="utf-8")), "evidence"),
        project_root=project_root,
        current_base_commit=current_base_commit,
        policy_path=policy_path,
    )
    return path


def _validate_policy(policy: G25ReadinessPolicy) -> None:
    if policy.source_phase != "ARCH-004G2.4":
        raise G25ReadinessError("SOURCE_PHASE", policy.source_phase)
    if policy.max_active_domain_workers != 2:
        raise G25ReadinessError("DOMAIN_WORKER_CAPACITY", str(policy.max_active_domain_workers))
    if policy.merge_order != CANONICAL_MERGE_ORDER:
        raise G25ReadinessError("MERGE_ORDER_DRIFT", ",".join(policy.merge_order))
    _validate_safety(policy.safety)
    domain_ids = {_text(row["domain_id"], "domain_id") for row in policy.domains}
    if domain_ids != EXPECTED_DOMAIN_IDS:
        raise G25ReadinessError("DOMAIN_SET", ",".join(sorted(domain_ids)))
    if [row["sequence"] for row in policy.domains] != [1, 2, 3]:
        raise G25ReadinessError("DOMAIN_SEQUENCE", str([row["sequence"] for row in policy.domains]))
    if [row["batch"] for row in policy.domains] != [1, 1, 2]:
        raise G25ReadinessError("DOMAIN_BATCHES", str([row["batch"] for row in policy.domains]))
    changes = [_text(row["change_id"], "change_id") for row in policy.domains]
    if changes != sorted(changes):
        raise G25ReadinessError("CHANGE_ID_ORDER", ",".join(changes))
    coordinator_paths = set(_paths(policy.coordinator["coordinator_only_paths"]))
    owned: dict[str, str] = {}
    source_paths: dict[str, str] = {}
    fragment_ids: set[str] = set()
    target_ids = {_text(row["target_id"], "target_id") for row in policy.aggregate_targets}
    for target in policy.aggregate_targets:
        _exact(target, {"target_id", "current_path", "fragment_root"}, "TARGET_FIELDS")
        _portable(target["current_path"], "current_path")
        _portable(target["fragment_root"], "fragment_root")
    for domain in policy.domains:
        _exact(domain, _DOMAIN_KEYS, "DOMAIN_FIELDS")
        domain_id = _text(domain["domain_id"], "domain_id")
        owner = _text(domain["owner"], "owner")
        if owner not in policy.reviewed_domain_owners:
            raise G25ReadinessError("UNKNOWN_DOMAIN_OWNER", owner)
        manifest = _domain_manifest(domain, "0" * 40)
        for path in manifest.owned_paths:
            if path in coordinator_paths:
                raise G25ReadinessError("COORDINATOR_ONLY_PATH_VIOLATION", path)
            if path in owned:
                raise G25ReadinessError("OWNED_PATH_OVERLAP", path)
            owned[path] = domain_id
        for path in _paths(domain["shared_paths_requested"]):
            if path not in coordinator_paths:
                raise G25ReadinessError("UNKNOWN_SHARED_PATH_REQUEST", path)
        for removal in _maps(domain["removal_targets"], "removal_targets"):
            _exact(removal, {"path", "removal_gate"}, "REMOVAL_FIELDS")
            if _portable(removal["path"], "removal.path") not in coordinator_paths:
                raise G25ReadinessError(
                    "REMOVAL_TARGET_NOT_COORDINATOR_OWNED", str(removal["path"])
                )
        for source in _maps(domain["source_inventory"], "source_inventory"):
            _exact(
                source,
                {"path", "role", "current_owner_profile", "canonical_owner_profile"},
                "SOURCE_INVENTORY_FIELDS",
            )
            path = _portable(source["path"], "source.path")
            if path in source_paths:
                raise G25ReadinessError("SOURCE_INVENTORY_OVERLAP", path)
            source_paths[path] = domain_id
        for fragment in _maps(domain["fragments"], "fragments"):
            _exact(
                fragment, {"fragment_id", "fragment_kind", "target_id", "path"}, "FRAGMENT_FIELDS"
            )
            fragment_id = _text(fragment["fragment_id"], "fragment_id")
            path = _portable(fragment["path"], "fragment.path")
            if fragment_id in fragment_ids:
                raise G25ReadinessError("DUPLICATE_FRAGMENT_ID", fragment_id)
            if path not in manifest.owned_paths:
                raise G25ReadinessError("UNKNOWN_FRAGMENT_PATH", path)
            if fragment["target_id"] not in target_ids:
                raise G25ReadinessError("UNKNOWN_FRAGMENT_TARGET", str(fragment["target_id"]))
            fragment_ids.add(fragment_id)
    _coordinator_manifest(policy, "0" * 40)


def _validate_safety(safety: Mapping[str, Any]) -> None:
    expected = _SAFETY_FALSE_FIELDS | {"source_of_truth", "production_effect", "broker_action"}
    _exact(safety, expected, "SAFETY_FIELDS")
    if safety["source_of_truth"] != "LEGACY_MARKDOWN_ONLY":
        raise G25ReadinessError("SOURCE_OF_TRUTH_CUTOVER_FORBIDDEN", str(safety["source_of_truth"]))
    unsafe = [field for field in _SAFETY_FALSE_FIELDS if safety[field] is not False]
    if unsafe:
        raise G25ReadinessError("UNSAFE_PERMISSION", ",".join(sorted(unsafe)))
    if safety["production_effect"] != "none" or safety["broker_action"] != "none":
        raise G25ReadinessError("UNSAFE_EFFECT", "production/broker")


def _source_gate(
    root: Path,
    policy: G25ReadinessPolicy,
    source_base_commit: str,
    *,
    git_project_root: Path | None = None,
) -> dict[str, object]:
    git_root = (git_project_root or root).resolve()
    handoff_path = _repo_file(root, policy.source_handoff_path)
    matrix_path = _repo_file(root, policy.migration_matrix_path)
    bundle_path = _repo_file(root, policy.source_validation_bundle_path)
    handoff = _mapping(_load_unique_yaml_path(handoff_path, "handoff"), "handoff")
    matrix = _mapping(_load_unique_yaml_path(matrix_path, "matrix"), "matrix")
    source_base = _commit(source_base_commit)
    handoff_base = _commit(handoff.get("base_commit"))
    handoff_head = _commit(handoff.get("head_commit"))
    handoff_branch = _text(handoff.get("branch"), "handoff.branch")
    for label, commit in (("base", handoff_base), ("head", handoff_head)):
        if not _git_commit_exists(git_root, commit):
            raise G25ReadinessError(f"HANDOFF_{label.upper()}_UNKNOWN", commit)
    if not _git_is_ancestor(git_root, handoff_base, handoff_head):
        raise G25ReadinessError(
            "HANDOFF_BASE_HEAD_LINEAGE",
            f"base={handoff_base} head={handoff_head}",
        )
    if not _git_is_ancestor(git_root, handoff_head, source_base):
        raise G25ReadinessError(
            "HANDOFF_HEAD_SOURCE_BASE_LINEAGE",
            f"handoff_head={handoff_head} source_base={source_base}",
        )
    frozen_matrix = _mapping(handoff.get("migration_matrix"), "handoff.migration_matrix")
    architecture_state = _mapping(handoff.get("architecture_state"), "handoff.architecture_state")
    attribution = _mapping(handoff.get("worktree_attribution"), "handoff.worktree_attribution")
    frozen_paths = {
        _portable(frozen_matrix.get("path"), "handoff.migration_matrix.path"),
        _portable(attribution.get("attribution_path"), "handoff.attribution_path"),
    }
    for raw in architecture_state.values():
        row = _mapping(raw, "handoff.architecture_state.record")
        frozen_paths.add(_portable(row.get("path"), "handoff.architecture_state.path"))
    if len(frozen_paths) != 6:
        raise G25ReadinessError("HANDOFF_FROZEN_SOURCE_SET", str(sorted(frozen_paths)))
    frozen_tracked_files = {
        path: _git_blob(git_root, handoff_head, path) for path in sorted(frozen_paths)
    }
    frozen_validation_artifacts = load_source_validation_bundle(
        path=bundle_path,
        expected_sha256=policy.source_validation_bundle_sha256,
        handoff=handoff,
        handoff_path=handoff_path,
        project_root=root,
    )
    try:
        validate_bootstrap_handoff(
            handoff,
            project_root=root,
            expected_head_commit=handoff_head,
            expected_branch=handoff_branch,
            frozen_tracked_files=frozen_tracked_files,
            frozen_validation_artifacts=frozen_validation_artifacts,
        )
    except BootstrapHandoffError as exc:
        raise G25ReadinessError(f"HANDOFF_{exc.code}", exc.message) from exc
    if handoff.get("completed_phase") != policy.source_phase:
        raise G25ReadinessError("HANDOFF_SOURCE_PHASE", str(handoff.get("completed_phase")))
    summary = _mapping(matrix.get("summary"), "matrix.summary")
    summary_fields = (
        "baseline_callback_count",
        "migrated_callback_count",
        "pending_callback_count",
        "unresolved_callback_count",
        "duplicate_callback_count",
    )
    current_summary = {field: summary.get(field) for field in summary_fields}
    current_summary["phase_exit_ready"] = summary.get("phase_exit_ready")
    counts = tuple(current_summary[field] for field in summary_fields)
    if (
        matrix.get("schema_version") != "arch_004g2_callback_migration_matrix.v1"
        or matrix.get("status") != "PASS"
        or matrix.get("source_phase") != policy.source_phase
        or counts != (967, 967, 0, 0, 0)
        or summary.get("phase_exit_ready") is not True
    ):
        raise G25ReadinessError("MIGRATION_MATRIX_INCOMPLETE", str(counts))
    frozen_sha = _text(frozen_matrix.get("sha256"), "handoff.migration_matrix.sha256")
    current_sha = _sha256_path(matrix_path)
    return {
        "status": "PASS",
        "source_phase": policy.source_phase,
        "canonical_handoff_validation": "PASS",
        "git_lineage": {
            "status": "PASS",
            "handoff_base_commit": handoff_base,
            "handoff_head_commit": handoff_head,
            "source_base_commit": source_base,
            "handoff_base_is_ancestor_of_handoff_head": True,
            "handoff_head_is_ancestor_of_source_base": True,
        },
        "handoff": {
            "path": policy.source_handoff_path,
            "sha256": _sha256_path(handoff_path),
            "checksum": handoff["handoff_checksum"],
            "head_commit": handoff_head,
            "base_commit": handoff["base_commit"],
            "branch": handoff_branch,
            "push_status": handoff["push_status"],
        },
        "validation_bundle": {
            "path": policy.source_validation_bundle_path,
            "sha256": policy.source_validation_bundle_sha256,
            "schema_version": VALIDATION_BUNDLE_SCHEMA_VERSION,
            "artifact_count": len(frozen_validation_artifacts),
            "original_paths": sorted(frozen_validation_artifacts),
            "complete": True,
        },
        "historical_next_slice_unblocked": False,
        "owner_resume_ref": policy.owner_resume_ref,
        "owner_resume_authorized": True,
        "frozen_phase_exit_matrix": {
            "path": frozen_matrix["path"],
            "sha256": frozen_sha,
            "status": frozen_matrix["status"],
            "summary": {
                "baseline_callback_count": frozen_matrix["baseline_callback_count"],
                "migrated_callback_count": frozen_matrix["migrated_callback_count"],
                "pending_callback_count": frozen_matrix["pending_callback_count"],
                "unresolved_callback_count": frozen_matrix["unresolved_callback_count"],
                "duplicate_callback_count": frozen_matrix["duplicate_registration_count"],
                "phase_exit_ready": frozen_matrix["phase_exit_criteria_passed"],
            },
        },
        "current_matrix": {
            "path": policy.migration_matrix_path,
            "sha256": current_sha,
            "matches_frozen_phase_exit_sha256": current_sha == frozen_sha,
            "schema_version": matrix["schema_version"],
            "status": matrix["status"],
            "source_phase": matrix["source_phase"],
            "current_callback_set_sha256": matrix.get("current_callback_set_sha256"),
            "summary": current_summary,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _domain_manifest(domain: Mapping[str, Any], base: str) -> ChangeManifest:
    return parse_change_manifest(
        {
            "schema_version": "change_manifest.v1",
            "change_id": domain["change_id"],
            "task_id": domain["task_id"],
            "lane_role": "DOMAIN",
            "base_commit": base,
            "owner": domain["owner"],
            "production_effect": "none",
            "owned_paths": list(_paths(domain["owned_paths"])),
            "shared_paths": [],
            "module_ids": list(_strings(domain["module_ids"])),
            "contract_claims": list(_maps(domain["contract_claims"], "contract_claims")),
            "required_validation_tiers": list(_strings(domain["required_validation_tiers"])),
        }
    )


def _coordinator_manifest(policy: G25ReadinessPolicy, base: str) -> ChangeManifest:
    row = policy.coordinator
    _exact(
        row,
        {
            "change_id",
            "task_id",
            "owner",
            "owned_paths",
            "coordinator_only_paths",
            "required_validation_tiers",
        },
        "COORDINATOR_FIELDS",
    )
    return parse_change_manifest(
        {
            "schema_version": "change_manifest.v1",
            "change_id": row["change_id"],
            "task_id": row["task_id"],
            "lane_role": "COORDINATOR",
            "base_commit": base,
            "owner": row["owner"],
            "production_effect": "none",
            "owned_paths": list(_paths(row["owned_paths"])),
            "shared_paths": sorted(
                {
                    path
                    for domain in policy.domains
                    for path in _paths(domain["shared_paths_requested"])
                }
            ),
            "module_ids": [],
            "contract_claims": [],
            "required_validation_tiers": list(_strings(row["required_validation_tiers"])),
        }
    )


def _scheduler_fixture(
    policy: G25ReadinessPolicy,
    manifests: Sequence[ChangeManifest],
    base: str,
    control_policy: ParallelControlPolicy,
) -> dict[str, object]:
    domains = policy.domain_by_change_id()
    tasks = tuple(
        TaskControlRecord(
            task_id=manifest.task_id,
            title=_text(domains[manifest.change_id]["domain_id"], "domain_id"),
            governance_status="IN_PROGRESS",
            priority=_text(domains[manifest.change_id]["priority"], "priority"),
            requirement_refs=policy.gate_requirement_refs,
            acceptance_criteria=policy.gate_acceptance_criteria,
            manifest=manifest,
        )
        for manifest in manifests
    )
    decision = build_shadow_scheduler_decision(
        PilotSpec(
            pilot_id="arch-004-g2-5-two-worker-shadow-fixture",
            policy_id=control_policy.policy_id,
            tasks=tasks,
            dependencies=(),
            governance_cycles=("fixture-cycle-1", "fixture-cycle-2"),
            failure_recovery={},
            safety={"production_effect": "none", "broker_action": "none"},
        ),
        policy=control_policy,
        current_base_commit=base,
        observed_statuses={},
    ).to_dict()
    selected = decision.get("selected")
    deferred = decision.get("not_selected")
    if not isinstance(selected, list) or len(selected) != 2:
        raise G25ReadinessError("TWO_WORKER_FIXTURE", "selected")
    if (
        not isinstance(deferred, list)
        or len(deferred) != 1
        or "DOMAIN_CAPACITY_REACHED" not in deferred[0].get("reason_codes", [])
    ):
        raise G25ReadinessError("TWO_WORKER_FIXTURE", "deferred")
    decision.update(
        fixture_status="PASS",
        active_domain_worker_count=2,
        max_active_domain_workers=policy.max_active_domain_workers,
    )
    return decision


def _negative_rehearsals(
    policy: G25ReadinessPolicy,
    manifests: Sequence[ChangeManifest],
    coordinator: ChangeManifest,
    base: str,
) -> list[dict[str, object]]:
    first, second = manifests[:2]
    raw_contract_claims = second.to_dict()["contract_claims"]
    if not isinstance(raw_contract_claims, list):
        raise G25ReadinessError("CONTRACT_FIXTURE_TYPE", second.change_id)
    contract_claims = list(raw_contract_claims)
    contract_claims.append({"contract_id": "workflow_spec.v1", "version": "v2", "access": "READ"})
    stale = "0" * 40 if base != "0" * 40 else "1" * 40
    batch_specs: tuple[tuple[str, int, Mapping[str, object], set[str]], ...] = (
        (
            "conflicting_owned_path",
            1,
            {"owned_paths": [*second.owned_paths[1:], first.owned_paths[0]]},
            {"OWNED_PATH_OVERLAP"},
        ),
        (
            "unknown_owned_path",
            0,
            {"owned_paths": [*first.owned_paths, "src/ai_trading_system/unknown.py"]},
            {"UNKNOWN_OWNED_PATH"},
        ),
        (
            "domain_shared_path_claim",
            0,
            {"shared_paths": ["docs/system_flow.md"]},
            {"DOMAIN_SHARED_PATH_CLAIM"},
        ),
        (
            "coordinator_only_path_claim",
            0,
            {"owned_paths": [*first.owned_paths, "docs/system_flow.md"]},
            {"COORDINATOR_ONLY_PATH_VIOLATION"},
        ),
        (
            "unknown_owner",
            0,
            {"owner": "unknown_owner"},
            {"UNKNOWN_DOMAIN_OWNER", "OWNERSHIP_MISMATCH"},
        ),
        (
            "unknown_module",
            0,
            {"module_ids": [*first.module_ids, "ai_trading_system.unknown"]},
            {"UNKNOWN_MODULE"},
        ),
        ("base_commit_drift", 0, {"base_commit": stale}, {"BASE_DRIFT"}),
        (
            "contract_version_conflict",
            1,
            {"contract_claims": contract_claims},
            {"CONTRACT_VERSION_CONFLICT"},
        ),
    )
    rows: list[dict[str, object]] = []
    for case_id, index, mutation, expected_codes in batch_specs:
        changed = list(manifests)
        payload = changed[index].to_dict()
        payload.update(mutation)
        changed[index] = parse_change_manifest(payload)
        result = evaluate_architecture_batch(
            changed, coordinator=coordinator, policy=policy, current_base_commit=base
        )
        actual = set(_strings(result["reason_codes"]))
        rows.append(_case(case_id, "BLOCKED", result["status"], expected_codes, actual))
    coordinator_specs: tuple[tuple[str, Mapping[str, object], str], ...] = (
        (
            "coordinator_lane_role_drift",
            {"lane_role": "DOMAIN"},
            "COORDINATOR_LANE_ROLE_MISMATCH",
        ),
        (
            "coordinator_change_binding_drift",
            {"change_id": "unexpected-coordinator"},
            "COORDINATOR_CHANGE_BINDING_MISMATCH",
        ),
        (
            "coordinator_task_binding_drift",
            {"task_id": "UNEXPECTED_COORDINATOR_TASK"},
            "COORDINATOR_TASK_BINDING_MISMATCH",
        ),
        (
            "coordinator_base_drift",
            {"base_commit": stale},
            "COORDINATOR_BASE_DRIFT",
        ),
        (
            "coordinator_owner_drift",
            {"owner": "unreviewed_actor"},
            "COORDINATOR_OWNER_MISMATCH",
        ),
        (
            "coordinator_owned_path_drift",
            {"owned_paths": ["inputs/architecture/unexpected_coordinator.json"]},
            "COORDINATOR_OWNED_PATH_SCOPE_DRIFT",
        ),
        (
            "coordinator_shared_path_drift",
            {"shared_paths": []},
            "COORDINATOR_SHARED_PATH_SCOPE_DRIFT",
        ),
        (
            "coordinator_module_drift",
            {"module_ids": ["ai_trading_system.unknown"]},
            "COORDINATOR_MODULE_SCOPE_DRIFT",
        ),
        (
            "coordinator_contract_drift",
            {
                "contract_claims": [
                    {"contract_id": "workflow_spec.v1", "version": "v1", "access": "READ"}
                ]
            },
            "COORDINATOR_CONTRACT_SCOPE_DRIFT",
        ),
        (
            "coordinator_validation_tier_drift",
            {"required_validation_tiers": ["focused"]},
            "COORDINATOR_VALIDATION_TIER_SCOPE_DRIFT",
        ),
    )
    for case_id, mutation, expected_code in coordinator_specs:
        payload = coordinator.to_dict()
        payload.update(mutation)
        changed_coordinator = parse_change_manifest(payload)
        result = evaluate_architecture_batch(
            manifests,
            coordinator=changed_coordinator,
            policy=policy,
            current_base_commit=base,
        )
        actual = set(_strings(result["reason_codes"]))
        rows.append(_case(case_id, "BLOCKED", result["status"], {expected_code}, actual))
    parse_specs: tuple[tuple[str, Mapping[str, object], str], ...] = (
        (
            "unsafe_production_effect",
            {"production_effect": "production"},
            "UNSAFE_PRODUCTION_EFFECT",
        ),
        (
            "missing_validation_tier",
            {"required_validation_tiers": []},
            "MISSING_VALIDATION_REQUIREMENTS",
        ),
    )
    for case_id, mutation, expected_code in parse_specs:
        payload = first.to_dict()
        payload.update(mutation)
        observed = "NO_REJECTION"
        try:
            parse_change_manifest(payload)
        except ParallelControlError as exc:
            observed = exc.code
        rows.append(
            _case(
                case_id,
                "PARSE_REJECTED",
                "PARSE_REJECTED" if observed == expected_code else observed,
                {expected_code},
                {observed},
            )
        )
    if any(row["rehearsal_status"] != "PASS" for row in rows):
        raise G25ReadinessError("FAIL_CLOSED_REHEARSAL", "negative fixture")
    return rows


def _case(
    case_id: str,
    expected_outcome: str,
    observed_outcome: object,
    expected_codes: set[str],
    observed_codes: set[str],
) -> dict[str, object]:
    passed = observed_outcome == expected_outcome and expected_codes.issubset(observed_codes)
    return {
        "case_id": case_id,
        "expected_outcome": expected_outcome,
        "observed_outcome": observed_outcome,
        "expected_reason_codes": sorted(expected_codes),
        "observed_reason_codes": sorted(observed_codes),
        "rehearsal_status": "PASS" if passed else "FAIL",
        "dispatch_allowed": False,
        "lease_acquisition_allowed": False,
        "production_effect": "none",
    }


def _fragment_key(row: Mapping[str, object]) -> tuple[str, str, str]:
    return (
        str(row.get("target_id", "")),
        str(row.get("fragment_id", "")),
        str(row.get("path", "")),
    )


def _load_unique_yaml_path(path: Path, label: str) -> object:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise G25ReadinessError("YAML_READ", f"{label}:{path}") from exc
    try:
        return yaml.load(text, Loader=_UniqueKeySafeLoader)
    except G25ReadinessError:
        raise
    except yaml.YAMLError as exc:
        raise G25ReadinessError("YAML_PARSE", f"{label}:{path}") from exc


def _load_unique_json_path(path: Path, label: str) -> object:
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(text, object_pairs_hook=_unique_json_object)
    except G25ReadinessError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise G25ReadinessError("JSON_READ", f"{label}:{path}") from exc


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise G25ReadinessError("JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise G25ReadinessError("MAPPING_REQUIRED", field)
    return value


def _maps(value: object, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise G25ReadinessError("LIST_REQUIRED", field)
    return tuple(_mapping(row, field) for row in value)


def _strings(value: object, *, preserve_order: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise G25ReadinessError("LIST_REQUIRED", "strings")
    rows = tuple(_text(row, "string") for row in value)
    if len(rows) != len(set(rows)):
        raise G25ReadinessError("DUPLICATE_VALUE", "strings")
    return rows if preserve_order else tuple(sorted(rows))


def _paths(value: object) -> tuple[str, ...]:
    return tuple(sorted(_portable(row, "path") for row in _strings(value)))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise G25ReadinessError("TEXT_REQUIRED", field)
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise G25ReadinessError("POSITIVE_INT_REQUIRED", str(value))
    return value


def _portable(value: object, field: str) -> str:
    text = _text(value, field)
    path = PurePosixPath(text)
    if (
        "\\" in text
        or text.endswith("/")
        or path.is_absolute()
        or ":" in path.parts[0]
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != text
    ):
        raise G25ReadinessError("UNSAFE_PATH", text)
    return text


def _commit(value: object) -> str:
    text = _text(value, "commit")
    if _COMMIT_RE.fullmatch(text) is None:
        raise G25ReadinessError("INVALID_COMMIT", text)
    return text


def _sha256_text(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA256_RE.fullmatch(text) is None:
        raise G25ReadinessError("INVALID_SHA256", f"{field}:{text}")
    return text


def _exact(payload: Mapping[str, Any], expected: set[str], code: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise G25ReadinessError(
            code, f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}"
        )


@contextmanager
def _g2_5_carrier_snapshot(project_root: Path, commit: str) -> Iterator[Path]:
    root = project_root.resolve()
    validated_commit = _commit(commit)
    with tempfile.TemporaryDirectory(prefix="aits-g2-5-readiness-") as raw_temp:
        temp_root = Path(raw_temp)
        archive_path = temp_root / "snapshot.tar"
        result = _git_process(
            root,
            "archive",
            "--format=tar",
            f"--output={archive_path}",
            validated_commit,
            "--",
            *_CARRIER_SNAPSHOT_PATHS,
        )
        if result.returncode != 0:
            raise G25ReadinessError(
                "EVIDENCE_CARRIER_SNAPSHOT_UNAVAILABLE",
                _git_error(result),
            )
        snapshot_root = temp_root / "snapshot"
        snapshot_root.mkdir()
        try:
            with tarfile.open(archive_path, mode="r:") as archive:
                extract_validated_archive(archive, snapshot_root)
        except (OSError, tarfile.TarError, WaveReadinessError) as exc:
            raise G25ReadinessError(
                "EVIDENCE_CARRIER_SNAPSHOT_INVALID",
                str(exc),
            ) from exc
        yield snapshot_root


def _first_parent_direct_child(
    *,
    project_root: Path,
    source_base_commit: str,
    current_head_commit: str,
) -> str:
    root = project_root.resolve()
    source_base = _commit(source_base_commit)
    current_head = _commit(current_head_commit)
    result = _git_process(
        root,
        "rev-list",
        "--first-parent",
        "--reverse",
        f"{source_base}..{current_head}",
    )
    if result.returncode != 0:
        raise G25ReadinessError("EVIDENCE_CARRIER_LINEAGE_UNAVAILABLE", _git_error(result))
    commits = [_commit(row) for row in result.stdout.decode("ascii").splitlines() if row]
    if not commits:
        raise G25ReadinessError(
            "EVIDENCE_CARRIER_REQUIRED",
            f"source_base={source_base} head={current_head}",
        )
    carrier = commits[0]
    parent_result = _git_process(root, "rev-parse", "--verify", f"{carrier}^1")
    if parent_result.returncode != 0:
        raise G25ReadinessError(
            "EVIDENCE_CARRIER_PARENT_UNAVAILABLE",
            _git_error(parent_result),
        )
    parent = _commit(parent_result.stdout.decode("ascii").strip())
    if parent != source_base:
        raise G25ReadinessError(
            "EVIDENCE_CARRIER_DIRECT_CHILD_REQUIRED",
            f"source_base={source_base} carrier={carrier} carrier_parent={parent}",
        )
    return carrier


def _git_head(root: Path) -> str:
    result = _git_process(root, "rev-parse", "--verify", "HEAD")
    if result.returncode != 0:
        raise G25ReadinessError("GIT_HEAD_UNAVAILABLE", _git_error(result))
    return _commit(result.stdout.decode("ascii").strip())


def _git_commit_exists(root: Path, commit: str) -> bool:
    result = _git_process(root, "cat-file", "-e", f"{_commit(commit)}^{{commit}}")
    return result.returncode == 0


def _git_is_ancestor(root: Path, ancestor: str, descendant: str) -> bool:
    result = _git_process(
        root,
        "merge-base",
        "--is-ancestor",
        _commit(ancestor),
        _commit(descendant),
    )
    if result.returncode not in {0, 1}:
        raise G25ReadinessError("GIT_ANCESTRY_CHECK_FAILED", _git_error(result))
    return result.returncode == 0


def _git_blob(root: Path, commit: str, portable: str) -> bytes:
    path = _portable(portable, "git_blob.path")
    result = _git_process(root, "show", f"{_commit(commit)}:{path}")
    if result.returncode != 0:
        raise G25ReadinessError("GIT_BLOB_UNAVAILABLE", f"{path}:{_git_error(result)}")
    return result.stdout


def _git_process(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            capture_output=True,
            shell=False,
        )
    except OSError as exc:
        raise G25ReadinessError("GIT_EXECUTION_FAILED", str(exc)) from exc


def _git_error(result: subprocess.CompletedProcess[bytes]) -> str:
    return result.stderr.decode("utf-8", errors="replace").strip() or "git command failed"


def _repo_path(root: Path, portable: str) -> Path:
    candidate = (root / portable).resolve()
    if not candidate.is_relative_to(root):
        raise G25ReadinessError("PATH_OUTSIDE_PROJECT", portable)
    return candidate


def _repo_file(root: Path, portable: str) -> Path:
    path = _repo_path(root, portable)
    if not path.is_file():
        raise G25ReadinessError("SOURCE_PATH_MISSING", portable)
    return path


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise G25ReadinessError("PATH_OUTSIDE_PROJECT", str(path)) from exc


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _digest(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生成或验证 ARCH-004 G2.5 双线 readiness 证据")
    parser.add_argument("action", choices=("bundle-build", "build", "validate"))
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_EVIDENCE_PATH)
    parser.add_argument("--handoff", type=Path, default=DEFAULT_HANDOFF_PATH)
    parser.add_argument("--bundle-output", type=Path, default=DEFAULT_VALIDATION_BUNDLE_PATH)
    parser.add_argument("--base-commit")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.project_root.resolve()
    policy_path = args.policy if args.policy.is_absolute() else root / args.policy
    output_path = args.output if args.output.is_absolute() else root / args.output
    if args.action == "bundle-build":
        handoff_path = args.handoff if args.handoff.is_absolute() else root / args.handoff
        bundle_path = (
            args.bundle_output if args.bundle_output.is_absolute() else root / args.bundle_output
        )
        output_path = write_source_validation_bundle(
            project_root=root,
            handoff_path=handoff_path,
            output_path=bundle_path,
        )
    elif args.base_commit is None:
        raise G25ReadinessError("BASE_COMMIT_REQUIRED", args.action)
    elif args.action == "build":
        write_g2_5_readiness_evidence(
            output_path,
            project_root=root,
            current_base_commit=args.base_commit,
            policy_path=policy_path,
        )
    else:
        validate_g2_5_readiness_evidence(
            _mapping(json.loads(output_path.read_text(encoding="utf-8")), "evidence"),
            project_root=root,
            current_base_commit=args.base_commit,
            policy_path=policy_path,
        )
    print(
        json.dumps(
            {
                "status": "PASS",
                "gate_id": "ARCH-004G2_PARALLEL_READINESS_GATE",
                "action": args.action,
                "output": _relative(output_path, root),
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
