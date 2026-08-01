from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import yaml

from ai_trading_system.platform.artifacts import write_bytes_atomic

HISTORICAL_COVERAGE_POLICY_SCHEMA_VERSION = "atlas_historical_coverage_policy.v1"
HISTORICAL_COVERAGE_INVENTORY_SCHEMA_VERSION = "atlas_historical_coverage_inventory.v1"
HISTORICAL_COVERAGE_VALIDATION_SCHEMA_VERSION = "atlas_historical_coverage_validation.v1"
DEFAULT_POLICY_REPOSITORY_PATH = "config/atlas/historical_coverage_inventory.yaml"
MANDATORY_EXCLUDED_REPOSITORY_PATH = "docs/research/growth_tilt_owner_diagnosis_pack.md"

ATLAS_SOURCE_BOUND = "ATLAS_SOURCE_BOUND"
REGISTERED_RESEARCH_ARTIFACT = "REGISTERED_RESEARCH_ARTIFACT"
TRACKED_UNREGISTERED_REVIEW_REQUIRED = "TRACKED_UNREGISTERED_REVIEW_REQUIRED"
WILDCARD_DECLARATION_REVIEW_REQUIRED = "WILDCARD_DECLARATION_REVIEW_REQUIRED"
DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT = "DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT"
CLASSIFICATION_CODES = (
    ATLAS_SOURCE_BOUND,
    REGISTERED_RESEARCH_ARTIFACT,
    TRACKED_UNREGISTERED_REVIEW_REQUIRED,
    WILDCARD_DECLARATION_REVIEW_REQUIRED,
    DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT,
)

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_MAGIC = frozenset("*?[")


class HistoricalCoverageInventoryError(ValueError):
    pass


@dataclass(frozen=True)
class HistoricalCoveragePolicy:
    policy_id: str
    report_registry_path: str
    atlas_source_registry_path: str
    tracked_research_roots: tuple[str, ...]
    excluded_paths: tuple[str, ...]
    research_report_group: str


@dataclass(frozen=True)
class HistoricalCoverageInventory:
    exact_commit: str
    policy_id: str
    input_receipts: tuple[Mapping[str, object], ...]
    tracked_path_manifest_sha256: str
    summary: Mapping[str, int]
    report_records: tuple[Mapping[str, object], ...]
    declaration_records: tuple[Mapping[str, object], ...]
    tracked_path_records: tuple[Mapping[str, object], ...]
    atlas_source_crosswalk: tuple[Mapping[str, object], ...]

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": HISTORICAL_COVERAGE_INVENTORY_SCHEMA_VERSION,
            "exact_commit": self.exact_commit,
            "policy_id": self.policy_id,
            "input_receipts": [dict(item) for item in self.input_receipts],
            "tracked_path_manifest_sha256": self.tracked_path_manifest_sha256,
            "summary": dict(self.summary),
            "report_records": [dict(item) for item in self.report_records],
            "declaration_records": [dict(item) for item in self.declaration_records],
            "tracked_path_records": [dict(item) for item in self.tracked_path_records],
            "atlas_source_crosswalk": [dict(item) for item in self.atlas_source_crosswalk],
            "safety": _safety_payload(),
        }

    @property
    def inventory_id(self) -> str:
        digest = hashlib.sha256(_canonical_json_bytes(self._identity_payload())).hexdigest()
        return f"atlas_historical_coverage_inventory_{digest[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"inventory_id": self.inventory_id, **self._identity_payload()}

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalCoverageValidationResult:
    status: str
    inventory_id: str
    exact_commit: str
    checks: tuple[str, ...]
    errors: tuple[str, ...]
    observed_summary: Mapping[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": HISTORICAL_COVERAGE_VALIDATION_SCHEMA_VERSION,
            "status": self.status,
            "inventory_id": self.inventory_id,
            "exact_commit": self.exact_commit,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "observed_summary": dict(self.observed_summary),
            "research_artifact_content_read": False,
            "result_projection_allowed": False,
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalCoverageRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


def build_historical_coverage_inventory(
    *,
    repository_root: Path,
    exact_commit: str,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> HistoricalCoverageInventory:
    _require_exact_commit(exact_commit)
    root = repository_root.resolve()
    policy_path = _safe_repository_path(policy_repository_path)
    policy_bytes = _git_blob_bytes(root, exact_commit, policy_path)
    policy = _load_policy(_yaml_mapping(policy_bytes, "policy"))
    report_registry_bytes = _git_blob_bytes(
        root,
        exact_commit,
        policy.report_registry_path,
    )
    atlas_source_registry_bytes = _git_blob_bytes(
        root,
        exact_commit,
        policy.atlas_source_registry_path,
    )
    tracked_paths = _git_tree_paths(
        root,
        exact_commit,
        roots=policy.tracked_research_roots,
        excluded_paths=policy.excluded_paths,
    )
    return build_historical_coverage_inventory_from_payloads(
        exact_commit=exact_commit,
        policy=policy,
        report_registry=_yaml_mapping(report_registry_bytes, "report_registry"),
        atlas_source_registry=_yaml_mapping(
            atlas_source_registry_bytes,
            "atlas_source_registry",
        ),
        tracked_paths=tracked_paths,
        input_receipts=(
            _input_receipt(policy_path, policy_bytes),
            _input_receipt(policy.report_registry_path, report_registry_bytes),
            _input_receipt(
                policy.atlas_source_registry_path,
                atlas_source_registry_bytes,
            ),
        ),
    )


def build_historical_coverage_inventory_from_payloads(
    *,
    exact_commit: str,
    policy: HistoricalCoveragePolicy,
    report_registry: Mapping[str, object],
    atlas_source_registry: Mapping[str, object],
    tracked_paths: Sequence[str],
    input_receipts: Sequence[Mapping[str, object]],
) -> HistoricalCoverageInventory:
    _require_exact_commit(exact_commit)
    normalized_tracked_paths = _normalized_tracked_paths(
        tracked_paths,
        roots=policy.tracked_research_roots,
        excluded_paths=policy.excluded_paths,
    )
    report_records, declaration_records, exact_path_to_reports = _research_report_records(
        report_registry,
        research_group=policy.research_report_group,
        tracked_paths=frozenset(normalized_tracked_paths),
        excluded_paths=frozenset(policy.excluded_paths),
    )
    atlas_paths_to_refs = _atlas_source_paths(atlas_source_registry)
    tracked_path_records = _tracked_path_records(
        normalized_tracked_paths,
        exact_path_to_reports=exact_path_to_reports,
        atlas_paths_to_refs=atlas_paths_to_refs,
    )
    atlas_source_crosswalk = _atlas_source_crosswalk(
        atlas_paths_to_refs,
        exact_path_to_reports=exact_path_to_reports,
        tracked_paths=frozenset(normalized_tracked_paths),
        roots=policy.tracked_research_roots,
    )
    exact_declarations = tuple(
        item for item in declaration_records if item["declaration_kind"] == "EXACT_PATH"
    )
    wildcard_declarations = tuple(
        item for item in declaration_records if item["declaration_kind"] == "WILDCARD"
    )
    unique_exact_paths = {str(item["artifact_pattern"]) for item in exact_declarations}
    registered_tracked_paths = set(normalized_tracked_paths) & unique_exact_paths
    tracked_atlas_paths = set(normalized_tracked_paths) & set(atlas_paths_to_refs)
    report_list = _mapping_sequence(report_registry.get("reports"), "reports")
    summary = {
        "report_registry_total_count": len(report_list),
        "research_report_count": len(report_records),
        "artifact_declaration_count": len(declaration_records),
        "exact_declaration_count": len(exact_declarations),
        "unique_exact_artifact_path_count": len(unique_exact_paths),
        "wildcard_declaration_count": len(wildcard_declarations),
        "tracked_research_path_count": len(normalized_tracked_paths),
        "tracked_registered_path_count": len(registered_tracked_paths),
        "tracked_unregistered_path_count": len(normalized_tracked_paths)
        - len(registered_tracked_paths),
        "tracked_atlas_source_path_count": len(tracked_atlas_paths),
        "atlas_source_count": len(atlas_paths_to_refs),
        "atlas_source_registered_exact_count": len(set(atlas_paths_to_refs) & unique_exact_paths),
        "atlas_source_outside_tracked_roots_count": sum(
            not _under_any_root(path, policy.tracked_research_roots) for path in atlas_paths_to_refs
        ),
        "declared_non_tracked_or_runtime_count": sum(
            item["classification"] == DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT
            for item in declaration_records
        ),
    }
    manifest_bytes = _tracked_path_manifest_bytes(normalized_tracked_paths)
    normalized_receipts = _normalized_input_receipts(input_receipts)
    return HistoricalCoverageInventory(
        exact_commit=exact_commit,
        policy_id=policy.policy_id,
        input_receipts=normalized_receipts,
        tracked_path_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        summary=summary,
        report_records=report_records,
        declaration_records=declaration_records,
        tracked_path_records=tracked_path_records,
        atlas_source_crosswalk=atlas_source_crosswalk,
    )


def validate_historical_coverage_inventory(
    inventory: HistoricalCoverageInventory,
    *,
    repository_root: Path,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> HistoricalCoverageValidationResult:
    errors: list[str] = []
    checks = (
        "EXACT_COMMIT_BOUND",
        "INPUT_RECEIPTS_BOUND",
        "TRACKED_PATH_MANIFEST_BOUND",
        "REPORT_IDS_UNIQUE",
        "DECLARATIONS_UNIQUE",
        "TRACKED_PATHS_UNIQUE",
        "CLASSIFICATIONS_CLOSED",
        "ATLAS_EXACT_PATH_CROSSWALK",
        "KNOWN_EXCLUSION_ABSENT",
        "RESEARCH_ARTIFACT_CONTENT_NOT_READ",
        "RESULT_PROJECTION_DISABLED",
        "INVESTMENT_CONCLUSION_DISABLED",
        "PRODUCTION_EFFECT_NONE",
        "BROKER_ACTION_NONE",
        "CANONICAL_REBUILD_BYTE_IDENTICAL",
    )
    try:
        rebuilt = build_historical_coverage_inventory(
            repository_root=repository_root,
            exact_commit=inventory.exact_commit,
            policy_repository_path=policy_repository_path,
        )
        if rebuilt.canonical_json_bytes() != inventory.canonical_json_bytes():
            errors.append("INVENTORY_CANONICAL_REBUILD_MISMATCH")
    except (HistoricalCoverageInventoryError, OSError, subprocess.SubprocessError) as exc:
        errors.append(f"INVENTORY_REBUILD_FAILED:{type(exc).__name__}:{exc}")
    serialized = inventory.to_dict()
    if MANDATORY_EXCLUDED_REPOSITORY_PATH in _all_serialized_paths(serialized):
        errors.append("KNOWN_EXCLUSION_LEAKED_INTO_INVENTORY")
    serialized_safety = serialized.get("safety")
    if not isinstance(serialized_safety, Mapping) or dict(serialized_safety) != _safety_payload():
        errors.append("INVENTORY_SAFETY_BOUNDARY_MISMATCH")
    return HistoricalCoverageValidationResult(
        status="PASS" if not errors else "FAIL",
        inventory_id=inventory.inventory_id,
        exact_commit=inventory.exact_commit,
        checks=checks,
        errors=tuple(errors),
        observed_summary=dict(inventory.summary),
    )


def render_historical_coverage_markdown(inventory: HistoricalCoverageInventory) -> str:
    summary = inventory.summary
    unregistered_paths = [
        str(item["path"])
        for item in inventory.tracked_path_records
        if item["classification"] == TRACKED_UNREGISTERED_REVIEW_REQUIRED
    ]
    atlas_rows = "\n".join(
        "|{source_ref_id}|`{source_path}`|{registered}|{tracked}|".format(
            source_ref_id=item["source_ref_id"],
            source_path=item["source_path"],
            registered="是" if item["registered_research_artifact"] else "否",
            tracked="是" if item["in_tracked_research_universe"] else "否",
        )
        for item in inventory.atlas_source_crosswalk
    )
    queue = "\n".join(f"- `{path}`" for path in unregistered_paths)
    return "\n".join(
        [
            "# Atlas 全仓历史研究覆盖 Inventory V1",
            "",
            f"- inventory_id：`{inventory.inventory_id}`",
            f"- exact_commit：`{inventory.exact_commit}`",
            "- 口径：只统计 report 声明与 Git tracked path；不读取研究 artifact 内容。",
            "- historical_repository_coverage_complete：`false`",
            "",
            "## 一眼看懂",
            "",
            "|指标|数量|",
            "|---|---:|",
            f"|Research report entries|{summary['research_report_count']}|",
            f"|Artifact declarations|{summary['artifact_declaration_count']}|",
            f"|Tracked research paths|{summary['tracked_research_path_count']}|",
            f"|Tracked + exact registered|{summary['tracked_registered_path_count']}|",
            f"|Tracked but unregistered review queue|{summary['tracked_unregistered_path_count']}|",
            f"|Current Atlas sources|{summary['atlas_source_count']}|",
            f"|Atlas sources exact-registered|{summary['atlas_source_registered_exact_count']}|",
            "",
            "这些数字说明“证据路径是否被登记”，不说明策略优劣、结果状态或投资价值。",
            "",
            "## 当前 Atlas source crosswalk",
            "",
            "|source_ref_id|path|research registry exact|tracked research universe|",
            "|---|---|---|---|",
            atlas_rows,
            "",
            "## Tracked 但未被 research registry exact 引用",
            "",
            queue or "- 无",
            "",
            "## 安全边界",
            "",
            "- `research_artifact_content_read=false`",
            "- `result_projection_allowed=false`",
            "- `investment_conclusion_generated=false`",
            "- `production_effect=none`",
            "- `broker_action=none`",
            "",
        ]
    )


def write_historical_coverage_inventory_artifacts(
    inventory: HistoricalCoverageInventory,
    output_directory: Path,
    *,
    repository_root: Path,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> tuple[HistoricalCoverageRenderedArtifact, ...]:
    validation = validate_historical_coverage_inventory(
        inventory,
        repository_root=repository_root,
        policy_repository_path=policy_repository_path,
    )
    if validation.status != "PASS":
        raise HistoricalCoverageInventoryError(
            "ATLAS_HISTORICAL_COVERAGE_VALIDATION_FAILED:" + ",".join(validation.errors)
        )
    output_directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "inventory.json": inventory.canonical_json_bytes(),
        "inventory.md": render_historical_coverage_markdown(inventory).encode("utf-8"),
        "validation.json": validation.canonical_json_bytes(),
    }
    artifacts: list[HistoricalCoverageRenderedArtifact] = []
    for name, payload in payloads.items():
        result = write_bytes_atomic(output_directory / name, payload)
        artifacts.append(
            HistoricalCoverageRenderedArtifact(
                path=result.path.as_posix(),
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        )
    return tuple(artifacts)


def _load_policy(payload: Mapping[str, object]) -> HistoricalCoveragePolicy:
    if payload.get("schema_version") != HISTORICAL_COVERAGE_POLICY_SCHEMA_VERSION:
        raise HistoricalCoverageInventoryError("HISTORICAL_COVERAGE_POLICY_SCHEMA_MISMATCH")
    authority = _mapping(payload.get("authority_inputs"), "authority_inputs")
    roots = tuple(
        _safe_repository_path(item)
        for item in _text_sequence(payload.get("tracked_research_roots"), "roots")
    )
    excluded = tuple(
        _safe_repository_path(item)
        for item in _text_sequence(payload.get("excluded_paths"), "excluded_paths")
    )
    if len(roots) != len(set(roots)) or not roots:
        raise HistoricalCoverageInventoryError("HISTORICAL_COVERAGE_ROOTS_INVALID")
    if MANDATORY_EXCLUDED_REPOSITORY_PATH not in excluded:
        raise HistoricalCoverageInventoryError("MANDATORY_KNOWN_EXCLUSION_MISSING")
    declared_codes = tuple(
        _text_sequence(payload.get("classification_codes"), "classification_codes")
    )
    if declared_codes != CLASSIFICATION_CODES:
        raise HistoricalCoverageInventoryError("CLASSIFICATION_CODES_MISMATCH")
    safety = _mapping(payload.get("safety"), "safety")
    if dict(safety) != _safety_payload():
        raise HistoricalCoverageInventoryError("HISTORICAL_COVERAGE_SAFETY_MISMATCH")
    return HistoricalCoveragePolicy(
        policy_id=_required_text(payload, "policy_id"),
        report_registry_path=_safe_repository_path(
            _required_text(authority, "report_registry_path")
        ),
        atlas_source_registry_path=_safe_repository_path(
            _required_text(authority, "atlas_source_registry_path")
        ),
        tracked_research_roots=roots,
        excluded_paths=excluded,
        research_report_group=_required_text(payload, "research_report_group"),
    )


def _research_report_records(
    report_registry: Mapping[str, object],
    *,
    research_group: str,
    tracked_paths: frozenset[str],
    excluded_paths: frozenset[str],
) -> tuple[
    tuple[Mapping[str, object], ...],
    tuple[Mapping[str, object], ...],
    Mapping[str, tuple[str, ...]],
]:
    reports = _mapping_sequence(report_registry.get("reports"), "reports")
    seen_report_ids: set[str] = set()
    report_records: list[Mapping[str, object]] = []
    declaration_records: list[Mapping[str, object]] = []
    exact_path_to_reports: dict[str, set[str]] = defaultdict(set)
    seen_declarations: set[tuple[str, str]] = set()
    for payload in reports:
        report_id = _required_text(payload, "report_id")
        if report_id in seen_report_ids:
            raise HistoricalCoverageInventoryError(f"DUPLICATE_REPORT_ID:{report_id}")
        seen_report_ids.add(report_id)
        if str(payload.get("group", "")) != research_group:
            continue
        artifact_patterns = _text_sequence(payload.get("artifact_globs"), "artifact_globs")
        exact_patterns: list[str] = []
        wildcard_patterns: list[str] = []
        for raw_pattern in artifact_patterns:
            pattern = _safe_repository_pattern(raw_pattern)
            if pattern in excluded_paths:
                continue
            key = (report_id, pattern)
            if key in seen_declarations:
                raise HistoricalCoverageInventoryError(
                    f"DUPLICATE_ARTIFACT_DECLARATION:{report_id}:{pattern}"
                )
            seen_declarations.add(key)
            is_wildcard = any(character in pattern for character in _GLOB_MAGIC)
            if is_wildcard:
                wildcard_patterns.append(pattern)
                classification = WILDCARD_DECLARATION_REVIEW_REQUIRED
                declaration_kind = "WILDCARD"
            else:
                exact_patterns.append(pattern)
                exact_path_to_reports[pattern].add(report_id)
                classification = (
                    REGISTERED_RESEARCH_ARTIFACT
                    if pattern in tracked_paths
                    else DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT
                )
                declaration_kind = "EXACT_PATH"
            declaration_records.append(
                {
                    "report_id": report_id,
                    "artifact_pattern": pattern,
                    "declaration_kind": declaration_kind,
                    "classification": classification,
                }
            )
        report_records.append(
            {
                "report_id": report_id,
                "title": _required_text(payload, "title"),
                "command": _required_text(payload, "command"),
                "owner_action": _required_text(payload, "owner_action"),
                "exact_artifact_paths": sorted(set(exact_patterns), key=str.casefold),
                "wildcard_artifact_patterns": sorted(set(wildcard_patterns), key=str.casefold),
            }
        )
    report_records.sort(key=lambda item: str(item["report_id"]).casefold())
    declaration_records.sort(
        key=lambda item: (
            str(item["artifact_pattern"]).casefold(),
            str(item["report_id"]).casefold(),
        )
    )
    normalized_mapping = {
        path: tuple(sorted(report_ids, key=str.casefold))
        for path, report_ids in exact_path_to_reports.items()
    }
    return tuple(report_records), tuple(declaration_records), normalized_mapping


def _atlas_source_paths(
    atlas_source_registry: Mapping[str, object],
) -> Mapping[str, tuple[str, ...]]:
    sources = _mapping_sequence(atlas_source_registry.get("sources"), "sources")
    path_to_refs: dict[str, list[str]] = defaultdict(list)
    seen_ids: set[str] = set()
    for source in sources:
        source_ref_id = _required_text(source, "source_ref_id")
        if source_ref_id in seen_ids:
            raise HistoricalCoverageInventoryError(f"DUPLICATE_ATLAS_SOURCE_REF_ID:{source_ref_id}")
        seen_ids.add(source_ref_id)
        source_path = _safe_repository_path(_required_text(source, "source_path"))
        path_to_refs[source_path].append(source_ref_id)
    return {
        path: tuple(sorted(refs, key=str.casefold))
        for path, refs in sorted(path_to_refs.items(), key=lambda item: item[0].casefold())
    }


def _tracked_path_records(
    tracked_paths: Sequence[str],
    *,
    exact_path_to_reports: Mapping[str, tuple[str, ...]],
    atlas_paths_to_refs: Mapping[str, tuple[str, ...]],
) -> tuple[Mapping[str, object], ...]:
    records: list[Mapping[str, object]] = []
    for path in tracked_paths:
        report_ids = exact_path_to_reports.get(path, ())
        source_ref_ids = atlas_paths_to_refs.get(path, ())
        classification = (
            ATLAS_SOURCE_BOUND
            if source_ref_ids
            else (
                REGISTERED_RESEARCH_ARTIFACT if report_ids else TRACKED_UNREGISTERED_REVIEW_REQUIRED
            )
        )
        records.append(
            {
                "path": path,
                "classification": classification,
                "registered_research_artifact": bool(report_ids),
                "atlas_source_bound": bool(source_ref_ids),
                "report_ids": list(report_ids),
                "atlas_source_ref_ids": list(source_ref_ids),
            }
        )
    return tuple(records)


def _atlas_source_crosswalk(
    atlas_paths_to_refs: Mapping[str, tuple[str, ...]],
    *,
    exact_path_to_reports: Mapping[str, tuple[str, ...]],
    tracked_paths: frozenset[str],
    roots: Sequence[str],
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for path, source_ref_ids in atlas_paths_to_refs.items():
        for source_ref_id in source_ref_ids:
            rows.append(
                {
                    "source_ref_id": source_ref_id,
                    "source_path": path,
                    "classification": ATLAS_SOURCE_BOUND,
                    "registered_research_artifact": path in exact_path_to_reports,
                    "report_ids": list(exact_path_to_reports.get(path, ())),
                    "under_declared_research_root": _under_any_root(path, roots),
                    "in_tracked_research_universe": path in tracked_paths,
                }
            )
    rows.sort(key=lambda item: str(item["source_ref_id"]).casefold())
    return tuple(rows)


def _normalized_tracked_paths(
    tracked_paths: Sequence[str],
    *,
    roots: Sequence[str],
    excluded_paths: Sequence[str],
) -> tuple[str, ...]:
    excluded = set(excluded_paths)
    normalized: list[str] = []
    seen_casefold: set[str] = set()
    for raw_path in tracked_paths:
        path = _safe_repository_path(str(raw_path))
        if path in excluded:
            continue
        if not _under_any_root(path, roots):
            raise HistoricalCoverageInventoryError(f"TRACKED_PATH_OUTSIDE_ROOTS:{path}")
        folded = path.casefold()
        if folded in seen_casefold:
            raise HistoricalCoverageInventoryError(f"DUPLICATE_TRACKED_PATH:{path}")
        seen_casefold.add(folded)
        normalized.append(path)
    normalized.sort(key=str.casefold)
    return tuple(normalized)


def _normalized_input_receipts(
    receipts: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    normalized: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for receipt in receipts:
        path = _safe_repository_path(_required_text(receipt, "path"))
        if path in seen:
            raise HistoricalCoverageInventoryError(f"DUPLICATE_INPUT_RECEIPT:{path}")
        seen.add(path)
        sha256 = _required_text(receipt, "sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise HistoricalCoverageInventoryError(f"INPUT_SHA256_INVALID:{path}")
        normalized.append({"path": path, "sha256": sha256})
    normalized.sort(key=lambda item: str(item["path"]).casefold())
    return tuple(normalized)


def _git_blob_bytes(repository_root: Path, exact_commit: str, repository_path: str) -> bytes:
    if repository_path == MANDATORY_EXCLUDED_REPOSITORY_PATH:
        raise HistoricalCoverageInventoryError("KNOWN_EXCLUSION_BLOB_READ_FORBIDDEN")
    result = subprocess.run(
        ["git", "cat-file", "blob", f"{exact_commit}:{repository_path}"],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        error_text = result.stderr.decode("utf-8", "replace").strip()
        raise HistoricalCoverageInventoryError(
            f"GIT_BLOB_READ_FAILED:{repository_path}:{error_text}"
        )
    return result.stdout


def _git_tree_paths(
    repository_root: Path,
    exact_commit: str,
    *,
    roots: Sequence[str],
    excluded_paths: Sequence[str],
) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "-z", "--name-only", exact_commit, "--", *roots],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise HistoricalCoverageInventoryError(
            "GIT_TREE_READ_FAILED:" + result.stderr.decode("utf-8", "replace").strip()
        )
    excluded = set(excluded_paths)
    paths = tuple(
        path
        for raw in result.stdout.split(b"\0")
        if raw
        for path in (raw.decode("utf-8"),)
        if path not in excluded
    )
    return _normalized_tracked_paths(paths, roots=roots, excluded_paths=excluded_paths)


def _input_receipt(path: str, payload: bytes) -> Mapping[str, object]:
    return {"path": path, "sha256": hashlib.sha256(payload).hexdigest()}


def _tracked_path_manifest_bytes(paths: Sequence[str]) -> bytes:
    return ("\n".join(paths) + "\n").encode("utf-8")


def _yaml_mapping(payload: bytes, name: str) -> Mapping[str, object]:
    loaded = yaml.safe_load(payload.decode("utf-8"))
    return _mapping(loaded, name)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise HistoricalCoverageInventoryError(f"MAPPING_REQUIRED:{field}")
    return value


def _mapping_sequence(
    value: object,
    field: str,
) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise HistoricalCoverageInventoryError(f"LIST_REQUIRED:{field}")
    items: list[Mapping[str, object]] = []
    for item in value:
        items.append(_mapping(item, field))
    return tuple(items)


def _text_sequence(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise HistoricalCoverageInventoryError(f"TEXT_LIST_REQUIRED:{field}")
    texts = tuple(str(item).strip() for item in value)
    if not all(texts):
        raise HistoricalCoverageInventoryError(f"TEXT_LIST_EMPTY:{field}")
    return texts


def _required_text(payload: Mapping[str, object], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise HistoricalCoverageInventoryError(f"TEXT_REQUIRED:{field}")
    return value


def _safe_repository_path(value: str) -> str:
    path_text = str(value).strip()
    if not path_text or "\\" in path_text or any(char in path_text for char in _GLOB_MAGIC):
        raise HistoricalCoverageInventoryError(f"REPOSITORY_PATH_INVALID:{value}")
    path = PurePosixPath(path_text)
    if path.is_absolute() or "." in path.parts or ".." in path.parts:
        raise HistoricalCoverageInventoryError(f"REPOSITORY_PATH_INVALID:{value}")
    return path.as_posix()


def _safe_repository_pattern(value: str) -> str:
    pattern = str(value).strip()
    if not pattern or "\\" in pattern:
        raise HistoricalCoverageInventoryError(f"REPOSITORY_PATTERN_INVALID:{value}")
    path = PurePosixPath(pattern)
    if path.is_absolute() or "." in path.parts or ".." in path.parts:
        raise HistoricalCoverageInventoryError(f"REPOSITORY_PATTERN_INVALID:{value}")
    return path.as_posix()


def _under_any_root(path: str, roots: Sequence[str]) -> bool:
    candidate = PurePosixPath(path)
    return any(
        candidate == PurePosixPath(root) or PurePosixPath(root) in candidate.parents
        for root in roots
    )


def _all_serialized_paths(payload: Mapping[str, object]) -> set[str]:
    values: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, str):
            values.add(value)
        elif isinstance(value, Mapping):
            for child in value.values():
                visit(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                visit(child)

    visit(payload)
    return values


def _safety_payload() -> dict[str, object]:
    return {
        "historical_repository_coverage_complete": False,
        "research_artifact_content_read": False,
        "result_projection_allowed": False,
        "investment_conclusion_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _require_exact_commit(value: str) -> None:
    if not _COMMIT_PATTERN.fullmatch(value):
        raise HistoricalCoverageInventoryError("EXACT_COMMIT_INVALID")


def _canonical_json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


__all__ = [
    "ATLAS_SOURCE_BOUND",
    "CLASSIFICATION_CODES",
    "DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT",
    "HISTORICAL_COVERAGE_INVENTORY_SCHEMA_VERSION",
    "HISTORICAL_COVERAGE_POLICY_SCHEMA_VERSION",
    "HISTORICAL_COVERAGE_VALIDATION_SCHEMA_VERSION",
    "HistoricalCoverageInventory",
    "HistoricalCoverageInventoryError",
    "HistoricalCoveragePolicy",
    "HistoricalCoverageRenderedArtifact",
    "HistoricalCoverageValidationResult",
    "REGISTERED_RESEARCH_ARTIFACT",
    "TRACKED_UNREGISTERED_REVIEW_REQUIRED",
    "WILDCARD_DECLARATION_REVIEW_REQUIRED",
    "build_historical_coverage_inventory",
    "build_historical_coverage_inventory_from_payloads",
    "render_historical_coverage_markdown",
    "validate_historical_coverage_inventory",
    "write_historical_coverage_inventory_artifacts",
]
