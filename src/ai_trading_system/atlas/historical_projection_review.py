"""Deterministic, review-only Atlas historical projection pack.

This module proposes a future node/result shape for owner review.  It never
mutates the canonical Atlas registry, snapshot, cited-query page, or investment
state.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from ai_trading_system.platform.artifacts import write_bytes_atomic

from .historical_source_adapters import (
    HistoricalSourceAdapterRecord,
    build_historical_source_adapter_bundle,
)
from .snapshot_builder import build_atlas_bundle

POLICY_SCHEMA_VERSION = "atlas_historical_projection_review_policy.v1"
PACK_SCHEMA_VERSION = "atlas_historical_projection_review_pack.v1"
VALIDATION_SCHEMA_VERSION = "atlas_historical_projection_review_validation.v1"
DEFAULT_POLICY_PATH = "config/atlas/historical_projection_review.yaml"
PRIMARY_RESEARCH_START = "2021-02-22"
FORBIDDEN_CANDIDATE_FAMILY_ID = "atlas_historical_candidate_next_roadmap_v1"
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COUNT_FIELDS = ("sources", "nodes", "edges", "results", "attributions")
_SAFETY = {
    "review_only": True,
    "source_registration_performed": False,
    "node_projection_performed": False,
    "result_projection_performed": False,
    "page_projection_performed": False,
    "current_snapshot_mutated": False,
    "investment_conclusion_generated": False,
    "production_effect": "none",
    "broker_action": "none",
}


class HistoricalProjectionReviewError(ValueError):
    """Raised when the review-only projection contract cannot be proven."""


@dataclass(frozen=True)
class HistoricalProjectionReviewPack:
    policy_id: str
    owner_decision: str
    evidence_exact_commit: str
    adapter_bundle_id: str
    current_snapshot_id: str
    primary_research_start: str
    policy_receipt: Mapping[str, object]
    canonical_page_receipt: Mapping[str, object]
    current_counts: Mapping[str, int]
    candidate_counts: Mapping[str, int]
    group_node: Mapping[str, object]
    candidate_nodes: tuple[Mapping[str, object], ...]
    candidate_edges: tuple[Mapping[str, object], ...]
    candidate_results: tuple[Mapping[str, object], ...]
    candidate_attributions: tuple[Mapping[str, object], ...]
    records: tuple[Mapping[str, object], ...]

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": PACK_SCHEMA_VERSION,
            "policy_id": self.policy_id,
            "owner_decision": self.owner_decision,
            "evidence_exact_commit": self.evidence_exact_commit,
            "adapter_bundle_id": self.adapter_bundle_id,
            "current_snapshot_id": self.current_snapshot_id,
            "primary_research_start": self.primary_research_start,
            "policy_receipt": dict(self.policy_receipt),
            "canonical_page_receipt": dict(self.canonical_page_receipt),
            "current_counts": dict(self.current_counts),
            "candidate_counts": dict(self.candidate_counts),
            "group_node": dict(self.group_node),
            "candidate_nodes": [dict(item) for item in self.candidate_nodes],
            "candidate_edges": [dict(item) for item in self.candidate_edges],
            "candidate_results": [dict(item) for item in self.candidate_results],
            "candidate_attributions": [dict(item) for item in self.candidate_attributions],
            "records": [dict(item) for item in self.records],
            "safety": dict(_SAFETY),
        }

    @property
    def review_pack_id(self) -> str:
        digest = sha256(_canonical_json_bytes(self._identity_payload())).hexdigest()
        return f"atlas_historical_projection_review_{digest[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"review_pack_id": self.review_pack_id, **self._identity_payload()}

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalProjectionReviewValidation:
    status: str
    review_pack_id: str
    evidence_exact_commit: str
    checks: tuple[str, ...]
    errors: tuple[str, ...]
    observed_counts: Mapping[str, Mapping[str, int]]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "status": self.status,
            "review_pack_id": self.review_pack_id,
            "evidence_exact_commit": self.evidence_exact_commit,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "observed_counts": {
                name: dict(counts) for name, counts in self.observed_counts.items()
            },
            **dict(_SAFETY),
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalProjectionRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


def build_historical_projection_review(
    *,
    repository_root: Path,
    evidence_exact_commit: str,
    canonical_page_file: Path,
    policy_path: Path | None = None,
) -> HistoricalProjectionReviewPack:
    """Build a review model without projecting anything into current Atlas state."""

    _require_exact_commit(evidence_exact_commit)
    root = repository_root.resolve()
    selected_policy = policy_path or root / DEFAULT_POLICY_PATH
    policy_bytes = _read_inside_repository(root, selected_policy)
    policy = _yaml_mapping(policy_bytes, "policy")
    _validate_policy(policy)

    source_registry_path = _safe_repository_path(_required_text(policy, "source_registry_path"))
    adapter_registry_path = _safe_repository_path(_required_text(policy, "adapter_registry_path"))
    _require_path_unchanged(root, evidence_exact_commit, source_registry_path)
    _require_path_unchanged(root, evidence_exact_commit, adapter_registry_path)

    adapter_bundle = build_historical_source_adapter_bundle(
        repository_root=root,
        exact_commit=evidence_exact_commit,
        adapter_registry_path=root / PurePosixPath(adapter_registry_path),
        source_registry_path=root / PurePosixPath(source_registry_path),
    )
    atlas_bundle = build_atlas_bundle(
        repository_root=root,
        exact_commit=evidence_exact_commit,
        registry_path=root / PurePosixPath(source_registry_path),
    )
    if adapter_bundle.primary_research_start != PRIMARY_RESEARCH_START:
        raise HistoricalProjectionReviewError("PRIMARY_RESEARCH_START_DRIFT")
    if atlas_bundle.primary_research_start != PRIMARY_RESEARCH_START:
        raise HistoricalProjectionReviewError("CURRENT_ATLAS_PRIMARY_START_DRIFT")

    current_counts = {
        "sources": len(atlas_bundle.snapshot.sources),
        "nodes": len(atlas_bundle.snapshot.nodes),
        "edges": len(atlas_bundle.snapshot.edges),
        "results": len(atlas_bundle.snapshot.results),
        "attributions": len(atlas_bundle.snapshot.attributions),
    }
    expected_current = _count_mapping(policy, "expected_current_counts")
    if current_counts != expected_current:
        raise HistoricalProjectionReviewError(
            f"CURRENT_COUNT_MISMATCH:{current_counts!r}:{expected_current!r}"
        )

    record_policies = _mapping_sequence(policy, "records")
    records_by_source = {record.source_ref_id: record for record in adapter_bundle.records}
    if len(records_by_source) != len(adapter_bundle.records):
        raise HistoricalProjectionReviewError("HISTORICAL_SOURCE_REF_DUPLICATE")
    expected_sources = tuple(_required_text(item, "source_ref_id") for item in record_policies)
    if len(set(expected_sources)) != len(expected_sources):
        raise HistoricalProjectionReviewError("POLICY_SOURCE_REF_DUPLICATE")
    if set(records_by_source) != set(expected_sources):
        raise HistoricalProjectionReviewError("HISTORICAL_SOURCE_SET_MISMATCH")
    if any(
        record.candidate_family_id == FORBIDDEN_CANDIDATE_FAMILY_ID
        for record in adapter_bundle.records
    ):
        raise HistoricalProjectionReviewError("FORBIDDEN_ROADMAP_RECORD_PRESENT")

    group_node = _string_mapping(policy, "group_node")
    root_edge = _string_mapping(policy, "root_edge")
    if root_edge["to_node_id"] != group_node["node_id"]:
        raise HistoricalProjectionReviewError("ROOT_EDGE_GROUP_BINDING_MISMATCH")
    existing_ids = _existing_id_sets(atlas_bundle.snapshot)
    candidate_nodes: list[Mapping[str, object]] = [dict(group_node)]
    candidate_edges: list[Mapping[str, object]] = [dict(root_edge)]
    candidate_results: list[Mapping[str, object]] = []
    candidate_attributions: list[Mapping[str, object]] = []
    review_records: list[Mapping[str, object]] = []

    for item in record_policies:
        source_ref_id = _required_text(item, "source_ref_id")
        record = records_by_source[source_ref_id]
        review_record = _build_review_record(item, record)
        review_records.append(review_record)
        candidate_nodes.append(
            {
                "node_id": review_record["candidate_node_id"],
                "node_kind": review_record["candidate_node_kind"],
                "assertion_kind": review_record["assertion_kind"],
                "raw_status": review_record["proposed_raw_status"],
                "title": review_record["title"],
                "summary": review_record["reader_summary"],
                "source_ref_ids": [source_ref_id],
                "investment_facing": False,
            }
        )
        candidate_edges.append(
            {
                "edge_id": review_record["candidate_edge_id"],
                "from_node_id": group_node["node_id"],
                "to_node_id": review_record["candidate_node_id"],
                "edge_kind": "CONTAINS",
            }
        )
        candidate_results.append(
            {
                "result_id": review_record["candidate_result_id"],
                "node_id": review_record["candidate_node_id"],
                "raw_status": review_record["proposed_raw_status"],
                "display_status": review_record["proposed_display_status"],
                "title": review_record["title"],
                "summary": review_record["reader_summary"],
                "source_ref_ids": [source_ref_id],
                "investment_facing": False,
            }
        )
        candidate_attributions.append(
            {
                "attribution_id": review_record["candidate_attribution_id"],
                "result_id": review_record["candidate_result_id"],
                "source_ref_id": source_ref_id,
                "direction": "NEUTRAL",
                "assertion_kind": review_record["assertion_kind"],
                "summary": "仅绑定历史来源与候选结果，不表达当前有效性或投资方向。",
            }
        )

    _validate_candidate_id_sets(
        existing_ids=existing_ids,
        nodes=candidate_nodes,
        edges=candidate_edges,
        results=candidate_results,
        attributions=candidate_attributions,
    )
    if any(edge.get("edge_kind") != "CONTAINS" for edge in candidate_edges):
        raise HistoricalProjectionReviewError("CANDIDATE_EDGE_KIND_NOT_CONTAINS")
    if any(item.get("direction") != "NEUTRAL" for item in candidate_attributions):
        raise HistoricalProjectionReviewError("CANDIDATE_ATTRIBUTION_NOT_NEUTRAL")

    candidate_counts = {
        "sources": current_counts["sources"],
        "nodes": current_counts["nodes"] + len(candidate_nodes),
        "edges": current_counts["edges"] + len(candidate_edges),
        "results": current_counts["results"] + len(candidate_results),
        "attributions": current_counts["attributions"] + len(candidate_attributions),
    }
    expected_candidate = _count_mapping(policy, "expected_candidate_counts")
    if candidate_counts != expected_candidate:
        raise HistoricalProjectionReviewError(
            f"CANDIDATE_COUNT_MISMATCH:{candidate_counts!r}:{expected_candidate!r}"
        )

    canonical_page_receipt = _canonical_page_receipt(
        canonical_page_file=canonical_page_file,
        policy=_mapping(policy.get("canonical_page"), "canonical_page"),
    )
    relative_policy_path = selected_policy.resolve().relative_to(root).as_posix()
    return HistoricalProjectionReviewPack(
        policy_id=_required_text(policy, "policy_id"),
        owner_decision=_required_text(policy, "owner_decision"),
        evidence_exact_commit=evidence_exact_commit,
        adapter_bundle_id=adapter_bundle.bundle_id,
        current_snapshot_id=atlas_bundle.snapshot.snapshot_id,
        primary_research_start=PRIMARY_RESEARCH_START,
        policy_receipt=_receipt(relative_policy_path, policy_bytes),
        canonical_page_receipt=canonical_page_receipt,
        current_counts=current_counts,
        candidate_counts=candidate_counts,
        group_node=group_node,
        candidate_nodes=tuple(candidate_nodes),
        candidate_edges=tuple(candidate_edges),
        candidate_results=tuple(candidate_results),
        candidate_attributions=tuple(candidate_attributions),
        records=tuple(review_records),
    )


def validate_historical_projection_review(
    review_pack: HistoricalProjectionReviewPack,
    *,
    repository_root: Path,
    canonical_page_file: Path,
    policy_path: Path | None = None,
) -> HistoricalProjectionReviewValidation:
    checks = (
        "EXACT_COMMIT_AND_TYPED_ADAPTER_BOUND",
        "FIVE_HISTORICAL_SOURCE_SET_EXACT",
        "CURRENT_COUNTS_BOUND",
        "CANDIDATE_COUNTS_BOUND",
        "CANDIDATE_IDS_UNIQUE_AND_NON_COLLIDING",
        "CONTAINS_ONLY_GRAPH_BOUND",
        "ORIGINAL_AND_PROPOSED_STATUS_MAPPING_BOUND",
        "ALL_DISPLAY_STATUS_LIMITED",
        "NEUTRAL_PROVENANCE_ONLY",
        "DQ_AND_WINDOW_DISTINCTIONS_PRESERVED",
        "PRIMARY_RESEARCH_START_2021_02_22",
        "CANONICAL_PAGE_IDENTITY_UNCHANGED",
        "ROADMAP_EXCLUDED",
        "CURRENT_PROJECTION_DISABLED",
        "CANONICAL_REBUILD_BYTE_IDENTICAL",
        "PRODUCTION_AND_BROKER_NONE",
    )
    errors: list[str] = []
    try:
        rebuilt = build_historical_projection_review(
            repository_root=repository_root,
            evidence_exact_commit=review_pack.evidence_exact_commit,
            canonical_page_file=canonical_page_file,
            policy_path=policy_path,
        )
        if rebuilt.canonical_json_bytes() != review_pack.canonical_json_bytes():
            errors.append("REVIEW_PACK_CANONICAL_REBUILD_MISMATCH")
    except (HistoricalProjectionReviewError, OSError, subprocess.SubprocessError) as exc:
        errors.append(f"REVIEW_PACK_REBUILD_FAILED:{type(exc).__name__}:{exc}")
    if review_pack.primary_research_start != PRIMARY_RESEARCH_START:
        errors.append("PRIMARY_RESEARCH_START_MISMATCH")
    if len(review_pack.records) != 5:
        errors.append("RECORD_COUNT_NOT_FIVE")
    if any(item.get("proposed_display_status") != "LIMITED" for item in review_pack.records):
        errors.append("DISPLAY_STATUS_NOT_ALL_LIMITED")
    if any(item.get("direction") != "NEUTRAL" for item in review_pack.candidate_attributions):
        errors.append("ATTRIBUTION_DIRECTION_NOT_ALL_NEUTRAL")
    if review_pack.to_dict().get("safety") != _SAFETY:
        errors.append("SAFETY_BOUNDARY_MISMATCH")
    return HistoricalProjectionReviewValidation(
        status="PASS" if not errors else "FAIL",
        review_pack_id=review_pack.review_pack_id,
        evidence_exact_commit=review_pack.evidence_exact_commit,
        checks=checks,
        errors=tuple(errors),
        observed_counts={
            "current": dict(review_pack.current_counts),
            "candidate": dict(review_pack.candidate_counts),
        },
    )


def render_historical_projection_review_markdown(
    review_pack: HistoricalProjectionReviewPack,
) -> str:
    count_rows = [
        f"|{name}|{review_pack.current_counts[name]}|{review_pack.candidate_counts[name]}|"
        for name in _COUNT_FIELDS
    ]
    record_rows = [
        "|`{source}`|{title}|`{original}`|`{raw}`|`{display}`|{dq}|".format(
            source=item["source_ref_id"],
            title=item["title"],
            original=item["original_raw_status"],
            raw=item["proposed_raw_status"],
            display=item["proposed_display_status"],
            dq=item["data_quality_label"],
        )
        for item in review_pack.records
    ]
    return "\n".join(
        [
            "# Atlas 历史投影审阅包 V1",
            "",
            f"- review_pack_id：`{review_pack.review_pack_id}`",
            f"- evidence_exact_commit：`{review_pack.evidence_exact_commit}`",
            f"- current_snapshot_id：`{review_pack.current_snapshot_id}`",
            (
                "- 边界：这是独立审阅包，不是 current Atlas 结果页；"
                "未执行 node/result/page projection。"
            ),
            "",
            "## Current 与候选结构",
            "",
            "|实体|current|candidate|",
            "|---|---:|---:|",
            *count_rows,
            "",
            "## 五份历史材料",
            "",
            "|source|读者标题|original raw|proposed raw|display|DQ|",
            "|---|---|---|---|---|---|",
            *record_rows,
            "",
            "## 审阅问题",
            "",
            "- stable IDs 与历史支线位置是否清晰？",
            "- original/proposed/display 三层状态是否足够避免误读？",
            "- 五张卡片的信息密度与限制语言是否适合金融知识较少的读者？",
            "- current→candidate 数量变化是否可接受？",
            "- 是否批准后续独立 canonical projection 任务？本包本身不构成该批准。",
            "",
            "## 安全边界",
            "",
            "- `node_projection_performed=false`",
            "- `result_projection_performed=false`",
            "- `page_projection_performed=false`",
            "- `investment_conclusion_generated=false`",
            "- `production_effect=none`",
            "- `broker_action=none`",
            "",
        ]
    )


def write_historical_projection_review_artifacts(
    review_pack: HistoricalProjectionReviewPack,
    output_directory: Path,
    *,
    repository_root: Path,
    canonical_page_file: Path,
    policy_path: Path | None = None,
) -> tuple[HistoricalProjectionRenderedArtifact, ...]:
    from .historical_projection_review_renderer import (
        render_historical_projection_review_html,
    )

    validation = validate_historical_projection_review(
        review_pack,
        repository_root=repository_root,
        canonical_page_file=canonical_page_file,
        policy_path=policy_path,
    )
    if validation.status != "PASS":
        raise HistoricalProjectionReviewError(
            "HISTORICAL_PROJECTION_REVIEW_VALIDATION_FAILED:" + ",".join(validation.errors)
        )
    payloads = {
        "index.html": render_historical_projection_review_html(review_pack).encode("utf-8"),
        "review_pack.json": review_pack.canonical_json_bytes(),
        "review_pack.md": render_historical_projection_review_markdown(review_pack).encode("utf-8"),
        "validation.json": validation.canonical_json_bytes(),
    }
    output_directory.mkdir(parents=True, exist_ok=True)
    artifacts: list[HistoricalProjectionRenderedArtifact] = []
    for name, payload in payloads.items():
        result = write_bytes_atomic(output_directory / name, payload)
        artifacts.append(
            HistoricalProjectionRenderedArtifact(
                path=result.path.as_posix(),
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        )
    return tuple(artifacts)


def _build_review_record(
    policy: Mapping[str, object],
    record: HistoricalSourceAdapterRecord,
) -> Mapping[str, object]:
    role_code = _required_text(policy, "role_code")
    if record.role.value != role_code:
        raise HistoricalProjectionReviewError(f"ROLE_MISMATCH:{record.source_ref_id}")
    expected_status = _required_text(policy, "expected_original_status")
    if record.raw_status != expected_status:
        raise HistoricalProjectionReviewError(f"ORIGINAL_STATUS_MISMATCH:{record.source_ref_id}")
    _validate_record_safety(record)
    proposed_raw = _required_text(policy, "proposed_raw_status")
    proposed_display = _required_text(policy, "proposed_display_status")
    if proposed_display != "LIMITED" or proposed_raw not in {"PASS", "LIMITED"}:
        raise HistoricalProjectionReviewError(f"STATUS_MAPPING_NOT_ALLOWED:{record.source_ref_id}")
    if record.role.value == "PROGRAM_SNAPSHOT" and proposed_raw != "LIMITED":
        raise HistoricalProjectionReviewError("PROGRAM_SNAPSHOT_RAW_STATUS_MUST_BE_LIMITED")
    if record.role.value != "PROGRAM_SNAPSHOT" and proposed_raw != "PASS":
        raise HistoricalProjectionReviewError(
            f"COMPLETED_RECORD_RAW_STATUS_MUST_BE_PASS:{record.source_ref_id}"
        )
    dq_payload = record.data_quality.to_dict() if record.data_quality is not None else None
    windows = [window.to_dict() for window in record.windows]
    return {
        "source_ref_id": record.source_ref_id,
        "candidate_family_id": record.candidate_family_id,
        "role_code": role_code,
        "source_path": record.source_path,
        "artifact_identity": record.artifact_identity,
        "git_blob_sha1": record.git_blob_sha1,
        "content_sha256": record.content_sha256,
        "as_of": record.as_of,
        "original_raw_status": record.raw_status,
        "proposed_raw_status": proposed_raw,
        "proposed_display_status": proposed_display,
        "mapping_rationale": _required_text(policy, "mapping_rationale"),
        "title": _required_text(policy, "title"),
        "reader_summary": _required_text(policy, "reader_summary"),
        "key_result": record.reader_brief.key_result,
        "blocking_issues": record.reader_brief.blocking_issues,
        "next_action": record.reader_brief.next_action,
        "data_quality": dq_payload,
        "data_quality_label": (
            str(dq_payload["status"]) if dq_payload is not None else "未提供（null）"
        ),
        "windows": windows,
        "limitations": list(record.limitations),
        "candidate_node_id": _required_text(policy, "node_id"),
        "candidate_node_kind": _required_text(policy, "node_kind"),
        "assertion_kind": _required_text(policy, "assertion_kind"),
        "candidate_edge_id": _required_text(policy, "edge_id"),
        "candidate_result_id": _required_text(policy, "result_id"),
        "candidate_attribution_id": _required_text(policy, "attribution_id"),
        "attribution_direction": "NEUTRAL",
        "investment_facing": False,
    }


def _validate_record_safety(record: HistoricalSourceAdapterRecord) -> None:
    expected = {
        "research_context_complete": False,
        "data_quality_ready": False,
        "legacy_history_partial": True,
        "research_only": True,
        "manual_review_only": True,
        "historical_record": True,
        "current_primary_default": False,
        "result_projection_allowed": False,
        "page_projection_allowed": False,
        "investment_conclusion_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    for field, value in expected.items():
        if getattr(record, field) != value:
            raise HistoricalProjectionReviewError(
                f"HISTORICAL_RECORD_SAFETY_MISMATCH:{record.source_ref_id}:{field}"
            )


def _validate_policy(policy: Mapping[str, object]) -> None:
    if _required_text(policy, "schema_version") != POLICY_SCHEMA_VERSION:
        raise HistoricalProjectionReviewError("POLICY_SCHEMA_UNSUPPORTED")
    if _required_text(policy, "primary_research_start") != PRIMARY_RESEARCH_START:
        raise HistoricalProjectionReviewError("POLICY_PRIMARY_RESEARCH_START_DRIFT")
    forbidden = _text_sequence(policy.get("forbidden_candidate_family_ids"), "forbidden")
    if forbidden != (FORBIDDEN_CANDIDATE_FAMILY_ID,):
        raise HistoricalProjectionReviewError("FORBIDDEN_CANDIDATE_SET_MISMATCH")
    safety = _mapping(policy.get("safety"), "safety")
    if dict(safety) != _SAFETY:
        raise HistoricalProjectionReviewError("POLICY_SAFETY_BOUNDARY_MISMATCH")


def _canonical_page_receipt(
    *, canonical_page_file: Path, policy: Mapping[str, object]
) -> Mapping[str, object]:
    payload = canonical_page_file.read_bytes()
    expected_sha = _required_text(policy, "expected_sha256")
    if not _SHA256_RE.fullmatch(expected_sha):
        raise HistoricalProjectionReviewError("CANONICAL_PAGE_EXPECTED_SHA_INVALID")
    expected_size = _positive_int(policy, "expected_size_bytes")
    observed_sha = sha256(payload).hexdigest()
    if len(payload) != expected_size or observed_sha != expected_sha:
        raise HistoricalProjectionReviewError("CANONICAL_PAGE_IDENTITY_MISMATCH")
    return {
        "repository_path": _safe_repository_path(_required_text(policy, "repository_path")),
        "size_bytes": len(payload),
        "sha256": observed_sha,
        "unchanged": True,
    }


def _existing_id_sets(snapshot: Any) -> Mapping[str, set[str]]:
    return {
        "nodes": {item.node_id for item in snapshot.nodes},
        "edges": {item.edge_id for item in snapshot.edges},
        "results": {item.result_id for item in snapshot.results},
        "attributions": {item.attribution_id for item in snapshot.attributions},
    }


def _validate_candidate_id_sets(
    *,
    existing_ids: Mapping[str, set[str]],
    nodes: Sequence[Mapping[str, object]],
    edges: Sequence[Mapping[str, object]],
    results: Sequence[Mapping[str, object]],
    attributions: Sequence[Mapping[str, object]],
) -> None:
    selections = {
        "nodes": (nodes, "node_id"),
        "edges": (edges, "edge_id"),
        "results": (results, "result_id"),
        "attributions": (attributions, "attribution_id"),
    }
    for kind, (items, field) in selections.items():
        values = [_required_text(item, field) for item in items]
        if len(values) != len(set(values)):
            raise HistoricalProjectionReviewError(f"CANDIDATE_ID_DUPLICATE:{kind}")
        collisions = set(values) & existing_ids[kind]
        if collisions:
            raise HistoricalProjectionReviewError(
                f"CANDIDATE_ID_COLLISION:{kind}:{','.join(sorted(collisions))}"
            )


def _count_mapping(payload: Mapping[str, object], field: str) -> Mapping[str, int]:
    mapping = _mapping(payload.get(field), field)
    if set(mapping) != set(_COUNT_FIELDS):
        raise HistoricalProjectionReviewError(f"COUNT_FIELDS_MISMATCH:{field}")
    return {name: _positive_int(mapping, name, allow_zero=True) for name in _COUNT_FIELDS}


def _string_mapping(payload: Mapping[str, object], field: str) -> Mapping[str, object]:
    mapping = _mapping(payload.get(field), field)
    result: dict[str, object] = {}
    for key, value in mapping.items():
        if not isinstance(value, str) or not value.strip():
            raise HistoricalProjectionReviewError(f"STRING_MAPPING_VALUE_REQUIRED:{field}:{key}")
        result[str(key)] = value.strip()
    return result


def _require_path_unchanged(root: Path, exact_commit: str, repository_path: str) -> None:
    completed = subprocess.run(
        ["git", "diff", "--quiet", exact_commit, "--", repository_path],
        cwd=root,
        check=False,
    )
    if completed.returncode != 0:
        raise HistoricalProjectionReviewError(f"INPUT_REGISTRY_DRIFT:{repository_path}")


def _read_inside_repository(root: Path, path: Path) -> bytes:
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise HistoricalProjectionReviewError("POLICY_PATH_OUTSIDE_REPOSITORY")
    return resolved.read_bytes()


def _safe_repository_path(value: str) -> str:
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise HistoricalProjectionReviewError(f"REPOSITORY_PATH_UNSAFE:{value}")
    return path.as_posix()


def _receipt(path: str, payload: bytes) -> Mapping[str, object]:
    return {"path": path, "size_bytes": len(payload), "sha256": sha256(payload).hexdigest()}


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _yaml_mapping(payload: bytes, name: str) -> Mapping[str, object]:
    loaded = yaml.safe_load(payload)
    return _mapping(loaded, name)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise HistoricalProjectionReviewError(f"MAPPING_REQUIRED:{field}")
    return value


def _mapping_sequence(
    payload: Mapping[str, object], field: str
) -> tuple[Mapping[str, object], ...]:
    value = payload.get(field)
    if not isinstance(value, list) or not value:
        raise HistoricalProjectionReviewError(f"NONEMPTY_MAPPING_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _text_sequence(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise HistoricalProjectionReviewError(f"TEXT_LIST_REQUIRED:{field}")
    items = tuple(str(item).strip() for item in value)
    if any(not item for item in items):
        raise HistoricalProjectionReviewError(f"TEXT_LIST_ITEM_EMPTY:{field}")
    return items


def _required_text(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise HistoricalProjectionReviewError(f"TEXT_REQUIRED:{field}")
    return value.strip()


def _positive_int(payload: Mapping[str, object], field: str, *, allow_zero: bool = False) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise HistoricalProjectionReviewError(f"INTEGER_REQUIRED:{field}")
    if value < 0 or (value == 0 and not allow_zero):
        raise HistoricalProjectionReviewError(f"POSITIVE_INTEGER_REQUIRED:{field}")
    return value


def _require_exact_commit(value: str) -> None:
    if not _COMMIT_RE.fullmatch(value):
        raise HistoricalProjectionReviewError("EXACT_COMMIT_REQUIRED")


__all__ = [
    "DEFAULT_POLICY_PATH",
    "HistoricalProjectionRenderedArtifact",
    "HistoricalProjectionReviewError",
    "HistoricalProjectionReviewPack",
    "HistoricalProjectionReviewValidation",
    "build_historical_projection_review",
    "render_historical_projection_review_markdown",
    "validate_historical_projection_review",
    "write_historical_projection_review_artifacts",
]
