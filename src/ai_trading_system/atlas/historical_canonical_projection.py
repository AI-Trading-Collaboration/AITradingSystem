from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any, cast

import yaml

from .historical_source_adapters import build_historical_source_adapter_bundle
from .source_projection import AtlasSourceRegistry

POLICY_SCHEMA_VERSION = "atlas_historical_canonical_projection_policy.v1"
DEFAULT_POLICY_PATH = "config/atlas/historical_canonical_projection.yaml"
PRIMARY_RESEARCH_START = "2021-02-22"
_EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2494:2026-08-02:"
    "advance_atlas_page_and_hold_trading_2481_2493_for_owner_review_v1"
)
_EXPECTED_EXCLUDED_TASK_IDS = tuple(f"TRADING-{task_id}" for task_id in range(2481, 2494))
_EXPECTED_SAFETY: Mapping[str, object] = {
    "canonical_projection_active": True,
    "historical_records_only": True,
    "qqq_options_projection_performed": False,
    "investment_conclusion_generated": False,
    "current_focus_changed": False,
    "data_quality_executed": False,
    "model_execution_allowed": False,
    "backtest_execution_allowed": False,
    "external_platform_action": "none",
    "production_effect": "none",
    "broker_action": "none",
}
_COUNT_FIELDS = ("sources", "nodes", "edges", "results", "attributions")


class HistoricalCanonicalProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class HistoricalCanonicalProjection:
    schema_version: str
    projection_id: str
    owner_decision: str
    primary_research_start: str
    base_registry_id: str
    projected_title: str
    reader_notice: str
    review_policy_path: str
    review_policy_sha256: str
    expected_base_counts: Mapping[str, int]
    expected_projected_counts: Mapping[str, int]
    source_ref_ids: tuple[str, ...]
    original_statuses: Mapping[str, str]
    nodes: tuple[Mapping[str, object], ...]
    edges: tuple[Mapping[str, object], ...]
    results: tuple[Mapping[str, object], ...]
    attributions: tuple[Mapping[str, object], ...]
    excluded_task_ids: tuple[str, ...]
    safety: Mapping[str, object]

    @property
    def projection_counts(self) -> Mapping[str, int]:
        return {
            "sources": 0,
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "results": len(self.results),
            "attributions": len(self.attributions),
        }


@dataclass(frozen=True)
class HistoricalCanonicalProjectionValidation:
    schema_version: str
    status: str
    projection_id: str
    evidence_exact_commit: str
    checks: tuple[str, ...]
    source_ref_ids: tuple[str, ...]
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "projection_id": self.projection_id,
            "evidence_exact_commit": self.evidence_exact_commit,
            "checks": list(self.checks),
            "source_ref_ids": list(self.source_ref_ids),
            "errors": list(self.errors),
            "production_effect": "none",
            "broker_action": "none",
        }


def build_historical_canonical_projection(
    *,
    repository_root: Path,
    policy_path: Path | None = None,
) -> HistoricalCanonicalProjection:
    root = repository_root.resolve()
    selected_policy = policy_path or root / DEFAULT_POLICY_PATH
    policy = _yaml_mapping(_read_inside_repository(root, selected_policy), "projection_policy")
    if _required_text(policy, "schema_version") != POLICY_SCHEMA_VERSION:
        raise HistoricalCanonicalProjectionError("PROJECTION_POLICY_SCHEMA_MISMATCH")
    if _required_text(policy, "status") != "ACTIVE_REVIEWED_BASELINE":
        raise HistoricalCanonicalProjectionError("PROJECTION_POLICY_NOT_ACTIVE_REVIEWED")
    owner_decision = _required_text(policy, "owner_decision")
    if owner_decision != _EXPECTED_OWNER_DECISION:
        raise HistoricalCanonicalProjectionError("PROJECTION_OWNER_DECISION_MISMATCH")
    if _required_text(policy, "primary_research_start") != PRIMARY_RESEARCH_START:
        raise HistoricalCanonicalProjectionError("PROJECTION_PRIMARY_RESEARCH_START_DRIFT")

    review_policy_path = _safe_repository_path(_required_text(policy, "review_policy_path"))
    review_bytes = _read_inside_repository(root, root / PurePosixPath(review_policy_path))
    expected_review_sha256 = _sha256_text(policy, "review_policy_sha256")
    if hashlib.sha256(review_bytes).hexdigest() != expected_review_sha256:
        raise HistoricalCanonicalProjectionError("PROJECTION_REVIEW_POLICY_HASH_MISMATCH")
    review = _yaml_mapping(review_bytes, "review_policy")
    _validate_review_safety(review)

    expected_base = _count_mapping(policy, "expected_base_counts")
    expected_projected = _count_mapping(policy, "expected_projected_counts")
    if _count_mapping(review, "expected_current_counts") != expected_base:
        raise HistoricalCanonicalProjectionError("PROJECTION_REVIEW_BASE_COUNT_MISMATCH")
    if _count_mapping(review, "expected_candidate_counts") != expected_projected:
        raise HistoricalCanonicalProjectionError("PROJECTION_REVIEW_CANDIDATE_COUNT_MISMATCH")

    excluded_task_ids = _text_sequence(policy, "excluded_task_ids")
    if excluded_task_ids != _EXPECTED_EXCLUDED_TASK_IDS:
        raise HistoricalCanonicalProjectionError("PROJECTION_EXCLUDED_TASK_SET_MISMATCH")
    safety = _mapping(policy.get("safety"), "safety")
    if dict(safety) != dict(_EXPECTED_SAFETY):
        raise HistoricalCanonicalProjectionError("PROJECTION_SAFETY_BOUNDARY_MISMATCH")

    enrichments = {
        _required_text(item, "source_ref_id"): item for item in _mapping_sequence(policy, "records")
    }
    if len(enrichments) != len(_mapping_sequence(policy, "records")):
        raise HistoricalCanonicalProjectionError("PROJECTION_ENRICHMENT_SOURCE_DUPLICATE")
    review_records = _mapping_sequence(review, "records")
    review_sources = tuple(_required_text(item, "source_ref_id") for item in review_records)
    if set(enrichments) != set(review_sources) or len(review_sources) != 5:
        raise HistoricalCanonicalProjectionError("PROJECTION_SOURCE_SET_MISMATCH")

    group = _mapping(review.get("group_node"), "group_node")
    root_edge = _mapping(review.get("root_edge"), "root_edge")
    group_node_id = _required_text(group, "node_id")
    if _required_text(root_edge, "to_node_id") != group_node_id:
        raise HistoricalCanonicalProjectionError("PROJECTION_ROOT_EDGE_GROUP_MISMATCH")
    group_source_ref_id = _required_text(policy, "group_source_ref_id")
    nodes: list[Mapping[str, object]] = [
        {
            "node_id": group_node_id,
            "node_kind": _required_text(group, "node_kind"),
            "title": _required_text(group, "title"),
            "summary": _required_text(group, "summary"),
            "assertion_kind": _required_text(group, "assertion_kind"),
            "source_ref_ids": [group_source_ref_id],
            "raw_status": _required_text(group, "raw_status"),
        }
    ]
    edges: list[Mapping[str, object]] = [
        {
            "edge_id": _required_text(root_edge, "edge_id"),
            "edge_kind": "CONTAINS",
            "from_node_id": _required_text(root_edge, "from_node_id"),
            "to_node_id": group_node_id,
            "label": _required_text(policy, "root_edge_label"),
        }
    ]
    results: list[Mapping[str, object]] = []
    attributions: list[Mapping[str, object]] = []
    original_statuses: dict[str, str] = {}

    for item in review_records:
        source_ref_id = _required_text(item, "source_ref_id")
        enrichment = enrichments[source_ref_id]
        node_id = _required_text(item, "node_id")
        result_id = _required_text(item, "result_id")
        original_status = _required_text(item, "expected_original_status")
        raw_status = _required_text(item, "proposed_raw_status")
        display_status = _required_text(item, "proposed_display_status")
        assertion_kind = _required_text(item, "assertion_kind")
        mapping_rationale = _required_text(item, "mapping_rationale")
        original_statuses[source_ref_id] = original_status
        nodes.append(
            {
                "node_id": node_id,
                "node_kind": _required_text(item, "node_kind"),
                "title": _required_text(item, "title"),
                "summary": _required_text(item, "reader_summary"),
                "assertion_kind": assertion_kind,
                "source_ref_ids": [source_ref_id],
                "raw_status": raw_status,
            }
        )
        edges.append(
            {
                "edge_id": _required_text(item, "edge_id"),
                "edge_kind": "CONTAINS",
                "from_node_id": group_node_id,
                "to_node_id": node_id,
                "label": _required_text(enrichment, "edge_label"),
            }
        )
        results.append(
            {
                "result_id": result_id,
                "node_id": node_id,
                "title": _required_text(item, "title"),
                "raw_status": raw_status,
                "display_status": display_status,
                "reader_summary": _required_text(item, "reader_summary"),
                "assertion_kind": assertion_kind,
                "source_ref_ids": [source_ref_id],
                "investment_facing": False,
                "limitations": list(_text_sequence(enrichment, "limitations")),
                "source_original_status": original_status,
                "status_mapping_rationale": mapping_rationale,
            }
        )
        attributions.append(
            {
                "attribution_id": _required_text(item, "attribution_id"),
                "result_id": result_id,
                "source_node_id": node_id,
                "direction": "NEUTRAL",
                "explanation": _required_text(enrichment, "attribution_explanation"),
                "assertion_kind": assertion_kind,
                "source_ref_ids": [source_ref_id],
            }
        )

    projection = HistoricalCanonicalProjection(
        schema_version=POLICY_SCHEMA_VERSION,
        projection_id=_required_text(policy, "projection_id"),
        owner_decision=owner_decision,
        primary_research_start=PRIMARY_RESEARCH_START,
        base_registry_id=_required_text(policy, "base_registry_id"),
        projected_title=_required_text(policy, "projected_title"),
        reader_notice=_required_text(policy, "reader_notice"),
        review_policy_path=review_policy_path,
        review_policy_sha256=expected_review_sha256,
        expected_base_counts=expected_base,
        expected_projected_counts=expected_projected,
        source_ref_ids=review_sources,
        original_statuses=original_statuses,
        nodes=tuple(nodes),
        edges=tuple(edges),
        results=tuple(results),
        attributions=tuple(attributions),
        excluded_task_ids=excluded_task_ids,
        safety=safety,
    )
    _validate_projection_shape(projection)
    return projection


def apply_historical_canonical_projection(
    registry: AtlasSourceRegistry,
    projection: HistoricalCanonicalProjection,
) -> AtlasSourceRegistry:
    if registry.registry_id != projection.base_registry_id:
        raise HistoricalCanonicalProjectionError("PROJECTION_BASE_REGISTRY_ID_MISMATCH")
    if registry.primary_research_start.isoformat() != projection.primary_research_start:
        raise HistoricalCanonicalProjectionError("PROJECTION_REGISTRY_PRIMARY_START_DRIFT")
    base_counts = _registry_counts(registry)
    if base_counts != projection.expected_base_counts:
        raise HistoricalCanonicalProjectionError("PROJECTION_BASE_COUNTS_MISMATCH")
    registered_source_ids = {
        _required_text(item, "source_ref_id") for item in registry.source_payloads
    }
    if not set(projection.source_ref_ids).issubset(registered_source_ids):
        raise HistoricalCanonicalProjectionError("PROJECTION_SOURCE_NOT_REGISTERED")

    _require_disjoint_ids(registry.node_payloads, projection.nodes, "node_id")
    _require_disjoint_ids(registry.edge_payloads, projection.edges, "edge_id")
    _require_disjoint_ids(registry.result_payloads, projection.results, "result_id")
    _require_disjoint_ids(registry.attribution_payloads, projection.attributions, "attribution_id")
    projected = replace(
        registry,
        registry_id=projection.projection_id,
        title=projection.projected_title,
        reader_notice=projection.reader_notice,
        node_payloads=(*registry.node_payloads, *projection.nodes),
        edge_payloads=(*registry.edge_payloads, *projection.edges),
        result_payloads=(*registry.result_payloads, *projection.results),
        attribution_payloads=(*registry.attribution_payloads, *projection.attributions),
    )
    if _registry_counts(projected) != projection.expected_projected_counts:
        raise HistoricalCanonicalProjectionError("PROJECTION_PROJECTED_COUNTS_MISMATCH")
    return projected


def validate_historical_canonical_projection_sources(
    *,
    repository_root: Path,
    evidence_exact_commit: str,
    policy_path: Path | None = None,
) -> HistoricalCanonicalProjectionValidation:
    projection = build_historical_canonical_projection(
        repository_root=repository_root,
        policy_path=policy_path,
    )
    bundle = build_historical_source_adapter_bundle(
        repository_root=repository_root,
        exact_commit=evidence_exact_commit,
    )
    records = {item.source_ref_id: item for item in bundle.records}
    if set(records) != set(projection.source_ref_ids):
        raise HistoricalCanonicalProjectionError("PROJECTION_TYPED_SOURCE_SET_MISMATCH")
    for source_ref_id in projection.source_ref_ids:
        record = records[source_ref_id]
        if record.raw_status != projection.original_statuses[source_ref_id]:
            raise HistoricalCanonicalProjectionError(
                "PROJECTION_TYPED_ORIGINAL_STATUS_MISMATCH:" + source_ref_id
            )
        if (
            not record.historical_record
            or record.current_primary_default
            or record.result_projection_allowed
            or record.page_projection_allowed
            or record.investment_conclusion_generated
            or record.production_effect != "none"
            or record.broker_action != "none"
        ):
            raise HistoricalCanonicalProjectionError(
                "PROJECTION_TYPED_SAFETY_BOUNDARY_MISMATCH:" + source_ref_id
            )
    return HistoricalCanonicalProjectionValidation(
        schema_version="atlas_historical_canonical_projection_validation.v1",
        status="PASS",
        projection_id=projection.projection_id,
        evidence_exact_commit=evidence_exact_commit,
        checks=(
            "OWNER_DECISION_BOUND",
            "REVIEW_POLICY_SHA256_BOUND",
            "FIVE_TYPED_SOURCE_RECORDS_EXACT",
            "ORIGINAL_STATUS_MAPPING_BOUND",
            "BASE_AND_PROJECTED_COUNTS_BOUND",
            "CONTAINS_ONLY_GRAPH_BOUND",
            "LIMITED_DISPLAY_ONLY",
            "NEUTRAL_PROVENANCE_ONLY",
            "QQQ_OPTIONS_2481_2493_EXCLUDED",
            "PRIMARY_RESEARCH_START_2021_02_22",
            "PRODUCTION_AND_BROKER_NONE",
        ),
        source_ref_ids=projection.source_ref_ids,
    )


def _validate_projection_shape(projection: HistoricalCanonicalProjection) -> None:
    if projection.projection_counts != {
        "sources": 0,
        "nodes": 6,
        "edges": 6,
        "results": 5,
        "attributions": 5,
    }:
        raise HistoricalCanonicalProjectionError("PROJECTION_DELTA_COUNTS_MISMATCH")
    if any(_required_text(item, "edge_kind") != "CONTAINS" for item in projection.edges):
        raise HistoricalCanonicalProjectionError("PROJECTION_EDGE_KIND_NOT_CONTAINS")
    if any(
        _required_text(item, "display_status") != "LIMITED"
        or item.get("investment_facing") is not False
        for item in projection.results
    ):
        raise HistoricalCanonicalProjectionError("PROJECTION_RESULT_DISPLAY_BOUNDARY_MISMATCH")
    if any(_required_text(item, "direction") != "NEUTRAL" for item in projection.attributions):
        raise HistoricalCanonicalProjectionError("PROJECTION_ATTRIBUTION_NOT_NEUTRAL")
    serialized = repr(
        (
            projection.nodes,
            projection.edges,
            projection.results,
            projection.attributions,
        )
    )
    if any(task_id in serialized for task_id in projection.excluded_task_ids):
        raise HistoricalCanonicalProjectionError("PROJECTION_EXCLUDED_TASK_LEAK")


def _validate_review_safety(review: Mapping[str, object]) -> None:
    forbidden = _text_sequence(review, "forbidden_candidate_family_ids")
    if forbidden != ("atlas_historical_candidate_next_roadmap_v1",):
        raise HistoricalCanonicalProjectionError("PROJECTION_REVIEW_FORBIDDEN_SET_DRIFT")
    safety = _mapping(review.get("safety"), "review.safety")
    expected = {
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
    if dict(safety) != expected:
        raise HistoricalCanonicalProjectionError("PROJECTION_REVIEW_SAFETY_DRIFT")


def _registry_counts(registry: AtlasSourceRegistry) -> Mapping[str, int]:
    return {
        "sources": len(registry.source_payloads),
        "nodes": len(registry.node_payloads),
        "edges": len(registry.edge_payloads),
        "results": len(registry.result_payloads),
        "attributions": len(registry.attribution_payloads),
    }


def _require_disjoint_ids(
    base: Sequence[Mapping[str, object]],
    additions: Sequence[Mapping[str, object]],
    field: str,
) -> None:
    base_ids = {_required_text(item, field) for item in base}
    addition_ids = tuple(_required_text(item, field) for item in additions)
    if len(set(addition_ids)) != len(addition_ids) or base_ids.intersection(addition_ids):
        raise HistoricalCanonicalProjectionError("PROJECTION_ID_COLLISION:" + field)


def _read_inside_repository(root: Path, path: Path) -> bytes:
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise HistoricalCanonicalProjectionError("PROJECTION_PATH_OUTSIDE_REPOSITORY") from exc
    if not resolved.is_file():
        raise HistoricalCanonicalProjectionError("PROJECTION_FILE_MISSING")
    return resolved.read_bytes()


def _safe_repository_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise HistoricalCanonicalProjectionError("PROJECTION_REPOSITORY_PATH_INVALID")
    return path.as_posix()


def _yaml_mapping(payload: bytes, field: str) -> Mapping[str, Any]:
    loaded = yaml.safe_load(payload)
    if not isinstance(loaded, Mapping):
        raise HistoricalCanonicalProjectionError("PROJECTION_MAPPING_REQUIRED:" + field)
    return loaded


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise HistoricalCanonicalProjectionError("PROJECTION_MAPPING_REQUIRED:" + field)
    return value


def _mapping_sequence(payload: Mapping[str, Any], field: str) -> tuple[Mapping[str, Any], ...]:
    value = payload.get(field)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, Mapping) for item in value)
    ):
        raise HistoricalCanonicalProjectionError("PROJECTION_MAPPING_LIST_REQUIRED:" + field)
    return tuple(item for item in value if isinstance(item, Mapping))


def _text_sequence(payload: Mapping[str, Any], field: str) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list) or not value:
        raise HistoricalCanonicalProjectionError("PROJECTION_TEXT_LIST_REQUIRED:" + field)
    result = tuple(str(item).strip() for item in value)
    if any(not item for item in result) or len(set(result)) != len(result):
        raise HistoricalCanonicalProjectionError("PROJECTION_TEXT_LIST_INVALID:" + field)
    return result


def _count_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, int]:
    value = _mapping(payload.get(field), field)
    result = {key: value.get(key) for key in _COUNT_FIELDS}
    if set(value) != set(_COUNT_FIELDS) or any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in result.values()
    ):
        raise HistoricalCanonicalProjectionError("PROJECTION_COUNT_MAPPING_INVALID:" + field)
    return {key: cast(int, result[key]) for key in _COUNT_FIELDS}


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise HistoricalCanonicalProjectionError("PROJECTION_TEXT_REQUIRED:" + field)
    return value


def _sha256_text(payload: Mapping[str, Any], field: str) -> str:
    value = _required_text(payload, field).lower()
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise HistoricalCanonicalProjectionError("PROJECTION_SHA256_INVALID:" + field)
    return value


__all__ = [
    "DEFAULT_POLICY_PATH",
    "HistoricalCanonicalProjection",
    "HistoricalCanonicalProjectionError",
    "HistoricalCanonicalProjectionValidation",
    "apply_historical_canonical_projection",
    "build_historical_canonical_projection",
    "validate_historical_canonical_projection_sources",
]
