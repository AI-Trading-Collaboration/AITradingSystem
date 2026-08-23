from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.atlas.page_effectiveness import (
    PageEffectivenessPolicy,
    build_page_task_coverage,
    load_page_effectiveness_policy,
    unclassified_page_successors,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import build_snapshot_diff
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.strategy_research_cited_query import CitedQueryQuestionId
from ai_trading_system.contracts.strategy_research_explorer import (
    AssertionKind,
    AttributionDirection,
    ExplorerSourceKind,
    ExplorerSourceRef,
    ResearchAttribution,
    ResearchEdgeKind,
    ResearchNodeKind,
    ResearchPathEdge,
    ResearchPathNode,
    ResearchResultCard,
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffEntityKind,
    StrategyResearchExplorerDiff,
)
from ai_trading_system.contracts.strategy_research_page_effectiveness import PageTaskCoverage
from ai_trading_system.platform.architecture.task_registry_canonical import (
    CanonicalTaskRegistry,
    validate_canonical_registry,
)

DEFAULT_LIVE_SNAPSHOT_POLICY_PATH = "config/atlas/live_snapshot.yaml"
LIVE_SNAPSHOT_POLICY_SCHEMA = "atlas_live_snapshot_policy.v1"
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_STATUS_MAPPING = {
    "DONE": "PASS",
    "BASELINE_DONE": "LIMITED",
    "PROPOSED": "NOT_DUE",
    "IN_PROGRESS": "RUNNING",
    "VALIDATING": "RUNNING",
    "BLOCKED_EXTERNAL": "BLOCKED",
    "BLOCKED_OWNER_INPUT": "BLOCKED",
}
_SAFETY = {
    "primary_research_start": "2021-02-22",
    "historical_snapshot_role": "LEGACY_COMPARISON_EVIDENCE",
    "live_task_records_are_investment_evidence": False,
    "investment_conclusion_generated": False,
    "production_effect": "none",
    "broker_action": "none",
}
_READER_TEXT_REPLACEMENTS = {
    "PENDING_OWNER_APPROVAL": "等待 Owner 逐轴批准",
    "BLOCKED_OWNER_REVIEW": "等待 Owner 审阅而阻塞",
    "threshold_bundle_frozen=false": "threshold bundle 尚未冻结",
    "APPROVE_EXACTLY_AS_DRAFTED": "按草案原样逐项批准",
    "CANONICAL_DQ_PIT": "canonical DQ/PIT 数据质量门",
    "OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY": "仅为 Owner 意向，尚非可执行权威",
    "AUTHORITY_UNAVAILABLE": "权威尚不可用",
    "false-risk-off": "错误避险",
    "slice": "Slice",
}


class AtlasLiveSnapshotError(ValueError):
    pass


@dataclass(frozen=True)
class AtlasLiveSnapshotPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    historical_source_registry_path: str
    page_effectiveness_policy_path: str
    classified_through_task_id: str
    current_mainline_task_id: str
    largest_blocker_task_id: str
    next_legal_action_task_id: str
    status_object_zh: str
    evidence_evaluated_at: str | None
    task_status_mapping: Mapping[str, str]
    safety: Mapping[str, object]
    policy_sha256: str


@dataclass(frozen=True)
class AtlasLiveSnapshotBundle:
    comparison_snapshot: StrategyResearchExplorerSnapshot
    current_snapshot: StrategyResearchExplorerSnapshot
    current_diff: StrategyResearchExplorerDiff
    target_ids: Mapping[CitedQueryQuestionId, str]
    source_commit: str
    research_state_as_of: str
    evidence_evaluated_at: str | None
    page_source_commit_at: str
    status_object_zh: str
    current_mainline_task_id: str
    largest_blocker_task_id: str
    next_legal_action_task_id: str
    policy_sha256: str


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_MAPPING_REQUIRED:{field}")
    return value


def _portable_path(root: Path, value: str, field: str) -> Path:
    normalized = value.replace("\\", "/")
    if (
        not normalized
        or normalized.startswith("/")
        or re.match(r"^[A-Za-z]:", normalized)
        or ".." in normalized.split("/")
    ):
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_PATH_INVALID:{field}:{value}")
    selected = (root / normalized).resolve()
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_PATH_OUTSIDE_REPOSITORY:{field}") from exc
    return selected


def _aware_datetime(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_DATETIME_INVALID:{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_DATETIME_TIMEZONE_REQUIRED:{field}")
    return parsed


def load_live_snapshot_policy(
    *, repository_root: Path, policy_path: str = DEFAULT_LIVE_SNAPSHOT_POLICY_PATH
) -> AtlasLiveSnapshotPolicy:
    root = repository_root.resolve()
    selected = _portable_path(root, policy_path, "policy")
    raw = selected.read_bytes()
    try:
        payload = _mapping(yaml.safe_load(raw.decode("utf-8")), "policy")
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_POLICY_INVALID") from exc
    expected = {
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "owner",
        "historical_source_registry_path",
        "page_effectiveness_policy_path",
        "classified_through_task_id",
        "current_mainline_task_id",
        "largest_blocker_task_id",
        "next_legal_action_task_id",
        "status_object_zh",
        "evidence_evaluated_at",
        "task_status_mapping",
        "safety",
    }
    if set(payload) != expected or payload.get("schema_version") != LIVE_SNAPSHOT_POLICY_SCHEMA:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_POLICY_SCHEMA_INVALID")
    if str(payload.get("status")) != "REVIEWED_FAIL_CLOSED_LIVE_PROJECTION":
        raise AtlasLiveSnapshotError("ATLAS_LIVE_POLICY_STATUS_INVALID")
    mapping = {str(key): str(value) for key, value in _mapping(
        payload.get("task_status_mapping"), "task_status_mapping"
    ).items()}
    if mapping != _STATUS_MAPPING:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_TASK_STATUS_MAPPING_INVALID")
    safety = _mapping(payload.get("safety"), "safety")
    if dict(safety) != _SAFETY:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_SAFETY_INVALID")
    evidence_at = payload.get("evidence_evaluated_at")
    if evidence_at is not None:
        _aware_datetime(str(evidence_at), "evidence_evaluated_at")
    for field in (
        "policy_id",
        "policy_version",
        "owner",
        "historical_source_registry_path",
        "page_effectiveness_policy_path",
        "classified_through_task_id",
        "current_mainline_task_id",
        "largest_blocker_task_id",
        "next_legal_action_task_id",
        "status_object_zh",
    ):
        if not str(payload.get(field, "")).strip():
            raise AtlasLiveSnapshotError(f"ATLAS_LIVE_POLICY_TEXT_REQUIRED:{field}")
    return AtlasLiveSnapshotPolicy(
        policy_id=str(payload["policy_id"]),
        policy_version=str(payload["policy_version"]),
        status=str(payload["status"]),
        owner=str(payload["owner"]),
        historical_source_registry_path=str(payload["historical_source_registry_path"]),
        page_effectiveness_policy_path=str(payload["page_effectiveness_policy_path"]),
        classified_through_task_id=str(payload["classified_through_task_id"]),
        current_mainline_task_id=str(payload["current_mainline_task_id"]),
        largest_blocker_task_id=str(payload["largest_blocker_task_id"]),
        next_legal_action_task_id=str(payload["next_legal_action_task_id"]),
        status_object_zh=str(payload["status_object_zh"]),
        evidence_evaluated_at=None if evidence_at is None else str(evidence_at),
        task_status_mapping=mapping,
        safety=safety,
        policy_sha256=hashlib.sha256(raw).hexdigest(),
    )


def repository_commit_time(*, repository_root: Path, exact_commit: str) -> str:
    if not _GIT_SHA.fullmatch(exact_commit):
        raise AtlasLiveSnapshotError("ATLAS_LIVE_EXACT_COMMIT_INVALID")
    try:
        result = subprocess.run(
            ["git", "show", "-s", "--format=%cI", exact_commit],
            cwd=repository_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.SubprocessError as exc:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_COMMIT_TIME_UNAVAILABLE") from exc
    value = result.stdout.strip()
    _aware_datetime(value, "page_source_commit_at")
    return value


def _task_slug(task_id: str) -> str:
    return task_id.lower().replace("_", "-")


def _task_sources(
    *, coverage: PageTaskCoverage, exact_commit: str
) -> tuple[ExplorerSourceRef, ExplorerSourceRef]:
    task_time = _aware_datetime(coverage.task_event_at, "task_event_at")
    slug = _task_slug(coverage.task_id)
    time_note = (
        "事件 occurred_at 可用。"
        if coverage.task_event_time_basis == "EVENT_OCCURRED_AT"
        else "legacy event 未记录 occurred_at；as_of 仅使用 event base commit 时间。"
    )
    limitation = (
        "这是 canonical task 治理状态，不是策略有效性、收益或风险证据。" + time_note
    )
    shared = {
        "source_kind": ExplorerSourceKind.GIT_AUTHORITY,
        "exact_commit": exact_commit,
        "as_of": task_time,
        "known_at": task_time,
        "available_at": task_time,
        "research_context_complete": False,
        "data_quality_ready": False,
        "limitation": limitation,
    }
    return (
        ExplorerSourceRef(
            source_ref_id=f"live-requirement-{slug}",
            source_path=coverage.requirement_path,
            content_sha256=coverage.requirement_sha256,
            artifact_identity=f"requirement:{coverage.task_id}",
            **shared,
        ),
        ExplorerSourceRef(
            source_ref_id=f"live-task-event-{slug}",
            source_path=coverage.task_fragment_path,
            content_sha256=coverage.task_fragment_sha256,
            artifact_identity=f"task-event:{coverage.task_event_id}",
            **shared,
        ),
    )


def _coverage_by_id(
    coverage: tuple[PageTaskCoverage, ...], task_id: str
) -> PageTaskCoverage:
    matches = tuple(item for item in coverage if item.task_id == task_id)
    if len(matches) != 1:
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_TASK_ROLE_NOT_COVERED:{task_id}")
    return matches[0]


def _source_ids(coverage: PageTaskCoverage) -> tuple[str, str]:
    slug = _task_slug(coverage.task_id)
    return f"live-requirement-{slug}", f"live-task-event-{slug}"


def _canonical_status(policy: AtlasLiveSnapshotPolicy, raw_status: str) -> CanonicalStatus:
    mapped = policy.task_status_mapping.get(raw_status)
    if mapped is None:
        raise AtlasLiveSnapshotError(f"ATLAS_LIVE_TASK_STATUS_UNMAPPED:{raw_status}")
    return CanonicalStatus(mapped)


def reader_safe_task_summary(value: str) -> str:
    result = value
    for raw, reader_text in _READER_TEXT_REPLACEMENTS.items():
        result = result.replace(raw, reader_text)
    return result


def _build_current_snapshot(
    *,
    comparison: StrategyResearchExplorerSnapshot,
    coverage: tuple[PageTaskCoverage, ...],
    policy: AtlasLiveSnapshotPolicy,
    exact_commit: str,
) -> StrategyResearchExplorerSnapshot:
    mainline = _coverage_by_id(coverage, policy.current_mainline_task_id)
    blocker = _coverage_by_id(coverage, policy.largest_blocker_task_id)
    mainline_summary = reader_safe_task_summary(mainline.reader_summary_zh)
    blocker_summary = reader_safe_task_summary(blocker.reader_summary_zh)
    current_event_times = tuple(
        _aware_datetime(item.task_event_at, f"task_event_at:{item.task_id}")
        for item in coverage
        if item.task_event_time_basis == "EVENT_OCCURRED_AT"
    )
    if not current_event_times:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_RESEARCH_STATE_EVENT_TIME_UNAVAILABLE")
    research_state_at = max(current_event_times)
    live_sources = tuple(
        source
        for item in coverage
        for source in _task_sources(coverage=item, exact_commit=exact_commit)
    )
    mainline_source_ids = _source_ids(mainline)
    current_nodes = tuple(
        replace(
            item,
            title="策略研究当前主线（live canonical）",
            summary=mainline_summary,
            source_ref_ids=tuple(dict.fromkeys((*item.source_ref_ids, *mainline_source_ids))),
        )
        if item.node_id == "program-strategy-research"
        else item
        for item in comparison.nodes
    )
    task_nodes = tuple(
        ResearchPathNode(
            node_id=f"live-task-{_task_slug(item.task_id)}",
            node_kind=ResearchNodeKind.ARTIFACT,
            title=f"Canonical task：{item.task_id}",
            summary=reader_safe_task_summary(item.reader_summary_zh),
            assertion_kind=AssertionKind.RULE_JUDGMENT,
            source_ref_ids=_source_ids(item),
            raw_status=_canonical_status(policy, item.task_status),
        )
        for item in coverage
    )
    task_edges = tuple(
        ResearchPathEdge(
            edge_id=f"edge-live-task-{_task_slug(item.task_id)}",
            edge_kind=ResearchEdgeKind.CONTAINS,
            from_node_id="program-strategy-research",
            to_node_id=f"live-task-{_task_slug(item.task_id)}",
            label="live canonical task coverage",
        )
        for item in coverage
    )
    blocker_status = _canonical_status(policy, blocker.task_status)
    blocker_result = ResearchResultCard(
        result_id="live-current-blocker-result",
        node_id=f"live-task-{_task_slug(blocker.task_id)}",
        title="当前最大阻塞与策略状态",
        raw_status=blocker_status,
        display_status=blocker_status,
        reader_summary=blocker_summary,
        assertion_kind=AssertionKind.RULE_JUDGMENT,
        source_ref_ids=_source_ids(blocker),
        investment_facing=False,
        limitations=("任务状态只界定当前研究与工程边界，不能升级为策略有效或交易授权。",),
    )
    blocker_attribution = ResearchAttribution(
        attribution_id="live-current-blocker-attribution",
        result_id=blocker_result.result_id,
        source_node_id=blocker_result.node_id,
        direction=AttributionDirection.CONTRADICTS,
        explanation=blocker_summary,
        assertion_kind=AssertionKind.RULE_JUDGMENT,
        source_ref_ids=_source_ids(blocker),
    )
    return StrategyResearchExplorerSnapshot.build(
        title="Atlas 策略研究 live canonical snapshot",
        generated_at=research_state_at,
        sources=(*comparison.sources, *live_sources),
        nodes=(*current_nodes, *task_nodes),
        edges=(*comparison.edges, *task_edges),
        results=(*comparison.results, blocker_result),
        attributions=(*comparison.attributions, blocker_attribution),
    )


def _page_policy(
    *, repository_root: Path, live_policy: AtlasLiveSnapshotPolicy
) -> PageEffectivenessPolicy:
    return load_page_effectiveness_policy(
        repository_root=repository_root,
        policy_path=_portable_path(
            repository_root,
            live_policy.page_effectiveness_policy_path,
            "page_effectiveness_policy_path",
        ),
    )


def build_live_snapshot_bundle(
    *, repository_root: Path, exact_commit: str
) -> AtlasLiveSnapshotBundle:
    root = repository_root.resolve()
    if not _GIT_SHA.fullmatch(exact_commit):
        raise AtlasLiveSnapshotError("ATLAS_LIVE_EXACT_COMMIT_INVALID")
    policy = load_live_snapshot_policy(repository_root=root)
    page_policy = _page_policy(repository_root=root, live_policy=policy)
    registry: CanonicalTaskRegistry = validate_canonical_registry(project_root=root)
    unknown = unclassified_page_successors(registry, page_policy)
    if unknown:
        raise AtlasLiveSnapshotError(
            "ATLAS_LIVE_UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED:" + ",".join(unknown)
        )
    coverage = build_page_task_coverage(root=root, policy=page_policy, registry=registry)
    if coverage[-1].task_id != policy.classified_through_task_id:
        raise AtlasLiveSnapshotError(
            "ATLAS_LIVE_CLASSIFIED_THROUGH_TASK_DRIFT:"
            f"policy={policy.classified_through_task_id}:latest={coverage[-1].task_id}"
        )
    comparison = build_atlas_bundle(
        repository_root=root,
        exact_commit=exact_commit,
        registry_path=_portable_path(
            root, policy.historical_source_registry_path, "historical_source_registry_path"
        ),
    ).snapshot
    current = _build_current_snapshot(
        comparison=comparison,
        coverage=coverage,
        policy=policy,
        exact_commit=exact_commit,
    )
    diff = build_snapshot_diff(comparison, current)
    program_change = tuple(
        item
        for item in diff.changes
        if item.entity_kind is ExplorerDiffEntityKind.NODE
        and item.entity_id == "program-strategy-research"
    )
    if len(program_change) != 1:
        raise AtlasLiveSnapshotError("ATLAS_LIVE_MAINLINE_CHANGE_ID_MISSING")
    mainline = _coverage_by_id(coverage, policy.current_mainline_task_id)
    research_state_as_of = current.generated_at.isoformat()
    return AtlasLiveSnapshotBundle(
        comparison_snapshot=comparison,
        current_snapshot=current,
        current_diff=diff,
        target_ids={
            CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY: "program-strategy-research",
            CitedQueryQuestionId.RESULT_AND_STATUS: "live-current-blocker-result",
            CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS: (
                "live-current-blocker-attribution"
            ),
            CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION: program_change[0].change_id,
            CitedQueryQuestionId.SOURCE_LINEAGE: _source_ids(mainline)[1],
        },
        source_commit=exact_commit,
        research_state_as_of=research_state_as_of,
        evidence_evaluated_at=policy.evidence_evaluated_at,
        page_source_commit_at=repository_commit_time(
            repository_root=root, exact_commit=exact_commit
        ),
        status_object_zh=policy.status_object_zh,
        current_mainline_task_id=policy.current_mainline_task_id,
        largest_blocker_task_id=policy.largest_blocker_task_id,
        next_legal_action_task_id=policy.next_legal_action_task_id,
        policy_sha256=policy.policy_sha256,
    )


__all__ = [
    "DEFAULT_LIVE_SNAPSHOT_POLICY_PATH",
    "LIVE_SNAPSHOT_POLICY_SCHEMA",
    "AtlasLiveSnapshotBundle",
    "AtlasLiveSnapshotError",
    "AtlasLiveSnapshotPolicy",
    "build_live_snapshot_bundle",
    "load_live_snapshot_policy",
    "reader_safe_task_summary",
    "repository_commit_time",
]
