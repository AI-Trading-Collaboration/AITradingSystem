from __future__ import annotations

import hashlib
import json
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
READER_DECISION_PROJECTION_SCHEMA = "atlas_reader_decision_projection.v1"

# Reviewed terminal evidence for the only QQQ Options transport gap.  This is a
# fixed evidence identity, not a tunable policy or an investment threshold.
_QQQ_RECOVERY_EVIDENCE_PATH = (
    "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v3/"
    "export_safe_terminal_evidence.json"
)
_QQQ_HISTORICAL_DQ_TASK_ID = (
    "TRADING-2533_QC_QQQ_OPTIONS_SESSION_FINALIZATION_V2_EXPORT_SAFE_DQ_PIT_"
    "EVIDENCE_ADMISSION_V1"
)
_QQQ_TRANSPORT_RECOVERY_TASK_ID = (
    "TRADING-2541_QC_QQQ_OPTIONS_EXACT_DATE_SUBSCRIPTION_MISSING_REMEDIATION_V1"
)
_ATLAS_READER_REPAIR_TASK_ID = (
    "TRADING-2545_ATLAS_CURRENT_STATE_DOMINANCE_AND_READER_CARD_REPAIR_V1"
)
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


@dataclass(frozen=True)
class AtlasReaderDecisionItem:
    item_id: str
    label_zh: str
    text_zh: str
    source_task_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "item_id": self.item_id,
            "label_zh": self.label_zh,
            "text_zh": self.text_zh,
            "source_task_ids": list(self.source_task_ids),
        }


@dataclass(frozen=True)
class AtlasReaderDecisionProjection:
    schema_version: str
    evidence_path: str
    normal_session_count: int
    recovered_session_count: int
    unresolved_session_count: int
    observed_session_count: int
    expected_session_count: int
    dq_pit_promoted: bool
    reader_cards: tuple[AtlasReaderDecisionItem, ...]
    quick_answers: tuple[AtlasReaderDecisionItem, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "evidence_path": self.evidence_path,
            "transport": {
                "normal_session_count": self.normal_session_count,
                "recovered_session_count": self.recovered_session_count,
                "unresolved_session_count": self.unresolved_session_count,
                "observed_session_count": self.observed_session_count,
                "expected_session_count": self.expected_session_count,
            },
            "dq_pit_promoted": self.dq_pit_promoted,
            "reader_cards": [item.to_dict() for item in self.reader_cards],
            "quick_answers": [item.to_dict() for item in self.quick_answers],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


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


def _reader_decision_sources(*items: PageTaskCoverage) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item.task_id for item in items))


def _load_qqq_transport_recovery_facts(*, repository_root: Path) -> Mapping[str, Any]:
    selected = _portable_path(
        repository_root,
        _QQQ_RECOVERY_EVIDENCE_PATH,
        "qqq_transport_recovery_evidence",
    )
    try:
        payload = _mapping(
            json.loads(selected.read_text(encoding="utf-8")),
            "qqq_transport_recovery_evidence",
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AtlasLiveSnapshotError("ATLAS_READER_RECOVERY_EVIDENCE_INVALID") from exc
    expected_identity = {
        "schema_version": (
            "qc_qqq_options_exact_date_subscription_recovery_terminal_evidence.v1"
        ),
        "task_id": _QQQ_TRANSPORT_RECOVERY_TASK_ID,
        "status": "EXPORT_SAFE_TERMINAL_EVIDENCE_COLLECTED",
        "technical_validation_state": "PASS",
        "recovery_status": "ACCEPTED",
        "delivery_path": "EXACT_DATE_PROVIDER_HISTORY_RECOVERY",
        "execution_terminal": "COMPLETE",
        "chain_presence_status": "PASS_WITH_EXACT_DATE_PROVIDER_HISTORY_RECOVERY",
        "data_quality_status": "PASS_FOR_RESEARCH_TRANSPORT_COMPLETENESS",
        "point_in_time_status": "PASS_FOR_EXACT_SOURCE_AND_AVAILABILITY_DATE",
        "strategy_engine_status": "NOT_IN_SCOPE_ZERO_ORDER_VALIDATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    for field, expected in expected_identity.items():
        if payload.get(field) != expected:
            raise AtlasLiveSnapshotError(
                f"ATLAS_READER_RECOVERY_EVIDENCE_IDENTITY_INVALID:{field}"
            )
    integer_fields = (
        "normal_slice_session_count",
        "recovered_session_count",
        "unresolved_session_count",
        "observed_session_count",
        "expected_session_count",
        "orders",
        "fills",
    )
    if any(type(payload.get(field)) is not int for field in integer_fields):
        raise AtlasLiveSnapshotError("ATLAS_READER_RECOVERY_COUNT_TYPE_INVALID")
    normal = int(payload["normal_slice_session_count"])
    recovered = int(payload["recovered_session_count"])
    unresolved = int(payload["unresolved_session_count"])
    observed = int(payload["observed_session_count"])
    expected = int(payload["expected_session_count"])
    if (
        normal + recovered != observed
        or observed != expected
        or unresolved != 0
        or int(payload["orders"]) != 0
        or int(payload["fills"]) != 0
        or payload.get("portfolio_invested") is not False
        or payload.get("target_source_date") != payload.get("recovery_source_date")
    ):
        raise AtlasLiveSnapshotError("ATLAS_READER_RECOVERY_TERMINAL_FACTS_INVALID")
    statistics = _mapping(payload.get("terminal_statistics"), "terminal_statistics")
    execution_terminal = str(statistics.get("TRADING2541_EXECUTION_TERMINAL", ""))
    if "dq_pit_promoted=false" not in execution_terminal:
        raise AtlasLiveSnapshotError("ATLAS_READER_DQ_PIT_PROMOTION_BOUNDARY_INVALID")
    return payload


def build_reader_decision_projection(
    *,
    repository_root: Path,
    coverage: tuple[PageTaskCoverage, ...],
    policy: AtlasLiveSnapshotPolicy,
) -> AtlasReaderDecisionProjection:
    historical_dq = _coverage_by_id(coverage, _QQQ_HISTORICAL_DQ_TASK_ID)
    recovery = _coverage_by_id(coverage, _QQQ_TRANSPORT_RECOVERY_TASK_ID)
    reader_repair = _coverage_by_id(coverage, _ATLAS_READER_REPAIR_TASK_ID)
    mainline = _coverage_by_id(coverage, policy.current_mainline_task_id)
    blocker = _coverage_by_id(coverage, policy.largest_blocker_task_id)
    next_step = _coverage_by_id(coverage, policy.next_legal_action_task_id)
    evidence = _load_qqq_transport_recovery_facts(repository_root=repository_root)
    normal = int(evidence["normal_slice_session_count"])
    recovered = int(evidence["recovered_session_count"])
    unresolved = int(evidence["unresolved_session_count"])
    observed = int(evidence["observed_session_count"])
    expected = int(evidence["expected_session_count"])
    transport_text = (
        f"QQQ Options transport 已补齐：{normal} 个 normal session + "
        f"{recovered} 个 exact-date recovery，unresolved={unresolved}，"
        f"合计 {observed}/{expected}。"
    )
    authority_text = (
        "但整体数据可信性尚未提升为通过，参数依据、负责人和独立复核仍未完成。"
    )
    current_decision_sources = _reader_decision_sources(
        recovery, mainline, blocker, reader_repair
    )
    why_sources = _reader_decision_sources(
        historical_dq, recovery, blocker, reader_repair
    )
    work_sources = _reader_decision_sources(recovery, mainline, blocker, reader_repair)
    next_sources = _reader_decision_sources(recovery, next_step, reader_repair)
    prohibited_sources = _reader_decision_sources(
        recovery, mainline, blocker, reader_repair
    )
    reader_cards = (
        AtlasReaderDecisionItem(
            item_id="CURRENT_DECISION",
            label_zh="01 · 当前决定",
            text_zh="暂不继续形成策略结论；期权链传递已修复，整体可信性与策略研究仍保持关闭。",
            source_task_ids=current_decision_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="WHY_PAUSED",
            label_zh="02 · 为什么",
            text_zh=f"{transport_text}{authority_text}",
            source_task_ids=why_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="CURRENT_WORK",
            label_zh="03 · 现在在查什么",
            text_zh=(
                "当前把已补齐的期权链传递与尚未完成的整体可信性、参数依据和人工复核分开判断，"
                "避免工程检查通过冒充策略通过。"
            ),
            source_task_ids=work_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="NEXT_STEP",
            label_zh="04 · 下一步",
            text_zh=(
                "将最新传递验证结果纳入整体可信性复核，并完成参数依据、负责人和独立复核；"
                "无需再次解释缺链日，也不授权新的外部平台运行。"
            ),
            source_task_ids=next_sources,
        ),
    )
    prohibited = AtlasReaderDecisionItem(
        item_id="PROHIBITED_INFERENCES",
        label_zh="04 · 不能推出什么",
        text_zh=(
            "不能把期权链传递完整解释为整体数据可信、参数获批、策略有效、收益稳健或风险可接受，"
            "更不表示可以投资、部署或交易。"
        ),
        source_task_ids=prohibited_sources,
    )
    quick_answers = (
        AtlasReaderDecisionItem(
            item_id="CURRENT_RESEARCH_MAINLINE",
            label_zh="01 · 当前主线",
            text_zh=(
                "QQQ 期权数据车道继续保留；期权链传递已补齐，当前主线转为整体可信性、"
                "参数依据与独立复核。"
            ),
            source_task_ids=current_decision_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="LARGEST_CURRENT_BLOCKER",
            label_zh="02 · 最大阻塞",
            text_zh=f"{transport_text}{authority_text}",
            source_task_ids=why_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="ENGINEERING_VS_RESEARCH_EVIDENCE",
            label_zh="03 · 已做到什么",
            text_zh=(
                f"受治理的外部验证已确认 {observed}/{expected} 个交易日的期权链传递完整；"
                "这只修复数据传递工程缺口，没有把整体可信性提升为通过，也不是盈利或风险证据。"
            ),
            source_task_ids=work_sources,
        ),
        prohibited,
        AtlasReaderDecisionItem(
            item_id="NEXT_OWNER_AND_ACTION",
            label_zh="05 · 下一步",
            text_zh=reader_cards[-1].text_zh,
            source_task_ids=next_sources,
        ),
        AtlasReaderDecisionItem(
            item_id="INVESTMENT_ORDER_ENGINE_AUTHORITY",
            label_zh="06 · 现在能否投资或下单",
            text_zh=(
                "不能。期权合约选择和真实策略执行引擎保持关闭，订单和成交均为 0；"
                "本页不授权外部动作、生产或交易。"
            ),
            source_task_ids=prohibited_sources,
        ),
    )
    projection = AtlasReaderDecisionProjection(
        schema_version=READER_DECISION_PROJECTION_SCHEMA,
        evidence_path=_QQQ_RECOVERY_EVIDENCE_PATH,
        normal_session_count=normal,
        recovered_session_count=recovered,
        unresolved_session_count=unresolved,
        observed_session_count=observed,
        expected_session_count=expected,
        dq_pit_promoted=False,
        reader_cards=reader_cards,
        quick_answers=quick_answers,
    )
    if tuple(item.item_id for item in reader_cards) != (
        "CURRENT_DECISION",
        "WHY_PAUSED",
        "CURRENT_WORK",
        "NEXT_STEP",
    ) or tuple(item.item_id for item in quick_answers) != (
        "CURRENT_RESEARCH_MAINLINE",
        "LARGEST_CURRENT_BLOCKER",
        "ENGINEERING_VS_RESEARCH_EVIDENCE",
        "PROHIBITED_INFERENCES",
        "NEXT_OWNER_AND_ACTION",
        "INVESTMENT_ORDER_ENGINE_AUTHORITY",
    ):
        raise AtlasLiveSnapshotError("ATLAS_READER_DECISION_ITEM_SET_INVALID")
    return projection


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
    "READER_DECISION_PROJECTION_SCHEMA",
    "AtlasLiveSnapshotBundle",
    "AtlasLiveSnapshotError",
    "AtlasLiveSnapshotPolicy",
    "AtlasReaderDecisionItem",
    "AtlasReaderDecisionProjection",
    "build_live_snapshot_bundle",
    "build_reader_decision_projection",
    "load_live_snapshot_policy",
    "reader_safe_task_summary",
    "repository_commit_time",
]
