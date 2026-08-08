from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_BLOB = re.compile(r"^[0-9a-f]{40}$")
QQQ_OPTIONS_PROJECTION_TASK_IDS = tuple(f"TRADING-{number}" for number in range(2481, 2494))
QQQ_OPTIONS_PROJECTION_GROUP_IDS = (
    "FOUNDATION_CONTRACTS",
    "POLICY_BLOCKED_MECHANICS",
    "EVIDENCE_SCAFFOLDING",
    "EXTERNAL_EVIDENCE_AND_GOVERNANCE",
)


class QQQOptionsProjectionContractError(ValueError):
    pass


class QQQOptionsProjectionLayer(StrEnum):
    MAINLINE_GOVERNANCE_FACT = "A"
    SECONDARY_EVIDENCE_FACT = "B"
    BLOCKED_MECHANICS_FACT = "C"


def _required(value: str, field: str) -> None:
    if not value.strip():
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_REQUIRED:{field}")


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise QQQOptionsProjectionContractError(
            f"QQQ_OPTIONS_PROJECTION_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_MAPPING_REQUIRED:{field}")
    return value


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_LIST_REQUIRED:{field}")
    values = tuple(str(item) for item in value)
    if any(not item.strip() for item in values):
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_EMPTY_ITEM:{field}")
    return values


def _unique(values: Sequence[str], field: str) -> None:
    if len(values) != len(set(values)):
        raise QQQOptionsProjectionContractError(f"QQQ_OPTIONS_PROJECTION_DUPLICATE:{field}")


@dataclass(frozen=True)
class QQQOptionsProjectionSource:
    task_id: str
    path: str
    git_blob: str
    byte_count: int

    def __post_init__(self) -> None:
        _required(self.task_id, "source.task_id")
        _required(self.path, "source.path")
        if self.task_id not in QQQ_OPTIONS_PROJECTION_TASK_IDS:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_TASK_INVALID:{self.task_id}"
            )
        if not self.path.startswith("docs/requirements/") or ".." in self.path:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_PATH_INVALID:{self.task_id}"
            )
        if not _GIT_BLOB.fullmatch(self.git_blob):
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_BLOB_INVALID:{self.task_id}"
            )
        if self.byte_count <= 0:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_BYTES_INVALID:{self.task_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "path": self.path,
            "git_blob": self.git_blob,
            "byte_count": self.byte_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> QQQOptionsProjectionSource:
        _exact_keys(payload, {"task_id", "path", "git_blob", "byte_count"}, "source")
        return cls(
            task_id=str(payload["task_id"]),
            path=str(payload["path"]),
            git_blob=str(payload["git_blob"]),
            byte_count=int(str(payload["byte_count"])),
        )


@dataclass(frozen=True)
class QQQOptionsProjectionStatusLayers:
    engineering_baseline: str
    evidence_quality: str
    policy_readiness: str
    external_authority: str
    strategy_conclusion: str

    def __post_init__(self) -> None:
        for field, value in self.to_dict().items():
            _required(str(value), f"layers.{field}")
        if self.strategy_conclusion == "PASS":
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_STRATEGY_PASS_PROHIBITED"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "engineering_baseline": self.engineering_baseline,
            "evidence_quality": self.evidence_quality,
            "policy_readiness": self.policy_readiness,
            "external_authority": self.external_authority,
            "strategy_conclusion": self.strategy_conclusion,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> QQQOptionsProjectionStatusLayers:
        expected = {
            "engineering_baseline",
            "evidence_quality",
            "policy_readiness",
            "external_authority",
            "strategy_conclusion",
        }
        _exact_keys(payload, expected, "layers")
        return cls(**{key: str(payload[key]) for key in expected})


@dataclass(frozen=True)
class QQQOptionsProjectionCard:
    task_id: str
    group_id: str
    layer: QQQOptionsProjectionLayer
    title_zh: str
    positioning_zh: str
    completed_zh: str
    not_proven_zh: str
    blocker_zh: str
    next_reader_action_zh: str
    priority_facts: tuple[str, ...]
    status_layers: QQQOptionsProjectionStatusLayers
    source: QQQOptionsProjectionSource
    source_status_note: str | None = None

    def __post_init__(self) -> None:
        if self.task_id != self.source.task_id:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_TASK_MISMATCH:{self.task_id}"
            )
        if self.task_id not in QQQ_OPTIONS_PROJECTION_TASK_IDS:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_CARD_TASK_INVALID:{self.task_id}"
            )
        if self.group_id not in QQQ_OPTIONS_PROJECTION_GROUP_IDS:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_GROUP_INVALID:{self.task_id}"
            )
        for field in (
            "title_zh",
            "positioning_zh",
            "completed_zh",
            "not_proven_zh",
            "blocker_zh",
            "next_reader_action_zh",
        ):
            _required(str(getattr(self, field)), f"card.{self.task_id}.{field}")
        if not self.priority_facts:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_PRIORITY_FACT_REQUIRED:{self.task_id}"
            )
        _unique(self.priority_facts, f"card.{self.task_id}.priority_facts")
        if self.task_id == "TRADING-2489" and (
            self.source_status_note != "SOURCE_STATUS_MISMATCH_REVIEW_REQUIRED"
        ):
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_2489_MISMATCH_REQUIRED")
        if self.task_id == "TRADING-2492" and self.priority_facts != (
            "PILOT_NO_GO_LICENSE_OR_EVIDENCE",
            "唯一 scope violation 是 PROCESSED_DATA_POINTS",
            "734127 > 250000",
            "1 order / 1 fill",
        ):
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_2492_READER_ORDER_INVALID"
            )
        if self.task_id == "TRADING-2493" and self.priority_facts[:3] != (
            "NO_GO_KEEP_BLOCKED",
            "SIGNED_NO_GO",
            "subordinate capability/technical axes are CONDITIONAL_GO only",
        ):
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_2493_DOMINANCE_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "group_id": self.group_id,
            "layer": self.layer.value,
            "title_zh": self.title_zh,
            "positioning_zh": self.positioning_zh,
            "completed_zh": self.completed_zh,
            "not_proven_zh": self.not_proven_zh,
            "blocker_zh": self.blocker_zh,
            "next_reader_action_zh": self.next_reader_action_zh,
            "priority_facts": list(self.priority_facts),
            "status_layers": self.status_layers.to_dict(),
            "source": self.source.to_dict(),
            "source_status_note": self.source_status_note,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> QQQOptionsProjectionCard:
        expected = {
            "task_id",
            "group_id",
            "layer",
            "title_zh",
            "positioning_zh",
            "completed_zh",
            "not_proven_zh",
            "blocker_zh",
            "next_reader_action_zh",
            "priority_facts",
            "status_layers",
            "source",
            "source_status_note",
        }
        _exact_keys(payload, expected, "card")
        note = payload["source_status_note"]
        return cls(
            task_id=str(payload["task_id"]),
            group_id=str(payload["group_id"]),
            layer=QQQOptionsProjectionLayer(str(payload["layer"])),
            title_zh=str(payload["title_zh"]),
            positioning_zh=str(payload["positioning_zh"]),
            completed_zh=str(payload["completed_zh"]),
            not_proven_zh=str(payload["not_proven_zh"]),
            blocker_zh=str(payload["blocker_zh"]),
            next_reader_action_zh=str(payload["next_reader_action_zh"]),
            priority_facts=_string_tuple(payload["priority_facts"], "card.priority_facts"),
            status_layers=QQQOptionsProjectionStatusLayers.from_dict(
                _mapping(payload["status_layers"], "card.status_layers")
            ),
            source=QQQOptionsProjectionSource.from_dict(_mapping(payload["source"], "card.source")),
            source_status_note=None if note is None else str(note),
        )


@dataclass(frozen=True)
class QQQOptionsProjectionGroup:
    group_id: str
    title_zh: str
    capability_zh: str
    not_proven_zh: str
    owner_need_zh: str
    task_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.group_id not in QQQ_OPTIONS_PROJECTION_GROUP_IDS:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_GROUP_INVALID:{self.group_id}"
            )
        for field in ("title_zh", "capability_zh", "not_proven_zh", "owner_need_zh"):
            _required(str(getattr(self, field)), f"group.{self.group_id}.{field}")
        if not self.task_ids:
            raise QQQOptionsProjectionContractError(
                f"QQQ_OPTIONS_PROJECTION_GROUP_TASKS_REQUIRED:{self.group_id}"
            )
        _unique(self.task_ids, f"group.{self.group_id}.task_ids")

    def to_dict(self) -> dict[str, object]:
        return {
            "group_id": self.group_id,
            "title_zh": self.title_zh,
            "capability_zh": self.capability_zh,
            "not_proven_zh": self.not_proven_zh,
            "owner_need_zh": self.owner_need_zh,
            "task_ids": list(self.task_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> QQQOptionsProjectionGroup:
        expected = {
            "group_id",
            "title_zh",
            "capability_zh",
            "not_proven_zh",
            "owner_need_zh",
            "task_ids",
        }
        _exact_keys(payload, expected, "group")
        return cls(
            group_id=str(payload["group_id"]),
            title_zh=str(payload["title_zh"]),
            capability_zh=str(payload["capability_zh"]),
            not_proven_zh=str(payload["not_proven_zh"]),
            owner_need_zh=str(payload["owner_need_zh"]),
            task_ids=_string_tuple(payload["task_ids"], "group.task_ids"),
        )


@dataclass(frozen=True)
class StrategyResearchQQQOptionsProjectionBundle:
    schema_id: ClassVar[str] = "strategy_research_qqq_options_projection.v1"
    schema_version: ClassVar[str] = "1.0.0"

    snapshot_id: str
    owner_decision: str
    primary_research_start: str
    aggregate_conclusion: str
    aggregate_explanation_zh: str
    source_set_sha256: str
    groups: tuple[QQQOptionsProjectionGroup, ...]
    cards: tuple[QQQOptionsProjectionCard, ...]
    policy_id: str
    policy_version: str
    policy_sha256: str
    investment_conclusion_generated: bool
    external_action: str
    production_effect: str
    broker_action: str
    content_sha256: str

    def __post_init__(self) -> None:
        for field in (
            "snapshot_id",
            "owner_decision",
            "primary_research_start",
            "aggregate_conclusion",
            "aggregate_explanation_zh",
            "source_set_sha256",
            "policy_id",
            "policy_version",
            "policy_sha256",
            "content_sha256",
        ):
            _required(str(getattr(self, field)), f"bundle.{field}")
        for field in ("snapshot_id", "source_set_sha256", "policy_sha256", "content_sha256"):
            if not _SHA256.fullmatch(str(getattr(self, field))):
                raise QQQOptionsProjectionContractError(
                    f"QQQ_OPTIONS_PROJECTION_SHA_INVALID:{field}"
                )
        if self.primary_research_start != "2021-02-22":
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_PRIMARY_START_INVALID")
        if self.aggregate_conclusion != "NO_GO_KEEP_BLOCKED":
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_AGGREGATE_NO_GO_REQUIRED"
            )
        if self.investment_conclusion_generated:
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_INVESTMENT_CONCLUSION_PROHIBITED"
            )
        if (self.external_action, self.production_effect, self.broker_action) != (
            "none",
            "none",
            "none",
        ):
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_SAFETY_INVALID")
        if tuple(item.group_id for item in self.groups) != QQQ_OPTIONS_PROJECTION_GROUP_IDS:
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_GROUP_ORDER_INVALID")
        card_ids = tuple(item.task_id for item in self.cards)
        if card_ids != QQQ_OPTIONS_PROJECTION_TASK_IDS:
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_CARD_ORDER_INVALID")
        grouped_ids = tuple(task_id for group in self.groups for task_id in group.task_ids)
        if grouped_ids != card_ids:
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_GROUP_MEMBERSHIP_INVALID"
            )
        by_id = {item.task_id: item for item in self.cards}
        if any(
            by_id[task_id].group_id != group.group_id
            for group in self.groups
            for task_id in group.task_ids
        ):
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_CARD_GROUP_BINDING_INVALID"
            )
        expected_layers = {
            "TRADING-2481": "A",
            "TRADING-2482": "A",
            "TRADING-2483": "A",
            "TRADING-2484": "B",
            "TRADING-2485": "C",
            "TRADING-2486": "C",
            "TRADING-2487": "C",
            "TRADING-2488": "C",
            "TRADING-2489": "B",
            "TRADING-2490": "B",
            "TRADING-2491": "B",
            "TRADING-2492": "A",
            "TRADING-2493": "A",
        }
        if any(by_id[task_id].layer.value != layer for task_id, layer in expected_layers.items()):
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_OWNER_LAYER_DRIFT")
        if self.compute_content_sha256() != self.content_sha256:
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_CONTENT_SHA256_MISMATCH"
            )

    def _payload_without_hash(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "owner_decision": self.owner_decision,
            "primary_research_start": self.primary_research_start,
            "aggregate_conclusion": self.aggregate_conclusion,
            "aggregate_explanation_zh": self.aggregate_explanation_zh,
            "source_set_sha256": self.source_set_sha256,
            "groups": [item.to_dict() for item in self.groups],
            "cards": [item.to_dict() for item in self.cards],
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
            "investment_conclusion_generated": self.investment_conclusion_generated,
            "external_action": self.external_action,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self._payload_without_hash(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    @classmethod
    def seal(cls, **payload: object) -> StrategyResearchQQQOptionsProjectionBundle:
        provisional = object.__new__(cls)
        for field, value in payload.items():
            object.__setattr__(provisional, field, value)
        object.__setattr__(provisional, "content_sha256", "0" * 64)
        content_sha256 = provisional.compute_content_sha256()
        return cls(**payload, content_sha256=content_sha256)  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, object]:
        return {**self._payload_without_hash(), "content_sha256": self.content_sha256}

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> StrategyResearchQQQOptionsProjectionBundle:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_JSON_INVALID") from exc
        data = _mapping(decoded, "bundle")
        expected = {
            "schema_id",
            "schema_version",
            "snapshot_id",
            "owner_decision",
            "primary_research_start",
            "aggregate_conclusion",
            "aggregate_explanation_zh",
            "source_set_sha256",
            "groups",
            "cards",
            "policy_id",
            "policy_version",
            "policy_sha256",
            "investment_conclusion_generated",
            "external_action",
            "production_effect",
            "broker_action",
            "content_sha256",
        }
        _exact_keys(data, expected, "bundle")
        if data["schema_id"] != cls.schema_id or data["schema_version"] != cls.schema_version:
            raise QQQOptionsProjectionContractError("QQQ_OPTIONS_PROJECTION_SCHEMA_INVALID")
        bundle = cls(
            snapshot_id=str(data["snapshot_id"]),
            owner_decision=str(data["owner_decision"]),
            primary_research_start=str(data["primary_research_start"]),
            aggregate_conclusion=str(data["aggregate_conclusion"]),
            aggregate_explanation_zh=str(data["aggregate_explanation_zh"]),
            source_set_sha256=str(data["source_set_sha256"]),
            groups=tuple(
                QQQOptionsProjectionGroup.from_dict(item)
                for item in _mapping_tuple(data["groups"], "bundle.groups")
            ),
            cards=tuple(
                QQQOptionsProjectionCard.from_dict(item)
                for item in _mapping_tuple(data["cards"], "bundle.cards")
            ),
            policy_id=str(data["policy_id"]),
            policy_version=str(data["policy_version"]),
            policy_sha256=str(data["policy_sha256"]),
            investment_conclusion_generated=bool(data["investment_conclusion_generated"]),
            external_action=str(data["external_action"]),
            production_effect=str(data["production_effect"]),
            broker_action=str(data["broker_action"]),
            content_sha256=str(data["content_sha256"]),
        )
        if bundle.canonical_bytes != payload:
            raise QQQOptionsProjectionContractError(
                "QQQ_OPTIONS_PROJECTION_CANONICAL_BYTES_REQUIRED"
            )
        return bundle


__all__ = [
    "QQQ_OPTIONS_PROJECTION_GROUP_IDS",
    "QQQ_OPTIONS_PROJECTION_TASK_IDS",
    "QQQOptionsProjectionCard",
    "QQQOptionsProjectionContractError",
    "QQQOptionsProjectionGroup",
    "QQQOptionsProjectionLayer",
    "QQQOptionsProjectionSource",
    "QQQOptionsProjectionStatusLayers",
    "StrategyResearchQQQOptionsProjectionBundle",
]
