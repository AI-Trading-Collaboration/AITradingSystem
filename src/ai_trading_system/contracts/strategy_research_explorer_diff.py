from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.core.production_effect import ProductionEffect

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LINEAGE_ONLY_SOURCE_FIELDS = frozenset({"exact_commit", "as_of", "known_at", "available_at"})


class StrategyResearchExplorerDiffContractError(ValueError):
    pass


class ExplorerDiffEntityKind(StrEnum):
    SOURCE = "SOURCE"
    NODE = "NODE"
    EDGE = "EDGE"
    RESULT = "RESULT"
    ATTRIBUTION = "ATTRIBUTION"


class ExplorerDiffChangeKind(StrEnum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    CHANGED = "CHANGED"


class ExplorerDiffSignificance(StrEnum):
    SEMANTIC = "SEMANTIC"
    LINEAGE_ONLY = "LINEAGE_ONLY"
    STRUCTURAL = "STRUCTURAL"


def _required_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_REQUIRED_FIELD:{field}"
        )


def _sha256(value: str | None, field: str) -> None:
    if value is not None and not _SHA256_PATTERN.fullmatch(value):
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_SHA256_INVALID:{field}"
        )


def _aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_TIMEZONE_REQUIRED:{field}"
        )


def _parse_datetime(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_DATETIME_INVALID:{field}"
        ) from exc
    _aware_datetime(parsed, field)
    return parsed


def _canonical_json_value(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _validate_canonical_json_value(value: str, field: str) -> None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_JSON_VALUE_INVALID:{field}"
        ) from exc
    if _canonical_json_value(parsed) != value:
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_JSON_VALUE_NONCANONICAL:{field}"
        )


@dataclass(frozen=True)
class ExplorerDiffFieldChange:
    schema_version: ClassVar[str] = "strategy_research_explorer_field_change.v1"

    field_name: str
    before_json: str
    after_json: str

    def __post_init__(self) -> None:
        _required_text(self.field_name, "field_name")
        _validate_canonical_json_value(self.before_json, f"{self.field_name}.before_json")
        _validate_canonical_json_value(self.after_json, f"{self.field_name}.after_json")
        if self.before_json == self.after_json:
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_FIELD_VALUE_UNCHANGED:{self.field_name}"
            )

    @classmethod
    def build(
        cls,
        *,
        field_name: str,
        before_value: object,
        after_value: object,
    ) -> ExplorerDiffFieldChange:
        return cls(
            field_name=field_name,
            before_json=_canonical_json_value(before_value),
            after_json=_canonical_json_value(after_value),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "field_name": self.field_name,
            "before_json": self.before_json,
            "after_json": self.after_json,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplorerDiffFieldChange:
        return cls(
            field_name=str(payload.get("field_name", "")),
            before_json=str(payload.get("before_json", "")),
            after_json=str(payload.get("after_json", "")),
        )


@dataclass(frozen=True)
class ExplorerEntityChange:
    schema_version: ClassVar[str] = "strategy_research_explorer_entity_change.v1"

    change_id: str
    entity_kind: ExplorerDiffEntityKind
    entity_id: str
    change_kind: ExplorerDiffChangeKind
    significance: ExplorerDiffSignificance
    before_sha256: str | None
    after_sha256: str | None
    field_changes: tuple[ExplorerDiffFieldChange, ...] = ()

    def __post_init__(self) -> None:
        _sha256(self.change_id, "change_id")
        _required_text(self.entity_id, "entity_id")
        _sha256(self.before_sha256, "before_sha256")
        _sha256(self.after_sha256, "after_sha256")
        field_names = [item.field_name for item in self.field_changes]
        if field_names != sorted(field_names, key=str.casefold):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_FIELD_ORDER_INVALID:{self.entity_id}"
            )
        if len(field_names) != len(set(field_names)):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_DUPLICATE_FIELD:{self.entity_id}"
            )
        self._validate_transition()
        if self.change_id != self.compute_change_id():
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_CHANGE_ID_MISMATCH:{self.entity_id}"
            )

    @property
    def changed_fields(self) -> tuple[str, ...]:
        return tuple(item.field_name for item in self.field_changes)

    def _validate_transition(self) -> None:
        if self.change_kind is ExplorerDiffChangeKind.ADDED:
            valid = (
                self.before_sha256 is None
                and self.after_sha256 is not None
                and not self.field_changes
                and self.significance is ExplorerDiffSignificance.STRUCTURAL
            )
        elif self.change_kind is ExplorerDiffChangeKind.REMOVED:
            valid = (
                self.before_sha256 is not None
                and self.after_sha256 is None
                and not self.field_changes
                and self.significance is ExplorerDiffSignificance.STRUCTURAL
            )
        else:
            valid = (
                self.before_sha256 is not None
                and self.after_sha256 is not None
                and self.before_sha256 != self.after_sha256
                and bool(self.field_changes)
                and self.significance is not ExplorerDiffSignificance.STRUCTURAL
            )
        if not valid:
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_CHANGE_TRANSITION_INVALID:{self.entity_id}"
            )
        if self.significance is ExplorerDiffSignificance.LINEAGE_ONLY and (
            self.entity_kind is not ExplorerDiffEntityKind.SOURCE
            or not set(self.changed_fields) <= _LINEAGE_ONLY_SOURCE_FIELDS
        ):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_LINEAGE_CLASSIFICATION_INVALID:{self.entity_id}"
            )
        if (
            self.change_kind is ExplorerDiffChangeKind.CHANGED
            and self.entity_kind is ExplorerDiffEntityKind.SOURCE
            and set(self.changed_fields) <= _LINEAGE_ONLY_SOURCE_FIELDS
            and self.significance is not ExplorerDiffSignificance.LINEAGE_ONLY
        ):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_LINEAGE_CHANGE_NOT_DOWNGRADED:{self.entity_id}"
            )

    @classmethod
    def build(
        cls,
        *,
        entity_kind: ExplorerDiffEntityKind,
        entity_id: str,
        change_kind: ExplorerDiffChangeKind,
        significance: ExplorerDiffSignificance,
        before_sha256: str | None,
        after_sha256: str | None,
        field_changes: Sequence[ExplorerDiffFieldChange] = (),
    ) -> ExplorerEntityChange:
        provisional = object.__new__(cls)
        values = {
            "change_id": "0" * 64,
            "entity_kind": entity_kind,
            "entity_id": entity_id,
            "change_kind": change_kind,
            "significance": significance,
            "before_sha256": before_sha256,
            "after_sha256": after_sha256,
            "field_changes": tuple(
                sorted(field_changes, key=lambda item: item.field_name.casefold())
            ),
        }
        for key, value in values.items():
            object.__setattr__(provisional, key, value)
        change_id = provisional.compute_change_id()
        return cls(
            change_id=change_id,
            **{key: value for key, value in values.items() if key != "change_id"},
        )

    def _payload_without_change_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "entity_kind": self.entity_kind.value,
            "entity_id": self.entity_id,
            "change_kind": self.change_kind.value,
            "significance": self.significance.value,
            "before_sha256": self.before_sha256,
            "after_sha256": self.after_sha256,
            "changed_fields": list(self.changed_fields),
            "field_changes": [item.to_dict() for item in self.field_changes],
        }

    def compute_change_id(self) -> str:
        return hashlib.sha256(
            (
                json.dumps(
                    self._payload_without_change_id(),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {"change_id": self.change_id, **self._payload_without_change_id()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplorerEntityChange:
        raw_fields = payload.get("field_changes")
        if not isinstance(raw_fields, list):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_FIELD_CHANGES_LIST_REQUIRED"
            )
        item = cls(
            change_id=str(payload.get("change_id", "")),
            entity_kind=ExplorerDiffEntityKind(str(payload.get("entity_kind", ""))),
            entity_id=str(payload.get("entity_id", "")),
            change_kind=ExplorerDiffChangeKind(str(payload.get("change_kind", ""))),
            significance=ExplorerDiffSignificance(str(payload.get("significance", ""))),
            before_sha256=(
                None if payload.get("before_sha256") is None else str(payload.get("before_sha256"))
            ),
            after_sha256=(
                None if payload.get("after_sha256") is None else str(payload.get("after_sha256"))
            ),
            field_changes=tuple(
                ExplorerDiffFieldChange.from_dict(_mapping(value, "field_changes"))
                for value in raw_fields
            ),
        )
        serialized_changed_fields = payload.get("changed_fields")
        if (
            not isinstance(serialized_changed_fields, list)
            or tuple(str(value) for value in serialized_changed_fields) != item.changed_fields
        ):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_CHANGED_FIELDS_MISMATCH:{item.entity_id}"
            )
        return item


@dataclass(frozen=True)
class ExplorerDiffEntitySummary:
    schema_version: ClassVar[str] = "strategy_research_explorer_diff_entity_summary.v1"

    entity_kind: ExplorerDiffEntityKind
    before_count: int
    after_count: int
    unchanged_count: int
    added_count: int
    removed_count: int
    changed_count: int

    def __post_init__(self) -> None:
        counts = (
            self.before_count,
            self.after_count,
            self.unchanged_count,
            self.added_count,
            self.removed_count,
            self.changed_count,
        )
        if any(type(value) is not int or value < 0 for value in counts):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_SUMMARY_COUNT_INVALID:{self.entity_kind.value}"
            )
        if self.before_count != (
            self.unchanged_count + self.removed_count + self.changed_count
        ) or self.after_count != (self.unchanged_count + self.added_count + self.changed_count):
            raise StrategyResearchExplorerDiffContractError(
                f"STRATEGY_EXPLORER_DIFF_SUMMARY_ARITHMETIC_INVALID:{self.entity_kind.value}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "entity_kind": self.entity_kind.value,
            "before_count": self.before_count,
            "after_count": self.after_count,
            "unchanged_count": self.unchanged_count,
            "added_count": self.added_count,
            "removed_count": self.removed_count,
            "changed_count": self.changed_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplorerDiffEntitySummary:
        return cls(
            entity_kind=ExplorerDiffEntityKind(str(payload.get("entity_kind", ""))),
            before_count=_integer(payload.get("before_count"), "before_count"),
            after_count=_integer(payload.get("after_count"), "after_count"),
            unchanged_count=_integer(payload.get("unchanged_count"), "unchanged_count"),
            added_count=_integer(payload.get("added_count"), "added_count"),
            removed_count=_integer(payload.get("removed_count"), "removed_count"),
            changed_count=_integer(payload.get("changed_count"), "changed_count"),
        )


@dataclass(frozen=True)
class StrategyResearchExplorerDiff:
    schema_version: ClassVar[str] = "strategy_research_explorer_diff.v1"

    diff_id: str
    before_snapshot_id: str
    after_snapshot_id: str
    before_generated_at: datetime
    after_generated_at: datetime
    changes: tuple[ExplorerEntityChange, ...]
    entity_summaries: tuple[ExplorerDiffEntitySummary, ...]
    manual_review_only: bool = True
    commands_executed: bool = False
    source_state_mutated: bool = False
    production_effect: ProductionEffect = ProductionEffect.NONE
    broker_action: str = "none"

    def __post_init__(self) -> None:
        _sha256(self.diff_id, "diff_id")
        _sha256(self.before_snapshot_id, "before_snapshot_id")
        _sha256(self.after_snapshot_id, "after_snapshot_id")
        _aware_datetime(self.before_generated_at, "before_generated_at")
        _aware_datetime(self.after_generated_at, "after_generated_at")
        if self.before_snapshot_id == self.after_snapshot_id:
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_SAME_SNAPSHOT_FORBIDDEN"
            )
        if self.before_generated_at > self.after_generated_at:
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_TIME_ORDER_INVALID"
            )
        if not self.changes:
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_EMPTY_FORBIDDEN"
            )
        if (
            not self.manual_review_only
            or self.commands_executed
            or self.source_state_mutated
            or self.production_effect is not ProductionEffect.NONE
            or self.broker_action != "none"
        ):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_READ_ONLY_BOUNDARY_VIOLATION"
            )
        change_keys = [(item.entity_kind.value, item.entity_id) for item in self.changes]
        if change_keys != sorted(change_keys, key=lambda item: (item[0], item[1].casefold())):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_CHANGE_ORDER_INVALID"
            )
        if len(change_keys) != len(set(change_keys)):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_DUPLICATE_ENTITY_CHANGE"
            )
        summary_kinds = [item.entity_kind for item in self.entity_summaries]
        if summary_kinds != list(ExplorerDiffEntityKind):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_SUMMARY_KIND_SET_INVALID"
            )
        self._validate_summary_binding()
        if self.diff_id != self.compute_diff_id():
            raise StrategyResearchExplorerDiffContractError("STRATEGY_EXPLORER_DIFF_ID_MISMATCH")

    def _validate_summary_binding(self) -> None:
        for summary in self.entity_summaries:
            changes = [item for item in self.changes if item.entity_kind is summary.entity_kind]
            observed = {
                kind: sum(item.change_kind is kind for item in changes)
                for kind in ExplorerDiffChangeKind
            }
            if (
                summary.added_count != observed[ExplorerDiffChangeKind.ADDED]
                or summary.removed_count != observed[ExplorerDiffChangeKind.REMOVED]
                or summary.changed_count != observed[ExplorerDiffChangeKind.CHANGED]
            ):
                raise StrategyResearchExplorerDiffContractError(
                    "STRATEGY_EXPLORER_DIFF_SUMMARY_CHANGE_BINDING_INVALID:"
                    f"{summary.entity_kind.value}"
                )

    @property
    def total_change_count(self) -> int:
        return len(self.changes)

    @property
    def added_count(self) -> int:
        return sum(item.change_kind is ExplorerDiffChangeKind.ADDED for item in self.changes)

    @property
    def removed_count(self) -> int:
        return sum(item.change_kind is ExplorerDiffChangeKind.REMOVED for item in self.changes)

    @property
    def changed_count(self) -> int:
        return sum(item.change_kind is ExplorerDiffChangeKind.CHANGED for item in self.changes)

    @property
    def semantic_count(self) -> int:
        return sum(item.significance is ExplorerDiffSignificance.SEMANTIC for item in self.changes)

    @property
    def lineage_only_count(self) -> int:
        return sum(
            item.significance is ExplorerDiffSignificance.LINEAGE_ONLY for item in self.changes
        )

    @property
    def structural_count(self) -> int:
        return sum(
            item.significance is ExplorerDiffSignificance.STRUCTURAL for item in self.changes
        )

    @classmethod
    def build(
        cls,
        *,
        before_snapshot_id: str,
        after_snapshot_id: str,
        before_generated_at: datetime,
        after_generated_at: datetime,
        changes: Sequence[ExplorerEntityChange],
        entity_summaries: Sequence[ExplorerDiffEntitySummary],
    ) -> StrategyResearchExplorerDiff:
        sorted_changes = tuple(
            sorted(
                changes,
                key=lambda item: (item.entity_kind.value, item.entity_id.casefold()),
            )
        )
        summary_map = {item.entity_kind: item for item in entity_summaries}
        if len(summary_map) != len(entity_summaries):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_DUPLICATE_SUMMARY_KIND"
            )
        sorted_summaries = tuple(
            summary_map[kind] for kind in ExplorerDiffEntityKind if kind in summary_map
        )
        provisional = object.__new__(cls)
        values = {
            "diff_id": "0" * 64,
            "before_snapshot_id": before_snapshot_id,
            "after_snapshot_id": after_snapshot_id,
            "before_generated_at": before_generated_at,
            "after_generated_at": after_generated_at,
            "changes": sorted_changes,
            "entity_summaries": sorted_summaries,
            "manual_review_only": True,
            "commands_executed": False,
            "source_state_mutated": False,
            "production_effect": ProductionEffect.NONE,
            "broker_action": "none",
        }
        for key, value in values.items():
            object.__setattr__(provisional, key, value)
        diff_id = provisional.compute_diff_id()
        return cls(
            diff_id=diff_id,
            **{key: value for key, value in values.items() if key != "diff_id"},
        )

    def _payload_without_diff_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "before_snapshot_id": self.before_snapshot_id,
            "after_snapshot_id": self.after_snapshot_id,
            "before_generated_at": self.before_generated_at.isoformat(),
            "after_generated_at": self.after_generated_at.isoformat(),
            "changes": [item.to_dict() for item in self.changes],
            "entity_summaries": [item.to_dict() for item in self.entity_summaries],
            "summary": {
                "total_change_count": self.total_change_count,
                "added_count": self.added_count,
                "removed_count": self.removed_count,
                "changed_count": self.changed_count,
                "semantic_count": self.semantic_count,
                "lineage_only_count": self.lineage_only_count,
                "structural_count": self.structural_count,
            },
            "manual_review_only": self.manual_review_only,
            "commands_executed": self.commands_executed,
            "source_state_mutated": self.source_state_mutated,
            "production_effect": self.production_effect.value,
            "broker_action": self.broker_action,
        }

    def compute_diff_id(self) -> str:
        return hashlib.sha256(
            (
                json.dumps(
                    self._payload_without_diff_id(),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {"diff_id": self.diff_id, **self._payload_without_diff_id()}

    def canonical_json_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> StrategyResearchExplorerDiff:
        raw_changes = payload.get("changes")
        raw_summaries = payload.get("entity_summaries")
        if not isinstance(raw_changes, list) or not isinstance(raw_summaries, list):
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_COLLECTION_LIST_REQUIRED"
            )
        item = cls(
            diff_id=str(payload.get("diff_id", "")),
            before_snapshot_id=str(payload.get("before_snapshot_id", "")),
            after_snapshot_id=str(payload.get("after_snapshot_id", "")),
            before_generated_at=_parse_datetime(
                payload.get("before_generated_at"), "before_generated_at"
            ),
            after_generated_at=_parse_datetime(
                payload.get("after_generated_at"), "after_generated_at"
            ),
            changes=tuple(
                ExplorerEntityChange.from_dict(_mapping(value, "changes")) for value in raw_changes
            ),
            entity_summaries=tuple(
                ExplorerDiffEntitySummary.from_dict(_mapping(value, "entity_summaries"))
                for value in raw_summaries
            ),
            manual_review_only=payload.get("manual_review_only") is True,
            commands_executed=payload.get("commands_executed") is True,
            source_state_mutated=payload.get("source_state_mutated") is True,
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
            broker_action=str(payload.get("broker_action", "")),
        )
        expected_summary = item._payload_without_diff_id()["summary"]
        if payload.get("summary") != expected_summary:
            raise StrategyResearchExplorerDiffContractError(
                "STRATEGY_EXPLORER_DIFF_TOP_LEVEL_SUMMARY_MISMATCH"
            )
        return item


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_MAPPING_REQUIRED:{field}"
        )
    return value


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise StrategyResearchExplorerDiffContractError(
            f"STRATEGY_EXPLORER_DIFF_INTEGER_REQUIRED:{field}"
        )
    return value


__all__ = [
    "ExplorerDiffChangeKind",
    "ExplorerDiffEntityKind",
    "ExplorerDiffEntitySummary",
    "ExplorerDiffFieldChange",
    "ExplorerDiffSignificance",
    "ExplorerEntityChange",
    "StrategyResearchExplorerDiff",
    "StrategyResearchExplorerDiffContractError",
]
