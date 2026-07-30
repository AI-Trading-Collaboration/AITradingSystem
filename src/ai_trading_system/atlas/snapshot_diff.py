from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from ai_trading_system.contracts.strategy_research_explorer import (
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffEntitySummary,
    ExplorerDiffFieldChange,
    ExplorerDiffSignificance,
    ExplorerEntityChange,
    StrategyResearchExplorerDiff,
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LINEAGE_ONLY_SOURCE_FIELDS = frozenset({"exact_commit", "as_of", "known_at", "available_at"})
_ENTITY_COLLECTIONS = (
    (ExplorerDiffEntityKind.SOURCE, "sources", "source_ref_id"),
    (ExplorerDiffEntityKind.NODE, "nodes", "node_id"),
    (ExplorerDiffEntityKind.EDGE, "edges", "edge_id"),
    (ExplorerDiffEntityKind.RESULT, "results", "result_id"),
    (ExplorerDiffEntityKind.ATTRIBUTION, "attributions", "attribution_id"),
)


class AtlasSnapshotDiffError(ValueError):
    pass


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _entity_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _entity_maps(
    snapshot: StrategyResearchExplorerSnapshot,
) -> dict[ExplorerDiffEntityKind, dict[str, dict[str, object]]]:
    result: dict[ExplorerDiffEntityKind, dict[str, dict[str, object]]] = {}
    for kind, collection_name, id_field in _ENTITY_COLLECTIONS:
        entities = getattr(snapshot, collection_name)
        mapped: dict[str, dict[str, object]] = {}
        for item in entities:
            payload = item.to_dict()
            entity_id = str(payload[id_field])
            if entity_id in mapped:
                raise AtlasSnapshotDiffError(
                    f"ATLAS_DIFF_DUPLICATE_ENTITY:{kind.value}:{entity_id}"
                )
            mapped[entity_id] = payload
        result[kind] = mapped
    return result


def _field_changes(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> tuple[ExplorerDiffFieldChange, ...]:
    fields = sorted(set(before) | set(after), key=str.casefold)
    return tuple(
        ExplorerDiffFieldChange.build(
            field_name=field,
            before_value=before.get(field),
            after_value=after.get(field),
        )
        for field in fields
        if before.get(field) != after.get(field)
    )


def _changed_significance(
    kind: ExplorerDiffEntityKind,
    field_changes: Sequence[ExplorerDiffFieldChange],
) -> ExplorerDiffSignificance:
    changed_fields = {item.field_name for item in field_changes}
    if (
        kind is ExplorerDiffEntityKind.SOURCE
        and changed_fields
        and changed_fields <= _LINEAGE_ONLY_SOURCE_FIELDS
    ):
        return ExplorerDiffSignificance.LINEAGE_ONLY
    return ExplorerDiffSignificance.SEMANTIC


def build_snapshot_diff(
    before: StrategyResearchExplorerSnapshot,
    after: StrategyResearchExplorerSnapshot,
) -> StrategyResearchExplorerDiff:
    if before.snapshot_id == after.snapshot_id:
        raise AtlasSnapshotDiffError("ATLAS_DIFF_SAME_SNAPSHOT_FORBIDDEN")
    before_maps = _entity_maps(before)
    after_maps = _entity_maps(after)
    changes: list[ExplorerEntityChange] = []
    summaries: list[ExplorerDiffEntitySummary] = []
    for kind in ExplorerDiffEntityKind:
        before_map = before_maps[kind]
        after_map = after_maps[kind]
        before_ids = set(before_map)
        after_ids = set(after_map)
        common_ids = before_ids & after_ids
        changed_ids = {
            entity_id
            for entity_id in common_ids
            if _entity_sha256(before_map[entity_id]) != _entity_sha256(after_map[entity_id])
        }
        for entity_id in sorted(after_ids - before_ids, key=str.casefold):
            changes.append(
                ExplorerEntityChange.build(
                    entity_kind=kind,
                    entity_id=entity_id,
                    change_kind=ExplorerDiffChangeKind.ADDED,
                    significance=ExplorerDiffSignificance.STRUCTURAL,
                    before_sha256=None,
                    after_sha256=_entity_sha256(after_map[entity_id]),
                )
            )
        for entity_id in sorted(before_ids - after_ids, key=str.casefold):
            changes.append(
                ExplorerEntityChange.build(
                    entity_kind=kind,
                    entity_id=entity_id,
                    change_kind=ExplorerDiffChangeKind.REMOVED,
                    significance=ExplorerDiffSignificance.STRUCTURAL,
                    before_sha256=_entity_sha256(before_map[entity_id]),
                    after_sha256=None,
                )
            )
        for entity_id in sorted(changed_ids, key=str.casefold):
            fields = _field_changes(before_map[entity_id], after_map[entity_id])
            changes.append(
                ExplorerEntityChange.build(
                    entity_kind=kind,
                    entity_id=entity_id,
                    change_kind=ExplorerDiffChangeKind.CHANGED,
                    significance=_changed_significance(kind, fields),
                    before_sha256=_entity_sha256(before_map[entity_id]),
                    after_sha256=_entity_sha256(after_map[entity_id]),
                    field_changes=fields,
                )
            )
        summaries.append(
            ExplorerDiffEntitySummary(
                entity_kind=kind,
                before_count=len(before_map),
                after_count=len(after_map),
                unchanged_count=len(common_ids - changed_ids),
                added_count=len(after_ids - before_ids),
                removed_count=len(before_ids - after_ids),
                changed_count=len(changed_ids),
            )
        )
    if not changes:
        raise AtlasSnapshotDiffError("ATLAS_DIFF_EMPTY_FORBIDDEN")
    return StrategyResearchExplorerDiff.build(
        before_snapshot_id=before.snapshot_id,
        after_snapshot_id=after.snapshot_id,
        before_generated_at=before.generated_at,
        after_generated_at=after.generated_at,
        changes=changes,
        entity_summaries=summaries,
    )


@dataclass(frozen=True)
class AtlasDiffInput:
    schema_version: ClassVar[str] = "atlas_explorer_diff_input.v1"

    role: str
    source_path: str
    file_sha256: str
    size_bytes: int
    snapshot_id: str

    def __post_init__(self) -> None:
        if self.role not in {"before", "after"}:
            raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_ROLE_INVALID:{self.role}")
        if not self.source_path.strip() or Path(self.source_path).is_absolute():
            raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_PATH_NOT_PORTABLE:{self.role}")
        if not _SHA256_PATTERN.fullmatch(self.file_sha256):
            raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_SHA256_INVALID:{self.role}")
        if not _SHA256_PATTERN.fullmatch(self.snapshot_id):
            raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_SNAPSHOT_ID_INVALID:{self.role}")
        if type(self.size_bytes) is not int or self.size_bytes <= 0:
            raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_SIZE_INVALID:{self.role}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "source_path": self.source_path,
            "file_sha256": self.file_sha256,
            "size_bytes": self.size_bytes,
            "snapshot_id": self.snapshot_id,
        }


@dataclass(frozen=True)
class AtlasDiffInputReceipt:
    schema_version: ClassVar[str] = "atlas_explorer_diff_input_receipt.v1"

    receipt_id: str
    recorded_at: datetime
    inputs: tuple[AtlasDiffInput, AtlasDiffInput]
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        if not _SHA256_PATTERN.fullmatch(self.receipt_id):
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_ID_INVALID")
        if self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None:
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_TIMEZONE_REQUIRED")
        if tuple(item.role for item in self.inputs) != ("before", "after"):
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_INPUT_ORDER_INVALID")
        if self.inputs[0].snapshot_id == self.inputs[1].snapshot_id:
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_SAME_SNAPSHOT_FORBIDDEN")
        if self.production_effect != "none" or self.broker_action != "none":
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_SAFETY_INVALID")
        if self.receipt_id != self.compute_receipt_id():
            raise AtlasSnapshotDiffError("ATLAS_DIFF_RECEIPT_ID_MISMATCH")

    @classmethod
    def build(
        cls,
        *,
        recorded_at: datetime,
        inputs: tuple[AtlasDiffInput, AtlasDiffInput],
    ) -> AtlasDiffInputReceipt:
        provisional = object.__new__(cls)
        object.__setattr__(provisional, "receipt_id", "0" * 64)
        object.__setattr__(provisional, "recorded_at", recorded_at)
        object.__setattr__(provisional, "inputs", inputs)
        object.__setattr__(provisional, "production_effect", "none")
        object.__setattr__(provisional, "broker_action", "none")
        return cls(
            receipt_id=provisional.compute_receipt_id(),
            recorded_at=recorded_at,
            inputs=inputs,
        )

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "inputs": [item.to_dict() for item in self.inputs],
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }

    def compute_receipt_id(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self._identity_payload())).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            **self._identity_payload(),
            "recorded_at": self.recorded_at.isoformat(),
            "identity_excludes": ["recorded_at"],
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class AtlasSnapshotDiffBundle:
    before: StrategyResearchExplorerSnapshot
    after: StrategyResearchExplorerSnapshot
    diff: StrategyResearchExplorerDiff
    input_receipt: AtlasDiffInputReceipt


def _portable_path(path: Path, path_root: Path | None) -> str:
    resolved = path.resolve()
    if path_root is not None:
        try:
            return resolved.relative_to(path_root.resolve()).as_posix()
        except ValueError as exc:
            raise AtlasSnapshotDiffError(
                f"ATLAS_DIFF_INPUT_OUTSIDE_ROOT:{path.as_posix()}"
            ) from exc
    return path.name


def _load_snapshot(path: Path) -> tuple[bytes, StrategyResearchExplorerSnapshot]:
    payload = path.read_bytes()
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_JSON_INVALID:{path.name}") from exc
    if not isinstance(parsed, Mapping):
        raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_MAPPING_REQUIRED:{path.name}")
    snapshot = StrategyResearchExplorerSnapshot.from_dict(parsed)
    if snapshot.canonical_json_bytes() != payload:
        raise AtlasSnapshotDiffError(f"ATLAS_DIFF_INPUT_NONCANONICAL_BYTES:{path.name}")
    return payload, snapshot


def load_snapshot_diff_bundle(
    *,
    before_path: Path,
    after_path: Path,
    recorded_at: datetime,
    path_root: Path | None = None,
) -> AtlasSnapshotDiffBundle:
    before_bytes, before = _load_snapshot(before_path)
    after_bytes, after = _load_snapshot(after_path)
    inputs = (
        AtlasDiffInput(
            role="before",
            source_path=_portable_path(before_path, path_root),
            file_sha256=hashlib.sha256(before_bytes).hexdigest(),
            size_bytes=len(before_bytes),
            snapshot_id=before.snapshot_id,
        ),
        AtlasDiffInput(
            role="after",
            source_path=_portable_path(after_path, path_root),
            file_sha256=hashlib.sha256(after_bytes).hexdigest(),
            size_bytes=len(after_bytes),
            snapshot_id=after.snapshot_id,
        ),
    )
    diff = build_snapshot_diff(before, after)
    receipt = AtlasDiffInputReceipt.build(recorded_at=recorded_at, inputs=inputs)
    return AtlasSnapshotDiffBundle(
        before=before,
        after=after,
        diff=diff,
        input_receipt=receipt,
    )


__all__ = [
    "AtlasDiffInput",
    "AtlasDiffInputReceipt",
    "AtlasSnapshotDiffBundle",
    "AtlasSnapshotDiffError",
    "build_snapshot_diff",
    "load_snapshot_diff_bundle",
]
