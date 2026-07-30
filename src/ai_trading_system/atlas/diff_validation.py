from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from ai_trading_system.contracts.strategy_research_explorer import (
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffSignificance,
    StrategyResearchExplorerDiff,
)

from .snapshot_diff import AtlasSnapshotDiffBundle

_LINEAGE_ONLY_SOURCE_FIELDS = frozenset({"exact_commit", "as_of", "known_at", "available_at"})
_ENTITY_SPECS = {
    ExplorerDiffEntityKind.SOURCE: ("sources", "source_ref_id"),
    ExplorerDiffEntityKind.NODE: ("nodes", "node_id"),
    ExplorerDiffEntityKind.EDGE: ("edges", "edge_id"),
    ExplorerDiffEntityKind.RESULT: ("results", "result_id"),
    ExplorerDiffEntityKind.ATTRIBUTION: ("attributions", "attribution_id"),
}


@dataclass(frozen=True)
class AtlasDiffValidationResult:
    schema_version: str
    status: str
    before_snapshot_id: str
    after_snapshot_id: str
    diff_id: str
    change_count: int
    error_count: int
    errors: tuple[str, ...]
    production_effect: str = "none"
    broker_action: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "before_snapshot_id": self.before_snapshot_id,
            "after_snapshot_id": self.after_snapshot_id,
            "diff_id": self.diff_id,
            "change_count": self.change_count,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }


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


def _canonical_json_value(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _entity_hash(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _maps(
    snapshot: StrategyResearchExplorerSnapshot,
) -> dict[ExplorerDiffEntityKind, dict[str, Mapping[str, object]]]:
    payload = snapshot.to_dict()
    result: dict[ExplorerDiffEntityKind, dict[str, Mapping[str, object]]] = {}
    for kind, (collection_name, id_field) in _ENTITY_SPECS.items():
        raw_items = payload[collection_name]
        if not isinstance(raw_items, list):
            raise TypeError(f"{collection_name} must be a list")
        mapped: dict[str, Mapping[str, object]] = {}
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                raise TypeError(f"{collection_name} item must be a mapping")
            entity_id = str(raw[id_field])
            if entity_id in mapped:
                raise ValueError(f"duplicate entity: {kind.value}:{entity_id}")
            mapped[entity_id] = raw
        result[kind] = mapped
    return result


def _expected_transition(
    *,
    entity_id: str,
    before_map: Mapping[str, Mapping[str, object]],
    after_map: Mapping[str, Mapping[str, object]],
) -> ExplorerDiffChangeKind | None:
    if entity_id not in before_map:
        return ExplorerDiffChangeKind.ADDED
    if entity_id not in after_map:
        return ExplorerDiffChangeKind.REMOVED
    if _entity_hash(before_map[entity_id]) != _entity_hash(after_map[entity_id]):
        return ExplorerDiffChangeKind.CHANGED
    return None


def _validate_change(
    *,
    kind: ExplorerDiffEntityKind,
    entity_id: str,
    change: object,
    before_map: Mapping[str, Mapping[str, object]],
    after_map: Mapping[str, Mapping[str, object]],
    errors: list[str],
) -> None:
    expected_transition = _expected_transition(
        entity_id=entity_id,
        before_map=before_map,
        after_map=after_map,
    )
    if expected_transition is None or change.change_kind is not expected_transition:
        errors.append(f"ATLAS_DIFF_TRANSITION_MISMATCH:{kind.value}:{entity_id}")
        return
    before_payload = before_map.get(entity_id)
    after_payload = after_map.get(entity_id)
    expected_before_hash = None if before_payload is None else _entity_hash(before_payload)
    expected_after_hash = None if after_payload is None else _entity_hash(after_payload)
    if change.before_sha256 != expected_before_hash:
        errors.append(f"ATLAS_DIFF_BEFORE_HASH_MISMATCH:{kind.value}:{entity_id}")
    if change.after_sha256 != expected_after_hash:
        errors.append(f"ATLAS_DIFF_AFTER_HASH_MISMATCH:{kind.value}:{entity_id}")
    if expected_transition is not ExplorerDiffChangeKind.CHANGED:
        return
    assert before_payload is not None
    assert after_payload is not None
    expected_fields = tuple(
        sorted(
            (
                field
                for field in set(before_payload) | set(after_payload)
                if before_payload.get(field) != after_payload.get(field)
            ),
            key=str.casefold,
        )
    )
    if change.changed_fields != expected_fields:
        errors.append(f"ATLAS_DIFF_FIELD_SET_MISMATCH:{kind.value}:{entity_id}")
    for field_change in change.field_changes:
        if field_change.before_json != _canonical_json_value(
            before_payload.get(field_change.field_name)
        ):
            errors.append(
                f"ATLAS_DIFF_FIELD_BEFORE_VALUE_MISMATCH:{kind.value}:"
                f"{entity_id}:{field_change.field_name}"
            )
        if field_change.after_json != _canonical_json_value(
            after_payload.get(field_change.field_name)
        ):
            errors.append(
                f"ATLAS_DIFF_FIELD_AFTER_VALUE_MISMATCH:{kind.value}:"
                f"{entity_id}:{field_change.field_name}"
            )
    expected_significance = (
        ExplorerDiffSignificance.LINEAGE_ONLY
        if kind is ExplorerDiffEntityKind.SOURCE
        and set(expected_fields) <= _LINEAGE_ONLY_SOURCE_FIELDS
        else ExplorerDiffSignificance.SEMANTIC
    )
    if change.significance is not expected_significance:
        errors.append(f"ATLAS_DIFF_SIGNIFICANCE_MISMATCH:{kind.value}:{entity_id}")


def _validate_diff(
    *,
    before: StrategyResearchExplorerSnapshot,
    after: StrategyResearchExplorerSnapshot,
    diff: StrategyResearchExplorerDiff,
    errors: list[str],
) -> None:
    if diff.before_snapshot_id != before.snapshot_id:
        errors.append("ATLAS_DIFF_BEFORE_SNAPSHOT_ID_MISMATCH")
    if diff.after_snapshot_id != after.snapshot_id:
        errors.append("ATLAS_DIFF_AFTER_SNAPSHOT_ID_MISMATCH")
    if diff.before_generated_at != before.generated_at:
        errors.append("ATLAS_DIFF_BEFORE_GENERATED_AT_MISMATCH")
    if diff.after_generated_at != after.generated_at:
        errors.append("ATLAS_DIFF_AFTER_GENERATED_AT_MISMATCH")
    before_maps = _maps(before)
    after_maps = _maps(after)
    observed = {(item.entity_kind, item.entity_id): item for item in diff.changes}
    expected_keys: set[tuple[ExplorerDiffEntityKind, str]] = set()
    for kind in ExplorerDiffEntityKind:
        before_map = before_maps[kind]
        after_map = after_maps[kind]
        for entity_id in set(before_map) | set(after_map):
            if (
                _expected_transition(
                    entity_id=entity_id,
                    before_map=before_map,
                    after_map=after_map,
                )
                is not None
            ):
                expected_keys.add((kind, entity_id))
    if set(observed) != expected_keys:
        errors.append("ATLAS_DIFF_CHANGE_SET_MISMATCH")
    for kind, entity_id in sorted(
        set(observed) & expected_keys,
        key=lambda item: (item[0].value, item[1].casefold()),
    ):
        _validate_change(
            kind=kind,
            entity_id=entity_id,
            change=observed[(kind, entity_id)],
            before_map=before_maps[kind],
            after_map=after_maps[kind],
            errors=errors,
        )
    summary_map = {item.entity_kind: item for item in diff.entity_summaries}
    for kind in ExplorerDiffEntityKind:
        before_map = before_maps[kind]
        after_map = after_maps[kind]
        common = set(before_map) & set(after_map)
        changed = {
            entity_id
            for entity_id in common
            if _entity_hash(before_map[entity_id]) != _entity_hash(after_map[entity_id])
        }
        expected_summary = (
            len(before_map),
            len(after_map),
            len(common - changed),
            len(set(after_map) - set(before_map)),
            len(set(before_map) - set(after_map)),
            len(changed),
        )
        summary = summary_map.get(kind)
        observed_summary = (
            None
            if summary is None
            else (
                summary.before_count,
                summary.after_count,
                summary.unchanged_count,
                summary.added_count,
                summary.removed_count,
                summary.changed_count,
            )
        )
        if observed_summary != expected_summary:
            errors.append(f"ATLAS_DIFF_SUMMARY_MISMATCH:{kind.value}")
    if (
        not diff.manual_review_only
        or diff.commands_executed
        or diff.source_state_mutated
        or diff.production_effect.value != "none"
        or diff.broker_action != "none"
    ):
        errors.append("ATLAS_DIFF_READ_ONLY_BOUNDARY_INVALID")


def validate_serialized_snapshot_diff(
    *,
    before_payload: Mapping[str, object],
    after_payload: Mapping[str, object],
    diff_payload: Mapping[str, object],
) -> AtlasDiffValidationResult:
    errors: list[str] = []
    before_snapshot_id = str(before_payload.get("snapshot_id", ""))
    after_snapshot_id = str(after_payload.get("snapshot_id", ""))
    diff_id = str(diff_payload.get("diff_id", ""))
    change_count = 0
    try:
        before = StrategyResearchExplorerSnapshot.from_dict(before_payload)
        after = StrategyResearchExplorerSnapshot.from_dict(after_payload)
        diff = StrategyResearchExplorerDiff.from_dict(diff_payload)
        change_count = len(diff.changes)
        if diff.canonical_json_bytes() != _canonical_json_bytes(diff_payload):
            errors.append("ATLAS_DIFF_CANONICAL_ROUND_TRIP_MISMATCH")
        _validate_diff(before=before, after=after, diff=diff, errors=errors)
    except (KeyError, TypeError, ValueError) as exc:
        errors.append(f"ATLAS_DIFF_CONTRACT_INVALID:{type(exc).__name__}")
    errors_tuple = tuple(sorted(set(errors)))
    return AtlasDiffValidationResult(
        schema_version="atlas_explorer_diff_validation.v1",
        status="PASS" if not errors_tuple else "FAIL",
        before_snapshot_id=before_snapshot_id,
        after_snapshot_id=after_snapshot_id,
        diff_id=diff_id,
        change_count=change_count,
        error_count=len(errors_tuple),
        errors=errors_tuple,
    )


def validate_snapshot_diff_bundle(
    bundle: AtlasSnapshotDiffBundle,
) -> AtlasDiffValidationResult:
    result = validate_serialized_snapshot_diff(
        before_payload=bundle.before.to_dict(),
        after_payload=bundle.after.to_dict(),
        diff_payload=bundle.diff.to_dict(),
    )
    receipt_errors: list[str] = []
    before_input, after_input = bundle.input_receipt.inputs
    if before_input.snapshot_id != bundle.before.snapshot_id:
        receipt_errors.append("ATLAS_DIFF_RECEIPT_BEFORE_ID_MISMATCH")
    if after_input.snapshot_id != bundle.after.snapshot_id:
        receipt_errors.append("ATLAS_DIFF_RECEIPT_AFTER_ID_MISMATCH")
    if bundle.input_receipt.receipt_id != bundle.input_receipt.compute_receipt_id():
        receipt_errors.append("ATLAS_DIFF_RECEIPT_ID_MISMATCH")
    if not receipt_errors:
        return result
    errors = tuple(sorted(set((*result.errors, *receipt_errors))))
    return AtlasDiffValidationResult(
        schema_version=result.schema_version,
        status="FAIL",
        before_snapshot_id=result.before_snapshot_id,
        after_snapshot_id=result.after_snapshot_id,
        diff_id=result.diff_id,
        change_count=result.change_count,
        error_count=len(errors),
        errors=errors,
    )


def diff_validation_json_bytes(result: AtlasDiffValidationResult) -> bytes:
    return _canonical_json_bytes(result.to_dict())


__all__ = [
    "AtlasDiffValidationResult",
    "diff_validation_json_bytes",
    "validate_serialized_snapshot_diff",
    "validate_snapshot_diff_bundle",
]
