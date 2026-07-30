from __future__ import annotations

import json
from dataclasses import dataclass

from ai_trading_system.contracts.strategy_research_explorer import (
    ExplorerSourceKind,
    StrategyResearchExplorerSnapshot,
)

from .snapshot_builder import AtlasExplorerBundle


@dataclass(frozen=True)
class AtlasValidationResult:
    schema_version: str
    status: str
    snapshot_id: str
    error_count: int
    errors: tuple[str, ...]
    production_effect: str = "none"
    broker_action: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }


def validate_atlas_bundle(bundle: AtlasExplorerBundle) -> AtlasValidationResult:
    errors: list[str] = []
    snapshot = bundle.snapshot
    try:
        rebuilt = StrategyResearchExplorerSnapshot.from_dict(snapshot.to_dict())
        if rebuilt.canonical_json_bytes() != snapshot.canonical_json_bytes():
            errors.append("ATLAS_SNAPSHOT_ROUND_TRIP_BYTES_MISMATCH")
        if rebuilt.compute_snapshot_id() != snapshot.snapshot_id:
            errors.append("ATLAS_SNAPSHOT_ID_REBUILD_MISMATCH")
    except (TypeError, ValueError) as exc:
        errors.append(f"ATLAS_SNAPSHOT_CONTRACT_INVALID:{type(exc).__name__}")
    if any(item.source_kind is ExplorerSourceKind.UNVERIFIED_CONTEXT for item in snapshot.sources):
        errors.append("ATLAS_MVP_UNVERIFIED_CONTEXT_SOURCE_FORBIDDEN")
    if any(item.investment_facing for item in snapshot.results):
        errors.append("ATLAS_MVP_INVESTMENT_FACING_RESULT_FORBIDDEN")
    if (
        not snapshot.manual_review_only
        or snapshot.commands_executed
        or snapshot.source_state_mutated
    ):
        errors.append("ATLAS_READ_ONLY_BOUNDARY_INVALID")
    if not bundle.reader_notice.strip() or not bundle.glossary:
        errors.append("ATLAS_READER_CONTEXT_INCOMPLETE")
    errors_tuple = tuple(sorted(set(errors)))
    return AtlasValidationResult(
        schema_version="atlas_explorer_validation.v1",
        status="PASS" if not errors_tuple else "FAIL",
        snapshot_id=snapshot.snapshot_id,
        error_count=len(errors_tuple),
        errors=errors_tuple,
    )


def validation_json_bytes(result: AtlasValidationResult) -> bytes:
    return (
        json.dumps(
            result.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


__all__ = [
    "AtlasValidationResult",
    "validate_atlas_bundle",
    "validation_json_bytes",
]
