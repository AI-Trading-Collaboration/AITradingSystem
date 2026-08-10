from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_explorer import (
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_status_explanation import (
    ATLAS_STATUS_EXPLANATION_STAGE_IDS,
    StrategyResearchStatusExplanationBundle,
)
from ai_trading_system.contracts.strategy_research_work_progress import (
    ReaderConcept,
    StageWorkProgressRecord,
    StrategyResearchWorkProgressBundle,
)

DEFAULT_WORK_PROGRESS_POLICY_PATH = "config/atlas/work_progress_explanation.yaml"
WORK_PROGRESS_POLICY_SCHEMA_VERSION = "atlas_work_progress_explanation_authority.v1"
PRIMARY_RESEARCH_START = "2021-02-22"
_EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2506:2026-08-10:"
    "approve_work_progress_recursive_explanation_v1"
)
_EXPECTED_SAFETY = {
    "investment_conclusion_generated": False,
    "research_executed": False,
    "backtest_executed": False,
    "data_quality_executed": False,
    "canonical_status_mutated": False,
    "external_platform_action": False,
    "production_effect": "none",
    "broker_action": "none",
}


class WorkProgressProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class WorkProgressAuthorityPolicy:
    policy_id: str
    policy_version: str
    owner_decision: str
    contract_schema_id: str
    contract_schema_version: str
    template_version: str
    primary_research_start: str
    stage_records: tuple[StageWorkProgressRecord, ...]
    concepts: tuple[ReaderConcept, ...]
    safety: Mapping[str, object]
    policy_sha256: str


@dataclass(frozen=True)
class WorkProgressProjectionValidation:
    schema_version: str
    status: str
    snapshot_id: str
    bundle_sha256: str
    policy_sha256: str
    checks: tuple[str, ...]
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "bundle_sha256": self.bundle_sha256,
            "policy_sha256": self.policy_sha256,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        }


def load_work_progress_authority_policy(
    *,
    repository_root: Path,
    policy_path: Path | None = None,
) -> WorkProgressAuthorityPolicy:
    root = repository_root.resolve()
    selected = policy_path or root / DEFAULT_WORK_PROGRESS_POLICY_PATH
    policy_bytes = _read_inside_repository(root, selected)
    payload = _yaml_mapping(policy_bytes, "policy")
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "status",
            "owner_decision",
            "contract_schema_id",
            "contract_schema_version",
            "template_version",
            "primary_research_start",
            "safety",
            "stage_records",
            "concepts",
        },
        "policy",
    )
    if _text(payload, "schema_version") != WORK_PROGRESS_POLICY_SCHEMA_VERSION:
        raise WorkProgressProjectionError("WORK_PROGRESS_POLICY_SCHEMA_INVALID")
    if _text(payload, "status") != "OWNER_REVIEWED_BASELINE":
        raise WorkProgressProjectionError("WORK_PROGRESS_POLICY_STATUS_INVALID")
    owner_decision = _text(payload, "owner_decision")
    if owner_decision != _EXPECTED_OWNER_DECISION:
        raise WorkProgressProjectionError("WORK_PROGRESS_OWNER_DECISION_DRIFT")
    if _text(payload, "contract_schema_id") != StrategyResearchWorkProgressBundle.schema_id:
        raise WorkProgressProjectionError("WORK_PROGRESS_CONTRACT_SCHEMA_ID_DRIFT")
    if (
        _text(payload, "contract_schema_version")
        != StrategyResearchWorkProgressBundle.schema_version
    ):
        raise WorkProgressProjectionError("WORK_PROGRESS_CONTRACT_SCHEMA_VERSION_DRIFT")
    if _text(payload, "primary_research_start") != PRIMARY_RESEARCH_START:
        raise WorkProgressProjectionError("WORK_PROGRESS_PRIMARY_START_DRIFT")
    safety = _mapping(payload.get("safety"), "policy.safety")
    if dict(safety) != dict(_EXPECTED_SAFETY):
        raise WorkProgressProjectionError("WORK_PROGRESS_SAFETY_BOUNDARY_DRIFT")

    template_version = _text(payload, "template_version")
    stage_records = tuple(
        StageWorkProgressRecord.from_dict(
            {
                "schema_version": StageWorkProgressRecord.schema_version,
                **dict(item),
                "template_version": template_version,
            }
        )
        for item in _mapping_tuple(payload, "stage_records")
    )
    if tuple(item.stage_id for item in stage_records) != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise WorkProgressProjectionError("WORK_PROGRESS_POLICY_STAGE_SET_OR_ORDER_INVALID")
    concepts = tuple(
        ReaderConcept.from_dict(item)
        for item in _mapping_tuple(payload, "concepts")
    )
    return WorkProgressAuthorityPolicy(
        policy_id=_text(payload, "policy_id"),
        policy_version=_text(payload, "policy_version"),
        owner_decision=owner_decision,
        contract_schema_id=_text(payload, "contract_schema_id"),
        contract_schema_version=_text(payload, "contract_schema_version"),
        template_version=template_version,
        primary_research_start=PRIMARY_RESEARCH_START,
        stage_records=stage_records,
        concepts=concepts,
        safety=dict(safety),
        policy_sha256=hashlib.sha256(policy_bytes).hexdigest(),
    )


def project_work_progress(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    status_explanations: StrategyResearchStatusExplanationBundle,
    policy: WorkProgressAuthorityPolicy,
) -> StrategyResearchWorkProgressBundle:
    if snapshot.snapshot_id != status_explanations.snapshot_id:
        raise WorkProgressProjectionError("WORK_PROGRESS_STATUS_SNAPSHOT_MISMATCH")
    if status_explanations.primary_research_start != PRIMARY_RESEARCH_START:
        raise WorkProgressProjectionError("WORK_PROGRESS_STATUS_PRIMARY_START_DRIFT")
    if policy.primary_research_start != PRIMARY_RESEARCH_START:
        raise WorkProgressProjectionError("WORK_PROGRESS_POLICY_PRIMARY_START_DRIFT")
    status_by_stage = {
        record.stage_id: record.status_code
        for record in status_explanations.explanation_records
    }
    if tuple(status_by_stage) != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise WorkProgressProjectionError("WORK_PROGRESS_STATUS_STAGE_SET_INVALID")
    for record in policy.stage_records:
        if record.latest_execution_status != status_by_stage[record.stage_id]:
            raise WorkProgressProjectionError(
                f"WORK_PROGRESS_LATEST_STATUS_DRIFT:{record.stage_id}"
            )
    source_ids = {item.source_ref_id for item in snapshot.sources}
    for owner, source_ref_ids in (
        *(
            (f"stage:{record.stage_id}", record.source_ref_ids)
            for record in policy.stage_records
        ),
        *(
            (f"concept:{concept.concept_id}", concept.source_ref_ids)
            for concept in policy.concepts
        ),
    ):
        missing = sorted(set(source_ref_ids) - source_ids)
        if missing:
            raise WorkProgressProjectionError(
                f"WORK_PROGRESS_SOURCE_REF_MISSING:{owner}:" + ",".join(missing)
            )
    bundle = StrategyResearchWorkProgressBundle.seal(
        snapshot_id=snapshot.snapshot_id,
        primary_research_start=PRIMARY_RESEARCH_START,
        stage_records=policy.stage_records,
        concepts=policy.concepts,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy.policy_sha256,
    )
    replayed = StrategyResearchWorkProgressBundle.from_json_bytes(bundle.canonical_bytes)
    if replayed != bundle:
        raise WorkProgressProjectionError("WORK_PROGRESS_CANONICAL_REPLAY_MISMATCH")
    return bundle


def validate_work_progress_bundle(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    status_explanations: StrategyResearchStatusExplanationBundle,
    bundle: StrategyResearchWorkProgressBundle,
    policy: WorkProgressAuthorityPolicy,
) -> WorkProgressProjectionValidation:
    expected = project_work_progress(
        snapshot=snapshot,
        status_explanations=status_explanations,
        policy=policy,
    )
    if bundle.canonical_bytes != expected.canonical_bytes:
        raise WorkProgressProjectionError("WORK_PROGRESS_BUNDLE_POLICY_DRIFT")
    return WorkProgressProjectionValidation(
        schema_version="atlas_work_progress_projection_validation.v1",
        status="PASS",
        snapshot_id=snapshot.snapshot_id,
        bundle_sha256=bundle.content_sha256,
        policy_sha256=policy.policy_sha256,
        checks=(
            "SNAPSHOT_STATUS_BINDING_MATCH",
            "STAGE_SET_AND_ORDER_MATCH",
            "CAPABILITY_EXECUTION_RESEARCH_EFFECT_SEPARATE",
            "READER_FIRST_INTERNAL_TERMS_REJECTED",
            "CONCEPT_REFERENCE_CLOSURE_MATCH",
            "CONCEPT_GRAPH_ACYCLIC",
            "SOURCE_REFS_CLOSED",
            "PRIMARY_RESEARCH_START_MATCH",
            "CANONICAL_REPLAY_MATCH",
            "NO_CANONICAL_STATUS_MUTATION",
        ),
    )


def work_progress_validation_json_bytes(
    validation: WorkProgressProjectionValidation,
) -> bytes:
    return (
        json.dumps(
            validation.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _read_inside_repository(root: Path, path: Path) -> bytes:
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise WorkProgressProjectionError(
            "WORK_PROGRESS_POLICY_PATH_OUTSIDE_REPOSITORY"
        ) from exc
    if not resolved.is_file():
        raise WorkProgressProjectionError("WORK_PROGRESS_POLICY_FILE_MISSING")
    return resolved.read_bytes()


def _yaml_mapping(payload: bytes, field: str) -> Mapping[str, Any]:
    loaded = yaml.safe_load(payload)
    if not isinstance(loaded, Mapping):
        raise WorkProgressProjectionError(
            f"WORK_PROGRESS_POLICY_MAPPING_REQUIRED:{field}"
        )
    return loaded


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkProgressProjectionError(
            f"WORK_PROGRESS_POLICY_MAPPING_REQUIRED:{field}"
        )
    return value


def _mapping_tuple(
    payload: Mapping[str, Any],
    field: str,
) -> tuple[Mapping[str, Any], ...]:
    value = payload.get(field)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, Mapping) for item in value)
    ):
        raise WorkProgressProjectionError(
            f"WORK_PROGRESS_POLICY_MAPPING_LIST_REQUIRED:{field}"
        )
    return tuple(item for item in value if isinstance(item, Mapping))


def _text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise WorkProgressProjectionError(
            f"WORK_PROGRESS_POLICY_TEXT_REQUIRED:{field}"
        )
    return value


def _require_exact_keys(
    payload: Mapping[str, Any],
    expected: set[str],
    field: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        raise WorkProgressProjectionError(
            f"WORK_PROGRESS_POLICY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


__all__ = [
    "DEFAULT_WORK_PROGRESS_POLICY_PATH",
    "WORK_PROGRESS_POLICY_SCHEMA_VERSION",
    "WorkProgressAuthorityPolicy",
    "WorkProgressProjectionError",
    "WorkProgressProjectionValidation",
    "load_work_progress_authority_policy",
    "project_work_progress",
    "validate_work_progress_bundle",
    "work_progress_validation_json_bytes",
]
