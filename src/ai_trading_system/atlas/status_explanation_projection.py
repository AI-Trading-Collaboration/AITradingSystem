from __future__ import annotations

import hashlib
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
    CitedExplanationFact,
    ExplanationAuthorityBinding,
    ExplanationAuthorityKind,
    ExplanationTargetKind,
    ExplanationTransitionCondition,
    StatusExplanationRecord,
    StrategyResearchStatusExplanationBundle,
)

DEFAULT_STATUS_EXPLANATION_POLICY_PATH = "config/atlas/status_explanation_authority.yaml"
STATUS_EXPLANATION_POLICY_SCHEMA_VERSION = "atlas_status_explanation_authority.v1"
PRIMARY_RESEARCH_START = "2021-02-22"
_EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2495:2026-08-03:approve_contract_first_reader_explanation_v1"
)
_EXPECTED_EXCLUDED_TASK_IDS = tuple(f"TRADING-{task_id}" for task_id in range(2481, 2494))
_EXPECTED_SAFETY: Mapping[str, object] = {
    "investment_conclusion_generated": False,
    "research_executed": False,
    "backtest_executed": False,
    "data_quality_executed": False,
    "canonical_status_mutated": False,
    "external_platform_action": False,
    "production_effect": "none",
    "broker_action": "none",
}
_RENDERER_STAGE_STATUS_AUTHORITY = {
    "DATA_INPUTS": ("NOT_EXECUTED_BY_PAGE", "PAGE_EXECUTION_BOUNDARY"),
    "DATA_QUALITY_GATE": ("NOT_EXECUTED_BY_PAGE", "PAGE_EXECUTION_BOUNDARY"),
    "ATLAS_SNAPSHOT_DIFF": ("VALIDATED", "INDEPENDENT_VALIDATION"),
    "CITATION_FIRST_QUERY": ("VALIDATED", "INDEPENDENT_VALIDATION_SET"),
    "OWNER_DECISION_BOUNDARY": ("PENDING_OWNER_REVIEW", "OWNER_REVIEW_POLICY"),
}


class StatusExplanationProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class StatusExplanationAuthorityPolicy:
    policy_id: str
    policy_version: str
    owner_decision: str
    contract_schema_id: str
    contract_schema_version: str
    template_version: str
    primary_research_start: str
    excluded_task_ids: tuple[str, ...]
    stage_records: tuple[StatusExplanationRecord, ...]
    safety: Mapping[str, object]
    policy_sha256: str


@dataclass(frozen=True)
class StatusExplanationProjectionValidation:
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


def load_status_explanation_authority_policy(
    *,
    repository_root: Path,
    policy_path: Path | None = None,
) -> StatusExplanationAuthorityPolicy:
    root = repository_root.resolve()
    selected = policy_path or root / DEFAULT_STATUS_EXPLANATION_POLICY_PATH
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
            "excluded_task_ids",
            "safety",
            "stage_records",
        },
        "policy",
    )
    if _text(payload, "schema_version") != STATUS_EXPLANATION_POLICY_SCHEMA_VERSION:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_POLICY_SCHEMA_INVALID")
    if _text(payload, "status") != "OWNER_REVIEWED_BASELINE":
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_POLICY_STATUS_INVALID")
    owner_decision = _text(payload, "owner_decision")
    if owner_decision != _EXPECTED_OWNER_DECISION:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_OWNER_DECISION_DRIFT")
    if _text(payload, "contract_schema_id") != StrategyResearchStatusExplanationBundle.schema_id:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_CONTRACT_SCHEMA_ID_DRIFT")
    if (
        _text(payload, "contract_schema_version")
        != StrategyResearchStatusExplanationBundle.schema_version
    ):
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_CONTRACT_SCHEMA_VERSION_DRIFT")
    if _text(payload, "primary_research_start") != PRIMARY_RESEARCH_START:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_PRIMARY_START_DRIFT")
    excluded_task_ids = _text_tuple(payload, "excluded_task_ids")
    if excluded_task_ids != _EXPECTED_EXCLUDED_TASK_IDS:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_EXCLUDED_TASK_SET_DRIFT")
    safety = _mapping(payload.get("safety"), "policy.safety")
    if dict(safety) != dict(_EXPECTED_SAFETY):
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_SAFETY_BOUNDARY_DRIFT")

    template_version = _text(payload, "template_version")
    stage_payloads = _mapping_tuple(payload, "stage_records")
    records = tuple(
        _status_explanation_record_from_policy(item, template_version=template_version)
        for item in stage_payloads
    )
    if tuple(item.stage_id for item in records) != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise StatusExplanationProjectionError(
            "STATUS_EXPLANATION_POLICY_STAGE_SET_OR_ORDER_INVALID"
        )
    return StatusExplanationAuthorityPolicy(
        policy_id=_text(payload, "policy_id"),
        policy_version=_text(payload, "policy_version"),
        owner_decision=owner_decision,
        contract_schema_id=_text(payload, "contract_schema_id"),
        contract_schema_version=_text(payload, "contract_schema_version"),
        template_version=template_version,
        primary_research_start=PRIMARY_RESEARCH_START,
        excluded_task_ids=excluded_task_ids,
        stage_records=records,
        safety=dict(safety),
        policy_sha256=hashlib.sha256(policy_bytes).hexdigest(),
    )


def project_status_explanations(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    primary_research_start: str,
    policy: StatusExplanationAuthorityPolicy,
) -> StrategyResearchStatusExplanationBundle:
    if primary_research_start != PRIMARY_RESEARCH_START:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_SNAPSHOT_PRIMARY_START_DRIFT")
    if policy.primary_research_start != primary_research_start:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_POLICY_SNAPSHOT_START_MISMATCH")
    _validate_records_against_snapshot(snapshot=snapshot, records=policy.stage_records)
    bundle = StrategyResearchStatusExplanationBundle.seal(
        snapshot_id=snapshot.snapshot_id,
        primary_research_start=primary_research_start,
        excluded_task_ids=policy.excluded_task_ids,
        explanation_records=policy.stage_records,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy.policy_sha256,
    )
    replay = StrategyResearchStatusExplanationBundle.from_json_bytes(bundle.canonical_bytes)
    if replay != bundle:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_CANONICAL_REPLAY_MISMATCH")
    return bundle


def build_status_explanation_bundle(
    *,
    repository_root: Path,
    exact_commit: str,
    policy_path: Path | None = None,
) -> StrategyResearchStatusExplanationBundle:
    from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle

    atlas_bundle = build_atlas_bundle(
        repository_root=repository_root,
        exact_commit=exact_commit,
    )
    policy = load_status_explanation_authority_policy(
        repository_root=repository_root,
        policy_path=policy_path,
    )
    return project_status_explanations(
        snapshot=atlas_bundle.snapshot,
        primary_research_start=atlas_bundle.primary_research_start,
        policy=policy,
    )


def validate_status_explanation_bundle(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    bundle: StrategyResearchStatusExplanationBundle,
    policy: StatusExplanationAuthorityPolicy,
) -> StatusExplanationProjectionValidation:
    expected = project_status_explanations(
        snapshot=snapshot,
        primary_research_start=bundle.primary_research_start,
        policy=policy,
    )
    if bundle.canonical_bytes != expected.canonical_bytes:
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_BUNDLE_POLICY_DRIFT")
    return StatusExplanationProjectionValidation(
        schema_version="atlas_status_explanation_projection_validation.v1",
        status="PASS",
        snapshot_id=snapshot.snapshot_id,
        bundle_sha256=bundle.content_sha256,
        policy_sha256=policy.policy_sha256,
        checks=(
            "SNAPSHOT_FINGERPRINT_MATCH",
            "TARGET_STATUS_BINDINGS_MATCH",
            "PRESENT_FACT_SOURCE_REFS_CLOSED",
            "NOT_RECORDED_SCOPE_EXPLICIT",
            "EXCLUDED_TASK_SET_MATCH",
            "PRIMARY_RESEARCH_START_MATCH",
            "CANONICAL_REPLAY_MATCH",
            "NO_CANONICAL_STATUS_MUTATION",
        ),
    )


def _status_explanation_record_from_policy(
    payload: Mapping[str, Any],
    *,
    template_version: str,
) -> StatusExplanationRecord:
    _require_exact_keys(
        payload,
        {
            "stage_id",
            "target_kind",
            "target_id",
            "status_code",
            "status_object_scope",
            "summary_fact_ids",
            "facts",
            "transition_conditions",
            "responsible_role",
            "next_reader_action",
            "technical_refs",
            "checked_authority_scope",
            "checked_authority_ids",
            "authority_bindings",
        },
        "stage_record",
    )
    stage_id = _text(payload, "stage_id")
    facts = tuple(CitedExplanationFact.from_dict(item) for item in _mapping_tuple(payload, "facts"))
    role = CitedExplanationFact.from_dict(
        _mapping(payload.get("responsible_role"), "responsible_role")
    )
    summary_fact_ids = _text_tuple(payload, "summary_fact_ids")
    fact_by_id = {item.fact_id: item for item in (*facts, role)}
    if len(fact_by_id) != len(facts) + 1 or not set(summary_fact_ids).issubset(fact_by_id):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_SUMMARY_FACT_INVALID:{stage_id}"
        )
    plain_summary = (
        "；".join(fact_by_id[fact_id].text_zh.rstrip("。") for fact_id in summary_fact_ids) + "。"
    )
    return StatusExplanationRecord(
        explanation_id="status-explanation-" + stage_id.lower().replace("_", "-"),
        stage_id=stage_id,
        target_kind=ExplanationTargetKind(_text(payload, "target_kind")),
        target_id=_text(payload, "target_id"),
        status_code=_text(payload, "status_code"),
        status_object_scope=_text(payload, "status_object_scope"),
        plain_summary=plain_summary,
        derived_from_fact_ids=summary_fact_ids,
        facts=facts,
        transition_conditions=tuple(
            ExplanationTransitionCondition.from_dict(item)
            for item in _mapping_tuple(payload, "transition_conditions")
        ),
        responsible_role=role,
        next_reader_action=_text(payload, "next_reader_action"),
        technical_refs=_text_tuple(payload, "technical_refs"),
        checked_authority_scope=_text_tuple(payload, "checked_authority_scope"),
        checked_authority_ids=_text_tuple(payload, "checked_authority_ids"),
        authority_bindings=tuple(
            ExplanationAuthorityBinding.from_dict(item)
            for item in _mapping_tuple(payload, "authority_bindings")
        ),
        template_version=template_version,
    )


def _validate_records_against_snapshot(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    records: tuple[StatusExplanationRecord, ...],
) -> None:
    sources = {item.source_ref_id for item in snapshot.sources}
    nodes = {item.node_id: item for item in snapshot.nodes}
    results = {item.result_id: item for item in snapshot.results}
    attributions = {item.attribution_id: item for item in snapshot.attributions}
    for record in records:
        if record.target_kind is ExplanationTargetKind.NODE:
            node_target = nodes.get(record.target_id)
            if node_target is None:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_NODE_TARGET_MISSING:{record.target_id}"
                )
            if record.status_code != node_target.raw_status.value:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_NODE_STATUS_DRIFT:{record.target_id}"
                )
        elif record.target_kind is ExplanationTargetKind.RESULT:
            result_target = results.get(record.target_id)
            if result_target is None:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_RESULT_TARGET_MISSING:{record.target_id}"
                )
            if record.status_code != result_target.display_status.value:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_RESULT_STATUS_DRIFT:{record.target_id}"
                )
        elif record.target_kind is ExplanationTargetKind.ATTRIBUTION:
            attribution_target = attributions.get(record.target_id)
            if attribution_target is None:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_ATTRIBUTION_TARGET_MISSING:{record.target_id}"
                )
            result = results.get(attribution_target.result_id)
            if result is None or record.status_code != result.display_status.value:
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_ATTRIBUTED_RESULT_STATUS_DRIFT:{record.target_id}"
                )
        else:
            expected = _RENDERER_STAGE_STATUS_AUTHORITY.get(record.stage_id)
            if expected is None or expected != (
                record.status_code,
                record.status_object_scope,
            ):
                raise StatusExplanationProjectionError(
                    f"STATUS_EXPLANATION_RENDERER_STAGE_STATUS_DRIFT:{record.stage_id}"
                )

        facts = (*record.facts, record.responsible_role)
        for fact in facts:
            _validate_authority_id(
                authority_kind=fact.authority_kind,
                authority_id=fact.authority_id,
                nodes=nodes,
                results=results,
                attributions=attributions,
            )
            _require_source_refs_exist(
                source_ref_ids=fact.source_ref_ids,
                sources=sources,
                owner=f"fact:{fact.fact_id}",
            )
        for transition in record.transition_conditions:
            if transition.deciding_authority_kind is not None:
                _validate_authority_id(
                    authority_kind=transition.deciding_authority_kind,
                    authority_id=transition.deciding_authority_id or "",
                    nodes=nodes,
                    results=results,
                    attributions=attributions,
                )
            _require_source_refs_exist(
                source_ref_ids=transition.source_ref_ids,
                sources=sources,
                owner=f"transition:{transition.condition_id}",
            )
        for binding in record.authority_bindings:
            _validate_authority_id(
                authority_kind=binding.authority_kind,
                authority_id=binding.authority_id,
                nodes=nodes,
                results=results,
                attributions=attributions,
            )
            _require_source_refs_exist(
                source_ref_ids=binding.source_ref_ids,
                sources=sources,
                owner=f"authority:{binding.authority_id}",
            )
        _require_source_refs_exist(
            source_ref_ids=record.technical_refs,
            sources=sources,
            owner=f"record:{record.stage_id}.technical_refs",
        )

    record_text = "\n".join(str(record.to_dict()) for record in records)
    leaked = tuple(task_id for task_id in _EXPECTED_EXCLUDED_TASK_IDS if task_id in record_text)
    if leaked:
        raise StatusExplanationProjectionError(
            "STATUS_EXPLANATION_EXCLUDED_TASK_AUTHORITY_LEAK:" + ",".join(leaked)
        )


def _validate_authority_id(
    *,
    authority_kind: ExplanationAuthorityKind,
    authority_id: str,
    nodes: Mapping[str, object],
    results: Mapping[str, object],
    attributions: Mapping[str, object],
) -> None:
    if authority_kind is ExplanationAuthorityKind.CANONICAL_NODE and authority_id not in nodes:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_NODE_AUTHORITY_MISSING:{authority_id}"
        )
    if authority_kind is ExplanationAuthorityKind.CANONICAL_RESULT and authority_id not in results:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_RESULT_AUTHORITY_MISSING:{authority_id}"
        )
    if authority_kind is ExplanationAuthorityKind.ATTRIBUTION and authority_id not in attributions:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_ATTRIBUTION_AUTHORITY_MISSING:{authority_id}"
        )
    if (
        authority_kind is ExplanationAuthorityKind.CANONICAL_SNAPSHOT
        and authority_id != "CURRENT_SNAPSHOT"
    ):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_SNAPSHOT_AUTHORITY_INVALID:{authority_id}"
        )


def _require_source_refs_exist(
    *,
    source_ref_ids: tuple[str, ...],
    sources: set[str],
    owner: str,
) -> None:
    missing = tuple(item for item in source_ref_ids if item not in sources)
    if missing:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_SOURCE_REF_MISSING:{owner}:" + ",".join(missing)
        )


def _read_inside_repository(root: Path, path: Path) -> bytes:
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise StatusExplanationProjectionError(
            "STATUS_EXPLANATION_POLICY_PATH_OUTSIDE_REPOSITORY"
        ) from exc
    if not resolved.is_file():
        raise StatusExplanationProjectionError("STATUS_EXPLANATION_POLICY_FILE_MISSING")
    return resolved.read_bytes()


def _yaml_mapping(payload: bytes, field: str) -> Mapping[str, Any]:
    loaded = yaml.safe_load(payload)
    if not isinstance(loaded, Mapping):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_MAPPING_REQUIRED:{field}"
        )
    return loaded


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_MAPPING_REQUIRED:{field}"
        )
    return value


def _mapping_tuple(payload: Mapping[str, Any], field: str) -> tuple[Mapping[str, Any], ...]:
    value = payload.get(field)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, Mapping) for item in value)
    ):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_MAPPING_LIST_REQUIRED:{field}"
        )
    return tuple(item for item in value if isinstance(item, Mapping))


def _text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise StatusExplanationProjectionError(f"STATUS_EXPLANATION_POLICY_TEXT_REQUIRED:{field}")
    return value


def _text_tuple(payload: Mapping[str, Any], field: str) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list) or not value:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_TEXT_LIST_REQUIRED:{field}"
        )
    result = tuple(str(item).strip() for item in value)
    if any(not item for item in result) or len(set(result)) != len(result):
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_TEXT_LIST_INVALID:{field}"
        )
    return result


def _require_exact_keys(payload: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise StatusExplanationProjectionError(
            f"STATUS_EXPLANATION_POLICY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


__all__ = [
    "DEFAULT_STATUS_EXPLANATION_POLICY_PATH",
    "STATUS_EXPLANATION_POLICY_SCHEMA_VERSION",
    "StatusExplanationAuthorityPolicy",
    "StatusExplanationProjectionError",
    "StatusExplanationProjectionValidation",
    "build_status_explanation_bundle",
    "load_status_explanation_authority_policy",
    "project_status_explanations",
    "validate_status_explanation_bundle",
]
