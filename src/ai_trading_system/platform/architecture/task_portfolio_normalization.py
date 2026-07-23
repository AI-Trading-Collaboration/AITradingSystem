from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode

from ai_trading_system.platform.architecture.task_registry_shadow import (
    ACTIVE_REGISTER_PATH,
    COMPLETED_REGISTER_PATH,
    LegacyRegisterDocument,
    LegacyTaskRow,
    load_legacy_documents,
)

POLICY_SCHEMA_VERSION = "gov_006_portfolio_normalization_policy.v2"
MANIFEST_SCHEMA_VERSION = "gov_006_portfolio_normalization_decision_manifest.v2"
APPLIED_CLOSEOUT_SCHEMA_VERSION = "gov_006_portfolio_normalization_applied_closeout.v1"
POLICY_ID = "gov_006_wave1_normalization"
POLICY_STATUS = "GOVERNANCE_COORDINATOR_REVIEWED_WAVE1"
AUTHORIZATION_SCOPE = "GOVERNANCE_TASK_AND_PARALLEL_EXECUTION_ONLY"
DECISION_REVIEW_SCOPE = "EXACT_WAVE1_DECISIONS"
APPLIED_CLOSEOUT_STATUS = "APPLIED_CLOSEOUT_READY"
REAL_WAVE_ID = "GOV-006-WAVE1-HIGH-CONFIDENCE"
REAL_WAVE_DECISION_COUNT = 30
TERMINAL_STATUSES = frozenset({"DONE", "DROPPED"})
SUCCESSOR_EVIDENCE_ROLES = frozenset(
    {"terminal_closure", "completed_consumer", "active_continuation"}
)
_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
_VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
_POLICY_KEYS = frozenset(
    {
        "schema_version",
        "policy_id",
        "wave_id",
        "version",
        "status",
        "authorization",
        "decision_review",
        "rationale",
        "required_validation",
        "decisions",
    }
)
_AUTHORIZATION_KEYS = frozenset({"authorized_by", "scope", "ref", "exact_decisions_approved"})
_DECISION_REVIEW_KEYS = frozenset({"reviewer", "scope"})
_DECISION_KEYS = frozenset(
    {
        "task_id",
        "expected_source_status",
        "target_status",
        "reason_code",
        "own_acceptance_claim",
        "successors",
        "remaining_work",
    }
)
_SUCCESSOR_KEYS = frozenset({"task_id", "evidence_role", "expected_source", "expected_status"})
_SECTION_CATEGORIES = {
    "当前任务": "main",
    "当前推进补充": "supplemental",
    "暂缓任务": "deferred",
}


class TaskPortfolioNormalizationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)  # type: ignore[no-untyped-call]
        try:
            duplicate = key in result
        except TypeError as exc:
            raise TaskPortfolioNormalizationError(
                "POLICY_UNHASHABLE_KEY", f"line={key_node.start_mark.line + 1}"
            ) from exc
        if duplicate:
            raise TaskPortfolioNormalizationError(
                "POLICY_DUPLICATE_KEY",
                f"key={key!r} line={key_node.start_mark.line + 1}",
            )
        result[key] = loader.construct_object(  # type: ignore[no-untyped-call]
            value_node, deep=deep
        )
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_normalization_policy(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise TaskPortfolioNormalizationError("POLICY_READ", str(path)) from exc
    return _parse_normalization_policy(raw, path=path)


def build_normalization_decision_manifest(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
) -> dict[str, Any]:
    root = project_root.resolve()
    payload = _build_normalization_decision_manifest(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        base_commit=_git_head(root),
    )
    validate_normalization_decision_manifest(
        payload,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )
    return payload


def _build_normalization_decision_manifest(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
    base_commit: str,
    documents: Sequence[LegacyRegisterDocument] | None = None,
    policy_raw: bytes | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    metadata = _validate_policy(policy)
    policy_binding = _policy_binding(
        root=root,
        path=policy_path,
        policy=policy,
        raw=policy_raw,
    )
    validated_base_commit = _commit_id(base_commit, "base_commit")

    resolved_documents = tuple(documents or load_legacy_documents(root))
    active, completed = _partition_documents(resolved_documents)
    all_rows = tuple(row for document in resolved_documents for row in document.rows)
    by_id = _unique_rows(all_rows)
    completed_ids = {row.task_id for row in completed.rows}
    specs = _sequence(policy.get("decisions"), "decisions")
    decisions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_spec in specs:
        spec = _mapping(raw_spec, "decision")
        _exact_keys(spec, expected=_DECISION_KEYS, field="decision")
        task_id = _required_text(spec.get("task_id"), "task_id")
        if task_id in seen:
            raise TaskPortfolioNormalizationError("DUPLICATE_DECISION", task_id)
        seen.add(task_id)
        row = by_id.get(task_id)
        if row is None:
            raise TaskPortfolioNormalizationError("DECISION_TASK_MISSING", task_id)
        if row.source != "active":
            raise TaskPortfolioNormalizationError("DECISION_TASK_NOT_ACTIVE", task_id)
        if task_id in completed_ids:
            raise TaskPortfolioNormalizationError("TASK_ALREADY_COMPLETED", task_id)
        expected_status = _required_text(
            spec.get("expected_source_status"), "expected_source_status"
        )
        actual_status = row.projected_cells[3]
        if actual_status != expected_status:
            raise TaskPortfolioNormalizationError(
                "SOURCE_STATUS_DRIFT",
                f"{task_id}: expected={expected_status} actual={actual_status}",
            )
        target_status = _required_text(spec.get("target_status"), "target_status")
        if target_status not in TERMINAL_STATUSES:
            raise TaskPortfolioNormalizationError("NON_TERMINAL_TARGET", task_id)
        reason_code = _required_text(spec.get("reason_code"), "reason_code")
        claim = _required_text(spec.get("own_acceptance_claim"), "own_acceptance_claim")
        successor_refs = _successor_refs(
            task_id=task_id,
            raw_successors=spec.get("successors"),
            by_id=by_id,
        )
        remaining_work = _string_sequence(spec.get("remaining_work"), "remaining_work")
        _validate_reason_invariants(
            task_id=task_id,
            target_status=target_status,
            reason_code=reason_code,
            successor_refs=successor_refs,
            remaining_work=remaining_work,
        )
        decisions.append(
            {
                "task_id": task_id,
                "expected_source_status": expected_status,
                "target_status": target_status,
                "reason_code": reason_code,
                "action": "MOVE_ACTIVE_ROW_TO_COMPLETED_REGISTER",
                "own_acceptance_refs": [_row_ref(row, claim=claim)],
                "successor_refs": successor_refs,
                "remaining_work": list(remaining_work),
                "confidence": "HIGH",
                "production_effect": "none",
            }
        )

    status_counts = Counter(row.projected_cells[3] for row in active.rows)
    priority_counts = Counter(row.projected_cells[2] for row in active.rows)
    section_counts = _active_section_counts(active)
    section_total = sum(int(item["task_count"]) for item in section_counts)
    if section_total != len(active.rows):
        raise TaskPortfolioNormalizationError(
            "ACTIVE_SECTION_COUNT_MISMATCH",
            f"sections={section_total} rows={len(active.rows)}",
        )
    count_formula = " + ".join(str(item["task_count"]) for item in section_counts)
    count_formula = f"{count_formula} = {len(active.rows)}"
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "task_id": "GOV-006_ACTIVE_TASK_PORTFOLIO_NORMALIZATION",
        "wave_id": metadata["wave_id"],
        "status": "DRY_RUN_READY",
        "policy": policy_binding,
        "source": {
            "base_commit": validated_base_commit,
            "base_commit_rule": "CURRENT_COMMIT_MUST_EQUAL_OR_DESCEND_FROM_BASE_COMMIT",
            "active_path": active.source_path,
            "active_sha256": active.sha256,
            "completed_path": completed.source_path,
            "completed_sha256": completed.sha256,
        },
        "before_inventory": {
            "inventory_scope": "all_legacy_task_rows",
            "active_task_count": len(active.rows),
            "completed_task_count": len(completed.rows),
            "active_section_counts": section_counts,
            "active_task_count_formula": count_formula,
            "active_section_count_sum": section_total,
            "status_counts": dict(sorted(status_counts.items())),
            "priority_counts": dict(sorted(priority_counts.items())),
        },
        "decision_count": len(decisions),
        "decisions": decisions,
        "apply_boundary": {
            "automatic_apply_allowed": False,
            "coordinator_only": True,
            "source_hash_policy_hash_status_and_ancestry_revalidation_required": True,
            "refresh_arch_005_shadow_views_required": True,
            "required_validation": list(metadata["required_validation"]),
        },
        "safety": {
            "task_source_of_truth_cutover": False,
            "strategy_logic_change": False,
            "data_or_runtime_change": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    manifest_sha256 = _manifest_sha256(payload)
    payload["manifest_sha256"] = manifest_sha256
    payload["manifest_id"] = _manifest_id(manifest_sha256)
    return payload


def validate_normalization_decision_manifest(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
) -> None:
    root = project_root.resolve()
    base_commit = _validate_decision_manifest_integrity(payload)
    _validate_repository_base_commit(root=root, base_commit=base_commit)
    expected = _build_normalization_decision_manifest(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        base_commit=base_commit,
    )
    if dict(payload) != expected:
        raise TaskPortfolioNormalizationError(
            "MANIFEST_SOURCE_DRIFT",
            "policy/register bytes or governed decisions no longer reproduce manifest",
        )


def validate_historical_normalization_decision_manifest(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
) -> None:
    """Replay a dry-run manifest against its exact committed source bytes.

    N1 moves the governed rows out of the live active register.  The historical
    decision evidence therefore cannot remain valid by reading the live files;
    it must be replayed from the source commit recorded by the dry-run.
    """

    root = project_root.resolve()
    base_commit = _validate_decision_manifest_integrity(payload)
    _validate_repository_base_commit(root=root, base_commit=base_commit)
    source = _mapping(payload.get("source"), "source")
    active_path = _required_portable_repository_path(
        source.get("active_path"),
        field="source.active_path",
        expected=ACTIVE_REGISTER_PATH,
    )
    completed_path = _required_portable_repository_path(
        source.get("completed_path"),
        field="source.completed_path",
        expected=COMPLETED_REGISTER_PATH,
    )
    documents = (
        _legacy_document_from_bytes(
            _git_blob(root=root, commit=base_commit, path=active_path),
            source="active",
            source_path=active_path,
        ),
        _legacy_document_from_bytes(
            _git_blob(root=root, commit=base_commit, path=completed_path),
            source="completed",
            source_path=completed_path,
        ),
    )
    active, completed = _partition_documents(documents)
    if active.sha256 != source.get("active_sha256"):
        raise TaskPortfolioNormalizationError("HISTORICAL_ACTIVE_HASH_DRIFT", active_path)
    if completed.sha256 != source.get("completed_sha256"):
        raise TaskPortfolioNormalizationError("HISTORICAL_COMPLETED_HASH_DRIFT", completed_path)
    policy_binding = _mapping(payload.get("policy"), "policy")
    portable_policy_path = _project_path(
        root=root,
        path=policy_path,
        field="policy_path",
    )
    if policy_binding.get("path") != portable_policy_path:
        raise TaskPortfolioNormalizationError("HISTORICAL_POLICY_PATH_DRIFT", portable_policy_path)
    committed_policy_raw = _git_blob(
        root=root,
        commit=base_commit,
        path=portable_policy_path,
    )
    if hashlib.sha256(committed_policy_raw).hexdigest() != policy_binding.get("raw_sha256"):
        raise TaskPortfolioNormalizationError("HISTORICAL_POLICY_HASH_DRIFT", portable_policy_path)
    committed_policy = _parse_normalization_policy(
        committed_policy_raw,
        path=policy_path,
    )
    expected = _build_normalization_decision_manifest(
        project_root=root,
        policy=committed_policy,
        policy_path=policy_path,
        base_commit=base_commit,
        documents=documents,
        policy_raw=committed_policy_raw,
    )
    if dict(payload) != expected:
        raise TaskPortfolioNormalizationError(
            "HISTORICAL_MANIFEST_SOURCE_DRIFT",
            "committed policy/register bytes do not reproduce the dry-run manifest",
        )


def _validate_decision_manifest_integrity(payload: Mapping[str, Any]) -> str:
    if payload.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise TaskPortfolioNormalizationError("MANIFEST_SCHEMA", str(payload.get("schema_version")))
    if payload.get("status") != "DRY_RUN_READY":
        raise TaskPortfolioNormalizationError("MANIFEST_STATUS", str(payload.get("status")))
    actual_sha256 = _required_text(payload.get("manifest_sha256"), "manifest_sha256")
    expected_sha256 = _manifest_sha256(payload)
    if actual_sha256 != expected_sha256:
        raise TaskPortfolioNormalizationError("MANIFEST_SHA256", actual_sha256)
    manifest_id = _required_text(payload.get("manifest_id"), "manifest_id")
    if manifest_id != _manifest_id(expected_sha256):
        raise TaskPortfolioNormalizationError("MANIFEST_ID", manifest_id)
    source = _mapping(payload.get("source"), "source")
    return _required_text(source.get("base_commit"), "base_commit")


def build_normalization_applied_closeout(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
    decision_manifest_path: Path,
    application_commit: str,
) -> dict[str, Any]:
    root = project_root.resolve()
    payload = _build_normalization_applied_closeout(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        decision_manifest_path=decision_manifest_path,
        application_commit=application_commit,
    )
    validate_normalization_applied_closeout(
        payload,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        decision_manifest_path=decision_manifest_path,
    )
    return payload


def validate_normalization_applied_closeout(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
    decision_manifest_path: Path,
) -> None:
    root = project_root.resolve()
    if payload.get("schema_version") != APPLIED_CLOSEOUT_SCHEMA_VERSION:
        raise TaskPortfolioNormalizationError(
            "APPLIED_CLOSEOUT_SCHEMA", str(payload.get("schema_version"))
        )
    if payload.get("status") != APPLIED_CLOSEOUT_STATUS:
        raise TaskPortfolioNormalizationError("APPLIED_CLOSEOUT_STATUS", str(payload.get("status")))
    actual_sha256 = _required_text(payload.get("closeout_sha256"), "closeout_sha256")
    expected_sha256 = _applied_closeout_sha256(payload)
    if actual_sha256 != expected_sha256:
        raise TaskPortfolioNormalizationError("APPLIED_CLOSEOUT_SHA256", actual_sha256)
    closeout_id = _required_text(payload.get("closeout_id"), "closeout_id")
    if closeout_id != _applied_closeout_id(expected_sha256):
        raise TaskPortfolioNormalizationError("APPLIED_CLOSEOUT_ID", closeout_id)
    lineage = _mapping(payload.get("lineage"), "lineage")
    application_commit = _required_text(
        lineage.get("application_commit"), "lineage.application_commit"
    )
    expected = _build_normalization_applied_closeout(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
        decision_manifest_path=decision_manifest_path,
        application_commit=application_commit,
    )
    if dict(payload) != expected:
        raise TaskPortfolioNormalizationError(
            "APPLIED_CLOSEOUT_SOURCE_DRIFT",
            "historical dry-run or committed application state does not reproduce closeout",
        )


def _build_normalization_applied_closeout(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
    decision_manifest_path: Path,
    application_commit: str,
) -> dict[str, Any]:
    root = project_root.resolve()
    validated_application_commit = _commit_id(application_commit, "application_commit")
    _validate_repository_base_commit(
        root=root,
        base_commit=validated_application_commit,
    )
    decision_manifest_portable = _project_path(
        root=root,
        path=decision_manifest_path,
        field="decision_manifest_path",
    )
    dry_run_raw = _git_blob(
        root=root,
        commit=validated_application_commit,
        path=decision_manifest_portable,
    )
    dry_run = _parse_json_mapping_bytes(
        dry_run_raw,
        path=decision_manifest_portable,
        code="DECISION_MANIFEST_READ",
    )
    validate_historical_normalization_decision_manifest(
        dry_run,
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )
    dry_source = _mapping(dry_run.get("source"), "source")
    historical_base_commit = _commit_id(dry_source.get("base_commit"), "historical_base_commit")
    _validate_commit_ancestry(
        root=root,
        ancestor=historical_base_commit,
        descendant=validated_application_commit,
        code="APPLICATION_COMMIT_NOT_DESCENDANT",
    )

    policy_portable = _project_path(
        root=root,
        path=policy_path,
        field="policy_path",
    )
    application_policy_raw = _git_blob(
        root=root,
        commit=validated_application_commit,
        path=policy_portable,
    )
    policy_binding = _mapping(dry_run.get("policy"), "policy")
    if hashlib.sha256(application_policy_raw).hexdigest() != policy_binding.get("raw_sha256"):
        raise TaskPortfolioNormalizationError("APPLICATION_POLICY_DRIFT", policy_portable)

    active_path = _required_portable_repository_path(
        dry_source.get("active_path"),
        field="source.active_path",
        expected=ACTIVE_REGISTER_PATH,
    )
    completed_path = _required_portable_repository_path(
        dry_source.get("completed_path"),
        field="source.completed_path",
        expected=COMPLETED_REGISTER_PATH,
    )
    before_documents = (
        _legacy_document_from_bytes(
            _git_blob(
                root=root,
                commit=historical_base_commit,
                path=active_path,
            ),
            source="active",
            source_path=active_path,
        ),
        _legacy_document_from_bytes(
            _git_blob(
                root=root,
                commit=historical_base_commit,
                path=completed_path,
            ),
            source="completed",
            source_path=completed_path,
        ),
    )
    after_documents = (
        _legacy_document_from_bytes(
            _git_blob(
                root=root,
                commit=validated_application_commit,
                path=active_path,
            ),
            source="active",
            source_path=active_path,
        ),
        _legacy_document_from_bytes(
            _git_blob(
                root=root,
                commit=validated_application_commit,
                path=completed_path,
            ),
            source="completed",
            source_path=completed_path,
        ),
    )
    before_rows = _unique_rows(tuple(row for document in before_documents for row in document.rows))
    after_rows = _unique_rows(tuple(row for document in after_documents for row in document.rows))
    if set(before_rows) != set(after_rows):
        missing = sorted(set(before_rows) - set(after_rows))
        added = sorted(set(after_rows) - set(before_rows))
        raise TaskPortfolioNormalizationError(
            "APPLIED_TASK_SET_DRIFT",
            f"missing={missing} added={added}",
        )

    raw_decisions = _sequence(dry_run.get("decisions"), "decisions")
    decision_count = len(raw_decisions)
    _validate_real_wave_decision_count(
        wave_id=str(dry_run.get("wave_id", "")),
        decision_count=decision_count,
    )
    dry_run_manifest_id = _required_text(dry_run.get("manifest_id"), "manifest_id")
    decision_ids: set[str] = set()
    vacated_source_lines: list[int] = []
    applied_decisions: list[dict[str, Any]] = []
    target_status_counts: Counter[str] = Counter()
    for raw_decision in raw_decisions:
        decision = _mapping(raw_decision, "decision")
        task_id = _required_text(decision.get("task_id"), "decision.task_id")
        if task_id in decision_ids:
            raise TaskPortfolioNormalizationError("DUPLICATE_APPLIED_DECISION", task_id)
        decision_ids.add(task_id)
        before = before_rows[task_id]
        after = after_rows[task_id]
        if before.source != "active":
            raise TaskPortfolioNormalizationError(
                "APPLIED_DECISION_NOT_HISTORICALLY_ACTIVE", task_id
            )
        target_status = _required_text(decision.get("target_status"), "decision.target_status")
        if after.source != "completed":
            raise TaskPortfolioNormalizationError("APPLIED_DECISION_NOT_COMPLETED", task_id)
        if after.projected_cells[3] != target_status:
            raise TaskPortfolioNormalizationError(
                "APPLIED_TARGET_STATUS_DRIFT",
                f"{task_id}: expected={target_status} actual={after.projected_cells[3]}",
            )
        own_acceptance_refs = _sequence(
            decision.get("own_acceptance_refs"), "decision.own_acceptance_refs"
        )
        if len(own_acceptance_refs) != 1:
            raise TaskPortfolioNormalizationError("APPLIED_OWN_ACCEPTANCE_REF_COUNT", task_id)
        own_ref = _mapping(own_acceptance_refs[0], "own_acceptance_ref")
        if (
            own_ref.get("source") != "active"
            or own_ref.get("status") != before.projected_cells[3]
            or own_ref.get("path") != before.source_path
            or own_ref.get("line") != before.line_number
            or own_ref.get("sha256") != before.row_sha256
        ):
            raise TaskPortfolioNormalizationError("APPLIED_OWN_ACCEPTANCE_REF_DRIFT", task_id)
        vacated_source_lines.append(before.line_number)
        reason_code = _required_text(decision.get("reason_code"), "decision.reason_code")
        row_transition = _validate_applied_row_transition(
            task_id=task_id,
            before=before,
            after=after,
            target_status=target_status,
            reason_code=reason_code,
            manifest_id=dry_run_manifest_id,
        )
        target_status_counts[target_status] += 1
        applied_decisions.append(
            {
                "task_id": task_id,
                "target_status": target_status,
                "reason_code": reason_code,
                "before": _application_row_ref(before),
                "after": _application_row_ref(after),
                "row_transition": row_transition,
                "active_absent_after": True,
                "completed_present_after": True,
                "production_effect": "none",
            }
        )

    for task_id in sorted(set(before_rows) - decision_ids):
        before = before_rows[task_id]
        after = after_rows[task_id]
        before_state = (
            before.source,
            before.projected_cells[2],
            before.projected_cells[3],
        )
        after_state = (
            after.source,
            after.projected_cells[2],
            after.projected_cells[3],
        )
        if before_state != after_state:
            raise TaskPortfolioNormalizationError(
                "UNTARGETED_PARTITION_PRIORITY_STATUS_DRIFT",
                f"{task_id}: before={before_state} after={after_state}",
            )

    before_inventory = _normalization_inventory(before_documents)
    after_inventory = _normalization_inventory(after_documents)
    expected_after = _expected_after_inventory(
        before_inventory=before_inventory,
        decisions=applied_decisions,
    )
    if after_inventory != expected_after:
        raise TaskPortfolioNormalizationError(
            "APPLIED_INVENTORY_DRIFT",
            json.dumps(
                {"expected": expected_after, "actual": after_inventory},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )

    before_active, before_completed = _partition_documents(before_documents)
    after_active, after_completed = _partition_documents(after_documents)
    line_churn = _validate_active_line_churn(
        before=before_active,
        after=after_active,
        vacated_source_lines=vacated_source_lines,
        decision_count=decision_count,
    )
    payload: dict[str, Any] = {
        "schema_version": APPLIED_CLOSEOUT_SCHEMA_VERSION,
        "status": APPLIED_CLOSEOUT_STATUS,
        "task_id": "GOV-006_ACTIVE_TASK_PORTFOLIO_NORMALIZATION",
        "wave_id": dry_run.get("wave_id"),
        "historical_dry_run": {
            "path": decision_manifest_portable,
            "file_sha256": hashlib.sha256(dry_run_raw).hexdigest(),
            "manifest_id": dry_run.get("manifest_id"),
            "manifest_sha256": dry_run.get("manifest_sha256"),
            "source_base_commit": historical_base_commit,
            "status": dry_run.get("status"),
            "decision_count": decision_count,
            "commit_bound_replay": "PASS",
        },
        "lineage": {
            "historical_base_commit": historical_base_commit,
            "application_commit": validated_application_commit,
            "historical_base_is_ancestor_of_application": True,
            "application_commit_must_equal_or_be_ancestor_of_validation_head": True,
        },
        "policy": {
            "path": policy_portable,
            "raw_sha256": policy_binding.get("raw_sha256"),
            "canonical_semantic_sha256": policy_binding.get("canonical_semantic_sha256"),
            "decision_reviewer": "governance_coordinator",
            "owner_exact_decisions_approved": False,
        },
        "source_hashes": {
            "before": {
                "active_path": active_path,
                "active_sha256": before_active.sha256,
                "completed_path": completed_path,
                "completed_sha256": before_completed.sha256,
            },
            "after": {
                "active_path": active_path,
                "active_sha256": after_active.sha256,
                "completed_path": completed_path,
                "completed_sha256": after_completed.sha256,
            },
        },
        "before_inventory": before_inventory,
        "after_inventory": after_inventory,
        "application": {
            "action": "MOVE_ACTIVE_ROWS_TO_COMPLETED_REGISTER",
            "decision_count": decision_count,
            "target_status_counts": dict(sorted(target_status_counts.items())),
            "applied_decisions": applied_decisions,
            "line_churn": line_churn,
            "untargeted_partition_priority_status_unchanged": True,
            "task_id_set_conserved": True,
            "total_task_count_conserved": True,
        },
        "apply_boundary": {
            "automatic_apply_allowed": False,
            "coordinator_only": True,
            "task_source_of_truth": "LEGACY_MARKDOWN_ONLY",
            "task_source_of_truth_cutover": False,
            "application_commit_required": True,
        },
        "safety": {
            "strategy_logic_change": False,
            "data_or_runtime_change": False,
            "scheduler_or_periodic_command_executed": False,
            "paper_shadow_or_portfolio_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    closeout_sha256 = _applied_closeout_sha256(payload)
    payload["closeout_sha256"] = closeout_sha256
    payload["closeout_id"] = _applied_closeout_id(closeout_sha256)
    return payload


def _parse_normalization_policy(raw: bytes, *, path: Path) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TaskPortfolioNormalizationError("POLICY_NOT_UTF8", str(path)) from exc
    try:
        payload = yaml.load(text, Loader=_UniqueKeySafeLoader)
    except TaskPortfolioNormalizationError:
        raise
    except yaml.YAMLError as exc:
        raise TaskPortfolioNormalizationError("POLICY_YAML", str(path)) from exc
    if not isinstance(payload, dict):
        raise TaskPortfolioNormalizationError("POLICY_NOT_MAPPING", str(path))
    return payload


def _validate_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    _exact_keys(policy, expected=_POLICY_KEYS, field="policy")
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise TaskPortfolioNormalizationError("POLICY_SCHEMA", str(policy.get("schema_version")))
    policy_id = _required_text(policy.get("policy_id"), "policy_id")
    if policy_id != POLICY_ID:
        raise TaskPortfolioNormalizationError("POLICY_ID", policy_id)
    version = _required_text(policy.get("version"), "version")
    if _VERSION_PATTERN.fullmatch(version) is None:
        raise TaskPortfolioNormalizationError("POLICY_VERSION", version)
    if policy.get("status") != POLICY_STATUS:
        raise TaskPortfolioNormalizationError("POLICY_STATUS", str(policy.get("status")))
    authorization = _mapping(policy.get("authorization"), "authorization")
    _exact_keys(authorization, expected=_AUTHORIZATION_KEYS, field="authorization")
    authorized_by = _required_text(authorization.get("authorized_by"), "authorized_by")
    if authorized_by != "project_owner":
        raise TaskPortfolioNormalizationError("POLICY_AUTHORIZER", authorized_by)
    authorization_scope = _required_text(authorization.get("scope"), "authorization.scope")
    if authorization_scope != AUTHORIZATION_SCOPE:
        raise TaskPortfolioNormalizationError("POLICY_AUTHORIZATION_SCOPE", authorization_scope)
    authorization_ref = _required_text(authorization.get("ref"), "authorization.ref")
    if authorization.get("exact_decisions_approved") is not False:
        raise TaskPortfolioNormalizationError(
            "POLICY_OWNER_EXACT_DECISION_APPROVAL",
            str(authorization.get("exact_decisions_approved")),
        )
    decision_review = _mapping(policy.get("decision_review"), "decision_review")
    _exact_keys(decision_review, expected=_DECISION_REVIEW_KEYS, field="decision_review")
    reviewer = _required_text(decision_review.get("reviewer"), "decision_review.reviewer")
    if reviewer != "governance_coordinator":
        raise TaskPortfolioNormalizationError("POLICY_DECISION_REVIEWER", reviewer)
    review_scope = _required_text(decision_review.get("scope"), "decision_review.scope")
    if review_scope != DECISION_REVIEW_SCOPE:
        raise TaskPortfolioNormalizationError("POLICY_DECISION_REVIEW_SCOPE", review_scope)
    rationale = _required_text(policy.get("rationale"), "rationale")
    required_validation = _string_sequence(policy.get("required_validation"), "required_validation")
    _sequence(policy.get("decisions"), "decisions")
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": policy_id,
        "wave_id": _required_text(policy.get("wave_id"), "wave_id"),
        "version": version,
        "status": POLICY_STATUS,
        "authorization": {
            "authorized_by": authorized_by,
            "scope": authorization_scope,
            "ref": authorization_ref,
            "exact_decisions_approved": False,
        },
        "decision_review": {
            "reviewer": reviewer,
            "scope": review_scope,
        },
        "rationale": rationale,
        "required_validation": required_validation,
    }


def _policy_binding(
    *,
    root: Path,
    path: Path,
    policy: Mapping[str, Any],
    raw: bytes | None = None,
) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        portable = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise TaskPortfolioNormalizationError("POLICY_OUTSIDE_ROOT", str(path)) from exc
    if raw is None:
        try:
            source_raw = resolved.read_bytes()
        except OSError as exc:
            raise TaskPortfolioNormalizationError("POLICY_READ", portable) from exc
    else:
        source_raw = raw
    loaded = _parse_normalization_policy(source_raw, path=resolved)
    if _canonical_json_bytes(loaded) != _canonical_json_bytes(policy):
        raise TaskPortfolioNormalizationError("POLICY_ARGUMENT_DRIFT", portable)
    metadata = _validate_policy(loaded)
    return {
        "path": portable,
        "raw_sha256": hashlib.sha256(source_raw).hexdigest(),
        "canonical_semantic_sha256": hashlib.sha256(_canonical_json_bytes(loaded)).hexdigest(),
        "schema_version": metadata["schema_version"],
        "policy_id": metadata["policy_id"],
        "wave_id": metadata["wave_id"],
        "version": metadata["version"],
        "status": metadata["status"],
        "authorization": metadata["authorization"],
        "decision_review": metadata["decision_review"],
    }


def _successor_refs(
    *,
    task_id: str,
    raw_successors: object,
    by_id: Mapping[str, LegacyTaskRow],
) -> list[dict[str, Any]]:
    successors = _sequence(raw_successors, "successors")
    refs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_successor in successors:
        spec = _mapping(raw_successor, "successor")
        _exact_keys(spec, expected=_SUCCESSOR_KEYS, field="successor")
        successor_id = _required_text(spec.get("task_id"), "successor.task_id")
        if successor_id in seen:
            raise TaskPortfolioNormalizationError(
                "DUPLICATE_SUCCESSOR", f"{task_id}: {successor_id}"
            )
        seen.add(successor_id)
        evidence_role = _required_text(spec.get("evidence_role"), "successor.evidence_role")
        if evidence_role not in SUCCESSOR_EVIDENCE_ROLES:
            raise TaskPortfolioNormalizationError(
                "SUCCESSOR_EVIDENCE_ROLE", f"{task_id}: {evidence_role}"
            )
        expected_source = _required_text(spec.get("expected_source"), "successor.expected_source")
        if expected_source not in {"active", "completed"}:
            raise TaskPortfolioNormalizationError(
                "SUCCESSOR_EXPECTED_SOURCE", f"{task_id}: {expected_source}"
            )
        expected_statuses = _status_sequence(
            spec.get("expected_status"), "successor.expected_status"
        )
        successor = by_id.get(successor_id)
        if successor is None:
            raise TaskPortfolioNormalizationError(
                "SUCCESSOR_TASK_MISSING", f"{task_id}: {successor_id}"
            )
        if successor.source != expected_source:
            raise TaskPortfolioNormalizationError(
                "SUCCESSOR_SOURCE_DRIFT",
                f"{task_id}: {successor_id} expected={expected_source} actual={successor.source}",
            )
        actual_status = successor.projected_cells[3]
        if actual_status not in expected_statuses:
            detail = (
                f"{task_id}: {successor_id} expected={list(expected_statuses)} "
                f"actual={actual_status}"
            )
            raise TaskPortfolioNormalizationError(
                "SUCCESSOR_STATUS_DRIFT",
                detail,
            )
        refs.append(
            _row_ref(
                successor,
                claim=f"typed successor/closure evidence: {successor_id}",
                evidence_role=evidence_role,
                expected_source=expected_source,
                expected_statuses=expected_statuses,
            )
        )
    return refs


def _validate_reason_invariants(
    *,
    task_id: str,
    target_status: str,
    reason_code: str,
    successor_refs: Sequence[Mapping[str, Any]],
    remaining_work: Sequence[str],
) -> None:
    if reason_code.startswith("SUPERSEDED_BY_"):
        if target_status != "DROPPED":
            raise TaskPortfolioNormalizationError("SUPERSEDED_TARGET", task_id)
        if not any(ref.get("evidence_role") == "terminal_closure" for ref in successor_refs):
            raise TaskPortfolioNormalizationError("SUPERSEDED_WITHOUT_TERMINAL_CLOSURE", task_id)
    if target_status == "DONE" and remaining_work:
        raise TaskPortfolioNormalizationError("DONE_REMAINING_WORK", task_id)


def _active_section_counts(document: LegacyRegisterDocument) -> list[dict[str, Any]]:
    row_lines = {row.line_number for row in document.rows}
    counts: Counter[str] = Counter()
    section = "document_preamble"
    text = document.raw_bytes.decode("utf-8")
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line.startswith("## "):
            section = line[3:].strip().rstrip("#").strip() or "unnamed_section"
        if line_number in row_lines:
            counts[section] += 1
    return [
        {
            "section": section_name,
            "category": _SECTION_CATEGORIES.get(section_name, "other"),
            "task_count": task_count,
        }
        for section_name, task_count in counts.items()
    ]


def _partition_documents(
    documents: Sequence[LegacyRegisterDocument],
) -> tuple[LegacyRegisterDocument, LegacyRegisterDocument]:
    by_source = {document.source: document for document in documents}
    if set(by_source) != {"active", "completed"}:
        raise TaskPortfolioNormalizationError("REGISTER_PARTITIONS", str(sorted(by_source)))
    return by_source["active"], by_source["completed"]


def _unique_rows(rows: Sequence[LegacyTaskRow]) -> dict[str, LegacyTaskRow]:
    result: dict[str, LegacyTaskRow] = {}
    for row in rows:
        if row.task_id in result:
            raise TaskPortfolioNormalizationError("DUPLICATE_TASK_ID", row.task_id)
        result[row.task_id] = row
    return result


def _row_ref(
    row: LegacyTaskRow,
    *,
    claim: str,
    evidence_role: str | None = None,
    expected_source: str | None = None,
    expected_statuses: Sequence[str] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "task_id": row.task_id,
        "source": row.source,
        "status": row.projected_cells[3],
        "path": row.source_path,
        "line": row.line_number,
        "sha256": row.row_sha256,
        "claim": claim,
    }
    if evidence_role is not None:
        result.update(
            {
                "evidence_role": evidence_role,
                "expected_source": expected_source,
                "expected_status": list(expected_statuses or ()),
            }
        )
    return result


def _application_row_ref(row: LegacyTaskRow) -> dict[str, Any]:
    return {
        "source": row.source,
        "path": row.source_path,
        "line": row.line_number,
        "status": row.projected_cells[3],
        "priority": row.projected_cells[2],
        "row_sha256": row.row_sha256,
    }


def _validate_applied_row_transition(
    *,
    task_id: str,
    before: LegacyTaskRow,
    after: LegacyTaskRow,
    target_status: str,
    reason_code: str,
    manifest_id: str,
) -> dict[str, Any]:
    if len(after.cells) != len(before.cells):
        raise TaskPortfolioNormalizationError(
            "APPLIED_ROW_CELL_COUNT_DRIFT",
            f"{task_id}: before={len(before.cells)} after={len(after.cells)}",
        )
    if before.cells[:3] != after.cells[:3] or before.cells[4:-1] != after.cells[4:-1]:
        raise TaskPortfolioNormalizationError("APPLIED_NON_STATUS_FIELD_DRIFT", task_id)
    if after.cells[3] != target_status:
        raise TaskPortfolioNormalizationError(
            "APPLIED_TARGET_STATUS_DRIFT",
            f"{task_id}: expected={target_status} actual={after.cells[3]}",
        )
    expected_suffix = _applied_audit_note_suffix(
        reason_code=reason_code,
        target_status=target_status,
        manifest_id=manifest_id,
    )
    actual_suffix = after.cells[-1][len(before.cells[-1]) :]
    if (
        not after.cells[-1].startswith(before.cells[-1])
        or actual_suffix != expected_suffix
        or actual_suffix.count(f"后按 {reason_code} 转 {target_status}") != 1
        or actual_suffix.count(f"manifest={manifest_id}") != 1
        or actual_suffix.count("production_effect=none") != 1
    ):
        raise TaskPortfolioNormalizationError("APPLIED_AUDIT_NOTE_DRIFT", task_id)
    return {
        "cell_count_preserved": True,
        "all_non_status_fields_preserved": True,
        "original_note_prefix_preserved": True,
        "audit_note_suffix": expected_suffix,
        "audit_note_suffix_sha256": hashlib.sha256(expected_suffix.encode("utf-8")).hexdigest(),
        "reason_target_manifest_and_safety_exactly_once": True,
    }


def _applied_audit_note_suffix(
    *,
    reason_code: str,
    target_status: str,
    manifest_id: str,
) -> str:
    return (
        f" 2026-07-23: GOV-006 N1 coordinator review 后按 {reason_code} "
        f"转 {target_status} 并归档；manifest={manifest_id}；"
        "production_effect=none。"
    )


def _validate_active_line_churn(
    *,
    before: LegacyRegisterDocument,
    after: LegacyRegisterDocument,
    vacated_source_lines: Sequence[int],
    decision_count: int,
) -> dict[str, Any]:
    before_lines = before.raw_bytes.decode("utf-8").splitlines()
    after_lines = after.raw_bytes.decode("utf-8").splitlines()
    if len(before_lines) != len(after_lines):
        raise TaskPortfolioNormalizationError(
            "APPLIED_ACTIVE_LINE_COUNT_DRIFT",
            f"before={len(before_lines)} after={len(after_lines)}",
        )
    unique_lines = tuple(sorted(set(vacated_source_lines)))
    if len(unique_lines) != decision_count:
        raise TaskPortfolioNormalizationError(
            "APPLIED_VACATED_LINE_COUNT_DRIFT",
            f"expected={decision_count} actual={len(unique_lines)}",
        )
    for line_number in unique_lines:
        if line_number < 1 or line_number > len(after_lines):
            raise TaskPortfolioNormalizationError(
                "APPLIED_VACATED_LINE_OUT_OF_RANGE", str(line_number)
            )
        if after_lines[line_number - 1].strip():
            raise TaskPortfolioNormalizationError(
                "APPLIED_VACATED_LINE_NOT_BLANK", str(line_number)
            )
    return {
        "before_active_physical_line_count": len(before_lines),
        "after_active_physical_line_count": len(after_lines),
        "physical_line_count_preserved": True,
        "vacated_source_line_count": len(unique_lines),
        "vacated_source_lines": list(unique_lines),
        "vacated_source_lines_preserved_blank": True,
    }


def _normalization_inventory(
    documents: Sequence[LegacyRegisterDocument],
) -> dict[str, Any]:
    active, completed = _partition_documents(documents)
    all_rows = tuple(row for document in documents for row in document.rows)
    _unique_rows(all_rows)
    active_status_counts = Counter(row.projected_cells[3] for row in active.rows)
    active_priority_counts = Counter(row.projected_cells[2] for row in active.rows)
    completed_status_counts = Counter(row.projected_cells[3] for row in completed.rows)
    completed_priority_counts = Counter(row.projected_cells[2] for row in completed.rows)
    return {
        "active_task_count": len(active.rows),
        "completed_task_count": len(completed.rows),
        "total_task_count": len(all_rows),
        "active_status_counts": dict(sorted(active_status_counts.items())),
        "active_priority_counts": dict(sorted(active_priority_counts.items())),
        "completed_status_counts": dict(sorted(completed_status_counts.items())),
        "completed_priority_counts": dict(sorted(completed_priority_counts.items())),
    }


def _expected_after_inventory(
    *,
    before_inventory: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    status_counts = Counter(
        {
            str(key): _strict_integer(value, "before_inventory.active_status_counts")
            for key, value in _mapping(
                before_inventory.get("active_status_counts"),
                "before_inventory.active_status_counts",
            ).items()
        }
    )
    priority_counts = Counter(
        {
            str(key): _strict_integer(value, "before_inventory.active_priority_counts")
            for key, value in _mapping(
                before_inventory.get("active_priority_counts"),
                "before_inventory.active_priority_counts",
            ).items()
        }
    )
    completed_status_counts = Counter(
        {
            str(key): _strict_integer(value, "before_inventory.completed_status_counts")
            for key, value in _mapping(
                before_inventory.get("completed_status_counts"),
                "before_inventory.completed_status_counts",
            ).items()
        }
    )
    completed_priority_counts = Counter(
        {
            str(key): _strict_integer(value, "before_inventory.completed_priority_counts")
            for key, value in _mapping(
                before_inventory.get("completed_priority_counts"),
                "before_inventory.completed_priority_counts",
            ).items()
        }
    )
    for decision in decisions:
        before = _mapping(decision.get("before"), "decision.before")
        source_status = _required_text(before.get("status"), "decision.before.status")
        priority = _required_text(before.get("priority"), "decision.before.priority")
        target_status = _required_text(decision.get("target_status"), "decision.target_status")
        status_counts[source_status] -= 1
        priority_counts[priority] -= 1
        completed_status_counts[target_status] += 1
        completed_priority_counts[priority] += 1
    if any(
        value < 0
        for value in (
            *status_counts.values(),
            *priority_counts.values(),
            *completed_status_counts.values(),
            *completed_priority_counts.values(),
        )
    ):
        raise TaskPortfolioNormalizationError(
            "APPLIED_INVENTORY_UNDERFLOW", "decision counts exceed before inventory"
        )
    active_before = _strict_integer(
        before_inventory.get("active_task_count"),
        "before_inventory.active_task_count",
    )
    completed_before = _strict_integer(
        before_inventory.get("completed_task_count"),
        "before_inventory.completed_task_count",
    )
    total_before = _strict_integer(
        before_inventory.get("total_task_count"),
        "before_inventory.total_task_count",
    )
    return {
        "active_task_count": active_before - len(decisions),
        "completed_task_count": completed_before + len(decisions),
        "total_task_count": total_before,
        "active_status_counts": dict(
            sorted((key, value) for key, value in status_counts.items() if value)
        ),
        "active_priority_counts": dict(
            sorted((key, value) for key, value in priority_counts.items() if value)
        ),
        "completed_status_counts": dict(
            sorted((key, value) for key, value in completed_status_counts.items() if value)
        ),
        "completed_priority_counts": dict(
            sorted((key, value) for key, value in completed_priority_counts.items() if value)
        ),
    }


def _legacy_document_from_bytes(
    raw: bytes,
    *,
    source: str,
    source_path: str,
) -> LegacyRegisterDocument:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TaskPortfolioNormalizationError("COMMITTED_REGISTER_NOT_UTF8", source_path) from exc
    rows: list[LegacyTaskRow] = []
    for line_number, physical_line in enumerate(text.splitlines(keepends=True), start=1):
        raw_line, ending = _split_line_ending(physical_line)
        cells = _legacy_cells(raw_line)
        if cells is None:
            continue
        rows.append(
            LegacyTaskRow(
                source=source,
                source_path=source_path,
                line_number=line_number,
                raw_line=raw_line,
                line_ending=ending,
                cells=cells,
            )
        )
    return LegacyRegisterDocument(
        source=source,
        source_path=source_path,
        raw_bytes=raw,
        rows=tuple(rows),
    )


def _legacy_cells(line: str) -> tuple[str, ...] | None:
    if not line.startswith("|") or line.startswith("|---") or line.startswith("|ID|"):
        return None
    cells = tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
    if len(cells) < 8 or not cells[0] or cells[0] == "---":
        return None
    return cells


def _split_line_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n") or line.endswith("\r"):
        return line[:-1], line[-1]
    return line, ""


def _parse_json_mapping_bytes(
    raw: bytes,
    *,
    path: str,
    code: str,
) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TaskPortfolioNormalizationError(f"{code}_INVALID", str(path)) from exc
    if not isinstance(value, dict):
        raise TaskPortfolioNormalizationError(f"{code}_MAPPING", str(path))
    return value


def _project_path(*, root: Path, path: Path, field: str) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise TaskPortfolioNormalizationError("PATH_OUTSIDE_PROJECT", f"{field}={path}") from exc


def _required_portable_repository_path(
    value: object,
    *,
    field: str,
    expected: str,
) -> str:
    path = _required_text(value, field).replace("\\", "/")
    if path != expected:
        raise TaskPortfolioNormalizationError(
            "REGISTER_PATH_DRIFT", f"{field}: expected={expected} actual={path}"
        )
    return path


def _git_blob(*, root: Path, commit: str, path: str) -> bytes:
    completed = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise TaskPortfolioNormalizationError("COMMIT_PATH_READ", f"{commit}:{path}: {detail}")
    return completed.stdout


def _validate_commit_ancestry(
    *,
    root: Path,
    ancestor: str,
    descendant: str,
    code: str,
) -> None:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return
    if completed.returncode == 1:
        raise TaskPortfolioNormalizationError(code, f"ancestor={ancestor} descendant={descendant}")
    detail = completed.stderr.strip() or "git merge-base failed"
    raise TaskPortfolioNormalizationError("GIT_ANCESTRY_CHECK", detail)


def _validate_real_wave_decision_count(*, wave_id: str, decision_count: int) -> None:
    if wave_id == REAL_WAVE_ID and decision_count != REAL_WAVE_DECISION_COUNT:
        raise TaskPortfolioNormalizationError(
            "REAL_WAVE_DECISION_COUNT",
            f"expected={REAL_WAVE_DECISION_COUNT} actual={decision_count}",
        )


def _manifest_sha256(payload: Mapping[str, Any]) -> str:
    material = dict(payload)
    material.pop("manifest_id", None)
    material.pop("manifest_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def _manifest_id(manifest_sha256: str) -> str:
    return f"gov_006_decision_manifest_{manifest_sha256[:20]}"


def _applied_closeout_sha256(payload: Mapping[str, Any]) -> str:
    material = dict(payload)
    material.pop("closeout_id", None)
    material.pop("closeout_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def _applied_closeout_id(closeout_sha256: str) -> str:
    return f"gov_006_applied_closeout_{closeout_sha256[:20]}"


def _canonical_json_bytes(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise TaskPortfolioNormalizationError("CANONICAL_JSON", type(value).__name__) from exc
    return encoded.encode("utf-8")


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TaskPortfolioNormalizationError("EXPECTED_MAPPING", field)
    return value


def _sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TaskPortfolioNormalizationError("EXPECTED_SEQUENCE", field)
    return value


def _string_sequence(value: object, field: str) -> tuple[str, ...]:
    items = _sequence(value, field)
    result = tuple(_required_text(item, field) for item in items)
    if len(result) != len(set(result)):
        raise TaskPortfolioNormalizationError("DUPLICATE_SEQUENCE_VALUE", field)
    return result


def _status_sequence(value: object, field: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return (_required_text(value, field),)
    return _string_sequence(value, field)


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TaskPortfolioNormalizationError("EXPECTED_TEXT", field)
    text = value.strip()
    if not text:
        raise TaskPortfolioNormalizationError("REQUIRED_TEXT", field)
    return text


def _strict_integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise TaskPortfolioNormalizationError("EXPECTED_INTEGER", field)
    return value


def _exact_keys(value: Mapping[str, Any], *, expected: frozenset[str], field: str) -> None:
    if not all(isinstance(key, str) for key in value):
        raise TaskPortfolioNormalizationError("NON_STRING_KEY", field)
    actual = frozenset(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise TaskPortfolioNormalizationError(
            "SCHEMA_KEYS", f"{field}: missing={missing} extra={extra}"
        )


def _git_head(root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "unable to resolve repository HEAD"
        raise TaskPortfolioNormalizationError("GIT_HEAD", detail)
    return _commit_id(completed.stdout.strip(), "current_head")


def _validate_repository_base_commit(*, root: Path, base_commit: str) -> None:
    validated_base = _commit_id(base_commit, "base_commit")
    current_head = _git_head(root)
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{validated_base}^{{commit}}"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if exists.returncode != 0:
        raise TaskPortfolioNormalizationError("BASE_COMMIT_UNKNOWN", validated_base)
    if validated_base == current_head:
        return
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", validated_base, current_head],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if ancestry.returncode == 0:
        return
    if ancestry.returncode == 1:
        raise TaskPortfolioNormalizationError(
            "BASE_COMMIT_NOT_ANCESTOR",
            f"base={validated_base} current={current_head}",
        )
    detail = ancestry.stderr.strip() or "git merge-base failed"
    raise TaskPortfolioNormalizationError("GIT_ANCESTRY_CHECK", detail)


def _commit_id(value: object, field: str) -> str:
    commit = _required_text(value, field)
    if _COMMIT_PATTERN.fullmatch(commit) is None:
        raise TaskPortfolioNormalizationError("COMMIT_ID", f"{field}={commit}")
    return commit


__all__ = [
    "APPLIED_CLOSEOUT_SCHEMA_VERSION",
    "APPLIED_CLOSEOUT_STATUS",
    "AUTHORIZATION_SCOPE",
    "DECISION_REVIEW_SCOPE",
    "MANIFEST_SCHEMA_VERSION",
    "POLICY_SCHEMA_VERSION",
    "TaskPortfolioNormalizationError",
    "build_normalization_applied_closeout",
    "build_normalization_decision_manifest",
    "load_normalization_policy",
    "validate_historical_normalization_decision_manifest",
    "validate_normalization_applied_closeout",
    "validate_normalization_decision_manifest",
]
