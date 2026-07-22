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
    LegacyRegisterDocument,
    LegacyTaskRow,
    load_legacy_documents,
)

POLICY_SCHEMA_VERSION = "gov_006_portfolio_normalization_policy.v2"
MANIFEST_SCHEMA_VERSION = "gov_006_portfolio_normalization_decision_manifest.v2"
POLICY_ID = "gov_006_wave1_normalization"
POLICY_STATUS = "GOVERNANCE_COORDINATOR_REVIEWED_WAVE1"
AUTHORIZATION_SCOPE = "GOVERNANCE_TASK_AND_PARALLEL_EXECUTION_ONLY"
DECISION_REVIEW_SCOPE = "EXACT_WAVE1_DECISIONS"
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
) -> dict[str, Any]:
    root = project_root.resolve()
    metadata = _validate_policy(policy)
    policy_binding = _policy_binding(root=root, path=policy_path, policy=policy)
    validated_base_commit = _commit_id(base_commit, "base_commit")

    documents = load_legacy_documents(root)
    active, completed = _partition_documents(documents)
    all_rows = tuple(row for document in documents for row in document.rows)
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
    base_commit = _required_text(source.get("base_commit"), "base_commit")
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


def _policy_binding(*, root: Path, path: Path, policy: Mapping[str, Any]) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        portable = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise TaskPortfolioNormalizationError("POLICY_OUTSIDE_ROOT", str(path)) from exc
    try:
        raw = resolved.read_bytes()
    except OSError as exc:
        raise TaskPortfolioNormalizationError("POLICY_READ", portable) from exc
    loaded = _parse_normalization_policy(raw, path=resolved)
    if _canonical_json_bytes(loaded) != _canonical_json_bytes(policy):
        raise TaskPortfolioNormalizationError("POLICY_ARGUMENT_DRIFT", portable)
    metadata = _validate_policy(loaded)
    return {
        "path": portable,
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
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


def _manifest_sha256(payload: Mapping[str, Any]) -> str:
    material = dict(payload)
    material.pop("manifest_id", None)
    material.pop("manifest_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def _manifest_id(manifest_sha256: str) -> str:
    return f"gov_006_decision_manifest_{manifest_sha256[:20]}"


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
    "AUTHORIZATION_SCOPE",
    "DECISION_REVIEW_SCOPE",
    "MANIFEST_SCHEMA_VERSION",
    "POLICY_SCHEMA_VERSION",
    "TaskPortfolioNormalizationError",
    "build_normalization_decision_manifest",
    "load_normalization_policy",
    "validate_normalization_decision_manifest",
]
