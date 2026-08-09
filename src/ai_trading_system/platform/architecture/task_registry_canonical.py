from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, cast

import yaml

from ai_trading_system.platform.architecture.devex import (
    write_generated_architecture_artifact,
)
from ai_trading_system.platform.architecture.task_registry_shadow import (
    TERMINAL_STATUSES,
    VALID_LEGACY_STATUSES,
    LegacyRegisterDocument,
    load_legacy_documents,
    load_shadow_v2_fragments,
)
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = "config/architecture/arch_005_s5_task_source_cutover.yaml"
CANONICAL_FRAGMENT_SCHEMA = "arch_005_task_registry_fragment.v1"
CANONICAL_INDEX_SCHEMA = "arch_005_task_registry_index.v1"
CUTOVER_MANIFEST_SCHEMA = "arch_005_s5_cutover_manifest.v1"
CONSUMER_INVENTORY_SCHEMA = "arch_005_s5_consumer_inventory.v1"
CANONICAL_SOURCE = "ARCH_005_TASK_REGISTRY"
CANONICAL_FRAGMENT_ROOT = "registry/development_tasks"
CANONICAL_INDEX_PATH = "inputs/architecture/arch_005_task_registry_index.yaml"
ACTIVE_VIEW_PATH = "docs/task_register.md"
COMPLETED_VIEW_PATH = "docs/task_register_completed.md"
ACTIVE_TEMPLATE_PATH = "inputs/architecture/arch_005_s5_active_view_template.md"
COMPLETED_TEMPLATE_PATH = "inputs/architecture/arch_005_s5_completed_view_template.md"
MANIFEST_PATH = "inputs/architecture/arch_005_s5_cutover_manifest.yaml"
CONSUMER_INVENTORY_PATH = "inputs/architecture/arch_005_s5_consumer_inventory.yaml"
GENERATED_BANNER = (
    "<!-- ARCH-005 S5 GENERATED COMPATIBILITY VIEW: DO NOT EDIT; "
    "source=registry/development_tasks + arch_005_task_registry_index.v1 -->\n\n"
)
GENERATED_TABLE_HEADER = (
    "|ID|领域 / 任务|优先级|状态|下一责任方|阻塞项 / 下一步|验收标准|备注|\n"
    "|---|---|---|---|---|---|---|---|\n"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DOC_LINK_RE = re.compile(r"docs/[A-Za-z0-9_./-]+\.md")

# ARCH-005S5 requires explicit invalid-transition rejection. Non-terminal task
# states remain workflow-flexible, while terminal states are immutable lifecycle
# invariants and may only receive an idempotent same-status event.
_ALLOWED_STATUS_TRANSITIONS = {
    status: (frozenset({status}) if status in TERMINAL_STATUSES else VALID_LEGACY_STATUSES)
    for status in VALID_LEGACY_STATUSES
}


class CanonicalTaskRegistryError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class CanonicalTaskRegistry:
    project_root: Path
    index: dict[str, Any]
    fragments: tuple[dict[str, Any], ...]

    def fragment(self, task_id: str) -> dict[str, Any]:
        for fragment in self.fragments:
            if _task_id(fragment) == task_id:
                return fragment
        raise CanonicalTaskRegistryError("TASK_NOT_FOUND", task_id)

    def projected_rows(self, partition: str) -> tuple[tuple[str, ...], ...]:
        by_id = {_task_id(fragment): fragment for fragment in self.fragments}
        records = _list(self.index.get("fragments"), "index.fragments")
        selected = [
            record
            for record in records
            if _mapping(record, "fragment_record").get("partition") == partition
        ]
        selected.sort(
            key=lambda item: _positive_int(_mapping(item, "record").get("order"), "order")
        )
        return tuple(_projection_cells(by_id[str(record["task_id"])]) for record in selected)


def build_cutover_candidate(*, project_root: Path, source_commit: str) -> dict[str, Any]:
    root = project_root.resolve()
    if not _GIT_SHA_RE.fullmatch(source_commit):
        raise CanonicalTaskRegistryError("SOURCE_COMMIT_INVALID", source_commit)
    policy = load_cutover_policy(root)
    documents = load_legacy_documents(root)
    v2_index_path = _policy_path(root, policy, "legacy_import", "v2_index_path")
    v2_index = _load_mapping(v2_index_path)
    v2_records = _list(v2_index.get("fragments"), "v2_index.fragments")
    v2_fragments = load_shadow_v2_fragments(project_root=root, records=v2_records)
    _assert_final_import_parity(documents, v2_index, v2_fragments)

    templates = _write_templates(root, documents)
    canonical_fragments = tuple(
        _canonicalize_legacy_fragment(fragment) for fragment in v2_fragments
    )
    record_order = {
        str(_mapping(record, "v2_record")["task_id"]): position
        for position, record in enumerate(v2_records, start=1)
    }
    fragment_records = _write_canonical_fragments(
        root,
        canonical_fragments,
        order_by_task=record_order,
    )
    projection_digest = _projection_digest(canonical_fragments, fragment_records)
    manifest = _build_cutover_manifest(
        root=root,
        policy=policy,
        source_commit=source_commit,
        documents=documents,
        v2_index_path=v2_index_path,
        v2_index=v2_index,
        templates=templates,
        projection_digest=projection_digest,
        task_count=len(canonical_fragments),
    )
    manifest_path = _policy_path(root, policy, "canonical", "manifest_path")
    write_generated_architecture_artifact(manifest_path, manifest)
    cycle = {
        "cycle_id": "arch-005-s5-cycle-1-final-import-cutover",
        "cycle_type": "FINAL_IMPORT_AND_CANONICAL_CUTOVER",
        "status": "PASS",
        "task_id": str(policy["task_id"]),
        "source_commit": source_commit,
        "event_count": len(canonical_fragments),
        "production_effect": "none",
    }
    inventory = build_consumer_inventory(root)
    write_generated_architecture_artifact(
        _policy_path(root, policy, "canonical", "consumer_inventory_path"),
        inventory,
    )
    index = _build_index(
        root=root,
        policy=policy,
        fragment_records=fragment_records,
        fragments=canonical_fragments,
        templates=templates,
        governance_cycles=[cycle],
        manifest_sha256=_sha256_file(manifest_path),
        consumer_inventory_sha256=_sha256_file(
            _policy_path(root, policy, "canonical", "consumer_inventory_path")
        ),
    )
    _write_index_and_views(root, policy, index, canonical_fragments)
    return _summary(index, inventory)


def validate_canonical_registry(
    *,
    project_root: Path,
    require_consumer_cutover: bool = True,
    require_inventory_freshness: bool = True,
    require_policy_freshness: bool = True,
    require_index_determinism: bool = True,
) -> CanonicalTaskRegistry:
    root = project_root.resolve()
    policy = load_cutover_policy(root)
    index_path = _policy_path(root, policy, "canonical", "index_path")
    index = _load_generated_mapping(index_path)
    if index.get("schema_version") != CANONICAL_INDEX_SCHEMA:
        _fail("INDEX_SCHEMA", str(index.get("schema_version")))
    if index.get("status") != "PASS" or index.get("source_of_truth") != CANONICAL_SOURCE:
        _fail("INDEX_AUTHORITY", str(index.get("source_of_truth")))
    if index.get("cutover_performed") is not True:
        _fail("INDEX_CUTOVER", "cutover_performed must be true")
    policy_sha256 = _sha256_file(_regular_path(root, POLICY_PATH, "policy"))
    if require_policy_freshness and index.get("policy_sha256") != policy_sha256:
        _fail("POLICY_DRIFT", POLICY_PATH)
    _verify_checksum(index, "index_checksum", "INDEX_CHECKSUM")

    records = _list(index.get("fragments"), "index.fragments")
    fragments = _load_canonical_fragments(root, records)
    templates = _load_and_validate_templates(root, index)
    manifest_path = _policy_path(root, policy, "canonical", "manifest_path")
    if _sha256_file(manifest_path) != index.get("cutover_manifest_sha256"):
        _fail("CUTOVER_MANIFEST_DRIFT", str(manifest_path))
    manifest = _load_generated_mapping(manifest_path)
    _validate_cutover_manifest(manifest, policy=policy)

    inventory_path = _policy_path(root, policy, "canonical", "consumer_inventory_path")
    inventory = _load_generated_mapping(inventory_path)
    expected_inventory = build_consumer_inventory(root)
    if require_inventory_freshness and inventory != expected_inventory:
        _fail("CONSUMER_INVENTORY_STALE", str(inventory_path))
    if _sha256_file(inventory_path) != index.get("consumer_inventory_sha256"):
        _fail("CONSUMER_INVENTORY_HASH", str(inventory_path))
    if require_consumer_cutover and (
        inventory.get("manual_semantic_runtime_consumer_count") != 0
        or inventory.get("manual_writer_count") != 0
    ):
        _fail("CONSUMER_CUTOVER_INCOMPLETE", json.dumps(inventory, ensure_ascii=False))

    rebuilt = _build_index(
        root=root,
        policy=policy,
        fragment_records=[
            {
                "task_id": record["task_id"],
                "path": record["path"],
                "file_sha256": record["file_sha256"],
                "fragment_checksum": record["fragment_checksum"],
                "partition": record["partition"],
                "order": record["order"],
            }
            for record in records
        ],
        fragments=fragments,
        templates=templates,
        governance_cycles=_list(index.get("governance_cycles"), "governance_cycles"),
        manifest_sha256=str(index["cutover_manifest_sha256"]),
        consumer_inventory_sha256=str(index["consumer_inventory_sha256"]),
    )
    if require_index_determinism and rebuilt != index:
        _fail("INDEX_NONDETERMINISTIC", str(index_path))
    _validate_generated_views(root, policy, rebuilt, fragments)
    return CanonicalTaskRegistry(root, index, fragments)


def refresh_consumer_inventory(*, project_root: Path) -> dict[str, Any]:
    registry = validate_canonical_registry(
        project_root=project_root,
        require_consumer_cutover=False,
        require_inventory_freshness=False,
        require_policy_freshness=False,
        require_index_determinism=False,
    )
    root = registry.project_root
    policy = load_cutover_policy(root)
    inventory = build_consumer_inventory(root)
    inventory_path = _policy_path(root, policy, "canonical", "consumer_inventory_path")
    write_generated_architecture_artifact(inventory_path, inventory)
    records = [
        _record_core(_mapping(record, "record"))
        for record in _list(registry.index.get("fragments"), "index.fragments")
    ]
    templates = [
        dict(_mapping(item, "template"))
        for item in _list(registry.index.get("templates"), "index.templates")
    ]
    index = _build_index(
        root=root,
        policy=policy,
        fragment_records=records,
        fragments=registry.fragments,
        templates=templates,
        governance_cycles=_list(registry.index.get("governance_cycles"), "governance_cycles"),
        manifest_sha256=str(registry.index["cutover_manifest_sha256"]),
        consumer_inventory_sha256=_sha256_file(inventory_path),
    )
    _write_index_and_views(root, policy, index, registry.fragments)
    validate_canonical_registry(project_root=root)
    return _summary(index, inventory)


def update_task(
    *,
    project_root: Path,
    task_id: str,
    actor: str,
    change_id: str,
    occurred_at: str,
    base_commit: str,
    status: str | None = None,
    next_owner: str | None = None,
    blocker_or_next_step: str | None = None,
    acceptance_criteria: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    registry = validate_canonical_registry(
        project_root=project_root,
        require_consumer_cutover=False,
    )
    fragment = json.loads(json.dumps(registry.fragment(task_id), ensure_ascii=False))
    cells = list(_projection_cells(fragment))
    old_status = cells[3]
    replacements = {
        3: status,
        4: next_owner,
        5: blocker_or_next_step,
        6: acceptance_criteria,
        7: notes,
    }
    for position, value in replacements.items():
        if value is not None:
            cells[position] = _canonical_cell(value, f"cells[{position}]")
    if cells[3] not in VALID_LEGACY_STATUSES:
        _fail("STATUS_INVALID", cells[3])
    if not _GIT_SHA_RE.fullmatch(base_commit):
        _fail("BASE_COMMIT_INVALID", base_commit)
    events = _list(fragment.get("events"), "fragment.events")
    previous_event_id = str(_mapping(events[-1], "last_event")["event_id"])
    event_payload = {
        "schema_version": "task_event.v1",
        "task_id": task_id,
        "event_type": "TASK_UPDATED",
        "occurred_at": occurred_at,
        "actor": _required_text(actor, "actor"),
        "change_id": _required_text(change_id, "change_id"),
        "lane_id": None,
        "base_commit": base_commit,
        "previous_state_event_id": previous_event_id,
        "from_status": old_status,
        "to_status": cells[3],
        "payload": {
            "priority": cells[2],
            "next_owner": cells[4],
            "blocker_or_next_step": cells[5],
            "acceptance_criteria": cells[6],
            "notes": cells[7],
        },
        "rationale": "Governed canonical task-registry update after ARCH-005 S5 cutover.",
        "evidence_refs": [f"change:{change_id}"],
        "history_completeness": "CANONICAL_EVENT_COMPLETE",
    }
    event_payload["event_id"] = _canonical_event_id(event_payload)
    events.append(event_payload)
    fragment["events"] = events
    fragment["task_record"] = _task_record_from_cells(
        cells,
        prior=_mapping(fragment.get("task_record"), "task_record"),
    )
    fragment["projection"] = _projection_from_cells(cells)
    fragment["last_event_id"] = event_payload["event_id"]
    fragment["fragment_checksum"] = _payload_checksum(fragment, "fragment_checksum")
    validate_canonical_fragment(fragment)
    cycle_count = len(_list(registry.index["governance_cycles"], "cycles"))
    return _persist_registry_mutation(
        registry=registry,
        changed_fragment=fragment,
        cycle_type="SELF_HOSTED_TASK_UPDATE",
        cycle_id=f"arch-005-s5-cycle-{cycle_count + 1}",
        change_id=change_id,
    )


def register_task(
    *,
    project_root: Path,
    cells: list[str],
    actor: str,
    change_id: str,
    occurred_at: str,
    base_commit: str,
) -> dict[str, Any]:
    registry = validate_canonical_registry(project_root=project_root)
    normalized = tuple(
        _canonical_cell(value, f"cells[{position}]") for position, value in enumerate(cells)
    )
    if len(normalized) != 8:
        _fail("REGISTER_CELLS", "exactly eight cells required")
    task_id = normalized[0]
    if not task_id:
        _fail("REGISTER_TASK_ID", "task id must not be empty")
    if any(_task_id(fragment) == task_id for fragment in registry.fragments):
        _fail("TASK_ALREADY_EXISTS", task_id)
    if normalized[3] not in VALID_LEGACY_STATUSES or normalized[3] in TERMINAL_STATUSES:
        _fail("REGISTER_STATUS", normalized[3])
    if not _GIT_SHA_RE.fullmatch(base_commit):
        _fail("BASE_COMMIT_INVALID", base_commit)
    event = {
        "schema_version": "task_event.v1",
        "task_id": task_id,
        "event_type": "TASK_REGISTERED",
        "occurred_at": occurred_at,
        "actor": _required_text(actor, "actor"),
        "change_id": _required_text(change_id, "change_id"),
        "lane_id": None,
        "base_commit": base_commit,
        "previous_state_event_id": None,
        "from_status": None,
        "to_status": normalized[3],
        "payload": {"legacy_projection": list(normalized)},
        "rationale": "Task registered through ARCH-005 canonical registry.",
        "evidence_refs": [f"change:{change_id}"],
        "history_completeness": "CANONICAL_EVENT_COMPLETE",
    }
    event["event_id"] = _canonical_event_id(event)
    fragment: dict[str, Any] = {
        "schema_version": CANONICAL_FRAGMENT_SCHEMA,
        "source_of_truth": CANONICAL_SOURCE,
        "stable_task_identity": {
            "task_id": task_id,
            "task_id_sha256": _sha256_bytes(task_id.encode("utf-8")),
        },
        "task_record": _task_record_from_cells(normalized, prior={}),
        "legacy_import_evidence": None,
        "events": [event],
        "projection": _projection_from_cells(normalized),
        "last_event_id": event["event_id"],
        "producer_version": "arch_005_s5_canonical_registry.v1",
    }
    fragment["fragment_checksum"] = _payload_checksum(fragment, "fragment_checksum")
    validate_canonical_fragment(fragment)
    cycle_count = len(_list(registry.index["governance_cycles"], "cycles"))
    return _persist_registry_mutation(
        registry=registry,
        changed_fragment=fragment,
        cycle_type="SELF_HOSTED_TASK_REGISTRATION",
        cycle_id=f"arch-005-s5-cycle-{cycle_count + 1}",
        change_id=change_id,
    )


def run_rollback_rehearsal(*, project_root: Path, output_root: Path) -> dict[str, Any]:
    registry = validate_canonical_registry(project_root=project_root)
    root = registry.project_root
    policy = load_cutover_policy(root)
    target = output_root.resolve()
    target.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for partition, name in (
        ("active", "task_register.md"),
        ("completed", "task_register_completed.md"),
    ):
        rendered = _render_view(root, policy, registry.index, registry.fragments, partition)
        path = target / name
        write_bytes_atomic(path, rendered)
        results.append(
            {
                "partition": partition,
                "path": path.name,
                "sha256": _sha256_bytes(rendered),
                "row_count": len(registry.projected_rows(partition)),
                "canonical_event_loss_count": 0,
            }
        )
    payload = {
        "schema_version": "arch_005_s5_rollback_rehearsal.v1",
        "status": "PASS",
        "mode": "OWNER_REVIEWED_LEGACY_COMPATIBLE_SNAPSHOT_ONLY",
        "source_of_truth_reverted": False,
        "task_count": len(registry.fragments),
        "views": results,
        "canonical_event_loss_count": 0,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["rehearsal_checksum"] = _payload_checksum(payload, "rehearsal_checksum")
    write_generated_architecture_artifact(target / "rollback_rehearsal.yaml", payload)
    return payload


def canonical_task_register_view_path(
    project_root: Path,
    partition: str,
    *,
    allow_unmanaged_fixture: bool = False,
) -> Path:
    if partition not in {"active", "completed"}:
        _fail("PARTITION_INVALID", partition)
    root = project_root.resolve()
    if not (root / POLICY_PATH).is_file():
        if not allow_unmanaged_fixture:
            _fail("CANONICAL_POLICY_REQUIRED", str(root / POLICY_PATH))
        name = "task_register.md" if partition == "active" else "task_register_completed.md"
        return root / "docs" / name
    registry = validate_canonical_registry(project_root=project_root)
    policy = load_cutover_policy(registry.project_root)
    if partition == "active":
        return _policy_path(registry.project_root, policy, "generated_views", "active_path")
    if partition == "completed":
        return _policy_path(registry.project_root, policy, "generated_views", "completed_path")
    _fail("PARTITION_INVALID", partition)


def load_cutover_policy(project_root: Path) -> dict[str, Any]:
    policy = _load_mapping(_regular_path(project_root, POLICY_PATH, "policy"))
    if policy.get("schema_version") != "arch_005_s5_task_source_cutover_policy.v1":
        _fail("POLICY_SCHEMA", str(policy.get("schema_version")))
    if policy.get("source_of_truth") != CANONICAL_SOURCE:
        _fail("POLICY_SOURCE", str(policy.get("source_of_truth")))
    safety = _mapping(policy.get("safety"), "policy.safety")
    required_false = (
        "yaml_markdown_dual_write_allowed",
        "worker_generated_view_write_allowed",
        "automatic_dispatch_allowed",
        "automatic_merge_allowed",
        "automatic_push_allowed",
    )
    if any(safety.get(field) is not False for field in required_false):
        _fail("POLICY_SAFETY", str(required_false))
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        _fail("POLICY_PRODUCTION", json.dumps(safety))
    return policy


def validate_canonical_fragment(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != CANONICAL_FRAGMENT_SCHEMA:
        _fail("FRAGMENT_SCHEMA", str(payload.get("schema_version")))
    if payload.get("source_of_truth") != CANONICAL_SOURCE:
        _fail("FRAGMENT_AUTHORITY", str(payload.get("source_of_truth")))
    identity = _mapping(payload.get("stable_task_identity"), "stable_task_identity")
    task_id = _required_text(identity.get("task_id"), "task_id")
    if identity.get("task_id_sha256") != _sha256_bytes(task_id.encode("utf-8")):
        _fail("TASK_ID_HASH", task_id)
    task = _mapping(payload.get("task_record"), "task_record")
    if task.get("task_id") != task_id or task.get("schema_version") != "task_record.v1":
        _fail("TASK_RECORD_ID", task_id)
    projection = _mapping(payload.get("projection"), "projection")
    cells = _projection_cells(payload)
    if cells[0] != task_id or cells[3] not in VALID_LEGACY_STATUSES:
        _fail("PROJECTION_ID_OR_STATUS", task_id)
    raw_line = _row_from_cells(cells)
    if projection.get("canonical_row_sha256") != _sha256_bytes(raw_line.encode("utf-8")):
        _fail("PROJECTION_ROW_HASH", task_id)
    if projection.get("terminal") != (cells[3] in TERMINAL_STATUSES):
        _fail("PROJECTION_TERMINAL", task_id)
    events = _list(payload.get("events"), "events")
    if not events:
        _fail("EVENTS_EMPTY", task_id)
    previous: str | None = None
    projected_status: str | None = None
    for position, raw_event in enumerate(events):
        event = _mapping(raw_event, f"events[{position}]")
        event_id = _required_text(event.get("event_id"), "event_id")
        event_type = _required_text(event.get("event_type"), "event_type")
        if event.get("schema_version") != "task_event.v1":
            _fail("EVENT_SCHEMA", event_id)
        if event.get("task_id") != task_id:
            _fail("EVENT_TASK_ID", event_id)
        if position > 0 and event.get("previous_state_event_id") != previous:
            _fail("EVENT_CHAIN", event_id)
        if position == 0 and event.get("previous_state_event_id") is not None:
            _fail("EVENT_GENESIS", event_id)
        if position == 0:
            if event_type not in {"LEGACY_IMPORT", "TASK_REGISTERED"}:
                _fail("EVENT_GENESIS_TYPE", event_type)
        elif event_type != "TASK_UPDATED":
            _fail("EVENT_TYPE", event_type)
        if event_type != "LEGACY_IMPORT" and event_id != _canonical_event_id(event):
            _fail("EVENT_ID_HASH", event_id)
        _required_text(event.get("actor"), "event.actor")
        _required_text(event.get("change_id"), "event.change_id")
        base_commit = _required_text(event.get("base_commit"), "event.base_commit")
        if not _GIT_SHA_RE.fullmatch(base_commit):
            _fail("EVENT_BASE_COMMIT", event_id)
        _mapping(event.get("payload"), "event.payload")
        _list(event.get("evidence_refs"), "event.evidence_refs")
        if event_type == "LEGACY_IMPORT":
            if position != 0 or event.get("occurred_at") is not None:
                _fail("LEGACY_EVENT_POSITION_OR_TIME", event_id)
        else:
            _validate_event_timestamp(event.get("occurred_at"), event_id)
        from_status = event.get("from_status")
        to_status = event.get("to_status")
        if not isinstance(to_status, str) or to_status not in VALID_LEGACY_STATUSES:
            _fail("EVENT_TO_STATUS", event_id)
        if position == 0:
            if from_status is not None:
                _fail("EVENT_GENESIS_STATUS", event_id)
        else:
            if from_status != projected_status:
                _fail("EVENT_FROM_STATUS", event_id)
            if to_status not in _ALLOWED_STATUS_TRANSITIONS[cast(str, projected_status)]:
                _fail("EVENT_STATUS_TRANSITION", f"{from_status}->{to_status}")
        projected_status = to_status
        previous = event_id
    if payload.get("last_event_id") != previous:
        _fail("LAST_EVENT_ID", task_id)
    if projected_status != cells[3]:
        _fail("EVENT_PROJECTION_STATUS", task_id)
    _verify_checksum(payload, "fragment_checksum", "FRAGMENT_CHECKSUM")


def build_consumer_inventory(project_root: Path) -> dict[str, Any]:
    root = project_root.resolve()
    records: list[dict[str, Any]] = []
    manual_semantic_runtime = 0
    manual_writers = 0
    for top in ("src", "scripts", "tests"):
        base = root / top
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            names = [
                name for name in ("task_register.md", "task_register_completed.md") if name in text
            ]
            if not names:
                continue
            portable = path.relative_to(root).as_posix()
            role = "runtime" if top == "src" else "script" if top == "scripts" else "test"
            category = _consumer_category(portable, text, role)
            semantic_manual = role == "runtime" and category == "MANUAL_MARKDOWN_SEMANTIC_DIRECT"
            writer = (
                role == "runtime"
                and _looks_like_manual_writer(text)
                and category != "CANONICAL_AUTHORITY"
            )
            manual_semantic_runtime += int(semantic_manual)
            manual_writers += int(writer)
            records.append(
                {
                    "path": portable,
                    "role": role,
                    "targets": names,
                    "category": category,
                    "manual_semantic_runtime_consumer": semantic_manual,
                    "manual_writer": writer,
                    "rollback": (
                        "VALIDATE_CANONICAL_REGISTRY_THEN_READ_GENERATED_VIEW"
                        if category in {"CANONICAL_LOADER", "GENERATED_VIEW_METADATA_OR_HASH"}
                        else "IMMUTABLE_MIGRATION_OR_TEST_EVIDENCE"
                    ),
                }
            )
    payload: dict[str, Any] = {
        "schema_version": CONSUMER_INVENTORY_SCHEMA,
        "status": "PASS"
        if manual_semantic_runtime == 0 and manual_writers == 0
        else "MIGRATION_REQUIRED",
        "source_of_truth": CANONICAL_SOURCE,
        "consumer_count": len(records),
        "runtime_consumer_count": sum(record["role"] == "runtime" for record in records),
        "script_consumer_count": sum(record["role"] == "script" for record in records),
        "test_consumer_count": sum(record["role"] == "test" for record in records),
        "manual_semantic_runtime_consumer_count": manual_semantic_runtime,
        "manual_writer_count": manual_writers,
        "consumers": records,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["inventory_checksum"] = _payload_checksum(payload, "inventory_checksum")
    return payload


def _consumer_category(portable: str, text: str, role: str) -> str:
    if portable.endswith("task_registry_canonical.py"):
        return "CANONICAL_AUTHORITY"
    if portable.endswith("task_registry_shadow.py") or portable.endswith(
        "architecture_arch005_registry.py"
    ):
        return "IMMUTABLE_FINAL_IMPORT_ONLY"
    if "canonical_task_register_view_path" in text:
        return "CANONICAL_LOADER"
    metadata_only = {
        "src/ai_trading_system/docs_freshness.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "src/ai_trading_system/reports/canonical_system_status.py",
        "src/ai_trading_system/reports/engineering_closeout.py",
    }
    if portable in metadata_only:
        return "GENERATED_VIEW_METADATA_OR_HASH"
    if role != "runtime":
        return "IMMUTABLE_MIGRATION_OR_TEST_EVIDENCE"
    return "MANUAL_MARKDOWN_SEMANTIC_DIRECT"


def _looks_like_manual_writer(text: str) -> bool:
    if "task_register" not in text:
        return False
    direct_targets = (
        r"(?:task_register_path|completed_register_path|completed_task_register_path)"
        r"\s*\.\s*write_(?:text|bytes)\s*\(",
        r"write_bytes_atomic\s*\(\s*"
        r"(?:task_register_path|completed_register_path|completed_task_register_path)",
        r"open\s*\(\s*(?:task_register_path|completed_register_path|"
        r"completed_task_register_path)\s*,\s*['\"]w",
        r"task_register(?:_completed)?\.md['\"]\s*\)\s*\.\s*write_(?:text|bytes)\s*\(",
    )
    return any(re.search(pattern, text) for pattern in direct_targets)


def _canonicalize_legacy_fragment(fragment: dict[str, Any]) -> dict[str, Any]:
    identity = dict(_mapping(fragment.get("stable_task_identity"), "stable_task_identity"))
    task = dict(_mapping(fragment.get("task_record"), "task_record"))
    task["schema_version"] = "task_record.v1"
    event = dict(_mapping(fragment.get("initial_event"), "initial_event"))
    event["event_type"] = "LEGACY_IMPORT"
    projection_cells = list(_projection_cells(fragment))
    payload: dict[str, Any] = {
        "schema_version": CANONICAL_FRAGMENT_SCHEMA,
        "source_of_truth": CANONICAL_SOURCE,
        "stable_task_identity": identity,
        "task_record": task,
        "legacy_import_evidence": fragment.get("legacy_row_evidence"),
        "events": [event],
        "projection": _projection_from_cells(projection_cells),
        "last_event_id": event["event_id"],
        "producer_version": "arch_005_s5_canonical_registry.v1",
    }
    payload["fragment_checksum"] = _payload_checksum(payload, "fragment_checksum")
    validate_canonical_fragment(payload)
    return payload


def _task_record_from_cells(
    cells: tuple[str, ...] | list[str],
    *,
    prior: dict[str, Any] | Any,
) -> dict[str, Any]:
    previous = dict(prior) if isinstance(prior, dict) else dict(prior)
    return {
        "schema_version": "task_record.v1",
        "task_id": cells[0],
        "title": previous.get("title"),
        "domain": cells[1],
        "parent_task_id": previous.get("parent_task_id"),
        "created_at": previous.get("created_at"),
        "created_by": previous.get("created_by"),
        "priority": cells[2],
        "accountable_owner": previous.get("accountable_owner"),
        "next_owner": cells[4],
        "requirement_refs": sorted(set(_DOC_LINK_RE.findall(" ".join(cells)))),
        "module_ids": list(previous.get("module_ids") or []),
        "contract_versions": list(previous.get("contract_versions") or []),
        "dependencies": list(previous.get("dependencies") or []),
        "unstructured_legacy_blocker_or_next_step": cells[5],
        "acceptance_criteria": [{"criterion_id": "legacy_compatibility", "text": cells[6]}],
        "production_effect": previous.get("production_effect", "none"),
        "broker_action": previous.get("broker_action", "none"),
    }


def _projection_from_cells(cells: tuple[str, ...] | list[str]) -> dict[str, Any]:
    normalized = [
        _canonical_cell(str(value), f"projection[{position}]")
        for position, value in enumerate(cells)
    ]
    if len(normalized) != 8:
        _fail("PROJECTION_CELL_COUNT", str(len(normalized)))
    raw_line = _row_from_cells(normalized)
    return {
        "legacy_first_eight_cells": normalized,
        "docs_links": sorted(set(_DOC_LINK_RE.findall(raw_line))),
        "terminal": normalized[3] in TERMINAL_STATUSES,
        "canonical_row_sha256": _sha256_bytes(raw_line.encode("utf-8")),
    }


def _write_templates(
    root: Path,
    documents: tuple[LegacyRegisterDocument, ...],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    paths = {"active": ACTIVE_TEMPLATE_PATH, "completed": COMPLETED_TEMPLATE_PATH}
    for document in documents:
        row_lines = {row.line_number for row in document.rows}
        lines = document.raw_bytes.decode("utf-8").splitlines(keepends=True)
        rendered = "".join(
            physical
            for position, physical in enumerate(lines, start=1)
            if position not in row_lines
        ).encode("utf-8")
        target = _regular_output_path(root, paths[document.source], "template")
        write_bytes_atomic(target, rendered)
        records.append(
            {
                "partition": document.source,
                "path": target.relative_to(root).as_posix(),
                "sha256": _sha256_bytes(rendered),
                "byte_count": len(rendered),
                "removed_task_row_count": len(document.rows),
            }
        )
    return records


def _load_and_validate_templates(root: Path, index: dict[str, Any]) -> list[dict[str, Any]]:
    records = _list(index.get("templates"), "index.templates")
    if {str(record.get("partition")) for record in records} != {"active", "completed"}:
        _fail("TEMPLATE_PARTITIONS", str(records))
    for record in records:
        path = _regular_path(root, str(record["path"]), "template")
        if _sha256_file(path) != record.get("sha256") or path.stat().st_size != record.get(
            "byte_count"
        ):
            _fail("TEMPLATE_DRIFT", str(path))
        text = path.read_text(encoding="utf-8")
        if any(_looks_like_task_row(line) for line in text.splitlines()):
            _fail("TEMPLATE_TASK_ROW", str(path))
    return [dict(record) for record in records]


def _write_canonical_fragments(
    root: Path,
    fragments: tuple[dict[str, Any], ...],
    *,
    order_by_task: dict[str, int],
) -> list[dict[str, Any]]:
    target_root = _regular_output_path(root, CANONICAL_FRAGMENT_ROOT, "fragment_root")
    expected: set[Path] = set()
    records: list[dict[str, Any]] = []
    for fragment in fragments:
        validate_canonical_fragment(fragment)
        task_id = _task_id(fragment)
        relative = _canonical_fragment_path(task_id)
        path = _regular_output_path(root, relative, "fragment")
        path.relative_to(target_root)
        write_generated_architecture_artifact(path, fragment)
        expected.add(path)
        cells = _projection_cells(fragment)
        records.append(
            {
                "task_id": task_id,
                "path": relative,
                "file_sha256": _sha256_file(path),
                "fragment_checksum": fragment["fragment_checksum"],
                "partition": "completed" if cells[3] in TERMINAL_STATUSES else "active",
                "order": order_by_task[task_id],
            }
        )
    if target_root.exists():
        expected_resolved = {path.resolve() for path in expected}
        for stale in sorted(target_root.rglob("*.yaml")):
            if stale.resolve() not in expected_resolved:
                stale.unlink()
    return records


def _load_canonical_fragments(
    root: Path,
    records: list[Any],
) -> tuple[dict[str, Any], ...]:
    seen: set[str] = set()
    fragments: list[dict[str, Any]] = []
    previous_chain = _chain_genesis()
    orders: set[int] = set()
    for position, raw_record in enumerate(records):
        record = _mapping(raw_record, f"record[{position}]")
        task_id = _required_text(record.get("task_id"), "record.task_id")
        if task_id in seen:
            _fail("INDEX_DUPLICATE_TASK", task_id)
        seen.add(task_id)
        order = _positive_int(record.get("order"), "record.order")
        if order in orders:
            _fail("INDEX_DUPLICATE_ORDER", str(order))
        orders.add(order)
        expected_path = _canonical_fragment_path(task_id)
        if record.get("path") != expected_path:
            _fail("INDEX_PATH_BINDING", task_id)
        path = _regular_path(root, expected_path, "canonical_fragment")
        if _sha256_file(path) != record.get("file_sha256"):
            _fail("INDEX_FILE_HASH", task_id)
        fragment = _load_generated_mapping(path)
        validate_canonical_fragment(fragment)
        if _task_id(fragment) != task_id or fragment.get("fragment_checksum") != record.get(
            "fragment_checksum"
        ):
            _fail("INDEX_FRAGMENT_BINDING", task_id)
        partition = "completed" if _projection_cells(fragment)[3] in TERMINAL_STATUSES else "active"
        if record.get("partition") != partition:
            _fail("INDEX_PARTITION", task_id)
        core = _record_core(record)
        expected_chain = _entry_chain(previous_chain, core)
        if (
            record.get("previous_entry_sha256") != previous_chain
            or record.get("entry_sha256") != expected_chain
        ):
            _fail("INDEX_CHAIN", task_id)
        previous_chain = expected_chain
        fragments.append(fragment)
    return tuple(fragments)


def _build_index(
    *,
    root: Path,
    policy: dict[str, Any],
    fragment_records: list[dict[str, Any]],
    fragments: tuple[dict[str, Any], ...],
    templates: list[dict[str, Any]],
    governance_cycles: list[Any],
    manifest_sha256: str,
    consumer_inventory_sha256: str,
) -> dict[str, Any]:
    by_id = {_task_id(fragment): fragment for fragment in fragments}
    if len(by_id) != len(fragments) or set(by_id) != {
        str(record["task_id"]) for record in fragment_records
    }:
        _fail("INDEX_TASK_SET", "fragment record mismatch")
    ordered = sorted(fragment_records, key=lambda record: _positive_int(record["order"], "order"))
    previous = _chain_genesis()
    chained: list[dict[str, Any]] = []
    for record in ordered:
        task_id = str(record["task_id"])
        cells = _projection_cells(by_id[task_id])
        expected_partition = "completed" if cells[3] in TERMINAL_STATUSES else "active"
        core = {
            "task_id": task_id,
            "path": str(record["path"]),
            "file_sha256": str(record["file_sha256"]),
            "fragment_checksum": str(record["fragment_checksum"]),
            "partition": expected_partition,
            "order": _positive_int(record["order"], "order"),
        }
        entry_sha = _entry_chain(previous, core)
        chained.append({**core, "previous_entry_sha256": previous, "entry_sha256": entry_sha})
        previous = entry_sha
    views = [
        _view_record(root, policy, chained, fragments, partition)
        for partition in ("active", "completed")
    ]
    payload: dict[str, Any] = {
        "schema_version": CANONICAL_INDEX_SCHEMA,
        "status": "PASS",
        "stage": "ARCH_005_S5_CANONICAL_SOURCE_CUTOVER",
        "task_id": str(policy["task_id"]),
        "source_of_truth": CANONICAL_SOURCE,
        "cutover_performed": True,
        "legacy_markdown_writable": False,
        "policy_sha256": _sha256_file(_regular_path(root, POLICY_PATH, "policy")),
        "fragment_root": CANONICAL_FRAGMENT_ROOT,
        "task_count": len(chained),
        "active_task_count": sum(record["partition"] == "active" for record in chained),
        "completed_task_count": sum(record["partition"] == "completed" for record in chained),
        "fragment_count": len(chained),
        "missing_task_count": 0,
        "duplicate_task_count": 0,
        "chain_genesis_sha256": _chain_genesis(),
        "final_chain_sha256": previous,
        "cutover_manifest_sha256": manifest_sha256,
        "consumer_inventory_sha256": consumer_inventory_sha256,
        "templates": templates,
        "fragments": chained,
        "generated_views": views,
        "governance_cycles": governance_cycles,
        "governance_cycle_count": len(governance_cycles),
        "manual_row_move_workflow_enabled": len(governance_cycles) < 2,
        "rollback": {
            "mode": "OWNER_REVIEWED_LEGACY_COMPATIBLE_SNAPSHOT_ONLY",
            "automatic_source_reversion_allowed": False,
            "canonical_event_loss_allowed": False,
        },
        "safety": {
            "dual_write_allowed": False,
            "worker_generated_view_write_allowed": False,
            "automatic_dispatch_allowed": False,
            "automatic_merge_allowed": False,
            "automatic_push_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    payload["index_checksum"] = _payload_checksum(payload, "index_checksum")
    return payload


def _view_record(
    root: Path,
    policy: dict[str, Any],
    records: list[dict[str, Any]],
    fragments: tuple[dict[str, Any], ...],
    partition: str,
) -> dict[str, Any]:
    rendered = _render_view_from_records(root, policy, records, fragments, partition)
    target = _view_path(root, policy, partition)
    return {
        "partition": partition,
        "path": target.relative_to(root).as_posix(),
        "sha256": _sha256_bytes(rendered),
        "byte_count": len(rendered),
        "row_count": sum(record["partition"] == partition for record in records),
        "generated_do_not_edit": True,
    }


def _render_view(
    root: Path,
    policy: dict[str, Any],
    index: dict[str, Any],
    fragments: tuple[dict[str, Any], ...],
    partition: str,
) -> bytes:
    return _render_view_from_records(
        root,
        policy,
        [dict(_mapping(item, "record")) for item in _list(index["fragments"], "fragments")],
        fragments,
        partition,
    )


def _render_view_from_records(
    root: Path,
    policy: dict[str, Any],
    records: list[dict[str, Any]],
    fragments: tuple[dict[str, Any], ...],
    partition: str,
) -> bytes:
    by_id = {_task_id(fragment): fragment for fragment in fragments}
    selected = sorted(
        (record for record in records if record["partition"] == partition),
        key=lambda record: _positive_int(record["order"], "order"),
    )
    rows = "".join(
        f"{_render_fragment_row(by_id[str(record['task_id'])])}\n" for record in selected
    )
    template_path = _policy_path(
        root,
        policy,
        "generated_views",
        "active_template_path" if partition == "active" else "completed_template_path",
    )
    template = template_path.read_bytes()
    return (
        GENERATED_BANNER.encode("utf-8")
        + GENERATED_TABLE_HEADER.encode("utf-8")
        + rows.encode("utf-8")
        + b"\n"
        + template
    )


def _write_index_and_views(
    root: Path,
    policy: dict[str, Any],
    index: dict[str, Any],
    fragments: tuple[dict[str, Any], ...],
) -> None:
    write_generated_architecture_artifact(
        _policy_path(root, policy, "canonical", "index_path"),
        index,
    )
    for partition in ("active", "completed"):
        write_bytes_atomic(
            _view_path(root, policy, partition),
            _render_view(root, policy, index, fragments, partition),
        )


def _validate_generated_views(
    root: Path,
    policy: dict[str, Any],
    index: dict[str, Any],
    fragments: tuple[dict[str, Any], ...],
) -> None:
    records = {
        str(record["partition"]): record
        for record in _list(index.get("generated_views"), "generated_views")
    }
    for partition in ("active", "completed"):
        expected = _render_view(root, policy, index, fragments, partition)
        path = _view_path(root, policy, partition)
        actual = path.read_bytes()
        record = _mapping(records.get(partition), f"view.{partition}")
        if actual != expected or record.get("sha256") != _sha256_bytes(expected):
            _fail("GENERATED_VIEW_DRIFT", partition)
        if (
            record.get("byte_count") != len(expected)
            or record.get("generated_do_not_edit") is not True
        ):
            _fail("GENERATED_VIEW_RECORD", partition)


def _persist_registry_mutation(
    *,
    registry: CanonicalTaskRegistry,
    changed_fragment: dict[str, Any],
    cycle_type: str,
    cycle_id: str,
    change_id: str,
) -> dict[str, Any]:
    root = registry.project_root
    policy = load_cutover_policy(root)
    by_id = {_task_id(fragment): fragment for fragment in registry.fragments}
    is_new = _task_id(changed_fragment) not in by_id
    by_id[_task_id(changed_fragment)] = changed_fragment
    old_records = [
        dict(_mapping(record, "record"))
        for record in _list(registry.index["fragments"], "fragments")
    ]
    order_by_task = {str(record["task_id"]): int(record["order"]) for record in old_records}
    if is_new:
        order_by_task[_task_id(changed_fragment)] = max(order_by_task.values(), default=0) + 1
    changed_task_id = _task_id(changed_fragment)
    changed_path = _regular_output_path(
        root,
        _canonical_fragment_path(changed_task_id),
        "changed_fragment",
    )
    validate_canonical_fragment(changed_fragment)
    write_generated_architecture_artifact(changed_path, changed_fragment)
    changed_record = {
        "task_id": changed_task_id,
        "path": _canonical_fragment_path(changed_task_id),
        "file_sha256": _sha256_file(changed_path),
        "fragment_checksum": changed_fragment["fragment_checksum"],
        "partition": (
            "completed" if _projection_cells(changed_fragment)[3] in TERMINAL_STATUSES else "active"
        ),
        "order": order_by_task[changed_task_id],
    }
    fragment_records = [
        changed_record if str(record["task_id"]) == changed_task_id else _record_core(record)
        for record in old_records
    ]
    if is_new:
        fragment_records.append(changed_record)
    inventory = build_consumer_inventory(root)
    inventory_path = _policy_path(root, policy, "canonical", "consumer_inventory_path")
    write_generated_architecture_artifact(inventory_path, inventory)
    cycles = [
        dict(_mapping(item, "cycle"))
        for item in _list(registry.index["governance_cycles"], "cycles")
    ]
    cycles.append(
        {
            "cycle_id": cycle_id,
            "cycle_type": cycle_type,
            "status": "PASS",
            "task_id": _task_id(changed_fragment),
            "change_id": change_id,
            "event_count": len(_list(changed_fragment["events"], "events")),
            "production_effect": "none",
        }
    )
    templates = [
        dict(_mapping(item, "template")) for item in _list(registry.index["templates"], "templates")
    ]
    index = _build_index(
        root=root,
        policy=policy,
        fragment_records=fragment_records,
        fragments=tuple(by_id.values()),
        templates=templates,
        governance_cycles=cycles,
        manifest_sha256=str(registry.index["cutover_manifest_sha256"]),
        consumer_inventory_sha256=_sha256_file(inventory_path),
    )
    _write_index_and_views(root, policy, index, tuple(by_id.values()))
    validate_canonical_registry(project_root=root, require_consumer_cutover=False)
    return _summary(index, inventory)


def _build_cutover_manifest(
    *,
    root: Path,
    policy: dict[str, Any],
    source_commit: str,
    documents: tuple[LegacyRegisterDocument, ...],
    v2_index_path: Path,
    v2_index: dict[str, Any],
    templates: list[dict[str, Any]],
    projection_digest: str,
    task_count: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": CUTOVER_MANIFEST_SCHEMA,
        "status": "PASS",
        "task_id": str(policy["task_id"]),
        "owner_decision": str(policy["owner_decision"]),
        "exact_start_base": str(policy["exact_start_base"]),
        "final_import_source_commit": source_commit,
        "source_of_truth_before": "LEGACY_MARKDOWN_ONLY",
        "source_of_truth_after": CANONICAL_SOURCE,
        "legacy_documents": [
            {
                "partition": document.source,
                "path": document.source_path,
                "sha256": document.sha256,
                "byte_count": len(document.raw_bytes),
                "task_count": len(document.rows),
            }
            for document in documents
        ],
        "shadow_v2": {
            "path": v2_index_path.relative_to(root).as_posix(),
            "sha256": _sha256_file(v2_index_path),
            "index_checksum": v2_index.get("index_checksum"),
            "task_count": v2_index.get("task_count"),
            "cutover_performed": v2_index.get("cutover_performed"),
        },
        "canonical_import": {
            "task_count": task_count,
            "projection_sha256": projection_digest,
            "missing_task_count": 0,
            "duplicate_task_count": 0,
            "semantic_parity": "PASS",
        },
        "templates": templates,
        "rollback": {
            "owner": "architecture_coordinator_and_project_owner",
            "mode": "OWNER_REVIEWED_LEGACY_COMPATIBLE_SNAPSHOT_ONLY",
            "automatic_source_reversion_allowed": False,
            "canonical_event_loss_allowed": False,
        },
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["manifest_checksum"] = _payload_checksum(payload, "manifest_checksum")
    return payload


def _validate_cutover_manifest(manifest: dict[str, Any], *, policy: dict[str, Any]) -> None:
    if (
        manifest.get("schema_version") != CUTOVER_MANIFEST_SCHEMA
        or manifest.get("status") != "PASS"
    ):
        _fail("MANIFEST_SCHEMA_OR_STATUS", str(manifest.get("status")))
    if manifest.get("source_of_truth_after") != CANONICAL_SOURCE:
        _fail("MANIFEST_SOURCE", str(manifest.get("source_of_truth_after")))
    if manifest.get("task_id") != policy.get("task_id"):
        _fail("MANIFEST_TASK", str(manifest.get("task_id")))
    _verify_checksum(manifest, "manifest_checksum", "MANIFEST_CHECKSUM")


def _assert_final_import_parity(
    documents: tuple[LegacyRegisterDocument, ...],
    v2_index: dict[str, Any],
    fragments: tuple[dict[str, Any], ...],
) -> None:
    rows = [row for document in documents for row in document.rows]
    if (
        v2_index.get("source_of_truth") != "LEGACY_MARKDOWN_ONLY"
        or v2_index.get("cutover_performed") is not False
    ):
        _fail("V2_FINAL_IMPORT_AUTHORITY", str(v2_index.get("source_of_truth")))
    if len(rows) != len(fragments) or v2_index.get("task_count") != len(rows):
        _fail("V2_FINAL_IMPORT_COUNT", f"rows={len(rows)} fragments={len(fragments)}")
    by_id = {_task_id(fragment): fragment for fragment in fragments}
    if set(by_id) != {row.task_id for row in rows}:
        _fail("V2_FINAL_IMPORT_TASK_SET", "task set mismatch")
    for row in rows:
        if list(row.cells[:8]) != list(_projection_cells(by_id[row.task_id])):
            _fail("V2_FINAL_IMPORT_PROJECTION", row.task_id)


def _projection_digest(
    fragments: tuple[dict[str, Any], ...],
    records: list[dict[str, Any]],
) -> str:
    by_id = {_task_id(fragment): fragment for fragment in fragments}
    ordered = sorted(records, key=lambda record: int(record["order"]))
    return _canonical_sha256(
        [
            {
                "task_id": record["task_id"],
                "partition": record["partition"],
                "order": record["order"],
                "cells": list(_projection_cells(by_id[str(record["task_id"])])),
            }
            for record in ordered
        ]
    )


def _summary(index: dict[str, Any], inventory: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "PASS",
        "source_of_truth": index["source_of_truth"],
        "task_count": index["task_count"],
        "active_task_count": index["active_task_count"],
        "completed_task_count": index["completed_task_count"],
        "governance_cycle_count": index["governance_cycle_count"],
        "manual_row_move_workflow_enabled": index["manual_row_move_workflow_enabled"],
        "manual_semantic_runtime_consumer_count": inventory[
            "manual_semantic_runtime_consumer_count"
        ],
        "manual_writer_count": inventory["manual_writer_count"],
        "production_effect": "none",
        "broker_action": "none",
    }


def _policy_path(root: Path, policy: dict[str, Any], section: str, field: str) -> Path:
    value = _mapping(policy.get(section), f"policy.{section}").get(field)
    return _regular_output_path(root, _portable_path(value, f"{section}.{field}"), field)


def _view_path(root: Path, policy: dict[str, Any], partition: str) -> Path:
    field = "active_path" if partition == "active" else "completed_path"
    return _policy_path(root, policy, "generated_views", field)


def _canonical_fragment_path(task_id: str) -> str:
    digest = _sha256_bytes(task_id.encode("utf-8"))
    return f"{CANONICAL_FRAGMENT_ROOT}/{digest[:2]}/{digest}.yaml"


def _canonical_event_id(event: dict[str, Any] | Any) -> str:
    mapping = dict(event)
    mapping.pop("event_id", None)
    return f"task-event-{_canonical_sha256(mapping)[:32]}"


def _projection_cells(fragment: dict[str, Any] | Any) -> tuple[str, ...]:
    projection = _mapping(fragment.get("projection"), "projection")
    raw = projection.get("legacy_first_eight_cells")
    if (
        not isinstance(raw, list)
        or len(raw) != 8
        or not all(isinstance(value, str) for value in raw)
    ):
        _fail("PROJECTION_CELLS", str(_task_id(fragment)))
    return tuple(cast(list[str], raw))


def _task_id(fragment: dict[str, Any] | Any) -> str:
    identity = _mapping(fragment.get("stable_task_identity"), "stable_task_identity")
    return _required_text(identity.get("task_id"), "task_id")


def _row_from_cells(cells: tuple[str, ...] | list[str]) -> str:
    if len(cells) != 8:
        _fail("ROW_CELL_COUNT", str(len(cells)))
    return "|" + "|".join(cells) + "|"


def _render_fragment_row(fragment: dict[str, Any]) -> str:
    events = _list(fragment.get("events"), "fragment.events")
    evidence = fragment.get("legacy_import_evidence")
    if (
        len(events) == 1
        and _mapping(events[0], "initial_event").get("event_type") == "LEGACY_IMPORT"
        and isinstance(evidence, dict)
    ):
        raw_line = evidence.get("raw_line")
        if isinstance(raw_line, str) and raw_line.startswith("|") and raw_line.endswith("|"):
            return raw_line
    return _row_from_cells(_projection_cells(fragment))


def _canonical_cell(value: str, field: str) -> str:
    normalized = value.strip()
    if "\n" in normalized or "\r" in normalized or "|" in normalized:
        _fail("CELL_INVALID", field)
    return normalized


def _looks_like_task_row(line: str) -> bool:
    if not line.startswith("|") or line.startswith("|---") or line.startswith("|ID|"):
        return False
    cells = line.strip().strip("|").split("|")
    return len(cells) >= 8 and bool(cells[0].strip())


def _record_core(record: dict[str, Any] | Any) -> dict[str, Any]:
    return {
        "task_id": record["task_id"],
        "path": record["path"],
        "file_sha256": record["file_sha256"],
        "fragment_checksum": record["fragment_checksum"],
        "partition": record["partition"],
        "order": record["order"],
    }


def _chain_genesis() -> str:
    return _sha256_bytes(b"arch_005_task_registry_index.v1:genesis")


def _entry_chain(previous: str, core: dict[str, Any]) -> str:
    return _sha256_bytes(f"{previous}:{_canonical_sha256(core)}".encode())


def _yaml_bytes(payload: dict[str, Any]) -> bytes:
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).encode("utf-8")


def _load_mapping(path: Path) -> dict[str, Any]:
    value = safe_load_yaml_path(path)
    if not isinstance(value, dict):
        _fail("MAPPING_REQUIRED", str(path))
    return value


def _load_generated_mapping(path: Path) -> dict[str, Any]:
    value = _load_mapping(path)
    if path.read_bytes() != _yaml_bytes(value):
        _fail("NON_CANONICAL_GENERATED_YAML", str(path))
    return value


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("MAPPING_REQUIRED", label)
    return value


def _list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        _fail("LIST_REQUIRED", label)
    return value


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail("POSITIVE_INT_REQUIRED", label)
    return value


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail("TEXT_REQUIRED", label)
    return value.strip()


def _portable_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("PATH_REQUIRED", label)
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or normalized != path.as_posix():
        _fail("PATH_INVALID", f"{label}:{value}")
    return normalized


def _regular_path(root: Path, relative: str, label: str) -> Path:
    path = (root / _portable_path(relative, label)).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        _fail("PATH_OUTSIDE_ROOT", f"{label}:{relative}")
    if not path.is_file() or path.is_symlink():
        _fail("REGULAR_FILE_REQUIRED", f"{label}:{relative}")
    return path


def _regular_output_path(root: Path, relative: str, label: str) -> Path:
    path = (root / _portable_path(relative, label)).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        _fail("PATH_OUTSIDE_ROOT", f"{label}:{relative}")
    if path.exists() and path.is_symlink():
        _fail("SYMLINK_FORBIDDEN", f"{label}:{relative}")
    return path


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return _sha256_bytes(encoded)


def _payload_checksum(payload: dict[str, Any] | Any, field: str) -> str:
    return _canonical_sha256({key: value for key, value in payload.items() if key != field})


def _verify_checksum(payload: dict[str, Any], field: str, code: str) -> None:
    value = payload.get(field)
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        _fail(code, str(value))
    if value != _payload_checksum(payload, field):
        _fail(code, "checksum mismatch")


def _validate_event_timestamp(value: object, event_id: str) -> None:
    timestamp = _required_text(value, "event.occurred_at")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        _fail("EVENT_OCCURRED_AT", event_id)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("EVENT_OCCURRED_AT_TIMEZONE", event_id)


def _fail(code: str, message: str) -> NoReturn:
    raise CanonicalTaskRegistryError(code, message)


__all__ = [
    "ACTIVE_VIEW_PATH",
    "CANONICAL_FRAGMENT_ROOT",
    "CANONICAL_INDEX_PATH",
    "CANONICAL_SOURCE",
    "COMPLETED_VIEW_PATH",
    "CanonicalTaskRegistry",
    "CanonicalTaskRegistryError",
    "build_consumer_inventory",
    "build_cutover_candidate",
    "canonical_task_register_view_path",
    "load_cutover_policy",
    "refresh_consumer_inventory",
    "register_task",
    "run_rollback_rehearsal",
    "update_task",
    "validate_canonical_fragment",
    "validate_canonical_registry",
]
