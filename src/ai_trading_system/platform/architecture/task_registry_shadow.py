from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.platform.architecture.devex import (
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_RECORD_SCHEMA_VERSION = "task_record.v1"
TASK_EVENT_SCHEMA_VERSION = "task_event.v1"
TASK_DEPENDENCY_SCHEMA_VERSION = "task_dependency.v1"
EXECUTION_LEASE_SCHEMA_VERSION = "execution_lease.v1"
SCHEDULER_DECISION_SCHEMA_VERSION = "scheduler_decision.v1"
GENERATED_VIEW_SCHEMA_VERSION = "task_register_generated_view.v1"
S0_BASELINE_SCHEMA_VERSION = "arch_005_task_registry_baseline.v1"
SHADOW_FRAGMENT_SCHEMA_VERSION = "arch_005_task_shadow_fragment.v1"
SHADOW_INDEX_SCHEMA_VERSION = "arch_005_task_shadow_index.v1"
LEGACY_PARSER_VERSION = "task_register_markdown_parser.v1_characterized"
SHADOW_COMPILER_VERSION = "arch_005_shadow_registry_compiler.v1"

ACTIVE_REGISTER_PATH = "docs/task_register.md"
COMPLETED_REGISTER_PATH = "docs/task_register_completed.md"
SHADOW_REGISTRY_ROOT = "registry/development_tasks_shadow"

TERMINAL_STATUSES = frozenset({"DONE", "DROPPED"})
VALID_LEGACY_STATUSES = frozenset(
    {
        "PROPOSED",
        "READY",
        "IN_PROGRESS",
        "BLOCKED_OWNER_INPUT",
        "BLOCKED_EXTERNAL",
        "BASELINE_DONE",
        "VALIDATING",
        "DONE",
        "DEFERRED",
        "DROPPED",
    }
)

_DOC_LINK_RE = re.compile(r"docs/[A-Za-z0-9_./-]+\.md")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class TaskRegistryShadowError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class LegacyTaskRow:
    source: str
    source_path: str
    line_number: int
    raw_line: str
    line_ending: str
    cells: tuple[str, ...]

    @property
    def task_id(self) -> str:
        return self.cells[0]

    @property
    def projected_cells(self) -> tuple[str, ...]:
        return self.cells[:8]

    @property
    def row_sha256(self) -> str:
        return _sha256_bytes(self.raw_line.encode("utf-8"))

    @property
    def docs_links(self) -> tuple[str, ...]:
        return tuple(sorted(set(_DOC_LINK_RE.findall(self.raw_line))))


@dataclass(frozen=True)
class LegacyRegisterDocument:
    source: str
    source_path: str
    raw_bytes: bytes
    rows: tuple[LegacyTaskRow, ...]

    @property
    def sha256(self) -> str:
        return _sha256_bytes(self.raw_bytes)


def parse_legacy_register(path: Path, *, source: str, project_root: Path) -> LegacyRegisterDocument:
    root = project_root.resolve()
    resolved = path.resolve()
    try:
        portable = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise TaskRegistryShadowError("REGISTER_OUTSIDE_ROOT", str(path)) from exc
    raw = resolved.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TaskRegistryShadowError("REGISTER_NOT_UTF8", portable) from exc
    rows: list[LegacyTaskRow] = []
    for line_number, physical_line in enumerate(text.splitlines(keepends=True), start=1):
        raw_line, ending = _split_line_ending(physical_line)
        cells = _legacy_cells(raw_line)
        if cells is None:
            continue
        rows.append(
            LegacyTaskRow(
                source=source,
                source_path=portable,
                line_number=line_number,
                raw_line=raw_line,
                line_ending=ending,
                cells=cells,
            )
        )
    return LegacyRegisterDocument(source, portable, raw, tuple(rows))


def load_legacy_documents(project_root: Path) -> tuple[LegacyRegisterDocument, ...]:
    root = project_root.resolve()
    return (
        parse_legacy_register(
            root / ACTIVE_REGISTER_PATH,
            source="active",
            project_root=root,
        ),
        parse_legacy_register(
            root / COMPLETED_REGISTER_PATH,
            source="completed",
            project_root=root,
        ),
    )


def build_s0_baseline(
    *,
    project_root: Path,
    handoff: Mapping[str, Any],
    documents: Sequence[LegacyRegisterDocument] | None = None,
) -> dict[str, Any]:
    docs = tuple(documents or load_legacy_documents(project_root))
    rows = tuple(row for document in docs for row in document.rows)
    _validate_inventory(rows)
    consumers = characterize_task_register_consumers(project_root)
    document_records = [_document_record(document) for document in docs]
    status_counts = Counter(row.projected_cells[3] for row in rows)
    owner_values = sorted({row.projected_cells[4] for row in rows})
    docs_links = sorted({link for row in rows for link in row.docs_links})
    row_records = [
        {
            "task_id": row.task_id,
            "source": row.source,
            "line_number": row.line_number,
            "row_sha256": row.row_sha256,
            "cell_count": len(row.cells),
            "field_projection": "first_eight_cells_legacy_v1",
            "history_completeness": "LEGACY_HISTORY_PARTIAL",
        }
        for row in rows
    ]
    payload: dict[str, Any] = {
        "schema_version": S0_BASELINE_SCHEMA_VERSION,
        "status": "PASS",
        "task_id": "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE",
        "stage": "S0_CONTRACT_INVENTORY_CHARACTERIZATION",
        "entry_gate": {
            "schema_version": handoff.get("schema_version"),
            "handoff_checksum": handoff.get("handoff_checksum"),
            "source_commit": handoff.get("head_commit"),
            "handoff_commit_is_separate": True,
            "next_slice_unblocked": handoff.get("next_slice_unblocked"),
            "status": "PASS",
        },
        "contract_schemas": _contract_schema_freeze(),
        "source_of_truth": {
            "mode": "LEGACY_MARKDOWN_ONLY",
            "writable_paths": [ACTIVE_REGISTER_PATH, COMPLETED_REGISTER_PATH],
            "shadow_registry_root": SHADOW_REGISTRY_ROOT,
            "shadow_registry_writable": False,
            "dual_write_allowed": False,
            "cutover_performed": False,
        },
        "inventory": {
            "parser_version": LEGACY_PARSER_VERSION,
            "documents": document_records,
            "active_task_count": len(docs[0].rows),
            "completed_task_count": len(docs[1].rows),
            "total_task_count": len(rows),
            "unique_task_count": len({row.task_id for row in rows}),
            "task_id_overlap_count": len(rows) - len({row.task_id for row in rows}),
            "ambiguous_extra_cell_row_count": sum(len(row.cells) > 8 for row in rows),
            "status_counts": dict(sorted(status_counts.items())),
            "task_id_set_sha256": _canonical_sha256(sorted(row.task_id for row in rows)),
            "status_set_sha256": _canonical_sha256(sorted(status_counts)),
            "owner_set_sha256": _canonical_sha256(owner_values),
            "docs_link_set_sha256": _canonical_sha256(docs_links),
            "row_checksums": row_records,
        },
        "consumer_characterization": consumers,
        "compatibility_projection": {
            "active_statuses": sorted(VALID_LEGACY_STATUSES - TERMINAL_STATUSES),
            "terminal_statuses": sorted(TERMINAL_STATUSES),
            "terminal_projection": "source_register_boundary_and_status_must_agree",
            "row_order": "source_line_order",
            "renderer": "replace_task_rows_in_legacy_document_skeleton",
            "last_updated_rule": "preserve_legacy_source_until_event_time_cutover",
            "legacy_extra_cell_policy": (
                "preserve_raw_line_and_all_cells; expose legacy first-eight-cell projection; "
                "do_not_guess_unescaped_pipe_boundaries"
            ),
        },
        "ownership": {
            "cutover_owner": "project_owner",
            "rollback_owner": "architecture_coordinator_and_project_owner",
            "generated_view_owner": "integration_coordinator",
            "worker_generated_view_write_allowed": False,
        },
        "safety": {
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_status_mutation_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "producer_version": SHADOW_COMPILER_VERSION,
    }
    payload["baseline_checksum"] = _payload_checksum(payload, "baseline_checksum")
    validate_s0_baseline(payload, documents=docs)
    return payload


def validate_s0_baseline(
    payload: Mapping[str, Any],
    *,
    documents: Sequence[LegacyRegisterDocument],
) -> None:
    if payload.get("schema_version") != S0_BASELINE_SCHEMA_VERSION:
        raise TaskRegistryShadowError("S0_BASELINE_SCHEMA", "unsupported schema")
    if payload.get("status") != "PASS":
        raise TaskRegistryShadowError("S0_BASELINE_STATUS", "baseline must PASS")
    inventory = _mapping(payload.get("inventory"), "inventory")
    rows = tuple(row for document in documents for row in document.rows)
    _validate_inventory(rows)
    expected_docs = [_document_record(document) for document in documents]
    if inventory.get("documents") != expected_docs:
        raise TaskRegistryShadowError("S0_DOCUMENT_DRIFT", "register bytes changed")
    if inventory.get("total_task_count") != len(rows):
        raise TaskRegistryShadowError("S0_TASK_COUNT_DRIFT", str(len(rows)))
    checksum = str(payload.get("baseline_checksum") or "")
    if not _SHA256_RE.fullmatch(checksum) or checksum != _payload_checksum(
        payload, "baseline_checksum"
    ):
        raise TaskRegistryShadowError("S0_BASELINE_CHECKSUM", "checksum mismatch")
    source = _mapping(payload.get("source_of_truth"), "source_of_truth")
    if source.get("mode") != "LEGACY_MARKDOWN_ONLY" or source.get("dual_write_allowed"):
        raise TaskRegistryShadowError("S0_SOURCE_OF_TRUTH", "shadow stage must not cut over")


def build_shadow_fragment(row: LegacyTaskRow, *, source_commit: str) -> dict[str, Any]:
    if not _GIT_SHA_RE.fullmatch(source_commit):
        raise TaskRegistryShadowError("SHADOW_SOURCE_COMMIT", source_commit)
    fields = row.projected_cells
    event_id = f"legacy-import-{row.row_sha256[:24]}"
    payload: dict[str, Any] = {
        "schema_version": SHADOW_FRAGMENT_SCHEMA_VERSION,
        "shadow_only": True,
        "source_of_truth": "LEGACY_MARKDOWN",
        "task_record": {
            "schema_version": TASK_RECORD_SCHEMA_VERSION,
            "task_id": row.task_id,
            "title": None,
            "domain": fields[1],
            "parent_task_id": None,
            "created_at": None,
            "created_by": None,
            "priority": fields[2],
            "accountable_owner": None,
            "next_owner": fields[4],
            "requirement_refs": list(row.docs_links),
            "module_ids": [],
            "contract_versions": [],
            "dependencies": [],
            "unstructured_legacy_blocker_or_next_step": fields[5],
            "acceptance_criteria": [
                {"criterion_id": "legacy_markdown", "text": fields[6]}
            ],
            "production_effect": "UNKNOWN_LEGACY",
            "broker_action": "UNKNOWN_LEGACY",
            "legacy_source": {
                "path": row.source_path,
                "source_partition": row.source,
                "line_number": row.line_number,
                "row_sha256": row.row_sha256,
                "cell_count": len(row.cells),
                "all_cells": list(row.cells),
                "raw_line": row.raw_line,
                "history_completeness": "LEGACY_HISTORY_PARTIAL",
                "ambiguous_unescaped_pipe_boundaries": len(row.cells) > 8,
            },
        },
        "initial_event": {
            "schema_version": TASK_EVENT_SCHEMA_VERSION,
            "event_id": event_id,
            "task_id": row.task_id,
            "event_type": "LEGACY_IMPORT",
            "occurred_at": None,
            "actor": "arch_005_shadow_importer",
            "change_id": "ARCH-005-S1-SHADOW-IMPORT",
            "lane_id": None,
            "base_commit": source_commit,
            "previous_state_event_id": None,
            "from_status": None,
            "to_status": fields[3],
            "payload": {
                "priority": fields[2],
                "next_owner": fields[4],
                "notes": fields[7],
            },
            "rationale": "Lossless legacy row import; historical event time is unavailable.",
            "evidence_refs": [f"sha256:{row.row_sha256}"],
            "history_completeness": "LEGACY_HISTORY_PARTIAL",
        },
        "projection": {
            "legacy_first_eight_cells": list(fields),
            "docs_links": list(row.docs_links),
            "terminal": fields[3] in TERMINAL_STATUSES,
            "raw_row_sha256": row.row_sha256,
        },
        "producer_version": SHADOW_COMPILER_VERSION,
    }
    payload["fragment_checksum"] = _payload_checksum(payload, "fragment_checksum")
    validate_shadow_fragment(payload)
    return payload


def validate_shadow_fragment(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SHADOW_FRAGMENT_SCHEMA_VERSION:
        raise TaskRegistryShadowError("SHADOW_FRAGMENT_SCHEMA", "unsupported schema")
    if (
        payload.get("shadow_only") is not True
        or payload.get("source_of_truth") != "LEGACY_MARKDOWN"
    ):
        raise TaskRegistryShadowError("SHADOW_FRAGMENT_AUTHORITY", "must remain shadow-only")
    task = _mapping(payload.get("task_record"), "task_record")
    event = _mapping(payload.get("initial_event"), "initial_event")
    projection = _mapping(payload.get("projection"), "projection")
    if task.get("schema_version") != TASK_RECORD_SCHEMA_VERSION:
        raise TaskRegistryShadowError("TASK_RECORD_SCHEMA", str(task.get("task_id")))
    if event.get("schema_version") != TASK_EVENT_SCHEMA_VERSION:
        raise TaskRegistryShadowError("TASK_EVENT_SCHEMA", str(task.get("task_id")))
    if task.get("task_id") != event.get("task_id"):
        raise TaskRegistryShadowError("TASK_EVENT_IDENTITY", str(task.get("task_id")))
    cells = projection.get("legacy_first_eight_cells")
    if not isinstance(cells, list) or len(cells) != 8 or cells[0] != task.get("task_id"):
        raise TaskRegistryShadowError("SHADOW_PROJECTION_FIELDS", str(task.get("task_id")))
    legacy = _mapping(task.get("legacy_source"), "legacy_source")
    raw_line = str(legacy.get("raw_line") or "")
    if _sha256_bytes(raw_line.encode("utf-8")) != projection.get("raw_row_sha256"):
        raise TaskRegistryShadowError("SHADOW_RAW_ROW_HASH", str(task.get("task_id")))
    checksum = str(payload.get("fragment_checksum") or "")
    if not _SHA256_RE.fullmatch(checksum) or checksum != _payload_checksum(
        payload, "fragment_checksum"
    ):
        raise TaskRegistryShadowError("SHADOW_FRAGMENT_CHECKSUM", str(task.get("task_id")))


def shadow_fragment_path(fragment: Mapping[str, Any]) -> str:
    task = _mapping(fragment.get("task_record"), "task_record")
    legacy = _mapping(task.get("legacy_source"), "legacy_source")
    task_id = str(task["task_id"])
    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
    source = str(legacy["source_partition"])
    return f"{SHADOW_REGISTRY_ROOT}/{source}/{digest[:2]}/{digest}.yaml"


def write_shadow_fragments(
    *,
    project_root: Path,
    fragments: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    root = project_root.resolve()
    target_root = (root / SHADOW_REGISTRY_ROOT).resolve()
    try:
        target_root.relative_to(root)
    except ValueError as exc:
        raise TaskRegistryShadowError("SHADOW_ROOT_OUTSIDE_PROJECT", str(target_root)) from exc
    expected: set[Path] = set()
    records: list[dict[str, Any]] = []
    for fragment in sorted(
        fragments,
        key=lambda item: str(_mapping(item.get("task_record"), "task_record")["task_id"]),
    ):
        validate_shadow_fragment(fragment)
        relative = shadow_fragment_path(fragment)
        path = (root / relative).resolve()
        path.relative_to(target_root)
        write_generated_architecture_artifact(path, fragment)
        expected.add(path)
        task = _mapping(fragment["task_record"], "task_record")
        records.append(
            {
                "task_id": task["task_id"],
                "path": relative,
                "file_sha256": _sha256_bytes(path.read_bytes()),
                "fragment_checksum": fragment["fragment_checksum"],
            }
        )
    if target_root.exists():
        for stale in sorted(target_root.rglob("*.yaml")):
            if stale.resolve() not in expected:
                stale.unlink()
    return tuple(records)


def load_shadow_fragments(
    *, project_root: Path, records: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], ...]:
    root = project_root.resolve()
    fragments: list[dict[str, Any]] = []
    for record in records:
        relative = _portable_path(record.get("path"), "fragment.path")
        path = (root / relative).resolve()
        if _sha256_bytes(path.read_bytes()) != record.get("file_sha256"):
            raise TaskRegistryShadowError("SHADOW_FILE_HASH", relative)
        value = safe_load_yaml_path(path)
        if not isinstance(value, dict):
            raise TaskRegistryShadowError("SHADOW_FILE_MAPPING", relative)
        validate_shadow_fragment(value)
        task = _mapping(value.get("task_record"), "task_record")
        if task.get("task_id") != record.get("task_id"):
            raise TaskRegistryShadowError("SHADOW_INDEX_TASK_BINDING", relative)
        if shadow_fragment_path(value) != relative:
            raise TaskRegistryShadowError("SHADOW_INDEX_PATH_BINDING", relative)
        if value.get("fragment_checksum") != record.get("fragment_checksum"):
            raise TaskRegistryShadowError("SHADOW_INDEX_BINDING", relative)
        fragments.append(value)
    return tuple(fragments)


def build_shadow_index(
    *,
    baseline: Mapping[str, Any],
    documents: Sequence[LegacyRegisterDocument],
    fragments: Sequence[Mapping[str, Any]],
    fragment_files: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_id: dict[str, Mapping[str, Any]] = {}
    for fragment in fragments:
        validate_shadow_fragment(fragment)
        task = _mapping(fragment["task_record"], "task_record")
        event = _mapping(fragment["initial_event"], "initial_event")
        task_id = str(task["task_id"])
        if task_id in by_id:
            raise TaskRegistryShadowError("SHADOW_DUPLICATE_TASK", task_id)
        entry_gate = _mapping(baseline.get("entry_gate"), "entry_gate")
        if event.get("base_commit") != entry_gate.get("source_commit"):
            raise TaskRegistryShadowError("SHADOW_SOURCE_COMMIT", task_id)
        by_id[task_id] = fragment
    source_rows = [row for document in documents for row in document.rows]
    if set(by_id) != {row.task_id for row in source_rows}:
        raise TaskRegistryShadowError("SHADOW_TASK_SET_DRIFT", "fragment/source mismatch")
    for row in source_rows:
        _assert_fragment_matches_row(row, by_id[row.task_id])
    views: list[dict[str, Any]] = []
    for document in documents:
        rendered = render_compatibility_view(document, by_id)
        views.append(
            {
                "source": document.source,
                "source_path": document.source_path,
                "shadow_output_path": (
                    f"outputs/architecture/arch_005_shadow_views/{Path(document.source_path).name}"
                ),
                "source_sha256": document.sha256,
                "rendered_sha256": _sha256_bytes(rendered),
                "byte_identical": rendered == document.raw_bytes,
                "persisted_in_git": False,
            }
        )
    if not all(record["byte_identical"] for record in views):
        raise TaskRegistryShadowError("SHADOW_VIEW_PARITY", "rendered bytes differ")
    payload: dict[str, Any] = {
        "schema_version": SHADOW_INDEX_SCHEMA_VERSION,
        "status": "PASS",
        "stage": "S1_SHADOW_REGISTRY_AND_COMPATIBILITY_PROJECTION",
        "baseline_checksum": baseline.get("baseline_checksum"),
        "source_of_truth": "LEGACY_MARKDOWN_ONLY",
        "task_count": len(by_id),
        "fragment_count": len(fragment_files),
        "missing_task_count": 0,
        "duplicate_task_count": 0,
        "semantic_parity": {
            "task_id": "PASS",
            "all_raw_cells": "PASS",
            "legacy_first_eight_field_projection": "PASS",
            "status": "PASS",
            "priority": "PASS",
            "owner": "PASS",
            "blocker_or_next_step": "PASS",
            "acceptance_criteria": "PASS",
            "notes": "PASS",
            "terminal_classification": "PASS",
            "docs_links": "PASS",
            "ambiguous_rows_preserved_without_guessing": "PASS",
        },
        "fragments": list(fragment_files),
        "generated_views": views,
        "replay": {
            "deterministic": True,
            "byte_identical": True,
            "renderer_schema_version": GENERATED_VIEW_SCHEMA_VERSION,
            "compiler_version": SHADOW_COMPILER_VERSION,
        },
        "safety": {
            "dual_write_allowed": False,
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_status_mutation_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    payload["index_checksum"] = _payload_checksum(payload, "index_checksum")
    validate_shadow_index(payload, baseline=baseline, documents=documents)
    return payload


def validate_shadow_index(
    payload: Mapping[str, Any],
    *,
    baseline: Mapping[str, Any],
    documents: Sequence[LegacyRegisterDocument],
) -> None:
    if payload.get("schema_version") != SHADOW_INDEX_SCHEMA_VERSION:
        raise TaskRegistryShadowError("SHADOW_INDEX_SCHEMA", "unsupported schema")
    if payload.get("status") != "PASS" or payload.get("source_of_truth") != "LEGACY_MARKDOWN_ONLY":
        raise TaskRegistryShadowError("SHADOW_INDEX_STATUS", "shadow index must PASS")
    if payload.get("baseline_checksum") != baseline.get("baseline_checksum"):
        raise TaskRegistryShadowError("SHADOW_BASELINE_BINDING", "baseline checksum mismatch")
    expected_count = sum(len(document.rows) for document in documents)
    if (
        payload.get("task_count") != expected_count
        or payload.get("fragment_count") != expected_count
    ):
        raise TaskRegistryShadowError("SHADOW_INDEX_COUNT", str(expected_count))
    views = payload.get("generated_views")
    if not isinstance(views, list) or not all(
        isinstance(record, Mapping) and record.get("byte_identical") is True
        for record in views
    ):
        raise TaskRegistryShadowError("SHADOW_VIEW_PARITY", "views must be byte-identical")
    checksum = str(payload.get("index_checksum") or "")
    if not _SHA256_RE.fullmatch(checksum) or checksum != _payload_checksum(
        payload, "index_checksum"
    ):
        raise TaskRegistryShadowError("SHADOW_INDEX_CHECKSUM", "checksum mismatch")


def render_compatibility_view(
    document: LegacyRegisterDocument,
    fragments_by_task_id: Mapping[str, Mapping[str, Any]],
) -> bytes:
    text = document.raw_bytes.decode("utf-8")
    rendered: list[str] = []
    for physical_line in text.splitlines(keepends=True):
        raw_line, ending = _split_line_ending(physical_line)
        cells = _legacy_cells(raw_line)
        if cells is None:
            rendered.append(physical_line)
            continue
        task_id = cells[0]
        try:
            fragment = fragments_by_task_id[task_id]
        except KeyError as exc:
            raise TaskRegistryShadowError("SHADOW_VIEW_TASK_MISSING", task_id) from exc
        task = _mapping(fragment.get("task_record"), "task_record")
        legacy = _mapping(task.get("legacy_source"), "legacy_source")
        if legacy.get("source_partition") != document.source:
            raise TaskRegistryShadowError("SHADOW_VIEW_SOURCE_DRIFT", task_id)
        rendered.append(f"{legacy['raw_line']}{ending}")
    return "".join(rendered).encode("utf-8")


def characterize_task_register_consumers(project_root: Path) -> dict[str, Any]:
    root = project_root.resolve()
    records: list[dict[str, Any]] = []
    for top in ("src", "tests", "scripts"):
        base = root / top
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            targets = [
                target
                for target in (ACTIVE_REGISTER_PATH, COMPLETED_REGISTER_PATH)
                if target in text
                or Path(target).name in text
            ]
            if not targets:
                continue
            role = "runtime" if top == "src" else "test" if top == "tests" else "script"
            records.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "role": role,
                    "targets": targets,
                    "reference_count": sum(text.count(Path(target).name) for target in targets),
                    "migration_status": "LEGACY_DIRECT_OR_LITERAL_CONSUMER",
                }
            )
    return {
        "status": "PASS_CHARACTERIZED",
        "consumer_count": len(records),
        "runtime_consumer_count": sum(record["role"] == "runtime" for record in records),
        "test_consumer_count": sum(record["role"] == "test" for record in records),
        "script_consumer_count": sum(record["role"] == "script" for record in records),
        "consumer_set_sha256": _canonical_sha256(records),
        "consumers": records,
    }


def _document_record(document: LegacyRegisterDocument) -> dict[str, Any]:
    newline_counts = Counter(_newline_name(row.line_ending) for row in document.rows)
    return {
        "source": document.source,
        "path": document.source_path,
        "byte_count": len(document.raw_bytes),
        "sha256": document.sha256,
        "row_count": len(document.rows),
        "final_newline": document.raw_bytes.endswith((b"\n", b"\r")),
        "task_id_order_sha256": _canonical_sha256([row.task_id for row in document.rows]),
        "row_checksum_order_sha256": _canonical_sha256(
            [row.row_sha256 for row in document.rows]
        ),
        "task_row_newline_counts": dict(sorted(newline_counts.items())),
    }


def _assert_fragment_matches_row(
    row: LegacyTaskRow,
    fragment: Mapping[str, Any],
) -> None:
    task = _mapping(fragment.get("task_record"), "task_record")
    event = _mapping(fragment.get("initial_event"), "initial_event")
    projection = _mapping(fragment.get("projection"), "projection")
    legacy = _mapping(task.get("legacy_source"), "legacy_source")
    fields = row.projected_cells
    expected = {
        "task_id": task.get("task_id") == row.task_id,
        "all_cells": legacy.get("all_cells") == list(row.cells),
        "raw_line": legacy.get("raw_line") == row.raw_line,
        "projected_cells": projection.get("legacy_first_eight_cells") == list(fields),
        "domain": task.get("domain") == fields[1],
        "priority": task.get("priority") == fields[2],
        "status": event.get("to_status") == fields[3],
        "next_owner": task.get("next_owner") == fields[4],
        "blocker": task.get("unstructured_legacy_blocker_or_next_step") == fields[5],
        "acceptance": task.get("acceptance_criteria")
        == [{"criterion_id": "legacy_markdown", "text": fields[6]}],
        "notes": _mapping(event.get("payload"), "event.payload").get("notes")
        == fields[7],
        "docs_links": projection.get("docs_links") == list(row.docs_links),
        "terminal": projection.get("terminal")
        == (fields[3] in TERMINAL_STATUSES),
    }
    failed = sorted(name for name, passed in expected.items() if not passed)
    if failed:
        raise TaskRegistryShadowError(
            "SHADOW_SEMANTIC_DRIFT",
            f"{row.task_id}:{failed}",
        )


def _validate_inventory(rows: Sequence[LegacyTaskRow]) -> None:
    ids = [row.task_id for row in rows]
    duplicates = sorted(task_id for task_id, count in Counter(ids).items() if count > 1)
    if duplicates:
        raise TaskRegistryShadowError("TASK_ID_OVERLAP", str(duplicates))
    invalid_statuses = sorted(
        {
            row.projected_cells[3]
            for row in rows
            if row.projected_cells[3] not in VALID_LEGACY_STATUSES
        }
    )
    if invalid_statuses:
        raise TaskRegistryShadowError("LEGACY_STATUS_UNKNOWN", str(invalid_statuses))
    for row in rows:
        terminal = row.projected_cells[3] in TERMINAL_STATUSES
        if (row.source == "completed") != terminal:
            raise TaskRegistryShadowError(
                "TERMINAL_PROJECTION_DRIFT",
                f"{row.task_id}:{row.source}:{row.projected_cells[3]}",
            )


def _contract_schema_freeze() -> dict[str, Any]:
    return {
        "task_record": {
            "schema_version": TASK_RECORD_SCHEMA_VERSION,
            "required_fields": [
                "task_id", "title", "domain", "parent_task_id", "created_at",
                "created_by", "priority", "accountable_owner", "next_owner",
                "requirement_refs", "module_ids", "contract_versions", "dependencies",
                "acceptance_criteria", "production_effect", "broker_action", "legacy_source",
            ],
        },
        "task_event": {
            "schema_version": TASK_EVENT_SCHEMA_VERSION,
            "required_fields": [
                "event_id", "task_id", "event_type", "occurred_at", "actor",
                "change_id", "lane_id", "base_commit", "previous_state_event_id",
                "from_status", "to_status", "payload", "rationale", "evidence_refs",
            ],
            "mutually_exclusive_changes_require_causal_chain": True,
        },
        "dependency": {
            "schema_version": TASK_DEPENDENCY_SCHEMA_VERSION,
            "edge_types": ["blocks_start", "blocks_completion", "parent_child", "informational"],
            "legacy_prose_inference_allowed": False,
        },
        "lease": {
            "schema_version": EXECUTION_LEASE_SCHEMA_VERSION,
            "lifecycle": ["REQUESTED", "ACTIVE", "RELEASED", "EXPIRED", "REASSIGNED", "BLOCKED"],
            "acquisition_enabled_in_s0_s1": False,
        },
        "scheduler_decision": {
            "schema_version": SCHEDULER_DECISION_SCHEMA_VERSION,
            "required_outputs": [
                "selected",
                "not_selected",
                "reason_codes",
                "alternatives",
                "policy_version",
            ],
            "dispatch_enabled_in_s0_s1": False,
        },
        "generated_view": {
            "schema_version": GENERATED_VIEW_SCHEMA_VERSION,
            "targets": [ACTIVE_REGISTER_PATH, COMPLETED_REGISTER_PATH],
            "worker_write_allowed": False,
        },
    }


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


def _newline_name(value: str) -> str:
    return {"\r\n": "CRLF", "\n": "LF", "\r": "CR", "": "NONE"}[value]


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TaskRegistryShadowError("MAPPING_REQUIRED", label)
    return value


def _portable_path(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TaskRegistryShadowError("PATH_REQUIRED", field)
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or normalized != path.as_posix():
        raise TaskRegistryShadowError("PATH_INVALID", f"{field}:{value}")
    return normalized


def _canonical_sha256(value: object) -> str:
    return _sha256_bytes(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    )


def _payload_checksum(payload: Mapping[str, Any], field: str) -> str:
    return _canonical_sha256({key: value for key, value in payload.items() if key != field})


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


__all__ = [
    "ACTIVE_REGISTER_PATH",
    "COMPLETED_REGISTER_PATH",
    "EXECUTION_LEASE_SCHEMA_VERSION",
    "GENERATED_VIEW_SCHEMA_VERSION",
    "LEGACY_PARSER_VERSION",
    "S0_BASELINE_SCHEMA_VERSION",
    "SCHEDULER_DECISION_SCHEMA_VERSION",
    "SHADOW_COMPILER_VERSION",
    "SHADOW_FRAGMENT_SCHEMA_VERSION",
    "SHADOW_INDEX_SCHEMA_VERSION",
    "SHADOW_REGISTRY_ROOT",
    "TASK_DEPENDENCY_SCHEMA_VERSION",
    "TASK_EVENT_SCHEMA_VERSION",
    "TASK_RECORD_SCHEMA_VERSION",
    "LegacyRegisterDocument",
    "LegacyTaskRow",
    "TaskRegistryShadowError",
    "build_s0_baseline",
    "build_shadow_fragment",
    "build_shadow_index",
    "characterize_task_register_consumers",
    "load_legacy_documents",
    "load_shadow_fragments",
    "parse_legacy_register",
    "render_compatibility_view",
    "shadow_fragment_path",
    "validate_s0_baseline",
    "validate_shadow_fragment",
    "validate_shadow_index",
    "write_shadow_fragments",
]
