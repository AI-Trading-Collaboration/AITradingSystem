from __future__ import annotations

import hashlib
import json
import os
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts.validation_session import (
    ArtifactFingerprintInventory,
    ArtifactFingerprintScope,
    artifact_content_identity,
    cached_artifact_validation,
)

# Bounded protocol limits for resolving only the committed validation DAG. These are
# parser-safety constants, not investment or research-sample thresholds.
_MAX_SNAPSHOTS = 64
_MAX_DEPTH = 12
_MAX_FILE_BYTES = 64 * 1024 * 1024
_MAX_TOTAL_FILE_BYTES = 512 * 1024 * 1024
_MAX_JSON_NODES_PER_SNAPSHOT = 500_000
_MAX_TOTAL_JSON_NODES = 2_000_000
_MAX_BINDING_FILES = 256
_MAX_CACHE_BINDINGS = 64
_MAX_EXPLICIT_PATHS = 256
_MAX_INVENTORIES = 64
_MAX_TOTAL_PATHS = 512
_MAX_PATH_BYTES = 32 * 1024
_MAX_PATH_COMPONENTS = 256
_MAX_TOTAL_PATH_BYTES = 1024 * 1024
_MAX_TOTAL_PATH_COMPONENTS = 4096

VALIDATION_SCOPE_VERSION = "targeted-upstream-hardened-dag.v1"

_SNAPSHOT_SCHEMAS = {
    "search_coverage_gap_input_snapshot.json": "search_coverage_gap_input_snapshot.v2",
    "cash_buffer_attribution_input_snapshot.json": "cash_buffer_attribution_input_snapshot.v2",
    "near_miss_candidates_input_snapshot.json": "near_miss_candidates_input_snapshot.v2",
    "no_promotion_review_input_snapshot.json": "no_promotion_review_input_snapshot.v2",
    "weight_scorecard_input_snapshot.json": "weight_scorecard_input_snapshot.v2",
    "weight_batch_backfill_input_snapshot.json": "weight_batch_backfill_input_snapshot.v2",
    "weight_experiment_batch2_input_snapshot.json": "weight_experiment_batch2_input_snapshot.v2",
    "weight_search_space_input_snapshot.json": "weight_search_space_input_snapshot.v2",
    "paper_shadow_backfill_input_snapshot.json": "paper_shadow_backfill_input_snapshot.v2",
    "model_target_input_snapshot.json": "model_target_input_snapshot.v2",
}

_ROOT_SNAPSHOTS = frozenset(
    {
        "search_coverage_gap_input_snapshot.json",
        "cash_buffer_attribution_input_snapshot.json",
        "near_miss_candidates_input_snapshot.json",
        "no_promotion_review_input_snapshot.json",
        "weight_scorecard_input_snapshot.json",
        "weight_batch_backfill_input_snapshot.json",
        "weight_search_space_input_snapshot.json",
        "paper_shadow_backfill_input_snapshot.json",
    }
)

# Each tuple is (snapshot field, artifact kind, child snapshot, binding envelope).
_EDGE_SPECS = {
    "search_coverage_gap_input_snapshot.v2": (
        (
            "search_space_source",
            "weight_search_space",
            "weight_search_space_input_snapshot.json",
            "foundation",
        ),
        (
            "near_miss_source",
            "near_miss_candidates",
            "near_miss_candidates_input_snapshot.json",
            "foundation",
        ),
        (
            "attribution_source",
            "cash_buffer_attribution",
            "cash_buffer_attribution_input_snapshot.json",
            "foundation",
        ),
    ),
    "cash_buffer_attribution_input_snapshot.v2": (
        (
            "scorecard_source",
            "weight_scorecard",
            "weight_scorecard_input_snapshot.json",
            "foundation",
        ),
        (
            "near_miss_source",
            "near_miss_candidates",
            "near_miss_candidates_input_snapshot.json",
            "foundation",
        ),
    ),
    "near_miss_candidates_input_snapshot.v2": (
        (
            "scorecard_source",
            "weight_scorecard",
            "weight_scorecard_input_snapshot.json",
            "foundation",
        ),
        (
            "review_source",
            "no_promotion_review",
            "no_promotion_review_input_snapshot.json",
            "foundation",
        ),
    ),
    "no_promotion_review_input_snapshot.v2": (
        (
            "scorecard_source",
            "weight_scorecard",
            "weight_scorecard_input_snapshot.json",
            "foundation",
        ),
    ),
    "weight_scorecard_input_snapshot.v2": (
        (
            "backfill_source",
            "weight_batch_backfill",
            "weight_batch_backfill_input_snapshot.json",
            "foundation",
        ),
        (
            "matrix_source",
            "weight_experiment_batch2",
            "weight_experiment_batch2_input_snapshot.json",
            "foundation",
        ),
    ),
    "weight_batch_backfill_input_snapshot.v2": (
        (
            "matrix_source",
            "weight_experiment_batch2",
            "weight_experiment_batch2_input_snapshot.json",
            "foundation",
        ),
        (
            "paper_backfill_source",
            "paper_shadow_backfill",
            "paper_shadow_backfill_input_snapshot.json",
            "foundation",
        ),
    ),
    "weight_experiment_batch2_input_snapshot.v2": (
        (
            "search_source",
            "weight_search_space",
            "weight_search_space_input_snapshot.json",
            "foundation",
        ),
    ),
    "weight_search_space_input_snapshot.v2": (),
    "paper_shadow_backfill_input_snapshot.v2": (
        (
            "model_target_source",
            "model_target",
            "model_target_input_snapshot.json",
            "operations",
        ),
    ),
    "model_target_input_snapshot.v2": (),
}


class _ValidationScopeResolutionError(ValueError):
    """The live source DAG cannot be fingerprinted completely and safely."""


@dataclass
class _ValidationScopeBudget:
    snapshot_bytes: int = 0
    json_nodes: int = 0
    path_bytes: int = 0
    path_components: int = 0
    seen_paths: set[str] = field(default_factory=set)


def _canonical_scope_key(path: Path) -> str:
    try:
        return os.path.normcase(str(path.resolve(strict=False)))
    except (OSError, RuntimeError, ValueError) as exc:
        raise _ValidationScopeResolutionError(f"path cannot be resolved: {path}") from exc


def _bounded_scope_path(
    value: Any,
    *,
    source: str,
    budget: _ValidationScopeBudget,
) -> Path:
    if not isinstance(value, str) or not value:
        raise _ValidationScopeResolutionError(f"{source} path missing")
    try:
        path = Path(value)
        encoded = value.encode("utf-8")
    except (OSError, RuntimeError, UnicodeError, ValueError) as exc:
        raise _ValidationScopeResolutionError(f"{source} path invalid") from exc
    if (
        not path.is_absolute()
        or ".." in path.parts
        or len(encoded) > _MAX_PATH_BYTES
        or len(path.parts) > _MAX_PATH_COMPONENTS
    ):
        raise _ValidationScopeResolutionError(f"{source} path outside bounded contract")
    key = _canonical_scope_key(path)
    if key not in budget.seen_paths:
        if len(budget.seen_paths) >= _MAX_TOTAL_PATHS:
            raise _ValidationScopeResolutionError("validation scope aggregate path count exceeded")
        next_path_bytes = budget.path_bytes + len(encoded)
        next_path_components = budget.path_components + len(path.parts)
        if (
            next_path_bytes > _MAX_TOTAL_PATH_BYTES
            or next_path_components > _MAX_TOTAL_PATH_COMPONENTS
        ):
            raise _ValidationScopeResolutionError("validation scope aggregate path budget exceeded")
        budget.seen_paths.add(key)
        budget.path_bytes = next_path_bytes
        budget.path_components = next_path_components
    return path


def _consume_scope_json_budget(
    payload: Mapping[str, Any], *, budget: _ValidationScopeBudget
) -> None:
    stack: list[Any] = [payload]
    local_nodes = 0
    while stack:
        local_nodes += 1
        budget.json_nodes += 1
        if local_nodes > _MAX_JSON_NODES_PER_SNAPSHOT or budget.json_nodes > _MAX_TOTAL_JSON_NODES:
            raise _ValidationScopeResolutionError("validation scope JSON node budget exceeded")
        value = stack.pop()
        children: Sequence[Any] = ()
        if isinstance(value, Mapping):
            children = tuple(value.values())
        elif isinstance(value, list):
            children = value
        if (
            local_nodes + len(stack) + len(children) > _MAX_JSON_NODES_PER_SNAPSHOT
            or budget.json_nodes + len(stack) + len(children) > _MAX_TOTAL_JSON_NODES
        ):
            raise _ValidationScopeResolutionError("validation scope JSON node budget exceeded")
        stack.extend(children)


def _stable_scope_snapshot(
    path: Path,
    *,
    expected_schema: str,
    expected_sha256: str | None,
    expected_size_bytes: int | None,
    budget: _ValidationScopeBudget,
) -> tuple[dict[str, Any], str]:
    before_identity = artifact_content_identity(path)
    if before_identity is None:
        raise _ValidationScopeResolutionError(f"snapshot cannot be fingerprinted: {path}")
    try:
        size_bytes = path.stat().st_size
        if size_bytes > _MAX_FILE_BYTES:
            raise _ValidationScopeResolutionError(f"snapshot exceeds size budget: {path}")
        raw = bytearray()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                if len(raw) + len(chunk) > _MAX_FILE_BYTES:
                    raise _ValidationScopeResolutionError(
                        f"snapshot exceeds size budget while reading: {path}"
                    )
                raw.extend(chunk)
    except OSError as exc:
        raise _ValidationScopeResolutionError(f"snapshot cannot be read: {path}") from exc
    if len(raw) != size_bytes:
        raise _ValidationScopeResolutionError(f"snapshot size changed while reading: {path}")
    if budget.snapshot_bytes + len(raw) > _MAX_TOTAL_FILE_BYTES:
        raise _ValidationScopeResolutionError(
            "validation scope aggregate snapshot byte budget exceeded"
        )
    budget.snapshot_bytes += len(raw)
    digest = hashlib.sha256(raw).hexdigest()
    after_identity = artifact_content_identity(path)
    if before_identity != digest or after_identity != before_identity:
        raise _ValidationScopeResolutionError(f"snapshot changed while resolving scope: {path}")
    if expected_sha256 is not None and digest != expected_sha256:
        raise _ValidationScopeResolutionError(f"snapshot commitment drift: {path}")
    if expected_size_bytes is not None and len(raw) != expected_size_bytes:
        raise _ValidationScopeResolutionError(f"snapshot size commitment drift: {path}")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise _ValidationScopeResolutionError(f"snapshot JSON invalid: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != expected_schema:
        raise _ValidationScopeResolutionError(f"snapshot schema mismatch: {path}")
    _consume_scope_json_budget(payload, budget=budget)
    return payload, before_identity


def _scope_file_commitment(
    raw: Any,
    *,
    source: str,
    budget: _ValidationScopeBudget,
    expected_path: Path | None = None,
) -> tuple[Path, str, int]:
    if not isinstance(raw, Mapping):
        raise _ValidationScopeResolutionError(f"{source} commitment missing")
    path = _bounded_scope_path(raw.get("path"), source=source, budget=budget)
    sha256 = raw.get("sha256")
    size_bytes = raw.get("size_bytes")
    if (
        not isinstance(sha256, str)
        or len(sha256) != 64
        or any(character not in "0123456789abcdef" for character in sha256)
        or isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or size_bytes < 0
    ):
        raise _ValidationScopeResolutionError(f"{source} commitment invalid")
    if expected_path is not None and _canonical_scope_key(path) != _canonical_scope_key(
        expected_path
    ):
        raise _ValidationScopeResolutionError(f"{source} commitment path mismatch")
    return path, sha256, size_bytes


def _scope_binding_files(
    raw: Any,
    *,
    field_name: str,
    expected_kind: str,
    envelope: str,
    budget: _ValidationScopeBudget,
) -> tuple[str, Path, dict[str, tuple[Path, str, int]]]:
    if not isinstance(raw, Mapping) or raw.get("kind") != expected_kind:
        raise _ValidationScopeResolutionError(f"{field_name} binding kind mismatch")
    artifact_id = raw.get("artifact_id")
    if (
        not isinstance(artifact_id, str)
        or not artifact_id
        or Path(artifact_id).name != artifact_id
        or artifact_id in {".", ".."}
    ):
        raise _ValidationScopeResolutionError(f"{field_name} artifact id invalid")
    operations_bundle: Mapping[str, Any] | None = None
    if envelope == "foundation":
        source_dir_raw = raw.get("source_dir")
        files_raw = raw.get("files")
    elif envelope == "operations":
        bundle = raw.get("bundle")
        if not isinstance(bundle, Mapping):
            raise _ValidationScopeResolutionError(f"{field_name} bundle missing")
        operations_bundle = bundle
        files_raw = bundle.get("files")
        if (
            bundle.get("schema_version") != "content_commitment_bundle.v1"
            or isinstance(bundle.get("canonical_file_count"), bool)
            or bundle.get("canonical_file_count")
            != (len(files_raw) if isinstance(files_raw, Mapping) else -1)
            or any(not isinstance(bundle.get(name), Mapping) for name in ("json", "jsonl", "text"))
        ):
            raise _ValidationScopeResolutionError(f"{field_name} bundle envelope invalid")
        source_dir_raw = bundle.get("source_dir")
    else:
        raise _ValidationScopeResolutionError(f"{field_name} envelope unsupported")
    source_dir = _bounded_scope_path(
        source_dir_raw,
        source=f"{field_name} source directory",
        budget=budget,
    )
    if source_dir.name != artifact_id:
        raise _ValidationScopeResolutionError(f"{field_name} artifact directory mismatch")
    if not isinstance(files_raw, Mapping) or not files_raw or len(files_raw) > _MAX_BINDING_FILES:
        raise _ValidationScopeResolutionError(f"{field_name} binding files invalid")
    files: dict[str, tuple[Path, str, int]] = {}
    for name, commitment in files_raw.items():
        if not isinstance(name, str) or not name or Path(name).name != name or name in {".", ".."}:
            raise _ValidationScopeResolutionError(f"{field_name} file name invalid")
        files[name] = _scope_file_commitment(
            commitment,
            source=f"{field_name}.{name}",
            budget=budget,
            expected_path=source_dir / name,
        )
    if operations_bundle is not None:
        for view_type in ("json", "jsonl", "text"):
            view_names = operations_bundle.get(view_type)
            if not isinstance(view_names, Mapping):
                raise _ValidationScopeResolutionError(f"{field_name} {view_type} views invalid")
            if any(name not in files for name in view_names):
                raise _ValidationScopeResolutionError(
                    f"{field_name} {view_type} view is not committed"
                )
    return artifact_id, source_dir, files


def _scope_snapshot_edges(
    payload: Mapping[str, Any],
    *,
    expected_schema: str,
    budget: _ValidationScopeBudget,
) -> dict[str, tuple[Path, str, int]]:
    specs = _EDGE_SPECS.get(expected_schema)
    if specs is None:
        raise _ValidationScopeResolutionError(
            f"validation scope schema is not allowlisted: {expected_schema}"
        )
    edges: dict[str, tuple[Path, str, int]] = {}
    for field_name, expected_kind, snapshot_name, envelope in specs:
        _, _, files = _scope_binding_files(
            payload.get(field_name),
            field_name=field_name,
            expected_kind=expected_kind,
            envelope=envelope,
            budget=budget,
        )
        edge = files.get(snapshot_name)
        if edge is None:
            raise _ValidationScopeResolutionError(
                f"{field_name} required snapshot commitment missing"
            )
        edges[field_name] = edge
    return edges


def hardened_upstream_validation_scope(
    *,
    artifact_root: Path,
    snapshot_name: str,
) -> ArtifactFingerprintScope | None:
    """Resolve the complete committed weight-search validation DAG or bypass caching."""
    if snapshot_name not in _ROOT_SNAPSHOTS:
        return None
    expected_root_schema = _SNAPSHOT_SCHEMAS.get(snapshot_name)
    if expected_root_schema is None:
        return None
    budget = _ValidationScopeBudget()
    try:
        root_snapshot = (artifact_root / snapshot_name).resolve(strict=False)
        root_snapshot = _bounded_scope_path(
            str(root_snapshot), source="root snapshot", budget=budget
        )
        root_key = _canonical_scope_key(root_snapshot)
    except (OSError, RuntimeError, TypeError, ValueError):
        return None
    queue: deque[tuple[Path, str, str | None, int | None, int]] = deque(
        [(root_snapshot, expected_root_schema, None, None, 0)]
    )
    scheduled_commitments: dict[str, tuple[str | None, int | None]] = {root_key: (None, None)}
    parsed_identities: dict[str, tuple[Path, str]] = {}
    explicit_paths: dict[str, Path] = {}
    inventories: dict[tuple[str, tuple[str, ...]], ArtifactFingerprintInventory] = {}
    saw_paper_snapshot = False
    saw_model_snapshot = False
    requires_complete_paper_dag = expected_root_schema != "weight_search_space_input_snapshot.v2"

    def add_explicit_path(raw_path: Any, *, source: str) -> Path:
        path = _bounded_scope_path(raw_path, source=source, budget=budget)
        key = _canonical_scope_key(path)
        if key not in explicit_paths:
            if len(explicit_paths) >= _MAX_EXPLICIT_PATHS:
                raise _ValidationScopeResolutionError(
                    "validation scope explicit path budget exceeded"
                )
            explicit_paths[key] = path
        return path

    def add_inventory(raw_root: Any, *, pattern: str, source: str) -> None:
        root = _bounded_scope_path(raw_root, source=source, budget=budget)
        key = (_canonical_scope_key(root), (pattern,))
        if key not in inventories:
            if len(inventories) >= _MAX_INVENTORIES:
                raise _ValidationScopeResolutionError("validation scope inventory budget exceeded")
            inventories[key] = ArtifactFingerprintInventory(root=root, patterns=(pattern,))

    try:
        while queue:
            path, expected_schema, expected_sha256, expected_size, depth = queue.popleft()
            if depth > _MAX_DEPTH:
                raise _ValidationScopeResolutionError("validation scope depth budget exceeded")
            path_key = _canonical_scope_key(path)
            if path_key in parsed_identities:
                continue
            payload, identity = _stable_scope_snapshot(
                path,
                expected_schema=expected_schema,
                expected_sha256=expected_sha256,
                expected_size_bytes=expected_size,
                budget=budget,
            )
            parsed_identities[path_key] = (path, identity)
            edges = _scope_snapshot_edges(payload, expected_schema=expected_schema, budget=budget)

            if expected_schema == "paper_shadow_backfill_input_snapshot.v2":
                saw_paper_snapshot = True
                cache_bindings = payload.get("cache_bindings")
                if (
                    not isinstance(cache_bindings, list)
                    or not cache_bindings
                    or len(cache_bindings) > _MAX_CACHE_BINDINGS
                ):
                    raise _ValidationScopeResolutionError("paper cache bindings missing")
                for binding in cache_bindings:
                    if (
                        not isinstance(binding, Mapping)
                        or not isinstance(binding.get("kind"), str)
                        or not isinstance(binding.get("required"), bool)
                    ):
                        raise _ValidationScopeResolutionError("paper cache binding invalid")
                    cache_path = add_explicit_path(
                        binding.get("path"), source="paper cache binding"
                    )
                    commitment = binding.get("commitment")
                    if commitment is not None:
                        committed_path, _, _ = _scope_file_commitment(
                            commitment,
                            source="paper cache binding",
                            budget=budget,
                            expected_path=cache_path,
                        )
                        if _canonical_scope_key(committed_path) != _canonical_scope_key(cache_path):
                            raise _ValidationScopeResolutionError(
                                "paper cache commitment path mismatch"
                            )
                selection = payload.get("model_target_selection")
                if not isinstance(selection, Mapping):
                    raise _ValidationScopeResolutionError("model target selection missing")
                model_edge = edges["model_target_source"]
                selected_model_dir = model_edge[0].parent
                selection_root = _bounded_scope_path(
                    selection.get("root"),
                    source="model target selection",
                    budget=budget,
                )
                if selection.get("artifact_id") != selected_model_dir.name or _canonical_scope_key(
                    selection_root / selected_model_dir.name
                ) != _canonical_scope_key(selected_model_dir):
                    raise _ValidationScopeResolutionError(
                        "model target selection does not match committed edge"
                    )
                add_inventory(
                    str(selection_root),
                    pattern="*/model_target_manifest.json",
                    source="model target selection",
                )
            elif expected_schema == "weight_batch_backfill_input_snapshot.v2":
                price_path, _, _ = _scope_file_commitment(
                    payload.get("price_source"),
                    source="weight price source",
                    budget=budget,
                )
                _scope_file_commitment(
                    payload.get("rates_source"),
                    source="weight rates source",
                    budget=budget,
                )
                price_path = add_explicit_path(str(price_path), source="weight price source")
                add_explicit_path(
                    str(price_path.parent / "download_manifest.csv"),
                    source="weight download manifest sibling",
                )
                add_explicit_path(
                    str(price_path.parent / "prices_marketstack_daily.csv"),
                    source="weight secondary price sibling",
                )
            elif expected_schema == "model_target_input_snapshot.v2":
                saw_model_snapshot = True
                daily_id, daily_dir, daily_files = _scope_binding_files(
                    payload.get("daily_source"),
                    field_name="daily_source",
                    expected_kind="daily_advisory",
                    envelope="operations",
                    budget=budget,
                )
                required_daily_files = {
                    "daily_advisory_manifest.json",
                    "daily_advisory_actions.json",
                    "daily_candidate_targets.jsonl",
                    "daily_consensus_weights.csv",
                }
                if not required_daily_files.issubset(daily_files):
                    raise _ValidationScopeResolutionError(
                        "daily advisory canonical files incomplete"
                    )
                selection = payload.get("source_selection")
                if not isinstance(selection, Mapping):
                    raise _ValidationScopeResolutionError("daily advisory selection missing")
                selection_root = _bounded_scope_path(
                    selection.get("root"),
                    source="daily advisory selection",
                    budget=budget,
                )
                if (
                    selection.get("role") != "daily_advisory"
                    or selection.get("artifact_id") != daily_id
                    or _canonical_scope_key(selection_root / daily_id)
                    != _canonical_scope_key(daily_dir)
                ):
                    raise _ValidationScopeResolutionError(
                        "daily advisory selection does not match committed source"
                    )
                add_inventory(
                    str(selection_root),
                    pattern="*/daily_advisory_manifest.json",
                    source="daily advisory selection",
                )

            for edge_path, edge_sha256, edge_size in edges.values():
                edge_schema = _SNAPSHOT_SCHEMAS[edge_path.name]
                edge_key = _canonical_scope_key(edge_path)
                edge_commitment = (edge_sha256, edge_size)
                previous = scheduled_commitments.get(edge_key)
                if previous is not None and previous != edge_commitment:
                    raise _ValidationScopeResolutionError(
                        f"conflicting transitive snapshot edge: {edge_path}"
                    )
                if previous is None:
                    if len(scheduled_commitments) >= _MAX_SNAPSHOTS:
                        raise _ValidationScopeResolutionError(
                            "validation scope snapshot budget exceeded"
                        )
                    scheduled_commitments[edge_key] = edge_commitment
                    queue.append((edge_path, edge_schema, edge_sha256, edge_size, depth + 1))

        if requires_complete_paper_dag and (not saw_paper_snapshot or not saw_model_snapshot):
            raise _ValidationScopeResolutionError(
                "validation scope does not reach the complete paper/model DAG"
            )
        if requires_complete_paper_dag and (
            not explicit_paths
            or not {
                ("*/model_target_manifest.json",),
                ("*/daily_advisory_manifest.json",),
            }.issubset({inventory.patterns for inventory in inventories.values()})
        ):
            raise _ValidationScopeResolutionError("validation scope dependency coverage incomplete")
        if any(
            artifact_content_identity(path) != identity
            for path, identity in parsed_identities.values()
        ):
            raise _ValidationScopeResolutionError(
                "validation scope snapshot changed before resolution completed"
            )
    except (OSError, RuntimeError, TypeError, ValueError):
        return None

    return ArtifactFingerprintScope(
        paths=tuple(explicit_paths[key] for key in sorted(explicit_paths)),
        metadata_paths=(),
        inventories=tuple(inventories[key] for key in sorted(inventories)),
        discover_bound_paths=True,
    )


def validate_upstream_with_hardened_scope(
    *,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    artifact_id: str,
    output_dir: Path,
    snapshot_name: str,
) -> dict[str, Any]:
    """Use PASS-only synchronous-session reuse or execute the real validator directly."""
    try:
        scope = hardened_upstream_validation_scope(
            artifact_root=output_dir / artifact_id,
            snapshot_name=snapshot_name,
        )
    except Exception:  # noqa: BLE001 - resolver failure must bypass caching, never validation.
        scope = None
    if scope is None:
        return validator(**{validator_key: artifact_id, "output_dir": output_dir})
    return cached_artifact_validation(
        validator=validator,
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=output_dir,
        validator_version=VALIDATION_SCOPE_VERSION,
        fingerprint_scope=scope,
    )


__all__ = [
    "VALIDATION_SCOPE_VERSION",
    "hardened_upstream_validation_scope",
    "validate_upstream_with_hardened_scope",
]
