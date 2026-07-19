from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture import (
    build_s0_baseline,
    build_shadow_fragment,
    build_shadow_index,
    load_legacy_documents,
    load_shadow_fragments,
    render_compatibility_view,
    validate_bootstrap_handoff,
    validate_s0_baseline,
    validate_shadow_index,
    write_generated_architecture_artifact,
    write_shadow_fragments,
)
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HANDOFF_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_bootstrap_handoff.yaml"
BASELINE_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_task_registry_baseline.yaml"
INDEX_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_task_shadow_index.yaml"
VIEW_ROOT = PROJECT_ROOT / "outputs/architecture/arch_005_shadow_views"


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-005 S0/S1 shadow task registry")
    parser.add_argument("command", choices=("generate", "validate"))
    args = parser.parse_args()
    if args.command == "generate":
        return _generate()
    return _validate()


def _generate() -> int:
    handoff = _load_mapping(HANDOFF_PATH)
    _validate_entry_handoff(handoff)
    documents = load_legacy_documents(PROJECT_ROOT)
    baseline = build_s0_baseline(
        project_root=PROJECT_ROOT,
        handoff=handoff,
        documents=documents,
    )
    write_generated_architecture_artifact(BASELINE_PATH, baseline)
    source_commit = str(handoff["head_commit"])
    fragments = tuple(
        build_shadow_fragment(row, source_commit=source_commit)
        for document in documents
        for row in document.rows
    )
    fragment_files = write_shadow_fragments(
        project_root=PROJECT_ROOT,
        fragments=fragments,
    )
    index = build_shadow_index(
        baseline=baseline,
        documents=documents,
        fragments=fragments,
        fragment_files=fragment_files,
    )
    write_generated_architecture_artifact(INDEX_PATH, index)
    by_id = {
        str(_mapping(fragment["task_record"])["task_id"]): fragment
        for fragment in fragments
    }
    for document in documents:
        write_bytes_atomic(
            VIEW_ROOT / Path(document.source_path).name,
            render_compatibility_view(document, by_id),
        )
    _print_summary(baseline, index)
    return 0


def _validate() -> int:
    handoff = _load_mapping(HANDOFF_PATH)
    _validate_entry_handoff(handoff)
    documents = load_legacy_documents(PROJECT_ROOT)
    baseline = _load_mapping(BASELINE_PATH)
    validate_s0_baseline(baseline, documents=documents)
    expected_baseline = build_s0_baseline(
        project_root=PROJECT_ROOT,
        handoff=handoff,
        documents=documents,
    )
    if baseline != expected_baseline:
        raise SystemExit("S0 baseline is stale or non-deterministic")
    index = _load_mapping(INDEX_PATH)
    records = index.get("fragments")
    if not isinstance(records, list):
        raise SystemExit("shadow index fragments must be a list")
    fragments = load_shadow_fragments(project_root=PROJECT_ROOT, records=records)
    expected_index = build_shadow_index(
        baseline=baseline,
        documents=documents,
        fragments=fragments,
        fragment_files=records,
    )
    validate_shadow_index(index, baseline=baseline, documents=documents)
    if index != expected_index:
        raise SystemExit("S1 shadow index is stale or non-deterministic")
    _print_summary(baseline, index)
    return 0


def _validate_entry_handoff(payload: dict[str, Any]) -> None:
    validate_bootstrap_handoff(
        payload,
        project_root=PROJECT_ROOT,
        frozen_tracked_files=_frozen_tracked_files(payload),
    )
    if payload.get("next_slice_unblocked") is not False:
        raise SystemExit("ARCH-005 S0 requires next_slice_unblocked=false")


def _frozen_tracked_files(payload: dict[str, Any]) -> dict[str, bytes]:
    head = str(payload["head_commit"])
    matrix = _mapping(payload["migration_matrix"])
    architecture_state = _mapping(payload["architecture_state"])
    attribution = _mapping(payload["worktree_attribution"])
    paths = {str(matrix["path"]), str(attribution["attribution_path"])}
    for value in architecture_state.values():
        paths.add(str(_mapping(value)["path"]))
    result: dict[str, bytes] = {}
    for relative in sorted(paths):
        completed = subprocess.run(
            ["git", "show", f"{head}:{relative}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
        )
        result[relative] = completed.stdout
    return result


def _load_mapping(path: Path) -> dict[str, Any]:
    value = safe_load_yaml_path(path)
    if not isinstance(value, dict):
        raise SystemExit(f"expected mapping: {path}")
    return value


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit("expected mapping")
    return value


def _print_summary(baseline: dict[str, Any], index: dict[str, Any]) -> None:
    inventory = _mapping(baseline["inventory"])
    consumers = _mapping(baseline["consumer_characterization"])
    print(
        json.dumps(
            {
                "status": "PASS",
                "s0_baseline": str(BASELINE_PATH),
                "s1_index": str(INDEX_PATH),
                "task_count": index["task_count"],
                "active_task_count": inventory["active_task_count"],
                "completed_task_count": inventory["completed_task_count"],
                "ambiguous_extra_cell_row_count": inventory[
                    "ambiguous_extra_cell_row_count"
                ],
                "consumer_count": consumers["consumer_count"],
                "byte_identical": _mapping(index["replay"])["byte_identical"],
                "source_of_truth": index["source_of_truth"],
                "production_effect": _mapping(index["safety"])["production_effect"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
