from __future__ import annotations

import copy
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    SHADOW_FRAGMENT_SCHEMA_VERSION,
    TaskRegistryShadowError,
    build_s0_baseline,
    build_shadow_fragment,
    build_shadow_index,
    load_legacy_documents,
    load_shadow_fragments,
    render_compatibility_view,
    validate_s0_baseline,
    validate_shadow_fragment,
    validate_shadow_index,
    write_shadow_fragments,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_s0_baseline_freezes_lossless_inventory_and_honest_ambiguity(
    tmp_path: Path,
) -> None:
    documents = _legacy_documents(tmp_path)
    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )

    inventory = baseline["inventory"]
    assert inventory["active_task_count"] == 1
    assert inventory["completed_task_count"] == 1
    assert inventory["total_task_count"] == 2
    assert inventory["unique_task_count"] == 2
    assert inventory["task_id_overlap_count"] == 0
    assert inventory["ambiguous_extra_cell_row_count"] == 1
    assert baseline["source_of_truth"] == {
        "mode": "LEGACY_MARKDOWN_ONLY",
        "writable_paths": ["docs/task_register.md", "docs/task_register_completed.md"],
        "shadow_registry_root": "registry/development_tasks_shadow",
        "shadow_registry_writable": False,
        "dual_write_allowed": False,
        "cutover_performed": False,
    }
    assert baseline["contract_schemas"]["lease"]["acquisition_enabled_in_s0_s1"] is False
    assert baseline["contract_schemas"]["scheduler_decision"]["dispatch_enabled_in_s0_s1"] is False
    validate_s0_baseline(baseline, documents=documents)


def test_s1_shadow_fragment_preserves_all_cells_without_guessing(tmp_path: Path) -> None:
    row = _legacy_documents(tmp_path)[0].rows[0]
    fragment = build_shadow_fragment(row, source_commit="a" * 40)

    assert fragment["schema_version"] == SHADOW_FRAGMENT_SCHEMA_VERSION
    assert fragment["shadow_only"] is True
    assert fragment["task_record"]["legacy_source"]["ambiguous_unescaped_pipe_boundaries"] is True
    assert fragment["task_record"]["legacy_source"]["all_cells"] == [
        "ARCH-101",
        "Architecture / sample",
        "P0",
        "IN_PROGRESS",
        "owner",
        "next step",
        "criterion `A",
        "B`",
        "notes",
    ]
    assert fragment["projection"]["legacy_first_eight_cells"][-1] == "B`"
    assert fragment["initial_event"]["occurred_at"] is None
    assert fragment["initial_event"]["history_completeness"] == "LEGACY_HISTORY_PARTIAL"
    validate_shadow_fragment(fragment)


def test_s1_shadow_registry_replays_byte_identical_views(tmp_path: Path) -> None:
    documents = _legacy_documents(tmp_path)
    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )
    fragments = tuple(
        build_shadow_fragment(row, source_commit="a" * 40)
        for document in documents
        for row in document.rows
    )
    files = write_shadow_fragments(project_root=tmp_path, fragments=fragments)
    loaded = load_shadow_fragments(project_root=tmp_path, records=files)
    index = build_shadow_index(
        baseline=baseline,
        documents=documents,
        fragments=loaded,
        fragment_files=files,
    )

    assert index["status"] == "PASS"
    assert index["task_count"] == 2
    assert index["semantic_parity"]["all_raw_cells"] == "PASS"
    assert all(view["byte_identical"] for view in index["generated_views"])
    by_id = {fragment["task_record"]["task_id"]: fragment for fragment in loaded}
    for document in documents:
        assert render_compatibility_view(document, by_id) == document.raw_bytes


def test_shadow_fragment_and_s0_baseline_tamper_fail_closed(tmp_path: Path) -> None:
    documents = _legacy_documents(tmp_path)
    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )
    tampered_baseline = copy.deepcopy(baseline)
    tampered_baseline["inventory"]["total_task_count"] = 3
    with pytest.raises(TaskRegistryShadowError, match="S0_TASK_COUNT_DRIFT"):
        validate_s0_baseline(tampered_baseline, documents=documents)

    fragment = build_shadow_fragment(documents[0].rows[0], source_commit="a" * 40)
    fragment["task_record"]["legacy_source"]["raw_line"] += "tamper"
    with pytest.raises(TaskRegistryShadowError, match="SHADOW_RAW_ROW_HASH"):
        validate_shadow_fragment(fragment)

    with pytest.raises(TaskRegistryShadowError, match="SHADOW_SOURCE_COMMIT"):
        build_shadow_fragment(documents[0].rows[0], source_commit="not-a-commit")


def test_shadow_index_rejects_fragment_from_another_source_commit(tmp_path: Path) -> None:
    documents = _legacy_documents(tmp_path)
    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )
    fragments = tuple(
        build_shadow_fragment(row, source_commit="c" * 40)
        for document in documents
        for row in document.rows
    )

    with pytest.raises(TaskRegistryShadowError, match="SHADOW_SOURCE_COMMIT"):
        build_shadow_index(
            baseline=baseline,
            documents=documents,
            fragments=fragments,
            fragment_files=[],
        )


def test_s0_rejects_terminal_partition_drift(tmp_path: Path) -> None:
    _write_registers(tmp_path, active_status="DONE")
    documents = load_legacy_documents(tmp_path)

    with pytest.raises(TaskRegistryShadowError, match="TERMINAL_PROJECTION_DRIFT"):
        build_s0_baseline(
            project_root=tmp_path,
            handoff=_handoff(),
            documents=documents,
        )


def test_repository_s0_s1_artifacts_are_fresh_and_replayable() -> None:
    documents = load_legacy_documents(PROJECT_ROOT)
    baseline = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_task_registry_baseline.yaml"
    )
    index = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_task_shadow_index.yaml"
    )
    assert isinstance(baseline, dict)
    assert isinstance(index, dict)
    validate_s0_baseline(baseline, documents=documents)
    records = index["fragments"]
    fragments = load_shadow_fragments(project_root=PROJECT_ROOT, records=records)
    rebuilt = build_shadow_index(
        baseline=baseline,
        documents=documents,
        fragments=fragments,
        fragment_files=records,
    )
    validate_shadow_index(index, baseline=baseline, documents=documents)

    assert rebuilt == index
    assert index["task_count"] == baseline["inventory"]["total_task_count"]
    assert index["missing_task_count"] == 0
    assert index["duplicate_task_count"] == 0
    assert all(view["byte_identical"] for view in index["generated_views"])


def _legacy_documents(root: Path):
    _write_registers(root, active_status="IN_PROGRESS")
    return load_legacy_documents(root)


def _write_registers(root: Path, *, active_status: str) -> None:
    docs = root / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    header = (
        "|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|\r\n"
        "|---|---|---|---|---|---|---|---|\r\n"
    )
    active_row = (
        "|ARCH-101|Architecture / sample|P0|"
        f"{active_status}|owner|next step|criterion `A|B`|notes|\r\n"
    )
    completed_row = "|ARCH-102|Architecture / complete|P1|DONE|owner|none|accepted|notes|\r\n"
    (docs / "task_register.md").write_bytes(
        ("# Active\r\n\r\n" + header + active_row).encode("utf-8")
    )
    (docs / "task_register_completed.md").write_bytes(
        ("# Completed\r\n\r\n" + header + completed_row).encode("utf-8")
    )


def _handoff() -> dict[str, object]:
    return {
        "schema_version": "arch_005_bootstrap_handoff.v1",
        "handoff_checksum": "b" * 64,
        "head_commit": "a" * 40,
        "next_slice_unblocked": False,
    }
