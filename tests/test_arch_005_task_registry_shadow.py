from __future__ import annotations

import copy
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    SHADOW_FRAGMENT_SCHEMA_VERSION,
    SHADOW_V2_FRAGMENT_SCHEMA_VERSION,
    TaskRegistryShadowError,
    build_s0_baseline,
    build_shadow_fragment,
    build_shadow_index,
    build_shadow_v2_fragment,
    build_shadow_v2_index,
    characterize_task_register_consumers,
    load_legacy_documents,
    load_shadow_fragments,
    load_shadow_v2_fragments,
    render_compatibility_view,
    render_compatibility_view_v2,
    shadow_v2_fragment_path,
    validate_s0_baseline,
    validate_shadow_fragment,
    validate_shadow_v2_fragment,
    validate_shadow_v2_index,
    write_shadow_fragments,
    write_shadow_v2_fragments,
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


def test_task_shadow_v2_fragment_excludes_mutable_source_location(
    tmp_path: Path,
) -> None:
    row = _legacy_documents(tmp_path)[0].rows[0]
    fragment = build_shadow_v2_fragment(row, source_commit="a" * 40)

    assert fragment["schema_version"] == SHADOW_V2_FRAGMENT_SCHEMA_VERSION
    assert fragment["stable_task_identity"]["task_id"] == "ARCH-101"
    assert fragment["legacy_row_evidence"]["all_cells"] == list(row.cells)
    assert fragment["legacy_row_evidence"]["row_sha256"] == row.row_sha256
    serialized = repr(fragment)
    assert "source_partition" not in serialized
    assert "source_path" not in serialized
    assert "line_number" not in serialized
    assert shadow_v2_fragment_path(fragment).startswith("registry/development_tasks_shadow_v2/")
    validate_shadow_v2_fragment(fragment)


def test_task_shadow_v2_replays_with_index_only_locators(tmp_path: Path) -> None:
    documents = _legacy_documents(tmp_path)
    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )
    fragments = tuple(
        build_shadow_v2_fragment(row, source_commit="a" * 40)
        for document in documents
        for row in document.rows
    )
    files = write_shadow_v2_fragments(
        project_root=tmp_path,
        fragments=fragments,
    )
    loaded = load_shadow_v2_fragments(project_root=tmp_path, records=files)
    index = build_shadow_v2_index(
        baseline=baseline,
        documents=documents,
        fragments=loaded,
        fragment_files=files,
    )

    assert index["status"] == "PASS"
    assert index["source_of_truth"] == "LEGACY_MARKDOWN_ONLY"
    assert index["cutover_performed"] is False
    assert index["v1_coexists"] is True
    assert index["location_contract"]["locator_authority"] == "INDEX_ONLY"
    assert {record["locator"]["source_partition"] for record in index["fragments"]} == {
        "active",
        "completed",
    }
    by_id = {fragment["stable_task_identity"]["task_id"]: fragment for fragment in loaded}
    for document in documents:
        assert render_compatibility_view_v2(document, by_id) == document.raw_bytes
    validate_shadow_v2_index(index, baseline=baseline, documents=documents)


def test_task_shadow_v2_middle_insert_does_not_rewrite_existing_fragments(
    tmp_path: Path,
) -> None:
    _write_task_rows(
        tmp_path,
        active_rows=(
            "|ARCH-101|Architecture / first|P0|IN_PROGRESS|owner|next|accept|notes|\n",
            "|ARCH-103|Architecture / third|P1|READY|owner|next|accept|notes|\n",
        ),
        completed_rows=("|ARCH-104|Architecture / done|P1|DONE|owner|none|accepted|notes|\n",),
    )
    before_documents = load_legacy_documents(tmp_path)
    before = {
        row.task_id: build_shadow_v2_fragment(row, source_commit="a" * 40)
        for document in before_documents
        for row in document.rows
    }
    before_paths = {
        task_id: shadow_v2_fragment_path(fragment) for task_id, fragment in before.items()
    }

    _write_task_rows(
        tmp_path,
        active_rows=(
            "|ARCH-101|Architecture / first|P0|IN_PROGRESS|owner|next|accept|notes|\n",
            "|ARCH-102|Architecture / inserted|P1|READY|owner|next|accept|notes|\n",
            "|ARCH-103|Architecture / third|P1|READY|owner|next|accept|notes|\n",
        ),
        completed_rows=("|ARCH-104|Architecture / done|P1|DONE|owner|none|accepted|notes|\n",),
    )
    after_documents = load_legacy_documents(tmp_path)
    after = {
        row.task_id: build_shadow_v2_fragment(row, source_commit="a" * 40)
        for document in after_documents
        for row in document.rows
    }

    assert set(after) - set(before) == {"ARCH-102"}
    for task_id in before:
        assert after[task_id] == before[task_id]
        assert shadow_v2_fragment_path(after[task_id]) == before_paths[task_id]


def test_task_shadow_v2_active_to_completed_keeps_path_and_moves_locator(
    tmp_path: Path,
) -> None:
    _write_task_rows(
        tmp_path,
        active_rows=("|ARCH-101|Architecture / sample|P0|IN_PROGRESS|owner|next|accept|notes|\n",),
        completed_rows=(),
    )
    active_documents = load_legacy_documents(tmp_path)
    active_row = active_documents[0].rows[0]
    active_fragment = build_shadow_v2_fragment(active_row, source_commit="a" * 40)

    _write_task_rows(
        tmp_path,
        active_rows=(),
        completed_rows=("|ARCH-101|Architecture / sample|P0|DONE|owner|none|accept|notes|\n",),
    )
    completed_documents = load_legacy_documents(tmp_path)
    completed_row = completed_documents[1].rows[0]
    completed_fragment = build_shadow_v2_fragment(
        completed_row,
        source_commit="a" * 40,
    )
    assert shadow_v2_fragment_path(active_fragment) == shadow_v2_fragment_path(completed_fragment)

    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=completed_documents,
    )
    files = write_shadow_v2_fragments(
        project_root=tmp_path,
        fragments=(completed_fragment,),
    )
    index = build_shadow_v2_index(
        baseline=baseline,
        documents=completed_documents,
        fragments=(completed_fragment,),
        fragment_files=files,
    )
    assert index["fragments"][0]["locator"]["source_partition"] == "completed"
    assert completed_fragment["initial_event"]["to_status"] == "DONE"


def test_task_shadow_v2_tamper_and_duplicate_fail_closed(tmp_path: Path) -> None:
    documents = _legacy_documents(tmp_path)
    fragment = build_shadow_v2_fragment(
        documents[0].rows[0],
        source_commit="a" * 40,
    )
    tampered = copy.deepcopy(fragment)
    tampered["legacy_row_evidence"]["raw_line"] += "tamper"
    with pytest.raises(TaskRegistryShadowError, match="SHADOW_V2_RAW_ROW_HASH"):
        validate_shadow_v2_fragment(tampered)

    baseline = build_s0_baseline(
        project_root=tmp_path,
        handoff=_handoff(),
        documents=documents,
    )
    with pytest.raises(TaskRegistryShadowError, match="SHADOW_V2_DUPLICATE_TASK"):
        build_shadow_v2_index(
            baseline=baseline,
            documents=documents,
            fragments=(fragment, fragment),
            fragment_files=(),
        )


def test_task_register_consumer_inventory_has_explicit_rollback(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src"
    source.mkdir(parents=True)
    (source / "consumer.py").write_text(
        'PATH = "docs/task_register.md"\n',
        encoding="utf-8",
    )

    inventory = characterize_task_register_consumers(tmp_path)

    assert inventory["consumer_count"] == 1
    assert inventory["source_cutover_allowed"] is False
    assert inventory["consumers"][0]["migration_status"] == ("LEGACY_DIRECT_OR_LITERAL_CONSUMER")
    assert inventory["consumers"][0]["rollback"] == "READ_LEGACY_MARKDOWN_DIRECT"


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


def test_repository_s0_s1_artifacts_are_immutable_final_import_evidence() -> None:
    baseline = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_task_registry_baseline.yaml"
    )
    index = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_task_shadow_index.yaml"
    )
    assert isinstance(baseline, dict)
    assert isinstance(index, dict)
    records = index["fragments"]
    fragments = load_shadow_fragments(project_root=PROJECT_ROOT, records=records)
    for fragment in fragments:
        validate_shadow_fragment(fragment)
    assert index["task_count"] == baseline["inventory"]["total_task_count"]
    assert index["missing_task_count"] == 0
    assert index["duplicate_task_count"] == 0
    assert baseline["source_of_truth"]["cutover_performed"] is False

    v2_index = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_task_shadow_v2_index.yaml"
    )
    assert isinstance(v2_index, dict)
    v2_records = v2_index["fragments"]
    v2_fragments = load_shadow_v2_fragments(
        project_root=PROJECT_ROOT,
        records=v2_records,
    )
    for fragment in v2_fragments:
        validate_shadow_v2_fragment(fragment)

    manifest = safe_load_yaml_path(
        PROJECT_ROOT / "inputs/architecture/arch_005_s5_cutover_manifest.yaml"
    )
    assert isinstance(manifest, dict)
    assert manifest["source_of_truth_before"] == "LEGACY_MARKDOWN_ONLY"
    assert manifest["source_of_truth_after"] == "ARCH_005_TASK_REGISTRY"
    assert manifest["shadow_v2"] == {
        "path": "inputs/architecture/arch_005_task_shadow_v2_index.yaml",
        "sha256": _sha256_file(
            PROJECT_ROOT / "inputs/architecture/arch_005_task_shadow_v2_index.yaml"
        ),
        "index_checksum": v2_index["index_checksum"],
        "task_count": v2_index["task_count"],
        "cutover_performed": False,
    }
    assert v2_index["cutover_performed"] is False


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


def _sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_task_rows(
    root: Path,
    *,
    active_rows: tuple[str, ...],
    completed_rows: tuple[str, ...],
) -> None:
    docs = root / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    header = (
        "|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|\n"
        "|---|---|---|---|---|---|---|---|\n"
    )
    (docs / "task_register.md").write_text(
        "# Active\n\n" + header + "".join(active_rows),
        encoding="utf-8",
    )
    (docs / "task_register_completed.md").write_text(
        "# Completed\n\n" + header + "".join(completed_rows),
        encoding="utf-8",
    )


def _handoff() -> dict[str, object]:
    return {
        "schema_version": "arch_005_bootstrap_handoff.v1",
        "handoff_checksum": "b" * 64,
        "head_commit": "a" * 40,
        "next_slice_unblocked": False,
    }
