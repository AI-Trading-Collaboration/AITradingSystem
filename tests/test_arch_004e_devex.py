from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    ScaffoldKind,
    build_aggregate_shadow_index,
    build_architecture_fitness,
    build_module_manifest,
    build_test_manifest,
    create_scaffold,
    select_impacted_tests,
    write_generated_architecture_artifact,
)
from ai_trading_system.platform.architecture import (
    devex as devex_module,
)
from ai_trading_system.reports.engineering_closeout import (
    build_engineering_surface_inventory_payload,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import build_command

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/devex_ownership_policy.yaml"
MODULE_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_module_manifest.yaml"
TEST_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_test_manifest.yaml"
AGGREGATE_INDEX_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_aggregate_shadow_index.yaml"
DEPENDENCY_POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_004c_dependency_policy.yaml"
DIRECT_WRITER_BASELINE_PATH = (
    PROJECT_ROOT / "inputs/architecture/arch_004c_direct_writer_baseline.yaml"
)


@pytest.mark.parametrize("suffix", [".md", ".py", ".toml", ".yaml", ".yml"])
def test_devex_sha256_uses_canonical_lf_for_repository_text(
    tmp_path: Path, suffix: str
) -> None:
    lf_payload = b"alpha\nbeta\ngamma\n"
    crlf_payload = b"alpha\r\nbeta\r\ngamma\r\n"
    mixed_payload = b"alpha\r\nbeta\ngamma\r\n"
    expected = hashlib.sha256(lf_payload).hexdigest()

    paths = [tmp_path / f"source-{index}{suffix}" for index in range(3)]
    for path, payload in zip(
        paths, (lf_payload, crlf_payload, mixed_payload), strict=True
    ):
        path.write_bytes(payload)

    assert {devex_module._sha256(path) for path in paths} == {expected}


def test_devex_sha256_preserves_binary_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"\x00alpha\r\nbeta\x00")
    second.write_bytes(b"\x00alpha\nbeta\x00")

    assert devex_module._sha256(first) != devex_module._sha256(second)


def test_generated_module_and_test_manifests_cover_every_python_file_once() -> None:
    module_manifest = build_module_manifest(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    test_manifest = build_test_manifest(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    source_paths = {
        path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
        for path in (PROJECT_ROOT / "src/ai_trading_system").rglob("*.py")
    }
    test_paths = {
        path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
        for path in (PROJECT_ROOT / "tests").rglob("*.py")
    }

    assert module_manifest["status"] == "PASS"
    assert module_manifest["module_count"] == len(source_paths)
    assert {row["path"] for row in module_manifest["modules"]} == source_paths
    assert test_manifest["status"] == "PASS"
    assert test_manifest["test_file_count"] == len(test_paths)
    assert {row["path"] for row in test_manifest["tests"]} == test_paths
    assert module_manifest["orphan_count"] == 0
    assert module_manifest["specific_overlap_count"] == 0
    assert test_manifest["orphan_count"] == 0
    assert test_manifest["specific_overlap_count"] == 0
    required_owner_fields = {
        "code_owner",
        "policy_owner",
        "data_owner",
        "artifact_owner",
        "runtime_owner",
    }
    assert all(set(row["owners"]) == required_owner_fields for row in module_manifest["modules"])
    assert all(row["category"] and row["suite"] for row in test_manifest["tests"])


def test_impact_selection_routes_source_shared_and_unknown_paths_fail_closed() -> None:
    module_manifest = _mapping(safe_load_yaml_path(MODULE_MANIFEST_PATH))
    test_manifest = _mapping(safe_load_yaml_path(TEST_MANIFEST_PATH))

    source = select_impacted_tests(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest=module_manifest,
        test_manifest=test_manifest,
        changed_paths=["src/ai_trading_system/research_framework/runner.py"],
    )
    assert source.owner_profiles == ("research",)
    assert "tests/test_arch_004d_reference_vertical_slice.py" in source.focused_tests
    assert "architecture-fitness" in source.validation_tiers
    assert source.full_validation_required is True

    shared = select_impacted_tests(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest=module_manifest,
        test_manifest=test_manifest,
        changed_paths=["docs/system_flow.md"],
    )
    assert shared.integration_coordinator_required is True
    assert "full" in shared.validation_tiers
    assert "tests/test_documentation_contract.py" in shared.focused_tests

    unknown = select_impacted_tests(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest=module_manifest,
        test_manifest=test_manifest,
        changed_paths=["new_top_level_surface.xyz"],
    )
    assert unknown.unresolved_paths == ("new_top_level_surface.xyz",)
    assert unknown.integration_coordinator_required is True
    assert "full" in unknown.validation_tiers


def test_aggregate_shadow_index_is_deterministic_and_keeps_current_sources() -> None:
    before = {
        path: path.read_bytes()
        for path in (
            PROJECT_ROOT / "config/report_registry.yaml",
            PROJECT_ROOT / "docs/artifact_catalog.md",
            PROJECT_ROOT / "docs/system_flow.md",
        )
    }

    first = build_aggregate_shadow_index(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    second = build_aggregate_shadow_index(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)

    assert first == second
    assert first["status"] == "SHADOW_COMPATIBILITY_PASS"
    assert first["target_count"] == 3
    assert first["fragment_count"] >= 4
    assert first["existing_aggregate_source_of_truth_changed"] is False
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize("kind", list(ScaffoldKind))
def test_scaffold_writes_only_kind_specific_files_and_fails_if_existing(
    tmp_path: Path, kind: ScaffoldKind
) -> None:
    result = create_scaffold(
        project_root=tmp_path,
        kind=kind,
        identifier=f"sample_{kind.value}",
        owner="test_owner",
    )

    assert all(path.exists() for path in result.paths)
    assert not (tmp_path / "src/ai_trading_system/cli.py").exists()
    assert not (tmp_path / "config/report_registry.yaml").exists()
    assert not (tmp_path / "docs/artifact_catalog.md").exists()
    with pytest.raises(FileExistsError, match="already exists"):
        create_scaffold(
            project_root=tmp_path,
            kind=kind,
            identifier=f"sample_{kind.value}",
            owner="test_owner",
        )


def test_architecture_fitness_passes_and_detects_stale_manifest(tmp_path: Path) -> None:
    fitness = _fitness(MODULE_MANIFEST_PATH)

    assert fitness["status"] == "PASS"
    assert fitness["violation_count"] == 0
    assert fitness["module_orphan_count"] == 0
    assert fitness["test_orphan_count"] == 0
    assert fitness["dependency_gate"]["status"] == "PASS"

    stale = _mapping(safe_load_yaml_path(MODULE_MANIFEST_PATH))
    stale["module_count"] = 0
    stale_path = tmp_path / "stale_module_manifest.yaml"
    write_generated_architecture_artifact(stale_path, stale)
    failed = _fitness(stale_path)
    assert failed["status"] == "FAIL"
    assert any(item["rule_id"] == "module_manifest_fresh" for item in failed["violations"])


def test_architecture_validation_tier_is_generated_from_test_manifest() -> None:
    command = build_command(
        "architecture-fitness",
        python_executable="python",
        repo_root=PROJECT_ROOT,
    )
    joined = " ".join(command).replace("\\", "/")

    assert "tests/test_arch_004e_devex.py" in joined
    assert "tests/test_arch_004_refactor_policy.py" in joined
    assert "tests/test_documentation_contract.py" in joined
    assert "-n 16 --dist loadfile" in joined


def test_engineering_surface_inventory_links_generated_devex_control_plane() -> None:
    payload = build_engineering_surface_inventory_payload(
        as_of=date(2026, 7, 11),
        project_root=PROJECT_ROOT,
    )

    assert (
        payload["summary"]["owned_module_count"]
        == payload["generated_devex_control_plane"]["module_manifest"]["module_count"]
    )
    assert (
        payload["summary"]["classified_test_file_count"]
        == payload["generated_devex_control_plane"]["test_manifest"]["test_file_count"]
    )
    assert payload["summary"]["architecture_fitness_status"] == "PASS"
    assert payload["methodology"]["generated_manifests_linked_when_available"] is True


def _fitness(module_manifest_path: Path) -> dict[str, object]:
    return build_architecture_fitness(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest_path=module_manifest_path,
        test_manifest_path=TEST_MANIFEST_PATH,
        aggregate_index_path=AGGREGATE_INDEX_PATH,
        dependency_policy_path=DEPENDENCY_POLICY_PATH,
        direct_writer_baseline_path=DIRECT_WRITER_BASELINE_PATH,
    )


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value
