from __future__ import annotations

from pathlib import Path

import pytest

from scripts import architecture_devex as architecture_devex_script


def test_architecture_devex_generate_binds_deprecation_scan_to_active_worktree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    sentinel_policy = object()

    def load_policy(path: Path) -> object:
        observed["policy_path"] = path
        return sentinel_policy

    class Inventory:
        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "test.deprecation_inventory.v1"}

    def scan_inventory(
        policy: object,
        *,
        project_root: Path,
        architecture_fitness_path: Path,
    ) -> Inventory:
        observed["policy"] = policy
        observed["project_root"] = project_root
        observed["fitness_path"] = architecture_fitness_path
        return Inventory()

    monkeypatch.setattr(architecture_devex_script, "build_module_manifest", lambda **_: {})
    monkeypatch.setattr(architecture_devex_script, "build_test_manifest", lambda **_: {})
    monkeypatch.setattr(architecture_devex_script, "build_aggregate_shadow_index", lambda **_: {})
    monkeypatch.setattr(
        architecture_devex_script,
        "write_generated_architecture_artifact",
        lambda *_: None,
    )
    monkeypatch.setattr(architecture_devex_script, "_fitness", lambda: {"status": "PASS"})
    monkeypatch.setattr(architecture_devex_script, "load_deprecation_policy", load_policy)
    monkeypatch.setattr(architecture_devex_script, "scan_deprecation_inventory", scan_inventory)

    assert architecture_devex_script._generate() == 0
    assert observed == {
        "policy_path": architecture_devex_script.DEPRECATION_POLICY_PATH,
        "policy": sentinel_policy,
        "project_root": architecture_devex_script.PROJECT_ROOT,
        "fitness_path": architecture_devex_script.FITNESS_PATH,
    }
