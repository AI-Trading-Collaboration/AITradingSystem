from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.platform.architecture.parallel_control import (  # type: ignore[import-untyped]
    CHANGE_MANIFEST_SCHEMA_VERSION,
    build_deterministic_lane_plan,
    parse_change_manifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = Path("config/architecture/arch_004_wave12_g4_d0b_readiness.yaml")
EVIDENCE_PATH = Path("inputs/architecture/arch_004_wave12_g4_d0b_parallel_readiness.json")
BASE_COMMIT = "12b1fb86369f146c9ef1c7ac54872eb8150ed791"

EXPECTED_COORDINATOR_PATHS = {
    "config/architecture/arch_004_refactor_policy.yaml",
    "config/data_quality.yaml",
    "config/operations/periodic_control.yaml",
    "config/operations/runtime_control.yaml",
    "config/report_registry.yaml",
    "config/scheduled_tasks.yaml",
    "docs/architecture/dual_lane_development_operating_model.md",
    "docs/artifact_catalog.md",
    "docs/operations/operations_runbook.md",
    "docs/requirements/ARCH-004G4_D0B_Shared_DQ_Preflight_and_Periodic_Consumer_Migration.md",
    "docs/requirements/ARCH-004G_Domain_Migration_and_Subtraction.md",
    "docs/requirements/ARCH-004_Post_2438N_System_Architecture_Refactor_Program.md",
    "docs/requirements/DATA-GOV-001_Unified_Data_Foundation_Governance.md",
    "docs/system_flow.md",
    "docs/task_register.md",
    "docs/task_register_completed.md",
    "inputs/architecture/arch_004_compatibility_baseline.yaml",
    "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
    "inputs/architecture/arch_004e_architecture_fitness.yaml",
    "inputs/architecture/arch_004e_module_manifest.yaml",
    "inputs/architecture/arch_004e_test_manifest.yaml",
    "inputs/architecture/arch_004g_deprecation_inventory.yaml",
    "inputs/architecture/arch_005_task_registry_baseline.yaml",
    "inputs/architecture/arch_005_task_shadow_index.yaml",
    "src/ai_trading_system/cli.py",
    "src/ai_trading_system/cli_commands/data_cache.py",
    "src/ai_trading_system/cli_commands/ops.py",
    "src/ai_trading_system/config.py",
    "src/ai_trading_system/contracts/__init__.py",
    "src/ai_trading_system/contracts/data_quality_execution.py",
    "src/ai_trading_system/contracts/operations.py",
    "src/ai_trading_system/data/__init__.py",
    "src/ai_trading_system/data/quality_execution_discovery.py",
    "src/ai_trading_system/data_refresh_audit.py",
    "src/ai_trading_system/legacy/periodic_operations_adapter.py",
    "src/ai_trading_system/legacy/scheduled_tasks_adapter.py",
    "src/ai_trading_system/ops_daily.py",
    "src/ai_trading_system/platform/operations/__init__.py",
    "src/ai_trading_system/platform/operations/periodic_control.py",
    "src/ai_trading_system/platform/operations/runtime_control.py",
    "src/ai_trading_system/scheduled_tasks.py",
    "src/ai_trading_system/trading_calendar.py",
    "tests/test_arch_004g_deprecation.py",
    "tests/test_arch_004g4_d0b_shared_integration.py",
    "tests/test_data_quality.py",
    "tests/test_data_quality_execution_discovery.py",
    "tests/test_ops_daily.py",
    "tests/test_trading_calendar.py",
}


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)  # type: ignore[no-untyped-call]
        if key in result:
            raise AssertionError(f"duplicate YAML key: {key!r}")
        result[key] = loader.construct_object(  # type: ignore[no-untyped-call]
            value_node, deep=deep
        )
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _mapping(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    assert all(isinstance(key, str) for key in value)
    return value


def _policy() -> dict[str, Any]:
    return _mapping(
        yaml.load(
            (PROJECT_ROOT / POLICY_PATH).read_text(encoding="utf-8"),
            Loader=_UniqueKeyLoader,
        )
    )


def _evidence() -> dict[str, Any]:
    return _mapping(json.loads((PROJECT_ROOT / EVIDENCE_PATH).read_text(encoding="utf-8")))


def _sha256(path: str | Path) -> str:
    return hashlib.sha256((PROJECT_ROOT / path).read_bytes()).hexdigest()


def _canonical_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _private_symbol_references(path: Path, private_symbol: str) -> list[int]:
    content = path.read_text(encoding="utf-8")
    if private_symbol not in content:
        return []
    tree = ast.parse(content, filename=str(path))
    lines: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and any(alias.name == private_symbol for alias in node.names)
        ) or (isinstance(node, ast.Attribute) and node.attr == private_symbol):
            lines.append(node.lineno)
    return sorted(set(lines))


def test_policy_freezes_only_g4a_and_d0b1_on_current_wave_base() -> None:
    policy = _policy()

    assert policy["schema_version"] == "arch_004_wave12_g4_d0b_readiness_policy.v1"
    assert policy["status"] == "SCOPE_FROZEN_NOT_DISPATCHED"
    assert policy["source_phase"] == "G2_5_COMPLETE_G4_D0B_NEXT"
    assert policy["source_base_commit"] == BASE_COMMIT
    assert policy["max_parallel_domain_lanes"] == 2
    assert policy["selected_domain_ids"] == [
        "G4A_PERIODIC_PARITY",
        "D0B1_DQ_EXECUTION_RECEIPT",
    ]
    assert policy["not_selected_domain_ids"] == [
        {"domain_id": "G3_REPORTING_NATIVE", "reason_code": "NOT_SELECTED_BY_WAVE12"},
        {"domain_id": "G5_RESEARCH_WRAPPER", "reason_code": "NOT_SELECTED_BY_WAVE12"},
    ]

    manifests = [parse_change_manifest(row) for row in policy["change_manifests"]]
    domain = [item for item in manifests if item.lane_role.value == "DOMAIN"]
    coordinator = [item for item in manifests if item.lane_role.value == "COORDINATOR"]
    assert {item.change_id for item in domain} == {
        "arch-004-wave12-d0b1-dq-execution-receipt",
        "arch-004-wave12-g4a-periodic-parity",
    }
    assert len(coordinator) == 1
    assert all(item.base_commit == BASE_COMMIT for item in manifests)
    assert all(item.shared_paths == () for item in domain)


def test_shared_contract_and_reviewed_data_quality_policy_hashes_are_bound() -> None:
    bindings = _mapping(_policy()["source_bindings"])
    contract = _mapping(bindings["shared_contract"])
    data_policy = _mapping(bindings["data_quality_policy"])

    assert contract["contract_id"] == "data_quality_execution_receipt.v1"
    assert contract["sha256"] == _sha256(contract["path"])
    assert data_policy == {
        "policy_id": "DATA_QUALITY_CACHE_GATE",
        "policy_version": "data_quality_cache_gate.v1",
        "status": "REVIEWED",
        "path": "config/data_quality.yaml",
        "sha256": _sha256("config/data_quality.yaml"),
    }

    governed = _mapping(
        yaml.load(
            (PROJECT_ROOT / data_policy["path"]).read_text(encoding="utf-8"),
            Loader=_UniqueKeyLoader,
        )
    )["governance"]
    assert governed["policy_id"] == data_policy["policy_id"]
    assert governed["policy_version"] == data_policy["policy_version"]
    assert governed["status"] == "REVIEWED"
    assert governed["role"] == "data_quality"


def test_coordinator_has_the_complete_exclusive_shared_path_lease() -> None:
    policy = _policy()
    coordinator_policy = _mapping(policy["coordinator"])
    coordinator_only = set(coordinator_policy["coordinator_only_paths"])
    coordinator_manifest = next(
        parse_change_manifest(row)
        for row in policy["change_manifests"]
        if row["lane_role"] == "COORDINATOR"
    )

    assert coordinator_only == EXPECTED_COORDINATOR_PATHS
    assert set(coordinator_manifest.shared_paths) == EXPECTED_COORDINATOR_PATHS
    assert coordinator_manifest.change_id == coordinator_policy["change_id"]
    assert set(coordinator_manifest.owned_paths) == {
        "config/architecture/arch_004_wave12_g4_d0b_readiness.yaml",
        "inputs/architecture/arch_004_wave12_g4_d0b_parallel_readiness.json",
        "tests/test_arch_004_wave12_g4_d0b_readiness.py",
        "tests/test_data_quality_execution_contract.py",
    }
    assert not any(
        EXPECTED_COORDINATOR_PATHS.intersection(parse_change_manifest(row).owned_paths)
        for row in policy["change_manifests"]
        if row["lane_role"] == "DOMAIN"
    )


def test_private_preflight_factory_import_whitelist_is_fail_closed() -> None:
    capability = _mapping(_policy()["verified_preflight_capability"])
    private_factory = capability["private_factory"]
    private_seal = capability["private_seal"]
    whitelist = set(capability["direct_import_whitelist"])
    seal_whitelist = set(capability["private_seal_import_whitelist"])

    assert whitelist == {
        "src/ai_trading_system/data/quality_execution.py",
        "tests/test_data_quality_execution_contract.py",
    }
    assert capability["canonical_receipt_path_template"] == (
        "outputs/data_quality/executions/{receipt_id}/receipt.json"
    )
    assert capability["g4_adapter_direct_factory_import_allowed"] is False
    assert capability["g4_adapter_path"] not in whitelist
    assert capability["g4_adapter_required_entrypoint"] == (
        "data_quality_execution.verify_data_quality_execution_receipt"
    )
    assert capability["definition_path_exempt_from_import_scan"] is True
    assert seal_whitelist == set()

    defining_path = capability["defining_path"]
    unauthorized: dict[str, list[int]] = {}
    for root_name in ("src", "tests"):
        for path in (PROJECT_ROOT / root_name).rglob("*.py"):
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            if relative == defining_path:
                continue
            lines = _private_symbol_references(path, private_factory)
            if lines and relative not in whitelist:
                unauthorized[relative] = lines
    assert unauthorized == {}

    seal_importers: dict[str, list[int]] = {}
    for root_name in ("src", "tests"):
        for path in (PROJECT_ROOT / root_name).rglob("*.py"):
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            if relative == defining_path:
                continue
            lines = _private_symbol_references(path, private_seal)
            if lines and relative not in seal_whitelist:
                seal_importers[relative] = lines
    assert seal_importers == {}


def test_tracked_evidence_rebuilds_with_parallel_control_and_is_tamper_evident() -> None:
    policy = _policy()
    evidence = _evidence()
    manifests = [parse_change_manifest(row) for row in evidence["change_manifests"]]
    coordinator_paths = _mapping(policy["coordinator"])["coordinator_only_paths"]
    rebuilt = build_deterministic_lane_plan(
        manifests,
        current_base_commit=BASE_COMMIT,
        coordinator_only_paths=coordinator_paths,
        max_parallel_domain_lanes=2,
    ).to_dict()

    assert evidence["schema_version"] == "arch_004_wave12_g4_d0b_parallel_readiness.v1"
    assert evidence["source_base_commit"] == BASE_COMMIT
    assert evidence["policy_path"] == POLICY_PATH.as_posix()
    assert evidence["policy_sha256"] == _sha256(POLICY_PATH)
    assert evidence["source_bindings"] == policy["source_bindings"]
    assert evidence["change_manifests"] == [
        parse_change_manifest(row).to_dict() for row in policy["change_manifests"]
    ]
    assert all(
        row["schema_version"] == CHANGE_MANIFEST_SCHEMA_VERSION
        for row in evidence["change_manifests"]
    )
    assert evidence["lane_plan"] == rebuilt
    assert rebuilt["status"] == "PASS"
    assert [wave["kind"] for wave in rebuilt["waves"]] == ["DOMAIN", "COORDINATOR"]
    assert len(rebuilt["waves"][0]["assignments"]) == 2
    assert rebuilt["blocking_issues"] == []

    body = {key: value for key, value in evidence.items() if key != "readiness_sha256"}
    assert evidence["readiness_sha256"] == _canonical_sha256(body)


def test_evidence_records_zero_active_shared_leases_and_no_execution_authority() -> None:
    policy = _policy()
    evidence = _evidence()
    attribution = _mapping(evidence["worktree_attribution"])
    assignment = _mapping(evidence["assignment_control"])
    safety = _mapping(evidence["safety"])

    assert attribution == policy["worktree_attribution"]
    assert attribution == {
        "active_shared_path_owner_count": 0,
        "active_shared_path_lease_count": 0,
        "active_shared_path_integration_count": 0,
        "known_unrelated_worktree_files": ["docs/research/growth_tilt_owner_diagnosis_pack.md"],
    }
    assert safety == policy["safety"]
    assert assignment == policy["assignment_control"]
    assert assignment == {
        "worker_assignment_allowed_after_s0_pass": True,
        "assignment_authority": "architecture_coordinator",
        "automatic_command_dispatch": False,
        "lease_acquisition_by_artifact": False,
        "automatic_merge_by_artifact": False,
    }
    assert safety["dispatch_allowed"] is False
    assert safety["automatic_merge_allowed"] is False
    assert safety["lease_acquisition_allowed"] is False
    assert safety["automatic_command_dispatch_enabled"] is False
    assert safety["consumer_cutover_allowed"] is False
    assert safety["production_effect"] == "none"
    assert safety["broker_action"] == "none"
    assert evidence["selected_domain_ids"] == policy["selected_domain_ids"]
    assert evidence["not_selected_domain_ids"] == policy["not_selected_domain_ids"]


def test_frozen_source_base_exists_and_remains_an_ancestor_of_head() -> None:
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{BASE_COMMIT}^{{commit}}"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        shell=False,
    )
    lineage = subprocess.run(
        ["git", "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        shell=False,
    )

    assert exists.returncode == 0
    assert lineage.returncode == 0
