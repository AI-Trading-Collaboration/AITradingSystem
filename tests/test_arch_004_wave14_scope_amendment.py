from __future__ import annotations

import subprocess

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

AMENDMENT_PATH = PROJECT_ROOT / "config/architecture/arch_004_wave14_d0b2_g3_scope_amendment.yaml"
READINESS_PATH = PROJECT_ROOT / "config/architecture/arch_004_wave14_d0b2_g3_readiness.yaml"
SOURCE_CARRIER = "39a3ea7306a3937beda835020df4d8419c1cbbdf"


def test_wave14_scope_amendment_preserves_carrier_and_adds_exact_direct_paths() -> None:
    amendment = safe_load_yaml_path(AMENDMENT_PATH)
    readiness = safe_load_yaml_path(READINESS_PATH)

    assert amendment["schema_version"] == "architecture_wave_scope_amendment.v1"
    assert amendment["amendment_id"] == "ARCH-004-WAVE14-A1-DIRECT-DQ-PROFILE"
    assert amendment["status"] == "APPROVED_IMPLEMENTED_PENDING_FORMAL_EXIT"
    assert amendment["source_carrier"]["commit"] == SOURCE_CARRIER
    assert amendment["source_carrier"]["mutation_allowed"] is False
    assert amendment["scope"] == {
        "lane_role": "COORDINATOR",
        "exact_added_paths": [
            "src/ai_trading_system/cli_direct.py",
            "tests/test_cli_direct.py",
        ],
        "governance_artifact_paths": [
            "config/architecture/arch_004_wave14_d0b2_g3_scope_amendment.yaml",
            "tests/test_arch_004_wave14_scope_amendment.py",
        ],
        "domain_owned_paths_changed": False,
        "shared_writer_count": 1,
    }
    assert amendment["behavior"] == {
        "explicit_profile_forwarded_unchanged": True,
        "missing_profile_forwarded_as": "auto",
        "explicit_as_of_without_profile_resolves_to": "manual.v1",
        "daily_default_shape_revalidated_by_data_cache_cli": True,
        "implicit_retry_allowed": False,
    }
    assert amendment["safety"]["current_change_production_effect"] == "none"
    assert amendment["safety"]["consumer_cutover_allowed"] is False
    assert amendment["safety"]["task_source_cutover_allowed"] is False
    assert amendment["safety"]["order_or_broker_action"] == "none"

    original_coordinator_paths = set(readiness["coordinator_only_paths"])
    assert not set(amendment["scope"]["exact_added_paths"]) & original_coordinator_paths


def test_wave14_scope_amendment_binds_exact_historical_carrier_blobs() -> None:
    amendment = safe_load_yaml_path(AMENDMENT_PATH)

    for name in ("policy", "evidence"):
        binding = amendment["source_carrier"][name]
        observed = subprocess.run(
            [
                "git",
                "rev-parse",
                f"{SOURCE_CARRIER}:{binding['path']}",
            ],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert observed == binding["git_blob_sha1"]


def test_wave14_scope_amendment_is_linked_from_requirement_and_register() -> None:
    requirement = (
        PROJECT_ROOT / "docs/requirements/ARCH-004_Wave14_D0B2_G3_Parallel_Readiness.md"
    ).read_text(encoding="utf-8")
    register = (PROJECT_ROOT / "docs/task_register.md").read_text(encoding="utf-8")

    artifact = "config/architecture/arch_004_wave14_d0b2_g3_scope_amendment.yaml"
    amendment_id = "ARCH-004-WAVE14-A1-DIRECT-DQ-PROFILE"
    assert artifact in requirement
    assert amendment_id in register
