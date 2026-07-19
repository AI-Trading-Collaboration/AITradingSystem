from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    BOOTSTRAP_HANDOFF_SCHEMA_VERSION,
    BootstrapHandoffError,
    bootstrap_handoff_checksum,
    build_bootstrap_handoff,
    validate_bootstrap_handoff,
    write_generated_architecture_artifact,
)

HEAD = "a" * 40
BASE = "b" * 40
BRANCH = "codex/arch004g2-eb0-s3a-tail-optimization"


def test_bootstrap_handoff_builds_complete_fail_closed_contract(tmp_path: Path) -> None:
    artifacts = _frozen_project(tmp_path)

    payload = build_bootstrap_handoff(
        project_root=tmp_path,
        head_commit=HEAD,
        base_commit=BASE,
        branch=BRANCH,
        validation_artifacts=artifacts,
        known_unrelated_worktree_files=[
            "docs/task_register.md",
            "src/ai_trading_system/data/market_data.py",
        ],
        generated_at=datetime(2026, 7, 19, 18, 30, tzinfo=UTC),
    )

    assert payload["schema_version"] == BOOTSTRAP_HANDOFF_SCHEMA_VERSION
    assert payload["migration_matrix"]["migrated_callback_count"] == 967
    assert payload["migration_matrix"]["pending_callback_count"] == 0
    assert set(payload["validation_artifacts"]) == {
        "focused",
        "architecture_fitness",
        "contract_validation",
        "full_validation",
    }
    assert payload["shared_path_activity"] == {
        "status": "PASS",
        "active_shared_path_owner_count": 0,
        "active_shared_path_lease_count": 0,
        "active_shared_path_integration_count": 0,
        "lease_registry_present": False,
        "evidence": (
            "ARCH-005-PB1 is non-cutover and fixes lease_acquisition_allowed=false; "
            "no canonical task/lease/integration registry exists before S0/S2."
        ),
    }
    assert payload["next_slice_unblocked"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["handoff_checksum"] == bootstrap_handoff_checksum(payload)

    validate_bootstrap_handoff(
        payload,
        project_root=tmp_path,
        expected_head_commit=HEAD,
        expected_branch=BRANCH,
    )


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda payload: payload.__setitem__("next_slice_unblocked", True),
            "HANDOFF_NEXT_SLICE_UNSAFE",
        ),
        (
            lambda payload: payload.__setitem__("push_status", "LOCAL_ONLY"),
            "HANDOFF_NOT_PUSHED",
        ),
        (
            lambda payload: payload["shared_path_activity"].__setitem__(
                "active_shared_path_lease_count", 1
            ),
            "HANDOFF_SHARED_ACTIVITY_ACTIVE",
        ),
        (
            lambda payload: payload["migration_matrix"].__setitem__(
                "pending_callback_count", 1
            ),
            "HANDOFF_MATRIX_INCOMPLETE",
        ),
    ],
)
def test_bootstrap_handoff_rejects_unsafe_or_incomplete_state(
    tmp_path: Path,
    mutation: object,
    code: str,
) -> None:
    payload = _payload(tmp_path)
    assert callable(mutation)
    mutation(payload)
    payload["handoff_checksum"] = bootstrap_handoff_checksum(payload)

    with pytest.raises(BootstrapHandoffError, match=code):
        validate_bootstrap_handoff(payload, project_root=tmp_path)


def test_bootstrap_handoff_rejects_artifact_hash_drift(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    artifact = tmp_path / payload["validation_artifacts"]["focused"]["artifact_path"]
    artifact.write_text('{"status":"FAIL","exit_code":1}', encoding="utf-8")

    with pytest.raises(BootstrapHandoffError, match="HANDOFF_FILE_HASH_DRIFT"):
        validate_bootstrap_handoff(payload, project_root=tmp_path)


def test_bootstrap_handoff_rejects_payload_checksum_drift(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["branch"] = "codex/drifted"

    with pytest.raises(BootstrapHandoffError, match="HANDOFF_CHECKSUM_DRIFT"):
        validate_bootstrap_handoff(payload, project_root=tmp_path)


def test_bootstrap_handoff_rejects_expected_source_drift(tmp_path: Path) -> None:
    payload = _payload(tmp_path)

    with pytest.raises(BootstrapHandoffError, match="HANDOFF_HEAD_DRIFT"):
        validate_bootstrap_handoff(
            payload,
            project_root=tmp_path,
            expected_head_commit="c" * 40,
        )


def _payload(root: Path) -> dict[str, object]:
    artifacts = _frozen_project(root)
    return copy.deepcopy(
        build_bootstrap_handoff(
            project_root=root,
            head_commit=HEAD,
            base_commit=BASE,
            branch=BRANCH,
            validation_artifacts=artifacts,
            known_unrelated_worktree_files=[],
            generated_at=datetime(2026, 7, 19, 18, 30, tzinfo=UTC),
        )
    )


def _frozen_project(root: Path) -> dict[str, Path]:
    architecture = root / "inputs/architecture"
    architecture.mkdir(parents=True, exist_ok=True)
    write_generated_architecture_artifact(
        architecture / "arch_004g2_callback_migration_matrix.yaml",
        {
            "summary": {
                "baseline_callback_count": 967,
                "migrated_callback_count": 967,
                "pending_callback_count": 0,
                "unresolved_callback_count": 0,
                "duplicate_callback_count": 0,
                "phase_exit_ready": True,
            }
        },
    )
    write_generated_architecture_artifact(
        architecture / "arch_004e_module_manifest.yaml",
        {"status": "PASS", "orphan_count": 0},
    )
    write_generated_architecture_artifact(
        architecture / "arch_004e_test_manifest.yaml",
        {"status": "PASS", "orphan_count": 0},
    )
    write_generated_architecture_artifact(
        architecture / "arch_004_compatibility_baseline.yaml",
        {"phase_g2_4_phase_exit": {"status": "PASS"}},
    )
    write_generated_architecture_artifact(
        architecture / "arch_004g_deprecation_inventory.yaml",
        {"inventory_id": "arch_004g_deprecation_inventory_fixture"},
    )
    write_generated_architecture_artifact(
        architecture / "arch_004_worktree_attribution.yaml",
        {"status": "fixture"},
    )
    artifacts: dict[str, Path] = {}
    for tier in (
        "focused",
        "architecture_fitness",
        "contract_validation",
        "full_validation",
    ):
        path = root / "outputs/validation_runtime" / tier / "test_runtime_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"status": "PASS", "exit_code": 0, "tier": tier}),
            encoding="utf-8",
        )
        artifacts[tier] = path
    return artifacts
