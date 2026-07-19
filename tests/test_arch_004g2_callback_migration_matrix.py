from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    CALLBACK_MIGRATION_SCHEMA_VERSION,
    CallbackMigrationError,
    assert_frozen_callback_migration_matrix,
    baseline_callbacks_from_matrix,
    build_callback_migration_matrix,
    scan_callback_source,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = PROJECT_ROOT / "inputs/architecture/arch_004g2_callback_migration_matrix.yaml"


def test_callback_scanner_supports_explicit_and_inferred_names() -> None:
    rows = scan_callback_source(
        '@alpha_app.command("run")\ndef run_command():\n    pass\n\n'
        "@beta_app.command()\ndef report_command():\n    pass\n",
        source_path="fixture.py",
    )

    assert [(row.app_name, row.command_name) for row in rows] == [
        ("alpha_app", "run"),
        ("beta_app", "report-command"),
    ]


def test_callback_scanner_rejects_duplicate_app_command_identity() -> None:
    with pytest.raises(CallbackMigrationError, match="CALLBACK_MIGRATION_DUPLICATE"):
        scan_callback_source(
            '@alpha_app.command("run")\ndef first():\n    pass\n\n'
            '@alpha_app.command("run")\ndef second():\n    pass\n',
            source_path="fixture.py",
        )


def test_tracked_g2_4_callback_matrix_is_current_and_complete_as_inventory() -> None:
    tracked = safe_load_yaml_path(MATRIX_PATH)
    baseline = tracked["baseline"]
    actual = build_callback_migration_matrix(
        baseline_callbacks=baseline_callbacks_from_matrix(MATRIX_PATH),
        baseline_source_commit=baseline["commit"],
        baseline_source_path=baseline["path"],
        baseline_source_sha256=baseline["sha256"],
        project_root=PROJECT_ROOT,
    )

    assert_frozen_callback_migration_matrix(actual, baseline_path=MATRIX_PATH)
    assert actual["schema_version"] == CALLBACK_MIGRATION_SCHEMA_VERSION
    assert actual["status"] == "PASS"
    assert actual["source_phase"] == "ARCH-004G2.4"
    summary = actual["summary"]
    assert summary["baseline_callback_count"] == 967
    assert summary["migrated_callback_count"] == 931
    assert summary["pending_callback_count"] == 36
    assert summary["unresolved_callback_count"] == 0
    assert summary["duplicate_callback_count"] == 0
    assert summary["pre_g2_4_canonical_callback_count"] == 26
    assert summary["current_total_callback_count"] == 993
    assert summary["phase_exit_ready"] is False
    assert summary["phase_completion_status"] == "IN_PROGRESS"
    assert len(actual["callbacks"]) == 967
    assert actual["production_effect"] == "none"
    assert actual["broker_action"] == "none"
