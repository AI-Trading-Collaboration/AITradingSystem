from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    DynamicV3ConfirmationOperationsError,
    build_confirmation_cycle_plan,
    confirmation_cycle_plan_report_payload,
    validate_confirmation_cycle_plan_artifact,
    validate_confirmation_cycle_schedule_config,
)


def test_confirmation_cycle_plan_generates_safe_command_pack(tmp_path: Path) -> None:
    config_path = tmp_path / "confirmation_cycle_schedule_v1.yaml"
    config_path.write_text(
        DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
    assert validation["status"] == "PASS"

    result = build_confirmation_cycle_plan(
        config_path=config_path,
        output_dir=tmp_path / "confirmation_cycle_plan",
    )
    pack = result["scheduled_command_pack"]
    commands = {row["step"]: row for row in pack["commands"]}

    assert result["manifest"]["status"] == "PASS"
    assert "outcome_due_scan" in commands
    assert "outcome_update_if_ready" in commands
    assert commands["outcome_update_if_ready"]["execution_mode"] == "explicit_update_only"
    assert pack["safety"]["broker_action_allowed"] is False
    assert pack["safety"]["production_effect"] == "none"
    assert pack["safety"]["auto_apply_policy"] is False

    payload = confirmation_cycle_plan_report_payload(
        plan_id=result["plan_id"],
        output_dir=tmp_path / "confirmation_cycle_plan",
    )
    assert payload["plan_id"] == result["plan_id"]
    assert "confirmation_cycle_runbook.md" in payload["confirmation_cycle_runbook_path"]
    assert (
        validate_confirmation_cycle_plan_artifact(
            plan_id=result["plan_id"], output_dir=tmp_path / "confirmation_cycle_plan"
        )["status"]
        == "PASS"
    )

    manifest_path = Path(result["plan_dir"]) / "confirmation_cycle_plan_manifest.json"
    original_manifest = manifest_path.read_text(encoding="utf-8")
    tampered_manifest = json.loads(original_manifest)
    tampered_manifest["confirmation_cycle_plan_report_path"] = "wrong-path.md"
    manifest_path.write_text(
        json.dumps(tampered_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        validate_confirmation_cycle_plan_artifact(
            plan_id=result["plan_id"], output_dir=tmp_path / "confirmation_cycle_plan"
        )["status"]
        == "FAIL"
    )
    manifest_path.write_text(original_manifest, encoding="utf-8")

    config_path.write_text(config_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert (
        validate_confirmation_cycle_plan_artifact(
            plan_id=result["plan_id"], output_dir=tmp_path / "confirmation_cycle_plan"
        )["status"]
        == "FAIL"
    )

    naive_output = tmp_path / "naive_plan"
    with pytest.raises(DynamicV3ConfirmationOperationsError, match="timezone-aware"):
        build_confirmation_cycle_plan(
            config_path=config_path,
            output_dir=naive_output,
            generated_at=datetime(2026, 7, 31),
        )
    assert not naive_output.exists()

    (Path(result["plan_dir"]) / "confirmation_cycle_plan_input_snapshot.json").unlink()
    legacy_payload = confirmation_cycle_plan_report_payload(
        plan_id=result["plan_id"], output_dir=tmp_path / "confirmation_cycle_plan"
    )
    assert legacy_payload["status"] == "PASS_WITH_WARNINGS"
    assert legacy_payload["legacy_unsnapshotted"] is True
    assert legacy_payload["current_conclusion_eligible"] is False
