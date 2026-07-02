from __future__ import annotations

from pathlib import Path

from dynamic_target_source_remediation_fixtures import (
    build_dynamic_target_source_remediation_fixture,
    read_json,
)

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    run_dynamic_target_baseline_source_remediation,
)


def test_wrapper_generated_when_source_remediable(tmp_path: Path) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)
    output_dir = tmp_path / "out"

    run_dynamic_target_baseline_source_remediation(
        dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        candidate_artifact_roots=str(fixture["candidate_root"]),
        output_dir=output_dir,
        docs_root=tmp_path / "docs",
    )

    wrapper = read_json(output_dir / "dynamic_target_baseline_wrapper_artifact.json")
    rows = wrapper["rows"]
    assert rows
    assert rows[0]["baseline_id"]
    assert rows[0]["source_id"]
    assert rows[0]["source_artifact_hash"]
    assert rows[0]["as_of_timestamp"]
    assert rows[0]["decision_timestamp"]
    assert rows[0]["valid_from"]
    assert rows[0]["valid_until"]
    assert rows[0]["promotion_allowed"] is False
    assert rows[0]["broker_action"] == "none"
    assert rows[0]["target_exposure_role"] == (
        "research_baseline_field_only_not_trading_instruction"
    )
