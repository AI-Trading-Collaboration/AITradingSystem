from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.reporting.g3_close_readiness import (
    G3CloseReadinessError,
    build_g3_close_readiness_evidence,
)


def test_live_g3_bounded_slice_close_readiness_is_content_derived_and_safe() -> None:
    first = build_g3_close_readiness_evidence()
    second = build_g3_close_readiness_evidence()

    assert first == second
    assert first.evidence_id == second.evidence_id
    assert first.status == "PASS"
    assert first.bounded_slice_complete is True
    assert first.projected_field_count == 19
    assert (first.native_provider_count, first.generic_provider_count) == (1, 9)
    assert first.legacy_projector_definition_count == 0
    assert first.native_projector_import_count == 1
    assert first.native_projector_call_count == 1
    assert first.reader_brief_sha256 == (
        "b2a089e8b4995f31e982f4a5b0ba9446038e1f617610db8ba04b1b0521b5ba8e"
    )
    assert first.reader_brief_line_count == 29005
    assert first.reader_brief_top_level_function_count == 366
    assert first.historical_f3_raw_sha256 == (
        "1804dcd6392f692c8e24c592f19888219f30f8b11405ec3eb1f3b05b8d918e06"
    )
    assert first.report_fragment_count == 5
    assert first.active_source_of_truth_count == 0
    assert len(first.remaining_generic_providers) == 9
    assert all(
        item.prerequisite_contract.startswith("typed_")
        for item in first.remaining_generic_providers
    )
    assert first.migration_executed is False
    assert first.g5_authorized is False
    assert first.reporting_recompute_allowed is False
    assert first.production_effect == "none"
    assert first.broker_action == "none"
    assert json.dumps(first.to_dict(), ensure_ascii=False, sort_keys=True)


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("reader", "G3_CLOSE_READER_BRIEF_RATCHET_DRIFT"),
        ("historical", "G3_CLOSE_F3_INVENTORY_DRIFT"),
        ("inventory", "G3_CLOSE_REMAINING_INVENTORY_DRIFT"),
        ("migration", "G3_CLOSE_PREMATURE_MIGRATION"),
        ("fragment", "G3_CLOSE_FRAGMENT_RATCHET_DRIFT"),
    ],
)
def test_g3_close_readiness_rejects_ratchet_inventory_and_scope_drift(
    tmp_path: Path,
    case: str,
    expected_code: str,
) -> None:
    root = _copy_inputs(tmp_path)
    policy_path = root / "config/architecture/arch_004_wave15_g3_close_readiness.yaml"

    if case == "reader":
        reader = root / "src/ai_trading_system/reports/reader_brief.py"
        reader.write_text(reader.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    elif case == "historical":
        historical = root / "inputs/architecture/arch_004f3_reporting_inventory.yaml"
        historical.write_text(
            historical.read_text(encoding="utf-8") + "\n# drift\n",
            encoding="utf-8",
        )
    elif case == "inventory":
        reporting = root / "config/reporting/reporting_architecture.yaml"
        payload = yaml.safe_load(reporting.read_text(encoding="utf-8"))
        payload["owner_daily_brief"]["core_sections"][0]["source_keys"].append("drift")
        reporting.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
    elif case == "migration":
        payload = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
        payload["bounded_slice"]["migration_executed"] = True
        policy_path.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
    else:
        fragment = (
            root / "config/architecture/fragments/reports/arch_004g3_reader_brief_native.yaml"
        )
        payload = yaml.safe_load(fragment.read_text(encoding="utf-8"))
        payload["generated_source_of_truth_active"] = True
        fragment.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )

    with pytest.raises(G3CloseReadinessError) as error:
        build_g3_close_readiness_evidence(project_root=root)

    assert error.value.code == expected_code


def test_remaining_inventory_is_exact_reporting_policy_minus_bounded_section() -> None:
    evidence = build_g3_close_readiness_evidence()
    observed = tuple(item.section_id for item in evidence.remaining_generic_providers)

    assert observed == (
        "system_status",
        "today_decision",
        "market_and_score_change",
        "position_and_binding_gates",
        "owner_action_queue",
        "portfolio_and_shadow",
        "research_review_due",
        "operations_health",
        "safety_and_navigation",
    )
    assert "data_quality_and_pit" not in observed
    assert all(
        item.owner == "reporting_governance" for item in evidence.remaining_generic_providers
    )


def _copy_inputs(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    paths = (
        "config/architecture/arch_004_wave15_g3_close_readiness.yaml",
        "config/reporting/reporting_architecture.yaml",
        "inputs/architecture/arch_004f3_reporting_inventory.yaml",
        "src/ai_trading_system/reports/reader_brief.py",
    )
    fragment_root = PROJECT_ROOT / "config/architecture/fragments"
    fragment_paths = tuple(
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in fragment_root.rglob("arch_004g3_reader_brief_native.yaml")
    ) + tuple(
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in (fragment_root / "reports").glob("*.yaml")
        if path.name != "arch_004g3_reader_brief_native.yaml"
    )
    for relative in (*paths, *fragment_paths):
        source = PROJECT_ROOT / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return root
