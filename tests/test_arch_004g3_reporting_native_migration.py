from __future__ import annotations

import ast
import copy
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.contracts.report_spec import (
    ReaderTier,
    ReportSectionSpec,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import EntrypointRef
from ai_trading_system.platform.architecture.devex import (
    build_aggregate_shadow_index,
)
from ai_trading_system.platform.architecture.wave_readiness import (
    load_strict_yaml_path,
)
from ai_trading_system.platform.reporting.owner_daily import (
    _provide_legacy_payload_section,
)
from ai_trading_system.platform.reporting.reader_brief_native import (
    DATA_QUALITY_AND_PIT_SECTION_ID,
    DATA_QUALITY_AND_PIT_SOURCE_KEYS,
    project_data_quality_pit_safety,
    provide_data_quality_and_pit_section,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NATIVE_MODULE_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/platform/reporting/reader_brief_native.py"
)
READER_BRIEF_MODULE_PATH = PROJECT_ROOT / "src/ai_trading_system/reports/reader_brief.py"
OWNERSHIP_POLICY_PATH = PROJECT_ROOT / "config/architecture/devex_ownership_policy.yaml"
FRAGMENT_PATHS = (
    PROJECT_ROOT / "config/architecture/fragments/artifacts/arch_004g3_reader_brief_native.yaml",
    PROJECT_ROOT / "config/architecture/fragments/flows/arch_004g3_reader_brief_native.yaml",
    PROJECT_ROOT / "config/architecture/fragments/reports/arch_004g3_reader_brief_native.yaml",
)
PROJECTED_FIELD_ORDER = (
    "as_of_date",
    "decision_snapshot_id",
    "data_gate_status",
    "market_data_status",
    "market_data_latest_date",
    "market_data_error_count",
    "market_data_warning_count",
    "feature_status",
    "sec_feature_status",
    "sec_data_latest_filing",
    "fmp_valuation_snapshot_timestamp",
    "future_data_check",
    "carried_forward_fields",
    "stale_fields",
    "blocking_reasons",
    "stale_report_count",
    "missing_report_count",
    "pit_visibility_note",
    "production_effect",
)
ProjectorInputs = tuple[
    date,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]
ProjectorInputsFactory = Callable[[], ProjectorInputs]


def _pass_projector_inputs() -> ProjectorInputs:
    return (
        date(2026, 7, 23),
        {
            "signal_date": "2026-07-22",
            "snapshot_id": "decision_snapshot_20260722",
            "quality": {
                "market_data_status": "PASS",
                "market_data_latest_date": "2026-07-22",
                "market_data_error_count": 0,
                "market_data_warning_count": 1,
                "feature_status": "PASS",
                "sec_feature_status": "PASS",
                "sec_data_latest_filing": "2026-07-18",
                "fmp_valuation_snapshot_timestamp": "2026-07-22T21:00:00Z",
                "carried_forward_fields": ["macro_policy_rate"],
                "stale_fields": [],
            },
        },
        {
            "data_gate": {
                "status": "PASS_WITH_WARNINGS；one carried-forward field",
                "blocking_reasons": [],
            }
        },
        {"stale_count": 0, "missing_count": 0},
    )


def _blocked_projector_inputs() -> ProjectorInputs:
    return (
        date(2026, 7, 23),
        {
            "snapshot_id": "",
            "quality": {
                "latest_market_data_date": "2026-07-18",
                "latest_sec_filing": "2026-06-30",
                "latest_fmp_valuation_timestamp": "2026-07-18T21:00:00Z",
                "carried_forward_fields": [None, "", "sec_filing"],
                "stale_fields": ["market_prices"],
            },
        },
        {
            "data_gate": {
                "status": "BLOCKED; requested-window gap",
                "blocking_reasons": ["INTERNAL_GAP"],
            }
        },
        {"stale_count": 2, "missing_count": 3},
    )


def _pass_projection_golden() -> dict[str, Any]:
    return {
        "as_of_date": "2026-07-22",
        "decision_snapshot_id": "decision_snapshot_20260722",
        "data_gate_status": "PASS_WITH_WARNINGS；one carried-forward field",
        "market_data_status": "PASS",
        "market_data_latest_date": "2026-07-22",
        "market_data_error_count": "0",
        "market_data_warning_count": "1",
        "feature_status": "PASS",
        "sec_feature_status": "PASS",
        "sec_data_latest_filing": "2026-07-18",
        "fmp_valuation_snapshot_timestamp": "2026-07-22T21:00:00Z",
        "future_data_check": "PASS",
        "carried_forward_fields": ["macro_policy_rate"],
        "stale_fields": [],
        "blocking_reasons": [],
        "stale_report_count": 0,
        "missing_report_count": 0,
        "pit_visibility_note": (
            "UNKNOWN_IN_SNAPSHOT 表示该源的可见时间未在当前 decision snapshot 明确披露；"
            "不得据此补造 PIT 结论。"
        ),
        "production_effect": "none",
    }


def _blocked_projection_golden() -> dict[str, Any]:
    return {
        "as_of_date": "2026-07-23",
        "decision_snapshot_id": "UNKNOWN",
        "data_gate_status": "BLOCKED; requested-window gap",
        "market_data_status": "UNKNOWN",
        "market_data_latest_date": "2026-07-18",
        "market_data_error_count": "UNKNOWN",
        "market_data_warning_count": "UNKNOWN",
        "feature_status": "UNKNOWN",
        "sec_feature_status": "UNKNOWN",
        "sec_data_latest_filing": "2026-06-30",
        "fmp_valuation_snapshot_timestamp": "2026-07-18T21:00:00Z",
        "future_data_check": "REVIEW_REQUIRED",
        "carried_forward_fields": ["sec_filing"],
        "stale_fields": ["market_prices"],
        "blocking_reasons": ["INTERNAL_GAP"],
        "stale_report_count": 2,
        "missing_report_count": 3,
        "pit_visibility_note": (
            "UNKNOWN_IN_SNAPSHOT 表示该源的可见时间未在当前 decision snapshot 明确披露；"
            "不得据此补造 PIT 结论。"
        ),
        "production_effect": "none",
    }


def _section_spec() -> ReportSectionSpec:
    return ReportSectionSpec(
        section_id=DATA_QUALITY_AND_PIT_SECTION_ID,
        title="Data Quality And Pit",
        owner="reporting_governance",
        reader_tier=ReaderTier.OWNER_DAILY_BRIEF,
        provider=EntrypointRef(
            module=("ai_trading_system.platform.reporting.reader_brief_native"),
            callable_name="provide_data_quality_and_pit_section",
        ),
        provider_version="1.0.0",
        source_keys=DATA_QUALITY_AND_PIT_SOURCE_KEYS,
        core_order=5,
    )


@pytest.mark.parametrize(
    ("inputs_factory", "golden_factory"),
    [
        (_pass_projector_inputs, _pass_projection_golden),
        (_blocked_projector_inputs, _blocked_projection_golden),
    ],
)
def test_native_projector_matches_lane_base_legacy_golden(
    inputs_factory: ProjectorInputsFactory,
    golden_factory: Callable[[], dict[str, Any]],
) -> None:
    as_of, snapshot, daily_summary, report_summary = inputs_factory()
    before = copy.deepcopy((snapshot, daily_summary, report_summary))

    native = project_data_quality_pit_safety(
        as_of=as_of,
        snapshot=snapshot,
        daily_decision_summary=daily_summary,
        report_index_summary=report_summary,
    )
    golden = golden_factory()

    # Frozen from lane-base C's legacy builder so this characterization survives
    # the coordinator-owned deletion of that private function during S2 cut-in.
    assert native == golden
    assert tuple(native) == PROJECTED_FIELD_ORDER
    assert len(native) == 19
    assert json.dumps(native, ensure_ascii=False, separators=(",", ":")) == json.dumps(
        golden, ensure_ascii=False, separators=(",", ":")
    )
    assert (snapshot, daily_summary, report_summary) == before


def test_native_projector_is_byte_deterministic_and_preserves_missing_values() -> None:
    as_of, snapshot, daily_summary, report_summary = _blocked_projector_inputs()

    first = project_data_quality_pit_safety(
        as_of=as_of,
        snapshot=snapshot,
        daily_decision_summary=daily_summary,
        report_index_summary=report_summary,
    )
    second = project_data_quality_pit_safety(
        as_of=as_of,
        snapshot=copy.deepcopy(snapshot),
        daily_decision_summary=copy.deepcopy(daily_summary),
        report_index_summary=copy.deepcopy(report_summary),
    )

    assert json.dumps(first, ensure_ascii=False) == json.dumps(second, ensure_ascii=False)
    assert first["decision_snapshot_id"] == "UNKNOWN"
    assert first["market_data_status"] == "UNKNOWN"
    assert first["feature_status"] == "UNKNOWN"
    assert first["future_data_check"] == "REVIEW_REQUIRED"
    assert first["blocking_reasons"] == ["INTERNAL_GAP"]
    assert first["production_effect"] == "none"


def test_native_typed_provider_preserves_pass_and_exposes_dq_gate_fact() -> None:
    as_of, snapshot, daily_summary, report_summary = _pass_projector_inputs()
    payload = {
        "data_quality_pit_safety": project_data_quality_pit_safety(
            as_of=as_of,
            snapshot=snapshot,
            daily_decision_summary=daily_summary,
            report_index_summary=report_summary,
        ),
        "pit_source_manifest": {
            "status": "PASS",
            "summary": "source-level PIT lineage verified",
        },
        "data_refresh_audit": {
            "status": "PASS",
            "summary": "refresh audit verified",
        },
    }
    before = copy.deepcopy(payload)
    spec = _section_spec()

    native = provide_data_quality_and_pit_section(payload, spec=spec)
    generic = _provide_legacy_payload_section(payload, spec=spec)

    assert native.status is CanonicalStatus.PASS
    assert generic.status is CanonicalStatus.PASS
    assert native.facts[0] == (
        "data_quality_pit_safety",
        "PASS_WITH_WARNINGS；one carried-forward field",
    )
    assert generic.facts[0] == ("data_quality_pit_safety", "AVAILABLE")
    assert native.source_keys == DATA_QUALITY_AND_PIT_SOURCE_KEYS
    assert payload == before


def test_native_typed_provider_preserves_missing_and_blocked_semantics() -> None:
    as_of, snapshot, daily_summary, report_summary = _blocked_projector_inputs()
    payload = {
        "data_quality_pit_safety": project_data_quality_pit_safety(
            as_of=as_of,
            snapshot=snapshot,
            daily_decision_summary=daily_summary,
            report_index_summary=report_summary,
        ),
        "pit_source_manifest": {
            "status": "BLOCKED",
            "summary": "source-level PIT lineage unavailable",
        },
    }
    spec = _section_spec()

    native = provide_data_quality_and_pit_section(payload, spec=spec)
    generic = _provide_legacy_payload_section(payload, spec=spec)

    assert native.status is CanonicalStatus.BLOCKED
    assert generic.status is CanonicalStatus.BLOCKED
    assert native.facts[0] == (
        "data_quality_pit_safety",
        "BLOCKED; requested-window gap",
    )
    assert native.facts[-1] == ("data_refresh_audit", "MISSING")
    assert native.caveats == ("缺少source keys：data_refresh_audit",)


@pytest.mark.parametrize(
    ("dq_overrides", "expected_status", "expected_fact", "expected_summary"),
    [
        (
            {"market_data_status": "FAIL"},
            CanonicalStatus.BLOCKED,
            "FAIL",
            "FAIL",
        ),
        (
            {"market_data_status": "UNKNOWN"},
            CanonicalStatus.LIMITED,
            "UNKNOWN",
            "data_quality_and_pit: LIMITED",
        ),
        (
            {"future_data_check": "REVIEW_REQUIRED"},
            CanonicalStatus.BLOCKED,
            "REVIEW_REQUIRED",
            "REVIEW_REQUIRED",
        ),
    ],
)
def test_native_provider_exposes_projected_dq_non_pass_state(
    dq_overrides: dict[str, str],
    expected_status: CanonicalStatus,
    expected_fact: str,
    expected_summary: str,
) -> None:
    dq_summary = {
        "data_gate_status": "PASS",
        "future_data_check": "PASS",
        "market_data_status": "PASS",
    }
    dq_summary.update(dq_overrides)
    payload = {
        "data_quality_pit_safety": dq_summary,
        "pit_source_manifest": {"status": "PASS"},
        "data_refresh_audit": {"status": "PASS"},
    }
    spec = _section_spec()

    native = provide_data_quality_and_pit_section(payload, spec=spec)
    inherited_generic = _provide_legacy_payload_section(payload, spec=spec)

    assert native.status is expected_status
    assert native.facts[0] == ("data_quality_pit_safety", expected_fact)
    assert native.summary == expected_summary
    # This is the exact inherited fail-open behavior removed by the native slice.
    assert inherited_generic.status is CanonicalStatus.PASS


@pytest.mark.parametrize(
    ("source_key", "unsafe_summary", "expected_fact"),
    [
        (
            "pit_source_manifest",
            {
                "status": "PASS",
                "validation_status": "PASS",
                "safety_status": "REVIEW_REQUIRED",
                "summary": "manifest is present",
            },
            "REVIEW_REQUIRED",
        ),
        (
            "data_refresh_audit",
            {
                "status": "REVIEW_REQUIRED",
                "summary": "refresh needs review",
            },
            "REVIEW_REQUIRED",
        ),
    ],
)
def test_native_provider_blocks_nested_source_safety_status(
    source_key: str,
    unsafe_summary: dict[str, str],
    expected_fact: str,
) -> None:
    as_of, snapshot, daily_summary, report_summary = _pass_projector_inputs()
    payload = {
        "data_quality_pit_safety": project_data_quality_pit_safety(
            as_of=as_of,
            snapshot=snapshot,
            daily_decision_summary=daily_summary,
            report_index_summary=report_summary,
        ),
        "pit_source_manifest": {"status": "PASS"},
        "data_refresh_audit": {"status": "PASS"},
    }
    payload[source_key] = unsafe_summary

    native = provide_data_quality_and_pit_section(payload, spec=_section_spec())

    assert native.status is CanonicalStatus.BLOCKED
    assert dict(native.facts)[source_key] == expected_fact
    assert native.summary == expected_fact


def test_native_provider_limits_untyped_nonempty_source_mapping() -> None:
    as_of, snapshot, daily_summary, report_summary = _pass_projector_inputs()
    payload = {
        "data_quality_pit_safety": project_data_quality_pit_safety(
            as_of=as_of,
            snapshot=snapshot,
            daily_decision_summary=daily_summary,
            report_index_summary=report_summary,
        ),
        "pit_source_manifest": {"summary": "no machine status"},
        "data_refresh_audit": {"status": "PASS"},
    }

    native = provide_data_quality_and_pit_section(payload, spec=_section_spec())

    assert native.status is CanonicalStatus.LIMITED
    assert dict(native.facts)["pit_source_manifest"] == "no machine status"
    assert native.summary == "no machine status"


def test_native_provider_fails_closed_for_non_frozen_section_contract() -> None:
    valid = _section_spec()
    wrong_section = ReportSectionSpec(
        section_id="system_status",
        title=valid.title,
        owner=valid.owner,
        reader_tier=valid.reader_tier,
        provider=valid.provider,
        provider_version=valid.provider_version,
        source_keys=valid.source_keys,
        core_order=valid.core_order,
    )
    wrong_sources = ReportSectionSpec(
        section_id=valid.section_id,
        title=valid.title,
        owner=valid.owner,
        reader_tier=valid.reader_tier,
        provider=valid.provider,
        provider_version=valid.provider_version,
        source_keys=("data_quality_pit_safety",),
        core_order=valid.core_order,
    )

    with pytest.raises(ValueError, match="section_id=data_quality_and_pit"):
        provide_data_quality_and_pit_section({}, spec=wrong_section)
    with pytest.raises(ValueError, match="frozen three source keys"):
        provide_data_quality_and_pit_section({}, spec=wrong_sources)


def test_native_module_has_no_legacy_io_or_investment_recompute_dependency() -> None:
    source = NATIVE_MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert "ai_trading_system.reports.reader_brief" not in imported_modules
    assert "ai_trading_system.platform.reporting.owner_daily" not in imported_modules
    assert not any(
        forbidden in source
        for forbidden in (
            "open(",
            "read_text(",
            "read_bytes(",
            "write_",
            "requests.",
            "score",
            "backtest",
            "position_gate",
        )
    )


def test_reader_brief_consumer_cut_in_is_single_and_legacy_builder_is_removed() -> None:
    tree = ast.parse(READER_BRIEF_MODULE_PATH.read_text(encoding="utf-8"))
    imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "ai_trading_system.platform.reporting.reader_brief_native"
        for alias in node.names
    ]
    local_definitions = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    native_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "project_data_quality_pit_safety"
    ]
    legacy_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_data_quality_pit_safety"
    ]

    assert imports.count("project_data_quality_pit_safety") == 1
    assert "_data_quality_pit_safety" not in local_definitions
    assert len(native_calls) == 1
    assert legacy_calls == []


def test_g3_fragments_are_strict_and_aggregate_compatible() -> None:
    payloads = tuple(_fragment_payload(path) for path in FRAGMENT_PATHS)
    artifact, flow, report = payloads

    assert tuple(payload["schema_version"] for payload in payloads) == (
        "arch_004e_artifact_fragment.v1",
        "arch_004e_flow_fragment.v1",
        "arch_004e_report_fragment.v1",
    )
    assert tuple(payload["fragment_id"] for payload in payloads) == (
        "artifact.arch_004g3_reader_brief_native",
        "flow.arch_004g3_reader_brief_native",
        "report.arch_004g3_reader_brief_native",
    )
    assert tuple(payload["target_id"] for payload in payloads) == (
        "artifact_catalog",
        "system_flow",
        "report_registry",
    )
    assert {payload["current_state_ratchet_id"] for payload in payloads} == {
        "arch_004g3_reader_brief_native_current.v1"
    }
    assert all(payload["generated_source_of_truth_active"] is False for payload in payloads)
    assert all(payload["production_effect"] == "none" for payload in payloads)
    assert artifact["legacy_artifact_bytes_preserved"] is True
    assert artifact["legacy_artifact_path_preserved"] is True
    assert artifact["legacy_artifact_schema_preserved"] is True
    assert artifact["legacy_artifact_status_preserved"] is True
    assert artifact["html_renderer_migrated"] is False
    assert flow["consumer_cut_in_complete"] is True
    assert flow["reporting_layer_recompute_allowed"] is False
    assert report["consumer_cut_in_complete"] is True
    assert report["current_state_ratchet"] == {
        "historical_f3_inventory": {
            "path": "inputs/architecture/arch_004f3_reporting_inventory.yaml",
            "inventory_id": "reporting_inventory_9f39c169eda16e98097c",
            "raw_sha256": "1804dcd6392f692c8e24c592f19888219f30f8b11405ec3eb1f3b05b8d918e06",
            "immutable": True,
        },
        "reader_brief_source": {
            "path": "src/ai_trading_system/reports/reader_brief.py",
            "sha256": ("b2a089e8b4995f31e982f4a5b0ba9446038e1f617610db8ba04b1b0521b5ba8e"),
            "line_count": 29005,
            "top_level_function_count": 366,
        },
        "owner_daily_core_sections": {
            "total_count": 10,
            "native_count": 1,
            "generic_count": 9,
        },
        "report_fragments": {
            "total_count": 5,
            "active_source_of_truth_count": 0,
        },
    }

    first = build_aggregate_shadow_index(
        project_root=PROJECT_ROOT,
        policy_path=OWNERSHIP_POLICY_PATH,
    )
    second = build_aggregate_shadow_index(
        project_root=PROJECT_ROOT,
        policy_path=OWNERSHIP_POLICY_PATH,
    )
    expected_ids = {payload["fragment_id"] for payload in payloads}
    aggregate_ids = {
        row["fragment_id"] for row in first["fragments"] if row["fragment_id"] in expected_ids
    }

    assert first == second
    assert aggregate_ids == expected_ids
    assert first["existing_aggregate_source_of_truth_changed"] is False


def _fragment_payload(path: Path) -> dict[str, Any]:
    payload = load_strict_yaml_path(path)
    if not isinstance(payload, dict):
        raise AssertionError(f"fragment must be a mapping: {path}")
    return payload
