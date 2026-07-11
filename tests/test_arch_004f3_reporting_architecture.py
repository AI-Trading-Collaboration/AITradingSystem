from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.contracts.report_spec import (
    OwnerActionItem,
    OwnerDailyBriefViewModel,
    ReaderTier,
    ReportCatalogDisposition,
    ReportContractError,
    ReportSectionSpec,
    ReportSectionViewModel,
)
from ai_trading_system.contracts.research_lifecycle import (
    ResearchReviewDecision,
    apply_periodic_research_review,
)
from ai_trading_system.contracts.research_review import ResearchReviewPackViewModel
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import EntrypointRef
from ai_trading_system.legacy.reporting_catalog_adapter import (
    assess_report_registry_catalog,
)
from ai_trading_system.platform.reporting import (
    ReportingArchitectureError,
    assert_frozen_reporting_inventory,
    build_owner_daily_brief_view_model,
    build_report_audit_index,
    build_research_review_pack,
    load_reporting_architecture_policy,
    render_owner_daily_brief_html,
    scan_reporting_architecture,
    write_owner_daily_brief_sidecars,
    write_report_audit_index,
    write_research_review_pack,
)


def test_reporting_architecture_policy_freezes_three_tier_safety_boundary() -> None:
    policy = load_reporting_architecture_policy()

    assert policy.policy_id == "arch_004f3_reporting_architecture_v1"
    assert policy.max_core_sections == 10
    assert tuple(item.section_id for item in policy.core_sections) == (
        "system_status",
        "today_decision",
        "market_and_score_change",
        "position_and_binding_gates",
        "data_quality_and_pit",
        "owner_action_queue",
        "portfolio_and_shadow",
        "research_review_due",
        "operations_health",
        "safety_and_navigation",
    )
    assert policy.owner_queue_requires_due is True
    assert policy.owner_queue_requires_actionable is True
    assert policy.research_auto_tune_allowed is False
    assert policy.proposal_may_equal_adoption is False
    assert policy.audit_include_all_registry_entries is True
    assert policy.audit_include_legacy_unclassified is True
    assert policy.legacy_unclassified_disposition == "AUDIT_INDEX_LIMITED_UNCLASSIFIED"
    assert policy.reader_brief_cut_in_enabled is False
    assert policy.additive_sidecars_only is True
    assert policy.preserve_legacy_path_schema_status is True
    assert policy.reporting_layer_recompute_allowed is False
    assert policy.production_effect == "none"
    assert policy.broker_action == "none"


def test_reporting_inventory_matches_frozen_f3_baseline() -> None:
    inventory = scan_reporting_architecture()

    assert inventory.reader_brief_line_count == 29027
    assert inventory.reader_brief_top_level_function_count == 366
    assert inventory.report_registry_entry_count == 1358
    assert inventory.explicit_production_effect_count == 669
    assert inventory.missing_explicit_production_effect_count == 689
    assert inventory.explicit_reader_tier_count == 0
    assert inventory.explicit_actionable_count == 0
    assert inventory.explicit_section_provider_count == 0
    assert inventory.explicit_view_model_count == 0
    assert inventory.explicit_renderer_count == 0
    assert inventory.explicit_canonical_source_count == 0
    assert dict(inventory.cadence_counts) == {
        "ad_hoc": 949,
        "daily": 83,
        "event_driven": 1,
        "manual": 236,
        "monthly": 15,
        "on_change": 10,
        "weekly": 64,
    }
    assert dict(inventory.audience_counts) == {
        "daily_reader": 1,
        "investor": 8,
        "operator": 127,
        "owner": 77,
        "project_owner": 773,
        "reviewer": 371,
        "system": 1,
    }
    assert inventory.report_fragment_count == 4
    assert inventory.active_report_fragment_count == 0
    assert inventory.legacy_unclassified_entry_count == 1358
    assert_frozen_reporting_inventory(inventory)


def test_reporting_inventory_detects_reader_brief_drift(tmp_path: Path) -> None:
    source = tmp_path / "reader_brief.py"
    source.write_text("def build():\n    return {}\n", encoding="utf-8")
    inventory = scan_reporting_architecture(reader_brief_path=source)

    with pytest.raises(ReportingArchitectureError, match="REPORTING_INVENTORY_DRIFT"):
        assert_frozen_reporting_inventory(inventory)


def test_reporting_policy_rejects_more_than_ten_core_sections(tmp_path: Path) -> None:
    source = Path("config/reporting/reporting_architecture.yaml").read_text(encoding="utf-8")
    invalid = source.replace("max_core_sections: 10", "max_core_sections: 9")
    path = tmp_path / "invalid_reporting_policy.yaml"
    path.write_text(invalid, encoding="utf-8")

    with pytest.raises(ReportingArchitectureError, match="CORE_SECTION_LIMIT_EXCEEDED"):
        load_reporting_architecture_policy(path)


def _section_spec(section_id: str, order: int) -> ReportSectionSpec:
    return ReportSectionSpec(
        section_id=section_id,
        title=section_id.replace("_", " ").title(),
        owner="reporting_governance",
        reader_tier=ReaderTier.OWNER_DAILY_BRIEF,
        provider=EntrypointRef(module="tests.reporting", callable_name=section_id),
        provider_version="1.0.0",
        source_keys=(f"source_{section_id}",),
        core_order=order,
    )


def _section_view(spec: ReportSectionSpec) -> ReportSectionViewModel:
    return ReportSectionViewModel(
        section_spec_id=spec.spec_id,
        section_id=spec.section_id,
        title=spec.title,
        reader_tier=spec.reader_tier,
        status=CanonicalStatus.PASS,
        summary=f"{spec.section_id} summary",
        facts=(("status", "PASS"),),
        source_keys=spec.source_keys,
    )


def test_reporting_section_and_owner_daily_view_model_round_trip() -> None:
    first = _section_spec("system_status", 1)
    second = _section_spec("today_decision", 2)
    action = OwnerActionItem(
        action_id="owner_action_1",
        title="Review due evidence",
        owner_action="review",
        due_status=CanonicalStatus.DUE,
        actionable=True,
        priority=1,
        source_artifact_ids=("artifact_1",),
    )
    view = OwnerDailyBriefViewModel(
        policy_id="arch_004f3_reporting_architecture_v1",
        as_of=date(2026, 7, 11),
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
        status=CanonicalStatus.PASS,
        sections=(_section_view(first), _section_view(second)),
        owner_queue=(action,),
    )

    assert ReportSectionSpec.from_dict(first.to_dict()) == first
    assert ReportSectionViewModel.from_dict(_section_view(first).to_dict()) == _section_view(first)
    assert OwnerDailyBriefViewModel.from_dict(view.to_dict()) == view


def test_owner_daily_view_model_rejects_non_due_or_non_actionable_queue_items() -> None:
    spec = _section_spec("system_status", 1)
    ineligible = OwnerActionItem(
        action_id="not_due",
        title="Not due",
        owner_action="wait",
        due_status=CanonicalStatus.NOT_DUE,
        actionable=True,
        priority=1,
        source_artifact_ids=("artifact_1",),
    )

    with pytest.raises(ReportContractError, match="OWNER_BRIEF_QUEUE_ITEM_INELIGIBLE"):
        OwnerDailyBriefViewModel(
            policy_id="policy",
            as_of=date(2026, 7, 11),
            generated_at=datetime(2026, 7, 11, tzinfo=UTC),
            status=CanonicalStatus.LIMITED,
            sections=(_section_view(spec),),
            owner_queue=(ineligible,),
        )


def test_actual_report_registry_is_fully_covered_as_limited_unclassified() -> None:
    policy = load_reporting_architecture_policy()
    assessment = assess_report_registry_catalog(
        Path("config/report_registry.yaml"),
        policy=policy,
    )

    assert assessment.status is CanonicalStatus.LIMITED
    assert len(assessment.entries) == 1358
    assert all(item.report_spec is None for item in assessment.entries)
    assert all(item.status is CanonicalStatus.LIMITED for item in assessment.entries)
    assert {item.disposition for item in assessment.entries} == {
        ReportCatalogDisposition.AUDIT_INDEX_LIMITED_UNCLASSIFIED
    }
    assert assessment.to_dict()["typed_count"] == 0
    assert assessment.to_dict()["limited_count"] == 1358
    assert assessment.to_dict()["blocked_count"] == 0


def test_explicit_typed_registry_entry_maps_to_report_spec(tmp_path: Path) -> None:
    path = tmp_path / "typed_registry.yaml"
    path.write_text(
        """schema_version: 1
reports:
  - report_id: typed_owner_report
    title: Typed Owner Report
    owner: reporting_governance
    audience: owner
    cadence: daily
    artifact_globs: [outputs/reports/typed_owner_report.json]
    freshness_sla_days: 1
    owner_action: review
    visibility_policy: current
    production_effect: none
    reader_tier: owner_daily_brief
    actionable: true
    canonical_source: {module: tests.reporting, callable_name: source}
    section_provider: {module: tests.reporting, callable_name: provider}
    view_model: {module: tests.reporting, callable_name: view_model}
    renderer: {module: tests.reporting, callable_name: renderer}
""",
        encoding="utf-8",
    )
    assessment = assess_report_registry_catalog(
        path,
        policy=load_reporting_architecture_policy(),
    )

    assert assessment.status is CanonicalStatus.PASS
    assert assessment.entries[0].disposition is ReportCatalogDisposition.TYPED
    assert assessment.entries[0].report_spec is not None
    assert assessment.entries[0].report_spec.reader_tier is ReaderTier.OWNER_DAILY_BRIEF


def _legacy_reader_payload() -> dict[str, object]:
    return {
        "as_of": "2026-07-11",
        "generated_at": "2026-07-11T09:00:00+00:00",
        "status": "PASS",
        "narrative_executive_summary": {"summary": "维持观察，不改变生产策略。"},
        "market_situation_snapshot": {"status": "PASS", "summary": "AI regime stable"},
        "score_to_position_funnel": {"status": "PASS", "summary": "No binding change"},
        "data_quality_pit_safety": {"status": "PASS", "summary": "PIT PASS"},
        "manual_review_queue": [{"title": "legacy queue item"}],
        "portfolio_control_research": {"status": "PASS", "summary": "shadow only"},
        "research_governance_summary": {"status": "PASS", "summary": "review not due"},
        "daily_task_dashboard": {"status": "PASS", "summary": "daily complete"},
        "production_effect": "none",
    }


def test_owner_daily_provider_builds_fixed_ten_sections_without_recomputing() -> None:
    payload = _legacy_reader_payload()
    view = build_owner_daily_brief_view_model(
        payload,
        generated_at=datetime(2026, 7, 11, 9, 0, tzinfo=UTC),
    )

    assert len(view.sections) == 10
    assert tuple(item.section_id for item in view.sections) == tuple(
        item.section_id for item in load_reporting_architecture_policy().core_sections
    )
    assert view.owner_queue == ()
    assert render_owner_daily_brief_html(view).count('data-section-id="') == 10
    assert payload == _legacy_reader_payload()


def test_owner_daily_queue_accepts_only_explicit_due_and_actionable_items() -> None:
    payload = _legacy_reader_payload()
    payload["owner_action_queue"] = [
        {
            "action_id": "due_action",
            "title": "Review due evidence",
            "owner_action": "review",
            "due_status": "DUE",
            "actionable": True,
            "priority": 2,
            "source_artifact_ids": ["artifact_1"],
        },
        {
            "action_id": "not_due",
            "title": "Wait",
            "owner_action": "wait",
            "due_status": "NOT_DUE",
            "actionable": True,
            "priority": 1,
            "source_artifact_ids": ["artifact_2"],
        },
        {
            "action_id": "not_actionable",
            "title": "Informational",
            "owner_action": "none",
            "due_status": "DUE",
            "actionable": False,
            "priority": 1,
            "source_artifact_ids": ["artifact_3"],
        },
    ]

    view = build_owner_daily_brief_view_model(payload)

    assert tuple(item.action_id for item in view.owner_queue) == ("due_action",)


def test_owner_daily_ignores_unrelated_legacy_payload_growth() -> None:
    payload = _legacy_reader_payload()
    generated_at = datetime(2026, 7, 11, 9, 0, tzinfo=UTC)
    baseline = build_owner_daily_brief_view_model(payload, generated_at=generated_at)
    payload["new_unclassified_report"] = {"status": "BLOCKED", "summary": "must not leak"}

    expanded = build_owner_daily_brief_view_model(payload, generated_at=generated_at)

    assert expanded.to_dict() == baseline.to_dict()


def test_owner_daily_sidecars_round_trip(tmp_path: Path) -> None:
    json_path, html_path = write_owner_daily_brief_sidecars(
        _legacy_reader_payload(),
        output_dir=tmp_path,
    )

    assert json_path == tmp_path / "owner_daily_brief_2026-07-11.json"
    assert html_path == tmp_path / "owner_daily_brief_2026-07-11.html"
    assert OwnerDailyBriefViewModel.from_dict(
        json.loads(json_path.read_text(encoding="utf-8"))
    ).as_of == date(2026, 7, 11)
    assert html_path.read_text(encoding="utf-8").count('data-section-id="') == 10


def test_research_review_pack_separates_retirement_investigation_and_adoption(
    tmp_path: Path,
) -> None:
    retired = apply_periodic_research_review(
        lifecycle_id="experiment:growth_tilt_terminal",
        owner="research_owner",
        observation_ref="artifact:growth_tilt_source",
        evidence_refs=("artifact:growth_tilt_closure",),
        decision=ResearchReviewDecision.RETIRE,
        at=datetime(2026, 7, 11, 10, 0, tzinfo=UTC),
        actor="research_owner",
        reason_codes=("NO_CONTRACT_COMPLETE_CANDIDATE",),
    )
    investigating = apply_periodic_research_review(
        lifecycle_id="experiment:blocked_source_contract",
        owner="research_owner",
        observation_ref="artifact:blocked_source",
        evidence_refs=("artifact:blocked_evidence",),
        decision=ResearchReviewDecision.INVESTIGATE,
        at=datetime(2026, 7, 11, 11, 0, tzinfo=UTC),
        actor="research_owner",
        reason_codes=("PIT_LINEAGE_MISSING",),
    )

    view = build_research_review_pack(
        (retired, investigating),
        as_of=date(2026, 7, 11),
        generated_at=datetime(2026, 7, 11, 12, 0, tzinfo=UTC),
    )
    json_path, markdown_path = write_research_review_pack(view, output_dir=tmp_path)

    assert view.status is CanonicalStatus.BLOCKED
    assert view.auto_tune_allowed is False
    assert view.proposal_may_equal_adoption is False
    assert all(item.adoption_recorded is False for item in view.items)
    assert all(item.proposal_is_adoption is False for item in view.items)
    assert view.items[0].blocker_codes == ("PIT_LINEAGE_MISSING",)
    assert ResearchReviewPackViewModel.from_dict(
        json.loads(json_path.read_text(encoding="utf-8"))
    ) == view
    assert "proposal不等于adoption" in markdown_path.read_text(encoding="utf-8")


def test_report_audit_index_preserves_every_legacy_registry_entry(tmp_path: Path) -> None:
    policy = load_reporting_architecture_policy()
    catalog = assess_report_registry_catalog(Path("config/report_registry.yaml"), policy=policy)
    view = build_report_audit_index(
        catalog,
        generated_at=datetime(2026, 7, 11, 12, 30, tzinfo=UTC),
        policy=policy,
    )
    json_path, markdown_path = write_report_audit_index(view, output_dir=tmp_path)
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    assert payload["entry_count"] == 1358
    assert payload["typed_count"] == 0
    assert payload["limited_count"] == 1358
    assert len(payload["entries"]) == 1358
    assert len({item["report_id"] for item in payload["entries"]}) == 1358
    assert payload["include_all_registry_entries"] is True
    assert payload["include_legacy_unclassified"] is True
    assert payload["production_effect"] == "none"
    assert "不静默推断" in markdown_path.read_text(encoding="utf-8")
