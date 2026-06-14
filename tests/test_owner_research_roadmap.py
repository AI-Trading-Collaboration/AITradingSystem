from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_owner_research_roadmap_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_owner_research_roadmap_records_checklist_without_production_effect(tmp_path) -> None:
    fixture = run_owner_research_roadmap_fixture(tmp_path)
    roadmap = fixture["owner_roadmap"]

    assert roadmap["manifest"]["status"] == "PASS"
    assert roadmap["owner_roadmap_summary"]["current_phase"]
    assert "broker_action_allowed=false" in roadmap["owner_roadmap_checklist"]
    assert roadmap["manifest"]["production_effect"] == "none"
    assert "Owner Research Roadmap" in roadmap["reader_brief_section"]

    validation = weight_search.validate_owner_research_roadmap_artifact(
        roadmap_id=roadmap["roadmap_id"],
        output_dir=tmp_path / "owner_research_roadmap",
    )
    assert validation["status"] == "PASS"
