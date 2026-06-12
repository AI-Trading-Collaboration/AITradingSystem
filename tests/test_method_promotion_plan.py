from __future__ import annotations

from dynamic_v3_system_target_helpers import run_method_promotion_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_method_promotion_plan_is_research_only_and_reader_brief_visible(tmp_path) -> None:
    fixture = run_method_promotion_plan_fixture(tmp_path)
    promotion_plan = fixture["promotion_plan"]
    manifest = promotion_plan["manifest"]
    specs = promotion_plan["promoted_method_specs"]
    method = specs["methods"][0]

    assert manifest["status"] == "PASS"
    assert method["source_variant_id"] == "sideways_choppy_hold_previous"
    assert method["implementation_scope"] == "research_only"
    assert method["auto_apply"] is False
    assert method["broker_action_allowed"] is False
    assert method["production_effect"] == "none"
    assert len(manifest["proposed_method_names"]) == len(set(manifest["proposed_method_names"]))
    assert "推荐正式实现 method" in promotion_plan["formal_implementation_plan"]
    assert "Owner Review Checklist" in promotion_plan["owner_review_checklist"]

    validation = system_target.validate_method_promotion_plan_artifact(
        promotion_plan_id=promotion_plan["promotion_plan_id"],
        output_dir=tmp_path / "method_promotion_plan",
    )
    assert validation["status"] == "PASS"

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_experiment_triage",
                "latest_artifact_path": str(
                    fixture["triage"]["triage_dir"] / "triage_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_top_variant_interpretation",
                "latest_artifact_path": str(
                    fixture["interpretation"]["interpretation_dir"]
                    / "top_variant_interpretation_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_method_promotion_plan",
                "latest_artifact_path": str(
                    promotion_plan["promotion_plan_dir"] / "method_promotion_manifest.json"
                ),
            },
        ]
    }
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["experiment_triage_id"] == fixture["triage"]["triage_id"]
    assert summary["best_experiment_variant"] == "sideways_choppy_hold_previous"
    assert summary["method_promotion_plan_id"] == promotion_plan["promotion_plan_id"]
    assert method["proposed_method_name"] in summary["proposed_method_names"]
    assert summary["promotion_implementation_scope"] == "research_only"
    assert summary["broker_action_allowed"] is False
