from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.research_governance_summary import (
    GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS,
    GROUP_BLOCKED,
    GROUP_CANDIDATE_RESEARCH,
    GROUP_ROLLBACK_WARNING,
    GROUP_SHADOW_OBSERVE,
    PROMOTION_STATUS_BLOCKED_MANUAL_REVIEW,
    PROMOTION_STATUS_BLOCKED_MISSING,
    build_research_governance_summary_payload,
    render_research_governance_summary_markdown,
)


def test_research_governance_summary_groups_existing_artifacts(tmp_path: Path) -> None:
    _write_governance_inputs(tmp_path)

    payload = build_research_governance_summary_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
    )
    markdown = render_research_governance_summary_markdown(payload)

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["governance_status"] == GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS
    assert payload["research_readiness"] == "READY_FOR_REVIEW"
    assert payload["promotion_status"] == PROMOTION_STATUS_BLOCKED_MANUAL_REVIEW
    assert payload["production_effect"] == "none"
    assert payload["as_of_date"] == "2026-05-04"
    assert payload["manual_review_required"] is True
    assert payload["summary"]["card_count"] >= 10
    assert payload["summary"]["manual_review_required_count"] >= 3
    assert payload["backtest"]["backtest_status"] == "AVAILABLE"
    assert payload["backtest"]["robustness_status"] == "MISSING"
    assert payload["weight_iteration"]["weight_promotion_gate_status"] == "READY_FOR_MANUAL_REVIEW"
    assert payload["shadow_observe"]["sec_pit_capex_intensity_lane_status"] == "observe_only"
    assert payload["shadow_observe"]["rollback_recommended"] is True
    assert payload["sec_pit"]["pit_grade_policy"] == "B_RECONSTRUCTED_SEC_FILING_PIT"
    assert payload["sec_pit"]["production_effect"] == "none"
    assert payload["documentation"]["report_index_status"] == "MISSING"
    assert any(
        item["item_id"].startswith("manual_review:") for item in payload["manual_review_queue"]
    )
    assert any(
        source["artifact_id"] == "weight_promotion_gate" for source in payload["source_artifacts"]
    )
    cards = {card["card_id"]: card for card in payload["cards"]}
    assert cards["sec_pit_evaluation"]["path"].endswith(
        "sec_pit_evaluation_summary_2026-05-03.json"
    )
    assert cards["backtest_daily"]["path"].endswith("backtest_2026-05-01_2026-05-04.md")
    assert not cards["backtest_daily"]["path"].endswith(
        "backtest_robustness_2026-05-04_2026-05-04.md"
    )
    assert cards["sec_pit_shadow_observe"]["group"] == GROUP_SHADOW_OBSERVE
    assert cards["weight_candidate_evaluation"]["group"] == GROUP_CANDIDATE_RESEARCH
    assert cards["weight_adjustment_candidates"]["group"] == GROUP_BLOCKED
    assert cards["sec_pit_shadow_monitor"]["group"] == GROUP_ROLLBACK_WARNING
    assert any("sec_pit_shadow_monitor_rollback_or_warning" in item for item in payload["warnings"])
    assert cards["weight_promotion_gate"]["candidate_id"] == "weight_candidate:limited"
    assert cards["weight_promotion_gate"]["manual_review_required"] is True
    assert "Research Governance Summary 2026-05-04" in markdown
    assert "## Executive Summary" in markdown
    assert "## Manual Review Queue" in markdown
    assert "Shadow observe-only" in markdown
    assert "production_effect=none" in markdown


def test_reports_research_governance_summary_cli_writes_markdown_and_json(
    tmp_path: Path,
) -> None:
    _write_governance_inputs(tmp_path)
    markdown_path = tmp_path / "research_governance_summary_2026-05-04.md"
    json_path = tmp_path / "research_governance_summary_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "research-governance-summary",
            "--as-of",
            "2026-05-04",
            "--project-root",
            str(tmp_path),
            "--output-path",
            str(markdown_path),
            "--json-output-path",
            str(json_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Research governance summary：PASS_WITH_LIMITATIONS" in result.output
    assert "只读汇总" in result.output
    assert markdown_path.exists()
    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "research_governance_summary"
    assert payload["governance_status"] == GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS
    assert payload["production_effect"] == "none"
    assert any(card["card_id"] == "sec_pit_shadow_observe" for card in payload["cards"])


def test_research_governance_summary_missing_artifacts_blocks_promotion(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    _write_json(
        reports_dir / "parameter_governance_2026-05-04.json",
        {
            "report_type": "parameter_governance",
            "status": "PASS_WITH_LIMITATIONS",
            "production_effect": "none",
        },
    )

    payload = build_research_governance_summary_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
    )

    assert payload["promotion_status"] == PROMOTION_STATUS_BLOCKED_MISSING
    assert payload["backtest"]["backtest_status"] == "MISSING"
    assert payload["backtest"]["impact_level"] == "IMPORTANT"
    assert payload["backtest"]["recommended_action"] == "rerun_after_scoring_or_gate_change"
    assert payload["weight_iteration"]["weight_candidate_evaluation_status"] == "MISSING"
    assert any(
        item["item_id"] == "missing_weight_promotion_gate"
        for item in payload["manual_review_queue"]
    )
    assert any(
        item["item_id"] == "missing_weight_candidate_evaluation"
        for item in payload["manual_review_queue"]
    )
    assert payload["production_effect"] == "none"
    assert all(card["production_effect"] == "none" for card in payload["cards"])
    assert all(source["production_effect"] == "none" for source in payload["source_artifacts"])


def test_research_governance_summary_source_order_is_deterministic(tmp_path: Path) -> None:
    _write_governance_inputs(tmp_path)

    first = build_research_governance_summary_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
    )
    second = build_research_governance_summary_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
    )

    assert [item["artifact_id"] for item in first["source_artifacts"]] == [
        item["artifact_id"] for item in second["source_artifacts"]
    ]
    assert [item["item_id"] for item in first["manual_review_queue"]] == [
        item["item_id"] for item in second["manual_review_queue"]
    ]


def _write_governance_inputs(project_root: Path) -> None:
    reports_dir = project_root / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    backtests_dir = project_root / "outputs" / "backtests"
    backtests_dir.mkdir(parents=True)
    (backtests_dir / "backtest_2026-05-01_2026-05-04.md").write_text(
        "# Daily Score Backtest\n",
        encoding="utf-8",
    )
    (backtests_dir / "backtest_robustness_2026-05-04_2026-05-04.md").write_text(
        "# Backtest Robustness Markdown\n",
        encoding="utf-8",
    )
    _write_json(
        reports_dir / "parameter_governance_2026-05-04.json",
        {
            "report_type": "parameter_governance",
            "status": "PASS_WITH_LIMITATIONS",
            "production_effect": "none",
            "selected_trial_id": "source_current__grid_gate_0217",
            "summary": "production profile unchanged; owner input limited",
            "warnings": [{"code": "owner_input_limited"}],
        },
    )
    _write_json(
        reports_dir / "weight_adjustment_candidates_2026-05-04.json",
        {
            "report_type": "weight_adjustment_candidates",
            "gate_status": "BLOCKED",
            "production_effect": "none",
            "top_candidate_id": "weight_candidate:limited",
            "manual_review_required": True,
            "main_blocked_by": "manual_approval_required",
        },
    )
    _write_json(
        reports_dir / "weight_candidate_evaluation_2026-05-04.json",
        {
            "report_type": "weight_candidate_evaluation",
            "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
            "production_effect": "none",
            "top_candidate_id": "weight_candidate:limited",
            "manual_review_required": True,
            "summary": "paper sample remains limited",
        },
    )
    _write_json(
        reports_dir / "weight_promotion_gate_2026-05-04.json",
        {
            "report_type": "weight_promotion_gate",
            "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
            "production_effect": "none",
            "top_candidate_id": "weight_candidate:limited",
            "manual_review_required": True,
            "next_action": "owner_review_required",
        },
    )
    _write_json(
        project_root
        / "outputs"
        / "sec_pit_evaluation"
        / "sec_pit_evaluation_summary_2026-05-03.json",
        {
            "report_type": "sec_pit_evaluation",
            "status": "PASS_WITH_LIMITATIONS",
            "production_effect": "none",
            "candidate_feature": "capex_intensity",
            "manual_review_required": True,
        },
    )
    _write_json(
        project_root
        / "outputs"
        / "sec_pit_evaluation"
        / "sec_pit_evaluation_summary_2026-05-05.json",
        {
            "report_type": "sec_pit_evaluation",
            "status": "PASS",
            "production_effect": "none",
            "candidate_feature": "future_feature",
        },
    )
    _write_json(
        project_root
        / "outputs"
        / "sec_pit_shadow_observe"
        / "sec_pit_shadow_observe_summary_2026-05-04.json",
        {
            "report_type": "sec_pit_shadow_observe",
            "shadow_status": "MONITORING_ACTIVE",
            "production_effect": "none",
            "candidate_feature": "capex_intensity",
            "manual_review_required": True,
            "monitoring_status_reason": "minimum evidence not yet reached",
            "lane_id": "sec_pit_capex_intensity_observe_only",
            "lane_status": "observe_only",
            "limitations": ["observe-only lane"],
        },
    )
    _write_json(
        project_root
        / "outputs"
        / "sec_pit_shadow_monitor"
        / "sec_pit_shadow_monitor_summary_2026-05-04.json",
        {
            "report_type": "sec_pit_shadow_monitor",
            "monitor_status": "WARNING",
            "production_effect": "none",
            "candidate_feature": "capex_intensity",
            "recommendation": "ROLLBACK_RECOMMENDED",
            "rollback_recommended": True,
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
