from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.weekly_review import (
    build_weekly_review_aggregation,
    build_weekly_review_report,
    build_weekly_review_validation_report,
    render_weekly_review_markdown,
    validate_weekly_review_action_items,
)
from ai_trading_system.reports.reader_brief import _etf_weekly_review_summary


def test_weekly_review_aggregator_loads_sources_and_marks_missing_required(
    tmp_path: Path,
) -> None:
    context = _weekly_context(tmp_path)

    aggregation = build_weekly_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        target_weights_path=context["target_weights_path"],
        required_report_ids=["etf_forward_dashboard"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert aggregation["aggregation_status"] == "PASS_WITH_WARNINGS"
    assert "etf_forward_dashboard" in aggregation["loaded_sections"]
    assert aggregation["portfolio_state"]["market_regime"] == "Overheated"
    assert any(
        item["report_id"] == "etf_portfolio_brief"
        and item["reason_code"] == "REPORT_NOT_FOUND"
        for item in aggregation["missing_sections"]
    )
    source = next(
        item
        for item in aggregation["source_reports"]
        if item["report_id"] == "etf_forward_dashboard"
    )
    assert source["source_report_path"].endswith("forward_dashboard_2026-06-01.json")

    failed = build_weekly_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        target_weights_path=context["target_weights_path"],
        required_report_ids=["etf_portfolio_brief"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    assert failed["aggregation_status"] == "FAIL"


def test_weekly_review_report_sections_actions_and_markdown(tmp_path: Path) -> None:
    context = _weekly_context(tmp_path)
    aggregation = build_weekly_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        target_weights_path=context["target_weights_path"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    payload = build_weekly_review_report(
        as_of=date(2026, 6, 1),
        aggregation_payload=aggregation,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["schema_version"] == "etf_weekly_review_v1"
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True
    assert payload["status"] in {"candidate_review_required", "risk_watch"}
    shadow = payload["sections"]["shadow_candidate_review"]
    assert shadow["active_shadow_candidates"][0]["recommended_observation_action"] == "watch"
    assert "promote_to_production" not in json.dumps(payload)
    assert any(
        action["action_type"] in {"mark_candidate_watch", "review_candidate"}
        for action in payload["manual_review_actions"]
    )
    markdown = render_weekly_review_markdown(payload)
    assert "Safety Banner" in markdown
    assert "Manual Review Action Items" in markdown
    assert "production_effect = none" in markdown


def test_weekly_review_rejects_unsafe_action() -> None:
    with pytest.raises(ValueError, match="unsafe"):
        validate_weekly_review_action_items(
            [
                {
                    "action_type": "place_order",
                    "requires_manual_review": True,
                    "evidence": [{"source_module": "unit"}],
                }
            ]
        )


def test_weekly_review_validation_gate_passes_with_registry() -> None:
    payload = build_weekly_review_validation_report(
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert any(check["check_id"] == "unsafe_actions_blocked" for check in payload["checks"])


def test_reader_brief_weekly_review_summary_reads_latest_report(tmp_path: Path) -> None:
    context = _weekly_context(tmp_path)
    aggregation = build_weekly_review_aggregation(
        as_of=date(2026, 6, 1),
        report_index_payload=context["report_index"],
        target_weights_path=context["target_weights_path"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    payload = build_weekly_review_report(
        as_of=date(2026, 6, 1),
        aggregation_payload=aggregation,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    weekly_path = tmp_path / "weekly_review_2026-06-01.json"
    _write_json(weekly_path, payload)

    summary = _etf_weekly_review_summary(
        {
            "reports": [
                {
                    "report_id": "etf_weekly_review",
                    "latest_artifact_path": str(weekly_path),
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["active_shadow_candidates"] == 1
    assert summary["manual_review_actions"] >= 1
    assert "observe_only=true" in summary["safety_status"]


def test_weekly_review_cli_generate_writes_outputs(tmp_path: Path) -> None:
    context = _weekly_context(tmp_path)
    report_index_path = tmp_path / "report_index_2026-06-01.json"
    _write_json(report_index_path, context["report_index"])
    output_dir = tmp_path / "weekly"
    aggregation_dir = tmp_path / "weekly" / "aggregation"

    result = CliRunner().invoke(
        etf_app,
        [
            "weekly-review",
            "generate",
            "--as-of",
            "2026-06-01",
            "--report-index-path",
            str(report_index_path),
            "--target-weights-path",
            str(context["target_weights_path"]),
            "--output-dir",
            str(output_dir),
            "--aggregation-dir",
            str(aggregation_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "weekly_review_2026-06-01.json").exists()
    assert (output_dir / "weekly_review_2026-06-01.md").exists()
    assert (aggregation_dir / "weekly_review_aggregation_2026-06-01.json").exists()


def _weekly_context(tmp_path: Path) -> dict[str, object]:
    target_weights_path = tmp_path / "target_weights.csv"
    target_weights_path.write_text(
        "\n".join(
            [
                "date,symbol,target_weight,previous_weight,trade_delta,composite_score,regime,"
                "reason_codes,constraints_applied,model_version,config_hash,data_quality_status",
                '2026-06-01,SPY,0.30,0.30,0.0,80,Overheated,"[]","[]",0.1.0,hash,PASS',
                '2026-06-01,QQQ,0.40,0.38,0.02,90,Overheated,"[]","[]",0.1.0,hash,PASS',
                '2026-06-01,SMH,0.15,0.17,-0.02,88,Overheated,"[]","[]",0.1.0,hash,PASS',
                '2026-06-01,CASH,0.15,0.15,0.0,50,Overheated,"[]","[]",0.1.0,hash,PASS',
            ]
        ),
        encoding="utf-8",
    )
    forward_dashboard_path = tmp_path / "forward_dashboard_2026-06-01.json"
    forward_watchlist_path = tmp_path / "forward_watchlist_2026-06-01.json"
    ai_path = tmp_path / "ai_confirmation_report_2026-06-01.json"
    satellite_path = tmp_path / "satellite_replacement_report_2026-06-01.json"
    validation_path = tmp_path / "forward_validation_2026-06-01.json"
    _write_json(forward_dashboard_path, _forward_dashboard_payload())
    _write_json(forward_watchlist_path, _forward_watchlist_payload())
    _write_json(ai_path, _ai_confirmation_payload())
    _write_json(satellite_path, _satellite_payload())
    _write_json(validation_path, {"status": "PASS", "production_effect": "none"})
    report_index = {
        "status": "PASS",
        "reports": [
            _report_index_record("etf_forward_dashboard", forward_dashboard_path),
            _report_index_record("etf_forward_watchlist", forward_watchlist_path),
            _report_index_record("etf_ai_confirmation_report", ai_path),
            _report_index_record("etf_satellite_replacement_report", satellite_path),
            _report_index_record("etf_forward_validation", validation_path),
        ],
    }
    return {"target_weights_path": target_weights_path, "report_index": report_index}


def _report_index_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "title": report_id,
        "latest_artifact_path": str(path),
        "artifact_status": "PASS",
        "freshness_status": "FRESH",
        "artifact_date": "2026-06-01",
    }


def _forward_dashboard_payload() -> dict[str, object]:
    return {
        "status": "AVAILABLE",
        "as_of": "2026-06-01",
        "candidate_summary_table": [
            {
                "shadow_id": "shadow_base_ai_growth",
                "candidate_id": "unit_run:base_ai_growth",
                "experiment_id": "base_ai_growth",
                "status": "watch",
                "days_since_enrollment": 25,
                "return_since_enrollment": 0.03,
                "excess_return_vs_baseline": -0.02,
                "excess_return_vs_QQQ": -0.01,
                "excess_return_vs_SPY": 0.01,
                "excess_return_vs_SMH": -0.03,
                "max_drawdown_since_enrollment": -0.06,
                "rolling_metrics": {"5d": {"return": 0.01}, "20d": {"return": 0.02}},
            }
        ],
        "status_summary": {"active_candidate_count": 1, "watch_count": 1},
        "constraint_hit_summary": {"hit_count": 1},
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def _forward_watchlist_payload() -> dict[str, object]:
    return {
        "status": "ATTENTION_REQUIRED",
        "as_of": "2026-06-01",
        "attention_required": [
            {
                "severity": "warning",
                "issue": "candidate under baseline",
                "candidate_id": "unit_run:base_ai_growth",
                "recommended_action": "needs_manual_review",
                "reason_code": "FORWARD_UNDERPERFORMANCE",
            }
        ],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _ai_confirmation_payload() -> dict[str, object]:
    return {
        "date": "2026-06-01",
        "status": "AVAILABLE",
        "AIConfirmationScore": {
            "score_value": 72.5,
            "score_band": "supportive",
            "action_hint": "supports_neutral_ai_exposure",
            "data_coverage_ratio": 1.0,
        },
        "component_scores": {
            "semiconductor_breadth": 70,
            "mega_cap_ai": 75,
            "ai_relative_strength": 72,
        },
        "event_risk_overlay": {"risk_band": "medium", "event_risk_score": 30},
        "data_coverage": {"coverage_ratio": 1.0},
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _satellite_payload() -> dict[str, object]:
    return {
        "date": "2026-06-01",
        "eligible_stocks": ["NVDA"],
        "watchlist": ["AMD"],
        "fallback_to_etf_stocks": ["TSM"],
        "stock_vs_etf_features": [{"ticker": "NVDA", "relative_return_60d": 0.05}],
        "replacement_eligibility": [{"ticker": "NVDA", "status": "eligible"}],
        "replacement_plan": {
            "replacement_plan_id": "plan-1",
            "total_replaced_weight": 0.03,
            "satellite_allocations": [{"ticker": "NVDA", "weight": 0.03}],
            "fallback_positions": [{"ticker": "TSM", "benchmark_etf": "SMH"}],
            "constraints_applied": ["max_single_stock_weight"],
        },
        "risk_constraints": {"max_single_stock_weight": 0.05},
        "ai_confirmation_context": {"action_hint": "supports_neutral_ai_exposure"},
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
