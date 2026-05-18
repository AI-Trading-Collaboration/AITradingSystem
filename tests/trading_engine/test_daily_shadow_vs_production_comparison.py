from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.reports.daily_shadow_vs_production_comparison import (
    build_daily_shadow_vs_production_comparison_payload,
    write_daily_shadow_vs_production_comparison_report,
)


def test_shadow_vs_production_comparison_scores_same_components(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    _write_inputs(context, as_of)

    payload = write_daily_shadow_vs_production_comparison_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        scoring_rules_path=context["scoring_rules"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["comparison_status"] == "COMPARISON_AVAILABLE"
    assert payload["production"]["score"] == 62.0
    assert payload["shadow"]["score"] == 69.0
    assert payload["production"]["decision"] == "中性"
    assert payload["shadow"]["decision"] == "偏重仓/仓位受限"
    assert payload["difference"]["score_delta"] == 7.0
    assert payload["difference"]["normalized_score_delta"] == 0.07
    assert payload["difference"]["decision_changed"] is True
    assert "trend" in payload["difference"]["main_reason"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert payload["pipeline_contract"]["runs_broker_runner"] is False
    assert payload["pipeline_contract"]["runs_replay_runner"] is False
    assert payload["pipeline_contract"]["writes_production_profile"] is False


def test_shadow_vs_production_missing_shadow_state_is_insufficient_data(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    _write_decision_snapshot(context["data_root"], as_of)
    _write_shadow_candidate(context["data_root"], as_of)

    payload = build_daily_shadow_vs_production_comparison_payload(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        scoring_rules_path=context["scoring_rules"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["comparison_status"] == "INSUFFICIENT_DATA"
    assert (
        "current_shadow_weights_missing_or_invalid"
        in payload["input_validation"]["blocking_reasons"]
    )
    assert payload["production"] == {}
    assert payload["shadow"] == {}


def test_shadow_vs_production_dashboard_card_reads_existing_json(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 19)
    _write_inputs(context, as_of)
    write_daily_shadow_vs_production_comparison_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        production_profile_path=context["production_profile"],
        scoring_rules_path=context["scoring_rules"],
        generated_at=_fixed_generated_at(),
    )
    _remove_comparison_inputs(context, as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["shadow_vs_production_comparison"]
    assert summary["status"] == "COMPARISON_AVAILABLE"
    assert summary["production_decision"] == "中性"
    assert summary["shadow_decision"] == "偏重仓/仓位受限"
    assert summary["score_delta"] == "+7.00"
    assert summary["decision_changed"] is True
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert "Shadow vs Production Comparison" in html
    assert "daily_shadow_vs_production_2026-05-19.md" in html


def _write_context(tmp_path: Path) -> dict[str, Path]:
    reports_dir = tmp_path / "outputs" / "reports"
    data_root = tmp_path / "data"
    config_dir = tmp_path / "config"
    weights_dir = config_dir / "weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    production_profile = weights_dir / "weight_profile_current.yaml"
    production_profile.write_text(
        "\n".join(
            [
                'version: "test-production"',
                'status: "production"',
                "base_weights:",
                "  trend: 0.20",
                "  fundamentals: 0.40",
                "  macro_liquidity: 0.40",
                "",
            ]
        ),
        encoding="utf-8",
    )
    scoring_rules = config_dir / "scoring_rules.yaml"
    scoring_rules.write_text(
        "\n".join(
            [
                "position_bands:",
                "  - min_score: 65",
                "    min_position: 0.60",
                "    max_position: 0.80",
                '    label: "偏重仓"',
                "  - min_score: 50",
                "    min_position: 0.40",
                "    max_position: 0.60",
                '    label: "中性"',
                "  - min_score: 0",
                "    min_position: 0.00",
                "    max_position: 0.20",
                '    label: "防守"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "reports_dir": reports_dir,
        "data_root": data_root,
        "production_profile": production_profile,
        "scoring_rules": scoring_rules,
    }


def _write_inputs(context: dict[str, Path], as_of: date) -> None:
    _write_decision_snapshot(context["data_root"], as_of)
    _write_current_shadow(context["data_root"], as_of)
    _write_shadow_candidate(context["data_root"], as_of)
    _write_daily_artifacts(context["reports_dir"], as_of)


def _write_decision_snapshot(data_root: Path, as_of: date) -> None:
    _write_json(
        data_root / "processed" / "decision_snapshots" / f"decision_snapshot_{as_of}.json",
        {
            "schema_version": 1,
            "snapshot_id": f"decision_snapshot:{as_of}",
            "signal_date": as_of.isoformat(),
            "market_regime": {"regime_id": "ai_after_chatgpt"},
            "scores": {
                "components": [
                    {
                        "component": "trend",
                        "score": 90.0,
                        "weight": 20.0,
                        "source_type": "hard_data",
                        "coverage": 1.0,
                        "confidence": 1.0,
                    },
                    {
                        "component": "fundamentals",
                        "score": 60.0,
                        "weight": 40.0,
                        "source_type": "hard_data",
                        "coverage": 1.0,
                        "confidence": 1.0,
                    },
                    {
                        "component": "macro_liquidity",
                        "score": 50.0,
                        "weight": 40.0,
                        "source_type": "hard_data",
                        "coverage": 1.0,
                        "confidence": 1.0,
                    },
                ],
            },
            "positions": {
                "position_gates": [
                    {
                        "gate_id": "score_model",
                        "label": "评分模型仓位",
                        "max_position": 0.6,
                        "triggered": True,
                        "source": "weighted_score_model",
                        "reason": "production score model",
                    },
                    {
                        "gate_id": "valuation",
                        "label": "估值拥挤",
                        "max_position": 0.7,
                        "triggered": True,
                        "source": "valuation",
                        "reason": "test valuation cap",
                    },
                ],
            },
            "quality": {
                "market_data_status": "PASS",
                "market_data_error_count": 0,
                "market_data_warning_count": 0,
                "feature_status": "PASS",
                "feature_warning_count": 0,
            },
        },
    )


def _write_current_shadow(data_root: Path, as_of: date) -> None:
    _write_json(
        data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json",
        {
            "schema_version": "1.0",
            "report_type": "current_shadow_weights",
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "last_updated_date": as_of.isoformat(),
            "weights": {
                "trend": 0.40,
                "fundamentals": 0.30,
                "macro_liquidity": 0.30,
            },
            "audit": {"update_count": 1, "last_decision": "UPDATE"},
        },
    )


def _write_shadow_candidate(data_root: Path, as_of: date) -> None:
    _write_json(
        data_root
        / "derived"
        / "weight_iterations"
        / "shadow"
        / "candidates"
        / f"shadow_weight_candidate_{as_of}.json",
        {
            "schema_version": "1.0",
            "report_type": "daily_shadow_weight_iteration",
            "date": as_of.isoformat(),
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "decision": "UPDATE",
            "decision_reason": "test update",
            "proposed_delta": {
                "trend": 0.20,
                "fundamentals": -0.10,
                "macro_liquidity": -0.10,
            },
            "run_log": {"current_state_updated": True, "history_written": True},
        },
    )


def _write_daily_artifacts(reports_dir: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    _write_json(
        reports_dir / f"daily_decision_summary_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "daily_decision_summary",
            "as_of": suffix,
            "production_effect": "none",
            "data_gate": {"status": "PASS"},
        },
    )
    _write_json(
        reports_dir / f"daily_weight_adjustment_summary_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "daily_weight_adjustment_summary",
            "as_of": suffix,
            "production_effect": "none",
            "manual_review_only": True,
        },
    )


def _remove_comparison_inputs(context: dict[str, Path], as_of: date) -> None:
    for path in (
        context["data_root"]
        / "processed"
        / "decision_snapshots"
        / f"decision_snapshot_{as_of}.json",
        context["data_root"]
        / "derived"
        / "weight_iterations"
        / "shadow"
        / "current_shadow_weights.json",
        context["data_root"]
        / "derived"
        / "weight_iterations"
        / "shadow"
        / "candidates"
        / f"shadow_weight_candidate_{as_of}.json",
    ):
        if path.exists():
            path.unlink()


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    _write_json(
        metadata_path,
        {
            "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
            "as_of": as_of.isoformat(),
            "generated_at": _fixed_generated_at().isoformat(),
            "project_root": str(tmp_path),
            "status": "PASS",
            "started_at": _fixed_generated_at().isoformat(),
            "finished_at": _fixed_generated_at().isoformat(),
            "visibility_cutoff": "2026-05-19T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return metadata_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 19, 8, 0, tzinfo=UTC)
