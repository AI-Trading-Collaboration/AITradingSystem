from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.trading_engine.reports.parameter_promotion_report import (
    write_parameter_promotion_decision,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    render_shadow_backtest_markdown,
    validate_shadow_backtest_payload,
    write_shadow_backtest_summary,
)


def test_shadow_backtest_report_writes_stable_json_and_markdown(tmp_path: Path) -> None:
    payload = _summary_payload()
    json_path = tmp_path / "shadow_backtest_summary.json"
    markdown_path = tmp_path / "shadow_backtest_summary.md"

    write_shadow_backtest_summary(payload, json_path, markdown_path)

    written = json.loads(json_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == 1
    assert written["metadata"]["production_effect"] == "none"
    assert written["metadata"]["manual_review_required"] is True
    assert written["metadata"]["auto_promotion"] is False
    assert validate_shadow_backtest_payload(written) == []
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "# Shadow Parameter Backtest Summary" in markdown
    assert "## 8. Promotion Decision" in markdown
    assert "trend_momentum" in markdown


def test_shadow_backtest_markdown_includes_required_sections() -> None:
    markdown = render_shadow_backtest_markdown(_summary_payload())

    for title in (
        "## 1. Run Metadata",
        "## 3. Baseline vs Candidate",
        "## 4. Walk-forward Results",
        "## 5. Parameter Changes",
        "## 10. Input / Output Artifacts",
    ):
        assert title in markdown


def test_parameter_promotion_report_preserves_observe_only_boundary(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "report_type": "parameter_promotion_decision",
        "metadata": {
            "run_id": "shadow-backtest-2026-05-29",
            "generated_at": "2026-05-29T00:00:00+00:00",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "baseline_parameter_version": "production-test",
            "candidate_parameter_version": "shadow-test",
        },
        "promotion_decision": {
            "status": "watch",
            "reason": "Needs more validation windows.",
            "hard_rejections": [],
            "manual_review_items": ["criterion_failed:stability"],
        },
    }

    _, markdown_path = write_parameter_promotion_decision(
        payload,
        tmp_path / "parameter_promotion_decision.json",
        tmp_path / "parameter_promotion_decision.md",
    )

    markdown = markdown_path.read_text(encoding="utf-8")
    assert "production_effect：`none`" in markdown
    assert "auto_promotion：`False`" in markdown
    assert "不修改 production 参数" in markdown


def _summary_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "shadow_parameter_backtest",
        "metadata": {
            "run_id": "shadow-backtest-2026-05-29",
            "generated_at": "2026-05-29T00:00:00+00:00",
            "status": "OK",
            "market_regime": "ai_after_chatgpt",
            "date_range": "2026-01-01..2026-05-29",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "baseline_parameter_version": "production-test",
            "candidate_parameter_version": "shadow-test",
        },
        "input_artifacts": {"baseline_parameters": "config/parameters/production/current.yaml"},
        "output_artifacts": {"summary": "artifacts/shadow_backtest/2026-05-29"},
        "data_quality": {"status": "OK"},
        "baseline_result": {
            "annualized_return": 0.10,
            "max_drawdown": -0.10,
            "sharpe_ratio": 1.0,
            "turnover": 1.0,
        },
        "candidate_result": {
            "annualized_return": 0.13,
            "max_drawdown": -0.08,
            "sharpe_ratio": 1.1,
            "turnover": 1.1,
        },
        "relative_comparison": {
            "annualized_return_delta": 0.03,
            "max_drawdown_delta": 0.02,
            "sharpe_ratio_delta": 0.1,
            "turnover_delta": 0.1,
        },
        "parameter_changes": [
            {
                "name": "trend_momentum",
                "baseline": 0.25,
                "candidate": 0.30,
                "delta": 0.05,
                "reason": "Improved validation participation.",
                "risk": "May underperform in choppy markets.",
            }
        ],
        "walk_forward_windows": [
            {
                "window_id": "wf-001",
                "train_start": date(2026, 1, 1).isoformat(),
                "train_end": date(2026, 1, 5).isoformat(),
                "validation_start": date(2026, 1, 6).isoformat(),
                "validation_end": date(2026, 1, 8).isoformat(),
                "baseline_metrics": {"annualized_return": 0.10},
                "candidate_metrics": {"annualized_return": 0.13},
                "status": "PASS",
            }
        ],
        "promotion_decision": {
            "status": "candidate",
            "reason": "Candidate passed conservative criteria.",
            "hard_rejections": [],
            "manual_review_items": ["review_parameter_change:trend_momentum"],
        },
        "warnings": [],
    }
