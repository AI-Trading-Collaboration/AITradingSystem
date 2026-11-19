"""TRADING-2572: the production daily holds the final band at 1.0 and the gates only inform.

Acceptance tests (a)-(m) and (o) of
knowledge/claude/research-review/full-exposure-default-requirements-2026-11-19.md (v1.3,
owner_decision:TRADING-2572:2026-11-19:full_exposure_default). The wording (g) is tested here for
the daily body, which scoring/daily.py renders; (g) for the reader brief and the other report
modules, and the shadow comparison (n), belong to TRADING-2572-02. Tests marked 【M5】 assume the
total-asset band is held at 1.0 too.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml
from test_daily_scoring import (
    _empty_risk_event_occurrence_review_report_with_attestation,
    _feature_set,
    _feature_set_with_macro_stress,
    _fundamental_feature_report,
    _position_gate,
    _quality_report,
    _sample_prices,
    _sample_rates,
    _valuation_review_report,
    _write_sec_companies_config,
    _write_sec_features_config,
    _write_sec_metrics_config,
    _write_sec_metrics_csv,
)
from test_historical_replay import _write_replay_fixture
from test_research_forward_ledger import PROVENANCE, _write_capture
from typer.testing import CliRunner

import ai_trading_system.backtest.daily as backtest_daily_module
import ai_trading_system.scoring.baseline_score_backfill as backfill_module
from ai_trading_system.alerts import AlertRecord, build_daily_alert_report
from ai_trading_system.belief_state import build_belief_state, render_belief_state_summary
from ai_trading_system.cli import app
from ai_trading_system.config import (
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    configured_price_tickers,
    configured_rate_series,
    load_features,
    load_portfolio,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.decision_snapshots import build_decision_snapshot, write_decision_snapshot
from ai_trading_system.historical_replay import run_historical_day_replay
from ai_trading_system.research_probes import forward_added as fa
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.rule_governance import (
    build_rule_version_manifest,
    load_rule_card_store,
    validate_rule_card_store,
)
from ai_trading_system.scoring.daily import (
    DailyScoreReport,
    PreviousDailyScoreSnapshot,
    build_daily_score_report,
    load_previous_daily_score_snapshot,
    render_daily_change_explanation,
    render_daily_review_questions_section,
    render_daily_score_report,
    write_scores_csv,
)

DECISION = "owner_decision:TRADING-2572:2026-11-19:full_exposure_default"
FULL = "full_exposure_default"
GATED = "gated_min"
CHANGE_NOTE = f"政策变更（{DECISION}），不是信号"
BAND_KEYS = {"min_position", "max_position", "label"}
NEW_POSITION_KEYS = {"gated_risk_asset_ai_band", "gated_total_asset_ai_band", "position_policy"}
# positions keys of a decision snapshot before TRADING-2572 (main 3e64d6f7).
OLD_POSITION_KEYS = {
    "model_risk_asset_ai_band",
    "final_risk_asset_ai_band",
    "confidence_adjusted_risk_asset_ai_band",
    "static_total_risk_asset_band",
    "final_total_risk_asset_band",
    "macro_risk_asset_budget",
    "total_asset_ai_band",
    "minimum_action_delta",
    "position_gates",
}
OLD_GATE_KEYS = {"gate_id", "label", "source", "max_position", "triggered", "reason"}
NEW_CSV_COLUMNS = ("gated_risk_asset_ai_min", "gated_risk_asset_ai_max", "position_policy_mode")


def _report(
    mode: str | None,
    *,
    valuation: bool = True,
    owner_decision: str | None = DECISION,
    **overrides: Any,
) -> DailyScoreReport:
    """A synthetic day; the valuation gate is EXTREME (percentile 96) unless valuation=False."""
    kwargs: dict[str, Any] = {
        "feature_set": _feature_set(),
        "data_quality_report": _quality_report(),
        "rules": load_scoring_rules(),
        "total_risk_asset_min": 0.60,
        "total_risk_asset_max": 0.80,
    }
    if valuation:
        kwargs["valuation_review_report"] = _valuation_review_report(
            valuation_percentile=96.0,
            overall_assessment="extreme",
        )
    kwargs.update(overrides)
    if mode is not None:
        kwargs["position_policy_mode"] = mode
        kwargs["position_policy_owner_decision"] = owner_decision
    return build_daily_score_report(**kwargs)


def _band(band: Any) -> tuple[float, float]:
    return (band.min_position, band.max_position)


def _record(band: dict[str, Any]) -> tuple[float, float]:
    return (band["min_position"], band["max_position"])


def _snapshot(report: DailyScoreReport, tmp_path: Path) -> dict[str, Any]:
    return build_decision_snapshot(
        report=report,
        trace_bundle_path=tmp_path / "daily_score_trace.json",
        market_regime=None,
        config_paths={"scoring_rules": DEFAULT_SCORING_RULES_CONFIG_PATH},
    )


def _quality(severity: Severity) -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 30),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(DataQualityIssue(severity=severity, code="test", message="测试数据质量问题。"),),
    )


# (a) backtest, backfill and the gated mode keep the old behaviour ---------------------------------


def test_a_production_rules_name_full_exposure_but_the_builder_defaults_to_gated() -> None:
    rules = load_scoring_rules()
    assert rules.position_policy.mode == FULL
    assert rules.position_policy.owner_decision == DECISION

    default = _report(None)
    explicit = _report(GATED)
    recommendation = default.recommendation
    assert recommendation.position_policy_mode == GATED
    assert default.position_policy_owner_decision is None
    assert _band(recommendation.risk_asset_ai_band) == _band(
        explicit.recommendation.risk_asset_ai_band
    )
    # The old rule: the EXTREME valuation gate caps the final band.
    assert recommendation.risk_asset_ai_band.max_position == (
        rules.position_gates.valuation.extreme_overheated_max_position
    )
    assert _band(recommendation.gated_risk_asset_band) == _band(recommendation.risk_asset_ai_band)
    assert _band(recommendation.gated_total_asset_band) == _band(recommendation.total_asset_ai_band)
    assert {gate.effect for gate in recommendation.position_gates} == {"cap"}
    assert recommendation.capping_position_gates == recommendation.triggered_position_gates


def test_a_gated_snapshot_and_csv_only_add_keys(tmp_path: Path) -> None:
    report = _report(None)
    positions = _snapshot(report, tmp_path)["positions"]

    assert set(positions) - NEW_POSITION_KEYS == OLD_POSITION_KEYS
    assert positions["position_policy"] == {"mode": GATED, "owner_decision": None}
    assert positions["gated_risk_asset_ai_band"] == positions["final_risk_asset_ai_band"]
    assert positions["gated_total_asset_ai_band"] == positions["total_asset_ai_band"]
    for gate in positions["position_gates"]:
        assert set(gate) - {"effect"} == OLD_GATE_KEYS
        assert gate["effect"] == "cap"

    scores_path = write_scores_csv(report, tmp_path / "scores_daily.csv")
    overall = pd.read_csv(scores_path).set_index("component").loc["overall"]
    assert overall["position_policy_mode"] == GATED
    assert overall["gated_risk_asset_ai_min"] == overall["final_risk_asset_ai_min"]
    assert overall["gated_risk_asset_ai_max"] == overall["final_risk_asset_ai_max"]
    assert overall["final_risk_asset_ai_max"] == pytest.approx(0.40)


def _spy(monkeypatch: pytest.MonkeyPatch, module: Any) -> list[DailyScoreReport]:
    reports: list[DailyScoreReport] = []
    real = module.build_daily_score_report

    def spy(*args: Any, **kwargs: Any) -> DailyScoreReport:
        report: DailyScoreReport = real(*args, **kwargs)
        reports.append(report)
        return report

    monkeypatch.setattr(module, "build_daily_score_report", spy)
    return reports


def test_a_backtest_with_production_rules_stays_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = _spy(monkeypatch, backtest_daily_module)
    universe = load_universe()
    result = backtest_daily_module.run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 20),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("QQQ",),
        cost_bps=5.0,
    )

    assert reports and result.rows
    for report in reports:
        recommendation = report.recommendation
        assert recommendation.position_policy_mode == GATED
        assert _band(recommendation.risk_asset_ai_band) == _band(
            recommendation.gated_risk_asset_band
        )
        assert {gate.effect for gate in recommendation.position_gates} == {"cap"}
    assert all(row.gated_target_exposure < 1.0 for row in result.rows)
    assert all(row.total_asset_gated_target_exposure < 1.0 for row in result.rows)


def test_a_backfill_with_production_rules_stays_gated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reports = _spy(monkeypatch, backfill_module)
    tickers = ["NVDA", "MSFT", "SPY", "QQQ", "SMH", "SOXX", "^VIX"]
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    _sample_prices(tickers, periods=260).to_csv(prices_path, index=False)
    _sample_rates(["DGS2", "DGS10", "DTWEXBGS"], periods=260).to_csv(rates_path, index=False)

    backfill_module.run_baseline_score_backfill(
        start=date(2026, 4, 27),
        end=date(2026, 4, 30),
        tickers=["NVDA", "MSFT"],
        prices_path=prices_path,
        rates_path=rates_path,
        output_path=tmp_path / "backfill" / "scores_daily.csv",
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio=load_portfolio(),
        data_quality_status="PASS",
        data_quality_report_path=tmp_path / "quality.md",
    )

    assert reports
    for report in reports:
        assert report.recommendation.position_policy_mode == GATED
        assert {gate.effect for gate in report.recommendation.position_gates} == {"cap"}


# (b) (c) (c2) (d) full-exposure mode ------------------------------------------------------------


def test_b_full_mode_holds_final_at_one_and_keeps_the_valuation_gate_as_information() -> None:
    rules = load_scoring_rules()
    full = _report(FULL)
    gated = _report(GATED)
    recommendation = full.recommendation

    assert recommendation.position_policy_mode == FULL
    assert full.position_policy_owner_decision == DECISION
    assert _band(recommendation.risk_asset_ai_band) == (1.0, 1.0)
    assert recommendation.risk_asset_ai_band.label == "满仓（默认）"
    extreme = rules.position_gates.valuation.extreme_overheated_max_position
    assert recommendation.gated_risk_asset_band.max_position == extreme
    assert _band(recommendation.gated_risk_asset_band) == _band(
        gated.recommendation.risk_asset_ai_band
    )
    # The score mapping and the confidence-adjusted band are written as before (S2).
    assert _band(recommendation.model_risk_asset_ai_band) == _band(
        gated.recommendation.model_risk_asset_ai_band
    )
    assert recommendation.total_score == gated.recommendation.total_score
    assert _band(full.confidence_assessment.adjusted_risk_asset_ai_band) == _band(
        gated.confidence_assessment.adjusted_risk_asset_ai_band
    )

    valuation = _position_gate(full, "valuation")
    assert valuation.triggered
    assert valuation.effect == "info_only"
    assert valuation.max_position == _position_gate(gated, "valuation").max_position
    assert {gate.effect for gate in recommendation.position_gates} == {"info_only"}
    assert [gate.gate_id for gate in recommendation.triggered_position_gates] == [
        gate.gate_id for gate in gated.recommendation.triggered_position_gates
    ]
    assert recommendation.capping_position_gates == ()


def test_c_data_fail_at_function_level_keeps_final_one_and_gated_zero() -> None:
    report = _report(FULL, data_quality_report=_quality(Severity.ERROR))

    assert report.data_quality_report.status == "FAIL"
    assert _band(report.recommendation.risk_asset_ai_band) == (1.0, 1.0)
    assert _band(report.recommendation.gated_risk_asset_band) == (0.0, 0.0)
    gate = _position_gate(report, "data_confidence")
    assert gate.triggered and gate.effect == "info_only"


def test_c2_data_warning_in_full_mode_is_information_only() -> None:
    rules = load_scoring_rules()
    report = _report(FULL, valuation=False, data_quality_report=_quality(Severity.WARNING))

    assert report.data_quality_report.status == "PASS_WITH_WARNINGS"
    assert _band(report.recommendation.risk_asset_ai_band) == (1.0, 1.0)
    warning_cap = rules.position_gates.data_confidence.data_quality_warning_max_position
    assert report.recommendation.gated_risk_asset_band.max_position <= warning_cap
    gate = _position_gate(report, "data_confidence")
    assert gate.max_position == warning_cap
    assert gate.effect == "info_only"


def test_d_macro_stress_total_asset_bands_are_one_and_gated_total_keeps_the_old_rule(
    tmp_path: Path,
) -> None:
    """【M5】 the total-asset band is held at 1.0 as well."""
    portfolio = load_portfolio()
    kwargs: dict[str, Any] = {
        "feature_set": _feature_set_with_macro_stress(
            dgs10_rate_change_20d=0.55,
            dollar_return_20d=0.01,
        ),
        "total_risk_asset_min": portfolio.portfolio.total_risk_asset_min,
        "total_risk_asset_max": portfolio.portfolio.total_risk_asset_max,
        "macro_risk_asset_budget": portfolio.macro_risk_asset_budget,
        "fundamental_feature_report": _fundamental_feature_report(),
        "risk_event_occurrence_review_report": (
            _empty_risk_event_occurrence_review_report_with_attestation()
        ),
    }
    full = _report(FULL, valuation=False, **kwargs)
    gated = _report(GATED, valuation=False, **kwargs)
    assert full.macro_risk_asset_budget.level == "stress"

    positions = _snapshot(full, tmp_path)["positions"]
    gated_positions = _snapshot(gated, tmp_path)["positions"]
    assert _record(positions["final_total_risk_asset_band"]) == (1.0, 1.0)
    assert _record(positions["total_asset_ai_band"]) == (1.0, 1.0)
    assert positions["gated_total_asset_ai_band"] == gated_positions["total_asset_ai_band"]
    assert _record(positions["gated_total_asset_ai_band"])[1] < 1.0
    assert (
        positions["static_total_risk_asset_band"] == gated_positions["static_total_risk_asset_band"]
    )
    assert positions["macro_risk_asset_budget"] == gated_positions["macro_risk_asset_budget"]

    # The daily body prints the old rule's budget: the macro-adjusted one, not the static one.
    adjusted = gated.recommendation.total_risk_asset_band
    static = gated.macro_risk_asset_budget.static_total_risk_asset_band
    assert adjusted.max_position < static.max_position
    adjusted_text = f"{adjusted.min_position:.0%}-{adjusted.max_position:.0%}"
    body = render_daily_score_report(
        full,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores_daily.csv",
    )
    assert (
        "- 股票/风险资产预算（总资产内）：100%-100%"
        f"（满仓为底，闸门只提示；按原规则会给 {adjusted_text}）"
    ) in body


# (e) configuration --------------------------------------------------------------------------------


def _rules_copy(tmp_path: Path, policy: dict[str, Any] | None) -> Path:
    raw = yaml.safe_load(DEFAULT_SCORING_RULES_CONFIG_PATH.read_text(encoding="utf-8"))
    if policy is None:
        raw.pop("position_policy")
    else:
        raw["position_policy"] = policy
    path = tmp_path / "scoring_rules.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def test_e_missing_policy_is_gated_and_an_unknown_mode_fails_to_load(tmp_path: Path) -> None:
    assert load_scoring_rules(_rules_copy(tmp_path, None)).position_policy.mode == GATED
    with pytest.raises(ValueError):
        load_scoring_rules(_rules_copy(tmp_path, {"mode": "half_exposure"}))
    with pytest.raises(ValueError):
        _report("half_exposure")


# (g) the daily body: the final line is the policy, the gated band is printed beside it ------------


# Wording that says a gate holds the final band down; none of it may appear in the full mode.
GATE_CAPS_FINAL_WORDING = (
    "仓位受限",
    "中高配但受限",
    "风险闸门后最终",
    "不得超过已触发闸门",
    "将最终仓位上限压到",
    "进一步压低仓位上限",
    "已触发仓位闸门解除",
    "闸门未解除",
    "导致 position_gate 上限低于当前最终区间",
)


def _daily_body(report: DailyScoreReport, tmp_path: Path) -> str:
    body = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores_daily.csv",
    )
    return (
        body
        + render_daily_review_questions_section(report)
        + render_daily_change_explanation(report)
    )


def test_g_daily_body_in_full_mode_prints_the_policy_and_the_gated_band(tmp_path: Path) -> None:
    body = _daily_body(_report(FULL), tmp_path)

    note = "（满仓为底，闸门只提示；按原规则会给 40%-40%）"
    assert f"- 最终 AI 仓位（股票风险资产内）：100%-100%{note}" in body
    assert f"- Final risk-asset AI position：100%-100%{note}" in body
    assert f"最终 AI 仓位为 100%-100%{note}；" in body
    # 【M5】 the total-asset AI band is 1.0; the old rule gives 0.40 x 0.60-0.80.
    assert "- AI 仓位（总资产内）：100%-100%（满仓为底，闸门只提示；按原规则会给 24%-32%）" in body
    # 【M5】 the total risk-asset budget is 1.0; the old rule gives the macro-adjusted budget.
    assert (
        "- 股票/风险资产预算（总资产内）：100%-100%（满仓为底，闸门只提示；按原规则会给 60%-80%）"
        in body
    )
    assert "满仓为底，没有 binding gate；先检查评分输入" in body
    assert "优先检查 binding gate" not in body
    assert "| 最终仓位（满仓为底） | 100%-100% |" in body
    assert "| 按原规则仓位（只提示） | 40%-40% |" in body
    assert "| 状态标签 | 中高配 |" in body
    assert "估值拥挤 按原规则会把上限压到 40%（只提示，不压仓）" in body
    assert "满仓为底，已触发的闸门只提示、不压仓（" in body
    assert "按原规则最终仓位会是 40%-40%。" in body
    assert "按原规则 binding 是 估值拥挤 (`valuation`) 40%，最终仓位会是 40%-40%。" in body
    assert "binding=无（满仓为底，闸门只提示；按原规则：估值拥挤 (`valuation`) 40%）" in body
    assert f"config/scoring_rules.yaml:position_policy（{DECISION}）" in body
    assert "| 估值拥挤 (`valuation`) | 40% | 是 | 否 |" in body
    assert "新增 L3 或可评分 active 风险事件（满仓为底下闸门只提示；按原规则会压低上限）" in body
    for wording in GATE_CAPS_FINAL_WORDING:
        assert wording not in body, wording


def test_g_daily_body_in_gated_mode_keeps_the_old_wording(tmp_path: Path) -> None:
    body = _daily_body(_report(GATED), tmp_path)

    assert body == _daily_body(_report(None), tmp_path)
    assert "- 最终 AI 仓位（股票风险资产内）：40%-40%\n" in body
    assert "| 风险闸门后最终仓位 | 40%-40% |" in body
    assert "| 状态标签 | 中高配但受限 |" in body
    assert "估值拥挤 将最终仓位上限压到 40%" in body
    assert "不主动加仓；最终仓位不得超过已触发闸门中的最严格上限" in body
    assert "binding=估值拥挤 (`valuation`) 40% |" in body
    assert "| 估值拥挤 (`valuation`) | 40% | 是 | 是 |" in body
    assert "已触发仓位闸门解除或上限不再低于模型区间" in body
    assert "- 股票/风险资产预算（总资产内）：60%-80%\n" in body
    assert "优先检查 binding gate 的来源和原因；当前 Main Invalidator：" in body
    for wording in ("满仓为底", "按原规则", "只提示"):
        assert wording not in body, wording


def test_g_daily_body_in_full_mode_without_a_capping_gate_reads_plainly(tmp_path: Path) -> None:
    body = _daily_body(_report(FULL, valuation=False), tmp_path)

    gated = _report(GATED, valuation=False).recommendation.risk_asset_ai_band
    gated_text = f"{gated.min_position:.0%}-{gated.max_position:.0%}"
    note = f"（满仓为底，闸门只提示；按原规则会给 {gated_text}）"
    assert f"- 最终 AI 仓位（股票风险资产内）：100%-100%{note}" in body
    assert f"| 按原规则仓位（只提示） | {gated_text} |" in body
    for wording in GATE_CAPS_FINAL_WORDING:
        assert wording not in body, wording


def _with_a_tied_gate(report: DailyScoreReport, cap: float) -> tuple[DailyScoreReport, str]:
    """Trigger one more gate (not valuation or score_model) at the binding cap."""
    gates = list(report.recommendation.position_gates)
    index = next(
        index
        for index, gate in enumerate(gates)
        if gate.gate_id not in {"valuation", "score_model"}
    )
    gates[index] = replace(gates[index], max_position=cap, triggered=True)
    recommendation = replace(report.recommendation, position_gates=tuple(gates))
    return replace(report, recommendation=recommendation), gates[index].gate_id


def _binding_row(report: DailyScoreReport) -> str:
    section = render_daily_review_questions_section(report)
    return next(line for line in section.splitlines() if "最终仓位由哪个 gate 决定" in line)


@pytest.mark.parametrize("mode", [FULL, GATED])
def test_g_tied_binding_gates_are_all_named(mode: str) -> None:
    report = _report(mode)
    tied, other = _with_a_tied_gate(
        report, report.recommendation.gated_risk_asset_band.max_position
    )

    row = _binding_row(tied)
    assert "`valuation`" in row
    assert f"`{other}`" in row


def test_g_first_full_run_without_a_previous_row_prints_no_policy_change(tmp_path: Path) -> None:
    assert "政策变更" not in _daily_body(_report(FULL), tmp_path)

    root = _score_daily(tmp_path / "first_run")
    assert "政策变更" not in (root / "daily_score.md").read_text(encoding="utf-8")


# (h) (k) the score-daily command ------------------------------------------------------------------


def _write_extreme_valuation(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "nvda_valuation_2026_04_30.yaml").write_text(
        "\n".join(
            [
                "snapshot_id: nvda_valuation_2026_04_30",
                "ticker: NVDA",
                "as_of: '2026-04-30'",
                "source_type: manual_input",
                "source_name: manual_sheet",
                "captured_at: '2026-04-30'",
                "valuation_metrics:",
                "  - metric_id: forward_pe",
                "    value: 36.0",
                "    unit: ratio",
                "    period: next_12m",
                "valuation_percentile: 96.0",
                "overall_assessment: extreme",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _seed_previous_row(scores_path: Path, mode: str | None) -> None:
    """A 2026-04-29 overall row: before the switch (no new columns) or a full-exposure day."""
    row: dict[str, Any] = {
        "as_of": "2026-04-29",
        "component": "overall",
        "score": 60.0,
        "confidence": 70.0,
        "confidence_level": "medium",
        "model_risk_asset_ai_min": 0.40,
        "model_risk_asset_ai_max": 0.60,
        "final_risk_asset_ai_min": 0.40,
        "final_risk_asset_ai_max": 0.40,
        "confidence_adjusted_risk_asset_ai_min": 0.40,
        "confidence_adjusted_risk_asset_ai_max": 0.60,
        "total_asset_ai_min": 0.24,
        "total_asset_ai_max": 0.32,
    }
    if mode is not None:
        row.update(
            {
                "final_risk_asset_ai_min": 1.0,
                "final_risk_asset_ai_max": 1.0,
                "total_asset_ai_min": 1.0,
                "total_asset_ai_max": 1.0,
                "gated_risk_asset_ai_min": 0.40,
                "gated_risk_asset_ai_max": 0.40,
                "position_policy_mode": mode,
            }
        )
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_csv(scores_path, index=False)


def _score_daily(
    root: Path, *extra: str, previous: bool = False, previous_mode: str | None = None
) -> Path:
    """Run score-daily for 2026-04-30 on synthetic inputs under root; returns root.

    With previous=True a 2026-04-29 row is seeded first (previous_mode None: before the switch).
    """
    universe = load_universe()
    root.mkdir(parents=True)
    _sample_prices(configured_price_tickers(universe), periods=260).to_csv(
        root / "prices_daily.csv", index=False
    )
    _sample_rates(configured_rate_series(universe), periods=260).to_csv(
        root / "rates_daily.csv", index=False
    )
    _write_sec_companies_config(root / "sec_companies.yaml")
    _write_sec_metrics_config(root / "fundamental_metrics.yaml")
    _write_sec_features_config(root / "fundamental_features.yaml")
    _write_sec_metrics_csv(root / "sec_fundamentals.csv")
    _write_extreme_valuation(root / "valuation_snapshots")
    if previous:
        _seed_previous_row(root / "scores_daily.csv", previous_mode)

    paths = {
        "--prices-path": "prices_daily.csv",
        "--rates-path": "rates_daily.csv",
        "--features-path": "features_daily.csv",
        "--scores-path": "scores_daily.csv",
        "--report-path": "daily_score.md",
        "--feature-report-path": "feature_summary.md",
        "--feature-availability-report-path": "feature_availability.md",
        "--quality-report-path": "quality.md",
        "--execution-policy-report-path": "execution_policy.md",
        "--portfolio-positions-path": "portfolio_positions.csv",
        "--portfolio-exposure-report-path": "portfolio_exposure.md",
        "--sec-companies-path": "sec_companies.yaml",
        "--sec-metrics-path": "fundamental_metrics.yaml",
        "--fundamental-feature-config-path": "fundamental_features.yaml",
        "--sec-fundamentals-path": "sec_fundamentals.csv",
        "--sec-fundamental-features-path": "sec_fundamental_features.csv",
        "--sec-fundamental-feature-report-path": "sec_fundamental_features.md",
        "--sec-metrics-validation-report-path": "sec_fundamentals_validation.md",
        "--valuation-path": "valuation_snapshots",
        "--risk-event-occurrences-path": "risk_event_occurrences",
        "--decision-snapshot-path": "decision_snapshot.json",
        "--calculation-explainers-path": "calculation_explainers.json",
        "--prediction-ledger-path": "prediction_ledger.csv",
        "--belief-state-path": "belief_state.json",
        "--belief-state-history-path": "belief_state_history.csv",
        "--alert-report-path": "alerts.md",
    }
    args = ["score-daily", "--skip-risk-event-openai-precheck", "--as-of", "2026-04-30"]
    args += ["--run-id", "daily_ops_run:2026-04-30:trading2572"]
    for option, name in paths.items():
        args += [option, str(root / name)]
    result = CliRunner().invoke(app, [*args, *extra], env={"COLUMNS": "160"}, terminal_width=160)
    assert result.exit_code == 0, result.output
    return root


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    end = text.find("\n## ", start + len(heading))
    return text[start:] if end == -1 else text[start:end]


def _change_line(text: str) -> str:
    return next(line for line in text.splitlines() if line.startswith("- 本期仓位变化："))


def test_h_k_score_daily_reads_the_mode_from_config_and_marks_the_switch_day(
    tmp_path: Path,
) -> None:
    root = _score_daily(tmp_path / "switch_day", previous=True)

    snapshot = json.loads((root / "decision_snapshot.json").read_text(encoding="utf-8"))
    positions = snapshot["positions"]
    assert positions["position_policy"] == {"mode": FULL, "owner_decision": DECISION}
    assert _record(positions["final_risk_asset_ai_band"]) == (1.0, 1.0)
    gates = {gate["gate_id"]: gate for gate in positions["position_gates"]}
    assert gates["valuation"]["triggered"] is True
    assert gates["valuation"]["effect"] == "info_only"
    assert _record(positions["gated_risk_asset_ai_band"])[1] <= gates["valuation"]["max_position"]

    overall = pd.read_csv(root / "scores_daily.csv")
    today = overall[(overall["component"] == "overall") & (overall["as_of"] == "2026-04-30")]
    assert today.iloc[0]["position_policy_mode"] == FULL
    assert today.iloc[0]["final_risk_asset_ai_max"] == 1.0

    text = (root / "daily_score.md").read_text(encoding="utf-8")
    advisory = _section(text, "## 执行建议")
    assert "禁止主动加仓" not in advisory
    assert "人工复核 gate" not in advisory
    assert CHANGE_NOTE in advisory
    assert CHANGE_NOTE in _change_line(text)


def test_k_two_full_days_in_a_row_print_no_policy_change(tmp_path: Path) -> None:
    root = _score_daily(tmp_path / "second_day", previous=True, previous_mode=FULL)

    text = (root / "daily_score.md").read_text(encoding="utf-8")
    assert "政策变更" not in text
    advisory = _section(text, "## 执行建议")
    assert "禁止主动加仓" not in advisory


def test_h_the_gated_flag_restores_the_old_daily(tmp_path: Path) -> None:
    # The previous row is from before the switch: gated on both days, so no policy change.
    root = _score_daily(tmp_path / "gated", "--position-policy-mode", GATED, previous=True)

    positions = json.loads((root / "decision_snapshot.json").read_text(encoding="utf-8"))[
        "positions"
    ]
    assert positions["position_policy"]["mode"] == GATED
    assert positions["final_risk_asset_ai_band"] == positions["gated_risk_asset_ai_band"]
    gates = {gate["gate_id"]: gate for gate in positions["position_gates"]}
    assert gates["valuation"]["triggered"] is True
    assert gates["valuation"]["effect"] == "cap"
    assert (
        positions["final_risk_asset_ai_band"]["max_position"] <= gates["valuation"]["max_position"]
    )
    text = (root / "daily_score.md").read_text(encoding="utf-8")
    advisory = _section(text, "## 执行建议")
    assert "禁止主动加仓 gate：" in advisory and "valuation" in advisory
    assert "政策变更" not in text


# (i) the forward ledger reads a full-exposure snapshot --------------------------------------------


def test_i_forward_ledger_and_added_lines_read_full_exposure_snapshots(tmp_path: Path) -> None:
    snapshot = _snapshot(_report(FULL), tmp_path)
    positions = snapshot["positions"]
    for key in ("final_risk_asset_ai_band", "model_risk_asset_ai_band", "total_asset_ai_band"):
        assert set(positions[key]) == BAND_KEYS

    daily_root = tmp_path / "daily"
    for day in fl.trading_days(date(2026, 10, 8), date(2026, 10, 23)):
        _write_capture(daily_root, day)
        path = daily_root / fl.SNAPSHOT_REL.format(day=day.isoformat())
        write_decision_snapshot({**snapshot, "signal_date": day.isoformat()}, path)

    band = fl.read_band(daily_root, date(2026, 10, 22))
    assert band is not None and band.mid == 1.0 and band.label == "满仓（默认）"
    ledger_dir = tmp_path / "forward"
    rows = fl.append(ledger_dir, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    assert rows and all(row["exp_daily"] == 1.0 for row in rows)
    assert fl.check_day(ledger_dir, daily_root, date(2026, 10, 22)) == []

    held = fa.held_snapshot(daily_root, date(2026, 10, 23))
    assert held is not None
    total, problem = fa.read_total(daily_root, held)
    assert problem is None and total is not None
    assert total.mid == 1.0  # 【M5】


# (j) historical replay uses the production mode of day D ------------------------------------------


def _production_snapshot_dir(project_root: Path) -> Path:
    return project_root / "data" / "processed" / "decision_snapshots"


def _old_format(snapshot: dict[str, Any]) -> dict[str, Any]:
    """The snapshot as main 3e64d6f7 wrote it: no position_policy, gated_* or gate effect."""
    positions = {
        key: value for key, value in snapshot["positions"].items() if key not in NEW_POSITION_KEYS
    }
    positions["position_gates"] = [
        {key: value for key, value in gate.items() if key != "effect"}
        for gate in positions["position_gates"]
    ]
    return {**snapshot, "positions": positions}


def _replay(project_root: Path, output_root: Path, tmp_path: Path) -> tuple[Any, list[str]]:
    modes: list[str] = []

    def runner(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if "score-daily" in command:
            mode = command[command.index("--position-policy-mode") + 1]
            modes.append(mode)
            target = Path(command[command.index("--decision-snapshot-path") + 1])
            write_decision_snapshot(_snapshot(_report(mode), tmp_path), target)
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=output_root,
        run_id=f"trading2572_{output_root.name}",
        runner=runner,
        compare_to_production=True,
        env={},
    )
    return replay, modes


def _core_status(replay: Any) -> str:
    assert replay.production_diff is not None
    statuses = {item.artifact_id: item.status for item in replay.production_diff.artifacts}
    status: str = statuses["decision_snapshot_core_fields"]
    return status


def test_j_replay_before_the_switch_runs_gated_and_matches(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    production = _old_format(_snapshot(_report(None), tmp_path))
    write_decision_snapshot(
        production, _production_snapshot_dir(project_root) / "decision_snapshot_2026-05-08.json"
    )

    replay, modes = _replay(project_root, tmp_path / "before", tmp_path)

    assert modes == [GATED]
    assert _core_status(replay) == "MATCH"


def test_j_replay_after_the_switch_runs_full_exposure_and_matches(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    write_decision_snapshot(
        _snapshot(_report(FULL), tmp_path),
        _production_snapshot_dir(project_root) / "decision_snapshot_2026-05-08.json",
    )

    replay, modes = _replay(project_root, tmp_path / "after", tmp_path)

    assert modes == [FULL]
    assert _core_status(replay) == "MATCH"


def test_j_replay_without_a_day_d_snapshot_uses_the_nearest_earlier_mode(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    directory = _production_snapshot_dir(project_root)
    (directory / "decision_snapshot_2026-05-08.json").unlink()
    full = _snapshot(_report(FULL), tmp_path)
    write_decision_snapshot(_old_format(full), directory / "decision_snapshot_2026-05-06.json")
    write_decision_snapshot(full, directory / "decision_snapshot_2026-05-07.json")
    # A later snapshot is not visible on day D.
    write_decision_snapshot(
        _snapshot(_report(GATED), tmp_path), directory / "decision_snapshot_2026-05-10.json"
    )

    _, modes = _replay(project_root, tmp_path / "earlier_full", tmp_path)
    assert modes == [FULL]

    (directory / "decision_snapshot_2026-05-07.json").unlink()
    _, modes = _replay(project_root, tmp_path / "earlier_old", tmp_path)
    assert modes == [GATED]


def test_j_replay_skips_an_unreadable_newest_snapshot(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    directory = _production_snapshot_dir(project_root)
    (directory / "decision_snapshot_2026-05-08.json").unlink()
    write_decision_snapshot(
        _snapshot(_report(FULL), tmp_path), directory / "decision_snapshot_2026-05-07.json"
    )
    (directory / "decision_snapshot_2026-05-08.json").write_text("{not json", encoding="utf-8")

    _, modes = _replay(project_root, tmp_path / "unreadable", tmp_path)
    assert modes == [FULL]


# (k) the policy-change sentence and the previous row ----------------------------------------------


def test_k_previous_row_reads_the_new_columns_and_old_rows_leave_them_empty(
    tmp_path: Path,
) -> None:
    scores_path = tmp_path / "scores_daily.csv"
    _seed_previous_row(scores_path, None)
    old = load_previous_daily_score_snapshot(scores_path, date(2026, 4, 30))
    assert old is not None
    assert old.gated_risk_asset_ai_max is None and old.position_policy_mode is None

    write_scores_csv(_report(FULL), scores_path)
    previous = load_previous_daily_score_snapshot(scores_path, date(2026, 5, 1))
    assert previous is not None and previous.as_of == date(2026, 4, 30)
    assert previous.position_policy_mode == FULL
    assert previous.final_risk_asset_ai_max == 1.0
    assert previous.gated_risk_asset_ai_max == pytest.approx(0.40)


# (l) alerts -------------------------------------------------------------------------------------


def _previous(
    *, final_max: float, gated_max: float | None, mode: str | None
) -> PreviousDailyScoreSnapshot:
    return PreviousDailyScoreSnapshot(
        as_of=date(2026, 4, 29),
        overall_score=60.0,
        confidence_score=70.0,
        confidence_level="medium",
        component_scores={},
        component_confidence_scores={},
        model_risk_asset_ai_min=0.40,
        model_risk_asset_ai_max=0.60,
        final_risk_asset_ai_min=final_max,
        final_risk_asset_ai_max=final_max,
        confidence_adjusted_risk_asset_ai_min=0.40,
        confidence_adjusted_risk_asset_ai_max=0.60,
        total_asset_ai_min=None,
        total_asset_ai_max=None,
        triggered_position_gates=None,
        gated_risk_asset_ai_min=gated_max,
        gated_risk_asset_ai_max=gated_max,
        position_policy_mode=mode,
    )


def _alerts(
    report: DailyScoreReport, previous: PreviousDailyScoreSnapshot | None
) -> dict[str, AlertRecord]:
    alert_report = build_daily_alert_report(report, previous_score_snapshot=previous)
    return {alert.alert_id.rsplit(":", 1)[-1]: alert for alert in alert_report.alerts}


def test_l_full_mode_gate_alerts_are_information_only() -> None:
    alerts = _alerts(_report(FULL), None)
    valuation = alerts["position_gate_valuation"]
    assert "只提示" in valuation.title
    assert valuation.severity == "info"

    gated = _alerts(_report(GATED), None)["position_gate_valuation"]
    assert gated.title == f"{_position_gate(_report(GATED), 'valuation').label} 触发仓位上限"
    assert gated.severity != "info"


def test_l_cap_drop_compares_gated_bands_on_both_sides() -> None:
    report = _report(FULL)
    assert report.recommendation.gated_risk_asset_band.max_position == pytest.approx(0.40)

    dropped = _alerts(report, _previous(final_max=1.0, gated_max=0.80, mode=FULL))
    assert dropped["position_cap_drop"].title == "按原规则：AI 仓位上限大幅下降"
    assert dropped["position_cap_drop"].severity == "high"

    steady = _alerts(report, _previous(final_max=1.0, gated_max=0.40, mode=FULL))
    assert "position_cap_drop" not in steady

    switch_day = _alerts(report, _previous(final_max=0.40, gated_max=None, mode=None))
    assert "position_cap_drop" not in switch_day

    # An old row has no gated band: its final band is the old rule's band.
    from_old_row = _alerts(report, _previous(final_max=0.80, gated_max=None, mode=None))
    assert from_old_row["position_cap_drop"].title == "按原规则：AI 仓位上限大幅下降"

    old_rule = _alerts(_report(GATED), _previous(final_max=0.80, gated_max=None, mode=None))
    assert old_rule["position_cap_drop"].title == "最终 AI 仓位上限大幅下降"


# (m) rule cards ---------------------------------------------------------------------------------


def test_m_rule_cards_validate_and_name_the_full_exposure_policy() -> None:
    store = load_rule_card_store()
    report = validate_rule_card_store(store, as_of=date(2026, 11, 20))
    assert report.passed
    cards = {card.rule_id: card for card in store.cards}

    shared = cards["position_gate.shared_caps.v1"]
    assert "score-daily" not in shared.applies_to
    assert "backtest" in shared.applies_to

    policy = cards["position_policy.full_exposure_default.v1"]
    assert policy.status == "production"
    assert policy.rule_type == "position_gate"
    assert policy.applies_to == ["score-daily"]
    assert [str(path) for path in policy.source_config_paths] == ["config/scoring_rules.yaml"]
    assert policy.approval.approval_status == "approved"
    assert DECISION in policy.approval.rationale
    assert policy.validation.validation_refs
    assert "gated_min" in policy.rollback.procedure

    manifest = build_rule_version_manifest(report, applies_to="score-daily")
    rule_ids = {rule["rule_id"] for rule in manifest["rules"]}
    assert "position_policy.full_exposure_default.v1" in rule_ids
    assert "position_gate.shared_caps.v1" not in rule_ids


# (o) the gated band is in every snapshot ----------------------------------------------------------


@pytest.mark.parametrize("mode", [GATED, FULL])
def test_o_every_snapshot_keeps_a_gated_band_with_a_midpoint(mode: str, tmp_path: Path) -> None:
    positions = _snapshot(_report(mode), tmp_path)["positions"]
    for key in ("gated_risk_asset_ai_band", "gated_total_asset_ai_band"):
        band = positions[key]
        assert set(band) == BAND_KEYS
        low, high = _record(band)
        assert 0.0 <= low <= high <= 1.0
        assert isinstance((low + high) / 2, float)


def test_o_full_mode_gated_band_equals_the_gated_final_band(tmp_path: Path) -> None:
    full = _snapshot(_report(FULL), tmp_path)["positions"]
    gated = _snapshot(_report(GATED), tmp_path)["positions"]
    assert full["gated_risk_asset_ai_band"] == gated["final_risk_asset_ai_band"]
    assert full["gated_total_asset_ai_band"] == gated["total_asset_ai_band"]
    assert full["position_policy"] == {"mode": FULL, "owner_decision": DECISION}


# belief state (requirement 2.8) -----------------------------------------------------------------


def test_belief_state_keeps_the_gated_band_and_says_it_only_informs(tmp_path: Path) -> None:
    def state(report: DailyScoreReport) -> tuple[dict[str, Any], str]:
        belief = build_belief_state(
            report=report,
            trace_bundle_path=tmp_path / "trace.json",
            decision_snapshot_path=tmp_path / "decision_snapshot.json",
            market_regime=None,
            config_paths={},
        )
        return belief, render_belief_state_summary(belief, tmp_path / "belief_state.json")

    full, full_text = state(_report(FULL))
    gated, gated_text = state(_report(GATED))
    boundary = full["position_boundary"]
    assert _record(boundary["final_risk_asset_ai_band"]) == (1.0, 1.0)
    assert (
        boundary["gated_risk_asset_ai_band"]
        == gated["position_boundary"]["final_risk_asset_ai_band"]
    )
    assert boundary["position_policy"]["mode"] == FULL
    assert {gate["effect"] for gate in boundary["limiting_gates"]} == {"info_only"}
    assert "按原规则" in full_text and "只提示" in full_text
    assert "按原规则" not in gated_text


def test_gated_band_property_falls_back_to_the_final_band() -> None:
    recommendation = replace(
        _report(GATED).recommendation,
        gated_risk_asset_ai_band=None,
        gated_total_asset_ai_band=None,
    )
    assert recommendation.gated_risk_asset_band == recommendation.risk_asset_ai_band
    assert recommendation.gated_total_asset_band == recommendation.total_asset_ai_band
