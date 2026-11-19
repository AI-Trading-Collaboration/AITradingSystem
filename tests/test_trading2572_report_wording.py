"""TRADING-2572-02: the report modules read a full-exposure decision snapshot.

Acceptance tests (g) for the reader brief and the other report modules, and (n), of
knowledge/claude/research-review/full-exposure-default-requirements-2026-11-19.md (v1.3.1,
owner_decision:TRADING-2572:2026-11-19:full_exposure_default). In the full-exposure mode the final
band is the policy (1.0) and the gates only inform: a report prints the band the old rule would
give beside the final band, matches the binding gate against that old-rule band, and does not say
that a gate holds the final band down. A gated snapshot, and one written before TRADING-2572, reads
as before. The daily body is tested in test_trading2572_full_exposure_default.py (TRADING-2572-01).
Tests marked 【M5】 assume the total-asset band is held at 1.0 too.
"""

from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest
from test_daily_scoring import (
    _empty_risk_event_occurrence_review_report_with_attestation,
    _feature_set_with_macro_stress,
    _fundamental_feature_report,
)
from test_decision_causal_chains import _snapshot as _causal_snapshot
from test_decision_causal_chains import _write_trace_bundle
from test_evidence_dashboard import _write_dashboard_inputs
from test_reader_brief import _write_decision_snapshot as _write_reader_snapshot
from test_reader_brief import _write_reader_brief_inputs
from test_score_change_attribution import _write_decision_snapshot as _write_attribution_snapshot
from test_shadow_weight_profiles import _write_manifest as _write_shadow_manifest
from test_shadow_weight_profiles import _write_objective as _write_shadow_objective
from test_shadow_weight_profiles import _write_search_space as _write_shadow_search_space
from test_shadow_weight_profiles import _write_snapshot as _write_shadow_snapshot
from test_trading2572_full_exposure_default import DECISION, FULL, GATED, _report, _snapshot

from ai_trading_system.config import DEFAULT_SCORING_RULES_CONFIG_PATH, load_portfolio
from ai_trading_system.decision_causal_chains import (
    build_decision_causal_chain_ledger,
    render_decision_causal_chain_report,
)
from ai_trading_system.evidence_dashboard import (
    build_evidence_dashboard_payload,
    build_evidence_dashboard_report,
    render_evidence_dashboard,
)
from ai_trading_system.indicator_research import build_daily_indicator_weight_trace
from ai_trading_system.position_policy_view import (
    band_text,
    binding_reference_max,
    full_exposure_note,
    gated_band_record,
    is_full_exposure,
    position_policy_mode,
    snapshot_positions,
)
from ai_trading_system.report_traceability import build_daily_score_trace_bundle
from ai_trading_system.reports.calculation_explainers import (
    build_calculation_explainers_payload,
    write_calculation_explainers_json,
)
from ai_trading_system.reports.reader_brief import (
    build_reader_brief_payload,
    render_reader_brief_html,
)
from ai_trading_system.reports.score_change_attribution import (
    build_score_change_attribution_payload,
    render_score_change_attribution_markdown,
)
from ai_trading_system.shadow_weight_profiles import (
    build_shadow_parameter_search_report,
    build_shadow_weight_profile_run_report,
)

FULL_LABEL = "满仓（默认）"
FULL_TOTAL_RISK_LABEL = "总风险资产（满仓）"
POLICY_NOTE = "（满仓为底，闸门只提示；按原规则会给 {band}）"
OLD_RULE_BINDING = "（按原规则，只提示）"
INFO_ONLY_GATE = "（只提示，不压仓）"
POLICY_CHANGE = "（仓位政策变更，不是信号）"
CARD_BUDGET_ROW = "| 总风险资产预算 | 50%-70%（normal） |\n"
# Sentences that say a gate holds the final band down; none may appear in the full-exposure mode.
GATE_HOLDS_FINAL_WORDING = (
    "由 binding gate=",
    "最终仓位受 ",
    "除非人工确认",
    "受限结果",
    "仍是最终约束",
    "先确认当前最大约束",
)
AS_OF = date(2026, 5, 4)


def _note(band: str) -> str:
    return POLICY_NOTE.format(band=band)


def _full_exposure_positions(positions: dict[str, Any]) -> dict[str, Any]:
    """A positions block as the TRADING-2572-01 writer gives it in the full-exposure mode."""
    positions["gated_risk_asset_ai_band"] = positions["final_risk_asset_ai_band"]
    positions["final_risk_asset_ai_band"] = {
        "min_position": 1.0,
        "max_position": 1.0,
        "label": FULL_LABEL,
    }
    # 【M5】 the total-asset AI band and the total risk-asset band are held at 1.0 too.
    if "total_asset_ai_band" in positions:
        positions["gated_total_asset_ai_band"] = positions["total_asset_ai_band"]
        positions["total_asset_ai_band"] = {
            "min_position": 1.0,
            "max_position": 1.0,
            "label": FULL_LABEL,
        }
    if "final_total_risk_asset_band" in positions:
        positions["final_total_risk_asset_band"] = {
            "min_position": 1.0,
            "max_position": 1.0,
            "label": FULL_TOTAL_RISK_LABEL,
        }
    positions["position_policy"] = {"mode": FULL, "owner_decision": DECISION}
    for gate in positions.get("position_gates", []):
        gate["effect"] = "info_only"
    return positions


def _to_full_exposure(path: Path) -> Path:
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    _full_exposure_positions(snapshot["positions"])
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    return path


def _old_positions() -> dict[str, Any]:
    """A positions block written before TRADING-2572: no policy block, no gated bands."""
    return {
        "final_risk_asset_ai_band": {"min_position": 0.4, "max_position": 0.4, "label": "受限"},
        "total_asset_ai_band": {"min_position": 0.24, "max_position": 0.32, "label": "总资产"},
        "position_gates": [
            {"gate_id": "score_model", "max_position": 0.8, "triggered": True},
            {"gate_id": "valuation", "max_position": 0.4, "triggered": True},
        ],
    }


# The fixture converts a snapshot as the TRADING-2572-01 writer does ------------------------------


def test_g_fixture_converts_a_gated_snapshot_as_the_full_exposure_writer_does(
    tmp_path: Path,
) -> None:
    gated = _snapshot(_report(GATED), tmp_path)["positions"]
    full = _snapshot(_report(FULL), tmp_path)["positions"]
    converted = _full_exposure_positions(copy.deepcopy(gated))

    for key in (
        "final_risk_asset_ai_band",
        "gated_risk_asset_ai_band",
        "position_policy",
        "position_gates",
        # 【M5】
        "total_asset_ai_band",
        "gated_total_asset_ai_band",
        "final_total_risk_asset_band",
    ):
        assert converted[key] == full[key], key


# The shared reading of a snapshot's position policy ----------------------------------------------


def test_g_view_reads_an_old_snapshot_as_gated() -> None:
    positions = _old_positions()

    assert position_policy_mode(positions) == GATED
    assert is_full_exposure(positions) is False
    assert gated_band_record(positions) == positions["final_risk_asset_ai_band"]
    assert binding_reference_max(positions) == 0.4
    assert full_exposure_note(positions) == ""
    assert snapshot_positions({"positions": positions}) == positions
    assert snapshot_positions(None) == {}
    assert snapshot_positions({"positions": "not a mapping"}) == {}


def test_g_view_reads_a_full_exposure_snapshot_against_the_old_rule_band() -> None:
    positions = _full_exposure_positions(_old_positions())

    assert position_policy_mode(positions) == FULL
    assert is_full_exposure(positions) is True
    assert gated_band_record(positions)["max_position"] == 0.4
    # The binding gate is matched against 0.40, not against the policy's 1.0.
    assert binding_reference_max(positions) == 0.4
    assert full_exposure_note(positions) == _note("40%-40%")
    assert band_text(positions["final_risk_asset_ai_band"]) == "100%-100%"


def test_g_view_falls_back_to_the_final_band_and_rejects_bad_values() -> None:
    positions = _old_positions()
    positions["gated_risk_asset_ai_band"] = {"label": "no bounds"}
    positions["position_policy"] = {"mode": "", "owner_decision": ""}

    assert gated_band_record(positions) == positions["final_risk_asset_ai_band"]
    assert position_policy_mode(positions) == GATED
    assert band_text({"min_position": True, "max_position": 1.0}) == "UNKNOWN"
    assert band_text({}) == "UNKNOWN"
    assert binding_reference_max({}) is None


# (g) calculation explainers ----------------------------------------------------------------------


def _explainers(snapshot_path: Path, tmp_path: Path) -> dict[str, Any]:
    return build_calculation_explainers_payload(
        as_of=AS_OF,
        decision_snapshot_path=snapshot_path,
        scores_daily_path=tmp_path / "scores_daily.csv",
    )


def test_g_calculation_explainers_match_the_binding_gate_against_the_old_rule_band(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_reader_snapshot(tmp_path)
    gated = _explainers(snapshot_path, tmp_path)["metrics"]
    full = _explainers(_to_full_exposure(snapshot_path), tmp_path)["metrics"]

    gated_inputs = gated["final_position_max"]["input_values"]
    assert gated_inputs["binding_gate"]["gate_id"] == "valuation"
    assert "position_policy_mode" not in gated_inputs
    assert "position_policy_mode" not in gated["final_position_band"]["input_values"]

    full_inputs = full["final_position_max"]["input_values"]
    assert full["final_position_max"]["value"] == 1.0
    # Matched against the final 1.0 the gate would be none of the triggered ones.
    assert full_inputs["binding_gate"]["gate_id"] == "valuation"
    assert full_inputs["position_policy_mode"] == FULL
    assert full_inputs["gated_risk_asset_ai_band"]["max_position"] == 0.4
    band_inputs = full["final_position_band"]["input_values"]
    assert band_inputs["position_policy_mode"] == FULL
    assert band_inputs["gated_risk_asset_ai_band"]["max_position"] == 0.4


# (g) reader brief --------------------------------------------------------------------------------


def _reader_brief(tmp_path: Path, *, full: bool) -> tuple[dict[str, Any], str]:
    inputs = _write_reader_brief_inputs(tmp_path)
    if full:
        _to_full_exposure(inputs["snapshot"])
        write_calculation_explainers_json(
            _explainers(inputs["snapshot"], tmp_path), inputs["calculation_explainers"]
        )
        summary = json.loads(inputs["daily_decision_summary"].read_text(encoding="utf-8"))
        summary["investment_conclusion"]["position_band"] = "100%-100%"
        inputs["daily_decision_summary"].write_text(
            json.dumps(summary, ensure_ascii=False), encoding="utf-8"
        )
    payload = build_reader_brief_payload(
        as_of=AS_OF,
        reports_dir=tmp_path,
        decision_snapshot_path=inputs["snapshot"],
        calculation_explainers_path=inputs["calculation_explainers"],
        daily_decision_summary_path=inputs["daily_decision_summary"],
        evidence_dashboard_json_path=inputs["evidence_dashboard"],
        daily_task_dashboard_json_path=inputs["daily_task_dashboard"],
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace_bundle"],
        score_change_attribution_path=inputs["score_change_attribution"],
        market_panel_path=inputs["market_panel"],
        research_governance_summary_path=inputs["research_governance_summary"],
        report_index_path=inputs["report_index"],
        documentation_contract_path=inputs["documentation_contract"],
    )
    return payload, render_reader_brief_html(payload)


def _funnel_value(payload: dict[str, Any], metric_id: str) -> str:
    steps = payload["score_to_position_funnel"]["steps"]
    return str(next(step for step in steps if step["metric_id"] == metric_id)["current_value"])


def test_g_reader_brief_in_full_mode_prints_the_policy_and_the_old_rule_binding(
    tmp_path: Path,
) -> None:
    payload, html = _reader_brief(tmp_path, full=True)
    decision = payload["executive_decision"]
    text = html + json.dumps(payload, ensure_ascii=False)

    assert decision["final_risk_asset_ai_position"] == "100%-100%" + _note("40%-40%")
    assert decision["position_policy_mode"] == FULL
    assert decision["binding_gate_id"] == "valuation"
    assert payload["binding_gate_ladder"]["gates"][1]["binding"] is True
    binding = f"估值拥挤{OLD_RULE_BINDING}"
    assert f"<div>Binding Gate</div><strong>{binding}</strong>" in html
    assert f"<tr><th>最大限制</th><td>{binding}</td></tr>" in html
    assert f"<small>{binding}</small>" in html
    assert _funnel_value(payload, "position_gate").endswith(OLD_RULE_BINDING)
    assert _note("40%-40%") in _funnel_value(payload, "final_position_band")
    assert "仓位受限" not in decision["final_risk_asset_ai_position"]
    for phrase in GATE_HOLDS_FINAL_WORDING:
        assert phrase not in text, phrase
    narrative = payload["narrative_executive_summary"]
    assert "满仓为底" in narrative["binding_constraint"]
    assert "满仓为底" in payload["contribution_summary"]["binding_gate_vs_score_explanation"]
    assert "满仓为底" in payload["score_change_narrative"]["position_interpretation"]
    checklist = payload["action_checklist"]
    assert "满仓为底" in checklist[0]["action"]
    assert "满仓为底" in checklist[1]["rationale"]


def test_g_reader_brief_for_an_old_snapshot_keeps_the_old_wording(tmp_path: Path) -> None:
    payload, html = _reader_brief(tmp_path, full=False)
    decision = payload["executive_decision"]
    text = html + json.dumps(payload, ensure_ascii=False)

    assert decision["final_risk_asset_ai_position"] == "40%-60%"
    assert decision["binding_gate_id"] == "valuation"
    assert "position_policy_mode" not in decision
    assert _funnel_value(payload, "position_gate") == "估值拥挤 -> 40%"
    assert "满仓为底" not in text
    assert "按原规则" not in text
    narrative = payload["narrative_executive_summary"]
    assert "最终仓位受 估值拥挤 约束。" in narrative["binding_constraint"]
    assert "除非人工确认 估值拥挤 gate 可解除" in payload["action_checklist"][0]["action"]


# (g) score change attribution --------------------------------------------------------------------


def _attribution_snapshot(tmp_path: Path, day: date) -> Path:
    return _write_attribution_snapshot(
        tmp_path,
        day,
        overall_score=70.0,
        confidence_score=60.0,
        final_max=0.40,
        valuation_cap=0.40,
        trend_score=65.0,
        trend_weight=25.0,
        valuation_score=80.0,
        valuation_weight=25.0,
        market_data_status="PASS",
    )


def _attribution(tmp_path: Path, *, previous_full: bool, current_full: bool) -> dict[str, Any]:
    previous = _attribution_snapshot(tmp_path, date(2026, 5, 3))
    current = _attribution_snapshot(tmp_path, AS_OF)
    if previous_full:
        _to_full_exposure(previous)
    if current_full:
        _to_full_exposure(current)
    return build_score_change_attribution_payload(
        as_of=AS_OF,
        decision_snapshot_path=current,
        previous_decision_snapshot_path=previous,
    )


def test_g_attribution_marks_the_switch_day_as_a_policy_change(tmp_path: Path) -> None:
    payload = _attribution(tmp_path, previous_full=False, current_full=True)
    position = payload["position_attribution"]
    markdown = render_score_change_attribution_markdown(payload)

    assert payload["final_position_delta"]["max_delta"] == pytest.approx(0.6)
    assert position["previous_position_policy_mode"] == GATED
    assert position["current_position_policy_mode"] == FULL
    assert position["position_policy_changed"] is True
    assert position["previous_gated_max"] == 0.4
    assert position["current_gated_max"] == 0.4
    assert position["gated_max_delta"] == pytest.approx(0.0)
    assert payload["binding_gate_current"]["gate_id"] == "valuation"
    assert payload["binding_gate_changed"] is False
    assert (
        "（仓位政策变更：gated_min -> full_exposure_default，不是信号；按原规则 40% -> 40%）"
        in markdown
    )
    assert f"- Binding gate：valuation -> valuation{OLD_RULE_BINDING}" in markdown
    # The reader-facing sentence says why the final cap moved.
    assert f"- 最终仓位上限发生变化：40% -> 100% (+0.6000){POLICY_CHANGE}。" in markdown


def test_g_attribution_between_two_full_exposure_days_reads_the_old_rule_maxima(
    tmp_path: Path,
) -> None:
    payload = _attribution(tmp_path, previous_full=True, current_full=True)
    position = payload["position_attribution"]
    markdown = render_score_change_attribution_markdown(payload)

    assert payload["final_position_delta"]["max_delta"] == pytest.approx(0.0)
    assert position["position_policy_changed"] is False
    assert payload["binding_gate_previous"]["gate_id"] == "valuation"
    assert "（满仓为底，闸门只提示；按原规则 40% -> 40%）" in markdown
    assert "仓位政策变更" not in markdown


def test_g_attribution_between_two_old_snapshots_is_unchanged(tmp_path: Path) -> None:
    payload = _attribution(tmp_path, previous_full=False, current_full=False)
    markdown = render_score_change_attribution_markdown(payload)

    assert "current_position_policy_mode" not in payload["position_attribution"]
    assert "满仓为底" not in markdown
    assert "仓位政策变更" not in markdown
    assert "按原规则" not in markdown
    assert "- Binding gate：valuation -> valuation\n" in markdown


# (g) decision causal chains ----------------------------------------------------------------------


def _causal_markdown(
    tmp_path: Path, *, current_full: bool, previous_full: bool = False
) -> tuple[dict[str, Any], str]:
    trace_path = _write_trace_bundle(tmp_path)
    previous = _causal_snapshot("2026-04-01", 70.0, 80.0, 0.45, trace_path)
    current = _causal_snapshot("2026-04-02", 62.0, 65.0, 0.45, trace_path)
    if previous_full:
        _full_exposure_positions(cast(dict[str, Any], previous["positions"]))
    if current_full:
        _full_exposure_positions(cast(dict[str, Any], current["positions"]))
    outcomes = pd.DataFrame(
        [
            {
                "snapshot_id": "decision_snapshot:2026-04-02",
                "horizon_days": 5,
                "outcome_status": "AVAILABLE",
                "outcome_end_date": "2026-04-09",
                "ai_proxy_return": -0.03,
                "ai_proxy_max_drawdown": -0.05,
                "ai_proxy_realized_volatility": 0.30,
                "hit": False,
            }
        ]
    )
    ledger = build_decision_causal_chain_ledger(
        snapshots=(previous, current),
        outcomes=outcomes,
        generated_at=datetime(2026, 4, 10, tzinfo=UTC),
    )
    delta = ledger.chains[1]["signal_time_context"]["position_delta"]
    return delta, render_decision_causal_chain_report(ledger, tmp_path / "chains.jsonl")


def test_g_causal_chain_marks_the_switch_day_as_a_policy_change(tmp_path: Path) -> None:
    delta, markdown = _causal_markdown(tmp_path, current_full=True)

    assert delta["final_max_delta"] == pytest.approx(0.55)
    assert delta["position_policy_mode"] == FULL
    assert delta["previous_position_policy_mode"] == GATED
    assert delta["position_policy_changed"] is True
    assert delta["gated_max"] == 0.45
    assert delta["gated_max_delta"] == pytest.approx(0.0)
    assert POLICY_CHANGE in markdown


def test_g_causal_chain_between_two_full_exposure_days_is_not_a_policy_change(
    tmp_path: Path,
) -> None:
    delta, markdown = _causal_markdown(tmp_path, current_full=True, previous_full=True)

    assert delta["final_max_delta"] == pytest.approx(0.0)
    assert delta["position_policy_changed"] is False
    assert delta["gated_max_delta"] == pytest.approx(0.0)
    assert POLICY_CHANGE not in markdown


def test_g_causal_chain_between_two_old_snapshots_is_unchanged(tmp_path: Path) -> None:
    delta, markdown = _causal_markdown(tmp_path, current_full=False)

    assert "position_policy_mode" not in delta
    assert POLICY_CHANGE not in markdown


# (g) evidence dashboard --------------------------------------------------------------------------


def _dashboard(
    tmp_path: Path, *, full: bool, card_budget: bool = True
) -> tuple[dict[str, Any], str]:
    inputs = _write_dashboard_inputs(tmp_path)
    if full:
        _to_full_exposure(inputs["snapshot"])
    if not card_budget:
        daily = inputs["daily_report"].read_text(encoding="utf-8")
        assert CARD_BUDGET_ROW in daily
        inputs["daily_report"].write_text(daily.replace(CARD_BUDGET_ROW, ""), encoding="utf-8")
    report = build_evidence_dashboard_report(
        as_of=AS_OF,
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace"],
        decision_snapshot_path=inputs["snapshot"],
        alerts_report_path=inputs["alerts"],
        scores_daily_path=inputs["scores"],
    )
    return build_evidence_dashboard_payload(report), render_evidence_dashboard(report)


def test_g_evidence_dashboard_prints_the_old_rule_band_and_info_only_gates(
    tmp_path: Path,
) -> None:
    payload, html = _dashboard(tmp_path, full=True)
    final = payload["decision"]["final_risk_asset_ai_position"]

    assert final.startswith("100%-100%")
    assert final.endswith(_note("40%-60%"))
    assert "仓位受限" not in final
    assert f"最终 AI 100%-100%（{FULL_LABEL}）{_note('40%-60%')}" in html
    assert f"估值拥挤 上限 40%{INFO_ONLY_GATE}" in html
    # 【M5】 the total risk-asset band is the policy; the old rule's budget is the card's.
    assert (
        f"总风险资产 100%-100%（{FULL_TOTAL_RISK_LABEL}）（按原规则会给 50%-70%（normal））" in html
    )


def test_g_evidence_dashboard_without_a_card_budget_adds_no_budget_note(tmp_path: Path) -> None:
    _, html = _dashboard(tmp_path, full=True, card_budget=False)

    assert f"总风险资产 100%-100%（{FULL_TOTAL_RISK_LABEL}）" in html
    assert f"总风险资产 100%-100%（{FULL_TOTAL_RISK_LABEL}）（" not in html


def test_g_evidence_dashboard_for_an_old_snapshot_is_unchanged(tmp_path: Path) -> None:
    payload, html = _dashboard(tmp_path, full=False)

    assert payload["decision"]["final_risk_asset_ai_position"] == "40%-60%（中高配）"
    assert "满仓为底" not in html
    assert INFO_ONLY_GATE not in html
    assert "按原规则" not in html
    assert "估值拥挤 上限 40%: 估值分位过高。" in html


# (g) the daily trace bundle's claim --------------------------------------------------------------


def _claim(mode: str | None, tmp_path: Path, **overrides: Any) -> tuple[str, Any]:
    report = _report(mode, **overrides)
    bundle = build_daily_score_trace_bundle(
        report=report,
        report_path=tmp_path / "daily_score.md",
        data_quality_report_path=tmp_path / "data_quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features_daily.csv",
        scores_path=tmp_path / "scores_daily.csv",
        market_regime=None,
        config_paths={"scoring_rules": DEFAULT_SCORING_RULES_CONFIG_PATH},
    )
    claim = next(claim for claim in bundle.claims if claim["claim_id"].endswith("overall_position"))
    return str(claim["statement"]), report


def test_g_trace_claim_prints_the_old_rule_bands(tmp_path: Path) -> None:
    # Macro stress with the configured budget, as in -01 (d): the adjusted budget is not the static.
    portfolio = load_portfolio()
    statement, report = _claim(
        FULL,
        tmp_path,
        valuation=False,
        feature_set=_feature_set_with_macro_stress(
            dgs10_rate_change_20d=0.55, dollar_return_20d=0.01
        ),
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        macro_risk_asset_budget=portfolio.macro_risk_asset_budget,
        fundamental_feature_report=_fundamental_feature_report(),
        risk_event_occurrence_review_report=(
            _empty_risk_event_occurrence_review_report_with_attestation()
        ),
    )
    recommendation = report.recommendation
    gated = recommendation.gated_risk_asset_band
    gated_total = recommendation.gated_total_asset_band
    budget = report.macro_risk_asset_budget
    adjusted = budget.adjusted_total_risk_asset_band
    static = budget.static_total_risk_asset_band
    assert budget.level == "stress"
    assert (adjusted.min_position, adjusted.max_position) != (
        static.min_position,
        static.max_position,
    )

    assert statement.startswith(
        "最终 AI 仓位为 100%-100%"
        + _note(f"{gated.min_position:.0%}-{gated.max_position:.0%}")
        + "，"
    )
    # 【M5】 the total-asset AI band is held at 1.0 and prints the old rule's band.
    assert (
        "总资产内 AI 仓位为 100%-100%"
        f"（按原规则会给 {gated_total.min_position:.0%}-{gated_total.max_position:.0%}）"
    ) in statement
    # 【M5】 the total risk-asset band is held at 1.0; the old rule's is the macro-adjusted budget.
    assert (
        "宏观调整后总风险资产预算为 100%-100%"
        + _note(f"{adjusted.min_position:.0%}-{adjusted.max_position:.0%}")
        + "，"
    ) in statement
    assert "仓位受限" not in statement


def test_g_trace_claim_in_gated_mode_is_unchanged(tmp_path: Path) -> None:
    statement, report = _claim(None, tmp_path)
    recommendation = report.recommendation
    final = recommendation.risk_asset_ai_band
    total = recommendation.total_asset_ai_band

    assert statement.startswith(
        f"最终 AI 仓位为 {final.min_position:.0%}-{final.max_position:.0%}，"
    )
    assert f"总资产内 AI 仓位为 {total.min_position:.0%}-{total.max_position:.0%}，" in statement
    assert "满仓为底" not in statement
    assert "按原规则" not in statement


# (g) indicator research measures gate masking on the old-rule band --------------------------------


def _trace_rows(mode: str | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = build_daily_indicator_weight_trace(_report(mode))["rows"]
    return rows


def _component_rows(mode: str | None) -> list[dict[str, Any]]:
    return [row for row in _trace_rows(mode) if row["row_type"] == "indicator_component"]


def test_g_indicator_trace_measures_gate_masking_on_the_old_rule_band() -> None:
    full_rows = _component_rows(FULL)
    gated_rows = _component_rows(None)
    gated_band = _report(FULL).recommendation.gated_risk_asset_band

    assert full_rows
    assert len(full_rows) == len(gated_rows)
    for full_row, gated_row in zip(full_rows, gated_rows, strict=True):
        assert full_row["post_constraint_signal_weight"] == gated_band.max_position
        assert full_row["weight_after"] == gated_row["weight_after"]
        full_target = full_row["stages"]["post_all_constraints_target"]
        assert full_target == gated_row["stages"]["post_all_constraints_target"]
        assert full_target["max_position"] == gated_band.max_position
    # Every row, the non-component and the gate rows too, measures against the old-rule band.
    full_all = _trace_rows(FULL)
    gated_all = _trace_rows(None)
    assert len(full_all) == len(gated_all) > len(full_rows)
    for full_row, gated_row in zip(full_all, gated_all, strict=True):
        assert full_row["row_type"] == gated_row["row_type"]
        assert (
            full_row["post_constraint_signal_weight"]
            == (gated_row["post_constraint_signal_weight"])
        ), full_row["row_type"]


# (n) the shadow comparison reads the old-rule band -----------------------------------------------


def test_n_shadow_profiles_compare_against_the_old_rule_band(tmp_path: Path) -> None:
    gated_dir = tmp_path / "gated"
    full_dir = tmp_path / "full"
    gated_dir.mkdir()
    full_dir.mkdir()
    reports = []
    for directory, full in ((gated_dir, False), (full_dir, True)):
        snapshot_path = _write_shadow_snapshot(directory)
        if full:
            _to_full_exposure(snapshot_path)
        reports.append(
            build_shadow_weight_profile_run_report(
                as_of=date(2026, 5, 14),
                decision_snapshot_path=snapshot_path,
                manifest_path=_write_shadow_manifest(directory),
                generated_at=datetime.fromisoformat("2026-05-14T12:00:00+00:00"),
            )
        )
    gated_report, full_report = reports

    assert full_report.status == "PASS"
    full_observation = full_report.observations[0]
    assert full_observation.production_final_band["max_position"] == 0.4
    assert full_observation.production_final_band == (
        gated_report.observations[0].production_final_band
    )
    assert full_observation.shadow_final_band == gated_report.observations[0].shadow_final_band


def _shadow_search(directory: Path, *, full: bool) -> Any:
    snapshot_dir = directory / "snapshots"
    snapshot_dir.mkdir(parents=True)
    for day in ("2026-05-01", "2026-05-04"):
        path = _write_shadow_snapshot(
            directory,
            output_path=snapshot_dir / f"decision_snapshot_{day}.json",
            signal_date=day,
        )
        if full:
            _to_full_exposure(path)
    prices_path = directory / "prices.csv"
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)
    return build_shadow_parameter_search_report(
        run_id="trading2572_search",
        start=date(2026, 5, 1),
        end=date(2026, 5, 5),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=_write_shadow_search_space(directory),
        objective_path=_write_shadow_objective(directory),
        output_dir=directory / "search_output",
        horizon_days=1,
        cost_bps=5.0,
    )


def test_n_shadow_search_compares_against_the_old_rule_band(tmp_path: Path) -> None:
    gated = _shadow_search(tmp_path / "gated", full=False)
    full = _shadow_search(tmp_path / "full", full=True)

    assert full.status == "PASS"
    assert full.position_change_rows
    assert [row.production_position for row in full.position_change_rows] == [
        row.production_position for row in gated.position_change_rows
    ]
    assert full.best_trial is not None
    assert gated.best_trial is not None
    assert full.best_trial.trial_id == gated.best_trial.trial_id
    assert full.best_trial.excess_total_return == pytest.approx(
        gated.best_trial.excess_total_return
    )
