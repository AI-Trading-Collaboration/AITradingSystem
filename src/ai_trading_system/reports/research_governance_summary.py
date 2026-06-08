from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "research_governance_summary"
PRODUCTION_EFFECT = "none"

GROUP_PRODUCTION_ACTIVE = "Production-active"
GROUP_APPROVED_INACTIVE = "Approved but inactive"
GROUP_SHADOW_OBSERVE = "Shadow observe-only"
GROUP_CANDIDATE_RESEARCH = "Candidate / research-only"
GROUP_BLOCKED = "Blocked / insufficient data"
GROUP_ROLLBACK_WARNING = "Rollback / warning"
GROUP_DOCUMENTATION = "Governance / documentation"

GROUP_ORDER = (
    GROUP_PRODUCTION_ACTIVE,
    GROUP_APPROVED_INACTIVE,
    GROUP_SHADOW_OBSERVE,
    GROUP_CANDIDATE_RESEARCH,
    GROUP_BLOCKED,
    GROUP_ROLLBACK_WARNING,
    GROUP_DOCUMENTATION,
)

BLOCKED_STATUS_TOKENS = (
    "BLOCKED",
    "INSUFFICIENT",
    "MISSING",
    "LOW_DATA_QUALITY",
    "UNAVAILABLE",
    "SOURCE_UNAVAILABLE",
    "FAILED",
    "FAIL",
    "NOT_PROMOTABLE",
)
ROLLBACK_STATUS_TOKENS = ("ROLLBACK", "WARNING")
GOVERNANCE_STATUS_OK = "OK"
GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS = "PASS_WITH_LIMITATIONS"
GOVERNANCE_STATUS_LIMITED_CONTEXT = "LIMITED_CONTEXT"
GOVERNANCE_STATUS_FAILED_VALIDATION = "FAILED_VALIDATION"

PROMOTION_STATUS_PROMOTABLE = "PROMOTABLE"
PROMOTION_STATUS_NOT_PROMOTABLE = "NOT_PROMOTABLE"
PROMOTION_STATUS_BLOCKED_MISSING = "BLOCKED_BY_MISSING_ARTIFACTS"
PROMOTION_STATUS_BLOCKED_MANUAL_REVIEW = "BLOCKED_BY_MANUAL_REVIEW"
PROMOTION_STATUS_BLOCKED_DATA_QUALITY = "BLOCKED_BY_DATA_QUALITY"


@dataclass(frozen=True)
class GovernanceArtifactSpec:
    artifact_id: str
    title: str
    source_task: str
    directory: Path
    filename_prefix: str
    filename_suffix: str
    default_group: str
    expected: bool = True


def default_research_governance_summary_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_governance_summary_{as_of.isoformat()}.json"


def default_research_governance_summary_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_governance_summary_{as_of.isoformat()}.md"


def default_research_governance_specs(
    project_root: Path = PROJECT_ROOT,
) -> tuple[GovernanceArtifactSpec, ...]:
    reports_dir = project_root / "outputs" / "reports"
    backtests_dir = project_root / "outputs" / "backtests"
    return (
        GovernanceArtifactSpec(
            "parameter_governance",
            "Parameter Governance",
            "aits feedback evaluate-parameter-governance",
            reports_dir,
            "parameter_governance_",
            ".json",
            GROUP_PRODUCTION_ACTIVE,
        ),
        GovernanceArtifactSpec(
            "report_index",
            "Report Index",
            "aits reports index",
            reports_dir,
            "report_index_",
            ".json",
            GROUP_DOCUMENTATION,
        ),
        GovernanceArtifactSpec(
            "documentation_contract",
            "Documentation Contract",
            "aits docs report-contract",
            reports_dir,
            "documentation_contract_",
            ".json",
            GROUP_DOCUMENTATION,
        ),
        GovernanceArtifactSpec(
            "sec_pit_backfill",
            "SEC PIT Backfill Validation",
            "aits sec-pit backfill",
            project_root / "outputs" / "reports" / "sec_pit_backfill",
            "sec_pit_validation_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "backtest_daily",
            "Daily Score Backtest",
            "aits backtest",
            backtests_dir,
            "backtest_",
            ".md",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "backtest_robustness",
            "Backtest Robustness",
            "aits backtest --robustness",
            backtests_dir,
            "backtest_robustness_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "gate_event_attribution",
            "Gate Event Attribution",
            "aits backtest gate-event-attribution",
            backtests_dir,
            "gate_event_attribution_",
            ".md",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_evaluation",
            "SEC PIT Evaluation",
            "aits sec-pit evaluate",
            project_root / "outputs" / "sec_pit_evaluation",
            "sec_pit_evaluation_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_baseline_comparison",
            "SEC PIT Baseline Comparison",
            "aits sec-pit compare-baseline",
            project_root / "outputs" / "sec_pit_baseline_comparison",
            "sec_pit_baseline_comparison_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_real_run_diagnostics",
            "SEC PIT Real Run Diagnostics",
            "aits sec-pit diagnose-run",
            project_root / "outputs" / "sec_pit_diagnostics",
            "sec_pit_real_run_diagnostics_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_candidate_review",
            "SEC PIT Candidate Review",
            "aits sec-pit review-candidates",
            project_root / "outputs" / "sec_pit_candidate_review",
            "sec_pit_candidate_review_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_baseline_coverage",
            "SEC PIT Baseline Coverage",
            "aits sec-pit audit-baseline-coverage",
            project_root / "outputs" / "sec_pit_baseline_coverage",
            "sec_pit_baseline_coverage_summary_",
            ".json",
            GROUP_BLOCKED,
        ),
        GovernanceArtifactSpec(
            "sec_pit_shadow_observe",
            "SEC PIT Shadow Observe",
            "aits sec-pit shadow-observe",
            project_root / "outputs" / "sec_pit_shadow_observe",
            "sec_pit_shadow_observe_summary_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "sec_pit_shadow_monitor",
            "SEC PIT Shadow Monitor",
            "aits sec-pit shadow-monitor",
            project_root / "outputs" / "sec_pit_shadow_monitor",
            "sec_pit_shadow_monitor_summary_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "shadow_parameter_impact",
            "Shadow Parameter Impact",
            "python scripts/run_shadow_parameter_impact.py",
            reports_dir,
            "shadow_parameter_impact_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "weight_adjustment_candidates",
            "Weight Adjustment Candidates",
            "python scripts/run_weight_adjustment_candidates.py",
            reports_dir,
            "weight_adjustment_candidates_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "weight_candidate_evaluation",
            "Weight Candidate Evaluation",
            "python scripts/run_weight_candidate_evaluation.py",
            reports_dir,
            "weight_candidate_evaluation_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "weight_promotion_gate",
            "Weight Promotion Gate",
            "python scripts/run_weight_promotion_gate.py",
            reports_dir,
            "weight_promotion_gate_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "daily_weight_adjustment_summary",
            "Daily Weight Adjustment Summary",
            "python scripts/run_daily_weight_adjustment.py",
            reports_dir,
            "daily_weight_adjustment_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
    )


def build_research_governance_summary_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    specs: tuple[GovernanceArtifactSpec, ...] | None = None,
) -> dict[str, Any]:
    resolved_specs = specs or default_research_governance_specs(project_root)
    cards = [_card_from_spec(spec, as_of) for spec in resolved_specs]
    payload_by_id = _payloads_by_card_id(cards)
    backtest = _backtest_section(cards=cards, payload_by_id=payload_by_id)
    weight_iteration = _weight_iteration_section(cards=cards, payload_by_id=payload_by_id)
    shadow_observe = _shadow_observe_section(cards=cards, payload_by_id=payload_by_id)
    sec_pit = _sec_pit_section(cards=cards, payload_by_id=payload_by_id)
    documentation = _documentation_section(cards=cards, payload_by_id=payload_by_id)
    source_artifacts = _consolidated_source_artifacts(cards)
    manual_review_queue = _manual_review_queue(
        cards=cards,
        backtest=backtest,
        weight_iteration=weight_iteration,
        shadow_observe=shadow_observe,
        sec_pit=sec_pit,
        documentation=documentation,
    )
    limitations = _limitations(
        cards=cards,
        backtest=backtest,
        weight_iteration=weight_iteration,
        shadow_observe=shadow_observe,
        sec_pit=sec_pit,
        documentation=documentation,
    )
    governance_status = _governance_status(
        cards=cards,
        backtest=backtest,
        weight_iteration=weight_iteration,
        documentation=documentation,
    )
    research_readiness = _research_readiness(
        governance_status=governance_status,
        backtest=backtest,
        shadow_observe=shadow_observe,
        sec_pit=sec_pit,
    )
    summary_text = _summary_text(
        governance_status=governance_status,
        research_readiness=research_readiness,
        promotion_status=_text(weight_iteration.get("promotion_status")),
        manual_review_required=bool(manual_review_queue),
        limitations=limitations,
    )
    groups = [
        {
            "group": group,
            "count": len([card for card in cards if card["group"] == group]),
            "cards": [card for card in cards if card["group"] == group],
        }
        for group in GROUP_ORDER
    ]
    missing = [card for card in cards if card["availability"] == "MISSING"]
    limited = [card for card in cards if card["availability"] == "LIMITED"]
    warning_cards = [
        card
        for card in cards
        if card["group"] == GROUP_ROLLBACK_WARNING or card["production_effect_risk"] is True
    ]
    status = "PASS"
    if missing or limited or warning_cards:
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "as_of_date": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "governance_status": governance_status,
        "research_readiness": research_readiness,
        "promotion_status": _text(weight_iteration.get("promotion_status")),
        "manual_review_required": bool(manual_review_queue),
        "production_effect": PRODUCTION_EFFECT,
        "summary_text": summary_text,
        "market_regime": {
            "regime_id": "ai_after_chatgpt",
            "start_date": "2022-12-01",
            "anchor_date": "2022-11-30",
        },
        "summary": {
            "card_count": len(cards),
            "missing_count": len(missing),
            "limited_count": len(limited),
            "warning_count": len(warning_cards),
            "production_effect_risk_count": len(
                [card for card in cards if card["production_effect_risk"] is True]
            ),
            "manual_review_required_count": len(
                [card for card in cards if card["manual_review_required"] is True]
            ),
            "groups": {item["group"]: item["count"] for item in groups},
            "governance_status": governance_status,
            "research_readiness": research_readiness,
            "promotion_status": _text(weight_iteration.get("promotion_status")),
            "manual_review_required": bool(manual_review_queue),
        },
        "executive_governance_status": {
            "governance_status": governance_status,
            "research_readiness": research_readiness,
            "promotion_status": _text(weight_iteration.get("promotion_status")),
            "manual_review_required": bool(manual_review_queue),
            "production_effect": PRODUCTION_EFFECT,
            "summary_text": summary_text,
        },
        "backtest": backtest,
        "weight_iteration": weight_iteration,
        "shadow_observe": shadow_observe,
        "sec_pit": sec_pit,
        "documentation": documentation,
        "manual_review_queue": manual_review_queue,
        "limitations": limitations,
        "source_artifacts": source_artifacts,
        "groups": groups,
        "cards": cards,
        "warnings": _warnings(cards),
        "methodology": {
            "collector_mode": "read_existing_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
            "group_order": list(GROUP_ORDER),
        },
    }


def write_research_governance_summary_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_research_governance_summary_report(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_research_governance_summary_markdown(payload), encoding="utf-8")
    return output_path


def render_research_governance_summary_markdown(payload: Mapping[str, Any]) -> str:
    as_of = _text(payload.get("as_of"), "UNKNOWN")
    summary = _mapping(payload.get("summary"))
    backtest = _mapping(payload.get("backtest"))
    weight_iteration = _mapping(payload.get("weight_iteration"))
    shadow_observe = _mapping(payload.get("shadow_observe"))
    sec_pit = _mapping(payload.get("sec_pit"))
    documentation = _mapping(payload.get("documentation"))
    manual_review_queue = _records(payload.get("manual_review_queue"))
    lines = [
        f"# Research Governance Summary {as_of}",
        "",
        "## Executive Summary",
        "",
        f"- governance_status：{_text(payload.get('governance_status'), 'UNKNOWN')}",
        f"- research_readiness：{_text(payload.get('research_readiness'), 'UNKNOWN')}",
        f"- promotion_status：{_text(payload.get('promotion_status'), 'UNKNOWN')}",
        f"- manual_review_required：{_text(payload.get('manual_review_required'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- 摘要：{_text(payload.get('summary_text'), 'UNKNOWN')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- 卡片数：{_text(summary.get('card_count'), '0')}；"
        f"缺失：{_text(summary.get('missing_count'), '0')}；"
        f"限制：{_text(summary.get('limited_count'), '0')}；"
        f"警告：{_text(summary.get('warning_count'), '0')}",
        "",
        "## Promotion Status",
        "",
        f"- promotion_status：{_text(weight_iteration.get('promotion_status'), 'UNKNOWN')}",
        f"- weight_promotion_gate_status："
        f"{_text(weight_iteration.get('weight_promotion_gate_status'), 'UNKNOWN')}",
        f"- candidate_count：{_text(weight_iteration.get('candidate_count'), 'UNKNOWN')}",
        f"- ready_for_review_count："
        f"{_text(weight_iteration.get('ready_for_review_count'), 'UNKNOWN')}",
        f"- promotable_count：{_text(weight_iteration.get('promotable_count'), 'UNKNOWN')}",
        f"- not_promotable_count："
        f"{_text(weight_iteration.get('not_promotable_count'), 'UNKNOWN')}",
        "",
        "## Backtest Status",
        "",
        f"- backtest_status：{_text(backtest.get('backtest_status'), 'UNKNOWN')}",
        f"- latest_backtest_date：{_text(backtest.get('latest_backtest_date'), 'UNKNOWN')}",
        f"- sample_count：{_text(backtest.get('sample_count'), 'UNKNOWN')}",
        f"- robustness_status：{_text(backtest.get('robustness_status'), 'UNKNOWN')}",
        f"- recommended_action：{_text(backtest.get('recommended_action'), 'UNKNOWN')}",
        "",
        "## Weight Iteration Status",
        "",
        _markdown_status_table(
            [
                (
                    "candidate_evaluation",
                    weight_iteration.get("weight_candidate_evaluation_status"),
                ),
                ("promotion_gate", weight_iteration.get("weight_promotion_gate_status")),
                ("production_effect", weight_iteration.get("production_effect")),
            ]
        ),
        "",
        "## Shadow Observe Status",
        "",
        _markdown_status_table(
            [
                ("active_shadow_lanes", shadow_observe.get("active_shadow_lanes")),
                ("observe_only_lanes", shadow_observe.get("observe_only_lanes")),
                (
                    "sec_pit_capex_intensity_lane_status",
                    shadow_observe.get("sec_pit_capex_intensity_lane_status"),
                ),
                ("shadow_monitor_status", shadow_observe.get("shadow_monitor_status")),
                ("rollback_recommended", shadow_observe.get("rollback_recommended")),
                ("production_effect", shadow_observe.get("production_effect")),
            ]
        ),
        "",
        "## SEC PIT Research Status",
        "",
        _markdown_status_table(
            [
                ("sec_pit_backfill_status", sec_pit.get("sec_pit_backfill_status")),
                ("sec_pit_evaluation_status", sec_pit.get("sec_pit_evaluation_status")),
                (
                    "sec_pit_baseline_comparison_status",
                    sec_pit.get("sec_pit_baseline_comparison_status"),
                ),
                ("sec_pit_diagnostics_status", sec_pit.get("sec_pit_diagnostics_status")),
                (
                    "sec_pit_candidate_review_status",
                    sec_pit.get("sec_pit_candidate_review_status"),
                ),
                ("sec_pit_shadow_observe_status", sec_pit.get("sec_pit_shadow_observe_status")),
                ("sec_pit_shadow_monitor_status", sec_pit.get("sec_pit_shadow_monitor_status")),
                ("pit_grade_policy", sec_pit.get("pit_grade_policy")),
                ("production_effect", sec_pit.get("production_effect")),
            ]
        ),
        "",
        "## Documentation / Registry Status",
        "",
        _markdown_status_table(
            [
                ("report_index_status", documentation.get("report_index_status")),
                (
                    "documentation_contract_status",
                    documentation.get("documentation_contract_status"),
                ),
                ("missing_report_count", documentation.get("missing_report_count")),
                ("stale_report_count", documentation.get("stale_report_count")),
                ("required_missing_count", documentation.get("required_missing_count")),
                ("documentation_warning_count", documentation.get("documentation_warning_count")),
                ("documentation_error_count", documentation.get("documentation_error_count")),
            ]
        ),
        "",
        "## Manual Review Queue",
        "",
    ]
    if manual_review_queue:
        lines.extend(
            [
                "| item_id | severity | category | reason | next action | decision impact |",
                "|---|---|---|---|---|---|",
            ]
        )
        for item in manual_review_queue:
            lines.append(
                "| "
                f"{_text(item.get('item_id'))} | "
                f"{_text(item.get('severity'))} | "
                f"{_text(item.get('category'))} | "
                f"{_text(item.get('reason'))} | "
                f"{_text(item.get('recommended_next_action'))} | "
                f"{_text(item.get('decision_impact'))} |"
            )
        lines.append("")
    else:
        lines.extend(["无。", ""])
    lines.extend(["## Source Artifacts", ""])
    for group in _records(payload.get("groups")):
        cards = _records(group.get("cards"))
        if not cards:
            continue
        lines.extend(
            [
                f"### {group['group']}",
                "",
                "| Artifact | Status | Candidate | Production effect | "
                "Manual review | Next action | Source |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for card in cards:
            lines.append(
                "| "
                f"{_text(card.get('title'))} | "
                f"{_text(card.get('artifact_status'), 'UNKNOWN')} | "
                f"`{_text(card.get('candidate_id'), 'UNKNOWN')}` | "
                f"`{_text(card.get('production_effect'), 'UNKNOWN')}` | "
                f"{_text(card.get('manual_review_required'), 'UNKNOWN')} | "
                f"{_text(card.get('next_action'), 'review_source_artifact')} | "
                f"`{Path(_text(card.get('path'))).name if card.get('path') else 'MISSING'}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## Limitations",
            "",
            *[f"- {_text(item)}" for item in _texts(payload.get("limitations"))],
            "",
            "## 审计边界",
            "",
            "- `Production-active` 只表示该 artifact 描述生产参数或生产 profile 状态；"
            "本报告本身仍固定 `production_effect=none`。",
            "- `Shadow observe-only`、`Candidate / research-only`、`Blocked / insufficient data` "
            "均不得被解读为 production promotion。",
            "- 缺少 artifact 时只标记 `MISSING`，不补造 research 结论。",
        ]
    )
    return "\n".join(lines)


def _card_from_spec(spec: GovernanceArtifactSpec, as_of: date) -> dict[str, Any]:
    path = _latest_artifact_path(spec.directory, spec.filename_prefix, spec.filename_suffix, as_of)
    exists = path.exists()
    payload = _read_json_or_empty(path) if exists and spec.filename_suffix == ".json" else {}
    availability = "AVAILABLE" if exists else "MISSING"
    if exists and spec.filename_suffix == ".json" and not payload:
        availability = "LIMITED"
    status = _artifact_status(payload, exists=exists)
    production_effect = _production_effect(payload, spec)
    manual_review_required = _manual_review_required(payload, spec)
    candidate_id = _candidate_id(payload, spec)
    group = _card_group(
        default_group=spec.default_group,
        status=status,
        payload=payload,
        exists=exists,
    )
    return {
        "card_id": spec.artifact_id,
        "title": spec.title,
        "group": group,
        "source_task": spec.source_task,
        "path": str(path),
        "exists": exists,
        "availability": availability,
        "artifact_status": status,
        "artifact_date": (
            parsed_date.isoformat()
            if (parsed_date := _date_from_path(path, spec.filename_prefix)) is not None
            else None
        ),
        "candidate_id": candidate_id,
        "production_effect": production_effect,
        "production_effect_risk": production_effect not in {"", "none", "read_only", "advisory"},
        "manual_review_required": manual_review_required,
        "evidence_summary": _evidence_summary(payload, spec, exists=exists),
        "next_review_date": _first_text(
            payload,
            "next_review_date",
            "next_review_after",
            "review_after",
        ),
        "next_action": _next_action(payload, status=status, exists=exists),
        "risk_flags": _risk_flags(payload, status=status, production_effect=production_effect),
        "source_artifacts": _source_artifacts(payload),
    }


def _payloads_by_card_id(cards: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for card in cards:
        path_text = _text(card.get("path"))
        path = Path(path_text) if path_text else Path()
        payloads[_text(card.get("card_id"))] = (
            _read_json_or_empty(path) if path.suffix == ".json" and path.exists() else {}
        )
    return payloads


def _backtest_section(
    *,
    cards: list[dict[str, Any]],
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    daily = _card_by_id(cards, "backtest_daily")
    robustness = _card_by_id(cards, "backtest_robustness")
    latest_date = _text(daily.get("artifact_date"))
    exists = bool(daily.get("exists"))
    limitations = _texts(_mapping(payload_by_id.get("backtest_daily")).get("limitations"))
    if not exists:
        limitations.append("missing backtest artifact")
    return {
        "backtest_status": _text(daily.get("artifact_status"), "MISSING"),
        "latest_backtest_date": latest_date,
        "sample_count": _first_non_empty(
            _mapping(payload_by_id.get("backtest_daily")).get("sample_count"),
            _mapping(payload_by_id.get("backtest_daily")).get("row_count"),
            "UNKNOWN" if exists else "",
        ),
        "return_summary": _first_non_empty(
            _mapping(payload_by_id.get("backtest_daily")).get("return_summary"),
            "SEE_SOURCE_ARTIFACT" if exists else "",
        ),
        "drawdown_summary": _first_non_empty(
            _mapping(payload_by_id.get("backtest_daily")).get("drawdown_summary"),
            "SEE_SOURCE_ARTIFACT" if exists else "",
        ),
        "robustness_status": _text(robustness.get("artifact_status"), "MISSING"),
        "impact_level": "INFO" if exists else "IMPORTANT",
        "recommended_action": (
            "review_latest_backtest_and_robustness"
            if exists
            else "rerun_after_scoring_or_gate_change"
        ),
        "production_effect": PRODUCTION_EFFECT,
        "limitations": _dedupe_texts(limitations),
        "source_artifacts": _artifact_records(
            (daily, robustness),
            payload_by_id=payload_by_id,
        ),
    }


def _weight_iteration_section(
    *,
    cards: list[dict[str, Any]],
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate = _card_by_id(cards, "weight_candidate_evaluation")
    gate = _card_by_id(cards, "weight_promotion_gate")
    adjustment = _card_by_id(cards, "weight_adjustment_candidates")
    candidate_payload = _mapping(payload_by_id.get("weight_candidate_evaluation"))
    gate_payload = _mapping(payload_by_id.get("weight_promotion_gate"))
    candidate_count = _first_value(
        gate_payload.get("candidate_count"),
        candidate_payload.get("candidate_count"),
        _mapping(payload_by_id.get("weight_adjustment_candidates")).get("candidate_count"),
        0 if not any(card.get("exists") for card in (candidate, gate, adjustment)) else "",
    )
    promotion_status = _promotion_status_from_gate(gate=gate, gate_payload=gate_payload)
    blocking_reasons = _dedupe_texts(
        [
            *_texts(candidate_payload.get("blocked_by")),
            *_texts(gate_payload.get("blocked_by")),
            *_texts(candidate_payload.get("blocking_reasons")),
            *_texts(gate_payload.get("blocking_reasons")),
            _text(candidate.get("next_action")) if not candidate.get("exists") else "",
            _text(gate.get("next_action")) if not gate.get("exists") else "",
        ]
    )
    return {
        "weight_candidate_evaluation_status": _text(
            candidate.get("artifact_status"),
            "MISSING",
        ),
        "weight_promotion_gate_status": _text(gate.get("artifact_status"), "MISSING"),
        "candidate_count": candidate_count,
        "ready_for_review_count": _first_value(
            gate_payload.get("ready_for_manual_review_count"),
            candidate_payload.get("ready_for_review_count"),
            0,
        ),
        "promotable_count": _first_value(gate_payload.get("promotable_count"), 0),
        "not_promotable_count": _first_value(
            gate_payload.get("blocked_count"),
            gate_payload.get("not_promotable_count"),
            0 if not gate.get("exists") else "",
        ),
        "latest_weight_iteration_date": _max_card_date((candidate, gate, adjustment)),
        "promotion_status": promotion_status,
        "production_effect": PRODUCTION_EFFECT,
        "blocking_reasons": blocking_reasons,
        "source_artifacts": _artifact_records(
            (adjustment, candidate, gate),
            payload_by_id=payload_by_id,
        ),
        "limitations": _dedupe_texts(
            [
                *_texts(candidate_payload.get("limitations")),
                *_texts(gate_payload.get("limitations")),
                "weight_promotion_gate missing blocks promotion" if not gate.get("exists") else "",
                (
                    "weight_candidate_evaluation missing limits candidate review"
                    if not candidate.get("exists")
                    else ""
                ),
            ]
        ),
    }


def _shadow_observe_section(
    *,
    cards: list[dict[str, Any]],
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    observe = _card_by_id(cards, "sec_pit_shadow_observe")
    monitor = _card_by_id(cards, "sec_pit_shadow_monitor")
    shadow_impact = _card_by_id(cards, "shadow_parameter_impact")
    observe_payload = _mapping(payload_by_id.get("sec_pit_shadow_observe"))
    monitor_payload = _mapping(payload_by_id.get("sec_pit_shadow_monitor"))
    observe_lanes = (
        [
            _first_non_empty(
                observe_payload.get("lane_id"),
                observe_payload.get("candidate_feature"),
                "sec_pit_capex_intensity_observe_only",
            )
        ]
        if observe.get("exists")
        else []
    )
    active_lanes = (
        [_first_non_empty(monitor_payload.get("candidate_feature"), "sec_pit_capex_intensity")]
        if monitor.get("exists")
        else []
    )
    return {
        "active_shadow_lanes": active_lanes,
        "observe_only_lanes": observe_lanes,
        "sec_pit_capex_intensity_lane_status": _first_non_empty(
            observe_payload.get("lane_status"),
            observe_payload.get("shadow_status"),
            observe.get("artifact_status"),
            "MISSING",
        ),
        "shadow_monitor_status": _first_non_empty(
            monitor_payload.get("monitor_status"),
            monitor_payload.get("monitoring_status"),
            monitor.get("artifact_status"),
            "MISSING",
        ),
        "rollback_recommended": bool(monitor_payload.get("rollback_recommended")),
        "warning_count": _first_value(monitor_payload.get("warning_count"), 0),
        "monitoring_status": _first_non_empty(
            observe_payload.get("monitoring_status"),
            monitor_payload.get("monitor_status"),
            "MISSING",
        ),
        "production_effect": PRODUCTION_EFFECT,
        "source_artifacts": _artifact_records(
            (observe, monitor, shadow_impact),
            payload_by_id=payload_by_id,
        ),
        "limitations": _dedupe_texts(
            [
                *_texts(observe_payload.get("limitations")),
                *_texts(monitor_payload.get("limitations")),
                "SEC PIT observe lane missing" if not observe.get("exists") else "",
                "SEC PIT shadow monitor missing" if not monitor.get("exists") else "",
            ]
        ),
    }


def _sec_pit_section(
    *,
    cards: list[dict[str, Any]],
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    backfill = _card_by_id(cards, "sec_pit_backfill")
    evaluation = _card_by_id(cards, "sec_pit_evaluation")
    comparison = _card_by_id(cards, "sec_pit_baseline_comparison")
    diagnostics = _card_by_id(cards, "sec_pit_real_run_diagnostics")
    candidate_review = _card_by_id(cards, "sec_pit_candidate_review")
    baseline_coverage = _card_by_id(cards, "sec_pit_baseline_coverage")
    observe = _card_by_id(cards, "sec_pit_shadow_observe")
    monitor = _card_by_id(cards, "sec_pit_shadow_monitor")
    payloads = [
        _mapping(payload_by_id.get(artifact_id))
        for artifact_id in (
            "sec_pit_backfill",
            "sec_pit_evaluation",
            "sec_pit_baseline_comparison",
            "sec_pit_real_run_diagnostics",
            "sec_pit_candidate_review",
            "sec_pit_baseline_coverage",
            "sec_pit_shadow_observe",
            "sec_pit_shadow_monitor",
        )
    ]
    return {
        "sec_pit_backfill_status": _text(backfill.get("artifact_status"), "MISSING"),
        "sec_pit_evaluation_status": _text(evaluation.get("artifact_status"), "MISSING"),
        "sec_pit_baseline_comparison_status": _text(
            comparison.get("artifact_status"),
            "MISSING",
        ),
        "sec_pit_diagnostics_status": _text(diagnostics.get("artifact_status"), "MISSING"),
        "sec_pit_candidate_review_status": _text(
            candidate_review.get("artifact_status"),
            "MISSING",
        ),
        "sec_pit_baseline_coverage_status": _text(
            baseline_coverage.get("artifact_status"),
            "MISSING",
        ),
        "sec_pit_shadow_observe_status": _text(observe.get("artifact_status"), "MISSING"),
        "sec_pit_shadow_monitor_status": _text(monitor.get("artifact_status"), "MISSING"),
        "pit_grade_policy": _first_non_empty(
            *[
                _text(_mapping(payload.get("safety")).get("pit_grade"))
                or _text(_mapping(payload.get("safety")).get("pit_grade_policy"))
                or _text(payload.get("pit_grade_policy"))
                or _text(payload.get("backtest_data_grade"))
                for payload in payloads
            ],
            "B_RECONSTRUCTED_SEC_FILING_PIT",
        ),
        "observe_only": any(
            _text(_mapping(payload.get("safety")).get("observe_only")).lower() == "true"
            or _text(payload.get("lane_status")).lower() == "observe_only"
            for payload in payloads
        ),
        "production_effect": PRODUCTION_EFFECT,
        "source_artifacts": _artifact_records(
            (
                backfill,
                evaluation,
                comparison,
                diagnostics,
                candidate_review,
                baseline_coverage,
                observe,
                monitor,
            ),
            payload_by_id=payload_by_id,
        ),
        "limitations": _dedupe_texts(
            [
                *[item for payload in payloads for item in _texts(payload.get("limitations"))],
                "SEC reconstructed PIT remains B_RECONSTRUCTED_SEC_FILING_PIT.",
                "SEC PIT shadow lanes are observe-only and production_effect=none.",
            ]
        ),
    }


def _documentation_section(
    *,
    cards: list[dict[str, Any]],
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    report_index = _card_by_id(cards, "report_index")
    contract = _card_by_id(cards, "documentation_contract")
    report_index_payload = _mapping(payload_by_id.get("report_index"))
    contract_payload = _mapping(payload_by_id.get("documentation_contract"))
    report_index_summary = _mapping(report_index_payload.get("summary"))
    contract_summary = _mapping(contract_payload.get("summary"))
    return {
        "report_index_status": _text(report_index.get("artifact_status"), "MISSING"),
        "documentation_contract_status": _text(contract.get("artifact_status"), "MISSING"),
        "missing_report_count": _first_value(report_index_summary.get("missing_count"), 0),
        "stale_report_count": _first_value(report_index_summary.get("stale_count"), 0),
        "required_missing_count": _first_value(
            report_index_summary.get("required_missing_count"),
            0,
        ),
        "documentation_warning_count": _first_value(
            contract_summary.get("warning_count"),
            0,
        ),
        "documentation_error_count": _first_value(contract_summary.get("error_count"), 0),
        "artifact_catalog_status": "DOCUMENTED",
        "report_registry_status": "DOCUMENTED",
        "production_effect": PRODUCTION_EFFECT,
        "source_artifacts": _artifact_records(
            (report_index, contract),
            payload_by_id=payload_by_id,
        )
        + [
            {
                "artifact_id": "artifact_catalog",
                "path": "docs/artifact_catalog.md",
                "exists": True,
                "status": "DOCUMENTATION",
                "availability": "AVAILABLE",
                "production_effect": PRODUCTION_EFFECT,
            },
            {
                "artifact_id": "report_registry",
                "path": "config/report_registry.yaml",
                "exists": True,
                "status": "DOCUMENTATION",
                "availability": "AVAILABLE",
                "production_effect": PRODUCTION_EFFECT,
            },
        ],
        "limitations": _dedupe_texts(
            [
                (
                    "report_index missing limits runtime freshness aggregation"
                    if not report_index.get("exists")
                    else ""
                ),
                (
                    "documentation_contract missing limits registry/catalog audit"
                    if not contract.get("exists")
                    else ""
                ),
            ]
        ),
    }


def _manual_review_queue(
    *,
    cards: list[dict[str, Any]],
    backtest: Mapping[str, Any],
    weight_iteration: Mapping[str, Any],
    shadow_observe: Mapping[str, Any],
    sec_pit: Mapping[str, Any],
    documentation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if _card_missing(cards, "weight_promotion_gate"):
        items.append(
            _review_item(
                "missing_weight_promotion_gate",
                "critical",
                "weight_iteration",
                "缺少 weight_promotion_gate，promotion 默认阻断。",
                "run_weight_promotion_gate_after_candidate_evaluation",
                "promotion_status=BLOCKED_BY_MISSING_ARTIFACTS",
                "weight_promotion_gate",
            )
        )
    if _card_missing(cards, "weight_candidate_evaluation"):
        items.append(
            _review_item(
                "missing_weight_candidate_evaluation",
                "warning",
                "weight_iteration",
                "缺少 weight_candidate_evaluation，候选权重复核上下文受限。",
                "run_weight_candidate_evaluation_before_promotion_gate",
                "无法确认 candidate_count / ready_for_review_count。",
                "weight_candidate_evaluation",
            )
        )
    if _text(backtest.get("backtest_status")) == "MISSING":
        items.append(
            _review_item(
                "missing_backtest",
                "warning",
                "backtest",
                "缺少 latest backtest artifact。",
                "rerun_after_scoring_or_gate_change",
                "研究证据可读性下降，不允许据此 promotion。",
                "backtest_daily",
            )
        )
    if _text(backtest.get("robustness_status")) == "MISSING":
        items.append(
            _review_item(
                "missing_backtest_robustness",
                "warning",
                "backtest",
                "缺少 backtest robustness artifact。",
                "rerun_before_promotion_review",
                "缺少稳健性证据，不允许把候选规则视为可晋级。",
                "backtest_robustness",
            )
        )
    monitor_status = _text(shadow_observe.get("shadow_monitor_status"))
    if monitor_status in {"", "MISSING", "LIMITED"}:
        items.append(
            _review_item(
                "missing_or_limited_sec_pit_monitor",
                "warning",
                "sec_pit",
                "SEC PIT shadow monitor 缺失或受限。",
                "run_or_review_sec_pit_shadow_monitor",
                "observe-only lane 风险监控上下文受限。",
                "sec_pit_shadow_monitor",
            )
        )
    if bool(shadow_observe.get("rollback_recommended")):
        items.append(
            _review_item(
                "sec_pit_rollback_review",
                "critical",
                "sec_pit",
                "SEC PIT shadow monitor 建议 rollback。",
                "owner_review_sec_pit_rollback_recommendation",
                "仅人工复核建议，不自动修改 production 或 shadow weights。",
                "sec_pit_shadow_monitor",
            )
        )
    if _int(documentation.get("documentation_error_count")):
        items.append(
            _review_item(
                "documentation_contract_errors",
                "critical",
                "documentation",
                "documentation contract 存在 ERROR。",
                "fix_registry_or_artifact_catalog_contract",
                "文档治理不完整，影响审计可读性。",
                "documentation_contract",
            )
        )
    if _int(documentation.get("required_missing_count")):
        items.append(
            _review_item(
                "required_report_missing",
                "critical",
                "documentation",
                "report_index 显示 required_for_daily_reading artifact 缺失。",
                "rerun_required_daily_reading_reports",
                "Reader Brief 结论上下文应视为受限。",
                "report_index",
            )
        )
    for card in cards:
        if card.get("manual_review_required") is True and card.get("exists") is True:
            items.append(
                _review_item(
                    f"manual_review:{_text(card.get('card_id'))}",
                    "warning",
                    "research_governance",
                    f"{_text(card.get('title'))} requires manual review.",
                    _text(card.get("next_action"), "review_source_artifact"),
                    "manual review only; no production effect.",
                    _text(card.get("card_id")),
                    source_path=_text(card.get("path")),
                )
            )
    deduped = {_text(item.get("item_id")): item for item in items}
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    return sorted(
        deduped.values(),
        key=lambda item: (
            severity_order.get(_text(item.get("severity")), 9),
            _text(item.get("item_id")),
        ),
    )


def _limitations(
    *,
    cards: list[dict[str, Any]],
    backtest: Mapping[str, Any],
    weight_iteration: Mapping[str, Any],
    shadow_observe: Mapping[str, Any],
    sec_pit: Mapping[str, Any],
    documentation: Mapping[str, Any],
) -> list[str]:
    values = [
        *[item for card in cards for item in _texts(card.get("risk_flags"))],
        *_texts(backtest.get("limitations")),
        *_texts(weight_iteration.get("limitations")),
        *_texts(shadow_observe.get("limitations")),
        *_texts(sec_pit.get("limitations")),
        *_texts(documentation.get("limitations")),
        "This summary is read-only and does not run upstream commands.",
        "production_effect=none.",
    ]
    return _dedupe_texts(values)


def _governance_status(
    *,
    cards: list[dict[str, Any]],
    backtest: Mapping[str, Any],
    weight_iteration: Mapping[str, Any],
    documentation: Mapping[str, Any],
) -> str:
    statuses = [_text(card.get("artifact_status")).upper() for card in cards]
    if any(status.startswith("FAIL") or "FAILED" in status for status in statuses):
        return GOVERNANCE_STATUS_FAILED_VALIDATION
    available_count = len([card for card in cards if card.get("exists") is True])
    if not available_count:
        return GOVERNANCE_STATUS_LIMITED_CONTEXT
    if (
        _text(backtest.get("backtest_status")) == "MISSING"
        and _text(weight_iteration.get("weight_promotion_gate_status")) == "MISSING"
        and _int(documentation.get("missing_report_count")) > 0
    ):
        return GOVERNANCE_STATUS_LIMITED_CONTEXT
    if any(card.get("availability") != "AVAILABLE" for card in cards):
        return GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS
    if _text(weight_iteration.get("promotion_status")) != PROMOTION_STATUS_PROMOTABLE:
        return GOVERNANCE_STATUS_PASS_WITH_LIMITATIONS
    return GOVERNANCE_STATUS_OK


def _research_readiness(
    *,
    governance_status: str,
    backtest: Mapping[str, Any],
    shadow_observe: Mapping[str, Any],
    sec_pit: Mapping[str, Any],
) -> str:
    if governance_status == GOVERNANCE_STATUS_FAILED_VALIDATION:
        return "BLOCKED"
    has_evidence = any(
        _text(value) not in {"", "MISSING", "UNKNOWN"}
        for value in (
            backtest.get("backtest_status"),
            shadow_observe.get("shadow_monitor_status"),
            sec_pit.get("sec_pit_evaluation_status"),
        )
    )
    return "READY_FOR_REVIEW" if has_evidence else "LIMITED_CONTEXT"


def _promotion_status_from_gate(
    *,
    gate: Mapping[str, Any],
    gate_payload: Mapping[str, Any],
) -> str:
    if not gate.get("exists"):
        return PROMOTION_STATUS_BLOCKED_MISSING
    raw = _first_non_empty(
        gate_payload.get("promotion_status"),
        gate_payload.get("promotion_gate_status"),
        gate_payload.get("gate_status"),
        gate.get("artifact_status"),
    ).upper()
    blockers = " ".join(_texts(gate_payload.get("blocked_by"))).upper()
    if "DATA" in raw and ("QUALITY" in raw or "GATE" in raw):
        return PROMOTION_STATUS_BLOCKED_DATA_QUALITY
    if "LOW_DATA_QUALITY" in blockers or "DATA_GATE" in blockers:
        return PROMOTION_STATUS_BLOCKED_DATA_QUALITY
    if "MISSING" in raw or "INSUFFICIENT" in raw:
        return PROMOTION_STATUS_BLOCKED_MISSING
    if "MANUAL" in raw or "REVIEW" in raw:
        return PROMOTION_STATUS_BLOCKED_MANUAL_REVIEW
    if "PROMOTABLE" in raw and "NOT" not in raw:
        return PROMOTION_STATUS_PROMOTABLE
    if "BLOCKED" in raw:
        return PROMOTION_STATUS_NOT_PROMOTABLE
    return PROMOTION_STATUS_NOT_PROMOTABLE


def _summary_text(
    *,
    governance_status: str,
    research_readiness: str,
    promotion_status: str,
    manual_review_required: bool,
    limitations: list[str],
) -> str:
    review_text = "需要人工复核" if manual_review_required else "当前没有强制人工复核项"
    limitation_text = "；存在限制项" if limitations else ""
    return (
        f"当前研究治理状态为 {governance_status}，"
        f"research_readiness={research_readiness}，"
        f"promotion_status={promotion_status}。"
        f"{review_text}{limitation_text}；本报告 production_effect=none。"
    )


def _consolidated_source_artifacts(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _artifact_records(tuple(cards), payload_by_id={})


def _artifact_records(
    cards: tuple[Mapping[str, Any], ...],
    *,
    payload_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records = []
    for card in cards:
        if not card:
            continue
        artifact_id = _text(card.get("card_id"))
        records.append(
            {
                "artifact_id": artifact_id,
                "title": _text(card.get("title")),
                "path": _text(card.get("path")),
                "exists": bool(card.get("exists")),
                "availability": _text(card.get("availability"), "UNKNOWN"),
                "status": _text(card.get("artifact_status"), "UNKNOWN"),
                "artifact_date": _text(card.get("artifact_date")),
                "production_effect": _text(card.get("production_effect"), PRODUCTION_EFFECT),
                "source_task": _text(card.get("source_task")),
                "source_artifacts": _source_artifacts(_mapping(payload_by_id.get(artifact_id))),
            }
        )
    return sorted(records, key=lambda item: _text(item.get("artifact_id")))


def _review_item(
    item_id: str,
    severity: str,
    category: str,
    reason: str,
    recommended_next_action: str,
    decision_impact: str,
    source_artifact: str,
    *,
    source_path: str = "",
) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "severity": severity,
        "category": category,
        "reason": reason,
        "recommended_next_action": recommended_next_action,
        "decision_impact": decision_impact,
        "source_artifact": source_artifact,
        "source_artifact_full_path": source_path,
        "production_effect": PRODUCTION_EFFECT,
    }


def _card_by_id(cards: list[dict[str, Any]], card_id: str) -> dict[str, Any]:
    for card in cards:
        if card.get("card_id") == card_id:
            return card
    return {
        "card_id": card_id,
        "title": card_id,
        "path": "",
        "exists": False,
        "availability": "MISSING",
        "artifact_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
    }


def _card_missing(cards: list[dict[str, Any]], card_id: str) -> bool:
    return _card_by_id(cards, card_id).get("exists") is not True


def _max_card_date(cards: tuple[Mapping[str, Any], ...]) -> str:
    dates = sorted(_text(card.get("artifact_date")) for card in cards if card.get("artifact_date"))
    return dates[-1] if dates else ""


def _dedupe_texts(values: list[object]) -> list[str]:
    return [item for item in dict.fromkeys(_text(value) for value in values if _text(value))]


def _markdown_status_table(rows: list[tuple[str, object]]) -> str:
    lines = ["| Field | Value |", "|---|---|"]
    lines.extend(f"| {field} | {_text(value, 'UNKNOWN')} |" for field, value in rows)
    return "\n".join(lines)


def _latest_artifact_path(directory: Path, prefix: str, suffix: str, as_of: date) -> Path:
    default_path = directory / f"{prefix}{as_of.isoformat()}{suffix}"
    if not directory.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in directory.glob(f"{prefix}*{suffix}"):
        if (
            prefix == "backtest_"
            and suffix == ".md"
            and not re.fullmatch(
                r"backtest_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.md",
                path.name,
            )
        ):
            continue
        parsed = _date_from_path(path, prefix)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _date_from_path(path: Path, prefix: str) -> date | None:
    raw = path.stem.removeprefix(prefix)
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", raw)
    if not matches:
        return None
    try:
        return date.fromisoformat(matches[-1])
    except ValueError:
        return None


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _artifact_status(payload: Mapping[str, Any], *, exists: bool) -> str:
    if not exists:
        return "MISSING"
    for key in (
        "status",
        "gate_status",
        "evaluation_status",
        "promotion_gate_status",
        "impact_status",
        "shadow_status",
        "monitor_status",
        "monitoring_status",
        "recommendation",
        "readiness",
    ):
        value = _text(payload.get(key))
        if value:
            return value
    return "AVAILABLE"


def _production_effect(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> str:
    if not payload:
        return PRODUCTION_EFFECT
    safety = _mapping(payload.get("safety"))
    execution = _mapping(payload.get("execution_boundary"))
    return _first_non_empty(
        payload.get("production_effect"),
        safety.get("production_effect"),
        execution.get("production_effect"),
        "none",
    )


def _manual_review_required(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> bool:
    if not payload:
        return spec.default_group in {
            GROUP_CANDIDATE_RESEARCH,
            GROUP_SHADOW_OBSERVE,
            GROUP_BLOCKED,
        }
    for key in ("manual_review_required", "manual_review_only", "requires_manual_review"):
        if key in payload:
            return bool(payload.get(key))
    if _records(payload.get("required_manual_review_items")):
        return True
    return spec.default_group in {GROUP_CANDIDATE_RESEARCH, GROUP_SHADOW_OBSERVE}


def _candidate_id(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> str:
    for key in (
        "candidate_id",
        "top_candidate_id",
        "selected_trial_id",
        "candidate_feature",
        "feature",
        "profile_id",
    ):
        value = _text(payload.get(key))
        if value:
            return value
    candidate = _mapping(payload.get("candidate"))
    profile = _mapping(payload.get("target_profile"))
    return _first_non_empty(
        candidate.get("candidate_id"),
        profile.get("profile_id"),
        spec.artifact_id,
    )


def _card_group(
    *,
    default_group: str,
    status: str,
    payload: Mapping[str, Any],
    exists: bool,
) -> str:
    upper_status = status.upper()
    if not exists or any(token in upper_status for token in BLOCKED_STATUS_TOKENS):
        return GROUP_BLOCKED
    if any(token in upper_status for token in ROLLBACK_STATUS_TOKENS):
        return GROUP_ROLLBACK_WARNING
    recommendation = _text(payload.get("recommendation")).upper()
    if "ROLLBACK" in recommendation or "WARNING" in recommendation:
        return GROUP_ROLLBACK_WARNING
    return default_group


def _evidence_summary(
    payload: Mapping[str, Any],
    spec: GovernanceArtifactSpec,
    *,
    exists: bool,
) -> str:
    if not exists:
        return "artifact missing"
    for key in (
        "summary",
        "evidence_summary",
        "recommendation",
        "main_blocked_by",
        "main_blocked_reason",
        "monitoring_status_reason",
        "status_reason",
        "primary",
        "readiness",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, Mapping):
            compact = _compact_mapping(value)
            if compact:
                return compact
    return f"{spec.title} artifact available"


def _next_action(payload: Mapping[str, Any], *, status: str, exists: bool) -> str:
    if not exists:
        return "generate_or_review_source_artifact"
    for key in ("next_action", "recommended_next_action", "recommendation", "next_step"):
        value = _text(payload.get(key))
        if value:
            return value
    required = _records(payload.get("required_manual_review_items"))
    if required:
        return "manual_review_required"
    if any(token in status.upper() for token in BLOCKED_STATUS_TOKENS):
        return "resolve_blocker_before_promotion"
    return "review_source_artifact"


def _risk_flags(
    payload: Mapping[str, Any],
    *,
    status: str,
    production_effect: str,
) -> list[str]:
    flags: list[str] = []
    if production_effect not in {"", "none", "read_only", "advisory"}:
        flags.append("production_effect_not_none")
    if any(token in status.upper() for token in BLOCKED_STATUS_TOKENS):
        flags.append("blocked_or_insufficient_data")
    if any(token in status.upper() for token in ROLLBACK_STATUS_TOKENS):
        flags.append("rollback_or_warning")
    for key in ("blocked_by", "warnings", "risk_flags"):
        value = payload.get(key)
        if isinstance(value, list):
            flags.extend(_text(item) for item in value if item is not None and _text(item))
        elif isinstance(value, str) and value:
            flags.append(value)
    return list(dict.fromkeys(flags))


def _source_artifacts(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("source_artifacts") or payload.get("source_inputs") or {}
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        return [
            {"artifact_id": str(key), **dict(value)}
            for key, value in raw.items()
            if isinstance(value, Mapping)
        ]
    return []


def _warnings(cards: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for card in cards:
        if card["availability"] == "MISSING":
            warnings.append(f"{card['card_id']}_missing:{card['path']}")
        if card["availability"] == "LIMITED":
            warnings.append(f"{card['card_id']}_limited:{card['path']}")
        if card["group"] == GROUP_ROLLBACK_WARNING:
            warnings.append(f"{card['card_id']}_rollback_or_warning:{card['artifact_status']}")
        if card["production_effect_risk"] is True:
            warnings.append(
                f"{card['card_id']}_production_effect_not_none:{card['production_effect']}"
            )
    return warnings


def _first_text(payload: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(payload.get(key))
        if value:
            return value
    return ""


def _first_non_empty(*values: object) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _first_value(*values: object) -> object:
    for value in values:
        if value is not None and value != "":
            return value
    return ""


def _compact_mapping(value: Mapping[str, Any]) -> str:
    items = []
    for key, raw in value.items():
        if isinstance(raw, (dict, list)):
            continue
        text = _text(raw)
        if text:
            items.append(f"{key}={text}")
        if len(items) >= 4:
            break
    return "；".join(items)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {None, ""}]


def _int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)
