from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    write_csv_rows,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_new_candidate_family_prioritization"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2301_FIRST_LAYER_NEW_CANDIDATE_FAMILY_RESEARCH_BACKLOG_AND_FEASIBILITY_AUDIT"
REPORT_TYPE = "first_layer_new_candidate_family_prioritization"
MODE = "first_layer_new_candidate_family_prioritization"
STATUS = "FIRST_LAYER_NEW_CANDIDATE_FAMILY_BACKLOG_READY_PROMOTION_BLOCKED"
ARTIFACT_ROLE = "first_layer_new_candidate_family_prioritization"

SAFETY_FIELDS: dict[str, Any] = {
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "candidate_generation_allowed": False,
    "actual_path_validation_executed": False,
    "forward_observe_runtime_started": False,
    "production_effect": "none",
    "research_only": True,
}

SCORE_DIMENSIONS: tuple[dict[str, Any], ...] = (
    {
        "dimension": "portfolio_relevance",
        "weight": 20,
        "description": "是否直接服务于 QQQ / SMH / AI / high-beta 风险暴露。",
    },
    {
        "dimension": "pit_data_feasibility",
        "weight": 20,
        "description": "是否可获得历史数据、时间戳、可复现，是否避免未来函数。",
    },
    {
        "dimension": "expected_marginal_value",
        "weight": 20,
        "description": "相对现有 volatility risk-cap 是否提供新增信息。",
    },
    {
        "dimension": "validation_clarity",
        "weight": 15,
        "description": "是否能定义清晰的 actual-path validation 标签。",
    },
    {
        "dimension": "implementation_tractability",
        "weight": 10,
        "description": "是否能较快接入现有 generator / validator / report pipeline。",
    },
    {
        "dimension": "overfit_risk_control",
        "weight": 10,
        "description": "是否容易被参数搜索或样本选择误导。",
    },
    {
        "dimension": "forward_observe_suitability",
        "weight": 5,
        "description": "是否适合进入 observe-only evidence collection。",
    },
)

STANDARD_VALIDATION_PATH: tuple[str, ...] = (
    "candidate_family_spec",
    "pit_data_feasibility_audit",
    "candidate_bound_generator",
    "actual_path_validation",
    "inconclusive_diagnostics",
    "scope_narrowing",
    "forward_observe_readiness",
)


@dataclass(frozen=True)
class CandidateFamilyPriority:
    rank: int | None
    family_id: str
    title: str
    family_type: str
    score: int
    priority: str
    legal_task_id: str
    legacy_alias: str
    recommended_next_step: str
    primary_use: tuple[str, ...]
    not_recommended_as: tuple[str, ...]
    possible_inputs: tuple[str, ...]
    validation_targets: tuple[str, ...]
    major_risks: tuple[str, ...]
    pit_policy: str
    source_schema_status: str
    implementation_route: str
    rationale: str


FAMILY_PRIORITIES: tuple[CandidateFamilyPriority, ...] = (
    CandidateFamilyPriority(
        rank=0,
        family_id="volatility_risk_cap_forward_observe",
        title="Volatility risk-cap forward observe",
        family_type="retained_mainline",
        score=87,
        priority="P0",
        legal_task_id="TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN",
        legacy_alias="TRADING-2294 mainline",
        recommended_next_step="risk-cap forward observe runtime evidence and cap mechanics plan",
        primary_use=(
            "risk_cap_only",
            "veto_only",
            "exposure_limiter",
            "cooldown_trigger_candidate",
        ),
        not_recommended_as=("return_predictor", "buy_sell_signal", "broker_action_signal"),
        possible_inputs=("TRADING-2293 readiness outputs", "TRADING-2292 risk-cap evidence"),
        validation_targets=(
            "risk-cap trigger interpretation",
            "observe-only evidence collection",
            "cooldown / decay / cap release behavior",
        ),
        major_risks=(
            "trigger sample remains sparse",
            "cap / cooldown may miss rebound windows",
            "runtime semantics can overfit if evidence rules proliferate",
        ),
        pit_policy="validated_prior_artifacts",
        source_schema_status="source_artifacts_validated_with_warnings",
        implementation_route="mainline_forward_observe_runtime_design",
        rationale="唯一通过 scope-narrowed validation 的 candidate，定位为 risk-cap / veto。",
    ),
    CandidateFamilyPriority(
        rank=1,
        family_id="breadth_participation",
        title="Breadth / participation",
        family_type="new_candidate_family",
        score=83,
        priority="P1",
        legal_task_id="TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC",
        legacy_alias="TRADING-2294B",
        recommended_next_step="data feasibility + candidate spec",
        primary_use=(
            "trend_quality_filter",
            "trend_confirmation",
            "trend_fragility_warning",
            "risk_on_quality_confirmation",
        ),
        not_recommended_as=("primary_directional_signal", "broker_action_signal"),
        possible_inputs=(
            "QQQ / SMH / SPY component participation proxy",
            "20d constituent return distribution",
            "above moving average ratio",
            "new high / new low proxy",
            "equal-weight vs cap-weight relative strength",
        ),
        validation_targets=(
            "active breadth confirmation vs inactive records",
            "narrow breadth drawdown / fragility warning",
            "false trend confirmation reduction",
            "false risk-on reduction",
        ),
        major_risks=(
            "survivorship bias",
            "PIT constituent membership incomplete",
            "overlap with price momentum",
            "mega-cap driven market false warnings",
        ),
        pit_policy="pit_required_or_pit_approximation",
        source_schema_status="proxy_source_until_true_breadth_approved",
        implementation_route="first_new_family_after_backlog",
        rationale="最适合作为失败 trend confirmation 的替代方向。",
    ),
    CandidateFamilyPriority(
        rank=2,
        family_id="ai_semiconductor_leadership",
        title="AI / semiconductor leadership",
        family_type="new_candidate_family",
        score=80,
        priority="P1",
        legal_task_id=(
            "TRADING-2303_AI_SEMICONDUCTOR_LEADERSHIP_CANDIDATE_FAMILY_SPEC"
        ),
        legacy_alias="TRADING-2295B",
        recommended_next_step="candidate family spec",
        primary_use=(
            "SMH_exposure_confirmation",
            "AI_chain_trend_quality_confirmation",
            "semiconductor_leadership_weakening_warning",
            "SMH_overweight_cap_modifier",
        ),
        not_recommended_as=("all_market_risk_on_signal", "generic_risk_appetite_signal"),
        possible_inputs=(
            "SMH vs QQQ",
            "SMH vs SPY",
            "NVDA vs SMH",
            "AI core basket vs QQQ",
            "semiconductor basket breadth",
            "software vs semiconductor relative strength",
        ),
        validation_targets=(
            "SMH forward risk/reward improvement",
            "SMH drawdown early warning",
            "QQQ / SMH exposure-cap marginal value",
        ),
        major_risks=(
            "basket hindsight selection",
            "NVDA concentration",
            "overlap with SMH price momentum",
            "earnings / capex PIT complexity",
        ),
        pit_policy="price_relative_strength_first_then_event_pit_audit",
        source_schema_status="price_proxy_ready_event_sources_pending",
        implementation_route="second_new_family_after_breadth",
        rationale="最贴合 QQQ / SMH / AI 暴露，比泛化 risk appetite 更具体。",
    ),
    CandidateFamilyPriority(
        rank=3,
        family_id="liquidity_rates_pressure",
        title="Liquidity / rates pressure",
        family_type="new_candidate_family",
        score=77,
        priority="P1/P2",
        legal_task_id=(
            "TRADING-2304_LIQUIDITY_RATES_PRESSURE_PROXY_AUDIT_AND_CANDIDATE_SPEC"
        ),
        legacy_alias="TRADING-2296B",
        recommended_next_step="proxy audit + PIT design",
        primary_use=(
            "liquidity_supportive_filter",
            "duration_asset_pressure_warning",
            "risk_on_confirmation",
            "maximum_exposure_limiter",
            "risk_cap_modifier",
        ),
        not_recommended_as=("1d_direction_predictor", "standalone_buy_sell_signal"),
        possible_inputs=(
            "TLT / IEF / SHY",
            "UUP or DXY proxy",
            "HYG vs LQD",
            "credit spread proxy",
            "10Y yield proxy",
            "2Y yield proxy",
            "real-rate proxy",
        ),
        validation_targets=(
            "QQQ / SMH drawdown risk under rates pressure",
            "risk-on reliability under liquidity supportive state",
            "max exposure limiter usefulness",
        ),
        major_risks=(
            "macro horizon is slower",
            "overlap with volatility risk-cap",
            "rates can be overwhelmed by earnings cycle",
            "ETF proxy may not represent true macro variable",
        ),
        pit_policy="price_proxy_available_macro_timestamp_audit_required",
        source_schema_status="proxy_source_with_pit_audit_required",
        implementation_route="third_new_family_after_breadth_and_leadership",
        rationale="数据 proxy 相对容易，但 horizon 较慢且可能与 volatility risk-cap 重叠。",
    ),
    CandidateFamilyPriority(
        rank=4,
        family_id="regime_state_machine",
        title="Regime state machine",
        family_type="diagnostic_selector_layer",
        score=72,
        priority="P2",
        legal_task_id="TRADING-2305_REGIME_STATE_MACHINE_DIAGNOSTIC_LABEL_FRAMEWORK",
        legacy_alias="TRADING-2297B",
        recommended_next_step="regime label framework",
        primary_use=(
            "diagnostic_segmentation",
            "candidate_usage_selector",
            "validation_stratification",
            "risk_cap_interpretation_modifier",
        ),
        not_recommended_as=("direct_strategy_signal", "primary_candidate"),
        possible_inputs=(
            "uptrend / late_uptrend / pullback labels",
            "drawdown / panic / rebound labels",
            "range_bound / high_volatility / low_volatility labels",
        ),
        validation_targets=(
            "candidate validity by regime",
            "risk-cap false signal concentration",
            "breadth / leadership regime interaction",
        ),
        major_risks=(
            "regime labels can become hindsight labels",
            "state transitions can overfit",
            "signal generation use could introduce lookahead risk",
        ),
        pit_policy="diagnostic_only_pit_transition_rules_required",
        source_schema_status="label_framework_not_model_ready",
        implementation_route="diagnostic_layer_after_p1_family_specs",
        rationale="更适合作为 validation / interpretation layer。",
    ),
    CandidateFamilyPriority(
        rank=5,
        family_id="event_calendar_gating",
        title="Event calendar gating",
        family_type="gating_layer",
        score=65,
        priority="P2",
        legal_task_id="TRADING-2306_EVENT_CALENDAR_GATING_FEASIBILITY_AUDIT",
        legacy_alias="TRADING-2298B",
        recommended_next_step="PIT event calendar audit",
        primary_use=(
            "pre_event_no_add",
            "post_event_confirmation_window",
            "event_risk_mode",
            "manual_review_trigger",
            "earnings_cluster_risk",
        ),
        not_recommended_as=("event_outcome_predictor", "direct_return_predictor"),
        possible_inputs=(
            "FOMC",
            "CPI",
            "PCE",
            "NFP",
            "NVDA earnings",
            "major AI / semiconductor earnings cluster",
            "TSM monthly revenue window",
        ),
        validation_targets=(
            "pre-event no-add false risk-on reduction",
            "post-event confirmation window usefulness",
            "earnings cluster volatility / drawdown risk",
        ),
        major_risks=(
            "event outcome is unknowable",
            "event importance varies",
            "earnings date PIT revision risk",
            "may remain explanation layer with limited marginal decision value",
        ),
        pit_policy="event_timestamp_pit_audit_required",
        source_schema_status="event_calendar_source_audit_required",
        implementation_route="gating_layer_after_family_specs",
        rationale="稳定但更像 gating / review layer，不是第一批主 candidate family。",
    ),
    CandidateFamilyPriority(
        rank=6,
        family_id="execution_cooldown_decay_cap_mechanics",
        title="Execution cooldown / decay / cap mechanics",
        family_type="execution_research",
        score=69,
        priority="P0.5/P2",
        legal_task_id="TRADING-2307_FORWARD_OBSERVE_RUNTIME_EVIDENCE_AND_CAP_MECHANICS_PLAN",
        legacy_alias="TRADING-2294 runtime / execution mechanics",
        recommended_next_step="pair with risk-cap runtime",
        primary_use=(
            "no_add_mode",
            "max_exposure_cap",
            "cooldown_trigger",
            "cap_release_rule",
            "manual_review_trigger",
            "signal_aging_detection",
        ),
        not_recommended_as=("standalone_candidate_family", "broker_action_signal"),
        possible_inputs=(
            "risk_cap_low / medium / high trigger states",
            "repeated risk-cap trigger count",
            "valid_until / signal age",
            "post-trigger outcome evidence",
        ),
        validation_targets=(
            "cooldown evidence collection",
            "cap release timing",
            "signal stale detection",
            "manual review escalation rules",
        ),
        major_risks=(
            "too many execution rules can hide overfit",
            "cap / cooldown can miss rebounds",
            "requires forward observe evidence",
        ),
        pit_policy="observe_only_runtime_evidence_required",
        source_schema_status="runtime_contract_not_started",
        implementation_route="after_trading_2293_risk_cap_readiness",
        rationale="对 risk-cap 主线重要，但不是标准 new candidate family。",
    ),
)

DEFERRED_CURRENT_FORMS: tuple[dict[str, Any], ...] = (
    {
        "family_id": "baseline_plus_trend_structure_scope_narrowed_confirmation_v1",
        "current_status": "SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED",
        "score": 42,
        "priority": "P3",
        "deferred_action": "stop_near_term_re_tune",
        "resume_condition": "new breadth / participation inputs change the family design",
        "do_not_do": (
            "neutral band retune",
            "confirmation threshold retune",
            "confidence scaling retune",
            "asset / horizon expansion without new input family",
        ),
    },
    {
        "family_id": "risk_appetite_refined_confidence_v1",
        "current_status": "current_form_archived",
        "score": 38,
        "priority": "P3",
        "deferred_action": "archive_current_form",
        "resume_condition": (
            "redesign as AI / semiconductor leadership, liquidity/rates, "
            "or breadth-based participation appetite"
        ),
        "do_not_do": (
            "risk_appetite score retune",
            "missing proxy penalty retune",
            "risk_on / risk_off scaling retune",
        ),
    },
)


class FirstLayerNewCandidateFamilyPrioritizationError(ValueError):
    pass


def run_first_layer_new_candidate_family_prioritization(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise FirstLayerNewCandidateFamilyPrioritizationError(
            f"first-layer new candidate family prioritization only supports {MODE}"
        )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_first_layer_new_candidate_family_artifacts(generated_at=generated_at)
    write_first_layer_new_candidate_family_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    return artifacts["summary"]


def build_first_layer_new_candidate_family_artifacts(
    *,
    generated_at: datetime,
) -> dict[str, dict[str, Any]]:
    common = _common_payload(generated_at)
    score_rows = build_candidate_family_score_matrix()
    feasibility_rows = build_candidate_family_data_feasibility_matrix()
    task_rows = build_candidate_family_task_backlog()
    deferred_rows = build_deferred_current_form_matrix()
    validation_path = build_standard_validation_path()
    safety_boundary = build_safety_boundary()
    owner_review_note = build_owner_review_note()
    summary = build_prioritization_summary(
        score_rows=score_rows,
        task_rows=task_rows,
        generated_at=generated_at,
    )
    docs = build_prioritization_docs(
        summary=summary,
        score_rows=score_rows,
        feasibility_rows=feasibility_rows,
        task_rows=task_rows,
        deferred_rows=deferred_rows,
        validation_path=validation_path,
        safety_boundary=safety_boundary,
        owner_review_note=owner_review_note,
    )
    return {
        "summary": summary,
        "score_matrix": {**common, "score_dimensions": list(SCORE_DIMENSIONS), "rows": score_rows},
        "feasibility_matrix": {**common, "rows": feasibility_rows},
        "task_backlog": {**common, "rows": task_rows},
        "standard_validation_path": {**common, **validation_path},
        "deferred_current_form_matrix": {**common, "rows": deferred_rows},
        "safety_boundary": {**common, **safety_boundary},
        "owner_review_note": {**common, **owner_review_note},
        "docs": docs,
    }


def build_candidate_family_score_matrix() -> list[dict[str, Any]]:
    rows = []
    for family in FAMILY_PRIORITIES:
        rows.append(
            {
                "rank": family.rank if family.rank is not None else "deferred",
                "family_id": family.family_id,
                "title": family.title,
                "family_type": family.family_type,
                "score": family.score,
                "priority": family.priority,
                "legal_task_id": family.legal_task_id,
                "legacy_alias": family.legacy_alias,
                "recommended_next_step": family.recommended_next_step,
                "primary_use": "; ".join(family.primary_use),
                "not_recommended_as": "; ".join(family.not_recommended_as),
                "rationale": family.rationale,
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_candidate_family_data_feasibility_matrix() -> list[dict[str, Any]]:
    rows = []
    for family in FAMILY_PRIORITIES:
        rows.append(
            {
                "family_id": family.family_id,
                "title": family.title,
                "priority": family.priority,
                "pit_policy": family.pit_policy,
                "source_schema_status": family.source_schema_status,
                "possible_inputs": "; ".join(family.possible_inputs),
                "validation_targets": "; ".join(family.validation_targets),
                "major_risks": "; ".join(family.major_risks),
                "implementation_route": family.implementation_route,
                "data_quality_gate": "not_applicable_static_owner_brief",
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_candidate_family_task_backlog() -> list[dict[str, Any]]:
    unique_rows: dict[str, dict[str, Any]] = {}
    for family in FAMILY_PRIORITIES:
        unique_rows.setdefault(
            family.legal_task_id,
            {
                "task_id": family.legal_task_id,
                "legacy_alias": family.legacy_alias,
                "family_id": family.family_id,
                "priority": family.priority,
                "recommended_sequence": family.rank,
                "next_step": family.recommended_next_step,
                "owner_action": "review_research_backlog_before_candidate_generation",
                "acceptance_gate": _acceptance_gate_for_family(family),
                "blocked_until": _blocked_until_for_family(family),
                **SAFETY_FIELDS,
            },
        )
    return list(unique_rows.values())


def build_deferred_current_form_matrix() -> list[dict[str, Any]]:
    rows = []
    for item in DEFERRED_CURRENT_FORMS:
        rows.append(
            {
                **item,
                "do_not_do": "; ".join(item["do_not_do"]),
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_standard_validation_path() -> dict[str, Any]:
    return {
        "standard_path": list(STANDARD_VALIDATION_PATH),
        "must_not_skip": [
            "candidate_bound_artifact",
            "pit_timestamp",
            "source_hash",
            "provenance",
            "actual_path_validation",
            "safety_gates",
        ],
        "initial_candidate_safety": {
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        "data_quality_requirement": (
            "Future cached-data-dependent candidate generation, scoring, backtest, "
            "or report output must run aits validate-data or the same code path first."
        ),
    }


def build_safety_boundary() -> dict[str, Any]:
    return {
        "boundary_status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED",
        "this_task_only_prioritizes_research": True,
        "does_not_generate_candidate_bound_artifacts": True,
        "does_not_run_actual_path_validation": True,
        "does_not_start_forward_observe_runtime": True,
        "does_not_write_production_config": True,
        "does_not_use_cached_market_data": True,
        "data_quality_status": "NOT_APPLICABLE_STATIC_OWNER_BRIEF",
        "owner_review_required_before_candidate_generation": True,
        **SAFETY_FIELDS,
    }


def build_owner_review_note() -> dict[str, Any]:
    return {
        "note": (
            "本文件不是 promotion proposal，也不是 paper-shadow proposal；只回答哪些新方向"
            "值得研究、哪个方向应先做、哪些方向应暂缓、每个方向用途和风险是什么。"
        ),
        "not_allowed_to_open": [
            "promotion",
            "paper_shadow",
            "production",
            "broker_action",
        ],
        "best_new_family": "breadth_participation",
        "second_new_family": "ai_semiconductor_leadership",
        "third_new_family": "liquidity_rates_pressure",
        "mainline_to_continue": "volatility_risk_cap_forward_observe",
        **SAFETY_FIELDS,
    }


def build_prioritization_summary(
    *,
    score_rows: Sequence[Mapping[str, Any]],
    task_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        **_common_payload(generated_at),
        "summary": {
            "mainline": "volatility_risk_cap_forward_observe",
            "best_new_family": "breadth_participation",
            "second_new_family": "ai_semiconductor_leadership",
            "third_new_family": "liquidity_rates_pressure",
            "new_family_count": 3,
            "diagnostic_or_gating_count": 2,
            "execution_research_count": 1,
            "deferred_current_form_count": len(DEFERRED_CURRENT_FORMS),
            "task_backlog_count": len(task_rows),
            "score_matrix_row_count": len(score_rows),
            "data_quality_status": "NOT_APPLICABLE_STATIC_OWNER_BRIEF",
        },
        "selected_market_regime": "ai_after_chatgpt",
        "actual_requested_date_range": "owner_static_research_prioritization",
        "owner_attachment": (
            "G:/Download/first_layer_new_candidate_family_research_prioritization.md"
        ),
        "next_new_family_task": (
            "TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC"
        ),
        "next_mainline_task": (
            "TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN"
        ),
        **SAFETY_FIELDS,
    }


def write_first_layer_new_candidate_family_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> None:
    write_json(
        output_dir / "first_layer_new_candidate_family_prioritization_summary.json",
        artifacts["summary"],
    )
    write_json(output_dir / "candidate_family_score_matrix.json", artifacts["score_matrix"])
    write_csv_rows(
        output_dir / "candidate_family_score_matrix.csv",
        artifacts["score_matrix"]["rows"],
    )
    write_json(
        output_dir / "candidate_family_data_feasibility_matrix.json",
        artifacts["feasibility_matrix"],
    )
    write_csv_rows(
        output_dir / "candidate_family_data_feasibility_matrix.csv",
        artifacts["feasibility_matrix"]["rows"],
    )
    write_json(output_dir / "candidate_family_task_backlog.json", artifacts["task_backlog"])
    write_csv_rows(
        output_dir / "candidate_family_task_backlog.csv",
        artifacts["task_backlog"]["rows"],
    )
    write_json(
        output_dir / "candidate_family_standard_validation_path.json",
        artifacts["standard_validation_path"],
    )
    write_json(
        output_dir / "deferred_current_form_matrix.json",
        artifacts["deferred_current_form_matrix"],
    )
    write_csv_rows(
        output_dir / "deferred_current_form_matrix.csv",
        artifacts["deferred_current_form_matrix"]["rows"],
    )
    write_json(output_dir / "candidate_family_safety_boundary.json", artifacts["safety_boundary"])
    write_json(
        output_dir / "candidate_family_owner_review_note.json",
        artifacts["owner_review_note"],
    )

    docs = artifacts["docs"]
    write_markdown(
        docs_root / "first_layer_new_candidate_family_prioritization.md",
        str(docs["prioritization"]),
    )
    write_markdown(
        docs_root / "first_layer_new_candidate_family_task_backlog.md",
        str(docs["task_backlog"]),
    )
    write_markdown(
        docs_root / "first_layer_new_candidate_family_safety_boundary.md",
        str(docs["safety_boundary"]),
    )
    write_markdown(
        docs_root / "first_layer_new_candidate_family_owner_review_note.md",
        str(docs["owner_review_note"]),
    )


def build_prioritization_docs(
    *,
    summary: Mapping[str, Any],
    score_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    task_rows: Sequence[Mapping[str, Any]],
    deferred_rows: Sequence[Mapping[str, Any]],
    validation_path: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    owner_review_note: Mapping[str, Any],
) -> dict[str, str]:
    prioritization = "\n".join(
        [
            "# First-Layer New Candidate Family Prioritization",
            "",
            "TRADING-2301 把 owner 附件的新 candidate family 排序固化为 research-only backlog。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            "- actual_requested_date_range: `owner_static_research_prioritization`",
            f"- next_new_family_task: `{summary['next_new_family_task']}`",
            f"- next_mainline_task: `{summary['next_mainline_task']}`",
            "",
            "## Ranking",
            "",
            "|rank|family|score|priority|next task|",
            "|---:|---|---:|---|---|",
            *[
                (
                    f"|{row['rank']}|`{row['family_id']}`|{row['score']}|"
                    f"{row['priority']}|`{row['legal_task_id']}`|"
                )
                for row in score_rows
            ],
            "",
            "## Feasibility Focus",
            "",
            *[
                (
                    f"- `{row['family_id']}`: pit_policy=`{row['pit_policy']}`, "
                    f"source_schema_status=`{row['source_schema_status']}`"
                )
                for row in feasibility_rows
            ],
            "",
            "当前输出不生成 candidate-bound artifacts，不执行 actual-path validation。",
            "",
        ]
    )
    task_backlog = "\n".join(
        [
            "# First-Layer New Candidate Family Task Backlog",
            "",
            "|task_id|legacy_alias|priority|next_step|blocked_until|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['task_id']}`|`{row['legacy_alias']}`|{row['priority']}|"
                    f"{row['next_step']}|{row['blocked_until']}|"
                )
                for row in task_rows
            ],
            "",
            "## Standard Validation Path",
            "",
            " -> ".join(f"`{step}`" for step in validation_path["standard_path"]),
            "",
            "禁止跳过 candidate-bound artifact、PIT timestamp、source hash、provenance、"
            "actual-path validation 和 safety gates。",
            "",
        ]
    )
    safety_doc = "\n".join(
        [
            "# First-Layer New Candidate Family Safety Boundary",
            "",
            f"- boundary_status: `{safety_boundary['boundary_status']}`",
            f"- data_quality_status: `{safety_boundary['data_quality_status']}`",
            f"- promotion_allowed: `{safety_boundary['promotion_allowed']}`",
            f"- paper_shadow_allowed: `{safety_boundary['paper_shadow_allowed']}`",
            f"- production_allowed: `{safety_boundary['production_allowed']}`",
            f"- broker_action: `{safety_boundary['broker_action']}`",
            f"- candidate_generation_allowed: `{safety_boundary['candidate_generation_allowed']}`",
            (
                "- actual_path_validation_executed: "
                f"`{safety_boundary['actual_path_validation_executed']}`"
            ),
            (
                "- forward_observe_runtime_started: "
                f"`{safety_boundary['forward_observe_runtime_started']}`"
            ),
            "",
            "TRADING-2301 只做排序和 backlog，不接日报生产链路、不写 production config。",
            "",
        ]
    )
    owner_note_doc = "\n".join(
        [
            "# First-Layer New Candidate Family Owner Review Note",
            "",
            str(owner_review_note["note"]),
            "",
            "- mainline_to_continue: `volatility_risk_cap_forward_observe`",
            "- best_new_family: `breadth_participation`",
            "- second_new_family: `ai_semiconductor_leadership`",
            "- third_new_family: `liquidity_rates_pressure`",
            "",
            "## Deferred Current Forms",
            "",
            *[
                (
                    f"- `{row['family_id']}`: `{row['deferred_action']}`, "
                    f"resume_condition={row['resume_condition']}"
                )
                for row in deferred_rows
            ],
            "",
            "本 note 不得用于打开 promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return {
        "prioritization": prioritization,
        "task_backlog": task_backlog,
        "safety_boundary": safety_doc,
        "owner_review_note": owner_note_doc,
    }


def _acceptance_gate_for_family(family: CandidateFamilyPriority) -> str:
    if family.family_id == "volatility_risk_cap_forward_observe":
        return "observe_only_runtime_contract_ready_and_safety_gates_closed"
    if family.family_id == "execution_cooldown_decay_cap_mechanics":
        return "cap_mechanics_spec_ready_after_forward_observe_evidence_plan"
    if family.family_type == "new_candidate_family":
        return "data_feasibility_and_candidate_family_spec_ready"
    return "diagnostic_or_gating_framework_ready"


def _blocked_until_for_family(family: CandidateFamilyPriority) -> str:
    if family.family_id == "breadth_participation":
        return "TRADING-2301_validated"
    if family.family_id == "ai_semiconductor_leadership":
        return "TRADING-2301_validated_and_breadth_path_started"
    if family.family_id == "liquidity_rates_pressure":
        return "TRADING-2301_validated_and_p1_family_sequence_confirmed"
    if family.family_id == "execution_cooldown_decay_cap_mechanics":
        return "TRADING-2293_owner_review_or_TRADING-2301_route_confirmed"
    return "TRADING-2301_validated_and_owner_sequence_reviewed"


def _common_payload(generated_at: datetime) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "First-Layer New Candidate Family Prioritization",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "source": "owner_static_research_prioritization",
        **SAFETY_FIELDS,
    }


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "FirstLayerNewCandidateFamilyPrioritizationError",
    "build_candidate_family_data_feasibility_matrix",
    "build_candidate_family_score_matrix",
    "build_candidate_family_task_backlog",
    "build_safety_boundary",
    "build_standard_validation_path",
    "run_first_layer_new_candidate_family_prioritization",
]
