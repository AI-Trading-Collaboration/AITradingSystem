from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    write_csv_rows,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "ai_semiconductor_leadership_feasibility_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2307_AI_SEMICONDUCTOR_LEADERSHIP_CANDIDATE_FAMILY_FEASIBILITY_AUDIT"
REPORT_TYPE = "ai_semiconductor_leadership_feasibility_audit"
MODE = "feasibility_audit"
STATUS = "AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"
ARTIFACT_ROLE = "ai_semiconductor_leadership_feasibility_audit"

DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "generator_implemented": False,
    "candidate_generation_allowed": False,
    "actual_path_validation_executed": False,
    "candidate_artifact_generated": False,
    "candidate_signal_series_generated": False,
    "prediction_artifact_generated": False,
    "forward_observe_runtime_started": False,
    "runtime_started": False,
}


@dataclass(frozen=True)
class LeadershipInputSpec:
    input_id: str
    title: str
    input_category: str
    required_symbols: tuple[str, ...]
    source_status: str
    pit_status: str
    usage_role: str
    feasibility_grade: str
    major_risks: tuple[str, ...]
    required_before_generator: tuple[str, ...]
    recommended_usage: str


class AISemiconductorLeadershipFeasibilityAuditError(ValueError):
    pass


INPUT_SPECS: tuple[LeadershipInputSpec, ...] = (
    LeadershipInputSpec(
        input_id="smh_vs_qqq_relative_strength",
        title="SMH vs QQQ relative strength",
        input_category="etf_price_relative_strength",
        required_symbols=("SMH", "QQQ"),
        source_status="CACHE_VALIDATION_REQUIRED_BEFORE_USE",
        pit_status="PRICE_PROXY_PIT_APPROXIMATION_READY_AFTER_CACHE_VALIDATION",
        usage_role="SMH_overweight_confirmation",
        feasibility_grade="PRICE_PROXY_FEASIBLE_AFTER_DATA_QUALITY_GATE",
        major_risks=("overlap_with_smh_momentum", "sector_rotation_noise"),
        required_before_generator=("aits_validate_data_pass", "relative_strength_policy_manifest"),
        recommended_usage="primary_price_proxy_for_trading_2308_poc",
    ),
    LeadershipInputSpec(
        input_id="nvda_vs_smh_leadership",
        title="NVDA vs SMH leadership",
        input_category="single_name_price_relative_strength",
        required_symbols=("NVDA", "SMH"),
        source_status="CACHE_VALIDATION_REQUIRED_BEFORE_USE",
        pit_status="PRICE_PROXY_PIT_APPROXIMATION_READY_AFTER_CACHE_VALIDATION",
        usage_role="AI_leadership_concentration_warning",
        feasibility_grade="PRICE_PROXY_FEASIBLE_AFTER_DATA_QUALITY_GATE",
        major_risks=("NVDA_concentration", "earnings_gap_risk", "single_name_overfit"),
        required_before_generator=(
            "aits_validate_data_pass",
            "single_name_concentration_guardrail",
        ),
        recommended_usage="warning_or_confirmation_component_not_standalone_signal",
    ),
    LeadershipInputSpec(
        input_id="semiconductor_peer_relative_strength",
        title="AMD / TSM / AVGO / ASML relative strength",
        input_category="peer_basket_price_relative_strength",
        required_symbols=("AMD", "TSM", "AVGO", "ASML", "SMH"),
        source_status="CACHE_VALIDATION_REQUIRED_BEFORE_USE",
        pit_status="PRICE_PROXY_PIT_APPROXIMATION_READY_AFTER_CACHE_VALIDATION",
        usage_role="AI_chain_confirmation",
        feasibility_grade="PRICE_PROXY_FEASIBLE_WITH_UNIVERSE_POLICY",
        major_risks=(
            "hindsight_basket_selection",
            "ADR_calendar_mismatch",
            "earnings_cluster_risk",
        ),
        required_before_generator=("aits_validate_data_pass", "ai_core_universe_policy"),
        recommended_usage="peer_diffusion_component_after_basket_policy",
    ),
    LeadershipInputSpec(
        input_id="ai_core_basket_vs_qqq",
        title="AI core basket vs QQQ",
        input_category="basket_price_relative_strength",
        required_symbols=("NVDA", "AMD", "TSM", "AVGO", "ASML", "QQQ"),
        source_status="BASKET_POLICY_REQUIRED_BEFORE_USE",
        pit_status="PIT_APPROXIMATION_DEPENDS_ON_PRE_REGISTERED_BASKET",
        usage_role="AI_chain_confirmation",
        feasibility_grade="CONDITIONAL_ON_UNIVERSE_POLICY",
        major_risks=("hindsight_basket_selection", "survivorship_bias", "mega_cap_concentration"),
        required_before_generator=(
            "pre_registered_ai_core_basket_policy",
            "aits_validate_data_pass",
        ),
        recommended_usage="candidate_component_only_after_universe_policy",
    ),
    LeadershipInputSpec(
        input_id="semiconductor_basket_breadth",
        title="Semiconductor basket breadth",
        input_category="basket_breadth_proxy",
        required_symbols=("NVDA", "AMD", "TSM", "AVGO", "ASML", "LRCX", "AMAT", "MU"),
        source_status="BASKET_POLICY_AND_PRICE_COVERAGE_REQUIRED",
        pit_status="NOT_TRUE_CONSTITUENT_BREADTH",
        usage_role="leadership_diffusion_diagnostic",
        feasibility_grade="DIAGNOSTICS_ONLY_UNTIL_BASKET_POLICY",
        major_risks=("not_smh_constituent_breadth", "basket_survivorship_bias", "coverage_gap"),
        required_before_generator=(
            "basket_policy",
            "price_coverage_audit",
            "proxy_bias_disclosure",
        ),
        recommended_usage="diagnostics_only_not_promotion_evidence",
    ),
    LeadershipInputSpec(
        input_id="mega_cap_ai_leadership_concentration",
        title="Mega-cap AI leadership concentration",
        input_category="weights_or_market_cap_concentration",
        required_symbols=("NVDA", "MSFT", "GOOGL", "AMZN", "META", "QQQ", "SMH"),
        source_status="WEIGHT_OR_MARKET_CAP_SOURCE_AUDIT_REQUIRED",
        pit_status="BLOCKED_PENDING_WEIGHT_SOURCE_AUDIT",
        usage_role="concentration_risk_warning",
        feasibility_grade="SOURCE_AUDIT_REQUIRED",
        major_risks=("weight_timestamp_gap", "market_cap_float_adjustment_gap", "lookahead_risk"),
        required_before_generator=("weight_or_market_cap_source", "known_at_timestamp_policy"),
        recommended_usage="warning_only_after_source_audit",
    ),
    LeadershipInputSpec(
        input_id="ai_earnings_capex_event_context",
        title="AI earnings / capex / event context",
        input_category="event_or_fundamental_context",
        required_symbols=("NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSM"),
        source_status="PIT_EVENT_SOURCE_AUDIT_REQUIRED",
        pit_status="BLOCKED_PENDING_EVENT_KNOWN_AT_AUDIT",
        usage_role="manual_review_context",
        feasibility_grade="NOT_GENERATOR_READY",
        major_risks=("earnings_date_revision", "reported_at_gap", "event_outcome_hindsight"),
        required_before_generator=("event_calendar_pit_audit", "known_at_timestamp_manifest"),
        recommended_usage="manual_review_context_not_return_predictor",
    ),
)


def run_ai_semiconductor_leadership_feasibility_audit(
    *,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    horizons: str | Sequence[str] = DEFAULT_HORIZONS,
    candidate_family: str = "ai_semiconductor_leadership",
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    request = _validated_request(
        target_assets=target_assets,
        horizons=horizons,
        candidate_family=candidate_family,
        mode=mode,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_ai_semiconductor_leadership_feasibility_artifacts(
        target_assets=request["target_assets"],
        horizons=request["horizons"],
        candidate_family=request["candidate_family"],
        generated_at=generated_at,
    )
    artifact_paths = write_ai_semiconductor_leadership_feasibility_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def build_ai_semiconductor_leadership_feasibility_artifacts(
    *,
    target_assets: Sequence[str],
    horizons: Sequence[str],
    candidate_family: str,
    generated_at: datetime,
) -> dict[str, Any]:
    common = _common_payload(
        target_assets=target_assets,
        horizons=horizons,
        candidate_family=candidate_family,
        generated_at=generated_at,
    )
    inventory_rows = build_ai_leadership_input_inventory()
    design_sketch = build_ai_leadership_candidate_design_sketch(
        target_assets=target_assets,
        horizons=horizons,
    )
    validation_route = build_ai_leadership_validation_route()
    safety_boundary = build_ai_leadership_safety_boundary()
    summary = build_feasibility_summary(
        common=common,
        inventory_rows=inventory_rows,
        validation_route=validation_route,
    )
    docs = build_ai_leadership_docs(
        summary=summary,
        inventory_rows=inventory_rows,
        design_sketch=design_sketch,
        validation_route=validation_route,
        safety_boundary=safety_boundary,
    )
    return {
        "summary": summary,
        "input_inventory": {**common, "rows": inventory_rows},
        "candidate_design_sketch": {**common, **design_sketch},
        "validation_route": {**common, "rows": validation_route},
        "safety_boundary": {**common, **safety_boundary},
        "docs": docs,
    }


def build_ai_leadership_input_inventory() -> list[dict[str, Any]]:
    return [
        {
            "input_id": spec.input_id,
            "title": spec.title,
            "input_category": spec.input_category,
            "required_symbols": list(spec.required_symbols),
            "source_status": spec.source_status,
            "source_path": _source_path_for_spec(spec),
            "provider_class": _provider_class_for_spec(spec),
            "pit_status": spec.pit_status,
            "usage_role": spec.usage_role,
            "feasibility_grade": spec.feasibility_grade,
            "major_risks": list(spec.major_risks),
            "required_before_generator": list(spec.required_before_generator),
            "recommended_usage": spec.recommended_usage,
            "data_quality_requirement": (
                "Run aits validate-data or the same validation code path before any "
                "price-dependent TRADING-2308 generator computation."
            ),
            **SAFETY_FIELDS,
        }
        for spec in INPUT_SPECS
    ]


def build_ai_leadership_candidate_design_sketch(
    *,
    target_assets: Sequence[str],
    horizons: Sequence[str],
) -> dict[str, Any]:
    return {
        "candidate_family": "ai_semiconductor_leadership",
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "candidate_ids": [
            "ai_semiconductor_leadership_quality_v1",
            "smh_relative_strength_leadership_v1",
            "ai_core_basket_leadership_v1",
        ],
        "primary_use": [
            "SMH_overweight_confirmation",
            "AI_chain_confirmation",
            "semiconductor_leadership_weakening_warning",
            "exposure_cap_modifier",
        ],
        "not_recommended_as": [
            "generic_risk_appetite_reopen",
            "standalone_buy_sell_signal",
            "broker_action_signal",
            "event_outcome_predictor",
        ],
        "signal_components": {
            "price_proxy_core": [
                "SMH_vs_QQQ_relative_strength",
                "SMH_vs_SPY_relative_strength",
                "NVDA_vs_SMH_relative_strength",
            ],
            "peer_diffusion": [
                "AMD_TSM_AVGO_ASML_vs_SMH_relative_strength",
                "AI_core_basket_vs_QQQ_relative_strength",
            ],
            "risk_warnings": [
                "mega_cap_AI_concentration",
                "semiconductor_leadership_weakening",
                "earnings_or_capex_event_cluster_context",
            ],
        },
        "design_conclusion": (
            "Price relative-strength concepts are feasible for a future POC after "
            "the cached-data quality gate and basket policy are satisfied. Basket "
            "breadth, concentration and event context remain source-audit items."
        ),
        "recommended_next_task": (
            "TRADING-2308_AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC"
        ),
        "generator_poc_prerequisites": [
            "aits_validate_data_pass_for_required_price_symbols",
            "pre_registered_ai_core_basket_policy",
            "single_name_concentration_guardrail",
            "proxy_bias_disclosure",
        ],
        **SAFETY_FIELDS,
    }


def build_ai_leadership_validation_route() -> list[dict[str, Any]]:
    return [
        _route_row(
            candidate_id="smh_relative_strength_leadership_v1",
            readiness_status="GENERATOR_POC_READY_AFTER_PRICE_DQ_GATE",
            data_mode="price_relative_strength_proxy",
            required_inputs=[
                "SMH_vs_QQQ_relative_strength",
                "SMH_vs_SPY_relative_strength",
            ],
            blocked_until=[
                "aits_validate_data_pass",
                "candidate_signal_spec",
            ],
            allowed_next_step="TRADING-2308_GENERATOR_POC",
            blocked_validation="actual_path_validation; promotion; paper_shadow; production",
        ),
        _route_row(
            candidate_id="ai_semiconductor_leadership_quality_v1",
            readiness_status="CONDITIONAL_ON_BASKET_POLICY_AND_PRICE_DQ_GATE",
            data_mode="price_proxy_plus_peer_basket_policy",
            required_inputs=[
                "NVDA_vs_SMH_relative_strength",
                "AMD_TSM_AVGO_ASML_vs_SMH_relative_strength",
                "single_name_concentration_guardrail",
            ],
            blocked_until=[
                "aits_validate_data_pass",
                "pre_registered_ai_core_basket_policy",
                "proxy_bias_disclosure",
            ],
            allowed_next_step="TRADING-2308_GENERATOR_POC_AFTER_POLICY",
            blocked_validation="actual_path_validation; promotion; paper_shadow; production",
        ),
        _route_row(
            candidate_id="ai_core_basket_leadership_v1",
            readiness_status="BLOCKED_PENDING_BASKET_POLICY",
            data_mode="ai_core_basket_relative_strength",
            required_inputs=[
                "pre_registered_ai_core_basket_policy",
                "AI_core_basket_vs_QQQ_relative_strength",
                "basket_price_coverage_audit",
            ],
            blocked_until=[
                "basket_policy",
                "price_coverage_audit",
                "aits_validate_data_pass",
            ],
            allowed_next_step="TRADING-2308_GENERATOR_POC_AFTER_BASKET_POLICY",
            blocked_validation="actual_path_validation; promotion; paper_shadow; production",
        ),
        _route_row(
            candidate_id="mega_cap_ai_concentration_warning_v1",
            readiness_status="SOURCE_AUDIT_REQUIRED",
            data_mode="weights_or_market_cap_concentration",
            required_inputs=[
                "historical_or_current_weights",
                "market_cap_or_float_adjusted_cap_source",
                "known_at_timestamp_policy",
            ],
            blocked_until=[
                "weight_or_market_cap_source_audit",
                "known_at_timestamp_manifest",
            ],
            allowed_next_step="source_audit_before_generator",
            blocked_validation="generator_poc; actual_path_validation; promotion",
        ),
    ]


def build_ai_leadership_safety_boundary() -> dict[str, Any]:
    return {
        "boundary_status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED",
        "does_not_read_market_cache": True,
        "does_not_download_external_data": True,
        "does_not_generate_signal_series": True,
        "does_not_generate_candidate_bound_artifacts": True,
        "does_not_run_actual_path_validation": True,
        "does_not_reopen_generic_risk_appetite_current_form": True,
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "This static feasibility audit does not consume cached market/macro data. "
            "TRADING-2308 price-dependent generator work must run aits validate-data "
            "or the same validation code path first."
        ),
        **SAFETY_FIELDS,
    }


def build_feasibility_summary(
    *,
    common: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    validation_route: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    price_proxy_ready = [
        row
        for row in inventory_rows
        if str(row["feasibility_grade"]).startswith("PRICE_PROXY_FEASIBLE")
    ]
    source_audit_required = [
        row
        for row in inventory_rows
        if "SOURCE_AUDIT" in str(row["feasibility_grade"])
        or "NOT_GENERATOR_READY" in str(row["feasibility_grade"])
    ]
    return {
        **dict(common),
        "summary": {
            "input_count": len(inventory_rows),
            "price_proxy_ready_after_dq_count": len(price_proxy_ready),
            "source_audit_required_count": len(source_audit_required),
            "route_count": len(validation_route),
            "generator_poc_ready_now": False,
            "recommended_next_action": (
                "TRADING-2308_GENERATOR_POC_AFTER_PRICE_DQ_AND_BASKET_POLICY"
            ),
            "data_quality_status": DATA_QUALITY_STATUS,
        },
        "recommended_next_task": (
            "TRADING-2308_AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC"
        ),
        "generator_poc_ready_now": False,
        "price_proxy_feasible_after_data_quality_gate": True,
        "basket_policy_required": True,
        "event_source_audit_required": True,
        "selected_market_regime": MARKET_REGIME,
        **SAFETY_FIELDS,
    }


def write_ai_semiconductor_leadership_feasibility_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "ai_semiconductor_leadership_feasibility_summary.json",
        "input_inventory_json": output_dir / "ai_leadership_input_inventory.json",
        "input_inventory_csv": output_dir / "ai_leadership_input_inventory.csv",
        "candidate_design_sketch": output_dir / "ai_leadership_candidate_design_sketch.json",
        "validation_route_json": output_dir / "ai_leadership_validation_route.json",
        "validation_route_csv": output_dir / "ai_leadership_validation_route.csv",
        "safety_boundary": output_dir / "ai_leadership_safety_boundary.json",
        "audit_doc": docs_root / "ai_semiconductor_leadership_feasibility_audit.md",
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["input_inventory_json"], artifacts["input_inventory"])
    write_csv_rows(paths["input_inventory_csv"], artifacts["input_inventory"]["rows"])
    write_json(paths["candidate_design_sketch"], artifacts["candidate_design_sketch"])
    write_json(paths["validation_route_json"], artifacts["validation_route"])
    write_csv_rows(paths["validation_route_csv"], artifacts["validation_route"]["rows"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])
    write_markdown(paths["audit_doc"], artifacts["docs"]["audit"])
    return {key: str(path) for key, path in paths.items()}


def build_ai_leadership_docs(
    *,
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    design_sketch: Mapping[str, Any],
    validation_route: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    audit = "\n".join(
        [
            "# AI / 半导体 Leadership 可行性审计",
            "",
            "TRADING-2307 只做 AI / semiconductor leadership candidate family 可行性审计。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- input_count: `{summary['summary']['input_count']}`",
            (
                "- price_proxy_ready_after_dq_count: "
                f"`{summary['summary']['price_proxy_ready_after_dq_count']}`"
            ),
            f"- generator_poc_ready_now: `{summary['generator_poc_ready_now']}`",
            f"- recommended_next_task: `{summary['recommended_next_task']}`",
            "",
            "## Input Inventory",
            "",
            "|input_id|category|feasibility|pit_status|recommended_usage|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['input_id']}`|`{row['input_category']}`|"
                    f"`{row['feasibility_grade']}`|`{row['pit_status']}`|"
                    f"{row['recommended_usage']}|"
                )
                for row in inventory_rows
            ],
            "",
            "## Candidate Design Sketch",
            "",
            *[f"- `{candidate_id}`" for candidate_id in design_sketch["candidate_ids"]],
            "",
            "## Validation Route",
            "",
            "|candidate_id|readiness_status|allowed_next_step|blocked_validation|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['candidate_id']}`|`{row['readiness_status']}`|"
                    f"`{row['allowed_next_step']}`|{row['blocked_validation']}|"
                )
                for row in validation_route
            ],
            "",
            "## Safety",
            "",
            _safety_sentence(safety_boundary),
            "",
            "本报告不得用于 candidate generation、actual-path validation、promotion、"
            "paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return {"audit": audit}


def _validated_request(
    *,
    target_assets: str | Sequence[str],
    horizons: str | Sequence[str],
    candidate_family: str,
    mode: str,
) -> dict[str, Any]:
    if mode != MODE:
        raise AISemiconductorLeadershipFeasibilityAuditError(
            f"AI / 半导体 leadership 可行性审计只支持 {MODE}"
        )
    if candidate_family != "ai_semiconductor_leadership":
        raise AISemiconductorLeadershipFeasibilityAuditError(
            "candidate-family must be ai_semiconductor_leadership"
        )
    assets = _parse_list(target_assets)
    parsed_horizons = _parse_list(horizons, uppercase=False)
    if not assets:
        raise AISemiconductorLeadershipFeasibilityAuditError("--target-assets is required")
    if not parsed_horizons:
        raise AISemiconductorLeadershipFeasibilityAuditError("--horizons is required")
    return {
        "target_assets": assets,
        "horizons": parsed_horizons,
        "candidate_family": candidate_family,
    }


def _common_payload(
    *,
    target_assets: Sequence[str],
    horizons: Sequence[str],
    candidate_family: str,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "AI / 半导体 Leadership 可行性审计",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "candidate_family": candidate_family,
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "static_feasibility_audit",
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "This static audit does not consume cached market/macro data. Future "
            "generator, scoring, backtest, daily report, or actual-path validation "
            "must run aits validate-data or the same validation code path first."
        ),
        **SAFETY_FIELDS,
    }


def _route_row(
    *,
    candidate_id: str,
    readiness_status: str,
    data_mode: str,
    required_inputs: Sequence[str],
    blocked_until: Sequence[str],
    allowed_next_step: str,
    blocked_validation: str,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "candidate_family": "ai_semiconductor_leadership",
        "readiness_status": readiness_status,
        "data_mode": data_mode,
        "required_inputs": list(required_inputs),
        "blocked_until": list(blocked_until),
        "allowed_next_step": allowed_next_step,
        "blocked_validation": blocked_validation,
        "actual_path_validation_ready": False,
        "promotion_eligible": False,
        **SAFETY_FIELDS,
    }


def _source_path_for_spec(spec: LeadershipInputSpec) -> str:
    if "PRICE" in spec.pit_status:
        return "data/raw/prices_daily.csv (validation required before use)"
    if spec.input_category == "event_or_fundamental_context":
        return "event_or_fundamental_provider_required"
    return "policy_or_source_audit_required"


def _provider_class_for_spec(spec: LeadershipInputSpec) -> str:
    if "price" in spec.input_category:
        return "cached_price_vendor_or_public_price_source"
    if spec.input_category == "event_or_fundamental_context":
        return "event_or_fundamental_source_pending"
    if "concentration" in spec.input_category:
        return "weights_or_market_cap_source_pending"
    return "policy_defined_proxy"


def _parse_list(value: str | Sequence[str], *, uppercase: bool = True) -> list[str]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    cleaned = [part.strip() for part in parts if part.strip()]
    if uppercase:
        return [part.upper() for part in cleaned]
    return cleaned


def _safety_sentence(payload: Mapping[str, Any]) -> str:
    return (
        f"promotion_allowed=`{payload['promotion_allowed']}`, "
        f"paper_shadow_allowed=`{payload['paper_shadow_allowed']}`, "
        f"production_allowed=`{payload['production_allowed']}`, "
        f"broker_action=`{payload['broker_action']}`, "
        f"generator_implemented=`{payload['generator_implemented']}`, "
        f"candidate_artifact_generated=`{payload['candidate_artifact_generated']}`, "
        f"actual_path_validation_executed=`{payload['actual_path_validation_executed']}`."
    )


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "AISemiconductorLeadershipFeasibilityAuditError",
    "build_ai_leadership_candidate_design_sketch",
    "build_ai_leadership_input_inventory",
    "build_ai_leadership_safety_boundary",
    "build_ai_leadership_validation_route",
    "build_ai_semiconductor_leadership_feasibility_artifacts",
    "run_ai_semiconductor_leadership_feasibility_audit",
]
