from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_trading_system.dynamic_strategy_report_common import json_block
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)
from ai_trading_system.research_quality import growth_tilt_candidate_family_closure as closure


class GrowthTiltCandidateFamilyClosureCalculator:
    plugin_id = "growth_tilt_candidate_family_closure_calculator"
    version = "v1"

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]:
        sources = context.sources
        return closure.build_growth_tilt_candidate_family_closure(
            sources,
            report_registry=_mapping(sources.get("report_registry")),
            artifact_catalog_text=str(sources.get("artifact_catalog_text") or ""),
            system_flow_text=str(sources.get("system_flow_text") or ""),
            requirement_text=str(sources.get("requirement_text") or ""),
            source_artifacts=context.source_artifacts,
            as_of=context.as_of.isoformat(),
        )


class GrowthTiltCandidateFamilyClosureReport:
    plugin_id = "growth_tilt_candidate_family_closure_report"
    version = "v1"

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]:
        if section_id != "negative_result_ledger":
            raise ValueError(f"unknown growth-tilt closure section: {section_id}")
        return _mapping(payload.get(section_id))

    def render_markdown(self, payload: Mapping[str, Any]) -> str:
        return render_growth_tilt_family_closure_markdown(payload)


def growth_tilt_candidate_family_closure_registry() -> PluginRegistry:
    return PluginRegistry(
        calculators=(GrowthTiltCandidateFamilyClosureCalculator(),),
        reports=(GrowthTiltCandidateFamilyClosureReport(),),
    )


def negative_result_ledger_section(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("negative_result_ledger"))


def growth_tilt_family_closure_view_model(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "family_id": payload.get("family_id"),
        "closure_status": payload.get("closure_status"),
        "closure_reason_codes": payload.get("closure_reason_codes"),
        "candidate_dispositions": payload.get("candidate_dispositions"),
        "prerequisite_pass_count": payload.get("prerequisite_pass_count"),
        "prerequisite_blocked_count": payload.get("prerequisite_blocked_count"),
        "baseline_adapter_ready_count": payload.get("baseline_adapter_ready_count"),
        "baseline_adapter_blocked_count": payload.get("baseline_adapter_blocked_count"),
        "pit_candidates_tested": payload.get("pit_candidates_tested"),
        "next_route": payload.get("next_route"),
    }


def render_growth_tilt_family_closure_markdown(payload: Mapping[str, Any]) -> str:
    summary = growth_tilt_family_closure_view_model(payload)
    return "\n".join(
        [
            "# Growth Tilt Candidate Family Closure",
            "",
            "当前 A/B/C/replacement-A family 已正式关闭为 completed negative research "
            "evidence。关闭不是 FAIL 或实现失败；它表示没有 approved、contract-complete、"
            "PIT-executable candidate。",
            "",
            "```json",
            json_block(summary),
            "```",
            "",
            "## Exact M1E prerequisite matrix",
            "",
            "```json",
            json_block(payload.get("replacement_a_prerequisite_matrix")),
            "```",
            "",
            "## Negative-result ledger",
            "",
            "```json",
            json_block(payload.get("negative_result_ledger")),
            "```",
            "",
            "## Reopen policy",
            "",
            "```json",
            json_block(payload.get("reopen_policy")),
            "```",
            "",
            "## 结论",
            "",
            "旧 family 的 M2 route 已关闭。只有 candidate-independent baseline work产生"
            "受治理的新 capability evidence并重新 owner approval/refreeze policy后，才可"
            "新开 reopen task；当前下一步是 read-only TRADING-2438N2 capability graph。",
            "",
        ]
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
