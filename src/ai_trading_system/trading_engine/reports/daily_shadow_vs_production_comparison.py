from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

SHADOW_VS_PRODUCTION_SCHEMA_VERSION = 1
SHADOW_VS_PRODUCTION_REPORT_TYPE = "daily_shadow_vs_production_comparison"
SHADOW_VS_PRODUCTION_TASK_ID = "TRADING-018C"
MODE_OFFLINE_COMPARISON = "offline_comparison"
PRODUCTION_EFFECT_NONE = "none"
STATUS_COMPARISON_AVAILABLE = "COMPARISON_AVAILABLE"
STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"

# Display-only summary limit; the JSON keeps every module in contribution_breakdown.
TOP_CONTRIBUTOR_DISPLAY_LIMIT = 2

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"
DEFAULT_SCORING_RULES_PATH = REPO_ROOT / "config" / "scoring_rules.yaml"


def default_weight_iteration_comparison_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "comparison"


def default_shadow_vs_production_comparison_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_weight_iteration_comparison_root(data_root)
        / f"daily_shadow_vs_production_{as_of.isoformat()}.json"
    )


def build_daily_shadow_vs_production_comparison_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    data_root: Path = REPO_ROOT / "data",
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    scoring_rules_path: Path = DEFAULT_SCORING_RULES_PATH,
    current_shadow_weights_path: Path | None = None,
    shadow_iteration_candidate_path: Path | None = None,
    decision_snapshot_path: Path | None = None,
    daily_decision_summary_path: Path | None = None,
    daily_weight_adjustment_summary_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    weight_promotion_gate_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    suffix = as_of.isoformat()
    output_json_path = output_json_path or default_shadow_vs_production_comparison_json_path(
        data_root,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    current_shadow_weights_path = current_shadow_weights_path or (
        data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"
    )
    shadow_iteration_candidate_path = shadow_iteration_candidate_path or (
        data_root
        / "derived"
        / "weight_iterations"
        / "shadow"
        / "candidates"
        / f"shadow_weight_candidate_{suffix}.json"
    )
    decision_snapshot_path = decision_snapshot_path or (
        data_root / "processed" / "decision_snapshots" / f"decision_snapshot_{suffix}.json"
    )
    daily_decision_summary_path = daily_decision_summary_path or (
        reports_dir / f"daily_decision_summary_{suffix}.json"
    )
    daily_weight_adjustment_summary_path = daily_weight_adjustment_summary_path or (
        reports_dir / f"daily_weight_adjustment_summary_{suffix}.json"
    )
    weight_candidate_evaluation_path = weight_candidate_evaluation_path or (
        reports_dir / f"weight_candidate_evaluation_{suffix}.json"
    )
    weight_promotion_gate_path = weight_promotion_gate_path or (
        reports_dir / f"weight_promotion_gate_{suffix}.json"
    )

    production_profile = _load_yaml_object(production_profile_path)
    scoring_rules = _load_yaml_object(scoring_rules_path)
    current_shadow = _read_json_object(current_shadow_weights_path)
    shadow_candidate = _read_json_object(shadow_iteration_candidate_path)
    decision_snapshot = _read_json_object(decision_snapshot_path)
    daily_summary = _read_json_object(daily_decision_summary_path)
    daily_weight_adjustment = _read_json_object(daily_weight_adjustment_summary_path)
    candidate_evaluation = _read_json_object(weight_candidate_evaluation_path)
    promotion_gate = _read_json_object(weight_promotion_gate_path)

    source_artifacts = {
        "production_profile": _artifact_record(production_profile_path, reports_dir),
        "current_shadow_weights": _artifact_record(
            current_shadow_weights_path,
            reports_dir,
            expected_report_type="current_shadow_weights",
            payload=current_shadow,
        ),
        "shadow_iteration_candidate": _artifact_record(
            shadow_iteration_candidate_path,
            reports_dir,
            expected_report_type="daily_shadow_weight_iteration",
            payload=shadow_candidate,
        ),
        "decision_snapshot": _artifact_record(decision_snapshot_path, reports_dir),
        "scoring_rules": _artifact_record(scoring_rules_path, reports_dir),
        "daily_decision_summary": _artifact_record(
            daily_decision_summary_path,
            reports_dir,
            expected_report_type="daily_decision_summary",
            payload=daily_summary,
            optional=True,
        ),
        "daily_weight_adjustment_summary": _artifact_record(
            daily_weight_adjustment_summary_path,
            reports_dir,
            expected_report_type="daily_weight_adjustment_summary",
            payload=daily_weight_adjustment,
            optional=True,
        ),
        "weight_candidate_evaluation": _artifact_record(
            weight_candidate_evaluation_path,
            reports_dir,
            expected_report_type="weight_candidate_evaluation",
            payload=candidate_evaluation,
            optional=True,
        ),
        "weight_promotion_gate": _artifact_record(
            weight_promotion_gate_path,
            reports_dir,
            expected_report_type="weight_promotion_gate",
            payload=promotion_gate,
            optional=True,
        ),
        "daily_score_markdown": _artifact_record(
            reports_dir / f"daily_score_{suffix}.md",
            reports_dir,
            optional=True,
        ),
        "market_feedback_optimization": _artifact_record(
            reports_dir / f"market_feedback_optimization_{suffix}.md",
            reports_dir,
            optional=True,
        ),
        "feedback_loop_review": _artifact_record(
            reports_dir / f"feedback_loop_review_{suffix}.md",
            reports_dir,
            optional=True,
        ),
        "investment_weekly_review": _artifact_record(
            reports_dir / f"investment_weekly_review_{suffix}.md",
            reports_dir,
            optional=True,
        ),
    }

    components = _score_components(decision_snapshot)
    production_weights = _weights_from_mapping(_mapping(production_profile.get("base_weights")))
    shadow_weights = _weights_from_mapping(_mapping(current_shadow.get("weights")))
    position_bands = _position_bands(scoring_rules)
    non_score_gates = _non_score_position_gates(decision_snapshot)
    data_quality = _data_quality_record(decision_snapshot, daily_summary)
    shadow_iteration = _shadow_iteration_record(shadow_candidate, current_shadow)
    validation = _validation(
        source_artifacts=source_artifacts,
        components=components,
        production_weights=production_weights,
        shadow_weights=shadow_weights,
        position_bands=position_bands,
        current_shadow=current_shadow,
        shadow_candidate=shadow_candidate,
        data_quality=data_quality,
    )

    production_result: dict[str, Any] = {}
    shadow_result: dict[str, Any] = {}
    comparison: dict[str, Any] = {
        "score_delta": None,
        "normalized_score_delta": None,
        "decision_changed": None,
        "score_band_changed": None,
        "risk_flags_changed": None,
        "main_reason": "输入不足，未计算 production/shadow 差异。",
        "contribution_deltas": [],
        "weight_deltas": [],
    }
    if not validation["blocking_reasons"]:
        production_result = _profile_result(
            profile_id="production",
            weights=production_weights,
            components=components,
            position_bands=position_bands,
            non_score_gates=non_score_gates,
        )
        shadow_result = _profile_result(
            profile_id="shadow",
            weights=shadow_weights,
            components=components,
            position_bands=position_bands,
            non_score_gates=non_score_gates,
        )
        comparison = _comparison_result(
            production_result=production_result,
            shadow_result=shadow_result,
        )

    status = (
        STATUS_INSUFFICIENT_DATA if validation["blocking_reasons"] else STATUS_COMPARISON_AVAILABLE
    )
    payload = {
        "schema_version": SHADOW_VS_PRODUCTION_SCHEMA_VERSION,
        "report_type": SHADOW_VS_PRODUCTION_REPORT_TYPE,
        "task_id": SHADOW_VS_PRODUCTION_TASK_ID,
        "generated_at": generated.isoformat(),
        "as_of": suffix,
        "market_regime": _market_regime_id(decision_snapshot),
        "mode": MODE_OFFLINE_COMPARISON,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "comparison_status": status,
        "source_artifacts": source_artifacts,
        "data_quality": data_quality,
        "shadow_iteration": shadow_iteration,
        "input_validation": validation,
        "production": production_result,
        "shadow": shadow_result,
        "difference": comparison,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "uses_same_input_component_scores": True,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "notes": [
            "本报告只比较同一 decision_snapshot 组件分在 production/shadow 权重下的离线结果。",
            "本报告不重新抓取数据、不重新生成特征、不运行 broker 或 replay runner。",
            "shadow 更优或 decision 改变也不代表可采用；后续必须另走人工复核和 promotion 任务。",
        ],
    }
    _assert_safety_invariants(payload)
    return payload


def write_daily_shadow_vs_production_comparison_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    data_root: Path = REPO_ROOT / "data",
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    scoring_rules_path: Path = DEFAULT_SCORING_RULES_PATH,
    current_shadow_weights_path: Path | None = None,
    shadow_iteration_candidate_path: Path | None = None,
    decision_snapshot_path: Path | None = None,
    daily_decision_summary_path: Path | None = None,
    daily_weight_adjustment_summary_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    weight_promotion_gate_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_daily_shadow_vs_production_comparison_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        data_root=data_root,
        production_profile_path=production_profile_path,
        scoring_rules_path=scoring_rules_path,
        current_shadow_weights_path=current_shadow_weights_path,
        shadow_iteration_candidate_path=shadow_iteration_candidate_path,
        decision_snapshot_path=decision_snapshot_path,
        daily_decision_summary_path=daily_decision_summary_path,
        daily_weight_adjustment_summary_path=daily_weight_adjustment_summary_path,
        weight_candidate_evaluation_path=weight_candidate_evaluation_path,
        weight_promotion_gate_path=weight_promotion_gate_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated_at,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    _write_json(json_path, payload)
    _write_text(md_path, render_daily_shadow_vs_production_comparison_report(payload))
    return payload


def render_daily_shadow_vs_production_comparison_report(payload: dict[str, Any]) -> str:
    production = _mapping(payload.get("production"))
    shadow = _mapping(payload.get("shadow"))
    difference = _mapping(payload.get("difference"))
    validation = _mapping(payload.get("input_validation"))
    data_quality = _mapping(payload.get("data_quality"))
    shadow_iteration = _mapping(payload.get("shadow_iteration"))
    lines = [
        f"# Shadow vs Production Comparison - {payload.get('as_of')}",
        "",
        "## 1. 运行摘要",
        "",
        f"- comparison_status：`{payload.get('comparison_status')}`",
        f"- market_regime：`{payload.get('market_regime')}`",
        f"- mode：`{payload.get('mode')}`",
        "- production_effect：`none`",
        "- manual_review_only：`true`",
        f"- data_quality_status：`{data_quality.get('market_data_status', 'missing')}`",
        f"- TRADING-018B decision：`{shadow_iteration.get('decision', 'missing')}`",
        "",
        "## 2. Production",
        "",
        *_profile_summary_lines(production),
        "",
        "## 3. Shadow",
        "",
        *_profile_summary_lines(shadow),
        "",
        "## 4. Difference",
        "",
        f"- score_delta：{_format_signed_float(difference.get('score_delta'))}",
        (
            "- normalized_score_delta："
            f"{_format_signed_float(difference.get('normalized_score_delta'), digits=4)}"
        ),
        f"- decision_changed：`{difference.get('decision_changed')}`",
        f"- score_band_changed：`{difference.get('score_band_changed')}`",
        f"- risk_flags_changed：`{difference.get('risk_flags_changed')}`",
        f"- main_reason：{difference.get('main_reason', '')}",
        "",
        "## 5. Contribution Breakdown",
        "",
        (
            "| Component | Production Weight | Shadow Weight | Production Contribution | "
            "Shadow Contribution | Delta |"
        ),
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in _list_mappings(difference.get("contribution_deltas")):
        lines.append(
            "| "
            f"{item.get('component')} | "
            f"{_format_float(item.get('production_normalized_weight'))} | "
            f"{_format_float(item.get('shadow_normalized_weight'))} | "
            f"{_format_float(item.get('production_contribution'))} | "
            f"{_format_float(item.get('shadow_contribution'))} | "
            f"{_format_signed_float(item.get('contribution_delta'))} |"
        )
    blockers = _strings(validation.get("blocking_reasons"))
    warnings = _strings(validation.get("warnings"))
    lines.extend(
        [
            "",
            "## 6. Input Validation",
            "",
            f"- blocking_reasons：{', '.join(blockers) or 'none'}",
            f"- warnings：{', '.join(warnings) or 'none'}",
            "",
            "## 7. Production Safety",
            "",
            "- production_effect：`none`",
            "- manual_review_only：`true`",
            "- broker runner：未触发",
            "- replay runner：未触发",
            "- production profile 修改：无",
            "- approved profile 写入：无",
            "- promotion：无",
            "",
        ]
    )
    return "\n".join(lines)


def _profile_summary_lines(profile: dict[str, Any]) -> list[str]:
    if not profile:
        return ["- 输入不足，未计算。"]
    top = [
        f"{item.get('component')} ({_format_float(item.get('contribution'))})"
        for item in _list_mappings(profile.get("top_contributors"))
    ]
    risk_flags = [
        item.get("gate_id", "")
        for item in _list_mappings(profile.get("risk_flags"))
        if item.get("triggered")
    ]
    return [
        f"- decision：`{profile.get('decision')}`",
        f"- score：{_format_float(profile.get('score'))}/100",
        f"- normalized_score：{_format_float(profile.get('normalized_score'))}",
        f"- score_band：`{_mapping(profile.get('score_band')).get('label', '')}`",
        f"- final_position：{_position_range(_mapping(profile.get('final_position_band')))}",
        f"- top_contributors：{', '.join(top) or 'none'}",
        f"- risk_flags：{', '.join(risk_flags) or 'none'}",
    ]


def _profile_result(
    *,
    profile_id: str,
    weights: dict[str, float],
    components: list[dict[str, Any]],
    position_bands: list[dict[str, Any]],
    non_score_gates: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered_components = [_string_value(item.get("component")) for item in components]
    weight_subset = {key: weights[key] for key in ordered_components}
    total_weight = sum(weight_subset.values())
    breakdown: list[dict[str, Any]] = []
    for component in components:
        name = _string_value(component.get("component"))
        component_score = _float_value(component.get("score"), default=0.0)
        raw_weight = weight_subset[name]
        normalized_weight = raw_weight / total_weight
        contribution = component_score * normalized_weight
        breakdown.append(
            {
                "component": name,
                "component_score": round(component_score, 10),
                "raw_weight": round(raw_weight, 10),
                "normalized_weight": round(normalized_weight, 10),
                "contribution": round(contribution, 10),
                "source_type": _string_value(component.get("source_type")),
                "coverage": component.get("coverage"),
                "confidence": component.get("confidence"),
            }
        )
    score = sum(_float_value(item.get("contribution"), default=0.0) for item in breakdown)
    score_band = _score_band(score, position_bands)
    final_band = _apply_non_score_gates(score_band, non_score_gates)
    risk_flags = [
        {
            "gate_id": "score_model",
            "label": "评分模型仓位",
            "max_position": score_band["max_position"],
            "triggered": True,
            "reason": f"综合评分映射出的 AI 仓位区间上限：{score_band['max_position']:.0%}。",
            "source": "daily_shadow_vs_production_comparison",
        },
        *non_score_gates,
    ]
    return {
        "profile_id": profile_id,
        "score": round(score, 10),
        "normalized_score": round(score / 100.0, 10),
        "decision": final_band["label"],
        "score_band": score_band,
        "final_position_band": final_band,
        "risk_flags": risk_flags,
        "top_contributors": sorted(
            breakdown,
            key=lambda item: _float_value(item.get("contribution"), default=0.0),
            reverse=True,
        )[:TOP_CONTRIBUTOR_DISPLAY_LIMIT],
        "contribution_breakdown": breakdown,
    }


def _comparison_result(
    *,
    production_result: dict[str, Any],
    shadow_result: dict[str, Any],
) -> dict[str, Any]:
    production_breakdown = {
        _string_value(item.get("component")): item
        for item in _list_mappings(production_result.get("contribution_breakdown"))
    }
    shadow_breakdown = {
        _string_value(item.get("component")): item
        for item in _list_mappings(shadow_result.get("contribution_breakdown"))
    }
    contribution_deltas: list[dict[str, Any]] = []
    for component in production_breakdown:
        production_item = production_breakdown[component]
        shadow_item = shadow_breakdown[component]
        contribution_delta = _float_value(shadow_item.get("contribution"), default=0.0) - (
            _float_value(production_item.get("contribution"), default=0.0)
        )
        weight_delta = _float_value(shadow_item.get("normalized_weight"), default=0.0) - (
            _float_value(production_item.get("normalized_weight"), default=0.0)
        )
        contribution_deltas.append(
            {
                "component": component,
                "component_score": production_item.get("component_score"),
                "production_normalized_weight": production_item.get("normalized_weight"),
                "shadow_normalized_weight": shadow_item.get("normalized_weight"),
                "weight_delta": round(weight_delta, 10),
                "production_contribution": production_item.get("contribution"),
                "shadow_contribution": shadow_item.get("contribution"),
                "contribution_delta": round(contribution_delta, 10),
            }
        )
    contribution_deltas.sort(
        key=lambda item: abs(_float_value(item.get("contribution_delta"), default=0.0)),
        reverse=True,
    )
    score_delta = _float_value(shadow_result.get("score"), default=0.0) - _float_value(
        production_result.get("score"),
        default=0.0,
    )
    decision_changed = production_result.get("decision") != shadow_result.get("decision")
    score_band_changed = _mapping(production_result.get("score_band")).get("label") != _mapping(
        shadow_result.get("score_band")
    ).get("label")
    return {
        "score_delta": round(score_delta, 10),
        "normalized_score_delta": round(score_delta / 100.0, 10),
        "decision_changed": decision_changed,
        "score_band_changed": score_band_changed,
        "risk_flags_changed": _risk_signature(production_result) != _risk_signature(shadow_result),
        "main_reason": _main_reason(contribution_deltas),
        "contribution_deltas": contribution_deltas,
        "weight_deltas": [
            {
                "component": item["component"],
                "weight_delta": item["weight_delta"],
            }
            for item in contribution_deltas
        ],
    }


def _main_reason(contribution_deltas: list[dict[str, Any]]) -> str:
    if not contribution_deltas:
        return "production 与 shadow 没有可比较的 contribution 差异。"
    top = contribution_deltas[0]
    delta = _float_value(top.get("contribution_delta"), default=0.0)
    weight_delta = _float_value(top.get("weight_delta"), default=0.0)
    if abs(delta) <= 1e-12 and abs(weight_delta) <= 1e-12:
        return "shadow 权重与 production 在已评分组件上没有实质差异。"
    direction = "提高" if weight_delta > 0 else "降低"
    return (
        f"shadow {direction}了 {top.get('component')} 的 normalized weight "
        f"({_format_signed_float(weight_delta, digits=4)})；该组件 score="
        f"{_format_float(top.get('component_score'))}，对总分差异贡献 "
        f"{_format_signed_float(delta)}。"
    )


def _validation(
    *,
    source_artifacts: dict[str, dict[str, Any]],
    components: list[dict[str, Any]],
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
    position_bands: list[dict[str, Any]],
    current_shadow: dict[str, Any],
    shadow_candidate: dict[str, Any],
    data_quality: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    for key in (
        "production_profile",
        "current_shadow_weights",
        "shadow_iteration_candidate",
        "decision_snapshot",
        "scoring_rules",
    ):
        if not bool(source_artifacts[key].get("valid")):
            blockers.append(f"{key}_missing_or_invalid")
    if current_shadow and current_shadow.get("production_effect") != PRODUCTION_EFFECT_NONE:
        blockers.append("current_shadow_production_effect_not_none")
    if current_shadow and current_shadow.get("manual_review_only") is not True:
        blockers.append("current_shadow_not_manual_review_only")
    if shadow_candidate and shadow_candidate.get("production_effect") != PRODUCTION_EFFECT_NONE:
        blockers.append("shadow_iteration_candidate_production_effect_not_none")
    if shadow_candidate and shadow_candidate.get("manual_review_only") is not True:
        blockers.append("shadow_iteration_candidate_not_manual_review_only")
    component_names = [_string_value(item.get("component")) for item in components]
    if not component_names:
        blockers.append("missing_decision_snapshot_component_scores")
    if not position_bands:
        blockers.append("missing_position_bands")
    production_missing = sorted(set(component_names) - set(production_weights))
    shadow_missing = sorted(set(component_names) - set(shadow_weights))
    if production_missing:
        blockers.append("production_weights_missing_components:" + ",".join(production_missing))
    if shadow_missing:
        blockers.append("shadow_weights_missing_components:" + ",".join(shadow_missing))
    production_extra = sorted(set(production_weights) - set(component_names))
    shadow_extra = sorted(set(shadow_weights) - set(component_names))
    if production_extra:
        warnings.append("production_weights_extra_components:" + ",".join(production_extra))
    if shadow_extra:
        warnings.append("shadow_weights_extra_components:" + ",".join(shadow_extra))
    if any(value < 0 for value in production_weights.values()):
        blockers.append("production_weights_negative")
    if any(value < 0 for value in shadow_weights.values()):
        blockers.append("shadow_weights_negative")
    if component_names and sum(production_weights.get(key, 0.0) for key in component_names) <= 0:
        blockers.append("production_weights_total_not_positive")
    if component_names and sum(shadow_weights.get(key, 0.0) for key in component_names) <= 0:
        blockers.append("shadow_weights_total_not_positive")
    market_status = _string_value(data_quality.get("market_data_status"))
    feature_status = _string_value(data_quality.get("feature_status"))
    if market_status.startswith("FAIL") or feature_status.startswith("FAIL"):
        blockers.append("upstream_data_quality_failed")
    elif market_status and market_status != "PASS":
        warnings.append(f"market_data_status:{market_status}")
    if feature_status and feature_status != "PASS":
        warnings.append(f"feature_status:{feature_status}")
    return {
        "blocking_reasons": _dedupe(blockers),
        "warnings": _dedupe(warnings),
        "component_count": len(component_names),
        "component_names": component_names,
        "production_weight_keys": sorted(production_weights),
        "shadow_weight_keys": sorted(shadow_weights),
        "position_band_count": len(position_bands),
    }


def _score_components(decision_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    scores = _mapping(decision_snapshot.get("scores"))
    components: list[dict[str, Any]] = []
    for item in _list_mappings(scores.get("components")):
        name = _string_value(item.get("component"))
        score = _optional_float(item.get("score"))
        if not name or score is None:
            continue
        components.append(
            {
                "component": name,
                "score": score,
                "source_type": _string_value(item.get("source_type")),
                "coverage": item.get("coverage"),
                "confidence": item.get("confidence"),
                "reason": _string_value(item.get("reason")),
            }
        )
    return components


def _position_bands(scoring_rules: dict[str, Any]) -> list[dict[str, Any]]:
    bands: list[dict[str, Any]] = []
    for item in _list_mappings(scoring_rules.get("position_bands")):
        min_score = _optional_float(item.get("min_score"))
        min_position = _optional_float(item.get("min_position"))
        max_position = _optional_float(item.get("max_position"))
        label = _string_value(item.get("label"))
        if min_score is None or min_position is None or max_position is None or not label:
            continue
        bands.append(
            {
                "min_score": min_score,
                "min_position": min_position,
                "max_position": max_position,
                "label": label,
            }
        )
    return sorted(bands, key=lambda item: item["min_score"], reverse=True)


def _score_band(score: float, position_bands: list[dict[str, Any]]) -> dict[str, Any]:
    for band in position_bands:
        if score >= _float_value(band.get("min_score"), default=0.0):
            return {
                "min_score": band["min_score"],
                "min_position": band["min_position"],
                "max_position": band["max_position"],
                "label": band["label"],
            }
    floor = position_bands[-1]
    return {
        "min_score": floor["min_score"],
        "min_position": floor["min_position"],
        "max_position": floor["max_position"],
        "label": floor["label"],
    }


def _apply_non_score_gates(
    score_band: dict[str, Any],
    non_score_gates: list[dict[str, Any]],
) -> dict[str, Any]:
    score_max = _float_value(score_band.get("max_position"), default=1.0)
    final_max = score_max
    binding_gate = ""
    for gate in non_score_gates:
        max_position = _optional_float(gate.get("max_position"))
        if max_position is None:
            continue
        if max_position < final_max:
            final_max = max_position
            binding_gate = _string_value(gate.get("gate_id"))
    label = _string_value(score_band.get("label"))
    if final_max < score_max - 1e-12:
        label = f"{label}/仓位受限"
    return {
        "min_position": min(_float_value(score_band.get("min_position"), default=0.0), final_max),
        "max_position": final_max,
        "label": label,
        "binding_gate": binding_gate,
    }


def _non_score_position_gates(decision_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    positions = _mapping(decision_snapshot.get("positions"))
    gates = []
    for gate in _list_mappings(positions.get("position_gates")):
        if _string_value(gate.get("gate_id")) == "score_model":
            continue
        max_position = _optional_float(gate.get("max_position"))
        if max_position is None:
            continue
        gates.append(
            {
                "gate_id": _string_value(gate.get("gate_id")),
                "label": _string_value(gate.get("label")),
                "source": _string_value(gate.get("source")),
                "max_position": max_position,
                "triggered": bool(gate.get("triggered")),
                "reason": _string_value(gate.get("reason")),
            }
        )
    return gates


def _risk_signature(profile: dict[str, Any]) -> tuple[tuple[str, float, bool], ...]:
    signature = []
    for gate in _list_mappings(profile.get("risk_flags")):
        signature.append(
            (
                _string_value(gate.get("gate_id")),
                round(_float_value(gate.get("max_position"), default=0.0), 10),
                bool(gate.get("triggered")),
            )
        )
    return tuple(signature)


def _data_quality_record(
    decision_snapshot: dict[str, Any],
    daily_summary: dict[str, Any],
) -> dict[str, Any]:
    quality = _mapping(decision_snapshot.get("quality"))
    daily_gate = _mapping(daily_summary.get("data_gate"))
    return {
        "market_data_status": _string_value(quality.get("market_data_status"))
        or _string_value(daily_gate.get("status"))
        or "missing",
        "market_data_error_count": quality.get("market_data_error_count"),
        "market_data_warning_count": quality.get("market_data_warning_count"),
        "feature_status": _string_value(quality.get("feature_status")) or "missing",
        "feature_warning_count": quality.get("feature_warning_count"),
        "sec_feature_status": quality.get("sec_feature_status"),
        "source": "decision_snapshot.quality",
    }


def _shadow_iteration_record(
    shadow_candidate: dict[str, Any],
    current_shadow: dict[str, Any],
) -> dict[str, Any]:
    audit = _mapping(current_shadow.get("audit"))
    run_log = _mapping(shadow_candidate.get("run_log"))
    return {
        "candidate_decision": _string_value(shadow_candidate.get("decision")),
        "decision": _string_value(shadow_candidate.get("decision"))
        or _string_value(audit.get("last_decision"))
        or "missing",
        "decision_reason": _string_value(shadow_candidate.get("decision_reason"))
        or _string_value(audit.get("last_reason")),
        "current_state_updated": run_log.get("current_state_updated"),
        "history_written": run_log.get("history_written"),
        "last_updated_date": _string_value(current_shadow.get("last_updated_date")) or "missing",
        "update_count": audit.get("update_count"),
        "latest_delta": _mapping(shadow_candidate.get("proposed_delta")),
    }


def _market_regime_id(decision_snapshot: dict[str, Any]) -> str:
    market_regime = _mapping(decision_snapshot.get("market_regime"))
    return _string_value(market_regime.get("regime_id")) or "ai_after_chatgpt"


def _artifact_record(
    path: Path,
    base_dir: Path,
    *,
    expected_report_type: str | None = None,
    payload: dict[str, Any] | None = None,
    optional: bool = False,
) -> dict[str, Any]:
    exists = path.exists()
    checksum = _sha256(path) if exists and path.is_file() else ""
    actual_report_type = _string_value((payload or {}).get("report_type"))
    report_type_valid = True
    if expected_report_type is not None:
        report_type_valid = actual_report_type == expected_report_type
    valid = bool(exists and report_type_valid)
    if optional and not exists:
        valid = True
    return {
        "path": str(path),
        "href": _report_href(path, base_dir),
        "exists": exists,
        "optional": optional,
        "checksum_sha256": checksum,
        "size_bytes": path.stat().st_size if exists else 0,
        "expected_report_type": expected_report_type or "",
        "report_type": actual_report_type,
        "valid": valid,
    }


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("shadow vs production comparison production_effect must be none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("shadow vs production comparison manual_review_only must be true")
    contract = _mapping(payload.get("pipeline_contract"))
    for field in (
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is not False:
            raise ValueError(f"unsafe comparison contract field: {field}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = safe_load_yaml_path(path) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: Any, *, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_href(path: Path, base_dir: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(base_dir.resolve()).as_posix()
        except (OSError, RuntimeError, ValueError):
            return path.as_posix()


def _format_float(value: Any) -> str:
    parsed = _optional_float(value)
    return "NA" if parsed is None else f"{parsed:.2f}"


def _format_signed_float(value: Any, *, digits: int = 2) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NA"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.{digits}f}"


def _position_range(band: dict[str, Any]) -> str:
    minimum = _optional_float(band.get("min_position"))
    maximum = _optional_float(band.get("max_position"))
    if minimum is None or maximum is None:
        return "NA"
    return f"{minimum:.0%}-{maximum:.0%}"
