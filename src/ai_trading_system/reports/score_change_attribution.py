from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
REPORT_TYPE = "score_change_attribution"
PRODUCTION_EFFECT = "none"


def default_score_change_attribution_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"score_change_attribution_{as_of.isoformat()}.json"


def default_score_change_attribution_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"score_change_attribution_{as_of.isoformat()}.md"


def build_score_change_attribution_payload(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    previous_decision_snapshot_path: Path | None = None,
    snapshot_dir: Path | None = None,
) -> dict[str, Any]:
    current = _read_json_object(decision_snapshot_path, "decision_snapshot")
    warnings: list[str] = []
    current_signal_date = _snapshot_signal_date(current)
    if current_signal_date is not None and current_signal_date != as_of:
        warnings.append(
            "decision_snapshot_signal_date_mismatch:"
            f"expected={as_of.isoformat()};actual={current_signal_date.isoformat()}"
        )

    previous_path = previous_decision_snapshot_path
    if previous_path is None:
        previous_path = _find_previous_snapshot_path(
            snapshot_dir or decision_snapshot_path.parent,
            as_of,
        )
    if previous_path is None or not previous_path.exists():
        if previous_path is None:
            warnings.append("previous_decision_snapshot_missing")
        else:
            warnings.append(f"previous_decision_snapshot_missing:{previous_path}")
        return _insufficient_payload(
            as_of=as_of,
            current=current,
            decision_snapshot_path=decision_snapshot_path,
            previous_decision_snapshot_path=previous_path,
            snapshot_dir=snapshot_dir or decision_snapshot_path.parent,
            warnings=warnings,
        )

    previous = _read_json_object(previous_path, "previous_decision_snapshot")
    previous_signal_date = _snapshot_signal_date(previous)
    if previous_signal_date is None:
        warnings.append("previous_decision_snapshot_signal_date_missing")
        return _insufficient_payload(
            as_of=as_of,
            current=current,
            decision_snapshot_path=decision_snapshot_path,
            previous_decision_snapshot_path=previous_path,
            snapshot_dir=snapshot_dir or decision_snapshot_path.parent,
            warnings=warnings,
        )
    if previous_signal_date >= as_of:
        warnings.append(
            "previous_decision_snapshot_not_prior:"
            f"current={as_of.isoformat()};previous={previous_signal_date.isoformat()}"
        )
        return _insufficient_payload(
            as_of=as_of,
            current=current,
            decision_snapshot_path=decision_snapshot_path,
            previous_decision_snapshot_path=previous_path,
            snapshot_dir=snapshot_dir or decision_snapshot_path.parent,
            warnings=warnings,
        )

    components = _component_attribution(current, previous)
    gates = _gate_attribution(current, previous)
    overall = _overall_attribution(current, previous)
    confidence = _confidence_attribution(current, previous)
    position = _position_attribution(current, previous)
    data_quality = _data_quality_attribution(current, previous)
    top_changes = _top_changes(components, gates, confidence, data_quality)
    current_binding_gate = _binding_gate_record(current)
    previous_binding_gate = _binding_gate_record(previous)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": "PASS_WITH_WARNINGS" if warnings else "PASS",
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": _market_regime_record(current),
        "comparison_window": {
            "requested_as_of": as_of.isoformat(),
            "current_signal_date": (
                current_signal_date.isoformat() if current_signal_date else None
            ),
            "previous_signal_date": previous_signal_date.isoformat(),
        },
        "current_date": (
            current_signal_date.isoformat() if current_signal_date else as_of.isoformat()
        ),
        "previous_date": previous_signal_date.isoformat(),
        "source_inputs": _source_inputs(
            decision_snapshot_path=decision_snapshot_path,
            previous_decision_snapshot_path=previous_path,
            snapshot_dir=snapshot_dir or decision_snapshot_path.parent,
        ),
        "warnings": warnings,
        "overall_score_delta": overall,
        "overall_score_current": overall.get("current"),
        "overall_score_previous": overall.get("previous"),
        "component_attribution": components,
        "component_score_deltas": _component_score_deltas(components),
        "component_contribution_deltas": _component_contribution_deltas(components),
        "confidence_attribution": confidence,
        "position_attribution": position,
        "final_position_current": _position_band(current),
        "final_position_previous": _position_band(previous),
        "final_position_delta": {
            "min_delta": position.get("final_min_delta"),
            "max_delta": position.get("final_max_delta"),
        },
        "gate_attribution": gates,
        "binding_gate_current": current_binding_gate,
        "binding_gate_previous": previous_binding_gate,
        "binding_gate_changed": _text(current_binding_gate.get("gate_id"))
        != _text(previous_binding_gate.get("gate_id")),
        "gate_state_changes": _gate_state_changes(gates),
        "data_quality_attribution": data_quality,
        "data_quality_status_delta": data_quality,
        "manual_review_count_current": _manual_review_count(current),
        "manual_review_count_previous": _manual_review_count(previous),
        "manual_review_count_delta": _manual_review_count(current) - _manual_review_count(previous),
        "top_changes": top_changes,
        "top_positive_change_drivers": _records(top_changes.get("positive_contribution_drivers"))
        or _records(top_changes.get("positive_score_drivers")),
        "top_negative_change_drivers": _records(top_changes.get("negative_contribution_drivers"))
        or _records(top_changes.get("negative_score_drivers")),
        "methodology": {
            "component_contribution_formula": "component_score * effective_weight",
            "component_delta_decomposition": (
                "score_delta_effect=(current_score-previous_score)*previous_weight; "
                "weight_delta_effect=previous_score*(current_weight-previous_weight); "
                "interaction_effect=(current_score-previous_score)*(current_weight-previous_weight)"
            ),
            "production_effect": PRODUCTION_EFFECT,
            "does_not_recompute_score": True,
        },
    }


def write_score_change_attribution_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_score_change_attribution_report(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_score_change_attribution_markdown(payload), encoding="utf-8")
    return output_path


def render_score_change_attribution_markdown(payload: Mapping[str, Any]) -> str:
    as_of = _text(payload.get("as_of"), "UNKNOWN")
    status = _text(payload.get("status"), "UNKNOWN")
    market_regime = _mapping(payload.get("market_regime"))
    window = _mapping(payload.get("comparison_window"))
    overall = _mapping(payload.get("overall_score_delta"))
    confidence = _mapping(payload.get("confidence_attribution"))
    position = _mapping(payload.get("position_attribution"))
    data_quality = _mapping(payload.get("data_quality_attribution"))
    top_changes = _mapping(payload.get("top_changes"))
    final_position_delta = _delta_line(
        position,
        "current_final_max",
        "previous_final_max",
        "final_max_delta",
    )
    positive_change_drivers = _driver_list_from_records(payload.get("top_positive_change_drivers"))
    negative_change_drivers = _driver_list_from_records(payload.get("top_negative_change_drivers"))
    lines = [
        f"# Score Change Attribution {as_of}",
        "",
        f"- 状态：{status}",
        f"- 市场阶段：`{_text(market_regime.get('regime_id'), 'ai_after_chatgpt')}`",
        (
            "- 对比窗口："
            f"{_text(window.get('previous_signal_date'), 'UNKNOWN')} -> "
            f"{_text(window.get('current_signal_date'), as_of)}"
        ),
        "- production_effect=none；本报告只读比较 signal-time snapshot，不重算 score、"
        "不修改 weights/gates，也不是交易指令。",
        "",
    ]
    if status == "INSUFFICIENT_DATA":
        lines.extend(
            [
                "## 限制",
                "",
                (
                    "- 缺少有效上一条 decision snapshot，不能生成变化归因；"
                    "不得用当前状态补造变化原因。"
                ),
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## 核心变化",
            "",
            f"- Overall score：{_delta_line(overall, 'current', 'previous', 'delta')}",
            (
                "- Confidence："
                f"{_delta_line(confidence, 'current_score', 'previous_score', 'score_delta')}；"
                f" level {_text(confidence.get('previous_level'), 'UNKNOWN')} -> "
                f"{_text(confidence.get('current_level'), 'UNKNOWN')}"
            ),
            f"- Final AI position max：{final_position_delta}",
            (
                "- Data Gate："
                f"{_text(data_quality.get('previous_market_data_status'), 'UNKNOWN')} -> "
                f"{_text(data_quality.get('current_market_data_status'), 'UNKNOWN')}"
            ),
            (
                "- Binding gate："
                f"{_text(_mapping(payload.get('binding_gate_previous')).get('gate_id'), 'UNKNOWN')}"
                " -> "
                f"{_text(_mapping(payload.get('binding_gate_current')).get('gate_id'), 'UNKNOWN')}"
            ),
            (
                "- Manual review count："
                f"{_text(payload.get('manual_review_count_previous'), 'UNKNOWN')} -> "
                f"{_text(payload.get('manual_review_count_current'), 'UNKNOWN')} "
                f"({_text(payload.get('manual_review_count_delta'), 'UNKNOWN')})"
            ),
            "",
            "## 读者解释",
            "",
            f"- 今天变化主要来自：{positive_change_drivers}；负向拖累来自："
            f"{negative_change_drivers}。",
            f"- {_score_vs_gate_sentence(payload)}",
            f"- {_final_position_sentence(payload)}",
            f"- {_quality_manual_review_sentence(payload)}",
            "",
            "## Top Changes",
            "",
            (
                "- Positive contribution drivers："
                f"{_driver_list(top_changes, 'positive_contribution_drivers')}"
            ),
            (
                "- Negative contribution drivers："
                f"{_driver_list(top_changes, 'negative_contribution_drivers')}"
            ),
            f"- Weight changes：{_driver_list(top_changes, 'weight_changes')}",
            f"- Coverage changes：{_driver_list(top_changes, 'coverage_changes')}",
            f"- Gate changes：{_driver_list(top_changes, 'gate_changes')}",
            "",
            "## Component Attribution",
            "",
            "| Component | Status | Score delta effect | Weight delta effect | "
            "Interaction | Contribution delta | Coverage delta | Confidence delta |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in _records(payload.get("component_attribution")):
        lines.append(
            "| "
            f"`{_text(row.get('component'))}` | "
            f"{_text(row.get('status'))} | "
            f"{_format_optional_number(row.get('score_delta_effect'))} | "
            f"{_format_optional_number(row.get('weight_delta_effect'))} | "
            f"{_format_optional_number(row.get('interaction_effect'))} | "
            f"{_format_optional_number(row.get('contribution_delta'))} | "
            f"{_format_optional_number(row.get('coverage_delta'))} | "
            f"{_format_optional_number(row.get('confidence_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## Gate Attribution",
            "",
            "| Gate | Status | Previous cap | Current cap | Cap delta | "
            "Previous triggered | Current triggered | Binding change |",
            "|---|---|---:|---:|---:|---|---|---|",
        ]
    )
    for row in _records(payload.get("gate_attribution")):
        lines.append(
            "| "
            f"`{_text(row.get('gate_id'))}` | "
            f"{_text(row.get('status'))} | "
            f"{_format_percent(row.get('previous_cap'))} | "
            f"{_format_percent(row.get('current_cap'))} | "
            f"{_format_optional_number(row.get('cap_delta'))} | "
            f"{_text(row.get('previous_triggered'), 'UNKNOWN')} | "
            f"{_text(row.get('current_triggered'), 'UNKNOWN')} | "
            f"{_text(row.get('binding_change'), 'none')} |"
        )
    lines.extend(
        [
            "",
            "## 审计边界",
            "",
            (
                "- component contribution 使用 snapshot 中 component score "
                "与归一化 effective weight 分解。"
            ),
            "- coverage / confidence / gate 变化是解释维度，不代表自动因果确认或交易建议。",
            "- 缺少上一快照、字段缺失或非 prior 快照时必须输出 `INSUFFICIENT_DATA`。",
        ]
    )
    return "\n".join(lines)


def _insufficient_payload(
    *,
    as_of: date,
    current: Mapping[str, Any],
    decision_snapshot_path: Path,
    previous_decision_snapshot_path: Path | None,
    snapshot_dir: Path,
    warnings: list[str],
) -> dict[str, Any]:
    current_signal_date = _snapshot_signal_date(current)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": "INSUFFICIENT_DATA",
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": _market_regime_record(current),
        "comparison_window": {
            "requested_as_of": as_of.isoformat(),
            "current_signal_date": (
                current_signal_date.isoformat() if current_signal_date else None
            ),
            "previous_signal_date": None,
        },
        "current_date": (
            current_signal_date.isoformat() if current_signal_date else as_of.isoformat()
        ),
        "previous_date": None,
        "source_inputs": _source_inputs(
            decision_snapshot_path=decision_snapshot_path,
            previous_decision_snapshot_path=previous_decision_snapshot_path,
            snapshot_dir=snapshot_dir,
        ),
        "warnings": warnings,
        "overall_score_delta": {},
        "overall_score_current": _float_or_none(
            _mapping(current.get("scores")).get("overall_score")
        ),
        "overall_score_previous": None,
        "component_attribution": [],
        "component_score_deltas": [],
        "component_contribution_deltas": [],
        "confidence_attribution": {},
        "position_attribution": {},
        "final_position_current": _position_band(current),
        "final_position_previous": {},
        "final_position_delta": {},
        "gate_attribution": [],
        "binding_gate_current": _binding_gate_record(current),
        "binding_gate_previous": {},
        "binding_gate_changed": False,
        "gate_state_changes": [],
        "data_quality_attribution": {},
        "data_quality_status_delta": {},
        "manual_review_count_current": _manual_review_count(current),
        "manual_review_count_previous": None,
        "manual_review_count_delta": None,
        "top_changes": {},
        "top_positive_change_drivers": [],
        "top_negative_change_drivers": [],
        "methodology": {
            "production_effect": PRODUCTION_EFFECT,
            "does_not_recompute_score": True,
        },
    }


def _component_attribution(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> list[dict[str, Any]]:
    current_components = _component_index(current)
    previous_components = _component_index(previous)
    component_ids = list(dict.fromkeys([*current_components.keys(), *previous_components.keys()]))
    rows: list[dict[str, Any]] = []
    for component_id in component_ids:
        current_row = current_components.get(component_id)
        previous_row = previous_components.get(component_id)
        current_score = _float_or_none(_mapping(current_row).get("score"))
        previous_score = _float_or_none(_mapping(previous_row).get("score"))
        current_weight = _float_or_none(_mapping(current_row).get("effective_weight"))
        previous_weight = _float_or_none(_mapping(previous_row).get("effective_weight"))
        current_contribution = _multiply(current_score, current_weight)
        previous_contribution = _multiply(previous_score, previous_weight)
        score_delta = _delta(current_score, previous_score)
        weight_delta = _delta(current_weight, previous_weight)
        coverage_delta = _delta(
            _float_or_none(_mapping(current_row).get("coverage")),
            _float_or_none(_mapping(previous_row).get("coverage")),
        )
        confidence_delta = _delta(
            _float_or_none(_mapping(current_row).get("confidence")),
            _float_or_none(_mapping(previous_row).get("confidence")),
        )
        source_type_changed = _text(_mapping(current_row).get("source_type")) != _text(
            _mapping(previous_row).get("source_type")
        )
        rows.append(
            {
                "component": component_id,
                "status": _component_status(
                    current=current_row,
                    previous=previous_row,
                    numeric_deltas=(
                        score_delta,
                        weight_delta,
                        coverage_delta,
                        confidence_delta,
                        _delta(current_contribution, previous_contribution),
                    ),
                    source_type_changed=source_type_changed,
                ),
                "current_score": current_score,
                "previous_score": previous_score,
                "score_delta": score_delta,
                "current_effective_weight": current_weight,
                "previous_effective_weight": previous_weight,
                "effective_weight_delta": weight_delta,
                "current_coverage": _float_or_none(_mapping(current_row).get("coverage")),
                "previous_coverage": _float_or_none(_mapping(previous_row).get("coverage")),
                "coverage_delta": coverage_delta,
                "current_confidence": _float_or_none(_mapping(current_row).get("confidence")),
                "previous_confidence": _float_or_none(_mapping(previous_row).get("confidence")),
                "confidence_delta": confidence_delta,
                "current_source_type": _text(_mapping(current_row).get("source_type")),
                "previous_source_type": _text(_mapping(previous_row).get("source_type")),
                "source_type_changed": source_type_changed,
                "current_contribution": current_contribution,
                "previous_contribution": previous_contribution,
                "contribution_delta": _delta(current_contribution, previous_contribution),
                "score_delta_effect": _multiply(score_delta, previous_weight),
                "weight_delta_effect": _multiply(previous_score, weight_delta),
                "interaction_effect": _multiply(score_delta, weight_delta),
                "current_reason": _text(_mapping(current_row).get("reason")),
                "previous_reason": _text(_mapping(previous_row).get("reason")),
            }
        )
    return rows


def _gate_attribution(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> list[dict[str, Any]]:
    current_gates = _gate_index(current)
    previous_gates = _gate_index(previous)
    current_binding = _binding_gate_id(current)
    previous_binding = _binding_gate_id(previous)
    gate_ids = list(dict.fromkeys([*current_gates.keys(), *previous_gates.keys()]))
    rows: list[dict[str, Any]] = []
    for gate_id in gate_ids:
        current_gate = current_gates.get(gate_id)
        previous_gate = previous_gates.get(gate_id)
        current_cap = _float_or_none(_mapping(current_gate).get("max_position"))
        previous_cap = _float_or_none(_mapping(previous_gate).get("max_position"))
        flags = _gate_change_flags(
            current_gate=current_gate,
            previous_gate=previous_gate,
            current_binding=current_binding,
            previous_binding=previous_binding,
            gate_id=gate_id,
            current_cap=current_cap,
            previous_cap=previous_cap,
        )
        rows.append(
            {
                "gate_id": gate_id,
                "label": _text(
                    _mapping(current_gate).get("label"),
                    _text(_mapping(previous_gate).get("label")),
                ),
                "status": "UNCHANGED" if not flags else "CHANGED",
                "change_flags": flags,
                "previous_cap": previous_cap,
                "current_cap": current_cap,
                "cap_delta": _delta(current_cap, previous_cap),
                "previous_triggered": _mapping(previous_gate).get("triggered"),
                "current_triggered": _mapping(current_gate).get("triggered"),
                "previous_binding": gate_id == previous_binding,
                "current_binding": gate_id == current_binding,
                "binding_change": _binding_change_text(
                    gate_id,
                    current_binding=current_binding,
                    previous_binding=previous_binding,
                ),
                "previous_reason": _text(_mapping(previous_gate).get("reason")),
                "current_reason": _text(_mapping(current_gate).get("reason")),
            }
        )
    return rows


def _overall_attribution(current: Mapping[str, Any], previous: Mapping[str, Any]) -> dict[str, Any]:
    current_score = _float_or_none(_mapping(current.get("scores")).get("overall_score"))
    previous_score = _float_or_none(_mapping(previous.get("scores")).get("overall_score"))
    return {
        "current": current_score,
        "previous": previous_score,
        "delta": _delta(current_score, previous_score),
    }


def _confidence_attribution(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> dict[str, Any]:
    current_scores = _mapping(current.get("scores"))
    previous_scores = _mapping(previous.get("scores"))
    current_score = _float_or_none(current_scores.get("confidence_score"))
    previous_score = _float_or_none(previous_scores.get("confidence_score"))
    current_band = _mapping(
        _mapping(current.get("positions")).get("confidence_adjusted_risk_asset_ai_band")
    )
    previous_band = _mapping(
        _mapping(previous.get("positions")).get("confidence_adjusted_risk_asset_ai_band")
    )
    return {
        "current_score": current_score,
        "previous_score": previous_score,
        "score_delta": _delta(current_score, previous_score),
        "current_level": _text(current_scores.get("confidence_level")),
        "previous_level": _text(previous_scores.get("confidence_level")),
        "current_confidence_adjusted_max": _float_or_none(current_band.get("max_position")),
        "previous_confidence_adjusted_max": _float_or_none(previous_band.get("max_position")),
        "confidence_adjusted_max_delta": _delta(
            _float_or_none(current_band.get("max_position")),
            _float_or_none(previous_band.get("max_position")),
        ),
    }


def _position_attribution(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> dict[str, Any]:
    current_final = _mapping(_mapping(current.get("positions")).get("final_risk_asset_ai_band"))
    previous_final = _mapping(_mapping(previous.get("positions")).get("final_risk_asset_ai_band"))
    current_model = _mapping(_mapping(current.get("positions")).get("model_risk_asset_ai_band"))
    previous_model = _mapping(_mapping(previous.get("positions")).get("model_risk_asset_ai_band"))
    return {
        "current_model_min": _float_or_none(current_model.get("min_position")),
        "previous_model_min": _float_or_none(previous_model.get("min_position")),
        "model_min_delta": _delta(
            _float_or_none(current_model.get("min_position")),
            _float_or_none(previous_model.get("min_position")),
        ),
        "current_model_max": _float_or_none(current_model.get("max_position")),
        "previous_model_max": _float_or_none(previous_model.get("max_position")),
        "model_max_delta": _delta(
            _float_or_none(current_model.get("max_position")),
            _float_or_none(previous_model.get("max_position")),
        ),
        "current_final_min": _float_or_none(current_final.get("min_position")),
        "previous_final_min": _float_or_none(previous_final.get("min_position")),
        "final_min_delta": _delta(
            _float_or_none(current_final.get("min_position")),
            _float_or_none(previous_final.get("min_position")),
        ),
        "current_final_max": _float_or_none(current_final.get("max_position")),
        "previous_final_max": _float_or_none(previous_final.get("max_position")),
        "final_max_delta": _delta(
            _float_or_none(current_final.get("max_position")),
            _float_or_none(previous_final.get("max_position")),
        ),
    }


def _data_quality_attribution(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> dict[str, Any]:
    current_quality = _mapping(current.get("quality"))
    previous_quality = _mapping(previous.get("quality"))
    return {
        "current_market_data_status": _text(current_quality.get("market_data_status")),
        "previous_market_data_status": _text(previous_quality.get("market_data_status")),
        "market_data_status_changed": _text(current_quality.get("market_data_status"))
        != _text(previous_quality.get("market_data_status")),
        "current_feature_status": _text(current_quality.get("feature_status")),
        "previous_feature_status": _text(previous_quality.get("feature_status")),
        "feature_status_changed": _text(current_quality.get("feature_status"))
        != _text(previous_quality.get("feature_status")),
        "current_sec_feature_status": _text(current_quality.get("sec_feature_status")),
        "previous_sec_feature_status": _text(previous_quality.get("sec_feature_status")),
        "sec_feature_status_changed": _text(current_quality.get("sec_feature_status"))
        != _text(previous_quality.get("sec_feature_status")),
        "market_data_error_delta": _delta(
            _float_or_none(current_quality.get("market_data_error_count")),
            _float_or_none(previous_quality.get("market_data_error_count")),
        ),
        "market_data_warning_delta": _delta(
            _float_or_none(current_quality.get("market_data_warning_count")),
            _float_or_none(previous_quality.get("market_data_warning_count")),
        ),
    }


def _top_changes(
    components: list[dict[str, Any]],
    gates: list[dict[str, Any]],
    confidence: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "positive_contribution_drivers": _component_drivers(
            components,
            field="contribution_delta",
            positive=True,
        ),
        "negative_contribution_drivers": _component_drivers(
            components,
            field="contribution_delta",
            positive=False,
        ),
        "positive_score_drivers": _component_drivers(
            components,
            field="score_delta",
            positive=True,
        ),
        "negative_score_drivers": _component_drivers(
            components,
            field="score_delta",
            positive=False,
        ),
        "weight_changes": _component_drivers(
            components,
            field="effective_weight_delta",
            positive=None,
        ),
        "coverage_changes": _component_drivers(components, field="coverage_delta", positive=None),
        "confidence_changes": [
            {
                "scope": "overall_confidence",
                "score_delta": confidence.get("score_delta"),
                "level_change": (
                    f"{_text(confidence.get('previous_level'), 'UNKNOWN')} -> "
                    f"{_text(confidence.get('current_level'), 'UNKNOWN')}"
                ),
            }
        ],
        "gate_changes": [
            {
                "gate_id": row["gate_id"],
                "cap_delta": row.get("cap_delta"),
                "change_flags": row.get("change_flags"),
            }
            for row in gates
            if row.get("change_flags")
        ],
        "data_quality_changes": [
            key
            for key in (
                "market_data_status_changed",
                "feature_status_changed",
                "sec_feature_status_changed",
            )
            if data_quality.get(key) is True
        ],
        "new_components": [row["component"] for row in components if row.get("status") == "ADDED"],
        "missing_components": [
            row["component"] for row in components if row.get("status") == "REMOVED"
        ],
    }


def _component_score_deltas(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "component": row.get("component"),
            "status": row.get("status"),
            "current_score": row.get("current_score"),
            "previous_score": row.get("previous_score"),
            "score_delta": row.get("score_delta"),
            "current_reason": row.get("current_reason"),
            "previous_reason": row.get("previous_reason"),
        }
        for row in rows
    ]


def _component_contribution_deltas(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "component": row.get("component"),
            "status": row.get("status"),
            "current_contribution": row.get("current_contribution"),
            "previous_contribution": row.get("previous_contribution"),
            "contribution_delta": row.get("contribution_delta"),
            "score_delta_effect": row.get("score_delta_effect"),
            "weight_delta_effect": row.get("weight_delta_effect"),
            "interaction_effect": row.get("interaction_effect"),
        }
        for row in rows
    ]


def _gate_state_changes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "gate_id": row.get("gate_id"),
            "label": row.get("label"),
            "previous_cap": row.get("previous_cap"),
            "current_cap": row.get("current_cap"),
            "cap_delta": row.get("cap_delta"),
            "previous_triggered": row.get("previous_triggered"),
            "current_triggered": row.get("current_triggered"),
            "binding_change": row.get("binding_change"),
            "change_flags": row.get("change_flags"),
        }
        for row in rows
        if row.get("change_flags")
    ]


def _position_band(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    final = _mapping(_mapping(snapshot.get("positions")).get("final_risk_asset_ai_band"))
    return {
        "min": _float_or_none(final.get("min_position")),
        "max": _float_or_none(final.get("max_position")),
        "label": _text(final.get("label")),
    }


def _binding_gate_record(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    gate_id = _binding_gate_id(snapshot)
    gate = _gate_index(snapshot).get(gate_id, {})
    return {
        "gate_id": gate_id,
        "label": _text(gate.get("label")),
        "max_position": _float_or_none(gate.get("max_position")),
        "triggered": gate.get("triggered"),
        "reason": _text(gate.get("reason")),
    }


def _manual_review_count(snapshot: Mapping[str, Any]) -> int:
    return len(
        [
            item
            for item in _records(snapshot.get("manual_review"))
            if _text(item.get("status"), "UNKNOWN") != "PASS"
        ]
    )


def _component_drivers(
    rows: list[dict[str, Any]],
    *,
    field: str,
    positive: bool | None,
) -> list[dict[str, Any]]:
    candidates = []
    for row in rows:
        value = _float_or_none(row.get(field))
        if value is None or value == 0:
            continue
        if positive is True and value <= 0:
            continue
        if positive is False and value >= 0:
            continue
        candidates.append({"component": row["component"], field: value})
    return sorted(candidates, key=lambda item: abs(float(item[field])), reverse=True)


def _component_index(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    components = _records(_mapping(snapshot.get("scores")).get("components"))
    total_weight = sum(
        weight
        for weight in (_float_or_none(component.get("weight")) for component in components)
        if weight is not None
    )
    result: dict[str, dict[str, Any]] = {}
    for component in components:
        component_id = _text(component.get("component"))
        if not component_id:
            continue
        row = dict(component)
        raw_weight = _float_or_none(row.get("weight"))
        row["effective_weight"] = (
            None if raw_weight is None or total_weight <= 0 else raw_weight / total_weight
        )
        result[component_id] = row
    return result


def _gate_index(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _text(gate.get("gate_id")): gate
        for gate in _records(_mapping(snapshot.get("positions")).get("position_gates"))
        if _text(gate.get("gate_id"))
    }


def _binding_gate_id(snapshot: Mapping[str, Any]) -> str:
    final_max = _float_or_none(
        _mapping(_mapping(snapshot.get("positions")).get("final_risk_asset_ai_band")).get(
            "max_position"
        )
    )
    if final_max is None:
        return ""
    for gate in _records(_mapping(snapshot.get("positions")).get("position_gates")):
        cap = _float_or_none(gate.get("max_position"))
        if cap is not None and abs(cap - final_max) < 1e-9:
            return _text(gate.get("gate_id"))
    return ""


def _gate_change_flags(
    *,
    current_gate: Mapping[str, Any] | None,
    previous_gate: Mapping[str, Any] | None,
    current_binding: str,
    previous_binding: str,
    gate_id: str,
    current_cap: float | None,
    previous_cap: float | None,
) -> list[str]:
    flags: list[str] = []
    if current_gate is None:
        flags.append("REMOVED")
    if previous_gate is None:
        flags.append("ADDED")
    if _delta(current_cap, previous_cap) not in {None, 0}:
        flags.append("CAP_CHANGED")
    if _mapping(current_gate).get("triggered") != _mapping(previous_gate).get("triggered"):
        flags.append("TRIGGER_CHANGED")
    if (gate_id == current_binding) != (gate_id == previous_binding):
        flags.append("BINDING_CHANGED")
    return flags


def _binding_change_text(
    gate_id: str,
    *,
    current_binding: str,
    previous_binding: str,
) -> str:
    if gate_id == current_binding and gate_id != previous_binding:
        return "became_binding"
    if gate_id == previous_binding and gate_id != current_binding:
        return "no_longer_binding"
    return "none"


def _source_inputs(
    *,
    decision_snapshot_path: Path,
    previous_decision_snapshot_path: Path | None,
    snapshot_dir: Path,
) -> dict[str, Any]:
    return {
        "decision_snapshot": {
            "path": str(decision_snapshot_path),
            "exists": decision_snapshot_path.exists(),
            "role": "current_signal_snapshot",
        },
        "previous_decision_snapshot": {
            "path": (
                ""
                if previous_decision_snapshot_path is None
                else str(previous_decision_snapshot_path)
            ),
            "exists": (
                False
                if previous_decision_snapshot_path is None
                else previous_decision_snapshot_path.exists()
            ),
            "role": "prior_signal_snapshot",
        },
        "snapshot_dir": {
            "path": str(snapshot_dir),
            "exists": snapshot_dir.exists(),
            "role": "previous_snapshot_discovery",
        },
    }


def _find_previous_snapshot_path(snapshot_dir: Path, as_of: date) -> Path | None:
    if not snapshot_dir.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_*.json"):
        signal_date = _date_from_snapshot_filename(path)
        if signal_date is not None and signal_date < as_of:
            candidates.append((signal_date, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _date_from_snapshot_filename(path: Path) -> date | None:
    stem = path.stem
    prefix = "decision_snapshot_"
    if not stem.startswith(prefix):
        return None
    try:
        return date.fromisoformat(stem.removeprefix(prefix))
    except ValueError:
        return None


def _snapshot_signal_date(snapshot: Mapping[str, Any]) -> date | None:
    raw = _text(snapshot.get("signal_date"))
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _market_regime_record(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    regime = _mapping(snapshot.get("market_regime"))
    return {
        "regime_id": _text(regime.get("regime_id"), "ai_after_chatgpt"),
        "anchor_date": _text(regime.get("anchor_date"), "2022-11-30"),
        "start_date": _text(regime.get("start_date"), "2022-12-01"),
    }


def _component_status(
    *,
    current: object,
    previous: object,
    numeric_deltas: tuple[float | None, ...],
    source_type_changed: bool,
) -> str:
    if current is None:
        return "REMOVED"
    if previous is None:
        return "ADDED"
    if source_type_changed:
        return "CHANGED"
    if any(delta is not None and delta != 0 for delta in numeric_deltas):
        return "CHANGED"
    return "UNCHANGED"


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return raw


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _delta(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return current - previous


def _multiply(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left * right


def _format_optional_number(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "UNKNOWN"
    return f"{number:+.4f}"


def _format_percent(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "UNKNOWN"
    return f"{number:.0%}"


def _delta_line(row: Mapping[str, Any], current_key: str, previous_key: str, delta_key: str) -> str:
    return (
        f"{_format_optional_number(row.get(previous_key))} -> "
        f"{_format_optional_number(row.get(current_key))} "
        f"({_format_optional_number(row.get(delta_key))})"
    )


def _driver_list(top_changes: Mapping[str, Any], key: str) -> str:
    records = _records(top_changes.get(key))
    if not records:
        return "无"
    parts = []
    for record in records:
        component = _text(record.get("component"), _text(record.get("gate_id"), "UNKNOWN"))
        metrics = [
            f"{field}={_format_optional_number(value)}"
            for field, value in record.items()
            if field not in {"component", "gate_id", "change_flags"}
            and _float_or_none(value) is not None
        ]
        if record.get("change_flags"):
            metrics.append("flags=" + ",".join(str(item) for item in record["change_flags"]))
        parts.append(f"{component} ({'; '.join(metrics)})")
    return "；".join(parts)


def _driver_list_from_records(value: object) -> str:
    records = _records(value)
    if not records:
        return "无"
    parts = []
    for record in records[:5]:
        component = _text(record.get("component"), _text(record.get("gate_id"), "UNKNOWN"))
        metric_parts = [
            f"{key}={_format_optional_number(metric_value)}"
            for key, metric_value in record.items()
            if key not in {"component", "gate_id", "status", "change_flags"}
            and _float_or_none(metric_value) is not None
        ]
        parts.append(component if not metric_parts else f"{component} ({'; '.join(metric_parts)})")
    return "；".join(parts)


def _score_vs_gate_sentence(payload: Mapping[str, Any]) -> str:
    overall = _mapping(payload.get("overall_score_delta"))
    score_delta = _float_or_none(overall.get("delta"))
    gate_changed = payload.get("binding_gate_changed") is True or bool(
        _records(payload.get("gate_state_changes"))
    )
    if score_delta not in {None, 0} and gate_changed:
        return (
            "本次变化同时包含 score 变化和 gate 状态变化，最终解释需同时查看 "
            "component 与 gate attribution。"
        )
    if gate_changed:
        return "本次最终状态主要需要关注 gate 变化；score 变化不是唯一解释。"
    if score_delta not in {None, 0}:
        return "本次变化主要来自 score/component 变化，binding gate 未发生实质切换。"
    return "overall score 和 binding gate 均未显示实质变化。"


def _final_position_sentence(payload: Mapping[str, Any]) -> str:
    current = _mapping(payload.get("final_position_current"))
    previous = _mapping(payload.get("final_position_previous"))
    delta = _mapping(payload.get("final_position_delta"))
    max_delta = _float_or_none(delta.get("max_delta"))
    if max_delta in {None, 0}:
        return (
            "最终仓位上限未发生变化："
            f"{_format_percent(previous.get('max'))} -> {_format_percent(current.get('max'))}。"
        )
    return (
        "最终仓位上限发生变化："
        f"{_format_percent(previous.get('max'))} -> {_format_percent(current.get('max'))} "
        f"({_format_optional_number(max_delta)})。"
    )


def _quality_manual_review_sentence(payload: Mapping[str, Any]) -> str:
    data_quality = _mapping(payload.get("data_quality_status_delta"))
    changed = any(
        data_quality.get(key) is True
        for key in (
            "market_data_status_changed",
            "feature_status_changed",
            "sec_feature_status_changed",
        )
    )
    manual_delta = _float_or_none(payload.get("manual_review_count_delta"))
    current_manual = _float_or_none(payload.get("manual_review_count_current"))
    if changed and (manual_delta not in {None, 0} or (current_manual or 0) > 0):
        return (
            "data quality 状态与 manual review 数量均有变化，"
            "Reader Brief 使用结论时应保留限制说明。"
        )
    if changed:
        return "data quality 状态发生变化，需确认是否限制今日结论使用。"
    if manual_delta not in {None, 0} or (current_manual or 0) > 0:
        return "manual review 项存在或数量变化，需人工确认是否限制今日结论使用。"
    return "未观察到 data quality 或 manual review 导致的新增使用限制。"
