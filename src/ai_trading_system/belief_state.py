from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.backtest.daily import BacktestRegimeContext
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.scoring.daily import (
    COMPONENT_LABELS,
    SOURCE_TYPE_LABELS,
    DailyScoreComponent,
    DailyScoreReport,
)
from ai_trading_system.scoring.position_model import PositionBand

SCHEMA_VERSION = 1
DEFAULT_BELIEF_STATE_DIR = PROJECT_ROOT / "data" / "processed" / "belief_state"
DEFAULT_BELIEF_STATE_HISTORY_PATH = (
    PROJECT_ROOT / "data" / "processed" / "belief_state_history.csv"
)


def default_belief_state_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"belief_state_{as_of.isoformat()}.json"


def build_belief_state(
    *,
    report: DailyScoreReport,
    trace_bundle_path: Path,
    decision_snapshot_path: Path | None,
    market_regime: BacktestRegimeContext | None,
    config_paths: dict[str, Path],
) -> dict[str, Any]:
    confidence = report.confidence_assessment
    return {
        "schema_version": SCHEMA_VERSION,
        "belief_state_id": f"belief_state:{report.as_of.isoformat()}",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "signal_date": report.as_of.isoformat(),
        "read_only": True,
        "production_effect": "none",
        "production_effect_note": (
            "belief_state 是只读解释层；不改变正式评分、position_gate、"
            "回测仓位或交易建议。"
        ),
        "market_regime": _market_regime_record(market_regime),
        "data_quality": {
            "status": report.data_quality_report.status,
            "error_count": report.data_quality_report.error_count,
            "warning_count": report.data_quality_report.warning_count,
            "confidence": _data_quality_confidence(report),
            "reference": "data_quality.current",
        },
        "market_state": _market_state_record(report),
        "industry_chain_state": _industry_chain_state_record(report),
        "valuation_state": _valuation_state_record(report),
        "risk_state": _risk_state_record(report),
        "thesis_state": _thesis_state_record(report),
        "position_boundary": _position_boundary_record(report),
        "confidence": {
            "overall_score": confidence.score,
            "overall_level": confidence.level,
            "data_quality_confidence": _data_quality_confidence(report),
            "evidence_strength": _evidence_strength(report),
            "regime_fit_confidence": (
                "medium" if market_regime is not None else "not_assessed"
            ),
            "model_calibration_confidence": "not_assessed",
            "human_review_status": _human_review_status(report),
            "reasons": list(confidence.reasons),
        },
        "limitations": _limitation_records(report),
        "references": {
            "trace_bundle_path": str(trace_bundle_path),
            "decision_snapshot_path": (
                None if decision_snapshot_path is None else str(decision_snapshot_path)
            ),
            "overall_claim_id": f"daily_score:{report.as_of.isoformat()}:overall_position",
            "config_paths": {key: str(path) for key, path in sorted(config_paths.items())},
        },
    }


def write_belief_state(belief_state: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(belief_state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def append_belief_state_history(
    belief_state: dict[str, Any],
    belief_state_path: Path,
    output_path: Path = DEFAULT_BELIEF_STATE_HISTORY_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    position_boundary = belief_state["position_boundary"]
    final_band = position_boundary["final_risk_asset_ai_band"]
    references = belief_state["references"]
    record = {
        "signal_date": belief_state["signal_date"],
        "belief_state_id": belief_state["belief_state_id"],
        "belief_state_path": str(belief_state_path),
        "generated_at": belief_state["generated_at"],
        "production_effect": belief_state["production_effect"],
        "confidence_score": belief_state["confidence"]["overall_score"],
        "confidence_level": belief_state["confidence"]["overall_level"],
        "data_quality_status": belief_state["data_quality"]["status"],
        "final_risk_asset_ai_min": final_band["min_position"],
        "final_risk_asset_ai_max": final_band["max_position"],
        "limitation_count": len(belief_state["limitations"]),
        "trace_bundle_path": references["trace_bundle_path"],
        "decision_snapshot_path": references["decision_snapshot_path"],
    }
    new_frame = pd.DataFrame([record])
    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "signal_date" not in existing.columns:
            raise ValueError(
                f"existing belief_state_history file is missing signal_date column: {output_path}"
            )
        existing = existing.loc[existing["signal_date"] != belief_state["signal_date"]]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame = new_frame.sort_values("signal_date").reset_index(drop=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_belief_state_summary(
    belief_state: dict[str, Any],
    belief_state_path: Path,
) -> str:
    market_state = belief_state["market_state"]
    position_boundary = belief_state["position_boundary"]
    confidence = belief_state["confidence"]
    lines = [
        "## 认知状态",
        "",
        f"- 机器可读状态：`{belief_state_path}`",
        "- 使用边界：只读解释层，不改变正式评分、position_gate、回测仓位或交易建议。",
        f"- 市场状态：{_market_state_summary(market_state)}",
        f"- 产业链节点状态：{_industry_chain_summary(belief_state['industry_chain_state'])}",
        f"- 估值状态：{belief_state['valuation_state']['summary']}",
        f"- 风险状态：{belief_state['risk_state']['summary']}",
        f"- Thesis 状态：{belief_state['thesis_state']['summary']}",
        (
            "- 仓位边界："
            f"模型 {_format_band_record(position_boundary['model_risk_asset_ai_band'])}；"
            f"最终 {_format_band_record(position_boundary['final_risk_asset_ai_band'])}；"
            "置信度调整后 "
            f"{_format_band_record(position_boundary['confidence_adjusted_band'])}。"
        ),
        (
            "- 多维置信度："
            f"整体 {confidence['overall_score']:.1f}/{confidence['overall_level']}；"
            f"数据质量 {confidence['data_quality_confidence']}；"
            f"证据强度 {confidence['evidence_strength']}；"
            f"regime fit {confidence['regime_fit_confidence']}；"
            f"校准 {confidence['model_calibration_confidence']}；"
            f"人工复核 {confidence['human_review_status']}。"
        ),
        f"- 主要限制：{_limitation_summary(belief_state['limitations'])}",
    ]
    return "\n".join(lines) + "\n"


def _market_state_record(report: DailyScoreReport) -> dict[str, Any]:
    return {
        name: _component_record(component)
        for name in ("trend", "macro_liquidity", "risk_sentiment")
        if (component := _component_by_name(report, name)) is not None
    }


def _industry_chain_state_record(report: DailyScoreReport) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    for row in report.feature_set.rows:
        if row.subject.startswith("^") or row.subject in {"DXY", "USD", "US10Y", "FEDFUNDS"}:
            continue
        node = nodes.setdefault(
            row.subject,
            {
                "subject": row.subject,
                "feature_count": 0,
                "categories": set(),
                "latest_source_date": row.source_date.isoformat(),
                "sources": set(),
            },
        )
        node["feature_count"] += 1
        node["categories"].add(row.category)
        node["sources"].add(row.source)
        if row.source_date.isoformat() > node["latest_source_date"]:
            node["latest_source_date"] = row.source_date.isoformat()

    normalized_nodes = [
        {
            "subject": value["subject"],
            "feature_count": value["feature_count"],
            "categories": sorted(value["categories"]),
            "latest_source_date": value["latest_source_date"],
            "sources": sorted(value["sources"]),
            "reference": f"market_features:{value['subject']}",
        }
        for value in sorted(nodes.values(), key=lambda item: item["subject"])
    ]
    return {
        "status": "available" if normalized_nodes else "data_limited",
        "node_count": len(normalized_nodes),
        "nodes": normalized_nodes,
        "summary": (
            f"已从市场特征聚合 {len(normalized_nodes)} 个产业链/观察池标的节点。"
            if normalized_nodes
            else "当前市场特征未能形成可复核的产业链节点状态。"
        ),
    }


def _valuation_state_record(report: DailyScoreReport) -> dict[str, Any]:
    review = report.valuation_review_report
    if review is None:
        return {
            "status": "not_connected",
            "summary": "未接入估值快照，估值状态不可用。",
            "items": [],
        }
    crowded = [
        item
        for item in review.items
        if item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
    ]
    return {
        "status": review.status,
        "summary": (
            f"估值快照 {len(review.items)} 个；昂贵/拥挤 {len(crowded)} 个；"
            f"校验状态 {review.validation_report.status}。"
        ),
        "items": [
            {
                "snapshot_id": item.snapshot_id,
                "ticker": item.ticker,
                "health": item.health,
                "valuation_percentile": item.valuation_percentile,
                "confidence_level": item.confidence_level,
                "confidence_reason": item.confidence_reason,
                "point_in_time_class": item.point_in_time_class,
                "backtest_use": item.backtest_use,
                "reference": f"valuation_snapshot:{item.snapshot_id}",
            }
            for item in review.items
        ],
    }


def _risk_state_record(report: DailyScoreReport) -> dict[str, Any]:
    review = report.risk_event_occurrence_review_report
    if review is None:
        return {
            "status": "not_connected",
            "summary": "未接入风险事件发生记录，不能把空记录当作无风险证明。",
            "items": [],
        }
    active_items = [item for item in review.items if item.status == "active"]
    watch_items = [item for item in review.items if item.status == "watch"]
    return {
        "status": review.status,
        "summary": (
            f"active {len(active_items)}，watch {len(watch_items)}，"
            f"可评分 active {len(review.score_eligible_active_items)}，"
            f"可触发仓位闸门 active {len(review.position_gate_eligible_active_items)}，"
            f"当前有效复核声明 "
            f"{review.validation_report.current_review_attestation_count}。"
        ),
        "items": [
            {
                "occurrence_id": item.occurrence_id,
                "event_id": item.event_id,
                "status": item.status,
                "level": item.level,
                "evidence_grade": item.evidence_grade,
                "severity": item.severity,
                "probability": item.probability,
                "scope": item.scope,
                "time_sensitivity": item.time_sensitivity,
                "reversibility": item.reversibility,
                "action_class": item.action_class,
                "score_eligible": item.score_eligible,
                "position_gate_eligible": item.position_gate_eligible,
                "reason": item.reason,
                "reference": f"risk_event_occurrence:{item.occurrence_id}",
            }
            for item in review.items
        ],
    }


def _thesis_state_record(report: DailyScoreReport) -> dict[str, Any]:
    thesis = report.review_summary.thesis if report.review_summary else None
    if thesis is None:
        return {
            "status": "not_connected",
            "summary": "未接入交易 thesis 复核摘要。",
            "source_path": None,
        }
    return {
        "status": thesis.status,
        "summary": thesis.summary,
        "error_count": thesis.error_count,
        "warning_count": thesis.warning_count,
        "source_path": None if thesis.source_path is None else str(thesis.source_path),
        "reference": "manual_review:thesis",
    }


def _position_boundary_record(report: DailyScoreReport) -> dict[str, Any]:
    recommendation = report.recommendation
    confidence = report.confidence_assessment
    return {
        "model_risk_asset_ai_band": _band_record(
            recommendation.model_risk_asset_ai_band
        ),
        "final_risk_asset_ai_band": _band_record(recommendation.risk_asset_ai_band),
        "confidence_adjusted_band": _band_record(
            confidence.adjusted_risk_asset_ai_band
        ),
        "total_asset_ai_band": _band_record(recommendation.total_asset_ai_band),
        "minimum_action_delta": report.minimum_action_delta,
        "limiting_gates": [
            {
                "gate_id": gate.gate_id,
                "label": gate.label,
                "max_position": gate.max_position,
                "triggered": gate.triggered,
                "reason": gate.reason,
                "reference": f"position_gate:{gate.gate_id}",
            }
            for gate in recommendation.position_gates
            if gate.triggered
        ],
    }


def _limitation_records(report: DailyScoreReport) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for component in report.components:
        if component.source_type in {"placeholder", "insufficient_data"}:
            records.append(
                {
                    "kind": "component_data_limit",
                    "subject": component.name,
                    "summary": component.reason,
                    "reference": f"score_component:{component.name}",
                }
            )
        elif component.confidence < 0.60:
            records.append(
                {
                    "kind": "component_low_confidence",
                    "subject": component.name,
                    "summary": f"模块置信度 {component.confidence:.0%}",
                    "reference": f"score_component:{component.name}",
                }
            )
    for gate in report.recommendation.triggered_position_gates:
        if gate.gate_id == "score_model":
            continue
        records.append(
            {
                "kind": "position_gate",
                "subject": gate.gate_id,
                "summary": gate.reason,
                "reference": f"position_gate:{gate.gate_id}",
            }
        )
    if report.feature_set.warnings:
        records.append(
            {
                "kind": "feature_warning",
                "subject": "market_features",
                "summary": f"市场特征存在 {len(report.feature_set.warnings)} 条警告",
                "reference": "feature_set:warnings",
            }
        )
    return records


def _component_record(component: DailyScoreComponent) -> dict[str, Any]:
    return {
        "component": component.name,
        "label": _component_label(component.name),
        "score": component.score,
        "source_type": component.source_type,
        "source_label": _source_type_label(component.source_type),
        "coverage": component.coverage,
        "confidence": component.confidence,
        "reason": component.reason,
        "reference": f"score_component:{component.name}",
    }


def _component_by_name(
    report: DailyScoreReport,
    component_name: str,
) -> DailyScoreComponent | None:
    return next(
        (component for component in report.components if component.name == component_name),
        None,
    )


def _band_record(band: PositionBand) -> dict[str, Any]:
    return {
        "min_position": band.min_position,
        "max_position": band.max_position,
        "label": band.label,
    }


def _market_regime_record(market_regime: BacktestRegimeContext | None) -> dict[str, Any]:
    if market_regime is None:
        return {
            "regime_id": None,
            "name": None,
            "start_date": None,
            "anchor_date": None,
            "anchor_event": None,
            "description": None,
        }
    return {
        "regime_id": market_regime.regime_id,
        "name": market_regime.name,
        "start_date": market_regime.start_date.isoformat(),
        "anchor_date": market_regime.anchor_date.isoformat(),
        "anchor_event": market_regime.anchor_event,
        "description": market_regime.description,
    }


def _data_quality_confidence(report: DailyScoreReport) -> str:
    if report.data_quality_report.status == "PASS" and not report.feature_set.warnings:
        return "high"
    if report.data_quality_report.passed:
        return "medium"
    return "low"


def _evidence_strength(report: DailyScoreReport) -> str:
    confidence = report.confidence_assessment.score
    if confidence >= 75:
        return "high"
    if confidence >= 60:
        return "medium"
    return "low"


def _human_review_status(report: DailyScoreReport) -> str:
    if report.review_summary is None or not report.review_summary.items:
        return "not_connected"
    if report.review_summary.has_failures:
        return "fail"
    if report.review_summary.has_warnings:
        return "warning"
    return "pass"


def _market_state_summary(market_state: dict[str, Any]) -> str:
    if not market_state:
        return "未形成市场状态。"
    return "；".join(
        (
            f"{record['label']} {record['score']:.1f}分/"
            f"{record['source_label']}/置信度{record['confidence']:.0%}"
        )
        for record in market_state.values()
    )


def _industry_chain_summary(industry_chain_state: dict[str, Any]) -> str:
    if industry_chain_state["status"] != "available":
        return industry_chain_state["summary"]
    nodes = industry_chain_state["nodes"][:5]
    node_text = "、".join(
        f"{node['subject']}({node['feature_count']})" for node in nodes
    )
    suffix = "" if industry_chain_state["node_count"] <= 5 else "等"
    return f"{industry_chain_state['summary']} 首批节点：{node_text}{suffix}。"


def _limitation_summary(limitations: list[dict[str, Any]]) -> str:
    if not limitations:
        return "未记录额外限制。"
    return "；".join(item["summary"] for item in limitations[:5])


def _format_band_record(record: dict[str, Any]) -> str:
    return f"{record['min_position']:.0%}-{record['max_position']:.0%}（{record['label']}）"


def _component_label(name: str) -> str:
    label = COMPONENT_LABELS.get(name)
    if label is None:
        return name
    return f"{label}（{name}）"


def _source_type_label(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type)
