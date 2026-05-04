from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.backtest.daily import BacktestRegimeContext
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.scoring.daily import DailyScoreReport
from ai_trading_system.scoring.position_model import PositionBand

SCHEMA_VERSION = 1
DEFAULT_DECISION_SNAPSHOT_DIR = PROJECT_ROOT / "data" / "processed" / "decision_snapshots"


def default_decision_snapshot_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"decision_snapshot_{as_of.isoformat()}.json"


def build_decision_snapshot(
    *,
    report: DailyScoreReport,
    trace_bundle_path: Path,
    market_regime: BacktestRegimeContext | None,
    config_paths: dict[str, Path],
    belief_state_path: Path | None = None,
    rule_version_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    confidence = report.confidence_assessment
    recommendation = report.recommendation
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": f"decision_snapshot:{report.as_of.isoformat()}",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "signal_date": report.as_of.isoformat(),
        "market_regime": _market_regime_record(market_regime),
        "scores": {
            "overall_score": recommendation.total_score,
            "confidence_score": confidence.score,
            "confidence_level": confidence.level,
            "confidence_reasons": list(confidence.reasons),
            "components": [
                {
                    "component": component.name,
                    "score": component.score,
                    "weight": component.weight,
                    "source_type": component.source_type,
                    "coverage": component.coverage,
                    "confidence": component.confidence,
                    "reason": component.reason,
                }
                for component in report.components
            ],
        },
        "positions": {
            "model_risk_asset_ai_band": _band_record(
                recommendation.model_risk_asset_ai_band
            ),
            "final_risk_asset_ai_band": _band_record(recommendation.risk_asset_ai_band),
            "confidence_adjusted_risk_asset_ai_band": _band_record(
                confidence.adjusted_risk_asset_ai_band
            ),
            "total_asset_ai_band": _band_record(recommendation.total_asset_ai_band),
            "minimum_action_delta": report.minimum_action_delta,
            "position_gates": [
                {
                    "gate_id": gate.gate_id,
                    "label": gate.label,
                    "source": gate.source,
                    "max_position": gate.max_position,
                    "triggered": gate.triggered,
                    "reason": gate.reason,
                }
                for gate in recommendation.position_gates
            ],
        },
        "quality": {
            "market_data_status": report.data_quality_report.status,
            "market_data_error_count": report.data_quality_report.error_count,
            "market_data_warning_count": report.data_quality_report.warning_count,
            "feature_status": report.feature_set.status,
            "feature_warning_count": len(report.feature_set.warnings),
            "sec_feature_status": (
                report.fundamental_feature_report.status
                if report.fundamental_feature_report is not None
                else None
            ),
        },
        "manual_review": _manual_review_record(report),
        "valuation_state": _valuation_state_record(report),
        "risk_event_state": _risk_event_state_record(report),
        "belief_state_ref": (
            None
            if belief_state_path is None
            else {
                "path": str(belief_state_path),
                "read_only": True,
                "production_effect": "none",
            }
        ),
        "trace": {
            "trace_bundle_path": str(trace_bundle_path),
            "overall_claim_id": f"daily_score:{report.as_of.isoformat()}:overall_position",
        },
        "rule_versions": rule_version_manifest,
        "config_paths": {key: str(path) for key, path in sorted(config_paths.items())},
    }


def write_decision_snapshot(snapshot: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def _band_record(band: PositionBand) -> dict[str, Any]:
    return {
        "min_position": band.min_position,
        "max_position": band.max_position,
        "label": band.label,
    }


def _manual_review_record(report: DailyScoreReport) -> list[dict[str, Any]]:
    if report.review_summary is None:
        return []
    return [
        {
            "name": item.name,
            "status": item.status,
            "summary": item.summary,
            "error_count": item.error_count,
            "warning_count": item.warning_count,
            "source_path": str(item.source_path) if item.source_path else None,
        }
        for item in report.review_summary.items
    ]


def _valuation_state_record(report: DailyScoreReport) -> dict[str, Any] | None:
    if report.valuation_review_report is None:
        return None
    review = report.valuation_review_report
    return {
        "status": review.status,
        "snapshot_count": review.validation_report.snapshot_count,
        "items": [
            {
                "snapshot_id": item.snapshot_id,
                "ticker": item.ticker,
                "health": item.health,
                "valuation_percentile": item.valuation_percentile,
                "confidence_level": item.confidence_level,
                "point_in_time_class": item.point_in_time_class,
                "backtest_use": item.backtest_use,
            }
            for item in review.items
        ],
    }


def _risk_event_state_record(report: DailyScoreReport) -> dict[str, Any] | None:
    if report.risk_event_occurrence_review_report is None:
        return None
    review = report.risk_event_occurrence_review_report
    return {
        "status": review.status,
        "occurrence_count": review.validation_report.occurrence_count,
        "score_eligible_active_count": len(review.score_eligible_active_items),
        "position_gate_eligible_active_count": len(
            review.position_gate_eligible_active_items
        ),
        "items": [
            {
                "occurrence_id": item.occurrence_id,
                "event_id": item.event_id,
                "status": item.status,
                "level": item.level,
                "evidence_grade": item.evidence_grade,
                "action_class": item.action_class,
                "score_eligible": item.score_eligible,
                "position_gate_eligible": item.position_gate_eligible,
                "reason": item.reason,
            }
            for item in review.items
        ],
    }


def _market_regime_record(market_regime: BacktestRegimeContext | None) -> dict[str, Any]:
    if market_regime is None:
        return {
            "regime_id": "ai_after_chatgpt",
            "anchor_date": "2022-11-30",
            "anchor_event": "ChatGPT public launch",
            "start_date": "2022-12-01",
        }
    return {
        "regime_id": market_regime.regime_id,
        "name": market_regime.name,
        "anchor_date": market_regime.anchor_date.isoformat(),
        "anchor_event": market_regime.anchor_event,
        "start_date": market_regime.start_date.isoformat(),
        "description": market_regime.description,
    }
