from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "calculation_explainers"
PRODUCTION_EFFECT = "none"
DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH = PROJECT_ROOT / "config" / "metric_explainers.yaml"

REQUIRED_METRIC_IDS: tuple[str, ...] = (
    "overall_score",
    "component_score",
    "effective_weight",
    "confidence_score",
    "model_position_band",
    "confidence_adjusted_position",
    "macro_risk_asset_budget",
    "position_gate",
    "final_position_max",
    "final_position_band",
    "rank_ic",
    "max_drawdown",
    "baseline_coverage",
)


def default_calculation_explainers_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"calculation_explainers_{as_of.isoformat()}.json"


def build_calculation_explainers_payload(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    registry_path: Path = DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    scores_daily_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_metric_explainer_registry(registry_path)
    snapshot = _read_json_object(decision_snapshot_path)
    warnings: list[str] = []
    if scores_daily_path is not None and not scores_daily_path.exists():
        warnings.append(f"scores_daily_missing:{scores_daily_path}")

    snapshot_signal_date = _string(snapshot.get("signal_date"))
    if snapshot_signal_date and snapshot_signal_date != as_of.isoformat():
        warnings.append(
            "decision_snapshot_signal_date_mismatch:"
            f"expected={as_of.isoformat()};actual={snapshot_signal_date}"
        )

    source_artifacts = {
        "decision_snapshot": _artifact_record(
            "decision_snapshot",
            decision_snapshot_path,
            exists=True,
            role="primary_values",
        ),
        "metric_explainer_registry": _artifact_record(
            "metric_explainer_registry",
            registry_path,
            exists=True,
            role="formula_registry",
        ),
    }
    if scores_daily_path is not None:
        source_artifacts["scores_daily"] = _artifact_record(
            "scores_daily",
            scores_daily_path,
            exists=scores_daily_path.exists(),
            role="score_cache",
        )

    metrics = _build_metrics(
        registry=registry,
        snapshot=snapshot,
        source_artifacts=source_artifacts,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": "PASS_WITH_WARNINGS" if warnings else "PASS",
        "production_effect": PRODUCTION_EFFECT,
        "registry": {
            "path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "policy_version": registry.get("policy_version"),
            "required_metric_ids": list(REQUIRED_METRIC_IDS),
        },
        "source_inputs": source_artifacts,
        "warnings": warnings,
        "metrics": metrics,
    }


def load_metric_explainer_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"metric explainer registry not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("metric explainer registry must be a mapping")
    metrics = raw.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("metric explainer registry must contain a metrics mapping")
    missing = [metric_id for metric_id in REQUIRED_METRIC_IDS if metric_id not in metrics]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"metric explainer registry missing required metrics: {missing_text}")
    for metric_id in REQUIRED_METRIC_IDS:
        metric = metrics.get(metric_id)
        if not isinstance(metric, dict):
            raise ValueError(f"metric explainer {metric_id} must be a mapping")
        for field in ("label", "audience_label", "formula", "input_fields", "pit_policy"):
            if field not in metric:
                raise ValueError(f"metric explainer {metric_id} missing required field: {field}")
        if not isinstance(metric.get("input_fields"), list):
            raise ValueError(f"metric explainer {metric_id} input_fields must be a list")
    return raw


def write_calculation_explainers_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def _build_metrics(
    *,
    registry: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    source_artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    scores = _mapping(snapshot.get("scores"))
    positions = _mapping(snapshot.get("positions"))
    components = _records(scores.get("components"))
    gates = _records(positions.get("position_gates"))
    final_band = _mapping(positions.get("final_risk_asset_ai_band"))
    model_band = _mapping(positions.get("model_risk_asset_ai_band"))
    confidence_band = _mapping(positions.get("confidence_adjusted_risk_asset_ai_band"))
    macro_budget = _mapping(positions.get("macro_risk_asset_budget"))
    component_inputs = _component_inputs(components)
    binding_gate = _binding_gate(gates, _float_or_none(final_band.get("max_position")))

    return {
        "overall_score": _metric_record(
            registry,
            "overall_score",
            value=_float_or_none(scores.get("overall_score")),
            input_values=component_inputs,
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "scores_daily",
                "metric_explainer_registry",
            ),
        ),
        "component_score": _metric_record(
            registry,
            "component_score",
            value={"component_count": len(component_inputs)},
            input_values=[
                {
                    "component": item.get("component"),
                    "score": item.get("score"),
                    "source_type": item.get("source_type"),
                    "coverage": item.get("coverage"),
                    "confidence": item.get("confidence"),
                    "reason": item.get("reason"),
                }
                for item in component_inputs
            ],
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "scores_daily",
                "metric_explainer_registry",
            ),
        ),
        "effective_weight": _metric_record(
            registry,
            "effective_weight",
            value={"component_count": len(component_inputs)},
            input_values=[
                {
                    "component": item.get("component"),
                    "raw_weight": item.get("raw_weight"),
                    "effective_weight": item.get("effective_weight"),
                }
                for item in component_inputs
            ],
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "confidence_score": _metric_record(
            registry,
            "confidence_score",
            value=_float_or_none(scores.get("confidence_score")),
            input_values={
                "confidence_level": _string(scores.get("confidence_level")),
                "confidence_reasons": _strings(scores.get("confidence_reasons")),
            },
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "model_position_band": _metric_record(
            registry,
            "model_position_band",
            value=_band_value(model_band),
            input_values={
                "overall_score": _float_or_none(scores.get("overall_score")),
                "model_risk_asset_ai_band": _band_value(model_band),
            },
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "confidence_adjusted_position": _metric_record(
            registry,
            "confidence_adjusted_position",
            value=_band_value(confidence_band),
            input_values={
                "confidence_score": _float_or_none(scores.get("confidence_score")),
                "confidence_level": _string(scores.get("confidence_level")),
                "confidence_adjusted_risk_asset_ai_band": _band_value(confidence_band),
            },
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "macro_risk_asset_budget": _metric_record(
            registry,
            "macro_risk_asset_budget",
            value={
                "level": _string(macro_budget.get("level")),
                "triggered": macro_budget.get("triggered"),
                "source": _string(macro_budget.get("source")),
                "reasons": _strings(macro_budget.get("reasons")),
            },
            input_values=macro_budget,
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "position_gate": _metric_record(
            registry,
            "position_gate",
            value={
                "gate_count": len(gates),
                "triggered_gate_count": sum(1 for gate in gates if bool(gate.get("triggered"))),
                "binding_gate": binding_gate,
            },
            input_values=gates,
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "final_position_max": _metric_record(
            registry,
            "final_position_max",
            value=_float_or_none(final_band.get("max_position")),
            input_values={
                "model_position_max": _float_or_none(model_band.get("max_position")),
                "confidence_adjusted_position_max": _float_or_none(
                    confidence_band.get("max_position")
                ),
                "gate_caps": [
                    {
                        "gate_id": _string(gate.get("gate_id")),
                        "max_position": _float_or_none(gate.get("max_position")),
                        "triggered": bool(gate.get("triggered")),
                    }
                    for gate in gates
                ],
                "binding_gate": binding_gate,
            },
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "final_position_band": _metric_record(
            registry,
            "final_position_band",
            value=_band_value(final_band),
            input_values={"final_risk_asset_ai_band": _band_value(final_band)},
            source_artifacts=_source_refs(
                source_artifacts,
                "decision_snapshot",
                "metric_explainer_registry",
            ),
        ),
        "rank_ic": _definition_only_record(
            registry,
            "rank_ic",
            source_artifacts,
            "not_present_in_decision_snapshot",
        ),
        "max_drawdown": _definition_only_record(
            registry,
            "max_drawdown",
            source_artifacts,
            "not_present_in_decision_snapshot",
        ),
        "baseline_coverage": _definition_only_record(
            registry,
            "baseline_coverage",
            source_artifacts,
            "not_present_in_decision_snapshot",
        ),
    }


def _metric_record(
    registry: Mapping[str, Any],
    metric_id: str,
    *,
    value: object,
    input_values: object,
    source_artifacts: list[dict[str, Any]],
    limitations: list[str] | None = None,
    status: str = "AVAILABLE",
) -> dict[str, Any]:
    definition = _metric_definition(registry, metric_id)
    return {
        "metric_id": metric_id,
        "status": status,
        "label": _string(definition.get("label")),
        "audience_label": _string(definition.get("audience_label")),
        "value": value,
        "formula": _string(definition.get("formula")),
        "input_fields": _strings(definition.get("input_fields")),
        "input_values": input_values,
        "output_range": _string(definition.get("output_range")),
        "interpretation": definition.get("interpretation") or {},
        "common_misread": _strings(definition.get("common_misread")),
        "report_sections": _strings(definition.get("report_sections")),
        "pit_policy": _string(definition.get("pit_policy")),
        "production_effect": PRODUCTION_EFFECT,
        "source_artifacts": source_artifacts,
        "limitations": limitations or _strings(definition.get("limitations")),
    }


def _definition_only_record(
    registry: Mapping[str, Any],
    metric_id: str,
    source_artifacts: Mapping[str, Any],
    limitation: str,
) -> dict[str, Any]:
    return _metric_record(
        registry,
        metric_id,
        value=None,
        input_values=[],
        source_artifacts=_source_refs(source_artifacts, "metric_explainer_registry"),
        limitations=[limitation],
        status="DEFINITION_ONLY",
    )


def _metric_definition(registry: Mapping[str, Any], metric_id: str) -> Mapping[str, Any]:
    metrics = registry.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("metric explainer registry metrics must be a mapping")
    metric = metrics.get(metric_id)
    if not isinstance(metric, Mapping):
        raise ValueError(f"metric explainer not found: {metric_id}")
    return metric


def _component_inputs(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weights = [_float_or_none(component.get("weight")) for component in components]
    total_weight = sum(weight for weight in weights if weight is not None)
    rows: list[dict[str, Any]] = []
    for component in components:
        raw_weight = _float_or_none(component.get("weight"))
        score = _float_or_none(component.get("score"))
        effective_weight = (
            raw_weight / total_weight if raw_weight is not None and total_weight > 0 else None
        )
        contribution = (
            score * effective_weight if score is not None and effective_weight is not None else None
        )
        rows.append(
            {
                "component": _string(component.get("component")),
                "score": score,
                "raw_weight": raw_weight,
                "effective_weight": effective_weight,
                "contribution_to_overall_score": contribution,
                "source_type": _string(component.get("source_type")),
                "coverage": _float_or_none(component.get("coverage")),
                "confidence": _float_or_none(component.get("confidence")),
                "reason": _string(component.get("reason")),
            }
        )
    return rows


def _binding_gate(
    gates: list[dict[str, Any]], final_max_position: float | None
) -> dict[str, Any] | None:
    if final_max_position is None:
        return None
    candidates = [
        gate
        for gate in gates
        if bool(gate.get("triggered"))
        and _float_or_none(gate.get("max_position")) is not None
        and abs((_float_or_none(gate.get("max_position")) or 0.0) - final_max_position) < 1e-9
    ]
    if not candidates:
        candidates = [
            gate
            for gate in gates
            if _float_or_none(gate.get("max_position")) is not None
            and abs((_float_or_none(gate.get("max_position")) or 0.0) - final_max_position) < 1e-9
        ]
    if not candidates:
        return None
    gate = candidates[0]
    return {
        "gate_id": _string(gate.get("gate_id")),
        "label": _string(gate.get("label")),
        "source": _string(gate.get("source")),
        "max_position": _float_or_none(gate.get("max_position")),
        "triggered": bool(gate.get("triggered")),
        "reason": _string(gate.get("reason")),
    }


def _source_refs(source_artifacts: Mapping[str, Any], *ids: str) -> list[dict[str, Any]]:
    return [dict(source_artifacts[source_id]) for source_id in ids if source_id in source_artifacts]


def _artifact_record(
    artifact_id: str,
    path: Path,
    *,
    exists: bool,
    role: str,
) -> dict[str, Any]:
    return {
        "id": artifact_id,
        "path": str(path),
        "exists": exists,
        "role": role,
    }


def _band_value(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    if not raw:
        return None
    return {
        "min_position": _float_or_none(raw.get("min_position")),
        "max_position": _float_or_none(raw.get("max_position")),
        "label": _string(raw.get("label")),
    }


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"decision snapshot not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON object expected: {path}")
    return raw


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _string(value: object) -> str:
    return "" if value is None else str(value)


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
