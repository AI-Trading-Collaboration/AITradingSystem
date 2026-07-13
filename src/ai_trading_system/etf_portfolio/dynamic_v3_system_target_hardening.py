from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as history
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_source_bundle,
    _validate_operations_source_bundle,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)

SELECTION_ATTRIBUTION_SNAPSHOT_SCHEMA = "selection_attribution_input_snapshot.v2"
LIMITED_LONG_RISK_SNAPSHOT_SCHEMA = "limited_long_risk_input_snapshot.v2"
LIMITED_CONSISTENCY_SNAPSHOT_SCHEMA = "limited_consistency_input_snapshot.v2"
DATA_WARNING_IMPACT_SNAPSHOT_SCHEMA = "data_warning_impact_input_snapshot.v2"
RESEARCH_METHOD_HARDENING_SNAPSHOT_SCHEMA = "research_method_hardening_input_snapshot.v2"

DEFAULT_SELECTION_ATTRIBUTION_DIR = legacy.DEFAULT_SELECTION_ATTRIBUTION_DIR
DEFAULT_LIMITED_LONG_RISK_DIR = legacy.DEFAULT_LIMITED_LONG_RISK_DIR
DEFAULT_LIMITED_CONSISTENCY_DIR = legacy.DEFAULT_LIMITED_CONSISTENCY_DIR
DEFAULT_DATA_WARNING_IMPACT_DIR = legacy.DEFAULT_DATA_WARNING_IMPACT_DIR
DEFAULT_RESEARCH_METHOD_HARDENING_DIR = legacy.DEFAULT_RESEARCH_METHOD_HARDENING_DIR
DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR = history.DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR = history.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR
DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR = history.DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR
DEFAULT_PAPER_SHADOW_STABILITY_DIR = history.DEFAULT_PAPER_SHADOW_STABILITY_DIR
SYSTEM_TARGET_SAFETY = history.SYSTEM_TARGET_SAFETY


class DynamicV3SystemTargetHardeningError(ValueError):
    """Raised when method-hardening evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SystemTargetHardeningError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SystemTargetHardeningError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return target_core._finite(value)


def _optional(value: Any, *, digits: int = 10) -> float | None:
    return round(float(value), digits) if _finite(value) else None


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
    )


def _find(rows: Sequence[Mapping[str, Any]], method: str) -> dict[str, Any]:
    return next((dict(row) for row in rows if row.get("target_method") == method), {})


def _policy_from_backfill_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = _mapping(
        _mapping(_mapping(binding.get("bundle")).get("json")).get(
            "paper_shadow_backfill_input_snapshot.json"
        )
    )
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    history._validate_backfill_config_payload(config)
    policy = _mapping(config.get("method_hardening_policy"))
    for field in (
        "candidate_method",
        "comparison_baselines",
        "risk_exposure_symbols",
        "semiconductor_symbols",
        "pressure_regimes",
        "exposure_similarity_tolerance",
        "rationale",
    ):
        _require(field in policy, f"method_hardening_policy.{field} required")
    candidate = _text(policy.get("candidate_method"))
    baselines = [_text(item) for item in policy.get("comparison_baselines", [])]
    risk_symbols = [_text(item) for item in policy.get("risk_exposure_symbols", [])]
    semiconductor = [_text(item) for item in policy.get("semiconductor_symbols", [])]
    pressure = [_text(item) for item in policy.get("pressure_regimes", [])]
    tolerance = policy.get("exposure_similarity_tolerance")
    _require(bool(candidate), "method hardening candidate required")
    _require(bool(baselines) and len(baselines) == len(set(baselines)), "baselines invalid")
    _require(
        bool(risk_symbols) and len(risk_symbols) == len(set(risk_symbols)), "risk symbols invalid"
    )
    _require(
        bool(semiconductor) and len(semiconductor) == len(set(semiconductor)),
        "semiconductor symbols invalid",
    )
    _require(bool(pressure) and len(pressure) == len(set(pressure)), "pressure regimes invalid")
    _require(_finite(tolerance) and 0.0 <= float(tolerance) <= 1.0, "exposure tolerance invalid")
    return dict(policy)


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    artifact_root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    validation = validator(**{validator_key: artifact_id, "output_dir": artifact_root})
    _require(validation.get("status") == "PASS", f"{kind} validation failed")
    return {
        "kind": kind,
        "artifact_id": artifact_id,
        "validation": validation,
        "bundle": _operations_source_bundle(
            source_dir=artifact_root / artifact_id,
            json_views=json_views,
            jsonl_views=jsonl_views,
            text_views=text_views,
        ),
    }


def _selection_binding(selection_review_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="system_target_selection_review",
        artifact_id=selection_review_id,
        artifact_root=root,
        validator=history.validate_system_target_selection_review_artifact,
        validator_key="selection_review_id",
        json_views=(
            "system_target_selection_input_snapshot.json",
            "system_target_selection_manifest.json",
            "target_method_scorecard.json",
            "selection_decision.json",
        ),
        text_views=(
            "owner_research_checklist.md",
            "system_target_selection_review_report.md",
            "reader_brief_section.md",
        ),
    )


def _backfill_binding(backfill_id: str, root: Path) -> dict[str, Any]:
    return history._backfill_binding(backfill_id, root)


def _rolling_binding(rolling_eval_id: str, root: Path) -> dict[str, Any]:
    return history._selection_source("paper_shadow_rolling_eval", rolling_eval_id, root)


def _regime_binding(regime_review_id: str, root: Path) -> dict[str, Any]:
    return history._selection_source("paper_shadow_regime_review", regime_review_id, root)


def _stability_binding(stability_id: str, root: Path) -> dict[str, Any]:
    return history._selection_source("paper_shadow_stability", stability_id, root)


def _attribution_binding(attribution_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="selection_attribution",
        artifact_id=attribution_id,
        artifact_root=root,
        validator=validate_selection_attribution_artifact,
        validator_key="attribution_id",
        json_views=(
            "selection_attribution_input_snapshot.json",
            "selection_attribution_manifest.json",
            "recommendation_reason_breakdown.json",
            "review_required_reason_breakdown.json",
        ),
        jsonl_views=("method_score_attribution.jsonl",),
        text_views=("selection_attribution_report.md",),
    )


def _risk_binding(risk_review_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="limited_long_risk",
        artifact_id=risk_review_id,
        artifact_root=root,
        validator=validate_limited_long_risk_artifact,
        validator_key="risk_review_id",
        json_views=(
            "limited_long_risk_input_snapshot.json",
            "limited_long_risk_manifest.json",
            "long_window_risk_return.json",
            "limited_vs_baseline_breakdown.json",
            "exposure_path_analysis.json",
        ),
        text_views=("limited_long_risk_report.md",),
    )


def _consistency_binding(consistency_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="limited_consistency",
        artifact_id=consistency_id,
        artifact_root=root,
        validator=validate_limited_consistency_artifact,
        validator_key="consistency_id",
        json_views=(
            "limited_consistency_input_snapshot.json",
            "limited_consistency_manifest.json",
            "rolling_consistency_summary.json",
            "regime_consistency_summary.json",
            "stability_consistency_summary.json",
        ),
        text_views=("limited_consistency_report.md",),
    )


def _warning_binding(impact_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="data_warning_impact",
        artifact_id=impact_id,
        artifact_root=root,
        validator=validate_data_warning_impact_artifact,
        validator_key="impact_id",
        json_views=(
            "data_warning_impact_input_snapshot.json",
            "data_warning_impact_manifest.json",
            "data_warning_inventory.json",
            "affected_metrics.json",
            "recommendation_sensitivity_to_warnings.json",
        ),
        text_views=("data_warning_impact_report.md",),
    )


def _validator_for_kind(kind: str) -> tuple[Callable[..., dict[str, Any]], str]:
    table: dict[str, tuple[Callable[..., dict[str, Any]], str]] = {
        "system_target_selection_review": (
            history.validate_system_target_selection_review_artifact,
            "selection_review_id",
        ),
        "paper_shadow_backfill": (history.validate_paper_shadow_backfill_artifact, "backfill_id"),
        "paper_shadow_rolling_eval": (
            history.validate_paper_shadow_rolling_eval_artifact,
            "rolling_eval_id",
        ),
        "paper_shadow_regime_review": (
            history.validate_paper_shadow_regime_review_artifact,
            "regime_review_id",
        ),
        "paper_shadow_stability": (
            history.validate_paper_shadow_stability_artifact,
            "stability_id",
        ),
        "selection_attribution": (validate_selection_attribution_artifact, "attribution_id"),
        "limited_long_risk": (validate_limited_long_risk_artifact, "risk_review_id"),
        "limited_consistency": (validate_limited_consistency_artifact, "consistency_id"),
        "data_warning_impact": (validate_data_warning_impact_artifact, "impact_id"),
    }
    _require(kind in table, f"unknown hardening source kind: {kind}")
    return table[kind]


def _validate_source_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        kind = _text(binding.get("kind"))
        artifact_id = _text(binding.get("artifact_id"))
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        validator, key = _validator_for_kind(kind)
        actual = validator(**{key: artifact_id, "output_dir": source_dir.parent})
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(binding.get("bundle")).get("json")).get(name))


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return _records(_mapping(_mapping(binding.get("bundle")).get("jsonl")).get(name))


def _manifest_generated(binding: Mapping[str, Any], name: str) -> datetime:
    return target_core._datetime(
        _bundle_json(binding, name).get("generated_at"),
        field=f"{name} generated_at",
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest_name: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest_name)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _report_payload(root: Path, files: Mapping[str, str]) -> dict[str, Any]:
    return {
        key: _read_jsonl(root / name) if name.endswith(".jsonl") else _read_json(root / name)
        for key, name in files.items()
    }


def _attribution_rows(
    scorecard: Mapping[str, Any], decision: Mapping[str, Any], data_quality_status: str
) -> list[dict[str, Any]]:
    source = _records(scorecard.get("methods"))
    eligible = [row for row in source if _finite(row.get("overall_score"))]
    ordered = sorted(eligible, key=lambda row: float(row["overall_score"]), reverse=True)
    missing = [row for row in source if not _finite(row.get("overall_score"))]
    recommended = _text(decision.get("recommended_research_method"))
    secondary = set(_text(item) for item in decision.get("secondary_research_methods", []))
    reference = set(_text(item) for item in decision.get("reference_only_methods", []))
    component_fields = (
        "return_score",
        "drawdown_score",
        "risk_adjusted_score",
        "regime_score",
        "stability_score",
    )
    best: dict[str, str | None] = {}
    for field in component_fields:
        available = [row for row in source if _finite(row.get(field))]
        best[field] = (
            _text(max(available, key=lambda row: float(row[field])).get("target_method"))
            if available
            else None
        )
    rows: list[dict[str, Any]] = []
    for index, row in enumerate([*ordered, *missing], start=1):
        method = _text(row.get("target_method"))
        rank = index if _finite(row.get("overall_score")) else None
        if method == recommended:
            selection_status = "recommended_research_method"
        elif method in secondary:
            selection_status = "secondary_research_method"
        elif method in reference or row.get("status") == "REFERENCE_ONLY":
            selection_status = "reference_only"
        elif row.get("status") == "NOT_RECOMMENDED":
            selection_status = "not_recommended"
        else:
            selection_status = "insufficient_data" if rank is None else "observed_method"
        components = {field: _optional(row.get(field), digits=6) for field in component_fields}
        components["turnover_penalty"] = _optional(row.get("turnover_penalty"), digits=6)
        components["data_quality_penalty"] = None
        reasons = [f"best_{field}" for field, owner in best.items() if owner == method]
        if method == recommended:
            reasons.insert(0, "selected_by_reviewed_selection_policy")
        if selection_status == "reference_only":
            reasons.append("reference_only_policy")
        weaknesses = [f"missing_{field}" for field, value in components.items() if value is None]
        review_reasons: list[str] = []
        if data_quality_status == "PASS_WITH_WARNINGS":
            review_reasons.append("data_quality_warning_impact_unquantified")
        if rank is None:
            review_reasons.append("overall_score_missing")
        rows.append(
            {
                "target_method": method,
                "overall_score": _optional(row.get("overall_score"), digits=6),
                "score_components": components,
                "data_quality_penalty_scored": False,
                "rank": rank,
                "selection_status": selection_status,
                "selection_reasons": reasons,
                "weaknesses": weaknesses,
                "review_required_reasons": review_reasons,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _recommendation_breakdown(
    rows: Sequence[Mapping[str, Any]], decision: Mapping[str, Any]
) -> dict[str, Any]:
    recommended = _text(decision.get("recommended_research_method"))
    row = _find(rows, recommended)
    top = next((item for item in rows if isinstance(item.get("rank"), int)), {})

    def why_not(method: str) -> list[str]:
        candidate = _find(rows, method)
        if not candidate:
            return [f"{method}_not_available"]
        reasons: list[str] = []
        if candidate.get("selection_status") == "reference_only":
            reasons.append(f"{method}_configured_reference_only")
        if candidate.get("selection_status") == "not_recommended":
            reasons.append(f"{method}_not_recommended")
        reasons.extend(_text(item) for item in candidate.get("weaknesses", [])[:3])
        return reasons or [f"{method}_not_selected_by_reviewed_policy"]

    return {
        "recommended_research_method": recommended or None,
        "primary_reasons": [
            {
                "reason": "reviewed_policy_selection",
                "evidence": [
                    f"recommended_rank={row.get('rank')}",
                    f"recommended_overall_score={row.get('overall_score')}",
                    f"top_score_method={top.get('target_method')}",
                ],
                "confidence": "MEDIUM" if row else "LOW",
            }
        ],
        "secondary_reasons": [
            {
                "reason": "historical_research_only_boundary",
                "evidence": ["not_pit_safe=true", "production_effect=none"],
                "confidence": "HIGH",
            }
        ],
        "why_not_consensus_target": why_not("consensus_target"),
        "why_not_defensive_limited_adjustment": why_not("defensive_limited_adjustment"),
        "why_not_static_baseline": why_not("static_baseline"),
        "why_not_selected_top_candidate": why_not("selected_top_candidate"),
        **SYSTEM_TARGET_SAFETY,
    }


def _review_breakdown(
    rows: Sequence[Mapping[str, Any]], decision: Mapping[str, Any], data_quality_status: str
) -> dict[str, Any]:
    recommended = _find(rows, _text(decision.get("recommended_research_method")))
    reasons: list[dict[str, Any]] = []
    if data_quality_status == "PASS_WITH_WARNINGS":
        reasons.append(
            {"reason": "data_quality_pass_with_warnings", "severity": "WARNING", "blocking": False}
        )
    elif data_quality_status == "FAIL":
        reasons.append({"reason": "data_quality_failed", "severity": "BLOCKER", "blocking": True})
    if decision.get("decision_status") == "REVIEW_REQUIRED":
        reasons.append(
            {
                "reason": "selection_owner_review_required",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if recommended.get("review_required_reasons"):
        reasons.append(
            {
                "reason": "recommended_method_evidence_limited",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if not reasons:
        reasons.append({"reason": "no_blocking_reason", "severity": "INFO", "blocking": False})
    return {
        "decision_status": decision.get("decision_status"),
        "review_required_reasons": reasons,
        "can_harden_research_method": not any(row["blocking"] for row in reasons),
        "warning_is_not_technical_failure": True,
        "can_trigger_official_target_weights": False,
        "can_trigger_production": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_attribution(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    recommendation: Mapping[str, Any],
    review: Mapping[str, Any],
) -> str:
    lines = [
        f"# Selection Attribution {manifest.get('attribution_id')}",
        "",
        f"- selection_review_id: {manifest.get('selection_review_id')}",
        f"- backfill_id: {manifest.get('backfill_id')}",
        f"- recommended_research_method: {recommendation.get('recommended_research_method')}",
        f"- decision_status: {review.get('decision_status')}",
        "- missing_components_remain_null: true",
        "- data_quality_penalty_scored: false",
        "- historical_simulation_only: true",
        "- not_official_target_weights: true",
        "- broker_action_allowed: false",
        "- production_effect: none",
        "",
        "| method | rank | overall_score | selection_status |",
        "| --- | ---: | ---: | --- |",
    ]
    lines.extend(
        "| "
        f"{row.get('target_method')} | {row.get('rank')} | {row.get('overall_score')} | "
        f"{row.get('selection_status')} |"
        for row in rows
    )
    return "\n".join(lines) + "\n"


def _selection_views(
    snapshot: Mapping[str, Any], *, attribution_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("selection_source"))
    source_manifest = _bundle_json(source, "system_target_selection_manifest.json")
    scorecard = _bundle_json(source, "target_method_scorecard.json")
    decision = _bundle_json(source, "selection_decision.json")
    rows = _attribution_rows(scorecard, decision, _text(source_manifest.get("data_quality_status")))
    recommendation = _recommendation_breakdown(rows, decision)
    review = _review_breakdown(rows, decision, _text(source_manifest.get("data_quality_status")))
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_selection_attribution_manifest",
        "attribution_id": attribution_id,
        "selection_review_id": source.get("artifact_id"),
        "backfill_id": source_manifest.get("backfill_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS" if rows else "FAIL",
        "market_regime": source_manifest.get("market_regime"),
        "date_start": source_manifest.get("date_start"),
        "date_end": source_manifest.get("date_end"),
        "data_quality_status": source_manifest.get("data_quality_status"),
        "input_snapshot_schema": SELECTION_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "selection_attribution_input_snapshot_path": str(
            output_dir / "selection_attribution_input_snapshot.json"
        ),
        "selection_attribution_manifest_path": str(
            output_dir / "selection_attribution_manifest.json"
        ),
        "method_score_attribution_path": str(output_dir / "method_score_attribution.jsonl"),
        "recommendation_reason_breakdown_path": str(
            output_dir / "recommendation_reason_breakdown.json"
        ),
        "review_required_reason_breakdown_path": str(
            output_dir / "review_required_reason_breakdown.json"
        ),
        "selection_attribution_report_path": str(output_dir / "selection_attribution_report.md"),
        "missing_components_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "selection_attribution_input_snapshot.json": _json_bytes(snapshot),
        "selection_attribution_manifest.json": _json_bytes(manifest),
        "method_score_attribution.jsonl": _jsonl_bytes(rows),
        "recommendation_reason_breakdown.json": _json_bytes(recommendation),
        "review_required_reason_breakdown.json": _json_bytes(review),
        "selection_attribution_report.md": target_core._text_bytes(
            _render_attribution(manifest, rows, recommendation, review)
        ),
    }
    return views, {
        "manifest": manifest,
        "method_score_attribution": rows,
        "recommendation_reason_breakdown": recommendation,
        "review_required_reason_breakdown": review,
    }


def run_selection_attribution(
    *,
    selection_review_id: str,
    selection_review_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source = _selection_binding(selection_review_id, selection_review_dir)
    _require(
        _manifest_generated(source, "system_target_selection_manifest.json") <= generated,
        "selection review generated after attribution",
    )
    snapshot = {
        "schema_version": SELECTION_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "selection_source": source,
        "missing_components_remain_null": True,
        "reference_only_policy_preserved": True,
        **SYSTEM_TARGET_SAFETY,
    }
    attribution_id = _stable_id(
        "selection-attribution-v2", selection_review_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / attribution_id)
    views, payload = _selection_views(snapshot, attribution_id=root.name, output_dir=root)
    _write(root, views, "latest_selection_attribution", "selection_attribution_manifest.json")
    return {"attribution_id": root.name, "attribution_dir": root, **payload}


def selection_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else attribution_id,
        pointer_name="latest_selection_attribution",
    )
    return {
        **_read_json(root / "selection_attribution_manifest.json"),
        **_report_payload(
            root,
            {
                "selection_attribution_input_snapshot": "selection_attribution_input_snapshot.json",
                "method_score_attribution": "method_score_attribution.jsonl",
                "recommendation_reason_breakdown": "recommendation_reason_breakdown.json",
                "review_required_reason_breakdown": "review_required_reason_breakdown.json",
            },
        ),
        "attribution_dir": str(root),
    }


def validate_selection_attribution_artifact(
    *, attribution_id: str, output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR
) -> dict[str, Any]:
    root = output_dir / attribution_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "selection_attribution_input_snapshot.json")
        errors = []
        _require(
            snapshot.get("schema_version") == SELECTION_ATTRIBUTION_SNAPSHOT_SCHEMA,
            "attribution snapshot schema invalid",
        )
        errors.extend(_validate_source_binding(_mapping(snapshot.get("selection_source"))))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="attribution generated_at"
        )
        _require(
            _manifest_generated(
                _mapping(snapshot.get("selection_source")), "system_target_selection_manifest.json"
            )
            <= generated,
            "attribution chronology invalid",
        )
        views, payload = _selection_views(snapshot, attribution_id=attribution_id, output_dir=root)
        drift = _view_errors(root, views)
        decision = payload["recommendation_reason_breakdown"]
        checks.extend(
            [
                _check("snapshot_and_live_selection", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "recommended_method_visible",
                    bool(decision.get("recommended_research_method")),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("selection_attribution_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_selection_attribution_validation",
        attribution_id,
        checks,
        artifact_id_key="attribution_id",
    )


def _path_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    if len(ordered) < 2:
        return {
            "observation_count": len(ordered),
            "total_return": None,
            "annualized_return": None,
            "max_drawdown": None,
            "realized_volatility": None,
            "turnover": None,
        }
    values = [float(row["portfolio_value"]) for row in ordered]
    daily = [float(row["daily_return"]) for row in ordered]
    total_return = values[-1] / values[0] - 1.0
    periods = len(ordered) - 1
    annualized = (1.0 + total_return) ** (252.0 / periods) - 1.0 if total_return > -1.0 else -1.0
    mean = sum(daily) / len(daily)
    variance = sum((value - mean) ** 2 for value in daily) / max(1, len(daily) - 1)
    return {
        "observation_count": len(ordered),
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(min(float(row["drawdown"]) for row in ordered), 10),
        "realized_volatility": round(math.sqrt(variance) * math.sqrt(252.0), 10),
        "turnover": round(sum(float(row["turnover"]) for row in ordered), 10),
    }


def _delta(left: Any, right: Any) -> float | None:
    return round(float(left) - float(right), 10) if _finite(left) and _finite(right) else None


def _risk_status(candidate: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    if not all(
        _finite(value)
        for value in (
            candidate.get("total_return"),
            candidate.get("max_drawdown"),
            baseline.get("total_return"),
            baseline.get("max_drawdown"),
        )
    ):
        return "INSUFFICIENT_DATA"
    return_improves = float(candidate["total_return"]) > float(baseline["total_return"])
    risk_improves = float(candidate["max_drawdown"]) >= float(baseline["max_drawdown"])
    return (
        "RETURN_IMPROVES_RISK_IMPROVES"
        if return_improves and risk_improves
        else "RETURN_IMPROVES_RISK_WORSENS"
        if return_improves
        else "RETURN_WORSE_RISK_IMPROVES"
        if risk_improves
        else "RETURN_WORSE_RISK_WORSE"
    )


def _exposure_summary(
    rows: Sequence[Mapping[str, Any]], risk_symbols: set[str], semiconductor_symbols: set[str]
) -> dict[str, Any]:
    if not rows:
        return {
            "avg_risk_asset_weight": None,
            "max_risk_asset_weight": None,
            "avg_semiconductor_weight": None,
            "max_semiconductor_weight": None,
            "avg_cash_weight": None,
            "min_cash_weight": None,
        }
    risk: list[float] = []
    semiconductor: list[float] = []
    cash: list[float] = []
    for row in rows:
        weights = target_core._weights(row.get("weights"), field="long risk weights")
        risk.append(sum(value for symbol, value in weights.items() if symbol in risk_symbols))
        semiconductor.append(
            sum(value for symbol, value in weights.items() if symbol in semiconductor_symbols)
        )
        cash.append(weights.get("CASH", 0.0))
    return {
        "avg_risk_asset_weight": round(sum(risk) / len(risk), 10),
        "max_risk_asset_weight": round(max(risk), 10),
        "avg_semiconductor_weight": round(sum(semiconductor) / len(semiconductor), 10),
        "max_semiconductor_weight": round(max(semiconductor), 10),
        "avg_cash_weight": round(sum(cash) / len(cash), 10),
        "min_cash_weight": round(min(cash), 10),
    }


def _risk_outputs(
    snapshot: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    binding = _mapping(snapshot.get("backfill_source"))
    states = history._state_rows(binding)
    manifest = _bundle_json(binding, "paper_shadow_backfill_manifest.json")
    policy = _policy_from_backfill_binding(binding)
    candidate_method = _text(policy.get("candidate_method"))
    baselines = [_text(item) for item in policy.get("comparison_baselines", [])]
    candidate_rows = [row for row in states if row.get("target_method") == candidate_method]
    candidate = _path_metrics(candidate_rows)
    baseline_metrics = {
        method: _path_metrics([row for row in states if row.get("target_method") == method])
        for method in baselines
    }
    primary_baseline = baseline_metrics[baselines[0]]
    metrics = {
        **candidate,
        "relative_to_static_baseline": _delta(
            candidate.get("total_return"),
            baseline_metrics.get("static_baseline", {}).get("total_return"),
        ),
        "relative_to_no_trade_baseline": _delta(
            candidate.get("total_return"),
            baseline_metrics.get("no_trade_baseline", {}).get("total_return"),
        ),
    }
    long_window = {
        "target_method": candidate_method,
        "date_start": manifest.get("date_start"),
        "date_end": manifest.get("date_end"),
        "metrics": metrics,
        "risk_return_status": _risk_status(candidate, primary_baseline),
        "confidence": "LOW" if manifest.get("data_quality_status") != "PASS" else "MEDIUM",
        "not_pit_safe": True,
        **SYSTEM_TARGET_SAFETY,
    }
    comparisons: list[dict[str, Any]] = []
    for method, baseline in baseline_metrics.items():
        return_delta = _delta(candidate.get("total_return"), baseline.get("total_return"))
        drawdown_delta = _delta(candidate.get("max_drawdown"), baseline.get("max_drawdown"))
        volatility_delta = _delta(
            candidate.get("realized_volatility"), baseline.get("realized_volatility")
        )
        turnover_delta = _delta(candidate.get("turnover"), baseline.get("turnover"))
        if any(value is None for value in (return_delta, drawdown_delta, volatility_delta)):
            conclusion = "insufficient_data"
        elif return_delta > 0.0 and drawdown_delta >= 0.0 and volatility_delta <= 0.0:
            conclusion = "limited_better"
        elif return_delta <= 0.0 and drawdown_delta < 0.0 and volatility_delta > 0.0:
            conclusion = "baseline_better"
        else:
            conclusion = "mixed"
        comparisons.append(
            {
                "baseline": method,
                "return_delta": return_delta,
                "drawdown_delta": drawdown_delta,
                "volatility_delta": volatility_delta,
                "turnover_delta": turnover_delta,
                "conclusion": conclusion,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    breakdown = {
        "comparisons": comparisons,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    risk_symbols = set(_text(item) for item in policy.get("risk_exposure_symbols", []))
    semiconductor = set(_text(item) for item in policy.get("semiconductor_symbols", []))
    candidate_exposure = _exposure_summary(candidate_rows, risk_symbols, semiconductor)
    baseline_rows = [row for row in states if row.get("target_method") == baselines[0]]
    baseline_exposure = _exposure_summary(baseline_rows, risk_symbols, semiconductor)
    left = candidate_exposure.get("avg_risk_asset_weight")
    right = baseline_exposure.get("avg_risk_asset_weight")
    tolerance = float(policy["exposure_similarity_tolerance"])
    if not _finite(left) or not _finite(right):
        interpretation = "mixed"
    elif float(left) > float(right) + tolerance:
        interpretation = "higher_risk_exposure"
    elif float(left) < float(right) - tolerance:
        interpretation = "lower_risk_exposure"
    else:
        interpretation = "similar_risk_exposure"
    exposure = {
        "target_method": candidate_method,
        **candidate_exposure,
        "comparison_baseline": baselines[0],
        "baseline_avg_risk_asset_weight": right,
        "exposure_similarity_tolerance": tolerance,
        "risk_exposure_interpretation": interpretation,
        "warnings": ["candidate_higher_risk_asset_exposure"]
        if interpretation == "higher_risk_exposure"
        else [],
        **SYSTEM_TARGET_SAFETY,
    }
    return long_window, breakdown, exposure


def _render_risk(
    manifest: Mapping[str, Any],
    long_window: Mapping[str, Any],
    comparisons: Mapping[str, Any],
    exposure: Mapping[str, Any],
) -> str:
    metrics = _mapping(long_window.get("metrics"))
    return "\n".join(
        [
            f"# Limited Long-window Risk {manifest.get('risk_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- total_return: {metrics.get('total_return')}",
            f"- max_drawdown: {metrics.get('max_drawdown')}",
            f"- realized_volatility: {metrics.get('realized_volatility')}",
            f"- turnover: {metrics.get('turnover')}",
            f"- risk_return_status: {long_window.get('risk_return_status')}",
            f"- risk_exposure_interpretation: {exposure.get('risk_exposure_interpretation')}",
            "- missing_metrics_remain_null: true",
            "- not_pit_safe: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _risk_views(
    snapshot: Mapping[str, Any], *, risk_review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    binding = _mapping(snapshot.get("backfill_source"))
    source_manifest = _bundle_json(binding, "paper_shadow_backfill_manifest.json")
    long_window, breakdown, exposure = _risk_outputs(snapshot)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_limited_long_risk_manifest",
        "risk_review_id": risk_review_id,
        "backfill_id": binding.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": source_manifest.get("market_regime"),
        "date_start": source_manifest.get("date_start"),
        "date_end": source_manifest.get("date_end"),
        "data_quality_status": source_manifest.get("data_quality_status"),
        "input_snapshot_schema": LIMITED_LONG_RISK_SNAPSHOT_SCHEMA,
        "limited_long_risk_input_snapshot_path": str(
            output_dir / "limited_long_risk_input_snapshot.json"
        ),
        "limited_long_risk_manifest_path": str(output_dir / "limited_long_risk_manifest.json"),
        "long_window_risk_return_path": str(output_dir / "long_window_risk_return.json"),
        "limited_vs_baseline_breakdown_path": str(
            output_dir / "limited_vs_baseline_breakdown.json"
        ),
        "exposure_path_analysis_path": str(output_dir / "exposure_path_analysis.json"),
        "limited_long_risk_report_path": str(output_dir / "limited_long_risk_report.md"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "limited_long_risk_input_snapshot.json": _json_bytes(snapshot),
        "limited_long_risk_manifest.json": _json_bytes(manifest),
        "long_window_risk_return.json": _json_bytes(long_window),
        "limited_vs_baseline_breakdown.json": _json_bytes(breakdown),
        "exposure_path_analysis.json": _json_bytes(exposure),
        "limited_long_risk_report.md": target_core._text_bytes(
            _render_risk(manifest, long_window, breakdown, exposure)
        ),
    }
    return views, {
        "manifest": manifest,
        "long_window_risk_return": long_window,
        "limited_vs_baseline_breakdown": breakdown,
        "exposure_path_analysis": exposure,
    }


def run_limited_long_risk_review(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source = _backfill_binding(backfill_id, backfill_dir)
    _policy_from_backfill_binding(source)
    _require(
        _manifest_generated(source, "paper_shadow_backfill_manifest.json") <= generated,
        "backfill generated after risk review",
    )
    snapshot = {
        "schema_version": LIMITED_LONG_RISK_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": source,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    risk_review_id = _stable_id("limited-long-risk-v2", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / risk_review_id)
    views, payload = _risk_views(snapshot, risk_review_id=root.name, output_dir=root)
    _write(root, views, "latest_limited_long_risk", "limited_long_risk_manifest.json")
    return {"risk_review_id": root.name, "risk_review_dir": root, **payload}


def limited_long_risk_report_payload(
    *,
    risk_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else risk_review_id,
        pointer_name="latest_limited_long_risk",
    )
    return {
        **_read_json(root / "limited_long_risk_manifest.json"),
        **_report_payload(
            root,
            {
                "limited_long_risk_input_snapshot": "limited_long_risk_input_snapshot.json",
                "long_window_risk_return": "long_window_risk_return.json",
                "limited_vs_baseline_breakdown": "limited_vs_baseline_breakdown.json",
                "exposure_path_analysis": "exposure_path_analysis.json",
            },
        ),
        "risk_review_dir": str(root),
    }


def validate_limited_long_risk_artifact(
    *, risk_review_id: str, output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR
) -> dict[str, Any]:
    root = output_dir / risk_review_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "limited_long_risk_input_snapshot.json")
        _require(
            snapshot.get("schema_version") == LIMITED_LONG_RISK_SNAPSHOT_SCHEMA,
            "risk snapshot schema invalid",
        )
        source = _mapping(snapshot.get("backfill_source"))
        errors = _validate_source_binding(source)
        _policy_from_backfill_binding(source)
        generated = target_core._datetime(snapshot.get("generated_at"), field="risk generated_at")
        _require(
            _manifest_generated(source, "paper_shadow_backfill_manifest.json") <= generated,
            "risk chronology invalid",
        )
        views, payload = _risk_views(snapshot, risk_review_id=risk_review_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_and_live_backfill", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "candidate_method_bound",
                    bool(payload["long_window_risk_return"].get("target_method")),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("limited_long_risk_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_limited_long_risk_validation",
        risk_review_id,
        checks,
        artifact_id_key="risk_review_id",
    )


def _consistency_outputs(
    snapshot: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    candidate = _text(policy.get("candidate_method"))
    rolling_source = _mapping(snapshot.get("rolling_source"))
    rolling_rank = _find(
        _records(_bundle_json(rolling_source, "rolling_rank_stability.json").get("methods")),
        candidate,
    )
    rolling_metrics = [
        row
        for row in _bundle_jsonl(rolling_source, "rolling_method_metrics.jsonl")
        if row.get("target_method") == candidate
        and isinstance(row.get("rank_by_risk_adjusted"), int)
    ]
    top_n = rolling_rank.get("top_n")
    top_risk = (
        sum(1 for row in rolling_metrics if int(row["rank_by_risk_adjusted"]) <= int(top_n))
        / len(rolling_metrics)
        if rolling_metrics and isinstance(top_n, int)
        else None
    )
    rolling = {
        "target_method": candidate,
        "rolling_windows_total": rolling_rank.get("eligible_window_count", 0),
        "top_n": top_n,
        "top_n_frequency_by_return": rolling_rank.get("top_n_frequency"),
        "top_n_frequency_by_risk_adjusted": _optional(top_risk, digits=6),
        "bottom_n_frequency": rolling_rank.get("bottom_n_frequency"),
        "avg_rank_return": rolling_rank.get("avg_rank_return"),
        "avg_rank_risk_adjusted": rolling_rank.get("avg_rank_risk_adjusted"),
        "rolling_consistency_status": _text(
            rolling_rank.get("rank_stability_status"), "INSUFFICIENT_DATA"
        ),
        "status_from_reviewed_rank_policy": True,
        **SYSTEM_TARGET_SAFETY,
    }
    regime_source = _mapping(snapshot.get("regime_source"))
    regime_metrics = _bundle_jsonl(regime_source, "method_regime_metrics.jsonl")
    rows: list[dict[str, Any]] = []
    for regime_name in history._configured_regimes():
        candidate_row = next(
            (
                row
                for row in regime_metrics
                if row.get("regime") == regime_name and row.get("target_method") == candidate
            ),
            {},
        )
        static_row = next(
            (
                row
                for row in regime_metrics
                if row.get("regime") == regime_name
                and row.get("target_method") == "static_baseline"
            ),
            {},
        )
        no_trade_row = next(
            (
                row
                for row in regime_metrics
                if row.get("regime") == regime_name
                and row.get("target_method") == "no_trade_baseline"
            ),
            {},
        )
        rel_static = _delta(candidate_row.get("total_return"), static_row.get("total_return"))
        rel_no_trade = _delta(candidate_row.get("total_return"), no_trade_row.get("total_return"))
        available = [
            row
            for row in regime_metrics
            if row.get("regime") == regime_name
            and row.get("status") == "PASS"
            and _finite(row.get("total_return"))
        ]
        rank = None
        if _finite(candidate_row.get("total_return")):
            ordered = sorted(available, key=lambda row: float(row["total_return"]), reverse=True)
            rank = next(
                (
                    index
                    for index, row in enumerate(ordered, start=1)
                    if row.get("target_method") == candidate
                ),
                None,
            )
        if rel_static is None or rel_no_trade is None:
            status = "INSUFFICIENT_DATA"
        elif rel_static >= 0.0 and rel_no_trade >= 0.0:
            status = "PASS"
        elif rel_static >= 0.0 or rel_no_trade >= 0.0:
            status = "PASS_WITH_WARNINGS"
        else:
            status = "FAIL"
        rows.append(
            {
                "regime": regime_name,
                "relative_to_static_baseline": rel_static,
                "relative_to_no_trade_baseline": rel_no_trade,
                "rank": rank,
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    pressure = set(_text(item) for item in policy.get("pressure_regimes", []))
    available_statuses = {row["status"] for row in rows if row["status"] != "INSUFFICIENT_DATA"}
    if not available_statuses:
        overall = "INSUFFICIENT_DATA"
    elif any(row["regime"] in pressure and row["status"] == "FAIL" for row in rows):
        overall = "WEAK_IN_PRESSURE"
    elif "FAIL" in available_statuses or "PASS_WITH_WARNINGS" in available_statuses:
        overall = "REGIME_DEPENDENT"
    else:
        overall = "BROADLY_CONSISTENT"
    regime = {
        "target_method": candidate,
        "regimes": rows,
        "regime_consistency_status": overall,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    stability_source = _mapping(snapshot.get("stability_source"))
    metrics = _find(_bundle_jsonl(stability_source, "method_stability_metrics.jsonl"), candidate)
    turnover = _find(
        _records(_bundle_json(stability_source, "turnover_diagnostics.json").get("methods")),
        candidate,
    )
    stability = {
        "target_method": candidate,
        "avg_rebalance_turnover": metrics.get("avg_rebalance_turnover"),
        "max_rebalance_turnover": metrics.get("max_rebalance_turnover"),
        "large_jump_count": metrics.get("large_jump_count", 0),
        "stability_status": _text(metrics.get("stability_status"), "INSUFFICIENT_DATA"),
        "turnover_status": _text(turnover.get("turnover_status"), "INSUFFICIENT_DATA"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    return rolling, regime, stability


def _render_consistency(
    manifest: Mapping[str, Any],
    rolling: Mapping[str, Any],
    regime: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Limited Consistency {manifest.get('consistency_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- rolling_consistency_status: {rolling.get('rolling_consistency_status')}",
            f"- regime_consistency_status: {regime.get('regime_consistency_status')}",
            f"- stability_status: {stability.get('stability_status')}",
            f"- turnover_status: {stability.get('turnover_status')}",
            "- exact_same_backfill_lineage: true",
            "- missing_metrics_remain_null: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _consistency_views(
    snapshot: Mapping[str, Any], *, consistency_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill = _mapping(snapshot.get("backfill_source"))
    backfill_manifest = _bundle_json(backfill, "paper_shadow_backfill_manifest.json")
    policy = _policy_from_backfill_binding(backfill)
    rolling, regime, stability = _consistency_outputs(snapshot, policy)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_limited_consistency_manifest",
        "consistency_id": consistency_id,
        "backfill_id": backfill.get("artifact_id"),
        "rolling_eval_id": _mapping(snapshot.get("rolling_source")).get("artifact_id"),
        "regime_review_id": _mapping(snapshot.get("regime_source")).get("artifact_id"),
        "stability_id": _mapping(snapshot.get("stability_source")).get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime"),
        "date_start": backfill_manifest.get("date_start"),
        "date_end": backfill_manifest.get("date_end"),
        "data_quality_status": backfill_manifest.get("data_quality_status"),
        "input_snapshot_schema": LIMITED_CONSISTENCY_SNAPSHOT_SCHEMA,
        "limited_consistency_input_snapshot_path": str(
            output_dir / "limited_consistency_input_snapshot.json"
        ),
        "limited_consistency_manifest_path": str(output_dir / "limited_consistency_manifest.json"),
        "rolling_consistency_summary_path": str(output_dir / "rolling_consistency_summary.json"),
        "regime_consistency_summary_path": str(output_dir / "regime_consistency_summary.json"),
        "stability_consistency_summary_path": str(
            output_dir / "stability_consistency_summary.json"
        ),
        "limited_consistency_report_path": str(output_dir / "limited_consistency_report.md"),
        "exact_same_backfill_lineage": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "limited_consistency_input_snapshot.json": _json_bytes(snapshot),
        "limited_consistency_manifest.json": _json_bytes(manifest),
        "rolling_consistency_summary.json": _json_bytes(rolling),
        "regime_consistency_summary.json": _json_bytes(regime),
        "stability_consistency_summary.json": _json_bytes(stability),
        "limited_consistency_report.md": target_core._text_bytes(
            _render_consistency(manifest, rolling, regime, stability)
        ),
    }
    return views, {
        "manifest": manifest,
        "rolling_consistency_summary": rolling,
        "regime_consistency_summary": regime,
        "stability_consistency_summary": stability,
    }


def _validate_consistency_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == LIMITED_CONSISTENCY_SNAPSHOT_SCHEMA,
            "consistency snapshot schema invalid",
        )
        bindings = [
            _mapping(snapshot.get("backfill_source")),
            _mapping(snapshot.get("rolling_source")),
            _mapping(snapshot.get("regime_source")),
            _mapping(snapshot.get("stability_source")),
        ]
        for binding in bindings:
            errors.extend(_validate_source_binding(binding))
        backfill_id = bindings[0].get("artifact_id")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="consistency generated_at"
        )
        for binding, name in zip(
            bindings[1:],
            (
                "rolling_eval_manifest.json",
                "paper_shadow_regime_manifest.json",
                "paper_shadow_stability_manifest.json",
            ),
            strict=True,
        ):
            manifest = _bundle_json(binding, name)
            _require(
                manifest.get("backfill_id") == backfill_id, "consistency cross-backfill lineage"
            )
            _require(
                target_core._datetime(
                    manifest.get("generated_at"), field="consistency source generated_at"
                )
                <= generated,
                "consistency source generated after result",
            )
        _policy_from_backfill_binding(bindings[0])
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _unique_history_source_id(
    *,
    root: Path,
    manifest_name: str,
    backfill_id: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> str:
    candidates: list[str] = []
    if root.exists():
        for path in sorted(root.glob(f"*/{manifest_name}")):
            try:
                manifest = _read_json(path)
                artifact_id = path.parent.name
                if manifest.get("backfill_id") != backfill_id:
                    continue
                validation = validator(**{validator_key: artifact_id, "output_dir": root})
                if validation.get("status") == "PASS":
                    candidates.append(artifact_id)
            except Exception:  # noqa: BLE001
                continue
    _require(
        len(candidates) == 1,
        f"{manifest_name} requires exactly one validated same-backfill source; "
        f"found={len(candidates)}",
    )
    return candidates[0]


def run_limited_consistency_check(
    *,
    backfill_id: str,
    rolling_eval_id: str | None = None,
    regime_review_id: str | None = None,
    stability_id: str | None = None,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    rolling_eval_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    regime_review_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    stability_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    resolved_rolling_id = rolling_eval_id or _unique_history_source_id(
        root=rolling_eval_dir,
        manifest_name="rolling_eval_manifest.json",
        backfill_id=backfill_id,
        validator=history.validate_paper_shadow_rolling_eval_artifact,
        validator_key="rolling_eval_id",
    )
    resolved_regime_id = regime_review_id or _unique_history_source_id(
        root=regime_review_dir,
        manifest_name="paper_shadow_regime_manifest.json",
        backfill_id=backfill_id,
        validator=history.validate_paper_shadow_regime_review_artifact,
        validator_key="regime_review_id",
    )
    resolved_stability_id = stability_id or _unique_history_source_id(
        root=stability_dir,
        manifest_name="paper_shadow_stability_manifest.json",
        backfill_id=backfill_id,
        validator=history.validate_paper_shadow_stability_artifact,
        validator_key="stability_id",
    )
    snapshot = {
        "schema_version": LIMITED_CONSISTENCY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": _backfill_binding(backfill_id, backfill_dir),
        "rolling_source": _rolling_binding(resolved_rolling_id, rolling_eval_dir),
        "regime_source": _regime_binding(resolved_regime_id, regime_review_dir),
        "stability_source": _stability_binding(resolved_stability_id, stability_dir),
        "exact_same_backfill_lineage_required": True,
        "implicit_upstream_run_allowed": False,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_consistency_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    consistency_id = _stable_id(
        "limited-consistency-v2",
        backfill_id,
        resolved_rolling_id,
        resolved_regime_id,
        resolved_stability_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / consistency_id)
    views, payload = _consistency_views(snapshot, consistency_id=root.name, output_dir=root)
    _write(root, views, "latest_limited_consistency", "limited_consistency_manifest.json")
    return {"consistency_id": root.name, "consistency_dir": root, **payload}


def limited_consistency_report_payload(
    *,
    consistency_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else consistency_id,
        pointer_name="latest_limited_consistency",
    )
    return {
        **_read_json(root / "limited_consistency_manifest.json"),
        **_report_payload(
            root,
            {
                "limited_consistency_input_snapshot": "limited_consistency_input_snapshot.json",
                "rolling_consistency_summary": "rolling_consistency_summary.json",
                "regime_consistency_summary": "regime_consistency_summary.json",
                "stability_consistency_summary": "stability_consistency_summary.json",
            },
        ),
        "consistency_dir": str(root),
    }


def validate_limited_consistency_artifact(
    *, consistency_id: str, output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR
) -> dict[str, Any]:
    root = output_dir / consistency_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "limited_consistency_input_snapshot.json")
        errors = _validate_consistency_snapshot(snapshot)
        views, _ = _consistency_views(snapshot, consistency_id=consistency_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_live_sources_and_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "implicit_upstream_run_forbidden",
                    snapshot.get("implicit_upstream_run_allowed") is False,
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("limited_consistency_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_limited_consistency_validation",
        consistency_id,
        checks,
        artifact_id_key="consistency_id",
    )


def _warning_outputs(
    snapshot: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    backfill = _mapping(snapshot.get("backfill_source"))
    selection = _mapping(snapshot.get("selection_source"))
    quality = _bundle_json(backfill, "backfill_data_quality.json")
    backfill_manifest = _bundle_json(backfill, "paper_shadow_backfill_manifest.json")
    selection_decision = _bundle_json(selection, "selection_decision.json")
    data_quality = _text(
        backfill_manifest.get("data_quality_status"), _text(quality.get("data_quality"))
    )
    warnings: list[dict[str, Any]] = []
    for item in [*_records(quality.get("warnings")), *_records(quality.get("issues"))]:
        if _text(item.get("severity"), "WARNING") not in {"WARNING", "INFO"}:
            continue
        warnings.append(
            {
                "warning_id": _text(
                    item.get("warning_id"), _text(item.get("code"), "data_quality_warning")
                ),
                "severity": _text(item.get("severity"), "WARNING"),
                "affected_symbols": [_text(value) for value in item.get("affected_symbols", [])],
                "affected_dates": [_text(value) for value in item.get("affected_dates", [])],
                "potential_metric_impact": _text(item.get("potential_metric_impact"), "UNKNOWN"),
            }
        )
    if data_quality == "PASS_WITH_WARNINGS" and not warnings:
        warnings.append(
            {
                "warning_id": "pass_with_warnings_detail_unavailable",
                "severity": "WARNING",
                "affected_symbols": [],
                "affected_dates": [],
                "potential_metric_impact": "UNKNOWN",
            }
        )
    inventory = {
        "backfill_id": backfill.get("artifact_id"),
        "data_quality": data_quality,
        "warnings": warnings,
        "warning_detail_complete": data_quality != "PASS_WITH_WARNINGS"
        or bool(_records(quality.get("warnings"))),
        **SYSTEM_TARGET_SAFETY,
    }
    levels = {_text(row.get("potential_metric_impact"), "UNKNOWN") for row in warnings}
    if not warnings:
        level, affected_value, reason = "LOW", False, "no_recorded_warning"
    elif "UNKNOWN" in levels:
        level, affected_value, reason = "UNKNOWN", None, "warning_detail_missing_or_unquantified"
    elif "HIGH" in levels:
        level, affected_value, reason = "HIGH", True, "high_potential_warning_impact"
    elif "MEDIUM" in levels:
        level, affected_value, reason = "MEDIUM", True, "medium_potential_warning_impact"
    else:
        level, affected_value, reason = "LOW", False, "warning_not_expected_to_move_core_metrics"
    affected = {
        "metrics": [
            {"metric": metric, "affected": affected_value, "impact_level": level, "reason": reason}
            for metric in ("total_return", "max_drawdown", "realized_volatility", "turnover")
        ],
        **SYSTEM_TARGET_SAFETY,
    }
    if data_quality == "FAIL":
        stability, would_change, dq_decision, blocking = (
            "UNSTABLE",
            True,
            "BLOCKED",
            ["data_quality_failed"],
        )
    elif level == "UNKNOWN" or level in {"HIGH", "MEDIUM"}:
        stability, would_change, dq_decision, blocking = (
            "REVIEW_REQUIRED",
            None,
            "REVIEW_REQUIRED",
            ["warning_metric_impact_unknown_or_material"],
        )
    else:
        stability, would_change, dq_decision, blocking = "STABLE", False, "ACCEPT_FOR_RESEARCH", []
    sensitivity = {
        "recommended_research_method": selection_decision.get("recommended_research_method"),
        "recommendation_stability": stability,
        "would_change_if_warnings_excluded": would_change,
        "warning_blocking_reasons": blocking,
        "warning_ids": [row["warning_id"] for row in warnings],
        "data_quality_decision": dq_decision,
        **SYSTEM_TARGET_SAFETY,
    }
    return inventory, affected, sensitivity


def _render_warning(
    manifest: Mapping[str, Any],
    inventory: Mapping[str, Any],
    affected: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Data Warning Impact {manifest.get('impact_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- selection_review_id: {manifest.get('selection_review_id')}",
            f"- data_quality: {inventory.get('data_quality')}",
            f"- warning_detail_complete: {str(inventory.get('warning_detail_complete')).lower()}",
            f"- recommendation_stability: {sensitivity.get('recommendation_stability')}",
            f"- data_quality_decision: {sensitivity.get('data_quality_decision')}",
            "- missing_warning_detail_is_unknown: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _warning_views(
    snapshot: Mapping[str, Any], *, impact_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill = _mapping(snapshot.get("backfill_source"))
    selection = _mapping(snapshot.get("selection_source"))
    manifest_source = _bundle_json(backfill, "paper_shadow_backfill_manifest.json")
    inventory, affected, sensitivity = _warning_outputs(snapshot)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_data_warning_impact_manifest",
        "impact_id": impact_id,
        "backfill_id": backfill.get("artifact_id"),
        "selection_review_id": selection.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": manifest_source.get("market_regime"),
        "date_start": manifest_source.get("date_start"),
        "date_end": manifest_source.get("date_end"),
        "data_quality_status": manifest_source.get("data_quality_status"),
        "input_snapshot_schema": DATA_WARNING_IMPACT_SNAPSHOT_SCHEMA,
        "data_warning_impact_input_snapshot_path": str(
            output_dir / "data_warning_impact_input_snapshot.json"
        ),
        "data_warning_impact_manifest_path": str(output_dir / "data_warning_impact_manifest.json"),
        "data_warning_inventory_path": str(output_dir / "data_warning_inventory.json"),
        "affected_metrics_path": str(output_dir / "affected_metrics.json"),
        "recommendation_sensitivity_to_warnings_path": str(
            output_dir / "recommendation_sensitivity_to_warnings.json"
        ),
        "data_warning_impact_report_path": str(output_dir / "data_warning_impact_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "data_warning_impact_input_snapshot.json": _json_bytes(snapshot),
        "data_warning_impact_manifest.json": _json_bytes(manifest),
        "data_warning_inventory.json": _json_bytes(inventory),
        "affected_metrics.json": _json_bytes(affected),
        "recommendation_sensitivity_to_warnings.json": _json_bytes(sensitivity),
        "data_warning_impact_report.md": target_core._text_bytes(
            _render_warning(manifest, inventory, affected, sensitivity)
        ),
    }
    return views, {
        "manifest": manifest,
        "data_warning_inventory": inventory,
        "affected_metrics": affected,
        "recommendation_sensitivity_to_warnings": sensitivity,
    }


def _validate_warning_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == DATA_WARNING_IMPACT_SNAPSHOT_SCHEMA,
            "warning snapshot schema invalid",
        )
        backfill = _mapping(snapshot.get("backfill_source"))
        selection = _mapping(snapshot.get("selection_source"))
        errors.extend(_validate_source_binding(backfill))
        errors.extend(_validate_source_binding(selection))
        selection_manifest = _bundle_json(selection, "system_target_selection_manifest.json")
        _require(
            selection_manifest.get("backfill_id") == backfill.get("artifact_id"),
            "warning cross-backfill selection",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="warning generated_at"
        )
        for binding, name in (
            (backfill, "paper_shadow_backfill_manifest.json"),
            (selection, "system_target_selection_manifest.json"),
        ):
            _require(_manifest_generated(binding, name) <= generated, "warning chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_data_warning_impact_review(
    *,
    backfill_id: str,
    selection_review_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    selection_review_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": DATA_WARNING_IMPACT_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": _backfill_binding(backfill_id, backfill_dir),
        "selection_source": _selection_binding(selection_review_id, selection_review_dir),
        "missing_warning_detail_is_unknown": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_warning_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    impact_id = _stable_id(
        "data-warning-impact-v2", backfill_id, selection_review_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / impact_id)
    views, payload = _warning_views(snapshot, impact_id=root.name, output_dir=root)
    _write(root, views, "latest_data_warning_impact", "data_warning_impact_manifest.json")
    return {"impact_id": root.name, "impact_dir": root, **payload}


def data_warning_impact_report_payload(
    *,
    impact_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else impact_id,
        pointer_name="latest_data_warning_impact",
    )
    return {
        **_read_json(root / "data_warning_impact_manifest.json"),
        **_report_payload(
            root,
            {
                "data_warning_impact_input_snapshot": "data_warning_impact_input_snapshot.json",
                "data_warning_inventory": "data_warning_inventory.json",
                "affected_metrics": "affected_metrics.json",
                "recommendation_sensitivity_to_warnings": (
                    "recommendation_sensitivity_to_warnings.json"
                ),
            },
        ),
        "impact_dir": str(root),
    }


def validate_data_warning_impact_artifact(
    *, impact_id: str, output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR
) -> dict[str, Any]:
    root = output_dir / impact_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "data_warning_impact_input_snapshot.json")
        errors = _validate_warning_snapshot(snapshot)
        views, payload = _warning_views(snapshot, impact_id=impact_id, output_dir=root)
        drift = _view_errors(root, views)
        inventory = payload["data_warning_inventory"]
        sensitivity = payload["recommendation_sensitivity_to_warnings"]
        unknown_required = (
            inventory.get("data_quality") != "PASS_WITH_WARNINGS"
            or inventory.get("warning_detail_complete") is True
            or sensitivity.get("data_quality_decision") == "REVIEW_REQUIRED"
        )
        checks.extend(
            [
                _check("snapshot_live_sources_and_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check("missing_warning_detail_remains_unknown", unknown_required, ""),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("data_warning_impact_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_data_warning_impact_validation",
        impact_id,
        checks,
        artifact_id_key="impact_id",
    )


def _leaf_manifest(binding: Mapping[str, Any]) -> dict[str, Any]:
    names = {
        "selection_attribution": "selection_attribution_manifest.json",
        "limited_long_risk": "limited_long_risk_manifest.json",
        "limited_consistency": "limited_consistency_manifest.json",
        "data_warning_impact": "data_warning_impact_manifest.json",
    }
    return _bundle_json(binding, names[_text(binding.get("kind"))])


def _leaf_payload(binding: Mapping[str, Any], name: str, *, jsonl: bool = False) -> Any:
    return _bundle_jsonl(binding, name) if jsonl else _bundle_json(binding, name)


def _hardening_decision(snapshot: Mapping[str, Any], hardening_id: str) -> dict[str, Any]:
    attribution_binding = _mapping(snapshot.get("attribution_source"))
    risk_binding = _mapping(snapshot.get("risk_source"))
    consistency_binding = _mapping(snapshot.get("consistency_source"))
    warning_binding = _mapping(snapshot.get("warning_source"))
    attribution = {
        "method_score_attribution": _leaf_payload(
            attribution_binding, "method_score_attribution.jsonl", jsonl=True
        ),
        "recommendation_reason_breakdown": _leaf_payload(
            attribution_binding, "recommendation_reason_breakdown.json"
        ),
        "review_required_reason_breakdown": _leaf_payload(
            attribution_binding, "review_required_reason_breakdown.json"
        ),
    }
    risk = {"long_window_risk_return": _leaf_payload(risk_binding, "long_window_risk_return.json")}
    consistency = {
        "rolling_consistency_summary": _leaf_payload(
            consistency_binding, "rolling_consistency_summary.json"
        ),
        "regime_consistency_summary": _leaf_payload(
            consistency_binding, "regime_consistency_summary.json"
        ),
        "stability_consistency_summary": _leaf_payload(
            consistency_binding, "stability_consistency_summary.json"
        ),
    }
    warning = {
        "recommendation_sensitivity_to_warnings": _leaf_payload(
            warning_binding, "recommendation_sensitivity_to_warnings.json"
        )
    }
    decision = legacy._research_method_hardening_decision(attribution, risk, consistency, warning)
    decision["hardening_id"] = hardening_id
    decision["historical_simulation_only"] = True
    decision["not_pit_safe"] = True
    decision["workflow_pass_is_not_investment_conclusion"] = True
    return decision


def _render_hardening_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Method Checklist {decision.get('hardening_id')}",
            "",
            "- [ ] 确认四路证据属于同一Backfill与Selection lineage。",
            "- [ ] 确认PASS_WITH_WARNINGS缺明细时仍为UNKNOWN/REVIEW_REQUIRED。",
            "- [ ] 确认historical current-definition replay不是PIT-safe结论。",
            "- [ ] 确认不生成official weights、order或broker action。",
            "",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            "- production_effect: none",
            "",
        ]
    )


def _render_hardening_report(manifest: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Research Method Hardening {manifest.get('hardening_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- selection_review_id: {manifest.get('selection_review_id')}",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- blocking_issues: {', '.join(decision.get('blocking_issues', []))}",
            f"- warnings: {', '.join(decision.get('warnings', []))}",
            "- exact_same_backfill_and_selection_lineage: true",
            "- historical_simulation_only: true",
            "- not_pit_safe: true",
            "- workflow_pass_is_not_investment_conclusion: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_hardening_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Research Method Hardening",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            "- historical_simulation_only: true",
            "- not_pit_safe: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _hardening_views(
    snapshot: Mapping[str, Any], *, hardening_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    attribution = _mapping(snapshot.get("attribution_source"))
    risk = _mapping(snapshot.get("risk_source"))
    consistency = _mapping(snapshot.get("consistency_source"))
    warning = _mapping(snapshot.get("warning_source"))
    attribution_manifest = _leaf_manifest(attribution)
    decision = _hardening_decision(snapshot, hardening_id)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_research_method_hardening_manifest",
        "hardening_id": hardening_id,
        "selection_attribution_id": attribution.get("artifact_id"),
        "risk_review_id": risk.get("artifact_id"),
        "consistency_id": consistency.get("artifact_id"),
        "data_warning_impact_id": warning.get("artifact_id"),
        "backfill_id": attribution_manifest.get("backfill_id"),
        "selection_review_id": attribution_manifest.get("selection_review_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "candidate_method": decision.get("candidate_method"),
        "hardening_decision": decision.get("hardening_decision"),
        "decision_confidence": decision.get("decision_confidence"),
        "input_snapshot_schema": RESEARCH_METHOD_HARDENING_SNAPSHOT_SCHEMA,
        "research_method_hardening_input_snapshot_path": str(
            output_dir / "research_method_hardening_input_snapshot.json"
        ),
        "research_method_hardening_manifest_path": str(
            output_dir / "research_method_hardening_manifest.json"
        ),
        "hardening_decision_path": str(output_dir / "hardening_decision.json"),
        "owner_research_method_checklist_path": str(
            output_dir / "owner_research_method_checklist.md"
        ),
        "research_method_hardening_report_path": str(
            output_dir / "research_method_hardening_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        "exact_same_backfill_and_selection_lineage": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "research_method_hardening_input_snapshot.json": _json_bytes(snapshot),
        "research_method_hardening_manifest.json": _json_bytes(manifest),
        "hardening_decision.json": _json_bytes(decision),
        "owner_research_method_checklist.md": target_core._text_bytes(
            _render_hardening_checklist(decision)
        ),
        "research_method_hardening_report.md": target_core._text_bytes(
            _render_hardening_report(manifest, decision)
        ),
        "reader_brief_section.md": target_core._text_bytes(
            _render_hardening_reader_brief(decision)
        ),
    }
    return views, {"manifest": manifest, "hardening_decision": decision}


def _validate_hardening_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RESEARCH_METHOD_HARDENING_SNAPSHOT_SCHEMA,
            "hardening snapshot schema invalid",
        )
        bindings = [
            _mapping(snapshot.get("attribution_source")),
            _mapping(snapshot.get("risk_source")),
            _mapping(snapshot.get("consistency_source")),
            _mapping(snapshot.get("warning_source")),
        ]
        for binding in bindings:
            errors.extend(_validate_source_binding(binding))
        manifests = [_leaf_manifest(binding) for binding in bindings]
        backfill_ids = {manifest.get("backfill_id") for manifest in manifests}
        selection_ids = {
            manifests[0].get("selection_review_id"),
            manifests[3].get("selection_review_id"),
        }
        _require(
            len(backfill_ids) == 1 and None not in backfill_ids, "hardening cross-backfill lineage"
        )
        _require(
            len(selection_ids) == 1 and None not in selection_ids,
            "hardening cross-selection lineage",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="hardening generated_at"
        )
        _require(
            all(
                target_core._datetime(
                    manifest.get("generated_at"), field="hardening source generated_at"
                )
                <= generated
                for manifest in manifests
            ),
            "hardening chronology invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_research_method_hardening_pack(
    *,
    selection_attribution_id: str,
    risk_review_id: str,
    consistency_id: str,
    data_warning_impact_id: str,
    selection_attribution_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
    risk_review_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
    consistency_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
    data_warning_impact_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
    output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": RESEARCH_METHOD_HARDENING_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "attribution_source": _attribution_binding(
            selection_attribution_id, selection_attribution_dir
        ),
        "risk_source": _risk_binding(risk_review_id, risk_review_dir),
        "consistency_source": _consistency_binding(consistency_id, consistency_dir),
        "warning_source": _warning_binding(data_warning_impact_id, data_warning_impact_dir),
        "exact_same_backfill_and_selection_lineage_required": True,
        "historical_simulation_only": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_hardening_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    hardening_id = _stable_id(
        "research-method-hardening-v2",
        selection_attribution_id,
        risk_review_id,
        consistency_id,
        data_warning_impact_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / hardening_id)
    views, payload = _hardening_views(snapshot, hardening_id=root.name, output_dir=root)
    _write(
        root, views, "latest_research_method_hardening", "research_method_hardening_manifest.json"
    )
    return {"hardening_id": root.name, "hardening_dir": root, **payload}


def research_method_hardening_report_payload(
    *,
    hardening_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else hardening_id,
        pointer_name="latest_research_method_hardening",
    )
    return {
        **_read_json(root / "research_method_hardening_manifest.json"),
        **_report_payload(
            root,
            {
                "research_method_hardening_input_snapshot": (
                    "research_method_hardening_input_snapshot.json"
                ),
                "hardening_decision_payload": "hardening_decision.json",
            },
        ),
        "hardening_dir": str(root),
    }


def validate_research_method_hardening_artifact(
    *, hardening_id: str, output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR
) -> dict[str, Any]:
    root = output_dir / hardening_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "research_method_hardening_input_snapshot.json")
        errors = _validate_hardening_snapshot(snapshot)
        views, payload = _hardening_views(snapshot, hardening_id=hardening_id, output_dir=root)
        drift = _view_errors(root, views)
        decision = payload["hardening_decision"]
        checks.extend(
            [
                _check(
                    "snapshot_live_sources_lineage_and_chronology", not errors, "; ".join(errors)
                ),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "historical_not_investment_conclusion",
                    decision.get("workflow_pass_is_not_investment_conclusion") is True,
                    "",
                ),
                _check("production_effect_none", decision.get("production_effect") == "none", ""),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("research_method_hardening_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_research_method_hardening_validation",
        hardening_id,
        checks,
        artifact_id_key="hardening_id",
    )
