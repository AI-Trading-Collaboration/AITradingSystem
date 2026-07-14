from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as history
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_risk_capped as risk_capped
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_evidence as evidence
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_method as method
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
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

SMOOTHED_EVIDENCE_GAP_SNAPSHOT_SCHEMA = "smoothed_evidence_gap_input_snapshot.v2"
SMOOTHED_CHURN_BACKFILL_SNAPSHOT_SCHEMA = "smoothed_churn_backfill_input_snapshot.v2"
SIDEWAYS_MIXED_ATTRIBUTION_SNAPSHOT_SCHEMA = "sideways_mixed_attribution_input_snapshot.v2"
SMOOTHED_READINESS_SCORECARD_SNAPSHOT_SCHEMA = "smoothed_readiness_scorecard_input_snapshot.v2"
SMOOTHED_OWNER_REVIEW_UPDATE_SNAPSHOT_SCHEMA = "smoothed_owner_review_update_input_snapshot.v2"

DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH = method.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR = legacy.DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR
DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR = legacy.DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR
DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR = legacy.DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR
DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR = legacy.DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR
DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR = legacy.DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR
DEFAULT_SMOOTHING_BENEFIT_LAG_DIR = evidence.DEFAULT_SMOOTHING_BENEFIT_LAG_DIR
DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR = evidence.DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR
DEFAULT_SMOOTHED_WATCH_PACK_DIR = evidence.DEFAULT_SMOOTHED_WATCH_PACK_DIR
DEFAULT_SMOOTHED_BACKFILL_DIR = method.DEFAULT_SMOOTHED_BACKFILL_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_RISK_CAPPED_BACKFILL_DIR = risk_capped.DEFAULT_RISK_CAPPED_BACKFILL_DIR
DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR = evidence.DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR
DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR = evidence.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR
SMOOTHED_METHOD_TO_VARIANT = method.SMOOTHED_METHOD_TO_VARIANT
SYSTEM_TARGET_SAFETY = method.SYSTEM_TARGET_SAFETY
_SCORE_COMPONENTS = (
    "return_preservation_score",
    "drawdown_score",
    "turnover_score",
    "weight_jump_score",
    "signal_churn_score",
    "sideways_score",
    "recovery_lag_score",
    "forward_confirmation_score",
)


class DynamicV3SmoothedReadinessError(ValueError):
    """Raised when readiness evidence is ambiguous or not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedReadinessError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedReadinessError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _nullable_float(value: Any) -> float | None:
    return float(value) if _finite(value) else None


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


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, data in views.items() if not _file_bytes_match(root / name, data)]


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _policy_binding(path: Path) -> dict[str, Any]:
    return target_core._config_binding(path, kind="smoothed_readiness_policy")


def _numeric_mapping(
    value: Any,
    *,
    name: str,
    expected: set[str] | None = None,
) -> dict[str, float]:
    raw = _mapping(value)
    if expected is not None:
        _require(set(raw) == expected, f"{name} fields must be exact")
    _require(bool(raw), f"{name} must not be empty")
    _require(all(_finite(item) for item in raw.values()), f"{name} values must be finite")
    result = {str(key): float(item) for key, item in raw.items()}
    _require(all(0.0 <= item <= 1.0 for item in result.values()), f"{name} values invalid")
    return result


def _readiness_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    target_core._policy_metadata(
        {"policy_metadata": _mapping(config.get("readiness_policy_metadata"))},
        name="smoothed readiness",
    )
    raw = _mapping(config.get("readiness_policy"))
    required = {
        "minimum_window_observations",
        "weight_jump_event_threshold",
        "weight_jump_high_severity_threshold",
        "score_weights",
        "status_scores",
        "delta_scores",
        "lag_status_scores",
        "forward_status_scores",
        "promote_review_score",
        "continue_observation_score",
        "hard_block_values",
        "candidate_required_for_promotion",
        "missing_component_score",
    }
    _require(set(raw) == required, "smoothed readiness_policy fields must be exact")
    minimum = raw.get("minimum_window_observations")
    _require(
        _finite(minimum) and float(minimum).is_integer() and float(minimum) >= 1.0,
        "minimum_window_observations must be a positive integer",
    )
    jump = raw.get("weight_jump_event_threshold")
    severe = raw.get("weight_jump_high_severity_threshold")
    promote = raw.get("promote_review_score")
    observe = raw.get("continue_observation_score")
    _require(
        all(_finite(item) for item in (jump, severe, promote, observe)),
        "readiness thresholds must be finite",
    )
    _require(0.0 < float(jump) < float(severe) <= 2.0, "weight jump thresholds invalid")
    _require(0.0 <= float(observe) < float(promote) <= 1.0, "readiness score gates invalid")
    weights = _numeric_mapping(
        raw.get("score_weights"), name="score_weights", expected=set(_SCORE_COMPONENTS)
    )
    _require(abs(sum(weights.values()) - 1.0) <= 1e-12, "score_weights must sum to one")
    statuses = _numeric_mapping(raw.get("status_scores"), name="status_scores")
    _require(
        "INSUFFICIENT_DATA" not in statuses and "INSUFFICIENT_EVIDENCE" not in statuses,
        "missing evidence must not receive a status score",
    )
    deltas = _numeric_mapping(
        raw.get("delta_scores"),
        name="delta_scores",
        expected={"positive", "neutral", "negative"},
    )
    lag = _numeric_mapping(
        raw.get("lag_status_scores"),
        name="lag_status_scores",
        expected={"LOW", "MEDIUM", "HIGH"},
    )
    forward = _numeric_mapping(
        raw.get("forward_status_scores"),
        name="forward_status_scores",
        expected={"PASS", "IN_PROGRESS", "WATCH_ONLY", "FAILED"},
    )
    _require("NOT_REGISTERED" not in forward, "NOT_REGISTERED must not receive a score")
    hard_blocks = _mapping(raw.get("hard_block_values"))
    _require(
        set(hard_blocks)
        == {
            "return_preservation",
            "recovery_lag",
            "sideways",
            "forward_confirmation",
        },
        "hard_block_values fields must be exact",
    )
    normalized_blocks: dict[str, list[str]] = {}
    for key, value in hard_blocks.items():
        values = list(value) if isinstance(value, list) else []
        _require(
            values and all(isinstance(item, str) and item for item in values),
            f"{key} hard blocks invalid",
        )
        normalized_blocks[key] = values
    _require(
        raw.get("candidate_required_for_promotion") is True, "candidate promotion gate must be true"
    )
    _require(raw.get("missing_component_score") is None, "missing_component_score must be null")
    return {
        "minimum_window_observations": int(minimum),
        "weight_jump_event_threshold": float(jump),
        "weight_jump_high_severity_threshold": float(severe),
        "score_weights": weights,
        "status_scores": statuses,
        "delta_scores": deltas,
        "lag_status_scores": lag,
        "forward_status_scores": forward,
        "promote_review_score": float(promote),
        "continue_observation_score": float(observe),
        "hard_block_values": normalized_blocks,
        "candidate_required_for_promotion": True,
        "missing_component_score": None,
    }


def _policy(snapshot: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = _mapping(snapshot.get("policy_binding"))
    config = _mapping(binding.get("payload"))
    return config, _readiness_policy(config)


def _validate_policy_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = target_core._validate_config_binding(binding)
    try:
        _require(binding.get("kind") == "smoothed_readiness_policy", "policy binding kind invalid")
        config = _mapping(binding.get("payload"))
        method._evaluation_policy(config)
        evidence._evidence_policy(config)
        _readiness_policy(config)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return evidence._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        root=root,
        validator=validator,
        validator_key=validator_key,
        json_views=json_views,
        jsonl_views=jsonl_views,
        text_views=text_views,
    )


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> list[str]:
    return method._validate_custom_binding(
        binding,
        kind=kind,
        validator=validator,
        validator_key=validator_key,
    )


def _watch_binding(watch_pack_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_watch_pack",
        artifact_id=watch_pack_id,
        root=root,
        validator=evidence.validate_smoothed_watch_pack_artifact,
        validator_key="watch_pack_id",
        json_views=(
            "smoothed_watch_input_snapshot.json",
            "smoothed_watch_manifest.json",
            "smoothed_watch_summary.json",
        ),
        text_views=(
            "owner_smoothed_watch_checklist.md",
            "smoothed_watch_pack_report.md",
            "reader_brief_section.md",
        ),
    )


def _watch_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_watch_manifest.json"),
        "smoothed_watch_summary": _bundle_json(binding, "smoothed_watch_summary.json"),
    }


def _method_row(payload: Mapping[str, Any], key: str, method_name: str) -> dict[str, Any]:
    rows = [
        row
        for row in _records(_mapping(payload.get(key)).get("methods"))
        if row.get("method") == method_name
    ]
    _require(len(rows) == 1, f"{key} must contain one row for {method_name}")
    return rows[0]


def _gap_matrix(
    benefit: Mapping[str, Any],
    regime: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    candidate = watch_summary.get("candidate_method")
    _require(
        candidate is None or candidate in SMOOTHED_METHOD_TO_VARIANT, "watch candidate invalid"
    )
    rows: list[dict[str, Any]] = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        benefit_row = _method_row(benefit, "smoothing_benefit_summary", method_name)
        sideways = _method_row(regime, "sideways_validation_summary", method_name)
        recovery = _method_row(regime, "recovery_lag_validation_summary", method_name)
        for evidence_type, value in (
            ("weight_jump_reduction", benefit_row.get("weight_jump_reduction")),
            ("signal_churn_reduction", benefit_row.get("signal_churn_reduction")),
        ):
            rows.append(
                {
                    "method": method_name,
                    "evidence_type": evidence_type,
                    "status": "AVAILABLE" if _finite(value) else "MISSING",
                    "blocking": candidate == method_name and not _finite(value),
                    "current_value": _nullable_float(value),
                    "reason": "source_metric_available"
                    if _finite(value)
                    else f"missing_{evidence_type}",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
        for evidence_type, source, status_key in (
            ("sideways_choppy_samples", sideways, "sideways_status"),
            ("strong_recovery_lag_cost", recovery, "lag_status"),
        ):
            status = _text(source.get(status_key), "INSUFFICIENT_DATA")
            sample = int(source["sample_count"]) if _finite(source.get("sample_count")) else None
            available = sample is not None and status != "INSUFFICIENT_DATA"
            rows.append(
                {
                    "method": method_name,
                    "evidence_type": evidence_type,
                    "status": "AVAILABLE" if available else "MISSING",
                    "blocking": candidate == method_name and not available,
                    "available_samples": sample,
                    "current_status": status,
                    "reason": "source_samples_available"
                    if available
                    else f"missing_{evidence_type}",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    if candidate is None:
        rows.extend(
            [
                {
                    "method": None,
                    "evidence_type": "eligible_candidate",
                    "status": "MISSING",
                    "blocking": True,
                    "reason": "no_evidence_backed_candidate",
                    **SYSTEM_TARGET_SAFETY,
                },
                {
                    "method": None,
                    "evidence_type": "forward_confirmation",
                    "status": "MISSING",
                    "blocking": True,
                    "reason": "forward_target_not_registered_without_candidate",
                    **SYSTEM_TARGET_SAFETY,
                },
            ]
        )
    return {
        "candidate_method": candidate,
        "secondary_method": watch_summary.get("secondary_method"),
        "current_tradeoff_status": watch_summary.get("benefit_lag_tradeoff"),
        "forward_confirmation_status": watch_summary.get("forward_confirmation_status"),
        "missing_evidence": rows,
        **SYSTEM_TARGET_SAFETY,
    }


def _gap_reason_summary(gap_id: str, matrix: Mapping[str, Any]) -> dict[str, Any]:
    missing = [
        row for row in _records(matrix.get("missing_evidence")) if row.get("status") != "AVAILABLE"
    ]
    reasons = [
        {
            "method": row.get("method"),
            "evidence_type": row.get("evidence_type"),
            "reason": row.get("reason"),
            "severity": "HIGH" if row.get("blocking") is True else "MEDIUM",
            "blocking": row.get("blocking") is True,
            "recommended_action": (
                "request_evidence_backed_candidate"
                if row.get("evidence_type") == "eligible_candidate"
                else "wait_for_candidate_before_forward_registration"
                if row.get("evidence_type") == "forward_confirmation"
                else "backfill_missing_source_metric"
            ),
            **SYSTEM_TARGET_SAFETY,
        }
        for row in missing
    ]
    return {
        "gap_id": gap_id,
        "candidate_method": matrix.get("candidate_method"),
        "primary_gap_reasons": reasons,
        "tradeoff_can_be_resolved_by_backfill": any(
            row.get("evidence_type") in {"weight_jump_reduction", "signal_churn_reduction"}
            for row in missing
        ),
        "requires_forward_data": any(
            row.get("evidence_type") == "forward_confirmation" for row in missing
        ),
        "requires_new_target_method": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _gap_plan(matrix: Mapping[str, Any]) -> dict[str, Any]:
    missing = [
        row for row in _records(matrix.get("missing_evidence")) if row.get("status") != "AVAILABLE"
    ]
    rows = [
        {
            "method": row.get("method"),
            "metric_family": row.get("evidence_type"),
            "required_for": "candidate_eligibility"
            if matrix.get("candidate_method") is None
            else "candidate_readiness",
            "priority": "HIGH" if row.get("blocking") is True else "MEDIUM",
            **SYSTEM_TARGET_SAFETY,
        }
        for row in missing
    ]
    return {
        "candidate_method": matrix.get("candidate_method"),
        "required_backfills": rows,
        "next_action": "request_evidence_backed_candidate"
        if matrix.get("candidate_method") is None
        else "complete_candidate_evidence",
        **SYSTEM_TARGET_SAFETY,
    }


def _render_gap(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    reason: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Evidence Gap {manifest.get('gap_id')}",
            "",
            f"- candidate_method: {matrix.get('candidate_method')}",
            f"- current_tradeoff_status: {matrix.get('current_tradeoff_status')}",
            f"- forward_confirmation_status: {matrix.get('forward_confirmation_status')}",
            "- tradeoff_can_be_resolved_by_backfill: "
            f"{reason.get('tradeoff_can_be_resolved_by_backfill')}",
            f"- requires_forward_data: {reason.get('requires_forward_data')}",
            f"- next_action: {plan.get('next_action')}",
            "- candidate_role_fixed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Evidence Matrix",
            "",
            *[
                f"- {row.get('method')} / {row.get('evidence_type')}: "
                f"status={row.get('status')}, blocking={row.get('blocking')}, "
                f"value={row.get('current_value', row.get('available_samples'))}"
                for row in _records(matrix.get("missing_evidence"))
            ],
            "",
            "工作流 PASS 只证明该诊断可重现，不代表存在可晋级候选。"
            "缺失证据保持 null，且不得由 readiness score 补造候选。",
            "",
        ]
    )


def _gap_views(
    snapshot: Mapping[str, Any], *, gap_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    benefit = evidence._simple_payload(
        _mapping(snapshot.get("benefit_lag_source")),
        "smoothing_benefit_lag_manifest.json",
        {
            "smoothing_benefit_summary": "smoothing_benefit_summary.json",
            "lag_cost_summary": "lag_cost_summary.json",
            "benefit_lag_tradeoff_matrix": "benefit_lag_tradeoff_matrix.json",
        },
    )
    regime = evidence._regime_payload(_mapping(snapshot.get("regime_source")))
    watch = _watch_payload(_mapping(snapshot.get("watch_source")))
    matrix = _gap_matrix(benefit, regime, watch)
    reason = _gap_reason_summary(gap_id, matrix)
    plan = _gap_plan(matrix)
    lineage = {
        "review_id": watch.get("review_id"),
        "comparison_id": watch.get("comparison_id"),
        "smoothed_backfill_id": watch.get("smoothed_backfill_id"),
        "baseline_backfill_id": watch.get("baseline_backfill_id"),
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_evidence_gap_manifest",
        "gap_id": gap_id,
        "benefit_lag_id": benefit.get("drilldown_id"),
        "regime_validation_id": regime.get("regime_validation_id"),
        "watch_pack_id": watch.get("watch_pack_id"),
        **lineage,
        "candidate_method": matrix.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": regime.get("market_regime"),
        "date_start": regime.get("date_start"),
        "date_end": regime.get("date_end"),
        "smoothed_evidence_gap_input_snapshot_path": str(
            root / "smoothed_evidence_gap_input_snapshot.json"
        ),
        "smoothed_evidence_gap_manifest_path": str(root / "smoothed_evidence_gap_manifest.json"),
        "missing_evidence_matrix_path": str(root / "missing_evidence_matrix.json"),
        "evidence_gap_reason_summary_path": str(root / "evidence_gap_reason_summary.json"),
        "required_metric_backfill_plan_path": str(root / "required_metric_backfill_plan.json"),
        "smoothed_evidence_gap_report_path": str(root / "smoothed_evidence_gap_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_gap(manifest, matrix, reason, plan)
    views = {
        "smoothed_evidence_gap_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_evidence_gap_manifest.json": _json_bytes(manifest),
        "missing_evidence_matrix.json": _json_bytes(matrix),
        "evidence_gap_reason_summary.json": _json_bytes(reason),
        "required_metric_backfill_plan.json": _json_bytes(plan),
        "smoothed_evidence_gap_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "missing_evidence_matrix": matrix,
        "evidence_gap_reason_summary": reason,
        "required_metric_backfill_plan": plan,
    }


def _validate_gap_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_EVIDENCE_GAP_SNAPSHOT_SCHEMA,
            "gap snapshot schema invalid",
        )
        specs = (
            (
                "benefit_lag_source",
                "smoothing_benefit_lag",
                evidence.validate_smoothing_benefit_lag_artifact,
                "drilldown_id",
                "smoothing_benefit_lag_manifest.json",
            ),
            (
                "regime_source",
                "smoothed_regime_validation",
                evidence.validate_smoothed_regime_validation_artifact,
                "regime_validation_id",
                "smoothed_regime_validation_manifest.json",
            ),
            (
                "watch_source",
                "smoothed_watch_pack",
                evidence.validate_smoothed_watch_pack_artifact,
                "watch_pack_id",
                "smoothed_watch_manifest.json",
            ),
        )
        manifests = []
        for field, kind, validator, key, manifest_name in specs:
            binding = _mapping(snapshot.get(field))
            errors.extend(
                _validate_binding(binding, kind=kind, validator=validator, validator_key=key)
            )
            manifests.append(_bundle_json(binding, manifest_name))
        generated = target_core._datetime(snapshot.get("generated_at"), field="gap generated_at")
        evidence._chronology(generated, *manifests)
        benefit, regime, watch = manifests
        for field in ("smoothed_backfill_id", "baseline_backfill_id"):
            _require(
                benefit.get(field) == regime.get(field) == watch.get(field), f"gap {field} mismatch"
            )
        _require(
            benefit.get("comparison_id") == watch.get("comparison_id"), "gap comparison_id mismatch"
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_evidence_gap_diagnosis(
    *,
    benefit_lag_id: str,
    regime_validation_id: str,
    watch_pack_id: str,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_EVIDENCE_GAP_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "benefit_lag_source": evidence._benefit_binding(benefit_lag_id, benefit_lag_dir),
        "regime_source": evidence._regime_binding(regime_validation_id, regime_validation_dir),
        "watch_source": _watch_binding(watch_pack_id, watch_pack_dir),
        "production_effect": "none",
    }
    errors = _validate_gap_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-evidence-gap", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _gap_views(snapshot, gap_id=root.name, root=root)
    _write(root, views, "latest_smoothed_evidence_gap", "smoothed_evidence_gap_manifest.json")
    return {"gap_id": root.name, "gap_dir": root, **payload}


def smoothed_evidence_gap_report_payload(
    *,
    gap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=gap_id if not latest else None,
        pointer_name="latest_smoothed_evidence_gap",
    )
    return {
        **_read_json(root / "smoothed_evidence_gap_manifest.json"),
        "missing_evidence_matrix": _read_json(root / "missing_evidence_matrix.json"),
        "evidence_gap_reason_summary": _read_json(root / "evidence_gap_reason_summary.json"),
        "required_metric_backfill_plan": _read_json(root / "required_metric_backfill_plan.json"),
        "input_snapshot": _read_json(root / "smoothed_evidence_gap_input_snapshot.json"),
        "gap_dir": str(root),
    }


def validate_smoothed_evidence_gap_artifact(
    *, gap_id: str, output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR
) -> dict[str, Any]:
    root = output_dir / gap_id
    snapshot = legacy._read_optional_json(root / "smoothed_evidence_gap_input_snapshot.json") or {}
    errors = _validate_gap_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _gap_views(snapshot, gap_id=gap_id, root=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_evidence_gap_validation",
        gap_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="gap_id",
    )


def _churn_binding(churn_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_churn_backfill",
        artifact_id=churn_id,
        root=root,
        validator=validate_smoothed_churn_backfill_artifact,
        validator_key="churn_id",
        json_views=(
            "smoothed_churn_backfill_input_snapshot.json",
            "smoothed_churn_manifest.json",
            "churn_reduction_summary.json",
        ),
        jsonl_views=(
            "churn_metrics_by_method.jsonl",
            "weight_jump_events.jsonl",
            "direction_flip_events.jsonl",
        ),
        text_views=("smoothed_churn_backfill_report.md",),
    )


def _ledger_delta(row: Mapping[str, Any], method_name: str) -> tuple[float, str]:
    turnover = row.get("turnover")
    _require(_finite(turnover) and float(turnover) >= 0.0, f"{method_name} ledger turnover invalid")
    deltas = _mapping(row.get("deltas"))
    if not deltas:
        _require(float(turnover) == 0.0, f"{method_name} missing deltas for nonzero turnover")
        return 0.0, "explicit_zero_turnover"
    _require(
        all(_finite(value) for value in deltas.values()), f"{method_name} ledger deltas invalid"
    )
    total = sum(abs(float(value)) for value in deltas.values())
    _require(
        abs(total / 2.0 - float(turnover)) <= 1e-8,
        f"{method_name} turnover/delta identity mismatch",
    )
    return round(total, 10), "explicit_deltas"


def _method_rows(rows: Sequence[Mapping[str, Any]], method_name: str) -> list[dict[str, Any]]:
    selected = [dict(row) for row in rows if row.get("target_method") == method_name]
    keys = [_text(row.get("date")) for row in selected]
    _require(
        all(keys) and len(keys) == len(set(keys)), f"{method_name} method/date identity invalid"
    )
    for key in keys:
        target_core._date(key, field=f"{method_name} date")
    return sorted(selected, key=lambda row: _text(row.get("date")))


def _direction_flip_rows(
    ledger: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    method_name: str,
) -> list[dict[str, Any]]:
    previous: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for row in ledger:
        day = _text(row.get("date"))
        for symbol, raw in _mapping(row.get("deltas")).items():
            _require(_finite(raw), f"{method_name} delta invalid")
            value = float(raw)
            if value == 0.0:
                continue
            direction = "increase" if value > 0.0 else "decrease"
            if symbol in previous and previous[symbol] != direction:
                rows.append(
                    {
                        "date": day,
                        "method": method_name,
                        "symbol": symbol,
                        "previous_direction": previous[symbol],
                        "current_direction": direction,
                        "flip_type": "semiconductor_flip"
                        if symbol in {"SMH", "SOXX"}
                        else "cash_flip"
                        if symbol == "CASH"
                        else "risk_asset_flip",
                        "regime_context": labels.get(day, "unknown"),
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            previous[symbol] = direction
    return rows


def _metric_delta(value: Any, reference: Any) -> float | int | None:
    if not (_finite(value) and _finite(reference)):
        return None
    result = float(value) - float(reference)
    return (
        int(result)
        if result.is_integer() and isinstance(value, int) and isinstance(reference, int)
        else round(result, 10)
    )


def _churn_payloads(
    snapshot: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]
]:
    smoothed_source = _mapping(snapshot.get("smoothed_backfill_source"))
    baseline_source = _mapping(snapshot.get("baseline_backfill_source"))
    risk_source = _mapping(snapshot.get("risk_capped_backfill_source"))
    smoothed_manifest = _bundle_json(smoothed_source, "smoothed_backfill_manifest.json")
    baseline_manifest = _bundle_json(baseline_source, "paper_shadow_backfill_manifest.json")
    smoothed_states = _bundle_jsonl(smoothed_source, "smoothed_method_states.jsonl")
    baseline_states = _bundle_jsonl(baseline_source, "backfill_method_states.jsonl")
    risk_states = _bundle_jsonl(risk_source, "risk_capped_method_states.jsonl")
    ledgers = {
        **{
            method_name: _method_rows(
                _bundle_jsonl(smoothed_source, "smoothed_trade_ledger.jsonl"), method_name
            )
            for method_name in SMOOTHED_METHOD_TO_VARIANT
        },
        "limited_adjustment": _method_rows(
            _bundle_jsonl(baseline_source, "backfill_trade_ledger.jsonl"), "limited_adjustment"
        ),
        "static_baseline": _method_rows(
            _bundle_jsonl(baseline_source, "backfill_trade_ledger.jsonl"), "static_baseline"
        ),
        "risk_capped_limited_adjustment": _method_rows(
            _bundle_jsonl(risk_source, "risk_capped_trade_ledger.jsonl"),
            "risk_capped_limited_adjustment",
        ),
    }
    states = {
        **{name: _method_rows(smoothed_states, name) for name in SMOOTHED_METHOD_TO_VARIANT},
        "limited_adjustment": _method_rows(baseline_states, "limited_adjustment"),
        "static_baseline": _method_rows(baseline_states, "static_baseline"),
        "risk_capped_limited_adjustment": _method_rows(
            risk_states, "risk_capped_limited_adjustment"
        ),
    }
    baseline_snapshot = _bundle_json(baseline_source, "paper_shadow_backfill_input_snapshot.json")
    baseline_config = _mapping(_mapping(baseline_snapshot.get("config_binding")).get("payload"))
    _require(bool(baseline_config), "baseline config commitment missing")
    labels = legacy._regime_labels_from_states(
        [*baseline_states, *smoothed_states, *risk_states], baseline_config
    )
    raw_metrics: list[dict[str, Any]] = []
    jump_events: list[dict[str, Any]] = []
    flip_events: list[dict[str, Any]] = []
    for method_name, ledger in ledgers.items():
        deltas = [_ledger_delta(row, method_name) for row in ledger]
        totals = [value for value, _source in deltas]
        sources = sorted({source for _value, source in deltas})
        flips = _direction_flip_rows(ledger, labels, method_name)
        flip_events.extend(flips)
        for row, (total, source) in zip(ledger, deltas, strict=True):
            if total < float(policy["weight_jump_event_threshold"]):
                continue
            values = [
                (symbol, float(value)) for symbol, value in _mapping(row.get("deltas")).items()
            ]
            symbol, value = max(values, key=lambda item: abs(item[1]), default=(None, None))
            jump_events.append(
                {
                    "date": row.get("date"),
                    "method": method_name,
                    "total_abs_weight_change": total,
                    "delta_source": source,
                    "largest_symbol_delta": {"symbol": symbol, "delta": value},
                    "jump_threshold": policy["weight_jump_event_threshold"],
                    "regime_context": labels.get(_text(row.get("date")), "unknown"),
                    "severity": "HIGH"
                    if total >= float(policy["weight_jump_high_severity_threshold"])
                    else "MEDIUM",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
        turnover_values = [row.get("turnover") for row in ledger]
        _require(
            all(_finite(value) for value in turnover_values),
            f"{method_name} turnover observations invalid",
        )
        complete = bool(ledger)
        jump_count = (
            sum(1 for value in totals if value >= float(policy["weight_jump_event_threshold"]))
            if complete
            else None
        )
        flip_count = len(flips) if complete else None
        turnover = round(sum(float(value) for value in turnover_values), 10) if complete else None
        raw_metrics.append(
            {
                "method": method_name,
                "state_observation_count": len(states[method_name]),
                "ledger_observation_count": len(ledger),
                "date_start": states[method_name][0].get("date") if states[method_name] else None,
                "date_end": states[method_name][-1].get("date") if states[method_name] else None,
                "metric_status": "PASS" if complete else "INSUFFICIENT_DATA",
                "delta_sources": sources,
                "avg_total_abs_weight_change": round(sum(totals) / len(totals), 10)
                if complete
                else None,
                "max_total_abs_weight_change": round(max(totals), 10) if complete else None,
                "weight_jump_count": jump_count,
                "direction_flip_count": flip_count,
                "risk_asset_direction_flip_count": sum(
                    row.get("flip_type") == "risk_asset_flip" for row in flips
                )
                if complete
                else None,
                "semiconductor_direction_flip_count": sum(
                    row.get("flip_type") == "semiconductor_flip" for row in flips
                )
                if complete
                else None,
                "turnover": turnover,
                "signal_churn_event_count": jump_count + flip_count
                if jump_count is not None and flip_count is not None
                else None,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    limited = next(row for row in raw_metrics if row.get("method") == "limited_adjustment")
    metrics: list[dict[str, Any]] = []
    for row in raw_metrics:
        relative = {
            "weight_jump_delta": _metric_delta(
                row.get("weight_jump_count"), limited.get("weight_jump_count")
            ),
            "direction_flip_delta": _metric_delta(
                row.get("direction_flip_count"), limited.get("direction_flip_count")
            ),
            "turnover_delta": _metric_delta(row.get("turnover"), limited.get("turnover")),
            "signal_churn_event_delta": _metric_delta(
                row.get("signal_churn_event_count"), limited.get("signal_churn_event_count")
            ),
        }
        complete = all(value is not None for value in relative.values())
        values = [float(value) for value in relative.values() if value is not None]
        status = (
            "INSUFFICIENT_DATA"
            if not complete
            else "IMPROVED"
            if all(value <= 0.0 for value in values) and any(value < 0.0 for value in values)
            else "WORSE"
            if all(value >= 0.0 for value in values) and any(value > 0.0 for value in values)
            else "MIXED"
        )
        metrics.append({**row, "relative_to_limited": relative, "churn_status": status})
    reductions = []
    for row in metrics:
        if row.get("method") not in SMOOTHED_METHOD_TO_VARIANT:
            continue
        relative = _mapping(row.get("relative_to_limited"))
        values = {
            key: (-float(value) if value is not None else None) for key, value in relative.items()
        }
        complete = all(value is not None for value in values.values())
        positive = sum(float(value) > 0.0 for value in values.values() if value is not None)
        status = (
            "INSUFFICIENT_DATA"
            if not complete
            else "STRONG"
            if positive >= 3
            else "MODERATE"
            if positive >= 2
            else "WEAK"
            if positive == 1
            else "NONE"
        )
        reductions.append(
            {
                "method": row.get("method"),
                "weight_jump_reduction_vs_limited": values["weight_jump_delta"],
                "direction_flip_reduction_vs_limited": values["direction_flip_delta"],
                "turnover_reduction_vs_limited": values["turnover_delta"],
                "signal_churn_reduction_vs_limited": values["signal_churn_event_delta"],
                "churn_reduction_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    ranked = [
        row
        for row in reductions
        if _finite(row.get("signal_churn_reduction_vs_limited"))
        and _finite(row.get("turnover_reduction_vs_limited"))
    ]
    ranked.sort(
        key=lambda row: (
            float(row["signal_churn_reduction_vs_limited"]),
            float(row["turnover_reduction_vs_limited"]),
        ),
        reverse=True,
    )
    best = (
        ranked[0].get("method")
        if ranked
        and (
            len(ranked) == 1
            or (
                ranked[0]["signal_churn_reduction_vs_limited"],
                ranked[0]["turnover_reduction_vs_limited"],
            )
            != (
                ranked[1]["signal_churn_reduction_vs_limited"],
                ranked[1]["turnover_reduction_vs_limited"],
            )
        )
        else None
    )
    summary = {
        "methods": reductions,
        "best_churn_reduction_method": best,
        "diagnostic_best_method_only": True,
        "candidate_role_fixed": False,
        **SYSTEM_TARGET_SAFETY,
    }
    lineage = {
        "smoothed_backfill_id": smoothed_manifest.get("smoothed_backfill_id"),
        "baseline_backfill_id": baseline_manifest.get("backfill_id"),
        "risk_capped_backfill_id": _bundle_json(
            risk_source, "risk_capped_backfill_manifest.json"
        ).get("risk_capped_backfill_id"),
    }
    return metrics, jump_events, flip_events, summary, lineage


def _render_churn(
    manifest: Mapping[str, Any], metrics: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Churn Backfill {manifest.get('churn_id')}",
            "",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- best_churn_reduction_method: {summary.get('best_churn_reduction_method')}",
            "- candidate_role_fixed: false",
            "- delta_fallback_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Methods",
            "",
            *[
                f"- {row.get('method')}: metric_status={row.get('metric_status')}, "
                f"weight_jumps={row.get('weight_jump_count')}, "
                f"direction_flips={row.get('direction_flip_count')}, "
                f"turnover={row.get('turnover')}, churn_status={row.get('churn_status')}"
                for row in metrics
            ],
            "",
            "total_abs_weight_change 只接受显式 deltas，或 turnover=0 的显式零交易；"
            "不会用缺失值或任意乘数补造 churn 证据。",
            "",
        ]
    )


def _churn_views(
    snapshot: Mapping[str, Any], *, churn_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    metrics, jumps, flips, summary, lineage = _churn_payloads(snapshot, policy)
    baseline_manifest = _bundle_json(
        _mapping(snapshot.get("baseline_backfill_source")), "paper_shadow_backfill_manifest.json"
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_churn_manifest",
        "churn_id": churn_id,
        **lineage,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": baseline_manifest.get("market_regime"),
        "date_start": baseline_manifest.get("date_start"),
        "date_end": baseline_manifest.get("date_end"),
        "data_quality_status": baseline_manifest.get("data_quality_status"),
        "jump_threshold": policy["weight_jump_event_threshold"],
        "policy_id": _mapping(config.get("readiness_policy_metadata")).get("policy_id"),
        "smoothed_churn_backfill_input_snapshot_path": str(
            root / "smoothed_churn_backfill_input_snapshot.json"
        ),
        "smoothed_churn_manifest_path": str(root / "smoothed_churn_manifest.json"),
        "churn_metrics_by_method_path": str(root / "churn_metrics_by_method.jsonl"),
        "weight_jump_events_path": str(root / "weight_jump_events.jsonl"),
        "direction_flip_events_path": str(root / "direction_flip_events.jsonl"),
        "churn_reduction_summary_path": str(root / "churn_reduction_summary.json"),
        "smoothed_churn_backfill_report_path": str(root / "smoothed_churn_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_churn(manifest, metrics, summary)
    views = {
        "smoothed_churn_backfill_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_churn_manifest.json": _json_bytes(manifest),
        "churn_metrics_by_method.jsonl": _jsonl_bytes(metrics),
        "weight_jump_events.jsonl": _jsonl_bytes(jumps),
        "direction_flip_events.jsonl": _jsonl_bytes(flips),
        "churn_reduction_summary.json": _json_bytes(summary),
        "smoothed_churn_backfill_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "churn_metrics_by_method": metrics,
        "weight_jump_events": jumps,
        "direction_flip_events": flips,
        "churn_reduction_summary": summary,
    }


def _validate_churn_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_CHURN_BACKFILL_SNAPSHOT_SCHEMA,
            "churn snapshot schema invalid",
        )
        smoothed = _mapping(snapshot.get("smoothed_backfill_source"))
        baseline = _mapping(snapshot.get("baseline_backfill_source"))
        risk = _mapping(snapshot.get("risk_capped_backfill_source"))
        errors.extend(method._validate_smoothed_backfill_binding(smoothed))
        errors.extend(history._validate_history_binding(baseline))
        errors.extend(risk_capped._validate_risk_backfill_binding(risk))
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        manifests = (
            _bundle_json(smoothed, "smoothed_backfill_manifest.json"),
            _bundle_json(baseline, "paper_shadow_backfill_manifest.json"),
            _bundle_json(risk, "risk_capped_backfill_manifest.json"),
        )
        smoothed_manifest, baseline_manifest, risk_manifest = manifests
        baseline_id = baseline.get("artifact_id")
        _require(
            smoothed_manifest.get("source_paper_shadow_backfill_id")
            == baseline_id
            == risk_manifest.get("source_paper_shadow_backfill_id"),
            "churn backfill lineage mismatch",
        )
        ranges = {(row.get("date_start"), row.get("date_end")) for row in manifests}
        _require(len(ranges) == 1, "churn date ranges mismatch")
        generated = target_core._datetime(snapshot.get("generated_at"), field="churn generated_at")
        evidence._chronology(generated, *manifests)
        _churn_payloads(
            snapshot,
            _readiness_policy(_mapping(_mapping(snapshot.get("policy_binding")).get("payload"))),
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_churn_backfill(
    *,
    smoothed_backfill_id: str,
    baseline_backfill_id: str,
    risk_capped_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_CHURN_BACKFILL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "smoothed_backfill_source": method._smoothed_backfill_binding(
            smoothed_backfill_id, smoothed_backfill_dir
        ),
        "baseline_backfill_source": history._backfill_binding(
            baseline_backfill_id, baseline_backfill_dir
        ),
        "risk_capped_backfill_source": risk_capped._risk_backfill_binding(
            risk_capped_backfill_id, risk_capped_backfill_dir
        ),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_churn_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-churn-backfill", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _churn_views(snapshot, churn_id=root.name, root=root)
    _write(root, views, "latest_smoothed_churn_backfill", "smoothed_churn_manifest.json")
    return {"churn_id": root.name, "churn_dir": root, **payload}


def smoothed_churn_backfill_report_payload(
    *,
    churn_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=churn_id if not latest else None,
        pointer_name="latest_smoothed_churn_backfill",
    )
    return {
        **_read_json(root / "smoothed_churn_manifest.json"),
        "churn_metrics_by_method": _read_jsonl(root / "churn_metrics_by_method.jsonl"),
        "weight_jump_events": _read_jsonl(root / "weight_jump_events.jsonl"),
        "direction_flip_events": _read_jsonl(root / "direction_flip_events.jsonl"),
        "churn_reduction_summary": _read_json(root / "churn_reduction_summary.json"),
        "input_snapshot": _read_json(root / "smoothed_churn_backfill_input_snapshot.json"),
        "churn_dir": str(root),
    }


def validate_smoothed_churn_backfill_artifact(
    *, churn_id: str, output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR
) -> dict[str, Any]:
    root = output_dir / churn_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_churn_backfill_input_snapshot.json") or {}
    )
    errors = _validate_churn_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _churn_views(snapshot, churn_id=churn_id, root=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_churn_backfill_validation",
        churn_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="churn_id",
    )


def _sideways_binding(sideways_attribution_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="sideways_mixed_attribution",
        artifact_id=sideways_attribution_id,
        root=root,
        validator=validate_sideways_mixed_attribution_artifact,
        validator_key="sideways_attribution_id",
        json_views=(
            "sideways_mixed_attribution_input_snapshot.json",
            "sideways_mixed_manifest.json",
            "sideways_mixed_reason_summary.json",
            "sideways_3d_vs_5d_breakdown.json",
        ),
        jsonl_views=("sideways_window_outcomes.jsonl",),
        text_views=("sideways_mixed_attribution_report.md",),
    )


def _sideways_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "sideways_mixed_manifest.json"),
        "sideways_window_outcomes": _bundle_jsonl(binding, "sideways_window_outcomes.jsonl"),
        "sideways_mixed_reason_summary": _bundle_json(
            binding, "sideways_mixed_reason_summary.json"
        ),
        "sideways_3d_vs_5d_breakdown": _bundle_json(binding, "sideways_3d_vs_5d_breakdown.json"),
    }


def _paired_rows(
    left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    left_by_date = {_text(row.get("date")): dict(row) for row in left}
    right_by_date = {_text(row.get("date")): dict(row) for row in right}
    dates = sorted(set(left_by_date) & set(right_by_date))
    return [left_by_date[day] for day in dates], [right_by_date[day] for day in dates]


def _event_delta(
    method_name: str,
    dates: set[str],
    jumps: Sequence[Mapping[str, Any]],
    flips: Sequence[Mapping[str, Any]],
) -> int:
    events = [*jumps, *flips]
    method_count = sum(
        row.get("method") == method_name and _text(row.get("date")) in dates for row in events
    )
    baseline_count = sum(
        row.get("method") == "limited_adjustment" and _text(row.get("date")) in dates
        for row in events
    )
    return method_count - baseline_count


def _sample_metrics(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    payload = legacy._sample_return_metrics(rows, min_sample=1)
    total_return = _nullable_float(payload.get("total_return"))
    drawdown = _nullable_float(payload.get("max_drawdown"))
    _require(
        total_return is not None and drawdown is not None,
        "sideways sample metrics invalid",
    )
    return total_return, drawdown


def _sideways_class(
    *,
    return_delta: float | None,
    drawdown_delta: float | None,
    turnover_delta: float | None,
    churn_delta: int | None,
    evidence_policy: Mapping[str, Any],
) -> str:
    if any(value is None for value in (return_delta, drawdown_delta, turnover_delta, churn_delta)):
        return "INSUFFICIENT_DATA"
    if (
        float(return_delta) >= float(evidence_policy["acceptable_return_delta_floor"])
        and float(drawdown_delta) >= float(evidence_policy["drawdown_improvement_floor"])
        and float(turnover_delta) <= float(evidence_policy["sideways_turnover_delta_ceiling"])
        and float(churn_delta) <= float(evidence_policy["sideways_signal_churn_delta_ceiling"])
    ):
        return "improved"
    if float(return_delta) < float(evidence_policy["acceptable_return_delta_floor"]) and (
        float(drawdown_delta) < float(evidence_policy["drawdown_improvement_floor"])
        or float(churn_delta) > float(evidence_policy["sideways_signal_churn_delta_ceiling"])
    ):
        return "worse"
    if all(
        float(value) == 0.0 for value in (return_delta, drawdown_delta, turnover_delta, churn_delta)
    ):
        return "neutral"
    return "mixed"


def _sideways_reason(row: Mapping[str, Any], evidence_policy: Mapping[str, Any]) -> str:
    if row.get("outcome_class") == "INSUFFICIENT_DATA":
        return "insufficient_window_observations"
    return_delta = float(row["return_delta_vs_limited"])
    drawdown_delta = float(row["drawdown_delta_vs_limited"])
    turnover_delta = float(row["turnover_delta_vs_limited"])
    churn_delta = float(row["churn_delta_vs_limited"])
    if (
        churn_delta < float(evidence_policy["sideways_signal_churn_delta_ceiling"])
        and turnover_delta <= float(evidence_policy["sideways_turnover_delta_ceiling"])
        and return_delta >= float(evidence_policy["acceptable_return_delta_floor"])
    ):
        return "churn_reduction_helped"
    if return_delta < float(
        evidence_policy["acceptable_return_delta_floor"]
    ) and churn_delta <= float(evidence_policy["sideways_signal_churn_delta_ceiling"]):
        return "lag_cost_hurt"
    if (
        drawdown_delta >= float(evidence_policy["drawdown_improvement_floor"])
        and return_delta < 0.0
    ):
        return "drawdown_improved_return_worse"
    return "mixed_evidence"


def _sideways_outputs(
    snapshot: Mapping[str, Any],
    readiness_policy: Mapping[str, Any],
    evidence_policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    regime = evidence._regime_payload(_mapping(snapshot.get("regime_source")))
    churn_binding = _mapping(snapshot.get("churn_source"))
    churn = {
        **_bundle_json(churn_binding, "smoothed_churn_manifest.json"),
        "weight_jump_events": _bundle_jsonl(churn_binding, "weight_jump_events.jsonl"),
        "direction_flip_events": _bundle_jsonl(churn_binding, "direction_flip_events.jsonl"),
    }
    churn_snapshot = _bundle_json(churn_binding, "smoothed_churn_backfill_input_snapshot.json")
    smoothed_source = _mapping(churn_snapshot.get("smoothed_backfill_source"))
    baseline_source = _mapping(churn_snapshot.get("baseline_backfill_source"))
    smoothed_states = _bundle_jsonl(smoothed_source, "smoothed_method_states.jsonl")
    baseline_states = _bundle_jsonl(baseline_source, "backfill_method_states.jsonl")
    baseline_snapshot = _bundle_json(baseline_source, "paper_shadow_backfill_input_snapshot.json")
    baseline_config = _mapping(_mapping(baseline_snapshot.get("config_binding")).get("payload"))
    _require(bool(baseline_config), "sideways baseline config commitment missing")
    labels = legacy._regime_labels_from_states(
        [*baseline_states, *smoothed_states], baseline_config
    )
    sideways_dates = sorted(day for day, label in labels.items() if label == "sideways_choppy")
    grouped: dict[str, set[str]] = {}
    for day in sideways_dates:
        grouped.setdefault(day[:7].replace("-", "_"), set()).add(day)
    minimum = int(readiness_policy["minimum_window_observations"])
    outcomes: list[dict[str, Any]] = []
    limited = _method_rows(baseline_states, "limited_adjustment")
    for suffix, dates in sorted(grouped.items()):
        for method_name in SMOOTHED_METHOD_TO_VARIANT:
            left, right = _paired_rows(
                [
                    row
                    for row in _method_rows(smoothed_states, method_name)
                    if _text(row.get("date")) in dates
                ],
                [row for row in limited if _text(row.get("date")) in dates],
            )
            sample_count = len(left)
            return_delta: float | None = None
            drawdown_delta: float | None = None
            turnover_delta: float | None = None
            churn_delta: int | None = None
            if sample_count >= minimum:
                left_return, left_drawdown = _sample_metrics(left)
                right_return, right_drawdown = _sample_metrics(right)
                turnover_values = [row.get("turnover") for row in [*left, *right]]
                _require(
                    all(_finite(value) for value in turnover_values),
                    "sideways turnover observations invalid",
                )
                return_delta = round(left_return - right_return, 10)
                drawdown_delta = round(left_drawdown - right_drawdown, 10)
                turnover_delta = round(
                    sum(float(row["turnover"]) for row in left)
                    - sum(float(row["turnover"]) for row in right),
                    10,
                )
                churn_delta = _event_delta(
                    method_name,
                    dates,
                    _records(churn.get("weight_jump_events")),
                    _records(churn.get("direction_flip_events")),
                )
            row = {
                "window_id": f"sideways_{suffix}",
                "start_date": min(dates),
                "end_date": max(dates),
                "method": method_name,
                "sample_count": sample_count,
                "evidence_status": ("PASS" if sample_count >= minimum else "INSUFFICIENT_DATA"),
                "return_delta_vs_limited": return_delta,
                "drawdown_delta_vs_limited": drawdown_delta,
                "turnover_delta_vs_limited": turnover_delta,
                "churn_delta_vs_limited": churn_delta,
                **SYSTEM_TARGET_SAFETY,
            }
            row["outcome_class"] = _sideways_class(
                return_delta=return_delta,
                drawdown_delta=drawdown_delta,
                turnover_delta=turnover_delta,
                churn_delta=churn_delta,
                evidence_policy=evidence_policy,
            )
            row["likely_reason"] = _sideways_reason(row, evidence_policy)
            outcomes.append(row)
    breakdown_rows: list[dict[str, Any]] = []
    method_summaries: list[dict[str, Any]] = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        selected = [row for row in outcomes if row.get("method") == method_name]
        complete = [
            row
            for row in selected
            if row.get("evidence_status") == "PASS"
            and all(
                _finite(row.get(field))
                for field in (
                    "return_delta_vs_limited",
                    "drawdown_delta_vs_limited",
                    "turnover_delta_vs_limited",
                    "churn_delta_vs_limited",
                )
            )
        ]
        classes = Counter(_text(row.get("outcome_class")) for row in complete)
        status = (
            "INSUFFICIENT_DATA"
            if not complete
            else "WORSE"
            if classes["worse"] > classes["improved"]
            else "IMPROVED"
            if classes["improved"] > 0 and classes["worse"] == 0
            else "MIXED"
        )

        def mean(field: str, complete_rows: Sequence[Mapping[str, Any]] = complete) -> float | None:
            values = [float(row[field]) for row in complete_rows]
            return round(sum(values) / len(values), 10) if values else None

        return_delta = mean("return_delta_vs_limited")
        churn_delta = mean("churn_delta_vs_limited")
        breakdown_rows.append(
            {
                "method": method_name,
                "window_count": len(selected),
                "complete_window_count": len(complete),
                "sideways_status": status,
                "churn_reduction": (round(-churn_delta, 10) if churn_delta is not None else None),
                "return_delta": return_delta,
                "drawdown_delta": mean("drawdown_delta_vs_limited"),
                "lag_cost": (
                    round(max(0.0, -return_delta), 10) if return_delta is not None else None
                ),
                "turnover_delta": mean("turnover_delta_vs_limited"),
                **SYSTEM_TARGET_SAFETY,
            }
        )
        reasons = [
            _text(row.get("likely_reason")) for row in complete if _text(row.get("likely_reason"))
        ]
        dominant = Counter(reasons).most_common(1)[0][0] if reasons else "INSUFFICIENT_DATA"
        method_summaries.append(
            {
                "method": method_name,
                "sideways_validation": status,
                "improved_window_count": classes["improved"],
                "worse_window_count": classes["worse"],
                "mixed_window_count": classes["mixed"],
                "insufficient_window_count": len(selected) - len(complete),
                "dominant_reason": dominant,
                "recommendation": (
                    "needs_more_evidence"
                    if status == "INSUFFICIENT_DATA"
                    else "continue_observation"
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    regime_statuses = {
        row.get("method"): row.get("sideways_status")
        for row in _records(_mapping(regime.get("sideways_validation_summary")).get("methods"))
    }
    for row in breakdown_rows:
        _require(
            row.get("method") in regime_statuses,
            "sideways regime method missing",
        )
        row["regime_validation_status"] = regime_statuses[row["method"]]
    reason_summary = {
        "candidate_method": None,
        "sideways_validation": "PER_METHOD_ONLY",
        "methods": method_summaries,
        "recommendation": "defer_selection_to_confirmation_candidate",
        **SYSTEM_TARGET_SAFETY,
    }
    breakdown = {
        "methods": breakdown_rows,
        "preferred_sideways_method": None,
        "selection_deferred_to_confirmation_candidate": True,
        **SYSTEM_TARGET_SAFETY,
    }
    lineage = {
        "regime_validation_id": regime.get("regime_validation_id"),
        "churn_id": churn.get("churn_id"),
        "smoothed_backfill_id": regime.get("smoothed_backfill_id"),
        "baseline_backfill_id": regime.get("baseline_backfill_id"),
    }
    return outcomes, reason_summary, breakdown, lineage


def _render_sideways(
    manifest: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    reason: Mapping[str, Any],
    breakdown: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Sideways Mixed Attribution {manifest.get('sideways_attribution_id')}",
            "",
            f"- sideways_validation: {reason.get('sideways_validation')}",
            f"- recommendation: {reason.get('recommendation')}",
            f"- preferred_sideways_method: {breakdown.get('preferred_sideways_method')}",
            "- candidate_role_fixed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Method Summary",
            "",
            *[
                f"- {row.get('method')}: status={row.get('sideways_status')}, "
                f"complete_windows={row.get('complete_window_count')}, "
                f"return_delta={row.get('return_delta')}, "
                f"drawdown_delta={row.get('drawdown_delta')}, "
                f"churn_reduction={row.get('churn_reduction')}"
                for row in _records(breakdown.get("methods"))
            ],
            "",
            f"窗口行数: {len(outcomes)}。每个方法独立归因；"
            "候选选择只由 upstream confirmation contract 提供。",
            "",
        ]
    )


def _sideways_views(
    snapshot: Mapping[str, Any], *, sideways_attribution_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, readiness_policy = _policy(snapshot)
    evidence_policy = evidence._evidence_policy(config)
    outcomes, reason, breakdown, lineage = _sideways_outputs(
        snapshot, readiness_policy, evidence_policy
    )
    regime = evidence._regime_payload(_mapping(snapshot.get("regime_source")))
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_sideways_mixed_manifest",
        "sideways_attribution_id": sideways_attribution_id,
        **lineage,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": regime.get("market_regime"),
        "date_start": regime.get("date_start"),
        "date_end": regime.get("date_end"),
        "policy_id": _mapping(config.get("readiness_policy_metadata")).get("policy_id"),
        "sideways_mixed_attribution_input_snapshot_path": str(
            root / "sideways_mixed_attribution_input_snapshot.json"
        ),
        "sideways_mixed_manifest_path": str(root / "sideways_mixed_manifest.json"),
        "sideways_window_outcomes_path": str(root / "sideways_window_outcomes.jsonl"),
        "sideways_mixed_reason_summary_path": str(root / "sideways_mixed_reason_summary.json"),
        "sideways_3d_vs_5d_breakdown_path": str(root / "sideways_3d_vs_5d_breakdown.json"),
        "sideways_mixed_attribution_report_path": str(
            root / "sideways_mixed_attribution_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_sideways(manifest, outcomes, reason, breakdown)
    views = {
        "sideways_mixed_attribution_input_snapshot.json": _json_bytes(dict(snapshot)),
        "sideways_mixed_manifest.json": _json_bytes(manifest),
        "sideways_window_outcomes.jsonl": _jsonl_bytes(outcomes),
        "sideways_mixed_reason_summary.json": _json_bytes(reason),
        "sideways_3d_vs_5d_breakdown.json": _json_bytes(breakdown),
        "sideways_mixed_attribution_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "sideways_window_outcomes": outcomes,
        "sideways_mixed_reason_summary": reason,
        "sideways_3d_vs_5d_breakdown": breakdown,
    }


def _validate_sideways_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SIDEWAYS_MIXED_ATTRIBUTION_SNAPSHOT_SCHEMA,
            "sideways snapshot schema invalid",
        )
        regime_binding = _mapping(snapshot.get("regime_source"))
        churn_binding = _mapping(snapshot.get("churn_source"))
        errors.extend(
            _validate_binding(
                regime_binding,
                kind="smoothed_regime_validation",
                validator=evidence.validate_smoothed_regime_validation_artifact,
                validator_key="regime_validation_id",
            )
        )
        errors.extend(
            _validate_binding(
                churn_binding,
                kind="smoothed_churn_backfill",
                validator=validate_smoothed_churn_backfill_artifact,
                validator_key="churn_id",
            )
        )
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        regime = _bundle_json(regime_binding, "smoothed_regime_validation_manifest.json")
        churn = _bundle_json(churn_binding, "smoothed_churn_manifest.json")
        for field in ("smoothed_backfill_id", "baseline_backfill_id"):
            _require(regime.get(field) == churn.get(field), f"sideways {field} mismatch")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="sideways generated_at"
        )
        evidence._chronology(generated, regime, churn)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_sideways_mixed_attribution(
    *,
    regime_validation_id: str,
    churn_id: str,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    churn_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    # smoothed_backfill_dir/baseline_backfill_dir remain accepted for CLI and
    # Python compatibility. The exact sources are committed by churn_source.
    del smoothed_backfill_dir, baseline_backfill_dir
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SIDEWAYS_MIXED_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "regime_source": evidence._regime_binding(regime_validation_id, regime_validation_dir),
        "churn_source": _churn_binding(churn_id, churn_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_sideways_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("sideways-mixed-attribution", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _sideways_views(snapshot, sideways_attribution_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_sideways_mixed_attribution",
        "sideways_mixed_manifest.json",
    )
    return {
        "sideways_attribution_id": root.name,
        "sideways_attribution_dir": root,
        **payload,
    }


def sideways_mixed_attribution_report_payload(
    *,
    sideways_attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=sideways_attribution_id if not latest else None,
        pointer_name="latest_sideways_mixed_attribution",
    )
    return {
        **_read_json(root / "sideways_mixed_manifest.json"),
        "sideways_window_outcomes": _read_jsonl(root / "sideways_window_outcomes.jsonl"),
        "sideways_mixed_reason_summary": _read_json(root / "sideways_mixed_reason_summary.json"),
        "sideways_3d_vs_5d_breakdown": _read_json(root / "sideways_3d_vs_5d_breakdown.json"),
        "input_snapshot": _read_json(root / "sideways_mixed_attribution_input_snapshot.json"),
        "sideways_attribution_dir": str(root),
    }


def validate_sideways_mixed_attribution_artifact(
    *,
    sideways_attribution_id: str,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / sideways_attribution_id
    snapshot = (
        legacy._read_optional_json(root / "sideways_mixed_attribution_input_snapshot.json") or {}
    )
    errors = _validate_sideways_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _sideways_views(
            snapshot, sideways_attribution_id=sideways_attribution_id, root=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_sideways_mixed_attribution_validation",
        sideways_attribution_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="sideways_attribution_id",
    )


def _scorecard_binding(scorecard_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_readiness_scorecard",
        artifact_id=scorecard_id,
        root=root,
        validator=validate_smoothed_readiness_scorecard_artifact,
        validator_key="scorecard_id",
        json_views=(
            "smoothed_readiness_scorecard_input_snapshot.json",
            "smoothed_readiness_manifest.json",
            "smoothed_method_scorecard.json",
            "promotion_readiness_decision.json",
        ),
        text_views=("smoothed_readiness_scorecard_report.md",),
    )


def _scorecard_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_readiness_manifest.json"),
        "smoothed_method_scorecard": _bundle_json(binding, "smoothed_method_scorecard.json"),
        "promotion_readiness_decision": _bundle_json(binding, "promotion_readiness_decision.json"),
    }


def _support_status(attribution: Mapping[str, Any], method_name: str, metric: str) -> str:
    rows = [
        row
        for row in _records(
            _mapping(attribution.get("smoothed_metric_support_matrix")).get("methods")
        )
        if row.get("method") == method_name
    ]
    _require(len(rows) == 1, f"support matrix row invalid for {method_name}")
    return _text(_mapping(rows[0].get("statuses")).get(metric), "INSUFFICIENT_DATA")


def _status_score(status: str, policy: Mapping[str, Any]) -> float | None:
    value = _mapping(policy.get("status_scores")).get(status)
    return float(value) if _finite(value) else None


def _delta_score(value: Any, policy: Mapping[str, Any]) -> float | None:
    if not _finite(value):
        return None
    key = "positive" if float(value) > 0.0 else "negative" if float(value) < 0.0 else "neutral"
    score = _mapping(policy.get("delta_scores")).get(key)
    _require(_finite(score), f"delta score missing for {key}")
    return float(score)


def _forward_status(confirmation: Mapping[str, Any], method_name: str) -> str:
    targets = _mapping(confirmation.get("smoothed_confirmation_targets"))
    rows = [row for row in _records(targets.get("targets")) if row.get("method") == method_name]
    if not rows:
        return "NOT_REGISTERED"
    statuses = {_text(row.get("status")) for row in rows}
    if "FAILED" in statuses:
        return "FAILED"
    if "IN_PROGRESS" in statuses:
        return "IN_PROGRESS"
    if "WATCH_ONLY" in statuses:
        return "WATCH_ONLY"
    return "PASS"


def _hard_blocks(
    *,
    return_status: str,
    lag_status: str,
    sideways_status: str,
    forward_status: str,
    policy: Mapping[str, Any],
) -> list[str]:
    configured = _mapping(policy.get("hard_block_values"))
    reasons = []
    for field, status, reason in (
        ("return_preservation", return_status, "return_preservation_poor"),
        ("recovery_lag", lag_status, "recovery_lag_high"),
        ("sideways", sideways_status, "sideways_status_worse"),
        ("forward_confirmation", forward_status, "forward_confirmation_failed"),
    ):
        if status in list(configured.get(field) or []):
            reasons.append(reason)
    return reasons


def _method_scorecard(
    attribution: Mapping[str, Any],
    benefit: Mapping[str, Any],
    churn: Mapping[str, Any],
    sideways: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    churn_rows = _records(_mapping(churn.get("churn_reduction_summary")).get("methods"))
    sideways_rows = _records(_mapping(sideways.get("sideways_3d_vs_5d_breakdown")).get("methods"))
    tradeoff_rows = _records(_mapping(benefit.get("benefit_lag_tradeoff_matrix")).get("methods"))
    rows: list[dict[str, Any]] = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        churn_row = next((row for row in churn_rows if row.get("method") == method_name), None)
        sideways_row = next(
            (row for row in sideways_rows if row.get("method") == method_name), None
        )
        tradeoff = next((row for row in tradeoff_rows if row.get("method") == method_name), None)
        _require(
            churn_row is not None and sideways_row is not None and tradeoff is not None,
            f"scorecard source rows incomplete for {method_name}",
        )
        return_status = _support_status(attribution, method_name, "return_preservation")
        drawdown_status = _support_status(attribution, method_name, "drawdown")
        sideways_status = _text(sideways_row.get("sideways_status"))
        lag_status = _text(tradeoff.get("lag_cost_status"))
        forward_status = _forward_status(confirmation, method_name)
        scores = {
            "return_preservation_score": _status_score(return_status, policy),
            "drawdown_score": _status_score(drawdown_status, policy),
            "turnover_score": _delta_score(churn_row.get("turnover_reduction_vs_limited"), policy),
            "weight_jump_score": _delta_score(
                churn_row.get("weight_jump_reduction_vs_limited"), policy
            ),
            "signal_churn_score": _delta_score(
                churn_row.get("signal_churn_reduction_vs_limited"), policy
            ),
            "sideways_score": _status_score(sideways_status, policy),
            "recovery_lag_score": (
                float(_mapping(policy.get("lag_status_scores"))[lag_status])
                if _finite(_mapping(policy.get("lag_status_scores")).get(lag_status))
                else None
            ),
            "forward_confirmation_score": (
                float(_mapping(policy.get("forward_status_scores"))[forward_status])
                if _finite(_mapping(policy.get("forward_status_scores")).get(forward_status))
                else None
            ),
        }
        missing = [key for key, value in scores.items() if value is None]
        hard_blocks = _hard_blocks(
            return_status=return_status,
            lag_status=lag_status,
            sideways_status=sideways_status,
            forward_status=forward_status,
            policy=policy,
        )
        overall = (
            round(
                sum(
                    float(scores[key]) * float(_mapping(policy["score_weights"])[key])
                    for key in _SCORE_COMPONENTS
                ),
                10,
            )
            if not missing
            else None
        )
        readiness_status = (
            "REJECT"
            if hard_blocks
            else "INSUFFICIENT_EVIDENCE"
            if overall is None
            else "PROMOTE_FOR_REVIEW"
            if overall >= float(policy["promote_review_score"])
            else "CONTINUE_OBSERVATION"
            if overall >= float(policy["continue_observation_score"])
            else "REVIEW_REQUIRED"
        )
        rows.append(
            {
                "method": method_name,
                "source_statuses": {
                    "return_preservation": return_status,
                    "drawdown": drawdown_status,
                    "sideways": sideways_status,
                    "recovery_lag": lag_status,
                    "forward_confirmation": forward_status,
                },
                **scores,
                "missing_score_components": missing,
                "hard_block_reasons": hard_blocks,
                "overall_readiness_score": overall,
                "readiness_status": readiness_status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    targets = _mapping(confirmation.get("smoothed_confirmation_targets"))
    candidate = targets.get("candidate_method")
    _require(
        candidate is None or candidate in SMOOTHED_METHOD_TO_VARIANT,
        "scorecard confirmation candidate invalid",
    )
    return {
        "candidate_method": candidate,
        "candidate_source": "smoothed_confirmation_targets",
        "candidate_role_fixed": False,
        "methods": rows,
        "score_weights": dict(policy["score_weights"]),
        "missing_component_score": None,
        **SYSTEM_TARGET_SAFETY,
    }


def _readiness_decision(
    scorecard: Mapping[str, Any], confirmation: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = scorecard.get("candidate_method")
    targets = _mapping(confirmation.get("smoothed_confirmation_targets"))
    target_rows = _records(targets.get("targets"))
    if candidate is None:
        _require(not target_rows, "candidate-less confirmation must contain zero targets")
        return {
            "recommended_method": None,
            "secondary_method": None,
            "candidate_source": "smoothed_confirmation_targets",
            "evidence_status": "INSUFFICIENT_EVIDENCE",
            "decision": "CONTINUE_OBSERVATION",
            "confidence": "LOW",
            "primary_reasons": ["no_evidence_backed_candidate"],
            "blocking_reasons": [
                "no_eligible_candidate",
                "forward_target_not_registered",
                "readiness_score_cannot_create_candidate",
            ],
            "required_forward_confirmation": [],
            "auto_apply": False,
            **SYSTEM_TARGET_SAFETY,
        }
    rows = [row for row in _records(scorecard.get("methods")) if row.get("method") == candidate]
    _require(len(rows) == 1, "candidate readiness row invalid")
    row = rows[0]
    required = [
        row.get("target_id")
        for row in target_rows
        if row.get("method") == candidate and row.get("status") in {"IN_PROGRESS", "WATCH_ONLY"}
    ]
    blocking = list(row.get("hard_block_reasons") or [])
    blocking.extend(f"missing_{key}" for key in list(row.get("missing_score_components") or []))
    if required:
        blocking.append("forward_confirmation_in_progress")
    status = _text(row.get("readiness_status"), "INSUFFICIENT_EVIDENCE")
    decision = "CONTINUE_OBSERVATION" if status == "INSUFFICIENT_EVIDENCE" else status
    return {
        "recommended_method": candidate,
        "secondary_method": None,
        "candidate_source": "smoothed_confirmation_targets",
        "evidence_status": status,
        "decision": decision,
        "confidence": "LOW" if required or blocking else "MEDIUM",
        "primary_reasons": ["confirmation_candidate_evaluated"],
        "blocking_reasons": blocking,
        "required_forward_confirmation": required,
        "auto_apply": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_scorecard(
    manifest: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Readiness Scorecard {manifest.get('scorecard_id')}",
            "",
            f"- recommended_method: {decision.get('recommended_method')}",
            f"- secondary_method: {decision.get('secondary_method')}",
            f"- evidence_status: {decision.get('evidence_status')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            "- blocking_reasons: "
            f"{', '.join(str(item) for item in decision.get('blocking_reasons', []))}",
            "- missing_evidence_scores_zero: false",
            "- candidate_role_fixed: false",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Methods",
            "",
            *[
                f"- {row.get('method')}: overall={row.get('overall_readiness_score')}, "
                f"status={row.get('readiness_status')}, "
                f"missing={','.join(row.get('missing_score_components', []))}"
                for row in _records(scorecard.get("methods"))
            ],
            "",
            "只有 confirmation 已登记的 evidence-backed candidate 才能成为 "
            "recommended_method；方法分数不能反向创造候选。",
            "",
        ]
    )


def _scorecard_views(
    snapshot: Mapping[str, Any], *, scorecard_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config, policy = _policy(snapshot)
    attribution = evidence._simple_payload(
        _mapping(snapshot.get("attribution_source")),
        "smoothed_review_attribution_manifest.json",
        {
            "smoothed_decision_reason_breakdown": "smoothed_decision_reason_breakdown.json",
            "smoothed_metric_support_matrix": "smoothed_metric_support_matrix.json",
        },
    )
    benefit = evidence._simple_payload(
        _mapping(snapshot.get("benefit_lag_source")),
        "smoothing_benefit_lag_manifest.json",
        {
            "smoothing_benefit_summary": "smoothing_benefit_summary.json",
            "lag_cost_summary": "lag_cost_summary.json",
            "benefit_lag_tradeoff_matrix": "benefit_lag_tradeoff_matrix.json",
        },
    )
    churn_binding = _mapping(snapshot.get("churn_source"))
    churn = {
        **_bundle_json(churn_binding, "smoothed_churn_manifest.json"),
        "churn_reduction_summary": _bundle_json(churn_binding, "churn_reduction_summary.json"),
    }
    sideways = _sideways_payload(_mapping(snapshot.get("sideways_source")))
    confirmation = evidence._simple_payload(
        _mapping(snapshot.get("confirmation_source")),
        "smoothed_confirmation_manifest.json",
        {"smoothed_confirmation_targets": "smoothed_confirmation_targets.json"},
    )
    scorecard = _method_scorecard(attribution, benefit, churn, sideways, confirmation, policy)
    decision = _readiness_decision(scorecard, confirmation)
    lineage = {
        "review_id": attribution.get("review_id"),
        "comparison_id": attribution.get("comparison_id"),
        "smoothed_backfill_id": attribution.get("smoothed_backfill_id"),
        "baseline_backfill_id": attribution.get("baseline_backfill_id"),
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_readiness_manifest",
        "scorecard_id": scorecard_id,
        "attribution_id": attribution.get("attribution_id"),
        "benefit_lag_id": benefit.get("drilldown_id"),
        "churn_id": churn.get("churn_id"),
        "sideways_attribution_id": sideways.get("sideways_attribution_id"),
        "confirmation_id": confirmation.get("confirmation_id"),
        **lineage,
        "candidate_method": scorecard.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "score_weights": dict(policy["score_weights"]),
        "policy_id": _mapping(config.get("readiness_policy_metadata")).get("policy_id"),
        "smoothed_readiness_scorecard_input_snapshot_path": str(
            root / "smoothed_readiness_scorecard_input_snapshot.json"
        ),
        "smoothed_readiness_manifest_path": str(root / "smoothed_readiness_manifest.json"),
        "smoothed_method_scorecard_path": str(root / "smoothed_method_scorecard.json"),
        "promotion_readiness_decision_path": str(root / "promotion_readiness_decision.json"),
        "smoothed_readiness_scorecard_report_path": str(
            root / "smoothed_readiness_scorecard_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_scorecard(manifest, scorecard, decision)
    views = {
        "smoothed_readiness_scorecard_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_readiness_manifest.json": _json_bytes(manifest),
        "smoothed_method_scorecard.json": _json_bytes(scorecard),
        "promotion_readiness_decision.json": _json_bytes(decision),
        "smoothed_readiness_scorecard_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_method_scorecard": scorecard,
        "promotion_readiness_decision": decision,
    }


def _validate_scorecard_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_READINESS_SCORECARD_SNAPSHOT_SCHEMA,
            "scorecard snapshot schema invalid",
        )
        specs = (
            (
                "attribution_source",
                "smoothed_review_attribution",
                evidence.validate_smoothed_review_attribution_artifact,
                "attribution_id",
                "smoothed_review_attribution_manifest.json",
            ),
            (
                "benefit_lag_source",
                "smoothing_benefit_lag",
                evidence.validate_smoothing_benefit_lag_artifact,
                "drilldown_id",
                "smoothing_benefit_lag_manifest.json",
            ),
            (
                "churn_source",
                "smoothed_churn_backfill",
                validate_smoothed_churn_backfill_artifact,
                "churn_id",
                "smoothed_churn_manifest.json",
            ),
            (
                "sideways_source",
                "sideways_mixed_attribution",
                validate_sideways_mixed_attribution_artifact,
                "sideways_attribution_id",
                "sideways_mixed_manifest.json",
            ),
            (
                "confirmation_source",
                "smoothed_confirmation",
                evidence.validate_smoothed_confirmation_artifact,
                "confirmation_id",
                "smoothed_confirmation_manifest.json",
            ),
        )
        manifests: dict[str, dict[str, Any]] = {}
        for field, kind, validator, key, manifest_name in specs:
            binding = _mapping(snapshot.get(field))
            errors.extend(
                _validate_binding(binding, kind=kind, validator=validator, validator_key=key)
            )
            manifests[field] = _bundle_json(binding, manifest_name)
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        attribution = manifests["attribution_source"]
        benefit = manifests["benefit_lag_source"]
        churn = manifests["churn_source"]
        sideways = manifests["sideways_source"]
        confirmation = manifests["confirmation_source"]
        _require(
            attribution.get("review_id") == confirmation.get("review_id"),
            "scorecard review_id mismatch",
        )
        _require(
            attribution.get("comparison_id")
            == benefit.get("comparison_id")
            == confirmation.get("comparison_id"),
            "scorecard comparison_id mismatch",
        )
        for field in ("smoothed_backfill_id", "baseline_backfill_id"):
            _require(
                len(
                    {
                        attribution.get(field),
                        benefit.get(field),
                        churn.get(field),
                        sideways.get(field),
                        confirmation.get(field),
                    }
                )
                == 1,
                f"scorecard {field} mismatch",
            )
        _require(
            sideways.get("churn_id") == churn.get("churn_id"),
            "scorecard sideways/churn mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="scorecard generated_at"
        )
        evidence._chronology(generated, *manifests.values())
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_readiness_scorecard(
    *,
    attribution_id: str,
    benefit_lag_id: str,
    churn_id: str,
    sideways_attribution_id: str,
    confirmation_id: str,
    attribution_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    churn_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    sideways_attribution_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_READINESS_SCORECARD_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "attribution_source": evidence._attribution_binding(attribution_id, attribution_dir),
        "benefit_lag_source": evidence._benefit_binding(benefit_lag_id, benefit_lag_dir),
        "churn_source": _churn_binding(churn_id, churn_dir),
        "sideways_source": _sideways_binding(sideways_attribution_id, sideways_attribution_dir),
        "confirmation_source": evidence._confirmation_binding(confirmation_id, confirmation_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_scorecard_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-readiness-scorecard", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _scorecard_views(snapshot, scorecard_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_readiness_scorecard",
        "smoothed_readiness_manifest.json",
    )
    return {"scorecard_id": root.name, "scorecard_dir": root, **payload}


def smoothed_readiness_scorecard_report_payload(
    *,
    scorecard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=scorecard_id if not latest else None,
        pointer_name="latest_smoothed_readiness_scorecard",
    )
    return {
        **_read_json(root / "smoothed_readiness_manifest.json"),
        "smoothed_method_scorecard": _read_json(root / "smoothed_method_scorecard.json"),
        "promotion_readiness_decision": _read_json(root / "promotion_readiness_decision.json"),
        "input_snapshot": _read_json(root / "smoothed_readiness_scorecard_input_snapshot.json"),
        "scorecard_dir": str(root),
    }


def validate_smoothed_readiness_scorecard_artifact(
    *,
    scorecard_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> dict[str, Any]:
    root = output_dir / scorecard_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_readiness_scorecard_input_snapshot.json") or {}
    )
    errors = _validate_scorecard_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _scorecard_views(snapshot, scorecard_id=scorecard_id, root=root)
        mismatches = _view_errors(root, views)
        decision = _mapping(payload.get("promotion_readiness_decision"))
        if decision.get("recommended_method") is None:
            _require(
                decision.get("decision") != "PROMOTE_FOR_REVIEW",
                "candidate-less scorecard cannot promote",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_readiness_scorecard_validation",
        scorecard_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="scorecard_id",
    )


def _owner_options(
    owner_update_id: str,
    scorecard: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    candidate = decision.get("recommended_method")
    _require(
        candidate == watch_summary.get("candidate_method"),
        "owner scorecard/watch candidate mismatch",
    )
    readiness = _text(decision.get("decision"), "CONTINUE_OBSERVATION")
    forward_status = _text(watch_summary.get("forward_confirmation_status"), "NOT_REGISTERED")
    promote = (
        candidate is not None
        and readiness == "PROMOTE_FOR_REVIEW"
        and forward_status == "PASS"
        and not list(decision.get("blocking_reasons") or [])
    )
    if promote:
        recommended_action = "review_for_manual_promotion_decision"
    elif candidate is None:
        recommended_action = "request_additional_evidence"
    elif forward_status in {"IN_PROGRESS", "WATCH_ONLY", "NOT_REGISTERED"}:
        recommended_action = "continue_forward_observation"
    elif readiness == "REJECT":
        recommended_action = "reject_smoothed_promotion"
    else:
        recommended_action = "continue_observation"
    options = [
        {
            "decision": "request_additional_evidence",
            "recommended": recommended_action == "request_additional_evidence",
            "reason": "No evidence-backed candidate exists; readiness scoring cannot create one.",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "decision": "continue_observation",
            "recommended": recommended_action
            in {"continue_observation", "continue_forward_observation"},
            "reason": "Continue only the registered research observation path.",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "decision": "promote_for_research_review",
            "recommended": promote,
            "reason": "Allowed only for the upstream confirmation candidate after all "
            "readiness and forward gates pass.",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "decision": "reject_smoothed_method",
            "recommended": recommended_action == "reject_smoothed_promotion",
            "reason": "Use only when candidate evidence triggers a configured hard block.",
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    _require(
        sum(row.get("recommended") is True for row in options) == 1,
        "owner options must contain one recommended action",
    )
    return {
        "owner_update_id": owner_update_id,
        "candidate_method": candidate,
        "secondary_method": decision.get("secondary_method"),
        "current_decision": watch_summary.get("current_decision"),
        "readiness_decision": readiness,
        "readiness_evidence_status": decision.get("evidence_status"),
        "recommended_owner_action": recommended_action,
        "forward_confirmation_status": forward_status,
        "owner_decision_options": options,
        "candidate_role_fixed": False,
        "auto_apply": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_owner_checklist(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Review Checklist",
            "",
            "- [ ] candidate_method 是否来自 validated confirmation artifact？",
            "- [ ] 缺失 score component 是否保持 null？",
            "- [ ] 是否存在独立 forward / PIT / DQ / cost / holdout 证据？",
            "- [ ] 是否确认 readiness score 未创造候选？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            "",
        ]
    )


def _render_owner_reader(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Owner Review",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- readiness_evidence_status: {options.get('readiness_evidence_status')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            "- candidate_role_fixed: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_owner_report(manifest: Mapping[str, Any], options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Smoothed Owner Review Update {manifest.get('owner_update_id')}",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- secondary_method: {options.get('secondary_method')}",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- readiness_evidence_status: {options.get('readiness_evidence_status')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Owner Options",
            "",
            *[
                f"- {row.get('decision')}: recommended={row.get('recommended')}, "
                f"reason={row.get('reason')}"
                for row in _records(options.get("owner_decision_options"))
            ],
            "",
            "候选为空时，promotion 选项必须保持非推荐；本报告只提供人工研究决策输入。",
            "",
        ]
    )


def _owner_views(
    snapshot: Mapping[str, Any], *, owner_update_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    scorecard = _scorecard_payload(_mapping(snapshot.get("scorecard_source")))
    watch = _watch_payload(_mapping(snapshot.get("watch_source")))
    options = _owner_options(owner_update_id, scorecard, watch)
    checklist = _render_owner_checklist(options)
    reader = _render_owner_reader(options)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_owner_update_manifest",
        "owner_update_id": owner_update_id,
        "scorecard_id": scorecard.get("scorecard_id"),
        "watch_pack_id": watch.get("watch_pack_id"),
        "review_id": scorecard.get("review_id"),
        "comparison_id": scorecard.get("comparison_id"),
        "smoothed_backfill_id": scorecard.get("smoothed_backfill_id"),
        "baseline_backfill_id": scorecard.get("baseline_backfill_id"),
        "candidate_method": options.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_owner_review_update_input_snapshot_path": str(
            root / "smoothed_owner_review_update_input_snapshot.json"
        ),
        "smoothed_owner_update_manifest_path": str(root / "smoothed_owner_update_manifest.json"),
        "smoothed_owner_decision_options_path": str(root / "smoothed_owner_decision_options.json"),
        "smoothed_owner_review_checklist_path": str(root / "smoothed_owner_review_checklist.md"),
        "smoothed_owner_review_update_report_path": str(
            root / "smoothed_owner_review_update_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_owner_report(manifest, options)
    views = {
        "smoothed_owner_review_update_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_owner_update_manifest.json": _json_bytes(manifest),
        "smoothed_owner_decision_options.json": _json_bytes(options),
        "smoothed_owner_review_checklist.md": checklist.encode("utf-8"),
        "smoothed_owner_review_update_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_owner_decision_options": options,
        "smoothed_owner_review_checklist": checklist,
        "reader_brief_section": reader,
    }


def _validate_owner_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_OWNER_REVIEW_UPDATE_SNAPSHOT_SCHEMA,
            "owner snapshot schema invalid",
        )
        scorecard_binding = _mapping(snapshot.get("scorecard_source"))
        watch_binding = _mapping(snapshot.get("watch_source"))
        errors.extend(
            _validate_binding(
                scorecard_binding,
                kind="smoothed_readiness_scorecard",
                validator=validate_smoothed_readiness_scorecard_artifact,
                validator_key="scorecard_id",
            )
        )
        errors.extend(
            _validate_binding(
                watch_binding,
                kind="smoothed_watch_pack",
                validator=evidence.validate_smoothed_watch_pack_artifact,
                validator_key="watch_pack_id",
            )
        )
        scorecard = _bundle_json(scorecard_binding, "smoothed_readiness_manifest.json")
        watch = _bundle_json(watch_binding, "smoothed_watch_manifest.json")
        for field in (
            "review_id",
            "comparison_id",
            "smoothed_backfill_id",
            "baseline_backfill_id",
        ):
            _require(scorecard.get(field) == watch.get(field), f"owner {field} mismatch")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="owner update generated_at"
        )
        evidence._chronology(generated, scorecard, watch)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_owner_review_update(
    *,
    scorecard_id: str,
    watch_pack_id: str,
    scorecard_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_OWNER_REVIEW_UPDATE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "scorecard_source": _scorecard_binding(scorecard_id, scorecard_dir),
        "watch_source": _watch_binding(watch_pack_id, watch_pack_dir),
        "production_effect": "none",
    }
    errors = _validate_owner_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-owner-review-update", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _owner_views(snapshot, owner_update_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_owner_review_update",
        "smoothed_owner_update_manifest.json",
    )
    return {"owner_update_id": root.name, "owner_update_dir": root, **payload}


def smoothed_owner_review_update_report_payload(
    *,
    owner_update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=owner_update_id if not latest else None,
        pointer_name="latest_smoothed_owner_review_update",
    )
    return {
        **_read_json(root / "smoothed_owner_update_manifest.json"),
        "smoothed_owner_decision_options": _read_json(
            root / "smoothed_owner_decision_options.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_owner_review_update_input_snapshot.json"),
        "owner_update_dir": str(root),
    }


def validate_smoothed_owner_review_update_artifact(
    *,
    owner_update_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_update_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_owner_review_update_input_snapshot.json") or {}
    )
    errors = _validate_owner_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _owner_views(snapshot, owner_update_id=owner_update_id, root=root)
        mismatches = _view_errors(root, views)
        options = _mapping(payload.get("smoothed_owner_decision_options"))
        if options.get("candidate_method") is None:
            promotion = next(
                row
                for row in _records(options.get("owner_decision_options"))
                if row.get("decision") == "promote_for_research_review"
            )
            _require(
                promotion.get("recommended") is False,
                "candidate-less owner update cannot recommend promotion",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_review_update_validation",
        owner_update_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="owner_update_id",
    )
