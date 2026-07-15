from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_followup as followup
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "signal_diagnosis_foundation_v1.yaml"
)
DEFAULT_GATE_CALIBRATION_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "gate_calibration_review"
)
DEFAULT_SCORECARD_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "scorecard_attribution"
)
DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_instability_diagnosis"
)
DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "consensus_quality_review"
)
DEFAULT_NO_PROMOTION_REVIEW_DIR = diagnostics.DEFAULT_NO_PROMOTION_REVIEW_DIR
DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR = (
    followup.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR
)
DEFAULT_WEIGHT_SCORECARD_DIR = evaluation.DEFAULT_WEIGHT_SCORECARD_DIR
DEFAULT_TARGETED_SEARCH_V3_DIR = targeted.DEFAULT_TARGETED_SEARCH_V3_DIR
DEFAULT_TARGETED_V3_BACKFILL_DIR = targeted.DEFAULT_TARGETED_V3_BACKFILL_DIR

GATE_INPUT_SCHEMA = "gate_calibration_review_input_snapshot.v2"
ATTRIBUTION_INPUT_SCHEMA = "scorecard_attribution_input_snapshot.v2"
SIGNAL_INPUT_SCHEMA = "signal_instability_diagnosis_input_snapshot.v2"
CONSENSUS_INPUT_SCHEMA = "consensus_quality_review_input_snapshot.v2"

GATE_VIEWS = (
    "gate_calibration_manifest.json",
    "gate_strictness_diagnosis.json",
    "gate_component_impact.json",
    "diagnostic_relaxed_gate_result.json",
    "gate_calibration_review_report.md",
    "reader_brief_section.md",
)
ATTRIBUTION_VIEWS = (
    "scorecard_attribution_manifest.json",
    "score_component_distribution.json",
    "rejected_variant_component_matrix.jsonl",
    "family_component_weakness.json",
    "scorecard_attribution_report.md",
)
SIGNAL_VIEWS = (
    "signal_instability_manifest.json",
    "method_signal_stability.jsonl",
    "signal_flip_events.jsonl",
    "regime_mismatch_events.jsonl",
    "signal_instability_summary.json",
    "signal_instability_diagnosis_report.md",
    "reader_brief_section.md",
)
CONSENSUS_VIEWS = (
    "consensus_quality_manifest.json",
    "consensus_dispersion_summary.json",
    "ensemble_method_quality.jsonl",
    "consensus_failure_reasons.json",
    "consensus_quality_review_report.md",
    "reader_brief_section.md",
)
GATE_FILES = (*GATE_VIEWS, "gate_calibration_review_input_snapshot.json")
ATTRIBUTION_FILES = (*ATTRIBUTION_VIEWS, "scorecard_attribution_input_snapshot.json")
SIGNAL_FILES = (*SIGNAL_VIEWS, "signal_instability_diagnosis_input_snapshot.json")
CONSENSUS_FILES = (*CONSENSUS_VIEWS, "consensus_quality_review_input_snapshot.json")

PROMOTION_GATE_UNIVERSE = (
    "composite_score_gate",
    "return_preservation_gate",
    "drawdown_gate",
    "rolling_consistency_gate",
    "turnover_gate",
    "regime_gate",
    "recovery_lag_gate",
    "data_quality_gate",
)

_mapping = foundation._mapping
_records = foundation._records
_texts = foundation._texts
_text = foundation._text
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_artifact_dir = foundation._artifact_dir
_read_json = foundation._read_json
_read_jsonl = foundation._read_jsonl
_write_json = foundation._write_json
_write_jsonl = foundation._write_jsonl
_write_text = foundation._write_text
_write_latest_pointer = foundation._write_latest_pointer
_validation_payload = foundation._validation_payload


class DynamicV3SignalDiagnosisFoundationError(ValueError):
    """Raised when TRADING-316～319 evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SignalDiagnosisFoundationError(message)


def _generated_time(generated_at: datetime | None) -> datetime:
    return diagnostics._generated_time(generated_at)


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    return diagnostics._aware_datetime(value, field)


def _source_dir(binding: Mapping[str, Any]) -> Path:
    return Path(_text(binding.get("source_dir")))


def _source_id(binding: Mapping[str, Any]) -> str:
    return _text(binding.get("artifact_id"))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _validate_binding(binding: Mapping[str, Any], *, kind: str) -> None:
    foundation._validate_artifact_binding(binding, kind=kind)


def _snapshot_preflight(
    *,
    root: Path,
    snapshot_name: str,
    schema: str,
    id_key: str,
    artifact_id: str,
    view_names: Sequence[str],
) -> tuple[list[dict[str, Any]], bool]:
    return diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=snapshot_name,
        schema=schema,
        id_key=id_key,
        artifact_id=artifact_id,
        view_names=view_names,
    )


def _validate_content(
    *,
    report_type: str,
    artifact_id: str,
    checks: list[dict[str, Any]],
    rebuild: Callable[[], list[dict[str, Any]]],
) -> dict[str, Any]:
    return diagnostics._validate_content(
        report_type=report_type,
        artifact_id=artifact_id,
        checks=checks,
        rebuild=rebuild,
    )


def _check_bytes(root: Path, expected: Mapping[str, bytes]) -> list[dict[str, Any]]:
    return diagnostics._check_bytes(root, expected)


def _view_hash_check(root: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return diagnostics._view_hash_check(root, snapshot)


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version")
        == "dynamic_v3_signal_diagnosis_foundation_policy.v1",
        "signal diagnosis policy schema mismatch",
    )
    metadata = _mapping(payload.get("policy_metadata"))
    for field in (
        "policy_version",
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
    ):
        _require(bool(_text(metadata.get(field))), f"policy metadata missing: {field}")
    _require(metadata.get("status") in {"pilot_baseline", "reviewed", "active"}, "policy status")

    gate = _mapping(payload.get("gate_calibration"))
    high_share = _number(gate.get("high_impact_failed_share_min"), "high impact share")
    _require(0.0 < high_share <= 1.0, "high impact share outside (0, 1]")
    _require(gate.get("official_gate_change_allowed") is False, "official gate change forbidden")
    assessments = set(_texts(gate.get("calibrated_assessments")))
    _require(
        assessments == {"REASONABLE", "TOO_STRICT", "TOO_LOOSE", "INCONCLUSIVE"},
        "calibrated assessments invalid",
    )
    conclusions = _mapping(gate.get("diagnostic_conclusions"))
    for name in ("gate_not_primary_issue", "gate_may_be_too_strict", "inconclusive"):
        rule = _mapping(conclusions.get(name))
        _require(_text(rule.get("calibrated_assessment")) in assessments, f"gate rule: {name}")
        _require(bool(_text(rule.get("recommendation"))), f"gate recommendation: {name}")

    attribution = _mapping(payload.get("scorecard_attribution"))
    components = _texts(attribution.get("components"))
    _require(len(components) == len(set(components)) >= 3, "scorecard components invalid")
    high = _number(attribution.get("weakness_high_below"), "weakness high")
    medium = _number(attribution.get("weakness_medium_below"), "weakness medium")
    _require(0.0 < high < medium <= 1.0, "weakness bands invalid")
    _require(
        0 < _integer(attribution.get("dominant_weak_component_count"), "dominant count")
        <= len(components),
        "dominant component count invalid",
    )

    signal = _mapping(payload.get("signal_diagnosis"))
    _require(signal.get("dated_signal_events_required_for_event_claims") is True, "dated gate")
    _validate_method_policy(_mapping(signal.get("methods")), "signal")
    consensus = _mapping(payload.get("consensus_quality"))
    _validate_method_policy(_mapping(consensus.get("methods")), "consensus")
    _require(
        _text(consensus.get("no_dated_dispersion_status")) == "INSUFFICIENT_DATA",
        "consensus missing-data status",
    )

    safety = _mapping(payload.get("safety"))
    for field in (
        "research_screening_only",
        "experiment_only",
        "not_formal_research_method",
        "not_official_target_weights",
        "paper_shadow_only",
        "owner_review_required",
    ):
        _require(safety.get(field) is True, f"safety must enable {field}")
    for field in (
        "paper_shadow_primary_changed",
        "broker_action_allowed",
        "broker_action_taken",
        "order_ticket_generated",
        "auto_apply",
    ):
        _require(safety.get(field) is False, f"safety must disable {field}")
    _require(safety.get("production_effect") == "none", "production effect invalid")
    return payload


def _validate_method_policy(methods: Mapping[str, Any], scope: str) -> None:
    _require(bool(methods), f"{scope} methods missing")
    for name, raw in methods.items():
        _require(bool(_text(name)), f"{scope} method name missing")
        variant_ids = _texts(_mapping(raw).get("exact_variant_ids"))
        _require(bool(variant_ids), f"{scope} exact variants missing: {name}")
        _require(len(variant_ids) == len(set(variant_ids)), f"{scope} duplicate variants: {name}")


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise DynamicV3SignalDiagnosisFoundationError(f"{field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DynamicV3SignalDiagnosisFoundationError(f"{field} must be numeric") from exc
    _require(math.isfinite(number), f"{field} must be finite")
    return number


def _integer(value: Any, field: str) -> int:
    number = _number(value, field)
    _require(number.is_integer(), f"{field} must be an integer")
    return int(number)


def _optional_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _validated_review(review_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnostics.validate_no_promotion_review_artifact,
        validator_key="review_id",
        artifact_id=review_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source no-promotion review validation failed")
    return diagnostics.no_promotion_review_report_payload(
        review_id=review_id, output_dir=output_dir
    )


def _validated_sensitivity(sensitivity_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=followup.validate_promotion_threshold_sensitivity_artifact,
        validator_key="sensitivity_id",
        artifact_id=sensitivity_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source sensitivity validation failed")
    return followup.promotion_threshold_sensitivity_report_payload(
        sensitivity_id=sensitivity_id, output_dir=output_dir
    )


def _validated_scorecard(scorecard_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=evaluation.validate_weight_scorecard_artifact,
        validator_key="scorecard_id",
        artifact_id=scorecard_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source scorecard validation failed")
    return evaluation.weight_scorecard_report_payload(
        scorecard_id=scorecard_id, output_dir=output_dir
    )


def _validated_matrix(matrix_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=targeted.validate_targeted_search_v3_artifact,
        validator_key="v3_matrix_id",
        artifact_id=matrix_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source targeted matrix validation failed")
    return targeted.targeted_search_v3_report_payload(v3_matrix_id=matrix_id, output_dir=output_dir)


def _validated_backfill(backfill_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=targeted.validate_targeted_v3_backfill_artifact,
        validator_key="v3_backfill_id",
        artifact_id=backfill_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source targeted backfill validation failed")
    return targeted.targeted_v3_backfill_report_payload(
        v3_backfill_id=backfill_id, output_dir=output_dir
    )


def _validated_attribution(attribution_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_scorecard_attribution_artifact,
        validator_key="scorecard_attribution_id",
        artifact_id=attribution_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source attribution validation failed")
    return scorecard_attribution_report_payload(
        scorecard_attribution_id=attribution_id, output_dir=output_dir
    )


def _validated_signal(signal_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_signal_instability_diagnosis_artifact,
        validator_key="signal_diagnosis_id",
        artifact_id=signal_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source signal diagnosis validation failed")
    return signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=signal_id, output_dir=output_dir
    )


def _matrix_from_sensitivity(sensitivity: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(_mapping(sensitivity.get("input_snapshot")).get("matrix_source"))
    _validate_binding(source, kind="targeted_search_v3")
    return _validated_matrix(_source_id(source), _source_dir(source).parent)


def _scorecard_id_from_review(review: Mapping[str, Any]) -> str:
    source = _mapping(_mapping(review.get("input_snapshot")).get("scorecard_source"))
    _validate_binding(source, kind="weight_scorecard")
    return _source_id(source)


def _chronology(generated: datetime, *sources: Mapping[str, Any]) -> None:
    generated_time = _aware_datetime(generated, "generated_at")
    for source in sources:
        source_time = _aware_datetime(_text(source.get("generated_at")), "source.generated_at")
        _require(source_time <= generated_time, "source generated after output cutoff")


def _gate_outputs(
    review: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    summary = _mapping(review.get("no_promotion_reason_summary"))
    total = _integer(review.get("variants_reviewed"), "review.variants_reviewed")
    failures = {
        _text(row.get("gate")): row
        for row in _records(_mapping(review.get("gate_failure_distribution")).get("failures"))
    }
    high_share = _number(
        _mapping(policy.get("gate_calibration")).get("high_impact_failed_share_min"),
        "high impact share",
    )
    components: list[dict[str, Any]] = []
    for gate in PROMOTION_GATE_UNIVERSE:
        row = _mapping(failures.get(gate))
        blocked = _integer(row.get("failed_count", 0), f"{gate}.failed_count")
        near_miss = _integer(row.get("near_miss_count", 0), f"{gate}.near_miss_count")
        share = blocked / total if total else None
        components.append(
            {
                "component": gate,
                "blocked_count": blocked,
                "near_miss_count": near_miss,
                "blocked_share": round(share, 6) if share is not None else None,
                "median_margin_to_pass": None,
                "margin_evidence_status": "NOT_AVAILABLE_FROM_SOURCE",
                "impact_level": (
                    "HIGH"
                    if share is not None and share >= high_share
                    else "MEDIUM"
                    if blocked
                    else "LOW"
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    impact = {
        "schema_version": st.SCHEMA_VERSION,
        "components": components,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }

    scenarios = []
    for row in _records(sensitivity.get("threshold_scenarios")):
        scenarios.append(
            {
                "scenario": _text(row.get("scenario")),
                "promoted_count": _integer(row.get("promote_count"), "promote_count"),
                "high_risk_count": _integer(
                    row.get("high_risk_promote_count"), "high_risk_promote_count"
                ),
                "recommended": row.get("recommended") is True,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    authoritative = [row for row in scenarios if row.get("recommended")]
    relaxed = [row for row in scenarios if not row.get("recommended")]
    _require(len(authoritative) == 1 and bool(relaxed), "sensitivity scenarios invalid")
    if authoritative[0]["promoted_count"] == 0 and all(
        row["promoted_count"] == 0 for row in relaxed
    ):
        conclusion = "gate_not_primary_issue"
    elif any(row["promoted_count"] > 0 for row in relaxed):
        conclusion = "gate_may_be_too_strict"
    else:
        conclusion = "inconclusive"
    relaxed_result = {
        "schema_version": st.SCHEMA_VERSION,
        "diagnostic_only": True,
        "official_gate_changed": False,
        "scenarios": scenarios,
        "diagnostic_conclusion": conclusion,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    rule = _mapping(
        _mapping(_mapping(policy.get("gate_calibration")).get("diagnostic_conclusions")).get(
            conclusion
        )
    )
    bottlenecks = [
        row["component"]
        for row in sorted(
            components,
            key=lambda item: (-int(item["blocked_count"]), -int(item["near_miss_count"])),
        )
        if row["blocked_count"] > 0
    ][:4]
    diagnosis = {
        "schema_version": st.SCHEMA_VERSION,
        "gate_calibration_id": "",
        "source_no_promotion_review": review.get("review_id"),
        "original_gate_assessment": _text(summary.get("gate_assessment"), "INCONCLUSIVE"),
        "calibrated_assessment": _text(rule.get("calibrated_assessment")),
        "primary_gate_bottlenecks": bottlenecks,
        "recommendation": _text(rule.get("recommendation")),
        "can_change_official_gate": False,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return diagnosis, impact, relaxed_result


def _gate_material(
    *,
    root: Path,
    artifact_id: str,
    review: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    diagnosis, impact, relaxed = _gate_outputs(review, sensitivity, policy)
    diagnosis["gate_calibration_id"] = artifact_id
    matrix = _matrix_from_sensitivity(sensitivity)
    source_scorecard_id = _scorecard_id_from_review(review)
    _require(
        source_scorecard_id == _text(matrix.get("source_scorecard_id")),
        "review and sensitivity source scorecard mismatch",
    )
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_calibration_review_manifest",
        "gate_calibration_id": artifact_id,
        "source_no_promotion_review": review.get("review_id"),
        "threshold_sensitivity_id": sensitivity.get("sensitivity_id"),
        "source_scorecard_id": source_scorecard_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": review.get("market_regime", "ai_after_chatgpt"),
        "can_change_official_gate": False,
        "official_gate_changed": False,
        "diagnosis_policy_version": _policy_version(policy),
        "gate_calibration_manifest_path": str(root / GATE_VIEWS[0]),
        "gate_strictness_diagnosis_path": str(root / GATE_VIEWS[1]),
        "gate_component_impact_path": str(root / GATE_VIEWS[2]),
        "diagnostic_relaxed_gate_result_path": str(root / GATE_VIEWS[3]),
        "gate_calibration_review_report_path": str(root / GATE_VIEWS[4]),
        "reader_brief_section_path": str(root / GATE_VIEWS[5]),
        "gate_calibration_review_input_snapshot_path": str(
            root / "gate_calibration_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = render_gate_calibration_review_report(manifest, diagnosis, impact, relaxed)
    reader = render_gate_calibration_reader_brief(diagnosis, relaxed)
    views = {
        GATE_VIEWS[0]: foundation._json_bytes(manifest),
        GATE_VIEWS[1]: foundation._json_bytes(diagnosis),
        GATE_VIEWS[2]: foundation._json_bytes(impact),
        GATE_VIEWS[3]: foundation._json_bytes(relaxed),
        GATE_VIEWS[4]: foundation._text_file_bytes(report),
        GATE_VIEWS[5]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def run_gate_calibration_review(
    *,
    no_promotion_review_id: str,
    threshold_sensitivity_id: str,
    review_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    sensitivity_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    review = _validated_review(no_promotion_review_id, review_dir)
    sensitivity = _validated_sensitivity(threshold_sensitivity_id, sensitivity_dir)
    policy = _policy(policy_path)
    _chronology(generated, review, sensitivity)
    artifact_id = _stable_id(
        "gate-calibration-review",
        no_promotion_review_id,
        threshold_sensitivity_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _gate_material(
        root=root,
        artifact_id=root.name,
        review=review,
        sensitivity=sensitivity,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_expected_views(root, views)
    snapshot = {
        "schema_version": GATE_INPUT_SCHEMA,
        "gate_calibration_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "review_source": _binding(
            kind="no_promotion_review",
            artifact_id=no_promotion_review_id,
            root=review_dir / no_promotion_review_id,
            names=diagnostics.REVIEW_FILES,
        ),
        "sensitivity_source": _binding(
            kind="promotion_threshold_sensitivity",
            artifact_id=threshold_sensitivity_id,
            root=sensitivity_dir / threshold_sensitivity_id,
            names=followup.SENSITIVITY_FILES,
        ),
        "source_scorecard_id": manifest["source_scorecard_id"],
        "view_hashes": foundation._view_hashes(root, GATE_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "gate_calibration_review_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_gate_calibration_review", root.name, root / GATE_VIEWS[0])
    return {
        "gate_calibration_id": root.name,
        "gate_calibration_dir": root,
        "manifest": manifest,
        "gate_strictness_diagnosis": _read_json(root / GATE_VIEWS[1]),
        "gate_component_impact": _read_json(root / GATE_VIEWS[2]),
        "diagnostic_relaxed_gate_result": _read_json(root / GATE_VIEWS[3]),
        "reader_brief_section": (root / GATE_VIEWS[5]).read_text(encoding="utf-8"),
    }


def gate_calibration_review_report_payload(
    *,
    gate_calibration_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gate_calibration_id,
        latest_pointer="latest_gate_calibration_review",
        latest=latest,
        output_dir=output_dir,
        required_name=GATE_VIEWS[0],
    )
    return {
        **_read_json(root / GATE_VIEWS[0]),
        "gate_strictness_diagnosis": _read_json(root / GATE_VIEWS[1]),
        "gate_component_impact": _read_json(root / GATE_VIEWS[2]),
        "diagnostic_relaxed_gate_result": _read_json(root / GATE_VIEWS[3]),
        "reader_brief_section": (root / GATE_VIEWS[5]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "gate_calibration_review_input_snapshot.json"),
        "gate_calibration_dir": str(root),
    }


def _rebuild_gate(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "gate_calibration_review_input_snapshot.json")
    _require(snapshot.get("schema_version") == GATE_INPUT_SCHEMA, "gate snapshot schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    review_source = _mapping(snapshot.get("review_source"))
    sensitivity_source = _mapping(snapshot.get("sensitivity_source"))
    _validate_binding(review_source, kind="no_promotion_review")
    _validate_binding(sensitivity_source, kind="promotion_threshold_sensitivity")
    review = _validated_review(_source_id(review_source), _source_dir(review_source).parent)
    sensitivity = _validated_sensitivity(
        _source_id(sensitivity_source), _source_dir(sensitivity_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, review, sensitivity)
    manifest, expected = _gate_material(
        root=root,
        artifact_id=artifact_id,
        review=review,
        sensitivity=sensitivity,
        policy=policy,
        generated=generated,
    )
    _require(
        snapshot.get("source_scorecard_id") == manifest.get("source_scorecard_id"),
        "gate snapshot scorecard lineage",
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_gate_calibration_review_artifact(
    *, gate_calibration_id: str, output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / gate_calibration_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="gate_calibration_review_input_snapshot.json",
        schema=GATE_INPUT_SCHEMA,
        id_key="gate_calibration_id",
        artifact_id=gate_calibration_id,
        view_names=GATE_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_gate_calibration_review_validation", gate_calibration_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_gate_calibration_review_validation",
        artifact_id=gate_calibration_id,
        checks=checks,
        rebuild=lambda: _rebuild_gate(root, gate_calibration_id),
    )


def _component_score(row: Mapping[str, Any], component: str) -> float | None:
    scores = _mapping(row.get("score_components"))
    keys = {
        "return_score": "return",
        "drawdown_score": "drawdown",
        "volatility_score": "volatility",
        "turnover_score": "turnover",
        "rolling_consistency_score": "rolling_consistency",
        "signal_churn_score": "signal_churn",
        "weight_jump_score": "weight_jumps",
        "lag_cost_score": "strong_recovery_lag",
        "simplicity_score": "simplicity",
        "data_quality_score": "data_quality",
    }
    if component == "composite_score":
        return _optional_number(row.get("overall_score"))
    if component == "regime_score":
        values = [
            _optional_number(scores.get(name))
            for name in ("sideways_choppy", "tech_drawdown", "strong_recovery_lag")
        ]
        return sum(values) / len(values) if all(value is not None for value in values) else None
    return _optional_number(scores.get(keys.get(component, "")))


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _scorecard_outputs(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any], scorecard_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    rules = _mapping(policy.get("scorecard_attribution"))
    components = _texts(rules.get("components"))
    high = _number(rules.get("weakness_high_below"), "weakness high")
    medium = _number(rules.get("weakness_medium_below"), "weakness medium")
    distribution_rows: list[dict[str, Any]] = []
    for component in components:
        values = [value for row in rows if (value := _component_score(row, component)) is not None]
        median = _percentile(values, 0.5)
        weakness = (
            "INSUFFICIENT_DATA"
            if median is None
            else "HIGH"
            if median < high
            else "MEDIUM"
            if median < medium
            else "LOW"
        )
        distribution_rows.append(
            {
                "component": component,
                "observation_count": len(values),
                "missing_count": len(rows) - len(values),
                "mean": round(sum(values) / len(values), 6) if values else None,
                "median": round(median, 6) if median is not None else None,
                "p75": _round_optional(_percentile(values, 0.75)),
                "p90": _round_optional(_percentile(values, 0.9)),
                "weakness_level": weakness,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    available_rank = [row for row in distribution_rows if row.get("median") is not None]
    available_rank.sort(key=lambda row: (float(row["median"]), float(row["mean"])))
    count = _integer(rules.get("dominant_weak_component_count"), "dominant count")
    distribution = {
        "schema_version": st.SCHEMA_VERSION,
        "scorecard_id": scorecard_id,
        "variant_count": len(rows),
        "components": distribution_rows,
        "dominant_weak_components": [row["component"] for row in available_rank[:count]],
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    matrix: list[dict[str, Any]] = []
    for row in rows:
        score_map = {
            component: _round_optional(_component_score(row, component))
            for component in components
        }
        available = sorted(
            ((name, value) for name, value in score_map.items() if value is not None),
            key=lambda item: item[1],
        )
        families = _texts(row.get("families"))
        matrix.append(
            {
                "variant_id": row.get("variant_id"),
                "family": families[0] if families else "UNKNOWN",
                "families": families,
                "component_scores": score_map,
                "largest_weakness": available[0][0] if available else None,
                "secondary_weakness": available[1][0] if len(available) > 1 else None,
                "failure_pattern": "AGGREGATE_SCORE_DIAGNOSTIC_ONLY",
                "evidence_role": "AGGREGATE_SCORECARD",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    family_names = sorted({family for row in matrix for family in _texts(row.get("families"))})
    family_rows = []
    for family in family_names:
        selected = [row for row in matrix if family in _texts(row.get("families"))]
        weakness_counts = {
            name: sum(row.get("largest_weakness") == name for row in selected)
            for name in components
        }
        observed = {name: count for name, count in weakness_counts.items() if count}
        dominant = max(observed, key=observed.get) if observed else None
        best = max(
            selected,
            key=lambda row: _optional_number(
                _mapping(row.get("component_scores")).get("composite_score")
            )
            or -math.inf,
        )
        family_rows.append(
            {
                "family": family,
                "variant_count": len(selected),
                "dominant_weakness": dominant,
                "best_variant": best.get("variant_id"),
                "family_status": "AGGREGATE_DIAGNOSTIC_ONLY",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    family = {
        "schema_version": st.SCHEMA_VERSION,
        "families": family_rows,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return distribution, matrix, family


def _round_optional(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None else None


def _attribution_material(
    *,
    root: Path,
    artifact_id: str,
    scorecard: Mapping[str, Any],
    backfill: Mapping[str, Any],
    matrix_source: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    scorecard_id = _text(scorecard.get("scorecard_id"))
    _require(
        scorecard_id == _text(matrix_source.get("source_scorecard_id")),
        "scorecard and targeted matrix lineage mismatch",
    )
    _require(
        _text(backfill.get("v3_matrix_id")) == _text(matrix_source.get("v3_matrix_id")),
        "targeted backfill and matrix lineage mismatch",
    )
    rows = followup._targeted_v3_scorecard_rows(backfill, matrix_source)
    rejected = [
        row for row in rows if row.get("scorecard_decision") != "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ]
    distribution, component_matrix, family = _scorecard_outputs(rejected, policy, scorecard_id)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_scorecard_attribution_manifest",
        "scorecard_attribution_id": artifact_id,
        "scorecard_id": scorecard_id,
        "v3_backfill_id": backfill.get("v3_backfill_id"),
        "v3_matrix_id": matrix_source.get("v3_matrix_id"),
        "source_backfill_id": backfill.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if component_matrix else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_count": len(component_matrix),
        "source_batch2_scorecard_variant_count": len(_records(scorecard.get("variant_scorecard"))),
        "diagnosis_policy_version": _policy_version(policy),
        "scorecard_attribution_manifest_path": str(root / ATTRIBUTION_VIEWS[0]),
        "score_component_distribution_path": str(root / ATTRIBUTION_VIEWS[1]),
        "rejected_variant_component_matrix_path": str(root / ATTRIBUTION_VIEWS[2]),
        "family_component_weakness_path": str(root / ATTRIBUTION_VIEWS[3]),
        "scorecard_attribution_report_path": str(root / ATTRIBUTION_VIEWS[4]),
        "scorecard_attribution_input_snapshot_path": str(
            root / "scorecard_attribution_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = render_scorecard_attribution_report(manifest, distribution, family)
    views = {
        ATTRIBUTION_VIEWS[0]: foundation._json_bytes(manifest),
        ATTRIBUTION_VIEWS[1]: foundation._json_bytes(distribution),
        ATTRIBUTION_VIEWS[2]: foundation._jsonl_bytes(component_matrix),
        ATTRIBUTION_VIEWS[3]: foundation._json_bytes(family),
        ATTRIBUTION_VIEWS[4]: foundation._text_file_bytes(report),
    }
    return manifest, views


@with_artifact_validation_session
def run_scorecard_attribution(
    *,
    scorecard_id: str,
    v3_backfill_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    backfill = _validated_backfill(v3_backfill_id, v3_backfill_dir)
    matrix_id = _text(backfill.get("v3_matrix_id"))
    matrix_source = _validated_matrix(matrix_id, v3_matrix_dir)
    policy = _policy(policy_path)
    _chronology(generated, scorecard, backfill, matrix_source)
    artifact_id = _stable_id(
        "scorecard-attribution", scorecard_id, v3_backfill_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _attribution_material(
        root=root,
        artifact_id=root.name,
        scorecard=scorecard,
        backfill=backfill,
        matrix_source=matrix_source,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_expected_views(root, views)
    snapshot = {
        "schema_version": ATTRIBUTION_INPUT_SCHEMA,
        "scorecard_attribution_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=scorecard_dir / scorecard_id,
            names=evaluation.SCORECARD_FILES,
        ),
        "matrix_source": _binding(
            kind="targeted_search_v3",
            artifact_id=matrix_id,
            root=v3_matrix_dir / matrix_id,
            names=targeted.MATRIX_FILES,
        ),
        "backfill_source": _binding(
            kind="targeted_v3_backfill",
            artifact_id=v3_backfill_id,
            root=v3_backfill_dir / v3_backfill_id,
            names=targeted.BACKFILL_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, ATTRIBUTION_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "scorecard_attribution_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_scorecard_attribution", root.name, root / ATTRIBUTION_VIEWS[0])
    return {
        "scorecard_attribution_id": root.name,
        "scorecard_attribution_dir": root,
        "manifest": manifest,
        "score_component_distribution": _read_json(root / ATTRIBUTION_VIEWS[1]),
        "rejected_variant_component_matrix": _read_jsonl(root / ATTRIBUTION_VIEWS[2]),
        "family_component_weakness": _read_json(root / ATTRIBUTION_VIEWS[3]),
    }


def scorecard_attribution_report_payload(
    *,
    scorecard_attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=scorecard_attribution_id,
        latest_pointer="latest_scorecard_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name=ATTRIBUTION_VIEWS[0],
    )
    return {
        **_read_json(root / ATTRIBUTION_VIEWS[0]),
        "score_component_distribution": _read_json(root / ATTRIBUTION_VIEWS[1]),
        "rejected_variant_component_matrix": _read_jsonl(root / ATTRIBUTION_VIEWS[2]),
        "family_component_weakness": _read_json(root / ATTRIBUTION_VIEWS[3]),
        "input_snapshot": _read_json(root / "scorecard_attribution_input_snapshot.json"),
        "scorecard_attribution_dir": str(root),
    }


def _rebuild_attribution(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "scorecard_attribution_input_snapshot.json")
    _require(snapshot.get("schema_version") == ATTRIBUTION_INPUT_SCHEMA, "attribution schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    score_source = _mapping(snapshot.get("scorecard_source"))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(matrix_source, kind="targeted_search_v3")
    _validate_binding(backfill_source, kind="targeted_v3_backfill")
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    matrix_payload = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    backfill = _validated_backfill(_source_id(backfill_source), _source_dir(backfill_source).parent)
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, scorecard, matrix_payload, backfill)
    _, expected = _attribution_material(
        root=root,
        artifact_id=artifact_id,
        scorecard=scorecard,
        backfill=backfill,
        matrix_source=matrix_payload,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_scorecard_attribution_artifact(
    *,
    scorecard_attribution_id: str,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / scorecard_attribution_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="scorecard_attribution_input_snapshot.json",
        schema=ATTRIBUTION_INPUT_SCHEMA,
        id_key="scorecard_attribution_id",
        artifact_id=scorecard_attribution_id,
        view_names=ATTRIBUTION_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_scorecard_attribution_validation",
            scorecard_attribution_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_scorecard_attribution_validation",
        artifact_id=scorecard_attribution_id,
        checks=checks,
        rebuild=lambda: _rebuild_attribution(root, scorecard_attribution_id),
    )


def _exact_source_row(
    rows_by_id: Mapping[str, Mapping[str, Any]], method_policy: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    for variant_id in _texts(method_policy.get("exact_variant_ids")):
        if variant_id in rows_by_id:
            return rows_by_id[variant_id]
    return None


def _signal_outputs(
    attribution: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    matrix = _records(attribution.get("rejected_variant_component_matrix"))
    rows_by_id = {_text(row.get("variant_id")): row for row in matrix}
    rules = _mapping(policy.get("signal_diagnosis"))
    methods: list[dict[str, Any]] = []
    for method_name, raw in _mapping(rules.get("methods")).items():
        source = _exact_source_row(rows_by_id, _mapping(raw))
        methods.append(
            {
                "method": method_name,
                "source_variant_id": source.get("variant_id") if source else None,
                "aggregate_component_scores": (
                    _mapping(source.get("component_scores")) if source else {}
                ),
                "aggregate_proxy_available": source is not None,
                "evidence_role": _text(rules.get("evidence_role")),
                "dated_signal_ledger_available": False,
                "direction_flip_count": None,
                "risk_asset_flip_count": None,
                "semiconductor_flip_count": None,
                "large_weight_jump_count": None,
                "avg_consensus_dispersion": None,
                "max_consensus_dispersion": None,
                "false_risk_on_count": None,
                "false_risk_off_count": None,
                "signal_stability_status": _text(rules.get("no_dated_ledger_status")),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "signal_diagnosis_id": "",
        "dominant_signal_issue": "INSUFFICIENT_DATED_SIGNAL_EVIDENCE",
        "affected_methods": [row["method"] for row in methods if row["aggregate_proxy_available"]],
        "parameter_search_likely_sufficient": None,
        "requires_signal_level_fix": None,
        "evidence_status": "INSUFFICIENT_DATA",
        "dated_signal_event_count": 0,
        "regime_mismatch_event_count": 0,
        "recommended_next_action": _text(rules.get("no_dated_ledger_recommended_action")),
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return methods, [], [], summary


def _signal_material(
    *,
    root: Path,
    artifact_id: str,
    attribution: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    methods, flips, mismatches, summary = _signal_outputs(attribution, policy)
    summary["signal_diagnosis_id"] = artifact_id
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_instability_manifest",
        "signal_diagnosis_id": artifact_id,
        "scorecard_attribution_id": attribution.get("scorecard_attribution_id"),
        "scorecard_id": attribution.get("scorecard_id"),
        "v3_backfill_id": attribution.get("v3_backfill_id"),
        "v3_matrix_id": attribution.get("v3_matrix_id"),
        "source_backfill_id": attribution.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "date_start": attribution.get("date_start"),
        "date_end": attribution.get("date_end"),
        "diagnosis_policy_version": _policy_version(policy),
        "signal_instability_manifest_path": str(root / SIGNAL_VIEWS[0]),
        "method_signal_stability_path": str(root / SIGNAL_VIEWS[1]),
        "signal_flip_events_path": str(root / SIGNAL_VIEWS[2]),
        "regime_mismatch_events_path": str(root / SIGNAL_VIEWS[3]),
        "signal_instability_summary_path": str(root / SIGNAL_VIEWS[4]),
        "signal_instability_diagnosis_report_path": str(root / SIGNAL_VIEWS[5]),
        "reader_brief_section_path": str(root / SIGNAL_VIEWS[6]),
        "signal_instability_diagnosis_input_snapshot_path": str(
            root / "signal_instability_diagnosis_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = render_signal_instability_report(manifest, methods, summary)
    reader = render_signal_instability_reader_brief(summary)
    views = {
        SIGNAL_VIEWS[0]: foundation._json_bytes(manifest),
        SIGNAL_VIEWS[1]: foundation._jsonl_bytes(methods),
        SIGNAL_VIEWS[2]: foundation._jsonl_bytes(flips),
        SIGNAL_VIEWS[3]: foundation._jsonl_bytes(mismatches),
        SIGNAL_VIEWS[4]: foundation._json_bytes(summary),
        SIGNAL_VIEWS[5]: foundation._text_file_bytes(report),
        SIGNAL_VIEWS[6]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def run_signal_instability_diagnosis(
    *,
    scorecard_attribution_id: str,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    attribution = _validated_attribution(scorecard_attribution_id, attribution_dir)
    policy = _policy(policy_path)
    _chronology(generated, attribution)
    artifact_id = _stable_id(
        "signal-instability-diagnosis", scorecard_attribution_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _signal_material(
        root=root,
        artifact_id=root.name,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_expected_views(root, views)
    snapshot = {
        "schema_version": SIGNAL_INPUT_SCHEMA,
        "signal_diagnosis_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "attribution_source": _binding(
            kind="scorecard_attribution",
            artifact_id=scorecard_attribution_id,
            root=attribution_dir / scorecard_attribution_id,
            names=ATTRIBUTION_FILES,
        ),
        "dated_signal_ledger_source": None,
        "view_hashes": foundation._view_hashes(root, SIGNAL_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "signal_instability_diagnosis_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_signal_instability_diagnosis", root.name, root / SIGNAL_VIEWS[0])
    return {
        "signal_diagnosis_id": root.name,
        "signal_diagnosis_dir": root,
        "manifest": manifest,
        "method_signal_stability": _read_jsonl(root / SIGNAL_VIEWS[1]),
        "signal_flip_events": [],
        "regime_mismatch_events": [],
        "signal_instability_summary": _read_json(root / SIGNAL_VIEWS[4]),
        "reader_brief_section": (root / SIGNAL_VIEWS[6]).read_text(encoding="utf-8"),
    }


def signal_instability_diagnosis_report_payload(
    *,
    signal_diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=signal_diagnosis_id,
        latest_pointer="latest_signal_instability_diagnosis",
        latest=latest,
        output_dir=output_dir,
        required_name=SIGNAL_VIEWS[0],
    )
    return {
        **_read_json(root / SIGNAL_VIEWS[0]),
        "method_signal_stability": _read_jsonl(root / SIGNAL_VIEWS[1]),
        "signal_flip_events": _read_jsonl(root / SIGNAL_VIEWS[2]),
        "regime_mismatch_events": _read_jsonl(root / SIGNAL_VIEWS[3]),
        "signal_instability_summary": _read_json(root / SIGNAL_VIEWS[4]),
        "reader_brief_section": (root / SIGNAL_VIEWS[6]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "signal_instability_diagnosis_input_snapshot.json"),
        "signal_diagnosis_dir": str(root),
    }


def _rebuild_signal(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_instability_diagnosis_input_snapshot.json")
    _require(snapshot.get("schema_version") == SIGNAL_INPUT_SCHEMA, "signal snapshot schema")
    _require(snapshot.get("dated_signal_ledger_source") is None, "unexpected signal ledger source")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    attribution_source = _mapping(snapshot.get("attribution_source"))
    _validate_binding(attribution_source, kind="scorecard_attribution")
    attribution = _validated_attribution(
        _source_id(attribution_source), _source_dir(attribution_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, attribution)
    _, expected = _signal_material(
        root=root,
        artifact_id=artifact_id,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_signal_instability_diagnosis_artifact(
    *,
    signal_diagnosis_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    root = output_dir / signal_diagnosis_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="signal_instability_diagnosis_input_snapshot.json",
        schema=SIGNAL_INPUT_SCHEMA,
        id_key="signal_diagnosis_id",
        artifact_id=signal_diagnosis_id,
        view_names=SIGNAL_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_signal_instability_diagnosis_validation",
            signal_diagnosis_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_signal_instability_diagnosis_validation",
        artifact_id=signal_diagnosis_id,
        checks=checks,
        rebuild=lambda: _rebuild_signal(root, signal_diagnosis_id),
    )


def _consensus_outputs(
    signal: Mapping[str, Any],
    attribution: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    rows_by_id = {
        _text(row.get("variant_id")): row
        for row in _records(attribution.get("rejected_variant_component_matrix"))
    }
    rules = _mapping(policy.get("consensus_quality"))
    methods = []
    for method_name, raw in _mapping(rules.get("methods")).items():
        source = _exact_source_row(rows_by_id, _mapping(raw))
        methods.append(
            {
                "ensemble_method": method_name,
                "source_variant_id": source.get("variant_id") if source else None,
                "exact_source_available": source is not None,
                "return_delta_vs_limited": None,
                "drawdown_delta_vs_limited": None,
                "turnover_delta_vs_limited": None,
                "dispersion_sensitivity": "INSUFFICIENT_DATA",
                "quality_status": _text(rules.get("missing_method_status")),
                "failure_reason": "dated_candidate_weight_path_not_available",
                "aggregate_component_scores": (
                    _mapping(source.get("component_scores")) if source else {}
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    dispersion = {
        "schema_version": st.SCHEMA_VERSION,
        "avg_candidate_dispersion": None,
        "max_candidate_dispersion": None,
        "high_disagreement_days": None,
        "high_disagreement_regimes": [],
        "dispersion_status": _text(rules.get("no_dated_dispersion_status")),
        "reason": _text(rules.get("no_dated_dispersion_reason")),
        "dated_candidate_weight_path_available": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    failure = {
        "schema_version": st.SCHEMA_VERSION,
        "primary_failure_reason": "insufficient_dated_consensus_evidence",
        "recommended_fix": "build_validated_candidate_weight_path_panel",
        "signal_evidence_status": signal.get("evidence_status"),
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return dispersion, methods, failure


def _consensus_material(
    *,
    root: Path,
    artifact_id: str,
    signal: Mapping[str, Any],
    attribution: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    _require(
        _text(signal.get("scorecard_attribution_id"))
        == _text(attribution.get("scorecard_attribution_id")),
        "signal and attribution lineage mismatch",
    )
    dispersion, quality, failure = _consensus_outputs(signal, attribution, policy)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_quality_manifest",
        "consensus_review_id": artifact_id,
        "signal_diagnosis_id": signal.get("signal_diagnosis_id"),
        "scorecard_attribution_id": signal.get("scorecard_attribution_id"),
        "v3_backfill_id": signal.get("v3_backfill_id"),
        "source_backfill_id": signal.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        "market_regime": signal.get("market_regime", "ai_after_chatgpt"),
        "diagnosis_policy_version": _policy_version(policy),
        "consensus_quality_manifest_path": str(root / CONSENSUS_VIEWS[0]),
        "consensus_dispersion_summary_path": str(root / CONSENSUS_VIEWS[1]),
        "ensemble_method_quality_path": str(root / CONSENSUS_VIEWS[2]),
        "consensus_failure_reasons_path": str(root / CONSENSUS_VIEWS[3]),
        "consensus_quality_review_report_path": str(root / CONSENSUS_VIEWS[4]),
        "reader_brief_section_path": str(root / CONSENSUS_VIEWS[5]),
        "consensus_quality_review_input_snapshot_path": str(
            root / "consensus_quality_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = render_consensus_quality_report(manifest, dispersion, quality, failure)
    reader = render_consensus_quality_reader_brief(failure)
    views = {
        CONSENSUS_VIEWS[0]: foundation._json_bytes(manifest),
        CONSENSUS_VIEWS[1]: foundation._json_bytes(dispersion),
        CONSENSUS_VIEWS[2]: foundation._jsonl_bytes(quality),
        CONSENSUS_VIEWS[3]: foundation._json_bytes(failure),
        CONSENSUS_VIEWS[4]: foundation._text_file_bytes(report),
        CONSENSUS_VIEWS[5]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def run_consensus_quality_review(
    *,
    signal_diagnosis_id: str,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    signal = _validated_signal(signal_diagnosis_id, signal_dir)
    attribution_id = _text(signal.get("scorecard_attribution_id"))
    attribution = _validated_attribution(attribution_id, attribution_dir)
    policy = _policy(policy_path)
    _chronology(generated, signal, attribution)
    artifact_id = _stable_id("consensus-quality-review", signal_diagnosis_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _consensus_material(
        root=root,
        artifact_id=root.name,
        signal=signal,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_expected_views(root, views)
    snapshot = {
        "schema_version": CONSENSUS_INPUT_SCHEMA,
        "consensus_review_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "signal_source": _binding(
            kind="signal_instability_diagnosis",
            artifact_id=signal_diagnosis_id,
            root=signal_dir / signal_diagnosis_id,
            names=SIGNAL_FILES,
        ),
        "attribution_source": _binding(
            kind="scorecard_attribution",
            artifact_id=attribution_id,
            root=attribution_dir / attribution_id,
            names=ATTRIBUTION_FILES,
        ),
        "dated_candidate_weight_path_source": None,
        "view_hashes": foundation._view_hashes(root, CONSENSUS_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "consensus_quality_review_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_consensus_quality_review", root.name, root / CONSENSUS_VIEWS[0])
    return {
        "consensus_review_id": root.name,
        "consensus_review_dir": root,
        "manifest": manifest,
        "consensus_dispersion_summary": _read_json(root / CONSENSUS_VIEWS[1]),
        "ensemble_method_quality": _read_jsonl(root / CONSENSUS_VIEWS[2]),
        "consensus_failure_reasons": _read_json(root / CONSENSUS_VIEWS[3]),
        "reader_brief_section": (root / CONSENSUS_VIEWS[5]).read_text(encoding="utf-8"),
    }


def consensus_quality_review_report_payload(
    *,
    consensus_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=consensus_review_id,
        latest_pointer="latest_consensus_quality_review",
        latest=latest,
        output_dir=output_dir,
        required_name=CONSENSUS_VIEWS[0],
    )
    return {
        **_read_json(root / CONSENSUS_VIEWS[0]),
        "consensus_dispersion_summary": _read_json(root / CONSENSUS_VIEWS[1]),
        "ensemble_method_quality": _read_jsonl(root / CONSENSUS_VIEWS[2]),
        "consensus_failure_reasons": _read_json(root / CONSENSUS_VIEWS[3]),
        "reader_brief_section": (root / CONSENSUS_VIEWS[5]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "consensus_quality_review_input_snapshot.json"),
        "consensus_review_dir": str(root),
    }


def _rebuild_consensus(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "consensus_quality_review_input_snapshot.json")
    _require(snapshot.get("schema_version") == CONSENSUS_INPUT_SCHEMA, "consensus snapshot schema")
    _require(
        snapshot.get("dated_candidate_weight_path_source") is None,
        "unexpected dated candidate weight source",
    )
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    signal_source = _mapping(snapshot.get("signal_source"))
    attribution_source = _mapping(snapshot.get("attribution_source"))
    _validate_binding(signal_source, kind="signal_instability_diagnosis")
    _validate_binding(attribution_source, kind="scorecard_attribution")
    signal = _validated_signal(_source_id(signal_source), _source_dir(signal_source).parent)
    attribution = _validated_attribution(
        _source_id(attribution_source), _source_dir(attribution_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, signal, attribution)
    _, expected = _consensus_material(
        root=root,
        artifact_id=artifact_id,
        signal=signal,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_consensus_quality_review_artifact(
    *,
    consensus_review_id: str,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / consensus_review_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="consensus_quality_review_input_snapshot.json",
        schema=CONSENSUS_INPUT_SCHEMA,
        id_key="consensus_review_id",
        artifact_id=consensus_review_id,
        view_names=CONSENSUS_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_consensus_quality_review_validation",
            consensus_review_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_consensus_quality_review_validation",
        artifact_id=consensus_review_id,
        checks=checks,
        rebuild=lambda: _rebuild_consensus(root, consensus_review_id),
    )


def _write_expected_views(root: Path, views: Mapping[str, bytes]) -> None:
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def render_gate_calibration_review_report(
    manifest: Mapping[str, Any],
    diagnosis: Mapping[str, Any],
    impact: Mapping[str, Any],
    relaxed: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Gate Calibration Review {manifest.get('gate_calibration_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- source scorecard：{manifest.get('source_scorecard_id')}",
            f"- calibrated assessment：{diagnosis.get('calibrated_assessment')}",
            f"- diagnostic conclusion：{relaxed.get('diagnostic_conclusion')}",
            f"- component count：{len(_records(impact.get('components')))}",
            "- official gate changed：false",
            "- 结论边界：仅research diagnostic，不修改official gate或production state。",
            "",
        ]
    )


def render_gate_calibration_reader_brief(
    diagnosis: Mapping[str, Any], relaxed: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "## Gate Calibration Review",
            "",
            f"- calibrated_assessment: {diagnosis.get('calibrated_assessment')}",
            f"- diagnostic_conclusion: {relaxed.get('diagnostic_conclusion')}",
            "- official_gate_changed: false",
            "",
        ]
    )


def render_scorecard_attribution_report(
    manifest: Mapping[str, Any], distribution: Mapping[str, Any], family: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Scorecard Attribution {manifest.get('scorecard_attribution_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- variants：{manifest.get('variant_count')}",
            "- dominant weak components："
            + ",".join(_texts(distribution.get("dominant_weak_components"))),
            f"- family count：{len(_records(family.get('families')))}",
            "- evidence role：aggregate scorecard diagnosis only。",
            "",
        ]
    )


def render_signal_instability_report(
    manifest: Mapping[str, Any], methods: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Signal Instability Diagnosis {manifest.get('signal_diagnosis_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- method_count：{len(methods)}",
            "- dated signal events：0（source未提供validated dated signal ledger）",
            "- requires_signal_level_fix：null",
            f"- recommended_next_action：{summary.get('recommended_next_action')}",
            "",
        ]
    )


def render_signal_instability_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Instability Diagnosis",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            "- requires_signal_level_fix: null",
            f"- next_action: {summary.get('recommended_next_action')}",
            "",
        ]
    )


def render_consensus_quality_report(
    manifest: Mapping[str, Any],
    dispersion: Mapping[str, Any],
    quality: Sequence[Mapping[str, Any]],
    failure: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Consensus Quality Review {manifest.get('consensus_review_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- dispersion_status：{dispersion.get('dispersion_status')}",
            f"- method_count：{len(quality)}",
            f"- primary_failure_reason：{failure.get('primary_failure_reason')}",
            "- 结论边界：无validated dated candidate weight path，不报告伪造dispersion或quality。",
            "",
        ]
    )


def render_consensus_quality_reader_brief(failure: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Consensus Quality Review",
            "",
            f"- primary_failure_reason: {failure.get('primary_failure_reason')}",
            f"- recommended_fix: {failure.get('recommended_fix')}",
            "",
        ]
    )


__all__ = [
    "ATTRIBUTION_FILES",
    "CONSENSUS_FILES",
    "DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR",
    "DEFAULT_GATE_CALIBRATION_REVIEW_DIR",
    "DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH",
    "DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR",
    "DEFAULT_SCORECARD_ATTRIBUTION_DIR",
    "GATE_FILES",
    "SIGNAL_FILES",
    "consensus_quality_review_report_payload",
    "gate_calibration_review_report_payload",
    "run_consensus_quality_review",
    "run_gate_calibration_review",
    "run_scorecard_attribution",
    "run_signal_instability_diagnosis",
    "scorecard_attribution_report_payload",
    "signal_instability_diagnosis_report_payload",
    "validate_consensus_quality_review_artifact",
    "validate_gate_calibration_review_artifact",
    "validate_scorecard_attribution_artifact",
    "validate_signal_instability_diagnosis_artifact",
]
