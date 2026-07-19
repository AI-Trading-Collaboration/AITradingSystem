from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_pipeline as upstream,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as diagnosis_foundation,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_FILTERED_FORMALIZATION_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "filtered_formalization_policy_v1.yaml"
)
DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR = upstream.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR
DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR = (
    upstream.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR
)
DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_evidence"
)
DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "median_regime_filter_spec"
)
DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_stress_backfill"
)
DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "drawdown_mismatch_reduction"
)
DEFAULT_FLIP_ROTATION_REDUCTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "flip_rotation_reduction"
)
DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_ab_review"
)
DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_gate_confirmation"
)
DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_formalization_readiness"
)
DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_filtered_candidate_review"
)
DEFAULT_FILTERED_NEXT_DECISION_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_next_decision"

TOP_FILTERED_CANDIDATE = "median_plus_regime_mismatch_filter"

EVIDENCE_INPUT_SCHEMA = "filtered_candidate_evidence_input_snapshot.v2"
SPEC_INPUT_SCHEMA = "median_regime_filter_spec_input_snapshot.v2"
STRESS_INPUT_SCHEMA = "filtered_candidate_stress_backfill_input_snapshot.v2"
MISMATCH_INPUT_SCHEMA = "drawdown_mismatch_reduction_input_snapshot.v2"
FLIP_INPUT_SCHEMA = "flip_rotation_reduction_input_snapshot.v2"
AB_INPUT_SCHEMA = "filtered_candidate_ab_review_input_snapshot.v2"
CONFIRMATION_INPUT_SCHEMA = "signal_gate_confirmation_input_snapshot.v2"
READINESS_INPUT_SCHEMA = "filtered_formalization_readiness_input_snapshot.v2"
OWNER_INPUT_SCHEMA = "owner_filtered_candidate_review_input_snapshot.v2"
DECISION_INPUT_SCHEMA = "filtered_next_decision_input_snapshot.v2"

EVIDENCE_VIEWS = (
    "filtered_candidate_evidence_manifest.json",
    "filtered_candidate_evidence_summary.json",
    "evidence_component_breakdown.json",
    "evidence_strength_weakness_matrix.json",
    "filtered_candidate_evidence_report.md",
    "reader_brief_section.md",
)
SPEC_VIEWS = (
    "median_regime_filter_spec_manifest.json",
    "median_regime_filter_spec.yaml",
    "median_regime_filter_contract.json",
    "median_regime_filter_spec_report.md",
)
STRESS_VIEWS = (
    "filtered_candidate_stress_manifest.json",
    "stress_window_inventory.jsonl",
    "stress_window_metrics.jsonl",
    "filtered_candidate_stress_summary.json",
    "filtered_candidate_stress_report.md",
)
MISMATCH_VIEWS = (
    "drawdown_mismatch_reduction_manifest.json",
    "mismatch_reduction_events.jsonl",
    "mismatch_reduction_summary.json",
    "drawdown_mismatch_reduction_report.md",
)
FLIP_VIEWS = (
    "flip_rotation_reduction_manifest.json",
    "flip_rotation_events.jsonl",
    "flip_rotation_reduction_summary.json",
    "flip_rotation_reduction_report.md",
)
AB_VIEWS = (
    "filtered_candidate_ab_manifest.json",
    "ab_method_comparison.jsonl",
    "ab_summary.json",
    "filtered_candidate_ab_report.md",
    "reader_brief_section.md",
)
CONFIRMATION_VIEWS = (
    "signal_gate_confirmation_manifest.json",
    "signal_gate_confirmation_targets.json",
    "signal_gate_confirmation_report.md",
    "reader_brief_section.md",
)
READINESS_VIEWS = (
    "filtered_formalization_manifest.json",
    "formalization_readiness_decision.json",
    "formalization_blockers.json",
    "filtered_formalization_report.md",
    "reader_brief_section.md",
)
OWNER_VIEWS = (
    "owner_filtered_candidate_manifest.json",
    "owner_filtered_candidate_summary.json",
    "owner_filtered_candidate_checklist.md",
    "owner_filtered_candidate_review_report.md",
    "reader_brief_section.md",
)
DECISION_VIEWS = (
    "filtered_next_decision_manifest.json",
    "filtered_next_decision.json",
    "next_task_plan.json",
    "filtered_next_decision_report.md",
    "reader_brief_section.md",
)

EVIDENCE_FILES = (*EVIDENCE_VIEWS, "filtered_candidate_evidence_input_snapshot.json")
SPEC_FILES = (*SPEC_VIEWS, "median_regime_filter_spec_input_snapshot.json")
STRESS_FILES = (*STRESS_VIEWS, "filtered_candidate_stress_backfill_input_snapshot.json")
MISMATCH_FILES = (*MISMATCH_VIEWS, "drawdown_mismatch_reduction_input_snapshot.json")
FLIP_FILES = (*FLIP_VIEWS, "flip_rotation_reduction_input_snapshot.json")
AB_FILES = (*AB_VIEWS, "filtered_candidate_ab_review_input_snapshot.json")
CONFIRMATION_FILES = (*CONFIRMATION_VIEWS, "signal_gate_confirmation_input_snapshot.json")
READINESS_FILES = (*READINESS_VIEWS, "filtered_formalization_readiness_input_snapshot.json")
OWNER_FILES = (*OWNER_VIEWS, "owner_filtered_candidate_review_input_snapshot.json")
DECISION_FILES = (*DECISION_VIEWS, "filtered_next_decision_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text
_float = foundation._float
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_write_latest_pointer = foundation._write_latest_pointer
_read_json = foundation._read_json
_read_jsonl = foundation._read_jsonl
_artifact_dir = foundation._artifact_dir
_validation_payload = foundation._validation_payload


class DynamicV3FilteredCandidateReadinessPipelineError(ValueError):
    """Raised when EB3 evidence is not source-derived or reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3FilteredCandidateReadinessPipelineError(message)


def _generated_time(value: datetime | None) -> datetime:
    try:
        return upstream._generated_time(value)
    except ValueError as exc:
        raise DynamicV3FilteredCandidateReadinessPipelineError(str(exc)) from exc


def _aware_time(value: Any, field: str) -> datetime:
    try:
        return upstream._aware_time(value, field)
    except ValueError as exc:
        raise DynamicV3FilteredCandidateReadinessPipelineError(str(exc)) from exc


def _chronology(generated: datetime, *payloads: Mapping[str, Any]) -> None:
    try:
        upstream._chronology(generated, *payloads)
    except ValueError as exc:
        raise DynamicV3FilteredCandidateReadinessPipelineError(str(exc)) from exc


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return upstream._binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _binding_id(binding: Mapping[str, Any]) -> str:
    return upstream._binding_id(binding)


def _binding_root(binding: Mapping[str, Any]) -> Path:
    return upstream._binding_root(binding)


def _validate_binding(binding: Mapping[str, Any], *, kind: str, names: Sequence[str]) -> None:
    try:
        upstream._validate_binding(binding, kind=kind, names=names)
    except ValueError as exc:
        raise DynamicV3FilteredCandidateReadinessPipelineError(str(exc)) from exc


def _policy(path: Path = DEFAULT_FILTERED_FORMALIZATION_POLICY_PATH) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    _require(isinstance(payload, Mapping), "filtered formalization policy must be a mapping")
    policy = dict(payload)
    _require(policy.get("schema_version") == "filtered_formalization_policy.v1", "policy schema")
    _require(policy.get("status") == "reviewed_baseline", "policy status")
    rules = _mapping(policy.get("formalization_rules"))
    _require(rules.get("allow_official_target_weights") is False, "policy official weights")
    _require(rules.get("allow_automatic_promotion") is False, "policy automatic promotion")
    _require(rules.get("allow_broker_action") is False, "policy broker action")
    _require(rules.get("production_effect") == "none", "policy production effect")
    return policy


def _policy_source(path: Path = DEFAULT_FILTERED_FORMALIZATION_POLICY_PATH) -> dict[str, Any]:
    _policy(path)
    return foundation._file_binding(path)


def _policy_from_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    foundation._validate_file_binding(binding)
    return _policy(Path(_text(binding.get("path"))))


def _optional_snapshot(root: Path, name: str) -> dict[str, Any]:
    path = root / name
    return {"input_snapshot": _read_json(path)} if path.is_file() else {}


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    upstream._write_views(root, views)


def _run_result(
    payload: Mapping[str, Any],
    *,
    manifest_path_key: str,
    legacy_directory_key: str,
    current_directory_key: str,
) -> dict[str, Any]:
    """Preserve the legacy writer shape while exposing the v2 fields."""

    result = dict(payload)
    result["manifest"] = _read_json(Path(_text(payload.get(manifest_path_key))))
    result[legacy_directory_key] = Path(_text(payload.get(current_directory_key)))
    return result


def _safety() -> dict[str, Any]:
    return {
        **st.EXPERIMENT_FACTORY_SAFETY,
        "manual_review_required": True,
        "automatic_candidate_promotion": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _manifest_context(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "market_regime": source.get("market_regime", "ai_after_chatgpt"),
        "date_start": source.get("date_start"),
        "date_end": source.get("date_end"),
        "data_quality_status": source.get("data_quality_status"),
        "policy_version": source.get("policy_version"),
    }


def _validated_upstream(
    *,
    artifact_id: str,
    output_dir: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    reader: Callable[..., dict[str, Any]],
    reader_key: str,
    label: str,
) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validator,
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", f"{label} source validation failed")
    payload = reader(**{reader_key: artifact_id, "output_dir": output_dir})
    _require(_text(payload.get(reader_key)) == artifact_id, f"{label} id mismatch")
    return payload


def _validated_comparison(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_upstream(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=upstream.validate_filtered_vs_original_comparison_artifact,
        validator_key="comparison_id",
        reader=upstream.filtered_vs_original_comparison_report_payload,
        reader_key="comparison_id",
        label="filtered comparison",
    )


def _validated_promotion_review(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_upstream(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=upstream.validate_filtered_candidate_promotion_review_artifact,
        validator_key="filtered_review_id",
        reader=upstream.filtered_candidate_promotion_review_report_payload,
        reader_key="filtered_review_id",
        label="promotion review",
    )


def _source_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(payload.get("input_snapshot")).get("policy_source"))


def _lineage_match(
    left: Mapping[str, Any], right: Mapping[str, Any], fields: Sequence[str]
) -> None:
    for field in fields:
        _require(left.get(field) == right.get(field), f"source {field} lineage mismatch")


def _observed_candidate_rows(candidate: str, comparison: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _records(comparison.get("filtered_comparison_matrix")):
        if row.get("variant_id") != candidate:
            continue
        metrics = (
            row.get("return_delta_vs_base"),
            row.get("drawdown_delta_vs_base"),
            row.get("signal_churn_delta_vs_base"),
            row.get("regime_mismatch_delta_vs_base"),
        )
        if row.get("outcome_sample_count") and all(value is not None for value in metrics):
            rows.append(dict(row))
    return rows


def _evidence_material(
    *,
    root: Path,
    artifact_id: str,
    candidate: str,
    comparison: Mapping[str, Any],
    review: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    observed = _observed_candidate_rows(candidate, comparison)
    decision = _mapping(review.get("filtered_promotion_decision"))
    candidate_matches = decision.get("best_filtered_variant") == candidate
    sufficient = (
        bool(observed)
        and candidate_matches
        and decision.get("evidence_status")
        not in {
            "INSUFFICIENT_DATA",
            None,
        }
    )
    components = [
        {
            "schema_version": st.SCHEMA_VERSION,
            "component": field,
            "observed_value": row.get(field),
            "outcome_sample_count": row.get("outcome_sample_count"),
            "metric_source": row.get("metric_source"),
            **_safety(),
        }
        for row in observed
        for field in (
            "return_delta_vs_base",
            "drawdown_delta_vs_base",
            "signal_churn_delta_vs_base",
            "regime_mismatch_delta_vs_base",
        )
    ]
    status = "SUFFICIENT_FOR_RESEARCH_REVIEW" if sufficient else "INSUFFICIENT_DATA"
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "evidence_status": status,
        "observed_comparison_row_count": len(observed),
        "primary_improvements": [],
        "primary_weaknesses": ([] if sufficient else ["validated_dated_filtered_outcomes_missing"]),
        "requires_more_evidence": not sufficient,
        "source_recommendation": decision.get("recommended_next_action") if sufficient else None,
        "promotion_review_decision": decision.get("decision") if sufficient else None,
        **_safety(),
    }
    breakdown = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "evidence_status": status,
        "components": components,
        **_safety(),
    }
    matrix = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "evidence_status": status,
        "primary_improvements": [],
        "primary_weaknesses": summary["primary_weaknesses"],
        "comparison_rows": observed,
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_evidence_manifest",
        "evidence_id": artifact_id,
        "candidate": candidate,
        "filtered_comparison_id": comparison.get("comparison_id"),
        "promotion_review_id": review.get("filtered_review_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if sufficient else "PASS_WITH_WARNINGS",
        "evidence_status": status,
        **_manifest_context(comparison),
        "filtered_candidate_evidence_manifest_path": str(root / EVIDENCE_VIEWS[0]),
        "filtered_candidate_evidence_summary_path": str(root / EVIDENCE_VIEWS[1]),
        "evidence_component_breakdown_path": str(root / EVIDENCE_VIEWS[2]),
        "evidence_strength_weakness_matrix_path": str(root / EVIDENCE_VIEWS[3]),
        "filtered_candidate_evidence_report_path": str(root / EVIDENCE_VIEWS[4]),
        "reader_brief_section_path": str(root / EVIDENCE_VIEWS[5]),
        "filtered_candidate_evidence_input_snapshot_path": str(
            root / "filtered_candidate_evidence_input_snapshot.json"
        ),
        **_safety(),
    }
    report = render_filtered_candidate_evidence_report(manifest, summary, breakdown, matrix)
    reader = render_filtered_candidate_evidence_reader_brief(summary)
    views = {
        EVIDENCE_VIEWS[0]: foundation._json_bytes(manifest),
        EVIDENCE_VIEWS[1]: foundation._json_bytes(summary),
        EVIDENCE_VIEWS[2]: foundation._json_bytes(breakdown),
        EVIDENCE_VIEWS[3]: foundation._json_bytes(matrix),
        EVIDENCE_VIEWS[4]: foundation._text_file_bytes(report),
        EVIDENCE_VIEWS[5]: foundation._text_file_bytes(reader),
    }
    return manifest, views


def render_filtered_candidate_evidence_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    breakdown: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> str:
    _ = matrix
    return "\n".join(
        [
            f"# Filtered Candidate Evidence {manifest.get('evidence_id')}",
            "",
            f"- candidate：{summary.get('candidate')}",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- observed_comparison_row_count：{summary.get('observed_comparison_row_count')}",
            f"- component_count：{len(_records(breakdown.get('components')))}",
            "- 解释边界：aggregate proxy、spec和registered target均不是observed performance。",
            "- safety：manual review / no official weights / no broker / no production",
            "",
        ]
    )


def render_filtered_candidate_evidence_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate Evidence",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- observed_rows: {summary.get('observed_comparison_row_count')}",
            "- safety: research evidence only / no production",
            "",
        ]
    )


@with_artifact_validation_session
def run_filtered_candidate_evidence(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    filtered_comparison_id: str,
    promotion_review_id: str,
    comparison_dir: Path = upstream.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    promotion_review_dir: Path = upstream.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    policy_path: Path = DEFAULT_FILTERED_FORMALIZATION_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    comparison = _validated_comparison(filtered_comparison_id, comparison_dir)
    review = _validated_promotion_review(promotion_review_id, promotion_review_dir)
    _lineage_match(
        comparison,
        review,
        ("comparison_id", "filter_design_id", "source_ledger_id", "policy_version"),
    )
    _require(_source_policy(comparison) == _source_policy(review), "EB2 policy lineage mismatch")
    _chronology(generated, comparison, review)
    artifact_id = _stable_id(
        "filtered-candidate-evidence",
        candidate,
        filtered_comparison_id,
        promotion_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    _, views = _evidence_material(
        root=root,
        artifact_id=root.name,
        candidate=candidate,
        comparison=comparison,
        review=review,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": EVIDENCE_INPUT_SCHEMA,
        "evidence_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "policy_source": _policy_source(policy_path),
        "comparison_source": _binding(
            kind="filtered_vs_original_comparison",
            artifact_id=filtered_comparison_id,
            root=comparison_dir / filtered_comparison_id,
            names=upstream.COMPARISON_FILES,
        ),
        "promotion_review_source": _binding(
            kind="filtered_candidate_promotion_review",
            artifact_id=promotion_review_id,
            root=promotion_review_dir / promotion_review_id,
            names=upstream.REVIEW_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, EVIDENCE_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(root / "filtered_candidate_evidence_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_filtered_candidate_evidence", root.name, root / EVIDENCE_VIEWS[0])
    return _run_result(
        filtered_candidate_evidence_report_payload(evidence_id=root.name, output_dir=output_dir),
        manifest_path_key="filtered_candidate_evidence_manifest_path",
        legacy_directory_key="evidence_dir",
        current_directory_key="filtered_candidate_evidence_dir",
    )


def filtered_candidate_evidence_report_payload(
    *,
    evidence_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=evidence_id,
        latest_pointer="latest_filtered_candidate_evidence",
        latest=latest,
        output_dir=output_dir,
        required_name=EVIDENCE_VIEWS[0],
    )
    return {
        **_read_json(root / EVIDENCE_VIEWS[0]),
        "filtered_candidate_evidence_summary": _read_json(root / EVIDENCE_VIEWS[1]),
        "evidence_component_breakdown": _read_json(root / EVIDENCE_VIEWS[2]),
        "evidence_strength_weakness_matrix": _read_json(root / EVIDENCE_VIEWS[3]),
        "reader_brief_section": (root / EVIDENCE_VIEWS[5]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "filtered_candidate_evidence_input_snapshot.json"),
        "filtered_candidate_evidence_dir": str(root),
    }


def _rebuild_evidence(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_candidate_evidence_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    comparison_source = _mapping(snapshot.get("comparison_source"))
    review_source = _mapping(snapshot.get("promotion_review_source"))
    _validate_binding(
        comparison_source, kind="filtered_vs_original_comparison", names=upstream.COMPARISON_FILES
    )
    _validate_binding(
        review_source, kind="filtered_candidate_promotion_review", names=upstream.REVIEW_FILES
    )
    comparison = _validated_comparison(
        _binding_id(comparison_source), _binding_root(comparison_source).parent
    )
    review = _validated_promotion_review(
        _binding_id(review_source), _binding_root(review_source).parent
    )
    _lineage_match(
        comparison,
        review,
        ("comparison_id", "filter_design_id", "source_ledger_id", "policy_version"),
    )
    _require(_source_policy(comparison) == _source_policy(review), "EB2 policy lineage mismatch")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, comparison, review)
    _, expected = _evidence_material(
        root=root,
        artifact_id=artifact_id,
        candidate=_text(snapshot.get("candidate")),
        comparison=comparison,
        review=review,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_filtered_candidate_evidence_artifact(
    *, evidence_id: str, output_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=evidence_id,
        output_dir=output_dir,
        snapshot_name="filtered_candidate_evidence_input_snapshot.json",
        schema=EVIDENCE_INPUT_SCHEMA,
        id_key="evidence_id",
        view_names=EVIDENCE_VIEWS,
        report_type="etf_dynamic_v3_filtered_candidate_evidence_validation",
        rebuild=_rebuild_evidence,
    )


def _spec_material(
    *, root: Path, artifact_id: str, candidate: str, policy: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    candidate_contract = _mapping(policy.get("candidate_contract"))
    spec = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "method": {
            "name": candidate,
            "base_method": candidate_contract.get("base_method"),
            "mode": candidate_contract.get("method_mode"),
        },
        "filters": {
            "risk_off": {"block_risk_increase": True},
            "strong_recovery": {"allow_risk_restore": True},
        },
        "observed_evidence_status": "NOT_BOUND",
        **_safety(),
    }
    contract = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "contract_status": "RESEARCH_SPEC_ONLY",
        "observed_performance_available": False,
        "requires_new_external_data": True,
        "next_required_action": _mapping(policy.get("evidence_rules")).get(
            "missing_dated_evidence_action"
        ),
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_median_regime_filter_spec_manifest",
        "spec_id": artifact_id,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        "policy_version": policy.get("version"),
        "market_regime": "ai_after_chatgpt",
        "date_start": "2022-12-01",
        "date_end": None,
        "data_quality_status": None,
        "median_regime_filter_spec_manifest_path": str(root / SPEC_VIEWS[0]),
        "median_regime_filter_spec_path": str(root / SPEC_VIEWS[1]),
        "median_regime_filter_contract_path": str(root / SPEC_VIEWS[2]),
        "median_regime_filter_spec_report_path": str(root / SPEC_VIEWS[3]),
        "median_regime_filter_spec_input_snapshot_path": str(
            root / "median_regime_filter_spec_input_snapshot.json"
        ),
        **_safety(),
    }
    report = render_median_regime_filter_spec_report(manifest, spec, contract)
    views = {
        SPEC_VIEWS[0]: foundation._json_bytes(manifest),
        SPEC_VIEWS[1]: yaml.safe_dump(spec, sort_keys=True, allow_unicode=True).encode("utf-8"),
        SPEC_VIEWS[2]: foundation._json_bytes(contract),
        SPEC_VIEWS[3]: foundation._text_file_bytes(report),
    }
    return manifest, views


def render_median_regime_filter_spec_report(
    manifest: Mapping[str, Any], spec: Mapping[str, Any], contract: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Median Regime Filter Spec {manifest.get('spec_id')}",
            "",
            f"- candidate：{spec.get('candidate')}",
            f"- contract_status：{contract.get('contract_status')}",
            f"- observed_performance_available：{contract.get('observed_performance_available')}",
            "- 解释边界：method specification不是observed evidence。",
            "- safety：research spec only / no official weights / no production",
            "",
        ]
    )


@with_artifact_validation_session
def review_median_regime_filter_spec(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    output_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    policy_path: Path = DEFAULT_FILTERED_FORMALIZATION_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    policy = _policy(policy_path)
    artifact_id = _stable_id("median-regime-filter-spec", candidate, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _spec_material(
        root=root, artifact_id=root.name, candidate=candidate, policy=policy, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": SPEC_INPUT_SCHEMA,
        "spec_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "policy_source": _policy_source(policy_path),
        "view_hashes": foundation._view_hashes(root, SPEC_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(root / "median_regime_filter_spec_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_median_regime_filter_spec", root.name, root / SPEC_VIEWS[0])
    return _run_result(
        median_regime_filter_spec_report_payload(spec_id=root.name, output_dir=output_dir),
        manifest_path_key="median_regime_filter_spec_manifest_path",
        legacy_directory_key="spec_dir",
        current_directory_key="median_regime_filter_spec_dir",
    )


def median_regime_filter_spec_report_payload(
    *,
    spec_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=spec_id,
        latest_pointer="latest_median_regime_filter_spec",
        latest=latest,
        output_dir=output_dir,
        required_name=SPEC_VIEWS[0],
    )
    return {
        **_read_json(root / SPEC_VIEWS[0]),
        "median_regime_filter_spec": yaml.safe_load(
            (root / SPEC_VIEWS[1]).read_text(encoding="utf-8")
        ),
        "median_regime_filter_contract": _read_json(root / SPEC_VIEWS[2]),
        **_optional_snapshot(root, "median_regime_filter_spec_input_snapshot.json"),
        "median_regime_filter_spec_dir": str(root),
    }


def _rebuild_spec(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "median_regime_filter_spec_input_snapshot.json")
    policy = _policy_from_binding(_mapping(snapshot.get("policy_source")))
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _, expected = _spec_material(
        root=root,
        artifact_id=artifact_id,
        candidate=_text(snapshot.get("candidate")),
        policy=policy,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_median_regime_filter_spec_artifact(
    *, spec_id: str, output_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=spec_id,
        output_dir=output_dir,
        snapshot_name="median_regime_filter_spec_input_snapshot.json",
        schema=SPEC_INPUT_SCHEMA,
        id_key="spec_id",
        view_names=SPEC_VIEWS,
        report_type="etf_dynamic_v3_median_regime_filter_spec_validation",
        rebuild=_rebuild_spec,
    )


def _empty_stress_summary(candidate: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "evidence_status": "INSUFFICIENT_DATA",
        "stress_windows_total": 0,
        "improved_count": 0,
        "worse_count": 0,
        "mixed_count": 0,
        "insufficient_count": 0,
        "best_regime": None,
        "weakest_regime": None,
        "stress_robustness_status": "INSUFFICIENT_DATA",
        **_safety(),
    }


def _stress_material(
    *, root: Path, artifact_id: str, candidate: str, spec: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    inventory: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    summary = _empty_stress_summary(candidate)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_stress_backfill_manifest",
        "stress_backfill_id": artifact_id,
        "spec_id": spec.get("spec_id"),
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(spec),
        "filtered_candidate_stress_manifest_path": str(root / STRESS_VIEWS[0]),
        "stress_window_inventory_path": str(root / STRESS_VIEWS[1]),
        "stress_window_metrics_path": str(root / STRESS_VIEWS[2]),
        "filtered_candidate_stress_summary_path": str(root / STRESS_VIEWS[3]),
        "filtered_candidate_stress_report_path": str(root / STRESS_VIEWS[4]),
        "filtered_candidate_stress_backfill_input_snapshot_path": str(
            root / "filtered_candidate_stress_backfill_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        STRESS_VIEWS[0]: foundation._json_bytes(manifest),
        STRESS_VIEWS[1]: foundation._jsonl_bytes(inventory),
        STRESS_VIEWS[2]: foundation._jsonl_bytes(metrics),
        STRESS_VIEWS[3]: foundation._json_bytes(summary),
        STRESS_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_candidate_stress_report(manifest, inventory, metrics, summary)
        ),
    }
    return manifest, views


def render_filtered_candidate_stress_report(
    manifest: Mapping[str, Any],
    inventory: Sequence[Mapping[str, Any]],
    metrics: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Filtered Candidate Stress Backfill {manifest.get('stress_backfill_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- stress_window_count：{len(inventory)}",
            f"- metric_row_count：{len(metrics)}",
            "- 解释边界：没有validated dated window source时不生成stress结果。",
            "- safety：research only / no official weights / no production",
            "",
        ]
    )


@with_artifact_validation_session
def run_filtered_candidate_stress_backfill(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    spec_id: str,
    spec_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    spec = _validated_self(
        artifact_id=spec_id,
        output_dir=spec_dir,
        validator=validate_median_regime_filter_spec_artifact,
        validator_key="spec_id",
        reader=median_regime_filter_spec_report_payload,
        reader_key="spec_id",
        label="median regime filter spec",
    )
    _require(spec.get("candidate") == candidate, "stress candidate/spec lineage mismatch")
    _chronology(generated, spec)
    artifact_id = _stable_id("filtered-candidate-stress", candidate, spec_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _stress_material(
        root=root, artifact_id=root.name, candidate=candidate, spec=spec, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": STRESS_INPUT_SCHEMA,
        "stress_backfill_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(spec.get("input_snapshot")).get("policy_source")),
        "spec_source": _binding(
            kind="median_regime_filter_spec",
            artifact_id=spec_id,
            root=spec_dir / spec_id,
            names=SPEC_FILES,
        ),
        "dated_window_sources": [],
        "view_hashes": foundation._view_hashes(root, STRESS_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(
        root / "filtered_candidate_stress_backfill_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_filtered_candidate_stress_backfill", root.name, root / STRESS_VIEWS[0]
    )
    return _run_result(
        filtered_candidate_stress_backfill_report_payload(
            stress_backfill_id=root.name, output_dir=output_dir
        ),
        manifest_path_key="filtered_candidate_stress_manifest_path",
        legacy_directory_key="stress_backfill_dir",
        current_directory_key="filtered_candidate_stress_backfill_dir",
    )


def filtered_candidate_stress_backfill_report_payload(
    *,
    stress_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=stress_backfill_id,
        latest_pointer="latest_filtered_candidate_stress_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name=STRESS_VIEWS[0],
    )
    return {
        **_read_json(root / STRESS_VIEWS[0]),
        "stress_window_inventory": _read_jsonl(root / STRESS_VIEWS[1]),
        "stress_window_metrics": _read_jsonl(root / STRESS_VIEWS[2]),
        "filtered_candidate_stress_summary": _read_json(root / STRESS_VIEWS[3]),
        **_optional_snapshot(root, "filtered_candidate_stress_backfill_input_snapshot.json"),
        "filtered_candidate_stress_backfill_dir": str(root),
    }


def _rebuild_stress(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_candidate_stress_backfill_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    _require(not _records(snapshot.get("dated_window_sources")), "unreviewed dated window source")
    source = _mapping(snapshot.get("spec_source"))
    _validate_binding(source, kind="median_regime_filter_spec", names=SPEC_FILES)
    spec = _validated_self(
        artifact_id=_binding_id(source),
        output_dir=_binding_root(source).parent,
        validator=validate_median_regime_filter_spec_artifact,
        validator_key="spec_id",
        reader=median_regime_filter_spec_report_payload,
        reader_key="spec_id",
        label="median regime filter spec",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, spec)
    _, expected = _stress_material(
        root=root,
        artifact_id=artifact_id,
        candidate=_text(snapshot.get("candidate")),
        spec=spec,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_filtered_candidate_stress_backfill_artifact(
    *, stress_backfill_id: str, output_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=stress_backfill_id,
        output_dir=output_dir,
        snapshot_name="filtered_candidate_stress_backfill_input_snapshot.json",
        schema=STRESS_INPUT_SCHEMA,
        id_key="stress_backfill_id",
        view_names=STRESS_VIEWS,
        report_type="etf_dynamic_v3_filtered_candidate_stress_backfill_validation",
        rebuild=_rebuild_stress,
    )


def _mismatch_material(
    *, root: Path, artifact_id: str, stress: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    events: list[dict[str, Any]] = []
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "risk_increase_during_drawdown_before": None,
        "risk_increase_during_drawdown_after": None,
        "reduction_count": None,
        "reduction_pct": None,
        "blocked_signal_helpful_rate": None,
        "blocked_signal_harmful_rate": None,
        "drawdown_mismatch_reduction_status": "INSUFFICIENT_DATA",
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_drawdown_mismatch_reduction_manifest",
        "reduction_id": artifact_id,
        "stress_backfill_id": stress.get("stress_backfill_id"),
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(stress),
        "drawdown_mismatch_reduction_manifest_path": str(root / MISMATCH_VIEWS[0]),
        "mismatch_reduction_events_path": str(root / MISMATCH_VIEWS[1]),
        "mismatch_reduction_summary_path": str(root / MISMATCH_VIEWS[2]),
        "drawdown_mismatch_reduction_report_path": str(root / MISMATCH_VIEWS[3]),
        "drawdown_mismatch_reduction_input_snapshot_path": str(
            root / "drawdown_mismatch_reduction_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        MISMATCH_VIEWS[0]: foundation._json_bytes(manifest),
        MISMATCH_VIEWS[1]: foundation._jsonl_bytes(events),
        MISMATCH_VIEWS[2]: foundation._json_bytes(summary),
        MISMATCH_VIEWS[3]: foundation._text_file_bytes(
            render_drawdown_mismatch_reduction_report(manifest, events, summary)
        ),
    }
    return manifest, views


def render_drawdown_mismatch_reduction_report(
    manifest: Mapping[str, Any], events: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Drawdown Mismatch Reduction {manifest.get('reduction_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- observed_event_count：{len(events)}",
            "- 解释边界：无dated stress events时所有reduction指标保持null。",
            "- safety：research only / no production",
            "",
        ]
    )


def _flip_material(
    *, root: Path, artifact_id: str, stress: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    events: list[dict[str, Any]] = []
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "direction_flip_before": None,
        "direction_flip_after": None,
        "top_candidate_rotation_before": None,
        "top_candidate_rotation_after": None,
        "signal_churn_before": None,
        "signal_churn_after": None,
        "flip_reduction_status": "INSUFFICIENT_DATA",
        "rotation_reduction_status": "INSUFFICIENT_DATA",
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_flip_rotation_reduction_manifest",
        "flip_reduction_id": artifact_id,
        "stress_backfill_id": stress.get("stress_backfill_id"),
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(stress),
        "flip_rotation_reduction_manifest_path": str(root / FLIP_VIEWS[0]),
        "flip_rotation_events_path": str(root / FLIP_VIEWS[1]),
        "flip_rotation_reduction_summary_path": str(root / FLIP_VIEWS[2]),
        "flip_rotation_reduction_report_path": str(root / FLIP_VIEWS[3]),
        "flip_rotation_reduction_input_snapshot_path": str(
            root / "flip_rotation_reduction_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        FLIP_VIEWS[0]: foundation._json_bytes(manifest),
        FLIP_VIEWS[1]: foundation._jsonl_bytes(events),
        FLIP_VIEWS[2]: foundation._json_bytes(summary),
        FLIP_VIEWS[3]: foundation._text_file_bytes(
            render_flip_rotation_reduction_report(manifest, events, summary)
        ),
    }
    return manifest, views


def render_flip_rotation_reduction_report(
    manifest: Mapping[str, Any], events: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Flip Rotation Reduction {manifest.get('flip_reduction_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- observed_event_count：{len(events)}",
            "- 解释边界：不再用row index生成flip或rotation count。",
            "- safety：research only / no production",
            "",
        ]
    )


def _single_source_run(
    *,
    source_id: str,
    source_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
    source_kind: str,
    source_files: Sequence[str],
    source_validator: Callable[..., dict[str, Any]],
    source_validator_key: str,
    source_reader: Callable[..., dict[str, Any]],
    source_reader_key: str,
    source_label: str,
    artifact_prefix: str,
    id_key: str,
    snapshot_name: str,
    snapshot_schema: str,
    view_names: Sequence[str],
    latest_pointer: str,
    material: Callable[..., tuple[dict[str, Any], dict[str, bytes]]],
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    source = _validated_self(
        artifact_id=source_id,
        output_dir=source_dir,
        validator=source_validator,
        validator_key=source_validator_key,
        reader=source_reader,
        reader_key=source_reader_key,
        label=source_label,
    )
    _chronology(generated, source)
    artifact_id = _stable_id(artifact_prefix, source_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = material(root=root, artifact_id=root.name, stress=source, generated=generated)
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": snapshot_schema,
        id_key: root.name,
        "candidate": source.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(source.get("input_snapshot")).get("policy_source")),
        "stress_source": _binding(
            kind=source_kind,
            artifact_id=source_id,
            root=source_dir / source_id,
            names=source_files,
        ),
        "view_hashes": foundation._view_hashes(root, view_names),
        **_safety(),
    }
    foundation._write_snapshot(root / snapshot_name, snapshot)
    _write_latest_pointer(latest_pointer, root.name, root / view_names[0])
    return {id_key: root.name}


@with_artifact_validation_session
def run_drawdown_mismatch_reduction(
    *,
    stress_backfill_id: str,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    result = _single_source_run(
        source_id=stress_backfill_id,
        source_dir=stress_backfill_dir,
        output_dir=output_dir,
        generated_at=generated_at,
        source_kind="filtered_candidate_stress_backfill",
        source_files=STRESS_FILES,
        source_validator=validate_filtered_candidate_stress_backfill_artifact,
        source_validator_key="stress_backfill_id",
        source_reader=filtered_candidate_stress_backfill_report_payload,
        source_reader_key="stress_backfill_id",
        source_label="filtered candidate stress",
        artifact_prefix="drawdown-mismatch-reduction",
        id_key="reduction_id",
        snapshot_name="drawdown_mismatch_reduction_input_snapshot.json",
        snapshot_schema=MISMATCH_INPUT_SCHEMA,
        view_names=MISMATCH_VIEWS,
        latest_pointer="latest_drawdown_mismatch_reduction",
        material=_mismatch_material,
    )
    return _run_result(
        drawdown_mismatch_reduction_report_payload(
            reduction_id=result["reduction_id"], output_dir=output_dir
        ),
        manifest_path_key="drawdown_mismatch_reduction_manifest_path",
        legacy_directory_key="reduction_dir",
        current_directory_key="drawdown_mismatch_reduction_dir",
    )


@with_artifact_validation_session
def run_flip_rotation_reduction(
    *,
    stress_backfill_id: str,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    result = _single_source_run(
        source_id=stress_backfill_id,
        source_dir=stress_backfill_dir,
        output_dir=output_dir,
        generated_at=generated_at,
        source_kind="filtered_candidate_stress_backfill",
        source_files=STRESS_FILES,
        source_validator=validate_filtered_candidate_stress_backfill_artifact,
        source_validator_key="stress_backfill_id",
        source_reader=filtered_candidate_stress_backfill_report_payload,
        source_reader_key="stress_backfill_id",
        source_label="filtered candidate stress",
        artifact_prefix="flip-rotation-reduction",
        id_key="flip_reduction_id",
        snapshot_name="flip_rotation_reduction_input_snapshot.json",
        snapshot_schema=FLIP_INPUT_SCHEMA,
        view_names=FLIP_VIEWS,
        latest_pointer="latest_flip_rotation_reduction",
        material=_flip_material,
    )
    return _run_result(
        flip_rotation_reduction_report_payload(
            flip_reduction_id=result["flip_reduction_id"], output_dir=output_dir
        ),
        manifest_path_key="flip_rotation_reduction_manifest_path",
        legacy_directory_key="flip_reduction_dir",
        current_directory_key="flip_rotation_reduction_dir",
    )


def _simple_report_payload(
    *,
    artifact_id: str | None,
    latest: bool,
    output_dir: Path,
    latest_pointer: str,
    views: Sequence[str],
) -> tuple[Path, dict[str, Any]]:
    root = _artifact_dir(
        artifact_id=artifact_id,
        latest_pointer=latest_pointer,
        latest=latest,
        output_dir=output_dir,
        required_name=views[0],
    )
    return root, _read_json(root / views[0])


def drawdown_mismatch_reduction_report_payload(
    *,
    reduction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=reduction_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_drawdown_mismatch_reduction",
        views=MISMATCH_VIEWS,
    )
    return {
        **manifest,
        "mismatch_reduction_events": _read_jsonl(root / MISMATCH_VIEWS[1]),
        "mismatch_reduction_summary": _read_json(root / MISMATCH_VIEWS[2]),
        **_optional_snapshot(root, "drawdown_mismatch_reduction_input_snapshot.json"),
        "drawdown_mismatch_reduction_dir": str(root),
    }


def flip_rotation_reduction_report_payload(
    *,
    flip_reduction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=flip_reduction_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_flip_rotation_reduction",
        views=FLIP_VIEWS,
    )
    return {
        **manifest,
        "flip_rotation_events": _read_jsonl(root / FLIP_VIEWS[1]),
        "flip_rotation_reduction_summary": _read_json(root / FLIP_VIEWS[2]),
        **_optional_snapshot(root, "flip_rotation_reduction_input_snapshot.json"),
        "flip_rotation_reduction_dir": str(root),
    }


def _rebuild_single_source(
    *,
    root: Path,
    artifact_id: str,
    snapshot_name: str,
    source_kind: str,
    source_files: Sequence[str],
    source_validator: Callable[..., dict[str, Any]],
    source_validator_key: str,
    source_reader: Callable[..., dict[str, Any]],
    source_reader_key: str,
    source_label: str,
    material: Callable[..., tuple[dict[str, Any], dict[str, bytes]]],
) -> list[dict[str, Any]]:
    snapshot = _read_json(root / snapshot_name)
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    source_binding = _mapping(snapshot.get("stress_source"))
    _validate_binding(source_binding, kind=source_kind, names=source_files)
    source = _validated_self(
        artifact_id=_binding_id(source_binding),
        output_dir=_binding_root(source_binding).parent,
        validator=source_validator,
        validator_key=source_validator_key,
        reader=source_reader,
        reader_key=source_reader_key,
        label=source_label,
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, source)
    _, expected = material(root=root, artifact_id=artifact_id, stress=source, generated=generated)
    return diagnosis_foundation._check_bytes(root, expected)


def _rebuild_mismatch(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    return _rebuild_single_source(
        root=root,
        artifact_id=artifact_id,
        snapshot_name="drawdown_mismatch_reduction_input_snapshot.json",
        source_kind="filtered_candidate_stress_backfill",
        source_files=STRESS_FILES,
        source_validator=validate_filtered_candidate_stress_backfill_artifact,
        source_validator_key="stress_backfill_id",
        source_reader=filtered_candidate_stress_backfill_report_payload,
        source_reader_key="stress_backfill_id",
        source_label="filtered candidate stress",
        material=_mismatch_material,
    )


def _rebuild_flip(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    return _rebuild_single_source(
        root=root,
        artifact_id=artifact_id,
        snapshot_name="flip_rotation_reduction_input_snapshot.json",
        source_kind="filtered_candidate_stress_backfill",
        source_files=STRESS_FILES,
        source_validator=validate_filtered_candidate_stress_backfill_artifact,
        source_validator_key="stress_backfill_id",
        source_reader=filtered_candidate_stress_backfill_report_payload,
        source_reader_key="stress_backfill_id",
        source_label="filtered candidate stress",
        material=_flip_material,
    )


@with_artifact_validation_session
def validate_drawdown_mismatch_reduction_artifact(
    *, reduction_id: str, output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=reduction_id,
        output_dir=output_dir,
        snapshot_name="drawdown_mismatch_reduction_input_snapshot.json",
        schema=MISMATCH_INPUT_SCHEMA,
        id_key="reduction_id",
        view_names=MISMATCH_VIEWS,
        report_type="etf_dynamic_v3_drawdown_mismatch_reduction_validation",
        rebuild=_rebuild_mismatch,
    )


@with_artifact_validation_session
def validate_flip_rotation_reduction_artifact(
    *, flip_reduction_id: str, output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=flip_reduction_id,
        output_dir=output_dir,
        snapshot_name="flip_rotation_reduction_input_snapshot.json",
        schema=FLIP_INPUT_SCHEMA,
        id_key="flip_reduction_id",
        view_names=FLIP_VIEWS,
        report_type="etf_dynamic_v3_flip_rotation_reduction_validation",
        rebuild=_rebuild_flip,
    )


def _ab_material(
    *,
    root: Path,
    artifact_id: str,
    stress: Mapping[str, Any],
    mismatch: Mapping[str, Any],
    flip: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    rows: list[dict[str, Any]] = []
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "comparison_row_count": 0,
        "best_against": [],
        "weak_against": [],
        "winner": None,
        "confidence": None,
        "overall_ab_status": "INSUFFICIENT_DATA",
        "recommended_next_action": "COLLECT_VALIDATED_DATED_FILTERED_OUTCOMES",
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_ab_review_manifest",
        "ab_review_id": artifact_id,
        "stress_backfill_id": stress.get("stress_backfill_id"),
        "mismatch_reduction_id": mismatch.get("reduction_id"),
        "flip_reduction_id": flip.get("flip_reduction_id"),
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(stress),
        "filtered_candidate_ab_manifest_path": str(root / AB_VIEWS[0]),
        "ab_method_comparison_path": str(root / AB_VIEWS[1]),
        "ab_summary_path": str(root / AB_VIEWS[2]),
        "filtered_candidate_ab_report_path": str(root / AB_VIEWS[3]),
        "reader_brief_section_path": str(root / AB_VIEWS[4]),
        "filtered_candidate_ab_review_input_snapshot_path": str(
            root / "filtered_candidate_ab_review_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        AB_VIEWS[0]: foundation._json_bytes(manifest),
        AB_VIEWS[1]: foundation._jsonl_bytes(rows),
        AB_VIEWS[2]: foundation._json_bytes(summary),
        AB_VIEWS[3]: foundation._text_file_bytes(
            render_filtered_candidate_ab_report(manifest, rows, summary)
        ),
        AB_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_candidate_ab_reader_brief(summary)
        ),
    }
    return manifest, views


def render_filtered_candidate_ab_report(
    manifest: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Filtered Candidate A/B Review {manifest.get('ab_review_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- comparison_row_count：{len(rows)}",
            f"- winner：{summary.get('winner')}",
            "- 解释边界：不再用公式常量合成baseline delta或winner。",
            "- safety：manual review / no official weights / no production",
            "",
        ]
    )


def render_filtered_candidate_ab_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate A/B Review",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- winner: {summary.get('winner')}",
            "- safety: evidence review only / no production",
            "",
        ]
    )


@with_artifact_validation_session
def run_filtered_candidate_ab_review(
    *,
    stress_backfill_id: str,
    mismatch_reduction_id: str,
    flip_reduction_id: str,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    stress = _validated_self(
        artifact_id=stress_backfill_id,
        output_dir=stress_backfill_dir,
        validator=validate_filtered_candidate_stress_backfill_artifact,
        validator_key="stress_backfill_id",
        reader=filtered_candidate_stress_backfill_report_payload,
        reader_key="stress_backfill_id",
        label="filtered candidate stress",
    )
    mismatch = _validated_self(
        artifact_id=mismatch_reduction_id,
        output_dir=mismatch_reduction_dir,
        validator=validate_drawdown_mismatch_reduction_artifact,
        validator_key="reduction_id",
        reader=drawdown_mismatch_reduction_report_payload,
        reader_key="reduction_id",
        label="drawdown mismatch reduction",
    )
    flip = _validated_self(
        artifact_id=flip_reduction_id,
        output_dir=flip_reduction_dir,
        validator=validate_flip_rotation_reduction_artifact,
        validator_key="flip_reduction_id",
        reader=flip_rotation_reduction_report_payload,
        reader_key="flip_reduction_id",
        label="flip rotation reduction",
    )
    _require(
        stress.get("stress_backfill_id")
        == mismatch.get("stress_backfill_id")
        == flip.get("stress_backfill_id"),
        "A/B source stress lineage mismatch",
    )
    _require(
        stress.get("candidate") == mismatch.get("candidate") == flip.get("candidate"),
        "A/B candidate lineage mismatch",
    )
    _chronology(generated, stress, mismatch, flip)
    artifact_id = _stable_id(
        "filtered-candidate-ab",
        stress_backfill_id,
        mismatch_reduction_id,
        flip_reduction_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    _, views = _ab_material(
        root=root,
        artifact_id=root.name,
        stress=stress,
        mismatch=mismatch,
        flip=flip,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    policy_source = _mapping(_mapping(stress.get("input_snapshot")).get("policy_source"))
    snapshot = {
        "schema_version": AB_INPUT_SCHEMA,
        "ab_review_id": root.name,
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "stress_source": _binding(
            kind="filtered_candidate_stress_backfill",
            artifact_id=stress_backfill_id,
            root=stress_backfill_dir / stress_backfill_id,
            names=STRESS_FILES,
        ),
        "mismatch_source": _binding(
            kind="drawdown_mismatch_reduction",
            artifact_id=mismatch_reduction_id,
            root=mismatch_reduction_dir / mismatch_reduction_id,
            names=MISMATCH_FILES,
        ),
        "flip_source": _binding(
            kind="flip_rotation_reduction",
            artifact_id=flip_reduction_id,
            root=flip_reduction_dir / flip_reduction_id,
            names=FLIP_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, AB_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(root / "filtered_candidate_ab_review_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_filtered_candidate_ab_review", root.name, root / AB_VIEWS[0])
    return _run_result(
        filtered_candidate_ab_review_report_payload(ab_review_id=root.name, output_dir=output_dir),
        manifest_path_key="filtered_candidate_ab_manifest_path",
        legacy_directory_key="ab_review_dir",
        current_directory_key="filtered_candidate_ab_review_dir",
    )


def filtered_candidate_ab_review_report_payload(
    *,
    ab_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=ab_review_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_filtered_candidate_ab_review",
        views=AB_VIEWS,
    )
    return {
        **manifest,
        "ab_method_comparison": _read_jsonl(root / AB_VIEWS[1]),
        "ab_summary": _read_json(root / AB_VIEWS[2]),
        "reader_brief_section": (root / AB_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "filtered_candidate_ab_review_input_snapshot.json"),
        "filtered_candidate_ab_review_dir": str(root),
    }


def _rebuild_ab(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_candidate_ab_review_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    specs = (
        ("stress_source", "filtered_candidate_stress_backfill", STRESS_FILES),
        ("mismatch_source", "drawdown_mismatch_reduction", MISMATCH_FILES),
        ("flip_source", "flip_rotation_reduction", FLIP_FILES),
    )
    bindings: dict[str, Mapping[str, Any]] = {}
    for key, kind, names in specs:
        binding = _mapping(snapshot.get(key))
        _validate_binding(binding, kind=kind, names=names)
        bindings[key] = binding
    stress = _validated_self(
        artifact_id=_binding_id(bindings["stress_source"]),
        output_dir=_binding_root(bindings["stress_source"]).parent,
        validator=validate_filtered_candidate_stress_backfill_artifact,
        validator_key="stress_backfill_id",
        reader=filtered_candidate_stress_backfill_report_payload,
        reader_key="stress_backfill_id",
        label="filtered candidate stress",
    )
    mismatch = _validated_self(
        artifact_id=_binding_id(bindings["mismatch_source"]),
        output_dir=_binding_root(bindings["mismatch_source"]).parent,
        validator=validate_drawdown_mismatch_reduction_artifact,
        validator_key="reduction_id",
        reader=drawdown_mismatch_reduction_report_payload,
        reader_key="reduction_id",
        label="drawdown mismatch reduction",
    )
    flip = _validated_self(
        artifact_id=_binding_id(bindings["flip_source"]),
        output_dir=_binding_root(bindings["flip_source"]).parent,
        validator=validate_flip_rotation_reduction_artifact,
        validator_key="flip_reduction_id",
        reader=flip_rotation_reduction_report_payload,
        reader_key="flip_reduction_id",
        label="flip rotation reduction",
    )
    _require(
        stress.get("stress_backfill_id")
        == mismatch.get("stress_backfill_id")
        == flip.get("stress_backfill_id"),
        "A/B source stress lineage mismatch",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, stress, mismatch, flip)
    _, expected = _ab_material(
        root=root,
        artifact_id=artifact_id,
        stress=stress,
        mismatch=mismatch,
        flip=flip,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_filtered_candidate_ab_review_artifact(
    *, ab_review_id: str, output_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=ab_review_id,
        output_dir=output_dir,
        snapshot_name="filtered_candidate_ab_review_input_snapshot.json",
        schema=AB_INPUT_SCHEMA,
        id_key="ab_review_id",
        view_names=AB_VIEWS,
        report_type="etf_dynamic_v3_filtered_candidate_ab_review_validation",
        rebuild=_rebuild_ab,
    )


def _confirmation_material(
    *, root: Path, artifact_id: str, ab: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    rows = _records(ab.get("ab_method_comparison"))
    targets = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": ab.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "targets": [],
        "registered_target_count": 0,
        "completed_observation_count": 0,
        "auto_apply": False,
        **_safety(),
    }
    _require(not rows, "observed A/B rows require a reviewed confirmation observation contract")
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_gate_confirmation_manifest",
        "confirmation_id": artifact_id,
        "ab_review_id": ab.get("ab_review_id"),
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(ab),
        "signal_gate_confirmation_manifest_path": str(root / CONFIRMATION_VIEWS[0]),
        "signal_gate_confirmation_targets_path": str(root / CONFIRMATION_VIEWS[1]),
        "signal_gate_confirmation_report_path": str(root / CONFIRMATION_VIEWS[2]),
        "reader_brief_section_path": str(root / CONFIRMATION_VIEWS[3]),
        "signal_gate_confirmation_input_snapshot_path": str(
            root / "signal_gate_confirmation_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        CONFIRMATION_VIEWS[0]: foundation._json_bytes(manifest),
        CONFIRMATION_VIEWS[1]: foundation._json_bytes(targets),
        CONFIRMATION_VIEWS[2]: foundation._text_file_bytes(
            render_signal_gate_confirmation_report(manifest, targets)
        ),
        CONFIRMATION_VIEWS[3]: foundation._text_file_bytes(
            render_signal_gate_confirmation_reader_brief(targets)
        ),
    }
    return manifest, views


def render_signal_gate_confirmation_report(
    manifest: Mapping[str, Any], targets: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Signal Gate Confirmation {manifest.get('confirmation_id')}",
            "",
            f"- evidence_status：{targets.get('evidence_status')}",
            f"- registered_target_count：{targets.get('registered_target_count')}",
            f"- completed_observation_count：{targets.get('completed_observation_count')}",
            "- 解释边界：registered target不等于completed observation。",
            "- safety：manual review / no auto apply / no production",
            "",
        ]
    )


def render_signal_gate_confirmation_reader_brief(targets: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Gate Confirmation",
            "",
            f"- evidence_status: {targets.get('evidence_status')}",
            f"- completed_observations: {targets.get('completed_observation_count')}",
            "- safety: target registration is not confirmation evidence",
            "",
        ]
    )


@with_artifact_validation_session
def register_signal_gate_confirmation(
    *,
    ab_review_id: str,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    ab = _validated_self(
        artifact_id=ab_review_id,
        output_dir=ab_review_dir,
        validator=validate_filtered_candidate_ab_review_artifact,
        validator_key="ab_review_id",
        reader=filtered_candidate_ab_review_report_payload,
        reader_key="ab_review_id",
        label="filtered candidate A/B review",
    )
    _chronology(generated, ab)
    artifact_id = _stable_id("signal-gate-confirmation", ab_review_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _confirmation_material(root=root, artifact_id=root.name, ab=ab, generated=generated)
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": CONFIRMATION_INPUT_SCHEMA,
        "confirmation_id": root.name,
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(ab.get("input_snapshot")).get("policy_source")),
        "ab_source": _binding(
            kind="filtered_candidate_ab_review",
            artifact_id=ab_review_id,
            root=ab_review_dir / ab_review_id,
            names=AB_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, CONFIRMATION_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(root / "signal_gate_confirmation_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_signal_gate_confirmation", root.name, root / CONFIRMATION_VIEWS[0]
    )
    return _run_result(
        signal_gate_confirmation_report_payload(confirmation_id=root.name, output_dir=output_dir),
        manifest_path_key="signal_gate_confirmation_manifest_path",
        legacy_directory_key="confirmation_dir",
        current_directory_key="signal_gate_confirmation_dir",
    )


def signal_gate_confirmation_report_payload(
    *,
    confirmation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=confirmation_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_signal_gate_confirmation",
        views=CONFIRMATION_VIEWS,
    )
    return {
        **manifest,
        "signal_gate_confirmation_targets": _read_json(root / CONFIRMATION_VIEWS[1]),
        "reader_brief_section": (root / CONFIRMATION_VIEWS[3]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "signal_gate_confirmation_input_snapshot.json"),
        "signal_gate_confirmation_dir": str(root),
    }


def _rebuild_confirmation(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_gate_confirmation_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    source = _mapping(snapshot.get("ab_source"))
    _validate_binding(source, kind="filtered_candidate_ab_review", names=AB_FILES)
    ab = _validated_self(
        artifact_id=_binding_id(source),
        output_dir=_binding_root(source).parent,
        validator=validate_filtered_candidate_ab_review_artifact,
        validator_key="ab_review_id",
        reader=filtered_candidate_ab_review_report_payload,
        reader_key="ab_review_id",
        label="filtered candidate A/B review",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, ab)
    _, expected = _confirmation_material(
        root=root, artifact_id=artifact_id, ab=ab, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_signal_gate_confirmation_artifact(
    *, confirmation_id: str, output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=confirmation_id,
        output_dir=output_dir,
        snapshot_name="signal_gate_confirmation_input_snapshot.json",
        schema=CONFIRMATION_INPUT_SCHEMA,
        id_key="confirmation_id",
        view_names=CONFIRMATION_VIEWS,
        report_type="etf_dynamic_v3_signal_gate_confirmation_validation",
        rebuild=_rebuild_confirmation,
    )


def _readiness_material(
    *,
    root: Path,
    artifact_id: str,
    ab: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    ab_summary = _mapping(ab.get("ab_summary"))
    targets = _mapping(confirmation.get("signal_gate_confirmation_targets"))
    complete = (
        ab_summary.get("evidence_status") != "INSUFFICIENT_DATA"
        and _float(targets.get("completed_observation_count")) > 0
    )
    _require(not complete, "formalization PASS requires a reviewed completed-observation contract")
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": ab.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "decision": "INSUFFICIENT_DATA",
        "confidence": None,
        "implementation_complexity": None,
        "requires_forward_confirmation": True,
        "registered_confirmation_targets": targets.get("registered_target_count", 0),
        "completed_confirmation_observations": targets.get("completed_observation_count", 0),
        "can_implement_research_only_method": False,
        "can_write_official_target_weights": False,
        **_safety(),
    }
    blockers = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": ab.get("candidate"),
        "blockers": [
            {
                "blocker": "validated_dated_filtered_outcomes_missing",
                "blocks_formal_research_method": True,
                "blocks_official_target": True,
                "severity": "BLOCKING",
            },
            {
                "blocker": "completed_confirmation_observations_missing",
                "blocks_formal_research_method": True,
                "blocks_official_target": True,
                "severity": "BLOCKING",
            },
        ],
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_formalization_readiness_manifest",
        "readiness_id": artifact_id,
        "ab_review_id": ab.get("ab_review_id"),
        "confirmation_id": confirmation.get("confirmation_id"),
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(ab),
        "filtered_formalization_manifest_path": str(root / READINESS_VIEWS[0]),
        "formalization_readiness_decision_path": str(root / READINESS_VIEWS[1]),
        "formalization_blockers_path": str(root / READINESS_VIEWS[2]),
        "filtered_formalization_report_path": str(root / READINESS_VIEWS[3]),
        "reader_brief_section_path": str(root / READINESS_VIEWS[4]),
        "filtered_formalization_readiness_input_snapshot_path": str(
            root / "filtered_formalization_readiness_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        READINESS_VIEWS[0]: foundation._json_bytes(manifest),
        READINESS_VIEWS[1]: foundation._json_bytes(decision),
        READINESS_VIEWS[2]: foundation._json_bytes(blockers),
        READINESS_VIEWS[3]: foundation._text_file_bytes(
            render_filtered_formalization_report(manifest, decision, blockers)
        ),
        READINESS_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_formalization_reader_brief(decision)
        ),
    }
    return manifest, views


def render_filtered_formalization_report(
    manifest: Mapping[str, Any], decision: Mapping[str, Any], blockers: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Filtered Formalization Readiness {manifest.get('readiness_id')}",
            "",
            f"- evidence_status：{decision.get('evidence_status')}",
            f"- decision：{decision.get('decision')}",
            f"- blocker_count：{len(_records(blockers.get('blockers')))}",
            "- 解释边界：未完成真实confirmation时不得formalize。",
            "- safety：manual review / no official weights / no production",
            "",
        ]
    )


def render_filtered_formalization_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Formalization Readiness",
            "",
            f"- evidence_status: {decision.get('evidence_status')}",
            f"- decision: {decision.get('decision')}",
            "- safety: not formalized / no production",
            "",
        ]
    )


@with_artifact_validation_session
def run_filtered_formalization_readiness(
    *,
    ab_review_id: str,
    confirmation_id: str,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    ab = _validated_self(
        artifact_id=ab_review_id,
        output_dir=ab_review_dir,
        validator=validate_filtered_candidate_ab_review_artifact,
        validator_key="ab_review_id",
        reader=filtered_candidate_ab_review_report_payload,
        reader_key="ab_review_id",
        label="filtered candidate A/B review",
    )
    confirmation = _validated_self(
        artifact_id=confirmation_id,
        output_dir=confirmation_dir,
        validator=validate_signal_gate_confirmation_artifact,
        validator_key="confirmation_id",
        reader=signal_gate_confirmation_report_payload,
        reader_key="confirmation_id",
        label="signal gate confirmation",
    )
    _require(confirmation.get("ab_review_id") == ab_review_id, "readiness A/B lineage mismatch")
    _require(confirmation.get("candidate") == ab.get("candidate"), "readiness candidate mismatch")
    _chronology(generated, ab, confirmation)
    artifact_id = _stable_id(
        "filtered-formalization", ab_review_id, confirmation_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    _, views = _readiness_material(
        root=root, artifact_id=root.name, ab=ab, confirmation=confirmation, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    policy_source = _mapping(_mapping(ab.get("input_snapshot")).get("policy_source"))
    snapshot = {
        "schema_version": READINESS_INPUT_SCHEMA,
        "readiness_id": root.name,
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "ab_source": _binding(
            kind="filtered_candidate_ab_review",
            artifact_id=ab_review_id,
            root=ab_review_dir / ab_review_id,
            names=AB_FILES,
        ),
        "confirmation_source": _binding(
            kind="signal_gate_confirmation",
            artifact_id=confirmation_id,
            root=confirmation_dir / confirmation_id,
            names=CONFIRMATION_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, READINESS_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(
        root / "filtered_formalization_readiness_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_filtered_formalization_readiness", root.name, root / READINESS_VIEWS[0]
    )
    return _run_result(
        filtered_formalization_readiness_report_payload(
            readiness_id=root.name, output_dir=output_dir
        ),
        manifest_path_key="filtered_formalization_manifest_path",
        legacy_directory_key="readiness_dir",
        current_directory_key="filtered_formalization_readiness_dir",
    )


def filtered_formalization_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=readiness_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_filtered_formalization_readiness",
        views=READINESS_VIEWS,
    )
    return {
        **manifest,
        "formalization_readiness_decision": _read_json(root / READINESS_VIEWS[1]),
        "formalization_blockers": _read_json(root / READINESS_VIEWS[2]),
        "reader_brief_section": (root / READINESS_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "filtered_formalization_readiness_input_snapshot.json"),
        "filtered_formalization_readiness_dir": str(root),
    }


def _rebuild_readiness(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_formalization_readiness_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    ab_source = _mapping(snapshot.get("ab_source"))
    confirmation_source = _mapping(snapshot.get("confirmation_source"))
    _validate_binding(ab_source, kind="filtered_candidate_ab_review", names=AB_FILES)
    _validate_binding(
        confirmation_source, kind="signal_gate_confirmation", names=CONFIRMATION_FILES
    )
    ab = _validated_self(
        artifact_id=_binding_id(ab_source),
        output_dir=_binding_root(ab_source).parent,
        validator=validate_filtered_candidate_ab_review_artifact,
        validator_key="ab_review_id",
        reader=filtered_candidate_ab_review_report_payload,
        reader_key="ab_review_id",
        label="filtered candidate A/B review",
    )
    confirmation = _validated_self(
        artifact_id=_binding_id(confirmation_source),
        output_dir=_binding_root(confirmation_source).parent,
        validator=validate_signal_gate_confirmation_artifact,
        validator_key="confirmation_id",
        reader=signal_gate_confirmation_report_payload,
        reader_key="confirmation_id",
        label="signal gate confirmation",
    )
    _require(confirmation.get("ab_review_id") == ab.get("ab_review_id"), "readiness A/B lineage")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, ab, confirmation)
    _, expected = _readiness_material(
        root=root, artifact_id=artifact_id, ab=ab, confirmation=confirmation, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_filtered_formalization_readiness_artifact(
    *, readiness_id: str, output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=readiness_id,
        output_dir=output_dir,
        snapshot_name="filtered_formalization_readiness_input_snapshot.json",
        schema=READINESS_INPUT_SCHEMA,
        id_key="readiness_id",
        view_names=READINESS_VIEWS,
        report_type="etf_dynamic_v3_filtered_formalization_readiness_validation",
        rebuild=_rebuild_readiness,
    )


def _owner_material(
    *, root: Path, artifact_id: str, readiness: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    decision = _mapping(readiness.get("formalization_readiness_decision"))
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": readiness.get("candidate"),
        "evidence_status": decision.get("evidence_status"),
        "readiness_decision": decision.get("decision"),
        "recommended_owner_action": "COLLECT_VALIDATED_DATED_FILTERED_OUTCOMES",
        "key_supporting_evidence": [],
        "key_risks": [
            "validated_dated_filtered_outcomes_missing",
            "completed_confirmation_observations_missing",
        ],
        **_safety(),
    }
    checklist = render_owner_filtered_candidate_checklist(summary)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_filtered_candidate_review_manifest",
        "owner_review_id": artifact_id,
        "readiness_id": readiness.get("readiness_id"),
        "candidate": readiness.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(readiness),
        "owner_filtered_candidate_manifest_path": str(root / OWNER_VIEWS[0]),
        "owner_filtered_candidate_summary_path": str(root / OWNER_VIEWS[1]),
        "owner_filtered_candidate_checklist_path": str(root / OWNER_VIEWS[2]),
        "owner_filtered_candidate_review_report_path": str(root / OWNER_VIEWS[3]),
        "reader_brief_section_path": str(root / OWNER_VIEWS[4]),
        "owner_filtered_candidate_review_input_snapshot_path": str(
            root / "owner_filtered_candidate_review_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        OWNER_VIEWS[0]: foundation._json_bytes(manifest),
        OWNER_VIEWS[1]: foundation._json_bytes(summary),
        OWNER_VIEWS[2]: foundation._text_file_bytes(checklist),
        OWNER_VIEWS[3]: foundation._text_file_bytes(
            render_owner_filtered_candidate_report(manifest, summary, checklist)
        ),
        OWNER_VIEWS[4]: foundation._text_file_bytes(
            render_owner_filtered_candidate_reader_brief(summary)
        ),
    }
    return manifest, views


def render_owner_filtered_candidate_checklist(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Owner Filtered Candidate Checklist",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- action: {summary.get('recommended_owner_action')}",
            "- official target weights: forbidden",
            "- no broker / no production",
            "",
        ]
    )


def render_owner_filtered_candidate_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], checklist: str
) -> str:
    _ = checklist
    return "\n".join(
        [
            f"# Owner Filtered Candidate Review {manifest.get('owner_review_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- recommended_owner_action：{summary.get('recommended_owner_action')}",
            "- safety：manual evidence collection / no official weights / no production",
            "",
        ]
    )


def render_owner_filtered_candidate_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Filtered Candidate Review",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- action: {summary.get('recommended_owner_action')}",
            "- safety: owner review only / no production",
            "",
        ]
    )


@with_artifact_validation_session
def build_owner_filtered_candidate_review(
    *,
    readiness_id: str,
    readiness_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    readiness = _validated_self(
        artifact_id=readiness_id,
        output_dir=readiness_dir,
        validator=validate_filtered_formalization_readiness_artifact,
        validator_key="readiness_id",
        reader=filtered_formalization_readiness_report_payload,
        reader_key="readiness_id",
        label="filtered formalization readiness",
    )
    _chronology(generated, readiness)
    artifact_id = _stable_id("owner-filtered-candidate-review", readiness_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _owner_material(
        root=root, artifact_id=root.name, readiness=readiness, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": OWNER_INPUT_SCHEMA,
        "owner_review_id": root.name,
        "candidate": readiness.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(readiness.get("input_snapshot")).get("policy_source")),
        "readiness_source": _binding(
            kind="filtered_formalization_readiness",
            artifact_id=readiness_id,
            root=readiness_dir / readiness_id,
            names=READINESS_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, OWNER_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(
        root / "owner_filtered_candidate_review_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_owner_filtered_candidate_review", root.name, root / OWNER_VIEWS[0]
    )
    return _run_result(
        owner_filtered_candidate_review_report_payload(
            owner_review_id=root.name, output_dir=output_dir
        ),
        manifest_path_key="owner_filtered_candidate_manifest_path",
        legacy_directory_key="owner_review_dir",
        current_directory_key="owner_filtered_candidate_review_dir",
    )


def owner_filtered_candidate_review_report_payload(
    *,
    owner_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=owner_review_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_owner_filtered_candidate_review",
        views=OWNER_VIEWS,
    )
    return {
        **manifest,
        "owner_filtered_candidate_summary": _read_json(root / OWNER_VIEWS[1]),
        "owner_filtered_candidate_checklist": (root / OWNER_VIEWS[2]).read_text(encoding="utf-8"),
        "reader_brief_section": (root / OWNER_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "owner_filtered_candidate_review_input_snapshot.json"),
        "owner_filtered_candidate_review_dir": str(root),
    }


def _rebuild_owner(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "owner_filtered_candidate_review_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    source = _mapping(snapshot.get("readiness_source"))
    _validate_binding(source, kind="filtered_formalization_readiness", names=READINESS_FILES)
    readiness = _validated_self(
        artifact_id=_binding_id(source),
        output_dir=_binding_root(source).parent,
        validator=validate_filtered_formalization_readiness_artifact,
        validator_key="readiness_id",
        reader=filtered_formalization_readiness_report_payload,
        reader_key="readiness_id",
        label="filtered formalization readiness",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, readiness)
    _, expected = _owner_material(
        root=root, artifact_id=artifact_id, readiness=readiness, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_owner_filtered_candidate_review_artifact(
    *, owner_review_id: str, output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=owner_review_id,
        output_dir=output_dir,
        snapshot_name="owner_filtered_candidate_review_input_snapshot.json",
        schema=OWNER_INPUT_SCHEMA,
        id_key="owner_review_id",
        view_names=OWNER_VIEWS,
        report_type="etf_dynamic_v3_owner_filtered_candidate_review_validation",
        rebuild=_rebuild_owner,
    )


def _decision_material(
    *, root: Path, artifact_id: str, owner: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    summary = _mapping(owner.get("owner_filtered_candidate_summary"))
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": owner.get("candidate"),
        "evidence_status": "INSUFFICIENT_DATA",
        "decision": "COLLECT_DATED_EVIDENCE",
        "confidence": None,
        "reason": [
            "validated_dated_filtered_outcomes_missing",
            "completed_confirmation_observations_missing",
        ],
        "next_action": summary.get("recommended_owner_action"),
        **_safety(),
    }
    task_plan = {
        "schema_version": st.SCHEMA_VERSION,
        # A missing evidence prerequisite is an owner action, not authority to
        # invent or register a development task from an investment-facing report.
        "next_tasks": [],
        "selected_next_action": decision.get("next_action"),
        **_safety(),
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_next_decision_manifest",
        "decision_id": artifact_id,
        "owner_review_id": owner.get("owner_review_id"),
        "candidate": owner.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": "INSUFFICIENT_DATA",
        **_manifest_context(owner),
        "filtered_next_decision_manifest_path": str(root / DECISION_VIEWS[0]),
        "filtered_next_decision_path": str(root / DECISION_VIEWS[1]),
        "next_task_plan_path": str(root / DECISION_VIEWS[2]),
        "filtered_next_decision_report_path": str(root / DECISION_VIEWS[3]),
        "reader_brief_section_path": str(root / DECISION_VIEWS[4]),
        "filtered_next_decision_input_snapshot_path": str(
            root / "filtered_next_decision_input_snapshot.json"
        ),
        **_safety(),
    }
    views = {
        DECISION_VIEWS[0]: foundation._json_bytes(manifest),
        DECISION_VIEWS[1]: foundation._json_bytes(decision),
        DECISION_VIEWS[2]: foundation._json_bytes(task_plan),
        DECISION_VIEWS[3]: foundation._text_file_bytes(
            render_filtered_next_decision_report(manifest, decision, task_plan)
        ),
        DECISION_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_next_decision_reader_brief(decision)
        ),
    }
    return manifest, views


def render_filtered_next_decision_report(
    manifest: Mapping[str, Any], decision: Mapping[str, Any], task_plan: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Filtered Next Decision {manifest.get('decision_id')}",
            "",
            f"- evidence_status：{decision.get('evidence_status')}",
            f"- decision：{decision.get('decision')}",
            f"- next_task_count：{len(_records(task_plan.get('next_tasks')))}",
            "- 解释边界：缺证据时只登记收集任务，不formalize或promote。",
            "- safety：manual research only / no official weights / no production",
            "",
        ]
    )


def render_filtered_next_decision_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Next Decision",
            "",
            f"- evidence_status: {decision.get('evidence_status')}",
            f"- decision: {decision.get('decision')}",
            "- safety: research planning only / no production",
            "",
        ]
    )


@with_artifact_validation_session
def run_filtered_next_decision(
    *,
    owner_review_id: str,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    owner = _validated_self(
        artifact_id=owner_review_id,
        output_dir=owner_review_dir,
        validator=validate_owner_filtered_candidate_review_artifact,
        validator_key="owner_review_id",
        reader=owner_filtered_candidate_review_report_payload,
        reader_key="owner_review_id",
        label="owner filtered candidate review",
    )
    _chronology(generated, owner)
    artifact_id = _stable_id("filtered-next-decision", owner_review_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _decision_material(
        root=root, artifact_id=root.name, owner=owner, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": DECISION_INPUT_SCHEMA,
        "decision_id": root.name,
        "candidate": owner.get("candidate"),
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(owner.get("input_snapshot")).get("policy_source")),
        "owner_review_source": _binding(
            kind="owner_filtered_candidate_review",
            artifact_id=owner_review_id,
            root=owner_review_dir / owner_review_id,
            names=OWNER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, DECISION_VIEWS),
        **_safety(),
    }
    foundation._write_snapshot(root / "filtered_next_decision_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_filtered_next_decision", root.name, root / DECISION_VIEWS[0])
    return _run_result(
        filtered_next_decision_report_payload(decision_id=root.name, output_dir=output_dir),
        manifest_path_key="filtered_next_decision_manifest_path",
        legacy_directory_key="decision_dir",
        current_directory_key="filtered_next_decision_dir",
    )


def filtered_next_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> dict[str, Any]:
    root, manifest = _simple_report_payload(
        artifact_id=decision_id,
        latest=latest,
        output_dir=output_dir,
        latest_pointer="latest_filtered_next_decision",
        views=DECISION_VIEWS,
    )
    return {
        **manifest,
        "filtered_next_decision": _read_json(root / DECISION_VIEWS[1]),
        "next_task_plan": _read_json(root / DECISION_VIEWS[2]),
        "reader_brief_section": (root / DECISION_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_snapshot(root, "filtered_next_decision_input_snapshot.json"),
        "filtered_next_decision_dir": str(root),
    }


def _rebuild_decision(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_next_decision_input_snapshot.json")
    _policy_from_binding(_mapping(snapshot.get("policy_source")))
    source = _mapping(snapshot.get("owner_review_source"))
    _validate_binding(source, kind="owner_filtered_candidate_review", names=OWNER_FILES)
    owner = _validated_self(
        artifact_id=_binding_id(source),
        output_dir=_binding_root(source).parent,
        validator=validate_owner_filtered_candidate_review_artifact,
        validator_key="owner_review_id",
        reader=owner_filtered_candidate_review_report_payload,
        reader_key="owner_review_id",
        label="owner filtered candidate review",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, owner)
    _, expected = _decision_material(
        root=root, artifact_id=artifact_id, owner=owner, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def validate_filtered_next_decision_artifact(
    *, decision_id: str, output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR
) -> dict[str, Any]:
    return _validate_content_stage(
        artifact_id=decision_id,
        output_dir=output_dir,
        snapshot_name="filtered_next_decision_input_snapshot.json",
        schema=DECISION_INPUT_SCHEMA,
        id_key="decision_id",
        view_names=DECISION_VIEWS,
        report_type="etf_dynamic_v3_filtered_next_decision_validation",
        rebuild=_rebuild_decision,
    )


def _validated_self(
    *,
    artifact_id: str,
    output_dir: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    reader: Callable[..., dict[str, Any]],
    reader_key: str,
    label: str,
) -> dict[str, Any]:
    return _validated_upstream(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=validator,
        validator_key=validator_key,
        reader=reader,
        reader_key=reader_key,
        label=label,
    )


def _validate_content_stage(
    *,
    artifact_id: str,
    output_dir: Path,
    snapshot_name: str,
    schema: str,
    id_key: str,
    view_names: Sequence[str],
    report_type: str,
    rebuild: Callable[[Path, str], list[dict[str, Any]]],
) -> dict[str, Any]:
    root = output_dir / artifact_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name=snapshot_name,
        schema=schema,
        id_key=id_key,
        artifact_id=artifact_id,
        view_names=view_names,
    )
    if not ok:
        return _validation_payload(report_type, artifact_id, checks)
    return diagnosis_foundation._validate_content(
        report_type=report_type,
        artifact_id=artifact_id,
        checks=checks,
        rebuild=lambda: rebuild(root, artifact_id),
    )
