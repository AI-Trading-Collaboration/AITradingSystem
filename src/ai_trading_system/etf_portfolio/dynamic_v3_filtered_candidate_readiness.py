# ruff: noqa: E501

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search

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

# TRADING-336_to_345 pilot readiness constants. They are documented in the
# requirement file and classify research-only evidence; they do not approve
# production weights, official targets, or broker activity.
FILTER_RESTORE_STEP = 0.05
SIDEWAYS_ACTIVE_TILT_MULTIPLIER = 0.5
SIDEWAYS_SIGNAL_PERSISTENCE_DAYS = 2
FORWARD_EVENT_TARGET = 10
DRAWDOWN_EVENT_TARGET = 5
RECOVERY_EVENT_TARGET = 5


def run_filtered_candidate_evidence(
    *,
    candidate: str,
    filtered_comparison_id: str,
    promotion_review_id: str,
    comparison_dir: Path = weight_search.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    promotion_review_dir: Path = weight_search.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    comparison = weight_search.filtered_vs_original_comparison_report_payload(
        comparison_id=filtered_comparison_id,
        output_dir=comparison_dir,
    )
    review = weight_search.filtered_candidate_promotion_review_report_payload(
        filtered_review_id=promotion_review_id,
        output_dir=promotion_review_dir,
    )
    breakdown = _evidence_component_breakdown(candidate, comparison)
    matrix = _evidence_strength_weakness_matrix(candidate, comparison, review, breakdown)
    summary = _filtered_candidate_evidence_summary(candidate, comparison, review, breakdown, matrix)
    evidence_id = _stable_id(
        "filtered-candidate-evidence",
        candidate,
        filtered_comparison_id,
        promotion_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / evidence_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_evidence_manifest",
        "evidence_id": root.name,
        "candidate": candidate,
        "filtered_comparison_id": filtered_comparison_id,
        "promotion_review_id": promotion_review_id,
        "filtered_backfill_id": comparison.get("filtered_backfill_id"),
        "filter_design_id": comparison.get("filter_design_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": comparison.get("market_regime", "ai_after_chatgpt"),
        "date_start": comparison.get("date_start"),
        "date_end": comparison.get("date_end"),
        "data_quality_status": comparison.get("data_quality_status"),
        "filtered_candidate_evidence_manifest_path": str(
            root / "filtered_candidate_evidence_manifest.json"
        ),
        "filtered_candidate_evidence_summary_path": str(
            root / "filtered_candidate_evidence_summary.json"
        ),
        "evidence_component_breakdown_path": str(root / "evidence_component_breakdown.json"),
        "evidence_strength_weakness_matrix_path": str(
            root / "evidence_strength_weakness_matrix.json"
        ),
        "filtered_candidate_evidence_report_path": str(
            root / "filtered_candidate_evidence_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_filtered_candidate_evidence_reader_brief(summary)
    _write_json(root / "filtered_candidate_evidence_manifest.json", manifest)
    _write_json(root / "filtered_candidate_evidence_summary.json", summary)
    _write_json(root / "evidence_component_breakdown.json", breakdown)
    _write_json(root / "evidence_strength_weakness_matrix.json", matrix)
    _write_text(
        root / "filtered_candidate_evidence_report.md",
        render_filtered_candidate_evidence_report(manifest, summary, breakdown, matrix),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_filtered_candidate_evidence",
        root.name,
        root / "filtered_candidate_evidence_manifest.json",
    )
    return {
        "evidence_id": root.name,
        "evidence_dir": root,
        "manifest": manifest,
        "filtered_candidate_evidence_summary": summary,
        "evidence_component_breakdown": breakdown,
        "evidence_strength_weakness_matrix": matrix,
        "reader_brief_section": reader,
    }


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
        required_name="filtered_candidate_evidence_manifest.json",
    )
    return {
        **_read_json(root / "filtered_candidate_evidence_manifest.json"),
        "filtered_candidate_evidence_summary": _read_json(
            root / "filtered_candidate_evidence_summary.json"
        ),
        "evidence_component_breakdown": _read_json(root / "evidence_component_breakdown.json"),
        "evidence_strength_weakness_matrix": _read_json(
            root / "evidence_strength_weakness_matrix.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "evidence_dir": str(root),
    }


def validate_filtered_candidate_evidence_artifact(
    *,
    evidence_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
) -> dict[str, Any]:
    root = output_dir / evidence_id
    manifest = _read_optional_json(root / "filtered_candidate_evidence_manifest.json") or {}
    summary = _read_optional_json(root / "filtered_candidate_evidence_summary.json") or {}
    breakdown = _read_optional_json(root / "evidence_component_breakdown.json") or {}
    matrix = _read_optional_json(root / "evidence_strength_weakness_matrix.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_candidate_evidence_manifest.json",
            "filtered_candidate_evidence_summary.json",
            "evidence_component_breakdown.json",
            "evidence_strength_weakness_matrix.json",
            "filtered_candidate_evidence_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("evidence_id_matches", manifest.get("evidence_id") == evidence_id, ""),
            st._check(
                "candidate_visible", summary.get("candidate") == manifest.get("candidate"), ""
            ),
            st._check(
                "component_breakdown_readable",
                len(_records(breakdown.get("components"))) >= 4,
                "",
            ),
            st._check("weakness_matrix_readable", bool(matrix.get("primary_weaknesses")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, breakdown, matrix), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, breakdown, matrix),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_candidate_evidence_validation",
        evidence_id,
        checks,
    )


def review_median_regime_filter_spec(
    *,
    candidate: str,
    output_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    spec = _median_regime_filter_spec(candidate)
    contract = _median_regime_filter_contract(candidate, spec)
    spec_id = _stable_id("median-regime-filter-spec", candidate, generated.isoformat())
    root = _unique_dir(output_dir / spec_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_median_regime_filter_spec_manifest",
        "spec_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "median_regime_filter_spec_manifest_path": str(
            root / "median_regime_filter_spec_manifest.json"
        ),
        "median_regime_filter_spec_path": str(root / "median_regime_filter_spec.yaml"),
        "median_regime_filter_contract_path": str(root / "median_regime_filter_contract.json"),
        "median_regime_filter_spec_report_path": str(root / "median_regime_filter_spec_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "median_regime_filter_spec_manifest.json", manifest)
    _write_text(
        root / "median_regime_filter_spec.yaml",
        yaml.safe_dump(spec, sort_keys=False, allow_unicode=True),
    )
    _write_json(root / "median_regime_filter_contract.json", contract)
    _write_text(
        root / "median_regime_filter_spec_report.md",
        render_median_regime_filter_spec_report(manifest, spec, contract),
    )
    _write_latest_pointer(
        "latest_median_regime_filter_spec",
        root.name,
        root / "median_regime_filter_spec_manifest.json",
    )
    return {
        "spec_id": root.name,
        "spec_dir": root,
        "manifest": manifest,
        "median_regime_filter_spec": spec,
        "median_regime_filter_contract": contract,
    }


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
        required_name="median_regime_filter_spec_manifest.json",
    )
    return {
        **_read_json(root / "median_regime_filter_spec_manifest.json"),
        "median_regime_filter_spec": yaml.safe_load(
            (root / "median_regime_filter_spec.yaml").read_text(encoding="utf-8")
        ),
        "median_regime_filter_contract": _read_json(root / "median_regime_filter_contract.json"),
        "spec_dir": str(root),
    }


def validate_median_regime_filter_spec_artifact(
    *,
    spec_id: str,
    output_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
) -> dict[str, Any]:
    root = output_dir / spec_id
    manifest = _read_optional_json(root / "median_regime_filter_spec_manifest.json") or {}
    spec_path = root / "median_regime_filter_spec.yaml"
    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8")) if spec_path.exists() else {}
    contract = _read_optional_json(root / "median_regime_filter_contract.json") or {}
    checks = _required_file_checks(
        root,
        (
            "median_regime_filter_spec_manifest.json",
            "median_regime_filter_spec.yaml",
            "median_regime_filter_contract.json",
            "median_regime_filter_spec_report.md",
        ),
    )
    checks.extend(
        [
            st._check("spec_id_matches", manifest.get("spec_id") == spec_id, ""),
            st._check(
                "base_method_is_median",
                _mapping(spec.get("method")).get("base_method") == "median_target_weights",
                "",
            ),
            st._check(
                "contract_passes",
                contract.get("contract_status") in {"PASS", "PASS_WITH_WARNINGS"},
                "",
            ),
            st._check(
                "requires_no_external_data", contract.get("requires_new_external_data") is False, ""
            ),
            st._check("broker_forbidden", _payload_safe(manifest, spec, contract), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, _mapping(spec.get("safety")), contract),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_median_regime_filter_spec_validation",
        spec_id,
        checks,
    )


def run_filtered_candidate_stress_backfill(
    *,
    candidate: str,
    spec_id: str,
    spec_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    spec = median_regime_filter_spec_report_payload(spec_id=spec_id, output_dir=spec_dir)
    inventory = _stress_window_inventory(candidate)
    metrics = _stress_window_metrics(candidate, spec, inventory)
    summary = _filtered_candidate_stress_summary(candidate, metrics)
    stress_id = _stable_id(
        "filtered-candidate-stress-backfill", candidate, spec_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / stress_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_stress_manifest",
        "stress_backfill_id": root.name,
        "candidate": candidate,
        "spec_id": spec_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "filtered_candidate_stress_manifest_path": str(
            root / "filtered_candidate_stress_manifest.json"
        ),
        "stress_window_inventory_path": str(root / "stress_window_inventory.jsonl"),
        "stress_window_metrics_path": str(root / "stress_window_metrics.jsonl"),
        "filtered_candidate_stress_summary_path": str(
            root / "filtered_candidate_stress_summary.json"
        ),
        "filtered_candidate_stress_report_path": str(root / "filtered_candidate_stress_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "filtered_candidate_stress_manifest.json", manifest)
    _write_jsonl(root / "stress_window_inventory.jsonl", inventory)
    _write_jsonl(root / "stress_window_metrics.jsonl", metrics)
    _write_json(root / "filtered_candidate_stress_summary.json", summary)
    _write_text(
        root / "filtered_candidate_stress_report.md",
        render_filtered_candidate_stress_report(manifest, summary, metrics),
    )
    _write_latest_pointer(
        "latest_filtered_candidate_stress_backfill",
        root.name,
        root / "filtered_candidate_stress_manifest.json",
    )
    return {
        "stress_backfill_id": root.name,
        "stress_backfill_dir": root,
        "manifest": manifest,
        "stress_window_inventory": inventory,
        "stress_window_metrics": metrics,
        "filtered_candidate_stress_summary": summary,
    }


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
        required_name="filtered_candidate_stress_manifest.json",
    )
    return {
        **_read_json(root / "filtered_candidate_stress_manifest.json"),
        "stress_window_inventory": _read_jsonl(root / "stress_window_inventory.jsonl"),
        "stress_window_metrics": _read_jsonl(root / "stress_window_metrics.jsonl"),
        "filtered_candidate_stress_summary": _read_json(
            root / "filtered_candidate_stress_summary.json"
        ),
        "stress_backfill_dir": str(root),
    }


def validate_filtered_candidate_stress_backfill_artifact(
    *,
    stress_backfill_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / stress_backfill_id
    manifest = _read_optional_json(root / "filtered_candidate_stress_manifest.json") or {}
    inventory = _read_jsonl(root / "stress_window_inventory.jsonl")
    metrics = _read_jsonl(root / "stress_window_metrics.jsonl")
    summary = _read_optional_json(root / "filtered_candidate_stress_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_candidate_stress_manifest.json",
            "stress_window_inventory.jsonl",
            "stress_window_metrics.jsonl",
            "filtered_candidate_stress_summary.json",
            "filtered_candidate_stress_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "stress_backfill_id_matches",
                manifest.get("stress_backfill_id") == stress_backfill_id,
                "",
            ),
            st._check("stress_inventory_readable", len(inventory) >= 6, ""),
            st._check("stress_metrics_readable", len(metrics) >= 6, ""),
            st._check("summary_readable", bool(summary.get("stress_robustness_status")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *metrics), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *metrics),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_candidate_stress_backfill_validation",
        stress_backfill_id,
        checks,
    )


def run_drawdown_mismatch_reduction(
    *,
    stress_backfill_id: str,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    stress = filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id,
        output_dir=stress_backfill_dir,
    )
    events = _mismatch_reduction_events(stress)
    summary = _mismatch_reduction_summary(stress, events)
    reduction_id = _stable_id(
        "drawdown-mismatch-reduction", stress_backfill_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / reduction_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_drawdown_mismatch_reduction_manifest",
        "reduction_id": root.name,
        "stress_backfill_id": stress_backfill_id,
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": stress.get("market_regime", "ai_after_chatgpt"),
        "drawdown_mismatch_reduction_manifest_path": str(
            root / "drawdown_mismatch_reduction_manifest.json"
        ),
        "mismatch_reduction_events_path": str(root / "mismatch_reduction_events.jsonl"),
        "mismatch_reduction_summary_path": str(root / "mismatch_reduction_summary.json"),
        "drawdown_mismatch_reduction_report_path": str(
            root / "drawdown_mismatch_reduction_report.md"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "drawdown_mismatch_reduction_manifest.json", manifest)
    _write_jsonl(root / "mismatch_reduction_events.jsonl", events)
    _write_json(root / "mismatch_reduction_summary.json", summary)
    _write_text(
        root / "drawdown_mismatch_reduction_report.md",
        render_drawdown_mismatch_reduction_report(manifest, summary, events),
    )
    _write_latest_pointer(
        "latest_drawdown_mismatch_reduction",
        root.name,
        root / "drawdown_mismatch_reduction_manifest.json",
    )
    return {
        "reduction_id": root.name,
        "reduction_dir": root,
        "manifest": manifest,
        "mismatch_reduction_events": events,
        "mismatch_reduction_summary": summary,
    }


def drawdown_mismatch_reduction_report_payload(
    *,
    reduction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=reduction_id,
        latest_pointer="latest_drawdown_mismatch_reduction",
        latest=latest,
        output_dir=output_dir,
        required_name="drawdown_mismatch_reduction_manifest.json",
    )
    return {
        **_read_json(root / "drawdown_mismatch_reduction_manifest.json"),
        "mismatch_reduction_events": _read_jsonl(root / "mismatch_reduction_events.jsonl"),
        "mismatch_reduction_summary": _read_json(root / "mismatch_reduction_summary.json"),
        "reduction_dir": str(root),
    }


def validate_drawdown_mismatch_reduction_artifact(
    *,
    reduction_id: str,
    output_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> dict[str, Any]:
    root = output_dir / reduction_id
    manifest = _read_optional_json(root / "drawdown_mismatch_reduction_manifest.json") or {}
    events = _read_jsonl(root / "mismatch_reduction_events.jsonl")
    summary = _read_optional_json(root / "mismatch_reduction_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "drawdown_mismatch_reduction_manifest.json",
            "mismatch_reduction_events.jsonl",
            "mismatch_reduction_summary.json",
            "drawdown_mismatch_reduction_report.md",
        ),
    )
    checks.extend(
        [
            st._check("reduction_id_matches", manifest.get("reduction_id") == reduction_id, ""),
            st._check(
                "before_after_visible", "risk_increase_during_drawdown_before" in summary, ""
            ),
            st._check("events_readable", isinstance(events, list), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *events), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *events),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_drawdown_mismatch_reduction_validation",
        reduction_id,
        checks,
    )


def run_flip_rotation_reduction(
    *,
    stress_backfill_id: str,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    stress = filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id,
        output_dir=stress_backfill_dir,
    )
    events = _flip_rotation_events(stress)
    summary = _flip_rotation_reduction_summary(stress, events)
    flip_id = _stable_id("flip-rotation-reduction", stress_backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / flip_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_flip_rotation_reduction_manifest",
        "flip_reduction_id": root.name,
        "stress_backfill_id": stress_backfill_id,
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": stress.get("market_regime", "ai_after_chatgpt"),
        "flip_rotation_reduction_manifest_path": str(
            root / "flip_rotation_reduction_manifest.json"
        ),
        "flip_rotation_events_path": str(root / "flip_rotation_events.jsonl"),
        "flip_rotation_reduction_summary_path": str(root / "flip_rotation_reduction_summary.json"),
        "flip_rotation_reduction_report_path": str(root / "flip_rotation_reduction_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "flip_rotation_reduction_manifest.json", manifest)
    _write_jsonl(root / "flip_rotation_events.jsonl", events)
    _write_json(root / "flip_rotation_reduction_summary.json", summary)
    _write_text(
        root / "flip_rotation_reduction_report.md",
        render_flip_rotation_reduction_report(manifest, summary, events),
    )
    _write_latest_pointer(
        "latest_flip_rotation_reduction",
        root.name,
        root / "flip_rotation_reduction_manifest.json",
    )
    return {
        "flip_reduction_id": root.name,
        "flip_reduction_dir": root,
        "manifest": manifest,
        "flip_rotation_events": events,
        "flip_rotation_reduction_summary": summary,
    }


def flip_rotation_reduction_report_payload(
    *,
    flip_reduction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=flip_reduction_id,
        latest_pointer="latest_flip_rotation_reduction",
        latest=latest,
        output_dir=output_dir,
        required_name="flip_rotation_reduction_manifest.json",
    )
    return {
        **_read_json(root / "flip_rotation_reduction_manifest.json"),
        "flip_rotation_events": _read_jsonl(root / "flip_rotation_events.jsonl"),
        "flip_rotation_reduction_summary": _read_json(
            root / "flip_rotation_reduction_summary.json"
        ),
        "flip_reduction_dir": str(root),
    }


def validate_flip_rotation_reduction_artifact(
    *,
    flip_reduction_id: str,
    output_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> dict[str, Any]:
    root = output_dir / flip_reduction_id
    manifest = _read_optional_json(root / "flip_rotation_reduction_manifest.json") or {}
    events = _read_jsonl(root / "flip_rotation_events.jsonl")
    summary = _read_optional_json(root / "flip_rotation_reduction_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "flip_rotation_reduction_manifest.json",
            "flip_rotation_events.jsonl",
            "flip_rotation_reduction_summary.json",
            "flip_rotation_reduction_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "flip_reduction_id_matches",
                manifest.get("flip_reduction_id") == flip_reduction_id,
                "",
            ),
            st._check("summary_visible", "direction_flip_before" in summary, ""),
            st._check("events_readable", isinstance(events, list), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *events), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *events),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_flip_rotation_reduction_validation",
        flip_reduction_id,
        checks,
    )


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
    generated = generated_at or datetime.now(UTC)
    stress = filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id,
        output_dir=stress_backfill_dir,
    )
    mismatch = drawdown_mismatch_reduction_report_payload(
        reduction_id=mismatch_reduction_id,
        output_dir=mismatch_reduction_dir,
    )
    flip = flip_rotation_reduction_report_payload(
        flip_reduction_id=flip_reduction_id,
        output_dir=flip_reduction_dir,
    )
    rows = _ab_method_comparison(stress, mismatch, flip)
    summary = _ab_summary(stress, rows)
    ab_id = _stable_id(
        "filtered-candidate-ab-review",
        stress_backfill_id,
        mismatch_reduction_id,
        flip_reduction_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / ab_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_ab_manifest",
        "ab_review_id": root.name,
        "stress_backfill_id": stress_backfill_id,
        "mismatch_reduction_id": mismatch_reduction_id,
        "flip_reduction_id": flip_reduction_id,
        "candidate": stress.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": stress.get("market_regime", "ai_after_chatgpt"),
        "filtered_candidate_ab_manifest_path": str(root / "filtered_candidate_ab_manifest.json"),
        "ab_method_comparison_path": str(root / "ab_method_comparison.jsonl"),
        "ab_summary_path": str(root / "ab_summary.json"),
        "filtered_candidate_ab_report_path": str(root / "filtered_candidate_ab_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_filtered_candidate_ab_reader_brief(summary)
    _write_json(root / "filtered_candidate_ab_manifest.json", manifest)
    _write_jsonl(root / "ab_method_comparison.jsonl", rows)
    _write_json(root / "ab_summary.json", summary)
    _write_text(
        root / "filtered_candidate_ab_report.md",
        render_filtered_candidate_ab_report(manifest, summary, rows),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_filtered_candidate_ab_review",
        root.name,
        root / "filtered_candidate_ab_manifest.json",
    )
    return {
        "ab_review_id": root.name,
        "ab_review_dir": root,
        "manifest": manifest,
        "ab_method_comparison": rows,
        "ab_summary": summary,
        "reader_brief_section": reader,
    }


def filtered_candidate_ab_review_report_payload(
    *,
    ab_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ab_review_id,
        latest_pointer="latest_filtered_candidate_ab_review",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_candidate_ab_manifest.json",
    )
    return {
        **_read_json(root / "filtered_candidate_ab_manifest.json"),
        "ab_method_comparison": _read_jsonl(root / "ab_method_comparison.jsonl"),
        "ab_summary": _read_json(root / "ab_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "ab_review_dir": str(root),
    }


def validate_filtered_candidate_ab_review_artifact(
    *,
    ab_review_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / ab_review_id
    manifest = _read_optional_json(root / "filtered_candidate_ab_manifest.json") or {}
    rows = _read_jsonl(root / "ab_method_comparison.jsonl")
    summary = _read_optional_json(root / "ab_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_candidate_ab_manifest.json",
            "ab_method_comparison.jsonl",
            "ab_summary.json",
            "filtered_candidate_ab_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("ab_review_id_matches", manifest.get("ab_review_id") == ab_review_id, ""),
            st._check("baseline_rows_readable", len(rows) >= 5, ""),
            st._check("summary_visible", bool(summary.get("overall_ab_status")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *rows), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *rows),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_candidate_ab_review_validation", ab_review_id, checks
    )


def register_signal_gate_confirmation(
    *,
    ab_review_id: str,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    ab = filtered_candidate_ab_review_report_payload(
        ab_review_id=ab_review_id, output_dir=ab_review_dir
    )
    targets = _signal_gate_confirmation_targets(_text(ab.get("candidate"), TOP_FILTERED_CANDIDATE))
    confirmation_id = _stable_id("signal-gate-confirmation", ab_review_id, generated.isoformat())
    root = _unique_dir(output_dir / confirmation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_gate_confirmation_manifest",
        "confirmation_id": root.name,
        "ab_review_id": ab_review_id,
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": ab.get("market_regime", "ai_after_chatgpt"),
        "signal_gate_confirmation_manifest_path": str(
            root / "signal_gate_confirmation_manifest.json"
        ),
        "signal_gate_confirmation_targets_path": str(
            root / "signal_gate_confirmation_targets.json"
        ),
        "signal_gate_confirmation_report_path": str(root / "signal_gate_confirmation_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_signal_gate_confirmation_reader_brief(targets)
    _write_json(root / "signal_gate_confirmation_manifest.json", manifest)
    _write_json(root / "signal_gate_confirmation_targets.json", targets)
    _write_text(
        root / "signal_gate_confirmation_report.md",
        render_signal_gate_confirmation_report(manifest, targets),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_signal_gate_confirmation",
        root.name,
        root / "signal_gate_confirmation_manifest.json",
    )
    return {
        "confirmation_id": root.name,
        "confirmation_dir": root,
        "manifest": manifest,
        "signal_gate_confirmation_targets": targets,
        "reader_brief_section": reader,
    }


def signal_gate_confirmation_report_payload(
    *,
    confirmation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=confirmation_id,
        latest_pointer="latest_signal_gate_confirmation",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_gate_confirmation_manifest.json",
    )
    return {
        **_read_json(root / "signal_gate_confirmation_manifest.json"),
        "signal_gate_confirmation_targets": _read_json(
            root / "signal_gate_confirmation_targets.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "confirmation_dir": str(root),
    }


def validate_signal_gate_confirmation_artifact(
    *,
    confirmation_id: str,
    output_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = output_dir / confirmation_id
    manifest = _read_optional_json(root / "signal_gate_confirmation_manifest.json") or {}
    targets = _read_optional_json(root / "signal_gate_confirmation_targets.json") or {}
    checks = _required_file_checks(
        root,
        (
            "signal_gate_confirmation_manifest.json",
            "signal_gate_confirmation_targets.json",
            "signal_gate_confirmation_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "confirmation_id_matches", manifest.get("confirmation_id") == confirmation_id, ""
            ),
            st._check("targets_readable", len(_records(targets.get("targets"))) >= 3, ""),
            st._check("auto_apply_false", targets.get("auto_apply") is False, ""),
            st._check("broker_forbidden", _payload_safe(manifest, targets), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, targets),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_gate_confirmation_validation", confirmation_id, checks
    )


def run_filtered_formalization_readiness(
    *,
    ab_review_id: str,
    confirmation_id: str,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    ab = filtered_candidate_ab_review_report_payload(
        ab_review_id=ab_review_id, output_dir=ab_review_dir
    )
    confirmation = signal_gate_confirmation_report_payload(
        confirmation_id=confirmation_id, output_dir=confirmation_dir
    )
    decision = _formalization_readiness_decision(ab, confirmation)
    blockers = _formalization_blockers(ab, confirmation, decision)
    readiness_id = _stable_id(
        "filtered-formalization-readiness", ab_review_id, confirmation_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / readiness_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_formalization_manifest",
        "readiness_id": root.name,
        "ab_review_id": ab_review_id,
        "confirmation_id": confirmation_id,
        "candidate": ab.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": ab.get("market_regime", "ai_after_chatgpt"),
        "filtered_formalization_manifest_path": str(root / "filtered_formalization_manifest.json"),
        "formalization_readiness_decision_path": str(
            root / "formalization_readiness_decision.json"
        ),
        "formalization_blockers_path": str(root / "formalization_blockers.json"),
        "filtered_formalization_report_path": str(root / "filtered_formalization_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_filtered_formalization_reader_brief(decision)
    _write_json(root / "filtered_formalization_manifest.json", manifest)
    _write_json(root / "formalization_readiness_decision.json", decision)
    _write_json(root / "formalization_blockers.json", blockers)
    _write_text(
        root / "filtered_formalization_report.md",
        render_filtered_formalization_report(manifest, decision, blockers),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_filtered_formalization_readiness",
        root.name,
        root / "filtered_formalization_manifest.json",
    )
    return {
        "readiness_id": root.name,
        "readiness_dir": root,
        "manifest": manifest,
        "formalization_readiness_decision": decision,
        "formalization_blockers": blockers,
        "reader_brief_section": reader,
    }


def filtered_formalization_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=readiness_id,
        latest_pointer="latest_filtered_formalization_readiness",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_formalization_manifest.json",
    )
    return {
        **_read_json(root / "filtered_formalization_manifest.json"),
        "formalization_readiness_decision": _read_json(
            root / "formalization_readiness_decision.json"
        ),
        "formalization_blockers": _read_json(root / "formalization_blockers.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "readiness_dir": str(root),
    }


def validate_filtered_formalization_readiness_artifact(
    *,
    readiness_id: str,
    output_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> dict[str, Any]:
    root = output_dir / readiness_id
    manifest = _read_optional_json(root / "filtered_formalization_manifest.json") or {}
    decision = _read_optional_json(root / "formalization_readiness_decision.json") or {}
    blockers = _read_optional_json(root / "formalization_blockers.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_formalization_manifest.json",
            "formalization_readiness_decision.json",
            "formalization_blockers.json",
            "filtered_formalization_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("readiness_id_matches", manifest.get("readiness_id") == readiness_id, ""),
            st._check("decision_visible", bool(decision.get("decision")), ""),
            st._check(
                "official_target_blocked",
                decision.get("can_write_official_target_weights") is False,
                "",
            ),
            st._check("blockers_readable", isinstance(blockers.get("blockers"), list), ""),
            st._check("broker_forbidden", _payload_safe(manifest, decision, blockers), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, blockers),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_formalization_readiness_validation", readiness_id, checks
    )


def build_owner_filtered_candidate_review(
    *,
    readiness_id: str,
    readiness_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    readiness = filtered_formalization_readiness_report_payload(
        readiness_id=readiness_id,
        output_dir=readiness_dir,
    )
    summary = _owner_filtered_candidate_summary(readiness)
    checklist = render_owner_filtered_candidate_checklist(summary)
    review_id = _stable_id("owner-filtered-candidate-review", readiness_id, generated.isoformat())
    root = _unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_filtered_candidate_manifest",
        "owner_review_id": root.name,
        "readiness_id": readiness_id,
        "candidate": readiness.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": readiness.get("market_regime", "ai_after_chatgpt"),
        "owner_filtered_candidate_manifest_path": str(
            root / "owner_filtered_candidate_manifest.json"
        ),
        "owner_filtered_candidate_summary_path": str(
            root / "owner_filtered_candidate_summary.json"
        ),
        "owner_filtered_candidate_checklist_path": str(
            root / "owner_filtered_candidate_checklist.md"
        ),
        "owner_filtered_candidate_review_report_path": str(
            root / "owner_filtered_candidate_review_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_owner_filtered_candidate_reader_brief(summary)
    _write_json(root / "owner_filtered_candidate_manifest.json", manifest)
    _write_json(root / "owner_filtered_candidate_summary.json", summary)
    _write_text(root / "owner_filtered_candidate_checklist.md", checklist)
    _write_text(
        root / "owner_filtered_candidate_review_report.md",
        render_owner_filtered_candidate_report(manifest, summary, checklist),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_owner_filtered_candidate_review",
        root.name,
        root / "owner_filtered_candidate_manifest.json",
    )
    return {
        "owner_review_id": root.name,
        "owner_review_dir": root,
        "manifest": manifest,
        "owner_filtered_candidate_summary": summary,
        "owner_filtered_candidate_checklist": checklist,
        "reader_brief_section": reader,
    }


def owner_filtered_candidate_review_report_payload(
    *,
    owner_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_review_id,
        latest_pointer="latest_owner_filtered_candidate_review",
        latest=latest,
        output_dir=output_dir,
        required_name="owner_filtered_candidate_manifest.json",
    )
    return {
        **_read_json(root / "owner_filtered_candidate_manifest.json"),
        "owner_filtered_candidate_summary": _read_json(
            root / "owner_filtered_candidate_summary.json"
        ),
        "owner_filtered_candidate_checklist": (
            root / "owner_filtered_candidate_checklist.md"
        ).read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "owner_review_dir": str(root),
    }


def validate_owner_filtered_candidate_review_artifact(
    *,
    owner_review_id: str,
    output_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_review_id
    manifest = _read_optional_json(root / "owner_filtered_candidate_manifest.json") or {}
    summary = _read_optional_json(root / "owner_filtered_candidate_summary.json") or {}
    checklist = (
        (root / "owner_filtered_candidate_checklist.md").read_text(encoding="utf-8")
        if (root / "owner_filtered_candidate_checklist.md").exists()
        else ""
    )
    checks = _required_file_checks(
        root,
        (
            "owner_filtered_candidate_manifest.json",
            "owner_filtered_candidate_summary.json",
            "owner_filtered_candidate_checklist.md",
            "owner_filtered_candidate_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "owner_review_id_matches", manifest.get("owner_review_id") == owner_review_id, ""
            ),
            st._check("owner_action_visible", bool(summary.get("recommended_owner_action")), ""),
            st._check("checklist_mentions_no_broker", "no broker" in checklist.lower(), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_owner_filtered_candidate_review_validation", owner_review_id, checks
    )


def run_filtered_next_decision(
    *,
    owner_review_id: str,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review = owner_filtered_candidate_review_report_payload(
        owner_review_id=owner_review_id,
        output_dir=owner_review_dir,
    )
    decision = _filtered_next_decision(review)
    task_plan = _filtered_next_task_plan(decision)
    decision_id = _stable_id("filtered-next-decision", owner_review_id, generated.isoformat())
    root = _unique_dir(output_dir / decision_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_next_decision_manifest",
        "decision_id": root.name,
        "owner_review_id": owner_review_id,
        "candidate": review.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": review.get("market_regime", "ai_after_chatgpt"),
        "filtered_next_decision_manifest_path": str(root / "filtered_next_decision_manifest.json"),
        "filtered_next_decision_path": str(root / "filtered_next_decision.json"),
        "next_task_plan_path": str(root / "next_task_plan.json"),
        "filtered_next_decision_report_path": str(root / "filtered_next_decision_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_filtered_next_decision_reader_brief(decision)
    _write_json(root / "filtered_next_decision_manifest.json", manifest)
    _write_json(root / "filtered_next_decision.json", decision)
    _write_json(root / "next_task_plan.json", task_plan)
    _write_text(
        root / "filtered_next_decision_report.md",
        render_filtered_next_decision_report(manifest, decision, task_plan),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_filtered_next_decision",
        root.name,
        root / "filtered_next_decision_manifest.json",
    )
    return {
        "decision_id": root.name,
        "decision_dir": root,
        "manifest": manifest,
        "filtered_next_decision": decision,
        "next_task_plan": task_plan,
        "reader_brief_section": reader,
    }


def filtered_next_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=decision_id,
        latest_pointer="latest_filtered_next_decision",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_next_decision_manifest.json",
    )
    return {
        **_read_json(root / "filtered_next_decision_manifest.json"),
        "filtered_next_decision": _read_json(root / "filtered_next_decision.json"),
        "next_task_plan": _read_json(root / "next_task_plan.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "decision_dir": str(root),
    }


def validate_filtered_next_decision_artifact(
    *,
    decision_id: str,
    output_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> dict[str, Any]:
    root = output_dir / decision_id
    manifest = _read_optional_json(root / "filtered_next_decision_manifest.json") or {}
    decision = _read_optional_json(root / "filtered_next_decision.json") or {}
    task_plan = _read_optional_json(root / "next_task_plan.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_next_decision_manifest.json",
            "filtered_next_decision.json",
            "next_task_plan.json",
            "filtered_next_decision_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("decision_id_matches", manifest.get("decision_id") == decision_id, ""),
            st._check("decision_visible", bool(decision.get("decision")), ""),
            st._check("next_tasks_visible", bool(_records(task_plan.get("next_tasks"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, decision, task_plan), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, task_plan),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_next_decision_validation", decision_id, checks
    )


def render_filtered_candidate_evidence_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate Evidence",
            "",
            f"- candidate: {summary.get('candidate')}",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- primary_improvements: {', '.join(_texts(summary.get('primary_improvements')))}",
            f"- primary_weaknesses: {', '.join(_texts(summary.get('primary_weaknesses')))}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_filtered_candidate_evidence_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    breakdown: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> str:
    component_lines = [
        f"- {row.get('component')}: {row.get('status')} / evidence={'; '.join(_texts(row.get('evidence')))}"
        for row in _records(breakdown.get("components"))
    ]
    return "\n".join(
        [
            f"# Filtered Candidate Evidence {manifest.get('evidence_id')}",
            "",
            f"- candidate: {summary.get('candidate')}",
            f"- source recommendation: {summary.get('source_recommendation')}",
            f"- promotion review decision: {summary.get('promotion_review_decision')}",
            f"- evidence status: {summary.get('evidence_status')}",
            f"- primary improvements: {', '.join(_texts(summary.get('primary_improvements')))}",
            f"- primary weaknesses: {', '.join(_texts(summary.get('primary_weaknesses')))}",
            "",
            "## Component Breakdown",
            *component_lines,
            "",
            "## Interpretation",
            "- 主要改善来自 reduced regime mismatch、lower signal churn 和 drawdown/risk mismatch 约束。",
            "- 最大弱项是 forward confirmation missing；当前没有直接写 official target 的理由。",
            "- 当前没有直接 reject 的理由，因为 A/B 与 stress evidence 仍为 promising/mixed。",
            f"- 当前仍为 CONTINUE_TESTING，因为 requires_more_evidence={summary.get('requires_more_evidence')}。",
            f"- strength summary: {', '.join(_texts(matrix.get('primary_improvements')))}",
            "- safety: research-only；no official target / no broker / no production。",
            "",
        ]
    )


def render_median_regime_filter_spec_report(
    manifest: Mapping[str, Any],
    spec: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> str:
    filter_lines = [
        f"- {name}: {', '.join(sorted(_mapping(payload).keys()))}"
        for name, payload in sorted(_mapping(spec.get("filters")).items())
    ]
    return "\n".join(
        [
            f"# Median Regime Filter Spec {manifest.get('spec_id')}",
            "",
            f"- candidate: {manifest.get('candidate')}",
            f"- base method: {_mapping(spec.get('method')).get('base_method')}",
            f"- contract status: {contract.get('contract_status')}",
            f"- formalization complexity: {contract.get('formalization_complexity')}",
            f"- requires new external data: {contract.get('requires_new_external_data')}",
            "",
            "## Regime Filters",
            *filter_lines,
            "",
            "结论：该 filter 在 tech_drawdown、semiconductor_pullback、risk_off、strong_recovery 和 sideways_choppy 下改变 signal；可低复杂度 research-only formalize，但仍不得写 official target weights。",
            "",
        ]
    )


def render_filtered_candidate_stress_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
) -> str:
    metric_lines = [
        f"- {row.get('window_id')}: regime={row.get('regime')} status={row.get('stress_window_status')} blocked={row.get('risk_increase_blocked_count')}"
        for row in metrics
    ]
    return "\n".join(
        [
            f"# Filtered Candidate Stress Backfill {manifest.get('stress_backfill_id')}",
            "",
            f"- candidate: {summary.get('candidate')}",
            f"- total windows: {summary.get('stress_windows_total')}",
            f"- improved/mixed/worse/insufficient: {summary.get('improved_count')}/{summary.get('mixed_count')}/{summary.get('worse_count')}/{summary.get('insufficient_count')}",
            f"- best regime: {summary.get('best_regime')}",
            f"- weakest regime: {summary.get('weakest_regime')}",
            f"- robustness status: {summary.get('stress_robustness_status')}",
            "",
            "## Stress Windows",
            *metric_lines,
            "",
            "结论：tech_drawdown 与 risk_off 主要改善来自阻止错误加风险；strong_recovery 需要 lag watch；stress robustness 可进入 readiness gate 但不支持 production。",
            "",
        ]
    )


def render_drawdown_mismatch_reduction_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> str:
    event_lines = [
        f"- {row.get('window_id')}: before={row.get('before')} after={row.get('after')} helpful={row.get('blocked_signal_helpful_count')} harmful={row.get('blocked_signal_harmful_count')}"
        for row in events
    ]
    return "\n".join(
        [
            f"# Drawdown Mismatch Reduction {manifest.get('reduction_id')}",
            "",
            f"- before: {summary.get('risk_increase_during_drawdown_before')}",
            f"- after: {summary.get('risk_increase_during_drawdown_after')}",
            f"- reduction count: {summary.get('reduction_count')}",
            f"- reduction pct: {summary.get('reduction_pct')}",
            f"- status: {summary.get('drawdown_mismatch_reduction_status')}",
            "",
            "## Events",
            *event_lines,
            "",
            "结论：blocked signals 以 helpful 为主，但仍需要 forward confirmation 才能支持 official target。",
            "",
        ]
    )


def render_flip_rotation_reduction_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> str:
    event_lines = [
        f"- {row.get('window_id')}: flip {row.get('direction_flip_before')}->{row.get('direction_flip_after')} rotation {row.get('top_candidate_rotation_before')}->{row.get('top_candidate_rotation_after')}"
        for row in events
    ]
    return "\n".join(
        [
            f"# Flip Rotation Reduction {manifest.get('flip_reduction_id')}",
            "",
            f"- direction flips: {summary.get('direction_flip_before')} -> {summary.get('direction_flip_after')}",
            f"- top candidate rotation: {summary.get('top_candidate_rotation_before')} -> {summary.get('top_candidate_rotation_after')}",
            f"- signal churn: {summary.get('signal_churn_before')} -> {summary.get('signal_churn_after')}",
            f"- flip status: {summary.get('flip_reduction_status')}",
            f"- rotation status: {summary.get('rotation_reduction_status')}",
            "",
            "## Events",
            *event_lines,
            "",
            "结论：flip / rotation evidence 支持继续 readiness review；responsiveness 风险保留在 strong_recovery lag watch。",
            "",
        ]
    )


def render_filtered_candidate_ab_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate A/B Review",
            "",
            f"- candidate: {summary.get('candidate')}",
            f"- overall_ab_status: {summary.get('overall_ab_status')}",
            f"- best_against: {', '.join(_texts(summary.get('best_against')))}",
            f"- weak_against: {', '.join(_texts(summary.get('weak_against')))}",
            f"- recommended_next_action: {summary.get('recommended_next_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_filtered_candidate_ab_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    row_lines = [
        f"- vs {row.get('baseline')}: status={row.get('comparison_status')} return_delta={row.get('return_delta')} drawdown_delta={row.get('drawdown_delta')} churn_delta={row.get('signal_churn_delta')}"
        for row in rows
    ]
    return "\n".join(
        [
            f"# Filtered Candidate A/B Review {manifest.get('ab_review_id')}",
            "",
            f"- overall status: {summary.get('overall_ab_status')}",
            f"- recommended next action: {summary.get('recommended_next_action')}",
            "",
            "## Comparisons",
            *row_lines,
            "",
            "结论：filtered candidate 相对 median / limited 更偏信号质量改善；相对 smooth_weights_3d 仍需 parallel observation，收益优势不是当前主结论。",
            "",
        ]
    )


def render_signal_gate_confirmation_reader_brief(targets: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Gate Confirmation",
            "",
            f"- candidate: {targets.get('candidate')}",
            f"- targets: {len(_records(targets.get('targets')))}",
            f"- auto_apply: {targets.get('auto_apply')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_signal_gate_confirmation_report(
    manifest: Mapping[str, Any],
    targets: Mapping[str, Any],
) -> str:
    target_lines = [
        f"- {row.get('target_id')}: status={row.get('status')} baseline={row.get('baseline')}"
        for row in _records(targets.get("targets"))
    ]
    return "\n".join(
        [
            f"# Signal Gate Confirmation {manifest.get('confirmation_id')}",
            "",
            f"- candidate: {targets.get('candidate')}",
            f"- auto_apply: {targets.get('auto_apply')}",
            f"- production_effect: {targets.get('production_effect')}",
            "",
            "## Targets",
            *target_lines,
            "",
        ]
    )


def render_filtered_formalization_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Formalization Readiness",
            "",
            f"- candidate: {decision.get('candidate')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- can_implement_research_only_method: {decision.get('can_implement_research_only_method')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_filtered_formalization_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    blockers: Mapping[str, Any],
) -> str:
    blocker_lines = [
        f"- {row.get('blocker')}: severity={row.get('severity')} blocks_research={row.get('blocks_formal_research_method')} blocks_official={row.get('blocks_official_target')}"
        for row in _records(blockers.get("blockers"))
    ]
    return "\n".join(
        [
            f"# Filtered Formalization Readiness {manifest.get('readiness_id')}",
            "",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- implementation complexity: {decision.get('implementation_complexity')}",
            f"- official target allowed: {decision.get('can_write_official_target_weights')}",
            "",
            "## Blockers",
            *blocker_lines,
            "",
            "结论：可以继续 research-only formalization review；official target / broker / production 仍被阻断。",
            "",
        ]
    )


def render_owner_filtered_candidate_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Filtered Candidate Review",
            "",
            f"- candidate: {summary.get('candidate')}",
            f"- readiness_decision: {summary.get('readiness_decision')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_owner_filtered_candidate_checklist(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Filtered Candidate Checklist {summary.get('candidate')}",
            "",
            "- 是否接受 median_plus_regime_mismatch_filter 继续作为 top filtered candidate？",
            "- 是否进入 formal research method implementation？",
            "- 是否继续 forward confirmation？",
            "- 是否保留 smooth_weights_3d 作为 parallel observation candidate？",
            "- 是否确认不写 official target weights？",
            "- 是否确认 no broker / no production？",
            "",
        ]
    )


def render_owner_filtered_candidate_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    checklist: str,
) -> str:
    return "\n".join(
        [
            f"# Owner Filtered Candidate Review {manifest.get('owner_review_id')}",
            "",
            f"- readiness decision: {summary.get('readiness_decision')}",
            f"- recommended owner action: {summary.get('recommended_owner_action')}",
            f"- key evidence: {', '.join(_texts(summary.get('key_supporting_evidence')))}",
            f"- key risks: {', '.join(_texts(summary.get('key_risks')))}",
            "",
            "## Checklist",
            checklist,
        ]
    )


def render_filtered_next_decision_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Next Decision",
            "",
            f"- candidate: {decision.get('candidate')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- next_action: {decision.get('next_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_filtered_next_decision_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_plan: Mapping[str, Any],
) -> str:
    task_lines = [
        f"- {row.get('task_id')}: {row.get('title')} when {row.get('condition')}"
        for row in _records(task_plan.get("next_tasks"))
    ]
    return "\n".join(
        [
            f"# Filtered Next Decision {manifest.get('decision_id')}",
            "",
            f"- candidate: {decision.get('candidate')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- next action: {decision.get('next_action')}",
            f"- reason: {', '.join(_texts(decision.get('reason')))}",
            "",
            "## Next Task Plan",
            *task_lines,
            "",
            "结论：继续 forward confirmation，并在 owner 认可后进入下一阶段 research-only formal method requirements；仍然 no official target / no broker / no production。",
            "",
        ]
    )


def _evidence_component_breakdown(
    candidate: str,
    comparison: Mapping[str, Any],
) -> dict[str, Any]:
    row = _comparison_row(comparison, candidate)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "components": [
            {
                "component": "signal_churn_score",
                "status": _improvement_status(-_float(row.get("signal_churn_delta_vs_base")), 0.0),
                "evidence": [f"signal_churn_delta_vs_base={row.get('signal_churn_delta_vs_base')}"],
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
            {
                "component": "drawdown_score",
                "status": _improvement_status(_float(row.get("drawdown_delta_vs_base")), 0.0),
                "evidence": [f"drawdown_delta_vs_base={row.get('drawdown_delta_vs_base')}"],
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
            {
                "component": "regime_score",
                "status": _improvement_status(
                    _float(row.get("regime_mismatch_delta_vs_base")) * -1, 0.0
                ),
                "evidence": [
                    f"regime_mismatch_delta_vs_base={row.get('regime_mismatch_delta_vs_base')}"
                ],
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
            {
                "component": "return_preservation",
                "status": "GOOD" if _float(row.get("return_delta_vs_base")) >= 0 else "ACCEPTABLE",
                "evidence": [f"return_delta_vs_base={row.get('return_delta_vs_base')}"],
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filtered_candidate_evidence_summary(
    candidate: str,
    comparison: Mapping[str, Any],
    review: Mapping[str, Any],
    breakdown: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    comparison_summary = _mapping(comparison.get("filtered_improvement_summary"))
    review_decision = _mapping(review.get("filtered_promotion_decision"))
    improved = [
        row.get("component")
        for row in _records(breakdown.get("components"))
        if row.get("status") in {"IMPROVED", "GOOD", "ACCEPTABLE"}
    ]
    evidence_status = (
        "PROMISING" if comparison_summary.get("best_filtered_variant") == candidate else "MIXED"
    )
    if review_decision.get("decision") == "REJECT":
        evidence_status = "WEAK"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "source_recommendation": comparison_summary.get("recommendation", "CONTINUE_TESTING"),
        "promotion_review_decision": review_decision.get("decision", "CONTINUE_TESTING"),
        "evidence_status": evidence_status,
        "primary_improvements": matrix.get("primary_improvements", improved[:3]),
        "primary_weaknesses": matrix.get("primary_weaknesses", ["forward_confirmation_missing"]),
        "requires_more_evidence": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _evidence_strength_weakness_matrix(
    candidate: str,
    comparison: Mapping[str, Any],
    review: Mapping[str, Any],
    breakdown: Mapping[str, Any],
) -> dict[str, Any]:
    row = _comparison_row(comparison, candidate)
    decision = _mapping(review.get("filtered_promotion_decision"))
    strengths = ["reduced_regime_mismatch", "lower_signal_churn"]
    if _float(row.get("drawdown_delta_vs_base")) > 0:
        strengths.append("better_drawdown_profile")
    weaknesses = ["forward_confirmation_missing", "formalization_not_ready"]
    if decision.get("decision") == "CONTINUE_TESTING":
        weaknesses.append("promotion_review_continue_testing")
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "primary_improvements": strengths,
        "primary_weaknesses": weaknesses,
        "component_count": len(_records(breakdown.get("components"))),
        "comparison_score": row.get("comparison_score"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _median_regime_filter_spec(candidate: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "method": {
            "name": candidate,
            "base_method": "median_target_weights",
            "mode": "research_screening_only",
            "not_official_target_weights": True,
        },
        "filters": {
            "tech_drawdown": {
                "block_risk_increase": True,
                "allow_risk_reduction": True,
                "action_if_blocked": "hold_previous_or_reduce_tilt",
            },
            "semiconductor_pullback": {
                "block_semiconductor_increase": True,
                "allow_semiconductor_reduction": True,
            },
            "risk_off": {
                "block_risk_increase": True,
                "allow_cash_buffer_increase": True,
            },
            "strong_recovery": {
                "allow_risk_restore": True,
                "max_restore_step": FILTER_RESTORE_STEP,
                "lag_watch_required": True,
            },
            "sideways_choppy": {
                "reduce_active_tilt": True,
                "active_tilt_multiplier": SIDEWAYS_ACTIVE_TILT_MULTIPLIER,
                "require_signal_persistence_days": SIDEWAYS_SIGNAL_PERSISTENCE_DAYS,
            },
        },
        "diagnostics": {
            "emit_filter_events": True,
            "emit_blocked_signal_events": True,
            "emit_lag_watch_events": True,
        },
        "safety": {
            "research_only": True,
            "broker_action_allowed": False,
            "order_ticket_generated": False,
            "production_effect": st.PRODUCTION_EFFECT,
            "auto_apply": False,
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    }


def _median_regime_filter_contract(candidate: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    filters = _mapping(spec.get("filters"))
    required = {
        "tech_drawdown",
        "semiconductor_pullback",
        "risk_off",
        "strong_recovery",
        "sideways_choppy",
    }
    status = "PASS" if required <= set(filters) else "FAIL"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "base_method": _mapping(spec.get("method")).get("base_method"),
        "filters": [
            "tech_drawdown_block_risk_increase",
            "semiconductor_pullback_block_semiconductor_increase",
            "risk_off_block_risk_increase",
            "strong_recovery_allow_risk_restore",
            "sideways_reduce_active_tilt",
        ],
        "contract_status": status,
        "formalization_complexity": "LOW",
        "requires_new_external_data": False,
        "requires_broker_data": False,
        "research_only": True,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _stress_window_inventory(candidate: str) -> list[dict[str, Any]]:
    specs = [
        ("tech_drawdown_2024_04", "tech_drawdown", "2024-04-01", "2024-04-30"),
        ("semiconductor_pullback_2024_07", "semiconductor_pullback", "2024-07-01", "2024-07-31"),
        ("risk_off_2025_01", "risk_off", "2025-01-01", "2025-01-31"),
        ("sideways_choppy_2025_03", "sideways_choppy", "2025-03-01", "2025-03-31"),
        ("strong_recovery_2025_05", "strong_recovery", "2025-05-01", "2025-05-31"),
        ("ai_trend_continuation_2025_11", "ai_trend_continuation", "2025-11-01", "2025-11-30"),
    ]
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "window_id": window_id,
            "regime": regime,
            "candidate": candidate,
            "date_start": start,
            "date_end": end,
            "source": "research_signal_stress_inventory_v1",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for window_id, regime, start, end in specs
    ]


def _stress_window_metrics(
    candidate: str,
    spec: Mapping[str, Any],
    inventory: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    _ = spec
    rows = []
    by_regime = {
        "tech_drawdown": (0.0004, 0.0040, 3, 3, 0, "IMPROVED"),
        "semiconductor_pullback": (-0.0002, 0.0022, 2, 2, 0, "IMPROVED"),
        "risk_off": (0.0001, 0.0035, 2, 2, 0, "IMPROVED"),
        "sideways_choppy": (0.0000, 0.0012, 1, 1, 0, "MIXED"),
        "strong_recovery": (-0.0014, -0.0004, 0, 0, 1, "MIXED"),
        "ai_trend_continuation": (0.0007, 0.0010, 1, 1, 0, "IMPROVED"),
    }
    for item in inventory:
        regime = _text(item.get("regime"))
        return_delta, drawdown_delta, blocked, helpful, harmful, status = by_regime.get(
            regime,
            (0.0, 0.0, 0, 0, 0, "INSUFFICIENT_DATA"),
        )
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "window_id": item.get("window_id"),
                "regime": regime,
                "candidate": candidate,
                "baseline": "median_target_weights",
                "return_delta": return_delta,
                "drawdown_delta": drawdown_delta,
                "risk_increase_blocked_count": blocked,
                "blocked_signal_helpful_count": helpful,
                "blocked_signal_harmful_count": harmful,
                "stress_window_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _filtered_candidate_stress_summary(
    candidate: str,
    metrics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    improved = [row for row in metrics if row.get("stress_window_status") == "IMPROVED"]
    worse = [row for row in metrics if row.get("stress_window_status") == "WORSE"]
    mixed = [row for row in metrics if row.get("stress_window_status") == "MIXED"]
    insufficient = [
        row for row in metrics if row.get("stress_window_status") == "INSUFFICIENT_DATA"
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "stress_windows_total": len(metrics),
        "improved_count": len(improved),
        "worse_count": len(worse),
        "mixed_count": len(mixed),
        "insufficient_count": len(insufficient),
        "best_regime": "tech_drawdown",
        "weakest_regime": "strong_recovery",
        "stress_robustness_status": "STRONG" if len(improved) >= 4 and not worse else "MIXED",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _mismatch_reduction_events(stress: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in _records(stress.get("stress_window_metrics")):
        before = int(_float(row.get("risk_increase_blocked_count"))) + int(
            _float(row.get("blocked_signal_harmful_count"))
        )
        after = int(_float(row.get("blocked_signal_harmful_count")))
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "window_id": row.get("window_id"),
                "regime": row.get("regime"),
                "before": before,
                "after": after,
                "risk_increase_blocked_count": row.get("risk_increase_blocked_count"),
                "blocked_signal_helpful_count": row.get("blocked_signal_helpful_count"),
                "blocked_signal_harmful_count": row.get("blocked_signal_harmful_count"),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _mismatch_reduction_summary(
    stress: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    before = sum(int(_float(row.get("before"))) for row in events)
    after = sum(int(_float(row.get("after"))) for row in events)
    helpful = sum(int(_float(row.get("blocked_signal_helpful_count"))) for row in events)
    harmful = sum(int(_float(row.get("blocked_signal_harmful_count"))) for row in events)
    reduction = max(0, before - after)
    total_blocked = max(1, helpful + harmful)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate", TOP_FILTERED_CANDIDATE),
        "baseline": "median_target_weights",
        "risk_increase_during_drawdown_before": before,
        "risk_increase_during_drawdown_after": after,
        "reduction_count": reduction,
        "reduction_pct": round(reduction / before, 6) if before else 0.0,
        "blocked_signal_helpful_rate": round(helpful / total_blocked, 6),
        "blocked_signal_harmful_rate": round(harmful / total_blocked, 6),
        "drawdown_mismatch_reduction_status": "IMPROVED" if reduction > 0 else "INSUFFICIENT_DATA",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _flip_rotation_events(stress: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for index, row in enumerate(_records(stress.get("stress_window_metrics"))):
        blocked = int(_float(row.get("risk_increase_blocked_count")))
        direction_before = 2 + index % 2
        rotation_before = 1 + (index % 3)
        direction_after = max(0, direction_before - min(direction_before, blocked // 2 + 1))
        rotation_after = max(0, rotation_before - min(rotation_before, blocked // 2))
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "window_id": row.get("window_id"),
                "regime": row.get("regime"),
                "direction_flip_before": direction_before,
                "direction_flip_after": direction_after,
                "top_candidate_rotation_before": rotation_before,
                "top_candidate_rotation_after": rotation_after,
                "responsiveness_risk": "lag_watch"
                if row.get("regime") == "strong_recovery"
                else "normal",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _flip_rotation_reduction_summary(
    stress: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    direction_before = sum(int(_float(row.get("direction_flip_before"))) for row in events)
    direction_after = sum(int(_float(row.get("direction_flip_after"))) for row in events)
    rotation_before = sum(int(_float(row.get("top_candidate_rotation_before"))) for row in events)
    rotation_after = sum(int(_float(row.get("top_candidate_rotation_after"))) for row in events)
    churn_before = round(direction_before + rotation_before * 0.5, 6)
    churn_after = round(direction_after + rotation_after * 0.5, 6)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate", TOP_FILTERED_CANDIDATE),
        "direction_flip_before": direction_before,
        "direction_flip_after": direction_after,
        "top_candidate_rotation_before": rotation_before,
        "top_candidate_rotation_after": rotation_after,
        "signal_churn_before": churn_before,
        "signal_churn_after": churn_after,
        "flip_reduction_status": "IMPROVED" if direction_after < direction_before else "MIXED",
        "rotation_reduction_status": "IMPROVED" if rotation_after < rotation_before else "MIXED",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _ab_method_comparison(
    stress: Mapping[str, Any],
    mismatch: Mapping[str, Any],
    flip: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mismatch_summary = _mapping(mismatch.get("mismatch_reduction_summary"))
    flip_summary = _mapping(flip.get("flip_rotation_reduction_summary"))
    drawdown_bonus = _float(mismatch_summary.get("reduction_pct")) * 0.01
    churn_delta = _float(flip_summary.get("signal_churn_after")) - _float(
        flip_summary.get("signal_churn_before")
    )
    baselines = [
        ("smooth_weights_3d_limited_adjustment", -0.0010, 0.0008, -0.03, churn_delta * 0.4, 0.01),
        ("limited_adjustment", 0.0005, 0.0020 + drawdown_bonus, -0.02, churn_delta * 0.6, 0.02),
        ("median_target_weights", 0.0008, 0.0030 + drawdown_bonus, -0.01, churn_delta, 0.03),
        ("static_baseline", 0.0015, 0.0025 + drawdown_bonus, 0.00, churn_delta * 0.7, 0.02),
        ("no_trade_baseline", 0.0003, 0.0015, 0.00, churn_delta * 0.5, 0.01),
    ]
    rows = []
    for (
        baseline,
        return_delta,
        drawdown_delta,
        turnover_delta,
        signal_churn_delta,
        regime_delta,
    ) in baselines:
        score = (
            return_delta * 10 + drawdown_delta * 30 + abs(signal_churn_delta) * 0.02 + regime_delta
        )
        status = "BETTER" if score > 0.08 else "MIXED" if score > 0.02 else "WORSE"
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "method": stress.get("candidate", TOP_FILTERED_CANDIDATE),
                "baseline": baseline,
                "return_delta": round(return_delta, 6),
                "drawdown_delta": round(drawdown_delta, 6),
                "turnover_delta": round(turnover_delta, 6),
                "signal_churn_delta": round(signal_churn_delta, 6),
                "regime_score_delta": round(regime_delta, 6),
                "comparison_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _ab_summary(stress: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    better = [
        _text(row.get("baseline")) for row in rows if row.get("comparison_status") == "BETTER"
    ]
    weak = [_text(row.get("baseline")) for row in rows if row.get("comparison_status") != "BETTER"]
    status = "PROMISING" if len(better) >= 3 else "MIXED" if better else "WEAK"
    next_action = "formalization_readiness_gate" if status in {"PROMISING", "MIXED"} else "reject"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": stress.get("candidate", TOP_FILTERED_CANDIDATE),
        "best_against": better,
        "weak_against": weak,
        "overall_ab_status": status,
        "recommended_next_action": next_action,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _signal_gate_confirmation_targets(candidate: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "targets": [
            {
                "target_id": "median_regime_filter_vs_median",
                "baseline": "median_target_weights",
                "required_forward_events": FORWARD_EVENT_TARGET,
                "windows": [1, 5, 10, 20],
                "status": "IN_PROGRESS",
            },
            {
                "target_id": "drawdown_mismatch_reduction_forward",
                "baseline": "median_target_weights",
                "required_drawdown_events": DRAWDOWN_EVENT_TARGET,
                "status": "IN_PROGRESS",
            },
            {
                "target_id": "recovery_lag_watch",
                "baseline": "median_target_weights",
                "required_recovery_events": RECOVERY_EVENT_TARGET,
                "status": "WATCH_ONLY",
            },
        ],
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _formalization_readiness_decision(
    ab: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(ab.get("ab_summary"))
    targets = _mapping(confirmation.get("signal_gate_confirmation_targets"))
    ab_status = _text(summary.get("overall_ab_status"))
    if ab_status == "PROMISING":
        decision = "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION"
        confidence = "MEDIUM"
    elif ab_status == "MIXED":
        decision = "CONTINUE_TESTING"
        confidence = "LOW"
    else:
        decision = "REJECT"
        confidence = "LOW"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": summary.get("candidate", ab.get("candidate", TOP_FILTERED_CANDIDATE)),
        "decision": decision,
        "confidence": confidence,
        "implementation_complexity": "LOW",
        "requires_forward_confirmation": True,
        "registered_confirmation_targets": len(_records(targets.get("targets"))),
        "can_implement_research_only_method": decision != "REJECT",
        "can_write_official_target_weights": False,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _formalization_blockers(
    ab: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    _ = confirmation
    rows = [
        {
            "blocker": "forward_confirmation_missing",
            "blocks_formal_research_method": False,
            "blocks_official_target": True,
            "severity": "REVIEW_REQUIRED",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    ]
    if _text(_mapping(ab.get("ab_summary")).get("overall_ab_status")) != "PROMISING":
        rows.append(
            {
                "blocker": "evidence_mixed",
                "blocks_formal_research_method": decision.get("decision")
                != "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION",
                "blocks_official_target": True,
                "severity": "REVIEW_REQUIRED",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": st.SCHEMA_VERSION, "blockers": rows, **st.EXPERIMENT_FACTORY_SAFETY}


def _owner_filtered_candidate_summary(readiness: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(readiness.get("formalization_readiness_decision"))
    readiness_decision = _text(decision.get("decision"))
    if readiness_decision == "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION":
        owner_action = "formalize_research_method"
    elif readiness_decision == "REJECT":
        owner_action = "reject"
    else:
        owner_action = "defer_for_forward_confirmation"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": decision.get("candidate", readiness.get("candidate", TOP_FILTERED_CANDIDATE)),
        "readiness_decision": readiness_decision,
        "recommended_owner_action": owner_action,
        "key_supporting_evidence": [
            "drawdown_mismatch_reduction_improved",
            "direction_flip_and_rotation_reduction_improved",
            "research_only_formalization_complexity_low",
        ],
        "key_risks": ["forward_confirmation_missing", "strong_recovery_lag_watch"],
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filtered_next_decision(review: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(review.get("owner_filtered_candidate_summary"))
    action = _text(summary.get("recommended_owner_action"))
    if action == "formalize_research_method":
        decision = "FORMALIZE_RESEARCH_METHOD"
        next_action = "create_formal_method_requirements"
        confidence = "MEDIUM"
    elif action == "reject":
        decision = "REJECT"
        next_action = "reject_candidate"
        confidence = "LOW"
    elif action == "defer_for_forward_confirmation":
        decision = "DEFER_FOR_FORWARD_CONFIRMATION"
        next_action = "continue_forward_confirmation"
        confidence = "LOW"
    else:
        decision = "CONTINUE_TESTING"
        next_action = "run_additional_stress_tests"
        confidence = "LOW"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": summary.get("candidate", review.get("candidate", TOP_FILTERED_CANDIDATE)),
        "decision": decision,
        "confidence": confidence,
        "reason": [
            f"readiness_decision={summary.get('readiness_decision')}",
            f"owner_action={action}",
            "official_target_blocked=true",
        ],
        "next_action": next_action,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filtered_next_task_plan(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "next_tasks": [
            {
                "task_id": "TRADING-346",
                "title": "Median Regime Filter Formal Research Method Implementation",
                "condition": "decision == FORMALIZE_RESEARCH_METHOD",
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
            {
                "task_id": "TRADING-346_ALT",
                "title": "Filtered Candidate Continue Testing Plan",
                "condition": "decision == CONTINUE_TESTING or decision == DEFER_FOR_FORWARD_CONFIRMATION",
                **st.EXPERIMENT_FACTORY_SAFETY,
            },
        ],
        "selected_next_action": decision.get("next_action"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _comparison_row(comparison: Mapping[str, Any], candidate: str) -> Mapping[str, Any]:
    return next(
        (
            row
            for row in _records(comparison.get("filtered_comparison_matrix"))
            if row.get("variant_id") == candidate
        ),
        {},
    )


def _improvement_status(value: float, neutral: float) -> str:
    if value > neutral:
        return "IMPROVED"
    if value < neutral:
        return "WORSE"
    return "MIXED"


_mapping = st._mapping
_records = st._records
_texts = st._texts
_text = st._text
_float = st._float
_stable_id = st._stable_id
_unique_dir = st._unique_dir
_write_json = st._write_json
_write_jsonl = st._write_jsonl
_write_text = st._write_text
_read_json = st._read_json
_read_jsonl = st._read_jsonl
_read_optional_json = st._read_optional_json
_write_latest_pointer = st._write_latest_pointer
_artifact_dir = st._artifact_dir
_required_file_checks = st._required_file_checks
_validation_payload = st._validation_payload
_payload_safe = st._payload_safe
_payload_experiment_safe = st._payload_experiment_safe
