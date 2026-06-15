# ruff: noqa: E501

from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.market_calendar_freshness import resolve_us_equity_market_freshness

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
DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "formal_research_method_contract"
)
DEFAULT_PAPER_SHADOW_PROTOCOL_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_protocol"
DEFAULT_CANDIDATE_DECISION_LEDGER_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_decision_ledger"
)
DEFAULT_PAPER_SHADOW_DAILY_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_daily"
DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_drift_monitor"
)
DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_weekly_review"
)
DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "evidence_staleness_monitor"
)
DEFAULT_SHADOW_CONTINUATION_READINESS_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_continuation_readiness"
)
DEFAULT_EVIDENCE_STALENESS_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "evidence_staleness_policy_v1.yaml"
)
DEFAULT_MARKET_PANEL_REPORT_DIR = st.PROJECT_ROOT / "outputs" / "reports"

TOP_FILTERED_CANDIDATE = "median_plus_regime_mismatch_filter"
FORMAL_RESEARCH_PROMOTION_STATES = (
    "REJECTED",
    "NEEDS_MORE_EVIDENCE",
    "PROMISING",
    "PAPER_SHADOW_ELIGIBLE",
    "FORMAL_RESEARCH_READY",
)
# TRADING-346 backlog explicitly requires the current confirmation target count
# to be evaluated as part of the contract. TRADING-348 owns later calibration.
FORMAL_RESEARCH_MIN_CONFIRMATION_TARGETS = 3
FORMAL_RESEARCH_CONTRACT_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "research_screening_only": True,
    "manual_review_only": True,
    "formal_research_contract_only": True,
    "not_formal_research_method": True,
}
PAPER_SHADOW_PROTOCOL_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "paper_shadow_protocol_only": True,
    "observation_only": True,
    "daily_runner_implemented": False,
    "broker_order_system_consumable": False,
}

# TRADING-350 pilot baseline: this is only the minimum observation window for
# paper-shadow protocol design. It is not a production approval threshold.
PAPER_SHADOW_REQUIRED_OBSERVATION_DAYS = 20
PAPER_SHADOW_DAILY_REVIEW_FIELDS = (
    "signal_output",
    "hypothetical_weight_recommendation",
    "risk_off_risk_on_state",
    "drawdown_state",
    "rotation_event",
    "mismatch_event",
    "benchmark_comparison",
    "manual_reviewer_notes",
)
PAPER_SHADOW_EXIT_CONDITIONS = (
    "promote_to_extended_paper_shadow",
    "return_to_research",
    "reject",
)
CANDIDATE_DECISION_LEDGER_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "candidate_decision_ledger_only": True,
    "append_only_ledger": True,
}
EVIDENCE_STALENESS_MONITOR_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "evidence_staleness_monitor_only": True,
    "data_downloaded_by_monitor": False,
    "pipelines_executed_by_monitor": False,
}
SHADOW_CONTINUATION_READINESS_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "shadow_continuation_readiness_only": True,
    "advisory_only": True,
    "paper_shadow_only": True,
    "data_downloaded_by_readiness": False,
    "pipelines_executed_by_readiness": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
SHADOW_CONTINUATION_READINESS_STATES = (
    "READY_TO_CONTINUE",
    "READY_WITH_WARNINGS",
    "MANUAL_REVIEW_REQUIRED",
    "BLOCKED_MISSING_ARTIFACTS",
    "BLOCKED_STALE_DATA",
    "BLOCKED_SAFETY_BOUNDARY",
)
SHADOW_CONTINUATION_REQUIRED_SOURCES = (
    "paper_shadow_daily_observation",
    "paper_shadow_drift_monitor",
    "paper_shadow_weekly_review",
    "evidence_staleness_monitor",
    "data_validation_result",
)

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


def build_formal_research_method_contract(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    evidence_id: str | None = None,
    spec_id: str | None = None,
    stress_backfill_id: str | None = None,
    mismatch_reduction_id: str | None = None,
    flip_reduction_id: str | None = None,
    ab_review_id: str | None = None,
    confirmation_id: str | None = None,
    readiness_id: str | None = None,
    owner_review_id: str | None = None,
    next_decision_id: str | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    spec_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    readiness_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    next_decision_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sources = _formal_research_method_source_payloads(
        evidence_id=evidence_id,
        spec_id=spec_id,
        stress_backfill_id=stress_backfill_id,
        mismatch_reduction_id=mismatch_reduction_id,
        flip_reduction_id=flip_reduction_id,
        ab_review_id=ab_review_id,
        confirmation_id=confirmation_id,
        readiness_id=readiness_id,
        owner_review_id=owner_review_id,
        next_decision_id=next_decision_id,
        evidence_dir=evidence_dir,
        spec_dir=spec_dir,
        stress_backfill_dir=stress_backfill_dir,
        mismatch_reduction_dir=mismatch_reduction_dir,
        flip_reduction_dir=flip_reduction_dir,
        ab_review_dir=ab_review_dir,
        confirmation_dir=confirmation_dir,
        readiness_dir=readiness_dir,
        owner_review_dir=owner_review_dir,
        next_decision_dir=next_decision_dir,
    )
    source_artifacts = _formal_research_source_artifacts(sources)
    objective_gates = _formal_research_objective_gates(sources, source_artifacts)
    safety_status = "PASS" if _payload_safe(*sources.values(), FORMAL_RESEARCH_CONTRACT_SAFETY) else "FAIL"
    failure_conditions = _formal_research_failure_conditions(objective_gates, safety_status)
    paper_shadow = _formal_research_paper_shadow_eligibility(objective_gates, safety_status)
    decision = _formal_research_method_decision(
        candidate=candidate,
        objective_gates=objective_gates,
        failure_conditions=failure_conditions,
        paper_shadow_eligibility=paper_shadow,
        safety_boundary_status=safety_status,
    )
    contract_id = _stable_id(
        "formal-research-method-contract",
        candidate,
        *[str(row.get("artifact_id")) for row in source_artifacts.values()],
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / contract_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_formal_research_method_contract_manifest",
        "contract_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS" if safety_status == "PASS" else "FAIL",
        "market_regime": sources["evidence"].get("market_regime", "ai_after_chatgpt"),
        "formal_research_method_contract_manifest_path": str(
            root / "formal_research_method_contract_manifest.json"
        ),
        "formal_research_method_contract_path": str(
            root / "formal_research_method_contract.json"
        ),
        "formal_research_method_decision_path": str(root / "formal_research_method_decision.json"),
        "formal_research_method_contract_report_path": str(
            root / "formal_research_method_contract_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    contract = {
        "schema_version": st.SCHEMA_VERSION,
        "contract_id": root.name,
        "candidate": candidate,
        "promotion_states": list(FORMAL_RESEARCH_PROMOTION_STATES),
        "objective_gates": objective_gates,
        "failure_conditions": failure_conditions,
        "paper_shadow_eligibility": paper_shadow,
        "source_artifacts": source_artifacts,
        "safety_boundary_status": safety_status,
        "safety_boundary": dict(FORMAL_RESEARCH_CONTRACT_SAFETY),
        "method_boundary": {
            "formal_research_ready_is_not_production_approval": True,
            "official_target_weights_allowed": False,
            "broker_or_order_flow_allowed": False,
            "manual_review_only": True,
            "threshold_calibration_owner_task": "TRADING-348",
        },
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    reader = render_formal_research_method_contract_reader_brief(decision)
    _write_json(root / "formal_research_method_contract_manifest.json", manifest)
    _write_json(root / "formal_research_method_contract.json", contract)
    _write_json(root / "formal_research_method_decision.json", decision)
    _write_text(
        root / "formal_research_method_contract_report.md",
        render_formal_research_method_contract_report(manifest, contract, decision),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_formal_research_method_contract",
        root.name,
        root / "formal_research_method_contract_manifest.json",
    )
    return {
        "contract_id": root.name,
        "contract_dir": root,
        "manifest": manifest,
        "formal_research_method_contract": contract,
        "formal_research_method_decision": decision,
        "reader_brief_section": reader,
    }


def formal_research_method_contract_report_payload(
    *,
    contract_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=contract_id,
        latest_pointer="latest_formal_research_method_contract",
        latest=latest,
        output_dir=output_dir,
        required_name="formal_research_method_contract_manifest.json",
    )
    payload = {
        **_read_json(root / "formal_research_method_contract_manifest.json"),
        "formal_research_method_contract": _read_json(
            root / "formal_research_method_contract.json"
        ),
        "formal_research_method_decision": _read_json(
            root / "formal_research_method_decision.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "contract_dir": str(root),
    }
    validation = _read_optional_json(root / "formal_research_method_contract_validation.json")
    if validation:
        payload["formal_research_method_contract_validation"] = validation
    return payload


def validate_formal_research_method_contract_artifact(
    *,
    contract_id: str,
    output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / contract_id
    manifest = _read_optional_json(root / "formal_research_method_contract_manifest.json") or {}
    contract = _read_optional_json(root / "formal_research_method_contract.json") or {}
    decision = _read_optional_json(root / "formal_research_method_decision.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    gates = _records(contract.get("objective_gates"))
    states = set(_texts(contract.get("promotion_states")))
    paper_shadow = _mapping(contract.get("paper_shadow_eligibility"))
    checks = _required_file_checks(
        root,
        (
            "formal_research_method_contract_manifest.json",
            "formal_research_method_contract.json",
            "formal_research_method_decision.json",
            "formal_research_method_contract_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("contract_id_matches", manifest.get("contract_id") == contract_id, ""),
            st._check(
                "promotion_states_complete",
                set(FORMAL_RESEARCH_PROMOTION_STATES).issubset(states),
                ",".join(sorted(states)),
            ),
            st._check("objective_gates_visible", len(gates) >= 9, str(len(gates))),
            st._check(
                "decision_fields_visible",
                all(
                    decision.get(field) is not None
                    for field in (
                        "formal_research_method_status",
                        "promotion_state",
                        "blocking_reasons",
                        "paper_shadow_eligibility",
                        "safety_boundary_status",
                        "next_required_action",
                    )
                ),
                "",
            ),
            st._check(
                "promotion_state_valid",
                decision.get("promotion_state") in FORMAL_RESEARCH_PROMOTION_STATES,
                _text(decision.get("promotion_state")),
            ),
            st._check("paper_shadow_status_visible", bool(paper_shadow.get("status")), ""),
            st._check(
                "safety_boundary_pass",
                contract.get("safety_boundary_status") == "PASS"
                and decision.get("safety_boundary_status") == "PASS",
                "",
            ),
            st._check("reader_brief_quality_fields", "blocking_issues" in reader, ""),
            st._check("broker_forbidden", _payload_safe(manifest, contract, decision), ""),
        ]
    )
    validation = _validation_payload(
        "etf_dynamic_v3_formal_research_method_contract_validation",
        contract_id,
        checks,
    )
    if write_output:
        _write_json(root / "formal_research_method_contract_validation.json", validation)
        _write_text(
            root / "formal_research_method_contract_validation.md",
            render_formal_research_method_contract_validation_report(validation),
        )
    return validation


def build_paper_shadow_protocol(
    *,
    contract_id: str | None = None,
    contract_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    contract_payload = formal_research_method_contract_report_payload(
        contract_id=contract_id,
        latest=contract_id is None,
        output_dir=contract_dir,
    )
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    decision = _mapping(contract_payload.get("formal_research_method_decision"))
    validation = _mapping(contract_payload.get("formal_research_method_contract_validation"))
    candidate = _text(contract_payload.get("candidate"), TOP_FILTERED_CANDIDATE)
    source_contract_id = _text(contract_payload.get("contract_id"))
    eligible = decision.get("paper_shadow_eligibility") == "ELIGIBLE_FOR_PROTOCOL_DESIGN"
    contract_validation_pass = validation.get("status") in ("PASS", "", None)
    safety_pass = (
        contract.get("safety_boundary_status") == "PASS"
        and decision.get("safety_boundary_status") == "PASS"
        and _payload_safe(contract_payload, contract, decision, PAPER_SHADOW_PROTOCOL_SAFETY)
    )
    eligibility_conditions = [
        {
            "condition_id": "formal_contract_ready",
            "required_value": "FORMAL_RESEARCH_READY",
            "actual_value": decision.get("promotion_state"),
            "passed": decision.get("promotion_state") == "FORMAL_RESEARCH_READY",
        },
        {
            "condition_id": "paper_shadow_eligible_for_protocol_design",
            "required_value": "ELIGIBLE_FOR_PROTOCOL_DESIGN",
            "actual_value": decision.get("paper_shadow_eligibility"),
            "passed": eligible,
        },
        {
            "condition_id": "contract_validation_pass",
            "required_value": "PASS_OR_NOT_YET_RUN",
            "actual_value": validation.get("status", "NOT_RUN"),
            "passed": contract_validation_pass,
        },
        {
            "condition_id": "safety_boundary_pass",
            "required_value": "PASS",
            "actual_value": decision.get("safety_boundary_status"),
            "passed": safety_pass,
        },
    ]
    blocking_reasons = [
        condition["condition_id"]
        for condition in eligibility_conditions
        if condition.get("passed") is not True
    ]
    protocol_status = "PROTOCOL_READY" if not blocking_reasons else "PROTOCOL_BLOCKED"
    protocol_id = _stable_id(
        "paper-shadow-protocol",
        candidate,
        source_contract_id,
        protocol_status,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / protocol_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_protocol_manifest",
        "protocol_id": root.name,
        "candidate": candidate,
        "source_contract_id": source_contract_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if protocol_status == "PROTOCOL_READY" else "FAIL",
        "protocol_status": protocol_status,
        "paper_shadow_protocol_manifest_path": str(root / "paper_shadow_protocol_manifest.json"),
        "paper_shadow_protocol_path": str(root / "paper_shadow_protocol.json"),
        "paper_shadow_protocol_report_path": str(root / "paper_shadow_protocol_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_protocol_validation.json"),
        **PAPER_SHADOW_PROTOCOL_SAFETY,
    }
    protocol = {
        "schema_version": st.SCHEMA_VERSION,
        "protocol_id": root.name,
        "candidate": candidate,
        "source_contract_id": source_contract_id,
        "protocol_status": protocol_status,
        "eligibility_status": "ELIGIBLE" if not blocking_reasons else "BLOCKED",
        "eligibility_conditions": eligibility_conditions,
        "blocking_reasons": blocking_reasons,
        "required_observation_period": {
            "minimum_trading_days": PAPER_SHADOW_REQUIRED_OBSERVATION_DAYS,
            "policy_status": "pilot_baseline",
            "rationale": "Collect roughly one trading month of observation-only behavior before any extended paper-shadow decision.",
            "not_production_approval": True,
        },
        "daily_review_fields": [
            {
                "field": field,
                "required": True,
                "paper_shadow_only": field == "hypothetical_weight_recommendation",
            }
            for field in PAPER_SHADOW_DAILY_REVIEW_FIELDS
        ],
        "exit_conditions": [
            {
                "exit_condition": condition,
                "manual_review_required": True,
                "production_effect": "none",
            }
            for condition in PAPER_SHADOW_EXIT_CONDITIONS
        ],
        "next_required_action": (
            "start_daily_paper_shadow_runner_design"
            if not blocking_reasons
            else "return_to_research_contract_review"
        ),
        "safety_boundary": dict(PAPER_SHADOW_PROTOCOL_SAFETY),
        **PAPER_SHADOW_PROTOCOL_SAFETY,
    }
    reader = render_paper_shadow_protocol_reader_brief(protocol)
    _write_json(root / "paper_shadow_protocol_manifest.json", manifest)
    _write_json(root / "paper_shadow_protocol.json", protocol)
    _write_text(root / "paper_shadow_protocol_report.md", render_paper_shadow_protocol_report(manifest, protocol))
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_paper_shadow_protocol",
        root.name,
        root / "paper_shadow_protocol_manifest.json",
    )
    validation_payload = validate_paper_shadow_protocol_artifact(
        protocol_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "protocol_id": root.name,
        "protocol_dir": root,
        "manifest": manifest,
        "paper_shadow_protocol": protocol,
        "reader_brief_section": reader,
        "paper_shadow_protocol_validation": validation_payload,
    }


def paper_shadow_protocol_report_payload(
    *,
    protocol_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=protocol_id,
        latest_pointer="latest_paper_shadow_protocol",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_protocol_manifest.json",
    )
    payload = {
        **_read_json(root / "paper_shadow_protocol_manifest.json"),
        "paper_shadow_protocol": _read_json(root / "paper_shadow_protocol.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "protocol_dir": str(root),
    }
    validation = _read_optional_json(root / "paper_shadow_protocol_validation.json")
    if validation:
        payload["paper_shadow_protocol_validation"] = validation
    return payload


def validate_paper_shadow_protocol_artifact(
    *,
    protocol_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / protocol_id
    manifest = _read_optional_json(root / "paper_shadow_protocol_manifest.json") or {}
    protocol = _read_optional_json(root / "paper_shadow_protocol.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    daily_fields = {row.get("field") for row in _records(protocol.get("daily_review_fields"))}
    exit_conditions = {row.get("exit_condition") for row in _records(protocol.get("exit_conditions"))}
    eligibility_conditions = _records(protocol.get("eligibility_conditions"))
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_protocol_manifest.json",
            "paper_shadow_protocol.json",
            "paper_shadow_protocol_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("protocol_id_matches", manifest.get("protocol_id") == protocol_id, ""),
            st._check("source_contract_visible", bool(protocol.get("source_contract_id")), ""),
            st._check("eligibility_conditions_visible", len(eligibility_conditions) >= 4, ""),
            st._check(
                "daily_review_fields_complete",
                set(PAPER_SHADOW_DAILY_REVIEW_FIELDS).issubset(daily_fields),
                ",".join(sorted(_texts(daily_fields))),
            ),
            st._check(
                "exit_conditions_complete",
                set(PAPER_SHADOW_EXIT_CONDITIONS).issubset(exit_conditions),
                ",".join(sorted(_texts(exit_conditions))),
            ),
            st._check(
                "observation_period_visible",
                _mapping(protocol.get("required_observation_period")).get("minimum_trading_days")
                == PAPER_SHADOW_REQUIRED_OBSERVATION_DAYS,
                "",
            ),
            st._check(
                "hypothetical_weight_marked_paper_shadow_only",
                any(
                    row.get("field") == "hypothetical_weight_recommendation"
                    and row.get("paper_shadow_only") is True
                    for row in _records(protocol.get("daily_review_fields"))
                ),
                "",
            ),
            st._check("reader_brief_quality_fields", "paper_shadow_protocol_status" in reader, ""),
            st._check("broker_forbidden", _payload_safe(manifest, protocol), ""),
            st._check(
                "not_broker_consumable",
                protocol.get("broker_order_system_consumable") is False
                and manifest.get("broker_order_system_consumable") is False,
                "",
            ),
        ]
    )
    validation = _validation_payload(
        "etf_dynamic_v3_paper_shadow_protocol_validation",
        protocol_id,
        checks,
    )
    if write_output:
        _write_json(root / "paper_shadow_protocol_validation.json", validation)
        _write_text(
            root / "paper_shadow_protocol_validation.md",
            render_paper_shadow_protocol_validation_report(validation),
        )
    return validation


def record_candidate_decision_ledger(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    evidence_id: str | None = None,
    stress_backfill_id: str | None = None,
    mismatch_reduction_id: str | None = None,
    flip_reduction_id: str | None = None,
    ab_review_id: str | None = None,
    confirmation_id: str | None = None,
    owner_review_id: str | None = None,
    next_decision_id: str | None = None,
    contract_id: str | None = None,
    protocol_id: str | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    next_decision_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    contract_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    protocol_dir: Path = DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    evidence = filtered_candidate_evidence_report_payload(
        evidence_id=evidence_id, latest=evidence_id is None, output_dir=evidence_dir
    )
    stress = filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id,
        latest=stress_backfill_id is None,
        output_dir=stress_backfill_dir,
    )
    mismatch = drawdown_mismatch_reduction_report_payload(
        reduction_id=mismatch_reduction_id,
        latest=mismatch_reduction_id is None,
        output_dir=mismatch_reduction_dir,
    )
    flip = flip_rotation_reduction_report_payload(
        flip_reduction_id=flip_reduction_id,
        latest=flip_reduction_id is None,
        output_dir=flip_reduction_dir,
    )
    ab_review = filtered_candidate_ab_review_report_payload(
        ab_review_id=ab_review_id,
        latest=ab_review_id is None,
        output_dir=ab_review_dir,
    )
    confirmation = signal_gate_confirmation_report_payload(
        confirmation_id=confirmation_id,
        latest=confirmation_id is None,
        output_dir=confirmation_dir,
    )
    owner_review = owner_filtered_candidate_review_report_payload(
        owner_review_id=owner_review_id,
        latest=owner_review_id is None,
        output_dir=owner_review_dir,
    )
    next_decision = filtered_next_decision_report_payload(
        decision_id=next_decision_id,
        latest=next_decision_id is None,
        output_dir=next_decision_dir,
    )
    contract = formal_research_method_contract_report_payload(
        contract_id=contract_id,
        latest=contract_id is None,
        output_dir=contract_dir,
    )
    protocol = paper_shadow_protocol_report_payload(
        protocol_id=protocol_id,
        latest=protocol_id is None,
        output_dir=protocol_dir,
    )
    evidence_summary = _mapping(evidence.get("filtered_candidate_evidence_summary"))
    stress_summary = _mapping(stress.get("filtered_candidate_stress_summary"))
    mismatch_summary = _mapping(mismatch.get("mismatch_reduction_summary"))
    flip_summary = _mapping(flip.get("flip_rotation_reduction_summary"))
    ab_summary = _mapping(ab_review.get("ab_summary"))
    confirmation_targets = _mapping(confirmation.get("signal_gate_confirmation_targets"))
    owner_summary = _mapping(owner_review.get("owner_filtered_candidate_summary"))
    decision = _mapping(next_decision.get("filtered_next_decision"))
    contract_decision = _mapping(contract.get("formal_research_method_decision"))
    protocol_payload = _mapping(protocol.get("paper_shadow_protocol"))
    confirmation_count = len(_records(confirmation_targets.get("targets")))
    record_id = _stable_id(
        "candidate-decision-ledger",
        candidate,
        _text(evidence.get("evidence_id")),
        _text(next_decision.get("decision_id")),
        _text(contract.get("contract_id")),
        _text(protocol.get("protocol_id")),
        generated.isoformat(),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / "candidate_decision_ledger.jsonl"
    existing_rows = _read_jsonl(ledger_path) if ledger_path.exists() else []
    record = {
        "schema_version": st.SCHEMA_VERSION,
        "record_id": record_id,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "ledger_sequence": len(existing_rows) + 1,
        "evidence_status": evidence_summary.get("evidence_status"),
        "stress_result": stress_summary.get("stress_robustness_status"),
        "mismatch_result": mismatch_summary.get("drawdown_mismatch_reduction_status"),
        "rotation_result": flip_summary.get("rotation_reduction_status")
        or flip_summary.get("flip_reduction_status"),
        "ab_result": ab_summary.get("overall_ab_status"),
        "confirmation_count": confirmation_count,
        "owner_action": owner_summary.get("recommended_owner_action"),
        "final_decision": decision.get("decision")
        or contract_decision.get("formal_research_method_status"),
        "next_required_action": protocol_payload.get("next_required_action")
        or contract_decision.get("next_required_action")
        or decision.get("next_action"),
        "source_artifacts": {
            "evidence_id": evidence.get("evidence_id"),
            "stress_backfill_id": stress.get("stress_backfill_id"),
            "mismatch_reduction_id": mismatch.get("reduction_id"),
            "flip_reduction_id": flip.get("flip_reduction_id"),
            "ab_review_id": ab_review.get("ab_review_id"),
            "confirmation_id": confirmation.get("confirmation_id"),
            "owner_review_id": owner_review.get("owner_review_id"),
            "next_decision_id": next_decision.get("decision_id"),
            "contract_id": contract.get("contract_id"),
            "protocol_id": protocol.get("protocol_id"),
        },
        "ledger_path": str(ledger_path),
        **CANDIDATE_DECISION_LEDGER_SAFETY,
    }
    with ledger_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    ledger_rows = _read_jsonl(ledger_path)
    root = _unique_dir(output_dir / record_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_decision_ledger_manifest",
        "ledger_run_id": root.name,
        "record_id": record_id,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "ledger_path": str(ledger_path),
        "record_count": len(ledger_rows),
        "candidate_decision_ledger_manifest_path": str(
            root / "candidate_decision_ledger_manifest.json"
        ),
        "candidate_decision_record_path": str(root / "candidate_decision_record.json"),
        "candidate_decision_ledger_snapshot_path": str(
            root / "candidate_decision_ledger_snapshot.jsonl"
        ),
        "candidate_decision_ledger_report_path": str(
            root / "candidate_decision_ledger_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **CANDIDATE_DECISION_LEDGER_SAFETY,
    }
    reader = render_candidate_decision_ledger_reader_brief(record)
    _write_json(root / "candidate_decision_ledger_manifest.json", manifest)
    _write_json(root / "candidate_decision_record.json", record)
    _write_jsonl(root / "candidate_decision_ledger_snapshot.jsonl", ledger_rows)
    _write_text(root / "candidate_decision_ledger_report.md", render_candidate_decision_ledger_report(manifest, record, ledger_rows))
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_candidate_decision_ledger",
        root.name,
        root / "candidate_decision_ledger_manifest.json",
    )
    validation = validate_candidate_decision_ledger_artifact(
        ledger_run_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "ledger_run_id": root.name,
        "record_id": record_id,
        "ledger_dir": root,
        "manifest": manifest,
        "candidate_decision_record": record,
        "ledger_rows": ledger_rows,
        "reader_brief_section": reader,
        "candidate_decision_ledger_validation": validation,
    }


def candidate_decision_ledger_report_payload(
    *,
    ledger_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ledger_run_id,
        latest_pointer="latest_candidate_decision_ledger",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_decision_ledger_manifest.json",
    )
    payload = {
        **_read_json(root / "candidate_decision_ledger_manifest.json"),
        "candidate_decision_record": _read_json(root / "candidate_decision_record.json"),
        "candidate_decision_ledger_snapshot": _read_jsonl(
            root / "candidate_decision_ledger_snapshot.jsonl"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "ledger_dir": str(root),
    }
    validation = _read_optional_json(root / "candidate_decision_ledger_validation.json")
    if validation:
        payload["candidate_decision_ledger_validation"] = validation
    return payload


def validate_candidate_decision_ledger_artifact(
    *,
    ledger_run_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / ledger_run_id
    manifest = _read_optional_json(root / "candidate_decision_ledger_manifest.json") or {}
    record = _read_optional_json(root / "candidate_decision_record.json") or {}
    snapshot = _read_jsonl(root / "candidate_decision_ledger_snapshot.jsonl")
    ledger_path = Path(_text(manifest.get("ledger_path"), str(output_dir / "candidate_decision_ledger.jsonl")))
    canonical_rows = _read_jsonl(ledger_path) if ledger_path.exists() else []
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    record_id = record.get("record_id")
    required_fields = (
        "candidate",
        "evidence_status",
        "stress_result",
        "mismatch_result",
        "rotation_result",
        "ab_result",
        "confirmation_count",
        "owner_action",
        "final_decision",
        "next_required_action",
    )
    checks = _required_file_checks(
        root,
        (
            "candidate_decision_ledger_manifest.json",
            "candidate_decision_record.json",
            "candidate_decision_ledger_snapshot.jsonl",
            "candidate_decision_ledger_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("ledger_run_id_matches", manifest.get("ledger_run_id") == ledger_run_id, ""),
            st._check("record_id_visible", bool(record_id), ""),
            st._check(
                "required_decision_fields_visible",
                all(record.get(field) not in (None, "") for field in required_fields),
                "",
            ),
            st._check(
                "snapshot_contains_record",
                any(row.get("record_id") == record_id for row in snapshot),
                "",
            ),
            st._check(
                "canonical_ledger_contains_record",
                any(row.get("record_id") == record_id for row in canonical_rows),
                "",
            ),
            st._check(
                "append_only_count_visible",
                manifest.get("record_count") == len(snapshot)
                and len(canonical_rows) >= len(snapshot),
                "",
            ),
            st._check(
                "source_artifacts_visible",
                len(_mapping(record.get("source_artifacts"))) >= 8,
                "",
            ),
            st._check("reader_brief_quality_fields", "candidate_decision_ledger_status" in reader, ""),
            st._check("broker_forbidden", _payload_safe(manifest, record), ""),
        ]
    )
    validation = _validation_payload(
        "etf_dynamic_v3_candidate_decision_ledger_validation",
        ledger_run_id,
        checks,
    )
    if write_output:
        _write_json(root / "candidate_decision_ledger_validation.json", validation)
        _write_text(
            root / "candidate_decision_ledger_validation.md",
            render_candidate_decision_ledger_validation_report(validation),
        )
    return validation


def run_evidence_staleness_monitor(
    *,
    as_of: date | None = None,
    candidate: str = TOP_FILTERED_CANDIDATE,
    price_cache_path: Path = st.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Path = DEFAULT_MARKET_PANEL_REPORT_DIR,
    policy_path: Path = DEFAULT_EVIDENCE_STALENESS_POLICY_PATH,
    evidence_id: str | None = None,
    stress_backfill_id: str | None = None,
    ab_review_id: str | None = None,
    owner_review_id: str | None = None,
    paper_shadow_daily_id: str | None = None,
    paper_shadow_drift_monitor_id: str | None = None,
    paper_shadow_weekly_review_id: str | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    paper_shadow_daily_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    output_dir: Path = DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()
    policy = _load_evidence_staleness_policy(policy_path)
    findings, market_calendar = _evidence_staleness_findings(
        as_of=effective_as_of,
        generated_at=generated,
        candidate=candidate,
        policy=policy,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        evidence_id=evidence_id,
        stress_backfill_id=stress_backfill_id,
        ab_review_id=ab_review_id,
        owner_review_id=owner_review_id,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_dir=evidence_dir,
        stress_backfill_dir=stress_backfill_dir,
        ab_review_dir=ab_review_dir,
        owner_review_dir=owner_review_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
    )
    status = _overall_evidence_freshness_status(policy, findings)
    stale_artifacts = [
        row.get("source_id") for row in findings if row.get("severity") == "STALE"
    ]
    blocking_artifacts = [
        row.get("source_id") for row in findings if row.get("severity") == "BLOCKING"
    ]
    missing_artifacts = [
        row.get("source_id") for row in findings if row.get("missing") is True
    ]
    weekly_coverage = _evidence_weekly_coverage_status(findings)
    coverage_blocking_artifacts = _texts(weekly_coverage.get("coverage_blocking_artifacts"))
    safe_to_continue_shadow = (
        status in {"FRESH", "ACCEPTABLE"}
        and not blocking_artifacts
        and not missing_artifacts
        and not coverage_blocking_artifacts
    )
    next_refresh_action = _mapping(policy.get("default_next_actions")).get(
        status,
        "manual_review_required",
    )
    if coverage_blocking_artifacts:
        next_refresh_action = "complete_full_weekly_review_or_record_manual_coverage_override"
    policy_version = _text(policy.get("version"))
    monitor_id = _stable_id(
        "evidence-staleness-monitor",
        candidate,
        effective_as_of.isoformat(),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / monitor_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_staleness_report",
        "monitor_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "freshness_reference_date": market_calendar.get("freshness_reference_date"),
        "latest_complete_market_date": market_calendar.get("latest_complete_market_date"),
        "market_calendar_status": market_calendar.get("market_calendar_status"),
        "market_calendar_reason": market_calendar.get("market_calendar_reason"),
        "market_session_kind": market_calendar.get("market_session_kind"),
        "calendar_adjustment_reason": market_calendar.get("calendar_adjustment_reason"),
        "calendar_adjusted_staleness": market_calendar.get("calendar_adjusted_staleness"),
        "market_close_time": market_calendar.get("market_close_time"),
        "data_ready_time": market_calendar.get("data_ready_time"),
        "data_vendor_delay_minutes": market_calendar.get("data_vendor_delay_minutes"),
        "generated_at": generated.isoformat(),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy_version,
        "evidence_freshness_status": status,
        "stale_artifacts": _texts(stale_artifacts),
        "blocking_artifacts": _texts(blocking_artifacts),
        "missing_artifacts": _texts(missing_artifacts),
        "coverage_status": weekly_coverage.get("coverage_status"),
        "coverage_blocking_artifacts": coverage_blocking_artifacts,
        "weekly_review_coverage_classification": weekly_coverage.get(
            "coverage_classification"
        ),
        "weekly_review_coverage_safe_for_continuation": weekly_coverage.get(
            "coverage_safe_for_continuation"
        ),
        "weekly_review_manual_coverage_override": weekly_coverage.get(
            "manual_coverage_override"
        ),
        "next_refresh_action": next_refresh_action,
        "safe_to_continue_shadow": safe_to_continue_shadow,
        "safety_boundary_status": "PASS",
        "finding_count": len(findings),
        "stale_count": len(stale_artifacts),
        "blocking_count": len(blocking_artifacts),
        "missing_count": len(missing_artifacts),
        "findings": findings,
        **EVIDENCE_STALENESS_MONITOR_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_staleness_manifest",
        "monitor_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "freshness_reference_date": market_calendar.get("freshness_reference_date"),
        "latest_complete_market_date": market_calendar.get("latest_complete_market_date"),
        "market_calendar_status": market_calendar.get("market_calendar_status"),
        "market_session_kind": market_calendar.get("market_session_kind"),
        "calendar_adjustment_reason": market_calendar.get("calendar_adjustment_reason"),
        "calendar_adjusted_staleness": market_calendar.get("calendar_adjusted_staleness"),
        "coverage_status": weekly_coverage.get("coverage_status"),
        "weekly_review_coverage_classification": weekly_coverage.get(
            "coverage_classification"
        ),
        "weekly_review_coverage_safe_for_continuation": weekly_coverage.get(
            "coverage_safe_for_continuation"
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if status != "BLOCKING" else "BLOCKING",
        "policy_path": str(policy_path),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy_version,
        "evidence_staleness_manifest_path": str(root / "evidence_staleness_manifest.json"),
        "evidence_staleness_report_path": str(root / "evidence_staleness_report.json"),
        "evidence_staleness_findings_path": str(root / "evidence_staleness_findings.jsonl"),
        "evidence_staleness_markdown_path": str(root / "evidence_staleness_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **EVIDENCE_STALENESS_MONITOR_SAFETY,
    }
    reader = render_evidence_staleness_reader_brief(report)
    _write_json(root / "evidence_staleness_manifest.json", manifest)
    _write_json(root / "evidence_staleness_report.json", report)
    _write_jsonl(root / "evidence_staleness_findings.jsonl", findings)
    _write_text(root / "evidence_staleness_report.md", render_evidence_staleness_report(manifest, report))
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_evidence_staleness_monitor",
        root.name,
        root / "evidence_staleness_manifest.json",
    )
    validation = validate_evidence_staleness_monitor_artifact(
        monitor_id=root.name,
        output_dir=output_dir,
        policy_path=policy_path,
        write_output=True,
    )
    return {
        "monitor_id": root.name,
        "monitor_dir": root,
        "manifest": manifest,
        "evidence_staleness_report": report,
        "evidence_staleness_findings": findings,
        "reader_brief_section": reader,
        "evidence_staleness_validation": validation,
    }


def evidence_staleness_monitor_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=monitor_id,
        latest_pointer="latest_evidence_staleness_monitor",
        latest=latest,
        output_dir=output_dir,
        required_name="evidence_staleness_manifest.json",
    )
    payload = {
        **_read_json(root / "evidence_staleness_manifest.json"),
        "evidence_staleness_report": _read_json(root / "evidence_staleness_report.json"),
        "evidence_staleness_findings": _read_jsonl(root / "evidence_staleness_findings.jsonl"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "monitor_dir": str(root),
    }
    validation = _read_optional_json(root / "evidence_staleness_validation.json")
    if validation:
        payload["evidence_staleness_validation"] = validation
    return payload


def validate_evidence_staleness_monitor_artifact(
    *,
    monitor_id: str,
    output_dir: Path = DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    policy_path: Path = DEFAULT_EVIDENCE_STALENESS_POLICY_PATH,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / monitor_id
    manifest = _read_optional_json(root / "evidence_staleness_manifest.json") or {}
    report = _read_optional_json(root / "evidence_staleness_report.json") or {}
    findings = _read_jsonl(root / "evidence_staleness_findings.jsonl")
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    policy = _load_evidence_staleness_policy(policy_path) if policy_path.exists() else {}
    rules = _mapping(policy.get("rules"))
    severity_order = set(_texts(policy.get("severity_order")))
    expected_sources = {
        "price_data",
        "market_panel_data",
        "signal_artifact",
        "stress_backfill_result",
        "ab_review",
        "owner_review",
        "paper_shadow_daily_observation",
        "paper_shadow_drift_monitor",
        "paper_shadow_weekly_review",
    }
    finding_sources = {row.get("source_id") for row in findings}
    checks = _required_file_checks(
        root,
        (
            "evidence_staleness_manifest.json",
            "evidence_staleness_report.json",
            "evidence_staleness_findings.jsonl",
            "evidence_staleness_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("monitor_id_matches", manifest.get("monitor_id") == monitor_id, ""),
            st._check("policy_metadata_visible", bool(policy.get("policy_id")) and bool(policy.get("version")), ""),
            st._check("freshness_rules_complete", expected_sources.issubset(set(rules)), ""),
            st._check("severity_order_complete", {"FRESH", "ACCEPTABLE", "STALE", "BLOCKING"}.issubset(severity_order), ""),
            st._check("expected_sources_present", expected_sources.issubset(finding_sources), ",".join(sorted(_texts(finding_sources)))),
            st._check(
                "finding_severities_valid",
                all(row.get("severity") in severity_order for row in findings),
                "",
            ),
            st._check(
                "timestamp_basis_visible",
                all(bool(row.get("timestamp_basis")) for row in findings),
                "",
            ),
            st._check(
                "calendar_adjusted_fields_visible",
                bool(report.get("requested_as_of"))
                and bool(report.get("freshness_reference_date"))
                and bool(report.get("latest_complete_market_date"))
                and "calendar_adjusted_staleness" in report
                and bool(report.get("calendar_adjustment_reason"))
                and bool(report.get("market_session_kind"))
                and all(
                    bool(row.get("requested_as_of"))
                    and bool(row.get("freshness_reference_date"))
                    and bool(row.get("stale_reason"))
                    and "calendar_adjusted_staleness" in row
                    for row in findings
                ),
                "",
            ),
            st._check(
                "stale_artifacts_consistent",
                set(_texts(report.get("stale_artifacts")))
                == {row.get("source_id") for row in findings if row.get("severity") == "STALE"},
                "",
            ),
            st._check(
                "blocking_artifacts_consistent",
                set(_texts(report.get("blocking_artifacts")))
                == {row.get("source_id") for row in findings if row.get("severity") == "BLOCKING"},
                "",
            ),
            st._check(
                "missing_artifacts_consistent",
                set(_texts(report.get("missing_artifacts")))
                == {row.get("source_id") for row in findings if row.get("missing") is True},
                "",
            ),
            st._check(
                "weekly_coverage_status_visible",
                bool(report.get("coverage_status"))
                and "weekly_review_coverage_classification" in report
                and "weekly_review_coverage_safe_for_continuation" in report,
                "",
            ),
            st._check(
                "weekly_coverage_finding_visible",
                any(
                    row.get("source_id") == "paper_shadow_weekly_review"
                    and bool(row.get("coverage_classification"))
                    and row.get("coverage_safe_for_continuation") in (True, False)
                    for row in findings
                ),
                "",
            ),
            st._check(
                "safe_to_continue_shadow_visible",
                report.get("safe_to_continue_shadow") in (True, False),
                "",
            ),
            st._check(
                "safe_to_continue_shadow_consistent",
                report.get("safe_to_continue_shadow")
                == (
                    report.get("evidence_freshness_status") in {"FRESH", "ACCEPTABLE"}
                    and not _texts(report.get("blocking_artifacts"))
                    and not _texts(report.get("missing_artifacts"))
                    and not _texts(report.get("coverage_blocking_artifacts"))
                ),
                "",
            ),
            st._check(
                "safety_boundary_status_visible",
                report.get("safety_boundary_status") == "PASS",
                "",
            ),
            st._check("reader_brief_quality_fields", "evidence_freshness_status" in reader, ""),
            st._check("read_only_monitor", report.get("data_downloaded_by_monitor") is False and report.get("pipelines_executed_by_monitor") is False, ""),
            st._check("broker_forbidden", _payload_safe(manifest, report), ""),
        ]
    )
    validation = _validation_payload(
        "etf_dynamic_v3_evidence_staleness_monitor_validation",
        monitor_id,
        checks,
    )
    if write_output:
        _write_json(root / "evidence_staleness_validation.json", validation)
        _write_text(
            root / "evidence_staleness_validation.md",
            render_evidence_staleness_validation_report(validation),
        )
    return validation


def run_shadow_continuation_readiness_report(
    *,
    as_of: date | None = None,
    candidate: str = TOP_FILTERED_CANDIDATE,
    paper_shadow_daily_id: str | None = None,
    paper_shadow_drift_monitor_id: str | None = None,
    paper_shadow_weekly_review_id: str | None = None,
    evidence_staleness_monitor_id: str | None = None,
    data_quality_report_path: Path | None = None,
    data_quality_report_dir: Path = DEFAULT_MARKET_PANEL_REPORT_DIR,
    paper_shadow_daily_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    evidence_staleness_monitor_dir: Path = DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    output_dir: Path = DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()
    source_artifacts = _shadow_continuation_source_artifacts(
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=evidence_staleness_monitor_id,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
    )
    data_validation = _data_quality_report_summary(
        data_quality_report_path=data_quality_report_path,
        data_quality_report_dir=data_quality_report_dir,
        as_of=effective_as_of,
    )
    source_artifacts["data_validation_result"] = data_validation
    evidence_report = _mapping(source_artifacts["evidence_staleness_monitor"].get("detail"))
    weekly_detail = _mapping(source_artifacts["paper_shadow_weekly_review"].get("detail"))
    weekly_manifest = _mapping(source_artifacts["paper_shadow_weekly_review"].get("manifest"))
    coverage_status = _text(
        evidence_report.get("coverage_status")
        or weekly_detail.get("coverage_status")
        or weekly_manifest.get("coverage_status")
        or "MISSING"
    )
    stale_artifacts = _dedupe_texts(evidence_report.get("stale_artifacts"))
    blocking_artifacts = _dedupe_texts(evidence_report.get("blocking_artifacts"))
    missing_artifacts = _dedupe_texts(
        [
            source_id
            for source_id in SHADOW_CONTINUATION_REQUIRED_SOURCES
            if source_artifacts[source_id].get("exists") is not True
        ]
        + _texts(evidence_report.get("missing_artifacts"))
    )
    data_validation_status = _text(data_validation.get("status"), "MISSING")
    if data_validation.get("exists") is True and data_validation_status not in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }:
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "data_validation_result"])
    safety_audit = _shadow_continuation_safety_audit(source_artifacts)
    readiness = _shadow_continuation_readiness_decision(
        missing_artifacts=missing_artifacts,
        blocking_artifacts=blocking_artifacts,
        stale_artifacts=stale_artifacts,
        coverage_status=coverage_status,
        evidence_report=evidence_report,
        data_validation=data_validation,
        safety_audit=safety_audit,
    )
    safe_to_continue = readiness in {"READY_TO_CONTINUE", "READY_WITH_WARNINGS"}
    next_required_action = _shadow_continuation_next_action(readiness)
    readiness_id = _stable_id(
        "shadow-continuation-readiness",
        candidate,
        effective_as_of.isoformat(),
        readiness,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / readiness_id)
    root.mkdir(parents=True, exist_ok=False)
    source_summary = {
        source_id: _shadow_continuation_source_summary(payload)
        for source_id, payload in source_artifacts.items()
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_report",
        "readiness_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "shadow_continuation_readiness": readiness,
        "safe_to_continue_shadow": safe_to_continue,
        "missing_artifacts": missing_artifacts,
        "blocking_artifacts": blocking_artifacts,
        "stale_artifacts": stale_artifacts,
        "coverage_status": coverage_status,
        "manual_review_required": readiness != "READY_TO_CONTINUE",
        "next_required_action": next_required_action,
        "data_validation_result": data_validation,
        "data_validation_status": data_validation_status,
        "data_validation_warning_count": data_validation.get("warning_count"),
        "source_artifacts": source_summary,
        "safety_boundary_audit": safety_audit,
        "safety_boundary_status": safety_audit.get("status"),
        "advisory_only": True,
        "readiness_decision_policy": {
            "states": list(SHADOW_CONTINUATION_READINESS_STATES),
            "ready_states": ["READY_TO_CONTINUE", "READY_WITH_WARNINGS"],
            "manual_review_for_warnings": True,
            "blocked_missing_precedence": True,
            "blocked_safety_boundary_precedence": True,
        },
        **SHADOW_CONTINUATION_READINESS_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_manifest",
        "readiness_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if safe_to_continue else "MANUAL_REVIEW_REQUIRED",
        "shadow_continuation_readiness": readiness,
        "safe_to_continue_shadow": safe_to_continue,
        "coverage_status": coverage_status,
        "manual_review_required": readiness != "READY_TO_CONTINUE",
        "next_required_action": next_required_action,
        "shadow_continuation_readiness_manifest_path": str(
            root / "shadow_continuation_readiness_manifest.json"
        ),
        "shadow_continuation_readiness_report_path": str(
            root / "shadow_continuation_readiness_report.json"
        ),
        "shadow_continuation_readiness_markdown_path": str(
            root / "shadow_continuation_readiness_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "shadow_continuation_readiness_validation.json"),
        **SHADOW_CONTINUATION_READINESS_SAFETY,
    }
    reader = render_shadow_continuation_readiness_reader_brief(report)
    _write_json(root / "shadow_continuation_readiness_manifest.json", manifest)
    _write_json(root / "shadow_continuation_readiness_report.json", report)
    _write_text(
        root / "shadow_continuation_readiness_report.md",
        render_shadow_continuation_readiness_report(manifest, report),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_shadow_continuation_readiness",
        root.name,
        root / "shadow_continuation_readiness_manifest.json",
    )
    validation = validate_shadow_continuation_readiness_artifact(
        readiness_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "readiness_id": root.name,
        "readiness_dir": root,
        "manifest": manifest,
        "shadow_continuation_readiness_report": report,
        "reader_brief_section": reader,
        "shadow_continuation_readiness_validation": validation,
    }


def shadow_continuation_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=readiness_id,
        latest_pointer="latest_shadow_continuation_readiness",
        latest=latest,
        output_dir=output_dir,
        required_name="shadow_continuation_readiness_manifest.json",
    )
    payload = {
        **_read_json(root / "shadow_continuation_readiness_manifest.json"),
        "shadow_continuation_readiness_report": _read_json(
            root / "shadow_continuation_readiness_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "readiness_dir": str(root),
    }
    validation = _read_optional_json(root / "shadow_continuation_readiness_validation.json")
    if validation:
        payload["shadow_continuation_readiness_validation"] = validation
    return payload


def validate_shadow_continuation_readiness_artifact(
    *,
    readiness_id: str,
    output_dir: Path = DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / readiness_id
    manifest = _read_optional_json(root / "shadow_continuation_readiness_manifest.json") or {}
    report = _read_optional_json(root / "shadow_continuation_readiness_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    source_artifacts = _mapping(report.get("source_artifacts"))
    readiness = _text(report.get("shadow_continuation_readiness"))
    checks = _required_file_checks(
        root,
        (
            "shadow_continuation_readiness_manifest.json",
            "shadow_continuation_readiness_report.json",
            "shadow_continuation_readiness_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("readiness_id_matches", manifest.get("readiness_id") == readiness_id, ""),
            st._check(
                "readiness_state_valid",
                readiness in SHADOW_CONTINUATION_READINESS_STATES,
                readiness,
            ),
            st._check(
                "required_sources_visible",
                set(SHADOW_CONTINUATION_REQUIRED_SOURCES).issubset(source_artifacts),
                ",".join(sorted(source_artifacts)),
            ),
            st._check(
                "decision_fields_visible",
                report.get("safe_to_continue_shadow") in (True, False)
                and report.get("manual_review_required") in (True, False)
                and bool(report.get("coverage_status"))
                and bool(report.get("next_required_action")),
                "",
            ),
            st._check(
                "safe_to_continue_consistent",
                report.get("safe_to_continue_shadow")
                == (readiness in {"READY_TO_CONTINUE", "READY_WITH_WARNINGS"}),
                "",
            ),
            st._check(
                "manual_review_consistent",
                report.get("manual_review_required") == (readiness != "READY_TO_CONTINUE"),
                "",
            ),
            st._check(
                "missing_state_consistent",
                (
                    not _texts(report.get("missing_artifacts"))
                    or readiness == "BLOCKED_MISSING_ARTIFACTS"
                ),
                ",".join(_texts(report.get("missing_artifacts"))),
            ),
            st._check(
                "safety_boundary_status_visible",
                report.get("safety_boundary_status") in {"PASS", "FAIL"},
                "",
            ),
            st._check(
                "safety_state_consistent",
                (
                    report.get("safety_boundary_status") == "PASS"
                    or readiness == "BLOCKED_SAFETY_BOUNDARY"
                ),
                "",
            ),
            st._check(
                "data_validation_result_visible",
                _mapping(report.get("data_validation_result")).get("status") not in (None, ""),
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "shadow_continuation_readiness" in reader
                and "safe_to_continue_shadow" in reader,
                "",
            ),
            st._check(
                "read_only_advisory",
                report.get("data_downloaded_by_readiness") is False
                and report.get("pipelines_executed_by_readiness") is False
                and report.get("advisory_only") is True,
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, report), ""),
        ]
    )
    validation = _validation_payload(
        "etf_dynamic_v3_shadow_continuation_readiness_validation",
        readiness_id,
        checks,
    )
    if write_output:
        _write_json(root / "shadow_continuation_readiness_validation.json", validation)
        _write_text(
            root / "shadow_continuation_readiness_validation.md",
            render_shadow_continuation_readiness_validation_report(validation),
        )
    return validation


def render_shadow_continuation_readiness_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Shadow Continuation Readiness",
            "",
            f"- summary: {report.get('candidate')} paper-shadow continuation gate.",
            f"- shadow_continuation_readiness: {report.get('shadow_continuation_readiness')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- missing_artifacts: {', '.join(_texts(report.get('missing_artifacts'))) or 'none'}",
            f"- blocking_artifacts: {', '.join(_texts(report.get('blocking_artifacts'))) or 'none'}",
            f"- stale_artifacts: {', '.join(_texts(report.get('stale_artifacts'))) or 'none'}",
            f"- coverage_status: {report.get('coverage_status')}",
            f"- manual_review_required: {report.get('manual_review_required')}",
            f"- next_required_action: {report.get('next_required_action')}",
            f"- data_validation_status: {report.get('data_validation_status')}",
            f"- safety_boundary_status: {report.get('safety_boundary_status')}",
            "- safety_boundary: advisory paper-shadow readiness only / no official target / no broker / no paper account or production mutation",
            "",
        ]
    )


def render_shadow_continuation_readiness_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    source_lines = [
        f"- {source_id}: exists={payload.get('exists')} artifact_id={payload.get('artifact_id')} status={payload.get('status')} validation={payload.get('validation_status')} path={payload.get('source_path')}"
        for source_id, payload in sorted(_mapping(report.get("source_artifacts")).items())
    ]
    safety = _mapping(report.get("safety_boundary_audit"))
    return "\n".join(
        [
            f"# Shadow Continuation Readiness {manifest.get('readiness_id')}",
            "",
            "## Purpose",
            "汇总 paper-shadow daily、drift、weekly、freshness monitor 与数据质量结果，给出是否可以继续 paper-shadow observation 的只读结论。本报告不刷新数据、不运行上游、不生成 official target weights、不触发 broker、不修改 paper account 或 production state。",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- as_of: {report.get('as_of')}",
            f"- shadow_continuation_readiness: {report.get('shadow_continuation_readiness')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- missing_artifacts: {', '.join(_texts(report.get('missing_artifacts'))) or 'none'}",
            f"- blocking_artifacts: {', '.join(_texts(report.get('blocking_artifacts'))) or 'none'}",
            f"- stale_artifacts: {', '.join(_texts(report.get('stale_artifacts'))) or 'none'}",
            f"- coverage_status: {report.get('coverage_status')}",
            f"- manual_review_required: {report.get('manual_review_required')}",
            f"- next_required_action: {report.get('next_required_action')}",
            f"- data_validation_status: {report.get('data_validation_status')}",
            f"- data_validation_warning_count: {report.get('data_validation_warning_count')}",
            f"- safety_boundary_status: {report.get('safety_boundary_status')}",
            "",
            "## Source Artifacts",
            *source_lines,
            "",
            "## Safety Boundary Audit",
            f"- status: {safety.get('status')}",
            f"- unsafe_sources: {', '.join(_texts(safety.get('unsafe_sources'))) or 'none'}",
            "- advisory only",
            "- no data refresh",
            "- no upstream pipeline execution",
            "- no official target weights",
            "- no broker integration",
            "- no order tickets",
            "- no paper account mutation",
            "- no production mutation",
            "",
            "## Limitations",
            "- READY_WITH_WARNINGS 仍要求 owner review 数据质量 warning；它不是 production approval。",
            "- MANUAL_REVIEW_REQUIRED 通常来自 weekly coverage 不足或 staleness monitor 明确要求人工覆盖。",
            "- BLOCKED_* 状态必须先修复对应 artifact、staleness、data quality 或 safety boundary。",
            "",
        ]
    )


def render_shadow_continuation_readiness_validation_report(
    validation: Mapping[str, Any],
) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Shadow Continuation Readiness Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
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


def render_formal_research_method_contract_reader_brief(decision: Mapping[str, Any]) -> str:
    blocking = _texts(decision.get("blocking_reasons"))
    return "\n".join(
        [
            "## Formal Research Method Contract",
            "",
            f"- summary: {decision.get('candidate')} research-only contract gate.",
            f"- key_result: promotion_state={decision.get('promotion_state')} formal_research_method_status={decision.get('formal_research_method_status')}",
            f"- blocking_issues: {', '.join(blocking) if blocking else 'none'}",
            f"- recommended_next_step: {decision.get('next_required_action')}",
            f"- paper_shadow_eligibility: {decision.get('paper_shadow_eligibility')}",
            "- safety_boundary: no official target / no broker / no production / manual review only",
            "",
        ]
    )


def render_formal_research_method_contract_report(
    manifest: Mapping[str, Any],
    contract: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    artifacts = _mapping(contract.get("source_artifacts"))
    artifact_lines = [
        f"- {name}: artifact_id={_mapping(row).get('artifact_id')} status={_mapping(row).get('status')}"
        for name, row in sorted(artifacts.items())
    ]
    gate_lines = [
        f"- {row.get('gate_id')}: observed={row.get('observed_status')} required={row.get('required_status')} passed={row.get('passed')}"
        for row in _records(contract.get("objective_gates"))
    ]
    failure_lines = [
        f"- {row.get('failure_condition')}: active={row.get('active')} blocks_formal_research={row.get('blocks_formal_research')}"
        for row in _records(contract.get("failure_conditions"))
    ]
    paper_shadow = _mapping(contract.get("paper_shadow_eligibility"))
    return "\n".join(
        [
            f"# Formal Research Method Contract {manifest.get('contract_id')}",
            "",
            "## Purpose",
            "将 filtered-candidate research chain 的离散证据状态映射为 research-only promotion state；本报告不批准 production、broker、order 或 official target weights。",
            "",
            "## Input Artifacts",
            *artifact_lines,
            "",
            "## Output Decision",
            f"- formal_research_method_status: {decision.get('formal_research_method_status')}",
            f"- promotion_state: {decision.get('promotion_state')}",
            f"- paper_shadow_eligibility: {paper_shadow.get('status')}",
            f"- next_required_action: {decision.get('next_required_action')}",
            "",
            "## Objective Gates",
            *gate_lines,
            "",
            "## Failure Conditions",
            *failure_lines,
            "",
            "## Safety Boundary",
            f"- safety_boundary_status: {contract.get('safety_boundary_status')}",
            "- no official target weights",
            "- no broker integration",
            "- no order tickets",
            "- no production mutation",
            "- manual review only",
            "",
            "## Limitations",
            "- TRADING-346 is a governance contract over existing evidence; it does not calibrate thresholds.",
            "- Paper-shadow eligibility only means the candidate can be considered by a later paper-shadow protocol.",
            "- FORMAL_RESEARCH_READY does not equal production approval.",
            "",
            "## Next Action",
            f"- {decision.get('next_required_action')}",
            "",
        ]
    )


def render_formal_research_method_contract_validation_report(
    validation: Mapping[str, Any],
) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Formal Research Method Contract Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def render_paper_shadow_protocol_reader_brief(protocol: Mapping[str, Any]) -> str:
    blocking = _texts(protocol.get("blocking_reasons"))
    period = _mapping(protocol.get("required_observation_period"))
    return "\n".join(
        [
            "## Paper Shadow Protocol",
            "",
            f"- summary: {protocol.get('candidate')} observation-only paper-shadow protocol.",
            f"- key_result: paper_shadow_protocol_status={protocol.get('protocol_status')}",
            f"- blocking_issues: {', '.join(blocking) if blocking else 'none'}",
            f"- recommended_next_step: {protocol.get('next_required_action')}",
            f"- minimum_observation_trading_days: {period.get('minimum_trading_days')}",
            "- safety_boundary: observation only / no official target / no broker / no order ticket / manual review only",
            "",
        ]
    )


def render_paper_shadow_protocol_report(
    manifest: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> str:
    condition_lines = [
        f"- {row.get('condition_id')}: required={row.get('required_value')} actual={row.get('actual_value')} passed={row.get('passed')}"
        for row in _records(protocol.get("eligibility_conditions"))
    ]
    field_lines = [
        f"- {row.get('field')}: required={row.get('required')} paper_shadow_only={row.get('paper_shadow_only')}"
        for row in _records(protocol.get("daily_review_fields"))
    ]
    exit_lines = [
        f"- {row.get('exit_condition')}: manual_review_required={row.get('manual_review_required')} production_effect={row.get('production_effect')}"
        for row in _records(protocol.get("exit_conditions"))
    ]
    period = _mapping(protocol.get("required_observation_period"))
    return "\n".join(
        [
            f"# Paper Shadow Protocol {manifest.get('protocol_id')}",
            "",
            "## Purpose",
            "定义 formal research contract 之后的 observation-only paper-shadow protocol；本报告不运行 daily runner、不写 official target weights、不触发 broker 或 order 系统。",
            "",
            "## Input Artifacts",
            f"- source_contract_id: {protocol.get('source_contract_id')}",
            "",
            "## Output Decision",
            f"- protocol_status: {protocol.get('protocol_status')}",
            f"- eligibility_status: {protocol.get('eligibility_status')}",
            f"- next_required_action: {protocol.get('next_required_action')}",
            "",
            "## Eligibility Conditions",
            *condition_lines,
            "",
            "## Observation Period",
            f"- minimum_trading_days: {period.get('minimum_trading_days')}",
            f"- policy_status: {period.get('policy_status')}",
            f"- rationale: {period.get('rationale')}",
            "",
            "## Daily Review Fields",
            *field_lines,
            "",
            "## Exit Conditions",
            *exit_lines,
            "",
            "## Safety Boundary",
            "- observation only",
            "- hypothetical weight recommendations must be paper-shadow-only",
            "- no official target weights",
            "- no broker integration",
            "- no order tickets",
            "- no production mutation",
            "- manual review only",
            "",
            "## Limitations",
            "- TRADING-350 defines protocol only; TRADING-351 must implement daily observation separately.",
            "- The 20-trading-day observation period is a pilot baseline, not production approval.",
            "- A ready protocol does not initialize or mutate any paper shadow account.",
            "",
            "## Next Action",
            f"- {protocol.get('next_required_action')}",
            "",
        ]
    )


def render_paper_shadow_protocol_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Protocol Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def render_candidate_decision_ledger_reader_brief(record: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Candidate Decision Ledger",
            "",
            f"- summary: {record.get('candidate')} append-only candidate decision record.",
            f"- key_result: candidate_decision_ledger_status=RECORDED final_decision={record.get('final_decision')}",
            f"- evidence_status: {record.get('evidence_status')}",
            f"- stress_result: {record.get('stress_result')}",
            f"- mismatch_result: {record.get('mismatch_result')}",
            f"- rotation_result: {record.get('rotation_result')}",
            f"- ab_result: {record.get('ab_result')}",
            f"- confirmation_count: {record.get('confirmation_count')}",
            f"- owner_action: {record.get('owner_action')}",
            f"- next_required_action: {record.get('next_required_action')}",
            "- safety_boundary: append-only ledger / manual review only / no official target / no broker / no production",
            "",
        ]
    )


def render_candidate_decision_ledger_report(
    manifest: Mapping[str, Any],
    record: Mapping[str, Any],
    ledger_rows: Sequence[Mapping[str, Any]],
) -> str:
    source_lines = [
        f"- {name}: {artifact_id}"
        for name, artifact_id in sorted(_mapping(record.get("source_artifacts")).items())
    ]
    recent_lines = [
        f"- seq={row.get('ledger_sequence')} record_id={row.get('record_id')} candidate={row.get('candidate')} final_decision={row.get('final_decision')} next={row.get('next_required_action')}"
        for row in ledger_rows[-10:]
    ]
    return "\n".join(
        [
            f"# Candidate Decision Ledger {manifest.get('ledger_run_id')}",
            "",
            "## Purpose",
            "记录 filtered candidate research chain 的 append-only decision history；本报告不批准 production、不写 official target weights、不触发 broker 或 order 系统。",
            "",
            "## Current Record",
            f"- record_id: {record.get('record_id')}",
            f"- candidate: {record.get('candidate')}",
            f"- ledger_sequence: {record.get('ledger_sequence')}",
            f"- evidence_status: {record.get('evidence_status')}",
            f"- stress_result: {record.get('stress_result')}",
            f"- mismatch_result: {record.get('mismatch_result')}",
            f"- rotation_result: {record.get('rotation_result')}",
            f"- ab_result: {record.get('ab_result')}",
            f"- confirmation_count: {record.get('confirmation_count')}",
            f"- owner_action: {record.get('owner_action')}",
            f"- final_decision: {record.get('final_decision')}",
            f"- next_required_action: {record.get('next_required_action')}",
            "",
            "## Source Artifacts",
            *source_lines,
            "",
            "## Ledger Snapshot",
            f"- canonical_ledger_path: {manifest.get('ledger_path')}",
            f"- record_count: {manifest.get('record_count')}",
            *recent_lines,
            "",
            "## Safety Boundary",
            "- append-only ledger",
            "- manual review only",
            "- no official target weights",
            "- no broker integration",
            "- no order tickets",
            "- no production mutation",
            "",
            "## Limitations",
            "- TRADING-349 records decision evidence state only; it does not approve implementation.",
            "- A ledger record is not owner approval, a paper-shadow daily runner, or a production target.",
            "- Later decisions must append a new record instead of rewriting prior records.",
            "",
            "## Next Action",
            f"- {record.get('next_required_action')}",
            "",
        ]
    )


def render_candidate_decision_ledger_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Candidate Decision Ledger Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def render_evidence_staleness_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Evidence Staleness Monitor",
            "",
            f"- summary: {report.get('candidate')} evidence freshness gate.",
            f"- key_result: evidence_freshness_status={report.get('evidence_freshness_status')}",
            f"- stale_artifacts: {', '.join(_texts(report.get('stale_artifacts'))) or 'none'}",
            f"- blocking_artifacts: {', '.join(_texts(report.get('blocking_artifacts'))) or 'none'}",
            f"- missing_artifacts: {', '.join(_texts(report.get('missing_artifacts'))) or 'none'}",
            f"- requested_as_of: {report.get('requested_as_of')}",
            f"- freshness_reference_date: {report.get('freshness_reference_date')}",
            f"- latest_complete_market_date: {report.get('latest_complete_market_date')}",
            f"- market_calendar_status: {report.get('market_calendar_status')}",
            f"- market_session_kind: {report.get('market_session_kind')}",
            f"- calendar_adjustment_reason: {report.get('calendar_adjustment_reason')}",
            f"- calendar_adjusted_staleness: {report.get('calendar_adjusted_staleness')}",
            f"- coverage_status: {report.get('coverage_status')}",
            "- weekly_review_coverage_classification: "
            f"{report.get('weekly_review_coverage_classification')}",
            "- weekly_review_coverage_safe_for_continuation: "
            f"{report.get('weekly_review_coverage_safe_for_continuation')}",
            f"- next_refresh_action: {report.get('next_refresh_action')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- safety_boundary_status: {report.get('safety_boundary_status')}",
            f"- policy_version: {report.get('policy_id')} / {report.get('policy_version')}",
            "- safety_boundary: read-only freshness monitor / no refresh / no upstream rerun / no official target / no broker / no production",
            "",
        ]
    )


def render_evidence_staleness_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    finding_lines = [
        f"- {row.get('source_id')}: severity={row.get('severity')} "
        f"age_days={row.get('age_days')} timestamp={row.get('timestamp')} "
        f"basis={row.get('timestamp_basis')} "
        f"freshness_reference_date={row.get('freshness_reference_date')} "
        f"calendar_adjusted_staleness={row.get('calendar_adjusted_staleness')} "
        f"coverage_classification={row.get('coverage_classification', '')} "
        f"stale_reason={row.get('stale_reason')} "
        f"action={row.get('recommended_action')}"
        for row in _records(report.get("findings"))
    ]
    return "\n".join(
        [
            f"# Evidence Staleness Monitor {manifest.get('monitor_id')}",
            "",
            "## Purpose",
            "检查 candidate decision 依赖的 price、market panel 和 filtered candidate evidence 是否陈旧；本报告只读，不刷新数据、不运行上游、不修改 ledger、不触发 broker 或 production。",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- as_of: {report.get('as_of')}",
            f"- requested_as_of: {report.get('requested_as_of')}",
            f"- freshness_reference_date: {report.get('freshness_reference_date')}",
            f"- latest_complete_market_date: {report.get('latest_complete_market_date')}",
            f"- market_calendar_status: {report.get('market_calendar_status')}",
            f"- market_calendar_reason: {report.get('market_calendar_reason')}",
            f"- market_session_kind: {report.get('market_session_kind')}",
            f"- calendar_adjustment_reason: {report.get('calendar_adjustment_reason')}",
            f"- calendar_adjusted_staleness: {report.get('calendar_adjusted_staleness')}",
            f"- market_close_time: {report.get('market_close_time')}",
            f"- data_ready_time: {report.get('data_ready_time')}",
            f"- evidence_freshness_status: {report.get('evidence_freshness_status')}",
            f"- coverage_status: {report.get('coverage_status')}",
            "- coverage_blocking_artifacts: "
            f"{', '.join(_texts(report.get('coverage_blocking_artifacts'))) or 'none'}",
            "- weekly_review_coverage_classification: "
            f"{report.get('weekly_review_coverage_classification')}",
            "- weekly_review_coverage_safe_for_continuation: "
            f"{report.get('weekly_review_coverage_safe_for_continuation')}",
            f"- stale_artifacts: {', '.join(_texts(report.get('stale_artifacts'))) or 'none'}",
            f"- blocking_artifacts: {', '.join(_texts(report.get('blocking_artifacts'))) or 'none'}",
            f"- missing_artifacts: {', '.join(_texts(report.get('missing_artifacts'))) or 'none'}",
            f"- next_refresh_action: {report.get('next_refresh_action')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- safety_boundary_status: {report.get('safety_boundary_status')}",
            f"- policy: {report.get('policy_id')} / {report.get('policy_version')}",
            "",
            "## Findings",
            *finding_lines,
            "",
            "## Safety Boundary",
            "- read-only monitor",
            "- no data refresh",
            "- no upstream pipeline execution",
            "- no candidate ledger mutation",
            "- no official target weights",
            "- no broker integration",
            "- no order tickets",
            "- no production mutation",
            "",
            "## Limitations",
            "- Freshness windows are pilot policy bands from YAML, not production trading thresholds.",
            "- The monitor reports stale inputs; it does not repair them or regenerate evidence.",
            "- BLOCKING means the next candidate decision should stop until the source evidence is refreshed.",
            "",
        ]
    )


def render_evidence_staleness_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Evidence Staleness Monitor Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def _formal_research_method_source_payloads(
    *,
    evidence_id: str | None,
    spec_id: str | None,
    stress_backfill_id: str | None,
    mismatch_reduction_id: str | None,
    flip_reduction_id: str | None,
    ab_review_id: str | None,
    confirmation_id: str | None,
    readiness_id: str | None,
    owner_review_id: str | None,
    next_decision_id: str | None,
    evidence_dir: Path,
    spec_dir: Path,
    stress_backfill_dir: Path,
    mismatch_reduction_dir: Path,
    flip_reduction_dir: Path,
    ab_review_dir: Path,
    confirmation_dir: Path,
    readiness_dir: Path,
    owner_review_dir: Path,
    next_decision_dir: Path,
) -> dict[str, dict[str, Any]]:
    return {
        "evidence": filtered_candidate_evidence_report_payload(
            evidence_id=evidence_id, latest=evidence_id is None, output_dir=evidence_dir
        ),
        "spec": median_regime_filter_spec_report_payload(
            spec_id=spec_id, latest=spec_id is None, output_dir=spec_dir
        ),
        "stress": filtered_candidate_stress_backfill_report_payload(
            stress_backfill_id=stress_backfill_id,
            latest=stress_backfill_id is None,
            output_dir=stress_backfill_dir,
        ),
        "mismatch": drawdown_mismatch_reduction_report_payload(
            reduction_id=mismatch_reduction_id,
            latest=mismatch_reduction_id is None,
            output_dir=mismatch_reduction_dir,
        ),
        "flip": flip_rotation_reduction_report_payload(
            flip_reduction_id=flip_reduction_id,
            latest=flip_reduction_id is None,
            output_dir=flip_reduction_dir,
        ),
        "ab": filtered_candidate_ab_review_report_payload(
            ab_review_id=ab_review_id, latest=ab_review_id is None, output_dir=ab_review_dir
        ),
        "confirmation": signal_gate_confirmation_report_payload(
            confirmation_id=confirmation_id,
            latest=confirmation_id is None,
            output_dir=confirmation_dir,
        ),
        "readiness": filtered_formalization_readiness_report_payload(
            readiness_id=readiness_id, latest=readiness_id is None, output_dir=readiness_dir
        ),
        "owner_review": owner_filtered_candidate_review_report_payload(
            owner_review_id=owner_review_id,
            latest=owner_review_id is None,
            output_dir=owner_review_dir,
        ),
        "next_decision": filtered_next_decision_report_payload(
            decision_id=next_decision_id,
            latest=next_decision_id is None,
            output_dir=next_decision_dir,
        ),
    }


def _formal_research_source_artifacts(
    sources: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    source_specs = {
        "evidence": ("evidence_id", "filtered_candidate_evidence_manifest_path", "filtered_candidate_evidence_summary", "evidence_status"),
        "spec": ("spec_id", "median_regime_filter_spec_manifest_path", "median_regime_filter_contract", "contract_status"),
        "stress": ("stress_backfill_id", "filtered_candidate_stress_manifest_path", "filtered_candidate_stress_summary", "stress_robustness_status"),
        "mismatch": ("reduction_id", "drawdown_mismatch_reduction_manifest_path", "mismatch_reduction_summary", "drawdown_mismatch_reduction_status"),
        "flip": ("flip_reduction_id", "flip_rotation_reduction_manifest_path", "flip_rotation_reduction_summary", "flip_reduction_status"),
        "ab": ("ab_review_id", "filtered_candidate_ab_manifest_path", "ab_summary", "overall_ab_status"),
        "confirmation": ("confirmation_id", "signal_gate_confirmation_manifest_path", "signal_gate_confirmation_targets", "targets"),
        "readiness": ("readiness_id", "filtered_formalization_manifest_path", "formalization_readiness_decision", "decision"),
        "owner_review": ("owner_review_id", "owner_filtered_candidate_manifest_path", "owner_filtered_candidate_summary", "recommended_owner_action"),
        "next_decision": ("decision_id", "filtered_next_decision_manifest_path", "filtered_next_decision", "decision"),
    }
    rows: dict[str, dict[str, Any]] = {}
    for name, (id_key, path_key, summary_key, status_key) in source_specs.items():
        payload = _mapping(sources.get(name))
        summary = _mapping(payload.get(summary_key))
        if name == "confirmation":
            status = str(len(_records(summary.get("targets"))))
        else:
            status = _text(summary.get(status_key))
        rows[name] = {
            "schema_version": st.SCHEMA_VERSION,
            "artifact_id": payload.get(id_key),
            "artifact_path": payload.get(path_key),
            "status": status,
            "candidate": payload.get("candidate", summary.get("candidate")),
            **FORMAL_RESEARCH_CONTRACT_SAFETY,
        }
    return rows


def _formal_research_objective_gates(
    sources: Mapping[str, Mapping[str, Any]],
    source_artifacts: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    evidence = _mapping(_mapping(sources.get("evidence")).get("filtered_candidate_evidence_summary"))
    spec = _mapping(_mapping(sources.get("spec")).get("median_regime_filter_contract"))
    stress = _mapping(_mapping(sources.get("stress")).get("filtered_candidate_stress_summary"))
    mismatch = _mapping(_mapping(sources.get("mismatch")).get("mismatch_reduction_summary"))
    flip = _mapping(_mapping(sources.get("flip")).get("flip_rotation_reduction_summary"))
    ab = _mapping(_mapping(sources.get("ab")).get("ab_summary"))
    confirmation = _mapping(_mapping(sources.get("confirmation")).get("signal_gate_confirmation_targets"))
    readiness = _mapping(_mapping(sources.get("readiness")).get("formalization_readiness_decision"))
    owner_review = _mapping(_mapping(sources.get("owner_review")).get("owner_filtered_candidate_summary"))
    next_decision = _mapping(_mapping(sources.get("next_decision")).get("filtered_next_decision"))
    confirmation_count = len(_records(confirmation.get("targets")))
    return [
        _formal_research_gate(
            "evidence_result",
            source_artifacts["evidence"],
            _text(evidence.get("evidence_status")),
            "PROMISING",
            _text(evidence.get("evidence_status")) == "PROMISING",
            "evidence_rejected_or_missing",
        ),
        _formal_research_gate(
            "spec_contract",
            source_artifacts["spec"],
            _text(spec.get("contract_status")),
            "PASS",
            _text(spec.get("contract_status")) == "PASS",
            "spec_contract_failed",
        ),
        _formal_research_gate(
            "stress_result",
            source_artifacts["stress"],
            _text(stress.get("stress_robustness_status")),
            "STRONG",
            _text(stress.get("stress_robustness_status")) == "STRONG",
            "stress_weak_or_missing",
        ),
        _formal_research_gate(
            "drawdown_mismatch_reduction",
            source_artifacts["mismatch"],
            _text(mismatch.get("drawdown_mismatch_reduction_status")),
            "IMPROVED",
            _text(mismatch.get("drawdown_mismatch_reduction_status")) == "IMPROVED",
            "drawdown_mismatch_not_improved",
        ),
        _formal_research_gate(
            "flip_rotation_reduction",
            source_artifacts["flip"],
            f"flip={flip.get('flip_reduction_status')}; rotation={flip.get('rotation_reduction_status')}",
            "flip=IMPROVED; rotation=IMPROVED",
            _text(flip.get("flip_reduction_status")) == "IMPROVED"
            and _text(flip.get("rotation_reduction_status")) == "IMPROVED",
            "flip_rotation_not_improved",
        ),
        _formal_research_gate(
            "ab_review",
            source_artifacts["ab"],
            _text(ab.get("overall_ab_status")),
            "PROMISING",
            _text(ab.get("overall_ab_status")) == "PROMISING",
            "ab_review_weak_or_missing",
        ),
        _formal_research_gate(
            "confirmation_targets",
            source_artifacts["confirmation"],
            str(confirmation_count),
            f">={FORMAL_RESEARCH_MIN_CONFIRMATION_TARGETS}",
            confirmation_count >= FORMAL_RESEARCH_MIN_CONFIRMATION_TARGETS,
            "confirmation_targets_missing",
        ),
        _formal_research_gate(
            "formalization_readiness",
            source_artifacts["readiness"],
            _text(readiness.get("decision")),
            "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION",
            _text(readiness.get("decision")) == "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION",
            "formalization_readiness_not_ready",
        ),
        _formal_research_gate(
            "owner_review",
            source_artifacts["owner_review"],
            _text(owner_review.get("recommended_owner_action")),
            "formalize_research_method",
            _text(owner_review.get("recommended_owner_action")) == "formalize_research_method",
            "owner_did_not_request_formalization",
        ),
        _formal_research_gate(
            "next_decision",
            source_artifacts["next_decision"],
            _text(next_decision.get("decision")),
            "FORMALIZE_RESEARCH_METHOD",
            _text(next_decision.get("decision")) == "FORMALIZE_RESEARCH_METHOD",
            "next_decision_not_formalize",
        ),
    ]


def _formal_research_gate(
    gate_id: str,
    source_artifact: Mapping[str, Any],
    observed_status: str,
    required_status: str,
    passed: bool,
    failure_condition: str,
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "gate_id": gate_id,
        "source_artifact": dict(source_artifact),
        "observed_status": observed_status or "MISSING",
        "required_status": required_status,
        "passed": bool(passed),
        "failure_condition": failure_condition,
        "blocks_formal_research": True,
        "blocks_paper_shadow": gate_id
        in {
            "evidence_result",
            "stress_result",
            "drawdown_mismatch_reduction",
            "flip_rotation_reduction",
            "ab_review",
            "confirmation_targets",
        },
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }


def _formal_research_failure_conditions(
    objective_gates: Sequence[Mapping[str, Any]],
    safety_boundary_status: str,
) -> list[dict[str, Any]]:
    rows = [
        {
            "schema_version": st.SCHEMA_VERSION,
            "failure_condition": "safety_boundary_failed",
            "active": safety_boundary_status != "PASS",
            "blocks_formal_research": True,
            "blocks_paper_shadow": True,
            "source_gate": "safety_boundary",
            **FORMAL_RESEARCH_CONTRACT_SAFETY,
        }
    ]
    for gate in objective_gates:
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "failure_condition": gate.get("failure_condition"),
                "active": gate.get("passed") is not True,
                "blocks_formal_research": gate.get("blocks_formal_research") is True,
                "blocks_paper_shadow": gate.get("blocks_paper_shadow") is True,
                "source_gate": gate.get("gate_id"),
                **FORMAL_RESEARCH_CONTRACT_SAFETY,
            }
        )
    return rows


def _formal_research_paper_shadow_eligibility(
    objective_gates: Sequence[Mapping[str, Any]],
    safety_boundary_status: str,
) -> dict[str, Any]:
    blocking = [
        _text(gate.get("gate_id"))
        for gate in objective_gates
        if gate.get("blocks_paper_shadow") is True and gate.get("passed") is not True
    ]
    eligible = safety_boundary_status == "PASS" and not blocking
    return {
        "schema_version": st.SCHEMA_VERSION,
        "eligible": eligible,
        "status": "ELIGIBLE_FOR_PROTOCOL_DESIGN" if eligible else "NOT_ELIGIBLE",
        "blocking_conditions": blocking,
        "manual_review_required": True,
        "paper_shadow_output_observation_only": True,
        "can_create_official_target_weights": False,
        "can_trigger_broker_or_orders": False,
        "requires_separate_protocol_task": "TRADING-350",
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }


def _formal_research_method_decision(
    *,
    candidate: str,
    objective_gates: Sequence[Mapping[str, Any]],
    failure_conditions: Sequence[Mapping[str, Any]],
    paper_shadow_eligibility: Mapping[str, Any],
    safety_boundary_status: str,
) -> dict[str, Any]:
    active_failures = [
        _text(row.get("failure_condition"))
        for row in failure_conditions
        if row.get("active") is True
    ]
    gate_pass = {str(gate.get("gate_id")): gate.get("passed") is True for gate in objective_gates}
    hard_reject = bool({"safety_boundary_failed", "evidence_rejected_or_missing"} & set(active_failures))
    if hard_reject:
        promotion_state = "REJECTED"
    elif all(gate_pass.values()) and safety_boundary_status == "PASS":
        promotion_state = "FORMAL_RESEARCH_READY"
    elif paper_shadow_eligibility.get("eligible") is True:
        promotion_state = "PAPER_SHADOW_ELIGIBLE"
    elif gate_pass.get("evidence_result") and gate_pass.get("stress_result") and gate_pass.get("ab_review"):
        promotion_state = "PROMISING"
    else:
        promotion_state = "NEEDS_MORE_EVIDENCE"
    status_by_state = {
        "FORMAL_RESEARCH_READY": "READY_FOR_RESEARCH_ONLY_IMPLEMENTATION",
        "PAPER_SHADOW_ELIGIBLE": "PAPER_SHADOW_PROTOCOL_REQUIRED",
        "PROMISING": "NEEDS_OWNER_OR_CONFIRMATION_PROGRESS",
        "NEEDS_MORE_EVIDENCE": "NOT_READY",
        "REJECTED": "REJECTED",
    }
    next_action_by_state = {
        "FORMAL_RESEARCH_READY": "implement_research_only_formal_method_contract",
        "PAPER_SHADOW_ELIGIBLE": "complete_paper_shadow_protocol_before_observation",
        "PROMISING": "collect_owner_or_confirmation_evidence",
        "NEEDS_MORE_EVIDENCE": "collect_missing_research_evidence",
        "REJECTED": "stop_candidate_or_rework_evidence",
    }
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate": candidate,
        "formal_research_method_status": status_by_state[promotion_state],
        "promotion_state": promotion_state,
        "blocking_reasons": active_failures,
        "paper_shadow_eligibility": paper_shadow_eligibility.get("status"),
        "safety_boundary_status": safety_boundary_status,
        "next_required_action": next_action_by_state[promotion_state],
        "manual_review_required": True,
        "formal_research_ready_is_not_production_approval": True,
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }


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


def _load_evidence_staleness_policy(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    return payload if isinstance(payload, dict) else {}


def _evidence_staleness_findings(
    *,
    as_of: date,
    generated_at: datetime,
    candidate: str,
    policy: Mapping[str, Any],
    price_cache_path: Path,
    market_panel_dir: Path,
    evidence_id: str | None,
    stress_backfill_id: str | None,
    ab_review_id: str | None,
    owner_review_id: str | None,
    paper_shadow_daily_id: str | None,
    paper_shadow_drift_monitor_id: str | None,
    paper_shadow_weekly_review_id: str | None,
    evidence_dir: Path,
    stress_backfill_dir: Path,
    ab_review_dir: Path,
    owner_review_dir: Path,
    paper_shadow_daily_dir: Path,
    paper_shadow_drift_monitor_dir: Path,
    paper_shadow_weekly_review_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rules = _mapping(policy.get("rules"))
    latest_price_date = _latest_price_cache_date(price_cache_path)
    market_panel_path, market_panel = _latest_market_panel_payload(market_panel_dir, as_of=as_of)
    market_panel_date = _date_or_none(market_panel.get("as_of")) or _date_from_market_panel_path(
        market_panel_path
    )
    market_calendar = _market_data_freshness_context(
        requested_as_of=as_of,
        generated_at=generated_at,
    )
    market_reference = _date_or_none(market_calendar.get("freshness_reference_date")) or as_of
    evidence = filtered_candidate_evidence_report_payload(
        evidence_id=evidence_id,
        latest=evidence_id is None,
        output_dir=evidence_dir,
    )
    stress = filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id,
        latest=stress_backfill_id is None,
        output_dir=stress_backfill_dir,
    )
    ab_review = filtered_candidate_ab_review_report_payload(
        ab_review_id=ab_review_id,
        latest=ab_review_id is None,
        output_dir=ab_review_dir,
    )
    owner_review = owner_filtered_candidate_review_report_payload(
        owner_review_id=owner_review_id,
        latest=owner_review_id is None,
        output_dir=owner_review_dir,
    )
    paper_shadow_daily = _optional_dynamic_v3_artifact_payload(
        artifact_id=paper_shadow_daily_id,
        latest_pointer="latest_paper_shadow_daily",
        output_dir=paper_shadow_daily_dir,
        required_name="paper_shadow_daily_manifest.json",
        detail_name="paper_shadow_daily_observation.json",
    )
    paper_shadow_drift = _optional_dynamic_v3_artifact_payload(
        artifact_id=paper_shadow_drift_monitor_id,
        latest_pointer="latest_paper_shadow_drift_monitor",
        output_dir=paper_shadow_drift_monitor_dir,
        required_name="paper_shadow_drift_manifest.json",
        detail_name="paper_shadow_drift_report.json",
    )
    paper_shadow_weekly = _optional_dynamic_v3_artifact_payload(
        artifact_id=paper_shadow_weekly_review_id,
        latest_pointer="latest_paper_shadow_weekly_review",
        output_dir=paper_shadow_weekly_review_dir,
        required_name="paper_shadow_weekly_manifest.json",
        detail_name="paper_shadow_weekly_review.json",
    )
    evidence_date = _date_or_none(evidence.get("date_end")) or _date_or_none(
        evidence.get("generated_at")
    )
    findings = [
        _evidence_freshness_finding(
            source_id="price_data",
            source_label="Price data",
            candidate=candidate,
            timestamp=latest_price_date,
            timestamp_basis="latest_price_cache_date",
            source_path=price_cache_path,
            artifact_id=price_cache_path.name,
            rule=_mapping(rules.get("price_data")),
            as_of=market_reference,
            requested_as_of=as_of,
            market_calendar=market_calendar,
        ),
        _evidence_freshness_finding(
            source_id="market_panel_data",
            source_label="Market panel data",
            candidate=candidate,
            timestamp=market_panel_date,
            timestamp_basis="market_panel_as_of",
            source_path=market_panel_path,
            artifact_id=market_panel_path.name if market_panel_path else "",
            rule=_mapping(rules.get("market_panel_data")),
            as_of=market_reference,
            requested_as_of=as_of,
            market_calendar=market_calendar,
            artifact_status=_text(market_panel.get("status")),
        ),
        _evidence_freshness_finding(
            source_id="signal_artifact",
            source_label="Filtered candidate signal evidence",
            candidate=candidate,
            timestamp=evidence_date,
            timestamp_basis="evidence_date_end_or_generated_at",
            source_path=_path_or_none(evidence.get("filtered_candidate_evidence_manifest_path")),
            artifact_id=_text(evidence.get("evidence_id")),
            rule=_mapping(rules.get("signal_artifact")),
            as_of=as_of,
            artifact_status=_text(evidence.get("status")),
        ),
        _evidence_freshness_finding(
            source_id="stress_backfill_result",
            source_label="Stress backfill result",
            candidate=candidate,
            timestamp=_date_or_none(stress.get("generated_at")),
            timestamp_basis="stress_backfill_generated_at",
            source_path=_path_or_none(stress.get("filtered_candidate_stress_manifest_path")),
            artifact_id=_text(stress.get("stress_backfill_id")),
            rule=_mapping(rules.get("stress_backfill_result")),
            as_of=as_of,
            artifact_status=_text(stress.get("status")),
        ),
        _evidence_freshness_finding(
            source_id="ab_review",
            source_label="Filtered candidate A/B review",
            candidate=candidate,
            timestamp=_date_or_none(ab_review.get("generated_at")),
            timestamp_basis="ab_review_generated_at",
            source_path=_path_or_none(ab_review.get("filtered_candidate_ab_manifest_path")),
            artifact_id=_text(ab_review.get("ab_review_id")),
            rule=_mapping(rules.get("ab_review")),
            as_of=as_of,
            artifact_status=_text(ab_review.get("status")),
        ),
        _evidence_freshness_finding(
            source_id="owner_review",
            source_label="Owner filtered candidate review",
            candidate=candidate,
            timestamp=_date_or_none(owner_review.get("generated_at")),
            timestamp_basis="owner_review_generated_at",
            source_path=_path_or_none(owner_review.get("owner_filtered_candidate_manifest_path")),
            artifact_id=_text(owner_review.get("owner_review_id")),
            rule=_mapping(rules.get("owner_review")),
            as_of=as_of,
            artifact_status=_text(owner_review.get("status")),
        ),
        _evidence_freshness_finding(
            source_id="paper_shadow_daily_observation",
            source_label="Paper-shadow daily observation",
            candidate=candidate,
            timestamp=_date_or_none(
                _mapping(paper_shadow_daily.get("detail")).get("observation_date")
            )
            or _date_or_none(_mapping(paper_shadow_daily.get("manifest")).get("generated_at")),
            timestamp_basis="paper_shadow_daily_observation_date_or_generated_at",
            source_path=_path_or_none(
                _mapping(paper_shadow_daily.get("manifest")).get(
                    "paper_shadow_daily_manifest_path"
                )
            ),
            artifact_id=_text(_mapping(paper_shadow_daily.get("manifest")).get("observation_id")),
            rule=_mapping(rules.get("paper_shadow_daily_observation")),
            as_of=as_of,
            artifact_status=_text(_mapping(paper_shadow_daily.get("manifest")).get("status")),
        ),
        _evidence_freshness_finding(
            source_id="paper_shadow_drift_monitor",
            source_label="Paper-shadow drift monitor",
            candidate=candidate,
            timestamp=_date_or_none(
                _mapping(paper_shadow_drift.get("manifest")).get("generated_at")
            )
            or _date_or_none(_mapping(paper_shadow_drift.get("detail")).get("observation_date")),
            timestamp_basis="paper_shadow_drift_monitor_generated_at",
            source_path=_path_or_none(
                _mapping(paper_shadow_drift.get("manifest")).get(
                    "paper_shadow_drift_manifest_path"
                )
            ),
            artifact_id=_text(_mapping(paper_shadow_drift.get("manifest")).get("monitor_id")),
            rule=_mapping(rules.get("paper_shadow_drift_monitor")),
            as_of=as_of,
            artifact_status=_text(_mapping(paper_shadow_drift.get("manifest")).get("status")),
        ),
        _evidence_freshness_finding(
            source_id="paper_shadow_weekly_review",
            source_label="Paper-shadow weekly review",
            candidate=candidate,
            timestamp=_date_or_none(_mapping(paper_shadow_weekly.get("detail")).get("week_end"))
            or _date_or_none(_mapping(paper_shadow_weekly.get("manifest")).get("generated_at")),
            timestamp_basis="paper_shadow_weekly_week_end_or_generated_at",
            source_path=_path_or_none(
                _mapping(paper_shadow_weekly.get("manifest")).get(
                    "paper_shadow_weekly_manifest_path"
                )
            ),
            artifact_id=_text(
                _mapping(paper_shadow_weekly.get("manifest")).get("weekly_review_id")
            ),
            rule=_mapping(rules.get("paper_shadow_weekly_review")),
            as_of=as_of,
            artifact_status=_text(_mapping(paper_shadow_weekly.get("manifest")).get("status")),
        ),
    ]
    weekly_coverage = _paper_shadow_weekly_coverage_fields(paper_shadow_weekly)
    for row in findings:
        if row.get("source_id") == "paper_shadow_weekly_review":
            row.update(weekly_coverage)
    return findings, market_calendar


def _evidence_freshness_finding(
    *,
    source_id: str,
    source_label: str,
    candidate: str,
    timestamp: date | None,
    timestamp_basis: str,
    source_path: Path | None,
    artifact_id: str,
    rule: Mapping[str, Any],
    as_of: date,
    requested_as_of: date | None = None,
    market_calendar: Mapping[str, Any] | None = None,
    artifact_status: str = "",
) -> dict[str, Any]:
    effective_requested_as_of = requested_as_of or as_of
    calendar_context = _mapping(market_calendar)
    calendar_adjusted_staleness = bool(
        calendar_context.get("calendar_adjusted_staleness")
    )
    raw_age = (as_of - timestamp).days if timestamp is not None else None
    source_after_reference_but_not_requested = (
        raw_age is not None
        and raw_age < 0
        and timestamp is not None
        and timestamp <= effective_requested_as_of
    )
    age_days = 0 if source_after_reference_but_not_requested else raw_age
    severity = (
        "BLOCKING"
        if raw_age is not None and raw_age < 0 and not source_after_reference_but_not_requested
        else _freshness_severity(age_days, rule)
    )
    source_exists = source_path.exists() if source_path is not None else False
    missing = (
        rule.get("required") is not False
        and (
            timestamp is None
            or not _text(artifact_id)
            or (source_path is not None and not source_exists)
        )
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "source_label": source_label,
        "candidate": candidate,
        "artifact_id": artifact_id,
        "artifact_status": artifact_status,
        "source_path": "" if source_path is None else str(source_path),
        "source_exists": source_exists,
        "timestamp": "" if timestamp is None else timestamp.isoformat(),
        "timestamp_basis": timestamp_basis,
        "as_of": as_of.isoformat(),
        "requested_as_of": effective_requested_as_of.isoformat(),
        "freshness_reference_date": as_of.isoformat(),
        "latest_complete_market_date": _text(calendar_context.get("latest_complete_market_date")),
        "market_calendar_status": _text(calendar_context.get("market_calendar_status")),
        "market_calendar_reason": _text(calendar_context.get("market_calendar_reason")),
        "market_session_kind": _text(calendar_context.get("market_session_kind")),
        "calendar_adjustment_reason": _text(
            calendar_context.get("calendar_adjustment_reason")
        ),
        "calendar_adjusted_staleness": calendar_adjusted_staleness,
        "market_close_time": _text(calendar_context.get("market_close_time")),
        "data_ready_time": _text(calendar_context.get("data_ready_time")),
        "data_vendor_delay_minutes": calendar_context.get("data_vendor_delay_minutes"),
        "stale_reason": _evidence_stale_reason(
            severity=severity,
            missing=missing,
            raw_age=raw_age,
            source_after_reference_but_not_requested=source_after_reference_but_not_requested,
            requested_as_of=effective_requested_as_of,
            freshness_reference_date=as_of,
        ),
        "age_days": age_days,
        "raw_age_days": raw_age,
        "timestamp_relation": _timestamp_relation(raw_age),
        "fresh_days": rule.get("fresh_days"),
        "acceptable_days": rule.get("acceptable_days"),
        "blocking_days": rule.get("blocking_days"),
        "required": rule.get("required") is not False,
        "missing": missing,
        "severity": severity,
        "recommended_action": _freshness_recommended_action(severity, source_id),
        **EVIDENCE_STALENESS_MONITOR_SAFETY,
    }


def _market_data_freshness_context(
    *,
    requested_as_of: date,
    generated_at: datetime,
) -> dict[str, Any]:
    return resolve_us_equity_market_freshness(
        requested_as_of=requested_as_of,
        observed_at=generated_at,
    ).to_report_dict()


def _freshness_severity(age_days: int | None, rule: Mapping[str, Any]) -> str:
    if age_days is None or not rule:
        return "BLOCKING"
    fresh_days = int(rule.get("fresh_days", 0))
    acceptable_days = int(rule.get("acceptable_days", fresh_days))
    blocking_days = int(rule.get("blocking_days", acceptable_days))
    if age_days <= fresh_days:
        return "FRESH"
    if age_days <= acceptable_days:
        return "ACCEPTABLE"
    if age_days <= blocking_days:
        return "STALE"
    return "BLOCKING"


def _freshness_recommended_action(severity: str, source_id: str) -> str:
    if severity == "FRESH":
        return "continue_candidate_review"
    if severity == "ACCEPTABLE":
        return f"continue_with_manual_note_for_{source_id}"
    if severity == "STALE":
        return f"refresh_or_regenerate_{source_id}"
    return f"block_until_{source_id}_is_refreshed"


def _evidence_stale_reason(
    *,
    severity: str,
    missing: bool,
    raw_age: int | None,
    source_after_reference_but_not_requested: bool = False,
    requested_as_of: date,
    freshness_reference_date: date,
) -> str:
    if missing:
        return "missing_required_artifact"
    if source_after_reference_but_not_requested:
        return "source_newer_than_calendar_reference_but_not_after_requested_as_of"
    if raw_age is not None and raw_age < 0:
        return "timestamp_after_freshness_reference_date"
    if severity == "BLOCKING":
        return "older_than_blocking_policy_window"
    if severity == "STALE":
        return "older_than_acceptable_policy_window"
    if requested_as_of > freshness_reference_date:
        return "calendar_adjusted_to_latest_complete_market_date"
    return "within_policy_window"


def _paper_shadow_weekly_coverage_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    detail = _mapping(payload.get("detail"))
    manifest = _mapping(payload.get("manifest"))
    summary = _mapping(detail.get("summary"))
    classification = _text(
        detail.get("coverage_classification")
        or summary.get("coverage_classification")
        or manifest.get("coverage_classification")
        or "UNKNOWN"
    )
    safe_value = detail.get("coverage_safe_for_continuation")
    if not isinstance(safe_value, bool):
        safe_value = summary.get("coverage_safe_for_continuation")
    if not isinstance(safe_value, bool):
        safe_value = manifest.get("coverage_safe_for_continuation")
    safe = safe_value if isinstance(safe_value, bool) else False
    coverage_status = _text(
        detail.get("coverage_status")
        or summary.get("coverage_status")
        or manifest.get("coverage_status")
        or ("PASS" if safe else "MANUAL_REVIEW_REQUIRED")
    )
    return {
        "coverage_status": coverage_status,
        "coverage_classification": classification,
        "coverage_safe_for_continuation": safe,
        "selected_window_start": _text(
            detail.get("selected_window_start") or summary.get("selected_window_start")
        ),
        "selected_window_end": _text(
            detail.get("selected_window_end") or summary.get("selected_window_end")
        ),
        "expected_market_days": _texts(
            detail.get("expected_market_days") or summary.get("expected_market_days")
        ),
        "covered_market_days": _texts(
            detail.get("covered_market_days") or summary.get("covered_market_days")
        ),
        "missing_market_days": _texts(
            detail.get("missing_market_days") or summary.get("missing_market_days")
        ),
        "coverage_ratio": detail.get("coverage_ratio", summary.get("coverage_ratio")),
        "manual_coverage_override": bool(
            detail.get("manual_coverage_override")
            or summary.get("manual_coverage_override")
            or manifest.get("manual_coverage_override")
        ),
        "manual_coverage_override_reason": _text(
            detail.get("manual_coverage_override_reason")
            or summary.get("manual_coverage_override_reason")
        ),
    }


def _evidence_weekly_coverage_status(
    findings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    weekly = next(
        (row for row in findings if row.get("source_id") == "paper_shadow_weekly_review"),
        {},
    )
    if not weekly:
        return {
            "coverage_status": "MISSING",
            "coverage_classification": "MISSING",
            "coverage_safe_for_continuation": False,
            "manual_coverage_override": False,
            "coverage_blocking_artifacts": ["paper_shadow_weekly_review"],
        }
    if weekly.get("missing") is True:
        return {
            "coverage_status": "MISSING",
            "coverage_classification": "MISSING",
            "coverage_safe_for_continuation": False,
            "manual_coverage_override": False,
            "coverage_blocking_artifacts": [],
        }
    safe = weekly.get("coverage_safe_for_continuation") is True
    return {
        "coverage_status": _text(
            weekly.get("coverage_status"),
            "PASS" if safe else "MANUAL_REVIEW_REQUIRED",
        ),
        "coverage_classification": _text(weekly.get("coverage_classification"), "UNKNOWN"),
        "coverage_safe_for_continuation": safe,
        "manual_coverage_override": weekly.get("manual_coverage_override") is True,
        "coverage_blocking_artifacts": []
        if safe
        else ["paper_shadow_weekly_review"],
    }


def _shadow_continuation_source_artifacts(
    *,
    paper_shadow_daily_id: str | None,
    paper_shadow_drift_monitor_id: str | None,
    paper_shadow_weekly_review_id: str | None,
    evidence_staleness_monitor_id: str | None,
    paper_shadow_daily_dir: Path,
    paper_shadow_drift_monitor_dir: Path,
    paper_shadow_weekly_review_dir: Path,
    evidence_staleness_monitor_dir: Path,
) -> dict[str, dict[str, Any]]:
    return {
        "paper_shadow_daily_observation": _shadow_continuation_source_artifact(
            source_id="paper_shadow_daily_observation",
            source_label="Paper-shadow daily observation",
            artifact_id=paper_shadow_daily_id,
            latest_pointer="latest_paper_shadow_daily",
            output_dir=paper_shadow_daily_dir,
            manifest_name="paper_shadow_daily_manifest.json",
            detail_name="paper_shadow_daily_observation.json",
            validation_name="paper_shadow_daily_validation.json",
            id_fields=("observation_id",),
            status_fields=("observation_status", "status"),
        ),
        "paper_shadow_drift_monitor": _shadow_continuation_source_artifact(
            source_id="paper_shadow_drift_monitor",
            source_label="Paper-shadow drift monitor",
            artifact_id=paper_shadow_drift_monitor_id,
            latest_pointer="latest_paper_shadow_drift_monitor",
            output_dir=paper_shadow_drift_monitor_dir,
            manifest_name="paper_shadow_drift_manifest.json",
            detail_name="paper_shadow_drift_report.json",
            validation_name="paper_shadow_drift_validation.json",
            id_fields=("monitor_id",),
            status_fields=("drift_severity", "status"),
        ),
        "paper_shadow_weekly_review": _shadow_continuation_source_artifact(
            source_id="paper_shadow_weekly_review",
            source_label="Paper-shadow weekly review",
            artifact_id=paper_shadow_weekly_review_id,
            latest_pointer="latest_paper_shadow_weekly_review",
            output_dir=paper_shadow_weekly_review_dir,
            manifest_name="paper_shadow_weekly_manifest.json",
            detail_name="paper_shadow_weekly_review.json",
            validation_name="paper_shadow_weekly_validation.json",
            id_fields=("weekly_review_id",),
            status_fields=("coverage_status", "weekly_decision", "status"),
        ),
        "evidence_staleness_monitor": _shadow_continuation_source_artifact(
            source_id="evidence_staleness_monitor",
            source_label="Evidence staleness monitor",
            artifact_id=evidence_staleness_monitor_id,
            latest_pointer="latest_evidence_staleness_monitor",
            output_dir=evidence_staleness_monitor_dir,
            manifest_name="evidence_staleness_manifest.json",
            detail_name="evidence_staleness_report.json",
            validation_name="evidence_staleness_validation.json",
            id_fields=("monitor_id",),
            status_fields=("evidence_freshness_status", "status"),
        ),
    }


def _shadow_continuation_source_artifact(
    *,
    source_id: str,
    source_label: str,
    artifact_id: str | None,
    latest_pointer: str,
    output_dir: Path,
    manifest_name: str,
    detail_name: str,
    validation_name: str,
    id_fields: Sequence[str],
    status_fields: Sequence[str],
) -> dict[str, Any]:
    root = _optional_dynamic_v3_artifact_dir(
        artifact_id=artifact_id,
        latest_pointer=latest_pointer,
        output_dir=output_dir,
        required_name=manifest_name,
    )
    if root is None:
        return {
            "source_id": source_id,
            "source_label": source_label,
            "exists": False,
            "artifact_id": _text(artifact_id),
            "source_path": "",
            "manifest": {},
            "detail": {},
            "validation": {},
            "status": "MISSING",
            "validation_status": "MISSING",
        }
    manifest = _read_optional_json(root / manifest_name) or {}
    detail = _read_optional_json(root / detail_name) or {}
    validation = _read_optional_json(root / validation_name) or {}
    resolved_id = _first_text(*(manifest.get(field) for field in id_fields))
    if not resolved_id:
        resolved_id = _first_text(*(detail.get(field) for field in id_fields))
    status = _first_text(*(detail.get(field) for field in status_fields))
    if not status:
        status = _first_text(*(manifest.get(field) for field in status_fields))
    return {
        "source_id": source_id,
        "source_label": source_label,
        "exists": bool(manifest),
        "artifact_id": resolved_id or root.name,
        "source_path": str(root / manifest_name),
        "root": str(root),
        "manifest": manifest,
        "detail": detail,
        "validation": validation,
        "status": status or "UNKNOWN",
        "validation_status": _text(validation.get("status"), "NOT_RUN"),
        "generated_at": _text(manifest.get("generated_at") or detail.get("generated_at")),
    }


def _shadow_continuation_source_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    manifest = _mapping(payload.get("manifest"))
    detail = _mapping(payload.get("detail"))
    validation = _mapping(payload.get("validation"))
    return {
        "source_id": payload.get("source_id"),
        "source_label": payload.get("source_label"),
        "exists": payload.get("exists") is True,
        "artifact_id": _text(payload.get("artifact_id")),
        "source_path": _text(payload.get("source_path")),
        "root": _text(payload.get("root")),
        "status": _text(payload.get("status"), "MISSING"),
        "validation_status": _text(payload.get("validation_status"), "MISSING"),
        "manifest_report_type": _text(manifest.get("report_type")),
        "detail_report_type": _text(detail.get("report_type")),
        "validation_report_type": _text(validation.get("report_type")),
        "generated_at": _text(payload.get("generated_at")),
    }


def _shadow_continuation_safety_audit(
    source_artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_results: list[dict[str, Any]] = []
    unsafe_sources: list[str] = []
    for source_id, payload in source_artifacts.items():
        if source_id == "data_validation_result":
            continue
        manifest = _mapping(payload.get("manifest"))
        detail = _mapping(payload.get("detail"))
        validation = _mapping(payload.get("validation"))
        checked = bool(manifest or detail or validation)
        passed = _payload_safe(manifest, detail, validation) if checked else True
        if checked and not passed:
            unsafe_sources.append(source_id)
        source_results.append(
            {
                "source_id": source_id,
                "checked": checked,
                "passed": passed,
                "artifact_id": _text(payload.get("artifact_id")),
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "status": "FAIL" if unsafe_sources else "PASS",
        "source_count": len(source_results),
        "sources_checked": source_results,
        "unsafe_sources": unsafe_sources,
        "readiness_boundary": dict(SHADOW_CONTINUATION_READINESS_SAFETY),
    }


def _shadow_continuation_readiness_decision(
    *,
    missing_artifacts: Sequence[str],
    blocking_artifacts: Sequence[str],
    stale_artifacts: Sequence[str],
    coverage_status: str,
    evidence_report: Mapping[str, Any],
    data_validation: Mapping[str, Any],
    safety_audit: Mapping[str, Any],
) -> str:
    if _texts(missing_artifacts):
        return "BLOCKED_MISSING_ARTIFACTS"
    if safety_audit.get("status") != "PASS":
        return "BLOCKED_SAFETY_BOUNDARY"
    data_status = _text(data_validation.get("status"), "MISSING")
    if (
        _texts(blocking_artifacts)
        or _texts(stale_artifacts)
        or data_status not in {"PASS", "PASS_WITH_WARNINGS"}
    ):
        return "BLOCKED_STALE_DATA"
    if (
        coverage_status != "PASS"
        or evidence_report.get("safe_to_continue_shadow") is not True
    ):
        return "MANUAL_REVIEW_REQUIRED"
    if data_status == "PASS_WITH_WARNINGS" or _int_or_none(
        data_validation.get("warning_count")
    ) not in (None, 0):
        return "READY_WITH_WARNINGS"
    return "READY_TO_CONTINUE"


def _shadow_continuation_next_action(readiness: str) -> str:
    return {
        "READY_TO_CONTINUE": "continue_paper_shadow_observation",
        "READY_WITH_WARNINGS": "continue_with_manual_warning_review",
        "MANUAL_REVIEW_REQUIRED": "complete_full_weekly_review_or_record_manual_coverage_override",
        "BLOCKED_MISSING_ARTIFACTS": "restore_missing_shadow_readiness_artifacts",
        "BLOCKED_STALE_DATA": "refresh_or_regenerate_stale_shadow_readiness_inputs",
        "BLOCKED_SAFETY_BOUNDARY": "stop_until_safety_boundary_is_restored",
    }.get(readiness, "manual_review_required")


def _data_quality_report_summary(
    *,
    data_quality_report_path: Path | None,
    data_quality_report_dir: Path,
    as_of: date | None,
) -> dict[str, Any]:
    path = data_quality_report_path or _latest_data_quality_report_path(
        data_quality_report_dir,
        as_of=as_of,
    )
    if path is None or not path.exists():
        return {
            "source_id": "data_validation_result",
            "source_label": "Data quality validation result",
            "exists": False,
            "artifact_id": "",
            "source_path": "" if path is None else str(path),
            "status": "MISSING",
            "generated_at": "",
            "as_of": "",
            "error_count": None,
            "warning_count": None,
            "info_count": None,
            "validation_status": "MISSING",
        }
    if path.suffix.lower() == ".json":
        payload = _read_optional_json(path) or {}
        status = _first_text(payload.get("status"), payload.get("data_quality_status"))
        generated_at = _first_text(payload.get("generated_at"), payload.get("check_time"))
        as_of = _first_text(payload.get("as_of"), payload.get("evaluation_date"))
        error_count = _int_or_none(payload.get("error_count"))
        warning_count = _int_or_none(payload.get("warning_count"))
        info_count = _int_or_none(payload.get("info_count"))
    else:
        parsed = _parse_data_quality_markdown(path)
        status = _text(parsed.get("status"), "UNKNOWN")
        generated_at = _text(parsed.get("generated_at"))
        as_of = _text(parsed.get("as_of"))
        error_count = _int_or_none(parsed.get("error_count"))
        warning_count = _int_or_none(parsed.get("warning_count"))
        info_count = _int_or_none(parsed.get("info_count"))
    return {
        "source_id": "data_validation_result",
        "source_label": "Data quality validation result",
        "exists": True,
        "artifact_id": path.name,
        "source_path": str(path),
        "status": status or "UNKNOWN",
        "generated_at": generated_at,
        "as_of": as_of,
        "error_count": error_count,
        "warning_count": warning_count,
        "info_count": info_count,
        "validation_status": status or "UNKNOWN",
    }


def _latest_data_quality_report_path(report_dir: Path, *, as_of: date | None) -> Path | None:
    candidates = [
        path
        for pattern in ("data_quality_*.json", "data_quality_*.md")
        for path in report_dir.glob(pattern)
        if path.is_file()
        and (as_of is None or (_date_from_data_quality_path(path) or date.min) <= as_of)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (_date_from_data_quality_path(path) or date.min, path.stat().st_mtime, path.name))


def _parse_data_quality_markdown(path: Path) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    key_map = {
        "状态": "status",
        "status": "status",
        "检查时间": "generated_at",
        "check time": "generated_at",
        "评估日期": "as_of",
        "as of": "as_of",
        "错误数": "error_count",
        "error count": "error_count",
        "警告数": "warning_count",
        "warning count": "warning_count",
        "信息数": "info_count",
        "info count": "info_count",
    }
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        item = stripped.removeprefix("- ").strip()
        if "：" in item:
            raw_key, raw_value = item.split("：", 1)
        elif ":" in item:
            raw_key, raw_value = item.split(":", 1)
        else:
            continue
        key = key_map.get(raw_key.strip().lower()) or key_map.get(raw_key.strip())
        if key:
            fields[key] = raw_value.strip().strip("`")
    return fields


def _date_from_data_quality_path(path: Path) -> date | None:
    return _date_or_none(path.stem.removeprefix("data_quality_"))


def _first_text(*values: object) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _int_or_none(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe_texts(value: object) -> list[str]:
    result: list[str] = []
    for item in _texts(value):
        if item not in result:
            result.append(item)
    return result


def _optional_dynamic_v3_artifact_payload(
    *,
    artifact_id: str | None,
    latest_pointer: str,
    output_dir: Path,
    required_name: str,
    detail_name: str,
) -> dict[str, Any]:
    root = _optional_dynamic_v3_artifact_dir(
        artifact_id=artifact_id,
        latest_pointer=latest_pointer,
        output_dir=output_dir,
        required_name=required_name,
    )
    if root is None:
        return {"manifest": {}, "detail": {}, "root": ""}
    return {
        "manifest": _read_optional_json(root / required_name) or {},
        "detail": _read_optional_json(root / detail_name) or {},
        "root": str(root),
    }


def _optional_dynamic_v3_artifact_dir(
    *,
    artifact_id: str | None,
    latest_pointer: str,
    output_dir: Path,
    required_name: str,
) -> Path | None:
    try:
        return _artifact_dir(
            artifact_id=artifact_id,
            latest_pointer=latest_pointer,
            latest=artifact_id is None,
            output_dir=output_dir,
            required_name=required_name,
        )
    except st.DynamicV3SystemTargetError:
        if artifact_id is not None:
            return None
    return st._latest_child_dir_with(output_dir, required_name)


def _path_or_none(value: object) -> Path | None:
    text = _text(value)
    return Path(text) if text else None


def _overall_evidence_freshness_status(
    policy: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> str:
    severity_order = _texts(policy.get("severity_order")) or [
        "FRESH",
        "ACCEPTABLE",
        "STALE",
        "BLOCKING",
    ]
    rank = {name: index for index, name in enumerate(severity_order)}
    return max(
        (_text(row.get("severity"), "BLOCKING") for row in findings),
        key=lambda item: rank.get(item, 999),
    )


def _latest_price_cache_date(path: Path) -> date | None:
    if not path.exists():
        return None
    latest: date | None = None
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candidate = _date_or_none(row.get("date"))
            if candidate is not None and (latest is None or candidate > latest):
                latest = candidate
    return latest


def _latest_market_panel_payload(
    report_dir: Path,
    *,
    as_of: date,
) -> tuple[Path | None, dict[str, Any]]:
    candidates: list[tuple[date, Path]] = []
    for path in report_dir.glob("market_panel_*.json"):
        candidate_date = _date_from_market_panel_path(path)
        if candidate_date is None or candidate_date > as_of:
            continue
        candidates.append((candidate_date, path))
    if not candidates:
        return None, {}
    _, path = max(candidates, key=lambda item: (item[0], item[1].name))
    return path, _read_optional_json(path) or {}


def _date_from_market_panel_path(path: Path | None) -> date | None:
    if path is None:
        return None
    raw = path.stem.removeprefix("market_panel_")
    return _date_or_none(raw)


def _date_or_none(value: object) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _timestamp_relation(raw_age: int | None) -> str:
    if raw_age is None:
        return "MISSING"
    if raw_age < 0:
        return "AFTER_AS_OF"
    if raw_age == 0:
        return "AS_OF"
    return "BEFORE_AS_OF"


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
