# ruff: noqa: E501

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.cache_catalog import (
    DEFAULT_CACHE_CATALOG_DIR,
    latest_cache_catalog_summary,
)
from ai_trading_system.data_source_fallback_policy import (
    DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    FALLBACK_STATE_FALLBACK_UNAVAILABLE,
    FALLBACK_STATE_FALLBACK_USED,
    latest_data_source_fallback_policy_summary,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness as signal_inputs,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
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
DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR = signal_inputs.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR
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
    "signal_input_completeness",
    "paper_shadow_daily_observation",
    "paper_shadow_drift_monitor",
    "paper_shadow_weekly_review",
    "evidence_staleness_monitor",
    "data_validation_result",
)


def _call_filtered_candidate_readiness_pipeline(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_filtered_candidate_readiness_pipeline,
    )

    return getattr(dynamic_v3_filtered_candidate_readiness_pipeline, name)(*args, **kwargs)


def _call_research_contract_ledger(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_research_contract_ledger

    return getattr(dynamic_v3_research_contract_ledger, name)(*args, **kwargs)


def build_formal_research_method_contract(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger("build_formal_research_method_contract", *args, **kwargs)


def formal_research_method_contract_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger(
        "formal_research_method_contract_report_payload", *args, **kwargs
    )


def validate_formal_research_method_contract_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger(
        "validate_formal_research_method_contract_artifact", *args, **kwargs
    )


def record_candidate_decision_ledger(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger("record_candidate_decision_ledger", *args, **kwargs)


def candidate_decision_ledger_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger(
        "candidate_decision_ledger_report_payload", *args, **kwargs
    )


def validate_candidate_decision_ledger_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_research_contract_ledger(
        "validate_candidate_decision_ledger_artifact", *args, **kwargs
    )


def run_filtered_candidate_evidence(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_filtered_candidate_evidence", *args, **kwargs
    )


def filtered_candidate_evidence_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "filtered_candidate_evidence_report_payload", *args, **kwargs
    )


def validate_filtered_candidate_evidence_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_filtered_candidate_evidence_artifact", *args, **kwargs
    )


def review_median_regime_filter_spec(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "review_median_regime_filter_spec", *args, **kwargs
    )


def median_regime_filter_spec_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "median_regime_filter_spec_report_payload", *args, **kwargs
    )


def validate_median_regime_filter_spec_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_median_regime_filter_spec_artifact", *args, **kwargs
    )


def run_filtered_candidate_stress_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_filtered_candidate_stress_backfill", *args, **kwargs
    )


def filtered_candidate_stress_backfill_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "filtered_candidate_stress_backfill_report_payload", *args, **kwargs
    )


def validate_filtered_candidate_stress_backfill_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_filtered_candidate_stress_backfill_artifact", *args, **kwargs
    )


def run_drawdown_mismatch_reduction(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_drawdown_mismatch_reduction", *args, **kwargs
    )


def drawdown_mismatch_reduction_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "drawdown_mismatch_reduction_report_payload", *args, **kwargs
    )


def validate_drawdown_mismatch_reduction_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_drawdown_mismatch_reduction_artifact", *args, **kwargs
    )


def run_flip_rotation_reduction(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_flip_rotation_reduction", *args, **kwargs
    )


def flip_rotation_reduction_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "flip_rotation_reduction_report_payload", *args, **kwargs
    )


def validate_flip_rotation_reduction_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_flip_rotation_reduction_artifact", *args, **kwargs
    )


def run_filtered_candidate_ab_review(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_filtered_candidate_ab_review", *args, **kwargs
    )


def filtered_candidate_ab_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "filtered_candidate_ab_review_report_payload", *args, **kwargs
    )


def validate_filtered_candidate_ab_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_filtered_candidate_ab_review_artifact", *args, **kwargs
    )


def register_signal_gate_confirmation(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "register_signal_gate_confirmation", *args, **kwargs
    )


def signal_gate_confirmation_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "signal_gate_confirmation_report_payload", *args, **kwargs
    )


def validate_signal_gate_confirmation_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_signal_gate_confirmation_artifact", *args, **kwargs
    )


def run_filtered_formalization_readiness(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_filtered_formalization_readiness", *args, **kwargs
    )


def filtered_formalization_readiness_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "filtered_formalization_readiness_report_payload", *args, **kwargs
    )


def validate_filtered_formalization_readiness_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_filtered_formalization_readiness_artifact", *args, **kwargs
    )


def build_owner_filtered_candidate_review(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "build_owner_filtered_candidate_review", *args, **kwargs
    )


def owner_filtered_candidate_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "owner_filtered_candidate_review_report_payload", *args, **kwargs
    )


def validate_owner_filtered_candidate_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_owner_filtered_candidate_review_artifact", *args, **kwargs
    )


def run_filtered_next_decision(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "run_filtered_next_decision", *args, **kwargs
    )


def filtered_next_decision_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "filtered_next_decision_report_payload", *args, **kwargs
    )


def validate_filtered_next_decision_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_readiness_pipeline(
        "validate_filtered_next_decision_artifact", *args, **kwargs
    )


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
    _write_text(
        root / "paper_shadow_protocol_report.md",
        render_paper_shadow_protocol_report(manifest, protocol),
    )
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
    exit_conditions = {
        row.get("exit_condition") for row in _records(protocol.get("exit_conditions"))
    }
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
    signal_input_completeness_id: str | None = None,
    signal_input_completeness_report_path: Path | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    paper_shadow_daily_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    signal_input_completeness_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    fallback_policy_report_path: Path | None = None,
    fallback_policy_output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Path | None = None,
    cache_catalog_output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
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
    fallback_summary = latest_data_source_fallback_policy_summary(
        report_path=fallback_policy_report_path,
        output_dir=fallback_policy_output_dir,
    )
    fallback_status = _text(fallback_summary.get("fallback_status"), "MISSING")
    cache_catalog_summary = latest_cache_catalog_summary(
        report_path=cache_catalog_report_path,
        output_dir=cache_catalog_output_dir,
    )
    cache_integrity_status = _text(
        cache_catalog_summary.get("cache_integrity_status"),
        "MISSING",
    )
    signal_input_summary = signal_inputs.latest_signal_input_completeness_summary(
        monitor_id=signal_input_completeness_id,
        report_path=signal_input_completeness_report_path,
        output_dir=signal_input_completeness_dir,
    )
    signal_input_status = _text(
        signal_input_summary.get("signal_input_status"),
        "MISSING",
    )
    stale_artifacts = [row.get("source_id") for row in findings if row.get("severity") == "STALE"]
    blocking_artifacts = [
        row.get("source_id") for row in findings if row.get("severity") == "BLOCKING"
    ]
    if fallback_status in {
        FALLBACK_STATE_FALLBACK_UNAVAILABLE,
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    }:
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "data_source_fallback_policy"])
        status = "BLOCKING"
    if cache_integrity_status == "FAIL" or _text(cache_catalog_summary.get("status")) == "FAIL":
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "cache_catalog"])
        status = "BLOCKING"
    if signal_input_status not in {"OK", "WARNING"}:
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "signal_input_completeness"])
        status = "BLOCKING"
    missing_artifacts = [row.get("source_id") for row in findings if row.get("missing") is True]
    if signal_input_summary.get("exists") is not True:
        missing_artifacts = _dedupe_texts([*missing_artifacts, "signal_input_completeness"])
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
    if fallback_status in {
        FALLBACK_STATE_FALLBACK_UNAVAILABLE,
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    }:
        next_refresh_action = _text(
            fallback_summary.get("next_action"),
            "restore_primary_or_valid_fallback_source",
        )
    if cache_integrity_status == "FAIL" or _text(cache_catalog_summary.get("status")) == "FAIL":
        next_refresh_action = _text(
            cache_catalog_summary.get("next_action"),
            "repair_cache_lineage_then_rerun_validate_data_and_cache_catalog",
        )
    if signal_input_status not in {"OK", "WARNING"}:
        next_refresh_action = _text(
            signal_input_summary.get("next_required_action"),
            "run_signal_input_completeness_monitor_before_paper_shadow",
        )
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
        "fallback_policy_summary": fallback_summary,
        "fallback_status": fallback_status,
        "fallback_used_count": fallback_summary.get("fallback_used_count"),
        "fallback_blocking_data_types": fallback_summary.get("blocking_data_types"),
        "cache_catalog_summary": cache_catalog_summary,
        "cache_integrity_status": cache_integrity_status,
        "cache_blocking_entry_count": cache_catalog_summary.get("blocking_entry_count"),
        "cache_blocking_entry_ids": cache_catalog_summary.get("blocking_entry_ids"),
        "cache_checksum_mismatch_count": cache_catalog_summary.get("checksum_mismatch_count"),
        "signal_input_completeness_summary": signal_input_summary,
        "signal_input_status": signal_input_status,
        "signal_input_blocking_count": signal_input_summary.get("blocking_count"),
        "signal_input_warning_count": signal_input_summary.get("warning_count"),
        "signal_input_blocking_input_ids": signal_input_summary.get("blocking_input_ids"),
        "signal_input_warning_input_ids": signal_input_summary.get("warning_input_ids"),
        "signal_input_report_path": signal_input_summary.get("report_path"),
        "coverage_blocking_artifacts": coverage_blocking_artifacts,
        "weekly_review_coverage_classification": weekly_coverage.get("coverage_classification"),
        "weekly_review_coverage_safe_for_continuation": weekly_coverage.get(
            "coverage_safe_for_continuation"
        ),
        "weekly_review_manual_coverage_override": weekly_coverage.get("manual_coverage_override"),
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
        "fallback_status": fallback_status,
        "fallback_policy_report_path": fallback_summary.get("report_path"),
        "cache_integrity_status": cache_integrity_status,
        "cache_catalog_report_path": cache_catalog_summary.get("report_path"),
        "signal_input_status": signal_input_status,
        "signal_input_completeness_id": signal_input_summary.get("monitor_id"),
        "signal_input_report_path": signal_input_summary.get("report_path"),
        "weekly_review_coverage_classification": weekly_coverage.get("coverage_classification"),
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
    _write_text(
        root / "evidence_staleness_report.md", render_evidence_staleness_report(manifest, report)
    )
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


def _expected_evidence_staleness_blocking_artifacts(
    report: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> set[str]:
    expected = {
        _text(row.get("source_id")) for row in findings if row.get("severity") == "BLOCKING"
    }
    fallback_status = _text(report.get("fallback_status"))
    if fallback_status in {
        FALLBACK_STATE_FALLBACK_UNAVAILABLE,
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    }:
        expected.add("data_source_fallback_policy")
    if (
        _text(report.get("cache_integrity_status")) == "FAIL"
        or _text(_mapping(report.get("cache_catalog_summary")).get("status")) == "FAIL"
    ):
        expected.add("cache_catalog")
    if _text(report.get("signal_input_status"), "MISSING") not in {"OK", "WARNING"}:
        expected.add("signal_input_completeness")
    expected.discard("")
    return expected


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
            st._check(
                "policy_metadata_visible",
                bool(policy.get("policy_id")) and bool(policy.get("version")),
                "",
            ),
            st._check("freshness_rules_complete", expected_sources.issubset(set(rules)), ""),
            st._check(
                "severity_order_complete",
                {"FRESH", "ACCEPTABLE", "STALE", "BLOCKING"}.issubset(severity_order),
                "",
            ),
            st._check(
                "expected_sources_present",
                expected_sources.issubset(finding_sources),
                ",".join(sorted(_texts(finding_sources))),
            ),
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
                == _expected_evidence_staleness_blocking_artifacts(report, findings),
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
                "fallback_policy_fields_visible",
                bool(report.get("fallback_status"))
                and "fallback_policy_summary" in report
                and "fallback_used_count" in report,
                "",
            ),
            st._check(
                "cache_catalog_fields_visible",
                bool(report.get("cache_integrity_status"))
                and "cache_catalog_summary" in report
                and "cache_blocking_entry_count" in report
                and "cache_checksum_mismatch_count" in report,
                "",
            ),
            st._check(
                "signal_input_completeness_fields_visible",
                bool(report.get("signal_input_status"))
                and "signal_input_completeness_summary" in report
                and "signal_input_blocking_count" in report,
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
            st._check(
                "read_only_monitor",
                report.get("data_downloaded_by_monitor") is False
                and report.get("pipelines_executed_by_monitor") is False,
                "",
            ),
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
    signal_input_completeness_id: str | None = None,
    signal_input_completeness_report_path: Path | None = None,
    data_quality_report_path: Path | None = None,
    data_quality_report_dir: Path = DEFAULT_MARKET_PANEL_REPORT_DIR,
    paper_shadow_daily_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    evidence_staleness_monitor_dir: Path = DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    signal_input_completeness_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    fallback_policy_report_path: Path | None = None,
    fallback_policy_output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Path | None = None,
    cache_catalog_output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
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
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
    )
    data_validation = _data_quality_report_summary(
        data_quality_report_path=data_quality_report_path,
        data_quality_report_dir=data_quality_report_dir,
        as_of=effective_as_of,
    )
    source_artifacts["data_validation_result"] = data_validation
    evidence_report = _mapping(source_artifacts["evidence_staleness_monitor"].get("detail"))
    if fallback_policy_report_path is not None:
        fallback_summary = latest_data_source_fallback_policy_summary(
            report_path=fallback_policy_report_path,
            output_dir=fallback_policy_output_dir,
        )
    else:
        fallback_summary = _mapping(evidence_report.get("fallback_policy_summary"))
        if not fallback_summary or _text(fallback_summary.get("fallback_status")) == "MISSING":
            fallback_summary = latest_data_source_fallback_policy_summary(
                output_dir=fallback_policy_output_dir,
            )
    fallback_status = _text(fallback_summary.get("fallback_status"), "MISSING")
    if cache_catalog_report_path is not None:
        cache_catalog_summary = latest_cache_catalog_summary(
            report_path=cache_catalog_report_path,
            output_dir=cache_catalog_output_dir,
        )
    else:
        cache_catalog_summary = _mapping(evidence_report.get("cache_catalog_summary"))
        if (
            not cache_catalog_summary
            or _text(cache_catalog_summary.get("cache_integrity_status")) == "MISSING"
        ):
            cache_catalog_summary = latest_cache_catalog_summary(
                output_dir=cache_catalog_output_dir,
            )
    cache_integrity_status = _text(
        cache_catalog_summary.get("cache_integrity_status"),
        "MISSING",
    )
    signal_input_summary = _mapping(source_artifacts["signal_input_completeness"].get("summary"))
    signal_input_status = _text(
        signal_input_summary.get("signal_input_status"),
        "MISSING",
    )
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
    if fallback_status in {
        FALLBACK_STATE_FALLBACK_UNAVAILABLE,
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    }:
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "data_source_fallback_policy"])
    if cache_integrity_status == "FAIL" or _text(cache_catalog_summary.get("status")) == "FAIL":
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "cache_catalog"])
    if signal_input_status not in {"OK", "WARNING"}:
        blocking_artifacts = _dedupe_texts([*blocking_artifacts, "signal_input_completeness"])
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
    if readiness == "READY_TO_CONTINUE" and fallback_status == FALLBACK_STATE_FALLBACK_USED:
        readiness = "READY_WITH_WARNINGS"
    safe_to_continue = readiness in {"READY_TO_CONTINUE", "READY_WITH_WARNINGS"}
    next_required_action = _shadow_continuation_next_action(readiness)
    if cache_integrity_status == "FAIL" or _text(cache_catalog_summary.get("status")) == "FAIL":
        next_required_action = _text(
            cache_catalog_summary.get("next_action"),
            next_required_action,
        )
    if signal_input_status not in {"OK", "WARNING"}:
        next_required_action = _text(
            signal_input_summary.get("next_required_action"),
            next_required_action,
        )
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
        "fallback_policy_summary": fallback_summary,
        "fallback_status": fallback_status,
        "fallback_used_count": fallback_summary.get("fallback_used_count"),
        "fallback_blocking_data_types": fallback_summary.get("blocking_data_types"),
        "cache_catalog_summary": cache_catalog_summary,
        "cache_integrity_status": cache_integrity_status,
        "cache_blocking_entry_count": cache_catalog_summary.get("blocking_entry_count"),
        "cache_blocking_entry_ids": cache_catalog_summary.get("blocking_entry_ids"),
        "cache_checksum_mismatch_count": cache_catalog_summary.get("checksum_mismatch_count"),
        "signal_input_completeness_summary": signal_input_summary,
        "signal_input_status": signal_input_status,
        "signal_input_blocking_count": signal_input_summary.get("blocking_count"),
        "signal_input_warning_count": signal_input_summary.get("warning_count"),
        "signal_input_blocking_input_ids": signal_input_summary.get("blocking_input_ids"),
        "signal_input_warning_input_ids": signal_input_summary.get("warning_input_ids"),
        "signal_input_report_path": signal_input_summary.get("report_path"),
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
        "fallback_status": fallback_status,
        "fallback_policy_report_path": fallback_summary.get("report_path"),
        "cache_integrity_status": cache_integrity_status,
        "cache_catalog_report_path": cache_catalog_summary.get("report_path"),
        "signal_input_status": signal_input_status,
        "signal_input_completeness_id": signal_input_summary.get("monitor_id"),
        "signal_input_report_path": signal_input_summary.get("report_path"),
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
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
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
                "fallback_policy_fields_visible",
                bool(report.get("fallback_status"))
                and "fallback_policy_summary" in report
                and "fallback_used_count" in report,
                "",
            ),
            st._check(
                "cache_catalog_fields_visible",
                bool(report.get("cache_integrity_status"))
                and "cache_catalog_summary" in report
                and "cache_blocking_entry_count" in report
                and "cache_checksum_mismatch_count" in report,
                "",
            ),
            st._check(
                "signal_input_completeness_fields_visible",
                bool(report.get("signal_input_status"))
                and "signal_input_completeness_summary" in report
                and "signal_input_blocking_count" in report,
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "shadow_continuation_readiness" in reader
                and "safe_to_continue_shadow" in reader
                and "signal_input_status" in reader,
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
            f"- fallback_status: {report.get('fallback_status')}",
            f"- fallback_used_count: {report.get('fallback_used_count')}",
            f"- fallback_blocking_data_types: {report.get('fallback_blocking_data_types')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- cache_blocking_entry_ids: {', '.join(_texts(report.get('cache_blocking_entry_ids'))) or 'none'}",
            f"- cache_checksum_mismatch_count: {report.get('cache_checksum_mismatch_count')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- signal_input_blocking_count: {report.get('signal_input_blocking_count')}",
            f"- signal_input_warning_count: {report.get('signal_input_warning_count')}",
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
            f"- fallback_status: {report.get('fallback_status')}",
            f"- fallback_used_count: {report.get('fallback_used_count')}",
            f"- fallback_blocking_data_types: {report.get('fallback_blocking_data_types')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- cache_blocking_entry_ids: {', '.join(_texts(report.get('cache_blocking_entry_ids'))) or 'none'}",
            f"- cache_checksum_mismatch_count: {report.get('cache_checksum_mismatch_count')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- signal_input_blocking_count: {report.get('signal_input_blocking_count')}",
            f"- signal_input_warning_count: {report.get('signal_input_warning_count')}",
            f"- signal_input_blocking_input_ids: {', '.join(_texts(report.get('signal_input_blocking_input_ids'))) or 'none'}",
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
            f"- fallback_status: {report.get('fallback_status')}",
            f"- fallback_used_count: {report.get('fallback_used_count')}",
            f"- fallback_blocking_data_types: {report.get('fallback_blocking_data_types')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- cache_blocking_entry_ids: {', '.join(_texts(report.get('cache_blocking_entry_ids'))) or 'none'}",
            f"- cache_checksum_mismatch_count: {report.get('cache_checksum_mismatch_count')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- signal_input_blocking_count: {report.get('signal_input_blocking_count')}",
            f"- signal_input_warning_count: {report.get('signal_input_warning_count')}",
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
            f"- fallback_status: {report.get('fallback_status')}",
            f"- fallback_used_count: {report.get('fallback_used_count')}",
            f"- fallback_blocking_data_types: {report.get('fallback_blocking_data_types')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- cache_blocking_entry_ids: {', '.join(_texts(report.get('cache_blocking_entry_ids'))) or 'none'}",
            f"- cache_checksum_mismatch_count: {report.get('cache_checksum_mismatch_count')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- signal_input_blocking_count: {report.get('signal_input_blocking_count')}",
            f"- signal_input_warning_count: {report.get('signal_input_warning_count')}",
            f"- signal_input_blocking_input_ids: {', '.join(_texts(report.get('signal_input_blocking_input_ids'))) or 'none'}",
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
                _mapping(paper_shadow_daily.get("manifest")).get("paper_shadow_daily_manifest_path")
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
                _mapping(paper_shadow_drift.get("manifest")).get("paper_shadow_drift_manifest_path")
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
    calendar_adjusted_staleness = bool(calendar_context.get("calendar_adjusted_staleness"))
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
    missing = rule.get("required") is not False and (
        timestamp is None
        or not _text(artifact_id)
        or (source_path is not None and not source_exists)
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
        "calendar_adjustment_reason": _text(calendar_context.get("calendar_adjustment_reason")),
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
        "coverage_blocking_artifacts": [] if safe else ["paper_shadow_weekly_review"],
    }


def _shadow_continuation_source_artifacts(
    *,
    paper_shadow_daily_id: str | None,
    paper_shadow_drift_monitor_id: str | None,
    paper_shadow_weekly_review_id: str | None,
    evidence_staleness_monitor_id: str | None,
    signal_input_completeness_id: str | None,
    signal_input_completeness_report_path: Path | None,
    paper_shadow_daily_dir: Path,
    paper_shadow_drift_monitor_dir: Path,
    paper_shadow_weekly_review_dir: Path,
    evidence_staleness_monitor_dir: Path,
    signal_input_completeness_dir: Path,
) -> dict[str, dict[str, Any]]:
    signal_input_summary = signal_inputs.latest_signal_input_completeness_summary(
        monitor_id=signal_input_completeness_id,
        report_path=signal_input_completeness_report_path,
        output_dir=signal_input_completeness_dir,
    )
    return {
        "signal_input_completeness": _shadow_continuation_signal_input_source_artifact(
            signal_input_summary
        ),
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


def _shadow_continuation_signal_input_source_artifact(
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    report_path = (
        Path(_text(summary.get("report_path"))) if _text(summary.get("report_path")) else None
    )
    validation_path = (
        report_path.with_name("signal_input_completeness_validation.json")
        if report_path is not None
        else None
    )
    validation = st._read_optional_json(validation_path) if validation_path is not None else {}
    detail = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_summary",
        **dict(summary),
        **signal_inputs.SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    return {
        "source_id": "signal_input_completeness",
        "source_label": "Signal input completeness",
        "exists": summary.get("exists") is True,
        "artifact_id": _text(summary.get("monitor_id")),
        "source_path": "" if report_path is None else str(report_path),
        "root": "" if report_path is None else str(report_path.parent),
        "manifest": {},
        "detail": detail,
        "validation": validation or {},
        "summary": dict(summary),
        "status": _text(summary.get("signal_input_status"), "MISSING"),
        "validation_status": _text(_mapping(validation).get("status"), "NOT_RUN"),
        "generated_at": _text(summary.get("generated_at")),
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
    if coverage_status != "PASS" or evidence_report.get("safe_to_continue_shadow") is not True:
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
    return max(
        candidates,
        key=lambda path: (
            _date_from_data_quality_path(path) or date.min,
            path.stat().st_mtime,
            path.name,
        ),
    )


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
