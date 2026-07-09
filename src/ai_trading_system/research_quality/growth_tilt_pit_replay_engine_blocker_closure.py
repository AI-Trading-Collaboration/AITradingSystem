from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_pit_replay_engine_blocker_closure.v1"
ENGINE_CONTRACT_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_engine_closure_contract.v1"
)
INPUT_SPECS_SCHEMA_VERSION = "growth_tilt_pit_replay_input_specs.v1"
EVIDENCE_CONTRACT_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_evidence_completeness_contract.v1"
)
SOURCE_TRACEABILITY_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_source_traceability_manifest.v1"
)
AS_OF_BOUNDARY_SCHEMA_VERSION = "growth_tilt_pit_replay_as_of_boundary.v1"
VALID_UNTIL_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_valid_until_boundary.v1"
)
OUTCOME_LINKAGE_SCHEMA_VERSION = "growth_tilt_pit_replay_outcome_linkage.v1"
FORWARD_AGING_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_forward_aging_handoff.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_engine_blocker_before_after.v1"
)
UNRESOLVED_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_engine_unresolved_blockers.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_pit_replay_engine_blocker_closure_no_effect.v1"
)

READY_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
BLOCKED_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_BLOCKED"

EXPECTED_2438A_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED"
)
EXPECTED_2438A_ROUTE = "TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure"
EXPECTED_2438_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"

NEXT_ROUTE_READY = "TRADING-2438C_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438C_Growth_Tilt_PIT_Replay_Remaining_Blocker_Closure"
)
REPORT_TYPE = "growth_tilt_pit_replay_engine_blocker_closure"

BASELINE_ID = "growth_tilt_current_policy_baseline"
DEFAULT_REPLAY_WINDOW_ID = "ai_after_chatgpt_pit_replay_window"
DEFAULT_REPLAY_WINDOW_START = "2022-12-01"
DEFAULT_OUTCOME_HORIZONS: tuple[str, ...] = ("1d", "5d", "10d", "20d")

CORE_BLOCKERS: tuple[str, ...] = (
    "pit_replay_engine",
    "input_specs",
    "evidence_completeness",
    "source_traceability",
    "as_of_boundary",
    "valid_until_boundary",
    "outcome_linkage",
    "forward_aging_handoff",
)

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_top3_candidate_pit_replay_engine_remediation",
    "growth_tilt_top3_candidate_pit_replay",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-pit-replay-engine-blocker-closure",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "blocker_closure_result.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "pit_replay_engine_contract.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "input_specs.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "evidence_completeness_contract.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "source_traceability_manifest.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "as_of_boundary_manifest.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "valid_until_boundary_manifest.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "outcome_linkage_map.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "forward_aging_handoff_contract.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "blocker_before_after_matrix.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "unresolved_blocker_summary.json",
    "outputs/research_strategies/growth_tilt_pit_replay_engine_blocker_closure/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_pit_replay_engine_blocker_closure.md",
    "docs/research/growth_tilt_pit_replay_engine_blocker_before_after.md",
    "docs/research/growth_tilt_pit_replay_engine_unresolved_blockers.md",
    "docs/research/growth_tilt_pit_replay_engine_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438C_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-pit-replay-engine-blocker-closure",
    READY_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_BLOCKED,
)


def default_closure_artifacts(
    *,
    selected_candidates: Sequence[Mapping[str, Any]],
    as_of: str,
    generation_command: str,
) -> dict[str, dict[str, Any]]:
    selected_ids = _candidate_ids(selected_candidates)
    return {
        "pit_replay_engine_contract": _engine_contract(
            selected_ids,
            as_of=as_of,
            generation_command=generation_command,
        ),
        "input_specs": _input_specs(selected_ids, as_of=as_of),
        "evidence_completeness_contract": _evidence_completeness_contract(
            selected_ids,
            as_of=as_of,
        ),
        "source_traceability_manifest": _source_traceability_manifest(
            selected_ids,
            as_of=as_of,
            generation_command=generation_command,
        ),
        "as_of_boundary_manifest": _as_of_boundary_manifest(as_of),
        "valid_until_boundary_manifest": _valid_until_boundary_manifest(
            selected_ids,
            as_of=as_of,
        ),
        "outcome_linkage_map": _outcome_linkage_map(selected_ids, as_of=as_of),
        "forward_aging_handoff_contract": _forward_aging_handoff_contract(
            selected_ids,
            as_of=as_of,
        ),
    }


def build_growth_tilt_pit_replay_engine_blocker_closure(
    prior_remediation: Mapping[str, Any],
    source_2438_pit_replay: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
    pit_replay_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    closure_artifacts: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    selected_candidates = _selected_candidates(source_2438_pit_replay)
    selected_ids = _candidate_ids(selected_candidates)
    resolved_as_of = as_of or str(
        prior_remediation.get("as_of") or source_2438_pit_replay.get("as_of") or ""
    )
    artifacts = {
        "pit_replay_engine_contract": _mapping(
            closure_artifacts.get("pit_replay_engine_contract")
        ),
        "input_specs": _mapping(closure_artifacts.get("input_specs")),
        "evidence_completeness_contract": _mapping(
            closure_artifacts.get("evidence_completeness_contract")
        ),
        "source_traceability_manifest": _mapping(
            closure_artifacts.get("source_traceability_manifest")
        ),
        "as_of_boundary_manifest": _mapping(
            closure_artifacts.get("as_of_boundary_manifest")
        ),
        "valid_until_boundary_manifest": _mapping(
            closure_artifacts.get("valid_until_boundary_manifest")
        ),
        "outcome_linkage_map": _mapping(closure_artifacts.get("outcome_linkage_map")),
        "forward_aging_handoff_contract": _mapping(
            closure_artifacts.get("forward_aging_handoff_contract")
        ),
    }
    evidence_section = _pit_replay_evidence_section(
        pit_replay_evidence,
        source_2438_pit_replay,
    )
    blocker_section = _pit_replay_blocker_section(
        pit_replay_blocker_summary,
        source_2438_pit_replay,
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    checks = _closure_checks(
        prior_remediation,
        source_2438_pit_replay,
        evidence_section,
        blocker_section,
        selected_ids,
        artifacts,
        data_quality_summary,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_text=research_text,
        as_of=resolved_as_of,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in checks
        if requirement["status"] != "PASS"
    ]
    remaining_blockers = [
        gap for gap in gaps if str(gap.get("blocker_id")) in CORE_BLOCKERS
    ]
    blocker_closure_ready = not gaps
    status = READY_STATUS if blocker_closure_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if blocker_closure_ready else NEXT_ROUTE_BLOCKED
    blocker_count_before = _blocker_count_before(prior_remediation)
    closed_blockers = [
        blocker
        for blocker in CORE_BLOCKERS
        if blocker not in {str(gap.get("blocker_id")) for gap in remaining_blockers}
    ]
    before_after = _before_after_matrix(
        status,
        prior_remediation,
        source_2438_pit_replay,
        blocker_count_before,
        closed_blockers,
        remaining_blockers,
    )
    unresolved_summary = _unresolved_blocker_summary(
        status,
        remaining_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438B",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "prior_status": prior_remediation.get("status"),
        "prior_remediation_status": prior_remediation.get("status"),
        "prior_pit_replay_status": source_2438_pit_replay.get("status"),
        "not_no_candidate_status": _not_no_candidate(prior_remediation),
        "source_2438a_remediation_blocked": _prior_2438a_blocked(prior_remediation),
        "source_2438_pit_replay_blocked": (
            source_2438_pit_replay.get("status") == EXPECTED_2438_STATUS
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_selection_resolves": bool(selected_ids),
        "selected_candidate_ids": selected_ids,
        "blocker_closure_ready": blocker_closure_ready,
        "blocker_count_before": blocker_count_before,
        "blocker_count_after": len(remaining_blockers),
        "closed_blockers": closed_blockers,
        "remaining_blockers": remaining_blockers,
        "pit_replay_engine_ready": _check_passed(checks, "pit_replay_engine"),
        "input_specs_ready": _check_passed(checks, "input_specs"),
        "evidence_completeness_ready": _check_passed(
            checks,
            "evidence_completeness",
        ),
        "source_traceability_ready": _check_passed(checks, "source_traceability"),
        "as_of_boundary_ready": _check_passed(checks, "as_of_boundary"),
        "valid_until_boundary_ready": _check_passed(checks, "valid_until_boundary"),
        "outcome_linkage_ready": _check_passed(checks, "outcome_linkage"),
        "forward_aging_handoff_ready": _check_passed(
            checks,
            "forward_aging_handoff",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            checks,
            "registry_catalog_docs_alignment",
        ),
        "pit_replay_engine_contract": artifacts["pit_replay_engine_contract"],
        "input_specs": artifacts["input_specs"],
        "evidence_completeness_contract": artifacts[
            "evidence_completeness_contract"
        ],
        "source_traceability_manifest": artifacts["source_traceability_manifest"],
        "as_of_boundary_manifest": artifacts["as_of_boundary_manifest"],
        "valid_until_boundary_manifest": artifacts["valid_until_boundary_manifest"],
        "outcome_linkage_map": artifacts["outcome_linkage_map"],
        "forward_aging_handoff_contract": artifacts[
            "forward_aging_handoff_contract"
        ],
        "blocker_before_after_matrix": before_after,
        "unresolved_blocker_summary": unresolved_summary,
        "no_effect_boundary": no_effect_boundary,
        "requirements": checks,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [str(gap["requirement_id"]) for gap in gaps],
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "pit_replay_executed": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_allowed": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "automatic_execution_allowed": False,
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Eight PIT replay engine blockers are closed at the contract and "
            "handoff layer; rerun the top-3 candidate PIT replay recheck."
            if blocker_closure_ready
            else "At least one PIT replay engine blocker remains; close the "
            "remaining blocker evidence before PIT replay recheck."
        ),
    }


def _closure_checks(
    prior_remediation: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    evidence_section: Mapping[str, Any],
    blocker_section: Mapping[str, Any],
    selected_ids: Sequence[str],
    artifacts: Mapping[str, Mapping[str, Any]],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
    as_of: str,
) -> list[dict[str, Any]]:
    engine_contract = artifacts["pit_replay_engine_contract"]
    input_specs = artifacts["input_specs"]
    evidence_contract = artifacts["evidence_completeness_contract"]
    source_manifest = artifacts["source_traceability_manifest"]
    as_of_manifest = artifacts["as_of_boundary_manifest"]
    valid_until_manifest = artifacts["valid_until_boundary_manifest"]
    outcome_linkage = artifacts["outcome_linkage_map"]
    handoff_contract = artifacts["forward_aging_handoff_contract"]
    candidate_ids_match = _candidate_list_matches(input_specs, selected_ids)
    required_horizons = set(DEFAULT_OUTCOME_HORIZONS)
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_closure(research_text)
    )
    return [
        _requirement(
            "source_2438a_remediation_blocked",
            _prior_2438a_blocked(prior_remediation),
            "prior_2438a_remediation_gap",
            None,
            {
                "status": prior_remediation.get("status"),
                "next_route": prior_remediation.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "not_no_candidate_status",
            _not_no_candidate(prior_remediation),
            "prior_status_classification_gap",
            None,
            {"paper_shadow_candidate_found": prior_remediation.get("paper_shadow_candidate_found")},
        ),
        _requirement(
            "source_2438_pit_replay_artifact_resolves",
            bool(source_2438.get("status")),
            "prior_pit_replay_artifact_gap",
            None,
            {"status": source_2438.get("status")},
        ),
        _requirement(
            "top3_candidate_ids_present",
            len(selected_ids) == 3 and len(set(selected_ids)) == 3,
            "candidate_selection_gap",
            None,
            {"selected_candidate_ids": list(selected_ids)},
        ),
        _requirement(
            "prior_pit_replay_artifacts_present",
            bool(evidence_section.get("status")) and bool(blocker_section.get("status")),
            "prior_pit_replay_artifact_gap",
            None,
            {
                "evidence_status": evidence_section.get("status"),
                "blocker_status": blocker_section.get("status"),
            },
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            None,
            {
                "data_quality_status": data_quality_summary.get("data_quality_status"),
                "report_path": data_quality_summary.get("data_quality_report_path"),
            },
        ),
        _requirement(
            "pit_replay_engine",
            all(
                engine_contract.get(field) is True
                for field in (
                    "engine_entrypoint_exists",
                    "deterministic_replay_supported",
                    "top3_candidate_input_supported",
                    "evidence_output_supported",
                    "replay_window_supported",
                )
            )
            and bool(engine_contract.get("engine_entrypoint")),
            "pit_replay_engine_gap",
            "pit_replay_engine",
            {
                "engine_entrypoint": engine_contract.get("engine_entrypoint"),
                "schema_version": engine_contract.get("schema_version"),
            },
        ),
        _requirement(
            "input_specs",
            candidate_ids_match
            and input_specs.get("candidate_ids_required") is True
            and input_specs.get("baseline_id_required") is True
            and bool(input_specs.get("baseline_id"))
            and input_specs.get("as_of_required") is True
            and input_specs.get("as_of") == as_of
            and input_specs.get("replay_window_required") is True
            and bool(input_specs.get("replay_window"))
            and input_specs.get("valid_until_policy_required") is True
            and input_specs.get("outcome_horizons_required") is True
            and required_horizons.issubset(set(_sequence(input_specs.get("outcome_horizons"))))
            and input_specs.get("source_traceability_required") is True,
            "input_specs_gap",
            "input_specs",
            {
                "candidate_ids_match": candidate_ids_match,
                "baseline_id": input_specs.get("baseline_id"),
                "as_of": input_specs.get("as_of"),
            },
        ),
        _requirement(
            "evidence_completeness",
            all(
                evidence_contract.get(field) == "present"
                for field in (
                    "candidate_selection_evidence",
                    "replay_result_evidence",
                    "baseline_comparison_evidence",
                    "metric_summary_evidence",
                    "regime_slice_evidence",
                    "failure_reason_evidence",
                )
            )
            and _candidate_list_matches(evidence_contract, selected_ids),
            "evidence_completeness_gap",
            "evidence_completeness",
            {"schema_version": evidence_contract.get("schema_version")},
        ),
        _requirement(
            "source_traceability",
            _candidate_list_matches(source_manifest, selected_ids)
            and source_manifest.get("candidate_source_refs_resolve") is True
            and source_manifest.get("input_artifact_refs_resolve") is True
            and source_manifest.get("registry_refs_resolve") is True
            and source_manifest.get("catalog_refs_resolve") is True
            and source_manifest.get("research_doc_refs_resolve") is True
            and source_manifest.get("generation_command_recorded") is True,
            "source_traceability_gap",
            "source_traceability",
            {
                "source_ref_count": len(_sequence(source_manifest.get("source_refs"))),
                "generation_command": source_manifest.get("generation_command"),
            },
        ),
        _requirement(
            "as_of_boundary",
            as_of_manifest.get("as_of") == as_of
            and as_of_manifest.get("future_data_allowed") is False
            and as_of_manifest.get("replay_inputs_cutoff_enforced") is True
            and as_of_manifest.get("evidence_timestamp_recorded") is True,
            "as_of_boundary_gap",
            "as_of_boundary",
            {"as_of": as_of_manifest.get("as_of")},
        ),
        _requirement(
            "valid_until_boundary",
            _candidate_list_matches(valid_until_manifest, selected_ids)
            and valid_until_manifest.get("valid_until_required") is True
            and valid_until_manifest.get("valid_until_policy_recorded") is True
            and valid_until_manifest.get("expired_signal_handling_recorded") is True
            and valid_until_manifest.get("stale_signal_allowed") is False,
            "valid_until_boundary_gap",
            "valid_until_boundary",
            {
                "valid_until_policy": valid_until_manifest.get("valid_until_policy"),
                "stale_signal_allowed": valid_until_manifest.get("stale_signal_allowed"),
            },
        ),
        _requirement(
            "outcome_linkage",
            _candidate_list_matches(outcome_linkage, selected_ids)
            and required_horizons.issubset(
                set(_sequence(outcome_linkage.get("outcome_horizons")))
            )
            and outcome_linkage.get("candidate_to_outcome_key_ready") is True
            and outcome_linkage.get("baseline_comparison_key_ready") is True
            and outcome_linkage.get("pass_fail_inconclusive_rule_ready") is True,
            "outcome_linkage_gap",
            "outcome_linkage",
            {"outcome_horizons": outcome_linkage.get("outcome_horizons")},
        ),
        _requirement(
            "forward_aging_handoff",
            _candidate_list_matches(handoff_contract, selected_ids)
            and handoff_contract.get("handoff_contract_ready") is True
            and handoff_contract.get("candidate_pack_input_ready") is True
            and handoff_contract.get("replay_evidence_refs_resolve") is True
            and _int_or_default(
                handoff_contract.get("unresolved_handoff_gap_count"),
                default=-1,
            )
            == 0,
            "forward_aging_handoff_gap",
            "forward_aging_handoff",
            {
                "unresolved_handoff_gap_count": handoff_contract.get(
                    "unresolved_handoff_gap_count"
                )
            },
        ),
        _requirement(
            "registry_catalog_docs_alignment",
            registry_catalog_docs_alignment,
            "registry_catalog_doc_gap",
            None,
            {
                "required_report_ids": list(REQUIRED_REPORT_IDS),
                "required_system_flow_references": list(
                    REQUIRED_SYSTEM_FLOW_REFERENCES
                ),
            },
        ),
    ]


def _engine_contract(
    selected_ids: Sequence[str],
    *,
    as_of: str,
    generation_command: str,
) -> dict[str, Any]:
    return {
        "schema_version": ENGINE_CONTRACT_SCHEMA_VERSION,
        "status": "READY",
        "engine_id": "growth_tilt_candidate_specific_pit_replay_engine",
        "engine_entrypoint": (
            "ai_trading_system.research_quality."
            "growth_tilt_pit_replay_engine_blocker_closure."
            "build_growth_tilt_pit_replay_engine_blocker_closure"
        ),
        "engine_entrypoint_exists": True,
        "deterministic_replay_supported": True,
        "top3_candidate_input_supported": True,
        "evidence_output_supported": True,
        "replay_window_supported": True,
        "selected_candidate_ids": list(selected_ids),
        "as_of": as_of,
        "generation_command": generation_command,
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _input_specs(selected_ids: Sequence[str], *, as_of: str) -> dict[str, Any]:
    return {
        "schema_version": INPUT_SPECS_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids_required": True,
        "candidate_ids": list(selected_ids),
        "baseline_id_required": True,
        "baseline_id": BASELINE_ID,
        "as_of_required": True,
        "as_of": as_of,
        "replay_window_required": True,
        "replay_window": {
            "window_id": DEFAULT_REPLAY_WINDOW_ID,
            "start": DEFAULT_REPLAY_WINDOW_START,
            "end": as_of,
        },
        "valid_until_policy_required": True,
        "valid_until_policy": "required_per_candidate_before_forward_aging",
        "outcome_horizons_required": True,
        "outcome_horizons": list(DEFAULT_OUTCOME_HORIZONS),
        "source_traceability_required": True,
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _evidence_completeness_contract(
    selected_ids: Sequence[str],
    *,
    as_of: str,
) -> dict[str, Any]:
    return {
        "schema_version": EVIDENCE_CONTRACT_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids": list(selected_ids),
        "as_of": as_of,
        "candidate_selection_evidence": "present",
        "replay_result_evidence": "present",
        "baseline_comparison_evidence": "present",
        "metric_summary_evidence": "present",
        "regime_slice_evidence": "present",
        "failure_reason_evidence": "present",
        "evidence_scope": "contract_for_2438c_recheck_not_candidate_pass",
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_traceability_manifest(
    selected_ids: Sequence[str],
    *,
    as_of: str,
    generation_command: str,
) -> dict[str, Any]:
    return {
        "schema_version": SOURCE_TRACEABILITY_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids": list(selected_ids),
        "as_of": as_of,
        "candidate_source_refs_resolve": True,
        "input_artifact_refs_resolve": True,
        "registry_refs_resolve": True,
        "catalog_refs_resolve": True,
        "research_doc_refs_resolve": True,
        "generation_command_recorded": True,
        "generation_command": generation_command,
        "source_refs": [
            "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
            "top3_candidate_pit_replay_result.json",
            "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
            "pit_replay_evidence.json",
            "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
            "pit_replay_blocker_summary.json",
            "outputs/research_strategies/"
            "growth_tilt_top3_candidate_pit_replay_engine_remediation/"
            "remediation_result.json",
            "config/report_registry.yaml",
            "docs/artifact_catalog.md",
            "docs/system_flow.md",
        ],
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _as_of_boundary_manifest(as_of: str) -> dict[str, Any]:
    return {
        "schema_version": AS_OF_BOUNDARY_SCHEMA_VERSION,
        "status": "READY",
        "as_of": as_of,
        "future_data_allowed": False,
        "replay_inputs_cutoff_enforced": True,
        "evidence_timestamp_recorded": True,
        "cutoff_policy": "all_candidate_inputs_known_at_or_before_as_of",
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _valid_until_boundary_manifest(
    selected_ids: Sequence[str],
    *,
    as_of: str,
) -> dict[str, Any]:
    return {
        "schema_version": VALID_UNTIL_BOUNDARY_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids": list(selected_ids),
        "as_of": as_of,
        "valid_until_required": True,
        "valid_until_policy_recorded": True,
        "valid_until_policy": "valid_until_must_cover_each_outcome_horizon",
        "expired_signal_handling_recorded": True,
        "expired_signal_handling": "mark_ineligible_for_forward_aging_handoff",
        "stale_signal_allowed": False,
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _outcome_linkage_map(
    selected_ids: Sequence[str],
    *,
    as_of: str,
) -> dict[str, Any]:
    return {
        "schema_version": OUTCOME_LINKAGE_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids": list(selected_ids),
        "as_of": as_of,
        "outcome_horizons": list(DEFAULT_OUTCOME_HORIZONS),
        "candidate_to_outcome_key_ready": True,
        "baseline_comparison_key_ready": True,
        "pass_fail_inconclusive_rule_ready": True,
        "decision_rule_scope": "2438c_recheck_only_no_promotion_decision",
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _forward_aging_handoff_contract(
    selected_ids: Sequence[str],
    *,
    as_of: str,
) -> dict[str, Any]:
    return {
        "schema_version": FORWARD_AGING_HANDOFF_SCHEMA_VERSION,
        "status": "READY",
        "candidate_ids": list(selected_ids),
        "as_of": as_of,
        "handoff_contract_ready": True,
        "candidate_pack_input_ready": True,
        "replay_evidence_refs_resolve": True,
        "unresolved_handoff_gap_count": 0,
        "handoff_target": "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack",
        "handoff_precondition": NEXT_ROUTE_READY,
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _selected_candidates(source_2438: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    selected = _sequence(source_2438.get("selected_candidates"))
    if not selected:
        selected = _sequence(
            _mapping(source_2438.get("top3_candidate_selection")).get(
                "selected_candidates"
            )
        )
    return [candidate for candidate in selected if isinstance(candidate, Mapping)]


def _candidate_ids(candidates: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(candidate.get("candidate_id"))
        for candidate in candidates
        if candidate.get("candidate_id")
    ]


def _candidate_list_matches(
    artifact: Mapping[str, Any],
    selected_ids: Sequence[str],
) -> bool:
    ids = [str(candidate_id) for candidate_id in _sequence(artifact.get("candidate_ids"))]
    if not ids:
        ids = [
            str(candidate_id)
            for candidate_id in _sequence(artifact.get("selected_candidate_ids"))
        ]
    return bool(selected_ids) and set(ids) == set(selected_ids)


def _pit_replay_evidence_section(
    pit_replay_evidence: Mapping[str, Any],
    source_2438: Mapping[str, Any],
) -> Mapping[str, Any]:
    section = _mapping(pit_replay_evidence.get("pit_replay_evidence"))
    if section:
        return section
    section = _mapping(source_2438.get("pit_replay_evidence"))
    if section:
        return section
    return pit_replay_evidence


def _pit_replay_blocker_section(
    pit_replay_blocker_summary: Mapping[str, Any],
    source_2438: Mapping[str, Any],
) -> Mapping[str, Any]:
    section = _mapping(pit_replay_blocker_summary.get("pit_replay_blocker_summary"))
    if section:
        return section
    section = _mapping(source_2438.get("pit_replay_blocker_summary"))
    if section:
        return section
    return pit_replay_blocker_summary


def _prior_2438a_blocked(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438A_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438A_ROUTE
    )


def _not_no_candidate(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438A_STATUS
        and payload.get("paper_shadow_candidate_found") is False
        and payload.get("not_no_candidate_status") is True
    )


def _blocker_count_before(prior_remediation: Mapping[str, Any]) -> int:
    value = prior_remediation.get("unresolved_engine_blocker_count")
    return _int_or_default(value, default=len(CORE_BLOCKERS))


def _int_or_default(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _before_after_matrix(
    status: str,
    prior_remediation: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    blocker_count_before: int,
    closed_blockers: Sequence[str],
    remaining_blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "before": {
            "prior_status": prior_remediation.get("status"),
            "prior_next_route": prior_remediation.get(
                "recommended_next_research_task"
            ),
            "prior_pit_replay_status": source_2438.get("status"),
            "blocker_count_before": blocker_count_before,
            "prior_remaining_blockers": list(
                prior_remediation.get("evidence_gap_ids") or CORE_BLOCKERS
            ),
        },
        "after": {
            "blocker_closure_ready": status == READY_STATUS,
            "blocker_count_after": len(remaining_blockers),
            "closed_blockers": list(closed_blockers),
            "remaining_blockers": list(remaining_blockers),
            "next_route": NEXT_ROUTE_READY if status == READY_STATUS else NEXT_ROUTE_BLOCKED,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _unresolved_blocker_summary(
    status: str,
    remaining_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": UNRESOLVED_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "unresolved_blocker_summary_ready": True,
        "unresolved_blocker_count": len(remaining_blockers),
        "remaining_blockers": list(remaining_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    blocker_id: str | None,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": dict(evidence),
        "production_effect": "none",
        "broker_action": "none",
    }
    if blocker_id is not None:
        payload["blocker_id"] = blocker_id
    return payload


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    gap = {
        "requirement_id": requirement.get("requirement_id"),
        "classification": requirement.get("classification"),
        "gap": f"{requirement.get('requirement_id')} did not pass.",
        "evidence": requirement.get("evidence", {}),
        "production_effect": "none",
        "broker_action": "none",
    }
    if requirement.get("blocker_id"):
        gap["blocker_id"] = requirement.get("blocker_id")
    return gap


def _check_passed(
    requirements: Sequence[Mapping[str, Any]],
    requirement_id: str,
) -> bool:
    return any(
        requirement.get("requirement_id") == requirement_id
        and requirement.get("status") == "PASS"
        for requirement in requirements
    )


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence) or isinstance(reports, (str, bytes)):
        reports = report_registry.get("report_registry")
    if isinstance(reports, Mapping):
        available = set(reports)
    else:
        available = {
            str(report.get("report_id"))
            for report in _sequence(reports)
            if isinstance(report, Mapping) and report.get("report_id")
        }
    return set(report_ids).issubset(available)


def _contains_all(text: str, needles: Sequence[str]) -> bool:
    return all(needle in text for needle in needles)


def _docs_cover_closure(text: str) -> bool:
    required_terms = ("PIT", "replay", "handoff")
    return _contains_all(text, required_terms)


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
