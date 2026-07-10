from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_baseline_capability_graph.v1"
NODE_SCHEMA_VERSION = "growth_tilt_baseline_capability_node.v1"
EDGE_SCHEMA_VERSION = "growth_tilt_baseline_capability_edge.v1"

REPORT_TYPE = "growth_tilt_baseline_capability_graph"
READY_ZERO_STATUS = (
    "GROWTH_TILT_BASELINE_CAPABILITY_GRAPH_READY_NO_MUTATION_READY_CAPABILITY"
)
READY_AVAILABLE_STATUS = (
    "GROWTH_TILT_BASELINE_CAPABILITY_GRAPH_READY_MUTATION_CAPABILITIES_AVAILABLE"
)
BLOCKED_STATUS = "GROWTH_TILT_BASELINE_CAPABILITY_GRAPH_BLOCKED_SOURCE_CONTRACT"
NEXT_ROUTE_AVAILABLE = "TRADING-2438N3_GROWTH_TILT_CONTRACT_FIRST_CANDIDATE_GENERATOR"
NEXT_ROUTE_ZERO = "TRADING-2438N3_NOT_STARTED_NO_MUTATION_READY_CAPABILITY"

EXPECTED_CONFIG_SCHEMA = "growth_tilt_baseline_capability_graph_config.v1"
EXPECTED_CLOSURE_SCHEMA = "growth_tilt_candidate_family_closure.v1"
EXPECTED_CLOSURE_STATUS = (
    "GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE"
)
EXPECTED_ADAPTER_SCHEMA = "growth_tilt_baseline_contract_adapters_readiness.v1"
EXPECTED_SIGNAL_INVENTORY_SCHEMA = "growth_tilt_baseline_signal_inventory.v1"
EXPECTED_BASE_POLICY_SCHEMA = "base_overlay_veto_policy_schema.v1"
EXPECTED_RISK_VETO_SCHEMA = "risk_on_veto_policy.v1"
EXPECTED_METRIC_SCHEMA = "growth_tilt_candidate_replay_metric_contract.v1"
EXPECTED_SCREENING_SCHEMA = "growth_tilt_candidate_pit_screening_policy.v1"

ALLOWED_TYPES = (
    "SIGNAL",
    "DECISION_REQUEST",
    "PERSISTENCE_RULE",
    "HARD_VETO",
    "REGIME_TRANSITION",
    "EXPOSURE_SCALAR",
    "EXPOSURE_CAP",
    "RAMP_RULE",
    "COOLDOWN_RULE",
    "EXPIRY_RULE",
    "METRIC",
    "REPLAY_RUNNER",
)
ALLOWED_STATUSES = ("READY", "BLOCKED", "DIAGNOSTIC_ONLY", "NOT_APPLICABLE")
ALLOWED_RELATIONS = (
    "PRODUCES",
    "CONSUMED_BY",
    "GUARDED_BY",
    "REQUESTS",
    "APPLIES",
    "CAPS",
    "MEASURED_BY",
    "REPLAYED_BY",
    "SUPERSEDES",
)
EXPECTED_NODE_IDS = (
    "signal_re_risk_allowed_probability",
    "signal_growth_allowed",
    "signal_first_layer_trend_state",
    "hard_veto_risk_off",
    "hard_veto_volatility",
    "hard_veto_event_risk",
    "hard_veto_trend_break",
    "hard_veto_tqqq",
    "hard_veto_aggregate",
    "decision_request_non_hard_defensive",
    "persistence_recovery",
    "transition_regime_current_requested_applied",
    "exposure_scalar_native",
    "exposure_cap_qqq_equivalent",
    "exposure_cap_turnover",
    "ramp_rule_recovery",
    "cooldown_rule_transition",
    "expiry_rule_signal_validity",
    "metric_contract_six_runtime_metrics",
    "replay_runner_candidate_overlay_executor",
    "replay_runner_growth_tilt_pit",
)
EXPECTED_MUTATION_READY_COUNT = 0

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-baseline-capability-graph",
    "growth_tilt_baseline_capability_graph.yaml",
    "growth_tilt_baseline_capability_graph.json",
    "growth_tilt_baseline_capability_graph.md",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438N2",
    READY_ZERO_STATUS,
    "mutation-ready capability=0",
    "callable-but-unconsumed",
    "TRADING-2438N3_NOT_STARTED",
)


def build_growth_tilt_baseline_capability_graph(
    sources: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    as_of: str,
) -> dict[str, Any]:
    config = _mapping(sources.get("graph_config"))
    raw_nodes = [
        item for item in _sequence(config.get("nodes")) if isinstance(item, Mapping)
    ]
    raw_edges = [
        item for item in _sequence(config.get("edges")) if isinstance(item, Mapping)
    ]
    base_readiness = {
        str(item.get("capability_id")): _mapping(item.get("readiness")).get("status")
        for item in raw_nodes
    }
    nodes = [
        _evaluate_node(item, base_readiness, _mapping(config.get("mutation_ready_gate")))
        for item in raw_nodes
    ]
    node_by_id = {str(item.get("capability_id")): item for item in nodes}
    edges = [_evaluate_edge(item, node_by_id) for item in raw_edges]
    strict_errors = _strict_validation_errors(
        sources,
        config,
        nodes,
        edges,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    mutation_ready_ids = [
        str(item["capability_id"]) for item in nodes if item["mutation_ready"] is True
    ]
    status_counts = {
        status: sum(item["readiness"]["status"] == status for item in nodes)
        for status in ALLOWED_STATUSES
    }
    if strict_errors:
        status = BLOCKED_STATUS
    elif mutation_ready_ids:
        status = READY_AVAILABLE_STATUS
    else:
        status = READY_ZERO_STATUS
    n3_allowed = bool(mutation_ready_ids) and not strict_errors
    next_route = NEXT_ROUTE_AVAILABLE if n3_allowed else NEXT_ROUTE_ZERO
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438N2",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "graph_id": config.get("graph_id"),
        "source_artifacts": [dict(item) for item in source_artifacts],
        "node_schema_version": NODE_SCHEMA_VERSION,
        "edge_schema_version": EDGE_SCHEMA_VERSION,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
        "readiness_status_counts": status_counts,
        "ready_capability_count": status_counts["READY"],
        "blocked_capability_count": status_counts["BLOCKED"],
        "diagnostic_only_capability_count": status_counts["DIAGNOSTIC_ONLY"],
        "not_applicable_capability_count": status_counts["NOT_APPLICABLE"],
        "mutation_ready_capability_count": len(mutation_ready_ids),
        "mutation_ready_capability_ids": mutation_ready_ids,
        "callable_but_unconsumed_capability_ids": [
            str(item["capability_id"])
            for item in nodes
            if item["runtime"]["callable"] is True
            and item["runtime"]["consumed_by_baseline"] is not True
        ],
        "n3_candidate_generation_allowed": n3_allowed,
        "n3_status": (
            "READY_TO_START" if n3_allowed else "NOT_STARTED_NO_MUTATION_READY_CAPABILITY"
        ),
        "n4_status": "NOT_STARTED_NO_CONTRACT_READY_CANDIDATE",
        "new_baseline_behavior_required_for_current_blockers": True,
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": next_route,
        "next_route": next_route,
        "data_quality_gate_executed": False,
        "data_quality_status": "NOT_APPLICABLE_CODE_CONFIG_CONTRACT_AUDIT_ONLY",
        **_safety(),
    }


def _evaluate_node(
    node: Mapping[str, Any],
    readiness_by_id: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    runtime = dict(_mapping(node.get("runtime")))
    pit = dict(_mapping(node.get("pit")))
    governance = dict(_mapping(node.get("governance")))
    replay = dict(_mapping(node.get("replay")))
    readiness = dict(_mapping(node.get("readiness")))
    dependencies = dict(_mapping(node.get("mutation_dependencies")))
    blockers: list[str] = []
    if readiness.get("status") != "READY":
        blockers.append(f"CAPABILITY_STATUS_NOT_READY:{readiness.get('status')}")
    if runtime.get("callable") is not gate.get("callable"):
        blockers.append("RUNTIME_NOT_CALLABLE")
    if runtime.get("consumed_by_baseline") is not gate.get("consumed_by_baseline"):
        blockers.append("NOT_CONSUMED_BY_BASELINE")
    if runtime.get("deterministic") is not True:
        blockers.append("RUNTIME_NOT_DETERMINISTIC")
    if not str(pit.get("lineage_status") or "").startswith("VALID"):
        blockers.append("PIT_LINEAGE_NOT_VALID")
    if governance.get("semantics_approved") is not gate.get("semantics_approved"):
        blockers.append("SEMANTICS_NOT_APPROVED")
    if not pit.get("missing_policy"):
        blockers.append("MISSING_POLICY_NOT_EXPLICIT")
    if not replay.get("runner_binding"):
        blockers.append("RUNNER_BINDING_MISSING")
    mutable_dimensions = [
        str(item) for item in _sequence(governance.get("mutable_dimensions")) if item
    ]
    minimum_mutable = int(gate.get("mutable_dimension_count_minimum") or 1)
    if len(mutable_dimensions) < minimum_mutable:
        blockers.append("NO_APPROVED_MUTABLE_DIMENSION")
    dependency_resolution: list[dict[str, Any]] = []
    for group in ("hard_veto_ids", "transition_ids", "exposure_ids"):
        for dependency_id in _sequence(dependencies.get(group)):
            dependency_status = readiness_by_id.get(str(dependency_id))
            ready = dependency_status == gate.get("required_dependency_status")
            dependency_resolution.append(
                {
                    "dependency_group": group,
                    "capability_id": dependency_id,
                    "status": dependency_status,
                    "ready": ready,
                }
            )
            if not ready:
                blockers.append(f"DEPENDENCY_NOT_READY:{dependency_id}")
    blockers = sorted(set(blockers))
    return {
        "schema_version": NODE_SCHEMA_VERSION,
        "capability_id": node.get("capability_id"),
        "capability_type": node.get("capability_type"),
        "runtime": runtime,
        "pit": pit,
        "governance": governance,
        "replay": replay,
        "mutation_dependencies": dependencies,
        "dependency_resolution": dependency_resolution,
        "readiness": readiness,
        "capability_contract_ready": _capability_contract_ready(
            runtime, pit, governance, readiness
        ),
        "mutation_ready": not blockers,
        "mutation_blocker_codes": blockers,
    }


def _capability_contract_ready(
    runtime: Mapping[str, Any],
    pit: Mapping[str, Any],
    governance: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> bool:
    return bool(
        readiness.get("status") == "READY"
        and runtime.get("callable") is True
        and runtime.get("deterministic") is True
        and str(pit.get("lineage_status") or "").startswith("VALID")
        and pit.get("missing_policy")
        and governance.get("semantics_approved") is True
    )


def _evaluate_edge(
    edge: Mapping[str, Any], node_by_id: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    from_id = str(edge.get("from_capability") or "")
    to_id = str(edge.get("to_capability") or "")
    blockers: list[str] = []
    if from_id not in node_by_id:
        blockers.append("FROM_CAPABILITY_MISSING")
    if to_id not in node_by_id:
        blockers.append("TO_CAPABILITY_MISSING")
    if edge.get("relation") not in ALLOWED_RELATIONS:
        blockers.append("RELATION_INVALID")
    if edge.get("runtime_resolvable") is not True:
        blockers.append("EDGE_RUNTIME_NOT_RESOLVABLE")
    if edge.get("pit_valid") is not True:
        blockers.append("EDGE_PIT_INVALID")
    return {
        "schema_version": EDGE_SCHEMA_VERSION,
        "from_capability": from_id,
        "to_capability": to_id,
        "relation": edge.get("relation"),
        "runtime_resolvable": edge.get("runtime_resolvable"),
        "pit_valid": edge.get("pit_valid"),
        "edge_ready": not blockers,
        "blocker_codes": blockers,
    }


def _strict_validation_errors(
    sources: Mapping[str, Any],
    config: Mapping[str, Any],
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    closure = _mapping(sources.get("closure"))
    adapters = _mapping(sources.get("adapters"))
    signal_inventory = _unwrap_section(
        sources.get("signal_inventory"), "baseline_signal_inventory"
    )
    base_policy = _mapping(sources.get("base_policy"))
    risk_veto_policy = _mapping(sources.get("risk_veto_policy"))
    metric_contract = _mapping(sources.get("metric_contract"))
    screening_policy = _mapping(sources.get("screening_policy"))
    expected = (
        (
            config.get("schema_version"),
            EXPECTED_CONFIG_SCHEMA,
            "graph_config_schema_mismatch",
        ),
        (
            closure.get("schema_version"),
            EXPECTED_CLOSURE_SCHEMA,
            "closure_schema_mismatch",
        ),
        (
            closure.get("status"),
            EXPECTED_CLOSURE_STATUS,
            "closure_status_mismatch",
        ),
        (
            adapters.get("schema_version"),
            EXPECTED_ADAPTER_SCHEMA,
            "adapter_schema_mismatch",
        ),
        (
            signal_inventory.get("schema_version"),
            EXPECTED_SIGNAL_INVENTORY_SCHEMA,
            "signal_inventory_schema_mismatch",
        ),
        (
            base_policy.get("schema_version"),
            EXPECTED_BASE_POLICY_SCHEMA,
            "base_policy_schema_mismatch",
        ),
        (
            risk_veto_policy.get("schema_version"),
            EXPECTED_RISK_VETO_SCHEMA,
            "risk_veto_policy_schema_mismatch",
        ),
        (
            metric_contract.get("schema_version"),
            EXPECTED_METRIC_SCHEMA,
            "metric_contract_schema_mismatch",
        ),
        (
            screening_policy.get("schema_version"),
            EXPECTED_SCREENING_SCHEMA,
            "screening_policy_schema_mismatch",
        ),
    )
    for actual, wanted, code in expected:
        if actual != wanted:
            errors.append(code)
    node_ids = [str(item.get("capability_id")) for item in nodes]
    if tuple(node_ids) != EXPECTED_NODE_IDS:
        errors.append("capability_identity_or_order_mismatch")
    if len(node_ids) != len(set(node_ids)):
        errors.append("duplicate_capability_id")
    if any(item.get("capability_type") not in ALLOWED_TYPES for item in nodes):
        errors.append("capability_type_invalid")
    if any(
        _mapping(item.get("readiness")).get("status") not in ALLOWED_STATUSES
        for item in nodes
    ):
        errors.append("capability_readiness_status_invalid")
    if any(
        item.get("readiness", {}).get("status") == "READY"
        and item.get("capability_contract_ready") is not True
        for item in nodes
    ):
        errors.append("ready_capability_contract_incomplete")
    if sum(item.get("mutation_ready") is True for item in nodes) != (
        EXPECTED_MUTATION_READY_COUNT
    ):
        errors.append("unexpected_mutation_ready_capability_count")
    if any(item.get("relation") not in ALLOWED_RELATIONS for item in edges):
        errors.append("edge_relation_invalid")
    edge_keys = [
        (
            item.get("from_capability"),
            item.get("to_capability"),
            item.get("relation"),
        )
        for item in edges
    ]
    if len(edge_keys) != len(set(edge_keys)):
        errors.append("duplicate_edge")
    node_id_set = set(node_ids)
    if any(
        item.get("from_capability") not in node_id_set
        or item.get("to_capability") not in node_id_set
        for item in edges
    ):
        errors.append("edge_capability_reference_unresolved")
    if adapters.get("adapter_contract_ready_count") != 0:
        errors.append("adapter_readiness_drift")
    if metric_contract.get("status") != "PENDING_OWNER_PREREGISTRATION":
        errors.append("metric_contract_status_drift")
    if screening_policy.get("policy_status") != "PENDING_OWNER_PREREGISTRATION":
        errors.append("screening_policy_status_drift")
    compiler_code = str(sources.get("compiler_code_text") or "")
    executor_code = str(sources.get("executor_code_text") or "")
    if "QQQ_equivalent_exposure_max" not in compiler_code or "3.0" not in compiler_code:
        errors.append("qqq_equivalent_cap_code_evidence_missing")
    if "turnover_max" not in compiler_code:
        errors.append("turnover_cap_code_evidence_missing")
    if "class GrowthTiltCandidateOverlayExecutor" not in executor_code:
        errors.append("candidate_overlay_executor_code_evidence_missing")
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    if REPORT_TYPE not in report_ids:
        errors.append("report_registry_alignment_failed")
    if not all(item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES):
        errors.append("artifact_catalog_alignment_failed")
    if not all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES):
        errors.append("system_flow_alignment_failed")
    if not all(
        item in requirement_text
        for item in (
            "TRADING-2438N2",
            "Callable 但未被 baseline consumed 的 producer 绝不是 mutation-ready",
            "N2 不得新增 transition",
            "mutation_ready_capability_count > 0",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _safety() -> dict[str, Any]:
    return {
        "read_only": True,
        "runtime_code_invoked": False,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "new_signal_integrated": False,
        "new_transition_created": False,
        "new_persistence_created": False,
        "new_veto_created": False,
        "new_exposure_unit_created": False,
        "threshold_values_changed": False,
        "baseline_behavior_changed": False,
        "candidate_generation_run": False,
        "candidate_count": 0,
        "promotion_allowed": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _unwrap_section(value: Any, section_name: str) -> Mapping[str, Any]:
    root = _mapping(value)
    return _mapping(root.get(section_name)) or root


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
