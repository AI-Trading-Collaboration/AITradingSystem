from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.v1"
STAGE_TRACE_SCHEMA_VERSION = "growth_tilt_candidate_runtime_stage_trace.v1"
METRIC_MATERIALIZATION_SCHEMA_VERSION = (
    "growth_tilt_candidate_runtime_metric_materialization.v1"
)
THRESHOLD_EVALUATION_SCHEMA_VERSION = (
    "growth_tilt_candidate_runtime_threshold_evaluations.v1"
)
BLOCKER_MATRIX_SCHEMA_VERSION = "growth_tilt_candidate_runtime_blocker_matrix.v1"
PROVENANCE_SCHEMA_VERSION = "growth_tilt_candidate_runtime_provenance.v1"

REPORT_TYPE = "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution"
READY_STATUS = "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_READY"
PARTIAL_STATUS = (
    "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_PARTIAL"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED"
)
EXPECTED_2438L_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED"
)

NEXT_ROUTE_PASS = "TRADING-2438N_Growth_Tilt_Top3_Candidate_PIT_Replay_Qualification_Rollup"
NEXT_ROUTE_ALL_FAIL = (
    "TRADING-2438N_Growth_Tilt_Top3_Candidate_PIT_Replay_Failure_Attribution_And_Pivot"
)
NEXT_ROUTE_RUNTIME_SPEC = (
    "TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval"
)
NEXT_ROUTE_SCOPED_REMEDIATION = (
    "TRADING-2438M1_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Scoped_Remediation"
)

# These metric identifiers are the existing 2438L runtime contract. They are
# identifiers only; this module does not define formulas, units, or thresholds.
REQUIRED_METRIC_IDS: tuple[str, ...] = (
    "return_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
    "turnover_delta_vs_baseline",
    "false_risk_off_delta",
    "missed_upside_delta",
    "whipsaw_delta",
)
STAGE_IDS: tuple[str, ...] = (
    "CANDIDATE_SELECTED",
    "RUNTIME_CONTRACT_RESOLVED",
    "RUNTIME_INPUT_HYDRATED",
    "REPLAY_RUNNER_INVOKED",
    "RAW_REPLAY_OUTPUT_EMITTED",
    "METRIC_DEPENDENCIES_RESOLVED",
    "RUNTIME_METRICS_COMPUTED",
    "RUNTIME_METRICS_NORMALIZED",
    "THRESHOLD_SPECS_RESOLVED",
    "THRESHOLDS_EVALUATED",
    "CANDIDATE_OUTCOME_RESOLVED",
    "ARTIFACT_PERSISTED",
    "ARTIFACT_RELOADED",
)
SUPPORTED_OPERATORS = {"GT", "GTE", "LT", "LTE", "EQ", "BETWEEN", "OUTSIDE"}

INPUT_CONTRACT_MISSING = "CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING"
RUNNER_NOT_INVOKED = "CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED"
RUNNER_FAILED = "CANDIDATE_RUNTIME_REPLAY_RUNNER_FAILED"
REPLAY_RESULT_EMPTY = "CANDIDATE_RUNTIME_REPLAY_RESULT_EMPTY"
METRIC_DEPENDENCY_UNRESOLVED = "CANDIDATE_RUNTIME_METRIC_DEPENDENCY_UNRESOLVED"
METRIC_CALCULATOR_NOT_INVOKED = "CANDIDATE_RUNTIME_METRIC_CALCULATOR_NOT_INVOKED"
METRIC_SOURCE_FIELD_MISSING = "CANDIDATE_RUNTIME_METRIC_SOURCE_FIELD_MISSING"
METRIC_SCHEMA_MISMATCH = "CANDIDATE_RUNTIME_METRIC_SCHEMA_MISMATCH"
METRIC_VALUE_NULL = "CANDIDATE_RUNTIME_METRIC_VALUE_NULL"
METRIC_VALUE_NON_FINITE = "CANDIDATE_RUNTIME_METRIC_VALUE_NON_FINITE"
METRIC_UNIT_MISMATCH = "CANDIDATE_RUNTIME_METRIC_UNIT_MISMATCH"
METRIC_NORMALIZATION_FAILED = "CANDIDATE_RUNTIME_METRIC_NORMALIZATION_FAILED"
REQUIRED_METRIC_INCOMPLETE = "CANDIDATE_RUNTIME_REQUIRED_METRIC_INCOMPLETE"
THRESHOLD_SPEC_MISSING = "CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING"
THRESHOLD_UNREGISTERED = "CANDIDATE_RUNTIME_THRESHOLD_UNREGISTERED"
THRESHOLD_METRIC_BINDING_MISSING = (
    "CANDIDATE_RUNTIME_THRESHOLD_METRIC_BINDING_MISSING"
)
THRESHOLD_EVALUATOR_NOT_INVOKED = (
    "CANDIDATE_RUNTIME_THRESHOLD_EVALUATOR_NOT_INVOKED"
)
THRESHOLD_EVALUATION_FAILED = "CANDIDATE_RUNTIME_THRESHOLD_EVALUATION_FAILED"
THRESHOLD_EVALUATION_OUTPUT_MISSING = (
    "CANDIDATE_RUNTIME_THRESHOLD_EVALUATION_OUTPUT_MISSING"
)
THRESHOLD_EVALUATION_BLOCKED_BY_METRIC = (
    "CANDIDATE_RUNTIME_THRESHOLD_EVALUATION_BLOCKED_BY_METRIC"
)
THRESHOLD_OPERATOR_UNSUPPORTED = "CANDIDATE_RUNTIME_THRESHOLD_OPERATOR_UNSUPPORTED"

REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "growth-tilt-post-runtime-candidate-pit-replay-blocker-resolution",
    "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.json",
    "growth_tilt_candidate_runtime_stage_trace.json",
    "growth_tilt_candidate_runtime_metric_materialization.json",
    "growth_tilt_candidate_runtime_threshold_evaluations.json",
    "growth_tilt_candidate_runtime_blocker_matrix.json",
    "growth_tilt_candidate_runtime_provenance.json",
)
REQUIRED_FLOW_REFERENCES: tuple[str, ...] = (
    "TRADING-2438M",
    READY_STATUS,
    PARTIAL_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_RUNTIME_SPEC,
)


def build_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
    source_2438l_recheck: Mapping[str, Any],
    candidate_config: Mapping[str, Any],
    engine_contract: Mapping[str, Any],
    runtime_evaluation_inputs: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
    candidate_limit: int = 3,
    source_run_id: str | None = None,
) -> dict[str, Any]:
    candidate_ids = _candidate_ids(source_2438l_recheck)
    configured_candidates = _records_by_id(_sequence(candidate_config.get("candidates")))
    prior_decisions = _prior_decisions(source_2438l_recheck)
    candidate_specs = _records_by_id(
        _sequence(runtime_evaluation_inputs.get("candidate_specs"))
    )
    raw_outputs = _records_by_id(
        _sequence(runtime_evaluation_inputs.get("raw_runtime_outputs"))
    )
    metric_specs = _sequence(runtime_evaluation_inputs.get("metric_specs"))
    threshold_specs = _sequence(runtime_evaluation_inputs.get("threshold_specs"))

    strict_errors = _strict_validation_errors(
        source_2438l_recheck,
        candidate_ids,
        runtime_evaluation_inputs,
        metric_specs,
        threshold_specs,
        candidate_limit=candidate_limit,
        source_run_id=source_run_id,
    )
    results = [
        _evaluate_candidate(
            candidate_id=candidate_id,
            source_rank=index,
            configured_candidate=configured_candidates.get(candidate_id, {}),
            prior_decision=prior_decisions.get(candidate_id, {}),
            candidate_spec=candidate_specs.get(candidate_id, {}),
            raw_runtime_output=raw_outputs.get(candidate_id, {}),
            metric_specs=metric_specs,
            threshold_specs=threshold_specs,
            engine_contract=engine_contract,
            as_of=as_of,
        )
        for index, candidate_id in enumerate(candidate_ids, start=1)
    ]

    pass_count = _status_count(results, "PASS")
    fail_count = _status_count(results, "FAIL")
    blocked_count = _status_count(results, "BLOCKED")
    status = _overall_status(results, blocked_count)
    all_blockers = [
        dict(blocker)
        for result in results
        for blocker in _sequence(result.get("blockers"))
        if isinstance(blocker, Mapping)
    ]
    next_route = _next_route(
        blocked_count=blocked_count,
        pass_count=pass_count,
        fail_count=fail_count,
        blockers=all_blockers,
    )
    metrics = [
        dict(metric)
        for result in results
        for metric in _sequence(result.get("runtime_metrics"))
        if isinstance(metric, Mapping)
    ]
    evaluations = [
        dict(evaluation)
        for result in results
        for evaluation in _sequence(result.get("threshold_evaluations"))
        if isinstance(evaluation, Mapping)
    ]
    traces = [
        {
            "candidate_id": result.get("candidate_id"),
            "source_rank": result.get("source_rank"),
            "first_failed_stage": result.get("first_failed_stage"),
            "stage_trace": result.get("stage_trace"),
        }
        for result in results
    ]
    blockers_by_code = dict(
        sorted(Counter(str(item.get("blocker_code")) for item in all_blockers).items())
    )
    documentation_alignment = _documentation_alignment(
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    requirements = _requirements(
        source_2438l_recheck,
        candidate_ids,
        data_quality_summary,
        documentation_alignment,
        candidate_limit=candidate_limit,
    )
    gaps = [item for item in requirements if item["status"] != "PASS"]
    computed_metrics = [item for item in metrics if item.get("compute_status") == "PASS"]
    null_metrics = [
        item
        for item in metrics
        if item.get("raw_value") is None or item.get("normalized_value") is None
    ]
    invalid_metrics = [
        item
        for item in metrics
        if item.get("compute_status") == "BLOCKED"
        and any(
            code
            in {
                METRIC_SCHEMA_MISMATCH,
                METRIC_VALUE_NON_FINITE,
                METRIC_UNIT_MISMATCH,
                METRIC_NORMALIZATION_FAILED,
            }
            for code in _sequence(item.get("blocker_codes"))
        )
    ]
    completed_thresholds = [
        item
        for item in evaluations
        if item.get("evaluation_status") in {"PASS", "FAIL"}
    ]
    missing_thresholds = [
        item for item in evaluations if item.get("evaluation_status") == "BLOCKED"
    ]
    safety = _safety()
    run_id = f"growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution:{as_of}"

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M",
        "report_type": REPORT_TYPE,
        "status": status,
        "readiness_status": status,
        "run_id": run_id,
        "source_run_id": source_run_id or source_2438l_recheck.get("run_id"),
        "as_of_date": as_of,
        "source_artifacts": [dict(item) for item in source_artifacts],
        "candidate_count": len(results),
        "top3_candidate_ids": candidate_ids,
        "candidate_replay_outcome_rechecked": (
            source_2438l_recheck.get("candidate_replay_outcome_rechecked") is True
        ),
        "runtime_invoked_candidate_count": sum(
            result.get("runtime_invoked") is True for result in results
        ),
        "raw_runtime_output_present_count": sum(
            _mapping(result.get("raw_runtime_output")).get("present") is True
            for result in results
        ),
        "required_runtime_metric_count": len(metrics),
        "computed_runtime_metric_count": len(computed_metrics),
        "null_runtime_metric_count": len(null_metrics),
        "invalid_runtime_metric_count": len(invalid_metrics),
        "required_threshold_evaluation_count": len(evaluations),
        "completed_threshold_evaluation_count": len(completed_thresholds),
        "missing_threshold_evaluation_count": len(missing_thresholds),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "blocked_count": blocked_count,
        "resolved_blocker_count": 0 if blocked_count else len(all_blockers),
        "unresolved_blocker_count": len(all_blockers) if blocked_count else 0,
        "blockers_by_code": blockers_by_code,
        "candidate_results": results,
        "recommended_next_research_task": next_route,
        "next_route": next_route,
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "documentation_alignment": documentation_alignment,
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        ),
        "data_quality_gate_passed": data_quality_summary.get(
            "data_quality_gate_passed"
        ),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get(
            "data_quality_report_path"
        ),
        "growth_tilt_candidate_runtime_stage_trace": {
            "schema_version": STAGE_TRACE_SCHEMA_VERSION,
            "status": status,
            "candidate_traces": traces,
        },
        "growth_tilt_candidate_runtime_metric_materialization": {
            "schema_version": METRIC_MATERIALIZATION_SCHEMA_VERSION,
            "status": status,
            "required_metric_count": len(metrics),
            "computed_metric_count": len(computed_metrics),
            "null_metric_count": len(null_metrics),
            "invalid_metric_count": len(invalid_metrics),
            "metrics": metrics,
        },
        "growth_tilt_candidate_runtime_threshold_evaluations": {
            "schema_version": THRESHOLD_EVALUATION_SCHEMA_VERSION,
            "status": status,
            "required_evaluation_count": len(evaluations),
            "completed_evaluation_count": len(completed_thresholds),
            "missing_evaluation_count": len(missing_thresholds),
            "threshold_evaluations": evaluations,
        },
        "growth_tilt_candidate_runtime_blocker_matrix": {
            "schema_version": BLOCKER_MATRIX_SCHEMA_VERSION,
            "status": status,
            "blocker_count": len(all_blockers),
            "blockers_by_code": blockers_by_code,
            "blockers": all_blockers,
        },
        "growth_tilt_candidate_runtime_provenance": {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "status": status,
            "run_id": run_id,
            "as_of_date": as_of,
            "source_artifacts": [dict(item) for item in source_artifacts],
            "candidate_ids": candidate_ids,
            "source_2438l_schema_version": source_2438l_recheck.get("schema_version"),
            "candidate_config_schema_version": candidate_config.get("schema_version"),
            "engine_contract_schema_version": engine_contract.get("schema_version"),
        },
        "safety": safety,
        **safety,
    }


def _evaluate_candidate(
    *,
    candidate_id: str,
    source_rank: int,
    configured_candidate: Mapping[str, Any],
    prior_decision: Mapping[str, Any],
    candidate_spec: Mapping[str, Any],
    raw_runtime_output: Mapping[str, Any],
    metric_specs: Sequence[Any],
    threshold_specs: Sequence[Any],
    engine_contract: Mapping[str, Any],
    as_of: str,
) -> dict[str, Any]:
    blockers: list[dict[str, Any]] = []
    selected = configured_candidate.get("candidate_id") == candidate_id
    contract_ready = _runtime_contract_ready(prior_decision, engine_contract)
    input_ready = _candidate_spec_ready(candidate_spec)
    runtime_invoked = raw_runtime_output.get("producer_invoked") is True
    raw_output_present = _raw_output_present(raw_runtime_output)
    candidate_metric_specs = _metric_specs_for_candidate(metric_specs, candidate_id)
    metric_spec_ids = {
        str(item.get("metric_id"))
        for item in candidate_metric_specs
        if isinstance(item, Mapping) and item.get("metric_id")
    }
    metric_dependencies_ready = set(REQUIRED_METRIC_IDS).issubset(metric_spec_ids)

    if not input_ready:
        blockers.append(
            _blocker(
                INPUT_CONTRACT_MISSING,
                candidate_id,
                "RUNTIME_INPUT_HYDRATED",
                "runtime_contract.parameters",
                "owner-approved executable candidate parameters and engine mapping",
                candidate_spec or configured_candidate,
                "candidate config / runtime evaluation input",
                "Define and approve a parameterized candidate spec; do not infer "
                "it from the candidate name or rationale.",
            )
        )
    if not runtime_invoked:
        blockers.append(
            _blocker(
                RUNNER_NOT_INVOKED,
                candidate_id,
                "REPLAY_RUNNER_INVOKED",
                "raw_runtime_output.producer_invoked",
                True,
                raw_runtime_output.get("producer_invoked"),
                "runtime evaluation input",
                "Bind the approved candidate spec to an actual compute-plane replay runner.",
            )
        )
    elif raw_runtime_output.get("execution_status") == "FAIL":
        blockers.append(
            _blocker(
                RUNNER_FAILED,
                candidate_id,
                "REPLAY_RUNNER_INVOKED",
                "raw_runtime_output.execution_status",
                "PASS",
                raw_runtime_output.get("execution_status"),
                str(raw_runtime_output.get("artifact_path") or "runtime evaluation input"),
                "Investigate the replay runner exception without substituting a prior run.",
            )
        )
    if runtime_invoked and not raw_output_present:
        blockers.append(
            _blocker(
                REPLAY_RESULT_EMPTY,
                candidate_id,
                "RAW_REPLAY_OUTPUT_EMITTED",
                "raw_runtime_output.metrics",
                "non-empty current-run output with run/schema/hash provenance",
                raw_runtime_output,
                str(raw_runtime_output.get("artifact_path") or "runtime evaluation input"),
                "Persist the current-run raw replay output before metric materialization.",
            )
        )
    if not metric_dependencies_ready:
        blockers.append(
            _blocker(
                METRIC_DEPENDENCY_UNRESOLVED,
                candidate_id,
                "METRIC_DEPENDENCIES_RESOLVED",
                "metric_specs",
                list(REQUIRED_METRIC_IDS),
                sorted(metric_spec_ids),
                "runtime evaluation input",
                "Register source-field, unit, normalization, and calculator "
                "provenance for every required metric.",
            )
        )

    metrics: list[dict[str, Any]] = []
    for metric_id in REQUIRED_METRIC_IDS:
        spec = next(
            (
                item
                for item in candidate_metric_specs
                if isinstance(item, Mapping) and item.get("metric_id") == metric_id
            ),
            {},
        )
        metric, metric_blockers = _materialize_metric(
            candidate_id,
            metric_id,
            _mapping(spec),
            raw_runtime_output,
        )
        metrics.append(metric)
        blockers.extend(metric_blockers)

    candidate_threshold_specs = _threshold_specs_for_candidate(
        threshold_specs, candidate_id
    )
    threshold_specs_ready = bool(candidate_threshold_specs) and all(
        _threshold_spec_ready(_mapping(item)) for item in candidate_threshold_specs
    )
    if not candidate_threshold_specs:
        blockers.append(
            _blocker(
                THRESHOLD_SPEC_MISSING,
                candidate_id,
                "THRESHOLD_SPECS_RESOLVED",
                "threshold_specs",
                "reviewed threshold policy with metric binding and version",
                configured_candidate.get("threshold_value"),
                "candidate config / runtime evaluation input",
                "Provide an owner-reviewed threshold policy under heuristic governance.",
            )
        )
        blockers.append(
            _blocker(
                THRESHOLD_EVALUATOR_NOT_INVOKED,
                candidate_id,
                "THRESHOLDS_EVALUATED",
                "threshold_evaluation_output",
                "current-run threshold evaluator output",
                None,
                "runtime evaluation input",
                "Invoke the registered evaluator only after a reviewed threshold spec "
                "and finite runtime metric map are available.",
            )
        )
        evaluations = [_missing_threshold_evaluation(candidate_id)]
    else:
        evaluations = []
        metric_by_id = {str(item["metric_id"]): item for item in metrics}
        for item in candidate_threshold_specs:
            evaluation, evaluation_blockers = _evaluate_threshold(
                candidate_id,
                _mapping(item),
                metric_by_id,
            )
            evaluations.append(evaluation)
            blockers.extend(evaluation_blockers)

    metrics_ready = all(item.get("compute_status") == "PASS" for item in metrics)
    thresholds_evaluated = bool(evaluations) and all(
        item.get("evaluation_status") in {"PASS", "FAIL"} for item in evaluations
    )
    candidate_status = _candidate_status(
        selected=selected,
        contract_ready=contract_ready,
        input_ready=input_ready,
        raw_output_present=raw_output_present,
        metrics_ready=metrics_ready,
        threshold_specs_ready=threshold_specs_ready,
        thresholds_evaluated=thresholds_evaluated,
        evaluations=evaluations,
    )
    trace = _stage_trace(
        candidate_id=candidate_id,
        selected=selected,
        contract_ready=contract_ready,
        input_ready=input_ready,
        runtime_invoked=runtime_invoked,
        raw_output_present=raw_output_present,
        metric_dependencies_ready=metric_dependencies_ready,
        metrics_ready=metrics_ready,
        threshold_specs_ready=threshold_specs_ready,
        thresholds_evaluated=thresholds_evaluated,
        candidate_status=candidate_status,
        blockers=blockers,
        as_of=as_of,
    )
    first_failed_stage = next(
        (
            str(item.get("stage_id"))
            for item in trace
            if item.get("status") in {"FAIL", "BLOCKED"}
        ),
        None,
    )
    return {
        "candidate_id": candidate_id,
        "source_rank": source_rank,
        "as_of_date": as_of,
        "runtime_contract": {
            "executable": contract_ready,
            "executor_id": candidate_spec.get("executor_id"),
            "executor_version": candidate_spec.get("executor_version"),
            "input_contract_version": candidate_spec.get("input_contract_version"),
        },
        "runtime_invoked": runtime_invoked,
        "raw_runtime_output": {
            "present": raw_output_present,
            "artifact_path": raw_runtime_output.get("artifact_path"),
            "schema_version": raw_runtime_output.get("schema_version"),
            "output_hash": raw_runtime_output.get("output_hash"),
            "run_id": raw_runtime_output.get("run_id"),
        },
        "runtime_metrics": metrics,
        "threshold_evaluations": evaluations,
        "candidate_replay_outcome": {
            "status": candidate_status,
            "pass_reason_codes": (
                ["ALL_REQUIRED_RUNTIME_THRESHOLDS_PASSED"]
                if candidate_status == "PASS"
                else []
            ),
            "fail_reason_codes": [
                str(item.get("threshold_id"))
                for item in evaluations
                if item.get("evaluation_status") == "FAIL"
            ],
            "blocker_codes": sorted(
                {str(item.get("blocker_code")) for item in blockers}
            ),
        },
        "first_failed_stage": first_failed_stage,
        "stage_trace": trace,
        "blockers": blockers,
        "safety": _safety(),
    }


def _materialize_metric(
    candidate_id: str,
    metric_id: str,
    metric_spec: Mapping[str, Any],
    raw_runtime_output: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    raw_metrics = _mapping(raw_runtime_output.get("metrics"))
    source = _mapping(raw_metrics.get(metric_id))
    if not metric_spec:
        code = METRIC_DEPENDENCY_UNRESOLVED
        blockers.append(
            _blocker(
                code,
                candidate_id,
                "METRIC_DEPENDENCIES_RESOLVED",
                f"metric_specs.{metric_id}",
                "registered metric spec",
                None,
                "runtime evaluation input",
                "Register source field, unit, normalization, and calculator provenance.",
            )
        )
    elif not source:
        code = (
            METRIC_CALCULATOR_NOT_INVOKED
            if raw_runtime_output.get("producer_invoked") is not True
            else METRIC_SOURCE_FIELD_MISSING
        )
        blockers.append(
            _blocker(
                code,
                candidate_id,
                "RUNTIME_METRICS_COMPUTED",
                f"raw_runtime_output.metrics.{metric_id}",
                "current-run calculator output record",
                None,
                str(raw_runtime_output.get("artifact_path") or "runtime evaluation input"),
                "Invoke the registered calculator and preserve its output record.",
            )
        )
    raw_value = source.get("raw_value") if source else None
    normalized_value = source.get("normalized_value") if source else None
    expected_unit = metric_spec.get("unit")
    observed_unit = source.get("unit") if source else None
    if source:
        if raw_value is None:
            blockers.append(
                _metric_value_blocker(
                    METRIC_VALUE_NULL, candidate_id, metric_id, raw_value, raw_runtime_output
                )
            )
        elif not _is_finite_number(raw_value):
            blockers.append(
                _metric_value_blocker(
                    METRIC_VALUE_NON_FINITE,
                    candidate_id,
                    metric_id,
                    raw_value,
                    raw_runtime_output,
                )
            )
        if expected_unit is None or observed_unit != expected_unit:
            blockers.append(
                _blocker(
                    METRIC_UNIT_MISMATCH,
                    candidate_id,
                    "RUNTIME_METRICS_NORMALIZED",
                    f"raw_runtime_output.metrics.{metric_id}.unit",
                    expected_unit,
                    observed_unit,
                    str(raw_runtime_output.get("artifact_path") or "runtime evaluation input"),
                    "Use the governed metric unit; do not silently rescale an unknown unit.",
                )
            )
        if not source.get("calculator_id") or not source.get("calculator_version"):
            blockers.append(
                _blocker(
                    METRIC_SCHEMA_MISMATCH,
                    candidate_id,
                    "RUNTIME_METRICS_COMPUTED",
                    f"raw_runtime_output.metrics.{metric_id}.calculator_provenance",
                    "calculator_id and calculator_version",
                    {
                        "calculator_id": source.get("calculator_id"),
                        "calculator_version": source.get("calculator_version"),
                    },
                    str(raw_runtime_output.get("artifact_path") or "runtime evaluation input"),
                    "Emit calculator provenance with the current-run metric.",
                )
            )
        normalization = metric_spec.get("normalization")
        if _is_finite_number(raw_value):
            if normalization == "identity" and normalized_value is None:
                normalized_value = raw_value
            if not _is_finite_number(normalized_value):
                blockers.append(
                    _blocker(
                        METRIC_NORMALIZATION_FAILED,
                        candidate_id,
                        "RUNTIME_METRICS_NORMALIZED",
                        f"raw_runtime_output.metrics.{metric_id}.normalized_value",
                        "finite normalized runtime value",
                        normalized_value,
                        str(
                            raw_runtime_output.get("artifact_path")
                            or "runtime evaluation input"
                        ),
                        "Apply only the registered normalization rule and preserve "
                        "its provenance.",
                    )
                )
    blocker_codes = sorted({str(item["blocker_code"]) for item in blockers})
    return (
        {
            "candidate_id": candidate_id,
            "metric_id": metric_id,
            "required": True,
            "source_field": metric_spec.get("source_field"),
            "raw_value": raw_value,
            "normalized_value": normalized_value,
            "normalization_applied": bool(source and normalized_value != raw_value),
            "normalization_rule_id": metric_spec.get("normalization"),
            "unit": observed_unit,
            "compute_status": "PASS" if not blockers else "BLOCKED",
            "calculator_id": source.get("calculator_id") if source else None,
            "calculator_version": source.get("calculator_version") if source else None,
            "blocker_codes": blocker_codes,
        },
        blockers,
    )


def _evaluate_threshold(
    candidate_id: str,
    spec: Mapping[str, Any],
    metric_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    threshold_id = spec.get("threshold_id")
    metric_id = str(spec.get("metric_id") or "")
    operator = str(spec.get("operator") or "").upper()
    metric = _mapping(metric_by_id.get(metric_id))
    metric_value = metric.get("normalized_value")
    threshold_value = spec.get("threshold_value")
    if not threshold_id or not spec.get("policy_ref") or not spec.get("policy_version"):
        blockers.append(
            _blocker(
                THRESHOLD_UNREGISTERED,
                candidate_id,
                "THRESHOLD_SPECS_RESOLVED",
                "threshold_specs.registration",
                "threshold_id, policy_ref, and policy_version",
                spec,
                "runtime evaluation input",
                "Register the threshold in a reviewed policy manifest.",
            )
        )
    if operator not in SUPPORTED_OPERATORS:
        blockers.append(
            _blocker(
                THRESHOLD_OPERATOR_UNSUPPORTED,
                candidate_id,
                "THRESHOLD_SPECS_RESOLVED",
                f"threshold_specs.{threshold_id}.operator",
                sorted(SUPPORTED_OPERATORS),
                operator,
                "runtime evaluation input",
                "Use an explicitly supported operator and add focused boundary tests.",
            )
        )
    if not metric or metric.get("compute_status") != "PASS":
        blockers.append(
            _blocker(
                THRESHOLD_EVALUATION_BLOCKED_BY_METRIC,
                candidate_id,
                "THRESHOLDS_EVALUATED",
                f"runtime_metrics.{metric_id}",
                "finite normalized runtime metric",
                metric_value,
                "metric materialization output",
                "Resolve the metric blocker before threshold evaluation.",
            )
        )
    if not _threshold_value_ready(operator, threshold_value):
        blockers.append(
            _blocker(
                THRESHOLD_SPEC_MISSING,
                candidate_id,
                "THRESHOLD_SPECS_RESOLVED",
                f"threshold_specs.{threshold_id}.threshold_value",
                "finite governed threshold value",
                threshold_value,
                "runtime evaluation input",
                "Provide a reviewed threshold value; do not infer one from current metrics.",
            )
        )
    evaluation_status = "BLOCKED"
    if not blockers:
        try:
            passed = _compare(operator, float(metric_value), threshold_value)
            evaluation_status = "PASS" if passed else "FAIL"
        except (TypeError, ValueError, ArithmeticError):
            blockers.append(
                _blocker(
                    THRESHOLD_EVALUATION_FAILED,
                    candidate_id,
                    "THRESHOLDS_EVALUATED",
                    f"threshold_evaluations.{threshold_id}",
                    "deterministic comparison output",
                    None,
                    "threshold evaluator",
                    "Investigate the evaluator exception; do not swallow it or infer PASS.",
                )
            )
    return (
        {
            "candidate_id": candidate_id,
            "threshold_id": threshold_id,
            "metric_id": metric_id or None,
            "operator": operator or None,
            "threshold_value": threshold_value,
            "metric_value": metric_value,
            "evaluation_status": evaluation_status,
            "evaluator_id": "growth_tilt_runtime_threshold_evaluator",
            "evaluator_version": "v1",
            "policy_ref": spec.get("policy_ref"),
            "policy_version": spec.get("policy_version"),
            "blocker_codes": sorted(
                {str(item.get("blocker_code")) for item in blockers}
            ),
        },
        blockers,
    )


def _missing_threshold_evaluation(candidate_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "threshold_id": None,
        "metric_id": None,
        "operator": None,
        "threshold_value": None,
        "metric_value": None,
        "evaluation_status": "BLOCKED",
        "evaluator_id": None,
        "evaluator_version": None,
        "policy_ref": None,
        "policy_version": None,
        "blocker_codes": [THRESHOLD_SPEC_MISSING, THRESHOLD_EVALUATOR_NOT_INVOKED],
    }


def _stage_trace(
    *,
    candidate_id: str,
    selected: bool,
    contract_ready: bool,
    input_ready: bool,
    runtime_invoked: bool,
    raw_output_present: bool,
    metric_dependencies_ready: bool,
    metrics_ready: bool,
    threshold_specs_ready: bool,
    thresholds_evaluated: bool,
    candidate_status: str,
    blockers: Sequence[Mapping[str, Any]],
    as_of: str,
) -> list[dict[str, Any]]:
    states = {
        "CANDIDATE_SELECTED": "PASS" if selected else "BLOCKED",
        "RUNTIME_CONTRACT_RESOLVED": "PASS" if contract_ready else "BLOCKED",
        "RUNTIME_INPUT_HYDRATED": "PASS" if input_ready else "BLOCKED",
        "REPLAY_RUNNER_INVOKED": (
            "PASS" if runtime_invoked else ("BLOCKED" if input_ready else "NOT_STARTED")
        ),
        "RAW_REPLAY_OUTPUT_EMITTED": (
            "PASS" if raw_output_present else ("BLOCKED" if runtime_invoked else "NOT_STARTED")
        ),
        "METRIC_DEPENDENCIES_RESOLVED": (
            "PASS" if metric_dependencies_ready else "BLOCKED"
        ),
        "RUNTIME_METRICS_COMPUTED": (
            "PASS" if metrics_ready else ("BLOCKED" if raw_output_present else "NOT_STARTED")
        ),
        "RUNTIME_METRICS_NORMALIZED": (
            "PASS" if metrics_ready else ("BLOCKED" if raw_output_present else "NOT_STARTED")
        ),
        "THRESHOLD_SPECS_RESOLVED": (
            "PASS" if threshold_specs_ready else "BLOCKED"
        ),
        "THRESHOLDS_EVALUATED": (
            "PASS"
            if thresholds_evaluated
            else ("BLOCKED" if threshold_specs_ready else "NOT_STARTED")
        ),
        "CANDIDATE_OUTCOME_RESOLVED": "PASS",
        "ARTIFACT_PERSISTED": "PASS" if raw_output_present else "NOT_STARTED",
        "ARTIFACT_RELOADED": "PASS" if raw_output_present else "NOT_STARTED",
    }
    return [
        {
            "stage_id": stage_id,
            "status": states[stage_id],
            "producer": _stage_producer(stage_id),
            "input_refs": [candidate_id, as_of],
            "output_refs": [],
            "started_at": None,
            "completed_at": None,
            "blocker_codes": sorted(
                {
                    str(item.get("blocker_code"))
                    for item in blockers
                    if item.get("stage_id") == stage_id
                }
            ),
            "details": {
                "candidate_outcome": candidate_status
                if stage_id == "CANDIDATE_OUTCOME_RESOLVED"
                else None,
                "persistence_scope": "runtime_compute_artifact"
                if stage_id in {"ARTIFACT_PERSISTED", "ARTIFACT_RELOADED"}
                else None,
            },
        }
        for stage_id in STAGE_IDS
    ]


def _candidate_status(
    *,
    selected: bool,
    contract_ready: bool,
    input_ready: bool,
    raw_output_present: bool,
    metrics_ready: bool,
    threshold_specs_ready: bool,
    thresholds_evaluated: bool,
    evaluations: Sequence[Mapping[str, Any]],
) -> str:
    if not all(
        (
            selected,
            contract_ready,
            input_ready,
            raw_output_present,
            metrics_ready,
            threshold_specs_ready,
            thresholds_evaluated,
        )
    ):
        return "BLOCKED"
    if any(item.get("evaluation_status") == "FAIL" for item in evaluations):
        return "FAIL"
    return "PASS"


def _compare(operator: str, metric_value: float, threshold_value: object) -> bool:
    if operator == "GT":
        return metric_value > float(threshold_value)
    if operator == "GTE":
        return metric_value >= float(threshold_value)
    if operator == "LT":
        return metric_value < float(threshold_value)
    if operator == "LTE":
        return metric_value <= float(threshold_value)
    if operator == "EQ":
        return metric_value == float(threshold_value)
    bounds = _sequence(threshold_value)
    lower, upper = float(bounds[0]), float(bounds[1])
    if operator == "BETWEEN":
        return lower <= metric_value <= upper
    if operator == "OUTSIDE":
        return metric_value < lower or metric_value > upper
    raise ValueError(f"Unsupported threshold operator: {operator}")


def _threshold_value_ready(operator: str, value: object) -> bool:
    if operator in {"BETWEEN", "OUTSIDE"}:
        bounds = _sequence(value)
        return len(bounds) == 2 and all(_is_finite_number(item) for item in bounds)
    return _is_finite_number(value)


def _threshold_spec_ready(spec: Mapping[str, Any]) -> bool:
    return bool(
        spec.get("threshold_id")
        and spec.get("metric_id") in REQUIRED_METRIC_IDS
        and str(spec.get("operator") or "").upper() in SUPPORTED_OPERATORS
        and _threshold_value_ready(
            str(spec.get("operator") or "").upper(), spec.get("threshold_value")
        )
        and spec.get("policy_ref")
        and spec.get("policy_version")
    )


def _candidate_spec_ready(spec: Mapping[str, Any]) -> bool:
    parameters = spec.get("parameters")
    return bool(
        spec.get("candidate_id")
        and spec.get("approved") is True
        and spec.get("executor_id")
        and spec.get("executor_version")
        and spec.get("input_contract_version")
        and spec.get("source_policy_ref")
        and isinstance(parameters, Mapping)
        and parameters
    )


def _raw_output_present(output: Mapping[str, Any]) -> bool:
    return bool(
        output.get("producer_invoked") is True
        and output.get("execution_status") == "PASS"
        and output.get("run_id")
        and output.get("schema_version")
        and output.get("output_hash")
        and _mapping(output.get("metrics"))
    )


def _runtime_contract_ready(
    prior_decision: Mapping[str, Any], engine_contract: Mapping[str, Any]
) -> bool:
    contract = _mapping(engine_contract.get("pit_replay_engine_contract"))
    return bool(
        prior_decision.get("runtime_executable") is True
        and contract.get("engine_entrypoint_exists") is True
        and contract.get("status") == "READY"
    )


def _metric_specs_for_candidate(
    specs: Sequence[Any], candidate_id: str
) -> list[Mapping[str, Any]]:
    return [
        item
        for item in specs
        if isinstance(item, Mapping)
        and item.get("candidate_id") in {None, "*", candidate_id}
    ]


def _threshold_specs_for_candidate(
    specs: Sequence[Any], candidate_id: str
) -> list[Mapping[str, Any]]:
    return [
        item
        for item in specs
        if isinstance(item, Mapping) and item.get("candidate_id") in {"*", candidate_id}
    ]


def _prior_decisions(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    section = _mapping(payload.get("candidate_pass_fail_blocked_decision_matrix"))
    return _records_by_id(_sequence(section.get("decisions") or section.get("rows")))


def _candidate_ids(payload: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in _sequence(payload.get("top3_candidate_ids")) if item]


def _records_by_id(records: Sequence[Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(item.get("candidate_id")): item
        for item in records
        if isinstance(item, Mapping) and item.get("candidate_id")
    }


def _strict_validation_errors(
    source: Mapping[str, Any],
    candidate_ids: Sequence[str],
    runtime_inputs: Mapping[str, Any],
    metric_specs: Sequence[Any],
    threshold_specs: Sequence[Any],
    *,
    candidate_limit: int,
    source_run_id: str | None,
) -> list[str]:
    errors: list[str] = []
    if source.get("schema_version") != (
        "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation.v1"
    ):
        errors.append("source_2438l_schema_version_mismatch")
    if source.get("status") != EXPECTED_2438L_STATUS:
        errors.append("source_2438l_status_mismatch")
    if len(candidate_ids) != candidate_limit or len(set(candidate_ids)) != candidate_limit:
        errors.append("candidate_identity_or_count_drift")
    declared_ids = [
        str(item) for item in _sequence(runtime_inputs.get("candidate_ids")) if item
    ]
    if declared_ids and declared_ids != list(candidate_ids):
        errors.append("runtime_input_candidate_identity_or_order_drift")
    if source_run_id and source.get("run_id") != source_run_id:
        errors.append("source_run_id_mismatch")
    unknown_metrics = {
        str(item.get("metric_id"))
        for item in metric_specs
        if isinstance(item, Mapping)
        and item.get("metric_id")
        and item.get("metric_id") not in REQUIRED_METRIC_IDS
    }
    if unknown_metrics:
        errors.append(f"unknown_required_metric:{','.join(sorted(unknown_metrics))}")
    unsupported = {
        str(item.get("operator"))
        for item in threshold_specs
        if isinstance(item, Mapping)
        and str(item.get("operator") or "").upper() not in SUPPORTED_OPERATORS
    }
    if unsupported:
        errors.append(f"unsupported_threshold_operator:{','.join(sorted(unsupported))}")
    return errors


def _overall_status(results: Sequence[Mapping[str, Any]], blocked_count: int) -> str:
    if not results or blocked_count == len(results):
        return BLOCKED_STATUS
    if blocked_count:
        return PARTIAL_STATUS
    return READY_STATUS


def _next_route(
    *,
    blocked_count: int,
    pass_count: int,
    fail_count: int,
    blockers: Sequence[Mapping[str, Any]],
) -> str:
    if blocked_count == 0 and pass_count:
        return NEXT_ROUTE_PASS
    if blocked_count == 0 and fail_count:
        return NEXT_ROUTE_ALL_FAIL
    codes = {str(item.get("blocker_code")) for item in blockers}
    if INPUT_CONTRACT_MISSING in codes or THRESHOLD_SPEC_MISSING in codes:
        return NEXT_ROUTE_RUNTIME_SPEC
    return NEXT_ROUTE_SCOPED_REMEDIATION


def _documentation_alignment(
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> dict[str, bool]:
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    return {
        "report_registry": REPORT_TYPE in report_ids,
        "artifact_catalog": all(
            item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES
        ),
        "system_flow": all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES),
        "requirement_doc": "TRADING-2438M" in requirement_text
        and INPUT_CONTRACT_MISSING in requirement_text,
    }


def _requirements(
    source: Mapping[str, Any],
    candidate_ids: Sequence[str],
    data_quality: Mapping[str, Any],
    documentation_alignment: Mapping[str, bool],
    *,
    candidate_limit: int,
) -> list[dict[str, Any]]:
    checks = (
        (
            "source_2438l_blocked_recheck_ready",
            source.get("status") == EXPECTED_2438L_STATUS
            and source.get("candidate_replay_outcome_rechecked") is True,
        ),
        (
            "top3_candidate_identity_ready",
            len(candidate_ids) == candidate_limit
            and len(set(candidate_ids)) == candidate_limit,
        ),
        (
            "data_quality_gate_passed",
            data_quality.get("data_quality_gate_executed") is True
            and data_quality.get("data_quality_gate_passed") is True,
        ),
        ("registry_catalog_docs_alignment", all(documentation_alignment.values())),
    )
    return [
        {
            "requirement_id": requirement_id,
            "status": "PASS" if passed else "FAIL",
            "classification": "source_or_governance_gap",
        }
        for requirement_id, passed in checks
    ]


def _blocker(
    code: str,
    candidate_id: str,
    stage_id: str,
    field_path: str,
    expected: object,
    observed: object,
    source_artifact: str,
    recommended_repair: str,
) -> dict[str, Any]:
    return {
        "blocker_code": code,
        "category": _blocker_category(code),
        "severity": "ERROR",
        "candidate_id": candidate_id,
        "stage_id": stage_id,
        "field_path": field_path,
        "expected": expected,
        "observed": observed,
        "source_artifact": source_artifact,
        "repairable": True,
        "recommended_repair": recommended_repair,
    }


def _metric_value_blocker(
    code: str,
    candidate_id: str,
    metric_id: str,
    observed: object,
    raw_output: Mapping[str, Any],
) -> dict[str, Any]:
    return _blocker(
        code,
        candidate_id,
        "RUNTIME_METRICS_COMPUTED",
        f"raw_runtime_output.metrics.{metric_id}.raw_value",
        "finite calculator-emitted numeric value",
        observed,
        str(raw_output.get("artifact_path") or "runtime evaluation input"),
        "Repair the calculator/source mapping; never replace a missing value with zero.",
    )


def _blocker_category(code: str) -> str:
    if "THRESHOLD" in code:
        return "threshold_evaluation"
    if "METRIC" in code:
        return "metric_computation"
    if "ARTIFACT" in code:
        return "persistence_reload"
    return "runtime_input_invocation"


def _stage_producer(stage_id: str) -> str:
    if stage_id in {"REPLAY_RUNNER_INVOKED", "RAW_REPLAY_OUTPUT_EMITTED"}:
        return "compute_plane_replay_runner"
    if stage_id in {
        "METRIC_DEPENDENCIES_RESOLVED",
        "RUNTIME_METRICS_COMPUTED",
        "RUNTIME_METRICS_NORMALIZED",
    }:
        return "compute_plane_metric_materializer"
    if stage_id in {"THRESHOLD_SPECS_RESOLVED", "THRESHOLDS_EVALUATED"}:
        return "runtime_threshold_evaluator"
    return "growth_tilt_post_runtime_resolution"


def _status_count(results: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(
        _mapping(item.get("candidate_replay_outcome")).get("status") == status
        for item in results
    )


def _safety() -> dict[str, Any]:
    return {
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
