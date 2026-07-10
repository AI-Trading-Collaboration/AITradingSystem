from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ai_trading_system import (
    dynamic_strategy_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as impl,
)
from ai_trading_system.research_quality import (
    growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as resolution,
)

CANDIDATE_IDS = [
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
]


def test_real_baseline_is_precisely_blocked_at_input_hydration() -> None:
    payload = _build({})

    assert payload["status"] == resolution.BLOCKED_STATUS
    assert (payload["pass_count"], payload["fail_count"], payload["blocked_count"]) == (
        0,
        0,
        3,
    )
    assert payload["computed_runtime_metric_count"] == 0
    assert payload["null_runtime_metric_count"] == 18
    assert payload["missing_threshold_evaluation_count"] == 3
    assert {
        item["first_failed_stage"] for item in payload["candidate_results"]
    } == {"RUNTIME_INPUT_HYDRATED"}
    assert resolution.INPUT_CONTRACT_MISSING in payload["blockers_by_code"]
    assert resolution.THRESHOLD_SPEC_MISSING in payload["blockers_by_code"]


def test_runtime_executable_does_not_imply_metric_computed() -> None:
    payload = _build({})

    assert all(
        item["runtime_contract"]["executable"] is True
        for item in payload["candidate_results"]
    )
    assert payload["computed_runtime_metric_count"] == 0


def test_recheck_true_does_not_imply_threshold_evaluated() -> None:
    payload = _build({})

    assert payload["candidate_replay_outcome_rechecked"] is True
    assert payload["completed_threshold_evaluation_count"] == 0


def test_static_threshold_contract_does_not_count_as_runtime_evaluation() -> None:
    payload = _build({})

    evaluations = payload[
        "growth_tilt_candidate_runtime_threshold_evaluations"
    ]["threshold_evaluations"]
    assert {item["evaluation_status"] for item in evaluations} == {"BLOCKED"}
    assert all(item["threshold_value"] is None for item in evaluations)


def test_complete_runtime_outputs_can_resolve_all_candidates_to_pass() -> None:
    payload = _build(_runtime_inputs())

    assert payload["status"] == resolution.READY_STATUS
    assert (payload["pass_count"], payload["fail_count"], payload["blocked_count"]) == (
        3,
        0,
        0,
    )
    assert payload["computed_runtime_metric_count"] == 18
    assert payload["null_runtime_metric_count"] == 0
    assert payload["completed_threshold_evaluation_count"] == 3
    assert payload["recommended_next_research_task"] == resolution.NEXT_ROUTE_PASS


def test_all_computed_threshold_failures_are_resolution_ready() -> None:
    inputs = _runtime_inputs(return_value=-1.0)
    payload = _build(inputs)

    assert payload["status"] == resolution.READY_STATUS
    assert (payload["pass_count"], payload["fail_count"], payload["blocked_count"]) == (
        0,
        3,
        0,
    )
    assert payload["recommended_next_research_task"] == resolution.NEXT_ROUTE_ALL_FAIL


def test_candidate_failure_is_isolated_and_partial_status_is_explicit() -> None:
    inputs = _runtime_inputs()
    inputs["candidate_specs"] = inputs["candidate_specs"][:-1]
    payload = _build(inputs)

    assert payload["status"] == resolution.PARTIAL_STATUS
    assert (payload["pass_count"], payload["fail_count"], payload["blocked_count"]) == (
        2,
        0,
        1,
    )


def test_blocked_has_priority_over_an_explicit_threshold_failure() -> None:
    inputs = _runtime_inputs(return_value=-1.0)
    del inputs["raw_runtime_outputs"][0]["metrics"]["whipsaw_delta"]
    payload = _build(inputs)

    assert payload["candidate_results"][0]["candidate_replay_outcome"]["status"] == (
        "BLOCKED"
    )


def test_calculator_emitted_zero_is_preserved_as_valid() -> None:
    payload = _build(_runtime_inputs(return_value=0.0))
    metric = _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert metric["raw_value"] == 0.0
    assert metric["normalized_value"] == 0.0
    assert metric["compute_status"] == "PASS"


def test_valid_negative_metric_is_preserved() -> None:
    payload = _build(_runtime_inputs(return_value=-0.25, operator="LTE", threshold=0.0))
    metric = _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert metric["normalized_value"] == -0.25
    assert payload["candidate_results"][0]["candidate_replay_outcome"]["status"] == "PASS"


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), float("-inf")])
def test_null_nan_and_infinite_metrics_remain_blocked(invalid: float | None) -> None:
    inputs = _runtime_inputs()
    record = inputs["raw_runtime_outputs"][0]["metrics"]["return_delta_vs_baseline"]
    record["raw_value"] = invalid
    record["normalized_value"] = invalid
    payload = _build(inputs)
    metric = _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert metric["compute_status"] == "BLOCKED"
    assert payload["candidate_results"][0]["candidate_replay_outcome"]["status"] == (
        "BLOCKED"
    )


def test_default_zero_does_not_resolve_missing_metric() -> None:
    inputs = _runtime_inputs()
    del inputs["raw_runtime_outputs"][0]["metrics"]["return_delta_vs_baseline"]
    payload = _build(inputs)
    metric = _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert metric["raw_value"] is None
    assert metric["normalized_value"] is None
    assert metric["compute_status"] == "BLOCKED"


def test_previous_run_metric_cannot_fill_current_run() -> None:
    source = _source()
    source["candidate_pass_fail_blocked_decision_matrix"]["decisions"][0][
        "metric_summary"
    ] = {metric_id: 9.0 for metric_id in resolution.REQUIRED_METRIC_IDS}
    payload = _build({}, source=source)

    assert _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")[
        "raw_value"
    ] is None


def test_metric_unit_mismatch_is_blocked() -> None:
    inputs = _runtime_inputs()
    inputs["raw_runtime_outputs"][0]["metrics"]["return_delta_vs_baseline"][
        "unit"
    ] = "percent"
    payload = _build(inputs)
    metric = _metric(payload, CANDIDATE_IDS[0], "return_delta_vs_baseline")

    assert resolution.METRIC_UNIT_MISMATCH in metric["blocker_codes"]


def test_metric_calculator_provenance_is_required() -> None:
    inputs = _runtime_inputs()
    del inputs["raw_runtime_outputs"][0]["metrics"]["return_delta_vs_baseline"][
        "calculator_id"
    ]
    payload = _build(inputs)

    assert resolution.METRIC_SCHEMA_MISMATCH in _metric(
        payload, CANDIDATE_IDS[0], "return_delta_vs_baseline"
    )["blocker_codes"]


@pytest.mark.parametrize(
    ("operator", "metric_value", "threshold", "expected"),
    [
        ("GT", 1.0, 0.0, "PASS"),
        ("GTE", 0.0, 0.0, "PASS"),
        ("LT", -1.0, 0.0, "PASS"),
        ("LTE", 0.0, 0.0, "PASS"),
        ("EQ", 0.0, 0.0, "PASS"),
        ("BETWEEN", 0.0, [-1.0, 1.0], "PASS"),
        ("OUTSIDE", 2.0, [-1.0, 1.0], "PASS"),
    ],
)
def test_threshold_operator_and_equality_boundaries_are_explicit(
    operator: str,
    metric_value: float,
    threshold: float | list[float],
    expected: str,
) -> None:
    payload = _build(
        _runtime_inputs(
            return_value=metric_value,
            operator=operator,
            threshold=threshold,
        )
    )
    evaluation = payload[
        "growth_tilt_candidate_runtime_threshold_evaluations"
    ]["threshold_evaluations"][0]

    assert evaluation["evaluation_status"] == expected


def test_unknown_metric_and_operator_fail_strict_validation() -> None:
    inputs = _runtime_inputs(operator="APPROX")
    inputs["metric_specs"].append(
        {
            "metric_id": "unknown_metric",
            "source_field": "unknown_metric",
            "unit": "decimal_delta",
            "normalization": "identity",
        }
    )
    payload = _build(inputs)

    assert any(
        item.startswith("unknown_required_metric")
        for item in payload["strict_validation_errors"]
    )
    assert any(
        item.startswith("unsupported_threshold_operator")
        for item in payload["strict_validation_errors"]
    )


def test_candidate_identity_and_order_drift_fails_closed() -> None:
    inputs = _runtime_inputs()
    inputs["candidate_ids"] = list(reversed(CANDIDATE_IDS))
    payload = _build(inputs)

    assert "runtime_input_candidate_identity_or_order_drift" in payload[
        "strict_validation_errors"
    ]


def test_stage_trace_is_complete_and_preserves_first_failed_stage() -> None:
    result = _build({})["candidate_results"][0]

    assert [item["stage_id"] for item in result["stage_trace"]] == list(
        resolution.STAGE_IDS
    )
    assert result["first_failed_stage"] == "RUNTIME_INPUT_HYDRATED"


def test_lineage_hashes_and_safety_boundary_are_preserved() -> None:
    source_artifacts = [
        {"path": "source.json", "sha256": "abc", "schema_version": "v1"}
    ]
    payload = _build({}, source_artifacts=source_artifacts)

    assert payload["source_artifacts"] == source_artifacts
    assert payload["observe_only"] is True
    assert payload["candidate_only"] is True
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_dynamic_runner_writes_primary_and_supporting_artifacts(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path)
    output_root = tmp_path / "outputs"
    docs_root = tmp_path / "docs"

    payload = impl.run_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
        source_2438l_path=paths["source"],
        candidate_config_path=paths["candidate_config"],
        engine_contract_path=paths["engine"],
        runtime_evaluation_input_path=paths["runtime_inputs"],
        requirement_doc_path=paths["requirement"],
        report_registry_path=paths["registry"],
        artifact_catalog_path=paths["catalog"],
        system_flow_path=paths["flow"],
        data_quality_summary_path=paths["data_quality"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
        strict=True,
    )

    assert payload["status"] == resolution.READY_STATUS
    assert (
        output_root
        / "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.json"
    ).exists()
    assert (output_root / "growth_tilt_candidate_runtime_stage_trace.json").exists()
    assert (
        output_root / "growth_tilt_candidate_runtime_metric_materialization.json"
    ).exists()
    assert (
        output_root / "growth_tilt_candidate_runtime_threshold_evaluations.json"
    ).exists()
    assert (output_root / "growth_tilt_candidate_runtime_blocker_matrix.json").exists()
    assert (output_root / "growth_tilt_candidate_runtime_provenance.json").exists()
    assert (
        docs_root / "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.md"
    ).exists()


def _build(
    runtime_inputs: dict[str, object],
    *,
    source: dict[str, object] | None = None,
    source_artifacts: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return resolution.build_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
        source or _source(),
        _candidate_config(),
        _engine_contract(),
        runtime_inputs,
        _data_quality(),
        source_artifacts=source_artifacts or [],
        report_registry={"reports": [{"report_id": resolution.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(resolution.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(resolution.REQUIRED_FLOW_REFERENCES),
        requirement_text=(
            f"TRADING-2438M {resolution.INPUT_CONTRACT_MISSING}"
        ),
        as_of="2026-07-08",
    )


def _source() -> dict[str, object]:
    return {
        "schema_version": (
            "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation.v1"
        ),
        "status": resolution.EXPECTED_2438L_STATUS,
        "candidate_replay_outcome_rechecked": True,
        "top3_candidate_ids": list(CANDIDATE_IDS),
        "candidate_pass_fail_blocked_decision_matrix": {
            "decisions": [
                {
                    "candidate_id": candidate_id,
                    "runtime_executable": True,
                    "metric_summary": {
                        metric_id: None for metric_id in resolution.REQUIRED_METRIC_IDS
                    },
                }
                for candidate_id in CANDIDATE_IDS
            ]
        },
    }


def _candidate_config() -> dict[str, object]:
    return {
        "schema_version": "growth_tilt_false_risk_off_missed_upside_candidate_set.v1",
        "candidates": [
            {
                "candidate_id": candidate_id,
                "candidate_family": "fixture_family",
                "threshold_source": "future_policy_required",
                "threshold_value": None,
            }
            for candidate_id in CANDIDATE_IDS
        ],
    }


def _engine_contract() -> dict[str, object]:
    return {
        "schema_version": "growth_tilt_pit_replay_engine_closure_contract.v1",
        "pit_replay_engine_contract": {
            "status": "READY",
            "engine_entrypoint_exists": True,
        },
    }


def _runtime_inputs(
    *,
    return_value: float = 1.0,
    operator: str = "GTE",
    threshold: float | list[float] = 0.0,
) -> dict[str, object]:
    metric_specs = [
        {
            "metric_id": metric_id,
            "source_field": metric_id,
            "unit": "decimal_delta",
            "normalization": "identity",
        }
        for metric_id in resolution.REQUIRED_METRIC_IDS
    ]
    outputs = []
    for candidate_id in CANDIDATE_IDS:
        values = {
            metric_id: {
                "raw_value": return_value if metric_id == "return_delta_vs_baseline" else 0.1,
                "normalized_value": (
                    return_value if metric_id == "return_delta_vs_baseline" else 0.1
                ),
                "unit": "decimal_delta",
                "calculator_id": "fixture_runtime_calculator",
                "calculator_version": "v1",
            }
            for metric_id in resolution.REQUIRED_METRIC_IDS
        }
        outputs.append(
            {
                "candidate_id": candidate_id,
                "producer_invoked": True,
                "execution_status": "PASS",
                "run_id": f"run:{candidate_id}",
                "schema_version": "fixture_runtime_output.v1",
                "output_hash": f"hash:{candidate_id}",
                "artifact_path": f"runtime/{candidate_id}.json",
                "metrics": values,
            }
        )
    return {
        "candidate_ids": list(CANDIDATE_IDS),
        "candidate_specs": [
            {
                "candidate_id": candidate_id,
                "approved": True,
                "executor_id": "fixture_executor",
                "executor_version": "v1",
                "input_contract_version": "v1",
                "source_policy_ref": "fixture_policy",
                "parameters": {"fixture_parameter": 1},
            }
            for candidate_id in CANDIDATE_IDS
        ],
        "raw_runtime_outputs": outputs,
        "metric_specs": metric_specs,
        "threshold_specs": [
            {
                "candidate_id": candidate_id,
                "threshold_id": f"threshold:{candidate_id}",
                "metric_id": "return_delta_vs_baseline",
                "operator": operator,
                "threshold_value": threshold,
                "policy_ref": "fixture_threshold_policy",
                "policy_version": "v1",
            }
            for candidate_id in CANDIDATE_IDS
        ],
    }


def _data_quality() -> dict[str, object]:
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS",
        "data_quality_report_path": "quality.json",
    }


def _metric(
    payload: dict[str, object], candidate_id: str, metric_id: str
) -> dict[str, object]:
    section = payload["growth_tilt_candidate_runtime_metric_materialization"]
    assert isinstance(section, dict)
    return next(
        item
        for item in section["metrics"]
        if item["candidate_id"] == candidate_id and item["metric_id"] == metric_id
    )


def _runner_sources(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "source": tmp_path / "source.json",
        "candidate_config": tmp_path / "candidate_config.yaml",
        "engine": tmp_path / "engine.json",
        "runtime_inputs": tmp_path / "runtime_inputs.json",
        "requirement": tmp_path / "requirement.md",
        "registry": tmp_path / "registry.yaml",
        "catalog": tmp_path / "catalog.md",
        "flow": tmp_path / "flow.md",
        "data_quality": tmp_path / "data_quality.json",
    }
    paths["source"].write_text(json.dumps(_source()), encoding="utf-8")
    paths["candidate_config"].write_text(
        json.dumps(_candidate_config()), encoding="utf-8"
    )
    paths["engine"].write_text(json.dumps(_engine_contract()), encoding="utf-8")
    paths["runtime_inputs"].write_text(
        json.dumps(_runtime_inputs()), encoding="utf-8"
    )
    paths["requirement"].write_text(
        f"TRADING-2438M {resolution.INPUT_CONTRACT_MISSING}", encoding="utf-8"
    )
    paths["registry"].write_text(
        json.dumps({"reports": [{"report_id": resolution.REPORT_TYPE}]}),
        encoding="utf-8",
    )
    paths["catalog"].write_text(
        "\n".join(resolution.REQUIRED_CATALOG_REFERENCES), encoding="utf-8"
    )
    paths["flow"].write_text(
        "\n".join(resolution.REQUIRED_FLOW_REFERENCES), encoding="utf-8"
    )
    paths["data_quality"].write_text(json.dumps(_data_quality()), encoding="utf-8")
    return paths
