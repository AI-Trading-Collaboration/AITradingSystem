from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION,
    OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION,
    OPERATIONS_SCHEDULE_SCHEMA_VERSION,
    ETFOperationsScheduleConfig,
    OperationsCommandGraphError,
    build_biweekly_operations_command_graph,
    build_daily_operations_command_graph,
    build_monthly_operations_command_graph,
    build_weekly_operations_command_graph,
    check_operations_artifact_freshness,
    evaluate_operations_failure_policy,
    load_operations_schedule_config,
    operations_schedule_required_step_ids,
    operations_schedule_step_ids,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_operations_schedule_config_loads_default() -> None:
    config = load_operations_schedule_config()

    assert config.schema_version == OPERATIONS_SCHEDULE_SCHEMA_VERSION
    assert config.policy_metadata.version == "etf_operations_schedule_v0_1"
    assert config.safety.observe_only is True
    assert config.safety.candidate_only is True
    assert config.safety.production_effect == "none"
    assert config.safety.broker_action == "none"
    assert config.safety.manual_review_required is True


def test_operations_schedule_includes_required_cadence_sections() -> None:
    config = load_operations_schedule_config()

    assert config.daily_pipeline
    assert config.weekly_pipeline
    assert config.biweekly_pipeline
    assert config.monthly_pipeline
    assert config.manual_review_steps
    assert all(step.cadence == "daily" for step in config.daily_pipeline)
    assert all(step.cadence == "weekly" for step in config.weekly_pipeline)
    assert all(step.cadence == "biweekly" for step in config.biweekly_pipeline)
    assert all(step.cadence == "monthly" for step in config.monthly_pipeline)


def test_operations_schedule_daily_required_nodes_exist() -> None:
    config = load_operations_schedule_config()

    daily_ids = {step.step_id for step in config.daily_pipeline}

    assert {
        "data_freshness_check",
        "etf_daily_run",
        "forward_update",
        "ai_confirmation_run",
        "satellite_replacement_run",
        "ai_attribution_update",
        "satellite_attribution_update",
        "reader_brief_generate",
        "report_registry_update",
        "operations_health_check",
    }.issubset(daily_ids)


def test_operations_schedule_step_ids_are_unique() -> None:
    config = load_operations_schedule_config()
    step_ids = operations_schedule_step_ids(config)

    assert len(step_ids) == len(set(step_ids))


def test_operations_schedule_commands_are_non_empty() -> None:
    config = load_operations_schedule_config()

    assert all(step.command.strip() for step in config.steps())


def test_operations_schedule_dependencies_reference_valid_step_ids() -> None:
    config = load_operations_schedule_config()
    step_ids = set(operations_schedule_step_ids(config))

    for step in config.steps():
        assert set(step.dependencies).issubset(step_ids), step.step_id


def test_operations_schedule_required_steps_have_expected_outputs() -> None:
    config = load_operations_schedule_config()

    required_ids = operations_schedule_required_step_ids(config)
    assert "data_freshness_check" in required_ids
    for step in config.steps():
        if step.required:
            assert step.expected_outputs, step.step_id


def test_operations_schedule_rejects_duplicate_step_ids() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][1]["step_id"] = "data_freshness_check"

    with pytest.raises(ValueError, match="step IDs must be unique"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_missing_dependency() -> None:
    raw = _raw_schedule()
    raw["weekly_pipeline"][0]["dependencies"] = ["missing_daily_gate"]

    with pytest.raises(ValueError, match="dependencies reference unknown steps"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_required_step_without_outputs() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["expected_outputs"] = []

    with pytest.raises(ValueError, match="required step must declare expected_outputs"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_safety_fields_are_required() -> None:
    raw = _raw_schedule()
    del raw["safety"]["manual_review_required"]

    with pytest.raises(ValueError, match="manual_review_required"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_unsafe_production_effect_fails() -> None:
    raw = _raw_schedule()
    raw["safety"]["production_effect"] = "apply_weights"

    with pytest.raises(ValueError, match="production_effect"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_empty_command() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["command"] = ""

    with pytest.raises(ValueError, match="command"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_weight_search_is_not_daily() -> None:
    config = load_operations_schedule_config()

    daily_ids = {step.step_id for step in config.daily_pipeline}
    weekly_ids = {step.step_id for step in config.weekly_pipeline}
    biweekly_ids = {step.step_id for step in config.biweekly_pipeline}
    monthly_ids = {step.step_id for step in config.monthly_pipeline}
    assert "weight_calibration_search" not in daily_ids
    assert "weight_calibration_search" not in weekly_ids
    assert "weight_calibration_search" not in biweekly_ids
    assert "weight_calibration_search" in monthly_ids


def test_daily_operations_command_graph_builds() -> None:
    graph = build_daily_operations_command_graph()

    expected_nodes = {
        "data_freshness_check",
        "etf_daily_run",
        "forward_update",
        "ai_confirmation_run",
        "satellite_replacement_run",
        "ai_attribution_update",
        "satellite_attribution_update",
        "reader_brief_generate",
        "report_registry_update",
        "operations_health_check",
    }
    assert graph.schema_version == OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    assert graph.cadence == "daily"
    assert graph.dry_run_only is True
    assert graph.commands_executed is False
    assert graph.execution_order == [node.node_id for node in graph.nodes]
    assert set(graph.execution_order) == expected_nodes


def test_daily_operations_command_graph_nodes_expose_required_fields() -> None:
    graph = build_daily_operations_command_graph()

    by_id = {node.node_id: node for node in graph.nodes}
    node = by_id["satellite_replacement_run"]
    assert node.command == "aits etf satellite report --date {as_of}"
    assert node.dependencies == ["data_freshness_check", "ai_confirmation_run"]
    assert node.inputs
    assert node.outputs == [
        "reports/etf_portfolio/satellite/reports/satellite_replacement_report_{as_of}.json"
    ]
    assert node.required is True
    assert node.failure_policy == "block_dependent_steps"
    assert node.estimated_runtime_class == "medium"


def test_daily_operations_command_graph_dependencies_topologically_sorted() -> None:
    graph = build_daily_operations_command_graph()
    position = {node_id: index for index, node_id in enumerate(graph.execution_order)}

    for node in graph.nodes:
        for dependency in node.dependencies:
            assert position[dependency] < position[node.node_id]


def test_daily_operations_command_graph_allows_optional_nodes_to_skip() -> None:
    graph = build_daily_operations_command_graph(include_optional=False)

    node_ids = {node.node_id for node in graph.nodes}
    assert "ai_attribution_update" not in node_ids
    assert "satellite_attribution_update" not in node_ids
    assert set(graph.skipped_optional_steps) == {
        "ai_attribution_update",
        "satellite_attribution_update",
    }
    report_registry = next(node for node in graph.nodes if node.node_id == "report_registry_update")
    assert "ai_attribution_update" not in report_registry.dependencies
    assert "satellite_attribution_update" not in report_registry.dependencies


def test_daily_operations_command_graph_refuses_to_skip_required_node() -> None:
    with pytest.raises(OperationsCommandGraphError, match="cannot skip required nodes"):
        build_daily_operations_command_graph(skipped_optional_step_ids={"etf_daily_run"})


def test_daily_operations_command_graph_missing_required_node_fails() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"] = [
        step for step in raw["daily_pipeline"] if step["step_id"] != "etf_daily_run"
    ]
    for step in raw["daily_pipeline"]:
        step["dependencies"] = [
            dependency
            for dependency in step["dependencies"]
            if dependency != "etf_daily_run"
        ]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="missing required nodes"):
        build_daily_operations_command_graph(config)


def test_daily_operations_command_graph_cycle_detection_works() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["dependencies"] = ["operations_health_check"]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="cycle detected"):
        build_daily_operations_command_graph(config)


def test_daily_operations_command_graph_safety_flags_included() -> None:
    graph = build_daily_operations_command_graph()

    assert graph.safety.observe_only is True
    assert graph.safety.candidate_only is True
    assert graph.safety.production_effect == "none"
    assert graph.safety.broker_action == "none"
    assert graph.safety.manual_review_required is True
    assert all(node.safety.production_effect == "none" for node in graph.nodes)
    assert all(node.safety.broker_action == "none" for node in graph.nodes)


def test_weekly_operations_command_graph_builds() -> None:
    graph = build_weekly_operations_command_graph()

    expected_nodes = {
        "weekly_review_aggregate",
        "weekly_review_generate",
        "forward_weekly_review",
        "decision_journal_review_prompt",
        "parameter_review_aggregate",
        "parameter_review_report",
        "watchlist_review",
        "operations_report",
        "reader_brief_weekly_navigation",
    }
    assert graph.schema_version == OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    assert graph.cadence == "weekly"
    assert graph.dry_run_only is True
    assert graph.commands_executed is False
    assert graph.execution_order == [node.node_id for node in graph.nodes]
    assert set(graph.external_dependencies) == {
        "forward_update",
        "operations_health_check",
    }
    assert set(graph.execution_order) == expected_nodes


def test_weekly_operations_command_graph_preserves_external_daily_inputs() -> None:
    graph = build_weekly_operations_command_graph()
    by_id = {node.node_id: node for node in graph.nodes}

    weekly_aggregate = by_id["weekly_review_aggregate"]
    forward_review = by_id["forward_weekly_review"]
    assert weekly_aggregate.dependencies == []
    assert weekly_aggregate.external_dependencies == ["operations_health_check"]
    assert weekly_aggregate.inputs == [
        "reports/etf_portfolio/operations/daily/operations_health_{as_of}.json",
        "reports/etf_portfolio/operations/daily/operations_health_{as_of}.md",
    ]
    assert forward_review.dependencies == []
    assert forward_review.external_dependencies == ["forward_update"]
    assert forward_review.inputs == [
        "reports/etf_portfolio/forward/updates/forward_update_{as_of}.json"
    ]


def test_weekly_operations_command_graph_dependencies_topologically_sorted() -> None:
    graph = build_weekly_operations_command_graph()
    position = {node_id: index for index, node_id in enumerate(graph.execution_order)}

    for node in graph.nodes:
        for dependency in node.dependencies:
            assert position[dependency] < position[node.node_id]


def test_weekly_operations_command_graph_manual_review_nodes_flagged() -> None:
    graph = build_weekly_operations_command_graph()
    by_id = {node.node_id: node for node in graph.nodes}

    manual_review_nodes = {
        "weekly_review_generate",
        "decision_journal_review_prompt",
        "parameter_review_report",
        "watchlist_review",
        "operations_report",
        "reader_brief_weekly_navigation",
    }
    assert all(by_id[node_id].owner_review_required for node_id in manual_review_nodes)
    assert by_id["weekly_review_generate"].failure_policy == "manual_review_required"
    assert by_id["operations_report"].failure_policy == "manual_review_required"


def test_weekly_operations_command_graph_allows_optional_parameter_review_to_skip() -> None:
    raw = _raw_schedule()
    for step in raw["weekly_pipeline"]:
        if step["step_id"] in {"parameter_review_aggregate", "parameter_review_report"}:
            step["required"] = False
            step["failure_policy"] = "skip_optional_step"
    config = ETFOperationsScheduleConfig.model_validate(raw)

    graph = build_weekly_operations_command_graph(
        config,
        skipped_optional_step_ids={
            "parameter_review_aggregate",
            "parameter_review_report",
        },
    )

    node_ids = {node.node_id for node in graph.nodes}
    assert "parameter_review_aggregate" not in node_ids
    assert "parameter_review_report" not in node_ids
    assert set(graph.skipped_optional_steps) == {
        "parameter_review_aggregate",
        "parameter_review_report",
    }
    operations_report = next(node for node in graph.nodes if node.node_id == "operations_report")
    assert operations_report.dependencies == [
        "weekly_review_generate",
        "watchlist_review",
    ]


def test_weekly_operations_command_graph_missing_weekly_review_blocks_dependents() -> None:
    raw = _raw_schedule()
    raw["weekly_pipeline"] = [
        step
        for step in raw["weekly_pipeline"]
        if step["step_id"] != "weekly_review_generate"
    ]
    for step in raw["weekly_pipeline"]:
        step["dependencies"] = [
            dependency
            for dependency in step["dependencies"]
            if dependency != "weekly_review_generate"
        ]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="missing required nodes"):
        build_weekly_operations_command_graph(config)


def test_weekly_operations_command_graph_cycle_detection_works() -> None:
    raw = _raw_schedule()
    raw["weekly_pipeline"][0]["dependencies"].append("reader_brief_weekly_navigation")
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="cycle detected"):
        build_weekly_operations_command_graph(config)


def test_weekly_operations_command_graph_safety_flags_included() -> None:
    graph = build_weekly_operations_command_graph()

    assert graph.safety.observe_only is True
    assert graph.safety.candidate_only is True
    assert graph.safety.production_effect == "none"
    assert graph.safety.broker_action == "none"
    assert graph.safety.manual_review_required is True
    assert all(node.safety.production_effect == "none" for node in graph.nodes)
    assert all(node.safety.broker_action == "none" for node in graph.nodes)


def test_biweekly_operations_command_graph_builds() -> None:
    graph = build_biweekly_operations_command_graph()

    expected_nodes = {
        "ai_attribution_scorecard_review",
        "satellite_attribution_scorecard_review",
        "weight_calibration_evidence_update",
        "operations_report_biweekly",
    }
    assert graph.schema_version == OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    assert graph.cadence == "biweekly"
    assert graph.dry_run_only is True
    assert graph.commands_executed is False
    assert graph.execution_order == [node.node_id for node in graph.nodes]
    assert set(graph.execution_order) == expected_nodes
    assert set(graph.external_dependencies) == {
        "ai_attribution_update",
        "forward_weekly_review",
        "parameter_review_report",
        "satellite_attribution_update",
    }


def test_biweekly_operations_command_graph_dependencies_topologically_sorted() -> None:
    graph = build_biweekly_operations_command_graph()
    position = {node_id: index for index, node_id in enumerate(graph.execution_order)}

    for node in graph.nodes:
        for dependency in node.dependencies:
            assert position[dependency] < position[node.node_id]


def test_biweekly_operations_command_graph_preserves_external_inputs() -> None:
    graph = build_biweekly_operations_command_graph()
    by_id = {node.node_id: node for node in graph.nodes}

    weight_evidence = by_id["weight_calibration_evidence_update"]
    assert weight_evidence.dependencies == []
    assert weight_evidence.external_dependencies == [
        "forward_weekly_review",
        "parameter_review_report",
    ]
    assert weight_evidence.inputs == [
        "reports/etf_portfolio/forward/weekly_reviews/weekly_review_{as_of}.json",
        "reports/etf_portfolio/forward/weekly_reviews/weekly_review_{as_of}.md",
        "reports/etf_portfolio/parameter_review/reports/parameter_review_{as_of}.json",
        "reports/etf_portfolio/parameter_review/reports/parameter_review_{as_of}.md",
    ]


def test_monthly_operations_command_graph_builds() -> None:
    graph = build_monthly_operations_command_graph()

    expected_nodes = {
        "data_quality_audit",
        "weight_calibration_search",
        "weight_calibration_report",
        "parameter_review_governance",
        "strategy_evidence_dashboard_update",
        "operations_report_monthly",
    }
    assert graph.schema_version == OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    assert graph.cadence == "monthly"
    assert graph.dry_run_only is True
    assert graph.commands_executed is False
    assert graph.execution_order == [node.node_id for node in graph.nodes]
    assert set(graph.execution_order) == expected_nodes
    assert set(graph.external_dependencies) == {
        "parameter_review_report",
        "weight_calibration_evidence_update",
    }


def test_monthly_operations_command_graph_dependencies_topologically_sorted() -> None:
    graph = build_monthly_operations_command_graph()
    position = {node_id: index for index, node_id in enumerate(graph.execution_order)}

    for node in graph.nodes:
        for dependency in node.dependencies:
            assert position[dependency] < position[node.node_id]


def test_monthly_operations_command_graph_marks_heavy_search_slow_cadence() -> None:
    graph = build_monthly_operations_command_graph()
    by_id = {node.node_id: node for node in graph.nodes}

    weight_search = by_id["weight_calibration_search"]
    assert weight_search.command == (
        "aits etf weight-calibration search --search etf_initial_weight_search_v1"
    )
    assert weight_search.dependencies == ["data_quality_audit"]
    assert weight_search.estimated_runtime_class == "slow"
    assert weight_search.owner_review_required is True
    assert "weight_calibration_search" not in build_daily_operations_command_graph().execution_order
    assert (
        "weight_calibration_search"
        not in build_weekly_operations_command_graph().execution_order
    )
    assert (
        "weight_calibration_search"
        not in build_biweekly_operations_command_graph().execution_order
    )


def test_monthly_operations_command_graph_manual_review_for_parameter_proposals() -> None:
    graph = build_monthly_operations_command_graph()
    by_id = {node.node_id: node for node in graph.nodes}

    parameter_governance = by_id["parameter_review_governance"]
    weight_report = by_id["weight_calibration_report"]
    assert parameter_governance.external_dependencies == ["parameter_review_report"]
    assert parameter_governance.failure_policy == "manual_review_required"
    assert parameter_governance.owner_review_required is True
    assert weight_report.external_dependencies == ["weight_calibration_evidence_update"]
    assert weight_report.failure_policy == "manual_review_required"
    assert weight_report.owner_review_required is True


def test_monthly_operations_command_graph_missing_required_node_fails() -> None:
    raw = _raw_schedule()
    raw["monthly_pipeline"] = [
        step
        for step in raw["monthly_pipeline"]
        if step["step_id"] != "weight_calibration_search"
    ]
    for step in raw["monthly_pipeline"]:
        step["dependencies"] = [
            dependency
            for dependency in step["dependencies"]
            if dependency != "weight_calibration_search"
        ]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="missing required nodes"):
        build_monthly_operations_command_graph(config)


def test_biweekly_operations_command_graph_cycle_detection_works() -> None:
    raw = _raw_schedule()
    raw["biweekly_pipeline"][0]["dependencies"].append("operations_report_biweekly")
    config = ETFOperationsScheduleConfig.model_validate(raw)

    with pytest.raises(OperationsCommandGraphError, match="cycle detected"):
        build_biweekly_operations_command_graph(config)


def test_operations_artifact_freshness_fresh_artifact_passes_and_parses_dates(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    _write_text_artifact(
        tmp_path,
        "outputs/reports/data_quality_2026-06-03.md",
        "\n".join(
            [
                "generated_at: 2026-06-03T09:30:00+00:00",
                "as_of_date: 2026-06-03",
            ]
        ),
    )
    _write_json_artifact(
        tmp_path,
        "reports/etf_portfolio/forward/updates/forward_update_2026-06-03.json",
        {
            "generated_at": "2026-06-03T10:30:00+00:00",
            "as_of_date": "2026-06-03",
        },
    )

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
        checked_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    artifact = _artifact_by_id(report, "forward_update:1")

    assert artifact.freshness_status == "fresh"
    assert artifact.dependency_status == "optional"
    assert artifact.as_of_date == date(2026, 6, 3)
    assert artifact.generated_at == datetime(2026, 6, 3, 10, 30, tzinfo=UTC)
    assert artifact.age_days == 0


def test_operations_artifact_freshness_stale_required_artifact_blocks(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    _write_json_artifact(
        tmp_path,
        "reports/etf_portfolio/forward/updates/forward_update_2026-06-03.json",
        {
            "generated_at": "2026-05-30T10:30:00+00:00",
            "as_of_date": "2026-05-30",
        },
    )

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    artifact = _artifact_by_id(report, "forward_update:1")

    assert artifact.freshness_status == "stale"
    assert artifact.dependency_status == "blocking"
    assert artifact.age_days == 4
    assert "forward_update:1" in report.blocking_artifacts


def test_operations_artifact_freshness_missing_required_artifact_blocks(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    artifact = _artifact_by_id(report, "data_freshness_check:1")

    assert artifact.freshness_status == "missing"
    assert artifact.dependency_status == "blocking"
    assert "data_freshness_check:1" in report.blocking_artifacts


def test_operations_artifact_freshness_missing_optional_artifact_warns(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    artifact = _artifact_by_id(report, "ai_attribution_update:1")

    assert artifact.required is False
    assert artifact.freshness_status == "missing"
    assert artifact.dependency_status == "warning"
    assert "ai_attribution_update:1" in report.warning_artifacts


def test_operations_artifact_freshness_dependency_chain_status_computed(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    _write_text_artifact(
        tmp_path,
        "reports/etf_portfolio/2026-06-03_portfolio_brief.md",
        "\n".join(
            [
                "generated_at: 2026-06-03T10:30:00+00:00",
                "as_of_date: 2026-06-03",
            ]
        ),
    )

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    artifact = _artifact_by_id(report, "etf_daily_run:1")

    assert artifact.freshness_status == "fresh"
    assert artifact.dependency_status == "blocking"
    assert artifact.blocking_dependencies == ["data_freshness_check"]
    assert "etf_daily_run:1" in report.blocking_artifacts


def test_operations_artifact_freshness_resolves_dynamic_run_id_glob(
    tmp_path: Path,
) -> None:
    graph = build_monthly_operations_command_graph()
    _write_json_artifact(
        tmp_path,
        "reports/etf_portfolio/weight_calibration/run_123/summary.json",
        {
            "generated_at": "2026-06-03T10:30:00+00:00",
            "as_of_date": "2026-06-03",
        },
    )

    report = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    artifact = _artifact_by_id(report, "weight_calibration_search:1")

    assert artifact.freshness_status == "fresh"
    assert Path(artifact.path).as_posix().endswith(
        "reports/etf_portfolio/weight_calibration/run_123/summary.json"
    )


def test_operations_failure_policy_maps_failure_to_severity(tmp_path: Path) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
        checked_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, 5, tzinfo=UTC),
    )
    validation_event = _failure_event_by_artifact(report, "data_freshness_check:1")
    optional_event = _failure_event_by_artifact(report, "ai_attribution_update:1")

    assert validation_event.failure_policy == "fail_pipeline"
    assert validation_event.severity == "critical"
    assert optional_event.failure_policy == "skip_optional_step"
    assert optional_event.severity == "warning"
    assert report.severity_summary["critical"] >= 1
    assert report.severity_summary["warning"] >= 1


def test_operations_failure_policy_critical_validation_failure_blocks(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )

    report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    event = _failure_event_by_artifact(report, "data_freshness_check:1")

    assert report.pipeline_status == "blocked"
    assert event.event_type == "artifact_missing"
    assert event.severity == "critical"
    assert event.blocks_pipeline is True
    assert event.blocks_dependent_steps is True
    assert event.recommended_action == "stop_pipeline_until_artifact_recovers"
    assert event.event_id in report.blocking_events


def test_operations_failure_policy_optional_missing_artifact_warns(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    _write_text_artifact(
        tmp_path,
        "outputs/reports/data_quality_2026-06-03.md",
        "\n".join(
            [
                "generated_at: 2026-06-03T09:30:00+00:00",
                "as_of_date: 2026-06-03",
            ]
        ),
    )
    _write_json_artifact(
        tmp_path,
        "reports/etf_portfolio/ai_confirmation/reports/"
        "ai_confirmation_report_2026-06-03.json",
        {
            "generated_at": "2026-06-03T10:30:00+00:00",
            "as_of_date": "2026-06-03",
        },
    )
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )

    report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    event = _failure_event_by_artifact(report, "ai_attribution_update:1")

    assert event.event_type == "artifact_missing"
    assert event.severity == "warning"
    assert event.blocks_pipeline is False
    assert event.blocks_dependent_steps is False
    assert event.requires_manual_review is False
    assert event.recommended_action == "skip_optional_step_and_warn"
    assert event.event_id in report.warning_events


def test_operations_failure_policy_required_stale_artifact_blocks(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    _write_text_artifact(
        tmp_path,
        "outputs/reports/data_quality_2026-06-03.md",
        "\n".join(
            [
                "generated_at: 2026-06-03T09:30:00+00:00",
                "as_of_date: 2026-06-03",
            ]
        ),
    )
    _write_json_artifact(
        tmp_path,
        "reports/etf_portfolio/forward/updates/forward_update_2026-06-03.json",
        {
            "generated_at": "2026-05-30T10:30:00+00:00",
            "as_of_date": "2026-05-30",
        },
    )
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )

    report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    event = _failure_event_by_artifact(report, "forward_update:1")

    assert report.pipeline_status == "blocked"
    assert event.event_type == "artifact_stale"
    assert event.failure_policy == "block_dependent_steps"
    assert event.severity == "error"
    assert event.blocks_pipeline is False
    assert event.blocks_dependent_steps is True
    assert event.recommended_action == "block_dependent_steps_until_artifact_recovers"


def test_operations_failure_policy_manual_review_failure_requires_owner_review(
    tmp_path: Path,
) -> None:
    graph = build_weekly_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )

    report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    event = _failure_event_by_artifact(report, "decision_journal_review_prompt:1")

    assert event.failure_policy == "manual_review_required"
    assert event.severity == "error"
    assert event.requires_manual_review is True
    assert event.blocks_pipeline is False
    assert event.blocks_dependent_steps is False
    assert event.recommended_action == "request_owner_manual_review"
    assert event.event_id in report.manual_review_events


def test_operations_failure_policy_serialization_is_stable(tmp_path: Path) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
        checked_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    evaluated_at = datetime(2026, 6, 3, 12, 5, tzinfo=UTC)

    first_report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=evaluated_at,
    )
    second_report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=evaluated_at,
    )

    assert first_report.schema_version == OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION
    assert first_report.model_dump(mode="json") == second_report.model_dump(mode="json")


def _raw_schedule() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _write_json_artifact(root: Path, relative_path: str, payload: dict[str, object]) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_text_artifact(root: Path, relative_path: str, text: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _artifact_by_id(report: Any, artifact_id: str) -> Any:
    return next(artifact for artifact in report.artifacts if artifact.artifact_id == artifact_id)


def _failure_event_by_artifact(report: Any, artifact_id: str) -> Any:
    return next(event for event in report.events if event.artifact_id == artifact_id)
