from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION,
    OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION,
    OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION,
    OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION,
    OPERATIONS_SCHEDULE_SCHEMA_VERSION,
    OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION,
    OPERATIONS_VALIDATION_SCHEMA_VERSION,
    ETFOperationsScheduleConfig,
    OperationsCommandGraphError,
    build_biweekly_operations_command_graph,
    build_daily_operations_command_graph,
    build_monthly_operations_command_graph,
    build_operations_health_report,
    build_operations_owner_review_checklist,
    build_operations_scheduler_dry_run,
    build_operations_validation_report,
    build_weekly_operations_command_graph,
    check_operations_artifact_freshness,
    evaluate_operations_failure_policy,
    load_operations_schedule_config,
    operations_schedule_required_step_ids,
    operations_schedule_step_ids,
    render_operations_health_report_markdown,
    render_operations_validation_report_markdown,
    write_operations_health_report,
    write_operations_validation_report,
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
        "data_quality_governance_report",
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
        "data_quality_governance_report",
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
    reader_brief = next(node for node in graph.nodes if node.node_id == "reader_brief_generate")
    assert reader_brief.dependencies == ["data_quality_governance_report"]


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


def test_operations_owner_review_checklist_daily_uses_manual_review_step(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    failure_report = evaluate_operations_failure_policy(
        freshness,
        evaluated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    checklist = build_operations_owner_review_checklist(
        cadence="daily",
        as_of="2026-06-03",
        failure_report=failure_report,
        generated_at=datetime(2026, 6, 3, 12, 5, tzinfo=UTC),
    )

    assert checklist.schema_version == OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION
    assert checklist.checklist_step_id == "daily_owner_review"
    assert checklist.checklist_command == "manual_review:daily_quick_check"
    assert checklist.checklist_dependencies == ["operations_health_check"]
    assert checklist.checklist_expected_outputs == [
        "manual_review_checklist:daily_quick_check"
    ]
    assert checklist.checklist_status == "blocked"
    assert checklist.source_pipeline_status == "blocked"
    assert checklist.read_only is True
    assert checklist.commands_executed is False
    assert _checklist_item_by_id(
        checklist,
        "safety:operations_boundary",
    ).owner_action == "confirm_no_production_or_broker_action"


def test_operations_owner_review_checklist_includes_blocking_failure_event(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    failure_report = evaluate_operations_failure_policy(freshness)
    event = _failure_event_by_artifact(failure_report, "data_freshness_check:1")

    checklist = build_operations_owner_review_checklist(
        cadence="daily",
        as_of="2026-06-03",
        failure_report=failure_report,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    item = _checklist_item_by_id(checklist, f"event:{event.event_id}")

    assert item.category == "blocking_event"
    assert item.blocking is True
    assert item.required is True
    assert item.source_step == "data_freshness_check"
    assert item.related_artifact_ids == ["data_freshness_check:1"]
    assert item.owner_action == "stop_pipeline_until_artifact_recovers"
    assert item.item_id in checklist.blocking_items


def test_operations_owner_review_checklist_includes_optional_warning_event(
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
    failure_report = evaluate_operations_failure_policy(freshness)
    event = _failure_event_by_artifact(failure_report, "ai_attribution_update:1")

    checklist = build_operations_owner_review_checklist(
        cadence="daily",
        as_of="2026-06-03",
        failure_report=failure_report,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    item = _checklist_item_by_id(checklist, f"event:{event.event_id}")

    assert item.category == "warning_event"
    assert item.blocking is False
    assert item.required is True
    assert item.source_step == "ai_attribution_update"
    assert item.owner_action == "skip_optional_step_and_warn"
    assert item.item_id in checklist.warning_items


def test_operations_owner_review_checklist_weekly_manual_review_event(
    tmp_path: Path,
) -> None:
    graph = build_weekly_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    failure_report = evaluate_operations_failure_policy(freshness)
    event = _failure_event_by_artifact(
        failure_report,
        "decision_journal_review_prompt:1",
    )

    checklist = build_operations_owner_review_checklist(
        cadence="weekly",
        as_of="2026-06-03",
        failure_report=failure_report,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    item = _checklist_item_by_id(checklist, f"event:{event.event_id}")

    assert checklist.checklist_step_id == "weekly_owner_review"
    assert item.category == "manual_review_event"
    assert item.blocking is False
    assert item.owner_action == "request_owner_manual_review"
    assert item.item_id in checklist.manual_review_items


def test_operations_owner_review_checklist_monthly_template_without_failure_report() -> None:
    checklist = build_operations_owner_review_checklist(
        cadence="monthly",
        as_of="2026-06-03",
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert checklist.checklist_step_id == "monthly_owner_review"
    assert checklist.checklist_command == "manual_review:monthly_parameter_review"
    assert checklist.checklist_status == "ready"
    assert checklist.source_pipeline_status is None
    assert checklist.source_event_count == 0
    assert _checklist_item_by_id(
        checklist,
        "cadence:monthly:entry_gate",
    ).owner_action == "confirm_monthly_governance_and_slow_cadence_scope"


def test_operations_owner_review_checklist_incident_template_is_stable() -> None:
    generated_at = datetime(2026, 6, 3, 12, tzinfo=UTC)

    first_checklist = build_operations_owner_review_checklist(
        cadence="incident",
        as_of="2026-06-03",
        generated_at=generated_at,
    )
    second_checklist = build_operations_owner_review_checklist(
        cadence="incident",
        as_of="2026-06-03",
        generated_at=generated_at,
    )

    assert first_checklist.checklist_step_id == "incident_review"
    assert first_checklist.checklist_status == "manual_review_required"
    assert _checklist_item_by_id(
        first_checklist,
        "cadence:incident:entry_gate",
    ).owner_action == "confirm_incident_scope_and_recovery_boundary"
    assert first_checklist.model_dump(mode="json") == second_checklist.model_dump(
        mode="json"
    )


def test_operations_owner_review_checklist_rejects_cadence_mismatch(
    tmp_path: Path,
) -> None:
    graph = build_daily_operations_command_graph()
    freshness = check_operations_artifact_freshness(
        graph,
        as_of="2026-06-03",
        root_path=tmp_path,
    )
    failure_report = evaluate_operations_failure_policy(freshness)

    with pytest.raises(OperationsCommandGraphError, match="cadence must match"):
        build_operations_owner_review_checklist(
            cadence="weekly",
            as_of="2026-06-03",
            failure_report=failure_report,
        )


def test_operations_scheduler_dry_run_daily_reports_plan_and_blockers(
    tmp_path: Path,
) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert dry_run.schema_version == OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION
    assert dry_run.dry_run_id == "daily:2026-06-03:20260603T120000Z"
    assert dry_run.cadence == "daily"
    assert dry_run.status == "blocked"
    assert dry_run.dry_run_only is True
    assert dry_run.commands_executed is False
    assert dry_run.production_state_mutated is False
    assert dry_run.execution_order == [step.step_id for step in dry_run.planned_steps]
    assert dry_run.execution_order[0] == "data_freshness_check"
    assert "data_freshness_check:1:artifact_missing" in dry_run.blocking_failures
    assert dry_run.owner_checklist_status == "blocked"
    assert dry_run.owner_checklist_item_count > 0


def test_operations_scheduler_dry_run_weekly_preserves_dependency_order(
    tmp_path: Path,
) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="weekly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    position = {step_id: index for index, step_id in enumerate(dry_run.execution_order)}

    assert dry_run.cadence == "weekly"
    assert dry_run.execution_order[0] == "weekly_review_aggregate"
    assert position["weekly_review_aggregate"] < position["weekly_review_generate"]
    assert position["weekly_review_generate"] < position["operations_report"]
    assert dry_run.owner_checklist_status == "blocked"
    assert all(step.command_executed is False for step in dry_run.planned_steps)


def test_operations_scheduler_dry_run_monthly_includes_expected_outputs(
    tmp_path: Path,
) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="monthly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert dry_run.cadence == "monthly"
    assert (
        "reports/etf_portfolio/data_quality/governance/data_quality_report_{as_of}.json"
        in dry_run.expected_outputs
    )
    assert (
        "reports/etf_portfolio/data_quality/governance/data_quality_report_{as_of}.md"
        in dry_run.expected_outputs
    )
    assert (
        "reports/etf_portfolio/operations/monthly/operations_health_{as_of}.json"
        in dry_run.expected_outputs
    )
    assert "weight_calibration_search" in dry_run.execution_order
    assert dry_run.owner_checklist_status == "blocked"


def test_operations_scheduler_dry_run_missing_required_artifact_blocks(
    tmp_path: Path,
) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    data_gate = _dry_run_step_by_id(dry_run, "data_freshness_check")

    assert data_gate.status == "blocked"
    assert data_gate.blocking_event_ids == [
        "data_freshness_check:1:artifact_missing"
    ]
    assert "data_freshness_check:1:artifact_missing" in dry_run.blocking_failures


def test_operations_scheduler_dry_run_optional_missing_artifact_warns(
    tmp_path: Path,
) -> None:
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

    dry_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    attribution = _dry_run_step_by_id(dry_run, "ai_attribution_update")

    assert attribution.status == "warning"
    assert attribution.warning_event_ids == [
        "ai_attribution_update:1:artifact_missing"
    ]
    assert "ai_attribution_update:1:artifact_missing" in dry_run.warnings


def test_operations_scheduler_dry_run_skip_optional_steps(tmp_path: Path) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        include_optional=False,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert set(dry_run.skipped_optional_steps) == {
        "ai_attribution_update",
        "satellite_attribution_update",
    }
    assert "ai_attribution_update" not in dry_run.execution_order
    assert all(step.command_executed is False for step in dry_run.planned_steps)


def test_operations_scheduler_dry_run_biweekly_has_no_owner_checklist(
    tmp_path: Path,
) -> None:
    dry_run = build_operations_scheduler_dry_run(
        cadence="biweekly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert dry_run.cadence == "biweekly"
    assert dry_run.owner_checklist_schema_version is None
    assert dry_run.owner_checklist_status is None
    assert dry_run.owner_checklist_item_count == 0


def test_operations_scheduler_dry_run_serialization_is_stable(tmp_path: Path) -> None:
    generated_at = datetime(2026, 6, 3, 12, tzinfo=UTC)

    first_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=generated_at,
    )
    second_run = build_operations_scheduler_dry_run(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=generated_at,
    )

    assert first_run.model_dump(mode="json") == second_run.model_dump(mode="json")


def test_operations_health_report_daily_includes_required_sections(
    tmp_path: Path,
) -> None:
    report = build_operations_health_report(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert report.schema_version == OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION
    assert report.report_id == "operations_health:daily:2026-06-03:20260603T120000Z"
    assert report.status == "blocked"
    assert report.commands_executed is False
    assert report.production_state_mutated is False
    assert report.safety_banner == {
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    assert report.pipeline_schedule[0]["step_id"] == "data_freshness_check"
    assert report.pipeline_schedule[0]["status"] == "blocked"
    assert report.command_graph_summary["node_count"] == len(report.pipeline_schedule)
    assert report.artifact_freshness_summary["blocking_artifact_count"] > 0
    assert "data_freshness_check:1" in report.dependency_status["blocking_artifacts"]
    assert any(
        failure["event_id"] == "data_freshness_check:1:artifact_missing"
        for failure in report.failures
    )
    assert report.owner_review_checklist is not None
    assert report.owner_review_checklist["checklist_status"] == "blocked"
    assert report.expected_next_run["production_scheduler_entry"] == "aits ops daily-run"
    assert report.source_artifacts
    assert report.source_schema_versions["dry_run"] == (
        OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION
    )


def test_operations_health_report_tracks_optional_warning(
    tmp_path: Path,
) -> None:
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

    report = build_operations_health_report(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert any(
        warning["event_id"] == "ai_attribution_update:1:artifact_missing"
        for warning in report.warnings
    )
    attribution_step = next(
        step for step in report.pipeline_schedule if step["step_id"] == "ai_attribution_update"
    )
    assert attribution_step["status"] == "warning"


def test_operations_health_report_weekly_includes_owner_checklist(
    tmp_path: Path,
) -> None:
    report = build_operations_health_report(
        cadence="weekly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert report.cadence == "weekly"
    assert report.command_graph_summary["execution_order"][0] == "weekly_review_aggregate"
    assert report.owner_review_checklist is not None
    assert report.owner_review_checklist["checklist_step_id"] == "weekly_owner_review"
    assert report.expected_next_run["source"] == "docs/operations/operations_runbook.md"
    assert any(
        artifact.source_step == "weekly_review_generate"
        for artifact in report.source_artifacts
    )


def test_operations_health_report_writes_json_and_markdown(
    tmp_path: Path,
) -> None:
    report = build_operations_health_report(
        cadence="daily",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )
    paths = write_operations_health_report(
        report,
        json_path=tmp_path / "operations_health.json",
        markdown_path=tmp_path / "operations_health.md",
    )
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")

    assert payload["schema_version"] == OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION
    assert payload["commands_executed"] is False
    assert payload["production_state_mutated"] is False
    assert "## Safety Banner / 安全边界" in markdown
    assert "| observe_only | true |" in markdown
    assert "## Command Graph Summary / Command Graph 摘要" in markdown
    assert "## Artifact Freshness Summary / Artifact Freshness 摘要" in markdown
    assert "## Owner Review Checklist / Owner Review Checklist" in markdown
    assert "## Source Artifacts / Source Artifacts" in markdown


def test_operations_health_report_markdown_is_stable(tmp_path: Path) -> None:
    generated_at = datetime(2026, 6, 3, 12, tzinfo=UTC)
    first_report = build_operations_health_report(
        cadence="monthly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=generated_at,
    )
    second_report = build_operations_health_report(
        cadence="monthly",
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=generated_at,
    )

    assert render_operations_health_report_markdown(first_report) == (
        render_operations_health_report_markdown(second_report)
    )


def test_operations_validation_report_passes_when_workflow_complete(
    tmp_path: Path,
) -> None:
    generated_at = datetime(2026, 6, 3, 12, tzinfo=UTC)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=generated_at,
    )

    assert report.schema_version == OPERATIONS_VALIDATION_SCHEMA_VERSION
    assert report.status == "PASS"
    assert report.failed_check_count == 0
    assert report.commands_executed is False
    assert report.production_state_mutated is False
    assert report.production_effect == "none"
    assert report.broker_action == "none"
    assert report.manual_review_required is True
    checks = {check.check_id: check for check in report.checks}
    assert checks["daily_graph_valid"].status == "PASS"
    assert checks["weekly_graph_valid"].status == "PASS"
    assert checks["monthly_graph_valid"].status == "PASS"
    assert checks["freshness_checker_available"].status == "PASS"
    assert checks["required_missing_blocks"].status == "PASS"
    assert checks["optional_missing_warns"].status == "PASS"
    assert checks["reader_brief_integration_available"].status == "PASS"


def test_operations_validation_fails_when_schedule_invalid(tmp_path: Path) -> None:
    raw = _raw_schedule()
    raw["weekly_pipeline"][0]["dependencies"] = ["missing_daily_gate"]
    config_path = _write_schedule_yaml(tmp_path, raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config_path=config_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    check = _validation_check_by_id(report, "schedule_spec_valid")
    assert report.status == "FAIL"
    assert check.status == "FAIL"
    assert "dependencies reference unknown steps" in check.evidence["error"]


def test_operations_validation_fails_when_required_step_missing(
    tmp_path: Path,
) -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"] = [
        step
        for step in raw["daily_pipeline"]
        if step["step_id"] != "operations_health_check"
    ]
    for field_name in (
        "weekly_pipeline",
        "biweekly_pipeline",
        "monthly_pipeline",
        "manual_review_steps",
    ):
        for step in raw[field_name]:
            step["dependencies"] = [
                dependency
                for dependency in step["dependencies"]
                if dependency != "operations_health_check"
            ]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config=config,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert _validation_check_by_id(report, "required_steps_present").status == "FAIL"
    assert _validation_check_by_id(report, "daily_graph_valid").status == "FAIL"


def test_operations_validation_fails_when_dependency_cycle_exists(
    tmp_path: Path,
) -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["dependencies"] = ["operations_health_check"]
    config = ETFOperationsScheduleConfig.model_validate(raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config=config,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    check = _validation_check_by_id(report, "daily_graph_valid")
    assert report.status == "FAIL"
    assert check.status == "FAIL"
    assert "dependency cycle detected" in check.evidence["error"]


def test_operations_validation_fails_when_production_effect_unsafe(
    tmp_path: Path,
) -> None:
    raw = _raw_schedule()
    raw["safety"]["production_effect"] = "apply_weights"
    config_path = _write_schedule_yaml(tmp_path, raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config_path=config_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    check = _validation_check_by_id(report, "schedule_spec_valid")
    assert report.status == "FAIL"
    assert check.status == "FAIL"
    assert "production_effect" in check.evidence["error"]


def test_operations_validation_fails_when_broker_action_unsafe(
    tmp_path: Path,
) -> None:
    raw = _raw_schedule()
    raw["safety"]["broker_action"] = "submit_order"
    config_path = _write_schedule_yaml(tmp_path, raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config_path=config_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    check = _validation_check_by_id(report, "schedule_spec_valid")
    assert report.status == "FAIL"
    assert check.status == "FAIL"
    assert "broker_action" in check.evidence["error"]


def test_operations_validation_fails_when_manual_review_required_missing(
    tmp_path: Path,
) -> None:
    raw = _raw_schedule()
    del raw["safety"]["manual_review_required"]
    config_path = _write_schedule_yaml(tmp_path, raw)

    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        config_path=config_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    check = _validation_check_by_id(report, "schedule_spec_valid")
    assert report.status == "FAIL"
    assert check.status == "FAIL"
    assert "manual_review_required" in check.evidence["error"]


def test_operations_validation_writes_json_and_markdown(tmp_path: Path) -> None:
    report = build_operations_validation_report(
        as_of="2026-06-03",
        root_path=tmp_path,
        generated_at=datetime(2026, 6, 3, 12, tzinfo=UTC),
    )

    paths = write_operations_validation_report(
        report,
        json_path=tmp_path / "operations_validation.json",
        markdown_path=tmp_path / "operations_validation.md",
    )

    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert payload["schema_version"] == OPERATIONS_VALIDATION_SCHEMA_VERSION
    assert payload["status"] == "PASS"
    assert "# ETF Operations Validation Gate" in markdown
    assert "## Checks / 校验项" in markdown
    assert render_operations_validation_report_markdown(report) == markdown


def _raw_schedule() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _write_schedule_yaml(tmp_path: Path, raw: dict[str, object]) -> Path:
    path = tmp_path / "operations_schedule.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


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


def _checklist_item_by_id(checklist: Any, item_id: str) -> Any:
    return next(item for item in checklist.items if item.item_id == item_id)


def _dry_run_step_by_id(dry_run: Any, step_id: str) -> Any:
    return next(step for step in dry_run.planned_steps if step.step_id == step_id)


def _validation_check_by_id(report: Any, check_id: str) -> Any:
    return next(check for check in report.checks if check.check_id == check_id)
