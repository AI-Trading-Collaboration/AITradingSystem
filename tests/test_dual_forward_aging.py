from __future__ import annotations

from pathlib import Path

from test_equal_risk_growth_tilt import _balanced_core_ready_activation_sources
from test_external_validation import (
    _write_external_validation_caches,
    _write_matching_external_records,
    _write_small_growth_config,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.equal_risk_growth_tilt import (
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
    run_balanced_core_definition_lock,
    run_balanced_core_first_observation_write_after_validation,
    run_balanced_core_forward_aging_dry_run,
    run_balanced_core_launch_owner_report,
    run_balanced_core_launch_preflight,
    run_balanced_core_observation_idempotency_proof,
    run_balanced_core_watchlist_activation_contract,
    run_dual_forward_aging_comparator_panel_after_launch,
    run_dual_forward_aging_monthly_monitor_contract,
    run_dual_forward_aging_reader_brief_safe_preview_after_launch,
    run_dual_forward_aging_scoreboard_safety_review,
    run_external_validation_balanced_core_launch_master_review,
    run_growth_tilt_candidate_result_summary,
)
from ai_trading_system.external_validation import (
    run_dynamic_weight_path_replay_final_check,
    run_external_independent_return_replay,
    run_external_validation_difference_attribution,
    run_external_validation_master_review,
    run_external_validation_owner_report,
    run_external_validation_real_result_status_reader,
    run_external_validation_scope_contract,
    run_external_validation_to_launch_gate,
    run_metric_and_sgov_reconciliation_signoff,
    run_metric_definition_reconciliation,
    run_sgov_total_return_external_check,
    run_static_baseline_external_reconciliation,
    run_static_baseline_reconciliation_final_check,
    run_strategy_weight_path_export,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
)


def test_external_validation_gate_launches_balanced_core_after_validation(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_external_validation_caches(
        tmp_path
    )
    config_path = _write_small_growth_config(tmp_path)
    external_root = tmp_path / "outputs" / "research_strategies" / "external_validation"
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    docs_root = tmp_path / "docs" / "research"
    static_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "simple_config_path": DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        "output_root": external_root,
        "as_of_date": as_of,
    }
    external_kwargs = {
        **static_kwargs,
        "growth_config_path": config_path,
        "growth_output_root": growth_root,
    }

    scope = run_external_validation_scope_contract(output_root=external_root)
    pending_static = run_static_baseline_external_reconciliation(**static_kwargs)
    external_records_path = _write_matching_external_records(tmp_path, pending_static)
    static = run_static_baseline_external_reconciliation(
        **static_kwargs,
        external_records_path=external_records_path,
    )
    weight_export = run_strategy_weight_path_export(**external_kwargs)
    replay = run_external_independent_return_replay(
        **external_kwargs,
        _weight_export_payload=weight_export,
    )
    metric = run_metric_definition_reconciliation(output_root=external_root)
    sgov = run_sgov_total_return_external_check(**external_kwargs)
    difference = run_external_validation_difference_attribution(
        **external_kwargs,
        _static_reconciliation_payload=static,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
    )
    owner = run_external_validation_owner_report(
        **external_kwargs,
        docs_path=docs_root / "external_validation_owner_report.md",
        _scope_payload=scope,
        _static_payload=static,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
        _difference_payload=difference,
    )
    master = run_external_validation_master_review(
        **external_kwargs,
        docs_path=docs_root / "external_validation_master_review.md",
        owner_docs_path=docs_root / "external_validation_owner_report.md",
        _owner_payload=owner,
    )

    status_reader = run_external_validation_real_result_status_reader(
        **external_kwargs,
        _scope_payload=scope,
        _static_payload=static,
        _weight_export_payload=weight_export,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
        _difference_payload=difference,
        _owner_payload=owner,
        _master_payload=master,
    )
    static_final = run_static_baseline_reconciliation_final_check(
        **external_kwargs,
        _static_payload=static,
        _metric_payload=metric,
        _sgov_payload=sgov,
    )
    dynamic_final = run_dynamic_weight_path_replay_final_check(
        **external_kwargs,
        _weight_export_payload=weight_export,
        _replay_payload=replay,
    )
    signoff = run_metric_and_sgov_reconciliation_signoff(
        **external_kwargs,
        _metric_payload=metric,
        _sgov_payload=sgov,
    )

    growth_common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }
    activation_sources = _balanced_core_ready_activation_sources()
    activation = run_balanced_core_watchlist_activation_contract(
        **growth_common,
        _master_payload=activation_sources["master"],
        _owner_payload=activation_sources["owner"],
        _watchlist_payload=activation_sources["watchlist"],
        _role_payload=activation_sources["role"],
        _finalist_payload=activation_sources["finalist"],
    )
    candidate_summary = run_growth_tilt_candidate_result_summary(**growth_common)
    definition_lock = run_balanced_core_definition_lock(
        **growth_common,
        _candidate_summary_payload=candidate_summary,
    )
    launch_gate = run_external_validation_to_launch_gate(
        **external_kwargs,
        _status_reader_payload=status_reader,
        _static_final_payload=static_final,
        _dynamic_final_payload=dynamic_final,
        _metric_sgov_payload=signoff,
        _definition_lock_payload=definition_lock,
    )
    dry_run = run_balanced_core_forward_aging_dry_run(
        **growth_common,
        _activation_payload=activation,
        _definition_lock_payload=definition_lock,
    )
    preflight = run_balanced_core_launch_preflight(
        **growth_common,
        simple_config_path=DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        external_validation_output_root=external_root,
        _launch_gate_payload=launch_gate,
        _definition_lock_payload=definition_lock,
        _dry_run_payload=dry_run,
    )
    observation = run_balanced_core_first_observation_write_after_validation(
        **growth_common,
        simple_config_path=DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        external_validation_output_root=external_root,
        _launch_gate_payload=launch_gate,
        _preflight_payload=preflight,
    )
    idempotency = run_balanced_core_observation_idempotency_proof(
        **growth_common,
        decision_date=as_of,
    )
    panel = run_dual_forward_aging_comparator_panel_after_launch(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        as_of_date=as_of,
    )
    scoreboard = run_dual_forward_aging_scoreboard_safety_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        as_of_date=as_of,
    )
    reader = run_dual_forward_aging_reader_brief_safe_preview_after_launch(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        external_validation_output_root=external_root,
        as_of_date=as_of,
        _launch_gate_payload=launch_gate,
        _scoreboard_payload=scoreboard,
    )
    owner_report = run_balanced_core_launch_owner_report(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        docs_path=docs_root / "balanced_core_launch_owner_report.md",
        external_validation_output_root=external_root,
        as_of_date=as_of,
        _launch_gate_payload=launch_gate,
        _preflight_payload=preflight,
        _observation_payload=observation,
        _idempotency_payload=idempotency,
        _panel_payload=panel,
        _scoreboard_payload=scoreboard,
        _reader_payload=reader,
    )
    master = run_external_validation_balanced_core_launch_master_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        docs_path=docs_root / "external_validation_balanced_core_launch_master_review.md",
        owner_docs_path=docs_root / "balanced_core_launch_owner_report.md",
        external_validation_output_root=external_root,
        as_of_date=as_of,
        _owner_report_payload=owner_report,
    )
    monthly = run_dual_forward_aging_monthly_monitor_contract(
        output_root=roadmap_root,
        docs_path=docs_root / "dual_forward_aging_monthly_monitor_contract.md",
    )

    assert status_reader["status"] in {
        "EXTERNAL_VALIDATION_RESULT_READY",
        "EXTERNAL_VALIDATION_RESULT_WARN",
    }
    assert static_final["status"] == "STATIC_BASELINE_FINAL_WARN"
    assert dynamic_final["status"] in {
        "DYNAMIC_REPLAY_FINAL_MATCHED",
        "DYNAMIC_REPLAY_FINAL_WARN",
    }
    assert signoff["status"] in {"METRIC_SGOV_SIGNOFF_READY", "METRIC_SGOV_SIGNOFF_WARN"}
    assert launch_gate["status"] in {
        "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS",
        "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN",
    }
    assert preflight["status"] in {
        "BALANCED_CORE_LAUNCH_PREFLIGHT_PASS",
        "BALANCED_CORE_LAUNCH_PREFLIGHT_WARN",
    }
    assert observation["status"] == "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN"
    assert observation["external_validation_status"] == launch_gate["status"]
    assert idempotency["status"] == "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_PASS"
    assert panel["status"] == "DUAL_FORWARD_PANEL_AFTER_LAUNCH_PENDING"
    assert len(panel["panel_rows"]) == 5
    assert scoreboard["status"] == "DUAL_SCOREBOARD_INSUFFICIENT_SAMPLE"
    assert reader["status"] == "DUAL_READER_BRIEF_AFTER_LAUNCH_SAFE"
    assert owner_report["owner_recommendation"] in {
        "BALANCED_CORE_FORWARD_AGING_LAUNCHED",
        "BALANCED_CORE_LAUNCH_WARN",
    }
    assert master["status"] in {
        "EXTERNAL_VALIDATION_AND_BALANCED_CORE_LAUNCH_PASS",
        "EXTERNAL_VALIDATION_AND_BALANCED_CORE_LAUNCH_WARN",
    }
    assert monthly["status"] == "DUAL_FORWARD_MONTHLY_MONITOR_READY"
    assert observation["observations"][0]["strategy_id"] == FOCUSED_GROWTH_TILT_CANDIDATE_ID

    for payload in (
        status_reader,
        static_final,
        dynamic_final,
        signoff,
        launch_gate,
        preflight,
        observation,
        idempotency,
        panel,
        scoreboard,
        reader,
        owner_report,
        master,
        monthly,
    ):
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["broker_action"] == "none"
        assert payload["manual_review_required"] is True

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "dual-forward-aging-monthly-monitor-contract",
            "--output-root",
            str(roadmap_root),
            "--docs-path",
            str(docs_root / "dual_forward_aging_monthly_monitor_contract.md"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (roadmap_root / "dual_forward_aging_monthly_monitor_contract.json").exists()
