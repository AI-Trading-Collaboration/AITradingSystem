from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.interfaces.cli.etf_portfolio import etf_app as canonical_etf_app
from ai_trading_system.platform.architecture import (
    CLI_CONTRACT_SCHEMA_VERSION,
    CliContractError,
    assert_frozen_cli_contract,
    build_cli_contract,
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "src/ai_trading_system/cli_commands/etf_portfolio.py"
BASELINE_PATH = PROJECT_ROOT / "inputs/architecture/arch_004g2_etf_cli_contract.yaml"
REGISTRATION_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/registration.py"
)
DATA_COMMANDS_PATH = PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/data.py"
DATA_QUALITY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/data_quality.py"
)
OPERATIONS_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/operations.py"
)
REPORTING_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/reporting.py"
)
WEEKLY_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/weekly_review.py"
)
PARAMETER_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/parameter_review.py"
)
SATELLITE_ATTRIBUTION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/satellite_attribution.py"
)
TREND_CALIBRATION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/trend_calibration.py"
)
BASELINE_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/baseline_review.py"
)
SHADOW_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/shadow_review.py"
)
DYNAMIC_ALLOCATION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_allocation.py"
)
DYNAMIC_CALIBRATION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_calibration.py"
)
DYNAMIC_ROBUSTNESS_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_robustness.py"
)
DYNAMIC_RESCUE_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_rescue.py"
)
DYNAMIC_V2_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v2_review.py"
)
DYNAMIC_V3_RESCUE_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_rescue.py"
)
DYNAMIC_V3_REAL_EVALUATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_real_evaluation.py"
)
DYNAMIC_V3_REAL_SNAPSHOT_INTAKE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_real_snapshot_intake.py"
)
DYNAMIC_V3_REAL_SNAPSHOT_DRY_RUN_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_real_snapshot_dry_run.py"
)
DYNAMIC_V3_REAL_EXECUTION_OWNER_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_real_execution_owner_review.py"
)
DYNAMIC_V3_REAL_SNAPSHOT_PAPER_ACTION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_real_snapshot_paper_action.py"
)
DYNAMIC_V3_WEEKLY_REAL_SNAPSHOT_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_weekly_real_snapshot_review.py"
)
DYNAMIC_V3_POSITION_ADVISORY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_position_advisory.py"
)
DYNAMIC_V3_POSITION_ADVISORY_DAILY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_position_advisory_daily.py"
)
DYNAMIC_V3_CONSENSUS_DRIFT_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_consensus_drift.py"
)
DYNAMIC_V3_OWNER_REVIEW_JOURNAL_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_owner_review_journal.py"
)
DYNAMIC_V3_PAPER_PORTFOLIO_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_paper_portfolio.py"
)
DYNAMIC_V3_ADVISORY_OUTCOME_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_advisory_outcome.py"
)
DYNAMIC_V3_OWNER_ATTRIBUTION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_owner_attribution.py"
)
DYNAMIC_V3_SHADOW_AGING_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_shadow_aging.py"
)
DYNAMIC_V3_WEEKLY_ADVISORY_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_weekly_advisory_review.py"
)
DYNAMIC_V3_REPLAY_INVENTORY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_replay_inventory.py"
)
DYNAMIC_V3_HISTORICAL_REPLAY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_historical_replay.py"
)
DYNAMIC_V3_BACKFILLED_OUTCOME_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_backfilled_outcome.py"
)
DYNAMIC_V3_HISTORICAL_PAPER_SIM_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_historical_paper_sim.py"
)
DYNAMIC_V3_REPLAY_PERFORMANCE_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_replay_performance_review.py"
)
DYNAMIC_V3_REPLAY_DIAGNOSIS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_replay_diagnosis.py"
)
DYNAMIC_V3_BACKFILL_REPAIR_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backfill_repair.py"
)
DYNAMIC_V3_VARIANT_COMPARISON_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_variant_comparison.py"
)
DYNAMIC_V3_RULE_CALIBRATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_rule_calibration.py"
)
DYNAMIC_V3_REPLAY_FORWARD_BRIDGE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_replay_forward_bridge.py"
)
DYNAMIC_V3_OUTCOME_DUE_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_outcome_due.py"
)
DYNAMIC_V3_OUTCOME_DASHBOARD_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_outcome_dashboard.py"
)
DYNAMIC_V3_LIMITED_VS_NOTRADE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_limited_vs_notrade.py"
)
DYNAMIC_V3_CONSENSUS_RISK_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_consensus_risk.py"
)
DYNAMIC_V3_OUTCOME_UPDATE_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_outcome_update_review.py"
)
DYNAMIC_V3_OUTCOME_UPDATE_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_outcome_update.py"
)
DYNAMIC_V3_ROLLING_EVIDENCE_REFRESH_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_rolling_evidence_refresh.py"
)
DYNAMIC_V3_EVIDENCE_TREND_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_evidence_trend.py"
)
DYNAMIC_V3_FORWARD_OUTCOME_DECISION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_forward_outcome_decision.py"
)
DYNAMIC_V3_BACKTEST_SIM_EVENTS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backtest_sim_events.py"
)
DYNAMIC_V3_BACKTEST_SIM_VARIANTS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backtest_sim_variants.py"
)
DYNAMIC_V3_BACKTEST_SIM_OUTCOME_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backtest_sim_outcome.py"
)
DYNAMIC_V3_BACKTEST_SIM_PAPER_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backtest_sim_paper.py"
)
DYNAMIC_V3_BACKTEST_SIM_REGIME_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_backtest_sim_regime.py"
)
DYNAMIC_V3_BACKTEST_SIM_SENSITIVITY_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_backtest_sim_sensitivity.py"
)
DYNAMIC_V3_BACKTEST_SIM_CALIBRATION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_backtest_sim_calibration.py"
)
DYNAMIC_V3_BACKTEST_SIM_FORWARD_BRIDGE_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_backtest_sim_forward_bridge.py"
)
DYNAMIC_V3_SIM_INTERPRETATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_sim_interpretation.py"
)
DYNAMIC_V3_SIM_RISK_RETURN_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_sim_risk_return.py"
)
DYNAMIC_V3_SIM_DEFENSIVE_VALIDATION_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_sim_defensive_validation.py"
)
DYNAMIC_V3_ADVISORY_PROPOSAL_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_advisory_proposal_review.py"
)
DYNAMIC_V3_FORWARD_CONFIRMATION_PLAN_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    "dynamic_v3_forward_confirmation_plan.py"
)
DYNAMIC_V3_CONFIRMATION_TARGETS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_confirmation_targets.py"
)
DYNAMIC_V3_CONFIRMATION_PROGRESS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_confirmation_progress.py"
)
DYNAMIC_V3_CONFIRMATION_EVALUATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_confirmation_evaluation.py"
)
DYNAMIC_V3_REPLAY_SAMPLE_EXPANSION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_replay_sample_expansion.py"
)
DYNAMIC_V3_FAILURE_ATTRIBUTION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_failure_attribution.py"
)
DYNAMIC_V3_SWEEP_CONFIG_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_sweep_config.py"
)
DYNAMIC_V3_SWEEP_RUNTIME_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_sweep_runtime.py"
)
DYNAMIC_V3_DATA_AUDIT_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_data_audit.py"
)
DYNAMIC_V3_DATA_PROVENANCE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_data_provenance.py"
)
DYNAMIC_V3_WINDOW_AUDIT_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_window_audit.py"
)
DYNAMIC_V3_INJECTION_AUDIT_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_injection_audit.py"
)
DYNAMIC_V3_WEIGHT_PATH_COMMANDS_PATH = (
    PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_weight_path.py"
)
DYNAMIC_V3_CANDIDATE_EVIDENCE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_candidate_evidence.py"
)
DYNAMIC_V3_VALIDATION_EVIDENCE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_validation_evidence.py"
)
DYNAMIC_V3_LEGACY_VALIDATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_legacy_validation.py"
)
DYNAMIC_V3_MANUAL_EXECUTION_REVIEW_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_manual_execution_review.py"
)
DYNAMIC_V3_SHADOW_REGISTRY_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_shadow_registry.py"
)
DYNAMIC_V3_RESEARCH_CONTROL_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_research_control.py"
)
DYNAMIC_V3_OBSERVATION_LIFECYCLE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_observation_lifecycle.py"
)
DYNAMIC_V3_EVIDENCE_READINESS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_evidence_readiness.py"
)
DYNAMIC_V3_EVIDENCE_GOVERNANCE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_evidence_governance.py"
)
DYNAMIC_V3_CANDIDATE_OBSERVATION_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_candidate_observation.py"
)
DYNAMIC_V3_PORTFOLIO_INTAKE_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_portfolio_intake.py"
)
DYNAMIC_V3_PORTFOLIO_RISK_CONTROLS_COMMANDS_PATH = (
    PROJECT_ROOT
    / "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_v3_portfolio_risk_controls.py"
)
COMMON_PATH = PROJECT_ROOT / "src/ai_trading_system/interfaces/cli/etf_portfolio/common.py"


def test_g2_1_etf_cli_contract_matches_frozen_runtime_tree() -> None:
    contract = build_cli_contract(
        etf_app,
        source_path=SOURCE_PATH,
        project_root=PROJECT_ROOT,
    )

    assert contract["schema_version"] == CLI_CONTRACT_SCHEMA_VERSION
    assert contract["counts"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "registered_leaf_count": 993,
        "unique_path_count": 1284,
        "duplicate_path_count": 0,
    }
    assert contract["tree_sha256"] == (
        "d4744f3ec1bbbfc05d10246f7969b3f9174e4cfebc9bec9d8b39a472e83bc6f3"
    )
    assert contract["production_effect"] == "none"
    assert contract == safe_load_yaml_path(BASELINE_PATH)
    assert_frozen_cli_contract(contract, baseline_path=BASELINE_PATH)


def test_g2_1_cli_contract_blocks_duplicate_registration(tmp_path: Path) -> None:
    app = typer.Typer()

    @app.command("same")
    def first() -> None:
        pass

    @app.command("same")
    def second() -> None:
        pass

    with pytest.raises(CliContractError, match="CLI_CONTRACT_DUPLICATE_PATH"):
        build_cli_contract(app, source_path=__file_path(), project_root=tmp_path)


def test_g2_1_cli_contract_detects_option_default_and_help_drift(tmp_path: Path) -> None:
    before = typer.Typer()
    after = typer.Typer()

    @before.command("run", help="before help")
    def before_run(limit: int = typer.Option(5, "--limit")) -> None:
        pass

    @after.command("run", help="after help")
    def after_run(limit: int = typer.Option(6, "--limit")) -> None:
        pass

    source = __file_path()
    before_contract = build_cli_contract(before, source_path=source, project_root=PROJECT_ROOT)
    after_contract = build_cli_contract(after, source_path=source, project_root=PROJECT_ROOT)
    assert before_contract["tree_sha256"] != after_contract["tree_sha256"]

    frozen_path = tmp_path / "cli_contract.yaml"
    write_generated_architecture_artifact(frozen_path, before_contract)
    with pytest.raises(CliContractError, match="CLI_CONTRACT_BASELINE_DRIFT"):
        assert_frozen_cli_contract(after_contract, baseline_path=frozen_path)


def test_g2_2_registration_shell_owns_every_app_and_group_relationship() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    registration_tree = ast.parse(REGISTRATION_PATH.read_text(encoding="utf-8"))

    assert canonical_etf_app is etf_app
    assert _typer_app_count(legacy_tree) == 0
    assert _add_typer_count(legacy_tree) == 0
    assert _typer_app_count(registration_tree) == 291
    assert _add_typer_count(registration_tree) == 290
    assert len(SOURCE_PATH.read_text(encoding="utf-8").splitlines()) == 23246
    assert len(REGISTRATION_PATH.read_text(encoding="utf-8").splitlines()) == 1855


@pytest.mark.parametrize(
    ("args", "expected_sha256"),
    [
        (["data", "--help"], "a3699045160cf408407036e9d4b9d6433ad4b7518ccfdd9656a8082525109a3f"),
        (
            ["portfolio", "--help"],
            "5b6a33a94f50471ec8b4f811f8b3ba51060483b9e963a9f0d0e50dae8045d161",
        ),
    ],
)
def test_g2_2_real_cli_help_fixtures_preserve_bytes(
    args: list[str],
    expected_sha256: str,
) -> None:
    result = CliRunner().invoke(etf_app, args, terminal_width=120, color=False)

    assert result.exit_code == 0
    assert result.exception is None
    assert hashlib.sha256(result.stdout.encode("utf-8")).hexdigest() == expected_sha256


def test_g2_3_data_feature_callbacks_and_common_helpers_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    data_names = _function_names(ast.parse(DATA_COMMANDS_PATH.read_text(encoding="utf-8")))
    common_names = _function_names(ast.parse(COMMON_PATH.read_text(encoding="utf-8")))

    callbacks = {"data_ingest_command", "data_validate_command", "features_build_command"}
    helpers = {"parse_date", "resolve_date", "satellite_symbols"}
    assert legacy_names.isdisjoint(callbacks | {f"_{name}" for name in helpers})
    assert callbacks <= data_names
    assert helpers <= common_names


def test_g2_3_data_quality_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DATA_QUALITY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "data_quality_price_freshness_command",
        "data_quality_report_command",
        "data_quality_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_3_operations_callbacks_and_parser_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(OPERATIONS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "ops_dry_run_command",
        "ops_report_command",
        "ops_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks | {"_parse_operations_graph_cadence"})
    assert callbacks | {"parse_operations_graph_cadence"} <= canonical_names


def test_g2_3_evidence_dashboard_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(REPORTING_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "evidence_dashboard_aggregate_command",
        "evidence_dashboard_report_command",
        "evidence_dashboard_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_3_weekly_review_callbacks_and_helpers_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(WEEKLY_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "weekly_review_aggregate_command",
        "weekly_review_generate_command",
        "weekly_review_run_command",
        "weekly_review_validate_command",
    }
    helpers = {"weekly_review_date", "run_weekly_review_generate"}
    assert legacy_names.isdisjoint(callbacks | {f"_{name}" for name in helpers})
    assert callbacks | helpers <= canonical_names


def test_g2_3_parameter_review_callbacks_and_helper_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(PARAMETER_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "parameter_review_aggregate_command",
        "parameter_review_report_command",
        "parameter_review_run_command",
        "parameter_review_validate_command",
    }
    helper = "run_parameter_review_report"
    assert legacy_names.isdisjoint(callbacks | {f"_{helper}_command"})
    assert callbacks | {helper} <= canonical_names


def test_g2_3_satellite_attribution_callbacks_and_shared_helpers_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(SATELLITE_ATTRIBUTION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    common_names = _function_names(ast.parse(COMMON_PATH.read_text(encoding="utf-8")))
    callbacks = {
        "satellite_attribution_build_command",
        "satellite_attribution_report_command",
        "satellite_attribution_validate_command",
    }
    shared_helpers = {"load_optional_json_payload", "quality_metadata"}
    assert legacy_names.isdisjoint(callbacks | {f"_{name}" for name in shared_helpers})
    assert callbacks | {"prepare_satellite_attribution_dataset"} <= canonical_names
    assert shared_helpers <= common_names


def test_g2_3_trend_calibration_callbacks_and_dq_helpers_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(TREND_CALIBRATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    data_quality_names = _function_names(
        ast.parse(DATA_QUALITY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "trend_calibration_run_command",
        "trend_calibration_report_command",
        "trend_calibration_validate_command",
    }
    dq_helpers = {
        "download_manifest_path",
        "marketstack_prices_path",
        "requires_marketstack_prices",
        "run_cached_data_quality_gate",
    }
    assert legacy_names.isdisjoint(callbacks | {f"_{name}" for name in dq_helpers})
    assert callbacks <= canonical_names
    assert dq_helpers <= data_quality_names


def test_g2_3_closeout_selected_groups_have_zero_legacy_definitions_and_imports() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    migrated_callbacks = {
        "data_ingest_command",
        "data_validate_command",
        "features_build_command",
        "data_quality_price_freshness_command",
        "data_quality_report_command",
        "data_quality_validate_command",
        "ops_dry_run_command",
        "ops_report_command",
        "ops_validate_command",
        "evidence_dashboard_aggregate_command",
        "evidence_dashboard_report_command",
        "evidence_dashboard_validate_command",
        "weekly_review_aggregate_command",
        "weekly_review_generate_command",
        "weekly_review_run_command",
        "weekly_review_validate_command",
        "parameter_review_aggregate_command",
        "parameter_review_report_command",
        "parameter_review_run_command",
        "parameter_review_validate_command",
        "satellite_attribution_build_command",
        "satellite_attribution_report_command",
        "satellite_attribution_validate_command",
        "trend_calibration_run_command",
        "trend_calibration_report_command",
        "trend_calibration_validate_command",
    }
    migrated_helpers = {
        "_parse_date",
        "_resolve_date",
        "_satellite_symbols",
        "_parse_operations_graph_cadence",
        "_weekly_review_date",
        "_run_weekly_review_generate",
        "_run_parameter_review_report_command",
        "_load_optional_json_payload",
        "_quality_metadata",
        "_download_manifest_path",
        "_marketstack_prices_path",
        "_requires_marketstack_prices",
        "_run_cached_data_quality_gate",
    }
    migrated_domain_imports = {
        "ai_trading_system.etf_portfolio.data_quality",
        "ai_trading_system.etf_portfolio.parameter_review",
        "ai_trading_system.etf_portfolio.satellite_attribution",
        "ai_trading_system.etf_portfolio.strategy_evidence_dashboard",
        "ai_trading_system.etf_portfolio.trend_calibration",
        "ai_trading_system.etf_portfolio.weekly_review",
    }

    assert len(migrated_callbacks) == 26
    assert len(migrated_helpers) == 13
    assert legacy_names.isdisjoint(migrated_callbacks | migrated_helpers)
    assert _imported_modules(legacy_tree).isdisjoint(migrated_domain_imports)
    assert len(SOURCE_PATH.read_text(encoding="utf-8").splitlines()) == 23246
    assert len(legacy_names) == 681


def test_g2_4_baseline_review_callbacks_and_shared_helper_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(BASELINE_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    common_names = _function_names(ast.parse(COMMON_PATH.read_text(encoding="utf-8")))
    callbacks = {
        "baseline_review_eligibility_command",
        "baseline_review_matrix_command",
        "baseline_review_package_command",
        "baseline_review_capture_decision_command",
        "baseline_review_proposal_draft_command",
        "baseline_review_outcome_command",
        "baseline_review_validate_command",
    }

    assert legacy_names.isdisjoint(callbacks | {"_artifact_stem"})
    assert callbacks <= canonical_names
    assert "artifact_stem" in common_names
    assert "ai_trading_system.etf_portfolio.baseline_review" not in _imported_modules(legacy_tree)


def test_g2_4_shadow_review_callbacks_and_domain_import_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(SHADOW_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "shadow_review_package_command",
        "shadow_review_approve_command",
        "shadow_review_enroll_approved_command",
        "shadow_review_validate_command",
    }

    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert "ai_trading_system.etf_portfolio.shadow_ready_review" not in _imported_modules(
        legacy_tree
    )


def test_g2_4_dynamic_allocation_callbacks_and_helpers_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_ALLOCATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    common_names = _function_names(ast.parse(COMMON_PATH.read_text(encoding="utf-8")))
    callbacks = {
        "dynamic_allocation_decide_command",
        "dynamic_allocation_report_command",
        "dynamic_allocation_validate_command",
    }

    assert legacy_names.isdisjoint(callbacks | {"_json_float_mapping_option", "_mapping_obj"})
    assert callbacks | {"json_float_mapping_option"} <= canonical_names
    assert "mapping_obj" in common_names


def test_g2_4_dynamic_calibration_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_CALIBRATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_calibration_run_command",
        "dynamic_calibration_report_command",
        "dynamic_calibration_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_robustness_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_ROBUSTNESS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {"dynamic_robustness_report_command", "dynamic_robustness_validate_command"}
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_rescue_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_RESCUE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_rescue_run_command",
        "dynamic_rescue_report_command",
        "dynamic_rescue_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v2_review_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V2_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v2_review_package_command",
        "dynamic_v2_review_report_command",
        "dynamic_v2_review_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_rescue_base_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_RESCUE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_rescue_run_command",
        "dynamic_v3_rescue_report_command",
        "dynamic_v3_rescue_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_real_evaluation_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REAL_EVALUATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_rescue_real_evaluate_command",
        "dynamic_v3_rescue_real_report_command",
        "dynamic_v3_rescue_validate_real_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_failure_attribution_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_FAILURE_ATTRIBUTION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_rescue_failure_attribution_command",
        "dynamic_v3_rescue_failure_attribution_report_command",
        "dynamic_v3_rescue_validate_attribution_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_sweep_config_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SWEEP_CONFIG_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_sweep_config_validate_command",
        "dynamic_v3_sweep_config_preview_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_sweep_runtime_callbacks_and_helper_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SWEEP_RUNTIME_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_sweep_profile_list_command",
        "dynamic_v3_sweep_profile_validate_command",
        "dynamic_v3_sweep_run_profile_command",
        "dynamic_v3_sweep_run_command",
        "dynamic_v3_sweep_status_command",
        "dynamic_v3_sweep_validate_command",
        "dynamic_v3_sweep_leaderboard_command",
        "dynamic_v3_sweep_report_command",
        "resolve_dynamic_v3_sweep_id",
    }
    assert legacy_names.isdisjoint(callbacks | {"_resolve_dynamic_v3_sweep_id"})
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_data_audit_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_DATA_AUDIT_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_data_audit_run_command",
        "dynamic_v3_data_audit_report_command",
        "dynamic_v3_validate_data_audit_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_data_provenance_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_DATA_PROVENANCE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_data_provenance_inspect_price_cache_command",
        "dynamic_v3_data_provenance_repair_price_manifest_command",
        "dynamic_v3_data_provenance_validate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_window_audit_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_WINDOW_AUDIT_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_window_audit_run_command",
        "dynamic_v3_window_audit_report_command",
        "dynamic_v3_window_audit_inspect_artifact_command",
        "dynamic_v3_validate_window_audit_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_injection_audit_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_INJECTION_AUDIT_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_injection_audit_run_command",
        "dynamic_v3_injection_audit_report_command",
        "dynamic_v3_validate_injection_audit_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_weight_path_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_WEIGHT_PATH_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_weight_path_validate_command",
        "dynamic_v3_weight_path_report_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_candidate_evidence_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CANDIDATE_EVIDENCE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_candidate_report_command",
        "dynamic_v3_candidate_attribution_command",
        "dynamic_v3_validate_candidate_attribution_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_validation_evidence_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_VALIDATION_EVIDENCE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_walk_forward_select_run_command",
        "dynamic_v3_walk_forward_selection_report_command",
        "dynamic_v3_validate_walk_forward_selection_command",
        "dynamic_v3_overfit_run_command",
        "dynamic_v3_overfit_report_command",
        "dynamic_v3_validate_overfit_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_legacy_validation_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_LEGACY_VALIDATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_walk_forward_run_command",
        "dynamic_v3_walk_forward_report_command",
        "dynamic_v3_validate_walk_forward_command",
        "dynamic_v3_robustness_run_command",
        "dynamic_v3_robustness_report_command",
        "dynamic_v3_validate_robustness_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_shadow_registry_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SHADOW_REGISTRY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_shadow_register_command",
        "dynamic_v3_shadow_list_command",
        "dynamic_v3_shadow_report_command",
        "dynamic_v3_validate_shadow_registry_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_research_control_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_RESEARCH_CONTROL_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_governance_validate_command",
        "dynamic_v3_governance_report_command",
        "dynamic_v3_governance_diff_command",
        "dynamic_v3_research_index_build_command",
        "dynamic_v3_research_query_command",
        "dynamic_v3_research_compare_command",
        "dynamic_v3_research_history_command",
        "dynamic_v3_artifacts_latest_command",
        "dynamic_v3_artifacts_validate_command",
        "dynamic_v3_artifacts_repair_latest_command",
        "dynamic_v3_artifacts_stale_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_observation_lifecycle_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OBSERVATION_LIFECYCLE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_shadow_monitor_run_command",
        "dynamic_v3_shadow_monitor_report_command",
        "dynamic_v3_validate_shadow_monitor_command",
        "dynamic_v3_schedule_observe_command",
        "dynamic_v3_promotion_review_command",
        "dynamic_v3_promotion_pack_command",
        "dynamic_v3_validate_promotion_pack_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_evidence_readiness_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_EVIDENCE_READINESS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_evidence_summary_run_command",
        "dynamic_v3_evidence_summary_report_command",
        "dynamic_v3_validate_evidence_summary_command",
        "dynamic_v3_medium_real_report_command",
        "dynamic_v3_validate_medium_real_command",
        "dynamic_v3_regime_coverage_run_command",
        "dynamic_v3_regime_coverage_report_command",
        "dynamic_v3_validate_regime_coverage_command",
        "dynamic_v3_candidate_interpretation_pack_command",
        "dynamic_v3_candidate_interpretation_report_command",
        "dynamic_v3_validate_interpretation_pack_command",
        "dynamic_v3_observe_pool_build_command",
        "dynamic_v3_observe_pool_report_command",
        "dynamic_v3_validate_observe_pool_command",
        "dynamic_v3_overnight_readiness_run_command",
        "dynamic_v3_overnight_readiness_report_command",
        "dynamic_v3_validate_overnight_readiness_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_evidence_governance_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_EVIDENCE_GOVERNANCE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_research_decision_run_command",
        "dynamic_v3_research_decision_report_command",
        "dynamic_v3_validate_research_decision_command",
        "dynamic_v3_evidence_diagnosis_run_command",
        "dynamic_v3_evidence_diagnosis_report_command",
        "dynamic_v3_validate_evidence_diagnosis_command",
        "dynamic_v3_gate_impact_run_command",
        "dynamic_v3_gate_impact_report_command",
        "dynamic_v3_validate_gate_impact_command",
        "dynamic_v3_gate_policy_validate_command",
        "dynamic_v3_gate_policy_report_command",
        "dynamic_v3_gate_policy_apply_command",
        "dynamic_v3_candidate_recovery_run_command",
        "dynamic_v3_candidate_recovery_report_command",
        "dynamic_v3_validate_candidate_recovery_command",
        "dynamic_v3_observe_pool_rebuild_command",
        "dynamic_v3_research_decision_update_command",
        "dynamic_v3_research_decision_update_report_command",
        "dynamic_v3_validate_research_decision_update_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_candidate_observation_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CANDIDATE_OBSERVATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_shortlist_build_command",
        "dynamic_v3_shortlist_report_command",
        "dynamic_v3_validate_shortlist_command",
        "dynamic_v3_candidate_cluster_run_command",
        "dynamic_v3_candidate_cluster_report_command",
        "dynamic_v3_validate_candidate_cluster_command",
        "dynamic_v3_shadow_shortlist_build_command",
        "dynamic_v3_shadow_shortlist_report_command",
        "dynamic_v3_validate_shadow_shortlist_command",
        "dynamic_v3_shadow_monitor_activate_command",
        "dynamic_v3_shadow_monitor_run_from_shortlist_command",
        "dynamic_v3_shadow_monitor_run_report_command",
        "dynamic_v3_validate_shadow_monitor_run_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_portfolio_intake_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_PORTFOLIO_INTAKE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_portfolio_snapshot_validate_command",
        "dynamic_v3_portfolio_snapshot_report_command",
        "dynamic_v3_portfolio_snapshot_normalize_command",
        "dynamic_v3_manual_portfolio_validate_command",
        "dynamic_v3_manual_portfolio_normalize_command",
        "dynamic_v3_manual_portfolio_report_command",
        "dynamic_v3_validate_manual_portfolio_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_portfolio_risk_controls_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_PORTFOLIO_RISK_CONTROLS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_portfolio_exposure_validate_command",
        "dynamic_v3_portfolio_exposure_report_command",
        "dynamic_v3_validate_portfolio_exposure_command",
        "dynamic_v3_position_drift_run_command",
        "dynamic_v3_position_drift_report_command",
        "dynamic_v3_validate_position_drift_command",
        "dynamic_v3_execution_guardrails_check_command",
        "dynamic_v3_execution_guardrails_report_command",
        "dynamic_v3_validate_execution_guardrails_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_manual_execution_review_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_MANUAL_EXECUTION_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_manual_execution_review_pack_command",
        "dynamic_v3_manual_execution_review_report_command",
        "dynamic_v3_validate_manual_execution_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_real_snapshot_intake_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REAL_SNAPSHOT_INTAKE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_real_snapshot_template_command",
        "dynamic_v3_real_snapshot_lint_command",
        "dynamic_v3_real_snapshot_intake_command",
        "dynamic_v3_real_snapshot_report_command",
        "dynamic_v3_validate_real_snapshot_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_real_snapshot_dry_run_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REAL_SNAPSHOT_DRY_RUN_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_real_snapshot_dry_run_command",
        "dynamic_v3_real_snapshot_dry_run_report_command",
        "dynamic_v3_validate_real_snapshot_dry_run_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_real_execution_owner_review_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REAL_EXECUTION_OWNER_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_real_execution_owner_review_create_command",
        "dynamic_v3_real_execution_owner_review_record_command",
        "dynamic_v3_real_execution_owner_review_report_command",
        "dynamic_v3_validate_real_execution_owner_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_real_snapshot_paper_action_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REAL_SNAPSHOT_PAPER_ACTION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_real_snapshot_paper_action_apply_command",
        "dynamic_v3_real_snapshot_paper_action_report_command",
        "dynamic_v3_validate_real_snapshot_paper_action_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_weekly_real_snapshot_review_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_WEEKLY_REAL_SNAPSHOT_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_weekly_real_snapshot_review_run_command",
        "dynamic_v3_weekly_real_snapshot_review_report_command",
        "dynamic_v3_validate_weekly_real_snapshot_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_position_advisory_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_POSITION_ADVISORY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_position_advisory_run_command",
        "dynamic_v3_position_advisory_report_command",
        "dynamic_v3_validate_position_advisory_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_position_advisory_daily_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_POSITION_ADVISORY_DAILY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_position_advisory_daily_run_command",
        "dynamic_v3_position_advisory_daily_report_command",
        "dynamic_v3_validate_position_advisory_daily_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_consensus_drift_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CONSENSUS_DRIFT_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_consensus_drift_run_command",
        "dynamic_v3_consensus_drift_report_command",
        "dynamic_v3_validate_consensus_drift_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_owner_review_journal_callbacks_leave_legacy_root() -> None:
    legacy_names = _function_names(ast.parse(SOURCE_PATH.read_text(encoding="utf-8")))
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OWNER_REVIEW_JOURNAL_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_owner_review_create_command",
        "dynamic_v3_owner_review_list_command",
        "dynamic_v3_owner_review_report_command",
        "dynamic_v3_owner_review_record_decision_command",
        "dynamic_v3_validate_owner_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names


def test_g2_4_dynamic_v3_paper_portfolio_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_PAPER_PORTFOLIO_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_paper_portfolio_init_command",
        "dynamic_v3_paper_portfolio_apply_review_command",
        "dynamic_v3_paper_portfolio_state_command",
        "dynamic_v3_paper_portfolio_report_command",
        "dynamic_v3_validate_paper_portfolio_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "init_paper_portfolio",
            "apply_owner_review_to_paper_portfolio",
            "paper_portfolio_state_payload",
            "paper_portfolio_report_payload",
            "validate_paper_portfolio_artifact",
        }
    )


def test_g2_4_dynamic_v3_advisory_outcome_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_ADVISORY_OUTCOME_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_advisory_outcome_track_command",
        "dynamic_v3_advisory_outcome_update_command",
        "dynamic_v3_advisory_outcome_report_command",
        "dynamic_v3_validate_advisory_outcome_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "advisory_outcome_report_payload",
            "track_advisory_outcome",
            "update_advisory_outcome",
            "validate_advisory_outcome_artifact",
        }
    )


def test_g2_4_dynamic_v3_owner_attribution_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OWNER_ATTRIBUTION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_owner_attribution_run_command",
        "dynamic_v3_owner_attribution_report_command",
        "dynamic_v3_validate_owner_attribution_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "owner_attribution_report_payload",
            "run_owner_attribution",
            "validate_owner_attribution_artifact",
        }
    )


def test_g2_4_dynamic_v3_shadow_aging_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SHADOW_AGING_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_shadow_aging_run_command",
        "dynamic_v3_shadow_aging_report_command",
        "dynamic_v3_validate_shadow_aging_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "run_shadow_aging",
            "shadow_aging_report_payload",
            "validate_shadow_aging_artifact",
        }
    )


def test_g2_4_dynamic_v3_weekly_advisory_review_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_WEEKLY_ADVISORY_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_weekly_advisory_review_run_command",
        "dynamic_v3_weekly_advisory_review_report_command",
        "dynamic_v3_validate_weekly_advisory_review_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "run_weekly_advisory_review",
            "weekly_advisory_review_report_payload",
            "validate_weekly_advisory_review_artifact",
        }
    )


def test_g2_4_dynamic_v3_replay_inventory_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REPLAY_INVENTORY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_replay_inventory_build_command",
        "dynamic_v3_replay_inventory_report_command",
        "dynamic_v3_validate_replay_inventory_command",
    }
    legacy_imported_names = _imported_names(legacy_tree)
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert legacy_imported_names.isdisjoint(
        {
            "build_replay_inventory",
            "replay_inventory_report_payload",
            "validate_replay_inventory_artifact",
        }
    )


def test_g2_4_dynamic_v3_historical_replay_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_HISTORICAL_REPLAY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_historical_replay_run_command",
        "dynamic_v3_historical_replay_report_command",
        "dynamic_v3_validate_historical_replay_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "historical_replay_report_payload",
            "run_historical_replay",
            "validate_historical_replay_artifact",
        }
    )


def test_g2_4_dynamic_v3_backfilled_outcome_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKFILLED_OUTCOME_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backfill_outcome_run_command",
        "dynamic_v3_backfill_outcome_report_command",
        "dynamic_v3_validate_backfill_outcome_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backfill_outcome_report_payload",
            "run_backfill_outcome",
            "validate_backfill_outcome_artifact",
        }
    )


def test_g2_4_dynamic_v3_historical_paper_sim_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_HISTORICAL_PAPER_SIM_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_historical_paper_sim_run_command",
        "dynamic_v3_historical_paper_sim_report_command",
        "dynamic_v3_validate_historical_paper_sim_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "historical_paper_sim_report_payload",
            "run_historical_paper_sim",
            "validate_historical_paper_sim_artifact",
        }
    )


def test_g2_4_dynamic_v3_replay_performance_review_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REPLAY_PERFORMANCE_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_replay_performance_review_run_command",
        "dynamic_v3_replay_performance_review_report_command",
        "dynamic_v3_validate_replay_performance_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "replay_performance_review_report_payload",
            "run_replay_performance_review",
            "validate_replay_performance_review_artifact",
        }
    )


def test_g2_4_dynamic_v3_replay_diagnosis_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REPLAY_DIAGNOSIS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_replay_diagnosis_run_command",
        "dynamic_v3_replay_diagnosis_report_command",
        "dynamic_v3_validate_replay_diagnosis_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "replay_diagnosis_report_payload",
            "run_replay_diagnosis",
            "validate_replay_diagnosis_artifact",
        }
    )


def test_g2_4_dynamic_v3_backfill_repair_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKFILL_REPAIR_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backfill_repair_run_command",
        "dynamic_v3_backfill_repair_report_command",
        "dynamic_v3_validate_backfill_repair_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backfill_repair_report_payload",
            "run_backfill_repair",
            "validate_backfill_repair_artifact",
        }
    )


def test_g2_4_dynamic_v3_variant_comparison_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_VARIANT_COMPARISON_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_variant_comparison_run_command",
        "dynamic_v3_variant_comparison_report_command",
        "dynamic_v3_validate_variant_comparison_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "run_variant_comparison",
            "validate_variant_comparison_artifact",
            "variant_comparison_report_payload",
        }
    )


def test_g2_4_dynamic_v3_rule_calibration_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_RULE_CALIBRATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_rule_calibration_run_command",
        "dynamic_v3_rule_calibration_report_command",
        "dynamic_v3_validate_rule_calibration_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "rule_calibration_report_payload",
            "run_rule_calibration",
            "validate_rule_calibration_artifact",
        }
    )


def test_g2_4_dynamic_v3_replay_forward_bridge_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REPLAY_FORWARD_BRIDGE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_replay_forward_bridge_run_command",
        "dynamic_v3_replay_forward_bridge_report_command",
        "dynamic_v3_validate_replay_forward_bridge_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "replay_forward_bridge_report_payload",
            "run_replay_forward_bridge",
            "validate_replay_forward_bridge_artifact",
        }
    )


def test_g2_4_dynamic_v3_outcome_due_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OUTCOME_DUE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_outcome_due_scan_command",
        "dynamic_v3_outcome_due_report_command",
        "dynamic_v3_outcome_due_update_ready_command",
        "dynamic_v3_validate_outcome_due_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "outcome_due_report_payload",
            "outcome_due_update_ready",
            "run_outcome_due_scan",
            "validate_outcome_due_artifact",
        }
    )


def test_g2_4_dynamic_v3_replay_sample_expansion_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_REPLAY_SAMPLE_EXPANSION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_replay_sample_expansion_run_command",
        "dynamic_v3_replay_sample_expansion_report_command",
        "dynamic_v3_validate_replay_sample_expansion_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "replay_sample_expansion_report_payload",
            "run_replay_sample_expansion",
            "validate_replay_sample_expansion_artifact",
        }
    )


def test_g2_4_dynamic_v3_outcome_dashboard_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OUTCOME_DASHBOARD_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_outcome_dashboard_build_command",
        "dynamic_v3_outcome_dashboard_report_command",
        "dynamic_v3_validate_outcome_dashboard_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "build_outcome_dashboard",
            "outcome_dashboard_report_payload",
            "validate_outcome_dashboard_artifact",
        }
    )


def test_g2_4_dynamic_v3_limited_vs_notrade_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_LIMITED_VS_NOTRADE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_limited_vs_notrade_run_command",
        "dynamic_v3_limited_vs_notrade_report_command",
        "dynamic_v3_validate_limited_vs_notrade_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "limited_vs_notrade_report_payload",
            "run_limited_vs_notrade_evaluation",
            "validate_limited_vs_notrade_artifact",
        }
    )


def test_g2_4_dynamic_v3_consensus_risk_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CONSENSUS_RISK_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_consensus_risk_run_command",
        "dynamic_v3_consensus_risk_report_command",
        "dynamic_v3_validate_consensus_risk_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "consensus_risk_report_payload",
            "run_consensus_risk_review",
            "validate_consensus_risk_artifact",
        }
    )


def test_g2_4_dynamic_v3_outcome_update_review_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OUTCOME_UPDATE_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_outcome_update_review_run_command",
        "dynamic_v3_outcome_update_review_report_command",
        "dynamic_v3_validate_outcome_update_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "outcome_update_review_report_payload",
            "run_outcome_update_review",
            "validate_outcome_update_review_artifact",
        }
    )


def test_g2_4_dynamic_v3_outcome_update_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_OUTCOME_UPDATE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_outcome_update_run_command",
        "dynamic_v3_outcome_update_report_command",
        "dynamic_v3_validate_outcome_update_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "outcome_update_report_payload",
            "run_outcome_update",
            "validate_outcome_update_artifact",
        }
    )


def test_g2_4_dynamic_v3_rolling_evidence_refresh_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_ROLLING_EVIDENCE_REFRESH_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_rolling_evidence_refresh_run_command",
        "dynamic_v3_rolling_evidence_refresh_report_command",
        "dynamic_v3_validate_rolling_evidence_refresh_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "rolling_evidence_refresh_report_payload",
            "run_rolling_evidence_refresh",
            "validate_rolling_evidence_refresh_artifact",
        }
    )


def test_g2_4_dynamic_v3_evidence_trend_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_EVIDENCE_TREND_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_evidence_trend_run_command",
        "dynamic_v3_evidence_trend_report_command",
        "dynamic_v3_validate_evidence_trend_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "evidence_trend_report_payload",
            "run_evidence_trend",
            "validate_evidence_trend_artifact",
        }
    )


def test_g2_4_dynamic_v3_forward_outcome_decision_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_FORWARD_OUTCOME_DECISION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_forward_outcome_decision_run_command",
        "dynamic_v3_forward_outcome_decision_report_command",
        "dynamic_v3_validate_forward_outcome_decision_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "forward_outcome_decision_report_payload",
            "run_forward_outcome_decision",
            "validate_forward_outcome_decision_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_event_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_EVENTS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_config_validate_command",
        "dynamic_v3_backtest_sim_event_generate_command",
        "dynamic_v3_backtest_sim_event_report_command",
        "dynamic_v3_validate_backtest_sim_events_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_event_report_payload",
            "generate_backtest_sim_events",
            "validate_backtest_sim_events_artifact",
            "validate_backtest_simulation_config",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_variant_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_VARIANTS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_variants_generate_command",
        "dynamic_v3_backtest_sim_variants_report_command",
        "dynamic_v3_validate_backtest_sim_variants_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_variant_report_payload",
            "generate_backtest_sim_variants",
            "validate_backtest_sim_variants_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_outcome_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_OUTCOME_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_outcome_run_command",
        "dynamic_v3_backtest_sim_outcome_report_command",
        "dynamic_v3_validate_backtest_sim_outcome_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_outcome_report_payload",
            "run_backtest_sim_outcome",
            "validate_backtest_sim_outcome_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_paper_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_PAPER_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_paper_run_command",
        "dynamic_v3_backtest_sim_paper_report_command",
        "dynamic_v3_validate_backtest_sim_paper_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_paper_report_payload",
            "run_backtest_sim_paper",
            "validate_backtest_sim_paper_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_regime_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_REGIME_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_regime_review_command",
        "dynamic_v3_backtest_sim_regime_report_command",
        "dynamic_v3_validate_backtest_sim_regime_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_regime_report_payload",
            "run_backtest_sim_regime_review",
            "validate_backtest_sim_regime_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_sensitivity_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_SENSITIVITY_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_sensitivity_run_command",
        "dynamic_v3_backtest_sim_sensitivity_report_command",
        "dynamic_v3_validate_backtest_sim_sensitivity_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_sensitivity_report_payload",
            "run_backtest_sim_sensitivity",
            "validate_backtest_sim_sensitivity_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_calibration_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_CALIBRATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_calibration_pack_command",
        "dynamic_v3_backtest_sim_calibration_report_command",
        "dynamic_v3_validate_backtest_sim_calibration_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_calibration_report_payload",
            "run_backtest_sim_calibration_pack",
            "validate_backtest_sim_calibration_artifact",
        }
    )


def test_g2_4_dynamic_v3_backtest_sim_forward_bridge_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_BACKTEST_SIM_FORWARD_BRIDGE_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_backtest_sim_forward_bridge_command",
        "dynamic_v3_backtest_sim_forward_bridge_report_command",
        "dynamic_v3_validate_backtest_sim_forward_bridge_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "backtest_sim_forward_bridge_report_payload",
            "run_backtest_sim_forward_bridge",
            "validate_backtest_sim_forward_bridge_artifact",
        }
    )


def test_g2_4_dynamic_v3_sim_interpretation_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SIM_INTERPRETATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_sim_interpretation_run_command",
        "dynamic_v3_sim_interpretation_report_command",
        "dynamic_v3_validate_sim_interpretation_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "run_sim_interpretation",
            "sim_interpretation_report_payload",
            "validate_sim_interpretation_artifact",
        }
    )


def test_g2_4_dynamic_v3_sim_risk_return_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SIM_RISK_RETURN_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_sim_risk_return_run_command",
        "dynamic_v3_sim_risk_return_report_command",
        "dynamic_v3_validate_sim_risk_return_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "run_sim_risk_return",
            "sim_risk_return_report_payload",
            "validate_sim_risk_return_artifact",
        }
    )


def test_g2_4_dynamic_v3_sim_defensive_validation_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_SIM_DEFENSIVE_VALIDATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_sim_defensive_validation_run_command",
        "dynamic_v3_sim_defensive_validation_report_command",
        "dynamic_v3_validate_sim_defensive_validation_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "run_sim_defensive_validation",
            "sim_defensive_validation_report_payload",
            "validate_sim_defensive_validation_artifact",
        }
    )


def test_g2_4_dynamic_v3_advisory_proposal_review_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_ADVISORY_PROPOSAL_REVIEW_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_advisory_proposal_review_run_command",
        "dynamic_v3_advisory_proposal_review_report_command",
        "dynamic_v3_validate_advisory_proposal_review_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "advisory_proposal_review_report_payload",
            "run_advisory_proposal_review",
            "validate_advisory_proposal_review_artifact",
        }
    )


def test_g2_4_dynamic_v3_forward_confirmation_plan_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_FORWARD_CONFIRMATION_PLAN_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_forward_confirmation_plan_run_command",
        "dynamic_v3_forward_confirmation_plan_report_command",
        "dynamic_v3_validate_forward_confirmation_plan_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "forward_confirmation_plan_report_payload",
            "run_forward_confirmation_plan",
            "validate_forward_confirmation_plan_artifact",
        }
    )


def test_g2_4_dynamic_v3_confirmation_targets_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CONFIRMATION_TARGETS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_confirmation_targets_register_command",
        "dynamic_v3_confirmation_targets_list_command",
        "dynamic_v3_confirmation_targets_report_command",
        "dynamic_v3_validate_confirmation_targets_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "confirmation_targets_report_payload",
            "list_confirmation_targets",
            "register_confirmation_targets",
            "validate_confirmation_targets_artifact",
        }
    )


def test_g2_4_dynamic_v3_confirmation_progress_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CONFIRMATION_PROGRESS_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_confirmation_progress_update_command",
        "dynamic_v3_confirmation_progress_report_command",
        "dynamic_v3_validate_confirmation_progress_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "confirmation_progress_report_payload",
            "update_confirmation_progress",
            "validate_confirmation_progress_artifact",
        }
    )


def test_g2_4_dynamic_v3_confirmation_evaluation_callbacks_leave_legacy_root() -> None:
    legacy_tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    legacy_names = _function_names(legacy_tree)
    canonical_names = _function_names(
        ast.parse(DYNAMIC_V3_CONFIRMATION_EVALUATION_COMMANDS_PATH.read_text(encoding="utf-8"))
    )
    callbacks = {
        "dynamic_v3_confirmation_evaluate_run_command",
        "dynamic_v3_confirmation_evaluate_report_command",
        "dynamic_v3_validate_confirmation_evaluate_command",
    }
    assert legacy_names.isdisjoint(callbacks)
    assert callbacks <= canonical_names
    assert _imported_names(legacy_tree).isdisjoint(
        {
            "confirmation_evaluation_report_payload",
            "run_confirmation_evaluation",
            "validate_confirmation_evaluation_artifact",
        }
    )


def __file_path() -> Path:
    return Path(__file__).resolve()


def _typer_app_count(tree: ast.Module) -> int:
    return sum(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and isinstance(node.value.func.value, ast.Name)
        and node.value.func.value.id == "typer"
        and node.value.func.attr == "Typer"
        for node in tree.body
    )


def _add_typer_count(tree: ast.Module) -> int:
    return sum(
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "add_typer"
        for node in tree.body
    )


def _function_names(tree: ast.Module) -> set[str]:
    return {
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imported_modules(tree: ast.Module) -> set[str]:
    return {
        node.module
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }


def _imported_names(tree: ast.Module) -> set[str]:
    return {
        alias.name for node in tree.body if isinstance(node, ast.ImportFrom) for alias in node.names
    }
