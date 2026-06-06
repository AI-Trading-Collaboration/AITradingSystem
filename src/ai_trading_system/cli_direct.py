from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace

import typer

from ai_trading_system import cli
from ai_trading_system.cli_commands import data as data_cli
from ai_trading_system.cli_commands import data_cache as data_cache_cli
from ai_trading_system.cli_commands import docs as docs_cli
from ai_trading_system.cli_commands import etf_portfolio as etf_cli
from ai_trading_system.cli_commands import feedback as feedback_cli
from ai_trading_system.cli_commands import fundamentals as fundamentals_cli
from ai_trading_system.cli_commands import ops as ops_cli
from ai_trading_system.cli_commands import parameters as parameters_cli
from ai_trading_system.cli_commands import pit_snapshots as pit_snapshots_cli
from ai_trading_system.cli_commands import portfolio as portfolio_cli
from ai_trading_system.cli_commands import reports as reports_cli
from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.cli_commands import security as security_cli
from ai_trading_system.cli_commands import signals as signals_cli
from ai_trading_system.cli_commands import valuation as valuation_cli


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        _dispatch(args)
    except typer.Exit as exc:
        return int(exc.exit_code or 0)
    except typer.BadParameter as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _dispatch(args: list[str]) -> None:
    if args[:1] == ["download-data"]:
        data_cache_cli.download_data(
            start=_option(args, "--start", "2018-01-01"),
            end=_option(args, "--end"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:1] == ["validate-data"]:
        data_cache_cli.validate_data(
            as_of=_option(args, "--as-of"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:2] == ["data", "diagnose-backtest-inputs"]:
        data_cli.data_diagnose_backtest_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
        )
        return
    if args[:2] == ["data", "repair-backtest-inputs"]:
        data_cli.data_repair_backtest_inputs_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
            price_only=_flag(args, "--price-only"),
            symbols=_values_after_option(args, "--symbols"),
            price_provider=_option(args, "--price-provider", "fmp") or "fmp",
            fmp_api_key_env=_option(args, "--fmp-api-key-env", "FMP_API_KEY") or "FMP_API_KEY",
        )
        return
    if args[:2] == ["data", "freshness"]:
        data_cli.data_freshness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            market=_option(args, "--market", "US") or "US",
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["data", "recover-freshness"]:
        data_cli.data_recover_freshness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            refresh_config_path=_path_option_with_default(
                args,
                "--refresh-config",
                data_cli.DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
            ),
            freshness_config_path=_path_option_with_default(
                args,
                "--freshness-config",
                data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
            ),
        )
        return
    if args[:2] == ["signals", "build-snapshot"]:
        signals_cli.signals_build_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
            dry_run=_flag(args, "--dry-run"),
            price_derived_only=_flag(args, "--price-derived-only"),
        )
        return
    if args[:2] == ["signals", "validate-snapshot"]:
        signals_cli.signals_validate_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "ablation"]:
        signals_cli.signals_ablation_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_signal_ablation_config_option(args, "--config"),
            signals=_values_after_option(args, "--signals"),
            dry_run=_flag(args, "--dry-run"),
            debug=_flag(args, "--debug"),
        )
        return
    if args[:2] == ["signals", "calibrate"]:
        signals_cli.signals_calibrate_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_signal_calibration_config_option(args, "--config"),
            profile=_option(args, "--profile"),
            profiles=_values_after_option(args, "--profiles"),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["signals", "explain-ablation"]:
        signals_cli.signals_explain_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "validate-ablation"]:
        signals_cli.signals_validate_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["pit-snapshots", "fetch-fmp-forward"]:
        pit_snapshots_cli.fetch_fmp_forward_pit_command(
            as_of=_option(args, "--as-of"),
            continue_on_failure=_flag(args, "--continue-on-failure"),
        )
        return
    if args[:2] == ["pit-snapshots", "build-manifest"]:
        pit_snapshots_cli.build_pit_snapshot_manifest_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["pit-snapshots", "validate"]:
        pit_snapshots_cli.validate_pit_snapshots_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "download-sec-companyfacts"]:
        fundamentals_cli.download_sec_companyfacts_command(
            user_agent=_option(args, "--user-agent") or os.getenv("SEC_USER_AGENT")
        )
        return
    if args[:2] == ["fundamentals", "extract-sec-metrics"]:
        fundamentals_cli.extract_sec_metrics_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "merge-tsm-ir-sec-metrics"]:
        fundamentals_cli.merge_tsm_ir_sec_metrics(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "validate-sec-metrics"]:
        fundamentals_cli.validate_sec_metrics_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["valuation", "fetch-fmp"]:
        valuation_cli.fetch_fmp_valuations(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["score-daily", "backfill-baseline"]:
        raise typer.BadParameter(
            "daily-run direct dispatcher 不支持 score-daily backfill-baseline；请使用主 CLI。"
        )
    if args[:1] == ["score-daily"]:
        max_candidates = _option(args, "--risk-event-openai-precheck-max-candidates")
        cli.score_daily(
            as_of=_option(args, "--as-of"),
            risk_event_openai_precheck_max_candidates=(
                int(max_candidates) if max_candidates is not None else None
            ),
            risk_event_openai_precheck=not _flag(args, "--skip-risk-event-openai-precheck"),
            llm_request_profile=_option(
                args,
                "--llm-request-profile",
                "risk_event_daily_official_precheck",
            )
            or "risk_event_daily_official_precheck",
            run_id=_option(args, "--run-id"),
            risk_event_openai_precheck_visibility_cutoff=_option(
                args,
                "--risk-event-openai-precheck-visibility-cutoff",
            ),
        )
        return
    if args[:2] == ["feedback", "optimize-market-feedback"]:
        feedback_cli.optimize_market_feedback_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "evaluate-parameter-governance"]:
        feedback_cli.evaluate_parameter_governance_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "loop-review"]:
        feedback_cli.feedback_loop_review_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "investment-review"]:
        reports_cli.investment_periodic_review_command(
            period=_option(args, "--period", "weekly") or "weekly",
            as_of=_option(args, "--as-of"),
        )
        return
    if args[:2] == ["reports", "dashboard"]:
        reports_cli.evidence_dashboard_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "calculation-explainers"]:
        reports_cli.calculation_explainers_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["portfolio", "track-candidate"]:
        portfolio_cli.portfolio_track_candidate_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            review_path=_optional_path(args, "--review"),
            config_path=_path_option_with_default(
                args,
                "--config",
                portfolio_cli.DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["portfolio", "review-tracking"]:
        portfolio_cli.portfolio_review_tracking_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            candidate_profile=_option(args, "--candidate"),
            window=_option(args, "--window"),
            show_window_progress=_flag(args, "--show-window-progress"),
            config_path=_path_option_with_default(
                args,
                "--config",
                portfolio_cli.DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:3] == ["etf", "forward", "update"]:
        etf_cli.forward_update_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            config_path=_path_option_with_default(
                args,
                "--config-path",
                etf_cli.DEFAULT_ETF_FORWARD_CONFIG_PATH,
            ),
            registry_path=_path_option_with_default(
                args,
                "--registry-path",
                etf_cli.DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
            ),
            decision_ledger_path=_path_option_with_default(
                args,
                "--decision-ledger-path",
                etf_cli.DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
            ),
            prices_path=_path_option_with_default(
                args,
                "--prices-path",
                etf_cli.DEFAULT_ETF_PRICE_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
            ),
        )
        return
    if args[:3] == ["etf", "forward", "dashboard"]:
        etf_cli.forward_dashboard_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            registry_path=_path_option_with_default(
                args,
                "--registry-path",
                etf_cli.DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
            ),
            update_dir=_path_option_with_default(
                args,
                "--update-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
            ),
        )
        return
    if args[:3] == ["etf", "forward", "watchlist"]:
        etf_cli.forward_watchlist_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            dashboard_dir=_path_option_with_default(
                args,
                "--dashboard-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist",
            ),
        )
        return
    if args[:3] == ["etf", "ops", "dry-run"]:
        etf_cli.ops_dry_run_command(
            cadence=_option(args, "--cadence", "daily") or "daily",
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_path=_optional_path(args, "--output-path"),
            include_optional=not _flag(args, "--skip-optional"),
            no_write=_flag(args, "--no-write"),
        )
        return
    if args[:3] == ["etf", "ops", "validate"]:
        etf_cli.ops_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "ops", "report"]:
        etf_cli.ops_report_command(
            cadence=_option(args, "--cadence", "daily") or "daily",
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_OPERATIONS_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
            include_optional=not _flag(args, "--skip-optional"),
        )
        return
    if args[:3] == ["etf", "data-quality", "report"]:
        etf_cli.data_quality_report_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            prices_path=_path_option_with_default(
                args,
                "--prices-path",
                etf_cli.DEFAULT_ETF_PRICE_PATH,
            ),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "data-quality", "validate"]:
        etf_cli.data_quality_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "aggregate"]:
        etf_cli.evidence_dashboard_aggregate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_index_path=_optional_path(args, "--report-index-path"),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "report"]:
        etf_cli.evidence_dashboard_report_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_index_path=_optional_path(args, "--report-index-path"),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "validate"]:
        etf_cli.evidence_dashboard_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:2] == ["parameters", "shadow-backtest"]:
        parameters_cli.parameters_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "validate-shadow-backtest"]:
        parameters_cli.validate_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "tune-weights-stable"]:
        parameters_cli.parameters_tune_weights_stable_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
            ),
            portfolio_profile=_option(args, "--portfolio-profile"),
            signals=_values_after_option(args, "--signals"),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "diagnose-weight-stability-inputs"]:
        parameters_cli.diagnose_weight_stability_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "recover-weight-stability-inputs"]:
        parameters_cli.recover_weight_stability_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "validate-weight-stability-readiness"]:
        parameters_cli.validate_weight_stability_readiness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "validate-weight-stability"]:
        parameters_cli.validate_weight_stability_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "explain-weight-stability"]:
        parameters_cli.explain_weight_stability_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["reports", "shadow-parameter-backtest"]:
        reports_cli.shadow_parameter_backtest_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "parameter-promotion"]:
        reports_cli.parameter_promotion_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "weight-stability"]:
        reports_cli.weight_stability_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "weight-stability-readiness"]:
        reports_cli.weight_stability_readiness_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-snapshot"]:
        reports_cli.signal_snapshot_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-ablation"]:
        reports_cli.signal_ablation_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-calibration"]:
        reports_cli.signal_calibration_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "reader-brief"]:
        reports_cli.reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "validate-reader-brief"]:
        reports_cli.validate_reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "score-change-attribution"]:
        reports_cli.score_change_attribution_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "market-panel"]:
        reports_cli.market_panel_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "research-governance-summary"]:
        reports_cli.research_governance_summary_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "portfolio-tracking-review"]:
        reports_cli.portfolio_tracking_review_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "index"]:
        reports_cli.report_index_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["docs", "report-contract"]:
        docs_cli.documentation_contract_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["sec-pit", "shadow-observe"]:
        sec_pit_cli.shadow_observe_command(latest=_flag(args, "--latest"))
        return
    if args[:2] == ["sec-pit", "shadow-monitor"]:
        sec_pit_cli.shadow_monitor_command(latest=_flag(args, "--latest"))
        return
    if args[:2] == ["ops", "health"]:
        ops_cli.pipeline_health_command(
            as_of=_option(args, "--as-of"),
            non_trading_day=_flag(args, "--non-trading-day"),
        )
        return
    if args[:2] == ["security", "scan-secrets"]:
        security_cli.security_scan_secrets_command(as_of=_option(args, "--as-of"))
        return
    raise typer.BadParameter(f"daily-run direct dispatcher 不支持命令：{' '.join(args)}")


def _option(args: Sequence[str], name: str, default: str | None = None) -> str | None:
    try:
        index = args.index(name)
    except ValueError:
        return default
    if index + 1 >= len(args):
        raise typer.BadParameter(f"缺少 {name} 的参数值")
    return args[index + 1]


def _flag(args: Sequence[str], name: str) -> bool:
    return name in args


def _path_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH if value is None else Path(value)


def _path_option_with_default(args: Sequence[str], name: str, default: Path):
    value = _option(args, name)
    return default if value is None else Path(value)


def _optional_path(args: Sequence[str], name: str):
    value = _option(args, name)
    return None if value is None else Path(value)


def _signal_ablation_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SIGNAL_ABLATION_CONFIG_PATH if value is None else Path(value)


def _signal_calibration_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH if value is None else Path(value)


def _values_after_option(args: Sequence[str], name: str) -> list[str]:
    values: list[str] = []
    index = 0
    while index < len(args):
        if args[index] != name:
            index += 1
            continue
        index += 1
        while index < len(args) and not args[index].startswith("--"):
            values.append(args[index])
            index += 1
    return values


if __name__ == "__main__":
    raise SystemExit(main())
