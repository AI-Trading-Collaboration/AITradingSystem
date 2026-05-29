from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace

import typer

from ai_trading_system import cli
from ai_trading_system.cli_commands import docs as docs_cli
from ai_trading_system.cli_commands import sec_pit as sec_pit_cli


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
        cli.download_data(
            start=_option(args, "--start", "2018-01-01"),
            end=_option(args, "--end"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:1] == ["validate-data"]:
        cli.validate_data(
            as_of=_option(args, "--as-of"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:2] == ["data", "diagnose-backtest-inputs"]:
        cli.data_diagnose_backtest_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
        )
        return
    if args[:2] == ["data", "repair-backtest-inputs"]:
        cli.data_repair_backtest_inputs_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
            dry_run=_flag(args, "--dry-run"),
            price_only=_flag(args, "--price-only"),
            symbols=_values_after_option(args, "--symbols"),
            price_provider=_option(args, "--price-provider", "fmp") or "fmp",
            fmp_api_key_env=_option(args, "--fmp-api-key-env", "FMP_API_KEY") or "FMP_API_KEY",
        )
        return
    if args[:2] == ["signals", "build-snapshot"]:
        cli.signals_build_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
            dry_run=_flag(args, "--dry-run"),
            price_derived_only=_flag(args, "--price-derived-only"),
        )
        return
    if args[:2] == ["signals", "validate-snapshot"]:
        cli.signals_validate_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "ablation"]:
        cli.signals_ablation_command(
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
        cli.signals_calibrate_command(
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
        cli.signals_explain_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "validate-ablation"]:
        cli.signals_validate_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["pit-snapshots", "fetch-fmp-forward"]:
        cli.fetch_fmp_forward_pit_command(
            as_of=_option(args, "--as-of"),
            continue_on_failure=_flag(args, "--continue-on-failure"),
        )
        return
    if args[:2] == ["pit-snapshots", "build-manifest"]:
        cli.build_pit_snapshot_manifest_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["pit-snapshots", "validate"]:
        cli.validate_pit_snapshots_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "download-sec-companyfacts"]:
        cli.download_sec_companyfacts_command(
            user_agent=_option(args, "--user-agent") or os.getenv("SEC_USER_AGENT")
        )
        return
    if args[:2] == ["fundamentals", "extract-sec-metrics"]:
        cli.extract_sec_metrics_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "merge-tsm-ir-sec-metrics"]:
        cli.merge_tsm_ir_sec_metrics(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "validate-sec-metrics"]:
        cli.validate_sec_metrics_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["valuation", "fetch-fmp"]:
        cli.fetch_fmp_valuations(as_of=_option(args, "--as-of"))
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
        )
        return
    if args[:2] == ["feedback", "optimize-market-feedback"]:
        cli.optimize_market_feedback_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "evaluate-parameter-governance"]:
        cli.evaluate_parameter_governance_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "loop-review"]:
        cli.feedback_loop_review_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "investment-review"]:
        cli.investment_periodic_review_command(
            period=_option(args, "--period", "weekly") or "weekly",
            as_of=_option(args, "--as-of"),
        )
        return
    if args[:2] == ["reports", "dashboard"]:
        cli.evidence_dashboard_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "calculation-explainers"]:
        cli.calculation_explainers_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["parameters", "shadow-backtest"]:
        cli.parameters_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "validate-shadow-backtest"]:
        cli.validate_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["reports", "shadow-parameter-backtest"]:
        cli.shadow_parameter_backtest_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "parameter-promotion"]:
        cli.parameter_promotion_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-snapshot"]:
        cli.signal_snapshot_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-ablation"]:
        cli.signal_ablation_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-calibration"]:
        cli.signal_calibration_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "reader-brief"]:
        cli.reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "validate-reader-brief"]:
        cli.validate_reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "score-change-attribution"]:
        cli.score_change_attribution_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "market-panel"]:
        cli.market_panel_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "research-governance-summary"]:
        cli.research_governance_summary_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "index"]:
        cli.report_index_command(
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
        cli.pipeline_health_command(
            as_of=_option(args, "--as-of"),
            non_trading_day=_flag(args, "--non-trading-day"),
        )
        return
    if args[:2] == ["security", "scan-secrets"]:
        cli.security_scan_secrets_command(as_of=_option(args, "--as-of"))
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
    return cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH if value is None else Path(value)


def _optional_path(args: Sequence[str], name: str):
    value = _option(args, name)
    return None if value is None else Path(value)


def _signal_ablation_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return cli.DEFAULT_SIGNAL_ABLATION_CONFIG_PATH if value is None else Path(value)


def _signal_calibration_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return cli.DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH if value is None else Path(value)


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
