from __future__ import annotations

import os
import sys
from collections.abc import Sequence

import typer

from ai_trading_system import cli


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
    if args[:2] == ["pit-snapshots", "fetch-fmp-forward"]:
        cli.fetch_fmp_forward_pit_command(
            as_of=_option(args, "--as-of"),
            continue_on_failure=_flag(args, "--continue-on-failure"),
        )
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


if __name__ == "__main__":
    raise SystemExit(main())
