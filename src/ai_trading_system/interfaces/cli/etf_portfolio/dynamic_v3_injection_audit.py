from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_INJECTION_AUDIT_DIR,
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DynamicV3ParameterResearchError,
    injection_audit_report_payload,
    run_injection_audit,
    validate_injection_audit_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_injection_audit_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_injection_audit_app.command("run")
def dynamic_v3_injection_audit_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: Annotated[str, typer.Option("--as-of", help="audit as-of date。")] = "2026-06-04",
    end: Annotated[str, typer.Option("--end", help="audit end date。")] = "2026-06-04",
    max_candidates: Annotated[
        int,
        typer.Option("--max-candidates", help="audit candidate count。"),
    ] = 20,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="real evaluator FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """运行 TRADING-102 parameter injection audit。"""
    try:
        result = run_injection_audit(
            config_path=config_path,
            as_of=parse_date(as_of),
            end=parse_date(end),
            max_candidates=max_candidates,
            prices_path=prices_path,
            rates_path=rates_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"audit_id={result['audit_id']}")
    typer.echo(f"audit_dir={result['audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate_count={report['candidate_count']}")
    typer.echo(
        "parameter_effect_pair_coverage_complete="
        f"{str(report['parameter_effect_pair_coverage_complete']).lower()}"
    )
    typer.echo(
        f"parameters_without_matched_pairs={','.join(report['parameters_without_matched_pairs'])}"
    )
    typer.echo(f"no_observed_effect_parameters={','.join(report['no_observed_effect_parameters'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_injection_audit_app.command("report")
def dynamic_v3_injection_audit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest injection audit pointer。"),
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="injection audit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """展示 TRADING-102 injection audit 摘要。"""
    payload = injection_audit_report_payload(
        audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"audit_id={payload['audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(
        "parameter_effect_pair_coverage_complete="
        f"{str(payload['parameter_effect_pair_coverage_complete']).lower()}"
    )
    typer.echo(
        f"parameters_without_matched_pairs={','.join(payload['parameters_without_matched_pairs'])}"
    )
    typer.echo(f"parameter_effect_summary_path={payload['parameter_effect_summary_path']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-injection-audit")
def dynamic_v3_validate_injection_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="injection audit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """校验 TRADING-102 injection audit artifacts。"""
    payload = validate_injection_audit_artifact(audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
