from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as system_target
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    mapping_obj as _mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_model_rebalance_app,
    dynamic_v3_model_target_app,
    dynamic_v3_paper_shadow_app,
    dynamic_v3_paper_shadow_performance_app,
    dynamic_v3_rescue_app,
    dynamic_v3_system_target_review_app,
)


@dynamic_v3_model_target_app.command("config-validate")
def dynamic_v3_model_target_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="model target config。"),
    ] = system_target.DEFAULT_MODEL_TARGET_CONFIG_PATH,
) -> None:
    """校验 TRADING-209 research model target config。"""
    payload = system_target.validate_model_target_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_model_target_app.command("generate")
def dynamic_v3_model_target_generate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="model target config。"),
    ] = system_target.DEFAULT_MODEL_TARGET_CONFIG_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="target as-of date。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model target artifact root。"),
    ] = system_target.DEFAULT_MODEL_TARGET_DIR,
) -> None:
    """生成 TRADING-209 research model target weights。"""
    result = system_target.generate_model_target(
        config_path=config_path,
        as_of=_parse_date(as_of) if as_of else None,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"target_id={result['target_id']}")
    typer.echo(f"target_dir={result['target_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"generated_methods={','.join(manifest['generated_methods'])}")
    typer.echo(f"recommended_research_method={manifest['recommended_research_method']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_model_target_app.command("report")
def dynamic_v3_model_target_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest model target。"),
    ] = False,
    target_id: Annotated[str | None, typer.Option("--target-id", help="target id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model target artifact root。"),
    ] = system_target.DEFAULT_MODEL_TARGET_DIR,
) -> None:
    """展示 TRADING-209 model target 摘要。"""
    payload = system_target.model_target_report_payload(
        target_id=target_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"target_id={payload['target_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"generated_methods={','.join(payload['generated_methods'])}")
    typer.echo(f"recommended_research_method={payload['recommended_research_method']}")
    typer.echo(f"report_path={payload['model_target_report_path']}")
    typer.echo("official_target_weights_written=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-model-target")
def dynamic_v3_validate_model_target_command(
    target_id: Annotated[str, typer.Option("--target-id", help="target id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model target artifact root。"),
    ] = system_target.DEFAULT_MODEL_TARGET_DIR,
) -> None:
    """校验 TRADING-209 model target artifact。"""
    payload = system_target.validate_model_target_artifact(
        target_id=target_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_app.command("init")
def dynamic_v3_paper_shadow_init_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper shadow account config。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_DIR,
) -> None:
    """初始化 TRADING-210 paper shadow account。"""
    result = system_target.init_paper_shadow_account(config_path=config_path, output_dir=output_dir)
    state = result["state"]
    typer.echo(f"paper_shadow_id={result['paper_shadow_id']}")
    typer.echo(f"paper_shadow_dir={result['paper_shadow_dir']}")
    typer.echo(f"state_status={state['state_status']}")
    typer.echo(f"tracked_methods={','.join(state['tracked_methods'])}")
    typer.echo("paper_shadow_only=true")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_shadow_app.command("state")
def dynamic_v3_paper_shadow_state_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper shadow。"),
    ] = False,
    paper_shadow_id: Annotated[
        str | None,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_DIR,
) -> None:
    """展示 TRADING-210 paper shadow state。"""
    payload = system_target.paper_shadow_state_payload(
        paper_shadow_id=paper_shadow_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"paper_shadow_id={payload['paper_shadow_id']}")
    typer.echo(f"state_status={payload['state_status']}")
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"tracked_methods={','.join(payload['tracked_methods'])}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_shadow_app.command("report")
def dynamic_v3_paper_shadow_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper shadow。"),
    ] = False,
    paper_shadow_id: Annotated[
        str | None,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_DIR,
) -> None:
    """展示 TRADING-210 paper shadow report。"""
    payload = system_target.paper_shadow_report_payload(
        paper_shadow_id=paper_shadow_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"paper_shadow_id={payload['paper_shadow_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"tracked_methods={','.join(payload['tracked_methods'])}")
    typer.echo(f"report_path={payload['paper_shadow_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow")
def dynamic_v3_validate_paper_shadow_command(
    paper_shadow_id: Annotated[
        str,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_DIR,
) -> None:
    """校验 TRADING-210 paper shadow artifact。"""
    payload = system_target.validate_paper_shadow_artifact(
        paper_shadow_id=paper_shadow_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("paper_shadow_only=true")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_model_rebalance_app.command("simulate")
def dynamic_v3_model_rebalance_simulate_command(
    paper_shadow_id: Annotated[
        str,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ],
    target_id: Annotated[str, typer.Option("--target-id", help="model target id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model rebalance artifact root。"),
    ] = system_target.DEFAULT_MODEL_REBALANCE_DIR,
) -> None:
    """执行 TRADING-211 model target paper rebalance。"""
    result = system_target.simulate_model_rebalance(
        paper_shadow_id=paper_shadow_id,
        target_id=target_id,
        output_dir=output_dir,
    )
    summary = result["rebalance_turnover_summary"]
    typer.echo(f"rebalance_id={result['rebalance_id']}")
    typer.echo(f"rebalance_dir={result['rebalance_dir']}")
    typer.echo(f"total_turnover={summary['total_turnover']}")
    typer.echo(f"skipped_methods={','.join(summary['skipped_methods'])}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_model_rebalance_app.command("report")
def dynamic_v3_model_rebalance_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest model rebalance。"),
    ] = False,
    rebalance_id: Annotated[
        str | None,
        typer.Option("--rebalance-id", help="rebalance id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model rebalance artifact root。"),
    ] = system_target.DEFAULT_MODEL_REBALANCE_DIR,
) -> None:
    """展示 TRADING-211 model rebalance report。"""
    payload = system_target.model_rebalance_report_payload(
        rebalance_id=rebalance_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("rebalance_turnover_summary"))
    typer.echo(f"rebalance_id={payload['rebalance_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_turnover={summary.get('total_turnover')}")
    typer.echo(f"skipped_methods={','.join(summary.get('skipped_methods') or [])}")
    typer.echo(f"report_path={payload['model_rebalance_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-model-rebalance")
def dynamic_v3_validate_model_rebalance_command(
    rebalance_id: Annotated[str, typer.Option("--rebalance-id", help="rebalance id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="model rebalance artifact root。"),
    ] = system_target.DEFAULT_MODEL_REBALANCE_DIR,
) -> None:
    """校验 TRADING-211 model rebalance artifact。"""
    payload = system_target.validate_model_rebalance_artifact(
        rebalance_id=rebalance_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("paper_shadow_only=true")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_performance_app.command("run")
def dynamic_v3_paper_shadow_performance_run_command(
    paper_shadow_id: Annotated[
        str,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow performance artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="performance evaluation date，YYYY-MM-DD；默认使用运行日期。"),
    ] = None,
) -> None:
    """运行 TRADING-212 paper shadow performance comparison。"""
    result = system_target.run_paper_shadow_performance(
        paper_shadow_id=paper_shadow_id,
        output_dir=output_dir,
        as_of=_parse_date(as_of) if as_of else None,
    )
    summary = result["method_performance_summary"]
    typer.echo(f"performance_id={result['performance_id']}")
    typer.echo(f"performance_dir={result['performance_dir']}")
    typer.echo(f"performance_start_date={summary['performance_start_date']}")
    typer.echo(f"evaluation_as_of={summary['evaluation_as_of']}")
    typer.echo(f"best_return_method={summary['best_return_method']}")
    typer.echo(f"best_drawdown_method={summary['best_drawdown_method']}")
    typer.echo(f"best_risk_adjusted_method={summary['best_risk_adjusted_method']}")
    typer.echo("data_quality_status=" + str(summary["data_quality_status"]))
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_shadow_performance_app.command("report")
def dynamic_v3_paper_shadow_performance_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper shadow performance。"),
    ] = False,
    performance_id: Annotated[
        str | None,
        typer.Option("--performance-id", help="performance id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow performance artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> None:
    """展示 TRADING-212 paper shadow performance report。"""
    payload = system_target.paper_shadow_performance_report_payload(
        performance_id=performance_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("method_performance_summary"))
    typer.echo(f"performance_id={payload['performance_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_return_method={summary.get('best_return_method')}")
    typer.echo(f"best_drawdown_method={summary.get('best_drawdown_method')}")
    typer.echo(f"best_risk_adjusted_method={summary.get('best_risk_adjusted_method')}")
    typer.echo(f"report_path={payload['paper_shadow_performance_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-performance")
def dynamic_v3_validate_paper_shadow_performance_command(
    performance_id: Annotated[str, typer.Option("--performance-id", help="performance id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow performance artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> None:
    """校验 TRADING-212 paper shadow performance artifact。"""
    payload = system_target.validate_paper_shadow_performance_artifact(
        performance_id=performance_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("paper_shadow_only=true")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_system_target_review_app.command("pack")
def dynamic_v3_system_target_review_pack_command(
    target_id: Annotated[str, typer.Option("--target-id", help="model target id。")],
    paper_shadow_id: Annotated[
        str,
        typer.Option("--paper-shadow-id", help="paper shadow id。"),
    ],
    performance_id: Annotated[str, typer.Option("--performance-id", help="performance id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="system target review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> None:
    """生成 TRADING-213 system target portfolio review pack。"""
    result = system_target.build_system_target_review_pack(
        target_id=target_id,
        paper_shadow_id=paper_shadow_id,
        performance_id=performance_id,
        output_dir=output_dir,
    )
    decision = result["system_target_decision"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"recommended_research_method={decision['recommended_research_method']}")
    typer.echo(f"decision_status={decision['decision_status']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_system_target_review_app.command("report")
def dynamic_v3_system_target_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest system target review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="system target review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> None:
    """展示 TRADING-213 system target review report。"""
    payload = system_target.system_target_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("system_target_decision"))
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_research_method={decision.get('recommended_research_method')}")
    typer.echo(f"decision_status={decision.get('decision_status')}")
    typer.echo(f"report_path={payload['system_target_review_report_path']}")
    typer.echo("research_target_only=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-system-target-review")
def dynamic_v3_validate_system_target_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="system target review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> None:
    """校验 TRADING-213 system target review artifact。"""
    payload = system_target.validate_system_target_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
