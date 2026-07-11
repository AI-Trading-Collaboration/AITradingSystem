from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_GOVERNANCE_DIR,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_RESEARCH_INDEX_DIR,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DEFAULT_SWEEP_OUTPUT_DIR,
    artifacts_latest_payload,
    build_research_index,
    governance_diff_payload,
    governance_report_payload,
    repair_latest_pointers_payload,
    research_compare_payload,
    research_history_payload,
    research_query_payload,
    stale_artifacts_payload,
    validate_artifacts_payload,
    validate_parameter_governance,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_artifacts_app,
    dynamic_v3_governance_app,
    dynamic_v3_research_app,
)


@dynamic_v3_governance_app.command("validate")
def dynamic_v3_governance_validate_command(
    governance_path: Annotated[
        Path,
        typer.Option("--governance", "--config", help="parameter governance config。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    sweep_config_path: Annotated[
        Path,
        typer.Option("--sweep-config", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """校验 TRADING-108 parameter governance。"""
    payload = validate_parameter_governance(
        governance_path=governance_path,
        config_path=sweep_config_path,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_governance_app.command("report")
def dynamic_v3_governance_report_command(
    governance_path: Annotated[
        Path,
        typer.Option("--governance", "--config", help="parameter governance config。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="governance artifact root。"),
    ] = DEFAULT_GOVERNANCE_DIR,
) -> None:
    """生成 TRADING-108 governance report。"""
    payload = governance_report_payload(
        governance_path=governance_path,
        output_dir=output_dir,
        write=True,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"policy_id={payload['policy_id']}")
    typer.echo(f"search_space_version={payload['search_space_version']}")
    typer.echo(f"governance_report={output_dir / 'parameter_governance_report.json'}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_governance_app.command("diff")
def dynamic_v3_governance_diff_command(
    old_config: Annotated[Path, typer.Option("--old-config", help="old governance config。")],
    new_config: Annotated[Path, typer.Option("--new-config", help="new governance config。")],
) -> None:
    """比较 TRADING-108 governance configs。"""
    payload = governance_diff_payload(old_config=old_config, new_config=new_config)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"change_count={payload['change_count']}")
    typer.echo(f"manual_review_required={payload['manual_review_required']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("index-build")
def dynamic_v3_research_index_build_command(
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """重建 TRADING-109 research result index。"""
    payload = build_research_index(
        sweep_output_dir=sweep_output_dir,
        shadow_registry_path=registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"index_dir={output_dir}")
    typer.echo(f"sweep_count={payload['sweep_count']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("query")
def dynamic_v3_research_query_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """查询 TRADING-109 candidate artifacts。"""
    payload = research_query_payload(candidate_id=candidate_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"match_count={len(payload['matches'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("compare")
def dynamic_v3_research_compare_command(
    candidate_ids: Annotated[
        list[str],
        typer.Option("--candidate-id", help="candidate id；provide exactly two。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """比较 TRADING-109 two candidates。"""
    if len(candidate_ids) != 2:
        raise typer.BadParameter("research compare requires exactly two --candidate-id values")
    payload = research_compare_payload(
        candidate_a=candidate_ids[0],
        candidate_b=candidate_ids[1],
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"parameter_diff_count={len(payload['parameter_diff'])}")
    typer.echo(f"metric_diff_count={len(payload['metric_diff'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("history")
def dynamic_v3_research_history_command(
    parameter: Annotated[str, typer.Option("--parameter", help="parameter name。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """查询 TRADING-109 parameter history。"""
    payload = research_history_payload(parameter=parameter, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"parameter={parameter}")
    typer.echo(f"observation_count={payload['observation_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_artifacts_app.command("latest")
def dynamic_v3_artifacts_latest_command() -> None:
    """展示 TRADING-099 latest pointers。"""
    payload = artifacts_latest_payload()
    typer.echo(f"status={payload['status']}")
    typer.echo(f"pointer_dir={payload['pointer_dir']}")
    for name, pointer in payload["pointers"].items():
        typer.echo(f"{name}={pointer.get('artifact_id')} path={pointer.get('path')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_artifacts_app.command("validate")
def dynamic_v3_artifacts_validate_command(
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
) -> None:
    """校验 TRADING-099 latest pointer targets。"""
    payload = validate_artifacts_payload(family=family)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_artifacts_app.command("repair-latest")
def dynamic_v3_artifacts_repair_latest_command(
    pointer_dir: Annotated[
        Path,
        typer.Option("--pointer-dir", help="latest pointer directory。"),
    ] = DEFAULT_LATEST_POINTER_DIR,
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="canonical dynamic-v3 artifact root。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """从 canonical artifact root 重建 TRADING-099 latest pointers。"""
    payload = repair_latest_pointers_payload(
        pointer_dir=pointer_dir,
        artifact_root=artifact_root,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"repaired_count={payload['repaired_count']}")
    typer.echo(f"skipped_count={payload['skipped_count']}")
    validation = mapping_obj(payload.get("validation") or {})
    if validation:
        typer.echo(f"validation_status={validation.get('status')}")
        typer.echo(f"validation_failed_check_count={validation.get('failed_check_count')}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_artifacts_app.command("stale")
def dynamic_v3_artifacts_stale_command(
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """检查 TRADING-099 stale artifacts。"""
    payload = stale_artifacts_payload(family=family, config_path=config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"stale_after_days={payload['stale_after_days']}")
    typer.echo(f"stale_count={len(payload['stale_artifacts'])}")
    typer.echo("production_candidate_generated=false")
