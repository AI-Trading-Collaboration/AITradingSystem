from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.dynamic_v3_clean_selection_preregistration_gate import (
    DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT,
    DEFAULT_CLEAN_SELECTION_POLICY_PATH,
    DynamicV3CleanSelectionGateError,
    build_dynamic_v3_clean_selection_preregistration_gate,
    validate_dynamic_v3_clean_selection_preregistration_gate,
    write_dynamic_v3_clean_selection_preregistration_gate,
)
from ai_trading_system.research_restart import (
    DEFAULT_COST_POLICY_PATH,
    DEFAULT_EXECUTION_POLICY_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESTART_OUTPUT_ROOT,
    DEFAULT_RESTART_POLICY_PATH,
    DEFAULT_SECONDARY_PRICES_PATH,
    DEFAULT_WINDOW_REGISTRY_PATH,
    ResearchRestartError,
    run_research_restart_preflight,
    validate_research_restart_preflight,
)
from ai_trading_system.research_restart_decision import (
    DEFAULT_FORWARD_CONTINUITY_PATH,
    DEFAULT_FORWARD_MATURITY_PATH,
    DEFAULT_R0_PREFLIGHT_PATH,
    DEFAULT_R1_ROBUSTNESS_DIR,
    DEFAULT_R1_WALK_FORWARD_DIR,
    DEFAULT_R2_OUTPUT_ROOT,
    ResearchRestartDecisionError,
    run_strategy_research_restart_decision,
    validate_strategy_research_restart_decision,
)


def register_research_restart_commands(app: typer.Typer) -> None:
    app.command("strategy-restart-preflight")(strategy_restart_preflight_command)
    app.command("validate-strategy-restart-preflight")(validate_strategy_restart_preflight_command)
    app.command("strategy-restart-decision")(strategy_restart_decision_command)
    app.command("validate-strategy-restart-decision")(validate_strategy_restart_decision_command)
    app.command("clean-selection-preregistration-gate")(
        clean_selection_preregistration_gate_command
    )
    app.command("validate-clean-selection-preregistration-gate")(
        validate_clean_selection_preregistration_gate_command
    )


def strategy_restart_preflight_command(
    source_sweep_dir: Annotated[
        Path, typer.Option("--source-sweep-dir", help="TRADING-096/097 source sweep目录。")
    ],
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="R0～R2 restart policy。")
    ] = DEFAULT_RESTART_POLICY_PATH,
    primary_window_policy_path: Annotated[
        Path, typer.Option("--primary-window-policy", help="Primary research window policy。")
    ] = DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
    window_registry_path: Annotated[
        Path, typer.Option("--window-registry", help="Research window registry。")
    ] = DEFAULT_WINDOW_REGISTRY_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="Primary prices cache。")
    ] = DEFAULT_PRICES_PATH,
    secondary_prices_path: Annotated[
        Path, typer.Option("--secondary-prices-path", help="Secondary prices cache。")
    ] = DEFAULT_SECONDARY_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="Rates cache。")
    ] = DEFAULT_RATES_PATH,
    download_manifest_path: Annotated[
        Path | None, typer.Option("--download-manifest", help="Download manifest。")
    ] = None,
    cost_policy_path: Annotated[
        Path, typer.Option("--cost-policy", help="Transaction cost policy。")
    ] = DEFAULT_COST_POLICY_PATH,
    execution_policy_path: Annotated[
        Path, typer.Option("--execution-policy", help="Execution lag policy。")
    ] = DEFAULT_EXECUTION_POLICY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="R0 artifact输出目录。")
    ] = DEFAULT_RESTART_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of", help="DQ as-of date。")] = None,
) -> None:
    try:
        payload = run_research_restart_preflight(
            source_sweep_dir=source_sweep_dir,
            policy_path=policy_path,
            primary_window_policy_path=primary_window_policy_path,
            window_registry_path=window_registry_path,
            prices_path=prices_path,
            secondary_prices_path=secondary_prices_path,
            rates_path=rates_path,
            download_manifest_path=download_manifest_path,
            cost_policy_path=cost_policy_path,
            execution_policy_path=execution_policy_path,
            output_root=output_root,
            as_of=None if as_of is None else date.fromisoformat(as_of),
        )
    except (ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_payload(payload)
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def validate_strategy_restart_preflight_command(
    artifact_path: Annotated[
        Path, typer.Option("--artifact-path", help="R0 preflight JSON。")
    ] = DEFAULT_RESTART_OUTPUT_ROOT
    / "strategy_research_restart_preflight.json",
) -> None:
    try:
        payload = validate_research_restart_preflight(artifact_path=artifact_path)
    except (ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_payload(payload)
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def strategy_restart_decision_command(
    walk_forward_id: Annotated[
        str, typer.Option("--walk-forward-id", help="Validated R1 walk-forward id。")
    ],
    robustness_id: Annotated[
        str, typer.Option("--robustness-id", help="Validated R1 robustness id。")
    ],
    r0_preflight_path: Annotated[
        Path, typer.Option("--r0-preflight", help="Validated R0 preflight JSON。")
    ] = DEFAULT_R0_PREFLIGHT_PATH,
    walk_forward_root: Annotated[
        Path, typer.Option("--walk-forward-root", help="R1 walk-forward root。")
    ] = DEFAULT_R1_WALK_FORWARD_DIR,
    robustness_root: Annotated[
        Path, typer.Option("--robustness-root", help="R1 robustness root。")
    ] = DEFAULT_R1_ROBUSTNESS_DIR,
    forward_maturity_path: Annotated[
        Path, typer.Option("--forward-maturity", help="TRADING-777 maturity JSON。")
    ] = DEFAULT_FORWARD_MATURITY_PATH,
    forward_continuity_path: Annotated[
        Path, typer.Option("--forward-continuity", help="Forward continuity JSON。")
    ] = DEFAULT_FORWARD_CONTINUITY_PATH,
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="R0～R2 restart policy。")
    ] = DEFAULT_RESTART_POLICY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="R2 decision output root。")
    ] = DEFAULT_R2_OUTPUT_ROOT,
) -> None:
    try:
        payload = run_strategy_research_restart_decision(
            walk_forward_id=walk_forward_id,
            robustness_id=robustness_id,
            r0_preflight_path=r0_preflight_path,
            walk_forward_root=walk_forward_root,
            robustness_root=robustness_root,
            forward_maturity_path=forward_maturity_path,
            forward_continuity_path=forward_continuity_path,
            policy_path=policy_path,
            output_root=output_root,
        )
    except (ResearchRestartDecisionError, ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"decision={payload['decision']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


def validate_strategy_restart_decision_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="R2 decision output root。")
    ] = DEFAULT_R2_OUTPUT_ROOT,
) -> None:
    try:
        payload = validate_strategy_research_restart_decision(output_root=output_root)
    except (ResearchRestartDecisionError, ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_payload(payload)
    typer.echo(f"decision={payload.get('decision')}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def clean_selection_preregistration_gate_command(
    r2_manifest_path: Annotated[
        Path, typer.Option("--r2-manifest", help="Validated R2 decision manifest。")
    ],
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="Clean-selection eligibility policy。")
    ] = DEFAULT_CLEAN_SELECTION_POLICY_PATH,
    preregistration_path: Annotated[
        Path | None,
        typer.Option(
            "--preregistration",
            help="可选 canonical preregistration；默认从 source contract 解析。",
        ),
    ] = None,
    research_context_path: Annotated[
        Path | None,
        typer.Option("--research-context", help="可选 canonical evaluation context。"),
    ] = None,
    campaign_spec_path: Annotated[
        Path | None,
        typer.Option("--campaign-spec", help="可选 canonical CampaignSpec。"),
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="资格门 artifact 输出目录。")
    ] = DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT,
) -> None:
    """构建 S0 clean-selection 资格证据；不运行 evaluator 或 backtest。"""

    try:
        report = build_dynamic_v3_clean_selection_preregistration_gate(
            r2_manifest_path=r2_manifest_path,
            policy_path=policy_path,
            preregistration_path=preregistration_path,
            research_context_path=research_context_path,
            campaign_spec_path=campaign_spec_path,
        )
        result = write_dynamic_v3_clean_selection_preregistration_gate(
            report,
            output_root=output_root,
        )
    except (DynamicV3CleanSelectionGateError, OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={result['status']}")
    typer.echo(f"gate_id={result['gate_id']}")
    typer.echo(f"report_path={result['report_path']}")
    typer.echo("clean_run_unblocked=false")
    typer.echo("candidate_expansion_allowed=false")
    typer.echo("new_parameter_search_allowed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


def validate_clean_selection_preregistration_gate_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="资格门 artifact 输出目录。")
    ] = DEFAULT_CLEAN_SELECTION_GATE_OUTPUT_ROOT,
) -> None:
    """重读 live source 并重算 S0 clean-selection 资格证据。"""

    payload = validate_dynamic_v3_clean_selection_preregistration_gate(output_root=output_root)
    _print_payload(payload)
    typer.echo(f"eligibility_status={payload.get('eligibility_status')}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _print_payload(payload: MappingLike) -> None:
    typer.echo(f"status={payload.get('status')}")
    if "research_execution_unblocked" in payload:
        typer.echo(
            f"research_execution_unblocked={str(payload.get('research_execution_unblocked')).lower()}"
        )
    typer.echo(f"failed_check_count={payload.get('failed_check_count', 0)}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


MappingLike = dict[str, Any]
