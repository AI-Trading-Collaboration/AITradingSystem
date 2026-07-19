from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    run_b2_risk_scaler_research,
)
from ai_trading_system.etf_portfolio.weight_research_b2_b3_v2 import (
    run_b2_b3_v2_research,
)
from ai_trading_system.etf_portfolio.weight_research_b2_control_windows import (
    run_b2_control_window_research,
)
from ai_trading_system.etf_portfolio.weight_research_b2_final_decision import (
    run_b2_final_evidence_role_decision,
)
from ai_trading_system.etf_portfolio.weight_research_b2_followup import (
    run_b2_followup_research,
)
from ai_trading_system.etf_portfolio.weight_research_b2_full_diagnostic import (
    run_b2_full_diagnostic_research,
)
from ai_trading_system.etf_portfolio.weight_research_b2_targeted_evidence import (
    run_b2_targeted_evidence_research,
)
from ai_trading_system.etf_portfolio.weight_research_b3 import run_b3_relative_tilt_research
from ai_trading_system.etf_portfolio.weight_research_b4 import run_b4_interaction_research
from ai_trading_system.etf_portfolio.weight_research_branching import (
    run_b2_b3_branching_checkpoint,
)
from ai_trading_system.etf_portfolio.weight_research_checkpoint import (
    run_weight_research_checkpoint,
)
from ai_trading_system.etf_portfolio.weight_research_diagnosis import (
    run_b1_b4_diagnosis_batch,
)
from ai_trading_system.etf_portfolio.weight_research_extended_diagnosis import (
    run_b2_b3_b4_diagnostic_expansion,
)
from ai_trading_system.etf_portfolio.weight_research_interfaces import (
    build_dependency_boundary_validation,
    build_research_layer_interface_contract,
    build_signal_diagnostics_framework_contract,
    write_dependency_boundary_validation,
    write_research_layer_interface_contract,
    write_signal_diagnostics_framework_contract,
)
from ai_trading_system.etf_portfolio.weight_research_post_branch import (
    run_post_b2_b3_research,
)
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    DEFAULT_HISTORICAL_B1_RESULT_PATH,
    DEFAULT_HOLDOUT_POLICY_PATH,
    DEFAULT_RESEARCH_SOURCE_DIR,
    DEFAULT_SCOPE_FREEZE_PATH,
    DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    build_b1_metric_semantics_audit,
    build_contract_validation,
    run_b1_execution_control,
    run_b1_isolated_attribution,
    run_static_baseline_family,
    write_b1_metric_semantics_audit,
    write_contract_validation,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import weight_research_app


@weight_research_app.command("validate-contracts")
def weight_research_validate_contracts_command(
    scope_path: Annotated[
        Path,
        typer.Option("--scope-path", help="511A ablation runner scope freeze JSON。"),
    ] = DEFAULT_SCOPE_FREEZE_PATH,
    signal_contract_path: Annotated[
        Path,
        typer.Option("--signal-contract-path", help="511B signal robustness contract JSON。"),
    ] = DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    holdout_policy_path: Annotated[
        Path,
        typer.Option("--holdout-policy-path", help="511C untouched holdout policy JSON。"),
    ] = DEFAULT_HOLDOUT_POLICY_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Weight research unblock policy YAML。"),
    ] = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    layer_id: Annotated[str, typer.Option("--layer-id", help="Layer to validate。")] = "B1",
    start: Annotated[str | None, typer.Option("--from", help="Optional run window start。")] = None,
    end: Annotated[str | None, typer.Option("--to", help="Optional run window end。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Validation output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
) -> None:
    """Validate 511A-C contracts before any B1-B6 ablation runner."""
    run_start = _parse_date(start) if start else None
    run_end = _parse_date(end) if end else None
    payload = build_contract_validation(
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        layer_id=layer_id,
        run_start=run_start,
        run_end=run_end,
    )
    json_path, markdown_path = write_contract_validation(payload, output_dir=output_dir)
    typer.echo(f"weight_research_contract_validation_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_research_app.command("audit-b1")
def weight_research_audit_b1_command(
    historical_b1_path: Annotated[
        Path,
        typer.Option("--historical-b1-path", help="Historical 511D B1 result JSON。"),
    ] = DEFAULT_HISTORICAL_B1_RESULT_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Audit output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical audit artifact。",
        ),
    ] = False,
) -> None:
    """Audit historical B1 metric semantics and comparator attribution."""
    payload = build_b1_metric_semantics_audit(historical_b1_path=historical_b1_path)
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    json_path, markdown_path = write_b1_metric_semantics_audit(
        payload,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    typer.echo(f"b1_metric_semantics_audit_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    if write_source_alias:
        typer.echo(
            "Source Alias："
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'b1_metric_semantics_and_comparator_audit.json'}"
        )
    if payload["status"] == "B1_ATTRIBUTION_INVALID":
        raise typer.Exit(code=1)


@weight_research_app.command("run-static-baselines")
def weight_research_run_static_baselines_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="Baseline mini-backfill start date。")] = (
        "2023-01-03"
    ),
    end: Annotated[str, typer.Option("--to", help="Baseline mini-backfill end date。")] = (
        "2023-07-31"
    ),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Baseline output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical baseline artifact。",
        ),
    ] = False,
) -> None:
    """Run B0H/B0R static baseline family for B1 attribution repair."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payload, json_path, markdown_path, daily_path = run_static_baseline_family(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        data_quality_output_path=data_quality_output_path,
        alias_dir=alias_dir,
    )
    typer.echo(f"static_baseline_family_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    typer.echo(f"Daily：{daily_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'static_baseline_family_result.json'}"
        )
    if payload["status"] == "STATIC_BASELINE_FAMILY_BLOCKED":
        raise typer.Exit(code=1)


@weight_research_app.command("run-b1-attribution")
def weight_research_run_b1_attribution_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="B1E attribution start date。")] = (
        "2023-01-03"
    ),
    end: Annotated[str, typer.Option("--to", help="B1E attribution end date。")] = ("2023-07-31"),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B1E attribution output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical attribution artifact。",
        ),
    ] = False,
) -> None:
    """Run isolated B1E vs B0R attribution gate."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payload, json_path, markdown_path, daily_path = run_b1_isolated_attribution(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        data_quality_output_path=data_quality_output_path,
        alias_dir=alias_dir,
    )
    typer.echo(f"b1_isolated_attribution_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    typer.echo(f"Daily：{daily_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b1_isolated_attribution_result.json'}"
        )
    if payload["status"] == "B1_ATTRIBUTION_INVALID":
        raise typer.Exit(code=1)


@weight_research_app.command("freeze-interfaces")
def weight_research_freeze_interfaces_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Interface contract output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical 512A/512B artifacts。",
        ),
    ] = False,
) -> None:
    """Freeze research layer interfaces and signal diagnostics framework contracts."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    interface_payload = build_research_layer_interface_contract()
    interface_json, interface_md = write_research_layer_interface_contract(
        interface_payload,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    dependency_payload = build_dependency_boundary_validation()
    dependency_json, dependency_md = write_dependency_boundary_validation(
        dependency_payload,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    diagnostics_payload = build_signal_diagnostics_framework_contract()
    diagnostics_json, diagnostics_md = write_signal_diagnostics_framework_contract(
        diagnostics_payload,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    typer.echo(f"research_layer_interface_status={interface_payload['status']}")
    typer.echo(f"dependency_boundary_validation_status={dependency_payload['status']}")
    typer.echo(f"signal_diagnostics_framework_status={diagnostics_payload['status']}")
    typer.echo(f"Interface JSON：{interface_json}")
    typer.echo(f"Interface Markdown：{interface_md}")
    typer.echo(f"Dependency JSON：{dependency_json}")
    typer.echo(f"Dependency Markdown：{dependency_md}")
    typer.echo(f"Diagnostics JSON：{diagnostics_json}")
    typer.echo(f"Diagnostics Markdown：{diagnostics_md}")
    if write_source_alias:
        typer.echo(
            "Source Aliases："
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'research_layer_interface_contract.json'}, "
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'dependency_boundary_validation.json'}, "
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'signal_diagnostics_framework_contract.json'}"
        )
    if dependency_payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_research_app.command("run-b2")
def weight_research_run_b2_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="B2 mini-backfill start date。")] = (
        "2024-07-10"
    ),
    end: Annotated[str, typer.Option("--to", help="B2 mini-backfill end date。")] = ("2024-08-09"),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    modules_config_path: Annotated[
        Path,
        typer.Option("--modules-config", help="Weight research modules policy YAML。"),
    ] = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical B2 artifact。",
        ),
    ] = False,
) -> None:
    """Run B2 risk signal, diagnostics, target mapping, and E0/E1 mini-backfill."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payload, json_path, markdown_path, component_paths = run_b2_risk_scaler_research(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        modules_config_path=modules_config_path,
        alias_dir=alias_dir,
    )
    typer.echo(f"b2_risk_scaler_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    for name, path in sorted(component_paths.items()):
        typer.echo(f"{name}：{path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_risk_scaler_research_result.json'}"
        )
    if payload["status"] in {"B2_SIGNAL_BLOCKED", "B2_SIGNAL_NEEDS_REVISION"}:
        raise typer.Exit(code=1)


@weight_research_app.command("run-b3")
def weight_research_run_b3_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="B3 mini-backfill start date。")] = (
        "2024-07-10"
    ),
    end: Annotated[str, typer.Option("--to", help="B3 mini-backfill end date。")] = ("2024-08-09"),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B3 output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    modules_config_path: Annotated[
        Path,
        typer.Option("--modules-config", help="Weight research modules policy YAML。"),
    ] = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical B3 artifact。",
        ),
    ] = False,
) -> None:
    """Run B3 relative-tilt signal, diagnostics, target mapping, and E0/E1 mini-backfill."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payload, json_path, markdown_path, component_paths = run_b3_relative_tilt_research(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        modules_config_path=modules_config_path,
        alias_dir=alias_dir,
    )
    typer.echo(f"b3_relative_tilt_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    for name, path in sorted(component_paths.items()):
        typer.echo(f"{name}：{path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b3_relative_tilt_research_result.json'}"
        )
    if payload["status"] in {"B3_SIGNAL_BLOCKED", "B3_SIGNAL_NEEDS_REVISION"}:
        raise typer.Exit(code=1)


@weight_research_app.command("run-b4")
def weight_research_run_b4_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="B4 mini-backfill start date。")] = (
        "2024-07-10"
    ),
    end: Annotated[str, typer.Option("--to", help="B4 mini-backfill end date。")] = ("2024-08-09"),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B4 output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    modules_config_path: Annotated[
        Path,
        typer.Option("--modules-config", help="Weight research modules policy YAML。"),
    ] = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    b2_result_path: Annotated[
        Path,
        typer.Option("--b2-result", help="Canonical B2 result JSON。"),
    ] = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_scaler_research_result.json",
    b3_result_path: Annotated[
        Path,
        typer.Option("--b3-result", help="Canonical B3 result JSON。"),
    ] = DEFAULT_RESEARCH_SOURCE_DIR / "b3_relative_tilt_research_result.json",
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research canonical B4 artifact。",
        ),
    ] = False,
) -> None:
    """Run B4 B2-risk x B3-tilt interaction and E0/E1 mini-backfill."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payload, json_path, markdown_path, component_paths = run_b4_interaction_research(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        modules_config_path=modules_config_path,
        b2_result_path=b2_result_path,
        b3_result_path=b3_result_path,
        alias_dir=alias_dir,
    )
    typer.echo(f"b4_interaction_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    for name, path in sorted(component_paths.items()):
        typer.echo(f"{name}：{path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b4_risk_tilt_interaction_result.json'}"
        )
    if payload["status"] in {
        "B4_INTERACTION_BLOCKED",
        "B4_COMPONENT_SIGNAL_BLOCKED",
    }:
        raise typer.Exit(code=1)


@weight_research_app.command("checkpoint")
def weight_research_checkpoint_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Checkpoint output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research checkpoint aliases。",
        ),
    ] = True,
) -> None:
    """Write B5/B6 blocked reviews, synthesis, v3 gates and TRADING-520 snapshot."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_weight_research_checkpoint(
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    snapshot = payloads["weight_research_program_v1_snapshot"]
    typer.echo(f"weight_research_checkpoint_status={snapshot['status']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            "Source Alias："
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'weight_research_program_v1_snapshot.json'}"
        )


@weight_research_app.command("diagnose-b1-b4")
def weight_research_diagnose_b1_b4_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Diagnosis output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research diagnosis aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-521~524 B1-B4 diagnosis without continuing B5/B6/v3."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b1_b4_diagnosis_batch(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    decision = payloads["b4_next_decision_checkpoint"]
    typer.echo(f"b1_b4_diagnosis_status={decision['status']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b4_next_decision_checkpoint.json'}"
        )
    if decision["b5_allowed"] or decision["b6_allowed"]:
        raise typer.Exit(code=1)


@weight_research_app.command("diagnose-b2-b4-expansion")
def weight_research_diagnose_b2_b4_expansion_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Diagnosis output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research extended diagnosis aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-525~529 B2/B3/B4 diagnostics without running B5/B6/v3."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_b3_b4_diagnostic_expansion(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    checkpoint = payloads["b5_admission_checkpoint"]
    typer.echo(f"b5_admission_status={checkpoint['status']}")
    typer.echo(f"b5_allowed={checkpoint['b5_allowed']}")
    typer.echo(f"b6_allowed={checkpoint['b6_allowed']}")
    typer.echo(f"v3_allowed={checkpoint['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b5_admission_checkpoint.json'}")
    if checkpoint["b6_allowed"] or checkpoint["v3_allowed"]:
        raise typer.Exit(code=1)


@weight_research_app.command("branch-b2-b3")
def weight_research_branch_b2_b3_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Branching checkpoint output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research branching aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-530~536 B2/B3 branching checkpoint without B5/B6/v3."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_b3_branching_checkpoint(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    program = payloads["research_program_checkpoint_after_branching"]
    typer.echo(f"research_program_branching_status={program['status']}")
    typer.echo(f"recommended_next_branch={program['recommended_next_branch']}")
    typer.echo(f"b5_allowed={program['b5_allowed']}")
    typer.echo(f"b6_allowed={program['b6_allowed']}")
    typer.echo(f"v3_allowed={program['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            "Source Alias："
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'research_program_checkpoint_after_branching.json'}"
        )
    if program["b5_allowed"] or program["b6_allowed"] or program["v3_allowed"]:
        raise typer.Exit(code=1)


@weight_research_app.command("post-b2-b3-research")
def weight_research_post_b2_b3_research_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Post-branch research output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-537~556 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-537~556 post-B2/B3 research gates without B5/B6/v3."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_post_b2_b3_research(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    final = payloads["final_branch_decision_snapshot"]
    typer.echo(f"final_branch_decision_status={final['status']}")
    typer.echo(f"b5_allowed={final['b5_allowed']}")
    typer.echo(f"b6_allowed={final['b6_allowed']}")
    typer.echo(f"v3_allowed={final['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'final_branch_decision_snapshot.json'}"
        )
    if final["b5_allowed"] or final["b6_allowed"] or final["v3_allowed"]:
        raise typer.Exit(code=1)


@weight_research_app.command("b2-b3-v2-research")
def weight_research_b2_b3_v2_research_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2/B3 v2 research output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-557~564 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-557~564 B2 evidence expansion and B3 signal-only precheck v2."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_b3_v2_research(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    branch = payloads["branch_decision_after_b2_v2_b3_precheck_v2"]
    typer.echo(f"branch_decision_status={branch['status']}")
    typer.echo(f"b5_allowed={branch['b5_allowed']}")
    typer.echo(f"b6_allowed={branch['b6_allowed']}")
    typer.echo(f"v3_allowed={branch['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            "Source Alias："
            f"{DEFAULT_RESEARCH_SOURCE_DIR / 'branch_decision_after_b2_v2_b3_precheck_v2.json'}"
        )
    if branch["b5_allowed"] or branch["b6_allowed"] or branch["v3_allowed"]:
        raise typer.Exit(code=1)


@weight_research_app.command("b2-full-diagnostic-research")
def weight_research_b2_full_diagnostic_research_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 full diagnostic output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-565~574 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-565~574 B2 full diagnostic and B3 signal resolution."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_full_diagnostic_research(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    snapshot = payloads["b2_b3_branch_status_snapshot"]
    typer.echo(f"branch_snapshot_status={snapshot['status']}")
    typer.echo(f"B4_retest_allowed={snapshot['B4_retest_allowed']}")
    typer.echo(f"b5_allowed={snapshot['b5_allowed']}")
    typer.echo(f"b6_allowed={snapshot['b6_allowed']}")
    typer.echo(f"v3_allowed={snapshot['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_b3_branch_status_snapshot.json'}"
        )
    if (
        snapshot["B4_retest_allowed"]
        or snapshot["b5_allowed"]
        or snapshot["b6_allowed"]
        or snapshot["v3_allowed"]
    ):
        raise typer.Exit(code=1)


@weight_research_app.command("b2-control-window-research")
def weight_research_b2_control_window_research_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 control-window output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-575~580 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-575~580 independent B2 control-window evidence completion."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_control_window_research(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    snapshot = payloads["b2_path_decision_snapshot"]
    typer.echo(f"b2_path_decision_status={snapshot['status']}")
    typer.echo(f"B4_retest_allowed={snapshot['B4_retest_allowed']}")
    typer.echo(f"b5_allowed={snapshot['b5_allowed']}")
    typer.echo(f"b6_allowed={snapshot['b6_allowed']}")
    typer.echo(f"v3_allowed={snapshot['v3_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_path_decision_snapshot.json'}"
        )
    if (
        snapshot["B4_retest_allowed"]
        or snapshot["b5_allowed"]
        or snapshot["b6_allowed"]
        or snapshot["v3_allowed"]
    ):
        raise typer.Exit(code=1)


@weight_research_app.command("b2-followup-research")
def weight_research_b2_followup_research_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 follow-up output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-582~587 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-582~587 B2 complete diagnostic follow-up decision plan."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_followup_research(
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    gate = payloads["b2_gate_v4_decision"]
    snapshot = payloads["b2_research_branch_snapshot"]
    typer.echo(f"b2_gate_v4_status={gate['status']}")
    typer.echo(f"b2_research_branch_status={snapshot['status']}")
    typer.echo(f"B4_retest_allowed={snapshot['B4_retest_allowed']}")
    typer.echo(f"b5_allowed={snapshot['b5_allowed']}")
    typer.echo(f"b6_allowed={snapshot['b6_allowed']}")
    typer.echo(f"v3_allowed={snapshot['v3_allowed']}")
    typer.echo(f"paper_shadow_allowed={snapshot['paper_shadow_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_research_branch_snapshot.json'}"
        )
    if (
        snapshot["B4_retest_allowed"]
        or snapshot["b5_allowed"]
        or snapshot["b6_allowed"]
        or snapshot["v3_allowed"]
        or snapshot["paper_shadow_allowed"]
    ):
        raise typer.Exit(code=1)


@weight_research_app.command("b2-targeted-evidence-research")
def weight_research_b2_targeted_evidence_research_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 targeted evidence output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-588~596 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-588~596 B2 targeted evidence and role-scope diagnostics."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_targeted_evidence_research(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    gate = payloads["b2_gate_v5"]
    snapshot = payloads["b2_research_branch_snapshot_v2"]
    typer.echo(f"b2_gate_v5_status={gate['status']}")
    typer.echo(f"b2_research_branch_snapshot_v2_status={snapshot['status']}")
    typer.echo(f"B4_retest_allowed={snapshot['B4_retest_allowed']}")
    typer.echo(f"b5_allowed={snapshot['b5_allowed']}")
    typer.echo(f"b6_allowed={snapshot['b6_allowed']}")
    typer.echo(f"v3_allowed={snapshot['v3_allowed']}")
    typer.echo(f"paper_shadow_allowed={snapshot['paper_shadow_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(
            f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_research_branch_snapshot_v2.json'}"
        )
    if (
        snapshot["B4_retest_allowed"]
        or snapshot["b5_allowed"]
        or snapshot["b6_allowed"]
        or snapshot["v3_allowed"]
        or snapshot["paper_shadow_allowed"]
    ):
        raise typer.Exit(code=1)


@weight_research_app.command("b2-final-decision-research")
def weight_research_b2_final_decision_research_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B2 final decision output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    write_source_alias: Annotated[
        bool,
        typer.Option(
            "--write-source-alias/--no-write-source-alias",
            help="Also update docs/research TRADING-597~604 aliases。",
        ),
    ] = False,
) -> None:
    """Run TRADING-597~604 B2 final evidence and role decision diagnostics."""
    alias_dir = DEFAULT_RESEARCH_SOURCE_DIR if write_source_alias else None
    payloads, paths = run_b2_final_evidence_role_decision(
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    gate = payloads["b2_final_research_gate"]
    snapshot = payloads["b2_branch_snapshot_final"]
    typer.echo(f"b2_final_research_gate_status={gate['status']}")
    typer.echo(f"b2_branch_snapshot_final_status={snapshot['status']}")
    typer.echo(f"B4_retest_allowed={snapshot['B4_retest_allowed']}")
    typer.echo(f"b5_allowed={snapshot['b5_allowed']}")
    typer.echo(f"b6_allowed={snapshot['b6_allowed']}")
    typer.echo(f"v3_allowed={snapshot['v3_allowed']}")
    typer.echo(f"paper_shadow_allowed={snapshot['paper_shadow_allowed']}")
    for name, (json_path, markdown_path) in sorted(paths.items()):
        typer.echo(f"{name}.json：{json_path}")
        typer.echo(f"{name}.md：{markdown_path}")
    if write_source_alias:
        typer.echo(f"Source Alias：{DEFAULT_RESEARCH_SOURCE_DIR / 'b2_branch_snapshot_final.json'}")
    if (
        snapshot["B4_retest_allowed"]
        or snapshot["b5_allowed"]
        or snapshot["b6_allowed"]
        or snapshot["v3_allowed"]
        or snapshot["paper_shadow_allowed"]
    ):
        raise typer.Exit(code=1)


@weight_research_app.command("run-b1")
def weight_research_run_b1_command(
    prices_path: Annotated[Path, typer.Option("--prices-path", help="价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    start: Annotated[str, typer.Option("--from", help="B1 mini-backfill start date。")] = (
        "2023-01-03"
    ),
    end: Annotated[str, typer.Option("--to", help="B1 mini-backfill end date。")] = ("2023-07-31"),
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="B1 output directory。"),
    ] = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
) -> None:
    """Run B1 execution-control only after 511A-C contract validation."""
    payload, json_path, markdown_path, daily_path = run_b1_execution_control(
        prices_path=prices_path,
        rates_path=rates_path,
        start=_parse_date(start),
        end=_parse_date(end),
        output_dir=output_dir,
        data_quality_output_path=data_quality_output_path,
    )
    typer.echo(f"b1_execution_control_status={payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")
    typer.echo(f"Daily：{daily_path}")
    if payload["status"] == "B1_BLOCKED":
        raise typer.Exit(code=1)
