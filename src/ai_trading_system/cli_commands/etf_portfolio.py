from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.config import (
    PROJECT_ROOT,
)
from ai_trading_system.etf_portfolio.ai_attribution import (
    DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
    DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    build_ai_attribution_dataset,
    build_ai_attribution_report,
    build_ai_attribution_validation_report,
    load_ai_confirmation_report_payloads,
    write_ai_attribution_dataset,
    write_ai_attribution_report,
    write_ai_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.ai_confirmation import (
    DEFAULT_AI_CONFIRMATION_FEATURE_DIR,
    DEFAULT_AI_CONFIRMATION_OVERLAY_DIR,
    DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH,
    DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH,
    DEFAULT_AI_CONFIRMATION_VALIDATION_DIR,
    ai_confirmation_price_group_ids,
    all_enabled_price_tickers,
    build_ai_confirmation_breadth_features,
    build_ai_confirmation_report,
    build_ai_confirmation_shadow_overlay_experiment,
    build_ai_confirmation_validation_report,
    latest_ai_confirmation_report_path,
    load_ai_confirmation_base_weights,
    load_ai_confirmation_events,
    load_ai_confirmation_policy_config,
    load_ai_confirmation_universe_config,
    validate_ai_confirmation_data_availability,
    write_ai_confirmation_breadth_features,
    write_ai_confirmation_report,
    write_ai_confirmation_shadow_overlay,
    write_ai_confirmation_validation_report,
)
from ai_trading_system.etf_portfolio.allocation import (
    allocate_portfolio,
    latest_weights_from_file,
    load_allocation,
    write_allocation,
)
from ai_trading_system.etf_portfolio.backtest import run_portfolio_backtest, write_backtest_run
from ai_trading_system.etf_portfolio.credibility import (
    DEFAULT_ETF_CREDIBILITY_DIR,
    build_credibility_gate,
    write_credibility_gate,
)
from ai_trading_system.etf_portfolio.data import (
    load_standard_prices,
)
from ai_trading_system.etf_portfolio.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
    DEFAULT_DECISION_JOURNAL_REPORT_DIR,
    DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
    DecisionJournalError,
    add_decision_entry,
    build_candidate_state_update_proposals,
    build_decision_entry_from_weekly_review,
    build_decision_journal_analytics,
    build_decision_journal_report,
    build_decision_journal_validation_report,
    decision_entries,
    load_decision_journal,
    remove_decision_entry,
    update_decision_entry,
    write_decision_journal,
    write_decision_journal_analytics,
    write_decision_journal_report,
    write_decision_journal_validation_report,
    write_decision_state_update_proposals,
)
from ai_trading_system.etf_portfolio.experiments import (
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
)
from ai_trading_system.etf_portfolio.features import (
    build_feature_store,
    load_feature_store,
    write_feature_store,
)
from ai_trading_system.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_CONFIG_PATH,
    DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    DEFAULT_ETF_FORWARD_REPORT_DIR,
    run_forward_dashboard,
    run_forward_update,
    run_forward_validation,
    run_forward_watchlist,
    run_forward_weekly_review,
)
from ai_trading_system.etf_portfolio.governance import (
    DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    evaluate_parameter_candidate,
    load_parameter_governance_policy,
    read_parameter_candidate,
    write_parameter_governance_summary,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    DEFAULT_ETF_BACKTEST_DIR,
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_LEDGER_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REGIME_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    DEFAULT_ETF_TARGET_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import (
    build_confirmation_scores,
    build_governance_status,
    build_portfolio_attribution,
    build_relative_strength_table,
    evaluate_event_risk,
    write_frame_and_report,
)
from ai_trading_system.etf_portfolio.regime import (
    generate_regime_for_date,
    load_regimes,
    select_regime_for_date,
    write_regime,
)
from ai_trading_system.etf_portfolio.reporting import render_daily_brief, write_daily_brief
from ai_trading_system.etf_portfolio.signals import (
    generate_signals_for_date,
    load_signals,
    select_signals_for_date,
    signals_to_frame,
    write_signals,
)
from ai_trading_system.etf_portfolio.simulation import (
    evaluate_simulation_ledger,
    record_simulation_snapshot,
    summarize_simulation_for_brief,
)
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    write_allocation_stability_diagnostics,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    available_price_symbols as _available_price_symbols,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload as _load_optional_json_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    p1_quality_metadata as _p1_quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    parse_date as _parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    price_requested_date_range as _price_requested_date_range,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    quality_metadata as _quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_date as _resolve_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_feature_date as _resolve_feature_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_compare_command as experiments_compare_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_register_command as experiments_register_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_run_command as experiments_run_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    ai_attribution_app,
    ai_confirmation_app,
    attribution_app,
    backtest_app,
    confirmation_app,
    credibility_app,
    decision_journal_app,
    etf_app,
    events_app,
    forward_app,
    governance_app,
    portfolio_app,
    regime_app,
    relative_strength_app,
    report_app,
    run_app,
    signals_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_shadow_app as dynamic_shadow_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    experiments_app as experiments_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    p2_app as p2_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    satellite_app as satellite_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    simulation_app as simulation_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    weight_calibration_app as weight_calibration_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    weight_research_app as weight_research_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_evaluate_command as simulation_evaluate_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_record_command as simulation_record_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_report_command as simulation_report_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.weekly_review import (
    weekly_review_date as _weekly_review_date,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


@etf_app.command("validate-config")
def validate_config_command() -> None:
    """校验 ETF P0 配置。"""
    config = load_etf_config_bundle()
    typer.echo("ETF 配置校验通过。")
    typer.echo(f"model_version={config.strategy.model.version}")
    typer.echo(f"config_hash={config.config_hash}")
    typer.echo(f"assets={', '.join(config.assets.assets)}")


@relative_strength_app.command("report")
def relative_strength_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 relative strength table/report。"""
    config = load_etf_config_bundle()
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    table = build_relative_strength_table(features, config=config, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_relative_strength.csv"
    md_path = output_dir / f"{run_date.isoformat()}_relative_strength.md"
    write_frame_and_report(
        table,
        csv_path,
        md_path,
        "ETF Relative Strength Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF relative strength report：{md_path}")


@confirmation_app.command("report")
def confirmation_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 AI / semiconductor confirmation scores。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    rs_table = build_relative_strength_table(features, config=config, run_date=run_date)
    scores = build_confirmation_scores(rs_table, p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.csv"
    md_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.md"
    write_frame_and_report(
        scores,
        csv_path,
        md_path,
        "ETF Confirmation Scores",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF confirmation scores：{md_path}")


@ai_confirmation_app.command("features")
def ai_confirmation_features_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_FEATURE_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066B AI / semiconductor breadth features。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation features。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 feature build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    records = build_ai_confirmation_breadth_features(
        prices,
        config=ai_config,
        run_date=run_date,
    )
    paths = write_ai_confirmation_breadth_features(
        records,
        output_dir=output_dir,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
    )
    typer.echo(f"AI confirmation features JSON：{paths['json']}")
    typer.echo(f"AI confirmation features CSV：{paths['csv']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo(f"ai_data_availability_status={availability['status']}")


@ai_confirmation_app.command("report")
def ai_confirmation_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="AI confirmation report 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
    events_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI event calendar JSON/CSV。缺失时按 no active events 处理。"),
    ] = None,
) -> None:
    """生成 TRADING-066G standalone AI confirmation JSON/Markdown report。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    policy_config = load_ai_confirmation_policy_config(policy_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 report build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    payload = build_ai_confirmation_report(
        prices=prices,
        events=load_ai_confirmation_events(events_path),
        universe_config=ai_config,
        policy_config=policy_config,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range=_price_requested_date_range(prices, run_date),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"ai_confirmation_report_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_report(payload, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation report JSON：{json_path}")
    typer.echo(f"AI confirmation report Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={payload['AIConfirmationScore']['score_value']}")
    typer.echo(f"score_band={payload['AIConfirmationScore']['score_band']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@ai_confirmation_app.command("overlay")
def ai_confirmation_overlay_command(
    candidate_id: Annotated[
        str,
        typer.Option("--candidate", help="Base candidate id for the shadow overlay output."),
    ],
    base_weights_path: Annotated[
        Path,
        typer.Option(help="JSON/YAML/CSV base candidate weights; read-only input."),
    ],
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="AI confirmation report JSON；缺省读取 latest report。"),
    ] = None,
    report_dir: Annotated[Path, typer.Option(help="AI confirmation report 查找目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="shadow overlay 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_OVERLAY_DIR
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066H candidate-only shadow overlay；不写 production weights。"""
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    report_path = ai_confirmation_report_path or latest_ai_confirmation_report_path(
        report_dir,
        as_of=run_date if date_option else None,
    )
    if report_path is None:
        typer.echo("AI confirmation report not found; run report before overlay.")
        raise typer.Exit(code=1)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    if isinstance(report_payload, dict) and report_payload.get("date"):
        run_date = date.fromisoformat(str(report_payload["date"]))
    overlay = build_ai_confirmation_shadow_overlay_experiment(
        base_weights=load_ai_confirmation_base_weights(base_weights_path),
        ai_confirmation_payload=report_payload,
        policy_config=load_ai_confirmation_policy_config(policy_path),
        run_date=run_date,
        base_candidate_id=candidate_id,
    )
    stem = f"ai_confirmation_overlay_{run_date.isoformat()}_{candidate_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_shadow_overlay(overlay, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation shadow overlay JSON：{json_path}")
    typer.echo(f"AI confirmation shadow overlay Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={overlay['AIConfirmationScore']}")
    typer.echo(f"overlay_direction={overlay['overlay_adjustment']['direction']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@ai_confirmation_app.command("validate")
def ai_confirmation_validate_command(
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_VALIDATION_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
    report_registry_path: Annotated[Path, typer.Option(help="report registry config。")] = (
        DEFAULT_REPORT_REGISTRY_PATH
    ),
) -> None:
    """生成 TRADING-066J final AI confirmation validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    generated = datetime.now(tz=UTC)
    payload = build_ai_confirmation_validation_report(
        universe_config=load_ai_confirmation_universe_config(universe_path),
        policy_config=load_ai_confirmation_policy_config(policy_path),
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        generated_at=generated.isoformat(),
    )
    stem = f"ai_confirmation_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_validation_report(
        payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"AI confirmation validation JSON：{json_path}")
    typer.echo(f"AI confirmation validation Markdown：{markdown_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@ai_attribution_app.command("build")
def ai_attribution_build_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution dataset 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
) -> None:
    """生成 TRADING-072A AI confirmation forward attribution dataset。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution build。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    payload = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    paths = write_ai_attribution_dataset(payload, output_dir=output_dir)
    typer.echo(f"AI attribution dataset JSON：{paths['json']}")
    typer.echo(f"AI attribution dataset CSV：{paths['csv']}")
    typer.echo(f"record_count={payload['record_count']}")
    typer.echo(f"available_sample_count={payload['available_sample_count']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("report")
def ai_attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    dataset_output_dir: Annotated[
        Path,
        typer.Option(help="同步写出的 AI attribution dataset 目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution report 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-072H AI confirmation forward attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    dataset_paths = write_ai_attribution_dataset(dataset, output_dir=dataset_output_dir)
    payload = build_ai_attribution_report(dataset)
    paths = write_ai_attribution_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution report JSON：{paths['json']}")
    typer.echo(f"AI attribution report Markdown：{paths['markdown']}")
    typer.echo(f"AI attribution dataset JSON：{dataset_paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_sample_count={dataset['available_sample_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("validate")
def ai_attribution_validate_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution validation 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-072H attribution report JSON path。"),
    ] = None,
) -> None:
    """执行 TRADING-072J AI attribution validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    payload = build_ai_attribution_validation_report(
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_ai_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@attribution_app.command("report")
def attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 portfolio attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 attribution。")
        raise typer.Exit(code=1)
    quality_metadata = _quality_metadata(quality_report)
    run_date = _resolve_date(date_option, prices=prices)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    attribution = build_portfolio_attribution(allocation, prices, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_attribution.csv"
    md_path = output_dir / f"{run_date.isoformat()}_attribution.md"
    write_frame_and_report(
        attribution,
        csv_path,
        md_path,
        "ETF Portfolio Attribution",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF attribution：{md_path}")


@events_app.command("risk-flag")
def event_risk_flag_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 basic event calendar risk flags。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    flags = evaluate_event_risk(p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.csv"
    md_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.md"
    write_frame_and_report(flags, csv_path, md_path, "ETF Event Risk Flags")
    typer.echo(f"ETF event risk flags：{md_path}")


@governance_app.command("status")
def governance_status_command(
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 weight governance status；manual-review-only。"""
    config = load_etf_config_bundle()
    status = build_governance_status(config)
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_governance_status.csv"
    md_path = output_dir / f"{run_date.isoformat()}_governance_status.md"
    write_frame_and_report(status, csv_path, md_path, "ETF Weight Governance Status")
    typer.echo(f"ETF governance status：{md_path}")


@governance_app.command("summary")
def governance_summary_command(
    candidate_path: Annotated[
        Path | None,
        typer.Option("--candidate", help="候选参数 JSON；缺省时输出 NO_CANDIDATE。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    policy_path: Annotated[
        Path,
        typer.Option(help="ETF 参数治理 policy。"),
    ] = DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_REPORT_DIR / "governance",
) -> None:
    """生成 ETF parameter governance summary；只允许人工复核，不自动 promotion。"""
    config = load_etf_config_bundle()
    if candidate_path is not None and not candidate_path.exists():
        raise typer.BadParameter(f"候选参数 JSON 不存在：{candidate_path}")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    policy = load_parameter_governance_policy(policy_path)
    candidate = read_parameter_candidate(candidate_path)
    payload = evaluate_parameter_candidate(
        config=config,
        policy=policy,
        candidate=candidate,
        candidate_path=candidate_path,
        policy_config_path=policy_path,
    )
    json_path = output_dir / f"{run_date.isoformat()}_parameter_governance.json"
    md_path = output_dir / f"{run_date.isoformat()}_parameter_governance.md"
    write_parameter_governance_summary(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter governance：{md_path}")
    typer.echo(f"promotion_status={payload['promotion_status']}")
    typer.echo(f"production_effect={payload['production_effect']}")


@credibility_app.command("validate")
def credibility_validate_command(
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_CREDIBILITY_DIR,
) -> None:
    """聚合 TRADING-063A~J credibility checks；fail closed。"""
    config = load_etf_config_bundle()
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    payload = build_credibility_gate(config=config)
    json_path = output_dir / f"{run_date.isoformat()}_credibility_gate.json"
    md_path = output_dir / f"{run_date.isoformat()}_credibility_gate.md"
    write_credibility_gate(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF credibility gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"production_effect={payload['production_effect']}")
    typer.echo(f"broker_action={payload['broker_action']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@forward_app.command("update")
def forward_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward evaluation 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 输出路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_dir: Annotated[Path, typer.Option(help="forward update 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
) -> None:
    """更新 active shadow candidates 的 evaluation-only forward performance。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    try:
        payload = run_forward_update(
            as_of=run_date,
            config_path=config_path,
            registry_path=registry_path,
            decision_ledger_path=decision_ledger_path,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except ValueError as exc:
        typer.echo(f"ETF forward update blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF forward update：{output_dir / f'forward_update_{run_date.isoformat()}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("dashboard")
def forward_dashboard_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="dashboard 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest forward update artifact。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    update_dir: Annotated[Path, typer.Option(help="forward update artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
    output_dir: Annotated[Path, typer.Option(help="dashboard 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
) -> None:
    """生成 candidate vs baseline vs benchmark forward dashboard。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_dashboard(
        as_of=run_date,
        latest=latest or date_option is None,
        registry_path=registry_path,
        update_dir=update_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward dashboard：{output_dir / f'forward_dashboard_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['status_summary']['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("weekly-review")
def forward_weekly_review_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="weekly review 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "weekly_reviews"
    ),
) -> None:
    """生成 TRADING-065 observe-only weekly review。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_weekly_review(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward weekly review：{output_dir / f'weekly_review_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['review_period']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("watchlist")
def forward_watchlist_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="watchlist 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="watchlist 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist"
    ),
) -> None:
    """生成本地 ETF forward watchlist；不发送外部 alert。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_watchlist(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward watchlist：{output_dir / f'forward_watchlist_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attention_count={payload['summary']['item_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("validate")
def forward_validate_command(
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "validation"
    ),
) -> None:
    """生成 TRADING-065 final forward simulation validation gate。"""
    payload = run_forward_validation(
        config_path=config_path,
        registry_path=registry_path,
        decision_ledger_path=decision_ledger_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"ETF forward validation gate：{output_dir}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@decision_journal_app.command("add")
def decision_journal_add_command(
    weekly_review_path: Annotated[
        Path,
        typer.Option(help="TRADING-068 weekly review JSON path。"),
    ],
    action_item_id: Annotated[
        str,
        typer.Option(help="weekly review manual_review_actions[].action_id。"),
    ],
    human_decision: Annotated[
        str,
        typer.Option(help="人工决策摘要。"),
    ],
    decision_status: Annotated[
        str,
        typer.Option(help="decision_status enum value。"),
    ],
    rationale: Annotated[
        str,
        typer.Option(help="人工决策依据。"),
    ],
    confidence: Annotated[
        float,
        typer.Option(help="人工信心 0.0-1.0。"),
    ],
    follow_up_task: Annotated[
        str,
        typer.Option(help="后续人工任务。"),
    ],
    linked_candidate: Annotated[
        str,
        typer.Option(help="关联 candidate / portfolio review target。"),
    ],
    linked_report: Annotated[
        Path | None,
        typer.Option(help="可选关联报告；默认使用 weekly review JSON。"),
    ] = None,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """追加人工 portfolio decision journal entry。"""
    try:
        journal = load_decision_journal(journal_path)
        entry = build_decision_entry_from_weekly_review(
            weekly_review_path=weekly_review_path,
            action_item_id=action_item_id,
            human_decision=human_decision,
            decision_status=decision_status,
            rationale=rationale,
            confidence=confidence,
            follow_up_task=follow_up_task,
            linked_candidate=linked_candidate,
            linked_report=linked_report,
        )
        updated = add_decision_entry(journal, entry)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry added：{journal_path}")
    typer.echo(f"decision_id={entry['decision_id']}")
    typer.echo(f"review_id={entry['review_id']}")
    typer.echo(f"action_item_id={entry['action_item_id']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("update")
def decision_journal_update_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to update。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    human_decision: Annotated[
        str | None,
        typer.Option(help="更新人工决策摘要。"),
    ] = None,
    decision_status: Annotated[
        str | None,
        typer.Option(help="更新 decision_status enum value。"),
    ] = None,
    rationale: Annotated[
        str | None,
        typer.Option(help="更新 rationale。"),
    ] = None,
    confidence: Annotated[
        float | None,
        typer.Option(help="更新 confidence 0.0-1.0。"),
    ] = None,
    follow_up_task: Annotated[
        str | None,
        typer.Option(help="更新 follow-up task。"),
    ] = None,
    linked_candidate: Annotated[
        str | None,
        typer.Option(help="更新 linked candidate。"),
    ] = None,
    linked_report: Annotated[
        Path | None,
        typer.Option(help="更新 linked report。"),
    ] = None,
) -> None:
    """更新人工 portfolio decision journal entry。"""
    updates = {
        "human_decision": human_decision,
        "decision_status": decision_status,
        "rationale": rationale,
        "confidence": confidence,
        "follow_up_task": follow_up_task,
        "linked_candidate": linked_candidate,
        "linked_report": None if linked_report is None else str(linked_report),
    }
    try:
        if not any(value is not None for value in updates.values()):
            raise DecisionJournalError("update requires at least one field")
        journal = load_decision_journal(journal_path)
        updated = update_decision_entry(journal, decision_id=decision_id, updates=updates)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry updated：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("list")
def decision_journal_list_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="输出 JSON payload。"),
    ] = False,
) -> None:
    """列出 active portfolio decision journal entries。"""
    try:
        journal = load_decision_journal(journal_path)
        entries = decision_entries(journal)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        raise typer.Exit(code=1) from exc
    if as_json:
        typer.echo(
            json.dumps(
                {"journal_path": str(journal_path), "entries": entries},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    typer.echo(f"ETF decision journal entries：{len(entries)}")
    for entry in entries:
        typer.echo(
            " | ".join(
                [
                    str(entry.get("decision_id")),
                    str(entry.get("review_date")),
                    str(entry.get("decision_status")),
                    str(entry.get("action_item_id")),
                    str(entry.get("linked_candidate")),
                ]
            )
        )
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("remove")
def decision_journal_remove_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to remove from active entries。"),
    ],
    reason: Annotated[
        str,
        typer.Option(help="remove reason；entry is moved to removed_entries audit trail。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """从 active journal 移除 entry，并保留 removed_entries audit trail。"""
    try:
        journal = load_decision_journal(journal_path)
        updated = remove_decision_entry(journal, decision_id=decision_id, reason=reason)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry removed：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("report")
def decision_journal_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 decision journal report。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal report 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal JSON/Markdown/HTML summary。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_report(
            journal,
            as_of=run_date,
            journal_path=journal_path,
        )
        json_path = output_dir / f"decision_journal_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_journal_{run_date.isoformat()}.md"
        html_path = output_dir / f"decision_journal_{run_date.isoformat()}.html"
        write_decision_journal_report(
            payload,
            json_path=json_path,
            markdown_path=md_path,
            html_path=html_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal report blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal report：{md_path}")
    typer.echo(f"json={json_path}")
    typer.echo(f"html={html_path}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("analytics")
def decision_journal_analytics_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal analytics 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 analytics。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal analytics 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal outcome analytics JSON。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_analytics(journal)
        output_path = output_dir / f"decision_journal_analytics_{run_date.isoformat()}.json"
        write_decision_journal_analytics(payload, output_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal analytics blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal analytics：{output_path}")
    typer.echo(f"entry_count={payload['entry_count']}")
    typer.echo(f"follow_up_task_count={payload['follow_up_task_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("propose-state-updates")
def decision_journal_propose_state_updates_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="proposal 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 proposal。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision state proposal 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
) -> None:
    """生成 candidate state update proposal；不修改 candidate registry。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_candidate_state_update_proposals(journal)
        json_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.md"
        write_decision_state_update_proposals(
            payload,
            json_path=json_path,
            markdown_path=md_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision state proposal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision state update proposal：{md_path}")
    typer.echo(f"proposal_count={payload['proposal_count']}")
    typer.echo("state_mutation_performed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("validate")
def decision_journal_validate_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal validation 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
) -> None:
    """生成 TRADING-069 decision journal validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_decision_journal_validation_report(
        journal_path=journal_path,
        generated_at=generated,
    )
    stem = f"decision_journal_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_decision_journal_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF decision journal validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@signals_app.command("generate")
def signals_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="信号日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="信号输出路径。")] = DEFAULT_ETF_SIGNAL_PATH,
) -> None:
    """生成 ETF signals，不输出仓位。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    records = generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    write_signals(records, output_path)
    typer.echo(f"ETF signals 已写入：{output_path}（date={run_date.isoformat()}）")


@regime_app.command("generate")
def regime_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="regime 日期或 latest。"),
    ] = None,
    output_path: Annotated[Path, typer.Option(help="Regime 输出路径。")] = DEFAULT_ETF_REGIME_PATH,
) -> None:
    """生成 ETF market regime。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    run_date = _resolve_feature_date(date_option, features)
    record = generate_regime_for_date(
        features,
        signals,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(record, output_path)
    typer.echo(f"ETF regime 已写入：{output_path}（{record.regime}）")


@portfolio_app.command("allocate")
def portfolio_allocate_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="组合日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="目标权重输出路径。")] = DEFAULT_ETF_TARGET_PATH,
) -> None:
    """由 ETF signals + regime 生成目标权重。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 allocation。")
        raise typer.Exit(code=1)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_date(date_option, prices=prices)
    signal_rows = select_signals_for_date(signals, run_date)
    regime_row = select_regime_for_date(regimes, run_date)
    previous_weights = latest_weights_from_file(output_path)
    records = allocate_portfolio(
        signal_rows,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=str(regime_row["regime"]),
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=previous_weights,
    )
    write_allocation(records, output_path)
    typer.echo(f"ETF target weights 已写入：{output_path}")
    typer.echo(f"sum={sum(record.target_weight for record in records):.6f}")


@backtest_app.command("run")
def backtest_run_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    start: Annotated[str | None, typer.Option("--from", help="回测开始日期。")] = None,
    end: Annotated[str | None, typer.Option("--to", help="回测结束日期。")] = None,
    output_dir: Annotated[Path, typer.Option(help="回测输出目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    fast: Annotated[bool, typer.Option("--fast", help="只跑最近约 90 个交易日 smoke。")] = False,
) -> None:
    """运行 ETF 组合级回测。"""
    config = load_etf_config_bundle(backtest_path=config_path)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 backtest。")
        raise typer.Exit(code=1)
    run = run_portfolio_backtest(
        prices,
        config=config,
        quality_report=quality_report,
        start=_parse_date(start) if start else None,
        end=_resolve_date(end, prices=prices) if end else None,
        fast=fast,
    )
    (
        daily_path,
        weights_path,
        trades_path,
        summary_json,
        metrics_json,
        summary_md,
        stability_json,
        stability_md,
    ) = write_backtest_run(run, output_dir)
    typer.echo(f"ETF backtest 完成：{run.run_id}")
    typer.echo(f"Daily：{daily_path}")
    typer.echo(f"Weights：{weights_path}")
    typer.echo(f"Trades：{trades_path}")
    typer.echo(f"Summary JSON：{summary_json}")
    typer.echo(f"Metrics JSON：{metrics_json}")
    typer.echo(f"Summary Markdown：{summary_md}")
    typer.echo(f"Stability JSON：{stability_json}")
    typer.echo(f"Stability Markdown：{stability_md}")


@backtest_app.command("report")
def backtest_report_command(
    run_id: Annotated[str, typer.Option(help="回测 run_id。")],
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
) -> None:
    """显示已生成 ETF backtest summary 路径。"""
    summary = output_dir / run_id / "summary.md"
    if not summary.exists():
        raise typer.BadParameter(f"未找到 ETF backtest summary：{summary}")
    typer.echo(str(summary))


@backtest_app.command("diagnostics")
def backtest_diagnostics_command(
    run_id: Annotated[str | None, typer.Option(help="回测 run_id；省略时使用 latest。")] = None,
    latest: Annotated[bool, typer.Option("--latest", help="读取最新 backtest run。")] = False,
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
) -> None:
    """从已生成 backtest run 计算 allocation stability diagnostics。"""
    selected_run_dir = _resolve_backtest_run_dir(output_dir, run_id=run_id, latest=latest)
    daily_path = selected_run_dir / "daily.csv"
    weights_path = selected_run_dir / "weights.csv"
    if not daily_path.exists() or not weights_path.exists():
        raise typer.BadParameter(f"backtest run 缺少 daily.csv 或 weights.csv：{selected_run_dir}")
    config = load_etf_config_bundle(backtest_path=config_path)
    payload = build_allocation_stability_diagnostics(
        pd.read_csv(daily_path),
        pd.read_csv(weights_path),
        max_daily_turnover=config.risk.portfolio_constraints.max_daily_turnover,
        max_rebalance_trade_weight=config.risk.portfolio_constraints.max_rebalance_trade_weight,
    )
    json_path, markdown_path = write_allocation_stability_diagnostics(
        payload,
        selected_run_dir / "stability_diagnostics.json",
        selected_run_dir / "stability_diagnostics.md",
    )
    typer.echo(f"ETF allocation stability status：{payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")


@report_app.command("daily")
def report_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日报日期或 latest。")] = None,
    output_path: Annotated[Path | None, typer.Option(help="日报输出路径。")] = None,
) -> None:
    """生成 ETF Daily Portfolio Brief。"""
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=ledger_path,
        date_option=date_option,
        output_path=output_path,
    )
    typer.echo(f"ETF daily brief：{report_path}")


@run_app.command("daily")
def run_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="运行日期或 latest。")] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="写入 dry-run artifact 目录。"),
    ] = False,
) -> None:
    """执行 ETF P0 日流程：validate -> features -> signals -> regime -> allocation -> report。"""
    root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run"
        if dry_run
        else PROJECT_ROOT / "data" / "etf_portfolio"
    )
    report_root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run_reports"
        if dry_run
        else DEFAULT_ETF_REPORT_DIR
    )
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 daily run。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    features_path = root / "features.csv"
    signals_path = root / "signals.csv"
    regime_path = root / "regimes.csv"
    allocation_path = root / "target_weights.csv"
    write_feature_store(features, features_path)
    signal_records = generate_signals_for_date(
        features,
        strategy=config.strategy,
        run_date=run_date,
    )
    write_signals(signal_records, signals_path)
    regime_record = generate_regime_for_date(
        features,
        signals_to_frame(signal_records),
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(regime_record, regime_path)
    allocation_records = allocate_portfolio(
        signals_to_frame(signal_records),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=regime_record.regime,
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=None if dry_run else latest_weights_from_file(DEFAULT_ETF_TARGET_PATH),
    )
    write_allocation(allocation_records, allocation_path)
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=DEFAULT_ETF_LEDGER_PATH,
        date_option=run_date.isoformat(),
        output_path=report_root / f"{run_date.isoformat()}_portfolio_brief.md",
    )
    if not dry_run:
        record_simulation_snapshot(
            allocation_records=allocation_records,
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            report_path=report_path,
        )
        evaluate_simulation_ledger(
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            prices=prices,
            as_of=run_date,
        )
    typer.echo(f"ETF daily run 完成：{run_date.isoformat()}")
    typer.echo(f"dry_run={str(dry_run).lower()}")
    typer.echo(f"report={report_path}")
    typer.echo(f"data_quality_status={quality_report.status}")


def _generate_daily_report(
    *,
    prices_path: Path,
    signals_path: Path,
    regime_path: Path,
    allocation_path: Path,
    ledger_path: Path | None,
    date_option: str | None,
    output_path: Path | None,
) -> Path:
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    run_date = _resolve_date(date_option, prices=prices)
    signals = select_signals_for_date(load_signals(signals_path), run_date)
    regime = select_regime_for_date(load_regimes(regime_path), run_date)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    if allocation.empty:
        raise typer.BadParameter(f"目标权重缺少日期：{run_date.isoformat()}")
    report_path = (
        output_path or DEFAULT_ETF_REPORT_DIR / f"{run_date.isoformat()}_portfolio_brief.md"
    )
    markdown = render_daily_brief(
        run_date=run_date,
        config=config,
        quality_report=quality_report,
        signals=signals,
        regime=regime,
        allocation=allocation,
        simulation_summary=(
            summarize_simulation_for_brief(ledger_path, as_of=run_date)
            if ledger_path is not None
            else None
        ),
    )
    return write_daily_brief(markdown, report_path)


def _resolve_backtest_run_dir(output_dir: Path, *, run_id: str | None, latest: bool) -> Path:
    if run_id is not None and latest:
        raise typer.BadParameter("run_id 和 --latest 只能选择一个")
    if run_id is not None:
        run_dir = output_dir / run_id
        if not run_dir.exists():
            raise typer.BadParameter(f"未找到 ETF backtest run：{run_dir}")
        return run_dir
    if not output_dir.exists():
        raise typer.BadParameter(f"ETF backtest 输出目录不存在：{output_dir}")
    candidates = [
        item for item in output_dir.iterdir() if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise typer.BadParameter(f"未找到 ETF backtest run：{output_dir}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)


def _records_obj(value: object) -> list[dict[str, object]]:
    return (
        [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []
    )


def _find_method_obj(value: object, method: str) -> dict[str, object]:
    for row in _records_obj(value):
        if row.get("method") == method or row.get("target_method") == method:
            return row
    return {}


def _texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)] if value != "" else []
