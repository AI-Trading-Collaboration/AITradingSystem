from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer

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
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    available_price_symbols as _available_price_symbols,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    price_requested_date_range as _price_requested_date_range,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    quality_metadata as _quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import ai_confirmation_app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


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
