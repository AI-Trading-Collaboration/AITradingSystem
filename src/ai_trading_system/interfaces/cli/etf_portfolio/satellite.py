from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.features import load_feature_store
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REGIME_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import evaluate_satellite_candidates, write_frame_and_report
from ai_trading_system.etf_portfolio.regime import load_regimes, select_regime_for_date
from ai_trading_system.etf_portfolio.satellite import (
    DEFAULT_SATELLITE_EXPERIMENT_DIR,
    DEFAULT_SATELLITE_FEATURE_DIR,
    DEFAULT_SATELLITE_POLICY_CONFIG_PATH,
    DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH,
    DEFAULT_SATELLITE_VALIDATION_DIR,
    build_satellite_policy_validation_report,
    build_satellite_relative_strength_features,
    build_satellite_replacement_report,
    build_satellite_shadow_portfolio_experiment,
    latest_satellite_report_path,
    load_satellite_policy_config,
    load_satellite_universe_config,
    satellite_price_symbols,
    validate_satellite_data_availability,
    write_satellite_features,
    write_satellite_policy_validation_report,
    write_satellite_replacement_report,
    write_satellite_shadow_experiment,
)
from ai_trading_system.etf_portfolio.signals import load_signals
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    available_price_symbols as _available_price_symbols,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    p1_quality_metadata as _p1_quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    price_requested_date_range as _price_requested_date_range,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    quality_metadata as _quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_feature_date as _resolve_feature_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import satellite_app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


@satellite_app.command("evaluate")
def satellite_evaluate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """评估 P1 satellite stock candidates；observe-only，不改 ETF 目标权重。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_feature_date(date_option, features)
    regime = select_regime_for_date(regimes, run_date)
    candidates = evaluate_satellite_candidates(
        features,
        signals,
        config=config,
        p1_config=config.p1,
        run_date=run_date,
        regime=str(regime["regime"]),
    )
    csv_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.csv"
    md_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.md"
    write_frame_and_report(
        candidates,
        csv_path,
        md_path,
        "ETF Satellite Candidate Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF satellite candidates：{md_path}")


@satellite_app.command("features")
def satellite_features_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite features 输出目录。")] = (
        DEFAULT_SATELLITE_FEATURE_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-067C stock-vs-ETF relative strength features。"""
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 satellite features。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_satellite_data_availability(
        satellite_config,
        _available_price_symbols(prices, run_date),
    )
    if availability["status"] == "FAIL":
        typer.echo("Satellite 数据覆盖状态：FAIL，已停止 feature build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    records = build_satellite_relative_strength_features(
        prices,
        universe_config=satellite_config,
        run_date=run_date,
    )
    paths = write_satellite_features(
        records,
        output_dir=output_dir,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
    )
    typer.echo(f"Satellite features JSON：{paths['json']}")
    typer.echo(f"Satellite features CSV：{paths['csv']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo(f"satellite_data_availability_status={availability['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")


@satellite_app.command("report")
def satellite_report_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite report 输出目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI confirmation report JSON。缺失时按 neutral context 处理。"),
    ] = None,
) -> None:
    """生成 TRADING-067I satellite replacement JSON/Markdown report。"""
    payload, run_date = _build_satellite_report_payload(
        prices_path=prices_path,
        date_option=date_option,
        universe_path=universe_path,
        policy_path=policy_path,
        ai_confirmation_report_path=ai_confirmation_report_path,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"satellite_replacement_report_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_replacement_report(payload, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"Satellite replacement report JSON：{json_path}")
    typer.echo(f"Satellite replacement report Markdown：{markdown_path}")
    typer.echo(f"eligible_stocks={','.join(str(item) for item in payload['eligible_stocks'])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_app.command("run")
def satellite_run_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite report 输出目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI confirmation report JSON。"),
    ] = None,
) -> None:
    """执行 satellite replacement report alias；不写 official ETF target weights。"""
    satellite_report_command(
        prices_path=prices_path,
        date_option=date_option,
        output_dir=output_dir,
        universe_path=universe_path,
        policy_path=policy_path,
        ai_confirmation_report_path=ai_confirmation_report_path,
    )


@satellite_app.command("experiment")
def satellite_experiment_command(
    report_path: Annotated[
        Path | None,
        typer.Option(help="satellite replacement report JSON；缺省读取 latest report。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    report_dir: Annotated[Path, typer.Option(help="satellite report 查找目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="satellite experiment 输出目录。")] = (
        DEFAULT_SATELLITE_EXPERIMENT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-067G satellite shadow portfolio experiment。"""
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    selected_report_path = report_path or latest_satellite_report_path(
        report_dir,
        as_of=run_date if date_option and date_option != "latest" else None,
    )
    if selected_report_path is None:
        typer.echo("Satellite replacement report not found; run report before experiment.")
        raise typer.Exit(code=1)
    report_payload = json.loads(selected_report_path.read_text(encoding="utf-8"))
    if report_payload.get("date"):
        run_date = date.fromisoformat(str(report_payload["date"]))
    experiment = build_satellite_shadow_portfolio_experiment(
        run_date=run_date,
        replacement_plan=report_payload["replacement_plan"],
        universe_config=load_satellite_universe_config(universe_path),
        base_candidate_id="satellite_replacement_v1",
    )
    stem = f"satellite_shadow_experiment_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_shadow_experiment(
        experiment,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"Satellite shadow experiment JSON：{json_path}")
    typer.echo(f"Satellite shadow experiment Markdown：{markdown_path}")
    typer.echo(f"status={experiment['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@satellite_app.command("validate")
def satellite_validate_command(
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_SATELLITE_VALIDATION_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    report_registry_path: Annotated[Path, typer.Option(help="report registry config。")] = (
        DEFAULT_REPORT_REGISTRY_PATH
    ),
) -> None:
    """生成 TRADING-067K final satellite replacement validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    generated = datetime.now(tz=UTC)
    payload = build_satellite_policy_validation_report(
        universe_config=load_satellite_universe_config(universe_path),
        policy_config=load_satellite_policy_config(policy_path),
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        generated_at=generated.isoformat(),
    )
    stem = f"satellite_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_policy_validation_report(
        payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"Satellite validation JSON：{json_path}")
    typer.echo(f"Satellite validation Markdown：{markdown_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _build_satellite_report_payload(
    *,
    prices_path: Path,
    date_option: str | None,
    universe_path: Path,
    policy_path: Path,
    ai_confirmation_report_path: Path | None,
) -> tuple[dict[str, object], date]:
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    policy_config = load_satellite_policy_config(policy_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 satellite report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_satellite_data_availability(
        satellite_config,
        _available_price_symbols(prices, run_date),
    )
    if availability["status"] == "FAIL":
        typer.echo("Satellite 数据覆盖状态：FAIL，已停止 report build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    ai_confirmation_payload = (
        json.loads(ai_confirmation_report_path.read_text(encoding="utf-8"))
        if ai_confirmation_report_path is not None and ai_confirmation_report_path.exists()
        else None
    )
    payload = build_satellite_replacement_report(
        prices=prices,
        universe_config=satellite_config,
        policy_config=policy_config,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        base_weights=_default_etf_base_weights(config),
        ai_confirmation_payload=ai_confirmation_payload,
        market_regime=config.backtest.backtest.regime,
        requested_date_range=_price_requested_date_range(prices, run_date),
    )
    return payload, run_date


def _default_etf_base_weights(config) -> dict[str, float]:
    return {
        symbol: float(asset.default_weight)
        for symbol, asset in config.assets.assets.items()
        if symbol in {"SPY", "QQQ", "SMH", "SOXX", "CASH"}
    }
