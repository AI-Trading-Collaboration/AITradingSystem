from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
)
from ai_trading_system.external_validation import (
    DEFAULT_EXTERNAL_VALIDATION_MASTER_REVIEW_DOC_PATH,
    DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH,
    run_external_independent_return_replay,
    run_external_platform_feasibility_review,
    run_external_validation_difference_attribution,
    run_external_validation_master_review,
    run_external_validation_owner_report,
    run_external_validation_reader_brief_safe_preview,
    run_external_validation_scope_contract,
    run_metric_definition_reconciliation,
    run_quantconnect_replication_dry_run_plan,
    run_sgov_total_return_external_check,
    run_static_baseline_external_reconciliation,
    run_strategy_weight_path_export,
)
from ai_trading_system.research_governance import ResearchGovernanceError
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
)

console = Console()


def register_external_validation_strategy_commands(strategies_app: typer.Typer) -> None:
    for command_name, command in _EXTERNAL_VALIDATION_COMMANDS:
        strategies_app.command(command_name)(command)


def _make_scope_command(
    builder: Callable[..., dict[str, object]], label: str
) -> Callable[..., None]:
    def command(
        output_root: Annotated[Path, typer.Option("--output-root")] = (
            DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
        ),
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    ) -> None:
        payload = _build_payload(
            lambda: builder(
                output_root=output_root,
                start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
                end_date=_parse_optional_date(end_date),
            )
        )
        _print_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_output_command(
    builder: Callable[..., dict[str, object]], label: str
) -> Callable[..., None]:
    def command(
        output_root: Annotated[Path, typer.Option("--output-root")] = (
            DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
        ),
    ) -> None:
        payload = _build_payload(lambda: builder(output_root=output_root))
        _print_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_data_command(
    builder: Callable[..., dict[str, object]], label: str
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path, typer.Option("--marketstack-prices-path")
        ] = DEFAULT_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
        simple_config_path: Annotated[
            Path, typer.Option("--simple-config")
        ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        growth_config_path: Annotated[
            Path, typer.Option("--growth-config")
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        output_root: Annotated[Path, typer.Option("--output-root")] = (
            DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
        ),
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    ) -> None:
        payload = _build_payload(
            lambda: builder(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                simple_config_path=simple_config_path,
                growth_config_path=growth_config_path,
                output_root=output_root,
                as_of_date=_parse_optional_date(as_of),
                start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
                end_date=_parse_optional_date(end_date),
            )
        )
        _print_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def strategies_static_baseline_external_reconciliation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    external_records_path: Annotated[
        Path | None, typer.Option("--external-records-path")
    ] = None,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_static_baseline_external_reconciliation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            output_root=output_root,
            external_records_path=external_records_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline external reconciliation", payload)


def strategies_external_validation_owner_report_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Annotated[
        Path, typer.Option("--growth-config")
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_external_validation_owner_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("External validation owner report", payload)


def strategies_external_validation_master_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Annotated[
        Path, typer.Option("--growth-config")
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_EXTERNAL_VALIDATION_MASTER_REVIEW_DOC_PATH
    ),
    owner_docs_path: Annotated[Path, typer.Option("--owner-docs-path")] = (
        DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_external_validation_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=docs_path,
            owner_docs_path=owner_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("External validation master review", payload)


_EXTERNAL_VALIDATION_COMMANDS = (
    (
        "external-validation-scope-contract",
        _make_scope_command(
            run_external_validation_scope_contract,
            "External validation scope contract",
        ),
    ),
    (
        "static-baseline-external-reconciliation",
        strategies_static_baseline_external_reconciliation_command,
    ),
    (
        "strategy-weight-path-export",
        _make_data_command(run_strategy_weight_path_export, "Strategy weight path export"),
    ),
    (
        "external-independent-return-replay",
        _make_data_command(
            run_external_independent_return_replay,
            "External independent return replay",
        ),
    ),
    (
        "metric-definition-reconciliation",
        _make_output_command(
            run_metric_definition_reconciliation,
            "Metric definition reconciliation",
        ),
    ),
    (
        "sgov-total-return-external-check",
        _make_data_command(
            run_sgov_total_return_external_check,
            "SGOV total-return external check",
        ),
    ),
    (
        "external-platform-feasibility-review",
        _make_output_command(
            run_external_platform_feasibility_review,
            "External platform feasibility review",
        ),
    ),
    (
        "quantconnect-replication-dry-run-plan",
        _make_output_command(
            run_quantconnect_replication_dry_run_plan,
            "QuantConnect replication dry-run plan",
        ),
    ),
    (
        "external-validation-difference-attribution",
        _make_data_command(
            run_external_validation_difference_attribution,
            "External validation difference attribution",
        ),
    ),
    (
        "external-validation-owner-report",
        strategies_external_validation_owner_report_command,
    ),
    (
        "external-validation-master-review",
        strategies_external_validation_master_review_command,
    ),
    (
        "external-validation-reader-brief-safe-preview",
        _make_data_command(
            run_external_validation_reader_brief_safe_preview,
            "External validation Reader Brief safe preview",
        ),
    ),
)


def _build_payload(builder: Callable[[], dict[str, object]]) -> dict[str, object]:
    try:
        return builder()
    except (ResearchGovernanceError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "MATCHED" in status or "PASS" in status else "yellow"
    if "BLOCKED" in status or "MISMATCH" in status or "FIX_REQUIRED" in status:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")
    console.print(f"status={status}")
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        console.print(f"JSON：{paths.get('json_path')}")
        console.print(f"Markdown：{paths.get('markdown_path')}")
    for field, expected in (
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("manual_review_required", True),
        ("production_effect", "none"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc
