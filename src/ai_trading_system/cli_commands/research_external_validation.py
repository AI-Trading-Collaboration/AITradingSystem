from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START_DATE
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
)
from ai_trading_system.external_validation import (
    DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_MASTER_REVIEW_DOC_PATH,
    DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH,
    DEFAULT_EXTERNAL_VALIDATION_MASTER_REVIEW_DOC_PATH,
    DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH,
    DEFAULT_MANUAL_EXTERNAL_RECORD_INPUT_GUIDE_DOC_PATH,
    DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR,
    DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH,
    DEFAULT_STATIC_BASELINE_EXTERNAL_MANUAL_RUNBOOK_DOC_PATH,
    run_dynamic_weight_path_external_support_check,
    run_dynamic_weight_path_replay_final_check,
    run_external_independent_return_replay,
    run_external_platform_feasibility_review,
    run_external_platform_metric_convention_signoff,
    run_external_validation_difference_attribution,
    run_external_validation_manual_evidence_master_review,
    run_external_validation_manual_evidence_owner_signoff,
    run_external_validation_master_review,
    run_external_validation_owner_report,
    run_external_validation_reader_brief_safe_preview,
    run_external_validation_real_result_status_reader,
    run_external_validation_scope_contract,
    run_external_validation_to_launch_gate,
    run_manual_external_record_template,
    run_metric_and_sgov_reconciliation_signoff,
    run_metric_definition_reconciliation,
    run_quantconnect_replication_dry_run_plan,
    run_quantconnect_weight_path_replay_preflight,
    run_sgov_external_convention_signoff,
    run_sgov_total_return_external_check,
    run_static_baseline_external_manual_input_ingestion,
    run_static_baseline_external_manual_runbook,
    run_static_baseline_external_reconciliation,
    run_static_baseline_final_reconciliation_after_manual_input,
    run_static_baseline_reconciliation_final_check,
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
                start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
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
                start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
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
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline external reconciliation", payload)


def strategies_static_baseline_reconciliation_final_check_command(
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
    external_records_path: Annotated[
        Path | None, typer.Option("--external-records-path")
    ] = None,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_static_baseline_reconciliation_final_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            external_records_path=external_records_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline reconciliation final check", payload)


def strategies_manual_external_record_template_command(
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    template_dir: Annotated[Path, typer.Option("--template-dir")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR
    ),
    guide_path: Annotated[Path, typer.Option("--guide-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORD_INPUT_GUIDE_DOC_PATH
    ),
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_manual_external_record_template(
            output_root=output_root,
            template_dir=template_dir,
            guide_path=guide_path,
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Manual external record template", payload)


def strategies_static_baseline_external_manual_runbook_command(
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_STATIC_BASELINE_EXTERNAL_MANUAL_RUNBOOK_DOC_PATH
    ),
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_static_baseline_external_manual_runbook(
            simple_config_path=simple_config_path,
            output_root=output_root,
            docs_path=docs_path,
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline external manual runbook", payload)


def strategies_static_baseline_external_manual_input_ingestion_command(
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    input_yaml_path: Annotated[Path, typer.Option("--input-yaml-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH
    ),
    input_csv_path: Annotated[Path, typer.Option("--input-csv-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH
    ),
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_static_baseline_external_manual_input_ingestion(
            simple_config_path=simple_config_path,
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline external manual input ingestion", payload)


def strategies_external_platform_metric_convention_signoff_command(
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT
    ),
    signoff_path: Annotated[Path, typer.Option("--signoff-path")] = (
        DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    input_yaml_path: Annotated[Path, typer.Option("--input-yaml-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH
    ),
    input_csv_path: Annotated[Path, typer.Option("--input-csv-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH
    ),
) -> None:
    payload = _build_payload(
        lambda: run_external_platform_metric_convention_signoff(
            output_root=output_root,
            signoff_path=signoff_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
        )
    )
    _print_payload("External platform metric convention signoff", payload)


def strategies_sgov_external_convention_signoff_command(
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
    signoff_path: Annotated[Path, typer.Option("--signoff-path")] = (
        DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_sgov_external_convention_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            signoff_path=signoff_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("SGOV external convention signoff", payload)


def strategies_static_baseline_final_reconciliation_after_manual_input_command(
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
    input_yaml_path: Annotated[Path, typer.Option("--input-yaml-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH
    ),
    input_csv_path: Annotated[Path, typer.Option("--input-csv-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH
    ),
    metric_signoff_path: Annotated[Path, typer.Option("--metric-signoff-path")] = (
        DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    sgov_signoff_path: Annotated[Path, typer.Option("--sgov-signoff-path")] = (
        DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_static_baseline_final_reconciliation_after_manual_input(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            metric_signoff_path=metric_signoff_path,
            sgov_signoff_path=sgov_signoff_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("Static baseline final reconciliation after manual input", payload)


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
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
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
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("External validation master review", payload)


def strategies_external_validation_manual_evidence_owner_signoff_command(
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
        DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH
    ),
    input_yaml_path: Annotated[Path, typer.Option("--input-yaml-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH
    ),
    input_csv_path: Annotated[Path, typer.Option("--input-csv-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH
    ),
    metric_signoff_path: Annotated[Path, typer.Option("--metric-signoff-path")] = (
        DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    sgov_signoff_path: Annotated[Path, typer.Option("--sgov-signoff-path")] = (
        DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_external_validation_manual_evidence_owner_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=docs_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            metric_signoff_path=metric_signoff_path,
            sgov_signoff_path=sgov_signoff_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("External validation manual evidence owner signoff", payload)


def strategies_external_validation_manual_evidence_master_review_command(
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
        DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_MASTER_REVIEW_DOC_PATH
    ),
    owner_docs_path: Annotated[Path, typer.Option("--owner-docs-path")] = (
        DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH
    ),
    input_yaml_path: Annotated[Path, typer.Option("--input-yaml-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH
    ),
    input_csv_path: Annotated[Path, typer.Option("--input-csv-path")] = (
        DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH
    ),
    metric_signoff_path: Annotated[Path, typer.Option("--metric-signoff-path")] = (
        DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    sgov_signoff_path: Annotated[Path, typer.Option("--sgov-signoff-path")] = (
        DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_payload(
        lambda: run_external_validation_manual_evidence_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=docs_path,
            owner_docs_path=owner_docs_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            metric_signoff_path=metric_signoff_path,
            sgov_signoff_path=sgov_signoff_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or PRIMARY_RESEARCH_START_DATE,
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_payload("External validation manual evidence master review", payload)


_EXTERNAL_VALIDATION_COMMANDS = (
    (
        "manual-external-record-template",
        strategies_manual_external_record_template_command,
    ),
    (
        "static-baseline-external-manual-runbook",
        strategies_static_baseline_external_manual_runbook_command,
    ),
    (
        "static-baseline-external-manual-input-ingestion",
        strategies_static_baseline_external_manual_input_ingestion_command,
    ),
    (
        "external-platform-metric-convention-signoff",
        strategies_external_platform_metric_convention_signoff_command,
    ),
    (
        "sgov-external-convention-signoff",
        strategies_sgov_external_convention_signoff_command,
    ),
    (
        "static-baseline-final-reconciliation-after-manual-input",
        strategies_static_baseline_final_reconciliation_after_manual_input_command,
    ),
    (
        "dynamic-weight-path-external-support-check",
        _make_output_command(
            run_dynamic_weight_path_external_support_check,
            "Dynamic weight path external support check",
        ),
    ),
    (
        "quantconnect-weight-path-replay-preflight",
        _make_output_command(
            run_quantconnect_weight_path_replay_preflight,
            "QuantConnect weight path replay preflight",
        ),
    ),
    (
        "external-validation-manual-evidence-owner-signoff",
        strategies_external_validation_manual_evidence_owner_signoff_command,
    ),
    (
        "external-validation-manual-evidence-master-review",
        strategies_external_validation_manual_evidence_master_review_command,
    ),
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
    (
        "external-validation-real-result-status-reader",
        _make_data_command(
            run_external_validation_real_result_status_reader,
            "External validation real result status reader",
        ),
    ),
    (
        "static-baseline-reconciliation-final-check",
        strategies_static_baseline_reconciliation_final_check_command,
    ),
    (
        "dynamic-weight-path-replay-final-check",
        _make_data_command(
            run_dynamic_weight_path_replay_final_check,
            "Dynamic weight path replay final check",
        ),
    ),
    (
        "metric-and-sgov-reconciliation-signoff",
        _make_data_command(
            run_metric_and_sgov_reconciliation_signoff,
            "Metric and SGOV reconciliation signoff",
        ),
    ),
    (
        "external-validation-to-launch-gate",
        _make_data_command(
            run_external_validation_to_launch_gate,
            "External validation to launch gate",
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
