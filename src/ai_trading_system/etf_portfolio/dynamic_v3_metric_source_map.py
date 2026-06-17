from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_metrics_materialization as baseline_metrics,
)
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost_sensitivity
from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_METRIC_SOURCE_MAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "metric_source_map"

METRIC_SOURCE_MAP_STATUSES = (
    "METRIC_SOURCE_MAP_READY",
    "METRIC_SOURCE_MAP_PARTIAL",
    "METRIC_SOURCE_MAP_BLOCKED",
)

METRIC_SOURCE_MAP_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "metric_source_map_only": True,
    "research_only": True,
    "manual_review_only": True,
    "cost_metrics_materialized": False,
    "benchmark_metrics_materialized": False,
    "cost_sensitivity_review_rerun": False,
    "benchmark_baseline_control_rerun": False,
    "data_downloaded_by_source_map": False,
    "pipelines_executed_by_source_map": False,
    "strategy_optimized_by_source_map": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}

CANDIDATE_METRIC_SPECS: tuple[dict[str, Any], ...] = (
    {
        "metric_name": "turnover",
        "derivation_method": "copy avg_turnover from source_variant summary row",
        "required_fields": ("summary[variant].avg_turnover",),
    },
    {
        "metric_name": "rotation_count",
        "derivation_method": "copy event_count from source_variant summary row",
        "required_fields": ("summary[variant].event_count",),
    },
    {
        "metric_name": "gross_performance_proxy",
        "derivation_method": "copy avg_5d_return from source_variant summary row",
        "required_fields": ("summary[variant].avg_5d_return",),
    },
    {
        "metric_name": "net_performance_proxy",
        "derivation_method": (
            "derive from gross_performance_proxy, turnover, and configured cost "
            "sensitivity scenarios during materialization"
        ),
        "required_fields": (
            "summary[variant].avg_5d_return",
            "summary[variant].avg_turnover",
            "cost_sensitivity_policy.scenarios",
        ),
    },
    {
        "metric_name": "drawdown_proxy",
        "derivation_method": "copy avg_max_drawdown_20d from source_variant summary row",
        "required_fields": ("summary[variant].avg_max_drawdown_20d",),
    },
    {
        "metric_name": "candidate_return_proxy",
        "derivation_method": (
            "use avg_relative_to_no_trade_5d, or derive avg_5d_return minus "
            "no_trade.avg_5d_return"
        ),
        "required_fields": (
            "summary[variant].avg_relative_to_no_trade_5d OR "
            "(summary[variant].avg_5d_return + summary[no_trade].avg_5d_return)",
        ),
    },
)

BASELINE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "metric_name": "static_allocation_baseline",
        "baseline_id": "static_allocation",
        "derivation_method": (
            "apply static baseline weights over existing BACKTEST_SIMULATION event windows "
            "using cached adjusted-close prices"
        ),
        "source_method": "price_cache_static_allocation",
    },
    {
        "metric_name": "no_trade_baseline",
        "baseline_id": "no_trade",
        "derivation_method": "read no_trade summary row and existing event windows",
        "source_method": "backtest_sim_outcome_no_trade",
    },
    {
        "metric_name": "qqq_only_baseline",
        "baseline_id": "qqq_only",
        "derivation_method": (
            "apply QQQ 100% hold-period baseline over existing event windows using "
            "cached adjusted-close prices"
        ),
        "source_method": "price_cache_single_asset",
    },
    {
        "metric_name": "spy_only_baseline",
        "baseline_id": "spy_only",
        "derivation_method": (
            "apply SPY 100% hold-period baseline over existing event windows using "
            "cached adjusted-close prices"
        ),
        "source_method": "price_cache_single_asset",
    },
    {
        "metric_name": "equal_weight_etf_baseline",
        "baseline_id": "equal_weight_etf",
        "derivation_method": (
            "apply SPY/QQQ/SMH/SOXX equal-weight hold-period baseline over existing "
            "event windows using cached adjusted-close prices"
        ),
        "source_method": "price_cache_equal_weight_etf",
    },
)


def run_metric_source_map(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str = "limited_adjustment",
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    price_cache_path: Path = DEFAULT_ETF_PRICE_PATH,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_payload = _load_sim_outcome(sim_outcome_id=sim_outcome_id, output_dir=sim_outcome_dir)
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    outcome_summary = _mapping(outcome_payload.get("simulated_variant_summary"))
    summary_rows = _records(outcome_summary.get("summary"))
    source_row = _variant_row(summary_rows, source_variant)
    no_trade_row = _variant_row(summary_rows, "no_trade")
    window_rows = _available_window_rows(
        _records(outcome_payload.get("simulated_outcome_windows")),
        window_days=5,
    )
    price_symbols = _price_symbols(price_cache_path)
    cost_policy_exists = cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH.exists()

    candidate_metric_sources = [
        _candidate_metric_row(
            spec,
            outcome_payload=outcome_payload,
            candidate=candidate,
            source_variant=source_variant,
            source_row=source_row,
            no_trade_row=no_trade_row,
            cost_policy_exists=cost_policy_exists,
        )
        for spec in CANDIDATE_METRIC_SPECS
    ]
    baseline_metric_sources = [
        _baseline_metric_row(
            spec,
            outcome_payload=outcome_payload,
            window_rows=window_rows,
            summary_rows=summary_rows,
            price_cache_path=price_cache_path,
            price_symbols=price_symbols,
        )
        for spec in BASELINE_SPECS
    ]
    all_rows = [*candidate_metric_sources, *baseline_metric_sources]
    derivable_count = len([row for row in all_rows if row["derivable_now"]])
    missing_rows = [row for row in all_rows if not row["derivable_now"]]
    status = _source_map_status(all_rows)
    source_map_id = st._stable_id(
        "metric-source-map",
        candidate,
        source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / source_map_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_metric_source_map_report",
        "source_map_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "metric_source_map_status": status,
        "candidate_metric_sources": candidate_metric_sources,
        "baseline_metric_sources": baseline_metric_sources,
        "source_summary": {
            "candidate_metric_count": len(candidate_metric_sources),
            "baseline_metric_count": len(baseline_metric_sources),
            "total_metric_count": len(all_rows),
            "derivable_now_count": derivable_count,
            "missing_metric_count": len(missing_rows),
            "missing_metric_names": [row["metric_name"] for row in missing_rows],
            "cost_policy_exists": cost_policy_exists,
            "price_cache_exists": price_cache_path.exists(),
            "price_symbol_count": len(price_symbols),
            "event_window_count": len(window_rows),
        },
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "cost_sensitivity_policy": {
                "path": str(cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH),
                "exists": cost_policy_exists,
                "production_effect": "none",
            },
            "price_cache": {
                "path": str(price_cache_path),
                "exists": price_cache_path.exists(),
                "production_effect": "none",
            },
        },
        "blocking_reasons": [
            f"{row['metric_name']}:{field}"
            for row in missing_rows
            for field in _texts(row.get("missing_fields"))
        ],
        "warnings": _source_map_warnings(outcome_payload, price_cache_path),
        "next_required_action": _next_action(status),
        "limitations": [
            "Source map identifies derivable metric inputs but does not materialize metrics.",
            "BACKTEST_SIMULATION sources are research-only and not PIT/live execution evidence.",
            "Price-derived baselines still require the benchmark materialization data gate.",
        ],
        **METRIC_SOURCE_MAP_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_metric_source_map_manifest",
        "source_map_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "metric_source_map_status": status,
        "metric_source_map_manifest_path": str(root / "metric_source_map_manifest.json"),
        "metric_source_map_report_path": str(root / "metric_source_map_report.json"),
        "metric_source_map_markdown_path": str(root / "metric_source_map_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "metric_source_map_validation.json"),
        **METRIC_SOURCE_MAP_SAFETY,
    }
    reader = render_metric_source_map_reader_brief(report)
    st._write_json(root / "metric_source_map_manifest.json", manifest)
    st._write_json(root / "metric_source_map_report.json", report)
    st._write_text(
        root / "metric_source_map_report.md",
        render_metric_source_map_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_metric_source_map",
        root.name,
        root / "metric_source_map_manifest.json",
    )
    validation = validate_metric_source_map_artifact(
        source_map_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "source_map_id": root.name,
        "source_map_dir": root,
        "manifest": manifest,
        "metric_source_map_report": report,
        "reader_brief_section": reader,
        "metric_source_map_validation": validation,
    }


def metric_source_map_report_payload(
    *,
    source_map_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=source_map_id,
        latest_pointer="latest_metric_source_map",
        latest=latest,
        output_dir=output_dir,
        required_name="metric_source_map_manifest.json",
    )
    payload = {
        **st._read_json(root / "metric_source_map_manifest.json"),
        "metric_source_map_report": st._read_json(root / "metric_source_map_report.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "source_map_dir": str(root),
    }
    validation = st._read_optional_json(root / "metric_source_map_validation.json")
    if validation:
        payload["metric_source_map_validation"] = validation
    return payload


def validate_metric_source_map_artifact(
    *,
    source_map_id: str,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / source_map_id
    manifest = st._read_optional_json(root / "metric_source_map_manifest.json") or {}
    report = st._read_optional_json(root / "metric_source_map_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    rows = [
        *_records(report.get("candidate_metric_sources")),
        *_records(report.get("baseline_metric_sources")),
    ]
    checks = [
        _check("manifest_exists", bool(manifest)),
        _check("report_exists", bool(report)),
        _check(
            "report_type",
            _text(report.get("report_type")) == "etf_dynamic_v3_metric_source_map_report",
        ),
        _check(
            "status_enum",
            _text(report.get("metric_source_map_status")) in METRIC_SOURCE_MAP_STATUSES,
        ),
        _check(
            "candidate_metric_count",
            len(_records(report.get("candidate_metric_sources"))) == 6,
        ),
        _check(
            "baseline_metric_count",
            len(_records(report.get("baseline_metric_sources"))) == 5,
        ),
        _check(
            "source_rows_are_complete",
            all(_source_row_complete(row) for row in rows),
        ),
        _check("reader_brief_exists", "metric_source_map_status" in reader),
        _check(
            "safety_no_materialization",
            report.get("cost_metrics_materialized") is False
            and report.get("benchmark_metrics_materialized") is False
            and report.get("broker_action_taken") is False
            and report.get("production_effect") == "none",
        ),
    ]
    failed = [check for check in checks if check["status"] == "FAIL"]
    warnings = [
        {
            "issue_id": "metric_sources_not_derivable",
            "missing_metric_names": [
                row.get("metric_name") for row in rows if row.get("derivable_now") is not True
            ],
        }
    ] if any(row.get("derivable_now") is not True for row in rows) else []
    status = "FAIL" if failed else "PASS_WITH_WARNINGS" if warnings else "PASS"
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_metric_source_map_validation",
        "source_map_id": source_map_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "warning_check_count": len(warnings),
        "checks": checks,
        "warning_issues": warnings,
        "source_status": report.get("metric_source_map_status"),
        "production_effect": "none",
        **METRIC_SOURCE_MAP_SAFETY,
    }
    if write_output:
        st._write_json(root / "metric_source_map_validation.json", validation)
        st._write_text(
            root / "metric_source_map_validation.md",
            render_metric_source_map_validation_report(validation),
        )
    return validation


def render_metric_source_map_reader_brief(report: Mapping[str, Any]) -> str:
    summary = _mapping(report.get("source_summary"))
    derivable_count = summary.get("derivable_now_count")
    total_count = summary.get("total_metric_count")
    missing_metric_names = ", ".join(_texts(summary.get("missing_metric_names"))) or "none"
    return "\n".join(
        [
            "## Dynamic v3 Metric Source Map",
            "",
            f"- metric_source_map_id: {report.get('source_map_id')}",
            f"- metric_source_map_status: {report.get('metric_source_map_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- derivable_now: {derivable_count}/{total_count}",
            f"- missing_metric_names: {missing_metric_names}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety: source-map only; no metric materialization, no promotion, "
            "no broker/order, production_effect=none",
            "",
        ]
    )


def render_metric_source_map_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic v3 Metric Source Map",
        "",
        f"- source_map_id: {report.get('source_map_id') or manifest.get('source_map_id')}",
        f"- status: {report.get('metric_source_map_status')}",
        f"- candidate: {report.get('candidate')}",
        f"- source_variant: {report.get('source_variant')}",
        f"- production_effect: {report.get('production_effect')}",
        f"- next_required_action: {report.get('next_required_action')}",
        "",
        "## Candidate Metric Sources",
        "",
        "|metric|source_scope|derivable_now|required_fields|missing_fields|",
        "|---|---|---|---|---|",
    ]
    for row in _records(report.get("candidate_metric_sources")):
        lines.append(_source_row_markdown(row))
    lines.extend(
        [
            "",
            "## Baseline Metric Sources",
            "",
            "|metric|source_scope|derivable_now|required_fields|missing_fields|",
            "|---|---|---|---|---|",
        ]
    )
    for row in _records(report.get("baseline_metric_sources")):
        lines.append(_source_row_markdown(row))
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- This report does not materialize cost or benchmark metrics.",
            "- This report does not rerun cost-sensitivity review or benchmark baseline control.",
            "- This report does not approve promotion, extended shadow, live trading, "
            "official target weights, broker/order, paper account mutation, or "
            "production mutation.",
            "",
        ]
    )
    return "\n".join(lines)


def render_metric_source_map_validation_report(validation: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic v3 Metric Source Map Validation",
        "",
        f"- source_map_id: {validation.get('source_map_id')}",
        f"- status: {validation.get('status')}",
        f"- checks: {validation.get('check_count')}",
        f"- failed: {validation.get('failed_check_count')}",
        f"- warnings: {validation.get('warning_check_count')}",
        "",
        "|check|status|",
        "|---|---|",
    ]
    for check in _records(validation.get("checks")):
        lines.append(f"|{_text(check.get('check_id'))}|{_text(check.get('status'))}|")
    lines.append("")
    return "\n".join(lines)


def _candidate_metric_row(
    spec: Mapping[str, Any],
    *,
    outcome_payload: Mapping[str, Any],
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    no_trade_row: Mapping[str, Any],
    cost_policy_exists: bool,
) -> dict[str, Any]:
    metric_name = _text(spec.get("metric_name"))
    missing = _candidate_missing_fields(metric_name, source_row, no_trade_row, cost_policy_exists)
    return {
        "metric_name": metric_name,
        "metric_group": "candidate",
        "candidate": candidate,
        "source_variant": source_variant,
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "source_scope": f"simulated_variant_summary[variant={source_variant}]",
        "derivation_method": _text(spec.get("derivation_method")),
        "required_fields": list(_texts(spec.get("required_fields"))),
        "missing_fields": missing,
        "derivable_now": not missing,
        "materialized_by_source_map": False,
        "outcome_mode": "BACKTEST_SIMULATION",
        "limitation": "research-only source proof; not materialized here",
    }


def _baseline_metric_row(
    spec: Mapping[str, Any],
    *,
    outcome_payload: Mapping[str, Any],
    window_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
    price_cache_path: Path,
    price_symbols: set[str],
) -> dict[str, Any]:
    baseline_id = _text(spec.get("baseline_id"))
    required_symbols = _baseline_required_symbols(baseline_id, outcome_payload)
    missing = _baseline_missing_fields(
        baseline_id=baseline_id,
        summary_rows=summary_rows,
        window_rows=window_rows,
        price_cache_path=price_cache_path,
        price_symbols=price_symbols,
        required_symbols=required_symbols,
    )
    return {
        "metric_name": _text(spec.get("metric_name")),
        "metric_group": "baseline",
        "baseline_id": baseline_id,
        "source_method": _text(spec.get("source_method")),
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "source_scope": _baseline_source_scope(baseline_id),
        "derivation_method": _text(spec.get("derivation_method")),
        "required_fields": _baseline_required_fields(baseline_id, required_symbols),
        "missing_fields": missing,
        "derivable_now": not missing,
        "materialized_by_source_map": False,
        "outcome_mode": "BACKTEST_SIMULATION",
        "limitation": "research-only source proof; not materialized here",
    }


def _candidate_missing_fields(
    metric_name: str,
    source_row: Mapping[str, Any],
    no_trade_row: Mapping[str, Any],
    cost_policy_exists: bool,
) -> list[str]:
    if not source_row:
        return ["summary[source_variant]"]
    missing: list[str] = []
    if metric_name == "turnover":
        _require_field(source_row, "avg_turnover", missing, "summary[variant].avg_turnover")
    elif metric_name == "rotation_count":
        _require_field(source_row, "event_count", missing, "summary[variant].event_count")
    elif metric_name == "gross_performance_proxy":
        _require_field(source_row, "avg_5d_return", missing, "summary[variant].avg_5d_return")
    elif metric_name == "net_performance_proxy":
        _require_field(source_row, "avg_5d_return", missing, "summary[variant].avg_5d_return")
        _require_field(source_row, "avg_turnover", missing, "summary[variant].avg_turnover")
        if not cost_policy_exists:
            missing.append("cost_sensitivity_policy.scenarios")
    elif metric_name == "drawdown_proxy":
        _require_field(
            source_row,
            "avg_max_drawdown_20d",
            missing,
            "summary[variant].avg_max_drawdown_20d",
        )
    elif metric_name == "candidate_return_proxy":
        relative = _float_or_none(source_row.get("avg_relative_to_no_trade_5d"))
        gross = _float_or_none(source_row.get("avg_5d_return"))
        baseline = _float_or_none(no_trade_row.get("avg_5d_return"))
        if relative is None and (gross is None or baseline is None):
            missing.append(
                "summary[variant].avg_relative_to_no_trade_5d OR "
                "(summary[variant].avg_5d_return + summary[no_trade].avg_5d_return)"
            )
    return missing


def _baseline_missing_fields(
    *,
    baseline_id: str,
    summary_rows: Sequence[Mapping[str, Any]],
    window_rows: Sequence[Mapping[str, Any]],
    price_cache_path: Path,
    price_symbols: set[str],
    required_symbols: set[str],
) -> list[str]:
    missing: list[str] = []
    if baseline_id == "no_trade":
        row = _variant_row(summary_rows, "no_trade")
        if not row:
            missing.append("summary[no_trade]")
        elif _float_or_none(row.get("avg_5d_return")) is None and not window_rows:
            missing.append("summary[no_trade].avg_5d_return OR available no_trade windows")
        return missing
    if not window_rows:
        missing.append("simulated_outcome_windows[variant=no_trade,window_days=5]")
    if not price_cache_path.exists():
        missing.append("price_cache_path")
    missing_symbols = sorted(required_symbols - price_symbols)
    if missing_symbols:
        missing.append("price_cache_symbols:" + ",".join(missing_symbols))
    return missing


def _baseline_required_fields(baseline_id: str, required_symbols: set[str]) -> list[str]:
    if baseline_id == "no_trade":
        return [
            "simulated_variant_summary[variant=no_trade].avg_5d_return",
            "optional simulated_outcome_windows[variant=no_trade,window_days=5]",
        ]
    return [
        "simulated_outcome_windows[variant=no_trade,window_days=5].start_date",
        "simulated_outcome_windows[variant=no_trade,window_days=5].end_date",
        "price_cache.date",
        "price_cache.ticker_or_symbol",
        "price_cache.adj_close",
        "price_cache_symbols:" + ",".join(sorted(required_symbols)),
    ]


def _baseline_required_symbols(
    baseline_id: str,
    outcome_payload: Mapping[str, Any],
) -> set[str]:
    if baseline_id == "static_allocation":
        return {
            symbol
            for symbol in baseline_metrics._static_baseline_weights(outcome_payload)  # noqa: SLF001
            if symbol.upper() != "CASH"
        }
    if baseline_id == "qqq_only":
        return {"QQQ"}
    if baseline_id == "spy_only":
        return {"SPY"}
    if baseline_id == "equal_weight_etf":
        return {"SPY", "QQQ", "SMH", "SOXX"}
    return set()


def _baseline_source_scope(baseline_id: str) -> str:
    if baseline_id == "no_trade":
        return "simulated_variant_summary[variant=no_trade]"
    return "simulated_outcome_windows + cached ETF adjusted-close prices"


def _source_map_status(rows: Sequence[Mapping[str, Any]]) -> str:
    derivable = [row for row in rows if row.get("derivable_now") is True]
    if len(derivable) == len(rows):
        return "METRIC_SOURCE_MAP_READY"
    if derivable:
        return "METRIC_SOURCE_MAP_PARTIAL"
    return "METRIC_SOURCE_MAP_BLOCKED"


def _source_map_warnings(
    outcome_payload: Mapping[str, Any],
    price_cache_path: Path,
) -> list[str]:
    warnings = ["metric_source_map:no_metrics_materialized"]
    if _text(outcome_payload.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("source:backtest_simulation_not_pit")
    if price_cache_path.exists():
        warnings.append("price_cache_requires_validate_data_before_materialization")
    return sorted(set(warnings))


def _next_action(status: str) -> str:
    if status == "METRIC_SOURCE_MAP_READY":
        return "materialize_only_derivable_cost_and_benchmark_metrics"
    if status == "METRIC_SOURCE_MAP_PARTIAL":
        return "materialize_derivable_metrics_and_keep_missing_metrics_blocked"
    return "restore_metric_source_artifacts_before_materialization"


def _source_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": _text(payload.get("sim_outcome_id")),
        "path": _text(payload.get("sim_outcome_manifest_path")),
        "summary_path": _text(payload.get("simulated_variant_summary_path")),
        "window_path": _text(payload.get("simulated_outcome_windows_path")),
        "outcome_mode": _text(payload.get("outcome_mode"), "BACKTEST_SIMULATION"),
        "production_effect": "none",
    }


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=sim_outcome_id is None,
        output_dir=output_dir,
    )


def _available_window_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    window_days: int,
) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        if (
            _text(row.get("variant")) == "no_trade"
            and _int(row.get("window_days")) == window_days
            and _text(row.get("outcome_status")) == "AVAILABLE"
        ):
            result.append(dict(row))
    return result


def _variant_row(rows: Sequence[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("variant")) == variant:
            return dict(row)
    return {}


def _price_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    symbols: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                symbol = _text(row.get("ticker") or row.get("symbol")).upper()
                if symbol:
                    symbols.add(symbol)
    except OSError:
        return set()
    return symbols


def _require_field(
    row: Mapping[str, Any],
    field: str,
    missing: list[str],
    label: str,
) -> None:
    if _float_or_none(row.get(field)) is None:
        missing.append(label)


def _source_row_complete(row: Mapping[str, Any]) -> bool:
    return (
        bool(_text(row.get("metric_name")))
        and bool(_text(row.get("source_artifact_id")))
        and bool(_text(row.get("source_scope")))
        and bool(_text(row.get("derivation_method")))
        and bool(_texts(row.get("required_fields")))
        and isinstance(row.get("missing_fields"), list)
        and isinstance(row.get("derivable_now"), bool)
        and row.get("materialized_by_source_map") is False
    )


def _check(check_id: str, condition: bool) -> dict[str, str]:
    return {"check_id": check_id, "status": "PASS" if condition else "FAIL"}


def _source_row_markdown(row: Mapping[str, Any]) -> str:
    return (
        f"|{_md(row.get('metric_name'))}|"
        f"{_md(row.get('source_scope'))}|"
        f"{_md(row.get('derivable_now'))}|"
        f"{_md(', '.join(_texts(row.get('required_fields'))))}|"
        f"{_md(', '.join(_texts(row.get('missing_fields'))) or 'none')}|"
    )


def _md(value: object) -> str:
    text = _text(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
