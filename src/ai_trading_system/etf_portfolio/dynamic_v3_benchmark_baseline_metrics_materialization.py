from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as baseline_control,
)
from ai_trading_system.etf_portfolio import dynamic_v3_cost_metrics_materialization as cost_metrics
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "benchmark_baseline_metrics_materialization"
)

BENCHMARK_BASELINE_METRICS_MATERIALIZATION_STATUSES = (
    "BASELINE_METRICS_AVAILABLE",
    "BASELINE_METRICS_PARTIAL",
    "INSUFFICIENT_BASELINE_METRICS",
)

BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY = {
    **baseline_control.BENCHMARK_BASELINE_SAFETY,
    "benchmark_baseline_metrics_materialization_only": True,
    "backtest_simulation_event_window_evidence_only": True,
    "data_quality_gate_required": True,
    "data_downloaded_by_materialization": False,
    "pipelines_executed_by_materialization": False,
    "strategy_optimized_by_materialization": False,
    "benchmark_comparison_live_signal": False,
}


def run_benchmark_baseline_metrics_materialization(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str = "limited_adjustment",
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    candidate_metrics_path: Path | None = None,
    candidate_cost_materialization_id: str | None = None,
    candidate_cost_materialization_dir: Path = (
        cost_metrics.DEFAULT_COST_METRICS_MATERIALIZATION_DIR
    ),
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    cost_sensitivity_review_id: str | None = None,
    cost_sensitivity_dir: Path = cost.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    benchmark_baseline_output_dir: Path = (
        baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR
    ),
    price_cache_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_payload = _load_sim_outcome(
        sim_outcome_id=sim_outcome_id,
        output_dir=sim_outcome_dir,
    )
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    materialization_id = st._stable_id(
        "benchmark-baseline-metrics-materialization",
        candidate,
        source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        _text(candidate_cost_materialization_id),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / materialization_id)
    root.mkdir(parents=True, exist_ok=False)

    quality = _run_data_quality_gate(
        as_of=effective_as_of,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        report_path=root / "validate_data_quality_report.md",
    )
    if not quality.passed:
        raise RuntimeError(
            "benchmark baseline metrics materialization stopped because "
            f"data quality gate failed: {quality.status}"
        )

    outcome_summary = _mapping(outcome_payload.get("simulated_variant_summary"))
    rows = _records(outcome_summary.get("summary"))
    window_rows = _available_window_rows(
        _records(outcome_payload.get("simulated_outcome_windows")),
        window_days=5,
    )
    prices = sim._load_prices(  # noqa: SLF001 - reuse project-standard ETF price loader.
        price_cache_path,
        extra_symbols=_required_price_symbols(),
    )
    candidate_metrics_payload = _candidate_benchmark_metrics(
        candidate=candidate,
        source_variant=source_variant,
        source_row=_variant_row(rows, source_variant),
        cost_review_payload=_load_cost_review(
            review_id=cost_sensitivity_review_id,
            output_dir=cost_sensitivity_dir,
        ),
        candidate_metrics_payload=_load_candidate_cost_metrics(
            metrics_path=candidate_metrics_path,
            materialization_id=candidate_cost_materialization_id,
            output_dir=candidate_cost_materialization_dir,
        ),
        outcome_payload=outcome_payload,
        effective_as_of=effective_as_of,
        generated_at=generated,
    )
    baseline_metrics_payload = _baseline_metrics(
        outcome_payload=outcome_payload,
        outcome_rows=window_rows,
        summary_rows=rows,
        prices=prices,
        effective_as_of=effective_as_of,
        generated_at=generated,
        price_cache_path=price_cache_path,
        data_quality_summary=_quality_summary(
            quality,
            report_path=root / "validate_data_quality_report.md",
        ),
    )
    metric_statuses = _metric_statuses(
        candidate_metrics_payload=candidate_metrics_payload,
        baseline_metrics_payload=baseline_metrics_payload,
    )
    candidate_path = root / "candidate_benchmark_metrics.json"
    baseline_path = root / "baseline_metrics.json"
    st._write_json(candidate_path, candidate_metrics_payload)
    st._write_json(baseline_path, baseline_metrics_payload)

    control_result = baseline_control.run_benchmark_baseline_control_pack(
        as_of=effective_as_of,
        candidate_metrics_path=candidate_path,
        baseline_metrics_path=baseline_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        cost_sensitivity_review_id=cost_sensitivity_review_id,
        cost_sensitivity_dir=cost_sensitivity_dir,
        output_dir=benchmark_baseline_output_dir,
        generated_at=generated,
    )
    control_pack = _mapping(control_result.get("benchmark_baseline_control_pack"))
    final_status = _materialization_status(metric_statuses, control_pack)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics_materialization_report",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "benchmark_baseline_metrics_status": final_status,
        "candidate_metrics_path": str(candidate_path),
        "baseline_metrics_path": str(baseline_path),
        "candidate_metrics": candidate_metrics_payload,
        "baseline_metrics": baseline_metrics_payload,
        "required_metric_statuses": metric_statuses,
        "benchmark_baseline_control_id": control_result.get("control_id"),
        "benchmark_baseline_status": control_pack.get("benchmark_baseline_status"),
        "benchmark_baseline_validation_status": _mapping(
            control_result.get("benchmark_baseline_validation")
        ).get("status"),
        "comparison_summary": control_pack.get("comparison_summary"),
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "candidate_cost_metrics": _candidate_source_artifact(candidate_metrics_payload),
            "cost_sensitivity_review": _cost_source_artifact(
                _mapping(candidate_metrics_payload.get("cost_sensitivity_source"))
            ),
            "price_cache": _price_source_artifact(price_cache_path),
            "data_quality_gate": _quality_summary(
                quality,
                report_path=root / "validate_data_quality_report.md",
            ),
            "benchmark_baseline_control": {
                "artifact_id": control_result.get("control_id"),
                "status": control_pack.get("benchmark_baseline_status"),
                "validation_status": _mapping(
                    control_result.get("benchmark_baseline_validation")
                ).get("status"),
                "report_path": _mapping(control_result.get("manifest")).get(
                    "benchmark_baseline_report_path"
                ),
            },
        },
        "blocking_reasons": _blocking_reasons(metric_statuses, control_pack),
        "warnings": _warnings(
            candidate_metrics_payload=candidate_metrics_payload,
            baseline_metrics_payload=baseline_metrics_payload,
            control_pack=control_pack,
            data_quality=quality,
        ),
        "next_required_action": _next_action(final_status, control_pack),
        "limitations": [
            "baseline metrics are materialized from existing simulation windows and cached prices",
            "BACKTEST_SIMULATION event windows are not PIT/live execution evidence",
            "benchmark comparison is research-only and not a live allocation signal",
            "equal_weight_shadow_candidates is not mapped to equal_weight_etf",
        ],
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics_materialization_manifest",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "benchmark_baseline_metrics_status": final_status,
        "candidate_metrics_path": str(candidate_path),
        "baseline_metrics_path": str(baseline_path),
        "benchmark_baseline_control_id": control_result.get("control_id"),
        "benchmark_baseline_status": control_pack.get("benchmark_baseline_status"),
        "benchmark_baseline_metrics_materialization_manifest_path": str(
            root / "benchmark_baseline_metrics_materialization_manifest.json"
        ),
        "benchmark_baseline_metrics_materialization_report_path": str(
            root / "benchmark_baseline_metrics_materialization_report.json"
        ),
        "benchmark_baseline_metrics_materialization_markdown_path": str(
            root / "benchmark_baseline_metrics_materialization_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(
            root / "benchmark_baseline_metrics_materialization_validation.json"
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    reader = render_benchmark_baseline_metrics_materialization_reader_brief(report)
    st._write_json(
        root / "benchmark_baseline_metrics_materialization_manifest.json",
        manifest,
    )
    st._write_json(root / "benchmark_baseline_metrics_materialization_report.json", report)
    st._write_text(
        root / "benchmark_baseline_metrics_materialization_report.md",
        render_benchmark_baseline_metrics_materialization_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_benchmark_baseline_metrics_materialization",
        root.name,
        root / "benchmark_baseline_metrics_materialization_manifest.json",
    )
    validation = validate_benchmark_baseline_metrics_materialization_artifact(
        materialization_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "materialization_id": root.name,
        "materialization_dir": root,
        "manifest": manifest,
        "benchmark_baseline_metrics_materialization_report": report,
        "reader_brief_section": reader,
        "benchmark_baseline_metrics_materialization_validation": validation,
        "benchmark_baseline_control_result": control_result,
    }


def benchmark_baseline_metrics_materialization_report_payload(
    *,
    materialization_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=materialization_id,
        latest_pointer="latest_benchmark_baseline_metrics_materialization",
        latest=latest,
        output_dir=output_dir,
        required_name="benchmark_baseline_metrics_materialization_manifest.json",
    )
    payload = {
        **st._read_json(root / "benchmark_baseline_metrics_materialization_manifest.json"),
        "benchmark_baseline_metrics_materialization_report": st._read_json(
            root / "benchmark_baseline_metrics_materialization_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "materialization_dir": str(root),
    }
    validation = st._read_optional_json(
        root / "benchmark_baseline_metrics_materialization_validation.json"
    )
    if validation:
        payload["benchmark_baseline_metrics_materialization_validation"] = validation
    return payload


def validate_benchmark_baseline_metrics_materialization_artifact(
    *,
    materialization_id: str,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / materialization_id
    manifest = (
        st._read_optional_json(
            root / "benchmark_baseline_metrics_materialization_manifest.json"
        )
        or {}
    )
    report = (
        st._read_optional_json(root / "benchmark_baseline_metrics_materialization_report.json")
        or {}
    )
    candidate_metrics = st._read_optional_json(root / "candidate_benchmark_metrics.json") or {}
    baseline_metrics = st._read_optional_json(root / "baseline_metrics.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    status = _text(report.get("benchmark_baseline_metrics_status"))
    baselines = _records(baseline_metrics.get("baselines"))
    baseline_ids = {_text(row.get("baseline_id")) for row in baselines}
    source_artifacts = _mapping(report.get("source_artifacts"))
    checks = st._required_file_checks(
        root,
        (
            "benchmark_baseline_metrics_materialization_manifest.json",
            "benchmark_baseline_metrics_materialization_report.json",
            "benchmark_baseline_metrics_materialization_report.md",
            "candidate_benchmark_metrics.json",
            "baseline_metrics.json",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "materialization_id_matches",
                manifest.get("materialization_id")
                == report.get("materialization_id")
                == materialization_id,
                "",
            ),
            st._check(
                "status_allowed",
                status in BENCHMARK_BASELINE_METRICS_MATERIALIZATION_STATUSES,
                status,
            ),
            st._check(
                "candidate_metric_visible",
                status != "BASELINE_METRICS_AVAILABLE"
                or _float_or_none(candidate_metrics.get("net_performance_proxy")) is not None,
                _text(candidate_metrics.get("net_performance_proxy")),
            ),
            st._check(
                "required_baselines_visible",
                set(baseline_control.REQUIRED_BASELINE_IDS).issubset(baseline_ids),
                ",".join(sorted(baseline_ids)),
            ),
            st._check(
                "all_required_metrics_available_or_fail_closed",
                (
                    status != "BASELINE_METRICS_AVAILABLE"
                    or all(
                        _float_or_none(row.get("net_performance_proxy")) is not None
                        for row in baselines
                    )
                ),
                "",
            ),
            st._check(
                "data_quality_gate_visible",
                _mapping(source_artifacts.get("data_quality_gate")).get("status")
                in {"PASS", "PASS_WITH_WARNINGS"},
                _text(_mapping(source_artifacts.get("data_quality_gate")).get("status")),
            ),
            st._check(
                "benchmark_control_rerun_visible",
                bool(report.get("benchmark_baseline_control_id"))
                and bool(report.get("benchmark_baseline_status")),
                "",
            ),
            st._check(
                "backtest_simulation_limitation_visible",
                "BACKTEST_SIMULATION"
                in " ".join(_texts(report.get("limitations")))
                and report.get("backtest_simulation_event_window_evidence_only") is True,
                "",
            ),
            st._check(
                "equal_weight_etf_not_shadow_candidate_mapping",
                any(
                    _text(row.get("baseline_id")) == "equal_weight_etf"
                    and _text(row.get("source_method")) == "price_cache_equal_weight_etf"
                    for row in baselines
                ),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "benchmark_baseline_metrics_status" in reader
                and "benchmark_baseline_status" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "research_only_materialization",
                report.get("research_only") is True
                and report.get("execution_model_ready") is False
                and report.get("benchmark_comparison_live_signal") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_benchmark_baseline_metrics_materialization_validation",
        materialization_id,
        checks,
    )
    if write_output:
        st._write_json(
            root / "benchmark_baseline_metrics_materialization_validation.json",
            validation,
        )
        st._write_text(
            root / "benchmark_baseline_metrics_materialization_validation.md",
            render_benchmark_baseline_metrics_materialization_validation_report(validation),
        )
    return validation


def render_benchmark_baseline_metrics_materialization_reader_brief(
    report: Mapping[str, Any],
) -> str:
    summary = _mapping(report.get("comparison_summary"))
    metric_statuses = _mapping(report.get("required_metric_statuses"))
    return "\n".join(
        [
            "## Benchmark Baseline Metrics Materialization",
            "",
            f"- benchmark_baseline_metrics_materialization_id: {report.get('materialization_id')}",
            "- benchmark_baseline_metrics_status: "
            f"{report.get('benchmark_baseline_metrics_status')}",
            f"- benchmark_baseline_candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- candidate_metric_status: {metric_statuses.get('candidate')}",
            f"- available_baseline_count: {metric_statuses.get('available_baseline_count')}",
            f"- missing_baseline_count: {metric_statuses.get('missing_baseline_count')}",
            f"- benchmark_baseline_control_id: {report.get('benchmark_baseline_control_id')}",
            f"- benchmark_baseline_status: {report.get('benchmark_baseline_status')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only benchmark metrics / not a live allocation signal / "
            "no broker / no order / no official target / no production",
            "",
        ]
    )


def render_benchmark_baseline_metrics_materialization_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    rows = [
        (
            f"| `{row.get('baseline_id')}` | {row.get('source_method')} | "
            f"{row.get('net_performance_proxy')} | {row.get('sample_count')} | "
            f"{row.get('missing_window_count')} | {row.get('metric_status')} |"
        )
        for row in _records(_mapping(report.get("baseline_metrics")).get("baselines"))
    ]
    summary = _mapping(report.get("comparison_summary"))
    data_quality = _mapping(_mapping(report.get("source_artifacts")).get("data_quality_gate"))
    return "\n".join(
        [
            f"# Benchmark Baseline Metrics Materialization {manifest.get('materialization_id')}",
            "",
            "## Purpose",
            "Materialize explicit candidate and benchmark baseline metrics for the existing "
            "benchmark baseline control pack.",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- status: {report.get('benchmark_baseline_metrics_status')}",
            f"- benchmark_baseline_status: {report.get('benchmark_baseline_status')}",
            f"- benchmark_baseline_control_id: {report.get('benchmark_baseline_control_id')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- data_quality_status: {data_quality.get('status')}",
            f"- candidate_metrics_path: {report.get('candidate_metrics_path')}",
            f"- baseline_metrics_path: {report.get('baseline_metrics_path')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Baseline Metrics",
            "| baseline | source method | net proxy | sample count | missing windows | status |",
            "|---|---|---:|---:|---:|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- research-only benchmark metrics materialization",
            "- BACKTEST_SIMULATION event windows are not PIT/live evidence",
            "- benchmark comparison is not a live allocation signal",
            "- no broker integration or order ticket",
            "- no official target weights",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_benchmark_baseline_metrics_materialization_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            "# Benchmark Baseline Metrics Materialization Validation "
            f"{validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *checks,
            "",
        ]
    )


def _candidate_benchmark_metrics(
    *,
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    cost_review_payload: Mapping[str, Any],
    candidate_metrics_payload: Mapping[str, Any],
    outcome_payload: Mapping[str, Any],
    effective_as_of: date,
    generated_at: datetime,
) -> dict[str, Any]:
    cost_review = _mapping(cost_review_payload.get("cost_sensitivity_review"))
    scenario = _conservative_cost_scenario(cost_review)
    gross = _first_float(candidate_metrics_payload, "gross_performance_proxy")
    if gross is None:
        gross = _first_float(source_row, "avg_5d_return")
    net = _first_float(scenario, "net_performance_proxy")
    if net is None:
        net = gross
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_benchmark_metrics",
        "metrics_id": st._stable_id(
            "candidate-benchmark-metrics",
            candidate,
            source_variant,
            _text(outcome_payload.get("sim_outcome_id")),
            generated_at.isoformat(),
        ),
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "source_variant": source_variant,
        "source_artifact_id": outcome_payload.get("sim_outcome_id"),
        "source_candidate_metrics_id": candidate_metrics_payload.get("metrics_id"),
        "cost_sensitivity_source": {
            "review_id": cost_review_payload.get("review_id"),
            "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
            "validation_status": _mapping(
                cost_review_payload.get("cost_sensitivity_validation")
            ).get("status"),
            "scenario_id": scenario.get("scenario_id"),
            "scenario_label": scenario.get("label"),
        },
        "gross_performance_proxy": _round_or_none(gross),
        "net_performance_proxy": _round_or_none(net),
        "turnover": _round_or_none(
            _first_float(candidate_metrics_payload, "turnover")
            or _first_float(source_row, "avg_turnover")
        ),
        "drawdown_proxy": _round_or_none(
            _first_float(candidate_metrics_payload, "drawdown_proxy")
            or _first_float(source_row, "avg_max_drawdown_20d")
        ),
        "trade_rotation_count": _int(
            candidate_metrics_payload.get("trade_rotation_count")
            or source_row.get("event_count")
        ),
        "sample_count": _int(source_row.get("available_count")),
        "metric_source": "cost_metrics_materialization + cost_sensitivity_review",
        "limitation": (
            "Candidate net proxy uses the conservative cost-sensitivity scenario; "
            "BACKTEST_SIMULATION metric proxy is not PIT/live execution evidence."
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }


def _baseline_metrics(
    *,
    outcome_payload: Mapping[str, Any],
    outcome_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
    prices: Any,
    effective_as_of: date,
    generated_at: datetime,
    price_cache_path: Path,
    data_quality_summary: Mapping[str, Any],
) -> dict[str, Any]:
    static_weights = _static_baseline_weights(outcome_payload)
    baseline_rows = [
        _price_baseline_row(
            baseline_id="static_allocation",
            source_method="price_cache_static_allocation",
            weights=static_weights,
            outcome_rows=outcome_rows,
            prices=prices,
        ),
        _summary_baseline_row(
            baseline_id="no_trade",
            source_method="backtest_sim_outcome_no_trade",
            row=_variant_row(summary_rows, "no_trade"),
            outcome_rows=outcome_rows,
        ),
        _price_baseline_row(
            baseline_id="qqq_only",
            source_method="price_cache_single_asset",
            weights={"QQQ": 1.0},
            outcome_rows=outcome_rows,
            prices=prices,
        ),
        _price_baseline_row(
            baseline_id="spy_only",
            source_method="price_cache_single_asset",
            weights={"SPY": 1.0},
            outcome_rows=outcome_rows,
            prices=prices,
        ),
        _price_baseline_row(
            baseline_id="equal_weight_etf",
            source_method="price_cache_equal_weight_etf",
            weights={"SPY": 0.25, "QQQ": 0.25, "SMH": 0.25, "SOXX": 0.25},
            outcome_rows=outcome_rows,
            prices=prices,
        ),
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics",
        "metrics_id": st._stable_id(
            "baseline-metrics",
            _text(outcome_payload.get("sim_outcome_id")),
            effective_as_of.isoformat(),
            generated_at.isoformat(),
        ),
        "as_of": effective_as_of.isoformat(),
        "source_artifact_id": outcome_payload.get("sim_outcome_id"),
        "source_window_days": 5,
        "source_window_count": len(outcome_rows),
        "price_cache_path": str(price_cache_path),
        "price_cache_sha256": _file_sha256(price_cache_path),
        "data_quality_status": data_quality_summary.get("status"),
        "baselines": baseline_rows,
        "baseline_count": len(baseline_rows),
        "limitation": (
            "Baseline metrics use existing BACKTEST_SIMULATION event windows and cached "
            "adjusted-close prices; they are research inputs only."
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }


def _summary_baseline_row(
    *,
    baseline_id: str,
    source_method: str,
    row: Mapping[str, Any],
    outcome_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    returns = [_float_or_none(item.get("return")) for item in outcome_rows]
    returns = [item for item in returns if item is not None]
    drawdowns = [_float_or_none(item.get("max_drawdown")) for item in outcome_rows]
    drawdowns = [item for item in drawdowns if item is not None]
    gross = _first_float(row, "avg_5d_return")
    if gross is None and returns:
        gross = sum(returns) / len(returns)
    metric_status = "AVAILABLE" if gross is not None else "MISSING"
    return {
        "baseline_id": baseline_id,
        "source_method": source_method,
        "gross_performance_proxy": _round_or_none(gross),
        "net_performance_proxy": _round_or_none(gross),
        "turnover": _round_or_none(_first_float(row, "avg_turnover") or 0.0),
        "drawdown_proxy": _round_or_none(
            _first_float(row, "avg_max_drawdown_20d")
            or (sum(drawdowns) / len(drawdowns) if drawdowns else None)
        ),
        "sample_count": len(returns),
        "missing_window_count": max(0, len(outcome_rows) - len(returns)),
        "metric_status": metric_status,
        "limitation": "No-trade baseline sourced from existing simulated no_trade windows.",
    }


def _price_baseline_row(
    *,
    baseline_id: str,
    source_method: str,
    weights: Mapping[str, float],
    outcome_rows: Sequence[Mapping[str, Any]],
    prices: Any,
) -> dict[str, Any]:
    returns: list[float] = []
    drawdowns: list[float] = []
    missing = 0
    for row in outcome_rows:
        start = _parse_date(row.get("start_date"))
        end = _parse_date(row.get("end_date"))
        if start is None or end is None:
            missing += 1
            continue
        metrics = replay._portfolio_metrics(prices, weights, start, end)  # noqa: SLF001
        if metrics.get("status") != "AVAILABLE":
            missing += 1
            continue
        returns.append(_float(metrics.get("return")))
        drawdowns.append(_float(metrics.get("max_drawdown")))
    gross = sum(returns) / len(returns) if returns else None
    drawdown = sum(drawdowns) / len(drawdowns) if drawdowns else None
    metric_status = "AVAILABLE" if gross is not None else "MISSING"
    return {
        "baseline_id": baseline_id,
        "source_method": source_method,
        "weights": {key: _round_or_none(value) for key, value in sorted(weights.items())},
        "gross_performance_proxy": _round_or_none(gross),
        "net_performance_proxy": _round_or_none(gross),
        "turnover": 0.0,
        "drawdown_proxy": _round_or_none(drawdown),
        "sample_count": len(returns),
        "missing_window_count": missing,
        "metric_status": metric_status,
        "limitation": (
            "Price-derived hold-period baseline over existing BACKTEST_SIMULATION "
            "event windows; no live allocation or execution model."
        ),
    }


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=sim_outcome_id is None,
        output_dir=output_dir,
    )


def _load_candidate_cost_metrics(
    *,
    metrics_path: Path | None,
    materialization_id: str | None,
    output_dir: Path,
) -> dict[str, Any]:
    if metrics_path is not None:
        return st._read_json(metrics_path)
    payload = cost_metrics.cost_metrics_materialization_report_payload(
        materialization_id=materialization_id,
        latest=materialization_id is None,
        output_dir=output_dir,
    )
    report = _mapping(payload.get("cost_metrics_materialization_report"))
    path = Path(_text(report.get("candidate_metrics_path")))
    metrics = st._read_json(path)
    metrics["source_materialization_id"] = payload.get("materialization_id")
    metrics["source_materialization_path"] = payload.get(
        "cost_metrics_materialization_report_path"
    )
    return metrics


def _load_cost_review(*, review_id: str | None, output_dir: Path) -> dict[str, Any]:
    return cost.cost_sensitivity_report_payload(
        review_id=review_id,
        latest=review_id is None,
        output_dir=output_dir,
    )


def _run_data_quality_gate(
    *,
    as_of: date,
    price_cache_path: Path,
    rates_cache_path: Path,
    report_path: Path,
) -> Any:
    universe = load_universe()
    quality = validate_data_cache(
        prices_path=price_cache_path,
        rates_path=rates_cache_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=replay._download_manifest_path(price_cache_path),  # noqa: SLF001
        secondary_prices_path=replay._marketstack_prices_path(price_cache_path),  # noqa: SLF001
        require_secondary_prices=replay._requires_marketstack_prices(price_cache_path),  # noqa: SLF001
    )
    write_data_quality_report(quality, report_path)
    return quality


def _quality_summary(quality: Any, *, report_path: Path) -> dict[str, Any]:
    return {
        "status": _text(getattr(quality, "status", "")),
        "as_of": getattr(quality, "as_of", None).isoformat()
        if getattr(quality, "as_of", None)
        else "",
        "error_count": _int(getattr(quality, "error_count", 0)),
        "warning_count": _int(getattr(quality, "warning_count", 0)),
        "report_path": str(report_path),
        "production_effect": "none",
    }


def _metric_statuses(
    *,
    candidate_metrics_payload: Mapping[str, Any],
    baseline_metrics_payload: Mapping[str, Any],
) -> dict[str, Any]:
    baseline_rows = _records(baseline_metrics_payload.get("baselines"))
    available_ids = [
        _text(row.get("baseline_id"))
        for row in baseline_rows
        if _float_or_none(row.get("net_performance_proxy")) is not None
    ]
    missing_ids = sorted(set(baseline_control.REQUIRED_BASELINE_IDS) - set(available_ids))
    return {
        "candidate": "AVAILABLE"
        if _float_or_none(candidate_metrics_payload.get("net_performance_proxy")) is not None
        else "MISSING",
        "required_baseline_ids": list(baseline_control.REQUIRED_BASELINE_IDS),
        "available_baseline_ids": sorted(available_ids),
        "missing_baseline_ids": missing_ids,
        "available_baseline_count": len(available_ids),
        "missing_baseline_count": len(missing_ids),
    }


def _materialization_status_from_metrics(metric_statuses: Mapping[str, Any]) -> str:
    if metric_statuses.get("candidate") != "AVAILABLE":
        return "INSUFFICIENT_BASELINE_METRICS"
    missing = _texts(metric_statuses.get("missing_baseline_ids"))
    if not missing:
        return "BASELINE_METRICS_AVAILABLE"
    if _int(metric_statuses.get("available_baseline_count")):
        return "BASELINE_METRICS_PARTIAL"
    return "INSUFFICIENT_BASELINE_METRICS"


def _materialization_status(
    metric_statuses: Mapping[str, Any],
    control_pack: Mapping[str, Any],
) -> str:
    status = _materialization_status_from_metrics(metric_statuses)
    if _text(control_pack.get("benchmark_baseline_status")) == "INSUFFICIENT_BASELINE_METRICS":
        return "INSUFFICIENT_BASELINE_METRICS"
    return status


def _blocking_reasons(
    metric_statuses: Mapping[str, Any],
    control_pack: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if metric_statuses.get("candidate") != "AVAILABLE":
        reasons.append("candidate_metrics:missing_net_performance_proxy")
    for baseline_id in _texts(metric_statuses.get("missing_baseline_ids")):
        reasons.append(f"baseline_metrics:{baseline_id}:missing_net_performance_proxy")
    if _text(control_pack.get("benchmark_baseline_status")) == "INSUFFICIENT_BASELINE_METRICS":
        reasons.append("benchmark_baseline_control:insufficient_metrics")
    return _dedupe(reasons)


def _warnings(
    *,
    candidate_metrics_payload: Mapping[str, Any],
    baseline_metrics_payload: Mapping[str, Any],
    control_pack: Mapping[str, Any],
    data_quality: Any,
) -> list[str]:
    warnings: list[str] = []
    if _text(getattr(data_quality, "status", "")) == "PASS_WITH_WARNINGS":
        warnings.append("data_quality:pass_with_warnings")
    if _text(candidate_metrics_payload.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("candidate_metrics:backtest_simulation_not_pit")
    if _text(baseline_metrics_payload.get("limitation")):
        warnings.append("baseline_metrics:backtest_simulation_event_windows")
    benchmark_status = _text(control_pack.get("benchmark_baseline_status"))
    if benchmark_status in {"MIXED_BASELINE_RESULT", "CANDIDATE_UNDERPERFORMS_BASELINES"}:
        warnings.append(f"benchmark_baseline_control:{benchmark_status.lower()}")
    cost_status = _text(
        _mapping(candidate_metrics_payload.get("cost_sensitivity_source")).get(
            "cost_sensitivity_status"
        )
    )
    if cost_status and cost_status not in {
        "MEANINGFUL_ALL_SCENARIOS",
        "MEANINGFUL_LOW_MEDIUM_ONLY",
    }:
        warnings.append(f"cost_sensitivity_review:{cost_status.lower()}")
    return _dedupe(warnings)


def _next_action(status: str, control_pack: Mapping[str, Any]) -> str:
    benchmark_status = _text(control_pack.get("benchmark_baseline_status"))
    if status == "INSUFFICIENT_BASELINE_METRICS":
        return "keep_baseline_control_insufficient_until_candidate_and_baseline_metrics_exist"
    if benchmark_status == "CANDIDATE_OUTPERFORMS_BASELINES":
        return "owner_review_required_before_any_promotion_board_use"
    if benchmark_status == "MIXED_BASELINE_RESULT":
        return "owner_review_mixed_benchmark_results_before_promotion_board"
    if benchmark_status == "CANDIDATE_UNDERPERFORMS_BASELINES":
        return "return_candidate_to_research_until_it_outperforms_baseline_controls"
    return _text(
        control_pack.get("next_required_action"),
        "review_benchmark_baseline_metrics_materialization",
    )


def _conservative_cost_scenario(cost_review: Mapping[str, Any]) -> dict[str, Any]:
    scenarios = _records(cost_review.get("scenario_results"))
    high = next((row for row in scenarios if _text(row.get("scenario_id")) == "high"), None)
    if high is not None:
        return dict(high)
    with_net = [
        row
        for row in scenarios
        if _float_or_none(row.get("net_performance_proxy")) is not None
    ]
    if not with_net:
        return {}
    return min(with_net, key=lambda row: _float(row.get("net_performance_proxy")))


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


def _static_baseline_weights(outcome_payload: Mapping[str, Any]) -> dict[str, float]:
    config = _sim_event_config(outcome_payload)
    weights = _mapping(_mapping(config.get("portfolio")).get("baseline_snapshot"))
    if not weights:
        weights = {"QQQ": 0.5, "SMH": 0.2, "TLT": 0.1, "CASH": 0.2}
    return _normalize_weights(weights)


def _sim_event_config(outcome_payload: Mapping[str, Any]) -> dict[str, Any]:
    event_set_id = _text(outcome_payload.get("event_set_id"))
    if event_set_id:
        snapshot = (
            sim.DEFAULT_BACKTEST_SIM_EVENT_DIR
            / event_set_id
            / "simulation_input_snapshot.json"
        )
        if snapshot.exists():
            return _mapping(st._read_json(snapshot).get("config"))
    return {}


def _required_price_symbols() -> set[str]:
    return {"QQQ", "SPY", "SMH", "SOXX", "TLT"}


def _source_artifact(outcome_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": outcome_payload.get("sim_outcome_id"),
        "event_set_id": outcome_payload.get("event_set_id"),
        "variant_set_id": outcome_payload.get("variant_set_id"),
        "outcome_mode": outcome_payload.get("outcome_mode"),
        "pit_safety_status": outcome_payload.get("pit_safety_status"),
        "manifest_path": outcome_payload.get("sim_outcome_manifest_path"),
        "window_path": outcome_payload.get("simulated_outcome_windows_path"),
        "summary_path": outcome_payload.get("simulated_variant_summary_path"),
        "production_effect": "none",
    }


def _candidate_source_artifact(candidate_metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": candidate_metrics.get("source_candidate_metrics_id")
        or candidate_metrics.get("metrics_id"),
        "source_materialization_id": candidate_metrics.get("source_materialization_id"),
        "status": "OK"
        if _float_or_none(candidate_metrics.get("net_performance_proxy")) is not None
        else "MISSING",
        "production_effect": "none",
    }


def _cost_source_artifact(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": source.get("review_id"),
        "status": source.get("cost_sensitivity_status"),
        "validation_status": source.get("validation_status"),
        "scenario_id": source.get("scenario_id"),
        "production_effect": "none",
    }


def _price_source_artifact(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _file_sha256(path),
        "row_count": _csv_row_count(path),
        "provider": "existing_price_cache",
        "endpoint": "local_cache",
        "request_parameters": "not_downloaded_by_materialization",
        "production_effect": "none",
    }


def _variant_row(rows: Sequence[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    return next((dict(row) for row in rows if _text(row.get("variant")) == variant), {})


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float(value: object, default: float = 0.0) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 10)


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    clean = {
        _text(symbol): _float(weight)
        for symbol, weight in weights.items()
        if _text(symbol) and _float(weight) > 0
    }
    total = sum(clean.values())
    if total <= 0:
        return {}
    return {symbol: value / total for symbol, value in clean.items()}


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_row_count(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _joined_texts(value: object) -> str:
    return ", ".join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
