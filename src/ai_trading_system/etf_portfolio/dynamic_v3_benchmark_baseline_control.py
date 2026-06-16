from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost_review
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_BENCHMARK_BASELINE_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "benchmark_baseline_control_v1.yaml"
)
DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "benchmark_baseline_control"
)
REQUIRED_BASELINE_IDS = (
    "static_allocation",
    "no_trade",
    "qqq_only",
    "spy_only",
    "equal_weight_etf",
)
BENCHMARK_BASELINE_STATUSES = (
    "CANDIDATE_OUTPERFORMS_BASELINES",
    "MIXED_BASELINE_RESULT",
    "CANDIDATE_UNDERPERFORMS_BASELINES",
    "INSUFFICIENT_BASELINE_METRICS",
    "BLOCKED_POLICY",
)
BENCHMARK_BASELINE_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "benchmark_control_pack_only": True,
    "execution_model_ready": False,
    "data_downloaded_by_pack": False,
    "pipelines_executed_by_pack": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}


def load_benchmark_baseline_policy(
    config_path: Path = DEFAULT_BENCHMARK_BASELINE_CONFIG_PATH,
) -> dict[str, Any]:
    return _normalized_policy(st._load_yaml_mapping(config_path), config_path=config_path)


def run_benchmark_baseline_control_pack(
    *,
    as_of: date | None = None,
    candidate_metrics_path: Path | None = None,
    candidate_metrics: Mapping[str, Any] | None = None,
    baseline_metrics_path: Path | None = None,
    baseline_metrics: Mapping[str, Any] | None = None,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    cost_sensitivity_review_id: str | None = None,
    cost_sensitivity_dir: Path = cost_review.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    config_path: Path = DEFAULT_BENCHMARK_BASELINE_CONFIG_PATH,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = load_benchmark_baseline_policy(config_path)
    weekly_source = _weekly_source(
        weekly_review_id=weekly_review_id,
        output_dir=weekly_review_dir,
    )
    cost_source = _cost_sensitivity_source(
        review_id=cost_sensitivity_review_id,
        output_dir=cost_sensitivity_dir,
    )
    candidate_source = _candidate_metrics_source(
        metrics=candidate_metrics,
        metrics_path=candidate_metrics_path,
        weekly_source=weekly_source,
    )
    baseline_source = _baseline_metrics_source(
        metrics=baseline_metrics,
        metrics_path=baseline_metrics_path,
    )
    effective_as_of = (
        as_of
        or _parse_optional_date(_mapping(candidate_source.get("summary")).get("as_of"))
        or _parse_optional_date(_mapping(weekly_source.get("summary")).get("week_end"))
        or generated.date()
    )
    baselines = _baseline_records(
        policy=policy,
        candidate_summary=_mapping(candidate_source.get("summary")),
        baseline_metrics_summary=_mapping(baseline_source.get("summary")),
    )
    blocking_reasons = _blocking_reasons(
        policy=policy,
        candidate_source=candidate_source,
        baseline_source=baseline_source,
    )
    warnings = _warnings(weekly_source=weekly_source, cost_source=cost_source)
    status = _control_status(
        blocking_reasons=blocking_reasons,
        baselines=baselines,
    )
    candidate = _text(
        _mapping(candidate_source.get("summary")).get("candidate"),
        _text(_mapping(weekly_source.get("summary")).get("candidate"), "UNKNOWN"),
    )
    control_id = st._stable_id(
        "benchmark-baseline-control",
        candidate,
        effective_as_of.isoformat(),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        _text(candidate_source.get("artifact_id")),
        _text(baseline_source.get("artifact_id")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / control_id)
    root.mkdir(parents=True, exist_ok=False)
    comparison_summary = _comparison_summary(baselines)
    monthly_inputs = _monthly_review_pack_inputs(
        control_id=root.name,
        candidate=candidate,
        status=status,
        comparison_summary=comparison_summary,
        weekly_source=weekly_source,
        cost_source=cost_source,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )
    pack = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_control_pack",
        "control_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "policy": policy,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "minimum_outperformance_threshold": _float(
            _mapping(policy.get("comparison_summary")).get(
                "minimum_outperformance_threshold"
            )
        ),
        "benchmark_baseline_status": status,
        "baseline_count": len(baselines),
        "required_baselines_present": _required_baselines_present(policy),
        "missing_required_baselines": _missing_required_baselines(policy),
        "candidate_metrics_summary": candidate_source.get("summary"),
        "baseline_metrics_summary": baseline_source.get("summary"),
        "baselines": baselines,
        "comparison_summary": comparison_summary,
        "source_artifacts": {
            "candidate_metrics": candidate_source,
            "baseline_metrics": baseline_source,
            "paper_shadow_weekly_review": weekly_source,
            "cost_sensitivity_review": cost_source,
        },
        "weekly_review_input": _weekly_review_input(weekly_source),
        "cost_sensitivity_input": _cost_sensitivity_input(cost_source),
        "monthly_review_pack_inputs": monthly_inputs,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_required_action": _next_required_action(status),
        "limitations": [
            "research-only benchmark baseline controls",
            "does not run backtests or refresh market data",
            "does not fabricate candidate or baseline metrics",
            "does not approve candidate promotion or production target weights",
        ],
        **BENCHMARK_BASELINE_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_manifest",
        "control_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if status != "BLOCKED_POLICY" else "BLOCKED_POLICY",
        "benchmark_baseline_status": status,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "benchmark_baseline_manifest_path": str(
            root / "benchmark_baseline_manifest.json"
        ),
        "benchmark_baseline_control_pack_path": str(
            root / "benchmark_baseline_control_pack.json"
        ),
        "benchmark_baseline_report_path": str(root / "benchmark_baseline_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "benchmark_baseline_validation.json"),
        **BENCHMARK_BASELINE_SAFETY,
    }
    reader = render_benchmark_baseline_reader_brief(pack)
    st._write_json(root / "benchmark_baseline_manifest.json", manifest)
    st._write_json(root / "benchmark_baseline_control_pack.json", pack)
    st._write_text(
        root / "benchmark_baseline_report.md",
        render_benchmark_baseline_report(manifest, pack),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_benchmark_baseline_control",
        root.name,
        root / "benchmark_baseline_manifest.json",
    )
    validation = validate_benchmark_baseline_artifact(
        control_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "control_id": root.name,
        "control_dir": root,
        "manifest": manifest,
        "benchmark_baseline_control_pack": pack,
        "reader_brief_section": reader,
        "benchmark_baseline_validation": validation,
    }


def benchmark_baseline_report_payload(
    *,
    control_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=control_id,
        latest_pointer="latest_benchmark_baseline_control",
        latest=latest,
        output_dir=output_dir,
        required_name="benchmark_baseline_manifest.json",
    )
    payload = {
        **st._read_json(root / "benchmark_baseline_manifest.json"),
        "benchmark_baseline_control_pack": st._read_json(
            root / "benchmark_baseline_control_pack.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "control_dir": str(root),
    }
    validation = st._read_optional_json(root / "benchmark_baseline_validation.json")
    if validation:
        payload["benchmark_baseline_validation"] = validation
    return payload


def validate_benchmark_baseline_artifact(
    *,
    control_id: str,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / control_id
    manifest = st._read_optional_json(root / "benchmark_baseline_manifest.json") or {}
    pack = st._read_optional_json(root / "benchmark_baseline_control_pack.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    baselines = _records(pack.get("baselines"))
    baseline_ids = {_text(row.get("baseline_id")) for row in baselines}
    source_artifacts = _mapping(pack.get("source_artifacts"))
    checks = st._required_file_checks(
        root,
        (
            "benchmark_baseline_manifest.json",
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_pack_id_match",
                manifest.get("control_id") == pack.get("control_id") == control_id,
                "",
            ),
            st._check(
                "status_allowed",
                pack.get("benchmark_baseline_status") in BENCHMARK_BASELINE_STATUSES,
                _text(pack.get("benchmark_baseline_status")),
            ),
            st._check(
                "required_baselines_present",
                set(REQUIRED_BASELINE_IDS).issubset(baseline_ids),
                ",".join(sorted(baseline_ids)),
            ),
            st._check(
                "baseline_metadata_complete",
                all(_baseline_metadata_complete(row) for row in baselines),
                "",
            ),
            st._check(
                "comparison_summary_visible",
                bool(_mapping(pack.get("comparison_summary")).get("baseline_count")),
                "",
            ),
            st._check(
                "insufficient_metrics_fail_closed",
                (
                    _mapping(source_artifacts.get("candidate_metrics")).get("status")
                    == "OK"
                    and _mapping(source_artifacts.get("baseline_metrics")).get("status")
                    == "OK"
                )
                or pack.get("benchmark_baseline_status")
                == "INSUFFICIENT_BASELINE_METRICS",
                "",
            ),
            st._check(
                "monthly_review_inputs_visible",
                bool(_mapping(pack.get("monthly_review_pack_inputs")).get("control_id")),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "benchmark_baseline_status" in reader
                and "baseline_count" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "research_only_control_pack",
                pack.get("research_only") is True
                and pack.get("execution_model_ready") is False
                and pack.get("data_downloaded_by_pack") is False
                and pack.get("pipelines_executed_by_pack") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, pack), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_benchmark_baseline_validation",
        control_id,
        checks,
    )
    if write_output:
        st._write_json(root / "benchmark_baseline_validation.json", validation)
        st._write_text(
            root / "benchmark_baseline_validation.md",
            render_benchmark_baseline_validation_report(validation),
        )
    return validation


def render_benchmark_baseline_reader_brief(pack: Mapping[str, Any]) -> str:
    summary = _mapping(pack.get("comparison_summary"))
    return "\n".join(
        [
            "## Benchmark Baseline Control",
            "",
            f"- benchmark_baseline_control_id: {pack.get('control_id')}",
            f"- benchmark_baseline_candidate: {pack.get('candidate')}",
            f"- benchmark_baseline_status: {pack.get('benchmark_baseline_status')}",
            f"- benchmark_baseline_policy: {pack.get('policy_id')} / {pack.get('policy_version')}",
            f"- baseline_count: {pack.get('baseline_count')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- worst_baseline_delta: {summary.get('worst_baseline_delta')}",
            f"- best_baseline_delta: {summary.get('best_baseline_delta')}",
            f"- blocking_reasons: {_joined_texts(pack.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(pack.get('warnings'))}",
            f"- next_required_action: {pack.get('next_required_action')}",
            "- safety_boundary: research-only benchmark controls / no broker / "
            "no order / no official target / no production",
            "",
        ]
    )


def render_benchmark_baseline_report(
    manifest: Mapping[str, Any],
    pack: Mapping[str, Any],
) -> str:
    rows = [
        (
            f"| `{row.get('baseline_id')}` | {row.get('baseline_type')} | "
            f"{row.get('candidate_net_performance_proxy')} | "
            f"{row.get('baseline_net_performance_proxy')} | "
            f"{row.get('candidate_delta_vs_baseline')} | "
            f"{row.get('comparison_classification')} |"
        )
        for row in _records(pack.get("baselines"))
    ]
    summary = _mapping(pack.get("comparison_summary"))
    return "\n".join(
        [
            f"# Benchmark Baseline Control {manifest.get('control_id')}",
            "",
            "## Purpose",
            "Standardize research-only candidate comparisons against static, no-trade, "
            "single-asset and equal-weight ETF baselines.",
            "",
            "## Summary",
            f"- candidate: {pack.get('candidate')}",
            f"- benchmark_baseline_status: {pack.get('benchmark_baseline_status')}",
            f"- policy: {pack.get('policy_id')} / {pack.get('policy_version')}",
            f"- baseline_count: {pack.get('baseline_count')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- insufficient_metric_baseline_count: "
            f"{summary.get('insufficient_metric_baseline_count')}",
            f"- worst_baseline_delta: {summary.get('worst_baseline_delta')}",
            f"- best_baseline_delta: {summary.get('best_baseline_delta')}",
            f"- blocking_reasons: {_joined_texts(pack.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(pack.get('warnings'))}",
            f"- next_required_action: {pack.get('next_required_action')}",
            "",
            "## Baselines",
            "| baseline | type | candidate net | baseline net | delta | classification |",
            "|---|---|---:|---:|---:|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- research-only benchmark baseline controls",
            "- no backtest execution or data refresh",
            "- no broker integration or order ticket",
            "- no official target weights",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_benchmark_baseline_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Benchmark Baseline Validation {validation.get('artifact_id')}",
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


def _normalized_policy(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {**BENCHMARK_BASELINE_SAFETY, **_mapping(config.get("safety_boundaries"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "policy_id": _text(
            config.get("policy_id"),
            "dynamic_v3_rescue_benchmark_baseline_control_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_manual_review_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "comparison_summary": _mapping(config.get("comparison_summary")),
        "baselines": [_normalized_baseline(row) for row in _records(config.get("baselines"))],
        "safety_boundaries": safety,
        **safety,
    }


def _normalized_baseline(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "baseline_id": _text(row.get("baseline_id")),
        "label": _text(row.get("label")),
        "baseline_type": _text(row.get("baseline_type")),
        "asset_universe": _texts(row.get("asset_universe")),
        "rebalancing_assumption": _text(row.get("rebalancing_assumption")),
        "cost_assumption": _text(row.get("cost_assumption")),
        "applicability": _text(row.get("applicability")),
        "limitations": _texts(row.get("limitations")),
        **BENCHMARK_BASELINE_SAFETY,
    }


def _weekly_source(*, weekly_review_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = weekly.paper_shadow_weekly_review_report_payload(
            weekly_review_id=weekly_review_id,
            latest=weekly_review_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_weekly_review", f"weekly review missing: {exc}")
    review = _mapping(payload.get("paper_shadow_weekly_review"))
    return _source(
        "paper_shadow_weekly_review",
        exists=True,
        artifact_id=_text(payload.get("weekly_review_id")),
        status=_text(review.get("weekly_decision"), _text(payload.get("status"), "UNKNOWN")),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_weekly_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_weekly_manifest_path"))),
        summary={
            "weekly_review_id": payload.get("weekly_review_id"),
            "candidate": review.get("candidate"),
            "week_start": review.get("week_start"),
            "week_end": review.get("week_end"),
            "weekly_decision": review.get("weekly_decision"),
            "coverage_status": review.get("coverage_status"),
        },
        payload=review,
    )


def _cost_sensitivity_source(*, review_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        summary = cost_review.latest_cost_sensitivity_summary(
            review_id=review_id,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("cost_sensitivity_review", f"cost review missing: {exc}")
    if summary.get("availability") != "AVAILABLE":
        return _missing_source(
            "cost_sensitivity_review",
            _text(summary.get("limitation"), "cost review missing"),
        )
    return _source(
        "cost_sensitivity_review",
        exists=True,
        artifact_id=_text(summary.get("review_id")),
        status=_text(summary.get("cost_sensitivity_status"), "UNKNOWN"),
        validation_status=_text(summary.get("validation_status"), "NOT_RUN"),
        source_path=Path(_text(summary.get("report_path"))),
        summary=summary,
        payload={**summary, **BENCHMARK_BASELINE_SAFETY},
    )


def _candidate_metrics_source(
    *,
    metrics: Mapping[str, Any] | None,
    metrics_path: Path | None,
    weekly_source: Mapping[str, Any],
) -> dict[str, Any]:
    if metrics is not None:
        payload = dict(metrics)
        source_path: Path | None = None
    elif metrics_path is not None:
        payload = st._read_json(metrics_path)
        source_path = metrics_path
    else:
        payload = {
            "metrics_id": "candidate_metrics_missing",
            "candidate": _mapping(weekly_source.get("summary")).get("candidate"),
            "as_of": _mapping(weekly_source.get("summary")).get("week_end"),
            "limitation": "explicit candidate metrics missing",
            **BENCHMARK_BASELINE_SAFETY,
        }
        source_path = None
    summary = _candidate_summary(payload)
    return _source(
        "candidate_metrics",
        exists=True,
        artifact_id=_text(summary.get("metrics_id"), "candidate_metrics"),
        status="OK" if _candidate_metrics_complete(summary) else "INSUFFICIENT_METRICS",
        validation_status=_text(payload.get("validation_status"), "NOT_APPLICABLE"),
        source_path=source_path,
        summary=summary,
        payload={**payload, **BENCHMARK_BASELINE_SAFETY},
    )


def _baseline_metrics_source(
    *,
    metrics: Mapping[str, Any] | None,
    metrics_path: Path | None,
) -> dict[str, Any]:
    if metrics is not None:
        payload = dict(metrics)
        source_path: Path | None = None
    elif metrics_path is not None:
        payload = st._read_json(metrics_path)
        source_path = metrics_path
    else:
        payload = {
            "metrics_id": "baseline_metrics_missing",
            "baselines": [],
            "limitation": "explicit baseline metrics missing",
            **BENCHMARK_BASELINE_SAFETY,
        }
        source_path = None
    summary = _baseline_metrics_summary(payload)
    return _source(
        "baseline_metrics",
        exists=True,
        artifact_id=_text(summary.get("metrics_id"), "baseline_metrics"),
        status="OK" if _baseline_metrics_complete(summary) else "INSUFFICIENT_METRICS",
        validation_status=_text(payload.get("validation_status"), "NOT_APPLICABLE"),
        source_path=source_path,
        summary=summary,
        payload={**payload, **BENCHMARK_BASELINE_SAFETY},
    )


def _candidate_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    net = _first_float(
        payload,
        "net_performance_proxy",
        "candidate_net_performance_proxy",
        "net_return_proxy",
        "total_return",
    )
    gross = _first_float(payload, "gross_performance_proxy", "candidate_return_proxy")
    if net is None:
        net = gross
    return {
        "metrics_id": _text(payload.get("metrics_id"), _text(payload.get("artifact_id"))),
        "candidate": _text(payload.get("candidate")),
        "as_of": _text(payload.get("as_of")),
        "net_performance_proxy": _round_or_none(net),
        "gross_performance_proxy": _round_or_none(gross),
        "turnover": _round_or_none(_first_float(payload, "turnover", "turnover_proxy")),
        "limitation": _text(payload.get("limitation")),
    }


def _baseline_metrics_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows = [_baseline_metric_row(row) for row in _records(payload.get("baselines"))]
    return {
        "metrics_id": _text(payload.get("metrics_id"), _text(payload.get("artifact_id"))),
        "as_of": _text(payload.get("as_of")),
        "baseline_count": len(rows),
        "baselines": rows,
        "limitation": _text(payload.get("limitation")),
    }


def _baseline_metric_row(row: Mapping[str, Any]) -> dict[str, Any]:
    net = _first_float(
        row,
        "net_performance_proxy",
        "baseline_net_performance_proxy",
        "return_proxy",
        "total_return",
    )
    gross = _first_float(row, "gross_performance_proxy", "baseline_return_proxy")
    if net is None:
        net = gross
    return {
        "baseline_id": _text(row.get("baseline_id")),
        "net_performance_proxy": _round_or_none(net),
        "gross_performance_proxy": _round_or_none(gross),
        "turnover": _round_or_none(_first_float(row, "turnover", "turnover_proxy")),
    }


def _baseline_records(
    *,
    policy: Mapping[str, Any],
    candidate_summary: Mapping[str, Any],
    baseline_metrics_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    metrics_by_id = {
        _text(row.get("baseline_id")): row
        for row in _records(baseline_metrics_summary.get("baselines"))
    }
    threshold = _float(
        _mapping(policy.get("comparison_summary")).get("minimum_outperformance_threshold")
    )
    return [
        _baseline_record(
            baseline,
            candidate_summary=candidate_summary,
            baseline_metric=_mapping(metrics_by_id.get(_text(baseline.get("baseline_id")))),
            threshold=threshold,
        )
        for baseline in _records(policy.get("baselines"))
    ]


def _baseline_record(
    baseline: Mapping[str, Any],
    *,
    candidate_summary: Mapping[str, Any],
    baseline_metric: Mapping[str, Any],
    threshold: float,
) -> dict[str, Any]:
    candidate_net = _float_or_none(candidate_summary.get("net_performance_proxy"))
    baseline_net = _float_or_none(baseline_metric.get("net_performance_proxy"))
    if candidate_net is None or baseline_net is None:
        delta = None
        outperformed = False
        classification = "INSUFFICIENT_METRICS"
    else:
        delta = candidate_net - baseline_net
        outperformed = delta >= threshold
        classification = "OUTPERFORMED" if outperformed else "NOT_OUTPERFORMED"
    return {
        **dict(baseline),
        "candidate_net_performance_proxy": _round_or_none(candidate_net),
        "baseline_net_performance_proxy": _round_or_none(baseline_net),
        "baseline_turnover": baseline_metric.get("turnover"),
        "candidate_delta_vs_baseline": _round_or_none(delta),
        "minimum_outperformance_threshold": threshold,
        "candidate_outperformed": outperformed,
        "comparison_classification": classification,
        **BENCHMARK_BASELINE_SAFETY,
    }


def _comparison_summary(baselines: list[Mapping[str, Any]]) -> dict[str, Any]:
    deltas = [
        value
        for row in baselines
        if (value := _float_or_none(row.get("candidate_delta_vs_baseline"))) is not None
    ]
    return {
        "baseline_count": len(baselines),
        "outperformed_baseline_count": sum(
            1 for row in baselines if row.get("candidate_outperformed") is True
        ),
        "underperformed_baseline_count": sum(
            1
            for row in baselines
            if row.get("comparison_classification") == "NOT_OUTPERFORMED"
        ),
        "insufficient_metric_baseline_count": sum(
            1
            for row in baselines
            if row.get("comparison_classification") == "INSUFFICIENT_METRICS"
        ),
        "worst_baseline_delta": _round_or_none(min(deltas)) if deltas else None,
        "best_baseline_delta": _round_or_none(max(deltas)) if deltas else None,
    }


def _blocking_reasons(
    *,
    policy: Mapping[str, Any],
    candidate_source: Mapping[str, Any],
    baseline_source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not _required_baselines_present(policy):
        reasons.append("benchmark_policy:missing_required_baselines")
    if candidate_source.get("status") != "OK":
        reasons.append("candidate_metrics:insufficient_metrics")
    if baseline_source.get("status") != "OK":
        reasons.append("baseline_metrics:insufficient_metrics")
    return _dedupe(reasons)


def _warnings(
    *,
    weekly_source: Mapping[str, Any],
    cost_source: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if weekly_source.get("exists") is not True:
        warnings.append("paper_shadow_weekly_review:missing")
    if cost_source.get("exists") is not True:
        warnings.append("cost_sensitivity_review:missing")
    elif _text(cost_source.get("status")) not in {
        "MEANINGFUL_ALL_SCENARIOS",
        "MEANINGFUL_LOW_MEDIUM_ONLY",
    }:
        warnings.append(f"cost_sensitivity_review:{_text(cost_source.get('status')).lower()}")
    return _dedupe(warnings)


def _control_status(*, blocking_reasons: list[str], baselines: list[Mapping[str, Any]]) -> str:
    if any(reason.startswith("benchmark_policy") for reason in blocking_reasons):
        return "BLOCKED_POLICY"
    if any("insufficient_metrics" in reason for reason in blocking_reasons):
        return "INSUFFICIENT_BASELINE_METRICS"
    outperformed = sum(1 for row in baselines if row.get("candidate_outperformed") is True)
    if outperformed == len(baselines) and baselines:
        return "CANDIDATE_OUTPERFORMS_BASELINES"
    if outperformed:
        return "MIXED_BASELINE_RESULT"
    return "CANDIDATE_UNDERPERFORMS_BASELINES"


def _monthly_review_pack_inputs(
    *,
    control_id: str,
    candidate: str,
    status: str,
    comparison_summary: Mapping[str, Any],
    weekly_source: Mapping[str, Any],
    cost_source: Mapping[str, Any],
    blocking_reasons: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "control_id": control_id,
        "candidate": candidate,
        "pack_use": "research_monthly_review_input_only",
        "benchmark_baseline_status": status,
        "source_weekly_review_id": _mapping(weekly_source.get("summary")).get(
            "weekly_review_id"
        ),
        "source_cost_sensitivity_review_id": _mapping(cost_source.get("summary")).get(
            "review_id"
        ),
        "comparison_summary": dict(comparison_summary),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "owner_review_required": True,
        "automatic_candidate_promotion": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_review_input(weekly_source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(weekly_source.get("summary"))
    return {
        "weekly_review_id": summary.get("weekly_review_id"),
        "candidate": summary.get("candidate"),
        "week_start": summary.get("week_start"),
        "week_end": summary.get("week_end"),
        "weekly_decision": summary.get("weekly_decision"),
        "coverage_status": summary.get("coverage_status"),
        "source_status": weekly_source.get("status"),
        "source_validation_status": weekly_source.get("validation_status"),
    }


def _cost_sensitivity_input(cost_source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(cost_source.get("summary"))
    return {
        "review_id": summary.get("review_id"),
        "cost_sensitivity_status": summary.get("cost_sensitivity_status"),
        "high_cost_improvement_meaningful": summary.get(
            "high_cost_improvement_meaningful"
        ),
        "worst_net_improvement_proxy": summary.get("worst_net_improvement_proxy"),
        "source_status": cost_source.get("status"),
        "source_validation_status": cost_source.get("validation_status"),
    }


def _next_required_action(status: str) -> str:
    if status == "CANDIDATE_OUTPERFORMS_BASELINES":
        return "include_baseline_control_pack_in_next_monthly_or_promotion_board_review"
    if status == "MIXED_BASELINE_RESULT":
        return "owner_review_mixed_baseline_results_before_promotion_board"
    if status == "CANDIDATE_UNDERPERFORMS_BASELINES":
        return "return_candidate_to_research_until_it_outperforms_baseline_controls"
    if status == "BLOCKED_POLICY":
        return "repair_benchmark_baseline_policy_before_control_pack_review"
    return "provide_candidate_and_baseline_metrics_before_baseline_control_review"


def _source(
    source_id: str,
    *,
    exists: bool,
    artifact_id: str,
    status: str,
    validation_status: str,
    source_path: Path | None,
    summary: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "safety_status": "PASS" if st._payload_safe(payload) else "FAIL",
        "production_effect": _text(payload.get("production_effect"), "none"),
    }


def _missing_source(source_id: str, reason: str) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {"limitation": reason},
        "safety_status": "PASS",
        "production_effect": "none",
    }


def _required_baselines_present(policy: Mapping[str, Any]) -> bool:
    return not _missing_required_baselines(policy)


def _missing_required_baselines(policy: Mapping[str, Any]) -> list[str]:
    baseline_ids = {_text(row.get("baseline_id")) for row in _records(policy.get("baselines"))}
    return sorted(set(REQUIRED_BASELINE_IDS) - baseline_ids)


def _baseline_metadata_complete(row: Mapping[str, Any]) -> bool:
    return all(
        [
            bool(_text(row.get("baseline_id"))),
            bool(_text(row.get("baseline_type"))),
            bool(_texts(row.get("asset_universe"))),
            bool(_text(row.get("rebalancing_assumption"))),
            bool(_text(row.get("cost_assumption"))),
            bool(_text(row.get("applicability"))),
            bool(_texts(row.get("limitations"))),
        ]
    )


def _candidate_metrics_complete(summary: Mapping[str, Any]) -> bool:
    return _float_or_none(summary.get("net_performance_proxy")) is not None


def _baseline_metrics_complete(summary: Mapping[str, Any]) -> bool:
    rows = _records(summary.get("baselines"))
    ids_with_metrics = {
        _text(row.get("baseline_id"))
        for row in rows
        if _float_or_none(row.get("net_performance_proxy")) is not None
    }
    return set(REQUIRED_BASELINE_IDS).issubset(ids_with_metrics)


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _float(value: object) -> float:
    parsed = _float_or_none(value)
    return 0.0 if parsed is None else parsed


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 10)


def _parse_optional_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


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
