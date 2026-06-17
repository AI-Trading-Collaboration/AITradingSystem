from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_COST_METRICS_MATERIALIZATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "cost_metrics_materialization"
)

COST_METRICS_MATERIALIZATION_STATUSES = (
    "COST_INPUTS_AVAILABLE",
    "COST_INPUTS_PARTIAL",
    "INSUFFICIENT_COST_INPUTS",
)
COST_METRICS_MATERIALIZATION_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "cost_metrics_materialization_only": True,
    "research_only": True,
    "manual_review_only": True,
    "backtest_simulation_evidence_only": True,
    "execution_model_ready": False,
    "data_downloaded_by_materialization": False,
    "pipelines_executed_by_materialization": False,
    "strategy_optimized_by_materialization": False,
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


def run_cost_metrics_materialization(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str = "limited_adjustment",
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: str | None = None,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    cost_sensitivity_output_dir: Path = cost.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_payload = _load_sim_outcome(sim_outcome_id=sim_outcome_id, output_dir=sim_outcome_dir)
    outcome_summary = _mapping(outcome_payload.get("simulated_variant_summary"))
    rows = _records(outcome_summary.get("summary"))
    source_row = _variant_row(rows, source_variant)
    baseline_row = _variant_row(rows, "no_trade")
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    materialized_metrics = _materialized_candidate_metrics(
        candidate=candidate,
        source_variant=source_variant,
        source_row=source_row,
        baseline_row=baseline_row,
        outcome_payload=outcome_payload,
        generated_at=generated,
        as_of=effective_as_of,
    )
    metric_statuses = _metric_statuses(materialized_metrics)
    pre_status = _materialization_status_from_metrics(metric_statuses)
    materialization_id = st._stable_id(
        "cost-metrics-materialization",
        candidate,
        source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        pre_status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / materialization_id)
    root.mkdir(parents=True, exist_ok=False)
    metrics_path = root / "candidate_cost_metrics.json"
    st._write_json(metrics_path, materialized_metrics)

    cost_result = cost.run_cost_sensitivity_review(
        as_of=effective_as_of,
        candidate_metrics_path=metrics_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_id=paper_shadow_health_id,
        paper_shadow_health_dir=paper_shadow_health_dir,
        output_dir=cost_sensitivity_output_dir,
        generated_at=generated,
    )
    cost_review = _mapping(cost_result.get("cost_sensitivity_review"))
    final_status = _materialization_status(metric_statuses, cost_review)
    blocking_reasons = _blocking_reasons(metric_statuses, cost_review)
    warnings = _warnings(materialized_metrics, cost_review, source_row, baseline_row)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_metrics_materialization_report",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "candidate_to_source_mapping": {
            "candidate": candidate,
            "source_variant": source_variant,
            "mapping_reason": (
                "governance candidate is a filtered signal candidate; numeric cost "
                "inputs are sourced from the existing paper-shadow simulation variant"
            ),
        },
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "cost_metrics_materialization_status": final_status,
        "materialized_metrics": materialized_metrics,
        "required_metric_statuses": metric_statuses,
        "candidate_metrics_path": str(metrics_path),
        "cost_sensitivity_review_id": cost_result.get("review_id"),
        "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
        "cost_sensitivity_validation_status": _mapping(
            cost_result.get("cost_sensitivity_validation")
        ).get("status"),
        "net_performance_proxy_by_scenario": _net_performance_by_scenario(cost_review),
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "candidate_metrics": {
                "artifact_id": materialized_metrics.get("metrics_id"),
                "path": str(metrics_path),
                "status": "OK"
                if pre_status != "INSUFFICIENT_COST_INPUTS"
                else "INSUFFICIENT_COST_INPUTS",
            },
            "cost_sensitivity_review": {
                "artifact_id": cost_result.get("review_id"),
                "status": cost_review.get("cost_sensitivity_status"),
                "report_path": _mapping(cost_result.get("manifest")).get(
                    "cost_sensitivity_report_path"
                ),
                "validation_status": _mapping(
                    cost_result.get("cost_sensitivity_validation")
                ).get("status"),
            },
        },
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_required_action": _next_action(final_status, cost_review),
        "limitations": [
            "metrics are materialized from existing research artifacts only",
            "source outcome mode is BACKTEST_SIMULATION and not PIT/live execution evidence",
            "candidate-to-source variant mapping is explicit and reviewable",
            "cost review rerun does not approve promotion, extended shadow, or production",
        ],
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_metrics_materialization_manifest",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "cost_metrics_materialization_status": final_status,
        "candidate_metrics_path": str(metrics_path),
        "cost_sensitivity_review_id": cost_result.get("review_id"),
        "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
        "cost_metrics_materialization_manifest_path": str(
            root / "cost_metrics_materialization_manifest.json"
        ),
        "cost_metrics_materialization_report_path": str(
            root / "cost_metrics_materialization_report.json"
        ),
        "cost_metrics_materialization_markdown_path": str(
            root / "cost_metrics_materialization_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "cost_metrics_materialization_validation.json"),
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }
    reader = render_cost_metrics_materialization_reader_brief(report)
    st._write_json(root / "cost_metrics_materialization_manifest.json", manifest)
    st._write_json(root / "cost_metrics_materialization_report.json", report)
    st._write_text(
        root / "cost_metrics_materialization_report.md",
        render_cost_metrics_materialization_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_cost_metrics_materialization",
        root.name,
        root / "cost_metrics_materialization_manifest.json",
    )
    validation = validate_cost_metrics_materialization_artifact(
        materialization_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "materialization_id": root.name,
        "materialization_dir": root,
        "manifest": manifest,
        "cost_metrics_materialization_report": report,
        "reader_brief_section": reader,
        "cost_metrics_materialization_validation": validation,
        "cost_sensitivity_result": cost_result,
    }


def cost_metrics_materialization_report_payload(
    *,
    materialization_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=materialization_id,
        latest_pointer="latest_cost_metrics_materialization",
        latest=latest,
        output_dir=output_dir,
        required_name="cost_metrics_materialization_manifest.json",
    )
    payload = {
        **st._read_json(root / "cost_metrics_materialization_manifest.json"),
        "cost_metrics_materialization_report": st._read_json(
            root / "cost_metrics_materialization_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "materialization_dir": str(root),
    }
    validation = st._read_optional_json(root / "cost_metrics_materialization_validation.json")
    if validation:
        payload["cost_metrics_materialization_validation"] = validation
    return payload


def validate_cost_metrics_materialization_artifact(
    *,
    materialization_id: str,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / materialization_id
    manifest = st._read_optional_json(root / "cost_metrics_materialization_manifest.json") or {}
    report = st._read_optional_json(root / "cost_metrics_materialization_report.json") or {}
    metrics = st._read_optional_json(root / "candidate_cost_metrics.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    status = _text(report.get("cost_metrics_materialization_status"))
    metric_statuses = _mapping(report.get("required_metric_statuses"))
    checks = st._required_file_checks(
        root,
        (
            "cost_metrics_materialization_manifest.json",
            "cost_metrics_materialization_report.json",
            "cost_metrics_materialization_report.md",
            "candidate_cost_metrics.json",
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
                "status_enum_valid",
                status in COST_METRICS_MATERIALIZATION_STATUSES,
                status,
            ),
            st._check(
                "candidate_metrics_safe",
                st._payload_safe(metrics),
                "",
            ),
            st._check(
                "required_metric_statuses_visible",
                {
                    "turnover",
                    "gross_performance_proxy",
                    "baseline_performance_proxy",
                    "gross_improvement_proxy",
                    "drawdown_proxy",
                    "trade_rotation_count",
                }.issubset(set(metric_statuses)),
                ",".join(sorted(metric_statuses)),
            ),
            st._check(
                "available_status_has_cost_review_inputs",
                status != "COST_INPUTS_AVAILABLE"
                or all(
                    _text(metric_statuses.get(key)) == "AVAILABLE"
                    for key in (
                        "turnover",
                        "gross_performance_proxy",
                        "gross_improvement_proxy",
                    )
                ),
                status,
            ),
            st._check(
                "cost_review_rerun_visible",
                bool(_text(report.get("cost_sensitivity_review_id")))
                and bool(_text(report.get("cost_sensitivity_status"))),
                "",
            ),
            st._check(
                "backtest_simulation_limitation_visible",
                _text(metrics.get("outcome_mode")) == "BACKTEST_SIMULATION"
                and bool(_text(metrics.get("pit_safety_status"))),
                "",
            ),
            st._check(
                "source_mapping_visible",
                bool(_mapping(report.get("candidate_to_source_mapping")).get("source_variant")),
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "cost_metrics_materialization_status" in reader
                and "cost_sensitivity_status" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "safety_boundary_locked",
                report.get("execution_model_ready") is False
                and report.get("broker_action_allowed") is False
                and report.get("official_target_weights") is False
                and report.get("production_state_mutated") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_cost_metrics_materialization_validation",
        materialization_id,
        checks,
    )
    if write_output:
        st._write_json(root / "cost_metrics_materialization_validation.json", validation)
        st._write_text(
            root / "cost_metrics_materialization_validation.md",
            render_cost_metrics_materialization_validation_report(validation),
        )
    return validation


def render_cost_metrics_materialization_reader_brief(report: Mapping[str, Any]) -> str:
    metrics = _mapping(report.get("materialized_metrics"))
    return "\n".join(
        [
            "## Cost Metrics Materialization",
            "",
            f"- cost_metrics_materialization_id: {report.get('materialization_id')}",
            "- cost_metrics_materialization_status: "
            f"{report.get('cost_metrics_materialization_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- turnover: {metrics.get('turnover')}",
            f"- gross_performance_proxy: {metrics.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {metrics.get('gross_improvement_proxy')}",
            f"- drawdown_proxy: {metrics.get('drawdown_proxy')}",
            f"- trade_rotation_count: {metrics.get('trade_rotation_count')}",
            f"- cost_sensitivity_review_id: {report.get('cost_sensitivity_review_id')}",
            f"- cost_sensitivity_status: {report.get('cost_sensitivity_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only metrics materialization / no execution model / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_cost_metrics_materialization_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    metric_lines = [
        f"- {key}: status={value} value={_mapping(report.get('materialized_metrics')).get(key)}"
        for key, value in sorted(_mapping(report.get("required_metric_statuses")).items())
    ]
    net_lines = [
        f"- {key}: {value}"
        for key, value in sorted(_mapping(report.get("net_performance_proxy_by_scenario")).items())
    ]
    return "\n".join(
        [
            f"# Cost Metrics Materialization {manifest.get('materialization_id')}",
            "",
            "## Purpose",
            (
                "Materialize numeric candidate cost inputs from existing research "
                "artifacts and rerun cost sensitivity review."
            ),
            "",
            "## Summary",
            f"- status: {report.get('cost_metrics_materialization_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- candidate_metrics_path: {report.get('candidate_metrics_path')}",
            f"- cost_sensitivity_review_id: {report.get('cost_sensitivity_review_id')}",
            f"- cost_sensitivity_status: {report.get('cost_sensitivity_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Required Metrics",
            *metric_lines,
            "",
            "## Net Performance Proxy By Scenario",
            *net_lines,
            "",
            "## Candidate Mapping",
            f"- candidate: {_mapping(report.get('candidate_to_source_mapping')).get('candidate')}",
            "- source_variant: "
            f"{_mapping(report.get('candidate_to_source_mapping')).get('source_variant')}",
            "- mapping_reason: "
            f"{_mapping(report.get('candidate_to_source_mapping')).get('mapping_reason')}",
            "",
            "## Safety Boundary",
            "- research-only materialization",
            "- backtest simulation evidence only",
            "- no new optimization or backtest execution",
            (
                "- no execution model, broker action, order ticket, official target, "
                "or production mutation"
            ),
            "",
        ]
    )


def render_cost_metrics_materialization_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Cost Metrics Materialization Validation {validation.get('artifact_id')}",
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


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=sim_outcome_id is None,
        output_dir=output_dir,
    )


def _materialized_candidate_metrics(
    *,
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    baseline_row: Mapping[str, Any],
    outcome_payload: Mapping[str, Any],
    generated_at: datetime,
    as_of: date,
) -> dict[str, Any]:
    gross = _float_or_none(source_row.get("avg_5d_return"))
    baseline = _float_or_none(baseline_row.get("avg_5d_return"))
    improvement = _float_or_none(source_row.get("avg_relative_to_no_trade_5d"))
    if improvement is None and gross is not None and baseline is not None:
        improvement = gross - baseline
    metrics_id = st._stable_id(
        "candidate-cost-metrics",
        candidate,
        source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        generated_at.isoformat(),
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_cost_metrics",
        "metrics_id": metrics_id,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "metric_source": "backtest_sim_outcome.simulated_variant_summary",
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "outcome_mode": "BACKTEST_SIMULATION",
        "pit_safety_status": "BACKTEST_SIMULATION_NOT_PIT",
        "turnover": _round_or_none(source_row.get("avg_turnover")),
        "gross_performance_proxy": _round_or_none(gross),
        "baseline_performance_proxy": _round_or_none(baseline),
        "gross_improvement_proxy": _round_or_none(improvement),
        "drawdown_proxy": _round_or_none(source_row.get("avg_max_drawdown_20d")),
        "trade_rotation_count": _int_or_none(source_row.get("event_count")),
        "available_count": _int_or_none(source_row.get("available_count")),
        "win_rate_vs_no_trade_5d": _round_or_none(source_row.get("win_rate_vs_no_trade_5d")),
        "candidate_to_source_mapping": {
            "candidate": candidate,
            "source_variant": source_variant,
            "source_candidate_row_exists": bool(source_row),
            "baseline_variant": "no_trade",
        },
        "limitation": (
            "BACKTEST_SIMULATION metric proxy; not PIT/live execution evidence and "
            "not an approval signal"
        ),
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }


def _metric_statuses(metrics: Mapping[str, Any]) -> dict[str, str]:
    return {
        "turnover": _available(metrics.get("turnover")),
        "gross_performance_proxy": _available(metrics.get("gross_performance_proxy")),
        "baseline_performance_proxy": _available(metrics.get("baseline_performance_proxy")),
        "gross_improvement_proxy": _available(metrics.get("gross_improvement_proxy")),
        "drawdown_proxy": _available(metrics.get("drawdown_proxy")),
        "trade_rotation_count": _available(metrics.get("trade_rotation_count")),
    }


def _materialization_status_from_metrics(metric_statuses: Mapping[str, Any]) -> str:
    cost_required = ("turnover", "gross_performance_proxy", "gross_improvement_proxy")
    if any(_text(metric_statuses.get(key)) != "AVAILABLE" for key in cost_required):
        return "INSUFFICIENT_COST_INPUTS"
    if any(_text(value) != "AVAILABLE" for value in metric_statuses.values()):
        return "COST_INPUTS_PARTIAL"
    return "COST_INPUTS_AVAILABLE"


def _materialization_status(
    metric_statuses: Mapping[str, Any],
    cost_review: Mapping[str, Any],
) -> str:
    status = _materialization_status_from_metrics(metric_statuses)
    if _text(cost_review.get("cost_sensitivity_status")) == "INSUFFICIENT_COST_INPUTS":
        return "INSUFFICIENT_COST_INPUTS"
    return status


def _blocking_reasons(
    metric_statuses: Mapping[str, Any],
    cost_review: Mapping[str, Any],
) -> list[str]:
    reasons = [
        f"{key}:missing"
        for key, value in metric_statuses.items()
        if _text(value) != "AVAILABLE"
        and key in {"turnover", "gross_performance_proxy", "gross_improvement_proxy"}
    ]
    if _text(cost_review.get("cost_sensitivity_status")) == "INSUFFICIENT_COST_INPUTS":
        reasons.append("cost_sensitivity_review:insufficient_cost_inputs")
    return sorted(set(reasons))


def _warnings(
    metrics: Mapping[str, Any],
    cost_review: Mapping[str, Any],
    source_row: Mapping[str, Any],
    baseline_row: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if not source_row:
        warnings.append("source_variant_row_missing")
    if not baseline_row:
        warnings.append("baseline_no_trade_row_missing")
    if _text(metrics.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("metrics_source:backtest_simulation_not_pit")
    if _text(cost_review.get("cost_sensitivity_status")) in {
        "NOT_MEANINGFUL_UNDER_COSTS",
        "MEANINGFUL_LOW_MEDIUM_ONLY",
    }:
        warnings.append(
            f"cost_sensitivity_review:{_text(cost_review.get('cost_sensitivity_status')).lower()}"
        )
    return sorted(set(warnings))


def _next_action(status: str, cost_review: Mapping[str, Any]) -> str:
    if status == "INSUFFICIENT_COST_INPUTS":
        return "identify_existing_numeric_cost_metrics_or_keep_cost_review_insufficient"
    if _text(cost_review.get("cost_sensitivity_status")) == "NOT_MEANINGFUL_UNDER_COSTS":
        return "keep_promotion_blocked_until_candidate_net_improvement_survives_costs"
    if status == "COST_INPUTS_PARTIAL":
        return "review_partial_cost_metrics_before_promotion_board_use"
    return "use_materialized_cost_metrics_as_research_cost_review_input_only"


def _net_performance_by_scenario(cost_review: Mapping[str, Any]) -> dict[str, Any]:
    return {
        _text(row.get("scenario_id")): row.get("net_performance_proxy")
        for row in _records(cost_review.get("scenario_results"))
    }


def _source_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": _text(payload.get("sim_outcome_id")),
        "path": _text(payload.get("sim_outcome_manifest_path")),
        "status": _text(payload.get("status"), "AVAILABLE"),
        "outcome_mode": "BACKTEST_SIMULATION",
    }


def _variant_row(rows: list[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("variant")) == variant:
            return dict(row)
    return {}


def _available(value: object) -> str:
    return "AVAILABLE" if _float_or_none(value) is not None else "MISSING"


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


def _round_or_none(value: object) -> float | None:
    parsed = _float_or_none(value)
    return None if parsed is None else round(parsed, 6)


def _int_or_none(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _joined_texts(value: object, sep: str = ", ") -> str:
    return sep.join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
