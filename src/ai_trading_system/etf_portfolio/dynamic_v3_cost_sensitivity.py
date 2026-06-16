from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_COST_SENSITIVITY_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "cost_sensitivity_review_v1.yaml"
)
DEFAULT_COST_SENSITIVITY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "cost_sensitivity_review"
)
REQUIRED_COST_SCENARIOS = ("zero", "low", "medium", "high")
COST_SENSITIVITY_STATUSES = (
    "MEANINGFUL_ALL_SCENARIOS",
    "MEANINGFUL_LOW_MEDIUM_ONLY",
    "NOT_MEANINGFUL_UNDER_COSTS",
    "INSUFFICIENT_COST_INPUTS",
    "BLOCKED_SOURCE",
)
COST_SENSITIVITY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "cost_sensitivity_review_only": True,
    "execution_model_ready": False,
    "data_downloaded_by_review": False,
    "pipelines_executed_by_review": False,
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


def load_cost_sensitivity_policy(
    config_path: Path = DEFAULT_COST_SENSITIVITY_CONFIG_PATH,
) -> dict[str, Any]:
    return _normalized_policy(st._load_yaml_mapping(config_path), config_path=config_path)


def run_cost_sensitivity_review(
    *,
    as_of: date | None = None,
    candidate_metrics_path: Path | None = None,
    candidate_metrics: Mapping[str, Any] | None = None,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: str | None = None,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    config_path: Path = DEFAULT_COST_SENSITIVITY_CONFIG_PATH,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = load_cost_sensitivity_policy(config_path)
    weekly_source = _weekly_source(
        weekly_review_id=weekly_review_id,
        output_dir=weekly_review_dir,
    )
    health_source = _health_source(
        health_id=paper_shadow_health_id,
        output_dir=paper_shadow_health_dir,
    )
    metrics_source = _candidate_metrics_source(
        metrics=candidate_metrics,
        metrics_path=candidate_metrics_path,
        weekly_source=weekly_source,
    )
    effective_as_of = (
        as_of
        or _parse_optional_date(metrics_source.get("as_of"))
        or _parse_optional_date(_mapping(weekly_source.get("summary")).get("week_end"))
        or generated.date()
    )
    scenario_results = _scenario_results(
        policy=policy,
        metrics_summary=_mapping(metrics_source.get("summary")),
    )
    blocking_reasons = _blocking_reasons(
        weekly_source=weekly_source,
        metrics_source=metrics_source,
    )
    warnings = _warnings(health_source=health_source, metrics_source=metrics_source)
    cost_status = _cost_sensitivity_status(
        blocking_reasons=blocking_reasons,
        metrics_source=metrics_source,
        scenario_results=scenario_results,
    )
    candidate = _text(
        metrics_source.get("candidate"),
        _text(_mapping(weekly_source.get("summary")).get("candidate"), "UNKNOWN"),
    )
    review_id = st._stable_id(
        "cost-sensitivity-review",
        candidate,
        effective_as_of.isoformat(),
        _text(metrics_source.get("artifact_id")),
        _text(weekly_source.get("artifact_id")),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    promotion_board_inputs = _promotion_board_inputs(
        review_id=root.name,
        candidate=candidate,
        cost_status=cost_status,
        scenario_results=scenario_results,
        weekly_source=weekly_source,
        health_source=health_source,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_review",
        "review_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "policy": policy,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "meaningful_improvement_threshold": _float(
            _mapping(policy.get("meaningful_improvement")).get("threshold"),
        ),
        "candidate_metrics_summary": metrics_source.get("summary"),
        "source_artifacts": {
            "candidate_metrics": metrics_source,
            "paper_shadow_weekly_review": weekly_source,
            "paper_shadow_health": health_source,
        },
        "cost_sensitivity_status": cost_status,
        "scenario_results": scenario_results,
        "scenario_count": len(scenario_results),
        "turnover": _mapping(metrics_source.get("summary")).get("turnover"),
        "gross_performance_proxy": _mapping(metrics_source.get("summary")).get(
            "gross_performance_proxy"
        ),
        "gross_improvement_proxy": _mapping(metrics_source.get("summary")).get(
            "gross_improvement_proxy"
        ),
        "worst_net_improvement_proxy": _worst_net_improvement(scenario_results),
        "high_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "high",
        ),
        "low_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "low",
        ),
        "medium_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "medium",
        ),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "weekly_review_input": _weekly_review_input(weekly_source),
        "promotion_board_inputs": promotion_board_inputs,
        "next_required_action": _next_required_action(cost_status),
        "limitations": [
            "research-level linear cost sensitivity only",
            "does not model live spreads, market impact, taxes, financing, or fills",
            "does not refresh market data or rerun paper-shadow source artifacts",
            "does not approve candidate promotion or production target weights",
        ],
        **COST_SENSITIVITY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_manifest",
        "review_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if cost_status != "BLOCKED_SOURCE" else "BLOCKED_SOURCE",
        "cost_sensitivity_status": cost_status,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "cost_sensitivity_manifest_path": str(root / "cost_sensitivity_manifest.json"),
        "cost_sensitivity_review_path": str(root / "cost_sensitivity_review.json"),
        "cost_sensitivity_report_path": str(root / "cost_sensitivity_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "cost_sensitivity_validation.json"),
        **COST_SENSITIVITY_SAFETY,
    }
    reader = render_cost_sensitivity_reader_brief(review)
    st._write_json(root / "cost_sensitivity_manifest.json", manifest)
    st._write_json(root / "cost_sensitivity_review.json", review)
    st._write_text(
        root / "cost_sensitivity_report.md",
        render_cost_sensitivity_report(manifest, review),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_cost_sensitivity_review",
        root.name,
        root / "cost_sensitivity_manifest.json",
    )
    validation = validate_cost_sensitivity_artifact(
        review_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "review_id": root.name,
        "review_dir": root,
        "manifest": manifest,
        "cost_sensitivity_review": review,
        "reader_brief_section": reader,
        "cost_sensitivity_validation": validation,
    }


def cost_sensitivity_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_cost_sensitivity_review",
        latest=latest,
        output_dir=output_dir,
        required_name="cost_sensitivity_manifest.json",
    )
    payload = {
        **st._read_json(root / "cost_sensitivity_manifest.json"),
        "cost_sensitivity_review": st._read_json(root / "cost_sensitivity_review.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "review_dir": str(root),
    }
    validation = st._read_optional_json(root / "cost_sensitivity_validation.json")
    if validation:
        payload["cost_sensitivity_validation"] = validation
    return payload


def validate_cost_sensitivity_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / review_id
    manifest = st._read_optional_json(root / "cost_sensitivity_manifest.json") or {}
    review = st._read_optional_json(root / "cost_sensitivity_review.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    scenario_results = _records(review.get("scenario_results"))
    scenario_ids = {_text(row.get("scenario_id")) for row in scenario_results}
    source_artifacts = _mapping(review.get("source_artifacts"))
    checks = st._required_file_checks(
        root,
        (
            "cost_sensitivity_manifest.json",
            "cost_sensitivity_review.json",
            "cost_sensitivity_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_review_id_match",
                manifest.get("review_id") == review.get("review_id") == review_id,
                "",
            ),
            st._check(
                "status_allowed",
                review.get("cost_sensitivity_status") in COST_SENSITIVITY_STATUSES,
                _text(review.get("cost_sensitivity_status")),
            ),
            st._check(
                "policy_metadata_visible",
                all(
                    _text(review.get(key))
                    for key in ("policy_id", "policy_version", "config_path")
                ),
                "",
            ),
            st._check(
                "meaningful_threshold_visible",
                _float_or_none(review.get("meaningful_improvement_threshold"))
                is not None,
                _text(review.get("meaningful_improvement_threshold")),
            ),
            st._check(
                "required_cost_scenarios_present",
                set(REQUIRED_COST_SCENARIOS).issubset(scenario_ids),
                ",".join(sorted(scenario_ids)),
            ),
            st._check(
                "scenario_outputs_complete",
                all(_scenario_output_complete(row) for row in scenario_results),
                "",
            ),
            st._check(
                "source_artifacts_visible",
                {
                    "candidate_metrics",
                    "paper_shadow_weekly_review",
                    "paper_shadow_health",
                }.issubset(set(source_artifacts)),
                ",".join(sorted(source_artifacts)),
            ),
            st._check(
                "insufficient_inputs_fail_closed",
                (
                    _mapping(source_artifacts.get("candidate_metrics")).get("status")
                    == "OK"
                    or review.get("cost_sensitivity_status")
                    == "INSUFFICIENT_COST_INPUTS"
                ),
                _text(_mapping(source_artifacts.get("candidate_metrics")).get("status")),
            ),
            st._check(
                "promotion_board_input_visible",
                bool(_mapping(review.get("promotion_board_inputs")).get("review_id")),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "cost_sensitivity_status" in reader
                and "high_cost_improvement_meaningful" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "research_only_cost_review",
                review.get("research_only") is True
                and review.get("execution_model_ready") is False
                and review.get("data_downloaded_by_review") is False
                and review.get("pipelines_executed_by_review") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, review), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_cost_sensitivity_validation",
        review_id,
        checks,
    )
    if write_output:
        st._write_json(root / "cost_sensitivity_validation.json", validation)
        st._write_text(
            root / "cost_sensitivity_validation.md",
            render_cost_sensitivity_validation_report(validation),
        )
    return validation


def latest_cost_sensitivity_summary(
    *,
    review_id: str | None = None,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> dict[str, Any]:
    try:
        payload = cost_sensitivity_report_payload(
            review_id=review_id,
            latest=review_id is None,
            output_dir=output_dir,
        )
        review = _mapping(payload.get("cost_sensitivity_review"))
        return {
            "availability": "AVAILABLE",
            "review_id": payload.get("review_id"),
            "candidate": review.get("candidate"),
            "cost_sensitivity_status": review.get("cost_sensitivity_status"),
            "high_cost_improvement_meaningful": review.get(
                "high_cost_improvement_meaningful"
            ),
            "worst_net_improvement_proxy": review.get("worst_net_improvement_proxy"),
            "policy_id": review.get("policy_id"),
            "policy_version": review.get("policy_version"),
            "validation_status": _mapping(
                payload.get("cost_sensitivity_validation")
            ).get("status", "NOT_RUN"),
            "report_path": payload.get("cost_sensitivity_report_path"),
            "next_required_action": review.get("next_required_action"),
        }
    except Exception as exc:
        return {
            "availability": "MISSING",
            "review_id": "MISSING",
            "cost_sensitivity_status": "MISSING",
            "high_cost_improvement_meaningful": "MISSING",
            "worst_net_improvement_proxy": "MISSING",
            "validation_status": "MISSING",
            "report_path": "",
            "next_required_action": "run_cost_sensitivity_review_before_promotion_board",
            "limitation": str(exc),
        }


def render_cost_sensitivity_reader_brief(review: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Cost Sensitivity Review",
            "",
            f"- cost_sensitivity_review_id: {review.get('review_id')}",
            f"- cost_sensitivity_candidate: {review.get('candidate')}",
            f"- cost_sensitivity_status: {review.get('cost_sensitivity_status')}",
            f"- policy_id: {review.get('policy_id')}",
            f"- policy_version: {review.get('policy_version')}",
            f"- turnover: {review.get('turnover')}",
            f"- gross_performance_proxy: {review.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {review.get('gross_improvement_proxy')}",
            f"- worst_net_improvement_proxy: {review.get('worst_net_improvement_proxy')}",
            "- high_cost_improvement_meaningful: "
            f"{review.get('high_cost_improvement_meaningful')}",
            f"- blocking_reasons: {_joined_texts(review.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(review.get('warnings'))}",
            f"- next_required_action: {review.get('next_required_action')}",
            "- safety_boundary: research-only cost sensitivity / no broker / "
            "no order / no official target / no production",
            "",
        ]
    )


def render_cost_sensitivity_report(
    manifest: Mapping[str, Any],
    review: Mapping[str, Any],
) -> str:
    scenario_lines = [
        (
            f"| `{row.get('scenario_id')}` | {row.get('total_cost_bps')} | "
            f"{row.get('turnover')} | {row.get('cost_drag')} | "
            f"{row.get('gross_performance_proxy')} | "
            f"{row.get('net_performance_proxy')} | "
            f"{row.get('net_improvement_proxy')} | "
            f"{row.get('improvement_remains_meaningful')} |"
        )
        for row in _records(review.get("scenario_results"))
    ]
    return "\n".join(
        [
            f"# Cost Sensitivity Review {manifest.get('review_id')}",
            "",
            "## Purpose",
            "Estimate whether paper-shadow candidate improvement survives configured "
            "transaction-cost assumptions using research-level linear cost drag.",
            "",
            "## Summary",
            f"- candidate: {review.get('candidate')}",
            f"- cost_sensitivity_status: {review.get('cost_sensitivity_status')}",
            f"- policy: {review.get('policy_id')} / {review.get('policy_version')}",
            f"- meaningful_improvement_threshold: "
            f"{review.get('meaningful_improvement_threshold')}",
            f"- turnover: {review.get('turnover')}",
            f"- gross_performance_proxy: {review.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {review.get('gross_improvement_proxy')}",
            f"- worst_net_improvement_proxy: {review.get('worst_net_improvement_proxy')}",
            "- high_cost_improvement_meaningful: "
            f"{review.get('high_cost_improvement_meaningful')}",
            f"- blocking_reasons: {_joined_texts(review.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(review.get('warnings'))}",
            f"- next_required_action: {review.get('next_required_action')}",
            "",
            "## Scenarios",
            "| scenario | cost bps | turnover | cost drag | gross perf | net perf | "
            "net improvement | meaningful |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
            *scenario_lines,
            "",
            "## Promotion Board Input",
            f"- use: {_mapping(review.get('promotion_board_inputs')).get('board_use')}",
            "- automatic_candidate_promotion: false",
            "- owner_review_required: true",
            "",
            "## Safety Boundary",
            "- research-only cost sensitivity review",
            "- no broker integration or order ticket",
            "- no paper account or production mutation",
            "- no official target weights",
            "- no data refresh or upstream rerun",
            "",
            "## Limitations",
            "- Cost drag is turnover multiplied by configured total cost bps.",
            "- These assumptions are not live execution, tax, financing, or capacity evidence.",
            "- Missing numeric candidate metrics produce INSUFFICIENT_COST_INPUTS.",
            "",
        ]
    )


def render_cost_sensitivity_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Cost Sensitivity Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def _normalized_policy(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {**COST_SENSITIVITY_SAFETY, **_mapping(config.get("safety_boundaries"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "policy_id": _text(
            config.get("policy_id"),
            "dynamic_v3_rescue_cost_sensitivity_review_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_manual_review_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "meaningful_improvement": _mapping(config.get("meaningful_improvement")),
        "scenarios": [_normalized_scenario(row) for row in _records(config.get("scenarios"))],
        "safety_boundaries": safety,
        **safety,
    }


def _normalized_scenario(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scenario_id": _text(row.get("scenario_id")),
        "label": _text(row.get("label")),
        "total_cost_bps": _float(row.get("total_cost_bps")),
        "commission_bps": _float(row.get("commission_bps")),
        "spread_bps": _float(row.get("spread_bps")),
        "slippage_bps": _float(row.get("slippage_bps")),
        "market_impact_bps": _float(row.get("market_impact_bps")),
        "rationale": _text(row.get("rationale")),
        **COST_SENSITIVITY_SAFETY,
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
            "coverage_classification": review.get("coverage_classification"),
            "cost_review_role": "source_weekly_paper_shadow_context",
        },
        payload=review,
    )


def _health_source(*, health_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = health.paper_shadow_health_report_payload(
            health_id=health_id,
            latest=health_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_health", f"paper-shadow health missing: {exc}")
    report = _mapping(payload.get("paper_shadow_health_report"))
    return _source(
        "paper_shadow_health",
        exists=True,
        artifact_id=_text(payload.get("health_id")),
        status=_text(report.get("paper_shadow_health_status"), "UNKNOWN"),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_health_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_health_manifest_path"))),
        summary={
            "health_id": payload.get("health_id"),
            "paper_shadow_health_status": report.get("paper_shadow_health_status"),
            "safe_to_continue_shadow": report.get("safe_to_continue_shadow"),
            "signal_input_status": report.get("signal_input_status"),
            "cost_review_role": "source_health_context",
        },
        payload=report,
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
        payload = _metrics_from_weekly_source(weekly_source)
        source_path = None
    summary = _normalized_metrics_summary(payload, weekly_source=weekly_source)
    return _source(
        "candidate_metrics",
        exists=True,
        artifact_id=_text(
            payload.get("metrics_id"),
            _text(payload.get("artifact_id"), "candidate_metrics_inline_or_weekly_proxy"),
        ),
        status="OK" if _metrics_complete(summary) else "INSUFFICIENT_COST_INPUTS",
        validation_status=_text(payload.get("validation_status"), "NOT_APPLICABLE"),
        source_path=source_path,
        summary=summary,
        payload={**payload, **COST_SENSITIVITY_SAFETY},
    )


def _metrics_from_weekly_source(weekly_source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(weekly_source.get("summary"))
    return {
        "metrics_id": f"weekly_proxy:{summary.get('weekly_review_id', 'MISSING')}",
        "candidate": summary.get("candidate"),
        "as_of": summary.get("week_end"),
        "turnover": None,
        "gross_performance_proxy": None,
        "baseline_performance_proxy": None,
        "gross_improvement_proxy": None,
        "metric_source": "paper_shadow_weekly_review_proxy",
        "limitation": "weekly review does not contain numeric turnover/performance metrics",
        **COST_SENSITIVITY_SAFETY,
    }


def _normalized_metrics_summary(
    payload: Mapping[str, Any],
    *,
    weekly_source: Mapping[str, Any],
) -> dict[str, Any]:
    weekly_summary = _mapping(weekly_source.get("summary"))
    gross = _first_float(
        payload,
        "gross_performance_proxy",
        "candidate_gross_performance_proxy",
        "candidate_performance_proxy",
        "candidate_return_proxy",
        "total_return",
    )
    baseline = _first_float(
        payload,
        "baseline_performance_proxy",
        "baseline_return_proxy",
        "benchmark_return_proxy",
    )
    gross_improvement = _first_float(
        payload,
        "gross_improvement_proxy",
        "candidate_improvement_proxy",
        "return_delta",
        "candidate_return_delta",
    )
    if gross_improvement is None and gross is not None and baseline is not None:
        gross_improvement = gross - baseline
    return {
        "metrics_id": _text(payload.get("metrics_id"), _text(payload.get("artifact_id"))),
        "candidate": _text(payload.get("candidate"), _text(weekly_summary.get("candidate"))),
        "as_of": _text(payload.get("as_of"), _text(weekly_summary.get("week_end"))),
        "metric_source": _text(payload.get("metric_source"), "candidate_metrics_json"),
        "turnover": _round_or_none(
            _first_float(
                payload,
                "turnover",
                "turnover_proxy",
                "candidate_turnover",
                "average_turnover",
                "avg_turnover",
            )
        ),
        "gross_performance_proxy": _round_or_none(gross),
        "baseline_performance_proxy": _round_or_none(baseline),
        "gross_improvement_proxy": _round_or_none(gross_improvement),
        "limitation": _text(payload.get("limitation")),
    }


def _scenario_results(
    *,
    policy: Mapping[str, Any],
    metrics_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    threshold = _float(_mapping(policy.get("meaningful_improvement")).get("threshold"))
    return [
        _scenario_result(row, metrics_summary=metrics_summary, threshold=threshold)
        for row in _records(policy.get("scenarios"))
    ]


def _scenario_result(
    scenario: Mapping[str, Any],
    *,
    metrics_summary: Mapping[str, Any],
    threshold: float,
) -> dict[str, Any]:
    turnover = _float_or_none(metrics_summary.get("turnover"))
    gross = _float_or_none(metrics_summary.get("gross_performance_proxy"))
    gross_improvement = _float_or_none(metrics_summary.get("gross_improvement_proxy"))
    total_cost_bps = _float(scenario.get("total_cost_bps"))
    if turnover is None or gross is None or gross_improvement is None:
        cost_drag = None
        net_performance = None
        net_improvement = None
        meaningful = False
        classification = "INSUFFICIENT_INPUTS"
    else:
        cost_drag = turnover * total_cost_bps / 10_000.0
        net_performance = gross - cost_drag
        net_improvement = gross_improvement - cost_drag
        meaningful = net_improvement >= threshold
        classification = "MEANINGFUL" if meaningful else "NOT_MEANINGFUL"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scenario_id": _text(scenario.get("scenario_id")),
        "label": _text(scenario.get("label")),
        "total_cost_bps": total_cost_bps,
        "turnover": _round_or_none(turnover),
        "cost_drag": _round_or_none(cost_drag),
        "gross_performance_proxy": _round_or_none(gross),
        "net_performance_proxy": _round_or_none(net_performance),
        "gross_improvement_proxy": _round_or_none(gross_improvement),
        "net_improvement_proxy": _round_or_none(net_improvement),
        "meaningful_improvement_threshold": threshold,
        "improvement_remains_meaningful": meaningful,
        "classification": classification,
        "cost_assumption": {
            key: scenario.get(key)
            for key in (
                "commission_bps",
                "spread_bps",
                "slippage_bps",
                "market_impact_bps",
                "rationale",
            )
        },
        **COST_SENSITIVITY_SAFETY,
    }


def _blocking_reasons(
    *,
    weekly_source: Mapping[str, Any],
    metrics_source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if weekly_source.get("exists") is not True:
        reasons.append("paper_shadow_weekly_review:missing")
    if _text(metrics_source.get("status")) == "INSUFFICIENT_COST_INPUTS":
        reasons.append("candidate_metrics:insufficient_cost_inputs")
    return _dedupe(reasons)


def _warnings(
    *,
    health_source: Mapping[str, Any],
    metrics_source: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if health_source.get("exists") is not True:
        warnings.append("paper_shadow_health:missing")
    elif _text(health_source.get("status")) not in {
        "HEALTHY",
        "HEALTHY_WITH_WARNINGS",
    }:
        warnings.append(f"paper_shadow_health:{_text(health_source.get('status')).lower()}")
    if _text(_mapping(metrics_source.get("summary")).get("limitation")):
        warnings.append("candidate_metrics:limited_source")
    return _dedupe(warnings)


def _cost_sensitivity_status(
    *,
    blocking_reasons: list[str],
    metrics_source: Mapping[str, Any],
    scenario_results: list[Mapping[str, Any]],
) -> str:
    if any(reason.endswith(":missing") for reason in blocking_reasons):
        return "BLOCKED_SOURCE"
    if _text(metrics_source.get("status")) != "OK":
        return "INSUFFICIENT_COST_INPUTS"
    meaningful = {
        _text(row.get("scenario_id")): row.get("improvement_remains_meaningful") is True
        for row in scenario_results
    }
    if all(meaningful.get(scenario_id) for scenario_id in REQUIRED_COST_SCENARIOS):
        return "MEANINGFUL_ALL_SCENARIOS"
    if meaningful.get("low") and meaningful.get("medium"):
        return "MEANINGFUL_LOW_MEDIUM_ONLY"
    return "NOT_MEANINGFUL_UNDER_COSTS"


def _promotion_board_inputs(
    *,
    review_id: str,
    candidate: str,
    cost_status: str,
    scenario_results: list[Mapping[str, Any]],
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
    blocking_reasons: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "review_id": review_id,
        "candidate": candidate,
        "board_use": "research_cost_sensitivity_input_only",
        "cost_sensitivity_status": cost_status,
        "source_weekly_review_id": _mapping(weekly_source.get("summary")).get(
            "weekly_review_id"
        ),
        "source_paper_shadow_health_id": _mapping(health_source.get("summary")).get(
            "health_id"
        ),
        "scenario_count": len(scenario_results),
        "scenario_ids": [_text(row.get("scenario_id")) for row in scenario_results],
        "high_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "high",
        ),
        "worst_net_improvement_proxy": _worst_net_improvement(scenario_results),
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
        "coverage_classification": summary.get("coverage_classification"),
        "source_status": weekly_source.get("status"),
        "source_validation_status": weekly_source.get("validation_status"),
    }


def _next_required_action(status: str) -> str:
    if status == "MEANINGFUL_ALL_SCENARIOS":
        return "include_cost_sensitivity_in_next_weekly_or_promotion_board_review"
    if status == "MEANINGFUL_LOW_MEDIUM_ONLY":
        return "owner_review_high_cost_fragility_before_promotion_board"
    if status == "NOT_MEANINGFUL_UNDER_COSTS":
        return "return_candidate_to_research_until_net_improvement_survives_costs"
    if status == "BLOCKED_SOURCE":
        return "restore_weekly_review_source_before_cost_sensitivity"
    return "provide_numeric_turnover_and_performance_metrics_before_cost_review"


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


def _scenario_output_complete(row: Mapping[str, Any]) -> bool:
    return all(
        key in row
        for key in (
            "scenario_id",
            "total_cost_bps",
            "turnover",
            "cost_drag",
            "gross_performance_proxy",
            "net_performance_proxy",
            "gross_improvement_proxy",
            "net_improvement_proxy",
            "improvement_remains_meaningful",
            "classification",
        )
    )


def _metrics_complete(summary: Mapping[str, Any]) -> bool:
    return all(
        _float_or_none(summary.get(key)) is not None
        for key in (
            "turnover",
            "gross_performance_proxy",
            "gross_improvement_proxy",
        )
    )


def _scenario_meaningful(
    scenario_results: list[Mapping[str, Any]],
    scenario_id: str,
) -> bool | str:
    for row in scenario_results:
        if _text(row.get("scenario_id")) == scenario_id:
            return row.get("improvement_remains_meaningful") is True
    return "MISSING"


def _worst_net_improvement(scenario_results: list[Mapping[str, Any]]) -> float | None:
    values = [
        value
        for row in scenario_results
        if (value := _float_or_none(row.get("net_improvement_proxy"))) is not None
    ]
    if not values:
        return None
    return _round_or_none(min(values))


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
