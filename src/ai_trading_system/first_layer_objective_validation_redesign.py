from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    SAFETY_BOUNDARY,
    clean_for_yaml,
    load_mapping,
    mapping,
    max_price_date,
    records,
    round_float,
    strings,
    to_float,
    validate_cached_market_data,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_objective_validation_policy.yaml"
)
DEFAULT_CURRENT_STATE_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_current_state_summary.yaml"
)
DEFAULT_FAILURE_TAXONOMY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_current_state"
    / "first_layer_failure_taxonomy.json"
)
DEFAULT_BENCHMARK_CONSISTENCY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_current_state"
    / "benchmark_consistency_report.json"
)
DEFAULT_PROXY_AUDIT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_proxy_coverage_audit.yaml"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_objective_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"

OBJECTIVE_TERM_IDS = (
    "false_risk_on_cost",
    "false_risk_off_cost",
    "drawdown_warning_lead_time",
    "recovery_delay_days",
    "regime_flip_penalty",
    "benchmark_consistency_score",
    "stress_slice_minimum_requirements",
)
BLOCKED_STATE = {
    "primary_window_validated": False,
    "reopen_gate_allowed": False,
    "validation_ready": False,
    "candidate_count": 0,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def run_first_layer_objective_validation_redesign_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    current_state_summary_path: Path = DEFAULT_CURRENT_STATE_SUMMARY_PATH,
    failure_taxonomy_path: Path = DEFAULT_FAILURE_TAXONOMY_PATH,
    benchmark_consistency_path: Path = DEFAULT_BENCHMARK_CONSISTENCY_PATH,
    proxy_audit_path: Path = DEFAULT_PROXY_AUDIT_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    current_state = load_mapping(current_state_summary_path)
    failure_taxonomy = load_mapping(failure_taxonomy_path)
    benchmark_consistency = load_mapping(benchmark_consistency_path)
    proxy_audit = load_mapping(proxy_audit_path)

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=as_of_date,
        expected_price_tickers=strings(policy.get("data_quality_expected_price_tickers")),
        expected_rate_series=(),
    )
    resolved_as_of = (
        _parse_date(data_quality.get("as_of"))
        or _parse_date(current_state.get("as_of"))
        or as_of_date
        or max_price_date(prices_path)
    )

    objective_terms = _objective_terms(
        policy=policy,
        current_state=current_state,
        failure_taxonomy=failure_taxonomy,
        benchmark_consistency=benchmark_consistency,
    )
    stress_requirements = _stress_requirements(policy, current_state)
    proxy_replacement_status = _proxy_replacement_status(proxy_audit)
    summary = _summary(
        policy=policy,
        current_state=current_state,
        objective_terms=objective_terms,
        stress_requirements=stress_requirements,
        proxy_replacement_status=proxy_replacement_status,
        data_quality=data_quality,
        as_of_date=resolved_as_of,
    )
    payload = _payload(
        policy=policy,
        current_state=current_state,
        data_quality=data_quality,
        objective_terms=objective_terms,
        stress_requirements=stress_requirements,
        proxy_replacement_status=proxy_replacement_status,
        summary=summary,
        as_of_date=resolved_as_of,
    )

    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    json_path = output_root / "first_layer_objective_validation_redesign.json"
    yaml_path = inputs_root / "first_layer_objective_validation_redesign.yaml"
    doc_path = docs_root / "first_layer_objective_validation_redesign.md"
    payload["artifact_paths"] = {
        "first_layer_objective_validation_redesign_json": str(json_path),
        "first_layer_objective_validation_redesign_yaml": str(yaml_path),
        "first_layer_objective_validation_redesign_doc": str(doc_path),
    }
    write_json(json_path, payload)
    write_yaml(yaml_path, payload)
    write_markdown(doc_path, _render_report(payload))
    return clean_for_yaml(payload)


def _objective_terms(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    failure_taxonomy: Mapping[str, Any],
    benchmark_consistency: Mapping[str, Any],
) -> list[dict[str, Any]]:
    term_specs = mapping(policy.get("objective_terms"))
    failure_by_type = {
        str(row.get("failure_type")): row
        for row in records(failure_taxonomy.get("failure_taxonomy"))
    }
    signal_summary = mapping(failure_taxonomy.get("signal_summary"))
    benchmark_summary = mapping(benchmark_consistency.get("summary"))
    consistency_by_type = {
        str(row.get("failure_type")): row
        for row in records(benchmark_consistency.get("consistency_by_failure_type"))
    }
    terms: list[dict[str, Any]] = []
    for term_id in OBJECTIVE_TERM_IDS:
        spec = mapping(term_specs.get(term_id))
        terms.append(
            {
                "term_id": term_id,
                "definition": str(spec.get("definition", "")),
                "measurement_source": str(spec.get("measurement_source", "")),
                "direction": str(spec.get("direction", "")),
                "current_baseline_value": _term_baseline_value(
                    term_id=term_id,
                    spec=spec,
                    current_state=current_state,
                    failure_by_type=failure_by_type,
                    signal_summary=signal_summary,
                    benchmark_summary=benchmark_summary,
                ),
                "baseline_unit": _term_unit(term_id),
                "supporting_evidence": _term_supporting_evidence(
                    term_id=term_id,
                    spec=spec,
                    failure_by_type=failure_by_type,
                    signal_summary=signal_summary,
                    benchmark_summary=benchmark_summary,
                    consistency_by_type=consistency_by_type,
                ),
                "validation_role": str(spec.get("validation_role", "")),
                "promotion_interpretation": str(
                    spec.get(
                        "promotion_interpretation",
                        "diagnostic_contract_only_not_promotion",
                    )
                ),
            }
        )
    return terms


def _term_baseline_value(
    *,
    term_id: str,
    spec: Mapping[str, Any],
    current_state: Mapping[str, Any],
    failure_by_type: Mapping[str, Mapping[str, Any]],
    signal_summary: Mapping[str, Any],
    benchmark_summary: Mapping[str, Any],
) -> int | float | str:
    if term_id == "false_risk_on_cost":
        return int(to_float(mapping(failure_by_type.get("false_risk_on")).get("event_count")))
    if term_id == "false_risk_off_cost":
        return int(to_float(mapping(failure_by_type.get("false_risk_off")).get("event_count")))
    if term_id == "drawdown_warning_lead_time":
        return int(to_float(spec.get("minimum_warning_lead_days")))
    if term_id == "recovery_delay_days":
        return int(to_float(spec.get("maximum_recovery_delay_days")))
    if term_id == "regime_flip_penalty":
        return round_float(signal_summary.get("regime_flip_rate_per_20_observations"))
    if term_id == "benchmark_consistency_score":
        return round_float(benchmark_summary.get("benchmark_consistency_score"))
    if term_id == "stress_slice_minimum_requirements":
        return _stress_baseline_summary(current_state)
    return ""


def _term_supporting_evidence(
    *,
    term_id: str,
    spec: Mapping[str, Any],
    failure_by_type: Mapping[str, Mapping[str, Any]],
    signal_summary: Mapping[str, Any],
    benchmark_summary: Mapping[str, Any],
    consistency_by_type: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if term_id == "false_risk_on_cost":
        row = mapping(failure_by_type.get("false_risk_on"))
        return {
            "failure_event_count": int(to_float(row.get("event_count"))),
            "benchmark_event_counts": mapping(row.get("benchmark_event_counts")),
            "benchmark_consistency": mapping(consistency_by_type.get("false_risk_on")),
        }
    if term_id == "false_risk_off_cost":
        row = mapping(failure_by_type.get("false_risk_off"))
        return {
            "failure_event_count": int(to_float(row.get("event_count"))),
            "benchmark_event_counts": mapping(row.get("benchmark_event_counts")),
            "benchmark_consistency": mapping(consistency_by_type.get("false_risk_off")),
        }
    if term_id == "drawdown_warning_lead_time":
        row = mapping(failure_by_type.get("late_risk_off"))
        return {
            "minimum_warning_lead_days": int(to_float(spec.get("minimum_warning_lead_days"))),
            "late_risk_off_event_count": int(to_float(row.get("event_count"))),
            "benchmark_event_counts": mapping(row.get("benchmark_event_counts")),
        }
    if term_id == "recovery_delay_days":
        row = mapping(failure_by_type.get("late_risk_on"))
        return {
            "maximum_recovery_delay_days": int(to_float(spec.get("maximum_recovery_delay_days"))),
            "late_risk_on_event_count": int(to_float(row.get("event_count"))),
            "benchmark_event_counts": mapping(row.get("benchmark_event_counts")),
        }
    if term_id == "regime_flip_penalty":
        return {
            "regime_flip_count": int(to_float(signal_summary.get("regime_flip_count"))),
            "regime_flip_rate_per_20_observations": round_float(
                signal_summary.get("regime_flip_rate_per_20_observations")
            ),
        }
    if term_id == "benchmark_consistency_score":
        return {
            "benchmark_consistency_score": round_float(
                benchmark_summary.get("benchmark_consistency_score")
            ),
            "benchmark_consistency_status": benchmark_summary.get(
                "benchmark_consistency_status", ""
            ),
            "core_benchmarks_available": strings(
                benchmark_summary.get("core_benchmarks_available")
            ),
            "optional_benchmarks_missing": strings(
                benchmark_summary.get("optional_benchmarks_missing")
            ),
        }
    if term_id == "stress_slice_minimum_requirements":
        return {"requirements": "see stress_slice_minimum_requirements section"}
    return {}


def _stress_requirements(
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
) -> dict[str, Any]:
    req = mapping(policy.get("stress_slice_minimum_requirements"))
    required_ids = strings(req.get("required_slice_ids"))
    primary_ai_ids = set(strings(req.get("primary_ai_conclusion_slice_ids")))
    minimum_observations = int(to_float(req.get("minimum_signal_observations")))
    required_status = str(req.get("required_coverage_status", "SIGNAL_COVERED"))
    slices = {str(row.get("slice_id")): row for row in records(current_state.get("regime_slices"))}
    rows: list[dict[str, Any]] = []
    for slice_id in required_ids:
        row = mapping(slices.get(slice_id))
        observations = int(to_float(row.get("signal_observation_count")))
        coverage_status = str(row.get("coverage_status", "MISSING_SLICE"))
        requirement_met = (
            coverage_status == required_status and observations >= minimum_observations
        )
        rows.append(
            {
                "slice_id": slice_id,
                "label": str(row.get("label", slice_id)),
                "role": str(row.get("role", "")),
                "coverage_status": coverage_status,
                "signal_observation_count": observations,
                "minimum_signal_observations": minimum_observations,
                "required_coverage_status": required_status,
                "requirement_met": requirement_met,
                "primary_ai_conclusion_slice": slice_id in primary_ai_ids,
            }
        )
    blocked_ids = [row["slice_id"] for row in rows if not row["requirement_met"]]
    return {
        "minimum_signal_observations": minimum_observations,
        "required_coverage_status": required_status,
        "required_slice_count": len(rows),
        "met_slice_count": len(rows) - len(blocked_ids),
        "blocked_slice_ids": blocked_ids,
        "all_requirements_met": not blocked_ids,
        "stress_validation_allowed": not blocked_ids,
        "stress_validation_requires_2022_signal_coverage": bool(
            req.get("stress_validation_requires_2022_signal_coverage", True)
        ),
        "rows": rows,
    }


def _proxy_replacement_status(proxy_audit: Mapping[str, Any]) -> dict[str, Any]:
    summary = mapping(proxy_audit.get("summary"))
    rows = records(proxy_audit.get("rows"))
    replacement_count = int(to_float(summary.get("replacement_for_true_breadth_count")))
    true_breadth_replaced = bool(summary.get("true_breadth_replaced")) or replacement_count > 0
    blocked_proxy_ids = [
        str(row.get("proxy_id"))
        for row in rows
        if not bool(row.get("replacement_for_true_breadth"))
    ]
    return {
        "proxy_audit_status": proxy_audit.get("status", ""),
        "proxy_count": int(to_float(summary.get("proxy_count"), len(rows))),
        "replacement_for_true_breadth_count": replacement_count,
        "true_breadth_replaced": true_breadth_replaced,
        "validation_blocker": ""
        if true_breadth_replaced
        else "TRUE_BREADTH_NOT_REPLACED_BY_FREE_OR_LOW_COST_PROXY",
        "blocked_proxy_ids": blocked_proxy_ids,
    }


def _summary(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    objective_terms: Sequence[Mapping[str, Any]],
    stress_requirements: Mapping[str, Any],
    proxy_replacement_status: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    current_summary = mapping(current_state.get("summary"))
    blockers: list[str] = []
    if not bool(stress_requirements.get("all_requirements_met")):
        blockers.append("STRESS_SLICE_MINIMUM_REQUIREMENTS_NOT_MET")
    if "2022_bear_rate_shock" in strings(stress_requirements.get("blocked_slice_ids")):
        blockers.append("2022_STRESS_SLICE_NO_SIGNAL_COVERAGE")
    if not bool(proxy_replacement_status.get("true_breadth_replaced")):
        blockers.append("TRUE_BREADTH_NOT_REPLACED")
    blockers.append("OWNER_REVIEW_REQUIRED_BEFORE_CHALLENGER_PROMOTION")
    return {
        "final_status": "FIRST_LAYER_OBJECTIVE_VALIDATION_CONTRACT_READY_PROMOTION_BLOCKED",
        "market_regime": MARKET_REGIME,
        "requested_start": str(requested.get("start", DEFAULT_BACKTEST_START)),
        "requested_end": str(requested.get("end", "latest")),
        "as_of": as_of_date.isoformat(),
        "actual_signal_start": _first_nonempty(
            current_state.get("actual_signal_start"),
            current_summary.get("actual_signal_start"),
        ),
        "actual_signal_end": _first_nonempty(
            current_state.get("actual_signal_end"),
            current_summary.get("actual_signal_end"),
        ),
        "data_quality_status": data_quality.get("status"),
        "objective_term_count": len(objective_terms),
        "required_objective_term_count": len(OBJECTIVE_TERM_IDS),
        "all_objective_terms_defined": len(objective_terms) == len(OBJECTIVE_TERM_IDS),
        "stress_slice_requirements_met": bool(stress_requirements.get("all_requirements_met")),
        "stress_validation_allowed": bool(stress_requirements.get("stress_validation_allowed")),
        "true_breadth_replaced": bool(proxy_replacement_status.get("true_breadth_replaced")),
        "validation_ready": False,
        "candidate_count": 0,
        "promotion_allowed": False,
        "blocking_conditions": blockers,
        "next_required_tasks": ["TRADING-2273"],
    }


def _payload(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    objective_terms: Sequence[Mapping[str, Any]],
    stress_requirements: Mapping[str, Any],
    proxy_replacement_status: Mapping[str, Any],
    summary: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    current_summary = mapping(current_state.get("summary"))
    return {
        "schema_version": "first_layer_objective_validation_redesign.v1",
        "report_type": "first_layer_objective_validation_redesign",
        "title": "First-Layer Objective And Validation Redesign",
        "status": "FIRST_LAYER_OBJECTIVE_VALIDATION_REDESIGN_READY_PROMOTION_BLOCKED",
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "requested_start": str(requested.get("start", DEFAULT_BACKTEST_START)),
        "requested_end": str(requested.get("end", "latest")),
        "as_of": as_of_date.isoformat(),
        "actual_signal_start": _first_nonempty(
            current_state.get("actual_signal_start"),
            current_summary.get("actual_signal_start"),
        ),
        "actual_signal_end": _first_nonempty(
            current_state.get("actual_signal_end"),
            current_summary.get("actual_signal_end"),
        ),
        "data_quality_status": data_quality.get("status"),
        "data_quality": data_quality,
        "policy_id": str(policy.get("policy_id", "")),
        "summary": clean_for_yaml(dict(summary)),
        "objective_terms": clean_for_yaml(list(objective_terms)),
        "stress_slice_minimum_requirements": clean_for_yaml(dict(stress_requirements)),
        "proxy_replacement_status": clean_for_yaml(dict(proxy_replacement_status)),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "first_layer_objective_validation_redesign",
            "model_version": "first_layer_objective_validation_redesign_v1",
            "threshold_policy": str(policy.get("policy_id", "")),
            "candidate_count": 0,
            "boundary_contract_version": "first_layer_objective_validation_research_only_v1",
        },
        **SAFETY_BOUNDARY,
        **BLOCKED_STATE,
    }


def _render_report(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    stress = mapping(payload.get("stress_slice_minimum_requirements"))
    proxy = mapping(payload.get("proxy_replacement_status"))
    lines = [
        "# First-layer objective validation redesign",
        "",
        f"- status: `{payload.get('status')}`",
        f"- market_regime: `{payload.get('market_regime')}`",
        (
            f"- requested_date_range: `{payload.get('requested_start')}` "
            f"to `{payload.get('requested_end')}`"
        ),
        (
            f"- actual_signal_range: `{payload.get('actual_signal_start')}` "
            f"to `{payload.get('actual_signal_end')}`"
        ),
        f"- data_quality_status: `{payload.get('data_quality_status')}`",
        (
            "- safety: `validation_ready=false`, `promotion_allowed=false`, "
            "`paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`"
        ),
        "",
        "## 结论",
        "",
        "first-layer objective 已改写为可审计的 validation contract，但当前只能用于 "
        "TRADING-2273 challenger experiment 设计输入。2022 stress slice 没有 signal coverage，"
        "free / low-cost proxy 也没有替代 true breadth，因此不能宣称 validation-ready "
        "或恢复任何 gate。",
        "",
        "## Objective terms",
        "",
        "|term_id|direction|current_baseline_value|validation_role|promotion_interpretation|",
        "|---|---|---:|---|---|",
    ]
    for row in records(payload.get("objective_terms")):
        lines.append(
            f"|`{row.get('term_id')}`|{row.get('direction')}|"
            f"`{row.get('current_baseline_value')}`|{row.get('validation_role')}|"
            f"{row.get('promotion_interpretation')}|"
        )
    lines.extend(
        [
            "",
            "## Stress slice requirements",
            "",
            "|slice|coverage_status|signal_obs|min_obs|requirement_met|",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in records(stress.get("rows")):
        lines.append(
            f"|`{row.get('slice_id')}`|`{row.get('coverage_status')}`|"
            f"{row.get('signal_observation_count')}|{row.get('minimum_signal_observations')}|"
            f"{row.get('requirement_met')}|"
        )
    lines.extend(
        [
            "",
            "## Validation blockers",
            "",
            f"- stress_validation_allowed: `{stress.get('stress_validation_allowed')}`",
            f"- blocked_slice_ids: `{','.join(strings(stress.get('blocked_slice_ids')))}`",
            f"- true_breadth_replaced: `{proxy.get('true_breadth_replaced')}`",
            f"- proxy_validation_blocker: `{proxy.get('validation_blocker')}`",
            f"- blocking_conditions: `{','.join(strings(summary.get('blocking_conditions')))}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _stress_baseline_summary(current_state: Mapping[str, Any]) -> str:
    rows = records(current_state.get("regime_slices"))
    covered = sum(1 for row in rows if row.get("coverage_status") == "SIGNAL_COVERED")
    return f"{covered}/{len(rows)} slices covered"


def _term_unit(term_id: str) -> str:
    units = {
        "false_risk_on_cost": "event_count",
        "false_risk_off_cost": "event_count",
        "drawdown_warning_lead_time": "signal_days",
        "recovery_delay_days": "signal_days",
        "regime_flip_penalty": "flips_per_20_observations",
        "benchmark_consistency_score": "score_0_to_1",
        "stress_slice_minimum_requirements": "slice_coverage_summary",
    }
    return units.get(term_id, "")


def _first_nonempty(*values: object) -> str:
    for value in values:
        text = str(value or "")
        if text:
            return text
    return ""


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None
