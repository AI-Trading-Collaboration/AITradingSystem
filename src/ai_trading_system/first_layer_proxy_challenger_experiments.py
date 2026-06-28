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
    strings,
    to_float,
    validate_cached_market_data,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_proxy_challenger_experiments_policy.yaml"
)
DEFAULT_CURRENT_STATE_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_current_state_summary.yaml"
)
DEFAULT_OBJECTIVE_VALIDATION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_objective_validation_redesign.yaml"
)
DEFAULT_PROXY_COVERAGE_AUDIT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_proxy_coverage_audit.yaml"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_proxy_challengers"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"

REQUIRED_EXPERIMENT_IDS = (
    "baseline",
    "baseline_plus_trend_structure",
    "volatility_regime",
    "risk_appetite",
    "equal_cap_weight_divergence",
    "combined_proxy",
)
BLOCKED_STATE = {
    "primary_window_validated": False,
    "reopen_gate_allowed": False,
    "candidate_count": 0,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def run_first_layer_proxy_challenger_experiments_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    current_state_summary_path: Path = DEFAULT_CURRENT_STATE_SUMMARY_PATH,
    objective_validation_path: Path = DEFAULT_OBJECTIVE_VALIDATION_PATH,
    proxy_coverage_audit_path: Path = DEFAULT_PROXY_COVERAGE_AUDIT_PATH,
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
    objective_validation = load_mapping(objective_validation_path)
    proxy_coverage = load_mapping(proxy_coverage_audit_path)

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
    rows = _experiment_rows(
        policy=policy,
        current_state=current_state,
        objective_validation=objective_validation,
        proxy_coverage=proxy_coverage,
    )
    summary = _summary(
        policy=policy,
        current_state=current_state,
        objective_validation=objective_validation,
        proxy_coverage=proxy_coverage,
        rows=rows,
        data_quality=data_quality,
        as_of_date=resolved_as_of,
    )
    payload = _payload(
        policy=policy,
        current_state=current_state,
        objective_validation=objective_validation,
        proxy_coverage=proxy_coverage,
        rows=rows,
        summary=summary,
        data_quality=data_quality,
        as_of_date=resolved_as_of,
    )

    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    json_path = output_root / "first_layer_proxy_challenger_experiments.json"
    yaml_path = inputs_root / "first_layer_proxy_challenger_experiments.yaml"
    doc_path = docs_root / "first_layer_proxy_challenger_experiments.md"
    payload["artifact_paths"] = {
        "first_layer_proxy_challenger_experiments_json": str(json_path),
        "first_layer_proxy_challenger_experiments_yaml": str(yaml_path),
        "first_layer_proxy_challenger_experiments_doc": str(doc_path),
    }
    write_json(json_path, payload)
    write_yaml(yaml_path, payload)
    write_markdown(doc_path, _render_report(payload))
    return clean_for_yaml(payload)


def _experiment_rows(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    objective_validation: Mapping[str, Any],
    proxy_coverage: Mapping[str, Any],
) -> list[dict[str, Any]]:
    validation_scope = str(
        policy.get("validation_ready_scope", "offline_challenger_experiment_only_not_promotion")
    )
    proxy_rows = {str(row.get("proxy_id")): row for row in records(proxy_coverage.get("rows"))}
    objective_terms = {
        str(row.get("term_id")): row for row in records(objective_validation.get("objective_terms"))
    }
    baseline_evidence = _baseline_evidence(current_state, objective_validation)
    rows: list[dict[str, Any]] = []
    for item in records(policy.get("experiment_definitions")):
        experiment_id = str(item.get("experiment_id"))
        required_proxy_ids = strings(item.get("required_proxy_ids"))
        target_terms = strings(item.get("target_objective_terms"))
        missing_proxy_ids = [
            proxy_id
            for proxy_id in required_proxy_ids
            if not bool(mapping(proxy_rows.get(proxy_id)).get("data_available"))
        ]
        proxy_not_true_breadth_ids = [
            proxy_id
            for proxy_id in required_proxy_ids
            if not bool(mapping(proxy_rows.get(proxy_id)).get("replacement_for_true_breadth"))
        ]
        missing_objective_terms = [
            term_id for term_id in target_terms if term_id not in objective_terms
        ]
        validation_ready = not missing_proxy_ids and not missing_objective_terms
        rows.append(
            {
                "experiment_id": experiment_id,
                "label": str(item.get("label", experiment_id)),
                "required_proxy_ids": required_proxy_ids,
                "target_objective_terms": target_terms,
                "expected_signal_role": str(item.get("expected_signal_role", "")),
                "data_available": not missing_proxy_ids,
                "missing_proxy_ids": missing_proxy_ids,
                "missing_objective_terms": missing_objective_terms,
                "validation_ready": validation_ready,
                "validation_ready_scope": validation_scope,
                "validation_readiness_reason": _readiness_reason(
                    missing_proxy_ids,
                    missing_objective_terms,
                ),
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "promotion_blockers": _promotion_blockers(
                    proxy_not_true_breadth_ids,
                    objective_validation,
                ),
                "proxy_not_true_breadth_ids": proxy_not_true_breadth_ids,
                "baseline_evidence": baseline_evidence,
            }
        )
    return rows


def _baseline_evidence(
    current_state: Mapping[str, Any],
    objective_validation: Mapping[str, Any],
) -> dict[str, Any]:
    objective_terms = {
        str(row.get("term_id")): row for row in records(objective_validation.get("objective_terms"))
    }
    summary = mapping(current_state.get("summary"))
    return {
        "failure_event_count": int(to_float(summary.get("failure_event_count"))),
        "benchmark_consistency_score": mapping(
            objective_terms.get("benchmark_consistency_score")
        ).get("current_baseline_value"),
        "false_risk_on_cost": mapping(objective_terms.get("false_risk_on_cost")).get(
            "current_baseline_value"
        ),
        "false_risk_off_cost": mapping(objective_terms.get("false_risk_off_cost")).get(
            "current_baseline_value"
        ),
        "regime_flip_penalty": mapping(objective_terms.get("regime_flip_penalty")).get(
            "current_baseline_value"
        ),
        "stress_slice_coverage": mapping(
            objective_terms.get("stress_slice_minimum_requirements")
        ).get("current_baseline_value"),
    }


def _promotion_blockers(
    proxy_not_true_breadth_ids: Sequence[str],
    objective_validation: Mapping[str, Any],
) -> list[str]:
    blockers = [
        "ROW_VALIDATION_READY_IS_OFFLINE_EXPERIMENT_ONLY",
        "PROMOTION_REQUIRES_OWNER_REVIEW_AND_SEPARATE_GATE",
    ]
    objective_summary = mapping(objective_validation.get("summary"))
    if not bool(objective_summary.get("stress_validation_allowed")):
        blockers.append("STRESS_VALIDATION_BLOCKED_BY_2022_SIGNAL_COVERAGE")
    if proxy_not_true_breadth_ids:
        blockers.append("PROXIES_ARE_NOT_TRUE_PIT_BREADTH")
    return blockers


def _summary(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    objective_validation: Mapping[str, Any],
    proxy_coverage: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    objective_summary = mapping(objective_validation.get("summary"))
    proxy_summary = mapping(proxy_coverage.get("summary"))
    validation_ready_count = sum(1 for row in rows if row.get("validation_ready"))
    promotion_allowed_count = sum(1 for row in rows if row.get("promotion_allowed"))
    missing_experiments = [
        str(row.get("experiment_id"))
        for row in rows
        if strings(row.get("missing_proxy_ids")) or strings(row.get("missing_objective_terms"))
    ]
    return {
        "final_status": "FIRST_LAYER_PROXY_CHALLENGER_EXPERIMENTS_READY_PROMOTION_BLOCKED",
        "market_regime": MARKET_REGIME,
        "requested_start": str(requested.get("start", DEFAULT_BACKTEST_START)),
        "requested_end": str(requested.get("end", "latest")),
        "as_of": as_of_date.isoformat(),
        "actual_signal_start": _first_nonempty(
            current_state.get("actual_signal_start"),
            mapping(current_state.get("summary")).get("actual_signal_start"),
        ),
        "actual_signal_end": _first_nonempty(
            current_state.get("actual_signal_end"),
            mapping(current_state.get("summary")).get("actual_signal_end"),
        ),
        "data_quality_status": data_quality.get("status"),
        "experiment_count": len(rows),
        "required_experiment_count": len(REQUIRED_EXPERIMENT_IDS),
        "validation_ready_count": validation_ready_count,
        "promotion_allowed_count": promotion_allowed_count,
        "missing_input_experiments": missing_experiments,
        "objective_contract_status": objective_validation.get("status", ""),
        "stress_validation_allowed": bool(objective_summary.get("stress_validation_allowed")),
        "true_breadth_replaced": bool(proxy_summary.get("true_breadth_replaced")),
        "validation_ready_does_not_imply_promotion_allowed": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "next_required_tasks": ["owner_review_or_future_true_breadth_data"],
    }


def _payload(
    *,
    policy: Mapping[str, Any],
    current_state: Mapping[str, Any],
    objective_validation: Mapping[str, Any],
    proxy_coverage: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    current_summary = mapping(current_state.get("summary"))
    return {
        "schema_version": "first_layer_proxy_challenger_experiments.v1",
        "report_type": "first_layer_proxy_challenger_experiments",
        "title": "First-Layer Proxy Challenger Experiments",
        "status": "FIRST_LAYER_PROXY_CHALLENGER_EXPERIMENTS_READY_PROMOTION_BLOCKED",
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
        "experiments": clean_for_yaml(list(rows)),
        "objective_validation_status": objective_validation.get("status", ""),
        "proxy_coverage_status": proxy_coverage.get("status", ""),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "first_layer_proxy_challenger_experiments",
            "model_version": "first_layer_proxy_challenger_experiments_v1",
            "threshold_policy": str(policy.get("policy_id", "")),
            "candidate_count": 0,
            "boundary_contract_version": "first_layer_proxy_challenger_research_only_v1",
        },
        **SAFETY_BOUNDARY,
        "validation_ready": False,
        **BLOCKED_STATE,
    }


def _render_report(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# First-layer proxy challenger experiments",
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
            "- safety: row-level `validation_ready` is offline-only; "
            "`promotion_allowed=false`, `paper_shadow_allowed=false`, "
            "`production_allowed=false`, `broker_action=none`"
        ),
        "",
        "## 结论",
        "",
        "Challenger matrix 已生成，但 validation_ready 只表示 offline experiment readiness。"
        "RSP / QQQE 缺失会阻塞 equal/cap-weight divergence 与 combined proxy；所有 rows "
        "仍不能进入 promotion、paper-shadow、production 或 broker。",
        "",
        "## Experiments",
        "",
        "|experiment|validation_ready|missing_proxy_ids|promotion_allowed|scope|",
        "|---|---:|---|---:|---|",
    ]
    for row in records(payload.get("experiments")):
        lines.append(
            f"|`{row.get('experiment_id')}`|{row.get('validation_ready')}|"
            f"`{','.join(strings(row.get('missing_proxy_ids')))}`|"
            f"{row.get('promotion_allowed')}|{row.get('validation_ready_scope')}|"
        )
    lines.extend(
        [
            "",
            "## Audit notes",
            "",
            f"- experiment_count: `{summary.get('experiment_count')}`",
            f"- validation_ready_count: `{summary.get('validation_ready_count')}`",
            f"- promotion_allowed_count: `{summary.get('promotion_allowed_count')}`",
            f"- true_breadth_replaced: `{summary.get('true_breadth_replaced')}`",
            f"- stress_validation_allowed: `{summary.get('stress_validation_allowed')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _readiness_reason(
    missing_proxy_ids: Sequence[str],
    missing_objective_terms: Sequence[str],
) -> str:
    if missing_proxy_ids:
        return "blocked_by_missing_proxy_inputs"
    if missing_objective_terms:
        return "blocked_by_missing_objective_terms"
    return "offline_validation_ready_not_promotion_ready"


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
