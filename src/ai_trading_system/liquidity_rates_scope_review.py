from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_VALIDATION_ROOT,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    STATUS_CONTINUE_RESEARCH as SOURCE_STATUS_CONTINUE_RESEARCH,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    STATUS_INCONCLUSIVE as SOURCE_STATUS_INCONCLUSIVE,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    round_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2314_LIQUIDITY_RATES_SCOPE_REVIEW"
SOURCE_TASK_ID = "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION"
REPORT_TYPE = "liquidity_rates_scope_review"
ARTIFACT_ROLE = "liquidity_rates_scope_review"
MODE = "scope_review"

STATUS_READY_RESEARCH_ONLY = "LIQUIDITY_RATES_SCOPE_REVIEW_READY_RESEARCH_ONLY"
STATUS_DIAGNOSTIC_ONLY = "LIQUIDITY_RATES_SCOPE_REVIEW_DIAGNOSTIC_ONLY"
STATUS_REJECT_RECOMMENDED = "LIQUIDITY_RATES_SCOPE_REVIEW_REJECT_RECOMMENDED"
ALLOWED_STATUSES = {
    STATUS_READY_RESEARCH_ONLY,
    STATUS_DIAGNOSTIC_ONLY,
    STATUS_REJECT_RECOMMENDED,
}
ALLOWED_SOURCE_STATUSES = {
    SOURCE_STATUS_CONTINUE_RESEARCH,
    SOURCE_STATUS_INCONCLUSIVE,
}

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "liquidity_rates_scope_review_policy.yaml"
)

DECISION_KEEP = "KEEP_RESEARCH_SCOPE"
DECISION_DIAGNOSTIC = "DIAGNOSTIC_ONLY"
DECISION_REJECT = "REJECT_CURRENT_SCOPE"
DECISION_SAMPLE_BLOCKED = "SAMPLE_BLOCKED"
DECISION_SOURCE_BLOCKED = "SOURCE_BLOCKED"

USE_CASES = (
    "risk_cap_modifier",
    "no_add_gate",
    "max_exposure_limiter",
    "diagnostic_only",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "actual_path_validation_consumed": True,
    "scope_review_executed": True,
    "forward_observe_started": False,
    "owner_approval_required_before_forward_observe": True,
    "partial_rates_only_scope_review": True,
    "liquidity_headwind_scope_review_executed": False,
    "full_liquidity_pressure_scope_ready": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class LiquidityRatesScopeReviewError(ValueError):
    pass


def run_liquidity_rates_scope_review(
    *,
    validation_dir: Path = DEFAULT_VALIDATION_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise LiquidityRatesScopeReviewError(
            "liquidity / rates scope review only supports scope_review mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = _load_source_validation(validation_dir, policy)
    source_summary = source["summary"]
    resolved_quality_as_of = _resolve_date(
        quality_as_of,
        default=_date_from_text(str(mapping(source_summary.get("data_quality")).get("as_of")))
        or _date_from_text(str(source_summary.get("requested_end_date")))
        or date.fromisoformat(DEFAULT_BACKTEST_START),
    )
    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_symbols=_required_price_symbols(policy),
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise LiquidityRatesScopeReviewError(
            f"TRADING-2314 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )
    eligible_rows = [
        row
        for row in source["outcome_rows"]
        if _bool_value(row.get("validation_eligible"))
    ]
    if not eligible_rows:
        raise LiquidityRatesScopeReviewError(
            "TRADING-2313 outcome matrix has no eligible rows"
        )
    candidate_rows = _candidate_scope_rows(
        source_candidate_rows=source["candidate_rows"],
        policy=policy,
    )
    horizon_rows = _horizon_scope_rows(
        source_horizon_rows=source["horizon_rows"],
        policy=policy,
    )
    use_case_rows = _use_case_scope_rows(
        rows=eligible_rows,
        policy=policy,
    )
    recommended_scope = _recommended_scope(
        source_summary=source_summary,
        candidate_rows=candidate_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
        objective_rows=source["objective_rows"],
        policy=policy,
    )
    status = _family_status(
        source_summary=source_summary,
        recommended_scope=recommended_scope,
        candidate_rows=candidate_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    summary = _summary_payload(
        status=status,
        generated_at=generated_at,
        validation_dir=validation_dir,
        policy_path=policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        source_summary=source_summary,
        source_objective_rows=source["objective_rows"],
        eligible_rows=eligible_rows,
        candidate_rows=candidate_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
        recommended_scope=recommended_scope,
    )
    common = _common_payload(summary=summary, generated_at=generated_at, mode=mode)
    paths = _write_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        candidate_rows=candidate_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
        recommended_scope=recommended_scope,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "candidate_scope_rows": candidate_rows,
            "horizon_scope_rows": horizon_rows,
            "use_case_scope_rows": use_case_rows,
            "recommended_scope": recommended_scope,
        }
    )


def _candidate_scope_rows(
    *,
    source_candidate_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    expected = _strings(mapping(policy.get("scope_options")).get("candidate_scopes"))
    source_by_id = {
        str(row.get("candidate_id")): row
        for row in source_candidate_rows
        if str(row.get("candidate_id")).strip()
    }
    result: list[dict[str, Any]] = []
    for candidate_id in expected:
        source = source_by_id.get(candidate_id, {})
        eligible_count = _int_value(source.get("validation_eligible_record_count"))
        average_score = _optional_float(source.get("average_alignment_score"))
        source_status = str(source.get("candidate_validation_status") or "")
        decision = _scope_decision(
            record_count=eligible_count,
            average_score=average_score,
            minimum_records=_policy_int(policy, "minimum_candidate_records"),
            keep_threshold=_policy_float(policy, "candidate_keep_alignment_score"),
            reject_threshold=_policy_float(policy, "candidate_reject_alignment_score"),
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "candidate",
                    "scope_id": candidate_id,
                    "candidate_id": candidate_id,
                    "source_candidate_validation_status": source_status,
                    "eligible_record_count": eligible_count,
                    "minimum_record_count": _policy_int(
                        policy, "minimum_candidate_records"
                    ),
                    "average_alignment_score": average_score,
                    "keep_alignment_score": _policy_float(
                        policy, "candidate_keep_alignment_score"
                    ),
                    "reject_alignment_score": _policy_float(
                        policy, "candidate_reject_alignment_score"
                    ),
                    "scope_decision": decision,
                    "scope_rationale": _candidate_scope_rationale(
                        candidate_id, source_status, decision
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return result


def _horizon_scope_rows(
    *,
    source_horizon_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    expected = _strings(mapping(policy.get("scope_options")).get("owner_review_horizons"))
    source_by_id = {
        str(row.get("horizon")): row
        for row in source_horizon_rows
        if str(row.get("horizon")).strip()
    }
    result: list[dict[str, Any]] = []
    for horizon in expected:
        source = source_by_id.get(horizon, {})
        eligible_count = _int_value(source.get("eligible_record_count"))
        average_score = _optional_float(source.get("average_alignment_score"))
        source_status = str(source.get("horizon_status") or "")
        decision = _scope_decision(
            record_count=eligible_count,
            average_score=average_score,
            minimum_records=_policy_int(policy, "minimum_horizon_records"),
            keep_threshold=_policy_float(policy, "horizon_keep_alignment_score"),
            reject_threshold=_policy_float(policy, "horizon_reject_alignment_score"),
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "horizon",
                    "scope_id": horizon,
                    "horizon": horizon,
                    "owner_review_requested": True,
                    "source_horizon_status": source_status,
                    "eligible_record_count": eligible_count,
                    "minimum_record_count": _policy_int(policy, "minimum_horizon_records"),
                    "average_alignment_score": average_score,
                    "keep_alignment_score": _policy_float(
                        policy, "horizon_keep_alignment_score"
                    ),
                    "reject_alignment_score": _policy_float(
                        policy, "horizon_reject_alignment_score"
                    ),
                    "scope_decision": decision,
                    "scope_rationale": _horizon_scope_rationale(horizon, decision),
                    **SAFETY_FIELDS,
                }
            )
        )
    return result


def _use_case_scope_rows(
    *,
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    definitions = mapping(mapping(policy.get("scope_options")).get("use_cases"))
    result: list[dict[str, Any]] = []
    for use_case_id in USE_CASES:
        definition = mapping(definitions.get(use_case_id))
        scope_rows = _rows_for_use_case(rows, definition)
        field_scores = _field_scores(scope_rows, definition)
        primary_score = _primary_score(definition, field_scores)
        decision = _use_case_decision(
            use_case_id=use_case_id,
            record_count=len(scope_rows),
            primary_score=primary_score,
            policy=policy,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "use_case",
                    "scope_id": use_case_id,
                    "intended_scope": definition.get("intended_scope", ""),
                    "primary_score_field": definition.get("primary_score_field", ""),
                    "eligible_record_count": len(scope_rows),
                    "minimum_record_count": _policy_int(
                        policy, "minimum_use_case_records"
                    ),
                    "field_average_scores": field_scores,
                    "average_alignment_score": primary_score,
                    "average_combined_alignment_score": _average(
                        scope_rows, "combined_alignment_score"
                    ),
                    "keep_alignment_score": _use_case_keep_threshold(policy, use_case_id),
                    "reject_alignment_score": _policy_float(
                        policy, "use_case_reject_alignment_score"
                    ),
                    "scope_decision": decision,
                    "scope_rationale": _use_case_rationale(use_case_id, decision),
                    **SAFETY_FIELDS,
                }
            )
        )
    return result


def _recommended_scope(
    *,
    source_summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    objective_pass_count = sum(
        1 for row in objective_rows if row.get("objective_status") == "PASS"
    )
    kept_candidates = _scope_ids(candidate_rows, DECISION_KEEP)
    diagnostic_candidates = _scope_ids(candidate_rows, DECISION_DIAGNOSTIC)
    kept_horizons = _scope_ids(horizon_rows, DECISION_KEEP)
    diagnostic_horizons = _scope_ids(horizon_rows, DECISION_DIAGNOSTIC)
    kept_use_cases = _scope_ids(use_case_rows, DECISION_KEEP)
    diagnostic_use_cases = _scope_ids(use_case_rows, DECISION_DIAGNOSTIC)
    rejected_use_cases = _scope_ids(use_case_rows, DECISION_REJECT)
    not_recommended = [
        *rejected_use_cases,
        "standalone_alpha",
        "paper_shadow",
        "production",
        "broker_action",
        "full_liquidity_headwind",
    ]
    if source_summary.get("status") != SOURCE_STATUS_CONTINUE_RESEARCH:
        not_recommended.append("scope_ready_research_only")
    result = _scope_review_result(
        source_status=str(source_summary.get("status") or ""),
        objective_pass_count=objective_pass_count,
        minimum_objective_pass_count=_policy_int(
            policy, "minimum_objective_pass_count_for_ready"
        ),
        kept_use_cases=kept_use_cases,
        diagnostic_use_cases=diagnostic_use_cases,
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.recommended_scope.v1",
            "task_id": TASK_ID,
            "source_task_id": SOURCE_TASK_ID,
            "source_status": source_summary.get("status"),
            "source_actual_requested_date_range": source_summary.get(
                "actual_requested_date_range"
            ),
            "objective_pass_count": objective_pass_count,
            "minimum_objective_pass_count_for_ready": _policy_int(
                policy, "minimum_objective_pass_count_for_ready"
            ),
            "recommended_candidate_ids": kept_candidates,
            "diagnostic_candidate_ids": diagnostic_candidates,
            "preferred_owner_review_horizons": kept_horizons,
            "diagnostic_owner_review_horizons": diagnostic_horizons,
            "recommended_use_cases": kept_use_cases,
            "diagnostic_use_cases": diagnostic_use_cases,
            "not_recommended_as": sorted(set(not_recommended)),
            "scope_review_result": result,
            "liquidity_headwind_scope_review_executed": False,
            "source_gap_exclusion": {
                "liquidity_headwind_proxy_v1": (
                    "blocked by TRADING-2311 / TRADING-2312 source gap; no "
                    "TRADING-2314 scope row generated"
                )
            },
            "next_task": "TRADING-2315_REGIME_STATE_MACHINE_DESIGN_AUDIT",
            **SAFETY_FIELDS,
        }
    )


def _family_status(
    *,
    source_summary: Mapping[str, Any],
    recommended_scope: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> str:
    kept_use_cases = [
        row
        for row in use_case_rows
        if row.get("scope_decision") == DECISION_KEEP
        and row.get("scope_id") != "diagnostic_only"
    ]
    diagnostic_keep = any(
        row.get("scope_id") == "diagnostic_only"
        and row.get("scope_decision") == DECISION_KEEP
        for row in use_case_rows
    )
    objective_ready = int(recommended_scope.get("objective_pass_count", 0)) >= int(
        recommended_scope.get("minimum_objective_pass_count_for_ready", 0)
    )
    if (
        source_summary.get("status") == SOURCE_STATUS_CONTINUE_RESEARCH
        and objective_ready
        and kept_use_cases
    ):
        return STATUS_READY_RESEARCH_ONLY
    if not candidate_rows or not horizon_rows or not diagnostic_keep:
        return STATUS_REJECT_RECOMMENDED
    return STATUS_DIAGNOSTIC_ONLY


def _summary_payload(
    *,
    status: str,
    generated_at: datetime,
    validation_dir: Path,
    policy_path: Path,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    source_summary: Mapping[str, Any],
    source_objective_rows: Sequence[Mapping[str, Any]],
    eligible_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "title": "Liquidity / Rates Scope Review",
            "task_id": TASK_ID,
            "source_task_id": SOURCE_TASK_ID,
            "status": status,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": source_summary.get(
                "actual_requested_date_range"
            ),
            "validation_dir": str(validation_dir),
            "policy_path": str(policy_path),
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "source_status": source_summary.get("status"),
            "source_data_quality_status": source_summary.get("data_quality_status"),
            "source_validation_eligible_record_count": source_summary.get(
                "validation_eligible_record_count"
            ),
            "source_objective_rows": list(source_objective_rows),
            "eligible_record_count": len(eligible_rows),
            "candidate_scope_count": len(candidate_rows),
            "horizon_scope_count": len(horizon_rows),
            "use_case_scope_count": len(use_case_rows),
            "candidate_scope_decisions": _decision_counts(candidate_rows),
            "horizon_scope_decisions": _decision_counts(horizon_rows),
            "use_case_scope_decisions": _decision_counts(use_case_rows),
            "recommended_candidate_ids": recommended_scope.get(
                "recommended_candidate_ids"
            ),
            "diagnostic_candidate_ids": recommended_scope.get(
                "diagnostic_candidate_ids"
            ),
            "preferred_owner_review_horizons": recommended_scope.get(
                "preferred_owner_review_horizons"
            ),
            "diagnostic_owner_review_horizons": recommended_scope.get(
                "diagnostic_owner_review_horizons"
            ),
            "recommended_use_cases": recommended_scope.get("recommended_use_cases"),
            "diagnostic_use_cases": recommended_scope.get("diagnostic_use_cases"),
            "not_recommended_as": recommended_scope.get("not_recommended_as"),
            "scope_review_result": recommended_scope.get("scope_review_result"),
            "blocked_candidate_ids": ["liquidity_headwind_proxy_v1"],
            "allowed_statuses": sorted(ALLOWED_STATUSES),
            "data_quality": _data_quality_payload(quality_report, quality_report_path),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "next_task": recommended_scope.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    summary: Mapping[str, Any],
    generated_at: datetime,
    mode: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Liquidity / Rates Scope Review",
        "task_id": TASK_ID,
        "status": summary["status"],
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        **SAFETY_FIELDS,
    }


def _write_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "liquidity_rates_scope_review_summary.json",
        "candidate_scope_json": output_dir / "liquidity_rates_candidate_scope_matrix.json",
        "candidate_scope_csv": output_dir / "liquidity_rates_candidate_scope_matrix.csv",
        "horizon_scope_json": output_dir / "liquidity_rates_horizon_scope_matrix.json",
        "horizon_scope_csv": output_dir / "liquidity_rates_horizon_scope_matrix.csv",
        "use_case_scope_json": output_dir / "liquidity_rates_use_case_scope_matrix.json",
        "use_case_scope_csv": output_dir / "liquidity_rates_use_case_scope_matrix.csv",
        "recommended_scope": output_dir / "liquidity_rates_recommended_scope.json",
        "safety_boundary": output_dir / "liquidity_rates_scope_review_safety_boundary.json",
        "report_doc": docs_root / "liquidity_rates_scope_review.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["candidate_scope_json"], {**dict(common), "rows": list(candidate_rows)})
    write_csv_rows(paths["candidate_scope_csv"], candidate_rows)
    write_json(paths["horizon_scope_json"], {**dict(common), "rows": list(horizon_rows)})
    write_csv_rows(paths["horizon_scope_csv"], horizon_rows)
    write_json(paths["use_case_scope_json"], {**dict(common), "rows": list(use_case_rows)})
    write_csv_rows(paths["use_case_scope_csv"], use_case_rows)
    write_json(
        paths["recommended_scope"],
        {**dict(common), "recommended_scope": recommended_scope},
    )
    write_json(paths["safety_boundary"], _safety_boundary(summary))
    write_markdown(
        paths["report_doc"],
        _render_report(
            summary=summary,
            candidate_rows=candidate_rows,
            horizon_rows=horizon_rows,
            use_case_rows=use_case_rows,
            recommended_scope=recommended_scope,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _safety_boundary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": summary["status"],
        "source_task_id": SOURCE_TASK_ID,
        "source_status": summary["source_status"],
        "data_quality_status": summary["data_quality_status"],
        "data_quality_report_path": summary["data_quality_report_path"],
        "does_not_modify_generator_artifacts": True,
        "does_not_modify_actual_path_validation_artifacts": True,
        "does_not_start_forward_observe": True,
        "does_not_create_liquidity_headwind_scope": True,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        "next_task": summary.get("next_task"),
        **SAFETY_FIELDS,
    }


def _render_report(
    *,
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Liquidity / Rates Scope Review",
            "",
            "TRADING-2314 对 TRADING-2313 actual-path validation evidence 执行 "
            "research-only scope review。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- scope_review_result: `{summary['scope_review_result']}`",
            "- recommended_use_cases: `{}`".format(
                ",".join(summary.get("recommended_use_cases", []))
            ),
            "- diagnostic_use_cases: `{}`".format(
                ",".join(summary.get("diagnostic_use_cases", []))
            ),
            "- preferred_owner_review_horizons: `{}`".format(
                ",".join(summary.get("preferred_owner_review_horizons", []))
            ),
            "- diagnostic_owner_review_horizons: `{}`".format(
                ",".join(summary.get("diagnostic_owner_review_horizons", []))
            ),
            "- liquidity_headwind_scope_review_executed: `False`",
            "- forward_observe_started: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "- dynamic_promotion_status: `BLOCKED`",
            "",
            "## Recommended Scope",
            "",
            f"- scope_review_result: `{recommended_scope.get('scope_review_result')}`",
            "- recommended_candidate_ids: `{}`".format(
                ",".join(recommended_scope.get("recommended_candidate_ids", []))
            ),
            "- diagnostic_candidate_ids: `{}`".format(
                ",".join(recommended_scope.get("diagnostic_candidate_ids", []))
            ),
            f"- not_recommended_as: `{','.join(recommended_scope.get('not_recommended_as', []))}`",
            "",
            "## Candidate Scope",
            "",
            *_table(
                candidate_rows,
                columns=[
                    "scope_id",
                    "eligible_record_count",
                    "average_alignment_score",
                    "scope_decision",
                ],
            ),
            "",
            "## Horizon Scope",
            "",
            *_table(
                horizon_rows,
                columns=[
                    "scope_id",
                    "eligible_record_count",
                    "average_alignment_score",
                    "scope_decision",
                ],
            ),
            "",
            "## Use-Case Scope",
            "",
            *_table(
                use_case_rows,
                columns=[
                    "scope_id",
                    "eligible_record_count",
                    "average_alignment_score",
                    "scope_decision",
                ],
            ),
            "",
            "## Safety",
            "",
            "`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 没有 "
            "TRADING-2313 validation rows，本报告不得为该 route 生成 scope row。"
            "本报告不修改 generator 或 actual-path validation artifacts，不启动 forward "
            "observe，不允许 promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )


def _table(rows: Sequence[Mapping[str, Any]], *, columns: Sequence[str]) -> list[str]:
    lines = ["|" + "|".join(columns) + "|", "|" + "|".join("---" for _ in columns) + "|"]
    for row in rows:
        lines.append("|" + "|".join(str(row.get(column, "")) for column in columns) + "|")
    return lines


def _load_source_validation(
    validation_dir: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    summary_payload = _read_json(
        validation_dir / "liquidity_rates_actual_path_validation_summary.json"
    )
    summary = mapping(summary_payload.get("summary"))
    if not summary:
        raise LiquidityRatesScopeReviewError("TRADING-2313 summary missing nested summary")
    _validate_source_summary(summary_payload, summary, policy)
    outcome_payload = _read_json(
        validation_dir / "liquidity_rates_prediction_outcome_matrix.json"
    )
    candidate_payload = _read_json(validation_dir / "liquidity_rates_candidate_scorecard.json")
    objective_payload = _read_json(
        validation_dir / "liquidity_rates_objective_coverage_matrix.json"
    )
    horizon_payload = _read_json(validation_dir / "liquidity_rates_horizon_coverage_matrix.json")
    outcome_rows = _rows_from_payload(outcome_payload, "rows")
    candidate_rows = _rows_from_payload(candidate_payload, "candidate_scorecards")
    objective_rows = _rows_from_payload(objective_payload, "objective_rows")
    horizon_rows = _rows_from_payload(horizon_payload, "horizon_rows")
    if not outcome_rows or not candidate_rows or not objective_rows or not horizon_rows:
        raise LiquidityRatesScopeReviewError("TRADING-2313 source artifacts are incomplete")
    return {
        "summary": summary,
        "outcome_rows": outcome_rows,
        "candidate_rows": candidate_rows,
        "objective_rows": objective_rows,
        "horizon_rows": horizon_rows,
    }


def _validate_source_summary(
    payload: Mapping[str, Any],
    summary: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    if summary.get("task_id") != SOURCE_TASK_ID:
        raise LiquidityRatesScopeReviewError("TRADING-2313 source task_id mismatch")
    source_boundary = mapping(policy.get("source_boundary"))
    allowed = set(_strings(source_boundary.get("allowed_source_statuses")))
    if not allowed:
        allowed = ALLOWED_SOURCE_STATUSES
    if summary.get("status") not in allowed:
        raise LiquidityRatesScopeReviewError(
            "TRADING-2314 source status is not allowed for scope review"
        )
    for field, expected in (
        ("actual_path_validation_executed", True),
        ("scope_review_ready", False),
        ("partial_rates_only_validation", True),
        ("liquidity_headwind_validation_executed", False),
        ("full_liquidity_pressure_validation_ready", False),
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("dynamic_promotion_status", "BLOCKED"),
    ):
        if summary.get(field) != expected and payload.get(field) != expected:
            raise LiquidityRatesScopeReviewError(
                f"TRADING-2313 source safety field {field} must be {expected}"
            )
    blocked = set(_strings(summary.get("blocked_candidate_ids")))
    if "liquidity_headwind_proxy_v1" not in blocked:
        raise LiquidityRatesScopeReviewError(
            "TRADING-2313 source does not preserve liquidity_headwind source gap"
        )


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    required_symbols: Sequence[str],
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    universe = load_universe()
    secondary_path = (
        marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(required_symbols),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=quality_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, quality_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def _data_quality_payload(report: DataQualityReport, report_path: Path) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "report_path": str(report_path),
    }


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LiquidityRatesScopeReviewError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise LiquidityRatesScopeReviewError(f"policy file must be object: {path}")
    return payload


def _validate_policy(policy: Mapping[str, Any]) -> None:
    required = (
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
        "data_quality",
        "source_boundary",
        "scope_options",
        "scope_thresholds",
        "status_rule",
        "safety",
    )
    missing = [field for field in required if not policy.get(field)]
    if missing:
        raise LiquidityRatesScopeReviewError(f"policy missing fields: {missing}")
    allowed = set(_strings(mapping(policy.get("status_rule")).get("allowed_statuses")))
    if allowed != ALLOWED_STATUSES:
        raise LiquidityRatesScopeReviewError(
            f"policy allowed_statuses must match {sorted(ALLOWED_STATUSES)}"
        )
    for key in (
        "minimum_candidate_records",
        "minimum_horizon_records",
        "minimum_use_case_records",
        "candidate_keep_alignment_score",
        "candidate_reject_alignment_score",
        "horizon_keep_alignment_score",
        "horizon_reject_alignment_score",
        "use_case_keep_alignment_score",
        "use_case_reject_alignment_score",
        "exposure_cap_support_score",
        "diagnostic_keep_min_records",
        "minimum_objective_pass_count_for_ready",
    ):
        _policy_float(policy, key)
    safety = mapping(policy.get("safety"))
    for field, expected in SAFETY_FIELDS.items():
        if safety.get(field) != expected:
            raise LiquidityRatesScopeReviewError(
                f"policy safety.{field} must be {expected}"
            )


def _required_price_symbols(policy: Mapping[str, Any]) -> list[str]:
    data_quality = mapping(policy.get("data_quality"))
    symbols = _strings(data_quality.get("required_price_symbols"))
    for asset in _strings(data_quality.get("outcome_assets")):
        if asset not in symbols:
            symbols.append(asset)
    return symbols


def _scope_decision(
    *,
    record_count: int,
    average_score: float | None,
    minimum_records: int,
    keep_threshold: float,
    reject_threshold: float,
) -> str:
    if record_count < minimum_records:
        return DECISION_SAMPLE_BLOCKED
    if average_score is None:
        return DECISION_SAMPLE_BLOCKED
    if average_score >= keep_threshold:
        return DECISION_KEEP
    if average_score <= reject_threshold:
        return DECISION_REJECT
    return DECISION_DIAGNOSTIC


def _use_case_decision(
    *,
    use_case_id: str,
    record_count: int,
    primary_score: float | None,
    policy: Mapping[str, Any],
) -> str:
    if use_case_id == "diagnostic_only":
        if record_count >= _policy_int(policy, "diagnostic_keep_min_records"):
            return DECISION_KEEP
        return DECISION_SAMPLE_BLOCKED
    keep_threshold = _use_case_keep_threshold(policy, use_case_id)
    return _scope_decision(
        record_count=record_count,
        average_score=primary_score,
        minimum_records=_policy_int(policy, "minimum_use_case_records"),
        keep_threshold=keep_threshold,
        reject_threshold=_policy_float(policy, "use_case_reject_alignment_score"),
    )


def _rows_for_use_case(
    rows: Sequence[Mapping[str, Any]],
    definition: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    patterns = [
        value.lower()
        for value in _strings(definition.get("signal_name_contains"))
        if value.strip()
    ]
    if not patterns:
        return list(rows)
    return [
        row
        for row in rows
        if any(pattern in str(row.get("signal_name", "")).lower() for pattern in patterns)
    ]


def _field_scores(
    rows: Sequence[Mapping[str, Any]],
    definition: Mapping[str, Any],
) -> dict[str, float | None]:
    return {
        field: _average(rows, field)
        for field in _strings(definition.get("score_fields"))
    }


def _primary_score(
    definition: Mapping[str, Any],
    field_scores: Mapping[str, float | None],
) -> float | None:
    primary = str(definition.get("primary_score_field") or "")
    if primary == "weakest_average_score":
        values = [value for value in field_scores.values() if value is not None]
        if not values:
            return None
        return round_float(min(values))
    return field_scores.get(primary)


def _use_case_keep_threshold(policy: Mapping[str, Any], use_case_id: str) -> float:
    if use_case_id in {"risk_cap_modifier", "max_exposure_limiter"}:
        return _policy_float(policy, "exposure_cap_support_score")
    return _policy_float(policy, "use_case_keep_alignment_score")


def _candidate_scope_rationale(candidate_id: str, source_status: str, decision: str) -> str:
    if decision == DECISION_KEEP:
        return f"{candidate_id} keeps a research-only scope under TRADING-2313 evidence."
    if decision == DECISION_DIAGNOSTIC:
        return f"{candidate_id} remains diagnostic because evidence is weak or mixed."
    if decision == DECISION_REJECT:
        return f"{candidate_id} is rejected in current form under scope policy."
    if not source_status:
        return f"{candidate_id} is missing from source candidate scorecard."
    return f"{candidate_id} lacks enough eligible records for scope review."


def _horizon_scope_rationale(horizon: str, decision: str) -> str:
    if decision == DECISION_KEEP:
        return f"{horizon} clears the research-only horizon keep threshold."
    if decision == DECISION_DIAGNOSTIC:
        return f"{horizon} remains diagnostic because evidence is weak or mixed."
    if decision == DECISION_REJECT:
        return f"{horizon} is rejected in current form under scope policy."
    return f"{horizon} lacks enough eligible records for scope review."


def _use_case_rationale(use_case_id: str, decision: str) -> str:
    if use_case_id == "diagnostic_only" and decision == DECISION_KEEP:
        return "Diagnostic-only scope remains usable because enough evidence rows exist."
    if decision == DECISION_KEEP:
        return f"{use_case_id} clears the research-only use-case threshold."
    if decision == DECISION_DIAGNOSTIC:
        return f"{use_case_id} remains diagnostic because evidence is weak or mixed."
    if decision == DECISION_REJECT:
        return f"{use_case_id} is not supported in the current partial rates-only form."
    return f"{use_case_id} lacks enough eligible records for scope review."


def _scope_review_result(
    *,
    source_status: str,
    objective_pass_count: int,
    minimum_objective_pass_count: int,
    kept_use_cases: Sequence[str],
    diagnostic_use_cases: Sequence[str],
) -> str:
    if (
        source_status == SOURCE_STATUS_CONTINUE_RESEARCH
        and objective_pass_count >= minimum_objective_pass_count
        and kept_use_cases
    ):
        return "PARTIAL_RATES_SCOPE_READY_RESEARCH_ONLY"
    if "diagnostic_only" in kept_use_cases or diagnostic_use_cases:
        return "DIAGNOSTIC_ONLY_WITH_LIMITED_RISK_CAP_RESEARCH_CANDIDATE"
    return "NO_LIQUIDITY_RATES_SCOPE_RECOMMENDED"


def _scope_ids(rows: Sequence[Mapping[str, Any]], decision: str) -> list[str]:
    return [
        str(row.get("scope_id"))
        for row in rows
        if row.get("scope_decision") == decision and str(row.get("scope_id")).strip()
    ]


def _decision_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        decision = str(row.get("scope_decision") or "")
        if decision:
            counts[decision] = counts.get(decision, 0) + 1
    return counts


def _average(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    values = [_optional_float(row.get(field)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round_float(sum(clean) / len(clean))


def _policy_float(policy: Mapping[str, Any], key: str) -> float:
    threshold = mapping(mapping(policy.get("scope_thresholds")).get(key))
    if "value" not in threshold:
        raise LiquidityRatesScopeReviewError(f"missing policy threshold: {key}")
    try:
        value = float(threshold.get("value"))
    except (TypeError, ValueError) as exc:
        raise LiquidityRatesScopeReviewError(f"invalid policy threshold: {key}") from exc
    if not math.isfinite(value):
        raise LiquidityRatesScopeReviewError(f"invalid policy threshold: {key}")
    return value


def _policy_int(policy: Mapping[str, Any], key: str) -> int:
    value = int(_policy_float(policy, key))
    if value <= 0:
        raise LiquidityRatesScopeReviewError(
            f"policy threshold must be positive: {key}"
        )
    return value


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LiquidityRatesScopeReviewError(f"required source artifact missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise LiquidityRatesScopeReviewError(f"{path}: expected JSON object")
    return payload


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise LiquidityRatesScopeReviewError(f"payload missing list field: {key}")
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _resolve_date(value: str | date | None, *, default: date) -> date:
    if value is None or value == "":
        return default
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise LiquidityRatesScopeReviewError(
            f"date must use YYYY-MM-DD: {value}"
        ) from exc


def _date_from_text(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _strings(value: Any) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _int_value(value: object) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "ALLOWED_STATUSES",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_VALIDATION_ROOT",
    "MODE",
    "STATUS_DIAGNOSTIC_ONLY",
    "STATUS_READY_RESEARCH_ONLY",
    "STATUS_REJECT_RECOMMENDED",
    "LiquidityRatesScopeReviewError",
    "run_liquidity_rates_scope_review",
]
