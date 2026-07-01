from __future__ import annotations

import csv
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.ai_leadership_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_VALIDATION_ROOT,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    STATUS_CONTINUE_RESEARCH as SOURCE_STATUS_CONTINUE_RESEARCH,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    FULL_UNIVERSE_BLOCKER,
)
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

TASK_ID = "TRADING-2310_AI_LEADERSHIP_SCOPE_REVIEW"
REPORT_TYPE = "ai_leadership_scope_review"
ARTIFACT_ROLE = "ai_leadership_scope_review"
MODE = "scope_review"
SOURCE_TASK_ID = "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION"

STATUS_READY_RESEARCH_ONLY = "AI_LEADERSHIP_SCOPE_REVIEW_READY_RESEARCH_ONLY"
STATUS_INCONCLUSIVE = "AI_LEADERSHIP_SCOPE_REVIEW_INCONCLUSIVE"
STATUS_REJECT_RECOMMENDED = "AI_LEADERSHIP_SCOPE_REVIEW_REJECT_RECOMMENDED"
ALLOWED_STATUSES = {
    STATUS_READY_RESEARCH_ONLY,
    STATUS_INCONCLUSIVE,
    STATUS_REJECT_RECOMMENDED,
}

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "ai_leadership_scope_review_policy.yaml"
)

DECISION_KEEP = "KEEP_RESEARCH_SCOPE"
DECISION_DIAGNOSTIC = "DIAGNOSTIC_ONLY"
DECISION_REJECT = "REJECT_CURRENT_SCOPE"
DECISION_SAMPLE_BLOCKED = "SAMPLE_BLOCKED"
DECISION_REFERENCE = "REFERENCE_ONLY_NOT_OWNER_SCOPE"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "actual_path_validation_consumed": True,
    "scope_review_executed": True,
    "forward_observe_started": False,
    "owner_approval_required_before_forward_observe": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


class AILeadershipScopeReviewError(ValueError):
    pass


def run_ai_leadership_scope_review(
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
        raise AILeadershipScopeReviewError(
            "AI leadership scope review only supports scope_review mode"
        )
    policy = _load_policy(policy_path)
    _validate_policy(policy)
    source = _load_source_validation(validation_dir)
    source_summary = source["summary"]
    resolved_quality_as_of = _resolve_date(
        quality_as_of,
        default=_date_from_text(
            str(mapping(source_summary.get("data_quality")).get("as_of"))
        )
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
        raise AILeadershipScopeReviewError(
            f"TRADING-2310 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )

    eligible_rows = [
        row for row in source["outcome_rows"] if _bool_value(row.get("validation_eligible"))
    ]
    if not eligible_rows:
        raise AILeadershipScopeReviewError("TRADING-2309 outcome matrix has no eligible rows")

    asset_rows = _asset_scope_rows(eligible_rows, policy)
    horizon_rows = _horizon_scope_rows(eligible_rows, policy)
    use_case_rows = _use_case_rows(eligible_rows, policy)
    recommended_scope = _recommended_scope(
        source_summary=source_summary,
        asset_rows=asset_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
        objective_rows=source["objective_rows"],
        policy=policy,
    )
    status = _family_status(
        recommended_scope=recommended_scope,
        asset_rows=asset_rows,
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
        source_candidate_rows=source["candidate_rows"],
        source_objective_rows=source["objective_rows"],
        eligible_rows=eligible_rows,
        asset_rows=asset_rows,
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
        asset_rows=asset_rows,
        horizon_rows=horizon_rows,
        use_case_rows=use_case_rows,
        recommended_scope=recommended_scope,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "asset_scope_rows": asset_rows,
            "horizon_scope_rows": horizon_rows,
            "use_case_scope_rows": use_case_rows,
            "recommended_scope": recommended_scope,
        }
    )


def _asset_scope_rows(
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    definitions = [
        (
            "qqq_only_diagnostic",
            "QQQ only diagnostic reference",
            [row for row in rows if row.get("target_asset") == "QQQ"],
            True,
        ),
        (
            "smh_only",
            "SMH only",
            [row for row in rows if row.get("target_asset") == "SMH"],
            False,
        ),
        (
            "qqq_plus_smh",
            "QQQ + SMH",
            [row for row in rows if row.get("target_asset") in {"QQQ", "SMH"}],
            False,
        ),
    ]
    result = []
    for scope_id, label, scope_rows, reference_only in definitions:
        average_score = _average(scope_rows, "combined_alignment_score")
        threshold_key = (
            "smh_only_primary_min_alignment_score"
            if scope_id == "smh_only"
            else "qqq_plus_smh_min_alignment_score"
            if scope_id == "qqq_plus_smh"
            else "asset_keep_alignment_score"
        )
        keep_threshold = _policy_float(policy, threshold_key)
        decision = _scope_decision(
            rows=scope_rows,
            average_score=average_score,
            minimum_records=_policy_int(policy, "minimum_scope_records"),
            keep_threshold=keep_threshold,
            reject_threshold=_policy_float(policy, "asset_reject_alignment_score"),
            reference_only=reference_only,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "asset",
                    "scope_id": scope_id,
                    "scope_label": label,
                    "eligible_record_count": len(scope_rows),
                    "minimum_record_count": _policy_int(policy, "minimum_scope_records"),
                    "average_alignment_score": average_score,
                    "keep_alignment_score": keep_threshold,
                    "reject_alignment_score": _policy_float(
                        policy, "asset_reject_alignment_score"
                    ),
                    "average_target_forward_return": _average(
                        scope_rows, "target_forward_return"
                    ),
                    "average_target_max_drawdown": _average(
                        scope_rows, "target_max_drawdown"
                    ),
                    "average_smh_relative_forward_return": _average(
                        scope_rows, "smh_relative_forward_return"
                    ),
                    "scope_decision": decision,
                    "scope_rationale": _asset_scope_rationale(scope_id, decision),
                    **SAFETY_FIELDS,
                }
            )
        )
    return result


def _horizon_scope_rows(
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    owner_horizons = set(
        _strings(mapping(policy.get("scope_options")).get("owner_review_horizons"))
    )
    diagnostic_horizons = set(
        _strings(mapping(policy.get("scope_options")).get("diagnostic_horizons"))
    )
    horizons = sorted(
        {str(row.get("horizon")) for row in rows if str(row.get("horizon")).strip()},
        key=_horizon_sort_key,
    )
    result = []
    for horizon in horizons:
        scope_rows = [row for row in rows if str(row.get("horizon")) == horizon]
        average_score = _average(scope_rows, "combined_alignment_score")
        reference_only = horizon in diagnostic_horizons and horizon not in owner_horizons
        decision = _scope_decision(
            rows=scope_rows,
            average_score=average_score,
            minimum_records=_policy_int(policy, "minimum_horizon_records"),
            keep_threshold=_policy_float(policy, "horizon_keep_alignment_score"),
            reject_threshold=_policy_float(policy, "horizon_reject_alignment_score"),
            reference_only=reference_only,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "horizon",
                    "scope_id": horizon,
                    "owner_review_requested": horizon in owner_horizons,
                    "diagnostic_reference_only": reference_only,
                    "eligible_record_count": len(scope_rows),
                    "minimum_record_count": _policy_int(policy, "minimum_horizon_records"),
                    "average_alignment_score": average_score,
                    "keep_alignment_score": _policy_float(
                        policy, "horizon_keep_alignment_score"
                    ),
                    "reject_alignment_score": _policy_float(
                        policy, "horizon_reject_alignment_score"
                    ),
                    "average_target_forward_return": _average(
                        scope_rows, "target_forward_return"
                    ),
                    "average_target_max_drawdown": _average(
                        scope_rows, "target_max_drawdown"
                    ),
                    "average_smh_relative_forward_return": _average(
                        scope_rows, "smh_relative_forward_return"
                    ),
                    "scope_decision": decision,
                    "scope_rationale": _horizon_scope_rationale(
                        horizon, decision, reference_only
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return result


def _use_case_rows(
    rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    use_cases = mapping(mapping(policy.get("scope_options")).get("use_cases"))
    result = []
    for use_case_id in ("confirmation_only", "exposure_cap_modifier", "standalone_alpha"):
        definition = mapping(use_cases.get(use_case_id))
        scope_rows = _rows_for_use_case(rows, use_case_id, definition)
        score_field = _use_case_primary_score_field(use_case_id)
        average_score = _average(scope_rows, score_field)
        average_combined = _average(scope_rows, "combined_alignment_score")
        keep_threshold = _use_case_keep_threshold(policy, use_case_id)
        reject_threshold = _policy_float(policy, "use_case_reject_alignment_score")
        decision = _scope_decision(
            rows=scope_rows,
            average_score=average_score,
            minimum_records=_policy_int(policy, "minimum_use_case_records"),
            keep_threshold=keep_threshold,
            reject_threshold=reject_threshold,
            reference_only=use_case_id == "standalone_alpha",
        )
        if use_case_id == "standalone_alpha" and decision == DECISION_REFERENCE:
            decision = DECISION_REJECT
        result.append(
            clean_for_yaml(
                {
                    "scope_type": "use_case",
                    "scope_id": use_case_id,
                    "intended_scope": definition.get("intended_scope", ""),
                    "primary_score_field": score_field,
                    "eligible_record_count": len(scope_rows),
                    "minimum_record_count": _policy_int(policy, "minimum_use_case_records"),
                    "average_alignment_score": average_score,
                    "average_combined_alignment_score": average_combined,
                    "keep_alignment_score": keep_threshold,
                    "reject_alignment_score": reject_threshold,
                    "average_drawdown_risk_score": _average(
                        scope_rows, "drawdown_risk_score"
                    ),
                    "average_weakening_window_score": _average(
                        scope_rows, "weakening_window_score"
                    ),
                    "average_smh_relative_return_score": _average(
                        scope_rows, "smh_relative_return_score"
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
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    objective_pass_count = sum(
        1 for row in objective_rows if row.get("objective_status") == "PASS"
    )
    qqq_plus = _row_by_id(asset_rows, "qqq_plus_smh")
    smh_only = _row_by_id(asset_rows, "smh_only")
    kept_owner_horizons = [
        str(row.get("scope_id"))
        for row in horizon_rows
        if row.get("owner_review_requested") is True
        and row.get("scope_decision") == DECISION_KEEP
    ]
    diagnostic_owner_horizons = [
        str(row.get("scope_id"))
        for row in horizon_rows
        if row.get("owner_review_requested") is True
        and row.get("scope_decision") == DECISION_DIAGNOSTIC
    ]
    kept_use_cases = [
        str(row.get("scope_id"))
        for row in use_case_rows
        if row.get("scope_decision") == DECISION_KEEP
    ]
    not_recommended = [
        "standalone_alpha",
        "paper_shadow",
        "production",
        "broker_action",
    ]
    if smh_only.get("scope_decision") != DECISION_KEEP:
        not_recommended.append("smh_only_primary_scope")
    if "20d" in diagnostic_owner_horizons:
        not_recommended.append("20d_primary_scope")
    recommended_asset_scope = (
        "QQQ_PLUS_SMH_RESEARCH_ONLY"
        if qqq_plus.get("scope_decision") == DECISION_KEEP
        else "NO_RECOMMENDED_ASSET_SCOPE"
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
            "recommended_asset_scope": recommended_asset_scope,
            "smh_only_scope_decision": smh_only.get("scope_decision"),
            "qqq_plus_smh_scope_decision": qqq_plus.get("scope_decision"),
            "preferred_owner_review_horizons": kept_owner_horizons,
            "diagnostic_owner_review_horizons": diagnostic_owner_horizons,
            "recommended_use_cases": kept_use_cases,
            "not_recommended_as": not_recommended,
            "scope_review_result": _scope_review_result(
                recommended_asset_scope=recommended_asset_scope,
                kept_owner_horizons=kept_owner_horizons,
                kept_use_cases=kept_use_cases,
            ),
            "next_task": "TRADING-2311_LIQUIDITY_RATES_PRESSURE_DATA_FEASIBILITY_AUDIT",
            "owner_approval_required_before_forward_observe": True,
            **SAFETY_FIELDS,
        }
    )


def _family_status(
    *,
    recommended_scope: Mapping[str, Any],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
) -> str:
    kept_assets = [
        row for row in asset_rows if row.get("scope_decision") == DECISION_KEEP
    ]
    kept_horizons = [
        row
        for row in horizon_rows
        if row.get("scope_decision") == DECISION_KEEP
        and row.get("owner_review_requested") is True
    ]
    kept_use_cases = [
        row
        for row in use_case_rows
        if row.get("scope_decision") == DECISION_KEEP
        and row.get("scope_id") != "standalone_alpha"
    ]
    if (
        kept_assets
        and kept_horizons
        and kept_use_cases
        and int(recommended_scope.get("objective_pass_count", 0))
        >= int(recommended_scope.get("minimum_objective_pass_count_for_ready", 0))
    ):
        return STATUS_READY_RESEARCH_ONLY
    if not kept_assets and not kept_use_cases:
        return STATUS_REJECT_RECOMMENDED
    return STATUS_INCONCLUSIVE


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
    source_candidate_rows: Sequence[Mapping[str, Any]],
    source_objective_rows: Sequence[Mapping[str, Any]],
    eligible_rows: Sequence[Mapping[str, Any]],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "title": "AI / 半导体 Leadership Scope Review",
            "task_id": TASK_ID,
            "status": status,
            "artifact_role": ARTIFACT_ROLE,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_task_id": SOURCE_TASK_ID,
            "source_validation_dir": str(validation_dir),
            "source_status": source_summary.get("status"),
            "source_actual_requested_date_range": source_summary.get(
                "actual_requested_date_range"
            ),
            "actual_requested_date_range": source_summary.get(
                "actual_requested_date_range"
            ),
            "policy_path": str(policy_path),
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "source_candidate_count": len(source_candidate_rows),
            "source_objective_count": len(source_objective_rows),
            "validation_eligible_record_count": len(eligible_rows),
            "asset_scope_row_count": len(asset_rows),
            "horizon_scope_row_count": len(horizon_rows),
            "use_case_scope_row_count": len(use_case_rows),
            "recommended_asset_scope": recommended_scope.get("recommended_asset_scope"),
            "preferred_owner_review_horizons": recommended_scope.get(
                "preferred_owner_review_horizons", []
            ),
            "diagnostic_owner_review_horizons": recommended_scope.get(
                "diagnostic_owner_review_horizons", []
            ),
            "recommended_use_cases": recommended_scope.get("recommended_use_cases", []),
            "not_recommended_as": recommended_scope.get("not_recommended_as", []),
            "scope_review_result": recommended_scope.get("scope_review_result"),
            "data_quality": _data_quality_payload(quality_report, quality_report_path),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "full_universe_validation_blocker_out_of_scope": FULL_UNIVERSE_BLOCKER,
            "allowed_statuses": sorted(ALLOWED_STATUSES),
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
        "title": "AI / 半导体 Leadership Scope Review",
        "task_id": TASK_ID,
        "status": summary["status"],
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        **SAFETY_FIELDS,
    }


def _write_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "ai_leadership_scope_review_summary.json",
        "asset_scope_json": output_dir / "ai_leadership_asset_scope_matrix.json",
        "asset_scope_csv": output_dir / "ai_leadership_asset_scope_matrix.csv",
        "horizon_scope_json": output_dir / "ai_leadership_horizon_scope_matrix.json",
        "horizon_scope_csv": output_dir / "ai_leadership_horizon_scope_matrix.csv",
        "use_case_scope_json": output_dir / "ai_leadership_use_case_scope_matrix.json",
        "use_case_scope_csv": output_dir / "ai_leadership_use_case_scope_matrix.csv",
        "recommended_scope": output_dir / "ai_leadership_recommended_scope.json",
        "safety_boundary": output_dir / "ai_leadership_scope_review_safety_boundary.json",
        "report_doc": docs_root / "ai_semiconductor_leadership_scope_review.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["asset_scope_json"], {**dict(common), "rows": list(asset_rows)})
    write_csv_rows(paths["asset_scope_csv"], asset_rows)
    write_json(paths["horizon_scope_json"], {**dict(common), "rows": list(horizon_rows)})
    write_csv_rows(paths["horizon_scope_csv"], horizon_rows)
    write_json(paths["use_case_scope_json"], {**dict(common), "rows": list(use_case_rows)})
    write_csv_rows(paths["use_case_scope_csv"], use_case_rows)
    write_json(paths["recommended_scope"], {**dict(common), "recommended_scope": recommended_scope})
    write_json(paths["safety_boundary"], _safety_boundary(summary))
    write_markdown(
        paths["report_doc"],
        _render_report(summary, asset_rows, horizon_rows, use_case_rows, recommended_scope),
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
        "full_universe_readiness_claimed": False,
        "does_not_modify_generator_artifacts": True,
        "does_not_modify_actual_path_validation_artifacts": True,
        "does_not_start_forward_observe": True,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        "next_task": summary.get("next_task"),
        **SAFETY_FIELDS,
    }


def _render_report(
    summary: Mapping[str, Any],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    use_case_rows: Sequence[Mapping[str, Any]],
    recommended_scope: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# AI / 半导体 Leadership Scope Review",
            "",
            "TRADING-2310 对 TRADING-2309 actual-path validation evidence 执行 "
            "research-only scope review。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- source_status: `{summary['source_status']}`",
            f"- recommended_asset_scope: `{summary['recommended_asset_scope']}`",
            "- preferred_owner_review_horizons: `{}`".format(
                ','.join(summary.get("preferred_owner_review_horizons", []))
            ),
            "- diagnostic_owner_review_horizons: `{}`".format(
                ','.join(summary.get("diagnostic_owner_review_horizons", []))
            ),
            "- recommended_use_cases: `{}`".format(
                ','.join(summary.get("recommended_use_cases", []))
            ),
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "- dynamic_promotion_status: `BLOCKED`",
            "",
            "## Recommended Scope",
            "",
            f"- scope_review_result: `{recommended_scope.get('scope_review_result')}`",
            f"- smh_only_scope_decision: `{recommended_scope.get('smh_only_scope_decision')}`",
            "- qqq_plus_smh_scope_decision: `{}`".format(
                recommended_scope.get("qqq_plus_smh_scope_decision")
            ),
            f"- not_recommended_as: `{','.join(recommended_scope.get('not_recommended_as', []))}`",
            "",
            "## Asset Scope",
            "",
            *_table(
                asset_rows,
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
                    "owner_review_requested",
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
            "本报告只做 scope review，不修改 TRADING-2308 generator artifacts 或 "
            "TRADING-2309 actual-path validation artifacts，不启动 forward observe，"
            "不允许 promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )


def _table(rows: Sequence[Mapping[str, Any]], *, columns: Sequence[str]) -> list[str]:
    lines = ["|" + "|".join(columns) + "|", "|" + "|".join("---" for _ in columns) + "|"]
    for row in rows:
        lines.append("|" + "|".join(str(row.get(column, "")) for column in columns) + "|")
    return lines


def _load_source_validation(validation_dir: Path) -> dict[str, Any]:
    summary_payload = _read_json(
        validation_dir / "ai_leadership_actual_path_validation_summary.json"
    )
    summary = mapping(summary_payload.get("summary"))
    if not summary:
        raise AILeadershipScopeReviewError("TRADING-2309 summary missing nested summary")
    _validate_source_summary(summary_payload, summary)
    outcome_payload = _read_json(
        validation_dir / "ai_leadership_prediction_outcome_matrix.json"
    )
    candidate_payload = _read_json(validation_dir / "ai_leadership_candidate_scorecard.json")
    objective_payload = _read_json(
        validation_dir / "ai_leadership_objective_coverage_matrix.json"
    )
    outcome_rows = _rows_from_payload(outcome_payload, "rows")
    candidate_rows = _rows_from_payload(candidate_payload, "candidate_scorecards")
    objective_rows = _rows_from_payload(objective_payload, "objective_rows")
    if not outcome_rows or not candidate_rows or not objective_rows:
        raise AILeadershipScopeReviewError("TRADING-2309 source artifacts are incomplete")
    return {
        "summary": summary,
        "outcome_rows": outcome_rows,
        "candidate_rows": candidate_rows,
        "objective_rows": objective_rows,
    }


def _validate_source_summary(
    payload: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> None:
    if summary.get("task_id") != SOURCE_TASK_ID:
        raise AILeadershipScopeReviewError("TRADING-2309 source task_id mismatch")
    if summary.get("status") != SOURCE_STATUS_CONTINUE_RESEARCH:
        raise AILeadershipScopeReviewError(
            "TRADING-2310 requires TRADING-2309 continue-research source status"
        )
    for field, expected in (
        ("actual_path_validation_executed", True),
        ("scope_review_ready", False),
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("dynamic_promotion_status", "BLOCKED"),
    ):
        if summary.get(field) != expected and payload.get(field) != expected:
            raise AILeadershipScopeReviewError(
                f"TRADING-2309 source safety field {field} must be {expected}"
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
        raise AILeadershipScopeReviewError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise AILeadershipScopeReviewError(f"policy file must be object: {path}")
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
        "scope_options",
        "scope_thresholds",
        "status_rule",
        "safety",
    )
    missing = [field for field in required if not policy.get(field)]
    if missing:
        raise AILeadershipScopeReviewError(f"policy missing fields: {missing}")
    allowed = set(_strings(mapping(policy.get("status_rule")).get("allowed_statuses")))
    if allowed != ALLOWED_STATUSES:
        raise AILeadershipScopeReviewError(
            f"policy allowed_statuses must match {sorted(ALLOWED_STATUSES)}"
        )
    for key in (
        "minimum_scope_records",
        "minimum_horizon_records",
        "minimum_use_case_records",
        "asset_keep_alignment_score",
        "asset_reject_alignment_score",
        "horizon_keep_alignment_score",
        "horizon_reject_alignment_score",
        "use_case_keep_alignment_score",
        "use_case_reject_alignment_score",
        "drawdown_support_score",
        "smh_only_primary_min_alignment_score",
        "qqq_plus_smh_min_alignment_score",
        "minimum_objective_pass_count_for_ready",
    ):
        _policy_float(policy, key)
    safety = mapping(policy.get("safety"))
    for field, expected in SAFETY_FIELDS.items():
        if safety.get(field) != expected:
            raise AILeadershipScopeReviewError(
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
    rows: Sequence[Mapping[str, Any]],
    average_score: float | None,
    minimum_records: int,
    keep_threshold: float,
    reject_threshold: float,
    reference_only: bool = False,
) -> str:
    if len(rows) < minimum_records:
        return DECISION_SAMPLE_BLOCKED
    if reference_only:
        return DECISION_REFERENCE
    if average_score is None:
        return DECISION_SAMPLE_BLOCKED
    if average_score >= keep_threshold:
        return DECISION_KEEP
    if average_score <= reject_threshold:
        return DECISION_REJECT
    return DECISION_DIAGNOSTIC


def _rows_for_use_case(
    rows: Sequence[Mapping[str, Any]],
    use_case_id: str,
    definition: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    if use_case_id == "confirmation_only":
        patterns = [value.lower() for value in _strings(definition.get("signal_name_contains"))]
        return [
            row
            for row in rows
            if any(pattern in str(row.get("signal_name", "")).lower() for pattern in patterns)
        ]
    if use_case_id == "exposure_cap_modifier":
        return [
            row
            for row in rows
            if row.get("drawdown_risk_score") not in (None, "")
            or row.get("weakening_window_score") not in (None, "")
        ]
    if use_case_id == "standalone_alpha":
        return [
            row
            for row in rows
            if row.get("smh_relative_return_score") not in (None, "")
        ]
    return []


def _use_case_primary_score_field(use_case_id: str) -> str:
    if use_case_id == "exposure_cap_modifier":
        return "drawdown_risk_score"
    if use_case_id == "standalone_alpha":
        return "smh_relative_return_score"
    return "combined_alignment_score"


def _use_case_keep_threshold(policy: Mapping[str, Any], use_case_id: str) -> float:
    if use_case_id == "exposure_cap_modifier":
        return _policy_float(policy, "drawdown_support_score")
    return _policy_float(policy, "use_case_keep_alignment_score")


def _asset_scope_rationale(scope_id: str, decision: str) -> str:
    if scope_id == "qqq_only_diagnostic":
        return "QQQ-only row is a diagnostic reference for QQQ + SMH comparison."
    if scope_id == "smh_only" and decision != DECISION_KEEP:
        return "SMH-only evidence is not strong enough to be the primary scope."
    if scope_id == "qqq_plus_smh" and decision == DECISION_KEEP:
        return "Combined QQQ + SMH evidence is the keepable research scope."
    return "Decision follows policy-governed average alignment and sample sufficiency."


def _horizon_scope_rationale(horizon: str, decision: str, reference_only: bool) -> str:
    if reference_only:
        return f"{horizon} is retained only as a diagnostic reference, not owner scope."
    if decision == DECISION_KEEP:
        return f"{horizon} clears the owner-reviewed horizon keep threshold."
    return f"{horizon} does not clear the owner-reviewed horizon keep threshold."


def _use_case_rationale(use_case_id: str, decision: str) -> str:
    if use_case_id == "standalone_alpha":
        return "Standalone-alpha use remains rejected as a negative control."
    if decision == DECISION_KEEP:
        return f"{use_case_id} clears the research-only use-case keep threshold."
    return f"{use_case_id} does not clear the research-only use-case keep threshold."


def _scope_review_result(
    *,
    recommended_asset_scope: str,
    kept_owner_horizons: Sequence[str],
    kept_use_cases: Sequence[str],
) -> str:
    if not kept_owner_horizons or not kept_use_cases:
        return "NO_NARROW_SCOPE_READY"
    return "{}_{}_{}_RESEARCH_ONLY".format(
        recommended_asset_scope,
        "_".join(kept_owner_horizons),
        "_".join(kept_use_cases).upper(),
    )


def _row_by_id(rows: Sequence[Mapping[str, Any]], scope_id: str) -> Mapping[str, Any]:
    for row in rows:
        if row.get("scope_id") == scope_id:
            return row
    return {}


def _average(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    values = [_optional_float(row.get(field)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round_float(sum(clean) / len(clean))


def _policy_float(policy: Mapping[str, Any], key: str) -> float:
    threshold = mapping(mapping(policy.get("scope_thresholds")).get(key))
    if "value" not in threshold:
        raise AILeadershipScopeReviewError(f"missing policy threshold: {key}")
    try:
        value = float(threshold.get("value"))
    except (TypeError, ValueError) as exc:
        raise AILeadershipScopeReviewError(f"invalid policy threshold: {key}") from exc
    if not math.isfinite(value):
        raise AILeadershipScopeReviewError(f"invalid policy threshold: {key}")
    return value


def _policy_int(policy: Mapping[str, Any], key: str) -> int:
    value = int(_policy_float(policy, key))
    if value <= 0:
        raise AILeadershipScopeReviewError(f"policy threshold must be positive: {key}")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AILeadershipScopeReviewError(f"required source artifact missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise AILeadershipScopeReviewError(f"{path}: expected JSON object")
    return payload


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise AILeadershipScopeReviewError(f"payload missing list field: {key}")
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
        raise AILeadershipScopeReviewError(f"date must use YYYY-MM-DD: {value}") from exc


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


def _horizon_sort_key(value: str) -> int:
    digits = "".join(character for character in value if character.isdigit())
    return int(digits or 0)


def _read_signal_series(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


__all__ = [
    "ALLOWED_STATUSES",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_VALIDATION_ROOT",
    "MODE",
    "STATUS_INCONCLUSIVE",
    "STATUS_READY_RESEARCH_ONLY",
    "STATUS_REJECT_RECOMMENDED",
    "run_ai_leadership_scope_review",
]
