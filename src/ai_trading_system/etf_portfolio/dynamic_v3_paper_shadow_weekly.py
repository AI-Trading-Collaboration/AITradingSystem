from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.trading_calendar import is_us_equity_trading_day

DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_weekly_review"
)
WEEKLY_DECISIONS = ("CONTINUE", "WATCH", "RETURN_TO_RESEARCH", "REJECT")
WEEKLY_COVERAGE_CLASSIFICATIONS = (
    "FULL_WEEK_REVIEW",
    "PARTIAL_ARTIFACT_WINDOW_REVIEW",
    "RECOVERY_MODE_REVIEW",
    "INSUFFICIENT_REVIEW",
)
WEEKLY_DECISION_POLICY = {
    "policy_id": "TRADING-353_SOURCE_SEVERITY_MAPPING",
    "policy_version": "2026-06-15",
    "status": "pilot_manual_review_baseline",
    "rationale": (
        "TRADING-353 maps already-audited daily and drift source states into a "
        "manual weekly review label; it does not calibrate promotion thresholds, "
        "approve trades, or execute rejection."
    ),
    "rules": [
        {
            "decision": "REJECT",
            "condition": (
                "candidate decision ledger already records a reject decision or a "
                "source drift monitor already recommends reject_candidate"
            ),
        },
        {
            "decision": "RETURN_TO_RESEARCH",
            "condition": (
                "any source drift severity is WARNING/BLOCKING, any daily source is "
                "not RECORDED, or source input artifacts are missing"
            ),
        },
        {
            "decision": "WATCH",
            "condition": (
                "any source drift severity is WATCH, source dates/candidates are "
                "inconsistent, or signal/recommendation/behavior is mixed"
            ),
        },
        {
            "decision": "CONTINUE",
            "condition": "all source states are clean and stable enough for manual continuation",
        },
    ],
}
WEEKLY_COVERAGE_POLICY = {
    "policy_id": "TRADING-353A_WEEKLY_REVIEW_COVERAGE_SUFFICIENCY",
    "policy_version": "2026-06-16",
    "status": "pilot_manual_review_baseline",
    "owner": "system_validation",
    "rationale": (
        "Paper-shadow weekly review must distinguish a full market-week review "
        "from a partial or recovery artifact window so continuation decisions do "
        "not silently treat one-day recovery evidence as full weekly coverage."
    ),
    "rules": [
        {
            "classification": "FULL_WEEK_REVIEW",
            "condition": (
                "selected window covers all expected U.S. equity market days in "
                "the week and daily observations cover every expected market day"
            ),
        },
        {
            "classification": "RECOVERY_MODE_REVIEW",
            "condition": (
                "selected artifact window is shorter than the full market week "
                "but every selected market day has a daily observation"
            ),
        },
        {
            "classification": "PARTIAL_ARTIFACT_WINDOW_REVIEW",
            "condition": (
                "some daily observations exist, but either the selected window "
                "or the covered market days do not satisfy full-week coverage"
            ),
        },
        {
            "classification": "INSUFFICIENT_REVIEW",
            "condition": "no selected market-day daily observation is available",
        },
    ],
}
PAPER_SHADOW_WEEKLY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "paper_shadow_weekly_review_only": True,
    "read_only_review": True,
    "observation_only": True,
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


def build_paper_shadow_weekly_review(
    *,
    candidate: str,
    week_start: str,
    week_end: str,
    daily_observation_ids: Sequence[str] | None = None,
    drift_monitor_ids: Sequence[str] | None = None,
    contract_id: str | None = None,
    ledger_run_id: str | None = None,
    observation_dir: Path = daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    drift_dir: Path = drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    contract_dir: Path = readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    ledger_dir: Path = readiness.DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    generated_at: datetime | None = None,
    manual_coverage_override: bool = False,
    manual_coverage_override_reason: str = "",
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    start_date = _parse_date(week_start)
    end_date = _parse_date(week_end)
    if start_date > end_date:
        raise st.DynamicV3SystemTargetError("week_start must be on or before week_end")
    if manual_coverage_override and not _text(manual_coverage_override_reason):
        raise st.DynamicV3SystemTargetError(
            "manual_coverage_override requires manual_coverage_override_reason"
        )

    daily_payloads = _load_daily_payloads(
        daily_observation_ids,
        observation_dir=observation_dir,
    )
    drift_payloads = _load_drift_payloads(drift_monitor_ids, drift_dir=drift_dir)
    resolved_contract_id = (
        contract_id
        or _first_text(
            _mapping(payload.get("paper_shadow_daily_observation")).get(
                "source_contract_id"
            )
            for payload in daily_payloads
        )
        or None
    )
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=resolved_contract_id,
        latest=resolved_contract_id is None,
        output_dir=contract_dir,
    )
    ledger_payload = readiness.candidate_decision_ledger_report_payload(
        ledger_run_id=ledger_run_id,
        latest=ledger_run_id is None,
        output_dir=ledger_dir,
    )

    daily_records = [
        _daily_record(payload, week_start=start_date, week_end=end_date)
        for payload in daily_payloads
    ]
    drift_records = [_drift_record(payload) for payload in drift_payloads]
    coverage = _weekly_review_coverage(
        selected_window_start=start_date,
        selected_window_end=end_date,
        daily_records=daily_records,
        manual_coverage_override=manual_coverage_override,
        manual_coverage_override_reason=manual_coverage_override_reason,
    )
    contract_decision = _mapping(
        contract_payload.get("formal_research_method_decision")
    )
    ledger_record = _mapping(ledger_payload.get("candidate_decision_record"))
    missing_inputs = _missing_input_artifacts(
        candidate=candidate,
        week_start=start_date,
        week_end=end_date,
        daily_records=daily_records,
        drift_records=drift_records,
        contract_payload=contract_payload,
        ledger_payload=ledger_payload,
    )
    stability = _weekly_stability(daily_records=daily_records, drift_records=drift_records)
    decision, decision_reasons = _weekly_decision(
        daily_records=daily_records,
        drift_records=drift_records,
        missing_inputs=missing_inputs,
        ledger_record=ledger_record,
        stability=stability,
    )
    source_artifacts = _source_artifacts(
        daily_payloads=daily_payloads,
        drift_payloads=drift_payloads,
        contract_payload=contract_payload,
        ledger_payload=ledger_payload,
    )
    weekly_review_id = st._stable_id(
        "paper-shadow-weekly-review",
        candidate,
        week_start,
        week_end,
        ",".join(_text(payload.get("observation_id")) for payload in daily_payloads),
        ",".join(_text(payload.get("monitor_id")) for payload in drift_payloads),
        _text(contract_payload.get("contract_id")),
        _text(ledger_payload.get("ledger_run_id")),
        str(manual_coverage_override),
        _text(manual_coverage_override_reason),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / weekly_review_id)
    root.mkdir(parents=True, exist_ok=False)
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": root.name,
        "candidate": candidate,
        "week_start": week_start,
        "week_end": week_end,
        "generated_at": generated.isoformat(),
        "source_contract_id": contract_payload.get("contract_id"),
        "source_contract_status": contract_decision.get(
            "formal_research_method_status"
        ),
        "source_contract_promotion_state": contract_decision.get("promotion_state"),
        "source_ledger_run_id": ledger_payload.get("ledger_run_id"),
        "source_ledger_record_id": ledger_record.get("record_id"),
        "source_ledger_final_decision": ledger_record.get("final_decision"),
        "source_artifacts": source_artifacts,
        "daily_observations": daily_records,
        "drift_monitors": drift_records,
        **coverage,
        "summary": {
            **stability,
            **coverage,
            "missing_input_artifacts": missing_inputs,
            "reviewer_notes_placeholder": (
                "manual_reviewer_notes_required_before_owner_decision"
            ),
        },
        "weekly_decision": decision,
        "weekly_decision_reasons": decision_reasons,
        "next_required_action": _next_required_action(decision),
        "decision_policy": WEEKLY_DECISION_POLICY,
        "coverage_policy": WEEKLY_COVERAGE_POLICY,
        "limitations": [
            "manual weekly aggregation of existing paper-shadow source artifacts",
            "does not download data or rerun daily/drift/source pipelines",
            "does not modify the append-only candidate decision ledger",
            "does not approve, reject, size, or execute any portfolio action",
        ],
        **PAPER_SHADOW_WEEKLY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_manifest",
        "weekly_review_id": root.name,
        "candidate": candidate,
        "week_start": week_start,
        "week_end": week_end,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "weekly_decision": decision,
        "coverage_classification": coverage["coverage_classification"],
        "coverage_safe_for_continuation": coverage["coverage_safe_for_continuation"],
        "coverage_status": coverage["coverage_status"],
        "manual_coverage_override": coverage["manual_coverage_override"],
        "coverage_policy_id": WEEKLY_COVERAGE_POLICY["policy_id"],
        "coverage_policy_version": WEEKLY_COVERAGE_POLICY["policy_version"],
        "source_contract_id": contract_payload.get("contract_id"),
        "source_ledger_run_id": ledger_payload.get("ledger_run_id"),
        "paper_shadow_weekly_manifest_path": str(
            root / "paper_shadow_weekly_manifest.json"
        ),
        "paper_shadow_weekly_review_path": str(
            root / "paper_shadow_weekly_review.json"
        ),
        "paper_shadow_weekly_report_path": str(
            root / "paper_shadow_weekly_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_weekly_validation.json"),
        **PAPER_SHADOW_WEEKLY_SAFETY,
    }
    reader = render_paper_shadow_weekly_reader_brief(review)
    st._write_json(root / "paper_shadow_weekly_manifest.json", manifest)
    st._write_json(root / "paper_shadow_weekly_review.json", review)
    st._write_text(
        root / "paper_shadow_weekly_report.md",
        render_paper_shadow_weekly_report(manifest, review),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_paper_shadow_weekly_review",
        root.name,
        root / "paper_shadow_weekly_manifest.json",
    )
    validation = validate_paper_shadow_weekly_review_artifact(
        weekly_review_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "weekly_review_id": root.name,
        "weekly_review_dir": root,
        "manifest": manifest,
        "paper_shadow_weekly_review": review,
        "reader_brief_section": reader,
        "paper_shadow_weekly_validation": validation,
    }


def paper_shadow_weekly_review_report_payload(
    *,
    weekly_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=weekly_review_id,
        latest_pointer="latest_paper_shadow_weekly_review",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_weekly_manifest.json",
    )
    payload = {
        **st._read_json(root / "paper_shadow_weekly_manifest.json"),
        "paper_shadow_weekly_review": st._read_json(
            root / "paper_shadow_weekly_review.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "weekly_review_dir": str(root),
    }
    validation = st._read_optional_json(root / "paper_shadow_weekly_validation.json")
    if validation:
        payload["paper_shadow_weekly_validation"] = validation
    return payload


def validate_paper_shadow_weekly_review_artifact(
    *,
    weekly_review_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / weekly_review_id
    manifest = st._read_optional_json(root / "paper_shadow_weekly_manifest.json") or {}
    review = st._read_optional_json(root / "paper_shadow_weekly_review.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    source_artifacts = _records(review.get("source_artifacts"))
    source_ids = {_text(row.get("source_id")) for row in source_artifacts}
    daily_records = _records(review.get("daily_observations"))
    drift_records = _records(review.get("drift_monitors"))
    summary = _mapping(review.get("summary"))
    checks = st._required_file_checks(
        root,
        (
            "paper_shadow_weekly_manifest.json",
            "paper_shadow_weekly_review.json",
            "paper_shadow_weekly_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "weekly_review_id_matches",
                manifest.get("weekly_review_id") == weekly_review_id,
                "",
            ),
            st._check("candidate_visible", bool(_text(review.get("candidate"))), ""),
            st._check(
                "week_window_valid",
                _valid_week_window(review.get("week_start"), review.get("week_end")),
                "",
            ),
            st._check(
                "source_contract_visible",
                bool(_text(review.get("source_contract_id"))),
                "",
            ),
            st._check(
                "source_ledger_visible",
                bool(_text(review.get("source_ledger_run_id"))),
                "",
            ),
            st._check("daily_records_visible", bool(daily_records), ""),
            st._check("drift_records_visible", bool(drift_records), ""),
            st._check(
                "source_types_complete",
                {
                    "paper_shadow_daily_observation",
                    "paper_shadow_drift_monitor",
                    "formal_research_method_contract",
                    "candidate_decision_ledger",
                }.issubset(source_ids),
                ",".join(sorted(source_ids)),
            ),
            st._check(
                "source_artifacts_exist",
                all(row.get("exists") is True for row in source_artifacts),
                "",
            ),
            st._check(
                "weekly_decision_valid",
                review.get("weekly_decision") in WEEKLY_DECISIONS,
                "",
            ),
            st._check(
                "decision_policy_visible",
                _mapping(review.get("decision_policy")).get("policy_id")
                == WEEKLY_DECISION_POLICY["policy_id"],
                "",
            ),
            st._check(
                "coverage_policy_visible",
                _mapping(review.get("coverage_policy")).get("policy_id")
                == WEEKLY_COVERAGE_POLICY["policy_id"]
                and manifest.get("coverage_policy_id")
                == WEEKLY_COVERAGE_POLICY["policy_id"],
                "",
            ),
            st._check(
                "summary_fields_visible",
                all(
                    field in summary
                    for field in (
                        "signal_stability",
                        "hypothetical_recommendation_stability",
                        "turnover_behavior",
                        "drawdown_behavior",
                        "flip_rotation_behavior",
                        "drift_severity_trend",
                        "benchmark_comparison_proxy",
                        "selected_window_start",
                        "selected_window_end",
                        "expected_market_days",
                        "covered_market_days",
                        "missing_market_days",
                        "coverage_ratio",
                        "coverage_classification",
                        "coverage_safe_for_continuation",
                        "coverage_status",
                        "manual_coverage_override",
                        "manual_coverage_override_reason",
                        "missing_input_artifacts",
                        "reviewer_notes_placeholder",
                    )
                ),
                "",
            ),
            st._check(
                "coverage_classification_valid",
                review.get("coverage_classification") in WEEKLY_COVERAGE_CLASSIFICATIONS
                and summary.get("coverage_classification")
                in WEEKLY_COVERAGE_CLASSIFICATIONS
                and manifest.get("coverage_classification")
                in WEEKLY_COVERAGE_CLASSIFICATIONS,
                "",
            ),
            st._check(
                "coverage_fields_visible",
                bool(_text(review.get("selected_window_start")))
                and bool(_text(review.get("selected_window_end")))
                and isinstance(review.get("expected_market_days"), list)
                and isinstance(review.get("covered_market_days"), list)
                and isinstance(review.get("missing_market_days"), list)
                and isinstance(review.get("coverage_safe_for_continuation"), bool)
                and isinstance(review.get("manual_coverage_override"), bool),
                "",
            ),
            st._check(
                "coverage_safe_for_continuation_consistent",
                review.get("coverage_safe_for_continuation")
                == _coverage_safe_for_continuation(
                    classification=_text(review.get("coverage_classification")),
                    manual_coverage_override=review.get("manual_coverage_override") is True,
                    manual_coverage_override_reason=_text(
                        review.get("manual_coverage_override_reason")
                    ),
                ),
                "",
            ),
            st._check(
                "missing_inputs_disclosed",
                isinstance(summary.get("missing_input_artifacts"), list),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "paper_shadow_weekly_review_id" in reader
                and "paper_shadow_weekly_decision" in reader
                and "paper_shadow_weekly_drift_trend" in reader
                and "paper_shadow_weekly_coverage_classification" in reader,
                "",
            ),
            st._check(
                "read_only_review",
                review.get("read_only_review") is True
                and review.get("data_downloaded_by_review") is False
                and review.get("pipelines_executed_by_review") is False,
                "",
            ),
            st._check(
                "not_official_target_weights",
                review.get("official_target_weights") is False
                and review.get("not_official_target_weights") is True
                and manifest.get("official_target_weights") is False,
                "",
            ),
            st._check(
                "paper_account_not_mutated",
                review.get("paper_account_state_mutated") is False
                and manifest.get("paper_account_state_mutated") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, review), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_paper_shadow_weekly_review_validation",
        weekly_review_id,
        checks,
    )
    if write_output:
        st._write_json(root / "paper_shadow_weekly_validation.json", validation)
        st._write_text(
            root / "paper_shadow_weekly_validation.md",
            render_paper_shadow_weekly_validation_report(validation),
        )
    return validation


def render_paper_shadow_weekly_reader_brief(review: Mapping[str, Any]) -> str:
    summary = _mapping(review.get("summary"))
    return "\n".join(
        [
            "## Paper Shadow Weekly Review",
            "",
            f"- paper_shadow_weekly_review_id: {review.get('weekly_review_id')}",
            f"- paper_shadow_weekly_candidate: {review.get('candidate')}",
            f"- paper_shadow_weekly_window: {review.get('week_start')}..{review.get('week_end')}",
            f"- paper_shadow_weekly_decision: {review.get('weekly_decision')}",
            "- paper_shadow_weekly_coverage_classification: "
            f"{review.get('coverage_classification')}",
            "- paper_shadow_weekly_coverage_ratio: "
            f"{review.get('coverage_ratio')}",
            "- paper_shadow_weekly_coverage_safe_for_continuation: "
            f"{review.get('coverage_safe_for_continuation')}",
            "- paper_shadow_weekly_missing_inputs: "
            f"{_join_or_none(summary.get('missing_input_artifacts'))}",
            "- paper_shadow_weekly_drift_trend: "
            f"{_drift_trend_text(summary.get('drift_severity_trend'))}",
            "- safety_boundary: manual weekly review only / read-only source aggregation / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_paper_shadow_weekly_report(
    manifest: Mapping[str, Any],
    review: Mapping[str, Any],
) -> str:
    summary = _mapping(review.get("summary"))
    daily_lines = [
        f"- {row.get('observation_id')}: date={row.get('observation_date')} "
        f"status={row.get('observation_status')} signal={row.get('signal_output')} "
        f"risk={row.get('risk_off_risk_on_state')}"
        for row in _records(review.get("daily_observations"))
    ]
    drift_lines = [
        f"- {row.get('monitor_id')}: observation={row.get('observation_id')} "
        f"severity={row.get('drift_severity')} next_action={row.get('next_action')}"
        for row in _records(review.get("drift_monitors"))
    ]
    source_lines = [
        f"- {row.get('source_id')}: artifact_id={row.get('artifact_id')} "
        f"exists={row.get('exists')} path={row.get('path')}"
        for row in _records(review.get("source_artifacts"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Weekly Review {manifest.get('weekly_review_id')}",
            "",
            "## 目的",
            "聚合一周内的 paper-shadow daily observation、drift monitor、formal "
            "research method contract 和 candidate decision ledger，供 owner 人工复核。",
            "",
            "## 输入 Artifact",
            *source_lines,
            "",
            "## 周度结论",
            f"- weekly_decision: {review.get('weekly_decision')}",
            f"- next_required_action: {review.get('next_required_action')}",
            f"- decision_policy: {_mapping(review.get('decision_policy')).get('policy_id')}",
            f"- coverage_policy: {_mapping(review.get('coverage_policy')).get('policy_id')}",
            "- decision_reasons: "
            f"{_join_or_none(review.get('weekly_decision_reasons'))}",
            f"- selected_window_start: {review.get('selected_window_start')}",
            f"- selected_window_end: {review.get('selected_window_end')}",
            "- expected_market_days: "
            f"{_join_or_none(review.get('expected_market_days'))}",
            "- covered_market_days: "
            f"{_join_or_none(review.get('covered_market_days'))}",
            "- missing_market_days: "
            f"{_join_or_none(review.get('missing_market_days'))}",
            f"- coverage_ratio: {review.get('coverage_ratio')}",
            f"- coverage_classification: {review.get('coverage_classification')}",
            "- coverage_safe_for_continuation: "
            f"{review.get('coverage_safe_for_continuation')}",
            f"- coverage_status: {review.get('coverage_status')}",
            f"- manual_coverage_override: {review.get('manual_coverage_override')}",
            "- manual_coverage_override_reason: "
            f"{review.get('manual_coverage_override_reason') or 'none'}",
            "",
            "## 周度摘要",
            f"- signal_stability: {summary.get('signal_stability')}",
            "- hypothetical_recommendation_stability: "
            f"{summary.get('hypothetical_recommendation_stability')}",
            f"- turnover_behavior: {summary.get('turnover_behavior')}",
            f"- drawdown_behavior: {summary.get('drawdown_behavior')}",
            f"- flip_rotation_behavior: {summary.get('flip_rotation_behavior')}",
            f"- drift_severity_trend: {_drift_trend_text(summary.get('drift_severity_trend'))}",
            f"- benchmark_comparison_proxy: {summary.get('benchmark_comparison_proxy')}",
            "- missing_input_artifacts: "
            f"{_join_or_none(summary.get('missing_input_artifacts'))}",
            f"- reviewer_notes_placeholder: {summary.get('reviewer_notes_placeholder')}",
            "",
            "## Daily Observations",
            *daily_lines,
            "",
            "## Drift Monitors",
            *drift_lines,
            "",
            "## Safety Boundary",
            "- manual weekly review only",
            "- read-only source artifact aggregation",
            "- no data download or upstream pipeline execution",
            "- no candidate decision ledger mutation",
            "- no paper account mutation",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_paper_shadow_weekly_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Weekly Review Validation {validation.get('artifact_id')}",
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


def _load_daily_payloads(
    daily_observation_ids: Sequence[str] | None,
    *,
    observation_dir: Path,
) -> list[dict[str, Any]]:
    ids = [_text(item) for item in (daily_observation_ids or []) if _text(item)]
    if not ids:
        return [daily.paper_shadow_daily_report_payload(latest=True, output_dir=observation_dir)]
    return [
        daily.paper_shadow_daily_report_payload(
            observation_id=observation_id,
            output_dir=observation_dir,
        )
        for observation_id in ids
    ]


def _load_drift_payloads(
    drift_monitor_ids: Sequence[str] | None,
    *,
    drift_dir: Path,
) -> list[dict[str, Any]]:
    ids = [_text(item) for item in (drift_monitor_ids or []) if _text(item)]
    if not ids:
        return [
            drift.paper_shadow_drift_monitor_report_payload(
                latest=True,
                output_dir=drift_dir,
            )
        ]
    return [
        drift.paper_shadow_drift_monitor_report_payload(
            monitor_id=monitor_id,
            output_dir=drift_dir,
        )
        for monitor_id in ids
    ]


def _daily_record(
    payload: Mapping[str, Any],
    *,
    week_start: date,
    week_end: date,
) -> dict[str, Any]:
    observation = _mapping(payload.get("paper_shadow_daily_observation"))
    review = _mapping(observation.get("daily_review"))
    hypothetical = _mapping(review.get("hypothetical_weight_recommendation"))
    observation_date = _parse_optional_date(observation.get("observation_date"))
    return {
        "observation_id": payload.get("observation_id"),
        "candidate": observation.get("candidate"),
        "observation_date": observation.get("observation_date"),
        "within_week_window": observation_date is not None
        and week_start <= observation_date <= week_end,
        "observation_status": observation.get("observation_status"),
        "signal_output": review.get("signal_output"),
        "hypothetical_weight_recommendation": hypothetical.get("value"),
        "risk_off_risk_on_state": review.get("risk_off_risk_on_state"),
        "drawdown_state": review.get("drawdown_state"),
        "rotation_event": review.get("rotation_event"),
        "mismatch_event": review.get("mismatch_event"),
        "benchmark_comparison": review.get("benchmark_comparison"),
        "manual_reviewer_notes": review.get("manual_reviewer_notes"),
        "input_artifacts": _records(observation.get("input_artifacts")),
        "source_contract_id": observation.get("source_contract_id"),
        "manifest_path": payload.get("paper_shadow_daily_manifest_path"),
    }


def _drift_record(payload: Mapping[str, Any]) -> dict[str, Any]:
    report = _mapping(payload.get("paper_shadow_drift_report"))
    return {
        "monitor_id": payload.get("monitor_id"),
        "candidate": report.get("candidate"),
        "observation_id": report.get("observation_id"),
        "observation_date": report.get("observation_date"),
        "observation_status": report.get("observation_status"),
        "drift_severity": report.get("drift_severity"),
        "watch_count": report.get("watch_count"),
        "warning_count": report.get("warning_count"),
        "blocking_count": report.get("blocking_count"),
        "next_action": report.get("next_action"),
        "finding_count": report.get("finding_count"),
        "findings": _records(report.get("findings")),
        "manifest_path": payload.get("paper_shadow_drift_manifest_path"),
    }


def _weekly_review_coverage(
    *,
    selected_window_start: date,
    selected_window_end: date,
    daily_records: Sequence[Mapping[str, Any]],
    manual_coverage_override: bool,
    manual_coverage_override_reason: str,
) -> dict[str, Any]:
    expected_days = _market_week_trading_days(selected_window_end)
    selected_days = _trading_days_between(selected_window_start, selected_window_end)
    selected_day_set = set(selected_days)
    expected_day_set = set(expected_days)
    covered_days = sorted(
        {
            parsed
            for row in daily_records
            if (parsed := _parse_optional_date(row.get("observation_date"))) is not None
            and row.get("within_week_window") is True
            and parsed in expected_day_set
        }
    )
    covered_day_set = set(covered_days)
    missing_days = [value for value in expected_days if value not in covered_day_set]
    selected_covers_full_week = expected_day_set.issubset(selected_day_set)
    covered_covers_full_week = expected_day_set.issubset(covered_day_set)
    classification = _coverage_classification(
        selected_covers_full_week=selected_covers_full_week,
        covered_covers_full_week=covered_covers_full_week,
        covered_days=covered_days,
        selected_days=selected_days,
    )
    override_reason = _text(manual_coverage_override_reason)
    coverage_safe = _coverage_safe_for_continuation(
        classification=classification,
        manual_coverage_override=manual_coverage_override,
        manual_coverage_override_reason=override_reason,
    )
    return {
        "selected_window_start": selected_window_start.isoformat(),
        "selected_window_end": selected_window_end.isoformat(),
        "expected_market_days": [value.isoformat() for value in expected_days],
        "covered_market_days": [value.isoformat() for value in covered_days],
        "missing_market_days": [value.isoformat() for value in missing_days],
        "coverage_ratio": _coverage_ratio(covered_days, expected_days),
        "coverage_classification": classification,
        "coverage_safe_for_continuation": coverage_safe,
        "coverage_status": "PASS" if coverage_safe else "MANUAL_REVIEW_REQUIRED",
        "manual_coverage_override": manual_coverage_override,
        "manual_coverage_override_reason": override_reason,
    }


def _market_week_trading_days(value: date) -> list[date]:
    week_start = value - timedelta(days=value.weekday())
    week_end = week_start + timedelta(days=4)
    return _trading_days_between(week_start, week_end)


def _trading_days_between(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _coverage_ratio(covered_days: Sequence[date], expected_days: Sequence[date]) -> float:
    if not expected_days:
        return 0.0
    return round(len(set(covered_days)) / len(set(expected_days)), 4)


def _coverage_classification(
    *,
    selected_covers_full_week: bool,
    covered_covers_full_week: bool,
    covered_days: Sequence[date],
    selected_days: Sequence[date],
) -> str:
    if not covered_days:
        return "INSUFFICIENT_REVIEW"
    if selected_covers_full_week and covered_covers_full_week:
        return "FULL_WEEK_REVIEW"
    if not selected_covers_full_week and set(selected_days).issubset(set(covered_days)):
        return "RECOVERY_MODE_REVIEW"
    return "PARTIAL_ARTIFACT_WINDOW_REVIEW"


def _coverage_safe_for_continuation(
    *,
    classification: str,
    manual_coverage_override: bool,
    manual_coverage_override_reason: str,
) -> bool:
    return classification == "FULL_WEEK_REVIEW" or (
        manual_coverage_override and bool(_text(manual_coverage_override_reason))
    )


def _weekly_stability(
    *,
    daily_records: Sequence[Mapping[str, Any]],
    drift_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    drift_sequence = [_text(row.get("drift_severity")) for row in drift_records]
    return {
        "signal_stability": _stability_label(
            [row.get("signal_output") for row in daily_records]
        ),
        "hypothetical_recommendation_stability": _stability_label(
            [row.get("hypothetical_weight_recommendation") for row in daily_records]
        ),
        "turnover_behavior": _behavior_label(
            [row.get("rotation_event") for row in daily_records],
            _finding_values(drift_records, "unexpected_turnover_increase"),
        ),
        "drawdown_behavior": _behavior_label(
            [row.get("drawdown_state") for row in daily_records]
            + [row.get("mismatch_event") for row in daily_records],
            _finding_values(drift_records, "drawdown_mismatch_regression"),
        ),
        "flip_rotation_behavior": _behavior_label(
            [row.get("rotation_event") for row in daily_records],
            _finding_values(drift_records, "flip_rotation_regression"),
        ),
        "drift_severity_trend": {
            "sequence": drift_sequence,
            "max_severity": _max_severity(drift_sequence),
            "blocking_count": sum(
                1 for value in drift_sequence if value == "BLOCKING"
            ),
            "warning_count": sum(1 for value in drift_sequence if value == "WARNING"),
            "watch_count": sum(1 for value in drift_sequence if value == "WATCH"),
        },
        "benchmark_comparison_proxy": _behavior_label(
            [row.get("benchmark_comparison") for row in daily_records],
            _finding_values(drift_records, "benchmark_underperformance"),
        ),
    }


def _weekly_decision(
    *,
    daily_records: Sequence[Mapping[str, Any]],
    drift_records: Sequence[Mapping[str, Any]],
    missing_inputs: Sequence[str],
    ledger_record: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    ledger_decision = _text(ledger_record.get("final_decision")).lower()
    if "reject" in ledger_decision:
        reasons.append("source_candidate_decision_ledger_reject")
        return "REJECT", reasons
    if any(row.get("next_action") == "reject_candidate" for row in drift_records):
        reasons.append("source_drift_monitor_reject_candidate")
        return "REJECT", reasons
    if any(row.get("drift_severity") in ("BLOCKING", "WARNING") for row in drift_records):
        reasons.append("source_drift_warning_or_blocking")
    if any(row.get("observation_status") != "RECORDED" for row in daily_records):
        reasons.append("daily_observation_not_recorded")
    if missing_inputs:
        reasons.append("missing_inputs_disclosed")
    if reasons:
        return "RETURN_TO_RESEARCH", reasons
    if any(row.get("drift_severity") == "WATCH" for row in drift_records):
        reasons.append("source_drift_watch")
    if any(row.get("within_week_window") is not True for row in daily_records):
        reasons.append("daily_observation_outside_week_window")
    mixed_fields = (
        "signal_stability",
        "hypothetical_recommendation_stability",
        "turnover_behavior",
        "drawdown_behavior",
        "flip_rotation_behavior",
        "benchmark_comparison_proxy",
    )
    if any(stability.get(field) == "MIXED" for field in mixed_fields):
        reasons.append("mixed_weekly_source_behavior")
    if reasons:
        return "WATCH", reasons
    return "CONTINUE", ["source_artifacts_clean_and_stable"]


def _next_required_action(decision: str) -> str:
    return {
        "CONTINUE": "continue_weekly_paper_shadow_review",
        "WATCH": "manual_watch_review_before_continuing_shadow",
        "RETURN_TO_RESEARCH": "return_candidate_to_research_review",
        "REJECT": "manual_reject_candidate_review_required",
    }[decision]


def _missing_input_artifacts(
    *,
    candidate: str,
    week_start: date,
    week_end: date,
    daily_records: Sequence[Mapping[str, Any]],
    drift_records: Sequence[Mapping[str, Any]],
    contract_payload: Mapping[str, Any],
    ledger_payload: Mapping[str, Any],
) -> list[str]:
    missing: list[str] = []
    daily_ids = {_text(row.get("observation_id")) for row in daily_records}
    for row in daily_records:
        observation_id = _text(row.get("observation_id"), "MISSING_DAILY")
        if row.get("candidate") != candidate:
            missing.append(f"{observation_id}:candidate_mismatch")
        if row.get("within_week_window") is not True:
            missing.append(f"{observation_id}:outside_week_window")
        if row.get("observation_status") != "RECORDED":
            missing.append(
                f"{observation_id}:observation_status={row.get('observation_status')}"
            )
        for artifact in _records(row.get("input_artifacts")):
            if artifact.get("exists") is not True or not _text(
                artifact.get("checksum_sha256")
            ):
                missing.append(f"{observation_id}:{artifact.get('source_id')}")
    for row in drift_records:
        monitor_id = _text(row.get("monitor_id"), "MISSING_DRIFT")
        if row.get("candidate") != candidate:
            missing.append(f"{monitor_id}:candidate_mismatch")
        if _text(row.get("observation_id")) not in daily_ids:
            missing.append(f"{monitor_id}:daily_observation_not_in_weekly_input")
    if not _text(contract_payload.get("contract_id")):
        missing.append("formal_research_method_contract:missing_contract_id")
    if not _text(ledger_payload.get("ledger_run_id")):
        missing.append("candidate_decision_ledger:missing_ledger_run_id")
    if not daily_records:
        missing.append("paper_shadow_daily_observation:none_loaded")
    if not drift_records:
        missing.append("paper_shadow_drift_monitor:none_loaded")
    if week_start > week_end:
        missing.append("week_window:invalid")
    return sorted(set(missing))


def _source_artifacts(
    *,
    daily_payloads: Sequence[Mapping[str, Any]],
    drift_payloads: Sequence[Mapping[str, Any]],
    contract_payload: Mapping[str, Any],
    ledger_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(
        _source_row(
            source_id="paper_shadow_daily_observation",
            artifact_id=payload.get("observation_id"),
            path=payload.get("paper_shadow_daily_manifest_path"),
            status=payload.get("status"),
        )
        for payload in daily_payloads
    )
    rows.extend(
        _source_row(
            source_id="paper_shadow_drift_monitor",
            artifact_id=payload.get("monitor_id"),
            path=payload.get("paper_shadow_drift_manifest_path"),
            status=payload.get("status"),
        )
        for payload in drift_payloads
    )
    rows.append(
        _source_row(
            source_id="formal_research_method_contract",
            artifact_id=contract_payload.get("contract_id"),
            path=contract_payload.get("formal_research_method_contract_path"),
            status=contract_payload.get("status"),
        )
    )
    rows.append(
        _source_row(
            source_id="candidate_decision_ledger",
            artifact_id=ledger_payload.get("ledger_run_id"),
            path=ledger_payload.get("candidate_decision_ledger_manifest_path"),
            status=ledger_payload.get("status"),
        )
    )
    return rows


def _source_row(
    *,
    source_id: str,
    artifact_id: object,
    path: object,
    status: object,
) -> dict[str, Any]:
    path_text = _text(path)
    return {
        "source_id": source_id,
        "artifact_id": artifact_id,
        "path": path_text,
        "exists": bool(path_text) and Path(path_text).exists(),
        "status": status,
    }


def _finding_values(
    drift_records: Sequence[Mapping[str, Any]],
    family: str,
) -> list[str]:
    values: list[str] = []
    for record in drift_records:
        for finding in _records(record.get("findings")):
            if finding.get("family") == family:
                values.append(_text(finding.get("severity")))
    return values


def _stability_label(values: Sequence[object]) -> str:
    cleaned = sorted({_text(value) for value in values if _text(value)})
    if not cleaned:
        return "MISSING"
    if len(cleaned) == 1:
        return "STABLE"
    return "MIXED"


def _behavior_label(source_values: Sequence[object], severity_values: Sequence[str]) -> str:
    severities = {_text(value) for value in severity_values if _text(value)}
    cleaned = {_text(value).lower() for value in source_values if _text(value)}
    if "BLOCKING" in severities or "WARNING" in severities:
        return "REGRESSION"
    if "WATCH" in severities:
        return "WATCH"
    if severities and severities.issubset({"NONE"}):
        return "STABLE"
    if not cleaned:
        return "MISSING"
    if len(cleaned) == 1:
        return "STABLE"
    return "MIXED"


def _max_severity(values: Sequence[str]) -> str:
    rank = {"NONE": 0, "WATCH": 1, "WARNING": 2, "BLOCKING": 3}
    cleaned = [value for value in values if value in rank]
    if not cleaned:
        return "MISSING"
    return max(cleaned, key=lambda value: rank[value])


def _drift_trend_text(value: object) -> str:
    trend = _mapping(value)
    if not trend:
        return "MISSING"
    return (
        f"max={trend.get('max_severity')}; "
        f"sequence={','.join(_texts(trend.get('sequence')))}"
    )


def _valid_week_window(start: object, end: object) -> bool:
    start_date = _parse_optional_date(start)
    end_date = _parse_optional_date(end)
    return start_date is not None and end_date is not None and start_date <= end_date


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise st.DynamicV3SystemTargetError(f"invalid date: {value}") from exc


def _parse_optional_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _first_text(values: Sequence[object]) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _join_or_none(value: object) -> str:
    return ", ".join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
